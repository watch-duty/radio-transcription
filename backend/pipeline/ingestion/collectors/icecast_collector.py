from __future__ import annotations

import asyncio
import base64
import contextlib
import datetime
import logging
import os
import sys
import tempfile
import time
from pathlib import Path
from typing import TYPE_CHECKING

from backend.pipeline.common.constants import AUDIO_SAMPLE_RATE, CHUNK_DURATION_SECONDS
from backend.pipeline.ingestion.normalizer_runtime import NormalizerRuntime
from backend.pipeline.ingestion.settings import NormalizerSettings
from backend.pipeline.shared_constants import CHUNK_DURATION_SECONDS

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from backend.pipeline.storage.feed_store import LeasedFeed

logger = logging.getLogger(__name__)

# Audio processing constants
SAMPLE_FORMAT = "s16"  # 16-bit signed integer
SAMPLE_WIDTH = 2
BYTES_PER_CHUNK = AUDIO_SAMPLE_RATE * CHUNK_DURATION_SECONDS * SAMPLE_WIDTH
READ_TIMEOUT_SEC = 30
POLL_INTERVAL_SEC = 0.25

# Authentication for Icecast/Broadcastify streams
USER = os.getenv("BROADCASTIFY_USERNAME")
PASS = os.getenv("BROADCASTIFY_PASSWORD")

if not USER or not PASS:
    logger.critical(
        "BROADCASTIFY_USERNAME and BROADCASTIFY_PASSWORD env vars must be set."
    )
    sys.exit(1)

# Create the Basic Auth header value for ffmpeg
credentials = f"{USER}:{PASS}"
encoded_credentials = base64.b64encode(credentials.encode()).decode()
auth_header = f"Authorization: Basic {encoded_credentials}\r\n"


def _segment_path(directory: Path, index: int) -> Path:
    return directory / f"chunk_{index:06d}.flac"


async def capture_icecast_stream(
    feed: LeasedFeed,
    shutdown_event: asyncio.Event,
) -> AsyncIterator[tuple[bytes, datetime.datetime]]:
    """
    Capture audio chunks from an Icecast stream using ffmpeg segment muxing.

    This implementation asks ffmpeg to write complete FLAC files for fixed
    CHUNK_DURATION_SECONDS windows. Each yielded chunk is therefore a standalone
    decodable FLAC file rather than an arbitrary slice of a continuous bytestream.

    Args:
        feed: Leased feed containing stream_url and metadata
        shutdown_event: Signals graceful shutdown request

    Yields:
        A tuple containing:
        - (bytes) Complete audio file bytes for the segment
        - (datetime.datetime) The exact audio start time of the segment window

    Raises:
        ValueError: If stream_url is missing from feed properties
        RuntimeError: If ffmpeg exits unexpectedly or stalls

    """
    url = feed.get("stream_url")
    feed_id = feed.get("id")
    feed_name = feed.get("name")
    if not url:
        msg = f"Feed {feed_id} ({feed_name}) missing stream_url in feed_properties_icecast"
        raise ValueError(msg)

    with tempfile.TemporaryDirectory(prefix="icecast_segments_") as tmp_dir:
        segment_dir = Path(tmp_dir)
        segment_pattern = str(segment_dir / "chunk_%06d.flac")

        process = await _create_ffmpeg_process(url, segment_pattern)
        logger.info(
            "Feed %s (%s): Started ffmpeg segmenter (PID: %s)",
            feed_id,
            feed_name,
            process.pid,
        )

        next_index = 0
        last_activity_time = time.monotonic()
        wait_task = asyncio.create_task(process.wait())

        # Anchor the stream timeline to the exact moment ffmpeg starts
        stream_anchor_time = datetime.datetime.now(tz=datetime.UTC)

        try:
            while True:
                if shutdown_event.is_set():
                    logger.info(
                        "Feed %s (%s): Shutdown requested, stopping capture",
                        feed_id,
                        feed_name,
                    )
                    return

                current_segment = _segment_path(segment_dir, next_index)
                next_segment = _segment_path(segment_dir, next_index + 1)
                process_done = wait_task.done()

                # Read a segment only once we know ffmpeg finished writing it.
                # A segment is considered finalized when either:
                # - the next segment exists, or
                # - ffmpeg has exited.
                if current_segment.exists() and (next_segment.exists() or process_done):
                    segment_bytes = await asyncio.to_thread(current_segment.read_bytes)
                    if segment_bytes:
                        # Calculate the start time of this specific chunk's window
                        chunk_start_time = stream_anchor_time + datetime.timedelta(
                            seconds=next_index * CHUNK_DURATION_SECONDS
                        )
                        yield segment_bytes, chunk_start_time

                        last_activity_time = time.monotonic()
                    await asyncio.to_thread(current_segment.unlink, missing_ok=True)
                    next_index += 1
                    continue

                # If ffmpeg is done and there is no pending finalized segment,
                # we are finished.
                if process_done and not current_segment.exists():
                    exit_code = wait_task.result()
                    if exit_code != 0:
                        msg = (
                            f"Feed {feed_id} ({feed_name}): "
                            f"ffmpeg exited with code {exit_code}"
                        )
                        raise RuntimeError(msg)
                    logger.info(
                        "Feed %s (%s): ffmpeg exited normally", feed_id, feed_name
                    )
                    return

                if time.monotonic() - last_activity_time > READ_TIMEOUT_SEC:
                    msg = (
                        f"Feed {feed_id} ({feed_name}): no finalized segment within "
                        f"{READ_TIMEOUT_SEC}s"
                    )
                    raise RuntimeError(msg)

                await asyncio.sleep(POLL_INTERVAL_SEC)

        finally:
            if not wait_task.done():
                wait_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await wait_task
            await _cleanup_ffmpeg_process(process, str(feed_id), str(feed_name))


async def _create_ffmpeg_process(
    url: str,
    segment_pattern: str,
) -> asyncio.subprocess.Process:
    """
    Create and launch ffmpeg subprocess configured for segmented FLAC output.

    Args:
        url: The stream URL to connect to
        segment_pattern: Segment filename pattern for ffmpeg

    Returns:
        The subprocess process object

    """
    # Low-latency live stream network optimizations used below:
    # 1. -analyzeduration 0 / -probesize 32768: Bypasses the default 5-second/5MB
    #    initialization handicap, instantly locking the demuxer on the first 32KB of data.
    #    This reduces the time-to-first-byte from the start timestamp when ffmpeg starts recording.
    # 2. -fflags nobuffer+flush_packets: Drops the demuxer/muxer packet buffering
    #    for true real-time network flow.
    # 3. discardcorrupt: Mitigates parsing crashes over TCP jitter, which is necessary
    #    since our micro probesize doesn't deeply validate stream integrity.
    return await asyncio.create_subprocess_exec(
        "ffmpeg", "-nostdin",
        "-analyzeduration", "0",
        "-probesize", "32768",
        "-fflags", "nobuffer+flush_packets+discardcorrupt",
        "-headers", auth_header,
        "-i", url,
        "-vn", "-sn", "-dn",
        "-acodec", "flac",
        "-ar", str(AUDIO_SAMPLE_RATE),
        "-sample_fmt", SAMPLE_FORMAT,
        "-ac", "1",
        "-compression_level", "0",
        "-f", "segment",
        "-segment_time", str(CHUNK_DURATION_SECONDS),
        "-segment_format", "flac",
        "-reset_timestamps", "1",
        "-segment_start_number", "0",
        segment_pattern,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.DEVNULL,
    )  # fmt: skip


async def _cleanup_ffmpeg_process(
    process: asyncio.subprocess.Process,
    feed_id: str,
    feed_name: str,
) -> None:
    """
    Clean up and terminate ffmpeg process.

    Args:
        process: The ffmpeg subprocess process object
        feed_id: The feed ID for logging
        feed_name: The feed name for logging

    """
    if process.returncode is None:
        try:
            process.terminate()
            await asyncio.wait_for(process.wait(), timeout=5)
        except TimeoutError:
            process.kill()
            logger.warning(
                "Feed %s (%s): Force-killed ffmpeg process", feed_id, feed_name
            )
            await process.wait()
        except Exception as e:
            logger.exception(
                "Feed %s (%s): Error terminating ffmpeg: %s", feed_id, feed_name, e
            )


def main() -> None:
    """
    Entry point for the Icecast stream collector.

    Initializes NormalizerRuntime with the Icecast capture function and
    blocks until graceful shutdown completes.
    """
    settings = NormalizerSettings()
    runtime = NormalizerRuntime(capture_icecast_stream, settings)
    runtime.run()


if __name__ == "__main__":
    main()
