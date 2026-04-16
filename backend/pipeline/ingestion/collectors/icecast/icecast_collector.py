from __future__ import annotations

import asyncio
import base64
import collections
import contextlib
import datetime
import logging
import os
import tempfile
import time
import uuid
from pathlib import Path
from typing import TYPE_CHECKING
from urllib.parse import urlencode, urljoin

from google.cloud import secretmanager

from backend.pipeline.common.constants import (
    AUDIO_FORMAT,
    CHUNK_DURATION_SECONDS,
    NUM_AUDIO_CHANNELS,
    SAMPLE_RATE_HZ,
)
from backend.pipeline.ingestion.models import CapturedChunk

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from backend.pipeline.storage.feed_store import LeasedFeed

logger = logging.getLogger(__name__)

# Audio processing constants
SAMPLE_FORMAT = "s16"  # 16-bit signed integer

READ_TIMEOUT_SEC = 30  # Max seconds without a finalized segment before timeout
POLL_INTERVAL_SEC = 0.25  # Polling interval for segment file checks
STDERR_TAIL_LINES = 30  # Ring buffer size for ffmpeg stderr diagnostics


def _get_broadcastify_credentials() -> tuple[str, str]:
    """Fetch Broadcastify username and password synchronously from Secret Manager.

    Returns:
        A tuple of (username, password).

    Raises:
        RuntimeError: If required env vars are missing or the secret cannot be accessed.
    """
    project_id = os.getenv("GOOGLE_CLOUD_PROJECT")
    username_secret_id = os.getenv("BROADCASTIFY_USERNAME_SECRET_ID")
    password_secret_id = os.getenv("BROADCASTIFY_PASSWORD_SECRET_ID")
    if not project_id or not username_secret_id or not password_secret_id:
        msg = (
            "GOOGLE_CLOUD_PROJECT, BROADCASTIFY_USERNAME_SECRET_ID, and "
            "BROADCASTIFY_PASSWORD_SECRET_ID must be set"
        )
        raise RuntimeError(msg)

    client = secretmanager.SecretManagerServiceClient()

    def _fetch_secret(secret_id: str) -> str:
        name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
        try:
            response = client.access_secret_version(request={"name": name})
            return response.payload.data.decode("UTF-8").strip()
        except Exception as e:
            logger.exception("Failed to access secret %s: %s", name, e)
            msg = f"Failed to access secret {name}"
            raise RuntimeError(msg) from e

    username = _fetch_secret(username_secret_id)
    password = _fetch_secret(password_secret_id)
    return username, password


def _build_auth_header(username: str, password: str) -> str:
    """Build Basic Auth header from credentials.

    Args:
        username: Broadcastify username.
        password: Broadcastify password.

    Returns:
        An HTTP Authorization header string for Basic auth.
    """
    credentials = f"{username}:{password}"
    encoded = base64.b64encode(credentials.encode()).decode()
    return f"Authorization: Basic {encoded}\r\n"


def _now_utc() -> datetime.datetime:
    return datetime.datetime.now(tz=datetime.UTC)


async def _drain_stderr(
    stderr: asyncio.StreamReader,
    tail: collections.deque[str],
) -> None:
    """Read stderr line-by-line, keeping only the last *STDERR_TAIL_LINES* in *tail*.

    Draining prevents the OS pipe buffer from filling, which would deadlock
    ffmpeg on long-running streams.  The tail buffer provides error context
    when the process exits with a non-zero code.

    Exceptions are caught and logged so they cannot mask exceptions from
    the caller's ``try`` block when this task is awaited in ``finally``.
    """
    try:
        while True:
            line = await stderr.readline()
            if not line:  # EOF — process closed stderr
                break
            tail.append(line.decode("utf-8", errors="replace").rstrip())
    except asyncio.CancelledError:
        raise
    except Exception:
        logger.warning("stderr drain failed", exc_info=True)


def _segment_path(directory: Path, index: int) -> Path:
    return directory / f"chunk_{index:06d}.{AUDIO_FORMAT}"


async def capture_icecast_stream(  # noqa: PLR0915
    feed: LeasedFeed, shutdown_event: asyncio.Event, url_base: str
) -> AsyncIterator[CapturedChunk]:
    """
    Capture audio chunks from an Icecast stream using ffmpeg segment muxing.

    This implementation asks ffmpeg to write complete audio files for fixed
    CHUNK_DURATION_SECONDS windows. Each yielded chunk is therefore a standalone
    decodable file rather than an arbitrary slice of a continuous bytestream.

    Args:
        feed: Leased feed containing source_feed_id and metadata
        shutdown_event: Signals graceful shutdown request
        url_base: The base URL to prepend to the source_feed_id for stream access

    Yields:
        A CapturedChunk containing:
        - audio_bytes: Complete audio file bytes for the segment
        - chunk_start_time: The exact audio start time of the segment window
        - chunk_end_time: The exact audio end time of the segment window

    Raises:
        ValueError: If source_feed_id is missing from feed properties
        RuntimeError: If ffmpeg exits unexpectedly or stalls

    """
    session_id = str(uuid.uuid4())
    source_feed_id = feed.get("source_feed_id")
    feed_id = feed.get("id")
    feed_name = feed.get("name")
    if not source_feed_id:
        msg = f"Feed {feed_id} ({feed_name}) missing source_feed_id in feed_properties"
        raise ValueError(msg)

    auth_header = _build_auth_header(*await asyncio.to_thread(_get_broadcastify_credentials))
    normalized_url_base = url_base if url_base.endswith("/") else f"{url_base}/"
    # Disable burst-on-connect behavior to prevent sputtering during initial ffmpeg streaming.
    # Note: Some Icecast servers may not support this parameter.
    params = urlencode({"burst": 0})
    url = urljoin(normalized_url_base, f"{source_feed_id.strip()}.mp3?{params}")

    with tempfile.TemporaryDirectory(prefix="icecast_segments_") as tmp_dir:
        segment_dir = Path(tmp_dir)
        segment_pattern = str(segment_dir / f"chunk_%06d.{AUDIO_FORMAT}")

        process = await _create_ffmpeg_process(
            url, segment_pattern, auth_header
        )
        if (
            process.stderr is None
        ):  # pragma: no cover — guaranteed by stderr=PIPE
            msg = "stderr is None; _create_ffmpeg_process must use stderr=PIPE"
            raise RuntimeError(msg)
        stderr_tail: collections.deque[str] = collections.deque(
            maxlen=STDERR_TAIL_LINES
        )
        drain_task = asyncio.create_task(
            _drain_stderr(process.stderr, stderr_tail)
        )
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
        stream_anchor_time = _now_utc()

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
                if current_segment.exists() and (
                    next_segment.exists() or process_done
                ):
                    segment_bytes = await asyncio.to_thread(
                        current_segment.read_bytes
                    )
                    if segment_bytes:
                        # Calculate the start and end times of this specific chunk's window
                        chunk_start_time = (
                            stream_anchor_time
                            + datetime.timedelta(
                                seconds=next_index * CHUNK_DURATION_SECONDS
                            )
                        )
                        chunk_end_time = chunk_start_time + datetime.timedelta(
                            seconds=CHUNK_DURATION_SECONDS
                        )
                        if process_done:
                            chunk_end_time = min(chunk_end_time, _now_utc())
                        yield CapturedChunk(
                            audio_bytes=segment_bytes,
                            chunk_start_time=chunk_start_time,
                            chunk_end_time=chunk_end_time,
                            session_id=session_id,
                        )

                        last_activity_time = time.monotonic()
                    await asyncio.to_thread(
                        current_segment.unlink, missing_ok=True
                    )
                    next_index += 1
                    continue

                # If ffmpeg is done and there is no pending finalized segment,
                # we are finished.
                if process_done and not current_segment.exists():
                    exit_code = wait_task.result()
                    if exit_code != 0:
                        stderr_snippet = (
                            "\n".join(stderr_tail)
                            if stderr_tail
                            else "(no stderr captured)"
                        )
                        msg = (
                            f"Feed {feed_id} ({feed_name}): "
                            f"ffmpeg exited with code {exit_code}\n"
                            f"stderr tail:\n{stderr_snippet}"
                        )
                        raise RuntimeError(msg)
                    logger.info(
                        "Feed %s (%s): ffmpeg exited normally",
                        feed_id,
                        feed_name,
                    )
                    return

                if time.monotonic() - last_activity_time > READ_TIMEOUT_SEC:
                    stderr_snippet = (
                        "\n".join(stderr_tail)
                        if stderr_tail
                        else "(no stderr captured)"
                    )
                    msg = (
                        f"Feed {feed_id} ({feed_name}): no finalized segment within "
                        f"{READ_TIMEOUT_SEC}s\n"
                        f"stderr tail:\n{stderr_snippet}"
                    )
                    raise RuntimeError(msg)

                await asyncio.sleep(POLL_INTERVAL_SEC)

        finally:
            drain_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await drain_task
            if not wait_task.done():
                wait_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await wait_task
            await _cleanup_ffmpeg_process(process, str(feed_id), str(feed_name))


async def _create_ffmpeg_process(
    url: str,
    segment_pattern: str,
    auth_header: str,
) -> asyncio.subprocess.Process:
    """
    Create and launch ffmpeg subprocess configured for segmented audio output.

    Args:
        url: The stream URL to connect to
        segment_pattern: Segment filename pattern for ffmpeg
        auth_header: HTTP Authorization header for the stream

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
    # 4. -reconnect 1 / -reconnect_at_eof 1 / -reconnect_streamed 1: Enables native
    #    HTTP/TCP reconnects for short internet drops. The external Python timeout
    #    (30s) acts as a secondary dead-man's switch if ffmpeg stalls.
    return await asyncio.create_subprocess_exec(
        "ffmpeg", "-nostdin",
        "-reconnect", "1",
        "-reconnect_at_eof", "1",
        "-reconnect_streamed", "1",
        "-reconnect_delay_max", "2",
        "-analyzeduration", "0",
        "-probesize", "32768",
        "-fflags", "nobuffer+flush_packets+discardcorrupt",
        "-headers", auth_header,
        "-i", url,
        "-vn", "-sn", "-dn",
        "-acodec", AUDIO_FORMAT,
        "-ar", str(SAMPLE_RATE_HZ),
        "-sample_fmt", SAMPLE_FORMAT,
        "-ac", str(NUM_AUDIO_CHANNELS),
        "-compression_level", "0",
        "-f", "segment",
        "-segment_time", str(CHUNK_DURATION_SECONDS),
        "-segment_format", AUDIO_FORMAT,
        "-reset_timestamps", "1",
        "-segment_start_number", "0",
        segment_pattern,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.PIPE,
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
                "Feed %s (%s): Error terminating ffmpeg: %s",
                feed_id,
                feed_name,
                e,
            )
