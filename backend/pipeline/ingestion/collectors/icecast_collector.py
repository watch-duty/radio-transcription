from __future__ import annotations

import asyncio
import base64
import io
import logging
import os
import sys
import wave
from typing import TYPE_CHECKING

from backend.pipeline.ingestion.normalizer_runtime import NormalizerRuntime
from backend.pipeline.ingestion.settings import NormalizerSettings

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from backend.pipeline.storage.feed_store import LeasedFeed

logger = logging.getLogger(__name__)

# Audio processing constants
NUM_CHANNELS = 1
CHUNK_DURATION_SECONDS = 15
SAMPLE_RATE = 16000
SAMPLE_WIDTH = 2
BYTES_PER_CHUNK = SAMPLE_RATE * CHUNK_DURATION_SECONDS * SAMPLE_WIDTH

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
encoded_bytes = base64.b64encode(credentials.encode("utf-8"))
encoded_string = encoded_bytes.decode("utf-8")
auth_header = f"Authorization: Basic {encoded_string}\r\n"


async def capture_icecast_stream(
    feed: LeasedFeed,
    shutdown_event: asyncio.Event,
) -> AsyncIterator[bytes]:
    """
    Capture audio chunks from an Icecast stream using ffmpeg.

    This is the core capture function used by NormalizerRuntime. It:
    1. Spawns ffmpeg to connect to the stream URL with Basic Auth
    2. Reads raw PCM audio from ffmpeg's stdout
    3. Converts chunks to WAV format
    4. Yields WAV bytes for the runtime to upload to GCS

    Args:
        feed: Leased feed containing stream_url and metadata
        shutdown_event: Signals graceful shutdown request

    Yields:
        WAV-formatted audio chunks of CHUNK_DURATION_SECONDS each

    Raises:
        ValueError: If stream_url is missing from feed properties
        RuntimeError: If ffmpeg subprocess fails to start or exits prematurely

    """
    url = feed.get("stream_url")
    if not url:
        msg = f"Feed {feed['name']} missing stream_url in feed_properties_icecast"
        raise ValueError(msg)

    feed_name = feed["name"]

    # Launch ffmpeg subprocess
    process = await _create_ffmpeg_process(url)
    logger.info(f"Feed {feed_name}: Started ffmpeg (PID: {process.pid})")

    buffer = bytearray()

    try:
        while True:
            # Check for shutdown signal
            if shutdown_event.is_set():
                logger.info(f"Feed {feed_name}: Shutdown requested, stopping capture")
                return

            # Read from ffmpeg stdout
            if process.stdout:
                chunk_raw = await process.stdout.read(4096)
                if not chunk_raw:
                    # ffmpeg exited
                    exit_code = await process.wait()
                    if exit_code != 0:
                        msg = f"Feed {feed_name}: ffmpeg exited with code {exit_code}"
                        raise RuntimeError(msg)
                    logger.info(f"Feed {feed_name}: ffmpeg exited normally")
                    return
            else:
                msg = f"Feed {feed_name}: ffmpeg stdout is None"
                raise RuntimeError(msg)

            buffer.extend(chunk_raw)

            # Process complete chunks
            while len(buffer) >= BYTES_PER_CHUNK:
                current_chunk = buffer[:BYTES_PER_CHUNK]
                del buffer[:BYTES_PER_CHUNK]

                # Convert raw PCM to WAV format
                wav_data = _pcm_to_wav(current_chunk, feed_name)
                yield wav_data

    finally:
        # Cleanup: terminate ffmpeg if still running
        if process.returncode is None:
            try:
                process.terminate()
                await asyncio.wait_for(process.wait(), timeout=5)
            except TimeoutError:
                process.kill()
                logger.warning(f"Feed {feed_name}: Force-killed ffmpeg process")
                await process.wait()


async def _create_ffmpeg_process(url: str) -> asyncio.subprocess.Process:
    """
    Create and launch ffmpeg subprocess.

    Args:
        url: The stream URL to connect to

    Returns:
        The subprocess process object

    """
    return await asyncio.create_subprocess_exec(
            "ffmpeg", "-nostdin", "-re",
            "-headers", auth_header,
            "-i", url,
            "-f", "s16le",
            "-acodec", "pcm_s16le",
            "-ac", "1",
            "-ar", str(SAMPLE_RATE),
            "-",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
        )  # fmt: skip


def _pcm_to_wav(pcm_data: bytearray, feed_name: str) -> bytes:
    """
    Convert raw PCM audio data to WAV format.

    Args:
        pcm_data: Raw PCM s16le audio bytes
        feed_name: Name of the feed (for logging context)


    Returns:
        WAV-formatted audio bytes

    """
    try:
        with io.BytesIO() as wav_io:
            with wave.open(wav_io, "wb") as f:
                f.setnchannels(NUM_CHANNELS)
                f.setsampwidth(SAMPLE_WIDTH)
                f.setframerate(SAMPLE_RATE)
                f.writeframes(pcm_data)
            wav_data = wav_io.getvalue()
    except Exception as e:
        logger.exception(f"Feed {feed_name}: Failed to format WAV data: {e}")
    return wav_data


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
    try:
        main()
    except KeyboardInterrupt:
        pass
