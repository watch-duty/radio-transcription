from __future__ import annotations

import asyncio
import base64
import logging
import os
import struct
import sys
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
READ_TIMEOUT_SEC = 30

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

    Integration Contract:
    Should check shutdown.is_set() between I/O operations for prompt
    SIGTERM response. During graceful shutdown, the runtime gives tasks
    a 5-second grace period to notice shutdown.is_set() and exit
    cleanly before forcibly cancelling stragglers. Checking the event
    lets the generator close network connections gracefully via the
    normal cleanup path rather than being interrupted mid-stream.

    IMPORTANT: Implement read timeouts on all network I/O (e.g. timeout
    for process.stdout.read). The runtime enforces a max silence timeout
    as defense-in-depth, but normalizers should detect dead connections
    quickly via their own read timeouts and not block the event loop.

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
    feed_id = feed.get("id")
    feed_name = feed.get("name")
    if not url:
        msg = f"Feed {feed_id} ({feed_name}) missing stream_url in feed_properties_icecast"
        raise ValueError(msg)

    # Launch ffmpeg subprocess
    process = await _create_ffmpeg_process(url)
    logger.info(f"Feed {feed_id} ({feed_name}): Started ffmpeg (PID: {process.pid})")

    buffer = bytearray()

    try:
        while True:
            # Check for shutdown signal
            if shutdown_event.is_set():
                logger.info(
                    f"Feed {feed_id} ({feed_name}): Shutdown requested, stopping capture"
                )
                return

            # Read from ffmpeg stdout
            if process.stdout:
                chunk_raw = await asyncio.wait_for(
                    process.stdout.read(4096),
                    timeout=READ_TIMEOUT_SEC,
                )
                if not chunk_raw:
                    # ffmpeg exited
                    exit_code = await process.wait()
                    if exit_code != 0:
                        msg = f"Feed {feed_id} ({feed_name}): ffmpeg exited with code {exit_code}"
                        raise RuntimeError(msg)
                    logger.info(f"Feed {feed_id} ({feed_name}): ffmpeg exited normally")
                    return
            else:
                msg = f"Feed {feed_id} ({feed_name}): ffmpeg stdout is None"
                raise RuntimeError(msg)

            buffer.extend(chunk_raw)

            # Process complete chunks
            while len(buffer) >= BYTES_PER_CHUNK:
                current_chunk = buffer[:BYTES_PER_CHUNK]
                del buffer[:BYTES_PER_CHUNK]

                # Prepend WAV header to raw PCM data
                wav_header = _get_wav_header(
                    len(current_chunk), SAMPLE_RATE, NUM_CHANNELS, SAMPLE_WIDTH
                )
                wav_data = wav_header + current_chunk
                yield wav_data

    finally:
        # Cleanup: terminate ffmpeg if still running
        if process.returncode is None:
            try:
                process.terminate()
                await asyncio.wait_for(process.wait(), timeout=5)
            except TimeoutError:
                process.kill()
                logger.warning(
                    f"Feed {feed_id} ({feed_name}): Force-killed ffmpeg process"
                )
                await process.wait()
            except Exception as e:
                logger.exception(
                    f"Feed {feed_id} ({feed_name}): Error terminating ffmpeg: {e}"
                )


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


def _get_wav_header(
    pcm_length: int, sample_rate: int, num_channels: int, sample_width: int
) -> bytes:
    """
    Generates a canonical 44-byte WAV (RIFF) header for PCM data.

    Args:
        pcm_length: Total bytes of raw PCM data (data chunk size).
        sample_rate: e.g., 16000, 44100.
        num_channels: 1 for mono, 2 for stereo.
        sample_width: Bytes per sample (e.g., 2 for 16-bit).

    Returns:
        A bytes object containing the WAV header to be prepended to PCM data.

    """
    header = bytearray(44)

    # --- RIFF Chunk Descriptor ---
    header[0:4] = b"RIFF"
    # File size - 8 bytes (the 'RIFF' and 'size' fields themselves aren't counted)
    struct.pack_into("<I", header, 4, 36 + pcm_length)
    header[8:12] = b"WAVE"

    # --- The "fmt " (format) Sub-chunk ---
    header[12:16] = b"fmt "
    struct.pack_into(
        "<IHHIIHH",
        header,  # Buffer to write into
        16,  # Offset to start writing
        16,  # Subchunk1Size (16 for PCM)
        1,  # AudioFormat (1 = PCM / Linear Quantization)
        num_channels,
        sample_rate,
        sample_rate * num_channels * sample_width,  # ByteRate
        num_channels * sample_width,  # BlockAlign
        sample_width * 8,  # BitsPerSample (e.g., 16)
    )

    # --- The "data" Sub-chunk ---
    header[36:40] = b"data"
    # Size of the actual raw PCM data following this header
    struct.pack_into("<I", header, 40, pcm_length)

    return bytes(header)


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
