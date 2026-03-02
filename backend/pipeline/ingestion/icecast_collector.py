import asyncio
import io
import logging
import os
import random
import sys
import wave
from datetime import datetime

from google.cloud import storage

# Env variables
USER = os.getenv("BROADCASTIFY_USERNAME")
PASS = os.getenv("BROADCASTIFY_PASSWORD")
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")
CHUNK_DURATION_SECONDS = int(os.getenv("CHUNK_DURATION_SECONDS", "15"))
NUM_STREAMS = int(os.getenv("NUM_STREAMS", "20"))
STARTUP_THROTTLE_NUM = int(os.getenv("STARTUP_THROTTLE", "5"))

# Limit concurrent starts to avoid a CPU "thundering herd" on launch
STARTUP_THROTTLE = asyncio.Semaphore(STARTUP_THROTTLE_NUM)
SAMPLE_RATE = 16000
SAMPLE_WIDTH = 2
BYTES_PER_CHUNK = SAMPLE_RATE * CHUNK_DURATION_SECONDS * SAMPLE_WIDTH

# Limit concurrent uploads per stream to prevent unbounded task accumulation
MAX_CONCURRENT_UPLOADS = 10

# Exponential backoff configuration
BASE_BACKOFF = 5  # Initial retry delay in seconds
MAX_BACKOFF = 300  # Maximum retry delay (5 minutes)

logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

if not USER or not PASS:
    logger.critical("USERNAME and PASSWORD env vars must be set.")
    sys.exit(1)

if not GCS_BUCKET_NAME:
    logger.critical("GCS_BUCKET_NAME env var must be set.")
    sys.exit(1)

# Initialize the client once outside the function for better performance
storage_client = storage.Client()
bucket = storage_client.bucket(GCS_BUCKET_NAME)


async def monitor_stream() -> None:
    """Robust 24/7 monitor for a single ffmpeg process."""
    consecutive_failures = 0

    while True:  # Auto-restart loop
        # TODO(axian0420): https://linear.app/watchduty/issue/GOO-58/stream-normalizer
        # REPLACE WITH FEED FETCHER UTILITY LATER
        feed_id = random.randint(1000, 9999)  # noqa: S311
        url = f"https://{USER}:{PASS}@audio.broadcastify.com/{feed_id}.mp3"
        source_type = "bcfy_feeds"

        chunks_processed = 0
        upload_tasks: set[asyncio.Task] = set()
        upload_semaphore = asyncio.Semaphore(MAX_CONCURRENT_UPLOADS)

        process = await _create_ffmpeg_process(url, feed_id)

        try:
            chunks_processed = await _process_stream_chunks(
                process,
                upload_semaphore,
                upload_tasks,
                feed_id,
                source_type,
            )
            # Reset failure count after successfully processing chunks
            if chunks_processed == 1:
                consecutive_failures = 0

        finally:
            await _cleanup_stream(process, upload_tasks, feed_id)
            exit_code = process.returncode
            consecutive_failures, backoff_delay = await _handle_stream_completion(
                exit_code,
                chunks_processed,
                feed_id,
                consecutive_failures,
            )
            await asyncio.sleep(backoff_delay)


async def _create_ffmpeg_process(url: str, feed_id: int) -> asyncio.subprocess.Process:
    """
    Create and launch ffmpeg subprocess.

    Args:
        url: The stream URL to connect to
        feed_id: Feed identifier for logging

    Returns:
        The subprocess process object

    """
    async with STARTUP_THROTTLE:
        process = await asyncio.create_subprocess_exec(
            "ffmpeg", "-nostdin", "-re",
            "-i", url,
            "-f", "s16le",
            "-acodec", "pcm_s16le",
            "-ac", "1",
            "-ar", str(SAMPLE_RATE),
            "-",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
        )  # fmt: skip
        logger.info(f"Launched {feed_id} (PID: {process.pid})")
    return process


async def _process_stream_chunks(
    process: asyncio.subprocess.Process,
    upload_semaphore: asyncio.Semaphore,
    upload_tasks: set[asyncio.Task],
    feed_id: int,
    source_type: str,
) -> int:
    """
    Read from stream and process audio chunks.

    Args:
        process: The ffmpeg subprocess
        upload_semaphore: Semaphore for controlling concurrent uploads
        upload_tasks: Set to track active upload tasks
        feed_id: Feed identifier
        source_type: Source type identifier

    Returns:
        Number of chunks processed

    """
    buffer = bytearray()
    chunks_processed = 0

    while True:
        if process.stdout:
            chunk_raw = await process.stdout.read(4096)
            if not chunk_raw:
                break
        else:
            break

        buffer.extend(chunk_raw)
        if len(buffer) >= BYTES_PER_CHUNK:
            current_chunk = buffer[:BYTES_PER_CHUNK]
            del buffer[:BYTES_PER_CHUNK]

            cur_time_iso = datetime.now().isoformat().replace(":", "-")
            # Fire-and-forget upload to avoid blocking the read loop
            # This prevents ffmpeg's stdout pipe buffer from filling up
            task = asyncio.create_task(
                _upload_chunk_with_semaphore(
                    upload_semaphore,
                    current_chunk,
                    feed_id,
                    source_type,
                    cur_time_iso,
                )
            )
            upload_tasks.add(task)
            # Clean up completed tasks to prevent unbounded memory growth
            task.add_done_callback(upload_tasks.discard)

            chunks_processed += 1

    return chunks_processed


async def _upload_chunk_with_semaphore(
    semaphore: asyncio.Semaphore,
    chunk: bytearray,
    feed_id: int,
    source_type: str,
    cur_time_iso: str,
) -> None:
    """
    Upload audio chunk with semaphore-based concurrency control.
    This prevents unbounded accumulation of upload tasks.
    """
    async with semaphore:
        await asyncio.to_thread(
            process_audio_chunk,
            chunk,
            feed_id,
            source_type,
            cur_time_iso,
        )


async def _cleanup_stream(
    process: asyncio.subprocess.Process,
    upload_tasks: set[asyncio.Task],
    feed_id: int,
) -> None:
    """
    Cleanup stream resources and wait for pending uploads.

    Args:
        process: The ffmpeg subprocess to terminate
        upload_tasks: Set of pending upload tasks
        feed_id: Feed identifier for logging

    """
    # Await all pending uploads before terminating the process
    if upload_tasks:
        logger.info(
            f"Feed {feed_id}: Waiting for {len(upload_tasks)} pending uploads to complete..."
        )
        await asyncio.gather(*upload_tasks, return_exceptions=True)
    # TODO(axian0420): https://linear.app/watchduty/issue/GOO-58/stream-normalizer
    # Handle feed claim state on failure
    if process.returncode is None:
        try:
            process.terminate()
            await asyncio.wait_for(process.wait(), timeout=5)
        except TimeoutError:
            process.kill()  # Force kill if terminate fails


async def _handle_stream_completion(
    exit_code: int | None,
    chunks_processed: int,
    feed_id: int,
    consecutive_failures: int,
) -> tuple[int, float]:
    """
    Handle stream completion: update failure count, calculate backoff, and log.

    Args:
        exit_code: Process exit code
        chunks_processed: Number of chunks successfully processed
        feed_id: Feed identifier
        consecutive_failures: Current consecutive failure count

    Returns:
        Tuple of (updated_consecutive_failures, backoff_delay)

    """
    # If no chunks were processed, this is a failure
    if chunks_processed == 0:
        consecutive_failures += 1

    # Calculate exponential backoff delay
    backoff_delay = min(BASE_BACKOFF * (2**consecutive_failures), MAX_BACKOFF)

    if exit_code == 0:
        logger.info(
            f"Feed {feed_id} exited normally (exit code 0). Restarting in {backoff_delay:.1f}s..."
        )
    else:
        logger.warning(
            f"Feed {feed_id} crashed or exited with code {exit_code}. "
            f"Retry {consecutive_failures}, restarting in {backoff_delay:.1f}s..."
        )

    return consecutive_failures, backoff_delay


def process_audio_chunk(
    chunk: bytearray, feed_id: int, source_type: str, cur_time_iso: str
) -> None:
    """
    Process a single audio chunk: convert raw PCM to WAV format and upload to GCS.
        - chunk: Raw PCM audio data
        - feed_id: Identifier for the feed, used in GCS path
        - source_type: Icecast source (e.g., "bcfy_feeds")
    """
    # 1. Create the WAV in memory
    try:
        with io.BytesIO() as wav_io:
            with wave.open(wav_io, "wb") as f:
                f.setnchannels(1)
                f.setsampwidth(SAMPLE_WIDTH)
                f.setframerate(SAMPLE_RATE)
                f.writeframes(chunk)
            wav_data = wav_io.getvalue()
    except Exception as e:
        logger.exception(f"Feed {feed_id}: Failed to format WAV data: {e}")
        return

    # 2. Define the destination path
    blob_name = f"{source_type}/{feed_id}/{cur_time_iso}.wav"

    # 3. Upload to GCS
    try:
        blob = bucket.blob(blob_name)
        blob.upload_from_string(wav_data, content_type="audio/wav")
        logger.info(f"Feed {feed_id}: Uploaded {len(wav_data)} bytes to {blob_name}")
    except Exception as e:
        logger.exception(f"Feed {feed_id}: Failed to upload {blob_name} - {e}")


async def main() -> None:
    tasks = [monitor_stream() for _ in range(NUM_STREAMS)]
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
