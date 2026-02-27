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
USER = os.getenv("USERNAME")
PASS = os.getenv("PASSWORD")
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME", "wd-radio-test")
CHUNK_DURATION_SECONDS = int(os.getenv("CHUNK_DURATION_SECONDS", "15"))
NUM_STREAMS = int(os.getenv("NUM_STREAMS", "20"))

# Limit concurrent starts to avoid a CPU "thundering herd" on launch
STARTUP_THROTTLE = asyncio.Semaphore(20)
SAMPLE_RATE = 16000
SAMPLE_WIDTH = 2
BYTES_PER_CHUNK = SAMPLE_RATE * CHUNK_DURATION_SECONDS * SAMPLE_WIDTH

logger = logging.getLogger(__name__)

if not USER or not PASS:
    logger.critical("USERNAME and PASSWORD env vars must be set.")
    sys.exit(1)

# Initialize the client once outside the function for better performance
storage_client = storage.Client()
bucket = storage_client.bucket(GCS_BUCKET_NAME)


async def monitor_stream() -> None:
    """Robust 24/7 monitor for a single ffmpeg process."""
    while True:  # Auto-restart loop
        # TODO(axian0420): https://linear.app/watchduty/issue/GOO-58/stream-normalizer
        # REPLACE WITH FEED FETCHER UTILITY LATER
        feed_id = random.randint(1000, 9999)  # noqa: S311
        url = f"https://{USER}:{PASS}@audio.broadcastify.com/{feed_id}.mp3"
        source_type = "bcfy_feeds"

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

            buffer = bytearray()
            try:
                while True:
                    chunk_raw = await process.stdout.read(4096)
                    if not chunk_raw:
                        break

                    buffer.extend(chunk_raw)
                    if len(buffer) >= BYTES_PER_CHUNK:
                        current_chunk = buffer[:BYTES_PER_CHUNK]
                        del buffer[:BYTES_PER_CHUNK]
                        # Offload CPU-bound WAVE formatting to a thread to keep async loop snappy
                        await asyncio.to_thread(
                            process_audio_chunk, current_chunk, feed_id, source_type
                        )

            finally:
                # TODO(axian0420): https://linear.app/watchduty/issue/GOO-58/stream-normalizer
                # Handle feed claim state on failure
                if process.returncode is None:
                    try:
                        process.terminate()
                        await asyncio.wait_for(process.wait(), timeout=5)
                    except TimeoutError:
                        process.kill()  # Force kill if terminate fails
                logger.warning(f"Feed {feed_id} crashed. Restarting in 5s...")
                await asyncio.sleep(5)


def process_audio_chunk(chunk: bytearray, feed_id: int, source_type: str) -> None:
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
    cur_time_iso = datetime.now().isoformat().replace(":", "-")
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
