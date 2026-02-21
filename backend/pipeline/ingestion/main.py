import asyncio
import hashlib
import io
import logging
import os
import time
import wave
from datetime import datetime

import functions_framework
import functions_framework.aio
from google.cloud import pubsub_v1

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

USER = os.getenv("BROADCASTIFY_USERNAME")
PASS = os.getenv("BROADCASTIFY_PASSWORD")
PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT")
OUTPUT_TOPIC_ID = os.getenv("OUTPUT_TOPIC")
# keep under 30 seconds to avoid Pub/Sub message size limits
CHUNK_DURATION_SECONDS = os.getenv("CHUNK_DURATION_SECONDS", "15")
RUN_DURATION_SECONDS = int(os.getenv("RUN_DURATION_SECONDS", "60"))

SAMPLE_RATE = 16000
SAMPLE_WIDTH = 2
BYTES_PER_CHUNK = SAMPLE_RATE * int(CHUNK_DURATION_SECONDS) * SAMPLE_WIDTH


@functions_framework.aio.http
async def run_logger(req) -> tuple[str, int]:
    # Initialize the Publisher Client once (global scope)
    publisher = pubsub_v1.PublisherClient()

    # Construct the fully qualified topic path
    if PROJECT_ID and OUTPUT_TOPIC_ID:
        output_topic_path = publisher.topic_path(PROJECT_ID, OUTPUT_TOPIC_ID)
    else:
        logger.exception("PROJECT_ID or OUTPUT_TOPIC env var not set.")
        output_topic_path = None

    data = await req.json()
    feed_id = data.get("feed_id")
    start = time.perf_counter()
    try:
        logger.info(f"🚀 Starting 24/7 Fire Logger: Feed {feed_id}")
        """Single instance of the logger logic."""
        url = f"https://{USER}:{PASS}@audio.broadcastify.com/{feed_id}.mp3"

        process = await asyncio.create_subprocess_exec(
            "ffmpeg", "-re",
            "-i", url,
            "-f", "s16le",
            "-acodec", "pcm_s16le",
            "-ac", "1",
            "-ar", str(SAMPLE_RATE),
            "-",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
        )  # fmt: skip

        buffer = bytearray()
        run_loop = True
        try:
            while run_loop:
                chunk_raw = await process.stdout.read(4096)
                if not chunk_raw:
                    logger.warning("📡 Feed disconnected.")
                    break

                buffer.extend(chunk_raw)
                if len(buffer) >= BYTES_PER_CHUNK:
                    current_chunk = buffer[:BYTES_PER_CHUNK]
                    buffer = buffer[BYTES_PER_CHUNK:]

                    with io.BytesIO() as wav_io:
                        with wave.open(wav_io, "wb") as f:
                            f.setnchannels(1)
                            f.setsampwidth(SAMPLE_WIDTH)
                            f.setframerate(SAMPLE_RATE)
                            f.writeframes(current_chunk)
                        wav_data = wav_io.getvalue()

                    cur_time_iso = datetime.now().isoformat()

                    # Get the audio byte hash as a hexadecimal string
                    hex_hash = hashlib.sha256(wav_data).hexdigest()
                    logger.info(f"The SHA-256 hash is: {hex_hash}")

                    future = publisher.publish(
                        output_topic_path,
                        wav_data,  # audio bytes
                        timestamp=cur_time_iso,
                        feed_id=str(feed_id),
                    )
                    message_id = future.result(
                        timeout=30
                    )  # Block until the publish is complete

                    run_loop = (time.perf_counter() - start) < RUN_DURATION_SECONDS

                    logger.info(
                        f"Success! Published {len(wav_data)} bytes "
                        f"to {OUTPUT_TOPIC_ID} at {cur_time_iso}"
                    )
        finally:
            process.terminate()
            await process.wait()
    except Exception as e:
        logger.exception(f"\n[🔄 Error in logger loop: {e}]")

    logger.info("Waiting 5 seconds to reconnect...")
    await asyncio.sleep(5)
    return "done", 200
