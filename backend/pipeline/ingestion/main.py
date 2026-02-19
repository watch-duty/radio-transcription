import asyncio
import io
import logging
import os
import wave
from datetime import datetime

import aiohttp
import functions_framework
import functions_framework.aio
from aiohttp import web
from google.cloud import pubsub_v1

logger = logging.getLogger(__name__)

# Initialize the Publisher Client once (global scope)
publisher = pubsub_v1.PublisherClient()

USER = os.getenv("BROADCASTIFY_USERNAME")
PASS = os.getenv("BROADCASTIFY_PASSWORD")
PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT")
OUTPUT_TOPIC_ID = os.getenv("OUTPUT_TOPIC")
# keep under 30 seconds to avoid Pub/Sub message size limits
CHUNK_DURATION_SECONDS = int(os.getenv("CHUNK_DURATION_SECONDS", "15"))
RECONNECT_DELAY = int(os.getenv("RECONNECT_DELAY_SECONDS", "5"))

SAMPLE_RATE = 16000
SAMPLE_WIDTH = 2
BYTES_PER_CHUNK = SAMPLE_RATE * CHUNK_DURATION_SECONDS * SAMPLE_WIDTH


# Construct the fully qualified topic path
if PROJECT_ID and OUTPUT_TOPIC_ID:
    output_topic_path = publisher.topic_path(PROJECT_ID, OUTPUT_TOPIC_ID)
else:
    logger.error("PROJECT_ID or OUTPUT_TOPIC env var not set.")
    output_topic_path = None


@functions_framework.aio.http
async def run_logger(req: web.Request) -> tuple[str, int]:
    data = await req.json()
    feed_id = data.get("feed_id")
    try:
        logger.info(f"🚀 Starting 24/7 Fire Logger: Feed {feed_id}")
        """Single instance of the logger logic."""
        url = f"https://{USER}:{PASS}@audio.broadcastify.com/{feed_id}.mp3"

        process = await asyncio.create_subprocess_exec(
            "ffmpeg",
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
        try:
            while True:
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

                    timestamp = datetime.now().isoformat()
                    future = publisher.publish(
                        output_topic_path,
                        wav_data,  # audio bytes
                        timestamp=timestamp,
                        feed_id=str(feed_id),
                    )
                    message_id = future.result()  # Block until the publish is complete

                    logger.info(
                        f"Success! Published enriched message {message_id} "
                        f"to {OUTPUT_TOPIC_ID} at {timestamp}"
                    )
        finally:
            process.terminate()
            await process.wait()
    except Exception as e:
        logger.exception(f"\n[🔄 Error in logger loop: {e}]")

    logger.info(f"Waiting {RECONNECT_DELAY} seconds to reconnect...")
    await asyncio.sleep(RECONNECT_DELAY)
    return "done", 200
