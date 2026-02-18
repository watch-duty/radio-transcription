import asyncio
import io
import os
import wave
from datetime import datetime

from google.cloud import pubsub_v1

# Initialize the Publisher Client once (global scope)
publisher = pubsub_v1.PublisherClient()

FEED_ID = os.getenv("FEED_ID")
USER = os.getenv("BROADCASTIFY_USERNAME")
PASS = os.getenv("BROADCASTIFY_PASSWORD")
PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT")
OUTPUT_TOPIC_ID = os.getenv("OUTPUT_TOPIC")
CHUNK_DURATION_SECONDS = os.getenv("CHUNK_DURATION_SECONDS", "15")


SAMPLE_RATE = 16000
SAMPLE_WIDTH = 2
BYTES_PER_CHUNK = SAMPLE_RATE * int(CHUNK_DURATION_SECONDS) * SAMPLE_WIDTH


# Construct the fully qualified topic path
if PROJECT_ID and OUTPUT_TOPIC_ID:
    output_topic_path = publisher.topic_path(PROJECT_ID, OUTPUT_TOPIC_ID)
else:
    print("WARNING: OUTPUT_TOPIC env var not set.")
    output_topic_path = None


async def run_logger() -> None:
    print("hello there")
    while True:
        try:
            print(f"🚀 Starting 24/7 Fire Logger: Feed {FEED_ID}")
            """Single instance of the logger logic."""
            url = f"https://{USER}:{PASS}@audio.broadcastify.com/{FEED_ID}.mp3"

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
                        print("\n📡 Feed disconnected.")
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
                            feed_id=str(FEED_ID),
                        )
                        message_id = (
                            future.result()
                        )  # Block until the publish is complete

                        print(
                            f"Success! Published enriched message {message_id} "
                            f"to {OUTPUT_TOPIC_ID} at {timestamp}"
                        )
            finally:
                process.terminate()
                await process.wait()
        except Exception as e:
            print(f"\n[🔄 Error in logger loop: {e}]")

        print("Waiting 5 seconds to reconnect...")
        await asyncio.sleep(5)


if __name__ == "__main__":
    asyncio.run(run_logger())
