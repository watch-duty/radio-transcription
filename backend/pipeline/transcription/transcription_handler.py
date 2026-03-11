import base64
import logging
import os

import google.cloud.logging
from cloudevents.http.event import CloudEvent
from google.cloud import pubsub_v1

from backend.pipeline.schema_types.raw_audio_chunk_pb2 import AudioChunk
from backend.pipeline.schema_types.transcribed_audio_pb2 import TranscribedAudio
from backend.pipeline.transcription.audio_fetcher import AudioFetcher, GCSAudioFetcher
from backend.pipeline.transcription.transcriber import (
    BaseTranscriber,
    GeminiTranscriber,
)

logger = logging.getLogger(__name__)

if not os.environ.get("LOCAL_DEV"):
    client = google.cloud.logging.Client()
    client.setup_logging()
else:
    # If local, just print normally to the Docker console
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler()
    logger.addHandler(handler)
    logger.info("Running in LOCAL_DEV mode. Logs will print here.")


def handle_transcription_event(
    cloud_event: CloudEvent,
    transcriber: BaseTranscriber | None = None,
    audio_fetcher: AudioFetcher | None = None,
) -> None:
    """
    Orchestrates audio transcription and publishing.

    Args:
        cloud_event: The Pub/Sub event wrapper.
        transcriber: An optional BaseTranscriber implementation. Defaults to GeminiTranscriber.
        audio_fetcher: An optional AudioFetcher implementation. Defaults to GCSAudioFetcher.

    """
    project_id = os.environ.get("GOOGLE_CLOUD_PROJECT")
    output_topic_id = os.environ.get("OUTPUT_TOPIC")
    api_key = os.getenv("GEMINI_API_KEY")

    # 1. Get the message payload from cloud_event.data
    message = cloud_event.data.get("message", {})

    # 2. Access custom attributes from the message dictionary
    attributes = message.get("attributes", {})
    feed_id = attributes.get("feed_id")
    timestamp = attributes.get("timestamp")

    # 3. Decode the base64 data (wav_data)
    pubsub_data = message.get("data")
    if not pubsub_data:
        logger.info("No data in pubsub message, skipping")
        return

    decoded_data = base64.b64decode(pubsub_data)

    audio_chunk = AudioChunk()
    audio_chunk.ParseFromString(decoded_data)
    gcs_file_path = audio_chunk.gcs_file_path

    if audio_fetcher is None:
        audio_fetcher = GCSAudioFetcher()

    wav_data = audio_fetcher.fetch(gcs_file_path)
    logger.info("processing wav data for feed: %s at: %s", feed_id, timestamp)

    # 4. Transcribe using provided or default transcriber
    if transcriber is None:
        transcriber = GeminiTranscriber(api_key=api_key)

    transcript = transcriber.transcribe(wav_data)

    if not transcript:
        logger.info("No transcript in audio, skipping message")
        return

    logger.info("Success! translated bytes to transcription %s", transcript)

    # 5. Build and publish TranscribedAudio message
    transcribed_payload = TranscribedAudio(transcript=transcript, feed_id=feed_id)

    publisher = pubsub_v1.PublisherClient()
    if project_id and output_topic_id:
        output_topic_path = publisher.topic_path(project_id, output_topic_id)
    else:
        logger.warning(
            "OUTPUT_TOPIC or PROJECT_ID env var not set: project_id=%s, output_topic_id=%s",
            project_id,
            output_topic_id,
        )
        output_topic_path = None

    if output_topic_path:
        encoded_data = transcribed_payload.SerializeToString()
        try:
            future = publisher.publish(output_topic_path, encoded_data)
            message_id = future.result()  # Block until published (ensure reliability)

            logger.info(
                "Success! Published enriched message %s to %s",
                message_id,
                output_topic_id,
            )
        except Exception:
            logger.exception("Error publishing evaluated message")
    else:
        logger.warning("Skipping publish: Output topic not configured.")
