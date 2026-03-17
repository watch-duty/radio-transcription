"""Publishes test messages to the Transcription topic for Rules Evaluation."""

import base64
import logging
import os

import requests
from google.protobuf import text_format

from backend.pipeline.schema_types.transcribed_audio_pb2 import TranscribedAudio

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# For testing payloads sent to the Pub/Sub between the Transcription and Rules
# Evaluation services.
SAMPLE_TRANSCRIPTION_MESSAGE_WITH_EVENT = TranscribedAudio(
    transmission_id="test-trans-123",
    transcript="evacuation needed immediately",
    source_chunk_ids=["chunk1", "chunk2"],
    feed_id="test-feed",
    start_timestamp={"seconds": 0, "nanos": 0},
    end_timestamp={"seconds": 10, "nanos": 0},
)

SAMPLE_TRANSCRIPTION_MESSAGE_NO_EVENT = TranscribedAudio(
    transmission_id="test-trans-123",
    transcript="parade at 123 main street",
    source_chunk_ids=["chunk1", "chunk2"],
    feed_id="test-feed",
    start_timestamp={"seconds": 0, "nanos": 0},
    end_timestamp={"seconds": 10, "nanos": 0},
)


def publish_test_message() -> None:
    """Publishes a test message to the transcription topic."""
    # Update this sample to test out different messages
    test_message = SAMPLE_TRANSCRIPTION_MESSAGE_WITH_EVENT

    payload = {
        "messages": [
            {
                "data": base64.b64encode(
                    test_message.SerializeToString()
                ).decode("utf-8")
            }
        ]
    }

    pubsub_emulator_host = os.environ["PUBSUB_EMULATOR_HOST"]
    project_id = os.environ["GOOGLE_CLOUD_PROJECT"]
    input_topic = os.environ["TRANSCRIPTION_TOPIC"]

    url = (
        f"http://{pubsub_emulator_host}/v1/projects/"
        f"{project_id}/topics/{input_topic}:publish"
    )

    logger.info("====== INPUT ======")
    logger.info(
        "Publishing to %s:\n %s",
        url,
        text_format.MessageToString(test_message),
    )
    response = requests.post(url, json=payload)
    logger.info("====== OUTPUT ======")
    logger.info("Status: %s", response.status_code)
    logger.info("Response: %s", response.text)


if __name__ == "__main__":
    publish_test_message()
