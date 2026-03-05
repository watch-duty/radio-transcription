import base64
import logging
import os

import functions_framework
import google.cloud.logging
from cloudevents.http.event import CloudEvent
from google.cloud import pubsub_v1

from backend.pipeline.evaluation.rules_evaluation.evaluator import StaticTextEvaluator
from backend.pipeline.schema_types.evaluated_transcribed_audio_pb2 import (
    EvaluatedTranscribedAudio,
)
from backend.pipeline.schema_types.transcribed_audio_pb2 import TranscribedAudio

# Initialize the Publisher Client once (global scope) for performance
publisher = pubsub_v1.PublisherClient()
PROJECT_ID = os.environ.get("GOOGLE_CLOUD_PROJECT")
OUTPUT_TOPIC_ID = os.environ.get("OUTPUT_TOPIC")
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

if PROJECT_ID and OUTPUT_TOPIC_ID:
    output_topic_path = publisher.topic_path(PROJECT_ID, OUTPUT_TOPIC_ID)
else:
    logger.warning("OUTPUT_TOPIC or PROJECT_ID env var not set.")
    output_topic_path = None


def parse_cloud_event(cloud_event: CloudEvent) -> TranscribedAudio | None:
    pubsub_message = cloud_event.data.get("message", {})
    transcribed_audio = TranscribedAudio()
    raw_data = pubsub_message.get("data", "")
    if not raw_data:
        logger.warning("No data provided in CloudEvent")
        return None
    decoded_data = base64.b64decode(raw_data)
    transcribed_audio.ParseFromString(decoded_data)
    return transcribed_audio


def publish_evaluation_result(evaluated_payload: EvaluatedTranscribedAudio) -> None:
    if output_topic_path:
        encoded_data = evaluated_payload.SerializeToString()
        future = publisher.publish(output_topic_path, encoded_data)
        message_id = future.result()

        logger.info(
            "Success! Published enriched message %s to %s",
            message_id,
            OUTPUT_TOPIC_ID,
        )
    else:
        logger.warning("Skipping publish: Output topic not configured.")


@functions_framework.cloud_event
def evaluate_transcribed_audio_segment(cloud_event: CloudEvent) -> None:
    """
    Triggered from a message on a Cloud Pub/Sub topic.
    """
    try:
        # 1. Decode the Incoming Message
        new_transcribed_audio = parse_cloud_event(cloud_event)
        if new_transcribed_audio is None:
            logger.warning(
                "Transcribed audio could not be parsed. Skipping evaluation."
            )
            return

        audio_id = new_transcribed_audio.audio_id
        logger.info("Processing audio ID: %s", audio_id)
        if not new_transcribed_audio.transcript.strip():
            logger.info(
                "No transcript for audio ID: %s. Skipping evaluation.", audio_id
            )
            return

        # 3. Call the evaluator
        evaluation_result = StaticTextEvaluator.evaluate(
            new_transcribed_audio.transcript
        )
        logger.info(
            "Decision for ID: %s is: %s", audio_id, evaluation_result.get("is_flagged")
        )
        # 3a. If not flagged, we can skip publishing to the downstream topic
        if not evaluation_result.get("is_flagged"):
            logger.info("No rules triggered for ID: %s. Skipping publish.", audio_id)
            return
        # 4. Create Evaluation Result Payload
        evaluated_payload = EvaluatedTranscribedAudio(
            file_path=new_transcribed_audio.file_path,
            source=new_transcribed_audio.source,
            feed_id=new_transcribed_audio.feed_id,
            audio_id=new_transcribed_audio.audio_id,
            start_timestamp={
                "seconds": new_transcribed_audio.start_timestamp.seconds,
                "nanos": new_transcribed_audio.start_timestamp.nanos,
            },
            end_timestamp={
                "seconds": new_transcribed_audio.end_timestamp.seconds,
                "nanos": new_transcribed_audio.end_timestamp.nanos,
            },
            transcript=new_transcribed_audio.transcript,
            context=new_transcribed_audio.context,
            evaluation_decisions=evaluation_result.get("triggered_rules", []),
        )

        # 5. Publish to Downstream Topic
        publish_evaluation_result(evaluated_payload)

    except Exception:
        logger.exception("Error processing new audio message")
        raise
