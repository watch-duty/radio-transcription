import base64
import json
import logging
import os

import functions_framework
from cloudevents.http.event import CloudEvent
from google.cloud import pubsub_v1

from backend.pipeline.evaluation.rules_evaluation import evaluator

# Initialize the Publisher Client once (global scope) for performance
publisher = pubsub_v1.PublisherClient()
PROJECT_ID = os.environ.get("GOOGLE_CLOUD_PROJECT")
OUTPUT_TOPIC_ID = os.environ.get("OUTPUT_TOPIC")
logger = logging.getLogger(__name__)

# Construct the fully qualified topic path
# output_topic_path will look like: "projects/my-project/topics/processed-transcriptions"
if PROJECT_ID and OUTPUT_TOPIC_ID:
    output_topic_path = publisher.topic_path(PROJECT_ID, OUTPUT_TOPIC_ID)
else:
    logger.warning("OUTPUT_TOPIC or PROJECT_ID env var not set.")
    output_topic_path = None

@functions_framework.cloud_event
def evaluate_transcribed_audio_segment(cloud_event: CloudEvent) -> None:
    """
    Triggered from a message on a Cloud Pub/Sub topic.
    """
    try:
        # 1. Decode the Incoming Message
        pubsub_message = cloud_event.data.get("message", {})
        data_str = base64.b64decode(pubsub_message.get("data")).decode("utf-8")
        payload = json.loads(data_str)
        payload_id = payload.get("id", "unknown")
        logger.info("Processing ID: %s", payload_id)

        # 2. Extract Transcription
        text_to_analyze = payload.get("transcript", "")

        # 3. Call the Logic Package
        # This is where the magic happens
        decision = evaluator.evaluate_text(text_to_analyze)
        logger.info("Decision for ID: %s is: %s", payload_id, decision)

        # 4. Enrich the Payload
        # We add the decision results to the original message
        payload["analysis"] = decision
        payload["processed_at"] = cloud_event.data.get("publish_time")

        # 5. Publish to Downstream Topic
        if output_topic_path:
            # Pub/Sub requires data to be a bytestring
            new_data_str = json.dumps(payload)
            new_data_bytes = new_data_str.encode("utf-8")

            future = publisher.publish(output_topic_path, new_data_bytes)
            message_id = future.result() # Block until published (ensure reliability)

            logger.info("Success! Published enriched message %s to $%s", message_id, {OUTPUT_TOPIC_ID})
        else:
            logger.warning("Skipping publish: Output topic not configured.")

    except Exception:
        logger.exception("Error processing new audio message")
        raise
