import base64
import logging
import os

import functions_framework
import google.cloud.logging
import requests
from cloudevents.http.event import CloudEvent
from google.protobuf.json_format import MessageToJson

from backend.common.storage.redis_service import RedisService
from backend.pipeline.notification.notification_deduplication import (
    NotificationDeduplication,
)
from backend.pipeline.schema_types.alert_notification_pb2 import AlertNotification
from backend.pipeline.schema_types.evaluated_transcribed_audio_pb2 import (
    EvaluatedTranscribedAudio,
)

# TODO(schew): https://linear.app/watchduty/issue/GOO-100/create-shared-logging-util-for-consistent-setup-across-pipeline
if not os.environ.get("LOCAL_DEV"):
    client = google.cloud.logging.Client()
    client.setup_logging()
    logger = logging.getLogger(__name__)
else:
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler()
    logger.addHandler(handler)
    logger.info(
        "Running in LOCAL_DEV mode. Logs will print to console instead of configurable endpoint."
    )

POST_TIMEOUT_SECONDS = 5

NOTIFICATION_ENDPOINT = os.environ.get("NOTIFICATION_ENDPOINT")
NOTIFICATION_ENDPOINT_API_KEY = os.environ.get("NOTIFICATION_ENDPOINT_API_KEY")

# Keeping the notification deduplicate connection outside the main function. This is so the connection is
# maintained while the function is warm instead of reconnecting each invocation.
deduplication = NotificationDeduplication(RedisService())


def parse_cloud_event(cloud_event: CloudEvent) -> EvaluatedTranscribedAudio | None:
    pubsub_message = cloud_event.data.get("message", {})
    evaluated_transcribed_audio = EvaluatedTranscribedAudio()
    raw_data = pubsub_message.get("data", "")
    if raw_data:
        decoded_data = base64.b64decode(raw_data)
        evaluated_transcribed_audio.ParseFromString(decoded_data)
        return evaluated_transcribed_audio
    logger.warning("No data provided in CloudEvent")
    return None


def convert_to_notification(
    evaluated_transcribed_audio: EvaluatedTranscribedAudio,
) -> AlertNotification:
    notification = AlertNotification(
        feed_id=evaluated_transcribed_audio.feed_id,
        transmission_id=evaluated_transcribed_audio.transmission_id,
        source_chunk_ids=evaluated_transcribed_audio.source_chunk_ids,
        transcript=evaluated_transcribed_audio.transcript,
        evaluation_decisions=evaluated_transcribed_audio.evaluation_decisions,
    )
    if evaluated_transcribed_audio.start_timestamp.seconds:
        notification.start_timestamp.CopyFrom(
            evaluated_transcribed_audio.start_timestamp
        )
    if evaluated_transcribed_audio.end_timestamp.seconds:
        notification.end_timestamp.CopyFrom(evaluated_transcribed_audio.end_timestamp)
    return notification


@functions_framework.cloud_event
def send_notification(cloud_event: CloudEvent) -> None:
    try:
        evaluated_transcribed_audio = parse_cloud_event(cloud_event)
        if not evaluated_transcribed_audio:
            return

        alert_notification = convert_to_notification(evaluated_transcribed_audio)
        request_data = MessageToJson(alert_notification, indent=None)
        logger.info(f"Sending payload: {request_data}")

        notification_id = alert_notification.transmission_id
        if deduplication.process_notification(notification_id):
            message = f"Duplicate transmission_id detected, skipping notification with ID: {notification_id}"
            logger.warning(message)
            return

        # Send POST request to endpoint.
        response = requests.post(
            NOTIFICATION_ENDPOINT,
            data=request_data,
            headers={
                "Content-Type": "application/json",
                "X-Api-Key": NOTIFICATION_ENDPOINT_API_KEY,
            },
            timeout=POST_TIMEOUT_SECONDS,
        )

        # Raise an exception for bad status codes
        response.raise_for_status()

        logger.info(f"Message sent successfully! Status code: {response.status_code}")

    except requests.exceptions.RequestException as e:
        logger.exception(f"POST request failed: {e}")
        raise
