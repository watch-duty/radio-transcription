import base64
import logging
import os

import functions_framework
import google.cloud.logging
import requests
from cloudevents.http.event import CloudEvent
from google.protobuf.json_format import MessageToJson

from backend.pipeline.common.storage.redis_service import RedisService
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

# Keeping the notification deduplicate connection outside the main function. This is so the connection is
# maintained while the function is warm instead of reconnecting each invocation.
# TODO(schew): https://linear.app/watchduty/issue/GOO-173/update-local-dev-pipeline-with-redis
deduplication = NotificationDeduplication(RedisService())

POST_TIMEOUT_SECONDS = int(os.environ.get("POST_TIMEOUT_SECONDS", "5"))
NOTIFICATION_ENDPOINT = os.environ.get("NOTIFICATION_ENDPOINT")
NOTIFICATION_ENDPOINT_API_KEY = os.environ.get("NOTIFICATION_ENDPOINT_API_KEY")


def parse_cloud_event(cloud_event: CloudEvent) -> EvaluatedTranscribedAudio | None:
    pubsub_message = cloud_event.data.get("message", {})
    evaluated_transcribed_audio = EvaluatedTranscribedAudio()
    raw_data = pubsub_message.get("data", "")
    if raw_data:
        decoded_data = base64.b64decode(raw_data)
        evaluated_transcribed_audio.ParseFromString(decoded_data)
        return evaluated_transcribed_audio
    return None


def convert_to_notification(
    evaluated_transcribed_audio: EvaluatedTranscribedAudio,
) -> AlertNotification:
    notification = AlertNotification(
        feed_id=evaluated_transcribed_audio.feed_id,
        transmission_id=evaluated_transcribed_audio.transmission_id,
        source_audio_uris=evaluated_transcribed_audio.source_audio_uris,
        transcript=evaluated_transcribed_audio.transcript,
        evaluation_decisions=evaluated_transcribed_audio.evaluation_decisions,
    )
    if evaluated_transcribed_audio.start_timestamp.seconds:
        notification.start_timestamp.CopyFrom(
            evaluated_transcribed_audio.start_timestamp
        )
    if evaluated_transcribed_audio.end_timestamp.seconds:
        notification.end_timestamp.CopyFrom(evaluated_transcribed_audio.end_timestamp)
    if (
        evaluated_transcribed_audio.start_audio_offset.seconds
        or evaluated_transcribed_audio.start_audio_offset.nanos
    ):
        notification.start_audio_offset.CopyFrom(
            evaluated_transcribed_audio.start_audio_offset
        )
    if (
        evaluated_transcribed_audio.end_audio_offset.seconds
        or evaluated_transcribed_audio.end_audio_offset.nanos
    ):
        notification.end_audio_offset.CopyFrom(
            evaluated_transcribed_audio.end_audio_offset
        )
    return notification


@functions_framework.cloud_event
def send_notification(cloud_event: CloudEvent) -> None:
    # Process the incoming CloudEvent message
    evaluated_transcribed_audio = parse_cloud_event(cloud_event)
    if not evaluated_transcribed_audio:
        logger.warning("Unable to parse incoming message")
        return

    # Convert the EvaluatedTranscribedAudio into an AlertNotifcation
    alert_notification = convert_to_notification(evaluated_transcribed_audio)
    notification_id = alert_notification.transmission_id
    if not deduplication.process_notification(notification_id):
        message = f"Duplicate transmission_id detected, skipping notification with ID: {notification_id}"
        logger.warning(message)
        return

    if not NOTIFICATION_ENDPOINT:
        logger.warning("NOTIFICATION_ENDPOINT is not set, skipping notification")
        return

    # Send a POST request to the endpoint
    headers = {"Content-Type": "application/json"}
    if NOTIFICATION_ENDPOINT_API_KEY:
        headers["X-Api-Key"] = NOTIFICATION_ENDPOINT_API_KEY
    request_data = MessageToJson(alert_notification)

    try:
        response = requests.post(
            NOTIFICATION_ENDPOINT,
            data=request_data,
            headers=headers,
            timeout=POST_TIMEOUT_SECONDS,
        )
        response.raise_for_status()
        logger.info(f"Successfully sent notification: {response.status_code}")
    except requests.exceptions.RequestException:
        logger.exception("Failed to send notification")
