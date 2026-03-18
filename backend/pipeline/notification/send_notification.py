import base64
import logging
import os

import functions_framework
import google.cloud.logging
from cloudevents.http.event import CloudEvent

from backend.common.storage.redis_service import RedisService
from backend.pipeline.notification.notification_deduplication import (
    NotificationDeduplication,
)
from backend.pipeline.notification.request_handler import RequestHandler
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

# The request handler which can make POST requests to an endpoint.
request_handler = RequestHandler(logger)


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
    # Process the incoming CloudEvent message
    evaluated_transcribed_audio = parse_cloud_event(cloud_event)
    if not evaluated_transcribed_audio:
        logger.warning("Unable to parse incoming message")
        return

    # Convert the EvaluatedTranscribedAudio into an AlertNotifcation
    alert_notification = convert_to_notification(evaluated_transcribed_audio)

    # Evaluate if this message is a duplicate
    notification_id = alert_notification.transmission_id
    if deduplication.process_notification(notification_id):
        message = f"Duplicate transmission_id detected, skipping notification with ID: {notification_id}"
        logger.warning(message)
        return

    # Send a POST request to the endpoint
    request_handler.send_notification(alert_notification)
