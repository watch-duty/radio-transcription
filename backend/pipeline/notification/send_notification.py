import base64
import logging
import os
import urllib.parse

import functions_framework
from cloudevents.http.event import CloudEvent

from backend.pipeline.common.logging import setup_logging
from backend.pipeline.common.storage.redis_service import RedisService
from backend.pipeline.common.tracing_utils import (
    setup_tracing,
    with_tracer_context,
)
from backend.pipeline.notification.notification_deduplication import (
    NotificationDeduplication,
)
from backend.pipeline.notification.request_handler import RequestHandler
from backend.pipeline.schema_types.alert_notification_pb2 import (
    AlertNotification,
)
from backend.pipeline.schema_types.evaluated_transcribed_audio_pb2 import (
    EvaluatedTranscribedAudio,
)

# Setup Logging and Tracing
setup_logging()
setup_tracing(use_batch=False)
logger = logging.getLogger(__name__)

APP_URL = os.environ.get("APP_URL")
if APP_URL is None or not APP_URL.strip():
    msg = "APP_URL environment variable is not set or is empty."
    raise ValueError(msg)
APP_URL = APP_URL.strip()

# Keeping the notification deduplicate connection outside the main function. This is so the connection is
# maintained while the function is warm instead of reconnecting each invocation.
# TODO(schew): https://linear.app/watchduty/issue/GOO-173/update-local-dev-pipeline-with-redis
deduplication = NotificationDeduplication(RedisService())

# The request handler which can make POST requests to an endpoint.
request_handler = RequestHandler(logger)


def parse_cloud_event(
    cloud_event: CloudEvent,
) -> EvaluatedTranscribedAudio | None:
    pubsub_message = cloud_event.data.get("message", {})
    evaluated_transcribed_audio = EvaluatedTranscribedAudio()
    raw_data = pubsub_message.get("data", "")
    if raw_data:
        decoded_data = base64.b64decode(raw_data)
        evaluated_transcribed_audio.ParseFromString(decoded_data)
        return evaluated_transcribed_audio
    return None


def _build_app_url(
    evaluated_transcribed_audio: EvaluatedTranscribedAudio,
) -> str:
    query_params = {
        "feedId": evaluated_transcribed_audio.feed_id,
        "transmissionId": evaluated_transcribed_audio.transmission_id,
    }
    if evaluated_transcribed_audio.start_timestamp.seconds:
        timestamp = evaluated_transcribed_audio.start_timestamp
        query_params["timestamp"] = str(
            timestamp.seconds * 1000 + timestamp.nanos // 1_000_000
        )

    return f"{APP_URL}?{urllib.parse.urlencode(query_params)}"


def convert_to_notification(
    evaluated_transcribed_audio: EvaluatedTranscribedAudio,
) -> AlertNotification:
    app_url = _build_app_url(evaluated_transcribed_audio)
    return AlertNotification(
        feed_id=evaluated_transcribed_audio.feed_id,
        transmission_id=evaluated_transcribed_audio.transmission_id,
        source_audio_uris=evaluated_transcribed_audio.source_audio_uris,
        transcript=evaluated_transcribed_audio.transcript,
        missing_prior_context=evaluated_transcribed_audio.missing_prior_context,
        missing_post_context=evaluated_transcribed_audio.missing_post_context,
        evaluation_decisions=evaluated_transcribed_audio.evaluation_decisions,
        canonical_audio_uri=evaluated_transcribed_audio.canonical_audio_uri,
        playback_audio_uri=evaluated_transcribed_audio.playback_audio_uri,
        feed_name=evaluated_transcribed_audio.feed_name,
        app_url=app_url,
        external_id=evaluated_transcribed_audio.external_id,
        start_timestamp=evaluated_transcribed_audio.start_timestamp,
        end_timestamp=evaluated_transcribed_audio.end_timestamp,
        start_audio_offset=evaluated_transcribed_audio.start_audio_offset,
        end_audio_offset=evaluated_transcribed_audio.end_audio_offset,
    )


@functions_framework.cloud_event
def send_notification(cloud_event: CloudEvent) -> None:
    pubsub_message = cloud_event.data.get("message", {})
    attributes = pubsub_message.get("attributes", {}) or {}
    traceparent = attributes.get("traceparent", "")

    with with_tracer_context(traceparent, "send_notification", __name__):
        # Process the incoming CloudEvent message
        evaluated_transcribed_audio = parse_cloud_event(cloud_event)
        if not evaluated_transcribed_audio:
            logger.warning("Unable to parse incoming message")
            return

        # Convert the EvaluatedTranscribedAudio into an AlertNotifcation
        alert_notification = convert_to_notification(
            evaluated_transcribed_audio
        )
        notification_id = alert_notification.transmission_id
        if not deduplication.process_notification(notification_id):
            message = f"Duplicate transmission_id detected, skipping notification with ID: {notification_id}"
            logger.warning(message)
            return

        # Send a POST request to the endpoint
        try:
            request_handler.send_notification(alert_notification)
        except Exception:
            logger.exception("Failed to send notification")
