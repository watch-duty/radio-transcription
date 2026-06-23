# Notification pipeline sender entrypoint
import base64
import logging
import os
import urllib.parse

import functions_framework
from cloudevents.http.event import CloudEvent

from backend.pipeline.common import env
from backend.pipeline.common.clients.feeds_client import FeedsClient
from backend.pipeline.common.constants import MS_PER_SECOND, NANOS_PER_MS
from backend.pipeline.common.container_helper import ForkDetector, fork_checked
from backend.pipeline.common.exceptions import NonRetryableError
from backend.pipeline.common.log_helper import (
    record_pipeline_stage,
    setup_logging,
)
from backend.pipeline.common.storage.redis_service import RedisService
from backend.pipeline.common.tracing_utils import (
    parse_pubsub_cloudevent,
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
from backend.services.feeds.models import Tag

# Setup Logging
setup_logging()
logger = logging.getLogger(__name__)


class NotificationServiceContainer:
    """Encapsulates the warm-started cached service container instances for GCF."""

    def __init__(self) -> None:
        self._fork_detector = ForkDetector(self.reset_clients)
        self._deduplication: NotificationDeduplication | None = None
        self._request_handler: RequestHandler | None = None
        self._feeds_client: FeedsClient | None = None

    def reset_clients(self) -> None:
        if self._deduplication is not None:
            try:
                redis_service = getattr(self._deduplication, "cache", None)
                if redis_service is not None:
                    client = getattr(redis_service, "client", None)
                    if client is not None:
                        client.close()
            except Exception:
                logger.exception("Failed to close Redis client on fork reset")
        self._deduplication = None
        self._request_handler = None
        self._feeds_client = None

    @fork_checked
    def get_deduplication(self) -> NotificationDeduplication:
        """Warms up and caches the NotificationDeduplication instance."""
        if self._deduplication is None:
            logger.info("Initializing NotificationDeduplication")
            self._deduplication = NotificationDeduplication(RedisService())
        return self._deduplication

    @fork_checked
    def get_request_handler(self) -> RequestHandler:
        """Warms up and caches the RequestHandler instance."""
        if self._request_handler is None:
            logger.info("Initializing RequestHandler")
            self._request_handler = RequestHandler(logger)
        return self._request_handler

    @property
    def app_url(self) -> str:
        app_url = os.environ.get("APP_URL")
        if not app_url or not app_url.strip():
            msg = "APP_URL environment variable is not set or is empty."
            logger.error(msg)
            raise ValueError(msg)
        return app_url.strip()

    @property
    def feeds_api_url(self) -> str:
        feeds_api_url = os.environ.get("FEEDS_API_URL", "")
        if not feeds_api_url or not feeds_api_url.strip():
            msg = "FEEDS_API_URL environment variable is not set or is empty."
            logger.error(msg)
            raise ValueError(msg)
        return feeds_api_url.strip()

    @fork_checked
    def get_feeds_client(self) -> FeedsClient:
        """Warms up and caches the FeedsClient instance."""
        if self._feeds_client is None:
            logger.info("Initializing FeedsClient")
            self._feeds_client = FeedsClient(self.feeds_api_url)
        return self._feeds_client

    def eager_warmup(self) -> None:
        """Eagerly warms up and caches all clients during container initialization."""
        logger.info("Performing eager warm-start for container services...")
        try:
            self.get_deduplication()
            self.get_request_handler()
            self.get_feeds_client()
            _ = self.app_url
            _ = self.feeds_api_url
            logger.info("Container services eagerly warmed up successfully.")
        except Exception as e:
            if env.is_gcp_env() and isinstance(e, ValueError):
                logger.exception(
                    "Eager warm-up failed with ValueError in GCP environment, bubbling up error."
                )
                raise
            logger.warning(
                "Eager warm-start skipped or failed (expected in some test/local envs): %s",
                e,
            )


# Global container instance managed by the GCF container instance lifecycle
container = NotificationServiceContainer()
container.eager_warmup()


def parse_cloud_event(
    raw_data: str,
) -> EvaluatedTranscribedAudio | None:
    if raw_data:
        evaluated_transcribed_audio = EvaluatedTranscribedAudio()
        decoded_data = base64.b64decode(raw_data)
        evaluated_transcribed_audio.ParseFromString(decoded_data)
        return evaluated_transcribed_audio
    return None


def _build_app_url(
    evaluated_transcribed_audio: EvaluatedTranscribedAudio,
    app_url: str,
) -> str:
    query_params = {
        "feedId": evaluated_transcribed_audio.feed_id,
        "segmentId": evaluated_transcribed_audio.segment_id,
    }
    if evaluated_transcribed_audio.start_timestamp.seconds:
        timestamp = evaluated_transcribed_audio.start_timestamp
        query_params["timestamp"] = str(
            timestamp.seconds * MS_PER_SECOND + timestamp.nanos // NANOS_PER_MS
        )

    return f"{app_url}/transcripts?{urllib.parse.urlencode(query_params)}"


def convert_to_notification(
    evaluated_transcribed_audio: EvaluatedTranscribedAudio,
    tags: list[Tag] | None,
    app_url: str,
) -> AlertNotification:
    url = _build_app_url(evaluated_transcribed_audio, app_url)
    notification = AlertNotification(
        feed_id=evaluated_transcribed_audio.feed_id,
        segment_id=evaluated_transcribed_audio.segment_id,
        source_audio_uris=evaluated_transcribed_audio.source_audio_uris,
        transcript=evaluated_transcribed_audio.transcript,
        missing_prior_context=evaluated_transcribed_audio.missing_prior_context,
        missing_post_context=evaluated_transcribed_audio.missing_post_context,
        evaluation_decisions=evaluated_transcribed_audio.evaluation_decisions,
        evaluation_errors=evaluated_transcribed_audio.errors,
        canonical_audio_uri=evaluated_transcribed_audio.canonical_audio_uri,
        playback_audio_uri=evaluated_transcribed_audio.playback_audio_uri,
        feed_name=evaluated_transcribed_audio.feed_name,
        app_url=url,
        start_timestamp=evaluated_transcribed_audio.start_timestamp,
        end_timestamp=evaluated_transcribed_audio.end_timestamp,
        start_audio_offset=evaluated_transcribed_audio.start_audio_offset,
        end_audio_offset=evaluated_transcribed_audio.end_audio_offset,
    )

    if tags:
        for tag in tags:
            t = notification.tags.add()
            t.key = tag.key
            t.value = tag.value

    return notification


@functions_framework.cloud_event
def send_notification(cloud_event: CloudEvent) -> None:
    setup_tracing(service_name="notification-service", use_batch=False)
    try:
        combined_attributes, raw_data = parse_pubsub_cloudevent(cloud_event)
    except Exception as e:
        logger.exception("Failed to parse CloudEvent payload envelope: %s", e)
        return

    with with_tracer_context(
        combined_attributes, "send_notification", __name__
    ):
        record_pipeline_stage("notification", "start")
        # Process the incoming CloudEvent message
        evaluated_transcribed_audio = parse_cloud_event(raw_data)
        if not evaluated_transcribed_audio:
            logger.warning("Unable to parse incoming message")
            return

        notification_id = evaluated_transcribed_audio.segment_id
        deduplication = container.get_deduplication()
        if not deduplication.process_notification(notification_id):
            message = f"Duplicate segment_id detected, skipping notification with ID: {notification_id}"
            logger.info(message)
            record_pipeline_stage("notification", "skipped")
            return

        try:
            # Fetch tags from feeds API
            feeds_client = container.get_feeds_client()
            tags = feeds_client.get_feed_tags(
                evaluated_transcribed_audio.feed_id
            )

            # Convert the EvaluatedTranscribedAudio into an AlertNotifcation
            alert_notification = convert_to_notification(
                evaluated_transcribed_audio,
                tags,
                container.app_url,
            )

            # Send a POST request to the endpoint
            try:
                request_handler = container.get_request_handler()
                request_handler.send_notification(alert_notification)
                record_pipeline_stage("notification", "success")
            except NonRetryableError:
                record_pipeline_stage("notification", "error")
                logger.exception(
                    "Failed to send notification for audio segment (segment_id: %s) due to client (4xx) error. Message will not be retried.",
                    notification_id,
                )
        except Exception:
            record_pipeline_stage("notification", "error")
            try:
                deduplication.clear_notification(notification_id)
            except Exception as e:
                logger.exception(
                    "Failed to clear deduplication key for segment_id: %s: %s",
                    notification_id,
                    e,
                )
            raise
