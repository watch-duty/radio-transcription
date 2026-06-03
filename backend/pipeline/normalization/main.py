"""Serverless Cloud Run / Cloud Function normalization entry point.

Triggered by Pub/Sub push events containing serialized SegmentedAudio
claim-check metadata. Delegates processing to NormalizationEventProcessor.
"""

import logging
import os

import functions_framework
from cloudevents.http.event import CloudEvent
from google.cloud import pubsub_v1, storage

from backend.pipeline.common.clients.audio_segments_client import AudioSegmentsClient
from backend.pipeline.common.log_helper import setup_logging
from backend.pipeline.common.tracing_utils import setup_tracing
from backend.pipeline.normalization.processor import NormalizationEventProcessor

# Setup Logging and Tracing
setup_logging()
setup_tracing(use_batch=False)
logger = logging.getLogger(__name__)


class NormalizationServiceContainer:
    """Encapsulates the warm-started cached service container instances for GCF."""

    def __init__(self) -> None:
        self._processor: NormalizationEventProcessor | None = None
        self._publisher: pubsub_v1.PublisherClient | None = None
        self._gcs_client: storage.Client | None = None

    def get_publisher(self) -> pubsub_v1.PublisherClient:
        """Warms up and caches the Pub/Sub publisher client with ordering enabled.

        Returns:
            The cached PubSub PublisherClient instance.
        """
        if self._publisher is None:
            logger.info("Initializing Pub/Sub PublisherClient")
            publisher_options = pubsub_v1.types.PublisherOptions(
                enable_message_ordering=True
            )
            self._publisher = pubsub_v1.PublisherClient(
                publisher_options=publisher_options
            )
        return self._publisher

    def get_gcs_client(self, project_id: str) -> storage.Client:
        """Warms up and caches the GCS client.

        Args:
            project_id: The Google Cloud Project ID.

        Returns:
            The cached storage.Client instance.
        """
        if self._gcs_client is None:
            logger.info("Initializing GCS Client")
            self._gcs_client = storage.Client(project=project_id)
        return self._gcs_client

    def get_processor(self) -> NormalizationEventProcessor:
        """Warms up and caches the Event Processor instance.

        Returns:
            The cached NormalizationEventProcessor instance.

        Raises:
            ValueError: If required environment variables are not set.
        """
        if self._processor is None:
            project_id = os.environ.get("PROJECT_ID", "watch-duty-dev")
            canonical_audio_bucket = os.environ.get("AUDIO_CANONICAL_BUCKET")
            output_topic = os.environ.get("OUTPUT_TOPIC")

            if not canonical_audio_bucket:
                msg = "AUDIO_CANONICAL_BUCKET environment variable must be set"
                logger.error(msg)
                raise ValueError(msg)

            if not output_topic:
                msg = "OUTPUT_TOPIC environment variable must be set"
                logger.error(msg)
                raise ValueError(msg)

            publisher = self.get_publisher()
            gcs_client = self.get_gcs_client(project_id)

            api_url = os.environ.get("AUDIO_SEGMENTS_API_URL")
            audio_segments_client_instance = None
            if api_url:
                logger.info("Initializing AudioSegmentsClient at: %s", api_url)
                audio_segments_client_instance = AudioSegmentsClient(api_url=api_url)
            else:
                logger.error(
                    "Missing AUDIO_SEGMENTS_API_URL environment variable."
                    "Audio segments will not be written to DB."
                )

            self._processor = NormalizationEventProcessor(
                project_id=project_id,
                canonical_audio_bucket=canonical_audio_bucket,
                output_topic=output_topic,
                audio_segments_client=audio_segments_client_instance,
                publisher=publisher,
                gcs_client=gcs_client,
            )
        return self._processor

    def eager_warmup(self) -> None:
        """Eagerly warms up and caches all gRPC/API clients during container initialization."""
        logger.info("Performing eager warm-start for container services...")
        try:
            self.get_processor()
            logger.info("Container services eagerly warmed up successfully.")
        except Exception as e:
            logger.warning(
                "Eager warm-start skipped or failed (expected in some test/local envs): %s",
                e,
            )


# Global container instance managed by the GCF container instance lifecycle
container = NormalizationServiceContainer()
container.eager_warmup()


@functions_framework.cloud_event
def normalize_claim_check(cloud_event: CloudEvent) -> None:
    """Entry point for Cloud Function Pub/Sub trigger events.

    Args:
        cloud_event: The triggered Pub/Sub CloudEvent.
    """
    processor = container.get_processor()
    processor.process_event(cloud_event)
