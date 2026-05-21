"""Serverless Cloud Run / Cloud Function transcription entry point.

Triggered by Pub/Sub push events containing serialized AudioReadyForTranscription
claim-check metadata. Delegates processing to TranscriptionEventProcessor.
"""

import logging
import os

import functions_framework
from cloudevents.http.event import CloudEvent
from google.cloud import pubsub_v1

from backend.pipeline.common.logging import setup_logging
from backend.pipeline.common.tracing_utils import setup_tracing
from backend.pipeline.normalization.common.enums import TranscriberType
from backend.pipeline.transcription.processor import TranscriptionEventProcessor
from backend.pipeline.transcription.transcribers.base import Transcriber
from backend.pipeline.transcription.transcribers.factory import get_transcriber

# Setup Logging and Tracing
setup_logging()
setup_tracing(use_batch=False)
logger = logging.getLogger(__name__)


class TranscriptionServiceContainer:
    """Encapsulates the warm-started cached service container instances for GCF."""

    def __init__(self) -> None:
        self._transcriber: Transcriber | None = None
        self._publisher: pubsub_v1.PublisherClient | None = None
        self._processor: TranscriptionEventProcessor | None = None

    def get_transcriber(self, project_id: str) -> Transcriber:
        """Warms up and caches the transcriber instance.

        Args:
            project_id: The Google Cloud Project ID.

        Returns:
            The cached Transcriber instance.
        """
        if self._transcriber is None:
            t_type_str = os.environ.get("TRANSCRIBER_TYPE", "GOOGLE_CHIRP_V3")
            t_config_json = os.environ.get("TRANSCRIBER_CONFIG", "{}")
            t_type = TranscriberType(t_type_str.lower())
            logger.info("Initializing transcriber type %s", t_type.name)
            self._transcriber = get_transcriber(
                t_type, project_id, t_config_json
            )
            self._transcriber.setup()
        return self._transcriber

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

    def get_processor(self) -> TranscriptionEventProcessor:
        """Warms up and caches the Event Processor instance.

        Returns:
            The cached TranscriptionEventProcessor instance.

        Raises:
            ValueError: If the OUTPUT_TOPIC environment variable is not set.
        """
        if self._processor is None:
            project_id = os.environ.get("PROJECT_ID", "watch-duty-dev")
            output_topic = os.environ.get("OUTPUT_TOPIC")
            if not output_topic:
                msg = "OUTPUT_TOPIC environment variable must be set"
                logger.error(msg)
                raise ValueError(msg)

            transcriber = self.get_transcriber(project_id)
            publisher = self.get_publisher()

            self._processor = TranscriptionEventProcessor(
                project_id=project_id,
                output_topic=output_topic,
                transcriber=transcriber,
                publisher=publisher,
            )
        return self._processor

# Global container instance managed by the GCF container instance lifecycle
container = TranscriptionServiceContainer()


@functions_framework.cloud_event
def transcribe_claim_check(cloud_event: CloudEvent) -> None:
    """Entry point for Cloud Function Pub/Sub trigger events.

    Args:
        cloud_event: The triggered Pub/Sub CloudEvent.
    """
    processor = container.get_processor()
    processor.process_event(cloud_event)
