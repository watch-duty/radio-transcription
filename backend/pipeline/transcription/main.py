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
from backend.pipeline.transcription import transcribers
from backend.pipeline.transcription.processor import TranscriptionEventProcessor

# Setup Logging and Tracing
setup_logging()
setup_tracing(use_batch=False)
logger = logging.getLogger(__name__)

# Warm start cached instances
_transcriber_instance: transcribers.Transcriber | None = None
_publisher_client: pubsub_v1.PublisherClient | None = None
_processor_instance: TranscriptionEventProcessor | None = None


def get_lazy_transcriber(project_id: str) -> transcribers.Transcriber:
    """Warms up and caches the transcriber instance."""
    global _transcriber_instance  # noqa: PLW0603
    if _transcriber_instance is None:
        t_type_str = os.environ.get("TRANSCRIBER_TYPE", "GOOGLE_CHIRP_V3")
        t_config_json = os.environ.get("TRANSCRIBER_CONFIG", "{}")
        t_type = TranscriberType[t_type_str]
        logger.info("Initializing transcriber type %s", t_type.name)
        _transcriber_instance = transcribers.get_transcriber(
            t_type, project_id, t_config_json
        )
        _transcriber_instance.setup()
    return _transcriber_instance


def get_lazy_publisher() -> pubsub_v1.PublisherClient:
    """Warms up and caches the Pub/Sub publisher client."""
    global _publisher_client  # noqa: PLW0603
    if _publisher_client is None:
        logger.info("Initializing Pub/Sub PublisherClient")
        # Enable publisher side ordering keys
        publisher_options = pubsub_v1.types.PublisherOptions(
            enable_message_ordering=True
        )
        _publisher_client = pubsub_v1.PublisherClient(
            publisher_options=publisher_options
        )
    return _publisher_client


def get_lazy_processor() -> TranscriptionEventProcessor:
    """Warms up and caches the Event Processor instance."""
    global _processor_instance  # noqa: PLW0603
    if _processor_instance is None:
        project_id = os.environ.get("PROJECT_ID", "watch-duty-dev")
        output_topic = os.environ.get("OUTPUT_TOPIC")
        if not output_topic:
            msg = "OUTPUT_TOPIC environment variable must be set"
            logger.error(msg)
            raise ValueError(msg)
        transcriber = get_lazy_transcriber(project_id)
        publisher = get_lazy_publisher()

        _processor_instance = TranscriptionEventProcessor(
            project_id=project_id,
            output_topic=output_topic,
            transcriber=transcriber,
            publisher=publisher,
        )
    return _processor_instance


@functions_framework.cloud_event
def transcribe_claim_check(cloud_event: CloudEvent) -> None:
    """Entry point for Cloud Function Pub/Sub trigger events."""
    processor = get_lazy_processor()
    processor.process_event(cloud_event)
