import functions_framework
from cloudevents.http.event import CloudEvent

from backend.pipeline.transcription.transcription_handler import (
    handle_transcription_event,
)


@functions_framework.cloud_event
def transcribe_audio(cloud_event: CloudEvent) -> None:
    """Cloud Function entry point triggered by Pub/Sub."""
    handle_transcription_event(cloud_event)
