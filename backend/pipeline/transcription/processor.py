"""Transcription event processor module.

Extracts logic from the entry point, making it framework-independent
and highly unit-testable.
"""

import base64
import logging

from cloudevents.http.event import CloudEvent
from google.cloud import pubsub_v1

from backend.pipeline.common.constants import (
    MS_PER_SECOND,
    NANOS_PER_MS,
)
from backend.pipeline.common.tracing_utils import with_tracer_context
from backend.pipeline.schema_types.audio_ready_for_transcription_pb2 import (
    AudioReadyForTranscription,
)
from backend.pipeline.schema_types.transcribed_audio_pb2 import (
    TranscribedAudio,
)
from backend.pipeline.transcription import transcribers

logger = logging.getLogger(__name__)

CHIRP_UNINTELLIGIBLE_MARKER = "[UNINTELLIGIBLE]"


class TranscriptionEventProcessor:
    """Processes Pub/Sub claim-check CloudEvents, transcribes referenced GCS audio,
    and publishes the final TranscribedAudio results.
    """

    def __init__(
        self,
        *,
        project_id: str,
        output_topic: str,
        transcriber: transcribers.Transcriber,
        publisher: pubsub_v1.PublisherClient,
    ) -> None:
        self.project_id = project_id
        self.output_topic = output_topic
        self.transcriber = transcriber
        self.publisher = publisher

    def process_event(self, cloud_event: CloudEvent) -> None:
        """Decodes, processes, and transcribes the given CloudEvent."""
        pubsub_message = cloud_event.data.get("message", {}) or {}
        attributes = pubsub_message.get("attributes", {}) or {}
        traceparent = attributes.get("traceparent", "")

        with with_tracer_context(
            traceparent, "transcribe_claim_check", __name__
        ):
            raw_data = pubsub_message.get("data", "")
            if not raw_data:
                logger.error("Bad Request: Missing Pub/Sub data payload")
                return

            # Parse claim-check payload
            try:
                data_bytes = base64.b64decode(raw_data)
                claim = AudioReadyForTranscription()
                claim.ParseFromString(data_bytes)
            except Exception as e:
                logger.exception(
                    "Failed to parse AudioReadyForTranscription: %s", e
                )
                raise

            feed_id = claim.feed_id
            transmission_id = claim.transmission_id

            logger.info(
                "Received claim for transmission %s (feed %s, uri: %s)",
                transmission_id,
                feed_id,
                claim.canonical_audio_uri,
            )

            try:
                # Determine audio duration from start and end timestamps
                start_ms = (
                    claim.start_timestamp.seconds * MS_PER_SECOND
                    + claim.start_timestamp.nanos // NANOS_PER_MS
                )
                end_ms = (
                    claim.end_timestamp.seconds * MS_PER_SECOND
                    + claim.end_timestamp.nanos // NANOS_PER_MS
                )
                duration_ms = max(0, int(end_ms - start_ms))

                # Retrieve active transcriber and run Speech API
                transcript = self.transcriber.transcribe(
                    uri=claim.canonical_audio_uri,
                    duration_ms=duration_ms,
                )

                if not transcript:
                    logger.info(
                        "Speech API returned empty transcription. Using fallback unintelligible marker."
                    )
                    transcript = CHIRP_UNINTELLIGIBLE_MARKER

                # Build TranscribedAudio egress protobuf message
                out_proto = TranscribedAudio(
                    transmission_id=claim.transmission_id,
                    feed_id=claim.feed_id,
                    transcript=transcript,
                    start_timestamp=claim.start_timestamp,
                    end_timestamp=claim.end_timestamp,
                    missing_prior_context=claim.missing_prior_context,
                    missing_post_context=claim.missing_post_context,
                    source_audio_uris=claim.source_audio_uris,
                    start_audio_offset=claim.start_audio_offset,
                    end_audio_offset=claim.end_audio_offset,
                    canonical_audio_uri=claim.canonical_audio_uri,
                    playback_audio_uri=claim.playback_audio_uri,
                    feed_name=claim.feed_name,
                    external_id=claim.external_id,
                )

                # Egress to final output topic, strictly ordered by feed_id
                topic_path = self.publisher.topic_path(
                    self.project_id, self.output_topic.split("/")[-1]
                )

                attrs: dict[str, str] = {}
                if traceparent:
                    attrs["traceparent"] = traceparent

                future = self.publisher.publish(
                    topic=topic_path,
                    data=out_proto.SerializeToString(),
                    ordering_key=feed_id,
                    **attrs,
                )
                message_id = future.result()
                logger.info(
                    "Successfully transcribed and published egress message %s for transmission %s (feed %s)",
                    message_id,
                    transmission_id,
                    feed_id,
                )
            except Exception as e:
                logger.exception(
                    "Failed to process transcription claim for transmission %s (feed %s): %s",
                    transmission_id,
                    feed_id,
                    e,
                )
                raise
