"""Transcription event processor module.

Extracts logic from the entry point, making it framework-independent
and highly unit-testable.
"""

import base64
import logging

import grpc
import requests
from cloudevents.http.event import CloudEvent
from google.api_core.exceptions import GoogleAPICallError
from google.cloud import pubsub_v1

from backend.pipeline.common.clients import audio_segments_client
from backend.pipeline.common.constants import (
    MS_PER_SECOND,
    NANOS_PER_MS,
)
from backend.pipeline.common.tracing_utils import (
    get_current_traceparent,
    with_tracer_context,
)
from backend.pipeline.schema_types.normalized_audio_pb2 import (
    NormalizedAudio,
)
from backend.pipeline.schema_types.transcribed_audio_pb2 import (
    TranscribedAudio,
)
from backend.pipeline.transcription.transcribers.base import Transcriber
from backend.services.audio_segments import models as audio_segments_models

CHIRP_UNINTELLIGIBLE_MARKER = "[UNINTELLIGIBLE]"

logger = logging.getLogger(__name__)


class TranscriptionEventProcessor:
    """Processes Pub/Sub claim-check CloudEvents, transcribes referenced GCS audio,
    and publishes the final TranscribedAudio results.
    """

    def __init__(
        self,
        *,
        project_id: str,
        output_topic: str,
        transcriber: Transcriber,
        publisher: pubsub_v1.PublisherClient,
        audio_segments_client: audio_segments_client.AudioSegmentsClient
        | None = None,
    ) -> None:
        self.project_id = project_id
        self.output_topic = output_topic
        self.transcriber = transcriber
        self.publisher = publisher
        self.audio_segments_client = audio_segments_client

    def process_event(self, cloud_event: CloudEvent) -> None:
        """Decodes, processes, and transcribes the given CloudEvent."""
        pubsub_message = cloud_event.data.get("message", {}) or {}
        attributes = pubsub_message.get("attributes", {}) or {}
        traceparent = attributes.get("traceparent", "")

        with with_tracer_context(
            traceparent, "transcribe_claim_check", __name__
        ):
            errors = []
            transcript = ""
            segment_id = ""
            raw_data = pubsub_message.get("data", "")
            if not raw_data:
                logger.error("Bad Request: Missing Pub/Sub data payload")
                return

            # Parse claim-check payload
            claim = self._parse_claim(raw_data)

            feed_id = claim.feed_id
            segment_id = claim.segment_id

            logger.info(
                "Received claim for transmission %s (feed %s, uri: %s)",
                segment_id,
                feed_id,
                claim.canonical_audio_uri,
            )

            if claim.audio_classification == claim.AUDIO_CLASSIFICATION_OTHER:
                logger.info(
                    "Skipping transcription for non-speech segment %s (feed %s)",
                    segment_id,
                    feed_id,
                )
                return

            try:
                # Determine audio duration from start and end timestamps
                duration_ms = self._get_duration_ms(claim)

                # Retrieve active transcriber and run Speech API
                transcript = self.transcriber.transcribe(
                    uri=claim.canonical_audio_uri,
                    duration_ms=duration_ms,
                )

                if not transcript:
                    logger.info(
                        "Speech API returned empty transcription. Using fallback unintelligible marker."
                    )
                    errors.append("Empty transcription from Speech Model")
                    transcript = CHIRP_UNINTELLIGIBLE_MARKER

                # Build TranscribedAudio egress protobuf message
                out_proto = TranscribedAudio(
                    segment_id=claim.segment_id,
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
                )

                # Egress to final output topic, strictly ordered by feed_id
                topic_name = self.output_topic.split("/")[-1]
                topic_path = self.publisher.topic_path(
                    self.project_id, topic_name
                )

                attrs: dict[str, str] = {}
                current_tp = get_current_traceparent() or traceparent
                if current_tp:
                    attrs["traceparent"] = current_tp

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
                    segment_id,
                    feed_id,
                )
            except Exception as e:
                if _is_transient_exception(e):
                    logger.warning(
                        "Transient failure processing transcription claim for transmission %s (feed %s): %s. "
                        "Retrying...",
                        segment_id,
                        feed_id,
                        e,
                    )
                    errors.append(f"Transient Failure: {e}")
                    raise

                logger.exception(
                    "Permanent failure processing transcription claim for transmission %s (feed %s): %s. "
                    "Acknowledging message without retry.",
                    segment_id,
                    feed_id,
                    e,
                )
                errors.append(f"Permanent Failure: {e}")
            finally:
                if segment_id:
                    self._write_transcript_annotation(
                        segment_id,
                        transcript or "",
                        errors,
                    )

    def _get_duration_ms(self, claim: NormalizedAudio) -> int:
        """Determine audio duration in milliseconds from start and end timestamps."""
        start_ms = (
            claim.start_timestamp.seconds * MS_PER_SECOND
            + claim.start_timestamp.nanos // NANOS_PER_MS
        )
        end_ms = (
            claim.end_timestamp.seconds * MS_PER_SECOND
            + claim.end_timestamp.nanos // NANOS_PER_MS
        )
        return max(0, int(end_ms - start_ms))

    def _parse_claim(self, raw_data: str) -> NormalizedAudio:
        """Parses the base64 encoded NormalizedAudio protobuf payload."""
        try:
            data_bytes = base64.b64decode(raw_data)
            claim = NormalizedAudio()
            claim.ParseFromString(data_bytes)
        except Exception as e:
            logger.exception("Failed to parse NormalizedAudio: %s", e)
            raise
        else:
            return claim

    def _write_transcript_annotation(
        self, segment_id: str, transcript: str, errors: list[str]
    ) -> None:
        """Writes transcript annotation to audio segments API."""
        if self.audio_segments_client is None:
            return

        try:
            annotation_data = {
                "text": transcript,
                "errors": errors,
            }
            self.audio_segments_client.add_audio_segment_annotation(
                audio_segment_id=segment_id,
                annotation_type=(
                    audio_segments_models.AnnotationType.TRANSCRIPT
                ),
                data=annotation_data,
            )
            logger.info(
                "Successfully added transcript annotation for segment %s",
                segment_id,
            )
        except Exception as write_err:
            logger.exception(
                "Failed to add transcript annotation for segment %s: %s",
                segment_id,
                write_err,
            )


def _is_transient_exception(e: Exception) -> bool:
    """Determines if an exception is transient and should be retried."""
    is_transient = False
    match e:
        case GoogleAPICallError() if e.code in (429, 409) or (
            e.code and e.code >= 500
        ):
            is_transient = True
        case grpc.Call():
            try:
                match e.code():
                    case (
                        grpc.StatusCode.UNAVAILABLE
                        | grpc.StatusCode.DEADLINE_EXCEEDED
                        | grpc.StatusCode.RESOURCE_EXHAUSTED
                        | grpc.StatusCode.INTERNAL
                        | grpc.StatusCode.ABORTED
                    ):
                        is_transient = True
            except (AttributeError, TypeError, ValueError):
                pass
        case ConnectionError() | TimeoutError():
            is_transient = True
        case (
            requests.exceptions.Timeout()
            | requests.exceptions.ConnectionError()
        ):
            is_transient = True
        case requests.exceptions.HTTPError() if e.response is not None and (
            e.response.status_code == 429 or e.response.status_code >= 500
        ):
            is_transient = True
    return is_transient
