"""Transcription event processor module.

Extracts logic from the entry point, making it framework-independent
and highly unit-testable.
"""

import asyncio
import base64
import logging
from typing import Any

import aiohttp
import grpc
import httpx
import requests
from cloudevents.http.event import CloudEvent
from google.api_core import exceptions
from google.cloud import pubsub_v1
from google.genai import errors as genai_errors
from opentelemetry.trace import StatusCode

from backend.pipeline.common import constants
from backend.pipeline.common.clients import audio_segments_client
from backend.pipeline.common.exceptions import PartialTranscriptionError
from backend.pipeline.common.log_helper import record_pipeline_stage
from backend.pipeline.common.tracing_utils import (
    get_tracer,
    inject_otel_context,
    parse_pubsub_cloudevent,
    with_tracer_context,
)
from backend.pipeline.schema_types.normalized_audio_pb2 import (
    NormalizedAudio,
)
from backend.pipeline.schema_types.transcribed_audio_pb2 import (
    TranscribedAudio,
)
from backend.pipeline.transcription.enums import TranscriptionStatus
from backend.pipeline.transcription.transcribers.base import Transcriber
from backend.pipeline.transcription.transcribers.gemini import (
    GeminiTranscriptionError,
    GeminiTransientTranscriptionError,
)
from backend.services.audio_segments import models as audio_segments_models

CHIRP_UNINTELLIGIBLE_MARKER = constants.UNINTELLIGIBLE_MARKER

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
        audio_segments_client: audio_segments_client.AsyncAudioSegmentsClient
        | None = None,
    ) -> None:
        self.project_id = project_id
        self.output_topic = output_topic
        self.transcriber = transcriber
        self.publisher = publisher
        self.audio_segments_client = audio_segments_client

    async def process_event(self, cloud_event: CloudEvent | dict) -> None:
        """Decodes, processes, and transcribes the given CloudEvent or raw dictionary."""
        record_pipeline_stage("transcription", "start")
        try:
            combined_attributes, raw_data = parse_pubsub_cloudevent(cloud_event)
        except Exception as e:
            logger.exception(
                "Failed to parse CloudEvent payload envelope: %s", e
            )
            return

        with with_tracer_context(
            combined_attributes, "transcribe_claim_check", __name__
        ) as root_span:
            if not raw_data:
                logger.error("Bad Request: Missing Pub/Sub data payload")
                return

            claim = self._parse_claim(raw_data)
            await self._transcribe_and_publish(claim, root_span)

    async def _transcribe_and_publish(
        self, claim: NormalizedAudio, root_span: Any
    ) -> None:
        """Transcribes the given audio claim and publishes the egress message."""
        errors = []
        transcript = ""
        segment_id = claim.segment_id
        feed_id = claim.feed_id

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
            record_pipeline_stage("transcription", "skipped")
            return

        try:
            transcript = await self._transcribe_audio_segment(claim, errors)

            # Build TranscribedAudio egress protobuf message
            out_proto = self._build_egress_message(claim, transcript)

            # Egress to final output topic, strictly ordered by feed_id
            message_id = await self._publish_egress(
                feed_id, segment_id, out_proto
            )
            record_pipeline_stage("transcription", "success")
            logger.info(
                "Successfully transcribed and published egress message %s for transmission %s (feed %s)",
                message_id,
                segment_id,
                feed_id,
            )
        except Exception as e:
            record_pipeline_stage("transcription", "error")
            status = _status_for_exception(e)
            record_pipeline_stage("transcription_status", status)
            if status == TranscriptionStatus.TRANSIENT_ERROR:
                logger.warning(
                    "Transient failure processing transcription claim for transmission %s (feed %s): %s. "
                    "Retrying...",
                    segment_id,
                    feed_id,
                    e,
                )
                errors.append(f"Transient Failure: {e}")
                raise

            root_span.record_exception(e)
            root_span.set_status(StatusCode.ERROR)
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
                has_transient_error = any(
                    err.startswith("Transient Failure:") for err in errors
                )
                if not has_transient_error:
                    await self._write_transcript_annotation(
                        segment_id,
                        transcript or "",
                        errors,
                    )

    async def _transcribe_audio_segment(
        self, claim: NormalizedAudio, errors: list[str]
    ) -> str:
        """Invokes the active transcriber, handles empty transcripts, and returns the text."""
        duration_ms = self._get_duration_ms(claim)

        record_pipeline_stage(
            "transcription_status", TranscriptionStatus.ATTEMPTS
        )
        tracer = get_tracer(__name__)
        with tracer.start_as_current_span("transcribe_audio") as span:
            span.set_attribute("segment_id", claim.segment_id)
            span.set_attribute("feed_id", claim.feed_id)
            span.set_attribute("duration_ms", duration_ms)
            try:
                transcript = await self.transcriber.transcribe(
                    uri=claim.transcription_audio_uri
                    or claim.canonical_audio_uri,
                    duration_ms=duration_ms,
                )
            except PartialTranscriptionError as e:
                logger.warning(
                    "Saved partial transcript for segment %s. Reason: %s",
                    claim.segment_id,
                    e.reason,
                )
                errors.append(f"Partial transcription ({e.reason})")
                record_pipeline_stage(
                    "transcription_status", TranscriptionStatus.PARTIAL
                )
                return e.partial_text.strip() if e.partial_text else ""

        transcript = transcript.strip() if transcript else ""
        return self._record_status_and_get_fallback_text(transcript)

    def _record_status_and_get_fallback_text(self, transcript: str) -> str:
        """Determines transcription status and returns the formatted text."""
        if not transcript:
            # We intentionally do NOT append to `errors` here. Appending an error causes
            # the UI to display "[Transcription failed]", which implies a backend crash.
            record_pipeline_stage(
                "transcription_status", TranscriptionStatus.EMPTY
            )
            return ""

        if transcript == CHIRP_UNINTELLIGIBLE_MARKER:
            record_pipeline_stage(
                "transcription_status", TranscriptionStatus.UNINTELLIGIBLE
            )
            return transcript

        record_pipeline_stage(
            "transcription_status", TranscriptionStatus.SUCCESS
        )
        return transcript

    def _get_duration_ms(self, claim: NormalizedAudio) -> int:
        """Determine audio duration in milliseconds from start and end timestamps."""
        start_ms = (
            claim.start_timestamp.seconds * constants.MS_PER_SECOND
            + claim.start_timestamp.nanos // constants.NANOS_PER_MS
        )
        end_ms = (
            claim.end_timestamp.seconds * constants.MS_PER_SECOND
            + claim.end_timestamp.nanos // constants.NANOS_PER_MS
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

    def _build_egress_message(
        self, claim: NormalizedAudio, transcript: str
    ) -> TranscribedAudio:
        """Constructs the TranscribedAudio egress message."""
        return TranscribedAudio(
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
            transcription_audio_uri=claim.transcription_audio_uri,
        )

    async def _publish_egress(
        self, feed_id: str, segment_id: str, egress_msg: TranscribedAudio
    ) -> str:
        """Publishes the TranscribedAudio egress message downstream and awaits acknowledgment."""
        topic_name = self.output_topic.split("/")[-1]
        topic_path = self.publisher.topic_path(self.project_id, topic_name)

        tracer = get_tracer(__name__)
        with tracer.start_as_current_span("publish_transcribed_audio") as span:
            span.set_attribute("segment_id", segment_id)
            span.set_attribute("feed_id", feed_id)

            attrs: dict[str, str] = {}
            inject_otel_context(attrs)

            future = self.publisher.publish(
                topic=topic_path,
                data=egress_msg.SerializeToString(),
                ordering_key=feed_id,
                **attrs,
            )
            return await asyncio.wrap_future(future)

    async def _write_transcript_annotation(
        self, segment_id: str, transcript: str, errors: list[str]
    ) -> None:
        """Writes transcript annotation to audio segments API."""
        if self.audio_segments_client is None:
            return

        tracer = get_tracer(__name__)
        with tracer.start_as_current_span(
            "write_transcript_annotation"
        ) as span:
            span.set_attribute("segment_id", segment_id)
            try:
                annotation_data = {
                    "text": transcript,
                    "errors": errors,
                }
                await self.audio_segments_client.add_audio_segment_annotation(
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
                span.record_exception(write_err)
                span.set_status(StatusCode.ERROR)
                logger.exception(
                    "Failed to add transcript annotation for segment %s: %s",
                    segment_id,
                    write_err,
                )


def _is_grpc_transient(e: grpc.Call) -> bool:
    """Determines if a gRPC call represents a transient failure status."""
    try:
        return e.code() in (
            grpc.StatusCode.UNAVAILABLE,
            grpc.StatusCode.DEADLINE_EXCEEDED,
            grpc.StatusCode.RESOURCE_EXHAUSTED,
            grpc.StatusCode.INTERNAL,
            grpc.StatusCode.ABORTED,
        )
    except (AttributeError, TypeError, ValueError):
        return False


def _status_for_exception(e: Exception) -> str:
    """Determine the transcription status category for a given exception."""
    if is_transient_exception(e):
        return TranscriptionStatus.TRANSIENT_ERROR

    match e:
        case GeminiTranscriptionError():
            return TranscriptionStatus.POLICY_BLOCKED
        case _:
            return TranscriptionStatus.PERMANENT_ERROR


def _is_http_transient(e: requests.exceptions.HTTPError) -> bool:
    """Determines if a requests HTTPError is retryable based on status code."""
    if e.response is None:
        return False
    return e.response.status_code == 429 or e.response.status_code >= 500


def is_transient_exception(e: Exception) -> bool:
    """Determines if an exception is transient and should be retried."""
    match e:
        case GeminiTransientTranscriptionError():
            result = True
        case exceptions.RetryError():
            cause = e.cause or e.__cause__
            result = (
                is_transient_exception(cause)
                if isinstance(cause, Exception)
                else True
            )

        case exceptions.GoogleAPICallError() | genai_errors.APIError():
            result = e.code in (409, 429, 499) or bool(e.code and e.code >= 500)

        case grpc.Call():
            result = _is_grpc_transient(e)

        case (
            ConnectionError()
            | TimeoutError()
            | requests.exceptions.Timeout()
            | requests.exceptions.ConnectionError()
            | httpx.RequestError()
            | httpx.TimeoutException()
            | aiohttp.ClientError()
        ):
            result = True

        case requests.exceptions.HTTPError():
            result = _is_http_transient(e)

        case _:
            result = False

    return result
