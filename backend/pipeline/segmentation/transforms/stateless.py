"""Stateless Apache Beam transforms for the radio transcription pipeline.

This module defines the stateless mapper and serializer DoFns in our Apache
Beam DAG. These transforms perform zero stateful buffering or timer scheduling
and are highly optimized for parallel worker execution:
1. ParseAndKeyFn: Unmarshals raw Pub/Sub messages, validates protobuf chunk
   fields, extracts Telemetry tracing context, and sets a deterministic key.
2. SerializeFn: Formats downstream TranscriptionResult payloads into standard
   TranscribedAudio protobuf formats, serializes them, and prepares them for
   egress Pub/Sub topic publication.
"""

import datetime
import io
from collections.abc import Iterator
from typing import Any, override

import apache_beam as beam
import numpy as np
import soundfile as sf
from apache_beam.io.gcp.pubsub import PubsubMessage
from google.cloud import storage
from google.protobuf.duration_pb2 import Duration  # type: ignore
from google.protobuf.timestamp_pb2 import Timestamp  # type: ignore
from opentelemetry import trace

from backend.pipeline.common.constants import (
    MICROSECONDS_PER_MS,
    MS_PER_SECOND,
    NANOS_PER_MS,
)
from backend.pipeline.common.log_helper import get_task_logger
from backend.pipeline.common.tracing_utils import (
    extract_trace_context,
    get_current_traceparent,
    setup_tracing,
)
from backend.pipeline.schema_types.continuous_audio_pb2 import (
    ContinuousAudio,
)
from backend.pipeline.schema_types.segmented_audio_pb2 import (
    SegmentedAudio,
)
from backend.pipeline.schema_types.transcribed_audio_pb2 import (
    TranscribedAudio,
)
from backend.pipeline.segmentation.constants import (
    DEAD_LETTER_QUEUE_TAG,
)
from backend.pipeline.segmentation.datatypes import (
    ChunkMetadata,
    FeedMetadata,
    SegmentationDlqOutput,
    TranscriptionResult,
)
from backend.pipeline.segmentation.options import (
    DataflowSystemOptions,  # noqa: F401
    TranscriptionOptions,  # noqa: F401
)

logger = get_task_logger(
    __name__, {"system": "transcription", "component": "transforms"}
)


@beam.typehints.with_input_types(PubsubMessage)
@beam.typehints.with_output_types(tuple[str, ChunkMetadata])
class ParseAndKeyFn(beam.DoFn):
    """Extracts the feed_id, parses the protobuf, and builds ChunkMetadata.

    Routes messages missing required attributes or with invalid payload to the DLQ.
    Yields a tuple of `(feed_id, ChunkMetadata)` to establish a deterministic routing key
    for all subsequent stateful operations on that feed.

    Args:
        is_continuous: Whether this instance is processing continuous feeds (e.g. BCFY_FEEDS).
            Determines whether session_id is required and sets the flag on ChunkMetadata.
            Set by orchestration based on which Pub/Sub subscription the message arrived from.
    """

    def __init__(self, *, is_continuous: bool) -> None:
        self.is_continuous = is_continuous

    @override
    def setup(self) -> None:
        """Initializes tracing for the worker."""
        setup_tracing(service_name="normalization-pipeline")

    @override
    def process(
        self, element: PubsubMessage, *args: Any, **kwargs: Any
    ) -> Iterator[tuple[str, ChunkMetadata] | SegmentationDlqOutput]:
        """Extracts the feed_id and parses the protobuf payload."""

        def _raise(msg: str) -> None:
            raise ValueError(msg)

        outputs = []
        try:
            chunk_proto = ContinuousAudio()
            chunk_proto.ParseFromString(element.data)
            feed_id = chunk_proto.feed_id
            context = extract_trace_context(element.attributes)
            tracer = trace.get_tracer(__name__)
            with tracer.start_as_current_span(
                "receive_audio_chunk_for_normalization", context=context
            ):
                if not feed_id:
                    msg = "ContinuousAudio missing required feed_id"
                    _raise(msg)
                if not chunk_proto.gcs_uri:
                    msg = "ContinuousAudio missing required gcs_uri"
                    _raise(msg)
                if self.is_continuous and not chunk_proto.session_id:
                    msg = "ContinuousAudio missing required session_id for continuous feed"
                    _raise(msg)
                if not chunk_proto.feed_name:
                    msg = "ContinuousAudio missing required feed_name"
                    _raise(msg)

                source_type = (
                    element.attributes.get("source_type")
                    if element.attributes
                    else None
                )
                if source_type:
                    if self.is_continuous and source_type != "bcfy_feeds":
                        msg = f"Received segmented source type '{source_type}' on continuous subscription"
                        _raise(msg)
                    elif not self.is_continuous and source_type == "bcfy_feeds":
                        msg = f"Received continuous source type '{source_type}' on segmented subscription"
                        _raise(msg)

                traceparent = (
                    element.attributes.get("traceparent")
                    if element.attributes
                    else None
                )
                start_ms = (
                    (
                        chunk_proto.start_timestamp.seconds * MS_PER_SECOND
                        + chunk_proto.start_timestamp.nanos // NANOS_PER_MS
                    )
                    if chunk_proto.HasField("start_timestamp")
                    and chunk_proto.start_timestamp.seconds > 0
                    else None
                )
                metadata = ChunkMetadata(
                    gcs_uri=chunk_proto.gcs_uri,
                    # For segmented feeds, session_id is typically the call ID set by ingestion.
                    # Fall back to feed_id (not a static string) so per-feed session change
                    # detection in the ordering buffer remains meaningful.
                    session_id=chunk_proto.session_id or feed_id,
                    duration_ms=chunk_proto.duration_ms,
                    feed_metadata=FeedMetadata(
                        feed_name=chunk_proto.feed_name,
                    ),
                    is_continuous=self.is_continuous,
                    traceparent=traceparent,
                    timestamp_ms=start_ms,
                )
                logger.debug(
                    "Parsed ContinuousAudio feed_id=%s gcs_uri=%s duration=%dms",
                    feed_id,
                    chunk_proto.gcs_uri,
                    chunk_proto.duration_ms,
                )
                outputs.append((feed_id, metadata))
        except Exception as e:
            msg = f"Failed to parse or validate payload: {e}"
            logger.exception(msg)
            outputs.append(
                beam.pvalue.TaggedOutput(
                    DEAD_LETTER_QUEUE_TAG,
                    {
                        "error": msg,
                        "attributes": dict(element.attributes),
                    },
                )
            )

        yield from outputs


@beam.typehints.with_input_types(TranscriptionResult)
@beam.typehints.with_output_types(PubsubMessage)
class SerializeFn(beam.DoFn):
    """Serializes the final TranscribedAudio message to Pub/Sub."""

    @override
    def process(
        self,
        element: TranscriptionResult,
    ) -> Iterator[PubsubMessage | SegmentationDlqOutput]:
        def _raise(msg: str) -> None:
            raise ValueError(msg)

        try:
            value = element

            if value.start_audio_offset_ms is None:
                msg = f"Missing start_audio_offset_ms for feed_id: {value.feed_id} (session: {value.session_id})"
                _raise(msg)
            start_offset = datetime.timedelta(
                milliseconds=value.start_audio_offset_ms
            )

            if value.end_audio_offset_ms is None:
                msg = f"Missing end_audio_offset_ms for feed_id: {value.feed_id} (session: {value.session_id})"
                _raise(msg)
            end_offset = datetime.timedelta(
                milliseconds=value.end_audio_offset_ms
            )

            if value.feed_metadata is None:
                msg = f"Missing feed_metadata in TranscriptionResult for feed_id: {value.feed_id} (session: {value.session_id})"
                _raise(msg)

            if not value.contributing_audio_uris:
                msg = f"Missing contributing_audio_uris in TranscriptionResult for feed_id: {value.feed_id} (session: {value.session_id})"
                _raise(msg)

            proto = TranscribedAudio(
                feed_id=value.feed_id,
                source_audio_uris=value.contributing_audio_uris,
                segment_id=value.segment_id,
                transcript=value.transcript,
                missing_prior_context=value.missing_prior_context,
                missing_post_context=value.missing_post_context,
                start_audio_offset=start_offset,
                end_audio_offset=end_offset,
                canonical_audio_uri=value.canonical_audio_uri,
                playback_audio_uri=value.playback_audio_uri,
                feed_name=value.feed_metadata.feed_name,
            )
            proto.start_timestamp.FromMicroseconds(
                value.time_range.start_ms * MICROSECONDS_PER_MS
            )
            proto.end_timestamp.FromMicroseconds(
                value.time_range.end_ms * MICROSECONDS_PER_MS
            )
            attrs: dict[str, str] = {}
            current_tp = get_current_traceparent() or value.traceparent
            if current_tp:
                attrs["traceparent"] = current_tp

            yield PubsubMessage(
                data=proto.SerializeToString(),
                attributes=attrs,
                ordering_key=value.feed_id,
            )
        except Exception as e:
            logger.exception(
                "Error serializing transcription result for feed %s",
                element.feed_id,
            )
            yield beam.pvalue.TaggedOutput(
                DEAD_LETTER_QUEUE_TAG,
                {"error": str(e), "feed_id": element.feed_id},
            )


@beam.typehints.with_input_types(tuple)
@beam.typehints.with_output_types(PubsubMessage)
class UploadRawSegmentFn(beam.DoFn):
    """Stateless DoFn to upload PCM audio bytes as a raw WAV file to the GCS staging bucket
    and yield a SegmentedAudio claim-check protobuf message.
    """

    def __init__(
        self, staging_audio_bucket: str | None, project_id: str
    ) -> None:
        self.staging_audio_bucket = staging_audio_bucket
        self.project_id = project_id
        self.gcs_client = None

    @override
    def setup(self) -> None:
        """Initializes GCS client for uploading raw segments."""
        setup_tracing()
        self.gcs_client = storage.Client(project=self.project_id)

    def _pcm_to_flac(self, pcm_bytes: bytes, sample_rate: int) -> bytes:
        audio_arr = np.frombuffer(pcm_bytes, dtype=np.int16)
        flac_io = io.BytesIO()
        sf.write(
            flac_io, audio_arr, sample_rate, format="FLAC", subtype="PCM_16"
        )
        return flac_io.getvalue()

    @override
    def process(
        self,
        element: tuple[str, Any],
    ) -> Iterator[PubsubMessage | SegmentationDlqOutput]:
        feed_id, request = element
        try:
            dt = datetime.datetime.fromtimestamp(
                request.time_range.start_ms / MS_PER_SECOND,
                tz=datetime.UTC,
            )

            # Convert PCM buffer to compressed lossless FLAC format bytes
            flac_bytes = self._pcm_to_flac(request.buffer, request.sample_rate)

            # Construct raw segment GCS path
            flac_path = f"raw_segments/{request.feed_id}/{dt:%Y/%m/%d}/{request.segment_id}.flac"

            if not self.staging_audio_bucket:
                err_msg = "staging_audio_bucket is not configured"
                raise ValueError(err_msg)  # noqa: TRY301
            if not self.gcs_client:
                err_msg = "GCS client not initialized"
                raise RuntimeError(err_msg)  # noqa: TRY301

            bucket = self.gcs_client.bucket(self.staging_audio_bucket)
            blob = bucket.blob(flac_path)
            blob.upload_from_string(flac_bytes, content_type="audio/flac")
            gcs_uri = f"gs://{self.staging_audio_bucket}/{flac_path}"

            # Build SegmentedAudio claim-check protobuf message
            start_timestamp = Timestamp()
            start_timestamp.FromMicroseconds(
                request.time_range.start_ms * MICROSECONDS_PER_MS
            )

            end_timestamp = Timestamp()
            end_timestamp.FromMicroseconds(
                request.time_range.end_ms * MICROSECONDS_PER_MS
            )

            start_offset = Duration()
            if request.start_audio_offset_ms:
                start_offset.FromMicroseconds(
                    int(request.start_audio_offset_ms * MICROSECONDS_PER_MS)
                )

            end_offset = Duration()
            if request.end_audio_offset_ms:
                end_offset.FromMicroseconds(
                    int(request.end_audio_offset_ms * MICROSECONDS_PER_MS)
                )

            proto = SegmentedAudio(
                segment_id=request.segment_id,
                feed_id=request.feed_id,
                start_timestamp=start_timestamp,
                end_timestamp=end_timestamp,
                missing_prior_context=request.missing_prior_context,
                missing_post_context=request.missing_post_context,
                source_audio_uris=request.contributing_audio_uris,
                start_audio_offset=start_offset,
                end_audio_offset=end_offset,
                feed_name=request.feed_metadata.feed_name,
                audio_classification=request.audio_classification,
                raw_audio_uri=gcs_uri,
            )

            attrs: dict[str, str] = {}
            if request.traceparent:
                attrs["traceparent"] = request.traceparent

            yield PubsubMessage(
                data=proto.SerializeToString(),
                attributes=attrs,
                ordering_key=request.feed_id,
            )

        except Exception as e:
            logger.exception(
                "Error uploading raw segment for feed %s",
                feed_id,
            )
            yield beam.pvalue.TaggedOutput(
                DEAD_LETTER_QUEUE_TAG,
                {"error": str(e), "feed_id": feed_id},
            )
