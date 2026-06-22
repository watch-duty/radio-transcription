"""Stateless Apache Beam transforms for the radio transcription pipeline.

This module defines the stateless mapper and serializer DoFns in our Apache
Beam DAG. These transforms perform zero stateful buffering or timer scheduling
and are highly optimized for parallel worker execution:
ParseAndKeyFn: Unmarshals raw Pub/Sub messages, validates protobuf chunk
   fields, extracts Telemetry tracing context, and sets a deterministic key.
"""

import datetime
import io
from collections.abc import Iterator
from typing import Any, override

import apache_beam as beam
import numpy as np
import soundfile as sf
from apache_beam.io.gcp.pubsub import PubsubMessage
from apache_beam.metrics import Metrics
from apache_beam.utils.shared import Shared
from google.protobuf.duration_pb2 import Duration
from google.protobuf.timestamp_pb2 import Timestamp
from opentelemetry import trace

from backend.pipeline.common.constants import (
    MICROSECONDS_PER_MS,
    MS_PER_SECOND,
    NANOS_PER_MS,
)
from backend.pipeline.common.log_helper import get_task_logger
from backend.pipeline.common.tracing_utils import (
    extract_trace_context,
    inject_otel_context,
    setup_tracing,
    with_tracer_context,
)
from backend.pipeline.schema_types.continuous_audio_pb2 import (
    ContinuousAudio,
)
from backend.pipeline.schema_types.segmented_audio_pb2 import (
    SegmentedAudio,
)
from backend.pipeline.segmentation.constants import (
    DEAD_LETTER_QUEUE_TAG,
)
from backend.pipeline.segmentation.datatypes import (
    AudioClassification,
    ChunkMetadata,
    FeedMetadata,
    FlushRequest,
    SegmentationDlqOutput,
)
from backend.pipeline.segmentation.options import (
    DataflowSystemOptions,  # noqa: F401
    SegmentationOptions,  # noqa: F401
)
from backend.pipeline.segmentation.storage import (
    acquire_shared_gcs_client,
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

    def __init__(self, *, is_continuous: bool = True) -> None:
        self.is_continuous = is_continuous

    @override
    def setup(self) -> None:
        """Initializes tracing and metrics for the worker."""
        setup_tracing(service_name="segmentation-pipeline")
        self.segmentation_start = Metrics.counter(
            self.__class__, "segmentation_start"
        )
        self.segmentation_error = Metrics.counter(
            self.__class__, "segmentation_error"
        )

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

            self.segmentation_start.inc()

            context = extract_trace_context(element.attributes)
            tracer = trace.get_tracer(__name__)
            with tracer.start_as_current_span(
                "receive_audio_chunk_for_segmentation", context=context
            ):
                if not feed_id:
                    msg = "ContinuousAudio missing required feed_id"
                    _raise(msg)
                if not chunk_proto.gcs_uri:
                    msg = "ContinuousAudio missing required gcs_uri"
                    _raise(msg)
                if not chunk_proto.session_id:
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
                if source_type and source_type != "bcfy_feeds":
                    msg = f"Received segmented source type '{source_type}' on continuous subscription"
                    _raise(msg)

                traceparent = (
                    element.attributes.get("traceparent")
                    if element.attributes
                    else None
                )
                baggage = (
                    element.attributes.get("baggage")
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
                    session_id=chunk_proto.session_id,
                    duration_ms=chunk_proto.duration_ms,
                    feed_metadata=FeedMetadata(
                        feed_name=chunk_proto.feed_name,
                    ),
                    is_continuous=True,
                    traceparent=traceparent,
                    baggage=baggage,
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
            self.segmentation_error.inc()
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


@beam.typehints.with_input_types(tuple[str, FlushRequest])
@beam.typehints.with_output_types(PubsubMessage)
class UploadRawSegmentFn(beam.DoFn):
    """Stateless DoFn to upload PCM audio bytes as a raw WAV file to the GCS staging bucket
    and yield a SegmentedAudio claim-check protobuf message.
    """

    SHARED_GCS_HANDLE = Shared()
    segmentation_success: Any
    segmentation_error: Any

    def __init__(
        self, staging_audio_bucket: str | None, project_id: str
    ) -> None:
        self.staging_audio_bucket = staging_audio_bucket
        self.project_id = project_id
        self.gcs_client = None

    @override
    def setup(self) -> None:
        setup_tracing(service_name="segmentation-pipeline")
        self.gcs_client = acquire_shared_gcs_client(
            self.project_id, shared_handle=self.SHARED_GCS_HANDLE
        )
        self.segmentation_success = Metrics.counter(
            self.__class__, "segmentation_success"
        )
        self.segmentation_error = Metrics.counter(
            self.__class__, "segmentation_error"
        )

    def _pcm_to_flac(self, pcm_bytes: bytes, sample_rate: int) -> bytes:
        audio_arr = np.frombuffer(pcm_bytes, dtype=np.int16)
        flac_io = io.BytesIO()
        sf.write(
            flac_io, audio_arr, sample_rate, format="FLAC", subtype="PCM_16"
        )
        return flac_io.getvalue()

    def _upload_raw_audio(
        self, request: FlushRequest, start_datetime: datetime.datetime
    ) -> str:
        """Converts PCM to FLAC, uploads to GCS, and returns the GCS URI."""
        flac_bytes = self._pcm_to_flac(request.buffer, request.sample_rate)
        flac_path = f"raw_segments/{request.feed_id}/{start_datetime:%Y/%m/%d}/{request.segment_id}.flac"

        if not self.staging_audio_bucket:
            err_msg = "staging_audio_bucket is not configured"
            raise ValueError(err_msg)
        if not self.gcs_client:
            err_msg = "GCS client not initialized"
            raise RuntimeError(err_msg)

        bucket = self.gcs_client.bucket(self.staging_audio_bucket)
        blob = bucket.blob(flac_path)
        blob.upload_from_string(flac_bytes, content_type="audio/flac")
        return f"gs://{self.staging_audio_bucket}/{flac_path}"

    def _build_segmented_audio_proto(
        self, request: FlushRequest, gcs_uri: str
    ) -> SegmentedAudio:
        """Constructs and returns the SegmentedAudio protobuf message."""
        start_timestamp = Timestamp()
        start_timestamp.FromMicroseconds(
            request.time_range.start_ms * MICROSECONDS_PER_MS
        )

        end_timestamp = Timestamp()
        end_timestamp.FromMicroseconds(
            request.time_range.end_ms * MICROSECONDS_PER_MS
        )

        start_offset = Duration()
        if (
            request.start_audio_offset_ms is not None
            and request.start_audio_offset_ms > 0
        ):
            start_offset.FromMicroseconds(
                int(request.start_audio_offset_ms * MICROSECONDS_PER_MS)
            )

        end_offset = Duration()
        if (
            request.end_audio_offset_ms is not None
            and request.end_audio_offset_ms > 0
        ):
            end_offset.FromMicroseconds(
                int(request.end_audio_offset_ms * MICROSECONDS_PER_MS)
            )

        return SegmentedAudio(
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
            audio_classification=AudioClassification(
                request.audio_classification
            ).name,
            raw_audio_uri=gcs_uri,
        )

    @override
    def process(
        self,
        element: tuple[str, FlushRequest],
    ) -> Iterator[PubsubMessage | SegmentationDlqOutput]:
        feed_id, request = element
        trace_attrs: dict[str, str] = {}
        if request.traceparent:
            trace_attrs["traceparent"] = request.traceparent
        baggage_val = getattr(request, "baggage", None)
        if baggage_val is not None:
            trace_attrs["baggage"] = str(baggage_val)

        try:
            with with_tracer_context(
                trace_attrs,
                "upload_raw_segment",
                "backend.pipeline.segmentation.transforms.stateless",
            ):
                start_datetime = datetime.datetime.fromtimestamp(
                    request.time_range.start_ms / MS_PER_SECOND,
                    tz=datetime.UTC,
                )

                gcs_uri = self._upload_raw_audio(request, start_datetime)
                segmented_audio_pb = self._build_segmented_audio_proto(
                    request, gcs_uri
                )

                pubsub_attributes: dict[str, str] = {}
                inject_otel_context(pubsub_attributes)

                yield PubsubMessage(
                    data=segmented_audio_pb.SerializeToString(),
                    attributes=pubsub_attributes,
                    ordering_key=request.feed_id,
                )
                self.segmentation_success.inc()

        except Exception as e:
            logger.exception(
                "Error uploading raw segment for feed %s",
                feed_id,
            )
            self.segmentation_error.inc()
            yield beam.pvalue.TaggedOutput(
                DEAD_LETTER_QUEUE_TAG,
                {"error": str(e), "feed_id": feed_id},
            )
