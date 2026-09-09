"""Stateless Apache Beam transforms for the radio transcription pipeline.

## What "stateless" means here

Strictly: these DoFns declare no `StateSpec` and no `TimerSpec`, so Beam is
free to distribute them across the fleet. That is the property Stage 3's
reshuffle depends on, and it holds -- nothing in this module accumulates
per-element or per-key state.

It does not mean these DoFns hold nothing. `UploadRawSegmentFn` acquires two
per-worker resources in `setup()`:
- a GCS client, via a `Shared` handle;
- the process-wide download `ThreadPoolExecutor`, which is **shared with the
  stateful stitcher stage** so that total download concurrency is bounded per
  worker rather than per stage (see `storage.acquire_shared_download_executor`).

That second one is worth knowing about. It was sized when Stage 3 was fused
onto Stage 2 and therefore serialized per key. The reshuffle removes that
serialization, so many more uploads now contend for the same fixed pool. If
Stage 3 saturates it, downloads queue: expect `download_latency_ms` to climb
while worker CPU sits idle, which looks like the reshuffle failing when it is
really this pool.

This module defines the stateless mapper and serializer DoFns in our Apache
Beam DAG:
- **ParseAndKeyFn**: Unmarshals raw Pub/Sub messages, validates protobuf chunk
   fields, extracts Telemetry tracing context, and sets a deterministic key.
- **UploadRawSegmentFn**: Stateless audio stitching stage. Downloads contributing
   chunks from GCS, slices them according to VAD segments, stitches them, and
   uploads the final FLAC segment to GCS.

## Decoupled Stateful/Stateless Hybrid Architecture & Key Scattering

To prevent Dataflow Windmill state locking, key-fusion CPU bottlenecks, and
GIL contention, the pipeline uses a decoupled 4-stage hybrid architecture:
1. **Stage 2 (Stateful - OrderedStitchAudioFn & TagSequenceNumberFn)**:
   Performs chronological sequencing, session FSM tracking, and VAD segment
   calculations. To keep persistent state sizes extremely small (<1 KB) and
   lock times under microseconds, **no raw audio bytes are stored in
   stateful persistent bag states**. `TagSequenceNumberFn` tags each segment
   with a strictly monotonic per-session sequence number (`1, 2, 3...`).
2. **Stage 3 (Stateless - UploadRawSegmentFn with Reshuffle)**: Performs the
   heavy physical work of downloading contributing audio chunks from GCS,
   slicing them according to Stage 2's VAD segments, stitching them, and
   compressing the result to FLAC. To prevent single-worker vCPU saturation
   on busy feeds, elements exiting Stage 2 are re-keyed with a randomized
   UUID suffix (`feed_id#session_id#uuid`) and passed through
   `beam.Reshuffle()`, spreading Stage 3 encoding across the Dataflow worker
   fleet instead of pinning it to one vCPU per feed.
3. **Stage 4 (Stateful - PubSubOrderRestorerFn)**: Because parallel Stage 3
   execution causes segments to finish out of order, `PubSubOrderRestorerFn`
   buffers out-of-order completions per `feed_id#session_id` and restores the
   order they were sequenced in before publishing to Pub/Sub. Ordering is
   best-effort with a bounded delay, not a guarantee: sequence numbers reflect
   arrival order at `TagSequenceNumberFn` rather than audio timestamps, and a
   fallback timer publishes out of order when a segment is late. See
   ARCHITECTURE.md section 5C.
"""

import concurrent.futures
import datetime
import io
import logging
import time
import urllib.parse
from collections.abc import Iterator
from operator import attrgetter
from typing import Any, override

import apache_beam as beam
import numpy as np
import soundfile as sf
from apache_beam.io.gcp.pubsub import PubsubMessage
from apache_beam.metrics import Metrics
from apache_beam.metrics.metric import Counter, Distribution
from apache_beam.utils.shared import Shared
from google.protobuf.duration_pb2 import Duration
from google.protobuf.timestamp_pb2 import Timestamp
from opentelemetry import context as otel_context

from backend.pipeline.common.constants import (
    CONTINUOUS_SOURCE_TYPES,
    MICROSECONDS_PER_MS,
    MS_PER_SECOND,
    NANOS_PER_MS,
)
from backend.pipeline.common.log_helper import get_task_logger
from backend.pipeline.common.tracing_utils import (
    extract_trace_context,
    get_tracer,
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
from backend.pipeline.segmentation import cpu_time, log_helper
from backend.pipeline.segmentation.constants import (
    DEAD_LETTER_QUEUE_TAG,
    GCS_DOWNLOAD_TIMEOUT_SEC,
)
from backend.pipeline.segmentation.datatypes import (
    AudioClassification,
    ChunkMetadata,
    FeedMetadata,
    FlushRequest,
    PendingPubSubMessage,
    SegmentationDlqOutput,
)
from backend.pipeline.segmentation.options import (
    DataflowSystemOptions,  # noqa: F401
    SegmentationOptions,  # noqa: F401
)
from backend.pipeline.segmentation.storage import (
    acquire_shared_download_executor,
    acquire_shared_gcs_client,
)

# WARNING: Do NOT remove or bypass setup_logging().
# It explicitly configures structured log propagation for the
# Dataflow worker harness. Removing this will cause all worker logs
# to be rendered as DEBUG severity in Cloud Logging.
log_helper.setup_logging()

logger = get_task_logger(
    __name__, {"system": "transcription", "component": "transforms"}
)


def _parse_receipt_ms(
    chunk_proto: ContinuousAudio, element: PubsubMessage
) -> int | None:
    if (
        chunk_proto.HasField("receipt_timestamp")
        and chunk_proto.receipt_timestamp.seconds > 0
    ):
        return (
            chunk_proto.receipt_timestamp.seconds * MS_PER_SECOND
            + chunk_proto.receipt_timestamp.nanos // NANOS_PER_MS
        )
    if element.attributes and "timestamp_ms" in element.attributes:
        try:
            return int(element.attributes["timestamp_ms"])
        except (ValueError, TypeError):
            return None
    return None


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
        self.segmentation_start = Metrics.counter(
            self.__class__, "segmentation_start"
        )
        self.segmentation_error = Metrics.counter(
            self.__class__, "segmentation_error"
        )
        self.data_freshness_ms = Metrics.distribution(
            self.__class__, "data_freshness_ms"
        )

    @override
    def setup(self) -> None:
        """Initializes tracing for the worker."""
        setup_tracing(service_name="segmentation-pipeline")

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
            tracer = get_tracer(__name__)
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
                if (
                    source_type
                    and source_type.lower() not in CONTINUOUS_SOURCE_TYPES
                ):
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
                receipt_ms = _parse_receipt_ms(chunk_proto, element)
                freshness_reference_ms = (
                    receipt_ms if receipt_ms is not None else start_ms
                )

                if freshness_reference_ms is not None:
                    freshness_ms = (
                        int(time.time() * MS_PER_SECOND)
                        - freshness_reference_ms
                    )
                    self.data_freshness_ms.update(freshness_ms)
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
                    receipt_time_ms=receipt_ms,
                )
                logger.debug(
                    "Parsed ContinuousAudio feed_id=%s gcs_uri=%s duration=%dms",
                    feed_id,
                    chunk_proto.gcs_uri,
                    chunk_proto.duration_ms,
                )
                combined_key = f"{feed_id}#{metadata.session_id}"
                outputs.append((combined_key, metadata))
        except Exception as e:
            msg = f"Failed to parse or validate payload: {e}"
            logger.exception(msg)
            self.segmentation_error.inc()
            outputs.append(
                beam.pvalue.TaggedOutput(
                    DEAD_LETTER_QUEUE_TAG,
                    {
                        "error": msg,
                        "attributes": dict(element.attributes or {}),
                    },
                )
            )

        yield from outputs


@beam.typehints.with_input_types(tuple[str, tuple[int, FlushRequest]])
@beam.typehints.with_output_types(tuple[str, tuple[int, PendingPubSubMessage]])
class UploadRawSegmentFn(beam.DoFn):
    """Stateless DoFn to upload PCM audio bytes as a raw WAV file to the GCS staging bucket
    and yield a SegmentedAudio claim-check protobuf message.
    """

    SHARED_GCS_HANDLE = Shared()
    segmentation_success: Counter
    segmentation_error: Counter
    gcs_chunks_downloaded: Counter
    stitched_segments_uploaded: Counter
    download_latency_ms: Distribution
    stitch_latency_ms: Distribution
    post_flush_download_decode_thread_cpu_us: Distribution
    post_flush_stitch_thread_cpu_us: Distribution
    post_flush_encode_thread_cpu_us: Distribution
    post_flush_upload_thread_cpu_us: Distribution
    post_flush_message_thread_cpu_us: Distribution

    def __init__(
        self, staging_audio_bucket: str | None, project_id: str
    ) -> None:
        self.staging_audio_bucket = staging_audio_bucket
        self.project_id = project_id
        self.gcs_client = None
        self._executor = None
        self.post_flush_download_decode_thread_cpu_us = Metrics.distribution(
            self.__class__,
            "post_flush_download_decode_thread_cpu_us",
        )
        self.post_flush_stitch_thread_cpu_us = Metrics.distribution(
            self.__class__, "post_flush_stitch_thread_cpu_us"
        )
        self.post_flush_encode_thread_cpu_us = Metrics.distribution(
            self.__class__, "post_flush_encode_thread_cpu_us"
        )
        self.post_flush_upload_thread_cpu_us = Metrics.distribution(
            self.__class__, "post_flush_upload_thread_cpu_us"
        )
        self.post_flush_message_thread_cpu_us = Metrics.distribution(
            self.__class__, "post_flush_message_thread_cpu_us"
        )

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
        self.gcs_chunks_downloaded = Metrics.counter(
            self.__class__, "gcs_chunks_downloaded"
        )
        self.stitched_segments_uploaded = Metrics.counter(
            self.__class__, "stitched_segments_uploaded"
        )
        self.download_latency_ms = Metrics.distribution(
            self.__class__, "download_latency_ms"
        )
        self.stitch_latency_ms = Metrics.distribution(
            self.__class__, "stitch_latency_ms"
        )
        self.upload_latency_ms = Metrics.distribution(
            self.__class__, "upload_latency_ms"
        )

        self._executor = acquire_shared_download_executor()

    def _pcm_to_flac(self, pcm_bytes: bytes, sample_rate: int) -> bytes:
        audio_arr = np.frombuffer(pcm_bytes, dtype=np.int16)
        flac_io = io.BytesIO()
        sf.write(
            flac_io, audio_arr, sample_rate, format="FLAC", subtype="PCM_16"
        )
        return flac_io.getvalue()

    def _download_contributing_chunks(
        self,
        request: FlushRequest,
        task_logger: logging.LoggerAdapter[Any] | logging.Logger,
        parent_context: otel_context.Context,
    ) -> dict[str, tuple[np.ndarray, int, int]]:
        """Downloads and decodes all contributing chunks in parallel."""
        gcs_client = self.gcs_client
        executor = self._executor
        if not gcs_client or not executor:
            err_msg = "GCS client or thread pool not initialized"
            raise RuntimeError(err_msg)

        decoded_chunks = {}

        def _download_and_decode(
            chunk: Any,
        ) -> tuple[str, tuple[np.ndarray, int, int], int]:
            # Propagate and activate the parent thread's context in this thread
            token = otel_context.attach(parent_context)
            try:
                download_cpu_start_ns = cpu_time.read_thread_cpu_ns()
                task_logger.debug(
                    "[Stateless Stitch] Downloading GCS chunk: %s",
                    chunk.gcs_uri,
                )
                parsed_uri = urllib.parse.urlparse(chunk.gcs_uri)
                bucket_name = parsed_uri.netloc
                blob_name = parsed_uri.path.lstrip("/")

                bucket = gcs_client.bucket(bucket_name)
                blob = bucket.blob(blob_name)

                in_mem_file = io.BytesIO()
                blob.download_to_file(
                    in_mem_file,
                    timeout=GCS_DOWNLOAD_TIMEOUT_SEC,
                )
                in_mem_file.seek(0)

                # Decode FLAC using soundfile (releases the GIL for
                # CPU-intensive loading)
                samples, sr = sf.read(in_mem_file, dtype="int16")
            except Exception as e:
                err_msg = f"Failed downloading chunk {chunk.gcs_uri}: {e}"
                task_logger.exception(err_msg)
                raise RuntimeError(err_msg) from e
            else:
                self.gcs_chunks_downloaded.inc()
                download_cpu_us = cpu_time.elapsed_thread_cpu_us(
                    download_cpu_start_ns
                )
                return (
                    chunk.gcs_uri,
                    (samples, sr, chunk.timestamp_ms),
                    download_cpu_us,
                )
            finally:
                # Clean up the context on this thread
                otel_context.detach(token)

        # Measure GCS download and decoding phase latency
        download_start = time.perf_counter_ns()

        if len(request.contributing_chunks) > 1:
            # Concurrently download via the process-wide shared pool
            futures = {
                executor.submit(_download_and_decode, chunk): chunk
                for chunk in request.contributing_chunks
            }
            try:
                for future in concurrent.futures.as_completed(futures):
                    gcs_uri, val, download_cpu_us = future.result()
                    decoded_chunks[gcs_uri] = val
                    self.post_flush_download_decode_thread_cpu_us.update(
                        download_cpu_us
                    )
            except Exception:
                # Staff-Level defensive purge: immediately cancel all
                # remaining pending download tasks to save network bandwidth
                # and worker resources.
                for f in futures:
                    if not f.done():
                        f.cancel()
                raise
        else:
            # Fallback to sequential to avoid thread context-switching
            # for a single chunk
            for chunk in request.contributing_chunks:
                gcs_uri, val, download_cpu_us = _download_and_decode(chunk)
                decoded_chunks[gcs_uri] = val
                self.post_flush_download_decode_thread_cpu_us.update(
                    download_cpu_us
                )

        download_duration_ms = (
            time.perf_counter_ns() - download_start
        ) // NANOS_PER_MS
        self.download_latency_ms.update(int(download_duration_ms))
        return decoded_chunks

    def _stitch_audio_segments(
        self,
        request: FlushRequest,
        decoded_chunks: dict[str, tuple[np.ndarray, int, int]],
        task_logger: logging.LoggerAdapter[Any] | logging.Logger,
    ) -> bytes:
        """Slices, concatenates, and converts the entire continuous segment time range to FLAC bytes."""
        stitch_start = time.perf_counter_ns()
        stitch_cpu_start_ns = cpu_time.read_thread_cpu_ns()

        segment_start = request.time_range.start_ms
        segment_end = request.time_range.end_ms

        sorted_chunks = sorted(
            request.contributing_chunks, key=attrgetter("timestamp_ms")
        )

        stitched_segments = []
        for chunk in sorted_chunks:
            samples, sr, chunk_start_ms = decoded_chunks[chunk.gcs_uri]
            chunk_duration_ms = int(len(samples) / sr * MS_PER_SECOND)
            chunk_end_ms = chunk_start_ms + chunk_duration_ms

            # Calculate overlap between this chunk and the entire segment time range
            overlap_start = max(segment_start, chunk_start_ms)
            overlap_end = min(segment_end, chunk_end_ms)

            if overlap_start < overlap_end:
                rel_start_samples = int(
                    (overlap_start - chunk_start_ms) * (sr / MS_PER_SECOND)
                )
                rel_end_samples = int(
                    (overlap_end - chunk_start_ms) * (sr / MS_PER_SECOND)
                )
                stitched_segments.append(
                    samples[rel_start_samples:rel_end_samples]
                )

        # Concatenate and convert to FLAC bytes
        if stitched_segments:
            final_pcm_arr = np.concatenate(stitched_segments)
        else:
            task_logger.warning(
                "[Stateless Stitch] No overlapping audio found for time range. Creating silent placeholder."
            )
            final_pcm_arr = np.zeros(
                int(request.sample_rate * 0.1), dtype=np.int16
            )

        final_pcm_bytes = final_pcm_arr.tobytes()
        self.post_flush_stitch_thread_cpu_us.update(
            cpu_time.elapsed_thread_cpu_us(stitch_cpu_start_ns)
        )

        encode_cpu_start_ns = cpu_time.read_thread_cpu_ns()
        flac_bytes = self._pcm_to_flac(final_pcm_bytes, request.sample_rate)
        self.post_flush_encode_thread_cpu_us.update(
            cpu_time.elapsed_thread_cpu_us(encode_cpu_start_ns)
        )

        stitch_duration_ms = (
            time.perf_counter_ns() - stitch_start
        ) // NANOS_PER_MS
        self.stitch_latency_ms.update(int(stitch_duration_ms))
        return flac_bytes

    def _upload_stitched_audio(
        self,
        request: FlushRequest,
        flac_bytes: bytes,
        start_datetime: datetime.datetime,
        task_logger: logging.LoggerAdapter[Any] | logging.Logger,
    ) -> str:
        """Uploads the finalized FLAC audio bytes to GCS staging bucket."""
        gcs_client = self.gcs_client
        if not gcs_client or not self.staging_audio_bucket:
            err_msg = "GCS client or staging bucket not configured"
            raise RuntimeError(err_msg)

        upload_start = time.perf_counter_ns()
        upload_cpu_start_ns = cpu_time.read_thread_cpu_ns()

        flac_path = f"raw_segments/{request.feed_id}/{start_datetime:%Y/%m/%d}/{request.segment_id}.flac"
        bucket = gcs_client.bucket(self.staging_audio_bucket)
        blob = bucket.blob(flac_path)
        blob.upload_from_string(flac_bytes, content_type="audio/flac")
        self.post_flush_upload_thread_cpu_us.update(
            cpu_time.elapsed_thread_cpu_us(upload_cpu_start_ns)
        )
        self.stitched_segments_uploaded.inc()

        upload_duration_ms = (
            time.perf_counter_ns() - upload_start
        ) // 1_000_000
        self.upload_latency_ms.update(int(upload_duration_ms))

        task_logger.info(
            f"[Stateless Stitch] Successfully uploaded stitched FLAC: gs://{self.staging_audio_bucket}/{flac_path}"
        )
        return f"gs://{self.staging_audio_bucket}/{flac_path}"

    def _upload_raw_audio(
        self, request: FlushRequest, start_datetime: datetime.datetime
    ) -> str:
        """Downloads contributing GCS chunks, extracts speech segments, stitches them, and uploads to GCS as FLAC."""
        if not self.gcs_client:
            err_msg = "GCS client not initialized"
            raise RuntimeError(err_msg)
        if not self.staging_audio_bucket:
            err_msg = "staging_audio_bucket is not configured"
            raise ValueError(err_msg)
        if not request.contributing_chunks:
            err_msg = "FlushRequest contains empty contributing_chunks"
            raise ValueError(err_msg)

        task_logger = get_task_logger(
            "transcription-stitcher",
            {"feed_id": request.feed_id, "segment_id": request.segment_id},
        )

        parent_context = otel_context.get_current()

        # 1. Download and decode chunks in parallel
        decoded_chunks = self._download_contributing_chunks(
            request=request,
            task_logger=task_logger,
            parent_context=parent_context,
        )

        # 2. Extract, stitch, and compress segments to FLAC
        flac_bytes = self._stitch_audio_segments(
            request=request,
            decoded_chunks=decoded_chunks,
            task_logger=task_logger,
        )

        # 3. Upload the stitched FLAC to GCS
        return self._upload_stitched_audio(
            request=request,
            flac_bytes=flac_bytes,
            start_datetime=start_datetime,
            task_logger=task_logger,
        )

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
        element: tuple[str, tuple[int, FlushRequest]],
    ) -> Iterator[
        tuple[str, tuple[int, PendingPubSubMessage]] | SegmentationDlqOutput
    ]:
        _key, (seq_num, request) = element
        feed_id = request.feed_id
        session_key = f"{feed_id}#{request.session_id}"
        trace_attrs: dict[str, str] = {}
        if request.traceparent:
            trace_attrs["traceparent"] = request.traceparent
        if request.baggage is not None:
            trace_attrs["baggage"] = str(request.baggage)

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
                message_cpu_start_ns = cpu_time.read_thread_cpu_ns()
                segmented_audio_pb = self._build_segmented_audio_proto(
                    request, gcs_uri
                )

                pubsub_attributes: dict[str, str] = {}
                inject_otel_context(pubsub_attributes)
                serialized_audio = segmented_audio_pb.SerializeToString()
                self.post_flush_message_thread_cpu_us.update(
                    cpu_time.elapsed_thread_cpu_us(message_cpu_start_ns)
                )

                yield (
                    session_key,
                    (
                        seq_num,
                        {
                            "data": serialized_audio,
                            "attributes": pubsub_attributes,
                            "ordering_key": request.feed_id,
                            "is_tombstone": False,
                        },
                    ),
                )
                self.segmentation_success.inc()

        except Exception as e:
            logger.exception(
                "Error uploading raw segment for feed %s",
                feed_id,
            )
            self.segmentation_error.inc()
            yield (
                session_key,
                (
                    seq_num,
                    {
                        "data": b"",
                        "attributes": {},
                        "ordering_key": feed_id,
                        "is_tombstone": True,
                    },
                ),
            )
            yield beam.pvalue.TaggedOutput(
                DEAD_LETTER_QUEUE_TAG,
                {"error": str(e), "feed_id": feed_id},
            )
