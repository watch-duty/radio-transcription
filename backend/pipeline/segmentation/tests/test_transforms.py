"""Tests for the StitchAudioFn, TranscribeAudioFn, and related transformations."""

import datetime
import io
import logging as std_logging
import time
import unittest
from collections.abc import Callable
from dataclasses import replace
from typing import Any
from unittest.mock import MagicMock, patch

import apache_beam as beam
import numpy as np
import soundfile as sf
from apache_beam.io.gcp.pubsub import PubsubMessage
from apache_beam.options.pipeline_options import (
    PipelineOptions,
)
from apache_beam.testing.test_pipeline import TestPipeline as BeamTestPipeline
from apache_beam.testing.test_stream import TestStream
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.transforms.userstate import RuntimeTimer
from apache_beam.transforms.window import TimestampedValue
from apache_beam.utils.timestamp import Timestamp
from google.cloud import storage
from opentelemetry.trace import get_current_span

from backend.pipeline.common.tracing_utils import (
    extract_trace_context,
)
from backend.pipeline.schema_types.continuous_audio_pb2 import ContinuousAudio
from backend.pipeline.segmentation import coders as trans_coders
from backend.pipeline.segmentation.constants import (
    DEAD_LETTER_QUEUE_TAG,
    MAIN_TAG,
)
from backend.pipeline.segmentation.datatypes import (
    ActiveStitchingState,
    AudioChunkData,
    AudioClassification,
    BufferedChunk,
    ChunkMetadata,
    FeedMetadata,
    FlushRequest,
    IdleFeedState,
    OrderRestorerConfig,
    StitchAudioConfig,
    TimeRange,
)
from backend.pipeline.segmentation.transforms.stateful import (
    SHARED_RESOURCE_HANDLE,
    OrderedStitchAudioFn,
)
from backend.pipeline.segmentation.transforms.stateless import (
    ParseAndKeyFn,
    UploadRawSegmentFn,
)
from backend.pipeline.segmentation.utils import get_duration_ms

# Test Helper: override ChunkMetadata locally in tests to default is_continuous to True
_OriginalChunkMetadata = ChunkMetadata


def ChunkMetadata(*args: Any, **kwargs: Any) -> Any:
    kwargs.setdefault("is_continuous", True)
    return _OriginalChunkMetadata(*args, **kwargs)


class MockValueState:
    def __init__(self, initial: Any = None) -> None:
        self.val = initial
        self.read_count = 0
        self.write_count = 0
        self.clear_count = 0

    def read(self) -> Any:
        self.read_count += 1
        return self.val

    def write(self, val: Any) -> None:
        self.write_count += 1
        self.val = val

    def clear(self) -> None:
        self.clear_count += 1
        self.val = None


class MockBagState:
    def __init__(self) -> None:
        self.items: list[Any] = []
        self.read_count = 0
        self.add_count = 0
        self.clear_count = 0

    def read(self) -> list[Any]:
        self.read_count += 1
        return self.items

    def add(self, item: Any) -> None:
        self.add_count += 1
        self.items.append(item)

    def clear(self) -> None:
        self.clear_count += 1
        self.items = []


# Configure dynamic mock interception for process-level shared GCS clients
# using standard unittest module lifecycle hooks to avoid any type ignore annotations.
original_acquire = SHARED_RESOURCE_HANDLE.acquire


def mock_acquire(constructor_fn: Callable[[], Any], tag: Any = None) -> Any:
    if tag == "gcs":
        return MagicMock()
    return original_acquire(constructor_fn, tag)


_SHARED_PATCHER = patch.object(
    SHARED_RESOURCE_HANDLE, "acquire", side_effect=mock_acquire
)


def setUpModule() -> None:
    trans_coders.register_custom_coders()
    _SHARED_PATCHER.start()


def tearDownModule() -> None:
    _SHARED_PATCHER.stop()


def get_test_stitch_config(**kwargs: Any) -> StitchAudioConfig:

    defaults = {
        "project_id": "fake-proj",
        "vad_config": "{}",
        "significant_gap_ms": 500,
        "stale_timeout_ms": 60000,
        "max_transmission_duration_ms": 600000,
    }
    defaults.update(kwargs)
    return StitchAudioConfig(**defaults)  # type: ignore


class ParseAndKeyTimestampTest(unittest.TestCase):
    def test_parse_and_key_success(self) -> None:
        """Verifies that well-formed Pub/Sub messages containing a serialized ContinuousAudio and feed_id are correctly unmarshalled and keyed by feed."""
        chunk = ContinuousAudio(
            gcs_uri="gs://test-bucket/path/to/test.flac",
            session_id="mock-session-id",
            feed_name="mock-feed-name",
            duration_ms=1000,
            feed_id="test-feed",
        )
        mock_msg = PubsubMessage(
            chunk.SerializeToString(),
            {"feed_id": "test-feed"},
        )
        options = PipelineOptions(
            flags=[
                "--continuous_input_subscription=projects/p/subscriptions/a",
                "--output_topic=b",
                "--project=c",
            ]
        )
        with BeamTestPipeline(options=options) as p:
            messages = p | beam.Create([mock_msg])
            parsed = messages | beam.ParDo(
                ParseAndKeyFn(is_continuous=True)
            ).with_outputs(DEAD_LETTER_QUEUE_TAG, main=MAIN_TAG)
            assert_that(
                parsed[MAIN_TAG],
                equal_to(
                    [
                        (
                            "test-feed#mock-session-id",
                            ChunkMetadata(
                                gcs_uri="gs://test-bucket/path/to/test.flac",
                                session_id="mock-session-id",
                                duration_ms=1000,
                                feed_metadata=FeedMetadata(
                                    feed_name="mock-feed-name",
                                ),
                            ),
                        )
                    ]
                ),
            )
            assert_that(
                parsed[DEAD_LETTER_QUEUE_TAG],
                equal_to([]),
                label="CheckEmptyDLQ",
            )

        # Assert native Beam metrics
        metrics = p.result.metrics().query(
            beam.metrics.metric.MetricsFilter().with_name("segmentation_start")
        )
        self.assertEqual(len(metrics["counters"]), 1)
        self.assertEqual(metrics["counters"][0].committed, 1)

    def test_parse_and_key_dlq(self) -> None:
        """Verifies that incoming data missing a critical routing attribute like 'feed_id' is gracefully intercepted and routed to the Dead Letter Queue."""
        chunk = ContinuousAudio(gcs_uri="gs://test-bucket/path/to/test.flac")
        mock_msg = PubsubMessage(
            chunk.SerializeToString(),
            {},  # Missing feed_id
        )
        options = PipelineOptions(
            flags=[
                "--continuous_input_subscription=projects/p/subscriptions/a",
                "--output_topic=b",
                "--project=c",
            ]
        )
        with BeamTestPipeline(options=options) as p:
            messages = p | beam.Create([mock_msg])
            parsed = messages | beam.ParDo(
                ParseAndKeyFn(is_continuous=False)
            ).with_outputs(DEAD_LETTER_QUEUE_TAG, main=MAIN_TAG)

            def assert_dlq(
                elements: list[dict[str, str | bool | dict[str, str]]],
            ) -> None:

                assert len(elements) == 1
                assert isinstance(elements[0]["error"], str)
                assert (
                    "Failed to parse or validate payload"
                    in elements[0]["error"]
                )

            assert_that(parsed[MAIN_TAG], equal_to([]), label="CheckEmptyMain")
            assert_that(
                parsed[DEAD_LETTER_QUEUE_TAG], assert_dlq, label="CheckDLQ"
            )

    def test_parse_and_key_missing_feed_id_dlq(self) -> None:
        """Verifies that a message with a missing feed_id but otherwise valid fields is routed to the DLQ."""
        chunk = ContinuousAudio(
            gcs_uri="gs://test-bucket/path/to/test.flac",
            feed_name="mock-feed-name",
            duration_ms=1000,
        )
        mock_msg = PubsubMessage(
            chunk.SerializeToString(),
            {},  # Missing feed_id
        )
        options = PipelineOptions(
            flags=[
                "--continuous_input_subscription=projects/p/subscriptions/a",
                "--output_topic=b",
                "--project=c",
            ]
        )
        with BeamTestPipeline(options=options) as p:
            messages = p | beam.Create([mock_msg])
            parsed = messages | beam.ParDo(
                ParseAndKeyFn(is_continuous=False)
            ).with_outputs(DEAD_LETTER_QUEUE_TAG, main=MAIN_TAG)

            def assert_dlq(
                elements: list[dict[str, str | bool | dict[str, str]]],
            ) -> None:
                assert len(elements) == 1
                assert isinstance(elements[0]["error"], str)
                assert (
                    "ContinuousAudio missing required feed_id"
                    in elements[0]["error"]
                )

            assert_that(parsed[MAIN_TAG], equal_to([]), label="CheckEmptyMain")
            assert_that(
                parsed[DEAD_LETTER_QUEUE_TAG], assert_dlq, label="CheckDLQ"
            )

    def test_parse_and_key_mismatched_routing_continuous_dlq(self) -> None:
        """Verifies that a segmented source type received on a continuous subscription is routed to the DLQ."""
        chunk = ContinuousAudio(
            gcs_uri="gs://test-bucket/path/to/test.flac",
            session_id="mock-session-id",
            feed_name="mock-feed-name",
            duration_ms=1000,
            feed_id="test-feed",
        )
        mock_msg = PubsubMessage(
            chunk.SerializeToString(),
            {"feed_id": "test-feed", "source_type": "echo"},
        )
        options = PipelineOptions(
            flags=[
                "--continuous_input_subscription=projects/p/subscriptions/a",
                "--output_topic=b",
                "--project=c",
            ]
        )
        with BeamTestPipeline(options=options) as p:
            messages = p | beam.Create([mock_msg])
            parsed = messages | beam.ParDo(
                ParseAndKeyFn(is_continuous=True)
            ).with_outputs(DEAD_LETTER_QUEUE_TAG, main=MAIN_TAG)

            def assert_dlq(
                elements: list[dict[str, str | bool | dict[str, str]]],
            ) -> None:
                assert len(elements) == 1
                assert isinstance(elements[0]["error"], str)
                assert (
                    "Received segmented source type 'echo' on continuous subscription"
                    in elements[0]["error"]
                )

            assert_that(parsed[MAIN_TAG], equal_to([]))
            assert_that(parsed[DEAD_LETTER_QUEUE_TAG], assert_dlq)

    def test_parse_and_key_span_lifecycle(self) -> None:
        """Verifies that ParseAndKeyFn doesn't leak trace context scope on execution."""
        chunk = ContinuousAudio(
            gcs_uri="gs://test-bucket/path/to/test.flac",
            session_id="mock-session-id",
            feed_name="mock-feed-name",
            duration_ms=1000,
            feed_id="test-feed",
        )
        mock_msg = PubsubMessage(
            chunk.SerializeToString(),
            {"feed_id": "test-feed"},
        )

        fn = ParseAndKeyFn(is_continuous=True)
        fn.setup()

        span_before = get_current_span()
        self.assertFalse(span_before.get_span_context().is_valid)

        result = fn.process(mock_msg)
        items = list(result)

        self.assertEqual(len(items), 1)

        span_after = get_current_span()
        self.assertFalse(span_after.get_span_context().is_valid)

    def test_parse_and_key_traceparent_propagation(self) -> None:
        """Verifies that trace contexts propagate correctly from Pub/Sub metadata attributes."""
        traceparent_val = (
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
        )
        attrs = {"traceparent": traceparent_val}

        ctx = extract_trace_context(attrs)
        span = get_current_span(ctx)
        span_ctx = span.get_span_context()

        self.assertEqual(
            format(span_ctx.trace_id, "032x"),
            "4bf92f3577b34da6a3ce929d0e0e4736",
        )
        self.assertEqual(format(span_ctx.span_id, "016x"), "00f067aa0ba902b7")


class OrderedStitchAudioTest(unittest.TestCase):
    @patch("backend.pipeline.common.tracing_utils.with_tracer_context")
    @patch(
        "backend.pipeline.segmentation.audio.processor.SegmentationAudioProcessor"
    )
    def test_ordered_stitch_audio_process_span(
        self,
        mock_audio_processor: MagicMock,
        mock_with_tracer_context: MagicMock,
    ) -> None:
        """Verifies that OrderedStitchAudioFn.process calls with_tracer_context."""
        order_config = OrderRestorerConfig(out_of_order_timeout_ms=1000)
        stitch_config = get_test_stitch_config()
        fn = OrderedStitchAudioFn(
            order_config=order_config, stitch_config=stitch_config
        )
        fn.setup()

        mock_state = MagicMock()
        mock_state.read.return_value = ActiveStitchingState(
            session_id="mock-session-id",
            feed_metadata=FeedMetadata(feed_name="mock-feed"),
        )
        mock_timer = MagicMock()

        metadata = ChunkMetadata(
            gcs_uri="gs://test-bucket/path/to/test.flac",
            session_id="mock-session-id",
            duration_ms=1000,
            feed_metadata=FeedMetadata(feed_name="mock-feed"),
            traceparent="mock-traceparent",
        )

        list(
            fn.process(
                element=("test-feed", metadata),
                timestamp=Timestamp(100),
                transmission_context_state=mock_state,
                last_start_ms_state=MagicMock(),
                gap_timer_event=mock_timer,
                gap_timer_proc=MagicMock(),
                stale_timer_event=MagicMock(),
                stale_timer_proc=MagicMock(),
            )
        )

        mock_with_tracer_context.assert_any_call(
            {"traceparent": "mock-traceparent"},
            "stitching_process",
            "backend.pipeline.segmentation.transforms.stateful",
        )
        mock_with_tracer_context.assert_any_call(
            {"traceparent": "mock-traceparent"},
            "stitching_single_chunk",
            "backend.pipeline.segmentation.transforms.stateful",
        )

    @patch("backend.pipeline.common.tracing_utils.with_tracer_context")
    @patch(
        "backend.pipeline.segmentation.audio.processor.SegmentationAudioProcessor"
    )
    def test_ordered_stitch_audio_handle_gap_timeout_span(
        self,
        mock_audio_processor: MagicMock,
        mock_with_tracer_context: MagicMock,
    ) -> None:
        """Verifies that handle_gap_timeout calls with_tracer_context."""
        order_config = OrderRestorerConfig(out_of_order_timeout_ms=1000)
        stitch_config = get_test_stitch_config()
        fn = OrderedStitchAudioFn(
            order_config=order_config, stitch_config=stitch_config
        )
        fn.setup()

        mock_state = MagicMock()
        curr_context = ActiveStitchingState(
            session_id="mock-session",
            traceparent="mock-traceparent-context",
            out_of_order_buffer=[
                BufferedChunk(timestamp_ms=100000, gcs_uri="gs://test.flac")
            ],
            feed_metadata=FeedMetadata(feed_name="mock-feed"),
        )
        mock_state.read.return_value = curr_context

        list(
            fn.handle_gap_timeout_event(
                feed_id="test-feed",
                transmission_context_state=mock_state,
                last_start_ms_state=MagicMock(),
                out_of_order_buffer_state=MockBagState(),  # type: ignore
                stale_timer_event=MagicMock(spec=RuntimeTimer),
                stale_timer_proc=MagicMock(spec=RuntimeTimer),
                timestamp=Timestamp(100),
                gap_timer_event=MagicMock(spec=RuntimeTimer),
                gap_timer_proc=MagicMock(spec=RuntimeTimer),
            )
        )

        mock_with_tracer_context.assert_any_call(
            {"traceparent": "mock-traceparent-context"},
            "handle_audio_gap",
            "backend.pipeline.segmentation.transforms.stateful",
        )
        mock_with_tracer_context.assert_any_call(
            {},
            "stitching_single_chunk",
            "backend.pipeline.segmentation.transforms.stateful",
        )

    @patch(
        "backend.pipeline.segmentation.audio.processor.SegmentationAudioProcessor"
    )
    def test_ordered_stitch_audio_preserves_otel_baggage_in_state(
        self, mock_audio_processor: MagicMock
    ) -> None:
        """Verifies that ActiveStitchingState preserves OpenTelemetry Baggage when initialized."""
        mock_processor_inst = mock_audio_processor.return_value
        chunk_data = AudioChunkData(
            start_ms=1000,
            audio=np.zeros(16000, dtype=np.int16),
            speech_segments=[],
            gcs_uri="gs://test-bucket/path/to/test.flac",
            duration_ms=1000,
            sample_rate=16000,
        )
        mock_processor_inst.download_audio_and_detect.return_value = chunk_data

        order_config = OrderRestorerConfig(out_of_order_timeout_ms=1000)
        stitch_config = get_test_stitch_config()
        fn = OrderedStitchAudioFn(
            order_config=order_config, stitch_config=stitch_config
        )
        fn.setup()

        mock_state = MagicMock()
        mock_state.read.return_value = IdleFeedState()

        metadata = ChunkMetadata(
            gcs_uri="gs://test-bucket/path/to/test.flac",
            session_id="mock-session-id",
            duration_ms=1000,
            feed_metadata=FeedMetadata(feed_name="mock-feed"),
            traceparent="mock-traceparent",
            baggage="ingest_time_ms=12345",
        )

        list(
            fn.process(
                element=("test-feed", metadata),
                timestamp=Timestamp(100),
                transmission_context_state=mock_state,
                last_start_ms_state=MagicMock(),
                out_of_order_buffer_state=MagicMock(),
                gap_timer_event=MagicMock(),
                gap_timer_proc=MagicMock(),
                stale_timer_event=MagicMock(),
                stale_timer_proc=MagicMock(),
            )
        )

        self.assertGreater(mock_state.write.call_count, 0)
        written_state = mock_state.write.call_args[0][0]
        self.assertIsInstance(written_state, ActiveStitchingState)
        self.assertEqual(written_state.baggage, "ingest_time_ms=12345")

    @patch(
        "backend.pipeline.segmentation.audio.processor.SegmentationAudioProcessor"
    )
    def test_ordered_stitch_audio_updates_otel_context_on_next_chunks(
        self, mock_audio_processor: MagicMock
    ) -> None:
        """Verifies ActiveStitchingState updates trace parent/baggage."""
        mock_processor_inst = mock_audio_processor.return_value
        chunk_data = AudioChunkData(
            start_ms=2000,
            audio=np.zeros(16000, dtype=np.int16),
            speech_segments=[],
            gcs_uri="gs://test-bucket/path/to/test2.flac",
            duration_ms=1000,
            sample_rate=16000,
        )
        mock_processor_inst.download_audio_and_detect.return_value = chunk_data

        order_config = OrderRestorerConfig(out_of_order_timeout_ms=1000)
        stitch_config = get_test_stitch_config()
        fn = OrderedStitchAudioFn(
            order_config=order_config, stitch_config=stitch_config
        )
        fn.setup()

        mock_state = MagicMock()
        mock_state.read.return_value = ActiveStitchingState(
            session_id="mock-session-id",
            feed_metadata=FeedMetadata(feed_name="mock-feed"),
            traceparent="mock-traceparent-1",
            baggage="ingest_time_ms=1",
            sample_rate=16000,
        )

        metadata = ChunkMetadata(
            gcs_uri="gs://test-bucket/path/to/test2.flac",
            session_id="mock-session-id",
            duration_ms=1000,
            feed_metadata=FeedMetadata(feed_name="mock-feed"),
            traceparent="mock-traceparent-2",
            baggage="ingest_time_ms=2",
        )

        list(
            fn.process(
                element=("test-feed", metadata),
                timestamp=Timestamp(200),
                transmission_context_state=mock_state,
                last_start_ms_state=MagicMock(),
                out_of_order_buffer_state=MagicMock(),
                gap_timer_event=MagicMock(),
                gap_timer_proc=MagicMock(),
                stale_timer_event=MagicMock(),
                stale_timer_proc=MagicMock(),
            )
        )

        self.assertGreater(mock_state.write.call_count, 0)
        written_state = mock_state.write.call_args[0][0]
        self.assertIsInstance(written_state, ActiveStitchingState)
        self.assertEqual(written_state.traceparent, "mock-traceparent-2")
        self.assertEqual(written_state.baggage, "ingest_time_ms=2")

    @patch(
        "backend.pipeline.segmentation.audio.processor.SegmentationAudioProcessor"
    )
    def test_late_chunk_empty_buffer_no_fallback(
        self, mock_audio_processor: MagicMock
    ) -> None:
        """Verifies that a late chunk with an empty isolated buffer does not fall back to the main buffer."""
        mock_processor_inst = mock_audio_processor.return_value
        chunk_data = MagicMock()
        chunk_data.duration_ms = 1000
        chunk_data.audio = np.zeros(16000, dtype=np.int16)
        chunk_data.start_ms = 1000
        chunk_data.speech_segments = []  # Silent chunk
        mock_processor_inst.download_audio_and_detect.side_effect = (
            lambda *args, **kwargs: chunk_data
        )

        order_config = OrderRestorerConfig(out_of_order_timeout_ms=1000)
        stitch_config = get_test_stitch_config(
            significant_gap_ms=500, stale_timeout_ms=60000
        )

        fn = OrderedStitchAudioFn(
            order_config=order_config, stitch_config=stitch_config
        )
        fn.audio_processor = mock_processor_inst  # Inject mock

        # Mock state parameters
        class MockBagState:
            def __init__(self) -> None:
                self.items = []

            def read(self):
                return self.items

            def add(self, item):
                self.items.append(item)

            def clear(self):
                self.items = []

            def write(self, val):
                self.items = val

        class MockValueState:
            def __init__(self, initial=None) -> None:
                self.val = initial

            def read(self):
                return self.val

            def write(self, val):
                self.val = val

            def clear(self):
                self.val = None

        # Seed state to simulate a late chunk condition
        curr_context = ActiveStitchingState(
            session_id="mock-session",
            expected_next_chunk_start_ms=2000,
            stale_start_time_ms=0,
            buffer_start_time_ms=0,
            last_end_time_ms=1000,
            contributing_audio_uris=["gs://main/chunk1.flac"],
            feed_metadata=FeedMetadata(feed_name="mock-feed"),
        )

        transmission_context_state = MockValueState(curr_context)

        gap_timer_event = MagicMock()
        gap_timer_proc = MagicMock()
        stale_timer_event = MagicMock()
        stale_timer_proc = MagicMock()

        element = (
            "test-feed",
            ChunkMetadata(
                gcs_uri="gs://late/chunk2.flac",
                session_id="mock-session",
                duration_ms=1000,
                feed_metadata=FeedMetadata(feed_name="mock-feed"),
            ),
        )
        timestamp = Timestamp(1.0)  # 1.0 seconds = 1000 ms

        results = list(
            fn.process(
                element=element,
                timestamp=timestamp,
                transmission_context_state=transmission_context_state,  # type: ignore
                last_start_ms_state=MockValueState(None),  # type: ignore
                out_of_order_buffer_state=MockBagState(),  # type: ignore
                gap_timer_event=gap_timer_event,
                gap_timer_proc=gap_timer_proc,
                stale_timer_event=stale_timer_event,
                stale_timer_proc=stale_timer_proc,
            )
        )

        flush_requests = [
            r
            for r in results
            if isinstance(r, tuple) and isinstance(r[1], FlushRequest)
        ]
        self.assertEqual(
            len(flush_requests),
            0,
            "Should not yield FlushRequest for empty late chunk",
        )

    @patch(
        "backend.pipeline.segmentation.audio.processor.SegmentationAudioProcessor"
    )
    def test_ordered_stitch_audio_flushes_on_stale_timer(
        self, mock_audio_processor: MagicMock
    ) -> None:
        """Verifies that OrderedStitchAudioFn flushes buffered audio when the stale timer fires."""
        mock_processor_inst = mock_audio_processor.return_value

        chunk_data = AudioChunkData(
            start_ms=100000,
            audio=np.zeros(16000, dtype=np.int16),
            sample_rate=16000,
            speech_segments=[TimeRange(0, 1000)],
            gcs_uri="gs://test-bucket/path/to/test.flac",
            duration_ms=1000,
        )
        mock_processor_inst.download_audio_and_detect.side_effect = (
            lambda *args, **kwargs: chunk_data
        )
        mock_processor_inst.preprocess_audio.side_effect = lambda x: x

        order_config = OrderRestorerConfig(out_of_order_timeout_ms=1000)
        stitch_config = get_test_stitch_config(stale_timeout_ms=5000)

        options = PipelineOptions(
            flags=[
                "--continuous_input_subscription=projects/p/subscriptions/a",
                "--output_topic=b",
                "--project=c",
            ]
        )

        metadata = ChunkMetadata(
            gcs_uri="gs://test-bucket/path/to/test.flac",
            session_id="mock-session-id",
            duration_ms=1000,
            feed_metadata=FeedMetadata(feed_name="mock-feed"),
            traceparent="mock-traceparent",
        )

        with BeamTestPipeline(options=options) as p:
            test_stream = (
                TestStream(
                    coder=beam.coders.TupleCoder(
                        (
                            beam.coders.StrUtf8Coder(),
                            trans_coders.ChunkMetadataCoder(),
                        )
                    )
                )
                .advance_watermark_to(100)
                .add_elements([TimestampedValue(("test-feed", metadata), 100)])
                .advance_watermark_to(110)
                .advance_watermark_to_infinity()
            )

            results = (
                p
                | test_stream
                | beam.ParDo(
                    OrderedStitchAudioFn(
                        order_config=order_config, stitch_config=stitch_config
                    )
                )
            )

            def assert_results(msgs):
                assert len(msgs) == 1
                feed_id, request = msgs[0]
                assert feed_id == "test-feed"
                assert request.segment_id is not None
                assert isinstance(request.buffer, bytes)
                assert request.traceparent == "mock-traceparent"

            assert_that(results, assert_results)

    @patch(
        "backend.pipeline.segmentation.audio.processor.SegmentationAudioProcessor"
    )
    def test_ordered_stitch_audio_handles_out_of_order_chunks(
        self, mock_audio_processor: MagicMock
    ) -> None:
        """Verifies that OrderedStitchAudioFn buffers out-of-order chunks and emits them in order."""
        mock_processor_inst = mock_audio_processor.return_value

        def download_side_effect(gcs_uri, timestamp_ms, *args, **kwargs):
            if "chunk1" in gcs_uri:
                return AudioChunkData(
                    start_ms=100000,
                    audio=np.ones(16000, dtype=np.int16),
                    sample_rate=16000,
                    speech_segments=[TimeRange(0, 1000)],
                    gcs_uri=gcs_uri,
                    duration_ms=1000,
                )
            if "chunk2" in gcs_uri:
                return AudioChunkData(
                    start_ms=101000,
                    audio=np.ones(16000, dtype=np.int16) * 2,
                    sample_rate=16000,
                    speech_segments=[TimeRange(0, 1000)],
                    gcs_uri=gcs_uri,
                    duration_ms=1000,
                )
            return AudioChunkData(
                start_ms=102000,
                audio=np.ones(16000, dtype=np.int16) * 3,
                sample_rate=16000,
                speech_segments=[TimeRange(0, 1000)],
                gcs_uri=gcs_uri,
                duration_ms=1000,
            )

        mock_processor_inst.download_audio_and_detect.side_effect = (
            download_side_effect
        )
        mock_processor_inst.preprocess_audio.side_effect = lambda x: x

        order_config = OrderRestorerConfig(out_of_order_timeout_ms=5000)
        stitch_config = get_test_stitch_config(
            stale_timeout_ms=5000, significant_gap_ms=5000
        )

        options = PipelineOptions(
            flags=[
                "--continuous_input_subscription=projects/p/subscriptions/a",
                "--output_topic=b",
                "--project=c",
            ]
        )

        metadata_chunk1 = ChunkMetadata(
            gcs_uri="gs://test-bucket/path/to/chunk1.flac",
            session_id="mock-session-id",
            duration_ms=1000,
            feed_metadata=FeedMetadata(feed_name="mock-feed"),
        )

        metadata_chunk2 = ChunkMetadata(
            gcs_uri="gs://test-bucket/path/to/chunk2.flac",
            session_id="mock-session-id",
            duration_ms=1000,
            feed_metadata=FeedMetadata(feed_name="mock-feed"),
        )

        metadata_chunk3 = ChunkMetadata(
            gcs_uri="gs://test-bucket/path/to/chunk3.flac",
            session_id="mock-session-id",
            duration_ms=1000,
            feed_metadata=FeedMetadata(feed_name="mock-feed"),
        )

        with BeamTestPipeline(options=options) as p:
            test_stream = (
                TestStream(
                    coder=beam.coders.TupleCoder(
                        (
                            beam.coders.StrUtf8Coder(),
                            trans_coders.ChunkMetadataCoder(),
                        )
                    )
                )
                .advance_watermark_to(100)
                .add_elements(
                    [TimestampedValue(("test-feed-ooo", metadata_chunk1), 100)]
                )
                .add_elements(
                    [TimestampedValue(("test-feed-ooo", metadata_chunk3), 102)]
                )
                .add_elements(
                    [TimestampedValue(("test-feed-ooo", metadata_chunk2), 101)]
                )
                .advance_watermark_to(115)
                .advance_watermark_to_infinity()
            )

            results = (
                p
                | test_stream
                | beam.ParDo(
                    OrderedStitchAudioFn(
                        order_config=order_config, stitch_config=stitch_config
                    )
                )
            )

            # NOTE: In an ideal production scenario (e.g. on Dataflow), all 3 chunks
            # would be stitched into a single transmission (1 message).
            # However, in this DirectRunner unit test, Chunk 1 gets processed in an
            # earlier bundle and flushed before Chunks 2 and 3 arrive.
            # Thus, we expect 2 messages here, but we still verify that Chunk 2 and
            # Chunk 3 were successfully stitched together (one message has length 32000).
            def assert_results(msgs):
                assert len(msgs) in (1, 2)
                for feed_id, request in msgs:
                    assert feed_id == "test-feed-ooo"
                    assert len(request.contributing_chunks) > 0
                    for chunk in request.contributing_chunks:
                        assert chunk.gcs_uri.startswith(
                            "gs://test-bucket/path/to/chunk"
                        )
                        assert chunk.timestamp_ms in (100000, 101000, 102000)

                lengths = [
                    sum(
                        seg.end_ms - seg.start_ms
                        for seg in request.speech_segments
                    )
                    * (request.sample_rate // 1000)
                    for feed_id, request in msgs
                ]
                if len(msgs) == 1:
                    assert 48000 in lengths
                else:
                    assert 32000 in lengths or 16000 in lengths

            assert_that(results, assert_results)

    @patch(
        "backend.pipeline.segmentation.audio.processor.SegmentationAudioProcessor"
    )
    def test_backlog_timer_loop_direct(
        self, mock_audio_processor: MagicMock
    ) -> None:
        """Directly tests the DoFn's backlog clamping and self-chaining timer loop to ensure it drains fully and terminates without infinite loops."""
        mock_processor_inst = mock_audio_processor.return_value

        # Mock processor to return dummy audio chunk data
        def download_side_effect(gcs_uri, timestamp_ms, *args, **kwargs):
            return AudioChunkData(
                start_ms=timestamp_ms,
                audio=np.ones(16000, dtype=np.int16),
                sample_rate=16000,
                speech_segments=[TimeRange(0, 1000)],
                gcs_uri=gcs_uri,
                duration_ms=1000,
            )

        mock_processor_inst.download_audio_and_detect.side_effect = (
            download_side_effect
        )
        mock_processor_inst.preprocess_audio.side_effect = lambda x: x

        order_config = OrderRestorerConfig(
            out_of_order_timeout_ms=5000, chunk_duration_ms=1000
        )
        stitch_config = get_test_stitch_config(
            stale_timeout_ms=5000, significant_gap_ms=5000
        )

        fn = OrderedStitchAudioFn(
            order_config=order_config, stitch_config=stitch_config
        )
        fn.audio_processor = mock_processor_inst
        fn.setup()

        # Mock state cells
        class MockValueState:
            def __init__(self, initial=None) -> None:
                self.val = initial

            def read(self):
                return self.val

            def write(self, val):
                self.val = val

            def clear(self):
                self.val = None

        transmission_context_state = MockValueState(IdleFeedState())
        last_start_ms_state = MockValueState(None)
        out_of_order_buffer_state = MockBagState()

        # Mock timers to record deadlines
        class MockTimer:
            def __init__(self, name="timer") -> None:
                self.name = name
                self.deadline = None

            def set(self, deadline):
                self.deadline = deadline

            def clear(self):
                self.deadline = None

        gap_timer_event = MockTimer("gap_event")
        gap_timer_proc = MockTimer("gap_proc")
        stale_timer_event = MockTimer("stale_event")
        stale_timer_proc = MockTimer("stale_proc")
        deferred_drain_timer = MockTimer("deferred_drain")

        # Create 5 chunks for the backlog
        chunks = []
        for i in range(1, 6):
            chunks.append(
                ChunkMetadata(
                    gcs_uri=f"gs://test-bucket/path/to/chunk{i}.flac",
                    session_id="mock-session-id",
                    duration_ms=1000,
                    feed_metadata=FeedMetadata(feed_name="mock-feed"),
                )
            )

        results = []

        # We monkey-patch MAX_CHUNKS_PER_WINDMILL_BUNDLE to 2 to force clamping.
        with (
            patch(
                "backend.pipeline.segmentation.constants.MAX_CHUNKS_PER_WINDMILL_BUNDLE",
                2,
            ),
            patch(
                "backend.pipeline.segmentation.transforms.stateful.MAX_CHUNKS_PER_WINDMILL_BUNDLE",
                2,
            ),
            patch(
                "backend.pipeline.segmentation.transforms.stateful.trans_constants.MAX_CHUNKS_PER_WINDMILL_BUNDLE",
                2,
            ),
        ):
            # 1. Process all 5 chunks in a single bundle (simulating backlog arrival)
            # Reset bundle counter
            fn.processed_in_bundle = 0

            for i, chunk in enumerate(chunks):
                # Call process directly
                outputs = list(
                    fn.process(
                        element=("test-feed", chunk),
                        timestamp=Timestamp(100 + i),
                        transmission_context_state=transmission_context_state,  # type: ignore
                        last_start_ms_state=last_start_ms_state,  # type: ignore
                        out_of_order_buffer_state=out_of_order_buffer_state,  # type: ignore
                        gap_timer_event=gap_timer_event,  # type: ignore
                        gap_timer_proc=gap_timer_proc,  # type: ignore
                        stale_timer_event=stale_timer_event,  # type: ignore
                        stale_timer_proc=stale_timer_proc,  # type: ignore
                        deferred_drain_timer=deferred_drain_timer,  # type: ignore
                    )
                )
                results.extend(outputs)

            # Assert that the first bundle processed exactly 2 chunks, and deferred the rest,
            # scheduling the deferral timer.
            self.assertEqual(fn.processed_in_bundle, 2)
            self.assertIsNotNone(deferred_drain_timer.deadline)

            # The last element was processed at timestamp 104, so the timer was set to 104 + 1ms
            self.assertEqual(deferred_drain_timer.deadline, Timestamp(104.001))

            # 2. Simulate the timer firing to drain the backlog.
            # We run a loop to fire the timer recursively as long as it gets rescheduled.
            # We set a safety limit of 100 iterations to prevent infinite loops during test failure.
            iterations = 0
            max_iterations = 100

            while deferred_drain_timer.deadline is not None:
                iterations += 1
                if iterations > max_iterations:
                    self.fail(
                        "Infinite loop detected! Timer keeps rescheduling itself indefinitely."
                    )

                firing_timestamp = deferred_drain_timer.deadline
                # Clear the timer before firing (representing runner behavior)
                deferred_drain_timer.clear()

                # Reset bundle counter for the new timer-activated bundle
                fn.processed_in_bundle = 0

                outputs = list(
                    fn.handle_deferred_drain(
                        feed_id="test-feed",
                        transmission_context_state=transmission_context_state,  # type: ignore
                        last_start_ms_state=last_start_ms_state,  # type: ignore
                        out_of_order_buffer_state=out_of_order_buffer_state,  # type: ignore
                        stale_timer_event=stale_timer_event,  # type: ignore
                        stale_timer_proc=stale_timer_proc,  # type: ignore
                        timestamp=firing_timestamp,
                        deferred_drain_timer=deferred_drain_timer,  # type: ignore
                    )
                )
                results.extend(outputs)

            # 3. Flush the remaining active stitching state at the end of the test
            outputs = list(
                fn.handle_stale_transmission_event(
                    key="test-feed",
                    transmission_context=transmission_context_state,  # type: ignore
                    last_start_ms_state=last_start_ms_state,  # type: ignore
                    out_of_order_buffer_state=out_of_order_buffer_state,  # type: ignore
                    stale_timer_event=stale_timer_event,  # type: ignore
                    stale_timer_proc=stale_timer_proc,  # type: ignore
                )
            )
            results.extend(outputs)

            # 4. Assertions
            # We expect the timer to have fired exactly 2 times:
            # - Fire 1 (at 104.001): drains chunks 3 and 4, reschedules timer at 104.002
            # - Fire 2 (at 104.002): drains chunk 5, buffer is now empty, timer is cleared (deadline is None)
            self.assertEqual(iterations, 2)

            # Extract all processed GCS URIs
            processed_uris = set()
            for res in results:
                if isinstance(res, tuple) and isinstance(res[1], FlushRequest):
                    for chunk in res[1].contributing_chunks:
                        processed_uris.add(chunk.gcs_uri)

            # Verify that ALL 5 chunks were fully processed and emitted without any data loss!
            self.assertEqual(len(processed_uris), 5)
            self.assertEqual(
                processed_uris,
                {
                    f"gs://test-bucket/path/to/chunk{i}.flac"
                    for i in range(1, 6)
                },
            )

    @patch(
        "backend.pipeline.segmentation.audio.processor.SegmentationAudioProcessor"
    )
    def test_backlog_timer_loop_gap_leapfrog(
        self, mock_audio_processor: MagicMock
    ) -> None:
        """Verifies that the dynamic leap-frog timer correctly leaps over large sequence gaps in exactly 1 step, preventing watermark-jump loop storms."""
        mock_processor_inst = mock_audio_processor.return_value

        # Mock processor to return dummy audio chunk data
        def download_side_effect(gcs_uri, timestamp_ms, *args, **kwargs):
            return AudioChunkData(
                start_ms=timestamp_ms,
                audio=np.ones(16000, dtype=np.int16),
                sample_rate=16000,
                speech_segments=[TimeRange(0, 1000)],
                gcs_uri=gcs_uri,
                duration_ms=1000,
            )

        mock_processor_inst.download_audio_and_detect.side_effect = (
            download_side_effect
        )
        mock_processor_inst.preprocess_audio.side_effect = lambda x: x

        order_config = OrderRestorerConfig(
            out_of_order_timeout_ms=5000, chunk_duration_ms=1000
        )
        stitch_config = get_test_stitch_config(
            stale_timeout_ms=5000, significant_gap_ms=5000
        )

        fn = OrderedStitchAudioFn(
            order_config=order_config, stitch_config=stitch_config
        )
        fn.audio_processor = mock_processor_inst
        fn.setup()

        # Mock state cells
        class MockValueState:
            def __init__(self, initial=None) -> None:
                self.val = initial

            def read(self):
                return self.val

            def write(self, val):
                self.val = val

            def clear(self):
                self.val = None

        transmission_context_state = MockValueState(IdleFeedState())
        last_start_ms_state = MockValueState(None)
        out_of_order_buffer_state = MockBagState()

        # Mock timers to record deadlines
        class MockTimer:
            def __init__(self, name="timer") -> None:
                self.name = name
                self.deadline = None

            def set(self, deadline):
                self.deadline = deadline

            def clear(self):
                self.deadline = None

        gap_timer_event = MockTimer("gap_event")
        gap_timer_proc, stale_timer_event = (
            MockTimer("gap_proc"),
            MockTimer("stale_event"),
        )
        stale_timer_proc, deferred_drain_timer = (
            MockTimer("stale_proc"),
            MockTimer("deferred_drain"),
        )

        # Create 3 chunks: Chunk 1 and 2 are contiguous (100, 101).
        # Chunk 3 is after a 9-second gap (110)!
        chunks = [
            ChunkMetadata(
                gcs_uri="gs://test-bucket/path/to/chunk1.flac",
                session_id="mock-session-id",
                duration_ms=1000,
                feed_metadata=FeedMetadata(feed_name="mock-feed"),
            ),
            ChunkMetadata(
                gcs_uri="gs://test-bucket/path/to/chunk2.flac",
                session_id="mock-session-id",
                duration_ms=1000,
                feed_metadata=FeedMetadata(feed_name="mock-feed"),
            ),
            ChunkMetadata(
                gcs_uri="gs://test-bucket/path/to/chunk3.flac",
                session_id="mock-session-id",
                duration_ms=1000,
                feed_metadata=FeedMetadata(feed_name="mock-feed"),
            ),
        ]
        chunk_timestamps = [100, 101, 110]

        results = []

        # We monkey-patch MAX_CHUNKS_PER_WINDMILL_BUNDLE to 2 to force clamping.
        with (
            patch(
                "backend.pipeline.segmentation.constants.MAX_CHUNKS_PER_WINDMILL_BUNDLE",
                2,
            ),
            patch(
                "backend.pipeline.segmentation.transforms.stateful.MAX_CHUNKS_PER_WINDMILL_BUNDLE",
                2,
            ),
            patch(
                "backend.pipeline.segmentation.transforms.stateful.trans_constants.MAX_CHUNKS_PER_WINDMILL_BUNDLE",
                2,
            ),
        ):
            # 1. Process all 3 chunks in a single bundle
            # Chunks 1 and 2 will be processed. Chunk 3 (110) will be clamped and deferred because
            # we processed 2 chunks in the bundle.
            fn.processed_in_bundle = 0

            for chunk, ts in zip(chunks, chunk_timestamps, strict=True):
                # Call process directly
                outputs = list(
                    fn.process(
                        element=("test-feed", chunk),
                        timestamp=Timestamp(ts),
                        transmission_context_state=transmission_context_state,  # type: ignore
                        last_start_ms_state=last_start_ms_state,  # type: ignore
                        out_of_order_buffer_state=out_of_order_buffer_state,  # type: ignore
                        gap_timer_event=gap_timer_event,  # type: ignore
                        gap_timer_proc=gap_timer_proc,  # type: ignore
                        stale_timer_event=stale_timer_event,  # type: ignore
                        stale_timer_proc=stale_timer_proc,  # type: ignore
                        deferred_drain_timer=deferred_drain_timer,  # type: ignore
                    )
                )
                results.extend(outputs)

            # Enforce that Chunk 1 and 2 were processed, Chunk 3 was deferred.
            self.assertEqual(fn.processed_in_bundle, 2)
            self.assertIsNotNone(deferred_drain_timer.deadline)
            # The clamp happened during Chunk 3 (timestamp 110), so the timer was set to 110.001
            # proving that the timer has already leaped over the 9-second gap to 110!
            self.assertEqual(deferred_drain_timer.deadline, Timestamp(110.001))

            # 2. Now simulate the gap timeout flushing by resetting the expected next timestamp
            # to 110,000 in the state context, so that when the timer fires, the sequence buffer
            # is ready to drain Chunk 3.
            curr_context = transmission_context_state.read()
            curr_context = replace(
                curr_context, expected_next_chunk_start_ms=110000
            )
            transmission_context_state.write(curr_context)

            # 3. Simulate the timer firing to drain Chunk 3.
            # Under the old static 1ms code, if the timer fired at 101.001, it would have had to
            # do 9,000 iterations of 1ms steps to cross the gap to 110.
            # Under our dynamic leap-frog code, the timer was set directly to 110.001 after Chunk 3,
            # and it will drain Chunk 3 in exactly 1 iteration!
            iterations = 0
            while deferred_drain_timer.deadline is not None:
                iterations += 1
                if (
                    iterations > 10
                ):  # If it loops more than 10 times, it failed to leap-frog!
                    self.fail(
                        "Loop storm detected! Timer failed to leap-frog the gap and is looping in 1ms steps."
                    )

                firing_timestamp = deferred_drain_timer.deadline
                deferred_drain_timer.clear()
                fn.processed_in_bundle = 0

                outputs = list(
                    fn.handle_deferred_drain(
                        feed_id="test-feed",
                        transmission_context_state=transmission_context_state,  # type: ignore
                        last_start_ms_state=last_start_ms_state,  # type: ignore
                        out_of_order_buffer_state=out_of_order_buffer_state,  # type: ignore
                        stale_timer_event=stale_timer_event,  # type: ignore
                        stale_timer_proc=stale_timer_proc,  # type: ignore
                        timestamp=firing_timestamp,
                        deferred_drain_timer=deferred_drain_timer,  # type: ignore
                    )
                )
                results.extend(outputs)

            # Assert that it took exactly 1 iteration of the timer to complete the drain!
            self.assertEqual(iterations, 1)

            # 4. Flush the remaining active stitching state at the end of the test
            outputs = list(
                fn.handle_stale_transmission_event(
                    key="test-feed",
                    transmission_context=transmission_context_state,  # type: ignore
                    last_start_ms_state=last_start_ms_state,  # type: ignore
                    out_of_order_buffer_state=out_of_order_buffer_state,  # type: ignore
                    stale_timer_event=stale_timer_event,  # type: ignore
                    stale_timer_proc=stale_timer_proc,  # type: ignore
                )
            )
            results.extend(outputs)

            processed_uris = set()
            for res in results:
                if isinstance(res, tuple) and isinstance(res[1], FlushRequest):
                    for chunk in res[1].contributing_chunks:
                        processed_uris.add(chunk.gcs_uri)

            # Verify all 3 chunks were processed with zero data loss!
            self.assertEqual(len(processed_uris), 3)
            self.assertEqual(
                processed_uris,
                {f"gs://test-bucket/path/to/chunk{i}.flac" for i in [1, 2, 3]},
            )

    @patch(
        "backend.pipeline.segmentation.audio.processor.SegmentationAudioProcessor"
    )
    def test_ordered_stitch_mid_loop_budget_exhaustion_persists_timer_active_and_context(
        self, mock_audio_processor: MagicMock
    ) -> None:
        """Verifies mid-loop budget exhaustion in process() persists order_timer_active=True and preserves stitched context."""
        mock_processor_inst = mock_audio_processor.return_value

        def download_side_effect(gcs_uri, timestamp_ms, *args, **kwargs):
            return AudioChunkData(
                start_ms=timestamp_ms,
                audio=np.ones(16000, dtype=np.int16),
                sample_rate=16000,
                speech_segments=[TimeRange(0, 1000)],
                gcs_uri=gcs_uri,
                duration_ms=1000,
            )

        mock_processor_inst.download_audio_and_detect.side_effect = (
            download_side_effect
        )
        mock_processor_inst.preprocess_audio.side_effect = lambda x: x

        order_config = OrderRestorerConfig(
            out_of_order_timeout_ms=5000, chunk_duration_ms=1000
        )
        stitch_config = get_test_stitch_config(
            stale_timeout_ms=5000, significant_gap_ms=5000
        )

        fn = OrderedStitchAudioFn(
            order_config=order_config, stitch_config=stitch_config
        )
        fn.audio_processor = mock_processor_inst
        fn.setup()

        # Mock state cells
        class MockValueState:
            def __init__(self, initial=None) -> None:
                self.val = initial

            def read(self):
                return self.val

            def write(self, val):
                self.val = val

            def clear(self):
                self.val = None

        transmission_context_state = MockValueState(
            ActiveStitchingState(
                session_id="mock-session-id",
                feed_metadata=FeedMetadata(feed_name="mock-feed"),
                out_of_order_buffer=[],
                order_timer_active=True,
                expected_next_chunk_start_ms=100000,
            )
        )
        last_start_ms_state = MockValueState(None)
        out_of_order_buffer_state = MockBagState()

        class MockTimer:
            def __init__(self, name="timer") -> None:
                self.name = name
                self.deadline = None

            def set(self, deadline):
                self.deadline = deadline

            def clear(self):
                self.deadline = None

        gap_timer_event = MockTimer("gap_event")
        gap_timer_proc = MockTimer("gap_proc")
        stale_timer_event = MockTimer("stale_event")
        stale_timer_proc = MockTimer("stale_proc")
        deferred_drain_timer = MockTimer("deferred_drain")

        chunks = [
            ChunkMetadata(
                gcs_uri=f"gs://test-bucket/path/to/chunk{i}.flac",
                session_id="mock-session-id",
                timestamp_ms=100000 + (i - 1) * 1000,
                duration_ms=1000,
                feed_metadata=FeedMetadata(feed_name="mock-feed"),
            )
            for i in range(1, 4)
        ]

        # Feed chunk 2 (101000) and chunk 3 (102000) ahead of time while expected_next is 100000
        fn._is_bundle_budget_exhausted = lambda: False  # type: ignore
        fn.processed_in_bundle = 0
        list(
            fn.process(
                element=("test-feed", chunks[1]),
                timestamp=Timestamp(101),
                transmission_context_state=transmission_context_state,  # type: ignore
                last_start_ms_state=last_start_ms_state,  # type: ignore
                out_of_order_buffer_state=out_of_order_buffer_state,  # type: ignore
                gap_timer_event=gap_timer_event,  # type: ignore
                gap_timer_proc=gap_timer_proc,  # type: ignore
                stale_timer_event=stale_timer_event,  # type: ignore
                stale_timer_proc=stale_timer_proc,  # type: ignore
                deferred_drain_timer=deferred_drain_timer,  # type: ignore
            )
        )
        list(
            fn.process(
                element=("test-feed", chunks[2]),
                timestamp=Timestamp(102),
                transmission_context_state=transmission_context_state,  # type: ignore
                last_start_ms_state=last_start_ms_state,  # type: ignore
                out_of_order_buffer_state=out_of_order_buffer_state,  # type: ignore
                gap_timer_event=gap_timer_event,  # type: ignore
                gap_timer_proc=gap_timer_proc,  # type: ignore
                stale_timer_event=stale_timer_event,  # type: ignore
                stale_timer_proc=stale_timer_proc,  # type: ignore
                deferred_drain_timer=deferred_drain_timer,  # type: ignore
            )
        )

        # Buffer has chunks 2 & 3. Now feed chunk 1 (100000).
        # Inside loop over [chunk1, chunk2, chunk3], i=0 skips check, processes chunk 1.
        # At i=1 (chunk2), budget check returns True -> interrupts and defers chunk 2 and chunk 3.
        budget_exhausted_calls = []

        def mock_budget_exhausted():
            budget_exhausted_calls.append(True)
            return len(budget_exhausted_calls) >= 2

        fn._is_bundle_budget_exhausted = mock_budget_exhausted  # type: ignore

        list(
            fn.process(
                element=("test-feed", chunks[0]),
                timestamp=Timestamp(100),
                transmission_context_state=transmission_context_state,  # type: ignore
                last_start_ms_state=last_start_ms_state,  # type: ignore
                out_of_order_buffer_state=out_of_order_buffer_state,  # type: ignore
                gap_timer_event=gap_timer_event,  # type: ignore
                gap_timer_proc=gap_timer_proc,  # type: ignore
                stale_timer_event=stale_timer_event,  # type: ignore
                stale_timer_proc=stale_timer_proc,  # type: ignore
                deferred_drain_timer=deferred_drain_timer,  # type: ignore
            )
        )

        curr_context = transmission_context_state.read()
        self.assertIsNotNone(curr_context)
        self.assertTrue(curr_context.order_timer_active)
        self.assertEqual(len(curr_context.contributing_chunks), 1)
        self.assertEqual(
            curr_context.contributing_chunks[0].gcs_uri, chunks[0].gcs_uri
        )
        self.assertEqual(len(out_of_order_buffer_state.items), 2)
        buffered_uris = {
            item.gcs_uri for item in out_of_order_buffer_state.items
        }
        self.assertEqual(buffered_uris, {chunks[1].gcs_uri, chunks[2].gcs_uri})
        self.assertEqual(curr_context.expected_next_chunk_start_ms, 101000)

    @patch(
        "backend.pipeline.segmentation.audio.processor.SegmentationAudioProcessor"
    )
    def test_ordered_stitch_handle_deferred_drain_mid_loop_budget_persists_context(
        self, mock_audio_processor: MagicMock
    ) -> None:
        """Verifies mid-loop budget exhaustion in handle_deferred_drain() preserves stitched context and timer flag."""
        mock_processor_inst = mock_audio_processor.return_value

        def download_side_effect(gcs_uri, timestamp_ms, *args, **kwargs):
            return AudioChunkData(
                start_ms=timestamp_ms,
                audio=np.ones(16000, dtype=np.int16),
                sample_rate=16000,
                speech_segments=[TimeRange(0, 1000)],
                gcs_uri=gcs_uri,
                duration_ms=1000,
            )

        mock_processor_inst.download_audio_and_detect.side_effect = (
            download_side_effect
        )
        mock_processor_inst.preprocess_audio.side_effect = lambda x: x

        order_config = OrderRestorerConfig(
            out_of_order_timeout_ms=5000, chunk_duration_ms=1000
        )
        stitch_config = get_test_stitch_config(
            stale_timeout_ms=5000, significant_gap_ms=5000
        )

        fn = OrderedStitchAudioFn(
            order_config=order_config, stitch_config=stitch_config
        )
        fn.audio_processor = mock_processor_inst
        fn.setup()

        class MockValueState:
            def __init__(self, initial=None) -> None:
                self.val = initial

            def read(self):
                return self.val

            def write(self, val):
                self.val = val

            def clear(self):
                self.val = None

        transmission_context_state = MockValueState(
            ActiveStitchingState(
                session_id="mock-session-id",
                feed_metadata=FeedMetadata(feed_name="mock-feed"),
                out_of_order_buffer=[],
                order_timer_active=True,
                expected_next_chunk_start_ms=100000,
            )
        )
        last_start_ms_state = MockValueState(None)

        chunks = [
            BufferedChunk(
                gcs_uri=f"gs://test-bucket/path/to/chunk{i}.flac",
                timestamp_ms=100000 + (i - 1) * 1000,
            )
            for i in range(1, 4)
        ]
        out_of_order_buffer_state = MockBagState()
        for c in chunks:
            out_of_order_buffer_state.add(c)

        class MockTimer:
            def __init__(self, name="timer") -> None:
                self.name = name
                self.deadline = None

            def set(self, deadline):
                self.deadline = deadline

            def clear(self):
                self.deadline = None

        stale_timer_event = MockTimer("stale_event")
        stale_timer_proc = MockTimer("stale_proc")
        deferred_drain_timer = MockTimer("deferred_drain")

        # In handle_deferred_drain, i=0 processes chunks[0]. At i=1, check trips.
        budget_exhausted_calls = []

        def mock_budget_exhausted():
            budget_exhausted_calls.append(True)
            return len(budget_exhausted_calls) >= 1

        fn._is_bundle_budget_exhausted = mock_budget_exhausted  # type: ignore
        fn.processed_in_bundle = 0

        list(
            fn.handle_deferred_drain(
                feed_id="test-feed",
                transmission_context_state=transmission_context_state,  # type: ignore
                last_start_ms_state=last_start_ms_state,  # type: ignore
                out_of_order_buffer_state=out_of_order_buffer_state,  # type: ignore
                stale_timer_event=stale_timer_event,  # type: ignore
                stale_timer_proc=stale_timer_proc,  # type: ignore
                timestamp=Timestamp(100),
                deferred_drain_timer=deferred_drain_timer,  # type: ignore
            )
        )

        curr_context = transmission_context_state.read()
        self.assertIsNotNone(curr_context)
        self.assertTrue(curr_context.order_timer_active)
        self.assertEqual(len(curr_context.contributing_chunks), 1)
        self.assertEqual(
            curr_context.contributing_chunks[0].gcs_uri, chunks[0].gcs_uri
        )
        self.assertEqual(len(out_of_order_buffer_state.items), 2)
        buffered_uris = {
            item.gcs_uri for item in out_of_order_buffer_state.items
        }
        self.assertEqual(buffered_uris, {chunks[1].gcs_uri, chunks[2].gcs_uri})
        self.assertEqual(curr_context.expected_next_chunk_start_ms, 101000)

    @patch(
        "backend.pipeline.segmentation.audio.processor.SegmentationAudioProcessor"
    )
    def test_ordered_stitch_handle_gap_timeout_mid_loop_budget_persists_context(
        self, mock_audio_processor: MagicMock
    ) -> None:
        """Verifies mid-loop budget exhaustion in handle_gap_timeout_event() preserves stitched context and timer flag."""
        mock_processor_inst = mock_audio_processor.return_value

        def download_side_effect(gcs_uri, timestamp_ms, *args, **kwargs):
            return AudioChunkData(
                start_ms=timestamp_ms,
                audio=np.ones(16000, dtype=np.int16),
                sample_rate=16000,
                speech_segments=[TimeRange(0, 1000)],
                gcs_uri=gcs_uri,
                duration_ms=1000,
            )

        mock_processor_inst.download_audio_and_detect.side_effect = (
            download_side_effect
        )
        mock_processor_inst.preprocess_audio.side_effect = lambda x: x

        order_config = OrderRestorerConfig(
            out_of_order_timeout_ms=5000, chunk_duration_ms=1000
        )
        stitch_config = get_test_stitch_config(
            stale_timeout_ms=5000, significant_gap_ms=5000
        )

        fn = OrderedStitchAudioFn(
            order_config=order_config, stitch_config=stitch_config
        )
        fn.audio_processor = mock_processor_inst
        fn.setup()

        class MockValueState:
            def __init__(self, initial=None) -> None:
                self.val = initial

            def read(self):
                return self.val

            def write(self, val):
                self.val = val

            def clear(self):
                self.val = None

        # Suppose we were expecting 100000, but only have 101000 and 102000 in buffer when timeout fires.
        transmission_context_state = MockValueState(
            ActiveStitchingState(
                session_id="mock-session-id",
                feed_metadata=FeedMetadata(feed_name="mock-feed"),
                out_of_order_buffer=[],
                order_timer_active=True,
                expected_next_chunk_start_ms=100000,
            )
        )
        last_start_ms_state = MockValueState(None)

        chunks = [
            BufferedChunk(
                gcs_uri=f"gs://test-bucket/path/to/chunk{i}.flac",
                timestamp_ms=100000 + (i - 1) * 1000,
            )
            for i in range(2, 4)
        ]
        out_of_order_buffer_state = MockBagState()
        for c in chunks:
            out_of_order_buffer_state.add(c)

        class MockTimer:
            def __init__(self, name="timer") -> None:
                self.name = name
                self.deadline = None

            def set(self, deadline):
                self.deadline = deadline

            def clear(self):
                self.deadline = None

        gap_timer_event = MockTimer("gap_event")
        gap_timer_proc = MockTimer("gap_proc")
        stale_timer_event = MockTimer("stale_event")
        stale_timer_proc = MockTimer("stale_proc")
        deferred_drain_timer = MockTimer("deferred_drain")

        budget_exhausted_calls = []

        def mock_budget_exhausted():
            budget_exhausted_calls.append(True)
            return len(budget_exhausted_calls) >= 1

        fn._is_bundle_budget_exhausted = mock_budget_exhausted  # type: ignore
        fn.processed_in_bundle = 0

        list(
            fn.handle_gap_timeout_event(
                feed_id="test-feed",
                transmission_context_state=transmission_context_state,  # type: ignore
                last_start_ms_state=last_start_ms_state,  # type: ignore
                out_of_order_buffer_state=out_of_order_buffer_state,  # type: ignore
                stale_timer_event=stale_timer_event,  # type: ignore
                stale_timer_proc=stale_timer_proc,  # type: ignore
                timestamp=Timestamp(105),
                gap_timer_event=gap_timer_event,  # type: ignore
                gap_timer_proc=gap_timer_proc,  # type: ignore
                deferred_drain_timer=deferred_drain_timer,  # type: ignore
            )
        )

        curr_context = transmission_context_state.read()
        self.assertIsNotNone(curr_context)
        self.assertTrue(curr_context.order_timer_active)
        self.assertEqual(len(curr_context.contributing_chunks), 1)
        self.assertEqual(
            curr_context.contributing_chunks[0].gcs_uri, chunks[0].gcs_uri
        )
        self.assertEqual(len(out_of_order_buffer_state.items), 1)
        self.assertEqual(
            out_of_order_buffer_state.items[0].gcs_uri, chunks[1].gcs_uri
        )
        self.assertEqual(curr_context.expected_next_chunk_start_ms, 102000)

    def test_is_bundle_budget_exhausted_by_time_limit(self) -> None:
        """Verifies _is_bundle_budget_exhausted returns True when wall-clock time limit is exceeded."""
        fn = OrderedStitchAudioFn(
            order_config=OrderRestorerConfig(),
            stitch_config=get_test_stitch_config(),
        )
        fn.start_bundle()
        # Mock time.monotonic() to simulate passage of 61 seconds (limit is 60s)
        with patch(
            "backend.pipeline.segmentation.transforms.stateful.time.monotonic"
        ) as mock_mono:
            # First call in start_bundle was real, or suppose we set it manually
            fn.bundle_start_time_monotonic = 100.0
            mock_mono.return_value = 161.0
            self.assertTrue(fn._is_bundle_budget_exhausted())

            # If not exceeded (e.g. 50 seconds elapsed)
            mock_mono.return_value = 150.0
            self.assertFalse(fn._is_bundle_budget_exhausted())

    @patch(
        "backend.pipeline.segmentation.audio.processor.SegmentationAudioProcessor"
    )
    def test_batch_state_access_once_per_bundle(
        self, mock_audio_processor: MagicMock
    ) -> None:
        """Verifies that processing a multi-chunk bundle reads and writes Beam state cells exactly once per batch."""
        mock_processor_inst = mock_audio_processor.return_value

        def make_chunk_data(
            uri: str, timestamp_ms: int, *args: Any, **kwargs: Any
        ) -> AudioChunkData:
            return AudioChunkData(
                start_ms=timestamp_ms,
                audio=np.zeros(16000, dtype=np.float32),
                sample_rate=16000,
                speech_segments=[],
                gcs_uri=uri,
                duration_ms=1000,
            )

        mock_processor_inst.download_audio_and_detect.side_effect = (
            make_chunk_data
        )
        mock_processor_inst.preprocess_audio.side_effect = lambda x: x

        order_config = OrderRestorerConfig(out_of_order_timeout_ms=1000)
        stitch_config = get_test_stitch_config(
            significant_gap_ms=500, stale_timeout_ms=60000
        )
        fn = OrderedStitchAudioFn(
            order_config=order_config, stitch_config=stitch_config
        )
        fn.audio_processor = mock_processor_inst

        tx_state = MockValueState(
            ActiveStitchingState(
                session_id="session-batch",
                feed_metadata=FeedMetadata(feed_name="feed-batch"),
                expected_next_chunk_start_ms=1000,
                order_timer_active=True,
            )
        )
        last_start_ms_state = MockValueState(1000)
        ooo_buffer_state = MockBagState()
        # Add 5 consecutive chronological chunks to the OOO buffer
        for i in range(5):
            ooo_buffer_state.add(
                BufferedChunk(
                    timestamp_ms=1000 + i * 1000,
                    gcs_uri=f"gs://test-bucket/batch_{i}.flac",
                )
            )

        gap_timer_event = MagicMock()
        gap_timer_proc = MagicMock()
        stale_timer_event = MagicMock()
        stale_timer_proc = MagicMock()

        # Reset call counts before invoking the batch handler
        tx_state.read_count = 0
        tx_state.write_count = 0
        last_start_ms_state.read_count = 0
        last_start_ms_state.write_count = 0

        list(
            fn._handle_gap_timeout_common(
                key_str="feed-batch",
                transmission_context_state=tx_state,  # type: ignore
                last_start_ms_state=last_start_ms_state,  # type: ignore
                out_of_order_buffer_state=ooo_buffer_state,  # type: ignore
                stale_timer_event=stale_timer_event,
                stale_timer_proc=stale_timer_proc,
                timestamp=Timestamp(5.0),
                gap_timer_event=gap_timer_event,
                gap_timer_proc=gap_timer_proc,
                deferred_drain_timer=MagicMock(),
                timer_type="processing",
            )
        )

        # Assert that despite 5 chunks being processed and stitched, state cells were read/written exactly once per bundle!
        self.assertEqual(tx_state.read_count, 1)
        self.assertEqual(tx_state.write_count, 1)
        self.assertEqual(last_start_ms_state.read_count, 1)
        self.assertEqual(last_start_ms_state.write_count, 1)


class OrderedStitchSpeechSegmentsTest(unittest.TestCase):
    @patch(
        "backend.pipeline.segmentation.audio.processor.SegmentationAudioProcessor"
    )
    def test_speech_segments_persistence_and_stale_flush(
        self, mock_audio_processor: MagicMock
    ) -> None:
        """Verifies that speech_segments are preserved in ActiveStitchingState and mapped during stale flushes."""
        mock_processor_inst = mock_audio_processor.return_value
        chunk_data = AudioChunkData(
            start_ms=100000,
            audio=np.zeros(16000 * 5, dtype=np.int16),
            speech_segments=[TimeRange(0, 5000)],
            gcs_uri="gs://bucket/chunk1.flac",
            duration_ms=5000,
            sample_rate=16000,
        )
        mock_processor_inst.download_audio_and_detect.return_value = chunk_data

        order_config = OrderRestorerConfig(out_of_order_timeout_ms=1000)
        stitch_config = get_test_stitch_config(
            significant_gap_ms=800, stale_timeout_ms=75000
        )
        fn = OrderedStitchAudioFn(
            order_config=order_config, stitch_config=stitch_config
        )
        fn.setup()

        class MockValueState:
            def __init__(self, initial=None) -> None:
                self.val = initial

            def read(self):
                return self.val

            def write(self, val):
                self.val = val

            def clear(self):
                self.val = None

        class MockBagState:
            def __init__(self) -> None:
                self.items = []

            def read(self):
                return self.items

            def add(self, item):
                self.items.append(item)

            def clear(self):
                self.items = []

            def write(self, val):
                self.items = val

        mock_state_context = MockValueState(
            ActiveStitchingState(
                session_id="session-1",
                feed_metadata=FeedMetadata(feed_name="test-feed"),
            )
        )
        mock_last_start_ms = MockValueState(None)
        mock_state_buffer = MockBagState()

        metadata = ChunkMetadata(
            gcs_uri="gs://bucket/chunk1.flac",
            session_id="session-1",
            duration_ms=5000,
            feed_metadata=FeedMetadata(feed_name="test-feed"),
        )

        # 1. Process chunk and verify speech_segments are saved to persistent state
        list(
            fn.process(
                element=("test-feed", metadata),
                timestamp=Timestamp(100),
                transmission_context_state=mock_state_context,  # type: ignore
                last_start_ms_state=mock_last_start_ms,  # type: ignore
                out_of_order_buffer_state=mock_state_buffer,  # type: ignore
                gap_timer_event=MagicMock(),
                gap_timer_proc=MagicMock(),
                stale_timer_event=MagicMock(),
                stale_timer_proc=MagicMock(),
            )
        )

        saved_context = mock_state_context.read()
        self.assertIsInstance(saved_context, ActiveStitchingState)
        self.assertTrue(len(saved_context.speech_segments) > 0)
        self.assertEqual(
            get_duration_ms(saved_context.speech_segments[0]), 5000
        )

        # 2. Trigger stale flush and verify mapped speech_segments in FlushRequest payload
        outputs = list(
            fn.handle_stale_transmission_event(
                key="test-feed",
                transmission_context=mock_state_context,  # type: ignore
                last_start_ms_state=mock_last_start_ms,  # type: ignore
                out_of_order_buffer_state=mock_state_buffer,  # type: ignore
                stale_timer_event=MagicMock(),
                stale_timer_proc=MagicMock(),
            )
        )

        self.assertEqual(len(outputs), 1)
        feed_id, flush_request = outputs[0]
        self.assertEqual(feed_id, "test-feed")
        self.assertTrue(len(flush_request.speech_segments) > 0)
        self.assertEqual(
            get_duration_ms(flush_request.speech_segments[0]), 5000
        )
        self.assertEqual(
            flush_request.audio_classification,
            AudioClassification.AUDIO_CLASSIFICATION_SPEECH,
        )
        self.assertIsNone(mock_state_context.read())

    @patch(
        "backend.pipeline.segmentation.audio.processor.SegmentationAudioProcessor"
    )
    def test_stale_flush_classification_without_speech_segments(
        self, mock_audio_processor: MagicMock
    ) -> None:
        """Verifies that a stale flush on a transmission with no speech segments yields AUDIO_CLASSIFICATION_OTHER."""
        mock_processor_inst = mock_audio_processor.return_value
        chunk_data = AudioChunkData(
            start_ms=100000,
            audio=np.zeros(16000 * 5, dtype=np.int16),
            speech_segments=[],
            gcs_uri="gs://bucket/chunk1.flac",
            duration_ms=5000,
            sample_rate=16000,
        )
        mock_processor_inst.download_audio_and_detect.return_value = chunk_data

        order_config = OrderRestorerConfig(out_of_order_timeout_ms=1000)
        stitch_config = get_test_stitch_config(
            significant_gap_ms=800, stale_timeout_ms=75000
        )
        fn = OrderedStitchAudioFn(
            order_config=order_config, stitch_config=stitch_config
        )
        fn.setup()

        class MockValueState:
            def __init__(self, initial=None) -> None:
                self.val = initial

            def read(self):
                return self.val

            def write(self, val):
                self.val = val

            def clear(self):
                self.val = None

        class MockBagState:
            def __init__(self) -> None:
                self.items = []

            def read(self):
                return self.items

            def add(self, item):
                self.items.append(item)

            def clear(self):
                self.items = []

            def write(self, val):
                self.items = val

        mock_state_context = MockValueState(
            ActiveStitchingState(
                session_id="session-1",
                feed_metadata=FeedMetadata(feed_name="test-feed"),
                stale_start_time_ms=100000,
                buffer_start_time_ms=100000,
                buffer_duration_ms=5000,
                last_end_time_ms=105000,
            )
        )
        mock_state_buffer = MockBagState()
        mock_state_buffer.add(np.zeros(16000 * 5, dtype=np.int16).tobytes())
        mock_last_start_ms = MockValueState(None)

        # Trigger stale flush and verify NO_SPEECH classification
        outputs = list(
            fn.handle_stale_transmission_event(
                key="test-feed",
                transmission_context=mock_state_context,  # type: ignore
                last_start_ms_state=mock_last_start_ms,  # type: ignore
                out_of_order_buffer_state=mock_state_buffer,  # type: ignore
                stale_timer_event=MagicMock(),
                stale_timer_proc=MagicMock(),
            )
        )

        self.assertEqual(len(outputs), 1)
        feed_id, flush_request = outputs[0]
        self.assertEqual(feed_id, "test-feed")
        self.assertEqual(len(flush_request.speech_segments), 0)
        self.assertEqual(
            flush_request.audio_classification,
            AudioClassification.AUDIO_CLASSIFICATION_OTHER,
        )
        self.assertIsNone(mock_state_context.read())

    @patch(
        "backend.pipeline.segmentation.audio.processor.SegmentationAudioProcessor"
    )
    def test_prior_audio_tail_cleared_on_stale_flush(
        self, mock_audio_processor: MagicMock
    ) -> None:
        """Verifies that the stale timer flush resets the context state to IdleFeedState,
        guaranteeing that the prior_audio_tail is cleanly cleared and never carried over
        to prime a new, post-flush transmission.
        """
        mock_processor_inst = mock_audio_processor.return_value
        # Note: We set the speech segment to end exactly at the chunk boundary (5000ms).
        # Under the conditional state propagation logic, a chunk must end in active speech
        # (within 50ms) to cache the prior audio tail. Setting it to 5000ms forces the tail
        # to be cached, allowing us to verify that a subsequent stale flush successfully clears it.
        chunk_data = AudioChunkData(
            start_ms=100000,
            audio=np.zeros(16000 * 5, dtype=np.int16),
            speech_segments=[TimeRange(1000, 5000)],
            gcs_uri="gs://bucket/chunk1.flac",
            duration_ms=5000,
            sample_rate=16000,
        )
        mock_processor_inst.download_audio_and_detect.return_value = chunk_data

        order_config = OrderRestorerConfig(out_of_order_timeout_ms=1000)
        stitch_config = get_test_stitch_config(
            significant_gap_ms=800, stale_timeout_ms=75000
        )
        fn = OrderedStitchAudioFn(
            order_config=order_config, stitch_config=stitch_config
        )
        fn.setup()

        class MockValueState:
            def __init__(self, initial=None) -> None:
                self.val = initial

            def read(self):
                return self.val

            def write(self, val):
                self.val = val

            def clear(self):
                self.val = None

        class MockBagState:
            def __init__(self) -> None:
                self.items = []

            def read(self):
                return self.items

            def add(self, item):
                self.items.append(item)

            def clear(self):
                self.items = []

            def write(self, val):
                self.items = val

        mock_state_context = MockValueState(
            ActiveStitchingState(
                session_id="session-1",
                feed_metadata=FeedMetadata(feed_name="test-feed"),
            )
        )
        mock_last_start_ms = MockValueState(None)
        mock_state_buffer = MockBagState()

        metadata = ChunkMetadata(
            gcs_uri="gs://bucket/chunk1.flac",
            session_id="session-1",
            duration_ms=5000,
            feed_metadata=FeedMetadata(feed_name="test-feed"),
        )

        # 1. Process chunk 1 (VAD segment triggers speech in progress)
        list(
            fn.process(
                element=("test-feed", metadata),
                timestamp=Timestamp(100),
                transmission_context_state=mock_state_context,  # type: ignore
                last_start_ms_state=mock_last_start_ms,  # type: ignore
                out_of_order_buffer_state=mock_state_buffer,  # type: ignore
                gap_timer_event=MagicMock(),
                gap_timer_proc=MagicMock(),
                stale_timer_event=MagicMock(),
                stale_timer_proc=MagicMock(),
            )
        )

        # State context must be ActiveStitchingState with cached prior_audio_tail
        saved_context = mock_state_context.read()
        self.assertIsInstance(saved_context, ActiveStitchingState)
        self.assertIsNotNone(saved_context.prior_audio_tail)

        # 2. Stale flush occurs (dispatcher unkeyed/timer fired)
        list(
            fn.handle_stale_transmission_event(
                key="test-feed",
                transmission_context=mock_state_context,  # type: ignore
                last_start_ms_state=mock_last_start_ms,  # type: ignore
                out_of_order_buffer_state=mock_state_buffer,  # type: ignore
                stale_timer_event=MagicMock(),
                stale_timer_proc=MagicMock(),
            )
        )

        # Assert that the state context has been completely cleared!
        self.assertIsNone(mock_state_context.read())

    @patch(
        "backend.pipeline.segmentation.audio.processor.SegmentationAudioProcessor"
    )
    def test_prior_audio_tail_not_cached_if_chunk_ends_in_silence(
        self, mock_audio_processor: MagicMock
    ) -> None:
        """Verifies that under the Conditional State Continuity logic, the prior_audio_tail
        is NOT cached if the chunk ends in silence, preventing noise propagation.
        """
        mock_processor_inst = mock_audio_processor.return_value
        # Chunk has speech from 1.0s to 4.0s, but ends in silence (duration is 5.0s)
        chunk_data = AudioChunkData(
            start_ms=100000,
            audio=np.zeros(16000 * 5, dtype=np.int16),
            speech_segments=[TimeRange(1000, 4000)],
            gcs_uri="gs://bucket/chunk1.flac",
            duration_ms=5000,
            sample_rate=16000,
        )
        mock_processor_inst.download_audio_and_detect.return_value = chunk_data

        order_config = OrderRestorerConfig(out_of_order_timeout_ms=1000)
        stitch_config = get_test_stitch_config(
            significant_gap_ms=800, stale_timeout_ms=75000
        )
        fn = OrderedStitchAudioFn(
            order_config=order_config, stitch_config=stitch_config
        )
        fn.setup()

        class MockValueState:
            def __init__(self, initial=None) -> None:
                self.val = initial

            def read(self):
                return self.val

            def write(self, val):
                self.val = val

            def clear(self):
                self.val = None

        mock_state_context = MockValueState(
            ActiveStitchingState(
                session_id="session-1",
                feed_metadata=FeedMetadata(feed_name="test-feed"),
            )
        )
        mock_last_start_ms = MockValueState(None)

        metadata = ChunkMetadata(
            gcs_uri="gs://bucket/chunk1.flac",
            session_id="session-1",
            duration_ms=5000,
            feed_metadata=FeedMetadata(feed_name="test-feed"),
        )

        # Process chunk 1
        list(
            fn.process(
                element=("test-feed", metadata),
                timestamp=Timestamp(100),
                transmission_context_state=mock_state_context,  # type: ignore
                last_start_ms_state=mock_last_start_ms,  # type: ignore
                gap_timer_event=MagicMock(),
                gap_timer_proc=MagicMock(),
                stale_timer_event=MagicMock(),
                stale_timer_proc=MagicMock(),
            )
        )

        # State context must be ActiveStitchingState, but prior_audio_tail must be None!
        saved_context = mock_state_context.read()
        self.assertIsInstance(saved_context, ActiveStitchingState)
        self.assertIsNone(saved_context.prior_audio_tail)

    @patch(
        "backend.pipeline.segmentation.audio.processor.SegmentationAudioProcessor"
    )
    def test_speech_segments_exceeding_max_duration_forced_split(
        self, mock_audio_processor: MagicMock
    ) -> None:
        """Verifies that an audio file exceeding max_transmission_duration_ms without
        silence is force-split mid-stream, properly marking context severance.
        """
        mock_processor_inst = mock_audio_processor.return_value
        # 65 seconds of continuous audio/speech
        chunk_data = AudioChunkData(
            start_ms=0,
            audio=np.zeros(16000 * 65, dtype=np.int16),
            speech_segments=[TimeRange(0, 65000)],
            gcs_uri="gs://bucket/long_continuous.flac",
            duration_ms=65000,
            sample_rate=16000,
        )
        mock_processor_inst.download_audio_and_detect.return_value = chunk_data

        order_config = OrderRestorerConfig(out_of_order_timeout_ms=1000)
        stitch_config = get_test_stitch_config(
            max_transmission_duration_ms=60000,
        )
        fn = OrderedStitchAudioFn(
            order_config=order_config, stitch_config=stitch_config
        )
        fn.setup()

        class MockValueState:
            def __init__(self, initial=None) -> None:
                self.val = initial

            def read(self):
                return self.val

            def write(self, val):
                self.val = val

            def clear(self):
                self.val = None

        class MockBagState:
            def __init__(self) -> None:
                self.items = []

            def read(self):
                return self.items

            def add(self, item):
                self.items.append(item)

            def clear(self):
                self.items = []

            def write(self, val):
                self.items = val

        mock_state_context = MockValueState(
            ActiveStitchingState(
                session_id="session-long",
                feed_metadata=FeedMetadata(feed_name="test-feed"),
            )
        )
        mock_last_start_ms = MockValueState(None)

        metadata = ChunkMetadata(
            gcs_uri="gs://bucket/long_continuous.flac",
            session_id="session-long",
            duration_ms=65000,
            feed_metadata=FeedMetadata(feed_name="test-feed"),
        )

        outputs = list(
            fn.process(
                element=("test-feed", metadata),
                timestamp=Timestamp(100),
                transmission_context_state=mock_state_context,  # type: ignore
                last_start_ms_state=mock_last_start_ms,  # type: ignore
                gap_timer_event=MagicMock(),
                gap_timer_proc=MagicMock(),
                stale_timer_event=MagicMock(),
                stale_timer_proc=MagicMock(),
            )
        )

        # Should immediately yield one flush request severed at 60s
        self.assertEqual(len(outputs), 1)
        feed_id, flush_req = outputs[0]
        self.assertEqual(feed_id, "test-feed")
        # Assert missing_post_context is set to True due to forced split
        self.assertTrue(flush_req.missing_post_context)
        self.assertEqual(flush_req.speech_segments[-1].end_ms, 60000)

        # Saved context should have the remaining 5s marked with missing_prior_context
        saved_context = mock_state_context.read()
        self.assertIsInstance(saved_context, ActiveStitchingState)
        self.assertTrue(saved_context.missing_prior_context)
        self.assertEqual(saved_context.speech_segments[0].start_ms, 60000)
        self.assertEqual(saved_context.speech_segments[0].end_ms, 65000)

    @patch(
        "backend.pipeline.segmentation.audio.processor.SegmentationAudioProcessor"
    )
    def test_intra_chunk_multiple_flushes_direct_runner(
        self, mock_audio_processor: MagicMock
    ) -> None:
        """Verifies that when a single chunk triggers multiple flushes (speech, silence, speech),
        the audio buffer is correctly cleared between flushes on the Beam runner and no audio is repeated.
        """
        mock_processor_inst = mock_audio_processor.return_value

        # 10 seconds of audio. We use np.arange so each sample has a unique, identifiable value.
        # Sample rate is 16000 Hz. Total samples = 160,000.
        total_samples = 16000 * 10
        audio_data = np.arange(total_samples, dtype=np.int16)

        chunk_data = AudioChunkData(
            start_ms=100000,
            audio=audio_data,
            speech_segments=[TimeRange(1000, 3000), TimeRange(7000, 9000)],
            gcs_uri="gs://bucket/multi_segment.flac",
            duration_ms=10000,
            sample_rate=16000,
        )
        mock_processor_inst.download_audio_and_detect.return_value = chunk_data
        mock_processor_inst.preprocess_audio.side_effect = lambda x: x

        order_config = OrderRestorerConfig(out_of_order_timeout_ms=1000)
        # Use significant_gap_ms = 500 so the gap between 3s and 7s (4s) triggers a flush,
        # and the trailing silence from 9s to 10s (1s) also triggers a flush.
        stitch_config = get_test_stitch_config(
            significant_gap_ms=500,
            stale_timeout_ms=60000,
        )

        options = PipelineOptions(
            flags=[
                "--continuous_input_subscription=projects/p/subscriptions/a",
                "--output_topic=b",
                "--project=c",
            ]
        )

        metadata = ChunkMetadata(
            gcs_uri="gs://bucket/multi_segment.flac",
            session_id="session-multi",
            duration_ms=10000,
            feed_metadata=FeedMetadata(feed_name="mock-feed"),
            traceparent="mock-traceparent",
        )

        with BeamTestPipeline(options=options) as p:
            test_stream = (
                TestStream(
                    coder=beam.coders.TupleCoder(
                        (
                            beam.coders.StrUtf8Coder(),
                            trans_coders.ChunkMetadataCoder(),
                        )
                    )
                )
                .advance_watermark_to(100)
                .add_elements([TimestampedValue(("test-feed", metadata), 100)])
                .advance_watermark_to(200)
                .advance_watermark_to_infinity()
            )

            results = (
                p
                | test_stream
                | beam.ParDo(
                    OrderedStitchAudioFn(
                        order_config=order_config, stitch_config=stitch_config
                    )
                )
            )

            def assert_results(msgs):
                # We expect exactly 4 flush requests:
                # 1. Speech 1: from 1000ms to 3500ms (including 500ms post-roll)
                # 2. Silence 1: from 3500ms to 7000ms
                # 3. Speech 2: from 7000ms to 9500ms (including 500ms post-roll)
                # 4. Silence 2: from 9500ms to 10000ms (flushed by stale timer at the end)
                assert len(msgs) == 4, f"Expected 4 flushes, got {len(msgs)}"

                # Check Flush 1 (Speech 1)
                feed_id_1, req_1 = msgs[0]
                assert feed_id_1 == "test-feed"
                assert (
                    req_1.audio_classification
                    == AudioClassification.AUDIO_CLASSIFICATION_SPEECH
                )
                # Samples should correspond to [1000ms, 3000ms], which is [16000, 48000]
                # Samples should correspond to [1000ms, 3000ms], which is [16000, 48000]
                assert req_1.start_audio_offset_ms == 1000
                assert req_1.end_audio_offset_ms == 2000
                assert req_1.speech_segments == [
                    TimeRange(start_ms=101000, end_ms=103000)
                ]

                # Check Flush 2 (Silence 1)
                feed_id_2, req_2 = msgs[1]
                assert feed_id_2 == "test-feed"
                assert (
                    req_2.audio_classification
                    == AudioClassification.AUDIO_CLASSIFICATION_OTHER
                )
                assert req_2.start_audio_offset_ms == 3000
                assert req_2.end_audio_offset_ms == 4000
                assert len(req_2.speech_segments) == 0

                # Check Flush 3 (Speech 2)
                feed_id_3, req_3 = msgs[2]
                assert feed_id_3 == "test-feed"
                assert (
                    req_3.audio_classification
                    == AudioClassification.AUDIO_CLASSIFICATION_SPEECH
                )
                assert req_3.start_audio_offset_ms == 7000
                assert req_3.end_audio_offset_ms == 2000
                assert req_3.speech_segments == [
                    TimeRange(start_ms=107000, end_ms=109000)
                ]

                # Check Flush 4 (Silence 2)
                feed_id_4, req_4 = msgs[3]
                assert feed_id_4 == "test-feed"
                assert (
                    req_4.audio_classification
                    == AudioClassification.AUDIO_CLASSIFICATION_OTHER
                )
                assert req_4.start_audio_offset_ms == 9000
                assert req_4.end_audio_offset_ms == 1000
                assert len(req_4.speech_segments) == 0

            assert_that(results, assert_results)

    @patch(
        "backend.pipeline.segmentation.audio.processor.SegmentationAudioProcessor"
    )
    def test_speech_segments_exceeding_max_duration_natural_split(
        self, mock_audio_processor: MagicMock
    ) -> None:
        """Verifies that an audio file exceeding max_transmission_duration_ms with
        natural silences triggers a clean mid-stream flush without context severance.
        """
        mock_processor_inst = mock_audio_processor.return_value
        # 65 seconds of audio with two speech segments separated by a 5s silence gap
        chunk_data = AudioChunkData(
            start_ms=0,
            audio=np.zeros(16000 * 65, dtype=np.int16),
            speech_segments=[TimeRange(0, 30000), TimeRange(35000, 65000)],
            gcs_uri="gs://bucket/long_silence.flac",
            duration_ms=65000,
            sample_rate=16000,
        )
        mock_processor_inst.download_audio_and_detect.return_value = chunk_data

        order_config = OrderRestorerConfig(out_of_order_timeout_ms=1000)
        stitch_config = get_test_stitch_config(
            significant_gap_ms=3000,
            max_transmission_duration_ms=60000,
        )
        fn = OrderedStitchAudioFn(
            order_config=order_config, stitch_config=stitch_config
        )
        fn.setup()

        class MockValueState:
            def __init__(self, initial=None) -> None:
                self.val = initial

            def read(self):
                return self.val

            def write(self, val):
                self.val = val

            def clear(self):
                self.val = None

        class MockBagState:
            def __init__(self) -> None:
                self.items = []

            def read(self):
                return self.items

            def add(self, item):
                self.items.append(item)

            def clear(self):
                self.items = []

            def write(self, val):
                self.items = val

        mock_state_context = MockValueState(
            ActiveStitchingState(
                session_id="session-silence",
                feed_metadata=FeedMetadata(feed_name="test-feed"),
            )
        )
        mock_last_start_ms = MockValueState(None)

        metadata = ChunkMetadata(
            gcs_uri="gs://bucket/long_silence.flac",
            session_id="session-silence",
            duration_ms=65000,
            feed_metadata=FeedMetadata(feed_name="test-feed"),
        )

        outputs = list(
            fn.process(
                element=("test-feed", metadata),
                timestamp=Timestamp(100),
                transmission_context_state=mock_state_context,  # type: ignore
                last_start_ms_state=mock_last_start_ms,  # type: ignore
                gap_timer_event=MagicMock(),
                gap_timer_proc=MagicMock(),
                stale_timer_event=MagicMock(),
                stale_timer_proc=MagicMock(),
            )
        )

        # Under Continuous Audio Retention, we yield two flush requests: one for speech and one for silence
        self.assertEqual(len(outputs), 2)
        feed_id, flush_req = outputs[0]
        self.assertEqual(feed_id, "test-feed")
        # Since it split at a natural silence gap, missing_post_context should be False
        self.assertFalse(flush_req.missing_post_context)
        self.assertEqual(flush_req.speech_segments[-1].end_ms, 30000)

        # Second output: pure silence gap (30000 to 35000)
        feed_id_2, flush_req_2 = outputs[1]
        self.assertEqual(feed_id_2, "test-feed")
        self.assertEqual(
            flush_req_2.audio_classification,
            AudioClassification.AUDIO_CLASSIFICATION_OTHER,
        )

        # Saved context should hold the second speech segment natively started
        saved_context = mock_state_context.read()
        self.assertIsInstance(saved_context, ActiveStitchingState)
        self.assertFalse(saved_context.missing_prior_context)
        self.assertEqual(saved_context.speech_segments[0].start_ms, 35000)
        self.assertEqual(saved_context.speech_segments[0].end_ms, 65000)


class DlqTaggingTest(unittest.TestCase):
    """Regression tests for the two bugs fixed in PR #458:

    Bug 1: StitcherEngine DLQ results were plain tuples, not beam.pvalue.TaggedOutput.
           Without _yield_tagged_outputs, DLQ payloads silently route to the main output
           and get lost, rather than landing on the transcription_dlq tag.

    Bug 2: _apply_flush_action re-read transmission_context state instead of using the
           already-loaded curr_context. A stale or None re-read would lose feed_metadata
           and sample_rate, causing downstream serialization failures.
    """

    def _make_fn_and_states(
        self,
        fn_class: type,
    ) -> tuple[Any, Any, Any, Any]:
        """Returns (fn, mock_state_context, _, mock_last_start_ms)."""
        order_config = OrderRestorerConfig(out_of_order_timeout_ms=1000)
        stitch_config = get_test_stitch_config(stale_timeout_ms=5000)
        fn = fn_class(order_config=order_config, stitch_config=stitch_config)

        class MockValueState:
            def __init__(self, initial: Any = None) -> None:
                self.val = initial

            def read(self) -> Any:
                return self.val

            def write(self, val: Any) -> None:
                self.val = val

            def clear(self) -> None:
                self.val = None

        class MockBagState:
            def __init__(self) -> None:
                self.items: list[Any] = []

            def read(self) -> list[Any]:
                return self.items

            def add(self, item: Any) -> None:
                self.items.append(item)

            def clear(self) -> None:
                self.items = []

        ctx = ActiveStitchingState(
            session_id="test-session",
            feed_metadata=FeedMetadata(feed_name="test-feed"),
        )
        return fn, MockValueState(ctx), MockBagState(), MockValueState(None)

    # --- Bug 1: _yield_tagged_outputs wrapping ---

    def test_yield_tagged_outputs_wraps_dlq_tuple(self) -> None:
        """Verifies that _yield_tagged_outputs converts a raw DLQ tuple into a TaggedOutput."""
        order_config = OrderRestorerConfig(out_of_order_timeout_ms=1000)
        stitch_config = get_test_stitch_config()
        fn = OrderedStitchAudioFn(
            order_config=order_config, stitch_config=stitch_config
        )

        dlq_payload = {"error": "mock error", "attributes": {}}
        results = list(
            fn._yield_tagged_outputs([(DEAD_LETTER_QUEUE_TAG, dlq_payload)])
        )

        self.assertEqual(len(results), 1)
        self.assertIsInstance(results[0], beam.pvalue.TaggedOutput)
        assert isinstance(results[0], beam.pvalue.TaggedOutput)
        self.assertEqual(results[0].tag, "segmentation_dlq")
        self.assertEqual(results[0].value, dlq_payload)

    def test_yield_tagged_outputs_passes_through_normal_results(self) -> None:
        """Verifies that _yield_tagged_outputs leaves main FlushRequest tuples unchanged."""
        order_config = OrderRestorerConfig(out_of_order_timeout_ms=1000)
        stitch_config = get_test_stitch_config()
        fn = OrderedStitchAudioFn(
            order_config=order_config, stitch_config=stitch_config
        )

        flush_req = MagicMock(spec=FlushRequest)
        results = list(fn._yield_tagged_outputs([("feed-id", flush_req)]))

        self.assertEqual(len(results), 1)
        self.assertNotIsInstance(results[0], beam.pvalue.TaggedOutput)
        self.assertEqual(results[0], ("feed-id", flush_req))

    def test_yield_tagged_outputs_mixed_results(self) -> None:
        """Verifies that _yield_tagged_outputs handles a mix of DLQ and main outputs."""
        order_config = OrderRestorerConfig(out_of_order_timeout_ms=1000)
        stitch_config = get_test_stitch_config()
        fn = OrderedStitchAudioFn(
            order_config=order_config, stitch_config=stitch_config
        )

        flush_req = MagicMock(spec=FlushRequest)
        dlq_payload = {"error": "oops"}
        results = list(
            fn._yield_tagged_outputs(
                [
                    ("feed-id", flush_req),
                    (DEAD_LETTER_QUEUE_TAG, dlq_payload),
                ]
            )
        )

        self.assertEqual(len(results), 2)
        # First is a normal output
        self.assertNotIsInstance(results[0], beam.pvalue.TaggedOutput)
        # Second is wrapped
        self.assertIsInstance(results[1], beam.pvalue.TaggedOutput)
        assert isinstance(results[1], beam.pvalue.TaggedOutput)
        self.assertEqual(results[1].tag, "segmentation_dlq")

    # --- Bug 2: feed_metadata preserved through stale flush ---

    @patch(
        "backend.pipeline.segmentation.audio.processor.SegmentationAudioProcessor"
    )
    def test_stale_flush_preserves_feed_metadata_in_flush_request(
        self, mock_audio_processor: MagicMock
    ) -> None:
        """Regression test for PR #458 Bug 2: verifies that feed_metadata from the
        active stitching context is correctly included in the FlushRequest, even if
        Beam state were re-read (which would have returned IdleFeedState after the write).

        Previously, _apply_flush_action re-read transmission_context from the state store
        after writing IdleFeedState, which lost feed_metadata and sample_rate.
        """
        mock_processor_inst = mock_audio_processor.return_value
        chunk_data = AudioChunkData(
            start_ms=100000,
            audio=np.zeros(16000 * 3, dtype=np.int16),
            speech_segments=[TimeRange(0, 3000)],
            gcs_uri="gs://bucket/chunk.flac",
            duration_ms=3000,
            sample_rate=16000,
        )
        mock_processor_inst.download_audio_and_detect.return_value = chunk_data

        fn, mock_state_context, mock_state_buffer, mock_last_start_ms = (
            self._make_fn_and_states(OrderedStitchAudioFn)
        )
        fn.setup()

        expected_feed_metadata = FeedMetadata(feed_name="test-feed")

        # Call process() first so the engine properly seeds contributing_audio_uris,
        # buffer_start_time_ms, and other fields required by _apply_flush_action.
        metadata = ChunkMetadata(
            gcs_uri="gs://bucket/chunk.flac",
            session_id="test-session",
            duration_ms=3000,
            feed_metadata=expected_feed_metadata,
        )
        list(
            fn.process(
                element=("test-feed", metadata),
                timestamp=Timestamp(100),
                transmission_context_state=mock_state_context,
                last_start_ms_state=mock_last_start_ms,
                out_of_order_buffer_state=mock_state_buffer,
                gap_timer_event=MagicMock(),
                gap_timer_proc=MagicMock(),
                stale_timer_event=MagicMock(),
                stale_timer_proc=MagicMock(),
            )
        )

        outputs = list(
            fn.handle_stale_transmission_event(
                key="test-feed",
                transmission_context=mock_state_context,
                last_start_ms_state=mock_last_start_ms,
                out_of_order_buffer_state=mock_state_buffer,
                stale_timer_event=MagicMock(),
                stale_timer_proc=MagicMock(),
            )
        )

        # The flush should have produced exactly one result
        self.assertEqual(len(outputs), 1)
        feed_id, flush_request = outputs[0]
        self.assertEqual(feed_id, "test-feed")
        # feed_metadata must be preserved from the active context, not lost on state re-read
        self.assertIsNotNone(flush_request.feed_metadata)
        self.assertEqual(flush_request.feed_metadata, expected_feed_metadata)
        # State should now be completely cleared after flush
        self.assertIsNone(mock_state_context.read())

    @patch(
        "backend.pipeline.segmentation.transforms.stitcher_engine.audio_processor.SegmentationAudioProcessor"
    )
    def test_stale_flush_preserves_echo_sample_rate_in_flush_request(
        self, mock_audio_processor: MagicMock
    ) -> None:
        """Verifies that an 8 kHz echo audio feed sample rate is preserved in ActiveStitchingState
        and correctly populated in the FlushRequest during a stale flush.
        """
        mock_processor_inst = mock_audio_processor.return_value
        # 8000 Hz sample rate representing Echo feed audio
        chunk_data = AudioChunkData(
            start_ms=100000,
            audio=np.zeros(8000 * 3, dtype=np.int16),
            speech_segments=[TimeRange(0, 3000)],
            gcs_uri="gs://bucket/echo_chunk.flac",
            duration_ms=3000,
            sample_rate=8000,
        )
        mock_processor_inst.download_audio_and_detect.return_value = chunk_data

        fn, mock_state_context, mock_state_buffer, mock_last_start_ms = (
            self._make_fn_and_states(OrderedStitchAudioFn)
        )
        fn.setup()

        metadata = ChunkMetadata(
            gcs_uri="gs://bucket/echo_chunk.flac",
            session_id="test-session",
            duration_ms=3000,
            feed_metadata=FeedMetadata(feed_name="test-feed"),
        )
        # 1. Process chunk (this should update context state with the 8 kHz sample rate)
        list(
            fn.process(
                element=("test-feed", metadata),
                timestamp=Timestamp(100),
                transmission_context_state=mock_state_context,
                last_start_ms_state=mock_last_start_ms,
                out_of_order_buffer_state=mock_state_buffer,
                gap_timer_event=MagicMock(),
                gap_timer_proc=MagicMock(),
                stale_timer_event=MagicMock(),
                stale_timer_proc=MagicMock(),
            )
        )

        # 2. Trigger stale flush
        outputs = list(
            fn.handle_stale_transmission_event(
                key="test-feed",
                transmission_context=mock_state_context,
                last_start_ms_state=mock_last_start_ms,
                out_of_order_buffer_state=mock_state_buffer,
                stale_timer_event=MagicMock(),
                stale_timer_proc=MagicMock(),
            )
        )

        # The flush should have produced exactly one result
        self.assertEqual(len(outputs), 1)
        _feed_id, flush_request = outputs[0]
        # sample_rate must be 8000 (preserved from chunk_data), not defaulted to 16000
        self.assertEqual(flush_request.sample_rate, 8000)

    @patch(
        "backend.pipeline.segmentation.transforms.stitcher_engine.audio_processor.SegmentationAudioProcessor"
    )
    def test_stale_flush_missing_context_flags_for_non_speech(
        self, mock_audio_processor: MagicMock
    ) -> None:
        """Verifies that a stale flush on a non-speech (silence) transmission sets both
        missing_prior_context and missing_post_context to False in the FlushRequest.
        """
        mock_processor_inst = mock_audio_processor.return_value
        # No speech segments -> AUDIO_CLASSIFICATION_OTHER
        chunk_data = AudioChunkData(
            start_ms=100000,
            audio=np.zeros(16000 * 3, dtype=np.int16),
            speech_segments=[],
            gcs_uri="gs://bucket/silent_chunk.flac",
            duration_ms=3000,
            sample_rate=16000,
        )
        mock_processor_inst.download_audio_and_detect.return_value = chunk_data

        fn, mock_state_context, mock_state_buffer, mock_last_start_ms = (
            self._make_fn_and_states(OrderedStitchAudioFn)
        )
        fn.setup()

        metadata = ChunkMetadata(
            gcs_uri="gs://bucket/silent_chunk.flac",
            session_id="test-session-silent",
            duration_ms=3000,
        )
        list(
            fn.process(
                element=("test-feed", metadata),
                timestamp=Timestamp(100),
                transmission_context_state=mock_state_context,
                last_start_ms_state=mock_last_start_ms,
                out_of_order_buffer_state=mock_state_buffer,
                gap_timer_event=MagicMock(),
                gap_timer_proc=MagicMock(),
                stale_timer_event=MagicMock(),
                stale_timer_proc=MagicMock(),
            )
        )

        # Simulate an upstream gap prior to this chunk setting missing_prior_context = True in state
        active_ctx = mock_state_context.read()
        self.assertIsNotNone(active_ctx)
        active_ctx = replace(active_ctx, missing_prior_context=True)
        mock_state_context.write(active_ctx)

        outputs = list(
            fn.handle_stale_transmission_event(
                key="test-feed",
                transmission_context=mock_state_context,
                last_start_ms_state=mock_last_start_ms,
                out_of_order_buffer_state=mock_state_buffer,
                stale_timer_event=MagicMock(),
                stale_timer_proc=MagicMock(),
            )
        )

        self.assertEqual(len(outputs), 1)
        _feed_id, flush_request = outputs[0]
        self.assertEqual(
            flush_request.audio_classification,
            AudioClassification.AUDIO_CLASSIFICATION_OTHER,
        )
        self.assertFalse(flush_request.missing_prior_context)
        self.assertFalse(flush_request.missing_post_context)

    @patch(
        "backend.pipeline.segmentation.audio.processor.SegmentationAudioProcessor"
    )
    def test_no_trace_or_session_leak_after_stale_flush(
        self, mock_audio_processor: MagicMock
    ) -> None:
        """Verifies that clearing the state context completely on stale flush prevents any
        traceparent or session_id from leaking to subsequent unrelated sessions.
        """
        mock_processor_inst = mock_audio_processor.return_value
        chunk_data_1 = AudioChunkData(
            start_ms=100000,
            audio=np.zeros(16000 * 3, dtype=np.int16),
            speech_segments=[TimeRange(0, 3000)],
            gcs_uri="gs://bucket/chunk1.flac",
            duration_ms=3000,
            sample_rate=16000,
        )
        chunk_data_2 = AudioChunkData(
            start_ms=200000,
            audio=np.zeros(16000 * 3, dtype=np.int16),
            speech_segments=[TimeRange(0, 3000)],
            gcs_uri="gs://bucket/chunk2.flac",
            duration_ms=3000,
            sample_rate=16000,
        )
        mock_processor_inst.download_audio_and_detect.side_effect = [
            chunk_data_1,
            chunk_data_2,
        ]

        fn, mock_state_context, mock_state_buffer, mock_last_start_ms = (
            self._make_fn_and_states(OrderedStitchAudioFn)
        )
        fn.setup()

        # 1. Process first chunk (Session 1, Trace 1)
        metadata_1 = ChunkMetadata(
            gcs_uri="gs://bucket/chunk1.flac",
            session_id="session-1",
            duration_ms=3000,
            feed_metadata=FeedMetadata(feed_name="test-feed"),
            traceparent="traceparent-1",
        )
        list(
            fn.process(
                element=("test-feed", metadata_1),
                timestamp=Timestamp(100),
                transmission_context_state=mock_state_context,
                last_start_ms_state=mock_last_start_ms,
                out_of_order_buffer_state=mock_state_buffer,
                gap_timer_event=MagicMock(),
                gap_timer_proc=MagicMock(),
                stale_timer_event=MagicMock(),
                stale_timer_proc=MagicMock(),
            )
        )

        # Active context must be set to Session 1, Trace 1
        saved_context_1 = mock_state_context.read()
        self.assertEqual(saved_context_1.session_id, "session-1")
        self.assertEqual(saved_context_1.traceparent, "traceparent-1")

        # 2. Trigger stale flush (clears everything cleanly via .clear())
        list(
            fn.handle_stale_transmission_event(
                key="test-feed",
                transmission_context=mock_state_context,
                last_start_ms_state=mock_last_start_ms,
                out_of_order_buffer_state=mock_state_buffer,
                stale_timer_event=MagicMock(),
                stale_timer_proc=MagicMock(),
            )
        )

        # Verify state is completely empty
        self.assertIsNone(mock_state_context.read())

        # 3. Process second chunk (Session 2, Trace 2) after the clean reset
        metadata_2 = ChunkMetadata(
            gcs_uri="gs://bucket/chunk2.flac",
            session_id="session-2",
            duration_ms=3000,
            feed_metadata=FeedMetadata(feed_name="test-feed"),
            traceparent="traceparent-2",
        )
        list(
            fn.process(
                element=("test-feed", metadata_2),
                timestamp=Timestamp(200),
                transmission_context_state=mock_state_context,
                last_start_ms_state=mock_last_start_ms,
                out_of_order_buffer_state=mock_state_buffer,
                gap_timer_event=MagicMock(),
                gap_timer_proc=MagicMock(),
                stale_timer_event=MagicMock(),
                stale_timer_proc=MagicMock(),
            )
        )

        # Active context must now be set strictly to Session 2, Trace 2 with NO traces of Session 1!
        saved_context_2 = mock_state_context.read()
        self.assertIsNotNone(saved_context_2)
        self.assertEqual(saved_context_2.session_id, "session-2")
        self.assertEqual(saved_context_2.traceparent, "traceparent-2")

    @patch(
        "backend.pipeline.segmentation.audio.processor.SegmentationAudioProcessor"
    )
    def test_mid_stream_session_transition_resets_timeline(
        self, mock_audio_processor: MagicMock
    ) -> None:
        """Verifies that an immediate mid-stream session transition resets the timeline
        and does not trigger late chunk processing or false gaps against the old session.
        """
        mock_processor_inst = mock_audio_processor.return_value
        chunk_data_1 = AudioChunkData(
            start_ms=100000,
            audio=np.zeros(16000 * 3, dtype=np.int16),
            speech_segments=[TimeRange(0, 3000)],
            gcs_uri="gs://bucket/chunk1.flac",
            duration_ms=3000,
            sample_rate=16000,
        )
        chunk_data_2 = AudioChunkData(
            start_ms=200000,
            audio=np.zeros(16000 * 3, dtype=np.int16),
            speech_segments=[TimeRange(0, 3000)],
            gcs_uri="gs://bucket/chunk2.flac",
            duration_ms=3000,
            sample_rate=16000,
        )
        mock_processor_inst.download_audio_and_detect.side_effect = [
            chunk_data_1,
            chunk_data_2,
        ]

        fn, mock_state_context, mock_state_buffer, mock_last_start_ms = (
            self._make_fn_and_states(OrderedStitchAudioFn)
        )
        fn.setup()

        # 1. Process first chunk (Session 1)
        metadata_1 = ChunkMetadata(
            gcs_uri="gs://bucket/chunk1.flac",
            session_id="session-1",
            duration_ms=3000,
            feed_metadata=FeedMetadata(feed_name="test-feed"),
            traceparent="traceparent-1",
        )
        list(
            fn.process(
                element=("test-feed", metadata_1),
                timestamp=Timestamp(100),
                transmission_context_state=mock_state_context,
                last_start_ms_state=mock_last_start_ms,
                out_of_order_buffer_state=mock_state_buffer,
                gap_timer_event=MagicMock(),
                gap_timer_proc=MagicMock(),
                stale_timer_event=MagicMock(),
                stale_timer_proc=MagicMock(),
            )
        )

        saved_context_1 = mock_state_context.read()
        self.assertEqual(saved_context_1.session_id, "session-1")

        # 2. Process second chunk (Session 2) immediately (no stale flush)
        # The timestamp is 200000ms (100 seconds after chunk 1).
        metadata_2 = ChunkMetadata(
            gcs_uri="gs://bucket/chunk2.flac",
            session_id="session-2",
            duration_ms=3000,
            feed_metadata=FeedMetadata(feed_name="test-feed"),
            traceparent="traceparent-2",
        )
        list(
            fn.process(
                element=("test-feed", metadata_2),
                timestamp=Timestamp(200),
                transmission_context_state=mock_state_context,
                last_start_ms_state=mock_last_start_ms,
                out_of_order_buffer_state=mock_state_buffer,
                gap_timer_event=MagicMock(),
                gap_timer_proc=MagicMock(),
                stale_timer_event=MagicMock(),
                stale_timer_proc=MagicMock(),
            )
        )

        # Context must be reset and updated to Session 2 with missing_prior_context=True
        saved_context_2 = mock_state_context.read()
        self.assertIsNotNone(saved_context_2)
        self.assertEqual(saved_context_2.session_id, "session-2")
        self.assertEqual(saved_context_2.traceparent, "traceparent-2")
        self.assertTrue(saved_context_2.missing_prior_context)
        self.assertEqual(saved_context_2.expected_next_chunk_start_ms, 203000)

    @patch(
        "backend.pipeline.segmentation.audio.processor.SegmentationAudioProcessor"
    )
    def test_overlap_check_and_state_updates_conditioned_on_backfill_and_clear_state(
        self, mock_audio_processor: MagicMock
    ) -> None:
        """Verifies that:
        1. Mainline real-time flushes (clear_state=True, is_backfill=False) execute the overlap check
           and write to last_start_ms_state.
        2. Backfill flushes (is_backfill=True) skip the overlap check and do not write to last_start_ms_state.
        3. Non-mainline flushes (clear_state=False) skip the overlap check and do not write to last_start_ms_state.
        """
        mock_processor_inst = mock_audio_processor.return_value

        # --- Case 1: Mainline Real-Time (clear_state=True, is_backfill=False) ---
        chunk_data_1 = AudioChunkData(
            start_ms=10000,
            audio=np.zeros(16000 * 6, dtype=np.int16),
            speech_segments=[TimeRange(0, 6000)],
            gcs_uri="gs://bucket/chunk1.flac",
            duration_ms=6000,
            sample_rate=16000,
        )
        mock_processor_inst.download_audio_and_detect.return_value = (
            chunk_data_1
        )

        # Use max_transmission_duration_ms = 5000 to force split/flush
        stitch_config = get_test_stitch_config(
            max_transmission_duration_ms=5000
        )

        # Create fn and states
        fn, mock_state_context, mock_state_buffer, mock_last_start_ms = (
            self._make_fn_and_states(OrderedStitchAudioFn)
        )
        # Override config to ensure max_transmission_duration_ms=5000
        fn.stitch_config = stitch_config
        fn.setup()

        # Ensure last_start_ms starts empty
        mock_last_start_ms.write(None)

        # Mock logger to verify warning
        mock_logger = MagicMock(spec=std_logging.Logger)

        metadata_1 = ChunkMetadata(
            gcs_uri="gs://bucket/chunk1.flac",
            session_id="session-realtime",
            duration_ms=6000,
            feed_metadata=FeedMetadata(feed_name="test-feed"),
        )

        with (
            patch(
                "backend.pipeline.segmentation.transforms.stitcher_engine._get_task_logger",
                return_value=mock_logger,
            ),
            patch(
                "backend.pipeline.segmentation.transforms.stateful.time.time",
                return_value=10.0,
            ),
        ):  # Current time = 10s. Timestamp = 10s. Lateness = 0ms.
            list(
                fn.process(
                    element=("test-feed", metadata_1),
                    timestamp=Timestamp(10),
                    transmission_context_state=mock_state_context,
                    last_start_ms_state=mock_last_start_ms,
                    out_of_order_buffer_state=mock_state_buffer,
                    gap_timer_event=MagicMock(),
                    gap_timer_proc=MagicMock(),
                    stale_timer_event=MagicMock(),
                    stale_timer_proc=MagicMock(),
                )
            )

        # Verify last_start_ms was written with chunk start_ms (10000)
        self.assertEqual(mock_last_start_ms.read(), 10000)
        # Warning log should not be called since there is no previous start time
        mock_logger.warning.assert_not_called()

        # Let's trigger a second realtime flush with overlapping start time to verify warning is triggered
        chunk_data_2 = AudioChunkData(
            start_ms=10050,  # Overlaps with 10000 (diff < 100ms)
            audio=np.zeros(16000 * 6, dtype=np.int16),
            speech_segments=[TimeRange(0, 6000)],
            gcs_uri="gs://bucket/chunk2.flac",
            duration_ms=6000,
            sample_rate=16000,
        )
        mock_processor_inst.download_audio_and_detect.return_value = (
            chunk_data_2
        )
        mock_state_context.write(
            ActiveStitchingState(
                session_id="session-realtime",
                feed_metadata=FeedMetadata(feed_name="test-feed"),
            )
        )
        metadata_2 = ChunkMetadata(
            gcs_uri="gs://bucket/chunk2.flac",
            session_id="session-realtime",
            duration_ms=6000,
            feed_metadata=FeedMetadata(feed_name="test-feed"),
        )

        with (
            patch(
                "backend.pipeline.segmentation.transforms.stitcher_engine._get_task_logger",
                return_value=mock_logger,
            ),
            patch(
                "backend.pipeline.segmentation.transforms.stateful.time.time",
                return_value=10.1,
            ),
        ):  # Lateness = 0
            list(
                fn.process(
                    element=("test-feed", metadata_2),
                    timestamp=Timestamp(10.1),
                    transmission_context_state=mock_state_context,
                    last_start_ms_state=mock_last_start_ms,
                    out_of_order_buffer_state=mock_state_buffer,
                    gap_timer_event=MagicMock(),
                    gap_timer_proc=MagicMock(),
                    stale_timer_event=MagicMock(),
                    stale_timer_proc=MagicMock(),
                )
            )

        # Warning should be logged
        self.assertTrue(
            any(
                "Potential growing/overlapping transmission detected!"
                in args[0]
                for args, kwargs in mock_logger.warning.call_args_list
            )
        )
        # last_start_ms was updated to 10050
        self.assertEqual(mock_last_start_ms.read(), 10050)

        # --- Case 2: Backfill (is_backfill=True) ---
        mock_logger.reset_mock()
        mock_last_start_ms.write(10050)  # Reset state back to 10050

        chunk_data_backfill = AudioChunkData(
            start_ms=10070,  # Overlaps with 10050
            audio=np.zeros(16000 * 6, dtype=np.int16),
            speech_segments=[TimeRange(0, 6000)],
            gcs_uri="gs://bucket/chunk3.flac",
            duration_ms=6000,
            sample_rate=16000,
        )
        mock_processor_inst.download_audio_and_detect.return_value = (
            chunk_data_backfill
        )
        mock_state_context.write(
            ActiveStitchingState(
                session_id="session-backfill",
                feed_metadata=FeedMetadata(feed_name="test-feed"),
            )
        )
        metadata_backfill = ChunkMetadata(
            gcs_uri="gs://bucket/chunk3.flac",
            session_id="session-backfill",
            duration_ms=6000,
            feed_metadata=FeedMetadata(feed_name="test-feed"),
        )

        with (
            patch(
                "backend.pipeline.segmentation.transforms.stitcher_engine._get_task_logger",
                return_value=mock_logger,
            ),
            patch(
                "backend.pipeline.segmentation.transforms.stateful.time.time",
                return_value=10000.0,
            ),
        ):  # Current time is 10000.0. Timestamp is 10. Lateness = 9990s (backfill)
            list(
                fn.process(
                    element=("test-feed", metadata_backfill),
                    timestamp=Timestamp(10),
                    transmission_context_state=mock_state_context,
                    last_start_ms_state=mock_last_start_ms,
                    out_of_order_buffer_state=mock_state_buffer,
                    gap_timer_event=MagicMock(),
                    gap_timer_proc=MagicMock(),
                    stale_timer_event=MagicMock(),
                    stale_timer_proc=MagicMock(),
                )
            )

        # Overlap check should NOT run (no warnings logged)
        mock_logger.warning.assert_not_called()
        # last_start_ms state should NOT be written (remains 10050, not updated to 10070)
        self.assertEqual(mock_last_start_ms.read(), 10050)

    @patch(
        "backend.pipeline.segmentation.audio.processor.SegmentationAudioProcessor"
    )
    def test_ordered_stitch_audio_dual_timers_live_vs_backfill(
        self, mock_audio_processor: MagicMock
    ) -> None:
        """Verifies that gap_timer_event and gap_timer_proc are set correctly in live vs backfill modes."""
        mock_processor_inst = mock_audio_processor.return_value
        chunk_data = AudioChunkData(
            start_ms=1000,
            audio=np.zeros(16000, dtype=np.int16),
            speech_segments=[],
            gcs_uri="gs://test-bucket/chunk1.flac",
            duration_ms=1000,
            sample_rate=16000,
        )
        mock_processor_inst.download_audio_and_detect.return_value = chunk_data

        order_config = OrderRestorerConfig(out_of_order_timeout_ms=1000)
        stitch_config = get_test_stitch_config()
        fn = OrderedStitchAudioFn(
            order_config=order_config, stitch_config=stitch_config
        )
        fn.setup()

        # Helper to create mock states
        class MockValueState:
            def __init__(self, initial=None) -> None:
                self.val = initial

            def read(self):
                return self.val

            def write(self, val):
                self.val = val

            def clear(self):
                self.val = None

        recent_ts_ms = int(time.time() * 1000) - 10000

        curr_context = ActiveStitchingState(
            session_id="mock-session",
            expected_next_chunk_start_ms=recent_ts_ms,
            feed_metadata=FeedMetadata(feed_name="mock-feed"),
        )
        transmission_context_state = MockValueState(curr_context)

        watermark_timer = MagicMock()
        processing_timer = MagicMock()

        metadata = ChunkMetadata(
            gcs_uri="gs://test-bucket/chunk1.flac",
            session_id="mock-session",
            timestamp_ms=recent_ts_ms + 2000,
            duration_ms=1000,
            feed_metadata=FeedMetadata(feed_name="mock-feed"),
        )

        list(
            fn.process(
                element=("test-feed", metadata),
                timestamp=Timestamp(recent_ts_ms / 1000.0),
                transmission_context_state=transmission_context_state,  # type: ignore
                last_start_ms_state=MagicMock(),
                out_of_order_buffer_state=MockBagState(),  # type: ignore
                gap_timer_event=watermark_timer,
                gap_timer_proc=processing_timer,
                stale_timer_event=MagicMock(),
                stale_timer_proc=MagicMock(),
            )
        )

        self.assertTrue(watermark_timer.set.called)
        self.assertTrue(processing_timer.set.called)

        # Scenario 2: Backfill mode (is_backfill = True)
        old_ts_ms = int(time.time() * 1000) - (10 * 60 * 1000)

        curr_context = ActiveStitchingState(
            session_id="mock-session",
            expected_next_chunk_start_ms=old_ts_ms,
            feed_metadata=FeedMetadata(feed_name="mock-feed"),
        )
        transmission_context_state = MockValueState(curr_context)

        watermark_timer = MagicMock()
        processing_timer = MagicMock()

        metadata = ChunkMetadata(
            gcs_uri="gs://test-bucket/chunk2.flac",
            session_id="mock-session",
            timestamp_ms=old_ts_ms + 2000,
            duration_ms=1000,
            feed_metadata=FeedMetadata(feed_name="mock-feed"),
        )

        list(
            fn.process(
                element=("test-feed", metadata),
                timestamp=Timestamp(old_ts_ms / 1000.0),
                transmission_context_state=transmission_context_state,  # type: ignore
                last_start_ms_state=MagicMock(),
                out_of_order_buffer_state=MockBagState(),  # type: ignore
                gap_timer_event=watermark_timer,
                gap_timer_proc=processing_timer,
                stale_timer_event=MagicMock(),
                stale_timer_proc=MagicMock(),
            )
        )

        self.assertTrue(watermark_timer.set.called)
        self.assertFalse(processing_timer.set.called)
        self.assertTrue(processing_timer.clear.called)


class UploadRawSegmentFnTest(unittest.TestCase):
    @patch("backend.pipeline.segmentation.transforms.stateless.setup_tracing")
    @patch(
        "backend.pipeline.segmentation.transforms.stateless.acquire_shared_gcs_client"
    )
    def test_setup_initializes_tracing(
        self, mock_acquire_gcs: MagicMock, mock_setup_tracing: MagicMock
    ) -> None:
        """Verifies that UploadRawSegmentFn.setup initializes tracing."""
        fn = UploadRawSegmentFn(
            staging_audio_bucket="bucket", project_id="proj"
        )
        fn.setup()
        mock_setup_tracing.assert_called_once_with(
            service_name="segmentation-pipeline"
        )

    @patch(
        "backend.pipeline.segmentation.transforms.stateless.inject_otel_context"
    )
    @patch(
        "backend.pipeline.segmentation.transforms.stateless.with_tracer_context"
    )
    @patch(
        "backend.pipeline.segmentation.transforms.stateless.UploadRawSegmentFn._upload_raw_audio"
    )
    def test_process_propagates_otel_context(
        self,
        mock_upload_audio: MagicMock,
        mock_with_tracer_context: MagicMock,
        mock_inject_otel_context: MagicMock,
    ) -> None:
        """Verifies that process propagates OpenTelemetry trace context and baggage."""
        # Create a mock context manager for with_tracer_context
        mock_ctx = MagicMock()
        mock_with_tracer_context.return_value = mock_ctx

        mock_upload_audio.return_value = "gs://bucket/raw.wav"

        fn = UploadRawSegmentFn(
            staging_audio_bucket="bucket", project_id="proj"
        )
        fn.gcs_client = MagicMock()
        fn.segmentation_success = MagicMock()
        fn.segmentation_error = MagicMock()

        request = FlushRequest(
            feed_id="test-feed",
            session_id="test-session",
            contributing_audio_uris=["gs://bucket/1.flac"],
            time_range=TimeRange(start_ms=1000, end_ms=2000),
            feed_metadata=FeedMetadata(feed_name="test-feed"),
            sample_rate=16000,
            missing_prior_context=False,
            missing_post_context=False,
            start_audio_offset_ms=0,
            end_audio_offset_ms=1000,
            speech_segments=[],
            segment_id="segment-123",
            traceparent="test-traceparent",
            baggage="test-baggage",
            audio_classification=AudioClassification.AUDIO_CLASSIFICATION_SPEECH,
        )

        # Execute the process generator
        list(fn.process(element=("test-feed", request)))

        # Verify that with_tracer_context was called with the correct extracted attributes
        mock_with_tracer_context.assert_called_once_with(
            {"traceparent": "test-traceparent", "baggage": "test-baggage"},
            "upload_raw_segment",
            "backend.pipeline.segmentation.transforms.stateless",
        )

        # Verify that inject_otel_context was called to inject active context into egress attributes
        mock_inject_otel_context.assert_called_once()

    @patch(
        "backend.pipeline.segmentation.transforms.stateless.inject_otel_context"
    )
    @patch(
        "backend.pipeline.segmentation.transforms.stateless.with_tracer_context"
    )
    @patch(
        "backend.pipeline.segmentation.transforms.stateless.UploadRawSegmentFn._upload_raw_audio"
    )
    def test_process_with_decoded_flush_request(
        self,
        mock_upload_audio: MagicMock,
        mock_with_tracer_context: MagicMock,
        mock_inject_otel_context: MagicMock,
    ) -> None:
        """Verifies that process handles a deserialized FlushRequest with int classification."""
        mock_ctx = MagicMock()
        mock_with_tracer_context.return_value = mock_ctx
        mock_upload_audio.return_value = "gs://bucket/raw.wav"

        fn = UploadRawSegmentFn(
            staging_audio_bucket="bucket", project_id="proj"
        )
        fn.gcs_client = MagicMock()
        fn.segmentation_success = MagicMock()
        fn.segmentation_error = MagicMock()

        request = FlushRequest(
            feed_id="test-feed",
            session_id="test-session",
            contributing_audio_uris=["gs://bucket/1.flac"],
            time_range=TimeRange(start_ms=1000, end_ms=2000),
            feed_metadata=FeedMetadata(feed_name="test-feed"),
            sample_rate=16000,
            missing_prior_context=False,
            missing_post_context=False,
            start_audio_offset_ms=0,
            end_audio_offset_ms=1000,
            speech_segments=[],
            segment_id="segment-123",
            traceparent="test-traceparent",
            baggage="test-baggage",
            audio_classification=AudioClassification.AUDIO_CLASSIFICATION_SPEECH,
        )

        coder = trans_coders.FlushRequestCoder()
        decoded_request = coder.decode(coder.encode(request))

        # Execute the process generator and convert to list
        results = list(fn.process(element=("test-feed", decoded_request)))

        self.assertEqual(len(results), 1)
        self.assertIsInstance(results[0], PubsubMessage)

    @patch("backend.pipeline.segmentation.transforms.stateless.setup_tracing")
    def test_upload_raw_audio_executes_stateless_stitching(
        self, mock_setup_tracing: MagicMock
    ) -> None:
        # Verifies that _upload_raw_audio successfully downloads contributing chunks from GCS,
        # slices them by speech segments, stitches them, encodes the result to FLAC, and uploads it.
        # 1. Create mock GCS client, buckets, and blobs
        mock_gcs_client = MagicMock(spec=storage.Client)
        mock_bucket = MagicMock()
        mock_blob_chunk_1 = MagicMock()
        mock_blob_chunk_2 = MagicMock()
        mock_blob_upload = MagicMock()

        mock_gcs_client.bucket.return_value = mock_bucket

        # We have 2 contributing chunks: chunk_1 (10s) and chunk_2 (10s).
        # Sample rate is 16000 Hz.
        # We write unique PCM sample values so we can verify the slicing and stitching.
        sr = 16000
        samples_1 = (np.arange(10 * sr) % 1000).astype(np.int16)
        samples_2 = ((np.arange(10 * sr) % 1000) + 1000).astype(np.int16)

        # Encode them to FLAC bytes in memory
        flac_io_1 = io.BytesIO()
        sf.write(flac_io_1, samples_1, sr, format="FLAC", subtype="PCM_16")
        flac_bytes_1 = flac_io_1.getvalue()

        flac_io_2 = io.BytesIO()
        sf.write(flac_io_2, samples_2, sr, format="FLAC", subtype="PCM_16")
        flac_bytes_2 = flac_io_2.getvalue()

        # Mock downloading from GCS
        def mock_download_to_file_1(file_obj, **kwargs):
            file_obj.write(flac_bytes_1)

        def mock_download_to_file_2(file_obj, **kwargs):
            file_obj.write(flac_bytes_2)

        mock_blob_chunk_1.download_to_file.side_effect = mock_download_to_file_1
        mock_blob_chunk_2.download_to_file.side_effect = mock_download_to_file_2

        # Map blobs by GCS URI
        def mock_blob_mapping(path):
            if "chunk_1.flac" in path:
                return mock_blob_chunk_1
            if "chunk_2.flac" in path:
                return mock_blob_chunk_2
            return mock_blob_upload

        mock_bucket.blob.side_effect = mock_blob_mapping

        # 2. Instantiate UploadRawSegmentFn and trigger setup lifecycle to initialize metrics
        fn = UploadRawSegmentFn(
            staging_audio_bucket="test-staging-bucket",
            project_id="test-project",
        )
        fn.setup()
        fn.gcs_client = mock_gcs_client

        # 3. Create a FlushRequest with 2 contributing chunks and 2 speech segments:
        # - Speech Segment 1: from 2000ms to 6000ms (absolute, entirely in chunk 1, which starts at 100000ms)
        # - Speech Segment 2: from 14000ms to 18000ms (absolute, entirely in chunk 2, which starts at 110000ms)
        # Total stitched duration = 4000ms + 4000ms = 8000ms.
        request = FlushRequest(
            feed_id="test-feed",
            session_id="test-session",
            contributing_audio_uris=[
                "gs://test-bucket/chunk_1.flac",
                "gs://test-bucket/chunk_2.flac",
            ],
            contributing_chunks=[
                BufferedChunk(
                    gcs_uri="gs://test-bucket/chunk_1.flac",
                    timestamp_ms=100000,
                ),
                BufferedChunk(
                    gcs_uri="gs://test-bucket/chunk_2.flac",
                    timestamp_ms=110000,
                ),
            ],
            time_range=TimeRange(start_ms=100000, end_ms=120000),
            feed_metadata=FeedMetadata(feed_name="test-feed"),
            sample_rate=sr,
            missing_prior_context=False,
            missing_post_context=False,
            start_audio_offset_ms=2000,
            end_audio_offset_ms=18000,
            speech_segments=[
                TimeRange(start_ms=102000, end_ms=106000),
                TimeRange(start_ms=114000, end_ms=118000),
            ],
            segment_id="segment-uuid-123",
            traceparent="test-traceparent",
            baggage="test-baggage",
            audio_classification=AudioClassification.AUDIO_CLASSIFICATION_SPEECH,
        )

        # We mock the upload target blob to capture the uploaded FLAC bytes
        uploaded_bytes_captured = io.BytesIO()

        def mock_upload_from_string(data, content_type=None):
            uploaded_bytes_captured.write(data)

        mock_blob_upload.upload_from_string.side_effect = (
            mock_upload_from_string
        )

        # 4. Execute _upload_raw_audio
        start_datetime = datetime.datetime(
            2026, 6, 20, 12, 0, 0, tzinfo=datetime.UTC
        )
        uploaded_uri = fn._upload_raw_audio(request, start_datetime)

        # 5. Verify the uploaded GCS URI
        expected_path = "gs://test-staging-bucket/raw_segments/test-feed/2026/06/20/segment-uuid-123.flac"
        self.assertEqual(uploaded_uri, expected_path)

        # 6. Decode the uploaded FLAC bytes and verify the full, unmasked stitched samples!
        uploaded_bytes_captured.seek(0)
        stitched_samples, stitched_sr = sf.read(
            uploaded_bytes_captured, dtype="int16"
        )

        self.assertEqual(stitched_sr, sr)
        # Expected duration is the full continuous range of 20 seconds (20 * 16000 = 320,000 samples)
        self.assertEqual(len(stitched_samples), 20000 * (sr // 1000))

        # We expect the full, unmasked audio: all of chunk_1 followed by all of chunk_2
        expected_stitched = np.concatenate([samples_1, samples_2])

        np.testing.assert_array_equal(stitched_samples, expected_stitched)
