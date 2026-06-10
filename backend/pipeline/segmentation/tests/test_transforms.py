"""Tests for the StitchAudioFn, TranscribeAudioFn, and related transformations."""

import logging as std_logging
import unittest
from collections.abc import Callable
from typing import Any
from unittest.mock import MagicMock, patch

import apache_beam as beam
import numpy as np
from apache_beam.io.gcp.pubsub import PubsubMessage
from apache_beam.options.pipeline_options import (
    PipelineOptions,
)
from apache_beam.testing.test_pipeline import TestPipeline as BeamTestPipeline
from apache_beam.testing.test_stream import TestStream
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.transforms.window import TimestampedValue
from apache_beam.utils.timestamp import Timestamp
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
    BufferedChunk,
    ChunkMetadata,
    FeedMetadata,
    FlushRequest,
    OrderRestorerConfig,
    StitchAudioConfig,
    TimeRange,
)
from backend.pipeline.segmentation.transforms.stateful import (
    SHARED_RESOURCE_HANDLE,
    OrderedContinuousStitchAudioFn,
    OrderedSegmentedStitchAudioFn,
)
from backend.pipeline.segmentation.transforms.stateless import (
    ParseAndKeyFn,
)
from backend.pipeline.segmentation.utils import get_duration_ms

# Test Helper: override ChunkMetadata locally in tests to default is_continuous to True
_OriginalChunkMetadata = ChunkMetadata


def ChunkMetadata(*args: Any, **kwargs: Any) -> Any:
    kwargs.setdefault("is_continuous", True)
    return _OriginalChunkMetadata(*args, **kwargs)


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
                            "test-feed",
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

    def test_parse_and_key_mismatched_routing_segmented_dlq(self) -> None:
        """Verifies that a continuous source type received on a segmented subscription is routed to the DLQ."""
        chunk = ContinuousAudio(
            gcs_uri="gs://test-bucket/path/to/test.flac",
            session_id="mock-session-id",
            feed_name="mock-feed-name",
            duration_ms=1000,
            feed_id="test-feed",
        )
        mock_msg = PubsubMessage(
            chunk.SerializeToString(),
            {"feed_id": "test-feed", "source_type": "bcfy_feeds"},
        )
        options = PipelineOptions(
            flags=[
                "--continuous_input_subscription=projects/p/subscriptions/a",
                "--segmented_input_subscription=projects/p/subscriptions/b",
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
                    "Received continuous source type 'bcfy_feeds' on segmented subscription"
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
        """Verifies that OrderedContinuousStitchAudioFn.process calls with_tracer_context."""
        order_config = OrderRestorerConfig(out_of_order_timeout_ms=1000)
        stitch_config = get_test_stitch_config()
        fn = OrderedContinuousStitchAudioFn(
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
                transmission_buffer_state=MagicMock(),
                transmission_context_state=mock_state,
                last_start_ms_state=MagicMock(),
                out_of_order_timer=mock_timer,
                stale_timer_event=MagicMock(),
                stale_timer_proc=MagicMock(),
            )
        )

        mock_with_tracer_context.assert_any_call(
            "mock-traceparent",
            "stitching_process",
            "backend.pipeline.segmentation.transforms.stateful",
        )
        mock_with_tracer_context.assert_any_call(
            "mock-traceparent",
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
        fn = OrderedContinuousStitchAudioFn(
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
            fn.handle_gap_timeout(
                feed_id="test-feed",
                transmission_buffer_state=MagicMock(),
                transmission_context_state=mock_state,
                last_start_ms_state=MagicMock(),
                stale_timer_event=MagicMock(),
                stale_timer_proc=MagicMock(),
            )
        )

        mock_with_tracer_context.assert_any_call(
            "mock-traceparent-context",
            "handle_audio_gap",
            "backend.pipeline.segmentation.transforms.stateful",
        )
        mock_with_tracer_context.assert_any_call(
            "",
            "stitching_single_chunk",
            "backend.pipeline.segmentation.transforms.stateful",
        )

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

        fn = OrderedContinuousStitchAudioFn(
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
        transmission_buffer_state = MockBagState()
        transmission_buffer_state.add(
            np.ones(16000, dtype=np.int16)
        )  # Main buffer content

        out_of_order_timer = MagicMock()
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
                transmission_buffer_state=transmission_buffer_state,  # type: ignore
                transmission_context_state=transmission_context_state,  # type: ignore
                out_of_order_timer=out_of_order_timer,
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
        self.assertEqual(
            len(transmission_buffer_state.read()),
            1,
            "Main buffer should not be cleared",
        )

    @patch(
        "backend.pipeline.segmentation.audio.processor.SegmentationAudioProcessor"
    )
    def test_ordered_stitch_audio_flushes_on_stale_timer(
        self, mock_audio_processor: MagicMock
    ) -> None:
        """Verifies that OrderedContinuousStitchAudioFn flushes buffered audio when the stale timer fires."""
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
                    OrderedContinuousStitchAudioFn(
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
    def test_segmented_sample_rate_isolation(
        self, mock_audio_processor: MagicMock
    ) -> None:
        """Verifies that consecutive segmented files with different sample rates are isolated cleanly,
        ensuring the second segment's FlushRequest receives its own native sample rate rather than leaking from the first.
        """
        mock_processor_inst = mock_audio_processor.return_value

        # Track which GCS URI is downloaded to return different sample rates
        def download_side_effect(gcs_uri, *args, **kwargs):
            if "first_chunk" in gcs_uri:
                return AudioChunkData(
                    start_ms=100000,
                    audio=np.zeros(22050, dtype=np.int16),
                    sample_rate=22050,  # First is 22.05kHz
                    speech_segments=[TimeRange(0, 1000)],
                    gcs_uri=gcs_uri,
                    duration_ms=1000,
                )
            return AudioChunkData(
                start_ms=200000,
                audio=np.zeros(8000, dtype=np.int16),
                sample_rate=8000,  # Second is 8kHz
                speech_segments=[TimeRange(0, 1000)],
                gcs_uri=gcs_uri,
                duration_ms=1000,
            )

        mock_processor_inst.download_audio_and_detect.side_effect = (
            download_side_effect
        )
        mock_processor_inst.preprocess_audio.side_effect = lambda x: x

        order_config = OrderRestorerConfig(out_of_order_timeout_ms=1000)
        stitch_config = get_test_stitch_config()

        options = PipelineOptions(
            flags=[
                "--continuous_input_subscription=projects/p/subscriptions/a",
                "--segmented_input_subscription=projects/p/subscriptions/b",
                "--output_topic=b",
                "--project=c",
            ]
        )

        metadata_chunk1 = ChunkMetadata(
            gcs_uri="gs://test-bucket/first_chunk.flac",
            session_id="session-A",
            duration_ms=1000,
            feed_metadata=FeedMetadata(feed_name="f"),
        )
        metadata_chunk2 = ChunkMetadata(
            gcs_uri="gs://test-bucket/second_chunk.flac",
            session_id="session-B",
            duration_ms=1000,
            feed_metadata=FeedMetadata(feed_name="f"),
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
                    [TimestampedValue(("test-feed", metadata_chunk1), 100)]
                )
                .advance_watermark_to(200)
                .add_elements(
                    [TimestampedValue(("test-feed", metadata_chunk2), 200)]
                )
                .advance_watermark_to_infinity()
            )

            results = (
                p
                | test_stream
                | beam.ParDo(
                    OrderedSegmentedStitchAudioFn(
                        order_config=order_config, stitch_config=stitch_config
                    )
                )
            )

            def assert_results(msgs):
                assert len(msgs) == 2
                msgs.sort(key=lambda x: x[1].time_range.start_ms)

                _, request1 = msgs[0]
                _, request2 = msgs[1]

                assert request1.session_id == "session-A"
                assert request1.sample_rate == 22050

                assert request2.session_id == "session-B"
                assert request2.sample_rate == 8000

            assert_that(results, assert_results)

    @patch(
        "backend.pipeline.segmentation.audio.processor.SegmentationAudioProcessor"
    )
    def test_ordered_stitch_audio_handles_out_of_order_chunks(
        self, mock_audio_processor: MagicMock
    ) -> None:
        """Verifies that OrderedContinuousStitchAudioFn buffers out-of-order chunks and emits them in order."""
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
                    OrderedContinuousStitchAudioFn(
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

                lengths = [
                    len(request.buffer) // 2 for feed_id, request in msgs
                ]
                if len(msgs) == 1:
                    assert 48000 in lengths
                else:
                    assert 32000 in lengths or 16000 in lengths

            assert_that(results, assert_results)


class OrderedContinuousStitchSpeechSegmentsTest(unittest.TestCase):
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
        fn = OrderedContinuousStitchAudioFn(
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

        mock_state_context = MockValueState(
            ActiveStitchingState(
                session_id="session-1",
                feed_metadata=FeedMetadata(feed_name="test-feed"),
            )
        )
        mock_state_buffer = MockBagState()
        mock_last_start_ms = MockValueState(None)

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
                transmission_buffer_state=mock_state_buffer,  # type: ignore
                transmission_context_state=mock_state_context,  # type: ignore
                last_start_ms_state=mock_last_start_ms,  # type: ignore
                out_of_order_timer=MagicMock(),
                stale_timer_event=MagicMock(),
                stale_timer_proc=MagicMock(),
            )
        )

        saved_context = mock_state_context.read()
        self.assertIsInstance(saved_context, ActiveStitchingState)
        self.assertTrue(len(saved_context.speech_segments) > 0)
        self.assertEqual(
            get_duration_ms(saved_context.speech_segments[0]), 3000
        )

        # 2. Trigger stale flush and verify mapped speech_segments in FlushRequest payload
        outputs = list(
            fn.handle_stale_transmission_event(
                key="test-feed",
                transmission_buffer=mock_state_buffer,  # type: ignore
                transmission_context=mock_state_context,  # type: ignore
                last_start_ms_state=mock_last_start_ms,  # type: ignore
                stale_timer_event=MagicMock(),
                stale_timer_proc=MagicMock(),
            )
        )

        self.assertEqual(len(outputs), 1)
        feed_id, flush_request = outputs[0]
        self.assertEqual(feed_id, "test-feed")
        self.assertTrue(len(flush_request.speech_segments) > 0)
        self.assertEqual(
            get_duration_ms(flush_request.speech_segments[0]), 3000
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
        fn = OrderedContinuousStitchAudioFn(
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

        mock_state_context = MockValueState(
            ActiveStitchingState(
                session_id="session-1",
                feed_metadata=FeedMetadata(feed_name="test-feed"),
            )
        )
        mock_state_buffer = MockBagState()
        mock_last_start_ms = MockValueState(None)

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
                transmission_buffer_state=mock_state_buffer,  # type: ignore
                transmission_context_state=mock_state_context,  # type: ignore
                last_start_ms_state=mock_last_start_ms,  # type: ignore
                out_of_order_timer=MagicMock(),
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
                transmission_buffer=mock_state_buffer,  # type: ignore
                transmission_context=mock_state_context,  # type: ignore
                last_start_ms_state=mock_last_start_ms,  # type: ignore
                stale_timer_event=MagicMock(),
                stale_timer_proc=MagicMock(),
            )
        )

        # Assert that the state context has been completely cleared!
        self.assertIsNone(mock_state_context.read())

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
        fn = OrderedContinuousStitchAudioFn(
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

        mock_state_context = MockValueState(
            ActiveStitchingState(
                session_id="session-long",
                feed_metadata=FeedMetadata(feed_name="test-feed"),
            )
        )
        mock_state_buffer = MockBagState()
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
                transmission_buffer_state=mock_state_buffer,  # type: ignore
                transmission_context_state=mock_state_context,  # type: ignore
                last_start_ms_state=mock_last_start_ms,  # type: ignore
                out_of_order_timer=MagicMock(),
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
        fn = OrderedContinuousStitchAudioFn(
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

        mock_state_context = MockValueState(
            ActiveStitchingState(
                session_id="session-silence",
                feed_metadata=FeedMetadata(feed_name="test-feed"),
            )
        )
        mock_state_buffer = MockBagState()
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
                transmission_buffer_state=mock_state_buffer,  # type: ignore
                transmission_context_state=mock_state_context,  # type: ignore
                last_start_ms_state=mock_last_start_ms,  # type: ignore
                out_of_order_timer=MagicMock(),
                stale_timer_event=MagicMock(),
                stale_timer_proc=MagicMock(),
            )
        )

        # Should immediately yield one flush request for the first speech segment
        self.assertEqual(len(outputs), 1)
        feed_id, flush_req = outputs[0]
        self.assertEqual(feed_id, "test-feed")
        # Since it split at a natural silence gap, missing_post_context should be False
        self.assertFalse(flush_req.missing_post_context)
        self.assertEqual(flush_req.speech_segments[-1].end_ms, 30000)

        # Saved context should hold the second speech segment natively started
        saved_context = mock_state_context.read()
        self.assertIsInstance(saved_context, ActiveStitchingState)
        self.assertFalse(saved_context.missing_prior_context)
        self.assertEqual(saved_context.speech_segments[0].start_ms, 35000)
        self.assertEqual(saved_context.speech_segments[0].end_ms, 65000)


class OrderedSegmentedStitchAudioTest(unittest.TestCase):
    @patch("backend.pipeline.common.tracing_utils.with_tracer_context")
    @patch(
        "backend.pipeline.segmentation.audio.processor.SegmentationAudioProcessor"
    )
    def test_ordered_segmented_stitch_isolates_and_splits(
        self,
        mock_audio_processor: MagicMock,
        mock_with_tracer_context: MagicMock,
    ) -> None:
        """Verifies that OrderedSegmentedStitchAudioFn isolates and splits chunks > 60 seconds."""
        order_config = OrderRestorerConfig(out_of_order_timeout_ms=1000)
        stitch_config = get_test_stitch_config(
            max_transmission_duration_ms=10000,  # 10s limit for splitting
        )
        fn = OrderedSegmentedStitchAudioFn(
            order_config=order_config, stitch_config=stitch_config
        )
        self.assertTrue(fn.stitch_config.isolate_segmented_chunks)

        mock_processor_inst = mock_audio_processor.return_value

        chunk_data = AudioChunkData(
            start_ms=10000,
            audio=np.zeros(15000 * 16, dtype=np.int16),
            speech_segments=[TimeRange(0, 15000)],
            gcs_uri="gs://bucket/segmented_15s.flac",
            duration_ms=15000,
            sample_rate=16000,
        )
        mock_processor_inst.download_audio_and_detect.return_value = chunk_data

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

        mock_state_context = MockValueState(
            ActiveStitchingState(
                session_id="same-session-id",
                feed_metadata=FeedMetadata(feed_name="segmented-feed"),
            )
        )
        mock_state_buffer = MockBagState()
        mock_last_start_ms = MockValueState(None)

        metadata = ChunkMetadata(
            gcs_uri="gs://bucket/segmented_15s.flac",
            session_id="same-session-id",
            duration_ms=15000,
            feed_metadata=FeedMetadata(feed_name="segmented-feed"),
        )

        outputs = list(
            fn.process(
                element=("segmented-feed", metadata),
                timestamp=Timestamp(10),
                transmission_buffer_state=mock_state_buffer,  # type: ignore
                transmission_context_state=mock_state_context,  # type: ignore
                last_start_ms_state=mock_last_start_ms,  # type: ignore
                out_of_order_timer=MagicMock(),
                stale_timer_event=MagicMock(),
                stale_timer_proc=MagicMock(),
            )
        )

        # We expect exactly 2 flushed parts because the 15s chunk was split!
        self.assertEqual(len(outputs), 2)

        # First flushed part: 10 seconds
        feed_id_1, flush_req_1 = outputs[0]
        self.assertEqual(feed_id_1, "segmented-feed")
        self.assertEqual(get_duration_ms(flush_req_1.time_range), 10000)
        self.assertTrue(flush_req_1.missing_post_context)
        self.assertFalse(flush_req_1.missing_prior_context)

        # Second flushed part: 5 seconds
        feed_id_2, flush_req_2 = outputs[1]
        self.assertEqual(feed_id_2, "segmented-feed")
        self.assertEqual(get_duration_ms(flush_req_2.time_range), 5000)
        self.assertFalse(flush_req_2.missing_post_context)
        self.assertTrue(flush_req_2.missing_prior_context)


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
        """Returns (fn, mock_state_context, mock_state_buffer, mock_last_start_ms)."""
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
        fn = OrderedContinuousStitchAudioFn(
            order_config=order_config, stitch_config=stitch_config
        )

        dlq_payload = {"error": "mock error", "attributes": {}}
        results = list(
            fn._yield_tagged_outputs([(DEAD_LETTER_QUEUE_TAG, dlq_payload)])
        )

        self.assertEqual(len(results), 1)
        self.assertIsInstance(results[0], beam.pvalue.TaggedOutput)
        assert isinstance(results[0], beam.pvalue.TaggedOutput)
        self.assertEqual(results[0].tag, "normalization_dlq")
        self.assertEqual(results[0].value, dlq_payload)

    def test_yield_tagged_outputs_passes_through_normal_results(self) -> None:
        """Verifies that _yield_tagged_outputs leaves main FlushRequest tuples unchanged."""
        order_config = OrderRestorerConfig(out_of_order_timeout_ms=1000)
        stitch_config = get_test_stitch_config()
        fn = OrderedContinuousStitchAudioFn(
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
        fn = OrderedContinuousStitchAudioFn(
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
        self.assertEqual(results[1].tag, "normalization_dlq")

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
            self._make_fn_and_states(OrderedContinuousStitchAudioFn)
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
                transmission_buffer_state=mock_state_buffer,
                transmission_context_state=mock_state_context,
                last_start_ms_state=mock_last_start_ms,
                out_of_order_timer=MagicMock(),
                stale_timer_event=MagicMock(),
                stale_timer_proc=MagicMock(),
            )
        )

        outputs = list(
            fn.handle_stale_transmission_event(
                key="test-feed",
                transmission_buffer=mock_state_buffer,
                transmission_context=mock_state_context,
                last_start_ms_state=mock_last_start_ms,
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
            self._make_fn_and_states(OrderedContinuousStitchAudioFn)
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
                transmission_buffer_state=mock_state_buffer,
                transmission_context_state=mock_state_context,
                last_start_ms_state=mock_last_start_ms,
                out_of_order_timer=MagicMock(),
                stale_timer_event=MagicMock(),
                stale_timer_proc=MagicMock(),
            )
        )

        # 2. Trigger stale flush
        outputs = list(
            fn.handle_stale_transmission_event(
                key="test-feed",
                transmission_buffer=mock_state_buffer,
                transmission_context=mock_state_context,
                last_start_ms_state=mock_last_start_ms,
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
        "backend.pipeline.segmentation.audio.processor.SegmentationAudioProcessor"
    )
    def test_immediate_segmented_flush_preserves_echo_sample_rate_in_flush_request(
        self, mock_audio_processor: MagicMock
    ) -> None:
        """Verifies that an isolated segmented chunk (representing Echo feed audio)
        emits an immediate FlushRequest that correctly preserves the 8 kHz sample rate,
        even on the very first chunk of a session when the context state hasn't been updated.
        """
        mock_processor_inst = mock_audio_processor.return_value
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
            self._make_fn_and_states(OrderedSegmentedStitchAudioFn)
        )
        fn.setup()

        metadata = ChunkMetadata(
            gcs_uri="gs://bucket/echo_chunk.flac",
            session_id="test-session",
            duration_ms=3000,
            feed_metadata=FeedMetadata(feed_name="test-feed"),
        )
        # Process chunk (since it is segmented, it will trigger an immediate flush within the same process call)
        outputs = list(
            fn.process(
                element=("test-feed", metadata),
                timestamp=Timestamp(100),
                transmission_buffer_state=mock_state_buffer,
                transmission_context_state=mock_state_context,
                last_start_ms_state=mock_last_start_ms,
                out_of_order_timer=MagicMock(),
                stale_timer_event=MagicMock(),
                stale_timer_proc=MagicMock(),
            )
        )

        # The immediate flush should have produced exactly one result
        self.assertEqual(len(outputs), 1)
        _feed_id, flush_request = outputs[0]
        # sample_rate must be 8000 (correctly resolved from chunk_data on immediate flush)
        self.assertEqual(flush_request.sample_rate, 8000)

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
            self._make_fn_and_states(OrderedContinuousStitchAudioFn)
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
                transmission_buffer_state=mock_state_buffer,
                transmission_context_state=mock_state_context,
                last_start_ms_state=mock_last_start_ms,
                out_of_order_timer=MagicMock(),
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
                transmission_buffer=mock_state_buffer,
                transmission_context=mock_state_context,
                last_start_ms_state=mock_last_start_ms,
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
                transmission_buffer_state=mock_state_buffer,
                transmission_context_state=mock_state_context,
                last_start_ms_state=mock_last_start_ms,
                out_of_order_timer=MagicMock(),
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
            self._make_fn_and_states(OrderedContinuousStitchAudioFn)
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
                    transmission_buffer_state=mock_state_buffer,  # type: ignore
                    transmission_context_state=mock_state_context,  # type: ignore
                    last_start_ms_state=mock_last_start_ms,  # type: ignore
                    out_of_order_timer=MagicMock(),
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
                    transmission_buffer_state=mock_state_buffer,  # type: ignore
                    transmission_context_state=mock_state_context,  # type: ignore
                    last_start_ms_state=mock_last_start_ms,  # type: ignore
                    out_of_order_timer=MagicMock(),
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
                    transmission_buffer_state=mock_state_buffer,  # type: ignore
                    transmission_context_state=mock_state_context,  # type: ignore
                    last_start_ms_state=mock_last_start_ms,  # type: ignore
                    out_of_order_timer=MagicMock(),
                    stale_timer_event=MagicMock(),
                    stale_timer_proc=MagicMock(),
                )
            )

        # Overlap check should NOT run (no warnings logged)
        mock_logger.warning.assert_not_called()
        # last_start_ms state should NOT be written (remains 10050, not updated to 10070)
        self.assertEqual(mock_last_start_ms.read(), 10050)

        # --- Case 3: Non-mainline Flush (clear_state=False) ---
        mock_logger.reset_mock()

        # Create fn and states for segmented stitch
        (
            fn_segmented,
            mock_state_context_seg,
            mock_state_buffer_seg,
            mock_last_start_ms_seg,
        ) = self._make_fn_and_states(OrderedSegmentedStitchAudioFn)
        fn_segmented.stitch_config = stitch_config
        fn_segmented.setup()

        mock_last_start_ms_seg.write(10050)

        chunk_data_segmented = AudioChunkData(
            start_ms=10070,  # Overlaps with 10050
            audio=np.zeros(16000 * 3, dtype=np.int16),
            speech_segments=[TimeRange(0, 3000)],
            gcs_uri="gs://bucket/chunk4.flac",
            duration_ms=3000,
            sample_rate=16000,
        )
        mock_processor_inst.download_audio_and_detect.return_value = (
            chunk_data_segmented
        )
        mock_state_context_seg.write(
            ActiveStitchingState(
                session_id="session-segmented",
                feed_metadata=FeedMetadata(feed_name="test-feed"),
            )
        )
        metadata_segmented = ChunkMetadata(
            gcs_uri="gs://bucket/chunk4.flac",
            session_id="session-segmented",
            duration_ms=3000,
            feed_metadata=FeedMetadata(feed_name="test-feed"),
        )

        with (
            patch(
                "backend.pipeline.segmentation.transforms.stitcher_engine._get_task_logger",
                return_value=mock_logger,
            ),
            patch(
                "backend.pipeline.segmentation.transforms.stateful.time.time",
                return_value=10.2,
            ),
        ):  # Lateness = 0, so NOT backfill
            list(
                fn_segmented.process(
                    element=("test-feed", metadata_segmented),
                    timestamp=Timestamp(10.2),
                    transmission_buffer_state=mock_state_buffer_seg,  # type: ignore
                    transmission_context_state=mock_state_context_seg,  # type: ignore
                    last_start_ms_state=mock_last_start_ms_seg,  # type: ignore
                    out_of_order_timer=MagicMock(),
                    stale_timer_event=MagicMock(),
                    stale_timer_proc=MagicMock(),
                )
            )

        # Overlap check should NOT run (no warnings logged)
        mock_logger.warning.assert_not_called()
        # last_start_ms state should NOT be written (remains 10050)
        self.assertEqual(mock_last_start_ms_seg.read(), 10050)
