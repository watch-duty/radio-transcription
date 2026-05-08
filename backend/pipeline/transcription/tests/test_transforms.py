"""Tests for the StitchAudioFn, TranscribeAudioFn, and related transformations."""

import unittest
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
from backend.pipeline.schema_types.raw_audio_chunk_pb2 import AudioChunk
from backend.pipeline.transcription.common.constants import (
    DEAD_LETTER_QUEUE_TAG,
)
from backend.pipeline.transcription.common.datatypes import (
    AudioChunkData,
    BufferedChunk,
    ChunkMetadata,
    FeedMetadata,
    FlushRequest,
    OrderRestorerConfig,
    StitchAudioConfig,
    TimeRange,
    TranscribeAudioConfig,
    TranscriptionResult,
    TransmissionContext,
)
from backend.pipeline.transcription.common.enums import TranscriberType
from backend.pipeline.transcription.services.transcribers import Transcriber
from backend.pipeline.transcription.transforms.stateful import (
    OrderedBypassFn,
    OrderedStitchAudioFn,
    TranscribeAudioFn,
)
from backend.pipeline.transcription.transforms.stateless import (
    ParseAndKeyFn,
    SerializeFn,
)


class MockTranscriberFactory:
    def __init__(
        self, transcript: str, *, raise_exception: bool = False
    ) -> None:

        self.transcript = transcript
        self.raise_exception = raise_exception

    def __call__(
        self,
        transcriber_type: TranscriberType,
        project_id: str,
        config_json: str,
        *args: Any,
        **kwargs: Any,
    ) -> Transcriber:

        mock = MagicMock()
        if self.raise_exception:
            mock.transcribe.side_effect = Exception("Transcription API outage!")
        else:
            mock.transcribe.return_value = self.transcript
        return mock


def get_mock_factory(
    transcript: str = "Simulated transcript.", *, raise_exception: bool = False
) -> MockTranscriberFactory:

    return MockTranscriberFactory(transcript, raise_exception=raise_exception)


def get_test_stitch_config(**kwargs: Any) -> StitchAudioConfig:

    defaults = {
        "project_id": "fake-proj",
        "vad_config": "{}",
        "significant_gap_ms": 500,
        "stale_timeout_ms": 60000,
        "max_transmission_duration_ms": 600000,
        "vad_pre_roll_ms": 0,
        "vad_post_roll_ms": 0,
    }
    defaults.update(kwargs)
    return StitchAudioConfig(**defaults)  # type: ignore


def get_test_transcribe_config(**kwargs: Any) -> TranscribeAudioConfig:

    defaults = {
        "project_id": "fake-proj",
        "transcriber_type": TranscriberType.GOOGLE_CHIRP_V3,
        "transcriber_config": "{}",
        "vad_config": "{}",
    }
    defaults.update(kwargs)
    return TranscribeAudioConfig(**defaults)  # type: ignore


class ParseAndKeyTimestampTest(unittest.TestCase):
    def test_parse_and_key_success(self) -> None:
        """Verifies that well-formed Pub/Sub messages containing a serialized AudioChunk and feed_id are correctly unmarshalled and keyed by feed."""
        chunk = AudioChunk(
            gcs_uri="gs://test-bucket/path/to/test.flac",
            session_id="mock-session-id",
            feed_name="mock-feed-name",
            duration_ms=1000,
            feed_id="test-feed",
            external_id="mock-external-id",
        )
        mock_msg = PubsubMessage(
            chunk.SerializeToString(),
            {"feed_id": "test-feed"},
        )
        options = PipelineOptions(
            flags=["--input_subscription=a", "--output_topic=b", "--project=c"]
        )
        with BeamTestPipeline(options=options) as p:
            messages = p | beam.Create([mock_msg])
            parsed = messages | beam.ParDo(ParseAndKeyFn()).with_outputs(
                DEAD_LETTER_QUEUE_TAG, main="main"
            )

            assert_that(
                parsed.main,
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
                                    external_id="mock-external-id",
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
        chunk = AudioChunk(gcs_uri="gs://test-bucket/path/to/test.flac")
        mock_msg = PubsubMessage(
            chunk.SerializeToString(),
            {},  # Missing feed_id
        )
        options = PipelineOptions(
            flags=["--input_subscription=a", "--output_topic=b", "--project=c"]
        )
        with BeamTestPipeline(options=options) as p:
            messages = p | beam.Create([mock_msg])
            parsed = messages | beam.ParDo(ParseAndKeyFn()).with_outputs(
                DEAD_LETTER_QUEUE_TAG, main="main"
            )

            def assert_dlq(
                elements: list[dict[str, str | bool | dict[str, str]]],
            ) -> None:

                assert len(elements) == 1
                assert isinstance(elements[0]["error"], str)
                assert (
                    "Failed to parse or validate payload"
                    in elements[0]["error"]
                )

            assert_that(parsed.main, equal_to([]), label="CheckEmptyMain")
            assert_that(
                parsed[DEAD_LETTER_QUEUE_TAG], assert_dlq, label="CheckDLQ"
            )

    def test_parse_and_key_span_lifecycle(self) -> None:
        """Verifies that ParseAndKeyFn doesn't leak trace context scope on execution."""
        chunk = AudioChunk(
            gcs_uri="gs://test-bucket/path/to/test.flac",
            session_id="mock-session-id",
            feed_name="mock-feed-name",
            duration_ms=1000,
            feed_id="test-feed",
            external_id="mock-external-id",
        )
        mock_msg = PubsubMessage(
            chunk.SerializeToString(),
            {"feed_id": "test-feed"},
        )

        fn = ParseAndKeyFn()
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


class TranscribeAudioTest(unittest.TestCase):
    @patch("backend.pipeline.transcription.transforms.stateful.get_transcriber")
    @patch("backend.pipeline.transcription.transforms.stateful.AudioProcessor")
    def test_dlq_routing(
        self, mock_audio_processor: MagicMock, mock_get_transcriber: MagicMock
    ) -> None:
        """Verifies that explicit Python exceptions raised randomly within transformations dynamically populate a standardized and resilient Dataflow Dead Letter Queue error."""
        mock_processor_inst = mock_audio_processor.return_value
        mock_processor_inst.check_vad.return_value = True
        mock_processor_inst.preprocess_audio.side_effect = lambda x: x
        mock_processor_inst.export_flac.return_value = b"flac_bytes"
        mock_processor_inst.process_buffer.side_effect = (
            lambda *args, **kwargs: (
                True,
                b"flac_bytes",
                np.zeros(((500) * 16), dtype=np.int16),
            )
        )

        config = get_test_transcribe_config(route_to_dlq=True)

        options = PipelineOptions(
            flags=["--input_subscription=a", "--output_topic=b", "--project=c"]
        )
        with BeamTestPipeline(options=options) as p:
            elements = p | beam.Create(
                [
                    (
                        "feed-123",
                        FlushRequest(
                            feed_id="feed-123",
                            session_id="fake-session",
                            buffer=np.zeros(((500) * 16), dtype=np.int16),
                            contributing_audio_uris=["gs://f/11111111.flac"],
                            time_range=TimeRange(
                                start_ms=101000, end_ms=101500
                            ),
                            transmission_id="test-uuid",
                            missing_prior_context=False,
                            missing_post_context=False,
                            start_audio_offset_ms=0,
                            end_audio_offset_ms=500,
                            feed_metadata=FeedMetadata(
                                feed_name="fake-feed",
                                external_id="fake-external",
                            ),
                        ),
                    )
                ]
            )

            results = elements | beam.ParDo(
                TranscribeAudioFn(
                    config=config,
                    transcriber_factory=get_mock_factory(raise_exception=True),
                )
            ).with_outputs(DEAD_LETTER_QUEUE_TAG, main="main")

            def assert_dlq(
                elements: list[dict[str, str | bool | dict[str, str]]],
            ) -> None:

                assert len(elements) == 1
                assert isinstance(elements[0]["error"], str)
                assert "Transcription API outage!" in elements[0]["error"]

            def assert_empty(elements):
                assert len(elements) == 0

            assert_that(results.main, assert_empty, label="CheckEmptyMain")

            assert_that(
                results[DEAD_LETTER_QUEUE_TAG], assert_dlq, label="CheckDLQ"
            )


class SerializeAndEnrichTest(unittest.TestCase):
    def test_serialize_and_enrich(self) -> None:
        """Verifies that SerializeAndEnrichFn correctly enriches and serializes the transcript."""
        options = PipelineOptions(
            flags=["--input_subscription=a", "--output_topic=b", "--project=c"]
        )

        with BeamTestPipeline(options=options) as p:
            feed_metadata = FeedMetadata(
                feed_name="Test Feed Name",
                external_id="test-external-id",
            )

            res1 = TranscriptionResult(
                feed_id="test-feed",
                session_id="fake-session",
                contributing_audio_uris=["gs://bucket/1.flac"],
                transcript="Hello world",
                time_range=TimeRange(1000, 2000),
                transmission_id="uuid-1",
                missing_prior_context=False,
                missing_post_context=False,
                start_audio_offset_ms=100,
                end_audio_offset_ms=200,
                canonical_audio_uri="gs://bucket/1.flac",
                playback_audio_uri="gs://bucket/1_playback.flac",
                feed_metadata=feed_metadata,
                traceparent="00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
            )

            res2 = TranscriptionResult(
                feed_id="test-feed",
                session_id="fake-session",
                contributing_audio_uris=["gs://bucket/2.flac"],
                transcript="Hello world again",
                time_range=TimeRange(1000, 3000),
                transmission_id="uuid-2",
                missing_prior_context=False,
                missing_post_context=False,
                start_audio_offset_ms=100,
                end_audio_offset_ms=200,
                canonical_audio_uri="gs://bucket/2.flac",
                playback_audio_uri="gs://bucket/2_playback.flac",
                feed_metadata=feed_metadata,
                traceparent="00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
            )

            results = p | beam.Create([res1, res2]) | beam.ParDo(SerializeFn())

            def assert_results(msgs):
                from backend.pipeline.schema_types.transcribed_audio_pb2 import (  # noqa: PLC0415
                    TranscribedAudio,
                )

                assert len(msgs) == 2

                for m in msgs:
                    assert (
                        m.attributes.get("traceparent")
                        == "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
                    )

                protos = []
                for m in msgs:
                    p = TranscribedAudio()
                    p.ParseFromString(m.data)
                    protos.append(p)

                protos.sort(key=lambda p: p.transcript)

                assert protos[0].transcript == "Hello world"
                assert protos[0].feed_name == "Test Feed Name"
                assert protos[0].external_id == "test-external-id"

                assert protos[1].transcript == "Hello world again"
                assert protos[1].feed_name == "Test Feed Name"
                assert protos[1].external_id == "test-external-id"

            assert_that(results, assert_results)

    def test_ordered_bypass_buffers_and_sets_timer(self) -> None:
        """Verifies that OrderedBypassFn buffers chunks and sets the timer."""
        stitch_config = get_test_stitch_config()
        order_config = OrderRestorerConfig(out_of_order_timeout_ms=1000)
        fn = OrderedBypassFn(
            order_config=order_config, stitch_config=stitch_config
        )

        mock_state = MagicMock()
        mock_state.read.return_value = None
        mock_timer = MagicMock()

        metadata = ChunkMetadata(
            gcs_uri="gs://test-bucket/path/to/test.flac",
            session_id="mock-session-id",
            duration_ms=1000,
            feed_metadata=FeedMetadata(
                feed_name="mock-feed", external_id="mock-external-id"
            ),
        )

        list(
            fn.process(
                ("test-feed", metadata),
                timestamp=Timestamp(100),
                transmission_context_state=mock_state,
                out_of_order_timer=mock_timer,
            )
        )

        mock_timer.set.assert_called_once()
        mock_state.write.assert_called_once()

    @patch(
        "backend.pipeline.transcription.transforms.stitcher_engine.AudioProcessor"
    )
    def test_ordered_bypass_callback_flushes(
        self, mock_audio_processor: MagicMock
    ) -> None:
        """Verifies that handle_buffer_timeout flushes and yields."""
        mock_processor_inst = mock_audio_processor.return_value
        chunk_data = MagicMock()
        chunk_data.duration_ms = 1000
        chunk_data.audio = np.zeros(16000, dtype=np.int16)
        mock_processor_inst.download_audio_and_detect.side_effect = (
            lambda *args, **kwargs: chunk_data
        )

        stitch_config = get_test_stitch_config()
        order_config = OrderRestorerConfig(out_of_order_timeout_ms=1000)
        fn = OrderedBypassFn(
            order_config=order_config, stitch_config=stitch_config
        )
        fn.setup()

        mock_state = MagicMock()

        curr_context = TransmissionContext(
            out_of_order_buffer=[
                BufferedChunk(timestamp_ms=100000, gcs_uri="gs://test.flac")
            ],
            feed_metadata=FeedMetadata(
                feed_name="mock-feed", external_id="mock-id"
            ),
        )
        mock_state.read.return_value = curr_context

        results = list(
            fn.handle_buffer_timeout(
                feed_id="test-feed", transmission_context_state=mock_state
            )
        )

        assert len(results) == 1
        mock_state.write.assert_called_once()


class OrderedBypassTest(unittest.TestCase):
    @patch(
        "backend.pipeline.transcription.transforms.stateful.with_tracer_context"
    )
    @patch(
        "backend.pipeline.transcription.transforms.stitcher_engine.AudioProcessor"
    )
    def test_ordered_bypass_process_span(
        self,
        mock_audio_processor: MagicMock,
        mock_with_tracer_context: MagicMock,
    ) -> None:
        """Verifies that OrderedBypassFn.process calls with_tracer_context."""
        stitch_config = get_test_stitch_config()
        order_config = OrderRestorerConfig(out_of_order_timeout_ms=1000)
        fn = OrderedBypassFn(
            order_config=order_config, stitch_config=stitch_config
        )
        fn.setup()

        mock_state = MagicMock()
        mock_state.read.return_value = None
        mock_timer = MagicMock()

        metadata = ChunkMetadata(
            gcs_uri="gs://test-bucket/path/to/test.flac",
            session_id="mock-session-id",
            duration_ms=1000,
            feed_metadata=FeedMetadata(
                feed_name="mock-feed", external_id="mock-external-id"
            ),
            traceparent="mock-traceparent",
        )

        list(
            fn.process(
                ("test-feed", metadata),
                timestamp=Timestamp(100),
                transmission_context_state=mock_state,
                out_of_order_timer=mock_timer,
            )
        )

        mock_with_tracer_context.assert_called_once_with(
            "mock-traceparent",
            "stitching_bypass_process",
            "backend.pipeline.transcription.transforms.stateful",
        )

    @patch(
        "backend.pipeline.transcription.transforms.stateful.with_tracer_context"
    )
    @patch(
        "backend.pipeline.transcription.transforms.stitcher_engine.AudioProcessor"
    )
    def test_ordered_bypass_callback_flushes_span(
        self,
        mock_audio_processor: MagicMock,
        mock_with_tracer_context: MagicMock,
    ) -> None:
        """Verifies that handle_buffer_timeout calls with_tracer_context."""
        mock_processor_inst = mock_audio_processor.return_value
        chunk_data = MagicMock()
        chunk_data.duration_ms = 1000
        chunk_data.audio = np.zeros(16000, dtype=np.int16)
        mock_processor_inst.download_audio_and_detect.side_effect = (
            lambda *args, **kwargs: chunk_data
        )

        stitch_config = get_test_stitch_config()
        order_config = OrderRestorerConfig(out_of_order_timeout_ms=1000)
        fn = OrderedBypassFn(
            order_config=order_config, stitch_config=stitch_config
        )
        fn.setup()

        mock_state = MagicMock()

        curr_context = TransmissionContext(
            out_of_order_buffer=[
                BufferedChunk(timestamp_ms=100000, gcs_uri="gs://test.flac")
            ],
            feed_metadata=FeedMetadata(
                feed_name="mock-feed", external_id="mock-id"
            ),
            traceparent="mock-traceparent-context",
        )
        mock_state.read.return_value = curr_context

        list(
            fn.handle_buffer_timeout(
                feed_id="test-feed", transmission_context_state=mock_state
            )
        )

        mock_with_tracer_context.assert_any_call(
            "mock-traceparent-context",
            "handle_buffer",
            "backend.pipeline.transcription.transforms.stateful",
        )
        mock_with_tracer_context.assert_any_call(
            "mock-traceparent-context",
            "bypass_single_chunk",
            "backend.pipeline.transcription.transforms.stateful",
        )


class OrderedStitchAudioTest(unittest.TestCase):
    @patch(
        "backend.pipeline.transcription.transforms.stateful.with_tracer_context"
    )
    @patch(
        "backend.pipeline.transcription.transforms.stitcher_engine.AudioProcessor"
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
        mock_state.read.return_value = TransmissionContext(
            session_id="mock-session-id"
        )
        mock_timer = MagicMock()

        metadata = ChunkMetadata(
            gcs_uri="gs://test-bucket/path/to/test.flac",
            session_id="mock-session-id",
            duration_ms=1000,
            feed_metadata=FeedMetadata(
                feed_name="mock-feed", external_id="mock-external-id"
            ),
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
            "backend.pipeline.transcription.transforms.stateful",
        )
        mock_with_tracer_context.assert_any_call(
            "mock-traceparent",
            "stitching_single_chunk",
            "backend.pipeline.transcription.transforms.stateful",
        )

    @patch(
        "backend.pipeline.transcription.transforms.stateful.with_tracer_context"
    )
    @patch(
        "backend.pipeline.transcription.transforms.stitcher_engine.AudioProcessor"
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
        curr_context = TransmissionContext(
            session_id="mock-session",
            traceparent="mock-traceparent-context",
            out_of_order_buffer=[
                BufferedChunk(timestamp_ms=100000, gcs_uri="gs://test.flac")
            ],
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
            "backend.pipeline.transcription.transforms.stateful",
        )
        mock_with_tracer_context.assert_any_call(
            "",
            "stitching_single_chunk",
            "backend.pipeline.transcription.transforms.stateful",
        )

    @patch(
        "backend.pipeline.transcription.transforms.stitcher_engine.AudioProcessor"
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
        curr_context = TransmissionContext(
            session_id="mock-session",
            expected_next_chunk_start_ms=2000,
            stale_start_time_ms=0,
            buffer_start_time_ms=0,
            last_end_time_ms=1000,
            contributing_audio_uris=["gs://main/chunk1.flac"],
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
                feed_metadata=FeedMetadata(
                    feed_name="mock-feed", external_id="mock-external-id"
                ),
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
        "backend.pipeline.transcription.transforms.stitcher_engine.AudioProcessor"
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
        mock_processor_inst.check_vad.return_value = True

        order_config = OrderRestorerConfig(out_of_order_timeout_ms=1000)
        stitch_config = get_test_stitch_config(stale_timeout_ms=5000)

        options = PipelineOptions(
            flags=["--input_subscription=a", "--output_topic=b", "--project=c"]
        )

        metadata = ChunkMetadata(
            gcs_uri="gs://test-bucket/path/to/test.flac",
            session_id="mock-session-id",
            duration_ms=1000,
            feed_metadata=FeedMetadata(
                feed_name="mock-feed", external_id="mock-external-id"
            ),
        )

        with BeamTestPipeline(options=options) as p:
            test_stream = (
                TestStream(
                    coder=beam.coders.TupleCoder(
                        (
                            beam.coders.StrUtf8Coder(),
                            beam.coders.PickleCoder(),
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
                assert request.transmission_id is not None
                assert isinstance(request.buffer, np.ndarray)

            assert_that(results, assert_results)

    @patch(
        "backend.pipeline.transcription.transforms.stitcher_engine.AudioProcessor"
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
        mock_processor_inst.check_vad.return_value = True

        order_config = OrderRestorerConfig(out_of_order_timeout_ms=5000)
        stitch_config = get_test_stitch_config(
            stale_timeout_ms=5000, significant_gap_ms=5000
        )

        options = PipelineOptions(
            flags=["--input_subscription=a", "--output_topic=b", "--project=c"]
        )

        metadata_chunk1 = ChunkMetadata(
            gcs_uri="gs://test-bucket/path/to/chunk1.flac",
            session_id="mock-session-id",
            duration_ms=1000,
            feed_metadata=FeedMetadata(
                feed_name="mock-feed", external_id="mock-external-id"
            ),
        )

        metadata_chunk2 = ChunkMetadata(
            gcs_uri="gs://test-bucket/path/to/chunk2.flac",
            session_id="mock-session-id",
            duration_ms=1000,
            feed_metadata=FeedMetadata(
                feed_name="mock-feed", external_id="mock-external-id"
            ),
        )

        metadata_chunk3 = ChunkMetadata(
            gcs_uri="gs://test-bucket/path/to/chunk3.flac",
            session_id="mock-session-id",
            duration_ms=1000,
            feed_metadata=FeedMetadata(
                feed_name="mock-feed", external_id="mock-external-id"
            ),
        )

        with BeamTestPipeline(options=options) as p:
            test_stream = (
                TestStream(
                    coder=beam.coders.TupleCoder(
                        (
                            beam.coders.StrUtf8Coder(),
                            beam.coders.PickleCoder(),
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

                lengths = [len(request.buffer) for feed_id, request in msgs]
                if len(msgs) == 1:
                    assert 48000 in lengths
                else:
                    assert 32000 in lengths
                    assert 16000 in lengths

            assert_that(results, assert_results)
