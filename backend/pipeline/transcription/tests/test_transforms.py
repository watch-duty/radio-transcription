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

from backend.pipeline.schema_types.raw_audio_chunk_pb2 import AudioChunk
from backend.pipeline.transcription.constants import DEAD_LETTER_QUEUE_TAG
from backend.pipeline.transcription.datatypes import (
    AudioChunkData,
    ChunkMetadata,
    FeedMetadata,
    FlushRequest,
    OrderRestorerConfig,
    StitchAudioConfig,
    TimeRange,
    TranscribeAudioConfig,
    TranscriptionResult,
)
from backend.pipeline.transcription.enums import TranscriberType, VadType
from backend.pipeline.transcription.stateful_transforms import (
    OrderedBypassFn,
    OrderedStitchAudioFn,
    TranscribeAudioFn,
)
from backend.pipeline.transcription.transcribers import Transcriber
from backend.pipeline.transcription.transforms import (
    ParseAndKeyFn,
    SerializeAndEnrichFn,
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
        "vad_type": VadType.TEN_VAD,
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
        "vad_type": VadType.TEN_VAD,
        "vad_config": "{}",
    }
    defaults.update(kwargs)
    return TranscribeAudioConfig(**defaults)  # type: ignore


class ParseAndKeyTimestampTest(unittest.TestCase):
    def test_parse_and_key_success(self) -> None:
        """Verifies that well-formed Pub/Sub messages containing a serialized AudioChunk and feed_id are correctly unmarshalled and keyed by feed."""
        chunk = AudioChunk(gcs_uri="gs://test-bucket/path/to/test.flac")
        chunk.start_timestamp.FromMicroseconds(123456789000)
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
                equal_to([("test-feed", chunk.SerializeToString())]),
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

            def assert_dlq(elements: list[dict[str, Any]]) -> None:

                assert len(elements) == 1
                assert (
                    "Missing required payload attribute" in elements[0]["error"]
                )

            assert_that(parsed.main, equal_to([]), label="CheckEmptyMain")
            assert_that(
                parsed[DEAD_LETTER_QUEUE_TAG], assert_dlq, label="CheckDLQ"
            )


class TranscribeAudioTest(unittest.TestCase):
    @patch("backend.pipeline.transcription.stateful_transforms.get_transcriber")
    @patch("backend.pipeline.transcription.stateful_transforms.AudioProcessor")
    def test_dlq_routing(
        self, mock_audio_processor: MagicMock, mock_get_transcriber: MagicMock
    ) -> None:
        """Verifies that explicit Python exceptions raised randomly within transformations dynamically populate a standardized and resilient Dataflow Dead Letter Queue error."""
        mock_processor_inst = mock_audio_processor.return_value
        mock_processor_inst.check_vad.return_value = True
        mock_processor_inst.preprocess_audio.side_effect = lambda x: x
        mock_processor_inst.export_flac.return_value = b"flac_bytes"
        mock_processor_inst.process_buffer.return_value = (
            True,
            b"flac_bytes",
            np.zeros(((500) * 16), dtype=np.int16),
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

            def assert_dlq(elements: list[dict[str, Any]]) -> None:

                assert len(elements) == 1
                assert "Transcription API outage!" in elements[0]["error"]

            def assert_empty(elements):
                assert len(elements) == 0

            assert_that(results.main, assert_empty, label="CheckEmptyMain")

            assert_that(
                results[DEAD_LETTER_QUEUE_TAG], assert_dlq, label="CheckDLQ"
            )


class SerializeAndEnrichTest(unittest.TestCase):
    def test_serialize_and_enrich(self) -> None:
        """Verifies that SerializeAndEnrichFn correctly stores feed_name and enriches the transcript."""
        options = PipelineOptions(
            flags=["--input_subscription=a", "--output_topic=b", "--project=c"]
        )

        with BeamTestPipeline(options=options) as p:
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
            )

            elements = [
                (
                    "test-feed",
                    FeedMetadata(
                        feed_name="Test Feed Name",
                        external_id="test-external-id",
                    ),
                ),
                ("test-feed", res1),
                ("test-feed", res2),
            ]

            results = (
                p | beam.Create(elements) | beam.ParDo(SerializeAndEnrichFn())
            )

            def assert_results(msgs):
                from backend.pipeline.schema_types.transcribed_audio_pb2 import (  # noqa: PLC0415
                    TranscribedAudio,
                )

                assert len(msgs) == 2

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


class OrderedBypassTest(unittest.TestCase):
    @patch("backend.pipeline.transcription.stateful_transforms.AudioProcessor")
    def test_ordered_bypass_yields_correct_offsets(
        self, mock_audio_processor: MagicMock
    ) -> None:
        """Verifies that OrderedBypassFn sets end_audio_offset_ms correctly."""
        mock_processor_inst = mock_audio_processor.return_value
        chunk_data = MagicMock()
        chunk_data.duration_ms = 1000
        chunk_data.audio = np.zeros(16000, dtype=np.int16)
        mock_processor_inst.download_audio_and_detect.return_value = chunk_data

        order_config = OrderRestorerConfig(out_of_order_timeout_ms=1000)
        stitch_config = get_test_stitch_config()

        options = PipelineOptions(
            flags=["--input_subscription=a", "--output_topic=b", "--project=c"]
        )
        with BeamTestPipeline(options=options) as p:
            metadata = ChunkMetadata(
                gcs_uri="gs://test-bucket/path/to/test.flac",
                session_id="mock-session-id",
                duration_ms=1000,
                feed_metadata=FeedMetadata(
                    feed_name="mock-feed", external_id="mock-external-id"
                ),
            )

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
                .advance_watermark_to_infinity()
            )

            results = (
                p
                | test_stream
                | beam.ParDo(
                    OrderedBypassFn(
                        order_config=order_config, stitch_config=stitch_config
                    )
                )
            )

            def assert_results(msgs):
                assert len(msgs) == 1
                feed_id, request = msgs[0]
                assert feed_id == "test-feed"
                assert request.end_audio_offset_ms == 1000

            assert_that(results, assert_results)

    @patch("backend.pipeline.transcription.stateful_transforms.AudioProcessor")
    def test_ordered_bypass_flushes_on_timeout(
        self, mock_audio_processor: MagicMock
    ) -> None:
        """Verifies that OrderedBypassFn flushes buffered chunks when gap timeout fires."""
        mock_processor_inst = mock_audio_processor.return_value
        chunk_data = MagicMock()
        chunk_data.duration_ms = 1000
        chunk_data.audio = np.zeros(16000, dtype=np.int16)
        mock_processor_inst.download_audio_and_detect.return_value = chunk_data
    
        order_config = OrderRestorerConfig(out_of_order_timeout_ms=1000)
        stitch_config = get_test_stitch_config()
    
        options = PipelineOptions(
            flags=["--input_subscription=a", "--output_topic=b", "--project=c"]
        )
        
        metadata = ChunkMetadata(
            gcs_uri="gs://test-bucket/path/to/test.flac",
            session_id="mock-session-id",
            duration_ms=1000,
            feed_metadata=FeedMetadata(feed_name="mock-feed", external_id="mock-external-id"),
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
                .advance_watermark_to(102)
                .add_elements([TimestampedValue(("test-feed", metadata), 102)])
                .advance_watermark_to(105)
                .advance_watermark_to_infinity()
            )

            results = p | test_stream | beam.ParDo(
                OrderedBypassFn(
                    order_config=order_config, stitch_config=stitch_config
                )
            )

            def assert_results(msgs):
                assert len(msgs) == 2
                assert msgs[0][0] == "test-feed"
                assert msgs[1][0] == "test-feed"

            assert_that(results, assert_results)


class OrderedStitchAudioTest(unittest.TestCase):
    @patch("backend.pipeline.transcription.stateful_transforms.AudioProcessor")
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
        mock_processor_inst.download_audio_and_detect.return_value = chunk_data
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
            feed_metadata=FeedMetadata(feed_name="mock-feed", external_id="mock-external-id"),
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

            results = p | test_stream | beam.ParDo(
                OrderedStitchAudioFn(
                    order_config=order_config, stitch_config=stitch_config
                )
            )

            def assert_results(msgs):
                assert len(msgs) == 1
                feed_id, request = msgs[0]
                assert feed_id == "test-feed"
                assert request.transmission_id is not None
                assert isinstance(request.buffer, np.ndarray)

            assert_that(results, assert_results)

    @patch("backend.pipeline.transcription.stateful_transforms.AudioProcessor")
    def test_ordered_stitch_audio_handles_out_of_order_chunks(
        self, mock_audio_processor: MagicMock
    ) -> None:
        """Verifies that OrderedStitchAudioFn buffers out-of-order chunks and emits them in order."""
        mock_processor_inst = mock_audio_processor.return_value
        
        def download_side_effect(gcs_uri, timestamp_ms):
            if "chunk1" in gcs_uri:
                return AudioChunkData(
                    start_ms=100000,
                    audio=np.ones(16000, dtype=np.int16),
                    sample_rate=16000,
                    speech_segments=[TimeRange(0, 1000)],
                    gcs_uri=gcs_uri,
                    duration_ms=1000,
                )
            elif "chunk2" in gcs_uri:
                return AudioChunkData(
                    start_ms=101000,
                    audio=np.ones(16000, dtype=np.int16) * 2,
                    sample_rate=16000,
                    speech_segments=[TimeRange(0, 1000)],
                    gcs_uri=gcs_uri,
                    duration_ms=1000,
                )
            else:
                return AudioChunkData(
                    start_ms=102000,
                    audio=np.ones(16000, dtype=np.int16) * 3,
                    sample_rate=16000,
                    speech_segments=[TimeRange(0, 1000)],
                    gcs_uri=gcs_uri,
                    duration_ms=1000,
                )
                
        mock_processor_inst.download_audio_and_detect.side_effect = download_side_effect
        mock_processor_inst.preprocess_audio.side_effect = lambda x: x
        mock_processor_inst.check_vad.return_value = True

        order_config = OrderRestorerConfig(out_of_order_timeout_ms=5000)
        stitch_config = get_test_stitch_config(stale_timeout_ms=5000, significant_gap_ms=5000)

        options = PipelineOptions(
            flags=["--input_subscription=a", "--output_topic=b", "--project=c"]
        )
        
        metadata_chunk1 = ChunkMetadata(
            gcs_uri="gs://test-bucket/path/to/chunk1.flac",
            session_id="mock-session-id",
            duration_ms=1000,
            feed_metadata=FeedMetadata(feed_name="mock-feed", external_id="mock-external-id"),
        )
        
        metadata_chunk2 = ChunkMetadata(
            gcs_uri="gs://test-bucket/path/to/chunk2.flac",
            session_id="mock-session-id",
            duration_ms=1000,
            feed_metadata=FeedMetadata(feed_name="mock-feed", external_id="mock-external-id"),
        )
        
        metadata_chunk3 = ChunkMetadata(
            gcs_uri="gs://test-bucket/path/to/chunk3.flac",
            session_id="mock-session-id",
            duration_ms=1000,
            feed_metadata=FeedMetadata(feed_name="mock-feed", external_id="mock-external-id"),
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
                .add_elements([TimestampedValue(("test-feed-ooo", metadata_chunk1), 100)])
                .add_elements([TimestampedValue(("test-feed-ooo", metadata_chunk3), 102)])
                .add_elements([TimestampedValue(("test-feed-ooo", metadata_chunk2), 101)])
                .advance_watermark_to(115)
                .advance_watermark_to_infinity()
            )

            results = p | test_stream | beam.ParDo(
                OrderedStitchAudioFn(
                    order_config=order_config, stitch_config=stitch_config
                )
            )

            def assert_results(msgs):
                assert len(msgs) == 2
                for feed_id, request in msgs:
                    assert feed_id == "test-feed-ooo"
                
                lengths = [len(request.buffer) for feed_id, request in msgs]
                assert 32000 in lengths
                assert 16000 in lengths

            assert_that(results, assert_results)
