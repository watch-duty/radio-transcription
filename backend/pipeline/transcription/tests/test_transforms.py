"""
Tests for the StitchAndTranscribeFn and related transformations.
"""

import threading
import time
import unittest
from typing import Any
from unittest.mock import MagicMock, patch

import apache_beam as beam
from apache_beam.io.gcp.pubsub import PubsubMessage
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.testing.test_pipeline import TestPipeline as BeamTestPipeline
from apache_beam.testing.test_stream import TestStream as BeamTestStream
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.transforms.window import TimestampedValue
from pydub import AudioSegment

from backend.pipeline.schema_types.raw_audio_chunk_pb2 import AudioChunk
from backend.pipeline.transcription.constants import DEAD_LETTER_QUEUE_TAG
from backend.pipeline.transcription.datatypes import (
    AudioFileData,
    StitchAndTranscribeConfig,
    TimeRange,
    TranscriptionResult,
)
from backend.pipeline.transcription.enums import TranscriberType, VadType
from backend.pipeline.transcription.stitcher import StitchAndTranscribeFn
from backend.pipeline.transcription.transcribers import Transcriber
from backend.pipeline.transcription.transforms import (
    AddEventTimestamp,
    ParseAndKeyFn,
)


class MockTranscriberFactory:
    """Mock factory returning a MagicMock Transcriber."""

    def __init__(self, transcript: str, *, raise_exception: bool = False) -> None:
        self.transcript = transcript
        self.raise_exception = raise_exception

    def __call__(
        self, transcriber_type: TranscriberType, project_id: str, config_json: str
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
    """Return a MockTranscriberFactory with predefined transcript behavior."""
    return MockTranscriberFactory(transcript, raise_exception=raise_exception)


def get_test_config(**kwargs: Any) -> StitchAndTranscribeConfig:
    """Helper to return a StitchAndTranscribeConfig with defaults."""
    defaults = {
        "project_id": "fake-proj",
        "transcriber_type": TranscriberType.GOOGLE_CHIRP_V3,
        "transcriber_config": "{}",
        "vad_type": VadType.TEN_VAD,
        "vad_config": "{}",
        "metrics_exporter_type": "",
        "metrics_config": "{}",
        "significant_gap_ms": 500,
        "stale_timeout_ms": 60000,
        "max_transmission_duration_ms": 600000,
    }
    defaults.update(kwargs)
    return StitchAndTranscribeConfig(**defaults)  # type: ignore


class ParseAndKeyTimestampTest(unittest.TestCase):
    """Tests for ParseAndKeyFn."""

    def test_parse_and_key_success(self) -> None:
        """Test successful parsing and keying of AudioChunk."""
        chunk = AudioChunk(gcs_uri="gs://test-bucket/path/to/test.flac")
        chunk.start_timestamp.FromMicroseconds(123456789000)
        mock_msg = PubsubMessage(
            chunk.SerializeToString(),
            {"feed_id": "test-feed"},
        )
        with BeamTestPipeline() as p:
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
        """Test that missing feed_id routes to DLQ."""
        chunk = AudioChunk(gcs_uri="gs://test-bucket/path/to/test.flac")
        mock_msg = PubsubMessage(
            chunk.SerializeToString(),
            {},  # Missing feed_id
        )
        with BeamTestPipeline() as p:
            messages = p | beam.Create([mock_msg])
            parsed = messages | beam.ParDo(ParseAndKeyFn()).with_outputs(
                DEAD_LETTER_QUEUE_TAG, main="main"
            )

            def assert_dlq(elements: list[dict[str, Any]]) -> None:
                assert len(elements) == 1
                assert "Missing required payload attribute" in elements[0]["error"]

            assert_that(parsed.main, equal_to([]), label="CheckEmptyMain")
            assert_that(parsed[DEAD_LETTER_QUEUE_TAG], assert_dlq, label="CheckDLQ")


class AddEventTimestampTest(unittest.TestCase):
    """Tests for AddEventTimestamp."""

    def test_valid_timestamp_extraction(self) -> None:
        """Test successful timestamp extraction."""
        chunk = AudioChunk(
            gcs_uri="gs://bucket/hash/feed_id/YYYY-MM-DD/1678886400-bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb.flac"
        )
        chunk.start_timestamp.FromMicroseconds(1678886400000000)
        element = ("test-feed", chunk.SerializeToString())
        fn = AddEventTimestamp()
        result = list(fn.process(element))

        self.assertEqual(len(result), 1)
        self.assertIsInstance(result[0], TimestampedValue)
        self.assertEqual(
            result[0].value,  # type: ignore
            (
                "test-feed",
                "gs://bucket/hash/feed_id/YYYY-MM-DD/1678886400-bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb.flac",
            ),
        )
        self.assertEqual(result[0].timestamp, 1678886400)  # type: ignore

    def test_invalid_timestamp_raises_value_error(self) -> None:
        """Test invalid timestamp routes to DLQ."""
        chunk = AudioChunk(
            gcs_uri="gs://bucket/hash/feed_id/YYYY-MM-DD/invalid-bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb.flac"
        )
        element = ("test-feed", chunk.SerializeToString())
        fn = AddEventTimestamp()

        result = list(fn.process(element))
        self.assertEqual(len(result), 1)
        self.assertIsInstance(result[0], beam.pvalue.TaggedOutput)
        self.assertEqual(result[0].tag, DEAD_LETTER_QUEUE_TAG)  # type: ignore


class StitchAndTranscribeTest(unittest.TestCase):
    """Tests for StitchAndTranscribeFn."""

    @patch("backend.pipeline.transcription.stitcher.AudioProcessor")
    def test_stitching_and_silence_flush_logic(
        self, mock_audio_processor: MagicMock
    ) -> None:
        """Test complex stitching logic with silence and gaps."""
        mock_processor_inst = mock_audio_processor.return_value
        mock_processor_inst.check_vad.return_value = True
        mock_processor_inst.preprocess_audio.side_effect = lambda x: x
        mock_processor_inst.export_flac.return_value = b"flac_bytes"

        sed_map = {
            "100-11111111-1111-1111-1111-111111111111.flac": [(12.5, 15.0)],
            "115-22222222-2222-2222-2222-222222222222.flac": [(0.0, 2.5), (5.0, 7.0)],
            "130-33333333-3333-3333-3333-333333333333.flac": [(0.0, 2.5)],
            "150-44444444-4444-4444-4444-444444444444.flac": [],
            "160-55555555-5555-5555-5555-555555555555.flac": [(0.0, 2.0)],
            "190-66666666-6666-6666-6666-666666666666.flac": [(0.0, 2.0)],
        }

        def mock_download(path: str) -> AudioFileData:
            filename = path.rsplit("/", maxsplit=1)[-1]
            chunk_start = float(filename.split("-")[0]) if "-" in filename else 0.0
            return AudioFileData(
                start_ms=int(chunk_start * 1000),
                audio=AudioSegment.silent(duration=20000),
                speech_segments=[
                    TimeRange(int(s * 1000), int(e * 1000))
                    for s, e in sed_map.get(filename, [])
                ],
            )

        mock_processor_inst.download_audio_and_sed.side_effect = mock_download

        options = PipelineOptions()
        options.view_as(StandardOptions).streaming = True

        config = get_test_config(significant_gap_ms=3000)

        with BeamTestPipeline(options=options) as p:
            test_stream = (
                BeamTestStream(
                    coder=beam.coders.TupleCoder(
                        (beam.coders.StrUtf8Coder(), beam.coders.StrUtf8Coder())
                    )
                )
                .advance_watermark_to(100)
                .add_elements(
                    [
                        TimestampedValue(
                            (
                                "feed-123",
                                "gs://fake-bucket/ab12/feed-123/2026-03-06/100-11111111-1111-1111-1111-111111111111.flac",
                            ),
                            100,
                        )
                    ]
                )
                .advance_watermark_to(115)
                .add_elements(
                    [
                        TimestampedValue(
                            (
                                "feed-123",
                                "gs://fake-bucket/ab12/feed-123/2026-03-06/115-22222222-2222-2222-2222-222222222222.flac",
                            ),
                            115,
                        )
                    ]
                )
                .advance_watermark_to(130)
                .add_elements(
                    [
                        TimestampedValue(
                            (
                                "feed-123",
                                "gs://fake-bucket/ab12/feed-123/2026-03-06/130-33333333-3333-3333-3333-333333333333.flac",
                            ),
                            130,
                        )
                    ]
                )
                .advance_watermark_to(150)
                .add_elements(
                    [
                        TimestampedValue(
                            (
                                "feed-123",
                                "gs://fake-bucket/ab12/feed-123/2026-03-06/150-44444444-4444-4444-4444-444444444444.flac",
                            ),
                            150,
                        )
                    ]
                )
                .advance_watermark_to(160)
                .add_elements(
                    [
                        TimestampedValue(
                            (
                                "feed-123",
                                "gs://fake-bucket/ab12/feed-123/2026-03-06/160-55555555-5555-5555-5555-555555555555.flac",
                            ),
                            160,
                        )
                    ]
                )
                .advance_watermark_to(190)
                .add_elements(
                    [
                        TimestampedValue(
                            (
                                "feed-123",
                                "gs://fake-bucket/ab12/feed-123/2026-03-06/190-66666666-6666-6666-6666-666666666666.flac",
                            ),
                            190,
                        )
                    ]
                )
                .advance_watermark_to_infinity()
            )

            results = (
                p
                | test_stream
                | beam.ParDo(
                    StitchAndTranscribeFn(
                        config=config,
                        transcriber_factory=get_mock_factory("Simulated transcript."),
                    )
                ).with_outputs(DEAD_LETTER_QUEUE_TAG, main="main")
            )

            def assert_transcripts(elements: list[TranscriptionResult]) -> None:
                assert len(elements) == 3, (
                    f"Expected 3 transcripts, got {len(elements)}: {elements}"
                )
                uuids_list = [set(str(u) for u in el.audio_ids) for el in elements]
                assert {
                    "11111111-1111-1111-1111-111111111111",
                    "22222222-2222-2222-2222-222222222222",
                } in uuids_list
                assert {"33333333-3333-3333-3333-333333333333"} in uuids_list
                assert {"55555555-5555-5555-5555-555555555555"} in uuids_list

            assert_that(results.main, assert_transcripts, label="CheckMainTranscripts")

    @patch("backend.pipeline.transcription.stitcher.AudioProcessor")
    def test_max_transmission_duration_flush(
        self, mock_audio_processor: MagicMock
    ) -> None:
        """Test that a continuous transmission exceeding max duration is flushed even without gaps."""
        mock_processor_inst = mock_audio_processor.return_value
        mock_processor_inst.check_vad.return_value = True
        mock_processor_inst.preprocess_audio.side_effect = lambda x: x
        mock_processor_inst.export_flac.return_value = b"flac_bytes"

        sed_map = {
            "100-77777777-7777-7777-7777-777777777777.flac": [(0.0, 5.0)],
            "105-88888888-8888-8888-8888-888888888888.flac": [(0.0, 5.0)],
            "110-99999999-9999-9999-9999-999999999999.flac": [(0.0, 5.0)],
            "130-aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa.flac": [(0.0, 2.0)],
        }

        def mock_download(path: str) -> AudioFileData:
            filename = path.rsplit("/", maxsplit=1)[-1]
            chunk_start = float(filename.split("-")[0]) if "-" in filename else 0.0
            return AudioFileData(
                start_ms=int(chunk_start * 1000),
                audio=AudioSegment.silent(duration=5000),
                speech_segments=[
                    TimeRange(int(s * 1000), int(e * 1000))
                    for s, e in sed_map.get(filename, [])
                ],
            )

        mock_processor_inst.download_audio_and_sed.side_effect = mock_download

        options = PipelineOptions()
        options.view_as(StandardOptions).streaming = True

        # Set max duration to 10 seconds.
        config = get_test_config(
            max_transmission_duration_ms=10000, significant_gap_ms=9999
        )

        with BeamTestPipeline(options=options) as p:
            test_stream = (
                BeamTestStream(
                    coder=beam.coders.TupleCoder(
                        (beam.coders.StrUtf8Coder(), beam.coders.StrUtf8Coder())
                    )
                )
                .advance_watermark_to(100)
                .add_elements(
                    [
                        TimestampedValue(
                            (
                                "feed-max",
                                "gs://fake-bucket/ab12/feed-max/2026-03-06/100-77777777-7777-7777-7777-777777777777.flac",
                            ),
                            100,
                        )
                    ]
                )
                .advance_watermark_to(105)
                .add_elements(
                    [
                        TimestampedValue(
                            (
                                "feed-max",
                                "gs://fake-bucket/ab12/feed-max/2026-03-06/105-88888888-8888-8888-8888-888888888888.flac",
                            ),
                            105,
                        )
                    ]
                )
                .advance_watermark_to(110)
                .add_elements(
                    [
                        TimestampedValue(
                            (
                                "feed-max",
                                "gs://fake-bucket/ab12/feed-max/2026-03-06/110-99999999-9999-9999-9999-999999999999.flac",
                            ),
                            110,
                        )
                    ]
                )
                .advance_watermark_to(130)
                .add_elements(
                    [
                        TimestampedValue(
                            (
                                "feed-max",
                                "gs://fake-bucket/ab12/feed-max/2026-03-06/130-aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa.flac",
                            ),
                            130,
                        )
                    ]
                )
                .advance_watermark_to_infinity()
            )

            results = (
                p
                | test_stream
                | beam.ParDo(
                    StitchAndTranscribeFn(
                        config=config,
                        transcriber_factory=get_mock_factory(
                            "Simulated long transcript."
                        ),
                    )
                ).with_outputs(DEAD_LETTER_QUEUE_TAG, main="main")
            )

            def assert_transcripts(elements: list[TranscriptionResult]) -> None:
                assert len(elements) == 2, (
                    f"Expected 2 transcripts, got {len(elements)}: {elements}"
                )
                uuids_list = [set(str(u) for u in el.audio_ids) for el in elements]
                assert {
                    "77777777-7777-7777-7777-777777777777",
                    "88888888-8888-8888-8888-888888888888",
                } in uuids_list
                assert {"99999999-9999-9999-9999-999999999999"} in uuids_list

            assert_that(
                results.main, assert_transcripts, label="CheckMaxDurationTranscripts"
            )

    @unittest.skip("DirectRunner timer advancing is buggy and unsupported natively.")
    @patch("backend.pipeline.transcription.stitcher.time")
    @patch("backend.pipeline.transcription.stitcher.AudioProcessor")
    def test_stale_transmission_timer(
        self,
        mock_audio_processor: MagicMock,
        mock_time: MagicMock,
    ) -> None:
        """
        Test that stale audio chunks are flushed after a timeout.

        Note: This test is skipped because the Apache Beam DirectRunner (used for local
        execution and unit testing) has known bugs and limitations regarding timer advancing
        and watermark simulation in streaming pipelines. This test attempts to validate 
        the 30-second stale transmission timeout, which cannot be reliably tested locally.
        It is retained to document the intended behavior and can be run via an E2E test 
        on the DataflowRunner.
        """
        mock_time.time.return_value = 0
        mock_processor_inst = mock_audio_processor.return_value
        mock_processor_inst.check_vad.return_value = True
        mock_processor_inst.preprocess_audio.side_effect = lambda x: x
        mock_processor_inst.export_flac.return_value = b"flac_bytes"
        mock_processor_inst.download_audio_and_sed.return_value = AudioFileData(
            start_ms=101000,
            audio=AudioSegment.silent(duration=20000),
            speech_segments=[TimeRange(12500, 15000)],
        )

        options = PipelineOptions()
        options.view_as(StandardOptions).streaming = True

        config = get_test_config()

        with BeamTestPipeline(options=options) as p:
            test_stream = (
                BeamTestStream(
                    coder=beam.coders.TupleCoder(
                        (beam.coders.StrUtf8Coder(), beam.coders.StrUtf8Coder())
                    )
                )
                .advance_watermark_to(0)
                .add_elements(
                    [
                        TimestampedValue(
                            (
                                "feed-123",
                                "gs://fake-bucket/ab12/feed-123/2026-03-06/101-11111111-1111-1111-1111-111111111111.flac",
                            ),
                            101,
                        )
                    ]
                )
                .advance_watermark_to(170)
                .advance_watermark_to_infinity()
            )

            results = (
                p
                | test_stream
                | beam.ParDo(
                    StitchAndTranscribeFn(
                        config=config,
                        transcriber_factory=get_mock_factory("Simulated transcript."),
                    )
                ).with_outputs(DEAD_LETTER_QUEUE_TAG, main="main")
            )

            def assert_stale_match(elements: list[TranscriptionResult]) -> None:
                assert len(elements) == 1, f"Expected 1 element, got {len(elements)}"
                res = elements[0]
                assert res.feed_id == "feed-123"
                assert res.transcript == "Simulated transcript."

            assert_that(
                results.main,
                assert_stale_match,
                label="CheckStaleMain",
            )
            assert_that(
                results[DEAD_LETTER_QUEUE_TAG],
                equal_to([]),
                label="CheckStaleEmptyDLQ",
            )

    @patch("backend.pipeline.transcription.stitcher.AudioProcessor")
    def test_dlq_routing(self, mock_audio_processor: MagicMock) -> None:
        """Test that failing transcription routes to DLQ."""
        mock_processor_inst = mock_audio_processor.return_value
        mock_processor_inst.check_vad.return_value = True
        mock_processor_inst.preprocess_audio.side_effect = lambda x: x
        mock_processor_inst.export_flac.return_value = b"flac_bytes"

        def mock_download(path: str) -> AudioFileData:
            filename = path.rsplit("/", maxsplit=1)[-1]
            chunk_start = float(filename.split("-")[0]) if "-" in filename else 0.0
            if "silent" in path:
                return AudioFileData(
                    start_ms=int(chunk_start * 1000),
                    audio=AudioSegment.silent(duration=500),
                    speech_segments=[],
                )
            return AudioFileData(
                start_ms=int(chunk_start * 1000),
                audio=AudioSegment.silent(duration=500),
                speech_segments=[TimeRange(0, 1000)],
            )

        mock_processor_inst.download_audio_and_sed.side_effect = mock_download

        config = get_test_config()

        with BeamTestPipeline() as p:
            test_stream = (
                BeamTestStream(
                    coder=beam.coders.TupleCoder(
                        (beam.coders.StrUtf8Coder(), beam.coders.StrUtf8Coder())
                    )
                )
                .advance_watermark_to(0)
                .add_elements(
                    [
                        TimestampedValue(
                            (
                                "feed-123",
                                "gs://fake-bucket/ab12/feed-123/2026-03-06/101-11111111-1111-1111-1111-111111111111.flac",
                            ),
                            101,
                        )
                    ]
                )
                .advance_watermark_to(102)
                .add_elements(
                    [
                        TimestampedValue(
                            (
                                "feed-123",
                                "gs://fake-bucket/ab12/feed-123/2026-03-06/102-44444444-4444-4444-4444-444444444444.flac",
                            ),
                            102,
                        )
                    ]
                )
                .advance_watermark_to_infinity()
            )
            results = (
                p
                | test_stream
                | beam.ParDo(
                    StitchAndTranscribeFn(
                        config=config,
                        transcriber_factory=get_mock_factory(raise_exception=True),
                    )
                ).with_outputs(DEAD_LETTER_QUEUE_TAG, main="main")
            )

            def assert_dlq(elements: list[dict[str, Any]]) -> None:
                assert len(elements) == 1
                assert "Transcription API outage!" in elements[0]["error"]

            assert_that(results.main, equal_to([]), label="CheckEmptyMain")
            assert_that(results[DEAD_LETTER_QUEUE_TAG], assert_dlq, label="CheckDLQ")

    @patch("backend.pipeline.transcription.stitcher.AudioProcessor")
    def test_missing_gcs_file(self, mock_audio_processor: MagicMock) -> None:
        """Test missing file raises exception to fail fast."""
        mock_processor_inst = mock_audio_processor.return_value
        mock_processor_inst.download_audio_and_sed.side_effect = FileNotFoundError(
            "Not found"
        )

        config = get_test_config()

        with self.assertRaises(Exception):
            with BeamTestPipeline() as p:
                input_elements = [
                    (
                        "feed-123",
                        "gs://fake-bucket/123-00000000-0000-0000-0000-000000000000.flac",
                    )
                ]
                (
                    p
                    | "Create" >> beam.Create(input_elements)
                    | beam.ParDo(
                        StitchAndTranscribeFn(
                            config=config,
                            transcriber_factory=get_mock_factory(),
                        )
                    ).with_outputs(DEAD_LETTER_QUEUE_TAG, main="main")
                )

    @patch("backend.pipeline.transcription.stitcher.AudioProcessor")
    @patch("backend.pipeline.transcription.stitcher.get_transcriber")
    def test_missing_uuid_dlq(
        self, mock_get_transcriber: MagicMock, mock_audio_processor: MagicMock
    ) -> None:
        """Test missing uuid from GCS name routes to DLQ."""
        config = get_test_config()
        with BeamTestPipeline() as p:
            elements = p | beam.Create(
                [("feed-123", "gs://fake-bucket/no_dashes_here.flac")]
            )
            results = elements | beam.ParDo(
                StitchAndTranscribeFn(config=config)
            ).with_outputs(DEAD_LETTER_QUEUE_TAG, main="main")

            def assert_invalid(elements: list[dict[str, Any]]) -> None:
                assert len(elements) == 1
                assert "Could not extract UUID from filename" in elements[0]["error"]

            assert_that(results.main, equal_to([]), label="CheckEmptyMain")
            assert_that(
                results[DEAD_LETTER_QUEUE_TAG], assert_invalid, label="CheckDLQI"
            )

    @unittest.skip("DirectRunner background execution validation is unreliable.")
    @patch("backend.pipeline.transcription.stitcher.AudioProcessor")
    def test_concurrent_transcription_execution(
        self, mock_audio_processor: MagicMock
    ) -> None:
        """
        Verify that _flush_buffer executes in a background thread pool.

        Note: This test is skipped because the Apache Beam DirectRunner isolating
        workers locally makes multithreading execution validation extremely flaky
        and unreliable. Testing that audio flushing happens concurrently in a 
        separate ThreadPoolExecutor works in Dataflow but causes intermittent 
        test failures locally. It is retained to document the threading model.
        """
        execution_threads = set()
        lock = threading.Lock()

        class MockTrackingTranscriber(Transcriber):
            """Mock Transcriber that tracks thread execution."""

            def setup(self) -> None:
                pass

            def transcribe(self, audio_data: bytes) -> str:
                with lock:
                    execution_threads.add(threading.current_thread().name)
                # Small sleep to encourage concurrency when multiple items arrive
                time.sleep(0.1)
                return "Mock Output"

        def tracking_factory(
            t_type: TranscriberType, p_id: str, cfg: str
        ) -> Transcriber:
            return MockTrackingTranscriber()

        mock_processor_inst = mock_audio_processor.return_value
        mock_processor_inst.check_vad.return_value = True
        mock_processor_inst.preprocess_audio.side_effect = lambda x: x
        mock_processor_inst.export_flac.return_value = b"flac_bytes"

        # Generate fake audio chunks
        def mock_download(path: str) -> AudioFileData:
            filename = path.rsplit("/", maxsplit=1)[-1]
            chunk_start = float(filename.split("-")[0]) if "-" in filename else 0.0
            return AudioFileData(
                start_ms=int(chunk_start * 100000),  # 100s apart to force flushes
                audio=AudioSegment.silent(duration=1000),
                speech_segments=[TimeRange(0, 1000)],
            )

        mock_processor_inst.download_audio_and_sed.side_effect = mock_download

        main_thread_name = threading.current_thread().name

        options = PipelineOptions()
        options.view_as(StandardOptions).streaming = True

        config = get_test_config(significant_gap_ms=2000)

        with BeamTestPipeline(options=options) as p:
            test_stream = (
                BeamTestStream(
                    coder=beam.coders.TupleCoder(
                        (beam.coders.StrUtf8Coder(), beam.coders.StrUtf8Coder())
                    )
                )
                .advance_watermark_to(0)
                # Add two distinct chunks that will each trigger a flush because of the gap
                .add_elements(
                    [
                        TimestampedValue(
                            (
                                "feed-123",
                                "gs://b/0.0-bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb.flac",
                            ),
                            0,
                        ),
                    ]
                )
                .advance_watermark_to(5)  # create gap
                .add_elements(
                    [
                        TimestampedValue(
                            (
                                "feed-123",
                                "gs://b/5.0-cccccccc-cccc-cccc-cccc-cccccccccccc.flac",
                            ),
                            5,
                        ),
                    ]
                )
                .advance_watermark_to_infinity()
            )

            (
                p
                | test_stream
                | beam.ParDo(
                    StitchAndTranscribeFn(
                        config=config, transcriber_factory=tracking_factory
                    )
                ).with_outputs(DEAD_LETTER_QUEUE_TAG, main="main")
            )

        self.assertTrue(
            len(execution_threads) > 0, "Transcription should have executed"
        )
        self.assertFalse(
            main_thread_name in execution_threads,
            f"Transcription executed on MainThread ({main_thread_name}) instead of a background thread pool!",
        )
