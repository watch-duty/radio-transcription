"""
Tests for the StitchAndTranscribeFn and related transformations.
"""

import threading
import unittest
from typing import Any
from unittest.mock import MagicMock, patch

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.testing.test_pipeline import TestPipeline as BeamTestPipeline
from apache_beam.testing.test_stream import TestStream as BeamTestStream
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.transforms.window import TimestampedValue
from backend.pipeline.transcription.constants import DEAD_LETTER_QUEUE_TAG, MAIN_TAG
from backend.pipeline.transcription.enums import TranscriberType, VadType
from pydub import AudioSegment
from backend.pipeline.transcription.transcribers import Transcriber
from backend.pipeline.transcription.transforms import (
    AddEventTimestamp,
    FlushRequest,
    ParseAndKeyFn,
    StitchAndTranscribeConfig,
    StitchAndTranscribeFn,
    TranscriptionResult,
)


class MockTranscriberFactory:
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
    return MockTranscriberFactory(transcript, raise_exception=raise_exception)


def get_test_config(**kwargs: Any) -> StitchAndTranscribeConfig:
    defaults = {
        "project_id": "fake-proj",
        "transcriber_type": TranscriberType.GOOGLE_CHIRP_V3,
        "transcriber_config": "{}",
        "vad_type": VadType.TEN_VAD,
        "vad_config": "{}",
        "metrics_exporter_type": "",
        "metrics_config": "{}",
        "significant_gap_sec": 0.5,
        "stale_timeout_sec": 60.0,
    }
    defaults.update(kwargs)
    return StitchAndTranscribeConfig(**defaults)  # type: ignore


class ParseAndKeyTimestampTest(unittest.TestCase):
    def test_parse_and_key_success(self) -> None:
        mock_msg = beam.io.gcp.pubsub.PubsubMessage(  # type: ignore
            b"test_payload",
            {
                "feed_id": "test-feed",
                "bucketId": "test-bucket",
                "objectId": "path/to/test.flac",
            },
        )
        with BeamTestPipeline() as p:  # type: ignore
            messages = p | beam.Create([mock_msg])  # type: ignore
            parsed = messages | beam.ParDo(ParseAndKeyFn()).with_outputs(  # type: ignore
                DEAD_LETTER_QUEUE_TAG, main="main"
            )
            assert_that(  # type: ignore
                parsed.main,
                equal_to([("test-feed", "gs://test-bucket/path/to/test.flac")]),  # type: ignore
            )
            assert_that(  # type: ignore
                parsed[DEAD_LETTER_QUEUE_TAG],
                equal_to([]),  # type: ignore
                label="CheckEmptyDLQ",
            )

    def test_parse_and_key_dlq(self) -> None:
        mock_msg = beam.io.gcp.pubsub.PubsubMessage(  # type: ignore
            b"test_payload", {"feed_id": "test-feed"}
        )
        with BeamTestPipeline() as p:  # type: ignore
            messages = p | beam.Create([mock_msg])  # type: ignore
            parsed = messages | beam.ParDo(ParseAndKeyFn()).with_outputs(  # type: ignore
                DEAD_LETTER_QUEUE_TAG, main="main"
            )

            def assert_dlq(elements: list[dict[str, Any]]) -> None:
                assert len(elements) == 1
                assert "Missing required payload attribute" in elements[0]["error"]

            assert_that(parsed.main, equal_to([]), label="CheckEmptyMain")  # type: ignore
            assert_that(parsed[DEAD_LETTER_QUEUE_TAG], assert_dlq, label="CheckDLQ")  # type: ignore


class AddEventTimestampTest(unittest.TestCase):
    def test_valid_timestamp_extraction(self) -> None:
        element = (
            "test-feed",
            "gs://bucket/hash/feed_id/YYYY-MM-DD/1678123456-uuid1.flac",
        )
        with BeamTestPipeline() as p:  # type: ignore
            elements = p | beam.Create([element])  # type: ignore
            timestamped = elements | beam.ParDo(AddEventTimestamp())  # type: ignore
            assert_that(timestamped, equal_to([element]))  # type: ignore

    def test_invalid_timestamp_raises_value_error(self) -> None:
        element = (
            "test-feed",
            "gs://bucket/hash/feed_id/YYYY-MM-DD/invalid-uuid1.flac",
        )
        fn = AddEventTimestamp()  # type: ignore
        with self.assertRaises(ValueError):
            list(fn.process(element))


class StitchAndTranscribeTest(unittest.TestCase):
    @patch("backend.pipeline.transcription.transforms.AudioProcessor")
    def test_stitching_and_silence_flush_logic(
        self, mock_audio_processor: MagicMock
    ) -> None:
        mock_processor_inst = mock_audio_processor.return_value
        mock_processor_inst.check_vad.return_value = True
        mock_processor_inst.preprocess_audio.side_effect = lambda x: x
        mock_processor_inst.export_flac.return_value = b"flac_bytes"

        sad_map = {
            "101-uuid_starts.flac": [(12.5, 15.0)],
            "102-uuid_completes_contains.flac": [(0.0, 2.5), (5.0, 7.0)],
            "103-uuid_starts_again.flac": [(12.5, 15.0)],
            "104-uuid_silent.flac": [],
        }

        def mock_download(path: str) -> tuple[AudioSegment, list[tuple[float, float]]]:
            filename = path.rsplit("/", maxsplit=1)[-1]
            return AudioSegment.silent(duration=20000), sad_map.get(filename, [])

        mock_processor_inst.download_audio_and_sad.side_effect = mock_download

        options = PipelineOptions()
        options.view_as(StandardOptions).streaming = True

        config = get_test_config(significant_gap_sec=3.0)

        with BeamTestPipeline(options=options) as p:  # type: ignore
            test_stream = (
                BeamTestStream(  # type: ignore
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
                                "gs://fake-bucket/ab12/feed-123/2026-03-06/101-uuid_starts.flac",
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
                                "gs://fake-bucket/ab12/feed-123/2026-03-06/102-uuid_completes_contains.flac",
                            ),
                            102,
                        )
                    ]
                )
                .advance_watermark_to(103)
                .add_elements(
                    [
                        TimestampedValue(
                            (
                                "feed-123",
                                "gs://fake-bucket/ab12/feed-123/2026-03-06/103-uuid_starts_again.flac",
                            ),
                            103,
                        )
                    ]
                )
                .advance_watermark_to(104)
                .add_elements(
                    [
                        TimestampedValue(
                            (
                                "feed-123",
                                "gs://fake-bucket/ab12/feed-123/2026-03-06/104-uuid_silent.flac",
                            ),
                            104,
                        )
                    ]
                )
                .advance_watermark_to_infinity()
            )

            results = (
                p
                | test_stream
                | beam.ParDo(  # type: ignore
                    StitchAndTranscribeFn(
                        config=config,
                        transcriber_factory=get_mock_factory("Simulated transcript."),
                    )
                ).with_outputs(main=MAIN_TAG)
            )

            def assert_transcripts(elements: list[TranscriptionResult]) -> None:
                assert len(elements) == 2, (
                    f"Expected 2 transcripts, got {len(elements)}: {elements}"
                )
                uuids_list = [set(el.audio_ids) for el in elements]
                assert {"uuid_starts", "uuid_completes_contains"} in uuids_list
                assert {"uuid_starts_again"} in uuids_list

            assert_that(
                results[MAIN_TAG], assert_transcripts, label="CheckMainTranscripts"
            )  # type: ignore

    @patch("backend.pipeline.transcription.transforms.time")
    @patch("backend.pipeline.transcription.transforms.AudioProcessor")
    def test_stale_transmission_timer(
        self,
        mock_audio_processor: MagicMock,
        mock_time: MagicMock,
    ) -> None:
        mock_time.time.return_value = 0
        mock_processor_inst = mock_audio_processor.return_value
        mock_processor_inst.check_vad.return_value = True
        mock_processor_inst.preprocess_audio.side_effect = lambda x: x
        mock_processor_inst.export_flac.return_value = b"flac_bytes"
        mock_processor_inst.download_audio_and_sad.return_value = (
            AudioSegment.silent(duration=20000),
            [(12.5, 15.0)],
        )

        options = PipelineOptions()
        options.view_as(StandardOptions).streaming = True

        config = get_test_config()

        with BeamTestPipeline(options=options) as p:  # type: ignore
            test_stream = (
                BeamTestStream(  # type: ignore
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
                                "gs://fake-bucket/ab12/feed-123/2026-03-06/101-uuid_starts.flac",
                            ),
                            101,
                        )
                    ]
                )
                .advance_processing_time(65)
                .advance_watermark_to_infinity()
            )

            results = (
                p
                | test_stream
                | beam.ParDo(  # type: ignore
                    StitchAndTranscribeFn(
                        config=config,
                        transcriber_factory=get_mock_factory("Simulated transcript."),
                    )
                ).with_outputs(DEAD_LETTER_QUEUE_TAG, main=MAIN_TAG)
            )

            def assert_stale_match(elements: list[TranscriptionResult]) -> None:
                assert len(elements) == 1, f"Expected 1 element, got {len(elements)}"
                res = elements[0]
                assert res.feed_id == "feed-123"
                assert res.transcript == "Simulated transcript."

            assert_that(  # type: ignore
                results[MAIN_TAG],
                assert_stale_match,
                label="CheckStaleMain",
            )
            assert_that(  # type: ignore
                results[DEAD_LETTER_QUEUE_TAG],
                equal_to([]),  # type: ignore
                label="CheckStaleEmptyDLQ",
            )

    @patch("backend.pipeline.transcription.transforms.AudioProcessor")
    def test_dlq_routing(self, mock_audio_processor: MagicMock) -> None:
        mock_processor_inst = mock_audio_processor.return_value
        mock_processor_inst.check_vad.return_value = True
        mock_processor_inst.preprocess_audio.side_effect = lambda x: x
        mock_processor_inst.export_flac.return_value = b"flac_bytes"

        def mock_download(path: str) -> tuple[AudioSegment, list[tuple[float, float]]]:
            if "silent" in path:
                return AudioSegment.silent(duration=500), []
            return AudioSegment.silent(duration=500), [(0.0, 1.0)]

        mock_processor_inst.download_audio_and_sad.side_effect = mock_download

        config = get_test_config()

        with BeamTestPipeline() as p:  # type: ignore
            test_stream = (
                BeamTestStream(  # type: ignore
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
                                "gs://fake-bucket/ab12/feed-123/2026-03-06/101-uuid_starts.flac",
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
                                "gs://fake-bucket/ab12/feed-123/2026-03-06/102-uuid_silent.flac",
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
                | beam.ParDo(  # type: ignore
                    StitchAndTranscribeFn(
                        config=config,
                        transcriber_factory=get_mock_factory(raise_exception=True),
                    )
                ).with_outputs(DEAD_LETTER_QUEUE_TAG, main=MAIN_TAG)
            )

            def assert_dlq(elements: list[dict[str, Any]]) -> None:
                assert len(elements) == 1
                assert "Transcription API outage!" in elements[0]["error"]

            assert_that(results.main, equal_to([]), label="CheckEmptyMain")  # type: ignore
            assert_that(results[DEAD_LETTER_QUEUE_TAG], assert_dlq, label="CheckDLQ")  # type: ignore

    @patch("backend.pipeline.transcription.transforms.AudioProcessor")
    def test_missing_gcs_file(self, mock_audio_processor: MagicMock) -> None:
        mock_processor_inst = mock_audio_processor.return_value
        mock_processor_inst.download_audio_and_sad.side_effect = FileNotFoundError(
            "Not found"
        )

        config = get_test_config()

        with BeamTestPipeline() as p:  # type: ignore
            input_elements = [("feed-123", "gs://fake-bucket/123-missing.flac")]
            results = (
                p
                | "Create" >> beam.Create(input_elements)  # type: ignore
                | "Process"
                >> beam.ParDo(  # type: ignore
                    StitchAndTranscribeFn(
                        config=config,
                        transcriber_factory=get_mock_factory("Simulated transcript."),
                    )
                ).with_outputs(DEAD_LETTER_QUEUE_TAG, main="main")
            )

            def assert_missing(elements: list[dict[str, Any]]) -> None:
                assert len(elements) == 1
                assert "Not found" in elements[0]["error"]

            assert_that(  # type: ignore
                results[DEAD_LETTER_QUEUE_TAG], assert_missing, label="CheckDLQM"
            )

    @patch("backend.pipeline.transcription.transforms.AudioProcessor")
    @patch("backend.pipeline.transcription.transforms.get_transcriber")
    def test_missing_uuid_dlq(
        self, mock_get_transcriber: MagicMock, mock_audio_processor: MagicMock
    ) -> None:
        config = get_test_config()
        with BeamTestPipeline() as p:  # type: ignore
            elements = p | beam.Create(  # type: ignore
                [("feed-123", "gs://fake-bucket/no_dashes_here.flac")]
            )
            results = elements | beam.ParDo(  # type: ignore
                StitchAndTranscribeFn(config=config)
            ).with_outputs(DEAD_LETTER_QUEUE_TAG, main="main")

            def assert_invalid(elements: list[dict[str, Any]]) -> None:
                assert len(elements) == 1
                assert "Could not extract UUID from filename" in elements[0]["error"]

            assert_that(results.main, equal_to([]), label="CheckEmptyMain")  # type: ignore
            assert_that(  # type: ignore
                results[DEAD_LETTER_QUEUE_TAG], assert_invalid, label="CheckDLQI"
            )

    @patch("backend.pipeline.transcription.transforms.AudioProcessor")
    def test_concurrent_transcription_execution(
        self, mock_audio_processor: MagicMock
    ) -> None:
        """Verify that _flush_buffer executes in a background thread pool."""
        execution_threads = set()

        class MockTrackingTranscriber(Transcriber):
            def setup(self) -> None:
                pass

            def transcribe(self, *, audio_data: bytes) -> str:
                # Record the thread name executing this transcription
                execution_threads.add(threading.current_thread().name)
                return "Concurrent transcript"

        def tracking_factory(t: TranscriberType, p: str, c: str) -> Transcriber:
            return MockTrackingTranscriber()

        mock_processor_inst = mock_audio_processor.return_value
        mock_processor_inst.check_vad.return_value = True

        main_thread_name = threading.current_thread().name

        config = get_test_config()
        fn = StitchAndTranscribeFn(config=config, transcriber_factory=tracking_factory)
        fn.setup()

        req = FlushRequest(
            buffer=AudioSegment.silent(duration=5000, frame_rate=16000),
            feed_id="feed-123",
            processed_uuids={"uuid1"},
            start_sec=0.0,
            end_sec=5.0,
        )

        results = list(fn._execute_concurrent_flushes(flush_queue=[req, req]))  # noqa: SLF001
        fn.teardown()

        self.assertEqual(len(results), 2)
        self.assertTrue(
            len(execution_threads) > 0, "Transcription should have executed"
        )
        self.assertFalse(
            main_thread_name in execution_threads,
            f"Transcription executed on MainThread ({main_thread_name}) instead of a background thread pool!",
        )
