"""Tests for the StitchAudioFn, TranscribeAudioFn, and related transformations."""

import unittest
from typing import Any
from unittest.mock import MagicMock, patch

import apache_beam as beam
import numpy as np
from apache_beam.io.gcp.pubsub import PubsubMessage
from apache_beam.options.pipeline_options import (
    PipelineOptions,
    StandardOptions,
)
from apache_beam.testing.test_pipeline import TestPipeline as BeamTestPipeline
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.transforms.window import TimestampedValue

from backend.pipeline.schema_types.raw_audio_chunk_pb2 import AudioChunk
from backend.pipeline.transcription.constants import DEAD_LETTER_QUEUE_TAG
from backend.pipeline.transcription.datatypes import (
    AudioChunkData,
    ChunkMetadata,
    DownloadedChunkPayload,
    FeedMetadata,
    FlushRequest,
    StitchAudioConfig,
    TimeRange,
    TranscribeAudioConfig,
    TranscriptionResult,
)
from backend.pipeline.transcription.enums import TranscriberType, VadType
from backend.pipeline.transcription.stitcher import (
    StatelessStitchAudioFn,
    TranscribeAudioFn,
)
from backend.pipeline.transcription.transcribers import Transcriber
from backend.pipeline.transcription.transforms import (
    AddEventTimestamp,
    BypassStitchingFn,
    DownloadAudioFn,
    ParseAndKeyFn,
    SerializeAndEnrichFn,
    SortAndEmitFn,
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


class AddEventTimestampTest(unittest.TestCase):
    def test_valid_timestamp_extraction(self) -> None:
        """Verifies that AddEventTimestamp accurately regex-extracts and assigns the logical windowing timestamp natively from the chunk's standardized filename."""
        chunk = AudioChunk(
            gcs_uri="gs://bucket/hash/feed_id/YYYY-MM-DD/1678886400-bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb.flac",
            session_id="mock-session-id",
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
                "mock-session-id",
                ChunkMetadata(
                    gcs_uri="gs://bucket/hash/feed_id/YYYY-MM-DD/1678886400-bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb.flac",
                    session_id="mock-session-id",
                    duration_ms=0,
                    feed_id="test-feed",
                    timestamp_ms=1678886400000,
                    feed_metadata=FeedMetadata(feed_name=""),
                ),
            ),
        )
        self.assertEqual(result[0].timestamp, 1678886400)  # type: ignore

    def test_invalid_timestamp_raises_value_error(self) -> None:
        """Verifies that chunks possessing malformed or unidentifiable file names result in safely tagging the element for DLQ observation instead of crashing."""
        chunk = AudioChunk(
            gcs_uri="gs://bucket/hash/feed_id/YYYY-MM-DD/invalid-bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb.flac"
        )
        element = ("test-feed", chunk.SerializeToString())
        fn = AddEventTimestamp()

        result = list(fn.process(element))
        self.assertEqual(len(result), 1)
        self.assertIsInstance(result[0], beam.pvalue.TaggedOutput)
        self.assertEqual(result[0].tag, DEAD_LETTER_QUEUE_TAG)  # type: ignore


class BypassStitchingTest(unittest.TestCase):
    def test_bypass_stitching_maps_correctly(self) -> None:
        """Verifies that BypassStitchingFn correctly maps AudioChunkData to FlushRequest."""
        feed_id = "test-feed"
        gcs_path = "gs://bucket/test.flac"
        audio_len_ms = 5000
        chunk_data = AudioChunkData(
            start_ms=1000,
            audio=np.zeros(int((audio_len_ms) * 16), dtype=np.int16),
            speech_segments=[],
            gcs_uri=gcs_path,
            duration_ms=audio_len_ms,
        )

        element = (
            feed_id,
            DownloadedChunkPayload(
                gcs_uri=gcs_path,
                chunk_data=chunk_data,
                feed_metadata=FeedMetadata(feed_name="test-feed-name"),
                feed_id=feed_id,
            ),
        )

        fn = BypassStitchingFn()
        result = list(fn.process(element))

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0][0], feed_id)

        flush_request = result[0][1]
        self.assertIsInstance(flush_request, FlushRequest)
        self.assertEqual(flush_request.feed_id, feed_id)
        assert flush_request.feed_metadata is not None
        self.assertEqual(
            flush_request.feed_metadata.feed_name,
            "test-feed-name",
        )
        self.assertEqual(flush_request.contributing_audio_uris, [gcs_path])
        self.assertEqual(flush_request.time_range.start_ms, 1000)
        self.assertEqual(flush_request.time_range.end_ms, 1000 + audio_len_ms)
        self.assertFalse(flush_request.missing_prior_context)
        self.assertFalse(flush_request.missing_post_context)
        self.assertIsNotNone(flush_request.transmission_id)


class SortAndEmitTest(unittest.TestCase):
    def test_sort_and_emit(self) -> None:
        """Verifies that SortAndEmitFn correctly sorts chunks by timestamp."""
        chunks = [
            ChunkMetadata(
                "gs://b/130.flac",
                "session-A",
                15000,
                "feed-1",
                130000,
                FeedMetadata(feed_name="test-feed"),
            ),
            ChunkMetadata(
                "gs://b/100.flac",
                "session-A",
                15000,
                "feed-1",
                100000,
                FeedMetadata(feed_name="test-feed"),
            ),
            ChunkMetadata(
                "gs://b/115.flac",
                "session-A",
                15000,
                "feed-1",
                115000,
                FeedMetadata(feed_name="test-feed"),
            ),
        ]

        element = ("session-A", chunks)

        fn = SortAndEmitFn()
        result = list(fn.process(element))

        self.assertEqual(
            result,
            [
                ("feed-1", chunks[1]),
                ("feed-1", chunks[2]),
                ("feed-1", chunks[0]),
            ],
        )


class StitchAudioTest(unittest.TestCase):
    @patch("backend.pipeline.transcription.stitcher.AudioProcessor")
    def test_stitching_and_silence_flush_logic(
        self, mock_audio_processor: MagicMock
    ) -> None:
        """Verifies that AudioProcessor integrates multiple adjacent voice activity ranges correctly while accurately bounding isolated segments against configured gap timeouts."""
        mock_processor_inst = mock_audio_processor.return_value
        mock_processor_inst.check_vad.return_value = True
        mock_processor_inst.preprocess_audio.side_effect = lambda x: x
        mock_processor_inst.export_flac.return_value = b"flac_bytes"

        sed_map = {
            "100-11111111-1111-1111-1111-111111111111.flac": [(12.5, 15.0)],
            "115-22222222-2222-2222-2222-222222222222.flac": [
                (0.0, 2.5),
                (5.0, 7.0),
            ],
            "130-33333333-3333-3333-3333-333333333333.flac": [(0.0, 2.5)],
            "150-44444444-4444-4444-4444-444444444444.flac": [],
            "160-55555555-5555-5555-5555-555555555555.flac": [(0.0, 2.0)],
            "190-66666666-6666-6666-6666-666666666666.flac": [(0.0, 2.0)],
        }

        def mock_download(path: str, start_ms: int = 0) -> AudioChunkData:

            filename = path.rsplit("/", maxsplit=1)[-1]
            chunk_start = (
                float(filename.split("-")[0]) if "-" in filename else 0.0
            )

            duration_s = 20.0
            if filename.startswith(("100-", "115-")):
                duration_s = 15.0
            elif filename.startswith("150-"):
                duration_s = 5.0
            elif filename.startswith("160-"):
                duration_s = 30.0

            return AudioChunkData(
                start_ms=int(chunk_start * 1000),
                audio=np.zeros(int(duration_s * 16000), dtype=np.int16),
                speech_segments=[
                    TimeRange(int(s * 1000), int(e * 1000))
                    for s, e in sed_map.get(filename, [])
                ],
                gcs_uri=path,
                duration_ms=int(duration_s * 1000),
            )

        mock_processor_inst.download_audio_and_detect.side_effect = (
            mock_download
        )

        config = get_test_stitch_config(significant_gap_ms=3000)

        chunks = [
            DownloadedChunkPayload(
                gcs_uri="gs://fake-bucket/ab12/feed-123/2026-03-06/100-11111111-1111-1111-1111-111111111111.flac",
                chunk_data=mock_download(
                    "gs://fake-bucket/ab12/feed-123/2026-03-06/100-11111111-1111-1111-1111-111111111111.flac"
                ),
                feed_metadata=FeedMetadata(feed_name="test-feed"),
                feed_id="feed-123",
            ),
            DownloadedChunkPayload(
                gcs_uri="gs://fake-bucket/ab12/feed-123/2026-03-06/115-22222222-2222-2222-2222-222222222222.flac",
                chunk_data=mock_download(
                    "gs://fake-bucket/ab12/feed-123/2026-03-06/115-22222222-2222-2222-2222-222222222222.flac"
                ),
                feed_metadata=FeedMetadata(feed_name="test-feed"),
                feed_id="feed-123",
            ),
            DownloadedChunkPayload(
                gcs_uri="gs://fake-bucket/ab12/feed-123/2026-03-06/130-33333333-3333-3333-3333-333333333333.flac",
                chunk_data=mock_download(
                    "gs://fake-bucket/ab12/feed-123/2026-03-06/130-33333333-3333-3333-3333-333333333333.flac"
                ),
                feed_metadata=FeedMetadata(feed_name="test-feed"),
                feed_id="feed-123",
            ),
            DownloadedChunkPayload(
                gcs_uri="gs://fake-bucket/ab12/feed-123/2026-03-06/150-44444444-4444-4444-4444-444444444444.flac",
                chunk_data=mock_download(
                    "gs://fake-bucket/ab12/feed-123/2026-03-06/150-44444444-4444-4444-4444-444444444444.flac"
                ),
                feed_metadata=FeedMetadata(feed_name="test-feed"),
                feed_id="feed-123",
            ),
            DownloadedChunkPayload(
                gcs_uri="gs://fake-bucket/ab12/feed-123/2026-03-06/160-55555555-5555-5555-5555-555555555555.flac",
                chunk_data=mock_download(
                    "gs://fake-bucket/ab12/feed-123/2026-03-06/160-55555555-5555-5555-5555-555555555555.flac"
                ),
                feed_metadata=FeedMetadata(feed_name="test-feed"),
                feed_id="feed-123",
            ),
            DownloadedChunkPayload(
                gcs_uri="gs://fake-bucket/ab12/feed-123/2026-03-06/190-66666666-6666-6666-6666-666666666666.flac",
                chunk_data=mock_download(
                    "gs://fake-bucket/ab12/feed-123/2026-03-06/190-66666666-6666-6666-6666-666666666666.flac"
                ),
                feed_metadata=FeedMetadata(feed_name="test-feed"),
                feed_id="feed-123",
            ),
        ]

        element = ("session-A", chunks)
        fn = StatelessStitchAudioFn(config=config)
        results = list(fn.process(element))

        self.assertEqual(
            len(results),
            4,
            f"Expected 4 flush requests, got {len(results)}: {results}",
        )
        results.sort(key=lambda x: x[1].time_range.start_ms)

        # First element: chunks 100, 115
        self.assertTrue(
            any(
                "11111111-1111-1111-1111-111111111111" in u
                for u in results[0][1].contributing_audio_uris
            )
        )
        self.assertTrue(
            any(
                "22222222-2222-2222-2222-222222222222" in u
                for u in results[0][1].contributing_audio_uris
            )
        )

        # Second element: chunk 130
        self.assertTrue(
            any(
                "33333333-3333-3333-3333-333333333333" in u
                for u in results[1][1].contributing_audio_uris
            )
        )

        # Third element: chunk 160
        self.assertTrue(
            any(
                "55555555-5555-5555-5555-555555555555" in u
                for u in results[2][1].contributing_audio_uris
            )
        )

        # Fourth element: chunk 190 (force flush)
        self.assertTrue(
            any(
                "66666666-6666-6666-6666-666666666666" in u
                for u in results[3][1].contributing_audio_uris
            )
        )

    @patch("backend.pipeline.transcription.stitcher.AudioProcessor")
    def test_isolated_late_chunk_processing(
        self, mock_audio_processor: MagicMock
    ) -> None:
        """Verifies that an explicitly labeled late chunk bypassed by system ordering constraints uniquely triggers an isolated contextual processing branch natively."""
        mock_processor_inst = mock_audio_processor.return_value
        mock_processor_inst.check_vad.return_value = True
        mock_processor_inst.preprocess_audio.side_effect = lambda x: x
        mock_processor_inst.export_flac.return_value = b"flac_bytes"

        sed_map = {
            "100-11111111-1111-1111-1111-111111111111.flac": [(0.0, 15.0)],
            "130-33333333-3333-3333-3333-333333333333.flac": [(0.0, 15.0)],
            "115-22222222-2222-2222-2222-222222222222.flac": [(2.0, 15.0)],
        }

        def mock_download(path: str, start_ms: int = 0) -> AudioChunkData:

            filename = path.rsplit("/", maxsplit=1)[-1]
            chunk_start = (
                float(filename.split("-")[0]) if "-" in filename else 0.0
            )
            return AudioChunkData(
                start_ms=int(chunk_start * 1000),
                audio=np.zeros(((15000) * 16), dtype=np.int16),
                speech_segments=[
                    TimeRange(int(s * 1000), int(e * 1000))
                    for s, e in sed_map.get(filename, [])
                ],
                gcs_uri=path,
                duration_ms=15000,
            )

        mock_processor_inst.download_audio_and_detect.side_effect = (
            mock_download
        )
        config = get_test_stitch_config(significant_gap_ms=3000)

        chunks = [
            DownloadedChunkPayload(
                gcs_uri="gs://fake-bucket/100-11111111-1111-1111-1111-111111111111.flac",
                chunk_data=mock_download(
                    "gs://fake-bucket/100-11111111-1111-1111-1111-111111111111.flac"
                ),
                feed_metadata=FeedMetadata(feed_name="test-feed"),
                feed_id="feed-123",
            ),
            DownloadedChunkPayload(
                gcs_uri="gs://fake-bucket/130-33333333-3333-3333-3333-333333333333.flac",
                chunk_data=mock_download(
                    "gs://fake-bucket/130-33333333-3333-3333-3333-333333333333.flac"
                ),
                feed_metadata=FeedMetadata(feed_name="test-feed"),
                feed_id="feed-123",
            ),
            DownloadedChunkPayload(
                gcs_uri="gs://fake-bucket/115-22222222-2222-2222-2222-222222222222.flac",
                chunk_data=mock_download(
                    "gs://fake-bucket/115-22222222-2222-2222-2222-222222222222.flac"
                ),
                feed_metadata=FeedMetadata(feed_name="test-feed"),
                feed_id="feed-123",
            ),
        ]

        element = ("session-A", chunks)
        fn = StatelessStitchAudioFn(config=config)
        results = list(fn.process(element))

        self.assertEqual(
            len(results),
            1,
            f"Expected 1 flush request, got {len(results)}: {results}",
        )

        # Assertions for the single flush
        self.assertTrue(
            any(
                "11111111-1111-1111-1111-111111111111" in u
                for u in results[0][1].contributing_audio_uris
            )
        )
        self.assertTrue(
            any(
                "22222222-2222-2222-2222-222222222222" in u
                for u in results[0][1].contributing_audio_uris
            )
        )
        self.assertTrue(
            any(
                "33333333-3333-3333-3333-333333333333" in u
                for u in results[0][1].contributing_audio_uris
            )
        )

    @patch("backend.pipeline.transcription.stitcher.AudioProcessor")
    def test_max_transmission_duration_flush(
        self, mock_audio_processor: MagicMock
    ) -> None:
        """Verifies that seamlessly contiguous, infinite-duration voice segments forcibly truncate gracefully when hitting the globally configured memory limits."""
        mock_processor_inst = mock_audio_processor.return_value
        mock_processor_inst.check_vad.return_value = True
        mock_processor_inst.preprocess_audio.side_effect = lambda x: x
        mock_processor_inst.export_flac.return_value = b"flac_bytes"

        sed_map = {
            "100-77777777-7777-7777-7777-777777777777.flac": [(0.0, 15.0)],
            "115-88888888-8888-8888-8888-888888888888.flac": [(0.0, 15.0)],
            "130-99999999-9999-9999-9999-999999999999.flac": [(0.0, 15.0)],
            "160-aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa.flac": [(0.0, 2.0)],
        }

        def mock_download(path: str, start_ms: int = 0) -> AudioChunkData:

            filename = path.rsplit("/", maxsplit=1)[-1]
            chunk_start = (
                float(filename.split("-")[0]) if "-" in filename else 0.0
            )
            return AudioChunkData(
                start_ms=int(chunk_start * 1000),
                audio=np.zeros(((15000) * 16), dtype=np.int16),
                speech_segments=[
                    TimeRange(int(s * 1000), int(e * 1000))
                    for s, e in sed_map.get(filename, [])
                ],
                gcs_uri=path,
                duration_ms=15000,
            )

        mock_processor_inst.download_audio_and_detect.side_effect = (
            mock_download
        )

        options = PipelineOptions(
            flags=["--input_subscription=a", "--output_topic=b", "--project=c"]
        )
        options.view_as(StandardOptions).streaming = True

        # Set max duration to 30 seconds (2 full chunks).
        config = get_test_stitch_config(
            max_transmission_duration_ms=30000, significant_gap_ms=29999
        )

        chunks = [
            DownloadedChunkPayload(
                gcs_uri="gs://fake-bucket/ab12/feed-max/2026-03-06/100-77777777-7777-7777-7777-777777777777.flac",
                chunk_data=mock_download(
                    "gs://fake-bucket/ab12/feed-max/2026-03-06/100-77777777-7777-7777-7777-777777777777.flac"
                ),
                feed_metadata=FeedMetadata(feed_name="test-feed"),
                feed_id="feed-max",
            ),
            DownloadedChunkPayload(
                gcs_uri="gs://fake-bucket/ab12/feed-max/2026-03-06/115-88888888-8888-8888-8888-888888888888.flac",
                chunk_data=mock_download(
                    "gs://fake-bucket/ab12/feed-max/2026-03-06/115-88888888-8888-8888-8888-888888888888.flac"
                ),
                feed_metadata=FeedMetadata(feed_name="test-feed"),
                feed_id="feed-max",
            ),
            DownloadedChunkPayload(
                gcs_uri="gs://fake-bucket/ab12/feed-max/2026-03-06/130-99999999-9999-9999-9999-999999999999.flac",
                chunk_data=mock_download(
                    "gs://fake-bucket/ab12/feed-max/2026-03-06/130-99999999-9999-9999-9999-999999999999.flac"
                ),
                feed_metadata=FeedMetadata(feed_name="test-feed"),
                feed_id="feed-max",
            ),
            DownloadedChunkPayload(
                gcs_uri="gs://fake-bucket/ab12/feed-max/2026-03-06/160-aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa.flac",
                chunk_data=mock_download(
                    "gs://fake-bucket/ab12/feed-max/2026-03-06/160-aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa.flac"
                ),
                feed_metadata=FeedMetadata(feed_name="test-feed"),
                feed_id="feed-max",
            ),
        ]

        element = ("session-A", chunks)
        fn = StatelessStitchAudioFn(config=config)
        results = list(fn.process(element))

        self.assertEqual(
            len(results),
            3,
            f"Expected 3 flush requests, got {len(results)}: {results}",
        )
        results.sort(key=lambda x: x[1].time_range.start_ms)

        # First element: chunks 100, 115
        self.assertTrue(
            any(
                "77777777-7777-7777-7777-777777777777" in u
                for u in results[0][1].contributing_audio_uris
            )
        )
        self.assertTrue(
            any(
                "88888888-8888-8888-8888-888888888888" in u
                for u in results[0][1].contributing_audio_uris
            )
        )
        self.assertFalse(results[0][1].missing_prior_context)

        # Second element: chunk 130
        self.assertTrue(
            any(
                "99999999-9999-9999-9999-999999999999" in u
                for u in results[1][1].contributing_audio_uris
            )
        )
        self.assertTrue(results[1][1].missing_prior_context)

        # Third element: chunk 160 (force flush)
        self.assertTrue(
            any(
                "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa" in u
                for u in results[2][1].contributing_audio_uris
            )
        )
        self.assertTrue(results[2][1].missing_prior_context)


class TranscribeAudioTest(unittest.TestCase):
    @patch("backend.pipeline.transcription.stitcher.get_transcriber")
    @patch("backend.pipeline.transcription.stitcher.AudioProcessor")
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
                            buffer=np.zeros(((500) * 16), dtype=np.int16),
                            contributing_audio_uris=["gs://f/11111111.flac"],
                            time_range=TimeRange(
                                start_ms=101000, end_ms=101500
                            ),
                            transmission_id="test-uuid",
                            feed_metadata=FeedMetadata(
                                feed_name="Test Feed Name"
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

            def assert_dlq(elements: list[dict[str, Any]]) -> None:

                assert len(elements) == 1
                assert "Transcription API outage!" in elements[0]["error"]

            def assert_empty(elements):
                assert len(elements) == 0

            assert_that(results.main, assert_empty, label="CheckEmptyMain")

            assert_that(
                results[DEAD_LETTER_QUEUE_TAG], assert_dlq, label="CheckDLQ"
            )


class DownloadAudioTest(unittest.TestCase):
    @patch("backend.pipeline.transcription.transforms.AudioProcessor")
    def test_download_audio_timestamp_injection(
        self, mock_audio_processor: MagicMock
    ) -> None:
        """Verifies that DownloadAudioFn can be processed natively by Apache Beam without _DoFnParam injection errors."""
        mock_inst = mock_audio_processor.return_value
        mock_inst.download_audio_and_detect.return_value = AudioChunkData(
            start_ms=100000,
            audio=np.zeros(((1000) * 16), dtype=np.int16),
            speech_segments=[],
            gcs_uri="gs://fake-bucket/100-11111111.flac",
        )

        config = get_test_stitch_config()
        options = PipelineOptions(
            flags=["--input_subscription=a", "--output_topic=b", "--project=c"]
        )

        with BeamTestPipeline(options=options) as p:
            elements = (
                p
                | beam.Create(
                    [
                        (
                            "feed-123",
                            ChunkMetadata(
                                gcs_uri="gs://fake-bucket/100-11111111.flac",
                                session_id="session-A",
                                duration_ms=15000,
                                feed_id="feed-123",
                                timestamp_ms=100000,
                                feed_metadata=FeedMetadata(
                                    feed_name="Test Feed Name"
                                ),
                            ),
                        )
                    ]
                ).with_output_types(tuple[str, ChunkMetadata])
                | beam.Map(lambda x: TimestampedValue(x, 100))
            )

            results = elements | beam.ParDo(DownloadAudioFn(config))

            expected_audio = (
                mock_inst.download_audio_and_detect.return_value.audio
            )

            def assert_results(elements):
                assert len(elements) == 1
                feed_id, payload = elements[0]
                assert feed_id == "feed-123"
                assert payload.gcs_uri == "gs://fake-bucket/100-11111111.flac"
                assert payload.chunk_data.start_ms == 100000
                assert np.array_equal(payload.chunk_data.audio, expected_audio)

            assert_that(results, assert_results)


class SerializeAndEnrichTest(unittest.TestCase):
    def test_serialize_and_enrich(self) -> None:
        """Verifies that SerializeAndEnrichFn correctly stores feed_name and enriches the transcript."""
        options = PipelineOptions(
            flags=["--input_subscription=a", "--output_topic=b", "--project=c"]
        )

        with BeamTestPipeline(options=options) as p:
            res1 = TranscriptionResult(
                feed_id="test-feed",
                contributing_audio_uris=["gs://bucket/1.flac"],
                transcript="Hello world",
                time_range=TimeRange(1000, 2000),
                transmission_id="uuid-1",
                feed_metadata=FeedMetadata(feed_name="Test Feed Name"),
                start_audio_offset_ms=100,
                end_audio_offset_ms=200,
            )

            res2 = TranscriptionResult(
                feed_id="test-feed",
                contributing_audio_uris=["gs://bucket/2.flac"],
                transcript="Hello world again",
                time_range=TimeRange(1000, 3000),
                transmission_id="uuid-2",
                feed_metadata=FeedMetadata(feed_name="Test Feed Name"),
                start_audio_offset_ms=100,
                end_audio_offset_ms=200,
            )

            elements = [
                res1,
                res2,
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

                assert protos[1].transcript == "Hello world again"
                assert protos[1].feed_name == "Test Feed Name"

            assert_that(results, assert_results)


class StatelessStitchAudioTest(unittest.TestCase):
    @patch("backend.pipeline.transcription.stitcher.AudioProcessor")
    def test_stateless_stitching(self, mock_audio_processor: MagicMock) -> None:
        """Verifies that StatelessStitchAudioFn correctly stitches chunks."""
        mock_processor_inst = mock_audio_processor.return_value
        mock_processor_inst.check_vad.return_value = True
        mock_processor_inst.preprocess_audio.side_effect = lambda x: x
        mock_processor_inst.export_flac.return_value = b"flac_bytes"

        config = get_test_stitch_config(significant_gap_ms=3000)

        chunks = [
            DownloadedChunkPayload(
                gcs_uri="gs://b/100.flac",
                chunk_data=AudioChunkData(
                    start_ms=100000,
                    audio=np.zeros(((15000) * 16), dtype=np.int16),
                    speech_segments=[TimeRange(12500, 15000)],
                    gcs_uri="gs://b/100.flac",
                    duration_ms=15000,
                ),
                feed_metadata=FeedMetadata(feed_name="test-feed"),
                feed_id="feed-123",
            ),
            DownloadedChunkPayload(
                gcs_uri="gs://b/115.flac",
                chunk_data=AudioChunkData(
                    start_ms=115000,
                    audio=np.zeros(((15000) * 16), dtype=np.int16),
                    speech_segments=[TimeRange(0, 2500)],
                    gcs_uri="gs://b/115.flac",
                    duration_ms=15000,
                ),
                feed_metadata=FeedMetadata(feed_name="test-feed"),
                feed_id="feed-123",
            ),
        ]

        element = ("session-A", chunks)

        fn = StatelessStitchAudioFn(config=config)
        result = list(fn.process(element))

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0][0], "feed-123")
        self.assertIsInstance(result[0][1], FlushRequest)
