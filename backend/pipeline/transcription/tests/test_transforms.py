"""Tests for the StitchAudioFn, TranscribeAudioFn, and related transformations."""

import threading
import time
import unittest
from typing import Any
from unittest.mock import MagicMock, patch

import apache_beam as beam
from apache_beam.io.gcp.pubsub import PubsubMessage
from apache_beam.options.pipeline_options import (
    PipelineOptions,
    StandardOptions,
)
from apache_beam.testing.test_pipeline import TestPipeline as BeamTestPipeline
from apache_beam.testing.test_stream import TestStream as BeamTestStream
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.transforms.window import TimestampedValue
from pydub import AudioSegment

from backend.pipeline.schema_types.raw_audio_chunk_pb2 import AudioChunk
from backend.pipeline.transcription.constants import DEAD_LETTER_QUEUE_TAG
from backend.pipeline.transcription.datatypes import (
    AudioChunkData,
    FlushRequest,
    OrderRestorerConfig,
    StitchAudioConfig,
    TimeRange,
    TranscribeAudioConfig,
)
from backend.pipeline.transcription.enums import TranscriberType, VadType
from backend.pipeline.transcription.stitcher import (
    StitchAudioFn,
    TranscribeAudioFn,
)
from backend.pipeline.transcription.transcribers import Transcriber
from backend.pipeline.transcription.transforms import (
    AddEventTimestamp,
    ParseAndKeyFn,
    RestoreOrderFn,
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
        "metrics_exporter_type": "",
        "metrics_config": "{}",
        "significant_gap_ms": 500,
        "stale_timeout_ms": 60000,
        "max_transmission_duration_ms": 600000,
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
        "metrics_exporter_type": "",
        "metrics_config": "{}",
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
        """Verifies that incoming data missing a critical routing attribute like 'feed_id' is gracefully intercepted and routed to the Dead Letter Queue."""
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
                "test-feed",
                (
                    "gs://bucket/hash/feed_id/YYYY-MM-DD/1678886400-bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb.flac",
                    "mock-session-id",
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


class OrderRestorerTest(unittest.TestCase):
    def test_restore_order_buffers_and_releases(self) -> None:
        """Verifies that structurally disordered data streams correctly buffer elements in-memory to artificially re-align and emit chronologically."""
        config = OrderRestorerConfig(out_of_order_timeout_ms=5000)

        with BeamTestPipeline() as p:
            # Emit chunk 1, then chunk 3. Chunk 3 should be buffered.
            # Then emit chunk 2. Chunk 2 and 3 should be released.
            test_stream = (
                BeamTestStream(
                    coder=beam.coders.TupleCoder(
                        (
                            beam.coders.StrUtf8Coder(),
                            beam.coders.TupleCoder(
                                (
                                    beam.coders.StrUtf8Coder(),
                                    beam.coders.StrUtf8Coder(),
                                )
                            ),
                        )
                    )
                )
                .advance_watermark_to(100)
                .add_elements(
                    [
                        TimestampedValue(
                            ("feed-1", ("gs://b/100-uuid1.flac", "session-A")),
                            100,
                        )
                    ]
                )
                .advance_watermark_to(130)
                .add_elements(
                    [
                        TimestampedValue(
                            ("feed-1", ("gs://b/130-uuid3.flac", "session-A")),
                            130,
                        )
                    ]
                )
                .advance_watermark_to(140)
                .add_elements(
                    [
                        TimestampedValue(
                            ("feed-1", ("gs://b/115-uuid2.flac", "session-A")),
                            115,
                        )
                    ]
                )
                .advance_watermark_to_infinity()
            )

            restored = p | test_stream | beam.ParDo(RestoreOrderFn(config))

            assert_that(
                restored,
                equal_to(
                    [
                        ("feed-1", "gs://b/100-uuid1.flac"),
                        ("feed-1", "gs://b/115-uuid2.flac"),
                        ("feed-1", "gs://b/130-uuid3.flac"),
                    ]
                ),
            )

    def test_restore_order_yields_late_chunks(self) -> None:
        """Verifies that profoundly late stream elements exceeding the operational buffering timeout are explicitly yielded independently without corrupting stream progression."""
        config = OrderRestorerConfig(out_of_order_timeout_ms=5000)

        with BeamTestPipeline() as p:
            test_stream = (
                BeamTestStream(
                    coder=beam.coders.TupleCoder(
                        (
                            beam.coders.StrUtf8Coder(),
                            beam.coders.TupleCoder(
                                (
                                    beam.coders.StrUtf8Coder(),
                                    beam.coders.StrUtf8Coder(),
                                )
                            ),
                        )
                    )
                )
                .advance_watermark_to(100)
                .add_elements(
                    [
                        TimestampedValue(
                            (
                                "feed-1",
                                ("gs://b/100-11111111.flac", "session-A"),
                            ),
                            100,
                        )
                    ]
                )
                .advance_watermark_to(130)
                .add_elements(
                    [
                        TimestampedValue(
                            (
                                "feed-1",
                                ("gs://b/130-33333333.flac", "session-A"),
                            ),
                            130,
                        )
                    ]
                )
                # After 5 seconds (5000ms), the order restorer timed out waiting for 115.
                .advance_watermark_to(10000)
                # Now the late chunk arrives
                .add_elements(
                    [
                        TimestampedValue(
                            (
                                "feed-1",
                                ("gs://b/115-22222222.flac", "session-A"),
                            ),
                            115,
                        )
                    ]
                )
                .advance_watermark_to_infinity()
            )

            restored = p | test_stream | beam.ParDo(RestoreOrderFn(config))

            # The current behavior explicitly yields it out of order downstream
            assert_that(
                restored,
                equal_to(
                    [
                        ("feed-1", "gs://b/100-11111111.flac"),
                        ("feed-1", "gs://b/130-33333333.flac"),
                        ("feed-1", "gs://b/115-22222222.flac"),
                    ]
                ),
            )

    @unittest.skip("DirectRunner metrics validation is flaky")
    def test_delayed_gap_acceptance(self) -> None:
        pass


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

        def mock_download(path: str) -> AudioChunkData:

            filename = path.rsplit("/", maxsplit=1)[-1]
            chunk_start = (
                float(filename.split("-")[0]) if "-" in filename else 0.0
            )

            duration_s = 20.0
            if filename.startswith(("100-", "115-")):
                duration_s = 15.0
            elif filename.startswith("150-"):
                duration_s = 10.0
            elif filename.startswith("160-"):
                duration_s = 30.0

            return AudioChunkData(
                start_ms=int(chunk_start * 1000),
                audio=AudioSegment.silent(duration=int(duration_s * 1000)),
                speech_segments=[
                    TimeRange(int(s * 1000), int(e * 1000))
                    for s, e in sed_map.get(filename, [])
                ],
                gcs_uri=path,
            )

        mock_processor_inst.download_audio_and_sed.side_effect = mock_download

        options = PipelineOptions()
        options.view_as(StandardOptions).streaming = True

        config = get_test_stitch_config(significant_gap_ms=3000)

        with BeamTestPipeline(options=options) as p:
            test_stream = (
                BeamTestStream(
                    coder=beam.coders.TupleCoder(
                        (
                            beam.coders.StrUtf8Coder(),
                            beam.coders.TupleCoder(
                                (
                                    beam.coders.StrUtf8Coder(),
                                    beam.coders.PickleCoder(),
                                )
                            ),
                        )
                    )
                )
                .advance_watermark_to(100)
                .add_elements(
                    [
                        TimestampedValue(
                            (
                                "feed-123",
                                (
                                    "gs://fake-bucket/ab12/feed-123/2026-03-06/100-11111111-1111-1111-1111-111111111111.flac",
                                    mock_download(
                                        "gs://fake-bucket/ab12/feed-123/2026-03-06/100-11111111-1111-1111-1111-111111111111.flac"
                                    ),
                                ),
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
                                (
                                    "gs://fake-bucket/ab12/feed-123/2026-03-06/115-22222222-2222-2222-2222-222222222222.flac",
                                    mock_download(
                                        "gs://fake-bucket/ab12/feed-123/2026-03-06/115-22222222-2222-2222-2222-222222222222.flac"
                                    ),
                                ),
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
                                (
                                    "gs://fake-bucket/ab12/feed-123/2026-03-06/130-33333333-3333-3333-3333-333333333333.flac",
                                    mock_download(
                                        "gs://fake-bucket/ab12/feed-123/2026-03-06/130-33333333-3333-3333-3333-333333333333.flac"
                                    ),
                                ),
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
                                (
                                    "gs://fake-bucket/ab12/feed-123/2026-03-06/150-44444444-4444-4444-4444-444444444444.flac",
                                    mock_download(
                                        "gs://fake-bucket/ab12/feed-123/2026-03-06/150-44444444-4444-4444-4444-444444444444.flac"
                                    ),
                                ),
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
                                (
                                    "gs://fake-bucket/ab12/feed-123/2026-03-06/160-55555555-5555-5555-5555-555555555555.flac",
                                    mock_download(
                                        "gs://fake-bucket/ab12/feed-123/2026-03-06/160-55555555-5555-5555-5555-555555555555.flac"
                                    ),
                                ),
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
                                (
                                    "gs://fake-bucket/ab12/feed-123/2026-03-06/190-66666666-6666-6666-6666-666666666666.flac",
                                    mock_download(
                                        "gs://fake-bucket/ab12/feed-123/2026-03-06/190-66666666-6666-6666-6666-666666666666.flac"
                                    ),
                                ),
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
                    StitchAudioFn(
                        config=config,
                    )
                ).with_outputs(DEAD_LETTER_QUEUE_TAG, main="main")
            )

            def debug_print(el):

                return el

            results.main = results.main | beam.Map(debug_print)

            def print_dlq(el):

                print(f"DLQ MESSAGE: {el}")  # noqa: T201
                return el

            results[DEAD_LETTER_QUEUE_TAG] | "Print DLQ" >> beam.Map(print_dlq)

            def assert_flush_requests(
                elements: list[tuple[str, FlushRequest]],
            ) -> None:

                assert len(elements) == 3, (
                    f"Expected 3 flush requests, got {len(elements)}: {elements}"
                )

                # Sort elements by start time to make assertions deterministic
                elements.sort(key=lambda x: x[1].time_range.start_ms)

                # First element: chunks 100, 115
                assert any(
                    "11111111-1111-1111-1111-111111111111" in u
                    for u in elements[0][1].contributing_audio_uris
                )
                assert any(
                    "22222222-2222-2222-2222-222222222222" in u
                    for u in elements[0][1].contributing_audio_uris
                )
                assert elements[0][1].missing_prior_context is False

                # Second element: chunk 130
                # DID follow a gap (chunk 115 speech ended at 122, 130 starts at 130, gap=8s >= 3s)
                assert any(
                    "33333333-3333-3333-3333-333333333333" in u
                    for u in elements[1][1].contributing_audio_uris
                )
                assert elements[1][1].missing_prior_context is False

                # Third element: chunk 160 (chunk 150 was dropped as useless silence after a disconnected gap)
                assert any(
                    "55555555-5555-5555-5555-555555555555" in u
                    for u in elements[2][1].contributing_audio_uris
                )
                assert elements[2][1].missing_prior_context is True

            assert_that(
                results.main,
                assert_flush_requests,
                label="CheckMainFlushRequests",
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

        def mock_download(path: str) -> AudioChunkData:

            filename = path.rsplit("/", maxsplit=1)[-1]
            chunk_start = (
                float(filename.split("-")[0]) if "-" in filename else 0.0
            )
            return AudioChunkData(
                start_ms=int(chunk_start * 1000),
                audio=AudioSegment.silent(duration=15000),
                speech_segments=[
                    TimeRange(int(s * 1000), int(e * 1000))
                    for s, e in sed_map.get(filename, [])
                ],
                gcs_uri=path,
            )

        mock_processor_inst.download_audio_and_sed.side_effect = mock_download
        options = PipelineOptions()
        options.view_as(StandardOptions).streaming = True
        config = get_test_stitch_config(significant_gap_ms=3000)

        with BeamTestPipeline(options=options) as p:
            test_stream = (
                BeamTestStream(
                    coder=beam.coders.TupleCoder(
                        (
                            beam.coders.StrUtf8Coder(),
                            beam.coders.TupleCoder(
                                (
                                    beam.coders.StrUtf8Coder(),
                                    beam.coders.PickleCoder(),
                                )
                            ),
                        )
                    )
                )
                .advance_watermark_to(100)
                .add_elements(
                    [
                        TimestampedValue(
                            (
                                "feed-123",
                                (
                                    "gs://fake-bucket/100-11111111-1111-1111-1111-111111111111.flac",
                                    mock_download(
                                        "gs://fake-bucket/100-11111111-1111-1111-1111-111111111111.flac"
                                    ),
                                ),
                            ),
                            100,
                        )
                    ]
                )
                .advance_watermark_to(130)
                .add_elements(
                    [
                        TimestampedValue(
                            (
                                "feed-123",
                                (
                                    "gs://fake-bucket/130-33333333-3333-3333-3333-333333333333.flac",
                                    mock_download(
                                        "gs://fake-bucket/130-33333333-3333-3333-3333-333333333333.flac"
                                    ),
                                ),
                            ),
                            130,
                        )
                    ]
                )
                .advance_watermark_to(140)
                .add_elements(
                    [
                        TimestampedValue(
                            (
                                "feed-123",
                                (
                                    "gs://fake-bucket/115-22222222-2222-2222-2222-222222222222.flac",
                                    mock_download(
                                        "gs://fake-bucket/115-22222222-2222-2222-2222-222222222222.flac"
                                    ),
                                ),
                            ),
                            140,
                        )
                    ]
                )
                .advance_watermark_to_infinity()
            )

            results = (
                p
                | test_stream
                | beam.ParDo(StitchAudioFn(config=config)).with_outputs(
                    DEAD_LETTER_QUEUE_TAG, main="main"
                )
            )

            def assert_flush_requests(
                elements: list[tuple[str, FlushRequest]],
            ) -> None:

                assert len(elements) == 2, (
                    f"Expected 2 flush requests (Chunk 3 remains buffered due to skipped timer), got {len(elements)}"
                )

                elements.sort(key=lambda x: x[1].time_range.start_ms)

                assert any(
                    "11111111-1111-1111-1111-111111111111" in u
                    for u in elements[0][1].contributing_audio_uris
                )
                assert elements[0][1].missing_post_context is True

                assert any(
                    "22222222-2222-2222-2222-222222222222" in u
                    for u in elements[1][1].contributing_audio_uris
                )
                assert elements[1][1].missing_prior_context is False
                assert elements[1][1].missing_post_context is True

            assert_that(
                results.main,
                assert_flush_requests,
                label="CheckLateChunkRequests",
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

        def mock_download(path: str) -> AudioChunkData:

            filename = path.rsplit("/", maxsplit=1)[-1]
            chunk_start = (
                float(filename.split("-")[0]) if "-" in filename else 0.0
            )
            return AudioChunkData(
                start_ms=int(chunk_start * 1000),
                audio=AudioSegment.silent(duration=15000),
                speech_segments=[
                    TimeRange(int(s * 1000), int(e * 1000))
                    for s, e in sed_map.get(filename, [])
                ],
                gcs_uri=path,
            )

        mock_processor_inst.download_audio_and_sed.side_effect = mock_download

        options = PipelineOptions()
        options.view_as(StandardOptions).streaming = True

        # Set max duration to 30 seconds (2 full chunks).
        config = get_test_stitch_config(
            max_transmission_duration_ms=30000, significant_gap_ms=29999
        )

        with BeamTestPipeline(options=options) as p:
            test_stream = (
                BeamTestStream(
                    coder=beam.coders.TupleCoder(
                        (
                            beam.coders.StrUtf8Coder(),
                            beam.coders.TupleCoder(
                                (
                                    beam.coders.StrUtf8Coder(),
                                    beam.coders.PickleCoder(),
                                )
                            ),
                        )
                    )
                )
                .advance_watermark_to(100)
                .add_elements(
                    [
                        TimestampedValue(
                            (
                                "feed-max",
                                (
                                    "gs://fake-bucket/ab12/feed-max/2026-03-06/100-77777777-7777-7777-7777-777777777777.flac",
                                    mock_download(
                                        "gs://fake-bucket/ab12/feed-max/2026-03-06/100-77777777-7777-7777-7777-777777777777.flac"
                                    ),
                                ),
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
                                "feed-max",
                                (
                                    "gs://fake-bucket/ab12/feed-max/2026-03-06/115-88888888-8888-8888-8888-888888888888.flac",
                                    mock_download(
                                        "gs://fake-bucket/ab12/feed-max/2026-03-06/115-88888888-8888-8888-8888-888888888888.flac"
                                    ),
                                ),
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
                                "feed-max",
                                (
                                    "gs://fake-bucket/ab12/feed-max/2026-03-06/130-99999999-9999-9999-9999-999999999999.flac",
                                    mock_download(
                                        "gs://fake-bucket/ab12/feed-max/2026-03-06/130-99999999-9999-9999-9999-999999999999.flac"
                                    ),
                                ),
                            ),
                            130,
                        )
                    ]
                )
                .advance_watermark_to(160)
                .add_elements(
                    [
                        TimestampedValue(
                            (
                                "feed-max",
                                (
                                    "gs://fake-bucket/ab12/feed-max/2026-03-06/160-aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa.flac",
                                    mock_download(
                                        "gs://fake-bucket/ab12/feed-max/2026-03-06/160-aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa.flac"
                                    ),
                                ),
                            ),
                            160,
                        )
                    ]
                )
                .advance_watermark_to_infinity()
            )

            results = (
                p
                | test_stream
                | beam.ParDo(
                    StitchAudioFn(
                        config=config,
                    )
                ).with_outputs(DEAD_LETTER_QUEUE_TAG, main="main")
            )

            def assert_flush_requests(
                elements: list[tuple[str, FlushRequest]],
            ) -> None:

                assert len(elements) == 2, (
                    f"Expected 2 flush requests, got {len(elements)}: {elements}"
                )
                elements.sort(key=lambda x: x[1].time_range.start_ms)

                # First chunk reached max duration limit without a gap
                assert any(
                    "77777777-7777-7777-7777-777777777777" in u
                    for u in elements[0][1].contributing_audio_uris
                )
                assert any(
                    "88888888-8888-8888-8888-888888888888" in u
                    for u in elements[0][1].contributing_audio_uris
                )
                assert elements[0][1].missing_prior_context is False

                # Second chunk is immediately following the forced cut off -> missing context IS true (severed head inherited)
                assert any(
                    "99999999-9999-9999-9999-999999999999" in u
                    for u in elements[1][1].contributing_audio_uris
                )
                assert elements[1][1].missing_prior_context is True

            assert_that(
                results.main,
                assert_flush_requests,
                label="CheckMaxDurationFlushRequests",
            )

    @unittest.skip(
        "DirectRunner timer advancing is buggy and unsupported natively."
    )
    @patch("backend.pipeline.transcription.stitcher.time")
    @patch("backend.pipeline.transcription.stitcher.AudioProcessor")
    def test_stale_transmission_timer(
        self,
        mock_audio_processor: MagicMock,
        mock_time: MagicMock,
    ) -> None:
        """Verifies that incomplete segments remaining open abruptly dispatch once an explicit processing loop exceeds the permissible timeout."""
        mock_time.time.return_value = 0
        mock_processor_inst = mock_audio_processor.return_value
        mock_processor_inst.check_vad.return_value = True
        mock_processor_inst.preprocess_audio.side_effect = lambda x: x
        mock_processor_inst.export_flac.return_value = b"flac_bytes"
        mock_processor_inst.download_audio_and_sed.return_value = AudioChunkData(
            start_ms=101000,
            audio=AudioSegment.silent(duration=20000),
            speech_segments=[TimeRange(12500, 15000)],
            gcs_uri="gs://fake-bucket/ab12/feed-123/2026-03-06/101-11111111-1111-1111-1111-111111111111.flac",
        )

        options = PipelineOptions()
        options.view_as(StandardOptions).streaming = True

        config = get_test_stitch_config()

        with BeamTestPipeline(options=options) as p:
            test_stream = (
                BeamTestStream(
                    coder=beam.coders.TupleCoder(
                        (
                            beam.coders.StrUtf8Coder(),
                            beam.coders.TupleCoder(
                                (
                                    beam.coders.StrUtf8Coder(),
                                    beam.coders.PickleCoder(),
                                )
                            ),
                        )
                    )
                )
                .advance_watermark_to(0)
                .add_elements(
                    [
                        TimestampedValue(
                            (
                                "feed-123",
                                (
                                    "gs://fake-bucket/ab12/feed-123/2026-03-06/101-11111111-1111-1111-1111-111111111111.flac",
                                    mock_processor_inst.download_audio_and_sed.return_value,
                                ),
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
                    StitchAudioFn(
                        config=config,
                    )
                ).with_outputs(DEAD_LETTER_QUEUE_TAG, main="main")
            )

            def assert_stale_match(
                elements: list[tuple[str, FlushRequest]],
            ) -> None:

                assert len(elements) == 1, (
                    f"Expected 1 element, got {len(elements)}"
                )
                _, res = elements[0]
                assert res.feed_id == "feed-123"
                assert any(
                    "11111111-1111-1111-1111-111111111111" in u
                    for u in res.contributing_audio_uris
                )

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

        config = get_test_stitch_config()

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
                        StitchAudioFn(
                            config=config,
                        )
                    ).with_outputs(DEAD_LETTER_QUEUE_TAG, main="main")
                )


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

        config = get_test_transcribe_config(route_to_dlq=True)

        with BeamTestPipeline() as p:
            elements = p | beam.Create(
                [
                    (
                        "feed-123",
                        FlushRequest(
                            feed_id="feed-123",
                            buffer=AudioSegment.silent(duration=500),
                            contributing_audio_uris=["gs://f/11111111.flac"],
                            time_range=TimeRange(
                                start_ms=101000, end_ms=101500
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

            assert_that(results.main, equal_to([]), label="CheckEmptyMain")
            assert_that(
                results[DEAD_LETTER_QUEUE_TAG], assert_dlq, label="CheckDLQ"
            )

    @unittest.skip(
        "DirectRunner background execution validation is unreliable."
    )
    @patch("backend.pipeline.transcription.stitcher.AudioProcessor")
    @patch("backend.pipeline.transcription.stitcher.get_transcriber")
    def test_concurrent_transcription_execution(
        self,
        mock_transcriber_factory: MagicMock,
        mock_audio_processor: MagicMock,
    ) -> None:
        """Verifies that the concurrent orchestrator splits individual grouped task lists into multi-threaded parallel transcription operations smoothly."""
        execution_threads = set()
        lock = threading.Lock()

        class MockTrackingTranscriber(Transcriber):
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
        def mock_download(path: str) -> AudioChunkData:

            filename = path.rsplit("/", maxsplit=1)[-1]
            chunk_start = (
                float(filename.split("-")[0]) if "-" in filename else 0.0
            )
            return AudioChunkData(
                start_ms=int(
                    chunk_start * 100000
                ),  # 100s apart to force flushes
                audio=AudioSegment.silent(duration=1000),
                speech_segments=[TimeRange(0, 1000)],
                gcs_uri=path,
            )

        mock_processor_inst.download_audio_and_sed.side_effect = mock_download

        main_thread_name = threading.current_thread().name

        options = PipelineOptions()
        options.view_as(StandardOptions).streaming = True

        config = get_test_transcribe_config()

        with BeamTestPipeline(options=options) as p:
            elements = p | beam.Create(
                [
                    FlushRequest(
                        feed_id="feed-123",
                        buffer=AudioSegment.silent(duration=500),
                        contributing_audio_uris=["gs://bbbbbbbb.flac"],
                        time_range=TimeRange(start_ms=0, end_ms=500),
                    ),
                    FlushRequest(
                        feed_id="feed-123",
                        buffer=AudioSegment.silent(duration=500),
                        contributing_audio_uris=["gs://cccccccc.flac"],
                        time_range=TimeRange(start_ms=5000, end_ms=5500),
                    ),
                ]
            )

            (
                elements
                | beam.ParDo(
                    TranscribeAudioFn(
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
