from __future__ import annotations

import unittest

from backend.pipeline.detection.sidecar_builder import SidecarBuilder
from backend.pipeline.detection.types import CombinedResult, SpeechRegion


class TestSidecarBuilder(unittest.TestCase):
    """Tests for SidecarBuilder.build()."""

    def test_build_empty_combined_result(self) -> None:
        combined = CombinedResult(speech_regions=())
        sidecar = SidecarBuilder.build(combined)

        self.assertEqual(len(sidecar.sound_events), 0)
        self.assertEqual(sidecar.source_chunk_id, "")

    def test_build_single_region(self) -> None:
        combined = CombinedResult(
            speech_regions=(
                SpeechRegion(start_sec=1.5, end_sec=3.0, detector_type="test"),
            ),
        )
        sidecar = SidecarBuilder.build(
            combined, source_chunk_id="gs://bucket/audio.flac"
        )

        self.assertEqual(len(sidecar.sound_events), 1)
        event = sidecar.sound_events[0]
        self.assertEqual(event.start_time.seconds, 1)
        self.assertEqual(event.start_time.nanos, 500_000_000)
        self.assertEqual(event.duration.seconds, 1)
        self.assertEqual(event.duration.nanos, 500_000_000)
        self.assertEqual(sidecar.source_chunk_id, "gs://bucket/audio.flac")

    def test_build_multiple_regions(self) -> None:
        combined = CombinedResult(
            speech_regions=(
                SpeechRegion(start_sec=0.0, end_sec=1.0, detector_type="a"),
                SpeechRegion(start_sec=5.0, end_sec=7.5, detector_type="b"),
            ),
        )
        sidecar = SidecarBuilder.build(combined)

        self.assertEqual(len(sidecar.sound_events), 2)

        first = sidecar.sound_events[0]
        self.assertEqual(first.start_time.seconds, 0)
        self.assertEqual(first.start_time.nanos, 0)
        self.assertEqual(first.duration.seconds, 1)
        self.assertEqual(first.duration.nanos, 0)

        second = sidecar.sound_events[1]
        self.assertEqual(second.start_time.seconds, 5)
        self.assertEqual(second.start_time.nanos, 0)
        self.assertEqual(second.duration.seconds, 2)
        self.assertEqual(second.duration.nanos, 500_000_000)

    def test_build_source_chunk_id_full_gcs_uri(self) -> None:
        combined = CombinedResult(speech_regions=())
        uri = "gs://ingestion_canonical_bucket/bcfy_feeds/abc/20260305T120000Z_0.flac"
        sidecar = SidecarBuilder.build(combined, source_chunk_id=uri)

        self.assertEqual(sidecar.source_chunk_id, uri)

    def test_build_fractional_seconds_precision(self) -> None:
        combined = CombinedResult(
            speech_regions=(
                SpeechRegion(start_sec=0.123, end_sec=0.456, detector_type="test"),
            ),
        )
        sidecar = SidecarBuilder.build(combined)

        event = sidecar.sound_events[0]
        self.assertEqual(event.start_time.seconds, 0)
        self.assertAlmostEqual(event.start_time.nanos, 123_000_000, delta=1)

        expected_duration_nanos = 333_000_000
        self.assertEqual(event.duration.seconds, 0)
        self.assertAlmostEqual(event.duration.nanos, expected_duration_nanos, delta=1)

    def test_build_zero_length_region(self) -> None:
        combined = CombinedResult(
            speech_regions=(
                SpeechRegion(start_sec=2.0, end_sec=2.0, detector_type="test"),
            ),
        )
        sidecar = SidecarBuilder.build(combined)

        event = sidecar.sound_events[0]
        self.assertEqual(event.start_time.seconds, 2)
        self.assertEqual(event.start_time.nanos, 0)
        self.assertEqual(event.duration.seconds, 0)
        self.assertEqual(event.duration.nanos, 0)


if __name__ == "__main__":
    unittest.main()
