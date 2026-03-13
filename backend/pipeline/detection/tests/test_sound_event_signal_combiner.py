import unittest

from backend.pipeline.detection.sound_event_signal_combiner import (
    SoundEventSignalCombiner,
)
from backend.pipeline.detection.types import DetectionResult, SpeechRegion


def _region(start, end, detector_type="vad"):
    return SpeechRegion(
        start_sec=start,
        end_sec=end,
        detector_type=detector_type,
    )


def _result(regions, detector_type="vad"):
    return DetectionResult(
        speech_regions=tuple(regions),
        detector_type=detector_type,
    )


class TestCombineEmpty(unittest.TestCase):
    """Tests for combine with empty inputs."""

    def setUp(self) -> None:
        self.combiner = SoundEventSignalCombiner()

    def test_empty_results_list(self) -> None:
        combined = self.combiner.combine([])
        self.assertEqual(combined.speech_regions, ())

    def test_results_with_no_regions(self) -> None:
        result = _result([], detector_type="vad")
        combined = self.combiner.combine([result])
        self.assertEqual(combined.speech_regions, ())


class TestCombineSingleDetector(unittest.TestCase):
    """Tests for combine with regions from a single detector."""

    def setUp(self) -> None:
        self.combiner = SoundEventSignalCombiner()

    def test_single_result_passthrough(self) -> None:
        result = _result(
            [
                _region(0.0, 1.0),
                _region(2.0, 3.0),
            ]
        )
        combined = self.combiner.combine([result])
        self.assertEqual(len(combined.speech_regions), 2)
        self.assertEqual(combined.speech_regions[0].start_sec, 0.0)
        self.assertEqual(combined.speech_regions[0].end_sec, 1.0)
        self.assertEqual(combined.speech_regions[1].start_sec, 2.0)
        self.assertEqual(combined.speech_regions[1].end_sec, 3.0)

    def test_overlapping_regions_merge(self) -> None:
        result = _result(
            [
                _region(0.0, 1.0),
                _region(0.5, 1.5),
            ]
        )
        combined = self.combiner.combine([result])
        self.assertEqual(len(combined.speech_regions), 1)
        self.assertEqual(combined.speech_regions[0].start_sec, 0.0)
        self.assertEqual(combined.speech_regions[0].end_sec, 1.5)

    def test_non_overlapping_stay_separate(self) -> None:
        result = _result(
            [
                _region(0.0, 1.0),
                _region(1.5, 2.5),
            ]
        )
        combined = self.combiner.combine([result])
        self.assertEqual(len(combined.speech_regions), 2)


class TestCombineMultipleDetectors(unittest.TestCase):
    """Tests for combine with regions from multiple detectors."""

    def setUp(self) -> None:
        self.combiner = SoundEventSignalCombiner()

    def test_non_overlapping_from_different_detectors(self) -> None:
        r1 = _result([_region(0.0, 1.0, detector_type="vad")], detector_type="vad")
        r2 = _result(
            [_region(2.0, 3.0, detector_type="energy")], detector_type="energy"
        )
        combined = self.combiner.combine([r1, r2])
        self.assertEqual(len(combined.speech_regions), 2)
        self.assertEqual(combined.speech_regions[0].detector_type, "vad")
        self.assertEqual(combined.speech_regions[1].detector_type, "energy")

    def test_overlapping_from_different_detectors_merge(self) -> None:
        r1 = _result(
            [_region(0.0, 1.0, detector_type="vad")],
            detector_type="vad",
        )
        r2 = _result(
            [_region(0.5, 1.5, detector_type="energy")],
            detector_type="energy",
        )
        combined = self.combiner.combine([r1, r2])
        self.assertEqual(len(combined.speech_regions), 1)
        region = combined.speech_regions[0]
        self.assertEqual(region.start_sec, 0.0)
        self.assertEqual(region.end_sec, 1.5)
        self.assertEqual(region.detector_type, "energy,vad")

    def test_chain_merge(self) -> None:
        """Three overlapping regions from different detectors chain-merge."""
        r1 = _result([_region(0.0, 1.0, detector_type="a")], detector_type="a")
        r2 = _result([_region(0.9, 2.0, detector_type="b")], detector_type="b")
        r3 = _result([_region(1.9, 3.0, detector_type="c")], detector_type="c")
        combined = self.combiner.combine([r1, r2, r3])
        self.assertEqual(len(combined.speech_regions), 1)
        self.assertEqual(combined.speech_regions[0].start_sec, 0.0)
        self.assertEqual(combined.speech_regions[0].end_sec, 3.0)

    def test_detector_type_comma_joined_sorted(self) -> None:
        r1 = _result([_region(0.0, 1.0, detector_type="zebra")], detector_type="zebra")
        r2 = _result([_region(0.5, 1.5, detector_type="alpha")], detector_type="alpha")
        r3 = _result([_region(0.8, 1.2, detector_type="mid")], detector_type="mid")
        combined = self.combiner.combine([r1, r2, r3])
        self.assertEqual(combined.speech_regions[0].detector_type, "alpha,mid,zebra")


class TestCombineEdgeCases(unittest.TestCase):
    """Edge case tests for the combine method."""

    def setUp(self) -> None:
        self.combiner = SoundEventSignalCombiner()

    def test_identical_regions_merge(self) -> None:
        r1 = _result([_region(0.0, 1.0, detector_type="a")], detector_type="a")
        r2 = _result([_region(0.0, 1.0, detector_type="b")], detector_type="b")
        combined = self.combiner.combine([r1, r2])
        self.assertEqual(len(combined.speech_regions), 1)
        self.assertEqual(combined.speech_regions[0].detector_type, "a,b")

    def test_contained_region(self) -> None:
        """A region fully contained in another merges into the outer."""
        r1 = _result([_region(0.0, 3.0, detector_type="a")], detector_type="a")
        r2 = _result([_region(1.0, 2.0, detector_type="b")], detector_type="b")
        combined = self.combiner.combine([r1, r2])
        self.assertEqual(len(combined.speech_regions), 1)
        self.assertEqual(combined.speech_regions[0].start_sec, 0.0)
        self.assertEqual(combined.speech_regions[0].end_sec, 3.0)

    def test_touching_regions_merge(self) -> None:
        """Regions that touch exactly (start == end) merge."""
        r1 = _result([_region(0.0, 1.0, detector_type="a")], detector_type="a")
        r2 = _result([_region(1.0, 2.0, detector_type="b")], detector_type="b")
        combined = self.combiner.combine([r1, r2])
        self.assertEqual(len(combined.speech_regions), 1)
        self.assertEqual(combined.speech_regions[0].start_sec, 0.0)
        self.assertEqual(combined.speech_regions[0].end_sec, 2.0)

    def test_gap_does_not_merge(self) -> None:
        """Regions with any positive gap do not merge."""
        r1 = _result([_region(0.0, 1.0, detector_type="a")], detector_type="a")
        r2 = _result([_region(1.01, 2.0, detector_type="b")], detector_type="b")
        combined = self.combiner.combine([r1, r2])
        self.assertEqual(len(combined.speech_regions), 2)


if __name__ == "__main__":
    unittest.main()
