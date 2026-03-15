import unittest

from backend.pipeline.detection.types import (
    CombinedResult,
    DetectionResult,
    SpeechRegion,
)


class TestSpeechRegion(unittest.TestCase):
    """Tests for the SpeechRegion frozen dataclass."""

    def test_valid_construction(self) -> None:
        region = SpeechRegion(
            start_sec=1.0,
            end_sec=2.5,
            detector_type="silero_vad",
        )
        self.assertEqual(region.start_sec, 1.0)
        self.assertEqual(region.end_sec, 2.5)
        self.assertEqual(region.detector_type, "silero_vad")

    def test_frozen_immutability(self) -> None:
        region = SpeechRegion(start_sec=0.0, end_sec=1.0, detector_type="test")
        with self.assertRaises(AttributeError):
            region.end_sec = 2.0  # type: ignore[misc]

    def test_kw_only(self) -> None:
        with self.assertRaises(TypeError):
            SpeechRegion(0.0, 1.0, "test")  # type: ignore[misc]

    def test_equality(self) -> None:
        a = SpeechRegion(start_sec=0.0, end_sec=1.0, detector_type="test")
        b = SpeechRegion(start_sec=0.0, end_sec=1.0, detector_type="test")
        self.assertEqual(a, b)

    def test_negative_start_sec_raises(self) -> None:
        with self.assertRaises(ValueError) as ctx:
            SpeechRegion(start_sec=-0.1, end_sec=1.0, detector_type="test")
        self.assertIn("start_sec", str(ctx.exception))

    def test_end_before_start_raises(self) -> None:
        with self.assertRaises(ValueError) as ctx:
            SpeechRegion(start_sec=2.0, end_sec=1.0, detector_type="test")
        self.assertIn("end_sec", str(ctx.exception))

    def test_zero_length_region_is_valid(self) -> None:
        region = SpeechRegion(start_sec=1.0, end_sec=1.0, detector_type="test")
        self.assertEqual(region.start_sec, region.end_sec)

    def test_empty_detector_type_raises(self) -> None:
        with self.assertRaises(ValueError) as ctx:
            SpeechRegion(start_sec=0.0, end_sec=1.0, detector_type="")
        self.assertIn("detector_type", str(ctx.exception))


class TestDetectionResult(unittest.TestCase):
    """Tests for the DetectionResult frozen dataclass."""

    def test_valid_construction(self) -> None:
        region = SpeechRegion(start_sec=0.0, end_sec=1.0, detector_type="vad")
        result = DetectionResult(
            speech_regions=(region,),
            detector_type="vad",
        )
        self.assertEqual(len(result.speech_regions), 1)
        self.assertEqual(result.detector_type, "vad")

    def test_empty_regions_is_valid(self) -> None:
        result = DetectionResult(speech_regions=(), detector_type="vad")
        self.assertEqual(len(result.speech_regions), 0)

    def test_frozen_immutability(self) -> None:
        result = DetectionResult(speech_regions=(), detector_type="vad")
        with self.assertRaises(AttributeError):
            result.detector_type = "other"  # type: ignore[misc]

    def test_kw_only(self) -> None:
        with self.assertRaises(TypeError):
            DetectionResult((), "vad")  # type: ignore[misc]

    def test_empty_detector_type_raises(self) -> None:
        with self.assertRaises(ValueError) as ctx:
            DetectionResult(speech_regions=(), detector_type="")
        self.assertIn("detector_type", str(ctx.exception))

    def test_mismatched_region_detector_type_raises(self) -> None:
        region = SpeechRegion(start_sec=0.0, end_sec=1.0, detector_type="energy")
        with self.assertRaises(ValueError) as ctx:
            DetectionResult(speech_regions=(region,), detector_type="vad")
        self.assertIn("does not match", str(ctx.exception))

    def test_multiple_matching_regions_valid(self) -> None:
        regions = tuple(
            SpeechRegion(
                start_sec=float(i),
                end_sec=float(i + 1),
                detector_type="vad",
            )
            for i in range(3)
        )
        result = DetectionResult(speech_regions=regions, detector_type="vad")
        self.assertEqual(len(result.speech_regions), 3)


class TestCombinedResult(unittest.TestCase):
    """Tests for the CombinedResult frozen dataclass."""

    def test_valid_construction(self) -> None:
        regions = (
            SpeechRegion(start_sec=0.0, end_sec=1.0, detector_type="vad"),
            SpeechRegion(start_sec=2.0, end_sec=3.0, detector_type="energy"),
        )
        result = CombinedResult(speech_regions=regions)
        self.assertEqual(len(result.speech_regions), 2)

    def test_empty_regions_is_valid(self) -> None:
        result = CombinedResult(speech_regions=())
        self.assertEqual(len(result.speech_regions), 0)

    def test_frozen_immutability(self) -> None:
        result = CombinedResult(speech_regions=())
        with self.assertRaises(AttributeError):
            result.speech_regions = ()  # type: ignore[misc]

    def test_unsorted_regions_raises(self) -> None:
        regions = (
            SpeechRegion(start_sec=2.0, end_sec=3.0, detector_type="vad"),
            SpeechRegion(start_sec=0.0, end_sec=1.0, detector_type="vad"),
        )
        with self.assertRaises(ValueError) as ctx:
            CombinedResult(speech_regions=regions)
        self.assertIn("sorted", str(ctx.exception))

    def test_mixed_detector_types_valid(self) -> None:
        regions = (
            SpeechRegion(
                start_sec=0.0,
                end_sec=1.0,
                detector_type="energy_squelch,silero_vad",
            ),
            SpeechRegion(
                start_sec=2.0,
                end_sec=3.0,
                detector_type="silero_vad",
            ),
        )
        result = CombinedResult(speech_regions=regions)
        self.assertEqual(len(result.speech_regions), 2)


if __name__ == "__main__":
    unittest.main()
