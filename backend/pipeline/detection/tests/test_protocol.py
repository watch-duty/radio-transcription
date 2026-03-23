import unittest

from backend.pipeline.detection.protocol import SoundEventDetector
from backend.pipeline.detection.types import DetectionResult


class _StubDetector:
    """Minimal concrete implementation for Protocol conformance testing."""

    @property
    def detector_type(self) -> str:
        return "test_stub"

    def detect(self, samples) -> DetectionResult:
        return DetectionResult(speech_regions=(), detector_type="test_stub")


class TestSoundEventDetectorProtocol(unittest.TestCase):
    """Tests for the SoundEventDetector runtime-checkable Protocol."""

    def test_conforming_class_passes_isinstance(self) -> None:
        """A class implementing all required members is recognized."""
        detector = _StubDetector()
        self.assertIsInstance(detector, SoundEventDetector)

    def test_missing_detect_fails_isinstance(self) -> None:
        """A class missing the detect method is not recognized."""

        class _NoDetect:
            @property
            def detector_type(self) -> str:
                return "broken"

        self.assertNotIsInstance(_NoDetect(), SoundEventDetector)

    def test_missing_detector_type_fails_isinstance(self) -> None:
        """A class missing the detector_type property is not recognized."""

        class _NoDetectorType:
            def detect(self, samples):
                return DetectionResult(speech_regions=(), detector_type="x")

        self.assertNotIsInstance(_NoDetectorType(), SoundEventDetector)


if __name__ == "__main__":
    unittest.main()
