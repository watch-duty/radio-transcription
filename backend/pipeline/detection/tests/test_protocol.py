import unittest

from backend.pipeline.detection.protocol import SoundEventDetector
from backend.pipeline.detection.types import DetectionResult


class _StubDetector:
    """Minimal concrete implementation for Protocol conformance testing."""

    @property
    def detector_type(self) -> str:
        return "test_stub"

    @property
    def sample_rate(self) -> int:
        return 16000

    @property
    def is_healthy(self) -> bool:
        return True

    @property
    def needs_executor(self) -> bool:
        return False

    def feed(self, samples) -> None:
        pass

    def pop_results(self) -> list[DetectionResult]:
        return []

    def reset(self) -> None:
        pass


class TestSoundEventDetectorProtocol(unittest.TestCase):
    """Tests for the SoundEventDetector runtime-checkable Protocol."""

    def test_conforming_class_passes_isinstance(self) -> None:
        """A class implementing all required members is recognized."""
        detector = _StubDetector()
        self.assertIsInstance(detector, SoundEventDetector)

    def test_missing_feed_fails_isinstance(self) -> None:
        """A class missing the feed method is not recognized."""

        class _NoFeed:
            @property
            def detector_type(self) -> str:
                return "broken"

            @property
            def sample_rate(self) -> int:
                return 16000

            @property
            def is_healthy(self) -> bool:
                return True

            @property
            def needs_executor(self) -> bool:
                return False

            def pop_results(self) -> list[DetectionResult]:
                return []

            def reset(self) -> None:
                pass

        self.assertNotIsInstance(_NoFeed(), SoundEventDetector)

    def test_missing_detector_type_fails_isinstance(self) -> None:
        """A class missing the detector_type property is not recognized."""

        class _NoDetectorType:
            @property
            def sample_rate(self) -> int:
                return 16000

            @property
            def is_healthy(self) -> bool:
                return True

            @property
            def needs_executor(self) -> bool:
                return False

            def feed(self, samples) -> None:
                pass

            def pop_results(self) -> list[DetectionResult]:
                return []

            def reset(self) -> None:
                pass

        self.assertNotIsInstance(_NoDetectorType(), SoundEventDetector)

    def test_missing_needs_executor_fails_isinstance(self) -> None:
        """A class missing the needs_executor property is not recognized."""

        class _NoNeedsExecutor:
            @property
            def detector_type(self) -> str:
                return "broken"

            @property
            def sample_rate(self) -> int:
                return 16000

            @property
            def is_healthy(self) -> bool:
                return True

            def feed(self, samples) -> None:
                pass

            def pop_results(self) -> list[DetectionResult]:
                return []

            def reset(self) -> None:
                pass

        self.assertNotIsInstance(_NoNeedsExecutor(), SoundEventDetector)


if __name__ == "__main__":
    unittest.main()
