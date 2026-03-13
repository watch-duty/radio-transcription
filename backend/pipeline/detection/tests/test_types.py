import unittest

from backend.pipeline.detection.types import DetectionResult


class TestDetectionResult(unittest.TestCase):
    """Tests for the DetectionResult frozen dataclass."""

    def test_valid_construction(self) -> None:
        """Creates a valid result with all fields."""
        result = DetectionResult(
            signal_present=True,
            confidence=0.85,
            timestamp_ns=1_000_000,
            detector_type="silero_vad",
        )

        self.assertTrue(result.signal_present)
        self.assertEqual(result.confidence, 0.85)
        self.assertEqual(result.timestamp_ns, 1_000_000)
        self.assertEqual(result.detector_type, "silero_vad")

    def test_frozen_immutability(self) -> None:
        """Assigning to a field raises FrozenInstanceError."""
        result = DetectionResult(
            signal_present=False,
            confidence=0.1,
            timestamp_ns=0,
            detector_type="energy_squelch",
        )

        with self.assertRaises(AttributeError):
            result.confidence = 0.5  # type: ignore[misc]

    def test_kw_only(self) -> None:
        """Positional construction raises TypeError."""
        with self.assertRaises(TypeError):
            DetectionResult(True, 0.5, 100, "test")  # type: ignore[misc]  # noqa: FBT003

    def test_equality(self) -> None:
        """Two results with identical fields are equal."""
        a = DetectionResult(
            signal_present=True,
            confidence=0.9,
            timestamp_ns=42,
            detector_type="energy_squelch",
        )
        b = DetectionResult(
            signal_present=True,
            confidence=0.9,
            timestamp_ns=42,
            detector_type="energy_squelch",
        )
        self.assertEqual(a, b)

    def test_confidence_boundary_zero(self) -> None:
        """confidence=0.0 is valid (no signal detected)."""
        result = DetectionResult(
            signal_present=False,
            confidence=0.0,
            timestamp_ns=0,
            detector_type="test",
        )
        self.assertEqual(result.confidence, 0.0)

    def test_confidence_boundary_one(self) -> None:
        """confidence=1.0 is valid (maximum confidence)."""
        result = DetectionResult(
            signal_present=True,
            confidence=1.0,
            timestamp_ns=0,
            detector_type="test",
        )
        self.assertEqual(result.confidence, 1.0)

    def test_timestamp_zero_is_valid(self) -> None:
        """timestamp_ns=0 is valid (start of window)."""
        result = DetectionResult(
            signal_present=False,
            confidence=0.5,
            timestamp_ns=0,
            detector_type="test",
        )
        self.assertEqual(result.timestamp_ns, 0)

    def test_invalid_confidence_above_one(self) -> None:
        """Confidence > 1.0 raises ValueError."""
        with self.assertRaises(ValueError) as ctx:
            DetectionResult(
                signal_present=True,
                confidence=1.1,
                timestamp_ns=0,
                detector_type="test",
            )

        self.assertIn("confidence", str(ctx.exception))

    def test_invalid_confidence_below_zero(self) -> None:
        """Confidence < 0.0 raises ValueError."""
        with self.assertRaises(ValueError) as ctx:
            DetectionResult(
                signal_present=False,
                confidence=-0.1,
                timestamp_ns=0,
                detector_type="test",
            )

        self.assertIn("confidence", str(ctx.exception))

    def test_invalid_negative_timestamp(self) -> None:
        """Negative timestamp_ns raises ValueError."""
        with self.assertRaises(ValueError) as ctx:
            DetectionResult(
                signal_present=True,
                confidence=0.5,
                timestamp_ns=-1,
                detector_type="test",
            )

        self.assertIn("timestamp_ns", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
