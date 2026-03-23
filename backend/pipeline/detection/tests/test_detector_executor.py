from __future__ import annotations

import unittest
from unittest.mock import MagicMock

import numpy as np

from backend.pipeline.detection.detector_executor import DetectorExecutor
from backend.pipeline.detection.sound_event_signal_combiner import (
    SoundEventSignalCombiner,
)
from backend.pipeline.detection.types import DetectionResult, SpeechRegion


class TestDetectorExecutor(unittest.TestCase):
    """Tests for DetectorExecutor.run()."""

    def test_no_detectors_returns_empty(self) -> None:
        executor = DetectorExecutor([], SoundEventSignalCombiner())
        result = executor.run(np.zeros(16000, dtype=np.int16))

        self.assertEqual(len(result.speech_regions), 0)

    def test_single_detector_returns_results(self) -> None:
        detector = MagicMock()
        detector.detector_type = "test"
        detector.detect.return_value = DetectionResult(
            speech_regions=(
                SpeechRegion(start_sec=0.0, end_sec=1.0, detector_type="test"),
            ),
            detector_type="test",
        )
        executor = DetectorExecutor([detector], SoundEventSignalCombiner())

        result = executor.run(np.zeros(16000, dtype=np.int16))

        self.assertEqual(len(result.speech_regions), 1)
        detector.detect.assert_called_once()

    def test_failing_detector_continues_with_others(self) -> None:
        failing = MagicMock()
        failing.detector_type = "failing"
        failing.detect.side_effect = RuntimeError("Boom")

        good = MagicMock()
        good.detector_type = "good"
        good.detect.return_value = DetectionResult(
            speech_regions=(
                SpeechRegion(start_sec=1.0, end_sec=2.0, detector_type="good"),
            ),
            detector_type="good",
        )
        executor = DetectorExecutor([failing, good], SoundEventSignalCombiner())

        result = executor.run(
            np.zeros(16000, dtype=np.int16), context="test.flac"
        )

        self.assertEqual(len(result.speech_regions), 1)
        self.assertEqual(result.speech_regions[0].detector_type, "good")

    def test_all_detectors_fail_returns_empty(self) -> None:
        d1 = MagicMock()
        d1.detector_type = "d1"
        d1.detect.side_effect = RuntimeError("fail")

        d2 = MagicMock()
        d2.detector_type = "d2"
        d2.detect.side_effect = RuntimeError("fail")

        executor = DetectorExecutor([d1, d2], SoundEventSignalCombiner())

        result = executor.run(np.zeros(16000, dtype=np.int16))

        self.assertEqual(len(result.speech_regions), 0)


if __name__ == "__main__":
    unittest.main()
