import unittest
from unittest.mock import MagicMock, patch

import numpy as np

from backend.pipeline.common.constants import SAMPLE_RATE_HZ
from backend.pipeline.transcription.audio.detectors import AcousticGateDetector
from backend.pipeline.transcription.common.datatypes import TimeRange


class AcousticGateDetectorTest(unittest.TestCase):
    def test_detect_empty_audio(self) -> None:
        detector = AcousticGateDetector()
        results = detector.detect(np.array([], dtype=np.int16))
        self.assertEqual(results, [])

    def test_detect_short_audio(self) -> None:
        detector = AcousticGateDetector()
        # Default FFT size is 2048. Less than that should return []
        results = detector.detect(np.zeros(1000, dtype=np.int16))
        self.assertEqual(results, [])

    def test_detect_silence(self) -> None:
        detector = AcousticGateDetector()
        # Pass enough samples but all zeros
        results = detector.detect(np.zeros(16000, dtype=np.int16))
        self.assertEqual(results, [])

    @patch(
        "backend.pipeline.transcription.audio.detectors.compute_spectral_flatness"
    )
    @patch("backend.pipeline.transcription.audio.detectors.compute_rms_energy")
    def test_detect_speech_like(
        self, mock_rms: MagicMock, mock_flatness: MagicMock
    ) -> None:
        detector = AcousticGateDetector()

        # Mock high energy and low flatness (between 0.005 and 0.4)
        # We need some low energy frames to establish a low noise floor!
        mock_rms.return_value = np.array([0.01, 0.01] + [1.0] * 10)
        mock_flatness.return_value = np.ones(12) * 0.1

        samples = np.zeros(16000, dtype=np.int16)
        results = detector.detect(samples)

        # It should detect speech!
        self.assertTrue(len(results) > 0)
        self.assertIsInstance(results[0], TimeRange)

    def test_detect_noise_like(self) -> None:
        detector = AcousticGateDetector()
        # Create white noise (high energy, high flatness)
        rng = np.random.default_rng()
        samples = rng.integers(
            -32768, 32767, size=SAMPLE_RATE_HZ, dtype=np.int16
        )

        results = detector.detect(samples)
        # It should NOT detect speech (flatness should be high)
        self.assertEqual(results, [])
