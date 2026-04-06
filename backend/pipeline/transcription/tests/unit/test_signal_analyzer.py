import unittest

import numpy as np

from backend.pipeline.transcription.audio.signal_processing import (
    RadioSignalAnalyzer,
    SignalLabel,
)


class TestRadioSignalAnalyzer(unittest.TestCase):
    def setUp(self) -> None:
        self.analyzer = RadioSignalAnalyzer(sample_rate=16000)

    def test_pure_tone_rejection(self) -> None:
        """Verifies that a pure sine wave is classified as a tone (non-speech)."""
        # Generate 1 second of pure sine wave at 440Hz
        sample_rate = 16000
        duration = 1.0
        t = np.linspace(
            0, duration, int(sample_rate * duration), endpoint=False
        )
        amplitude = 10 ** (-10 / 20)  # -10 dBFS
        samples = (amplitude * np.sin(2 * np.pi * 440 * t)).astype(np.float32)

        result = self.analyzer.characterize(samples)

        self.assertEqual(result.label, SignalLabel.DETERMINISTIC_LINEAR)
        self.assertFalse(result.is_transcribable)

    def test_white_noise_fallback(self) -> None:
        """Verifies that white noise does not trigger tone detection and defaults to stochastic."""
        # Generate 1 second of white noise
        rng = np.random.default_rng()
        samples = rng.normal(0, 0.1, 16000).astype(np.float32)

        result = self.analyzer.characterize(samples)

        self.assertEqual(result.label, SignalLabel.STOCHASTIC)
        self.assertTrue(
            result.is_transcribable
        )  # In POC, stochastic is always marked is_transcribable=True
