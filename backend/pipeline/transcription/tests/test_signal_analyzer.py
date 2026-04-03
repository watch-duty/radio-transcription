import sys
from unittest.mock import MagicMock

# Mock sherpa_onnx before importing anything that might use it
mock_sherpa = MagicMock()
sys.modules["sherpa_onnx"] = mock_sherpa

import unittest
import numpy as np
from pydub.generators import Sine
from backend.pipeline.transcription.audio.signal_processing import RadioSignalAnalyzer

class TestRadioSignalAnalyzer(unittest.TestCase):
    def setUp(self):
        self.analyzer = RadioSignalAnalyzer(sample_rate=16000)

    def test_pure_tone_rejection(self):
        """Verifies that a pure sine wave is classified as a tone (non-speech)."""
        # Generate 1 second of pure sine wave at 440Hz
        tone = Sine(440).to_audio_segment(duration=1000, volume=-10.0).set_frame_rate(16000).set_channels(1)
        samples = np.array(tone.get_array_of_samples(), dtype=np.float32)
        # Normalize to [-1, 1]
        samples = samples / (2**15)
        
        result = self.analyzer.characterize(samples)
        
        self.assertEqual(result.label, "deterministic_linear")
        self.assertFalse(result.is_transcribable)

    def test_white_noise_fallback(self):
        """Verifies that white noise does not trigger tone detection and defaults to stochastic."""
        # Generate 1 second of white noise
        samples = np.random.normal(0, 0.1, 16000).astype(np.float32)
        
        result = self.analyzer.characterize(samples)
        
        self.assertEqual(result.label, "stochastic")
        self.assertTrue(result.is_transcribable)  # In POC, stochastic is always marked is_transcribable=True
