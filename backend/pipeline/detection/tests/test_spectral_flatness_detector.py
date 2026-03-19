from __future__ import annotations

import unittest

import numpy as np

from backend.pipeline.detection.detector_factory import DetectorFactory
from backend.pipeline.detection.protocol import SoundEventDetector
from backend.pipeline.detection.spectral_flatness_detector import (
    SpectralFlatnessDetector,
)


class TestConstructorDefaults(unittest.TestCase):
    """Default construction succeeds."""

    def test_default_construction(self) -> None:
        detector = SpectralFlatnessDetector()
        self.assertEqual(detector.detector_type, "spectral_flatness")


class TestConstructorValidation(unittest.TestCase):
    """Each invalid parameter raises ValueError with a descriptive message."""

    def test_fft_size_not_power_of_two_raises(self) -> None:
        with self.assertRaises(ValueError) as ctx:
            SpectralFlatnessDetector(fft_size=300)
        self.assertIn("fft_size", str(ctx.exception))

    def test_fft_size_too_small_raises(self) -> None:
        with self.assertRaises(ValueError) as ctx:
            SpectralFlatnessDetector(fft_size=128)
        self.assertIn("fft_size", str(ctx.exception))

    def test_hop_size_zero_raises(self) -> None:
        with self.assertRaises(ValueError) as ctx:
            SpectralFlatnessDetector(hop_size=0)
        self.assertIn("hop_size", str(ctx.exception))

    def test_hop_size_exceeds_fft_size_raises(self) -> None:
        with self.assertRaises(ValueError) as ctx:
            SpectralFlatnessDetector(hop_size=1024)
        self.assertIn("hop_size", str(ctx.exception))

    def test_threshold_below_zero_raises(self) -> None:
        with self.assertRaises(ValueError) as ctx:
            SpectralFlatnessDetector(threshold=-0.1)
        self.assertIn("threshold", str(ctx.exception))

    def test_threshold_above_one_raises(self) -> None:
        with self.assertRaises(ValueError) as ctx:
            SpectralFlatnessDetector(threshold=1.1)
        self.assertIn("threshold", str(ctx.exception))

    def test_hangover_frames_negative_raises(self) -> None:
        with self.assertRaises(ValueError) as ctx:
            SpectralFlatnessDetector(hangover_frames=-1)
        self.assertIn("hangover_frames", str(ctx.exception))

    def test_low_freq_hz_zero_raises(self) -> None:
        with self.assertRaises(ValueError) as ctx:
            SpectralFlatnessDetector(low_freq_hz=0.0)
        self.assertIn("low_freq_hz", str(ctx.exception))

    def test_high_freq_hz_above_nyquist_raises(self) -> None:
        with self.assertRaises(ValueError) as ctx:
            SpectralFlatnessDetector(high_freq_hz=8001.0)
        self.assertIn("high_freq_hz", str(ctx.exception))

    def test_low_freq_exceeds_high_freq_raises(self) -> None:
        with self.assertRaises(ValueError) as ctx:
            SpectralFlatnessDetector(low_freq_hz=4000.0, high_freq_hz=3000.0)
        self.assertIn("low_freq_hz", str(ctx.exception))

    def test_low_freq_equals_high_freq_raises(self) -> None:
        with self.assertRaises(ValueError) as ctx:
            SpectralFlatnessDetector(low_freq_hz=3000.0, high_freq_hz=3000.0)
        self.assertIn("low_freq_hz", str(ctx.exception))

    def test_valid_custom_params(self) -> None:
        detector = SpectralFlatnessDetector(
            threshold=0.5,
            hangover_frames=5,
            low_freq_hz=500.0,
            high_freq_hz=3000.0,
            fft_size=1024,
            hop_size=512,
        )
        self.assertEqual(detector.detector_type, "spectral_flatness")

    def test_unknown_param_raises(self) -> None:
        with self.assertRaises(ValueError) as ctx:
            SpectralFlatnessDetector(threshhold=0.4)
        self.assertIn("threshhold", str(ctx.exception))


class TestProtocolConformance(unittest.TestCase):
    """SpectralFlatnessDetector satisfies the SoundEventDetector protocol."""

    def test_isinstance_check(self) -> None:
        detector = SpectralFlatnessDetector()
        self.assertIsInstance(detector, SoundEventDetector)

    def test_detector_type_returns_string(self) -> None:
        detector = SpectralFlatnessDetector()
        self.assertEqual(detector.detector_type, "spectral_flatness")


class TestDetectEdgeCases(unittest.TestCase):
    """detect() handles degenerate inputs gracefully."""

    def setUp(self) -> None:
        self.detector = SpectralFlatnessDetector()

    def test_empty_array_returns_empty_result(self) -> None:
        samples = np.array([], dtype=np.int16)
        result = self.detector.detect(samples)
        self.assertEqual(result.speech_regions, ())
        self.assertEqual(result.detector_type, "spectral_flatness")

    def test_short_array_returns_empty_result(self) -> None:
        samples = np.zeros(100, dtype=np.int16)
        result = self.detector.detect(samples)
        self.assertEqual(result.speech_regions, ())

    def test_low_level_noise_returns_empty_result(self) -> None:
        """Low-amplitude noise (radio silence) has high flatness -> no speech."""
        rng = np.random.default_rng(42)
        samples = (rng.standard_normal(16000) * 100).astype(np.int16)
        result = self.detector.detect(samples)
        self.assertEqual(result.speech_regions, ())


class TestDetectCoreBehavior(unittest.TestCase):
    """detect() correctly distinguishes tonal (speech-like) from noise."""

    def setUp(self) -> None:
        self.detector = SpectralFlatnessDetector(threshold=0.4, hangover_frames=0)
        self.sr = 16_000

    def _make_sine(self, freq_hz: float, duration_sec: float) -> np.ndarray:
        t = np.arange(int(self.sr * duration_sec), dtype=np.float32) / self.sr
        audio = np.sin(2 * np.pi * freq_hz * t) * 0.5
        return (audio * 32767).astype(np.int16)

    def _make_noise(self, duration_sec: float, *, seed: int = 42) -> np.ndarray:
        rng = np.random.default_rng(seed)
        audio = (
            rng.standard_normal(int(self.sr * duration_sec)).astype(np.float32) * 0.1
        )
        return (audio * 32767).astype(np.int16)

    def test_pure_tone_detected_as_speech(self) -> None:
        samples = self._make_sine(1000.0, 2.0)
        result = self.detector.detect(samples)
        self.assertGreater(len(result.speech_regions), 0)
        total_speech = sum(r.end_sec - r.start_sec for r in result.speech_regions)
        self.assertGreater(total_speech, 1.5)

    def test_white_noise_not_detected_as_speech(self) -> None:
        samples = self._make_noise(2.0)
        result = self.detector.detect(samples)
        total_speech = sum(r.end_sec - r.start_sec for r in result.speech_regions)
        self.assertLess(total_speech, 0.5)

    def test_tone_then_noise_produces_region_in_tone_part(self) -> None:
        tone = self._make_sine(1000.0, 1.0)
        noise = self._make_noise(1.0, seed=99)
        samples = np.concatenate([tone, noise])
        result = self.detector.detect(samples)
        self.assertGreater(len(result.speech_regions), 0)
        self.assertLess(result.speech_regions[0].start_sec, 0.1)
        self.assertLess(result.speech_regions[0].end_sec, 1.5)

    def test_result_regions_have_correct_detector_type(self) -> None:
        samples = self._make_sine(440.0, 1.0)
        result = self.detector.detect(samples)
        for region in result.speech_regions:
            self.assertEqual(region.detector_type, "spectral_flatness")

    def test_440hz_sine_detected(self) -> None:
        """440 Hz is within the default 300-3400 Hz sub-band."""
        samples = self._make_sine(440.0, 2.0)
        result = self.detector.detect(samples)
        self.assertGreater(len(result.speech_regions), 0)


class TestHangoverSmoothing(unittest.TestCase):
    """Hangover extends speech regions to bridge micro-pauses."""

    def setUp(self) -> None:
        self.sr = 16_000

    def _make_sine(self, freq_hz: float, duration_sec: float) -> np.ndarray:
        t = np.arange(int(self.sr * duration_sec), dtype=np.float32) / self.sr
        audio = np.sin(2 * np.pi * freq_hz * t) * 0.5
        return (audio * 32767).astype(np.int16)

    def _make_noise(self, duration_sec: float, *, seed: int = 42) -> np.ndarray:
        rng = np.random.default_rng(seed)
        audio = (
            rng.standard_normal(int(self.sr * duration_sec)).astype(np.float32) * 0.1
        )
        return (audio * 32767).astype(np.int16)

    def test_hangover_zero_no_extension(self) -> None:
        detector = SpectralFlatnessDetector(hangover_frames=0)
        samples = self._make_sine(1000.0, 1.0)
        result_no_hangover = detector.detect(samples)
        total_no = sum(
            r.end_sec - r.start_sec for r in result_no_hangover.speech_regions
        )
        self.assertGreater(total_no, 0)

    def test_hangover_extends_regions(self) -> None:
        det_no = SpectralFlatnessDetector(hangover_frames=0)
        det_yes = SpectralFlatnessDetector(hangover_frames=10)
        samples = self._make_sine(1000.0, 2.0)
        total_no = sum(
            r.end_sec - r.start_sec for r in det_no.detect(samples).speech_regions
        )
        total_yes = sum(
            r.end_sec - r.start_sec for r in det_yes.detect(samples).speech_regions
        )
        self.assertGreaterEqual(total_yes, total_no)

    def test_hangover_bridges_short_gap(self) -> None:
        tone1 = self._make_sine(1000.0, 0.5)
        gap = self._make_noise(0.05, seed=77)
        tone2 = self._make_sine(1000.0, 0.5)
        samples = np.concatenate([tone1, gap, tone2])

        det_no = SpectralFlatnessDetector(hangover_frames=0)
        det_yes = SpectralFlatnessDetector(hangover_frames=5)
        regions_no = det_no.detect(samples).speech_regions
        regions_yes = det_yes.detect(samples).speech_regions
        self.assertLessEqual(len(regions_yes), len(regions_no))


class TestFactoryRegistration(unittest.TestCase):
    """SpectralFlatnessDetector auto-registers with DetectorFactory."""

    def test_registered_in_factory(self) -> None:
        self.assertIn("spectral_flatness", DetectorFactory._registry)
        self.assertIs(
            DetectorFactory._registry["spectral_flatness"],
            SpectralFlatnessDetector,
        )

    def test_create_ensemble_instantiates(self) -> None:
        config = {"detectors": [{"type": "spectral_flatness", "threshold": 0.5}]}
        detectors, _ = DetectorFactory.create_ensemble(config)
        self.assertEqual(len(detectors), 1)
        self.assertIsInstance(detectors[0], SpectralFlatnessDetector)


if __name__ == "__main__":
    unittest.main()
