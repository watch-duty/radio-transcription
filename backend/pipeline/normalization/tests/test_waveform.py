"""Unit tests for waveform peak extraction."""

import io
import itertools
import math
import unittest
from unittest.mock import MagicMock, patch

import numpy as np
import soundfile as sf

from backend.pipeline.normalization import waveform

_SAMPLE_RATE = 16000


def _flac_bytes(samples: np.ndarray, sample_rate: int = _SAMPLE_RATE) -> bytes:
    buf = io.BytesIO()
    sf.write(buf, samples, sample_rate, format="FLAC")
    return buf.getvalue()


def _sine(
    freq_hz: float,
    duration_s: float,
    amplitude: float,
    sample_rate: int = _SAMPLE_RATE,
) -> np.ndarray:
    t = np.arange(int(sample_rate * duration_s)) / sample_rate
    return (amplitude * np.sin(2 * np.pi * freq_hz * t)).astype(np.float32)


class ComputeWaveformTest(unittest.TestCase):
    def test_bucket_count_and_duration(self) -> None:
        samples = np.full(_SAMPLE_RATE * 10, 0.5, dtype=np.float32)
        result = waveform.compute_waveform(_flac_bytes(samples))

        self.assertEqual(len(result.peaks), 1)
        self.assertEqual(
            len(result.peaks[0]),
            int(10 / waveform.MAX_BIN_SIZE_SECONDS),
        )
        self.assertEqual(result.duration_seconds, 10.0)
        for value in result.peaks[0]:
            self.assertAlmostEqual(value, 0.5, places=3)

    def test_short_clip_gets_one_bucket_and_correct_duration(self) -> None:
        samples = np.full(
            int(_SAMPLE_RATE * 0.2), 0.3, dtype=np.float32
        )  # 0.2s
        result = waveform.compute_waveform(_flac_bytes(samples))

        self.assertEqual(len(result.peaks[0]), 1)
        self.assertAlmostEqual(result.duration_seconds, 0.2, places=3)

    def test_peaks_are_per_bucket_max_abs(self) -> None:
        # 1s -> 2 buckets of 8000 samples. Each half is quiet (0.1) except one
        # loud sample, so the result must be each half's max-abs, not its mean.
        samples = np.full(_SAMPLE_RATE, 0.1, dtype=np.float32)  # 1.0s
        samples[100] = -0.9  # bucket 0: abs(-0.9) = 0.9
        samples[9000] = 0.5  # bucket 1: 0.5
        result = waveform.compute_waveform(_flac_bytes(samples))

        self.assertEqual(len(result.peaks[0]), 2)
        self.assertAlmostEqual(result.peaks[0][0], 0.9, places=2)
        self.assertAlmostEqual(result.peaks[0][1], 0.5, places=2)

    def test_silence_is_all_zero(self) -> None:
        samples = np.zeros(_SAMPLE_RATE * 2, dtype=np.float32)
        result = waveform.compute_waveform(_flac_bytes(samples))

        self.assertTrue(all(value == 0.0 for value in result.peaks[0]))

    def test_stereo_reduces_by_max_magnitude(self) -> None:
        # Right is louder and opposite-sign: 0.8 distinguishes max-magnitude
        # from left-only (0.2) and from averaging (|-0.3| = 0.3).
        left = np.full(_SAMPLE_RATE * 10, 0.2, dtype=np.float32)
        right = np.full(_SAMPLE_RATE * 10, -0.8, dtype=np.float32)
        stereo = np.stack([left, right], axis=1)

        result = waveform.compute_waveform(_flac_bytes(stereo))

        self.assertEqual(len(result.peaks), 1)
        for value in result.peaks[0]:
            self.assertAlmostEqual(value, 0.8, places=3)

    def test_peaks_are_unsigned_and_bounded(self) -> None:
        rng = np.random.default_rng(0)
        samples = (rng.random(_SAMPLE_RATE * 5) * 2 - 1).astype(np.float32)

        result = waveform.compute_waveform(_flac_bytes(samples))

        for value in result.peaks[0]:
            self.assertGreaterEqual(value, 0.0)
            self.assertLessEqual(value, 1.0)

    @patch.object(waveform.sf, "read")
    def test_empty_audio_raises(self, mock_read: MagicMock) -> None:
        # The processor calls this best-effort, so the raise surfaces as a
        # skipped annotation rather than a pipeline failure.
        mock_read.return_value = (np.array([], dtype=np.float32), _SAMPLE_RATE)
        with self.assertRaises(ValueError):
            waveform.compute_waveform(b"")

    def test_matches_independent_bucketing_oracle(self) -> None:
        rng = np.random.default_rng(42)
        samples = (rng.random(_SAMPLE_RATE * 3) * 2 - 1).astype(np.float32)
        flac = _flac_bytes(samples)
        # Decode the same FLAC so quantization matches, then bucket it
        # independently of the implementation under test.
        decoded, _ = sf.read(io.BytesIO(flac), dtype="float32")
        num_buckets = max(1, math.ceil(3 / waveform.MAX_BIN_SIZE_SECONDS))
        expected = [
            round(float(np.abs(bucket).max()), waveform.PEAK_DECIMALS)
            for bucket in np.array_split(decoded, num_buckets)
        ]

        result = waveform.compute_waveform(flac)

        self.assertEqual(result.peaks[0], expected)

    def test_spike_localizes_to_its_bucket(self) -> None:
        samples = np.zeros(
            _SAMPLE_RATE * 4, dtype=np.float32
        )  # 4s -> 8 buckets
        samples[int(_SAMPLE_RATE * 2.7)] = 0.9  # bucket 5 spans 2.5-3.0s
        result = waveform.compute_waveform(_flac_bytes(samples))

        peaks = result.peaks[0]
        self.assertEqual(len(peaks), 8)
        self.assertEqual([i for i, v in enumerate(peaks) if v > 0.0], [5])
        self.assertAlmostEqual(peaks[5], 0.9, places=2)

    def test_full_scale_sine_preserves_amplitude(self) -> None:
        result = waveform.compute_waveform(_flac_bytes(_sine(440, 2.0, 0.8)))

        for value in result.peaks[0]:
            self.assertAlmostEqual(value, 0.8, places=2)

    def test_sample_rate_invariance(self) -> None:
        duration, freq, amp = 4.0, 300.0, 0.7
        low = waveform.compute_waveform(
            _flac_bytes(_sine(freq, duration, amp, 8000), sample_rate=8000)
        )
        high = waveform.compute_waveform(
            _flac_bytes(_sine(freq, duration, amp, 44100), sample_rate=44100)
        )

        self.assertEqual(len(low.peaks[0]), len(high.peaks[0]))
        self.assertAlmostEqual(
            low.duration_seconds, high.duration_seconds, places=3
        )
        for lo, hi in zip(low.peaks[0], high.peaks[0], strict=True):
            self.assertAlmostEqual(lo, hi, places=2)

    def test_rising_envelope_yields_increasing_peaks(self) -> None:
        duration = 5.0  # 10 buckets
        tone = _sine(200, duration, 1.0)
        envelope = (np.arange(tone.size) / tone.size).astype(np.float32)
        result = waveform.compute_waveform(_flac_bytes(tone * envelope))

        peaks = result.peaks[0]
        self.assertEqual(len(peaks), 10)
        for earlier, later in itertools.pairwise(peaks):
            self.assertLess(earlier, later)

    def test_deterministic_across_calls(self) -> None:
        rng = np.random.default_rng(7)
        samples = (rng.random(_SAMPLE_RATE * 3) * 2 - 1).astype(np.float32)
        flac = _flac_bytes(samples)

        first = waveform.compute_waveform(flac)
        second = waveform.compute_waveform(flac)

        self.assertEqual(first.peaks, second.peaks)
        self.assertEqual(first.duration_seconds, second.duration_seconds)
