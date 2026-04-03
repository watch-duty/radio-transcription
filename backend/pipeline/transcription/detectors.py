import logging

import numpy as np
from scipy.signal import medfilt

from backend.pipeline.common.constants import MS_PER_SECOND, SAMPLE_RATE_HZ
from backend.pipeline.transcription.constants import (
    BACKGROUND_NOISE_PERCENTILE,
    DEFAULT_AGD_DEBOUNCE_WINDOW_SEC,
    DEFAULT_AGD_ENERGY_THRESHOLD,
    DEFAULT_AGD_FFT_SIZE,
    DEFAULT_AGD_HANGOVER_FRAMES,
    DEFAULT_AGD_HIGH_FREQ_HZ,
    DEFAULT_AGD_HOP_SIZE,
    DEFAULT_AGD_LOW_FREQ_HZ,
    DEFAULT_AGD_LOWER_THRESHOLD,
    DEFAULT_AGD_THRESHOLD,
    DYNAMIC_ENERGY_MULTIPLIER,
    INT16_MAX_FLOAT,
    MEDIAN_FILTER_VOTING_THRESHOLD,
    MIN_AGD_FFT_SIZE,
)
from backend.pipeline.transcription.datatypes import TimeRange
from backend.pipeline.transcription.dsp import (
    compute_rms_energy,
    compute_spectral_flatness,
)

logger = logging.getLogger(__name__)


class AcousticGateDetector:
    """Spectral flatness detector with adaptive sub-band selection.

    Uses scipy STFT for spectrogram extraction and geometric/arithmetic
    mean ratio for per-frame flatness computation. Low flatness
    (tonal/speech) produces signal_present=True; consecutive speech
    frames are grouped into TimeRange objects in milliseconds.

    All internal state is read-only (numpy boolean mask), making this
    detector safe for concurrent use across threads.
    """

    def __init__(
        self,
        *,
        threshold: float = DEFAULT_AGD_THRESHOLD,
        lower_threshold: float = DEFAULT_AGD_LOWER_THRESHOLD,
        energy_threshold: float = DEFAULT_AGD_ENERGY_THRESHOLD,
        debounce_window: float = DEFAULT_AGD_DEBOUNCE_WINDOW_SEC,
        hangover_frames: int = DEFAULT_AGD_HANGOVER_FRAMES,
        low_freq_hz: float = DEFAULT_AGD_LOW_FREQ_HZ,
        high_freq_hz: float = DEFAULT_AGD_HIGH_FREQ_HZ,
        fft_size: int = DEFAULT_AGD_FFT_SIZE,
        hop_size: int = DEFAULT_AGD_HOP_SIZE,
    ) -> None:
        if fft_size < MIN_AGD_FFT_SIZE:
            msg = f"fft_size must be >= {MIN_AGD_FFT_SIZE}, got {fft_size}"
            raise ValueError(msg)
        if fft_size & (fft_size - 1) != 0:
            msg = f"fft_size must be a power of 2, got {fft_size}"
            raise ValueError(msg)
        if hop_size <= 0 or hop_size > fft_size:
            msg = f"hop_size must be in (0, {fft_size}], got {hop_size}"
            raise ValueError(msg)
        if not (0.0 <= threshold <= 1.0):
            msg = f"threshold must be in [0.0, 1.0], got {threshold}"
            raise ValueError(msg)
        if not (0.0 <= lower_threshold < threshold):
            msg = f"lower_threshold must be in [0.0, {threshold}), got {lower_threshold}"
            raise ValueError(msg)
        if hangover_frames < 0:
            msg = f"hangover_frames must be >= 0, got {hangover_frames}"
            raise ValueError(msg)
        if low_freq_hz <= 0:
            msg = f"low_freq_hz must be > 0, got {low_freq_hz}"
            raise ValueError(msg)
        nyquist = SAMPLE_RATE_HZ / 2.0
        if high_freq_hz > nyquist:
            msg = f"high_freq_hz must be <= {nyquist}, got {high_freq_hz}"
            raise ValueError(msg)
        if low_freq_hz >= high_freq_hz:
            msg = f"low_freq_hz ({low_freq_hz}) must be < high_freq_hz ({high_freq_hz})"
            raise ValueError(msg)

        self._threshold = threshold
        self._lower_threshold = lower_threshold
        self._energy_threshold = energy_threshold
        self._debounce_window = debounce_window
        self._hangover_frames = hangover_frames
        self._fft_size = fft_size
        self._hop_size = hop_size

        # Precompute read-only frequency bin mask for sub-band filtering
        freqs = np.fft.rfftfreq(fft_size, d=1.0 / SAMPLE_RATE_HZ)
        self._freq_mask = (freqs >= low_freq_hz) & (freqs <= high_freq_hz)

        if not np.any(self._freq_mask):
            msg = (
                f"Frequency range [{low_freq_hz}, {high_freq_hz}] Hz "
                f"contains no bins at fft_size={fft_size}. "
                f"Increase fft_size or widen the frequency range."
            )
            raise ValueError(msg)

    def detect(
        self, samples: np.ndarray, file_start_ms: int = 0
    ) -> list[TimeRange]:
        if samples.size == 0 or samples.size < self._fft_size:
            return []

        audio = samples.astype(np.float32) / INT16_MAX_FLOAT

        # 1. Mathematical Evaluation using shared DSP libraries
        rms_energy = compute_rms_energy(
            audio, hop_length=self._hop_size, frame_length=self._fft_size
        )
        flatness_arr = compute_spectral_flatness(
            audio,
            sample_rate=SAMPLE_RATE_HZ,
            n_fft=self._fft_size,
            hop_length=self._hop_size,
            freq_mask=self._freq_mask,
        )

        n_time = min(len(rms_energy), len(flatness_arr))
        if n_time == 0:
            return []

        rms_energy = rms_energy[:n_time]
        flatness_arr = flatness_arr[:n_time]

        # 2. Dual-Sensor Acoustic Gate
        # Establish dynamic background acoustic floor
        noise_floor = (
            np.percentile(rms_energy, BACKGROUND_NOISE_PERCENTILE)
            if len(rms_energy) > 0
            else 0.0
        )
        dynamic_energy_thresh = max(
            self._energy_threshold, noise_floor * DYNAMIC_ENERGY_MULTIPLIER
        )

        # Vectorized logical AND for energy/flatness gating
        signal_present = (
            (rms_energy > dynamic_energy_thresh)
            & (flatness_arr < self._threshold)
            & (flatness_arr > self._lower_threshold)
        )

        # 3. Time domain smoothing (Medfilt debounce + hangover)
        time_per_frame = self._hop_size / SAMPLE_RATE_HZ
        debounce_frames = int(self._debounce_window / time_per_frame)
        if debounce_frames % 2 == 0:
            debounce_frames += 1

        if debounce_frames > 1 and len(signal_present) >= debounce_frames:
            signal_present = (
                medfilt(
                    signal_present.astype(float), kernel_size=debounce_frames
                )
                > MEDIAN_FILTER_VOTING_THRESHOLD
            )

        if self._hangover_frames > 0:
            # Convolution applies a rolling asymmetric forward hangover
            kernel = np.ones(self._hangover_frames + 1, dtype=int)
            signal_present = (
                np.convolve(signal_present, kernel, mode="full")[
                    : len(signal_present)
                ]
                > 0
            )

        # Group consecutive True frames into TimeRanges using vectorized boundary detection
        regions: list[TimeRange] = []
        if len(signal_present) > 0:
            padded = np.pad(signal_present.astype(int), (1, 1), mode="constant")
            diffs = np.diff(padded)
            starts = np.flatnonzero(diffs == 1)
            ends = np.flatnonzero(diffs == -1)

            time_per_frame_ms = (
                self._hop_size / SAMPLE_RATE_HZ
            ) * MS_PER_SECOND
            for start_idx, end_idx in zip(starts, ends, strict=True):
                regions.append(
                    TimeRange(
                        start_ms=file_start_ms
                        + int(start_idx * time_per_frame_ms),
                        end_ms=file_start_ms + int(end_idx * time_per_frame_ms),
                    )
                )

        return regions
