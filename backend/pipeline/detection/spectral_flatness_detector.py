from __future__ import annotations

import logging

import numpy as np
from scipy.signal import stft
from scipy.stats import gmean

from backend.pipeline.common.constants import AUDIO_SAMPLE_RATE
from backend.pipeline.detection.detector_factory import DetectorFactory
from backend.pipeline.detection.types import DetectionResult, SpeechRegion

logger = logging.getLogger(__name__)

_DETECTOR_TYPE = "spectral_flatness"


class SpectralFlatnessDetector:
    """Spectral flatness detector with adaptive sub-band selection.

    Uses scipy STFT for spectrogram extraction and geometric/arithmetic
    mean ratio for per-frame flatness computation. Low flatness
    (tonal/speech) produces signal_present=True; consecutive speech
    frames are grouped into SpeechRegion objects.

    All internal state is read-only (numpy boolean mask), making this
    detector safe for concurrent use across threads.
    """

    def __init__(  # noqa: PLR0913
        self,
        *,
        threshold: float = 0.4,
        hangover_frames: int = 3,
        low_freq_hz: float = 300.0,
        high_freq_hz: float = 3400.0,
        fft_size: int = 512,
        hop_size: int = 320,
    ) -> None:
        if fft_size < 256:
            msg = f"fft_size must be >= 256, got {fft_size}"
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
        if hangover_frames < 0:
            msg = f"hangover_frames must be >= 0, got {hangover_frames}"
            raise ValueError(msg)
        if low_freq_hz <= 0:
            msg = f"low_freq_hz must be > 0, got {low_freq_hz}"
            raise ValueError(msg)
        nyquist = AUDIO_SAMPLE_RATE / 2
        if high_freq_hz > nyquist:
            msg = f"high_freq_hz must be <= {nyquist}, got {high_freq_hz}"
            raise ValueError(msg)
        if low_freq_hz >= high_freq_hz:
            msg = f"low_freq_hz ({low_freq_hz}) must be < high_freq_hz ({high_freq_hz})"
            raise ValueError(msg)

        self._threshold = threshold
        self._hangover_frames = hangover_frames
        self._fft_size = fft_size
        self._hop_size = hop_size

        # Precompute read-only frequency bin mask for sub-band filtering
        freqs = np.fft.rfftfreq(fft_size, d=1.0 / AUDIO_SAMPLE_RATE)
        self._freq_mask = (freqs >= low_freq_hz) & (freqs <= high_freq_hz)

        if not np.any(self._freq_mask):
            msg = (
                f"Frequency range [{low_freq_hz}, {high_freq_hz}] Hz "
                f"contains no bins at fft_size={fft_size}. "
                f"Increase fft_size or widen the frequency range."
            )
            raise ValueError(msg)

    @property
    def detector_type(self) -> str:
        return _DETECTOR_TYPE

    def detect(self, samples: np.ndarray) -> DetectionResult:
        if samples.size == 0 or samples.size < self._fft_size:
            return DetectionResult(speech_regions=(), detector_type=_DETECTOR_TYPE)

        audio = samples.astype(np.float32) / 32768.0

        # STFT -> magnitude spectrogram (explicit hann window)
        _, _, zxx = stft(
            audio,
            fs=AUDIO_SAMPLE_RATE,
            window="hann",
            nperseg=self._fft_size,
            noverlap=self._fft_size - self._hop_size,
        )
        spec_subband = np.abs(zxx[self._freq_mask, :])
        n_time = spec_subband.shape[1]

        if n_time == 0:
            return DetectionResult(speech_regions=(), detector_type=_DETECTOR_TYPE)

        # Floor tiny values to avoid log(0) warnings from gmean
        spec_subband = np.maximum(spec_subband, np.finfo(np.float32).tiny)

        # Spectral flatness = geometric_mean / arithmetic_mean per frame
        geo = gmean(spec_subband, axis=0)
        arith = np.mean(spec_subband, axis=0)
        flatness_arr = np.where(arith > 0, geo / arith, 1.0)

        signal_present = flatness_arr < self._threshold

        # Apply hangover smoothing
        if self._hangover_frames > 0:
            hangover_counter = 0
            for i in range(len(signal_present)):
                if signal_present[i]:
                    hangover_counter = self._hangover_frames
                elif hangover_counter > 0:
                    signal_present[i] = True
                    hangover_counter -= 1

        # Group consecutive True frames into SpeechRegions
        regions: list[SpeechRegion] = []
        in_region = False
        first_frame = 0

        for i in range(len(signal_present)):
            if signal_present[i] and not in_region:
                first_frame = i
                in_region = True
            elif not signal_present[i] and in_region:
                regions.append(
                    SpeechRegion(
                        start_sec=first_frame * self._hop_size / AUDIO_SAMPLE_RATE,
                        end_sec=i * self._hop_size / AUDIO_SAMPLE_RATE,
                        detector_type=_DETECTOR_TYPE,
                    )
                )
                in_region = False

        if in_region:
            regions.append(
                SpeechRegion(
                    start_sec=first_frame * self._hop_size / AUDIO_SAMPLE_RATE,
                    end_sec=len(signal_present) * self._hop_size / AUDIO_SAMPLE_RATE,
                    detector_type=_DETECTOR_TYPE,
                )
            )

        return DetectionResult(
            speech_regions=tuple(regions), detector_type=_DETECTOR_TYPE
        )


if _DETECTOR_TYPE not in DetectorFactory._registry:  # noqa: SLF001
    DetectorFactory.register(_DETECTOR_TYPE, SpectralFlatnessDetector)
