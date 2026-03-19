from __future__ import annotations

import logging

import audioflux as af
import numpy as np
from audioflux.type import SpectralDataType, SpectralFilterBankScaleType

from backend.pipeline.detection.detector_factory import DetectorFactory
from backend.pipeline.detection.types import DetectionResult, SpeechRegion

logger = logging.getLogger(__name__)

_DETECTOR_TYPE = "spectral_flatness"
_SAMPLE_RATE = 16_000


class SpectralFlatnessDetector:
    """Spectral flatness detector with adaptive sub-band selection.

    Uses audioFlux's BFT for spectrogram extraction and Spectral.flatness()
    for per-frame flatness computation. Low flatness (tonal/speech) produces
    signal_present=True; consecutive speech frames are grouped into
    SpeechRegion objects.
    """

    def __init__(self, **kwargs: object) -> None:
        threshold = float(kwargs.get("threshold", 0.4))
        hangover_frames = int(kwargs.get("hangover_frames", 3))
        low_freq_hz = float(kwargs.get("low_freq_hz", 300.0))
        high_freq_hz = float(kwargs.get("high_freq_hz", 3400.0))
        fft_size = int(kwargs.get("fft_size", 512))
        hop_size = int(kwargs.get("hop_size", 320))

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
        nyquist = _SAMPLE_RATE / 2
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

        self._bft = af.BFT(
            num=fft_size // 2 + 1,
            samplate=_SAMPLE_RATE,
            radix2_exp=int(np.log2(fft_size)),
            slide_length=hop_size,
            data_type=SpectralDataType.MAG,
            scale_type=SpectralFilterBankScaleType.LINEAR,
        )

        fre_band_arr = self._bft.get_fre_band_arr()
        start_bin = int(np.searchsorted(fre_band_arr, low_freq_hz))
        end_bin = int(np.searchsorted(fre_band_arr, high_freq_hz))
        end_bin = min(end_bin, self._bft.num - 1)

        if start_bin >= end_bin:
            msg = (
                f"Frequency range [{low_freq_hz}, {high_freq_hz}] Hz maps to "
                f"bins [{start_bin}, {end_bin}), which is empty at "
                f"fft_size={fft_size}. Increase fft_size or widen the "
                f"frequency range."
            )
            raise ValueError(msg)

        self._spectral = af.Spectral(
            num=self._bft.num,
            fre_band_arr=fre_band_arr,
        )
        self._spectral.set_edge(start_bin, end_bin)

    @property
    def detector_type(self) -> str:
        return _DETECTOR_TYPE

    def detect(self, samples: np.ndarray) -> DetectionResult:
        if samples.size == 0 or samples.size < self._fft_size:
            return DetectionResult(speech_regions=(), detector_type=_DETECTOR_TYPE)

        audio = samples.astype(np.float32) / 32768.0

        spec_arr = np.abs(self._bft.bft(audio))
        n_time = spec_arr.shape[-1]
        if n_time == 0:
            return DetectionResult(speech_regions=(), detector_type=_DETECTOR_TYPE)

        self._spectral.set_time_length(n_time)
        flatness_arr = self._spectral.flatness(spec_arr)

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
                        start_sec=first_frame * self._hop_size / _SAMPLE_RATE,
                        end_sec=i * self._hop_size / _SAMPLE_RATE,
                        detector_type=_DETECTOR_TYPE,
                    )
                )
                in_region = False

        if in_region:
            regions.append(
                SpeechRegion(
                    start_sec=first_frame * self._hop_size / _SAMPLE_RATE,
                    end_sec=len(signal_present) * self._hop_size / _SAMPLE_RATE,
                    detector_type=_DETECTOR_TYPE,
                )
            )

        return DetectionResult(
            speech_regions=tuple(regions), detector_type=_DETECTOR_TYPE
        )


if _DETECTOR_TYPE not in DetectorFactory._registry:  # noqa: SLF001
    DetectorFactory.register(_DETECTOR_TYPE, SpectralFlatnessDetector)


if __name__ == "__main__":
    import sys

    sr = _SAMPLE_RATE
    duration = 30
    n_samples = sr * duration
    rng = np.random.default_rng(42)

    # Build 30s test audio: noise -> 440 Hz sine -> noise
    audio = np.zeros(n_samples, dtype=np.float32)
    audio[: 10 * sr] = rng.standard_normal(10 * sr).astype(np.float32) * 0.1
    t = np.arange(10 * sr, dtype=np.float32) / sr
    audio[10 * sr : 20 * sr] = np.sin(2 * np.pi * 440 * t).astype(np.float32) * 0.5
    audio[20 * sr :] = rng.standard_normal(10 * sr).astype(np.float32) * 0.1

    samples_int16 = (audio * 32767).astype(np.int16)

    detector = SpectralFlatnessDetector(threshold=0.4, hangover_frames=3)
    result = detector.detect(samples_int16)

    print(f"Detected {len(result.speech_regions)} speech region(s):")  # noqa: T201
    for region in result.speech_regions:
        print(  # noqa: T201
            f"  {region.start_sec:.3f}s - {region.end_sec:.3f}s"
        )

    # Per-frame diagnostics
    audio_f32 = samples_int16.astype(np.float32) / 32768.0
    spec = np.abs(detector._bft.bft(audio_f32))  # noqa: SLF001
    n_time = spec.shape[-1]
    detector._spectral.set_time_length(n_time)  # noqa: SLF001
    flatness = detector._spectral.flatness(spec)  # noqa: SLF001

    hop = detector._hop_size  # noqa: SLF001
    noise1_end = int(10.0 * sr / hop)
    tone_end = int(20.0 * sr / hop)
    n_frames = len(flatness)

    noise1_flatness = flatness[:noise1_end].mean() if noise1_end > 0 else 0
    tone_flatness = flatness[noise1_end:tone_end].mean() if tone_end > noise1_end else 0
    noise2_flatness = flatness[tone_end:].mean() if n_frames > tone_end else 0

    print(f"\nTotal frames: {n_frames}")  # noqa: T201
    print(f"Noise1 (0-10s) mean flatness:  {noise1_flatness:.4f}")  # noqa: T201
    print(f"Tone  (10-20s) mean flatness:  {tone_flatness:.4f}")  # noqa: T201
    print(f"Noise2 (20-30s) mean flatness: {noise2_flatness:.4f}")  # noqa: T201

    # Verify tone region detected
    tone_regions = [
        r for r in result.speech_regions if r.start_sec < 20.0 and r.end_sec > 10.0
    ]
    if not tone_regions:
        print("FAIL: Tone region (10-20s) not detected as speech")  # noqa: T201
        sys.exit(1)
    print("PASS: Tone region detected as speech")  # noqa: T201
