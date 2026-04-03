"""Signal processing utilities for audio characterization."""

import logging
from dataclasses import dataclass

import librosa
import numpy as np
from scipy.signal import butter, lfilter

from backend.pipeline.common.constants import MS_PER_SECOND
from backend.pipeline.transcription.constants import (
    SIGNAL_ANALYZER_DEFAULT_VARIANCE,
    SIGNAL_ANALYZER_FMAX,
    SIGNAL_ANALYZER_FMIN,
    SIGNAL_ANALYZER_HOP_LENGTH,
    SIGNAL_ANALYZER_MIN_FRAMES,
    SIGNAL_ANALYZER_SLOPE_VARIANCE_THRESHOLD,
    SIGNAL_ANALYZER_VOICED_PROB_HIGH_THRESHOLD,
    SIGNAL_ANALYZER_VOICED_PROB_THRESHOLD,
)

logger = logging.getLogger(__name__)


@dataclass
class AudioCharacterization:
    label: str  # 'stochastic', 'deterministic_linear', 'deterministic_static', or 'void'
    is_transcribable: bool
    confidence: float
    hnr: float = 0.0
    trimmed_duration_ms: int = 0


class RadioSignalAnalyzer:
    """Analyzes audio signals to characterize them using pYIN and HNR.

    This class is used to distinguish between speech, static, and tonal noise.
    """

    def __init__(self, sample_rate=16000, cutoff_freq=4000):
        self.fs = sample_rate
        self.cutoff_freq = cutoff_freq
        self.nyquist = self.fs / 2
        self.order = 4

    def _butter_lowpass(self):
        normalized_cutoff = self.cutoff_freq / self.nyquist
        b, a = butter(self.order, normalized_cutoff, btype="low", analog=False)
        return b, a

    def _apply_filter(self, data):
        b, a = self._butter_lowpass()
        return lfilter(b, a, data)

    def characterize(self, audio_data) -> AudioCharacterization:
        """Characterizes the audio segment using pYIN and HNR.

        Args:
            audio_data: The audio samples to analyze.

        Returns:
            AudioCharacterization object with label and confidence.
        """
        # Pre-filter to remove noise above speech range
        clean_audio = self._apply_filter(audio_data)

        # 1. pYIN Pitch Tracking
        f0, _voiced_flag, voiced_probs = librosa.pyin(
            clean_audio,
            fmin=SIGNAL_ANALYZER_FMIN,
            fmax=SIGNAL_ANALYZER_FMAX,
            sr=self.fs,
            hop_length=SIGNAL_ANALYZER_HOP_LENGTH,
        )

        # 2. HNR Calculation via HPSS
        y_harmonic, y_percussive = librosa.effects.hpss(audio_data)
        harmonic_energy = np.sum(y_harmonic**2)
        noise_energy = np.sum(y_percussive**2)
        hnr = harmonic_energy / (noise_energy + 1e-10)

        # Calculate heuristics
        valid_f0 = f0[voiced_probs > SIGNAL_ANALYZER_VOICED_PROB_HIGH_THRESHOLD]
        # Use max probability instead of mean to prevent noise dilution in merged segments
        mean_prob = np.max(voiced_probs) if len(voiced_probs) > 0 else 0.0

        # Calculate trimmed duration based on threshold
        voiced_indices = np.where(voiced_probs > SIGNAL_ANALYZER_VOICED_PROB_THRESHOLD)[0]
        if len(voiced_indices) > 0:
            last_voiced_frame = voiced_indices[-1]
            trimmed_duration_ms = int(
                last_voiced_frame * SIGNAL_ANALYZER_HOP_LENGTH * MS_PER_SECOND / self.fs
            )
        else:
            trimmed_duration_ms = 0

        pitch_slope = np.diff(valid_f0) if len(valid_f0) > 1 else []
        slope_variance = np.var(pitch_slope) if len(pitch_slope) > 0 else SIGNAL_ANALYZER_DEFAULT_VARIANCE

        if len(valid_f0) >= SIGNAL_ANALYZER_MIN_FRAMES and slope_variance < SIGNAL_ANALYZER_SLOPE_VARIANCE_THRESHOLD:
            return AudioCharacterization(
                "deterministic_linear",
                False,
                mean_prob,
                hnr,
                trimmed_duration_ms,
            )

        return AudioCharacterization(
            "stochastic", True, mean_prob, hnr, trimmed_duration_ms
        )
