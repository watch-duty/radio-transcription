"""Signal processing utilities for audio characterization."""

import logging
from dataclasses import dataclass
from enum import Enum

import librosa
import numpy as np
from scipy.signal import butter, lfilter

from backend.pipeline.common.constants import MS_PER_SECOND

# --- Local Constants ---
SIGNAL_ANALYZER_FMIN = 100
SIGNAL_ANALYZER_FMAX = 1500
SIGNAL_ANALYZER_HOP_LENGTH = 512
SIGNAL_ANALYZER_VOICED_PROB_THRESHOLD = 0.2
SIGNAL_ANALYZER_VOICED_PROB_HIGH_THRESHOLD = 0.6
SIGNAL_ANALYZER_SLOPE_VARIANCE_THRESHOLD = 5.0
SIGNAL_ANALYZER_MIN_FRAMES = 10
SIGNAL_ANALYZER_DEFAULT_VARIANCE = 100.0
VAD_HIGHPASS_FREQ = 300

DEFAULT_SAMPLE_RATE = 16000
DEFAULT_CUTOFF_FREQ = 4000
BUTTERWORTH_ORDER = 4
FFT_PADDING_FACTOR = 2
ZERO_DIVISION_EPSILON = 1e-10
HNR_SIGMA_MIN = 0.0001
HNR_SIGMA_MAX = 0.9999
DB_SCALE_FACTOR = 10
DEFAULT_AGC_TARGET_RMS = 0.05
DEFAULT_AGC_MAX_GAIN = 5.0
DEFAULT_AGC_WINDOW_MS = 30
GAUSSIAN_SMOOTHING_SIGMA = 2.0
LIMITER_MIN = -0.99
LIMITER_MAX = 0.99
INT16_NORMALIZATION_FACTOR = 32768.0
SPEECH_VERIFICATION_HNR_THRESHOLD = 3.0
SPEECH_VERIFICATION_CONFIDENCE_THRESHOLD = 0.5
# -----------------------

logger = logging.getLogger(__name__)


class SignalLabel(Enum):
    STOCHASTIC = "stochastic"
    DETERMINISTIC_LINEAR = "deterministic_linear"
    DETERMINISTIC_STATIC = "deterministic_static"
    VOID = "void"


@dataclass
class AudioCharacterization:
    label: SignalLabel
    is_transcribable: bool
    confidence: float
    hnr: float = 0.0
    trimmed_duration_ms: int = 0


class RadioSignalAnalyzer:
    """Analyzes audio signals to characterize them using pYIN and HNR.

    This class is used to distinguish between speech, static, and tonal noise.
    """

    def __init__(
        self, sample_rate=DEFAULT_SAMPLE_RATE, cutoff_freq=DEFAULT_CUTOFF_FREQ
    ):
        self.fs = sample_rate
        self.cutoff_freq = cutoff_freq
        self.nyquist = self.fs / 2
        self.order = BUTTERWORTH_ORDER

    def _butter_lowpass(self):
        normalized_cutoff = self.cutoff_freq / self.nyquist
        b, a = butter(self.order, normalized_cutoff, btype="low", analog=False)
        return b, a

    def _apply_filter(self, data):
        b, a = self._butter_lowpass()
        return lfilter(b, a, data)

    def _calculate_hnr_fft(self, audio_data):
        """Fast FFT-based HNR. Replaces slow HPSS logic."""
        sig = audio_data - np.mean(audio_data)
        sig *= np.hanning(len(sig))
        n = len(sig)
        f_sig = np.fft.rfft(sig, n=FFT_PADDING_FACTOR * n)
        autocorr = np.fft.irfft(f_sig * np.conj(f_sig))[:n]
        if autocorr[0] <= ZERO_DIVISION_EPSILON:
            return 0.0
        autocorr /= autocorr[0]
        lower, upper = (
            int(self.fs / SIGNAL_ANALYZER_FMAX),
            int(self.fs / VAD_HIGHPASS_FREQ),
        )
        r_max = np.max(autocorr[lower:upper])
        r_safe = np.clip(r_max, HNR_SIGMA_MIN, HNR_SIGMA_MAX)
        return DB_SCALE_FACTOR * np.log10(r_safe / (1 - r_safe))

    def characterize(self, audio_data) -> AudioCharacterization:
        """Characterizes the audio segment using pYIN, HNR, and Spectral Flatness.

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

        # 2. Fast HNR Calculation
        hnr = self._calculate_hnr_fft(clean_audio)

        # Calculate heuristics
        valid_f0 = f0[voiced_probs > SIGNAL_ANALYZER_VOICED_PROB_HIGH_THRESHOLD]
        # Use max probability instead of mean to prevent noise dilution in merged segments
        mean_prob = np.max(voiced_probs) if len(voiced_probs) > 0 else 0.0

        # Calculate trimmed duration based on threshold
        voiced_indices = np.where(
            voiced_probs > SIGNAL_ANALYZER_VOICED_PROB_THRESHOLD
        )[0]
        if len(voiced_indices) > 0:
            last_voiced_frame = voiced_indices[-1]
            trimmed_duration_ms = int(
                last_voiced_frame
                * SIGNAL_ANALYZER_HOP_LENGTH
                * MS_PER_SECOND
                / self.fs
            )
        else:
            trimmed_duration_ms = 0

        pitch_slope = np.diff(valid_f0) if len(valid_f0) > 1 else []
        slope_variance = (
            np.var(pitch_slope)
            if len(pitch_slope) > 0
            else SIGNAL_ANALYZER_DEFAULT_VARIANCE
        )

        if (
            len(valid_f0) >= SIGNAL_ANALYZER_MIN_FRAMES
            and slope_variance < SIGNAL_ANALYZER_SLOPE_VARIANCE_THRESHOLD
        ):
            return AudioCharacterization(
                SignalLabel.DETERMINISTIC_LINEAR,
                False,
                mean_prob,
                hnr,
                trimmed_duration_ms,
            )

        return AudioCharacterization(
            SignalLabel.STOCHASTIC, True, mean_prob, hnr, trimmed_duration_ms
        )


def apply_sliding_agc(
    samples: np.ndarray,
    sample_rate: int = DEFAULT_SAMPLE_RATE,
    target_rms: float = DEFAULT_AGC_TARGET_RMS,
    max_gain: float = DEFAULT_AGC_MAX_GAIN,
    window_ms: int = DEFAULT_AGC_WINDOW_MS,
) -> np.ndarray:
    """Applies a fast, stateless sliding-window AGC to level audio volume.
    Operates entirely in float32 space [-1.0, 1.0].
    """
    from scipy.ndimage import gaussian_filter1d

    # 1. Standardize input to float32
    if samples.dtype != np.float32:
        audio_float = samples.astype(np.float32) / INT16_NORMALIZATION_FACTOR
    else:
        audio_float = np.copy(samples)

    window_size = int((window_ms / float(MS_PER_SECOND)) * sample_rate)
    num_chunks = len(audio_float) // window_size

    if num_chunks == 0:
        return audio_float  # Audio is too short to level

    # 2. Reshape into chunks and calculate local RMS energy
    usable_length = num_chunks * window_size
    reshaped = audio_float[:usable_length].reshape((num_chunks, window_size))

    # Add 1e-10 to prevent divide-by-zero on pure digital silence
    chunk_rms = np.sqrt(np.mean(reshaped**2, axis=1) + ZERO_DIVISION_EPSILON)

    # 3. Calculate target gain per chunk
    chunk_gains = target_rms / chunk_rms

    # Clamp to max_gain
    chunk_gains = np.clip(chunk_gains, 0.0, max_gain)

    # 4. Smooth the gain envelope
    smoothed_gains = gaussian_filter1d(chunk_gains, sigma=2.0)

    # 5. Interpolate chunk gains back to per-sample gains
    x_chunks = np.arange(num_chunks) * window_size + (window_size // 2)
    x_samples = np.arange(len(audio_float))

    # Pad the ends to ensure the interpolation reaches the absolute edges of the array
    x_chunks = np.pad(x_chunks, (1, 1), mode="edge")
    x_chunks[0] = 0
    x_chunks[-1] = len(audio_float) - 1
    smoothed_gains_padded = np.pad(smoothed_gains, (1, 1), mode="edge")

    # Linear interpolation for sample-perfect gain curves
    sample_gains = np.interp(x_samples, x_chunks, smoothed_gains_padded)

    # 6. Apply the gain and apply a hard limiter to prevent digital clipping
    agc_samples = audio_float * sample_gains
    agc_samples = np.clip(agc_samples, LIMITER_MIN, LIMITER_MAX)

    return agc_samples


def verify_speech_segment(audio_buffer: np.ndarray) -> bool:
    """Heuristic check to see if there is tonality (speech) in the buffer.

    Uses RadioSignalAnalyzer to evaluate pYIN pitch tracking and HNR.
    """
    samples = audio_buffer.astype(np.float32) / INT16_NORMALIZATION_FACTOR
    if len(samples) == 0:
        return False

    analyzer = RadioSignalAnalyzer()
    characterization = analyzer.characterize(samples)

    if characterization.label == SignalLabel.DETERMINISTIC_LINEAR:
        logger.info(
            "verify_speech_segment: Tonal noise (horn/siren) detected. Rejecting."
        )
        return False

    # Heuristic for speech: high HNR and high confidence (voiced probability)
    if (
        characterization.hnr > SPEECH_VERIFICATION_HNR_THRESHOLD
        and characterization.confidence
        > SPEECH_VERIFICATION_CONFIDENCE_THRESHOLD
        and characterization.trimmed_duration_ms > 0
    ):
        logger.info(
            "verify_speech_segment: Speech detected (HNR: %.1f, Confidence: %.2f, Duration: %d ms). Approving.",
            characterization.hnr,
            characterization.confidence,
            characterization.trimmed_duration_ms,
        )
        return True

    logger.info(
        "verify_speech_segment: Signal appears to be static or noise (HNR: %.1f, Confidence: %.2f). Rejecting.",
        characterization.hnr,
        characterization.confidence,
    )
    return False
