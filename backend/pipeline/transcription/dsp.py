"""Shared Digital Signal Processing (DSP) mathematical functions.

Optimized native equivalent logic without the massively heavyweight
`scikit-learn` dependencies. Used natively by both the ingestion Detection
and transcription Audio Processor pipelines.
"""

import numpy as np
from scipy.signal import stft
from scipy.stats import gmean

from backend.pipeline.transcription.constants import (
    DEFAULT_SED_FFT_SIZE,
    DEFAULT_SED_HOP_SIZE,
)


def compute_rms_energy(
    samples: np.ndarray,
    hop_length: int = DEFAULT_SED_HOP_SIZE,
    frame_length: int = DEFAULT_SED_FFT_SIZE,
) -> np.ndarray:
    """Computes the root-mean-square (RMS) energy for each frame of an audio signal.

    Computes the standard RMS math across a sliding window.

    Args:
        samples: A 1D float32 numpy array.
        hop_length: Number of samples between frames.
        frame_length: The length of the analysis window.

    Returns:
        A 1D numpy array of RMS energy values, aligned with the sliding frames.
    """
    if len(samples) == 0:
        return np.array([], dtype=np.float32)

    # Pad mode 'reflect' to center frames tightly
    pad_len = frame_length // 2
    y_padded = np.pad(samples, pad_len, mode="reflect")

    # Convert to sliding window view
    num_frames = 1 + (len(y_padded) - frame_length) // hop_length
    if num_frames <= 0:
        return np.array([], dtype=np.float32)

    shape = (num_frames, frame_length)
    strides = (y_padded.strides[0] * hop_length, y_padded.strides[0])
    frames = np.lib.stride_tricks.as_strided(y_padded, shape=shape, strides=strides)

    # Compute RMS natively
    mean_sq = np.mean(frames**2, axis=-1)
    return np.sqrt(mean_sq)


def compute_spectral_flatness(
    samples: np.ndarray,
    sample_rate: int,
    n_fft: int = DEFAULT_SED_FFT_SIZE,
    hop_length: int = DEFAULT_SED_HOP_SIZE,
    power: float = 2.0,
    amin: float = 1e-10,
    freq_mask: np.ndarray | None = None,
) -> np.ndarray:
    """Computes spectral flatness (Wiener entropy) for each frame.

    Standard natively optimized sliding-window flatness algorithm.

    Args:
        samples: A 1D float32 numpy array.
        sample_rate: Audio sampling rate.
        n_fft: Length of the FFT window.
        hop_length: Number of samples between frames.
        power: Exponent for the magnitude spectrogram (1=magnitude, 2=power). Default is 2.
        amin: Minimum value floor to prevent math domain error (log(0)).
        freq_mask: Optional boolean array to isolate specific FFT bins (e.g., 300Hz-3400Hz).

    Returns:
        1D numpy array of spectral flatness values per frame in range [0.0, 1.0].
    """
    if len(samples) == 0:
        return np.array([], dtype=np.float32)

    # STFT parameterization roughly aligns with standard hanning reflection boundaries
    _, _, zxx = stft(
        samples,
        fs=sample_rate,
        window="hann",
        nperseg=n_fft,
        noverlap=n_fft - hop_length,
        boundary="zeros",
        padded=True,
    )

    spec = np.abs(zxx)
    if power != 1.0:
        spec = spec**power

    if freq_mask is not None:
        spec = spec[freq_mask, :]

    n_time = spec.shape[1]
    if n_time == 0:
        return np.array([], dtype=np.float32)

    # Floor tiny values
    spec = np.maximum(spec, amin)

    # Flatness is Geometric mean / Arithmetic mean
    geo = gmean(spec, axis=0)
    arith = np.mean(spec, axis=0)

    # Return pure 1.0 for entirely silent (arith==0) frames
    return np.where(arith > 0, geo / arith, 1.0)
