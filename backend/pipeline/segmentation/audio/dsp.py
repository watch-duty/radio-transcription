"""Shared Digital Signal Processing (DSP) mathematical functions.

Optimized native equivalent logic without the massively heavyweight
`scikit-learn` dependencies. Used natively by both the ingestion Detection
and transcription Audio Processor pipelines.
"""

import math

import numpy as np
from numba import njit


def get_periodic_hann(window_length: int) -> np.ndarray:
    """Generates a periodic Hann window of length `window_length`."""
    return 0.5 * (
        1 - np.cos(2 * np.pi * np.arange(window_length) / window_length)
    )


def custom_numpy_stft(
    wav: np.ndarray, n_fft: int = 512, hop_length: int = 256
) -> np.ndarray:
    """Calculates Short-Time Fourier Transform (STFT) via NumPy arrays."""
    window = get_periodic_hann(n_fft)
    pad_len = n_fft // 2
    wav_padded = np.pad(wav, ((0, 0), (pad_len, pad_len)), mode="reflect")
    num_frames = 1 + (wav_padded.shape[1] - n_fft) // hop_length
    strides = (
        wav_padded.strides[0],
        wav_padded.strides[1] * hop_length,
        wav_padded.strides[1],
    )
    frames = np.lib.stride_tricks.as_strided(
        wav_padded, shape=(wav.shape[0], num_frames, n_fft), strides=strides
    )
    windowed = frames * window
    spec = np.fft.rfft(windowed, n=n_fft, axis=-1)
    spec = np.transpose(spec, (0, 2, 1))
    return np.stack((np.real(spec), np.imag(spec)), axis=-1).astype(np.float32)


@njit(fastmath=True)
def _ola_step(
    frames: np.ndarray,
    window: np.ndarray,
    window_sq: np.ndarray,
    out: np.ndarray,
    window_sum: np.ndarray,
    num_frames: int,
    hop_length: int,
    n_fft: int,
) -> None:
    """Accelerates the overlap-add operation for the inverse STFT execution via Numba."""
    batch_size = frames.shape[0]
    for b in range(batch_size):
        for i in range(num_frames):
            start = i * hop_length
            for j in range(n_fft):
                out[b, start + j] += frames[b, i, j] * window[j]
                window_sum[b, start + j] += window_sq[j]


def custom_numpy_istft(
    spec_stacked: np.ndarray,
    length: int,
    n_fft: int = 512,
    hop_length: int = 256,
) -> np.ndarray:
    """Calculates the inverse Short-Time Fourier Transform (ISTFT) using Overlap-Add (OLA)."""
    window = get_periodic_hann(n_fft).astype(np.float32)
    spec_complex = spec_stacked[..., 0] + 1j * spec_stacked[..., 1]
    spec_complex = np.transpose(spec_complex, (0, 2, 1))
    frames = np.fft.irfft(spec_complex, n=n_fft, axis=-1).astype(np.float32)
    batch_size, num_frames, _ = frames.shape
    expected_len = (num_frames - 1) * hop_length + n_fft
    out = np.zeros((batch_size, expected_len), dtype=np.float32)
    window_sum = np.zeros((batch_size, expected_len), dtype=np.float32)
    window_sq = (window**2).astype(np.float32)

    _ola_step(
        frames,
        window,
        window_sq,
        out,
        window_sum,
        num_frames,
        hop_length,
        n_fft,
    )

    window_sum[window_sum < 1e-10] = 1.0
    out = out / window_sum
    pad_len = n_fft // 2
    return out[:, pad_len : pad_len + length]


class TorchaudioHannResampler:
    """Resampling implementation equivalent to Torchaudio's Hann-window sinc method."""

    def __init__(
        self,
        orig_freq: int,
        new_freq: int,
        lowpass_filter_width: int = 6,
        rolloff: float = 0.99,
    ) -> None:
        self.orig_freq = orig_freq
        self.new_freq = new_freq
        self.lowpass_filter_width = lowpass_filter_width
        self.gcd = math.gcd(orig_freq, new_freq)
        self.down = orig_freq // self.gcd
        self.up = new_freq // self.gcd
        self.kernel, self.width = self._get_sinc_resample_kernel(
            self.down, self.up, lowpass_filter_width, rolloff
        )

    def _get_sinc_resample_kernel(
        self,
        orig_freq: int,
        new_freq: int,
        lowpass_filter_width: int,
        rolloff: float,
    ) -> tuple[np.ndarray, int]:
        base_freq = min(orig_freq, new_freq) * rolloff
        width = math.ceil(lowpass_filter_width * orig_freq / base_freq)
        idx = (
            np.arange(-width, width + orig_freq, dtype=np.float64)[
                None, None, :
            ]
            / orig_freq
        )
        t = (
            np.arange(0, -new_freq, -1, dtype=np.float64)[:, None, None]
            / new_freq
            + idx
        )
        t *= base_freq
        t = np.clip(t, -lowpass_filter_width, lowpass_filter_width)
        window = np.cos(t * math.pi / lowpass_filter_width / 2) ** 2
        t_pi = t * math.pi
        scale = base_freq / orig_freq
        kernels = np.zeros_like(t_pi)
        mask = t_pi == 0
        kernels[mask] = 1.0
        kernels[~mask] = np.sin(t_pi[~mask]) / t_pi[~mask]
        kernels *= window * scale
        return kernels.astype(np.float64), width

    def resample(self, waveform: np.ndarray) -> np.ndarray:
        """Resamples input 1D or 2D waveform to target frequency."""
        is_1d = waveform.ndim == 1
        if is_1d:
            waveform = waveform[np.newaxis, :]
        orig_len = waveform.shape[-1]
        wav_padded = np.pad(
            waveform.astype(np.float64),
            ((0, 0), (self.width, self.width + self.down)),
            mode="constant",
        )
        batch, length = wav_padded.shape
        kernel_size = self.kernel.shape[2]
        num_frames = (length - kernel_size) // self.down + 1
        strides = (
            wav_padded.strides[0],
            wav_padded.strides[1] * self.down,
            wav_padded.strides[1],
        )
        frames = np.lib.stride_tricks.as_strided(
            wav_padded,
            shape=(batch, num_frames, kernel_size),
            strides=strides,
        )
        resampled = np.tensordot(frames, self.kernel[:, 0, :], axes=([2], [1]))
        resampled = resampled.reshape(batch, -1)
        target_length = math.ceil(self.up * orig_len / self.down)
        resampled = resampled[:, :target_length]
        if is_1d:
            return resampled[0].astype(np.float32)
        return resampled.astype(np.float32)
