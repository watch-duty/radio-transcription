import numpy as np

from backend.pipeline.transcription.dsp import (
    compute_rms_energy,
    compute_spectral_flatness,
)


def test_compute_rms_energy_silence() -> None:
    """Silence should result in zero energy."""
    samples = np.zeros(4000, dtype=np.float32)
    energy = compute_rms_energy(samples)
    assert np.all(energy == 0.0)


def test_compute_rms_energy_constant_tone() -> None:
    """A constant peak-to-peak signal must evaluate accurately."""
    # A full-scale sine-like wave but statically fixed
    # Note: float32 audio max capacity is 1.0
    samples = np.full(4000, 1.0, dtype=np.float32)
    energy = compute_rms_energy(samples)

    # RMS of a constant signal 1.0 is 1.0
    assert np.allclose(energy, 1.0)


def test_compute_spectral_flatness_pure_silence() -> None:
    """A purely silent matrix must gracefully resist divide by zero logic via EPSILON."""
    samples = np.zeros(4096, dtype=np.float32)
    # The magnitude spectrogram of zeros is zero.
    # Because of our np.maximum(..., 1e-10) epsilon clamp, it shouldn't raise a Warning
    # It evaluates to 1.0 since it's perfectly flat at epsilon.
    flatness = compute_spectral_flatness(samples, sample_rate=16000)
    assert flatness.shape[0] > 0
    # Every timeframe's flatness should be roughly 1.0 (flat) due to the epsilon clamp
    assert np.allclose(flatness, 1.0, atol=1e-5)


def test_compute_spectral_flatness_noise() -> None:
    """White noise is perfectly balanced (flat) mathematically, expecting ~1.0."""
    rng = np.random.default_rng(42)
    # Gaussian noise
    samples = rng.normal(0, 0.5, 16000).astype(np.float32)
    flatness = compute_spectral_flatness(samples, sample_rate=16000)

    # Noise flatness is highly uniform
    mean_flatness = np.mean(flatness)
    assert 0.4 < mean_flatness <= 1.0


def test_compute_spectral_flatness_pure_tone() -> None:
    """A single strong frequency (sine wave) is exactly NOT flat (~0.0)."""
    # 1 second of 440 Hz
    t = np.linspace(0, 1, 16000, endpoint=False)
    sine_wave = np.sin(440 * 2 * np.pi * t)
    # multiply to get a float amplitude
    samples = (sine_wave * 0.9).astype(np.float32)

    flatness = compute_spectral_flatness(samples, sample_rate=16000)

    # Tone flatness is highly concentrated / spiky
    mean_flatness = np.mean(flatness)
    assert mean_flatness < 0.2
