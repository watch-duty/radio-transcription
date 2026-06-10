import numpy as np

from backend.pipeline.segmentation.audio.dsp import (
    compute_rms_energy,
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
