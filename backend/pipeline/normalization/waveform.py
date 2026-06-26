"""Downsampled waveform peak extraction for timeline rendering."""

import io
import math

import numpy as np
import soundfile as sf

from backend.services.audio_segments.models import WaveformAnnotationData

MAX_BIN_SIZE_SECONDS = 0.5
PEAK_DECIMALS = 3


def compute_waveform(flac_bytes: bytes) -> WaveformAnnotationData:
    """Downsamples FLAC audio into per-bucket peak amplitudes in [0, 1]."""
    samples, sample_rate = sf.read(io.BytesIO(flac_bytes), dtype="float32")
    if samples.size == 0:
        msg = "cannot compute a waveform for empty audio"
        raise ValueError(msg)
    if samples.ndim > 1:
        samples = np.abs(samples).max(axis=1)

    duration_seconds = len(samples) / sample_rate
    num_buckets = math.ceil(duration_seconds / MAX_BIN_SIZE_SECONDS)

    magnitudes = np.abs(samples)
    peaks = [
        round(float(bucket.max()), PEAK_DECIMALS)
        for bucket in np.array_split(magnitudes, num_buckets)
    ]
    return WaveformAnnotationData(
        peaks=[peaks], duration_seconds=duration_seconds
    )
