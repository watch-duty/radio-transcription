"""Audio format conversion utilities for the ingestion layer."""

from __future__ import annotations

import io

from pydub import AudioSegment

from backend.pipeline.common.constants import (
    NUM_AUDIO_CHANNELS,
    SAMPLE_RATE_HZ,
)

# 16-bit PCM sample width in bytes
_SAMPLE_WIDTH_16BIT = 2


def convert_to_flac(audio_bytes: bytes, input_format: str) -> bytes:
    """Convert audio to canonical FLAC format (16 kHz, 16-bit, mono).

    Args:
        audio_bytes: Raw audio bytes in any pydub-supported format.
        input_format: Input audio format (e.g. ``"mp3"``, ``"wav"``).

    Returns:
        FLAC-encoded bytes at the pipeline's canonical sample rate,
        channel count, and bit depth.
    """
    audio = AudioSegment.from_file(io.BytesIO(audio_bytes), format=input_format)
    audio = audio.set_frame_rate(SAMPLE_RATE_HZ)
    audio = audio.set_channels(NUM_AUDIO_CHANNELS)
    audio = audio.set_sample_width(_SAMPLE_WIDTH_16BIT)
    buf = io.BytesIO()
    audio.export(buf, format="flac")
    return buf.getvalue()
