"""Audio format conversion utilities for the ingestion layer."""

from __future__ import annotations

import io
import logging
import subprocess

from pydub import AudioSegment

from backend.pipeline.common.constants import (
    NUM_AUDIO_CHANNELS,
    SAMPLE_RATE_HZ,
    SAMPLE_WIDTH_16BIT,
)

logger = logging.getLogger(__name__)


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
    audio = audio.set_sample_width(SAMPLE_WIDTH_16BIT)
    buf = io.BytesIO()
    audio.export(buf, format="flac")
    return buf.getvalue()


def get_audio_duration(audio_bytes: bytes) -> int:
    """Calculate duration of audio bytes using ffprobe.

    Supports various formats like MP3, M4A, WAV, etc.

    Args:
        audio_bytes: Raw audio bytes.

    Returns:
        Duration in milliseconds.
    """
    try:
        result = subprocess.run(
            [
                "ffprobe",
                "-v",
                "error",
                "-show_entries",
                "format=duration",
                "-of",
                "default=noprint_wrappers=1:nokey=1",
                "-",
            ],
            input=audio_bytes,
            capture_output=True,
            check=True,
        )
        duration_sec = float(result.stdout.decode().strip())
        return int(duration_sec * 1000)
    except Exception as e:
        logger.exception("Failed to calculate duration using ffprobe: %s", e)
        raise
