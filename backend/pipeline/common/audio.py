"""Audio format conversion utilities for the ingestion layer."""

from __future__ import annotations

import logging
import subprocess

from backend.pipeline.common.constants import (
    FLAC_COMPRESSION_LEVEL,
    NUM_AUDIO_CHANNELS,
    SAMPLE_RATE_HZ,
)

logger = logging.getLogger(__name__)


def convert_to_flac(audio_bytes: bytes, input_format: str) -> bytes:
    """Convert audio to canonical FLAC format (16 kHz, 16-bit, mono).

    Args:
        audio_bytes: Raw audio bytes in any supported format.
        input_format: Input audio format (e.g. ``"mp3"``, ``"wav"``).

    Returns:
        FLAC-encoded bytes at the pipeline's canonical sample rate,
        channel count, and bit depth.
    """
    process = subprocess.run(
        [
            "ffmpeg",
            "-f",
            input_format,
            "-i",
            "pipe:0",
            "-f",
            "flac",
            "-ar",
            str(SAMPLE_RATE_HZ),
            "-ac",
            str(NUM_AUDIO_CHANNELS),
            "-sample_fmt",
            "s16",
            "-compression_level",
            FLAC_COMPRESSION_LEVEL,
            "pipe:1",
        ],
        input=audio_bytes,
        capture_output=True,
        check=False,
    )
    if process.returncode != 0:
        logger.error(f"ffmpeg error: {process.stderr.decode()}")
        msg = "Failed to convert to FLAC via ffmpeg"
        raise RuntimeError(msg)
    return process.stdout



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
