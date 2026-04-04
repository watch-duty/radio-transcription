"""Audio format conversion utilities for the ingestion layer."""

from __future__ import annotations

import io

import subprocess

from backend.pipeline.common.constants import (
    NUM_AUDIO_CHANNELS,
    SAMPLE_RATE_HZ,
)


def convert_to_flac(audio_bytes: bytes, input_format: str) -> bytes:
    """Convert audio to canonical FLAC format (16 kHz, 16-bit, mono).

    Args:
        audio_bytes: Raw audio bytes in any pydub-supported format.
        input_format: Input audio format (e.g. ``"mp3"``, ``"wav"``).

    Returns:
        FLAC-encoded bytes at the pipeline's canonical sample rate,
        channel count, and bit depth.
    """
    process = subprocess.Popen(
        [
            "ffmpeg",
            "-i",
            "pipe:0",  # Read from stdin
            "-f",
            "flac",  # Output format
            "-ar",
            str(SAMPLE_RATE_HZ),  # Sample rate
            "-ac",
            str(NUM_AUDIO_CHANNELS),  # Channels
            "-sample_fmt",
            "s16",  # 16-bit depth
            "pipe:1",  # Write to stdout
        ],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    stdout, stderr = process.communicate(input=audio_bytes)
    if process.returncode != 0:
        raise Exception(f"FFmpeg failed: {stderr.decode()}")
    return stdout
