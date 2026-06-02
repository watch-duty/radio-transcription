"""Stateless audio format transcoding utilities for the Normalization Cloud Function.

This module performs pure-transcoding of raw stitched audio bytes
into streaming playback M4A and lossless FLAC derivatives using standard ffmpeg.
It performs ZERO acoustic or volume preprocessing.
"""

import logging
import subprocess
import tempfile
from pathlib import Path
from typing import Any


logger = logging.getLogger(__name__)

# Audio Signal Processing Constants
FLAC_COMPRESSION_LEVEL = "5"
M4A_BITRATE = "32k"
DEFAULT_FFMPEG_TIMEOUT_SEC = 30


class AudioProcessor:
    """Stateless audio processor performing zero-preprocessing ffmpeg transcodes."""

    def __init__(self) -> None:
        pass

    def setup(self) -> None:
        """No-op setup for compatibility."""

    def transcode_to_flac(self, input_bytes: bytes) -> bytes:
        """Transcodes input audio bytes of any format (e.g., M4A, WAV, MP3) to lossless FLAC using ffmpeg."""
        with tempfile.NamedTemporaryFile(
            suffix=".flac", delete=False
        ) as temp_file:
            temp_filename = temp_file.name

        try:
            process = subprocess.run(
                [
                    "ffmpeg",
                    "-y",
                    "-i",
                    "pipe:0",
                    "-f",
                    "flac",
                    "-compression_level",
                    FLAC_COMPRESSION_LEVEL,
                    temp_filename,
                ],
                input=input_bytes,
                capture_output=True,
                check=False,
                timeout=DEFAULT_FFMPEG_TIMEOUT_SEC,
            )
            if process.returncode != 0:
                logger.error(
                    f"ffmpeg error during FLAC transcode: {process.stderr.decode()}"
                )
                msg = "Failed to transcode to FLAC via ffmpeg"
                raise RuntimeError(msg)

            with open(temp_filename, "rb") as f:
                return f.read()
        finally:
            try:
                Path(temp_filename).unlink()
            except OSError:
                pass

    def transcode_to_m4a(self, input_bytes: bytes) -> bytes:
        """Transcodes input audio bytes of any format (e.g., FLAC, WAV, MP3) to M4A (AAC) using ffmpeg."""
        with tempfile.NamedTemporaryFile(
            suffix=".m4a", delete=False
        ) as temp_file:
            temp_filename = temp_file.name

        try:
            process = subprocess.run(
                [
                    "ffmpeg",
                    "-y",
                    "-i",
                    "pipe:0",
                    "-f",
                    "ipod",
                    "-c:a",
                    "aac",
                    "-b:a",
                    M4A_BITRATE,
                    temp_filename,
                ],
                input=input_bytes,
                capture_output=True,
                check=False,
                timeout=DEFAULT_FFMPEG_TIMEOUT_SEC,
            )
            if process.returncode != 0:
                logger.error(
                    f"ffmpeg error during M4A transcode: {process.stderr.decode()}"
                )
                msg = "Failed to transcode to M4A via ffmpeg"
                raise RuntimeError(msg)

            with open(temp_filename, "rb") as f:
                return f.read()
        finally:
            try:
                Path(temp_filename).unlink()
            except OSError:
                pass


