"""Stateless audio format transcoding utilities for the Normalization Cloud Function.

This module performs pure-transcoding of raw stitched audio bytes
into streaming playback M4A and lossless FLAC derivatives using standard ffmpeg.
It performs ZERO acoustic or volume preprocessing.
"""

import logging
import subprocess
import tempfile
from pathlib import Path

logger = logging.getLogger(__name__)

# Audio Signal Processing Constants
FLAC_COMPRESSION_LEVEL = "5"
M4A_BITRATE = "32k"
DEFAULT_FFMPEG_TIMEOUT_SEC = 30


import contextlib
from typing import Iterator

class AudioProcessor:
    """Stateless audio processor performing zero-preprocessing ffmpeg transcodes."""

    def __init__(self) -> None:
        pass

    def setup(self) -> None:
        """No-op setup for compatibility."""

    @contextlib.contextmanager
    def _temp_files(self, *suffixes: str) -> Iterator[str | list[str]]:
        """Context manager to safely create and cleanup temporary files."""
        paths = []
        try:
            for suffix in suffixes:
                with tempfile.NamedTemporaryFile(suffix=suffix, delete=False) as f:
                    paths.append(f.name)
            
            if len(paths) == 1:
                yield paths[0]
            else:
                yield paths
        finally:
            for path in paths:
                try:
                    Path(path).unlink()
                except OSError:
                    pass

    def _execute_ffmpeg(
        self, cmd: list[str], input_bytes: bytes, err_msg: str
    ) -> None:
        """Helper to execute ffmpeg subprocess with timeout and safety checks."""
        process = subprocess.run(
            cmd,
            input=input_bytes,
            capture_output=True,
            check=False,
            timeout=DEFAULT_FFMPEG_TIMEOUT_SEC,
        )
        if process.returncode != 0:
            logger.error(
                f"ffmpeg error during transcode: {process.stderr.decode(errors='replace')}"
            )
            raise RuntimeError(err_msg)

    def transcode_to_flac(self, input_bytes: bytes) -> bytes:
        """Transcodes input audio bytes of any format to lossless FLAC using ffmpeg."""
        with self._temp_files(".flac") as temp_filename:
            self._execute_ffmpeg(
                [
                    "ffmpeg",
                    "-y",
                    "-i", "pipe:0",
                    "-f", "flac",
                    "-compression_level", FLAC_COMPRESSION_LEVEL,
                    temp_filename,
                ],
                input_bytes,
                "Failed to transcode to FLAC via ffmpeg",
            )
            with open(temp_filename, "rb") as f:
                return f.read()

    def transcode_to_mono_flac(self, input_bytes: bytes) -> bytes:
        """Transcodes input audio bytes to a 1D mono downmixed FLAC using ffmpeg."""
        with self._temp_files(".flac") as temp_filename:
            self._execute_ffmpeg(
                [
                    "ffmpeg",
                    "-y",
                    "-i", "pipe:0",
                    "-ac", "1",
                    "-f", "flac",
                    "-compression_level", FLAC_COMPRESSION_LEVEL,
                    temp_filename,
                ],
                input_bytes,
                "Failed to transcode to mono FLAC via ffmpeg",
            )
            with open(temp_filename, "rb") as f:
                return f.read()

    def transcode_to_m4a(self, input_bytes: bytes) -> bytes:
        """Transcodes input audio bytes of any format to M4A (AAC) using ffmpeg."""
        with self._temp_files(".m4a") as temp_filename:
            self._execute_ffmpeg(
                [
                    "ffmpeg",
                    "-y",
                    "-i", "pipe:0",
                    "-f", "ipod",
                    "-c:a", "aac",
                    "-b:a", M4A_BITRATE,
                    "-movflags", "+faststart",
                    temp_filename,
                ],
                input_bytes,
                "Failed to transcode to M4A via ffmpeg",
            )
            with open(temp_filename, "rb") as f:
                return f.read()

    def transcode_derivatives(self, input_bytes: bytes) -> tuple[bytes, bytes]:
        """Transcodes input audio bytes to FLAC and M4A simultaneously."""
        with self._temp_files(".flac", ".m4a") as (flac_name, m4a_name):
            self._execute_ffmpeg(
                [
                    "ffmpeg",
                    "-y",
                    "-i", "pipe:0",
                    "-f", "flac",
                    "-compression_level", FLAC_COMPRESSION_LEVEL,
                    flac_name,
                    "-f", "ipod",
                    "-c:a", "aac",
                    "-b:a", M4A_BITRATE,
                    "-movflags", "+faststart",
                    m4a_name,
                ],
                input_bytes,
                "Failed to transcode audio derivatives via ffmpeg",
            )
            
            with open(flac_name, "rb") as f:
                flac_bytes = f.read()
            with open(m4a_name, "rb") as f:
                m4a_bytes = f.read()

            return flac_bytes, m4a_bytes
