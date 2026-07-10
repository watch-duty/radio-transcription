"""Stateless audio format transcoding utilities for the Normalization Cloud Function.

This module performs pure-transcoding of raw stitched audio bytes
into streaming playback M4A and lossless FLAC derivatives using standard ffmpeg.
It performs ZERO acoustic or volume preprocessing.
"""

import contextlib
import io
import logging
import subprocess
import tempfile
from collections.abc import Generator
from dataclasses import dataclass
from pathlib import Path

import soundfile as sf

logger = logging.getLogger(__name__)

# Audio Signal Processing Constants
FLAC_COMPRESSION_LEVEL = "5"
M4A_BITRATE = "32k"
DEFAULT_FFMPEG_TIMEOUT_SEC = 30


@dataclass(frozen=True)
class TranscodeResult:
    """Strongly typed container for ffmpeg audio derivatives."""

    flac_bytes: bytes
    m4a_bytes: bytes


class AudioProcessor:
    """Stateless audio processor performing zero-preprocessing ffmpeg transcodes."""

    def __init__(self) -> None:
        pass

    def setup(self) -> None:
        """No-op setup for compatibility."""

    @contextlib.contextmanager
    def _temp_files(self, *suffixes: str) -> Generator[list[str]]:
        """Context manager to safely create and cleanup temporary files."""
        paths = []
        try:
            for suffix in suffixes:
                with tempfile.NamedTemporaryFile(
                    suffix=suffix, delete=False
                ) as f:
                    paths.append(f.name)

            yield paths
        finally:
            for path in paths:
                Path(path).unlink(missing_ok=True)

    def _execute_ffmpeg(
        self, cmd: list[str], input_bytes: bytes, op_label: str, err_msg: str
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
                f"ffmpeg error during {op_label}: {process.stderr.decode(errors='replace')}"
            )
            raise RuntimeError(err_msg)

    def transcode_to_flac(self, input_bytes: bytes) -> bytes:
        """Transcodes input audio bytes of any format to lossless FLAC using ffmpeg."""
        with self._temp_files(".flac") as (temp_filename,):
            self._execute_ffmpeg(
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
                input_bytes,
                "FLAC transcode",
                "Failed to transcode to FLAC via ffmpeg",
            )
            with open(temp_filename, "rb") as f:
                return f.read()

    def is_mono(self, input_bytes: bytes) -> bool:
        """Returns True if the given FLAC bytes represent a mono audio track."""
        with io.BytesIO(input_bytes) as flac_io:
            return sf.info(flac_io).channels <= 1

    def downmix_to_mono(self, input_bytes: bytes) -> bytes:
        """Always downmixes to mono FLAC via ffmpeg. Caller should check is_mono() first."""
        with self._temp_files(".flac") as (temp_filename,):
            self._execute_ffmpeg(
                [
                    "ffmpeg",
                    "-y",
                    "-i",
                    "pipe:0",
                    "-ac",
                    "1",
                    "-f",
                    "flac",
                    "-compression_level",
                    FLAC_COMPRESSION_LEVEL,
                    temp_filename,
                ],
                input_bytes,
                "mono FLAC transcode",
                "Failed to transcode to mono FLAC via ffmpeg",
            )
            with open(temp_filename, "rb") as f:
                return f.read()

    def transcode_to_m4a(self, input_bytes: bytes) -> bytes:
        """Transcodes input audio bytes of any format to M4A (AAC) using ffmpeg."""
        with self._temp_files(".m4a") as (temp_filename,):
            self._execute_ffmpeg(
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
                    "-movflags",
                    "frag_keyframe+empty_moov+default_base_moof",
                    temp_filename,
                ],
                input_bytes,
                "M4A transcode",
                "Failed to transcode to M4A via ffmpeg",
            )
            with open(temp_filename, "rb") as f:
                return f.read()

    def transcode_derivatives(self, input_bytes: bytes) -> TranscodeResult:
        """Transcodes input audio bytes to FLAC and M4A simultaneously."""
        with self._temp_files(".flac", ".m4a") as (flac_name, m4a_name):
            self._execute_ffmpeg(
                [
                    "ffmpeg",
                    "-y",
                    "-i",
                    "pipe:0",
                    "-f",
                    "flac",
                    "-compression_level",
                    FLAC_COMPRESSION_LEVEL,
                    flac_name,
                    "-f",
                    "ipod",
                    "-c:a",
                    "aac",
                    "-b:a",
                    M4A_BITRATE,
                    "-movflags",
                    "frag_keyframe+empty_moov+default_base_moof",
                    m4a_name,
                ],
                input_bytes,
                "derivatives transcode",
                "Failed to transcode audio derivatives via ffmpeg",
            )

            with open(flac_name, "rb") as f:
                flac_bytes = f.read()
            with open(m4a_name, "rb") as f:
                m4a_bytes = f.read()

            return TranscodeResult(flac_bytes=flac_bytes, m4a_bytes=m4a_bytes)
