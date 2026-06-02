"""Stateless audio format transcoding utilities for the Normalization Cloud Function.

This module performs pure-transcoding of raw stitched audio buffers into lossless FLAC
and streaming playback M4A derivatives using standard ffmpeg. It performs ZERO acoustic
or volume preprocessing.
"""

import logging
import subprocess
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
from google.cloud import storage

logger = logging.getLogger(__name__)

# Audio Signal Processing Constants
FLAC_COMPRESSION_LEVEL = "5"
M4A_BITRATE = "32k"
DEFAULT_FFMPEG_TIMEOUT_SEC = 30


@dataclass(frozen=True)
class ProcessorOutput:
    """A strongly-typed container holding the output of AudioProcessor.process_buffer."""

    success: bool
    flac_bytes: bytes | None = None
    processed_audio: np.ndarray | None = None


class AudioProcessor:
    """Stateless audio processor performing zero-preprocessing ffmpeg transcodes."""

    def __init__(
        self,
        gcs_client_instance: storage.Client | None = None,
    ) -> None:
        self.gcs_client = gcs_client_instance

    def setup(self) -> None:
        """Initializes GCS client if not provided."""
        if self.gcs_client is None:
            self.gcs_client = storage.Client()

    def export_flac(self, audio_buffer: np.ndarray, sample_rate: int) -> bytes:
        """Exports raw s16le PCM NumPy array to FLAC bytes using ffmpeg."""
        channels = 1 if audio_buffer.ndim == 1 else audio_buffer.shape[1]
        with tempfile.NamedTemporaryFile(
            suffix=".flac", delete=False
        ) as temp_file:
            temp_filename = temp_file.name

        try:
            process = subprocess.run(
                [
                    "ffmpeg",
                    "-y",
                    "-f",
                    "s16le",
                    "-ar",
                    str(sample_rate),
                    "-ac",
                    str(channels),
                    "-i",
                    "pipe:0",
                    "-f",
                    "flac",
                    "-compression_level",
                    FLAC_COMPRESSION_LEVEL,
                    temp_filename,
                ],
                input=audio_buffer.tobytes(),
                capture_output=True,
                check=False,
                timeout=DEFAULT_FFMPEG_TIMEOUT_SEC,
            )
            if process.returncode != 0:
                logger.error(
                    f"ffmpeg error during FLAC export: {process.stderr.decode()}"
                )
                msg = "Failed to export FLAC via ffmpeg"
                raise RuntimeError(msg)

            with open(temp_filename, "rb") as f:
                return f.read()
        finally:
            try:
                Path(temp_filename).unlink()
            except OSError:
                pass

    def export_m4a(self, audio_buffer: np.ndarray, sample_rate: int) -> bytes:
        """Exports raw s16le PCM NumPy array to M4A (AAC) bytes using ffmpeg."""
        channels = 1 if audio_buffer.ndim == 1 else audio_buffer.shape[1]
        with tempfile.NamedTemporaryFile(
            suffix=".m4a", delete=False
        ) as temp_file:
            temp_filename = temp_file.name

        try:
            process = subprocess.run(
                [
                    "ffmpeg",
                    "-y",
                    "-f",
                    "s16le",
                    "-ar",
                    str(sample_rate),
                    "-ac",
                    str(channels),
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
                input=audio_buffer.tobytes(),
                capture_output=True,
                check=False,
                timeout=DEFAULT_FFMPEG_TIMEOUT_SEC,
            )
            if process.returncode != 0:
                logger.error(
                    f"ffmpeg error during M4A export: {process.stderr.decode()}"
                )
                msg = "Failed to export M4A via ffmpeg"
                raise RuntimeError(msg)

            with open(temp_filename, "rb") as f:
                return f.read()
        finally:
            try:
                Path(temp_filename).unlink()
            except OSError:
                pass

    def process_buffer(
        self,
        audio_buffer: np.ndarray,
        sample_rate: int,
        speech_segments: list[Any],
    ) -> ProcessorOutput:
        """Performs direct transcoding of the unmodified PCM buffer to FLAC."""
        if not speech_segments:
            return ProcessorOutput(success=False)

        # No acoustic or volume preprocessing applied - raw PCM is exported directly
        flac_bytes = self.export_flac(audio_buffer, sample_rate)
        return ProcessorOutput(
            success=True,
            flac_bytes=flac_bytes,
            processed_audio=audio_buffer,
        )
