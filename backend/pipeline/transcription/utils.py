"""Utility functions for the radio transcription pipeline."""

import logging
import uuid
from typing import Self

import pydantic

logger = logging.getLogger(__name__)


class ConfigBase(pydantic.BaseModel):
    """Base Pydantic model for JSON configuration classes."""

    model_config = pydantic.ConfigDict(extra="ignore", frozen=True)

    @classmethod
    def from_json(cls, json_str: str) -> Self:
        """Creates an instance from a JSON string."""
        if not json_str:
            return cls()
        try:
            return cls.model_validate_json(json_str)
        except pydantic.ValidationError as e:
            logger.exception(
                "Failed to parse config JSON for %s: %s", cls.__name__, json_str
            )
            msg = f"Invalid config JSON for {cls.__name__}: {e}"
            raise ValueError(msg) from e


def generate_transmission_id(feed_id: str, start_ms: int, end_ms: int) -> str:
    """Creates a deterministic UUID string using uuid5 to ensure pipeline retries produce the exact same ID.

    Uses raw VAD start and end times to ensure stability across pre-roll/post-roll configuration changes.
    """
    deterministic_id = f"{feed_id}_{start_ms}_{end_ms}"
    return str(uuid.uuid5(uuid.NAMESPACE_OID, deterministic_id))


import tempfile
import subprocess
import os
import numpy as np
import soundfile as sf


def export_flac_from_numpy(
    audio_buffer: np.ndarray, sample_rate: int = 16000
) -> bytes:
    """Exports a NumPy array to FLAC bytes."""
    buf = io.BytesIO()
    sf.write(buf, audio_buffer, sample_rate, format="FLAC")
    return buf.getvalue()


def export_m4a_from_numpy(
    audio_buffer: np.ndarray, sample_rate: int = 16000
) -> bytes:
    """Exports a NumPy array to M4A bytes using ffmpeg."""
    with tempfile.NamedTemporaryFile(suffix=".wav", delete=False) as wav_file:
        wav_path = wav_file.name
        sf.write(wav_path, audio_buffer, sample_rate)

    m4a_path = wav_path.replace(".wav", ".m4a")
    try:
        subprocess.run(
            ["ffmpeg", "-y", "-i", wav_path, "-c:a", "aac", m4a_path],
            check=True,
            capture_output=True,
        )
        with open(m4a_path, "rb") as f:
            return f.read()
    finally:
        if os.path.exists(wav_path):
            os.remove(wav_path)
        if os.path.exists(m4a_path):
            os.remove(m4a_path)
