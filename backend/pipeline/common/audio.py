"""Audio helpers for the ingestion layer."""

from __future__ import annotations

import logging
import subprocess
import tempfile
from typing import Literal

logger = logging.getLogger(__name__)

FFPROBE_TIMEOUT_SEC = 10
type AudioInputFormat = Literal["mp3"]


def get_audio_duration(
    audio_bytes: bytes, *, input_format: AudioInputFormat | None = None
) -> int:
    """Calculate duration of audio bytes using ffprobe.

    Writes to a temp file so ffprobe can seek; reading from stdin returns
    ``N/A`` for MP3s without a Xing/Info frame (the encoding the echo
    field devices emit).

    Args:
        audio_bytes: Raw audio bytes.
        input_format: Optional ffprobe input format for known-format bytes.

    Returns:
        Duration in milliseconds.
    """
    with tempfile.NamedTemporaryFile() as f:
        f.write(audio_bytes)
        f.flush()
        result = subprocess.run(
            [
                "ffprobe",
                "-v",
                "error",
                *_input_format_args(input_format),
                "-show_entries",
                "format=duration",
                "-of",
                "default=noprint_wrappers=1:nokey=1",
                f.name,
            ],
            capture_output=True,
            check=True,
            timeout=FFPROBE_TIMEOUT_SEC,
        )
        output = result.stdout.decode().strip()
        if output == "N/A":
            stderr_parts = ["ffprobe returned N/A for duration"]
            try:
                probe_result = subprocess.run(
                    [
                        "ffprobe",
                        "-v",
                        "warning",
                        *_input_format_args(input_format),
                        "-show_format",
                        "-show_streams",
                        f.name,
                    ],
                    capture_output=True,
                    text=True,
                    check=False,
                    timeout=FFPROBE_TIMEOUT_SEC,
                )
                if probe_result.stdout:
                    stderr_parts.append(f"STDOUT:\n{probe_result.stdout}")
                if probe_result.stderr:
                    stderr_parts.append(f"STDERR:\n{probe_result.stderr}")
            except Exception as probe_err:
                stderr_parts.append(f"Failed to run verbose probe: {probe_err}")

            raise subprocess.CalledProcessError(
                returncode=1,
                cmd=result.args,
                output=result.stdout,
                stderr="\n".join(stderr_parts).encode("utf-8"),
            )
        duration_sec = float(output)
        return int(duration_sec * 1000)


def _input_format_args(input_format: AudioInputFormat | None) -> list[str]:
    if input_format is None:
        return []
    return ["-f", input_format]
