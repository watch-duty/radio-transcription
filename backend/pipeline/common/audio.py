"""Audio helpers for the ingestion layer."""

from __future__ import annotations

import json
import logging
import subprocess
import tempfile
from typing import Literal

from backend.pipeline.ingestion import models

logger = logging.getLogger(__name__)

FFPROBE_TIMEOUT_SEC = 10
type AudioInputFormat = Literal["mp3"]


def _map_format_to_mime(format_name: str | None) -> models.AudioMimeType:
    if not format_name:
        return models.AudioMimeType.MPEG
    names = [name.strip().lower() for name in format_name.split(",")]

    # Map candidate format/codec tags to the standard AudioMimeType enums
    mappings = [
        (("mp3", "mp2", "mp1"), models.AudioMimeType.MPEG),
        (("mov", "mp4", "m4a", "3g2", "mj2"), models.AudioMimeType.MP4),
        (("aac",), models.AudioMimeType.AAC),
        (("wav",), models.AudioMimeType.WAV),
        (("flac",), models.AudioMimeType.FLAC),
        (("ogg",), models.AudioMimeType.OGG),
    ]
    for keys, mime in mappings:
        if any(key in names for key in keys):
            return mime

    logger.warning(
        "Unrecognized ffprobe audio format name %r; defaulting to MPEG/MP3",
        format_name,
    )
    return models.AudioMimeType.MPEG


def _probe_file(
    file_path: str, format_args: list[str]
) -> tuple[str | None, str | None]:
    """Execute ffprobe JSON metadata inspection on the given file path.

    Raises:
        subprocess.CalledProcessError: If ffprobe execution fails.
        subprocess.TimeoutExpired: If ffprobe execution times out.
    """
    cmd = [
        "ffprobe",
        "-v",
        "error",
        *format_args,
        "-show_entries",
        "format=duration,format_name",
        "-of",
        "json",
        file_path,
    ]
    res = subprocess.run(
        cmd,
        capture_output=True,
        check=True,
        timeout=FFPROBE_TIMEOUT_SEC,
    )

    stdout_str = res.stdout.decode(errors="replace").strip()
    try:
        data = json.loads(stdout_str)
        if isinstance(data, dict):
            format_info = data.get("format", {})
            return (
                format_info.get("duration"),
                format_info.get("format_name"),
            )
        return str(data).strip(), None
    except json.JSONDecodeError:
        return stdout_str, None


def _run_verbose_probe(file_path: str) -> str:
    """Execute a detailed, verbose ffprobe diagnostic run on a corrupt file."""
    stderr_parts = ["ffprobe returned N/A for duration"]
    try:
        probe_result = subprocess.run(
            [
                "ffprobe",
                "-v",
                "warning",
                "-show_format",
                "-show_streams",
                file_path,
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
    except subprocess.SubprocessError as probe_err:
        stderr_parts.append(f"Failed to run verbose probe: {probe_err}")
    return "\n".join(stderr_parts)


def probe_audio_metadata(
    audio_bytes: bytes, *, input_format: AudioInputFormat | None = None
) -> tuple[int, models.AudioMimeType]:
    """Calculate duration and detect MIME type of audio bytes using ffprobe.

    Writes to a temp file so ffprobe can seek; reading from stdin returns
    ``N/A`` for MP3s without a Xing/Info frame (the encoding the echo
    field devices emit).

    Args:
        audio_bytes: Raw audio bytes.
        input_format: Optional ffprobe input format for known-format bytes.

    Returns:
        A tuple of (duration_ms, mime_type).
    """
    with tempfile.NamedTemporaryFile() as f:
        f.write(audio_bytes)
        f.flush()

        output_duration = None
        output_format = None

        # 1. Try with the specified input format first if provided
        if input_format is not None:
            try:
                output_duration, output_format = _probe_file(
                    f.name, _input_format_args(input_format)
                )
            except (subprocess.SubprocessError, UnicodeDecodeError) as e:
                logger.debug(
                    "ffprobe failed to probe metadata with forced format %s: %s",
                    input_format,
                    e,
                )

            if output_duration is None:
                logger.warning(
                    "ffprobe failed to probe metadata with forced format %s; "
                    "retrying with auto-detection",
                    input_format,
                )

        # 2. Try with auto-detection (either because input_format was None, or
        # forced format failed)
        if output_duration is None:
            try:
                output_duration, output_format = _probe_file(f.name, [])
            except subprocess.CalledProcessError as e:
                stderr_text = _run_verbose_probe(f.name)
                raise subprocess.CalledProcessError(
                    returncode=e.returncode,
                    cmd=e.cmd,
                    output=e.output,
                    stderr=stderr_text.encode("utf-8"),
                ) from e

            if output_duration == "N/A" or output_duration is None:
                stderr_text = _run_verbose_probe(f.name)
                raise subprocess.CalledProcessError(
                    returncode=1,
                    cmd=["ffprobe", f.name],
                    output=b"",
                    stderr=stderr_text.encode("utf-8"),
                )

        duration_sec = float(output_duration)
        mime_type = _map_format_to_mime(output_format)
        return int(duration_sec * 1000), mime_type


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
    duration_ms, _ = probe_audio_metadata(
        audio_bytes, input_format=input_format
    )
    return duration_ms


def _input_format_args(input_format: AudioInputFormat | None) -> list[str]:
    if input_format is None:
        return []
    return ["-f", input_format]
