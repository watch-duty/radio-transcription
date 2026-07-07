"""Audio helpers for the ingestion layer."""

from __future__ import annotations

import io
import json
import logging
import subprocess
import tempfile
from typing import Literal, NamedTuple

import soundfile

from backend.pipeline.ingestion import models

logger = logging.getLogger(__name__)

FFPROBE_TIMEOUT_SEC = 10
_SF_COUNT_MAX = 9223372036854775807
type AudioInputFormat = Literal["mp3"]

# Maps ffprobe format_name tokens to AudioMimeType. Keys are lower-cased
# substrings that may appear in a comma-separated format_name string.
_FORMAT_MIME_MAP: dict[str, models.AudioMimeType] = {
    "mp3": models.AudioMimeType.MPEG,
    "mp2": models.AudioMimeType.MPEG,
    "mp1": models.AudioMimeType.MPEG,
    "mov": models.AudioMimeType.MP4,
    "mp4": models.AudioMimeType.MP4,
    "m4a": models.AudioMimeType.MP4,
    "3g2": models.AudioMimeType.MP4,
    "mj2": models.AudioMimeType.MP4,
    "aac": models.AudioMimeType.AAC,
    "wav": models.AudioMimeType.WAV,
    "flac": models.AudioMimeType.FLAC,
    "ogg": models.AudioMimeType.OGG,
}


class ProbeResult(NamedTuple):
    """Parsed output from a single ffprobe invocation.

    Attributes:
        duration: Raw duration string from ffprobe (e.g. "12.345" or "N/A"),
            or None if the field was absent.
        format_name: Raw format_name string from ffprobe (e.g. "mp3" or
            "mov,mp4,m4a,3gp,3g2,mj2"), or None if the field was absent.
    """

    duration: str | None
    format_name: str | None


def _map_format_to_mime(format_name: str | None) -> models.AudioMimeType:
    if not format_name:
        return models.AudioMimeType.MPEG
    for token in (t.strip().lower() for t in format_name.split(",")):
        mime = _FORMAT_MIME_MAP.get(token)
        if mime is not None:
            return mime

    logger.warning(
        "Unrecognized ffprobe audio format name %r; defaulting to MPEG/MP3",
        format_name,
    )
    return models.AudioMimeType.MPEG


def _probe_file(file_path: str, format_args: list[str]) -> ProbeResult:
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
            return ProbeResult(
                duration=format_info.get("duration"),
                format_name=format_info.get("format_name"),
            )
        return ProbeResult(duration=str(data).strip(), format_name=None)
    except json.JSONDecodeError:
        return ProbeResult(duration=stdout_str, format_name=None)


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

        result: ProbeResult | None = None

        # 1. Try with the specified input format first if provided
        if input_format is not None:
            try:
                result = _probe_file(f.name, _input_format_args(input_format))
            except (subprocess.SubprocessError, UnicodeDecodeError) as e:
                logger.debug(
                    "ffprobe failed to probe metadata with forced format "
                    "%s: %s",
                    input_format,
                    e,
                )

            if result is None or result.duration is None:
                logger.warning(
                    "ffprobe failed to probe metadata with forced format %s; "
                    "retrying with auto-detection",
                    input_format,
                )
                result = None

        # 2. Try with auto-detection (either because input_format was None,
        # or the forced-format probe failed)
        if result is None:
            try:
                result = _probe_file(f.name, [])
            except subprocess.CalledProcessError as e:
                stderr_text = _run_verbose_probe(f.name)
                raise subprocess.CalledProcessError(
                    returncode=e.returncode,
                    cmd=e.cmd,
                    output=e.output,
                    stderr=stderr_text.encode("utf-8"),
                ) from e

            if result.duration in (None, "N/A"):
                stderr_text = _run_verbose_probe(f.name)
                raise subprocess.CalledProcessError(
                    returncode=1,
                    cmd=["ffprobe", f.name],
                    output=b"",
                    stderr=stderr_text.encode("utf-8"),
                )

        duration_str = result.duration or ""
        duration_sec = float(duration_str)
        mime_type = _map_format_to_mime(result.format_name)
        return int(duration_sec * 1000), mime_type


def get_audio_duration(
    audio_bytes: bytes, *, input_format: AudioInputFormat | None = None
) -> int:
    """Calculate duration of audio bytes using ffprobe.

    Backward-compatible shim over ``probe_audio_metadata`` for callers that
    only need the duration and do not require the detected MIME type.

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


def parse_flac_duration_from_bytes(audio_bytes: bytes) -> float | None:
    """Parse duration in seconds from STREAMINFO block of FLAC bytes using soundfile.

    This function is deliberately non-raising; it catches any exceptions during
    parsing and returns None to handle malformed stream packets gracefully.

    Args:
        audio_bytes: Raw FLAC audio bytes.

    Returns:
        Duration in seconds, or None if parsing fails.
    """
    try:
        f_info = soundfile.info(io.BytesIO(audio_bytes))
        # Check if duration is a sane, finite positive value and is a FLAC container.
        # If frames is SF_COUNT_MAX (9223372036854775807), duration will be huge/meaningless.
        if (
            f_info.format == "FLAC"
            and f_info.duration > 0
            and f_info.frames < _SF_COUNT_MAX
        ):
            return f_info.duration
    except Exception as e:
        logger.debug(
            "soundfile failed to parse FLAC duration, falling back to manual parsing: %s",
            e,
        )

    try:
        if (
            len(audio_bytes) < 42
            or audio_bytes[0:4] != b"fLaC"
            or (audio_bytes[4] & 0x7F) != 0
        ):
            return None
        sample_rate = (
            (audio_bytes[18] << 12)
            | (audio_bytes[19] << 4)
            | ((audio_bytes[20] & 0xF0) >> 4)
        )
        total_samples = (
            ((audio_bytes[21] & 0x0F) << 32)
            | (audio_bytes[22] << 24)
            | (audio_bytes[23] << 16)
            | (audio_bytes[24] << 8)
            | audio_bytes[25]
        )
        if sample_rate == 0:
            return None
        return total_samples / sample_rate
    except Exception as e:
        logger.exception("Failed to parse FLAC duration from bytes: %s", e)
        return None


def _input_format_args(input_format: AudioInputFormat | None) -> list[str]:
    if input_format is None:
        return []
    return ["-f", input_format]
