"""Audio helpers for the ingestion layer."""

import logging
import subprocess
import tempfile

logger = logging.getLogger(__name__)

FFPROBE_TIMEOUT_SEC = 10


def get_audio_duration(audio_bytes: bytes) -> int:
    """Calculate duration of audio bytes using ffprobe.

    Writes to a temp file so ffprobe can seek; reading from stdin returns
    ``N/A`` for MP3s without a Xing/Info frame (the encoding the echo
    field devices emit).

    Args:
        audio_bytes: Raw audio bytes.

    Returns:
        Duration in milliseconds.
    """
    try:
        with tempfile.NamedTemporaryFile() as f:
            f.write(audio_bytes)
            f.flush()
            result = subprocess.run(
                [
                    "ffprobe",
                    "-v",
                    "error",
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
        duration_sec = float(result.stdout.decode().strip())
        return int(duration_sec * 1000)
    except Exception as e:
        logger.exception("Failed to calculate duration using ffprobe: %s", e)
        raise
