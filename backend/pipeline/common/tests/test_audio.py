import shutil
import subprocess
import unittest
from unittest.mock import MagicMock, patch

from backend.pipeline.common.audio_duration import get_audio_duration

_ffmpeg_available = shutil.which("ffmpeg") is not None


def _make_headerless_lame_mp3(duration_ms: int) -> bytes:
    """Encode silence as an 8 kHz mono MP3 with ``-write_xing 0``.

    Matches the structural quirk of echo field-device uploads: LAME-encoded
    MP3 with no Xing/Info frame, so ``ffprobe -i -`` reports
    ``duration=N/A``. See ``get_audio_duration`` for the workaround.
    """
    process = subprocess.run(
        [
            "ffmpeg",
            "-f",
            "lavfi",
            "-i",
            "anullsrc=r=8000:cl=mono",
            "-t",
            str(duration_ms / 1000.0),
            "-c:a",
            "libmp3lame",
            "-write_xing",
            "0",
            "-f",
            "mp3",
            "pipe:1",
        ],
        capture_output=True,
        check=True,
    )
    return process.stdout


class TestAudioUtils(unittest.TestCase):
    @patch("subprocess.run")
    def test_get_audio_duration_success(self, mock_run: MagicMock) -> None:
        """Test successful duration extraction using ffprobe."""
        mock_result = MagicMock()
        mock_result.stdout = b"15.500000\n"
        mock_run.return_value = mock_result

        audio_bytes = b"dummy audio"

        duration_ms = get_audio_duration(audio_bytes)

        self.assertEqual(duration_ms, 15500)
        mock_run.assert_called_once()

    @patch("subprocess.run")
    def test_get_audio_duration_failure(self, mock_run: MagicMock) -> None:
        """Test handling of ffprobe failure."""
        mock_run.side_effect = subprocess.CalledProcessError(1, "ffprobe")

        audio_bytes = b"dummy audio"

        with self.assertRaises(subprocess.CalledProcessError):
            get_audio_duration(audio_bytes)

    @unittest.skipIf(not _ffmpeg_available, "ffmpeg not available")
    def test_get_audio_duration_handles_headerless_lame_mp3(self) -> None:
        """Regression: MP3 without a Xing/Info frame (what echo devices emit).

        Exercises the temp-file code path end-to-end with a real ffprobe
        invocation. Reading this MP3 via stdin returns literal ``'N/A'``;
        reading via a seekable file path returns an accurate duration.
        """
        audio_bytes = _make_headerless_lame_mp3(duration_ms=7500)

        duration_ms = get_audio_duration(audio_bytes)

        # Target 7500ms; allow ±300ms for encoder/decoder rounding + LAME
        # encoder delay padding.
        self.assertGreaterEqual(duration_ms, 7200)
        self.assertLessEqual(duration_ms, 7800)


if __name__ == "__main__":
    unittest.main()
