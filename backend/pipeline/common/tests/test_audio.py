from __future__ import annotations

import shutil
import subprocess
import unittest
from typing import Self
from unittest.mock import MagicMock, patch

from backend.pipeline.common import audio as audio_helper
from backend.pipeline.common.audio import get_audio_duration

_ffmpeg_available = shutil.which("ffmpeg") is not None


class _NamedTemporaryFileStub:
    def __init__(self) -> None:
        self.name = "audio-duration-test.mp3"
        self.closed = False

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        _exc_type: object,
        _exc_value: object,
        _traceback: object,
    ) -> None:
        self.closed = True

    def write(self, _audio_bytes: bytes) -> None:
        return None

    def flush(self) -> None:
        return None


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
        self.assertNotIn("-f", mock_run.call_args.args[0])

    @patch("subprocess.run")
    def test_get_audio_duration_forces_mp3_format(
        self, mock_run: MagicMock
    ) -> None:
        mock_result = MagicMock()
        mock_result.stdout = b"0.288000\n"
        mock_run.return_value = mock_result

        duration_ms = get_audio_duration(b"dummy audio", input_format="mp3")

        self.assertEqual(duration_ms, 288)
        command = mock_run.call_args.args[0]
        self.assertIn("-f", command)
        format_arg_index = command.index("-f")
        self.assertEqual(command[format_arg_index + 1], "mp3")
        self.assertLess(format_arg_index, len(command) - 1)

    @patch("subprocess.run")
    def test_get_audio_duration_failure(self, mock_run: MagicMock) -> None:
        """Test handling of ffprobe failure."""
        mock_run.side_effect = subprocess.CalledProcessError(1, "ffprobe")

        audio_bytes = b"dummy audio"

        with self.assertRaises(subprocess.CalledProcessError):
            get_audio_duration(audio_bytes)

    @patch("backend.pipeline.common.audio.logger")
    @patch("subprocess.run")
    def test_get_audio_duration_failure_does_not_log_traceback(
        self, mock_run: MagicMock, mock_logger: MagicMock
    ) -> None:
        mock_run.side_effect = subprocess.CalledProcessError(1, "ffprobe")

        with self.assertRaises(subprocess.CalledProcessError):
            get_audio_duration(b"dummy audio")

        mock_logger.exception.assert_not_called()

    @patch("backend.pipeline.common.audio.tempfile.NamedTemporaryFile")
    @patch("subprocess.run")
    def test_get_audio_duration_na_fallback_keeps_temp_file_alive(
        self, mock_run: MagicMock, mock_temp_file: MagicMock
    ) -> None:
        temp_file = _NamedTemporaryFileStub()
        mock_temp_file.return_value = temp_file
        verbose_probe_observed: dict[str, object] = {}

        def _run_side_effect(command, **kwargs):
            if "-show_format" in command:
                verbose_probe_observed["temp_file_closed"] = temp_file.closed
                verbose_probe_observed["timeout"] = kwargs.get("timeout")
                return MagicMock(stdout="", stderr="")
            return MagicMock(stdout=b"N/A\n")

        mock_run.side_effect = _run_side_effect

        duration_ms = get_audio_duration(b"dummy audio")

        self.assertEqual(duration_ms, 5000)
        self.assertFalse(verbose_probe_observed["temp_file_closed"])
        self.assertEqual(
            verbose_probe_observed["timeout"],
            audio_helper.FFPROBE_TIMEOUT_SEC,
        )

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
