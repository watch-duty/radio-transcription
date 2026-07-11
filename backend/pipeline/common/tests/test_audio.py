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
    def test_probe_audio_metadata_success(self, mock_run: MagicMock) -> None:
        """Test metadata extraction for various formats using JSON output."""
        # 1. Test MP3
        mock_result = MagicMock()
        mock_result.stdout = (
            b'{"format": {"duration": "15.500000", "format_name": "mp3"}}'
        )
        mock_run.return_value = mock_result

        duration, mime = audio_helper.probe_audio_metadata(b"dummy mp3")
        self.assertEqual(duration, 15500)
        self.assertEqual(mime, audio_helper.models.AudioMimeType.MPEG)

        # 2. Test M4A/MP4
        mock_result.stdout = (
            b'{"format": {"duration": "19.584000", "format_name": '
            b'"mov,mp4,m4a,3g2,mj2"}}'
        )
        duration, mime = audio_helper.probe_audio_metadata(b"dummy m4a")
        self.assertEqual(duration, 19584)
        self.assertEqual(mime, audio_helper.models.AudioMimeType.MP4)

        # 3. Test Unrecognized format defaults to MPEG and logs warning
        mock_result.stdout = (
            b'{"format": {"duration": "10.000000", "format_name": '
            b'"unknown_format"}}'
        )
        with patch("backend.pipeline.common.audio.logger") as mock_logger:
            duration, mime = audio_helper.probe_audio_metadata(b"dummy exotic")
            self.assertEqual(duration, 10000)
            self.assertEqual(mime, audio_helper.models.AudioMimeType.MPEG)
            mock_logger.warning.assert_called_once_with(
                "Unrecognized ffprobe audio format name %r; defaulting to MPEG/MP3",
                "unknown_format",
            )

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
    def test_get_audio_duration_na_raises_called_process_error_keeps_temp_file_alive(
        self, mock_run: MagicMock, mock_temp_file: MagicMock
    ) -> None:
        temp_file = _NamedTemporaryFileStub()
        mock_temp_file.return_value = temp_file
        verbose_probe_observed: dict[str, object] = {}

        def _run_side_effect(command, **kwargs):
            if "-show_format" in command:
                verbose_probe_observed["temp_file_closed"] = temp_file.closed
                verbose_probe_observed["timeout"] = kwargs.get("timeout")
                return MagicMock(stdout="mock-stdout", stderr="mock-stderr")
            res = MagicMock(stdout=b"N/A\n")
            res.args = command
            return res

        mock_run.side_effect = _run_side_effect

        with self.assertRaises(subprocess.CalledProcessError) as ctx:
            get_audio_duration(b"dummy audio")

        self.assertEqual(ctx.exception.returncode, 1)
        self.assertIn(
            b"ffprobe returned N/A for duration", ctx.exception.stderr
        )
        self.assertIn(b"STDOUT:\nmock-stdout", ctx.exception.stderr)
        self.assertIn(b"STDERR:\nmock-stderr", ctx.exception.stderr)
        self.assertFalse(verbose_probe_observed["temp_file_closed"])
        self.assertEqual(
            verbose_probe_observed["timeout"],
            audio_helper.FFPROBE_TIMEOUT_SEC,
        )

    @patch("backend.pipeline.common.audio.tempfile.NamedTemporaryFile")
    @patch("subprocess.run")
    def test_get_audio_duration_fallback_to_autodetect(
        self, mock_run: MagicMock, mock_temp_file: MagicMock
    ) -> None:
        temp_file = _NamedTemporaryFileStub()
        mock_temp_file.return_value = temp_file

        def _run_side_effect(command, **kwargs):
            if "-f" in command:
                raise subprocess.CalledProcessError(
                    1, command, stderr=b"forced format error"
                )
            return MagicMock(stdout=b"19.584000\n")

        mock_run.side_effect = _run_side_effect

        duration_ms = get_audio_duration(b"dummy audio", input_format="mp3")

        self.assertEqual(duration_ms, 19584)
        self.assertEqual(mock_run.call_count, 2)

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

    @patch("subprocess.run")
    def test_probe_audio_metadata_stream_duration_fallback_for_fmp4(
        self, mock_run: MagicMock
    ) -> None:
        """Test stream duration fallback when container format duration is N/A (e.g. fragmented M4A)."""
        mock_result = MagicMock()
        mock_result.stdout = (
            b'{"format": {"duration": "N/A", "format_name": "mov,mp4,m4a,3gp,3g2,mj2"}, '
            b'"streams": [{"codec_type": "audio", "duration": "12.345000"}]}'
        )
        mock_run.return_value = mock_result

        duration, mime = audio_helper.probe_audio_metadata(
            b"dummy fmp4", input_format="m4a"
        )
        self.assertEqual(duration, 12345)
        self.assertEqual(mime, audio_helper.models.AudioMimeType.MP4)
        command = mock_run.call_args.args[0]
        self.assertIn("-f", command)
        self.assertIn("m4a", command)

    @patch("backend.pipeline.common.audio.tempfile.NamedTemporaryFile")
    @patch("subprocess.run")
    def test_probe_audio_metadata_both_format_and_stream_duration_unusable_raises(
        self, mock_run: MagicMock, mock_temp_file: MagicMock
    ) -> None:
        """Test that probe_audio_metadata raises CalledProcessError with verbose probe when duration is missing everywhere."""
        temp_file = _NamedTemporaryFileStub()
        mock_temp_file.return_value = temp_file

        def _run_side_effect(command, **_kwargs):
            if "-show_format" in command:
                return MagicMock(
                    stdout="verbose-probe-stdout", stderr="verbose-probe-stderr"
                )
            res = MagicMock()
            res.stdout = (
                b'{"format": {"duration": "N/A", "format_name": "mov,mp4,m4a,3gp,3g2,mj2"}, '
                b'"streams": [{"codec_type": "audio", "duration": "N/A"}]}'
            )
            res.args = command
            return res

        mock_run.side_effect = _run_side_effect

        with self.assertRaises(subprocess.CalledProcessError) as ctx:
            audio_helper.probe_audio_metadata(b"dummy fmp4", input_format="m4a")

        self.assertEqual(ctx.exception.returncode, 1)
        self.assertIn(
            b"ffprobe returned N/A for duration", ctx.exception.stderr
        )
        self.assertIn(b"STDOUT:\nverbose-probe-stdout", ctx.exception.stderr)

    @patch("subprocess.run")
    def test_probe_audio_metadata_nonstandard_zero_string_fallback(
        self, mock_run: MagicMock
    ) -> None:
        """Test that non-standard zero strings like '0.00' trigger stream fallback."""
        mock_result = MagicMock()
        mock_result.stdout = (
            b'{"format": {"duration": "0.00", "format_name": "mov,mp4,m4a,3gp,3g2,mj2"}, '
            b'"streams": [{"codec_type": "audio", "duration": "8.500000"}]}'
        )
        mock_run.return_value = mock_result

        duration, mime = audio_helper.probe_audio_metadata(b"dummy fmp4")
        self.assertEqual(duration, 8500)
        self.assertEqual(mime, audio_helper.models.AudioMimeType.MP4)


if __name__ == "__main__":
    unittest.main()
