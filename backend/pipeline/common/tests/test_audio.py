import subprocess
import unittest
from unittest.mock import MagicMock, patch

from backend.pipeline.common.audio import get_audio_duration


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


if __name__ == "__main__":
    unittest.main()
