"""Unit tests for the audio processor."""

import sys
from unittest.mock import MagicMock

# Mock sherpa_onnx before importing anything that might use it
mock_sherpa = MagicMock()
sys.modules["sherpa_onnx"] = mock_sherpa

import io
import logging
import shutil
import unittest
from unittest.mock import MagicMock, patch

<<<<<<< HEAD
=======
import pytest
import numpy as np
>>>>>>> f1aee6b (feat: Add transcription VAD integration tests and update CI workflow)
from pydub import AudioSegment

from backend.pipeline.common.constants import AUDIO_FORMAT
from backend.pipeline.transcription.audio.audio_processor import AudioProcessor
from backend.pipeline.transcription.datatypes import AudioChunkData, TimeRange

logger = logging.getLogger(__name__)

# Warn if ffmpeg is missing for I/O tests
if shutil.which("ffmpeg") is None:
    logger.warning(
        "FFMPEG is not installed. Audio I/O tests requiring ffmpeg will be skipped."
    )


class AudioProcessorTest(unittest.TestCase):
    def setUp(self) -> None:

        self.processor = AudioProcessor()



    @unittest.skip("Legacy test for missing method _trim_trailing_clicks")
    def test_trim_trailing_clicks(self) -> None:
        """Verifies that _trim_trailing_clicks trims audio with a sharp energy spike at the end."""
        from pydub import AudioSegment

        # Create a silent audio segment of 500ms
        audio = AudioSegment.silent(duration=500)

        # Add a "click" (full scale square wave) at the end (last 50ms)
        # 50ms at 16kHz is 800 samples. Each sample is 2 bytes for int16.
        # So 1600 bytes of b'\xff\x7f' (max positive value for int16)
        click_data = b"\xff\x7f" * 800
        click = AudioSegment(
            data=click_data, sample_width=2, frame_rate=16000, channels=1
        )

        audio = audio[:450] + click

        # Call the private method with offset 400 (giving 50ms of silence before click)
        trimmed = self.processor._trim_trailing_clicks(audio, 400)

        # Verify that it is shorter than 500ms
        self.assertLess(len(trimmed), 500)
        # And specifically it should be around 450ms
        self.assertLessEqual(len(trimmed), 460)

    def test_download_audio_raises_if_not_setup(self) -> None:
        """Ensures that downloading audio before calling setup() correctly raises a runtime error to prevent missing GCS client exceptions."""
        # Create a new processor instance that hasn't been set up
        processor = AudioProcessor()
        # Act & Assert
        with self.assertRaises(RuntimeError):
            processor.download_audio_and_detect("gs://test/file.flac", 0, session_id="test-session")




    @patch("backend.pipeline.transcription.audio.audio_processor._get_gcs_client")
    def test_download_audio_not_found(
        self, mock_get_gcs: MagicMock
    ) -> None:
        """Ensures a FileNotFoundError is explicitly raised if the requested GCS audio blob does not exist in the bucket."""
        # Arrange
        processor = AudioProcessor()
        processor.setup()
        processor.gcs_client = MagicMock()
        mock_bucket = MagicMock()
        mock_bucket.get_blob.return_value = None
        processor.gcs_client.bucket.return_value = mock_bucket

        # Act & Assert
        with self.assertRaises(FileNotFoundError):
            processor.download_audio_and_detect(
                "gs://my-bucket/missing.flac", 0, session_id="test-session"
            )

    def test_detect_speech_and_noise_raises_if_not_setup(self) -> None:
        """Verifies that detect_speech_and_noise raises RuntimeError if VAD is not setup."""
        processor = AudioProcessor()
        samples = np.zeros(16000, dtype=np.float32)
        with self.assertRaises(RuntimeError) as cm:
            processor._detect_speech_and_noise(samples, 0)
        self.assertEqual(str(cm.exception), "VAD is not initialized")


