"""Unit tests for the audio processor."""

import io
import logging
import shutil
import unittest
from unittest.mock import MagicMock, patch

import numpy as np
import soundfile as sf

from backend.pipeline.transcription.audio.audio_processor import AudioProcessor

logger = logging.getLogger(__name__)

# Warn if ffmpeg is missing for I/O tests
if shutil.which("ffmpeg") is None:
    logger.warning(
        "FFMPEG is not installed. Audio I/O tests requiring ffmpeg will be skipped."
    )


class AudioProcessorTest(unittest.TestCase):
    def setUp(self) -> None:
        self.processor = AudioProcessor()

    @patch(
        "backend.pipeline.transcription.audio.audio_processor.get_gcs_client"
    )
    @patch(
        "backend.pipeline.transcription.audio.audio_processor.get_vad_engine"
    )
    def test_setup_initializes_vad_and_gcs(
        self, mock_get_vad: MagicMock, mock_get_gcs: MagicMock
    ) -> None:
        """Verifies that calling setup() correctly instantiates the lazy-loaded VAD engine and GCS client."""
        mock_vad_instance = MagicMock()
        mock_get_vad.return_value = mock_vad_instance

        self.processor.setup()
        mock_get_vad.assert_called_once_with("{}")
        mock_get_gcs.assert_called_once()
        self.assertIsNotNone(self.processor.vad)
        self.assertEqual(self.processor.gcs_client, mock_get_gcs.return_value)
        mock_vad_instance.setup.assert_called_once()

    def test_download_audio_raises_if_not_setup(self) -> None:
        """Ensures that downloading audio before calling setup() correctly raises a runtime error."""
        processor = AudioProcessor()
        with self.assertRaises(RuntimeError):
            processor.download_audio_and_detect("gs://test/file.flac", 0)

    @patch(
        "backend.pipeline.transcription.audio.audio_processor.get_gcs_client"
    )
    @patch(
        "backend.pipeline.transcription.audio.audio_processor.get_vad_engine"
    )
    def test_download_audio_and_detect_calculates_duration(
        self, mock_get_vad: MagicMock, mock_get_gcs: MagicMock
    ) -> None:
        """Tests that download_audio_and_detect calculates duration when not provided."""
        mock_vad_instance = MagicMock()
        # Update segment mock to accept arbitrary keyword arguments (e.g., prior_audio)
        mock_vad_instance.detect_speech_segments.side_effect = (
            lambda *args, **kwargs: []
        )
        mock_get_vad.return_value = mock_vad_instance

        self.processor.setup()
        self.processor.gcs_client = MagicMock()
        mock_bucket = MagicMock()
        mock_blob = MagicMock()

        # Create a tiny valid FLAC (100ms -> 1600 samples)
        audio = np.zeros(1600, dtype=np.int16)
        buf = io.BytesIO()
        sf.write(buf, audio, 16000, format="FLAC")
        flac_bytes = buf.getvalue()

        def download_to_file(f: io.BytesIO, **kwargs: object) -> None:
            f.write(flac_bytes)

        mock_blob.download_to_file = download_to_file
        mock_bucket.get_blob.return_value = mock_blob
        self.processor.gcs_client.bucket.return_value = mock_bucket

        # Act
        result = self.processor.download_audio_and_detect(
            "gs://fake-bucket/100-11111111-1111-1111-1111-111111111111.flac",
            100000,
        )

        # Assert
        self.assertEqual(result.duration_ms, 100)  # 1600 / 16 = 100

    def test_preprocess_audio_applies_bandpass(self) -> None:
        """Verifies that the audio preprocessing filters do not corrupt or truncate the np.ndarray structure."""
        audio = np.zeros(16000, dtype=np.float32)
        processed = self.processor.preprocess_audio(audio, 16000)
        self.assertIsInstance(processed, np.ndarray)
        self.assertEqual(len(processed), len(audio))

    @unittest.skipIf(
        shutil.which("ffmpeg") is None, "ffmpeg is required for I/O tests"
    )
    def test_export_flac(self) -> None:
        """Tests that exporting to FLAC produces a valid byte array containing the expected `fLaC` header signature."""
        audio = np.zeros(8000, dtype=np.int16)
        flac_bytes = self.processor.export_flac(audio, 16000)
        self.assertIsInstance(flac_bytes, bytes)
        self.assertTrue(flac_bytes.startswith(b"fLaC"))

    @unittest.skipIf(
        shutil.which("ffmpeg") is None, "ffmpeg is required for I/O tests"
    )
    def test_export_m4a(self) -> None:
        """Tests that exporting to M4A produces a valid byte array with valid ftyp header."""
        audio = np.zeros(8000, dtype=np.int16)
        m4a_bytes = self.processor.export_m4a(audio, 16000)
        self.assertIsInstance(m4a_bytes, bytes)
        self.assertTrue(len(m4a_bytes) > 0)
        self.assertIn(b"ftyp", m4a_bytes)

    @patch(
        "backend.pipeline.transcription.audio.audio_processor.get_vad_engine"
    )
    @patch(
        "backend.pipeline.transcription.audio.audio_processor.get_gcs_client"
    )
    def test_download_audio_not_found(
        self, mock_get_gcs: MagicMock, mock_get_vad: MagicMock
    ) -> None:
        """Ensures a FileNotFoundError is explicitly raised if the requested GCS audio blob does not exist in the bucket."""
        mock_vad_instance = MagicMock()
        mock_get_vad.return_value = mock_vad_instance

        processor = AudioProcessor()
        processor.setup()
        processor.gcs_client = MagicMock()
        mock_bucket = MagicMock()
        mock_bucket.get_blob.return_value = None
        processor.gcs_client.bucket.return_value = mock_bucket

        # Act & Assert
        with self.assertRaises(FileNotFoundError):
            processor.download_audio_and_detect(
                "gs://my-bucket/missing.flac", 0
            )
