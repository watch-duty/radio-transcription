"""Unit tests for the segmentation audio processor."""

import io
import logging
import shutil
import unittest
from unittest.mock import MagicMock, patch

import numpy as np
import soundfile as sf

from backend.pipeline.segmentation.audio.processor import (
    SegmentationAudioProcessor,
)

logger = logging.getLogger(__name__)

# Warn if ffmpeg is missing for I/O tests
if shutil.which("ffmpeg") is None:
    logger.warning(
        "FFMPEG is not installed. Audio I/O tests requiring ffmpeg will be skipped."
    )


class AudioProcessorTest(unittest.TestCase):
    def setUp(self) -> None:
        self.processor = SegmentationAudioProcessor(
            gcs_client_instance=MagicMock()
        )

    @patch("backend.pipeline.segmentation.audio.processor.get_vad_engine")
    def test_setup_initializes_vad_and_gcs(
        self, mock_get_vad: MagicMock
    ) -> None:
        """Verifies that calling setup() correctly instantiates the lazy-loaded VAD engine."""
        mock_vad_instance = MagicMock()
        mock_get_vad.return_value = mock_vad_instance
        mock_gcs = MagicMock()

        processor = SegmentationAudioProcessor(
            gcs_client_instance=mock_gcs,
            vad_factory=mock_get_vad,
        )
        processor.setup()
        mock_get_vad.assert_called_once_with("{}")
        self.assertIsNotNone(processor.vad)
        self.assertEqual(processor.gcs_client, mock_gcs)
        mock_vad_instance.setup.assert_called_once()

    def test_download_audio_raises_if_not_setup(self) -> None:
        """Ensures that downloading audio before calling setup() correctly raises a runtime error."""
        processor = SegmentationAudioProcessor(gcs_client_instance=None)
        with self.assertRaises(RuntimeError):
            processor.download_audio_and_detect("gs://test/file.flac", 0)

    @patch("backend.pipeline.segmentation.audio.processor.get_vad_engine")
    def test_download_audio_and_detect_calculates_duration(
        self, mock_get_vad: MagicMock
    ) -> None:
        """Tests that download_audio_and_detect calculates duration when not provided."""
        mock_vad_instance = MagicMock()
        mock_vad_instance.detect_speech_segments.side_effect = (
            lambda *args, **kwargs: []
        )
        mock_get_vad.return_value = mock_vad_instance

        mock_gcs = MagicMock()
        processor = SegmentationAudioProcessor(
            gcs_client_instance=mock_gcs,
            vad_factory=mock_get_vad,
        )
        processor.setup()
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
        mock_gcs.bucket.return_value = mock_bucket

        # Act
        result = processor.download_audio_and_detect(
            "gs://fake-bucket/100-11111111-1111-1111-1111-111111111111.flac",
            100000,
        )

        # Assert
        self.assertEqual(result.duration_ms, 100)  # 1600 / 16 = 100

    @patch("backend.pipeline.segmentation.audio.processor.get_vad_engine")
    def test_download_audio_not_found(self, mock_get_vad: MagicMock) -> None:
        """Ensures a FileNotFoundError is explicitly raised if the requested GCS audio blob does not exist in the bucket."""
        mock_vad_instance = MagicMock()
        mock_get_vad.return_value = mock_vad_instance

        mock_gcs = MagicMock()
        processor = SegmentationAudioProcessor(
            gcs_client_instance=mock_gcs,
            vad_factory=mock_get_vad,
        )
        processor.setup()
        mock_bucket = MagicMock()
        mock_bucket.get_blob.return_value = None
        mock_gcs.bucket.return_value = mock_bucket

        # Act & Assert
        with self.assertRaises(FileNotFoundError):
            processor.download_audio_and_detect(
                "gs://my-bucket/missing.flac", 0
            )
