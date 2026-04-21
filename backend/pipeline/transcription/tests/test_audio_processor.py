
"""Unit tests for the audio processor."""

import io
import logging
import shutil
import unittest
from unittest.mock import MagicMock, patch

import numpy as np
import soundfile as sf

from backend.pipeline.transcription.audio_processor import AudioProcessor
from backend.pipeline.transcription.datatypes import AudioChunkData, TimeRange
from backend.pipeline.transcription.enums import VadType

logger = logging.getLogger(__name__)

# Warn if ffmpeg is missing for I/O tests
if shutil.which("ffmpeg") is None:
    logger.warning(
        "FFMPEG is not installed. Audio I/O tests requiring ffmpeg will be skipped."
    )


class AudioProcessorTest(unittest.TestCase):
    def setUp(self) -> None:

        self.processor = AudioProcessor(vad_type=VadType.TEN_VAD)

    @patch("backend.pipeline.transcription.audio_processor.get_gcs_client")
    @patch("backend.pipeline.transcription.audio_processor.get_vad_plugin")
    def test_setup_initializes_vad_and_gcs(
        self, mock_get_vad: MagicMock, mock_get_gcs: MagicMock
    ) -> None:
        """Verifies that calling setup() correctly instantiates the lazy-loaded VAD plugin and GCS client."""
        self.processor.setup()
        mock_get_vad.assert_called_once_with(VadType.TEN_VAD, "{}")
        mock_get_gcs.assert_called_once()
        self.assertIsNotNone(self.processor.vad)
        self.assertEqual(self.processor.gcs_client, mock_get_gcs.return_value)

    def test_check_vad_raises_if_not_setup(self) -> None:
        """Ensures that attempting to evaluate VAD before setup() raises a clear runtime error."""
        audio = np.zeros(((1000) * 16), dtype=np.int16)
        with self.assertRaises(RuntimeError):
            self.processor.check_vad(audio)

    def test_download_audio_raises_if_not_setup(self) -> None:
        """Ensures that downloading audio before calling setup() correctly raises a runtime error to prevent missing GCS client exceptions."""
        # Create a new processor instance that hasn't been set up
        processor = AudioProcessor(vad_type=VadType.TEN_VAD)
        # Act & Assert
        with self.assertRaises(RuntimeError):
            processor.download_audio_and_detect("gs://test/file.flac", 0)

    @patch("backend.pipeline.transcription.audio_processor.get_gcs_client")
    @patch("backend.pipeline.transcription.audio_processor.get_vad_plugin")
    def test_check_vad_evaluates_speech(
        self, mock_get_vad: MagicMock, mock_get_gcs: MagicMock
    ) -> None:
        """Tests that check_vad returns True when VAD detects speech."""
        mock_vad_instance = MagicMock()
        mock_vad_instance.evaluate.return_value = True
        mock_get_vad.return_value = mock_vad_instance

        self.processor.setup()

        # Generate 1 second of 440Hz sine wave at 16kHz
        t = np.linspace(0, 1, 16000, endpoint=False)
        audio = (np.sin(2 * np.pi * 440 * t) * 32767).astype(np.int16)

        result = self.processor.check_vad(audio)
        self.assertTrue(result)
        mock_vad_instance.evaluate.assert_called_once()

    @patch("backend.pipeline.transcription.audio_processor.compute_spectral_flatness")
    @patch("backend.pipeline.transcription.audio_processor.get_vad_plugin")
    def test_check_vad_drops_static(
        self, mock_get_vad: MagicMock, mock_compute_flatness: MagicMock
    ) -> None:
        """Tests that check_vad returns False for white noise due to spectral flatness."""
        mock_vad_instance = MagicMock()
        mock_get_vad.return_value = mock_vad_instance

        # Mock flatness to be high (noise)
        mock_compute_flatness.return_value = np.array([0.9])

        self.processor.setup()

        # Audio data doesn't matter much now because we mock the DSP output
        audio = np.zeros(16000, dtype=np.int16)

        result = self.processor.check_vad(audio)
        self.assertFalse(result)
        # VAD evaluate should NOT be called because it should be dropped by heuristic
        mock_vad_instance.evaluate.assert_not_called()


    @patch("backend.pipeline.transcription.audio_processor.get_gcs_client")
    @patch("backend.pipeline.transcription.audio_processor.get_vad_plugin")
    def test_download_audio_and_detect_calculates_duration(
        self, mock_get_vad: MagicMock, mock_get_gcs: MagicMock
    ) -> None:
        """Tests that download_audio_and_detect calculates duration when not provided."""
        self.processor.setup()
        self.processor.gcs_client = MagicMock()
        mock_bucket = MagicMock()
        mock_blob = MagicMock()

        # Create a tiny valid FLAC (100ms -> 1600 samples)
        audio = np.zeros(((100) * 16), dtype=np.int16)
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
            # duration_ms omitted
        )

        # Assert
        self.assertEqual(result.duration_ms, 100)  # 1600 / 16 = 100


    def test_preprocess_audio_applies_bandpass(self) -> None:
        """Verifies that the audio preprocessing filters do not corrupt or truncate the np.ndarray structure."""
        # A 1-second audio segment with noise at different frequencies
        audio = np.zeros(((1000) * 16), dtype=np.int16)

        # We can't easily assert exactly what the pydub filters did without evaluating frequency domains,
        # so we just assert it returns an np.ndarray and doesn't crash.
        processed = self.processor.preprocess_audio(audio)
        self.assertIsInstance(processed, np.ndarray)
        self.assertEqual(len(processed), len(audio))

    @unittest.skipIf(
        shutil.which("ffmpeg") is None, "ffmpeg is required for pydub I/O tests"
    )
    def test_export_flac(self) -> None:
        """Tests that exporting to FLAC produces a valid byte array containing the expected `fLaC` header signature."""
        audio = np.zeros(((500) * 16), dtype=np.int16)
        flac_bytes = self.processor.export_flac(audio)
        self.assertIsInstance(flac_bytes, bytes)
        self.assertTrue(flac_bytes.startswith(b"fLaC"))

    @unittest.skipIf(
        shutil.which("ffmpeg") is None, "ffmpeg is required for pydub I/O tests"
    )
    def test_export_m4a(self) -> None:
        """Tests that exporting to M4A produces a valid byte array with valid ftyp header."""
        audio = np.zeros(((500) * 16), dtype=np.int16)
        m4a_bytes = self.processor.export_m4a(audio)
        self.assertIsInstance(m4a_bytes, bytes)
        self.assertTrue(len(m4a_bytes) > 0)
        # M4A (MP4 container) should contain an ftyp box
        self.assertIn(b"ftyp", m4a_bytes)

    @unittest.skipIf(
        shutil.which("ffmpeg") is None, "ffmpeg is required for pydub I/O tests"
    )
    @patch("backend.pipeline.transcription.audio_processor.get_vad_plugin")
    @patch(
        "backend.pipeline.transcription.audio_processor.AcousticGateDetector"
    )
    @patch("backend.pipeline.transcription.audio_processor.get_gcs_client")
    def test_download_audio_and_detect(
        self,
        mock_get_gcs: MagicMock,
        mock_detector_cls: MagicMock,
        mock_get_vad: MagicMock,
    ) -> None:
        """Simulates downloading a GCS FLAC file, mocking its associated Sound Event Detection (SED) metadata, and parsing it into AudioChunkData."""
        mock_detector_instance = MagicMock()
        mock_detector_instance.detect.return_value = [TimeRange(5000, 7000)]
        mock_detector_cls.return_value = mock_detector_instance

        processor = AudioProcessor(vad_type=VadType.TEN_VAD)
        processor.setup()
        processor.gcs_client = MagicMock()
        mock_bucket = MagicMock()
        mock_blob = MagicMock()

        # Create a tiny valid FLAC
        audio = np.zeros(((100) * 16), dtype=np.int16)
        buf = io.BytesIO()
        sf.write(buf, audio, 16000, format="FLAC")
        flac_bytes = buf.getvalue()

        def download_to_file(f: io.BytesIO, **kwargs: object) -> None:

            f.write(flac_bytes)

        mock_blob.download_to_file = download_to_file
        mock_bucket.get_blob.return_value = mock_blob
        processor.gcs_client.bucket.return_value = mock_bucket

        # Act
        result = processor.download_audio_and_detect(
            "gs://my-bucket/audio/feed1/12345.flac", start_ms=5000
        )

        # Assert
        mock_detector_instance.detect.assert_called_once()

        self.assertIsInstance(result, AudioChunkData)
        self.assertEqual(result.start_ms, 5000)
        self.assertIsInstance(result.audio, np.ndarray)
        self.assertAlmostEqual(len(result.audio) / 16000.0, 0.1, places=2)
        self.assertEqual(result.speech_segments, [TimeRange(5000, 7000)])
        processor.gcs_client.bucket.assert_called_with("my-bucket")
        mock_bucket.get_blob.assert_called_with("audio/feed1/12345.flac")

    @patch("backend.pipeline.transcription.audio_processor.get_vad_plugin")
    @patch("backend.pipeline.transcription.audio_processor.get_gcs_client")
    def test_download_audio_not_found(
        self, mock_get_gcs: MagicMock, mock_get_vad: MagicMock
    ) -> None:
        """Ensures a FileNotFoundError is explicitly raised if the requested GCS audio blob does not exist in the bucket."""
        # Arrange
        processor = AudioProcessor(vad_type=VadType.TEN_VAD)
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
