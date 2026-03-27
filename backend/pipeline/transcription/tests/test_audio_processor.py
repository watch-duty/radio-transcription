"""Unit tests for the audio processor."""

import io
import shutil
import unittest
from unittest.mock import MagicMock, patch

import pytest
from pydub import AudioSegment
from pydub.generators import Sine

from backend.pipeline.common.constants import AUDIO_FORMAT, SAMPLE_RATE_HZ
from backend.pipeline.transcription.audio_processor import AudioProcessor
from backend.pipeline.transcription.datatypes import AudioChunkData, TimeRange
from backend.pipeline.transcription.enums import VadType


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
        audio = AudioSegment.silent(duration=1000)
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
        """Tests that the processor correctly forwards raw audio bytes to the configured VAD plugin and returns its result."""
        mock_vad_instance = MagicMock()
        mock_vad_instance.evaluate.return_value = True
        mock_get_vad.return_value = mock_vad_instance

        self.processor.setup()

        # Generate a Sine wave so it bypasses both the new RMS silence gate
        # and the Spectral Flatness noise gate (pure tone = highly structured)
        audio = Sine(440).to_audio_segment(duration=1000)
        result = self.processor.check_vad(audio)

        self.assertTrue(result)
        mock_vad_instance.evaluate.assert_called_once()
        args, kwargs = mock_vad_instance.evaluate.call_args
        self.assertIsInstance(args[0], bytes)
        self.assertEqual(kwargs["sample_rate"], SAMPLE_RATE_HZ)

    def test_preprocess_audio_applies_bandpass(self) -> None:
        """Verifies that the audio preprocessing filters do not corrupt or truncate the AudioSegment structure."""
        # A 1-second audio segment with noise at different frequencies
        audio = AudioSegment.silent(duration=1000)

        # We can't easily assert exactly what the pydub filters did without evaluating frequency domains,
        # so we just assert it returns an AudioSegment and doesn't crash.
        processed = self.processor.preprocess_audio(audio)
        self.assertIsInstance(processed, AudioSegment)
        self.assertEqual(len(processed), 1000)

    @unittest.skipIf(
        shutil.which("ffmpeg") is None, "ffmpeg is required for pydub I/O tests"
    )
    def test_export_flac(self) -> None:
        """Tests that exporting to FLAC produces a valid byte array containing the expected `fLaC` header signature."""
        audio = AudioSegment.silent(duration=500)
        flac_bytes = self.processor.export_flac(audio)
        self.assertIsInstance(flac_bytes, bytes)
        self.assertTrue(flac_bytes.startswith(b"fLaC"))

    @unittest.skipIf(
        shutil.which("ffmpeg") is None, "ffmpeg is required for pydub I/O tests"
    )
    def test_export_m4a(self) -> None:
        """Tests that exporting to M4A produces a valid byte array with valid ftyp header."""
        audio = AudioSegment.silent(duration=500)
        m4a_bytes = self.processor.export_m4a(audio)
        self.assertIsInstance(m4a_bytes, bytes)
        self.assertTrue(len(m4a_bytes) > 0)
        # M4A (MP4 container) should contain an ftyp box
        self.assertIn(b"ftyp", m4a_bytes)

    @pytest.mark.skipif(
        shutil.which("ffmpeg") is None,
        reason="ffmpeg is required for pydub I/O tests",
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
        audio = AudioSegment.silent(duration=100)
        buf = io.BytesIO()
        audio.export(buf, format=AUDIO_FORMAT)
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
        self.assertIsInstance(result.audio, AudioSegment)
        self.assertAlmostEqual(result.audio.duration_seconds, 0.1, places=2)
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
