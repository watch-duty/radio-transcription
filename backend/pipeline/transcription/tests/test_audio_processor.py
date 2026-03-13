"""Unit tests for the audio processor."""

import io
import shutil
import unittest
from unittest.mock import MagicMock, patch

from pydub import AudioSegment

from backend.pipeline.transcription.audio_processor import AudioProcessor
from backend.pipeline.transcription.constants import AUDIO_FORMAT, SAMPLE_RATE_HZ
from backend.pipeline.transcription.datatypes import AudioChunkData, TimeRange
from backend.pipeline.transcription.enums import VadType


class AudioProcessorTest(unittest.TestCase):
    """Tests for the AudioProcessor setup and evaluation methods."""

    def setUp(self) -> None:
        self.processor = AudioProcessor(vad_type=VadType.TEN_VAD)

    @patch("backend.pipeline.transcription.audio_processor.get_gcs_client")
    @patch("backend.pipeline.transcription.audio_processor.get_vad_plugin")
    def test_setup_initializes_vad_and_gcs(
        self, mock_get_vad: MagicMock, mock_get_gcs: MagicMock
    ) -> None:
        """Test that setup initializes the VAD plugin and GCS client correctly."""
        self.processor.setup()
        mock_get_vad.assert_called_once_with(VadType.TEN_VAD, "{}")
        mock_get_gcs.assert_called_once()
        self.assertIsNotNone(self.processor.vad)
        self.assertEqual(self.processor.gcs_client, mock_get_gcs.return_value)

    def test_check_vad_raises_if_not_setup(self) -> None:
        """Test that check_vad raises RuntimeError if called before setup."""
        audio = AudioSegment.silent(duration=1000)
        with self.assertRaises(RuntimeError):
            self.processor.check_vad(audio)

    def test_download_audio_raises_if_not_setup(self) -> None:
        """Test that download_audio_and_sed raises RuntimeError if called before setup."""
        with self.assertRaises(RuntimeError):
            self.processor.download_audio_and_sed("gs://bucket/path/to/missing.flac")

    @patch("backend.pipeline.transcription.audio_processor.get_vad_plugin")
    def test_check_vad_evaluates_speech(self, mock_get_vad: MagicMock) -> None:
        """Test check_vad correctly evaluating audio data."""
        mock_vad_instance = mock_get_vad.return_value
        mock_vad_instance.evaluate.return_value = True
        self.processor.setup()

        audio = AudioSegment.silent(duration=1000)
        result = self.processor.check_vad(audio)

        self.assertTrue(result)
        mock_vad_instance.evaluate.assert_called_once()
        # Ensure it got PCM bytes of the right format
        args, kwargs = mock_vad_instance.evaluate.call_args
        self.assertIsInstance(args[0], bytes)
        self.assertEqual(kwargs["sample_rate"], SAMPLE_RATE_HZ)

    def test_preprocess_audio_applies_bandpass(self) -> None:
        """Test that preprocess_audio successfully filters the audio."""
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
        """Test FLAC format conversion."""
        audio = AudioSegment.silent(duration=500)
        flac_bytes = self.processor.export_flac(audio)
        self.assertIsInstance(flac_bytes, bytes)
        self.assertTrue(flac_bytes.startswith(b"fLaC"))

    @unittest.skipIf(
        shutil.which("ffmpeg") is None, "ffmpeg is required for pydub I/O tests"
    )
    @patch("backend.pipeline.transcription.audio_processor.read_sed_segments_from_blob")
    def test_download_audio_and_sed(self, mock_read_sed: MagicMock) -> None:
        """Test downloading and parsing an audio chunk from GCS."""
        self.processor.gcs_client = MagicMock()
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
        self.processor.gcs_client.bucket.return_value = mock_bucket

        mock_read_sed.return_value = (500, [TimeRange(0, 100)])

        result = self.processor.download_audio_and_sed("gs://bucket/path/to/file.flac")

        mock_read_sed.assert_called_once_with(mock_blob)

        self.assertIsInstance(result, AudioChunkData)
        self.assertEqual(result.start_ms, 500)
        self.assertIsInstance(result.audio, AudioSegment)
        self.assertAlmostEqual(result.audio.duration_seconds, 0.1, places=2)
        self.assertEqual(result.speech_segments, [TimeRange(0, 100)])
        self.processor.gcs_client.bucket.assert_called_with("bucket")
        mock_bucket.get_blob.assert_called_with("path/to/file.flac")

    def test_download_audio_not_found(self) -> None:
        """Test that missing GCS blob raises FileNotFoundError."""
        self.processor.gcs_client = MagicMock()
        mock_bucket = MagicMock()
        mock_bucket.get_blob.return_value = None
        self.processor.gcs_client.bucket.return_value = mock_bucket

        with self.assertRaises(FileNotFoundError):
            self.processor.download_audio_and_sed("gs://bucket/path/to/missing.flac")
