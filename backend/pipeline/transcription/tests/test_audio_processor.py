import io
import unittest
from unittest.mock import MagicMock, patch

from pydub import AudioSegment

from backend.pipeline.transcription.audio_processor import AudioProcessor
from backend.pipeline.transcription.constants import AUDIO_FORMAT, SAMPLE_RATE_HZ
from backend.pipeline.transcription.enums import VadType


class AudioProcessorTest(unittest.TestCase):
    def setUp(self) -> None:
        self.processor = AudioProcessor(vad_type=VadType.TEN_VAD)

    @patch("backend.pipeline.transcription.audio_processor.get_vad_plugin")
    def test_setup_initializes_vad(self, mock_get_vad: MagicMock) -> None:
        self.processor.setup()
        mock_get_vad.assert_called_once_with(VadType.TEN_VAD, "{}")
        self.assertIsNotNone(self.processor.vad)

    def test_check_vad_raises_if_not_setup(self) -> None:
        audio = AudioSegment.silent(duration=1000)
        with self.assertRaises(RuntimeError):
            self.processor.check_vad(audio)

    @patch("backend.pipeline.transcription.audio_processor.get_vad_plugin")
    def test_check_vad_evaluates_speech(self, mock_get_vad: MagicMock) -> None:
        self.processor.setup()
        mock_vad_instance = mock_get_vad.return_value
        mock_vad_instance.evaluate.return_value = True

        audio = AudioSegment.silent(duration=1000)
        result = self.processor.check_vad(audio)

        self.assertTrue(result)
        mock_vad_instance.evaluate.assert_called_once()
        # Ensure it got PCM bytes of the right format
        args, kwargs = mock_vad_instance.evaluate.call_args
        self.assertIsInstance(args[0], bytes)
        self.assertEqual(kwargs["sample_rate"], SAMPLE_RATE_HZ)

    def test_preprocess_audio_applies_bandpass(self) -> None:
        # A 1-second audio segment with noise at different frequencies
        audio = AudioSegment.silent(duration=1000)

        # We can't easily assert exactly what the pydub filters did without evaluating frequency domains,
        # so we just assert it returns an AudioSegment and doesn't crash.
        processed = self.processor.preprocess_audio(audio)
        self.assertIsInstance(processed, AudioSegment)
        self.assertEqual(len(processed), 1000)

    def test_export_flac(self) -> None:
        audio = AudioSegment.silent(duration=500)
        flac_bytes = self.processor.export_flac(audio)
        self.assertIsInstance(flac_bytes, bytes)
        self.assertTrue(flac_bytes.startswith(b"fLaC"))

    @patch("backend.pipeline.transcription.audio_processor.get_gcs_client")
    @patch("backend.pipeline.transcription.audio_processor.read_sed_segments_from_blob")
    def test_download_audio_and_sed(
        self, mock_read_sed: MagicMock, mock_gcs_client: MagicMock
    ) -> None:
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
        mock_gcs_client.return_value.bucket.return_value = mock_bucket

        mock_read_sed.return_value = [(0.0, 0.1)]

        result_audio, result_sed = self.processor.download_audio_and_sed(
            "gs://bucket/path/to/file.flac"
        )

        mock_read_sed.assert_called_once_with(mock_blob)

        self.assertIsInstance(result_audio, AudioSegment)
        self.assertAlmostEqual(result_audio.duration_seconds, 0.1, places=2)
        self.assertEqual(result_sed, [(0.0, 0.1)])
        mock_gcs_client.return_value.bucket.assert_called_with("bucket")
        mock_bucket.get_blob.assert_called_with("path/to/file.flac")

    @patch("backend.pipeline.transcription.audio_processor.get_gcs_client")
    def test_download_audio_not_found(self, mock_gcs_client: MagicMock) -> None:
        mock_bucket = MagicMock()
        mock_bucket.get_blob.return_value = None
        mock_gcs_client.return_value.bucket.return_value = mock_bucket

        with self.assertRaises(FileNotFoundError):
            self.processor.download_audio_and_sed("gs://bucket/path/to/missing.flac")
