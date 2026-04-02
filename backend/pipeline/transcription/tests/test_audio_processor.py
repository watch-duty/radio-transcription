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

from pydub import AudioSegment
from pydub.generators import Sine

from backend.pipeline.common.constants import AUDIO_FORMAT, SAMPLE_RATE_HZ
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
    @patch("backend.pipeline.transcription.audio_processor.ten_vad.TenVad")
    def test_setup_initializes_vad_and_gcs(
        self, mock_get_vad: MagicMock, mock_get_gcs: MagicMock
    ) -> None:
        """Verifies that calling setup() correctly instantiates the lazy-loaded VAD and GCS client."""
        self.processor.setup()
        mock_get_vad.assert_called_once_with(threshold=0.4, hop_size=256)
        mock_get_gcs.assert_called_once()
        self.assertIsNotNone(self.processor.vad)
        self.assertEqual(self.processor.gcs_client, mock_get_gcs.return_value)

    @patch("backend.pipeline.transcription.audio_processor.get_gcs_client")
    @patch(
        "backend.pipeline.transcription.audio_processor.sherpa_onnx.VoiceActivityDetector"
    )
    @patch(
        "backend.pipeline.transcription.audio_processor.sherpa_onnx.OfflineSpeechDenoiser"
    )
    def test_setup_initializes_sherpa_vad(
        self,
        mock_get_denoiser: MagicMock,
        mock_get_vad: MagicMock,
        mock_get_gcs: MagicMock,
    ) -> None:
        """Verifies that calling setup() correctly instantiates Sherpa VAD and Denoiser when SILERO_VAD is selected."""
        processor = AudioProcessor(vad_type=VadType.SILERO_VAD)
        processor.setup()

        mock_get_vad.assert_called_once()
        mock_get_denoiser.assert_called_once()
        mock_get_gcs.assert_called_once()
        self.assertIsNotNone(processor.vad)
        self.assertIsNotNone(processor.denoiser)

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
        processor = AudioProcessor(vad_type=VadType.TEN_VAD)
        # Act & Assert
        with self.assertRaises(RuntimeError):
            processor.download_audio_and_detect("gs://test/file.flac", 0)

    @patch("backend.pipeline.transcription.audio_processor.get_gcs_client")
    @patch("backend.pipeline.transcription.audio_processor.ten_vad.TenVad")
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

    @unittest.skipIf(
        shutil.which("ffmpeg") is None, "ffmpeg is required for pydub I/O tests"
    )
    @patch("backend.pipeline.transcription.audio_processor.ten_vad.TenVad")
    @patch("backend.pipeline.transcription.audio_processor.get_gcs_client")
    def test_download_audio_and_detect(
        self,
        mock_get_gcs: MagicMock,
        mock_get_vad: MagicMock,
    ) -> None:
        """Simulates downloading a GCS FLAC file, mocking its associated VAD metadata, and parsing it into AudioChunkData."""
        processor = AudioProcessor(vad_type=VadType.TEN_VAD)
        processor.setup()
        processor.gcs_client = MagicMock()
        mock_bucket = MagicMock()
        mock_blob = MagicMock()

        # Mock the new hybrid VAD method
        processor._detect_speech_and_noise = MagicMock()
        processor._detect_speech_and_noise.return_value = (
            [TimeRange(5000, 7000)],
            [],
            [],
        )

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

        self.assertIsInstance(result, AudioChunkData)
        self.assertEqual(result.start_ms, 5000)
        self.assertIsInstance(result.audio, AudioSegment)
        self.assertAlmostEqual(result.audio.duration_seconds, 0.1, places=2)
        self.assertEqual(result.speech_segments, [])
        processor.gcs_client.bucket.assert_called_with("my-bucket")
        mock_bucket.get_blob.assert_called_with("audio/feed1/12345.flac")

    @patch("backend.pipeline.transcription.audio_processor.ten_vad.TenVad")
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

    @unittest.skip("Legacy test for missing method _detect_clicks_derivative")
    @patch(
        "backend.pipeline.transcription.audio_processor.RadioSignalAnalyzer.characterize"
    )
    @patch(
        "backend.pipeline.transcription.audio_processor.AudioProcessor._detect_segments"
    )
    @patch(
        "backend.pipeline.transcription.audio_processor.AudioProcessor._detect_clicks_derivative"
    )
    def test_detect_speech_and_noise_trims_clicks(
        self, mock_detect_clicks, mock_detect_segments, mock_characterize
    ):
        """Verifies that _detect_speech_and_noise trims candidates based on detected clicks."""
        # Setup mocks
        from unittest.mock import MagicMock

        import numpy as np

        from backend.pipeline.transcription.datatypes import TimeRange

        # Mock VAD to return a candidate from 1000ms to 3000ms
        mock_detect_segments.return_value = [
            (TimeRange(start_ms=1000, end_ms=3000), 0.8)
        ]

        # Mock click detector to return a click at the end (2900ms to 2950ms)
        mock_detect_clicks.return_value = [
            TimeRange(start_ms=2900, end_ms=2950)
        ]

        # Mock characterize to return a valid speech result
        mock_result = MagicMock()
        mock_result.label = "speech"
        mock_result.confidence = 0.9
        mock_result.hnr = 20.0
        mock_result.trimmed_duration_ms = 1900
        mock_result.is_transcribable = True
        mock_characterize.return_value = mock_result

        # Create dummy samples (3 seconds at 16kHz)
        samples = np.zeros(3 * 16000, dtype=np.int16)

        # Fill candidate region (1000ms to 3000ms) with high value to pass energy threshold
        samples[16000:48000] = 30000

        # Call the method
        speech, noise, silence = self.processor._detect_speech_and_noise(
            samples, 0
        )

        # Verify that the speech segment was trimmed!
        # Original was 1000-3000. Click was at 2900-2950.
        # It should be trimmed to 1000-2900!
        self.assertEqual(len(speech), 1)
        self.assertEqual(speech[0].start_ms, 1000)
        self.assertEqual(speech[0].end_ms, 2900)
