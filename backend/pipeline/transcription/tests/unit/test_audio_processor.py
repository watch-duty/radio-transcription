import sys
from unittest.mock import MagicMock

# Save original if exists
original_sherpa = sys.modules.get("sherpa_onnx")

# Mock sherpa_onnx before importing anything that might use it
mock_sherpa = MagicMock()
sys.modules["sherpa_onnx"] = mock_sherpa

import io
import logging
import shutil
import unittest
from unittest.mock import MagicMock, patch

from backend.pipeline.transcription.audio.audio_processor import AudioProcessor


import pytest
import numpy as np

from backend.pipeline.common.constants import AUDIO_FORMAT
from backend.pipeline.transcription.audio.audio_processor import AudioProcessor
from backend.pipeline.transcription.datatypes import (
    AudioChunkData,
    TimeRange,
    VadResult,
)

logger = logging.getLogger(__name__)

# Warn if ffmpeg is missing for I/O tests
if shutil.which("ffmpeg") is None:
    logger.warning(
        "FFMPEG is not installed. Audio I/O tests requiring ffmpeg will be skipped."
    )


class AudioProcessorTest(unittest.TestCase):
    def setUp(self) -> None:
        self.processor = AudioProcessor()
        # Mock VAD creation to avoid loading missing model file
        self.patcher_vad = patch.object(AudioProcessor, "_create_sherpa_vad")
        self.mock_create_vad = self.patcher_vad.start()
        self.mock_create_vad.return_value = MagicMock()

        # Mock Denoiser creation to avoid loading missing model file
        self.patcher_denoiser = patch.object(
            AudioProcessor, "_create_sherpa_denoiser"
        )
        self.mock_create_denoiser = self.patcher_denoiser.start()
        self.mock_create_denoiser.return_value = MagicMock()

    def tearDown(self) -> None:
        self.patcher_vad.stop()
        self.patcher_denoiser.stop()

    def test_download_audio_raises_if_not_setup(self) -> None:
        """Ensures that downloading audio before calling setup() correctly raises a runtime error to prevent missing GCS client exceptions."""
        # Create a new processor instance that hasn't been set up
        processor = AudioProcessor()
        # Act & Assert
        with self.assertRaises(RuntimeError):
            processor.download_audio_and_detect(
                "gs://test/file.flac", 0, session_id="test-session"
            )

    @patch(
        "backend.pipeline.transcription.audio.audio_processor._get_gcs_client"
    )
    def test_download_audio_not_found(self, mock_get_gcs: MagicMock) -> None:
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

    @patch(
        "backend.pipeline.transcription.audio.audio_processor._get_gcs_client"
    )
    def test_download_audio_dirty_file_sanitization(
        self, mock_get_gcs: MagicMock
    ) -> None:
        """Verifies that download_audio_and_detect can handle files with non-standard metadata using ffmpeg."""
        # Arrange
        processor = AudioProcessor()
        processor.setup()
        processor.gcs_client = MagicMock()
        mock_bucket = MagicMock()
        mock_blob = MagicMock()

        # Create a valid clean FLAC in memory
        clean_flac = io.BytesIO()
        dummy_audio = np.zeros(16000, dtype=np.int16)  # 1 second of silence
        import soundfile as sf

        sf.write(clean_flac, dummy_audio, 16000, format="FLAC")
        clean_flac.seek(0)

        # Create dirty content by prepending garbage
        dirty_content = (
            b"ICY 200 OK\r\nicy-notice1: spam\r\n\r\n" + clean_flac.read()
        )

        # Mock blob.download_to_file to write dirty content
        def mock_download(fileobj):
            fileobj.write(dirty_content)
            return None

        mock_blob.download_to_file.side_effect = mock_download

        mock_bucket.get_blob.return_value = mock_blob
        processor.gcs_client.bucket.return_value = mock_bucket

        # Mock VAD and Denoiser creation and detection to avoid missing model files
        with (
            patch.object(processor, "_create_sherpa_vad") as mock_vad,
            patch.object(processor, "_create_sherpa_denoiser") as mock_denoiser,
            patch.object(processor, "_detect_speech_and_noise") as mock_detect,
        ):
            mock_vad.return_value = MagicMock()
            mock_denoiser.return_value = MagicMock()
            mock_detect.return_value = ([], [], [])

            # Act
            chunk = processor.download_audio_and_detect(
                "gs://my-bucket/dirty.flac", 0, session_id="test-session"
            )

            # Assert
            self.assertIsInstance(chunk, AudioChunkData)
            self.assertEqual(chunk.original_sr, 16000)
            self.assertEqual(len(chunk.audio), 16000)  # 16kHz
            self.assertEqual(
                len(chunk.stored_audio), 16000
            )  # original (which is also 16k in this dummy)

    def test_detect_speech_and_noise_raises_if_not_setup(self) -> None:
        """Verifies that detect_speech_and_noise raises RuntimeError if VAD is not setup."""
        processor = AudioProcessor()
        samples = np.zeros(16000, dtype=np.float32)
        with self.assertRaises(RuntimeError) as cm:
            processor._detect_speech_and_noise(samples, 0)
        self.assertEqual(str(cm.exception), "VAD instance is not initialized")

    @patch(
        "backend.pipeline.transcription.audio.silero_vad.process_vad_streaming"
    )
    def test_detect_speech_and_noise_success(
        self, mock_process_vad: MagicMock
    ) -> None:
        """Verifies that detect_speech_and_noise succeeds with mocked VAD and Denoiser."""
        processor = AudioProcessor()
        processor.vad_sensitive = MagicMock()
        processor.vad_strict = MagicMock()
        processor.denoiser = MagicMock()
        processor.signal_analyzer = MagicMock()

        # Mock denoiser to return something
        mock_denoised = MagicMock()
        mock_denoised.samples = [0.1] * 16000  # 1 second at 16kHz
        processor.denoiser.run.return_value = mock_denoised

        # Mock VAD results
        # Pass 1 returns one coarse segment
        mock_process_vad.side_effect = [
            VadResult(
                speech_segments=[TimeRange(start_ms=100, end_ms=900)],
                silence_segments=[],
            ),
            # Pass 2 returns refined segment
            VadResult(
                speech_segments=[TimeRange(start_ms=200, end_ms=800)],
                silence_segments=[],
            ),
        ]

        # Mock signal analyzer to characterize as speech
        mock_char = MagicMock()
        mock_char.label = "speech"
        mock_char.confidence = 0.9
        processor.signal_analyzer.characterize.return_value = mock_char

        samples = np.zeros(16000, dtype=np.int16)
        speech, noise, silence = processor._detect_speech_and_noise(samples, 0)
        self.assertEqual(len(speech), 1)
        self.assertEqual(speech[0].start_ms, 0)
        self.assertEqual(speech[0].end_ms, 900)

    @patch(
        "backend.pipeline.transcription.audio.silero_vad.process_vad_streaming"
    )
    def test_detect_speech_candidates(
        self, mock_process_vad: MagicMock
    ) -> None:
        """Verifies that _detect_speech_candidates works in isolation."""
        processor = AudioProcessor()

        mock_vad_sensitive = MagicMock()
        mock_denoiser = MagicMock()
        mock_signal_analyzer = MagicMock()

        # Mock denoiser
        mock_denoised = MagicMock()
        mock_denoised.samples = [0.1] * 16000  # 1 second
        mock_denoiser.run.return_value = mock_denoised

        # Mock VAD
        mock_process_vad.return_value = VadResult(
            speech_segments=[TimeRange(start_ms=100, end_ms=500)],
            silence_segments=[],
        )

        # Mock signal analyzer
        mock_char = MagicMock()
        mock_char.is_transcribable = True
        mock_char.label = "speech"
        mock_signal_analyzer.characterize.return_value = mock_char

        samples = np.zeros(16000, dtype=np.int16)
        start_ms = 0

        speech, _, noise = processor._detect_speech_candidates(
            samples,
            start_ms,
            mock_vad_sensitive,
            mock_denoiser,
            mock_signal_analyzer,
        )

        self.assertEqual(len(speech), 1)
        # With default padding (pre=200, post=0), clipped to duration (1000ms)
        # [100, 500] -> [0, 500]
        self.assertEqual(speech[0].start_ms, 0)
        self.assertEqual(speech[0].end_ms, 500)
