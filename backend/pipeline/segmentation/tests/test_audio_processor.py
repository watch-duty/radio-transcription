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

    def test_decode_audio_in_memory_golden(self) -> None:
        """Golden test: Verifies exact PCM audio sample decoding and sample rate extraction via PyAV."""
        expected_samples = np.array(
            [1000, -2000, 3000, -4000, 5000, -6000], dtype=np.int16
        )

        # Write to an in-memory FLAC container at 22050 Hz (testing source sample rate extraction)
        buf = io.BytesIO()
        sf.write(buf, expected_samples, 22050, format="FLAC")
        buf.seek(0)

        # Decode using in-process PyAV
        actual_samples, actual_sr = self.processor._decode_audio_in_memory(buf)

        self.assertEqual(actual_sr, 22050)
        np.testing.assert_array_equal(actual_samples, expected_samples)

    def test_decode_audio_in_memory_stereo_golden(self) -> None:
        """Golden test: Verifies multi-channel decoding and downstream downmix averaging."""
        channel0 = np.array([10, 20, 30, 40], dtype=np.int16)
        channel1 = np.array([100, 100, 100, 100], dtype=np.int16)
        stereo_array = np.vstack((channel0, channel1)).T  # (samples, 2)

        buf = io.BytesIO()
        sf.write(buf, stereo_array, 16000, format="FLAC")
        buf.seek(0)

        actual_samples, actual_sr = self.processor._decode_audio_in_memory(buf)

        self.assertEqual(actual_sr, 16000)
        np.testing.assert_array_equal(actual_samples, stereo_array)

        # Verify that _resample_to_16k_mono correctly averages (downmixes) the channels
        downmixed = self.processor._resample_to_16k_mono(
            actual_samples, actual_sr
        )
        expected_downmixed = np.array([55, 60, 65, 70], dtype=np.int16)
        self.assertEqual(downmixed.ndim, 1)
        np.testing.assert_array_equal(downmixed[:4], expected_downmixed)

    def test_download_audio_and_detect_with_prior_stereo_audio(self) -> None:
        """Tests that prior multi-channel audio is correctly downmixed to mono during streaming detection."""
        mock_vad = MagicMock()
        mock_vad.detect_speech_segments.return_value = []
        processor = SegmentationAudioProcessor(
            gcs_client_instance=MagicMock(),
            vad_factory=MagicMock(return_value=mock_vad),
        )
        processor.setup()

        # Create stereo current audio chunk
        curr_stereo = np.array([[10, 20], [30, 40]], dtype=np.int16)
        buf = io.BytesIO()
        sf.write(buf, curr_stereo, 32000, format="FLAC")
        buf.seek(0)
        # Create stereo prior audio tail bytes
        prior_stereo = np.array([[100, 200], [300, 400]], dtype=np.int16)
        prior_bytes = prior_stereo.tobytes()
        from unittest.mock import patch  # noqa: PLC0415

        with patch.object(
            processor,
            "_decode_audio_in_memory",
            return_value=(curr_stereo, 32000),
        ):
            processor.download_audio_and_detect(
                "gs://bucket/test.flac", 0, prior_audio=prior_bytes
            )

        # Verify that detect_speech_segments was called with correctly downmixed 1D mono arrays
        call_args = mock_vad.detect_speech_segments.call_args[1]
        actual_prior = call_args["prior_audio"]
        expected_prior = np.array([150, 350], dtype=np.int16)
        np.testing.assert_array_equal(actual_prior, expected_prior)
