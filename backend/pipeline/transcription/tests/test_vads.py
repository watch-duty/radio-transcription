"""Unit tests for the Voice Activity Detection plugins."""

import unittest
from unittest.mock import MagicMock, patch

import numpy as np

from backend.pipeline.transcription.enums import VadType
from backend.pipeline.transcription.vads import TenVadPlugin, get_vad_plugin


class TestVadPlugins(unittest.TestCase):
    def test_get_vad_plugin_ten_vad(self) -> None:
        """Verifies that the factory method accurately instantiates the correct TenVadPlugin class when requested utilizing accurate JSON configuration structures."""
        with patch(
            "backend.pipeline.transcription.vads.TenVadPlugin.setup"
        ) as mock_setup:
            plugin = get_vad_plugin(VadType.TEN_VAD, '{"threshold": 0.5}')
            self.assertIsInstance(plugin, TenVadPlugin)
            mock_setup.assert_called_once_with('{"threshold": 0.5}')

    def test_get_vad_plugin_unknown(self) -> None:
        """Verifies that attempting to synthesize an unsupported or entirely invalid VadType explicitly raises a distinct ValueError immediately."""
        with self.assertRaises(ValueError):
            get_vad_plugin("unknown_type", "{}")  # type: ignore

    def test_get_vad_plugin_invalid_json(self) -> None:
        """Verifies that deploying a functionally valid VadType with maliciously or malformed JSON payload parameters is strictly intercepted and blocked by a ValueError."""
        with self.assertRaises(ValueError):
            get_vad_plugin(VadType.TEN_VAD, "invalid-json")

    @patch("ten_vad.TenVad")
    def test_ten_vad_plugin_evaluate_speech_mocked(
        self, mock_ten_vad_class: MagicMock
    ) -> None:
        """Verifies that active speech sequences exceeding the minimum configured duration boundary comprehensively return True despite surrounding silence."""
        mock_instance = MagicMock()
        # Mock process to return (probability, flags)
        # At 16000Hz, hop_size=256, 1 frame = 16ms. 0.25s = ~16 frames.
        # Let's say it returns (0.9, 0) for 20 frames (0.32s).
        mock_instance.process.side_effect = [(0.9, 0)] * 20 + [(0.1, 0)] * 10
        mock_ten_vad_class.return_value = mock_instance

        plugin = TenVadPlugin()
        plugin.setup('{"min_speech_ms": 250}')

        # 30 frames * 256 samples = 7680 samples. * 2 bytes = 15360 bytes.
        dummy_pcm_data = b"\x00" * 15360
        result = plugin.evaluate(dummy_pcm_data, 16000)
        self.assertTrue(result)
        self.assertEqual(mock_instance.process.call_count, 30)

    @patch("ten_vad.TenVad")
    def test_ten_vad_plugin_evaluate_silence_mocked(
        self, mock_ten_vad_class: MagicMock
    ) -> None:
        """Verifies that excessively fragmented microscopic speech utterances severely below the required threshold context evaluate safely as overall silence (False)."""
        mock_instance = MagicMock()
        # Only 5 frames of speech (0.08s), below the 0.25s threshold
        mock_instance.process.side_effect = [(0.9, 0)] * 5 + [(0.1, 0)] * 25
        mock_ten_vad_class.return_value = mock_instance

        plugin = TenVadPlugin()
        plugin.setup('{"min_speech_ms": 250}')

        dummy_silence_data = b"\x00" * 15360
        result = plugin.evaluate(dummy_silence_data, 16000)
        self.assertFalse(result)
        self.assertEqual(mock_instance.process.call_count, 30)

    def test_ten_vad_plugin_integration_silence(self) -> None:
        """Verifies that the underlying real-world ONNX engine legitimately analyzes pure array zero-data (digital silence) and confidently denies speech activity natively."""
        plugin = TenVadPlugin()
        try:
            # Ensure it requires at least 0.25s of speech to pass
            plugin.setup('{"min_speech_ms": 250}')
        except OSError as e:
            if "libc++.so.1" in str(e):
                self.skipTest(
                    "Skipping integration test: libc++.so.1 not found on system."
                )
            raise

        # 1 second of absolute silence at 16kHz
        silence_pcm = np.zeros(16000, dtype=np.int16).tobytes()

        # This will actually run the ONNX model locally!
        result = plugin.evaluate(silence_pcm, 16000)

        # Silence should confidently be rejected.
        self.assertFalse(result)
