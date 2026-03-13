"""Unit tests for the Voice Activity Detection plugins."""

import unittest
from unittest.mock import MagicMock, patch

import numpy as np

from backend.pipeline.transcription.enums import VadType
from backend.pipeline.transcription.vads import TenVadPlugin, get_vad_plugin


class TestVadPlugins(unittest.TestCase):
    """Tests for the VAD plugins."""

    def test_get_vad_plugin_ten_vad(self) -> None:
        """Test that the factory returns a TenVadPlugin and calls setup."""
        with patch(
            "backend.pipeline.transcription.vads.TenVadPlugin.setup"
        ) as mock_setup:
            plugin = get_vad_plugin(VadType.TEN_VAD, '{"threshold": 0.5}')
            self.assertIsInstance(plugin, TenVadPlugin)
            mock_setup.assert_called_once_with('{"threshold": 0.5}')

    def test_get_vad_plugin_unknown(self) -> None:
        """Test that the factory raises ValueError for unknown plugin types."""
        with self.assertRaises(ValueError):
            get_vad_plugin("unknown_type", "{}")  # type: ignore[invalid-argument-type]

    def test_get_vad_plugin_invalid_json(self) -> None:
        """Test that the factory raises ValueError for invalid JSON config."""
        with self.assertRaises(ValueError):
            get_vad_plugin(VadType.TEN_VAD, "invalid-json")

    @patch("ten_vad.TenVad")
    def test_ten_vad_plugin_evaluate_speech_mocked(
        self, mock_ten_vad_class: MagicMock
    ) -> None:
        """Test the duration gate logic by mocking the process method."""
        mock_instance = MagicMock()
        # Mock process to return (probability, flags)
        # At 16000Hz, hop_size=256, 1 frame = 16ms. 0.25s = ~16 frames.
        # Let's say it returns (0.9, 0) for 20 frames (0.32s).
        mock_instance.process.side_effect = [(0.9, 0)] * 20 + [(0.1, 0)] * 10
        mock_ten_vad_class.return_value = mock_instance

        plugin = TenVadPlugin()
        plugin.setup('{"min_speech_sec": 0.25}')

        # 30 frames * 256 samples = 7680 samples. * 2 bytes = 15360 bytes.
        dummy_pcm_data = b"\x00" * 15360
        result = plugin.evaluate(dummy_pcm_data, 16000)
        self.assertTrue(result)
        self.assertEqual(mock_instance.process.call_count, 30)

    @patch("ten_vad.TenVad")
    def test_ten_vad_plugin_evaluate_silence_mocked(
        self, mock_ten_vad_class: MagicMock
    ) -> None:
        """Test that TenVadPlugin.evaluate returns False when speech is too short."""
        mock_instance = MagicMock()
        # Only 5 frames of speech (0.08s), below the 0.25s threshold
        mock_instance.process.side_effect = [(0.9, 0)] * 5 + [(0.1, 0)] * 25
        mock_ten_vad_class.return_value = mock_instance

        plugin = TenVadPlugin()
        plugin.setup('{"min_speech_sec": 0.25}')

        dummy_silence_data = b"\x00" * 15360
        result = plugin.evaluate(dummy_silence_data, 16000)
        self.assertFalse(result)
        self.assertEqual(mock_instance.process.call_count, 30)

    def test_ten_vad_plugin_integration_silence(self) -> None:
        """Integration test: actually instantiate TenVad and process real silence."""
        plugin = TenVadPlugin()
        try:
            # Ensure it requires at least 0.25s of speech to pass
            plugin.setup('{"min_speech_sec": 0.25}')
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
