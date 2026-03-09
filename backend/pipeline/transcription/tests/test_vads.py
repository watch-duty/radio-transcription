import unittest
from unittest.mock import MagicMock, patch

from enums import VadType
from vads import TenVadPlugin, get_vad_plugin


class TestVadPlugins(unittest.TestCase):
    def test_get_vad_plugin_ten_vad(self) -> None:
        """Test that the factory returns a TenVadPlugin and calls setup."""
        with patch("vads.TenVadPlugin.setup") as mock_setup:
            plugin = get_vad_plugin(VadType.TEN_VAD, '{"threshold": 0.5}')
            self.assertIsInstance(plugin, TenVadPlugin)
            mock_setup.assert_called_once_with('{"threshold": 0.5}')

    def test_get_vad_plugin_unknown(self) -> None:
        """Test that the factory raises ValueError for unknown plugin types."""
        with self.assertRaises(ValueError):
            get_vad_plugin("unknown_type", "{}")  # type: ignore

    def test_get_vad_plugin_invalid_json(self) -> None:
        """Test that the factory raises ValueError for invalid JSON config."""
        with self.assertRaises(ValueError):
            get_vad_plugin(VadType.TEN_VAD, "invalid-json")

    @patch("ten_vad.TenVad")
    def test_ten_vad_plugin_evaluate_speech(
        self, mock_ten_vad_class: MagicMock
    ) -> None:
        """Test that TenVadPlugin.evaluate returns True when speech is detected."""
        mock_instance = MagicMock()
        mock_instance.evaluate.return_value = [{"start": 0.1, "end": 0.5}]
        mock_ten_vad_class.return_value = mock_instance

        plugin = TenVadPlugin()
        plugin.setup("{}")

        result = plugin.evaluate(b"dummy_pcm_data", 16000)
        self.assertTrue(result)
        mock_instance.evaluate.assert_called_once_with(
            b"dummy_pcm_data", sample_rate=16000
        )

    @patch("ten_vad.TenVad")
    def test_ten_vad_plugin_evaluate_silence(
        self, mock_ten_vad_class: MagicMock
    ) -> None:
        """Test that TenVadPlugin.evaluate returns False when no speech is detected."""
        mock_instance = MagicMock()
        mock_instance.evaluate.return_value = []
        mock_ten_vad_class.return_value = mock_instance

        plugin = TenVadPlugin()
        plugin.setup("{}")

        result = plugin.evaluate(b"dummy_silence_data", 16000)
        self.assertFalse(result)
        mock_instance.evaluate.assert_called_once_with(
            b"dummy_silence_data", sample_rate=16000
        )
