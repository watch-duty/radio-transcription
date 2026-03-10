import unittest
from unittest.mock import MagicMock, patch

with (
    patch("google.cloud.logging.Client"),
    patch("google.cloud.pubsub_v1.PublisherClient"),
):
    from backend.pipeline.transcription.transcriber import (
        BaseTranscriber,
        GeminiTranscriber,
    )


class TestGeminiTranscriber(unittest.TestCase):
    """Tests for GeminiTranscriber implementation."""

    def test_base_transcriber_is_abstract(self) -> None:
        """Verify BaseTranscriber cannot be instantiated directly."""
        with self.assertRaises(TypeError):
            BaseTranscriber()

    def test_transcriber_mock_mode(self) -> None:
        """Test transcriber returns mock transcript when no API key is provided."""
        transcriber = GeminiTranscriber(api_key=None)
        result = transcriber.transcribe(b"dummy_data")
        self.assertIsInstance(result, str)
        if isinstance(result, str):
            self.assertIn("a fire has been transcribed", result)

    @patch("google.genai.Client")
    def test_transcriber_success(self, mock_client_class) -> None:
        """Test transcriber successfully calls Gemini API."""
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client

        mock_response = MagicMock()
        mock_response.text = '{"events": [{"unit": "Engine 1", "message": "On scene", "is_dispatch": false}]}'
        mock_client.models.generate_content.return_value = mock_response

        transcriber = GeminiTranscriber(api_key="fake_key")
        result = transcriber.transcribe(b"dummy_audio_data")

        self.assertIsInstance(result, str)
        if isinstance(result, str):
            self.assertIn("Engine 1", result)
        mock_client.models.generate_content.assert_called_once()

    @patch("google.genai.Client")
    def test_transcriber_error(self, mock_client_class) -> None:
        """Test transcriber handles API error gracefully."""
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        mock_client.models.generate_content.side_effect = Exception("API Error")

        transcriber = GeminiTranscriber(api_key="fake_key")
        result = transcriber.transcribe(b"dummy_audio_data")

        self.assertIsNone(result)


if __name__ == "__main__":
    unittest.main()
