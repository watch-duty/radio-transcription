import base64
import os
import unittest
from unittest.mock import MagicMock, patch

with (
    patch("google.cloud.logging.Client"),
    patch("google.cloud.pubsub_v1.PublisherClient"),
):
    from backend.pipeline.transcription.transcriber import BaseTranscriber
    from backend.pipeline.transcription.transcription_handler import (
        handle_transcription_event,
    )


class MockTranscriber(BaseTranscriber):
    """Mock implementation for polymorphism testing."""

    def transcribe(self, wav_data: bytes) -> str | None:
        return '{"events": [{"unit": "MockUnit", "message": "MockMessage", "is_dispatch": false}]}'


class TestTranscriptionHandler(unittest.TestCase):
    """Tests for the transcription event handler."""

    @patch(
        "backend.pipeline.transcription.transcription_handler.pubsub_v1.PublisherClient"
    )
    def test_handle_transcription_event_polymorphism(
        self, mock_publisher_class
    ) -> None:
        """Test the handler works with a custom polymorphic transcriber."""
        # Mock Publisher
        mock_publisher = MagicMock()
        mock_publisher_class.return_value = mock_publisher
        mock_publisher.topic_path.return_value = "projects/test/topics/test"
        mock_publisher.publish.return_value.result.return_value = "msg_id_polymorph"

        # Mock CloudEvent
        data = {
            "message": {
                "data": base64.b64encode(b"audio_bytes").decode("utf-8"),
                "attributes": {
                    "feed_id": "feed_poly",
                    "timestamp": "2024-01-01T00:00:00Z",
                },
            }
        }
        mock_event = MagicMock()
        mock_event.data = data

        custom_transcriber = MockTranscriber()

        env_vars = {
            "GOOGLE_CLOUD_PROJECT": "test-project",
            "OUTPUT_TOPIC": "test-topic",
        }
        with patch.dict(os.environ, env_vars):
            handle_transcription_event(mock_event, transcriber=custom_transcriber)

        # Verify publisher called with mock transcriber's output
        mock_publisher.publish.assert_called_once()
        call_args = mock_publisher.publish.call_args
        published_data = call_args[0][1]
        self.assertIn(b"MockUnit", published_data)

    @patch(
        "backend.pipeline.transcription.transcription_handler.pubsub_v1.PublisherClient"
    )
    @patch("backend.pipeline.transcription.transcription_handler.GeminiTranscriber")
    def test_handle_transcription_event_default_success(
        self, mock_transcriber_class, mock_publisher_class
    ) -> None:
        """Test the default Gemini flow on success."""
        mock_transcriber = MagicMock()
        mock_transcriber_class.return_value = mock_transcriber
        mock_transcriber.transcribe.return_value = (
            '{"events": [{"unit": "Dispatch", "message": "Copy", "is_dispatch": true}]}'
        )

        mock_publisher = MagicMock()
        mock_publisher_class.return_value = mock_publisher
        mock_publisher.topic_path.return_value = "projects/test/topics/test"
        mock_publisher.publish.return_value.result.return_value = "msg_id_123"

        data = {
            "message": {
                "data": base64.b64encode(b"audio_bytes").decode("utf-8"),
                "attributes": {
                    "feed_id": "feed_1",
                    "timestamp": "2024-01-01T00:00:00Z",
                },
            }
        }
        mock_event = MagicMock()
        mock_event.data = data

        env_vars = {
            "GOOGLE_CLOUD_PROJECT": "test-project",
            "OUTPUT_TOPIC": "test-topic",
        }
        with patch.dict(os.environ, env_vars):
            handle_transcription_event(mock_event)

        mock_transcriber.transcribe.assert_called_once()
        mock_publisher.publish.assert_called_once()


if __name__ == "__main__":
    unittest.main()
