import base64
import os
import unittest
from unittest.mock import MagicMock, patch

with (
    patch("google.cloud.logging.Client"),
    patch("google.cloud.pubsub_v1.PublisherClient"),
):
    from backend.pipeline.schema_types.raw_audio_chunk_pb2 import AudioChunk
    from backend.pipeline.transcription.audio_fetcher import AudioFetcher
    from backend.pipeline.transcription.transcriber import BaseTranscriber
    from backend.pipeline.transcription.transcription_handler import (
        handle_transcription_event,
    )


class MockTranscriber(BaseTranscriber):
    """Mock implementation for polymorphism testing."""

    def transcribe(self, wav_data: bytes) -> str | None:
        return '{"events": [{"unit": "MockUnit", "message": "MockMessage", "is_dispatch": false}]}'


class MockAudioFetcher(AudioFetcher):
    """Mock implementation for audio fetcher testing."""

    def fetch(self, uri: str) -> bytes:
        return b"mock_audio_bytes"


class TestTranscriptionHandler(unittest.TestCase):
    """Tests for the transcription event handler."""

    @patch(
        "backend.pipeline.transcription.transcription_handler.pubsub_v1.PublisherClient"
    )
    @patch("backend.pipeline.transcription.transcription_handler.GCSAudioFetcher")
    def test_handle_transcription_event_polymorphism(
        self, mock_audio_fetcher_class, mock_publisher_class
    ) -> None:
        """Test the handler works with a custom polymorphic transcriber and audio fetcher."""
        # Mock Publisher
        mock_publisher = MagicMock()
        mock_publisher_class.return_value = mock_publisher
        mock_publisher.topic_path.return_value = "projects/test/topics/test"
        mock_publisher.publish.return_value.result.return_value = "msg_id_polymorph"

        # Mock AudioChunk
        gcs_uri = "gs://test-bucket/test.wav"
        audio_chunk = AudioChunk(gcs_file_path=gcs_uri)
        message_data = audio_chunk.SerializeToString()
        encoded_data = base64.b64encode(message_data).decode("utf-8")

        # Mock CloudEvent
        data = {
            "message": {
                "data": encoded_data,
                "attributes": {
                    "feed_id": "feed_poly",
                    "timestamp": "2024-01-01T00:00:00Z",
                },
            }
        }
        mock_event = MagicMock()
        mock_event.data = data

        custom_transcriber = MockTranscriber()
        custom_audio_fetcher = MockAudioFetcher()

        env_vars = {
            "GOOGLE_CLOUD_PROJECT": "test-project",
            "OUTPUT_TOPIC": "test-topic",
        }
        with patch.dict(os.environ, env_vars):
            handle_transcription_event(
                mock_event,
                transcriber=custom_transcriber,
                audio_fetcher=custom_audio_fetcher,
            )

        # Verify publisher called with mock transcriber's output
        mock_publisher.publish.assert_called_once()
        call_args = mock_publisher.publish.call_args
        published_data = call_args[0][1]
        self.assertIn(b"MockUnit", published_data)

    @patch(
        "backend.pipeline.transcription.transcription_handler.pubsub_v1.PublisherClient"
    )
    @patch("backend.pipeline.transcription.transcription_handler.GCSAudioFetcher")
    @patch("backend.pipeline.transcription.transcription_handler.GeminiTranscriber")
    def test_handle_transcription_event_default_success(
        self, mock_transcriber_class, mock_audio_fetcher_class, mock_publisher_class
    ) -> None:
        """Test the default Gemini flow on success."""
        mock_transcriber = MagicMock()
        mock_transcriber_class.return_value = mock_transcriber
        mock_transcriber.transcribe.return_value = (
            '{"events": [{"unit": "Dispatch", "message": "Copy", "is_dispatch": true}]}'
        )

        mock_audio_fetcher = MagicMock()
        mock_audio_fetcher_class.return_value = mock_audio_fetcher
        mock_audio_fetcher.fetch.return_value = b"fetched_audio_bytes"

        mock_publisher = MagicMock()
        mock_publisher_class.return_value = mock_publisher
        mock_publisher.topic_path.return_value = "projects/test/topics/test"
        mock_publisher.publish.return_value.result.return_value = "msg_id_123"

        gcs_uri = "gs://test-bucket/test.wav"
        audio_chunk = AudioChunk(gcs_file_path=gcs_uri)
        message_data = audio_chunk.SerializeToString()
        encoded_data = base64.b64encode(message_data).decode("utf-8")

        data = {
            "message": {
                "data": encoded_data,
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

        mock_audio_fetcher.fetch.assert_called_once_with(gcs_uri)
        mock_transcriber.transcribe.assert_called_once_with(b"fetched_audio_bytes")
        mock_publisher.publish.assert_called_once()


if __name__ == "__main__":
    unittest.main()
