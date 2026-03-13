"""Unit tests for the audio transcription plugins."""

import unittest
from unittest.mock import MagicMock, patch

from google.api_core.exceptions import GoogleAPIError

from backend.pipeline.transcription.constants import BYTES_PER_SECOND_16KHZ_MONO
from backend.pipeline.transcription.enums import TranscriberType
from backend.pipeline.transcription.transcribers import get_transcriber


class TestTranscribers(unittest.TestCase):
    """Tests for Transcriber plugins."""

    def test_google_chirp_transcriber_success(self) -> None:
        """Test successful Google Chirp v3 transcription."""
        with patch(
            "backend.pipeline.transcription.transcribers.SpeechClient"
        ) as mock_speech_client_cls:
            mock_client_instance = MagicMock()
            mock_speech_client_cls.return_value = mock_client_instance

            # Mock successful response
            mock_response = MagicMock()
            mock_result = MagicMock()
            mock_result.alternatives = [MagicMock(transcript="Hello world from Chirp")]
            mock_response.results = [mock_result]
            mock_client_instance.recognize.return_value = mock_response

            # Initialize Transcriber
            transcriber = get_transcriber(
                TranscriberType.GOOGLE_CHIRP_V3, "test-project", '{"location": "us"}'
            )
            transcriber.setup()

            # Execute transcribe
            dummy_audio = b"\x00" * int(BYTES_PER_SECOND_16KHZ_MONO * 2.5)

            transcript = transcriber.transcribe(
                audio_data=dummy_audio,
            )

            # Assert output
            self.assertEqual(transcript, "Hello world from Chirp")

            # Assert recognize called
            mock_client_instance.recognize.assert_called_once()

    def test_google_chirp_transcriber_background(self) -> None:
        """Test Google Chirp raising error on [BACKGROUND] silence."""
        with patch(
            "backend.pipeline.transcription.transcribers.SpeechClient"
        ) as mock_speech_client_cls:
            mock_client_instance = MagicMock()
            mock_speech_client_cls.return_value = mock_client_instance

            # Mock [BACKGROUND] response
            mock_response = MagicMock()
            mock_result = MagicMock()
            mock_result.alternatives = [MagicMock(transcript="[BACKGROUND]")]
            mock_response.results = [mock_result]
            mock_client_instance.recognize.return_value = mock_response

            transcriber = get_transcriber(
                TranscriberType.GOOGLE_CHIRP_V3, "test-project", "{}"
            )
            transcriber.setup()

            dummy_audio = b"\x00" * int(BYTES_PER_SECOND_16KHZ_MONO * 2.5)

            with self.assertRaises(ValueError) as context:
                transcriber.transcribe(
                    audio_data=dummy_audio,
                )

            self.assertIn("returned [BACKGROUND] only", str(context.exception))


    def test_google_chirp_transcriber_retry_on_google_api_error(self) -> None:
        """Test retry logic recovers from transient Google API errors."""
        with patch(
            "backend.pipeline.transcription.transcribers.SpeechClient"
        ) as mock_speech_client_cls:
            mock_client_instance = MagicMock()
            mock_speech_client_cls.return_value = mock_client_instance

            # First call raises a transient error, second call succeeds
            mock_response = MagicMock()
            mock_result = MagicMock()
            mock_result.alternatives = [MagicMock(transcript="Success after retry")]
            mock_response.results = [mock_result]

            mock_client_instance.recognize.side_effect = [
                GoogleAPIError("Transient 503 Service Unavailable"),
                mock_response,
            ]

            transcriber = get_transcriber(
                TranscriberType.GOOGLE_CHIRP_V3, "test-project", "{}"
            )
            transcriber.setup()

            dummy_audio = b"\x00" * int(BYTES_PER_SECOND_16KHZ_MONO * 2.5)

            # Patch time.sleep to avoid actually waiting during the test
            with patch("time.sleep"):
                transcript = transcriber.transcribe(
                    audio_data=dummy_audio,
                )

            self.assertEqual(transcript, "Success after retry")
            self.assertEqual(mock_client_instance.recognize.call_count, 2)


if __name__ == "__main__":
    unittest.main()
