"""Unit tests for the audio transcription plugins."""

import json
import tempfile
import unittest
from unittest.mock import MagicMock, call, patch

from google.api_core.exceptions import GoogleAPIError

from backend.pipeline.common.constants import BYTES_PER_SECOND_16KHZ_MONO
from backend.pipeline.transcription.enums import TranscriberType
from backend.pipeline.transcription.transcribers import (
    ChirpConfig,
    GoogleChirpV3Transcriber,
    get_transcriber,
)


class TestTranscribers(unittest.TestCase):
    def test_google_chirp_transcriber_success(self) -> None:
        """Verifies that the GoogleChirpTranscriber interacts via the SpeechClient accurately rendering raw byte audio variants into basic text transcripts."""
        with patch(
            "backend.pipeline.transcription.transcribers.SpeechClient"
        ) as mock_speech_client_cls:
            mock_client_instance = MagicMock()
            mock_speech_client_cls.return_value = mock_client_instance

            # Mock successful response
            mock_response = MagicMock()
            mock_result = MagicMock()
            mock_result.alternatives = [
                MagicMock(transcript="Hello world from Chirp")
            ]
            mock_response.results = [mock_result]
            mock_client_instance.recognize.return_value = mock_response

            transcriber = get_transcriber(
                TranscriberType.GOOGLE_CHIRP_V3,
                "test-project",
                '{"location": "us", "keywords_file_path": null}',
            )
            transcriber.setup()

            dummy_audio = b"\x00" * int(BYTES_PER_SECOND_16KHZ_MONO * 2.5)

            transcript = transcriber.transcribe(
                audio_data=dummy_audio,
            )

            self.assertEqual(transcript, "Hello world from Chirp")
            mock_client_instance.recognize.assert_called_once()

    def test_google_chirp_transcriber_background(self) -> None:
        """Verifies that the system safely filters and intercepts implicit [BACKGROUND] generic filler outputs, converting them cleanly into None."""
        with patch(
            "backend.pipeline.transcription.transcribers.SpeechClient"
        ) as mock_speech_client_cls:
            mock_client_instance = MagicMock()
            mock_speech_client_cls.return_value = mock_client_instance

            mock_response = MagicMock()
            mock_result = MagicMock()
            mock_result.alternatives = [MagicMock(transcript="[BACKGROUND]")]
            mock_response.results = [mock_result]
            mock_client_instance.recognize.return_value = mock_response

            transcriber = GoogleChirpV3Transcriber(
                "test-project", ChirpConfig(keywords_file_path=None)
            )
            transcriber.setup()

            dummy_audio = b"\x00" * int(BYTES_PER_SECOND_16KHZ_MONO * 2.5)

            transcript = transcriber.transcribe(audio_data=dummy_audio)

            self.assertIsNone(transcript)

    def test_google_chirp_transcriber_retry_on_google_api_error(self) -> None:
        """Verifies that transient external dependencies generating 503 GoogleAPIErrors trigger a retry mechanism that subsequently fulfills the initial recognize request."""
        with patch(
            "backend.pipeline.transcription.transcribers.SpeechClient"
        ) as mock_speech_client_cls:
            mock_client_instance = MagicMock()
            mock_speech_client_cls.return_value = mock_client_instance

            mock_response = MagicMock()
            mock_result = MagicMock()
            mock_result.alternatives = [
                MagicMock(transcript="Success after retry")
            ]
            mock_response.results = [mock_result]

            mock_client_instance.recognize.side_effect = [
                GoogleAPIError("Transient 503 Service Unavailable"),
                mock_response,
            ]

            transcriber = GoogleChirpV3Transcriber(
                "test-project", ChirpConfig(keywords_file_path=None)
            )
            transcriber.setup()

            dummy_audio = b"\x00" * int(BYTES_PER_SECOND_16KHZ_MONO * 2.5)

            with patch("time.sleep"):
                transcript = transcriber.transcribe(audio_data=dummy_audio)

            self.assertEqual(transcript, "Success after retry")
            self.assertEqual(mock_client_instance.recognize.call_count, 2)

    def test_google_chirp_transcriber_no_keywords_omits_adaptation(
        self,
    ) -> None:
        """Verifies that adaptation=None is passed to RecognitionConfig when no keywords file is configured."""
        with (
            patch(
                "backend.pipeline.transcription.transcribers.SpeechClient"
            ) as mock_speech_client_cls,
            patch(
                "backend.pipeline.transcription.transcribers.cloud_speech"
            ) as mock_cs,
        ):
            mock_client_instance = MagicMock()
            mock_speech_client_cls.return_value = mock_client_instance

            mock_response = MagicMock()
            mock_result = MagicMock()
            mock_result.alternatives = [
                MagicMock(transcript="All units respond")
            ]
            mock_response.results = [mock_result]
            mock_client_instance.recognize.return_value = mock_response

            transcriber = GoogleChirpV3Transcriber(
                "test-project", ChirpConfig(keywords_file_path=None)
            )
            transcriber.setup()

            dummy_audio = b"\x00" * int(BYTES_PER_SECOND_16KHZ_MONO * 2.5)
            transcriber.transcribe(audio_data=dummy_audio)

            _, kwargs = mock_cs.RecognitionConfig.call_args
            self.assertIsNone(kwargs.get("adaptation"))

    def test_google_chirp_transcriber_keywords_file_loads_and_builds_adaptation(
        self,
    ) -> None:
        """Verifies that keywords are loaded from a JSON file and used to build SpeechAdaptation, with per-phrase boost respected and the default applied when absent."""
        keywords = [
            {"phrase": "Code 3", "boost": 20.0},
            {"phrase": "10-4"},  # no boost — uses KeywordItem default
        ]

        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".json", delete=False
        ) as f:
            json.dump(keywords, f)
            keywords_path = f.name

        config = ChirpConfig(keywords_file_path=keywords_path)

        with (
            patch(
                "backend.pipeline.transcription.transcribers.SpeechClient"
            ) as mock_speech_client_cls,
            patch(
                "backend.pipeline.transcription.transcribers.cloud_speech"
            ) as mock_cs,
        ):
            mock_client_instance = MagicMock()
            mock_speech_client_cls.return_value = mock_client_instance

            mock_response = MagicMock()
            mock_result = MagicMock()
            mock_result.alternatives = [MagicMock(transcript="Code 3")]
            mock_response.results = [mock_result]
            mock_client_instance.recognize.return_value = mock_response

            transcriber = GoogleChirpV3Transcriber("test-project", config)
            transcriber.setup()

            dummy_audio = b"\x00" * int(BYTES_PER_SECOND_16KHZ_MONO * 2.5)
            transcriber.transcribe(audio_data=dummy_audio)

            # Explicit boost for "Code 3"; KeywordItem default (10.0) for "10-4"
            expected_phrase_calls = [
                call(value="Code 3", boost=20.0),
                call(value="10-4", boost=10.0),
            ]
            mock_cs.PhraseSet.Phrase.assert_has_calls(
                expected_phrase_calls, any_order=False
            )
            mock_cs.SpeechAdaptation.assert_called_once()

    def test_google_chirp_transcriber_keywords_file_missing_raises(
        self,
    ) -> None:
        """Verifies that setup() raises FileNotFoundError when keywords_file_path points to a non-existent file."""
        config = ChirpConfig(
            keywords_file_path="/nonexistent/path/keywords.json"
        )

        with patch("backend.pipeline.transcription.transcribers.SpeechClient"):
            transcriber = GoogleChirpV3Transcriber("test-project", config)
            with self.assertRaises(FileNotFoundError):
                transcriber.setup()


if __name__ == "__main__":
    unittest.main()
