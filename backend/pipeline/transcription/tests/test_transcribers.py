"""Unit tests for the audio transcription plugins."""

import json
import pathlib
import tempfile
import unittest
from unittest.mock import MagicMock, patch

from google.api_core.exceptions import GoogleAPIError

from backend.pipeline.common.constants import BYTES_PER_SECOND_16KHZ_MONO
from backend.pipeline.transcription.enums import TranscriberType
from backend.pipeline.transcription.transcribers import get_transcriber


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

            # Initialize Transcriber
            transcriber = get_transcriber(
                TranscriberType.GOOGLE_CHIRP_V3,
                "test-project",
                '{"location": "us", "keywords_file_path": ""}',
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
        """Verifies that the system safely filters and intercepts implicit [BACKGROUND] generic filler outputs, converting them cleanly into None."""
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
                TranscriberType.GOOGLE_CHIRP_V3,
                "test-project",
                '{"keywords_file_path": ""}',
            )
            transcriber.setup()

            dummy_audio = b"\x00" * int(BYTES_PER_SECOND_16KHZ_MONO * 2.5)

            transcript = transcriber.transcribe(
                audio_data=dummy_audio,
            )

            self.assertIsNone(transcript)

    def test_google_chirp_transcriber_retry_on_google_api_error(self) -> None:
        """Verifies that transient external dependencies generating 503 GoogleAPIErrors trigger a retry mechanism that subsequently fulfills the initial recognize request."""
        with patch(
            "backend.pipeline.transcription.transcribers.SpeechClient"
        ) as mock_speech_client_cls:
            mock_client_instance = MagicMock()
            mock_speech_client_cls.return_value = mock_client_instance

            # First call raises a transient error, second call succeeds
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

            transcriber = get_transcriber(
                TranscriberType.GOOGLE_CHIRP_V3,
                "test-project",
                '{"keywords_file_path": ""}',
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

    def test_google_chirp_transcriber_keywords_json(self) -> None:
        """Verifies that keywords are loaded from a JSON file and passed to the RecognizeRequest."""
        with patch(
            "backend.pipeline.transcription.transcribers.SpeechClient"
        ) as mock_speech_client_cls:
            mock_client_instance = MagicMock()
            mock_speech_client_cls.return_value = mock_client_instance

            # Mock successful response
            mock_response = MagicMock()
            mock_result = MagicMock()
            mock_result.alternatives = [MagicMock(transcript="Hello world")]
            mock_response.results = [mock_result]
            mock_client_instance.recognize.return_value = mock_response

            # Create a dummy JSON file
            with tempfile.NamedTemporaryFile(mode="w", delete=False) as f:
                f.write('[{"phrase": "testphrase"}]')
                temp_path = f.name

            try:
                transcriber = get_transcriber(
                    TranscriberType.GOOGLE_CHIRP_V3,
                    "test-project",
                    f'{{"keywords_file_path": "{temp_path}", "boost": 12.0}}',
                )
                transcriber.setup()

                dummy_audio = b"\x00" * int(BYTES_PER_SECOND_16KHZ_MONO * 1.0)
                transcriber.transcribe(audio_data=dummy_audio)

                # Verify recognize was called with adaptation
                mock_client_instance.recognize.assert_called_once()
                args, kwargs = mock_client_instance.recognize.call_args
                request = kwargs.get("request") or args[0]

                self.assertIsNotNone(request.config.adaptation)
                self.assertEqual(
                    request.config.adaptation.phrase_sets[0]
                    .inline_phrase_set.phrases[0]
                    .value,
                    "testphrase",
                )
                self.assertEqual(
                    request.config.adaptation.phrase_sets[0]
                    .inline_phrase_set.phrases[0]
                    .boost,
                    12.0,
                )

            finally:
                pathlib.Path(temp_path).unlink()

    def test_chirp_keywords_json_valid(self) -> None:
        """Verifies that the default chirp_keywords.json file is valid JSON and non-empty."""
        p = pathlib.Path(__file__).parent.parent / "chirp_keywords.json"

        self.assertTrue(p.exists(), f"Keywords file {p} does not exist")
        with p.open("r") as f:
            data = json.load(f)
            self.assertIsInstance(data, list)
            self.assertTrue(len(data) > 0)
            for item in data:
                if isinstance(item, dict):
                    self.assertIn("phrase", item)
                else:
                    self.assertIsInstance(item, str)

    def test_google_chirp_transcriber_setup_missing_file_fail(self) -> None:
        """Verifies that setup() fails fast if keywords_file_path is specified but missing."""
        with patch(
            "backend.pipeline.transcription.transcribers.SpeechClient"
        ) as mock_speech_client_cls:
            mock_client_instance = MagicMock()
            mock_speech_client_cls.return_value = mock_client_instance

            transcriber = get_transcriber(
                TranscriberType.GOOGLE_CHIRP_V3,
                "test-project",
                '{"keywords_file_path": "/path/to/nonexistent/file.json"}',
            )
            with self.assertRaises(FileNotFoundError):
                transcriber.setup()

    def test_google_chirp_transcriber_setup_corrupt_file_fail(self) -> None:
        """Verifies that setup() fails fast if keywords_file_path is specified but un-parsable."""
        with patch(
            "backend.pipeline.transcription.transcribers.SpeechClient"
        ) as mock_speech_client_cls:
            mock_client_instance = MagicMock()
            mock_speech_client_cls.return_value = mock_client_instance

            with tempfile.NamedTemporaryFile(mode="w", delete=False) as f:
                f.write('{"corrupt": "invalid_json"')
                temp_path = f.name

            try:
                transcriber = get_transcriber(
                    TranscriberType.GOOGLE_CHIRP_V3,
                    "test-project",
                    f'{{"keywords_file_path": "{temp_path}"}}',
                )
                with self.assertRaises(ValueError):
                    transcriber.setup()
            finally:
                pathlib.Path(temp_path).unlink()

    def test_google_chirp_transcriber_transcribe_before_setup_fail(
        self,
    ) -> None:
        """Verifies that transcribe() throws RuntimeError if called before setup()."""
        transcriber = get_transcriber(
            TranscriberType.GOOGLE_CHIRP_V3,
            "test-project",
            '{"keywords_file_path": ""}',
        )
        with self.assertRaises(RuntimeError):
            transcriber.transcribe(audio_data=b"\x00")


if __name__ == "__main__":
    unittest.main()
