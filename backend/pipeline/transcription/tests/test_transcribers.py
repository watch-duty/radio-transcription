"""Unit tests for the audio transcription plugins."""

import tempfile
import unittest
from unittest.mock import MagicMock, call, patch

from google.api_core.retry import Retry
from google.api_core.exceptions import GoogleAPIError
import json
import pathlib

from backend.pipeline.common.constants import BYTES_PER_SECOND_16KHZ_MONO
from backend.pipeline.transcription.constants import CHIRP_UNINTELLIGIBLE_MARKER
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
                '{"location": "us", "phrase_hints_file_path": null, "custom_prompt_file_path": null}',
            )
            transcriber.setup()

            dummy_audio = b"\x00" * int(BYTES_PER_SECOND_16KHZ_MONO * 2.5)

            transcript = transcriber.transcribe(
                audio_data=dummy_audio,
            )

            self.assertEqual(transcript, "Hello world from Chirp")
            mock_client_instance.recognize.assert_called_once()

    def test_google_chirp_transcriber_background(self) -> None:
        """Verifies that the system safely filters and intercepts implicit [UNINTELLIGIBLE] generic filler outputs, converting them cleanly into None."""
        with patch(
            "backend.pipeline.transcription.transcribers.SpeechClient"
        ) as mock_speech_client_cls:
            mock_client_instance = MagicMock()
            mock_speech_client_cls.return_value = mock_client_instance

            mock_response = MagicMock()
            mock_result = MagicMock()
            mock_result.alternatives = [
                MagicMock(transcript=CHIRP_UNINTELLIGIBLE_MARKER)
            ]
            mock_response.results = [mock_result]
            mock_client_instance.recognize.return_value = mock_response

            transcriber = GoogleChirpV3Transcriber(
                "test-project",
                ChirpConfig(
                    phrase_hints_file_path=None, custom_prompt_file_path=None
                ),
            )
            transcriber.setup()

            dummy_audio = b"\x00" * int(BYTES_PER_SECOND_16KHZ_MONO * 2.5)

            transcript = transcriber.transcribe(audio_data=dummy_audio)

            self.assertIsNone(transcript)

    def test_google_chirp_transcriber_passes_retry_policy(self) -> None:
        """Verifies that the GoogleChirpV3Transcriber passes a native Retry policy to the SpeechClient."""
        with patch(
            "backend.pipeline.transcription.transcribers.SpeechClient"
        ) as mock_speech_client_cls:
            mock_client_instance = MagicMock()
            mock_speech_client_cls.return_value = mock_client_instance

            mock_response = MagicMock()
            mock_result = MagicMock()
            mock_result.alternatives = [MagicMock(transcript="Success")]
            mock_response.results = [mock_result]
            mock_client_instance.recognize.return_value = mock_response

            transcriber = GoogleChirpV3Transcriber(
                "test-project",
                ChirpConfig(
                    phrase_hints_file_path=None, custom_prompt_file_path=None
                ),
            )
            transcriber.setup()

            dummy_audio = b"\x00" * int(BYTES_PER_SECOND_16KHZ_MONO * 2.5)
            transcriber.transcribe(audio_data=dummy_audio)

            mock_client_instance.recognize.assert_called_once()
            _, kwargs = mock_client_instance.recognize.call_args
            self.assertIn("retry", kwargs)
            self.assertIsInstance(kwargs["retry"], Retry)

    def test_google_chirp_transcriber_no_phrase_hints_omits_adaptation(
        self,
    ) -> None:
        """Verifies that adaptation=None is passed to RecognitionConfig when no phrase hints file is configured."""
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
                "test-project",
                ChirpConfig(
                    phrase_hints_file_path=None, custom_prompt_file_path=None
                ),
            )
            transcriber.setup()

            dummy_audio = b"\x00" * int(BYTES_PER_SECOND_16KHZ_MONO * 2.5)
            transcriber.transcribe(audio_data=dummy_audio)

            _, kwargs = mock_cs.RecognitionConfig.call_args
            self.assertIsNone(kwargs.get("adaptation"))

    def test_google_chirp_transcriber_phrase_hints_file_loads_and_builds_adaptation(
        self,
    ) -> None:
        """Verifies that phrase hints are loaded from a plain text file and used to build SpeechAdaptation."""
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".txt", delete=False
        ) as f:
            f.write("# comment line\n")
            f.write("Code 3\n")
            f.write("\n")  # blank line — should be skipped
            f.write("10-4\n")
            phrase_hints_path = f.name

        config = ChirpConfig(
            phrase_hints_file_path=phrase_hints_path,
            custom_prompt_file_path=None,
        )

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

            # Comment and blank line are skipped; two phrases expected
            expected_phrase_calls = [
                call(value="Code 3"),
                call(value="10-4"),
            ]

            mock_cs.PhraseSet.Phrase.assert_has_calls(
                expected_phrase_calls, any_order=False
            )

            mock_cs.SpeechAdaptation.assert_called_once()

    def test_google_chirp_transcriber_phrase_hints_file_missing_raises(
        self,
    ) -> None:
        """Verifies that setup() raises FileNotFoundError when phrase_hints_file_path points to a non-existent file."""
        config = ChirpConfig(
            phrase_hints_file_path="/nonexistent/path/phrase_hints.txt",
            custom_prompt_file_path=None,
        )

        with patch("backend.pipeline.transcription.transcribers.SpeechClient"):
            transcriber = GoogleChirpV3Transcriber("test-project", config)
            with self.assertRaises(FileNotFoundError):
                transcriber.setup()

    def test_google_chirp_transcriber_denoiser_config(self) -> None:
        """Verifies that denoiser_config is passed to RecognitionConfig."""
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
            mock_result.alternatives = [MagicMock(transcript="Success")]
            mock_response.results = [mock_result]
            mock_client_instance.recognize.return_value = mock_response

            config = ChirpConfig(
                phrase_hints_file_path=None,
                custom_prompt_file_path=None,
                enable_denoiser=True,
            )
            transcriber = GoogleChirpV3Transcriber("test-project", config)
            transcriber.setup()

            dummy_audio = b"\x00" * int(BYTES_PER_SECOND_16KHZ_MONO * 2.5)
            transcriber.transcribe(audio_data=dummy_audio)

            _, kwargs = mock_cs.RecognitionConfig.call_args
            self.assertIn("denoiser_config", kwargs)
            mock_cs.DenoiserConfig.assert_called_once_with(denoise_audio=True)

    def test_google_chirp_transcriber_custom_prompt(self) -> None:
        """Verifies that custom_prompt is passed to RecognitionFeatures."""
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
            mock_result.alternatives = [MagicMock(transcript="Success")]
            mock_response.results = [mock_result]
            mock_client_instance.recognize.return_value = mock_response

            with tempfile.NamedTemporaryFile(
                mode="w", suffix=".txt", delete=False
            ) as f:
                f.write("Test prompt")
                prompt_path = f.name

            config = ChirpConfig(
                phrase_hints_file_path=None, custom_prompt_file_path=prompt_path
            )
            transcriber = GoogleChirpV3Transcriber("test-project", config)
            transcriber.setup()

            dummy_audio = b"\x00" * int(BYTES_PER_SECOND_16KHZ_MONO * 2.5)
            transcriber.transcribe(audio_data=dummy_audio)

            mock_cs.RecognitionFeatures.assert_called_once()
            _, features_kwargs = mock_cs.RecognitionFeatures.call_args
            self.assertIn("custom_prompt_config", features_kwargs)
            mock_cs.CustomPromptConfig.assert_called_once_with(
                custom_prompt="Test prompt"
            )

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

            transcriber = GoogleChirpV3Transcriber(
                "test-project", ChirpConfig(keywords_file_path="/path/to/nonexistent/file.json")
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
                transcriber = GoogleChirpV3Transcriber(
                    "test-project", ChirpConfig(keywords_file_path=temp_path)
                )
                with self.assertRaises(ValueError):
                    transcriber.setup()
            finally:
                pathlib.Path(temp_path).unlink()

    def test_google_chirp_transcriber_transcribe_before_setup_fail(self) -> None:
        """Verifies that transcribe() throws RuntimeError if called before setup()."""
        transcriber = GoogleChirpV3Transcriber(
            "test-project", ChirpConfig(keywords_file_path=None)
        )
        with self.assertRaises(RuntimeError):
            transcriber.transcribe(audio_data=b"\x00")
if __name__ == "__main__":
    unittest.main()
