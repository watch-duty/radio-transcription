"""Unit tests for the audio transcription plugins."""

import unittest
from unittest.mock import MagicMock, call, patch

from google.api_core.retry import Retry

from backend.pipeline.normalization.common.enums import TranscriberType
from backend.pipeline.transcription.transcribers.chirp import (
    CHIRP_UNINTELLIGIBLE_MARKER,
    ChirpConfig,
    GoogleChirpV3Transcriber,
)
from backend.pipeline.transcription.transcribers.factory import get_transcriber

BYTES_PER_SECOND_16KHZ_MONO = 16000 * 2


class TestTranscribers(unittest.TestCase):
    def test_google_chirp_transcriber_success(self) -> None:
        """Verifies that the GoogleChirpTranscriber interacts via the SpeechClient accurately rendering raw byte audio variants into basic text transcripts."""
        with patch(
            "backend.pipeline.transcription.transcribers.chirp.SpeechClient"
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
                '{"location": "us", "phrase_hints": [], "custom_prompt": null}',
            )
            transcriber.setup()

            dummy_audio = b"\x00" * int(BYTES_PER_SECOND_16KHZ_MONO * 2.5)

            transcript = transcriber.transcribe(
                audio_data=dummy_audio,
                duration_ms=2500,
            )

            self.assertEqual(transcript, "Hello world from Chirp")
            mock_client_instance.recognize.assert_called_once()

    def test_google_chirp_transcriber_background(self) -> None:
        """Verifies that the system safely filters and intercepts implicit [UNINTELLIGIBLE] generic filler outputs, converting them cleanly into None."""
        with patch(
            "backend.pipeline.transcription.transcribers.chirp.SpeechClient"
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
                ChirpConfig(phrase_hints=[], custom_prompt=None),
            )
            transcriber.setup()

            dummy_audio = b"\x00" * int(BYTES_PER_SECOND_16KHZ_MONO * 2.5)

            transcript = transcriber.transcribe(
                audio_data=dummy_audio, duration_ms=2500
            )

            self.assertIsNone(transcript)

    def test_google_chirp_transcriber_passes_retry_policy(self) -> None:
        """Verifies that the GoogleChirpV3Transcriber passes a native Retry policy to the SpeechClient."""
        with patch(
            "backend.pipeline.transcription.transcribers.chirp.SpeechClient"
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
                ChirpConfig(phrase_hints=[], custom_prompt=None),
            )
            transcriber.setup()

            dummy_audio = b"\x00" * int(BYTES_PER_SECOND_16KHZ_MONO * 2.5)
            transcriber.transcribe(audio_data=dummy_audio, duration_ms=2500)

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
                "backend.pipeline.transcription.transcribers.chirp.SpeechClient"
            ) as mock_speech_client_cls,
            patch(
                "backend.pipeline.transcription.transcribers.chirp.cloud_speech"
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
                ChirpConfig(phrase_hints=[], custom_prompt=None),
            )
            transcriber.setup()

            dummy_audio = b"\x00" * int(BYTES_PER_SECOND_16KHZ_MONO * 2.5)
            transcriber.transcribe(audio_data=dummy_audio, duration_ms=2500)

            _, kwargs = mock_cs.RecognitionConfig.call_args
            self.assertIsNone(kwargs.get("adaptation"))

    def test_google_chirp_transcriber_phrase_hints_adaptation(self) -> None:
        """Verifies that configured phrase hints are used directly to build SpeechAdaptation."""
        config = ChirpConfig(
            phrase_hints=["Code 3", "10-4"],
            custom_prompt=None,
        )

        with (
            patch(
                "backend.pipeline.transcription.transcribers.chirp.SpeechClient"
            ) as mock_speech_client_cls,
            patch(
                "backend.pipeline.transcription.transcribers.chirp.cloud_speech"
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
            transcriber.transcribe(audio_data=dummy_audio, duration_ms=2500)

            expected_phrase_calls = [
                call(value="Code 3"),
                call(value="10-4"),
            ]

            mock_cs.PhraseSet.Phrase.assert_has_calls(
                expected_phrase_calls, any_order=False
            )
            mock_cs.SpeechAdaptation.assert_called_once()

    def test_google_chirp_transcriber_denoiser_config(self) -> None:
        """Verifies that denoiser_config is passed to RecognitionConfig."""
        with (
            patch(
                "backend.pipeline.transcription.transcribers.chirp.SpeechClient"
            ) as mock_speech_client_cls,
            patch(
                "backend.pipeline.transcription.transcribers.chirp.cloud_speech"
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
                phrase_hints=[],
                custom_prompt=None,
                enable_denoiser=True,
            )
            transcriber = GoogleChirpV3Transcriber("test-project", config)
            transcriber.setup()

            dummy_audio = b"\x00" * int(BYTES_PER_SECOND_16KHZ_MONO * 2.5)
            transcriber.transcribe(audio_data=dummy_audio, duration_ms=2500)

            _, kwargs = mock_cs.RecognitionConfig.call_args
            self.assertIn("denoiser_config", kwargs)
            mock_cs.DenoiserConfig.assert_called_once_with(denoise_audio=True)

    def test_google_chirp_transcriber_custom_prompt(self) -> None:
        """Verifies that custom_prompt is passed to RecognitionFeatures."""
        with (
            patch(
                "backend.pipeline.transcription.transcribers.chirp.SpeechClient"
            ) as mock_speech_client_cls,
            patch(
                "backend.pipeline.transcription.transcribers.chirp.cloud_speech"
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
                phrase_hints=[],
                custom_prompt="Test prompt",
            )
            transcriber = GoogleChirpV3Transcriber("test-project", config)
            transcriber.setup()

            dummy_audio = b"\x00" * int(BYTES_PER_SECOND_16KHZ_MONO * 2.5)
            transcriber.transcribe(audio_data=dummy_audio, duration_ms=2500)

            mock_cs.RecognitionFeatures.assert_called_once()
            _, features_kwargs = mock_cs.RecognitionFeatures.call_args
            self.assertIn("custom_prompt_config", features_kwargs)
            mock_cs.CustomPromptConfig.assert_called_once_with(
                custom_prompt="Test prompt"
            )


class TestMockTranscriber(unittest.TestCase):
    def test_mock_transcriber_default(self) -> None:
        """Verifies the MockTranscriber returns the default static transcript when no sequence is set."""
        transcriber = get_transcriber(
            TranscriberType.MOCK,
            "test-project",
            "{}",
        )
        transcriber.setup()

        res = transcriber.transcribe(audio_data=b"\x00", duration_ms=1000)
        self.assertEqual(
            res, "This is a mock transcription of the radio transmission."
        )

    def test_mock_transcriber_sequence(self) -> None:
        """Verifies the MockTranscriber rotates through configured transcripts."""
        config_json = '{"transcripts": ["First Call", "Second Call"]}'
        transcriber = get_transcriber(
            TranscriberType.MOCK,
            "test-project",
            config_json,
        )
        transcriber.setup()

        res1 = transcriber.transcribe(audio_data=b"\x00", duration_ms=1000)
        res2 = transcriber.transcribe(audio_data=b"\x00", duration_ms=1000)
        res3 = transcriber.transcribe(audio_data=b"\x00", duration_ms=1000)

        self.assertEqual(res1, "First Call")
        self.assertEqual(res2, "Second Call")
        self.assertEqual(res3, "First Call")


if __name__ == "__main__":
    unittest.main()
