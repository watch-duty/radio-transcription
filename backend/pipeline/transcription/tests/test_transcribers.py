"""Unit tests for the audio transcription plugins."""

import unittest
from unittest.mock import AsyncMock, MagicMock, call, patch

import httpx
import requests
from google.api_core.retry_async import AsyncRetry
from google.genai import types

from backend.pipeline.common.exceptions import (
    PartialTranscriptionError,
)
from backend.pipeline.transcription.enums import TranscriberType
from backend.pipeline.transcription.transcribers.chirp import (
    CHIRP_UNINTELLIGIBLE_MARKER,
    ChirpConfig,
    GoogleChirpV3Transcriber,
)
from backend.pipeline.transcription.transcribers.factory import get_transcriber
from backend.pipeline.transcription.transcribers.gemini import (
    DEFAULT_GEMINI_MODEL,
    GeminiConfig,
    GeminiOtherFinishReasonError,
    GeminiTranscriber,
    GeminiTranscriptionError,
)

BYTES_PER_SECOND_16KHZ_MONO = 16000 * 2


class TestTranscribers(unittest.IsolatedAsyncioTestCase):
    async def test_google_chirp_transcriber_success(self) -> None:
        """Verifies that the GoogleChirpTranscriber interacts via the SpeechClient accurately rendering raw byte audio variants into basic text transcripts."""
        with patch(
            "backend.pipeline.transcription.transcribers.chirp.SpeechAsyncClient"
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
            mock_client_instance.recognize = AsyncMock(
                return_value=mock_response
            )

            transcriber = get_transcriber(
                TranscriberType.GOOGLE_CHIRP_V3,
                "test-project",
                '{"location": "us", "phrase_hints": [], "custom_prompt": null}',
            )
            transcriber.setup()

            dummy_audio = b"\x00" * int(BYTES_PER_SECOND_16KHZ_MONO * 2.5)

            transcript = await transcriber.transcribe(
                audio_data=dummy_audio,
                duration_ms=2500,
            )

            self.assertEqual(transcript, "Hello world from Chirp")
            mock_client_instance.recognize.assert_called_once()

    async def test_google_chirp_transcriber_background(self) -> None:
        """Verifies that the system safely propagates [UNINTELLIGIBLE] generic filler outputs downstream."""
        with patch(
            "backend.pipeline.transcription.transcribers.chirp.SpeechAsyncClient"
        ) as mock_speech_client_cls:
            mock_client_instance = MagicMock()
            mock_speech_client_cls.return_value = mock_client_instance

            mock_response = MagicMock()
            mock_result = MagicMock()
            mock_result.alternatives = [
                MagicMock(transcript=CHIRP_UNINTELLIGIBLE_MARKER)
            ]
            mock_response.results = [mock_result]
            mock_client_instance.recognize = AsyncMock(
                return_value=mock_response
            )

            transcriber = GoogleChirpV3Transcriber(
                "test-project",
                ChirpConfig(phrase_hints=[], custom_prompt=None),
            )
            transcriber.setup()

            dummy_audio = b"\x00" * int(BYTES_PER_SECOND_16KHZ_MONO * 2.5)

            transcript = await transcriber.transcribe(
                audio_data=dummy_audio, duration_ms=2500
            )

            self.assertEqual(transcript, CHIRP_UNINTELLIGIBLE_MARKER)

    async def test_google_chirp_transcriber_passes_retry_policy(self) -> None:
        """Verifies that the GoogleChirpV3Transcriber passes a native Retry policy to the SpeechClient."""
        with patch(
            "backend.pipeline.transcription.transcribers.chirp.SpeechAsyncClient"
        ) as mock_speech_client_cls:
            mock_client_instance = MagicMock()
            mock_speech_client_cls.return_value = mock_client_instance

            mock_response = MagicMock()
            mock_result = MagicMock()
            mock_result.alternatives = [MagicMock(transcript="Success")]
            mock_response.results = [mock_result]
            mock_client_instance.recognize = AsyncMock(
                return_value=mock_response
            )

            transcriber = GoogleChirpV3Transcriber(
                "test-project",
                ChirpConfig(phrase_hints=[], custom_prompt=None),
            )
            transcriber.setup()

            dummy_audio = b"\x00" * int(BYTES_PER_SECOND_16KHZ_MONO * 2.5)
            await transcriber.transcribe(
                audio_data=dummy_audio, duration_ms=2500
            )

            mock_client_instance.recognize.assert_called_once()
            _, kwargs = mock_client_instance.recognize.call_args
            self.assertIn("retry", kwargs)
            self.assertIsInstance(kwargs["retry"], AsyncRetry)

    async def test_google_chirp_transcriber_custom_retry_policy(self) -> None:
        """Verifies that custom retry settings in ChirpConfig are propagated to AsyncRetry."""
        with patch(
            "backend.pipeline.transcription.transcribers.chirp.SpeechAsyncClient"
        ) as mock_speech_client_cls:
            mock_client_instance = MagicMock()
            mock_speech_client_cls.return_value = mock_client_instance

            mock_response = MagicMock()
            mock_result = MagicMock()
            mock_result.alternatives = [MagicMock(transcript="Success")]
            mock_response.results = [mock_result]
            mock_client_instance.recognize = AsyncMock(
                return_value=mock_response
            )

            config = ChirpConfig(
                phrase_hints=[],
                custom_prompt=None,
                retry_initial_delay=1.5,
                retry_max_delay=45.0,
                retry_multiplier=1.8,
                retry_deadline=150.0,
            )
            transcriber = GoogleChirpV3Transcriber("test-project", config)
            transcriber.setup()

            dummy_audio = b"\x00" * int(BYTES_PER_SECOND_16KHZ_MONO * 2.5)
            await transcriber.transcribe(
                audio_data=dummy_audio, duration_ms=2500
            )

            mock_client_instance.recognize.assert_called_once()
            _, kwargs = mock_client_instance.recognize.call_args
            self.assertIn("retry", kwargs)
            retry_policy = kwargs["retry"]
            self.assertIsInstance(retry_policy, AsyncRetry)
            self.assertEqual(retry_policy._initial, 1.5)
            self.assertEqual(retry_policy._maximum, 45.0)
            self.assertEqual(retry_policy._multiplier, 1.8)
            self.assertEqual(retry_policy._timeout, 150.0)

    async def test_google_chirp_transcriber_no_phrase_hints_omits_adaptation(
        self,
    ) -> None:
        """Verifies that adaptation=None is passed to RecognitionConfig when no phrase hints file is configured."""
        with (
            patch(
                "backend.pipeline.transcription.transcribers.chirp.SpeechAsyncClient"
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
            mock_client_instance.recognize = AsyncMock(
                return_value=mock_response
            )

            transcriber = GoogleChirpV3Transcriber(
                "test-project",
                ChirpConfig(phrase_hints=[], custom_prompt=None),
            )
            transcriber.setup()

            dummy_audio = b"\x00" * int(BYTES_PER_SECOND_16KHZ_MONO * 2.5)
            await transcriber.transcribe(
                audio_data=dummy_audio, duration_ms=2500
            )

            _, kwargs = mock_cs.RecognitionConfig.call_args
            self.assertIsNone(kwargs.get("adaptation"))

    async def test_google_chirp_transcriber_phrase_hints_adaptation(
        self,
    ) -> None:
        """Verifies that configured phrase hints are used directly to build SpeechAdaptation."""
        config = ChirpConfig(
            phrase_hints=["Code 3", "10-4"],
            custom_prompt=None,
        )

        with (
            patch(
                "backend.pipeline.transcription.transcribers.chirp.SpeechAsyncClient"
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
            mock_client_instance.recognize = AsyncMock(
                return_value=mock_response
            )

            transcriber = GoogleChirpV3Transcriber("test-project", config)
            transcriber.setup()

            dummy_audio = b"\x00" * int(BYTES_PER_SECOND_16KHZ_MONO * 2.5)
            await transcriber.transcribe(
                audio_data=dummy_audio, duration_ms=2500
            )

            expected_phrase_calls = [
                call(value="Code 3"),
                call(value="10-4"),
            ]

            mock_cs.PhraseSet.Phrase.assert_has_calls(
                expected_phrase_calls, any_order=False
            )
            mock_cs.SpeechAdaptation.assert_called_once()

    async def test_google_chirp_transcriber_denoiser_config(self) -> None:
        """Verifies that denoiser_config is passed to RecognitionConfig."""
        with (
            patch(
                "backend.pipeline.transcription.transcribers.chirp.SpeechAsyncClient"
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
            mock_client_instance.recognize = AsyncMock(
                return_value=mock_response
            )

            config = ChirpConfig(
                phrase_hints=[],
                custom_prompt=None,
                enable_denoiser=True,
            )
            transcriber = GoogleChirpV3Transcriber("test-project", config)
            transcriber.setup()

            dummy_audio = b"\x00" * int(BYTES_PER_SECOND_16KHZ_MONO * 2.5)
            await transcriber.transcribe(
                audio_data=dummy_audio, duration_ms=2500
            )

            _, kwargs = mock_cs.RecognitionConfig.call_args
            self.assertIn("denoiser_config", kwargs)
            mock_cs.DenoiserConfig.assert_called_once_with(denoise_audio=True)

    async def test_google_chirp_transcriber_custom_prompt(self) -> None:
        """Verifies that custom_prompt is passed to RecognitionFeatures."""
        with (
            patch(
                "backend.pipeline.transcription.transcribers.chirp.SpeechAsyncClient"
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
            mock_client_instance.recognize = AsyncMock(
                return_value=mock_response
            )

            config = ChirpConfig(
                phrase_hints=[],
                custom_prompt="Test prompt",
            )
            transcriber = GoogleChirpV3Transcriber("test-project", config)
            transcriber.setup()

            dummy_audio = b"\x00" * int(BYTES_PER_SECOND_16KHZ_MONO * 2.5)
            await transcriber.transcribe(
                audio_data=dummy_audio, duration_ms=2500
            )

            mock_cs.RecognitionFeatures.assert_called_once()
            _, features_kwargs = mock_cs.RecognitionFeatures.call_args
            self.assertIn("custom_prompt_config", features_kwargs)
            mock_cs.CustomPromptConfig.assert_called_once_with(
                custom_prompt="Test prompt"
            )


class TestMockTranscriber(unittest.IsolatedAsyncioTestCase):
    async def test_mock_transcriber_default(self) -> None:
        """Verifies the MockTranscriber returns the default static transcript when no sequence is set."""
        transcriber = get_transcriber(
            TranscriberType.MOCK,
            "test-project",
            "{}",
        )
        transcriber.setup()

        res = await transcriber.transcribe(audio_data=b"\x00", duration_ms=1000)
        self.assertEqual(
            res, "This is a mock transcription of the radio transmission."
        )

    async def test_mock_transcriber_sequence(self) -> None:
        """Verifies the MockTranscriber rotates through configured transcripts."""
        config_json = '{"transcripts": ["First Call", "Second Call"]}'
        transcriber = get_transcriber(
            TranscriberType.MOCK,
            "test-project",
            config_json,
        )
        transcriber.setup()

        res1 = await transcriber.transcribe(
            audio_data=b"\x00", duration_ms=1000
        )
        res2 = await transcriber.transcribe(
            audio_data=b"\x00", duration_ms=1000
        )
        res3 = await transcriber.transcribe(
            audio_data=b"\x00", duration_ms=1000
        )

        self.assertEqual(res1, "First Call")
        self.assertEqual(res2, "Second Call")
        self.assertEqual(res3, "First Call")


class TestLocalApiTranscriber(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.env_patcher = patch.dict(
            "os.environ",
            {"LOCAL_ASR_API_URL": "http://local-whisper:8095/transcribe"},
        )
        self.env_patcher.start()

        self.get_patcher = patch(
            "backend.pipeline.transcription.transcribers.local_api.requests.get"
        )
        self.mock_get = self.get_patcher.start()
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        self.mock_get.return_value = mock_resp

    def tearDown(self) -> None:
        self.env_patcher.stop()
        self.get_patcher.stop()

    @patch(
        "backend.pipeline.transcription.transcribers.local_api.requests.post"
    )
    async def test_local_api_transcriber_success_uri(
        self, mock_post: MagicMock
    ) -> None:
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"text": "Hello from local API"}
        mock_post.return_value = mock_resp

        transcriber = get_transcriber(
            TranscriberType.LOCAL_WHISPER,
            "test-project",
            "{}",
        )
        transcriber.setup()

        res = await transcriber.transcribe(
            uri="gs://bucket/audio.flac", duration_ms=1000
        )
        self.assertEqual(res, "Hello from local API")
        mock_post.assert_called_once_with(
            "http://local-whisper:8095/transcribe",
            params={"uri": "gs://bucket/audio.flac"},
            timeout=60,
        )

    @patch(
        "backend.pipeline.transcription.transcribers.local_api.requests.post"
    )
    async def test_local_api_transcriber_success_bytes(
        self, mock_post: MagicMock
    ) -> None:
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"text": "   Spoken words   "}
        mock_post.return_value = mock_resp

        transcriber = get_transcriber(
            TranscriberType.LOCAL_WHISPER,
            "test-project",
            "{}",
        )
        transcriber.setup()

        res = await transcriber.transcribe(
            audio_data=b"dummybytes", duration_ms=1000
        )
        self.assertEqual(res, "Spoken words")
        mock_post.assert_called_once_with(
            "http://local-whisper:8095/transcribe",
            files={"file": ("audio.flac", b"dummybytes", "audio/flac")},
            timeout=60,
        )

    @patch(
        "backend.pipeline.transcription.transcribers.local_api.requests.post"
    )
    async def test_local_api_transcriber_empty_text_returns_none(
        self, mock_post: MagicMock
    ) -> None:
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"text": "   "}
        mock_post.return_value = mock_resp

        transcriber = get_transcriber(
            TranscriberType.LOCAL_WHISPER,
            "test-project",
            "{}",
        )
        transcriber.setup()

        res = await transcriber.transcribe(
            audio_data=b"dummybytes", duration_ms=1000
        )
        self.assertIsNone(res)

    @patch(
        "backend.pipeline.transcription.transcribers.local_api.requests.post"
    )
    async def test_local_api_transcriber_http_error_propagates(
        self, mock_post: MagicMock
    ) -> None:
        mock_resp = MagicMock()
        mock_resp.status_code = 500
        mock_resp.text = "Internal Server Error"
        mock_resp.raise_for_status.side_effect = requests.exceptions.HTTPError(
            "500 Server Error", response=mock_resp
        )
        mock_post.return_value = mock_resp

        transcriber = get_transcriber(
            TranscriberType.LOCAL_WHISPER,
            "test-project",
            "{}",
        )
        transcriber.setup()

        with self.assertRaises(requests.exceptions.HTTPError):
            await transcriber.transcribe(
                audio_data=b"dummybytes", duration_ms=1000
            )

    @patch(
        "backend.pipeline.transcription.transcribers.local_api.requests.post"
    )
    async def test_local_api_transcriber_invalid_json_propagates(
        self, mock_post: MagicMock
    ) -> None:
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.side_effect = ValueError("Invalid JSON")
        mock_post.return_value = mock_resp

        transcriber = get_transcriber(
            TranscriberType.LOCAL_WHISPER,
            "test-project",
            "{}",
        )
        transcriber.setup()

        with self.assertRaises(ValueError):
            await transcriber.transcribe(
                audio_data=b"dummybytes", duration_ms=1000
            )

    @patch(
        "backend.pipeline.transcription.transcribers.local_api.requests.post"
    )
    async def test_local_api_transcriber_timeout_propagates(
        self, mock_post: MagicMock
    ) -> None:
        mock_post.side_effect = requests.exceptions.Timeout("Request timed out")

        transcriber = get_transcriber(
            TranscriberType.LOCAL_WHISPER,
            "test-project",
            "{}",
        )
        transcriber.setup()

        with self.assertRaises(requests.exceptions.Timeout):
            await transcriber.transcribe(
                audio_data=b"dummybytes", duration_ms=1000
            )

    @patch.dict("os.environ", {"LOCAL_ASR_API_URL": ""})
    def test_local_api_transcriber_setup_missing_url_raises(self) -> None:
        transcriber = get_transcriber(
            TranscriberType.LOCAL_WHISPER,
            "test-project",
            "{}",
        )
        with self.assertRaises(ValueError) as ctx:
            transcriber.setup()
        self.assertIn(
            "LOCAL_ASR_API_URL environment variable is not set",
            str(ctx.exception),
        )


class TestGeminiTranscriber(unittest.IsolatedAsyncioTestCase):
    async def test_gemini_transcriber_success_bytes(self) -> None:
        """Verifies that the Gemini transcriber transcribes from raw bytes."""
        with patch(
            "backend.pipeline.transcription.transcribers.gemini.genai.Client"
        ) as mock_client_cls:
            mock_client_instance = MagicMock()
            mock_client_cls.return_value = mock_client_instance

            # Mock generate_content response
            mock_response = MagicMock()
            mock_candidate = MagicMock()
            mock_candidate.finish_reason = types.FinishReason.STOP
            mock_candidate.content.parts = [MagicMock(text="Hello from Gemini")]
            mock_response.candidates = [mock_candidate]
            mock_response.text = "Hello from Gemini"

            mock_client_instance.aio.models.generate_content = AsyncMock(
                return_value=mock_response
            )

            transcriber = get_transcriber(
                TranscriberType.GEMINI,
                "test-project",
                '{"location": "us-central1", "prompt": "Test Prompt"}',
            )
            transcriber.setup()

            dummy_audio = b"\x00" * 100

            transcript = await transcriber.transcribe(
                audio_data=dummy_audio,
                duration_ms=2500,
            )

            self.assertEqual(transcript, "Hello from Gemini")
            mock_client_instance.aio.models.generate_content.assert_called_once()
            _, kwargs = (
                mock_client_instance.aio.models.generate_content.call_args
            )
            self.assertEqual(kwargs["model"], "gemini-3.1-flash-lite")

            config = kwargs["config"]
            self.assertEqual(config.system_instruction, "Test Prompt")

            # Verify thinking config is disabled
            self.assertIsNotNone(config.thinking_config)
            self.assertEqual(config.thinking_config.thinking_budget, 0)

            # Verify all 5 safety settings are BLOCK_NONE
            self.assertIsNotNone(config.safety_settings)
            self.assertEqual(len(config.safety_settings), 5)
            for setting in config.safety_settings:
                self.assertEqual(
                    setting.threshold, types.HarmBlockThreshold.BLOCK_NONE
                )

            # Verify the exact categories are present
            categories = {
                setting.category for setting in config.safety_settings
            }
            expected_categories = {
                types.HarmCategory.HARM_CATEGORY_HATE_SPEECH,
                types.HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT,
                types.HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT,
                types.HarmCategory.HARM_CATEGORY_HARASSMENT,
                types.HarmCategory.HARM_CATEGORY_CIVIC_INTEGRITY,
            }
            self.assertEqual(categories, expected_categories)

    async def test_gemini_transcriber_unintelligible(self) -> None:
        """Verifies that [UNINTELLIGIBLE] response maps to "[UNINTELLIGIBLE]"."""
        with patch(
            "backend.pipeline.transcription.transcribers.gemini.genai.Client"
        ) as mock_client_cls:
            mock_client_instance = MagicMock()
            mock_client_cls.return_value = mock_client_instance

            mock_response = MagicMock()
            mock_candidate = MagicMock()
            mock_candidate.finish_reason = types.FinishReason.STOP
            mock_candidate.content.parts = [MagicMock(text="[UNINTELLIGIBLE]")]
            mock_response.candidates = [mock_candidate]
            mock_response.text = "[UNINTELLIGIBLE]"

            mock_client_instance.aio.models.generate_content = AsyncMock(
                return_value=mock_response
            )

            transcriber = get_transcriber(
                TranscriberType.GEMINI,
                "test-project",
                '{"location": "us-central1"}',
            )
            transcriber.setup()

            transcript = await transcriber.transcribe(
                audio_data=b"\x00" * 100,
                duration_ms=1000,
            )

            self.assertEqual(transcript, "[UNINTELLIGIBLE]")

    async def test_gemini_transcriber_recitation_fallback(self) -> None:
        """Verifies that RECITATION finish reason raises GeminiTranscriptionError."""
        with patch(
            "backend.pipeline.transcription.transcribers.gemini.genai.Client"
        ) as mock_client_cls:
            mock_client_instance = MagicMock()
            mock_client_cls.return_value = mock_client_instance

            mock_response = MagicMock()
            mock_candidate = MagicMock()

            # Mock finish_reason as the actual enum
            mock_candidate.finish_reason = types.FinishReason.RECITATION

            mock_candidate.content.parts = []
            mock_response.candidates = [mock_candidate]
            mock_response.response_id = "test-id"
            mock_response.sdk_http_response = None

            mock_client_instance.aio.models.generate_content = AsyncMock(
                return_value=mock_response
            )

            transcriber = get_transcriber(
                TranscriberType.GEMINI,
                "test-project",
                '{"location": "us-central1"}',
            )
            transcriber.setup()

            with self.assertRaises(GeminiTranscriptionError) as context:
                await transcriber.transcribe(
                    audio_data=b"\x00" * 100,
                    duration_ms=1000,
                )
            self.assertIn(
                "Gemini response blocked by safety filters. Finish Reason: RECITATION",
                str(context.exception),
            )

    async def test_gemini_transcriber_empty_response_stop(self) -> None:
        """Verifies that STOP finish reason with no content returns None and logs at INFO level."""
        with patch(
            "backend.pipeline.transcription.transcribers.gemini.genai.Client"
        ) as mock_client_cls:
            mock_client_instance = MagicMock()
            mock_client_cls.return_value = mock_client_instance

            mock_response = MagicMock()
            mock_candidate = MagicMock()
            mock_candidate.finish_reason = types.FinishReason.STOP
            mock_candidate.content = None
            mock_response.candidates = [mock_candidate]

            mock_client_instance.aio.models.generate_content = AsyncMock(
                return_value=mock_response
            )

            transcriber = get_transcriber(
                TranscriberType.GEMINI,
                "test-project",
                '{"location": "us-central1"}',
            )
            transcriber.setup()

            with self.assertLogs(
                "backend.pipeline.transcription.transcribers.gemini",
                level="INFO",
            ) as log_capture:
                transcript = await transcriber.transcribe(
                    audio_data=b"\x00" * 100,
                    duration_ms=1000,
                )

            self.assertEqual(transcript, "")
            # Verify it logged at INFO level
            info_logs = [
                record
                for record in log_capture.records
                if record.levelname == "INFO"
                and "Gemini returned empty content (finish reason: STOP)."
                in record.getMessage()
            ]
            self.assertEqual(len(info_logs), 1)

    async def test_gemini_transcriber_empty_response_other_reason(self) -> None:
        """Verifies that other finish reasons (e.g. MAX_TOKENS) with no content raise the correct exception."""
        with patch(
            "backend.pipeline.transcription.transcribers.gemini.genai.Client"
        ) as mock_client_cls:
            mock_client_instance = MagicMock()
            mock_client_cls.return_value = mock_client_instance

            mock_response = MagicMock()
            mock_candidate = MagicMock()
            mock_candidate.finish_reason = types.FinishReason.MAX_TOKENS
            mock_candidate.content = None
            mock_response.candidates = [mock_candidate]

            mock_client_instance.aio.models.generate_content = AsyncMock(
                return_value=mock_response
            )

            transcriber = get_transcriber(
                TranscriberType.GEMINI,
                "test-project",
                '{"location": "us-central1"}',
            )
            transcriber.setup()

            with self.assertRaises(PartialTranscriptionError) as context:
                await transcriber.transcribe(
                    audio_data=b"\x00" * 100,
                    duration_ms=1000,
                )
            self.assertEqual(context.exception.reason, "MAX_TOKENS")
            self.assertEqual(context.exception.partial_text, "")

    async def test_gemini_transcriber_no_candidates(self) -> None:
        """Verifies that empty candidates response raises GeminiTransientTranscriptionError."""
        with patch(
            "backend.pipeline.transcription.transcribers.gemini.genai.Client"
        ) as mock_client_cls:
            mock_client_instance = MagicMock()
            mock_client_cls.return_value = mock_client_instance

            mock_response = MagicMock()
            mock_response.candidates = []
            mock_response.prompt_feedback = None
            mock_response.response_id = "test-id"
            mock_response.sdk_http_response = None

            mock_client_instance.aio.models.generate_content = AsyncMock(
                return_value=mock_response
            )

            transcriber = get_transcriber(
                TranscriberType.GEMINI,
                "test-project",
                '{"location": "us-central1"}',
            )
            transcriber.setup()

            result = await transcriber.transcribe(
                audio_data=b"\x00" * 100,
                duration_ms=1000,
            )
            self.assertEqual(result, "")

    async def test_gemini_transcriber_prompt_blocked_safety(self) -> None:
        """Verifies that a prompt-level safety block raises a permanent GeminiTranscriptionError."""
        with patch(
            "backend.pipeline.transcription.transcribers.gemini.genai.Client"
        ) as mock_client_cls:
            mock_client_instance = MagicMock()
            mock_client_cls.return_value = mock_client_instance

            mock_response = MagicMock()
            mock_response.candidates = []
            mock_feedback = MagicMock()
            mock_feedback.block_reason = "SAFETY"
            mock_response.prompt_feedback = mock_feedback
            mock_response.response_id = "test-id"
            mock_response.sdk_http_response = None

            mock_client_instance.aio.models.generate_content = AsyncMock(
                return_value=mock_response
            )

            transcriber = get_transcriber(
                TranscriberType.GEMINI,
                "test-project",
                '{"location": "us-central1"}',
            )
            transcriber.setup()

            with self.assertRaises(GeminiTranscriptionError) as context:
                await transcriber.transcribe(
                    audio_data=b"\x00" * 100,
                    duration_ms=1000,
                )
            self.assertIn(
                "Gemini prompt blocked. Block Reason: SAFETY",
                str(context.exception),
            )

    async def test_gemini_transcriber_safety_block(self) -> None:
        """Verifies that SAFETY finish reason raises GeminiTranscriptionError."""
        with patch(
            "backend.pipeline.transcription.transcribers.gemini.genai.Client"
        ) as mock_client_cls:
            mock_client_instance = MagicMock()
            mock_client_cls.return_value = mock_client_instance

            mock_response = MagicMock()
            mock_candidate = MagicMock()
            mock_candidate.finish_reason = types.FinishReason.SAFETY
            mock_candidate.content = None
            mock_candidate.finish_message = "blocked by safety settings"
            mock_candidate.safety_ratings = []
            mock_response.candidates = [mock_candidate]
            mock_response.response_id = "test-id"
            mock_response.sdk_http_response = None

            mock_client_instance.aio.models.generate_content = AsyncMock(
                return_value=mock_response
            )

            transcriber = get_transcriber(
                TranscriberType.GEMINI,
                "test-project",
                '{"location": "us-central1"}',
            )
            transcriber.setup()

            with self.assertRaises(GeminiTranscriptionError) as context:
                await transcriber.transcribe(
                    audio_data=b"\x00" * 100,
                    duration_ms=1000,
                )
            self.assertIn(
                "Gemini response blocked by safety filters",
                str(context.exception),
            )

    async def test_gemini_transcriber_transient_empty_response(self) -> None:
        """Verifies that an empty response with no finish reason raises GeminiTransientTranscriptionError."""
        with patch(
            "backend.pipeline.transcription.transcribers.gemini.genai.Client"
        ) as mock_client_cls:
            mock_client_instance = MagicMock()
            mock_client_cls.return_value = mock_client_instance

            mock_response = MagicMock()
            mock_candidate = MagicMock()
            mock_candidate.finish_reason = None
            mock_candidate.content = None
            mock_response.candidates = [mock_candidate]
            mock_response.response_id = "test-id"
            mock_response.sdk_http_response = None

            mock_client_instance.aio.models.generate_content = AsyncMock(
                return_value=mock_response
            )

            transcriber = get_transcriber(
                TranscriberType.GEMINI,
                "test-project",
                '{"location": "us-central1"}',
            )
            transcriber.setup()

            result = await transcriber.transcribe(
                audio_data=b"\x00" * 100,
                duration_ms=1000,
            )
            self.assertEqual(result, "")

    async def test_gemini_transcriber_transient_other_reason(self) -> None:
        """Verifies that an empty response with OTHER finish reason raises GeminiTransientTranscriptionError and falls back."""
        with patch(
            "backend.pipeline.transcription.transcribers.gemini.genai.Client"
        ) as mock_client_cls:
            mock_client_instance = MagicMock()
            mock_client_cls.return_value = mock_client_instance

            mock_response = MagicMock()
            mock_candidate = MagicMock()
            mock_candidate.finish_reason = types.FinishReason.OTHER
            mock_candidate.finish_message = "Internal error occurred."
            mock_candidate.content = None
            mock_response.candidates = [mock_candidate]
            mock_response.response_id = "test-id"
            mock_response.sdk_http_response = None

            mock_client_instance.aio.models.generate_content = AsyncMock(
                return_value=mock_response
            )

            transcriber = get_transcriber(
                TranscriberType.GEMINI,
                "test-project",
                '{"location": "us-central1"}',
            )
            transcriber.setup()

            with self.assertRaises(GeminiOtherFinishReasonError):
                await transcriber.transcribe(
                    audio_data=b"\x00" * 100,
                    duration_ms=1000,
                )

    async def test_gemini_transcriber_finish_reason_none_safety_block(
        self,
    ) -> None:
        """Verifies that finish_reason=None with blocked ratings raises a permanent GeminiTranscriptionError."""
        with patch(
            "backend.pipeline.transcription.transcribers.gemini.genai.Client"
        ) as mock_client_cls:
            mock_client_instance = MagicMock()
            mock_client_cls.return_value = mock_client_instance

            mock_response = MagicMock()
            mock_candidate = MagicMock()
            mock_candidate.finish_reason = None
            mock_candidate.content = None

            # Mock safety rating with a blocked category
            mock_rating = MagicMock()
            mock_rating.blocked = True
            mock_rating.category.name = "HARM_CATEGORY_HATE_SPEECH"
            mock_rating.probability.name = "HIGH"
            mock_candidate.safety_ratings = [mock_rating]

            mock_response.candidates = [mock_candidate]
            mock_response.response_id = "test-id"
            mock_response.sdk_http_response = None

            mock_client_instance.aio.models.generate_content = AsyncMock(
                return_value=mock_response
            )

            transcriber = get_transcriber(
                TranscriberType.GEMINI,
                "test-project",
                '{"location": "us-central1"}',
            )
            transcriber.setup()

            with self.assertRaises(GeminiTranscriptionError) as context:
                await transcriber.transcribe(
                    audio_data=b"\x00" * 100,
                    duration_ms=1000,
                )
            self.assertIn(
                "Gemini response blocked by safety filters.",
                str(context.exception),
            )
            self.assertIn(
                "HARM_CATEGORY_HATE_SPEECH=HIGH",
                str(context.exception),
            )

    async def test_gemini_transcriber_tuned_model_fallback(self) -> None:
        """Verifies that tuned model failures fall back to the foundation model."""
        with patch(
            "backend.pipeline.transcription.transcribers.gemini.genai.Client"
        ) as mock_client_cls:
            mock_client_instance = MagicMock()
            mock_client_cls.return_value = mock_client_instance

            # Mock first response: tuned model returns None finish_reason
            mock_response_1 = MagicMock()
            mock_candidate_1 = MagicMock()
            mock_candidate_1.finish_reason = None
            mock_candidate_1.content = None
            mock_response_1.candidates = [mock_candidate_1]
            mock_response_1.response_id = "tuned-failed-id"

            # Mock second response: foundation model succeeds
            mock_response_2 = MagicMock()
            mock_candidate_2 = MagicMock()
            mock_candidate_2.finish_reason = types.FinishReason.STOP

            mock_part = MagicMock()
            mock_part.text = "Fallback succeeded text"
            mock_candidate_2.content.parts = [mock_part]
            mock_response_2.candidates = [mock_candidate_2]
            mock_response_2.response_id = "fallback-success-id"

            # AsyncMock to return response 1 on first three calls, response 2 on fourth
            mock_client_instance.aio.models.generate_content = AsyncMock(
                side_effect=[
                    mock_response_1,
                    mock_response_1,
                    mock_response_1,
                    mock_response_2,
                ]
            )

            # Initialize transcriber with a tuned model config
            config_json = (
                '{"model": "projects/123/locations/us/endpoints/456", '
                '"location": "us-central1"}'
            )
            transcriber = get_transcriber(
                TranscriberType.GEMINI,
                "test-project",
                config_json,
            )

            transcriber.setup()

            result = await transcriber.transcribe(
                audio_data=b"\x00" * 100,
                duration_ms=1000,
            )

            # Asserts
            self.assertEqual(result, "Fallback succeeded text")
            self.assertEqual(
                mock_client_instance.aio.models.generate_content.call_count, 4
            )

            # Check arguments of the first call (tuned endpoint)
            first_call_args = (
                mock_client_instance.aio.models.generate_content.call_args_list[
                    0
                ]
            )
            self.assertEqual(
                first_call_args.kwargs["model"],
                "projects/123/locations/us/endpoints/456",
            )

            # Check arguments of the fourth call (foundation fallback model)
            fourth_call_args = (
                mock_client_instance.aio.models.generate_content.call_args_list[
                    3
                ]
            )
            self.assertEqual(
                fourth_call_args.kwargs["model"], DEFAULT_GEMINI_MODEL
            )

    async def test_gemini_transcriber_tuned_model_fallback_on_empty_string(
        self,
    ) -> None:
        """Verifies that if the tuned model returns an empty transcript
        (STOP with empty text), we fall back.
        """
        with patch(
            "backend.pipeline.transcription.transcribers.gemini.genai.Client"
        ) as mock_client_cls:
            mock_client_instance = MagicMock()
            mock_client_cls.return_value = mock_client_instance

            # Mock first response: tuned model returns STOP but with empty/no text parts
            mock_response_1 = MagicMock()
            mock_candidate_1 = MagicMock()
            mock_candidate_1.finish_reason = types.FinishReason.STOP
            mock_candidate_1.content.parts = []
            mock_response_1.candidates = [mock_candidate_1]
            mock_response_1.response_id = "tuned-empty-id"

            # Mock second response: foundation model succeeds
            mock_response_2 = MagicMock()
            mock_candidate_2 = MagicMock()
            mock_candidate_2.finish_reason = types.FinishReason.STOP
            mock_part = MagicMock()
            mock_part.text = "Fallback succeeded on empty"
            mock_candidate_2.content.parts = [mock_part]
            mock_response_2.candidates = [mock_candidate_2]
            mock_response_2.response_id = "fallback-success-id"

            # AsyncMock to return response 1 on first three calls, response 2 on fourth
            mock_client_instance.aio.models.generate_content = AsyncMock(
                side_effect=[
                    mock_response_1,
                    mock_response_1,
                    mock_response_1,
                    mock_response_2,
                ]
            )

            # Initialize transcriber with a tuned model config
            config_json = (
                '{"model": "projects/123/locations/us/endpoints/456", '
                '"location": "us-central1"}'
            )
            transcriber = get_transcriber(
                TranscriberType.GEMINI,
                "test-project",
                config_json,
            )
            transcriber.setup()

            result = await transcriber.transcribe(
                audio_data=b"\x00" * 100,
                duration_ms=1000,
            )

            # Asserts
            self.assertEqual(result, "Fallback succeeded on empty")
            self.assertEqual(
                mock_client_instance.aio.models.generate_content.call_count, 4
            )

    async def test_gemini_transcriber_tuned_model_fallback_both_fail(
        self,
    ) -> None:
        """Verifies that if both tuned and fallback models fail, we return empty string."""
        with (
            patch(
                "backend.pipeline.transcription.transcribers.gemini.genai.Client"
            ) as mock_client_cls,
            patch(
                "backend.pipeline.transcription.transcribers.gemini.asyncio.sleep"
            ) as mock_sleep,
        ):
            mock_client_instance = MagicMock()
            mock_client_cls.return_value = mock_client_instance

            # Mock first response: tuned model returns None finish_reason
            mock_response_1 = MagicMock()
            mock_candidate_1 = MagicMock()
            mock_candidate_1.finish_reason = None
            mock_candidate_1.content = None
            mock_response_1.candidates = [mock_candidate_1]
            mock_response_1.response_id = "tuned-failed-id"

            # Mock second response: fallback model attempt 1 returns None
            mock_response_2 = MagicMock()
            mock_candidate_2 = MagicMock()
            mock_candidate_2.finish_reason = None
            mock_candidate_2.content = None
            mock_response_2.candidates = [mock_candidate_2]
            mock_response_2.response_id = "fallback-failed-1"

            # Mock third response: fallback model attempt 2 also returns None
            mock_response_3 = MagicMock()
            mock_candidate_3 = MagicMock()
            mock_candidate_3.finish_reason = None
            mock_candidate_3.content = None
            mock_response_3.candidates = [mock_candidate_3]
            mock_response_3.response_id = "fallback-failed-2"

            # AsyncMock to return resp 1 (3 times), resp 2, resp 3 sequentially
            mock_client_instance.aio.models.generate_content = AsyncMock(
                side_effect=[
                    mock_response_1,
                    mock_response_1,
                    mock_response_1,
                    mock_response_2,
                    mock_response_3,
                ]
            )

            # Initialize transcriber with a tuned model config
            config_json = (
                '{"model": "projects/123/locations/us/endpoints/456", '
                '"location": "us-central1"}'
            )
            transcriber = get_transcriber(
                TranscriberType.GEMINI,
                "test-project",
                config_json,
            )
            transcriber.setup()

            result = await transcriber.transcribe(
                audio_data=b"\x00" * 100,
                duration_ms=1000,
            )

            # Asserts: all 3 tuned + 2 fallback attempts made, result is empty transcript
            self.assertEqual(result, "")
            self.assertEqual(
                mock_client_instance.aio.models.generate_content.call_count, 5
            )
            self.assertEqual(mock_sleep.call_count, 3)

    async def test_gemini_transcriber_tuned_model_fallback_on_no_candidates(
        self,
    ) -> None:
        """Verifies that SFT model with zero candidates retries against SFT, then falls back to the foundation model."""
        with (
            patch(
                "backend.pipeline.transcription.transcribers.gemini.genai.Client"
            ) as mock_client_cls,
            patch(
                "backend.pipeline.transcription.transcribers.gemini.asyncio.sleep"
            ) as mock_sleep,
        ):
            mock_client_instance = MagicMock()
            mock_client_cls.return_value = mock_client_instance

            # Mock SFT response: empty candidates
            mock_response_1 = MagicMock()
            mock_response_1.candidates = []
            mock_response_1.prompt_feedback = None
            mock_response_1.response_id = "tuned-no-candidates-id"
            mock_response_1.sdk_http_response = None

            # Mock fallback response: foundation model succeeds
            mock_response_2 = MagicMock()
            mock_candidate_2 = MagicMock()
            mock_candidate_2.finish_reason = types.FinishReason.STOP
            mock_part = MagicMock()
            mock_part.text = "Fallback succeeded on no candidates"
            mock_candidate_2.content.parts = [mock_part]
            mock_response_2.candidates = [mock_candidate_2]
            mock_response_2.response_id = "fallback-success-id"

            mock_client_instance.aio.models.generate_content = AsyncMock(
                side_effect=[
                    mock_response_1,
                    mock_response_1,
                    mock_response_1,
                    mock_response_2,
                ]
            )

            config_json = (
                '{"model": "projects/123/locations/us/endpoints/456", '
                '"location": "us-central1"}'
            )
            transcriber = get_transcriber(
                TranscriberType.GEMINI,
                "test-project",
                config_json,
            )
            transcriber.setup()

            result = await transcriber.transcribe(
                audio_data=b"\x00" * 100,
                duration_ms=1000,
            )

            # Asserts: 3 attempts made against SFT model, then falls back
            # and succeeds against the foundation model.
            self.assertEqual(result, "Fallback succeeded on no candidates")
            self.assertEqual(
                mock_client_instance.aio.models.generate_content.call_count, 4
            )
            self.assertEqual(mock_sleep.call_count, 2)

            fourth_call_args = (
                mock_client_instance.aio.models.generate_content.call_args_list[
                    3
                ]
            )
            self.assertEqual(
                fourth_call_args.kwargs["model"], DEFAULT_GEMINI_MODEL
            )

    async def test_gemini_transcriber_tuned_model_fallback_on_empty_string_both_fail(
        self,
    ) -> None:
        """Verifies that fallback model retries and returns empty string if both return empty text."""
        with (
            patch(
                "backend.pipeline.transcription.transcribers.gemini.genai.Client"
            ) as mock_client_cls,
            patch(
                "backend.pipeline.transcription.transcribers.gemini.asyncio.sleep"
            ) as mock_sleep,
        ):
            mock_client_instance = MagicMock()
            mock_client_cls.return_value = mock_client_instance

            # Mock first response: tuned model returns STOP but with empty/no text parts
            mock_response_1 = MagicMock()
            mock_candidate_1 = MagicMock()
            mock_candidate_1.finish_reason = types.FinishReason.STOP
            mock_candidate_1.content.parts = []
            mock_response_1.candidates = [mock_candidate_1]
            mock_response_1.response_id = "tuned-empty-id"

            # Mock second response: fallback model attempt 1 returns STOP with empty text
            mock_response_2 = MagicMock()
            mock_candidate_2 = MagicMock()
            mock_candidate_2.finish_reason = types.FinishReason.STOP
            mock_candidate_2.content.parts = []
            mock_response_2.candidates = [mock_candidate_2]
            mock_response_2.response_id = "fallback-empty-1"

            # Mock third response: fallback model attempt 2 also returns STOP with empty text
            mock_response_3 = MagicMock()
            mock_candidate_3 = MagicMock()
            mock_candidate_3.finish_reason = types.FinishReason.STOP
            mock_candidate_3.content.parts = []
            mock_response_3.candidates = [mock_candidate_3]
            mock_response_3.response_id = "fallback-empty-2"

            # AsyncMock to return resp 1 (3 times), resp 2, resp 3 sequentially
            mock_client_instance.aio.models.generate_content = AsyncMock(
                side_effect=[
                    mock_response_1,
                    mock_response_1,
                    mock_response_1,
                    mock_response_2,
                    mock_response_3,
                ]
            )

            # Initialize transcriber with a tuned model config
            config_json = (
                '{"model": "projects/123/locations/us/endpoints/456", '
                '"location": "us-central1"}'
            )
            transcriber = get_transcriber(
                TranscriberType.GEMINI,
                "test-project",
                config_json,
            )
            transcriber.setup()

            result = await transcriber.transcribe(
                audio_data=b"\x00" * 100,
                duration_ms=1000,
            )

            # Asserts: all 3 tuned + 2 fallback attempts made, result is empty transcript
            self.assertEqual(result, "")
            self.assertEqual(
                mock_client_instance.aio.models.generate_content.call_count, 5
            )
            self.assertEqual(mock_sleep.call_count, 3)

    @patch("asyncio.sleep", new_callable=AsyncMock)
    async def test_gemini_transcriber_custom_fallback_settings(
        self, mock_sleep
    ) -> None:
        """Verifies that the transcriber respects custom fallback model and retry attempts settings."""
        with patch(
            "backend.pipeline.transcription.transcribers.gemini.genai.Client"
        ) as mock_client_cls:
            mock_client_instance = MagicMock()
            mock_client_cls.return_value = mock_client_instance

            # Mock first response: tuned model returns None finish_reason
            mock_response_1 = MagicMock()
            mock_candidate_1 = MagicMock()
            mock_candidate_1.finish_reason = None
            mock_candidate_1.content = None
            mock_response_1.candidates = [mock_candidate_1]
            mock_response_1.response_id = "tuned-failed-id"

            # Mock second response: custom fallback model fails (1st attempt)
            mock_response_2 = MagicMock()
            mock_candidate_2 = MagicMock()
            mock_candidate_2.finish_reason = None
            mock_candidate_2.content = None
            mock_response_2.candidates = [mock_candidate_2]
            mock_response_2.response_id = "fallback-failed-1"

            # Mock third response: custom fallback model succeeds (2nd attempt)
            mock_response_3 = MagicMock()
            mock_candidate_3 = MagicMock()
            mock_candidate_3.finish_reason = types.FinishReason.STOP
            mock_part = MagicMock()
            mock_part.text = "Custom fallback success"
            mock_candidate_3.content.parts = [mock_part]
            mock_response_3.candidates = [mock_candidate_3]
            mock_response_3.response_id = "fallback-success-id"

            # If we configure 3 attempts, it should call all 3 times
            mock_client_instance.aio.models.generate_content = AsyncMock(
                side_effect=[
                    mock_response_1,
                    mock_response_1,
                    mock_response_1,
                    mock_response_2,
                    mock_response_3,
                ]
            )

            # Initialize transcriber with a tuned model config, custom fallback model and 3 attempts
            config_json = (
                '{"model": "projects/123/locations/us/endpoints/456", '
                '"location": "us-central1", '
                '"fallback_model": "gemini-custom-fallback", '
                '"fallback_retry_attempts": 3}'
            )
            transcriber = get_transcriber(
                TranscriberType.GEMINI,
                "test-project",
                config_json,
            )

            transcriber.setup()

            result = await transcriber.transcribe(
                audio_data=b"\x00" * 100,
                duration_ms=1000,
            )

            self.assertEqual(result, "Custom fallback success")
            # 3 tuned calls + 2 fallback calls = 5 calls total
            self.assertEqual(
                mock_client_instance.aio.models.generate_content.call_count, 5
            )
            # Checked fallback calls use custom model
            fallback_call_1 = (
                mock_client_instance.aio.models.generate_content.call_args_list[
                    3
                ]
            )
            self.assertEqual(
                fallback_call_1.kwargs.get("model"), "gemini-custom-fallback"
            )
            fallback_call_2 = (
                mock_client_instance.aio.models.generate_content.call_args_list[
                    4
                ]
            )
            self.assertEqual(
                fallback_call_2.kwargs.get("model"), "gemini-custom-fallback"
            )

            # Sleep called 3 times (2 between tuned attempts, 1 between fallback attempts)
            self.assertEqual(mock_sleep.call_count, 3)

    @patch("asyncio.sleep", new_callable=AsyncMock)
    async def test_gemini_transcriber_multi_region_fallback(
        self, mock_sleep
    ) -> None:
        """Verifies that the transcriber initializes regional clients correctly when primary and fallback models are in different regions."""
        with patch(
            "backend.pipeline.transcription.transcribers.gemini.genai.Client"
        ) as mock_client_cls:
            primary_client = MagicMock()
            fallback_client = MagicMock()

            def get_mock_client(**kwargs):
                loc = kwargs.get("location")
                if loc == "us-central1":
                    return primary_client
                if loc == "us-east4":
                    return fallback_client
                raise ValueError

            mock_client_cls.side_effect = get_mock_client

            # Mock responses:
            # Primary model (on primary_client) fails with transient error
            mock_response_primary = MagicMock()
            mock_candidate_primary = MagicMock()
            mock_candidate_primary.finish_reason = None
            mock_candidate_primary.content = None
            mock_response_primary.candidates = [mock_candidate_primary]
            mock_response_primary.response_id = "primary-failed-id"

            primary_client.aio.models.generate_content = AsyncMock(
                return_value=mock_response_primary
            )

            # Fallback model (on fallback_client) succeeds
            mock_response_fallback = MagicMock()
            mock_candidate_fallback = MagicMock()
            mock_candidate_fallback.finish_reason = types.FinishReason.STOP
            mock_part = MagicMock()
            mock_part.text = "Fallback success"
            mock_candidate_fallback.content.parts = [mock_part]
            mock_response_fallback.candidates = [mock_candidate_fallback]
            mock_response_fallback.response_id = "fallback-success-id"

            fallback_client.aio.models.generate_content = AsyncMock(
                return_value=mock_response_fallback
            )

            # Config: primary in us-central1 (parsed from model URI), fallback in us-east4 (via config)
            config_json = (
                '{"model": "projects/123/locations/us-central1/endpoints/456", '
                '"fallback_model": "projects/123/locations/us-east4/endpoints/789", '
                '"fallback_retry_attempts": 1}'
            )
            transcriber = get_transcriber(
                TranscriberType.GEMINI,
                "test-project",
                config_json,
            )

            transcriber.setup()
            assert isinstance(transcriber, GeminiTranscriber)

            # Verify setup() initialized client for us-central1
            self.assertIn("us-central1", transcriber._clients)
            self.assertEqual(
                transcriber._clients["us-central1"], primary_client
            )

            result = await transcriber.transcribe(
                audio_data=b"\x00" * 100,
                duration_ms=1000,
            )

            self.assertEqual(result, "Fallback success")

            # Verify that client for us-east4 was lazily initialized during fallback
            self.assertIn("us-east4", transcriber._clients)
            self.assertEqual(transcriber._clients["us-east4"], fallback_client)

            # Verify correct clients were called
            self.assertEqual(
                primary_client.aio.models.generate_content.call_count, 3
            )
            fallback_client.aio.models.generate_content.assert_called_once()

    @patch("asyncio.sleep", new_callable=AsyncMock)
    async def test_gemini_transcriber_multi_region_fallback_explicit_location(
        self, mock_sleep
    ) -> None:
        """Verifies that the transcriber respects explicit fallback_location config."""
        with patch(
            "backend.pipeline.transcription.transcribers.gemini.genai.Client"
        ) as mock_client_cls:
            primary_client = MagicMock()
            fallback_client = MagicMock()

            def get_mock_client(**kwargs):
                loc = kwargs.get("location")
                if loc == "us-central1":
                    return primary_client
                if loc == "us-west2":
                    return fallback_client
                raise ValueError

            mock_client_cls.side_effect = get_mock_client

            # Mock responses
            mock_response_primary = MagicMock()
            mock_candidate_primary = MagicMock()
            mock_candidate_primary.finish_reason = None
            mock_candidate_primary.content = None
            mock_response_primary.candidates = [mock_candidate_primary]
            primary_client.aio.models.generate_content = AsyncMock(
                return_value=mock_response_primary
            )

            mock_response_fallback = MagicMock()
            mock_candidate_fallback = MagicMock()
            mock_candidate_fallback.finish_reason = types.FinishReason.STOP
            mock_part = MagicMock()
            mock_part.text = "Explicit fallback success"
            mock_candidate_fallback.content.parts = [mock_part]
            mock_response_fallback.candidates = [mock_candidate_fallback]
            fallback_client.aio.models.generate_content = AsyncMock(
                return_value=mock_response_fallback
            )

            # Config: primary model (tuned), fallback model (tuned)
            config_json = (
                '{"model": "projects/123/locations/us-central1/endpoints/456", '
                '"location": "us-central1", '
                '"fallback_model": "projects/123/locations/us-west2/endpoints/789", '
                '"fallback_location": "us-west2", '
                '"fallback_retry_attempts": 1}'
            )
            transcriber = get_transcriber(
                TranscriberType.GEMINI,
                "test-project",
                config_json,
            )

            transcriber.setup()
            assert isinstance(transcriber, GeminiTranscriber)
            result = await transcriber.transcribe(
                audio_data=b"\x00" * 100,
                duration_ms=1000,
            )

            self.assertEqual(result, "Explicit fallback success")
            self.assertIn("us-central1", transcriber._clients)
            self.assertIn("us-west2", transcriber._clients)

    @patch("asyncio.sleep", new_callable=AsyncMock)
    async def test_gemini_transcriber_multi_region_fallback_default_location(
        self, mock_sleep
    ) -> None:
        """Verifies that the fallback model defaults to DEFAULT_GEMINI_LOCATION when no fallback location is configured."""
        with patch(
            "backend.pipeline.transcription.transcribers.gemini.genai.Client"
        ) as mock_client_cls:
            primary_client = MagicMock()
            fallback_client = MagicMock()

            def get_mock_client(**kwargs):
                loc = kwargs.get("location")
                if loc == "us-central1":
                    return primary_client
                if loc == "us":
                    return fallback_client
                raise ValueError

            mock_client_cls.side_effect = get_mock_client

            # Mock responses
            mock_response_primary = MagicMock()
            mock_candidate_primary = MagicMock()
            mock_candidate_primary.finish_reason = None
            mock_candidate_primary.content = None
            mock_response_primary.candidates = [mock_candidate_primary]
            primary_client.aio.models.generate_content = AsyncMock(
                return_value=mock_response_primary
            )

            mock_response_fallback = MagicMock()
            mock_candidate_fallback = MagicMock()
            mock_candidate_fallback.finish_reason = types.FinishReason.STOP
            mock_part = MagicMock()
            mock_part.text = "Default fallback success"
            mock_candidate_fallback.content.parts = [mock_part]
            mock_response_fallback.candidates = [mock_candidate_fallback]
            fallback_client.aio.models.generate_content = AsyncMock(
                return_value=mock_response_fallback
            )

            # Config: location is us-central1, fallback_location is not configured (defaults to DEFAULT_GEMINI_LOCATION = "us")
            config_json = (
                '{"model": "projects/123/locations/us-central1/endpoints/456", '
                '"location": "us-central1", '
                '"fallback_model": "projects/123/locations/us/endpoints/789", '
                '"fallback_retry_attempts": 1}'
            )
            transcriber = get_transcriber(
                TranscriberType.GEMINI,
                "test-project",
                config_json,
            )

            transcriber.setup()
            assert isinstance(transcriber, GeminiTranscriber)
            result = await transcriber.transcribe(
                audio_data=b"\x00" * 100,
                duration_ms=1000,
            )

            self.assertEqual(result, "Default fallback success")
            self.assertIn("us-central1", transcriber._clients)
            self.assertIn("us", transcriber._clients)

    @patch("asyncio.sleep", new_callable=AsyncMock)
    async def test_gemini_transcriber_fallback_location_constructor_override(
        self, mock_sleep
    ) -> None:
        """Verifies that the fallback_location passed directly to GeminiTranscriber constructor overrides config."""
        with patch(
            "backend.pipeline.transcription.transcribers.gemini.genai.Client"
        ) as mock_client_cls:
            primary_client = MagicMock()
            fallback_client = MagicMock()

            def get_mock_client(**kwargs):
                loc = kwargs.get("location")
                if loc == "us-central1":
                    return primary_client
                if loc == "us-east1":
                    return fallback_client
                raise ValueError

            mock_client_cls.side_effect = get_mock_client

            # Mock responses
            mock_response_primary = MagicMock()
            mock_candidate_primary = MagicMock()
            mock_candidate_primary.finish_reason = None
            mock_candidate_primary.content = None
            mock_response_primary.candidates = [mock_candidate_primary]
            primary_client.aio.models.generate_content = AsyncMock(
                return_value=mock_response_primary
            )

            mock_response_fallback = MagicMock()
            mock_candidate_fallback = MagicMock()
            mock_candidate_fallback.finish_reason = types.FinishReason.STOP
            mock_part = MagicMock()
            mock_part.text = "Constructor override fallback success"
            mock_candidate_fallback.content.parts = [mock_part]
            mock_response_fallback.candidates = [mock_candidate_fallback]
            fallback_client.aio.models.generate_content = AsyncMock(
                return_value=mock_response_fallback
            )

            config = GeminiConfig(
                model="projects/123/locations/us-central1/endpoints/456",
                location="us-central1",
                fallback_model="gemini-3.5-flash",
                fallback_location="us-west2",
                fallback_retry_attempts=1,
            )
            # Instantiate GeminiTranscriber directly (passing fallback_location="us-east1" override)
            transcriber = GeminiTranscriber(
                project_id="test-project",
                config=config,
                fallback_location="us-east1",
            )

            transcriber.setup()
            result = await transcriber.transcribe(
                audio_data=b"\x00" * 100,
                duration_ms=1000,
            )

            self.assertEqual(result, "Constructor override fallback success")
            self.assertIn("us-central1", transcriber._clients)
            self.assertIn("us-east1", transcriber._clients)

    def test_gemini_transcriber_setup(self) -> None:
        """Verifies that the Gemini transcriber initializes the GenAI client with correct options."""
        with patch(
            "backend.pipeline.transcription.transcribers.gemini.genai.Client"
        ) as mock_client_cls:
            transcriber = get_transcriber(
                TranscriberType.GEMINI,
                "test-project",
                '{"location": "us-test"}',
            )
            transcriber.setup()

            mock_client_cls.assert_called_once()
            _, kwargs = mock_client_cls.call_args
            self.assertTrue(kwargs.get("enterprise"))
            self.assertEqual(kwargs.get("project"), "test-project")
            self.assertEqual(kwargs.get("location"), "us-test")

            # Verify retry options are set to 3 attempts and default timeout.
            http_options = kwargs.get("http_options")
            self.assertIsNotNone(http_options)
            self.assertEqual(http_options.timeout, 60000)
            self.assertIsNotNone(http_options.retry_options)
            self.assertEqual(http_options.retry_options.attempts, 3)
            self.assertIsNotNone(http_options.httpx_async_client)
            self.assertIsInstance(
                http_options.httpx_async_client,
                httpx.AsyncClient,
            )

    def test_gemini_transcriber_setup_custom_retry(self) -> None:
        """Verifies that the Gemini transcriber initializes the GenAI client with custom retry options."""
        with patch(
            "backend.pipeline.transcription.transcribers.gemini.genai.Client"
        ) as mock_client_cls:
            transcriber = get_transcriber(
                TranscriberType.GEMINI,
                "test-project",
                '{"location": "us-test", "retry_attempts": 8, "retry_initial_delay": 2.5, "retry_max_delay": 90.0, "retry_multiplier": 3.0, "client_timeout_ms": 120000}',
            )
            transcriber.setup()

            mock_client_cls.assert_called_once()
            _, kwargs = mock_client_cls.call_args
            http_options = kwargs.get("http_options")
            self.assertIsNotNone(http_options)
            self.assertEqual(http_options.timeout, 120000)
            self.assertIsNotNone(http_options.retry_options)
            self.assertEqual(http_options.retry_options.attempts, 8)
            self.assertEqual(http_options.retry_options.initial_delay, 2.5)
            self.assertEqual(http_options.retry_options.max_delay, 90.0)
            self.assertEqual(http_options.retry_options.exp_base, 3.0)


if __name__ == "__main__":
    unittest.main()
