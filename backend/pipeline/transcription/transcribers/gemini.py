"""Google Gemini 3.1 Flash Lite transcriber implementation."""

import dataclasses
import mimetypes

import pydantic
from google import genai
from google.genai import types

from backend.pipeline.common import log_helper, utils
from backend.pipeline.transcription.transcribers import base, prompts

DEFAULT_GEMINI_LOCATION = "us"
DEFAULT_GEMINI_MODEL = "gemini-3.1-flash-lite"
# Defensively handle both proto (UPPERCASE) and pythonic (PascalCase) SDK enum casings
_VALID_FINISH_REASONS = {"STOP", "MAX_TOKENS", "Stop", "MaxTokens"}

_DEFAULT_TEMPERATURE = 0.0
_DEFAULT_MAX_OUTPUT_TOKENS = 512

# API retry defaults
DEFAULT_GEMINI_RETRY_ATTEMPTS = 5
DEFAULT_GEMINI_RETRY_INITIAL_DELAY = 1.0
DEFAULT_GEMINI_RETRY_MAX_DELAY = 60.0
DEFAULT_GEMINI_RETRY_MULTIPLIER = 2.0
DEFAULT_GEMINI_TIMEOUT_MS = 60000


# Emergency dispatch traffic frequently contains graphic descriptions of
# violence, accidents, or criminal activity. To prevent dropping valid
# dispatches, we must disable all safety filters (BLOCK_NONE).
_DEFAULT_SAFETY_SETTINGS = [
    {"category": "HARM_CATEGORY_HATE_SPEECH", "threshold": "BLOCK_NONE"},
    {"category": "HARM_CATEGORY_SEXUALLY_EXPLICIT", "threshold": "BLOCK_NONE"},
    {"category": "HARM_CATEGORY_DANGEROUS_CONTENT", "threshold": "BLOCK_NONE"},
    {"category": "HARM_CATEGORY_HARASSMENT", "threshold": "BLOCK_NONE"},
    {"category": "HARM_CATEGORY_CIVIC_INTEGRITY", "threshold": "BLOCK_NONE"},
    {"category": "HARM_CATEGORY_JAILBREAK", "threshold": "BLOCK_NONE"},
]

logger = log_helper.get_task_logger(
    __name__, {"system": "transcription", "component": "gemini"}
)


@dataclasses.dataclass(frozen=True)
class SafetySetting:
    """Safety setting category and threshold."""

    category: str
    threshold: str


class GeminiConfig(utils.ConfigBase):
    """Strongly typed configuration for the Gemini Transcriber."""

    model: str = DEFAULT_GEMINI_MODEL
    location: str = DEFAULT_GEMINI_LOCATION
    mime_type: str = "audio/flac"
    temperature: float = _DEFAULT_TEMPERATURE
    max_output_tokens: int = _DEFAULT_MAX_OUTPUT_TOKENS
    safety_settings: list[SafetySetting] = pydantic.Field(
        default_factory=lambda: [
            SafetySetting(
                category=setting["category"], threshold=setting["threshold"]
            )
            for setting in _DEFAULT_SAFETY_SETTINGS
        ]
    )
    prompt: str | None = prompts.GEMINI_PROMPT

    retry_attempts: int = DEFAULT_GEMINI_RETRY_ATTEMPTS
    retry_initial_delay: float = DEFAULT_GEMINI_RETRY_INITIAL_DELAY
    retry_max_delay: float = DEFAULT_GEMINI_RETRY_MAX_DELAY
    retry_multiplier: float = DEFAULT_GEMINI_RETRY_MULTIPLIER
    timeout_ms: int = DEFAULT_GEMINI_TIMEOUT_MS


class GeminiTranscriber(base.Transcriber):
    """Transcriber implementation using Google GenAI SDK with Gemini 3.1."""

    def __init__(
        self,
        project_id: str,
        config: GeminiConfig,
        location: str | None = None,
    ) -> None:
        """Binds the GCP Project ID and parsed configuration."""
        self.project_id = project_id
        self.config = config
        self.client: genai.Client | None = None
        self.location = location or config.location

    def setup(self) -> None:
        """Instantiate the GenAI API client with a robust retry policy."""
        self.client = genai.Client(
            vertexai=True,
            project=self.project_id,
            location=self.location,
            http_options=types.HttpOptions(
                timeout=self.config.timeout_ms,
                retry_options=types.HttpRetryOptions(
                    attempts=self.config.retry_attempts,
                    initial_delay=self.config.retry_initial_delay,
                    max_delay=self.config.retry_max_delay,
                    exp_base=self.config.retry_multiplier,
                ),
            ),
        )

    async def transcribe(
        self,
        *,
        audio_data: bytes | None = None,
        uri: str | None = None,
        duration_ms: int,
    ) -> str | None:
        """Transcribes the audio payload using Gemini API.

        Accepts either raw bytes or a GCS URI reference.

        Args:
            audio_data: Optional raw audio bytes to transcribe.
            uri: Optional GCS URI (gs://...) of the audio file.
            duration_ms: Duration of the audio segment in milliseconds.

        Returns:
            The transcribed text, or None if transcription failed or the
            audio was determined to be empty/unintelligible.

        Raises:
            RuntimeError: If called before setup() has been run.
        """
        if self.client is None:
            msg = "Transcriber client used before setup() was called."
            raise RuntimeError(msg)

        if not uri and not audio_data:
            logger.error("No audio_data or uri provided to Gemini transcriber.")
            return None

        logger.info(
            "Transcribing %.3fs of audio %s",
            duration_ms / 1000,
            f"from GCS URI: {uri}" if uri else "from in-memory bytes",
        )

        # TODO(http://linear.app/watchduty/issue/GOO-580/extend-gemini-transcriber-to-support-context): Support context
        mime_type = self.config.mime_type
        if uri:
            guessed_mime, _ = mimetypes.guess_type(uri)
            if guessed_mime:
                mime_type = guessed_mime

        parts = []
        if uri:
            parts.append(types.Part.from_uri(file_uri=uri, mime_type=mime_type))
        elif audio_data:
            parts.append(
                types.Part.from_bytes(data=audio_data, mime_type=mime_type)
            )

        contents = types.Content(role="user", parts=parts)

        generation_config = types.GenerateContentConfig(
            temperature=self.config.temperature,
            max_output_tokens=self.config.max_output_tokens,
            system_instruction=self.config.prompt,
            thinking_config=types.ThinkingConfig(thinking_budget=0),
            safety_settings=[
                types.SafetySetting(
                    category=types.HarmCategory(setting.category),
                    threshold=types.HarmBlockThreshold(setting.threshold),
                )
                for setting in self.config.safety_settings
            ]
            if self.config.safety_settings
            else None,
        )

        # Note: Retry policy is configured globally on the client in setup()
        response = await self.client.aio.models.generate_content(
            # TODO(https://linear.app/watchduty/issue/GOO-584/update-gemini-31-flash-lite-to-use-fine-tuned-model): Use fine tuned model
            model=self.config.model,
            contents=contents,
            config=generation_config,
        )

        return self._parse_response(response)

    def _parse_response(
        self,
        response: types.GenerateContentResponse,
    ) -> str | None:
        """Extracts transcript text from Gemini GenerateContentResponse."""
        if not response.candidates:
            logger.warning("Gemini response returned no candidates.")
            return None

        candidate = response.candidates[0]
        reason_str = (
            candidate.finish_reason.name
            if candidate.finish_reason is not None
            else None
        )

        is_valid_reason = (
            reason_str is None or reason_str in _VALID_FINISH_REASONS
        )
        if not is_valid_reason:
            logger.warning(
                f"Gemini response finished with reason: {reason_str}"
            )
            if reason_str == "RECITATION":
                logger.info(
                    "Treating RECITATION block as UNINTELLIGIBLE fallback."
                )
                return "[UNINTELLIGIBLE]"
            return None

        if not candidate.content or not candidate.content.parts:
            logger.warning(
                "Gemini response candidate had no content or parts. "
                "Finish reason: %s. Candidate: %s",
                reason_str,
                candidate,
            )
            return None

        text_parts = [p.text for p in candidate.content.parts if p.text]
        if not text_parts:
            logger.warning("Gemini response candidate had no text parts.")
            return None

        transcript = "".join(text_parts).strip()
        return transcript or None
