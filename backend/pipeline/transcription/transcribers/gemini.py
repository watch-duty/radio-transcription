"""Google Gemini 3.1 Flash Lite transcriber implementation."""

import dataclasses
import mimetypes

import pydantic
from google import genai
from google.genai import types

from backend.pipeline.common.log_helper import get_task_logger
from backend.pipeline.common.utils import ConfigBase
from backend.pipeline.transcription.transcribers import base, prompts

DEFAULT_GEMINI_LOCATION = "global"
DEFAULT_GEMINI_MODEL = "gemini-3.1-flash-lite"
_VALID_FINISH_REASONS = {"STOP", "MAX_TOKENS", "Stop", "MaxTokens"}
# Model configuration defaults
_DEFAULT_TEMPERATURE = 0.0
_DEFAULT_MAX_OUTPUT_TOKENS = 512
_DEFAULT_SAFETY_SETTINGS = [
    {"category": "HARM_CATEGORY_HATE_SPEECH", "threshold": "BLOCK_NONE"},
    {"category": "HARM_CATEGORY_SEXUALLY_EXPLICIT", "threshold": "BLOCK_NONE"},
    {"category": "HARM_CATEGORY_DANGEROUS_CONTENT", "threshold": "BLOCK_NONE"},
    {"category": "HARM_CATEGORY_HARASSMENT", "threshold": "BLOCK_NONE"},
]

logger = get_task_logger(
    __name__, {"system": "transcription", "component": "gemini"}
)


@dataclasses.dataclass(frozen=True)
class SafetySetting:
    """Safety setting category and threshold."""

    category: str
    threshold: str


class GeminiConfig(ConfigBase):
    """Strongly typed configuration for the Gemini Transcriber."""

    model: str = DEFAULT_GEMINI_MODEL
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
    prompt: str | None = prompts.GEMINI_TRANSCRIBE_WITH_CONTEXT_SYSTEM_PROMPT


class GeminiTranscriber(base.Transcriber):
    """Transcriber implementation using Google GenAI SDK with Gemini 3.1."""

    def __init__(
        self,
        project_id: str,
        config: GeminiConfig,
        location: str = DEFAULT_GEMINI_LOCATION,
    ) -> None:
        """Binds the GCP Project ID and parsed configuration."""
        self.project_id = project_id
        self.config = config
        self.client: genai.Client | None = None
        self.location = location

    def setup(self) -> None:
        """Instantiate the GenAI API client."""
        self.client = genai.Client(
            vertexai=True,
            project=self.project_id,
            location=self.location,
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
        """
        if self.client is None:
            msg = "Transcriber client used before setup() was called."
            raise RuntimeError(msg)

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

        else:
            logger.error("No audio_data or uri provided to Gemini transcriber.")
            return None

        contents = types.Content(role="user", parts=parts)

        generation_config = types.GenerateContentConfig(
            temperature=self.config.temperature,
            max_output_tokens=self.config.max_output_tokens,
            system_instruction=self.config.prompt,
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

        # TODO(https://linear.app/watchduty/issue/GOO-579/add-retry-policy-for-gemini-transcriber): Add in retry policy
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
            return None

        has_parts = bool(candidate.content and candidate.content.parts)
        if not has_parts:
            logger.warning("Gemini response candidate had no content/parts.")
            return None

        transcript = None
        try:
            if response.text:
                transcript = response.text.strip()
        except ValueError as val_err:
            logger.warning(
                "Failed to retrieve text from Gemini response: %s", val_err
            )
            return None

        if transcript == "[UNINTELLIGIBLE]":
            logger.info(
                "Transcription returned [UNINTELLIGIBLE] "
                "(no discernable speech)."
            )
            return None

        return transcript
