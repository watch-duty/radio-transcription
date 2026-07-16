"""Google Gemini transcriber implementation."""

import dataclasses
import mimetypes

import pydantic
from google import genai
from google.genai import types

from backend.pipeline.common import exceptions, log_helper, utils
from backend.pipeline.transcription.transcribers import base, prompts

DEFAULT_GEMINI_LOCATION = "us"
DEFAULT_GEMINI_MODEL = "gemini-3.1-flash-lite"
# STOP indicates a complete, successful response.
# MAX_TOKENS is treated as a partial success (partial text returned, no retry).
_VALID_FINISH_REASONS = {
    types.FinishReason.STOP.name,
    types.FinishReason.MAX_TOKENS.name,
}

# These finish reasons result in a hard failure (no transcript, no retry)
# and are categorized as "safety_blocked".
_BLOCKED_FINISH_REASONS = {
    types.FinishReason.SAFETY,
    types.FinishReason.RECITATION,
    types.FinishReason.BLOCKLIST,
    types.FinishReason.PROHIBITED_CONTENT,
    types.FinishReason.SPII,
}

_DEFAULT_TEMPERATURE = 0.0
_DEFAULT_MAX_OUTPUT_TOKENS = 512

# API retry defaults
DEFAULT_GEMINI_RETRY_ATTEMPTS = 5
DEFAULT_GEMINI_RETRY_INITIAL_DELAY = 1.0
DEFAULT_GEMINI_RETRY_MAX_DELAY = 60.0
DEFAULT_GEMINI_RETRY_MULTIPLIER = 2.0
DEFAULT_GEMINI_CLIENT_TIMEOUT_MS = 120000


# Emergency dispatch traffic frequently contains graphic descriptions of
# violence, accidents, or criminal activity. To prevent dropping valid
# dispatches, we must disable all safety filters (BLOCK_NONE).
_DEFAULT_SAFETY_SETTINGS = [
    {"category": "HARM_CATEGORY_HATE_SPEECH", "threshold": "BLOCK_NONE"},
    {"category": "HARM_CATEGORY_SEXUALLY_EXPLICIT", "threshold": "BLOCK_NONE"},
    {"category": "HARM_CATEGORY_DANGEROUS_CONTENT", "threshold": "BLOCK_NONE"},
    {"category": "HARM_CATEGORY_HARASSMENT", "threshold": "BLOCK_NONE"},
    {"category": "HARM_CATEGORY_CIVIC_INTEGRITY", "threshold": "BLOCK_NONE"},
]

logger = log_helper.get_task_logger(
    __name__, {"system": "transcription", "component": "gemini"}
)


class GeminiTranscriptionError(ValueError):
    """Raised when Gemini transcription fails or is blocked."""

    def __init__(
        self, message: str, finish_reason: types.FinishReason | None = None
    ) -> None:
        super().__init__(message)
        self.finish_reason = finish_reason


class GeminiTransientTranscriptionError(GeminiTranscriptionError):
    """Raised when Gemini transcription fails due to a potentially transient model/backend issue."""


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
    client_timeout_ms: int = DEFAULT_GEMINI_CLIENT_TIMEOUT_MS


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
            enterprise=True,
            project=self.project_id,
            location=self.location,
            http_options=types.HttpOptions(
                timeout=self.config.client_timeout_ms,
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

        try:
            response = await self.client.aio.models.generate_content(
                model=self.config.model,
                contents=contents,
                config=generation_config,
            )
            transcript = self._parse_response(response)
            if not transcript.strip():
                return await self._fallback_transcribe(
                    contents,
                    generation_config,
                    "Tuned model returned empty transcript",
                )
        except GeminiTransientTranscriptionError as e:
            return await self._fallback_transcribe(
                contents,
                generation_config,
                str(e),
            )
        else:
            return transcript

    async def _fallback_transcribe(
        self,
        contents: types.Content,
        generation_config: types.GenerateContentConfig,
        reason: str,
    ) -> str:
        if self.client is None:
            msg = "Client not initialized"
            raise RuntimeError(msg)
        if self.config.model == "gemini-3.1-flash-lite":
            logger.info(
                "Model %s returned incomplete/empty response: %s. "
                "Treating as empty transcription.",
                self.config.model,
                reason,
            )
            return ""

        logger.warning(
            "Tuned model %s failed: %s. "
            "Falling back to foundation model gemini-3.1-flash-lite...",
            self.config.model,
            reason,
        )
        try:
            fallback_response = await self.client.aio.models.generate_content(
                model="gemini-3.1-flash-lite",
                contents=contents,
                config=generation_config,
            )
            return self._parse_response(fallback_response)
        except GeminiTransientTranscriptionError as e:
            logger.info(
                "Fallback model gemini-3.1-flash-lite also returned incomplete/empty response: %s. "
                "Treating as empty transcription.",
                e,
            )
            return ""

    def _get_blocked_ratings(self, candidate: types.Candidate) -> str:
        """Helper to extract a string list of blocked safety categories."""
        if not candidate.safety_ratings:
            return "None"
        blocked = []
        for r in candidate.safety_ratings:
            if getattr(r, "blocked", False):
                cat = getattr(r.category, "name", str(r.category))
                prob = getattr(r.probability, "name", str(r.probability))
                blocked.append(f"{cat}={prob}")
        return ", ".join(blocked) if blocked else "None"

    def _parse_response(
        self,
        response: types.GenerateContentResponse,
    ) -> str:
        """Extracts transcript text from Gemini GenerateContentResponse."""
        response_id = response.response_id or "Unknown"
        headers = (
            response.sdk_http_response.headers
            if response.sdk_http_response
            else {}
        )

        if not response.candidates:
            # Check prompt feedback blocks (safety filters at request level)
            if (
                response.prompt_feedback
                and response.prompt_feedback.block_reason
            ):
                block_reason = response.prompt_feedback.block_reason
                logger.error(
                    "Gemini prompt blocked at request level. Block Reason: %s. Response ID: %s",
                    block_reason,
                    response_id,
                )
                msg = f"Gemini prompt blocked. Block Reason: {block_reason}. (Response ID: {response_id})"
                raise GeminiTranscriptionError(msg)

            logger.warning(
                "Gemini response returned no candidates. "
                "Response ID: %s, Headers: %s",
                response_id,
                headers,
            )
            msg = f"Gemini response returned no candidates. (Response ID: {response_id})"
            raise GeminiTransientTranscriptionError(msg)

        candidate = response.candidates[0]
        reason_str = (
            candidate.finish_reason.name
            if candidate.finish_reason is not None
            else None
        )

        blocked_ratings = self._get_blocked_ratings(candidate)
        if (
            candidate.finish_reason in _BLOCKED_FINISH_REASONS
            or blocked_ratings != "None"
        ):
            logger.warning(
                "Gemini response candidate was blocked by safety filters. "
                "Finish Reason: %s, Blocked Ratings: %s. Response ID: %s",
                reason_str,
                blocked_ratings,
                response_id,
            )
            msg = (
                f"Gemini response blocked by safety filters. "
                f"Finish Reason: {reason_str}, Blocked Ratings: {blocked_ratings}. (Response ID: {response_id})"
            )
            raise GeminiTranscriptionError(
                msg, finish_reason=candidate.finish_reason
            )

        if reason_str is None:
            # If the response is returned successfully but has no finish reason,
            # it means the model finished without producing any text content parts.
            # We treat this as a successful empty transcription rather than a transient failure.
            logger.info(
                "Gemini response candidate contains no finish reason (Response ID: %s). "
                "Treating as empty transcription.",
                response_id,
            )
            return ""

        if reason_str not in _VALID_FINISH_REASONS:
            finish_msg = candidate.finish_message or "No finish message"
            logger.warning(
                "Gemini response finished with invalid reason: %s. "
                "Finish Message: %s. Response ID: %s, Headers: %s",
                reason_str,
                finish_msg,
                response_id,
                headers,
            )
            msg = (
                f"Gemini response finished with invalid reason: {reason_str}. "
                f"Finish Message: {finish_msg}. (Response ID: {response_id})"
            )

            raise exceptions.InvalidFinishReasonError(msg)

        transcript = ""
        if candidate.content and candidate.content.parts:
            text_parts = [p.text for p in candidate.content.parts if p.text]
            if text_parts:
                transcript = "".join(text_parts).strip()

        if reason_str == types.FinishReason.MAX_TOKENS.name:
            logger.warning(
                "Gemini response reached MAX_TOKENS limit. Transcript is likely truncated. Response ID: %s",
                response_id,
            )
            raise exceptions.PartialTranscriptionError(
                partial_text=transcript,
                reason="MAX_TOKENS",
            )

        if not transcript:
            logger.info("Gemini returned empty content (finish reason: STOP).")

        return transcript
