"""Google Gemini transcriber implementation."""

import asyncio
import dataclasses
import mimetypes
import re
import secrets
import typing

import httpx
import pydantic
from google import genai
from google.genai import errors as genai_errors
from google.genai import types

from backend.pipeline.common import exceptions, log_helper, utils
from backend.pipeline.transcription import enums
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
DEFAULT_GEMINI_RETRY_ATTEMPTS = 3
DEFAULT_GEMINI_RETRY_INITIAL_DELAY = 1.0
DEFAULT_GEMINI_RETRY_MAX_DELAY = 60.0
DEFAULT_GEMINI_RETRY_MULTIPLIER = 2.0
DEFAULT_GEMINI_CLIENT_TIMEOUT_MS = 60000


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
    """Raised when Gemini transcription fails due to a potentially transient
    model/backend issue.
    """


class GeminiOtherFinishReasonError(GeminiTranscriptionError):
    """Raised on OTHER finish reason, indicating GCS/internal failure."""


class GeminiNoCandidatesError(GeminiTranscriptionError):
    """Raised when Gemini returns a response with zero candidates."""


@dataclasses.dataclass(frozen=True)
class SafetySetting:
    """Safety setting category and threshold."""

    category: str
    threshold: str


@dataclasses.dataclass(frozen=True, slots=True)
class _GeminiAttempt:
    """Facts identifying one application call to Gemini."""

    context: base.TranscriptionContext | None
    audio_uri: str | None
    model: str
    call_stage: typing.Literal["primary", "fallback"]
    attempt: int

    @property
    def is_tuned_primary(self) -> bool:
        """Return whether empty text is a retryable tuned-primary failure."""
        return self.call_stage == "primary" and _is_tuned_model(self.model)


def _is_tuned_model(model: str) -> bool:
    """Return whether the model uses a tuned-endpoint resource path."""
    return "/endpoints/" in model


def _response_text(response: types.GenerateContentResponse) -> str:
    """Return the same first-candidate text consumed by the transcriber."""
    if not response.candidates:
        return ""
    candidate = response.candidates[0]
    if not candidate.content or not candidate.content.parts:
        return ""
    return "".join(
        part.text for part in candidate.content.parts if part.text
    ).strip()


def _response_log_fields(
    response: types.GenerateContentResponse | None,
) -> dict[str, object | None]:
    """Extract JSON-safe facts already available on a Gemini response."""
    if response is None:
        return {
            "response_id": None,
            "candidate_count": None,
            "finish_reason": None,
            "response_text_length": None,
        }

    candidates = response.candidates or []
    finish_reason = None
    if candidates and candidates[0].finish_reason is not None:
        finish_reason = candidates[0].finish_reason.name
    response_id = response.response_id
    return {
        "response_id": str(response_id) if response_id is not None else None,
        "candidate_count": len(candidates),
        "finish_reason": finish_reason,
        "response_text_length": len(_response_text(response)),
    }


def _log_inference_attempt(
    attempt: _GeminiAttempt,
    response: types.GenerateContentResponse | None,
    error: Exception | None,
) -> None:
    """Emit best-effort structured facts for one relevant Gemini call."""
    try:
        error_code = (
            error.code if isinstance(error, genai_errors.APIError) else None
        )
        fields: dict[str, object | None] = {
            "event_type": "gemini_inference_attempt",
            "segment_id": (
                attempt.context.segment_id if attempt.context else None
            ),
            "audio_uri": attempt.audio_uri,
            "model": attempt.model,
            "call_stage": attempt.call_stage,
            "attempt": attempt.attempt,
            **_response_log_fields(response),
            "exception_type": (
                type(error).__name__ if error is not None else None
            ),
            "error_code": error_code,
            "error_message": str(error) if error is not None else None,
        }
        logger.debug(
            "Gemini inference attempt",
            extra={"json_fields": fields},
        )
    except Exception:
        logger.exception("Failed to emit Gemini inference attempt log")


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

    gemini_concurrency_limit: int = pydantic.Field(default=12, gt=0)

    fallback_model: str | None = DEFAULT_GEMINI_MODEL
    fallback_location: str = DEFAULT_GEMINI_LOCATION
    fallback_retry_attempts: int = 2


class GeminiTranscriber(base.Transcriber):
    """Transcriber implementation using Google GenAI SDK with Gemini 3.1."""

    def __init__(
        self,
        project_id: str,
        config: GeminiConfig,
        location: str | None = None,
        fallback_location: str | None = None,
    ) -> None:
        """Binds the GCP Project ID and parsed configuration."""
        self.project_id = project_id
        self.config = config
        self.client: genai.Client | None = None
        self.location = location or config.location
        self.fallback_location = fallback_location or config.fallback_location
        self._clients: dict[str, genai.Client] = {}
        self._concurrency_semaphore: asyncio.Semaphore | None = None

    def _resolve_location(self, model: str, default_location: str) -> str:
        """Resolves the location/region for a model.

        If the model is a full Vertex resource name containing a location path,
        extracts and returns that location; otherwise returns default_location.
        """
        match = re.search(r"/locations/([^/]+)/", model)
        if match:
            return match.group(1)
        return default_location

    def _get_retry_delay(self, attempt: int) -> float:
        """Calculates exponential backoff delay with jitter."""
        raw_delay = min(
            self.config.retry_max_delay,
            self.config.retry_initial_delay
            * (self.config.retry_multiplier ** (attempt - 1)),
        )
        jittered = raw_delay * secrets.SystemRandom().uniform(0.5, 1.5)
        return min(self.config.retry_max_delay, jittered)

    def _get_concurrency_semaphore(self) -> asyncio.Semaphore:
        """Gets or creates the in-flight request concurrency semaphore."""
        if self._concurrency_semaphore is None:
            self._concurrency_semaphore = asyncio.Semaphore(
                self.config.gemini_concurrency_limit
            )
        return self._concurrency_semaphore

    def _get_client(self, location: str) -> genai.Client:
        """Gets or creates a cached genai.Client for the given location."""
        if location not in self._clients:
            logger.info("Initializing genai.Client for location %s", location)
            self._clients[location] = genai.Client(
                enterprise=True,
                project=self.project_id,
                location=location,
                http_options=types.HttpOptions(
                    timeout=self.config.client_timeout_ms,
                    httpx_async_client=httpx.AsyncClient(
                        limits=httpx.Limits(
                            max_connections=500,
                            max_keepalive_connections=100,
                        ),
                    ),
                    retry_options=types.HttpRetryOptions(
                        attempts=self.config.retry_attempts,
                        initial_delay=self.config.retry_initial_delay,
                        max_delay=self.config.retry_max_delay,
                        exp_base=self.config.retry_multiplier,
                    ),
                ),
            )
        return self._clients[location]

    def setup(self) -> None:
        """Instantiates client caching and warms up the primary client."""
        self._clients.clear()
        self._concurrency_semaphore = asyncio.Semaphore(
            self.config.gemini_concurrency_limit
        )
        primary_location = self._resolve_location(
            self.config.model, self.location
        )
        self.client = self._get_client(primary_location)

    async def transcribe(
        self,
        *,
        audio_data: bytes | None = None,
        uri: str | None = None,
        duration_ms: int,
        context: base.TranscriptionContext | None = None,
    ) -> str | None:
        """Transcribes the audio payload using Gemini API.

        Accepts either raw bytes or a GCS URI reference.

        Args:
            audio_data: Optional raw audio bytes to transcribe.
            uri: Optional GCS URI (gs://...) of the audio file.
            duration_ms: Duration of the audio segment in milliseconds.
            context: Optional segment metadata for request diagnostics.

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

        # TODO(GOO-580): Support context
        # https://linear.app/watchduty/issue/GOO-580
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

        return await self._transcribe_tuned(
            contents,
            generation_config,
            context=context,
            audio_uri=uri,
        )

    async def _execute_transcribe_attempt(
        self,
        client: genai.Client,
        contents: types.Content,
        generation_config: types.GenerateContentConfig,
        *,
        attempt: _GeminiAttempt,
    ) -> tuple[str, types.GenerateContentResponse]:
        """Executes a single transcription attempt and parses results."""
        response = None
        try:
            async with self._get_concurrency_semaphore():
                response = await client.aio.models.generate_content(
                    model=attempt.model,
                    contents=contents,
                    config=generation_config,
                )
            transcript = self._parse_response(response)
        except Exception as error:
            if attempt.is_tuned_primary or attempt.call_stage == "fallback":
                _log_inference_attempt(attempt, response, error)
            raise

        if attempt.call_stage == "fallback":
            # Record every parsed fallback response, including blank responses.
            # _fallback_transcribe owns whether the response is accepted or
            # retried.
            _log_inference_attempt(attempt, response, None)
            return transcript, response

        # A foundation model used as the primary may legitimately return empty
        # text for silent audio. A tuned primary instead treats empty text as
        # transient.
        if transcript.strip() or not attempt.is_tuned_primary:
            return transcript, response

        # SFT model returned empty transcript: treat as transient.
        msg = "Tuned model returned empty transcript"
        error = GeminiTransientTranscriptionError(msg)
        _log_inference_attempt(attempt, response, error)
        raise error

    async def _transcribe_tuned(
        self,
        contents: types.Content,
        generation_config: types.GenerateContentConfig,
        *,
        context: base.TranscriptionContext | None,
        audio_uri: str | None,
    ) -> str:
        primary_location = self._resolve_location(
            self.config.model, self.location
        )
        primary_client = self._get_client(primary_location)

        # Tuned/SFT models reside in projects/.../endpoints/... resource paths.
        # Default foundation models use string names (e.g. gemini-3.1-flash).
        is_tuned_model = _is_tuned_model(self.config.model)

        # We only run in-situ retries for SFT models because they occasionally
        # fail to process valid audio and return empty responses or NONE.
        # Foundation models do not need these retry policies or fallbacks.
        attempts = self.config.retry_attempts if is_tuned_model else 1
        last_exception = None

        for attempt in range(1, attempts + 1):
            attempt_context = _GeminiAttempt(
                context=context,
                audio_uri=audio_uri,
                model=self.config.model,
                call_stage="primary",
                attempt=attempt,
            )
            try:
                transcript, _ = await self._execute_transcribe_attempt(
                    primary_client,
                    contents,
                    generation_config,
                    attempt=attempt_context,
                )
            except (
                GeminiTransientTranscriptionError,
                GeminiNoCandidatesError,
            ) as e:
                # Caught finish_reason=None, empty candidates, or SFT-empty:
                # these are transient model issues we want to retry.
                last_exception = e
            except GeminiOtherFinishReasonError as e:
                # OTHER represents a GCS/internal prediction gateway error.
                # We want to retry these against SFT (since they are
                # transient) but MUST NOT fall back to foundation model.
                last_exception = e
                # On the final attempt, raise to the worker so it can retry
                # the original model later, bypassing fallback.
                if attempt == attempts:
                    raise
            else:
                return transcript

            if attempt < attempts:
                await asyncio.sleep(self._get_retry_delay(attempt))

        # Fallback to foundation model on SFT-specific empty transcript /
        # NONE / no-candidates outcomes, whether the primary model is the
        # tuned SFT model or the foundation model itself.
        fallback_model = self.config.fallback_model or DEFAULT_GEMINI_MODEL
        is_fallback_eligible_error = isinstance(
            last_exception,
            (GeminiTransientTranscriptionError, GeminiNoCandidatesError),
        )
        if is_fallback_eligible_error and (
            is_tuned_model or self.config.model == fallback_model
        ):
            return await self._fallback_transcribe(
                contents,
                generation_config,
                str(last_exception),
                context=context,
                audio_uri=audio_uri,
            )

        if last_exception:
            # Raised outside except block, specify name to re-raise.
            raise last_exception
        return ""

    async def _fallback_transcribe(
        self,
        contents: types.Content,
        generation_config: types.GenerateContentConfig,
        reason: str,
        *,
        context: base.TranscriptionContext | None,
        audio_uri: str | None,
    ) -> str:
        """Falls back to foundation model if tuned model fails with empty/None.

        If the configured model is already the foundation model, or if the
        fallback call also fails with a transient empty response, returns an
        empty string ("") to prevent infinite retries.
        """
        if not self._clients:
            msg = "Client not initialized"
            raise RuntimeError(msg)

        fallback_model = self.config.fallback_model or DEFAULT_GEMINI_MODEL
        if self.config.model == fallback_model:
            # No distinct fallback model is available (the configured model
            # already is the fallback model). Record this separately from a
            # genuine fallback attempt so the dashboard doesn't conflate
            # "fell back to a different model" with "had nothing to fall
            # back to."
            log_helper.record_pipeline_stage(
                "transcription_status",
                enums.TranscriptionStatus.FALLBACK_UNAVAILABLE,
            )
            logger.info(
                "Model %s returned incomplete/empty response: %s. "
                "Model is already the fallback model. "
                "Treating as empty transcription.",
                self.config.model,
                reason,
            )
            return ""

        log_helper.record_pipeline_stage(
            "transcription_status", enums.TranscriptionStatus.FALLBACK
        )

        fallback_location = self._resolve_location(
            fallback_model, self.fallback_location
        )
        fallback_client = self._get_client(fallback_location)

        attempts = self.config.fallback_retry_attempts
        for attempt in range(1, attempts + 1):
            attempt_context = _GeminiAttempt(
                context=context,
                audio_uri=audio_uri,
                model=fallback_model,
                call_stage="fallback",
                attempt=attempt,
            )
            try:
                (
                    transcript,
                    fallback_response,
                ) = await self._execute_transcribe_attempt(
                    fallback_client,
                    contents,
                    generation_config,
                    attempt=attempt_context,
                )
                if transcript.strip():
                    return transcript

                resp_id = fallback_response.response_id or "Unknown"
                msg = (
                    "Empty transcript returned by fallback model. "
                    f"(Response ID: {resp_id})"
                )
                e = GeminiTransientTranscriptionError(msg)
            except (
                GeminiTransientTranscriptionError,
                GeminiNoCandidatesError,
            ) as exc:
                e = exc

            if attempt == attempts:
                logger.info(
                    "Fallback model %s also returned "
                    "incomplete/empty response: %s. "
                    "Treating as empty transcription.",
                    fallback_model,
                    e,
                )
                return ""
            logger.warning(
                "Fallback call to %s "
                "returned incomplete response (attempt %d/%d): %s. "
                "Retrying in 1s...",
                fallback_model,
                attempt,
                attempts,
                e,
            )
            await asyncio.sleep(self._get_retry_delay(attempt))

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
                    "Gemini prompt blocked at request level. "
                    "Block Reason: %s. Response ID: %s",
                    block_reason,
                    response_id,
                )
                msg = (
                    "Gemini prompt blocked. "
                    f"Block Reason: {block_reason}. "
                    f"(Response ID: {response_id})"
                )
                raise GeminiTranscriptionError(msg)

            logger.warning(
                "Gemini response returned no candidates. "
                "Response ID: %s, Headers: %s",
                response_id,
                headers,
            )
            msg = (
                "Gemini response returned no candidates. "
                f"(Response ID: {response_id})"
            )
            raise GeminiNoCandidatesError(msg)

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
                "Gemini response blocked by safety filters. "
                f"Finish Reason: {reason_str}, "
                f"Blocked Ratings: {blocked_ratings}. "
                f"(Response ID: {response_id})"
            )
            raise GeminiTranscriptionError(
                msg, finish_reason=candidate.finish_reason
            )

        if reason_str is None:
            msg = (
                "Incomplete response from Gemini (finish_reason: None). "
                f"(Response ID: {response_id})"
            )
            raise GeminiTransientTranscriptionError(msg)

        if reason_str == types.FinishReason.OTHER.name:
            finish_msg = candidate.finish_message or "No finish message"
            msg = (
                "Incomplete response from Gemini due to internal error "
                "(finish_reason: OTHER). "
                f"Finish Message: {finish_msg}. (Response ID: {response_id})"
            )
            raise GeminiOtherFinishReasonError(
                msg, finish_reason=candidate.finish_reason
            )

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

        transcript = _response_text(response)

        if reason_str == types.FinishReason.MAX_TOKENS.name:
            logger.warning(
                "Gemini response reached MAX_TOKENS limit. "
                "Transcript is likely truncated. Response ID: %s",
                response_id,
            )
            raise exceptions.PartialTranscriptionError(
                partial_text=transcript,
                reason="MAX_TOKENS",
            )

        if not transcript:
            logger.info("Gemini returned empty content (finish reason: STOP).")

        return transcript
