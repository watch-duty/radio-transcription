"""ADK-backed Gemini prediction helpers for review ranking."""

from __future__ import annotations

import asyncio
import collections.abc
import dataclasses
import inspect
import uuid

from common import gemini_config, prompts, ranking

DEFAULT_FULL_MODEL = "gemini-3.5-flash"
DEFAULT_PREFLIGHT_MODEL = "gemini-3.1-flash-lite"
DEFAULT_MAX_ATTEMPTS = 5
DEFAULT_MAX_EMPTY_ATTEMPTS = 2
APP_NAME = "radio_transcription_review_ranking"
USER_ID = "review-ranking"
AGENT_NAME = "review_ranking_agent"

try:
    from google.adk.agents import LlmAgent
    from google.adk.agents.run_config import RunConfig
    from google.adk.models import Gemini
    from google.adk.runners import Runner
    from google.adk.sessions import InMemorySessionService
    from google.adk.sessions.base_session_service import GetSessionConfig
    from google.genai import types
except ImportError as _e:
    _ADK_MISSING = _e
    LlmAgent = None
    RunConfig = None
    Gemini = None
    Runner = None
    InMemorySessionService = None
    GetSessionConfig = None
    types = None
else:
    _ADK_MISSING = None


@dataclasses.dataclass(frozen=True)
class AdkSessionHandle:
    """Run-local ADK session identity."""

    source_group: str
    session_id: str
    session: object


class AdkRankingRunner:
    """Thin wrapper around an ADK root LLM agent for one-row predictions."""

    def __init__(
        self,
        *,
        project: str,
        location: str,
        model_id: str = DEFAULT_FULL_MODEL,
        system_prompt: str = prompts.GEMINI_TRANSCRIBE_SYSTEM_PROMPT,
        user_prompt: str = prompts.GEMINI_TRANSCRIBE_USER_PROMPT,
        request_timeout_ms: int | None = None,
    ) -> None:
        _require_adk()
        self.project = project
        self.location = location
        self.model_id = model_id
        self.system_prompt = system_prompt
        self.user_prompt = user_prompt
        self.request_timeout_ms = request_timeout_ms
        self.agent_name = AGENT_NAME
        self.app_name = APP_NAME
        self.user_id = USER_ID
        self._loop = asyncio.new_event_loop()
        self._closed = False
        self._session_service = InMemorySessionService()
        model = Gemini(
            model=_vertex_model_resource(project, location, model_id)
        )
        agent_kwargs = {
            "name": self.agent_name,
            "model": model,
            "instruction": system_prompt.strip(),
            "mode": "chat",
            "tools": [],
        }
        config = _generation_config()
        if "generate_content_config" in inspect.signature(LlmAgent).parameters:
            agent_kwargs["generate_content_config"] = config
        self._agent = LlmAgent(**agent_kwargs)
        self._runner = Runner(
            app_name=self.app_name,
            agent=self._agent,
            session_service=self._session_service,
        )
        self._run_config = RunConfig(
            max_llm_calls=1,
            get_session_config=GetSessionConfig(
                num_recent_events=ranking.NUM_RECENT_EVENTS,
            ),
        )

    def start_session(self, source_group: str) -> AdkSessionHandle:
        """Create a new in-memory ADK session for one source group."""
        session_id = f"{_safe_session_part(source_group)}-{uuid.uuid4().hex}"
        session = self._run_async(
            self._session_service.create_session(
                app_name=self.app_name,
                user_id=self.user_id,
                session_id=session_id,
            )
        )
        return AdkSessionHandle(
            source_group=source_group,
            session_id=session_id,
            session=session,
        )

    def append_success(
        self,
        session: AdkSessionHandle,
        row: collections.abc.Mapping[str, object],
        entry: ranking.PredictionCacheEntry,
    ) -> None:
        """Replay one successful audio/prediction pair into an ADK session."""
        user_event = _event(
            author=self.user_id,
            content=_audio_content(
                str(row["model_ready_audio_uri"]),
                text=prompts.ADK_PRIOR_AUDIO_WRAPPER,
            ),
        )
        model_event = _event(
            author=self.agent_name,
            content=_text_content(
                entry.prediction_text,
                prefix=prompts.ADK_PRIOR_PREDICTION_WRAPPER,
            ),
        )
        _append_event(
            self._session_service,
            session.session,
            user_event,
            run_async=self._run_async,
        )
        _append_event(
            self._session_service,
            session.session,
            model_event,
            run_async=self._run_async,
        )

    def predict_one(
        self,
        session: AdkSessionHandle,
        row: collections.abc.Mapping[str, object],
    ) -> str:
        """Generate one prediction through ADK and return final text."""
        message = _audio_content(
            str(row["model_ready_audio_uri"]),
            text=(f"{prompts.ADK_CURRENT_AUDIO_WRAPPER}\n{self.user_prompt}"),
        )
        events = self._run_events(session.session_id, message)
        return extract_final_response_text(events)

    def _run_events(self, session_id: str, message: object) -> list[object]:
        async def collect() -> list[object]:
            stream = self._runner.run_async(
                user_id=self.user_id,
                session_id=session_id,
                new_message=message,
                run_config=self._run_config,
            )
            if self.request_timeout_ms is None:
                return [event async for event in stream]
            try:
                return await asyncio.wait_for(
                    _collect_async_iterable(stream),
                    timeout=self.request_timeout_ms / 1000,
                )
            except asyncio.TimeoutError:
                await _close_async_iterable(stream)
                raise

        return self._run_async(collect())

    def close(self) -> None:
        """Close the runner-owned event loop after ADK work is complete."""
        self._closed = True
        loop = getattr(self, "_loop", None)
        if loop is None or loop.is_closed():
            return
        loop.run_until_complete(loop.shutdown_asyncgens())
        loop.run_until_complete(loop.shutdown_default_executor())
        loop.close()

    def _run_async(self, awaitable: object) -> object:
        if getattr(self, "_closed", False):
            _close_unawaited(awaitable)
            raise RuntimeError("ADK ranking runner is closed")
        loop = getattr(self, "_loop", None)
        if loop is None:
            loop = asyncio.new_event_loop()
            self._loop = loop
        return _run_async_on_loop(loop, awaitable)


def run_source_group_predictions_adk(
    source_rows: collections.abc.Iterable[collections.abc.Mapping[str, object]],
    cache_entries: collections.abc.Iterable[ranking.PredictionCacheEntry],
    runner: object,
    *,
    prompt_fp: str,
    context_policy_fp: str,
    num_recent_events: int = ranking.NUM_RECENT_EVENTS,
    created_at: str,
    on_new_entry: (
        collections.abc.Callable[[ranking.PredictionCacheEntry], None] | None
    ) = None,
    retry_errors: bool = False,
    max_attempts: int = DEFAULT_MAX_ATTEMPTS,
    max_empty_attempts: int = DEFAULT_MAX_EMPTY_ATTEMPTS,
) -> tuple[
    list[ranking.PredictionCacheEntry],
    dict[str, ranking.PredictionCacheEntry],
]:
    """Generate or reuse predictions while preserving source-group order."""
    row_list = [dict(row) for row in source_rows]
    grouped_rows = ranking.ordered_source_rows(row_list)
    history = ranking.cache_history_by_audio_id(cache_entries)
    final_cache: dict[str, ranking.PredictionCacheEntry] = {}
    compatible_successes: dict[str, ranking.PredictionCacheEntry] = {}
    new_entries: list[ranking.PredictionCacheEntry] = []
    max_context_rows = min(ranking.MAX_CONTEXT_ROWS, num_recent_events // 2)
    model_id = str(getattr(runner, "model_id", DEFAULT_FULL_MODEL))

    for source_group in sorted(grouped_rows):
        session = runner.start_session(source_group)
        accepted_entries: list[
            tuple[dict[str, object], ranking.PredictionCacheEntry]
        ] = []
        for row in grouped_rows[source_group]:
            audio_id = str(row["audio_segment_id"])
            context_entries = ranking.previous_prediction_context(
                grouped_rows,
                row,
                compatible_successes,
                max_context_rows=max_context_rows,
            )
            context_fp = ranking.context_fingerprint(context_entries)
            entry = ranking.select_latest_compatible_cache_entry(
                history.get(audio_id, []),
                row,
                model_id=model_id,
                prompt_fp=prompt_fp,
                context_policy_fp=context_policy_fp,
                num_recent_events=num_recent_events,
                context_fp=context_fp,
                include_failure=not retry_errors,
            )
            if entry is not None:
                final_cache[audio_id] = entry
                if _entry_successful(entry):
                    compatible_successes[audio_id] = entry
                    runner.append_success(session, row, entry)
                    accepted_entries.append((row, entry))
                continue

            entry, session = _predict_with_retries(
                runner,
                session,
                accepted_entries,
                row,
                model_id=model_id,
                prompt_fp=prompt_fp,
                context_policy_fp=context_policy_fp,
                num_recent_events=num_recent_events,
                context_fp=context_fp,
                created_at=created_at,
                max_attempts=max_attempts,
                max_empty_attempts=max_empty_attempts,
            )
            history.setdefault(audio_id, []).append(entry)
            final_cache[audio_id] = entry
            new_entries.append(entry)
            if on_new_entry is not None:
                on_new_entry(entry)
            if _entry_successful(entry):
                compatible_successes[audio_id] = entry
                accepted_entries.append((row, entry))
    return new_entries, final_cache


def extract_final_response_text(
    events: collections.abc.Iterable[object],
) -> str:
    """Extract stripped text from final-response ADK events."""
    text_parts: list[str] = []
    for event in events:
        is_final = getattr(event, "is_final_response", None)
        if callable(is_final) and not is_final():
            continue
        content = getattr(event, "content", None)
        if content is None:
            continue
        for part in getattr(content, "parts", []) or []:
            text = getattr(part, "text", None)
            if text:
                text_parts.append(str(text).strip())
    return " ".join(text_parts).strip()


def _predict_with_retries(
    runner: object,
    session: AdkSessionHandle,
    accepted_entries: list[
        tuple[dict[str, object], ranking.PredictionCacheEntry]
    ],
    row: collections.abc.Mapping[str, object],
    *,
    model_id: str,
    prompt_fp: str,
    context_policy_fp: str,
    num_recent_events: int,
    context_fp: str,
    created_at: str,
    max_attempts: int,
    max_empty_attempts: int,
) -> tuple[ranking.PredictionCacheEntry, AdkSessionHandle]:
    error_attempts = 0
    empty_attempts = 0
    last_error = ""
    max_context_rows = min(ranking.MAX_CONTEXT_ROWS, num_recent_events // 2)
    while True:
        try:
            prediction_text = str(runner.predict_one(session, row)).strip()
        except Exception as exc:
            error_attempts += 1
            last_error = _prediction_error(exc)
            if error_attempts >= max_attempts:
                session = _recreate_session(
                    runner,
                    session.source_group,
                    accepted_entries,
                    max_context_rows=max_context_rows,
                )
                return (
                    _cache_entry(
                        row,
                        prediction_text="",
                        model_id=model_id,
                        prompt_fp=prompt_fp,
                        context_policy_fp=context_policy_fp,
                        num_recent_events=num_recent_events,
                        context_fp=context_fp,
                        created_at=created_at,
                        error=last_error,
                    ),
                    session,
                )
            session = _recreate_session(
                runner,
                session.source_group,
                accepted_entries,
                max_context_rows=max_context_rows,
            )
            continue

        if prediction_text:
            return (
                _cache_entry(
                    row,
                    prediction_text=prediction_text,
                    model_id=model_id,
                    prompt_fp=prompt_fp,
                    context_policy_fp=context_policy_fp,
                    num_recent_events=num_recent_events,
                    context_fp=context_fp,
                    created_at=created_at,
                    error="",
                ),
                session,
            )

        empty_attempts += 1
        if empty_attempts >= max_empty_attempts:
            return (
                _cache_entry(
                    row,
                    prediction_text="",
                    model_id=model_id,
                    prompt_fp=prompt_fp,
                    context_policy_fp=context_policy_fp,
                    num_recent_events=num_recent_events,
                    context_fp=context_fp,
                    created_at=created_at,
                    error="",
                ),
                session,
            )
        session = _recreate_session(
            runner,
            session.source_group,
            accepted_entries,
            max_context_rows=max_context_rows,
        )


def _recreate_session(
    runner: object,
    source_group: str,
    accepted_entries: list[
        tuple[dict[str, object], ranking.PredictionCacheEntry]
    ],
    *,
    max_context_rows: int,
) -> AdkSessionHandle:
    session = runner.start_session(source_group)
    replay_entries = (
        accepted_entries[-max_context_rows:] if max_context_rows > 0 else []
    )
    for prior_row, prior_entry in replay_entries:
        runner.append_success(session, prior_row, prior_entry)
    return session


def _cache_entry(
    row: collections.abc.Mapping[str, object],
    *,
    prediction_text: str,
    model_id: str,
    prompt_fp: str,
    context_policy_fp: str,
    num_recent_events: int,
    context_fp: str,
    created_at: str,
    error: str,
) -> ranking.PredictionCacheEntry:
    return ranking.PredictionCacheEntry(
        audio_segment_id=str(row["audio_segment_id"]),
        prediction_text=prediction_text,
        model_id=model_id,
        prompt_fingerprint=prompt_fp,
        context_policy_fingerprint=context_policy_fp,
        num_recent_events=num_recent_events,
        context_fingerprint=context_fp,
        source_group=str(row["source_group"]),
        row_index=int(row["row_index"]),
        model_ready_audio_uri=str(row["model_ready_audio_uri"]),
        created_at=created_at,
        error=error,
    )


async def _collect_async_iterable(stream: object) -> list[object]:
    return [event async for event in stream]


async def _close_async_iterable(stream: object) -> None:
    close = getattr(stream, "aclose", None)
    if close is None:
        return
    result = close()
    if inspect.isawaitable(result):
        await result


def _audio_content(file_uri: str, *, text: str) -> object:
    parts = [types.Part(text=text)]
    if hasattr(types.Part, "from_uri"):
        parts.append(
            types.Part.from_uri(file_uri=file_uri, mime_type="audio/flac")
        )
    else:
        parts.append(
            types.Part(
                file_data=types.FileData(
                    file_uri=file_uri,
                    mime_type="audio/flac",
                )
            )
        )
    return types.Content(role="user", parts=parts)


def _text_content(text: str, *, prefix: str) -> object:
    content_text = f"{prefix}\n{text}"
    return types.Content(role="model", parts=[types.Part(text=content_text)])


def _event(*, author: str, content: object) -> object:
    try:
        from google.adk.events import Event
    except ImportError as exc:
        raise ImportError("google-adk Event type is unavailable") from exc
    return Event(author=author, content=content)


def _append_event(
    session_service: object,
    session: object,
    event: object,
    *,
    run_async: collections.abc.Callable[[object], object] | None = None,
) -> None:
    result = session_service.append_event(session=session, event=event)
    if inspect.isawaitable(result):
        runner = run_async or _run_async
        runner(result)


def _generation_config() -> object:
    return types.GenerateContentConfig(
        temperature=gemini_config.GEMINI_GENERATION_CONFIG["temperature"],
        max_output_tokens=gemini_config.GEMINI_GENERATION_CONFIG[
            "max_output_tokens"
        ],
        safety_settings=gemini_config.GEMINI_SAFETY_SETTINGS,
        thinking_config=types.ThinkingConfig(
            thinking_level=types.ThinkingLevel[
                gemini_config.GEMINI_THINKING_LEVEL
            ],
        ),
        http_options=types.HttpOptions(
            retry_options=types.HttpRetryOptions(
                **gemini_config.GEMINI_HTTP_RETRY_CONFIG,
            ),
        ),
    )


def _vertex_model_resource(project: str, location: str, model_id: str) -> str:
    if model_id.startswith("projects/"):
        return model_id
    return (
        f"projects/{project}/locations/{location}/publishers/google/models/"
        f"{model_id}"
    )


def _safe_session_part(value: str) -> str:
    safe = "".join(ch if ch.isalnum() else "-" for ch in value.lower())
    return safe.strip("-") or "source"


def _prediction_error(exc: Exception) -> str:
    message = f"{exc.__class__.__name__}: {exc}"
    return message[:1000]


def _entry_successful(entry: ranking.PredictionCacheEntry) -> bool:
    return not str(entry.error or "").strip()


def _run_async(awaitable: object) -> object:
    loop = asyncio.new_event_loop()
    try:
        return _run_async_on_loop(loop, awaitable, close=True)
    except Exception:
        if not loop.is_closed():
            loop.close()
        raise


def _run_async_on_loop(
    loop: asyncio.AbstractEventLoop,
    awaitable: object,
    *,
    close: bool = False,
) -> object:
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        try:
            return loop.run_until_complete(awaitable)
        finally:
            if close:
                loop.run_until_complete(loop.shutdown_asyncgens())
                loop.run_until_complete(loop.shutdown_default_executor())
                loop.close()
    _close_unawaited(awaitable)
    raise RuntimeError(
        "ADK ranking runner cannot run inside an active event loop"
    )


def _close_unawaited(awaitable: object) -> None:
    close = getattr(awaitable, "close", None)
    if close is not None:
        close()


def _require_adk() -> None:
    if _ADK_MISSING:
        raise ImportError(
            "adk_ranking requires the [adk] extra: pip install 'common[adk]'"
        ) from _ADK_MISSING
