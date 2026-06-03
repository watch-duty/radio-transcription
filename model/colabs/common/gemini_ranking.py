"""Context-aware Gemini prediction helpers for Phase 2 ranking."""

from __future__ import annotations

import collections.abc

from common import prompts
from common import ranking
from common import vertex


DEFAULT_FULL_MODEL = "gemini-3.5-flash"
DEFAULT_PREFLIGHT_MODEL = "gemini-3.1-pro-preview"


try:
    from google import genai
    from google.genai import types
except ImportError as _e:
    _VERTEX_MISSING = _e
    genai = None
    types = None
else:
    _VERTEX_MISSING = None


def build_contextual_contents(
    current_row: collections.abc.Mapping[str, object],
    context_entries: collections.abc.Iterable[ranking.PredictionCacheEntry],
    *,
    user_prompt: str = prompts.GEMINI_TRANSCRIBE_USER_PROMPT,
) -> list[dict[str, object]]:
    """Build Gemini contents with prior audio/model prediction turns.

    Args:
        current_row: Phase 1 enriched review row for the current audio.
        context_entries: Ordered prior prediction cache entries from the same
            source group.
        user_prompt: User prompt text for the current row.

    Returns:
        Gemini `contents` list. Prior turns use prediction text only; reference
        transcript text from rows is never included.
    """
    contents: list[dict[str, object]] = []
    for entry in context_entries:
        contents.append(
            {
                "role": "user",
                "parts": [
                    {
                        "file_data": {
                            "file_uri": entry.model_ready_audio_uri,
                            "mime_type": "audio/flac",
                        }
                    }
                ],
            }
        )
        contents.append(
            {
                "role": "model",
                "parts": [{"text": entry.prediction_text}],
            }
        )

    contents.append(
        {
            "role": "user",
            "parts": [
                {"text": user_prompt},
                {
                    "file_data": {
                        "file_uri": current_row["model_ready_audio_uri"],
                        "mime_type": "audio/flac",
                    }
                },
            ],
        }
    )
    return contents


def extract_response_text(response: object) -> str:
    """Extract stripped response text, returning empty string for no text.

    Args:
        response: Object returned by `models.generate_content`.

    Returns:
        Stripped response text or `""`.
    """
    text = getattr(response, "text", "") or ""
    return str(text).strip()


class GeminiRankingRunner:
    """Thin wrapper for context-aware Gemini prediction calls.

    Attributes:
        client: Gemini client or fake with `models.generate_content`.
        model_id: Gemini model ID used for predictions.
        system_prompt: System prompt text used for transcription.
        user_prompt: User prompt text for current audio turns.
        generation_config: Generation config copied from common Vertex defaults.
        safety_settings: Safety settings copied from common Vertex defaults.
    """

    def __init__(
        self,
        client: object,
        *,
        model_id: str = DEFAULT_FULL_MODEL,
        system_prompt: str = prompts.GEMINI_TRANSCRIBE_SYSTEM_PROMPT,
        user_prompt: str = prompts.GEMINI_TRANSCRIBE_USER_PROMPT,
        generation_config: collections.abc.Mapping[str, object] | None = None,
        safety_settings: collections.abc.Sequence[object] | None = None,
    ) -> None:
        self.client = client
        self.model_id = model_id
        self.system_prompt = system_prompt
        self.user_prompt = user_prompt
        if generation_config is None:
            generation_config = vertex.GEMINI_GENERATION_CONFIG
        if safety_settings is None:
            safety_settings = vertex.GEMINI_SAFETY_SETTINGS
        self.generation_config = dict(generation_config)
        self.safety_settings = list(safety_settings)

    def predict_one(
        self,
        row: collections.abc.Mapping[str, object],
        context_entries: collections.abc.Iterable[ranking.PredictionCacheEntry],
    ) -> str:
        """Generate one prediction for a row with same-source context.

        Args:
            row: Phase 1 enriched review row.
            context_entries: Ordered prior same-source prediction entries.

        Returns:
            Stripped Gemini prediction text, or `""` for empty/blocked output.
        """
        if types is None:
            _require_vertex()
        contents = build_contextual_contents(
            row,
            context_entries,
            user_prompt=self.user_prompt,
        )
        config = types.GenerateContentConfig(
            system_instruction=self.system_prompt.strip(),
            temperature=self.generation_config["temperature"],
            max_output_tokens=self.generation_config["max_output_tokens"],
            safety_settings=self.safety_settings,
        )
        response = self.client.models.generate_content(
            model=self.model_id,
            contents=contents,
            config=config,
        )
        return extract_response_text(response)


def new_vertex_client(project: str, location: str) -> object:
    """Return a Vertex-backed Gemini client.

    Args:
        project: Google Cloud project ID.
        location: Vertex AI location.

    Returns:
        `google.genai.Client` configured for Vertex AI.

    Raises:
        ImportError: If the `[vertex]` extra is not installed.
    """
    _require_vertex()
    return genai.Client(vertexai=True, project=project, location=location)


def run_source_group_predictions(
    source_rows: collections.abc.Iterable[collections.abc.Mapping[str, object]],
    cache_by_audio_id: collections.abc.Mapping[
        str,
        ranking.PredictionCacheEntry,
    ],
    runner: GeminiRankingRunner,
    *,
    prompt_fp: str,
    context_policy_fp: str,
    num_recent_events: int = ranking.NUM_RECENT_EVENTS,
    created_at: str,
    on_new_entry: (
        collections.abc.Callable[[ranking.PredictionCacheEntry], None] | None
    ) = None,
) -> tuple[
    list[ranking.PredictionCacheEntry],
    dict[str, ranking.PredictionCacheEntry],
]:
    """Generate or reuse predictions for rows while preserving context order.

    Args:
        source_rows: Review rows to process. Rows are grouped by source group
            and ordered by row index before processing.
        cache_by_audio_id: Existing prediction cache keyed by audio ID.
        runner: Gemini runner used for cache misses.
        prompt_fp: Active prompt fingerprint.
        context_policy_fp: Active context policy fingerprint.
        num_recent_events: Active context event cap.
        created_at: Timestamp/run ID to store on newly generated entries.

    Returns:
        `(new_entries, final_cache_by_audio_id)`.
    """
    row_list = [dict(row) for row in source_rows]
    grouped_rows = ranking.ordered_source_rows(row_list)
    final_cache = dict(cache_by_audio_id)
    compatible_cache: dict[str, ranking.PredictionCacheEntry] = {}
    new_entries: list[ranking.PredictionCacheEntry] = []
    max_context_rows = num_recent_events // 2
    model_id = str(getattr(runner, "model_id", DEFAULT_FULL_MODEL))

    for source_group in sorted(grouped_rows):
        for row in grouped_rows[source_group]:
            audio_id = str(row["audio_segment_id"])
            context_entries = ranking.previous_prediction_context(
                grouped_rows,
                row,
                compatible_cache,
                max_context_rows=max_context_rows,
            )
            context_fp = ranking.context_fingerprint(context_entries)
            entry = final_cache.get(audio_id)
            if entry is not None and ranking.is_cache_entry_compatible(
                entry,
                model_id=model_id,
                prompt_fp=prompt_fp,
                context_policy_fp=context_policy_fp,
                num_recent_events=num_recent_events,
                context_fp=context_fp,
            ):
                compatible_cache[audio_id] = entry
                continue

            try:
                prediction_text = runner.predict_one(row, context_entries)
            except Exception as exc:
                prediction_text = ""
                error = _prediction_error(exc)
            else:
                error = "" if prediction_text.strip() else "empty_prediction"
            entry = ranking.PredictionCacheEntry(
                audio_segment_id=audio_id,
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
            final_cache[audio_id] = entry
            new_entries.append(entry)
            if on_new_entry is not None:
                on_new_entry(entry)
            if not error:
                compatible_cache[audio_id] = entry

    return new_entries, final_cache


def _prediction_error(exc: Exception) -> str:
    message = f"{exc.__class__.__name__}: {exc}"
    return message[:1000]


def _require_vertex() -> None:
    """Raise a clear error if the `[vertex]` extra is not installed."""
    if _VERTEX_MISSING:
        raise ImportError(
            "gemini_ranking requires the [vertex] extra: "
            "pip install 'common[vertex]'"
        ) from _VERTEX_MISSING
