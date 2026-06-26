"""Rolling prior-turn context helpers for Gemini audio transcription."""

from __future__ import annotations

import math
from collections import defaultdict
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class ContextTurn:
    """One previous audio/transcript pair supplied as model context."""

    audio_uri: str
    text: str


def build_context_histories(
    rows: list[dict[str, Any]],
    *,
    max_turns: int,
) -> list[list[ContextTurn]]:
    """Return up to ``max_turns`` previous same-source turns for each row.

    Rows are grouped by source recording/session and sorted by source offset.
    Histories are returned in the same order as ``rows``. A row enters future
    histories only when its transcript is non-empty and not the explicit
    ``[UNINTELLIGIBLE]`` sentinel, matching the manual-context notebook.
    """
    if max_turns < 0:
        msg = "max_turns must be non-negative"
        raise ValueError(msg)
    histories: list[list[ContextTurn]] = [[] for _ in rows]
    if max_turns == 0 or not rows:
        return histories

    grouped_indices: dict[str, list[int]] = defaultdict(list)
    for index, row in enumerate(rows):
        grouped_indices[_episode_key(row)].append(index)

    for indices in grouped_indices.values():
        history: list[ContextTurn] = []
        for index in sorted(indices, key=lambda i: _row_sort_key(rows[i], i)):
            histories[index] = list(history[-max_turns:])
            row = rows[index]
            text = str(row.get("text") or "").strip()
            audio_uri = str(row.get("audio_filepath") or "").strip()
            if audio_uri and _usable_history_text(text):
                history.append(ContextTurn(audio_uri=audio_uri, text=text))
    return histories


def _episode_key(row: dict[str, Any]) -> str:
    value = row.get("original_audio_uri")
    if isinstance(value, str) and value.strip():
        return value.strip()
    source_audio = row.get("source_audio")
    if isinstance(source_audio, dict):
        value = source_audio.get("audio_filepath")
        if isinstance(value, str) and value.strip():
            return value.strip()
    for key in (
        "audio_uri",
        "source_group",
        "example_id",
        "audio_filepath",
    ):
        value = row.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return ""


def _row_sort_key(
    row: dict[str, Any], fallback_index: int
) -> tuple[float, int, str]:
    offset = _numeric_value(row.get("original_offset"))
    if offset is None:
        source_audio = row.get("source_audio")
        if isinstance(source_audio, dict):
            offset = _numeric_value(source_audio.get("offset"))
    if offset is None:
        offset = _numeric_value(row.get("offset"))
    if offset is None:
        offset = float(fallback_index)

    order = _int_value(row.get("row_index"))
    if order is None:
        order = _int_value(row.get("segment_id"))
    if order is None:
        order = fallback_index

    audio_uri = str(row.get("audio_filepath") or "")
    return (offset, order, audio_uri)


def _usable_history_text(text: str) -> bool:
    return bool(text) and text.strip() != "[UNINTELLIGIBLE]"


def _numeric_value(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)) and math.isfinite(float(value)):
        return float(value)
    if isinstance(value, str):
        try:
            parsed = float(value)
        except ValueError:
            return None
        if math.isfinite(parsed):
            return parsed
    return None


def _int_value(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            return None
    return None
