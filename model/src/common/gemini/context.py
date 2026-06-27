"""Rolling prior-turn context helpers for Gemini audio transcription."""

from __future__ import annotations

import math
from collections import defaultdict
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class ContextTurn:
    """One previous audio/transcript pair supplied as model context."""

    audio_uri: str
    text: str


VAPO_P3_CONTEXT_HEADER = "\n".join(
    [
        "The following prior same-source transcripts are for situational "
        "awareness only.",
        "Do not re-transcribe them. Do not continue them.",
        "Transcribe exclusively the current audio clip.",
        "",
        "Prior transcripts, oldest to newest:",
    ]
)
VAPO_P3_NO_HISTORY_TEXT = (
    "There are no prior transcripts for this original recording."
)


def build_transcript_context_prompt(
    history: Sequence[ContextTurn],
    user_prompt: str,
) -> str:
    """Return a user prompt with prior same-source transcripts as text context."""
    if not history:
        return user_prompt
    lines = [
        "Prior same-source transcripts for situational awareness only. "
        "Do not re-transcribe them; transcribe only the current audio clip.",
        "Prior same-source transcripts, oldest to newest:",
    ]
    lines.extend(
        f"{index}. {turn.text}" for index, turn in enumerate(history, 1)
    )
    lines.extend(["", user_prompt])
    return "\n".join(lines)


def build_vapo_p3_transcript_context_prompt(
    history: Sequence[ContextTurn],
    user_prompt: str,
) -> str:
    """Return the exact transcript-block prompt shape used by the P3/P13 VAPO gate."""
    prior_context = (
        "\n".join(
            f"{index}. {' '.join(turn.text.split())}"
            for index, turn in enumerate(history, 1)
        )
        if history
        else VAPO_P3_NO_HISTORY_TEXT
    )
    return "\n\n".join(
        [
            "\n".join([VAPO_P3_CONTEXT_HEADER, prior_context]),
            user_prompt,
        ]
    )


def build_prior_text_user_turn(user_prompt: str) -> str:
    """Return the text-only prior user turn.

    This intentionally reuses the current-turn prompt exactly. For the prior
    context SFT run that prompt is the manual-context notebook TURN_PROMPT; the
    only omitted prior-turn part is the audio.
    """
    return user_prompt


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
