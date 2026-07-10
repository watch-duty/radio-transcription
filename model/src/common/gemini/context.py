"""Rolling prior-turn context helpers for Gemini audio transcription."""

from __future__ import annotations

import collections
import collections.abc
import dataclasses
import math
import typing


@dataclasses.dataclass(frozen=True)
class ContextTurn:
    """One previous transcript retained for evaluation context.

    Attributes:
        audio_uri: Source audio URI retained for provenance and identity.
        text: Transcript text supplied as prior context.
    """

    audio_uri: str
    text: str


PRIOR_CONTEXT_MODES: typing.Final = frozenset(
    {"text_turns", "transcript", "guarded_transcript_block"}
)
GUARDED_TRANSCRIPT_CONTEXT_HEADER = (
    "The following prior same-source transcripts are for situational "
    "awareness only.\n"
    "Do not re-transcribe them. Do not continue them.\n"
    "Transcribe exclusively the current audio clip.\n"
    "\n"
    "Prior transcripts, oldest to newest:"
)
GUARDED_TRANSCRIPT_NO_HISTORY_TEXT = (
    "There are no prior transcripts for this original recording."
)


def build_transcript_context_prompt(
    history: collections.abc.Sequence[ContextTurn],
    user_prompt: str,
) -> str:
    """Prepend prior same-source transcripts to a user prompt.

    Args:
        history: Prior turns ordered from oldest to newest.
        user_prompt: Instruction for the current audio turn.

    Returns:
        The original prompt when history is empty; otherwise, a prompt with a
        numbered prior-transcript preamble.
    """
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


def build_guarded_transcript_context_prompt(
    history: collections.abc.Sequence[ContextTurn],
    user_prompt: str,
) -> str:
    """Build a guarded prior-transcript block plus the current prompt.

    Args:
        history: Prior turns ordered from oldest to newest.
        user_prompt: Instruction for the current audio turn.

    Returns:
        A prompt that marks prior transcripts as context only. When history is
        empty, the guarded block contains an explicit no-history sentence.
    """
    prior_context = (
        "\n".join(
            f"{index}. {' '.join(turn.text.split())}"
            for index, turn in enumerate(history, 1)
        )
        if history
        else GUARDED_TRANSCRIPT_NO_HISTORY_TEXT
    )
    return (
        f"{GUARDED_TRANSCRIPT_CONTEXT_HEADER}\n{prior_context}\n\n{user_prompt}"
    )


def build_prior_text_user_turn(user_prompt: str) -> str:
    """Return the text-only prior user turn.

    This intentionally reuses the current-turn prompt exactly. For the prior
    context SFT run that prompt is the manual-context notebook TURN_PROMPT; the
    only omitted prior-turn part is the audio.

    Args:
        user_prompt: Instruction reused for a prior text-only user turn.

    Returns:
        ``user_prompt`` unchanged.
    """
    return user_prompt


def build_transcription_contents(
    *,
    audio_uri: str,
    user_prompt: str,
    history: collections.abc.Sequence[ContextTurn] | None = None,
    history_mode: str = "text_turns",
) -> list[dict[str, typing.Any]]:
    """Build Gemini contents for prior context and the current audio turn.

    Args:
        audio_uri: GCS URI for the current FLAC audio segment.
        user_prompt: Instruction for the current audio turn.
        history: Prior turns ordered from oldest to newest.
        history_mode: One of the modes in ``PRIOR_CONTEXT_MODES``.

    Returns:
        Gemini content dictionaries ending with the current user audio turn.

    Raises:
        ValueError: If ``history_mode`` is unsupported.
    """
    history_mode = validate_history_mode(history_mode)
    contents: list[dict[str, typing.Any]] = []
    history_turns = list(history or ())
    if history_mode == "text_turns":
        for turn in history_turns:
            contents.extend(
                [
                    {
                        "role": "user",
                        "parts": [
                            {"text": build_prior_text_user_turn(user_prompt)}
                        ],
                    },
                    {"role": "model", "parts": [{"text": turn.text}]},
                ]
            )
        current_user_prompt = user_prompt
    elif history_mode == "transcript":
        current_user_prompt = build_transcript_context_prompt(
            history_turns,
            user_prompt,
        )
    else:
        current_user_prompt = build_guarded_transcript_context_prompt(
            history_turns,
            user_prompt,
        )
    contents.append(
        {
            "role": "user",
            "parts": [
                {"text": current_user_prompt},
                audio_file_data_part(audio_uri),
            ],
        }
    )
    return contents


def validate_history_mode(history_mode: str) -> str:
    """Normalize and validate a prior-context encoding mode.

    Args:
        history_mode: Candidate mode name.

    Returns:
        The stripped, lowercase mode name.

    Raises:
        ValueError: If the normalized name is not in ``PRIOR_CONTEXT_MODES``.
    """
    mode = history_mode.strip().lower()
    if mode not in PRIOR_CONTEXT_MODES:
        msg = (
            "history_mode must be 'text_turns', 'transcript', or "
            "'guarded_transcript_block'"
        )
        raise ValueError(msg)
    return mode


def audio_file_data_part(audio_uri: str) -> dict[str, typing.Any]:
    """Build a Gemini audio file-data part in canonical camelCase JSON.

    Args:
        audio_uri: GCS URI for a FLAC audio segment.

    Returns:
        A ``fileData`` part with ``audio/flac`` MIME type and ``audio_uri``.
    """
    return {
        "fileData": {
            "mimeType": "audio/flac",
            "fileUri": audio_uri,
        }
    }


def build_context_histories(
    rows: list[dict[str, typing.Any]],
    *,
    max_turns: int,
) -> list[list[ContextTurn]]:
    """Return up to ``max_turns`` previous same-source turns for each row.

    Rows are grouped by source recording/session and sorted by source offset.
    Histories are returned in the same order as ``rows``. A row enters future
    histories only when its transcript is non-empty and not the explicit
    ``[UNINTELLIGIBLE]`` sentinel, matching the manual-context notebook.

    Args:
        rows: Canonical or compatibility manifest rows in caller order.
        max_turns: Maximum number of preceding turns retained per row.

    Returns:
        Histories aligned one-for-one with ``rows``, each ordered oldest to
        newest.

    Raises:
        ValueError: If ``max_turns`` is negative.
    """
    if max_turns < 0:
        msg = "max_turns must be non-negative"
        raise ValueError(msg)
    histories: list[list[ContextTurn]] = [[] for _ in rows]
    if max_turns == 0 or not rows:
        return histories

    grouped_indices: dict[str, list[int]] = collections.defaultdict(list)
    for index, row in enumerate(rows):
        grouped_indices[_episode_key(row, index)].append(index)

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


def _episode_key(row: dict[str, typing.Any], fallback_index: int) -> str:
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
        "example_id",
        "audio_filepath",
    ):
        value = row.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return f"__missing_episode_key__:{fallback_index}"


def _row_sort_key(
    row: dict[str, typing.Any], fallback_index: int
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


def _numeric_value(value: typing.Any) -> float | None:
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


def _int_value(value: typing.Any) -> int | None:
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
