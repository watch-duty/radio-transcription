"""Provenance-safe context helpers for Gemini audio transcription."""

from __future__ import annotations

import collections
import collections.abc
import dataclasses
import math
import typing


@dataclasses.dataclass(frozen=True, slots=True)
class TrainingReferenceTurn:
    """One labeled prior transcript used only to build SFT examples.

    Attributes:
        text: Ground-truth transcript text for the prior training segment.
    """

    text: str


@dataclasses.dataclass(frozen=True, slots=True)
class PredictedHistoryTurn:
    """One model prediction authorized for evaluation history.

    Attributes:
        audio_uri: Audio URI whose model output supplied ``text``.
        text: Earlier model prediction supplied as evaluation context.
    """

    audio_uri: str
    text: str


@dataclasses.dataclass(frozen=True, slots=True)
class EvaluationSegment:
    """Reference-free segment metadata used to schedule evaluation.

    Attributes:
        audio_uri: Unique current-segment audio URI.
        split: Dataset split lineage.
        source_key: Source or conversation identity.
        start_seconds: Segment start in source-relative seconds.
        end_seconds: Segment end in source-relative seconds.
        manifest_index: Unique authoritative manifest position.
    """

    audio_uri: str
    split: str
    source_key: str
    start_seconds: float
    end_seconds: float
    manifest_index: int


@dataclasses.dataclass(frozen=True, slots=True)
class RollingHistoryScheduleRow:
    """One row in a strict-causal predicted-history execution schedule.

    Attributes:
        segment: Reference-free current segment metadata.
        dependency_audio_uris: Ordered prior segments whose own predictions
            may be supplied as history.
        wave: Zero-based causal execution wave.
    """

    segment: EvaluationSegment
    dependency_audio_uris: tuple[str, ...]
    wave: int


# Temporary stacked-PR bridge for latest-main callers. PR #1003 removes this
# block after switching evaluation to the prediction-only causal interface.
@dataclasses.dataclass(frozen=True)
class ContextTurn:
    """One previous transcript retained for the legacy context interface.

    Attributes:
        audio_uri: Source audio URI retained for provenance and identity.
        text: Transcript text supplied as prior context.
    """

    audio_uri: str
    text: str


def build_transcription_contents(
    *,
    audio_uri: str,
    user_prompt: str,
    history: collections.abc.Sequence[ContextTurn] | None = None,
    history_mode: str = "text_turns",
) -> list[dict[str, typing.Any]]:
    """Build contents for latest-main callers using generic context turns.

    Args:
        audio_uri: GCS URI for the current FLAC audio segment.
        user_prompt: Instruction for the current audio turn.
        history: Prior generic turns ordered oldest to newest.
        history_mode: One of the supported prior-context modes.

    Returns:
        Provider contents ending with the current user audio turn.
    """
    return _build_transcription_contents(
        audio_uri=audio_uri,
        user_prompt=user_prompt,
        history=list(history or ()),
        history_mode=history_mode,
    )


def build_context_histories(
    rows: list[dict[str, typing.Any]],
    *,
    max_turns: int,
) -> list[list[ContextTurn]]:
    """Build generic histories for latest-main evaluation callers.

    Args:
        rows: Canonical or compatibility manifest rows in caller order.
        max_turns: Maximum number of preceding turns retained per row.

    Returns:
        Generic histories aligned one-for-one with ``rows``.

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


# UP040: model supports Python 3.11.
_TranscriptTurn: typing.TypeAlias = (  # noqa: UP040
    TrainingReferenceTurn | PredictedHistoryTurn | ContextTurn
)

PRIOR_CONTEXT_MODES: typing.Final = frozenset(
    {"text_turns", "transcript", "guarded_transcript_block"}
)
_CAUSAL_BOUNDARY_TOLERANCE_SECONDS: typing.Final = 1e-7
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
    history: collections.abc.Sequence[_TranscriptTurn],
    user_prompt: str,
) -> str:
    """Prepend prior same-source transcripts to a user prompt.

    Args:
        history: Provenance-typed prior turns ordered oldest to newest.
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
    history: collections.abc.Sequence[_TranscriptTurn],
    user_prompt: str,
) -> str:
    """Build a guarded prior-transcript block plus the current prompt.

    Args:
        history: Provenance-typed prior turns ordered oldest to newest.
        user_prompt: Instruction for the current audio turn.

    Returns:
        A prompt that marks prior transcripts as context only. When history is
        empty, the guarded block contains an explicit no-history sentence.
    """
    if history:
        prior_context = "\n".join(
            f"{index}. {' '.join(turn.text.split())}"
            for index, turn in enumerate(history, 1)
        )
    else:
        prior_context = GUARDED_TRANSCRIPT_NO_HISTORY_TEXT
    return (
        f"{GUARDED_TRANSCRIPT_CONTEXT_HEADER}\n{prior_context}\n\n{user_prompt}"
    )


def build_training_transcription_contents(
    *,
    audio_uri: str,
    user_prompt: str,
    history: collections.abc.Sequence[TrainingReferenceTurn] | None = None,
    history_mode: str = "text_turns",
) -> list[dict[str, typing.Any]]:
    """Build SFT contents from explicitly labeled reference turns.

    Args:
        audio_uri: GCS URI for the current FLAC training segment.
        user_prompt: Instruction for the current audio turn.
        history: Prior ground-truth turns used only for supervised training.
        history_mode: One of the modes in ``PRIOR_CONTEXT_MODES``.

    Returns:
        Gemini SFT contents ending with the current user audio turn.

    Raises:
        TypeError: If a turn is not a ``TrainingReferenceTurn``.
        ValueError: If ``history_mode`` is unsupported.
    """
    history_turns = _validated_training_history(history)
    return _build_transcription_contents(
        audio_uri=audio_uri,
        user_prompt=user_prompt,
        history=history_turns,
        history_mode=history_mode,
    )


def build_evaluation_transcription_contents(
    *,
    audio_uri: str,
    user_prompt: str,
    history: collections.abc.Sequence[PredictedHistoryTurn] | None = None,
    history_mode: str = "text_turns",
) -> list[dict[str, typing.Any]]:
    """Build evaluation contents from model predictions only.

    Args:
        audio_uri: GCS URI for the current FLAC evaluation segment.
        user_prompt: Instruction for the current audio turn.
        history: Earlier model predictions ordered oldest to newest.
        history_mode: One of the modes in ``PRIOR_CONTEXT_MODES``.

    Returns:
        Evaluation contents containing exactly the current segment's audio.

    Raises:
        TypeError: If history contains a training reference or unknown turn.
        ValueError: If ``history_mode`` is unsupported or the rendered request
            does not contain exactly the current audio part.
    """
    history_turns = _validated_evaluation_history(history)
    contents = _build_transcription_contents(
        audio_uri=audio_uri,
        user_prompt=user_prompt,
        history=history_turns,
        history_mode=history_mode,
    )
    _require_exactly_current_audio(contents, audio_uri=audio_uri)
    return contents


def _build_transcription_contents(
    *,
    audio_uri: str,
    user_prompt: str,
    history: collections.abc.Sequence[_TranscriptTurn],
    history_mode: str,
) -> list[dict[str, typing.Any]]:
    """Build one transcription request with text-only prior context.

    Args:
        audio_uri: GCS URI for the current audio segment.
        user_prompt: Instruction applied to the current transcription turn.
        history: Prior transcript turns ordered oldest to newest.
        history_mode: Representation used to encode ``history``.

    Returns:
        Provider contents ending with exactly one current-audio part.

    Raises:
        ValueError: If ``history_mode`` is unsupported.
    """
    history_mode = validate_history_mode(history_mode)
    contents: list[dict[str, typing.Any]] = []
    if history_mode == "text_turns":
        for turn in history:
            contents.extend(
                [
                    {
                        "role": "user",
                        "parts": [{"text": user_prompt}],
                    },
                    {"role": "model", "parts": [{"text": turn.text}]},
                ]
            )
        current_user_prompt = user_prompt
    elif history_mode == "transcript":
        current_user_prompt = build_transcript_context_prompt(
            history,
            user_prompt,
        )
    else:
        current_user_prompt = build_guarded_transcript_context_prompt(
            history,
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


def validate_evaluation_context_contract(
    prior_context_count: int,
    history_mode: str,
) -> str:
    """Validate the evaluation window and its request-shape contract.

    Args:
        prior_context_count: Maximum structural prediction-history window.
        history_mode: Configured representation for prediction history.

    Returns:
        The normalized history mode.

    Raises:
        TypeError: If the context count is not an integer.
        ValueError: If the count is negative or the mode is unsupported.
    """
    if isinstance(prior_context_count, bool) or not isinstance(
        prior_context_count, int
    ):
        msg = "prior_context_count must be an integer"
        raise TypeError(msg)
    if prior_context_count < 0:
        msg = "prior_context_count must be non-negative"
        raise ValueError(msg)
    return validate_history_mode(history_mode)


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


def build_training_reference_histories(
    rows: list[dict[str, typing.Any]],
    *,
    max_turns: int,
) -> list[list[TrainingReferenceTurn]]:
    """Build labeled prior-reference histories for SFT preparation only.

    Rows are grouped by source recording/session and sorted by source offset.
    A row enters later training histories only when its labeled transcript is
    non-empty and is not the case-insensitive ``[UNINTELLIGIBLE]`` sentinel.

    Args:
        rows: Canonical training manifest rows in caller order.
        max_turns: Maximum number of preceding labeled turns per row.

    Returns:
        Training-reference histories aligned one-for-one with ``rows``.

    Raises:
        ValueError: If ``max_turns`` is negative.
    """
    if max_turns < 0:
        msg = "max_turns must be non-negative"
        raise ValueError(msg)
    histories: list[list[TrainingReferenceTurn]] = [[] for _ in rows]
    if max_turns == 0 or not rows:
        return histories

    grouped_indices: dict[str, list[int]] = collections.defaultdict(list)
    for index, row in enumerate(rows):
        grouped_indices[_episode_key(row, index)].append(index)

    for indices in grouped_indices.values():
        history: list[TrainingReferenceTurn] = []
        for index in sorted(indices, key=lambda i: _row_sort_key(rows[i], i)):
            histories[index] = list(history[-max_turns:])
            row = rows[index]
            text = str(row.get("text") or "").strip()
            audio_uri = str(row.get("audio_filepath") or "").strip()
            if audio_uri and _usable_history_text(text):
                history.append(TrainingReferenceTurn(text=text))
    return histories


def build_strict_causal_schedule(
    segments: collections.abc.Sequence[EvaluationSegment],
    *,
    max_turns: int,
) -> tuple[RollingHistoryScheduleRow, ...]:
    """Build a deterministic reference-free rolling-history schedule.

    Eligible dependencies share split and source, start strictly before the
    current segment, and end no later than the current start within floating-
    point boundary tolerance. Eligible rows are ordered by end, start, and
    audio URI; the most recent ``max_turns`` are retained. Returned rows follow
    authoritative ``manifest_index`` order.

    Args:
        segments: Reference-free evaluation metadata.
        max_turns: Maximum structural history slots per request.

    Returns:
        Immutable schedule rows with causal dependency waves.

    Raises:
        TypeError: If a segment or ``max_turns`` has an invalid type.
        ValueError: If identifiers, timing, or manifest indices are invalid.
    """
    if isinstance(max_turns, bool) or not isinstance(max_turns, int):
        msg = "max_turns must be an integer"
        raise TypeError(msg)
    if max_turns < 0:
        msg = "max_turns must be non-negative"
        raise ValueError(msg)
    segment_values = tuple(segments)
    _validate_evaluation_segments(segment_values)

    grouped: dict[tuple[str, str], list[EvaluationSegment]] = (
        collections.defaultdict(list)
    )
    for segment in segment_values:
        grouped[(segment.split, segment.source_key)].append(segment)
    for source_segments in grouped.values():
        source_segments.sort(key=_history_sort_key)

    dependency_map: dict[str, tuple[str, ...]] = {}
    wave_by_audio_uri: dict[str, int] = {}
    for current in sorted(segment_values, key=_execution_sort_key):
        candidates = [
            prior
            for prior in grouped[(current.split, current.source_key)]
            if prior.audio_uri != current.audio_uri
            and prior.start_seconds < current.start_seconds
            and prior.end_seconds
            <= current.start_seconds + _CAUSAL_BOUNDARY_TOLERANCE_SECONDS
        ]
        selected = candidates[-max_turns:] if max_turns else []
        dependencies = tuple(prior.audio_uri for prior in selected)
        dependency_map[current.audio_uri] = dependencies
        wave_by_audio_uri[current.audio_uri] = (
            0
            if not dependencies
            else 1
            + max(wave_by_audio_uri[audio_uri] for audio_uri in dependencies)
        )

    return tuple(
        RollingHistoryScheduleRow(
            segment=segment,
            dependency_audio_uris=dependency_map[segment.audio_uri],
            wave=wave_by_audio_uri[segment.audio_uri],
        )
        for segment in sorted(
            segment_values,
            key=lambda value: value.manifest_index,
        )
    )


def _validated_training_history(
    history: collections.abc.Sequence[TrainingReferenceTurn] | None,
) -> list[TrainingReferenceTurn]:
    """Validate and copy an optional training-reference history.

    Args:
        history: Training-reference turns to validate, or ``None``.

    Returns:
        A mutable copy containing only training-reference turns.

    Raises:
        TypeError: If any turn is not a ``TrainingReferenceTurn``.
    """
    turns = list(history or ())
    if any(not isinstance(turn, TrainingReferenceTurn) for turn in turns):
        msg = "training history must contain only TrainingReferenceTurn values"
        raise TypeError(msg)
    return turns


def _validated_evaluation_history(
    history: collections.abc.Sequence[PredictedHistoryTurn] | None,
) -> list[PredictedHistoryTurn]:
    """Validate and copy an optional prediction-only evaluation history.

    Args:
        history: Predicted-history turns to validate, or ``None``.

    Returns:
        A mutable copy containing only predicted-history turns.

    Raises:
        TypeError: If any turn is not a ``PredictedHistoryTurn``.
    """
    turns = list(history or ())
    if any(isinstance(turn, TrainingReferenceTurn) for turn in turns):
        msg = "evaluation history must not contain TrainingReferenceTurn values"
        raise TypeError(msg)
    if any(not isinstance(turn, PredictedHistoryTurn) for turn in turns):
        msg = "evaluation history must contain only PredictedHistoryTurn values"
        raise TypeError(msg)
    return turns


def _require_exactly_current_audio(
    contents: collections.abc.Sequence[dict[str, typing.Any]],
    *,
    audio_uri: str,
) -> None:
    """Enforce the one-current-audio evaluation boundary.

    Args:
        contents: Rendered Gemini conversation contents to inspect.
        audio_uri: Current segment URI that must be the sole audio part.

    Raises:
        ValueError: If audio is missing, duplicated, or belongs to another
            segment.
    """
    rendered_audio_uris: list[typing.Any] = []
    for turn in contents:
        parts = turn.get("parts")
        if not isinstance(parts, list):
            continue
        for part in parts:
            if not isinstance(part, dict):
                continue
            file_data = part.get("fileData")
            if isinstance(file_data, dict):
                rendered_audio_uris.append(file_data.get("fileUri"))
    if rendered_audio_uris != [audio_uri]:
        msg = "evaluation contents must contain exactly the current audio part"
        raise ValueError(msg)


def _validate_evaluation_segments(
    segments: collections.abc.Sequence[EvaluationSegment],
) -> None:
    """Validate identifiers and timing used for causal evaluation scheduling.

    Args:
        segments: Transcript-free evaluation descriptors to validate.

    Raises:
        TypeError: If any value is not an ``EvaluationSegment``.
        ValueError: If identifiers or manifest indices are invalid or
            duplicated, or if timing is non-finite or reversed.
    """
    audio_uris: set[str] = set()
    manifest_indices: set[int] = set()
    for segment in segments:
        if not isinstance(segment, EvaluationSegment):
            msg = "segments must contain only EvaluationSegment values"
            raise TypeError(msg)
        if (
            not isinstance(segment.audio_uri, str)
            or not segment.audio_uri.strip()
            or not isinstance(segment.split, str)
            or not segment.split.strip()
            or not isinstance(segment.source_key, str)
            or not segment.source_key.strip()
        ):
            msg = "evaluation segment identifiers must be non-empty strings"
            raise ValueError(msg)
        if segment.audio_uri in audio_uris:
            msg = "evaluation segment audio_uri must be unique"
            raise ValueError(msg)
        audio_uris.add(segment.audio_uri)
        if (
            isinstance(segment.manifest_index, bool)
            or not isinstance(segment.manifest_index, int)
            or segment.manifest_index < 0
        ):
            msg = "evaluation segment manifest_index must be non-negative"
            raise ValueError(msg)
        if segment.manifest_index in manifest_indices:
            msg = "evaluation segment manifest_index must be unique"
            raise ValueError(msg)
        manifest_indices.add(segment.manifest_index)
        if not _finite_number(segment.start_seconds) or not _finite_number(
            segment.end_seconds
        ):
            msg = "evaluation segment times must be finite numbers"
            raise ValueError(msg)
        if segment.end_seconds < segment.start_seconds:
            msg = "evaluation segment end must not precede start"
            raise ValueError(msg)


def _history_sort_key(
    segment: EvaluationSegment,
) -> tuple[float, float, str]:
    return (segment.end_seconds, segment.start_seconds, segment.audio_uri)


def _execution_sort_key(
    segment: EvaluationSegment,
) -> tuple[float, float, str]:
    return (segment.start_seconds, segment.end_seconds, segment.audio_uri)


def _finite_number(value: typing.Any) -> bool:
    return (
        not isinstance(value, bool)
        and isinstance(value, (int, float))
        and math.isfinite(float(value))
    )


def _episode_key(row: dict[str, typing.Any], fallback_index: int) -> str:
    """Return the best available same-source episode key for a training row.

    Args:
        row: Canonical training manifest row.
        fallback_index: Caller-order index used when provenance is absent.

    Returns:
        A stable source identifier or a row-specific fallback key.
    """
    value = row.get("original_audio_uri")
    if isinstance(value, str) and value.strip():
        return value.strip()
    source_audio = row.get("source_audio")
    if isinstance(source_audio, dict):
        value = source_audio.get("audio_filepath")
        if isinstance(value, str) and value.strip():
            return value.strip()
    for key in ("audio_uri", "example_id", "audio_filepath"):
        value = row.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return f"__missing_episode_key__:{fallback_index}"


def _row_sort_key(
    row: dict[str, typing.Any], fallback_index: int
) -> tuple[float, int, str]:
    """Return a deterministic source-order key for one training row.

    Args:
        row: Canonical training manifest row.
        fallback_index: Caller-order index used for absent source ordering.

    Returns:
        Source offset, within-source order, and audio URI.
    """
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
    normalized = text.strip()
    return bool(normalized) and normalized.upper() != "[UNINTELLIGIBLE]"


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
