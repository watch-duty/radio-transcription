"""Provenance-safe context helpers for Gemini audio transcription."""

from __future__ import annotations

import bisect
import collections
import collections.abc
import dataclasses
import heapq
import itertools
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
_DUPLICATE_SPAN_SAMPLE_LIMIT: typing.Final = 5
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


@dataclasses.dataclass(frozen=True, slots=True)
class _DuplicateSpanPair:
    """One canonicalized invalid same-source interval pair.

    Attributes:
        first: Lower-manifest-index member of the pair.
        second: Higher-manifest-index member of the pair.
        relationship: Whether the intervals are equal or one contains the
            other within boundary tolerance.
    """

    first: EvaluationSegment
    second: EvaluationSegment
    relationship: typing.Literal["equality", "containment"]


class _FenwickCounter:
    """Count compressed end coordinates with logarithmic updates and queries."""

    def __init__(self, size: int) -> None:
        self._tree = [0] * (size + 1)

    def add(self, index: int, delta: int) -> None:
        """Add one count delta at a zero-based compressed coordinate.

        Args:
            index: Zero-based coordinate to update.
            delta: Signed count change applied at ``index``.
        """
        tree_index = index + 1
        while tree_index < len(self._tree):
            self._tree[tree_index] += delta
            tree_index += tree_index & -tree_index

    def prefix_count(self, stop: int) -> int:
        """Return the count below one exclusive compressed-coordinate bound.

        Args:
            stop: Exclusive zero-based coordinate bound.

        Returns:
            Sum of all stored counts at coordinates lower than ``stop``.
        """
        total = 0
        tree_index = stop
        while tree_index:
            total += self._tree[tree_index]
            tree_index -= tree_index & -tree_index
        return total


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
    rows: collections.abc.Sequence[dict[str, typing.Any]],
    *,
    schedule: collections.abc.Sequence[RollingHistoryScheduleRow],
) -> list[list[TrainingReferenceTurn]]:
    """Resolve training-only references for frozen causal dependencies.

    Args:
        rows: Canonical training manifest rows in authoritative order.
        schedule: Transcript-free dependency rows compiled for ``rows``.

    Returns:
        Training-reference histories aligned one-for-one with ``rows``.

    Raises:
        ValueError: If row identity, uniqueness, or schedule alignment is
            invalid.
    """
    row_values = tuple(rows)
    schedule_values = tuple(schedule)
    if len(row_values) != len(schedule_values):
        msg = "training rows and causal schedule must have equal lengths"
        raise ValueError(msg)

    text_by_audio_uri: dict[str, str] = {}
    audio_uri_by_index: list[str] = []
    for row in row_values:
        audio_uri_value = row.get("audio_filepath")
        if not isinstance(audio_uri_value, str) or not (
            audio_uri := audio_uri_value.strip()
        ):
            msg = "training row audio_filepath must be a non-empty string"
            raise ValueError(msg)
        if audio_uri in text_by_audio_uri:
            msg = "training row audio_filepath must be unique"
            raise ValueError(msg)
        text_by_audio_uri[audio_uri] = str(row.get("text") or "").strip()
        audio_uri_by_index.append(audio_uri)

    histories: list[list[TrainingReferenceTurn]] = [[] for _ in row_values]
    seen_indices: set[int] = set()
    for schedule_row in schedule_values:
        index = schedule_row.segment.manifest_index
        if (
            index < 0
            or index >= len(row_values)
            or index in seen_indices
            or audio_uri_by_index[index] != schedule_row.segment.audio_uri
        ):
            msg = "training rows and causal schedule have invalid alignment"
            raise ValueError(msg)
        seen_indices.add(index)
        history: list[TrainingReferenceTurn] = []
        for dependency_audio_uri in schedule_row.dependency_audio_uris:
            if dependency_audio_uri not in text_by_audio_uri:
                msg = (
                    "training causal dependency is absent from rows: "
                    f"{dependency_audio_uri}"
                )
                raise ValueError(msg)
            text = text_by_audio_uri[dependency_audio_uri]
            if _usable_history_text(text):
                history.append(TrainingReferenceTurn(text=text))
        histories[index] = history
    if len(seen_indices) != len(row_values):
        msg = "training rows and causal schedule have invalid alignment"
        raise ValueError(msg)
    return histories


def build_strict_causal_schedule(
    segments: collections.abc.Sequence[EvaluationSegment],
    *,
    max_turns: int,
) -> tuple[RollingHistoryScheduleRow, ...]:
    """Build a deterministic reference-free rolling-history schedule.

    Eligible dependencies share split and source, start before the current
    segment outside floating-point boundary tolerance, and end no later than
    the current start within that tolerance. Eligible rows are ordered by end,
    start, and audio URI; the most recent ``max_turns`` are retained. Returned
    rows follow authoritative ``manifest_index`` order.

    Args:
        segments: Reference-free evaluation metadata.
        max_turns: Maximum structural history slots per request.

    Returns:
        Immutable schedule rows with causal dependency waves.

    Raises:
        TypeError: If a segment or ``max_turns`` has an invalid type.
        ValueError: If identifiers, timing, or manifest indices are invalid,
            or positive-context rows in one split and source are equal or
            contained spans.
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

    if max_turns > 0:
        _validate_no_duplicate_source_spans(grouped)

    dependency_map: dict[str, tuple[str, ...]] = {}
    wave_by_audio_uri: dict[str, int] = {}
    for group_key in sorted(grouped):
        source_dependencies, source_waves = _build_source_dependencies(
            grouped[group_key],
            max_turns=max_turns,
        )
        dependency_map.update(source_dependencies)
        wave_by_audio_uri.update(source_waves)

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


def _span_contains(
    container: EvaluationSegment,
    contained: EvaluationSegment,
) -> bool:
    """Return whether one interval contains another within tolerance.

    Args:
        container: Candidate outer segment.
        contained: Candidate inner segment.

    Returns:
        ``True`` when both boundaries satisfy tolerant containment.
    """
    tolerance = _CAUSAL_BOUNDARY_TOLERANCE_SECONDS
    return (
        container.start_seconds <= contained.start_seconds + tolerance
        and container.end_seconds >= contained.end_seconds - tolerance
    )


def _duplicate_span_pair(
    first: EvaluationSegment,
    second: EvaluationSegment,
) -> _DuplicateSpanPair:
    """Canonicalize and classify one already-invalid interval pair.

    Args:
        first: One member of an equality or containment pair.
        second: The other member of the pair.

    Returns:
        Pair ordered by manifest identity and classified by span relationship.
    """
    ordered = sorted(
        (first, second),
        key=lambda value: (value.manifest_index, value.audio_uri),
    )
    relationship: typing.Literal["equality", "containment"] = (
        "equality"
        if _span_contains(first, second) and _span_contains(second, first)
        else "containment"
    )
    return _DuplicateSpanPair(
        first=ordered[0],
        second=ordered[1],
        relationship=relationship,
    )


def _duplicate_pair_key(
    pair: _DuplicateSpanPair,
) -> tuple[int, str, int, str]:
    """Return the stable identity used to de-duplicate diagnostic samples.

    Args:
        pair: Canonicalized duplicate-span pair.

    Returns:
        Both manifest indices and audio URIs in canonical pair order.
    """
    return (
        pair.first.manifest_index,
        pair.first.audio_uri,
        pair.second.manifest_index,
        pair.second.audio_uri,
    )


def _append_duplicate_span_sample(
    samples: list[_DuplicateSpanPair],
    sample_keys: set[tuple[int, str, int, str]],
    first: EvaluationSegment,
    second: EvaluationSegment,
    *,
    sample_limit: int,
) -> None:
    """Append one canonical pair when diagnostic capacity remains.

    Args:
        samples: Mutable ordered diagnostic samples.
        sample_keys: Mutable identities already represented in ``samples``.
        first: One member of the candidate pair.
        second: The other member of the candidate pair.
        sample_limit: Maximum number of samples to retain.
    """
    if len(samples) >= sample_limit:
        return
    pair = _duplicate_span_pair(first, second)
    key = _duplicate_pair_key(pair)
    if key not in sample_keys:
        sample_keys.add(key)
        samples.append(pair)


def _scan_duplicate_source_spans(
    source_segments: collections.abc.Sequence[EvaluationSegment],
    *,
    sample_limit: int,
) -> tuple[int, tuple[_DuplicateSpanPair, ...]]:
    """Count invalid pairs and retain bounded witnesses for one source.

    The sweep uses compressed end coordinates to count prior-containing,
    current-containing, and equal pairs without enumerating every pair.
    Equality is subtracted once because it appears in both containment counts.

    Args:
        source_segments: Segments from one split and normalized source.
        sample_limit: Maximum number of deterministic pair witnesses to retain.

    Returns:
        Exact invalid-pair count and up to ``sample_limit`` diagnostic pairs.
    """
    ordered = sorted(
        source_segments,
        key=lambda value: (
            value.start_seconds,
            value.end_seconds,
            value.manifest_index,
            value.audio_uri,
        ),
    )
    end_values = sorted({value.end_seconds for value in ordered})
    end_indices = {value: index for index, value in enumerate(end_values)}
    all_prior_ends = _FenwickCounter(len(end_values))
    near_start_ends = _FenwickCounter(len(end_values))
    near_start_segments: collections.deque[EvaluationSegment] = (
        collections.deque()
    )
    near_active: set[tuple[int, str]] = set()
    max_end_heap: list[tuple[float, int, str, EvaluationSegment]] = []
    near_min_end_heap: list[tuple[float, int, str, EvaluationSegment]] = []
    samples: list[_DuplicateSpanPair] = []
    sample_keys: set[tuple[int, str, int, str]] = set()
    invalid_pair_count = 0
    tolerance = _CAUSAL_BOUNDARY_TOLERANCE_SECONDS

    for prior_count, current in enumerate(ordered):
        while (
            near_start_segments
            and near_start_segments[0].start_seconds + tolerance
            < current.start_seconds
        ):
            expired = near_start_segments.popleft()
            near_start_ends.add(end_indices[expired.end_seconds], -1)
            near_active.remove((expired.manifest_index, expired.audio_uri))
        while near_min_end_heap:
            candidate = near_min_end_heap[0][3]
            candidate_key = (
                candidate.manifest_index,
                candidate.audio_uri,
            )
            if candidate_key in near_active:
                break
            heapq.heappop(near_min_end_heap)

        lower_end = current.end_seconds - tolerance
        upper_end = current.end_seconds + tolerance
        lower_index = bisect.bisect_left(end_values, lower_end)
        upper_index = bisect.bisect_right(end_values, upper_end)
        prior_contains_current = prior_count - all_prior_ends.prefix_count(
            lower_index
        )
        current_contains_prior = near_start_ends.prefix_count(upper_index)
        equality_count = near_start_ends.prefix_count(
            upper_index
        ) - near_start_ends.prefix_count(lower_index)
        invalid_pair_count += (
            prior_contains_current + current_contains_prior - equality_count
        )

        if prior_contains_current:
            _append_duplicate_span_sample(
                samples,
                sample_keys,
                max_end_heap[0][3],
                current,
                sample_limit=sample_limit,
            )
        if current_contains_prior:
            _append_duplicate_span_sample(
                samples,
                sample_keys,
                near_min_end_heap[0][3],
                current,
                sample_limit=sample_limit,
            )

        end_index = end_indices[current.end_seconds]
        all_prior_ends.add(end_index, 1)
        near_start_ends.add(end_index, 1)
        near_start_segments.append(current)
        near_active.add((current.manifest_index, current.audio_uri))
        heapq.heappush(
            max_end_heap,
            (
                -current.end_seconds,
                current.manifest_index,
                current.audio_uri,
                current,
            ),
        )
        heapq.heappush(
            near_min_end_heap,
            (
                current.end_seconds,
                current.manifest_index,
                current.audio_uri,
                current,
            ),
        )

    return invalid_pair_count, tuple(samples)


def _format_duplicate_span_pair(pair: _DuplicateSpanPair) -> str:
    """Format one duplicate pair for a stable operator diagnostic.

    Args:
        pair: Canonicalized pair to serialize.

    Returns:
        Named relationship, population, identity, and interval fields.
    """
    first = pair.first
    second = pair.second
    return (
        f"relationship={pair.relationship}, split={first.split!r}, "
        f"source_key={first.source_key!r}, "
        "manifest_indices="
        f"({first.manifest_index}, {second.manifest_index}), "
        f"audio_uris=({first.audio_uri!r}, {second.audio_uri!r}), "
        "intervals=("
        f"[{first.start_seconds}, {first.end_seconds}), "
        f"[{second.start_seconds}, {second.end_seconds}))"
    )


def _validate_no_duplicate_source_spans(
    grouped: collections.abc.Mapping[
        tuple[str, str],
        collections.abc.Sequence[EvaluationSegment],
    ],
) -> None:
    """Reject equal or contained intervals across contextual populations.

    Args:
        grouped: Segments grouped by ``(split, source_key)``.

    Raises:
        ValueError: If any group contains equal or contained intervals.
    """
    total_invalid_pairs = 0
    samples: list[_DuplicateSpanPair] = []
    for group_key in sorted(grouped):
        invalid_count, group_samples = _scan_duplicate_source_spans(
            grouped[group_key],
            sample_limit=_DUPLICATE_SPAN_SAMPLE_LIMIT - len(samples),
        )
        total_invalid_pairs += invalid_count
        samples.extend(group_samples)
    if total_invalid_pairs:
        sample_text = "; ".join(
            _format_duplicate_span_pair(pair) for pair in samples
        )
        msg = (
            "same-source duplicate spans detected: "
            f"total_invalid_pairs={total_invalid_pairs}; "
            f"sample=[{sample_text}]"
        )
        raise ValueError(msg)


def _build_source_dependencies(
    source_segments: collections.abc.Sequence[EvaluationSegment],
    *,
    max_turns: int,
) -> tuple[
    dict[str, tuple[str, ...]],
    dict[str, int],
]:
    """Build bounded causal dependencies and waves for one source.

    Segments become eligible only after their start is strictly earlier than
    the current start outside tolerance and their end is no later than the
    current start within tolerance. A heap tracks newly completed segments,
    while a sorted list retains only the latest structural ``max_turns``.

    Args:
        source_segments: Segments from one split and normalized source.
        max_turns: Maximum number of completed dependencies per segment.

    Returns:
        Audio-URI dependency tuples and causal wave indices keyed by audio URI.
    """
    dependency_map: dict[str, tuple[str, ...]] = {}
    wave_by_audio_uri: dict[str, int] = {}
    ordered = sorted(source_segments, key=_execution_sort_key)
    if max_turns == 0:
        for segment in ordered:
            dependency_map[segment.audio_uri] = ()
            wave_by_audio_uri[segment.audio_uri] = 0
        return dependency_map, wave_by_audio_uri

    pending: list[tuple[float, float, str, EvaluationSegment]] = []
    recent_completed: list[tuple[float, float, str, EvaluationSegment]] = []
    activation_index = 0
    tolerance = _CAUSAL_BOUNDARY_TOLERANCE_SECONDS
    for current_start, batch_values in itertools.groupby(
        ordered,
        key=lambda value: value.start_seconds,
    ):
        batch = list(batch_values)
        while activation_index < len(ordered):
            candidate = ordered[activation_index]
            if candidate.start_seconds >= current_start - tolerance:
                break
            heapq.heappush(
                pending,
                (
                    candidate.end_seconds,
                    candidate.start_seconds,
                    candidate.audio_uri,
                    candidate,
                ),
            )
            activation_index += 1
        while pending and pending[0][0] <= current_start + tolerance:
            completed_item = heapq.heappop(pending)
            bisect.insort(recent_completed, completed_item)
            if len(recent_completed) > max_turns:
                del recent_completed[:-max_turns]

        dependencies = tuple(
            audio_uri for _, _, audio_uri, _ in recent_completed
        )
        wave = (
            0
            if not dependencies
            else 1
            + max(wave_by_audio_uri[audio_uri] for audio_uri in dependencies)
        )
        for current in batch:
            dependency_map[current.audio_uri] = dependencies
            wave_by_audio_uri[current.audio_uri] = wave
    return dependency_map, wave_by_audio_uri


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
