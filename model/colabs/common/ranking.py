"""Pure ranking, prediction-cache, and context helpers for review rows."""

from __future__ import annotations

import collections.abc
import csv
import dataclasses
import hashlib
import json
import pathlib
import typing

from common import scoring


NUM_RECENT_EVENTS = 1000
MAX_CONTEXT_ROWS = NUM_RECENT_EVENTS // 2
CONTEXT_POLICY_VERSION = "same-source-row-index-v1"

RANKED_OUTPUT_FIELDS = (
    "rank",
    "wer",
    "insertions",
    "deletions",
    "substitutions",
    "hits",
    "audio_segment_id",
    "source_window_id",
    "model_ready_audio_uri",
    "original_audio_uri",
    "offset",
    "duration",
    "source_group",
    "row_index",
    "split",
    "dataset_name",
    "text",
    "prediction_text",
    "model_id",
    "prompt_fingerprint",
    "context_policy_fingerprint",
    "num_recent_events",
    "context_fingerprint",
    "cache_created_at",
)


@dataclasses.dataclass(frozen=True)
class PredictionCacheEntry:
    """Cached Gemini prediction and compatibility metadata.

    Attributes:
        audio_segment_id: Stable exact-audio identity for the predicted row.
        prediction_text: Model prediction text used for WER and context.
        model_id: Gemini model ID that produced the prediction.
        prompt_fingerprint: Fingerprint of the active system/user prompt pair.
        context_policy_fingerprint: Fingerprint of context policy settings.
        num_recent_events: Session/context event cap used for the prediction.
        context_fingerprint: Fingerprint of prior prediction context.
        source_group: Source group used by same-source context selection.
        row_index: Source-group row index for ordering and audit.
        model_ready_audio_uri: Exact model-ready audio URI.
        created_at: Cache creation timestamp or run timestamp.
        error: Non-empty when prediction failed and must not be reused.
    """

    audio_segment_id: str
    prediction_text: str
    model_id: str
    prompt_fingerprint: str
    context_policy_fingerprint: str
    num_recent_events: int
    context_fingerprint: str
    source_group: str
    row_index: int
    model_ready_audio_uri: str
    created_at: str
    error: str


def prompt_fingerprint(system_prompt: str, user_prompt: str) -> str:
    """Return a stable fingerprint for the active Gemini prompt pair.

    Args:
        system_prompt: System instruction text.
        user_prompt: User prompt text.

    Returns:
        A `prompt-` prefixed SHA-256 fingerprint.
    """
    return _stable_hash(
        "prompt",
        {
            "system_prompt": system_prompt,
            "user_prompt": user_prompt,
        },
    )


def context_policy_fingerprint(
    num_recent_events: int = NUM_RECENT_EVENTS,
) -> str:
    """Return a stable fingerprint for context policy settings.

    Args:
        num_recent_events: Gemini session event cap. Explicit prior
            audio/prediction turns use half this many prior rows.

    Returns:
        A `context-` prefixed SHA-256 fingerprint.
    """
    return _stable_hash(
        "context",
        {
            "version": CONTEXT_POLICY_VERSION,
            "num_recent_events": num_recent_events,
            "max_context_rows": num_recent_events // 2,
        },
    )


def context_fingerprint(
    context_entries: collections.abc.Iterable[
        PredictionCacheEntry | collections.abc.Mapping[str, object]
    ],
) -> str:
    """Return a fingerprint for ordered prior prediction context.

    Args:
        context_entries: Ordered prior prediction cache entries.

    Returns:
        A `context-` prefixed SHA-256 fingerprint over audio ID and prediction
        text pairs.
    """
    pairs = []
    for entry in context_entries:
        pairs.append(
            {
                "audio_segment_id": _entry_value(
                    entry,
                    "audio_segment_id",
                ),
                "prediction_text": _entry_value(entry, "prediction_text"),
            }
        )
    return _stable_hash("context", {"entries": pairs})


def is_cache_entry_compatible(
    entry: PredictionCacheEntry,
    *,
    model_id: str,
    prompt_fp: str,
    context_policy_fp: str,
    num_recent_events: int,
    context_fp: str,
) -> bool:
    """Return whether a cache entry is compatible with active settings.

    Args:
        entry: Cache entry to evaluate.
        model_id: Active model ID.
        prompt_fp: Active prompt fingerprint.
        context_policy_fp: Active context policy fingerprint.
        num_recent_events: Active context event cap.
        context_fp: Active prior-context fingerprint for this row.

    Returns:
        True only when all compatibility fields match and the entry has no
        recorded error.
    """
    return (
        _entry_successful(entry)
        and entry.model_id == model_id
        and entry.prompt_fingerprint == prompt_fp
        and entry.context_policy_fingerprint == context_policy_fp
        and entry.num_recent_events == num_recent_events
        and entry.context_fingerprint == context_fp
    )


def ordered_source_rows(
    rows: collections.abc.Iterable[collections.abc.Mapping[str, object]],
) -> dict[str, list[dict[str, object]]]:
    """Group review rows by source group and sort by row order.

    Args:
        rows: Phase 1 enriched review-pool rows.

    Returns:
        Rows keyed by `source_group`, sorted by `(row_index,
        original_pool_index)` within each group.
    """
    grouped: dict[str, list[tuple[int, int, dict[str, object]]]] = {}
    for original_pool_index, row in enumerate(rows):
        row_dict = dict(row)
        source_group = str(row_dict["source_group"])
        grouped.setdefault(source_group, []).append(
            (
                int(row_dict["row_index"]),
                original_pool_index,
                row_dict,
            )
        )

    ordered = {}
    for source_group, group_rows in grouped.items():
        sorted_rows = sorted(group_rows, key=lambda item: (item[0], item[1]))
        ordered[source_group] = [row for _, _, row in sorted_rows]
    return ordered


def previous_prediction_context(
    source_rows: collections.abc.Mapping[
        str,
        collections.abc.Sequence[collections.abc.Mapping[str, object]],
    ],
    current_row: collections.abc.Mapping[str, object],
    cache_by_audio_id: collections.abc.Mapping[str, PredictionCacheEntry],
    *,
    max_context_rows: int = MAX_CONTEXT_ROWS,
) -> list[PredictionCacheEntry]:
    """Return same-source prior successful predictions for one row.

    Args:
        source_rows: Output from `ordered_source_rows`.
        current_row: Current Phase 1 enriched review-pool row.
        cache_by_audio_id: Prediction cache entries keyed by audio ID.
        max_context_rows: Maximum prior prediction rows to return.

    Returns:
        Successful prior prediction entries in oldest-to-newest order, capped
        to the most recent `max_context_rows` rows.

    Raises:
        ValueError: If `max_context_rows` is negative.
    """
    if max_context_rows < 0:
        raise ValueError("max_context_rows must be non-negative")

    source_group = str(current_row["source_group"])
    current_row_index = int(current_row["row_index"])
    context_entries: list[PredictionCacheEntry] = []
    for row in source_rows.get(source_group, []):
        if int(row["row_index"]) >= current_row_index:
            continue
        audio_id = str(row["audio_segment_id"])
        entry = cache_by_audio_id.get(audio_id)
        if entry is None or not _entry_successful(entry):
            continue
        if entry.source_group != source_group:
            continue
        if entry.row_index != int(row["row_index"]):
            continue
        context_entries.append(entry)

    if not max_context_rows:
        return []
    return context_entries[-max_context_rows:]


def score_ranked_rows(
    rows: collections.abc.Iterable[collections.abc.Mapping[str, object]],
    cache_by_audio_id: collections.abc.Mapping[str, PredictionCacheEntry],
    *,
    model_id: str,
    prompt_fp: str,
    context_policy_fp: str,
    num_recent_events: int,
    normalizer: collections.abc.Callable[[str], str] | None = None,
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    """Score cached predictions against references and return ranked rows.

    Args:
        rows: Phase 1 enriched review-pool rows.
        cache_by_audio_id: Prediction cache entries keyed by audio ID.
        model_id: Active Gemini model ID.
        prompt_fp: Active prompt fingerprint.
        context_policy_fp: Active context policy fingerprint.
        num_recent_events: Active context event cap.
        normalizer: Optional scoring normalizer. If omitted, the canonical
            `common.scoring.build_normalizer()` is used.

    Returns:
        `(ranked_rows, excluded_rows)`. Ranked rows are sorted descending by
        per-row WER with original input order as the tie-breaker. Excluded
        rows contain audit details for empty normalized references or
        incompatible cache entries.
    """
    normalizer = normalizer or scoring.build_normalizer()
    row_list = [dict(row) for row in rows]
    source_rows = ordered_source_rows(row_list)
    compatible_cache: dict[str, PredictionCacheEntry] = {}
    original_index_by_audio_id = {}
    for original_index, row in enumerate(row_list):
        original_index_by_audio_id[str(row["audio_segment_id"])] = (
            original_index
        )

    scored_items: list[tuple[float, int, dict[str, object]]] = []
    excluded_rows: list[dict[str, object]] = []
    max_context_rows = num_recent_events // 2

    for source_group in sorted(source_rows):
        for row in source_rows[source_group]:
            audio_id = str(row["audio_segment_id"])
            context_entries = previous_prediction_context(
                source_rows,
                row,
                compatible_cache,
                max_context_rows=max_context_rows,
            )
            context_fp = context_fingerprint(context_entries)
            entry = cache_by_audio_id.get(audio_id)
            if entry is None or not is_cache_entry_compatible(
                entry,
                model_id=model_id,
                prompt_fp=prompt_fp,
                context_policy_fp=context_policy_fp,
                num_recent_events=num_recent_events,
                context_fp=context_fp,
            ):
                excluded_rows.append(
                    _excluded_row(
                        row,
                        entry,
                        context_fp,
                        "missing_or_incompatible_prediction_cache",
                    )
                )
                continue

            compatible_cache[audio_id] = entry
            reference_text = str(row["text"])
            normalized_reference = normalizer(reference_text)
            if not normalized_reference.strip():
                excluded_rows.append(
                    _excluded_row(
                        row,
                        entry,
                        context_fp,
                        "empty_normalized_reference",
                        normalized_reference=normalized_reference,
                    )
                )
                continue

            metrics = scoring.compute_wer(
                [reference_text],
                [entry.prediction_text],
                normalizer=normalizer,
            )
            output_row = _ranked_row(row, entry, context_fp, metrics)
            scored_items.append(
                (
                    float(metrics["wer"]),
                    original_index_by_audio_id[audio_id],
                    output_row,
                )
            )

    scored_items.sort(key=lambda item: (-item[0], item[1]))
    ranked_rows = []
    for rank, (_, _, row) in enumerate(scored_items, start=1):
        ranked_row = dict(row)
        ranked_row["rank"] = rank
        ranked_rows.append(ranked_row)
    return ranked_rows, excluded_rows


def write_jsonl(
    path: str | pathlib.Path,
    rows: collections.abc.Iterable[collections.abc.Mapping[str, object]],
) -> None:
    """Write rows as readable JSONL to a local filesystem path.

    Args:
        path: Destination local path.
        rows: Rows to write.
    """
    output_path = pathlib.Path(path)
    with output_path.open("w", encoding="utf-8") as output_file:
        for row in rows:
            line = json.dumps(
                dict(row),
                ensure_ascii=True,
                separators=(",", ":"),
                sort_keys=True,
            )
            output_file.write(f"{line}\n")


def write_ranked_csv(
    path: str | pathlib.Path,
    rows: collections.abc.Iterable[collections.abc.Mapping[str, object]],
) -> None:
    """Write ranked rows as CSV to a local filesystem path.

    Args:
        path: Destination local path.
        rows: Ranked rows to write.
    """
    row_list = [dict(row) for row in rows]
    fieldnames = _csv_fieldnames(row_list)
    output_path = pathlib.Path(path)
    with output_path.open("w", encoding="utf-8", newline="") as output_file:
        writer = csv.DictWriter(output_file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(row_list)


def _stable_hash(prefix: str, payload: dict[str, object]) -> str:
    encoded = json.dumps(
        payload,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    digest = hashlib.sha256(encoded).hexdigest()
    return f"{prefix}-{digest}"


def _entry_value(
    entry: PredictionCacheEntry | collections.abc.Mapping[str, object],
    field: str,
) -> typing.Any:
    if dataclasses.is_dataclass(entry):
        return getattr(entry, field)
    return entry[field]


def _entry_successful(entry: PredictionCacheEntry) -> bool:
    if entry.error is None:
        return True
    return not str(entry.error).strip()


def _ranked_row(
    row: collections.abc.Mapping[str, object],
    entry: PredictionCacheEntry,
    context_fp: str,
    metrics: collections.abc.Mapping[str, object],
) -> dict[str, object]:
    output = dict(row)
    output.update(
        {
            "rank": None,
            "prediction_text": entry.prediction_text,
            "model_id": entry.model_id,
            "prompt_fingerprint": entry.prompt_fingerprint,
            "context_policy_fingerprint": entry.context_policy_fingerprint,
            "num_recent_events": entry.num_recent_events,
            "context_fingerprint": context_fp,
            "cache_created_at": entry.created_at,
            "wer": metrics["wer"],
            "insertions": metrics["insertions"],
            "deletions": metrics["deletions"],
            "substitutions": metrics["substitutions"],
            "hits": metrics["hits"],
        }
    )
    return output


def _excluded_row(
    row: collections.abc.Mapping[str, object],
    entry: PredictionCacheEntry | None,
    context_fp: str,
    exclusion_reason: str,
    *,
    normalized_reference: str | None = None,
) -> dict[str, object]:
    output = dict(row)
    output["exclusion_reason"] = exclusion_reason
    output["context_fingerprint"] = context_fp
    if normalized_reference is not None:
        output["normalized_reference"] = normalized_reference
    if entry is not None:
        output.update(
            {
                "prediction_text": entry.prediction_text,
                "model_id": entry.model_id,
                "prompt_fingerprint": entry.prompt_fingerprint,
                "context_policy_fingerprint": (
                    entry.context_policy_fingerprint
                ),
                "num_recent_events": entry.num_recent_events,
                "cache_created_at": entry.created_at,
            }
        )
    return output


def _csv_fieldnames(
    rows: collections.abc.Sequence[collections.abc.Mapping[str, object]],
) -> list[str]:
    fieldnames = list(RANKED_OUTPUT_FIELDS)
    seen = set(fieldnames)
    for row in rows:
        for field in row:
            if field in seen:
                continue
            fieldnames.append(field)
            seen.add(field)
    return fieldnames
