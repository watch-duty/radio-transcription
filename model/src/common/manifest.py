"""Manifest I/O and prediction-merge helpers for the transcription eval layer.

Exports:
  CanonicalRow                  — frozen dataclass, the single per-segment contract
  CanonicalManifestIssue        — structured strict validation issue
  DatasetAdapter                — structural Protocol any adapter must satisfy
  canonical_row_identity        — logical (example_id, segment_id) identity
  is_scoreable_manifest_entry   — shared predicate for rows eligible for scoring
  validate_canonical_manifest   — strict Canonical Manifest validation
  require_canonical_manifest    — fail-loud strict validation wrapper
  rows_from_manifest            — convert manifest dictionaries to typed rows
  strict_canonical_rows_from_manifest — validate and convert strict rows
  parse_manifest_text           — lenient JSON array/JSONL text parser
  parse_manifest_text_strict    — fail-loud JSON array/JSONL text parser
  load_manifest                 — lenient local JSON array/JSONL loader
  load_manifest_strict          — fail-loud local manifest loader
  merge_predictions_to_manifest — URI-first prediction merge onto GT rows
"""

from __future__ import annotations

import json
import logging
import math
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, Protocol

if TYPE_CHECKING:
    from collections.abc import Iterator, Mapping

logger = logging.getLogger(__name__)

_CANONICAL_DATASET_KEYS = (
    "name",
    "family",
)
_CANONICAL_REQUIRED_STRING_FIELDS = (
    "audio_filepath",
    "text",
    "example_id",
    "segment_id",
)


@dataclass(frozen=True)
class CanonicalRow:
    """Canonical per-segment contract shared by adapters and pipeline stages.

    Attributes:
        audio_filepath: Model-ready GCS URI for the segment audio.
        example_id: Logical example identifier.
        segment_id: Logical segment identifier within the example.
        offset: Segment offset in seconds.
        duration: Segment duration in seconds.
        text: Reference transcript for the segment.
        split: Optional dataset split label.
        dataset: Optional dataset metadata and extensions.
        source_audio: Optional source-audio metadata and extensions.
    """

    audio_filepath: str  # gs:// URI to the segment audio
    example_id: str
    segment_id: str
    offset: float
    duration: float
    text: str
    split: str | None = None
    dataset: dict[str, Any] | None = None
    source_audio: dict[str, Any] | None = None


@dataclass(frozen=True)
class CanonicalManifestIssue:
    """Structured issue returned by strict Canonical Manifest validation."""

    code: str
    message: str
    row_index: int | None = None
    field: str | None = None


class DatasetAdapter(Protocol):
    """Structural contract every dataset adapter satisfies: it yields CanonicalRows."""

    def iter_rows(self) -> Iterator[CanonicalRow]: ...


def is_scoreable_manifest_entry(entry: Mapping[str, Any]) -> bool:
    """Return whether a row has the reference fields needed for scoring."""
    return bool(
        _stripped_string(entry.get("audio_filepath"))
        and _stripped_string(entry.get("text"))
    )


def canonical_row_identity(
    row: CanonicalRow | dict[str, Any],
) -> tuple[str, str]:
    """Return the logical Canonical Manifest identity for a row.

    Args:
        row: A typed CanonicalRow or raw row dict with example_id and
            segment_id fields.

    Returns:
        The stripped ``(example_id, segment_id)`` identity.

    Raises:
        ValueError: If either identity field is missing or blank.
    """
    if isinstance(row, CanonicalRow):
        example_id = row.example_id
        segment_id = row.segment_id
    else:
        example_id = row.get("example_id")
        segment_id = row.get("segment_id")
    return (
        _required_identity_value(example_id, "example_id"),
        _required_identity_value(segment_id, "segment_id"),
    )


def validate_canonical_manifest(
    rows: list[dict[str, Any]],
    *,
    expected_split: str | None = None,
) -> list[CanonicalManifestIssue]:
    """Strictly validate Canonical Manifest rows.

    This is the single strict semantic validation path for required core
    fields and optional metadata semantics. Unknown row-level fields,
    prediction fields, and unknown keys inside metadata objects are tolerated.
    """
    issues: list[CanonicalManifestIssue] = []
    if not rows:
        _add_issue(
            issues,
            "empty_manifest",
            "manifest must contain at least one row",
        )
        return issues

    seen_identities: dict[tuple[str, str], int] = {}
    seen_audio_filepaths: dict[str, int] = {}

    for row_index, row in enumerate(rows):
        _validate_required_fields(row, row_index, issues)
        _validate_metadata(row, row_index, expected_split, issues)

        audio_filepath = _stripped_string(row.get("audio_filepath"))
        if audio_filepath:
            previous = seen_audio_filepaths.get(audio_filepath)
            if previous is not None:
                _add_issue(
                    issues,
                    "duplicate_audio_filepath",
                    f"audio_filepath duplicates row {previous}",
                    row_index=row_index,
                    field="audio_filepath",
                )
            else:
                seen_audio_filepaths[audio_filepath] = row_index

        try:
            identity = canonical_row_identity(row)
        except ValueError:
            continue
        previous_identity = seen_identities.get(identity)
        if previous_identity is not None:
            _add_issue(
                issues,
                "duplicate_identity",
                f"identity duplicates row {previous_identity}",
                row_index=row_index,
                field="example_id,segment_id",
            )
        else:
            seen_identities[identity] = row_index

    return issues


def require_canonical_manifest(
    rows: list[dict[str, Any]],
    *,
    expected_split: str | None = None,
) -> None:
    """Raise one aggregated ValueError when strict validation finds issues."""
    issues = validate_canonical_manifest(rows, expected_split=expected_split)
    if not issues:
        return

    details = [_format_issue(issue) for issue in issues]
    msg = "Canonical Manifest validation failed:\n" + "\n".join(details)
    raise ValueError(msg)


def _required_identity_value(value: Any, field: str) -> str:
    text = _stripped_string(value)
    if not text:
        msg = f"missing or blank identity field: {field}"
        raise ValueError(msg)
    return text


def _validate_required_fields(
    row: dict[str, Any],
    row_index: int,
    issues: list[CanonicalManifestIssue],
) -> None:
    for field in _CANONICAL_REQUIRED_STRING_FIELDS:
        if field not in row:
            _add_issue(
                issues,
                "missing_required",
                f"{field} is required",
                row_index=row_index,
                field=field,
            )
            continue
        value = _stripped_string(row[field])
        if not value:
            _add_issue(
                issues,
                "blank_required",
                f"{field} must be a non-empty string",
                row_index=row_index,
                field=field,
            )

    raw_audio_filepath = row.get("audio_filepath")
    audio_filepath = _stripped_string(raw_audio_filepath)
    if (
        isinstance(raw_audio_filepath, str)
        and audio_filepath is not None
        and raw_audio_filepath != audio_filepath
    ):
        _add_issue(
            issues,
            "unstripped_audio_filepath",
            "audio_filepath must not contain leading or trailing whitespace",
            row_index=row_index,
            field="audio_filepath",
        )
    if audio_filepath and not _is_gcs_flac_uri(audio_filepath):
        _add_issue(
            issues,
            "invalid_audio_uri",
            "audio_filepath must be a gs:// URI ending in .flac",
            row_index=row_index,
            field="audio_filepath",
        )

    if "offset" not in row:
        _add_issue(
            issues,
            "missing_required",
            "offset is required",
            row_index=row_index,
            field="offset",
        )
    elif not _is_number(row["offset"]) or row["offset"] < 0:
        _add_issue(
            issues,
            "invalid_offset",
            "offset must be numeric and non-negative",
            row_index=row_index,
            field="offset",
        )

    if "duration" not in row:
        _add_issue(
            issues,
            "missing_required",
            "duration is required",
            row_index=row_index,
            field="duration",
        )
    elif not _is_number(row["duration"]) or row["duration"] <= 0:
        _add_issue(
            issues,
            "invalid_duration",
            "duration must be numeric and greater than zero",
            row_index=row_index,
            field="duration",
        )


def _validate_metadata(
    row: dict[str, Any],
    row_index: int,
    expected_split: str | None,
    issues: list[CanonicalManifestIssue],
) -> None:
    if "split" in row and row["split"] is not None:
        split = _stripped_string(row["split"])
        if not split:
            _add_invalid_metadata(
                issues,
                row_index,
                "split",
                "split must be a non-empty string",
            )
        elif expected_split is not None and split != expected_split:
            hint = ""
            if {split, expected_split} == {"eval", "validation"}:
                hint = (
                    " (validation manifests are built by sampling eval and "
                    'relabeling split to "validation" -- see '
                    "model/scripts/sft/build_validation_manifest_from_eval.py "
                    "and docs/runbook.md's 'Build A Validation Manifest' "
                    "section)"
                )
            _add_issue(
                issues,
                "split_mismatch",
                f"split {split!r} does not match {expected_split!r}{hint}",
                row_index=row_index,
                field="split",
            )

    _validate_dataset(row, row_index, issues)
    _validate_source_audio(row, row_index, issues)


def _validate_dataset(
    row: dict[str, Any],
    row_index: int,
    issues: list[CanonicalManifestIssue],
) -> None:
    if "dataset" not in row or row["dataset"] is None:
        return
    dataset = row["dataset"]
    if not isinstance(dataset, dict):
        _add_invalid_metadata(
            issues,
            row_index,
            "dataset",
            "dataset must be an object",
        )
        return
    for key in _CANONICAL_DATASET_KEYS:
        field = f"dataset.{key}"
        if (
            key in dataset
            and dataset[key] is not None
            and not _stripped_string(dataset[key])
        ):
            _add_invalid_metadata(
                issues,
                row_index,
                field,
                f"{field} must be a non-empty string",
            )


def _validate_source_audio(
    row: dict[str, Any],
    row_index: int,
    issues: list[CanonicalManifestIssue],
) -> None:
    if "source_audio" not in row or row["source_audio"] is None:
        return
    source_audio = row["source_audio"]
    if not isinstance(source_audio, dict):
        _add_invalid_metadata(
            issues,
            row_index,
            "source_audio",
            "source_audio must be an object",
        )
        return
    if (
        "audio_filepath" in source_audio
        and source_audio["audio_filepath"] is not None
        and not _stripped_string(source_audio["audio_filepath"])
    ):
        _add_invalid_metadata(
            issues,
            row_index,
            "source_audio.audio_filepath",
            "source_audio.audio_filepath must be a non-empty string",
        )
    if (
        "offset" in source_audio
        and source_audio["offset"] is not None
        and (
            not _is_number(source_audio["offset"]) or source_audio["offset"] < 0
        )
    ):
        _add_invalid_metadata(
            issues,
            row_index,
            "source_audio.offset",
            "source_audio.offset must be numeric and non-negative",
        )
    if (
        "duration" in source_audio
        and source_audio["duration"] is not None
        and (
            not _is_number(source_audio["duration"])
            or source_audio["duration"] <= 0
        )
    ):
        _add_invalid_metadata(
            issues,
            row_index,
            "source_audio.duration",
            "source_audio.duration must be numeric and greater than zero",
        )


def _stripped_string(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    stripped = value.strip()
    return stripped or None


def _is_gcs_flac_uri(value: str) -> bool:
    return value.startswith("gs://") and value.lower().endswith(".flac")


def _is_number(value: Any) -> bool:
    return (
        isinstance(value, (float, int))
        and not isinstance(value, bool)
        and math.isfinite(value)
    )


def _add_invalid_metadata(
    issues: list[CanonicalManifestIssue],
    row_index: int,
    field: str,
    message: str,
) -> None:
    _add_issue(
        issues,
        "invalid_metadata",
        message,
        row_index=row_index,
        field=field,
    )


def _add_issue(
    issues: list[CanonicalManifestIssue],
    code: str,
    message: str,
    *,
    row_index: int | None = None,
    field: str | None = None,
) -> None:
    issues.append(
        CanonicalManifestIssue(
            code=code,
            message=message,
            row_index=row_index,
            field=field,
        )
    )


def _format_issue(issue: CanonicalManifestIssue) -> str:
    location = []
    if issue.row_index is not None:
        location.append(f"row {issue.row_index}")
    if issue.field is not None:
        location.append(f"field {issue.field}")
    location_text = ", ".join(location) if location else "manifest"
    return f"- {issue.code} ({location_text}): {issue.message}"


def rows_from_manifest(manifest: list[dict[str, Any]]) -> list[CanonicalRow]:
    """Convert manifest dicts to typed CanonicalRow instances.

    This is a compatibility conversion boundary for raw manifest rows:
    ``example_id`` falls back to the audio filename stem, ``segment_id`` falls
    back to ``"001"``, and a missing ``offset`` falls back to ``0.0``. The
    normalized rows are still validated against the canonical contract before
    conversion.

    Args:
        manifest: List of canonical manifest dicts, as returned by
            ``load_manifest``.

    Returns:
        List of CanonicalRow instances.

    Raises:
        ValueError: If the manifest violates the canonical contract.
    """
    normalized_manifest = [
        _normalize_manifest_entry(entry, row_index=i)
        for i, entry in enumerate(manifest)
    ]
    require_canonical_manifest(normalized_manifest)
    rows: list[CanonicalRow] = []
    for i, entry in enumerate(normalized_manifest):
        audio_filepath = _required_manifest_string(
            entry,
            "audio_filepath",
            row_index=i,
        )
        text = _required_manifest_string(entry, "text", row_index=i)
        example_id = _required_manifest_string(entry, "example_id", row_index=i)
        segment_id = _required_manifest_string(entry, "segment_id", row_index=i)
        split = _optional_manifest_string(entry, "split", row_index=i)
        offset = float(entry["offset"])
        duration = float(entry["duration"])
        rows.append(
            CanonicalRow(
                audio_filepath=audio_filepath,
                example_id=example_id,
                segment_id=segment_id,
                offset=offset,
                duration=duration,
                text=text,
                split=split,
                dataset=_optional_dataset(entry, row_index=i),
                source_audio=_optional_source_audio(entry, row_index=i),
            )
        )
    return rows


def strict_canonical_rows_from_manifest(
    manifest: list[dict[str, Any]],
    *,
    expected_split: str | None = None,
    source: str = "manifest",
) -> tuple[list[dict[str, Any]], list[CanonicalRow]]:
    """Validate and convert manifest entries through the strict canonical API.

    Args:
        manifest: Raw manifest dictionaries to validate and convert.
        expected_split: Optional split value required on rows that declare a
            split.
        source: Human-readable source name included in conversion errors.

    Returns:
        The original manifest dictionaries and their aligned canonical rows.

    Raises:
        ValueError: If strict validation or conversion fails, or if conversion
            does not preserve the row count.
    """
    require_canonical_manifest(manifest, expected_split=expected_split)
    rows = rows_from_manifest(manifest)
    if len(rows) != len(manifest):
        msg = (
            f"{source}: converted {len(rows)} canonical rows from "
            f"{len(manifest)} manifest entries"
        )
        raise ValueError(msg)
    return manifest, rows


def _normalize_manifest_entry(
    entry: dict[str, Any],
    *,
    row_index: int,
) -> dict[str, Any]:
    audio_filepath = _required_manifest_string(
        entry,
        "audio_filepath",
        row_index=row_index,
    )
    text = _required_manifest_string(entry, "text", row_index=row_index)

    normalized = dict(entry)
    normalized["audio_filepath"] = audio_filepath
    normalized["text"] = text
    normalized["example_id"] = _optional_identity_string(
        entry,
        "example_id",
        default=Path(audio_filepath).stem,
        row_index=row_index,
    )
    normalized["segment_id"] = _optional_identity_string(
        entry,
        "segment_id",
        default="001",
        row_index=row_index,
    )
    if "offset" not in normalized or normalized["offset"] is None:
        normalized["offset"] = 0.0
    return normalized


def _optional_identity_string(
    row: dict[str, Any],
    field: str,
    *,
    default: str,
    row_index: int,
) -> str:
    if field not in row or row[field] is None:
        return default
    return _required_manifest_string(row, field, row_index=row_index)


def _required_manifest_string(
    row: dict[str, Any],
    field: str,
    *,
    row_index: int,
    prefix: str = "",
) -> str:
    value = row.get(field)
    if value is None:
        msg = f"manifest row {row_index} missing or blank {prefix}{field}"
        raise ValueError(msg)
    if not isinstance(value, str):
        msg = (
            f"manifest row {row_index} field {prefix}{field} must be a "
            f"string, got {type(value).__name__}"
        )
        raise ValueError(msg)  # noqa: TRY004
    stripped = value.strip()
    if not stripped:
        msg = f"manifest row {row_index} missing or blank {prefix}{field}"
        raise ValueError(msg)
    return stripped


def _optional_manifest_string(
    row: dict[str, Any],
    field: str,
    *,
    row_index: int,
    prefix: str = "",
) -> str | None:
    if field not in row or row[field] is None:
        return None
    value = row[field]
    if not isinstance(value, str):
        msg = (
            f"manifest row {row_index} field {prefix}{field} must be a "
            f"string, got {type(value).__name__}"
        )
        raise ValueError(msg)  # noqa: TRY004
    stripped = value.strip()
    if not stripped:
        msg = f"manifest row {row_index} has blank {prefix}{field}"
        raise ValueError(msg)
    return stripped


def _optional_dataset(
    row: dict[str, Any],
    *,
    row_index: int,
) -> dict[str, Any] | None:
    dataset = row.get("dataset")
    if not isinstance(dataset, dict):
        return None
    dataset_row = {
        key: value
        for key, value in dataset.items()
        if key not in _CANONICAL_DATASET_KEYS or value is not None
    }
    for key in _CANONICAL_DATASET_KEYS:
        value = _optional_manifest_string(
            dataset, key, row_index=row_index, prefix="dataset."
        )
        if value is not None:
            dataset_row[key] = value
    return dataset_row or None


def _optional_source_audio(
    row: dict[str, Any],
    *,
    row_index: int,
) -> dict[str, Any] | None:
    source_audio = row.get("source_audio")
    if not isinstance(source_audio, dict):
        return None
    canonical_source_audio_keys = {"audio_filepath", "offset", "duration"}
    source_audio_row = {
        key: value
        for key, value in source_audio.items()
        if key not in canonical_source_audio_keys or value is not None
    }
    audio_filepath = _optional_manifest_string(
        source_audio,
        "audio_filepath",
        row_index=row_index,
        prefix="source_audio.",
    )
    if audio_filepath is not None:
        source_audio_row["audio_filepath"] = audio_filepath
    for field in ("offset", "duration"):
        if field in source_audio and source_audio[field] is not None:
            source_audio_row[field] = float(source_audio[field])
    return source_audio_row or None


def parse_manifest_text(
    content: str,
    *,
    source: str = "manifest",
) -> list[dict[str, Any]]:
    """Parse JSON array or JSONL text through the explicitly lenient API.

    Malformed JSONL rows and non-object rows are skipped, while malformed JSON
    arrays return an empty list. Strict workflow boundaries should use
    ``parse_manifest_text_strict`` instead.

    Args:
        content: JSON array or JSONL manifest text.
        source: Human-readable source name used in log messages.

    Returns:
        Parsed and normalized object rows, omitting invalid rows.
    """
    data: list[dict[str, Any]] = []
    content = content.removeprefix("\ufeff").strip()
    if not content:
        return data

    if content.startswith("["):
        try:
            parsed = json.loads(content)
        except json.JSONDecodeError as exc:
            logger.exception(
                "Failed to parse JSON array from %s: %s", source, exc
            )
            return []
        if not isinstance(parsed, list) or not all(
            isinstance(row, dict) for row in parsed
        ):
            logger.error(
                "Expected a JSON array of objects in %r, got unexpected shape",
                source,
            )
            return []
        data = parsed
    else:
        for line_number, line in enumerate(content.splitlines(), start=1):
            if not line.strip():
                continue
            try:
                parsed_row = json.loads(line)
            except json.JSONDecodeError:
                logger.warning(
                    "Skipping malformed JSON at line %s in %s",
                    line_number,
                    source,
                )
                continue
            if not isinstance(parsed_row, dict):
                logger.warning(
                    "Skipping non-object JSON at line %s in %s",
                    line_number,
                    source,
                )
                continue
            data.append(parsed_row)

    return _normalize_manifest_rows(data, coerce_non_string_text=True)


def parse_manifest_text_strict(
    content: str,
    *,
    source: str = "manifest",
) -> list[dict[str, Any]]:
    """Parse JSON array or JSONL text without skipping invalid rows.

    Args:
        content: JSON array or JSONL manifest text.
        source: Human-readable source name included in parse errors.

    Returns:
        Parsed and normalized object rows, or an empty list for empty input.

    Raises:
        ValueError: If JSON is malformed or any parsed row is not an object.
    """
    content = content.removeprefix("\ufeff")
    stripped = content.strip()
    if not stripped:
        return []

    if stripped.startswith("["):
        try:
            parsed = json.loads(stripped)
        except json.JSONDecodeError as exc:
            msg = f"{source}: malformed JSON array: {exc}"
            raise ValueError(msg) from exc
        if not isinstance(parsed, list) or not all(
            isinstance(row, dict) for row in parsed
        ):
            msg = f"{source}: expected JSON array of objects"
            raise ValueError(msg)
        return _normalize_manifest_rows(
            parsed,
            coerce_non_string_text=False,
        )

    rows: list[dict[str, Any]] = []
    for line_number, line in enumerate(content.splitlines(), start=1):
        if not line.strip():
            continue
        try:
            parsed_row = json.loads(line)
        except json.JSONDecodeError as exc:
            msg = f"{source}: malformed JSON at line {line_number}: {exc}"
            raise ValueError(msg) from exc
        if not isinstance(parsed_row, dict):
            msg = f"{source}: expected JSON object at line {line_number}"
            raise ValueError(msg)  # noqa: TRY004
        rows.append(parsed_row)
    return _normalize_manifest_rows(rows, coerce_non_string_text=False)


def _normalize_manifest_rows(
    rows: list[dict[str, Any]],
    *,
    coerce_non_string_text: bool,
) -> list[dict[str, Any]]:
    """Normalize transcript line breaks without masking strict type errors."""
    for row in rows:
        if "text" not in row:
            continue
        raw = row["text"]
        if isinstance(raw, str):
            text = raw
        elif coerce_non_string_text:
            text = "" if raw is None else str(raw)
        else:
            continue
        row["text"] = text.replace("\n", " ").replace("\r", " ")
    return rows


def load_manifest(path: str) -> list[dict[str, Any]]:
    """Leniently load a local JSON array or JSONL manifest.

    Args:
        path: Local filesystem path to a .json (array) or .jsonl manifest.

    Returns:
        Parsed and normalized object rows. Invalid rows are skipped. Missing
        or unreadable files produce an empty list.
    """
    manifest_path = Path(path)
    try:
        if not manifest_path.exists():
            logger.error("Manifest path not found: %s", path)
            return []
        content = manifest_path.read_text(encoding="utf-8-sig")
    except OSError as exc:
        logger.exception("Could not read manifest %s: %s", path, exc)
        return []
    return parse_manifest_text(content, source=path)


def load_manifest_strict(path: str) -> list[dict[str, Any]]:
    """Load and normalize a local manifest without skipping invalid rows.

    Args:
        path: Local filesystem path to a JSON array or JSONL manifest.

    Returns:
        Parsed and normalized object rows, or an empty list for an empty file.

    Raises:
        OSError: If ``path`` cannot be read.
        ValueError: If JSON is malformed or any parsed row is not an object.
    """
    content = Path(path).read_text(encoding="utf-8-sig")
    return parse_manifest_text_strict(content, source=path)


def merge_predictions_to_manifest(
    ground_truth: list[dict[str, Any]],
    predictions: list[dict[str, Any]],
    model_key: str,
    offset_tolerance: float = 0.25,
) -> list[dict[str, Any]]:
    """Align model predictions onto ground-truth rows.

    Matching is URI-first: candidates are grouped by exact ``audio_filepath``.
    Inside each URI group, identical ``(example_id, segment_id)`` values
    disambiguate candidates when both sides have identity. Remaining candidates
    use offset-tolerant closest-pair matching. Every prediction row must be
    consumed, and unmatched predictions raise ``ValueError``. Ground-truth rows
    without predictions remain allowed.

    Args:
        ground_truth: List of ground-truth rows with audio_filepath, offset,
            and reference text.
        predictions: List of prediction dicts, each with audio_filepath, offset, text.
        model_key: Short model identifier used as the field suffix (e.g. "gemini").
        offset_tolerance: Absolute offset difference (seconds) within which two
            segments are considered the same. Default: 0.25 s.

    Returns:
        The same ``ground_truth`` list, mutated IN PLACE — each matched row has
        ``pred_text_{model_key}`` written directly onto it. The row dicts are
        modified directly: a caller that must keep the original manifest pristine
        should pass a deep copy (``copy.deepcopy(ground_truth)``). A shallow
        copy is insufficient — it shares the same row dicts, which are the
        objects being mutated.

    Raises:
        ValueError: If required prediction or ground-truth keys are missing,
            malformed, or any prediction row cannot be matched.
    """
    if offset_tolerance < 0:
        msg = "offset_tolerance must be non-negative"
        raise ValueError(msg)
    pred_index = _prediction_index(predictions)
    gt_by_file = _ground_truth_index(ground_truth)

    field_name = f"pred_text_{model_key}"
    for row in ground_truth:
        row.pop(field_name, None)
    for audio_fp, gt_indices in gt_by_file.items():
        _merge_file_predictions(
            ground_truth=ground_truth,
            candidates=pred_index.get(audio_fp, []),
            gt_candidates=gt_indices,
            field_name=field_name,
            offset_tolerance=offset_tolerance,
        )
    unmatched = _unmatched_predictions(pred_index)
    if unmatched:
        raise _unmatched_predictions_error(unmatched)

    return ground_truth


@dataclass
class _PredictionCandidate:
    index: int
    audio_filepath: str
    offset: float
    text: str
    identity: tuple[str, str] | None
    matched: bool = False


@dataclass
class _GroundTruthCandidate:
    index: int
    audio_filepath: str
    offset: float
    identity: tuple[str, str] | None
    matched: bool = False


def _prediction_index(
    predictions: list[dict[str, Any]],
) -> dict[str, list[_PredictionCandidate]]:
    pred_index: dict[str, list[_PredictionCandidate]] = {}
    for i, pred in enumerate(predictions):
        audio_fp = _required_stripped_string(
            pred,
            "audio_filepath",
            "prediction",
        )
        p_offset = _required_offset(pred, "prediction")
        raw_text = pred.get("text")
        p_text = "" if raw_text is None else str(raw_text)
        pred_index.setdefault(audio_fp, []).append(
            _PredictionCandidate(
                index=i,
                audio_filepath=audio_fp,
                offset=p_offset,
                text=p_text,
                identity=_optional_identity(pred),
            )
        )
    return pred_index


def _ground_truth_index(
    ground_truth: list[dict[str, Any]],
) -> dict[str, list[_GroundTruthCandidate]]:
    gt_by_file: dict[str, list[_GroundTruthCandidate]] = {}
    for i, gt_row in enumerate(ground_truth):
        audio_fp = _required_stripped_string(
            gt_row,
            "audio_filepath",
            "ground truth row",
        )
        gt_by_file.setdefault(audio_fp, []).append(
            _GroundTruthCandidate(
                index=i,
                audio_filepath=audio_fp,
                offset=_required_offset(gt_row, "ground truth row"),
                identity=_optional_identity(gt_row),
            )
        )
    return gt_by_file


def _optional_identity(row: dict[str, Any]) -> tuple[str, str] | None:
    """Return a non-blank row identity when both identity fields are present."""
    example_id = _stripped_string(row.get("example_id"))
    segment_id = _stripped_string(row.get("segment_id"))
    if example_id is None or segment_id is None:
        return None
    return example_id, segment_id


def _required_key(row: dict[str, Any], key: str, row_kind: str) -> Any:
    if key in row:
        return row[key]
    msg = f"{row_kind} missing required '{key}': {row!r}"
    raise ValueError(msg)


def _required_stripped_string(
    row: dict[str, Any],
    key: str,
    row_kind: str,
) -> str:
    value = _required_key(row, key, row_kind)
    stripped = _stripped_string(value)
    if stripped is not None:
        return stripped
    msg = f"{row_kind} missing or blank '{key}': {row!r}"
    raise ValueError(msg)


def _required_offset(row: dict[str, Any], row_kind: str) -> float:
    """Parse a required finite, non-negative offset.

    Args:
        row: Prediction or ground-truth row containing the offset.
        row_kind: Human-readable row kind used in error messages.

    Returns:
        The parsed offset.

    Raises:
        ValueError: If the offset is absent, non-numeric, non-finite, or
            negative.
    """
    key = "offset"
    value = _required_key(row, key, row_kind)
    if isinstance(value, bool):
        msg = f"{row_kind} has non-numeric '{key}': {row!r}"
        raise ValueError(msg)  # noqa: TRY004
    try:
        parsed = float(value)
    except (TypeError, ValueError) as exc:
        msg = f"{row_kind} has non-numeric '{key}': {row!r}"
        raise ValueError(msg) from exc
    if not math.isfinite(parsed):
        msg = f"{row_kind} has non-finite '{key}': {row!r}"
        raise ValueError(msg)
    if parsed < 0:
        msg = f"{row_kind} has negative '{key}': {row!r}"
        raise ValueError(msg)
    return parsed


def _merge_file_predictions(
    *,
    ground_truth: list[dict[str, Any]],
    candidates: list[_PredictionCandidate],
    gt_candidates: list[_GroundTruthCandidate],
    field_name: str,
    offset_tolerance: float,
) -> None:
    _merge_identity_predictions(
        ground_truth=ground_truth,
        candidates=candidates,
        gt_candidates=gt_candidates,
        field_name=field_name,
    )
    pairs: list[tuple[float, _GroundTruthCandidate, _PredictionCandidate]] = []
    for gt_candidate in gt_candidates:
        if gt_candidate.matched:
            continue
        for pred_candidate in candidates:
            if pred_candidate.matched:
                continue
            diff = abs(gt_candidate.offset - pred_candidate.offset)
            if diff < offset_tolerance:
                pairs.append((diff, gt_candidate, pred_candidate))
    pairs.sort(key=lambda pair: pair[0])
    for _, gt_candidate, pred_candidate in pairs:
        if gt_candidate.matched or pred_candidate.matched:
            continue
        _assign_prediction(
            ground_truth,
            gt_candidate,
            pred_candidate,
            field_name,
        )


def _merge_identity_predictions(
    *,
    ground_truth: list[dict[str, Any]],
    candidates: list[_PredictionCandidate],
    gt_candidates: list[_GroundTruthCandidate],
    field_name: str,
) -> None:
    gt_by_identity: dict[tuple[str, str], list[_GroundTruthCandidate]] = {}
    for gt_candidate in gt_candidates:
        if gt_candidate.identity is None:
            continue
        gt_by_identity.setdefault(gt_candidate.identity, []).append(
            gt_candidate
        )
    for pred_candidate in candidates:
        if pred_candidate.identity is None or pred_candidate.matched:
            continue
        for gt_candidate in gt_by_identity.get(pred_candidate.identity, []):
            if gt_candidate.matched:
                continue
            _assign_prediction(
                ground_truth,
                gt_candidate,
                pred_candidate,
                field_name,
            )
            break


def _assign_prediction(
    ground_truth: list[dict[str, Any]],
    gt_candidate: _GroundTruthCandidate,
    pred_candidate: _PredictionCandidate,
    field_name: str,
) -> None:
    ground_truth[gt_candidate.index][field_name] = pred_candidate.text
    gt_candidate.matched = True
    pred_candidate.matched = True


def _unmatched_predictions(
    pred_index: dict[str, list[_PredictionCandidate]],
) -> list[_PredictionCandidate]:
    unmatched: list[_PredictionCandidate] = []
    for candidates in pred_index.values():
        for candidate in candidates:
            if not candidate.matched:
                unmatched.append(candidate)
    return unmatched


def _unmatched_predictions_error(
    unmatched: list[_PredictionCandidate],
) -> ValueError:
    samples = ", ".join(
        _format_prediction_sample(candidate) for candidate in unmatched[:5]
    )
    msg = (
        f"unmatched prediction row(s): {len(unmatched)} prediction(s) could "
        f"not be matched to ground truth; samples: {samples}"
    )
    return ValueError(msg)


def _format_prediction_sample(candidate: _PredictionCandidate) -> str:
    parts = [
        f"audio_filepath={candidate.audio_filepath}",
        f"offset={candidate.offset}",
    ]
    if candidate.identity is not None:
        parts.append(
            f"identity={candidate.identity[0]}/{candidate.identity[1]}"
        )
    return "{" + ", ".join(parts) + "}"
