"""Manifest I/O and prediction-merge helpers for the transcription eval layer.

Exports:
  CanonicalRow                  — frozen dataclass, the single per-segment contract
  CanonicalManifestIssue        — structured strict validation issue
  DatasetAdapter                — structural Protocol any adapter must satisfy
  canonical_row_identity        — logical (example_id, segment_id) identity
  validate_canonical_manifest   — strict Canonical Manifest validation
  require_canonical_manifest    — fail-loud strict validation wrapper
  rows_from_manifest            — convert raw manifest dicts to typed CanonicalRow instances
  load_manifest                 — load a JSON array or JSONL manifest from local disk
  merge_predictions_to_manifest — offset-tolerant merge of model predictions onto GT rows
"""

from __future__ import annotations

import json
import logging
import math
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, Protocol

if TYPE_CHECKING:
    from collections.abc import Iterator

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class CanonicalRow:
    """Canonical per-segment row — the single contract between dataset adapters and pipeline stages.

    This is a fan-in dependency: SFT example builders, dataset adapters, and
    the test suite all consume this exact shape.
    """

    audio_filepath: str  # gs:// URI to the segment audio
    example_id: str
    segment_id: str
    offset: float
    duration: float
    text: str


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

    This is the single strict semantic validation path. Exploratory parsing
    through load_manifest() remains lenient, and rows_from_manifest() remains a
    compatibility conversion boundary rather than a full strict validator.
    """
    issues: list[CanonicalManifestIssue] = []
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
    for field in ("audio_filepath", "text", "example_id", "segment_id"):
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

    audio_filepath = _stripped_string(row.get("audio_filepath"))
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
    elif not _is_number(row["offset"]):
        _add_issue(
            issues,
            "invalid_offset",
            "offset must be numeric",
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
    if "split" in row:
        split = _stripped_string(row["split"])
        if not split:
            _add_invalid_metadata(
                issues,
                row_index,
                "split",
                "split must be a non-empty string",
            )
        elif expected_split is not None and split != expected_split:
            _add_issue(
                issues,
                "split_mismatch",
                f"split {split!r} does not match {expected_split!r}",
                row_index=row_index,
                field="split",
            )

    if "lang" in row and not _stripped_string(row["lang"]):
        _add_invalid_metadata(
            issues,
            row_index,
            "lang",
            "lang must be a non-empty string",
        )

    _validate_dataset(row, row_index, issues)
    _validate_source_audio(row, row_index, issues)
    _validate_audio_processing(row, row_index, issues)


def _validate_dataset(
    row: dict[str, Any],
    row_index: int,
    issues: list[CanonicalManifestIssue],
) -> None:
    if "dataset" not in row:
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
    for key in ("name", "family"):
        if key in dataset and not _stripped_string(dataset[key]):
            _add_invalid_metadata(
                issues,
                row_index,
                f"dataset.{key}",
                f"dataset.{key} must be a non-empty string",
            )


def _validate_source_audio(
    row: dict[str, Any],
    row_index: int,
    issues: list[CanonicalManifestIssue],
) -> None:
    if "source_audio" not in row:
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
        and not _stripped_string(source_audio["audio_filepath"])
    ):
        _add_invalid_metadata(
            issues,
            row_index,
            "source_audio.audio_filepath",
            "source_audio.audio_filepath must be a non-empty string",
        )
    if "offset" in source_audio and (
        not _is_number(source_audio["offset"]) or source_audio["offset"] < 0
    ):
        _add_invalid_metadata(
            issues,
            row_index,
            "source_audio.offset",
            "source_audio.offset must be numeric and non-negative",
        )
    if "duration" in source_audio and (
        not _is_number(source_audio["duration"])
        or source_audio["duration"] <= 0
    ):
        _add_invalid_metadata(
            issues,
            row_index,
            "source_audio.duration",
            "source_audio.duration must be numeric and greater than zero",
        )


def _validate_audio_processing(
    row: dict[str, Any],
    row_index: int,
    issues: list[CanonicalManifestIssue],
) -> None:
    if "audio_processing" not in row:
        return
    audio_processing = row["audio_processing"]
    if not isinstance(audio_processing, dict):
        _add_invalid_metadata(
            issues,
            row_index,
            "audio_processing",
            "audio_processing must be an object",
        )
        return
    if "masked_categories" not in audio_processing:
        return
    masked_categories = audio_processing["masked_categories"]
    if not isinstance(masked_categories, list) or not all(
        _stripped_string(value) for value in masked_categories
    ):
        _add_invalid_metadata(
            issues,
            row_index,
            "audio_processing.masked_categories",
            "audio_processing.masked_categories must contain non-empty strings",
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
    """Convert raw manifest dicts to typed CanonicalRow instances.

    Derives fields from the NeMo-style manifest schema (audio_filepath, text,
    offset, duration). example_id and segment_id fall back to stable derived
    values when absent from the manifest dict.

    Args:
        manifest: List of raw manifest dicts (as returned by load_manifest).

    Returns:
        List of CanonicalRow instances.

    Raises:
        ValueError: If a row is missing audio_filepath or text, or either
            value is blank.
    """
    rows: list[CanonicalRow] = []
    for i, entry in enumerate(manifest):
        audio_filepath = _required_manifest_string(
            entry,
            "audio_filepath",
            row_index=i,
        )
        text = _required_manifest_string(entry, "text", row_index=i)
        offset: float = float(entry.get("offset") or 0.0)
        duration: float = float(entry.get("duration") or 0.0)
        # Derive stable example_id / segment_id from the manifest or fallback to basename
        example_id: str = str(
            entry.get("example_id") or Path(audio_filepath).stem
        )
        segment_id: str = str(entry.get("segment_id", "001"))
        rows.append(
            CanonicalRow(
                audio_filepath=audio_filepath,
                example_id=example_id,
                segment_id=segment_id,
                offset=offset,
                duration=duration,
                text=text,
            )
        )
    return rows


def _required_manifest_string(
    row: dict[str, Any],
    field: str,
    *,
    row_index: int,
) -> str:
    value = row.get(field)
    text = "" if value is None else str(value)
    if not text.strip():
        msg = f"manifest row {row_index} missing or blank {field}"
        raise ValueError(msg)
    return text


def load_manifest(path: str) -> list[dict[str, Any]]:
    """Loads a manifest file (JSON array or JSONL) from the local filesystem.

    Args:
        path: Local filesystem path to a .json (array) or .jsonl manifest.

    Returns:
        List of row dicts; an empty list if the path is missing, unreadable,
        or unparseable.
    """
    data: list[dict[str, Any]] = []
    try:
        if not Path(path).exists():
            logger.error(f"Manifest path not found: {path}")
            return []
        with open(path, encoding="utf-8") as f:
            content = f.read().strip()
    except OSError as e:
        # Path.exists() can raise (not just return False) when a parent
        # directory denies search/execute permission — and open() can raise
        # on an unreadable file. Either way, soft-fail to [] rather than
        # crashing the caller.
        logger.exception(f"Could not read manifest {path}: {e}")
        return []
    if content.startswith("["):
        try:
            data = json.loads(content)
        except json.JSONDecodeError as e:
            logger.exception(f"Failed to parse JSON array: {e}")
            return []
        if not isinstance(data, list) or not all(
            isinstance(row, dict) for row in data
        ):
            logger.error(
                f"Expected a JSON array of objects in {path!r}, got unexpected shape"
            )
            return []
    else:
        for i, obj_str in enumerate(content.splitlines(), start=1):
            if not obj_str.strip():
                continue
            try:
                obj = json.loads(obj_str)
            except json.JSONDecodeError:
                logger.warning(f"Skipping malformed JSON at line {i}")
                continue
            if not isinstance(obj, dict):
                logger.warning(f"Skipping non-object JSON at line {i}")
                continue
            data.append(obj)
    for row in data:
        if "text" in row:
            # Coerce a non-string text field (None / int / bool in a malformed
            # manifest) to a string so a downstream .strip()/.lower() never
            # raises; a null text becomes "" rather than the literal "None".
            raw = row["text"]
            text = "" if raw is None else str(raw)
            row["text"] = text.replace("\n", " ").replace("\r", " ")
    return data


def merge_predictions_to_manifest(
    ground_truth: list[dict[str, Any]],
    predictions: list[dict[str, Any]],
    model_key: str,
    offset_tolerance: float = 0.25,
) -> list[dict[str, Any]]:
    """Align model predictions onto ground-truth rows by (audio_filepath, offset).

    Uses an absolute-difference tolerance for offset matching — NEVER exact float
    equality (exact float equality is silently fragile). Predictions are matched
    to ground-truth rows ONE-TO-ONE per audio_filepath: the closest in-tolerance
    (row, prediction) pairs are bound first, and each prediction is consumed at
    most once — so a missing prediction leaves its row's pred_text_{model_key}
    unset (correctly scored as an error) instead of borrowing a neighbor's.

    Args:
        ground_truth: List of ground-truth manifest dicts (NeMo-style schema).
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
        ValueError: If required prediction or ground-truth keys are missing.
            Unexpected errors are intentionally not swallowed: returning an
            empty merge would let downstream scoring report false success.
    """
    pred_index = _prediction_index(predictions)
    gt_by_file = _ground_truth_index(ground_truth)

    field_name = f"pred_text_{model_key}"
    for row in ground_truth:
        row.pop(field_name, None)
    for audio_fp, gt_indices in gt_by_file.items():
        _merge_file_predictions(
            ground_truth=ground_truth,
            candidates=pred_index.get(audio_fp, []),
            gt_indices=gt_indices,
            field_name=field_name,
            offset_tolerance=offset_tolerance,
        )

    return ground_truth


def _prediction_index(
    predictions: list[dict[str, Any]],
) -> dict[str, list[tuple[float, str]]]:
    pred_index: dict[str, list[tuple[float, str]]] = {}
    for pred in predictions:
        audio_fp = str(_required_key(pred, "audio_filepath", "prediction"))
        p_offset = float(_required_key(pred, "offset", "prediction"))
        raw_text = pred.get("text")
        p_text = "" if raw_text is None else str(raw_text)
        pred_index.setdefault(audio_fp, []).append((p_offset, p_text))
    return pred_index


def _ground_truth_index(
    ground_truth: list[dict[str, Any]],
) -> dict[str, list[int]]:
    gt_by_file: dict[str, list[int]] = {}
    for i, gt_row in enumerate(ground_truth):
        audio_fp = str(
            _required_key(gt_row, "audio_filepath", "ground truth row")
        )
        _required_key(gt_row, "offset", "ground truth row")
        gt_by_file.setdefault(audio_fp, []).append(i)
    return gt_by_file


def _required_key(row: dict[str, Any], key: str, row_kind: str) -> Any:
    if key in row:
        return row[key]
    msg = f"{row_kind} missing required '{key}': {row!r}"
    raise ValueError(msg)


def _merge_file_predictions(
    *,
    ground_truth: list[dict[str, Any]],
    candidates: list[tuple[float, str]],
    gt_indices: list[int],
    field_name: str,
    offset_tolerance: float,
) -> None:
    pairs: list[tuple[float, int, int]] = []
    for gi in gt_indices:
        gt_offset = float(ground_truth[gi]["offset"])
        for pi, (p_offset, _) in enumerate(candidates):
            diff = abs(gt_offset - p_offset)
            if diff < offset_tolerance:
                pairs.append((diff, gi, pi))
    pairs.sort(key=lambda pair: pair[0])
    used_gt: set[int] = set()
    used_pred: set[int] = set()
    for _, gi, pi in pairs:
        if gi in used_gt or pi in used_pred:
            continue
        ground_truth[gi][field_name] = candidates[pi][1]
        used_gt.add(gi)
        used_pred.add(pi)
