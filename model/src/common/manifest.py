"""Manifest I/O and prediction-merge helpers for the transcription eval layer.

Exports:
  CanonicalRow                  — frozen dataclass, the single per-segment contract
  CanonicalManifestIssue        — structured strict validation issue
  DatasetAdapter                — structural Protocol any adapter must satisfy
  canonical_row_identity        — logical (example_id, segment_id) identity
  is_scoreable_manifest_entry   — shared predicate for rows eligible for scoring
  validate_canonical_manifest   — strict Canonical Manifest validation
  require_canonical_manifest    — fail-loud strict validation wrapper
  rows_from_manifest            — convert raw manifest dicts to typed CanonicalRow instances
  strict_canonical_rows_from_manifest — validate and convert via one strict boundary
  parse_manifest_text           — parse JSON array/JSONL text into normalized row dicts
  load_manifest                 — load a JSON array or JSONL manifest from local disk
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


def is_scoreable_manifest_entry(entry: Mapping[str, Any]) -> bool:
    """Return whether a raw manifest entry can enter JiWER scoring."""
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

    This is the single strict semantic validation path. Exploratory parsing
    through load_manifest() remains lenient, and rows_from_manifest() remains a
    row conversion boundary rather than a full strict validator.
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
    if "audio_filepath" in source_audio and not _stripped_string(
        source_audio["audio_filepath"]
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


def strict_canonical_rows_from_manifest(
    manifest: list[dict[str, Any]],
    *,
    expected_split: str | None = None,
    source: str = "manifest",
) -> tuple[list[dict[str, Any]], list[CanonicalRow]]:
    """Validate and convert manifest entries through the strict canonical API."""
    require_canonical_manifest(manifest, expected_split=expected_split)
    rows = rows_from_manifest(manifest)
    if len(rows) != len(manifest):
        msg = (
            f"{source}: converted {len(rows)} canonical rows from "
            f"{len(manifest)} manifest entries"
        )
        raise ValueError(msg)
    return manifest, rows


def _required_manifest_string(
    row: dict[str, Any],
    field: str,
    *,
    row_index: int,
) -> str:
    value = row.get(field)
    text = "" if value is None else str(value)
    stripped = text.strip()
    if not stripped:
        msg = f"manifest row {row_index} missing or blank {field}"
        raise ValueError(msg)
    return stripped


def parse_manifest_text(
    content: str,
    *,
    source: str = "manifest",
) -> list[dict[str, Any]]:
    """Parse JSON array or JSONL manifest text into normalized row dicts.

    Parsing is intentionally lenient: malformed JSONL rows and non-object rows
    are skipped, while malformed JSON arrays return an empty list. Strict
    semantic validation belongs in ``require_canonical_manifest``.
    """
    data: list[dict[str, Any]] = []
    content = content.strip()
    if not content:
        return data

    if content.startswith("["):
        try:
            parsed = json.loads(content)
        except json.JSONDecodeError as e:
            logger.exception(f"Failed to parse JSON array from {source}: {e}")
            return []
        if not isinstance(parsed, list) or not all(
            isinstance(row, dict) for row in parsed
        ):
            logger.error(
                f"Expected a JSON array of objects in {source!r}, got unexpected shape"
            )
            return []
        data = parsed
    else:
        for i, obj_str in enumerate(content.splitlines(), start=1):
            if not obj_str.strip():
                continue
            try:
                obj = json.loads(obj_str)
            except json.JSONDecodeError:
                logger.warning(
                    f"Skipping malformed JSON at line {i} in {source}"
                )
                continue
            if not isinstance(obj, dict):
                logger.warning(
                    f"Skipping non-object JSON at line {i} in {source}"
                )
                continue
            data.append(obj)

    return _normalize_manifest_rows(data)


def parse_manifest_text_strict(
    content: str,
    *,
    source: str = "manifest",
) -> list[dict[str, Any]]:
    """Parse a JSON array or JSONL manifest without skipping invalid rows."""
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
        return _normalize_manifest_rows(parsed)

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
            raise TypeError(msg)
        rows.append(parsed_row)
    return _normalize_manifest_rows(rows)


def _normalize_manifest_rows(
    rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    for row in rows:
        if "text" in row:
            # Coerce malformed text values once at the parser boundary. Null
            # text becomes absent transcript text rather than the literal word
            # "None".
            raw = row["text"]
            text = "" if raw is None else str(raw)
            row["text"] = text.replace("\n", " ").replace("\r", " ")
    return rows


def load_manifest(path: str) -> list[dict[str, Any]]:
    """Loads a manifest file (JSON array or JSONL) from the local filesystem.

    Args:
        path: Local filesystem path to a .json (array) or .jsonl manifest.

    Returns:
        List of row dicts; an empty list if the path is missing, unreadable,
        or unparseable.
    """
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
    return parse_manifest_text(content, source=path)


def load_manifest_strict(path: str) -> list[dict[str, Any]]:
    """Load a local JSON array or JSONL manifest without skipping rows."""
    content = Path(path).read_text(encoding="utf-8")
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
        ValueError: If required prediction or ground-truth keys are missing,
            malformed, or any prediction row cannot be matched.
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
        audio_fp = str(_required_key(pred, "audio_filepath", "prediction"))
        p_offset = _required_float(pred, "offset", "prediction")
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
        audio_fp = str(
            _required_key(gt_row, "audio_filepath", "ground truth row")
        )
        gt_by_file.setdefault(audio_fp, []).append(
            _GroundTruthCandidate(
                index=i,
                audio_filepath=audio_fp,
                offset=_required_float(gt_row, "offset", "ground truth row"),
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


def _required_float(row: dict[str, Any], key: str, row_kind: str) -> float:
    value = _required_key(row, key, row_kind)
    try:
        return float(value)
    except (TypeError, ValueError) as exc:
        msg = f"{row_kind} has non-numeric '{key}': {row!r}"
        raise ValueError(msg) from exc


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
