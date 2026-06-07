"""Manifest I/O and prediction-merge helpers for the transcription eval layer.

Exports:
  CanonicalRow                  — frozen dataclass, the single per-segment contract
  DatasetAdapter                — structural Protocol any adapter must satisfy
  rows_from_manifest            — convert raw manifest dicts to typed CanonicalRow instances
  load_manifest                 — load a JSON array or JSONL manifest from local disk
  merge_predictions_to_manifest — offset-tolerant merge of model predictions onto GT rows
"""

from __future__ import annotations

import json
import logging
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


class DatasetAdapter(Protocol):
    """Structural contract every dataset adapter satisfies: it yields CanonicalRows."""

    def iter_rows(self) -> Iterator[CanonicalRow]: ...


def rows_from_manifest(manifest: list[dict[str, Any]]) -> list[CanonicalRow]:
    """Convert raw manifest dicts to typed CanonicalRow instances.

    Derives fields from the NeMo-style manifest schema (audio_filepath, text,
    offset, duration). example_id and segment_id fall back to stable derived
    values when absent from the manifest dict.

    Args:
        manifest: List of raw manifest dicts (as returned by load_manifest).

    Returns:
        List of CanonicalRow instances. Rows missing audio_filepath or text are
        skipped with a warning.
    """
    rows: list[CanonicalRow] = []
    for i, entry in enumerate(manifest):
        audio_filepath: str | None = entry.get("audio_filepath")
        text: str | None = entry.get("text")
        if not audio_filepath:
            logger.warning(f"Skipping manifest row {i}: missing audio_filepath")
            continue
        if not text:
            logger.warning(
                f"Skipping manifest row {i}: missing text ({audio_filepath!r})"
            )
            continue
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
