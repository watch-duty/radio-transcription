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
from typing import Any, Iterator, Optional, Protocol

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
        audio_filepath: Optional[str] = entry.get("audio_filepath")
        text: Optional[str] = entry.get("text")
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
        logger.error(f"Could not read manifest {path}: {e}")
        return []
    if content.startswith("["):
        try:
            data = json.loads(content)
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse JSON array: {e}")
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
        Exception: Re-raises any unexpected exception after logging. An
            unexpected error here is a bug — silently returning [] would cause
            downstream WER scoring to read 0 segments and report false success.
    """
    try:
        # Build lookup: audio_filepath -> list of (offset, text) from predictions
        pred_index: dict[str, list[tuple[float, str]]] = {}
        for pred in predictions:
            # Fail loud on a malformed prediction missing the keys that drive
            # the merge — silently defaulting audio_filepath to "" or offset
            # to 0.0 would attach a stray prediction to whichever ground-truth
            # row happens to share that empty bucket / sit at offset 0.0.
            if "audio_filepath" not in pred:
                raise ValueError(
                    f"prediction missing required 'audio_filepath': {pred!r}"
                )
            if "offset" not in pred:
                raise ValueError(
                    f"prediction missing required 'offset': {pred!r}"
                )
            audio_fp = str(pred["audio_filepath"])
            p_offset = float(pred["offset"])
            # Mirror load_manifest's coercion: a null prediction text becomes
            # "" (absent), NOT the literal four-letter word "None" — which
            # would otherwise inflate WER as a real-looking prediction token.
            raw_text = pred.get("text")
            p_text = "" if raw_text is None else str(raw_text)
            pred_index.setdefault(audio_fp, []).append((p_offset, p_text))

        # Group ground-truth row indices by audio_filepath so the offset
        # match below is resolved within each source file. Validate the same
        # required keys as on the predictions side — silently defaulting
        # audio_filepath to "" or offset to 0.0 would mask a malformed GT
        # manifest by binding every row to whichever group / segment happens
        # to sit at the default.
        gt_by_file: dict[str, list[int]] = {}
        for i, gt_row in enumerate(ground_truth):
            if "audio_filepath" not in gt_row:
                raise ValueError(
                    f"ground truth row missing required 'audio_filepath': {gt_row!r}"
                )
            if "offset" not in gt_row:
                raise ValueError(
                    f"ground truth row missing required 'offset': {gt_row!r}"
                )
            audio_fp = str(gt_row["audio_filepath"])
            gt_by_file.setdefault(audio_fp, []).append(i)

        field_name = f"pred_text_{model_key}"
        # Clear any stale pred_text_{model_key} from a prior merge — a row
        # should only carry this field if THIS merge produced a match for
        # it. Otherwise a re-run with a missing prediction for some row
        # would leave the old prediction in place, masking the missing-
        # output failure as a successful prediction in downstream WER.
        for row in ground_truth:
            row.pop(field_name, None)
        for audio_fp, gt_indices in gt_by_file.items():
            candidates = pred_index.get(audio_fp, [])
            # One-to-one match: collect every (row, prediction) pair within
            # tolerance, then assign closest-first, consuming each row and
            # each prediction at most once. A prediction is therefore NEVER
            # bound to two rows — so a genuinely missing prediction leaves
            # its row blank and still scores as an error in WER, rather than
            # borrowing a neighbor's.
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

        return ground_truth
    except Exception as e:
        logger.error(
            f"Failed to merge manifest predictions for model '{model_key}': {e}"
        )
        raise
