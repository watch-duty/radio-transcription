"""Manifest I/O and prediction-merge helpers for the transcription eval layer.

Exports:
  load_manifest                 — load a JSON array or JSONL manifest from local disk
  merge_predictions_to_manifest — offset-tolerant merge of model predictions onto GT rows
"""

import json
import logging
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


def load_manifest(path: str) -> list[dict[str, Any]]:
    """Loads a manifest file (JSON array or JSONL) from the local filesystem.

    Args:
        path: Local filesystem path to a .json (array) or .jsonl manifest.

    Returns:
        List of row dicts; an empty list if the path is missing or unparseable.
    """
    data: list[dict[str, Any]] = []
    if not Path(path).exists():
        logger.error(f"Manifest path not found: {path}")
        return []
    with open(path, encoding="utf-8") as f:
        content = f.read().strip()
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
        if row.get("text"):
            # str() cast guards against a non-string text field in a malformed
            # manifest — .replace() on a list/int/bool would raise AttributeError.
            row["text"] = str(row["text"]).replace("\n", " ").replace("\r", " ")
    return data


def merge_predictions_to_manifest(
    ground_truth: list[dict[str, Any]],
    predictions: list[dict[str, Any]],
    model_key: str,
    offset_tolerance: float = 0.25,
) -> list[dict[str, Any]]:
    """Align model predictions onto ground-truth rows by (audio_filepath, offset).

    Uses an absolute-difference tolerance for offset matching — NEVER exact float
    equality (exact float equality is silently fragile). A matched
    prediction's text is written onto the GT row under pred_text_{model_key}.

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
            audio_fp = str(pred.get("audio_filepath", ""))
            p_offset = float(pred.get("offset", 0.0))
            p_text = str(pred.get("text", ""))
            pred_index.setdefault(audio_fp, []).append((p_offset, p_text))

        field_name = f"pred_text_{model_key}"
        for gt_row in ground_truth:
            audio_fp = str(gt_row.get("audio_filepath", ""))
            gt_offset = float(gt_row.get("offset", 0.0))
            candidates = pred_index.get(audio_fp, [])
            for p_offset, p_text in candidates:
                if abs(gt_offset - p_offset) < offset_tolerance:
                    gt_row[field_name] = p_text
                    break

        return ground_truth
    except Exception as e:
        logger.error(
            f"Failed to merge manifest predictions for model '{model_key}': {e}"
        )
        raise
