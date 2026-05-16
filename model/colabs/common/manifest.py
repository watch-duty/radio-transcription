"""
Typed canonical data contract for the SFT pipeline manifest layer.

Exports:
  CanonicalRow         — frozen dataclass, the single per-segment contract
  DatasetAdapter       — structural Protocol any adapter must satisfy
  load_manifest        — load a JSON array or JSONL manifest from local disk
  rows_from_manifest   — convert raw manifest dicts to typed CanonicalRow instances
  merge_predictions_to_manifest — offset-tolerant merge of model predictions onto GT rows

LIB-02 / D-09: CanonicalRow is a fan-in dependency of sft.build_example (Plan 02),
both Phase 3 adapters (gcs_manifest / hf_dataset), and the Phase 2 test suite.
"""

import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator, Optional, Protocol

from common.gcs_utils import parse_gcs_uri

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class CanonicalRow:
    """Canonical per-segment row — the single contract between dataset adapters and pipeline stages.

    This is a fan-in dependency: sft.build_example, the Phase 3 gcs_manifest /
    hf_dataset adapters, and the test suite all consume this exact shape.
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
    else:
        for i, obj_str in enumerate(content.splitlines()):
            if not obj_str.strip():
                continue
            try:
                data.append(json.loads(obj_str))
            except json.JSONDecodeError:
                logger.warning(f"Skipping malformed JSON at line {i}")
    for row in data:
        if row.get("text"):
            row["text"] = row["text"].replace("\n", " ").replace("\r", " ")
    return data


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
            logger.warning(f"Skipping manifest row {i}: missing text ({audio_filepath!r})")
            continue
        offset: float = float(entry.get("offset", 0.0))
        duration: float = float(entry.get("duration", 0.0))
        # Derive stable example_id / segment_id from the manifest or fallback to basename
        example_id: str = str(entry.get("example_id") or Path(audio_filepath).stem)
        segment_id: str = str(entry.get("segment_id") or f"{example_id}_{offset:.3f}")
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


def merge_predictions_to_manifest(
    ground_truth: list[dict[str, Any]],
    predictions: list[dict[str, Any]],
    model_key: str,
    offset_tolerance: float = 0.25,
) -> list[dict[str, Any]]:
    """Align model predictions onto ground-truth rows by (example_id, offset).

    Uses an absolute-difference tolerance for offset matching — NEVER exact float
    equality (CONCERNS.md flags exact equality as silently fragile). A matched
    prediction's text is written onto the GT row under pred_text_{model_key}.

    Args:
        ground_truth: List of ground-truth manifest dicts (NeMo-style schema).
        predictions: List of prediction dicts, each with example_id, offset, text.
        model_key: Short model identifier used as the field suffix (e.g. "gemini").
        offset_tolerance: Absolute offset difference (seconds) within which two
            segments are considered the same. Default: 0.25 s.

    Returns:
        The ground_truth list with pred_text_{model_key} written onto matched rows;
        an empty list if an unexpected error occurs.
    """
    try:
        # Build lookup: example_id -> list of (offset, text) from predictions
        pred_index: dict[str, list[tuple[float, str]]] = {}
        for pred in predictions:
            ex_id = str(pred.get("example_id", ""))
            p_offset = float(pred.get("offset", 0.0))
            p_text = str(pred.get("text", ""))
            pred_index.setdefault(ex_id, []).append((p_offset, p_text))

        field_name = f"pred_text_{model_key}"
        for gt_row in ground_truth:
            ex_id = str(gt_row.get("example_id", ""))
            gt_offset = float(gt_row.get("offset", 0.0))
            candidates = pred_index.get(ex_id, [])
            for p_offset, p_text in candidates:
                if abs(gt_offset - p_offset) < offset_tolerance:
                    gt_row[field_name] = p_text
                    break

        return ground_truth
    except Exception as e:
        logger.error(f"Failed to merge manifest predictions for model '{model_key}': {e}")
        return []
