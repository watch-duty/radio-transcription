"""Data-quality preflight -- hard gate before any paid tune run.

On any violation, write a preflight report to report_path and stop before tune.
The operator fixes the data; there is no auto-filter escape hatch.
Checks include per-example token limit, empty/whitespace targets, duplicate fileUris,
fileUri reachability via blob_exists, and non-empty/disjoint train/val splits.
"""

from __future__ import annotations

import json
import logging
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from functools import partial
from typing import TYPE_CHECKING, Any, Final

from common.gcs_utils import blob_exists
from common.gemini.tuning_data import validate_audio_tuning_example

if TYPE_CHECKING:
    from pathlib import Path

    from google.cloud import storage

logger = logging.getLogger(__name__)

# Gemini 3.1 Flash-Lite SFT supports a 128k context. This local check only
# catches obviously-too-large text examples; Vertex remains the source of truth
# for exact multimodal token accounting.
PREFLIGHT_TOKEN_CAP: Final = 131_072
PREFLIGHT_GCS_MAX_WORKERS: Final = 16
PREFLIGHT_GCS_BATCH_SIZE: Final = 256
PREFLIGHT_GCS_TIMEOUT_SECONDS: Final = 30.0


@dataclass
class PreflightReport:
    failures: list[str] = field(default_factory=list)
    offending_ids: list[str] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return len(self.failures) == 0


def _safe_blob_exists(storage_client: storage.Client, uri: str) -> bool:
    """``blob_exists`` that never raises -- a malformed/unparseable URI is unreachable.

    ``blob_exists`` calls ``parse_gcs_uri``, which raises on a non-``gs://`` URI. Without
    this guard a single malformed fileUri would crash ``run_preflight`` before the report
    is written. The hard gate should always write a preflight report.
    A malformed URI is reported downstream as "not reachable", so the operator
    still gets a clear, actionable failure when reachability is checked.
    """
    try:
        return blob_exists(
            storage_client,
            uri,
            timeout=PREFLIGHT_GCS_TIMEOUT_SECONDS,
        )
    except Exception:
        return False


def _estimate_text_tokens(
    example: dict[str, Any], system_prompt: str, user_prompt: str
) -> int:
    """Conservative token estimate for the text portion of an example.

    Audio duration is not known at preflight time (we only have the GCS URI,
    not audio content). Token cap is 131,072; for typical <30s clips the audio
    portion is <1,000 tokens. Text estimate catches pathologically long transcripts.
    """
    text_len = len(system_prompt) + len(user_prompt)
    contents = example.get("contents")
    if isinstance(contents, list):
        for turn in contents:
            if not isinstance(turn, dict):
                continue
            parts = turn.get("parts", [])
            if not isinstance(parts, list):
                continue
            for part in parts:
                if isinstance(part, dict) and isinstance(part.get("text"), str):
                    text_len += len(part["text"])
    # Approximate 3 chars/token (conservative)
    return text_len // 3


def _iter_batches(items: list[str], batch_size: int) -> list[list[str]]:
    if batch_size <= 0:
        msg = "gcs_batch_size must be > 0"
        raise ValueError(msg)
    return [items[i : i + batch_size] for i in range(0, len(items), batch_size)]


def _find_unreachable_gcs_uris(
    storage_client: storage.Client,
    uris: list[str],
    *,
    max_workers: int,
    batch_size: int,
    batch_pause_seconds: float,
) -> set[str]:
    if max_workers <= 0:
        msg = "gcs_max_workers must be > 0"
        raise ValueError(msg)
    if batch_pause_seconds < 0:
        msg = "gcs_batch_pause_seconds must be >= 0"
        raise ValueError(msg)
    if not uris:
        return set()

    unreachable: set[str] = set()
    batches = _iter_batches(uris, batch_size)
    worker_count = min(max_workers, len(uris))
    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        for batch_index, batch in enumerate(batches):
            flags = list(
                executor.map(partial(_safe_blob_exists, storage_client), batch)
            )
            unreachable.update(
                u for u, ok in zip(batch, flags, strict=True) if not ok
            )
            if batch_pause_seconds and batch_index < len(batches) - 1:
                time.sleep(batch_pause_seconds)
    return unreachable


def _extract_file_uris(example: dict[str, Any]) -> list[str]:
    uris: list[str] = []
    contents = example.get("contents", [])
    if not isinstance(contents, list):
        return uris
    for turn in contents:
        if not isinstance(turn, dict):
            continue
        parts = turn.get("parts", [])
        if not isinstance(parts, list):
            continue
        for part in parts:
            if not isinstance(part, dict):
                continue
            file_data = part.get("fileData")
            if isinstance(file_data, dict):
                uris.append(file_data.get("fileUri", ""))
    return uris


def _extract_target_file_uri(example: dict[str, Any]) -> str:
    uris = _extract_file_uris(example)
    return uris[-1] if uris else ""


def _check_examples(
    *,
    report: PreflightReport,
    split: str,
    examples: list[dict[str, Any]],
    system_prompt: str,
    user_prompt: str,
    storage_client: storage.Client | None,
    unreachable: set[str],
) -> None:
    for i, ex in enumerate(examples):
        ex_id = f"{split}[{i}]"
        if not validate_audio_tuning_example(ex):
            report.failures.append(
                f"{ex_id}: failed validate_audio_tuning_example "
                "(missing wrapper fields or empty target)"
            )
            if ex_id not in report.offending_ids:
                report.offending_ids.append(ex_id)
        # Token cap check (text portion only -- audio duration not available here)
        text_tokens = _estimate_text_tokens(ex, system_prompt, user_prompt)
        if text_tokens > PREFLIGHT_TOKEN_CAP:
            report.failures.append(
                f"{ex_id}: estimated text tokens {text_tokens} exceed cap {PREFLIGHT_TOKEN_CAP}"
            )
            if ex_id not in report.offending_ids:
                report.offending_ids.append(ex_id)
        # fileUri reachability check (only when storage_client is provided)
        if storage_client is not None:
            for uri in _extract_file_uris(ex):
                if uri and uri in unreachable:
                    report.failures.append(
                        f"{ex_id}: fileUri not reachable: {uri}"
                    )
                    if ex_id not in report.offending_ids:
                        report.offending_ids.append(ex_id)


def run_preflight(
    train_jsonl_path: Path,
    val_jsonl_path: Path | None,
    storage_client: storage.Client | None,
    report_path: Path,
    system_prompt: str = "",
    user_prompt: str = "",
    gcs_max_workers: int = PREFLIGHT_GCS_MAX_WORKERS,
    gcs_batch_size: int = PREFLIGHT_GCS_BATCH_SIZE,
    gcs_batch_pause_seconds: float = 0.0,
) -> PreflightReport:
    """Run all preflight checks. Write report + return result. Never mutates data.

    Checks:
    1. Non-empty train split (at least 1 example)
    2. If val provided: non-empty val split; disjoint train/val fileUris
    3. Per-example: local target-text contract, estimated token cap, and
       fileUri reachability
    4. Duplicate fileUri detection in train set
    """
    report = PreflightReport()
    report_path.parent.mkdir(parents=True, exist_ok=True)

    train_examples = _load_split_examples(train_jsonl_path, "train", report)
    if train_examples is None:
        _write_report(report, report_path)
        return report

    if not train_examples:
        report.failures.append(
            "Train JSONL is empty -- no examples to tune on."
        )
        _write_report(report, report_path)
        return report

    _check_duplicate_train_uris(train_examples, report)
    train_all_uris = _extract_all_file_uris(train_examples)
    val_examples, _ = _load_and_check_val_split(
        val_jsonl_path, train_all_uris, report
    )

    unreachable: set[str] = set()
    if storage_client is not None:
        # GCS reachability is network-bound and many examples can share the same
        # fileUri. Dedup here so large validation sets do not multiply metadata
        # calls for repeated audio.
        unique_uris = sorted(
            {
                u
                for u in [
                    *_extract_all_file_uris(train_examples),
                    *_extract_all_file_uris(val_examples),
                ]
                if u
            }
        )
        unreachable = _find_unreachable_gcs_uris(
            storage_client,
            unique_uris,
            max_workers=gcs_max_workers,
            batch_size=gcs_batch_size,
            batch_pause_seconds=gcs_batch_pause_seconds,
        )

    # The same schema/token/reachability checks run on validation examples:
    # malformed validation data can fail a paid Vertex job just like train data.
    _check_examples(
        report=report,
        split="train",
        examples=train_examples,
        system_prompt=system_prompt,
        user_prompt=user_prompt,
        storage_client=storage_client,
        unreachable=unreachable,
    )
    _check_examples(
        report=report,
        split="val",
        examples=val_examples,
        system_prompt=system_prompt,
        user_prompt=user_prompt,
        storage_client=storage_client,
        unreachable=unreachable,
    )

    _write_report(report, report_path)
    return report


def _load_split_examples(
    path: Path, split: str, report: PreflightReport
) -> list[dict[str, Any]] | None:
    try:
        with path.open(encoding="utf-8") as fh:
            return [json.loads(line) for line in fh if line.strip()]
    except Exception as exc:
        report.failures.append(f"Failed to load {split} JSONL: {exc}")
        return None


def _check_duplicate_train_uris(
    train_examples: list[dict[str, Any]], report: PreflightReport
) -> list[str]:
    train_uris: list[str] = []
    for ex in train_examples:
        train_uris.append(_extract_target_file_uri(ex))
    seen: set[str] = set()
    for uri in train_uris:
        if uri and uri in seen:
            report.failures.append(f"Duplicate fileUri in train: {uri}")
            if uri not in report.offending_ids:
                report.offending_ids.append(uri)
        if uri:
            seen.add(uri)
    return train_uris


def _extract_all_file_uris(examples: list[dict[str, Any]]) -> list[str]:
    uris: list[str] = []
    for ex in examples:
        uris.extend(_extract_file_uris(ex))
    return uris


def _load_and_check_val_split(
    val_jsonl_path: Path | None,
    train_uris: list[str],
    report: PreflightReport,
) -> tuple[list[dict[str, Any]], list[str]]:
    if val_jsonl_path is None:
        return [], []
    loaded = _load_split_examples(val_jsonl_path, "val", report)
    if loaded is None:
        return [], []
    val_examples = loaded
    if not val_examples:
        report.failures.append("Val JSONL is empty.")
    val_uris: list[str] = []
    for ex in val_examples:
        val_uris.append(_extract_target_file_uri(ex))
    overlap = {u for u in train_uris if u} & {u for u in val_uris if u}
    for uri in sorted(overlap):
        report.failures.append(
            f"fileUri in both train and val (not disjoint): {uri}"
        )
        if uri not in report.offending_ids:
            report.offending_ids.append(uri)
    return val_examples, val_uris


def _write_report(report: PreflightReport, report_path: Path) -> None:
    report_dict = {
        "passed": report.passed,
        "failures": report.failures,
        "offending_ids": report.offending_ids,
    }
    report_path.write_text(json.dumps(report_dict, indent=2), encoding="utf-8")
    if report.passed:
        logger.info(f"Preflight passed. Report: {report_path}")
    else:
        logger.error(
            f"Preflight FAILED ({len(report.failures)} issues). "
            f"Report: {report_path}. Fix the data and re-run."
        )
