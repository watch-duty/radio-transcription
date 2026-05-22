"""Data-quality preflight -- hard gate before any paid tune run.

D-13 (CONTEXT.md): On ANY violation, write a preflight report to report_path, exit non-zero,
do NOT proceed to tune. No --allow-filter escape hatch. Operator fixes the data.
D-14 checks: per-example token limit (131,072), empty/whitespace targets, duplicate fileUris,
fileUri reachability via blob_exists; pre-split: non-empty + disjoint train/val.
"""

from __future__ import annotations

import json
import logging
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from functools import partial
from pathlib import Path
from typing import Final

from common.gcs_utils import blob_exists
from common.sft import validate_example

logger = logging.getLogger(__name__)

PREFLIGHT_TOKEN_CAP: Final = 131_072  # VERIFIED: docs.cloud.google.com SFT docs
AUDIO_TOKENS_PER_SEC: Final = (
    32  # VERIFIED: ai.google.dev/gemini-api/docs/tokens
)


@dataclass
class PreflightReport:
    failures: list[str] = field(default_factory=list)
    offending_ids: list[str] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return len(self.failures) == 0


def _estimate_text_tokens(
    example: dict, system_prompt: str, user_prompt: str
) -> int:
    """Conservative token estimate for the text portion of an example.

    Audio duration is not known at preflight time (we only have the GCS URI,
    not audio content). Token cap is 131,072; for typical <30s clips the audio
    portion is <1,000 tokens. Text estimate catches pathologically long transcripts.
    """
    # Extract model text (ground truth)
    try:
        model_text = (
            (example.get("contents") or [{}, {}])[1]
            .get("parts", [{}])[0]
            .get("text", "")
        )
    except (IndexError, AttributeError):
        model_text = ""
    text_len = len(system_prompt) + len(user_prompt) + len(model_text)
    # Approximate 3 chars/token (conservative)
    return text_len // 3


def run_preflight(
    train_jsonl_path: Path,
    val_jsonl_path: Path | None,
    storage_client: object,
    report_path: Path,
    system_prompt: str = "",
    user_prompt: str = "",
) -> PreflightReport:
    """Run all preflight checks. Write report + return result. Never mutates data.

    Checks (D-14):
    1. Non-empty train split (at least 1 example)
    2. If val provided: non-empty val split; disjoint train/val fileUris
    3. Per-example: validate_example (empty target), estimated token cap, fileUri reachability
    4. Duplicate fileUri detection in train set
    """
    report = PreflightReport()
    report_path.parent.mkdir(parents=True, exist_ok=True)

    # Load train examples
    train_examples: list[dict] = []
    try:
        with train_jsonl_path.open() as tf:
            train_examples = [json.loads(line) for line in tf if line.strip()]
    except Exception as exc:
        report.failures.append(f"Failed to load train JSONL: {exc}")
        _write_report(report, report_path)
        return report

    # Check 1: non-empty train
    if not train_examples:
        report.failures.append(
            "Train JSONL is empty -- no examples to tune on."
        )
        _write_report(report, report_path)
        return report

    # Check 4: duplicate fileUris in train
    train_uris: list[str] = []
    for ex in train_examples:
        parts = (ex.get("contents") or [{}])[0].get("parts", [])
        for p in parts:
            if "fileData" in p:
                train_uris.append(p["fileData"].get("fileUri", ""))
    seen: set[str] = set()
    for uri in train_uris:
        if uri and uri in seen:
            report.failures.append(f"Duplicate fileUri in train: {uri}")
            if uri not in report.offending_ids:
                report.offending_ids.append(uri)
        if uri:
            seen.add(uri)

    # Pre-compute unreachable fileUris in parallel (network-bound; dedup + thread pool)
    # so the per-example reachability check below is a fast set-membership test.
    unreachable: set[str] = set()
    if storage_client is not None:
        unique_uris = sorted({u for u in train_uris if u})
        with ThreadPoolExecutor(max_workers=32) as executor:
            flags = list(
                executor.map(partial(blob_exists, storage_client), unique_uris)
            )
        unreachable = {
            u for u, ok in zip(unique_uris, flags, strict=True) if not ok
        }

    # Load val examples if provided
    val_uris: set[str] = set()
    if val_jsonl_path is not None:
        try:
            with val_jsonl_path.open() as vf:
                val_examples = [json.loads(line) for line in vf if line.strip()]
        except Exception as exc:
            report.failures.append(f"Failed to load val JSONL: {exc}")
            val_examples = []
        if not val_examples:
            report.failures.append("Val JSONL is empty.")
        for ex in val_examples:
            parts = (ex.get("contents") or [{}])[0].get("parts", [])
            for p in parts:
                if "fileData" in p:
                    val_uris.add(p["fileData"].get("fileUri", ""))
        # Check 2: disjoint
        overlap = set(train_uris) & val_uris
        for uri in sorted(overlap):
            report.failures.append(
                f"fileUri in both train and val (not disjoint): {uri}"
            )
            if uri not in report.offending_ids:
                report.offending_ids.append(uri)

    # Check 3: per-example validate_example + token cap + reachability
    for i, ex in enumerate(train_examples):
        ex_id = f"train[{i}]"
        if not validate_example(ex):
            report.failures.append(
                f"{ex_id}: failed validate_example (empty target or malformed schema)"
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
            parts = (ex.get("contents") or [{}])[0].get("parts", [])
            for p in parts:
                if "fileData" in p:
                    uri = p["fileData"].get("fileUri", "")
                    if uri and uri in unreachable:
                        report.failures.append(
                            f"{ex_id}: fileUri not reachable: {uri}"
                        )
                        if ex_id not in report.offending_ids:
                            report.offending_ids.append(ex_id)

    _write_report(report, report_path)
    return report


def _write_report(report: PreflightReport, report_path: Path) -> None:
    report_dict = {
        "passed": report.passed,
        "failures": report.failures,
        "offending_ids": report.offending_ids,
    }
    report_path.write_text(json.dumps(report_dict, indent=2))
    if report.passed:
        logger.info(f"Preflight passed. Report: {report_path}")
    else:
        logger.error(
            f"Preflight FAILED ({len(report.failures)} issues). "
            f"Report: {report_path}. Fix the data and re-run."
        )
