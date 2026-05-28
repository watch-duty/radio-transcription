"""Data-quality preflight -- hard gate before any paid tune run.

On any violation, raise a clear error and stop before tune. The operator fixes
the data; there is no auto-filter escape hatch.
Checks include per-example token limit, empty/whitespace targets, duplicate
fileUris, fileUri reachability via blob_exists, and non-empty/disjoint train/val
splits.
"""

from __future__ import annotations

import json
import logging
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from pathlib import Path
from typing import Final

from common.gcs_utils import blob_exists
from common.sft import validate_example

logger = logging.getLogger(__name__)

PREFLIGHT_TOKEN_CAP: Final = (
    131_072  # VERIFIED: docs.cloud.google.com Gemini SFT docs
)
PREFLIGHT_GCS_MAX_WORKERS: Final = 16
PREFLIGHT_GCS_BATCH_SIZE: Final = 256
PREFLIGHT_GCS_TIMEOUT_SECONDS: Final = 30.0


def _blob_exists_with_timeout(storage_client: object, uri: str) -> bool:
    """``blob_exists`` with the preflight timeout boundary."""
    return blob_exists(
        storage_client,
        uri,
        timeout=PREFLIGHT_GCS_TIMEOUT_SECONDS,
    )


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


def _iter_batches(items: list[str], batch_size: int) -> list[list[str]]:
    if batch_size <= 0:
        raise ValueError("gcs_batch_size must be > 0")
    return [items[i : i + batch_size] for i in range(0, len(items), batch_size)]


def _check_gcs_uris_reachable(
    storage_client: object,
    uris: list[str],
    *,
    max_workers: int,
    batch_size: int,
) -> None:
    if max_workers <= 0:
        raise ValueError("gcs_max_workers must be > 0")
    if not uris:
        return

    batches = _iter_batches(uris, batch_size)
    worker_count = min(max_workers, len(uris))
    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        for batch in batches:
            flags = list(
                executor.map(
                    partial(_blob_exists_with_timeout, storage_client), batch
                )
            )
            unreachable = [
                uri for uri, ok in zip(batch, flags, strict=True) if not ok
            ]
            if len(unreachable) == 1:
                raise ValueError(f"fileUri not reachable: {unreachable[0]}")
            if unreachable:
                raise ValueError(
                    f"{len(unreachable)} fileUris not reachable; first: {unreachable[0]}"
                )


def _extract_file_uris(example: dict) -> list[str]:
    uris: list[str] = []
    parts = (example.get("contents") or [{}])[0].get("parts", [])
    for p in parts:
        if "fileData" in p:
            uris.append(p["fileData"].get("fileUri", ""))
    return uris


def _check_examples(
    *,
    split: str,
    examples: list[dict],
    system_prompt: str,
    user_prompt: str,
) -> None:
    for i, ex in enumerate(examples):
        ex_id = f"{split}[{i}]"
        if not validate_example(ex):
            raise ValueError(
                f"{ex_id}: failed validate_example (empty target or malformed schema)"
            )
        # Token cap check (text portion only -- audio duration not available here)
        text_tokens = _estimate_text_tokens(ex, system_prompt, user_prompt)
        if text_tokens > PREFLIGHT_TOKEN_CAP:
            raise ValueError(
                f"{ex_id}: estimated text tokens {text_tokens} exceed cap {PREFLIGHT_TOKEN_CAP}"
            )


def _load_jsonl(path: Path, split: str) -> list[dict]:
    examples: list[dict] = []
    with path.open(encoding="utf-8") as f:
        for line_no, line in enumerate(f, start=1):
            if not line.strip():
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(
                    f"{split} JSONL line {line_no} is not valid JSON: {exc.msg}"
                ) from exc
            if not isinstance(obj, dict):
                raise TypeError(
                    f"{split} JSONL line {line_no} must be a JSON object"
                )
            examples.append(obj)
    return examples


def run_preflight(
    train_jsonl_path: Path,
    val_jsonl_path: Path | None,
    storage_client: object | None,
    system_prompt: str = "",
    user_prompt: str = "",
    gcs_max_workers: int = PREFLIGHT_GCS_MAX_WORKERS,
    gcs_batch_size: int = PREFLIGHT_GCS_BATCH_SIZE,
) -> None:
    """Run all preflight checks. Raises on first failure. Never mutates data.

    Checks:
    1. Non-empty train split (at least 1 example)
    2. If val provided: non-empty val split; disjoint train/val fileUris
    3. Per-example: validate_example (empty target), estimated token cap, fileUri reachability
    4. Duplicate fileUri detection in train set
    """
    train_examples = _load_jsonl(train_jsonl_path, "train")

    # Check 1: non-empty train
    if not train_examples:
        raise ValueError(
            "Train JSONL is empty -- no examples to tune on."
        )

    # Check 3: per-example validate_example + token cap.
    _check_examples(
        split="train",
        examples=train_examples,
        system_prompt=system_prompt,
        user_prompt=user_prompt,
    )

    # Check 4: duplicate fileUris in train
    train_uris: list[str] = []
    for ex in train_examples:
        train_uris.extend(_extract_file_uris(ex))
    seen: set[str] = set()
    for uri in train_uris:
        if uri and uri in seen:
            raise ValueError(f"Duplicate fileUri in train: {uri}")
        if uri:
            seen.add(uri)

    # Load val examples if provided
    val_examples: list[dict] = []
    val_uris: list[str] = []
    if val_jsonl_path is not None:
        val_examples = _load_jsonl(val_jsonl_path, "val")
        if not val_examples:
            raise ValueError("Val JSONL is empty.")
        _check_examples(
            split="val",
            examples=val_examples,
            system_prompt=system_prompt,
            user_prompt=user_prompt,
        )
        for ex in val_examples:
            val_uris.extend(_extract_file_uris(ex))
        # Check 2: disjoint
        overlap = {u for u in train_uris if u} & {u for u in val_uris if u}
        for uri in sorted(overlap):
            raise ValueError(
                f"fileUri in both train and val (not disjoint): {uri}"
            )

    # Pre-compute unreachable fileUris in parallel (network-bound; dedup + thread pool)
    # and fail as soon as the bounded check finds any missing object.
    if storage_client is not None:
        unique_uris = sorted({u for u in [*train_uris, *val_uris] if u})
        _check_gcs_uris_reachable(
            storage_client,
            unique_uris,
            max_workers=gcs_max_workers,
            batch_size=gcs_batch_size,
        )

    logger.info("Preflight passed.")
