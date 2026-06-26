#!/usr/bin/env python3
"""Directly score prior-context SFT prompt families on Gemini 3.1 Flash-Lite.

This is the fallback prompt-selection gate when Vertex AI Prompt Optimizer
cannot target ``gemini-3.1-flash-lite``. It intentionally does not generate new
prompts. It evaluates the same prompt families prepared for VAPO against the
same training manifests using direct ``google-genai`` calls.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
from collections import Counter
from dataclasses import dataclass
from datetime import UTC, datetime
from functools import cache
from pathlib import Path
from typing import Any

from google import genai
from google.cloud import storage
from google.genai import types

SCRIPT_DIR = Path(__file__).resolve().parent
MODEL_DIR = SCRIPT_DIR.parents[1]
sys.path.insert(0, str(SCRIPT_DIR))
sys.path.insert(0, str(MODEL_DIR / "src"))

from common.gemini.context import build_context_histories  # noqa: E402
from common.gemini.vertex import (  # noqa: E402
    GEMINI_GENERATION_CONFIG,
    GEMINI_SAFETY_SETTINGS,
    build_request,
)
from common.scoring import (  # noqa: E402
    build_normalizer,
    compute_cer,
    compute_wer,
    hallucination_rate,
)
from run_prompt_optimizer_gate import (  # noqa: E402
    DEFAULT_CONTEXT_COUNT,
    DEFAULT_PROJECT,
    DEFAULT_TARGET_MODEL,
    DEFAULT_TARGET_MODEL_LOCATION,
    DEFAULT_TRAIN_MANIFESTS,
    PromptFamily,
    _load_many,
    _prompt_families,
    _select_families,
    _stratified_sample,
)

LOGGER = logging.getLogger(__name__)
DEFAULT_RUN_ID = "20260627-prior-context-count8-direct-prompt-gate"


@dataclass(frozen=True)
class Prediction:
    audio_filepath: str
    pred_text: str
    error: str | None
    finish_reasons: list[str]
    prompt_feedback: Any


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project", default=DEFAULT_PROJECT)
    parser.add_argument("--location", default=DEFAULT_TARGET_MODEL_LOCATION)
    parser.add_argument("--model", default=DEFAULT_TARGET_MODEL)
    parser.add_argument("--sample-size", type=int, default=120)
    parser.add_argument("--context-count", type=int, default=DEFAULT_CONTEXT_COUNT)
    parser.add_argument("--seed", type=int, default=20260627)
    parser.add_argument("--concurrency", type=int, default=24)
    parser.add_argument("--max-retries", type=int, default=1)
    parser.add_argument("--retry-sleep-seconds", type=float, default=2.0)
    parser.add_argument("--log-every", type=int, default=25)
    parser.add_argument("--families", nargs="*", default=None)
    parser.add_argument("--save-predictions", action="store_true")
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("results") / DEFAULT_RUN_ID / "prompt_family_scores.json",
    )
    parser.add_argument(
        "--train-manifest",
        action="append",
        dest="train_manifests",
        default=None,
        help="Override training manifests. May be passed multiple times.",
    )
    args = parser.parse_args()
    if args.sample_size <= 0:
        raise ValueError("--sample-size must be positive")
    if args.context_count < 0:
        raise ValueError("--context-count must be non-negative")
    if args.concurrency <= 0:
        raise ValueError("--concurrency must be positive")
    if args.max_retries <= 0:
        raise ValueError("--max-retries must be positive")
    if "gemini-3.1-flash-lite" not in args.model:
        raise ValueError(
            "This direct prompt gate is intentionally restricted to "
            "gemini-3.1-flash-lite."
        )
    return args


def _generation_config(
    family: PromptFamily,
) -> types.GenerateContentConfig:
    return types.GenerateContentConfig(
        system_instruction=family.system_instruction,
        safety_settings=GEMINI_SAFETY_SETTINGS,
        temperature=float(GEMINI_GENERATION_CONFIG["temperature"]),
        max_output_tokens=int(GEMINI_GENERATION_CONFIG["max_output_tokens"]),
    )


async def _generate_one(
    *,
    client: genai.Client,
    model: str,
    contents: list[dict[str, Any]],
    config: types.GenerateContentConfig,
    max_retries: int,
    retry_sleep_seconds: float,
) -> tuple[str, str | None, list[str], Any]:
    last_error = "unknown error"
    finish_reasons: list[str] = []
    prompt_feedback = None
    for attempt in range(1, max_retries + 1):
        try:
            response = await client.aio.models.generate_content(
                model=model,
                contents=contents,
                config=config,
            )
        except Exception as exc:
            last_error = f"{type(exc).__name__}: {exc}"
            if attempt < max_retries:
                await asyncio.sleep(retry_sleep_seconds * attempt)
            continue

        dumped = response.model_dump(mode="json", by_alias=True, exclude_none=True)
        candidates = dumped.get("candidates") or []
        finish_reasons = [
            str(candidate.get("finishReason") or "UNKNOWN")
            for candidate in candidates
        ]
        prompt_feedback = dumped.get("promptFeedback")
        text = (getattr(response, "text", None) or "").strip()
        if not text:
            for candidate in candidates:
                parts = (candidate.get("content") or {}).get("parts") or []
                text = "".join(str(part.get("text") or "") for part in parts).strip()
                if text:
                    break
        if text:
            return text, None, finish_reasons or ["UNKNOWN"], prompt_feedback

        last_error = "empty response"
        if attempt < max_retries:
            await asyncio.sleep(retry_sleep_seconds * attempt)
    return "", last_error, finish_reasons or ["NO_CANDIDATE"], prompt_feedback


async def _score_family(
    *,
    client: genai.Client,
    family: PromptFamily,
    samples: list[tuple[dict[str, Any], list[Any]]],
    args: argparse.Namespace,
) -> dict[str, Any]:
    semaphore = asyncio.Semaphore(args.concurrency)
    config = _generation_config(family)
    progress = {"done": 0, "errors": 0}
    lock = asyncio.Lock()

    async def process_one(
        row: dict[str, Any], history: list[Any]
    ) -> Prediction:
        request = build_request(
            str(row["audio_filepath"]),
            system_prompt=family.system_instruction,
            user_prompt=family.current_prompt,
            history=history,
            history_mode="text_turns",
        )["request"]
        async with semaphore:
            pred_text, error, finish_reasons, prompt_feedback = await _generate_one(
                client=client,
                model=args.model,
                contents=request["contents"],
                config=config,
                max_retries=args.max_retries,
                retry_sleep_seconds=args.retry_sleep_seconds,
            )
        async with lock:
            progress["done"] += 1
            if error:
                progress["errors"] += 1
            if (
                progress["done"] == len(samples)
                or (
                    args.log_every > 0
                    and progress["done"] % args.log_every == 0
                )
            ):
                LOGGER.info(
                    "family=%s progress=%s/%s errors=%s",
                    family.family_id,
                    progress["done"],
                    len(samples),
                    progress["errors"],
                )
        return Prediction(
            audio_filepath=str(row["audio_filepath"]),
            pred_text=pred_text,
            error=error,
            finish_reasons=finish_reasons,
            prompt_feedback=prompt_feedback,
        )

    outputs = await asyncio.gather(
        *(process_one(row, history) for row, history in samples)
    )
    refs = [str(row.get("text") or "") for row, _ in samples]
    hyps = [output.pred_text for output in outputs]
    score = _score(refs, hyps)
    result: dict[str, Any] = {
        "family_id": family.family_id,
        "description": family.description,
        "system_instruction": family.system_instruction,
        "current_prompt": family.current_prompt,
        "history_mode": "text_turns",
        "prior_turn_audio": False,
        "sample_rows": len(samples),
        "non_empty": sum(bool(hyp.strip()) for hyp in hyps),
        "empty": sum(not hyp.strip() for hyp in hyps),
        "empty_rate": hallucination_rate(hyps),
        "finish_reasons": dict(
            Counter("|".join(output.finish_reasons) for output in outputs)
        ),
        "errors": dict(Counter(output.error for output in outputs if output.error)),
        "score": score,
    }
    if args.save_predictions:
        result["predictions"] = [
            {
                "audio_filepath": output.audio_filepath,
                "pred_text": output.pred_text,
                "error": output.error,
                "finish_reasons": output.finish_reasons,
                "prompt_feedback": output.prompt_feedback,
            }
            for output in outputs
        ]
    return result


@cache
def _normalizer() -> Any:
    return build_normalizer()


def _score(refs: list[str], hyps: list[str]) -> dict[str, Any]:
    wer = compute_wer(refs, hyps, normalizer=_normalizer())
    cer = compute_cer(refs, hyps, normalizer=_normalizer())
    return {
        "wer": wer["wer"],
        "cer": cer["cer"],
        "insertions": wer["insertions"],
        "deletions": wer["deletions"],
        "substitutions": wer["substitutions"],
        "hits": wer["hits"],
    }


def _dataset_counts(samples: list[tuple[dict[str, Any], list[Any]]]) -> dict[str, int]:
    counts: Counter[str] = Counter()
    for row, _ in samples:
        counts[str(row.get("dataset_name") or row.get("dataset_family") or "unknown")] += 1
    return dict(counts)


async def run_async(args: argparse.Namespace) -> dict[str, Any]:
    storage_client = storage.Client(project=args.project)
    rows = [
        row
        for row in _load_many(
            storage_client, args.train_manifests or DEFAULT_TRAIN_MANIFESTS
        )
        if row.get("audio_filepath") and row.get("text")
    ]
    histories = build_context_histories(rows, max_turns=args.context_count)
    samples = _stratified_sample(rows, histories, args.sample_size, seed=args.seed)
    families = _select_families(args.families)
    client = genai.Client(
        vertexai=True,
        project=args.project,
        location=args.location,
    )
    summary: dict[str, Any] = {
        "run_id": args.output.parent.name,
        "updated_at": datetime.now(UTC).isoformat(),
        "project": args.project,
        "location": args.location,
        "model": args.model,
        "context_count": args.context_count,
        "history_mode": "text_turns",
        "prior_turn_audio": False,
        "sample_rows": len(samples),
        "dataset_counts": _dataset_counts(samples),
        "train_manifests": args.train_manifests or DEFAULT_TRAIN_MANIFESTS,
        "families": [],
    }
    for family in families:
        LOGGER.info("Scoring prompt family %s", family.family_id)
        summary["families"].append(
            await _score_family(
                client=client,
                family=family,
                samples=samples,
                args=args,
            )
        )
        summary["families"].sort(
            key=lambda item: (
                float(item["score"]["wer"]),
                float(item["empty_rate"]),
                float(item["score"]["cer"]),
            )
        )
        _write_summary(args.output, summary)
    return summary


def _write_summary(path: Path, summary: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(summary, indent=2, ensure_ascii=False) + "\n")
    md_path = path.with_suffix(".md")
    lines = [
        "# Direct Prompt-Family Scores",
        "",
        f"- model: `{summary['model']}`",
        f"- location: `{summary['location']}`",
        f"- sample rows: `{summary['sample_rows']}`",
        f"- history mode: `{summary['history_mode']}`",
        f"- prior turn audio: `{summary['prior_turn_audio']}`",
        "",
        "| family | WER | CER | empty_rate | empty | non_empty |",
        "|---|---:|---:|---:|---:|---:|",
    ]
    for family in summary["families"]:
        lines.append(
            "| {family_id} | {wer:.2f} | {cer:.2f} | {empty_rate:.2f} | "
            "{empty} | {non_empty} |".format(
                family_id=family["family_id"],
                wer=family["score"]["wer"],
                cer=family["score"]["cer"],
                empty_rate=family["empty_rate"],
                empty=family["empty"],
                non_empty=family["non_empty"],
            )
        )
    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    args = parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    for logger_name in (
        "httpcore",
        "httpx",
        "google.auth.transport.requests",
        "google.genai",
        "google_genai",
        "google_genai.models",
    ):
        logging.getLogger(logger_name).setLevel(logging.WARNING)
    summary = asyncio.run(run_async(args))
    best = summary["families"][0]
    print(
        json.dumps(
            {
                "best_family": best["family_id"],
                "wer": best["score"]["wer"],
                "cer": best["score"]["cer"],
                "empty_rate": best["empty_rate"],
                "output": str(args.output),
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
