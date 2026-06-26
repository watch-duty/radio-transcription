"""Probe generation/context settings for checkpoint empty responses."""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
from collections import Counter
from dataclasses import dataclass
from datetime import UTC, datetime
from functools import cache
from pathlib import Path
from typing import Any

from common.gemini.context import (
    build_context_histories,
    build_transcript_context_prompt,
)
from common.gemini.vertex import (
    GEMINI_GENERATION_CONFIG,
    GEMINI_SAFETY_SETTINGS,
    build_request,
)
from common.scoring import build_normalizer, compute_cer, compute_wer
from gemini_sft.config import load_eval_run_config
from google import genai
from google.genai import types

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class Strategy:
    name: str
    history_mode: str = "text_turns"
    history_count: int = 8
    user_prompt: str | None = None
    safety: str = "block_none"
    temperature: float = 0.0
    top_p: float | None = None
    top_k: float | None = None
    max_output_tokens: int = 512
    response_mime_type: str | None = None
    response_modalities: list[str] | None = None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    parser.add_argument("--canonical-eval", required=True)
    parser.add_argument("--predictions", required=True)
    parser.add_argument("--endpoint", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--concurrency", type=int, default=8)
    parser.add_argument("--strategies", nargs="*", default=[])
    parser.add_argument("--save-predictions", action="store_true")
    return parser.parse_args()


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    return [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def _safety_settings(kind: str) -> list[types.SafetySetting] | list[dict[str, str]] | None:
    cats = [
        types.HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT,
        types.HarmCategory.HARM_CATEGORY_HARASSMENT,
        types.HarmCategory.HARM_CATEGORY_HATE_SPEECH,
        types.HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT,
    ]
    if kind == "block_none":
        return GEMINI_SAFETY_SETTINGS
    if kind == "off":
        return [
            types.SafetySetting(
                category=category,
                threshold=types.HarmBlockThreshold.OFF,
            )
            for category in cats
        ]
    if kind == "none":
        return None
    msg = f"unknown safety setting kind: {kind}"
    raise ValueError(msg)


def _config_for(run_cfg: Any, strategy: Strategy) -> types.GenerateContentConfig:
    kwargs: dict[str, Any] = {
        "system_instruction": run_cfg.system_prompt,
        "temperature": strategy.temperature,
        "max_output_tokens": strategy.max_output_tokens,
    }
    safety = _safety_settings(strategy.safety)
    if safety is not None:
        kwargs["safety_settings"] = safety
    if strategy.top_p is not None:
        kwargs["top_p"] = strategy.top_p
    if strategy.top_k is not None:
        kwargs["top_k"] = strategy.top_k
    if strategy.response_mime_type is not None:
        kwargs["response_mime_type"] = strategy.response_mime_type
    if strategy.response_modalities is not None:
        kwargs["response_modalities"] = strategy.response_modalities
    return types.GenerateContentConfig(**kwargs)


def _strategy_catalog(default_user_prompt: str) -> dict[str, Strategy]:
    force_prompt = (
        "COMMAND: Transcribe exactly the single audio clip attached to this "
        "message. Output the best verbatim transcript for this clip only. "
        "Do not output an empty response unless the clip contains no speech. "
        "Apply all CRITICAL RULES."
    )
    prior_plain_prompt = (
        "Prior transcript context only. No audio is attached to this turn."
    )
    return {
        "current_block_none": Strategy("current_block_none"),
        "safety_off": Strategy("safety_off", safety="off"),
        "no_safety": Strategy("no_safety", safety="none"),
        "text_plain": Strategy("text_plain", safety="off", response_mime_type="text/plain"),
        "response_text_modality": Strategy(
            "response_text_modality",
            safety="off",
            response_modalities=["TEXT"],
        ),
        "max2048": Strategy("max2048", safety="off", max_output_tokens=2048),
        "temp02": Strategy(
            "temp02", safety="off", temperature=0.2, top_p=0.95, top_k=40
        ),
        "temp04": Strategy(
            "temp04", safety="off", temperature=0.4, top_p=0.95, top_k=40
        ),
        "temp07": Strategy(
            "temp07", safety="off", temperature=0.7, top_p=0.95, top_k=40
        ),
        "temp10": Strategy(
            "temp10", safety="off", temperature=1.0, top_p=0.95, top_k=40
        ),
        "count0": Strategy("count0", safety="off", history_count=0),
        "count1": Strategy("count1", safety="off", history_count=1),
        "count2": Strategy("count2", safety="off", history_count=2),
        "count4": Strategy("count4", safety="off", history_count=4),
        "count6": Strategy("count6", safety="off", history_count=6),
        "transcript8": Strategy(
            "transcript8", safety="off", history_mode="transcript"
        ),
        "transcript4": Strategy(
            "transcript4",
            safety="off",
            history_mode="transcript",
            history_count=4,
        ),
        "force_current_prompt": Strategy(
            "force_current_prompt",
            safety="off",
            user_prompt=force_prompt,
        ),
        "prior_plain_prompt": Strategy(
            "prior_plain_prompt",
            safety="off",
            user_prompt=prior_plain_prompt,
        ),
    }


def _request_for(
    *,
    row: dict[str, Any],
    history: list[Any],
    run_cfg: Any,
    strategy: Strategy,
) -> dict[str, Any]:
    user_prompt = strategy.user_prompt or run_cfg.user_prompt
    if strategy.history_mode == "transcript":
        request = build_request(
            str(row["audio_filepath"]),
            system_prompt=run_cfg.system_prompt,
            user_prompt=build_transcript_context_prompt(history, user_prompt),
            history=[],
            history_mode="text_turns",
        )
    else:
        request = build_request(
            str(row["audio_filepath"]),
            system_prompt=run_cfg.system_prompt,
            user_prompt=user_prompt,
            history=history,
            history_mode=strategy.history_mode,
        )
    return request["request"]


async def _query_one(
    *,
    client: genai.Client,
    endpoint: str,
    row: dict[str, Any],
    history: list[Any],
    run_cfg: Any,
    strategy: Strategy,
    config: types.GenerateContentConfig,
    semaphore: asyncio.Semaphore,
) -> dict[str, Any]:
    request = _request_for(
        row=row, history=history, run_cfg=run_cfg, strategy=strategy
    )
    async with semaphore:
        try:
            response = await client.aio.models.generate_content(
                model=endpoint,
                contents=request["contents"],
                config=config,
            )
        except Exception as exc:
            return {
                "audio_filepath": row["audio_filepath"],
                "pred_text": "",
                "error": f"{type(exc).__name__}: {exc}",
                "finish_reasons": [],
            }
    dumped = response.model_dump(mode="json", by_alias=True, exclude_none=True)
    candidates = dumped.get("candidates") or []
    finish_reasons: list[str] = []
    texts: list[str] = []
    for candidate in candidates:
        finish_reasons.append(str(candidate.get("finishReason") or "UNKNOWN"))
        parts = (candidate.get("content") or {}).get("parts") or []
        texts.append(
            "".join(str(part.get("text") or "") for part in parts).strip()
        )
    text = (getattr(response, "text", None) or "").strip()
    if not text:
        text = next((candidate_text for candidate_text in texts if candidate_text), "")
    return {
        "audio_filepath": row["audio_filepath"],
        "pred_text": text,
        "error": None if text else "empty response",
        "finish_reasons": finish_reasons or ["NO_CANDIDATE"],
        "prompt_feedback": dumped.get("promptFeedback"),
    }


def _score(
    *,
    refs: list[str],
    hyps: list[str],
    normalizer: Any,
) -> dict[str, Any]:
    wer = compute_wer(refs, hyps, normalizer=normalizer)
    cer = compute_cer(refs, hyps, normalizer=normalizer)
    return {
        "wer": wer["wer"],
        "cer": cer["cer"],
        "insertions": wer["insertions"],
        "deletions": wer["deletions"],
        "substitutions": wer["substitutions"],
        "hits": wer["hits"],
    }


def _cached_normalizer(normalizer: Any) -> Any:
    @cache
    def normalize(text: str) -> str:
        return normalizer(text)

    class CachedNormalizer:
        def __call__(self, text: str) -> str:
            return normalize(text)

    return CachedNormalizer()


async def main_async(args: argparse.Namespace) -> dict[str, Any]:
    run_cfg = load_eval_run_config(args.config)
    rows = _read_jsonl(Path(args.canonical_eval))
    pred_rows = _read_jsonl(Path(args.predictions))
    pred_by_audio = {str(row["audio_filepath"]): row for row in pred_rows}
    histories = build_context_histories(rows, max_turns=8)
    empty_indices = [
        index
        for index, row in enumerate(rows)
        if (pred_by_audio.get(str(row["audio_filepath"])) or {}).get("error")
        == "empty response"
    ]
    if args.limit:
        empty_indices = empty_indices[: args.limit]
    catalog = _strategy_catalog(run_cfg.user_prompt)
    strategy_names = args.strategies or list(catalog)
    strategies = [catalog[name] for name in strategy_names]
    client = genai.Client(
        vertexai=True, project=run_cfg.gcp_project, location="us"
    )
    semaphore = asyncio.Semaphore(args.concurrency)
    normalizer = _cached_normalizer(build_normalizer())
    full_refs = [str(row.get("text") or "") for row in rows]
    primary_hyps = [
        str((pred_by_audio.get(str(row["audio_filepath"])) or {}).get("pred_text") or "")
        for row in rows
    ]
    empty_refs = [str(rows[index].get("text") or "") for index in empty_indices]

    result: dict[str, Any] = {
        "updated_at": datetime.now(UTC).isoformat(),
        "empty_rows": len(empty_indices),
        "strategies": {},
        "primary_full": _score(
            refs=full_refs, hyps=primary_hyps, normalizer=normalizer
        ),
    }
    for strategy in strategies:
        LOGGER.info("Running strategy %s on %s rows", strategy.name, len(empty_indices))
        config = _config_for(run_cfg, strategy)
        outputs = await asyncio.gather(
            *(
                _query_one(
                    client=client,
                    endpoint=args.endpoint,
                    row=rows[index],
                    history=histories[index][-strategy.history_count :]
                    if strategy.history_count
                    else [],
                    run_cfg=run_cfg,
                    strategy=strategy,
                    config=config,
                    semaphore=semaphore,
                )
                for index in empty_indices
            )
        )
        empty_hyps = [output["pred_text"] for output in outputs]
        fallback_hyps = list(primary_hyps)
        for index, output in zip(empty_indices, outputs, strict=True):
            if output["pred_text"]:
                fallback_hyps[index] = output["pred_text"]
        strategy_result = {
            "non_empty": sum(bool(output["pred_text"]) for output in outputs),
            "empty": sum(not output["pred_text"] for output in outputs),
            "errors": dict(
                Counter(
                    output["error"]
                    for output in outputs
                    if output.get("error") and output["error"] != "empty response"
                )
            ),
            "finish_reasons": dict(
                Counter("|".join(output["finish_reasons"]) for output in outputs)
            ),
            "empty_rows_score": _score(
                refs=empty_refs, hyps=empty_hyps, normalizer=normalizer
            ),
            "fallback_full_score": _score(
                refs=full_refs, hyps=fallback_hyps, normalizer=normalizer
            ),
            "examples": [
                {
                    "audio_filepath": rows[index]["audio_filepath"],
                    "dataset": rows[index].get("dataset_family"),
                    "history_count": len(histories[index]),
                    "duration": rows[index].get("duration"),
                    "ref": rows[index].get("text"),
                    "pred_text": output["pred_text"],
                    "finish_reasons": output["finish_reasons"],
                }
                for index, output in zip(empty_indices, outputs, strict=True)
                if output["pred_text"]
            ][:8],
        }
        if args.save_predictions:
            strategy_result["predictions"] = outputs
        result["strategies"][strategy.name] = strategy_result
    return result


def main() -> int:
    args = parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    for name in (
        "httpx",
        "httpcore",
        "google.auth.transport.requests",
        "google.genai",
        "google_genai",
        "google_genai.models",
    ):
        logging.getLogger(name).setLevel(logging.WARNING)
    result = asyncio.run(main_async(args))
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    compact = {
        "empty_rows": result["empty_rows"],
        "primary_full": result["primary_full"],
        "strategies": {
            name: {
                "non_empty": value["non_empty"],
                "empty": value["empty"],
                "empty_rows_wer": value["empty_rows_score"]["wer"],
                "fallback_full_wer": value["fallback_full_score"]["wer"],
                "fallback_full_cer": value["fallback_full_score"]["cer"],
                "errors": value["errors"],
            }
            for name, value in result["strategies"].items()
        },
    }
    print(json.dumps(compact, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
