"""Probe prompt variants for Gemini checkpoint empty responses."""

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

from common.gemini.context import ContextTurn, build_context_histories
from common.gemini.vertex import GEMINI_GENERATION_CONFIG, build_request
from common.scoring import build_normalizer, compute_cer, compute_wer
from gemini_sft.config import load_eval_run_config
from google import genai
from google.genai import types

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class PromptVariant:
    name: str
    system_prompt: str
    current_prompt: str
    prior_prompt: str
    mode: str = "text_turns"
    temperature: float = 0.0
    top_p: float | None = None
    top_k: float | None = None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    parser.add_argument("--canonical-eval", required=True)
    parser.add_argument("--predictions", required=True)
    parser.add_argument("--endpoint", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--concurrency", type=int, default=8)
    parser.add_argument("--variants", nargs="*", default=[])
    parser.add_argument("--save-predictions", action="store_true")
    parser.add_argument(
        "--scope",
        choices=["empty", "all"],
        default="empty",
        help="Rows to evaluate. Defaults to rows that were empty in predictions.",
    )
    return parser.parse_args()


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    return [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def _safety_off() -> list[types.SafetySetting]:
    categories = [
        types.HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT,
        types.HarmCategory.HARM_CATEGORY_HARASSMENT,
        types.HarmCategory.HARM_CATEGORY_HATE_SPEECH,
        types.HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT,
    ]
    return [
        types.SafetySetting(
            category=category,
            threshold=types.HarmBlockThreshold.OFF,
        )
        for category in categories
    ]


def _generation_config(variant: PromptVariant) -> types.GenerateContentConfig:
    kwargs: dict[str, Any] = {
        "system_instruction": variant.system_prompt,
        "safety_settings": _safety_off(),
        "temperature": variant.temperature,
        "max_output_tokens": int(GEMINI_GENERATION_CONFIG["max_output_tokens"]),
    }
    if variant.top_p is not None:
        kwargs["top_p"] = variant.top_p
    if variant.top_k is not None:
        kwargs["top_k"] = variant.top_k
    return types.GenerateContentConfig(**kwargs)


def _without_quality_gate(system_prompt: str) -> str:
    lines = []
    skip_next = False
    for line in system_prompt.splitlines():
        if line.startswith("5. QUALITY GATE:"):
            skip_next = False
            continue
        if not skip_next:
            lines.append(line)
    return "\n".join(lines).strip()


def _catalog(run_cfg: Any) -> dict[str, PromptVariant]:
    original = run_cfg.user_prompt
    current_direct = (
        "COMMAND: Transcribe only the audio clip attached to this final user "
        "message. Prior turns are transcript context only. Output strictly the "
        "best verbatim transcript for the attached clip."
    )
    current_direct_no_empty = (
        "COMMAND: Transcribe only the audio clip attached to this final user "
        "message. Prior turns are transcript context only. If the clip contains "
        "any audible speech, output the best verbatim transcript; do not return "
        "an empty response. Use [UNINTELLIGIBLE] only for unclear spans."
    )
    current_final_audio = (
        "IMPORTANT: The final user message is the only audio to transcribe. "
        "Use earlier model transcripts only as context. Return one line: the "
        "verbatim transcript of the final attached audio clip."
    )
    current_no_preceding_audio = (
        "Transcribe the attached audio clip verbatim. Earlier text turns are "
        "context only and contain no audio to transcribe. Output no explanation."
    )
    current_force_words = (
        "Transcribe the attached radio audio clip. Return the words spoken in "
        "this clip only. If speech is present, return words; do not stop before "
        "producing text."
    )
    prior_original = original
    prior_plain = "Prior transcript context only. No audio is attached to this turn."
    prior_context_only = (
        "CONTEXT TURN ONLY: no audio is attached here. The following model "
        "message is a previous transcript for context."
    )
    prior_no_task = "No transcription task in this turn; context transcript follows."
    system_no_quality = _without_quality_gate(run_cfg.system_prompt)
    system_short = (
        "You are a strict verbatim ASR system for VHF/UHF dispatch radio. "
        "Transcribe only the attached current audio clip. Use prior transcripts "
        "only as context for names, units, and terminology. Output only the "
        "transcript, with no explanation."
    )
    variants = [
        PromptVariant(
            "original_text_turns",
            run_cfg.system_prompt,
            original,
            prior_original,
        ),
        PromptVariant(
            "current_direct_prior_original",
            run_cfg.system_prompt,
            current_direct,
            prior_original,
        ),
        PromptVariant(
            "current_no_empty_prior_original",
            run_cfg.system_prompt,
            current_direct_no_empty,
            prior_original,
        ),
        PromptVariant(
            "current_final_audio_prior_original",
            run_cfg.system_prompt,
            current_final_audio,
            prior_original,
        ),
        PromptVariant(
            "current_no_preceding_prior_original",
            run_cfg.system_prompt,
            current_no_preceding_audio,
            prior_original,
        ),
        PromptVariant(
            "current_force_words_prior_original",
            run_cfg.system_prompt,
            current_force_words,
            prior_original,
        ),
        PromptVariant(
            "prior_plain_current_original",
            run_cfg.system_prompt,
            original,
            prior_plain,
        ),
        PromptVariant(
            "prior_plain_current_direct",
            run_cfg.system_prompt,
            current_direct,
            prior_plain,
        ),
        PromptVariant(
            "prior_plain_current_no_empty",
            run_cfg.system_prompt,
            current_direct_no_empty,
            prior_plain,
        ),
        PromptVariant(
            "prior_context_current_direct",
            run_cfg.system_prompt,
            current_direct,
            prior_context_only,
        ),
        PromptVariant(
            "prior_no_task_current_direct",
            run_cfg.system_prompt,
            current_direct,
            prior_no_task,
        ),
        PromptVariant(
            "system_short_prior_plain_current_direct",
            system_short,
            current_direct,
            prior_plain,
        ),
        PromptVariant(
            "system_no_quality_prior_plain_current_direct",
            system_no_quality,
            current_direct,
            prior_plain,
        ),
        PromptVariant(
            "block_original",
            run_cfg.system_prompt,
            original,
            prior_plain,
            mode="transcript_block",
        ),
        PromptVariant(
            "block_direct",
            run_cfg.system_prompt,
            current_direct,
            prior_plain,
            mode="transcript_block",
        ),
        PromptVariant(
            "block_no_empty",
            run_cfg.system_prompt,
            current_direct_no_empty,
            prior_plain,
            mode="transcript_block",
        ),
        PromptVariant(
            "block_system_short_direct",
            system_short,
            current_direct,
            prior_plain,
            mode="transcript_block",
        ),
        PromptVariant(
            "block_system_short_original",
            system_short,
            original,
            prior_plain,
            mode="transcript_block",
        ),
        PromptVariant(
            "block_system_short_no_empty",
            system_short,
            current_direct_no_empty,
            prior_plain,
            mode="transcript_block",
        ),
        PromptVariant(
            "block_system_short_final_audio",
            system_short,
            current_final_audio,
            prior_plain,
            mode="transcript_block",
        ),
        PromptVariant(
            "block_system_short_force_words",
            system_short,
            current_force_words,
            prior_plain,
            mode="transcript_block",
        ),
        PromptVariant(
            "block_system_short_no_preceding",
            system_short,
            current_no_preceding_audio,
            prior_plain,
            mode="transcript_block",
        ),
        PromptVariant(
            "block_system_short_force_words_temp02",
            system_short,
            current_force_words,
            prior_plain,
            mode="transcript_block",
            temperature=0.2,
            top_p=0.95,
            top_k=40,
        ),
        PromptVariant(
            "block_system_short_force_words_temp04",
            system_short,
            current_force_words,
            prior_plain,
            mode="transcript_block",
            temperature=0.4,
            top_p=0.95,
            top_k=40,
        ),
        PromptVariant(
            "block_system_short_force_words_temp07",
            system_short,
            current_force_words,
            prior_plain,
            mode="transcript_block",
            temperature=0.7,
            top_p=0.95,
            top_k=40,
        ),
    ]
    return {variant.name: variant for variant in variants}


def _make_request(
    row: dict[str, Any],
    history: list[ContextTurn],
    variant: PromptVariant,
) -> dict[str, Any]:
    if variant.mode == "transcript_block":
        if history:
            lines = [
                "Prior same-source transcripts for context only:",
                *[
                    f"{index}. {turn.text}"
                    for index, turn in enumerate(history, start=1)
                ],
                "",
                variant.current_prompt,
            ]
            current_prompt = "\n".join(lines)
        else:
            current_prompt = variant.current_prompt
        return build_request(
            str(row["audio_filepath"]),
            system_prompt=variant.system_prompt,
            user_prompt=current_prompt,
            history=[],
            history_mode="text_turns",
        )["request"]

    contents: list[dict[str, Any]] = []
    for turn in history:
        contents.extend(
            [
                {"role": "user", "parts": [{"text": variant.prior_prompt}]},
                {"role": "model", "parts": [{"text": turn.text}]},
            ]
        )
    current = build_request(
        str(row["audio_filepath"]),
        system_prompt=variant.system_prompt,
        user_prompt=variant.current_prompt,
        history=[],
        history_mode="text_turns",
    )["request"]["contents"][0]
    contents.append(current)
    return {
        "contents": contents,
        "system_instruction": {
            "role": "system",
            "parts": [{"text": variant.system_prompt}],
        },
    }


async def _query(
    *,
    client: genai.Client,
    endpoint: str,
    row: dict[str, Any],
    history: list[ContextTurn],
    variant: PromptVariant,
    semaphore: asyncio.Semaphore,
) -> dict[str, Any]:
    request = _make_request(row, history, variant)
    async with semaphore:
        try:
            response = await client.aio.models.generate_content(
                model=endpoint,
                contents=request["contents"],
                config=_generation_config(variant),
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


def _cached_normalizer(normalizer: Any) -> Any:
    @cache
    def normalize(text: str) -> str:
        return normalizer(text)

    class CachedNormalizer:
        def __call__(self, text: str) -> str:
            return normalize(text)

    return CachedNormalizer()


def _score(refs: list[str], hyps: list[str], normalizer: Any) -> dict[str, Any]:
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
    selected_indices = (
        list(range(len(rows))) if args.scope == "all" else empty_indices
    )
    if args.limit:
        selected_indices = selected_indices[: args.limit]
    catalog = _catalog(run_cfg)
    variant_names = args.variants or list(catalog)
    variants = [catalog[name] for name in variant_names]
    client = genai.Client(
        vertexai=True, project=run_cfg.gcp_project, location="us"
    )
    semaphore = asyncio.Semaphore(args.concurrency)
    normalizer = _cached_normalizer(build_normalizer())
    refs = [str(rows[index].get("text") or "") for index in selected_indices]
    result: dict[str, Any] = {
        "updated_at": datetime.now(UTC).isoformat(),
        "scope": args.scope,
        "empty_rows": len(empty_indices),
        "selected_rows": len(selected_indices),
        "variants": {},
    }
    for variant in variants:
        LOGGER.info(
            "Running prompt variant %s on %s rows",
            variant.name,
            len(selected_indices),
        )
        outputs = await asyncio.gather(
            *(
                _query(
                    client=client,
                    endpoint=args.endpoint,
                    row=rows[index],
                    history=histories[index],
                    variant=variant,
                    semaphore=semaphore,
                )
                for index in selected_indices
            )
        )
        hyps = [output["pred_text"] for output in outputs]
        variant_result = {
            "non_empty": sum(bool(text) for text in hyps),
            "empty": sum(not text for text in hyps),
            "score": _score(refs, hyps, normalizer),
            "finish_reasons": dict(
                Counter("|".join(output["finish_reasons"]) for output in outputs)
            ),
            "errors": dict(
                Counter(
                    output["error"]
                    for output in outputs
                    if output.get("error") and output["error"] != "empty response"
                )
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
                for index, output in zip(selected_indices, outputs, strict=True)
                if output["pred_text"]
            ][:8],
        }
        if args.save_predictions:
            variant_result["predictions"] = outputs
        result["variants"][variant.name] = variant_result
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
        "selected_rows": result["selected_rows"],
        "scope": result["scope"],
        "variants": {
            name: {
                "non_empty": value["non_empty"],
                "empty": value["empty"],
                "wer": value["score"]["wer"],
                "cer": value["score"]["cer"],
                "errors": value["errors"],
            }
            for name, value in result["variants"].items()
        },
    }
    print(json.dumps(compact, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
