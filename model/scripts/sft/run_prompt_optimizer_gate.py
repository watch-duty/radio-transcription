#!/usr/bin/env python3
"""Prepare and launch Vertex AI Prompt Optimizer jobs for prior-context SFT.

This script implements the prompt-optimization gate for the next Gemini SFT
round. It builds a deterministic train-manifest sample, writes one VAPO config
per prompt family, uploads the inputs/configs to GCS, and can optionally submit
the Vertex custom jobs.
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import random
import sys
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from google.cloud import storage

MODEL_DIR = Path(__file__).resolve().parents[2]
REPO_DIR = MODEL_DIR.parent
sys.path.insert(0, str(MODEL_DIR / "src"))

from common.gcs_utils import parse_gcs_uri, upload_text  # noqa: E402
from common.gemini.context import build_context_histories  # noqa: E402
from common.gemini.prompts import (  # noqa: E402
    GEMINI_TRANSCRIBE_SYSTEM_PROMPT,
    GEMINI_TRANSCRIBE_USER_PROMPT,
)
from common.prompts import COMMON_SYSTEM_PROMPT, COMMON_USER_INSTRUCTION  # noqa: E402

DEFAULT_PROJECT = "automatic-hawk-481415-m9"
DEFAULT_PROJECT_NUMBER = "781667204380"
DEFAULT_LOCATION = "us-central1"
DEFAULT_TARGET_MODEL = "gemini-3.1-flash-lite"
DEFAULT_TARGET_MODEL_LOCATION = "us"
DEFAULT_RUN_ID = "20260627-prior-context-count8-prompt-optimizer"
DEFAULT_OUTPUT_PREFIX = (
    "gs://wd-transcription-data/sft/experiments/"
    "gemini-prior-context-sft-v20260625/prompt_optimizer/"
    f"{DEFAULT_RUN_ID}"
)
DEFAULT_METRIC_NAME = "asr_wer_score"
DEFAULT_METRIC_FUNCTION = "wd-asr-wer-score"
DEFAULT_METRIC_FUNCTION_LOCATION = DEFAULT_LOCATION
DEFAULT_SAMPLE_SIZE = 600
DEFAULT_DATA_LIMIT = 100
DEFAULT_CONTEXT_COUNT = 8

DEFAULT_TRAIN_MANIFESTS = [
    "gs://wd-transcription-data/sft/dataset_versions/"
    "radio-transcription-sft-v20260528/manifests/per_dataset/"
    "bcfy_calls/train.jsonl",
    "gs://wd-transcription-data/sft/dataset_versions/"
    "radio-transcription-sft-v20260528/manifests/per_dataset/"
    "bcfy_feeds/train.jsonl",
    "gs://wd-transcription-data/sft/dataset_versions/"
    "radio-transcription-sft-v20260528/manifests/per_dataset/echo/train.jsonl",
    "gs://wd-transcription-data/sft/dataset_versions/"
    "radio-transcription-sft-v20260528/manifests/per_dataset/"
    "fire_notifications/train.jsonl",
]

SHORT_STRICT_ASR_SYSTEM = (
    "You are a strict verbatim ASR system for VHF/UHF dispatch radio. "
    "Transcribe only the attached current audio clip. Use prior transcripts "
    "only as context for names, units, and terminology. Output only the "
    "transcript, with no explanation."
)

FORCE_WORDS_CURRENT_PROMPT = (
    "Transcribe the attached radio audio clip. Return the words spoken in "
    "this clip only. If speech is present, return words; do not stop before "
    "producing text."
)

FINAL_AUDIO_CURRENT_PROMPT = (
    "IMPORTANT: The current audio is the only clip to transcribe. Use prior "
    "transcripts only as context. Return one line: the verbatim transcript of "
    "the attached radio audio clip."
)

NO_EMPTY_CURRENT_PROMPT = (
    "Transcribe only the attached current radio audio clip. If the clip "
    "contains audible speech, output the best verbatim transcript; do not "
    "return an empty response. Use [UNINTELLIGIBLE] only for unclear spans."
)

OUTPUT_ONLY_SYSTEM = (
    "You are a strict speech-to-text transcriber for emergency radio. Output "
    "only the transcribed words. Do not add pleasantries, do not offer help, "
    "and do not explain the audio quality."
)

FORMAT_FIRST_CURRENT_PROMPT = (
    "Transcribe the following speech segment in its original language. Only "
    "output the transcription, with no newlines. When transcribing numbers, "
    "write digits instead of number words."
)

CANARY_CURRENT_PROMPT = "Transcribe the following emergency dispatch radio traffic."

TRANSCRIPT_CONTEXT_CURRENT_PROMPT = (
    "CURRENT TASK:\n"
    "Transcribe only the attached audio clip.\n"
    "Output strictly the transcript for the current clip.\n"
    "Apply all critical rules from the system instruction."
)

GENERIC_CONTEXT_HEADER = "\n".join(
    [
        "Prior same-source transcripts for context only.",
        "Do not re-transcribe, copy, or continue them.",
        "Use them only for recurring units, locations, and jargon.",
        "",
        "Prior transcripts, oldest to newest:",
    ]
)

ERROR_AWARE_CONTEXT_HEADER = "\n".join(
    [
        "PRIOR TRANSCRIPTS FOR CONTEXT ONLY.",
        "They may contain transcription errors.",
        "Do not re-transcribe, copy, or continue them.",
        "Use them only for recurring units, locations, and jargon.",
        "",
        "Prior transcripts, oldest to newest:",
    ]
)

MANUAL_CONTEXT_HEADER = "\n".join(
    [
        "The following prior same-source transcripts are for situational "
        "awareness only.",
        "Do not re-transcribe them. Do not continue them.",
        "Transcribe exclusively the current audio clip.",
        "",
        "Prior transcripts, oldest to newest:",
    ]
)


@dataclass(frozen=True)
class PromptFamily:
    family_id: str
    description: str
    system_instruction: str
    current_prompt: str
    context_header: str
    optimization_mode: str = "instruction"

    @property
    def prompt_template(self) -> str:
        prior_context_block = "\n".join([self.context_header, "{prior_context}"])
        return "\n\n".join(
            [
                prior_context_block,
                self.current_prompt,
                "Current audio clip:",
                "{audio} @@@audio/flac",
            ]
        )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "action",
        choices=("prepare", "launch", "prepare-and-launch"),
        help="Prepare artifacts, launch existing configs, or do both.",
    )
    parser.add_argument("--project", default=DEFAULT_PROJECT)
    parser.add_argument("--project-number", default=DEFAULT_PROJECT_NUMBER)
    parser.add_argument("--location", default=DEFAULT_LOCATION)
    parser.add_argument("--target-model", default=DEFAULT_TARGET_MODEL)
    parser.add_argument(
        "--target-model-location",
        default=DEFAULT_TARGET_MODEL_LOCATION,
        help="Vertex location used for target model inference inside VAPO.",
    )
    parser.add_argument("--output-prefix", default=DEFAULT_OUTPUT_PREFIX)
    parser.add_argument("--sample-size", type=int, default=DEFAULT_SAMPLE_SIZE)
    parser.add_argument("--data-limit", type=int, default=DEFAULT_DATA_LIMIT)
    parser.add_argument("--context-count", type=int, default=DEFAULT_CONTEXT_COUNT)
    parser.add_argument("--seed", type=int, default=20260627)
    parser.add_argument("--metric-name", default=DEFAULT_METRIC_NAME)
    parser.add_argument("--metric-function", default=DEFAULT_METRIC_FUNCTION)
    parser.add_argument(
        "--metric-function-location",
        default=DEFAULT_METRIC_FUNCTION_LOCATION,
        help="Cloud Functions region for the custom VAPO metric endpoint.",
    )
    parser.add_argument(
        "--metric-mode",
        choices=("custom", "bleu", "bleu_rouge"),
        default="custom",
    )
    parser.add_argument("--optimization-mode", default=None)
    parser.add_argument("--target-model-qps", type=float, default=3.0)
    parser.add_argument("--eval-qps", type=float, default=3.0)
    parser.add_argument(
        "--optimizer-model-location",
        default=None,
        help="Optional VAPO optimizer_model_location override.",
    )
    parser.add_argument("--thinking-budget", type=int, default=0)
    parser.add_argument(
        "--target-model-harm-block-threshold",
        default=None,
        help=(
            "Optional VAPO target_model_harm_block_threshold override, for "
            "example OFF."
        ),
    )
    parser.add_argument("--num-steps", type=int, default=10)
    parser.add_argument(
        "--dataset-shape",
        choices=("prior_context", "pr800_plain", "pr800_marker"),
        default="prior_context",
        help=(
            "VAPO input/template shape. prior_context is the count-8 SFT "
            "gate shape; pr800_* reproduces PR 800's audio-only shape."
        ),
    )
    parser.add_argument("--families", nargs="*", default=None)
    parser.add_argument("--wait", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--local-output-dir",
        type=Path,
        default=Path("results") / DEFAULT_RUN_ID,
    )
    parser.add_argument(
        "--service-account",
        default=None,
        help="Explicit custom-job service account. Defaults to project-number compute service account.",
    )
    parser.add_argument(
        "--train-manifest",
        action="append",
        dest="train_manifests",
        default=None,
        help="Override train manifests. May be passed multiple times.",
    )
    args = parser.parse_args()

    if args.sample_size <= 0:
        raise ValueError("--sample-size must be positive")
    if not 5 <= args.data_limit <= 100:
        raise ValueError("--data-limit must be between 5 and 100")

    storage_client = storage.Client(project=args.project)
    prepared = None
    if args.action in {"prepare", "prepare-and-launch"}:
        prepared = prepare_optimizer_artifacts(storage_client, args)
    if args.action in {"launch", "prepare-and-launch"}:
        launch_optimizer_jobs(storage_client, args, prepared)
    return 0


def prepare_optimizer_artifacts(
    storage_client: storage.Client, args: argparse.Namespace
) -> dict[str, Any]:
    families = _select_families(args.families)
    rows = _load_many(storage_client, args.train_manifests or DEFAULT_TRAIN_MANIFESTS)
    histories = build_context_histories(rows, max_turns=args.context_count)
    samples = _stratified_sample(rows, histories, args.sample_size, seed=args.seed)

    local_dir = args.local_output_dir
    local_dir.mkdir(parents=True, exist_ok=True)

    output_prefix = args.output_prefix.rstrip("/")
    sample_uri = f"{output_prefix}/sample_prompts.jsonl"
    sample_text = "".join(
        json.dumps(_sample_record(row, history, args), ensure_ascii=False)
        + "\n"
        for row, history in samples
    )
    (local_dir / "sample_prompts.jsonl").write_text(sample_text, encoding="utf-8")
    if not args.dry_run:
        upload_text(
            storage_client,
            sample_text,
            sample_uri,
            content_type="application/jsonl",
        )

    family_records: list[dict[str, Any]] = []
    for family in families:
        config = _vapo_config(args, family, sample_uri)
        config_uri = f"{output_prefix}/configs/{family.family_id}.json"
        config_text = json.dumps(config, indent=2, ensure_ascii=False) + "\n"
        config_path = local_dir / "configs" / f"{family.family_id}.json"
        config_path.parent.mkdir(parents=True, exist_ok=True)
        config_path.write_text(config_text, encoding="utf-8")
        if not args.dry_run:
            upload_text(
                storage_client,
                config_text,
                config_uri,
                content_type="application/json",
            )
        family_records.append(
            {
                "family_id": family.family_id,
                "description": family.description,
                "config_uri": config_uri,
                "output_path": config["output_path"],
                "optimization_mode": config["optimization_mode"],
            }
        )

    manifest = {
        "run_id": Path(output_prefix).name,
        "output_prefix": output_prefix,
        "sample_uri": sample_uri,
        "sample_rows": len(samples),
        "data_limit": args.data_limit,
        "target_model": args.target_model,
        "optimizer_job_location": args.location,
        "target_model_location": args.target_model_location,
        "metric_mode": args.metric_mode,
        "metric_name": args.metric_name,
        "metric_function": (
            args.metric_function if args.metric_mode == "custom" else None
        ),
        "metric_function_location": (
            args.metric_function_location
            if args.metric_mode == "custom"
            else None
        ),
        "dataset_shape": args.dataset_shape,
        "families": family_records,
    }
    manifest_text = json.dumps(manifest, indent=2, ensure_ascii=False) + "\n"
    (local_dir / "manifest.json").write_text(manifest_text, encoding="utf-8")
    if not args.dry_run:
        upload_text(
            storage_client,
            manifest_text,
            f"{output_prefix}/manifest.json",
            content_type="application/json",
        )
    print(json.dumps(manifest, indent=2))
    return manifest


def launch_optimizer_jobs(
    storage_client: storage.Client,
    args: argparse.Namespace,
    prepared: dict[str, Any] | None,
) -> None:
    if prepared is None:
        manifest_uri = f"{args.output_prefix.rstrip('/')}/manifest.json"
        prepared = _download_json(storage_client, manifest_uri)
    families = prepared.get("families")
    if not isinstance(families, list) or not families:
        raise ValueError("Prepared manifest has no families to launch")

    import vertexai
    from vertexai import types

    client = vertexai.Client(project=args.project, location=args.location)
    service_account = args.service_account
    for family in families:
        family_id = str(family["family_id"])
        config_uri = str(family["config_uri"])
        display_name = f"{Path(args.output_prefix.rstrip('/')).name}-{family_id}"
        launch_config: dict[str, Any] = {
            "config_path": config_uri,
            "wait_for_completion": args.wait,
            "optimizer_job_display_name": display_name,
        }
        if service_account:
            launch_config["service_account"] = service_account
        else:
            launch_config["service_account_project_number"] = str(args.project_number)
        print(f"launching {family_id}: {config_uri}")
        if args.dry_run:
            print(json.dumps(launch_config, indent=2))
            continue
        job = client.prompts.launch_optimization_job(
            method=types.PromptOptimizerMethod.VAPO,
            config=launch_config,
        )
        print(job.model_dump_json(indent=2, exclude_none=True))


def _vapo_config(
    args: argparse.Namespace, family: PromptFamily, sample_uri: str
) -> dict[str, Any]:
    optimization_mode = args.optimization_mode or family.optimization_mode
    config: dict[str, Any] = {
        "project": args.project,
        "system_instruction": family.system_instruction,
        "prompt_template": _prompt_template(args, family),
        "target_model": args.target_model,
        "thinking_budget": args.thinking_budget,
        "optimization_mode": optimization_mode,
        "input_data_path": sample_uri,
        "output_path": f"{args.output_prefix.rstrip('/')}/outputs/{family.family_id}/",
        "target_model_location": args.target_model_location,
        "target_model_qps": args.target_model_qps,
        "eval_qps": args.eval_qps,
        "response_mime_type": "text/plain",
        "language": "English",
        "data_limit": args.data_limit,
        "num_steps": args.num_steps,
    }
    if args.target_model_harm_block_threshold:
        config["target_model_harm_block_threshold"] = (
            args.target_model_harm_block_threshold
        )
    if args.optimizer_model_location:
        config["optimizer_model_location"] = args.optimizer_model_location
    if args.metric_mode == "custom":
        config.update(
            {
                "eval_metric": "custom_metric",
                "custom_metric_name": args.metric_name,
                "custom_metric_cloud_function_project": args.project,
                "custom_metric_cloud_function_location": args.metric_function_location,
                "custom_metric_cloud_function_name": args.metric_function,
            }
        )
    elif args.metric_mode == "bleu":
        config["eval_metric"] = "bleu"
    else:
        config.update(
            {
                "eval_metrics_types": ["bleu", "rouge_l"],
                "eval_metrics_weights": [0.5, 0.5],
            }
        )
    return config


def _prompt_template(args: argparse.Namespace, family: PromptFamily) -> str:
    if args.dataset_shape == "pr800_plain":
        return "{input_text}\n{target}"
    if args.dataset_shape == "pr800_marker":
        return "{input_text} @@@audio/flac\n{target}"
    if args.metric_mode != "custom":
        return "\n".join([family.prompt_template, "{target}"])
    return family.prompt_template


def _sample_record(
    row: dict[str, Any], history: list[Any], args: argparse.Namespace
) -> dict[str, Any]:
    if args.dataset_shape in {"pr800_plain", "pr800_marker"}:
        return {
            "input_text": str(row["audio_filepath"]),
            "target": str(row["text"]),
        }
    return {
        "audio": str(row["audio_filepath"]),
        "target": str(row["text"]),
        "prior_context": _format_prior_context(history),
        "dataset": _dataset_slug(row),
        "duration": float(row.get("duration") or 0.0),
        "history_count": len(history),
    }


def _format_prior_context(history: list[Any]) -> str:
    if not history:
        return "There are no prior transcripts for this original recording."
    return "\n".join(
        f"{index}. {' '.join(turn.text.split())}"
        for index, turn in enumerate(history, 1)
    )


def _stratified_sample(
    rows: list[dict[str, Any]],
    histories: list[list[Any]],
    sample_size: int,
    *,
    seed: int,
) -> list[tuple[dict[str, Any], list[Any]]]:
    rng = random.Random(seed)
    groups: dict[tuple[str, str, str], list[int]] = defaultdict(list)
    for index, row in enumerate(rows):
        groups[_sample_group(row, histories[index])].append(index)

    for indices in groups.values():
        rng.shuffle(indices)

    selected: list[int] = []
    group_keys = sorted(groups)
    cursor = 0
    while len(selected) < sample_size and group_keys:
        key = group_keys[cursor % len(group_keys)]
        bucket = groups[key]
        if bucket:
            selected.append(bucket.pop())
        if not bucket:
            group_keys.remove(key)
            cursor = 0
        else:
            cursor += 1
    return [(rows[index], histories[index]) for index in selected]


def _sample_group(row: dict[str, Any], history: list[Any]) -> tuple[str, str, str]:
    duration = float(row.get("duration") or 0.0)
    if duration < 2.0:
        duration_bucket = "short"
    elif duration < 5.0:
        duration_bucket = "medium"
    else:
        duration_bucket = "long"
    if len(history) >= DEFAULT_CONTEXT_COUNT:
        history_bucket = "full"
    elif history:
        history_bucket = "partial"
    else:
        history_bucket = "none"
    return (_dataset_slug(row), duration_bucket, history_bucket)


def _select_families(requested: list[str] | None) -> list[PromptFamily]:
    families = _prompt_families()
    if not requested:
        return families
    wanted = set(requested)
    selected = [family for family in families if family.family_id in wanted]
    missing = wanted - {family.family_id for family in selected}
    if missing:
        raise ValueError(f"unknown prompt families: {sorted(missing)}")
    return selected


def _prompt_families() -> list[PromptFamily]:
    production_prompt = _load_backend_production_prompt()
    chirp_prompt = _load_text(
        REPO_DIR / "backend/pipeline/transcription/transcribers/chirp_prompt.txt"
    ).strip()
    phrase_hints = _load_phrase_hints()
    phrase_hint_system = "\n\n".join(
        [
            "You are a strict verbatim ASR system for emergency dispatch radio.",
            "Use this terminology inventory as acoustic context, not as text "
            "to copy unless heard:",
            ", ".join(phrase_hints),
            "Output only the transcript for the current audio clip.",
        ]
    )
    return [
        PromptFamily(
            "P1_force_words_incumbent",
            "Measured best no-fallback checkpoint-7 prompt.",
            SHORT_STRICT_ASR_SYSTEM,
            FORCE_WORDS_CURRENT_PROMPT,
            GENERIC_CONTEXT_HEADER,
        ),
        PromptFamily(
            "P2_canonical_gemini",
            "Shared model/src/common/gemini prompt.",
            GEMINI_TRANSCRIBE_SYSTEM_PROMPT,
            GEMINI_TRANSCRIBE_USER_PROMPT,
            GENERIC_CONTEXT_HEADER,
        ),
        PromptFamily(
            "P3_manual_quality_gate",
            "Production/manual-context quality-gate prompt.",
            production_prompt,
            FINAL_AUDIO_CURRENT_PROMPT,
            MANUAL_CONTEXT_HEADER,
        ),
        PromptFamily(
            "P4_chirp_phrase_hints",
            "Chirp-style prompt plus compact phrase hints.",
            "\n\n".join([chirp_prompt, phrase_hint_system]),
            FORCE_WORDS_CURRENT_PROMPT,
            GENERIC_CONTEXT_HEADER,
        ),
        PromptFamily(
            "P5_compact_common_asr",
            "Compact cross-model ASR prompt.",
            COMMON_SYSTEM_PROMPT,
            COMMON_USER_INSTRUCTION,
            GENERIC_CONTEXT_HEADER,
        ),
        PromptFamily(
            "P6_output_only",
            "Gemma-style output-only prompt without empty-output clause.",
            OUTPUT_ONLY_SYSTEM,
            FORCE_WORDS_CURRENT_PROMPT,
            GENERIC_CONTEXT_HEADER,
        ),
        PromptFamily(
            "P7_error_aware_context",
            "Transcript-context probe warning that prior transcripts may be wrong.",
            GEMINI_TRANSCRIBE_SYSTEM_PROMPT,
            TRANSCRIPT_CONTEXT_CURRENT_PROMPT,
            ERROR_AWARE_CONTEXT_HEADER,
        ),
        PromptFamily(
            "P8_no_empty_unintelligible",
            "No-empty current prompt with [UNINTELLIGIBLE] guidance.",
            SHORT_STRICT_ASR_SYSTEM,
            NO_EMPTY_CURRENT_PROMPT,
            GENERIC_CONTEXT_HEADER,
        ),
        PromptFamily(
            "P9_instruction_and_demo",
            "Instruction and demo mode seeded from incumbent prompt.",
            SHORT_STRICT_ASR_SYSTEM,
            FORCE_WORDS_CURRENT_PROMPT,
            GENERIC_CONTEXT_HEADER,
            optimization_mode="instruction_and_demo",
        ),
        PromptFamily(
            "P10_canary_minimal",
            "Minimal dispatch-specific user prompt as terse baseline.",
            COMMON_SYSTEM_PROMPT,
            CANARY_CURRENT_PROMPT,
            GENERIC_CONTEXT_HEADER,
        ),
        PromptFamily(
            "P11_format_first",
            "Formatting-first prompt with text before audio.",
            COMMON_SYSTEM_PROMPT,
            FORMAT_FIRST_CURRENT_PROMPT,
            GENERIC_CONTEXT_HEADER,
        ),
    ]


def _load_backend_production_prompt() -> str:
    path = REPO_DIR / "backend/pipeline/transcription/transcribers/prompts.py"
    spec = importlib.util.spec_from_file_location("_wd_backend_prompts", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"could not load {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return str(module.GEMINI_TRANSCRIBE_WITH_CONTEXT_SYSTEM_PROMPT)


def _load_phrase_hints() -> list[str]:
    path = REPO_DIR / "backend/pipeline/transcription/transcribers/chirp_phrase_hints.txt"
    hints = []
    for line in _load_text(path).splitlines():
        stripped = line.strip()
        if stripped and not stripped.startswith("#"):
            hints.append(stripped)
    return hints


def _load_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _load_many(
    storage_client: storage.Client, manifest_uris: list[str]
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for uri in manifest_uris:
        loaded = _load_jsonl(storage_client, uri)
        for row in loaded:
            row.setdefault("_source_manifest_uri", uri)
        rows.extend(loaded)
        print(f"loaded {len(loaded)} rows from {uri}")
    return rows


def _load_jsonl(
    storage_client: storage.Client, uri: str
) -> list[dict[str, Any]]:
    bucket_name, blob_path = parse_gcs_uri(uri)
    text = storage_client.bucket(bucket_name).blob(blob_path).download_as_text()
    return [json.loads(line) for line in text.splitlines() if line.strip()]


def _download_json(storage_client: storage.Client, uri: str) -> dict[str, Any]:
    bucket_name, blob_path = parse_gcs_uri(uri)
    text = storage_client.bucket(bucket_name).blob(blob_path).download_as_text()
    value = json.loads(text)
    if not isinstance(value, dict):
        raise TypeError(f"expected object JSON at {uri}")
    return value


def _dataset_slug(row: dict[str, Any]) -> str:
    for key in ("dataset_name", "dataset_family", "source_group"):
        value = row.get(key)
        if isinstance(value, str) and value.strip():
            return _safe_segment(value)
    source_uri = str(row.get("_source_manifest_uri") or "dataset")
    parts = source_uri.rstrip("/").split("/")
    return _safe_segment(parts[-2] if len(parts) >= 2 else source_uri)


def _safe_segment(value: str) -> str:
    safe = "".join(ch if ch.isalnum() else "-" for ch in value.lower())
    safe = "-".join(part for part in safe.split("-") if part)
    return safe or "dataset"


if __name__ == "__main__":
    raise SystemExit(main())
