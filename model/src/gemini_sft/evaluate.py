"""Evaluate base and tuned Gemini models for a config-driven SFT run."""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from common.gcs_utils import (
    download_json_text,
    download_jsonl_manifest,
    gcs_uri_exists,
)
from common.gemini.context import build_context_histories
from common.gemini.batch import BatchPredictionMap, run_batch_audio_inference
from common.gemini.prompts import GEMINI_TRANSCRIBE_KEYWORDS
from common.gemini.vertex import submit_batch_inference
from common.inference_manifest import (
    model_family_slug_from_model_id,
    upload_inference_manifest,
)
from common.scoring import (
    bootstrap_paired,
    build_normalizer,
    compute_cer,
    compute_wer,
    duration_bucket_wer,
    hallucination_rate,
    keyword_metrics,
)
from google.cloud import storage

from gemini_sft.artifacts import (
    DEFAULT_RESULTS_DIR,
    canonical_rows_from_entries,
    write_and_upload_config,
)
from gemini_sft.config import (
    RunConfig,
    RunConfigError,
    load_eval_run_config,
    require_config_int,
    require_config_str,
)
from gemini_sft.records import append_ledger, write_wer_summary

if TYPE_CHECKING:
    import argparse

logger = logging.getLogger(__name__)
RESULTS_DIR = DEFAULT_RESULTS_DIR


def evaluate(args: argparse.Namespace) -> int:
    """CLI handler for ``gemini-sft eval``."""
    try:
        run_cfg = load_eval_run_config(args.config)
        storage_client = storage.Client(project=run_cfg.gcp_project)
        if not gcs_uri_exists(storage_client, run_cfg.paths.config_uri):
            logger.error(
                "No GCS config.json found for round %s.", run_cfg.round_id
            )
            return 1
        config = download_json_text(storage_client, run_cfg.paths.config_uri)
        return evaluate_run(args, run_cfg, storage_client, config)
    except (
        ImportError,
        OSError,
        RunConfigError,
        TypeError,
        ValueError,
        RuntimeError,
        TimeoutError,
    ) as exc:
        return _log_cli_error(exc)


def evaluate_run(
    args: argparse.Namespace,
    run_cfg: RunConfig,
    storage_client: storage.Client,
    config: dict[str, Any],
) -> int:
    """Run batch inference and score one config-driven run."""
    system_prompt = require_config_str(config, "system_prompt")
    user_prompt = require_config_str(config, "user_prompt")
    base_model = require_config_str(config, "base_model")
    eval_manifest_uri = require_config_str(config, "canonical_eval_uri")
    gcp_project = require_config_str(config, "gcp_project")
    location = require_config_str(config, "location")
    run_gcs_prefix = require_config_str(config, "run_gcs_prefix")
    dataset = require_config_str(config, "dataset")
    inference_dataset_slug = require_config_str(
        config, "inference_dataset_slug"
    )
    gcs_bucket = require_config_str(config, "gcs_bucket")
    epoch_count = require_config_int(config, "epoch_count")
    prior_context_count = _optional_config_nonnegative_int(
        config,
        "prior_context_count",
    )
    prior_context_mode = _optional_config_prior_context_mode(
        config,
        "prior_context_mode",
    )
    tuned_endpoint = config.get("endpoint")
    base_only = bool(getattr(args, "base_only", False))
    if not base_only and not tuned_endpoint:
        # Base-only eval is useful before tune and after a failed tune, but it
        # must be visible in logs so a missing endpoint is not mistaken for a
        # tuned-model comparison.
        logger.warning(
            "No tuned endpoint in config.json; running base-only eval."
        )
        base_only = True

    eval_entries = download_jsonl_manifest(storage_client, eval_manifest_uri)
    source_rows, eval_rows = canonical_rows_from_entries(
        eval_entries,
        split="eval",
        source=eval_manifest_uri,
    )
    histories = build_context_histories(
        source_rows,
        max_turns=prior_context_count,
    )
    model_family_slug = model_family_slug_from_model_id(base_model)

    base_preds = batch_infer(
        storage_client=storage_client,
        run_gcs_prefix=run_gcs_prefix,
        gcp_project=gcp_project,
        location=location,
        model_id=base_model,
        label="base",
        eval_rows=eval_rows,
        system_prompt=system_prompt,
        user_prompt=user_prompt,
        histories=histories,
        history_mode=prior_context_mode,
    )
    if base_preds is None:
        return 1

    refs = [row.text for row in eval_rows]
    durations = [row.duration for row in eval_rows]
    # Empty-string fallback is intentional: skipped/missing Vertex outputs
    # score as deletions instead of disappearing from the denominator.
    base_hyps = [base_preds.get(row.audio_filepath, "") for row in eval_rows]
    normalizer = build_normalizer()
    metrics = build_metrics(
        round_id=run_cfg.round_id,
        base_model=base_model,
        refs=refs,
        durations=durations,
        base_hyps=base_hyps,
        normalizer=normalizer,
        n_eval_examples=len(eval_rows),
    )
    # Store raw batch output locations alongside metrics so future reviewers can
    # recalculate WER from Vertex responses without rerunning inference.
    metrics["base_batch_output_uri"] = base_preds.output_uri
    metrics["base_inference_manifest_uri"] = upload_inference_manifest(
        storage_client,
        bucket_name=gcs_bucket,
        inference_dataset_slug=inference_dataset_slug,
        model_family_slug=model_family_slug,
        run_id=run_cfg.round_id,
        artifact_label="base",
        source_rows=source_rows,
        predictions_by_audio_uri=base_preds,
    )

    if not base_only and tuned_endpoint:
        tuned_preds = batch_infer(
            storage_client=storage_client,
            run_gcs_prefix=run_gcs_prefix,
            gcp_project=gcp_project,
            location=location,
            model_id=str(tuned_endpoint),
            label="tuned",
            eval_rows=eval_rows,
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            histories=histories,
            history_mode=prior_context_mode,
        )
        if tuned_preds is None:
            return 1
        tuned_hyps = [
            tuned_preds.get(row.audio_filepath, "") for row in eval_rows
        ]
        add_tuned_metrics(
            metrics, refs, durations, base_hyps, tuned_hyps, normalizer
        )
        metrics["tuned_batch_output_uri"] = tuned_preds.output_uri
        metrics["tuned_inference_manifest_uri"] = upload_inference_manifest(
            storage_client,
            bucket_name=gcs_bucket,
            inference_dataset_slug=inference_dataset_slug,
            model_family_slug=model_family_slug,
            run_id=run_cfg.round_id,
            artifact_label="tuned",
            source_rows=source_rows,
            predictions_by_audio_uri=tuned_preds,
        )

    write_wer_summary(RESULTS_DIR, run_cfg.round_id, metrics)
    config.update(
        {
            "base_model": base_model,
            "base_wer": metrics.get("base_wer"),
            "tuned_wer": metrics.get("tuned_wer"),
            "last_eval_at": datetime.now(UTC).isoformat(),
        }
    )
    config = write_and_upload_config(
        results_dir=RESULTS_DIR,
        run_cfg=run_cfg,
        storage_client=storage_client,
        config=config,
    )
    append_ledger(
        RESULTS_DIR,
        {
            **metrics,
            "datasets": [dataset],
            "epochs": epoch_count,
            "git_sha": config.get("git_sha", "—"),
            "timestamp": datetime.now(UTC).strftime("%Y-%m-%d"),
        },
    )
    logger.info(
        "Eval complete. WER summary: %s",
        RESULTS_DIR / run_cfg.round_id / "wer_summary.md",
    )
    return 0


PredictionMap = BatchPredictionMap


def batch_infer(
    *,
    storage_client: storage.Client,
    run_gcs_prefix: str,
    gcp_project: str,
    location: str,
    model_id: str,
    label: str,
    eval_rows: list[Any],
    system_prompt: str,
    user_prompt: str,
    histories: list[Any] | None = None,
    history_mode: str = "text_turns",
) -> PredictionMap | None:
    """Build batch input JSONL, submit, download outputs, and parse predictions."""
    return run_batch_audio_inference(
        storage_client=storage_client,
        run_gcs_prefix=run_gcs_prefix,
        gcp_project=gcp_project,
        location=location,
        model_id=model_id,
        label=label,
        audio_uris=[str(row.audio_filepath) for row in eval_rows],
        system_prompt=system_prompt,
        user_prompt=user_prompt,
        histories=histories,
        history_mode=history_mode,
        submit_fn=submit_batch_inference,
    )


def _log_cli_error(exc: Exception) -> int:
    logger.error(str(exc))
    return 1


def _optional_config_nonnegative_int(config: dict[str, Any], key: str) -> int:
    value = config.get(key, 0)
    if isinstance(value, bool) or not isinstance(value, int):
        msg = f"config.json field must be a non-negative integer: {key}"
        raise TypeError(msg)
    if value < 0:
        msg = f"config.json field must be a non-negative integer: {key}"
        raise ValueError(msg)
    return value


def _optional_config_prior_context_mode(
    config: dict[str, Any], key: str
) -> str:
    allowed = {"text_turns", "transcript", "vapo_p3_transcript"}
    value = config.get(key, "text_turns")
    if not isinstance(value, str):
        msg = (
            f"config.json field must be one of {', '.join(sorted(allowed))}: "
            f"{key}"
        )
        raise TypeError(msg)
    mode = value.strip().lower()
    if mode not in allowed:
        msg = (
            f"config.json field must be one of {', '.join(sorted(allowed))}: "
            f"{key}"
        )
        raise ValueError(msg)
    return mode


def build_metrics(
    *,
    round_id: str,
    base_model: str,
    refs: list[str],
    durations: list[float],
    base_hyps: list[str],
    normalizer: Any,
    n_eval_examples: int,
) -> dict[str, Any]:
    """Build the base-model scoring panel."""
    base_wer_result = compute_wer(refs, base_hyps, normalizer=normalizer)
    base_cer_result = compute_cer(refs, base_hyps, normalizer=normalizer)
    metrics: dict[str, Any] = {
        "round_id": round_id,
        "base_model": base_model,
        "base_wer": base_wer_result["wer"],
        "base_cer": base_cer_result["cer"],
        "n_eval_examples": n_eval_examples,
    }
    add_error_breakdown(metrics, "base", base_wer_result)
    # Historical reports called this "empty rate"; the scorer flags both empty
    # strings and the explicit [UNINTELLIGIBLE] token emitted for unusable audio.
    metrics["base_empty_rate"] = hallucination_rate(base_hyps)
    base_keyword_rows = keyword_metrics(
        refs, base_hyps, GEMINI_TRANSCRIBE_KEYWORDS
    )
    metrics["base_keyword_metrics"] = base_keyword_rows
    metrics["base_keyword_accuracy"] = overall_keyword_accuracy(
        base_keyword_rows
    )
    try:
        metrics["duration_buckets"] = [
            {"bucket": row["bucket"], "base_wer": row["wer"]}
            for row in duration_bucket_wer(
                refs, base_hyps, durations, normalizer=normalizer
            )
        ]
    except Exception as exc:
        logger.warning("Could not compute duration bucket WER: %s", exc)
    return metrics


def add_tuned_metrics(
    metrics: dict[str, Any],
    refs: list[str],
    durations: list[float],
    base_hyps: list[str],
    tuned_hyps: list[str],
    normalizer: Any,
) -> None:
    """Add tuned-model metrics to an existing base metrics dictionary."""
    tuned_wer_result = compute_wer(refs, tuned_hyps, normalizer=normalizer)
    tuned_cer_result = compute_cer(refs, tuned_hyps, normalizer=normalizer)
    metrics["tuned_wer"] = tuned_wer_result["wer"]
    metrics["tuned_cer"] = tuned_cer_result["cer"]
    metrics["tuned_empty_rate"] = hallucination_rate(tuned_hyps)
    add_error_breakdown(metrics, "tuned", tuned_wer_result)
    tuned_keyword_rows = keyword_metrics(
        refs, tuned_hyps, GEMINI_TRANSCRIBE_KEYWORDS
    )
    metrics["tuned_keyword_metrics"] = tuned_keyword_rows
    metrics["tuned_keyword_accuracy"] = overall_keyword_accuracy(
        tuned_keyword_rows
    )
    try:
        bootstrap = bootstrap_paired(
            refs, base_hyps, tuned_hyps, normalizer=normalizer
        )
        metrics["bootstrap_p_value"] = bootstrap.get("p_value_one_sided")
        metrics["bootstrap_ci_low"] = bootstrap.get("ci_low")
        metrics["bootstrap_ci_high"] = bootstrap.get("ci_high")
        metrics["bootstrap_delta"] = bootstrap.get("delta")
    except Exception as exc:
        logger.warning("bootstrap_paired failed: %s", exc)
    try:
        tuned_by_bucket = {
            row["bucket"]: row["wer"]
            for row in duration_bucket_wer(
                refs, tuned_hyps, durations, normalizer=normalizer
            )
        }
        for entry in metrics.get("duration_buckets", []):
            entry["tuned_wer"] = tuned_by_bucket.get(entry["bucket"])
    except Exception as exc:
        logger.warning("Could not compute tuned duration bucket WER: %s", exc)


def add_error_breakdown(
    metrics: dict[str, Any],
    prefix: str,
    wer_result: dict[str, Any],
) -> None:
    """Add insertion/deletion/substitution rates to a metrics dictionary."""
    total_ref_words = (
        int(wer_result["hits"])
        + int(wer_result["substitutions"])
        + int(wer_result["deletions"])
    )
    if total_ref_words <= 0:
        return
    metrics[f"{prefix}_insertions"] = (
        wer_result["insertions"] / total_ref_words * 100
    )
    metrics[f"{prefix}_deletions"] = (
        wer_result["deletions"] / total_ref_words * 100
    )
    metrics[f"{prefix}_substitutions"] = (
        wer_result["substitutions"] / total_ref_words * 100
    )


def overall_keyword_accuracy(rows: list[dict[str, Any]]) -> float | None:
    """Return occurrence-weighted keyword accuracy."""
    total_occurrences = sum(row["occurrences"] for row in rows)
    if total_occurrences == 0:
        return None
    total_correct = sum(row["correctly_identified"] for row in rows)
    return total_correct / total_occurrences * 100
