"""Evaluate base and tuned Gemini models for a config-driven SFT run."""

from __future__ import annotations

import asyncio
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
    require_config_eval_execution,
    require_config_eval_models,
    require_config_int,
    require_config_str,
)
from gemini_sft.records import append_ledger, write_wer_summary
from gemini_sft.reporting import (
    EvalReport,
    ReportArtifacts,
    TargetMetrics,
    build_target_metrics,
    render_console_report,
)
from gemini_sft.target_execution import (
    resolve_target_backend,
    run_online_target_inference,
)

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
    """Run configured eval targets and score one config-driven run."""
    del args
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
    eval_model_targets = require_config_eval_models(config)
    eval_execution = require_config_eval_execution(config)
    logger.info(
        "Validated %d eval model target(s) from config.json.",
        len(eval_model_targets),
    )

    eval_entries = download_jsonl_manifest(storage_client, eval_manifest_uri)
    source_rows, eval_rows = canonical_rows_from_entries(
        eval_entries,
        split="eval",
        source=eval_manifest_uri,
    )
    if eval_execution.limit is not None:
        source_rows = source_rows[: eval_execution.limit]
        eval_rows = eval_rows[: eval_execution.limit]
    histories = build_context_histories(
        source_rows,
        max_turns=prior_context_count,
    )
    model_family_slug = model_family_slug_from_model_id(base_model)
    audio_uris = [row.audio_filepath for row in eval_rows]
    refs = [row.text for row in eval_rows]
    normalizer = build_normalizer()
    metrics: dict[str, Any] = {
        "round_id": run_cfg.round_id,
        "base_model": base_model,
        "n_eval_examples": len(eval_rows),
    }
    report_targets = []

    for target in eval_model_targets:
        backend = resolve_target_backend(target, eval_execution)
        if backend == "batch":
            preds = batch_infer(
                storage_client=storage_client,
                run_gcs_prefix=run_gcs_prefix,
                gcp_project=gcp_project,
                location=location,
                model_id=target.model,
                label=target.label,
                eval_rows=eval_rows,
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                histories=histories,
                history_mode=prior_context_mode,
            )
            if preds is None:
                return 1
            raw_output_uri = preds.output_uri
            online_predictions_uri = None
            metadata: dict[str, Any] = {"backend": "batch"}
        elif backend == "online":
            preds = asyncio.run(
                run_online_target_inference(
                    storage_client=storage_client,
                    run_gcs_prefix=run_gcs_prefix,
                    project=gcp_project,
                    default_location=location,
                    target_label=target.label,
                    target_model=target.model,
                    audio_uris=audio_uris,
                    histories=histories,
                    system_prompt=system_prompt,
                    user_prompt=user_prompt,
                    prior_context_count=prior_context_count,
                    prior_context_mode=prior_context_mode,
                    eval_manifest_uri=eval_manifest_uri,
                    local_dir=RESULTS_DIR / run_cfg.round_id / "online",
                    concurrency=eval_execution.concurrency,
                    max_retries=eval_execution.max_retries,
                )
            )
            raw_output_uri = None
            online_predictions_uri = preds.online_predictions_uri
            metadata = {
                "backend": "online",
                "online_error_count": preds.error_count,
            }
            request_identity_hash = getattr(
                preds, "request_identity_hash", None
            )
            if request_identity_hash:
                metadata["request_identity_hash"] = request_identity_hash
        else:
            msg = f"unsupported eval backend: {backend}"
            raise ValueError(msg)

        inference_manifest_uri = upload_inference_manifest(
            storage_client,
            bucket_name=gcs_bucket,
            inference_dataset_slug=inference_dataset_slug,
            model_family_slug=model_family_slug,
            run_id=run_cfg.round_id,
            artifact_label=target.label,
            source_rows=source_rows,
            predictions_by_audio_uri=preds,
        )
        artifacts = ReportArtifacts(
            raw_output_uri=raw_output_uri,
            online_predictions_uri=online_predictions_uri,
            normalized_manifest_uri=inference_manifest_uri,
        )
        # Empty-string fallback is intentional: skipped/missing provider outputs
        # score as deletions instead of disappearing from the denominator.
        hyps = [preds.get(row.audio_filepath, "") for row in eval_rows]
        missing_prediction_count = sum(
            1 for row in eval_rows if row.audio_filepath not in preds
        )
        target_metrics = build_target_metrics(
            label=target.label,
            model=target.model,
            refs=refs,
            hyps=hyps,
            normalizer=normalizer,
            keywords=GEMINI_TRANSCRIBE_KEYWORDS,
            missing_prediction_count=missing_prediction_count,
            artifacts=artifacts,
            metadata=metadata,
        )
        report_targets.append(target_metrics)
        metrics[f"{target.label}_wer"] = target_metrics.wer
        metrics[f"{target.label}_cer"] = target_metrics.cer
        metrics[f"{target.label}_inference_manifest_uri"] = (
            inference_manifest_uri
        )
        if raw_output_uri:
            metrics[f"{target.label}_batch_output_uri"] = raw_output_uri
        if online_predictions_uri:
            metrics[f"{target.label}_online_predictions_uri"] = (
                online_predictions_uri
            )

    base_target = _target_by_label(report_targets, "base")
    if base_target is not None:
        metrics["base_wer"] = base_target.wer
        metrics["base_cer"] = base_target.cer
    tuned_target = _target_by_label(report_targets, "tuned")
    if tuned_target is not None:
        metrics["tuned_wer"] = tuned_target.wer
        metrics["tuned_cer"] = tuned_target.cer

    report = EvalReport(
        round_id=run_cfg.round_id,
        generated_at=datetime.now(UTC).isoformat(),
        targets=report_targets,
        metadata={
            "eval_manifest_uri": eval_manifest_uri,
            "n_eval_examples": len(eval_rows),
        },
    )
    write_wer_summary(RESULTS_DIR, run_cfg.round_id, report)
    logger.info("\n%s", render_console_report(report))
    config.update(
        {
            "base_model": base_model,
            "last_eval_at": datetime.now(UTC).isoformat(),
        }
    )
    if "base_wer" in metrics:
        config["base_wer"] = metrics["base_wer"]
    if "tuned_wer" in metrics:
        config["tuned_wer"] = metrics["tuned_wer"]
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
            "targets": report_targets,
            "timestamp": datetime.now(UTC).strftime("%Y-%m-%d"),
        },
    )
    logger.info(
        "Eval complete. WER summary: %s",
        RESULTS_DIR / run_cfg.round_id / "wer_summary.md",
    )
    return 0


PredictionMap = BatchPredictionMap


def _target_by_label(
    targets: list[TargetMetrics], label: str
) -> TargetMetrics | None:
    for target in targets:
        if target.target_label == label:
            return target
    return None


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
