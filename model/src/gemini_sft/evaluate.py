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
    upload_local_file,
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
    build_normalizer,
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
    optional_config_prior_context_mode,
    require_config_eval_execution,
    require_config_eval_model,
    require_config_int,
    require_config_str,
)
from gemini_sft.records import (
    append_ledger,
    wer_summary_gcs_uris,
    write_wer_summary,
)
from gemini_sft.reporting import (
    EvalReport,
    ReportArtifacts,
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
    prior_context_mode = optional_config_prior_context_mode(
        config, "prior_context_mode"
    )
    target = require_config_eval_model(config)
    eval_execution = require_config_eval_execution(config)
    logger.info(
        "Validated eval model target %s from config.json.",
        target.label,
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
            prior_context_count=prior_context_count,
            prior_context_mode=prior_context_mode,
            eval_manifest_uri=eval_manifest_uri,
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
        request_identity_hash = getattr(preds, "request_identity_hash", None)
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
    summary_json_uri, summary_markdown_uri = wer_summary_gcs_uris(
        run_gcs_prefix
    )
    artifacts = ReportArtifacts(
        raw_output_uri=raw_output_uri,
        online_predictions_uri=online_predictions_uri,
        normalized_manifest_uri=inference_manifest_uri,
        summary_json_uri=summary_json_uri,
        summary_markdown_uri=summary_markdown_uri,
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
    metrics[f"{target.label}_wer"] = target_metrics.wer
    metrics[f"{target.label}_cer"] = target_metrics.cer
    metrics[f"{target.label}_inference_manifest_uri"] = inference_manifest_uri
    if raw_output_uri:
        metrics[f"{target.label}_batch_output_uri"] = raw_output_uri
    if online_predictions_uri:
        metrics[f"{target.label}_online_predictions_uri"] = (
            online_predictions_uri
        )
    if target.label == "base":
        metrics["base_wer"] = target_metrics.wer
        metrics["base_cer"] = target_metrics.cer
    if target.label == "tuned":
        metrics["tuned_wer"] = target_metrics.wer
        metrics["tuned_cer"] = target_metrics.cer

    report = EvalReport(
        round_id=run_cfg.round_id,
        generated_at=datetime.now(UTC).isoformat(),
        targets=[target_metrics],
        metadata={
            "eval_manifest_uri": eval_manifest_uri,
            "n_eval_examples": len(eval_rows),
        },
    )
    summary_json_path, summary_markdown_path = write_wer_summary(
        RESULTS_DIR, run_cfg.round_id, report
    )
    upload_local_file(storage_client, summary_json_path, summary_json_uri)
    upload_local_file(
        storage_client, summary_markdown_path, summary_markdown_uri
    )
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
            "targets": [target_metrics],
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
    prior_context_count: int,
    prior_context_mode: str,
    eval_manifest_uri: str,
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
        prior_context_count=prior_context_count,
        prior_context_mode=prior_context_mode,
        eval_manifest_uri=eval_manifest_uri,
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


