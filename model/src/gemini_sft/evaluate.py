"""Evaluate one Gemini model for a config-driven SFT run."""

from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from common.gcs_utils import (
    download_json_text,
    download_jsonl_manifest_strict,
    gcs_uri_exists,
    upload_local_file,
)
from common.gemini.batch import BatchPredictionMap, run_batch_audio_inference
from common.gemini.eval_artifacts import wer_summary_gcs_uris
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
    eval_rows_with_histories_from_entries,
    write_and_upload_config,
)
from gemini_sft.config import (
    EvalExecutionConfig,
    RunConfig,
    RunConfigError,
    load_eval_run_config,
    optional_config_prior_context_mode,
    require_config_eval_execution,
    require_config_eval_model,
    require_config_str,
)
from gemini_sft.records import (
    write_wer_summary,
)
from gemini_sft.reporting import (
    EvalReport,
    ReportArtifacts,
    build_target_metrics,
    render_console_report,
)
from gemini_sft.target_execution import (
    OnlinePredictionMap,
    resolve_target_backend,
    run_online_target_inference,
)

if TYPE_CHECKING:
    import argparse

logger = logging.getLogger(__name__)
RESULTS_DIR = DEFAULT_RESULTS_DIR
_LOCAL_DURABLE_EVAL_FIELDS = (
    "inference_dataset_slug",
    "eval_manifest_uri",
    "gcp_project",
    "gcs_bucket",
    "location",
    "base_model",
    "prior_context_count",
    "prior_context_mode",
    "system_prompt",
    "user_prompt",
)


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
        _validate_local_eval_config_matches_durable(run_cfg, config)
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


def evaluate_run(  # noqa: PLR0915
    args: argparse.Namespace,
    run_cfg: RunConfig,
    storage_client: storage.Client,
    config: dict[str, Any],
) -> int:
    """Run the configured eval model and score one config-driven run."""
    del args
    system_prompt = require_config_str(config, "system_prompt")
    user_prompt = require_config_str(config, "user_prompt")
    base_model = require_config_str(config, "base_model")
    eval_manifest_uri = require_config_str(config, "canonical_eval_uri")
    gcp_project = require_config_str(config, "gcp_project")
    location = require_config_str(config, "location")
    run_gcs_prefix = require_config_str(config, "run_gcs_prefix")
    inference_dataset_slug = require_config_str(
        config, "inference_dataset_slug"
    )
    gcs_bucket = require_config_str(config, "gcs_bucket")
    prior_context_count = _optional_config_nonnegative_int(
        config,
        "prior_context_count",
    )
    prior_context_mode = optional_config_prior_context_mode(
        config, "prior_context_mode"
    )
    target = require_config_eval_model(config)
    durable_eval_execution = require_config_eval_execution(config)
    eval_execution = _effective_eval_execution(
        durable_eval_execution,
        run_cfg.eval_execution,
    )
    logger.info(
        "Validated eval model target %s from config.json.",
        target.label,
    )

    eval_entries = download_jsonl_manifest_strict(
        storage_client,
        eval_manifest_uri,
    )
    eval_data = eval_rows_with_histories_from_entries(
        eval_entries,
        source=eval_manifest_uri,
        prior_context_count=prior_context_count,
        limit=eval_execution.limit,
    )
    source_rows = eval_data.source_rows
    eval_rows = eval_data.eval_rows
    histories = eval_data.histories
    model_family_slug = model_family_slug_from_model_id(base_model)
    audio_uris = [row.audio_filepath for row in eval_rows]
    refs = [row.text for row in eval_rows]
    normalizer = build_normalizer()
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

    report = EvalReport(
        round_id=run_cfg.round_id,
        generated_at=datetime.now(UTC).isoformat(),
        target=target_metrics,
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
    config["last_eval_at"] = datetime.now(UTC).isoformat()
    config = write_and_upload_config(
        results_dir=RESULTS_DIR,
        run_cfg=run_cfg,
        storage_client=storage_client,
        config=config,
    )
    logger.info(
        "Eval complete. WER summary: %s",
        RESULTS_DIR / run_cfg.round_id / "wer_summary.md",
    )
    return 0


PredictionMap = BatchPredictionMap | OnlinePredictionMap


def _validate_local_eval_config_matches_durable(
    run_cfg: RunConfig,
    config: dict[str, Any],
) -> None:
    """Fail loudly when a local eval TOML disagrees with durable GCS state."""
    local_record = run_cfg.to_record_dict()
    mismatches = [
        key
        for key in _LOCAL_DURABLE_EVAL_FIELDS
        if local_record.get(key) != config.get(key)
    ]

    if run_cfg.eval_model is None:
        msg = "local eval config missing required [eval.model]"
        raise ValueError(msg)
    local_target = run_cfg.eval_model.to_record_dict()
    durable_target = require_config_eval_model(config).to_record_dict()
    if local_target != durable_target:
        mismatches.append("eval_model")

    local_execution = run_cfg.eval_execution
    durable_execution = require_config_eval_execution(config)
    if _metric_affecting_eval_execution(local_execution) != (
        _metric_affecting_eval_execution(durable_execution)
    ):
        mismatches.append("eval_execution")

    if not mismatches:
        return
    fields = ", ".join(mismatches)
    msg = (
        "local eval config does not match durable GCS config.json for "
        f"round {run_cfg.round_id}; GCS config.json is the eval source of "
        "truth. Use the matching prepared config or create a separate prepared "
        f"round_id for this eval target. Mismatched field(s): {fields}"
    )
    raise ValueError(msg)


def _metric_affecting_eval_execution(
    execution: EvalExecutionConfig,
) -> dict[str, int | str]:
    """Return eval execution fields that can change reported metrics."""
    record: dict[str, int | str] = {}
    if execution.backend is not None:
        record["backend"] = execution.backend
    if execution.limit is not None:
        record["limit"] = execution.limit
    return record


def _effective_eval_execution(
    durable: EvalExecutionConfig,
    local: EvalExecutionConfig,
) -> EvalExecutionConfig:
    """Use durable eval identity with local operational runtime controls."""
    if (
        local.concurrency != durable.concurrency
        or local.max_retries != durable.max_retries
    ):
        logger.info(
            "Using local eval execution overrides: concurrency=%s, "
            "max_retries=%s.",
            local.concurrency,
            local.max_retries,
        )
    return EvalExecutionConfig(
        backend=durable.backend,
        limit=durable.limit,
        concurrency=local.concurrency,
        max_retries=local.max_retries,
    )


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
