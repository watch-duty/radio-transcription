"""Evaluate one Gemini model for a config-driven SFT run."""

from __future__ import annotations

import asyncio
import datetime
import logging
import typing

from common import gcs_utils, inference_manifest, scoring
from common.gemini import batch as gemini_batch
from common.gemini import context as gemini_context
from common.gemini import eval_artifacts, prompts, vertex
from google.api_core import exceptions as google_exceptions
from google.cloud import storage

from gemini_sft import artifacts as artifacts_lib
from gemini_sft import config as config_lib
from gemini_sft import records, reporting, target_execution

if typing.TYPE_CHECKING:
    import argparse

logger = logging.getLogger(__name__)
RESULTS_DIR = artifacts_lib.DEFAULT_RESULTS_DIR
_LOCAL_DURABLE_EVAL_FIELDS = (
    "round_id",
    "run_gcs_prefix",
    "canonical_eval_uri",
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
    """Validate durable state and run ``gemini-sft eval``.

    Args:
        args: Parsed CLI namespace containing the local config path.

    Returns:
        Zero after evaluation succeeds; one after a handled configuration,
        provider, storage, or local I/O failure.
    """
    try:
        run_cfg = config_lib.load_eval_run_config(args.config)
        storage_client = storage.Client(project=run_cfg.gcp_project)
        if not gcs_utils.gcs_uri_exists(
            storage_client,
            run_cfg.paths.config_uri,
        ):
            logger.error(
                "No GCS config.json found for round %s.", run_cfg.round_id
            )
            return 1
        config = gcs_utils.download_json_text(
            storage_client,
            run_cfg.paths.config_uri,
        )
        _validate_local_eval_config_matches_durable(run_cfg, config)
        return evaluate_run(run_cfg, storage_client, config)
    except (
        ImportError,
        OSError,
        google_exceptions.GoogleAPIError,
        config_lib.RunConfigError,
        TypeError,
        ValueError,
        RuntimeError,
        TimeoutError,
    ) as exc:
        return _log_cli_error(exc)


def _eval_model_family_id(
    target: config_lib.EvalModelTarget,
    base_model: str,
) -> str:
    """Return the publisher family represented by one eval target.

    Args:
        target: Durable evaluated model or endpoint target.
        base_model: Publisher model used to create endpoint targets.

    Returns:
        The target model for publisher targets, or ``base_model`` for
        endpoints.
    """
    if target.is_endpoint:
        return base_model
    return target.model


def evaluate_run(  # noqa: PLR0915
    run_cfg: config_lib.RunConfig,
    storage_client: storage.Client,
    config: dict[str, typing.Any],
) -> int:
    """Run the configured eval model and score one config-driven run.

    Args:
        run_cfg: Validated local configuration for the eval run.
        storage_client: Client used to read and write GCS artifacts.
        config: Durable run configuration loaded from GCS ``config.json``.

    Returns:
        Zero after evaluation and report publication complete, or one when
        inference produces no successful predictions.

    Raises:
        ImportError: If a required provider or scoring dependency is missing.
        OSError: If a local evaluation artifact cannot be read or written.
        TypeError: If the durable configuration has an invalid field type.
        ValueError: If configuration, manifest, or backend validation fails.
        RuntimeError: If a provider operation reaches a failed state.
        TimeoutError: If a provider operation exceeds its timeout.
    """
    system_prompt = config_lib.require_config_str(config, "system_prompt")
    user_prompt = config_lib.require_config_str(
        config,
        "user_prompt",
        allow_empty=True,
    )
    base_model = config_lib.require_config_str(config, "base_model")
    eval_manifest_uri = config_lib.require_config_str(
        config,
        "canonical_eval_uri",
    )
    gcp_project = config_lib.require_config_str(config, "gcp_project")
    location = config_lib.require_config_str(config, "location")
    run_gcs_prefix = config_lib.require_config_str(config, "run_gcs_prefix")
    inference_dataset_slug = config_lib.require_config_str(
        config, "inference_dataset_slug"
    )
    gcs_bucket = config_lib.require_config_str(config, "gcs_bucket")
    prior_context_count = _require_config_nonnegative_int(
        config,
        "prior_context_count",
    )
    prior_context_mode = config_lib.require_config_prior_context_mode(
        config, "prior_context_mode"
    )
    prior_context_mode = gemini_context.validate_evaluation_context_contract(
        prior_context_count,
        prior_context_mode,
    )
    target = config_lib.require_config_eval_model(config)
    durable_eval_execution = config_lib.require_config_eval_execution(config)
    eval_execution = _effective_eval_execution(
        durable_eval_execution,
        run_cfg.eval_execution,
    )
    logger.info(
        "Validated eval model target %s from config.json.",
        target.label,
    )

    eval_entries = gcs_utils.download_jsonl_manifest_strict(
        storage_client,
        eval_manifest_uri,
    )
    eval_data = artifacts_lib.eval_rows_for_inference_from_entries(
        eval_entries,
        source=eval_manifest_uri,
        limit=eval_execution.limit,
        prior_context_count=prior_context_count,
    )
    source_rows = eval_data.source_rows
    eval_rows = eval_data.eval_rows
    segments = eval_data.segments
    model_family_slug = inference_manifest.model_family_slug_from_model_id(
        _eval_model_family_id(target, base_model)
    )
    normalizer = scoring.build_normalizer()
    backend = target_execution.resolve_target_backend(
        target,
        eval_execution,
        prior_context_count=prior_context_count,
    )
    rolling_history_index_uri = None
    rolling_history_audit_uri = None
    if backend == "batch":
        preds = batch_infer(
            storage_client=storage_client,
            run_gcs_prefix=run_gcs_prefix,
            gcp_project=gcp_project,
            location=location,
            model_id=target.model,
            label=target.label,
            segments=segments,
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            prior_context_mode=prior_context_mode,
            eval_manifest_uri=eval_manifest_uri,
        )
        if preds is None:
            return 1
        raw_output_uri = preds.output_uri
        online_predictions_uri = None
        metadata: dict[str, typing.Any] = {"backend": "batch"}
    elif backend == "online":
        preds = asyncio.run(
            target_execution.run_online_target_inference(
                storage_client=storage_client,
                run_gcs_prefix=run_gcs_prefix,
                project=gcp_project,
                target_label=target.label,
                target_model=target.model,
                segments=segments,
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
        if not preds:
            logger.error(
                "Online inference produced no successful predictions for "
                "target %s; unresolved errors=%s.",
                target.label,
                preds.error_count,
            )
            return 1
        raw_output_uri = None
        online_predictions_uri = preds.online_predictions_uri
        rolling_history_index_uri = preds.rolling_history_index_uri
        rolling_history_audit_uri = preds.rolling_history_audit_uri
        metadata = {
            "backend": "online",
            "online_error_count": preds.error_count,
            "request_identity_hash": preds.request_identity_hash,
        }
    else:
        msg = f"unsupported eval backend: {backend}"
        raise ValueError(msg)

    inference_manifest_uri = inference_manifest.upload_inference_manifest(
        storage_client,
        bucket_name=gcs_bucket,
        inference_dataset_slug=inference_dataset_slug,
        model_family_slug=model_family_slug,
        run_id=run_cfg.round_id,
        artifact_label=target.label,
        source_rows=source_rows,
        predictions_by_audio_uri=preds,
    )
    summary_json_uri, summary_markdown_uri = (
        eval_artifacts.wer_summary_gcs_uris(run_gcs_prefix)
    )
    artifacts = reporting.ReportArtifacts(
        raw_output_uri=raw_output_uri,
        online_predictions_uri=online_predictions_uri,
        rolling_history_index_uri=rolling_history_index_uri,
        rolling_history_audit_uri=rolling_history_audit_uri,
        normalized_manifest_uri=inference_manifest_uri,
        summary_json_uri=summary_json_uri,
        summary_markdown_uri=summary_markdown_uri,
    )
    # References remain isolated in scoring rows while provider requests and
    # rolling-history artifacts are built, then pair with finalized predictions.
    refs = [row.text for row in eval_rows]
    # Empty-string fallback is intentional: skipped/missing provider outputs
    # score as deletions instead of disappearing from the denominator.
    hyps = [preds.get(row.audio_filepath, "") for row in eval_rows]
    missing_prediction_count = sum(
        1 for row in eval_rows if row.audio_filepath not in preds
    )
    target_metrics = reporting.build_target_metrics(
        label=target.label,
        model=target.model,
        refs=refs,
        hyps=hyps,
        normalizer=normalizer,
        keywords=prompts.GEMINI_TRANSCRIBE_KEYWORDS,
        missing_prediction_count=missing_prediction_count,
        artifacts=artifacts,
        metadata=metadata,
    )

    report = reporting.EvalReport(
        round_id=run_cfg.round_id,
        generated_at=datetime.datetime.now(datetime.UTC).isoformat(),
        target=target_metrics,
        metadata={
            "eval_manifest_uri": eval_manifest_uri,
            "n_eval_examples": len(eval_rows),
        },
    )
    summary_json_path, summary_markdown_path = records.write_wer_summary(
        RESULTS_DIR, run_cfg.round_id, report
    )
    gcs_utils.upload_local_file(
        storage_client,
        summary_json_path,
        summary_json_uri,
    )
    gcs_utils.upload_local_file(
        storage_client, summary_markdown_path, summary_markdown_uri
    )
    logger.info("\n%s", reporting.render_console_report(report))
    config["last_eval_at"] = datetime.datetime.now(datetime.UTC).isoformat()
    config = artifacts_lib.write_and_upload_config(
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


def _validate_local_eval_config_matches_durable(
    run_cfg: config_lib.RunConfig,
    config: dict[str, typing.Any],
) -> None:
    """Fail when a local eval TOML disagrees with durable GCS state.

    Args:
        run_cfg: Validated local evaluation configuration.
        config: Durable run configuration loaded from GCS.

    Raises:
        TypeError: If a durable field has an invalid type.
        ValueError: If local metric identity differs from durable state.
    """
    local_record = run_cfg.to_record_dict()
    durable_record = dict(config)
    durable_record["prior_context_count"] = _require_config_nonnegative_int(
        config, "prior_context_count"
    )
    durable_record["prior_context_mode"] = (
        config_lib.require_config_prior_context_mode(
            config,
            "prior_context_mode",
        )
    )
    mismatches = [
        key
        for key in _LOCAL_DURABLE_EVAL_FIELDS
        if local_record.get(key) != durable_record.get(key)
    ]

    if run_cfg.eval_model is None:
        msg = "local eval config missing required [eval.model]"
        raise ValueError(msg)
    local_target = run_cfg.eval_model.to_record_dict()
    durable_target = config_lib.require_config_eval_model(
        config
    ).to_record_dict()
    if local_target != durable_target:
        mismatches.append("eval_model")

    local_execution = run_cfg.eval_execution
    durable_execution = config_lib.require_config_eval_execution(config)
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
    execution: config_lib.EvalExecutionConfig,
) -> dict[str, int | str]:
    """Return eval execution fields that can change reported metrics."""
    record: dict[str, int | str] = {}
    if execution.backend is not None:
        record["backend"] = execution.backend
    if execution.limit is not None:
        record["limit"] = execution.limit
    return record


def _effective_eval_execution(
    durable: config_lib.EvalExecutionConfig,
    local: config_lib.EvalExecutionConfig,
) -> config_lib.EvalExecutionConfig:
    """Combine durable metric identity with local runtime controls.

    Args:
        durable: Prepared evaluation settings that determine metric identity.
        local: Current operator settings that may override runtime controls.

    Returns:
        Execution settings with durable backend/limit and local concurrency and
        retry controls.
    """
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
    return config_lib.EvalExecutionConfig(
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
    segments: typing.Sequence[gemini_context.EvaluationSegment],
    system_prompt: str,
    user_prompt: str,
    prior_context_mode: str,
    eval_manifest_uri: str,
) -> gemini_batch.BatchPredictionMap | None:
    """Build, submit, download, and parse one batch inference target.

    Args:
        storage_client: Client used to read and write GCS artifacts.
        run_gcs_prefix: Durable GCS prefix for the prepared run.
        gcp_project: GCP project used to submit batch inference.
        location: Preferred Vertex location for batch inference.
        model_id: Publisher model ID or full model resource name.
        label: Stable target label used in artifact paths and logs.
        segments: Transcript-free provider rows containing current audio URIs.
        system_prompt: System instruction included in every request.
        user_prompt: User instruction included in every current audio turn.
        prior_context_mode: Context encoding mode used for requests.
        eval_manifest_uri: Canonical eval manifest URI recorded in identity.

    Returns:
        Parsed predictions with their raw output URI, or ``None`` when batch
        submission or output loading fails.

    Raises:
        ImportError: If the optional Vertex dependency is unavailable.
        OSError: If a temporary batch artifact cannot be read or written.
        TypeError: If reusable request metadata has an invalid shape.
        ValueError: If request inputs or reusable metadata are inconsistent.
    """
    return gemini_batch.run_batch_audio_inference(
        storage_client=storage_client,
        run_gcs_prefix=run_gcs_prefix,
        gcp_project=gcp_project,
        location=location,
        model_id=model_id,
        label=label,
        audio_uris=[segment.audio_uri for segment in segments],
        system_prompt=system_prompt,
        user_prompt=user_prompt,
        prior_context_mode=prior_context_mode,
        eval_manifest_uri=eval_manifest_uri,
        submit_fn=vertex.submit_batch_inference,
    )


def _log_cli_error(exc: Exception) -> int:
    logger.error(str(exc))
    return 1


def _require_config_nonnegative_int(
    config: dict[str, typing.Any],
    key: str,
) -> int:
    value = config_lib.require_config_int(config, key)
    if value < 0:
        msg = f"config.json field must be a non-negative integer: {key}"
        raise ValueError(msg)
    return value
