"""Submit or resume config-driven Gemini SFT tuning jobs."""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING, Any

from common.gemini.vertex import poll_tuning_job, submit_tuning_job
from google.cloud import storage

from gemini_sft.artifacts import (
    DEFAULT_RESULTS_DIR,
    download_json_text,
    gcs_prefix_has_any_blob,
    gcs_uri_exists,
    local_config_path,
    local_run_dir,
    utc_now,
    write_and_upload_config,
    write_json_artifact,
    write_status,
)
from gemini_sft.config import RunConfig, RunConfigError, load_run_config
from gemini_sft.cost import (
    FALLBACK_SEGMENT_DURATION_SECONDS,
    confirm_tune_cost,
    print_tune_cost_estimate,
    validate_supported_model,
)
from gemini_sft.prepare import prepare_run

if TYPE_CHECKING:
    import argparse
    from pathlib import Path

logger = logging.getLogger(__name__)
RESULTS_DIR = DEFAULT_RESULTS_DIR


def tune(args: argparse.Namespace) -> int:
    """CLI handler for ``gemini-sft tune``."""
    try:
        run_cfg = load_run_config(args.config)
        validate_supported_model(run_cfg.base_model)
        storage_client = storage.Client(project=run_cfg.gcp_project)
        return tune_run(
            args=args,
            run_cfg=run_cfg,
            storage_client=storage_client,
            results_dir=RESULTS_DIR,
        )
    except (
        OSError,
        RunConfigError,
        ValueError,
        RuntimeError,
        TimeoutError,
    ) as exc:
        return _log_cli_error(exc)


def _log_cli_error(exc: Exception) -> int:
    logger.error(str(exc))
    return 1


def tune_run(
    *,
    args: argparse.Namespace,
    run_cfg: RunConfig,
    storage_client: storage.Client,
    results_dir: Path,
) -> int:
    """Submit or resume one config-driven tuning job."""
    # GCS config.json is the durable state machine for this CLI. A local
    # results/ directory is only a mirror; tune/eval must be recoverable from
    # the run prefix alone.
    if gcs_uri_exists(storage_client, run_cfg.paths.config_uri):
        config = download_json_text(storage_client, run_cfg.paths.config_uri)
        if config.get("job_name"):
            return resume_tune(run_cfg, storage_client, results_dir, config)
        if config.get("status") != "preflight_passed":
            logger.error(
                "Run prefix already exists without a resumable or prepared config; "
                "use a new round_id."
            )
            return 1
    elif gcs_prefix_has_any_blob(
        storage_client, run_cfg.paths.gcs_prefix + "/"
    ):
        logger.error(
            "Run prefix already exists without config.json; use a new round_id."
        )
        return 1
    else:
        _, config = prepare_run(
            run_cfg=run_cfg,
            storage_client=storage_client,
            results_dir=results_dir,
        )
        if config.get("status") != "preflight_passed":
            return 1

    total_secs = float(
        config.get("total_train_duration_seconds")
        or config.get("canonical_train_rows", 0)
        * FALLBACK_SEGMENT_DURATION_SECONDS
    )
    n_examples = int(config.get("canonical_train_rows") or 0)
    basis = f"{total_secs:,.0f}s actual total"
    print_tune_cost_estimate(
        n_examples=n_examples,
        epochs=run_cfg.epoch_count,
        total_secs=total_secs,
        base_model=run_cfg.base_model,
        basis=basis,
    )
    if rc := confirm_tune_cost(confirm=getattr(args, "confirm", False)):
        return rc
    return submit_prepared_tune(run_cfg, storage_client, results_dir, config)


def resume_tune(
    run_cfg: RunConfig,
    storage_client: storage.Client,
    results_dir: Path,
    config: dict[str, Any],
) -> int:
    """Resume polling a previously submitted tuning job."""
    run_dir = local_run_dir(results_dir, run_cfg.round_id)
    run_dir.mkdir(parents=True, exist_ok=True)
    local_config_path(results_dir, run_cfg.round_id).write_text(
        json.dumps(config, indent=2, default=str), encoding="utf-8"
    )
    job_name = str(config["job_name"])
    logger.info("Re-attaching to config-driven job %s", job_name)
    endpoint = poll_tuning_job(job_name, run_cfg.gcp_project, run_cfg.location)
    config.update(
        {
            "endpoint": endpoint,
            "status": "succeeded",
            "updated_at": utc_now(),
        }
    )
    config = write_and_upload_config(
        results_dir=results_dir,
        run_cfg=run_cfg,
        storage_client=storage_client,
        config=config,
    )
    write_succeeded_status(run_cfg, storage_client, results_dir, config)
    logger.info("Tune complete. Endpoint: %s", endpoint)
    return 0


def submit_prepared_tune(
    run_cfg: RunConfig,
    storage_client: storage.Client,
    results_dir: Path,
    config: dict[str, Any],
) -> int:
    """Submit a prepared Gemini SFT job, persist job name, then poll."""
    display_name = f"wd-radio-sft-{run_cfg.round_id}"
    job_name = submit_tuning_job(
        train_uri=run_cfg.paths.gemini_train_uri,
        display_name=display_name,
        project=run_cfg.gcp_project,
        location=run_cfg.location,
        base_model=run_cfg.base_model,
        val_uri=run_cfg.paths.gemini_validation_uri,
        epoch_count=run_cfg.epoch_count,
        adapter_size=run_cfg.adapter_size,
        lr_multiplier=run_cfg.learning_rate_multiplier,
    )
    config.update(
        {
            "job_name": job_name,
            "display_name": display_name,
            "status": "submitted",
            "updated_at": utc_now(),
        }
    )
    write_and_upload_config(
        results_dir=results_dir,
        run_cfg=run_cfg,
        storage_client=storage_client,
        config=config,
    )
    submitted_status = {
        "round_id": run_cfg.round_id,
        "status": "submitted",
        "job_name": job_name,
        "updated_at": utc_now(),
    }
    run_dir = local_run_dir(results_dir, run_cfg.round_id)
    write_status(
        run_dir, storage_client, run_cfg.paths.status_uri, submitted_status
    )
    write_json_artifact(
        run_dir / "tuning" / "status.json",
        storage_client,
        run_cfg.paths.tuning_status_uri,
        submitted_status,
    )
    logger.info("Persisted job_name: %s", job_name)
    endpoint = poll_tuning_job(job_name, run_cfg.gcp_project, run_cfg.location)
    config.update(
        {
            "endpoint": endpoint,
            "status": "succeeded",
            "updated_at": utc_now(),
        }
    )
    config = write_and_upload_config(
        results_dir=results_dir,
        run_cfg=run_cfg,
        storage_client=storage_client,
        config=config,
    )
    write_succeeded_status(run_cfg, storage_client, results_dir, config)
    logger.info("Tune complete. Endpoint: %s", endpoint)
    return 0


def write_succeeded_status(
    run_cfg: RunConfig,
    storage_client: storage.Client,
    results_dir: Path,
    config: dict[str, Any],
) -> None:
    """Write succeeded root and tuning status artifacts."""
    status = {
        "round_id": run_cfg.round_id,
        "status": "succeeded",
        "job_name": config.get("job_name"),
        "endpoint": config.get("endpoint"),
        "updated_at": utc_now(),
    }
    run_dir = local_run_dir(results_dir, run_cfg.round_id)
    write_status(run_dir, storage_client, run_cfg.paths.status_uri, status)
    write_json_artifact(
        run_dir / "tuning" / "status.json",
        storage_client,
        run_cfg.paths.tuning_status_uri,
        {**status, "base_model": config.get("base_model", run_cfg.base_model)},
    )
