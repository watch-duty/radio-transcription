"""Prepare config-driven Gemini SFT run artifacts."""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING, Any

from common.gemini.tuning_data import (
    build_audio_tuning_example,
    validate_audio_tuning_example,
)
from google.cloud import storage

from gemini_sft.artifacts import (
    DEFAULT_RESULTS_DIR,
    EVALS_README_TEXT,
    PreparedRunArtifacts,
    download_gcs_uri,
    gcs_prefix_has_any_blob,
    gcs_uri_exists,
    load_canonical_rows,
    local_run_dir,
    reject_split_overlap,
    upload_local_file,
    utc_now,
    write_and_upload_config,
    write_json_artifact,
    write_status,
    write_text_artifact,
)
from gemini_sft.config import RunConfig, RunConfigError, load_run_config
from gemini_sft.preflight import run_preflight

if TYPE_CHECKING:
    import argparse
    from pathlib import Path

    from common.manifest import CanonicalRow

logger = logging.getLogger(__name__)
RESULTS_DIR = DEFAULT_RESULTS_DIR


def prepare(args: argparse.Namespace) -> int:
    """CLI handler for ``gemini-sft prepare``."""
    try:
        run_cfg = load_run_config(args.config)
        storage_client = storage.Client(project=run_cfg.gcp_project)
        if gcs_uri_exists(storage_client, run_cfg.paths.config_uri):
            logger.error(
                "Run config already exists in GCS; use a new round_id or run tune/eval."
            )
            return 1
        if gcs_prefix_has_any_blob(
            storage_client, run_cfg.paths.gcs_prefix + "/"
        ):
            logger.error(
                "Run prefix already exists without config.json; use a new round_id."
            )
            return 1
        artifacts, config = prepare_run(
            run_cfg=run_cfg,
            storage_client=storage_client,
            results_dir=RESULTS_DIR,
        )
    except (OSError, RunConfigError, ValueError) as exc:
        return _log_cli_error(exc)
    logger.info(
        "Prepared %s train rows, %s validation rows, and %s eval rows.",
        artifacts.canonical_train_rows,
        artifacts.canonical_validation_rows,
        artifacts.canonical_eval_rows,
    )
    return 0 if config.get("status") == "preflight_passed" else 1


def _log_cli_error(exc: Exception) -> int:
    logger.error(str(exc))
    return 1


def prepare_run(
    *,
    run_cfg: RunConfig,
    storage_client: storage.Client,
    results_dir: Path,
) -> tuple[PreparedRunArtifacts, dict[str, Any]]:
    """Prepare local/GCS artifacts for one config-driven run."""
    run_dir = local_run_dir(results_dir, run_cfg.round_id)
    artifacts = prepare_artifacts(run_cfg, storage_client, run_dir)
    report = run_preflight(
        train_jsonl_path=artifacts.gemini_train_path,
        val_jsonl_path=artifacts.gemini_validation_path,
        storage_client=storage_client,
        report_path=artifacts.preflight_report_path,
        system_prompt=run_cfg.system_prompt,
        user_prompt=run_cfg.user_prompt,
    )
    config = {
        **run_cfg.to_record_dict(),
        "total_train_duration_seconds": artifacts.total_train_duration_seconds,
        "canonical_train_rows": artifacts.canonical_train_rows,
        "canonical_validation_rows": artifacts.canonical_validation_rows,
        "canonical_eval_rows": artifacts.canonical_eval_rows,
        "status": "preflight_passed" if report.passed else "preflight_failed",
    }
    upload_prepared_artifacts(artifacts, run_cfg, storage_client)
    config = write_and_upload_config(
        results_dir=results_dir,
        run_cfg=run_cfg,
        storage_client=storage_client,
        config=config,
    )
    status = {
        "round_id": run_cfg.round_id,
        "status": config["status"],
        "updated_at": utc_now(),
    }
    write_status(run_dir, storage_client, run_cfg.paths.status_uri, status)
    write_json_artifact(
        run_dir / "tuning" / "status.json",
        storage_client,
        run_cfg.paths.tuning_status_uri,
        {
            "round_id": run_cfg.round_id,
            "status": "not_submitted",
            "updated_at": utc_now(),
        },
    )
    write_text_artifact(
        run_dir / "evals" / "README.txt",
        storage_client,
        run_cfg.paths.evals_readme_uri,
        EVALS_README_TEXT,
    )
    if not report.passed:
        logger.error(
            "Preflight FAILED. %s issue(s) found. Report: %s.",
            len(report.failures),
            run_cfg.paths.preflight_report_uri,
        )
    return artifacts, config


def prepare_artifacts(
    run_cfg: RunConfig,
    storage_client: storage.Client,
    run_dir: Path,
) -> PreparedRunArtifacts:
    """Build canonical and Gemini model-input artifacts locally."""
    if (
        run_cfg.train_manifest_uri is None
        or run_cfg.validation_manifest_uri is None
    ):
        msg = "prepare requires train_manifest_uri and validation_manifest_uri"
        raise ValueError(msg)

    canonical_dir = run_dir / "manifests" / "canonical"
    model_inputs_dir = run_dir / "model_inputs" / "gemini"
    preflight_dir = run_dir / "preflight"
    for path in (canonical_dir, model_inputs_dir, preflight_dir):
        path.mkdir(parents=True, exist_ok=True)

    run_config_path = run_dir / "run_config.toml"
    run_config_path.write_text(run_cfg.raw_toml, encoding="utf-8")

    canonical_train_path = canonical_dir / "train.jsonl"
    canonical_validation_path = canonical_dir / "validation.jsonl"
    canonical_eval_path = canonical_dir / "eval.jsonl"
    download_gcs_uri(
        storage_client, run_cfg.train_manifest_uri, canonical_train_path
    )
    download_gcs_uri(
        storage_client,
        run_cfg.validation_manifest_uri,
        canonical_validation_path,
    )
    download_gcs_uri(
        storage_client, run_cfg.eval_manifest_uri, canonical_eval_path
    )

    _, train_rows = load_canonical_rows(canonical_train_path, "train")
    _, validation_rows = load_canonical_rows(
        canonical_validation_path, "validation"
    )
    _, eval_rows = load_canonical_rows(canonical_eval_path, "eval")
    # Training audio must stay out of both validation and eval. Validation and
    # eval may intentionally point at the same manifest for Gemini SFT runs.
    reject_split_overlap("train", train_rows, "validation", validation_rows)
    reject_split_overlap("train", train_rows, "eval", eval_rows)

    gemini_train_path = model_inputs_dir / "train.jsonl"
    gemini_validation_path = model_inputs_dir / "validation.jsonl"
    # Only train/validation need Gemini SFT JSONL. Eval remains canonical here;
    # batch-eval requests are built later so base and tuned models use the same
    # prompt/config recorded in config.json.
    write_gemini_jsonl(
        train_rows,
        gemini_train_path,
        system_prompt=run_cfg.system_prompt,
        user_prompt=run_cfg.user_prompt,
    )
    write_gemini_jsonl(
        validation_rows,
        gemini_validation_path,
        system_prompt=run_cfg.system_prompt,
        user_prompt=run_cfg.user_prompt,
    )

    return PreparedRunArtifacts(
        run_config_path=run_config_path,
        canonical_train_path=canonical_train_path,
        canonical_validation_path=canonical_validation_path,
        canonical_eval_path=canonical_eval_path,
        gemini_train_path=gemini_train_path,
        gemini_validation_path=gemini_validation_path,
        preflight_report_path=preflight_dir / "report.json",
        total_train_duration_seconds=sum(row.duration for row in train_rows),
        canonical_train_rows=len(train_rows),
        canonical_validation_rows=len(validation_rows),
        canonical_eval_rows=len(eval_rows),
    )


def write_gemini_jsonl(
    rows: list[CanonicalRow],
    path: Path,
    *,
    system_prompt: str,
    user_prompt: str,
) -> None:
    """Write Gemini audio-SFT JSONL from canonical rows."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        for row in rows:
            example = build_audio_tuning_example(
                audio_uri=row.audio_filepath,
                gt_text=row.text,
                system_prompt=system_prompt,
                user_prompt=user_prompt,
            )
            if not validate_audio_tuning_example(example):
                msg = f"invalid Gemini SFT example for {row.audio_filepath}"
                raise ValueError(msg)
            fh.write(json.dumps(example) + "\n")


def upload_prepared_artifacts(
    artifacts: PreparedRunArtifacts,
    run_cfg: RunConfig,
    storage_client: storage.Client,
) -> None:
    """Upload prepared local artifacts to their canonical GCS locations."""
    uploads = [
        (artifacts.run_config_path, run_cfg.paths.run_config_uri),
        (artifacts.canonical_train_path, run_cfg.paths.canonical_train_uri),
        (
            artifacts.canonical_validation_path,
            run_cfg.paths.canonical_validation_uri,
        ),
        (artifacts.canonical_eval_path, run_cfg.paths.canonical_eval_uri),
        (artifacts.gemini_train_path, run_cfg.paths.gemini_train_uri),
        (artifacts.gemini_validation_path, run_cfg.paths.gemini_validation_uri),
        (artifacts.preflight_report_path, run_cfg.paths.preflight_report_uri),
    ]
    for local_path, gcs_uri in uploads:
        upload_local_file(storage_client, local_path, gcs_uri)
