"""Prepare config-driven Gemini SFT run artifacts."""

from __future__ import annotations

import json
import logging
import typing

from common import gcs_utils
from common.gemini import context, tuning_data
from google.api_core import exceptions as google_exceptions
from google.cloud import storage

from gemini_sft import artifacts as artifacts_lib
from gemini_sft import config as config_lib
from gemini_sft import preflight

if typing.TYPE_CHECKING:
    import argparse
    import pathlib

logger = logging.getLogger(__name__)
RESULTS_DIR = artifacts_lib.DEFAULT_RESULTS_DIR


def prepare(args: argparse.Namespace) -> int:
    """Prepare and publish one training or eval-only run.

    Args:
        args: Parsed CLI namespace containing the config path.

    Returns:
        Zero when preparation succeeds; one for a validation or I/O failure.
    """
    try:
        run_cfg = config_lib.load_prepare_run_config(args.config)
        storage_client = storage.Client(project=run_cfg.gcp_project)
        if gcs_utils.gcs_uri_exists(storage_client, run_cfg.paths.config_uri):
            logger.error(
                "Run config already exists in GCS; use a new round_id or run tune/eval."
            )
            return 1
        if gcs_utils.gcs_prefix_has_any_blob(
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
    except (
        OSError,
        google_exceptions.GoogleAPIError,
        config_lib.RunConfigError,
        TypeError,
        ValueError,
    ) as exc:
        return _log_cli_error(exc)
    if isinstance(artifacts, artifacts_lib.PreparedEvalArtifacts):
        logger.info(
            "Prepared eval-only round with %s eval rows.",
            artifacts.canonical_eval_rows,
        )
        return 0 if config.get("status") == "eval_prepared" else 1
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
    run_cfg: config_lib.RunConfig,
    storage_client: storage.Client,
    results_dir: pathlib.Path,
) -> tuple[
    artifacts_lib.PreparedRunArtifacts | artifacts_lib.PreparedEvalArtifacts,
    dict[str, typing.Any],
]:
    """Prepare and publish one validated training or eval-only run.

    Args:
        run_cfg: Validated preparation config.
        storage_client: Client used for source and durable GCS artifacts.
        results_dir: Local root for the run mirror.

    Returns:
        Prepared local artifacts and the durable config record.

    Raises:
        google_exceptions.GoogleAPIError: If a GCS operation fails.
        OSError: If local or GCS artifacts cannot be read or written.
        ValueError: If strict parsing, canonical validation, or preparation
            invariants fail.
    """
    if (
        run_cfg.train_manifest_uri is None
        and run_cfg.validation_manifest_uri is None
    ):
        return _prepare_eval_run(
            run_cfg=run_cfg,
            storage_client=storage_client,
            results_dir=results_dir,
        )
    run_dir = artifacts_lib.local_run_dir(results_dir, run_cfg.round_id)
    artifacts = prepare_artifacts(run_cfg, storage_client, run_dir)
    report = preflight.run_preflight(
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
    config = artifacts_lib.write_and_upload_config(
        results_dir=results_dir,
        run_cfg=run_cfg,
        storage_client=storage_client,
        config=config,
    )
    status = {
        "round_id": run_cfg.round_id,
        "status": config["status"],
        "updated_at": artifacts_lib.utc_now(),
    }
    artifacts_lib.write_status(
        run_dir,
        storage_client,
        run_cfg.paths.status_uri,
        status,
    )
    artifacts_lib.write_json_artifact(
        run_dir / "tuning" / "status.json",
        storage_client,
        run_cfg.paths.tuning_status_uri,
        {
            "round_id": run_cfg.round_id,
            "status": "not_submitted",
            "updated_at": artifacts_lib.utc_now(),
        },
    )
    artifacts_lib.write_text_artifact(
        run_dir / "evals" / "README.txt",
        storage_client,
        run_cfg.paths.evals_readme_uri,
        artifacts_lib.EVALS_README_TEXT,
    )
    if not report.passed:
        logger.error(
            "Preflight FAILED. %s issue(s) found. Report: %s.",
            len(report.failures),
            run_cfg.paths.preflight_report_uri,
        )
    return artifacts, config


def _prepare_eval_run(
    *,
    run_cfg: config_lib.RunConfig,
    storage_client: storage.Client,
    results_dir: pathlib.Path,
) -> tuple[artifacts_lib.PreparedEvalArtifacts, dict[str, typing.Any]]:
    """Prepare and publish the durable state for one eval-only run.

    Args:
        run_cfg: Validated eval-only preparation config.
        storage_client: Client used for source and durable GCS artifacts.
        results_dir: Local root for the run mirror.

    Returns:
        Prepared local eval artifacts and their durable config record.

    Raises:
        google_exceptions.GoogleAPIError: If a GCS operation fails.
        OSError: If a local or GCS artifact cannot be read or written.
        ValueError: If strict parsing, the eval manifest, or the target is
            invalid.
    """
    run_dir = artifacts_lib.local_run_dir(results_dir, run_cfg.round_id)
    artifacts = _prepare_eval_artifacts(run_cfg, storage_client, run_dir)
    config = {
        **run_cfg.to_record_dict(),
        "canonical_eval_rows": artifacts.canonical_eval_rows,
        "status": "eval_prepared",
    }
    for local_path, gcs_uri in (
        (artifacts.run_config_path, run_cfg.paths.run_config_uri),
        (artifacts.canonical_eval_path, run_cfg.paths.canonical_eval_uri),
    ):
        gcs_utils.upload_local_file(storage_client, local_path, gcs_uri)
    config = artifacts_lib.write_and_upload_config(
        results_dir=results_dir,
        run_cfg=run_cfg,
        storage_client=storage_client,
        config=config,
    )
    return artifacts, config


def _prepare_eval_artifacts(
    run_cfg: config_lib.RunConfig,
    storage_client: storage.Client,
    run_dir: pathlib.Path,
) -> artifacts_lib.PreparedEvalArtifacts:
    """Build and validate local artifacts for one eval-only run.

    Args:
        run_cfg: Validated eval-only preparation config.
        storage_client: Client used to download the source eval manifest.
        run_dir: Local directory for the run mirror.

    Returns:
        Paths and row count for the validated local eval artifacts.

    Raises:
        google_exceptions.GoogleAPIError: If the source download fails.
        OSError: If a local or GCS artifact cannot be read or written.
        ValueError: If strict parsing, the eval manifest, or the target is
            invalid.
    """
    if run_cfg.eval_model is None:
        msg = "eval-only prepare requires one [eval.model] target"
        raise ValueError(msg)
    canonical_dir = run_dir / "manifests" / "canonical"
    canonical_dir.mkdir(parents=True, exist_ok=True)
    run_config_path = run_dir / "run_config.toml"
    run_config_path.write_text(run_cfg.raw_toml, encoding="utf-8")
    canonical_eval_path = canonical_dir / "eval.jsonl"
    gcs_utils.download_gcs_uri(
        storage_client,
        run_cfg.eval_manifest_uri,
        canonical_eval_path,
    )
    _, eval_rows = artifacts_lib.load_canonical_rows(
        canonical_eval_path,
        "eval",
    )
    return artifacts_lib.PreparedEvalArtifacts(
        run_config_path=run_config_path,
        canonical_eval_path=canonical_eval_path,
        canonical_eval_rows=len(eval_rows),
    )


def prepare_artifacts(
    run_cfg: config_lib.RunConfig,
    storage_client: storage.Client,
    run_dir: pathlib.Path,
) -> artifacts_lib.PreparedRunArtifacts:
    """Build canonical and Gemini model-input artifacts locally.

    Args:
        run_cfg: Validated training-mode run configuration.
        storage_client: Client used to download source manifests.
        run_dir: Local directory for the prepared run mirror.

    Returns:
        Paths and counts for the validated training artifacts.

    Raises:
        google_exceptions.GoogleAPIError: If a source download fails.
        OSError: If a local artifact cannot be read or written.
        ValueError: If strict parsing, training manifests, or generated
            examples are invalid.
    """
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
    gcs_utils.download_gcs_uri(
        storage_client, run_cfg.train_manifest_uri, canonical_train_path
    )
    gcs_utils.download_gcs_uri(
        storage_client,
        run_cfg.validation_manifest_uri,
        canonical_validation_path,
    )
    gcs_utils.download_gcs_uri(
        storage_client, run_cfg.eval_manifest_uri, canonical_eval_path
    )

    train_entries, train_rows = artifacts_lib.load_canonical_rows(
        canonical_train_path, "train"
    )
    validation_entries, validation_rows = artifacts_lib.load_canonical_rows(
        canonical_validation_path, "validation"
    )
    _, eval_rows = artifacts_lib.load_canonical_rows(
        canonical_eval_path,
        "eval",
    )
    # Training audio must stay out of both validation and eval. Validation and
    # eval may intentionally point at the same manifest for Gemini SFT runs.
    artifacts_lib.reject_split_overlap(
        "train",
        train_rows,
        "validation",
        validation_rows,
    )
    artifacts_lib.reject_split_overlap("train", train_rows, "eval", eval_rows)

    gemini_train_path = model_inputs_dir / "train.jsonl"
    gemini_validation_path = model_inputs_dir / "validation.jsonl"
    # Only train/validation need Gemini SFT JSONL. Eval remains canonical here;
    # eval requests are built later from the prompt/config recorded in
    # config.json.
    write_gemini_jsonl(
        train_entries,
        gemini_train_path,
        system_prompt=run_cfg.system_prompt,
        user_prompt=run_cfg.user_prompt,
        prior_context_count=run_cfg.prior_context_count,
        prior_context_mode=run_cfg.prior_context_mode,
    )
    write_gemini_jsonl(
        validation_entries,
        gemini_validation_path,
        system_prompt=run_cfg.system_prompt,
        user_prompt=run_cfg.user_prompt,
        prior_context_count=run_cfg.prior_context_count,
        prior_context_mode=run_cfg.prior_context_mode,
    )

    return artifacts_lib.PreparedRunArtifacts(
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
    rows: list[dict[str, typing.Any]],
    path: pathlib.Path,
    *,
    system_prompt: str,
    user_prompt: str,
    prior_context_count: int = 0,
    prior_context_mode: str = "text_turns",
) -> None:
    """Write Gemini audio-SFT JSONL from canonical rows.

    Args:
        rows: Canonical manifest rows to convert in input order.
        path: Local JSONL destination path.
        system_prompt: System instruction included in every example.
        user_prompt: User instruction included in every current audio turn.
        prior_context_count: Maximum prior same-source transcript turns.
        prior_context_mode: Context encoding mode used for each example.

    Raises:
        OSError: If the destination cannot be created or written.
        ValueError: If context settings or a generated example are invalid.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    histories = context.build_context_histories(
        rows,
        max_turns=prior_context_count,
    )
    with path.open("w", encoding="utf-8") as fh:
        for row, history in zip(rows, histories, strict=True):
            audio_uri = str(row.get("audio_filepath") or "")
            example = tuning_data.build_audio_tuning_example(
                audio_uri=audio_uri,
                gt_text=str(row.get("text") or ""),
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                history=history,
                history_mode=prior_context_mode,
            )
            if not tuning_data.validate_audio_tuning_example(example):
                msg = f"invalid Gemini SFT example for {audio_uri}"
                raise ValueError(msg)
            fh.write(json.dumps(example) + "\n")


def upload_prepared_artifacts(
    artifacts: artifacts_lib.PreparedRunArtifacts,
    run_cfg: config_lib.RunConfig,
    storage_client: storage.Client,
) -> None:
    """Upload prepared local artifacts to their canonical GCS locations.

    Args:
        artifacts: Validated local training artifact paths.
        run_cfg: Run configuration containing durable destination URIs.
        storage_client: Client used to upload each artifact.

    Raises:
        google_exceptions.GoogleAPIError: If a GCS upload fails.
        OSError: If a local artifact cannot be read.
    """
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
        gcs_utils.upload_local_file(storage_client, local_path, gcs_uri)
