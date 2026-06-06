"""Evaluate base and tuned Gemini models for a config-driven SFT run."""

from __future__ import annotations

import argparse
import json
import logging
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from common.gcs_utils import (
    download_blob_to_file,
    download_jsonl_manifest,
    parse_gcs_uri,
    upload_file_to_blob,
)
from common.gemini.prompts import GEMINI_TRANSCRIBE_KEYWORDS
from common.gemini.vertex import (
    build_request,
    parse_batch_output,
    submit_batch_inference,
)
from common.manifest import rows_from_manifest
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
    download_json_text,
    gcs_uri_exists,
    write_and_upload_config,
)
from gemini_sft.config import RunConfigError, load_run_config
from gemini_sft.cost import DEFAULT_BASE_MODEL
from gemini_sft.records import append_ledger, write_wer_summary

logger = logging.getLogger(__name__)
RESULTS_DIR = DEFAULT_RESULTS_DIR


def evaluate(args: argparse.Namespace) -> int:
    """CLI handler for ``gemini-sft eval``."""
    try:
        run_cfg = load_run_config(args.config)
        storage_client = storage.Client(project=run_cfg.gcp_project)
        if not gcs_uri_exists(storage_client, run_cfg.paths.config_uri):
            logger.error(
                "No GCS config.json found for round %s.", run_cfg.round_id
            )
            return 1
        config = download_json_text(storage_client, run_cfg.paths.config_uri)
        return evaluate_run(args, run_cfg, storage_client, config)
    except (
        OSError,
        RunConfigError,
        ValueError,
        RuntimeError,
        TimeoutError,
    ) as exc:
        logger.error(str(exc))
        return 1


def evaluate_run(
    args: argparse.Namespace,
    run_cfg: Any,
    storage_client: storage.Client,
    config: dict[str, Any],
) -> int:
    """Run batch inference and score one config-driven run."""
    system_prompt = str(config.get("system_prompt") or run_cfg.system_prompt)
    user_prompt = str(config.get("user_prompt") or run_cfg.user_prompt)
    base_model = str(config.get("base_model") or DEFAULT_BASE_MODEL)
    tuned_endpoint = config.get("endpoint")
    base_only = bool(getattr(args, "base_only", False))
    if not base_only and not tuned_endpoint:
        logger.warning(
            "No tuned endpoint in config.json; running base-only eval."
        )
        base_only = True

    eval_entries = download_jsonl_manifest(
        storage_client, run_cfg.eval_manifest_uri
    )
    eval_rows = rows_from_manifest(eval_entries)
    if not eval_rows:
        logger.error(
            "Eval manifest has no parsed rows: %s", run_cfg.eval_manifest_uri
        )
        return 1

    base_preds = batch_infer(
        storage_client=storage_client,
        run_cfg=run_cfg,
        model_id=base_model,
        label="base",
        eval_rows=eval_rows,
        system_prompt=system_prompt,
        user_prompt=user_prompt,
    )
    if base_preds is None:
        return 1

    refs = [row.text for row in eval_rows]
    durations = [row.duration for row in eval_rows]
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
    metrics["base_batch_output_uri"] = base_preds.output_uri

    if not base_only and tuned_endpoint:
        tuned_preds = batch_infer(
            storage_client=storage_client,
            run_cfg=run_cfg,
            model_id=str(tuned_endpoint),
            label="tuned",
            eval_rows=eval_rows,
            system_prompt=system_prompt,
            user_prompt=user_prompt,
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
            "datasets": config.get("datasets", []),
            "epochs": config.get("epochs", "—"),
            "git_sha": config.get("git_sha", "—"),
            "timestamp": datetime.now(UTC).strftime("%Y-%m-%d"),
        },
    )
    logger.info(
        "Eval complete. WER summary: %s",
        RESULTS_DIR / run_cfg.round_id / "wer_summary.md",
    )
    return 0


class PredictionMap(dict[str, str]):
    """Prediction map with the GCS output URI attached for provenance."""

    output_uri: str


def batch_infer(
    *,
    storage_client: storage.Client,
    run_cfg: Any,
    model_id: str,
    label: str,
    eval_rows: list[Any],
    system_prompt: str,
    user_prompt: str,
) -> PredictionMap | None:
    """Build batch input JSONL, submit, download outputs, and parse predictions."""
    with tempfile.TemporaryDirectory() as tmp:
        batch_input_gcs, batch_output_gcs = build_batch_jsonl(
            storage_client=storage_client,
            run_cfg=run_cfg,
            label=label,
            eval_rows=eval_rows,
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            tmp_dir=Path(tmp),
        )
        try:
            output_loc = submit_batch_inference(
                input_uri=batch_input_gcs,
                output_uri=batch_output_gcs,
                model=model_id,
                project=run_cfg.gcp_project,
                location=run_cfg.location,
            )
        except (RuntimeError, TimeoutError) as exc:
            logger.error("[%s] Batch inference failed: %s", label, exc)
            return None

        out_bucket, out_prefix = parse_gcs_uri(output_loc.rstrip("/") + "/")
        pred_blobs = [
            blob
            for blob in storage_client.bucket(out_bucket).list_blobs(
                prefix=out_prefix
            )
            if blob.name.endswith(".jsonl")
        ]
        if not pred_blobs:
            logger.error(
                "[%s] no .jsonl prediction output under %s.",
                label,
                output_loc,
            )
            preds = PredictionMap()
            preds.output_uri = output_loc
            return preds
        preds = PredictionMap()
        for i, blob in enumerate(pred_blobs):
            local_path = Path(tmp) / f"predictions_{i}.jsonl"
            download_blob_to_file(
                storage_client, out_bucket, blob.name, str(local_path)
            )
            preds.update(
                parse_batch_output(local_path.read_text(encoding="utf-8"))
            )
        expected_count = len({row.audio_filepath for row in eval_rows})
        missing = max(0, expected_count - len(preds))
        if missing > 0:
            logger.warning(
                "[%s] %s/%s unique segments returned no prediction; they "
                "score as full deletions.",
                label,
                missing,
                expected_count,
            )
        preds.output_uri = output_loc
        return preds


def build_batch_jsonl(
    *,
    storage_client: storage.Client,
    run_cfg: Any,
    label: str,
    eval_rows: list[Any],
    system_prompt: str,
    user_prompt: str,
    tmp_dir: Path,
) -> tuple[str, str]:
    """Write and upload a Vertex batch input JSONL file."""
    batch_input_path = tmp_dir / f"batch_input_{label}.jsonl"
    with batch_input_path.open("w", encoding="utf-8") as fh:
        for row in eval_rows:
            fh.write(
                json.dumps(
                    build_request(
                        row.audio_filepath,
                        system_prompt=system_prompt,
                        user_prompt=user_prompt,
                    )
                )
                + "\n"
            )
    batch_input_gcs = f"{run_cfg.paths.gcs_prefix}/evals/{label}/input.jsonl"
    batch_output_gcs = f"{run_cfg.paths.gcs_prefix}/evals/{label}/output/"
    in_bucket, in_blob = parse_gcs_uri(batch_input_gcs)
    upload_file_to_blob(
        storage_client, in_bucket, in_blob, str(batch_input_path)
    )
    return batch_input_gcs, batch_output_gcs


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
    add_error_breakdown(metrics, "base", base_wer_result, refs)
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
    add_error_breakdown(metrics, "tuned", tuned_wer_result, refs)
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
    refs: list[str],
) -> None:
    """Add insertion/deletion/substitution rates to a metrics dictionary."""
    total_ref_words = sum(len(ref.split()) for ref in refs)
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
