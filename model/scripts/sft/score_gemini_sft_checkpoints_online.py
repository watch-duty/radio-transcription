"""Score Gemini SFT checkpoint endpoints with online inference.

Vertex batch inference accepts the base publisher model for this run, but it
does not accept tuned endpoint resources. This script scores SFT checkpoints
through ``models.generate_content`` while keeping the same eval manifest and
prior-context request construction used by ``gemini-sft eval``.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import tempfile
import urllib.request
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import google.auth
from common.gcs_utils import (
    download_blob_to_file,
    download_json_text,
    download_jsonl_manifest,
    parse_gcs_uri,
    upload_file_to_blob,
)
from common.gemini.batch import BatchPredictionMap
from common.gemini.context import build_context_histories
from common.gemini.prompts import GEMINI_TRANSCRIBE_KEYWORDS
from common.gemini.vertex import (
    parse_batch_output,
)
from common.scoring import build_normalizer
from gemini_sft.artifacts import (
    DEFAULT_RESULTS_DIR,
    canonical_rows_from_entries,
)
from gemini_sft.config import load_eval_run_config, require_config_str
from gemini_sft.reporting import (
    EvalReport,
    ReportArtifacts,
    TargetMetrics,
    build_target_metrics,
    render_console_report,
    render_markdown_report,
    report_to_dict,
)
from gemini_sft.target_execution import run_online_target_inference
from google.auth.transport.requests import Request
from google.cloud import storage

LOGGER = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Online-score Gemini SFT checkpoint endpoints."
    )
    parser.add_argument("--config", required=True, help="Run TOML path")
    parser.add_argument(
        "--checkpoint-id",
        action="append",
        default=[],
        help="Checkpoint id to score. Repeatable. Defaults to all checkpoints.",
    )
    parser.add_argument("--concurrency", type=int, default=16)
    parser.add_argument("--max-retries", type=int, default=3)
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Optional row limit for smoke testing. Defaults to all rows.",
    )
    return parser.parse_args()


def fetch_tuning_job(job_name: str, location: str) -> dict[str, Any]:
    creds, _ = google.auth.default(
        scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )
    creds.refresh(Request())
    url = f"https://{location}-aiplatform.googleapis.com/v1beta1/{job_name}"
    req = urllib.request.Request(
        url, headers={"Authorization": f"Bearer {creds.token}"}
    )
    with urllib.request.urlopen(req) as response:
        return json.load(response)


def checkpoint_records(
    tuning_job: dict[str, Any], checkpoint_ids: set[str]
) -> list[dict[str, str]]:
    records: list[dict[str, str]] = []
    for checkpoint in tuning_job.get("tunedModel", {}).get("checkpoints", []):
        checkpoint_id = str(checkpoint.get("checkpointId") or "")
        endpoint = str(checkpoint.get("endpoint") or "")
        if not checkpoint_id or not endpoint:
            continue
        if checkpoint_ids and checkpoint_id not in checkpoint_ids:
            continue
        records.append(
            {
                "checkpoint_id": checkpoint_id,
                "epoch": str(checkpoint.get("epoch") or ""),
                "step": str(checkpoint.get("step") or ""),
                "endpoint": endpoint,
            }
        )
    if not records:
        raise RuntimeError("No checkpoint endpoints found to score.")
    return records


def load_base_batch_predictions(
    storage_client: storage.Client, output_uri: str
) -> BatchPredictionMap:
    out_bucket, out_prefix = parse_gcs_uri(output_uri.rstrip("/") + "/")
    pred_blobs = [
        blob
        for blob in storage_client.bucket(out_bucket).list_blobs(
            prefix=out_prefix
        )
        if blob.name.endswith(".jsonl")
    ]
    if not pred_blobs:
        raise RuntimeError(f"No base prediction JSONL under {output_uri}")
    preds = BatchPredictionMap()
    with tempfile.TemporaryDirectory() as tmp_s:
        tmp = Path(tmp_s)
        for index, blob in enumerate(pred_blobs):
            local_path = tmp / f"base_predictions_{index}.jsonl"
            download_blob_to_file(
                storage_client, out_bucket, blob.name, str(local_path)
            )
            preds.update(parse_batch_output(local_path.read_text()))
    preds.output_uri = output_uri
    return preds


def upload_predictions(
    storage_client: storage.Client, local_path: Path, gcs_uri: str
) -> None:
    bucket, blob = parse_gcs_uri(gcs_uri)
    upload_file_to_blob(storage_client, bucket, blob, str(local_path))


async def score_checkpoint_online(
    *,
    checkpoint: dict[str, str],
    source_rows: list[dict[str, Any]],
    histories: list[Any],
    system_prompt: str,
    user_prompt: str,
    prior_context_count: int,
    history_mode: str,
    project: str,
    default_location: str,
    run_gcs_prefix: str,
    eval_manifest_uri: str,
    local_dir: Path,
    storage_client: storage.Client,
    args: argparse.Namespace,
) -> dict[str, str]:
    endpoint = checkpoint["endpoint"]
    LOGGER.info(
        "Scoring checkpoint %s through packaged online target executor.",
        checkpoint["checkpoint_id"],
    )
    label = f"checkpoint_{checkpoint['checkpoint_id']}"
    return await run_online_target_inference(
        storage_client=storage_client,
        run_gcs_prefix=run_gcs_prefix,
        project=project,
        default_location=default_location,
        target_label=label,
        target_model=endpoint,
        audio_uris=[str(row["audio_filepath"]) for row in source_rows],
        histories=histories,
        system_prompt=system_prompt,
        user_prompt=user_prompt,
        prior_context_count=prior_context_count,
        prior_context_mode=history_mode,
        eval_manifest_uri=eval_manifest_uri,
        local_dir=local_dir,
        concurrency=args.concurrency,
        max_retries=args.max_retries,
    )


def score_predictions(
    *,
    label: str,
    model: str,
    refs: list[str],
    hyps: list[str],
    normalizer: Any,
    missing_prediction_count: int = 0,
    artifacts: ReportArtifacts | None = None,
    metadata: dict[str, Any] | None = None,
    keywords: list[str] | None = None,
) -> TargetMetrics:
    return build_target_metrics(
        label=label,
        model=model,
        refs=refs,
        hyps=hyps,
        normalizer=normalizer,
        keywords=keywords or [],
        missing_prediction_count=missing_prediction_count,
        artifacts=artifacts,
        metadata=metadata,
    )


def write_summary(
    *,
    local_dir: Path,
    storage_client: storage.Client,
    run_gcs_prefix: str,
    report: EvalReport,
    checkpoint_rankings: list[dict[str, Any]],
) -> dict[str, Any]:
    local_dir.mkdir(parents=True, exist_ok=True)
    json_path = local_dir / "checkpoint_score_summary.json"
    md_path = local_dir / "checkpoint_score_summary.md"
    payload = report_to_dict(report)
    payload["checkpoint_rankings"] = checkpoint_rankings
    payload["best_checkpoint"] = (
        checkpoint_rankings[0] if checkpoint_rankings else None
    )
    json_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    md_path.write_text(render_markdown_report(report), encoding="utf-8")
    for path in (json_path, md_path):
        upload_predictions(
            storage_client,
            path,
            f"{run_gcs_prefix}/evals/checkpoints/{path.name}",
        )
    return payload


async def run_async(args: argparse.Namespace) -> dict[str, Any]:
    run_cfg = load_eval_run_config(args.config)
    storage_client = storage.Client(project=run_cfg.gcp_project)
    config = download_json_text(storage_client, run_cfg.paths.config_uri)
    eval_entries = download_jsonl_manifest(
        storage_client, require_config_str(config, "canonical_eval_uri")
    )
    source_rows, eval_rows = canonical_rows_from_entries(
        eval_entries,
        split="eval",
        source=require_config_str(config, "canonical_eval_uri"),
    )
    if args.limit:
        source_rows = source_rows[: args.limit]
        eval_rows = eval_rows[: args.limit]
    prior_context_count = int(config.get("prior_context_count", 0))
    histories = build_context_histories(
        source_rows,
        max_turns=prior_context_count,
    )
    history_mode = str(config.get("prior_context_mode", "text_turns"))
    refs = [row.text for row in eval_rows]
    normalizer = build_normalizer()
    run_gcs_prefix = require_config_str(config, "run_gcs_prefix")
    base_preds = load_base_batch_predictions(
        storage_client, f"{run_gcs_prefix}/evals/base/output/"
    )
    base_hyps = [base_preds.get(row.audio_filepath, "") for row in eval_rows]
    missing_prediction_count = sum(
        1 for row in eval_rows if row.audio_filepath not in base_preds
    )
    base_score = score_predictions(
        label="base",
        model=require_config_str(config, "base_model"),
        refs=refs,
        hyps=base_hyps,
        normalizer=normalizer,
        missing_prediction_count=missing_prediction_count,
        artifacts=ReportArtifacts(raw_output_uri=base_preds.output_uri),
        keywords=GEMINI_TRANSCRIBE_KEYWORDS,
    )

    tuning_job = fetch_tuning_job(
        require_config_str(config, "job_name"),
        require_config_str(config, "location"),
    )
    checkpoints = checkpoint_records(tuning_job, set(args.checkpoint_id))
    local_dir = DEFAULT_RESULTS_DIR / run_cfg.round_id / "evals" / "checkpoints"
    checkpoint_targets: list[TargetMetrics] = []
    ranking_rows: list[dict[str, Any]] = []
    summary: dict[str, Any] | None = None
    report: EvalReport | None = None
    for checkpoint in checkpoints:
        label = f"checkpoint_{checkpoint['checkpoint_id']}"
        checkpoint_preds = await score_checkpoint_online(
            checkpoint=checkpoint,
            source_rows=source_rows,
            histories=histories,
            system_prompt=require_config_str(config, "system_prompt"),
            user_prompt=require_config_str(config, "user_prompt"),
            prior_context_count=prior_context_count,
            history_mode=history_mode,
            project=require_config_str(config, "gcp_project"),
            default_location=require_config_str(config, "location"),
            run_gcs_prefix=run_gcs_prefix,
            eval_manifest_uri=require_config_str(config, "canonical_eval_uri"),
            local_dir=local_dir,
            storage_client=storage_client,
            args=args,
        )
        hyps = [
            str(checkpoint_preds.get(row.audio_filepath, ""))
            for row in eval_rows
        ]
        missing_prediction_count = sum(
            1 for row in eval_rows if row.audio_filepath not in checkpoint_preds
        )
        online_predictions_uri = checkpoint_preds.online_predictions_uri
        checkpoint_metadata = {
            "checkpoint_id": checkpoint["checkpoint_id"],
            "epoch": checkpoint["epoch"],
            "step": checkpoint["step"],
            "online_error_count": checkpoint_preds.error_count,
        }
        request_identity_hash = getattr(
            checkpoint_preds, "request_identity_hash", None
        )
        if request_identity_hash:
            checkpoint_metadata["request_identity_hash"] = request_identity_hash
        score = score_predictions(
            label=label,
            model=checkpoint["endpoint"],
            refs=refs,
            hyps=hyps,
            normalizer=normalizer,
            missing_prediction_count=missing_prediction_count,
            artifacts=ReportArtifacts(
                online_predictions_uri=online_predictions_uri
            ),
            metadata=checkpoint_metadata,
            keywords=GEMINI_TRANSCRIBE_KEYWORDS,
        )
        checkpoint_targets.append(score)
        ranking_rows.append(
            {
                "target_label": label,
                "checkpoint_id": checkpoint["checkpoint_id"],
                "epoch": checkpoint["epoch"],
                "step": checkpoint["step"],
                "endpoint": checkpoint["endpoint"],
                "delta_vs_base": round(score.wer - base_score.wer, 2),
                "online_predictions_uri": online_predictions_uri,
            }
        )
        ranked_pairs = sorted(
            zip(checkpoint_targets, ranking_rows, strict=True),
            key=lambda item: item[0].wer,
        )
        ranked_targets = [target for target, _ in ranked_pairs]
        checkpoint_rankings = [row for _, row in ranked_pairs]
        report = EvalReport(
            round_id=run_cfg.round_id,
            generated_at=datetime.now(UTC).isoformat(),
            targets=[base_score, *ranked_targets],
            metadata={
                "job_name": config.get("job_name"),
                "updated_at": datetime.now(UTC).isoformat(),
            },
        )
        summary = write_summary(
            local_dir=local_dir,
            storage_client=storage_client,
            run_gcs_prefix=run_gcs_prefix,
            report=report,
            checkpoint_rankings=checkpoint_rankings,
        )
    if summary is None or report is None:
        raise RuntimeError("No checkpoint summaries were written.")
    return {"summary": summary, "report": report}


def main() -> int:
    args = parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    for logger_name in (
        "httpx",
        "httpcore",
        "google.auth.transport.requests",
        "google.genai",
        "google_genai",
        "google_genai.models",
    ):
        logging.getLogger(logger_name).setLevel(logging.WARNING)
    result = asyncio.run(run_async(args))
    print(render_console_report(result["report"]))
    best = result["summary"]["best_checkpoint"]
    print(json.dumps({"best_checkpoint": best}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
