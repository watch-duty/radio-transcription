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
import re
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
from common.gemini.vertex import (
    GEMINI_GENERATION_CONFIG,
    GEMINI_SAFETY_SETTINGS,
    build_request,
    parse_batch_output,
)
from common.scoring import (
    build_normalizer,
    compute_cer,
    compute_wer,
    hallucination_rate,
)
from gemini_sft.artifacts import (
    DEFAULT_RESULTS_DIR,
    canonical_rows_from_entries,
)
from gemini_sft.config import load_eval_run_config, require_config_str
from google import genai
from google.auth.transport.requests import Request
from google.cloud import storage
from google.genai import types

LOGGER = logging.getLogger(__name__)
_LOCATION_RE = re.compile(r"/locations/([^/]+)/")


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
    parser.add_argument("--retry-sleep-seconds", type=float, default=2.0)
    parser.add_argument("--sync-every", type=int, default=100)
    parser.add_argument("--log-every", type=int, default=100)
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Optional row limit for smoke testing. Defaults to all rows.",
    )
    return parser.parse_args()


def resource_location(resource_name: str, default: str) -> str:
    match = _LOCATION_RE.search(resource_name)
    return match.group(1) if match else default


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


def load_prediction_rows(path: Path) -> dict[str, dict[str, Any]]:
    if not path.exists():
        return {}
    rows: dict[str, dict[str, Any]] = {}
    with path.open(encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            row = json.loads(line)
            audio_uri = str(row.get("audio_filepath") or "")
            if audio_uri:
                rows[audio_uri] = row
    return rows


def download_existing_predictions(
    storage_client: storage.Client, gcs_uri: str, local_path: Path
) -> None:
    bucket, blob = parse_gcs_uri(gcs_uri)
    gcs_blob = storage_client.bucket(bucket).blob(blob)
    if not gcs_blob.exists():
        return
    local_path.parent.mkdir(parents=True, exist_ok=True)
    download_blob_to_file(storage_client, bucket, blob, str(local_path))


def upload_predictions(
    storage_client: storage.Client, local_path: Path, gcs_uri: str
) -> None:
    bucket, blob = parse_gcs_uri(gcs_uri)
    upload_file_to_blob(storage_client, bucket, blob, str(local_path))


def append_prediction(path: Path, row: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(row) + "\n")


async def generate_with_retries(
    *,
    client: genai.Client,
    model_id: str,
    contents: list[dict[str, Any]],
    config: types.GenerateContentConfig,
    max_retries: int,
    retry_sleep_seconds: float,
) -> tuple[str, str | None]:
    last_error = "unknown error"
    for attempt in range(1, max_retries + 1):
        try:
            response = await client.aio.models.generate_content(
                model=model_id,
                contents=contents,
                config=config,
            )
        except Exception as exc:
            last_error = f"{type(exc).__name__}: {exc}"
            if attempt < max_retries:
                await asyncio.sleep(retry_sleep_seconds * attempt)
            continue
        text = (response.text or "").strip()
        if text:
            return text, None
        last_error = "empty response"
        if attempt < max_retries:
            await asyncio.sleep(retry_sleep_seconds * attempt)
    return "", last_error


async def score_checkpoint_online(
    *,
    checkpoint: dict[str, str],
    source_rows: list[dict[str, Any]],
    histories: list[Any],
    system_prompt: str,
    user_prompt: str,
    history_mode: str,
    project: str,
    default_location: str,
    local_path: Path,
    gcs_uri: str,
    storage_client: storage.Client,
    args: argparse.Namespace,
) -> dict[str, dict[str, Any]]:
    endpoint = checkpoint["endpoint"]
    client = genai.Client(
        vertexai=True,
        project=project,
        location=resource_location(endpoint, default_location),
    )
    completed = load_prediction_rows(local_path)
    LOGGER.info(
        "Scoring checkpoint %s: %s rows already complete.",
        checkpoint["checkpoint_id"],
        len(completed),
    )
    config = types.GenerateContentConfig(
        system_instruction=system_prompt,
        safety_settings=GEMINI_SAFETY_SETTINGS,
        temperature=float(GEMINI_GENERATION_CONFIG["temperature"]),
        max_output_tokens=int(GEMINI_GENERATION_CONFIG["max_output_tokens"]),
    )
    lock = asyncio.Lock()
    semaphore = asyncio.Semaphore(args.concurrency)
    progress = {"done": len(completed), "since_sync": 0}
    total = len(source_rows)

    async def process_one(index: int, row: dict[str, Any]) -> None:
        audio_uri = str(row["audio_filepath"])
        if audio_uri in completed:
            return
        request = build_request(
            audio_uri,
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            history=histories[index],
            history_mode=history_mode,
        )["request"]
        async with semaphore:
            prediction, error = await generate_with_retries(
                client=client,
                model_id=endpoint,
                contents=request["contents"],
                config=config,
                max_retries=args.max_retries,
                retry_sleep_seconds=args.retry_sleep_seconds,
            )
        out_row = {
            "audio_filepath": audio_uri,
            "pred_text": prediction,
            "error": error,
            "checkpoint_id": checkpoint["checkpoint_id"],
            "epoch": checkpoint["epoch"],
            "step": checkpoint["step"],
            "endpoint": endpoint,
        }
        async with lock:
            completed[audio_uri] = out_row
            append_prediction(local_path, out_row)
            progress["done"] += 1
            progress["since_sync"] += 1
            should_sync = (
                args.sync_every > 0
                and progress["since_sync"] >= args.sync_every
            )
            if should_sync:
                upload_predictions(storage_client, local_path, gcs_uri)
                progress["since_sync"] = 0
            if (
                error
                or progress["done"] == total
                or (
                    args.log_every > 0
                    and progress["done"] % args.log_every == 0
                )
            ):
                LOGGER.info(
                    "checkpoint=%s progress=%s/%s error=%s",
                    checkpoint["checkpoint_id"],
                    progress["done"],
                    total,
                    bool(error),
                )

    await asyncio.gather(
        *(process_one(index, row) for index, row in enumerate(source_rows))
    )
    upload_predictions(storage_client, local_path, gcs_uri)
    return completed


def score_predictions(
    *,
    label: str,
    refs: list[str],
    hyps: list[str],
    normalizer: Any,
) -> dict[str, Any]:
    wer_result = compute_wer(refs, hyps, normalizer=normalizer)
    cer_result = compute_cer(refs, hyps, normalizer=normalizer)
    return {
        "label": label,
        "wer": wer_result["wer"],
        "cer": cer_result["cer"],
        "empty_rate": hallucination_rate(hyps),
        "insertions": wer_result["insertions"],
        "deletions": wer_result["deletions"],
        "substitutions": wer_result["substitutions"],
        "hits": wer_result["hits"],
    }


def write_summary(
    *,
    local_dir: Path,
    storage_client: storage.Client,
    run_gcs_prefix: str,
    summary: dict[str, Any],
) -> None:
    local_dir.mkdir(parents=True, exist_ok=True)
    json_path = local_dir / "checkpoint_score_summary.json"
    md_path = local_dir / "checkpoint_score_summary.md"
    json_path.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    lines = [
        "# Gemini SFT Checkpoint Scores",
        "",
        f"- base WER: {summary['base']['wer']:.2f}",
        f"- base CER: {summary['base']['cer']:.2f}",
        "",
        "| checkpoint | epoch | step | WER | CER | empty_rate | delta_vs_base |",
        "|---|---:|---:|---:|---:|---:|---:|",
    ]
    for row in summary["checkpoints"]:
        lines.append(
            "| {checkpoint_id} | {epoch} | {step} | {wer:.2f} | {cer:.2f} | "
            "{empty_rate:.2f} | {delta_vs_base:.2f} |".format(**row)
        )
    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    for path in (json_path, md_path):
        upload_predictions(
            storage_client,
            path,
            f"{run_gcs_prefix}/evals/checkpoints/{path.name}",
        )


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
    histories = build_context_histories(
        source_rows,
        max_turns=int(config.get("prior_context_count", 0)),
    )
    history_mode = str(config.get("prior_context_mode", "text_turns"))
    refs = [row.text for row in eval_rows]
    normalizer = build_normalizer()
    run_gcs_prefix = require_config_str(config, "run_gcs_prefix")
    base_preds = load_base_batch_predictions(
        storage_client, f"{run_gcs_prefix}/evals/base/output/"
    )
    base_hyps = [base_preds.get(row.audio_filepath, "") for row in eval_rows]
    base_score = score_predictions(
        label="base",
        refs=refs,
        hyps=base_hyps,
        normalizer=normalizer,
    )

    tuning_job = fetch_tuning_job(
        require_config_str(config, "job_name"),
        require_config_str(config, "location"),
    )
    checkpoints = checkpoint_records(tuning_job, set(args.checkpoint_id))
    local_dir = (
        DEFAULT_RESULTS_DIR
        / run_cfg.round_id
        / "evals"
        / "checkpoints"
    )
    scored: list[dict[str, Any]] = []
    for checkpoint in checkpoints:
        label = f"checkpoint_{checkpoint['checkpoint_id']}"
        local_path = local_dir / label / "online_predictions.jsonl"
        gcs_uri = f"{run_gcs_prefix}/evals/checkpoints/{label}/online_predictions.jsonl"
        if not local_path.exists():
            download_existing_predictions(storage_client, gcs_uri, local_path)
        rows_by_audio = await score_checkpoint_online(
            checkpoint=checkpoint,
            source_rows=source_rows,
            histories=histories,
            system_prompt=require_config_str(config, "system_prompt"),
            user_prompt=require_config_str(config, "user_prompt"),
            history_mode=history_mode,
            project=require_config_str(config, "gcp_project"),
            default_location=require_config_str(config, "location"),
            local_path=local_path,
            gcs_uri=gcs_uri,
            storage_client=storage_client,
            args=args,
        )
        hyps = [
            str(rows_by_audio.get(row.audio_filepath, {}).get("pred_text") or "")
            for row in eval_rows
        ]
        score = score_predictions(
            label=label, refs=refs, hyps=hyps, normalizer=normalizer
        )
        scored.append(
            {
                **checkpoint,
                **{key: value for key, value in score.items() if key != "label"},
                "delta_vs_base": round(score["wer"] - base_score["wer"], 2),
                "prediction_gcs_uri": gcs_uri,
            }
        )
        summary = {
            "round_id": run_cfg.round_id,
            "job_name": config.get("job_name"),
            "base": base_score,
            "checkpoints": sorted(scored, key=lambda item: item["wer"]),
            "updated_at": datetime.now(UTC).isoformat(),
        }
        write_summary(
            local_dir=local_dir,
            storage_client=storage_client,
            run_gcs_prefix=run_gcs_prefix,
            summary=summary,
        )
    return summary


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
    summary = asyncio.run(run_async(args))
    best = summary["checkpoints"][0]
    print(json.dumps({"best_checkpoint": best}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
