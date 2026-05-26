"""Watch Duty radio transcription Gemini SFT pipeline CLI.

Commands:
  build  -- Turn registered datasets into Vertex AI Gemini SFT JSONL.
  tune   -- Submit a Vertex AI Gemini SFT tuning job (--confirm gated; resume-safe).
  eval   -- Batch-infer and score a Gemini model on the held-out manifest (base-only or base+tuned).
  all    -- build -> tune -> eval in one Gemini SFT invocation.

Usage:
  python pipeline.py build --datasets echo --round-id 2026-06-01-echo
  python pipeline.py tune  --round-id 2026-06-01-echo --base-model gemini-2.5-flash --confirm
  python pipeline.py eval  --round-id 2026-06-01-echo
  python pipeline.py all   --datasets echo --round-id 2026-06-01-echo --base-model gemini-2.5-flash --confirm
"""

from __future__ import annotations

import argparse
import json
import logging
import shutil
import sys
import tomllib
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Final

if TYPE_CHECKING:
    from collections.abc import Callable

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

GCP_PROJECT: Final = "automatic-hawk-481415-m9"
GCS_BUCKET: Final = "wd-transcription-data"
GCS_SFT_PREFIX: Final = f"gs://{GCS_BUCKET}/sft"

_SCRIPT_DIR: Final = Path(__file__).resolve().parent
_DATASETS_TOML: Final = _SCRIPT_DIR / "datasets.toml"
RESULTS_DIR: Final = _SCRIPT_DIR / "results"


class PromptOverrideError(ValueError):
    """Clean CLI error for unreadable @file prompt overrides."""


def _load_registry() -> dict:
    with open(_DATASETS_TOML, "rb") as f:
        return tomllib.load(f)


def _load_round_config(round_id: str) -> dict:
    cfg_path = RESULTS_DIR / round_id / "config.json"
    if cfg_path.exists():
        return json.loads(cfg_path.read_text())
    return {}


def _parse_dataset_names(value: str) -> list[str]:
    """Parse comma-separated dataset names, preserving first occurrence order."""
    return list(dict.fromkeys(d.strip() for d in value.split(",")))


def _save_round_config(round_id: str, config: dict) -> None:
    cfg_path = RESULTS_DIR / round_id / "config.json"
    cfg_path.parent.mkdir(parents=True, exist_ok=True)
    cfg_path.write_text(json.dumps(config, indent=2, default=str))


def _load_prompt_override(value: str, label: str) -> str:
    if not value.startswith("@"):
        return value
    path = Path(value[1:]).expanduser()
    try:
        return path.read_text()
    except FileNotFoundError as exc:
        raise PromptOverrideError(
            f"{label} prompt file not found: {path}"
        ) from exc
    except OSError as exc:
        raise PromptOverrideError(
            f"could not read {label} prompt file {path}: {exc}"
        ) from exc


def _load_prompts(args: argparse.Namespace) -> tuple[str, str]:
    """Return (system_prompt, user_prompt) -- from args or pipeline defaults."""
    from prompts import PIPELINE_SYSTEM_PROMPT, PIPELINE_USER_PROMPT

    system_prompt = PIPELINE_SYSTEM_PROMPT
    user_prompt = PIPELINE_USER_PROMPT

    if getattr(args, "system_prompt", None):
        val = args.system_prompt
        system_prompt = _load_prompt_override(val, "system")

    if getattr(args, "user_prompt", None):
        val = args.user_prompt
        user_prompt = _load_prompt_override(val, "user")

    return system_prompt, user_prompt


def _overall_keyword_accuracy(rows: list[dict]) -> float | None:
    occurrences = sum(int(row.get("occurrences", 0)) for row in rows)
    if occurrences == 0:
        return None
    correct = sum(int(row.get("correctly_identified", 0)) for row in rows)
    return round(100 * correct / occurrences, 2)


def _make_adapter(
    dataset_cfg: dict, split: str, storage_client: object
) -> object:
    """Instantiate the correct adapter from a datasets.toml entry."""
    adapter_type = dataset_cfg["adapter"]
    if adapter_type == "gcs_manifest":
        from adapters.gcs_manifest import GcsManifestAdapter

        if split == "train":
            uri_key = "train_manifest_uri"
        elif split == "val":
            uri_key = "val_manifest_uri"
        elif split == "eval":
            uri_key = "eval_manifest_uri"
        else:
            raise ValueError(
                f"Unknown split: {split!r} (expected 'train', 'val', or 'eval')"
            )
        uri = dataset_cfg.get(uri_key, "")
        if not uri:
            raise ValueError(
                f"gcs_manifest adapter requires '{uri_key}' in datasets.toml "
                f"-- is empty. Ensure the cluster-split script has run (Phase 4 prerequisite)."
            )
        return GcsManifestAdapter(
            manifest_uri=uri,
            storage_client=storage_client,
            normalize=dataset_cfg.get("normalize", False),
        )
    raise ValueError(f"Unknown adapter type: {adapter_type!r}")


def _resolve_eval_manifest_uris(
    registry: dict, dataset_names: list[str]
) -> dict[str, str]:
    """Return eval manifest URIs for every configured gcs_manifest dataset."""
    resolved: dict[str, str] = {}
    for ds_name in dataset_names:
        ds_cfg = registry.get("datasets", {}).get(ds_name, {})
        if ds_cfg.get("adapter") != "gcs_manifest":
            continue
        eval_uri = ds_cfg.get("eval_manifest_uri", "")
        if eval_uri:
            resolved[ds_name] = eval_uri
    return resolved


def _build_split_jsonl(
    *,
    dataset_names: list[str],
    registry: dict,
    split: str,
    staging_dir: Path,
    storage_client: object,
    normalizer: Callable[[str], str],
    system_prompt: str,
    user_prompt: str,
    round_id: str,
) -> tuple[dict[str, str], str, float]:
    """Build per-dataset + combined Gemini SFT JSONL for one split and upload to GCS.

    Shared by the required ``train`` split and the optional ``val`` split so both go
    through identical example construction, validation, staging, and upload paths.
    Returns ``(per_dataset_uris, combined_uri, total_duration_seconds)``.
    """
    from common.gcs_utils import parse_gcs_uri, upload_file_to_blob
    from common.sft import build_example, validate_example

    per_dataset_uris: dict[str, str] = {}
    total_duration_seconds = 0.0

    for ds_name in dataset_names:
        ds_cfg = registry["datasets"][ds_name]
        adapter = _make_adapter(
            ds_cfg, split=split, storage_client=storage_client
        )
        do_normalize = ds_cfg.get("normalize", False)

        examples: list[dict] = []
        for row in adapter.iter_rows():
            text = row.text
            if do_normalize:
                text = normalizer(text)
            ex = build_example(
                audio_uri=row.audio_filepath,
                gt_text=text,
                system_prompt=system_prompt,
                user_prompt=user_prompt,
            )
            if not validate_example(ex):
                logger.warning(
                    f"[{ds_name}/{split}] skipping invalid example: {row.audio_filepath}"
                )
                continue
            examples.append(ex)
            total_duration_seconds += row.duration

        out_path = staging_dir / f"{split}_{ds_name}.jsonl"
        with open(out_path, "w") as f:
            for ex in examples:
                f.write(json.dumps(ex) + "\n")
        logger.info(
            f"[{ds_name}/{split}] wrote {len(examples)} examples -> {out_path}"
        )

        gcs_uri = f"{GCS_SFT_PREFIX}/{round_id}/{split}_{ds_name}.jsonl"
        bucket_name, blob_path = parse_gcs_uri(gcs_uri)
        upload_file_to_blob(
            storage_client, bucket_name, blob_path, str(out_path)
        )
        per_dataset_uris[ds_name] = gcs_uri
        logger.info(f"[{ds_name}/{split}] uploaded -> {gcs_uri}")

    # Combined JSONL for the exact dataset set. For a single dataset the combined name
    # equals the per-dataset name, so the per-dataset file IS the combined file --
    # re-concatenating would open that same path in "wb" (truncating it to empty) before
    # reading it, uploading an empty file. Reuse the per-dataset URI instead.
    if len(dataset_names) == 1:
        only_uri = per_dataset_uris[dataset_names[0]]
        return per_dataset_uris, only_uri, total_duration_seconds

    combined_name = "_".join(dataset_names)
    combined_path = staging_dir / f"{split}_{combined_name}.jsonl"
    with open(combined_path, "wb") as f:
        for ds_name in dataset_names:
            ds_path = staging_dir / f"{split}_{ds_name}.jsonl"
            if ds_path.exists():
                with ds_path.open("rb") as infile:
                    shutil.copyfileobj(infile, f)
    combined_uri = f"{GCS_SFT_PREFIX}/{round_id}/{split}_{combined_name}.jsonl"
    bucket_name, blob_path = parse_gcs_uri(combined_uri)
    upload_file_to_blob(
        storage_client, bucket_name, blob_path, str(combined_path)
    )
    logger.info(f"[combined/{split}] uploaded -> {combined_uri}")

    return per_dataset_uris, combined_uri, total_duration_seconds


def _build(args: argparse.Namespace) -> int:
    """Build subcommand: adapters -> Gemini SFT JSONL -> local staging -> GCS upload.

    Always builds the required ``train`` split. Also builds an optional ``val`` split
    for any dataset declaring a non-empty ``val_manifest_uri`` (D-12/PIPE-09): when set,
    ``combined_val_uri`` is recorded so ``tune`` wires a Vertex validation dataset
    (eval_total_loss). When no dataset declares one, the val split is skipped.
    """
    try:
        system_prompt, user_prompt = _load_prompts(args)
    except PromptOverrideError as e:
        logger.error(str(e))  # noqa: TRY400
        return 1

    from common.scoring import build_normalizer
    from google.cloud import storage

    registry = _load_registry()
    dataset_names = _parse_dataset_names(args.datasets)

    # Validate requested datasets exist in registry
    for ds_name in dataset_names:
        if ds_name not in registry.get("datasets", {}):
            logger.error(
                f"Dataset '{ds_name}' not found in datasets.toml. "
                f"Available: {list(registry['datasets'].keys())}"
            )
            return 1

    storage_client = storage.Client(project=GCP_PROJECT)
    staging_dir = (
        Path(args.staging_dir)
        if getattr(args, "staging_dir", None)
        else RESULTS_DIR / args.round_id / "staging"
    )
    staging_dir.mkdir(parents=True, exist_ok=True)

    normalizer = build_normalizer()

    # Train split (required)
    try:
        per_dataset_uris, combined_train_uri, total_duration_seconds = (
            _build_split_jsonl(
                dataset_names=dataset_names,
                registry=registry,
                split="train",
                staging_dir=staging_dir,
                storage_client=storage_client,
                normalizer=normalizer,
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                round_id=args.round_id,
            )
        )
    except ValueError as e:
        # e.g. echo's train_manifest_uri placeholder is empty until the Phase-4
        # cluster-split runs -- fail cleanly, not with a traceback.
        logger.error(f"cannot build train split: {e}")  # noqa: TRY400
        return 1

    # Val split (optional) -- only datasets declaring a non-empty val_manifest_uri.
    val_dataset_names = [
        ds_name
        for ds_name in dataset_names
        if registry["datasets"][ds_name].get("val_manifest_uri")
    ]
    combined_val_uri = ""
    if val_dataset_names:
        try:
            _, combined_val_uri, _ = _build_split_jsonl(
                dataset_names=val_dataset_names,
                registry=registry,
                split="val",
                staging_dir=staging_dir,
                storage_client=storage_client,
                normalizer=normalizer,
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                round_id=args.round_id,
            )
        except ValueError as e:
            logger.error(f"cannot build val split: {e}")  # noqa: TRY400
            return 1

    # Write/update config.json
    config = _load_round_config(args.round_id)
    config.update(
        {
            "round_id": args.round_id,
            "datasets": dataset_names,
            "system_prompt": system_prompt,
            "user_prompt": user_prompt,
            "train_uris": per_dataset_uris,
            "combined_train_uri": combined_train_uri,
            "combined_val_uri": combined_val_uri,
            "total_train_duration_seconds": total_duration_seconds,
        }
    )
    _save_round_config(args.round_id, config)
    logger.info(
        f"Build complete. Config: {RESULTS_DIR / args.round_id / 'config.json'}"
    )
    return 0


def _tune(args: argparse.Namespace) -> int:
    """Tune subcommand — submit or re-attach to a Vertex AI SFT tuning job.

    D-10/D-11: Persists job.name to config.json BEFORE entering the poll loop.
    On re-run, re-attaches to an in-flight job by name (no re-submit, no re-pay).
    PIPE-03: gemini-3-* base models rejected before any GCP call.
    --confirm gate: displays estimated token count + cost estimate before submitting.
    """
    import tempfile

    from common.gcs_utils import download_blob_to_file, parse_gcs_uri
    from common.vertex import poll_tuning_job, submit_tuning_job
    from google.cloud import storage
    from preflight import run_preflight
    from records import write_config

    # PIPE-03: gemini-3-* rejection (before any GCP call)
    if args.base_model.startswith("gemini-3"):
        logger.error(
            f"Base model '{args.base_model}' is rejected. "
            "gemini-3-* models are not supported by this pipeline. "
            "Use gemini-2.5-flash or a known-supported base model."
        )
        return 1

    config = _load_round_config(args.round_id)
    location = getattr(args, "location", "us-central1")

    # D-11: Resume — re-attach to an in-flight job if job_name already recorded
    if job_name := config.get("job_name"):
        from google import genai

        client = genai.Client(
            vertexai=True, project=GCP_PROJECT, location=location
        )
        cur = client.tunings.get(name=job_name)
        state = getattr(cur.state, "name", str(cur.state))
        if state in {"JOB_STATE_SUCCEEDED", "SUCCEEDED"}:
            endpoint = config.get("endpoint")
            if not endpoint:
                # Crash recovery: the job succeeded but the endpoint was never
                # persisted (the process died between submit and the endpoint write).
                # Re-read it from the job resource so eval does not silently
                # degrade to base-only.
                tuned = getattr(cur, "tuned_model", None)
                endpoint = getattr(tuned, "endpoint", None) if tuned else None
                if endpoint:
                    config["endpoint"] = endpoint
                    write_config(RESULTS_DIR, args.round_id, config)
                    logger.info(
                        f"Recovered endpoint from succeeded job {job_name}: {endpoint}"
                    )
                else:
                    logger.warning(
                        f"Job {job_name} is SUCCEEDED but exposes no endpoint; "
                        "eval will run base-only."
                    )
            else:
                logger.info(
                    f"Tuning job already succeeded. Endpoint: {endpoint}"
                )
            return 0
        if state not in {
            "JOB_STATE_FAILED",
            "FAILED",
            "JOB_STATE_CANCELLED",
            "CANCELLED",
        }:
            logger.info(
                f"Re-attaching to in-flight job {job_name} (state: {state})"
            )
            endpoint = poll_tuning_job(job_name, GCP_PROJECT, location)
            config["endpoint"] = endpoint
            write_config(RESULTS_DIR, args.round_id, config)
            return 0
        logger.warning(
            f"Prior job {job_name} ended in {state}; submitting a new job."
        )

    # Resolve train/val URIs from config
    train_uri = config.get("combined_train_uri") or ""
    val_uri = config.get("combined_val_uri", "")
    if not train_uri:
        logger.error("No combined_train_uri in config.json. Run `build` first.")
        return 1

    storage_client = storage.Client(project=GCP_PROJECT)

    # Download train JSONL for preflight and cost estimate (local temp)
    with tempfile.TemporaryDirectory() as tmp:
        train_bucket, train_blob = parse_gcs_uri(train_uri)
        train_local = Path(tmp) / "train.jsonl"
        download_blob_to_file(
            storage_client, train_bucket, train_blob, str(train_local)
        )

        val_local: Path | None = None
        if val_uri:
            val_bucket, val_blob = parse_gcs_uri(val_uri)
            val_local = Path(tmp) / "val.jsonl"
            download_blob_to_file(
                storage_client, val_bucket, val_blob, str(val_local)
            )

        system_prompt = config.get("system_prompt", "")
        user_prompt = config.get("user_prompt", "")
        preflight_report_path = (
            RESULTS_DIR / args.round_id / "preflight_report.json"
        )
        report = run_preflight(
            train_jsonl_path=train_local,
            val_jsonl_path=val_local,
            storage_client=storage_client,
            report_path=preflight_report_path,
            system_prompt=system_prompt,
            user_prompt=user_prompt,
        )
        if not report.passed:
            logger.error(
                f"Preflight FAILED. {len(report.failures)} issue(s) found. "
                f"Report: {preflight_report_path}. Fix the data and re-run."
            )
            return 1

        # Count examples for cost estimate
        with open(str(train_local)) as fh:
            n_examples = sum(1 for line in fh if line.strip())

    # Cost estimate and --confirm gate (D-13 / PIPE-03)
    AUDIO_TOKENS_PER_SEC: Final = (
        32  # VERIFIED: inference rate; ASSUMED same for SFT
    )
    COST_PER_MILLION_TOKENS: Final = (
        5.00  # Gemini 2.5 Flash supervised fine-tuning, per 1M training tokens
    )

    epochs = args.epochs
    total_secs = config.get("total_train_duration_seconds")
    if total_secs:
        total_secs = float(total_secs)
        basis = f"{total_secs:,.0f}s actual total"
    else:
        # No recorded durations (older build) -- worst-case fallback so the estimate
        # does not under-state cost (Echo segments run ~3-30s; avg ~15s).
        avg_secs = 15.0
        total_secs = n_examples * avg_secs
        basis = f"{n_examples} x {avg_secs:.1f}s avg (estimated)"
    estimated_tokens = total_secs * AUDIO_TOKENS_PER_SEC * epochs
    estimated_cost = (estimated_tokens / 1_000_000) * COST_PER_MILLION_TOKENS

    print("\n--- Tune Cost Estimate ---")
    print(f"  Examples:          {n_examples}")
    print(f"  Epochs:            {epochs}")
    print(f"  Est. audio tokens: {estimated_tokens:,.0f} ({basis} x 32 tok/s)")
    print(f"  Est. cost:        ~${estimated_cost:.2f} USD")
    print("  NOTE: Using Gemini 2.5 Flash SFT rate ($5.00/M training tokens).")
    print(
        "        Actual billing may differ. You accept responsibility for GCP charges.\n"
    )

    if not args.confirm:
        answer = input("Type 'yes' to proceed with tune: ").strip().lower()
        if answer != "yes":
            logger.info("Tune aborted by operator.")
            return 0

    # Submit (and persist job.name BEFORE polling — D-08/D-10)
    display_name = f"wd-radio-sft-{args.round_id}"
    job_name = submit_tuning_job(
        train_uri=train_uri,
        display_name=display_name,
        project=GCP_PROJECT,
        location=location,
        base_model=args.base_model,
        val_uri=val_uri or None,
        epoch_count=epochs,
        adapter_size=args.adapter_size,
        lr_multiplier=args.lr_multiplier,
    )
    config["job_name"] = job_name
    config["display_name"] = display_name
    config["base_model"] = args.base_model
    config["epochs"] = epochs
    write_config(
        RESULTS_DIR, args.round_id, config
    )  # PERSIST BEFORE POLL — D-10
    logger.info(f"Persisted job_name: {job_name}")

    # Poll to completion
    endpoint = poll_tuning_job(job_name, GCP_PROJECT, location)
    config["endpoint"] = endpoint
    write_config(RESULTS_DIR, args.round_id, config)
    logger.info(f"Tune complete. Endpoint: {endpoint}")
    return 0


def _eval(args: argparse.Namespace) -> int:
    """Eval subcommand — batch-infer and score the model on the held-out manifest.

    D-15: full scoring panel (WER, CER, ins/del/sub, empty/halluc rate, duration
    buckets, keyword accuracy, bootstrap_paired significance). Degrades gracefully
    to base-only metrics when no tuned model endpoint is available.
    """
    import tempfile

    from common.gcs_utils import (
        download_blob_to_file,
        download_jsonl_manifest,
        parse_gcs_uri,
        upload_file_to_blob,
    )
    from common.manifest import rows_from_manifest
    from common.prompts import GEMINI_TRANSCRIBE_KEYWORDS
    from common.scoring import (
        bootstrap_paired,
        build_normalizer,
        compute_cer,
        compute_wer,
        duration_bucket_wer,
        hallucination_rate,
        keyword_metrics,
    )
    from common.vertex import build_request, submit_batch_inference
    from google.cloud import storage
    from records import append_ledger, write_config, write_wer_summary

    config = _load_round_config(args.round_id)
    if not config:
        logger.error(
            f"No config.json found for round {args.round_id}. Run `build` first."
        )
        return 1

    storage_client = storage.Client(project=GCP_PROJECT)
    location = getattr(args, "location", "us-central1")
    system_prompt = config.get("system_prompt", "")
    user_prompt = config.get("user_prompt", "")

    base_only = getattr(args, "base_only", False)
    base_model = config.get("base_model", "gemini-2.5-flash")
    tuned_endpoint = config.get("endpoint")

    if not base_only and not tuned_endpoint:
        logger.warning(
            "No tuned endpoint in config.json — running base-only eval (D-15 graceful degradation)."
        )
        base_only = True

    registry = _load_registry()
    datasets = config.get("datasets", [])
    eval_uris = _resolve_eval_manifest_uris(registry, datasets)

    if not eval_uris:
        logger.error(
            "No eval_manifest_uri found for any gcs_manifest dataset in the build config. "
            "Check datasets.toml [datasets.echo] eval_manifest_uri."
        )
        return 1

    # Load eval manifest -> CanonicalRows
    eval_entries = []
    for ds_name, eval_uri in eval_uris.items():
        ds_entries = download_jsonl_manifest(storage_client, eval_uri)
        eval_entries.extend(ds_entries)
        logger.info(
            f"Eval manifest [{ds_name}]: {len(ds_entries)} rows from {eval_uri}"
        )
    eval_rows = rows_from_manifest(eval_entries)
    logger.info(
        f"Combined eval manifest: {len(eval_rows)} rows from {len(eval_uris)} dataset(s)"
    )

    def _build_batch_jsonl(
        model_id: str, label: str, tmp: str
    ) -> tuple[str, str]:
        """Write batch input JSONL, upload to GCS, return (input_gcs_uri, output_gcs_uri)."""
        batch_input_path = Path(tmp) / f"batch_input_{label}.jsonl"
        lines = []
        for row in eval_rows:
            req = build_request(
                row.audio_filepath,
                system_prompt=system_prompt,
                user_prompt=user_prompt,
            )
            lines.append(json.dumps(req))
        batch_input_path.write_text("\n".join(lines) + "\n")

        batch_input_gcs = (
            f"{GCS_SFT_PREFIX}/{args.round_id}/eval_batch_{label}_input.jsonl"
        )
        batch_output_gcs = (
            f"{GCS_SFT_PREFIX}/{args.round_id}/eval_batch_{label}_output/"
        )
        in_bucket, in_blob = parse_gcs_uri(batch_input_gcs)
        upload_file_to_blob(
            storage_client, in_bucket, in_blob, str(batch_input_path)
        )
        return batch_input_gcs, batch_output_gcs

    def _parse_batch_output(text: str) -> dict[str, str]:
        """Parse batch output JSONL lines; return {audio_uri: pred_text}."""
        result: dict[str, str] = {}
        for line in text.splitlines():
            if not line.strip():
                continue
            obj = json.loads(line)
            if (
                obj.get("status") or "request" not in obj
            ):  # error / non-prediction
                continue
            parts = obj["request"]["contents"][0]["parts"]
            # Vertex echoes the request back in camelCase even though we send
            # snake_case, so accept BOTH fileData/fileUri and file_data/file_uri.
            uri = None
            for p in parts:
                fd = p.get("file_data") or p.get("fileData")
                if fd:
                    uri = fd.get("file_uri") or fd.get("fileUri")
                    if uri:
                        break
            cands = obj.get("response", {}).get("candidates", [])
            pred = ""
            if cands:
                for tp in cands[0].get("content", {}).get("parts", []):
                    if "text" in tp:
                        pred = tp["text"]
                        break
            if uri:
                result[uri] = pred
        return result

    def _batch_infer(model_id: str, label: str) -> dict[str, str]:
        """Build batch JSONL, submit, download, parse -> {audio_uri: pred_text}."""
        with tempfile.TemporaryDirectory() as tmp:
            batch_input_gcs, batch_output_gcs = _build_batch_jsonl(
                model_id, label, tmp
            )

            output_loc = submit_batch_inference(
                input_uri=batch_input_gcs,
                output_uri=batch_output_gcs,
                model=model_id,
                project=GCP_PROJECT,
                location=location,
            )

            # Locate the batch results. The genai batches API writes them under
            # output_loc (often in a generated subfolder) and may shard the output,
            # so list and read every *.jsonl rather than hardcoding a single
            # output_loc/predictions.jsonl path (which 404s when the layout differs).
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
                    f"[{label}] no .jsonl prediction output under {output_loc} -- "
                    "batch produced no readable results."
                )
                return {}
            preds: dict[str, str] = {}
            for i, blob in enumerate(pred_blobs):
                local_path = Path(tmp) / f"predictions_{i}.jsonl"
                download_blob_to_file(
                    storage_client, out_bucket, blob.name, str(local_path)
                )
                preds.update(_parse_batch_output(local_path.read_text()))
            missing = len(eval_rows) - len(preds)
            if missing > 0:
                logger.warning(
                    f"[{label}] {missing}/{len(eval_rows)} segments returned no "
                    f"prediction (Vertex batch errors or skips) -- they score as full "
                    f"deletions; check for a batch/API failure, not just model quality."
                )
            return preds

    normalizer = build_normalizer()

    # Run base model batch inference
    logger.info("Running base model batch inference...")
    base_preds = _batch_infer(base_model, "base")

    refs = [row.text for row in eval_rows]
    durations = [row.duration for row in eval_rows]
    base_hyps = [base_preds.get(row.audio_filepath, "") for row in eval_rows]

    # compute_wer / compute_cer return dicts — extract the numeric value
    base_wer_result = compute_wer(refs, base_hyps, normalizer=normalizer)
    base_wer = base_wer_result["wer"]
    base_cer_result = compute_cer(refs, base_hyps, normalizer=normalizer)
    base_cer = base_cer_result["cer"]

    metrics: dict = {
        "round_id": args.round_id,
        "base_model": base_model,
        "base_wer": base_wer,
        "base_cer": base_cer,
        "n_eval_examples": len(eval_rows),
    }

    # Ins/del/sub breakdown (from the compute_wer result dict)
    total_ref_words = sum(len(r.split()) for r in refs)
    if total_ref_words > 0:
        metrics["base_insertions"] = (
            base_wer_result["insertions"] / total_ref_words * 100
        )
        metrics["base_deletions"] = (
            base_wer_result["deletions"] / total_ref_words * 100
        )
        metrics["base_substitutions"] = (
            base_wer_result["substitutions"] / total_ref_words * 100
        )

    # Empty/hallucinated rate
    metrics["base_empty_rate"] = hallucination_rate(base_hyps)

    base_keyword_rows = keyword_metrics(
        refs, base_hyps, GEMINI_TRANSCRIBE_KEYWORDS
    )
    metrics["base_keyword_metrics"] = base_keyword_rows
    metrics["base_keyword_accuracy"] = _overall_keyword_accuracy(
        base_keyword_rows
    )

    # Duration bucket WER (D-15)
    try:
        bucket_results = duration_bucket_wer(
            refs, base_hyps, durations, normalizer=normalizer
        )
        metrics["duration_buckets"] = [
            {"bucket": b["bucket"], "base_wer": b["wer"]}
            for b in bucket_results
        ]
    except Exception as e:
        logger.warning(f"Could not compute duration bucket WER: {e}")

    if not base_only and tuned_endpoint:
        logger.info("Running tuned model batch inference...")
        tuned_preds = _batch_infer(tuned_endpoint, "tuned")
        tuned_hyps = [
            tuned_preds.get(row.audio_filepath, "") for row in eval_rows
        ]

        tuned_wer_result = compute_wer(refs, tuned_hyps, normalizer=normalizer)
        tuned_wer = tuned_wer_result["wer"]
        tuned_cer_result = compute_cer(refs, tuned_hyps, normalizer=normalizer)
        tuned_cer = tuned_cer_result["cer"]

        metrics["tuned_wer"] = tuned_wer
        metrics["tuned_cer"] = tuned_cer
        metrics["tuned_empty_rate"] = hallucination_rate(tuned_hyps)
        tuned_keyword_rows = keyword_metrics(
            refs, tuned_hyps, GEMINI_TRANSCRIBE_KEYWORDS
        )
        metrics["tuned_keyword_metrics"] = tuned_keyword_rows
        metrics["tuned_keyword_accuracy"] = _overall_keyword_accuracy(
            tuned_keyword_rows
        )

        if total_ref_words > 0:
            metrics["tuned_insertions"] = (
                tuned_wer_result["insertions"] / total_ref_words * 100
            )
            metrics["tuned_deletions"] = (
                tuned_wer_result["deletions"] / total_ref_words * 100
            )
            metrics["tuned_substitutions"] = (
                tuned_wer_result["substitutions"] / total_ref_words * 100
            )

        # Bootstrap significance (D-15): bootstrap_paired takes (refs, hyps_a, hyps_b)
        try:
            bs = bootstrap_paired(
                refs, base_hyps, tuned_hyps, normalizer=normalizer
            )
            metrics["bootstrap_p_value"] = bs.get("p_value_one_sided")
            metrics["bootstrap_ci_low"] = bs.get("ci_low")
            metrics["bootstrap_ci_high"] = bs.get("ci_high")
            metrics["bootstrap_delta"] = bs.get("delta")
        except Exception as e:
            logger.warning(f"bootstrap_paired failed: {e}")

        # Tuned duration bucket WER
        try:
            tuned_bucket_results = duration_bucket_wer(
                refs, tuned_hyps, durations, normalizer=normalizer
            )
            # Merge tuned WER into the existing bucket entries
            tuned_wer_by_bucket = {
                b["bucket"]: b["wer"] for b in tuned_bucket_results
            }
            if "duration_buckets" in metrics:
                for entry in metrics["duration_buckets"]:
                    entry["tuned_wer"] = tuned_wer_by_bucket.get(
                        entry["bucket"]
                    )
        except Exception as e:
            logger.warning(f"Could not compute tuned duration bucket WER: {e}")

    # Write records (PIPE-07)
    write_wer_summary(RESULTS_DIR, args.round_id, metrics)
    config.update(
        {
            "base_model": base_model,
            "base_wer": metrics.get("base_wer"),
            "tuned_wer": metrics.get("tuned_wer"),
        }
    )
    write_config(RESULTS_DIR, args.round_id, config)
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
        f"Eval complete. WER summary: {RESULTS_DIR / args.round_id / 'wer_summary.md'}"
    )
    return 0


def _all(args: argparse.Namespace) -> int:
    """All subcommand — build -> tune -> eval in one invocation."""
    rc = _build(args)
    if rc != 0:
        return rc
    if not getattr(args, "base_only", False):
        rc = _tune(args)
        if rc != 0:
            return rc
    return _eval(args)


def _add_build_args(p: argparse.ArgumentParser) -> None:
    p.add_argument(
        "--datasets",
        required=True,
        help="Comma-separated dataset names from datasets.toml (e.g. echo)",
    )
    p.add_argument(
        "--round-id",
        required=True,
        help="Round identifier: YYYY-MM-DD-<slug> (e.g. 2026-06-01-echo)",
    )
    p.add_argument(
        "--system-prompt",
        default="",
        help="Override system prompt: inline string or @path/to/file",
    )
    p.add_argument(
        "--user-prompt",
        default="",
        help="Override user prompt: inline string or @path/to/file",
    )
    p.add_argument(
        "--staging-dir",
        default="",
        help="Local staging dir for JSONL files (default: results/<round-id>/staging/)",
    )


def _add_tune_args(p: argparse.ArgumentParser) -> None:
    p.add_argument(
        "--round-id",
        required=True,
        help="Round identifier (must match build output)",
    )
    p.add_argument(
        "--base-model",
        default="gemini-2.5-flash",
        help="Base model name (gemini-3-* rejected)",
    )
    p.add_argument(
        "--epochs", type=int, default=1, help="Training epochs (default: 1)"
    )
    p.add_argument(
        "--adapter-size",
        default="EIGHT",
        choices=["ONE", "FOUR", "EIGHT", "SIXTEEN"],
        help="Adapter size",
    )
    p.add_argument(
        "--lr-multiplier",
        type=float,
        default=1.0,
        help="Learning-rate multiplier",
    )
    p.add_argument(
        "--confirm",
        action="store_true",
        help="Skip interactive cost-confirmation prompt",
    )
    p.add_argument("--location", default="us-central1", help="Vertex AI region")


def _add_eval_args(p: argparse.ArgumentParser) -> None:
    p.add_argument("--round-id", required=True, help="Round identifier")
    p.add_argument(
        "--base-only",
        action="store_true",
        help="Eval base model only (no tuned model)",
    )
    p.add_argument("--location", default="us-central1", help="Vertex AI region")


def _add_all_args(p: argparse.ArgumentParser) -> None:
    _add_build_args(p)
    # Tune-specific args (--round-id is already added by _add_build_args above)
    p.add_argument(
        "--base-model",
        default="gemini-2.5-flash",
        help="Base model name (gemini-3-* rejected)",
    )
    p.add_argument(
        "--epochs", type=int, default=1, help="Training epochs (default: 1)"
    )
    p.add_argument(
        "--adapter-size",
        default="EIGHT",
        choices=["ONE", "FOUR", "EIGHT", "SIXTEEN"],
        help="Adapter size",
    )
    p.add_argument(
        "--lr-multiplier",
        type=float,
        default=1.0,
        help="Learning-rate multiplier",
    )
    p.add_argument(
        "--confirm",
        action="store_true",
        help="Skip interactive cost-confirmation prompt",
    )
    p.add_argument("--location", default="us-central1", help="Vertex AI region")
    p.add_argument(
        "--base-only",
        action="store_true",
        help="Skip tune; eval base model only",
    )


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Watch Duty radio transcription Gemini SFT pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    sub = ap.add_subparsers(
        dest="cmd", required=True, metavar="{build,tune,eval,all}"
    )
    _add_build_args(
        sub.add_parser(
            "build", help="Build Gemini SFT JSONL from registered datasets"
        )
    )
    _add_tune_args(
        sub.add_parser(
            "tune",
            help="Submit Vertex AI Gemini SFT tuning job (--confirm required)",
        )
    )
    _add_eval_args(
        sub.add_parser(
            "eval",
            help="Batch-infer and score Gemini model on held-out manifest",
        )
    )
    _add_all_args(
        sub.add_parser(
            "all", help="build -> tune -> eval in one Gemini SFT invocation"
        )
    )
    args = ap.parse_args()
    dispatch = {"build": _build, "tune": _tune, "eval": _eval, "all": _all}
    return dispatch[args.cmd](args)


if __name__ == "__main__":
    sys.exit(main())
