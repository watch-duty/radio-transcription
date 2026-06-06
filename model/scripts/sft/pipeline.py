"""Watch Duty radio transcription Gemini SFT pipeline CLI.

Commands:
  build  -- Turn registered datasets into Vertex AI Gemini SFT JSONL.
  tune   -- Submit a Vertex AI Gemini SFT tuning job (--confirm gated; resume-safe).
  eval   -- Batch-infer and score a Gemini model on the held-out manifest (base-only or base+tuned).
  all    -- build -> tune -> eval in one Gemini SFT invocation.

Usage:
  python pipeline.py build --datasets echo --round-id 2026-06-01-echo
  python pipeline.py tune  --round-id 2026-06-01-echo --base-model gemini-3.1-flash-lite --confirm
  python pipeline.py eval  --round-id 2026-06-01-echo
  python pipeline.py all   --datasets echo --round-id 2026-06-01-echo --base-model gemini-3.1-flash-lite --confirm
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import shutil
import sys
import tempfile
import tomllib
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Final, TypedDict

from adapters.gcs_manifest import GcsManifestAdapter
from common.gcs_utils import (
    download_blob_to_file,
    download_jsonl_manifest,
    parse_gcs_uri,
    upload_file_to_blob,
)
from common.manifest import (
    CanonicalRow,
    DatasetAdapter,
    load_manifest,
    rows_from_manifest,
)
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
from common.sft import build_example, validate_example
from common.vertex import (
    build_request,
    get_tuning_job_status,
    poll_tuning_job,
    submit_batch_inference,
    submit_tuning_job,
)
from google.cloud import storage
from preflight import run_preflight
from prompts import PIPELINE_SYSTEM_PROMPT, PIPELINE_USER_PROMPT
from records import append_ledger, write_config, write_wer_summary
from run_config import RunConfig, RunConfigError, load_run_config

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

DEFAULT_GCP_PROJECT: Final = "automatic-hawk-481415-m9"
DEFAULT_GCS_BUCKET: Final = "wd-transcription-data"
GCP_PROJECT_ENV_VAR: Final = "SFT_GCP_PROJECT"
GCS_BUCKET_ENV_VAR: Final = "SFT_GCS_BUCKET"
DEFAULT_BASE_MODEL: Final = "gemini-3.1-flash-lite"
FALLBACK_SEGMENT_DURATION_SECONDS: Final = 15.0
SUPPORTED_SFT_BASE_MODELS: Final = frozenset(
    {
        "gemini-3.1-flash-lite",
        "gemini-2.5-pro",
        "gemini-2.5-flash",
        "gemini-2.5-flash-lite",
    }
)
SFT_MODEL_DISPLAY_NAMES: Final = {
    "gemini-3.1-flash-lite": "Gemini 3.1 Flash-Lite",
    "gemini-2.5-pro": "Gemini 2.5 Pro",
    "gemini-2.5-flash": "Gemini 2.5 Flash",
    "gemini-2.5-flash-lite": "Gemini 2.5 Flash-Lite",
}
SFT_TRAINING_COST_PER_MILLION: Final = {
    "gemini-3.1-flash-lite": 3.00,
    "gemini-2.5-pro": 25.00,
    "gemini-2.5-flash": 5.00,
    "gemini-2.5-flash-lite": 1.50,
}

_SCRIPT_DIR: Final = Path(__file__).resolve().parent
_DATASETS_TOML: Final = _SCRIPT_DIR / "datasets.toml"
RESULTS_DIR: Final = _SCRIPT_DIR / "results"
CONFIG_TUNE_CONFLICT_FLAGS: Final = frozenset(
    {
        "--round-id",
        "--base-model",
        "--epochs",
        "--adapter-size",
        "--lr-multiplier",
        "--location",
        "--gcp-project",
        "--gcs-bucket",
    }
)
EVALS_README_TEXT: Final = "Reserved for future config-driven eval artifacts."
CONFIG_TUNE_ARTIFACT_PATHS: Final = (
    "manifests/canonical/train.jsonl",
    "manifests/canonical/validation.jsonl",
    "manifests/canonical/eval.jsonl",
    "model_inputs/gemini/train.jsonl",
    "model_inputs/gemini/validation.jsonl",
    "preflight/report.json",
    "tuning/status.json",
    "evals/README.txt",
)


class PromptOverrideError(ValueError):
    """Clean CLI error for unreadable @file prompt overrides."""


class PreparedConfigArtifacts(TypedDict):
    run_config_path: Path
    canonical_train_path: Path
    canonical_validation_path: Path
    canonical_eval_path: Path
    gemini_train_path: Path
    gemini_validation_path: Path
    preflight_report_path: Path
    total_train_duration_seconds: float
    canonical_train_rows: int
    canonical_validation_rows: int
    canonical_eval_rows: int


def _load_registry() -> dict[str, Any]:
    with open(_DATASETS_TOML, "rb") as f:
        return tomllib.load(f)


def _load_round_config(round_id: str) -> dict[str, Any]:
    cfg_path = RESULTS_DIR / round_id / "config.json"
    if cfg_path.exists():
        return json.loads(cfg_path.read_text(encoding="utf-8"))
    return {}


def _parse_dataset_names(value: str) -> list[str]:
    """Parse comma-separated dataset names, preserving first occurrence order."""
    return list(dict.fromkeys(d.strip() for d in value.split(",") if d.strip()))


def _resolve_gcp_project(
    args: argparse.Namespace, config: dict[str, Any] | None = None
) -> str:
    """Resolve the GCP project for storage and Vertex calls.

    Precedence is CLI > saved round config > environment > Watch Duty default.
    Saved config keeps tune/eval reruns attached to the same project used by build.
    """
    config = config or {}
    return (
        str(getattr(args, "gcp_project", "") or "").strip()
        or str(config.get("gcp_project") or "").strip()
        or os.environ.get(GCP_PROJECT_ENV_VAR, "").strip()
        or DEFAULT_GCP_PROJECT
    )


def _resolve_gcs_bucket(
    args: argparse.Namespace, config: dict[str, Any] | None = None
) -> str:
    """Resolve the GCS bucket for SFT staging output."""
    config = config or {}
    return (
        str(getattr(args, "gcs_bucket", "") or "").strip()
        or str(config.get("gcs_bucket") or "").strip()
        or os.environ.get(GCS_BUCKET_ENV_VAR, "").strip()
        or DEFAULT_GCS_BUCKET
    )


def _gcs_sft_prefix(bucket: str) -> str:
    bucket = bucket.strip()
    if not bucket or bucket.startswith("gs://") or "/" in bucket:
        raise ValueError(
            "--gcs-bucket expects a bucket name, not a gs:// URI or path"
        )
    return f"gs://{bucket}/sft"


def _save_round_config(round_id: str, config: dict[str, Any]) -> None:
    cfg_path = RESULTS_DIR / round_id / "config.json"
    cfg_path.parent.mkdir(parents=True, exist_ok=True)
    cfg_path.write_text(
        json.dumps(config, indent=2, default=str), encoding="utf-8"
    )


def _utc_now() -> str:
    return datetime.now(UTC).isoformat()


def _provided_option_flags(argv: list[str]) -> set[str]:
    """Return option flags explicitly present in argv.

    Detects both ``--flag value`` and ``--flag=value`` forms. Short options are
    not used by this CLI and are intentionally ignored.
    """
    flags: set[str] = set()
    for item in argv:
        if not item.startswith("--"):
            continue
        flags.add(item.split("=", maxsplit=1)[0])
    return flags


def _reject_config_tune_conflicts(args: argparse.Namespace) -> str | None:
    flags = getattr(args, "provided_flags", set())
    conflicts = sorted(flags & CONFIG_TUNE_CONFLICT_FLAGS)
    if not conflicts:
        return None
    return (
        "tune --config reads experiment settings from TOML; do not also pass "
        + ", ".join(conflicts)
    )


def _gcs_uri_exists(storage_client: storage.Client, uri: str) -> bool:
    bucket_name, blob_path = parse_gcs_uri(uri)
    blob = storage_client.bucket(bucket_name).blob(blob_path)
    return bool(blob.exists())


def _gcs_prefix_has_any_blob(
    storage_client: storage.Client, prefix_uri: str
) -> bool:
    bucket_name, blob_prefix = parse_gcs_uri(prefix_uri)
    bucket = storage_client.bucket(bucket_name)
    for _ in bucket.list_blobs(prefix=blob_prefix, max_results=1):
        return True
    return False


def _download_gcs_uri(
    storage_client: storage.Client, uri: str, local_path: Path
) -> None:
    bucket_name, blob_path = parse_gcs_uri(uri)
    local_path.parent.mkdir(parents=True, exist_ok=True)
    download_blob_to_file(
        storage_client, bucket_name, blob_path, str(local_path)
    )


def _upload_local_file(
    storage_client: storage.Client, local_path: Path, gcs_uri: str
) -> None:
    bucket_name, blob_path = parse_gcs_uri(gcs_uri)
    upload_file_to_blob(storage_client, bucket_name, blob_path, str(local_path))


def _upload_text(
    storage_client: storage.Client,
    text: str,
    gcs_uri: str,
    *,
    content_type: str = "text/plain",
) -> None:
    bucket_name, blob_path = parse_gcs_uri(gcs_uri)
    blob = storage_client.bucket(bucket_name).blob(blob_path)
    blob.upload_from_string(text, content_type=content_type)


def _upload_json_text(
    storage_client: storage.Client, obj: dict[str, Any], gcs_uri: str
) -> None:
    _upload_text(
        storage_client,
        json.dumps(obj, indent=2, default=str),
        gcs_uri,
        content_type="application/json",
    )


def _download_json_text(
    storage_client: storage.Client, gcs_uri: str
) -> dict[str, Any]:
    bucket_name, blob_path = parse_gcs_uri(gcs_uri)
    blob = storage_client.bucket(bucket_name).blob(blob_path)
    text = blob.download_as_text()
    obj = json.loads(text)
    if not isinstance(obj, dict):
        raise TypeError(f"Expected JSON object at {gcs_uri}")
    return obj


def _write_status(
    local_run_dir: Path,
    storage_client: storage.Client,
    status_uri: str,
    status: dict[str, Any],
) -> None:
    local_path = local_run_dir / "status.json"
    _write_json_artifact(local_path, storage_client, status_uri, status)


def _write_json_artifact(
    local_path: Path,
    storage_client: storage.Client,
    gcs_uri: str,
    obj: dict[str, Any],
) -> None:
    local_path.parent.mkdir(parents=True, exist_ok=True)
    local_path.write_text(
        json.dumps(obj, indent=2, default=str), encoding="utf-8"
    )
    _upload_local_file(storage_client, local_path, gcs_uri)


def _write_text_artifact(
    local_path: Path,
    storage_client: storage.Client,
    gcs_uri: str,
    text: str,
) -> None:
    local_path.parent.mkdir(parents=True, exist_ok=True)
    local_path.write_text(text, encoding="utf-8")
    _upload_local_file(storage_client, local_path, gcs_uri)


def _local_config_path(round_id: str) -> Path:
    return RESULTS_DIR / round_id / "config.json"


def _write_and_upload_config(
    config: dict[str, Any],
    run_cfg: RunConfig,
    storage_client: storage.Client,
) -> dict[str, Any]:
    written = write_config(RESULTS_DIR, run_cfg.round_id, config)
    _upload_local_file(
        storage_client, _local_config_path(run_cfg.round_id), run_cfg.paths.config_uri
    )
    return written


def _load_canonical_rows(
    path: Path, split: str
) -> tuple[list[dict[str, Any]], list[CanonicalRow]]:
    entries = load_manifest(str(path))
    rows = rows_from_manifest(entries)
    if not rows:
        raise ValueError(f"{split} manifest has zero parsed rows: {path}")
    if len(rows) != len(entries):
        raise ValueError(
            f"{split} manifest parsed {len(rows)}/{len(entries)} rows; "
            "fix malformed rows before tuning"
        )
    return entries, rows


def _reject_split_overlap(
    left_name: str,
    left_rows: list[CanonicalRow],
    right_name: str,
    right_rows: list[CanonicalRow],
) -> None:
    left_uris = {row.audio_filepath for row in left_rows}
    right_uris = {row.audio_filepath for row in right_rows}
    overlap = sorted(left_uris & right_uris)
    if not overlap:
        return
    sample = ", ".join(overlap[:5])
    raise ValueError(
        f"{left_name} and {right_name} manifests overlap on "
        f"{len(overlap)} audio URI(s): {sample}"
    )


def _write_gemini_jsonl(
    rows: list[CanonicalRow],
    path: Path,
    *,
    system_prompt: str,
    user_prompt: str,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        for row in rows:
            ex = build_example(
                audio_uri=row.audio_filepath,
                gt_text=row.text,
                system_prompt=system_prompt,
                user_prompt=user_prompt,
            )
            if not validate_example(ex):
                raise ValueError(
                    f"invalid Gemini SFT example for {row.audio_filepath}"
                )
            fh.write(json.dumps(ex) + "\n")


def _print_tune_cost_estimate(
    *,
    n_examples: int,
    epochs: int,
    total_secs: float,
    base_model: str,
    basis: str,
) -> None:
    audio_tokens_per_sec: Final = 32
    cost_per_million_tokens = SFT_TRAINING_COST_PER_MILLION[base_model]
    model_display_name = SFT_MODEL_DISPLAY_NAMES[base_model]
    estimated_tokens = total_secs * audio_tokens_per_sec * epochs
    estimated_cost = (estimated_tokens / 1_000_000) * cost_per_million_tokens

    print("\n--- Tune Cost Estimate ---")
    print(f"  Examples:          {n_examples}")
    print(f"  Epochs:            {epochs}")
    print(
        f"  Est. audio tokens: {estimated_tokens:,.0f} "
        f"({basis} x 32 tok/s)"
    )
    print(f"  Est. cost:        ~${estimated_cost:.2f} USD")
    print(
        f"  NOTE: Using {model_display_name} SFT rate "
        f"(${cost_per_million_tokens:.2f}/M training tokens)."
    )
    print(
        "        Actual billing may differ. You accept responsibility for GCP charges.\n"
    )


def _confirm_tune_cost(confirm: bool) -> int:
    if confirm:
        return 0
    try:
        answer = input("Type 'yes' to proceed with tune: ").strip().lower()
    except EOFError:
        answer = ""
    if answer != "yes":
        logger.info("Tune aborted by operator.")
        return 130
    return 0


def _load_prompt_override(value: str, label: str) -> str:
    if not value.startswith("@"):
        return value
    path = Path(value[1:]).expanduser()
    try:
        return path.read_text(encoding="utf-8")
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
    system_prompt = PIPELINE_SYSTEM_PROMPT
    user_prompt = PIPELINE_USER_PROMPT

    if getattr(args, "system_prompt", None):
        val = args.system_prompt
        system_prompt = _load_prompt_override(val, "system")

    if getattr(args, "user_prompt", None):
        val = args.user_prompt
        user_prompt = _load_prompt_override(val, "user")

    return system_prompt, user_prompt


def _overall_keyword_accuracy(rows: list[dict[str, Any]]) -> float | None:
    occurrences = sum(int(row.get("occurrences", 0)) for row in rows)
    if occurrences == 0:
        return None
    correct = sum(int(row.get("correctly_identified", 0)) for row in rows)
    return round(100 * correct / occurrences, 2)


def _make_adapter(
    dataset_cfg: dict[str, Any],
    split: str,
    storage_client: storage.Client,
) -> DatasetAdapter:
    """Instantiate the correct adapter from a datasets.toml entry."""
    adapter_type = dataset_cfg["adapter"]
    if adapter_type == "gcs_manifest":
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
                "-- is empty. Ensure the cluster-split script has run."
            )
        return GcsManifestAdapter(
            manifest_uri=uri,
            storage_client=storage_client,
            normalize=dataset_cfg.get("normalize", False),
        )
    raise ValueError(f"Unknown adapter type: {adapter_type!r}")


def _resolve_eval_manifest_uris(
    registry: dict[str, Any], dataset_names: list[str]
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
    registry: dict[str, Any],
    split: str,
    staging_dir: Path,
    storage_client: storage.Client,
    normalizer: Callable[[str], str],
    system_prompt: str,
    user_prompt: str,
    round_id: str,
    gcs_sft_prefix: str,
) -> tuple[dict[str, str], str, float]:
    """Build per-dataset + combined Gemini SFT JSONL for one split and upload to GCS.

    Shared by the required ``train`` split and the optional ``val`` split so both go
    through identical example construction, validation, staging, and upload paths.
    Returns ``(per_dataset_uris, combined_uri, total_duration_seconds)``.
    """
    per_dataset_uris: dict[str, str] = {}
    total_duration_seconds = 0.0

    for ds_name in dataset_names:
        ds_cfg = registry["datasets"][ds_name]
        adapter = _make_adapter(
            ds_cfg, split=split, storage_client=storage_client
        )
        do_normalize = ds_cfg.get("normalize", False)

        examples: list[dict[str, Any]] = []
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
        with out_path.open("w", encoding="utf-8") as f:
            for ex in examples:
                f.write(json.dumps(ex) + "\n")
        logger.info(
            f"[{ds_name}/{split}] wrote {len(examples)} examples -> {out_path}"
        )

        gcs_uri = f"{gcs_sft_prefix}/{round_id}/{split}_{ds_name}.jsonl"
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
    combined_uri = f"{gcs_sft_prefix}/{round_id}/{split}_{combined_name}.jsonl"
    bucket_name, blob_path = parse_gcs_uri(combined_uri)
    upload_file_to_blob(
        storage_client, bucket_name, blob_path, str(combined_path)
    )
    logger.info(f"[combined/{split}] uploaded -> {combined_uri}")

    return per_dataset_uris, combined_uri, total_duration_seconds


def _build(args: argparse.Namespace) -> int:
    """Build subcommand: adapters -> Gemini SFT JSONL -> local staging -> GCS upload.

    Always builds the required ``train`` split. Also builds an optional ``val`` split
    for any dataset declaring a non-empty ``val_manifest_uri``. When set,
    ``combined_val_uri`` is recorded so ``tune`` wires a Vertex validation dataset
    (eval_total_loss). When no dataset declares one, the val split is skipped.
    """
    try:
        system_prompt, user_prompt = _load_prompts(args)
    except PromptOverrideError as e:
        logger.error(str(e))  # noqa: TRY400
        return 1

    registry = _load_registry()
    dataset_names = _parse_dataset_names(args.datasets)
    if not dataset_names:
        logger.error("No datasets specified. Pass at least one dataset name.")
        return 1
    config = _load_round_config(args.round_id)
    gcp_project = _resolve_gcp_project(args, config)
    gcs_bucket = _resolve_gcs_bucket(args, config)
    try:
        gcs_sft_prefix = _gcs_sft_prefix(gcs_bucket)
    except ValueError as e:
        logger.error(str(e))  # noqa: TRY400
        return 1

    # Validate requested datasets exist in registry
    for ds_name in dataset_names:
        if ds_name not in registry.get("datasets", {}):
            logger.error(
                f"Dataset '{ds_name}' not found in datasets.toml. "
                f"Available: {list(registry['datasets'].keys())}"
            )
            return 1

    storage_client = storage.Client(project=gcp_project)
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
                gcs_sft_prefix=gcs_sft_prefix,
            )
        )
    except ValueError as e:
        # e.g. echo's train_manifest_uri placeholder is empty until the
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
                gcs_sft_prefix=gcs_sft_prefix,
            )
        except ValueError as e:
            logger.error(f"cannot build val split: {e}")  # noqa: TRY400
            return 1

    # Write/update config.json
    config.update(
        {
            "round_id": args.round_id,
            "datasets": dataset_names,
            "gcp_project": gcp_project,
            "gcs_bucket": gcs_bucket,
            "gcs_sft_prefix": gcs_sft_prefix,
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
    if getattr(args, "config", ""):
        if message := _reject_config_tune_conflicts(args):
            logger.error(message)
            return 1
        return _tune_from_config(args)
    if not getattr(args, "round_id", ""):
        logger.error("--round-id is required unless --config is provided")
        return 1
    return _tune_legacy(args)


def _tune_from_config(args: argparse.Namespace) -> int:
    """Config-driven tune branch backed by GCS-authoritative run records."""
    try:
        run_cfg = load_run_config(args.config)
    except RunConfigError as e:
        logger.error(str(e))  # noqa: TRY400
        return 1

    if run_cfg.base_model not in SUPPORTED_SFT_BASE_MODELS:
        logger.error(
            f"Base model '{run_cfg.base_model}' is not supported for this Gemini SFT pipeline. "
            f"Use one of: {', '.join(sorted(SUPPORTED_SFT_BASE_MODELS))}."
        )
        return 1

    storage_client = storage.Client(project=run_cfg.gcp_project)
    local_run_dir = RESULTS_DIR / run_cfg.round_id

    try:
        if _gcs_uri_exists(storage_client, run_cfg.paths.config_uri):
            config = _download_json_text(
                storage_client, run_cfg.paths.config_uri
            )
            if config.get("job_name"):
                return _resume_config_tune(run_cfg, storage_client, config)
            logger.error(
                "Run prefix already exists without job_name; use a new round_id"
            )
            return 1

        if _gcs_prefix_has_any_blob(
            storage_client, run_cfg.paths.gcs_prefix + "/"
        ):
            logger.error(
                "Run prefix already exists without job_name; use a new round_id"
            )
            return 1

        return _submit_config_tune(args, run_cfg, storage_client, local_run_dir)
    except (OSError, ValueError, RuntimeError, TimeoutError) as e:
        logger.error(str(e))  # noqa: TRY400
        return 1


def _resume_config_tune(
    run_cfg: RunConfig,
    storage_client: storage.Client,
    config: dict[str, Any],
) -> int:
    local_run_dir = RESULTS_DIR / run_cfg.round_id
    local_run_dir.mkdir(parents=True, exist_ok=True)
    _local_config_path(run_cfg.round_id).write_text(
        json.dumps(config, indent=2, default=str), encoding="utf-8"
    )

    job_name = str(config["job_name"])
    logger.info(f"Re-attaching to config-driven job {job_name}")
    endpoint = poll_tuning_job(
        job_name, run_cfg.gcp_project, run_cfg.location
    )
    config.update(
        {
            "endpoint": endpoint,
            "status": "succeeded",
            "updated_at": _utc_now(),
        }
    )
    config = _write_and_upload_config(config, run_cfg, storage_client)
    root_status = {
        "round_id": run_cfg.round_id,
        "status": "succeeded",
        "job_name": job_name,
        "endpoint": endpoint,
        "updated_at": _utc_now(),
    }
    tuning_status = {
        **root_status,
        "base_model": config.get("base_model", run_cfg.base_model),
    }
    _write_status(
        local_run_dir,
        storage_client,
        run_cfg.paths.status_uri,
        root_status,
    )
    _write_json_artifact(
        local_run_dir / "tuning" / "status.json",
        storage_client,
        run_cfg.paths.tuning_status_uri,
        tuning_status,
    )
    logger.info(f"Tune complete. Endpoint: {endpoint}")
    return 0


def _submit_config_tune(
    args: argparse.Namespace,
    run_cfg: RunConfig,
    storage_client: storage.Client,
    local_run_dir: Path,
) -> int:
    local_paths = _prepare_config_run_artifacts(
        run_cfg, storage_client, local_run_dir
    )
    total_secs = float(local_paths["total_train_duration_seconds"])
    n_examples = int(local_paths["canonical_train_rows"])
    basis = f"{total_secs:,.0f}s actual total"
    _print_tune_cost_estimate(
        n_examples=n_examples,
        epochs=run_cfg.epoch_count,
        total_secs=total_secs,
        base_model=run_cfg.base_model,
        basis=basis,
    )
    if rc := _confirm_tune_cost(getattr(args, "confirm", False)):
        return rc

    report = run_preflight(
        train_jsonl_path=local_paths["gemini_train_path"],
        val_jsonl_path=local_paths["gemini_validation_path"],
        storage_client=storage_client,
        report_path=local_paths["preflight_report_path"],
        system_prompt=run_cfg.system_prompt,
        user_prompt=run_cfg.user_prompt,
    )
    _upload_local_file(
        storage_client,
        local_paths["preflight_report_path"],
        run_cfg.paths.preflight_report_uri,
    )
    if not report.passed:
        status = {
            "round_id": run_cfg.round_id,
            "status": "preflight_failed",
            "updated_at": _utc_now(),
        }
        _write_status(
            local_run_dir, storage_client, run_cfg.paths.status_uri, status
        )
        logger.error(
            f"Preflight FAILED. {len(report.failures)} issue(s) found. "
            f"Report: {run_cfg.paths.preflight_report_uri}. Fix the data and re-run."
        )
        return 1

    resolved_config = {
        **run_cfg.to_record_dict(),
        "total_train_duration_seconds": total_secs,
        "canonical_train_rows": n_examples,
        "canonical_validation_rows": local_paths[
            "canonical_validation_rows"
        ],
        "canonical_eval_rows": local_paths["canonical_eval_rows"],
        "status": "preflight_passed",
    }
    config = _write_and_upload_config(
        resolved_config, run_cfg, storage_client
    )
    _upload_prepared_config_artifacts(local_paths, run_cfg, storage_client)

    root_status = {
        "round_id": run_cfg.round_id,
        "status": "preflight_passed",
        "updated_at": _utc_now(),
    }
    _write_status(
        local_run_dir, storage_client, run_cfg.paths.status_uri, root_status
    )
    _write_json_artifact(
        local_run_dir / "tuning" / "status.json",
        storage_client,
        run_cfg.paths.tuning_status_uri,
        {
            "round_id": run_cfg.round_id,
            "status": "not_submitted",
            "updated_at": _utc_now(),
        },
    )
    _write_text_artifact(
        local_run_dir / "evals" / "README.txt",
        storage_client,
        run_cfg.paths.evals_readme_uri,
        EVALS_README_TEXT,
    )

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
            "updated_at": _utc_now(),
        }
    )
    config = _write_and_upload_config(config, run_cfg, storage_client)
    submitted_status = {
        "round_id": run_cfg.round_id,
        "status": "submitted",
        "job_name": job_name,
        "updated_at": _utc_now(),
    }
    _write_status(
        local_run_dir,
        storage_client,
        run_cfg.paths.status_uri,
        submitted_status,
    )
    _write_json_artifact(
        local_run_dir / "tuning" / "status.json",
        storage_client,
        run_cfg.paths.tuning_status_uri,
        submitted_status,
    )
    logger.info(f"Persisted job_name: {job_name}")

    endpoint = poll_tuning_job(
        job_name, run_cfg.gcp_project, run_cfg.location
    )
    config.update(
        {
            "endpoint": endpoint,
            "status": "succeeded",
            "updated_at": _utc_now(),
        }
    )
    _write_and_upload_config(config, run_cfg, storage_client)
    succeeded_status = {
        "round_id": run_cfg.round_id,
        "status": "succeeded",
        "job_name": job_name,
        "endpoint": endpoint,
        "updated_at": _utc_now(),
    }
    _write_status(
        local_run_dir,
        storage_client,
        run_cfg.paths.status_uri,
        succeeded_status,
    )
    _write_json_artifact(
        local_run_dir / "tuning" / "status.json",
        storage_client,
        run_cfg.paths.tuning_status_uri,
        succeeded_status,
    )
    logger.info(f"Tune complete. Endpoint: {endpoint}")
    return 0


def _prepare_config_run_artifacts(
    run_cfg: RunConfig,
    storage_client: storage.Client,
    local_run_dir: Path,
) -> PreparedConfigArtifacts:
    canonical_dir = local_run_dir / "manifests" / "canonical"
    model_inputs_dir = local_run_dir / "model_inputs" / "gemini"
    preflight_dir = local_run_dir / "preflight"
    for path in (canonical_dir, model_inputs_dir, preflight_dir):
        path.mkdir(parents=True, exist_ok=True)

    run_config_path = local_run_dir / "run_config.toml"
    run_config_path.write_text(run_cfg.raw_toml, encoding="utf-8")
    _upload_local_file(
        storage_client, run_config_path, run_cfg.paths.run_config_uri
    )

    canonical_train_path = canonical_dir / "train.jsonl"
    canonical_validation_path = canonical_dir / "validation.jsonl"
    canonical_eval_path = canonical_dir / "eval.jsonl"
    _download_gcs_uri(
        storage_client, run_cfg.train_manifest_uri, canonical_train_path
    )
    _download_gcs_uri(
        storage_client,
        run_cfg.validation_manifest_uri,
        canonical_validation_path,
    )
    _download_gcs_uri(
        storage_client, run_cfg.eval_manifest_uri, canonical_eval_path
    )

    _, train_rows = _load_canonical_rows(canonical_train_path, "train")
    _, validation_rows = _load_canonical_rows(
        canonical_validation_path, "validation"
    )
    _, eval_rows = _load_canonical_rows(canonical_eval_path, "eval")
    _reject_split_overlap("train", train_rows, "validation", validation_rows)
    _reject_split_overlap("train", train_rows, "eval", eval_rows)

    gemini_train_path = model_inputs_dir / "train.jsonl"
    gemini_validation_path = model_inputs_dir / "validation.jsonl"
    _write_gemini_jsonl(
        train_rows,
        gemini_train_path,
        system_prompt=run_cfg.system_prompt,
        user_prompt=run_cfg.user_prompt,
    )
    _write_gemini_jsonl(
        validation_rows,
        gemini_validation_path,
        system_prompt=run_cfg.system_prompt,
        user_prompt=run_cfg.user_prompt,
    )

    return {
        "run_config_path": run_config_path,
        "canonical_train_path": canonical_train_path,
        "canonical_validation_path": canonical_validation_path,
        "canonical_eval_path": canonical_eval_path,
        "gemini_train_path": gemini_train_path,
        "gemini_validation_path": gemini_validation_path,
        "preflight_report_path": preflight_dir / "report.json",
        "total_train_duration_seconds": sum(
            row.duration for row in train_rows
        ),
        "canonical_train_rows": len(train_rows),
        "canonical_validation_rows": len(validation_rows),
        "canonical_eval_rows": len(eval_rows),
    }


def _upload_prepared_config_artifacts(
    local_paths: PreparedConfigArtifacts,
    run_cfg: RunConfig,
    storage_client: storage.Client,
) -> None:
    uploads = [
        ("canonical_train_path", run_cfg.paths.canonical_train_uri),
        (
            "canonical_validation_path",
            run_cfg.paths.canonical_validation_uri,
        ),
        ("canonical_eval_path", run_cfg.paths.canonical_eval_uri),
        ("gemini_train_path", run_cfg.paths.gemini_train_uri),
        ("gemini_validation_path", run_cfg.paths.gemini_validation_uri),
        ("preflight_report_path", run_cfg.paths.preflight_report_uri),
    ]
    for local_key, gcs_uri in uploads:
        _upload_local_file(storage_client, local_paths[local_key], gcs_uri)


def _tune_legacy(args: argparse.Namespace) -> int:
    """Tune subcommand — submit or re-attach to a Vertex AI SFT tuning job.

    Persists job.name to config.json before entering the poll loop.
    On re-run, re-attaches to an in-flight job by name (no re-submit, no re-pay).
    Unsupported base models are rejected before any GCP call.
    --confirm gate: displays estimated token count + cost estimate before submitting.
    """
    # Reject unsupported base models before any GCP call. The supported list is
    # intentionally narrow and mirrors Google's supervised-tuning model list.
    if args.base_model not in SUPPORTED_SFT_BASE_MODELS:
        logger.error(
            f"Base model '{args.base_model}' is not supported for this Gemini SFT pipeline. "
            f"Use one of: {', '.join(sorted(SUPPORTED_SFT_BASE_MODELS))}."
        )
        return 1

    config = _load_round_config(args.round_id)
    location = getattr(args, "location", "us-central1")
    gcp_project = _resolve_gcp_project(args, config)
    gcs_bucket = _resolve_gcs_bucket(args, config)
    try:
        gcs_sft_prefix = _gcs_sft_prefix(gcs_bucket)
    except ValueError as e:
        logger.error(str(e))  # noqa: TRY400
        return 1
    config["gcp_project"] = gcp_project
    config["gcs_bucket"] = gcs_bucket
    config["gcs_sft_prefix"] = gcs_sft_prefix

    # Resume: re-attach to an in-flight job if job_name is already recorded.
    if job_name := config.get("job_name"):
        state, endpoint = get_tuning_job_status(
            job_name, gcp_project, location
        )
        if state in {"JOB_STATE_SUCCEEDED", "SUCCEEDED"}:
            endpoint = config.get("endpoint") or endpoint
            if not endpoint:
                # Crash recovery: the job succeeded but the endpoint was never
                # persisted (the process died between submit and the endpoint write).
                # Re-read it from the job resource so eval does not silently
                # degrade to base-only.
                logger.warning(
                    f"Job {job_name} is SUCCEEDED but exposes no endpoint; "
                    "eval will run base-only."
                )
            elif not config.get("endpoint"):
                config["endpoint"] = endpoint
                write_config(RESULTS_DIR, args.round_id, config)
                logger.info(
                    f"Recovered endpoint from succeeded job {job_name}: {endpoint}"
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
            endpoint = poll_tuning_job(job_name, gcp_project, location)
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

    storage_client = storage.Client(project=gcp_project)

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
        with train_local.open(encoding="utf-8") as fh:
            n_examples = sum(1 for line in fh if line.strip())

    # Cost estimate and --confirm gate.
    AUDIO_TOKENS_PER_SEC: Final = (
        32  # VERIFIED: inference rate; ASSUMED same for SFT
    )
    cost_per_million_tokens = SFT_TRAINING_COST_PER_MILLION[args.base_model]
    model_display_name = SFT_MODEL_DISPLAY_NAMES[args.base_model]

    epochs = args.epochs
    total_secs = config.get("total_train_duration_seconds")
    if total_secs:
        total_secs = float(total_secs)
        basis = f"{total_secs:,.0f}s actual total"
    else:
        # No recorded durations (older build) -- worst-case fallback so the estimate
        # does not under-state cost (Echo segments run ~3-30s; avg ~15s).
        avg_secs = FALLBACK_SEGMENT_DURATION_SECONDS
        total_secs = n_examples * avg_secs
        basis = f"{n_examples} x {avg_secs:.1f}s avg (estimated)"
    estimated_tokens = total_secs * AUDIO_TOKENS_PER_SEC * epochs
    estimated_cost = (estimated_tokens / 1_000_000) * cost_per_million_tokens

    print("\n--- Tune Cost Estimate ---")
    print(f"  Examples:          {n_examples}")
    print(f"  Epochs:            {epochs}")
    print(f"  Est. audio tokens: {estimated_tokens:,.0f} ({basis} x 32 tok/s)")
    print(f"  Est. cost:        ~${estimated_cost:.2f} USD")
    print(
        f"  NOTE: Using {model_display_name} SFT rate "
        f"(${cost_per_million_tokens:.2f}/M training tokens)."
    )
    print(
        "        Actual billing may differ. You accept responsibility for GCP charges.\n"
    )

    if not args.confirm:
        try:
            answer = input("Type 'yes' to proceed with tune: ").strip().lower()
        except EOFError:
            answer = ""
        if answer != "yes":
            logger.info("Tune aborted by operator.")
            return 130

    # Submit and persist job.name before polling so the job can be resumed.
    display_name = f"wd-radio-sft-{args.round_id}"
    job_name = submit_tuning_job(
        train_uri=train_uri,
        display_name=display_name,
        project=gcp_project,
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
    config["gcp_project"] = gcp_project
    config["gcs_bucket"] = gcs_bucket
    config["gcs_sft_prefix"] = gcs_sft_prefix
    config["epochs"] = epochs
    write_config(RESULTS_DIR, args.round_id, config)
    logger.info(f"Persisted job_name: {job_name}")

    # Poll to completion
    endpoint = poll_tuning_job(job_name, gcp_project, location)
    config["endpoint"] = endpoint
    write_config(RESULTS_DIR, args.round_id, config)
    logger.info(f"Tune complete. Endpoint: {endpoint}")
    return 0


def _eval(args: argparse.Namespace) -> int:
    """Eval subcommand — batch-infer and score the model on the held-out manifest.

    Computes the full scoring panel: WER, CER, ins/del/sub, empty/hallucination
    rate, duration buckets, keyword accuracy, and bootstrap paired significance.
    Degrades gracefully to base-only metrics when no tuned model endpoint is available.
    """
    config = _load_round_config(args.round_id)
    if not config:
        logger.error(
            f"No config.json found for round {args.round_id}. Run `build` first."
        )
        return 1

    gcp_project = _resolve_gcp_project(args, config)
    gcs_bucket = _resolve_gcs_bucket(args, config)
    try:
        gcs_sft_prefix = _gcs_sft_prefix(gcs_bucket)
    except ValueError as e:
        logger.error(str(e))  # noqa: TRY400
        return 1

    storage_client = storage.Client(project=gcp_project)
    location = getattr(args, "location", "us-central1")
    system_prompt = config.get("system_prompt", "")
    user_prompt = config.get("user_prompt", "")

    base_only = getattr(args, "base_only", False)
    base_model = config.get("base_model", DEFAULT_BASE_MODEL)
    tuned_endpoint = config.get("endpoint")

    if not base_only and not tuned_endpoint:
        logger.warning(
            "No tuned endpoint in config.json — running base-only eval."
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

    def _build_batch_jsonl(label: str, tmp: str) -> tuple[str, str]:
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
        batch_input_path.write_text("\n".join(lines) + "\n", encoding="utf-8")

        batch_input_gcs = (
            f"{gcs_sft_prefix}/{args.round_id}/eval_batch_{label}_input.jsonl"
        )
        batch_output_gcs = (
            f"{gcs_sft_prefix}/{args.round_id}/eval_batch_{label}_output/"
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
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                logger.warning("Skipping malformed batch output JSONL row")
                continue
            if not isinstance(obj, dict):
                continue
            if obj.get("status") or not isinstance(
                obj.get("request"), dict
            ):  # error / non-prediction
                continue
            contents = obj["request"].get("contents", [])
            if (
                not isinstance(contents, list)
                or not contents
                or not isinstance(contents[0], dict)
            ):
                continue
            parts = contents[0].get("parts", [])
            if not isinstance(parts, list):
                continue
            # Vertex echoes the request back in camelCase even though we send
            # snake_case, so accept BOTH fileData/fileUri and file_data/file_uri.
            uri = None
            for p in parts:
                if not isinstance(p, dict):
                    continue
                fd = p.get("file_data") or p.get("fileData")
                if isinstance(fd, dict):
                    candidate_uri = fd.get("file_uri") or fd.get("fileUri")
                    uri = (
                        candidate_uri
                        if isinstance(candidate_uri, str)
                        else None
                    )
                    if uri:
                        break
            response = obj.get("response")
            cands = (
                response.get("candidates", [])
                if isinstance(response, dict)
                else []
            )
            pred = ""
            if cands and isinstance(cands, list) and isinstance(cands[0], dict):
                content = cands[0].get("content", {})
                text_parts = (
                    content.get("parts", [])
                    if isinstance(content, dict)
                    else []
                )
                if isinstance(text_parts, list):
                    for tp in text_parts:
                        if isinstance(tp, dict) and isinstance(
                            tp.get("text"), str
                        ):
                            pred = tp["text"]
                            break
            if uri:
                result[uri] = pred
        return result

    def _batch_infer(model_id: str, label: str) -> dict[str, str] | None:
        """Build batch JSONL, submit, download, parse -> {audio_uri: pred_text}."""
        with tempfile.TemporaryDirectory() as tmp:
            batch_input_gcs, batch_output_gcs = _build_batch_jsonl(label, tmp)

            try:
                output_loc = submit_batch_inference(
                    input_uri=batch_input_gcs,
                    output_uri=batch_output_gcs,
                    model=model_id,
                    project=gcp_project,
                    location=location,
                )
            except (RuntimeError, TimeoutError) as e:
                logger.error(f"[{label}] Batch inference failed: {e}")  # noqa: TRY400
                return None

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
                preds.update(
                    _parse_batch_output(local_path.read_text(encoding="utf-8"))
                )
            expected_count = len({row.audio_filepath for row in eval_rows})
            missing = max(0, expected_count - len(preds))
            if missing > 0:
                logger.warning(
                    f"[{label}] {missing}/{expected_count} unique segments returned no "
                    "prediction (Vertex batch errors or skips) -- they score as full "
                    "deletions; check for a batch/API failure, not just model quality."
                )
            return preds

    normalizer = build_normalizer()

    # Run base model batch inference
    logger.info("Running base model batch inference...")
    base_preds = _batch_infer(base_model, "base")
    if base_preds is None:
        return 1

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

    # Duration bucket WER.
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
        if tuned_preds is None:
            return 1
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

        # Bootstrap significance: bootstrap_paired takes (refs, hyps_a, hyps_b).
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

    # Write per-run records.
    write_wer_summary(RESULTS_DIR, args.round_id, metrics)
    config.update(
        {
            "base_model": base_model,
            "gcp_project": gcp_project,
            "gcs_bucket": gcs_bucket,
            "gcs_sft_prefix": gcs_sft_prefix,
            "base_wer": metrics.get("base_wer"),
            "tuned_wer": metrics.get("tuned_wer"),
        }
    )
    config = write_config(RESULTS_DIR, args.round_id, config)
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
    _add_gcp_args(p)
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


def _add_gcp_args(p: argparse.ArgumentParser) -> None:
    p.add_argument(
        "--gcp-project",
        default="",
        help=(
            f"GCP project for Storage and Vertex calls "
            f"(default: ${GCP_PROJECT_ENV_VAR} or {DEFAULT_GCP_PROJECT})"
        ),
    )
    p.add_argument(
        "--gcs-bucket",
        default="",
        help=(
            f"GCS bucket for SFT staging output "
            f"(default: ${GCS_BUCKET_ENV_VAR} or {DEFAULT_GCS_BUCKET})"
        ),
    )


def _add_tune_args(p: argparse.ArgumentParser) -> None:
    _add_gcp_args(p)
    p.add_argument(
        "--config",
        default="",
        help="External TOML config for one config-driven tune run",
    )
    p.add_argument(
        "--round-id",
        default="",
        help="Round identifier (must match build output)",
    )
    p.add_argument(
        "--base-model",
        default=DEFAULT_BASE_MODEL,
        choices=sorted(SUPPORTED_SFT_BASE_MODELS),
        help="Base model name for Gemini supervised tuning",
    )
    p.add_argument(
        "--epochs", type=int, default=1, help="Training epochs (default: 1)"
    )
    p.add_argument(
        "--adapter-size",
        default="EIGHT",
        choices=["ONE", "TWO", "FOUR", "EIGHT", "SIXTEEN"],
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
    _add_gcp_args(p)
    p.add_argument("--round-id", required=True, help="Round identifier")
    p.add_argument(
        "--base-only",
        action="store_true",
        help="Eval base model only (no tuned model)",
    )
    p.add_argument("--location", default="us-central1", help="Vertex AI region")


def _add_all_args(p: argparse.ArgumentParser) -> None:
    _add_build_args(p)
    p.add_argument(
        "--config",
        default="",
        help="Unsupported for all; use tune --config <run.toml>",
    )
    # Tune-specific args (--round-id is already added by _add_build_args above)
    p.add_argument(
        "--base-model",
        default=DEFAULT_BASE_MODEL,
        choices=sorted(SUPPORTED_SFT_BASE_MODELS),
        help="Base model name for Gemini supervised tuning",
    )
    p.add_argument(
        "--epochs", type=int, default=1, help="Training epochs (default: 1)"
    )
    p.add_argument(
        "--adapter-size",
        default="EIGHT",
        choices=["ONE", "TWO", "FOUR", "EIGHT", "SIXTEEN"],
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
    raw_args = sys.argv[1:]
    if raw_args and raw_args[0] == "all" and any(
        item == "--config" or item.startswith("--config=")
        for item in raw_args[1:]
    ):
        logger.error(
            "all --config is not supported in this milestone; use tune --config <run.toml>"
        )
        return 1

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
    args.provided_flags = _provided_option_flags(sys.argv[2:])
    dispatch = {"build": _build, "tune": _tune, "eval": _eval, "all": _all}
    return dispatch[args.cmd](args)


if __name__ == "__main__":
    sys.exit(main())
