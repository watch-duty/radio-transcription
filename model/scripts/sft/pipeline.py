"""Watch Duty radio transcription SFT pipeline CLI.

Commands:
  build  -- Turn registered datasets into per-dataset and combined Vertex SFT JSONL.
  tune   -- Submit a Vertex AI SFT tuning job (--confirm gated; PR3 implements fully).
  eval   -- Batch-infer and score a model on the held-out manifest (PR3 implements fully).
  all    -- build -> tune -> eval in one invocation (PR3 implements fully).

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
from pathlib import Path
from typing import Final

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

GCP_PROJECT: Final = "automatic-hawk-481415-m9"
GCS_BUCKET: Final = "wd-transcription-data"
GCS_SFT_PREFIX: Final = f"gs://{GCS_BUCKET}/sft"

_SCRIPT_DIR: Final = Path(__file__).resolve().parent
_DATASETS_TOML: Final = _SCRIPT_DIR / "datasets.toml"
RESULTS_DIR: Final = _SCRIPT_DIR / "results"


def _load_registry() -> dict:
    with open(_DATASETS_TOML, "rb") as f:
        return tomllib.load(f)


def _load_round_config(round_id: str) -> dict:
    cfg_path = RESULTS_DIR / round_id / "config.json"
    if cfg_path.exists():
        return json.loads(cfg_path.read_text())
    return {}


def _save_round_config(round_id: str, config: dict) -> None:
    cfg_path = RESULTS_DIR / round_id / "config.json"
    cfg_path.parent.mkdir(parents=True, exist_ok=True)
    cfg_path.write_text(json.dumps(config, indent=2, default=str))


def _load_prompts(args: argparse.Namespace) -> tuple[str, str]:
    """Return (system_prompt, user_prompt) -- from args or pipeline defaults."""
    from prompts import PIPELINE_SYSTEM_PROMPT, PIPELINE_USER_PROMPT

    system_prompt = PIPELINE_SYSTEM_PROMPT
    user_prompt = PIPELINE_USER_PROMPT

    if getattr(args, "system_prompt", None):
        val = args.system_prompt
        if val.startswith("@"):
            system_prompt = Path(val[1:]).read_text()
        else:
            system_prompt = val

    if getattr(args, "user_prompt", None):
        val = args.user_prompt
        if val.startswith("@"):
            user_prompt = Path(val[1:]).read_text()
        else:
            user_prompt = val

    return system_prompt, user_prompt


def _make_adapter(
    dataset_cfg: dict, split: str, storage_client: object
) -> object:
    """Instantiate the correct adapter from a datasets.toml entry."""
    adapter_type = dataset_cfg["adapter"]
    if adapter_type == "gcs_manifest":
        from adapters.gcs_manifest import GcsManifestAdapter

        uri_key = (
            "train_manifest_uri" if split == "train" else "eval_manifest_uri"
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


def _build(args: argparse.Namespace) -> int:
    """Build subcommand: adapters -> SFT JSONL -> local staging -> GCS upload."""
    from common.gcs_utils import parse_gcs_uri, upload_file_to_blob
    from common.scoring import build_normalizer
    from common.sft import build_example, validate_example
    from google.cloud import storage

    system_prompt, user_prompt = _load_prompts(args)
    registry = _load_registry()
    dataset_names = [d.strip() for d in args.datasets.split(",")]

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
    per_dataset_uris: dict[str, str] = {}
    total_duration_seconds = 0.0

    for ds_name in dataset_names:
        ds_cfg = registry["datasets"][ds_name]
        try:
            adapter = _make_adapter(
                ds_cfg, split="train", storage_client=storage_client
            )
        except ValueError as e:
            # e.g. echo's train_manifest_uri placeholder is empty until the
            # Phase-4 cluster-split runs -- fail cleanly, not with a traceback.
            # logger.error (not .exception): a clean one-line reason, no traceback.
            logger.error(f"[{ds_name}] cannot build: {e}")  # noqa: TRY400
            return 1
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
                    f"[{ds_name}] skipping invalid example: {row.audio_filepath}"
                )
                continue
            examples.append(ex)
            total_duration_seconds += row.duration

        out_path = staging_dir / f"train_{ds_name}.jsonl"
        with open(out_path, "w") as f:
            for ex in examples:
                f.write(json.dumps(ex) + "\n")
        logger.info(f"[{ds_name}] wrote {len(examples)} examples -> {out_path}")

        # Upload to GCS — parse URI into bucket + blob_path for upload_file_to_blob
        gcs_uri = f"{GCS_SFT_PREFIX}/{args.round_id}/train_{ds_name}.jsonl"
        bucket_name, blob_path = parse_gcs_uri(gcs_uri)
        upload_file_to_blob(
            storage_client, bucket_name, blob_path, str(out_path)
        )
        per_dataset_uris[ds_name] = gcs_uri
        logger.info(f"[{ds_name}] uploaded -> {gcs_uri}")

    # Combined JSONL for the exact --datasets set
    combined_name = "_".join(dataset_names)
    combined_path = staging_dir / f"train_{combined_name}.jsonl"
    with open(combined_path, "wb") as f:
        for ds_name in dataset_names:
            ds_path = staging_dir / f"train_{ds_name}.jsonl"
            if ds_path.exists():
                with ds_path.open("rb") as infile:
                    shutil.copyfileobj(infile, f)
    combined_gcs_uri = (
        f"{GCS_SFT_PREFIX}/{args.round_id}/train_{combined_name}.jsonl"
    )
    bucket_name, blob_path = parse_gcs_uri(combined_gcs_uri)
    upload_file_to_blob(
        storage_client, bucket_name, blob_path, str(combined_path)
    )
    logger.info(f"[combined] uploaded -> {combined_gcs_uri}")

    # Write/update config.json
    config = _load_round_config(args.round_id)
    config.update(
        {
            "round_id": args.round_id,
            "datasets": dataset_names,
            "system_prompt": system_prompt,
            "user_prompt": user_prompt,
            "train_uris": per_dataset_uris,
            "combined_train_uri": combined_gcs_uri,
            "total_train_duration_seconds": total_duration_seconds,
        }
    )
    _save_round_config(args.round_id, config)
    logger.info(
        f"Build complete. Config: {RESULTS_DIR / args.round_id / 'config.json'}"
    )
    return 0


def _tune(args: argparse.Namespace) -> int:
    """Tune subcommand stub -- fully implemented in PR3 (plan 03-03)."""
    logger.error("tune is not yet implemented. Run after PR3 merges.")
    return 1


def _eval(args: argparse.Namespace) -> int:
    """Eval subcommand stub -- fully implemented in PR3 (plan 03-03)."""
    logger.error("eval is not yet implemented. Run after PR3 merges.")
    return 1


def _all(args: argparse.Namespace) -> int:
    """All subcommand stub -- fully implemented in PR3 (plan 03-03)."""
    rc = _build(args)
    if rc != 0:
        return rc
    logger.error("tune and eval are not yet implemented. Run after PR3 merges.")
    return 1


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
        description="Watch Duty radio transcription SFT pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    sub = ap.add_subparsers(
        dest="cmd", required=True, metavar="{build,tune,eval,all}"
    )
    _add_build_args(
        sub.add_parser("build", help="Build SFT JSONL from registered datasets")
    )
    _add_tune_args(
        sub.add_parser(
            "tune", help="Submit Vertex AI SFT tuning job (--confirm required)"
        )
    )
    _add_eval_args(
        sub.add_parser(
            "eval", help="Batch-infer and score model on held-out manifest"
        )
    )
    _add_all_args(
        sub.add_parser("all", help="build -> tune -> eval in one invocation")
    )
    args = ap.parse_args()
    dispatch = {"build": _build, "tune": _tune, "eval": _eval, "all": _all}
    return dispatch[args.cmd](args)


if __name__ == "__main__":
    sys.exit(main())
