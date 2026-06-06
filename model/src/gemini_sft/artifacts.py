"""Local and GCS artifact helpers for Gemini SFT runs."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from common.gcs_utils import (
    download_blob_to_file,
    parse_gcs_uri,
    upload_file_to_blob,
)
from common.manifest import CanonicalRow, load_manifest, rows_from_manifest
from google.cloud import storage

from gemini_sft.config import RunConfig
from gemini_sft.records import write_config

DEFAULT_RESULTS_DIR = Path("results")
EVALS_README_TEXT = "Reserved for Gemini SFT eval artifacts."


@dataclass(frozen=True)
class PreparedRunArtifacts:
    """Local paths and counts produced by preparing a config-driven run."""

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


def utc_now() -> str:
    """Return an ISO UTC timestamp."""
    return datetime.now(UTC).isoformat()


def local_run_dir(results_dir: Path, round_id: str) -> Path:
    """Return the local mirror directory for a run."""
    return results_dir / round_id


def local_config_path(results_dir: Path, round_id: str) -> Path:
    """Return the local mirror config path for a run."""
    return local_run_dir(results_dir, round_id) / "config.json"


def gcs_uri_exists(storage_client: storage.Client, uri: str) -> bool:
    """Return whether a GCS object exists."""
    bucket_name, blob_path = parse_gcs_uri(uri)
    return bool(storage_client.bucket(bucket_name).blob(blob_path).exists())


def gcs_prefix_has_any_blob(
    storage_client: storage.Client, prefix_uri: str
) -> bool:
    """Return whether any object exists under a GCS prefix."""
    bucket_name, blob_prefix = parse_gcs_uri(prefix_uri)
    for _ in storage_client.bucket(bucket_name).list_blobs(
        prefix=blob_prefix, max_results=1
    ):
        return True
    return False


def download_gcs_uri(
    storage_client: storage.Client, uri: str, local_path: Path
) -> None:
    """Download a GCS object to a local path."""
    bucket_name, blob_path = parse_gcs_uri(uri)
    local_path.parent.mkdir(parents=True, exist_ok=True)
    download_blob_to_file(
        storage_client, bucket_name, blob_path, str(local_path)
    )


def upload_local_file(
    storage_client: storage.Client, local_path: Path, gcs_uri: str
) -> None:
    """Upload a local file to a GCS object."""
    bucket_name, blob_path = parse_gcs_uri(gcs_uri)
    upload_file_to_blob(storage_client, bucket_name, blob_path, str(local_path))


def upload_text(
    storage_client: storage.Client,
    text: str,
    gcs_uri: str,
    *,
    content_type: str = "text/plain",
) -> None:
    """Upload text directly to GCS."""
    bucket_name, blob_path = parse_gcs_uri(gcs_uri)
    storage_client.bucket(bucket_name).blob(blob_path).upload_from_string(
        text, content_type=content_type
    )


def download_json_text(
    storage_client: storage.Client, gcs_uri: str
) -> dict[str, Any]:
    """Download a JSON object from GCS."""
    bucket_name, blob_path = parse_gcs_uri(gcs_uri)
    obj = json.loads(
        storage_client.bucket(bucket_name).blob(blob_path).download_as_text()
    )
    if not isinstance(obj, dict):
        raise TypeError(f"Expected JSON object at {gcs_uri}")
    return obj


def write_json_artifact(
    local_path: Path,
    storage_client: storage.Client,
    gcs_uri: str,
    obj: dict[str, Any],
) -> None:
    """Write JSON locally and upload it to GCS."""
    local_path.parent.mkdir(parents=True, exist_ok=True)
    local_path.write_text(
        json.dumps(obj, indent=2, default=str), encoding="utf-8"
    )
    upload_local_file(storage_client, local_path, gcs_uri)


def write_text_artifact(
    local_path: Path,
    storage_client: storage.Client,
    gcs_uri: str,
    text: str,
) -> None:
    """Write text locally and upload it to GCS."""
    local_path.parent.mkdir(parents=True, exist_ok=True)
    local_path.write_text(text, encoding="utf-8")
    upload_local_file(storage_client, local_path, gcs_uri)


def write_status(
    run_dir: Path,
    storage_client: storage.Client,
    status_uri: str,
    status: dict[str, Any],
) -> None:
    """Write the run root status artifact locally and to GCS."""
    write_json_artifact(
        run_dir / "status.json", storage_client, status_uri, status
    )


def write_and_upload_config(
    *,
    results_dir: Path,
    run_cfg: RunConfig,
    storage_client: storage.Client,
    config: dict[str, Any],
) -> dict[str, Any]:
    """Write config.json with metadata and upload it to the run prefix."""
    written = write_config(results_dir, run_cfg.round_id, config)
    upload_local_file(
        storage_client,
        local_config_path(results_dir, run_cfg.round_id),
        run_cfg.paths.config_uri,
    )
    return written


def load_canonical_rows(
    path: Path, split: str
) -> tuple[list[dict[str, Any]], list[CanonicalRow]]:
    """Load a canonical manifest and return raw entries plus parsed rows."""
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


def reject_split_overlap(
    left_name: str,
    left_rows: list[CanonicalRow],
    right_name: str,
    right_rows: list[CanonicalRow],
) -> None:
    """Reject audio URI overlap between two splits."""
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
