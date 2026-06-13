"""Local and GCS artifact helpers for Gemini SFT runs."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

from common.gcs_utils import upload_local_file
from common.manifest import (
    CanonicalRow,
    canonical_row_identity,
    load_manifest,
    require_canonical_manifest,
    rows_from_manifest,
)

from gemini_sft.records import write_config

if TYPE_CHECKING:
    from google.cloud import storage

    from gemini_sft.config import RunConfig

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
    return canonical_rows_from_entries(entries, split=split, source=str(path))


def canonical_rows_from_entries(
    entries: list[dict[str, Any]],
    *,
    split: str,
    source: str,
) -> tuple[list[dict[str, Any]], list[CanonicalRow]]:
    """Validate and convert canonical manifest entries for packaged flows."""
    if not entries:
        msg = f"{split} manifest has zero parsed rows: {source}"
        raise ValueError(msg)
    require_canonical_manifest(entries, expected_split=split)
    rows = rows_from_manifest(entries)
    return entries, rows


def reject_split_overlap(
    left_name: str,
    left_rows: list[CanonicalRow],
    right_name: str,
    right_rows: list[CanonicalRow],
) -> None:
    """Reject audio URI or logical identity overlap between two splits."""
    left_uris = {row.audio_filepath for row in left_rows}
    right_uris = {row.audio_filepath for row in right_rows}
    uri_overlap = sorted(left_uris & right_uris)
    left_identities = {canonical_row_identity(row) for row in left_rows}
    right_identities = {canonical_row_identity(row) for row in right_rows}
    identity_overlap = sorted(left_identities & right_identities)
    if not uri_overlap and not identity_overlap:
        return
    parts = [f"{left_name} and {right_name} manifests overlap"]
    if uri_overlap:
        uri_sample = ", ".join(uri_overlap[:5])
        parts.append(f"{len(uri_overlap)} audio URI(s): {uri_sample}")
    if identity_overlap:
        identity_sample = ", ".join(
            _format_identity(identity) for identity in identity_overlap[:5]
        )
        parts.append(
            f"{len(identity_overlap)} identity value(s): {identity_sample}"
        )
    msg = "; ".join(parts)
    raise ValueError(msg)


def _format_identity(identity: tuple[str, str]) -> str:
    return f"{identity[0]}/{identity[1]}"
