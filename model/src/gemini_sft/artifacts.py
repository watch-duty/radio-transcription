"""Local and GCS artifact helpers for Gemini SFT runs."""

from __future__ import annotations

import dataclasses
import datetime
import json
import pathlib
import typing

from common import gcs_utils, manifest
from common.gemini import context

from gemini_sft import records

if typing.TYPE_CHECKING:
    from google.cloud import storage

    from gemini_sft import config

DEFAULT_RESULTS_DIR = pathlib.Path("results")
EVALS_README_TEXT = "Reserved for Gemini SFT eval artifacts."


@dataclasses.dataclass(frozen=True)
class PreparedRunArtifacts:
    """Local paths and counts produced by preparing a config-driven run.

    Attributes:
        run_config_path: Local copy of the operator TOML.
        canonical_train_path: Local canonical training manifest.
        canonical_validation_path: Local canonical validation manifest.
        canonical_eval_path: Local canonical evaluation manifest.
        gemini_train_path: Local Gemini training JSONL.
        gemini_validation_path: Local Gemini validation JSONL.
        preflight_report_path: Local preparation preflight report.
        total_train_duration_seconds: Total duration of canonical training
            audio.
        canonical_train_rows: Number of validated canonical training rows.
        canonical_validation_rows: Number of validated canonical validation
            rows.
        canonical_eval_rows: Number of validated canonical evaluation rows.
    """

    run_config_path: pathlib.Path
    canonical_train_path: pathlib.Path
    canonical_validation_path: pathlib.Path
    canonical_eval_path: pathlib.Path
    gemini_train_path: pathlib.Path
    gemini_validation_path: pathlib.Path
    preflight_report_path: pathlib.Path
    total_train_duration_seconds: float
    canonical_train_rows: int
    canonical_validation_rows: int
    canonical_eval_rows: int


@dataclasses.dataclass(frozen=True)
class EvalRowsWithHistory:
    """Canonical eval rows plus aligned prior-context histories.

    Attributes:
        source_rows: Validated raw eval rows preserved for normalized output.
        eval_rows: Typed canonical rows aligned with ``source_rows``.
        histories: Prior same-source transcript turns aligned with each eval
            row.
    """

    source_rows: list[dict[str, typing.Any]]
    eval_rows: list[manifest.CanonicalRow]
    histories: list[list[context.ContextTurn]]


def utc_now() -> str:
    """Return an ISO UTC timestamp."""
    return datetime.datetime.now(datetime.UTC).isoformat()


def local_run_dir(results_dir: pathlib.Path, round_id: str) -> pathlib.Path:
    """Return the local mirror directory for a run."""
    return results_dir / round_id


def local_config_path(
    results_dir: pathlib.Path,
    round_id: str,
) -> pathlib.Path:
    """Return the local mirror config path for a run."""
    return local_run_dir(results_dir, round_id) / "config.json"


def write_json_artifact(
    local_path: pathlib.Path,
    storage_client: storage.Client,
    gcs_uri: str,
    obj: dict[str, typing.Any],
) -> None:
    """Write JSON locally and upload it to GCS."""
    local_path.parent.mkdir(parents=True, exist_ok=True)
    local_path.write_text(
        json.dumps(obj, indent=2, default=str), encoding="utf-8"
    )
    gcs_utils.upload_local_file(storage_client, local_path, gcs_uri)


def write_text_artifact(
    local_path: pathlib.Path,
    storage_client: storage.Client,
    gcs_uri: str,
    text: str,
) -> None:
    """Write text locally and upload it to GCS."""
    local_path.parent.mkdir(parents=True, exist_ok=True)
    local_path.write_text(text, encoding="utf-8")
    gcs_utils.upload_local_file(storage_client, local_path, gcs_uri)


def write_status(
    run_dir: pathlib.Path,
    storage_client: storage.Client,
    status_uri: str,
    status: dict[str, typing.Any],
) -> None:
    """Write the run root status artifact locally and to GCS."""
    write_json_artifact(
        run_dir / "status.json", storage_client, status_uri, status
    )


def write_and_upload_config(
    *,
    results_dir: pathlib.Path,
    run_cfg: config.RunConfig,
    storage_client: storage.Client,
    config: dict[str, typing.Any],
) -> dict[str, typing.Any]:
    """Write config.json with metadata and upload it to the run prefix."""
    written = records.write_config(results_dir, run_cfg.round_id, config)
    gcs_utils.upload_local_file(
        storage_client,
        local_config_path(results_dir, run_cfg.round_id),
        run_cfg.paths.config_uri,
    )
    return written


def load_canonical_rows(
    path: pathlib.Path, split: str
) -> tuple[list[dict[str, typing.Any]], list[manifest.CanonicalRow]]:
    """Load a canonical manifest and return raw entries plus parsed rows."""
    entries = manifest.load_manifest_strict(str(path))
    return canonical_rows_from_entries(entries, split=split, source=str(path))


def canonical_rows_from_entries(
    entries: list[dict[str, typing.Any]],
    *,
    split: str,
    source: str,
) -> tuple[list[dict[str, typing.Any]], list[manifest.CanonicalRow]]:
    """Validate and convert canonical manifest entries for packaged flows."""
    if not entries:
        msg = f"{split} manifest has zero parsed rows: {source}"
        raise ValueError(msg)
    return manifest.strict_canonical_rows_from_manifest(
        entries,
        expected_split=split,
        source=source,
    )


def eval_rows_with_histories_from_entries(
    entries: list[dict[str, typing.Any]],
    *,
    source: str,
    prior_context_count: int,
    limit: int | None = None,
) -> EvalRowsWithHistory:
    """Return eval source rows, canonical rows, and aligned histories.

    Args:
        entries: Raw canonical eval manifest dictionaries.
        source: Human-readable manifest source used in validation errors.
        prior_context_count: Maximum prior same-source turns per eval row.
        limit: Optional maximum number of aligned eval rows to return.

    Returns:
        Validated source rows, canonical rows, and prior-context histories in
        matching order.

    Raises:
        ValueError: If the eval manifest is empty or invalid, or if
            ``prior_context_count`` is negative.
    """
    source_rows, eval_rows = canonical_rows_from_entries(
        entries,
        split="eval",
        source=source,
    )
    histories = context.build_context_histories(
        source_rows,
        max_turns=prior_context_count,
    )
    if limit is not None:
        source_rows = source_rows[:limit]
        eval_rows = eval_rows[:limit]
        histories = histories[:limit]
    return EvalRowsWithHistory(
        source_rows=source_rows,
        eval_rows=eval_rows,
        histories=histories,
    )


def reject_split_overlap(
    left_name: str,
    left_rows: list[manifest.CanonicalRow],
    right_name: str,
    right_rows: list[manifest.CanonicalRow],
) -> None:
    """Reject audio URI or logical identity overlap between two splits."""
    left_uris = {row.audio_filepath for row in left_rows}
    right_uris = {row.audio_filepath for row in right_rows}
    uri_overlap = sorted(left_uris & right_uris)
    left_identities = {
        manifest.canonical_row_identity(row) for row in left_rows
    }
    right_identities = {
        manifest.canonical_row_identity(row) for row in right_rows
    }
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
