"""External TOML config parsing for config-driven Gemini SFT tune runs."""

from __future__ import annotations

import re
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Final

from common.gemini.prompts import (
    GEMINI_TRANSCRIBE_SYSTEM_PROMPT,
    GEMINI_TRANSCRIBE_USER_PROMPT,
)
from common.inference_manifest import validate_inference_dataset_slug

ADAPTER_SIZES: Final = frozenset({"ONE", "TWO", "FOUR", "EIGHT", "SIXTEEN"})
ROUND_ID_PATTERN: Final = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")


class RunConfigError(ValueError):
    """Raised when an external SFT run config is invalid."""


@dataclass(frozen=True)
class RunPaths:
    """Resolved local/GCS artifact contract for one SFT round.

    These paths are part of the resume/eval contract: later stages discover
    their inputs by round_id under ``sft/runs/<round_id>`` rather than by
    re-reading the operator's local TOML file.
    """

    gcs_prefix: str
    run_config_uri: str
    config_uri: str
    status_uri: str
    canonical_train_uri: str
    canonical_validation_uri: str
    canonical_eval_uri: str
    gemini_train_uri: str
    gemini_validation_uri: str
    preflight_report_uri: str
    tuning_status_uri: str
    evals_readme_uri: str


@dataclass(frozen=True)
class RunConfig:
    """Validated operator config with defaults and derived paths resolved."""

    source_path: Path
    raw_toml: str
    round_id: str
    dataset: str
    inference_dataset_slug: str
    train_manifest_uri: str
    validation_manifest_uri: str
    eval_manifest_uri: str
    gcp_project: str
    gcs_bucket: str
    location: str
    base_model: str
    epoch_count: int
    adapter_size: str
    learning_rate_multiplier: float
    system_prompt: str
    user_prompt: str
    paths: RunPaths

    def to_record_dict(self) -> dict[str, Any]:
        """Return the resolved run config shape stored in config.json."""
        return {
            "round_id": self.round_id,
            "dataset": self.dataset,
            "inference_dataset_slug": self.inference_dataset_slug,
            "train_manifest_uri": self.train_manifest_uri,
            "validation_manifest_uri": self.validation_manifest_uri,
            "eval_manifest_uri": self.eval_manifest_uri,
            "gcp_project": self.gcp_project,
            "gcs_bucket": self.gcs_bucket,
            "location": self.location,
            "gcs_sft_prefix": f"gs://{self.gcs_bucket}/sft/runs",
            "run_gcs_prefix": self.paths.gcs_prefix,
            "base_model": self.base_model,
            "epoch_count": self.epoch_count,
            "adapter_size": self.adapter_size,
            "learning_rate_multiplier": self.learning_rate_multiplier,
            "system_prompt": self.system_prompt,
            "user_prompt": self.user_prompt,
            "canonical_train_uri": self.paths.canonical_train_uri,
            "canonical_validation_uri": self.paths.canonical_validation_uri,
            "canonical_eval_uri": self.paths.canonical_eval_uri,
            "gemini_train_uri": self.paths.gemini_train_uri,
            "gemini_validation_uri": self.paths.gemini_validation_uri,
        }


def load_run_config(path: str | Path) -> RunConfig:
    """Load, validate, and resolve an external TOML run config."""
    source_path = Path(path).expanduser()
    try:
        raw_toml = source_path.read_text(encoding="utf-8")
    except OSError as exc:
        msg = f"could not read run config {source_path}: {exc}"
        raise RunConfigError(msg) from exc

    try:
        data = tomllib.loads(raw_toml)
    except tomllib.TOMLDecodeError as exc:
        msg = f"could not parse TOML run config: {exc}"
        raise RunConfigError(msg) from exc

    round_id = _required_round_id(data, "round_id")
    dataset = _required_str(data, "dataset")
    inference_dataset_slug = _required_inference_dataset_slug(
        data, "inference_dataset_slug"
    )
    train_manifest_uri = _required_gcs_uri(data, "train_manifest_uri")
    validation_manifest_uri = _required_gcs_uri(data, "validation_manifest_uri")
    eval_manifest_uri = _required_gcs_uri(data, "eval_manifest_uri")

    gcp = _required_table(data, "gcp")
    gcp_project = _required_str(gcp, "gcp.project")
    gcs_bucket = _required_bucket(gcp, "gcp.bucket")
    location = _required_str(gcp, "gcp.location")

    sft = _required_table(data, "sft")
    base_model = _required_str(sft, "sft.base_model")
    epoch_count = _required_positive_int(sft, "sft.epoch_count")
    adapter_size = _required_adapter_size(sft, "sft.adapter_size")
    learning_rate_multiplier = _required_lr_multiplier(
        sft, "sft.learning_rate_multiplier"
    )

    prompts = data.get("prompts", {})
    if prompts is None:
        prompts = {}
    if not isinstance(prompts, dict):
        msg = "prompts must be a TOML table"
        raise RunConfigError(msg)
    system_prompt = _resolve_prompt(
        prompts,
        key="system",
        file_key="system_file",
        default=GEMINI_TRANSCRIBE_SYSTEM_PROMPT,
    )
    user_prompt = _resolve_prompt(
        prompts,
        key="user",
        file_key="user_file",
        default=GEMINI_TRANSCRIBE_USER_PROMPT,
    )

    paths = _build_paths(gcs_bucket, round_id)
    return RunConfig(
        source_path=source_path,
        raw_toml=raw_toml,
        round_id=round_id,
        dataset=dataset,
        inference_dataset_slug=inference_dataset_slug,
        train_manifest_uri=train_manifest_uri,
        validation_manifest_uri=validation_manifest_uri,
        eval_manifest_uri=eval_manifest_uri,
        gcp_project=gcp_project,
        gcs_bucket=gcs_bucket,
        location=location,
        base_model=base_model,
        epoch_count=epoch_count,
        adapter_size=adapter_size,
        learning_rate_multiplier=learning_rate_multiplier,
        system_prompt=system_prompt,
        user_prompt=user_prompt,
        paths=paths,
    )


def require_config_str(config: dict[str, Any], key: str) -> str:
    """Return a required string from durable GCS config.json state."""
    value = config.get(key)
    if not isinstance(value, str) or not value:
        msg = f"config.json missing required string field: {key}"
        raise ValueError(msg)
    return value


def require_config_int(config: dict[str, Any], key: str) -> int:
    """Return a required integer from durable GCS config.json state."""
    value = config.get(key)
    if isinstance(value, bool) or not isinstance(value, int):
        msg = f"config.json missing required integer field: {key}"
        raise TypeError(msg)
    return value


def require_config_float(config: dict[str, Any], key: str) -> float:
    """Return a required numeric value from durable GCS config.json state."""
    value = config.get(key)
    if isinstance(value, bool) or not isinstance(value, (float, int)):
        msg = f"config.json missing required numeric field: {key}"
        raise TypeError(msg)
    return float(value)


def _required_table(data: dict[str, Any], key: str) -> dict[str, Any]:
    value = data.get(key)
    if not isinstance(value, dict):
        msg = f"missing required [{key}] table"
        raise RunConfigError(msg)
    return value


def _required_str(data: dict[str, Any], key: str) -> str:
    value = _lookup(data, key)
    if not isinstance(value, str) or not value.strip():
        msg = f"missing required string field: {key}"
        raise RunConfigError(msg)
    return value.strip()


def _required_round_id(data: dict[str, Any], key: str) -> str:
    value = _required_str(data, key)
    if not ROUND_ID_PATTERN.fullmatch(value):
        msg = (
            f"{key} must be a single path component using only letters, "
            "numbers, '.', '_', and '-'"
        )
        raise RunConfigError(msg)
    return value


def _required_gcs_uri(data: dict[str, Any], key: str) -> str:
    value = _required_str(data, key)
    if not value.startswith("gs://"):
        msg = f"{key} must be a gs:// URI"
        raise RunConfigError(msg)
    return value


def _required_inference_dataset_slug(data: dict[str, Any], key: str) -> str:
    value = _required_str(data, key)
    try:
        return validate_inference_dataset_slug(value)
    except ValueError as exc:
        msg = f"{key} must be a safe relative path: {exc}"
        raise RunConfigError(msg) from exc


def _required_bucket(data: dict[str, Any], key: str) -> str:
    value = _required_str(data, key)
    if value.startswith("gs://") or "/" in value:
        msg = f"{key} must be a bucket name, not a gs:// URI or path"
        raise RunConfigError(msg)
    return value


def _required_positive_int(data: dict[str, Any], key: str) -> int:
    value = _lookup(data, key)
    if not isinstance(value, int) or value <= 0:
        msg = f"{key} must be a positive integer"
        raise RunConfigError(msg)
    return value


def _required_adapter_size(data: dict[str, Any], key: str) -> str:
    value = _required_str(data, key).upper()
    if value not in ADAPTER_SIZES:
        msg = f"{key} must be one of {', '.join(sorted(ADAPTER_SIZES))}"
        raise RunConfigError(msg)
    return value


def _required_lr_multiplier(data: dict[str, Any], key: str) -> float:
    value = _lookup(data, key)
    if not isinstance(value, (float, int)) or isinstance(value, bool):
        msg = f"{key} must be a number"
        raise RunConfigError(msg)
    lr_multiplier = float(value)
    if not 0.001 <= lr_multiplier <= 10.0:
        msg = f"{key} must be between 0.001 and 10.0"
        raise RunConfigError(msg)
    return lr_multiplier


def _resolve_prompt(
    prompts: dict[str, Any], *, key: str, file_key: str, default: str
) -> str:
    if file_key in prompts:
        msg = (
            f"prompts.{file_key} is intentionally not supported for "
            "reproducibility; use inline prompts instead"
        )
        raise RunConfigError(msg)
    value = prompts.get(key, default)
    if not isinstance(value, str) or not value.strip():
        msg = f"prompts.{key} must be a non-empty string"
        raise RunConfigError(msg)
    if value.startswith("@"):
        msg = f"prompts.{key} supports inline text only"
        raise RunConfigError(msg)
    # Prompts are copied into config.json in GCS for reproducibility. Local
    # prompt files would make a run depend on developer workstation state, so
    # this first config format only accepts inline text.
    return value


def _build_paths(bucket: str, round_id: str) -> RunPaths:
    # Keep every generated artifact for a round under one prefix. A non-empty
    # prefix is treated as owned by that round_id, which prevents accidentally
    # mixing manifests, tuning state, and eval outputs from different runs.
    gcs_prefix = f"gs://{bucket}/sft/runs/{round_id}"
    return RunPaths(
        gcs_prefix=gcs_prefix,
        run_config_uri=f"{gcs_prefix}/run_config.toml",
        config_uri=f"{gcs_prefix}/config.json",
        status_uri=f"{gcs_prefix}/status.json",
        canonical_train_uri=f"{gcs_prefix}/manifests/canonical/train.jsonl",
        canonical_validation_uri=(
            f"{gcs_prefix}/manifests/canonical/validation.jsonl"
        ),
        canonical_eval_uri=f"{gcs_prefix}/manifests/canonical/eval.jsonl",
        gemini_train_uri=f"{gcs_prefix}/model_inputs/gemini/train.jsonl",
        gemini_validation_uri=(
            f"{gcs_prefix}/model_inputs/gemini/validation.jsonl"
        ),
        preflight_report_uri=f"{gcs_prefix}/preflight/report.json",
        tuning_status_uri=f"{gcs_prefix}/tuning/status.json",
        evals_readme_uri=f"{gcs_prefix}/evals/README.txt",
    )


def _lookup(data: dict[str, Any], dotted_key: str) -> Any:
    """Return the leaf key from an already-selected TOML table.

    ``dotted_key`` is only used for human-readable error names such as
    ``gcp.project``. This is not a generic nested lookup; callers must pass the
    immediate parent table, for example ``_required_str(gcp, "gcp.project")``.
    """
    key = dotted_key.rsplit(".", maxsplit=1)[-1]
    return data.get(key)
