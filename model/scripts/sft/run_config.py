"""External TOML config parsing for config-driven Gemini SFT tune runs."""

from __future__ import annotations

import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Final

from prompts import PIPELINE_SYSTEM_PROMPT, PIPELINE_USER_PROMPT

ADAPTER_SIZES: Final = frozenset({"ONE", "TWO", "FOUR", "EIGHT", "SIXTEEN"})


class RunConfigError(ValueError):
    """Raised when an external SFT run config is invalid."""


@dataclass(frozen=True)
class RunPaths:
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
    source_path: Path
    raw_toml: str
    round_id: str
    dataset: str
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
            "datasets": [self.dataset],
            "dataset": self.dataset,
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
            "epochs": self.epoch_count,
            "adapter_size": self.adapter_size,
            "learning_rate_multiplier": self.learning_rate_multiplier,
            "lr_multiplier": self.learning_rate_multiplier,
            "system_prompt": self.system_prompt,
            "user_prompt": self.user_prompt,
            "combined_train_uri": self.paths.gemini_train_uri,
            "combined_val_uri": self.paths.gemini_validation_uri,
        }


def load_run_config(path: str | Path) -> RunConfig:
    """Load, validate, and resolve an external TOML run config."""
    source_path = Path(path).expanduser()
    try:
        raw_toml = source_path.read_text(encoding="utf-8")
    except OSError as exc:
        raise RunConfigError(f"could not read run config {source_path}: {exc}") from exc

    try:
        data = tomllib.loads(raw_toml)
    except tomllib.TOMLDecodeError as exc:
        raise RunConfigError(f"could not parse TOML run config: {exc}") from exc

    round_id = _required_str(data, "round_id")
    dataset = _required_str(data, "dataset")
    train_manifest_uri = _required_gcs_uri(data, "train_manifest_uri")
    validation_manifest_uri = _required_gcs_uri(
        data, "validation_manifest_uri"
    )
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
        raise RunConfigError("prompts must be a TOML table")
    system_prompt = _resolve_prompt(
        prompts, key="system", file_key="system_file", default=PIPELINE_SYSTEM_PROMPT
    )
    user_prompt = _resolve_prompt(
        prompts, key="user", file_key="user_file", default=PIPELINE_USER_PROMPT
    )

    paths = _build_paths(gcs_bucket, round_id)
    return RunConfig(
        source_path=source_path,
        raw_toml=raw_toml,
        round_id=round_id,
        dataset=dataset,
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


def _required_table(data: dict[str, Any], key: str) -> dict[str, Any]:
    value = data.get(key)
    if not isinstance(value, dict):
        raise RunConfigError(f"missing required [{key}] table")
    return value


def _required_str(data: dict[str, Any], key: str) -> str:
    value = _lookup(data, key)
    if not isinstance(value, str) or not value.strip():
        raise RunConfigError(f"missing required string field: {key}")
    return value.strip()


def _required_gcs_uri(data: dict[str, Any], key: str) -> str:
    value = _required_str(data, key)
    if not value.startswith("gs://"):
        raise RunConfigError(f"{key} must be a gs:// URI")
    return value


def _required_bucket(data: dict[str, Any], key: str) -> str:
    value = _required_str(data, key)
    if value.startswith("gs://") or "/" in value:
        raise RunConfigError(f"{key} must be a bucket name, not a gs:// URI or path")
    return value


def _required_positive_int(data: dict[str, Any], key: str) -> int:
    value = _lookup(data, key)
    if not isinstance(value, int) or value <= 0:
        raise RunConfigError(f"{key} must be a positive integer")
    return value


def _required_adapter_size(data: dict[str, Any], key: str) -> str:
    value = _required_str(data, key).upper()
    if value not in ADAPTER_SIZES:
        raise RunConfigError(
            f"{key} must be one of {', '.join(sorted(ADAPTER_SIZES))}"
        )
    return value


def _required_lr_multiplier(data: dict[str, Any], key: str) -> float:
    value = _lookup(data, key)
    if not isinstance(value, (float, int)) or isinstance(value, bool):
        raise RunConfigError(f"{key} must be a number")
    lr_multiplier = float(value)
    if not 0.001 <= lr_multiplier <= 10.0:
        raise RunConfigError(f"{key} must be between 0.001 and 10.0")
    return lr_multiplier


def _resolve_prompt(
    prompts: dict[str, Any], *, key: str, file_key: str, default: str
) -> str:
    if file_key in prompts:
        raise RunConfigError(f"prompts.{file_key} is not supported yet")
    value = prompts.get(key, default)
    if not isinstance(value, str) or not value.strip():
        raise RunConfigError(f"prompts.{key} must be a non-empty string")
    if value.startswith("@"):
        raise RunConfigError(f"prompts.{key} supports inline text only")
    return value


def _build_paths(bucket: str, round_id: str) -> RunPaths:
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
    key = dotted_key.rsplit(".", maxsplit=1)[-1]
    return data.get(key)
