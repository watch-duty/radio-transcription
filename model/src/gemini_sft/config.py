"""External TOML config parsing for config-driven Gemini SFT tune runs."""

from __future__ import annotations

import re
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Final

from common.gemini.context import PRIOR_CONTEXT_MODES
from common.gemini.prompts import (
    GEMINI_TRANSCRIBE_SYSTEM_PROMPT,
    GEMINI_TRANSCRIBE_USER_PROMPT,
)
from common.inference_manifest import (
    validate_artifact_label,
    validate_inference_dataset_slug,
)

ADAPTER_SIZES: Final = frozenset({"ONE", "TWO", "FOUR", "EIGHT", "SIXTEEN"})
EVAL_EXECUTION_BACKENDS: Final = frozenset({"batch", "online"})
EVAL_MODEL_FIELDS: Final = frozenset({"label", "model"})
EVAL_EXECUTION_FIELDS: Final = frozenset(
    {"backend", "concurrency", "limit", "max_retries"}
)
ROUND_ID_PATTERN: Final = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")
EVAL_MODEL_REQUIRED_MESSAGE: Final = (
    "eval configs must define one [eval.model] target with required label "
    "and model fields; no legacy base_model/endpoint fallback is used"
)
CONFIG_EVAL_MODEL_REQUIRED_MESSAGE: Final = (
    "config.json missing required eval_model object; configure one "
    "[eval.model] target with required label and model fields; "
    "base_model/endpoint fallback is not supported"
)


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
class EvalModelTarget:
    """Static eval target config for one model or endpoint string.

    Attributes:
        label: Safe artifact/report label for the target.
        model: Unclassified model or endpoint string supplied by the operator.
    """

    label: str
    model: str

    def to_record_dict(self) -> dict[str, str]:
        """Return the JSON-compatible config.json record for this target."""
        return {"label": self.label, "model": self.model}


@dataclass(frozen=True)
class EvalExecutionConfig:
    """Execution controls for eval target inference."""

    backend: str | None = None
    limit: int | None = None
    concurrency: int = 16
    max_retries: int = 3

    def to_record_dict(self) -> dict[str, int | str]:
        """Return the durable config.json execution record."""
        record: dict[str, int | str] = {
            "concurrency": self.concurrency,
            "max_retries": self.max_retries,
        }
        if self.backend is not None:
            record["backend"] = self.backend
        if self.limit is not None:
            record["limit"] = self.limit
        return record


@dataclass(frozen=True)
class RunConfig:
    """Validated operator config with defaults and derived paths resolved."""

    source_path: Path
    raw_toml: str
    round_id: str
    dataset: str
    inference_dataset_slug: str
    train_manifest_uri: str | None
    validation_manifest_uri: str | None
    eval_manifest_uri: str
    eval_model: EvalModelTarget | None
    eval_execution: EvalExecutionConfig
    gcp_project: str
    gcs_bucket: str
    location: str
    base_model: str
    epoch_count: int
    adapter_size: str
    learning_rate_multiplier: float
    continuous_tuned_model_name: str | None
    continuous_checkpoint_id: str | None
    prior_context_count: int
    prior_context_mode: str
    system_prompt: str
    user_prompt: str
    paths: RunPaths

    def to_record_dict(self) -> dict[str, Any]:
        """Return the resolved run config shape stored in config.json."""
        record = {
            "round_id": self.round_id,
            "dataset": self.dataset,
            "inference_dataset_slug": self.inference_dataset_slug,
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
            "prior_context_count": self.prior_context_count,
            "prior_context_mode": self.prior_context_mode,
            "system_prompt": self.system_prompt,
            "user_prompt": self.user_prompt,
            "canonical_train_uri": self.paths.canonical_train_uri,
            "canonical_validation_uri": self.paths.canonical_validation_uri,
            "canonical_eval_uri": self.paths.canonical_eval_uri,
            "gemini_train_uri": self.paths.gemini_train_uri,
            "gemini_validation_uri": self.paths.gemini_validation_uri,
        }
        if self.continuous_tuned_model_name is not None:
            record["continuous_tuned_model_name"] = (
                self.continuous_tuned_model_name
            )
        if self.continuous_checkpoint_id is not None:
            record["continuous_checkpoint_id"] = self.continuous_checkpoint_id
        if self.train_manifest_uri is not None:
            record["train_manifest_uri"] = self.train_manifest_uri
        if self.validation_manifest_uri is not None:
            record["validation_manifest_uri"] = self.validation_manifest_uri
        if self.eval_model is not None:
            record["eval_model"] = self.eval_model.to_record_dict()
        record["eval_execution"] = self.eval_execution.to_record_dict()
        return record


def load_run_config(path: str | Path) -> RunConfig:
    """Load, validate, and resolve an external TOML run config."""
    return _load_run_config(path, require_training_manifests=True)


def load_eval_run_config(path: str | Path) -> RunConfig:
    """Load an eval TOML config without requiring train/validation manifests."""
    return _load_run_config(path, require_training_manifests=False)


def _load_run_config(
    path: str | Path,
    *,
    require_training_manifests: bool,
) -> RunConfig:
    """Load and resolve a TOML run config."""
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
    if require_training_manifests:
        train_manifest_uri = _required_gcs_uri(data, "train_manifest_uri")
        validation_manifest_uri = _required_gcs_uri(
            data,
            "validation_manifest_uri",
        )
    else:
        train_manifest_uri = _optional_gcs_uri(data, "train_manifest_uri")
        validation_manifest_uri = _optional_gcs_uri(
            data,
            "validation_manifest_uri",
        )
    eval_manifest_uri = _required_gcs_uri(data, "eval_manifest_uri")
    eval_table = _eval_table(data)
    eval_model = _eval_model_target(
        eval_table,
        required=not require_training_manifests,
    )
    eval_execution = _eval_execution_config(eval_table)

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
    continuous = data.get("continuous_tuning", {})
    if continuous is None:
        continuous = {}
    if not isinstance(continuous, dict):
        msg = "continuous_tuning must be a TOML table"
        raise RunConfigError(msg)
    continuous_tuned_model_name = _optional_model_resource_name(
        continuous,
        "continuous_tuning.tuned_model_name",
    )
    continuous_checkpoint_id = _optional_stripped_str(
        continuous,
        "continuous_tuning.checkpoint_id",
    )
    if continuous_checkpoint_id and not continuous_tuned_model_name:
        msg = (
            "continuous_tuning.checkpoint_id requires "
            "continuous_tuning.tuned_model_name"
        )
        raise RunConfigError(msg)
    context = data.get("context", {})
    if context is None:
        context = {}
    if not isinstance(context, dict):
        msg = "context must be a TOML table"
        raise RunConfigError(msg)
    prior_context_count = _optional_nonnegative_int(
        context,
        "context.prior_turn_count",
        default=0,
    )
    prior_context_mode = _optional_prior_context_mode(
        context,
        "context.prior_context_mode",
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
        eval_model=eval_model,
        eval_execution=eval_execution,
        gcp_project=gcp_project,
        gcs_bucket=gcs_bucket,
        location=location,
        base_model=base_model,
        epoch_count=epoch_count,
        adapter_size=adapter_size,
        learning_rate_multiplier=learning_rate_multiplier,
        continuous_tuned_model_name=continuous_tuned_model_name,
        continuous_checkpoint_id=continuous_checkpoint_id,
        prior_context_count=prior_context_count,
        prior_context_mode=prior_context_mode,
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


def require_config_eval_model(config: dict[str, Any]) -> EvalModelTarget:
    """Return the validated eval target from durable GCS config.json state."""
    if "eval_models" in config:
        msg = "config.json field eval_models is not supported; use eval_model"
        raise ValueError(msg)

    raw_target = config.get("eval_model")
    if raw_target is None:
        raise ValueError(CONFIG_EVAL_MODEL_REQUIRED_MESSAGE)
    if not isinstance(raw_target, dict):
        msg = (
            "config.json field eval_model must be an object with exactly "
            "label and model fields"
        )
        raise TypeError(msg)
    return _parse_eval_model_mapping(
        raw_target,
        object_name="config.json field eval_model",
        label_name="config.json field eval_model.label",
        model_name="config.json field eval_model.model",
        error_cls=ValueError,
    )


def require_config_eval_execution(config: dict[str, Any]) -> EvalExecutionConfig:
    """Return validated eval execution controls from durable config.json."""
    raw_execution = config.get("eval_execution")
    if raw_execution is None:
        return EvalExecutionConfig()
    if not isinstance(raw_execution, dict):
        msg = "config.json field eval_execution must be an object"
        raise TypeError(msg)
    return _config_eval_execution_config(raw_execution)


def optional_config_prior_context_mode(
    config: dict[str, Any], key: str
) -> str:
    """Return a validated durable prior-context mode."""
    value = config.get(key, "text_turns")
    if not isinstance(value, str):
        msg = (
            f"config.json field must be one of "
            f"{', '.join(sorted(PRIOR_CONTEXT_MODES))}: {key}"
        )
        raise TypeError(msg)
    mode = value.strip().lower()
    if mode not in PRIOR_CONTEXT_MODES:
        msg = (
            f"config.json field must be one of "
            f"{', '.join(sorted(PRIOR_CONTEXT_MODES))}: {key}"
        )
        raise ValueError(msg)
    return mode


def _required_table(data: dict[str, Any], key: str) -> dict[str, Any]:
    value = data.get(key)
    if not isinstance(value, dict):
        msg = f"missing required [{key}] table"
        raise RunConfigError(msg)
    return value


def _eval_table(data: dict[str, Any]) -> dict[str, Any] | None:
    eval_table = data.get("eval")
    if eval_table is None:
        return None
    if not isinstance(eval_table, dict):
        msg = "eval must be a TOML table"
        raise RunConfigError(msg)

    if "models" in eval_table:
        msg = (
            "eval has no plural eval target support: use [eval.model], "
            "not [[eval.models]]"
        )
        raise RunConfigError(msg)

    unsupported_eval_fields = sorted(set(eval_table) - {"execution", "model"})
    if unsupported_eval_fields:
        msg = (
            "eval must contain only [eval.model] and [eval.execution]; "
            "unsupported fields: "
            f"{', '.join(unsupported_eval_fields)}"
        )
        raise RunConfigError(msg)
    return eval_table


def _eval_model_target(
    eval_table: dict[str, Any] | None,
    *,
    required: bool,
) -> EvalModelTarget | None:
    if eval_table is None:
        if required:
            raise RunConfigError(EVAL_MODEL_REQUIRED_MESSAGE)
        return None

    raw_target = eval_table.get("model")
    if raw_target is None:
        if required:
            raise RunConfigError(EVAL_MODEL_REQUIRED_MESSAGE)
        return None
    if not isinstance(raw_target, dict):
        msg = (
            "eval.model must be a TOML table with exactly label and model "
            "fields"
        )
        raise RunConfigError(msg)
    return _parse_eval_model_mapping(
        raw_target,
        object_name="eval.model",
        label_name="eval.model.label",
        model_name="eval.model.model",
        error_cls=RunConfigError,
    )


def _parse_eval_model_mapping(
    raw_target: dict[str, Any],
    *,
    object_name: str,
    label_name: str,
    model_name: str,
    error_cls: type[Exception],
) -> EvalModelTarget:
    keys = set(raw_target)
    if keys != EVAL_MODEL_FIELDS:
        missing = sorted(EVAL_MODEL_FIELDS - keys)
        unsupported = sorted(keys - EVAL_MODEL_FIELDS)
        details = []
        if missing:
            details.append(f"missing: {', '.join(missing)}")
        if unsupported:
            details.append(f"unsupported: {', '.join(unsupported)}")
        msg = (
            f"{object_name} must contain exactly label and model fields "
            f"({'; '.join(details)})"
        )
        raise error_cls(msg)

    label_value = raw_target["label"]
    if not isinstance(label_value, str) or not label_value.strip():
        msg = f"{label_name} must be a non-empty string"
        raise error_cls(msg)
    try:
        label = validate_artifact_label(label_value.strip())
    except ValueError as exc:
        msg = f"{label_name} is invalid: {exc}"
        raise error_cls(msg) from exc

    model_value = raw_target["model"]
    if not isinstance(model_value, str) or not model_value.strip():
        msg = f"{model_name} must be a non-empty string"
        raise error_cls(msg)
    return EvalModelTarget(label=label, model=model_value.strip())


def _eval_execution_config(
    eval_table: dict[str, Any] | None,
) -> EvalExecutionConfig:
    if eval_table is None:
        return EvalExecutionConfig()
    raw_execution = eval_table.get("execution")
    if raw_execution is None:
        return EvalExecutionConfig()
    if not isinstance(raw_execution, dict):
        msg = "eval.execution must be a TOML table"
        raise RunConfigError(msg)

    _reject_unsupported_fields(
        raw_execution,
        allowed=EVAL_EXECUTION_FIELDS,
        context="eval.execution",
        error_cls=RunConfigError,
    )
    return EvalExecutionConfig(
        backend=_optional_eval_execution_backend(
            raw_execution,
            "eval.execution.backend",
        ),
        limit=_optional_positive_int(
            raw_execution,
            "eval.execution.limit",
            default=None,
        ),
        concurrency=_optional_positive_int(
            raw_execution,
            "eval.execution.concurrency",
            default=16,
        ),
        max_retries=_optional_positive_int(
            raw_execution,
            "eval.execution.max_retries",
            default=3,
        ),
    )


def _config_eval_execution_config(
    raw_execution: dict[str, Any],
) -> EvalExecutionConfig:
    _reject_unsupported_fields(
        raw_execution,
        allowed=EVAL_EXECUTION_FIELDS,
        context="config.json field eval_execution",
        error_cls=ValueError,
    )
    return EvalExecutionConfig(
        backend=_optional_config_eval_execution_backend(raw_execution),
        limit=_optional_config_positive_int(
            raw_execution,
            "limit",
            default=None,
        ),
        concurrency=_optional_config_positive_int(
            raw_execution,
            "concurrency",
            default=16,
        ),
        max_retries=_optional_config_positive_int(
            raw_execution,
            "max_retries",
            default=3,
        ),
    )


def _reject_unsupported_fields(
    data: dict[str, Any],
    *,
    allowed: frozenset[str],
    context: str,
    error_cls: type[Exception],
) -> None:
    unsupported_fields = sorted(set(data) - allowed)
    if unsupported_fields:
        msg = f"{context} unsupported fields: {', '.join(unsupported_fields)}"
        raise error_cls(msg)


def _optional_eval_execution_backend(
    data: dict[str, Any],
    key: str,
) -> str | None:
    value = _lookup(data, key)
    if value is None:
        return None
    if not isinstance(value, str):
        msg = f"{key} must be one of batch, online"
        raise RunConfigError(msg)
    backend = value.strip()
    if backend not in EVAL_EXECUTION_BACKENDS:
        msg = f"{key} must be one of batch, online"
        raise RunConfigError(msg)
    return backend


def _optional_config_eval_execution_backend(
    data: dict[str, Any],
) -> str | None:
    value = data.get("backend")
    if value is None:
        return None
    if not isinstance(value, str):
        msg = "config.json field eval_execution.backend must be one of batch, online"
        raise TypeError(msg)
    backend = value.strip()
    if backend not in EVAL_EXECUTION_BACKENDS:
        msg = "config.json field eval_execution.backend must be one of batch, online"
        raise ValueError(msg)
    return backend


def _required_artifact_label(data: dict[str, Any], key: str) -> str:
    label = _required_str(data, key)
    try:
        return validate_artifact_label(label)
    except ValueError as exc:
        msg = f"{key} is invalid: {exc}"
        raise RunConfigError(msg) from exc


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


def _optional_stripped_str(data: dict[str, Any], key: str) -> str | None:
    value = _lookup(data, key)
    if value is None:
        return None
    if not isinstance(value, str):
        msg = f"{key} must be a string"
        raise RunConfigError(msg)
    text = value.strip()
    return text or None


def _optional_model_resource_name(
    data: dict[str, Any],
    key: str,
) -> str | None:
    value = _optional_stripped_str(data, key)
    if value is None:
        return None
    if not value.startswith("projects/"):
        msg = (
            f"{key} must be a Vertex model resource name starting with "
            "'projects/'"
        )
        raise RunConfigError(msg)
    return value


def _optional_gcs_uri(data: dict[str, Any], key: str) -> str | None:
    value = _lookup(data, key)
    if value is None:
        return None
    if not isinstance(value, str):
        msg = f"{key} must be a gs:// URI"
        raise RunConfigError(msg)
    if not value.strip():
        return None
    uri = value.strip()
    if not uri.startswith("gs://"):
        msg = f"{key} must be a gs:// URI"
        raise RunConfigError(msg)
    return uri


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


def _optional_positive_int(
    data: dict[str, Any],
    key: str,
    *,
    default: int | None,
) -> int | None:
    value = _lookup(data, key)
    if value is None:
        return default
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        msg = f"{key} must be a positive integer"
        raise RunConfigError(msg)
    return value


def _optional_config_positive_int(
    data: dict[str, Any],
    key: str,
    *,
    default: int | None,
) -> int | None:
    value = data.get(key)
    if value is None:
        return default
    field = f"config.json field eval_execution.{key}"
    if isinstance(value, bool) or not isinstance(value, int):
        msg = f"{field} must be a positive integer"
        raise TypeError(msg)
    if value <= 0:
        msg = f"{field} must be a positive integer"
        raise ValueError(msg)
    return value


def _optional_nonnegative_int(
    data: dict[str, Any], key: str, *, default: int
) -> int:
    value = _lookup(data, key)
    if value is None:
        return default
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        msg = f"{key} must be a non-negative integer"
        raise RunConfigError(msg)
    return value


def _optional_prior_context_mode(data: dict[str, Any], key: str) -> str:
    value = _lookup(data, key)
    if value is None:
        return "text_turns"
    if not isinstance(value, str):
        msg = f"{key} must be one of {', '.join(sorted(PRIOR_CONTEXT_MODES))}"
        raise RunConfigError(msg)
    mode = value.strip().lower()
    if mode not in PRIOR_CONTEXT_MODES:
        msg = (
            f"{key} must be one of {', '.join(sorted(PRIOR_CONTEXT_MODES))}; "
            "Vertex SFT examples can contain only one audio part"
        )
        raise RunConfigError(msg)
    return mode


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
