from __future__ import annotations

import math
import tomllib
from dataclasses import dataclass
from typing import Any

SUPPORTED_DATASET_FAMILIES = {
    "bcfy_calls",
    "bcfy_feeds",
    "echo",
    "fire_notifications",
}
SUPPORTED_SOURCE_STRATEGIES = {
    "bcfy_calls",
    "bcfy_feeds",
    "echo",
    "fire_notifications",
}


class ConfigValidationError(ValueError):
    """Raised when a dataset-version TOML config is invalid."""


@dataclass(frozen=True)
class InputDatasetConfig:
    name: str
    family: str
    manifest_uri: str
    source_strategy: str
    source_map_uri: str | None = None


@dataclass(frozen=True)
class DatasetVersionConfig:
    dataset_version_id: str
    train_ratio: float
    eval_ratio: float
    output_gcs_prefix: str
    datasets: tuple[InputDatasetConfig, ...]


def parse_dataset_version_config_toml(
    toml_text: str, *, source: str = "<memory>"
) -> DatasetVersionConfig:
    try:
        raw = tomllib.loads(toml_text)
    except tomllib.TOMLDecodeError as exc:
        raise ConfigValidationError(f"{source}: invalid TOML: {exc}") from exc

    _require_keys(
        raw,
        (
            "dataset_version_id",
            "train_ratio",
            "eval_ratio",
            "output_gcs_prefix",
            "datasets",
        ),
        source=source,
        label="config",
    )

    dataset_version_id = _require_str(raw, "dataset_version_id", source)
    train_ratio = _require_number(raw, "train_ratio", source)
    eval_ratio = _require_number(raw, "eval_ratio", source)
    output_gcs_prefix = _require_str(raw, "output_gcs_prefix", source)
    _require_gs_uri(
        output_gcs_prefix, label="output_gcs_prefix", source=source
    )

    if abs((train_ratio + eval_ratio) - 1.0) > 1e-9:
        raise ConfigValidationError(
            f"{source}: train_ratio + eval_ratio must equal 1.0"
        )

    raw_datasets = raw["datasets"]
    if not isinstance(raw_datasets, list) or not raw_datasets:
        raise ConfigValidationError(
            f"{source}: datasets must contain at least one entry"
        )

    names: set[str] = set()
    datasets: list[InputDatasetConfig] = []
    for index, dataset_raw in enumerate(raw_datasets):
        if not isinstance(dataset_raw, dict):
            raise ConfigValidationError(
                f"{source}: datasets[{index}] must be a table"
            )
        _require_keys(
            dataset_raw,
            ("name", "family", "manifest_uri", "source_strategy"),
            source=source,
            label=f"datasets[{index}]",
        )
        name = _require_str(dataset_raw, "name", source, index=index)
        if name in names:
            raise ConfigValidationError(
                f"{source}: duplicate dataset name: {name}"
            )
        names.add(name)

        family = _require_str(dataset_raw, "family", source, index=index)
        if family not in SUPPORTED_DATASET_FAMILIES:
            raise ConfigValidationError(
                f"{source}: datasets[{index}].family is unsupported: {family}"
            )

        source_strategy = _require_str(
            dataset_raw, "source_strategy", source, index=index
        )
        if source_strategy not in SUPPORTED_SOURCE_STRATEGIES:
            raise ConfigValidationError(
                f"{source}: datasets[{index}].source_strategy is unsupported: "
                f"{source_strategy}"
            )

        manifest_uri = _require_str(
            dataset_raw, "manifest_uri", source, index=index
        )
        _require_gs_uri(
            manifest_uri,
            label=f"datasets[{index}].manifest_uri",
            source=source,
        )

        source_map_uri = dataset_raw.get("source_map_uri")
        if source_map_uri is not None:
            if not isinstance(source_map_uri, str):
                raise ConfigValidationError(
                    f"{source}: datasets[{index}].source_map_uri must be a string"
                )
            _require_gs_uri(
                source_map_uri,
                label=f"datasets[{index}].source_map_uri",
                source=source,
            )

        datasets.append(
            InputDatasetConfig(
                name=name,
                family=family,
                manifest_uri=manifest_uri,
                source_strategy=source_strategy,
                source_map_uri=source_map_uri,
            )
        )

    return DatasetVersionConfig(
        dataset_version_id=dataset_version_id,
        train_ratio=train_ratio,
        eval_ratio=eval_ratio,
        output_gcs_prefix=output_gcs_prefix,
        datasets=tuple(datasets),
    )


def _require_keys(
    value: dict[str, Any],
    keys: tuple[str, ...],
    *,
    source: str,
    label: str,
) -> None:
    missing = [key for key in keys if key not in value]
    if missing:
        raise ConfigValidationError(
            f"{source}: {label} missing required key(s): "
            f"{', '.join(missing)}"
        )


def _require_str(
    value: dict[str, Any], key: str, source: str, *, index: int | None = None
) -> str:
    item = value[key]
    if not isinstance(item, str) or not item:
        raise ConfigValidationError(
            f"{source}: {_field_name(key, index)} must be a non-empty string"
        )
    return item


def _require_number(value: dict[str, Any], key: str, source: str) -> float:
    item = value[key]
    if not isinstance(item, int | float) or isinstance(item, bool):
        raise ConfigValidationError(f"{source}: {key} must be a number")
    parsed = float(item)
    if not math.isfinite(parsed):
        raise ConfigValidationError(
            f"{source}: {key} must be a finite number"
        )
    return parsed


def _require_gs_uri(uri: str, *, label: str, source: str) -> None:
    if not uri.startswith("gs://"):
        raise ConfigValidationError(f"{source}: {label} must be a gs:// URI")


def _field_name(key: str, index: int | None) -> str:
    if index is None:
        return key
    return f"datasets[{index}].{key}"
