from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

from dataset_split.config import DatasetVersionConfig
from dataset_split.gcs_io import GcsInputError, TextReader, read_json_or_jsonl
from dataset_split.normalize import normalize_manifest_rows
from dataset_split.types import (
    ExcludedRow,
    LabeledSegment,
    RowValidationError,
    SourceIdentityError,
)


@dataclass(frozen=True)
class DatasetValidationSummary:
    dataset_name: str
    family: str
    manifest_uri: str
    loaded_rows: int
    valid_rows: int
    excluded_empty_text: int


@dataclass(frozen=True)
class DatasetValidationResult:
    segments: tuple[LabeledSegment, ...]
    excluded: tuple[ExcludedRow, ...]
    summaries: tuple[DatasetValidationSummary, ...]


class DatasetValidationError(ValueError):
    """Raised when a configured dataset fails Phase 1 validation."""


def load_source_map(
    uri: str | None, reader: TextReader
) -> dict[str, dict[str, str]]:
    if uri is None:
        return {}
    rows = read_json_or_jsonl(uri, reader)
    source_map: dict[str, dict[str, str]] = {}
    for row in rows:
        audio_uri = row.get("audio_uri") or row.get("audio_filepath")
        area_code = row.get("area_code")
        echo_name = row.get("echo_name")
        if audio_uri and area_code and echo_name:
            source_map[str(audio_uri)] = {
                "area_code": str(area_code),
                "echo_name": str(echo_name),
            }
    return source_map


def load_and_validate_datasets(
    config: DatasetVersionConfig,
    *,
    reader: TextReader,
    echo_registry: Mapping[str, set[str]] | None = None,
) -> DatasetValidationResult:
    segments: list[LabeledSegment] = []
    excluded_rows: list[ExcludedRow] = []
    summaries: list[DatasetValidationSummary] = []

    for dataset in config.datasets:
        try:
            rows = read_json_or_jsonl(dataset.manifest_uri, reader)
            source_map = load_source_map(dataset.source_map_uri, reader)
            result = normalize_manifest_rows(
                dataset,
                rows,
                echo_registry=echo_registry,
                source_map=source_map,
            )
        except (GcsInputError, RowValidationError, SourceIdentityError) as exc:
            raise DatasetValidationError(
                f"dataset={dataset.name} manifest={dataset.manifest_uri} "
                f"source_strategy={dataset.source_strategy} error={exc}"
            ) from exc

        valid_rows = len(result.segments)
        if valid_rows == 0:
            raise DatasetValidationError(
                f"dataset={dataset.name} manifest={dataset.manifest_uri} "
                f"source_strategy={dataset.source_strategy} produced zero "
                "valid examples after exclusions"
            )

        segments.extend(result.segments)
        excluded_rows.extend(result.excluded)
        summaries.append(
            DatasetValidationSummary(
                dataset_name=dataset.name,
                family=dataset.family,
                manifest_uri=dataset.manifest_uri,
                loaded_rows=len(rows),
                valid_rows=valid_rows,
                excluded_empty_text=sum(
                    1
                    for excluded in result.excluded
                    if excluded.reason == "empty_text"
                ),
            )
        )

    return DatasetValidationResult(
        segments=tuple(segments),
        excluded=tuple(excluded_rows),
        summaries=tuple(summaries),
    )


def validate_dataset(
    config: DatasetVersionConfig,
    *,
    reader: TextReader,
    echo_registry: Mapping[str, set[str]] | None = None,
) -> tuple[DatasetValidationSummary, ...]:
    return load_and_validate_datasets(
        config, reader=reader, echo_registry=echo_registry
    ).summaries
