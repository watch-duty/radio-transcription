from __future__ import annotations

from collections.abc import Iterable, Mapping, Set

from dataset_split.config import InputDatasetConfig
from dataset_split.source_keys import resolve_source_group
from dataset_split.types import (
    ExcludedRow,
    LabeledSegment,
    NormalizationResult,
    RowValidationError,
)

_AUDIO_URI_FIELDS = (
    "audio_filepath",
    "audio_uri",
    "fileUri",
    "url",
    "s3_url",
    "original_audio_uri",
)


def normalize_text(value: object) -> str:
    if value is None:
        return ""
    return str(value).replace("\n", " ").replace("\r", " ").strip()


def normalize_manifest_rows(
    dataset: InputDatasetConfig,
    rows: Iterable[Mapping[str, object]],
    *,
    echo_registry: Mapping[str, set[str]] | None = None,
    ambiguous_echo_names: Set[str] | None = None,
    source_map: Mapping[str, Mapping[str, str]] | None = None,
) -> NormalizationResult:
    segments: list[LabeledSegment] = []
    excluded: list[ExcludedRow] = []

    for row_index, row in enumerate(rows):
        audio_uri = _first_present_text(row, _AUDIO_URI_FIELDS)
        text = normalize_text(row.get("text"))

        if not text:
            excluded.append(
                ExcludedRow(
                    dataset_name=dataset.name,
                    row_index=row_index,
                    audio_uri=audio_uri,
                    reason="empty_text",
                )
            )
            continue

        if audio_uri is None:
            raise RowValidationError(
                f"dataset={dataset.name} row_index={row_index} "
                "missing audio URI"
            )

        source_group = resolve_source_group(
            row,
            dataset_family=dataset.family,
            source_strategy=dataset.source_strategy,
            echo_registry=echo_registry,
            ambiguous_echo_names=ambiguous_echo_names,
            source_map=source_map,
        )

        segments.append(
            LabeledSegment(
                dataset_name=dataset.name,
                dataset_family=dataset.family,
                source_strategy=dataset.source_strategy,
                source_group=source_group,
                audio_uri=audio_uri,
                original_audio_uri=str(row.get("original_audio_uri") or audio_uri),
                text=text,
                row_index=row_index,
                offset=float(row.get("offset") or 0.0),
                duration=float(row.get("duration") or 0.0),
                timestamp=_optional_str(row, "timestamp"),
                example_id=_optional_str(row, "example_id"),
                segment_id=_optional_str(row, "segment_id"),
                raw_row=dict(row),
            )
        )

    return NormalizationResult(
        segments=tuple(segments), excluded=tuple(excluded)
    )


def _first_present_text(
    row: Mapping[str, object], keys: tuple[str, ...]
) -> str | None:
    for key in keys:
        value = row.get(key)
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return None


def _optional_str(row: Mapping[str, object], key: str) -> str | None:
    if key not in row or row[key] is None:
        return None
    return str(row[key])
