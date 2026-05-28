from __future__ import annotations

import json
from collections import defaultdict

from dataset_split.leakage import (
    validate_model_ready_audio,
    validate_split_integrity,
)
from dataset_split.types import LabeledSegment

_SPLITS = ("train", "eval")


class CanonicalManifestError(ValueError):
    """Raised when canonical manifest rows cannot be built."""


def canonical_row(segment: LabeledSegment) -> dict[str, object]:
    split = _require_split(
        segment.split, label=f"row_index={segment.row_index}"
    )
    return {
        "dataset_name": segment.dataset_name,
        "dataset_family": segment.dataset_family,
        "source_strategy": segment.source_strategy,
        "source_group": segment.source_group,
        "split": split,
        "audio_uri": segment.audio_uri,
        "original_audio_uri": segment.original_audio_uri,
        "text": segment.text,
        "offset": segment.offset,
        "duration": segment.duration,
        "row_index": segment.row_index,
        "example_id": segment.example_id,
        "segment_id": segment.segment_id,
        "timestamp": segment.timestamp,
        "model_ready_audio_uri": segment.model_ready_audio_uri,
        "derived_audio_uri": segment.derived_audio_uri,
        "transformation_metadata": segment.transformation_metadata,
    }


def canonical_rows(
    segments: tuple[LabeledSegment, ...], *, split: str
) -> tuple[dict[str, object], ...]:
    requested_split = _require_split(split, label="split")
    validate_split_integrity(segments)
    validate_model_ready_audio(segments)
    return tuple(
        canonical_row(segment)
        for segment in segments
        if segment.split == requested_split
    )


def canonical_manifests(segments: tuple[LabeledSegment, ...]) -> dict[str, str]:
    validate_split_integrity(segments)
    validate_model_ready_audio(segments)
    return {
        split: serialize_jsonl(
            tuple(
                canonical_row(segment)
                for segment in segments
                if segment.split == split
            )
        )
        for split in _SPLITS
    }


def per_dataset_manifests(
    segments: tuple[LabeledSegment, ...],
) -> dict[str, dict[str, str]]:
    validate_split_integrity(segments)
    validate_model_ready_audio(segments)
    rows_by_dataset_split: dict[str, dict[str, list[dict[str, object]]]] = (
        defaultdict(lambda: {split: [] for split in _SPLITS})
    )
    for segment in segments:
        rows_by_dataset_split[segment.dataset_name][segment.split].append(
            canonical_row(segment)
        )

    return {
        dataset_name: {
            split: serialize_jsonl(
                tuple(rows_by_dataset_split[dataset_name][split])
            )
            for split in _SPLITS
        }
        for dataset_name in sorted(rows_by_dataset_split)
    }


def serialize_jsonl(rows: tuple[dict[str, object], ...]) -> str:
    if not rows:
        return ""
    return (
        "\n".join(
            json.dumps(row, sort_keys=True, allow_nan=False) for row in rows
        )
        + "\n"
    )


def _require_split(value: str | None, *, label: str) -> str:
    if value not in _SPLITS:
        raise CanonicalManifestError(
            f"{label} must be one of {_SPLITS}: {value}"
        )
    return value
