from __future__ import annotations

from dataset_split.types import LabeledSegment

_SPLITS = {"train", "eval"}
_FIELD_LEAK_MESSAGES = {
    "source_group": "source_group appears in both splits",
    "original_audio_uri": "original_audio_uri appears in both splits",
    "model_ready_audio_uri": "model_ready_audio_uri appears in both splits",
}


class SplitLeakageError(ValueError):
    """Raised when a split result leaks source data across train/eval."""


def validate_split_leakage(segments: tuple[LabeledSegment, ...]) -> None:
    _validate_cross_split_overlap(
        segments,
        field_name="source_group",
        value_for_segment=lambda segment: segment.source_group,
    )
    _validate_cross_split_overlap(
        segments,
        field_name="original_audio_uri",
        value_for_segment=lambda segment: _normalized_uri(
            segment.original_audio_uri
        ),
    )
    _validate_cross_split_overlap(
        segments,
        field_name="model_ready_audio_uri",
        value_for_segment=lambda segment: _normalized_uri(
            segment.model_ready_audio_uri
        ),
    )


def validate_no_duplicate_audio_spans(
    segments: tuple[LabeledSegment, ...]
) -> None:
    seen_by_split: dict[str, set[tuple[str | None, float, float]]] = {
        "train": set(),
        "eval": set(),
    }
    for segment in segments:
        split = _require_split(segment)
        uri = _normalized_uri(segment.original_audio_uri)
        key = (uri, segment.offset, segment.duration)
        if key in seen_by_split[split]:
            raise SplitLeakageError(
                f"duplicate audio span in {split}: "
                f"original_audio_uri={uri} "
                f"offset={segment.offset} duration={segment.duration}"
            )
        seen_by_split[split].add(key)


def validate_split_integrity(segments: tuple[LabeledSegment, ...]) -> None:
    validate_split_leakage(segments)
    validate_no_duplicate_audio_spans(segments)


def _normalized_uri(value: object | None) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip()
    if not normalized:
        return None
    return normalized


def _validate_cross_split_overlap(segments, *, field_name, value_for_segment):
    values_by_split: dict[str, set[str]] = {"train": set(), "eval": set()}
    for segment in segments:
        split = _require_split(segment)
        value = value_for_segment(segment)
        if value is None:
            continue
        values_by_split[split].add(str(value))

    overlap = values_by_split["train"] & values_by_split["eval"]
    if overlap:
        value = sorted(overlap)[0]
        raise SplitLeakageError(
            f"leakage: {_FIELD_LEAK_MESSAGES[field_name]}: {value}"
        )


def _require_split(segment: LabeledSegment) -> str:
    if segment.split not in _SPLITS:
        raise SplitLeakageError(f"row_index={segment.row_index} missing split")
    return segment.split
