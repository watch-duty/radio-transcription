from __future__ import annotations

from collections.abc import Mapping

from dataset_split.types import LabeledSegment

_SPLITS = {"train", "eval"}
AUDIO_ACTIONS = frozenset({"reused", "copied", "derived", "transcoded"})
TRANSFORMATION_METADATA_KEYS = frozenset(
    {
        "original_audio_uri",
        "source_audio_uri",
        "offset",
        "duration",
        "source_duration",
        "output_duration",
        "source_format",
        "output_format",
        "source_channels",
        "output_channels",
        "mixed_to_mono",
        "resampled",
        "padded",
        "split",
        "source_group",
    }
)
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
    segments: tuple[LabeledSegment, ...],
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


def validate_model_ready_audio(segments: tuple[LabeledSegment, ...]) -> None:
    for segment in segments:
        split = _require_split(segment)
        model_ready_uri = _normalized_uri(segment.model_ready_audio_uri)
        if model_ready_uri is None or not model_ready_uri.startswith("gs://"):
            raise _model_ready_error(
                segment,
                None,
                "model_ready_audio_uri",
                "must be a non-empty gs:// URI",
            )

        metadata = segment.transformation_metadata
        if not isinstance(metadata, Mapping):
            raise _model_ready_error(
                segment,
                None,
                "transformation_metadata",
                "must be a mapping",
            )

        action = _metadata_action(metadata)
        if action is None:
            raise _model_ready_error(
                segment,
                None,
                "transformation_metadata.action",
                "is required",
            )
        if action not in AUDIO_ACTIONS:
            raise _model_ready_error(
                segment,
                action,
                "transformation_metadata.action",
                "is invalid",
            )

        missing_keys = sorted(
            key for key in TRANSFORMATION_METADATA_KEYS if key not in metadata
        )
        if missing_keys:
            raise _model_ready_error(
                segment,
                action,
                f"transformation_metadata.{missing_keys[0]}",
                "is required",
            )

        if metadata["split"] != split:
            raise _model_ready_error(
                segment,
                action,
                "transformation_metadata.split",
                f"must match segment split {split}",
            )
        if metadata["source_group"] != segment.source_group:
            raise _model_ready_error(
                segment,
                action,
                "transformation_metadata.source_group",
                f"must match segment source_group {segment.source_group}",
            )
        _validate_metadata_matches_segment(segment, metadata, action)

        derived_uri = _normalized_uri(segment.derived_audio_uri)
        if action == "derived":
            if derived_uri is None or not derived_uri.startswith("gs://"):
                raise _model_ready_error(
                    segment,
                    action,
                    "derived_audio_uri",
                    "must be a non-empty gs:// URI for derived action",
                )
            if derived_uri != model_ready_uri:
                raise _model_ready_error(
                    segment,
                    action,
                    "derived_audio_uri",
                    "must equal model_ready_audio_uri for derived action",
                )
        elif derived_uri is not None:
            raise _model_ready_error(
                segment,
                action,
                "derived_audio_uri",
                f"must be blank for {action} action",
            )


def _validate_metadata_matches_segment(
    segment: LabeledSegment,
    metadata: Mapping[str, object],
    action: str,
) -> None:
    expected_values = {
        "original_audio_uri": segment.original_audio_uri,
        "source_audio_uri": segment.audio_uri,
        "offset": segment.offset,
        "duration": segment.duration,
    }
    for field_name, expected in expected_values.items():
        if metadata[field_name] != expected:
            raise _model_ready_error(
                segment,
                action,
                f"transformation_metadata.{field_name}",
                f"must match segment {field_name}",
            )


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


def _metadata_action(metadata: Mapping[str, object]) -> str | None:
    action = metadata.get("action")
    if action is None:
        return None
    normalized = str(action).strip()
    if not normalized:
        return None
    return normalized


def _model_ready_error(
    segment: LabeledSegment,
    action: str | None,
    field_name: str,
    reason: str,
) -> SplitLeakageError:
    action_context = f" action={action}" if action is not None else ""
    return SplitLeakageError(
        f"row_index={segment.row_index}{action_context} "
        f"invalid {field_name}: {reason}"
    )
