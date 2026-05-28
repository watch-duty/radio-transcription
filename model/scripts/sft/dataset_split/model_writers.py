from __future__ import annotations

import json
from dataclasses import dataclass

from dataset_split.leakage import validate_split_integrity
from dataset_split.types import LabeledSegment

_SPLITS = ("train", "eval")
_WHISPER_RECOMMENDED_MAX_DURATION_SECONDS = 30.0
_WHISPER_PREPROCESSING = {
    "recommendation": "preserve_original_uri_with_offset_duration",
    "clip_derivation_phase": 4,
    "recommended_max_duration_seconds": (
        _WHISPER_RECOMMENDED_MAX_DURATION_SECONDS
    ),
}


class ModelWriterError(ValueError):
    """Raised when model-input artifacts cannot be built."""


@dataclass(frozen=True)
class WriterWarning:
    writer: str
    code: str
    severity: str
    row_index: int
    message: str
    details: dict[str, object]

    def to_dict(self) -> dict[str, object]:
        return {
            "writer": self.writer,
            "code": self.code,
            "severity": self.severity,
            "row_index": self.row_index,
            "message": self.message,
            "details": dict(self.details),
        }


@dataclass(frozen=True)
class ModelWriterResult:
    rows_by_split: dict[str, tuple[dict[str, object], ...]]
    config: dict[str, object] | None
    warnings: tuple[WriterWarning, ...]

    def jsonl_by_split(self) -> dict[str, str]:
        return {
            split: _serialize_jsonl(self.rows_by_split.get(split, ()))
            for split in _SPLITS
        }

    def warnings_by_writer(self) -> dict[str, list[dict[str, object]]]:
        grouped: dict[str, list[dict[str, object]]] = {}
        for warning in self.warnings:
            grouped.setdefault(warning.writer, []).append(warning.to_dict())
        return grouped


def build_nemo_inputs(
    segments: tuple[LabeledSegment, ...],
    *,
    train_manifest_uri: str,
    eval_manifest_uri: str,
) -> ModelWriterResult:
    segment_tuple = tuple(segments)
    validate_split_integrity(segment_tuple)
    rows_by_split = _empty_rows_by_split()
    for segment in segment_tuple:
        rows_by_split[_require_split(segment)].append(_nemo_row(segment))

    return ModelWriterResult(
        rows_by_split=_freeze_rows(rows_by_split),
        config={
            "train_manifest": _require_text(
                train_manifest_uri, label="train_manifest_uri"
            ),
            "validation_manifest": _require_text(
                eval_manifest_uri, label="eval_manifest_uri"
            ),
            "manifest_format": "nemo_jsonl",
        },
        warnings=(),
    )


def build_whisper_inputs(
    segments: tuple[LabeledSegment, ...],
) -> ModelWriterResult:
    segment_tuple = tuple(segments)
    validate_split_integrity(segment_tuple)
    rows_by_split = _empty_rows_by_split()
    warnings: list[WriterWarning] = []

    for segment in segment_tuple:
        rows_by_split[_require_split(segment)].append(_whisper_row(segment))
        if float(segment.duration) > _WHISPER_RECOMMENDED_MAX_DURATION_SECONDS:
            warnings.append(
                WriterWarning(
                    writer="whisper",
                    code="whisper_duration_over_30s",
                    severity="warning",
                    row_index=segment.row_index,
                    message=(
                        "Whisper segment duration exceeds the recommended "
                        "30.0 second maximum; row remains in output."
                    ),
                    details={
                        "duration": float(segment.duration),
                        "recommended_max_duration_seconds": (
                            _WHISPER_RECOMMENDED_MAX_DURATION_SECONDS
                        ),
                        "audio_uri": segment.audio_uri,
                        "example_id": segment.example_id,
                        "segment_id": segment.segment_id,
                    },
                )
            )

    return ModelWriterResult(
        rows_by_split=_freeze_rows(rows_by_split),
        config=None,
        warnings=tuple(warnings),
    )


def _nemo_row(segment: LabeledSegment) -> dict[str, object]:
    return {
        "audio_filepath": segment.audio_uri,
        "text": segment.text,
        "duration": segment.duration,
        "offset": segment.offset,
        "example_id": segment.example_id,
        "segment_id": segment.segment_id,
    }


def _whisper_row(segment: LabeledSegment) -> dict[str, object]:
    return {
        "audio_uri": segment.audio_uri,
        "text": segment.text,
        "duration": segment.duration,
        "offset": segment.offset,
        "dataset_name": segment.dataset_name,
        "dataset_family": segment.dataset_family,
        "source_group": segment.source_group,
        "split": _require_split(segment),
        "example_id": segment.example_id,
        "segment_id": segment.segment_id,
        "preprocessing": dict(_WHISPER_PREPROCESSING),
    }


def _empty_rows_by_split() -> dict[str, list[dict[str, object]]]:
    return {split: [] for split in _SPLITS}


def _freeze_rows(
    rows_by_split: dict[str, list[dict[str, object]]],
) -> dict[str, tuple[dict[str, object], ...]]:
    return {split: tuple(rows_by_split.get(split, ())) for split in _SPLITS}


def _require_split(segment: LabeledSegment) -> str:
    if segment.split not in _SPLITS:
        raise ModelWriterError(f"row_index={segment.row_index} missing split")
    return segment.split


def _require_text(value: str, *, label: str) -> str:
    text = value.strip()
    if not text:
        raise ModelWriterError(f"{label} must not be empty")
    return text


def _serialize_jsonl(rows: tuple[dict[str, object], ...]) -> str:
    if not rows:
        return ""
    return "\n".join(json.dumps(row, sort_keys=True) for row in rows) + "\n"
