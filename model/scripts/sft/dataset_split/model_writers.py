from __future__ import annotations

import json
import math
from dataclasses import dataclass
from typing import Any

from common.sft import build_example, validate_example

from dataset_split.leakage import validate_split_integrity
from dataset_split.types import LabeledSegment

_SPLITS = ("train", "eval")
DEFAULT_GEMINI_BASE_MODEL = "gemini-3.1-flash-lite"
DEFAULT_GEMINI_REGION = "us-central1"
_GEMINI_ADAPTER_SIZES = frozenset({"ONE", "FOUR", "EIGHT", "SIXTEEN"})
_WHISPER_RECOMMENDED_MAX_DURATION_SECONDS = 30.0
_WHISPER_PREPROCESSING = {
    "recommendation": "use_audio_uri_with_offset_relative_to_audio_uri",
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
    summary_by_split: dict[str, dict[str, float | int]]

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


def summarize_model_writer_result(
    result: ModelWriterResult,
) -> dict[str, object]:
    splits = {
        split: _count_duration(result.summary_by_split.get(split, {}))
        for split in _SPLITS
    }
    return {
        "splits": splits,
        "total": {
            "count": sum(int(value["count"]) for value in splits.values()),
            "duration_seconds": sum(
                float(value["duration_seconds"]) for value in splits.values()
            ),
        },
    }


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
        summary_by_split=_summary_by_split(segment_tuple),
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
        model_ready_duration = _model_ready_duration(segment)
        if model_ready_duration > _WHISPER_RECOMMENDED_MAX_DURATION_SECONDS:
            model_ready_audio_uri = _require_model_ready_audio_uri(segment)
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
                        "duration": model_ready_duration,
                        "recommended_max_duration_seconds": (
                            _WHISPER_RECOMMENDED_MAX_DURATION_SECONDS
                        ),
                        "audio_uri": model_ready_audio_uri,
                        "original_audio_uri": segment.original_audio_uri,
                        "example_id": segment.example_id,
                        "segment_id": segment.segment_id,
                    },
                )
            )

    return ModelWriterResult(
        rows_by_split=_freeze_rows(rows_by_split),
        config=None,
        warnings=tuple(warnings),
        summary_by_split=_summary_by_split(segment_tuple),
    )


def infer_audio_mime_type(audio_uri: str) -> str:
    normalized = _require_text(audio_uri, label="audio_uri").lower()
    if normalized.endswith(".flac"):
        return "audio/flac"
    if normalized.endswith(".mp3"):
        return "audio/mpeg"
    raise ModelWriterError(f"unsupported audio MIME type for uri={audio_uri}")


def build_gemini_tuning_config(
    *,
    training_dataset_uri: str,
    validation_dataset_uri: str | None = None,
    base_model: str = DEFAULT_GEMINI_BASE_MODEL,
    region: str = DEFAULT_GEMINI_REGION,
    adapter_size: str = "ONE",
    epoch_count: int = 5,
    learning_rate_multiplier: float = 1.0,
) -> dict[str, object]:
    normalized_adapter_size = _require_adapter_size(adapter_size)
    normalized_epoch_count = _require_int_range(
        epoch_count, label="epoch_count", minimum=1, maximum=100
    )
    normalized_learning_rate_multiplier = _require_float_range(
        learning_rate_multiplier,
        label="learning_rate_multiplier",
        minimum=0.001,
        maximum=10.0,
    )
    supervised_tuning_spec: dict[str, object] = {
        "trainingDatasetUri": _require_text(
            training_dataset_uri, label="training_dataset_uri"
        ),
        "hyperParameters": {
            "adapterSize": normalized_adapter_size,
            "epochCount": normalized_epoch_count,
            "learningRateMultiplier": normalized_learning_rate_multiplier,
        },
    }
    config: dict[str, object] = {
        "baseModel": _require_text(base_model, label="base_model"),
        "region": _require_text(region, label="region"),
        "supervisedTuningSpec": supervised_tuning_spec,
    }
    if validation_dataset_uri is not None:
        supervised_tuning_spec["validationDatasetUri"] = _require_text(
            validation_dataset_uri, label="validation_dataset_uri"
        )
    return config


def build_gemini_inputs(
    segments: tuple[LabeledSegment, ...],
    *,
    system_prompt: str,
    user_prompt: str,
    training_dataset_uri: str,
    validation_dataset_uri: str | None = None,
    base_model: str = DEFAULT_GEMINI_BASE_MODEL,
    region: str = DEFAULT_GEMINI_REGION,
    adapter_size: str = "ONE",
    epoch_count: int = 5,
    learning_rate_multiplier: float = 1.0,
) -> ModelWriterResult:
    segment_tuple = tuple(segments)
    validate_split_integrity(segment_tuple)
    rows_by_split = _empty_rows_by_split()

    for segment in segment_tuple:
        model_ready_audio_uri = _require_model_ready_audio_uri(segment)
        row = build_example(
            audio_uri=model_ready_audio_uri,
            gt_text=segment.text,
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            mime_type=_infer_audio_mime_type_for_uri(
                segment, model_ready_audio_uri
            ),
        )
        if not validate_example(row):
            raise ModelWriterError(
                f"row_index={segment.row_index} failed Gemini validation"
            )
        rows_by_split[_require_split(segment)].append(row)

    return ModelWriterResult(
        rows_by_split=_freeze_rows(rows_by_split),
        config=build_gemini_tuning_config(
            training_dataset_uri=training_dataset_uri,
            validation_dataset_uri=validation_dataset_uri,
            base_model=base_model,
            region=region,
            adapter_size=adapter_size,
            epoch_count=epoch_count,
            learning_rate_multiplier=learning_rate_multiplier,
        ),
        warnings=(),
        summary_by_split=_summary_by_split(segment_tuple),
    )


def _nemo_row(segment: LabeledSegment) -> dict[str, object]:
    model_ready_audio_uri = _require_model_ready_audio_uri(segment)
    return {
        "audio_filepath": model_ready_audio_uri,
        "text": segment.text,
        "duration": _model_ready_duration(segment),
        "offset": _offset_for_audio_uri(segment, model_ready_audio_uri),
        "example_id": segment.example_id,
        "segment_id": segment.segment_id,
    }


def _whisper_row(segment: LabeledSegment) -> dict[str, object]:
    model_ready_audio_uri = _require_model_ready_audio_uri(segment)
    return {
        "audio_uri": model_ready_audio_uri,
        "text": segment.text,
        "duration": _model_ready_duration(segment),
        "offset": _offset_for_audio_uri(segment, model_ready_audio_uri),
        "dataset_name": segment.dataset_name,
        "dataset_family": segment.dataset_family,
        "source_group": segment.source_group,
        "split": _require_split(segment),
        "original_audio_uri": segment.original_audio_uri,
        "example_id": segment.example_id,
        "segment_id": segment.segment_id,
        "preprocessing": dict(_WHISPER_PREPROCESSING),
    }


def _empty_rows_by_split() -> dict[str, list[dict[str, object]]]:
    return {split: [] for split in _SPLITS}


def _summary_by_split(
    segments: tuple[LabeledSegment, ...],
) -> dict[str, dict[str, float | int]]:
    summary: dict[str, dict[str, float | int]] = {
        split: {"count": 0, "duration_seconds": 0.0} for split in _SPLITS
    }
    for segment in segments:
        split = _require_split(segment)
        summary[split]["count"] = int(summary[split]["count"]) + 1
        summary[split]["duration_seconds"] = float(
            summary[split]["duration_seconds"]
        ) + _model_ready_duration(segment)
    return summary


def _count_duration(value: dict[str, float | int]) -> dict[str, object]:
    return {
        "count": int(value.get("count", 0)),
        "duration_seconds": float(value.get("duration_seconds", 0.0)),
    }


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


def _require_model_ready_audio_uri(segment: LabeledSegment) -> str:
    uri = (segment.model_ready_audio_uri or "").strip()
    if not uri.startswith("gs://"):
        raise ModelWriterError(
            f"row_index={segment.row_index} missing model_ready_audio_uri"
        )
    return uri


def _offset_for_audio_uri(segment: LabeledSegment, audio_uri: str) -> float:
    if audio_uri != segment.audio_uri:
        return 0.0
    return float(segment.offset)


def _model_ready_duration(segment: LabeledSegment) -> float:
    model_ready_audio_uri = _require_model_ready_audio_uri(segment)
    if model_ready_audio_uri != segment.audio_uri:
        metadata = segment.transformation_metadata
        if isinstance(metadata, dict) and "output_duration" in metadata:
            return _require_positive_duration(
                metadata["output_duration"],
                label="transformation_metadata.output_duration",
                row_index=segment.row_index,
            )
    return _require_positive_duration(
        segment.duration, label="duration", row_index=segment.row_index
    )


def _require_positive_duration(
    value: object, *, label: str, row_index: int
) -> float:
    try:
        duration = float(value)
    except (TypeError, ValueError) as exc:
        raise ModelWriterError(
            f"row_index={row_index} {label} must be a finite positive number"
        ) from exc
    if not math.isfinite(duration) or duration <= 0.0:
        raise ModelWriterError(
            f"row_index={row_index} {label} must be a finite positive number"
        )
    return duration


def _require_adapter_size(value: str) -> str:
    adapter_size = _require_text(value, label="adapter_size")
    if adapter_size not in _GEMINI_ADAPTER_SIZES:
        raise ModelWriterError(
            "adapter_size must be one of "
            f"{sorted(_GEMINI_ADAPTER_SIZES)}: {adapter_size}"
        )
    return adapter_size


def _require_int_range(
    value: Any, *, label: str, minimum: int, maximum: int
) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ModelWriterError(f"{label} must be an integer")
    if value < minimum or value > maximum:
        raise ModelWriterError(
            f"{label} must be between {minimum} and {maximum}: {value}"
        )
    return value


def _require_float_range(
    value: Any, *, label: str, minimum: float, maximum: float
) -> float:
    if isinstance(value, bool):
        raise ModelWriterError(f"{label} must be a finite number")
    try:
        normalized = float(value)
    except (TypeError, ValueError) as exc:
        raise ModelWriterError(f"{label} must be a finite number") from exc
    if not math.isfinite(normalized):
        raise ModelWriterError(f"{label} must be a finite number")
    if normalized < minimum or normalized > maximum:
        raise ModelWriterError(
            f"{label} must be between {minimum} and {maximum}: {normalized}"
        )
    return normalized


def _infer_audio_mime_type_for_uri(
    segment: LabeledSegment, audio_uri: str
) -> str:
    try:
        return infer_audio_mime_type(audio_uri)
    except ModelWriterError as exc:
        raise ModelWriterError(
            "row_index="
            f"{segment.row_index} unsupported model_ready_audio_uri={audio_uri}"
        ) from exc


def _serialize_jsonl(rows: tuple[dict[str, object], ...]) -> str:
    if not rows:
        return ""
    return (
        "\n".join(
            json.dumps(row, sort_keys=True, allow_nan=False) for row in rows
        )
        + "\n"
    )
