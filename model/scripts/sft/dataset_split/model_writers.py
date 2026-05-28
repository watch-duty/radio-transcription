from __future__ import annotations

import json
from dataclasses import dataclass

from common.sft import build_example, validate_example

from dataset_split.leakage import validate_split_integrity
from dataset_split.types import LabeledSegment

_SPLITS = ("train", "eval")
DEFAULT_GEMINI_BASE_MODEL = "gemini-3.1-flash-lite"
DEFAULT_GEMINI_REGION = "us-central1"
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
    config: dict[str, object] = {
        "trainingDatasetUri": _require_text(
            training_dataset_uri, label="training_dataset_uri"
        ),
        "baseModel": _require_text(base_model, label="base_model"),
        "region": _require_text(region, label="region"),
        "adapterSize": _require_text(adapter_size, label="adapter_size"),
        "epochCount": int(epoch_count),
        "learningRateMultiplier": float(learning_rate_multiplier),
    }
    if validation_dataset_uri is not None:
        config["validationDatasetUri"] = _require_text(
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
        row = build_example(
            audio_uri=segment.audio_uri,
            gt_text=segment.text,
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            mime_type=_infer_audio_mime_type_for_segment(segment),
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
        ) + float(segment.duration)
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


def _infer_audio_mime_type_for_segment(segment: LabeledSegment) -> str:
    try:
        return infer_audio_mime_type(segment.audio_uri)
    except ModelWriterError as exc:
        raise ModelWriterError(
            f"row_index={segment.row_index} unsupported audio_uri={segment.audio_uri}"
        ) from exc


def _serialize_jsonl(rows: tuple[dict[str, object], ...]) -> str:
    if not rows:
        return ""
    return "\n".join(json.dumps(row, sort_keys=True) for row in rows) + "\n"
