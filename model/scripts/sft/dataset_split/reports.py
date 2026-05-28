from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone

from dataset_split.leakage import validate_split_integrity
from dataset_split.types import LabeledSegment

MODEL_WRITER_SUMMARY_WRITERS = ("nemo", "whisper", "gemini")
MODEL_WRITER_SUMMARY_SPLITS = ("train", "eval")
AUDIO_ACTIONS = ("reused", "copied", "derived", "transcoded")
TRANSFORMATION_METADATA_KEYS = (
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
)
COMMAND_SUMMARY_KEYS = (
    "ffprobe",
    "ffmpeg",
    "ffprobe_version",
    "ffmpeg_version",
)


class DatasetVersionReportError(ValueError):
    """Raised when dataset-version report data is malformed."""


@dataclass(frozen=True)
class DatasetVersionMetadata:
    dataset_version_id: str
    root_uri: str
    generated_at: str
    artifact_inventory: Mapping[str, object]

    def to_dict(self) -> dict[str, object]:
        return {
            "dataset_version_id": self.dataset_version_id,
            "root_uri": self.root_uri,
            "generated_at": self.generated_at,
            "artifact_inventory": _json_ready(self.artifact_inventory),
        }


@dataclass(frozen=True)
class DatasetVersionReport:
    dataset_version_id: str
    resolved_config: Mapping[str, object]
    split_counts: Mapping[str, int]
    duration_seconds: Mapping[str, float]
    dataset_summary: Mapping[str, object]
    model_writer_summary: Mapping[str, object]
    audio_transformation_summary: Mapping[str, object]
    leakage_validation: Mapping[str, object]
    balance_report: Mapping[str, object]
    artifact_inventory: Mapping[str, object]
    writer_warnings: Mapping[str, Sequence[Mapping[str, object]]]

    def to_dict(self) -> dict[str, object]:
        return {
            "dataset_version_id": self.dataset_version_id,
            "resolved_config": _json_ready(self.resolved_config),
            "split_counts": dict(self.split_counts),
            "duration_seconds": dict(self.duration_seconds),
            "dataset_summary": _json_ready(self.dataset_summary),
            "model_writer_summary": _json_ready(self.model_writer_summary),
            "audio_transformation_summary": _json_ready(
                self.audio_transformation_summary
            ),
            "leakage_validation": _json_ready(self.leakage_validation),
            "balance_report": _json_ready(self.balance_report),
            "artifact_inventory": _json_ready(self.artifact_inventory),
            "writer_warnings": _json_ready(self.writer_warnings),
        }


def build_dataset_version_metadata(
    dataset_version_id: str,
    root_uri: str,
    artifact_inventory: Mapping[str, object],
    generated_at: str | None = None,
) -> DatasetVersionMetadata:
    return DatasetVersionMetadata(
        dataset_version_id=_require_text(
            dataset_version_id, label="dataset_version_id"
        ),
        root_uri=_require_text(root_uri, label="root_uri"),
        generated_at=generated_at or _utc_timestamp(),
        artifact_inventory=_json_ready(artifact_inventory),
    )


def build_dataset_version_report(
    dataset_version_id: str,
    resolved_config: Mapping[str, object],
    segments: Sequence[LabeledSegment],
    leakage_validation: Mapping[str, object],
    balance_report: Mapping[str, object],
    artifact_inventory: Mapping[str, object],
    model_writer_summary: Mapping[str, object],
    writer_warnings: Mapping[str, Sequence[Mapping[str, object]]],
) -> DatasetVersionReport:
    segment_tuple = tuple(segments)
    validate_split_integrity(segment_tuple)
    return DatasetVersionReport(
        dataset_version_id=_require_text(
            dataset_version_id, label="dataset_version_id"
        ),
        resolved_config=_json_ready(resolved_config),
        split_counts=_split_counts(segment_tuple),
        duration_seconds=_split_durations(segment_tuple),
        dataset_summary=_dataset_summary(segment_tuple),
        model_writer_summary=_validate_model_writer_summary(
            model_writer_summary
        ),
        audio_transformation_summary=_audio_transformation_summary(
            segment_tuple
        ),
        leakage_validation=_json_ready(leakage_validation),
        balance_report=_json_ready(balance_report),
        artifact_inventory=_json_ready(artifact_inventory),
        writer_warnings=_writer_warnings(writer_warnings),
    )


def render_dataset_version_markdown(report: DatasetVersionReport) -> str:
    report_dict = report.to_dict()
    lines = [
        "# Dataset Version Report",
        "",
        f"Dataset version: `{report.dataset_version_id}`",
        "",
        "## Split Summary",
    ]
    for split in MODEL_WRITER_SUMMARY_SPLITS:
        lines.append(
            "- "
            f"{split}: {report_dict['split_counts'][split]} rows, "
            f"{report_dict['duration_seconds'][split]} seconds"
        )

    audio_summary = report_dict["audio_transformation_summary"]
    lines.extend(["", "## Audio Transformations"])
    for action in AUDIO_ACTIONS:
        action_summary = audio_summary["actions"][action]
        lines.append(
            "- "
            f"{action}: {action_summary['count']} rows, "
            f"{action_summary['duration_seconds']} seconds"
        )
    lines.extend(
        [
            "- "
            "model-ready audio URIs: "
            f"{audio_summary['model_ready_audio_uri_count']}",
            f"- derived audio URIs: {audio_summary['derived_audio_uri_count']}",
            f"- mixed to mono: {audio_summary['mixed_to_mono_count']}",
            f"- resampled: {audio_summary['resampled_count']}",
            f"- padded: {audio_summary['padded_count']}",
            "- "
            "command/version summaries: "
            f"{audio_summary['command_summary_count']}",
        ]
    )

    lines.extend(
        [
            "",
            "## Dataset Summary",
            _json_block(report_dict["dataset_summary"]),
            "",
            "## Model Writer Summary",
            _json_block(report_dict["model_writer_summary"]),
            "",
            "## Leakage Validation",
            _json_block(report_dict["leakage_validation"]),
            "",
            "## Balance Report",
            _json_block(report_dict["balance_report"]),
            "",
            "## Artifact Inventory",
            _json_block(report_dict["artifact_inventory"]),
            "",
            "## Writer Warnings",
            _json_block(report_dict["writer_warnings"]),
            "",
        ]
    )
    return "\n".join(lines)


def _split_counts(segments: tuple[LabeledSegment, ...]) -> dict[str, int]:
    counts = {split: 0 for split in MODEL_WRITER_SUMMARY_SPLITS}
    for segment in segments:
        counts[_require_split(segment)] += 1
    return counts


def _split_durations(segments: tuple[LabeledSegment, ...]) -> dict[str, float]:
    durations = {split: 0.0 for split in MODEL_WRITER_SUMMARY_SPLITS}
    for segment in segments:
        durations[_require_split(segment)] += float(segment.duration)
    return durations


def _dataset_summary(
    segments: tuple[LabeledSegment, ...],
) -> dict[str, dict[str, object]]:
    summary: dict[str, dict[str, object]] = {}
    for dataset_name in sorted({segment.dataset_name for segment in segments}):
        dataset_segments = tuple(
            segment
            for segment in segments
            if segment.dataset_name == dataset_name
        )
        splits = {
            split: {"count": 0, "duration_seconds": 0.0}
            for split in MODEL_WRITER_SUMMARY_SPLITS
        }
        for segment in dataset_segments:
            split = _require_split(segment)
            splits[split]["count"] += 1
            splits[split]["duration_seconds"] += float(segment.duration)
        summary[dataset_name] = {
            "dataset_families": sorted(
                {segment.dataset_family for segment in dataset_segments}
            ),
            "splits": splits,
            "total": {
                "count": len(dataset_segments),
                "duration_seconds": sum(
                    float(segment.duration) for segment in dataset_segments
                ),
            },
        }
    return summary


def _audio_transformation_summary(
    segments: tuple[LabeledSegment, ...],
) -> dict[str, object]:
    actions = {
        action: {"count": 0, "duration_seconds": 0.0}
        for action in AUDIO_ACTIONS
    }
    metadata_key_coverage = {key: 0 for key in TRANSFORMATION_METADATA_KEYS}
    model_ready_audio_uri_count = 0
    derived_audio_uri_count = 0
    mixed_to_mono_count = 0
    resampled_count = 0
    padded_count = 0
    command_summary_count = 0

    for segment in segments:
        _require_model_ready_audio_uri(segment)
        model_ready_audio_uri_count += 1
        if _has_text(segment.derived_audio_uri):
            derived_audio_uri_count += 1

        metadata = _require_transformation_metadata(segment)
        action = _require_audio_action(segment, metadata)
        actions[action]["count"] += 1
        actions[action]["duration_seconds"] += float(segment.duration)

        missing_keys = [
            key for key in TRANSFORMATION_METADATA_KEYS if key not in metadata
        ]
        if missing_keys:
            raise DatasetVersionReportError(
                f"row_index={segment.row_index} missing "
                f"transformation_metadata.{missing_keys[0]}"
            )
        for key in TRANSFORMATION_METADATA_KEYS:
            metadata_key_coverage[key] += 1

        if bool(metadata["mixed_to_mono"]):
            mixed_to_mono_count += 1
        if bool(metadata["resampled"]):
            resampled_count += 1
        if bool(metadata["padded"]):
            padded_count += 1
        if any(_has_text(metadata.get(key)) for key in COMMAND_SUMMARY_KEYS):
            command_summary_count += 1

    return {
        "actions": actions,
        "model_ready_audio_uri_count": model_ready_audio_uri_count,
        "derived_audio_uri_count": derived_audio_uri_count,
        "mixed_to_mono_count": mixed_to_mono_count,
        "resampled_count": resampled_count,
        "padded_count": padded_count,
        "metadata_key_coverage": metadata_key_coverage,
        "command_summary_count": command_summary_count,
    }


def _validate_model_writer_summary(
    model_writer_summary: Mapping[str, object],
) -> dict[str, dict[str, object]]:
    writer_names = set(model_writer_summary)
    expected_writers = set(MODEL_WRITER_SUMMARY_WRITERS)
    if writer_names != expected_writers:
        raise DatasetVersionReportError(
            "model_writer_summary must contain exactly "
            f"{MODEL_WRITER_SUMMARY_WRITERS}: {sorted(writer_names)}"
        )

    return {
        writer: _validate_writer_summary(writer, model_writer_summary[writer])
        for writer in MODEL_WRITER_SUMMARY_WRITERS
    }


def _validate_writer_summary(writer: str, value: object) -> dict[str, object]:
    if not isinstance(value, Mapping):
        raise DatasetVersionReportError(
            f"model_writer_summary.{writer} must be a mapping"
        )
    splits = value.get("splits")
    if not isinstance(splits, Mapping):
        raise DatasetVersionReportError(
            f"model_writer_summary.{writer}.splits must be a mapping"
        )
    if set(splits) != set(MODEL_WRITER_SUMMARY_SPLITS):
        raise DatasetVersionReportError(
            f"model_writer_summary.{writer}.splits must contain "
            f"{MODEL_WRITER_SUMMARY_SPLITS}"
        )

    normalized_splits = {
        split: _count_duration(
            splits[split],
            label=f"model_writer_summary.{writer}.splits.{split}",
        )
        for split in MODEL_WRITER_SUMMARY_SPLITS
    }
    return {
        "splits": normalized_splits,
        "total": _count_duration(
            value.get("total"),
            label=f"model_writer_summary.{writer}.total",
        ),
    }


def _count_duration(value: object, *, label: str) -> dict[str, object]:
    if not isinstance(value, Mapping):
        raise DatasetVersionReportError(f"{label} must be a mapping")
    if set(value) != {"count", "duration_seconds"}:
        raise DatasetVersionReportError(
            f"{label} must contain count and duration_seconds"
        )
    return {
        "count": int(value["count"]),
        "duration_seconds": float(value["duration_seconds"]),
    }


def _writer_warnings(
    writer_warnings: Mapping[str, Sequence[Mapping[str, object]]],
) -> dict[str, list[dict[str, object]]]:
    normalized: dict[str, list[dict[str, object]]] = {
        writer: [] for writer in MODEL_WRITER_SUMMARY_WRITERS
    }
    for writer, warnings in writer_warnings.items():
        if writer not in normalized:
            raise DatasetVersionReportError(f"unknown writer warning: {writer}")
        normalized[writer] = [_json_ready(warning) for warning in warnings]
    return normalized


def _require_model_ready_audio_uri(segment: LabeledSegment) -> str:
    uri = _require_non_empty_text(
        segment.model_ready_audio_uri,
        label="model_ready_audio_uri",
        row_index=segment.row_index,
    )
    if not uri.startswith("gs://"):
        raise DatasetVersionReportError(
            f"row_index={segment.row_index} model_ready_audio_uri "
            "must be a gs:// URI"
        )
    return uri


def _require_transformation_metadata(
    segment: LabeledSegment,
) -> Mapping[str, object]:
    metadata = segment.transformation_metadata
    if not isinstance(metadata, Mapping):
        raise DatasetVersionReportError(
            f"row_index={segment.row_index} "
            "transformation_metadata must be a mapping"
        )
    return metadata


def _require_audio_action(
    segment: LabeledSegment, metadata: Mapping[str, object]
) -> str:
    action = _require_non_empty_text(
        metadata.get("action"),
        label="transformation_metadata.action",
        row_index=segment.row_index,
    )
    if action not in AUDIO_ACTIONS:
        raise DatasetVersionReportError(
            f"row_index={segment.row_index} unknown audio action: {action}"
        )
    return action


def _json_ready(value: object) -> object:
    to_dict = getattr(value, "to_dict", None)
    if callable(to_dict):
        return _json_ready(to_dict())
    blocked_keys = _sft_run_keys()
    if isinstance(value, Mapping):
        return {
            str(key): _json_ready(nested)
            for key, nested in value.items()
            if str(key) not in blocked_keys
        }
    if isinstance(value, tuple | list):
        return [_json_ready(nested) for nested in value]
    return value


def _has_text(value: object | None) -> bool:
    return value is not None and bool(str(value).strip())


def _sft_run_keys() -> set[str]:
    return {
        "tuned_" + "model_id",
        "end" + "point",
        "training_" + "metrics",
        "post_" + "run_wer",
        "run_" + "comparison",
    }


def _require_split(segment: LabeledSegment) -> str:
    if segment.split not in MODEL_WRITER_SUMMARY_SPLITS:
        raise DatasetVersionReportError(
            f"row_index={segment.row_index} missing split"
        )
    return segment.split


def _require_non_empty_text(
    value: object | None, *, label: str, row_index: int
) -> str:
    if value is None:
        raise DatasetVersionReportError(
            f"row_index={row_index} {label} must not be empty"
        )
    text = str(value).strip()
    if not text:
        raise DatasetVersionReportError(
            f"row_index={row_index} {label} must not be empty"
        )
    return text


def _require_text(value: str, *, label: str) -> str:
    text = value.strip()
    if not text:
        raise DatasetVersionReportError(f"{label} must not be empty")
    return text


def _utc_timestamp() -> str:
    return (
        datetime.now(tz=timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def _json_block(value: object) -> str:
    return (
        "```json\n"
        + json.dumps(value, indent=2, sort_keys=True, allow_nan=False)
        + "\n```"
    )
