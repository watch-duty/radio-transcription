from __future__ import annotations

import json
import urllib.parse
from dataclasses import asdict, replace
from pathlib import Path
from typing import Any

from dataset_split.artifacts import (
    DatasetArtifactError,
    DatasetArtifactLayout,
    audio_object_uri,
)
from dataset_split.canonical import canonical_manifests, per_dataset_manifests
from dataset_split.config import DatasetVersionConfig
from dataset_split.model_writers import (
    DEFAULT_GEMINI_BASE_MODEL,
    DEFAULT_GEMINI_REGION,
    build_gemini_inputs,
    build_nemo_inputs,
    build_whisper_inputs,
    summarize_model_writer_result,
)
from dataset_split.publisher import (
    _artifact_inventory,
    _excluded_rows_uri,
    _planned_artifacts,
)
from dataset_split.reports import (
    build_dataset_version_metadata,
    build_dataset_version_report,
    render_dataset_version_markdown,
    serialize_excluded_rows,
)
from dataset_split.types import ExcludedRow, LabeledSegment

_SUPPORTED_PASSTHROUGH_SUFFIXES = frozenset({"flac", "mp3"})


def build_dry_run_artifacts(
    *,
    config: DatasetVersionConfig,
    segments: tuple[LabeledSegment, ...],
    excluded_rows: tuple[ExcludedRow, ...],
    leakage_validation: dict[str, object],
    balance_report: dict[str, object],
    output_dir: Path,
    system_prompt: str,
    user_prompt: str,
    gemini_base_model: str = DEFAULT_GEMINI_BASE_MODEL,
    gemini_region: str = DEFAULT_GEMINI_REGION,
    gemini_adapter_size: str = "ONE",
    gemini_epoch_count: int = 5,
    gemini_learning_rate_multiplier: float = 1.0,
) -> dict[str, object]:
    if output_dir.exists():
        raise DatasetArtifactError(f"output_dir already exists: {output_dir}")

    layout = DatasetArtifactLayout.for_dataset_version(
        config.dataset_version_id,
        root_prefix=config.output_gcs_prefix,
    )
    preview_segments = tuple(
        _with_preview_audio(segment, layout=layout) for segment in segments
    )

    canonical = canonical_manifests(preview_segments)
    per_dataset = per_dataset_manifests(preview_segments)
    nemo = build_nemo_inputs(
        preview_segments,
        train_manifest_uri=layout.model_input_uri("nemo", "train"),
        eval_manifest_uri=layout.model_input_uri("nemo", "eval"),
    )
    whisper = build_whisper_inputs(preview_segments)
    gemini = build_gemini_inputs(
        preview_segments,
        system_prompt=system_prompt,
        user_prompt=user_prompt,
        training_dataset_uri=layout.model_input_uri("gemini", "train"),
        validation_dataset_uri=layout.model_input_uri("gemini", "eval"),
        base_model=gemini_base_model,
        region=gemini_region,
        adapter_size=gemini_adapter_size,
        epoch_count=gemini_epoch_count,
        learning_rate_multiplier=gemini_learning_rate_multiplier,
    )

    model_writer_summary = {
        "nemo": summarize_model_writer_result(nemo),
        "whisper": summarize_model_writer_result(whisper),
        "gemini": summarize_model_writer_result(gemini),
    }
    writer_warnings = {
        "nemo": nemo.warnings_by_writer().get("nemo", []),
        "whisper": whisper.warnings_by_writer().get("whisper", []),
        "gemini": gemini.warnings_by_writer().get("gemini", []),
    }
    artifact_inventory = _artifact_inventory(
        layout,
        per_dataset,
        uploaded_audio_uris=(),
    )
    metadata = build_dataset_version_metadata(
        layout.dataset_version_id,
        layout.root_uri,
        artifact_inventory,
    )
    report = build_dataset_version_report(
        layout.dataset_version_id,
        asdict(config),
        preview_segments,
        leakage_validation,
        balance_report,
        artifact_inventory,
        model_writer_summary,
        writer_warnings,
        audio_materialized=False,
        excluded_rows=excluded_rows,
        excluded_rows_artifact_uri=_excluded_rows_uri(layout),
        source_key_failures=0,
    )
    planned_artifacts = _planned_artifacts(
        layout=layout,
        canonical=canonical,
        per_dataset=per_dataset,
        nemo=nemo,
        whisper=whisper,
        gemini=gemini,
        resolved_config=asdict(config),
        metadata=metadata.to_dict(),
        report=report.to_dict(),
        markdown=render_dataset_version_markdown(report),
        excluded_rows_jsonl=serialize_excluded_rows(excluded_rows),
    )

    output_dir.mkdir(parents=True)
    local_artifacts: dict[str, str] = {}
    for artifact in planned_artifacts:
        relative_path = _relative_artifact_path(layout.root_uri, artifact.uri)
        destination = output_dir / relative_path
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_text(artifact.text)
        local_artifacts[artifact.name] = str(destination)

    return {
        "dataset_version_id": layout.dataset_version_id,
        "root_uri": layout.root_uri,
        "output_dir": str(output_dir),
        "report_path": str(output_dir / "reports/dataset_version_report.json"),
        "audio_materialized": False,
        "segments": preview_segments,
        "excluded_rows": excluded_rows,
        "artifact_inventory": artifact_inventory,
        "local_artifacts": local_artifacts,
    }


def _with_preview_audio(
    segment: LabeledSegment, *, layout: DatasetArtifactLayout
) -> LabeledSegment:
    if float(segment.duration) <= 0.0:
        raise DatasetArtifactError(
            f"dataset={segment.dataset_name} row_index={segment.row_index} "
            "duration must be greater than 0"
        )

    action = _preview_action(segment)
    suffix = _source_suffix(segment.audio_uri)
    if action == "reused":
        model_ready_audio_uri = segment.audio_uri
        derived_audio_uri = None
        output_suffix = suffix
    elif action == "copied":
        output_suffix = suffix
        model_ready_audio_uri = audio_object_uri(
            layout, action=action, segment=segment, suffix=output_suffix
        )
        derived_audio_uri = None
    else:
        output_suffix = "flac"
        model_ready_audio_uri = audio_object_uri(
            layout, action=action, segment=segment, suffix=output_suffix
        )
        derived_audio_uri = model_ready_audio_uri if action == "derived" else None

    return replace(
        segment,
        model_ready_audio_uri=model_ready_audio_uri,
        derived_audio_uri=derived_audio_uri,
        transformation_metadata={
            "action": action,
            "original_audio_uri": segment.original_audio_uri,
            "source_audio_uri": segment.audio_uri,
            "offset": segment.offset,
            "duration": segment.duration,
            "source_duration": segment.duration,
            "output_duration": segment.duration,
            "source_format": suffix,
            "output_format": output_suffix,
            "source_channels": 1,
            "output_channels": 1,
            "mixed_to_mono": False,
            "resampled": False,
            "padded": False,
            "split": segment.split,
            "source_group": segment.source_group,
            "audio_materialized": False,
        },
    )


def _preview_action(segment: LabeledSegment) -> str:
    suffix = _source_suffix(segment.audio_uri)
    if (
        segment.audio_uri.startswith("gs://")
        and suffix in _SUPPORTED_PASSTHROUGH_SUFFIXES
        and float(segment.offset) == 0.0
    ):
        return "reused"
    if float(segment.offset) > 0.0:
        return "derived"
    if suffix in _SUPPORTED_PASSTHROUGH_SUFFIXES:
        return "copied"
    return "transcoded"


def _source_suffix(uri: str) -> str:
    path = urllib.parse.urlparse(uri).path or uri
    suffix = Path(path).suffix.lower().lstrip(".")
    if suffix:
        return suffix
    return "flac"


def _relative_artifact_path(root_uri: str, artifact_uri: str) -> Path:
    normalized_root = root_uri.rstrip("/") + "/"
    if not artifact_uri.startswith(normalized_root):
        raise DatasetArtifactError(
            f"artifact_uri is outside dataset root: {artifact_uri}"
        )
    return Path(artifact_uri[len(normalized_root) :])


def _json_dump(value: Any) -> str:
    return json.dumps(value, indent=2, sort_keys=True, allow_nan=False) + "\n"
