from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
import tempfile

from dataset_split.artifacts import (
    DATASET_VERSION_ROOT,
    DatasetArtifactLayout,
    ensure_dataset_version_absent,
    upload_text_create_only,
)
from dataset_split.audio import (
    prepare_audio_for_publication,
    upload_prepared_audio,
)
from dataset_split.canonical import canonical_manifests, per_dataset_manifests
from dataset_split.model_writers import (
    DEFAULT_GEMINI_BASE_MODEL,
    DEFAULT_GEMINI_REGION,
    ModelWriterResult,
    build_gemini_inputs,
    build_nemo_inputs,
    build_whisper_inputs,
    summarize_model_writer_result,
)
from dataset_split.reports import (
    build_dataset_version_metadata,
    build_dataset_version_report,
    render_dataset_version_markdown,
    serialize_excluded_rows,
)
from dataset_split.types import ExcludedRow, LabeledSegment

_JSON = "application/json"
_JSONL = "application/x-ndjson"
_MARKDOWN = "text/markdown"
_FORBIDDEN_SFT_RUN_KEYS = frozenset(
    {
        "tuned_model_id",
        "endpoint",
        "training_metrics",
        "post_run_wer",
        "run_comparison",
    }
)


class DatasetPublicationError(ValueError):
    """Raised when dataset-version publication cannot be assembled."""


@dataclass(frozen=True)
class PublishedArtifact:
    name: str
    uri: str
    content_type: str


@dataclass(frozen=True)
class DatasetPublicationResult:
    dataset_version_id: str
    root_uri: str
    artifacts: tuple[PublishedArtifact, ...]
    artifact_inventory: dict[str, object]
    writer_warnings: dict[str, list[dict[str, object]]]
    audio_action_counts: dict[str, int]
    uploaded_audio_uris: tuple[str, ...]


@dataclass(frozen=True)
class _PlannedArtifact:
    name: str
    uri: str
    text: str
    content_type: str


def publish_dataset_version_artifacts(
    storage_client: object,
    *,
    dataset_version_id: str,
    segments: tuple[LabeledSegment, ...],
    resolved_config: dict[str, object],
    leakage_validation: dict[str, object],
    balance_report: dict[str, object],
    system_prompt: str,
    user_prompt: str,
    excluded_rows: tuple[ExcludedRow, ...] = (),
    root_prefix: str = DATASET_VERSION_ROOT,
    gemini_base_model: str = DEFAULT_GEMINI_BASE_MODEL,
    gemini_region: str = DEFAULT_GEMINI_REGION,
    gemini_adapter_size: str = "ONE",
    gemini_epoch_count: int = 5,
    gemini_learning_rate_multiplier: float = 1.0,
    scratch_dir: str | Path | None = None,
    audio_preparer=prepare_audio_for_publication,
    audio_uploader=upload_prepared_audio,
) -> DatasetPublicationResult:
    if scratch_dir is None:
        with tempfile.TemporaryDirectory(
            prefix="sft-audio-"
        ) as temporary_scratch:
            return publish_dataset_version_artifacts(
                storage_client,
                dataset_version_id=dataset_version_id,
                segments=segments,
                resolved_config=resolved_config,
                leakage_validation=leakage_validation,
                balance_report=balance_report,
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                excluded_rows=excluded_rows,
                root_prefix=root_prefix,
                gemini_base_model=gemini_base_model,
                gemini_region=gemini_region,
                gemini_adapter_size=gemini_adapter_size,
                gemini_epoch_count=gemini_epoch_count,
                gemini_learning_rate_multiplier=gemini_learning_rate_multiplier,
                scratch_dir=temporary_scratch,
                audio_preparer=audio_preparer,
                audio_uploader=audio_uploader,
            )

    segment_tuple = tuple(segments)
    _reject_sft_run_fields(resolved_config)
    layout = DatasetArtifactLayout.for_dataset_version(
        dataset_version_id, root_prefix=root_prefix
    )
    ensure_dataset_version_absent(storage_client, layout.root_uri)

    audio_result = audio_preparer(
        storage_client,
        layout=layout,
        segments=segment_tuple,
        scratch_dir=scratch_dir,
        upload=False,
    )
    enriched_segments = tuple(audio_result.segments)
    planned_uploaded_audio_uris = tuple(
        task.uri for task in audio_result.upload_tasks
    )

    canonical = canonical_manifests(enriched_segments)
    per_dataset = per_dataset_manifests(enriched_segments)
    nemo = build_nemo_inputs(
        enriched_segments,
        train_manifest_uri=layout.model_input_uri("nemo", "train"),
        eval_manifest_uri=layout.model_input_uri("nemo", "eval"),
    )
    whisper = build_whisper_inputs(enriched_segments)
    gemini = build_gemini_inputs(
        enriched_segments,
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
    writer_warnings = _combined_writer_warnings(nemo, whisper, gemini)
    artifact_inventory = _artifact_inventory(
        layout,
        per_dataset,
        uploaded_audio_uris=planned_uploaded_audio_uris,
    )
    metadata = build_dataset_version_metadata(
        layout.dataset_version_id,
        layout.root_uri,
        artifact_inventory,
    )
    report = build_dataset_version_report(
        layout.dataset_version_id,
        resolved_config,
        enriched_segments,
        leakage_validation,
        balance_report,
        artifact_inventory,
        model_writer_summary,
        writer_warnings,
        excluded_rows=excluded_rows,
        excluded_rows_artifact_uri=_excluded_rows_uri(layout),
        source_key_failures=0,
        audio_materialized=True,
    )
    planned = _planned_artifacts(
        layout=layout,
        canonical=canonical,
        per_dataset=per_dataset,
        nemo=nemo,
        whisper=whisper,
        gemini=gemini,
        resolved_config=resolved_config,
        metadata=metadata.to_dict(),
        report=report.to_dict(),
        markdown=render_dataset_version_markdown(report),
        excluded_rows_jsonl=serialize_excluded_rows(excluded_rows),
    )

    uploaded_audio_uris = tuple(audio_uploader(storage_client, audio_result))
    if uploaded_audio_uris != planned_uploaded_audio_uris:
        raise DatasetPublicationError(
            "audio uploader returned unexpected uploaded URIs"
        )

    for artifact in planned:
        upload_text_create_only(
            storage_client,
            artifact.uri,
            artifact.text,
            content_type=artifact.content_type,
        )

    return DatasetPublicationResult(
        dataset_version_id=layout.dataset_version_id,
        root_uri=layout.root_uri,
        artifacts=tuple(
            PublishedArtifact(
                name=artifact.name,
                uri=artifact.uri,
                content_type=artifact.content_type,
            )
            for artifact in planned
        ),
        artifact_inventory=artifact_inventory,
        writer_warnings=writer_warnings,
        audio_action_counts=dict(audio_result.action_counts),
        uploaded_audio_uris=uploaded_audio_uris,
    )


def _artifact_inventory(
    layout: DatasetArtifactLayout,
    per_dataset: Mapping[str, Mapping[str, str]],
    *,
    uploaded_audio_uris: Sequence[str],
) -> dict[str, object]:
    return {
        "root": layout.root_uri,
        "config": layout.config_uri,
        "metadata": layout.metadata_uri,
        "manifests": {
            "canonical": {
                "train": layout.canonical_train_uri,
                "eval": layout.canonical_eval_uri,
            },
            "per_dataset": {
                dataset_name: {
                    split: layout.per_dataset_manifest_uri(dataset_name, split)
                    for split in sorted(manifests)
                }
                for dataset_name, manifests in sorted(per_dataset.items())
            },
        },
        "model_inputs": {
            "nemo": {
                "train": layout.model_input_uri("nemo", "train"),
                "eval": layout.model_input_uri("nemo", "eval"),
                "config": layout.model_input_uri("nemo", "config", "json"),
            },
            "whisper": {
                "train": layout.model_input_uri("whisper", "train"),
                "eval": layout.model_input_uri("whisper", "eval"),
            },
            "gemini": {
                "train": layout.model_input_uri("gemini", "train"),
                "eval": layout.model_input_uri("gemini", "eval"),
                "tuning_config": layout.model_input_uri(
                    "gemini", "tuning_config", "json"
                ),
            },
        },
        "reports": {
            "json": layout.reports_json_uri,
            "markdown": layout.reports_markdown_uri,
            "excluded_rows": _excluded_rows_uri(layout),
        },
        "audio_prefix": layout.audio_prefix_uri,
        "audio_action_prefixes": {
            "copied": f"{layout.audio_prefix_uri}copied/",
            "derived": f"{layout.audio_prefix_uri}derived/",
            "transcoded": f"{layout.audio_prefix_uri}transcoded/",
        },
        "uploaded_audio_uris": tuple(uploaded_audio_uris),
    }


def _combined_writer_warnings(
    nemo: ModelWriterResult,
    whisper: ModelWriterResult,
    gemini: ModelWriterResult,
) -> dict[str, list[dict[str, object]]]:
    return {
        "nemo": nemo.warnings_by_writer().get("nemo", []),
        "whisper": whisper.warnings_by_writer().get("whisper", []),
        "gemini": gemini.warnings_by_writer().get("gemini", []),
    }


def _planned_artifacts(
    *,
    layout: DatasetArtifactLayout,
    canonical: Mapping[str, str],
    per_dataset: Mapping[str, Mapping[str, str]],
    nemo: ModelWriterResult,
    whisper: ModelWriterResult,
    gemini: ModelWriterResult,
    resolved_config: Mapping[str, object],
    metadata: Mapping[str, object],
    report: Mapping[str, object],
    markdown: str,
    excluded_rows_jsonl: str,
) -> tuple[_PlannedArtifact, ...]:
    planned = [
        _PlannedArtifact(
            "config/resolved_config",
            layout.config_uri,
            _json_dump(resolved_config),
            _JSON,
        ),
        _PlannedArtifact(
            "metadata/dataset_version",
            layout.metadata_uri,
            _json_dump(metadata),
            _JSON,
        ),
        _PlannedArtifact(
            "manifests/canonical/train",
            layout.canonical_train_uri,
            canonical["train"],
            _JSONL,
        ),
        _PlannedArtifact(
            "manifests/canonical/eval",
            layout.canonical_eval_uri,
            canonical["eval"],
            _JSONL,
        ),
    ]
    for dataset_name, manifests in sorted(per_dataset.items()):
        for split in ("train", "eval"):
            planned.append(
                _PlannedArtifact(
                    f"manifests/per_dataset/{dataset_name}/{split}",
                    layout.per_dataset_manifest_uri(dataset_name, split),
                    manifests[split],
                    _JSONL,
                )
            )

    nemo_jsonl = nemo.jsonl_by_split()
    whisper_jsonl = whisper.jsonl_by_split()
    gemini_jsonl = gemini.jsonl_by_split()
    planned.extend(
        [
            _PlannedArtifact(
                "model_inputs/nemo/train",
                layout.model_input_uri("nemo", "train"),
                nemo_jsonl["train"],
                _JSONL,
            ),
            _PlannedArtifact(
                "model_inputs/nemo/eval",
                layout.model_input_uri("nemo", "eval"),
                nemo_jsonl["eval"],
                _JSONL,
            ),
            _PlannedArtifact(
                "model_inputs/nemo/config",
                layout.model_input_uri("nemo", "config", "json"),
                _json_dump(nemo.config),
                _JSON,
            ),
            _PlannedArtifact(
                "model_inputs/whisper/train",
                layout.model_input_uri("whisper", "train"),
                whisper_jsonl["train"],
                _JSONL,
            ),
            _PlannedArtifact(
                "model_inputs/whisper/eval",
                layout.model_input_uri("whisper", "eval"),
                whisper_jsonl["eval"],
                _JSONL,
            ),
            _PlannedArtifact(
                "model_inputs/gemini/train",
                layout.model_input_uri("gemini", "train"),
                gemini_jsonl["train"],
                _JSONL,
            ),
            _PlannedArtifact(
                "model_inputs/gemini/eval",
                layout.model_input_uri("gemini", "eval"),
                gemini_jsonl["eval"],
                _JSONL,
            ),
            _PlannedArtifact(
                "model_inputs/gemini/tuning_config",
                layout.model_input_uri("gemini", "tuning_config", "json"),
                _json_dump(gemini.config),
                _JSON,
            ),
            _PlannedArtifact(
                "reports/dataset_version_report",
                layout.reports_json_uri,
                _json_dump(report),
                _JSON,
            ),
            _PlannedArtifact(
                "reports/dataset_version_report_markdown",
                layout.reports_markdown_uri,
                markdown,
                _MARKDOWN,
            ),
            _PlannedArtifact(
                "reports/excluded_rows",
                _excluded_rows_uri(layout),
                excluded_rows_jsonl,
                _JSONL,
            ),
        ]
    )
    return tuple(planned)


def _excluded_rows_uri(layout: DatasetArtifactLayout) -> str:
    return f"{layout.root_uri}reports/excluded_rows.jsonl"


def _json_dump(value: object) -> str:
    return json.dumps(value, indent=2, sort_keys=True, allow_nan=False) + "\n"


def _reject_sft_run_fields(
    value: object, *, path: str = "resolved_config"
) -> None:
    if isinstance(value, Mapping):
        for key, nested in value.items():
            child_path = f"{path}.{key}"
            if str(key) in _FORBIDDEN_SFT_RUN_KEYS:
                raise DatasetPublicationError(
                    f"{child_path} belongs to an SFT run, not a dataset version"
                )
            _reject_sft_run_fields(nested, path=child_path)
    elif isinstance(value, Sequence) and not isinstance(
        value, (str, bytes, bytearray)
    ):
        for index, nested in enumerate(value):
            _reject_sft_run_fields(nested, path=f"{path}.{index}")
