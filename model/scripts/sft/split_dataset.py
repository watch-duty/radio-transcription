from __future__ import annotations

import argparse
from dataclasses import asdict
from pathlib import Path

from dataset_split.artifacts import DatasetArtifactError
from dataset_split.audio import AudioDerivationError
from dataset_split.canonical import CanonicalManifestError
from dataset_split.config import (
    ConfigValidationError,
    DatasetVersionConfig,
    parse_dataset_version_config_toml,
)
from dataset_split.dry_run import build_dry_run_artifacts
from dataset_split.gcs_io import (
    GcsInputError,
    GoogleCloudTextReader,
    TextReader,
)
from dataset_split.leakage import SplitLeakageError, validate_split_integrity
from dataset_split.model_writers import ModelWriterError
from dataset_split.publisher import (
    DatasetPublicationError,
    DatasetPublicationResult,
    publish_dataset_version_artifacts,
)
from dataset_split.reports import DatasetVersionReportError
from dataset_split.split import (
    SplitAssignmentError,
    SplitResult,
    assign_train_eval_split,
)
from dataset_split.validate import (
    DatasetValidationError,
    DatasetValidationResult,
    load_and_validate_datasets,
)
from prompts import PIPELINE_SYSTEM_PROMPT, PIPELINE_USER_PROMPT


def _make_storage_client() -> object:
    from google.cloud import storage

    return storage.Client()


def _make_text_reader() -> GoogleCloudTextReader:
    return GoogleCloudTextReader(_make_storage_client())


def _load_config(config_uri: str, reader: TextReader) -> DatasetVersionConfig:
    config_text = reader.read_text(config_uri)
    return parse_dataset_version_config_toml(config_text, source=config_uri)


def _prepare_split(
    config: DatasetVersionConfig, reader: TextReader
) -> tuple[DatasetValidationResult, SplitResult, dict[str, object]]:
    validation_result = load_and_validate_datasets(config, reader=reader)
    split_result = assign_train_eval_split(
        validation_result.segments,
        train_ratio=config.train_ratio,
        eval_ratio=config.eval_ratio,
    )
    validate_split_integrity(split_result.segments)
    leakage_validation = {
        "passed": True,
        "checks": (
            "source_group",
            "original_audio_uri",
            "model_ready_audio_uri",
            "duplicate_audio_span",
        ),
    }
    return validation_result, split_result, leakage_validation


def _dry_run(args: argparse.Namespace) -> int:
    if not args.config_uri.startswith("gs://"):
        raise GcsInputError("config_uri must be a gs:// URI")

    reader = _make_text_reader()
    config = _load_config(args.config_uri, reader)
    validation_result, split_result, leakage_validation = _prepare_split(
        config, reader
    )
    dry_run_result = build_dry_run_artifacts(
        config=config,
        segments=split_result.segments,
        excluded_rows=validation_result.excluded,
        leakage_validation=leakage_validation,
        balance_report=split_result.balance_report or {},
        output_dir=Path(args.output_dir),
        system_prompt=PIPELINE_SYSTEM_PROMPT,
        user_prompt=PIPELINE_USER_PROMPT,
    )
    print(f"Dry run complete: {args.output_dir}")
    print(f"Report: {dry_run_result['report_path']}")
    return 0


def _generate(args: argparse.Namespace) -> int:
    if not args.config_uri.startswith("gs://"):
        raise GcsInputError("config_uri must be a gs:// URI")

    reader = _make_text_reader()
    storage_client = _make_storage_client()
    config = _load_config(args.config_uri, reader)
    validation_result, split_result, leakage_validation = _prepare_split(
        config, reader
    )
    result = publish_dataset_version_artifacts(
        storage_client,
        dataset_version_id=config.dataset_version_id,
        segments=split_result.segments,
        resolved_config=asdict(config),
        leakage_validation=leakage_validation,
        balance_report=split_result.balance_report or {},
        system_prompt=PIPELINE_SYSTEM_PROMPT,
        user_prompt=PIPELINE_USER_PROMPT,
        excluded_rows=validation_result.excluded,
        root_prefix=config.output_gcs_prefix,
    )
    _print_publication_result(result)
    return 0


def _print_publication_result(result: DatasetPublicationResult) -> None:
    print(f"Dataset generated: {result.root_uri}")
    for artifact in result.artifacts:
        if artifact.name.startswith("reports/") or artifact.name.startswith(
            "model_inputs/"
        ):
            print(f"{artifact.name}: {artifact.uri}")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Split configured SFT datasets into train/eval artifacts."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    dry_run = subparsers.add_parser(
        "dry-run",
        help="Write a local preview bundle without materializing audio.",
    )
    dry_run.add_argument("--config-uri", required=True)
    dry_run.add_argument("--output-dir", required=True)
    dry_run.set_defaults(handler=_dry_run)

    generate = subparsers.add_parser(
        "generate",
        help="Publish the dataset-version artifacts to GCS.",
    )
    generate.add_argument("--config-uri", required=True)
    generate.set_defaults(handler=_generate)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    try:
        return int(args.handler(args))
    except (
        ConfigValidationError,
        GcsInputError,
        DatasetValidationError,
        SplitAssignmentError,
        SplitLeakageError,
        DatasetPublicationError,
        DatasetArtifactError,
        AudioDerivationError,
        CanonicalManifestError,
        ModelWriterError,
        DatasetVersionReportError,
    ) as exc:
        print(str(exc))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
