from __future__ import annotations

import argparse

from dataset_versioning.config import (
    ConfigValidationError,
    parse_dataset_version_config_toml,
)
from dataset_versioning.gcs_io import GcsInputError, GoogleCloudTextReader
from dataset_versioning.validate import (
    DatasetValidationError,
    validate_dataset_version,
)


def _make_text_reader() -> GoogleCloudTextReader:
    from google.cloud import storage

    return GoogleCloudTextReader(storage.Client())


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Validate an SFT dataset-version config and manifests."
    )
    parser.add_argument("--config-uri", required=True)
    args = parser.parse_args(argv)

    config_uri = args.config_uri
    if not config_uri.startswith("gs://"):
        print("config_uri must be a gs:// URI")
        return 1

    try:
        reader = _make_text_reader()
        config_text = reader.read_text(config_uri)
        config = parse_dataset_version_config_toml(
            config_text, source=config_uri
        )
        summaries = validate_dataset_version(config, reader=reader)
    except (ConfigValidationError, GcsInputError, DatasetValidationError) as exc:
        print(str(exc))
        return 1

    for summary in summaries:
        print(
            f"dataset={summary.dataset_name} "
            f"family={summary.family} "
            f"loaded={summary.loaded_rows} "
            f"valid={summary.valid_rows} "
            f"excluded_empty_text={summary.excluded_empty_text}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
