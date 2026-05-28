from __future__ import annotations

import sys
import unittest
from pathlib import Path
from typing import Any

_SFT_DIR = str(Path(__file__).resolve().parent.parent)
_COLABS_DIR = str(
    Path(__file__).resolve().parent.parent.parent.parent / "colabs"
)
if _SFT_DIR not in sys.path:
    sys.path.insert(0, _SFT_DIR)
if _COLABS_DIR not in sys.path:
    sys.path.insert(0, _COLABS_DIR)

from dataset_split.artifacts import (  # noqa: E402
    DatasetArtifactLayout,
    DatasetVersionExistsError,
    ensure_dataset_version_absent,
    upload_text_create_only,
)


class FakeListedBlob:
    def __init__(self, name: str) -> None:
        self.name = name


class FakePreconditionFailure(Exception):
    pass


class FakeBlob:
    _missing = object()

    def __init__(self, *, upload_error: Exception | None = None) -> None:
        self.upload_error = upload_error
        self.uploads: list[dict[str, Any]] = []

    def upload_from_string(
        self,
        text: str,
        *,
        content_type: str,
        if_generation_match: int | object = _missing,
    ) -> None:
        if if_generation_match is self._missing:
            raise AssertionError("if_generation_match=0 is required")
        self.uploads.append(
            {
                "text": text,
                "content_type": content_type,
                "if_generation_match": if_generation_match,
            }
        )
        if self.upload_error is not None:
            raise self.upload_error


class FakeBucket:
    def __init__(self, *, upload_error: Exception | None = None) -> None:
        self.upload_error = upload_error
        self.blobs: dict[str, FakeBlob] = {}

    def blob(self, path: str) -> FakeBlob:
        blob = FakeBlob(upload_error=self.upload_error)
        self.blobs[path] = blob
        return blob


class FakeStorageClient:
    def __init__(
        self,
        listed_blobs: list[FakeListedBlob] | None = None,
        *,
        upload_error: Exception | None = None,
    ) -> None:
        self.listed_blobs = listed_blobs or []
        self.list_calls: list[dict[str, Any]] = []
        self.fake_bucket = FakeBucket(upload_error=upload_error)

    def list_blobs(
        self, bucket_name: str, *, prefix: str, max_results: int
    ) -> list[FakeListedBlob]:
        self.list_calls.append(
            {
                "bucket_name": bucket_name,
                "prefix": prefix,
                "max_results": max_results,
            }
        )
        return self.listed_blobs

    def bucket(self, bucket_name: str) -> FakeBucket:
        self.bucket_name = bucket_name
        return self.fake_bucket


def _inventory_strings(value: object) -> list[str]:
    if isinstance(value, str):
        return [value]
    if isinstance(value, dict):
        strings: list[str] = []
        for nested in value.values():
            strings.extend(_inventory_strings(nested))
        return strings
    if isinstance(value, (list, tuple)):
        strings = []
        for nested in value:
            strings.extend(_inventory_strings(nested))
        return strings
    return []


class TestDatasetArtifacts(unittest.TestCase):
    def test_layout_uses_dataset_version_root(self) -> None:
        layout = DatasetArtifactLayout.for_dataset_version("dv-001")

        self.assertEqual(
            layout.root_uri, "gs://wd-transcription-data/sft/dv-001/"
        )
        self.assertEqual(
            layout.config_uri,
            "gs://wd-transcription-data/sft/dv-001/config/resolved_config.json",
        )
        self.assertEqual(
            layout.metadata_uri,
            "gs://wd-transcription-data/sft/dv-001/metadata/dataset_version.json",
        )
        self.assertEqual(
            layout.canonical_train_uri,
            "gs://wd-transcription-data/sft/dv-001/manifests/canonical/train.jsonl",
        )
        self.assertEqual(
            layout.canonical_eval_uri,
            "gs://wd-transcription-data/sft/dv-001/manifests/canonical/eval.jsonl",
        )
        self.assertEqual(
            layout.reports_json_uri,
            "gs://wd-transcription-data/sft/dv-001/reports/dataset_version_report.json",
        )
        self.assertEqual(
            layout.reports_markdown_uri,
            "gs://wd-transcription-data/sft/dv-001/reports/dataset_version_report.md",
        )
        self.assertEqual(
            layout.audio_prefix_uri,
            "gs://wd-transcription-data/sft/dv-001/audio/",
        )
        self.assertEqual(
            layout.per_dataset_manifest_uri("bcfy_calls", "train"),
            "gs://wd-transcription-data/sft/dv-001/manifests/per_dataset/bcfy_calls/train.jsonl",
        )
        self.assertEqual(
            layout.model_input_uri("nemo", "eval"),
            "gs://wd-transcription-data/sft/dv-001/model_inputs/nemo/eval.jsonl",
        )

    def test_existing_prefix_fails(self) -> None:
        client = FakeStorageClient(
            [FakeListedBlob("sft/dv-001/config/resolved_config.json")]
        )

        with self.assertRaisesRegex(
            DatasetVersionExistsError, "gs://wd-transcription-data/sft/dv-001/"
        ):
            ensure_dataset_version_absent(
                client, "gs://wd-transcription-data/sft/dv-001/"
            )

        self.assertEqual(
            client.list_calls,
            [
                {
                    "bucket_name": "wd-transcription-data",
                    "prefix": "sft/dv-001/",
                    "max_results": 1,
                }
            ],
        )

    def test_create_only_precondition_failure_fails(self) -> None:
        client = FakeStorageClient(upload_error=FakePreconditionFailure("412"))

        with self.assertRaisesRegex(
            DatasetVersionExistsError,
            "gs://wd-transcription-data/sft/dv-001/config/resolved_config.json",
        ):
            upload_text_create_only(
                client,
                "gs://wd-transcription-data/sft/dv-001/config/resolved_config.json",
                "{}",
                content_type="application/json",
            )

        blob = client.fake_bucket.blobs[
            "sft/dv-001/config/resolved_config.json"
        ]
        self.assertEqual(blob.uploads[0]["if_generation_match"], 0)

    def test_generation_targets_only_new_artifacts(self) -> None:
        layout = DatasetArtifactLayout.for_dataset_version("dv-001")
        inventory = layout.artifact_uri_inventory()

        self.assertEqual(
            set(inventory),
            {
                "root",
                "config",
                "metadata",
                "canonical",
                "reports",
                "model_inputs",
                "audio_prefix",
            },
        )
        for uri in _inventory_strings(inventory):
            self.assertTrue(
                uri.startswith("gs://wd-transcription-data/sft/dv-001/"),
                uri,
            )
            for forbidden in (
                "model/data",
                "benchmark",
                "inference_manifests",
                ".env",
                "GOOGLE_APPLICATION_CREDENTIALS",
                "raw_row",
            ):
                self.assertNotIn(forbidden, uri)


if __name__ == "__main__":
    unittest.main()
