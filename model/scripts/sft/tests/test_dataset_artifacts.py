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
    DatasetArtifactError,
    DatasetArtifactLayout,
    DatasetVersionExistsError,
    audio_object_uri,
    ensure_dataset_version_absent,
    upload_file_create_only,
    upload_text_create_only,
)
from dataset_split.types import LabeledSegment  # noqa: E402


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

    def upload_from_filename(
        self,
        filename: str,
        *,
        content_type: str,
        if_generation_match: int | object = _missing,
    ) -> None:
        if if_generation_match is self._missing:
            raise AssertionError("if_generation_match=0 is required")
        self.uploads.append(
            {
                "filename": filename,
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


def _segment(*, split: str = "train", row_index: int = 7) -> LabeledSegment:
    return LabeledSegment(
        dataset_name="calls",
        dataset_family="bcfy_calls",
        source_strategy="bcfy_calls",
        source_group="bcfy_calls:group/with/slash",
        audio_uri="https://example.test/raw/path/source.mp3",
        original_audio_uri="gs://raw-bucket/source/path/source.mp3",
        text="engine 41 copy",
        row_index=row_index,
        offset=0.0,
        duration=4.25,
        timestamp="2026-05-27T12:00:00Z",
        example_id="example/unsafe",
        segment_id="segment:unsafe",
        split=split,
    )


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

    def test_upload_file_create_only_uses_generation_precondition(
        self,
    ) -> None:
        client = FakeStorageClient()
        local_path = Path("/tmp/audio.flac")

        uri = upload_file_create_only(
            client,
            "gs://wd-transcription-data/sft/dv-001/audio/derived/a.flac",
            local_path,
            content_type="audio/flac",
        )

        self.assertEqual(
            uri,
            "gs://wd-transcription-data/sft/dv-001/audio/derived/a.flac",
        )
        blob = client.fake_bucket.blobs["sft/dv-001/audio/derived/a.flac"]
        self.assertEqual(
            blob.uploads,
            [
                {
                    "filename": str(local_path),
                    "content_type": "audio/flac",
                    "if_generation_match": 0,
                }
            ],
        )

    def test_upload_file_create_only_maps_precondition_failure(self) -> None:
        client = FakeStorageClient(upload_error=FakePreconditionFailure("412"))

        with self.assertRaisesRegex(
            DatasetVersionExistsError,
            "gs://wd-transcription-data/sft/dv-001/audio/derived/a.flac",
        ):
            upload_file_create_only(
                client,
                "gs://wd-transcription-data/sft/dv-001/audio/derived/a.flac",
                Path("/tmp/audio.flac"),
                content_type="audio/flac",
            )

        blob = client.fake_bucket.blobs["sft/dv-001/audio/derived/a.flac"]
        self.assertEqual(blob.uploads[0]["if_generation_match"], 0)

    def test_audio_object_uri_uses_action_folders_without_split_parts(
        self,
    ) -> None:
        layout = DatasetArtifactLayout.for_dataset_version("dv-001")

        uris = {
            action: audio_object_uri(
                layout,
                action=action,
                segment=_segment(split="eval"),
                suffix=".flac" if action != "copied" else ".mp3",
            )
            for action in ("copied", "derived", "transcoded")
        }

        self.assertIn("audio/copied/", uris["copied"])
        self.assertIn("audio/derived/", uris["derived"])
        self.assertIn("audio/transcoded/", uris["transcoded"])
        self.assertTrue(uris["copied"].endswith(".mp3"))
        self.assertTrue(uris["derived"].endswith(".flac"))
        self.assertTrue(uris["transcoded"].endswith(".flac"))
        for uri in uris.values():
            self.assertTrue(uri.startswith(layout.audio_prefix_uri), uri)
            self.assertNotIn("/train/", uri)
            self.assertNotIn("/eval/", uri)
            self.assertNotIn("bcfy_calls:group/with/slash", uri)
            self.assertNotIn("https://example.test", uri)

    def test_audio_object_uri_rejects_reused_action(self) -> None:
        layout = DatasetArtifactLayout.for_dataset_version("dv-001")

        with self.assertRaises(DatasetArtifactError):
            audio_object_uri(
                layout,
                action="reused",
                segment=_segment(),
                suffix=".flac",
            )

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

    def test_layout_rejects_unsafe_path_components(self) -> None:
        for dataset_version_id in ("../bad", "a/b", r"a\b", ".", ".."):
            with self.subTest(dataset_version_id=dataset_version_id):
                with self.assertRaises(DatasetArtifactError):
                    DatasetArtifactLayout.for_dataset_version(
                        dataset_version_id
                    )

        layout = DatasetArtifactLayout.for_dataset_version("dv-001")
        for dataset_name in ("../calls", "calls/feed", r"calls\feed"):
            with self.subTest(dataset_name=dataset_name):
                with self.assertRaises(DatasetArtifactError):
                    layout.per_dataset_manifest_uri(dataset_name, "train")
        for writer in ("../nemo", "nemo/train", r"nemo\train"):
            with self.subTest(writer=writer):
                with self.assertRaises(DatasetArtifactError):
                    layout.model_input_uri(writer, "train")
        with self.assertRaises(DatasetArtifactError):
            layout.model_input_uri("nemo", "train", "../json")

    def test_layout_rejects_invalid_root_prefix(self) -> None:
        for root_prefix in (
            "",
            "s3://bucket/sft",
            "gs:///sft",
            "gs://b/../sft",
        ):
            with self.subTest(root_prefix=root_prefix):
                with self.assertRaises(DatasetArtifactError):
                    DatasetArtifactLayout.for_dataset_version(
                        "dv-001", root_prefix=root_prefix
                    )


if __name__ == "__main__":
    unittest.main()
