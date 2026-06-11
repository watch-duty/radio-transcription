from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path
from typing import Any

_SRC_DIR = str(Path(__file__).resolve().parents[3] / "src")
if _SRC_DIR not in sys.path:
    sys.path.insert(0, _SRC_DIR)

from common.inference_manifest import (  # noqa: E402
    build_inference_manifest_blob_path,
    build_inference_manifest_rows,
    model_family_slug_from_model_id,
    upload_inference_manifest,
)


class FakeBlob:
    def __init__(
        self, store: dict[tuple[str, str], str], bucket: str, name: str
    ) -> None:
        self._store = store
        self._bucket = bucket
        self.name = name
        self.content_type: str | None = None

    def upload_from_string(
        self, data: str, content_type: str | None = None, **_: Any
    ) -> None:
        self._store[(self._bucket, self.name)] = data
        self.content_type = content_type


class FakeBucket:
    def __init__(self, store: dict[tuple[str, str], str], name: str) -> None:
        self._store = store
        self.name = name

    def blob(self, name: str) -> FakeBlob:
        return FakeBlob(self._store, self.name, name)


class FakeStorageClient:
    def __init__(self) -> None:
        self.store: dict[tuple[str, str], str] = {}

    def bucket(self, name: str) -> FakeBucket:
        return FakeBucket(self.store, name)

    def get(self, uri: str) -> str:
        bucket, blob = uri[len("gs://") :].split("/", maxsplit=1)
        return self.store[(bucket, blob)]


class TestInferenceManifest(unittest.TestCase):
    def test_path_builder_returns_standard_path(self) -> None:
        path = build_inference_manifest_blob_path(
            inference_dataset_slug="echo/eval",
            model_family_slug="gemini_3_1_flash_lite",
            run_id="run-a",
            artifact_label="base",
        )

        self.assertEqual(
            path,
            "inference_manifests/echo/eval/gemini_3_1_flash_lite/"
            "run-a/base.jsonl",
        )

    def test_model_slug_conversion(self) -> None:
        self.assertEqual(
            model_family_slug_from_model_id("gemini-3.1-flash-lite"),
            "gemini_3_1_flash_lite",
        )
        self.assertEqual(
            model_family_slug_from_model_id(
                "publishers/google/models/gemini-3.1-flash-lite@001"
            ),
            "gemini_3_1_flash_lite",
        )

    def test_row_builder_preserves_metadata_and_writes_target_field(
        self,
    ) -> None:
        rows = build_inference_manifest_rows(
            model_family_slug="gemini_3_1_flash_lite",
            source_rows=[
                {
                    "audio_filepath": "gs://audio/a.flac",
                    "text": "alpha",
                    "dataset_name": "echo",
                    "duration": 3.0,
                }
            ],
            predictions_by_audio_uri={"gs://audio/a.flac": "predicted alpha"},
        )

        self.assertEqual(rows[0]["dataset_name"], "echo")
        self.assertEqual(
            rows[0]["pred_text_gemini_3_1_flash_lite"], "predicted alpha"
        )
        pred_fields = [key for key in rows[0] if key.startswith("pred_text_")]
        self.assertEqual(pred_fields, ["pred_text_gemini_3_1_flash_lite"])

    def test_missing_prediction_writes_empty_string(self) -> None:
        rows = build_inference_manifest_rows(
            model_family_slug="gemini_3_1_flash_lite",
            source_rows=[{"audio_filepath": "gs://audio/a.flac"}],
            predictions_by_audio_uri={},
        )

        self.assertEqual(rows[0]["pred_text_gemini_3_1_flash_lite"], "")

    def test_duplicate_audio_uri_rows_are_preserved(self) -> None:
        rows = build_inference_manifest_rows(
            model_family_slug="gemini_3_1_flash_lite",
            source_rows=[
                {"audio_filepath": "gs://audio/a.flac", "segment_id": "1"},
                {"audio_filepath": "gs://audio/a.flac", "segment_id": "2"},
            ],
            predictions_by_audio_uri={"gs://audio/a.flac": "shared"},
        )

        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["segment_id"], "1")
        self.assertEqual(rows[1]["segment_id"], "2")
        self.assertEqual(rows[0]["pred_text_gemini_3_1_flash_lite"], "shared")
        self.assertEqual(rows[1]["pred_text_gemini_3_1_flash_lite"], "shared")

    def test_stale_target_field_is_overwritten(self) -> None:
        rows = build_inference_manifest_rows(
            model_family_slug="gemini_3_1_flash_lite",
            source_rows=[
                {
                    "audio_filepath": "gs://audio/a.flac",
                    "pred_text_gemini_3_1_flash_lite": "stale",
                }
            ],
            predictions_by_audio_uri={"gs://audio/a.flac": "fresh"},
        )

        self.assertEqual(rows[0]["pred_text_gemini_3_1_flash_lite"], "fresh")

    def test_rejects_non_target_pred_text_field(self) -> None:
        with self.assertRaisesRegex(ValueError, "merged comparison manifest"):
            build_inference_manifest_rows(
                model_family_slug="gemini_3_1_flash_lite",
                source_rows=[
                    {
                        "audio_filepath": "gs://audio/a.flac",
                        "pred_text_other_model": "other",
                    }
                ],
                predictions_by_audio_uri={"gs://audio/a.flac": "fresh"},
            )

    def test_invalid_path_components_raise(self) -> None:
        invalid_values = {
            "inference_dataset_slug": [
                "../echo",
                "/echo/eval",
                "echo/eval/",
                "echo//eval",
                "echo\\eval",
            ],
            "model_family_slug": ["../model"],
            "run_id": ["nested/run"],
            "artifact_label": ["base.jsonl", "bad label"],
        }
        for field_name, values in invalid_values.items():
            for value in values:
                kwargs = {
                    "inference_dataset_slug": "echo/eval",
                    "model_family_slug": "gemini_3_1_flash_lite",
                    "run_id": "run-a",
                    "artifact_label": "base",
                }
                kwargs[field_name] = value
                with self.subTest(field_name=field_name, value=value):
                    with self.assertRaises(ValueError):
                        build_inference_manifest_blob_path(**kwargs)

    def test_upload_inference_manifest_writes_jsonl_and_returns_uri(
        self,
    ) -> None:
        storage = FakeStorageClient()

        uri = upload_inference_manifest(
            storage,
            bucket_name="test-bucket",
            inference_dataset_slug="echo/eval",
            model_family_slug="gemini_3_1_flash_lite",
            run_id="run-a",
            artifact_label="base",
            source_rows=[
                {
                    "audio_filepath": "gs://audio/a.flac",
                    "text": "alpha",
                    "dataset_name": "echo",
                }
            ],
            predictions_by_audio_uri={"gs://audio/a.flac": "predicted alpha"},
        )

        self.assertEqual(
            uri,
            "gs://test-bucket/inference_manifests/echo/eval/"
            "gemini_3_1_flash_lite/run-a/base.jsonl",
        )
        content = storage.get(uri)
        self.assertTrue(content.endswith("\n"))
        rows = [json.loads(line) for line in content.splitlines()]
        self.assertEqual(rows[0]["dataset_name"], "echo")
        self.assertEqual(
            rows[0]["pred_text_gemini_3_1_flash_lite"], "predicted alpha"
        )


if __name__ == "__main__":
    unittest.main()
