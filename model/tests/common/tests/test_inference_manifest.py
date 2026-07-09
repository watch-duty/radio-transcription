from __future__ import annotations

import json
import sys
import unittest
import unittest.mock
from pathlib import Path

from fake_gcs import FakeStorageClient

_SRC_DIR = str(Path(__file__).resolve().parents[3] / "src")
if _SRC_DIR not in sys.path:
    sys.path.insert(0, _SRC_DIR)

from common import inference_hf, inference_nemo  # noqa: E402
from common.inference_manifest import (  # noqa: E402
    build_inference_manifest_blob_path,
    build_inference_manifest_rows,
    model_family_slug_from_model_id,
    upload_inference_manifest,
)


def _canonical_source_row(**overrides: object) -> dict[str, object]:
    row: dict[str, object] = {
        "audio_filepath": "gs://audio/a.flac",
        "text": "alpha",
        "offset": 0.0,
        "duration": 3.0,
        "example_id": "example-a",
        "segment_id": "001",
        "split": "eval",
        "lang": "en",
        "dataset": {"name": "echo", "family": "radio"},
        "source_audio": {
            "audio_filepath": "gs://source/a.mp3",
            "offset": 10.0,
            "duration": 3.0,
        },
    }
    row.update(overrides)
    return row


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

    def test_model_slug_rejects_endpoint_resource(self) -> None:
        with self.assertRaisesRegex(ValueError, "endpoint resource"):
            model_family_slug_from_model_id(
                "projects/123/locations/us/endpoints/456"
            )

    def test_row_builder_preserves_metadata_and_writes_target_field(
        self,
    ) -> None:
        rows = build_inference_manifest_rows(
            model_family_slug="gemini_3_1_flash_lite",
            source_rows=[
                _canonical_source_row()
            ],
            predictions_by_audio_uri={"gs://audio/a.flac": "predicted alpha"},
        )

        self.assertEqual(rows[0]["dataset"]["name"], "echo")
        self.assertEqual(
            rows[0]["source_audio"]["audio_filepath"], "gs://source/a.mp3"
        )
        self.assertEqual(
            rows[0]["pred_text_gemini_3_1_flash_lite"], "predicted alpha"
        )
        pred_fields = [key for key in rows[0] if key.startswith("pred_text_")]
        self.assertEqual(pred_fields, ["pred_text_gemini_3_1_flash_lite"])

    def test_missing_prediction_omits_target_field(self) -> None:
        rows = build_inference_manifest_rows(
            model_family_slug="gemini_3_1_flash_lite",
            source_rows=[
                _canonical_source_row()
            ],
            predictions_by_audio_uri={},
        )

        self.assertNotIn("pred_text_gemini_3_1_flash_lite", rows[0])

    def test_empty_prediction_writes_empty_string(self) -> None:
        rows = build_inference_manifest_rows(
            model_family_slug="gemini_3_1_flash_lite",
            source_rows=[
                _canonical_source_row()
            ],
            predictions_by_audio_uri={"gs://audio/a.flac": ""},
        )

        self.assertEqual(rows[0]["pred_text_gemini_3_1_flash_lite"], "")

    def test_duplicate_audio_uri_rows_raise(self) -> None:
        with self.assertRaisesRegex(ValueError, "duplicate_audio_filepath"):
            build_inference_manifest_rows(
                model_family_slug="gemini_3_1_flash_lite",
                source_rows=[
                    _canonical_source_row(segment_id="1"),
                    _canonical_source_row(
                        audio_filepath="gs://audio/a.flac",
                        text="bravo",
                        segment_id="2",
                    ),
                ],
                predictions_by_audio_uri={"gs://audio/a.flac": "shared"},
            )

    def test_prediction_fields_in_source_rows_raise(
        self,
    ) -> None:
        with self.assertRaisesRegex(ValueError, "pred_text_gemini"):
            build_inference_manifest_rows(
                model_family_slug="gemini_3_1_flash_lite",
                source_rows=[
                    _canonical_source_row(
                        pred_text_gemini_3_1_flash_lite="stale"
                    )
                ],
                predictions_by_audio_uri={"gs://audio/a.flac": "fresh"},
            )

    def test_rejects_non_target_pred_text_field(self) -> None:
        with self.assertRaisesRegex(ValueError, "pred_text_other_model"):
            build_inference_manifest_rows(
                model_family_slug="gemini_3_1_flash_lite",
                source_rows=[
                    _canonical_source_row(pred_text_other_model="other")
                ],
                predictions_by_audio_uri={"gs://audio/a.flac": "fresh"},
            )

    def test_missing_reference_text_raises(self) -> None:
        for text in ("", "   ", None):
            with self.subTest(text=text):
                with self.assertRaisesRegex(
                    ValueError, "Canonical Manifest validation failed"
                ):
                    build_inference_manifest_rows(
                        model_family_slug="gemini_3_1_flash_lite",
                        source_rows=[
                            _canonical_source_row(text=text)
                        ],
                        predictions_by_audio_uri={},
                    )

    def test_extra_prediction_uri_raises(self) -> None:
        with self.assertRaisesRegex(
            ValueError, "not present in the source rows"
        ):
            build_inference_manifest_rows(
                model_family_slug="gemini_3_1_flash_lite",
                source_rows=[
                    _canonical_source_row()
                ],
                predictions_by_audio_uri={
                    "gs://audio/a.flac": "alpha",
                    "gs://audio/b.flac": "bravo",
                },
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
                _canonical_source_row()
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
        self.assertEqual(rows[0]["dataset"]["name"], "echo")
        self.assertEqual(
            rows[0]["pred_text_gemini_3_1_flash_lite"], "predicted alpha"
        )

    def test_hf_pipeline_rejects_partial_manifest_before_download(
        self,
    ) -> None:
        with unittest.mock.patch.object(inference_hf, "_require_hf"):
            with self.assertRaisesRegex(
                ValueError, "Canonical Manifest validation failed"
            ):
                inference_hf.run_huggingface_inference_pipeline(
                    model=object(),
                    processor=object(),
                    manifest_data=[
                        {
                            "audio_filepath": "gs://audio/a.flac",
                            "text": "alpha",
                        }
                    ],
                    storage_client=object(),
                    project_name="project",
                    selected_model="model",
                )

    def test_nemo_pipeline_rejects_partial_manifest_before_download(
        self,
    ) -> None:
        with unittest.mock.patch.object(inference_nemo, "_require_torch"):
            with self.assertRaisesRegex(
                ValueError, "Canonical Manifest validation failed"
            ):
                inference_nemo.run_inference_pipeline(
                    model=object(),
                    manifest_data=[
                        {
                            "audio_filepath": "gs://audio/a.flac",
                            "text": "alpha",
                        }
                    ],
                    prompt_fn=lambda *_: object(),
                    inference_fn=lambda *_: [],
                    decode_fn=lambda *_: "",
                    storage_client=object(),
                    project_name="project",
                    selected_model="model",
                )


if __name__ == "__main__":
    unittest.main()
