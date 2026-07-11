from __future__ import annotations

import json
import unittest

import fake_gcs
from common import inference_manifest as inference_manifest_lib


class TestInferenceManifest(unittest.TestCase):
    def test_path_builder_returns_standard_path(self) -> None:
        path = inference_manifest_lib.build_inference_manifest_blob_path(
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
            inference_manifest_lib.model_family_slug_from_model_id(
                "gemini-3.1-flash-lite"
            ),
            "gemini_3_1_flash_lite",
        )
        self.assertEqual(
            inference_manifest_lib.model_family_slug_from_model_id(
                "publishers/google/models/gemini-3.1-flash-lite@001"
            ),
            "gemini_3_1_flash_lite",
        )

    def test_model_slug_rejects_endpoint_resource(self) -> None:
        with self.assertRaisesRegex(ValueError, "endpoint resource"):
            inference_manifest_lib.model_family_slug_from_model_id(
                "projects/123/locations/us/endpoints/456"
            )

    def test_row_builder_preserves_metadata_and_writes_target_field(
        self,
    ) -> None:
        rows = inference_manifest_lib.build_inference_manifest_rows(
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

    def test_missing_prediction_omits_target_field(self) -> None:
        rows = inference_manifest_lib.build_inference_manifest_rows(
            model_family_slug="gemini_3_1_flash_lite",
            source_rows=[
                {"audio_filepath": "gs://audio/a.flac", "text": "alpha"}
            ],
            predictions_by_audio_uri={},
        )

        self.assertNotIn("pred_text_gemini_3_1_flash_lite", rows[0])

    def test_empty_prediction_writes_empty_string(self) -> None:
        rows = inference_manifest_lib.build_inference_manifest_rows(
            model_family_slug="gemini_3_1_flash_lite",
            source_rows=[
                {"audio_filepath": "gs://audio/a.flac", "text": "alpha"}
            ],
            predictions_by_audio_uri={"gs://audio/a.flac": ""},
        )

        self.assertEqual(rows[0]["pred_text_gemini_3_1_flash_lite"], "")

    def test_duplicate_audio_uri_rows_raise(self) -> None:
        with self.assertRaisesRegex(ValueError, "unique 'audio_filepath'"):
            inference_manifest_lib.build_inference_manifest_rows(
                model_family_slug="gemini_3_1_flash_lite",
                source_rows=[
                    {
                        "audio_filepath": "gs://audio/a.flac",
                        "text": "alpha",
                        "segment_id": "1",
                    },
                    {
                        "audio_filepath": "gs://audio/a.flac",
                        "text": "bravo",
                        "segment_id": "2",
                    },
                ],
                predictions_by_audio_uri={"gs://audio/a.flac": "shared"},
            )

    def test_stale_target_field_is_overwritten(self) -> None:
        rows = inference_manifest_lib.build_inference_manifest_rows(
            model_family_slug="gemini_3_1_flash_lite",
            source_rows=[
                {
                    "audio_filepath": "gs://audio/a.flac",
                    "text": "alpha",
                    "pred_text_gemini_3_1_flash_lite": "stale",
                }
            ],
            predictions_by_audio_uri={"gs://audio/a.flac": "fresh"},
        )

        self.assertEqual(rows[0]["pred_text_gemini_3_1_flash_lite"], "fresh")

    def test_stale_target_field_is_removed_when_prediction_is_missing(
        self,
    ) -> None:
        rows = inference_manifest_lib.build_inference_manifest_rows(
            model_family_slug="gemini_3_1_flash_lite",
            source_rows=[
                {
                    "audio_filepath": "gs://audio/a.flac",
                    "text": "alpha",
                    "pred_text_gemini_3_1_flash_lite": "stale",
                }
            ],
            predictions_by_audio_uri={},
        )

        self.assertNotIn("pred_text_gemini_3_1_flash_lite", rows[0])

    def test_rejects_non_target_pred_text_field(self) -> None:
        with self.assertRaisesRegex(ValueError, "merged comparison manifest"):
            inference_manifest_lib.build_inference_manifest_rows(
                model_family_slug="gemini_3_1_flash_lite",
                source_rows=[
                    {
                        "audio_filepath": "gs://audio/a.flac",
                        "text": "alpha",
                        "pred_text_other_model": "other",
                    }
                ],
                predictions_by_audio_uri={"gs://audio/a.flac": "fresh"},
            )

    def test_missing_reference_text_raises(self) -> None:
        for text in ("", "   ", None):
            with self.subTest(text=text):
                with self.assertRaisesRegex(
                    ValueError, "non-empty string 'text'"
                ):
                    inference_manifest_lib.build_inference_manifest_rows(
                        model_family_slug="gemini_3_1_flash_lite",
                        source_rows=[
                            {
                                "audio_filepath": "gs://audio/a.flac",
                                "text": text,
                            }
                        ],
                        predictions_by_audio_uri={},
                    )

    def test_extra_prediction_uri_raises(self) -> None:
        with self.assertRaisesRegex(
            ValueError, "not present in the source rows"
        ):
            inference_manifest_lib.build_inference_manifest_rows(
                model_family_slug="gemini_3_1_flash_lite",
                source_rows=[
                    {"audio_filepath": "gs://audio/a.flac", "text": "alpha"}
                ],
                predictions_by_audio_uri={
                    "gs://audio/a.flac": "alpha",
                    "gs://audio/b.flac": "bravo",
                },
            )

    def test_invalid_path_components_raise(self) -> None:
        build_path = inference_manifest_lib.build_inference_manifest_blob_path
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
                        build_path(**kwargs)

    def test_validate_artifact_label_returns_valid_labels(self) -> None:
        for label in ("base", "tuned", "checkpoint_6"):
            with self.subTest(label=label):
                self.assertEqual(
                    inference_manifest_lib.validate_artifact_label(label),
                    label,
                )

    def test_validate_artifact_label_rejects_invalid_labels(self) -> None:
        for label in (
            "",
            ".",
            "..",
            "bad label",
            "nested/label",
            "base.jsonl",
        ):
            with self.subTest(label=label):
                with self.assertRaises(ValueError):
                    inference_manifest_lib.validate_artifact_label(label)

    def test_upload_inference_manifest_writes_jsonl_and_returns_uri(
        self,
    ) -> None:
        storage = fake_gcs.FakeStorageClient()

        uri = inference_manifest_lib.upload_inference_manifest(
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
