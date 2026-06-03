"""Tests for the Phase 5 correction overlay builder CLI."""

from __future__ import annotations

import contextlib
import io
import json
import pathlib
import sys
import unittest
import unittest.mock

_REVIEW_DIR = str(pathlib.Path(__file__).resolve().parent.parent)
_COLABS_DIR = str(
    pathlib.Path(__file__).resolve().parent.parent.parent.parent / "colabs"
)
if _REVIEW_DIR not in sys.path:
    sys.path.insert(0, _REVIEW_DIR)
if _COLABS_DIR not in sys.path:
    sys.path.insert(0, _COLABS_DIR)


def _reviewed_row(
    audio_segment_id: str = "audio-a",
    *,
    original_reference_transcript: str = "Engine 41 copy",
    submitted_transcript: str = "Engine 41 copies",
) -> dict[str, object]:
    return {
        "task_id": 101,
        "annotation_id": 201,
        "annotation_completed_at": "2026-06-03T01:00:00Z",
        "annotation_updated_at": "2026-06-03T01:01:00Z",
        "annotation_completed_by": 88,
        "annotation_lead_time": 3.25,
        "audio": f"gs://bucket/{audio_segment_id}.flac",
        "audio_segment_id": audio_segment_id,
        "source_window_id": f"source-window-{audio_segment_id}",
        "model_ready_audio_uri": f"gs://bucket/{audio_segment_id}.flac",
        "original_audio_uri": "gs://bucket/original.wav",
        "offset": 12.5,
        "duration": 2.5,
        "source_group": "source-a",
        "row_index": 101,
        "dataset_name": "test-dataset",
        "rank": 1,
        "wer": 42.5,
        "insertions": 1,
        "deletions": 2,
        "substitutions": 3,
        "hits": 4,
        "model_id": "gemini-3.5-flash",
        "prompt_fingerprint": "prompt-fp",
        "context_policy_fingerprint": "context-policy-fp",
        "num_recent_events": 1000,
        "context_fingerprint": "context-fp",
        "cache_created_at": "2026-06-03T00:00:00Z",
        "label_studio_review_status": "Reviewed",
        "original_reference_transcript": original_reference_transcript,
        "submitted_transcript": submitted_transcript,
    }


class TestBuildCorrectionOverlayCli(unittest.TestCase):
    def test_help_lists_expected_arguments(self) -> None:
        import build_correction_overlay

        output = io.StringIO()
        with self.assertRaises(SystemExit) as ctx:
            with contextlib.redirect_stdout(output):
                build_correction_overlay.main(["--help"])

        self.assertEqual(ctx.exception.code, 0)
        help_text = output.getvalue()
        self.assertIn("--reviewed-jsonl", help_text)
        self.assertIn("--overlay-jsonl", help_text)
        self.assertIn("--summary-json", help_text)
        self.assertIn("--errors-jsonl", help_text)
        self.assertNotIn("--patched-train-jsonl", help_text)
        self.assertNotIn("--patched-eval-jsonl", help_text)
        self.assertNotIn("--train-jsonl", help_text)
        self.assertNotIn("--eval-jsonl", help_text)

    def test_rejects_local_shared_artifact_paths(self) -> None:
        import build_correction_overlay

        with self.assertRaises(SystemExit) as ctx:
            build_correction_overlay.main(
                [
                    "--reviewed-jsonl",
                    "reviewed.jsonl",
                    "--overlay-jsonl",
                    "gs://bucket/output/overlay.jsonl",
                    "--summary-json",
                    "gs://bucket/output/summary.json",
                    "--errors-jsonl",
                    "gs://bucket/output/errors.jsonl",
                ]
            )

        self.assertEqual(ctx.exception.code, 2)

    def test_gcs_success_uploads_overlay_summary_and_empty_errors(
        self,
    ) -> None:
        import build_correction_overlay

        uploaded_paths: list[str] = []
        uploaded_text_by_path: dict[str, str] = {}

        def fake_upload_file_to_blob(
            _storage_client: object,
            bucket_name: str,
            blob_path: str,
            source_file_name: str,
        ) -> None:
            uploaded_path = f"gs://{bucket_name}/{blob_path}"
            uploaded_paths.append(uploaded_path)
            uploaded_text_by_path[uploaded_path] = pathlib.Path(
                source_file_name
            ).read_text(encoding="utf-8")

        with (
            unittest.mock.patch.object(
                build_correction_overlay,
                "_new_storage_client",
                return_value=object(),
            ),
            unittest.mock.patch.object(
                build_correction_overlay.gcs_utils,
                "download_jsonl_manifest",
                return_value=[
                    _reviewed_row(
                        "audio-edited",
                        original_reference_transcript="Engine 41 copy",
                        submitted_transcript="Engine 41 copies",
                    ),
                    _reviewed_row(
                        "audio-unchanged",
                        original_reference_transcript="Medic 2 en route",
                        submitted_transcript="Medic 2 en route",
                    ),
                    _reviewed_row(
                        "audio-empty",
                        original_reference_transcript="Drop this audio",
                        submitted_transcript="",
                    ),
                ],
            ),
            unittest.mock.patch.object(
                build_correction_overlay.gcs_utils,
                "upload_file_to_blob",
                fake_upload_file_to_blob,
            ),
        ):
            exit_code = build_correction_overlay.main(
                [
                    "--reviewed-jsonl",
                    "gs://bucket/input/reviewed.jsonl",
                    "--overlay-jsonl",
                    "gs://bucket/output/overlay.jsonl",
                    "--summary-json",
                    "gs://bucket/output/summary.json",
                    "--errors-jsonl",
                    "gs://bucket/output/errors.jsonl",
                ]
            )

        self.assertEqual(exit_code, 0)
        self.assertEqual(
            uploaded_paths,
            [
                "gs://bucket/output/overlay.jsonl",
                "gs://bucket/output/summary.json",
                "gs://bucket/output/errors.jsonl",
            ],
        )
        overlay_rows = [
            json.loads(line)
            for line in uploaded_text_by_path[
                "gs://bucket/output/overlay.jsonl"
            ].splitlines()
            if line.strip()
        ]
        self.assertEqual(len(overlay_rows), 3)
        serialized_overlay = json.dumps(overlay_rows, sort_keys=True)
        self.assertIn('"replacement_transcript": ""', serialized_overlay)
        self.assertNotIn("overlay_action", serialized_overlay)
        self.assertNotIn("overlay_status", serialized_overlay)
        self.assertNotIn("exclude_from_future_inputs", serialized_overlay)
        self.assertNotIn("prediction_text", serialized_overlay)
        self.assertIn(
            '"source_window_fallback_matching": false',
            uploaded_text_by_path["gs://bucket/output/summary.json"],
        )
        self.assertEqual(
            uploaded_text_by_path["gs://bucket/output/errors.jsonl"], ""
        )

    def test_gcs_malformed_uploads_summary_and_errors_without_overlay(
        self,
    ) -> None:
        import build_correction_overlay

        uploaded_paths: list[str] = []
        uploaded_text_by_path: dict[str, str] = {}
        malformed_row = _reviewed_row()
        del malformed_row["audio_segment_id"]

        def fake_upload_file_to_blob(
            _storage_client: object,
            bucket_name: str,
            blob_path: str,
            source_file_name: str,
        ) -> None:
            uploaded_path = f"gs://{bucket_name}/{blob_path}"
            uploaded_paths.append(uploaded_path)
            uploaded_text_by_path[uploaded_path] = pathlib.Path(
                source_file_name
            ).read_text(encoding="utf-8")

        with (
            unittest.mock.patch.object(
                build_correction_overlay,
                "_new_storage_client",
                return_value=object(),
            ),
            unittest.mock.patch.object(
                build_correction_overlay.gcs_utils,
                "download_jsonl_manifest",
                return_value=[malformed_row],
            ),
            unittest.mock.patch.object(
                build_correction_overlay.gcs_utils,
                "upload_file_to_blob",
                fake_upload_file_to_blob,
            ),
        ):
            exit_code = build_correction_overlay.main(
                [
                    "--reviewed-jsonl",
                    "gs://bucket/input/reviewed.jsonl",
                    "--overlay-jsonl",
                    "gs://bucket/output/overlay.jsonl",
                    "--summary-json",
                    "gs://bucket/output/summary.json",
                    "--errors-jsonl",
                    "gs://bucket/output/errors.jsonl",
                ]
            )

        self.assertEqual(exit_code, 1)
        self.assertEqual(
            uploaded_paths,
            [
                "gs://bucket/output/summary.json",
                "gs://bucket/output/errors.jsonl",
            ],
        )
        summary = json.loads(
            uploaded_text_by_path["gs://bucket/output/summary.json"]
        )
        error_rows = [
            json.loads(line)
            for line in uploaded_text_by_path[
                "gs://bucket/output/errors.jsonl"
            ].splitlines()
            if line.strip()
        ]
        self.assertEqual(summary["malformed_error_count"], 1)
        self.assertEqual(len(error_rows), 1)
        self.assertEqual(error_rows[0]["error_code"], "missing_required_field")


if __name__ == "__main__":
    unittest.main()
