"""Tests for the Phase 5 correction overlay builder CLI."""

from __future__ import annotations

import contextlib
import io
import json
import pathlib
import sys
import tempfile
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
        "split": "train",
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


def _write_jsonl(
    path: pathlib.Path,
    rows: list[dict[str, object]],
) -> None:
    with path.open("w", encoding="utf-8") as output_file:
        for row in rows:
            output_file.write(json.dumps(row, sort_keys=True))
            output_file.write("\n")


def _read_jsonl(path: pathlib.Path) -> list[dict[str, object]]:
    rows = []
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        if raw_line.strip():
            rows.append(json.loads(raw_line))
    return rows


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
        self.assertNotIn("--patched-train-jsonl", help_text)
        self.assertNotIn("--patched-eval-jsonl", help_text)
        self.assertNotIn("--train-jsonl", help_text)
        self.assertNotIn("--eval-jsonl", help_text)

    def test_local_success_writes_overlay_jsonl_and_summary_json(self) -> None:
        import build_correction_overlay

        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = pathlib.Path(tmpdir)
            reviewed_path = tmp_path / "reviewed.jsonl"
            overlay_path = tmp_path / "overlay.jsonl"
            summary_path = tmp_path / "summary.json"
            _write_jsonl(
                reviewed_path,
                [
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
            )

            exit_code = build_correction_overlay.main(
                [
                    "--reviewed-jsonl",
                    str(reviewed_path),
                    "--overlay-jsonl",
                    str(overlay_path),
                    "--summary-json",
                    str(summary_path),
                ]
            )

            overlay_rows = _read_jsonl(overlay_path)
            summary = json.loads(summary_path.read_text(encoding="utf-8"))
            patched_train_exists = (tmp_path / "patched_train.jsonl").exists()
            patched_eval_exists = (tmp_path / "patched_eval.jsonl").exists()

        self.assertEqual(exit_code, 0)
        self.assertEqual(len(overlay_rows), 3)
        self.assertEqual(
            {row["overlay_action"] for row in overlay_rows},
            {"replace_reference", "exclude"},
        )
        self.assertEqual(summary["reviewed_edited"], 1)
        self.assertEqual(summary["reviewed_unchanged"], 1)
        self.assertEqual(summary["reviewed_empty"], 1)
        self.assertEqual(summary["matching_key"], "audio_segment_id")
        self.assertFalse(summary["source_window_fallback_matching"])
        self.assertFalse(patched_train_exists)
        self.assertFalse(patched_eval_exists)

    def test_local_malformed_writes_summary_and_nonzero_without_overlay(
        self,
    ) -> None:
        import build_correction_overlay

        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = pathlib.Path(tmpdir)
            reviewed_path = tmp_path / "reviewed.jsonl"
            overlay_path = tmp_path / "overlay.jsonl"
            summary_path = tmp_path / "summary.json"
            row = _reviewed_row()
            del row["audio_segment_id"]
            _write_jsonl(reviewed_path, [row])

            exit_code = build_correction_overlay.main(
                [
                    "--reviewed-jsonl",
                    str(reviewed_path),
                    "--overlay-jsonl",
                    str(overlay_path),
                    "--summary-json",
                    str(summary_path),
                ]
            )

            summary = json.loads(summary_path.read_text(encoding="utf-8"))
            overlay_text = (
                overlay_path.read_text(encoding="utf-8")
                if overlay_path.exists()
                else ""
            )

        self.assertEqual(exit_code, 1)
        self.assertEqual(summary["malformed_error_count"], 1)
        self.assertEqual(overlay_text, "")

    def test_gcs_input_and_output_wiring(self) -> None:
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
                return_value=[_reviewed_row()],
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
                ]
            )

        self.assertEqual(exit_code, 0)
        self.assertEqual(
            uploaded_paths,
            [
                "gs://bucket/output/overlay.jsonl",
                "gs://bucket/output/summary.json",
            ],
        )
        self.assertIn(
            '"overlay_action": "replace_reference"',
            uploaded_text_by_path["gs://bucket/output/overlay.jsonl"],
        )
        self.assertIn(
            '"source_window_fallback_matching": false',
            uploaded_text_by_path["gs://bucket/output/summary.json"],
        )


if __name__ == "__main__":
    unittest.main()
