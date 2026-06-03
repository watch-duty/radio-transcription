"""Tests for the Phase 4 Label Studio export parser CLI."""

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


def _export_task(
    *,
    annotations: list[dict[str, object]],
    task_id: int = 101,
    audio_segment_id: str = "audio-a",
) -> dict[str, object]:
    return {
        "id": task_id,
        "data": {
            "audio": f"gs://bucket/{audio_segment_id}.flac",
            "reference_transcript": "Engine 41 copy",
            "audio_segment_id": audio_segment_id,
            "source_window_id": f"source-window-{audio_segment_id}",
            "model_ready_audio_uri": f"gs://bucket/{audio_segment_id}.flac",
            "original_audio_uri": "gs://bucket/original.wav",
            "offset": 12.5,
            "duration": 2.5,
            "source_group": "source-a",
            "row_index": task_id,
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
        },
        "annotations": annotations,
    }


def _annotation(
    *,
    annotation_id: int = 201,
    result: list[dict[str, object]],
) -> dict[str, object]:
    return {
        "id": annotation_id,
        "completed_at": "2026-06-03T01:00:00Z",
        "updated_at": "2026-06-03T01:01:00Z",
        "last_action": "submitted",
        "was_cancelled": False,
        "completed_by": 88,
        "lead_time": 3.25,
        "result": result,
    }


def _review_status_result(status: str) -> dict[str, object]:
    return {
        "from_name": "review_status",
        "to_name": "audio",
        "type": "choices",
        "value": {"choices": [status]},
    }


def _transcription_result(text: str) -> dict[str, object]:
    return {
        "from_name": "transcription",
        "to_name": "audio",
        "type": "textarea",
        "value": {"text": [text]},
    }


def _valid_export_task() -> dict[str, object]:
    return _export_task(
        annotations=[
            _annotation(
                result=[
                    _review_status_result("Reviewed"),
                    _transcription_result("Corrected transcript"),
                ]
            )
        ]
    )


def _duplicate_transcription_export_task() -> dict[str, object]:
    duplicate_transcription = "duplicate transcription"
    return _export_task(
        annotations=[
            _annotation(
                result=[
                    _review_status_result("Reviewed"),
                    _transcription_result(duplicate_transcription),
                    _transcription_result("second duplicate transcript"),
                ]
            )
        ]
    )


def _write_export_json(
    path: pathlib.Path,
    tasks: list[dict[str, object]],
) -> None:
    path.write_text(json.dumps(tasks), encoding="utf-8")


def _read_jsonl(path: pathlib.Path) -> list[dict[str, object]]:
    rows = []
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        if raw_line.strip():
            rows.append(json.loads(raw_line))
    return rows


class TestParseLabelStudioExportCli(unittest.TestCase):
    def test_help_lists_expected_arguments(self) -> None:
        import parse_label_studio_export

        output = io.StringIO()
        with self.assertRaises(SystemExit) as ctx:
            with contextlib.redirect_stdout(output):
                parse_label_studio_export.main(["--help"])

        self.assertEqual(ctx.exception.code, 0)
        help_text = output.getvalue()
        self.assertIn("--label-studio-export-json", help_text)
        self.assertIn("--reviewed-jsonl", help_text)
        self.assertIn("--errors-jsonl", help_text)
        self.assertNotIn("--reviewed-csv", help_text)

    def test_local_export_success_uploads_reviewed_jsonl_and_empty_errors(
        self,
    ) -> None:
        import parse_label_studio_export

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

        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = pathlib.Path(tmpdir)
            export_path = tmp_path / "export.json"
            _write_export_json(export_path, [_valid_export_task()])

            with (
                unittest.mock.patch.object(
                    parse_label_studio_export,
                    "_new_storage_client",
                    return_value=object(),
                ),
                unittest.mock.patch.object(
                    parse_label_studio_export.gcs_utils,
                    "upload_file_to_blob",
                    fake_upload_file_to_blob,
                ),
            ):
                exit_code = parse_label_studio_export.main(
                    [
                        "--label-studio-export-json",
                        str(export_path),
                        "--reviewed-jsonl",
                        "gs://bucket/output/reviewed.jsonl",
                        "--errors-jsonl",
                        "gs://bucket/output/errors.jsonl",
                    ]
                )

        self.assertEqual(exit_code, 0)
        self.assertEqual(
            uploaded_paths,
            [
                "gs://bucket/output/reviewed.jsonl",
                "gs://bucket/output/errors.jsonl",
            ],
        )
        reviewed_rows = [
            json.loads(line)
            for line in uploaded_text_by_path[
                "gs://bucket/output/reviewed.jsonl"
            ].splitlines()
            if line.strip()
        ]
        self.assertEqual(len(reviewed_rows), 1)
        self.assertEqual(
            reviewed_rows[0]["label_studio_review_status"], "Reviewed"
        )
        self.assertEqual(
            reviewed_rows[0]["submitted_transcript"],
            "Corrected transcript",
        )
        self.assertNotIn("split", json.dumps(reviewed_rows[0], sort_keys=True))
        self.assertEqual(
            uploaded_text_by_path["gs://bucket/output/errors.jsonl"], ""
        )

    def test_local_export_malformed_uploads_errors_and_empty_reviewed(
        self,
    ) -> None:
        import parse_label_studio_export

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

        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = pathlib.Path(tmpdir)
            export_path = tmp_path / "export.json"
            _write_export_json(
                export_path,
                [_duplicate_transcription_export_task()],
            )

            with (
                unittest.mock.patch.object(
                    parse_label_studio_export,
                    "_new_storage_client",
                    return_value=object(),
                ),
                unittest.mock.patch.object(
                    parse_label_studio_export.gcs_utils,
                    "upload_file_to_blob",
                    fake_upload_file_to_blob,
                ),
            ):
                exit_code = parse_label_studio_export.main(
                    [
                        "--label-studio-export-json",
                        str(export_path),
                        "--reviewed-jsonl",
                        "gs://bucket/output/reviewed.jsonl",
                        "--errors-jsonl",
                        "gs://bucket/output/errors.jsonl",
                    ]
                )

        self.assertEqual(exit_code, 1)
        self.assertEqual(
            uploaded_paths,
            [
                "gs://bucket/output/reviewed.jsonl",
                "gs://bucket/output/errors.jsonl",
            ],
        )
        self.assertEqual(
            uploaded_text_by_path["gs://bucket/output/reviewed.jsonl"], ""
        )
        error_rows = [
            json.loads(line)
            for line in uploaded_text_by_path[
                "gs://bucket/output/errors.jsonl"
            ].splitlines()
            if line.strip()
        ]
        self.assertEqual(len(error_rows), 1)
        self.assertEqual(error_rows[0]["error_code"], "duplicate_transcription")
        self.assertEqual(error_rows[0]["task_id"], 101)
        self.assertEqual(error_rows[0]["audio_segment_id"], "audio-a")
        self.assertEqual(error_rows[0]["annotation_id"], 201)

    def test_rejects_local_reviewed_and_error_outputs(self) -> None:
        import parse_label_studio_export

        with self.assertRaises(SystemExit) as ctx:
            parse_label_studio_export.main(
                [
                    "--label-studio-export-json",
                    "export.json",
                    "--reviewed-jsonl",
                    "reviewed.jsonl",
                    "--errors-jsonl",
                    "gs://bucket/output/errors.jsonl",
                ]
            )

        self.assertEqual(ctx.exception.code, 2)

    def test_gcs_input_and_output_wiring(self) -> None:
        import parse_label_studio_export

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
                parse_label_studio_export,
                "_new_storage_client",
                return_value=object(),
            ),
            unittest.mock.patch.object(
                parse_label_studio_export,
                "_download_gcs_text",
                return_value=json.dumps([_valid_export_task()]),
            ),
            unittest.mock.patch.object(
                parse_label_studio_export.gcs_utils,
                "upload_file_to_blob",
                fake_upload_file_to_blob,
            ),
        ):
            exit_code = parse_label_studio_export.main(
                [
                    "--label-studio-export-json",
                    "gs://bucket/input/export.json",
                    "--reviewed-jsonl",
                    "gs://bucket/output/reviewed.jsonl",
                    "--errors-jsonl",
                    "gs://bucket/output/errors.jsonl",
                ]
            )

        self.assertEqual(exit_code, 0)
        self.assertEqual(
            uploaded_paths,
            [
                "gs://bucket/output/reviewed.jsonl",
                "gs://bucket/output/errors.jsonl",
            ],
        )
        self.assertIn(
            '"label_studio_review_status": "Reviewed"',
            uploaded_text_by_path["gs://bucket/output/reviewed.jsonl"],
        )
        self.assertEqual(
            uploaded_text_by_path["gs://bucket/output/errors.jsonl"], ""
        )


if __name__ == "__main__":
    unittest.main()
