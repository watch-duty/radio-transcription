"""Tests for the Phase 3 Label Studio package CLI."""

from __future__ import annotations

import contextlib
import csv
import io
import json
import sys
import tempfile
import unittest
import unittest.mock
from pathlib import Path

_REVIEW_DIR = str(Path(__file__).resolve().parent.parent)
_COLABS_DIR = str(
    Path(__file__).resolve().parent.parent.parent.parent / "colabs"
)
if _REVIEW_DIR not in sys.path:
    sys.path.insert(0, _REVIEW_DIR)
if _COLABS_DIR not in sys.path:
    sys.path.insert(0, _COLABS_DIR)


def _ranked_row(
    audio_segment_id: str = "audio-a",
    *,
    rank: int = 1,
) -> dict[str, object]:
    return {
        "rank": rank,
        "wer": 25.0 + rank,
        "insertions": 1,
        "deletions": 2,
        "substitutions": 3,
        "hits": 4,
        "audio_segment_id": audio_segment_id,
        "source_window_id": f"source-window-{audio_segment_id}",
        "model_ready_audio_uri": f"gs://bucket/{audio_segment_id}.flac",
        "original_audio_uri": "gs://bucket/original.wav",
        "offset": rank * 1.0,
        "duration": 2.5,
        "source_group": "source-a",
        "row_index": rank,
        "split": "train",
        "dataset_name": "test-dataset",
        "text": f"Engine {rank} copy",
        "prediction_text": "MODEL PREDICTION MUST NOT LEAK",
        "model_id": "gemini-3.5-flash",
        "prompt_fingerprint": "prompt-fp",
        "context_policy_fingerprint": "context-policy-fp",
        "num_recent_events": 1000,
        "context_fingerprint": f"context-fp-{rank}",
        "cache_created_at": "2026-06-03T00:00:00Z",
    }


def _write_jsonl(path: Path, rows: list[dict[str, object]]) -> None:
    with path.open("w", encoding="utf-8") as output_file:
        for row in rows:
            output_file.write(json.dumps(row) + "\n")


class TestPackageLabelStudioCli(unittest.TestCase):
    def test_help_lists_expected_arguments(self) -> None:
        import package_label_studio

        output = io.StringIO()
        with self.assertRaises(SystemExit) as ctx:
            with contextlib.redirect_stdout(output):
                package_label_studio.main(["--help"])

        self.assertEqual(ctx.exception.code, 0)
        help_text = output.getvalue()
        self.assertIn("--ranked-jsonl", help_text)
        self.assertIn("--tasks-json", help_text)
        self.assertIn("--label-config-xml", help_text)
        self.assertIn("--readme-md", help_text)
        self.assertIn("--preview-csv", help_text)
        self.assertIn("--limit", help_text)

    def test_local_package_outputs_omit_prediction_text(self) -> None:
        import package_label_studio

        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            ranked_path = tmp_path / "ranked.jsonl"
            tasks_path = tmp_path / "tasks.json"
            xml_path = tmp_path / "label_config.xml"
            readme_path = tmp_path / "README.md"
            preview_path = tmp_path / "preview.csv"
            _write_jsonl(
                ranked_path,
                [_ranked_row("audio-a"), _ranked_row("audio-b", rank=2)],
            )

            exit_code = package_label_studio.main(
                [
                    "--ranked-jsonl",
                    str(ranked_path),
                    "--tasks-json",
                    str(tasks_path),
                    "--label-config-xml",
                    str(xml_path),
                    "--readme-md",
                    str(readme_path),
                    "--preview-csv",
                    str(preview_path),
                    "--limit",
                    "1",
                ]
            )

            tasks = json.loads(tasks_path.read_text(encoding="utf-8"))
            xml = xml_path.read_text(encoding="utf-8")
            readme = readme_path.read_text(encoding="utf-8")
            preview_text = preview_path.read_text(encoding="utf-8")
            with preview_path.open(encoding="utf-8") as preview_file:
                preview_rows = list(csv.DictReader(preview_file))

        self.assertEqual(exit_code, 0)
        self.assertEqual(len(tasks), 1)
        data = tasks[0]["data"]
        self.assertEqual(data["audio"], "gs://bucket/audio-a.flac")
        self.assertEqual(data["reference_transcript"], "Engine 1 copy")
        self.assertEqual(data["audio_segment_id"], "audio-a")
        self.assertEqual(data["source_window_id"], "source-window-audio-a")
        self.assertEqual(data["context_fingerprint"], "context-fp-1")
        self.assertNotIn("prediction_text", json.dumps(tasks))
        self.assertIn('TextArea name="transcription"', xml)
        self.assertIn('Choices name="review_status"', xml)
        self.assertIn("Reviewed", xml)
        self.assertIn("Skip", xml)
        self.assertIn("roles/storage.objectViewer", readme)
        self.assertIn("GCS source storage", readme)
        self.assertIn(
            "gcloud storage buckets add-iam-policy-binding "
            "gs://wd-transcription-data",
            readme,
        )
        self.assertIn("reference_transcript", preview_text)
        self.assertEqual(preview_rows[0]["audio_segment_id"], "audio-a")
        self.assertNotIn("prediction_text", preview_text)

    def test_gcs_package_outputs_upload_all_artifacts(self) -> None:
        import package_label_studio

        uploaded_paths: list[str] = []

        def fake_upload_file_to_blob(
            _storage_client: object,
            bucket_name: str,
            blob_path: str,
            _source_file_name: str,
        ) -> None:
            uploaded_paths.append(f"gs://{bucket_name}/{blob_path}")

        with (
            unittest.mock.patch.object(
                package_label_studio,
                "_new_storage_client",
                return_value=object(),
            ),
            unittest.mock.patch.object(
                package_label_studio.gcs_utils,
                "download_jsonl_manifest",
                return_value=[_ranked_row()],
            ),
            unittest.mock.patch.object(
                package_label_studio.gcs_utils,
                "upload_file_to_blob",
                fake_upload_file_to_blob,
            ),
        ):
            exit_code = package_label_studio.main(
                [
                    "--ranked-jsonl",
                    "gs://bucket/input/ranked.jsonl",
                    "--tasks-json",
                    "gs://bucket/output/tasks.json",
                    "--label-config-xml",
                    "gs://bucket/output/label_config.xml",
                    "--readme-md",
                    "gs://bucket/output/README.md",
                    "--preview-csv",
                    "gs://bucket/output/preview.csv",
                ]
            )

        self.assertEqual(exit_code, 0)
        self.assertEqual(
            uploaded_paths,
            [
                "gs://bucket/output/tasks.json",
                "gs://bucket/output/label_config.xml",
                "gs://bucket/output/README.md",
                "gs://bucket/output/preview.csv",
            ],
        )


if __name__ == "__main__":
    unittest.main()
