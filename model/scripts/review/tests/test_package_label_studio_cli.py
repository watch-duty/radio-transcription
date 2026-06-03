"""Tests for the Phase 3 Label Studio package CLI."""

from __future__ import annotations

import contextlib
import csv
import io
import json
import sys
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


def _overlay_row(audio_segment_id: str = "audio-a") -> dict[str, object]:
    return {
        "audio_segment_id": audio_segment_id,
        "replacement_transcript": "Reviewed transcript",
    }


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
        self.assertIn("--correction-overlay-jsonl", help_text)

    def test_limit_is_required(self) -> None:
        import package_label_studio

        with self.assertRaises(SystemExit) as ctx:
            package_label_studio.main(
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

        self.assertEqual(ctx.exception.code, 2)

    def test_rejects_local_shared_artifact_paths(self) -> None:
        import package_label_studio

        with self.assertRaises(SystemExit) as ctx:
            package_label_studio.main(
                [
                    "--ranked-jsonl",
                    "ranked.jsonl",
                    "--tasks-json",
                    "gs://bucket/output/tasks.json",
                    "--label-config-xml",
                    "gs://bucket/output/label_config.xml",
                    "--readme-md",
                    "gs://bucket/output/README.md",
                    "--preview-csv",
                    "gs://bucket/output/preview.csv",
                    "--limit",
                    "1",
                ]
            )

        self.assertEqual(ctx.exception.code, 2)

    def test_gcs_package_uploads_artifacts_and_skips_reviewed_rows(
        self,
    ) -> None:
        import package_label_studio

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
            uploaded_text_by_path[uploaded_path] = Path(
                source_file_name
            ).read_text(encoding="utf-8")

        def fake_download_jsonl_manifest(
            _storage_client: object,
            gcs_uri: str,
        ) -> list[dict[str, object]]:
            if gcs_uri.endswith("/ranked.jsonl"):
                return [
                    _ranked_row("audio-a", rank=1),
                    _ranked_row("audio-b", rank=2),
                    _ranked_row("audio-c", rank=3),
                ]
            if gcs_uri.endswith("/overlay.jsonl"):
                return [_overlay_row("audio-a")]
            raise AssertionError(gcs_uri)

        with (
            unittest.mock.patch.object(
                package_label_studio,
                "_new_storage_client",
                return_value=object(),
            ),
            unittest.mock.patch.object(
                package_label_studio.gcs_utils,
                "download_jsonl_manifest",
                fake_download_jsonl_manifest,
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
                    "--correction-overlay-jsonl",
                    "gs://bucket/input/overlay.jsonl",
                    "--tasks-json",
                    "gs://bucket/output/tasks.json",
                    "--label-config-xml",
                    "gs://bucket/output/label_config.xml",
                    "--readme-md",
                    "gs://bucket/output/README.md",
                    "--preview-csv",
                    "gs://bucket/output/preview.csv",
                    "--limit",
                    "2",
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
        tasks = json.loads(
            uploaded_text_by_path["gs://bucket/output/tasks.json"]
        )
        readme = uploaded_text_by_path["gs://bucket/output/README.md"]
        preview_text = uploaded_text_by_path["gs://bucket/output/preview.csv"]
        preview_rows = list(csv.DictReader(io.StringIO(preview_text)))

        self.assertEqual(
            [task["data"]["audio_segment_id"] for task in tasks],
            ["audio-b", "audio-c"],
        )
        self.assertNotIn("prediction_text", json.dumps(tasks))
        self.assertNotIn("split", json.dumps(tasks))
        self.assertIn("Requested rows: 2", readme)
        self.assertIn("Packaged rows: 2", readme)
        self.assertIn("Already-reviewed rows skipped: 1", readme)
        self.assertIn("reference_transcript", preview_text)
        self.assertEqual(
            [row["audio_segment_id"] for row in preview_rows],
            ["audio-b", "audio-c"],
        )
        self.assertNotIn("prediction_text", preview_text)
        self.assertNotIn("split", preview_text)


if __name__ == "__main__":
    unittest.main()
