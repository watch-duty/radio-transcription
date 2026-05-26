"""Focused unit tests for the SFT eval scoring outputs."""

from __future__ import annotations

import argparse
import json
import sys
import tempfile
import types
import unittest
import unittest.mock
from pathlib import Path

_SFT_DIR = str(Path(__file__).resolve().parent.parent)
_COLABS_DIR = str(
    Path(__file__).resolve().parent.parent.parent.parent / "colabs"
)
if _SFT_DIR not in sys.path:
    sys.path.insert(0, _SFT_DIR)
if _COLABS_DIR not in sys.path:
    sys.path.insert(0, _COLABS_DIR)


class TestPipelineEvalKeywordMetrics(unittest.TestCase):
    def test_eval_records_base_keyword_metrics(self) -> None:
        import pipeline

        with tempfile.TemporaryDirectory() as tmp:
            results_dir = Path(tmp) / "results"
            round_dir = results_dir / "round-keywords"
            round_dir.mkdir(parents=True)
            (round_dir / "config.json").write_text(
                json.dumps(
                    {
                        "round_id": "round-keywords",
                        "datasets": ["echo"],
                        "system_prompt": "sys",
                        "user_prompt": "user",
                        "base_model": "gemini-2.5-flash",
                    }
                )
            )

            storage_client = unittest.mock.MagicMock()
            storage_client.bucket.return_value.list_blobs.return_value = [
                types.SimpleNamespace(name="out/predictions.jsonl")
            ]

            def fake_download_blob_to_file(
                _client, _bucket: str, _blob: str, local_path: str
            ) -> None:
                output = {
                    "request": {
                        "contents": [
                            {
                                "parts": [
                                    {
                                        "fileData": {
                                            "fileUri": "gs://bucket/a.flac"
                                        }
                                    }
                                ]
                            }
                        ]
                    },
                    "response": {
                        "candidates": [
                            {"content": {"parts": [{"text": "engine 41"}]}}
                        ]
                    },
                }
                Path(local_path).write_text(json.dumps(output) + "\n")

            with (
                unittest.mock.patch.object(
                    pipeline, "RESULTS_DIR", results_dir
                ),
                unittest.mock.patch.object(
                    pipeline,
                    "_load_registry",
                    return_value={
                        "datasets": {
                            "echo": {
                                "adapter": "gcs_manifest",
                                "eval_manifest_uri": "gs://bucket/eval.jsonl",
                            }
                        }
                    },
                ),
                unittest.mock.patch(
                    "google.cloud.storage.Client",
                    return_value=storage_client,
                ),
                unittest.mock.patch(
                    "common.gcs_utils.download_jsonl_manifest",
                    return_value=[
                        {
                            "audio_filepath": "gs://bucket/a.flac",
                            "text": "copy engine 41",
                            "duration": 5.0,
                        }
                    ],
                ),
                unittest.mock.patch("common.gcs_utils.upload_file_to_blob"),
                unittest.mock.patch(
                    "common.gcs_utils.download_blob_to_file",
                    side_effect=fake_download_blob_to_file,
                ),
                unittest.mock.patch(
                    "common.scoring.build_normalizer",
                    return_value=lambda text: text.lower(),
                ),
                unittest.mock.patch(
                    "common.vertex.submit_batch_inference",
                    return_value="gs://bucket/out/",
                ),
            ):
                rc = pipeline._eval(
                    argparse.Namespace(
                        round_id="round-keywords",
                        base_only=True,
                        location="us-central1",
                    )
                )

            self.assertEqual(rc, 0)
            metrics = json.loads((round_dir / "wer_summary.json").read_text())
            self.assertIn("base_keyword_metrics", metrics)
            self.assertEqual(metrics["base_keyword_accuracy"], 50.0)
            by_keyword = {
                row["keyword"]: row for row in metrics["base_keyword_metrics"]
            }
            self.assertEqual(by_keyword["copy"]["accuracy"], 0.0)
            self.assertEqual(by_keyword["engine"]["accuracy"], 100.0)


class TestWerSummaryKeywordMetrics(unittest.TestCase):
    def test_write_wer_summary_renders_keyword_accuracy(self) -> None:
        from records import write_wer_summary

        with tempfile.TemporaryDirectory() as tmp:
            results_dir = Path(tmp)
            write_wer_summary(
                results_dir,
                "round-keywords",
                {
                    "round_id": "round-keywords",
                    "base_wer": 10.0,
                    "base_cer": 5.0,
                    "base_keyword_accuracy": 50.0,
                    "base_keyword_metrics": [
                        {
                            "keyword": "copy",
                            "occurrences": 2,
                            "correctly_identified": 1,
                            "accuracy": 50.0,
                        }
                    ],
                },
            )

            summary = (
                results_dir / "round-keywords" / "wer_summary.md"
            ).read_text()

        self.assertIn("Keyword Accuracy", summary)
        self.assertIn("| copy | 2 | 50.00% |", summary)


if __name__ == "__main__":
    unittest.main()
