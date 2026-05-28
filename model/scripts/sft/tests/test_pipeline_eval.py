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
    def test_eval_requires_endpoint_unless_base_only(self) -> None:
        import pipeline

        with tempfile.TemporaryDirectory() as tmp:
            results_dir = Path(tmp) / "results"
            round_dir = results_dir / "round-no-endpoint"
            round_dir.mkdir(parents=True)
            (round_dir / "config.json").write_text(
                json.dumps(
                    {
                        "round_id": "round-no-endpoint",
                        "datasets": ["echo"],
                        "system_prompt": "sys",
                        "user_prompt": "user",
                        "base_model": "gemini-3.1-flash-lite",
                    }
                )
            )

            with (
                unittest.mock.patch.object(
                    pipeline, "RESULTS_DIR", results_dir
                ),
                self.assertRaisesRegex(ValueError, "--base-only"),
            ):
                pipeline._eval(
                    argparse.Namespace(
                        round_id="round-no-endpoint",
                        base_only=False,
                        location="us-central1",
                    )
                )

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
                unittest.mock.patch("records._git_sha", return_value="abc123"),
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
            ledger = (results_dir / "ledger.md").read_text()
            self.assertIn("| round-keywords | echo | gemini-2.5-flash", ledger)
            self.assertIn("| abc123 |", ledger)

    def test_eval_fails_on_malformed_batch_output_rows(self) -> None:
        import pipeline

        with tempfile.TemporaryDirectory() as tmp:
            results_dir = Path(tmp) / "results"
            round_dir = results_dir / "round-malformed"
            round_dir.mkdir(parents=True)
            (round_dir / "config.json").write_text(
                json.dumps(
                    {
                        "round_id": "round-malformed",
                        "datasets": ["echo"],
                        "system_prompt": "sys",
                        "user_prompt": "user",
                        "base_model": "gemini-3.1-flash-lite",
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
                malformed_output = {
                    "request": {},
                    "response": {
                        "candidates": [
                            {"content": {"parts": [{"text": "engine 41"}]}}
                        ]
                    },
                }
                Path(local_path).write_text(json.dumps(malformed_output) + "\n")

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
                with self.assertRaisesRegex(
                    TypeError, "malformed request contents"
                ):
                    pipeline._eval(
                        argparse.Namespace(
                            round_id="round-malformed",
                            base_only=True,
                            location="us-central1",
                        )
                    )

    def test_eval_scores_missing_batch_predictions_as_empty(self) -> None:
        import pipeline

        with tempfile.TemporaryDirectory() as tmp:
            results_dir = Path(tmp) / "results"
            round_dir = results_dir / "round-missing-prediction"
            round_dir.mkdir(parents=True)
            (round_dir / "config.json").write_text(
                json.dumps(
                    {
                        "round_id": "round-missing-prediction",
                        "datasets": ["echo"],
                        "system_prompt": "sys",
                        "user_prompt": "user",
                        "base_model": "gemini-3.1-flash-lite",
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
                Path(local_path).write_text(
                    json.dumps({"status": {"code": 13, "message": "error"}})
                    + "\n"
                )

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
                self.assertLogs("pipeline", level="WARNING") as logs,
            ):
                rc = pipeline._eval(
                    argparse.Namespace(
                        round_id="round-missing-prediction",
                        base_only=True,
                        location="us-central1",
                    )
                )

            self.assertEqual(rc, 0)
            metrics = json.loads((round_dir / "wer_summary.json").read_text())
            self.assertEqual(metrics["n_eval_examples"], 1)
            self.assertEqual(metrics["base_empty_rate"], 100.0)
            self.assertIn(
                "unique segments returned no prediction", "\n".join(logs.output)
            )

    def test_eval_duplicate_audio_uris_do_not_warn_missing_predictions(
        self,
    ) -> None:
        import pipeline

        with tempfile.TemporaryDirectory() as tmp:
            results_dir = Path(tmp) / "results"
            round_dir = results_dir / "round-duplicates"
            round_dir.mkdir(parents=True)
            (round_dir / "config.json").write_text(
                json.dumps(
                    {
                        "round_id": "round-duplicates",
                        "datasets": ["echo"],
                        "system_prompt": "sys",
                        "user_prompt": "user",
                        "base_model": "gemini-3.1-flash-lite",
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
                            "text": "engine 41",
                            "duration": 5.0,
                        },
                        {
                            "audio_filepath": "gs://bucket/a.flac",
                            "text": "engine 41",
                            "duration": 5.0,
                        },
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
                self.assertNoLogs("pipeline", level="WARNING"),
            ):
                rc = pipeline._eval(
                    argparse.Namespace(
                        round_id="round-duplicates",
                        base_only=True,
                        location="us-central1",
                    )
                )

        self.assertEqual(rc, 0)

    def test_eval_batch_submit_failure_propagates(self) -> None:
        import pipeline

        with tempfile.TemporaryDirectory() as tmp:
            results_dir = Path(tmp) / "results"
            round_dir = results_dir / "round-batch-fails"
            round_dir.mkdir(parents=True)
            (round_dir / "config.json").write_text(
                json.dumps(
                    {
                        "round_id": "round-batch-fails",
                        "datasets": ["echo"],
                        "system_prompt": "sys",
                        "user_prompt": "user",
                        "base_model": "gemini-3.1-flash-lite",
                    }
                )
            )

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
                unittest.mock.patch("google.cloud.storage.Client"),
                unittest.mock.patch(
                    "common.gcs_utils.download_jsonl_manifest",
                    return_value=[
                        {
                            "audio_filepath": "gs://bucket/a.flac",
                            "text": "engine 41",
                            "duration": 5.0,
                        }
                    ],
                ),
                unittest.mock.patch("common.gcs_utils.upload_file_to_blob"),
                unittest.mock.patch(
                    "common.scoring.build_normalizer",
                    return_value=lambda text: text.lower(),
                ),
                unittest.mock.patch(
                    "common.vertex.submit_batch_inference",
                    side_effect=TimeoutError("batch timed out"),
                ),
            ):
                with self.assertRaisesRegex(TimeoutError, "batch timed out"):
                    pipeline._eval(
                        argparse.Namespace(
                            round_id="round-batch-fails",
                            base_only=True,
                            location="us-central1",
                        )
                    )


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
