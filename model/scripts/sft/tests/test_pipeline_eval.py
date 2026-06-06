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

import pipeline  # noqa: E402
import records  # noqa: E402


class TestPipelineEvalKeywordMetrics(unittest.TestCase):
    def test_eval_records_base_keyword_metrics(self) -> None:
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
                _client: object, _bucket: str, _blob: str, local_path: str
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
                    "pipeline.storage.Client",
                    return_value=storage_client,
                ),
                unittest.mock.patch(
                    "pipeline.download_jsonl_manifest",
                    return_value=[
                        {
                            "audio_filepath": "gs://bucket/a.flac",
                            "text": "copy engine 41",
                            "duration": 5.0,
                        }
                    ],
                ),
                unittest.mock.patch("pipeline.upload_file_to_blob"),
                unittest.mock.patch(
                    "pipeline.download_blob_to_file",
                    side_effect=fake_download_blob_to_file,
                ),
                unittest.mock.patch(
                    "pipeline.build_normalizer",
                    return_value=lambda text: text.lower(),
                ),
                unittest.mock.patch(
                    "pipeline.submit_batch_inference",
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

    def test_eval_skips_malformed_batch_output_rows(self) -> None:
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
                _client: object, _bucket: str, _blob: str, local_path: str
            ) -> None:
                malformed_outputs = [
                    {
                        "request": {},
                        "response": {
                            "candidates": [
                                {"content": {"parts": [{"text": "engine 41"}]}}
                            ]
                        },
                    },
                    {
                        "request": {
                            "contents": [{"parts": [None]}],
                        },
                        "response": None,
                    },
                    {
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
                        "response": {"candidates": [None]},
                    },
                    {
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
                                {"content": {"parts": [None]}},
                            ]
                        },
                    },
                ]
                Path(local_path).write_text(
                    "\n".join(json.dumps(obj) for obj in malformed_outputs)
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
                    "pipeline.storage.Client",
                    return_value=storage_client,
                ),
                unittest.mock.patch(
                    "pipeline.download_jsonl_manifest",
                    return_value=[
                        {
                            "audio_filepath": "gs://bucket/a.flac",
                            "text": "copy engine 41",
                            "duration": 5.0,
                        }
                    ],
                ),
                unittest.mock.patch("pipeline.upload_file_to_blob"),
                unittest.mock.patch(
                    "pipeline.download_blob_to_file",
                    side_effect=fake_download_blob_to_file,
                ),
                unittest.mock.patch(
                    "pipeline.build_normalizer",
                    return_value=lambda text: text.lower(),
                ),
                unittest.mock.patch(
                    "pipeline.submit_batch_inference",
                    return_value="gs://bucket/out/",
                ),
            ):
                rc = pipeline._eval(
                    argparse.Namespace(
                        round_id="round-malformed",
                        base_only=True,
                        location="us-central1",
                    )
                )

            self.assertEqual(rc, 0)
            metrics = json.loads((round_dir / "wer_summary.json").read_text())
            self.assertEqual(metrics["n_eval_examples"], 1)
            self.assertEqual(metrics["base_empty_rate"], 100.0)

    def test_eval_duplicate_audio_uris_do_not_warn_missing_predictions(
        self,
    ) -> None:
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
                _client: object, _bucket: str, _blob: str, local_path: str
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
                    "pipeline.storage.Client",
                    return_value=storage_client,
                ),
                unittest.mock.patch(
                    "pipeline.download_jsonl_manifest",
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
                unittest.mock.patch("pipeline.upload_file_to_blob"),
                unittest.mock.patch(
                    "pipeline.download_blob_to_file",
                    side_effect=fake_download_blob_to_file,
                ),
                unittest.mock.patch(
                    "pipeline.build_normalizer",
                    return_value=lambda text: text.lower(),
                ),
                unittest.mock.patch(
                    "pipeline.submit_batch_inference",
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

    def test_eval_batch_submit_failure_returns_clean_error(self) -> None:
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
                unittest.mock.patch("pipeline.storage.Client"),
                unittest.mock.patch(
                    "pipeline.download_jsonl_manifest",
                    return_value=[
                        {
                            "audio_filepath": "gs://bucket/a.flac",
                            "text": "engine 41",
                            "duration": 5.0,
                        }
                    ],
                ),
                unittest.mock.patch("pipeline.upload_file_to_blob"),
                unittest.mock.patch(
                    "pipeline.build_normalizer",
                    return_value=lambda text: text.lower(),
                ),
                unittest.mock.patch(
                    "pipeline.submit_batch_inference",
                    side_effect=TimeoutError("batch timed out"),
                ),
                self.assertLogs("pipeline", level="ERROR") as logs,
            ):
                rc = pipeline._eval(
                    argparse.Namespace(
                        round_id="round-batch-fails",
                        base_only=True,
                        location="us-central1",
                    )
                )

        self.assertEqual(rc, 1)
        self.assertIn("[base] Batch inference failed", "\n".join(logs.output))


class TestWerSummaryKeywordMetrics(unittest.TestCase):
    def test_write_wer_summary_renders_keyword_accuracy(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            results_dir = Path(tmp)
            records.write_wer_summary(
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
