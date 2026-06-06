from __future__ import annotations

import argparse
import json
import tempfile
import types
import unittest
import unittest.mock
from pathlib import Path
from typing import Any

from gemini_sft import cli
from gemini_sft import evaluate as evaluate_module
from gemini_sft import tune as tune_module
from gemini_sft.config import load_run_config
from gemini_sft.prepare import prepare_run


def _split_gcs(uri: str) -> tuple[str, str]:
    assert uri.startswith("gs://")
    bucket, blob = uri[len("gs://") :].split("/", maxsplit=1)
    return bucket, blob


class FakeBlob:
    def __init__(
        self,
        store: dict[tuple[str, str], str],
        uploads: list[str],
        bucket: str,
        name: str,
    ) -> None:
        self._store = store
        self._uploads = uploads
        self._bucket = bucket
        self.name = name

    def exists(self, **_: Any) -> bool:
        return (self._bucket, self.name) in self._store

    def upload_from_filename(self, filename: str, **_: Any) -> None:
        self._store[(self._bucket, self.name)] = Path(filename).read_text(
            encoding="utf-8"
        )
        self._uploads.append(f"gs://{self._bucket}/{self.name}")

    def upload_from_string(
        self, data: str, content_type: str | None = None, **_: Any
    ) -> None:
        del content_type
        self._store[(self._bucket, self.name)] = data
        self._uploads.append(f"gs://{self._bucket}/{self.name}")

    def download_to_filename(self, filename: str, **_: Any) -> None:
        Path(filename).parent.mkdir(parents=True, exist_ok=True)
        Path(filename).write_text(
            self._store[(self._bucket, self.name)], encoding="utf-8"
        )

    def download_as_text(self, **_: Any) -> str:
        return self._store[(self._bucket, self.name)]


class FakeBucket:
    def __init__(
        self,
        store: dict[tuple[str, str], str],
        uploads: list[str],
        name: str,
    ) -> None:
        self._store = store
        self._uploads = uploads
        self.name = name

    def blob(self, name: str) -> FakeBlob:
        return FakeBlob(self._store, self._uploads, self.name, name)

    def list_blobs(
        self, prefix: str = "", max_results: int | None = None
    ) -> list[types.SimpleNamespace]:
        names = [
            blob_name
            for bucket_name, blob_name in self._store
            if bucket_name == self.name and blob_name.startswith(prefix)
        ]
        if max_results is not None:
            names = names[:max_results]
        return [types.SimpleNamespace(name=name) for name in names]


class FakeStorageClient:
    def __init__(self) -> None:
        self.store: dict[tuple[str, str], str] = {}
        self.uploads: list[str] = []

    def bucket(self, name: str) -> FakeBucket:
        return FakeBucket(self.store, self.uploads, name)

    def put(self, uri: str, text: str) -> None:
        self.store[_split_gcs(uri)] = text

    def get(self, uri: str) -> str:
        return self.store[_split_gcs(uri)]

    def has(self, uri: str) -> bool:
        return _split_gcs(uri) in self.store


def _manifest(rows: list[dict[str, Any]]) -> str:
    return "".join(json.dumps(row) + "\n" for row in rows)


def _row(
    uri: str, text: str = "alpha", duration: float = 3.0
) -> dict[str, Any]:
    return {
        "audio_filepath": uri,
        "text": text,
        "offset": 0.0,
        "duration": duration,
    }


def _config_text(round_id: str = "round-a") -> str:
    return f"""
round_id = "{round_id}"
dataset = "wd-internal-v1"
train_manifest_uri = "gs://source/manifests/train.jsonl"
validation_manifest_uri = "gs://source/manifests/validation.jsonl"
eval_manifest_uri = "gs://source/manifests/eval.jsonl"

[gcp]
project = "test-project"
bucket = "test-bucket"
location = "us-central1"

[sft]
base_model = "gemini-3.1-flash-lite"
epoch_count = 6
adapter_size = "SIXTEEN"
learning_rate_multiplier = 1.0
"""


def _write_config_file(tmp: Path, round_id: str = "round-a") -> Path:
    path = tmp / "run.toml"
    path.write_text(_config_text(round_id), encoding="utf-8")
    return path


def _seed_source_manifests(
    storage: FakeStorageClient,
    *,
    train_uri: str = "gs://audio/train.flac",
    validation_uri: str = "gs://audio/validation.flac",
    eval_uri: str = "gs://audio/eval.flac",
) -> None:
    storage.put(
        "gs://source/manifests/train.jsonl",
        _manifest([_row(train_uri, "train transcript", 4.0)]),
    )
    storage.put(
        "gs://source/manifests/validation.jsonl",
        _manifest([_row(validation_uri, "validation transcript", 5.0)]),
    )
    storage.put(
        "gs://source/manifests/eval.jsonl",
        _manifest([_row(eval_uri, "eval transcript", 6.0)]),
    )
    storage.put(train_uri, "audio")
    storage.put(validation_uri, "audio")
    storage.put(eval_uri, "audio")


class TestCli(unittest.TestCase):
    def test_help_lists_supported_commands_only(self) -> None:
        parser = cli.build_parser()
        choices = parser._subparsers._group_actions[0].choices

        self.assertEqual(set(choices), {"prepare", "tune", "eval"})

    def test_dispatches_prepare(self) -> None:
        with unittest.mock.patch(
            "gemini_sft.cli.prepare", return_value=0
        ) as mock:
            self.assertEqual(cli.main(["prepare", "--config", "run.toml"]), 0)

        mock.assert_called_once()


class TestPrepareRun(unittest.TestCase):
    def test_prepare_uploads_required_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_s:
            tmp = Path(tmp_s)
            storage = FakeStorageClient()
            _seed_source_manifests(storage)
            run_cfg = load_run_config(_write_config_file(tmp))

            artifacts, config = prepare_run(
                run_cfg=run_cfg,
                storage_client=storage,
                results_dir=tmp / "results",
            )

            self.assertEqual(config["status"], "preflight_passed")
            self.assertTrue(artifacts.gemini_train_path.exists())
            required = {
                "gs://test-bucket/sft/runs/round-a/run_config.toml",
                "gs://test-bucket/sft/runs/round-a/config.json",
                "gs://test-bucket/sft/runs/round-a/status.json",
                "gs://test-bucket/sft/runs/round-a/manifests/canonical/train.jsonl",
                "gs://test-bucket/sft/runs/round-a/manifests/canonical/validation.jsonl",
                "gs://test-bucket/sft/runs/round-a/manifests/canonical/eval.jsonl",
                "gs://test-bucket/sft/runs/round-a/model_inputs/gemini/train.jsonl",
                "gs://test-bucket/sft/runs/round-a/model_inputs/gemini/validation.jsonl",
                "gs://test-bucket/sft/runs/round-a/preflight/report.json",
                "gs://test-bucket/sft/runs/round-a/tuning/status.json",
                "gs://test-bucket/sft/runs/round-a/evals/README.txt",
            }
            self.assertTrue(required.issubset(set(storage.uploads)))

    def test_train_eval_overlap_fails_before_uploading_config(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_s:
            tmp = Path(tmp_s)
            storage = FakeStorageClient()
            _seed_source_manifests(
                storage,
                train_uri="gs://audio/shared.flac",
                eval_uri="gs://audio/shared.flac",
            )
            run_cfg = load_run_config(_write_config_file(tmp))

            with self.assertRaisesRegex(ValueError, "train and eval"):
                prepare_run(
                    run_cfg=run_cfg,
                    storage_client=storage,
                    results_dir=tmp / "results",
                )

        self.assertFalse(
            storage.has("gs://test-bucket/sft/runs/round-a/config.json")
        )


class TestTuneRun(unittest.TestCase):
    def test_existing_job_resumes_without_submit(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_s:
            tmp = Path(tmp_s)
            storage = FakeStorageClient()
            cfg_path = _write_config_file(tmp)
            run_cfg = load_run_config(cfg_path)
            storage.put(
                run_cfg.paths.config_uri,
                json.dumps(
                    {"job_name": "jobs/1", "base_model": run_cfg.base_model}
                ),
            )
            args = argparse.Namespace(config=str(cfg_path), confirm=True)

            with (
                unittest.mock.patch.object(
                    tune_module, "RESULTS_DIR", tmp / "results"
                ),
                unittest.mock.patch.object(
                    tune_module, "poll_tuning_job", return_value="endpoints/1"
                ) as poll,
                unittest.mock.patch.object(
                    tune_module, "submit_tuning_job"
                ) as submit,
            ):
                rc = tune_module.tune_run(
                    args=args,
                    run_cfg=run_cfg,
                    storage_client=storage,
                    results_dir=tmp / "results",
                )

        self.assertEqual(rc, 0)
        poll.assert_called_once_with("jobs/1", "test-project", "us-central1")
        submit.assert_not_called()

    def test_confirmation_decline_does_not_submit(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_s:
            tmp = Path(tmp_s)
            storage = FakeStorageClient()
            _seed_source_manifests(storage)
            cfg_path = _write_config_file(tmp)
            run_cfg = load_run_config(cfg_path)
            args = argparse.Namespace(config=str(cfg_path), confirm=False)

            with (
                unittest.mock.patch.object(
                    tune_module, "RESULTS_DIR", tmp / "results"
                ),
                unittest.mock.patch("builtins.input", side_effect=EOFError),
                unittest.mock.patch.object(
                    tune_module, "submit_tuning_job"
                ) as submit,
            ):
                rc = tune_module.tune_run(
                    args=args,
                    run_cfg=run_cfg,
                    storage_client=storage,
                    results_dir=tmp / "results",
                )

        self.assertEqual(rc, 130)
        submit.assert_not_called()


class TestEvaluateRun(unittest.TestCase):
    def test_eval_uses_shared_batch_parser_and_records_output_uri(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_s:
            tmp = Path(tmp_s)
            storage = FakeStorageClient()
            _seed_source_manifests(storage, eval_uri="gs://audio/eval.flac")
            cfg_path = _write_config_file(tmp)
            run_cfg = load_run_config(cfg_path)
            storage.put(
                run_cfg.paths.config_uri, json.dumps(run_cfg.to_record_dict())
            )
            output_uri = f"{run_cfg.paths.gcs_prefix}/evals/base/output/"
            pred_blob = f"{output_uri}predictions.jsonl"
            storage.put(
                pred_blob,
                json.dumps(
                    {
                        "request": {
                            "contents": [
                                {
                                    "parts": [
                                        {
                                            "fileData": {
                                                "fileUri": "gs://audio/eval.flac"
                                            }
                                        }
                                    ]
                                }
                            ]
                        },
                        "response": {
                            "candidates": [
                                {
                                    "content": {
                                        "parts": [{"text": "eval transcript"}]
                                    }
                                }
                            ]
                        },
                    }
                )
                + "\n",
            )
            args = argparse.Namespace(config=str(cfg_path), base_only=True)

            with (
                unittest.mock.patch.object(
                    evaluate_module, "RESULTS_DIR", tmp / "results"
                ),
                unittest.mock.patch.object(
                    evaluate_module,
                    "submit_batch_inference",
                    return_value=output_uri,
                ),
            ):
                rc = evaluate_module.evaluate_run(
                    args, run_cfg, storage, run_cfg.to_record_dict()
                )

            self.assertEqual(rc, 0)
            metrics = json.loads(
                (tmp / "results" / "round-a" / "wer_summary.json").read_text(
                    encoding="utf-8"
                )
            )
            self.assertEqual(metrics["base_batch_output_uri"], output_uri)
            self.assertEqual(metrics["base_wer"], 0.0)
