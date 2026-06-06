"""Tests for config-driven SFT tune orchestration.

All GCS and Vertex interactions are mocked; these tests must never submit a real
tuning job.
"""

from __future__ import annotations

import argparse
import contextlib
import io
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

    def exists(self, **_: object) -> bool:
        return (self._bucket, self.name) in self._store

    def upload_from_filename(self, filename: str, **_: object) -> None:
        self._store[(self._bucket, self.name)] = Path(filename).read_text(
            encoding="utf-8"
        )
        self._uploads.append(f"gs://{self._bucket}/{self.name}")

    def upload_from_string(
        self, data: str, content_type: str | None = None, **_: object
    ) -> None:
        del content_type
        self._store[(self._bucket, self.name)] = data
        self._uploads.append(f"gs://{self._bucket}/{self.name}")

    def download_to_filename(self, filename: str, **_: object) -> None:
        Path(filename).parent.mkdir(parents=True, exist_ok=True)
        Path(filename).write_text(
            self._store[(self._bucket, self.name)], encoding="utf-8"
        )

    def download_as_text(self, **_: object) -> str:
        return self._store[(self._bucket, self.name)]


class FakeBucket:
    def __init__(
        self, store: dict[tuple[str, str], str], uploads: list[str], name: str
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


def _manifest(rows: list[dict]) -> str:
    return "".join(json.dumps(row) + "\n" for row in rows)


def _row(uri: str, text: str = "alpha", duration: float = 3.0) -> dict:
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


def _fake_preflight(*_: object, **kwargs: object) -> types.SimpleNamespace:
    report_path = kwargs["report_path"]
    assert isinstance(report_path, Path)
    report_path.write_text(
        json.dumps({"passed": True, "failures": [], "offending_ids": []}),
        encoding="utf-8",
    )
    return types.SimpleNamespace(passed=True, failures=[])


class TestConfigTune(unittest.TestCase):
    def _run_tune(
        self,
        cfg_path: Path,
        storage: FakeStorageClient,
        results_dir: Path,
        *,
        submit_side_effect: object = "jobs/1",
        poll_return: str = "endpoints/1",
    ) -> tuple[int, unittest.mock.MagicMock, unittest.mock.MagicMock]:
        args = argparse.Namespace(
            config=str(cfg_path), confirm=True, provided_flags=set()
        )
        with (
            unittest.mock.patch.object(pipeline, "RESULTS_DIR", results_dir),
            unittest.mock.patch(
                "pipeline.storage.Client", return_value=storage
            ),
            unittest.mock.patch(
                "pipeline.run_preflight", side_effect=_fake_preflight
            ),
            unittest.mock.patch(
                "pipeline.submit_tuning_job",
                side_effect=(
                    submit_side_effect
                    if callable(submit_side_effect)
                    else None
                ),
                return_value=(
                    submit_side_effect
                    if not callable(submit_side_effect)
                    else unittest.mock.DEFAULT
                ),
            ) as mock_submit,
            unittest.mock.patch(
                "pipeline.poll_tuning_job", return_value=poll_return
            ) as mock_poll,
            contextlib.redirect_stdout(io.StringIO()),
        ):
            rc = pipeline._tune(args)
        return rc, mock_submit, mock_poll

    def test_fresh_config_uploads_required_artifacts_before_submit(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_s:
            tmp = Path(tmp_s)
            storage = FakeStorageClient()
            _seed_source_manifests(storage)
            cfg_path = _write_config_file(tmp)
            results_dir = tmp / "results"

            required_before_submit = {
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

            def submit_side_effect(**_: object) -> str:
                missing = [
                    uri
                    for uri in sorted(required_before_submit)
                    if not storage.has(uri)
                ]
                self.assertEqual(missing, [])
                return "jobs/1"

            rc, mock_submit, mock_poll = self._run_tune(
                cfg_path,
                storage,
                results_dir,
                submit_side_effect=submit_side_effect,
            )

            self.assertEqual(rc, 0)
            mock_submit.assert_called_once()
            mock_poll.assert_called_once_with(
                "jobs/1", "test-project", "us-central1"
            )
            for uri in required_before_submit:
                self.assertTrue(storage.has(uri), uri)
            self.assertTrue(
                (
                    results_dir
                    / "round-a"
                    / "manifests"
                    / "canonical"
                    / "train.jsonl"
                ).exists()
            )
            self.assertIn(
                '"fileUri": "gs://audio/train.flac"',
                storage.get(
                    "gs://test-bucket/sft/runs/round-a/model_inputs/gemini/train.jsonl"
                ),
            )

    def test_train_eval_overlap_returns_error_without_submit(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_s:
            tmp = Path(tmp_s)
            storage = FakeStorageClient()
            _seed_source_manifests(
                storage,
                train_uri="gs://audio/shared.flac",
                eval_uri="gs://audio/shared.flac",
            )
            cfg_path = _write_config_file(tmp)

            rc, mock_submit, _ = self._run_tune(
                cfg_path, storage, tmp / "results"
            )

            self.assertEqual(rc, 1)
            mock_submit.assert_not_called()

    def test_validation_eval_overlap_is_allowed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_s:
            tmp = Path(tmp_s)
            storage = FakeStorageClient()
            _seed_source_manifests(
                storage,
                validation_uri="gs://audio/shared.flac",
                eval_uri="gs://audio/shared.flac",
            )
            cfg_path = _write_config_file(tmp)

            rc, mock_submit, _ = self._run_tune(
                cfg_path, storage, tmp / "results"
            )

            self.assertEqual(rc, 0)
            mock_submit.assert_called_once()

    def test_prefix_without_job_name_returns_error_without_submit(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_s:
            tmp = Path(tmp_s)
            storage = FakeStorageClient()
            _seed_source_manifests(storage)
            storage.put(
                "gs://test-bucket/sft/runs/round-a/status.json",
                '{"status":"preflight_passed"}',
            )
            cfg_path = _write_config_file(tmp)

            rc, mock_submit, _ = self._run_tune(
                cfg_path, storage, tmp / "results"
            )

            self.assertEqual(rc, 1)
            mock_submit.assert_not_called()

    def test_gcs_config_with_job_name_resumes_without_submit(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_s:
            tmp = Path(tmp_s)
            storage = FakeStorageClient()
            _seed_source_manifests(storage)
            storage.put(
                "gs://test-bucket/sft/runs/round-a/config.json",
                json.dumps(
                    {
                        "round_id": "round-a",
                        "job_name": "jobs/existing",
                        "base_model": "gemini-3.1-flash-lite",
                    }
                ),
            )
            cfg_path = _write_config_file(tmp)

            rc, mock_submit, mock_poll = self._run_tune(
                cfg_path,
                storage,
                tmp / "results",
                poll_return="endpoints/existing",
            )

            self.assertEqual(rc, 0)
            mock_submit.assert_not_called()
            mock_poll.assert_called_once_with(
                "jobs/existing", "test-project", "us-central1"
            )
            updated = json.loads(
                storage.get(
                    "gs://test-bucket/sft/runs/round-a/config.json"
                )
            )
            self.assertEqual(updated["endpoint"], "endpoints/existing")
