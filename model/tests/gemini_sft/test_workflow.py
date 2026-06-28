from __future__ import annotations

import argparse
import json
import logging
import tempfile
import types
import unittest
import unittest.mock
from pathlib import Path
from typing import Any

from fake_gcs import FakeStorageClient
from gemini_sft import cli
from gemini_sft import evaluate as evaluate_module
from gemini_sft import tune as tune_module
from gemini_sft.config import load_run_config
from gemini_sft.prepare import prepare_run


def _manifest(rows: list[dict[str, Any]]) -> str:
    return "".join(json.dumps(row) + "\n" for row in rows)


def _row(
    uri: str,
    text: str = "alpha",
    duration: float = 3.0,
    *,
    example_id: str | None = None,
    segment_id: str = "001",
    offset: float = 0.0,
    split: str | None = None,
) -> dict[str, Any]:
    if example_id is None:
        example_id = uri.rsplit("/", maxsplit=1)[-1].removesuffix(".flac")
    row = {
        "audio_filepath": uri,
        "text": text,
        "offset": offset,
        "duration": duration,
        "example_id": example_id,
        "segment_id": segment_id,
    }
    if split is not None:
        row["split"] = split
    return row


def _config_text(
    round_id: str = "round-a", prior_context_count: int | None = None
) -> str:
    context = ""
    if prior_context_count is not None:
        context = f"""
[context]
prior_turn_count = {prior_context_count}
"""
    return f"""
round_id = "{round_id}"
dataset = "wd-internal-v1"
inference_dataset_slug = "echo/eval"
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
{context}

[[eval.models]]
label = "base"
model = "gemini-3.1-flash-lite"
"""


def _fake_wer(
    refs: list[str],
    hyps: list[str],
    normalizer: Any = None,
) -> dict[str, float | int]:
    del normalizer
    total_words = sum(len(ref.split()) for ref in refs)
    return {
        "wer": 0.0 if refs == hyps else 1.0,
        "hits": total_words if refs == hyps else 0,
        "substitutions": 0 if refs == hyps else total_words,
        "deletions": 0,
        "insertions": 0,
    }


def _fake_cer(
    refs: list[str],
    hyps: list[str],
    normalizer: Any = None,
) -> dict[str, float]:
    del normalizer
    return {"cer": 0.0 if refs == hyps else 1.0}


def _patched_eval_scoring() -> Any:
    return unittest.mock.patch.multiple(
        evaluate_module,
        build_normalizer=lambda: None,
        compute_wer=_fake_wer,
        compute_cer=_fake_cer,
        duration_bucket_wer=lambda *_, **__: [],
        keyword_metrics=lambda *_, **__: [],
    )


def _write_config_file(
    tmp: Path,
    round_id: str = "round-a",
    prior_context_count: int | None = None,
) -> Path:
    path = tmp / "run.toml"
    path.write_text(
        _config_text(round_id, prior_context_count),
        encoding="utf-8",
    )
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


def _assert_no_prepared_outputs(
    test_case: unittest.TestCase,
    tmp: Path,
    storage: FakeStorageClient,
) -> None:
    gemini_dir = tmp / "results" / "round-a" / "model_inputs" / "gemini"
    test_case.assertFalse((gemini_dir / "train.jsonl").exists())
    test_case.assertFalse((gemini_dir / "validation.jsonl").exists())
    test_case.assertFalse(
        storage.has("gs://test-bucket/sft/runs/round-a/config.json")
    )


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

    def test_quiets_dependency_http_loggers(self) -> None:
        loggers: dict[str, unittest.mock.Mock] = {}

        def fake_get_logger(name: str | None = None) -> unittest.mock.Mock:
            key = "" if name is None else name
            return loggers.setdefault(key, unittest.mock.Mock())

        with (
            unittest.mock.patch("gemini_sft.cli.prepare", return_value=0),
            unittest.mock.patch("logging.getLogger", side_effect=fake_get_logger),
        ):
            self.assertEqual(cli.main(["prepare", "--config", "run.toml"]), 0)

        expected_levels = {
            "httpx": logging.WARNING,
            "httpcore": logging.WARNING,
            "google.auth.transport.requests": logging.WARNING,
            "urllib3.connectionpool": logging.ERROR,
        }
        for logger_name, level in expected_levels.items():
            loggers[logger_name].setLevel.assert_called_once_with(level)


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

    def test_prepare_uploads_config_after_inputs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_s:
            tmp = Path(tmp_s)
            storage = FakeStorageClient()
            _seed_source_manifests(storage)
            run_cfg = load_run_config(_write_config_file(tmp))

            prepare_run(
                run_cfg=run_cfg,
                storage_client=storage,
                results_dir=tmp / "results",
            )

            config_index = storage.uploads.index(
                "gs://test-bucket/sft/runs/round-a/config.json"
            )
            prerequisite_indexes = [
                storage.uploads.index(uri)
                for uri in [
                    "gs://test-bucket/sft/runs/round-a/run_config.toml",
                    "gs://test-bucket/sft/runs/round-a/manifests/canonical/train.jsonl",
                    "gs://test-bucket/sft/runs/round-a/manifests/canonical/validation.jsonl",
                    "gs://test-bucket/sft/runs/round-a/manifests/canonical/eval.jsonl",
                    "gs://test-bucket/sft/runs/round-a/model_inputs/gemini/train.jsonl",
                    "gs://test-bucket/sft/runs/round-a/model_inputs/gemini/validation.jsonl",
                    "gs://test-bucket/sft/runs/round-a/preflight/report.json",
                ]
            ]
            self.assertGreater(config_index, max(prerequisite_indexes))

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

    def test_prepare_rejects_invalid_or_empty_manifests_before_gemini_jsonl(
        self,
    ) -> None:
        invalid_manifest = _manifest(
            [
                {
                    "audio_filepath": "local/audio.mp3",
                    "text": "bad",
                    "offset": 0.0,
                    "duration": 1.0,
                    "example_id": "bad",
                    "segment_id": "001",
                }
            ]
        )
        cases = [
            (
                "train",
                "invalid",
                invalid_manifest,
                "Canonical Manifest validation failed",
            ),
            (
                "validation",
                "invalid",
                invalid_manifest,
                "Canonical Manifest validation failed",
            ),
            (
                "eval",
                "invalid",
                invalid_manifest,
                "Canonical Manifest validation failed",
            ),
            ("train", "empty", "", "train manifest has zero parsed rows"),
            (
                "validation",
                "empty",
                "",
                "validation manifest has zero parsed rows",
            ),
            ("eval", "empty", "", "eval manifest has zero parsed rows"),
        ]
        manifest_uris = {
            "train": "gs://source/manifests/train.jsonl",
            "validation": "gs://source/manifests/validation.jsonl",
            "eval": "gs://source/manifests/eval.jsonl",
        }

        for role, mode, content, message in cases:
            with self.subTest(role=role, mode=mode):
                with tempfile.TemporaryDirectory() as tmp_s:
                    tmp = Path(tmp_s)
                    storage = FakeStorageClient()
                    _seed_source_manifests(storage)
                    storage.put(manifest_uris[role], content)
                    run_cfg = load_run_config(_write_config_file(tmp))

                    with self.assertRaisesRegex(ValueError, message):
                        prepare_run(
                            run_cfg=run_cfg,
                            storage_client=storage,
                            results_dir=tmp / "results",
                        )

                    _assert_no_prepared_outputs(self, tmp, storage)

    def test_train_eval_uri_and_identity_overlap_reports_both_categories(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp_s:
            tmp = Path(tmp_s)
            storage = FakeStorageClient()
            _seed_source_manifests(storage)
            storage.put(
                "gs://source/manifests/train.jsonl",
                _manifest(
                    [
                        _row(
                            "gs://audio/shared.flac",
                            "shared audio train",
                            4.0,
                            example_id="train-audio",
                            segment_id="001",
                        ),
                        _row(
                            "gs://audio/train-identity.flac",
                            "shared identity train",
                            4.0,
                            example_id="shared-example",
                            segment_id="seg-001",
                        ),
                    ]
                ),
            )
            storage.put(
                "gs://source/manifests/eval.jsonl",
                _manifest(
                    [
                        _row(
                            "gs://audio/shared.flac",
                            "shared audio eval",
                            6.0,
                            example_id="eval-audio",
                            segment_id="001",
                        ),
                        _row(
                            "gs://audio/eval-identity.flac",
                            "shared identity eval",
                            6.0,
                            example_id="shared-example",
                            segment_id="seg-001",
                        ),
                    ]
                ),
            )
            run_cfg = load_run_config(_write_config_file(tmp))

            with self.assertRaisesRegex(ValueError, "train and eval") as ctx:
                prepare_run(
                    run_cfg=run_cfg,
                    storage_client=storage,
                    results_dir=tmp / "results",
                )

            message = str(ctx.exception)
            self.assertIn("audio URI(s)", message)
            self.assertIn("identity value(s)", message)
            _assert_no_prepared_outputs(self, tmp, storage)

    def test_prepare_rejects_mismatched_split_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_s:
            tmp = Path(tmp_s)
            storage = FakeStorageClient()
            _seed_source_manifests(storage)
            storage.put(
                "gs://source/manifests/train.jsonl",
                _manifest(
                    [
                        _row(
                            "gs://audio/train.flac",
                            "train transcript",
                            4.0,
                            split="eval",
                        )
                    ]
                ),
            )
            storage.put(
                "gs://source/manifests/validation.jsonl",
                _manifest(
                    [
                        _row(
                            "gs://audio/validation.flac",
                            "validation transcript",
                            5.0,
                            split="train",
                        )
                    ]
                ),
            )
            storage.put(
                "gs://source/manifests/eval.jsonl",
                _manifest(
                    [
                        _row(
                            "gs://audio/eval.flac",
                            "eval transcript",
                            6.0,
                            split="validation",
                        )
                    ]
                ),
            )
            run_cfg = load_run_config(_write_config_file(tmp))

            with self.assertRaisesRegex(
                ValueError, "Canonical Manifest validation failed"
            ):
                prepare_run(
                    run_cfg=run_cfg,
                    storage_client=storage,
                    results_dir=tmp / "results",
                )

            _assert_no_prepared_outputs(self, tmp, storage)

    def test_train_eval_identity_overlap_fails_before_writing_gemini_jsonl(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp_s:
            tmp = Path(tmp_s)
            storage = FakeStorageClient()
            _seed_source_manifests(
                storage,
                train_uri="gs://audio/train.flac",
                eval_uri="gs://audio/eval.flac",
            )
            storage.put(
                "gs://source/manifests/train.jsonl",
                _manifest(
                    [
                        _row(
                            "gs://audio/train.flac",
                            "train transcript",
                            4.0,
                            example_id="shared-example",
                            segment_id="seg-001",
                        )
                    ]
                ),
            )
            storage.put(
                "gs://source/manifests/eval.jsonl",
                _manifest(
                    [
                        _row(
                            "gs://audio/eval.flac",
                            "eval transcript",
                            6.0,
                            example_id="shared-example",
                            segment_id="seg-001",
                        )
                    ]
                ),
            )
            run_cfg = load_run_config(_write_config_file(tmp))

            with self.assertRaisesRegex(ValueError, "identity"):
                prepare_run(
                    run_cfg=run_cfg,
                    storage_client=storage,
                    results_dir=tmp / "results",
                )

            gemini_dir = tmp / "results" / "round-a" / "model_inputs"
            self.assertFalse((gemini_dir / "gemini" / "train.jsonl").exists())
            self.assertFalse(
                (gemini_dir / "gemini" / "validation.jsonl").exists()
            )
            self.assertFalse(
                storage.has("gs://test-bucket/sft/runs/round-a/config.json")
            )

    def test_train_validation_identity_overlap_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_s:
            tmp = Path(tmp_s)
            storage = FakeStorageClient()
            _seed_source_manifests(
                storage,
                train_uri="gs://audio/train.flac",
                validation_uri="gs://audio/validation.flac",
            )
            storage.put(
                "gs://source/manifests/train.jsonl",
                _manifest(
                    [
                        _row(
                            "gs://audio/train.flac",
                            "train transcript",
                            4.0,
                            example_id="shared-example",
                            segment_id="seg-001",
                        )
                    ]
                ),
            )
            storage.put(
                "gs://source/manifests/validation.jsonl",
                _manifest(
                    [
                        _row(
                            "gs://audio/validation.flac",
                            "validation transcript",
                            5.0,
                            example_id="shared-example",
                            segment_id="seg-001",
                        )
                    ]
                ),
            )
            run_cfg = load_run_config(_write_config_file(tmp))

            with self.assertRaisesRegex(
                ValueError,
                "train and validation.*identity",
            ):
                prepare_run(
                    run_cfg=run_cfg,
                    storage_client=storage,
                    results_dir=tmp / "results",
                )

    def test_validation_eval_overlap_is_allowed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_s:
            tmp = Path(tmp_s)
            storage = FakeStorageClient()
            _seed_source_manifests(
                storage,
                validation_uri="gs://audio/shared.flac",
                eval_uri="gs://audio/shared.flac",
            )
            run_cfg = load_run_config(_write_config_file(tmp))

            _, config = prepare_run(
                run_cfg=run_cfg,
                storage_client=storage,
                results_dir=tmp / "results",
            )

        self.assertEqual(config["status"], "preflight_passed")
        self.assertTrue(
            storage.has("gs://test-bucket/sft/runs/round-a/config.json")
        )

    def test_prepare_builds_same_source_prior_text_turn_context_examples(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp_s:
            tmp = Path(tmp_s)
            storage = FakeStorageClient()
            train_rows = [
                {
                    **_row(
                        "gs://audio/source-a/001.flac",
                        "first",
                        example_id="source-a",
                        segment_id="001",
                        offset=0.0,
                    ),
                    "original_audio_uri": "gs://audio/source-a.flac",
                    "original_offset": 0.0,
                    "row_index": 1,
                },
                {
                    **_row(
                        "gs://audio/source-a/002.flac",
                        "second",
                        example_id="source-a",
                        segment_id="002",
                        offset=1.0,
                    ),
                    "original_audio_uri": "gs://audio/source-a.flac",
                    "original_offset": 1.0,
                    "row_index": 2,
                },
            ]
            storage.put(
                "gs://source/manifests/train.jsonl",
                _manifest(train_rows),
            )
            storage.put(
                "gs://source/manifests/validation.jsonl",
                _manifest(
                    [
                        _row(
                            "gs://audio/validation.flac",
                            "validation transcript",
                            5.0,
                        )
                    ]
                ),
            )
            storage.put(
                "gs://source/manifests/eval.jsonl",
                _manifest(
                    [
                        _row(
                            "gs://audio/eval.flac",
                            "eval transcript",
                            6.0,
                        )
                    ]
                ),
            )
            for uri in (
                "gs://audio/source-a/001.flac",
                "gs://audio/source-a/002.flac",
                "gs://audio/validation.flac",
                "gs://audio/eval.flac",
            ):
                storage.put(uri, "audio")
            run_cfg = load_run_config(
                _write_config_file(tmp, prior_context_count=8)
            )

            _, config = prepare_run(
                run_cfg=run_cfg,
                storage_client=storage,
                results_dir=tmp / "results",
            )

            train_examples = [
                json.loads(line)
                for line in storage.get(
                    run_cfg.paths.gemini_train_uri
                ).splitlines()
                if line.strip()
            ]

        self.assertEqual(config["status"], "preflight_passed")
        self.assertEqual(config["prior_context_count"], 8)
        self.assertEqual(len(train_examples[0]["contents"]), 2)
        second_contents = train_examples[1]["contents"]
        self.assertEqual(
            [turn["role"] for turn in second_contents],
            ["user", "model", "user", "model"],
        )
        self.assertNotIn("fileData", second_contents[0]["parts"][0])
        self.assertEqual(second_contents[1]["parts"][0]["text"], "first")
        current_user_parts = second_contents[2]["parts"]
        self.assertEqual(
            second_contents[0]["parts"][0]["text"],
            current_user_parts[0]["text"],
        )
        self.assertEqual(
            current_user_parts[1]["fileData"]["fileUri"],
            "gs://audio/source-a/002.flac",
        )
        self.assertEqual(second_contents[3]["parts"][0]["text"], "second")


class TestTuneRun(unittest.TestCase):
    def test_existing_job_resumes_without_submit(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_s:
            tmp = Path(tmp_s)
            storage = FakeStorageClient()
            cfg_path = _write_config_file(tmp)
            run_cfg = load_run_config(cfg_path)
            storage.put(
                run_cfg.paths.config_uri,
                json.dumps({**run_cfg.to_record_dict(), "job_name": "jobs/1"}),
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

    def test_existing_prepared_config_drives_tune_submission(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_s:
            tmp = Path(tmp_s)
            storage = FakeStorageClient()
            cfg_path = _write_config_file(tmp)
            run_cfg = load_run_config(cfg_path)
            prepared_config = {
                **run_cfg.to_record_dict(),
                "status": "preflight_passed",
                "base_model": "gemini-2.5-flash",
                "epoch_count": 3,
                "adapter_size": "FOUR",
                "learning_rate_multiplier": 0.5,
                "gemini_train_uri": "gs://prepared/train.jsonl",
                "gemini_validation_uri": "gs://prepared/validation.jsonl",
                "canonical_train_rows": 10,
                "total_train_duration_seconds": 30.0,
            }
            storage.put(run_cfg.paths.config_uri, json.dumps(prepared_config))
            args = argparse.Namespace(config=str(cfg_path), confirm=True)

            with (
                unittest.mock.patch.object(
                    tune_module, "RESULTS_DIR", tmp / "results"
                ),
                unittest.mock.patch.object(
                    tune_module, "submit_tuning_job", return_value="jobs/1"
                ) as submit,
                unittest.mock.patch.object(
                    tune_module, "poll_tuning_job", return_value="endpoints/1"
                ),
            ):
                rc = tune_module.tune_run(
                    args=args,
                    run_cfg=run_cfg,
                    storage_client=storage,
                    results_dir=tmp / "results",
                )

        self.assertEqual(rc, 0)
        kwargs = submit.call_args.kwargs
        self.assertEqual(kwargs["train_uri"], "gs://prepared/train.jsonl")
        self.assertEqual(kwargs["val_uri"], "gs://prepared/validation.jsonl")
        self.assertEqual(kwargs["base_model"], "gemini-2.5-flash")
        self.assertEqual(kwargs["epoch_count"], 3)
        self.assertEqual(kwargs["adapter_size"], "FOUR")
        self.assertEqual(kwargs["lr_multiplier"], 0.5)

    def test_existing_prepared_config_drives_continuous_tune_submission(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp_s:
            tmp = Path(tmp_s)
            storage = FakeStorageClient()
            cfg_path = _write_config_file(tmp)
            run_cfg = load_run_config(cfg_path)
            prepared_config = {
                **run_cfg.to_record_dict(),
                "status": "preflight_passed",
                "base_model": "gemini-3.1-flash-lite",
                "continuous_tuned_model_name": (
                    "projects/p/locations/us/models/123@1"
                ),
                "continuous_checkpoint_id": "7",
                "epoch_count": 4,
                "adapter_size": "SIXTEEN",
                "learning_rate_multiplier": 0.5,
                "gemini_train_uri": "gs://prepared/train.jsonl",
                "gemini_validation_uri": "gs://prepared/validation.jsonl",
                "canonical_train_rows": 10,
                "total_train_duration_seconds": 30.0,
            }
            storage.put(run_cfg.paths.config_uri, json.dumps(prepared_config))
            args = argparse.Namespace(config=str(cfg_path), confirm=True)

            with (
                unittest.mock.patch.object(
                    tune_module, "RESULTS_DIR", tmp / "results"
                ),
                unittest.mock.patch.object(
                    tune_module, "submit_tuning_job", return_value="jobs/1"
                ) as submit,
                unittest.mock.patch.object(
                    tune_module, "poll_tuning_job", return_value="endpoints/1"
                ),
            ):
                rc = tune_module.tune_run(
                    args=args,
                    run_cfg=run_cfg,
                    storage_client=storage,
                    results_dir=tmp / "results",
                )

        self.assertEqual(rc, 0)
        kwargs = submit.call_args.kwargs
        self.assertEqual(
            kwargs["base_model"], "projects/p/locations/us/models/123@1"
        )
        self.assertEqual(kwargs["pre_tuned_model_checkpoint_id"], "7")
        self.assertEqual(kwargs["epoch_count"], 4)

    def test_tune_validates_prepared_config_model_not_local_toml(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_s:
            tmp = Path(tmp_s)
            storage = FakeStorageClient()
            cfg_path = tmp / "run.toml"
            cfg_path.write_text(
                _config_text().replace(
                    'base_model = "gemini-3.1-flash-lite"',
                    'base_model = "local-edited-model"',
                ),
                encoding="utf-8",
            )
            run_cfg = load_run_config(cfg_path)
            prepared_config = {
                **run_cfg.to_record_dict(),
                "status": "preflight_passed",
                "base_model": "gemini-3.1-flash-lite",
                "canonical_train_rows": 10,
                "total_train_duration_seconds": 30.0,
            }
            storage.put(run_cfg.paths.config_uri, json.dumps(prepared_config))
            args = argparse.Namespace(config=str(cfg_path), confirm=True)

            with (
                unittest.mock.patch.object(
                    tune_module.storage, "Client", return_value=storage
                ),
                unittest.mock.patch.object(
                    tune_module, "RESULTS_DIR", tmp / "results"
                ),
                unittest.mock.patch.object(
                    tune_module, "submit_tuning_job", return_value="jobs/1"
                ) as submit,
                unittest.mock.patch.object(
                    tune_module, "poll_tuning_job", return_value="endpoints/1"
                ),
            ):
                rc = tune_module.tune(args)

        self.assertEqual(rc, 0)
        self.assertEqual(
            submit.call_args.kwargs["base_model"], "gemini-3.1-flash-lite"
        )

    def test_prepared_config_requires_canonical_gemini_input_keys(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp_s:
            tmp = Path(tmp_s)
            storage = FakeStorageClient()
            cfg_path = _write_config_file(tmp)
            run_cfg = load_run_config(cfg_path)
            prepared_config = {
                **run_cfg.to_record_dict(),
                "status": "preflight_passed",
                "base_model": "gemini-3.1-flash-lite",
                "canonical_train_rows": 10,
                "total_train_duration_seconds": 30.0,
            }
            prepared_config.pop("gemini_train_uri")
            storage.put(run_cfg.paths.config_uri, json.dumps(prepared_config))
            args = argparse.Namespace(config=str(cfg_path), confirm=True)

            with (
                unittest.mock.patch.object(
                    tune_module, "RESULTS_DIR", tmp / "results"
                ),
                unittest.mock.patch.object(
                    tune_module, "submit_tuning_job"
                ) as submit,
            ):
                with self.assertRaisesRegex(ValueError, "gemini_train_uri"):
                    tune_module.tune_run(
                        args=args,
                        run_cfg=run_cfg,
                        storage_client=storage,
                        results_dir=tmp / "results",
                    )

        submit.assert_not_called()

    def test_tune_handler_returns_clean_error_when_vertex_extra_missing(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp_s:
            tmp = Path(tmp_s)
            storage = FakeStorageClient()
            cfg_path = _write_config_file(tmp)
            run_cfg = load_run_config(cfg_path)
            prepared_config = {
                **run_cfg.to_record_dict(),
                "status": "preflight_passed",
                "canonical_train_rows": 1,
                "total_train_duration_seconds": 3.0,
            }
            storage.put(run_cfg.paths.config_uri, json.dumps(prepared_config))
            args = argparse.Namespace(config=str(cfg_path), confirm=True)

            with (
                unittest.mock.patch.object(
                    tune_module.storage, "Client", return_value=storage
                ),
                unittest.mock.patch.object(
                    tune_module, "RESULTS_DIR", tmp / "results"
                ),
                unittest.mock.patch.object(
                    tune_module,
                    "submit_tuning_job",
                    side_effect=ImportError("missing vertex"),
                ),
            ):
                rc = tune_module.tune(args)

        self.assertEqual(rc, 1)

    def test_missing_duration_is_rejected_before_submit(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp_s:
            tmp = Path(tmp_s)
            storage = FakeStorageClient()
            cfg_path = _write_config_file(tmp)
            run_cfg = load_run_config(cfg_path)
            prepared_config = {
                **run_cfg.to_record_dict(),
                "status": "preflight_passed",
                "total_train_duration_seconds": None,
            }
            storage.put(run_cfg.paths.config_uri, json.dumps(prepared_config))
            args = argparse.Namespace(config=str(cfg_path), confirm=True)

            with (
                unittest.mock.patch.object(
                    tune_module, "RESULTS_DIR", tmp / "results"
                ),
                unittest.mock.patch.object(
                    tune_module, "submit_tuning_job", return_value="jobs/1"
                ) as submit,
                unittest.mock.patch.object(
                    tune_module, "poll_tuning_job"
                ) as poll,
            ):
                with self.assertRaisesRegex(
                    TypeError, "total_train_duration_seconds"
                ):
                    tune_module.tune_run(
                        args=args,
                        run_cfg=run_cfg,
                        storage_client=storage,
                        results_dir=tmp / "results",
                    )

        submit.assert_not_called()
        poll.assert_not_called()


class TestEvaluateRun(unittest.TestCase):
    def test_eval_rejects_invalid_eval_manifest_before_batch_inference(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp_s:
            tmp = Path(tmp_s)
            storage = FakeStorageClient()
            _seed_source_manifests(storage)
            cfg_path = _write_config_file(tmp)
            run_cfg = load_run_config(cfg_path)
            storage.put(
                run_cfg.paths.canonical_eval_uri,
                _manifest(
                    [
                        {
                            "audio_filepath": "local/audio.mp3",
                            "text": "bad",
                            "offset": 0.0,
                            "duration": 1.0,
                            "example_id": "bad",
                            "segment_id": "001",
                        }
                    ]
                ),
            )
            config = run_cfg.to_record_dict()
            storage.put(run_cfg.paths.config_uri, json.dumps(config))
            args = argparse.Namespace(config=str(cfg_path), base_only=True)

            with unittest.mock.patch.object(
                evaluate_module,
                "submit_batch_inference",
            ) as submit:
                with self.assertRaisesRegex(
                    ValueError,
                    "Canonical Manifest validation failed",
                ):
                    evaluate_module.evaluate_run(
                        args,
                        run_cfg,
                        storage,
                        config,
                    )

        submit.assert_not_called()

    def test_eval_rejects_empty_eval_manifest_before_batch_inference(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp_s:
            tmp = Path(tmp_s)
            storage = FakeStorageClient()
            _seed_source_manifests(storage)
            cfg_path = _write_config_file(tmp)
            run_cfg = load_run_config(cfg_path)
            storage.put(run_cfg.paths.canonical_eval_uri, "")
            config = run_cfg.to_record_dict()
            storage.put(run_cfg.paths.config_uri, json.dumps(config))
            args = argparse.Namespace(config=str(cfg_path), base_only=True)

            with unittest.mock.patch.object(
                evaluate_module,
                "submit_batch_inference",
            ) as submit:
                with self.assertRaisesRegex(
                    ValueError,
                    "eval manifest has zero parsed rows",
                ):
                    evaluate_module.evaluate_run(
                        args,
                        run_cfg,
                        storage,
                        config,
                    )

        submit.assert_not_called()

    def test_eval_handler_returns_clean_error_when_vertex_extra_missing(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp_s:
            tmp = Path(tmp_s)
            storage = FakeStorageClient()
            _seed_source_manifests(storage, eval_uri="gs://audio/eval.flac")
            cfg_path = _write_config_file(tmp)
            run_cfg = load_run_config(cfg_path)
            storage.put(
                run_cfg.paths.canonical_eval_uri,
                _manifest([_row("gs://audio/eval.flac", "eval transcript")]),
            )
            storage.put(
                run_cfg.paths.config_uri, json.dumps(run_cfg.to_record_dict())
            )
            args = argparse.Namespace(config=str(cfg_path), base_only=True)

            with (
                unittest.mock.patch.object(
                    evaluate_module.storage, "Client", return_value=storage
                ),
                unittest.mock.patch.object(
                    evaluate_module, "RESULTS_DIR", tmp / "results"
                ),
                unittest.mock.patch.object(
                    evaluate_module,
                    "submit_batch_inference",
                    side_effect=ImportError("missing vertex"),
                ),
            ):
                rc = evaluate_module.evaluate(args)

        self.assertEqual(rc, 1)

    def test_eval_uses_shared_batch_parser_and_records_output_uri(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_s:
            tmp = Path(tmp_s)
            storage = FakeStorageClient()
            _seed_source_manifests(storage, eval_uri="gs://audio/eval.flac")
            cfg_path = _write_config_file(tmp)
            run_cfg = load_run_config(cfg_path)
            storage.put(
                run_cfg.paths.canonical_eval_uri,
                _manifest([_row("gs://audio/eval.flac", "eval transcript")]),
            )
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
                _patched_eval_scoring(),
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
            self.assertIn("targets", metrics)
            base_target = metrics["targets"][0]
            self.assertEqual(base_target["target_label"], "base")
            artifacts = base_target["artifacts"]
            self.assertEqual(artifacts["raw_output_uri"], output_uri)
            self.assertEqual(
                artifacts["normalized_manifest_uri"],
                "gs://test-bucket/inference_manifests/echo/eval/"
                "gemini_3_1_flash_lite/round-a/base.jsonl",
            )
            self.assertEqual(base_target["wer"], 0.0)
            manifest_rows = [
                json.loads(line)
                for line in storage.get(
                    artifacts["normalized_manifest_uri"]
                ).splitlines()
            ]
            self.assertEqual(
                manifest_rows[0]["pred_text_gemini_3_1_flash_lite"],
                "eval transcript",
            )

    def test_eval_builds_prior_context_batch_requests(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_s:
            tmp = Path(tmp_s)
            storage = FakeStorageClient()
            cfg_path = _write_config_file(tmp, prior_context_count=1)
            run_cfg = load_run_config(cfg_path)
            eval_rows = [
                {
                    **_row(
                        "gs://audio/eval-1.flac",
                        "first",
                        example_id="source-a",
                        segment_id="001",
                        offset=0.0,
                    ),
                    "original_audio_uri": "gs://audio/source-a.flac",
                    "original_offset": 0.0,
                    "row_index": 1,
                },
                {
                    **_row(
                        "gs://audio/eval-2.flac",
                        "second",
                        example_id="source-a",
                        segment_id="002",
                        offset=1.0,
                    ),
                    "original_audio_uri": "gs://audio/source-a.flac",
                    "original_offset": 1.0,
                    "row_index": 2,
                },
            ]
            storage.put(run_cfg.paths.canonical_eval_uri, _manifest(eval_rows))
            storage.put(
                run_cfg.paths.config_uri, json.dumps(run_cfg.to_record_dict())
            )
            output_uri = f"{run_cfg.paths.gcs_prefix}/evals/base/output/"
            storage.put(
                f"{output_uri}predictions.jsonl",
                "\n".join(
                    [
                        json.dumps(
                            {
                                "request": {
                                    "contents": [
                                        {
                                            "parts": [
                                                {
                                                    "fileData": {
                                                        "fileUri": "gs://audio/eval-1.flac"
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
                                                "parts": [{"text": "first"}]
                                            }
                                        }
                                    ]
                                },
                            }
                        ),
                        json.dumps(
                            {
                                "request": {
                                    "contents": [
                                        {
                                            "parts": [
                                                {
                                                    "fileData": {
                                                        "fileUri": "gs://audio/eval-2.flac"
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
                                                "parts": [{"text": "second"}]
                                            }
                                        }
                                    ]
                                },
                            }
                        ),
                    ]
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
                _patched_eval_scoring(),
            ):
                rc = evaluate_module.evaluate_run(
                    args,
                    run_cfg,
                    storage,
                    run_cfg.to_record_dict(),
                )

            batch_rows = [
                json.loads(line)
                for line in storage.get(
                    f"{run_cfg.paths.gcs_prefix}/evals/base/input.jsonl"
                ).splitlines()
                if line.strip()
            ]

        self.assertEqual(rc, 0)
        contents = batch_rows[1]["request"]["contents"]
        self.assertEqual(
            [turn["role"] for turn in contents],
            ["user", "model", "user"],
        )
        self.assertEqual(contents[1]["parts"][0]["text"], "first")
        current_user_parts = contents[2]["parts"]
        self.assertEqual(
            contents[0]["parts"][0]["text"], current_user_parts[0]["text"]
        )
        self.assertEqual(
            current_user_parts[1]["file_data"]["file_uri"],
            "gs://audio/eval-2.flac",
        )

    def test_eval_manifest_uri_comes_from_gcs_config(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_s:
            tmp = Path(tmp_s)
            storage = FakeStorageClient()
            _seed_source_manifests(
                storage,
                eval_uri="gs://audio/local-eval.flac",
            )
            storage.put(
                "gs://prepared/eval.jsonl",
                _manifest([_row("gs://audio/prepared-eval.flac", "prepared")]),
            )
            cfg_path = _write_config_file(tmp)
            run_cfg = load_run_config(cfg_path)
            config = {
                **run_cfg.to_record_dict(),
                "canonical_eval_uri": "gs://prepared/eval.jsonl",
            }
            output_uri = f"{run_cfg.paths.gcs_prefix}/evals/base/output/"
            storage.put(
                f"{output_uri}predictions.jsonl",
                json.dumps(
                    {
                        "request": {
                            "contents": [
                                {
                                    "parts": [
                                        {
                                            "fileData": {
                                                "fileUri": "gs://audio/prepared-eval.flac"
                                            }
                                        }
                                    ]
                                }
                            ]
                        },
                        "response": {
                            "candidates": [
                                {"content": {"parts": [{"text": "prepared"}]}}
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
                _patched_eval_scoring(),
            ):
                rc = evaluate_module.evaluate_run(
                    args, run_cfg, storage, config
                )

            metrics = json.loads(
                (tmp / "results" / "round-a" / "wer_summary.json").read_text(
                    encoding="utf-8"
                )
            )

        self.assertEqual(rc, 0)
        self.assertEqual(metrics["targets"][0]["wer"], 0.0)

    def test_eval_normalized_manifest_omits_missing_prediction_field(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp_s:
            tmp = Path(tmp_s)
            storage = FakeStorageClient()
            _seed_source_manifests(storage, eval_uri="gs://audio/eval-1.flac")
            cfg_path = _write_config_file(tmp)
            run_cfg = load_run_config(cfg_path)
            storage.put(
                run_cfg.paths.canonical_eval_uri,
                _manifest(
                    [
                        _row(
                            "gs://audio/eval-1.flac",
                            "first",
                            example_id="eval-1",
                        ),
                        _row(
                            "gs://audio/eval-2.flac",
                            "second",
                            example_id="eval-2",
                        ),
                    ]
                ),
            )
            storage.put(
                run_cfg.paths.config_uri, json.dumps(run_cfg.to_record_dict())
            )
            output_uri = f"{run_cfg.paths.gcs_prefix}/evals/base/output/"
            storage.put(
                f"{output_uri}predictions.jsonl",
                json.dumps(
                    {
                        "request": {
                            "contents": [
                                {
                                    "parts": [
                                        {
                                            "fileData": {
                                                "fileUri": "gs://audio/eval-1.flac"
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
                                        "parts": [
                                            {"text": "[UNINTELLIGIBLE]"}
                                        ]
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
                _patched_eval_scoring(),
            ):
                rc = evaluate_module.evaluate_run(
                    args, run_cfg, storage, run_cfg.to_record_dict()
                )

            metrics = json.loads(
                (tmp / "results" / "round-a" / "wer_summary.json").read_text(
                    encoding="utf-8"
                )
            )
            manifest_rows = [
                json.loads(line)
                for line in storage.get(
                    metrics["targets"][0]["artifacts"][
                        "normalized_manifest_uri"
                    ]
                ).splitlines()
            ]
            base_target = metrics["targets"][0]

        self.assertEqual(rc, 0)
        self.assertEqual(base_target["missing_prediction_count"], 1)
        self.assertEqual(base_target["empty_response_rate"], 50.0)
        self.assertEqual(base_target["empty_or_unintelligible_rate"], 100.0)
        self.assertIn("total_reference_words", base_target)
        self.assertIsInstance(base_target["insertions"], int)
        self.assertIsInstance(base_target["deletions"], int)
        self.assertIsInstance(base_target["substitutions"], int)
        self.assertEqual(
            base_target["artifacts"]["raw_output_uri"],
            output_uri,
        )
        self.assertIn(
            "normalized_manifest_uri",
            base_target["artifacts"],
        )
        self.assertEqual(
            manifest_rows[0]["pred_text_gemini_3_1_flash_lite"],
            "[UNINTELLIGIBLE]",
        )
        self.assertNotIn("pred_text_gemini_3_1_flash_lite", manifest_rows[1])

    def test_eval_requires_canonical_eval_uri_in_gcs_config(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_s:
            tmp = Path(tmp_s)
            storage = FakeStorageClient()
            _seed_source_manifests(storage)
            cfg_path = _write_config_file(tmp)
            run_cfg = load_run_config(cfg_path)
            config = run_cfg.to_record_dict()
            config.pop("canonical_eval_uri")
            storage.put(run_cfg.paths.config_uri, json.dumps(config))
            args = argparse.Namespace(config=str(cfg_path), base_only=True)

            with (
                unittest.mock.patch.object(
                    evaluate_module.storage, "Client", return_value=storage
                ),
                unittest.mock.patch.object(
                    evaluate_module, "submit_batch_inference"
                ) as submit,
            ):
                rc = evaluate_module.evaluate(args)

        self.assertEqual(rc, 1)
        submit.assert_not_called()

    def test_eval_requires_durable_eval_models_before_batch_inference(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp_s:
            tmp = Path(tmp_s)
            storage = FakeStorageClient()
            _seed_source_manifests(storage)
            cfg_path = _write_config_file(tmp)
            run_cfg = load_run_config(cfg_path)
            config = run_cfg.to_record_dict()
            config.pop("eval_models")
            config["endpoint"] = "projects/p/locations/us/endpoints/123"
            storage.put(run_cfg.paths.config_uri, json.dumps(config))
            args = argparse.Namespace(config=str(cfg_path), base_only=False)

            with (
                unittest.mock.patch.object(
                    evaluate_module.storage, "Client", return_value=storage
                ),
                unittest.mock.patch.object(
                    evaluate_module, "download_jsonl_manifest"
                ) as download_manifest,
                unittest.mock.patch.object(
                    evaluate_module, "submit_batch_inference"
                ) as submit,
            ):
                rc = evaluate_module.evaluate(args)

        self.assertEqual(rc, 1)
        download_manifest.assert_not_called()
        submit.assert_not_called()

    def test_eval_rejects_invalid_durable_eval_models_before_submit(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp_s:
            tmp = Path(tmp_s)
            storage = FakeStorageClient()
            _seed_source_manifests(storage)
            cfg_path = _write_config_file(tmp)
            run_cfg = load_run_config(cfg_path)
            config = run_cfg.to_record_dict()
            config["eval_models"] = [
                {"label": "bad label", "model": "gemini-3.1-flash-lite"}
            ]
            storage.put(run_cfg.paths.config_uri, json.dumps(config))
            args = argparse.Namespace(config=str(cfg_path), base_only=True)

            with (
                unittest.mock.patch.object(
                    evaluate_module.storage, "Client", return_value=storage
                ),
                unittest.mock.patch.object(
                    evaluate_module, "download_jsonl_manifest"
                ) as download_manifest,
                unittest.mock.patch.object(
                    evaluate_module, "submit_batch_inference"
                ) as submit,
            ):
                rc = evaluate_module.evaluate(args)

        self.assertEqual(rc, 1)
        download_manifest.assert_not_called()
        submit.assert_not_called()

    def test_eval_rejects_unsupported_eval_models_before_submit(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp_s:
            tmp = Path(tmp_s)
            storage = FakeStorageClient()
            _seed_source_manifests(storage)
            cfg_path = _write_config_file(tmp)
            run_cfg = load_run_config(cfg_path)
            config = run_cfg.to_record_dict()
            config["eval_models"] = [
                {
                    "label": "checkpoint_6",
                    "model": "projects/p/locations/us/endpoints/123",
                }
            ]
            storage.put(run_cfg.paths.config_uri, json.dumps(config))
            args = argparse.Namespace(config=str(cfg_path), base_only=True)

            with (
                unittest.mock.patch.object(
                    evaluate_module.storage, "Client", return_value=storage
                ),
                unittest.mock.patch.object(
                    evaluate_module, "download_jsonl_manifest"
                ) as download_manifest,
                unittest.mock.patch.object(
                    evaluate_module, "submit_batch_inference"
                ) as submit,
            ):
                rc = evaluate_module.evaluate(args)

        self.assertEqual(rc, 1)
        download_manifest.assert_not_called()
        submit.assert_not_called()

    def test_batch_infer_fails_when_vertex_writes_no_jsonl(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_s:
            tmp = Path(tmp_s)
            storage = FakeStorageClient()
            run_cfg = load_run_config(_write_config_file(tmp))
            output_uri = f"{run_cfg.paths.gcs_prefix}/evals/base/output/"
            eval_rows = [
                types.SimpleNamespace(audio_filepath="gs://audio/eval.flac")
            ]

            with unittest.mock.patch.object(
                evaluate_module,
                "submit_batch_inference",
                return_value=output_uri,
            ):
                preds = evaluate_module.batch_infer(
                    storage_client=storage,
                    run_gcs_prefix=run_cfg.paths.gcs_prefix,
                    gcp_project=run_cfg.gcp_project,
                    location=run_cfg.location,
                    model_id="gemini-3.1-flash-lite",
                    label="base",
                    eval_rows=eval_rows,
                    system_prompt="sys",
                    user_prompt="user",
                )

        self.assertIsNone(preds)

    def test_batch_infer_rejects_duplicate_eval_audio_uris(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_s:
            tmp = Path(tmp_s)
            storage = FakeStorageClient()
            run_cfg = load_run_config(_write_config_file(tmp))
            eval_rows = [
                types.SimpleNamespace(audio_filepath="gs://audio/eval.flac"),
                types.SimpleNamespace(audio_filepath="gs://audio/eval.flac"),
            ]

            with unittest.mock.patch.object(
                evaluate_module,
                "submit_batch_inference",
            ) as submit:
                preds = evaluate_module.batch_infer(
                    storage_client=storage,
                    run_gcs_prefix=run_cfg.paths.gcs_prefix,
                    gcp_project=run_cfg.gcp_project,
                    location=run_cfg.location,
                    model_id="gemini-3.1-flash-lite",
                    label="base",
                    eval_rows=eval_rows,
                    system_prompt="sys",
                    user_prompt="user",
                )

        self.assertIsNone(preds)
        submit.assert_not_called()

    def test_batch_infer_rejects_prediction_uri_outside_eval_manifest(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp_s:
            tmp = Path(tmp_s)
            storage = FakeStorageClient()
            run_cfg = load_run_config(_write_config_file(tmp))
            output_uri = f"{run_cfg.paths.gcs_prefix}/evals/base/output/"
            storage.put(
                f"{output_uri}predictions.jsonl",
                json.dumps(
                    {
                        "request": {
                            "contents": [
                                {
                                    "parts": [
                                        {
                                            "fileData": {
                                                "fileUri": "gs://audio/other.flac"
                                            }
                                        }
                                    ]
                                }
                            ]
                        },
                        "response": {
                            "candidates": [
                                {"content": {"parts": [{"text": "other"}]}}
                            ]
                        },
                    }
                )
                + "\n",
            )
            eval_rows = [
                types.SimpleNamespace(audio_filepath="gs://audio/eval.flac")
            ]

            with unittest.mock.patch.object(
                evaluate_module,
                "submit_batch_inference",
                return_value=output_uri,
            ):
                preds = evaluate_module.batch_infer(
                    storage_client=storage,
                    run_gcs_prefix=run_cfg.paths.gcs_prefix,
                    gcp_project=run_cfg.gcp_project,
                    location=run_cfg.location,
                    model_id="gemini-3.1-flash-lite",
                    label="base",
                    eval_rows=eval_rows,
                    system_prompt="sys",
                    user_prompt="user",
                )

        self.assertIsNone(preds)

    def test_error_breakdown_denominator_matches_wer_denominator(self) -> None:
        metrics: dict[str, Any] = {}

        evaluate_module.add_error_breakdown(
            metrics,
            "base",
            {
                "hits": 2,
                "substitutions": 1,
                "deletions": 1,
                "insertions": 1,
            },
        )

        self.assertEqual(metrics["base_insertions"], 25.0)
        self.assertEqual(metrics["base_deletions"], 25.0)
        self.assertEqual(metrics["base_substitutions"], 25.0)
