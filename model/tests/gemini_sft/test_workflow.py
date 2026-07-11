from __future__ import annotations

import argparse
import contextlib
import json
import logging
import pathlib
import tempfile
import types
import typing
import unittest
import unittest.mock

import fake_gcs
import sft_eval_fixtures
from common.gemini import batch, context, eval_artifacts, tuning_data
from gemini_sft import cli, preflight, prepare
from gemini_sft import config as config_module
from gemini_sft import evaluate as evaluate_module
from gemini_sft import reporting as reporting_module
from gemini_sft import tune as tune_module
from google.api_core import exceptions as google_exceptions


def _manifest(rows: list[dict[str, typing.Any]]) -> str:
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
) -> dict[str, typing.Any]:
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
    round_id: str = "round-a",
    prior_context_count: int | None = None,
    eval_label: str = "base",
    eval_model: str = "gemini-3.1-flash-lite",
) -> str:
    context = ""
    if prior_context_count is not None:
        context = f"""
[context]
prior_turn_count = {prior_context_count}
"""
    return f"""
round_id = "{round_id}"
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

[eval.model]
label = "{eval_label}"
model = "{eval_model}"
"""


def _eval_only_config_text(
    *,
    round_id: str = "round-a",
    eval_label: str = "base",
    eval_model: str = "gemini-3.1-flash-lite",
) -> str:
    body = _config_text(
        round_id=round_id,
        eval_label=eval_label,
        eval_model=eval_model,
    )
    excluded = ("train_manifest_uri =", "validation_manifest_uri =")
    return "\n".join(
        line for line in body.splitlines() if not line.startswith(excluded)
    )


def _fake_wer(
    refs: list[str],
    hyps: list[str],
    normalizer: typing.Any = None,
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
    normalizer: typing.Any = None,
) -> dict[str, float]:
    del normalizer
    return {"cer": 0.0 if refs == hyps else 1.0}


@contextlib.contextmanager
def _patched_eval_scoring() -> typing.Iterator[None]:
    """Patch scoring dependencies with deterministic test implementations.

    Yields:
        Control while the deterministic scoring patches are active.
    """
    with (
        unittest.mock.patch.object(
            evaluate_module.scoring, "build_normalizer", return_value=None
        ),
        unittest.mock.patch.multiple(
            reporting_module.scoring,
            compute_wer=_fake_wer,
            compute_cer=_fake_cer,
            keyword_metrics=lambda *_, **__: [],
        ),
    ):
        yield


def _batch_prediction_map(
    predictions: dict[str, str],
    *,
    output_uri: str = sft_eval_fixtures.batch_output_uri(
        "gs://test-bucket/sft/runs/round-a",
    ),
) -> typing.Any:
    preds = batch.BatchPredictionMap(predictions)
    preds.output_uri = output_uri
    return preds


class _OnlinePredictionMap(dict[str, str]):
    def __init__(
        self,
        predictions: dict[str, str],
        *,
        online_predictions_uri: str,
        metadata_uri: str,
        error_count: int = 0,
        request_identity_hash: str | None = "identity-hash",
    ) -> None:
        super().__init__(predictions)
        self.online_predictions_uri = online_predictions_uri
        self.metadata_uri = metadata_uri
        self.error_count = error_count
        self.request_identity_hash = request_identity_hash


def _online_prediction_map(
    predictions: dict[str, str],
    *,
    run_gcs_prefix: str,
    label: str = "base",
    error_count: int = 0,
    request_identity_hash: str | None = "identity-hash",
) -> _OnlinePredictionMap:
    return _OnlinePredictionMap(
        predictions,
        **sft_eval_fixtures.online_prediction_artifacts(run_gcs_prefix, label),
        error_count=error_count,
        request_identity_hash=request_identity_hash,
    )


def _write_config_file(
    tmp: pathlib.Path,
    round_id: str = "round-a",
    prior_context_count: int | None = None,
    eval_label: str = "base",
    eval_model: str = "gemini-3.1-flash-lite",
) -> pathlib.Path:
    path = tmp / "run.toml"
    path.write_text(
        _config_text(
            round_id,
            prior_context_count,
            eval_label=eval_label,
            eval_model=eval_model,
        ),
        encoding="utf-8",
    )
    return path


def _seed_source_manifests(
    storage: fake_gcs.FakeStorageClient,
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
    tmp: pathlib.Path,
    storage: fake_gcs.FakeStorageClient,
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
            unittest.mock.patch(
                "logging.getLogger", side_effect=fake_get_logger
            ),
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


class TestPreflight(unittest.TestCase):
    def test_validation_target_uri_cannot_appear_anywhere_in_train_files(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp_s:
            tmp = pathlib.Path(tmp_s)
            train_example = tuning_data.build_audio_tuning_example(
                "gs://audio/train.flac",
                "train",
                "sys",
                "user",
            )
            train_example["contents"][0]["parts"].insert(
                0,
                {
                    "fileData": {
                        "fileUri": "gs://audio/validation.flac",
                        "mimeType": "audio/flac",
                    }
                },
            )
            val_example = tuning_data.build_audio_tuning_example(
                "gs://audio/validation.flac",
                "validation",
                "sys",
                "user",
            )
            train_path = tmp / "train.jsonl"
            val_path = tmp / "val.jsonl"
            report_path = tmp / "report.json"
            train_path.write_text(json.dumps(train_example) + "\n")
            val_path.write_text(json.dumps(val_example) + "\n")

            report = preflight.run_preflight(
                train_jsonl_path=train_path,
                val_jsonl_path=val_path,
                storage_client=None,
                report_path=report_path,
                system_prompt="sys",
                user_prompt="user",
            )

        self.assertFalse(report.passed)
        self.assertTrue(
            any(
                "gs://audio/validation.flac" in item for item in report.failures
            )
        )


class TestPrepareRun(unittest.TestCase):
    def test_prepare_cli_publishes_only_eval_artifacts_for_eval_only_round(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp_s:
            tmp = pathlib.Path(tmp_s)
            storage = fake_gcs.FakeStorageClient()
            storage.put(
                "gs://source/manifests/eval.jsonl",
                _manifest([_row("gs://audio/eval.flac", "eval transcript")]),
            )
            cfg_path = tmp / "run.toml"
            cfg_path.write_text(_eval_only_config_text(), encoding="utf-8")
            run_cfg = config_module.load_prepare_run_config(cfg_path)

            with (
                unittest.mock.patch.object(
                    prepare.storage,
                    "Client",
                    return_value=storage,
                ),
                unittest.mock.patch.object(
                    prepare,
                    "RESULTS_DIR",
                    tmp / "results",
                ),
                unittest.mock.patch.object(
                    prepare.preflight,
                    "run_preflight",
                ) as run_preflight,
                unittest.mock.patch.object(
                    prepare,
                    "write_gemini_jsonl",
                ) as write_gemini,
            ):
                result = prepare.prepare(
                    argparse.Namespace(config=str(cfg_path))
                )

            self.assertEqual(result, 0)
            run_preflight.assert_not_called()
            write_gemini.assert_not_called()
            self.assertEqual(
                storage.uploads,
                [
                    run_cfg.paths.run_config_uri,
                    run_cfg.paths.canonical_eval_uri,
                    run_cfg.paths.config_uri,
                ],
            )
            durable = json.loads(storage.get(run_cfg.paths.config_uri))
            self.assertEqual(durable["status"], "eval_prepared")
            self.assertEqual(durable["canonical_eval_rows"], 1)
            self.assertNotIn("gemini_train_uri", durable)
            self.assertFalse(
                (tmp / "results" / "round-a" / "preflight").exists()
            )

    def test_eval_only_prepare_rejects_invalid_manifests_before_upload(
        self,
    ) -> None:
        cases = {
            "malformed JSONL": (
                _manifest([_row("gs://audio/eval.flac")]) + "{bad json}\n"
            ),
            "empty manifest": "",
            "invalid canonical row": _manifest(
                [_row("local/eval.mp3", "invalid audio URI")]
            ),
        }
        for name, content in cases.items():
            with (
                self.subTest(name=name),
                tempfile.TemporaryDirectory() as tmp_s,
            ):
                tmp = pathlib.Path(tmp_s)
                storage = fake_gcs.FakeStorageClient()
                storage.put("gs://source/manifests/eval.jsonl", content)
                cfg_path = tmp / "run.toml"
                cfg_path.write_text(
                    _eval_only_config_text(),
                    encoding="utf-8",
                )

                with (
                    unittest.mock.patch.object(
                        prepare.storage,
                        "Client",
                        return_value=storage,
                    ),
                    unittest.mock.patch.object(
                        prepare,
                        "RESULTS_DIR",
                        tmp / "results",
                    ),
                ):
                    result = prepare.prepare(
                        argparse.Namespace(config=str(cfg_path))
                    )

                self.assertEqual(result, 1)
                self.assertEqual(storage.uploads, [])

    def test_eval_only_prepare_reports_missing_source_as_cli_failure(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp_s:
            tmp = pathlib.Path(tmp_s)
            storage = fake_gcs.FakeStorageClient()
            cfg_path = tmp / "run.toml"
            cfg_path.write_text(_eval_only_config_text(), encoding="utf-8")

            with (
                unittest.mock.patch.object(
                    prepare.storage,
                    "Client",
                    return_value=storage,
                ),
                unittest.mock.patch.object(
                    prepare,
                    "RESULTS_DIR",
                    tmp / "results",
                ),
                unittest.mock.patch.object(
                    prepare.gcs_utils,
                    "download_gcs_uri",
                    side_effect=google_exceptions.NotFound("missing"),
                ),
            ):
                result = prepare.prepare(
                    argparse.Namespace(config=str(cfg_path))
                )

            self.assertEqual(result, 1)
            self.assertEqual(storage.uploads, [])

    def test_tune_rejects_eval_only_config_before_provider_submission(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp_s:
            tmp = pathlib.Path(tmp_s)
            cfg_path = tmp / "run.toml"
            cfg_path.write_text(_eval_only_config_text(), encoding="utf-8")

            with unittest.mock.patch.object(
                tune_module,
                "submit_tuning_job",
            ) as submit:
                result = tune_module.tune(
                    argparse.Namespace(config=str(cfg_path), confirm=True)
                )

            self.assertEqual(result, 1)
            submit.assert_not_called()

    def test_prepare_reports_manifest_type_errors_as_cli_failures(self) -> None:
        run_cfg = types.SimpleNamespace(
            gcp_project="project-id",
            paths=types.SimpleNamespace(
                config_uri="gs://bucket/run/config.json",
                gcs_prefix="gs://bucket/run",
            ),
        )
        with (
            unittest.mock.patch.object(
                prepare.config_lib,
                "load_prepare_run_config",
                return_value=run_cfg,
            ),
            unittest.mock.patch.object(prepare.storage, "Client"),
            unittest.mock.patch.object(
                prepare.gcs_utils,
                "gcs_uri_exists",
                return_value=False,
            ),
            unittest.mock.patch.object(
                prepare.gcs_utils,
                "gcs_prefix_has_any_blob",
                return_value=False,
            ),
            unittest.mock.patch.object(
                prepare,
                "prepare_run",
                side_effect=TypeError("expected JSON object at line 1"),
            ),
        ):
            result = prepare.prepare(argparse.Namespace(config="run.toml"))

        self.assertEqual(result, 1)

    def test_prepare_uploads_required_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_s:
            tmp = pathlib.Path(tmp_s)
            storage = fake_gcs.FakeStorageClient()
            _seed_source_manifests(storage)
            run_cfg = config_module.load_run_config(_write_config_file(tmp))

            artifacts, config = prepare.prepare_run(
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
            tmp = pathlib.Path(tmp_s)
            storage = fake_gcs.FakeStorageClient()
            _seed_source_manifests(storage)
            run_cfg = config_module.load_run_config(_write_config_file(tmp))

            prepare.prepare_run(
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
            tmp = pathlib.Path(tmp_s)
            storage = fake_gcs.FakeStorageClient()
            _seed_source_manifests(
                storage,
                train_uri="gs://audio/shared.flac",
                eval_uri="gs://audio/shared.flac",
            )
            run_cfg = config_module.load_run_config(_write_config_file(tmp))

            with self.assertRaisesRegex(ValueError, "train and eval"):
                prepare.prepare_run(
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
                    tmp = pathlib.Path(tmp_s)
                    storage = fake_gcs.FakeStorageClient()
                    _seed_source_manifests(storage)
                    storage.put(manifest_uris[role], content)
                    run_cfg = config_module.load_run_config(
                        _write_config_file(tmp)
                    )

                    with self.assertRaisesRegex(ValueError, message):
                        prepare.prepare_run(
                            run_cfg=run_cfg,
                            storage_client=storage,
                            results_dir=tmp / "results",
                        )

                    _assert_no_prepared_outputs(self, tmp, storage)

    def test_train_eval_uri_and_identity_overlap_reports_both_categories(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp_s:
            tmp = pathlib.Path(tmp_s)
            storage = fake_gcs.FakeStorageClient()
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
            run_cfg = config_module.load_run_config(_write_config_file(tmp))

            with self.assertRaisesRegex(ValueError, "train and eval") as ctx:
                prepare.prepare_run(
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
            tmp = pathlib.Path(tmp_s)
            storage = fake_gcs.FakeStorageClient()
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
            run_cfg = config_module.load_run_config(_write_config_file(tmp))

            with self.assertRaisesRegex(
                ValueError, "Canonical Manifest validation failed"
            ):
                prepare.prepare_run(
                    run_cfg=run_cfg,
                    storage_client=storage,
                    results_dir=tmp / "results",
                )

            _assert_no_prepared_outputs(self, tmp, storage)

    def test_train_eval_identity_overlap_fails_before_writing_gemini_jsonl(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp_s:
            tmp = pathlib.Path(tmp_s)
            storage = fake_gcs.FakeStorageClient()
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
            run_cfg = config_module.load_run_config(_write_config_file(tmp))

            with self.assertRaisesRegex(ValueError, "identity"):
                prepare.prepare_run(
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
            tmp = pathlib.Path(tmp_s)
            storage = fake_gcs.FakeStorageClient()
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
            run_cfg = config_module.load_run_config(_write_config_file(tmp))

            with self.assertRaisesRegex(
                ValueError,
                "train and validation.*identity",
            ):
                prepare.prepare_run(
                    run_cfg=run_cfg,
                    storage_client=storage,
                    results_dir=tmp / "results",
                )

    def test_validation_eval_overlap_is_allowed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_s:
            tmp = pathlib.Path(tmp_s)
            storage = fake_gcs.FakeStorageClient()
            _seed_source_manifests(
                storage,
                validation_uri="gs://audio/shared.flac",
                eval_uri="gs://audio/shared.flac",
            )
            run_cfg = config_module.load_run_config(_write_config_file(tmp))

            _, config = prepare.prepare_run(
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
            tmp = pathlib.Path(tmp_s)
            storage = fake_gcs.FakeStorageClient()
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
            run_cfg = config_module.load_run_config(
                _write_config_file(tmp, prior_context_count=8)
            )

            _, config = prepare.prepare_run(
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
        audio_parts = []
        for turn in second_contents:
            audio_parts.extend(
                part for part in turn["parts"] if "fileData" in part
            )
        self.assertEqual(
            [part["fileData"]["fileUri"] for part in audio_parts],
            ["gs://audio/source-a/002.flac"],
        )
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
            tmp = pathlib.Path(tmp_s)
            storage = fake_gcs.FakeStorageClient()
            cfg_path = _write_config_file(tmp)
            run_cfg = config_module.load_run_config(cfg_path)
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
            tmp = pathlib.Path(tmp_s)
            storage = fake_gcs.FakeStorageClient()
            _seed_source_manifests(storage)
            cfg_path = _write_config_file(tmp)
            run_cfg = config_module.load_run_config(cfg_path)
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
            tmp = pathlib.Path(tmp_s)
            storage = fake_gcs.FakeStorageClient()
            cfg_path = _write_config_file(tmp)
            run_cfg = config_module.load_run_config(cfg_path)
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

    def test_tune_validates_prepared_config_model_not_local_toml(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_s:
            tmp = pathlib.Path(tmp_s)
            storage = fake_gcs.FakeStorageClient()
            cfg_path = tmp / "run.toml"
            cfg_path.write_text(
                _config_text().replace(
                    'base_model = "gemini-3.1-flash-lite"',
                    'base_model = "local-edited-model"',
                ),
                encoding="utf-8",
            )
            run_cfg = config_module.load_run_config(cfg_path)
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
            tmp = pathlib.Path(tmp_s)
            storage = fake_gcs.FakeStorageClient()
            cfg_path = _write_config_file(tmp)
            run_cfg = config_module.load_run_config(cfg_path)
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
            tmp = pathlib.Path(tmp_s)
            storage = fake_gcs.FakeStorageClient()
            cfg_path = _write_config_file(tmp)
            run_cfg = config_module.load_run_config(cfg_path)
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
            tmp = pathlib.Path(tmp_s)
            storage = fake_gcs.FakeStorageClient()
            cfg_path = _write_config_file(tmp)
            run_cfg = config_module.load_run_config(cfg_path)
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
    def test_eval_model_family_uses_publisher_target_model(self) -> None:
        target = config_module.EvalModelTarget(
            label="base",
            model="gemini-2.5-flash",
        )

        self.assertEqual(
            evaluate_module._eval_model_family_id(
                target,
                "gemini-3.1-flash-lite",
            ),
            "gemini-2.5-flash",
        )

    def test_eval_model_family_uses_base_model_for_endpoint(self) -> None:
        target = config_module.EvalModelTarget(
            label="checkpoint_6",
            model="projects/p/locations/us-central1/endpoints/123",
        )

        self.assertEqual(
            evaluate_module._eval_model_family_id(
                target,
                "gemini-3.1-flash-lite",
            ),
            "gemini-3.1-flash-lite",
        )

    def test_eval_consumes_eval_only_prepared_state(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_s:
            tmp = pathlib.Path(tmp_s)
            storage = fake_gcs.FakeStorageClient()
            storage.put(
                "gs://source/manifests/eval.jsonl",
                _manifest([_row("gs://audio/eval.flac", "eval transcript")]),
            )
            cfg_path = tmp / "run.toml"
            cfg_path.write_text(
                _eval_only_config_text(eval_model="gemini-2.5-flash"),
                encoding="utf-8",
            )
            with (
                unittest.mock.patch.object(
                    prepare.storage,
                    "Client",
                    return_value=storage,
                ),
                unittest.mock.patch.object(
                    prepare,
                    "RESULTS_DIR",
                    tmp / "results",
                ),
            ):
                self.assertEqual(
                    prepare.prepare(argparse.Namespace(config=str(cfg_path))),
                    0,
                )
            predictions = _batch_prediction_map(
                {"gs://audio/eval.flac": "eval transcript"}
            )
            with (
                _patched_eval_scoring(),
                unittest.mock.patch.object(
                    evaluate_module.storage,
                    "Client",
                    return_value=storage,
                ),
                unittest.mock.patch.object(
                    evaluate_module,
                    "RESULTS_DIR",
                    tmp / "results",
                ),
                unittest.mock.patch.object(
                    evaluate_module,
                    "batch_infer",
                    return_value=predictions,
                ),
            ):
                result = evaluate_module.evaluate(
                    argparse.Namespace(config=str(cfg_path))
                )

        self.assertEqual(result, 0)
        normalized_uri = (
            "gs://test-bucket/inference_manifests/echo/eval/"
            "gemini_2_5_flash/round-a/base.jsonl"
        )
        self.assertTrue(storage.has(normalized_uri))
        normalized = json.loads(storage.get(normalized_uri).strip())
        self.assertEqual(
            normalized["pred_text_gemini_2_5_flash"],
            "eval transcript",
        )

    def test_eval_rejects_invalid_eval_manifest_before_batch_inference(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp_s:
            tmp = pathlib.Path(tmp_s)
            storage = fake_gcs.FakeStorageClient()
            _seed_source_manifests(storage)
            cfg_path = _write_config_file(tmp)
            run_cfg = config_module.load_run_config(cfg_path)
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
            args = argparse.Namespace(config=str(cfg_path))

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

    def test_eval_rejects_unstripped_audio_uri_before_batch_inference(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp_s:
            tmp = pathlib.Path(tmp_s)
            storage = fake_gcs.FakeStorageClient()
            cfg_path = _write_config_file(tmp)
            run_cfg = config_module.load_run_config(cfg_path)
            storage.put(
                run_cfg.paths.canonical_eval_uri,
                _manifest(
                    [
                        _row(
                            "  gs://audio/eval.flac  ",
                            "invalid URI spacing",
                        )
                    ]
                ),
            )
            config = run_cfg.to_record_dict()
            storage.put(run_cfg.paths.config_uri, json.dumps(config))

            with unittest.mock.patch.object(
                evaluate_module,
                "submit_batch_inference",
            ) as submit:
                with self.assertRaisesRegex(
                    ValueError,
                    "audio_filepath must not contain leading or trailing "
                    "whitespace",
                ):
                    evaluate_module.evaluate_run(
                        argparse.Namespace(config=str(cfg_path)),
                        run_cfg,
                        storage,
                        config,
                    )

        submit.assert_not_called()

    def test_eval_rejects_empty_eval_manifest_before_batch_inference(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp_s:
            tmp = pathlib.Path(tmp_s)
            storage = fake_gcs.FakeStorageClient()
            _seed_source_manifests(storage)
            cfg_path = _write_config_file(tmp)
            run_cfg = config_module.load_run_config(cfg_path)
            storage.put(run_cfg.paths.canonical_eval_uri, "")
            config = run_cfg.to_record_dict()
            storage.put(run_cfg.paths.config_uri, json.dumps(config))
            args = argparse.Namespace(config=str(cfg_path))

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

    def test_eval_rejects_partially_malformed_manifest_before_inference(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp_s:
            tmp = pathlib.Path(tmp_s)
            storage = fake_gcs.FakeStorageClient()
            cfg_path = _write_config_file(tmp)
            run_cfg = config_module.load_run_config(cfg_path)
            storage.put(
                run_cfg.paths.canonical_eval_uri,
                _manifest([_row("gs://audio/eval.flac", "valid")])
                + "{bad json}\n",
            )
            config = run_cfg.to_record_dict()
            storage.put(run_cfg.paths.config_uri, json.dumps(config))

            with unittest.mock.patch.object(
                evaluate_module,
                "submit_batch_inference",
            ) as submit:
                with self.assertRaisesRegex(
                    ValueError,
                    r"canonical/eval.jsonl: malformed JSON at line 2",
                ):
                    evaluate_module.evaluate_run(
                        argparse.Namespace(config=str(cfg_path)),
                        run_cfg,
                        storage,
                        config,
                    )

        submit.assert_not_called()

    def test_eval_handler_returns_clean_error_when_vertex_extra_missing(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp_s:
            tmp = pathlib.Path(tmp_s)
            storage = fake_gcs.FakeStorageClient()
            _seed_source_manifests(storage, eval_uri="gs://audio/eval.flac")
            cfg_path = _write_config_file(tmp)
            run_cfg = config_module.load_run_config(cfg_path)
            storage.put(
                run_cfg.paths.canonical_eval_uri,
                _manifest([_row("gs://audio/eval.flac", "eval transcript")]),
            )
            config = run_cfg.to_record_dict()
            storage.put(run_cfg.paths.config_uri, json.dumps(config))
            args = argparse.Namespace(config=str(cfg_path))

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

    def test_eval_handler_returns_clean_error_when_gcs_download_fails(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp_s:
            tmp = pathlib.Path(tmp_s)
            storage = fake_gcs.FakeStorageClient()
            cfg_path = _write_config_file(tmp)
            run_cfg = config_module.load_run_config(cfg_path)
            storage.put(
                run_cfg.paths.config_uri,
                json.dumps(run_cfg.to_record_dict()),
            )
            args = argparse.Namespace(config=str(cfg_path))

            with (
                unittest.mock.patch.object(
                    evaluate_module.storage, "Client", return_value=storage
                ),
                unittest.mock.patch.object(
                    evaluate_module,
                    "download_jsonl_manifest_strict",
                    side_effect=google_exceptions.NotFound("missing"),
                ),
                unittest.mock.patch.object(
                    evaluate_module, "batch_infer"
                ) as batch,
            ):
                rc = evaluate_module.evaluate(args)

        self.assertEqual(rc, 1)
        batch.assert_not_called()

    def test_eval_uses_shared_batch_parser_and_records_output_uri(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_s:
            tmp = pathlib.Path(tmp_s)
            storage = fake_gcs.FakeStorageClient()
            _seed_source_manifests(storage, eval_uri="gs://audio/eval.flac")
            cfg_path = _write_config_file(tmp)
            run_cfg = config_module.load_run_config(cfg_path)
            storage.put(
                run_cfg.paths.canonical_eval_uri,
                _manifest([_row("gs://audio/eval.flac", "eval transcript")]),
            )
            config = run_cfg.to_record_dict()
            storage.put(run_cfg.paths.config_uri, json.dumps(config))
            output_uri = sft_eval_fixtures.batch_output_uri(
                run_cfg.paths.gcs_prefix
            )
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
            sft_eval_fixtures.put_batch_metadata(
                storage,
                run_gcs_prefix=run_cfg.paths.gcs_prefix,
                eval_manifest_uri=config["canonical_eval_uri"],
                audio_uris=["gs://audio/eval.flac"],
                system_prompt=config["system_prompt"],
                user_prompt=config["user_prompt"],
            )
            args = argparse.Namespace(config=str(cfg_path))

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

            self.assertEqual(rc, 0)
            metrics = json.loads(
                (tmp / "results" / "round-a" / "wer_summary.json").read_text(
                    encoding="utf-8"
                )
            )
            self.assertIn("target", metrics)
            base_target = metrics["target"]
            self.assertEqual(base_target["target_label"], "base")
            artifacts = base_target["artifacts"]
            self.assertEqual(artifacts["raw_output_uri"], output_uri)
            self.assertEqual(
                artifacts["normalized_manifest_uri"],
                "gs://test-bucket/inference_manifests/echo/eval/"
                "gemini_3_1_flash_lite/round-a/base.jsonl",
            )
            summary = sft_eval_fixtures.summary_artifacts(
                run_cfg.paths.gcs_prefix
            )
            self.assertEqual(
                artifacts["summary_json_uri"],
                summary["summary_json_uri"],
            )
            self.assertEqual(
                artifacts["summary_markdown_uri"],
                summary["summary_markdown_uri"],
            )
            self.assertTrue(storage.has(summary["summary_json_uri"]))
            self.assertTrue(storage.has(summary["summary_markdown_uri"]))
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
            tmp = pathlib.Path(tmp_s)
            storage = fake_gcs.FakeStorageClient()
            cfg_path = _write_config_file(tmp, prior_context_count=1)
            run_cfg = config_module.load_run_config(cfg_path)
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
            config = run_cfg.to_record_dict()
            storage.put(run_cfg.paths.canonical_eval_uri, _manifest(eval_rows))
            storage.put(run_cfg.paths.config_uri, json.dumps(config))
            output_uri = sft_eval_fixtures.batch_output_uri(
                run_cfg.paths.gcs_prefix
            )
            prediction_uri = f"{output_uri}predictions.jsonl"
            storage.put(
                prediction_uri,
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
                                                        "fileUri": (
                                                            "gs://audio/"
                                                            "eval-1.flac"
                                                        )
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
                                                        "fileUri": (
                                                            "gs://audio/"
                                                            "eval-2.flac"
                                                        )
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
            sft_eval_fixtures.put_batch_metadata(
                storage,
                run_gcs_prefix=run_cfg.paths.gcs_prefix,
                eval_manifest_uri=config["canonical_eval_uri"],
                audio_uris=["gs://audio/eval-1.flac", "gs://audio/eval-2.flac"],
                system_prompt=config["system_prompt"],
                user_prompt=config["user_prompt"],
                prior_context_count=1,
                histories=[
                    [],
                    [
                        context.ContextTurn(
                            "gs://audio/eval-1.flac",
                            "first",
                        )
                    ],
                ],
            )
            prediction_output = storage.get(prediction_uri)
            storage.store.pop(fake_gcs.split_gcs(prediction_uri))

            def submit_batch(**_: object) -> str:
                storage.put(prediction_uri, prediction_output)
                return output_uri

            args = argparse.Namespace(config=str(cfg_path))

            with (
                unittest.mock.patch.object(
                    evaluate_module, "RESULTS_DIR", tmp / "results"
                ),
                unittest.mock.patch.object(
                    evaluate_module,
                    "submit_batch_inference",
                    side_effect=submit_batch,
                ),
                _patched_eval_scoring(),
            ):
                rc = evaluate_module.evaluate_run(
                    args,
                    run_cfg,
                    storage,
                    config,
                )

            batch_rows = [
                json.loads(line)
                for line in storage.get(
                    sft_eval_fixtures.batch_input_uri(run_cfg.paths.gcs_prefix)
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
            current_user_parts[1]["fileData"]["fileUri"],
            "gs://audio/eval-2.flac",
        )

    def test_eval_manifest_uri_comes_from_gcs_config(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_s:
            tmp = pathlib.Path(tmp_s)
            storage = fake_gcs.FakeStorageClient()
            _seed_source_manifests(
                storage,
                eval_uri="gs://audio/local-eval.flac",
            )
            storage.put(
                "gs://prepared/eval.jsonl",
                _manifest([_row("gs://audio/prepared-eval.flac", "prepared")]),
            )
            cfg_path = _write_config_file(tmp)
            run_cfg = config_module.load_run_config(cfg_path)
            config = {
                **run_cfg.to_record_dict(),
                "canonical_eval_uri": "gs://prepared/eval.jsonl",
            }
            output_uri = sft_eval_fixtures.batch_output_uri(
                run_cfg.paths.gcs_prefix
            )
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
            sft_eval_fixtures.put_batch_metadata(
                storage,
                run_gcs_prefix=run_cfg.paths.gcs_prefix,
                eval_manifest_uri=config["canonical_eval_uri"],
                audio_uris=["gs://audio/prepared-eval.flac"],
                system_prompt=config["system_prompt"],
                user_prompt=config["user_prompt"],
            )
            args = argparse.Namespace(config=str(cfg_path))

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
        self.assertEqual(metrics["target"]["wer"], 0.0)

    def test_eval_normalized_manifest_omits_missing_prediction_field(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp_s:
            tmp = pathlib.Path(tmp_s)
            storage = fake_gcs.FakeStorageClient()
            _seed_source_manifests(storage, eval_uri="gs://audio/eval-1.flac")
            cfg_path = _write_config_file(tmp)
            run_cfg = config_module.load_run_config(cfg_path)
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
            config = run_cfg.to_record_dict()
            storage.put(run_cfg.paths.config_uri, json.dumps(config))
            output_uri = sft_eval_fixtures.batch_output_uri(
                run_cfg.paths.gcs_prefix
            )
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
                                        "parts": [{"text": "[UNINTELLIGIBLE]"}]
                                    }
                                }
                            ]
                        },
                    }
                )
                + "\n",
            )
            sft_eval_fixtures.put_batch_metadata(
                storage,
                run_gcs_prefix=run_cfg.paths.gcs_prefix,
                eval_manifest_uri=config["canonical_eval_uri"],
                audio_uris=["gs://audio/eval-1.flac", "gs://audio/eval-2.flac"],
                system_prompt=config["system_prompt"],
                user_prompt=config["user_prompt"],
            )
            args = argparse.Namespace(config=str(cfg_path))

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
                    metrics["target"]["artifacts"]["normalized_manifest_uri"]
                ).splitlines()
            ]
            base_target = metrics["target"]

        self.assertEqual(rc, 0)
        self.assertEqual(base_target["missing_prediction_count"], 1)
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

    def test_eval_checkpoint_target_no_longer_fails_before_manifest_download(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp_s:
            tmp = pathlib.Path(tmp_s)
            storage = fake_gcs.FakeStorageClient()
            _seed_source_manifests(storage)
            cfg_path = _write_config_file(
                tmp,
                eval_label="checkpoint_6",
                eval_model="projects/p/locations/us-central1/endpoints/123",
            )
            run_cfg = config_module.load_run_config(cfg_path)
            config = run_cfg.to_record_dict()
            storage.put(run_cfg.paths.config_uri, json.dumps(config))
            online_preds = _online_prediction_map(
                {"gs://audio/eval.flac": "eval transcript"},
                run_gcs_prefix=run_cfg.paths.gcs_prefix,
                label="checkpoint_6",
            )

            with (
                unittest.mock.patch.object(
                    evaluate_module.storage, "Client", return_value=storage
                ),
                unittest.mock.patch.object(
                    evaluate_module,
                    "download_jsonl_manifest_strict",
                    return_value=[
                        _row("gs://audio/eval.flac", "eval transcript")
                    ],
                ) as download_manifest,
                unittest.mock.patch.object(
                    evaluate_module,
                    "run_online_target_inference",
                    unittest.mock.AsyncMock(return_value=online_preds),
                    create=True,
                ) as run_online,
                unittest.mock.patch.object(
                    evaluate_module, "batch_infer"
                ) as batch,
                unittest.mock.patch.object(
                    evaluate_module, "RESULTS_DIR", tmp / "results"
                ),
                _patched_eval_scoring(),
            ):
                rc = evaluate_module.evaluate(
                    args=argparse.Namespace(config=str(cfg_path))
                )

        self.assertEqual(rc, 0)
        download_manifest.assert_called_once()
        run_online.assert_awaited_once()
        batch.assert_not_called()

    def test_eval_execution_limit_scores_one_row_and_batch_input(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_s:
            tmp = pathlib.Path(tmp_s)
            storage = fake_gcs.FakeStorageClient()
            cfg_path = _write_config_file(tmp, prior_context_count=1)
            run_cfg = config_module.load_run_config(cfg_path)
            config = run_cfg.to_record_dict()
            config["eval_execution"]["limit"] = 1
            eval_rows = [
                {
                    **_row(
                        "gs://audio/eval-current.flac",
                        "current",
                        example_id="source-a",
                        segment_id="002",
                        offset=10.0,
                    ),
                    "original_audio_uri": "gs://audio/source-a.flac",
                    "original_offset": 10.0,
                    "row_index": 2,
                },
                {
                    **_row(
                        "gs://audio/eval-prior.flac",
                        "prior",
                        example_id="source-a",
                        segment_id="001",
                        offset=0.0,
                    ),
                    "original_audio_uri": "gs://audio/source-a.flac",
                    "original_offset": 0.0,
                    "row_index": 1,
                },
            ]
            storage.put(run_cfg.paths.canonical_eval_uri, _manifest(eval_rows))
            storage.put(run_cfg.paths.config_uri, json.dumps(config))
            output_uri = sft_eval_fixtures.batch_output_uri(
                run_cfg.paths.gcs_prefix
            )
            prediction_uri = f"{output_uri}predictions.jsonl"
            storage.put(
                prediction_uri,
                json.dumps(
                    {
                        "request": {
                            "contents": [
                                {
                                    "parts": [
                                        {
                                            "fileData": {
                                                "fileUri": (
                                                    "gs://audio/"
                                                    "eval-current.flac"
                                                )
                                            }
                                        }
                                    ]
                                }
                            ]
                        },
                        "response": {
                            "candidates": [
                                {"content": {"parts": [{"text": "current"}]}}
                            ]
                        },
                    }
                )
                + "\n",
            )
            sft_eval_fixtures.put_batch_metadata(
                storage,
                run_gcs_prefix=run_cfg.paths.gcs_prefix,
                eval_manifest_uri=config["canonical_eval_uri"],
                audio_uris=["gs://audio/eval-current.flac"],
                system_prompt=config["system_prompt"],
                user_prompt=config["user_prompt"],
                prior_context_count=1,
                histories=[
                    [
                        context.ContextTurn(
                            "gs://audio/eval-prior.flac",
                            "prior",
                        )
                    ]
                ],
            )
            prediction_output = storage.get(prediction_uri)
            storage.store.pop(fake_gcs.split_gcs(prediction_uri))

            def submit_batch(**_: object) -> str:
                storage.put(prediction_uri, prediction_output)
                return output_uri

            with (
                unittest.mock.patch.object(
                    evaluate_module, "RESULTS_DIR", tmp / "results"
                ),
                unittest.mock.patch.object(
                    evaluate_module,
                    "submit_batch_inference",
                    side_effect=submit_batch,
                ),
                _patched_eval_scoring(),
            ):
                rc = evaluate_module.evaluate_run(
                    argparse.Namespace(config=str(cfg_path)),
                    run_cfg,
                    storage,
                    config,
                )

            metrics = json.loads(
                (tmp / "results" / "round-a" / "wer_summary.json").read_text(
                    encoding="utf-8"
                )
            )
            batch_rows = [
                json.loads(line)
                for line in storage.get(
                    sft_eval_fixtures.batch_input_uri(run_cfg.paths.gcs_prefix)
                ).splitlines()
                if line.strip()
            ]

        self.assertEqual(rc, 0)
        self.assertEqual(metrics["metadata"]["n_eval_examples"], 1)
        self.assertEqual(len(batch_rows), 1)
        self.assertEqual(
            [turn["role"] for turn in batch_rows[0]["request"]["contents"]],
            ["user", "model", "user"],
        )
        self.assertEqual(
            batch_rows[0]["request"]["contents"][1]["parts"][0]["text"],
            "prior",
        )
        self.assertEqual(
            batch_rows[0]["request"]["contents"][-1]["parts"][-1]["fileData"][
                "fileUri"
            ],
            "gs://audio/eval-current.flac",
        )

    def test_eval_runs_single_batch_target_and_reports_artifacts(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp_s:
            tmp = pathlib.Path(tmp_s)
            storage = fake_gcs.FakeStorageClient()
            cfg_path = _write_config_file(tmp)
            run_cfg = config_module.load_run_config(cfg_path)
            config = run_cfg.to_record_dict()
            config["eval_model"] = {
                "label": "base",
                "model": "gemini-3.1-flash-lite",
            }
            storage.put(
                run_cfg.paths.canonical_eval_uri,
                _manifest([_row("gs://audio/eval.flac", "eval transcript")]),
            )
            storage.put(run_cfg.paths.config_uri, json.dumps(config))
            batch_preds = _batch_prediction_map(
                {"gs://audio/eval.flac": "eval transcript"},
                output_uri=sft_eval_fixtures.batch_output_uri(
                    run_cfg.paths.gcs_prefix
                ),
            )
            with (
                unittest.mock.patch.object(
                    evaluate_module,
                    "batch_infer",
                    return_value=batch_preds,
                ) as batch,
                unittest.mock.patch.object(
                    evaluate_module,
                    "run_online_target_inference",
                    unittest.mock.AsyncMock(),
                    create=True,
                ) as run_online,
                unittest.mock.patch.object(
                    evaluate_module, "RESULTS_DIR", tmp / "results"
                ),
                _patched_eval_scoring(),
            ):
                rc = evaluate_module.evaluate_run(
                    argparse.Namespace(config=str(cfg_path)),
                    run_cfg,
                    storage,
                    config,
                )

            metrics = json.loads(
                (tmp / "results" / "round-a" / "wer_summary.json").read_text(
                    encoding="utf-8"
                )
            )
        self.assertEqual(rc, 0)
        self.assertIn("target", metrics)
        self.assertEqual(metrics["metadata"]["n_eval_examples"], 1)
        batch.assert_called_once()
        self.assertEqual(batch.call_args.kwargs["label"], "base")
        self.assertEqual(
            batch.call_args.kwargs["model_id"], "gemini-3.1-flash-lite"
        )
        run_online.assert_not_awaited()
        target = metrics["target"]
        self.assertEqual(target["target_label"], "base")
        self.assertEqual(
            target["artifacts"]["raw_output_uri"],
            batch_preds.output_uri,
        )
        self.assertEqual(target["total_reference_words"], 2)
        self.assertIn(
            "normalized_manifest_uri",
            target["artifacts"],
        )
        summary = sft_eval_fixtures.summary_artifacts(run_cfg.paths.gcs_prefix)
        self.assertEqual(
            target["artifacts"]["summary_json_uri"],
            summary["summary_json_uri"],
        )
        self.assertEqual(
            target["artifacts"]["summary_markdown_uri"],
            summary["summary_markdown_uri"],
        )
        self.assertTrue(storage.has(summary["summary_json_uri"]))
        self.assertTrue(storage.has(summary["summary_markdown_uri"]))
        self.assertEqual(target["metadata"]["backend"], "batch")

    def test_eval_execution_forced_backend_overrides_target_shape(self) -> None:
        online_backend_toml = 'backend = "online"'
        batch_backend_toml = 'backend = "batch"'
        self.assertIn("online", online_backend_toml)
        self.assertIn("batch", batch_backend_toml)

        with tempfile.TemporaryDirectory() as tmp_s:
            tmp = pathlib.Path(tmp_s)
            storage = fake_gcs.FakeStorageClient()
            cfg_path = _write_config_file(tmp)
            run_cfg = config_module.load_run_config(cfg_path)
            config = run_cfg.to_record_dict()
            config["eval_execution"]["backend"] = "online"
            storage.put(
                run_cfg.paths.canonical_eval_uri,
                _manifest([_row("gs://audio/eval.flac", "eval transcript")]),
            )
            online_preds = _online_prediction_map(
                {"gs://audio/eval.flac": "eval transcript"},
                run_gcs_prefix=run_cfg.paths.gcs_prefix,
            )

            with (
                unittest.mock.patch.object(
                    evaluate_module, "batch_infer"
                ) as batch,
                unittest.mock.patch.object(
                    evaluate_module,
                    "run_online_target_inference",
                    unittest.mock.AsyncMock(return_value=online_preds),
                    create=True,
                ) as run_online,
                unittest.mock.patch.object(
                    evaluate_module, "RESULTS_DIR", tmp / "online-results"
                ),
                _patched_eval_scoring(),
            ):
                rc_online = evaluate_module.evaluate_run(
                    argparse.Namespace(config=str(cfg_path)),
                    run_cfg,
                    storage,
                    config,
                )

        self.assertEqual(rc_online, 0)
        run_online.assert_awaited_once()
        batch.assert_not_called()

        with tempfile.TemporaryDirectory() as tmp_s:
            tmp = pathlib.Path(tmp_s)
            storage = fake_gcs.FakeStorageClient()
            cfg_path = _write_config_file(tmp)
            run_cfg = config_module.load_run_config(cfg_path)
            config = run_cfg.to_record_dict()
            config["eval_model"] = {
                "label": "checkpoint_6",
                "model": "projects/p/locations/us-central1/endpoints/123",
            }
            config["eval_execution"]["backend"] = "batch"
            storage.put(
                run_cfg.paths.canonical_eval_uri,
                _manifest([_row("gs://audio/eval.flac", "eval transcript")]),
            )
            batch_preds = _batch_prediction_map(
                {"gs://audio/eval.flac": "eval transcript"},
                output_uri=(
                    sft_eval_fixtures.batch_output_uri(
                        run_cfg.paths.gcs_prefix, "checkpoint_6"
                    )
                ),
            )

            with (
                unittest.mock.patch.object(
                    evaluate_module,
                    "batch_infer",
                    return_value=batch_preds,
                ) as batch,
                unittest.mock.patch.object(
                    evaluate_module,
                    "run_online_target_inference",
                    unittest.mock.AsyncMock(),
                    create=True,
                ) as run_online,
                unittest.mock.patch.object(
                    evaluate_module, "RESULTS_DIR", tmp / "batch-results"
                ),
                _patched_eval_scoring(),
            ):
                rc_batch = evaluate_module.evaluate_run(
                    argparse.Namespace(config=str(cfg_path)),
                    run_cfg,
                    storage,
                    config,
                )

        self.assertEqual(rc_batch, 0)
        batch.assert_called_once()
        run_online.assert_not_awaited()

    def test_eval_requires_canonical_eval_uri_in_gcs_config(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_s:
            tmp = pathlib.Path(tmp_s)
            storage = fake_gcs.FakeStorageClient()
            _seed_source_manifests(storage)
            cfg_path = _write_config_file(tmp)
            run_cfg = config_module.load_run_config(cfg_path)
            config = run_cfg.to_record_dict()
            config.pop("canonical_eval_uri")
            storage.put(run_cfg.paths.config_uri, json.dumps(config))
            args = argparse.Namespace(config=str(cfg_path))

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

    def test_eval_requires_durable_eval_model_before_batch_inference(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp_s:
            tmp = pathlib.Path(tmp_s)
            storage = fake_gcs.FakeStorageClient()
            _seed_source_manifests(storage)
            cfg_path = _write_config_file(tmp)
            run_cfg = config_module.load_run_config(cfg_path)
            config = run_cfg.to_record_dict()
            config.pop("eval_model")
            storage.put(run_cfg.paths.config_uri, json.dumps(config))
            args = argparse.Namespace(config=str(cfg_path))

            with (
                unittest.mock.patch.object(
                    evaluate_module.storage, "Client", return_value=storage
                ),
                unittest.mock.patch.object(
                    evaluate_module, "download_jsonl_manifest_strict"
                ) as download_manifest,
                unittest.mock.patch.object(
                    evaluate_module, "submit_batch_inference"
                ) as submit,
            ):
                rc = evaluate_module.evaluate(args)

        self.assertEqual(rc, 1)
        download_manifest.assert_not_called()
        submit.assert_not_called()

    def test_eval_rejects_local_eval_model_mismatch_before_manifest_download(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp_s:
            tmp = pathlib.Path(tmp_s)
            storage = fake_gcs.FakeStorageClient()
            _seed_source_manifests(storage)
            base_cfg_path = _write_config_file(tmp)
            run_cfg = config_module.load_run_config(base_cfg_path)
            storage.put(
                run_cfg.paths.config_uri,
                json.dumps(run_cfg.to_record_dict()),
            )
            checkpoint_cfg_path = tmp / "checkpoint_eval.toml"
            checkpoint_cfg_path.write_text(
                _config_text(
                    eval_label="checkpoint_6",
                    eval_model=(
                        "projects/p/locations/us-central1/endpoints/123"
                    ),
                ),
                encoding="utf-8",
            )
            batch_preds = _batch_prediction_map(
                {"gs://audio/eval.flac": "eval transcript"},
                output_uri=sft_eval_fixtures.batch_output_uri(
                    run_cfg.paths.gcs_prefix
                ),
            )
            args = argparse.Namespace(config=str(checkpoint_cfg_path))

            with (
                unittest.mock.patch.object(
                    evaluate_module.storage, "Client", return_value=storage
                ),
                unittest.mock.patch.object(
                    evaluate_module,
                    "download_jsonl_manifest_strict",
                    return_value=[
                        _row("gs://audio/eval.flac", "eval transcript")
                    ],
                ) as download_manifest,
                unittest.mock.patch.object(
                    evaluate_module,
                    "batch_infer",
                    return_value=batch_preds,
                ) as batch,
                unittest.mock.patch.object(
                    evaluate_module, "RESULTS_DIR", tmp / "results"
                ),
                _patched_eval_scoring(),
            ):
                rc = evaluate_module.evaluate(args)

        self.assertEqual(rc, 1)
        download_manifest.assert_not_called()
        batch.assert_not_called()

    def test_eval_rejects_durable_round_routing_mismatches_before_work(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp_s:
            tmp = pathlib.Path(tmp_s)
            cfg_path = _write_config_file(tmp)
            run_cfg = config_module.load_eval_run_config(cfg_path)
            args = argparse.Namespace(config=str(cfg_path))
            mismatches = {
                "round_id": "round-b",
                "run_gcs_prefix": "gs://test-bucket/sft/runs/round-b",
                "canonical_eval_uri": (
                    "gs://test-bucket/sft/runs/round-b/manifests/"
                    "canonical/eval.jsonl"
                ),
            }

            for field, mismatched_value in mismatches.items():
                with self.subTest(field=field):
                    storage = fake_gcs.FakeStorageClient()
                    durable_config = run_cfg.to_record_dict()
                    durable_config[field] = mismatched_value
                    storage.put(
                        run_cfg.paths.config_uri,
                        json.dumps(durable_config),
                    )

                    with (
                        unittest.mock.patch.object(
                            evaluate_module.storage,
                            "Client",
                            return_value=storage,
                        ),
                        unittest.mock.patch.object(
                            evaluate_module,
                            "download_jsonl_manifest_strict",
                        ) as download_manifest,
                        unittest.mock.patch.object(
                            evaluate_module,
                            "batch_infer",
                        ) as batch,
                        self.assertLogs(
                            evaluate_module.logger,
                            level=logging.ERROR,
                        ) as logs,
                    ):
                        rc = evaluate_module.evaluate(args)

                    self.assertEqual(rc, 1)
                    self.assertIn(
                        f"Mismatched field(s): {field}",
                        "\n".join(logs.output),
                    )
                    download_manifest.assert_not_called()
                    batch.assert_not_called()

    def test_eval_match_defaults_missing_durable_prior_context_fields(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp_s:
            tmp = pathlib.Path(tmp_s)
            cfg_path = _write_config_file(tmp)
            run_cfg = config_module.load_eval_run_config(cfg_path)
            durable_config = run_cfg.to_record_dict()
            durable_config.pop("prior_context_count")
            durable_config.pop("prior_context_mode")

            evaluate_module._validate_local_eval_config_matches_durable(
                run_cfg,
                durable_config,
            )

    def test_eval_allows_local_operational_execution_overrides(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp_s:
            tmp = pathlib.Path(tmp_s)
            storage = fake_gcs.FakeStorageClient()
            _seed_source_manifests(storage)
            cfg_path = tmp / "online_eval.toml"
            cfg_path.write_text(
                _config_text(
                    eval_label="checkpoint_6",
                    eval_model=(
                        "projects/p/locations/us-central1/endpoints/123"
                    ),
                )
                + """
[eval.execution]
concurrency = 4
max_retries = 1
""",
                encoding="utf-8",
            )
            run_cfg = config_module.load_run_config(cfg_path)
            durable_config = run_cfg.to_record_dict()
            durable_config["eval_execution"]["concurrency"] = 16
            durable_config["eval_execution"]["max_retries"] = 3
            storage.put(run_cfg.paths.config_uri, json.dumps(durable_config))
            online_preds = _online_prediction_map(
                {"gs://audio/eval.flac": "eval transcript"},
                run_gcs_prefix=run_cfg.paths.gcs_prefix,
                label="checkpoint_6",
            )
            args = argparse.Namespace(config=str(cfg_path))

            with (
                unittest.mock.patch.object(
                    evaluate_module.storage, "Client", return_value=storage
                ),
                unittest.mock.patch.object(
                    evaluate_module,
                    "download_jsonl_manifest_strict",
                    return_value=[
                        _row("gs://audio/eval.flac", "eval transcript")
                    ],
                ),
                unittest.mock.patch.object(
                    evaluate_module,
                    "run_online_target_inference",
                    unittest.mock.AsyncMock(return_value=online_preds),
                ) as run_online,
                unittest.mock.patch.object(
                    evaluate_module, "batch_infer"
                ) as batch,
                unittest.mock.patch.object(
                    evaluate_module, "RESULTS_DIR", tmp / "results"
                ),
                _patched_eval_scoring(),
            ):
                rc = evaluate_module.evaluate(args)

        self.assertEqual(rc, 0)
        run_online.assert_awaited_once()
        self.assertEqual(run_online.call_args.kwargs["concurrency"], 4)
        self.assertEqual(run_online.call_args.kwargs["max_retries"], 1)
        batch.assert_not_called()

    def test_eval_rejects_invalid_durable_eval_model_before_submit(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp_s:
            tmp = pathlib.Path(tmp_s)
            storage = fake_gcs.FakeStorageClient()
            _seed_source_manifests(storage)
            cfg_path = _write_config_file(tmp)
            run_cfg = config_module.load_run_config(cfg_path)
            config = run_cfg.to_record_dict()
            config["eval_model"] = {
                "label": "bad label",
                "model": "gemini-3.1-flash-lite",
            }
            storage.put(run_cfg.paths.config_uri, json.dumps(config))
            args = argparse.Namespace(config=str(cfg_path))

            with (
                unittest.mock.patch.object(
                    evaluate_module.storage, "Client", return_value=storage
                ),
                unittest.mock.patch.object(
                    evaluate_module, "download_jsonl_manifest_strict"
                ) as download_manifest,
                unittest.mock.patch.object(
                    evaluate_module, "submit_batch_inference"
                ) as submit,
            ):
                rc = evaluate_module.evaluate(args)

        self.assertEqual(rc, 1)
        download_manifest.assert_not_called()
        submit.assert_not_called()

    def test_eval_endpoint_eval_model_runs_as_online_target(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp_s:
            tmp = pathlib.Path(tmp_s)
            storage = fake_gcs.FakeStorageClient()
            _seed_source_manifests(storage)
            cfg_path = _write_config_file(
                tmp,
                eval_label="checkpoint_6",
                eval_model="projects/p/locations/us/endpoints/123",
            )
            run_cfg = config_module.load_run_config(cfg_path)
            config = run_cfg.to_record_dict()
            storage.put(run_cfg.paths.config_uri, json.dumps(config))
            online_preds = _online_prediction_map(
                {"gs://audio/eval.flac": "eval transcript"},
                run_gcs_prefix=run_cfg.paths.gcs_prefix,
                label="checkpoint_6",
            )
            args = argparse.Namespace(config=str(cfg_path))

            with (
                unittest.mock.patch.object(
                    evaluate_module.storage, "Client", return_value=storage
                ),
                unittest.mock.patch.object(
                    evaluate_module,
                    "download_jsonl_manifest_strict",
                    return_value=[
                        _row("gs://audio/eval.flac", "eval transcript")
                    ],
                ) as download_manifest,
                unittest.mock.patch.object(
                    evaluate_module,
                    "run_online_target_inference",
                    unittest.mock.AsyncMock(return_value=online_preds),
                ) as run_online,
                unittest.mock.patch.object(
                    evaluate_module, "batch_infer"
                ) as batch,
                unittest.mock.patch.object(
                    evaluate_module, "RESULTS_DIR", tmp / "results"
                ),
                _patched_eval_scoring(),
            ):
                rc = evaluate_module.evaluate(args)

        self.assertEqual(rc, 0)
        download_manifest.assert_called_once()
        run_online.assert_awaited_once()
        batch.assert_not_called()

    def test_eval_all_online_failures_skip_reports_and_durable_success(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp_s:
            tmp = pathlib.Path(tmp_s)
            storage = fake_gcs.FakeStorageClient()
            cfg_path = _write_config_file(
                tmp,
                eval_label="checkpoint_6",
                eval_model="projects/p/locations/us/endpoints/123",
            )
            run_cfg = config_module.load_run_config(cfg_path)
            config = run_cfg.to_record_dict()
            storage.put(run_cfg.paths.config_uri, json.dumps(config))
            online_preds = _online_prediction_map(
                {},
                run_gcs_prefix=run_cfg.paths.gcs_prefix,
                label="checkpoint_6",
                error_count=1,
            )
            args = argparse.Namespace(config=str(cfg_path))

            with (
                unittest.mock.patch.object(
                    evaluate_module.storage, "Client", return_value=storage
                ),
                unittest.mock.patch.object(
                    evaluate_module,
                    "download_jsonl_manifest_strict",
                    return_value=[
                        _row("gs://audio/eval.flac", "eval transcript")
                    ],
                ),
                unittest.mock.patch.object(
                    evaluate_module,
                    "run_online_target_inference",
                    unittest.mock.AsyncMock(return_value=online_preds),
                ),
                unittest.mock.patch.object(
                    evaluate_module, "RESULTS_DIR", tmp / "results"
                ),
                _patched_eval_scoring(),
            ):
                rc = evaluate_module.evaluate(args)

            manifest_module = evaluate_module.inference_manifest
            manifest_path = manifest_module.build_inference_manifest_blob_path(
                inference_dataset_slug="echo/eval",
                model_family_slug="gemini_3_1_flash_lite",
                run_id=run_cfg.round_id,
                artifact_label="checkpoint_6",
            )
            normalized_uri = f"gs://{run_cfg.gcs_bucket}/{manifest_path}"
            summary = sft_eval_fixtures.summary_artifacts(
                run_cfg.paths.gcs_prefix
            )
            durable = json.loads(storage.get(run_cfg.paths.config_uri))

            self.assertEqual(rc, 1)
            self.assertFalse(storage.has(normalized_uri))
            self.assertFalse(storage.has(summary["summary_json_uri"]))
            self.assertFalse(storage.has(summary["summary_markdown_uri"]))
            self.assertNotIn("last_eval_at", durable)

    def test_online_unresolved_error_scores_as_missing_empty_hypothesis(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp_s:
            tmp = pathlib.Path(tmp_s)
            storage = fake_gcs.FakeStorageClient()
            _seed_source_manifests(storage)
            cfg_path = _write_config_file(
                tmp,
                eval_label="checkpoint_6",
                eval_model="projects/p/locations/us/endpoints/123",
            )
            run_cfg = config_module.load_run_config(cfg_path)
            config = run_cfg.to_record_dict()
            storage.put(run_cfg.paths.config_uri, json.dumps(config))
            online_preds = _online_prediction_map(
                {"gs://audio/eval.flac": "eval transcript"},
                run_gcs_prefix=run_cfg.paths.gcs_prefix,
                label="checkpoint_6",
                error_count=1,
            )
            args = argparse.Namespace(config=str(cfg_path))

            with (
                unittest.mock.patch.object(
                    evaluate_module.storage, "Client", return_value=storage
                ),
                unittest.mock.patch.object(
                    evaluate_module,
                    "download_jsonl_manifest_strict",
                    return_value=[
                        _row(
                            "gs://audio/eval.flac",
                            "eval transcript",
                            example_id="eval-1",
                        ),
                        _row(
                            "gs://audio/error.flac",
                            "missing transcript",
                            example_id="eval-2",
                        ),
                    ],
                ),
                unittest.mock.patch.object(
                    evaluate_module,
                    "run_online_target_inference",
                    unittest.mock.AsyncMock(return_value=online_preds),
                ),
                unittest.mock.patch.object(
                    evaluate_module, "RESULTS_DIR", tmp / "results"
                ),
                _patched_eval_scoring(),
            ):
                rc = evaluate_module.evaluate(args)

            metrics = json.loads(
                (tmp / "results" / "round-a" / "wer_summary.json").read_text(
                    encoding="utf-8"
                )
            )
            target = metrics["target"]
            manifest_rows = [
                json.loads(line)
                for line in storage.get(
                    target["artifacts"]["normalized_manifest_uri"]
                ).splitlines()
            ]

        self.assertEqual(rc, 0)
        self.assertEqual(target["missing_prediction_count"], 1)
        self.assertEqual(target["metadata"]["online_error_count"], 1)
        self.assertEqual(target["empty_or_unintelligible_rate"], 50.0)
        self.assertEqual(
            manifest_rows[0]["pred_text_gemini_3_1_flash_lite"],
            "eval transcript",
        )
        self.assertNotIn("pred_text_gemini_3_1_flash_lite", manifest_rows[1])

    def test_batch_infer_fails_when_vertex_writes_no_jsonl(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_s:
            tmp = pathlib.Path(tmp_s)
            storage = fake_gcs.FakeStorageClient()
            run_cfg = config_module.load_run_config(_write_config_file(tmp))
            output_uri = sft_eval_fixtures.batch_output_uri(
                run_cfg.paths.gcs_prefix
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
                    prior_context_count=0,
                    prior_context_mode="text_turns",
                    eval_manifest_uri=run_cfg.paths.canonical_eval_uri,
                )
            metadata_uri = eval_artifacts.batch_prediction_metadata_uri(
                run_cfg.paths.gcs_prefix,
                "base",
            )

        self.assertIsNone(preds)
        self.assertFalse(storage.has(metadata_uri))

    def test_batch_infer_rejects_existing_output_without_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_s:
            tmp = pathlib.Path(tmp_s)
            storage = fake_gcs.FakeStorageClient()
            run_cfg = config_module.load_run_config(_write_config_file(tmp))
            output_uri = sft_eval_fixtures.batch_output_uri(
                run_cfg.paths.gcs_prefix
            )
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
                                                "fileUri": (
                                                    "gs://audio/eval.flac"
                                                )
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
            eval_rows = [
                types.SimpleNamespace(audio_filepath="gs://audio/eval.flac")
            ]

            with unittest.mock.patch.object(
                evaluate_module,
                "submit_batch_inference",
            ) as submit:
                with self.assertRaisesRegex(
                    ValueError,
                    "batch prediction metadata missing",
                ):
                    evaluate_module.batch_infer(
                        storage_client=storage,
                        run_gcs_prefix=run_cfg.paths.gcs_prefix,
                        gcp_project=run_cfg.gcp_project,
                        location=run_cfg.location,
                        model_id="gemini-3.1-flash-lite",
                        label="base",
                        eval_rows=eval_rows,
                        system_prompt="sys",
                        user_prompt="user",
                        prior_context_count=0,
                        prior_context_mode="text_turns",
                        eval_manifest_uri=run_cfg.paths.canonical_eval_uri,
                    )

        submit.assert_not_called()

    def test_batch_infer_rejects_existing_output_metadata_mismatch(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp_s:
            tmp = pathlib.Path(tmp_s)
            storage = fake_gcs.FakeStorageClient()
            run_cfg = config_module.load_run_config(_write_config_file(tmp))
            output_uri = sft_eval_fixtures.batch_output_uri(
                run_cfg.paths.gcs_prefix
            )
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
                                                "fileUri": (
                                                    "gs://audio/eval.flac"
                                                )
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
            sft_eval_fixtures.put_batch_metadata(
                storage,
                run_gcs_prefix=run_cfg.paths.gcs_prefix,
                eval_manifest_uri=run_cfg.paths.canonical_eval_uri,
                audio_uris=["gs://audio/eval.flac"],
                system_prompt="old sys",
                user_prompt="user",
            )
            eval_rows = [
                types.SimpleNamespace(audio_filepath="gs://audio/eval.flac")
            ]

            with unittest.mock.patch.object(
                evaluate_module,
                "submit_batch_inference",
            ) as submit:
                with self.assertRaisesRegex(
                    ValueError,
                    "batch prediction request identity mismatch",
                ):
                    evaluate_module.batch_infer(
                        storage_client=storage,
                        run_gcs_prefix=run_cfg.paths.gcs_prefix,
                        gcp_project=run_cfg.gcp_project,
                        location=run_cfg.location,
                        model_id="gemini-3.1-flash-lite",
                        label="base",
                        eval_rows=eval_rows,
                        system_prompt="sys",
                        user_prompt="user",
                        prior_context_count=0,
                        prior_context_mode="text_turns",
                        eval_manifest_uri=run_cfg.paths.canonical_eval_uri,
                    )

        submit.assert_not_called()

    def test_batch_infer_reuses_exact_metadata_without_submit(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_s:
            tmp = pathlib.Path(tmp_s)
            storage = fake_gcs.FakeStorageClient()
            run_cfg = config_module.load_run_config(_write_config_file(tmp))
            output_uri = sft_eval_fixtures.batch_output_uri(
                run_cfg.paths.gcs_prefix
            )
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
                                                "fileUri": (
                                                    "gs://audio/eval.flac"
                                                )
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
            sft_eval_fixtures.put_batch_metadata(
                storage,
                run_gcs_prefix=run_cfg.paths.gcs_prefix,
                eval_manifest_uri=run_cfg.paths.canonical_eval_uri,
                audio_uris=["gs://audio/eval.flac"],
                system_prompt="sys",
                user_prompt="user",
            )
            eval_rows = [
                types.SimpleNamespace(audio_filepath="gs://audio/eval.flac")
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
                    prior_context_count=0,
                    prior_context_mode="text_turns",
                    eval_manifest_uri=run_cfg.paths.canonical_eval_uri,
                )

        submit.assert_not_called()
        self.assertEqual(preds["gs://audio/eval.flac"], "eval transcript")
        self.assertEqual(preds.output_uri, output_uri)

    def test_batch_infer_rejects_duplicate_eval_audio_uris(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_s:
            tmp = pathlib.Path(tmp_s)
            storage = fake_gcs.FakeStorageClient()
            run_cfg = config_module.load_run_config(_write_config_file(tmp))
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
                    prior_context_count=0,
                    prior_context_mode="text_turns",
                    eval_manifest_uri=run_cfg.paths.canonical_eval_uri,
                )

        self.assertIsNone(preds)
        submit.assert_not_called()

    def test_batch_infer_rejects_prediction_uri_outside_eval_manifest(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp_s:
            tmp = pathlib.Path(tmp_s)
            storage = fake_gcs.FakeStorageClient()
            run_cfg = config_module.load_run_config(_write_config_file(tmp))
            output_uri = sft_eval_fixtures.batch_output_uri(
                run_cfg.paths.gcs_prefix
            )
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
                                                "fileUri": (
                                                    "gs://audio/other.flac"
                                                )
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
            sft_eval_fixtures.put_batch_metadata(
                storage,
                run_gcs_prefix=run_cfg.paths.gcs_prefix,
                eval_manifest_uri=run_cfg.paths.canonical_eval_uri,
                audio_uris=["gs://audio/eval.flac"],
                system_prompt="sys",
                user_prompt="user",
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
                    prior_context_count=0,
                    prior_context_mode="text_turns",
                    eval_manifest_uri=run_cfg.paths.canonical_eval_uri,
                )

        self.assertIsNone(preds)
