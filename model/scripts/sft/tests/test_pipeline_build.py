"""TDD tests for pipeline.py build subcommand, preflight.py, and adapters.

RED phase: tests written before implementation — all should FAIL until
implementation is complete.
"""

from __future__ import annotations

import argparse
import contextlib
import io
import json
import os
import sys
import tempfile
import types
import unittest
import unittest.mock
from pathlib import Path

# Ensure scripts/sft/ and model/colabs/ (for common) are on path
_SFT_DIR = str(Path(__file__).resolve().parent.parent)
_COLABS_DIR = str(
    Path(__file__).resolve().parent.parent.parent.parent / "colabs"
)
if _SFT_DIR not in sys.path:
    sys.path.insert(0, _SFT_DIR)
if _COLABS_DIR not in sys.path:
    sys.path.insert(0, _COLABS_DIR)


class TestPipelineCLI(unittest.TestCase):
    """pipeline.py --help and build --help exit 0 and show expected subcommands."""

    def test_help_exits_zero(self) -> None:
        """Python pipeline.py --help exits 0."""
        import pipeline

        # Re-parse with --help should raise SystemExit(0)
        with self.assertRaises(SystemExit) as ctx:
            with unittest.mock.patch("sys.argv", ["pipeline.py", "--help"]):
                pipeline.main()
        self.assertEqual(ctx.exception.code, 0)

    def test_build_help_exits_zero(self) -> None:
        """Python pipeline.py build --help exits 0."""
        import pipeline

        with self.assertRaises(SystemExit) as ctx:
            with unittest.mock.patch(
                "sys.argv", ["pipeline.py", "build", "--help"]
            ):
                pipeline.main()
        self.assertEqual(ctx.exception.code, 0)

    def test_subcommands_listed(self) -> None:
        """Verify main() is callable and returns int."""
        import pipeline

        self.assertTrue(callable(pipeline.main))

    def test_help_names_gemini_sft_pipeline(self) -> None:
        """Main help labels the CLI as Gemini SFT-specific."""
        import pipeline

        out = io.StringIO()
        with self.assertRaises(SystemExit) as ctx:
            with (
                unittest.mock.patch("sys.argv", ["pipeline.py", "--help"]),
                contextlib.redirect_stdout(out),
            ):
                pipeline.main()

        self.assertEqual(ctx.exception.code, 0)
        self.assertIn("Gemini SFT pipeline", out.getvalue())
        self.assertIn("Batch-infer and score Gemini model", out.getvalue())

    def test_build_returns_clean_error_for_missing_prompt_file(self) -> None:
        """Missing @file prompt overrides should fail cleanly, not traceback."""
        import pipeline

        missing = Path(tempfile.gettempdir()) / "missing-sft-system-prompt.txt"
        args = argparse.Namespace(
            datasets="echo",
            round_id="test-round",
            system_prompt=f"@{missing}",
            user_prompt="",
            staging_dir="",
        )

        with self.assertLogs("pipeline", level="ERROR") as logs:
            rc = pipeline._build(args)

        self.assertEqual(rc, 1)
        self.assertIn("system prompt file not found", "\n".join(logs.output))
        self.assertIn(str(missing), "\n".join(logs.output))

    def test_dataset_names_are_deduped_in_order(self) -> None:
        """Repeated --datasets entries should not run adapters twice."""
        import pipeline

        self.assertEqual(
            pipeline._parse_dataset_names("echo, echo,bench,echo"),
            ["echo", "bench"],
        )

    def test_dataset_names_ignore_empty_entries(self) -> None:
        """Trailing commas and empty comma slots should not create fake datasets."""
        import pipeline

        self.assertEqual(
            pipeline._parse_dataset_names(" echo, ,bench,echo, "),
            ["echo", "bench"],
        )

    def test_build_rejects_empty_dataset_list(self) -> None:
        """All-empty --datasets should fail before staging or GCS work."""
        import pipeline

        args = argparse.Namespace(
            datasets=" , ",
            round_id="empty-datasets",
            system_prompt="",
            user_prompt="",
            staging_dir="",
            gcp_project="",
            gcs_bucket="",
        )

        with self.assertLogs("pipeline", level="ERROR") as logs:
            rc = pipeline._build(args)

        self.assertEqual(rc, 1)
        self.assertIn("No datasets specified", "\n".join(logs.output))

    def test_gcs_sft_prefix_rejects_full_gcs_uri(self) -> None:
        """--gcs-bucket expects a bucket name, not a gs:// URI."""
        import pipeline

        with self.assertRaisesRegex(ValueError, "bucket name"):
            pipeline._gcs_sft_prefix("gs://sandbox-bucket")

    def test_tune_defaults_to_gemini_31_flash_lite(self) -> None:
        """Tune CLI defaults to the current supported Gemini SFT model."""
        import pipeline

        with unittest.mock.patch("pipeline._tune") as mock_tune:
            mock_tune.return_value = 0
            with unittest.mock.patch(
                "sys.argv",
                ["pipeline.py", "tune", "--round-id", "2026-06-01-echo"],
            ):
                pipeline.main()

        args = mock_tune.call_args.args[0]
        self.assertEqual(args.base_model, "gemini-3.1-flash-lite")

    def test_tune_accepts_gcp_overrides(self) -> None:
        """Tune CLI accepts sandbox GCP project/bucket overrides."""
        import pipeline

        with unittest.mock.patch("pipeline._tune") as mock_tune:
            mock_tune.return_value = 0
            with unittest.mock.patch(
                "sys.argv",
                [
                    "pipeline.py",
                    "tune",
                    "--round-id",
                    "2026-06-01-echo",
                    "--gcp-project",
                    "sandbox-project",
                    "--gcs-bucket",
                    "sandbox-bucket",
                ],
            ):
                pipeline.main()

        args = mock_tune.call_args.args[0]
        self.assertEqual(args.gcp_project, "sandbox-project")
        self.assertEqual(args.gcs_bucket, "sandbox-bucket")

    def test_gcp_settings_prefer_cli_then_config_then_env(self) -> None:
        """GCP settings resolve consistently across build/tune/eval reruns."""
        import pipeline

        with unittest.mock.patch.dict(
            os.environ,
            {
                "SFT_GCP_PROJECT": "env-project",
                "SFT_GCS_BUCKET": "env-bucket",
            },
            clear=False,
        ):
            self.assertEqual(
                pipeline._resolve_gcp_project(
                    argparse.Namespace(gcp_project="cli-project"),
                    {"gcp_project": "config-project"},
                ),
                "cli-project",
            )
            self.assertEqual(
                pipeline._resolve_gcs_bucket(
                    argparse.Namespace(gcs_bucket="cli-bucket"),
                    {"gcs_bucket": "config-bucket"},
                ),
                "cli-bucket",
            )
            self.assertEqual(
                pipeline._resolve_gcp_project(
                    argparse.Namespace(gcp_project=""),
                    {"gcp_project": "config-project"},
                ),
                "config-project",
            )
            self.assertEqual(
                pipeline._resolve_gcs_bucket(
                    argparse.Namespace(gcs_bucket=""),
                    {"gcs_bucket": "config-bucket"},
                ),
                "config-bucket",
            )
            self.assertEqual(
                pipeline._resolve_gcp_project(
                    argparse.Namespace(gcp_project=""), {}
                ),
                "env-project",
            )
            self.assertEqual(
                pipeline._resolve_gcs_bucket(
                    argparse.Namespace(gcs_bucket=""), {}
                ),
                "env-bucket",
            )

    def test_tune_accepts_gemini_31_flash_lite_before_gcp_calls(self) -> None:
        """Gemini 3.1 Flash-Lite is supported for SFT and must not hit old gemini-3 rejection."""
        import pipeline

        args = argparse.Namespace(
            round_id="2026-06-01-echo",
            base_model="gemini-3.1-flash-lite",
            gcp_project="",
            gcs_bucket="",
            location="us-central1",
            epochs=1,
            adapter_size="EIGHT",
            lr_multiplier=1.0,
            confirm=True,
        )

        with (
            unittest.mock.patch("pipeline._load_round_config", return_value={}),
            self.assertLogs("pipeline", level="ERROR") as logs,
        ):
            rc = pipeline._tune(args)

        self.assertEqual(rc, 1)
        joined = "\n".join(logs.output)
        self.assertNotIn("rejected", joined)
        self.assertIn("Run `build` first", joined)

    def test_tune_cost_estimate_uses_gemini_31_flash_lite_rate(self) -> None:
        """Tune cost estimate references Gemini 3.1 Flash-Lite SFT pricing."""
        import pipeline

        self.assertEqual(pipeline.DEFAULT_BASE_MODEL, "gemini-3.1-flash-lite")
        self.assertEqual(pipeline.FALLBACK_SEGMENT_DURATION_SECONDS, 15.0)
        self.assertEqual(
            pipeline.SFT_TRAINING_COST_PER_MILLION["gemini-3.1-flash-lite"],
            3.00,
        )
        self.assertEqual(
            pipeline.SFT_MODEL_DISPLAY_NAMES["gemini-3.1-flash-lite"],
            "Gemini 3.1 Flash-Lite",
        )

    def test_tune_resume_path_checks_vertex_extra(self) -> None:
        """Resume path should use common.vertex's friendly dependency guard."""
        import pipeline

        fake_cur = unittest.mock.MagicMock()
        fake_cur.state.name = "JOB_STATE_SUCCEEDED"
        fake_client = unittest.mock.MagicMock()
        fake_client.tunings.get.return_value = fake_cur
        fake_genai = types.SimpleNamespace(
            Client=unittest.mock.MagicMock(return_value=fake_client)
        )
        fake_google = types.SimpleNamespace(genai=fake_genai)
        real_import = __import__

        def _fake_import(
            name, global_vars=None, local_vars=None, fromlist=(), level=0
        ):
            if name == "google" and "genai" in fromlist:
                return fake_google
            return real_import(name, global_vars, local_vars, fromlist, level)

        args = argparse.Namespace(
            round_id="2026-06-01-echo",
            base_model="gemini-3.1-flash-lite",
            gcp_project="",
            gcs_bucket="",
            location="us-central1",
            epochs=1,
            adapter_size="EIGHT",
            lr_multiplier=1.0,
            confirm=True,
        )

        with (
            unittest.mock.patch(
                "pipeline._load_round_config",
                return_value={
                    "job_name": "projects/p/locations/l/tuningJobs/123",
                    "endpoint": "projects/p/locations/l/endpoints/456",
                },
            ),
            unittest.mock.patch("common.vertex._require_vertex") as require,
            unittest.mock.patch("builtins.__import__", new=_fake_import),
        ):
            rc = pipeline._tune(args)

        self.assertEqual(rc, 0)
        require.assert_called_once_with()

    def test_tune_declined_confirmation_returns_user_abort(self) -> None:
        """Declined cost confirmation should stop `all` before eval."""
        import pipeline

        args = argparse.Namespace(
            round_id="2026-06-01-echo",
            base_model="gemini-3.1-flash-lite",
            gcp_project="",
            gcs_bucket="",
            location="us-central1",
            epochs=1,
            adapter_size="EIGHT",
            lr_multiplier=1.0,
            confirm=False,
        )

        def fake_download_blob_to_file(
            _client, _bucket: str, _blob: str, local_path: str
        ) -> None:
            Path(local_path).write_text('{"ok": true}\n')

        with (
            unittest.mock.patch(
                "pipeline._load_round_config",
                return_value={
                    "combined_train_uri": "gs://bucket/train.jsonl",
                    "combined_val_uri": "",
                    "system_prompt": "sys",
                    "user_prompt": "user",
                    "total_train_duration_seconds": 10,
                },
            ),
            unittest.mock.patch("google.cloud.storage.Client"),
            unittest.mock.patch(
                "common.gcs_utils.download_blob_to_file",
                side_effect=fake_download_blob_to_file,
            ),
            unittest.mock.patch(
                "preflight.run_preflight",
                return_value=types.SimpleNamespace(passed=True, failures=[]),
            ),
            unittest.mock.patch("builtins.input", return_value="no"),
            unittest.mock.patch("common.vertex.submit_tuning_job") as submit,
        ):
            rc = pipeline._tune(args)

        self.assertEqual(rc, 130)
        submit.assert_not_called()

    def test_tune_eof_confirmation_returns_user_abort(self) -> None:
        """Non-interactive tune without --confirm should abort cleanly."""
        import pipeline

        args = argparse.Namespace(
            round_id="2026-06-01-echo",
            base_model="gemini-3.1-flash-lite",
            gcp_project="",
            gcs_bucket="",
            location="us-central1",
            epochs=1,
            adapter_size="EIGHT",
            lr_multiplier=1.0,
            confirm=False,
        )

        def fake_download_blob_to_file(
            _client, _bucket: str, _blob: str, local_path: str
        ) -> None:
            Path(local_path).write_text('{"ok": true}\n')

        with (
            unittest.mock.patch(
                "pipeline._load_round_config",
                return_value={
                    "combined_train_uri": "gs://bucket/train.jsonl",
                    "combined_val_uri": "",
                    "system_prompt": "sys",
                    "user_prompt": "user",
                    "total_train_duration_seconds": 10,
                },
            ),
            unittest.mock.patch("google.cloud.storage.Client"),
            unittest.mock.patch(
                "common.gcs_utils.download_blob_to_file",
                side_effect=fake_download_blob_to_file,
            ),
            unittest.mock.patch(
                "preflight.run_preflight",
                return_value=types.SimpleNamespace(passed=True, failures=[]),
            ),
            unittest.mock.patch("builtins.input", side_effect=EOFError),
            unittest.mock.patch("common.vertex.submit_tuning_job") as submit,
            self.assertLogs("pipeline", level="INFO") as logs,
        ):
            rc = pipeline._tune(args)

        self.assertEqual(rc, 130)
        self.assertIn("Tune aborted by operator", "\n".join(logs.output))
        submit.assert_not_called()

    def test_tune_fallback_duration_constant_is_used_in_cost_estimate(
        self,
    ) -> None:
        """Older build configs without recorded durations use the named fallback."""
        import pipeline

        args = argparse.Namespace(
            round_id="2026-06-01-echo",
            base_model="gemini-3.1-flash-lite",
            gcp_project="",
            gcs_bucket="",
            location="us-central1",
            epochs=1,
            adapter_size="EIGHT",
            lr_multiplier=1.0,
            confirm=False,
        )

        def fake_download_blob_to_file(
            _client, _bucket: str, _blob: str, local_path: str
        ) -> None:
            Path(local_path).write_text('{"ok": true}\n{"ok": true}\n')

        stdout = io.StringIO()
        with (
            unittest.mock.patch(
                "pipeline._load_round_config",
                return_value={
                    "combined_train_uri": "gs://bucket/train.jsonl",
                    "combined_val_uri": "",
                    "system_prompt": "sys",
                    "user_prompt": "user",
                },
            ),
            unittest.mock.patch("google.cloud.storage.Client"),
            unittest.mock.patch(
                "common.gcs_utils.download_blob_to_file",
                side_effect=fake_download_blob_to_file,
            ),
            unittest.mock.patch(
                "preflight.run_preflight",
                return_value=types.SimpleNamespace(passed=True, failures=[]),
            ),
            unittest.mock.patch("builtins.input", return_value="no"),
            contextlib.redirect_stdout(stdout),
        ):
            rc = pipeline._tune(args)

        self.assertEqual(rc, 130)
        self.assertIn(
            f"2 x {pipeline.FALLBACK_SEGMENT_DURATION_SECONDS:.1f}s avg",
            stdout.getvalue(),
        )


class TestPreflightEmptyTarget(unittest.TestCase):
    """run_preflight aborts on empty model text (validate_example returns False)."""

    def _make_bad_example(self) -> dict:
        """Example with empty model text — validate_example should reject it."""
        return {
            "systemInstruction": {"role": "system", "parts": [{"text": "sys"}]},
            "contents": [
                {
                    "role": "user",
                    "parts": [
                        {
                            "fileData": {
                                "mimeType": "audio/flac",
                                "fileUri": "gs://b/a.flac",
                            }
                        },
                        {"text": "transcribe"},
                    ],
                },
                {"role": "model", "parts": [{"text": ""}]},  # EMPTY — invalid
            ],
        }

    def test_preflight_fails_on_empty_target(self) -> None:
        from preflight import run_preflight

        with tempfile.TemporaryDirectory() as tmp:
            train_path = Path(tmp) / "train.jsonl"
            report_path = Path(tmp) / "preflight_report.json"
            train_path.write_text(json.dumps(self._make_bad_example()) + "\n")

            report = run_preflight(
                train_jsonl_path=train_path,
                val_jsonl_path=None,
                storage_client=None,
                report_path=report_path,
            )

        self.assertFalse(report.passed)
        self.assertTrue(len(report.failures) > 0)

    def test_preflight_report_written_on_failure(self) -> None:
        from preflight import run_preflight

        with tempfile.TemporaryDirectory() as tmp:
            train_path = Path(tmp) / "train.jsonl"
            report_path = Path(tmp) / "preflight_report.json"
            train_path.write_text(json.dumps(self._make_bad_example()) + "\n")

            run_preflight(
                train_jsonl_path=train_path,
                val_jsonl_path=None,
                storage_client=None,
                report_path=report_path,
            )

            # Check inside the context while directory still exists
            self.assertTrue(report_path.exists())
            data = json.loads(report_path.read_text())
            self.assertFalse(data["passed"])


class TestPreflightDuplicateUri(unittest.TestCase):
    """run_preflight fails on duplicate fileUri."""

    def _make_good_example(self, uri: str) -> dict:
        return {
            "systemInstruction": {"role": "system", "parts": [{"text": "sys"}]},
            "contents": [
                {
                    "role": "user",
                    "parts": [
                        {
                            "fileData": {
                                "mimeType": "audio/flac",
                                "fileUri": uri,
                            }
                        },
                        {"text": "transcribe"},
                    ],
                },
                {"role": "model", "parts": [{"text": "engine 41 responding"}]},
            ],
        }

    def test_preflight_fails_on_duplicate_uri(self) -> None:
        from preflight import run_preflight

        duplicate_uri = "gs://bucket/audio/duplicate.flac"
        with tempfile.TemporaryDirectory() as tmp:
            train_path = Path(tmp) / "train.jsonl"
            report_path = Path(tmp) / "preflight_report.json"
            lines = [
                json.dumps(self._make_good_example(duplicate_uri)) + "\n",
                json.dumps(self._make_good_example(duplicate_uri))
                + "\n",  # duplicate
            ]
            train_path.write_text("".join(lines))

            report = run_preflight(
                train_jsonl_path=train_path,
                val_jsonl_path=None,
                storage_client=None,
                report_path=report_path,
            )

        self.assertFalse(report.passed)
        self.assertTrue(any("uplicate" in f for f in report.failures))

    def test_preflight_passes_on_valid_data(self) -> None:
        from preflight import run_preflight

        with tempfile.TemporaryDirectory() as tmp:
            train_path = Path(tmp) / "train.jsonl"
            report_path = Path(tmp) / "preflight_report.json"
            train_path.write_text(
                json.dumps(self._make_good_example("gs://b/audio1.flac")) + "\n"
            )

            # storage_client=None means no reachability check
            report = run_preflight(
                train_jsonl_path=train_path,
                val_jsonl_path=None,
                storage_client=None,
                report_path=report_path,
            )

        self.assertTrue(
            report.passed, f"Unexpected failures: {report.failures}"
        )

    def test_safe_blob_exists_uses_timeout(self) -> None:
        from preflight import PREFLIGHT_GCS_TIMEOUT_SECONDS, _safe_blob_exists

        storage_client = object()
        with unittest.mock.patch(
            "preflight.blob_exists", return_value=True
        ) as mock_exists:
            ok = _safe_blob_exists(storage_client, "gs://b/audio.flac")

        self.assertTrue(ok)
        mock_exists.assert_called_once_with(
            storage_client,
            "gs://b/audio.flac",
            timeout=PREFLIGHT_GCS_TIMEOUT_SECONDS,
        )

    def test_preflight_batches_gcs_reachability_checks(self) -> None:
        from preflight import run_preflight

        with tempfile.TemporaryDirectory() as tmp:
            train_path = Path(tmp) / "train.jsonl"
            report_path = Path(tmp) / "preflight_report.json"
            train_path.write_text(
                "".join(
                    json.dumps(self._make_good_example(f"gs://b/audio{i}.flac"))
                    + "\n"
                    for i in range(5)
                )
            )

            with (
                unittest.mock.patch(
                    "preflight._safe_blob_exists",
                    side_effect=lambda _client, _uri: True,
                ) as mock_exists,
                unittest.mock.patch("preflight.time.sleep") as mock_sleep,
            ):
                report = run_preflight(
                    train_jsonl_path=train_path,
                    val_jsonl_path=None,
                    storage_client=object(),
                    report_path=report_path,
                    gcs_max_workers=2,
                    gcs_batch_size=2,
                    gcs_batch_pause_seconds=0.5,
                )

        self.assertTrue(
            report.passed, f"Unexpected failures: {report.failures}"
        )
        self.assertEqual(mock_exists.call_count, 5)
        self.assertEqual(mock_sleep.call_count, 2)
        mock_sleep.assert_has_calls(
            [unittest.mock.call(0.5), unittest.mock.call(0.5)]
        )

    def test_preflight_validates_val_examples(self) -> None:
        from preflight import run_preflight

        with tempfile.TemporaryDirectory() as tmp:
            train_path = Path(tmp) / "train.jsonl"
            val_path = Path(tmp) / "val.jsonl"
            report_path = Path(tmp) / "preflight_report.json"
            train_path.write_text(
                json.dumps(self._make_good_example("gs://b/train.flac")) + "\n"
            )
            val_path.write_text(
                json.dumps(self._make_good_example("gs://b/missing.flac"))
                + "\n"
            )

            with unittest.mock.patch(
                "preflight._safe_blob_exists",
                side_effect=lambda _client, uri: uri != "gs://b/missing.flac",
            ):
                report = run_preflight(
                    train_jsonl_path=train_path,
                    val_jsonl_path=val_path,
                    storage_client=object(),
                    report_path=report_path,
                )

        self.assertFalse(report.passed)
        self.assertTrue(
            any("val[0]: fileUri not reachable" in f for f in report.failures)
        )
        self.assertIn("val[0]", report.offending_ids)

    def test_preflight_rejects_malformed_val_examples(self) -> None:
        from preflight import run_preflight

        with tempfile.TemporaryDirectory() as tmp:
            train_path = Path(tmp) / "train.jsonl"
            val_path = Path(tmp) / "val.jsonl"
            report_path = Path(tmp) / "preflight_report.json"
            train_path.write_text(
                json.dumps(self._make_good_example("gs://b/train.flac")) + "\n"
            )
            val_path.write_text(json.dumps(self._make_good_example("")) + "\n")

            report = run_preflight(
                train_jsonl_path=train_path,
                val_jsonl_path=val_path,
                storage_client=None,
                report_path=report_path,
            )

        self.assertFalse(report.passed)
        self.assertTrue(
            any("val[0]: failed validate_example" in f for f in report.failures)
        )
        self.assertIn("val[0]", report.offending_ids)

    def test_preflight_fails_on_empty_train(self) -> None:
        from preflight import run_preflight

        with tempfile.TemporaryDirectory() as tmp:
            train_path = Path(tmp) / "train.jsonl"
            report_path = Path(tmp) / "preflight_report.json"
            train_path.write_text("")  # empty

            report = run_preflight(
                train_jsonl_path=train_path,
                val_jsonl_path=None,
                storage_client=None,
                report_path=report_path,
            )

        self.assertFalse(report.passed)


class TestGcsManifestAdapter(unittest.TestCase):
    """GcsManifestAdapter.iter_rows yields CanonicalRow instances."""

    def test_iter_rows_yields_canonical_rows(self) -> None:
        from adapters.gcs_manifest import GcsManifestAdapter
        from common.manifest import CanonicalRow

        fake_manifest = [
            {
                "audio_filepath": "gs://bucket/audio/seg_001.flac",
                "example_id": "ex001",
                "segment_id": "001",
                "offset": 1.5,
                "duration": 2.8,
                "text": "engine 41 responding",
            }
        ]

        mock_client = unittest.mock.MagicMock()

        with unittest.mock.patch(
            "adapters.gcs_manifest.download_jsonl_manifest",
            return_value=fake_manifest,
        ):
            adapter = GcsManifestAdapter(
                manifest_uri="gs://bucket/manifests/train.jsonl",
                storage_client=mock_client,
            )
            rows = list(adapter.iter_rows())

        self.assertEqual(len(rows), 1)
        self.assertIsInstance(rows[0], CanonicalRow)
        self.assertEqual(
            rows[0].audio_filepath, "gs://bucket/audio/seg_001.flac"
        )
        self.assertEqual(rows[0].text, "engine 41 responding")

    def test_import_succeeds(self) -> None:
        from adapters.gcs_manifest import GcsManifestAdapter

        self.assertTrue(callable(GcsManifestAdapter))


class TestPreflightTokenCap(unittest.TestCase):
    """PREFLIGHT_TOKEN_CAP is 131_072."""

    def test_token_cap_value(self) -> None:
        from preflight import PREFLIGHT_TOKEN_CAP

        self.assertEqual(PREFLIGHT_TOKEN_CAP, 131_072)


class TestBuildSplitJsonl(unittest.TestCase):
    """_build_split_jsonl writes per-dataset + combined JSONL and returns URIs + duration."""

    def test_train_split_builds_uploads_and_sums_duration(self) -> None:
        from types import SimpleNamespace

        import pipeline

        rows = [
            SimpleNamespace(
                audio_filepath="gs://b/a1.flac",
                text="engine 41 responding",
                duration=2.0,
            ),
            SimpleNamespace(
                audio_filepath="gs://b/a2.flac",
                text="copy that",
                duration=3.0,
            ),
        ]
        fake_adapter = unittest.mock.MagicMock()
        fake_adapter.iter_rows.return_value = iter(rows)
        registry = {
            "datasets": {
                "echo": {
                    "adapter": "gcs_manifest",
                    "train_manifest_uri": "gs://b/train.jsonl",
                }
            }
        }

        with tempfile.TemporaryDirectory() as tmp:
            staging = Path(tmp)
            with (
                unittest.mock.patch(
                    "pipeline._make_adapter", return_value=fake_adapter
                ),
                unittest.mock.patch(
                    "common.gcs_utils.upload_file_to_blob"
                ) as mock_upload,
                unittest.mock.patch(
                    "common.gcs_utils.parse_gcs_uri",
                    return_value=("bucket", "path"),
                ),
            ):
                per_uris, combined_uri, total = pipeline._build_split_jsonl(
                    dataset_names=["echo"],
                    registry=registry,
                    split="train",
                    staging_dir=staging,
                    storage_client=object(),
                    normalizer=lambda s: s,
                    system_prompt="sys",
                    user_prompt="transcribe",
                    round_id="r1",
                    gcs_sft_prefix="gs://custom-sft-bucket/sft",
                )

            # Per-dataset staging file written with one line per example -- and NOT
            # truncated to empty by a self-referential combined-concat (regression guard).
            self.assertTrue((staging / "train_echo.jsonl").exists())
            self.assertEqual(
                (staging / "train_echo.jsonl").read_text().count("\n"), 2
            )
            # Returns per-dataset URI map, combined URI, and summed duration
            self.assertIn("echo", per_uris)
            # Single dataset: combined URI reuses the per-dataset URI (no empty re-concat)
            self.assertEqual(combined_uri, per_uris["echo"])
            self.assertEqual(
                combined_uri, "gs://custom-sft-bucket/sft/r1/train_echo.jsonl"
            )
            self.assertTrue(combined_uri.endswith("train_echo.jsonl"))
            self.assertEqual(total, 5.0)
            # Single dataset uploads once (per-dataset only; combined is the same file)
            self.assertEqual(mock_upload.call_count, 1)


class TestEvalManifestResolution(unittest.TestCase):
    """Eval resolves every configured gcs_manifest eval split, not just the first one."""

    def test_resolves_all_configured_gcs_eval_manifests(self) -> None:
        import pipeline

        registry = {
            "datasets": {
                "echo": {
                    "adapter": "gcs_manifest",
                    "eval_manifest_uri": "gs://b/echo_eval.jsonl",
                },
                "bench": {
                    "adapter": "gcs_manifest",
                    "eval_manifest_uri": "gs://b/bench_eval.jsonl",
                },
                "notes": {
                    "adapter": "notes_only",
                    "eval_manifest_uri": "gs://b/ignored.jsonl",
                },
            }
        }

        resolved = pipeline._resolve_eval_manifest_uris(
            registry, ["echo", "bench", "notes"]
        )

        self.assertEqual(
            resolved,
            {
                "echo": "gs://b/echo_eval.jsonl",
                "bench": "gs://b/bench_eval.jsonl",
            },
        )


if __name__ == "__main__":
    unittest.main()
