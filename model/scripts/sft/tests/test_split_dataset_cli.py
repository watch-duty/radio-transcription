from __future__ import annotations

import json
import sys
import tempfile
import unittest
import unittest.mock
from contextlib import redirect_stdout
from io import StringIO
from pathlib import Path

_SFT_DIR = str(Path(__file__).resolve().parent.parent)
_COLABS_DIR = str(
    Path(__file__).resolve().parent.parent.parent.parent / "colabs"
)
if _SFT_DIR not in sys.path:
    sys.path.insert(0, _SFT_DIR)
if _COLABS_DIR not in sys.path:
    sys.path.insert(0, _COLABS_DIR)


class FakeTextReader:
    def __init__(self, values: dict[str, str]) -> None:
        self.values = values

    def read_text(self, uri: str) -> str:
        return self.values[uri]


def _reader(config_uri: str = "gs://configs/radio.toml") -> FakeTextReader:
    return FakeTextReader(
        {
            config_uri: """
dataset_version_id = "radio-v1"
train_ratio = 0.8
eval_ratio = 0.2
output_gcs_prefix = "gs://bucket/out"

[[datasets]]
name = "calls"
family = "bcfy_calls"
manifest_uri = "gs://manifests/calls.jsonl"
source_strategy = "bcfy_calls"
""",
            "gs://manifests/calls.jsonl": "\n".join(
                [
                    (
                        '{"audio_filepath": "gs://audio/100_1700000000.flac", '
                        '"groupId": "100", "text": "alpha dispatch", '
                        '"duration": 8}'
                    ),
                    (
                        '{"audio_filepath": "gs://audio/200_1700000060.flac", '
                        '"groupId": "200", "text": "bravo response", '
                        '"duration": 11}'
                    ),
                    (
                        '{"audio_filepath": "gs://audio/300_1700000120.flac", '
                        '"groupId": "300", "text": "", "duration": 9}'
                    ),
                ]
            )
            + "\n",
        }
    )


class TestSplitDatasetCli(unittest.TestCase):
    def test_dry_run_writes_plan_bundle(self) -> None:
        import split_dataset as cli

        config_uri = "gs://configs/radio.toml"
        with tempfile.TemporaryDirectory() as temp_dir:
            output_dir = Path(temp_dir) / "dry-run-out"
            out = StringIO()
            with (
                unittest.mock.patch.object(
                    cli, "_make_text_reader", return_value=_reader(config_uri)
                ),
                redirect_stdout(out),
            ):
                rc = cli.main(
                    [
                        "dry-run",
                        "--config-uri",
                        config_uri,
                        "--output-dir",
                        str(output_dir),
                    ]
                )

            self.assertEqual(rc, 0)
            self.assertIn(f"Dry run complete: {output_dir}", out.getvalue())
            expected_paths = (
                "reports/dataset_version_report.json",
                "reports/dataset_version_report.md",
                "reports/excluded_rows.jsonl",
                "manifests/canonical/train.jsonl",
                "model_inputs/nemo/train.jsonl",
                "model_inputs/whisper/train.jsonl",
                "model_inputs/gemini/train.jsonl",
            )
            for relative_path in expected_paths:
                self.assertTrue(
                    (output_dir / relative_path).exists(), relative_path
                )
            report = json.loads(
                (output_dir / "reports/dataset_version_report.json").read_text()
            )
            self.assertIs(report["audio_materialized"], False)
            excluded_rows = (
                output_dir / "reports/excluded_rows.jsonl"
            ).read_text()
            self.assertIn('"reason": "empty_text"', excluded_rows)

    def test_hard_failure_writes_no_failure_artifacts(self) -> None:
        import split_dataset as cli

        with tempfile.TemporaryDirectory() as temp_dir:
            out = StringIO()
            with redirect_stdout(out):
                rc = cli.main(
                    [
                        "dry-run",
                        "--config-uri",
                        "./radio.toml",
                        "--output-dir",
                        str(Path(temp_dir) / "dry-run-out"),
                    ]
                )

            self.assertEqual(rc, 1)
            self.assertIn("config_uri must be a gs:// URI", out.getvalue())
            self.assertEqual(list(Path(temp_dir).rglob("*failure*")), [])

    def test_dry_run_rejects_existing_output_dir(self) -> None:
        import split_dataset as cli

        config_uri = "gs://configs/radio.toml"
        with tempfile.TemporaryDirectory() as temp_dir:
            output_dir = Path(temp_dir) / "dry-run-out"
            output_dir.mkdir()
            out = StringIO()
            with (
                unittest.mock.patch.object(
                    cli, "_make_text_reader", return_value=_reader(config_uri)
                ),
                redirect_stdout(out),
            ):
                rc = cli.main(
                    [
                        "dry-run",
                        "--config-uri",
                        config_uri,
                        "--output-dir",
                        str(output_dir),
                    ]
                )

            self.assertEqual(rc, 1)
            self.assertIn("output_dir already exists", out.getvalue())

    def test_generate_dispatches_to_publication_path(self) -> None:
        import split_dataset as cli
        from dataset_split.publisher import (
            DatasetPublicationResult,
            PublishedArtifact,
        )

        config_uri = "gs://configs/radio.toml"
        result = DatasetPublicationResult(
            dataset_version_id="radio-v1",
            root_uri="gs://bucket/out/radio-v1/",
            artifacts=(
                PublishedArtifact(
                    "reports/dataset_version_report",
                    "gs://bucket/out/radio-v1/reports/dataset_version_report.json",
                    "application/json",
                ),
                PublishedArtifact(
                    "model_inputs/gemini/train",
                    "gs://bucket/out/radio-v1/model_inputs/gemini/train.jsonl",
                    "application/x-ndjson",
                ),
            ),
            artifact_inventory={},
            writer_warnings={},
            audio_action_counts={},
            uploaded_audio_uris=(),
        )
        out = StringIO()
        with (
            unittest.mock.patch.object(
                cli, "_make_text_reader", return_value=_reader(config_uri)
            ),
            unittest.mock.patch.object(
                cli, "_make_storage_client", return_value=object()
            ),
            unittest.mock.patch.object(
                cli, "publish_dataset_version_artifacts", return_value=result
            ) as publish,
            redirect_stdout(out),
        ):
            rc = cli.main(["generate", "--config-uri", config_uri])

        self.assertEqual(rc, 0)
        publish.assert_called_once()
        self.assertIn("Dataset generated: gs://bucket/out/radio-v1/", out.getvalue())
        self.assertIn("dataset_version_report.json", out.getvalue())
        self.assertIn("model_inputs/gemini/train.jsonl", out.getvalue())

    def test_generate_failure_prints_short_error(self) -> None:
        import split_dataset as cli
        from dataset_split.publisher import DatasetPublicationError

        config_uri = "gs://configs/radio.toml"
        out = StringIO()
        with (
            unittest.mock.patch.object(
                cli, "_make_text_reader", return_value=_reader(config_uri)
            ),
            unittest.mock.patch.object(
                cli, "_make_storage_client", return_value=object()
            ),
            unittest.mock.patch.object(
                cli,
                "publish_dataset_version_artifacts",
                side_effect=DatasetPublicationError("publication failed"),
            ),
            redirect_stdout(out),
        ):
            rc = cli.main(["generate", "--config-uri", config_uri])

        self.assertEqual(rc, 1)
        self.assertIn("publication failed", out.getvalue())

    def test_help_lists_only_public_commands(self) -> None:
        import split_dataset as cli

        out = StringIO()
        with redirect_stdout(out), self.assertRaises(SystemExit) as ctx:
            cli.main(["--help"])

        self.assertEqual(ctx.exception.code, 0)
        help_text = out.getvalue()
        self.assertIn("dry-run", help_text)
        self.assertIn("generate", help_text)
        self.assertNotIn("validate", help_text)


if __name__ == "__main__":
    unittest.main()
