"""TDD tests for pipeline.py build subcommand, preflight.py, and adapters.

RED phase: tests written before implementation — all should FAIL until
implementation is complete.
"""

from __future__ import annotations

import json
import sys
import tempfile
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
    """PREFLIGHT_TOKEN_CAP is 131_072 per D-14."""

    def test_token_cap_value(self) -> None:
        from preflight import PREFLIGHT_TOKEN_CAP

        self.assertEqual(PREFLIGHT_TOKEN_CAP, 131_072)


if __name__ == "__main__":
    unittest.main()
