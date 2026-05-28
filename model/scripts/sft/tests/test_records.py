"""Unit tests for SFT run-record writers."""

from __future__ import annotations

import sys
import tempfile
import unittest
import unittest.mock
from pathlib import Path

_SFT_DIR = str(Path(__file__).resolve().parent.parent)
if _SFT_DIR not in sys.path:
    sys.path.insert(0, _SFT_DIR)


class TestRunRecords(unittest.TestCase):
    def test_git_sha_uses_timeout(self) -> None:
        import records

        completed = unittest.mock.Mock(returncode=0, stdout="abc123\n")
        with unittest.mock.patch(
            "records.subprocess.run", return_value=completed
        ) as run:
            self.assertEqual(records._git_sha(), "abc123")

        self.assertEqual(run.call_args.kwargs["timeout"], 5)

    def test_append_ledger_allows_none_datasets(self) -> None:
        from records import append_ledger

        with tempfile.TemporaryDirectory() as tmp:
            results_dir = Path(tmp)
            append_ledger(
                results_dir,
                {
                    "round_id": "round-none-datasets",
                    "datasets": None,
                    "base_model": "gemini-3.1-flash-lite",
                    "epochs": 1,
                    "base_wer": 10.0,
                    "git_sha": "abc123",
                    "timestamp": "2026-05-27",
                },
            )

            ledger = (results_dir / "ledger.md").read_text()

        self.assertIn(
            "| round-none-datasets |  | gemini-3.1-flash-lite", ledger
        )


if __name__ == "__main__":
    unittest.main()
