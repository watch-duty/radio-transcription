"""Unit tests for SFT run-record writers."""

from __future__ import annotations

import sys
import tempfile
import unittest
import unittest.mock
from pathlib import Path
from typing import Any

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

    def test_record_writers_use_utf8_encoding(self) -> None:
        import records

        write_encodings: list[str | None] = []
        open_encodings: list[str | None] = []
        real_write_text = Path.write_text
        real_open = Path.open

        def write_text_spy(
            path: Path,
            data: str,
            *args: Any,
            **kwargs: Any,
        ) -> int:
            if path.name in {
                "config.json",
                "wer_summary.json",
                "wer_summary.md",
                "ledger.md",
            }:
                write_encodings.append(kwargs.get("encoding"))
            return real_write_text(path, data, *args, **kwargs)

        def open_spy(path: Path, *args: Any, **kwargs: Any) -> Any:
            mode = args[0] if args else kwargs.get("mode", "r")
            if path.name == "ledger.md" and "a" in mode:
                open_encodings.append(kwargs.get("encoding"))
            return real_open(path, *args, **kwargs)

        with tempfile.TemporaryDirectory() as tmp:
            results_dir = Path(tmp)
            with (
                unittest.mock.patch.object(
                    Path, "write_text", new=write_text_spy
                ),
                unittest.mock.patch.object(Path, "open", new=open_spy),
                unittest.mock.patch("records._git_sha", return_value="abc123"),
                unittest.mock.patch("records._dep_versions", return_value={}),
            ):
                records.write_config(
                    results_dir,
                    "round-utf8",
                    {"note": "unicode — ok"},
                )
                records.write_wer_summary(
                    results_dir,
                    "round-utf8",
                    {
                        "round_id": "round-utf8",
                        "base_wer": 10.0,
                        "base_cer": 5.0,
                    },
                )
                records.append_ledger(
                    results_dir,
                    {
                        "round_id": "round-utf8",
                        "datasets": ["echo"],
                        "base_model": "gemini-3.1-flash-lite",
                        "epochs": 1,
                        "base_wer": 10.0,
                    },
                )

        self.assertTrue(write_encodings)
        self.assertTrue(open_encodings)
        self.assertTrue(all(enc == "utf-8" for enc in write_encodings))
        self.assertTrue(all(enc == "utf-8" for enc in open_encodings))


if __name__ == "__main__":
    unittest.main()
