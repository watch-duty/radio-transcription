from __future__ import annotations

import sys
import unittest
from pathlib import Path

_SFT_DIR = str(Path(__file__).resolve().parent.parent)
_COLABS_DIR = str(
    Path(__file__).resolve().parent.parent.parent.parent / "colabs"
)
if _SFT_DIR not in sys.path:
    sys.path.insert(0, _SFT_DIR)
if _COLABS_DIR not in sys.path:
    sys.path.insert(0, _COLABS_DIR)

from dataset_versioning.gcs_io import (  # noqa: E402
    GcsInputError,
    read_json_or_jsonl,
    require_gs_uri,
)


class FakeTextReader:
    def __init__(
        self, values: dict[str, str], errors: dict[str, Exception] | None = None
    ) -> None:
        self.values = values
        self.errors = errors or {}

    def read_text(self, uri: str) -> str:
        if uri in self.errors:
            raise self.errors[uri]
        return self.values[uri]


class TestDatasetVersionGcsIo(unittest.TestCase):
    def test_read_json_array_manifest(self) -> None:
        rows = read_json_or_jsonl(
            "gs://bucket/manifest.json",
            FakeTextReader({"gs://bucket/manifest.json": '[{"text": "a"}]'}),
        )

        self.assertEqual(rows, [{"text": "a"}])

    def test_read_jsonl_manifest(self) -> None:
        rows = read_json_or_jsonl(
            "gs://bucket/manifest.jsonl",
            FakeTextReader(
                {
                    "gs://bucket/manifest.jsonl": (
                        '{"text": "a"}\n\n{"text": "b"}\n'
                    )
                }
            ),
        )

        self.assertEqual(rows, [{"text": "a"}, {"text": "b"}])

    def test_rejects_non_gs_uri(self) -> None:
        with self.assertRaisesRegex(GcsInputError, "manifest must be a gs://"):
            require_gs_uri("https://example.com/manifest.jsonl", label="manifest")

    def test_rejects_empty_content(self) -> None:
        with self.assertRaisesRegex(GcsInputError, "is empty"):
            read_json_or_jsonl(
                "gs://bucket/empty.jsonl",
                FakeTextReader({"gs://bucket/empty.jsonl": " \n\t"}),
            )

    def test_read_jsonl_manifest_reports_line_number(self) -> None:
        with self.assertRaisesRegex(GcsInputError, "line 2"):
            read_json_or_jsonl(
                "gs://bucket/bad.jsonl",
                FakeTextReader(
                    {"gs://bucket/bad.jsonl": '{"ok": true}\nnot-json\n'}
                ),
            )

    def test_rejects_non_object_row(self) -> None:
        with self.assertRaisesRegex(GcsInputError, "line 1"):
            read_json_or_jsonl(
                "gs://bucket/list.jsonl",
                FakeTextReader({"gs://bucket/list.jsonl": '["not", "object"]'}),
            )

    def test_reader_exception_is_wrapped(self) -> None:
        with self.assertRaisesRegex(
            GcsInputError, "failed to read gs://bucket/missing.jsonl"
        ):
            read_json_or_jsonl(
                "gs://bucket/missing.jsonl",
                FakeTextReader(
                    {},
                    errors={
                        "gs://bucket/missing.jsonl": RuntimeError("missing")
                    },
                ),
            )


if __name__ == "__main__":
    unittest.main()
