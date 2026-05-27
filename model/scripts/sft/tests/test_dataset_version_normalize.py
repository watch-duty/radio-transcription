from __future__ import annotations

import sys
import unittest
import unittest.mock
from pathlib import Path

_SFT_DIR = str(Path(__file__).resolve().parent.parent)
if _SFT_DIR not in sys.path:
    sys.path.insert(0, _SFT_DIR)

from dataset_versioning.config import InputDatasetConfig  # noqa: E402
from dataset_versioning.normalize import (  # noqa: E402
    normalize_manifest_rows,
    normalize_text,
)
from dataset_versioning.types import RowValidationError  # noqa: E402


def _dataset() -> InputDatasetConfig:
    return InputDatasetConfig(
        name="calls",
        family="bcfy_calls",
        manifest_uri="gs://bucket/calls.jsonl",
        source_strategy="bcfy_calls",
    )


class TestDatasetVersionNormalize(unittest.TestCase):
    def test_empty_text_is_excluded(self) -> None:
        result = normalize_manifest_rows(
            _dataset(), [{"audio_filepath": "gs://bucket/a.flac", "text": "  "}]
        )

        self.assertEqual(result.segments, ())
        self.assertEqual(len(result.excluded), 1)
        self.assertEqual(result.excluded[0].reason, "empty_text")

    def test_null_text_is_excluded(self) -> None:
        result = normalize_manifest_rows(
            _dataset(), [{"audio_filepath": "gs://bucket/a.flac", "text": None}]
        )

        self.assertEqual(result.segments, ())
        self.assertEqual(result.excluded[0].reason, "empty_text")

    def test_normalize_text_replaces_newlines(self) -> None:
        self.assertEqual(normalize_text(" engine\n41\rresponding "), "engine 41 responding")

    def test_non_string_text_is_cast(self) -> None:
        self.assertEqual(normalize_text(1234), "1234")

    def test_missing_audio_uri_for_non_empty_text_fails(self) -> None:
        with self.assertRaisesRegex(RowValidationError, "row_index=0"):
            normalize_manifest_rows(_dataset(), [{"text": "hello"}])

    def test_source_group_resolved_for_non_empty_rows(self) -> None:
        with unittest.mock.patch(
            "dataset_versioning.normalize.resolve_source_group",
            return_value="bcfy_calls:123",
        ) as mock_resolve:
            result = normalize_manifest_rows(
                _dataset(),
                [
                    {
                        "audio_filepath": "gs://bucket/a.flac",
                        "text": "hello",
                        "groupId": "123",
                    }
                ],
            )

        self.assertEqual(result.segments[0].source_group, "bcfy_calls:123")
        mock_resolve.assert_called_once()

    def test_source_group_not_resolved_for_empty_text(self) -> None:
        with unittest.mock.patch(
            "dataset_versioning.normalize.resolve_source_group"
        ) as mock_resolve:
            normalize_manifest_rows(
                _dataset(),
                [{"audio_filepath": "gs://bucket/a.flac", "text": "\n"}],
            )

        mock_resolve.assert_not_called()

    def test_labeled_segment_contains_future_provenance_fields(self) -> None:
        result = normalize_manifest_rows(
            _dataset(),
            [
                {
                    "audio_filepath": "gs://bucket/a.flac",
                    "original_audio_uri": "https://cdn.example.com/a.mp3",
                    "text": "hello",
                    "groupId": "123",
                }
            ],
        )

        segment = result.segments[0]
        self.assertIsNone(segment.model_ready_audio_uri)
        self.assertIsNone(segment.derived_audio_uri)
        self.assertIsNone(segment.transformation_metadata)
        self.assertEqual(segment.raw_row["groupId"], "123")


if __name__ == "__main__":
    unittest.main()
