from __future__ import annotations

import json
import sys
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

from dataset_split.canonical import (  # noqa: E402
    canonical_manifests,
    canonical_row,
    per_dataset_manifests,
)
from dataset_split.leakage import SplitLeakageError  # noqa: E402
from dataset_split.types import LabeledSegment  # noqa: E402


def _segment(
    source_group: str,
    *,
    split: str = "train",
    dataset_name: str = "calls",
    dataset_family: str = "bcfy_calls",
    source_strategy: str = "bcfy_calls",
    original_audio_uri: str | None = None,
    text: str = "engine 41 responding",
    offset: float = 1.5,
    duration: float = 4.25,
    row_index: int = 0,
    example_id: str | None = None,
    segment_id: str | None = None,
    timestamp: str | None = "2026-01-02T03:04:05Z",
    model_ready_audio_uri: str | None = None,
    derived_audio_uri: str | None = None,
    transformation_metadata: dict[str, object] | None = None,
    raw_row: dict[str, object] | None = None,
) -> LabeledSegment:
    audio_uri = f"gs://bucket/{dataset_name}/{source_group}/{row_index}.flac"
    return LabeledSegment(
        dataset_name=dataset_name,
        dataset_family=dataset_family,
        source_strategy=source_strategy,
        source_group=source_group,
        audio_uri=audio_uri,
        original_audio_uri=original_audio_uri or audio_uri,
        text=text,
        row_index=row_index,
        offset=offset,
        duration=duration,
        timestamp=timestamp,
        example_id=example_id or f"example-{row_index}",
        segment_id=segment_id or f"segment-{row_index}",
        split=split,
        model_ready_audio_uri=model_ready_audio_uri,
        derived_audio_uri=derived_audio_uri,
        transformation_metadata=transformation_metadata,
        raw_row=raw_row,
    )


def _jsonl_rows(text: str) -> list[dict[str, object]]:
    return [json.loads(line) for line in text.splitlines() if line.strip()]


class TestDatasetCanonical(unittest.TestCase):
    def test_canonical_rows_preserve_source_split_provenance_and_omit_raw_row(
        self,
    ) -> None:
        segment = _segment(
            "feed-a",
            raw_row={"secret": "x"},
            model_ready_audio_uri="gs://ready/feed-a.flac",
            derived_audio_uri="gs://derived/feed-a.flac",
            transformation_metadata={"reused": False},
        )

        row = canonical_row(segment)

        self.assertEqual(
            set(row),
            {
                "dataset_name",
                "dataset_family",
                "source_strategy",
                "source_group",
                "split",
                "audio_uri",
                "original_audio_uri",
                "text",
                "offset",
                "duration",
                "row_index",
                "example_id",
                "segment_id",
                "timestamp",
                "model_ready_audio_uri",
                "derived_audio_uri",
                "transformation_metadata",
            },
        )
        self.assertEqual(row["dataset_name"], "calls")
        self.assertEqual(row["dataset_family"], "bcfy_calls")
        self.assertEqual(row["source_strategy"], "bcfy_calls")
        self.assertEqual(row["source_group"], "feed-a")
        self.assertEqual(row["split"], "train")
        self.assertEqual(row["original_audio_uri"], segment.original_audio_uri)
        self.assertEqual(row["offset"], 1.5)
        self.assertEqual(row["duration"], 4.25)
        self.assertEqual(row["transformation_metadata"], {"reused": False})
        self.assertNotIn("raw_row", row)
        self.assertNotIn("secret", json.dumps(row))
        for forbidden in (
            "draft",
            "pre_derivation",
            "requires_audio_derivation",
        ):
            self.assertNotIn(forbidden, row)

    def test_canonical_manifests_split_train_eval(self) -> None:
        segments = (
            _segment(
                "feed-a",
                split="train",
                original_audio_uri="gs://bucket/a.wav",
                row_index=0,
            ),
            _segment(
                "feed-b",
                split="eval",
                original_audio_uri="gs://bucket/b.wav",
                row_index=1,
            ),
        )

        manifests = canonical_manifests(segments)

        self.assertEqual(set(manifests), {"train", "eval"})
        train_rows = _jsonl_rows(manifests["train"])
        eval_rows = _jsonl_rows(manifests["eval"])
        self.assertEqual([row["split"] for row in train_rows], ["train"])
        self.assertEqual(
            [row["source_group"] for row in train_rows], ["feed-a"]
        )
        self.assertEqual([row["split"] for row in eval_rows], ["eval"])
        self.assertEqual([row["source_group"] for row in eval_rows], ["feed-b"])

    def test_per_dataset_slices(self) -> None:
        segments = (
            _segment(
                "calls-a",
                split="train",
                dataset_name="calls",
                dataset_family="bcfy_calls",
                original_audio_uri="gs://bucket/calls-a.wav",
                row_index=0,
            ),
            _segment(
                "calls-b",
                split="eval",
                dataset_name="calls",
                dataset_family="bcfy_calls",
                original_audio_uri="gs://bucket/calls-b.wav",
                row_index=1,
            ),
            _segment(
                "feeds-a",
                split="train",
                dataset_name="feeds",
                dataset_family="bcfy_feeds",
                original_audio_uri="gs://bucket/feeds-a.wav",
                row_index=2,
            ),
        )

        with unittest.mock.patch(
            "dataset_split.split.assign_train_eval_split"
        ) as mock_assign:
            manifests = per_dataset_manifests(segments)

        mock_assign.assert_not_called()
        self.assertEqual(list(manifests), ["calls", "feeds"])
        self.assertEqual(
            [
                row["source_group"]
                for row in _jsonl_rows(manifests["calls"]["train"])
            ],
            ["calls-a"],
        )
        self.assertEqual(
            [
                row["source_group"]
                for row in _jsonl_rows(manifests["calls"]["eval"])
            ],
            ["calls-b"],
        )
        self.assertEqual(
            [
                row["source_group"]
                for row in _jsonl_rows(manifests["feeds"]["train"])
            ],
            ["feeds-a"],
        )
        self.assertEqual(_jsonl_rows(manifests["feeds"]["eval"]), [])

    def test_canonical_build_fails_on_split_leakage(self) -> None:
        segments = (
            _segment(
                "feed-a",
                split="train",
                original_audio_uri="gs://bucket/a.wav",
                row_index=0,
            ),
            _segment(
                "feed-a",
                split="eval",
                original_audio_uri="gs://bucket/b.wav",
                row_index=1,
            ),
        )

        with self.assertRaises(SplitLeakageError):
            canonical_manifests(segments)

    def test_canonical_build_rejects_non_finite_numbers(self) -> None:
        with self.assertRaises(ValueError):
            canonical_manifests(
                (_segment("feed-a", duration=float("nan"), row_index=9),)
            )


if __name__ == "__main__":
    unittest.main()
