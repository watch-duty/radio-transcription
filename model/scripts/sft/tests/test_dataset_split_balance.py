from __future__ import annotations

import sys
import unittest
from pathlib import Path

_SFT_DIR = str(Path(__file__).resolve().parent.parent)
if _SFT_DIR not in sys.path:
    sys.path.insert(0, _SFT_DIR)

from dataset_split.balance import (  # noqa: E402
    BalanceReport,
    bucket_duration_seconds,
    bucket_transcript_words,
    build_balance_report,
)
from dataset_split.types import LabeledSegment  # noqa: E402


def _segment(
    source_group: str,
    *,
    split: str = "train",
    dataset_name: str = "calls",
    dataset_family: str = "bcfy_calls",
    text: str = "alpha bravo",
    duration: float = 10.0,
    timestamp: str | None = None,
    row_index: int = 0,
) -> LabeledSegment:
    audio_uri = f"gs://bucket/{dataset_name}/{source_group}/{row_index}.mp3"
    return LabeledSegment(
        dataset_name=dataset_name,
        dataset_family=dataset_family,
        source_strategy=dataset_family,
        source_group=source_group,
        audio_uri=audio_uri,
        original_audio_uri=audio_uri,
        text=text,
        row_index=row_index,
        duration=duration,
        timestamp=timestamp,
        split=split,
    )


class TestBalanceReport(unittest.TestCase):
    def test_duration_bucket_boundaries(self) -> None:
        self.assertEqual(bucket_duration_seconds(0.0), "0_5s")
        self.assertEqual(bucket_duration_seconds(4.999), "0_5s")
        self.assertEqual(bucket_duration_seconds(5.0), "5_15s")
        self.assertEqual(bucket_duration_seconds(15.0), "15_30s")
        self.assertEqual(bucket_duration_seconds(30.0), "30s_plus")

    def test_transcript_word_bucket_uses_word_count(self) -> None:
        self.assertEqual(bucket_transcript_words(""), "0_3_words")
        self.assertEqual(bucket_transcript_words("one two three"), "0_3_words")
        self.assertEqual(
            bucket_transcript_words("one two three four"), "4_10_words"
        )
        self.assertEqual(
            bucket_transcript_words(" ".join(str(i) for i in range(11))),
            "11_25_words",
        )
        self.assertEqual(
            bucket_transcript_words(" ".join(str(i) for i in range(26))),
            "26_plus_words",
        )

    def test_report_contains_weighted_score_and_component_deltas(self) -> None:
        report = build_balance_report(
            (
                _segment("calls-a", split="train", row_index=0),
                _segment("calls-b", split="eval", row_index=1),
                _segment(
                    "feeds-a",
                    split="train",
                    dataset_name="feeds",
                    dataset_family="bcfy_feeds",
                    row_index=2,
                ),
                _segment(
                    "feeds-b",
                    split="eval",
                    dataset_name="feeds",
                    dataset_family="bcfy_feeds",
                    row_index=3,
                ),
            )
        )

        self.assertIsInstance(report, BalanceReport)
        self.assertGreaterEqual(report.weighted_score, 0.0)
        names = {component.name for component in report.component_deltas}
        self.assertTrue(any(name.startswith("dataset:") for name in names))
        self.assertTrue(any(name.startswith("family:") for name in names))
        self.assertTrue(any(name.startswith("global:") for name in names))
        self.assertTrue(
            any(name.startswith("duration_bucket:") for name in names)
        )
        self.assertTrue(
            any(name.startswith("transcript_words:") for name in names)
        )
        self.assertIn("weighted_score", report.to_dict())
        self.assertIn("component_deltas", report.to_dict())

    def test_time_distribution_is_report_only(self) -> None:
        report = build_balance_report(
            (
                _segment(
                    "feed-a",
                    split="train",
                    timestamp="2026-01-02T03:04:05Z",
                    row_index=0,
                ),
                _segment(
                    "feed-b",
                    split="eval",
                    timestamp="2026-01-03T03:00:00+00:00",
                    row_index=1,
                ),
                _segment(
                    "feed-c",
                    split="train",
                    timestamp="not-a-timestamp",
                    row_index=2,
                ),
            )
        )

        self.assertTrue(report.time_distribution["report_only"])
        self.assertEqual(
            report.time_distribution["month"]["2026-01"],
            {"train": 1.0, "eval": 1.0},
        )
        self.assertEqual(
            report.time_distribution["hour_of_day"]["3"],
            {"train": 1.0, "eval": 1.0},
        )

    def test_balance_deltas_do_not_raise(self) -> None:
        report = build_balance_report(
            (
                _segment("feed-a", split="train", duration=1.0, row_index=0),
                _segment("feed-b", split="train", duration=30.0, row_index=1),
                _segment(
                    "feed-c",
                    split="train",
                    text=" ".join(str(i) for i in range(26)),
                    row_index=2,
                ),
                _segment("feed-d", split="eval", duration=2.0, row_index=3),
            )
        )

        self.assertGreater(report.weighted_score, 0.0)
        self.assertIn("0_5s", report.duration_buckets)
        self.assertIn("26_plus_words", report.transcript_length_buckets)


if __name__ == "__main__":
    unittest.main()
