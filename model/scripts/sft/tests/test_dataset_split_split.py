from __future__ import annotations

import sys
import unittest
from pathlib import Path

_SFT_DIR = str(Path(__file__).resolve().parent.parent)
if _SFT_DIR not in sys.path:
    sys.path.insert(0, _SFT_DIR)

from dataset_split.split import (  # noqa: E402
    SplitAssignmentError,
    assign_train_eval_split,
)
from dataset_split.types import LabeledSegment  # noqa: E402


def _segment(
    source_group: str,
    *,
    dataset_name: str = "calls",
    dataset_family: str = "bcfy_calls",
    text: str = "alpha bravo",
    row_index: int = 0,
    duration: float = 10.0,
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
    )


class TestAssignTrainEvalSplit(unittest.TestCase):
    def test_assigns_each_source_group_to_one_split(self) -> None:
        segments = (
            _segment("feed-a", row_index=0),
            _segment("feed-a", row_index=1),
            _segment("feed-b", row_index=2),
            _segment("feed-c", row_index=3),
        )

        result = assign_train_eval_split(
            segments, solver_time_limit_seconds=2.0
        )

        observed: dict[str, set[str]] = {}
        for segment in result.segments:
            observed.setdefault(segment.source_group, set()).add(
                segment.split or ""
            )
        self.assertEqual(
            {source_group: len(splits) for source_group, splits in observed.items()},
            {"feed-a": 1, "feed-b": 1, "feed-c": 1},
        )
        self.assertEqual(
            set(result.source_group_splits),
            {"feed-a", "feed-b", "feed-c"},
        )

    def test_each_dataset_gets_train_and_eval_when_two_groups_exist(
        self,
    ) -> None:
        segments = (
            _segment("calls-a", dataset_name="calls", row_index=0),
            _segment("calls-b", dataset_name="calls", row_index=1),
            _segment(
                "feeds-a",
                dataset_name="feeds",
                dataset_family="bcfy_feeds",
                row_index=2,
            ),
            _segment(
                "feeds-b",
                dataset_name="feeds",
                dataset_family="bcfy_feeds",
                row_index=3,
            ),
        )

        result = assign_train_eval_split(
            segments, solver_time_limit_seconds=2.0
        )

        splits_by_dataset: dict[str, set[str]] = {}
        for segment in result.segments:
            splits_by_dataset.setdefault(segment.dataset_name, set()).add(
                segment.split or ""
            )
        self.assertEqual(splits_by_dataset["calls"], {"train", "eval"})
        self.assertEqual(splits_by_dataset["feeds"], {"train", "eval"})

    def test_dataset_with_one_source_group_fails(self) -> None:
        segments = (
            _segment("only-source", row_index=0),
            _segment("only-source", row_index=1),
        )

        with self.assertRaisesRegex(
            SplitAssignmentError, "dataset=calls has fewer than 2 source groups"
        ):
            assign_train_eval_split(segments, solver_time_limit_seconds=2.0)

    def test_empty_segments_fail(self) -> None:
        with self.assertRaisesRegex(
            SplitAssignmentError, "segments must not be empty"
        ):
            assign_train_eval_split((), solver_time_limit_seconds=2.0)

    def test_result_records_algorithm_metadata(self) -> None:
        result = assign_train_eval_split(
            (
                _segment("feed-a", row_index=0),
                _segment("feed-b", row_index=1),
            ),
            solver_time_limit_seconds=2.0,
        )

        self.assertEqual(
            result.metadata.algorithm, "ortools_cp_sat_source_group_v1"
        )
        self.assertIn(result.metadata.solver_status, {"OPTIMAL", "FEASIBLE"})
        self.assertGreaterEqual(result.metadata.objective_value, 0.0)
        self.assertEqual(result.metadata.solver_time_limit_seconds, 2.0)
        self.assertIn("dataset_row_count", result.metadata.weights)
        self.assertIsNotNone(result.balance_report)
        self.assertIn("weighted_score", result.balance_report or {})

    def test_assignment_does_not_require_random_seed(self) -> None:
        result = assign_train_eval_split(
            (
                _segment("feed-a", row_index=0),
                _segment("feed-b", row_index=1),
            ),
            solver_time_limit_seconds=2.0,
        )

        self.assertEqual(
            {segment.split for segment in result.segments}, {"train", "eval"}
        )


if __name__ == "__main__":
    unittest.main()
