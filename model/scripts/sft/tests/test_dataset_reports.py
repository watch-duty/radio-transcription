from __future__ import annotations

import json
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

from dataset_split.balance import build_balance_report  # noqa: E402
from dataset_split.reports import (  # noqa: E402
    build_dataset_version_report,
    render_dataset_version_markdown,
)
from dataset_split.types import LabeledSegment  # noqa: E402


def _segment(
    source_group: str,
    *,
    split: str = "train",
    dataset_name: str = "calls",
    dataset_family: str = "bcfy_calls",
    duration: float = 10.0,
    row_index: int = 0,
) -> LabeledSegment:
    audio_uri = f"gs://bucket/{dataset_name}/{source_group}/{row_index}.flac"
    return LabeledSegment(
        dataset_name=dataset_name,
        dataset_family=dataset_family,
        source_strategy=dataset_family,
        source_group=source_group,
        audio_uri=audio_uri,
        original_audio_uri=audio_uri,
        text="engine 41 responding",
        row_index=row_index,
        duration=duration,
        split=split,
    )


def _segments() -> tuple[LabeledSegment, ...]:
    return (
        _segment(
            "calls-a",
            split="train",
            dataset_name="calls",
            dataset_family="bcfy_calls",
            duration=5.0,
            row_index=0,
        ),
        _segment(
            "calls-b",
            split="eval",
            dataset_name="calls",
            dataset_family="bcfy_calls",
            duration=7.0,
            row_index=1,
        ),
        _segment(
            "feeds-a",
            split="train",
            dataset_name="feeds",
            dataset_family="bcfy_feeds",
            duration=11.0,
            row_index=2,
        ),
    )


def _model_writer_summary() -> dict[str, object]:
    return {
        "nemo": {
            "splits": {
                "train": {"count": 2, "duration_seconds": 16.0},
                "eval": {"count": 1, "duration_seconds": 7.0},
            },
            "total": {"count": 3, "duration_seconds": 23.0},
        },
        "whisper": {
            "splits": {
                "train": {"count": 2, "duration_seconds": 16.0},
                "eval": {"count": 1, "duration_seconds": 7.0},
            },
            "total": {"count": 3, "duration_seconds": 23.0},
        },
        "gemini": {
            "splits": {
                "train": {"count": 2, "duration_seconds": 16.0},
                "eval": {"count": 1, "duration_seconds": 7.0},
            },
            "total": {"count": 3, "duration_seconds": 23.0},
        },
    }


def _report_dict() -> dict[str, object]:
    segments = _segments()
    report = build_dataset_version_report(
        dataset_version_id="dv-001",
        resolved_config={"dataset_version_id": "dv-001"},
        segments=segments,
        leakage_validation={"passed": True},
        balance_report=build_balance_report(segments).to_dict(),
        artifact_inventory={
            "canonical": {
                "train": "gs://bucket/sft/dv-001/manifests/canonical/train.jsonl",
                "eval": "gs://bucket/sft/dv-001/manifests/canonical/eval.jsonl",
            }
        },
        model_writer_summary=_model_writer_summary(),
        writer_warnings={
            "whisper": [{"row_index": 7, "warning": "duration over 30 seconds"}]
        },
    )
    return report.to_dict()


class TestDatasetReports(unittest.TestCase):
    def test_dataset_report_contains_required_generation_fields(self) -> None:
        report = _report_dict()

        for key in (
            "dataset_version_id",
            "resolved_config",
            "split_counts",
            "duration_seconds",
            "dataset_summary",
            "leakage_validation",
            "balance_report",
            "artifact_inventory",
            "writer_warnings",
        ):
            self.assertIn(key, report)
        self.assertEqual(report["split_counts"], {"train": 2, "eval": 1})
        self.assertEqual(report["duration_seconds"], {"train": 16.0, "eval": 7.0})
        self.assertEqual(
            report["dataset_summary"]["calls"]["splits"]["train"]["count"],
            1,
        )
        self.assertEqual(
            report["dataset_summary"]["feeds"]["splits"]["train"]["duration_seconds"],
            11.0,
        )

    def test_dataset_report_contains_model_writer_summary(self) -> None:
        report = _report_dict()
        model_writer_summary = report["model_writer_summary"]

        self.assertEqual(
            model_writer_summary["nemo"]["splits"]["train"]["count"],
            2,
        )
        self.assertEqual(
            model_writer_summary["whisper"]["splits"]["eval"]["duration_seconds"],
            7.0,
        )
        self.assertEqual(model_writer_summary["gemini"]["total"]["count"], 3)
        for writer in ("nemo", "whisper", "gemini"):
            self.assertIn("train", model_writer_summary[writer]["splits"])
            self.assertIn("eval", model_writer_summary[writer]["splits"])
            self.assertIn("duration_seconds", model_writer_summary[writer]["total"])

    def test_dataset_report_excludes_sft_run_fields(self) -> None:
        segments = _segments()
        report = build_dataset_version_report(
            dataset_version_id="dv-001",
            resolved_config={
                "dataset_version_id": "dv-001",
                "tuned_model_id": "must-not-appear",
                "endpoint": "must-not-appear",
                "training_metrics": {"wer": 0.1},
                "post_run_wer": 0.2,
                "run_comparison": {"baseline": 0.3},
            },
            segments=segments,
            leakage_validation={"passed": True},
            balance_report={},
            artifact_inventory={},
            model_writer_summary=_model_writer_summary(),
            writer_warnings={},
        )

        rendered = json.dumps(report.to_dict(), sort_keys=True)
        markdown = render_dataset_version_markdown(report)
        for forbidden in (
            "tuned_model_id",
            "endpoint",
            "training_metrics",
            "post_run_wer",
            "run_comparison",
            "must-not-appear",
        ):
            self.assertNotIn(forbidden, rendered)
            self.assertNotIn(forbidden, markdown)


if __name__ == "__main__":
    unittest.main()
