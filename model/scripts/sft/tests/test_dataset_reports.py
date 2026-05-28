from __future__ import annotations

import json
import sys
import unittest
from dataclasses import replace
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
    DatasetVersionReportError,
    build_dataset_version_report,
    render_dataset_version_markdown,
)
from dataset_split.types import ExcludedRow, LabeledSegment  # noqa: E402


def _segment(
    source_group: str,
    *,
    action: str = "reused",
    split: str = "train",
    dataset_name: str = "calls",
    dataset_family: str = "bcfy_calls",
    duration: float = 10.0,
    row_index: int = 0,
    mixed_to_mono: bool = False,
    resampled: bool = False,
    padded: bool = False,
) -> LabeledSegment:
    audio_uri = f"gs://bucket/{dataset_name}/{source_group}/{row_index}.flac"
    model_ready_audio_uri = (
        f"gs://bucket/sft/dv-001/audio/{action}/{row_index}.flac"
    )
    derived_audio_uri = model_ready_audio_uri if action == "derived" else None
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
        model_ready_audio_uri=model_ready_audio_uri,
        derived_audio_uri=derived_audio_uri,
        transformation_metadata=_transformation_metadata(
            action=action,
            audio_uri=audio_uri,
            model_ready_audio_uri=model_ready_audio_uri,
            source_group=source_group,
            split=split,
            duration=duration,
            mixed_to_mono=mixed_to_mono,
            resampled=resampled,
            padded=padded,
        ),
    )


def _transformation_metadata(
    *,
    action: str,
    audio_uri: str,
    model_ready_audio_uri: str,
    source_group: str,
    split: str,
    duration: float,
    mixed_to_mono: bool,
    resampled: bool,
    padded: bool,
) -> dict[str, object]:
    source_channels = 2 if mixed_to_mono else 1
    output_channels = 1
    return {
        "action": action,
        "original_audio_uri": audio_uri,
        "source_audio_uri": audio_uri,
        "offset": 0.0,
        "duration": duration,
        "source_duration": duration if action != "derived" else 300.0,
        "output_duration": duration,
        "source_format": "flac" if action != "derived" else "wav",
        "output_format": "flac",
        "source_channels": source_channels,
        "output_channels": output_channels,
        "mixed_to_mono": mixed_to_mono,
        "resampled": resampled,
        "padded": padded,
        "split": split,
        "source_group": source_group,
        "ffprobe": "ffprobe -v error ...",
        "ffmpeg": None
        if action in {"reused", "copied"}
        else "ffmpeg -hide_banner ...",
        "ffprobe_version": "ffprobe version 7.0.2",
        "ffmpeg_version": "ffmpeg version 7.0.2",
        "model_ready_audio_uri": model_ready_audio_uri,
    }


def _segments() -> tuple[LabeledSegment, ...]:
    return (
        _segment(
            "calls-a",
            split="train",
            dataset_name="calls",
            dataset_family="bcfy_calls",
            duration=5.0,
            row_index=0,
            action="reused",
        ),
        _segment(
            "calls-b",
            split="eval",
            dataset_name="calls",
            dataset_family="bcfy_calls",
            duration=7.0,
            row_index=1,
            action="copied",
        ),
        _segment(
            "feeds-a",
            split="train",
            dataset_name="feeds",
            dataset_family="bcfy_feeds",
            duration=11.0,
            row_index=2,
            action="derived",
            mixed_to_mono=True,
        ),
        _segment(
            "feeds-b",
            split="eval",
            dataset_name="feeds",
            dataset_family="bcfy_feeds",
            duration=13.0,
            row_index=3,
            action="transcoded",
            mixed_to_mono=True,
        ),
    )


def _model_writer_summary() -> dict[str, object]:
    return {
        "nemo": {
            "splits": {
                "train": {"count": 2, "duration_seconds": 16.0},
                "eval": {"count": 2, "duration_seconds": 20.0},
            },
            "total": {"count": 4, "duration_seconds": 36.0},
        },
        "whisper": {
            "splits": {
                "train": {"count": 2, "duration_seconds": 16.0},
                "eval": {"count": 2, "duration_seconds": 20.0},
            },
            "total": {"count": 4, "duration_seconds": 36.0},
        },
        "gemini": {
            "splits": {
                "train": {"count": 2, "duration_seconds": 16.0},
                "eval": {"count": 2, "duration_seconds": 20.0},
            },
            "total": {"count": 4, "duration_seconds": 36.0},
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
            },
            "reports": {
                "excluded_rows": "gs://bucket/sft/dv-001/reports/excluded_rows.jsonl"
            },
        },
        model_writer_summary=_model_writer_summary(),
        writer_warnings={
            "whisper": [{"row_index": 7, "warning": "duration over 30 seconds"}]
        },
        excluded_rows=(
            ExcludedRow(
                dataset_name="calls",
                row_index=99,
                audio_uri="gs://bucket/hidden.flac",
                reason="empty_text",
            ),
        ),
        excluded_rows_artifact_uri=(
            "gs://bucket/sft/dv-001/reports/excluded_rows.jsonl"
        ),
        source_key_failures=0,
        audio_materialized=True,
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
            "audio_transformation_summary",
            "leakage_validation",
            "balance_report",
            "artifact_inventory",
            "writer_warnings",
            "excluded_rows",
            "source_key_failures",
            "audio_materialized",
        ):
            self.assertIn(key, report)
        self.assertEqual(report["split_counts"], {"train": 2, "eval": 2})
        self.assertEqual(
            report["duration_seconds"], {"train": 16.0, "eval": 20.0}
        )
        self.assertEqual(
            report["dataset_summary"]["calls"]["splits"]["train"]["count"],
            1,
        )
        self.assertEqual(
            report["dataset_summary"]["feeds"]["splits"]["train"][
                "duration_seconds"
            ],
            11.0,
        )
        self.assertEqual(report["excluded_rows"]["count"], 1)
        self.assertEqual(report["excluded_rows"]["reasons"]["empty_text"], 1)
        self.assertEqual(
            report["excluded_rows"]["artifact_uri"],
            "gs://bucket/sft/dv-001/reports/excluded_rows.jsonl",
        )
        self.assertEqual(report["source_key_failures"], 0)
        self.assertIs(report["audio_materialized"], True)

    def test_dataset_report_contains_model_writer_summary(self) -> None:
        report = _report_dict()
        model_writer_summary = report["model_writer_summary"]

        self.assertEqual(
            model_writer_summary["nemo"]["splits"]["train"]["count"],
            2,
        )
        self.assertEqual(
            model_writer_summary["whisper"]["splits"]["eval"][
                "duration_seconds"
            ],
            20.0,
        )
        self.assertEqual(model_writer_summary["gemini"]["total"]["count"], 4)
        for writer in ("nemo", "whisper", "gemini"):
            self.assertIn("train", model_writer_summary[writer]["splits"])
            self.assertIn("eval", model_writer_summary[writer]["splits"])
            self.assertIn(
                "duration_seconds", model_writer_summary[writer]["total"]
            )

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

    def test_report_includes_audio_transformation_summary(self) -> None:
        report = _report_dict()

        summary = report["audio_transformation_summary"]
        self.assertEqual(
            summary["actions"],
            {
                "reused": {"count": 1, "duration_seconds": 5.0},
                "copied": {"count": 1, "duration_seconds": 7.0},
                "derived": {"count": 1, "duration_seconds": 11.0},
                "transcoded": {"count": 1, "duration_seconds": 13.0},
            },
        )
        self.assertEqual(summary["model_ready_audio_uri_count"], 4)
        self.assertEqual(summary["derived_audio_uri_count"], 1)
        self.assertEqual(summary["mixed_to_mono_count"], 2)
        self.assertEqual(summary["resampled_count"], 0)
        self.assertEqual(summary["padded_count"], 0)
        self.assertEqual(summary["command_summary_count"], 4)
        self.assertEqual(
            summary["metadata_key_coverage"],
            {
                "original_audio_uri": 4,
                "source_audio_uri": 4,
                "offset": 4,
                "duration": 4,
                "source_duration": 4,
                "output_duration": 4,
                "source_format": 4,
                "output_format": 4,
                "source_channels": 4,
                "output_channels": 4,
                "mixed_to_mono": 4,
                "resampled": 4,
                "padded": 4,
                "split": 4,
                "source_group": 4,
            },
        )

    def test_report_rejects_missing_audio_transformation_metadata(
        self,
    ) -> None:
        segments = (
            replace(_segments()[0], transformation_metadata=None),
            *_segments()[1:],
        )

        with self.assertRaisesRegex(
            DatasetVersionReportError, "transformation_metadata"
        ):
            build_dataset_version_report(
                dataset_version_id="dv-001",
                resolved_config={"dataset_version_id": "dv-001"},
                segments=segments,
                leakage_validation={"passed": True},
                balance_report=build_balance_report(segments).to_dict(),
                artifact_inventory={},
                model_writer_summary=_model_writer_summary(),
                writer_warnings={},
            )

    def test_markdown_includes_audio_transformation_summary(self) -> None:
        segments = _segments()
        report = build_dataset_version_report(
            dataset_version_id="dv-001",
            resolved_config={"dataset_version_id": "dv-001"},
            segments=segments,
            leakage_validation={"passed": True},
            balance_report=build_balance_report(segments).to_dict(),
            artifact_inventory={},
            model_writer_summary=_model_writer_summary(),
            writer_warnings={},
        )

        markdown = render_dataset_version_markdown(report)

        self.assertIn("## Audio Transformations", markdown)
        for label in ("reused", "copied", "derived", "transcoded"):
            self.assertIn(label, markdown)

    def test_markdown_mentions_excluded_rows_without_row_text(self) -> None:
        segments = _segments()
        report = build_dataset_version_report(
            dataset_version_id="dv-001",
            resolved_config={"dataset_version_id": "dv-001"},
            segments=segments,
            leakage_validation={"passed": True},
            balance_report=build_balance_report(segments).to_dict(),
            artifact_inventory={
                "reports": {
                    "excluded_rows": (
                        "gs://bucket/sft/dv-001/reports/excluded_rows.jsonl"
                    )
                }
            },
            model_writer_summary=_model_writer_summary(),
            writer_warnings={},
            excluded_rows=(
                ExcludedRow(
                    dataset_name="calls",
                    row_index=9,
                    audio_uri="gs://bucket/private.flac",
                    reason="empty_text",
                ),
            ),
            excluded_rows_artifact_uri=(
                "gs://bucket/sft/dv-001/reports/excluded_rows.jsonl"
            ),
        )

        markdown = render_dataset_version_markdown(report)

        self.assertIn("## Excluded Rows", markdown)
        self.assertIn("excluded_rows.jsonl", markdown)
        self.assertIn("empty_text: 1", markdown)
        self.assertNotIn("engine 41 responding", markdown)
        self.assertNotIn("private.flac", markdown)


if __name__ == "__main__":
    unittest.main()
