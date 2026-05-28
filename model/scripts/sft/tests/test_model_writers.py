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

from dataset_split.model_writers import (  # noqa: E402
    ModelWriterResult,
    build_nemo_inputs,
    build_whisper_inputs,
)
from dataset_split.types import LabeledSegment  # noqa: E402


def _segment(
    source_group: str,
    *,
    split: str = "train",
    row_index: int = 0,
    duration: float = 12.5,
    offset: float = 3.0,
    text: str = "engine 41 copy",
) -> LabeledSegment:
    return LabeledSegment(
        dataset_name="calls",
        dataset_family="bcfy_calls",
        source_strategy="bcfy_calls",
        source_group=source_group,
        audio_uri=f"gs://wd-source/{source_group}/{row_index}.mp3",
        original_audio_uri=f"gs://wd-raw/{source_group}/{row_index}.mp3",
        text=text,
        row_index=row_index,
        offset=offset,
        duration=duration,
        timestamp="2026-05-27T12:00:00Z",
        example_id=f"example-{row_index}",
        segment_id=f"segment-{row_index}",
        split=split,
        raw_row={
            "benchmark_path": "model/data/inference_manifests/benchmark.jsonl"
        },
    )


class TestNemoWriter(unittest.TestCase):
    def test_nemo_writer_shape(self) -> None:
        segment = _segment("feed-a", row_index=1)

        result = build_nemo_inputs(
            (segment,),
            train_manifest_uri="gs://wd-transcription-data/sft/v1/model_inputs/nemo/train.jsonl",
            eval_manifest_uri="gs://wd-transcription-data/sft/v1/model_inputs/nemo/eval.jsonl",
        )

        self.assertIsInstance(result, ModelWriterResult)
        row = result.rows_by_split["train"][0]
        self.assertEqual(
            set(row),
            {
                "audio_filepath",
                "text",
                "duration",
                "offset",
                "example_id",
                "segment_id",
            },
        )
        self.assertEqual(row["audio_filepath"], segment.audio_uri)
        self.assertEqual(row["text"], segment.text)
        self.assertEqual(row["duration"], segment.duration)
        self.assertEqual(row["offset"], segment.offset)
        self.assertEqual(row["example_id"], segment.example_id)
        self.assertEqual(row["segment_id"], segment.segment_id)
        self.assertEqual(result.rows_by_split["eval"], ())

    def test_nemo_config_references_train_and_eval_manifests(self) -> None:
        train_uri = (
            "gs://wd-transcription-data/sft/v1/model_inputs/nemo/train.jsonl"
        )
        eval_uri = (
            "gs://wd-transcription-data/sft/v1/model_inputs/nemo/eval.jsonl"
        )

        result = build_nemo_inputs(
            (_segment("feed-a"),),
            train_manifest_uri=train_uri,
            eval_manifest_uri=eval_uri,
        )

        self.assertEqual(
            result.config,
            {
                "train_manifest": train_uri,
                "validation_manifest": eval_uri,
                "manifest_format": "nemo_jsonl",
            },
        )


class TestWhisperWriter(unittest.TestCase):
    def test_whisper_writer_shape_and_warnings(self) -> None:
        segment = _segment("feed-a", row_index=1)

        result = build_whisper_inputs((segment,))

        self.assertIsInstance(result, ModelWriterResult)
        self.assertIsNone(result.config)
        row = result.rows_by_split["train"][0]
        self.assertEqual(
            set(row),
            {
                "audio_uri",
                "text",
                "duration",
                "offset",
                "dataset_name",
                "dataset_family",
                "source_group",
                "split",
                "example_id",
                "segment_id",
                "preprocessing",
            },
        )
        self.assertEqual(row["audio_uri"], segment.audio_uri)
        self.assertEqual(row["dataset_name"], segment.dataset_name)
        self.assertEqual(row["dataset_family"], segment.dataset_family)
        self.assertEqual(row["source_group"], segment.source_group)
        self.assertEqual(row["split"], segment.split)
        self.assertEqual(
            row["preprocessing"],
            {
                "recommendation": "preserve_original_uri_with_offset_duration",
                "clip_derivation_phase": 4,
                "recommended_max_duration_seconds": 30.0,
            },
        )
        self.assertEqual(result.warnings, ())

    def test_whisper_over_30_seconds_is_warning_not_failure(self) -> None:
        segment = _segment("feed-a", duration=31.0, row_index=7)

        result = build_whisper_inputs((segment,))

        self.assertEqual(len(result.rows_by_split["train"]), 1)
        self.assertEqual(len(result.warnings), 1)
        warning = result.warnings[0]
        self.assertEqual(warning.writer, "whisper")
        self.assertEqual(warning.code, "whisper_duration_over_30s")
        self.assertEqual(warning.severity, "warning")
        self.assertEqual(warning.row_index, segment.row_index)
        self.assertEqual(warning.details["duration"], 31.0)
        self.assertEqual(
            result.warnings_by_writer()["whisper"][0]["code"],
            "whisper_duration_over_30s",
        )


class TestWriterSafety(unittest.TestCase):
    def test_writers_do_not_mutate_benchmark_eval_manifests(self) -> None:
        segments = (
            _segment("feed-a", split="train", row_index=1),
            _segment("feed-b", split="eval", row_index=2),
        )

        nemo = build_nemo_inputs(
            segments,
            train_manifest_uri="gs://wd-transcription-data/sft/v1/model_inputs/nemo/train.jsonl",
            eval_manifest_uri="gs://wd-transcription-data/sft/v1/model_inputs/nemo/eval.jsonl",
        )
        whisper = build_whisper_inputs(segments)
        output = json.dumps(
            {
                "nemo_rows": nemo.rows_by_split,
                "nemo_config": nemo.config,
                "whisper_rows": whisper.rows_by_split,
                "warnings": whisper.warnings_by_writer(),
            },
            sort_keys=True,
        )

        for forbidden in (
            "model/data",
            "inference_manifests",
            "benchmark",
        ):
            self.assertNotIn(forbidden, output)


if __name__ == "__main__":
    unittest.main()
