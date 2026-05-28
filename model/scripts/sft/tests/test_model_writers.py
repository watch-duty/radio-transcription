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
    DEFAULT_GEMINI_BASE_MODEL,
    ModelWriterResult,
    ModelWriterError,
    build_gemini_inputs,
    build_gemini_tuning_config,
    build_nemo_inputs,
    build_whisper_inputs,
    infer_audio_mime_type,
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
    audio_suffix: str = "mp3",
    model_ready_audio_uri: str | None = "DEFAULT",
) -> LabeledSegment:
    ready_uri = (
        f"gs://wd-ready/{source_group}/{row_index}.{audio_suffix}"
        if model_ready_audio_uri == "DEFAULT"
        else model_ready_audio_uri
    )
    return LabeledSegment(
        dataset_name="calls",
        dataset_family="bcfy_calls",
        source_strategy="bcfy_calls",
        source_group=source_group,
        audio_uri=f"gs://wd-source/{source_group}/{row_index}.{audio_suffix}",
        original_audio_uri=(
            f"gs://wd-raw/{source_group}/{row_index}.{audio_suffix}"
        ),
        text=text,
        row_index=row_index,
        offset=offset,
        duration=duration,
        timestamp="2026-05-27T12:00:00Z",
        example_id=f"example-{row_index}",
        segment_id=f"segment-{row_index}",
        split=split,
        model_ready_audio_uri=ready_uri,
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
        self.assertEqual(
            row["audio_filepath"], segment.model_ready_audio_uri
        )
        self.assertNotEqual(row["audio_filepath"], segment.audio_uri)
        self.assertEqual(row["text"], segment.text)
        self.assertEqual(row["duration"], segment.duration)
        self.assertEqual(row["offset"], segment.offset)
        self.assertEqual(row["example_id"], segment.example_id)
        self.assertEqual(row["segment_id"], segment.segment_id)
        self.assertEqual(result.rows_by_split["eval"], ())

    def test_nemo_requires_model_ready_audio_uri(self) -> None:
        segment = _segment(
            "feed-a", row_index=11, model_ready_audio_uri=None
        )

        with self.assertRaisesRegex(
            ModelWriterError, "row_index=11 missing model_ready_audio_uri"
        ):
            build_nemo_inputs(
                (segment,),
                train_manifest_uri="gs://wd-transcription-data/sft/v1/model_inputs/nemo/train.jsonl",
                eval_manifest_uri="gs://wd-transcription-data/sft/v1/model_inputs/nemo/eval.jsonl",
            )

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
                "original_audio_uri",
                "example_id",
                "segment_id",
                "preprocessing",
            },
        )
        self.assertEqual(row["audio_uri"], segment.model_ready_audio_uri)
        self.assertNotEqual(row["audio_uri"], segment.audio_uri)
        self.assertEqual(
            row["original_audio_uri"], segment.original_audio_uri
        )
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

    def test_whisper_requires_model_ready_audio_uri(self) -> None:
        segment = _segment(
            "feed-a", row_index=12, model_ready_audio_uri=None
        )

        with self.assertRaisesRegex(
            ModelWriterError, "row_index=12 missing model_ready_audio_uri"
        ):
            build_whisper_inputs((segment,))


class TestGeminiWriter(unittest.TestCase):
    def test_gemini_writer_shape_and_config(self) -> None:
        segments = (
            _segment(
                "feed-a",
                split="train",
                row_index=1,
                audio_suffix="flac",
            ),
            _segment(
                "feed-b",
                split="eval",
                row_index=2,
                audio_suffix="mp3",
                text="ladder 5 responding",
            ),
        )

        result = build_gemini_inputs(
            segments,
            system_prompt="You are a radio transcription model.",
            user_prompt="Transcribe the emergency radio audio.",
            training_dataset_uri="gs://wd-transcription-data/sft/dv-001/model_inputs/gemini/train.jsonl",
            validation_dataset_uri="gs://wd-transcription-data/sft/dv-001/model_inputs/gemini/eval.jsonl",
        )

        self.assertIsInstance(result, ModelWriterResult)
        self.assertEqual(DEFAULT_GEMINI_BASE_MODEL, "gemini-3.1-flash-lite")
        self.assertEqual(result.warnings, ())
        train_row = result.rows_by_split["train"][0]
        eval_row = result.rows_by_split["eval"][0]
        self.assertEqual(
            set(train_row),
            {"systemInstruction", "contents"},
        )
        self.assertEqual(
            train_row["systemInstruction"]["parts"][0]["text"],
            "You are a radio transcription model.",
        )
        self.assertEqual(train_row["contents"][0]["role"], "user")
        self.assertEqual(train_row["contents"][1]["role"], "model")
        self.assertNotEqual(
            train_row["contents"][0]["parts"][0]["fileData"]["fileUri"],
            segments[0].audio_uri,
        )
        self.assertEqual(
            train_row["contents"][0]["parts"][0]["fileData"],
            {
                "mimeType": "audio/flac",
                "fileUri": segments[0].model_ready_audio_uri,
            },
        )
        self.assertEqual(
            eval_row["contents"][0]["parts"][0]["fileData"]["mimeType"],
            "audio/mpeg",
        )
        self.assertEqual(
            eval_row["contents"][1]["parts"][0]["text"],
            "ladder 5 responding",
        )
        self.assertEqual(
            result.config,
            {
                "trainingDatasetUri": "gs://wd-transcription-data/sft/dv-001/model_inputs/gemini/train.jsonl",
                "validationDatasetUri": "gs://wd-transcription-data/sft/dv-001/model_inputs/gemini/eval.jsonl",
                "baseModel": "gemini-3.1-flash-lite",
                "region": "us-central1",
                "adapterSize": "ONE",
                "epochCount": 5,
                "learningRateMultiplier": 1.0,
            },
        )

    def test_gemini_config_omits_validation_when_absent(self) -> None:
        config = build_gemini_tuning_config(
            training_dataset_uri="gs://wd-transcription-data/sft/dv-001/model_inputs/gemini/train.jsonl",
            validation_dataset_uri=None,
        )

        self.assertEqual(
            config["trainingDatasetUri"],
            "gs://wd-transcription-data/sft/dv-001/model_inputs/gemini/train.jsonl",
        )
        self.assertNotIn("validationDatasetUri", config)
        self.assertEqual(config["baseModel"], "gemini-3.1-flash-lite")

    def test_gemini_config_rejects_invalid_tuning_values(self) -> None:
        invalid_kwargs = (
            {"adapter_size": "INVALID"},
            {"epoch_count": 0},
            {"epoch_count": 101},
            {"epoch_count": True},
            {"learning_rate_multiplier": -1.0},
            {"learning_rate_multiplier": float("nan")},
            {"learning_rate_multiplier": True},
        )

        for kwargs in invalid_kwargs:
            with self.subTest(kwargs=kwargs):
                with self.assertRaises(ModelWriterError):
                    build_gemini_tuning_config(
                        training_dataset_uri=(
                            "gs://wd-transcription-data/sft/dv-001/model_inputs/gemini/train.jsonl"
                        ),
                        **kwargs,
                    )

    def test_gemini_writer_rejects_unsupported_audio_mime(self) -> None:
        segment = _segment(
            "feed-a",
            row_index=9,
            audio_suffix="flac",
            model_ready_audio_uri="gs://wd-ready/feed-a/9.wav",
        )

        with self.assertRaisesRegex(ModelWriterError, "row_index=9"):
            build_gemini_inputs(
                (segment,),
                system_prompt="sys",
                user_prompt="user",
                training_dataset_uri="gs://wd-transcription-data/sft/dv-001/model_inputs/gemini/train.jsonl",
            )

    def test_infer_audio_mime_type_maps_supported_audio(self) -> None:
        self.assertEqual(
            infer_audio_mime_type("gs://bucket/a.flac"), "audio/flac"
        )
        self.assertEqual(
            infer_audio_mime_type("gs://bucket/a.mp3"), "audio/mpeg"
        )

    def test_gemini_requires_model_ready_audio_uri(self) -> None:
        segment = _segment(
            "feed-a", row_index=13, model_ready_audio_uri=None
        )

        with self.assertRaisesRegex(
            ModelWriterError, "row_index=13 missing model_ready_audio_uri"
        ):
            build_gemini_inputs(
                (segment,),
                system_prompt="sys",
                user_prompt="user",
                training_dataset_uri="gs://wd-transcription-data/sft/dv-001/model_inputs/gemini/train.jsonl",
            )


class TestWriterSafety(unittest.TestCase):
    def test_writers_reject_non_gcs_model_ready_audio_uri(self) -> None:
        invalid_values = (
            "",
            " https://example.invalid/audio.flac ",
            "s3://bucket/audio.flac",
        )

        for invalid in invalid_values:
            segment = _segment(
                "feed-a",
                row_index=14,
                model_ready_audio_uri=invalid,
            )
            with self.subTest(writer="nemo", invalid=invalid):
                with self.assertRaisesRegex(
                    ModelWriterError,
                    "row_index=14 missing model_ready_audio_uri",
                ):
                    build_nemo_inputs(
                        (segment,),
                        train_manifest_uri="gs://wd-transcription-data/sft/v1/model_inputs/nemo/train.jsonl",
                        eval_manifest_uri="gs://wd-transcription-data/sft/v1/model_inputs/nemo/eval.jsonl",
                    )
            with self.subTest(writer="whisper", invalid=invalid):
                with self.assertRaisesRegex(
                    ModelWriterError,
                    "row_index=14 missing model_ready_audio_uri",
                ):
                    build_whisper_inputs((segment,))
            with self.subTest(writer="gemini", invalid=invalid):
                with self.assertRaisesRegex(
                    ModelWriterError,
                    "row_index=14 missing model_ready_audio_uri",
                ):
                    build_gemini_inputs(
                        (segment,),
                        system_prompt="sys",
                        user_prompt="user",
                        training_dataset_uri="gs://wd-transcription-data/sft/v1/model_inputs/gemini/train.jsonl",
                    )

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
        gemini = build_gemini_inputs(
            segments,
            system_prompt="sys",
            user_prompt="user",
            training_dataset_uri="gs://wd-transcription-data/sft/v1/model_inputs/gemini/train.jsonl",
            validation_dataset_uri="gs://wd-transcription-data/sft/v1/model_inputs/gemini/eval.jsonl",
        )
        output = json.dumps(
            {
                "nemo_rows": nemo.rows_by_split,
                "nemo_config": nemo.config,
                "whisper_rows": whisper.rows_by_split,
                "gemini_rows": gemini.rows_by_split,
                "gemini_config": gemini.config,
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
