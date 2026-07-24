"""Tests for Gemini evaluation artifact paths."""

import dataclasses
import unittest

from common.gemini import context, eval_artifacts
from gemini_sft import artifacts as sft_artifacts


def _eval_row(
    audio_uri: str,
    text: str,
    *,
    example_id: str,
    segment_id: str,
    offset: float,
    duration: float = 1.0,
) -> dict[str, object]:
    return {
        "audio_filepath": audio_uri,
        "text": text,
        "example_id": example_id,
        "segment_id": segment_id,
        "offset": offset,
        "duration": duration,
        "split": "eval",
        "dataset": {"name": "radio", "family": "radio"},
    }


class TestGeminiEvalArtifacts(unittest.TestCase):
    def test_builds_stable_eval_target_artifact_paths(self) -> None:
        prefix = "gs://bucket/sft/runs/run-a/"

        paths = eval_artifacts.eval_target_artifact_paths(
            prefix, "checkpoint_6"
        )

        self.assertEqual(
            eval_artifacts.evals_prefix(prefix),
            "gs://bucket/sft/runs/run-a/evals",
        )
        self.assertEqual(
            eval_artifacts.eval_target_prefix(prefix, "checkpoint_6"),
            "gs://bucket/sft/runs/run-a/evals/checkpoint_6",
        )
        self.assertEqual(
            paths.input_uri,
            "gs://bucket/sft/runs/run-a/evals/checkpoint_6/input.jsonl",
        )
        self.assertEqual(
            paths.output_uri,
            "gs://bucket/sft/runs/run-a/evals/checkpoint_6/output/",
        )
        self.assertEqual(
            paths.batch_metadata_uri,
            "gs://bucket/sft/runs/run-a/evals/checkpoint_6/"
            "batch_predictions.meta.json",
        )
        self.assertEqual(
            paths.batch_job_metadata_uri,
            "gs://bucket/sft/runs/run-a/evals/checkpoint_6/batch_job.meta.json",
        )
        self.assertEqual(
            paths.online_predictions_uri,
            "gs://bucket/sft/runs/run-a/evals/checkpoint_6/"
            "online_predictions.jsonl",
        )
        self.assertEqual(
            paths.online_metadata_uri,
            "gs://bucket/sft/runs/run-a/evals/checkpoint_6/"
            "online_predictions.meta.json",
        )
        self.assertEqual(
            eval_artifacts.batch_prediction_metadata_uri(
                prefix, "checkpoint_6"
            ),
            paths.batch_metadata_uri,
        )
        self.assertEqual(
            eval_artifacts.online_prediction_uri(prefix, "checkpoint_6"),
            paths.online_predictions_uri,
        )
        self.assertEqual(
            eval_artifacts.online_prediction_metadata_uri(
                prefix, "checkpoint_6"
            ),
            paths.online_metadata_uri,
        )
        self.assertEqual(
            eval_artifacts.wer_summary_gcs_uris(prefix),
            (
                "gs://bucket/sft/runs/run-a/evals/wer_summary.json",
                "gs://bucket/sft/runs/run-a/evals/wer_summary.md",
            ),
        )

    def test_causal_segments_normalize_training_and_eval_identically(
        self,
    ) -> None:
        source_rows_by_split = {
            split: [
                {
                    **_eval_row(
                        f"gs://bucket/audio/{split}.flac",
                        "reference",
                        example_id=f"{split}-example",
                        segment_id="001",
                        offset=0.0,
                    ),
                    "split": split,
                    "original_audio_uri": ("gs://bucket/source/original.wav"),
                    "original_offset": 12.5,
                }
            ]
            for split in ("train", "eval")
        }
        segments: dict[str, context.EvaluationSegment] = {}
        for split, source_rows in source_rows_by_split.items():
            _, canonical_rows = sft_artifacts.canonical_rows_from_entries(
                source_rows,
                split=split,
                source="test",
            )
            (segments[split],) = sft_artifacts.causal_segments_from_rows(
                source_rows,
                canonical_rows,
                split=split,
            )

        self.assertEqual(segments["train"].split, "train")
        self.assertEqual(segments["eval"].split, "eval")
        self.assertEqual(
            (
                segments["train"].source_key,
                segments["train"].start_seconds,
                segments["train"].end_seconds,
                segments["train"].manifest_index,
            ),
            (
                segments["eval"].source_key,
                segments["eval"].start_seconds,
                segments["eval"].end_seconds,
                segments["eval"].manifest_index,
            ),
        )
        self.assertEqual(
            segments["train"].source_key,
            "gs://bucket/source/original.wav",
        )
        self.assertEqual(segments["train"].start_seconds, 12.5)
        self.assertEqual(segments["train"].end_seconds, 13.5)
        self.assertEqual(segments["train"].manifest_index, 0)
        for segment in segments.values():
            self.assertNotIn("text", dataclasses.asdict(segment))

    def test_training_and_eval_normalization_produce_identical_schedules(
        self,
    ) -> None:
        source_rows_by_split = {
            split: [
                {
                    **_eval_row(
                        f"gs://bucket/audio/{name}.flac",
                        name,
                        example_id=f"{split}-{name}",
                        segment_id=str(index),
                        offset=0.0,
                        duration=duration,
                    ),
                    "split": split,
                    "original_audio_uri": ("gs://bucket/source/original.wav"),
                    "original_offset": source_offset,
                }
                for index, (name, source_offset, duration) in enumerate(
                    (
                        ("first", 0.0, 3.0),
                        ("overlap", 2.0, 2.0),
                        ("later", 5.0, 1.0),
                    )
                )
            ]
            for split in ("train", "eval")
        }
        schedules: dict[str, list[context.CausalScheduleRow]] = {}
        for split, source_rows in source_rows_by_split.items():
            _, canonical_rows = sft_artifacts.canonical_rows_from_entries(
                source_rows,
                split=split,
                source="test",
            )
            segments = sft_artifacts.causal_segments_from_rows(
                source_rows,
                canonical_rows,
                split=split,
            )
            schedules[split] = context.build_strict_causal_schedule(
                segments,
                max_turns=2,
            )

        schedule_shapes = {
            split: [(row.dependency_audio_uris, row.wave) for row in schedule]
            for split, schedule in schedules.items()
        }
        self.assertEqual(
            schedule_shapes["train"],
            schedule_shapes["eval"],
        )
        self.assertEqual(
            schedule_shapes["train"],
            [
                ((), 0),
                ((), 0),
                (
                    (
                        "gs://bucket/audio/first.flac",
                        "gs://bucket/audio/overlap.flac",
                    ),
                    1,
                ),
            ],
        )

    def test_causal_segments_reject_alignment_drift(self) -> None:
        row = {
            **_eval_row(
                "gs://bucket/audio/001.flac",
                "reference",
                example_id="example",
                segment_id="001",
                offset=0.0,
            ),
            "original_audio_uri": "gs://bucket/source/original.wav",
            "original_offset": 0.0,
        }
        _, canonical_rows = sft_artifacts.canonical_rows_from_entries(
            [row],
            split="eval",
            source="test",
        )

        with self.assertRaisesRegex(ValueError, "equal lengths"):
            sft_artifacts.causal_segments_from_rows(
                [],
                canonical_rows,
                split="eval",
            )

    def test_training_causal_segment_uses_contextual_diagnostic(self) -> None:
        row = {
            **_eval_row(
                "gs://bucket/audio/train.flac",
                "reference",
                example_id="train-example",
                segment_id="001",
                offset=0.0,
            ),
            "split": "train",
            "original_audio_uri": "gs://bucket/source/original.wav",
            "original_offset": True,
        }
        _, canonical_rows = sft_artifacts.canonical_rows_from_entries(
            [row],
            split="train",
            source="test",
        )

        with self.assertRaisesRegex(
            TypeError,
            "contextual row original_offset",
        ):
            sft_artifacts.causal_segments_from_rows(
                [row],
                canonical_rows,
                split="train",
            )

    def test_eval_provider_segments_use_complete_original_provenance(
        self,
    ) -> None:
        rows = [
            {
                **_eval_row(
                    "gs://bucket/audio/001.flac",
                    "reference must remain scoring-only",
                    example_id="fallback-example",
                    segment_id="001",
                    offset=2.0,
                ),
                "original_audio_uri": "gs://bucket/source/original.wav",
                "original_offset": 12.5,
                "source_audio": {
                    "audio_filepath": "gs://bucket/source/secondary.wav",
                    "offset": 7.0,
                    "duration": 1.25,
                },
            }
        ]

        prepared = sft_artifacts.eval_rows_for_inference_from_entries(
            rows,
            source="test",
            prior_context_count=8,
        )

        self.assertEqual(prepared.source_rows[0]["text"], rows[0]["text"])
        self.assertEqual(prepared.eval_rows[0].text, rows[0]["text"])
        segment = prepared.segments[0]
        self.assertNotIn("text", dataclasses.asdict(segment))
        self.assertEqual(segment.audio_uri, rows[0]["audio_filepath"])
        self.assertEqual(
            segment.source_key,
            "gs://bucket/source/original.wav",
        )
        self.assertEqual(segment.start_seconds, 12.5)
        self.assertEqual(segment.end_seconds, 13.5)

    def test_eval_segment_source_key_uses_source_audio_identity(
        self,
    ) -> None:
        rows = [
            {
                **_eval_row(
                    "gs://bucket/audio/001.flac",
                    "one",
                    example_id="example-a",
                    segment_id="001",
                    offset=2.0,
                ),
                "source_audio": {
                    "audio_filepath": "gs://bucket/source/source-a.wav",
                    "offset": 8.0,
                    "duration": 1.0,
                },
            },
        ]

        prepared = sft_artifacts.eval_rows_for_inference_from_entries(
            rows,
            source="test",
            prior_context_count=8,
        )

        self.assertEqual(
            [segment.source_key for segment in prepared.segments],
            ["gs://bucket/source/source-a.wav"],
        )
        self.assertEqual(
            [segment.start_seconds for segment in prepared.segments],
            [8.0],
        )

    def test_contextual_eval_rejects_example_id_as_source_identity(
        self,
    ) -> None:
        row = _eval_row(
            "gs://bucket/audio/001.flac",
            "reference remains scoring-only",
            example_id="not-a-durable-source-identity",
            segment_id="001",
            offset=3.0,
        )

        with self.assertRaisesRegex(ValueError, "durable source identity"):
            sft_artifacts.eval_rows_for_inference_from_entries(
                [row],
                source="test",
                prior_context_count=8,
            )

    def test_eval_limit_is_applied_before_provider_segments_are_built(
        self,
    ) -> None:
        rows = [
            {
                **_eval_row(
                    "gs://bucket/audio/001.flac",
                    "one",
                    example_id="example",
                    segment_id="001",
                    offset=0.0,
                ),
                "original_audio_uri": "gs://bucket/source/original.wav",
                "original_offset": 0.0,
            },
            {
                **_eval_row(
                    "gs://bucket/audio/002.flac",
                    "two",
                    example_id="example",
                    segment_id="002",
                    offset=2.0,
                ),
                "original_audio_uri": "gs://bucket/source/original.wav",
                "original_offset": 2.0,
            },
        ]

        prepared = sft_artifacts.eval_rows_for_inference_from_entries(
            rows,
            source="test",
            limit=1,
            prior_context_count=8,
        )

        self.assertEqual(len(prepared.source_rows), 1)
        self.assertEqual(len(prepared.eval_rows), 1)
        self.assertEqual(len(prepared.segments), 1)
        self.assertEqual(prepared.segments[0].manifest_index, 0)

    def test_eval_segment_rejects_malformed_preferred_source_metadata(
        self,
    ) -> None:
        row = {
            **_eval_row(
                "gs://bucket/audio/001.flac",
                "one",
                example_id="example",
                segment_id="001",
                offset=0.0,
            ),
            "original_audio_uri": " ",
        }

        with self.assertRaisesRegex(ValueError, "original_audio_uri"):
            sft_artifacts.eval_rows_for_inference_from_entries(
                [row],
                source="test",
                prior_context_count=8,
            )

    def test_eval_segment_rejects_partial_original_provenance(self) -> None:
        row = {
            **_eval_row(
                "gs://bucket/audio/001.flac",
                "one",
                example_id="example",
                segment_id="001",
                offset=0.0,
            ),
            "original_audio_uri": "gs://bucket/source/original.wav",
            "source_audio": {
                "audio_filepath": "gs://bucket/source/secondary.wav",
                "offset": 7.0,
                "duration": 1.25,
            },
        }

        with self.assertRaisesRegex(ValueError, "complete original provenance"):
            sft_artifacts.eval_rows_for_inference_from_entries(
                [row],
                source="test",
                prior_context_count=8,
            )

    def test_eval_segment_rejects_partial_source_audio_provenance(self) -> None:
        row = {
            **_eval_row(
                "gs://bucket/audio/001.flac",
                "one",
                example_id="example",
                segment_id="001",
                offset=0.0,
            ),
            "source_audio": {
                "audio_filepath": "gs://bucket/source/source.wav",
                "offset": 7.0,
            },
        }

        with self.assertRaisesRegex(ValueError, "complete source_audio"):
            sft_artifacts.eval_rows_for_inference_from_entries(
                [row],
                source="test",
                prior_context_count=8,
            )

    def test_stateless_provider_view_ignores_optional_causal_metadata(
        self,
    ) -> None:
        row = {
            **_eval_row(
                "gs://bucket/audio/001.flac",
                "reference remains scoring-only",
                example_id="example",
                segment_id="001",
                offset=3.0,
            ),
            "original_audio_uri": " ",
            "original_offset": "not-a-number",
        }

        prepared = sft_artifacts.eval_rows_for_inference_from_entries(
            [row],
            source="test",
            prior_context_count=0,
        )

        segment = prepared.segments[0]
        self.assertNotIn("text", dataclasses.asdict(segment))
        self.assertEqual(segment.source_key, "example")
        self.assertEqual(segment.start_seconds, 3.0)

    def test_legacy_history_loader_remains_available_to_main_callers(
        self,
    ) -> None:
        rows = [
            {
                **_eval_row(
                    "gs://bucket/audio/001.flac",
                    "first",
                    example_id="example",
                    segment_id="001",
                    offset=0.0,
                ),
                "original_audio_uri": "gs://bucket/source/original.wav",
                "original_offset": 0.0,
            },
            {
                **_eval_row(
                    "gs://bucket/audio/002.flac",
                    "second",
                    example_id="example",
                    segment_id="002",
                    offset=1.0,
                ),
                "original_audio_uri": "gs://bucket/source/original.wav",
                "original_offset": 1.0,
            },
        ]

        prepared = sft_artifacts.eval_rows_with_histories_from_entries(
            rows,
            source="test",
            prior_context_count=1,
        )

        self.assertEqual(
            prepared.histories,
            [
                [],
                [
                    context.ContextTurn(
                        "gs://bucket/audio/001.flac",
                        "first",
                    )
                ],
            ],
        )


if __name__ == "__main__":
    unittest.main()
