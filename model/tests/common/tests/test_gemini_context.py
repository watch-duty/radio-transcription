"""Tests for provenance-safe Gemini context construction."""

from __future__ import annotations

import dataclasses
import json
import unittest

from common.gemini import context


class TestTrainingReferenceHistories(unittest.TestCase):
    def test_groups_by_original_audio_uri_and_sorts_by_original_offset(
        self,
    ) -> None:
        rows = [
            {
                "audio_filepath": "gs://audio/source-a/002.flac",
                "original_audio_uri": "gs://audio/source-a.flac",
                "original_offset": 2.0,
                "row_index": 2,
                "text": "second",
            },
            {
                "audio_filepath": "gs://audio/source-b/001.flac",
                "original_audio_uri": "gs://audio/source-b.flac",
                "original_offset": 1.0,
                "row_index": 1,
                "text": "other source",
            },
            {
                "audio_filepath": "gs://audio/source-a/001.flac",
                "original_audio_uri": "gs://audio/source-a.flac",
                "original_offset": 1.0,
                "row_index": 1,
                "text": "first",
            },
            {
                "audio_filepath": "gs://audio/source-a/003.flac",
                "original_audio_uri": "gs://audio/source-a.flac",
                "original_offset": 3.0,
                "row_index": 3,
                "text": "[UNINTELLIGIBLE]",
            },
            {
                "audio_filepath": "gs://audio/source-a/004.flac",
                "original_audio_uri": "gs://audio/source-a.flac",
                "original_offset": 4.0,
                "row_index": 4,
                "text": "fourth",
            },
        ]

        histories = context.build_training_reference_histories(
            rows, max_turns=2
        )

        self.assertEqual(
            histories[0],
            [context.TrainingReferenceTurn("first")],
        )
        self.assertEqual(histories[1], [])
        self.assertEqual(histories[2], [])
        expected = [
            context.TrainingReferenceTurn("first"),
            context.TrainingReferenceTurn("second"),
        ]
        self.assertEqual(histories[3], expected)
        self.assertEqual(histories[4], expected)

    def test_limits_history_to_max_turns(self) -> None:
        rows = [
            {
                "audio_filepath": f"gs://audio/{i}.flac",
                "original_audio_uri": "gs://audio/source.flac",
                "original_offset": float(i),
                "text": str(i),
            }
            for i in range(5)
        ]

        histories = context.build_training_reference_histories(
            rows, max_turns=3
        )

        self.assertEqual(
            histories[-1],
            [
                context.TrainingReferenceTurn("1"),
                context.TrainingReferenceTurn("2"),
                context.TrainingReferenceTurn("3"),
            ],
        )

    def test_filters_unintelligible_history_case_insensitively(self) -> None:
        rows = [
            {
                "audio_filepath": "gs://audio/first.flac",
                "original_audio_uri": "gs://audio/source.flac",
                "original_offset": 0.0,
                "text": "[Unintelligible]",
            },
            {
                "audio_filepath": "gs://audio/second.flac",
                "original_audio_uri": "gs://audio/source.flac",
                "original_offset": 1.0,
                "text": "usable",
            },
        ]

        histories = context.build_training_reference_histories(
            rows,
            max_turns=1,
        )

        self.assertEqual(histories, [[], []])

    def test_source_group_does_not_group_unrelated_audio(self) -> None:
        rows = [
            {
                "audio_filepath": "gs://audio/a.flac",
                "source_group": "eval",
                "offset": 1.0,
                "text": "alpha",
            },
            {
                "audio_filepath": "gs://audio/b.flac",
                "source_group": "eval",
                "offset": 2.0,
                "text": "bravo",
            },
        ]

        histories = context.build_training_reference_histories(
            rows, max_turns=2
        )

        self.assertEqual(histories, [[], []])

    def test_missing_episode_key_falls_back_to_unique_row_key(self) -> None:
        self.assertNotEqual(
            context._episode_key({"text": "first"}, 0),
            context._episode_key({"text": "second"}, 1),
        )


class TestEvaluationContextBoundary(unittest.TestCase):
    def test_evaluation_rejects_training_reference_turn(self) -> None:
        with self.assertRaisesRegex(TypeError, "TrainingReferenceTurn"):
            context.build_evaluation_transcription_contents(
                audio_uri="gs://audio/current.flac",
                user_prompt="Transcribe.",
                history=[context.TrainingReferenceTurn("REFERENCE_SECRET")],
            )

    def test_training_rejects_predicted_history_turn(self) -> None:
        with self.assertRaisesRegex(TypeError, "TrainingReferenceTurn"):
            context.build_training_transcription_contents(
                audio_uri="gs://audio/current.flac",
                user_prompt="Transcribe.",
                history=[
                    context.PredictedHistoryTurn(
                        "gs://audio/prior.flac", "PREDICTION_SECRET"
                    )
                ],
            )

    def test_evaluation_contains_prediction_and_exactly_current_audio(
        self,
    ) -> None:
        contents = context.build_evaluation_transcription_contents(
            audio_uri="gs://audio/current.flac",
            user_prompt="Transcribe.",
            history=[
                context.PredictedHistoryTurn(
                    "gs://audio/prior.flac", "PREDICTION_SECRET"
                )
            ],
        )
        payload = json.dumps(contents, sort_keys=True)

        self.assertIn("PREDICTION_SECRET", payload)
        self.assertNotIn("REFERENCE_SECRET", payload)
        self.assertEqual(payload.count("gs://audio/current.flac"), 1)
        self.assertNotIn("gs://audio/prior.flac", payload)

    def test_reference_mutation_cannot_change_evaluation_bytes(self) -> None:
        prediction = context.PredictedHistoryTurn(
            "gs://audio/prior.flac", "PREDICTION_SECRET"
        )

        def evaluation_bytes(reference_text: str) -> bytes:
            reference = context.TrainingReferenceTurn(reference_text)
            self.assertEqual(reference.text, reference_text)
            contents = context.build_evaluation_transcription_contents(
                audio_uri="gs://audio/current.flac",
                user_prompt="Transcribe.",
                history=[prediction],
            )
            return json.dumps(
                contents, sort_keys=True, separators=(",", ":")
            ).encode()

        self.assertEqual(
            evaluation_bytes("REFERENCE_SECRET"),
            evaluation_bytes("MUTATED_REFERENCE_SECRET"),
        )


class TestStrictCausalSchedule(unittest.TestCase):
    def _segment(
        self,
        uri: str,
        *,
        start: float,
        end: float,
        index: int,
        split: str = "eval",
        source: str = "source-a",
    ) -> context.EvaluationSegment:
        return context.EvaluationSegment(
            audio_uri=uri,
            split=split,
            source_key=source,
            start_seconds=start,
            end_seconds=end,
            manifest_index=index,
        )

    def test_schedule_is_strict_causal_clustered_and_manifest_aligned(
        self,
    ) -> None:
        segments = [
            self._segment("gs://a/second", start=1, end=2, index=0),
            self._segment("gs://a/overlap", start=1.5, end=3, index=1),
            self._segment("gs://a/first", start=0, end=1, index=2),
            self._segment("gs://a/fourth", start=3, end=4, index=3),
            self._segment(
                "gs://a/other-source",
                start=0,
                end=1,
                index=4,
                source="source-b",
            ),
            self._segment(
                "gs://a/other-split",
                start=0,
                end=1,
                index=5,
                split="train",
            ),
        ]

        schedule = context.build_strict_causal_schedule(segments, max_turns=2)

        self.assertEqual(
            [row.segment.manifest_index for row in schedule], list(range(6))
        )
        self.assertEqual(schedule[0].dependency_audio_uris, ("gs://a/first",))
        self.assertEqual(schedule[0].wave, 1)
        self.assertEqual(schedule[1].dependency_audio_uris, ("gs://a/first",))
        self.assertEqual(schedule[1].wave, 1)
        self.assertEqual(
            schedule[3].dependency_audio_uris,
            ("gs://a/second", "gs://a/overlap"),
        )
        self.assertEqual(schedule[3].wave, 2)
        self.assertEqual(schedule[4].dependency_audio_uris, ())
        self.assertEqual(schedule[5].dependency_audio_uris, ())

    def test_candidate_order_is_end_start_uri_then_last_k(self) -> None:
        segments = [
            self._segment("gs://a/z", start=0, end=1, index=0),
            self._segment("gs://a/a", start=0, end=1, index=1),
            self._segment("gs://a/m", start=0.5, end=1, index=2),
            self._segment("gs://a/current", start=2, end=3, index=3),
        ]

        schedule = context.build_strict_causal_schedule(
            list(reversed(segments)), max_turns=2
        )

        current = next(
            row for row in schedule if row.segment.audio_uri.endswith("current")
        )
        self.assertEqual(
            current.dependency_audio_uris, ("gs://a/z", "gs://a/m")
        )

    def test_contiguous_boundary_tolerates_float_rounding(self) -> None:
        prior_start = 1.1
        current_start = 1.2
        prior_end = prior_start + 0.1
        self.assertGreater(prior_end, current_start)
        segments = [
            self._segment(
                "gs://a/prior",
                start=prior_start,
                end=prior_end,
                index=0,
            ),
            self._segment(
                "gs://a/current",
                start=current_start,
                end=2.0,
                index=1,
            ),
        ]

        schedule = context.build_strict_causal_schedule(segments, max_turns=1)

        self.assertEqual(
            schedule[1].dependency_audio_uris,
            ("gs://a/prior",),
        )

    def test_zero_history_has_no_dependencies_or_text_field(self) -> None:
        segment = self._segment("gs://a/current", start=0, end=1, index=0)

        (row,) = context.build_strict_causal_schedule([segment], max_turns=0)

        self.assertEqual(row.dependency_audio_uris, ())
        self.assertEqual(row.wave, 0)
        self.assertNotIn(
            "text", {field.name for field in dataclasses.fields(segment)}
        )

    def test_rejects_invalid_timing_and_duplicate_identity(self) -> None:
        invalid = self._segment("gs://a/invalid", start=2, end=1, index=0)
        with self.assertRaisesRegex(ValueError, "end"):
            context.build_strict_causal_schedule([invalid], max_turns=1)

        duplicate = self._segment("gs://a/one", start=1, end=2, index=0)
        with self.assertRaisesRegex(ValueError, "manifest_index"):
            context.build_strict_causal_schedule(
                [
                    self._segment("gs://a/zero", start=0, end=1, index=0),
                    duplicate,
                ],
                max_turns=1,
            )


class TestTemporaryLegacyBridge(unittest.TestCase):
    def test_context_turn_history_remains_available_to_main_callers(
        self,
    ) -> None:
        rows = [
            {
                "audio_filepath": "gs://audio/first.flac",
                "original_audio_uri": "gs://audio/source.flac",
                "original_offset": 0.0,
                "text": "first",
            },
            {
                "audio_filepath": "gs://audio/second.flac",
                "original_audio_uri": "gs://audio/source.flac",
                "original_offset": 1.0,
                "text": "second",
            },
        ]

        histories = context.build_context_histories(rows, max_turns=1)

        self.assertEqual(
            histories,
            [[], [context.ContextTurn("gs://audio/first.flac", "first")]],
        )

    def test_generic_content_builder_accepts_legacy_context_turn(self) -> None:
        contents = context.build_transcription_contents(
            audio_uri="gs://audio/current.flac",
            user_prompt="Transcribe.",
            history=[
                context.ContextTurn(
                    "gs://audio/prior.flac",
                    "prior transcript",
                )
            ],
        )

        self.assertEqual(contents[-1]["role"], "user")
        self.assertIn("prior transcript", json.dumps(contents))


if __name__ == "__main__":
    unittest.main()
