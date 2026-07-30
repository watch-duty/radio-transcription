"""Tests for provenance-safe Gemini context construction."""

from __future__ import annotations

import dataclasses
import json
import unittest

from common.gemini import context


class TestTrainingReferenceHistories(unittest.TestCase):
    def _segment(
        self,
        uri: str,
        *,
        start: float,
        end: float,
        index: int,
    ) -> context.EvaluationSegment:
        return context.EvaluationSegment(
            audio_uri=uri,
            split="train",
            source_key="source-a",
            start_seconds=start,
            end_seconds=end,
            manifest_index=index,
        )

    def test_resolves_references_from_frozen_dependencies(self) -> None:
        rows = [
            {"audio_filepath": "gs://a/one", "text": "one"},
            {"audio_filepath": "gs://a/two", "text": "two"},
            {"audio_filepath": "gs://a/three", "text": "three"},
        ]
        schedule = context.build_strict_causal_schedule(
            [
                self._segment("gs://a/one", start=0, end=1, index=0),
                self._segment("gs://a/two", start=1, end=2, index=1),
                self._segment("gs://a/three", start=2, end=3, index=2),
            ],
            max_turns=2,
        )

        histories = context.build_training_reference_histories(
            rows,
            schedule=schedule,
        )

        self.assertEqual(histories[0], [])
        self.assertEqual(
            histories[1],
            [context.TrainingReferenceTurn("one")],
        )
        self.assertEqual(
            histories[2],
            [
                context.TrainingReferenceTurn("one"),
                context.TrainingReferenceTurn("two"),
            ],
        )

    def test_omits_unusable_selected_reference_without_refill(
        self,
    ) -> None:
        rows = [
            {
                "audio_filepath": "gs://a/one",
                "text": "older usable",
            },
            {
                "audio_filepath": "gs://a/two",
                "text": "[Unintelligible]",
            },
            {"audio_filepath": "gs://a/three", "text": "current"},
        ]
        schedule = context.build_strict_causal_schedule(
            [
                self._segment("gs://a/one", start=0, end=1, index=0),
                self._segment("gs://a/two", start=1, end=2, index=1),
                self._segment("gs://a/three", start=2, end=3, index=2),
            ],
            max_turns=1,
        )

        histories = context.build_training_reference_histories(
            rows,
            schedule=schedule,
        )

        self.assertEqual(
            schedule[2].dependency_audio_uris,
            ("gs://a/two",),
        )
        self.assertEqual(histories[2], [])

    def test_rejects_schedule_and_row_alignment_drift(self) -> None:
        rows = [{"audio_filepath": "gs://a/wrong", "text": "one"}]
        schedule = context.build_strict_causal_schedule(
            [
                self._segment(
                    "gs://a/expected",
                    start=0,
                    end=1,
                    index=0,
                )
            ],
            max_turns=1,
        )

        with self.assertRaisesRegex(ValueError, "alignment"):
            context.build_training_reference_histories(
                rows,
                schedule=schedule,
            )


class TestEvaluationContextBoundary(unittest.TestCase):
    def test_zero_context_preserves_configured_backend(self) -> None:
        for backend in (None, "batch", "online"):
            with self.subTest(backend=backend):
                self.assertEqual(
                    context.resolve_evaluation_backend_for_context(0, backend),
                    backend,
                )

    def test_positive_context_requires_online_backend(self) -> None:
        for backend in (None, "online"):
            with self.subTest(backend=backend):
                self.assertEqual(
                    context.resolve_evaluation_backend_for_context(1, backend),
                    "online",
                )

    def test_positive_context_rejects_batch_backend(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            "predicted-history evaluation requires the online backend",
        ):
            context.resolve_evaluation_backend_for_context(1, "batch")

    def test_backend_contract_rejects_invalid_context_count(self) -> None:
        for count, error, message in (
            (True, TypeError, "must be an integer"),
            (-1, ValueError, "must be non-negative"),
        ):
            with (
                self.subTest(count=count),
                self.assertRaisesRegex(error, message),
            ):
                context.resolve_evaluation_backend_for_context(count, None)

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

    def test_rejects_identical_same_source_intervals(self) -> None:
        segments = [
            self._segment("gs://a/one", start=10, end=20, index=0),
            self._segment("gs://a/two", start=10, end=20, index=1),
        ]

        with self.assertRaisesRegex(
            ValueError,
            r"relationship=equality.*manifest_indices=\(0, 1\)",
        ):
            context.build_strict_causal_schedule(segments, max_turns=2)

    def test_counts_every_identical_pair_independent_of_input_order(
        self,
    ) -> None:
        segments = [
            self._segment("gs://a/one", start=10, end=20, index=0),
            self._segment("gs://a/two", start=10, end=20, index=1),
            self._segment("gs://a/three", start=10, end=20, index=2),
        ]

        for ordered in (segments, [segments[2], segments[0], segments[1]]):
            with (
                self.subTest(
                    order=[segment.audio_uri for segment in ordered],
                ),
                self.assertRaisesRegex(
                    ValueError,
                    r"total_invalid_pairs=3.*relationship=equality",
                ),
            ):
                context.build_strict_causal_schedule(
                    ordered,
                    max_turns=2,
                )

    def test_rejects_strict_containment_in_either_input_order(self) -> None:
        outer = self._segment("gs://a/outer", start=10, end=20, index=0)
        inner = self._segment("gs://a/inner", start=12, end=18, index=1)

        for segments in ([outer, inner], [inner, outer]):
            with (
                self.subTest(order=[row.audio_uri for row in segments]),
                self.assertRaisesRegex(
                    ValueError,
                    r"total_invalid_pairs=1.*relationship=containment",
                ),
            ):
                context.build_strict_causal_schedule(
                    segments,
                    max_turns=2,
                )

    def test_rejects_shared_start_or_end_containment(self) -> None:
        cases = (
            (
                self._segment("gs://a/outer", start=10, end=20, index=0),
                self._segment("gs://a/inner", start=10, end=18, index=1),
            ),
            (
                self._segment("gs://a/outer", start=10, end=20, index=0),
                self._segment("gs://a/inner", start=12, end=20, index=1),
            ),
        )

        for outer, inner in cases:
            with (
                self.subTest(inner=(inner.start_seconds, inner.end_seconds)),
                self.assertRaisesRegex(ValueError, "relationship=containment"),
            ):
                context.build_strict_causal_schedule(
                    [outer, inner],
                    max_turns=2,
                )

    def test_applies_containment_tolerance(self) -> None:
        tolerance = context._CAUSAL_BOUNDARY_TOLERANCE_SECONDS
        near_container = self._segment(
            "gs://a/near-container",
            start=10 + tolerance / 2,
            end=20,
            index=0,
        )
        contained = self._segment(
            "gs://a/contained",
            start=10,
            end=18,
            index=1,
        )

        with self.assertRaisesRegex(ValueError, "relationship=containment"):
            context.build_strict_causal_schedule(
                [near_container, contained],
                max_turns=2,
            )

    def test_partial_overlaps_are_independent_then_both_feed_later(
        self,
    ) -> None:
        segments = [
            self._segment("gs://a/first", start=10, end=20, index=0),
            self._segment("gs://a/second", start=15, end=25, index=1),
            self._segment("gs://a/later", start=25, end=30, index=2),
        ]

        for values in (segments, list(reversed(segments))):
            schedule = context.build_strict_causal_schedule(
                values,
                max_turns=2,
            )

            self.assertEqual(schedule[0].dependency_audio_uris, ())
            self.assertEqual(schedule[1].dependency_audio_uris, ())
            self.assertEqual(
                schedule[2].dependency_audio_uris,
                ("gs://a/first", "gs://a/second"),
            )

    def test_allows_equal_spans_in_other_contextual_populations(self) -> None:
        segments = [
            self._segment("gs://a/eval", start=10, end=20, index=0),
            self._segment(
                "gs://a/other-source",
                start=10,
                end=20,
                index=1,
                source="source-b",
            ),
            self._segment(
                "gs://a/other-split",
                start=10,
                end=20,
                index=2,
                split="train",
            ),
        ]

        schedule = context.build_strict_causal_schedule(
            segments,
            max_turns=2,
        )

        self.assertEqual(
            [row.dependency_audio_uris for row in schedule],
            [(), (), ()],
        )

    def test_zero_history_skips_contextual_duplicate_validation(self) -> None:
        segments = [
            self._segment("gs://a/outer", start=10, end=20, index=0),
            self._segment("gs://a/inner", start=12, end=18, index=1),
        ]

        schedule = context.build_strict_causal_schedule(
            segments,
            max_turns=0,
        )

        self.assertEqual(
            [row.dependency_audio_uris for row in schedule],
            [(), ()],
        )

    def test_candidate_order_retains_last_k_completed_segments(self) -> None:
        segments = [
            self._segment("gs://a/first", start=0, end=1, index=0),
            self._segment("gs://a/second", start=1, end=2, index=1),
            self._segment("gs://a/third", start=2, end=3, index=2),
            self._segment("gs://a/current", start=4, end=5, index=3),
        ]

        schedule = context.build_strict_causal_schedule(
            list(reversed(segments)),
            max_turns=2,
        )

        self.assertEqual(
            schedule[3].dependency_audio_uris,
            ("gs://a/second", "gs://a/third"),
        )

    def test_duplicate_diagnostics_are_deterministic_and_bounded(
        self,
    ) -> None:
        outer = self._segment(
            "gs://a/outer",
            start=0,
            end=100,
            index=0,
        )
        inners = [
            self._segment(
                f"gs://a/inner-{index}",
                start=float(index * 10),
                end=float(index * 10 + 5),
                index=index,
            )
            for index in range(1, 7)
        ]

        messages = []
        for segments in ([outer, *inners], [*reversed(inners), outer]):
            with self.assertRaises(ValueError) as raised:
                context.build_strict_causal_schedule(
                    segments,
                    max_turns=2,
                )
            messages.append(str(raised.exception))

        self.assertEqual(messages[0], messages[1])
        self.assertIn("total_invalid_pairs=6", messages[0])
        self.assertEqual(messages[0].count("relationship="), 5)
        self.assertIn("split='eval'", messages[0])
        self.assertIn("source_key='source-a'", messages[0])
        self.assertIn("audio_uris=", messages[0])
        self.assertIn("intervals=", messages[0])

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


if __name__ == "__main__":
    unittest.main()
