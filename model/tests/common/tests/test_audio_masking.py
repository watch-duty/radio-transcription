from __future__ import annotations

import unittest

from common import audio_masking
from common.audio_masking import Disposition


class TestClassify(unittest.TestCase):
    def test_plain_transcription_is_speech(self) -> None:
        self.assertIs(
            audio_masking.classify("TRANSCRIPTION"), Disposition.SPEECH
        )

    def test_composite_pii_labels_are_masked(self) -> None:
        # The exact labels annotators export. Matching these by equality
        # leaves them unmasked, which is the defect this taxonomy fixes.
        for category in (
            "PII",
            "PII/ADDRESS",
            "PII/ID/PHONE",
            "PII/MEDICAL",
        ):
            with self.subTest(category=category):
                self.assertIs(
                    audio_masking.classify(category), Disposition.MASK
                )

    def test_untranscribed_voice_categories_are_masked(self) -> None:
        for category in (
            "UNINTELLIGIBLE",
            "FOREIGN_SPEECH",
            "UNKNOWN",
            "LAUGHTER",
        ):
            with self.subTest(category=category):
                self.assertIs(
                    audio_masking.classify(category), Disposition.MASK
                )

    def test_automated_signaling_is_preserved(self) -> None:
        # Repeater tones, pager alerts, and carrier squelch are real
        # acoustics the model should hear rather than noise to hide.
        for category in ("DTMF", "RINGING", "STATIC"):
            with self.subTest(category=category):
                self.assertIs(
                    audio_masking.classify(category), Disposition.PRESERVE
                )

    def test_categories_are_normalized_before_matching(self) -> None:
        for category in ("  pii/address ", "Pii/Address", "PII/address"):
            with self.subTest(category=category):
                self.assertIs(
                    audio_masking.classify(category), Disposition.MASK
                )

    def test_speech_wins_over_a_maskable_subtype(self) -> None:
        self.assertIs(
            audio_masking.classify("TRANSCRIPTION/PII"), Disposition.SPEECH
        )

    def test_labels_outside_the_taxonomy_are_unrecognized(self) -> None:
        for category in ("MUSIC", "CROSSTALK", "", None):
            with self.subTest(category=category):
                self.assertIs(
                    audio_masking.classify(category),
                    Disposition.UNRECOGNIZED,
                )


class TestUnrecognizedCategories(unittest.TestCase):
    def test_reports_distinct_unknown_labels_only(self) -> None:
        categories = [
            "TRANSCRIPTION",
            "PII/ADDRESS",
            "MUSIC",
            "music",
            "DTMF",
            "CROSSTALK",
        ]
        self.assertEqual(
            audio_masking.unrecognized_categories(categories),
            ["CROSSTALK", "MUSIC"],
        )

    def test_reports_missing_categories(self) -> None:
        self.assertEqual(
            audio_masking.unrecognized_categories(["TRANSCRIPTION", None, ""]),
            ["<missing>"],
        )

    def test_fully_classified_manifest_reports_nothing(self) -> None:
        self.assertEqual(
            audio_masking.unrecognized_categories(
                ["TRANSCRIPTION", "PII", "DTMF", "STATIC"]
            ),
            [],
        )


class TestSubtractIntervals(unittest.TestCase):
    def test_protected_span_inside_the_mask_splits_it(self) -> None:
        # The regression that matters: a maskable annotation spanning an
        # utterance. Clamping a mask edge cannot express "hole in the
        # middle", so the utterance was masked over entirely.
        self.assertEqual(
            audio_masking.subtract_intervals((100, 400), [(200, 300)]),
            [(100, 200), (300, 400)],
        )

    def test_mask_with_no_protected_overlap_is_unchanged(self) -> None:
        self.assertEqual(
            audio_masking.subtract_intervals((100, 200), [(300, 400)]),
            [(100, 200)],
        )

    def test_fully_covered_mask_is_dropped(self) -> None:
        self.assertEqual(
            audio_masking.subtract_intervals((100, 200), [(50, 250)]),
            [],
        )

    def test_overlap_at_each_edge_trims_the_mask(self) -> None:
        self.assertEqual(
            audio_masking.subtract_intervals((100, 400), [(50, 150)]),
            [(150, 400)],
        )
        self.assertEqual(
            audio_masking.subtract_intervals((100, 400), [(350, 450)]),
            [(100, 350)],
        )

    def test_unsorted_and_overlapping_protected_spans_are_merged(self) -> None:
        self.assertEqual(
            audio_masking.subtract_intervals(
                (0, 500), [(300, 400), (100, 200), (150, 250)]
            ),
            [(0, 100), (250, 300), (400, 500)],
        )

    def test_touching_protected_spans_do_not_leave_a_gap(self) -> None:
        self.assertEqual(
            audio_masking.subtract_intervals(
                (0, 300), [(100, 200), (200, 250)]
            ),
            [(0, 100), (250, 300)],
        )

    def test_no_protected_spans_returns_the_mask(self) -> None:
        self.assertEqual(
            audio_masking.subtract_intervals((100, 200), []),
            [(100, 200)],
        )

    def test_degenerate_spans_are_dropped(self) -> None:
        self.assertEqual(audio_masking.subtract_intervals((100, 100), []), [])
        self.assertEqual(audio_masking.subtract_intervals((200, 100), []), [])
        self.assertEqual(
            audio_masking.subtract_intervals((0, 100), [(50, 50)]),
            [(0, 100)],
        )


class TestToSampleRange(unittest.TestCase):
    def test_rounds_outward(self) -> None:
        # 0.00005s either side of a sample boundary must not be dropped.
        self.assertEqual(
            audio_masking.to_sample_range(0.99999, 2.00001, 16000),
            (15999, 32001),
        )

    def test_offsets_by_the_segment_origin(self) -> None:
        self.assertEqual(
            audio_masking.to_sample_range(2.0, 3.0, 16000, origin_s=1.5),
            (8000, 24000),
        )

    def test_clamps_below_zero_and_above_the_limit(self) -> None:
        self.assertEqual(
            audio_masking.to_sample_range(
                -5.0, 99.0, 16000, origin_s=0.0, limit=16000
            ),
            (0, 16000),
        )

    def test_span_beyond_the_audio_is_empty(self) -> None:
        start, end = audio_masking.to_sample_range(
            50.0, 51.0, 16000, limit=16000
        )
        self.assertEqual(start, end)

    def test_rejects_a_nonpositive_sample_rate(self) -> None:
        with self.assertRaises(ValueError):
            audio_masking.to_sample_range(0.0, 1.0, 0)


class TestFadeBounds(unittest.TestCase):
    def test_ramp_extends_both_sides_when_unobstructed(self) -> None:
        self.assertEqual(
            audio_masking.fade_bounds((500, 900), 160, [], limit=16000),
            (340, 1060),
        )

    def test_ramp_stops_at_a_protected_span(self) -> None:
        # Mask abuts speech on its right; the ramp must not cross in.
        self.assertEqual(
            audio_masking.fade_bounds(
                (500, 900), 160, [(900, 2000)], limit=16000
            ),
            (340, 900),
        )

    def test_ramp_stops_at_protected_spans_on_both_sides(self) -> None:
        # A mask carved out from between two utterances gets no ramp room
        # at all, rather than eating 10ms off each neighbour.
        self.assertEqual(
            audio_masking.fade_bounds(
                (500, 900), 160, [(0, 500), (900, 2000)], limit=16000
            ),
            (500, 900),
        )

    def test_narrow_speech_gap_between_masks_is_untouched(self) -> None:
        # Speech shorter than two ramps used to be overwritten entirely.
        protected = [(500, 560)]
        left = audio_masking.fade_bounds(
            (100, 500), 160, protected, limit=16000
        )
        right = audio_masking.fade_bounds(
            (560, 900), 160, protected, limit=16000
        )
        self.assertEqual(left[1], 500)
        self.assertEqual(right[0], 560)

    def test_clamps_to_the_available_audio(self) -> None:
        self.assertEqual(
            audio_masking.fade_bounds((0, 100), 160, [], limit=200),
            (0, 200),
        )


class TestSegmentedAudioPrefix(unittest.TestCase):
    def test_masked_lane(self) -> None:
        self.assertEqual(
            audio_masking.segmented_audio_prefix("echo/eval", masked=True),
            "segmented_audio/echo/eval_masked",
        )

    def test_preprocessed_lane_has_no_suffix(self) -> None:
        self.assertEqual(
            audio_masking.segmented_audio_prefix(
                "echo/eval", preprocessed=True
            ),
            "segmented_audio/echo/eval",
        )

    def test_untouched_audio_lands_in_the_raw_lane(self) -> None:
        self.assertEqual(
            audio_masking.segmented_audio_prefix("echo/eval"),
            "segmented_audio/echo/eval_raw",
        )

    def test_producer_and_consumer_agree_on_the_same_dataset(self) -> None:
        # The contract that broke: the notebook writing masked audio and
        # the notebook reading it must derive the same prefix from the
        # dataset path an operator types into both.
        written = audio_masking.segmented_audio_prefix(
            "fire_notifications", masked=True
        )
        read = audio_masking.segmented_audio_prefix(
            "fire_notifications", masked=True
        )
        self.assertEqual(written, read)

    def test_masked_and_raw_lanes_never_collide(self) -> None:
        self.assertNotEqual(
            audio_masking.segmented_audio_prefix("echo/eval", masked=True),
            audio_masking.segmented_audio_prefix("echo/eval"),
        )

    def test_surrounding_slashes_and_whitespace_are_trimmed(self) -> None:
        self.assertEqual(
            audio_masking.segmented_audio_prefix("  /echo/eval/ ", masked=True),
            "segmented_audio/echo/eval_masked",
        )

    def test_rejects_a_path_that_already_carries_a_lane_suffix(self) -> None:
        # Guards the double-suffix failure an operator hits by pasting the
        # full written path back into the consumer notebook.
        for path in ("echo/eval_masked", "echo/eval_raw"):
            with self.subTest(path=path):
                with self.assertRaises(ValueError):
                    audio_masking.segmented_audio_prefix(path, masked=True)

    def test_rejects_an_empty_path(self) -> None:
        for path in ("", "   ", "/"):
            with self.subTest(path=path):
                with self.assertRaises(ValueError):
                    audio_masking.segmented_audio_prefix(path, masked=True)

    def test_rejects_both_lanes_at_once(self) -> None:
        with self.assertRaises(ValueError):
            audio_masking.segmented_audio_prefix(
                "echo/eval", masked=True, preprocessed=True
            )


if __name__ == "__main__":
    unittest.main()
