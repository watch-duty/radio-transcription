"""Golden + policy tests for common.scoring.

WER policy mapping:
  verbatim policy           = compute_wer(refs, hyps, normalizer=None)
  spelled-to-digit policy   = compute_wer(refs, hyps, normalizer=build_normalizer())
Both must satisfy WER(x, x) == 0 — the forward-comparability invariant.

Heavy-dep tests are skipif-gated on the [scoring] extra (jiwer + nemo_text_processing);
the suite SKIPS — never errors — on a bare-core checkout.
"""

import unittest

from common import scoring as scoring_lib
from common.scoring import (
    bootstrap_paired,
    build_normalizer,
    compute_cer,
    compute_wer,
    count_keyword_occurrences,
    duration_bucket_wer,
    keyword_metrics,
)

try:
    # build_normalizer() needs BOTH packages; probe both so a partial
    # install yields a clean skip rather than a build_normalizer() error.
    import jiwer  # noqa: F401 — presence check only
    import nemo_text_processing  # noqa: F401 — presence check only

    _SCORING_AVAILABLE = True
except ImportError:
    _SCORING_AVAILABLE = False

_scoring_required = unittest.skipIf(
    not _SCORING_AVAILABLE,
    "requires the [scoring] extra (jiwer + nemo_text_processing)",
)


@_scoring_required
class TestBuildNormalizerGolden(unittest.TestCase):
    """Golden tests pin normalizer behavior — changing scoring.py must update these."""

    def setUp(self) -> None:
        self.normalizer = build_normalizer()

    def test_strips_fillers(self) -> None:
        result = self.normalizer("uh engine forty one copy")
        # "uh" must not appear as a standalone token after filler stripping
        self.assertNotIn("uh", result.split())

    def test_strips_unintelligible_tokens(self) -> None:
        result = self.normalizer("engine [unintelligible] forty one copy")
        self.assertNotIn("unintelligible", result.split())

    def test_replaces_long_digit_strings_with_hallucination_placeholder(
        self,
    ) -> None:
        # 10-digit string triggers the hallucination placeholder
        result = self.normalizer("1234567890 engine 41")
        # RemovePunctuation strips brackets, so check for bare word
        self.assertIn("hallucination", result)

    def test_lowercases_output(self) -> None:
        result = self.normalizer("Engine 41 COPY")
        self.assertEqual(result, result.lower())

    def test_removes_hyphens(self) -> None:
        result = self.normalizer("10-4")
        self.assertNotIn("-", result)

    def test_collapses_whitespace(self) -> None:
        result = self.normalizer("engine\n41\tcopy")
        self.assertNotIn("\n", result)
        self.assertNotIn("\t", result)
        self.assertNotIn("  ", result)

    def test_identical_input_normalizes_identically(self) -> None:
        text = "Engine 41 copy"
        result1 = self.normalizer(text)
        result2 = self.normalizer(text)
        self.assertEqual(result1, result2)

    def test_splits_multi_digit_numbers_per_digit(self) -> None:
        """Multi-digit numbers ('41') split into single-digit tokens ('4 1')."""
        # Radio unit IDs and ten-codes are often spoken digit-by-digit; scoring
        # per digit keeps "41" and "4 1" equivalent transcripts.
        result = self.normalizer("engine 41 copy")
        tokens = result.split()
        self.assertNotIn("41", tokens)
        self.assertIn("4", tokens)
        self.assertIn("1", tokens)

    def test_inverse_normalizes_spelled_numbers_to_digits(self) -> None:
        """NeMo ITN converts spelled numbers to digits: 'forty one' -> '4 1'."""
        result = self.normalizer("engine forty one copy")
        tokens = result.split()
        self.assertNotIn("forty", tokens)
        self.assertNotIn("one", tokens)
        self.assertIn("4", tokens)
        self.assertIn("1", tokens)

    def test_small_number_words_fall_back_to_manual_map(self) -> None:
        """Small spelled numbers (one-ten) map to digits via manual fallback."""
        # Manual fallback is the safety net for tokens NeMo ITN leaves alone.
        result = self.normalizer("count one two three")
        tokens = result.split()
        self.assertIn("1", tokens)
        self.assertIn("2", tokens)
        self.assertIn("3", tokens)


@_scoring_required
class TestComputeWerPolicies(unittest.TestCase):
    """Verbatim vs normalized WER produce distinct results on the same input."""

    def test_verbatim_policy_identical_lists_score_zero(self) -> None:
        refs = ["engine 41 copy", "battalion 2 responding"]
        result = compute_wer(refs, refs, normalizer=None)
        self.assertEqual(result["wer"], 0)

    def test_normalized_policy_identical_lists_score_zero(self) -> None:
        refs = ["engine 41 copy", "battalion 2 responding"]
        result = compute_wer(refs, refs, normalizer=build_normalizer())
        self.assertEqual(result["wer"], 0)

    def test_verbatim_policy_is_case_sensitive(self) -> None:
        result = compute_wer(["Engine 41"], ["engine 41"], normalizer=None)
        self.assertGreater(result["wer"], 0)

    def test_normalized_policy_is_case_insensitive(self) -> None:
        # After NeMo normalization both sides are lowercased; "41" normalizes
        # identically on both sides — so WER must be 0
        result = compute_wer(
            ["Engine 41"], ["engine 41"], normalizer=build_normalizer()
        )
        self.assertEqual(result["wer"], 0)

    def test_compute_wer_result_has_breakdown_keys(self) -> None:
        result = compute_wer(["engine 41"], ["engine 41"], normalizer=None)
        for key in ("wer", "insertions", "deletions", "substitutions", "hits"):
            self.assertIn(key, result)

    def test_normalized_empty_reference_scores_empty_hypothesis_correct(
        self,
    ) -> None:
        result = compute_wer(["Um"], [""], normalizer=build_normalizer())
        self.assertEqual(result["wer"], 0)

    def test_normalized_empty_reference_penalizes_nonempty_hypothesis(
        self,
    ) -> None:
        result = compute_wer(["Um"], ["copy"], normalizer=build_normalizer())
        self.assertGreater(result["wer"], 0)

    def test_empty_reference_hallucination_counts_as_insertions(self) -> None:
        result = compute_wer(
            ["engine copy", ""],
            ["engine copy", "extra words"],
            normalizer=None,
        )

        self.assertEqual(result["wer"], 100.0)
        self.assertEqual(result["insertions"], 2)
        self.assertEqual(result["deletions"], 0)
        self.assertEqual(result["substitutions"], 0)
        self.assertEqual(result["hits"], 2)

    def test_all_empty_references_with_hypothesis_reports_insertions(
        self,
    ) -> None:
        result = compute_wer([""], ["extra words"], normalizer=None)

        self.assertEqual(result["wer"], 100.0)
        self.assertEqual(result["insertions"], 2)
        self.assertEqual(result["hits"], 0)


@_scoring_required
class TestComputeCer(unittest.TestCase):
    def test_identical_lists_score_zero_cer(self) -> None:
        result = compute_cer(["engine 41"], ["engine 41"])
        self.assertEqual(result["cer"], 0)

    def test_cer_result_has_cer_key(self) -> None:
        result = compute_cer(["engine 41"], ["engine 41"])
        self.assertIn("cer", result)

    def test_normalized_empty_reference_scores_empty_hypothesis_correct(
        self,
    ) -> None:
        result = compute_cer(["Uh,"], [""], normalizer=build_normalizer())
        self.assertEqual(result["cer"], 0)

    def test_empty_reference_hallucination_counts_as_char_insertions(
        self,
    ) -> None:
        result = compute_cer(["abc", ""], ["abc", "xy"], normalizer=None)

        self.assertEqual(result["cer"], 66.67)


class TestEmptyOrUnintelligibleRate(unittest.TestCase):
    def test_empty_list_returns_zero(self) -> None:
        self.assertEqual(scoring_lib.empty_or_unintelligible_rate([]), 0.0)

    def test_flags_empty_string(self) -> None:
        self.assertEqual(scoring_lib.empty_or_unintelligible_rate([""]), 100.0)

    def test_flags_unintelligible_token(self) -> None:
        self.assertEqual(
            scoring_lib.empty_or_unintelligible_rate(["[UNINTELLIGIBLE]"]),
            100.0,
        )

    def test_mixed_list_partial_rate(self) -> None:
        self.assertEqual(
            scoring_lib.empty_or_unintelligible_rate(
                ["engine 41", "", "copy", ""]
            ),
            50.0,
        )

    def test_mixed_unintelligible_and_empty_outputs(self) -> None:
        self.assertEqual(
            scoring_lib.empty_or_unintelligible_rate(
                ["", "[UNINTELLIGIBLE]", "copy"]
            ),
            66.67,
        )

    def test_missing_prediction_fallback_counts_as_empty_output(self) -> None:
        self.assertEqual(
            scoring_lib.empty_or_unintelligible_rate(["engine 41", ""]),
            50.0,
        )


class TestKeywordMetrics(unittest.TestCase):
    def test_absent_keyword_omitted(self) -> None:
        result = keyword_metrics(["engine 41"], ["engine 41"], ["battalion"])
        self.assertEqual(result, [])

    def test_present_keyword_returns_accuracy_dict(self) -> None:
        result = keyword_metrics(
            ["engine 41 copy"], ["engine 41 copy"], ["engine"]
        )
        self.assertEqual(len(result), 1)
        self.assertIn("keyword", result[0])
        self.assertIn("occurrences", result[0])
        self.assertIn("correctly_identified", result[0])
        self.assertIn("accuracy", result[0])

    def test_count_keyword_occurrences_whole_word_only(self) -> None:
        self.assertEqual(count_keyword_occurrences("copy", "copy that copy"), 2)
        self.assertEqual(count_keyword_occurrences("copy", "copycat"), 0)

    def test_keyword_metrics_length_mismatch_raises(self) -> None:
        with self.assertRaises(ValueError):
            keyword_metrics(["a"], ["a", "b"], ["a"])


@_scoring_required
class TestDurationBucketWer(unittest.TestCase):
    """duration_bucket_wer — duration bucketing, exclusion rules, WER/SER."""

    def test_length_mismatch_raises(self) -> None:
        with self.assertRaises(ValueError):
            duration_bucket_wer(["a"], ["a"], [1.0, 2.0])

    def test_buckets_by_duration_excluding_long_and_negative(self) -> None:
        # Short, Medium, Long, >=15s (excluded), negative (excluded).
        refs = ["one", "two", "three", "four", "five"]
        durations = [1.0, 3.0, 10.0, 20.0, -1.0]
        result = duration_bucket_wer(refs, refs, durations, normalizer=None)
        counts = {r["bucket"]: r["count"] for r in result}
        self.assertEqual(
            counts,
            {"Short (0-2s)": 1, "Medium (2-5s)": 1, "Long (5s+)": 1},
        )
        self.assertEqual(sum(r["count"] for r in result), 3)

    def test_empty_reference_segments_are_excluded(self) -> None:
        # The empty-ground-truth segment must not be counted (M-B2 filter).
        refs = ["engine one", "", "engine two"]
        hyps = ["engine one", "garbage", "engine two"]
        result = duration_bucket_wer(
            refs, hyps, [1.0, 1.0, 1.0], normalizer=None
        )
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["bucket"], "Short (0-2s)")
        self.assertEqual(result[0]["count"], 2)

    def test_identical_lists_score_zero_wer_and_ser(self) -> None:
        result = duration_bucket_wer(
            ["a b c"], ["a b c"], [1.0], normalizer=None
        )
        self.assertEqual(result[0]["wer"], 0)
        self.assertEqual(result[0]["ser"], 0.0)

    def test_ser_is_fraction_of_segments_with_errors(self) -> None:
        # One perfect segment, one wrong, both Short — SER must be 50%.
        refs = ["a b c", "d e f"]
        hyps = ["a b c", "x y z"]
        result = duration_bucket_wer(refs, hyps, [1.0, 1.0], normalizer=None)
        self.assertEqual(result[0]["count"], 2)
        self.assertEqual(result[0]["ser"], 50.0)
        self.assertGreater(result[0]["wer"], 0)

    def test_result_dicts_have_expected_keys(self) -> None:
        result = duration_bucket_wer(["a"], ["a"], [1.0], normalizer=None)
        for key in ("bucket", "wer", "ser", "count"):
            self.assertIn(key, result[0])


@_scoring_required
class TestBootstrapPaired(unittest.TestCase):
    """bootstrap_paired — input validation, determinism, and result shape."""

    def test_length_mismatch_raises(self) -> None:
        with self.assertRaises(ValueError):
            bootstrap_paired(["a", "b"], ["a"], ["a", "b"])

    def test_empty_eval_set_raises(self) -> None:
        with self.assertRaises(ValueError):
            bootstrap_paired([], [], [])

    def test_non_positive_n_resamples_raises(self) -> None:
        with self.assertRaises(ValueError):
            bootstrap_paired(["a"], ["a"], ["a"], n_resamples=0)

    def test_non_int_n_resamples_raises_valueerror(self) -> None:
        """The docstring promises ValueError; without the isinstance guard
        a float would crash with a cryptic TypeError from range() instead.
        """
        with self.assertRaises(ValueError):
            bootstrap_paired(["a"], ["a"], ["a"], n_resamples=1.5)

    def test_bool_n_resamples_raises_valueerror(self) -> None:
        """`bool` is a subclass of `int`, so True / False would pass a plain
        `isinstance(..., int)` check. Reject them explicitly so
        `n_resamples=True` can't silently mean "one resample".
        """
        with self.assertRaises(ValueError):
            bootstrap_paired(["a"], ["a"], ["a"], n_resamples=True)

    def test_confidence_outside_unit_interval_raises(self) -> None:
        for bad in (0.0, 1.0, 1.5, -0.1):
            with self.assertRaises(ValueError):
                bootstrap_paired(["a"], ["a"], ["a"], confidence=bad)

    def test_identical_systems_zero_delta_and_p_value_one(self) -> None:
        refs = ["engine one", "battalion two"]
        result = bootstrap_paired(
            refs, refs, refs, normalizer=None, n_resamples=200
        )
        self.assertEqual(result["delta"], 0)
        self.assertEqual(result["p_value_one_sided"], 1.0)

    def test_result_uses_p_value_one_sided_key(self) -> None:
        result = bootstrap_paired(
            ["a b"], ["a b"], ["a b"], normalizer=None, n_resamples=50
        )
        for key in (
            "wer_a",
            "wer_b",
            "delta",
            "p_value_one_sided",
            "ci_low",
            "ci_high",
            "confidence",
            "n_resamples",
        ):
            self.assertIn(key, result)
        # M-B3: the significance key is p_value_one_sided, never the
        # misleading two-sided name "p_value".
        self.assertNotIn("p_value", result)

    def test_deterministic_under_fixed_seed(self) -> None:
        refs = ["engine one copy", "battalion two", "engine three"]
        hyps_a = ["engine one copy", "battalion two", "engine three"]
        hyps_b = ["engine one", "battalion", "engine three four"]
        first = bootstrap_paired(
            refs, hyps_a, hyps_b, normalizer=None, n_resamples=200, seed=42
        )
        second = bootstrap_paired(
            refs, hyps_a, hyps_b, normalizer=None, n_resamples=200, seed=42
        )
        self.assertEqual(first, second)

    def test_delta_equals_wer_a_minus_wer_b(self) -> None:
        refs = ["engine one copy", "battalion two responding"]
        hyps_a = ["engine one copy", "battalion two responding"]  # perfect
        hyps_b = ["engine one", "battalion"]  # worse
        result = bootstrap_paired(
            refs, hyps_a, hyps_b, normalizer=None, n_resamples=100
        )
        self.assertEqual(
            result["delta"], round(result["wer_a"] - result["wer_b"], 4)
        )
        self.assertEqual(result["wer_a"], 0)
        self.assertGreater(result["wer_b"], 0)


if __name__ == "__main__":
    unittest.main()
