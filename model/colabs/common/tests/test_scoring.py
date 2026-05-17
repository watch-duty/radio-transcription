"""Golden + policy tests for common.scoring.

WER policy mapping:
  verbatim policy           = compute_wer(refs, hyps, normalizer=None)
  spelled-to-digit policy   = compute_wer(refs, hyps, normalizer=build_normalizer())
Both must satisfy WER(x, x) == 0 — the forward-comparability invariant.

Heavy-dep tests are skipif-gated on the [scoring] extra (jiwer + nemo_text_processing);
the suite SKIPS — never errors — on a bare-core checkout.
"""

import unittest

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
        from common.scoring import build_normalizer

        self.normalizer = build_normalizer()

    def test_strips_fillers(self) -> None:
        result = self.normalizer("uh engine forty one copy")
        # "uh" must not appear as a standalone token after filler stripping
        self.assertNotIn("uh", result.split())

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


@_scoring_required
class TestComputeWerPolicies(unittest.TestCase):
    """Verbatim vs normalized WER produce distinct results on the same input."""

    def test_verbatim_policy_identical_lists_score_zero(self) -> None:
        from common.scoring import compute_wer

        refs = ["engine 41 copy", "battalion 2 responding"]
        result = compute_wer(refs, refs, normalizer=None)
        self.assertEqual(result["wer"], 0)

    def test_normalized_policy_identical_lists_score_zero(self) -> None:
        from common.scoring import build_normalizer, compute_wer

        refs = ["engine 41 copy", "battalion 2 responding"]
        result = compute_wer(refs, refs, normalizer=build_normalizer())
        self.assertEqual(result["wer"], 0)

    def test_verbatim_policy_is_case_sensitive(self) -> None:
        from common.scoring import compute_wer

        result = compute_wer(["Engine 41"], ["engine 41"], normalizer=None)
        self.assertGreater(result["wer"], 0)

    def test_normalized_policy_is_case_insensitive(self) -> None:
        from common.scoring import build_normalizer, compute_wer

        # After NeMo normalization both sides are lowercased; "41" normalizes
        # identically on both sides — so WER must be 0
        result = compute_wer(
            ["Engine 41"], ["engine 41"], normalizer=build_normalizer()
        )
        self.assertEqual(result["wer"], 0)

    def test_compute_wer_result_has_breakdown_keys(self) -> None:
        from common.scoring import compute_wer

        result = compute_wer(["engine 41"], ["engine 41"], normalizer=None)
        for key in ("wer", "insertions", "deletions", "substitutions", "hits"):
            self.assertIn(key, result)


@_scoring_required
class TestComputeCer(unittest.TestCase):
    def test_identical_lists_score_zero_cer(self) -> None:
        from common.scoring import compute_cer

        result = compute_cer(["engine 41"], ["engine 41"])
        self.assertEqual(result["cer"], 0)

    def test_cer_result_has_cer_key(self) -> None:
        from common.scoring import compute_cer

        result = compute_cer(["engine 41"], ["engine 41"])
        self.assertIn("cer", result)


class TestHallucinationRate(unittest.TestCase):
    def test_empty_list_returns_zero(self) -> None:
        from common.scoring import hallucination_rate

        self.assertEqual(hallucination_rate([]), 0.0)

    def test_flags_empty_string(self) -> None:
        from common.scoring import hallucination_rate

        self.assertEqual(hallucination_rate([""]), 100.0)

    def test_flags_unintelligible_token(self) -> None:
        from common.scoring import hallucination_rate

        self.assertEqual(hallucination_rate(["[UNINTELLIGIBLE]"]), 100.0)

    def test_mixed_list_partial_rate(self) -> None:
        from common.scoring import hallucination_rate

        self.assertEqual(
            hallucination_rate(["engine 41", "", "copy", ""]), 50.0
        )


class TestKeywordMetrics(unittest.TestCase):
    def test_absent_keyword_omitted(self) -> None:
        from common.scoring import keyword_metrics

        result = keyword_metrics(["engine 41"], ["engine 41"], ["battalion"])
        self.assertEqual(result, [])

    def test_present_keyword_returns_accuracy_dict(self) -> None:
        from common.scoring import keyword_metrics

        result = keyword_metrics(
            ["engine 41 copy"], ["engine 41 copy"], ["engine"]
        )
        self.assertEqual(len(result), 1)
        self.assertIn("keyword", result[0])
        self.assertIn("occurrences", result[0])
        self.assertIn("correctly_identified", result[0])
        self.assertIn("accuracy", result[0])

    def test_count_keyword_occurrences_whole_word_only(self) -> None:
        from common.scoring import count_keyword_occurrences

        self.assertEqual(count_keyword_occurrences("copy", "copy that copy"), 2)
        self.assertEqual(count_keyword_occurrences("copy", "copycat"), 0)

    def test_keyword_metrics_length_mismatch_raises(self) -> None:
        from common.scoring import keyword_metrics

        with self.assertRaises(ValueError):
            keyword_metrics(["a"], ["a", "b"], ["a"])


@_scoring_required
class TestDurationBucketWer(unittest.TestCase):
    """duration_bucket_wer — duration bucketing, exclusion rules, WER/SER."""

    def test_length_mismatch_raises(self) -> None:
        from common.scoring import duration_bucket_wer

        with self.assertRaises(ValueError):
            duration_bucket_wer(["a"], ["a"], [1.0, 2.0])

    def test_buckets_by_duration_excluding_long_and_negative(self) -> None:
        from common.scoring import duration_bucket_wer

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
        from common.scoring import duration_bucket_wer

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
        from common.scoring import duration_bucket_wer

        result = duration_bucket_wer(
            ["a b c"], ["a b c"], [1.0], normalizer=None
        )
        self.assertEqual(result[0]["wer"], 0)
        self.assertEqual(result[0]["ser"], 0.0)

    def test_ser_is_fraction_of_segments_with_errors(self) -> None:
        from common.scoring import duration_bucket_wer

        # One perfect segment, one wrong, both Short — SER must be 50%.
        refs = ["a b c", "d e f"]
        hyps = ["a b c", "x y z"]
        result = duration_bucket_wer(refs, hyps, [1.0, 1.0], normalizer=None)
        self.assertEqual(result[0]["count"], 2)
        self.assertEqual(result[0]["ser"], 50.0)
        self.assertGreater(result[0]["wer"], 0)

    def test_result_dicts_have_expected_keys(self) -> None:
        from common.scoring import duration_bucket_wer

        result = duration_bucket_wer(["a"], ["a"], [1.0], normalizer=None)
        for key in ("bucket", "wer", "ser", "count"):
            self.assertIn(key, result[0])


@_scoring_required
class TestBootstrapPaired(unittest.TestCase):
    """bootstrap_paired — input validation, determinism, and result shape."""

    def test_length_mismatch_raises(self) -> None:
        from common.scoring import bootstrap_paired

        with self.assertRaises(ValueError):
            bootstrap_paired(["a", "b"], ["a"], ["a", "b"])

    def test_empty_eval_set_raises(self) -> None:
        from common.scoring import bootstrap_paired

        with self.assertRaises(ValueError):
            bootstrap_paired([], [], [])

    def test_identical_systems_zero_delta_and_p_value_one(self) -> None:
        from common.scoring import bootstrap_paired

        refs = ["engine one", "battalion two"]
        result = bootstrap_paired(
            refs, refs, refs, normalizer=None, n_resamples=200
        )
        self.assertEqual(result["delta"], 0)
        self.assertEqual(result["p_value_one_sided"], 1.0)

    def test_result_uses_p_value_one_sided_key(self) -> None:
        from common.scoring import bootstrap_paired

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
        from common.scoring import bootstrap_paired

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
        from common.scoring import bootstrap_paired

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
