"""Tests for Gemini SFT evaluation reporting."""

from __future__ import annotations

import unittest

from common import scoring
from gemini_sft import reporting

try:
    import jiwer  # noqa: F401
    import nemo_text_processing  # noqa: F401

    _SCORING_AVAILABLE = True
except ImportError:
    _SCORING_AVAILABLE = False

_scoring_required = unittest.skipIf(
    not _SCORING_AVAILABLE,
    "requires the [scoring] extra (jiwer + nemo_text_processing)",
)


@_scoring_required
class TestReportingContract(unittest.TestCase):
    def test_target_metrics_use_canonical_public_keys(self) -> None:
        refs = ["engine copy", "brush"]
        hyps = ["engine copy", ""]
        target = reporting.build_target_metrics(
            label="base",
            model="gemini-3.1-flash-lite",
            refs=refs,
            hyps=hyps,
            normalizer=None,
            keywords=["engine", "copy", "brush"],
            missing_prediction_count=1,
            artifacts=reporting.ReportArtifacts(
                raw_output_uri="gs://bucket/raw/",
                normalized_manifest_uri="gs://bucket/normalized.jsonl",
                summary_json_uri="gs://bucket/evals/wer_summary.json",
                summary_markdown_uri="gs://bucket/evals/wer_summary.md",
            ),
        )
        report = reporting.EvalReport(
            round_id="round-a",
            generated_at="2026-06-28T00:00:00Z",
            target=target,
        )

        row = reporting.report_to_dict(report)["target"]
        for key in (
            "empty_or_unintelligible_rate",
            "insertions",
            "deletions",
            "substitutions",
            "total_reference_words",
            "missing_prediction_count",
        ):
            self.assertIn(key, row)
        self.assertEqual(row["missing_prediction_count"], 1)
        self.assertEqual(row["empty_or_unintelligible_rate"], 50.0)
        self.assertEqual(
            row["artifacts"]["normalized_manifest_uri"],
            "gs://bucket/normalized.jsonl",
        )
        self.assertEqual(
            row["artifacts"]["summary_json_uri"],
            "gs://bucket/evals/wer_summary.json",
        )
        self.assertEqual(
            row["artifacts"]["summary_markdown_uri"],
            "gs://bucket/evals/wer_summary.md",
        )

    def test_total_reference_words_matches_wer_denominator(self) -> None:
        refs = ["engine copy", "brush"]
        hyps = ["engine copy", ""]
        target = reporting.build_target_metrics(
            label="base",
            model="gemini-3.1-flash-lite",
            refs=refs,
            hyps=hyps,
            normalizer=None,
            keywords=[],
        )
        wer = scoring.compute_wer(refs, hyps, normalizer=None)
        expected_total = (
            int(wer["hits"]) + int(wer["substitutions"]) + int(wer["deletions"])
        )

        self.assertEqual(target.total_reference_words, expected_total)

    def test_empty_or_unintelligible_metric_counts_scored_empty_hypotheses(
        self,
    ) -> None:
        target = reporting.build_target_metrics(
            label="base",
            model="gemini-3.1-flash-lite",
            refs=["copy", "engine", "brush"],
            hyps=["", "[UNINTELLIGIBLE]", ""],
            normalizer=None,
            keywords=[],
            missing_prediction_count=1,
        )

        self.assertEqual(target.empty_or_unintelligible_rate, 100.0)

    def test_markdown_and_console_share_target_header(self) -> None:
        report = reporting.EvalReport(
            round_id="round-a",
            generated_at="2026-06-28T00:00:00Z",
            target=reporting.build_target_metrics(
                label="base",
                model="gemini-3.1-flash-lite",
                refs=["engine copy"],
                hyps=["engine copy"],
                normalizer=None,
                keywords=["engine"],
            ),
        )
        expected_header = (
            "empty_or_unintelligible_rate | insertions | "
            "deletions | substitutions | "
            "total_reference_words | missing_prediction_count"
        )

        self.assertIn(expected_header, reporting.render_markdown_report(report))
        self.assertIn(expected_header, reporting.render_console_report(report))

    def test_markdown_cells_escape_pipes_and_fold_newlines(self) -> None:
        target = reporting.TargetMetrics(
            target_label="base",
            model="gemini|experimental\ncheckpoint",
            wer=0.0,
            cer=0.0,
            keyword_accuracy=None,
            empty_or_unintelligible_rate=0.0,
            insertions=0,
            deletions=0,
            substitutions=0,
            total_reference_words=1,
            missing_prediction_count=0,
            artifacts=reporting.ReportArtifacts(
                raw_output_uri="gs://bucket/raw|output\npart"
            ),
        )

        report = reporting.EvalReport(
            round_id="round-a",
            generated_at="2026-06-28T00:00:00Z",
            target=target,
        )

        rendered = reporting.render_console_report(report)

        self.assertEqual(len(rendered.splitlines()), 3)
        self.assertIn("gemini\\|experimental checkpoint", rendered)
        self.assertIn("gs://bucket/raw\\|output part", rendered)


if __name__ == "__main__":
    unittest.main()
