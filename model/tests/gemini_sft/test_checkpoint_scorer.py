from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path

from fake_gcs import FakeStorageClient

sys.path.append(str(Path(__file__).resolve().parents[2] / "scripts" / "sft"))
import score_gemini_sft_checkpoints_online as scorer  # noqa: E402


class TestCheckpointScorerSummary(unittest.TestCase):
    def test_score_predictions_uses_canonical_report_metrics(
        self,
    ) -> None:
        score = scorer.score_predictions(
            label="checkpoint_1",
            model="projects/p/locations/us-central1/endpoints/1",
            refs=["engine copy", "brush", "medic"],
            hyps=["engine copy", "", "[UNINTELLIGIBLE]"],
            normalizer=None,
            missing_prediction_count=1,
            artifacts=scorer.ReportArtifacts(
                online_predictions_uri="gs://bucket/run/checkpoint_1.jsonl"
            ),
            keywords=["engine", "copy", "brush", "medic"],
        )
        report = scorer.EvalReport(
            round_id="round-a",
            generated_at="2026-06-28T00:00:00Z",
            targets=[score],
        )
        row = scorer.report_to_dict(report)["targets"][0]

        self.assertEqual(row["empty_or_unintelligible_rate"], 66.67)
        self.assertEqual(row["empty_response_rate"], 33.33)
        self.assertAlmostEqual(row["keyword_accuracy"], 50.0)
        self.assertIsInstance(row["insertions"], int)
        self.assertIsInstance(row["deletions"], int)
        self.assertIsInstance(row["substitutions"], int)
        self.assertEqual(row["total_reference_words"], 4)
        self.assertEqual(row["missing_prediction_count"], 1)
        self.assertNotIn("empty_rate", row)
        self.assertNotIn("hallucination_rate", row)
        self.assertEqual(
            row["artifacts"]["online_predictions_uri"],
            "gs://bucket/run/checkpoint_1.jsonl",
        )
        self.assertEqual(
            row["keyword_metrics"],
            [
                {
                    "keyword": "engine",
                    "occurrences": 1,
                    "correctly_identified": 1,
                    "accuracy": 100.0,
                },
                {
                    "keyword": "copy",
                    "occurrences": 1,
                    "correctly_identified": 1,
                    "accuracy": 100.0,
                },
                {
                    "keyword": "brush",
                    "occurrences": 1,
                    "correctly_identified": 0,
                    "accuracy": 0.0,
                },
                {
                    "keyword": "medic",
                    "occurrences": 1,
                    "correctly_identified": 0,
                    "accuracy": 0.0,
                },
            ],
        )

    def test_write_summary_uses_shared_report_schema(self) -> None:
        storage = FakeStorageClient()
        base = scorer.score_predictions(
            label="base",
            model="gemini-3.1-flash-lite",
            refs=["engine copy"],
            hyps=["engine copy"],
            normalizer=None,
            artifacts=scorer.ReportArtifacts(
                raw_output_uri="gs://bucket/run/evals/base/output/"
            ),
            keywords=["engine", "copy"],
        )
        checkpoint = scorer.score_predictions(
            label="checkpoint_7",
            model="projects/p/locations/us-central1/endpoints/7",
            refs=["engine copy"],
            hyps=[""],
            normalizer=None,
            missing_prediction_count=0,
            artifacts=scorer.ReportArtifacts(
                online_predictions_uri=(
                    "gs://bucket/run/evals/checkpoints/checkpoint_7/"
                    "online_predictions.jsonl"
                )
            ),
            metadata={"checkpoint_id": "7", "epoch": "8", "step": "2961"},
            keywords=["engine", "copy"],
        )
        report = scorer.EvalReport(
            round_id="round-a",
            generated_at="2026-06-28T00:00:00Z",
            targets=[base, checkpoint],
            metadata={"job_name": "projects/p/locations/us/jobs/job-a"},
        )
        checkpoint_rankings = [
            {
                "target_label": "checkpoint_7",
                "checkpoint_id": "7",
                "epoch": "8",
                "step": "2961",
                "delta_vs_base": round(checkpoint.wer - base.wer, 2),
                "online_predictions_uri": (
                    "gs://bucket/run/evals/checkpoints/checkpoint_7/"
                    "online_predictions.jsonl"
                ),
            }
        ]
        with tempfile.TemporaryDirectory() as tmp:
            payload = scorer.write_summary(
                local_dir=Path(tmp),
                storage_client=storage,
                run_gcs_prefix="gs://bucket/run",
                report=report,
                checkpoint_rankings=checkpoint_rankings,
            )
            markdown = (Path(tmp) / "checkpoint_score_summary.md").read_text(
                encoding="utf-8"
            )
            summary_json = json.loads(
                (Path(tmp) / "checkpoint_score_summary.json").read_text(
                    encoding="utf-8"
                )
            )

        self.assertIn(
            "empty_or_unintelligible_rate | empty_response_rate | "
            "insertions | deletions | substitutions | total_reference_words | "
            "missing_prediction_count",
            markdown,
        )
        self.assertNotIn("hallucination_rate |", markdown)
        self.assertNotIn("empty_rate |", markdown)
        self.assertEqual(payload["best_checkpoint"]["checkpoint_id"], "7")
        self.assertEqual(summary_json["best_checkpoint"]["checkpoint_id"], "7")
        self.assertIn(
            "online_predictions_uri",
            summary_json["targets"][1]["artifacts"],
        )
        self.assertEqual(
            summary_json["targets"][0]["artifacts"]["raw_output_uri"],
            "gs://bucket/run/evals/base/output/",
        )
        self.assertEqual(
            storage.get(
                "gs://bucket/run/evals/checkpoints/"
                "checkpoint_score_summary.md"
            ),
            markdown,
        )


if __name__ == "__main__":
    unittest.main()
