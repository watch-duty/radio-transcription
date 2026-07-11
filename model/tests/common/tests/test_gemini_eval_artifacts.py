"""Tests for Gemini evaluation artifact paths."""

import unittest

from common.gemini import eval_artifacts


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


if __name__ == "__main__":
    unittest.main()
