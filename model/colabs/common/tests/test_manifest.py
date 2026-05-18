"""Tests for common.manifest.

Covers:
  - merge_predictions_to_manifest: fail-loud (re-raise) on unexpected error
  - merge_predictions_to_manifest: happy-path offset-tolerant merge
  - load_manifest: [] returns for missing/malformed input (unchanged)
"""

import unittest


class TestMergePredictionsToManifestFailLoud(unittest.TestCase):
    """merge_predictions_to_manifest must raise on unexpected error, never return []."""

    def test_raises_on_malformed_prediction_offset(self) -> None:
        """A prediction whose offset cannot be cast to float raises ValueError.

        The internal ``float(pred.get("offset", 0.0))`` call throws when the
        offset is a non-numeric string.  The function must propagate the
        exception rather than swallowing it and returning [].
        """
        from common.manifest import merge_predictions_to_manifest

        ground_truth = [
            {"audio_filepath": "gs://bucket/clip.flac", "offset": 0.0}
        ]
        # Non-numeric offset triggers float() ValueError inside the try block
        bad_predictions = [
            {
                "audio_filepath": "gs://bucket/clip.flac",
                "offset": "not-a-number",
                "text": "hi",
            }
        ]

        with self.assertRaises(ValueError):
            merge_predictions_to_manifest(
                ground_truth, bad_predictions, "gemini"
            )

    def test_does_not_return_empty_list_on_error(self) -> None:
        """Verify the old silent-failure path (return []) is gone."""
        from common.manifest import merge_predictions_to_manifest

        ground_truth = [
            {"audio_filepath": "gs://bucket/clip.flac", "offset": 0.0}
        ]
        bad_predictions = [
            {
                "audio_filepath": "gs://bucket/clip.flac",
                "offset": "not-a-number",
                "text": "hi",
            }
        ]

        result = None
        raised = False
        try:
            result = merge_predictions_to_manifest(
                ground_truth, bad_predictions, "gemini"
            )
        except Exception:
            raised = True

        self.assertTrue(raised, "Expected an exception but none was raised")
        self.assertIsNone(
            result, "Function must not return a value when it raises"
        )


class TestMergePredictionsHappyPath(unittest.TestCase):
    """Sanity-check the normal merge path is unaffected by the re-raise change."""

    def test_matched_prediction_written_to_gt_row(self) -> None:
        from common.manifest import merge_predictions_to_manifest

        gt = [
            {"audio_filepath": "gs://b/a.flac", "offset": 1.0, "text": "gold"}
        ]
        preds = [
            {
                "audio_filepath": "gs://b/a.flac",
                "offset": 1.05,
                "text": "predicted",
            }
        ]

        result = merge_predictions_to_manifest(gt, preds, "whisper")

        self.assertEqual(result[0]["pred_text_whisper"], "predicted")

    def test_unmatched_prediction_leaves_field_absent(self) -> None:
        from common.manifest import merge_predictions_to_manifest

        gt = [
            {"audio_filepath": "gs://b/a.flac", "offset": 1.0, "text": "gold"}
        ]
        preds = [
            {
                "audio_filepath": "gs://b/a.flac",
                "offset": 9.0,
                "text": "far away",
            }
        ]

        result = merge_predictions_to_manifest(gt, preds, "whisper")

        self.assertNotIn("pred_text_whisper", result[0])

    def test_returns_same_list_object(self) -> None:
        """Result is the ground_truth list mutated in place."""
        from common.manifest import merge_predictions_to_manifest

        gt = [{"audio_filepath": "gs://b/a.flac", "offset": 0.0, "text": "x"}]
        result = merge_predictions_to_manifest(gt, [], "m")

        self.assertIs(result, gt)


class TestLoadManifestEmptyReturns(unittest.TestCase):
    """load_manifest still returns [] for bad inputs — these paths are intentional."""

    def test_missing_file_returns_empty(self) -> None:
        from common.manifest import load_manifest

        result = load_manifest("/nonexistent/path/manifest.jsonl")

        self.assertEqual(result, [])
