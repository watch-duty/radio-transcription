"""Tests for common.manifest.

Covers:
  - merge_predictions_to_manifest: fail-loud (re-raise) on unexpected error
  - merge_predictions_to_manifest: happy-path offset-tolerant merge
  - load_manifest: [] on missing input; non-string text fields coerced
"""

import unittest

from common.manifest import load_manifest, merge_predictions_to_manifest


class TestMergePredictionsToManifestFailLoud(unittest.TestCase):
    """merge_predictions_to_manifest must raise on unexpected error, never return []."""

    def test_raises_on_malformed_prediction_offset(self) -> None:
        """A prediction whose offset cannot be cast to float raises ValueError.

        The internal ``float(pred.get("offset", 0.0))`` call throws when the
        offset is a non-numeric string.  The function must propagate the
        exception rather than swallowing it and returning [].
        """
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

    def test_binds_closest_of_multiple_in_tolerance_candidates(self) -> None:
        """When several predictions are within tolerance, the nearest wins."""
        gt = [
            {"audio_filepath": "gs://b/a.flac", "offset": 1.15, "text": "gold"}
        ]
        # 1.0 and 1.1 are both within the default 0.25s tolerance of 1.15;
        # 1.1 (diff 0.05) is nearer than 1.0 (diff 0.15), so "closer" must win
        # even though "first" appears earlier in the candidate list.
        preds = [
            {"audio_filepath": "gs://b/a.flac", "offset": 1.0, "text": "first"},
            {
                "audio_filepath": "gs://b/a.flac",
                "offset": 1.1,
                "text": "closer",
            },
        ]

        result = merge_predictions_to_manifest(gt, preds, "whisper")

        self.assertEqual(result[0]["pred_text_whisper"], "closer")

    def test_one_prediction_is_not_assigned_to_two_rows(self) -> None:
        """A single prediction near two rows binds to only the nearer one."""
        # Two GT segments 0.2 s apart; only one prediction survived (the
        # other model output was lost). 1.18 is within 0.25 s of BOTH
        # rows but must bind to exactly one, leaving the other blank so
        # WER still counts the missing output as an error.
        gt = [
            {"audio_filepath": "gs://b/a.flac", "offset": 1.0, "text": "g1"},
            {"audio_filepath": "gs://b/a.flac", "offset": 1.2, "text": "g2"},
        ]
        preds = [
            {"audio_filepath": "gs://b/a.flac", "offset": 1.18, "text": "only"},
        ]

        result = merge_predictions_to_manifest(gt, preds, "whisper")

        # 1.18 is nearer 1.2 than 1.0, so row 1 binds it; row 0 stays blank.
        self.assertNotIn("pred_text_whisper", result[0])
        self.assertEqual(result[1]["pred_text_whisper"], "only")

    def test_unmatched_prediction_leaves_field_absent(self) -> None:
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
        gt = [{"audio_filepath": "gs://b/a.flac", "offset": 0.0, "text": "x"}]
        result = merge_predictions_to_manifest(gt, [], "m")

        self.assertIs(result, gt)


class TestLoadManifestEmptyReturns(unittest.TestCase):
    """load_manifest still returns [] for bad inputs — these paths are intentional."""

    def test_missing_file_returns_empty(self) -> None:
        result = load_manifest("./nonexistent_manifest.jsonl")

        self.assertEqual(result, [])


class TestLoadManifestMalformedRows(unittest.TestCase):
    """load_manifest tolerates rows whose `text` field is not a string."""

    def test_non_string_text_is_coerced(self) -> None:
        """A row with a non-string `text` is str()-cast, not crashed."""
        import json
        import os
        import tempfile

        fd, path = tempfile.mkstemp(suffix=".jsonl")
        try:
            with os.fdopen(fd, "w") as f:
                f.write(
                    json.dumps({"audio_filepath": "gs://b/a.flac", "text": 123})
                )
            rows = load_manifest(path)
        finally:
            os.unlink(path)

        self.assertEqual(rows[0]["text"], "123")

    def test_falsy_non_string_text_is_coerced(self) -> None:
        """Falsy non-string text (0, False, None) is coerced to a string."""
        import json
        import os
        import tempfile

        rows_in = [
            {"audio_filepath": "gs://b/a.flac", "text": 0},
            {"audio_filepath": "gs://b/b.flac", "text": False},
            {"audio_filepath": "gs://b/c.flac", "text": None},
        ]
        fd, path = tempfile.mkstemp(suffix=".jsonl")
        try:
            with os.fdopen(fd, "w") as f:
                f.write("\n".join(json.dumps(row) for row in rows_in))
            rows = load_manifest(path)
        finally:
            os.unlink(path)

        # 0 / False are str()-cast; a null text becomes "" (absent transcript).
        self.assertEqual([r["text"] for r in rows], ["0", "False", ""])
        for row in rows:
            self.assertIsInstance(row["text"], str)
