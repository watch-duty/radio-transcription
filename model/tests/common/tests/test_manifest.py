"""Tests for common.manifest.

Covers:
  - merge_predictions_to_manifest: fail-loud (re-raise) on unexpected error
  - merge_predictions_to_manifest: happy-path offset-tolerant merge
  - load_manifest: [] on missing input; non-string text fields coerced
"""

import json
import os
import tempfile
import unittest
from pathlib import Path

from common.manifest import (
    is_scoreable_manifest_entry,
    load_manifest,
    merge_predictions_to_manifest,
    rows_from_manifest,
)


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

    def test_prediction_missing_audio_filepath_raises(self) -> None:
        """A prediction without `audio_filepath` fails loud, not silently merges to ""."""
        gt = [{"audio_filepath": "gs://b/a.flac", "offset": 1.0, "text": "g"}]
        preds = [{"offset": 1.0, "text": "p"}]  # no audio_filepath
        with self.assertRaises(ValueError):
            merge_predictions_to_manifest(gt, preds, "m")

    def test_prediction_missing_offset_raises(self) -> None:
        """A prediction without `offset` fails loud, not silently defaults to 0.0."""
        gt = [{"audio_filepath": "gs://b/a.flac", "offset": 1.0, "text": "g"}]
        preds = [{"audio_filepath": "gs://b/a.flac", "text": "p"}]  # no offset
        with self.assertRaises(ValueError):
            merge_predictions_to_manifest(gt, preds, "m")

    def test_raises_on_missing_ground_truth_offset(self) -> None:
        """A GT row missing 'offset' raises — symmetric to the predictions side.

        Silently defaulting to 0.0 would let a malformed manifest bind every
        row missing an offset to whichever prediction sits at 0.0.
        """
        gt = [{"audio_filepath": "gs://b/a.flac"}]  # no 'offset' key
        with self.assertRaises(ValueError):
            merge_predictions_to_manifest(gt, [], "gemini")

    def test_raises_on_missing_ground_truth_audio_filepath(self) -> None:
        """A GT row missing 'audio_filepath' raises — symmetric to predictions."""
        gt = [{"offset": 1.0}]  # no 'audio_filepath' key
        with self.assertRaises(ValueError):
            merge_predictions_to_manifest(gt, [], "gemini")

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

    def test_prediction_with_null_text_does_not_become_literal_none(
        self,
    ) -> None:
        """A prediction whose `text` is None coerces to '' (absent), not 'None'.

        The naive ``str(None)`` is the four-letter word "None", which would
        otherwise score as a real-looking prediction token against the ground
        truth. Mirror load_manifest's None-to-empty coercion.
        """
        gt = [
            {"audio_filepath": "gs://b/a.flac", "offset": 1.0, "text": "gold"}
        ]
        preds = [
            {"audio_filepath": "gs://b/a.flac", "offset": 1.0, "text": None}
        ]

        result = merge_predictions_to_manifest(gt, preds, "m")

        self.assertEqual(result[0]["pred_text_m"], "")

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

    def test_stale_pred_text_field_is_cleared_on_rerun(self) -> None:
        """Re-running clears a stale pred_text_{model_key} when no new prediction matches.

        Without this, a re-run that fails to produce a prediction for some
        row leaves the OLD prediction in place — and downstream WER scores
        the missing output as a successful prediction.
        """
        gt = [
            {"audio_filepath": "gs://b/a.flac", "offset": 1.0, "text": "g1"},
            {"audio_filepath": "gs://b/a.flac", "offset": 9.0, "text": "g2"},
        ]
        # First merge: both rows get predictions.
        preds_v1 = [
            {"audio_filepath": "gs://b/a.flac", "offset": 1.0, "text": "p1"},
            {"audio_filepath": "gs://b/a.flac", "offset": 9.0, "text": "p2"},
        ]
        merge_predictions_to_manifest(gt, preds_v1, "m")
        self.assertEqual(gt[0]["pred_text_m"], "p1")
        self.assertEqual(gt[1]["pred_text_m"], "p2")

        # Re-run with a missing prediction for row 1: stale value must clear.
        preds_v2 = [
            {
                "audio_filepath": "gs://b/a.flac",
                "offset": 1.0,
                "text": "p1_new",
            },
        ]
        merge_predictions_to_manifest(gt, preds_v2, "m")
        self.assertEqual(gt[0]["pred_text_m"], "p1_new")
        self.assertNotIn(
            "pred_text_m",
            gt[1],
            "stale prediction must be cleared on re-run",
        )

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
        fd, path = tempfile.mkstemp(suffix=".jsonl")
        try:
            with os.fdopen(fd, "w") as f:
                f.write(
                    json.dumps({"audio_filepath": "gs://b/a.flac", "text": 123})
                )
            rows = load_manifest(path)
        finally:
            Path(path).unlink()

        self.assertEqual(rows[0]["text"], "123")

    def test_falsy_non_string_text_is_coerced(self) -> None:
        """Falsy non-string text (0, False, None) is coerced to a string."""
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
            Path(path).unlink()

        # 0 / False are str()-cast; a null text becomes "" (absent transcript).
        self.assertEqual([r["text"] for r in rows], ["0", "False", ""])
        for row in rows:
            self.assertIsInstance(row["text"], str)


class TestRowsFromManifestNullSafe(unittest.TestCase):
    """rows_from_manifest tolerates explicit null offset/duration (no TypeError)."""

    def test_explicit_null_offset_duration_default_to_zero(self) -> None:
        rows = rows_from_manifest(
            [
                {
                    "audio_filepath": "gs://b/a.flac",
                    "text": "hello",
                    "offset": None,
                    "duration": None,
                }
            ]
        )
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].offset, 0.0)
        self.assertEqual(rows[0].duration, 0.0)


class TestScoreableManifestEntry(unittest.TestCase):
    def test_requires_audio_filepath_and_non_empty_text(self) -> None:
        self.assertTrue(
            is_scoreable_manifest_entry(
                {"audio_filepath": "gs://b/a.flac", "text": "hello"}
            )
        )
        self.assertFalse(
            is_scoreable_manifest_entry(
                {"audio_filepath": "gs://b/a.flac", "text": ""}
            )
        )
        self.assertFalse(is_scoreable_manifest_entry({"text": "hello"}))
