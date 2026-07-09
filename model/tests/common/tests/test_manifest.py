"""Tests for common.manifest.

Covers:
  - merge_predictions_to_manifest: fail-loud (re-raise) on unexpected error
  - merge_predictions_to_manifest: happy-path offset-tolerant merge
  - load_manifest: fail-loud JSON/JSONL parsing
"""

import json
import os
import tempfile
import unittest
from pathlib import Path

from common.manifest import (
    CanonicalManifestIssue,
    CanonicalRow,
    canonical_row_identity,
    is_scoreable_manifest_entry,
    load_manifest,
    merge_predictions_to_manifest,
    require_canonical_manifest,
    rows_from_manifest,
    validate_canonical_manifest,
)


def _canonical_row(**overrides: object) -> dict[str, object]:
    row: dict[str, object] = {
        "audio_filepath": "gs://bucket/audio/example.flac",
        "text": "dispatch transcript",
        "offset": 0.0,
        "duration": 1.25,
        "example_id": "example",
        "segment_id": "001",
        "split": "train",
        "dataset": {"name": "echo", "family": "radio"},
        "source_audio": {
            "audio_filepath": "gs://bucket/source/example.mp3",
            "offset": 12.5,
            "duration": 1.25,
        },
    }
    row.update(overrides)
    return row


class TestCanonicalManifestValidation(unittest.TestCase):
    """Strict Canonical Manifest validation covers the public contract."""

    def assertHasIssue(
        self,
        issues: list[CanonicalManifestIssue],
        code: str,
        field: str,
    ) -> None:
        self.assertTrue(
            any(
                issue.code == code and issue.field == field for issue in issues
            ),
            f"Missing {code}/{field} in {issues!r}",
        )

    def test_valid_row_with_optional_metadata_returns_no_issues(self) -> None:
        row = _canonical_row(
            audio_filepath="gs://bucket/audio/example.FLAC",
        )

        issues = validate_canonical_manifest([row], expected_split="train")

        self.assertEqual(issues, [])

    def test_valid_core_row_without_optional_metadata_returns_no_issues(
        self,
    ) -> None:
        row = _canonical_row()
        row.pop("split")
        row.pop("dataset")
        row.pop("source_audio")

        issues = validate_canonical_manifest([row], expected_split="train")

        self.assertEqual(issues, [])

    def test_empty_manifest_is_invalid(self) -> None:
        issues = validate_canonical_manifest([])

        self.assertTrue(any(issue.code == "empty_manifest" for issue in issues))
        with self.assertRaisesRegex(ValueError, "empty_manifest"):
            require_canonical_manifest([])

    def test_required_field_failures_report_code_and_field(self) -> None:
        cases = [
            (
                "missing audio_filepath",
                "audio_filepath",
                None,
                "missing_required",
            ),
            ("blank audio_filepath", "audio_filepath", " ", "blank_required"),
            ("missing text", "text", None, "missing_required"),
            ("blank text", "text", " ", "blank_required"),
            ("missing example_id", "example_id", None, "missing_required"),
            ("blank example_id", "example_id", " ", "blank_required"),
            ("missing segment_id", "segment_id", None, "missing_required"),
            ("blank segment_id", "segment_id", " ", "blank_required"),
            ("missing offset", "offset", None, "missing_required"),
            ("negative offset", "offset", -1, "invalid_offset"),
            ("non-numeric offset", "offset", "start", "invalid_offset"),
            ("bool offset", "offset", True, "invalid_offset"),
            ("missing duration", "duration", None, "missing_required"),
            ("zero duration", "duration", 0, "invalid_duration"),
            ("non-numeric duration", "duration", "short", "invalid_duration"),
            ("bool duration", "duration", True, "invalid_duration"),
        ]

        for name, field, value, expected_code in cases:
            with self.subTest(name=name):
                row = _canonical_row()
                if value is None:
                    row.pop(field)
                else:
                    row[field] = value

                issues = validate_canonical_manifest(
                    [row], expected_split="train"
                )

                self.assertHasIssue(issues, expected_code, field)

    def test_audio_uri_and_duplicate_failures_report_code_and_field(
        self,
    ) -> None:
        cases = [
            ("s3://bucket/audio/example.flac", "non-GCS"),
            ("gs://bucket/audio/example.wav", "non-FLAC"),
        ]

        for audio_filepath, name in cases:
            with self.subTest(name=name):
                issues = validate_canonical_manifest(
                    [_canonical_row(audio_filepath=audio_filepath)]
                )

                self.assertHasIssue(
                    issues,
                    "invalid_audio_uri",
                    "audio_filepath",
                )

        duplicate_audio_issues = validate_canonical_manifest(
            [
                _canonical_row(
                    audio_filepath="gs://bucket/audio/dup.flac",
                    example_id="example-a",
                    segment_id="001",
                ),
                _canonical_row(
                    audio_filepath="gs://bucket/audio/dup.flac",
                    example_id="example-b",
                    segment_id="001",
                ),
            ]
        )
        self.assertHasIssue(
            duplicate_audio_issues,
            "duplicate_audio_filepath",
            "audio_filepath",
        )

        duplicate_identity_issues = validate_canonical_manifest(
            [
                _canonical_row(
                    audio_filepath="gs://bucket/audio/a.flac",
                    example_id="shared",
                    segment_id="001",
                ),
                _canonical_row(
                    audio_filepath="gs://bucket/audio/b.flac",
                    example_id="shared",
                    segment_id="001",
                ),
            ]
        )
        self.assertHasIssue(
            duplicate_identity_issues,
            "duplicate_identity",
            "example_id,segment_id",
        )

    def test_optional_metadata_failures_report_code_and_field(self) -> None:
        cases = [
            ("blank split", {"split": " "}, "invalid_metadata", "split"),
            (
                "split mismatch",
                {"split": "eval"},
                "split_mismatch",
                "split",
            ),
            (
                "malformed dataset",
                {"dataset": []},
                "invalid_metadata",
                "dataset",
            ),
            (
                "blank dataset name",
                {"dataset": {"name": " ", "family": "radio"}},
                "invalid_metadata",
                "dataset.name",
            ),
            (
                "malformed source_audio",
                {"source_audio": []},
                "invalid_metadata",
                "source_audio",
            ),
            (
                "blank source audio filepath",
                {
                    "source_audio": {
                        "audio_filepath": " ",
                        "offset": 0,
                        "duration": 1.0,
                    }
                },
                "invalid_metadata",
                "source_audio.audio_filepath",
            ),
            (
                "invalid source audio offset",
                {
                    "source_audio": {
                        "audio_filepath": "gs://bucket/source/example.mp3",
                        "offset": "bad",
                        "duration": 1.0,
                    }
                },
                "invalid_metadata",
                "source_audio.offset",
            ),
        ]

        for name, overrides, expected_code, expected_field in cases:
            with self.subTest(name=name):
                issues = validate_canonical_manifest(
                    [_canonical_row(**overrides)],
                    expected_split="train",
                )

                self.assertHasIssue(
                    issues,
                    expected_code,
                    expected_field,
                )

    def test_explicit_null_optional_metadata_is_absent(self) -> None:
        issues = validate_canonical_manifest(
            [_canonical_row(split=None, dataset=None, source_audio=None)],
            expected_split="train",
        )

        self.assertEqual(issues, [])

    def test_unknown_prediction_and_removed_fields_are_tolerated(
        self,
    ) -> None:
        for field in (
            "pred_text_whisper",
            "unknown_future_field",
            "audio_processing",
            "sequence",
            "original_audio_uri",
            "source_group",
            "dataset_name",
            "lang",
        ):
            with self.subTest(field=field):
                issues = validate_canonical_manifest(
                    [_canonical_row(**{field: "not canonical"})]
                )
                self.assertEqual(issues, [])

    def test_unknown_nested_fields_are_tolerated(self) -> None:
        dataset_issues = validate_canonical_manifest(
            [
                _canonical_row(
                    dataset={
                        "name": "echo",
                        "family": "radio",
                        "extra": "not canonical",
                    }
                )
            ]
        )
        self.assertEqual(dataset_issues, [])

        source_issues = validate_canonical_manifest(
            [
                _canonical_row(
                    source_audio={
                        "audio_filepath": "gs://bucket/source/example.mp3",
                        "offset": 0,
                        "duration": 1.0,
                        "extra": "not canonical",
                    }
                )
            ]
        )
        self.assertEqual(source_issues, [])

    def test_invalid_rows_return_structured_issues(self) -> None:
        rows = [
            _canonical_row(
                audio_filepath="s3://bucket/audio/example.mp3",
                text=" ",
                offset=True,
                duration=0,
                split="eval",
                dataset={"name": " ", "family": 42},
                source_audio={
                    "audio_filepath": "",
                    "offset": False,
                    "duration": 0,
                },
                audio_processing={"masked_categories": ["ok", "", 42]},
            ),
            {
                "audio_filepath": "gs://bucket/audio/other.flac",
                "offset": 0,
                "duration": 1.0,
                "example_id": "example",
                "segment_id": "001",
                "split": "train",
                "dataset": {"name": "echo", "family": "radio"},
                "source_audio": {
                    "audio_filepath": "gs://bucket/source/other.mp3",
                    "offset": 0,
                    "duration": 1.0,
                },
            },
            _canonical_row(audio_filepath="gs://bucket/audio/other.flac"),
        ]

        issues = validate_canonical_manifest(rows, expected_split="train")
        codes = {issue.code for issue in issues}

        self.assertTrue(
            all(isinstance(issue, CanonicalManifestIssue) for issue in issues)
        )
        self.assertIn("missing_required", codes)
        self.assertIn("blank_required", codes)
        self.assertIn("invalid_audio_uri", codes)
        self.assertIn("invalid_duration", codes)
        self.assertIn("invalid_offset", codes)
        self.assertIn("duplicate_identity", codes)
        self.assertIn("duplicate_audio_filepath", codes)
        self.assertIn("split_mismatch", codes)
        self.assertIn("invalid_metadata", codes)
        self.assertTrue(
            any(
                issue.code == "missing_required"
                and issue.row_index == 1
                and issue.field == "text"
                for issue in issues
            )
        )
        self.assertTrue(
            any(
                issue.code == "duplicate_identity"
                and issue.row_index == 1
                and issue.field == "example_id,segment_id"
                for issue in issues
            )
        )
        self.assertTrue(
            any(
                issue.code == "duplicate_audio_filepath"
                and issue.row_index == 2
                and issue.field == "audio_filepath"
                for issue in issues
            )
        )

    def test_require_canonical_manifest_raises_aggregated_error(self) -> None:
        rows = [_canonical_row(text=" ", split="eval")]

        with self.assertRaisesRegex(ValueError, "blank_required") as ctx:
            require_canonical_manifest(rows, expected_split="train")

        message = str(ctx.exception)
        self.assertIn("split_mismatch", message)
        self.assertIn("row 0", message)
        self.assertIn("field text", message)

    def test_non_finite_timing_values_are_invalid(self) -> None:
        cases = [
            ("invalid_offset", "offset", float("nan")),
            ("invalid_offset", "offset", float("inf")),
            ("invalid_offset", "offset", float("-inf")),
            ("invalid_duration", "duration", float("nan")),
            ("invalid_duration", "duration", float("inf")),
            ("invalid_duration", "duration", float("-inf")),
        ]

        for expected_code, field, value in cases:
            with self.subTest(field=field, value=value):
                issues = validate_canonical_manifest(
                    [_canonical_row(**{field: value})]
                )

                self.assertTrue(
                    any(
                        issue.code == expected_code and issue.field == field
                        for issue in issues
                    )
                )

    def test_non_finite_source_audio_timing_is_invalid(self) -> None:
        cases = [
            ("source_audio.offset", float("nan")),
            ("source_audio.offset", float("inf")),
            ("source_audio.duration", float("nan")),
            ("source_audio.duration", float("inf")),
        ]

        for field, value in cases:
            parent, child = field.split(".")
            with self.subTest(field=field, value=value):
                issues = validate_canonical_manifest(
                    [
                        _canonical_row(
                            **{
                                parent: {
                                    "audio_filepath": "raw/source.wav",
                                    child: value,
                                }
                            }
                        )
                    ]
                )

                self.assertTrue(
                    any(
                        issue.code == "invalid_metadata"
                        and issue.field == field
                        for issue in issues
                    )
                )


class TestCanonicalRowIdentity(unittest.TestCase):
    """Row identity is public and stable for dict rows and CanonicalRow."""

    def test_identity_from_dict_and_canonical_row_is_stripped(self) -> None:
        dict_identity = canonical_row_identity(
            {"example_id": " example ", "segment_id": " 001 "}
        )
        typed_identity = canonical_row_identity(
            CanonicalRow(
                audio_filepath="gs://bucket/audio/example.flac",
                example_id="typed-example",
                segment_id="002",
                offset=0.0,
                duration=1.0,
                text="hello",
            )
        )

        self.assertEqual(dict_identity, ("example", "001"))
        self.assertEqual(typed_identity, ("typed-example", "002"))

    def test_identity_missing_or_blank_fields_raise(self) -> None:
        with self.assertRaisesRegex(ValueError, "segment_id"):
            canonical_row_identity({"example_id": "example"})
        with self.assertRaisesRegex(ValueError, "example_id"):
            canonical_row_identity({"example_id": " ", "segment_id": "001"})


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

    def test_prediction_without_identity_matches_exact_uri_and_offset(
        self,
    ) -> None:
        """Older predictions without identity still match by exact URI."""
        gt = [
            {
                "audio_filepath": "gs://b/a.flac",
                "offset": 1.0,
                "text": "gold",
                "example_id": "example",
                "segment_id": "001",
            }
        ]
        preds = [
            {
                "audio_filepath": "gs://b/a.flac",
                "offset": 1.0,
                "text": "legacy prediction",
            }
        ]

        result = merge_predictions_to_manifest(gt, preds, "legacy")

        self.assertEqual(
            result[0]["pred_text_legacy"],
            "legacy prediction",
        )

    def test_binds_closest_of_multiple_in_tolerance_candidates(self) -> None:
        """When several predictions are within tolerance, the nearest wins."""
        gt = [
            {
                "audio_filepath": "gs://b/a.flac",
                "offset": 1.15,
                "text": "gold",
            },
            {
                "audio_filepath": "gs://b/a.flac",
                "offset": 1.0,
                "text": "other",
            },
        ]
        # 1.0 and 1.1 are both within the default 0.25s tolerance of 1.15.
        # 1.1 is nearer than 1.0 for the first row, so "closer" wins there,
        # while "first" is still consumed by the exact-offset second row.
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
        self.assertEqual(result[1]["pred_text_whisper"], "first")

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

    def test_identity_disambiguates_predictions_with_same_audio_filepath(
        self,
    ) -> None:
        gt = [
            {
                "audio_filepath": "gs://b/shared.flac",
                "offset": 1.0,
                "text": "gold A",
                "example_id": "ex-a",
                "segment_id": "seg-a",
            },
            {
                "audio_filepath": "gs://b/shared.flac",
                "offset": 1.2,
                "text": "gold B",
                "example_id": "ex-b",
                "segment_id": "seg-b",
            },
        ]
        preds = [
            {
                "audio_filepath": "gs://b/shared.flac",
                "offset": 1.01,
                "text": "pred B",
                "example_id": "ex-b",
                "segment_id": "seg-b",
            },
            {
                "audio_filepath": "gs://b/shared.flac",
                "offset": 1.19,
                "text": "pred A",
                "example_id": "ex-a",
                "segment_id": "seg-a",
            },
        ]

        result = merge_predictions_to_manifest(gt, preds, "whisper")

        self.assertEqual(result[0]["pred_text_whisper"], "pred A")
        self.assertEqual(result[1]["pred_text_whisper"], "pred B")

    def test_matching_identity_with_different_audio_filepath_does_not_match(
        self,
    ) -> None:
        gt = [
            {
                "audio_filepath": "gs://b/a.flac",
                "offset": 1.0,
                "text": "gold",
                "example_id": "same",
                "segment_id": "001",
            }
        ]
        preds = [
            {
                "audio_filepath": "gs://b/b.flac",
                "offset": 1.0,
                "text": "predicted",
                "example_id": "same",
                "segment_id": "001",
            }
        ]

        with self.assertRaisesRegex(ValueError, "unmatched prediction"):
            merge_predictions_to_manifest(gt, preds, "whisper")

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

        with self.assertRaisesRegex(ValueError, "unmatched prediction"):
            merge_predictions_to_manifest(gt, preds, "whisper")

    def test_returns_same_list_object(self) -> None:
        """Result is the ground_truth list mutated in place."""
        gt = [{"audio_filepath": "gs://b/a.flac", "offset": 0.0, "text": "x"}]
        result = merge_predictions_to_manifest(gt, [], "m")

        self.assertIs(result, gt)


class TestLoadManifestFailLoud(unittest.TestCase):
    """load_manifest parses JSON/JSONL without compatibility coercion."""

    def test_missing_file_raises(self) -> None:
        with self.assertRaises(FileNotFoundError):
            load_manifest("./nonexistent_manifest.jsonl")

    def test_non_string_text_is_not_coerced(self) -> None:
        fd, path = tempfile.mkstemp(suffix=".jsonl")
        try:
            with os.fdopen(fd, "w") as f:
                f.write(
                    json.dumps({"audio_filepath": "gs://b/a.flac", "text": 123})
                )
            rows = load_manifest(path)
        finally:
            Path(path).unlink()

        self.assertEqual(rows[0]["text"], 123)


class TestRowsFromManifestStrict(unittest.TestCase):
    """rows_from_manifest converts valid rows and fills conversion defaults."""

    def test_row_maps_core_and_optional_fields(self) -> None:
        rows = rows_from_manifest([_canonical_row()])

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].offset, 0.0)
        self.assertEqual(rows[0].duration, 1.25)
        self.assertEqual(rows[0].example_id, "example")
        self.assertEqual(rows[0].segment_id, "001")
        self.assertEqual(rows[0].split, "train")
        self.assertEqual(rows[0].dataset["name"], "echo")
        self.assertEqual(
            rows[0].source_audio["audio_filepath"],
            "gs://bucket/source/example.mp3",
        )

    def test_optional_metadata_unknown_keys_are_preserved(self) -> None:
        rows = rows_from_manifest(
            [
                _canonical_row(
                    dataset={
                        "name": " echo ",
                        "family": " radio ",
                        "custom": {"priority": 1},
                    },
                    source_audio={
                        "audio_filepath": " gs://bucket/source/example.mp3 ",
                        "offset": 1,
                        "duration": 2,
                        "sample_rate_hz": 44100,
                    },
                )
            ]
        )

        self.assertEqual(rows[0].dataset["name"], "echo")
        self.assertEqual(rows[0].dataset["family"], "radio")
        self.assertEqual(rows[0].dataset["custom"], {"priority": 1})
        self.assertEqual(
            rows[0].source_audio["audio_filepath"],
            "gs://bucket/source/example.mp3",
        )
        self.assertEqual(rows[0].source_audio["offset"], 1.0)
        self.assertEqual(rows[0].source_audio["duration"], 2.0)
        self.assertEqual(rows[0].source_audio["sample_rate_hz"], 44100)

    def test_unknown_only_optional_metadata_is_preserved(self) -> None:
        rows = rows_from_manifest(
            [
                _canonical_row(
                    dataset={"custom_dataset_key": "echo"},
                    source_audio={"sample_rate_hz": 44100},
                )
            ]
        )

        self.assertEqual(rows[0].dataset, {"custom_dataset_key": "echo"})
        self.assertEqual(rows[0].source_audio, {"sample_rate_hz": 44100})

    def test_core_row_maps_optional_fields_to_none(self) -> None:
        row = _canonical_row()
        row.pop("split")
        row.pop("dataset")
        row.pop("source_audio")

        rows = rows_from_manifest([row])

        self.assertIsNone(rows[0].split)
        self.assertIsNone(rows[0].dataset)
        self.assertIsNone(rows[0].source_audio)

    def test_missing_identity_and_offset_are_derived_or_defaulted(
        self,
    ) -> None:
        rows = rows_from_manifest(
            [
                {
                    "audio_filepath": "gs://b/a.flac",
                    "text": "hello",
                    "duration": 2.0,
                }
            ]
        )

        self.assertEqual(rows[0].example_id, "a")
        self.assertEqual(rows[0].segment_id, "001")
        self.assertEqual(rows[0].offset, 0.0)
        self.assertEqual(rows[0].duration, 2.0)

    def test_missing_duration_still_fails(self) -> None:
        with self.assertRaisesRegex(ValueError, "duration"):
            rows_from_manifest(
                [
                    {
                        "audio_filepath": "gs://b/a.flac",
                        "text": "hello",
                    }
                ]
            )


class TestScoreableManifestEntry(unittest.TestCase):
    def test_requires_audio_filepath_and_non_blank_text(self) -> None:
        self.assertTrue(is_scoreable_manifest_entry(_canonical_row()))
        self.assertTrue(
            is_scoreable_manifest_entry(
                {
                    **_canonical_row(),
                    "pred_text_gemini": "derived prediction",
                }
            )
        )
        self.assertTrue(
            is_scoreable_manifest_entry(
                {"audio_filepath": "gs://b/a.flac", "text": "hello"}
            )
        )
        self.assertTrue(
            is_scoreable_manifest_entry(
                {**_canonical_row(), "unexpected": "not scoreable"}
            )
        )
        self.assertFalse(
            is_scoreable_manifest_entry(
                {"audio_filepath": "gs://b/a.flac", "text": ""}
            )
        )
        self.assertFalse(
            is_scoreable_manifest_entry(
                {"audio_filepath": "gs://b/a.flac", "text": "   "}
            )
        )
        self.assertFalse(
            is_scoreable_manifest_entry(
                {"audio_filepath": "gs://b/a.flac", "text": None}
            )
        )
        self.assertFalse(is_scoreable_manifest_entry({"text": "hello"}))


class TestRowsFromManifestRequiredFields(unittest.TestCase):
    """rows_from_manifest fails loudly for strict canonical fields."""

    def test_missing_audio_filepath_raises_with_row_context(self) -> None:
        row = _canonical_row()
        row.pop("audio_filepath")
        with self.assertRaisesRegex(ValueError, "audio_filepath"):
            rows_from_manifest([row])

    def test_blank_audio_filepath_raises_with_row_context(self) -> None:
        with self.assertRaisesRegex(ValueError, "audio_filepath"):
            rows_from_manifest([_canonical_row(audio_filepath="  ")])

    def test_missing_text_raises_with_row_context(self) -> None:
        row = _canonical_row()
        row.pop("text")
        with self.assertRaisesRegex(ValueError, "text"):
            rows_from_manifest([row])

    def test_blank_text_raises_with_row_context(self) -> None:
        with self.assertRaisesRegex(ValueError, "text"):
            rows_from_manifest([_canonical_row(text="  ")])

    def test_non_string_text_raises_type_error_with_row_context(self) -> None:
        with self.assertRaisesRegex(ValueError, "text must be a string"):
            rows_from_manifest([_canonical_row(text=123)])

    def test_blank_identity_fields_are_not_derived(self) -> None:
        with self.assertRaisesRegex(ValueError, "example_id"):
            rows_from_manifest([_canonical_row(example_id="  ")])
        with self.assertRaisesRegex(ValueError, "segment_id"):
            rows_from_manifest([_canonical_row(segment_id="  ")])

    def test_required_string_values_are_stripped(self) -> None:
        rows = rows_from_manifest(
            [
                _canonical_row(
                    audio_filepath=" gs://b/a.flac ",
                    text=" hello ",
                    example_id=" example ",
                    segment_id=" 001 ",
                    split=" train ",
                    dataset={"name": " echo ", "family": " radio "},
                    source_audio={
                        "audio_filepath": " gs://bucket/source/example.mp3 ",
                        "offset": 1,
                        "duration": 2,
                    },
                )
            ]
        )

        self.assertEqual(rows[0].audio_filepath, "gs://b/a.flac")
        self.assertEqual(rows[0].text, "hello")
        self.assertEqual(rows[0].example_id, "example")
        self.assertEqual(rows[0].dataset["name"], "echo")
        self.assertEqual(
            rows[0].source_audio["audio_filepath"],
            "gs://bucket/source/example.mp3",
        )


class TestLoadManifestBoundaries(unittest.TestCase):
    """load_manifest fails on malformed JSON/JSONL boundaries."""

    def test_json_array_is_loaded_without_canonical_validation(self) -> None:
        fd, path = tempfile.mkstemp(suffix=".json")
        try:
            with os.fdopen(fd, "w") as f:
                json.dump([{"audio_filepath": "local/audio.mp3"}], f)
            rows = load_manifest(path)
        finally:
            Path(path).unlink()

        self.assertEqual(rows, [{"audio_filepath": "local/audio.mp3"}])

    def test_json_array_with_utf8_bom_is_loaded(self) -> None:
        fd, path = tempfile.mkstemp(suffix=".json")
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                f.write(
                    "\ufeff"
                    + json.dumps([{"audio_filepath": "local/audio.mp3"}])
                )
            rows = load_manifest(path)
        finally:
            Path(path).unlink()

        self.assertEqual(rows, [{"audio_filepath": "local/audio.mp3"}])

    def test_malformed_jsonl_rows_raise(self) -> None:
        fd, path = tempfile.mkstemp(suffix=".jsonl")
        try:
            with os.fdopen(fd, "w") as f:
                f.write("{bad json}\n")
                f.write(json.dumps({"audio_filepath": "local/audio.mp3"}))
            with self.assertRaises(ValueError):
                load_manifest(path)
        finally:
            Path(path).unlink()
