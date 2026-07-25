from __future__ import annotations

import unittest

from common import manifest, recording_groups


def _row(
    audio_uri: str,
    source_uri: str,
    *,
    dataset: str = "echo",
) -> dict[str, object]:
    return {
        "audio_filepath": audio_uri,
        "dataset": {"name": dataset},
        "duration": 1.0,
        "example_id": audio_uri,
        "offset": 0.0,
        "segment_id": "001",
        "source_audio": {"audio_filepath": source_uri},
        "text": "transcript",
    }


class TestRejectSplitLeakage(unittest.TestCase):
    def test_rejects_different_clips_from_the_same_source_recording(
        self,
    ) -> None:
        rows = {
            "train": [
                _row(
                    "gs://clips/train.flac",
                    "gs://sources/recording.flac",
                )
            ],
            "validation": [],
            "eval": [
                _row(
                    "gs://clips/eval.flac",
                    "gs://sources/recording.flac",
                )
            ],
        }

        with self.assertRaisesRegex(
            ValueError,
            "train and eval share 1 physical recording group",
        ) as raised:
            recording_groups.reject_split_leakage(rows)
        message = str(raised.exception)
        self.assertIn("echo", message)
        self.assertIn("gs://sources/recording.flac", message)

    def test_top_level_original_source_takes_precedence(
        self,
    ) -> None:
        train = _row(
            "gs://clips/train.flac",
            "gs://sources/intermediate-train.flac",
        )
        train["original_audio_uri"] = "gs://sources/original.flac"
        evaluation = _row(
            "gs://clips/eval.flac",
            "gs://sources/intermediate-eval.flac",
        )
        evaluation["original_audio_uri"] = "gs://sources/original.flac"

        with self.assertRaisesRegex(
            ValueError,
            "train and eval share 1 physical recording group",
        ):
            recording_groups.reject_split_leakage(
                {
                    "train": [train],
                    "validation": [],
                    "eval": [evaluation],
                }
            )

    def test_nested_original_source_takes_precedence_over_intermediate(
        self,
    ) -> None:
        shared_source = "gs://sources/original.flac"
        train = _row(
            "gs://clips/train.flac",
            "gs://sources/intermediate-train.flac",
        )
        train["source_audio"] = {
            "audio_filepath": "gs://sources/intermediate-train.flac",
            "original_audio_uri": shared_source,
        }
        evaluation = _row(
            "gs://clips/eval.flac",
            "gs://sources/intermediate-eval.flac",
        )
        evaluation["source_audio"] = {
            "audio_filepath": "gs://sources/intermediate-eval.flac",
            "original_audio_uri": shared_source,
        }

        with self.assertRaisesRegex(
            ValueError,
            "train and eval share 1 physical recording group",
        ):
            recording_groups.reject_split_leakage(
                {
                    "train": [train],
                    "validation": [],
                    "eval": [evaluation],
                }
            )

    def test_blank_top_level_source_falls_back_to_source_audio(self) -> None:
        shared_source = "gs://sources/original.flac"
        train = _row("gs://clips/train.flac", shared_source)
        train["original_audio_uri"] = "  "
        evaluation = _row("gs://clips/eval.flac", shared_source)

        with self.assertRaisesRegex(
            ValueError,
            "train and eval share 1 physical recording group",
        ):
            recording_groups.reject_split_leakage(
                {
                    "train": [train],
                    "validation": [],
                    "eval": [evaluation],
                }
            )

    def test_unusable_basename_evidence_does_not_invalidate_row(self) -> None:
        source_locators = (
            "https://example.com/raw/recording.mp3",
            "gs://sources/raw/recording.unknown",
            "gs://sources/raw/%FF.flac",
            "opaque-source-id",
        )
        for source_locator in source_locators:
            with self.subTest(source_locator=source_locator):
                row = _row("gs://clips/train.flac", source_locator)
                manifest.require_canonical_manifest([row])

                recording_groups.reject_split_leakage(
                    {
                        "train": [row],
                        "validation": [],
                        "eval": [],
                    }
                )

    def test_exact_uri_still_matches_without_basename_evidence(self) -> None:
        shared_source = "https://example.com/raw/recording.mp3"

        with self.assertRaisesRegex(
            ValueError,
            "train and eval share 1 physical recording group",
        ):
            recording_groups.reject_split_leakage(
                {
                    "train": [_row("gs://clips/train.flac", shared_source)],
                    "validation": [],
                    "eval": [_row("gs://clips/eval.flac", shared_source)],
                }
            )

    def test_rejects_training_and_validation_source_overlap(self) -> None:
        shared_source = "gs://sources/recording.flac"

        with self.assertRaisesRegex(
            ValueError,
            "train and validation share 1 physical recording group",
        ):
            recording_groups.reject_split_leakage(
                {
                    "train": [_row("gs://clips/train.flac", shared_source)],
                    "validation": [
                        _row("gs://clips/validation.flac", shared_source)
                    ],
                    "eval": [],
                }
            )

    def test_allows_validation_and_eval_source_overlap(self) -> None:
        shared_source = "gs://sources/holdout.flac"

        recording_groups.reject_split_leakage(
            {
                "train": [
                    _row(
                        "gs://clips/train.flac",
                        "gs://sources/train.flac",
                    )
                ],
                "validation": [
                    _row("gs://clips/validation.flac", shared_source)
                ],
                "eval": [_row("gs://clips/eval.flac", shared_source)],
            }
        )

    def test_nested_original_source_is_used_as_fallback(self) -> None:
        shared_source = "gs://sources/original.flac"
        train = _row("gs://clips/train.flac", "unused")
        train["source_audio"] = {"original_audio_uri": shared_source}
        evaluation = _row("gs://clips/eval.flac", "unused")
        evaluation["source_audio"] = {"original_audio_uri": shared_source}

        with self.assertRaisesRegex(
            ValueError,
            "train and eval share 1 physical recording group",
        ):
            recording_groups.reject_split_leakage(
                {
                    "train": [train],
                    "validation": [],
                    "eval": [evaluation],
                }
            )

    def test_model_ready_audio_is_used_when_source_metadata_is_absent(
        self,
    ) -> None:
        train = _row(
            "gs://clips/train/shared-recording.flac",
            "unused",
        )
        del train["source_audio"]
        evaluation = _row(
            "gs://archive/eval/shared-recording.flac",
            "unused",
        )
        del evaluation["source_audio"]
        manifest.require_canonical_manifest([train, evaluation])

        with self.assertRaisesRegex(
            ValueError,
            "train and eval share 1 physical recording group",
        ):
            recording_groups.reject_split_leakage(
                {
                    "train": [train],
                    "validation": [],
                    "eval": [evaluation],
                }
            )

    def test_rejects_normalized_source_filename_aliases_within_dataset(
        self,
    ) -> None:
        rows = {
            "train": [
                _row(
                    "gs://clips/train.flac",
                    "gs://sources/a/Dispatch%20Capture.FLAC",
                )
            ],
            "validation": [],
            "eval": [
                _row(
                    "gs://clips/eval.flac",
                    "gs://archive/b/dispatch%20%20capture.wav",
                )
            ],
        }

        with self.assertRaisesRegex(
            ValueError,
            "train and eval share 1 physical recording group",
        ):
            recording_groups.reject_split_leakage(rows)

    def test_rejects_matching_existing_source_sha_across_renamed_objects(
        self,
    ) -> None:
        train = _row(
            "gs://clips/train.flac",
            "gs://sources/original-a.flac",
            dataset="echo",
        )
        train["source_lineage"] = {"source_encoded_sha256": "a" * 64}
        evaluation = _row(
            "gs://clips/eval.flac",
            "gs://archive/completely-different.wav",
            dataset="bcfy_calls",
        )
        evaluation["source_lineage"] = {"source_encoded_sha256": "a" * 64}

        with self.assertRaisesRegex(
            ValueError,
            "train and eval share 1 physical recording group",
        ):
            recording_groups.reject_split_leakage(
                {
                    "train": [train],
                    "validation": [],
                    "eval": [evaluation],
                }
            )

    def test_source_sha_is_case_insensitive(self) -> None:
        train = _row(
            "gs://clips/train.flac",
            "gs://sources/original-a.flac",
        )
        train["source_lineage"] = {"source_encoded_sha256": "A" * 64}
        evaluation = _row(
            "gs://clips/eval.flac",
            "gs://sources/original-b.flac",
        )
        evaluation["source_lineage"] = {"source_encoded_sha256": "a" * 64}

        with self.assertRaisesRegex(
            ValueError,
            "train and eval share 1 physical recording group",
        ):
            recording_groups.reject_split_leakage(
                {
                    "train": [train],
                    "validation": [],
                    "eval": [evaluation],
                }
            )

    def test_distinct_hashes_override_ambiguous_legacy_basename(self) -> None:
        train = _row(
            "gs://clips/train.flac",
            "gs://sources/a/shared-name.flac",
        )
        train["source_lineage"] = {"source_encoded_sha256": "a" * 64}
        evaluation = _row(
            "gs://clips/eval.flac",
            "gs://archive/b/shared-name.wav",
        )
        evaluation["source_lineage"] = {"source_encoded_sha256": "b" * 64}

        recording_groups.reject_split_leakage(
            {
                "train": [train],
                "validation": [
                    _row(
                        "gs://clips/validation.flac",
                        "gs://sources/unhashed.flac",
                    )
                ],
                "eval": [evaluation],
            }
        )

    def test_source_sha_disables_filename_guessing_for_complete_dataset(
        self,
    ) -> None:
        train = _row(
            "gs://clips/train.flac",
            "gs://sources/a/shared-name.flac",
        )
        train["source_lineage"] = {"source_encoded_sha256": "a" * 64}
        evaluation = _row(
            "gs://clips/eval.flac",
            "gs://archive/b/shared-name.wav",
        )
        evaluation["source_lineage"] = {"source_encoded_sha256": "b" * 64}

        recording_groups.reject_split_leakage(
            {
                "train": [train],
                "validation": [],
                "eval": [evaluation],
            }
        )

    def test_filename_aliases_are_scoped_to_one_dataset(self) -> None:
        rows = {
            "train": [
                _row(
                    "gs://clips/train.flac",
                    "gs://sources/a/shared-name.flac",
                    dataset="echo",
                )
            ],
            "validation": [],
            "eval": [
                _row(
                    "gs://clips/eval.flac",
                    "gs://archive/b/shared-name.wav",
                    dataset="bcfy_calls",
                )
            ],
        }

        recording_groups.reject_split_leakage(rows)


class TestUnionFind(unittest.TestCase):
    def test_root_handles_a_deep_alias_chain(self) -> None:
        values = [("echo", str(index)) for index in range(1_500)]
        groups = recording_groups._UnionFind(values)
        for new_root, previous_root in zip(
            values[1:],
            values[:-1],
            strict=True,
        ):
            groups.merge_all((new_root, previous_root))

        self.assertEqual(groups.root(values[0]), values[-1])
