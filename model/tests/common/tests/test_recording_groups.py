from __future__ import annotations

import unittest

from common import recording_groups


def _row(
    audio_uri: str,
    source_uri: str,
    *,
    dataset: str = "echo",
) -> dict[str, object]:
    return {
        "audio_filepath": audio_uri,
        "dataset": {"name": dataset},
        "example_id": audio_uri,
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
        ):
            recording_groups.reject_split_leakage(rows)

    def test_top_level_original_source_matches_latest_context_contract(
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
