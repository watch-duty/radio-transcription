from __future__ import annotations

import unittest

from common import recording_groups


def _row(
    audio_uri: str,
    source_uri: str,
) -> dict[str, object]:
    return {
        "audio_filepath": audio_uri,
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

    def test_rejects_matching_existing_source_sha_across_renamed_objects(
        self,
    ) -> None:
        train = _row(
            "gs://clips/train.flac",
            "gs://sources/original-a.flac",
        )
        train["source_lineage"] = {"source_encoded_sha256": "a" * 64}
        evaluation = _row(
            "gs://clips/eval.flac",
            "gs://archive/completely-different.wav",
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

    def test_exact_source_uri_matches_despite_different_encoded_hashes(
        self,
    ) -> None:
        shared_source = "gs://sources/original.flac"
        train = _row("gs://clips/train.flac", shared_source)
        train["source_lineage"] = {"source_encoded_sha256": "a" * 64}
        evaluation = _row("gs://clips/eval.flac", shared_source)
        evaluation["source_lineage"] = {"source_encoded_sha256": "b" * 64}

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
