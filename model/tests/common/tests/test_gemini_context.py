"""Tests for prior-context history construction."""

from __future__ import annotations

import unittest

from common.gemini import context


class TestGeminiContextHistories(unittest.TestCase):
    def test_groups_by_original_audio_uri_and_sorts_by_original_offset(
        self,
    ) -> None:
        rows = [
            {
                "audio_filepath": "gs://audio/source-a/002.flac",
                "original_audio_uri": "gs://audio/source-a.flac",
                "original_offset": 2.0,
                "row_index": 2,
                "text": "second",
            },
            {
                "audio_filepath": "gs://audio/source-b/001.flac",
                "original_audio_uri": "gs://audio/source-b.flac",
                "original_offset": 1.0,
                "row_index": 1,
                "text": "other source",
            },
            {
                "audio_filepath": "gs://audio/source-a/001.flac",
                "original_audio_uri": "gs://audio/source-a.flac",
                "original_offset": 1.0,
                "row_index": 1,
                "text": "first",
            },
            {
                "audio_filepath": "gs://audio/source-a/003.flac",
                "original_audio_uri": "gs://audio/source-a.flac",
                "original_offset": 3.0,
                "row_index": 3,
                "text": "[UNINTELLIGIBLE]",
            },
            {
                "audio_filepath": "gs://audio/source-a/004.flac",
                "original_audio_uri": "gs://audio/source-a.flac",
                "original_offset": 4.0,
                "row_index": 4,
                "text": "fourth",
            },
        ]

        histories = context.build_context_histories(rows, max_turns=2)

        self.assertEqual(
            histories[0],
            [context.ContextTurn("gs://audio/source-a/001.flac", "first")],
        )
        self.assertEqual(histories[1], [])
        self.assertEqual(histories[2], [])
        self.assertEqual(
            histories[3],
            [
                context.ContextTurn("gs://audio/source-a/001.flac", "first"),
                context.ContextTurn("gs://audio/source-a/002.flac", "second"),
            ],
        )
        self.assertEqual(
            histories[4],
            [
                context.ContextTurn("gs://audio/source-a/001.flac", "first"),
                context.ContextTurn("gs://audio/source-a/002.flac", "second"),
            ],
        )

    def test_limits_history_to_max_turns(self) -> None:
        rows = [
            {
                "audio_filepath": f"gs://audio/{i}.flac",
                "original_audio_uri": "gs://audio/source.flac",
                "original_offset": float(i),
                "text": str(i),
            }
            for i in range(5)
        ]

        histories = context.build_context_histories(rows, max_turns=3)

        self.assertEqual(
            histories[-1],
            [
                context.ContextTurn("gs://audio/1.flac", "1"),
                context.ContextTurn("gs://audio/2.flac", "2"),
                context.ContextTurn("gs://audio/3.flac", "3"),
            ],
        )

    def test_source_group_does_not_group_unrelated_audio(self) -> None:
        rows = [
            {
                "audio_filepath": "gs://audio/a.flac",
                "source_group": "eval",
                "offset": 1.0,
                "text": "alpha",
            },
            {
                "audio_filepath": "gs://audio/b.flac",
                "source_group": "eval",
                "offset": 2.0,
                "text": "bravo",
            },
        ]

        histories = context.build_context_histories(rows, max_turns=2)

        self.assertEqual(histories, [[], []])

    def test_missing_episode_key_falls_back_to_unique_row_key(self) -> None:
        self.assertNotEqual(
            context._episode_key({"text": "first"}, 0),
            context._episode_key({"text": "second"}, 1),
        )


if __name__ == "__main__":
    unittest.main()
