"""Tests for Phase 2 ranking, cache, and context helpers."""

import csv
import dataclasses
import json
import pathlib
import tempfile
import unittest


def _row(
    audio_segment_id: str,
    row_index: int,
    text: str = "Engine 41 copy",
    *,
    source_group: str = "source-a",
    model_ready_audio_uri: str | None = None,
) -> dict[str, object]:
    if model_ready_audio_uri is None:
        model_ready_audio_uri = f"gs://bucket/{audio_segment_id}.flac"
    return {
        "audio_segment_id": audio_segment_id,
        "source_window_id": f"source-window-{audio_segment_id}",
        "model_ready_audio_uri": model_ready_audio_uri,
        "original_audio_uri": "gs://bucket/original.wav",
        "offset": row_index * 1.0,
        "duration": 2.5,
        "source_group": source_group,
        "row_index": row_index,
        "split": "train",
        "dataset_name": "test",
        "text": text,
    }


def _entry(
    ranking: object,
    audio_segment_id: str,
    prediction_text: str,
    *,
    row_index: int = 1,
    model_id: str = "gemini-model",
    prompt_fp: str = "prompt-fp",
    context_policy_fp: str = "context-policy-fp",
    num_recent_events: int = 1000,
    context_fp: str = "context-fp",
    source_group: str = "source-a",
    error: str = "",
) -> object:
    return ranking.PredictionCacheEntry(
        audio_segment_id=audio_segment_id,
        prediction_text=prediction_text,
        model_id=model_id,
        prompt_fingerprint=prompt_fp,
        context_policy_fingerprint=context_policy_fp,
        num_recent_events=num_recent_events,
        context_fingerprint=context_fp,
        source_group=source_group,
        row_index=row_index,
        model_ready_audio_uri=f"gs://bucket/{audio_segment_id}.flac",
        created_at="2026-06-02T00:00:00Z",
        error=error,
    )


class TestContextSelection(unittest.TestCase):
    def test_fingerprints_are_stable_and_input_sensitive(self) -> None:
        from common import ranking

        first_prompt = ranking.prompt_fingerprint("system", "user")
        same_prompt = ranking.prompt_fingerprint("system", "user")
        changed_prompt = ranking.prompt_fingerprint("system", "changed")

        self.assertEqual(first_prompt, same_prompt)
        self.assertTrue(first_prompt.startswith("prompt-"))
        self.assertNotEqual(first_prompt, changed_prompt)

        default_policy = ranking.context_policy_fingerprint()
        changed_policy = ranking.context_policy_fingerprint(
            num_recent_events=ranking.NUM_RECENT_EVENTS + 2
        )

        self.assertTrue(default_policy.startswith("context-"))
        self.assertNotEqual(default_policy, changed_policy)

        first_entry = _entry(ranking, "audio-a", "prediction one")
        second_entry = _entry(ranking, "audio-b", "prediction two")
        context_fp = ranking.context_fingerprint([first_entry, second_entry])
        changed_context_fp = ranking.context_fingerprint(
            [
                first_entry,
                _entry(ranking, "audio-b", "changed prediction"),
            ]
        )

        self.assertTrue(context_fp.startswith("context-"))
        self.assertNotEqual(context_fp, changed_context_fp)

    def test_ordered_source_rows_groups_and_sorts_by_row_index(self) -> None:
        from common import ranking

        rows = [
            _row("audio-late", 3),
            _row("other-source", 1, source_group="source-b"),
            _row("audio-early", 1),
            _row("audio-tie", 1),
        ]

        source_rows = ranking.ordered_source_rows(rows)

        self.assertEqual(
            [row["audio_segment_id"] for row in source_rows["source-a"]],
            ["audio-early", "audio-tie", "audio-late"],
        )
        self.assertEqual(
            [row["audio_segment_id"] for row in source_rows["source-b"]],
            ["other-source"],
        )

    def test_previous_context_uses_same_source_prior_predictions(self) -> None:
        from common import ranking

        rows = [
            _row("source-a-3", 3),
            _row("source-b-1", 1, source_group="source-b"),
            _row("source-a-1", 1),
            _row("source-a-5", 5),
            _row("source-a-4", 4),
            _row("source-a-2", 2),
        ]
        source_rows = ranking.ordered_source_rows(rows)
        cache_by_audio_id = {
            "source-a-1": _entry(
                ranking,
                "source-a-1",
                "first prediction",
                row_index=1,
            ),
            "source-a-2": _entry(
                ranking,
                "source-a-2",
                "failed prediction",
                row_index=2,
                error="model failed",
            ),
            "source-a-3": _entry(
                ranking,
                "source-a-3",
                "third prediction",
                row_index=3,
            ),
            "source-a-4": _entry(
                ranking,
                "source-a-4",
                "current prediction",
                row_index=4,
            ),
            "source-a-5": _entry(
                ranking,
                "source-a-5",
                "future prediction",
                row_index=5,
            ),
            "source-b-1": _entry(
                ranking,
                "source-b-1",
                "other source prediction",
                row_index=1,
                source_group="source-b",
            ),
        }

        context = ranking.previous_prediction_context(
            source_rows,
            _row("source-a-4", 4),
            cache_by_audio_id,
        )

        self.assertEqual(
            [entry.audio_segment_id for entry in context],
            ["source-a-1", "source-a-3"],
        )
        self.assertEqual(
            [entry.prediction_text for entry in context],
            ["first prediction", "third prediction"],
        )

    def test_previous_context_uses_source_order_for_row_index_ties(
        self,
    ) -> None:
        from common import ranking

        rows = [
            _row("tie-prior", 1),
            _row("tie-current", 1),
            _row("future", 2),
        ]
        source_rows = ranking.ordered_source_rows(rows)
        cache_by_audio_id = {
            "tie-prior": _entry(
                ranking,
                "tie-prior",
                "prior tie prediction",
                row_index=1,
            ),
            "future": _entry(
                ranking,
                "future",
                "future prediction",
                row_index=2,
            ),
        }

        context = ranking.previous_prediction_context(
            source_rows,
            _row("tie-current", 1),
            cache_by_audio_id,
        )

        self.assertEqual(
            [entry.audio_segment_id for entry in context],
            ["tie-prior"],
        )

    def test_previous_context_caps_1000_events_to_500_rows(self) -> None:
        from common import ranking

        rows = [_row(f"audio-{i}", i) for i in range(501)]
        rows.append(_row("current", 501))
        source_rows = ranking.ordered_source_rows(rows)
        context_fp = ranking.context_fingerprint([])
        cache_by_audio_id = {}
        for i in range(501):
            audio_id = f"audio-{i}"
            cache_by_audio_id[audio_id] = _entry(
                ranking,
                audio_id,
                f"prediction {i}",
                row_index=i,
                context_fp=context_fp,
            )

        context = ranking.previous_prediction_context(
            source_rows,
            _row("current", 501),
            cache_by_audio_id,
        )

        self.assertEqual(ranking.NUM_RECENT_EVENTS, 1000)
        self.assertEqual(ranking.MAX_CONTEXT_ROWS, 500)
        self.assertEqual(len(context), ranking.MAX_CONTEXT_ROWS)
        self.assertEqual(context[0].audio_segment_id, "audio-1")
        self.assertEqual(context[-1].audio_segment_id, "audio-500")


class TestPredictionCacheCompatibility(unittest.TestCase):
    def test_cache_entry_requires_all_compatibility_fields(self) -> None:
        from common import ranking

        entry = _entry(ranking, "audio-a", "prediction")

        self.assertTrue(
            ranking.is_cache_entry_compatible(
                entry,
                model_id="gemini-model",
                prompt_fp="prompt-fp",
                context_policy_fp="context-policy-fp",
                num_recent_events=1000,
                context_fp="context-fp",
            )
        )

        changes = {
            "model_id": {"model_id": "other-model"},
            "prompt_fingerprint": {"prompt_fingerprint": "other-prompt"},
            "context_policy_fingerprint": {
                "context_policy_fingerprint": "other-policy"
            },
            "num_recent_events": {"num_recent_events": 998},
            "context_fingerprint": {"context_fingerprint": "other-context"},
            "error": {"error": "model failed"},
        }
        for field, kwargs in changes.items():
            with self.subTest(field=field):
                changed_entry = dataclasses.replace(entry, **kwargs)
                self.assertFalse(
                    ranking.is_cache_entry_compatible(
                        changed_entry,
                        model_id="gemini-model",
                        prompt_fp="prompt-fp",
                        context_policy_fp="context-policy-fp",
                        num_recent_events=1000,
                        context_fp="context-fp",
                    )
                )


class TestWerRanking(unittest.TestCase):
    def test_score_ranked_rows_sorts_and_excludes_empty_references(
        self,
    ) -> None:
        from common import ranking

        normalizer_calls = []

        def fake_normalizer(value: str) -> str:
            normalizer_calls.append(value)
            if value == "[empty]":
                return ""
            return value.strip().lower()

        compute_calls = []

        def fake_compute_wer(
            references: list[str],
            hypotheses: list[str],
            normalizer: object | None = None,
        ) -> dict[str, object]:
            compute_calls.append((references, hypotheses, normalizer))
            if hypotheses[0] == "major error":
                return {
                    "wer": 80.0,
                    "insertions": 3,
                    "deletions": 2,
                    "substitutions": 1,
                    "hits": 4,
                }
            return {
                "wer": 10.0,
                "insertions": 1,
                "deletions": 0,
                "substitutions": 0,
                "hits": 9,
            }

        original_build_normalizer = ranking.scoring.build_normalizer
        original_compute_wer = ranking.scoring.compute_wer
        ranking.scoring.build_normalizer = lambda: fake_normalizer
        ranking.scoring.compute_wer = fake_compute_wer
        try:
            empty_context_fp = ranking.context_fingerprint([])
            first_entry = _entry(
                ranking,
                "audio-a",
                "minor error",
                row_index=1,
                context_fp=empty_context_fp,
            )
            second_context_fp = ranking.context_fingerprint([first_entry])
            second_entry = _entry(
                ranking,
                "audio-b",
                "major error",
                row_index=2,
                context_fp=second_context_fp,
            )
            empty_context = ranking.context_fingerprint(
                [first_entry, second_entry]
            )
            empty_entry = _entry(
                ranking,
                "audio-empty",
                "empty prediction",
                row_index=3,
                context_fp=empty_context,
            )
            rows = [
                _row("audio-a", 1, "Engine 41 copy"),
                _row("audio-b", 2, "Engine 41 long response"),
                _row("audio-empty", 3, "[empty]"),
            ]

            ranked_rows, excluded_rows = ranking.score_ranked_rows(
                rows,
                {
                    "audio-a": first_entry,
                    "audio-b": second_entry,
                    "audio-empty": empty_entry,
                },
                model_id="gemini-model",
                prompt_fp="prompt-fp",
                context_policy_fp="context-policy-fp",
                num_recent_events=ranking.NUM_RECENT_EVENTS,
            )
        finally:
            ranking.scoring.build_normalizer = original_build_normalizer
            ranking.scoring.compute_wer = original_compute_wer

        self.assertEqual(
            [row["audio_segment_id"] for row in ranked_rows],
            ["audio-b", "audio-a"],
        )
        self.assertEqual([row["rank"] for row in ranked_rows], [1, 2])
        self.assertEqual(ranked_rows[0]["wer"], 80.0)
        self.assertEqual(ranked_rows[0]["insertions"], 3)
        self.assertEqual(ranked_rows[0]["deletions"], 2)
        self.assertEqual(ranked_rows[0]["substitutions"], 1)
        self.assertEqual(ranked_rows[0]["hits"], 4)
        self.assertEqual(ranked_rows[0]["prediction_text"], "major error")
        self.assertEqual(len(compute_calls), 2)
        self.assertTrue(normalizer_calls)
        self.assertEqual(len(excluded_rows), 1)
        self.assertEqual(excluded_rows[0]["audio_segment_id"], "audio-empty")
        self.assertEqual(
            excluded_rows[0]["exclusion_reason"],
            "empty_normalized_reference",
        )


class TestRankingWriters(unittest.TestCase):
    def test_jsonl_and_csv_writers_include_ranking_fields(self) -> None:
        from common import ranking

        rows = [
            {
                "rank": 1,
                "wer": 80.0,
                "audio_segment_id": "audio-a",
                "model_ready_audio_uri": "gs://bucket/audio-a.flac",
                "text": "Engine 41 copy",
                "prediction_text": "Engine 42 copy",
            }
        ]

        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = pathlib.Path(tmpdir)
            jsonl_path = tmp_path / "ranking.jsonl"
            csv_path = tmp_path / "ranking.csv"

            ranking.write_jsonl(jsonl_path, rows)
            ranking.write_ranked_csv(csv_path, rows)

            jsonl_rows = [
                json.loads(line)
                for line in jsonl_path.read_text(encoding="utf-8").splitlines()
            ]
            with csv_path.open(encoding="utf-8", newline="") as csv_file:
                csv_rows = list(csv.DictReader(csv_file))

        for field in (
            "rank",
            "wer",
            "audio_segment_id",
            "model_ready_audio_uri",
            "text",
            "prediction_text",
        ):
            self.assertIn(field, jsonl_rows[0])
            self.assertIn(field, csv_rows[0])
        self.assertEqual(jsonl_rows[0]["rank"], 1)
        self.assertEqual(csv_rows[0]["rank"], "1")


if __name__ == "__main__":
    unittest.main()
