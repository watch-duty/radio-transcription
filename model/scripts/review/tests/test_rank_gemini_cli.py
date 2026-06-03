"""Tests for the Phase 2 Gemini ranking CLI."""

from __future__ import annotations

import contextlib
import dataclasses
import io
import json
import sys
import tempfile
import unittest
import unittest.mock
from pathlib import Path

_REVIEW_DIR = str(Path(__file__).resolve().parent.parent)
_COLABS_DIR = str(
    Path(__file__).resolve().parent.parent.parent.parent / "colabs"
)
if _REVIEW_DIR not in sys.path:
    sys.path.insert(0, _REVIEW_DIR)
if _COLABS_DIR not in sys.path:
    sys.path.insert(0, _COLABS_DIR)


def _row(audio_segment_id: str, row_index: int) -> dict[str, object]:
    return {
        "audio_segment_id": audio_segment_id,
        "source_window_id": f"source-window-{audio_segment_id}",
        "model_ready_audio_uri": f"gs://bucket/{audio_segment_id}.flac",
        "original_audio_uri": "gs://bucket/original.wav",
        "offset": row_index * 1.0,
        "duration": 2.5,
        "source_group": "source-a",
        "row_index": row_index,
        "split": "train",
        "dataset_name": "test",
        "text": "Engine 41 copy",
    }


def _write_jsonl(path: Path, rows: list[dict[str, object]]) -> None:
    with path.open("w", encoding="utf-8") as output_file:
        for row in rows:
            output_file.write(json.dumps(row) + "\n")


def _paths(tmp_path: Path) -> dict[str, Path]:
    return {
        "review_pool": tmp_path / "review_pool.jsonl",
        "cache": tmp_path / "cache.jsonl",
        "ranked_jsonl": tmp_path / "ranked.jsonl",
        "ranked_csv": tmp_path / "ranked.csv",
        "excluded_jsonl": tmp_path / "excluded.jsonl",
    }


def _required_args(paths: dict[str, Path]) -> list[str]:
    return [
        "--review-pool-jsonl",
        str(paths["review_pool"]),
        "--prediction-cache-jsonl",
        str(paths["cache"]),
        "--ranked-jsonl",
        str(paths["ranked_jsonl"]),
        "--ranked-csv",
        str(paths["ranked_csv"]),
        "--excluded-jsonl",
        str(paths["excluded_jsonl"]),
    ]


def _fake_ranked_rows() -> tuple[
    list[dict[str, object]],
    list[dict[str, object]],
]:
    ranked = [
        {
            "rank": 1,
            "wer": 25.0,
            "audio_segment_id": "audio-a",
            "model_ready_audio_uri": "gs://bucket/audio-a.flac",
            "text": "Engine 41 copy",
            "prediction_text": "Engine 42 copy",
        }
    ]
    excluded = [{"audio_segment_id": "audio-b", "exclusion_reason": "empty"}]
    return ranked, excluded


class _FakeRunner:
    instances: list[_FakeRunner] = []

    def __init__(
        self,
        _client: object,
        *,
        model_id: str,
        **_kwargs: object,
    ) -> None:
        self.model_id = model_id
        self.instances.append(self)


class TestRankGeminiCli(unittest.TestCase):
    def test_help_lists_expected_subcommands(self) -> None:
        import rank_gemini

        output = io.StringIO()
        with self.assertRaises(SystemExit) as ctx:
            with contextlib.redirect_stdout(output):
                rank_gemini.main(["--help"])

        self.assertEqual(ctx.exception.code, 0)
        help_text = output.getvalue()
        self.assertIn("preflight", help_text)
        self.assertIn("run", help_text)
        self.assertIn("rank-cache", help_text)

    def test_rank_cache_writes_outputs_without_runner(self) -> None:
        import rank_gemini
        from common import ranking

        with tempfile.TemporaryDirectory() as tmpdir:
            paths = _paths(Path(tmpdir))
            _write_jsonl(paths["review_pool"], [_row("audio-a", 1)])
            cache_entry = ranking.PredictionCacheEntry(
                audio_segment_id="audio-a",
                prediction_text="Engine 42 copy",
                model_id="gemini-3.5-flash",
                prompt_fingerprint="prompt",
                context_policy_fingerprint="context-policy",
                num_recent_events=1000,
                context_fingerprint="context",
                source_group="source-a",
                row_index=1,
                model_ready_audio_uri="gs://bucket/audio-a.flac",
                created_at="2026-06-02T00:00:00Z",
                error="",
            )
            _write_jsonl(paths["cache"], [dataclasses.asdict(cache_entry)])

            with (
                unittest.mock.patch.object(
                    rank_gemini.gemini_ranking,
                    "GeminiRankingRunner",
                    side_effect=AssertionError("runner should not be used"),
                ),
                unittest.mock.patch.object(
                    rank_gemini.ranking,
                    "score_ranked_rows",
                    return_value=_fake_ranked_rows(),
                ),
            ):
                exit_code = rank_gemini.main(
                    ["rank-cache", *_required_args(paths)]
                )

            self.assertEqual(exit_code, 0)
            self.assertTrue(paths["ranked_jsonl"].exists())
            self.assertTrue(paths["ranked_csv"].exists())
            self.assertTrue(paths["excluded_jsonl"].exists())
            self.assertIn("ranked.csv", str(paths["ranked_csv"]))
            self.assertIn("excluded.jsonl", str(paths["excluded_jsonl"]))

    def test_missing_review_pool_raises_file_not_found(self) -> None:
        import rank_gemini

        with tempfile.TemporaryDirectory() as tmpdir:
            paths = _paths(Path(tmpdir))
            _write_jsonl(paths["cache"], [])

            with self.assertRaises(FileNotFoundError):
                rank_gemini.main(["rank-cache", *_required_args(paths)])

    def test_missing_prediction_cache_starts_empty(self) -> None:
        import rank_gemini

        with tempfile.TemporaryDirectory() as tmpdir:
            paths = _paths(Path(tmpdir))
            _write_jsonl(paths["review_pool"], [_row("audio-a", 1)])
            captured: dict[str, object] = {}

            def fake_score_ranked_rows(
                _review_rows: list[dict[str, object]],
                cache_by_audio_id: dict[str, object],
                **_kwargs: object,
            ) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
                captured["cache_by_audio_id"] = cache_by_audio_id
                return _fake_ranked_rows()

            with unittest.mock.patch.object(
                rank_gemini.ranking,
                "score_ranked_rows",
                fake_score_ranked_rows,
            ):
                exit_code = rank_gemini.main(
                    ["rank-cache", *_required_args(paths)]
                )

            self.assertEqual(exit_code, 0)
            self.assertEqual(captured["cache_by_audio_id"], {})

    def test_missing_gcs_prediction_cache_starts_empty(self) -> None:
        import rank_gemini

        with (
            unittest.mock.patch.object(
                rank_gemini,
                "_new_storage_client",
                return_value=object(),
            ),
            unittest.mock.patch.object(
                rank_gemini.gcs_utils,
                "blob_exists",
                return_value=False,
            ) as blob_exists,
            unittest.mock.patch.object(
                rank_gemini.gcs_utils,
                "download_jsonl_manifest",
                side_effect=AssertionError("missing cache should not download"),
            ),
        ):
            cache = rank_gemini._load_cache("gs://bucket/cache.jsonl")

        self.assertEqual(cache, {})
        blob_exists.assert_called_once()

    def test_preflight_defaults_preview_model_and_respects_limit(self) -> None:
        import rank_gemini

        _FakeRunner.instances = []
        with tempfile.TemporaryDirectory() as tmpdir:
            paths = _paths(Path(tmpdir))
            _write_jsonl(
                paths["review_pool"],
                [_row("audio-a", 1), _row("audio-b", 2), _row("audio-c", 3)],
            )
            _write_jsonl(paths["cache"], [])
            captured: dict[str, object] = {}

            def fake_run_source_group_predictions(
                rows: list[dict[str, object]],
                cache_by_audio_id: dict[str, object],
                runner: _FakeRunner,
                **_kwargs: object,
            ) -> tuple[list[object], dict[str, object]]:
                captured["row_count"] = len(rows)
                captured["model_id"] = runner.model_id
                return [], cache_by_audio_id

            with (
                unittest.mock.patch.object(
                    rank_gemini.gemini_ranking,
                    "new_vertex_client",
                    return_value=object(),
                ),
                unittest.mock.patch.object(
                    rank_gemini.gemini_ranking,
                    "GeminiRankingRunner",
                    _FakeRunner,
                ),
                unittest.mock.patch.object(
                    rank_gemini.gemini_ranking,
                    "run_source_group_predictions",
                    fake_run_source_group_predictions,
                ),
                unittest.mock.patch.object(
                    rank_gemini.ranking,
                    "score_ranked_rows",
                    return_value=_fake_ranked_rows(),
                ),
            ):
                exit_code = rank_gemini.main(
                    [
                        "preflight",
                        *_required_args(paths),
                        "--project",
                        "test-project",
                        "--limit",
                        "3",
                    ]
                )

        self.assertEqual(exit_code, 0)
        self.assertEqual(captured["row_count"], 3)
        self.assertEqual(captured["model_id"], "gemini-3.1-pro-preview")

    def test_prediction_run_flushes_cache_entries_by_interval(self) -> None:
        import rank_gemini
        from common import ranking

        _FakeRunner.instances = []
        with tempfile.TemporaryDirectory() as tmpdir:
            paths = _paths(Path(tmpdir))
            _write_jsonl(paths["review_pool"], [_row("audio-a", 1)])
            _write_jsonl(paths["cache"], [])
            entry_a = ranking.PredictionCacheEntry(
                audio_segment_id="audio-a",
                prediction_text="Engine 41 copy",
                model_id="gemini-3.5-flash",
                prompt_fingerprint="prompt",
                context_policy_fingerprint="context-policy",
                num_recent_events=1000,
                context_fingerprint="context",
                source_group="source-a",
                row_index=1,
                model_ready_audio_uri="gs://bucket/audio-a.flac",
                created_at="2026-06-02T00:00:00Z",
                error="",
            )
            entry_b = dataclasses.replace(
                entry_a,
                audio_segment_id="audio-b",
                model_ready_audio_uri="gs://bucket/audio-b.flac",
            )
            flushed: list[list[str]] = []

            def fake_run_source_group_predictions(
                _rows: list[dict[str, object]],
                cache_by_audio_id: dict[str, object],
                _runner: _FakeRunner,
                **kwargs: object,
            ) -> tuple[list[object], dict[str, object]]:
                on_new_entry = kwargs["on_new_entry"]
                on_new_entry(entry_a)
                on_new_entry(entry_b)
                return [entry_a, entry_b], cache_by_audio_id

            def fake_append_cache_entries(
                _path: str,
                entries: list[ranking.PredictionCacheEntry],
            ) -> None:
                flushed.append([entry.audio_segment_id for entry in entries])

            with (
                unittest.mock.patch.object(
                    rank_gemini.gemini_ranking,
                    "new_vertex_client",
                    return_value=object(),
                ),
                unittest.mock.patch.object(
                    rank_gemini.gemini_ranking,
                    "GeminiRankingRunner",
                    _FakeRunner,
                ),
                unittest.mock.patch.object(
                    rank_gemini.gemini_ranking,
                    "run_source_group_predictions",
                    fake_run_source_group_predictions,
                ),
                unittest.mock.patch.object(
                    rank_gemini,
                    "_append_cache_entries",
                    fake_append_cache_entries,
                ),
                unittest.mock.patch.object(
                    rank_gemini.ranking,
                    "score_ranked_rows",
                    return_value=_fake_ranked_rows(),
                ),
            ):
                exit_code = rank_gemini.main(
                    [
                        "run",
                        *_required_args(paths),
                        "--project",
                        "test-project",
                        "--cache-flush-interval",
                        "1",
                    ]
                )

        self.assertEqual(exit_code, 0)
        self.assertEqual(flushed, [["audio-a"], ["audio-b"]])

    def test_prediction_run_passes_request_timeout_to_vertex_client(
        self,
    ) -> None:
        import rank_gemini

        _FakeRunner.instances = []
        with tempfile.TemporaryDirectory() as tmpdir:
            paths = _paths(Path(tmpdir))
            _write_jsonl(paths["review_pool"], [_row("audio-a", 1)])
            _write_jsonl(paths["cache"], [])
            captured: dict[str, object] = {}

            def fake_new_vertex_client(
                project: str,
                location: str,
                *,
                timeout_ms: int,
            ) -> object:
                captured["project"] = project
                captured["location"] = location
                captured["timeout_ms"] = timeout_ms
                return object()

            with (
                unittest.mock.patch.object(
                    rank_gemini.gemini_ranking,
                    "new_vertex_client",
                    fake_new_vertex_client,
                ),
                unittest.mock.patch.object(
                    rank_gemini.gemini_ranking,
                    "GeminiRankingRunner",
                    _FakeRunner,
                ),
                unittest.mock.patch.object(
                    rank_gemini.gemini_ranking,
                    "run_source_group_predictions",
                    return_value=([], {}),
                ),
                unittest.mock.patch.object(
                    rank_gemini.ranking,
                    "score_ranked_rows",
                    return_value=_fake_ranked_rows(),
                ),
            ):
                exit_code = rank_gemini.main(
                    [
                        "run",
                        *_required_args(paths),
                        "--project",
                        "test-project",
                        "--location",
                        "global",
                        "--request-timeout-ms",
                        "120000",
                    ]
                )

        self.assertEqual(exit_code, 0)
        self.assertEqual(captured["project"], "test-project")
        self.assertEqual(captured["location"], "global")
        self.assertEqual(captured["timeout_ms"], 120000)

    def test_prediction_run_passes_source_workers_to_runner(self) -> None:
        import rank_gemini

        _FakeRunner.instances = []
        with tempfile.TemporaryDirectory() as tmpdir:
            paths = _paths(Path(tmpdir))
            _write_jsonl(paths["review_pool"], [_row("audio-a", 1)])
            _write_jsonl(paths["cache"], [])
            captured: dict[str, object] = {}

            def fake_run_source_group_predictions(
                _rows: list[dict[str, object]],
                cache_by_audio_id: dict[str, object],
                _runner: _FakeRunner,
                **kwargs: object,
            ) -> tuple[list[object], dict[str, object]]:
                captured["max_source_workers"] = kwargs["max_source_workers"]
                return [], cache_by_audio_id

            with (
                unittest.mock.patch.object(
                    rank_gemini.gemini_ranking,
                    "new_vertex_client",
                    return_value=object(),
                ),
                unittest.mock.patch.object(
                    rank_gemini.gemini_ranking,
                    "GeminiRankingRunner",
                    _FakeRunner,
                ),
                unittest.mock.patch.object(
                    rank_gemini.gemini_ranking,
                    "run_source_group_predictions",
                    fake_run_source_group_predictions,
                ),
                unittest.mock.patch.object(
                    rank_gemini.ranking,
                    "score_ranked_rows",
                    return_value=_fake_ranked_rows(),
                ),
            ):
                exit_code = rank_gemini.main(
                    [
                        "run",
                        *_required_args(paths),
                        "--project",
                        "test-project",
                        "--source-workers",
                        "8",
                    ]
                )

        self.assertEqual(exit_code, 0)
        self.assertEqual(captured["max_source_workers"], 8)

    def test_run_defaults_full_model(self) -> None:
        import rank_gemini

        _FakeRunner.instances = []
        with tempfile.TemporaryDirectory() as tmpdir:
            paths = _paths(Path(tmpdir))
            _write_jsonl(paths["review_pool"], [_row("audio-a", 1)])
            _write_jsonl(paths["cache"], [])
            captured: dict[str, object] = {}

            def fake_run_source_group_predictions(
                rows: list[dict[str, object]],
                cache_by_audio_id: dict[str, object],
                runner: _FakeRunner,
                **_kwargs: object,
            ) -> tuple[list[object], dict[str, object]]:
                captured["model_id"] = runner.model_id
                return [], cache_by_audio_id

            with (
                unittest.mock.patch.object(
                    rank_gemini.gemini_ranking,
                    "new_vertex_client",
                    return_value=object(),
                ),
                unittest.mock.patch.object(
                    rank_gemini.gemini_ranking,
                    "GeminiRankingRunner",
                    _FakeRunner,
                ),
                unittest.mock.patch.object(
                    rank_gemini.gemini_ranking,
                    "run_source_group_predictions",
                    fake_run_source_group_predictions,
                ),
                unittest.mock.patch.object(
                    rank_gemini.ranking,
                    "score_ranked_rows",
                    return_value=_fake_ranked_rows(),
                ),
            ):
                exit_code = rank_gemini.main(
                    [
                        "run",
                        *_required_args(paths),
                        "--project",
                        "test-project",
                    ]
                )

        self.assertEqual(exit_code, 0)
        self.assertEqual(captured["model_id"], "gemini-3.5-flash")


if __name__ == "__main__":
    unittest.main()
