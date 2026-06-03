"""Tests for the ADK Gemini ranking CLI."""

from __future__ import annotations

import contextlib
import dataclasses
import io
import pathlib
import sys
import unittest
import unittest.mock

_REVIEW_DIR = str(pathlib.Path(__file__).resolve().parent.parent)
_COLABS_DIR = str(
    pathlib.Path(__file__).resolve().parent.parent.parent.parent / "colabs"
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
        "dataset_name": "test",
        "text": "Engine 41 copy",
    }


def _required_args() -> list[str]:
    return [
        "--review-pool-jsonl",
        "gs://bucket/review_pool.jsonl",
        "--prediction-cache-jsonl",
        "gs://bucket/cache.jsonl",
        "--ranked-jsonl",
        "gs://bucket/ranked.jsonl",
        "--ranked-csv",
        "gs://bucket/ranked.csv",
        "--excluded-jsonl",
        "gs://bucket/excluded.jsonl",
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


def _entry(
    ranking: object,
    audio_segment_id: str,
    prediction_text: str,
    *,
    row_index: int,
    context_fp: str,
    error: str = "",
) -> object:
    return ranking.PredictionCacheEntry(
        audio_segment_id=audio_segment_id,
        prediction_text=prediction_text,
        model_id="gemini-3.5-flash",
        prompt_fingerprint=ranking.adk_prompt_fingerprint(
            "system",
            "user",
        ),
        context_policy_fingerprint=ranking.context_policy_fingerprint(),
        num_recent_events=ranking.NUM_RECENT_EVENTS,
        context_fingerprint=context_fp,
        source_group="source-a",
        row_index=row_index,
        model_ready_audio_uri=f"gs://bucket/{audio_segment_id}.flac",
        created_at="2026-06-02T00:00:00Z",
        error=error,
    )


class _FakeRunner:
    instances: list[_FakeRunner] = []

    def __init__(
        self,
        *,
        project: str,
        location: str,
        model_id: str,
        request_timeout_ms: int,
        **_kwargs: object,
    ) -> None:
        self.project = project
        self.location = location
        self.model_id = model_id
        self.request_timeout_ms = request_timeout_ms
        self.instances.append(self)


class TestRankGeminiCli(unittest.TestCase):
    def test_progress_heartbeat_emits_status_line(self) -> None:
        import rank_gemini
        from common import ranking

        stream = io.StringIO()
        heartbeat = rank_gemini._ProgressHeartbeat(
            total_rows=10,
            interval_seconds=900,
            stream=stream,
            clock=lambda: 1_000.0,
        )

        entry = ranking.PredictionCacheEntry(
            audio_segment_id="audio-a",
            prediction_text="",
            model_id="gemini-3.5-flash",
            prompt_fingerprint="prompt",
            context_policy_fingerprint="context-policy",
            num_recent_events=60,
            context_fingerprint="context",
            source_group="source-a",
            row_index=1,
            model_ready_audio_uri="gs://bucket/audio-a.flac",
            created_at="2026-06-02T00:00:00Z",
            error="ResourceExhausted: 429 quota exceeded",
        )
        heartbeat.record_processed(entry=entry, cache_hit=False)
        heartbeat.record_flush(1)
        heartbeat.emit_once("heartbeat")

        line = stream.getvalue()
        self.assertIn("review-ranking progress", line)
        self.assertIn("status=heartbeat", line)
        self.assertIn("processed=1/10", line)
        self.assertIn("cache_rows=1", line)
        self.assertIn("flushes=1", line)
        self.assertIn("final_failures=1", line)
        self.assertIn("quota_failures=1", line)

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

    def test_preflight_help_uses_sample_size_not_limit(self) -> None:
        import rank_gemini

        output = io.StringIO()
        with self.assertRaises(SystemExit) as ctx:
            with contextlib.redirect_stdout(output):
                rank_gemini.main(["preflight", "--help"])

        self.assertEqual(ctx.exception.code, 0)
        help_text = output.getvalue()
        self.assertIn("--sample-size", help_text)
        self.assertNotIn("--limit", help_text)

    def test_run_help_has_source_workers_but_no_limit(self) -> None:
        import rank_gemini

        output = io.StringIO()
        with self.assertRaises(SystemExit) as ctx:
            with contextlib.redirect_stdout(output):
                rank_gemini.main(["run", "--help"])

        self.assertEqual(ctx.exception.code, 0)
        help_text = output.getvalue()
        self.assertNotIn("--limit", help_text)
        self.assertIn("--source-workers", help_text)

    def test_local_shared_artifact_paths_are_rejected(self) -> None:
        import rank_gemini

        with self.assertRaises(SystemExit):
            rank_gemini.main(
                [
                    "rank-cache",
                    "--review-pool-jsonl",
                    "local.jsonl",
                    "--prediction-cache-jsonl",
                    "gs://bucket/cache.jsonl",
                    "--ranked-jsonl",
                    "gs://bucket/ranked.jsonl",
                    "--ranked-csv",
                    "gs://bucket/ranked.csv",
                    "--excluded-jsonl",
                    "gs://bucket/excluded.jsonl",
                ]
            )

    def test_rank_cache_writes_outputs_without_adk_runner(self) -> None:
        import rank_gemini
        from common import ranking

        row = _row("audio-a", 1)
        entry = dataclasses.replace(
            _entry(
                ranking,
                "audio-a",
                "Engine 42 copy",
                row_index=1,
                context_fp=ranking.context_fingerprint([]),
            ),
            prompt_fingerprint=ranking.adk_prompt_fingerprint(
                rank_gemini.prompts.GEMINI_TRANSCRIBE_SYSTEM_PROMPT,
                rank_gemini.prompts.GEMINI_TRANSCRIBE_USER_PROMPT,
            ),
        )
        written: list[str] = []

        with (
            unittest.mock.patch.object(
                rank_gemini,
                "_path_exists",
                return_value=False,
            ),
            unittest.mock.patch.object(
                rank_gemini,
                "_load_jsonl",
                return_value=[row],
            ),
            unittest.mock.patch.object(
                rank_gemini,
                "_load_cache_entries",
                return_value=[entry],
            ),
            unittest.mock.patch.object(
                rank_gemini.adk_ranking,
                "AdkRankingRunner",
                side_effect=AssertionError("runner should not be used"),
            ),
            unittest.mock.patch.object(
                rank_gemini.ranking,
                "score_ranked_rows",
                return_value=_fake_ranked_rows(),
            ),
            unittest.mock.patch.object(
                rank_gemini,
                "_write_jsonl",
                side_effect=lambda path, _rows: written.append(path),
            ),
            unittest.mock.patch.object(
                rank_gemini,
                "_write_ranked_csv",
                side_effect=lambda path, _rows: written.append(path),
            ),
        ):
            exit_code = rank_gemini.main(["rank-cache", *_required_args()])

        self.assertEqual(exit_code, 0)
        self.assertEqual(
            written,
            [
                "gs://bucket/ranked.jsonl",
                "gs://bucket/ranked.csv",
                "gs://bucket/excluded.jsonl",
            ],
        )

    def test_rank_cache_fails_when_cache_coverage_is_incomplete(self) -> None:
        import rank_gemini

        with (
            unittest.mock.patch.object(
                rank_gemini,
                "_path_exists",
                return_value=False,
            ),
            unittest.mock.patch.object(
                rank_gemini,
                "_load_jsonl",
                return_value=[_row("audio-a", 1)],
            ),
            unittest.mock.patch.object(
                rank_gemini,
                "_load_cache_entries",
                return_value=[],
            ),
            unittest.mock.patch.object(
                rank_gemini,
                "_write_jsonl",
                side_effect=AssertionError("must not write"),
            ),
        ):
            with self.assertRaisesRegex(ValueError, "cache is incomplete"):
                rank_gemini.main(["rank-cache", *_required_args()])

    def test_preflight_defaults_flash_lite_and_respects_sample_size(
        self,
    ) -> None:
        import rank_gemini

        _FakeRunner.instances = []
        captured: dict[str, object] = {}

        def fake_run_source_group_predictions_adk(
            rows: list[dict[str, object]],
            cache_entries: list[object],
            runner: _FakeRunner | None,
            **kwargs: object,
        ) -> tuple[list[object], dict[str, object]]:
            captured["row_count"] = len(rows)
            captured["cache_entries"] = cache_entries
            captured["source_workers"] = kwargs["source_workers"]
            created_runner = kwargs["runner_factory"]()
            captured["model_id"] = created_runner.model_id
            captured["runner"] = runner
            return [], {}

        with (
            unittest.mock.patch.object(
                rank_gemini,
                "_path_exists",
                return_value=False,
            ),
            unittest.mock.patch.object(
                rank_gemini,
                "_load_jsonl",
                return_value=[
                    _row("audio-a", 1),
                    _row("audio-b", 2),
                    _row("audio-c", 3),
                ],
            ),
            unittest.mock.patch.object(
                rank_gemini,
                "_load_cache_entries",
                return_value=[],
            ),
            unittest.mock.patch.object(
                rank_gemini.adk_ranking,
                "AdkRankingRunner",
                _FakeRunner,
            ),
            unittest.mock.patch.object(
                rank_gemini.adk_ranking,
                "run_source_group_predictions_adk",
                fake_run_source_group_predictions_adk,
            ),
            unittest.mock.patch.object(
                rank_gemini.ranking,
                "score_ranked_rows",
                return_value=_fake_ranked_rows(),
            ),
            unittest.mock.patch.object(rank_gemini, "_write_jsonl"),
            unittest.mock.patch.object(rank_gemini, "_write_ranked_csv"),
        ):
            exit_code = rank_gemini.main(
                [
                    "preflight",
                    *_required_args(),
                    "--project",
                    "test-project",
                    "--sample-size",
                    "2",
                ]
            )

        self.assertEqual(exit_code, 0)
        self.assertEqual(captured["row_count"], 2)
        self.assertEqual(captured["source_workers"], 16)
        self.assertEqual(captured["model_id"], "gemini-3.1-flash-lite")
        self.assertIsNone(captured["runner"])

    def test_run_processes_all_rows_and_passes_timeout(self) -> None:
        import rank_gemini

        _FakeRunner.instances = []
        captured: dict[str, object] = {}

        def fake_run_source_group_predictions_adk(
            rows: list[dict[str, object]],
            _cache_entries: list[object],
            runner: _FakeRunner | None,
            **kwargs: object,
        ) -> tuple[list[object], dict[str, object]]:
            captured["row_count"] = len(rows)
            captured["source_workers"] = kwargs["source_workers"]
            created_runner = kwargs["runner_factory"]()
            captured["model_id"] = created_runner.model_id
            captured["retry_errors"] = kwargs["retry_errors"]
            captured["runner"] = runner
            return [], {}

        with (
            unittest.mock.patch.object(
                rank_gemini,
                "_path_exists",
                return_value=False,
            ),
            unittest.mock.patch.object(
                rank_gemini,
                "_load_jsonl",
                return_value=[
                    _row("audio-a", 1),
                    _row("audio-b", 2),
                    _row("audio-c", 3),
                ],
            ),
            unittest.mock.patch.object(
                rank_gemini,
                "_load_cache_entries",
                return_value=[],
            ),
            unittest.mock.patch.object(
                rank_gemini.adk_ranking,
                "AdkRankingRunner",
                _FakeRunner,
            ),
            unittest.mock.patch.object(
                rank_gemini.adk_ranking,
                "run_source_group_predictions_adk",
                fake_run_source_group_predictions_adk,
            ),
            unittest.mock.patch.object(
                rank_gemini.ranking,
                "score_ranked_rows",
                return_value=_fake_ranked_rows(),
            ),
            unittest.mock.patch.object(rank_gemini, "_write_jsonl"),
            unittest.mock.patch.object(rank_gemini, "_write_ranked_csv"),
        ):
            exit_code = rank_gemini.main(
                [
                    "run",
                    *_required_args(),
                    "--project",
                    "test-project",
                    "--location",
                    "global",
                    "--request-timeout-ms",
                    "120000",
                    "--retry-errors",
                ]
            )

        self.assertEqual(exit_code, 0)
        self.assertEqual(captured["row_count"], 3)
        self.assertEqual(captured["source_workers"], 16)
        self.assertEqual(captured["model_id"], "gemini-3.5-flash")
        self.assertTrue(captured["retry_errors"])
        self.assertEqual(_FakeRunner.instances[0].project, "test-project")
        self.assertEqual(_FakeRunner.instances[0].location, "global")
        self.assertEqual(_FakeRunner.instances[0].request_timeout_ms, 120000)
        self.assertIsNone(captured["runner"])

    def test_prediction_run_flushes_cache_entries_by_interval(self) -> None:
        import rank_gemini
        from common import ranking

        entry_a = ranking.PredictionCacheEntry(
            audio_segment_id="audio-a",
            prediction_text="Engine 41 copy",
            model_id="gemini-3.5-flash",
            prompt_fingerprint="prompt",
            context_policy_fingerprint="context-policy",
            num_recent_events=60,
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

        def fake_run_source_group_predictions_adk(
            _rows: list[dict[str, object]],
            _cache_entries: list[object],
            _runner: _FakeRunner | None,
            **kwargs: object,
        ) -> tuple[list[object], dict[str, object]]:
            on_new_entry = kwargs["on_new_entry"]
            on_new_entry(entry_a)
            on_new_entry(entry_b)
            return [entry_a, entry_b], {}

        def fake_write_jsonl(
            path: str,
            rows: list[dict[str, object]],
        ) -> None:
            if path == "gs://bucket/cache.jsonl":
                flushed.append([str(row["audio_segment_id"]) for row in rows])

        with (
            unittest.mock.patch.object(
                rank_gemini,
                "_path_exists",
                return_value=False,
            ),
            unittest.mock.patch.object(
                rank_gemini,
                "_load_jsonl",
                return_value=[_row("audio-a", 1)],
            ),
            unittest.mock.patch.object(
                rank_gemini,
                "_load_cache_entries",
                return_value=[],
            ),
            unittest.mock.patch.object(
                rank_gemini.adk_ranking,
                "AdkRankingRunner",
                _FakeRunner,
            ),
            unittest.mock.patch.object(
                rank_gemini.adk_ranking,
                "run_source_group_predictions_adk",
                fake_run_source_group_predictions_adk,
            ),
            unittest.mock.patch.object(
                rank_gemini,
                "_write_jsonl",
                fake_write_jsonl,
            ),
            unittest.mock.patch.object(
                rank_gemini.ranking,
                "score_ranked_rows",
                return_value=_fake_ranked_rows(),
            ),
            unittest.mock.patch.object(rank_gemini, "_write_ranked_csv"),
        ):
            exit_code = rank_gemini.main(
                [
                    "run",
                    *_required_args(),
                    "--project",
                    "test-project",
                    "--cache-flush-interval",
                    "1",
                ]
            )

        self.assertEqual(exit_code, 0)
        self.assertEqual(flushed, [["audio-a"], ["audio-a", "audio-b"]])

    def test_source_workers_argument_is_forwarded(self) -> None:
        import rank_gemini

        captured: dict[str, object] = {}

        def fake_run_source_group_predictions_adk(
            _rows: list[dict[str, object]],
            _cache_entries: list[object],
            runner: _FakeRunner | None,
            **kwargs: object,
        ) -> tuple[list[object], dict[str, object]]:
            captured["runner"] = runner
            captured["source_workers"] = kwargs["source_workers"]
            return [], {}

        with (
            unittest.mock.patch.object(
                rank_gemini,
                "_path_exists",
                return_value=False,
            ),
            unittest.mock.patch.object(
                rank_gemini,
                "_load_jsonl",
                return_value=[_row("audio-a", 1)],
            ),
            unittest.mock.patch.object(
                rank_gemini,
                "_load_cache_entries",
                return_value=[],
            ),
            unittest.mock.patch.object(
                rank_gemini.adk_ranking,
                "AdkRankingRunner",
                _FakeRunner,
            ),
            unittest.mock.patch.object(
                rank_gemini.adk_ranking,
                "run_source_group_predictions_adk",
                fake_run_source_group_predictions_adk,
            ),
            unittest.mock.patch.object(
                rank_gemini.ranking,
                "score_ranked_rows",
                return_value=_fake_ranked_rows(),
            ),
            unittest.mock.patch.object(rank_gemini, "_write_jsonl"),
            unittest.mock.patch.object(rank_gemini, "_write_ranked_csv"),
        ):
            exit_code = rank_gemini.main(
                [
                    "run",
                    *_required_args(),
                    "--project",
                    "test-project",
                    "--source-workers",
                    "64",
                ]
            )

        self.assertEqual(exit_code, 0)
        self.assertEqual(captured["source_workers"], 64)
        self.assertIsNone(captured["runner"])

    def test_existing_final_outputs_fail_before_loading_cache(self) -> None:
        import rank_gemini

        with (
            unittest.mock.patch.object(
                rank_gemini,
                "_path_exists",
                side_effect=lambda path: path == "gs://bucket/ranked.jsonl",
            ),
            unittest.mock.patch.object(
                rank_gemini,
                "_load_jsonl",
                side_effect=AssertionError("must not read"),
            ),
        ):
            with self.assertRaises(FileExistsError):
                rank_gemini.main(["run", *_required_args(), "--project", "p"])

    def test_partial_inference_failure_flushes_cache_but_writes_no_outputs(
        self,
    ) -> None:
        import rank_gemini
        from common import ranking

        entry = ranking.PredictionCacheEntry(
            audio_segment_id="audio-a",
            prediction_text="copy",
            model_id="gemini-3.5-flash",
            prompt_fingerprint="prompt",
            context_policy_fingerprint="context-policy",
            num_recent_events=60,
            context_fingerprint="context",
            source_group="source-a",
            row_index=1,
            model_ready_audio_uri="gs://bucket/audio-a.flac",
            created_at="2026-06-02T00:00:00Z",
            error="",
        )
        flushed: list[str] = []

        def fake_run_source_group_predictions_adk(
            _rows: list[dict[str, object]],
            _cache_entries: list[object],
            _runner: _FakeRunner | None,
            **kwargs: object,
        ) -> tuple[list[object], dict[str, object]]:
            kwargs["on_new_entry"](entry)
            raise RuntimeError("fatal")

        def fake_write_jsonl(
            path: str,
            rows: list[dict[str, object]],
        ) -> None:
            if path == "gs://bucket/cache.jsonl":
                flushed.extend(str(row["audio_segment_id"]) for row in rows)
                return
            raise AssertionError("must not write final outputs")

        with (
            unittest.mock.patch.object(
                rank_gemini,
                "_path_exists",
                return_value=False,
            ),
            unittest.mock.patch.object(
                rank_gemini,
                "_load_jsonl",
                return_value=[_row("audio-a", 1)],
            ),
            unittest.mock.patch.object(
                rank_gemini,
                "_load_cache_entries",
                return_value=[],
            ),
            unittest.mock.patch.object(
                rank_gemini.adk_ranking,
                "AdkRankingRunner",
                _FakeRunner,
            ),
            unittest.mock.patch.object(
                rank_gemini.adk_ranking,
                "run_source_group_predictions_adk",
                fake_run_source_group_predictions_adk,
            ),
            unittest.mock.patch.object(
                rank_gemini,
                "_write_jsonl",
                fake_write_jsonl,
            ),
        ):
            with self.assertRaisesRegex(RuntimeError, "fatal"):
                rank_gemini.main(["run", *_required_args(), "--project", "p"])

        self.assertEqual(flushed, ["audio-a"])


if __name__ == "__main__":
    unittest.main()
