"""Tests for ADK review-ranking prediction helpers."""

from __future__ import annotations

import asyncio
import dataclasses
import unittest


def _row(
    audio_segment_id: str,
    row_index: int,
    *,
    source_group: str = "source-a",
) -> dict[str, object]:
    return {
        "audio_segment_id": audio_segment_id,
        "source_window_id": f"source-window-{audio_segment_id}",
        "model_ready_audio_uri": f"gs://bucket/{audio_segment_id}.flac",
        "original_audio_uri": "gs://bucket/original.wav",
        "offset": row_index * 1.0,
        "duration": 2.5,
        "source_group": source_group,
        "row_index": row_index,
        "dataset_name": "test",
        "text": "Engine 41 copy",
    }


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
        prompt_fingerprint="prompt-fp",
        context_policy_fingerprint="context-policy-fp",
        num_recent_events=60,
        context_fingerprint=context_fp,
        source_group="source-a",
        row_index=row_index,
        model_ready_audio_uri=f"gs://bucket/{audio_segment_id}.flac",
        created_at="2026-06-02T00:00:00Z",
        error=error,
    )


class _FakePart:
    def __init__(self, text: str | None) -> None:
        self.text = text


class _FakeContent:
    def __init__(self, parts: list[_FakePart]) -> None:
        self.parts = parts


class _FakeEvent:
    def __init__(self, text: str, *, final: bool = True) -> None:
        self.content = _FakeContent([_FakePart(text)])
        self._final = final

    def is_final_response(self) -> bool:
        return self._final


@dataclasses.dataclass
class _FakeSession:
    source_group: str
    session_id: str


class _FakeRunner:
    def __init__(self, outputs: list[object]) -> None:
        self.model_id = "gemini-3.5-flash"
        self.outputs = outputs
        self.started: list[str] = []
        self.appended: list[tuple[str, str]] = []
        self.predicted: list[str] = []

    def start_session(self, source_group: str) -> _FakeSession:
        session = _FakeSession(source_group, f"session-{len(self.started)}")
        self.started.append(session.session_id)
        return session

    def append_success(
        self,
        _session: _FakeSession,
        row: dict[str, object],
        entry: object,
    ) -> None:
        self.appended.append(
            (str(row["audio_segment_id"]), entry.prediction_text)
        )

    def predict_one(
        self,
        _session: _FakeSession,
        row: dict[str, object],
    ) -> str:
        self.predicted.append(str(row["audio_segment_id"]))
        output = self.outputs.pop(0)
        if isinstance(output, Exception):
            raise output
        return str(output)


class _SlowStream:
    def __init__(self) -> None:
        self.closed = False

    def __aiter__(self) -> "_SlowStream":
        return self

    async def __anext__(self) -> object:
        await asyncio.sleep(60)
        raise StopAsyncIteration

    async def aclose(self) -> None:
        self.closed = True


class _FakeAdkRunner:
    def __init__(self, stream: _SlowStream) -> None:
        self.stream = stream

    def run_async(self, **_kwargs: object) -> _SlowStream:
        return self.stream


class _LoopRecordingStream:
    def __init__(self, loops: list[asyncio.AbstractEventLoop]) -> None:
        self.loops = loops
        self._yielded = False

    def __aiter__(self) -> "_LoopRecordingStream":
        return self

    async def __anext__(self) -> object:
        if self._yielded:
            raise StopAsyncIteration
        self._yielded = True
        self.loops.append(asyncio.get_running_loop())
        return _FakeEvent("copy")


class _LoopRecordingAdkRunner:
    def __init__(self) -> None:
        self.loops: list[asyncio.AbstractEventLoop] = []

    def run_async(self, **_kwargs: object) -> _LoopRecordingStream:
        return _LoopRecordingStream(self.loops)


class TestAdkPredictionExtraction(unittest.TestCase):
    def test_extract_final_response_text_uses_final_events_only(self) -> None:
        from common import adk_ranking

        text = adk_ranking.extract_final_response_text(
            [
                _FakeEvent("partial", final=False),
                _FakeEvent(" copy "),
                _FakeEvent("that"),
            ]
        )

        self.assertEqual(text, "copy that")

    def test_run_events_closes_stream_on_timeout(self) -> None:
        from common import adk_ranking

        stream = _SlowStream()
        runner = object.__new__(adk_ranking.AdkRankingRunner)
        runner._runner = _FakeAdkRunner(stream)
        runner._run_config = object()
        runner.user_id = "review-ranking"
        runner.request_timeout_ms = 1

        with self.assertRaises(asyncio.TimeoutError):
            runner._run_events("session-a", object())

        self.assertTrue(stream.closed)

    def test_run_events_reuses_runner_event_loop(self) -> None:
        from common import adk_ranking

        adk_runner = _LoopRecordingAdkRunner()
        runner = object.__new__(adk_ranking.AdkRankingRunner)
        runner._runner = adk_runner
        runner._run_config = object()
        runner.user_id = "review-ranking"
        runner.request_timeout_ms = None

        try:
            runner._run_events("session-a", object())
            runner._run_events("session-a", object())
        finally:
            close = getattr(runner, "close", None)
            if close is not None:
                close()

        self.assertEqual(len(adk_runner.loops), 2)
        self.assertIs(adk_runner.loops[0], adk_runner.loops[1])

    def test_generation_config_uses_low_thinking_and_native_retry(self) -> None:
        from common import adk_ranking

        config = adk_ranking._generation_config()

        self.assertEqual(config.thinking_config.thinking_level.value, "LOW")
        retry_options = config.http_options.retry_options
        self.assertEqual(retry_options.attempts, 5)
        self.assertEqual(retry_options.initial_delay, 1.0)
        self.assertEqual(retry_options.max_delay, 60.0)
        self.assertEqual(retry_options.exp_base, 2.0)


class TestSourceGroupPredictions(unittest.TestCase):
    def test_cache_hit_replays_linked_prior_success(self) -> None:
        from common import adk_ranking, ranking

        first_row = _row("audio-a", 1)
        second_row = _row("audio-b", 2)
        empty_context = ranking.context_fingerprint([])
        first_entry = _entry(
            ranking,
            "audio-a",
            "first prediction",
            row_index=1,
            context_fp=empty_context,
        )
        second_context = ranking.context_fingerprint([first_entry])
        runner = _FakeRunner(["second prediction"])

        new_entries, final_cache = adk_ranking.run_source_group_predictions_adk(
            [first_row, second_row],
            [first_entry],
            runner,
            prompt_fp="prompt-fp",
            context_policy_fp="context-policy-fp",
            created_at="2026-06-03T00:00:00Z",
        )

        self.assertEqual(runner.appended, [("audio-a", "first prediction")])
        self.assertEqual(runner.predicted, ["audio-b"])
        self.assertEqual(new_entries[0].context_fingerprint, second_context)
        self.assertEqual(final_cache["audio-a"], first_entry)
        self.assertEqual(
            final_cache["audio-b"].prediction_text, "second prediction"
        )

    def test_exception_retry_recreates_session_before_success(self) -> None:
        from common import adk_ranking

        runner = _FakeRunner([RuntimeError("timeout"), "copy"])

        new_entries, final_cache = adk_ranking.run_source_group_predictions_adk(
            [_row("audio-a", 1)],
            [],
            runner,
            prompt_fp="prompt-fp",
            context_policy_fp="context-policy-fp",
            created_at="2026-06-03T00:00:00Z",
        )

        self.assertEqual(runner.started, ["session-0", "session-1"])
        self.assertEqual(new_entries[0].prediction_text, "copy")
        self.assertEqual(new_entries[0].error, "")
        self.assertEqual(final_cache["audio-a"].prediction_text, "copy")

    def test_exhausted_exception_retry_records_failed_prediction(self) -> None:
        from common import adk_ranking

        runner = _FakeRunner(
            [
                RuntimeError("first"),
                RuntimeError("second"),
                RuntimeError("third"),
            ]
        )

        new_entries, final_cache = adk_ranking.run_source_group_predictions_adk(
            [_row("audio-a", 1)],
            [],
            runner,
            prompt_fp="prompt-fp",
            context_policy_fp="context-policy-fp",
            created_at="2026-06-03T00:00:00Z",
            max_attempts=3,
        )

        self.assertIn("RuntimeError", new_entries[0].error)
        self.assertEqual(final_cache["audio-a"].error, new_entries[0].error)
        self.assertEqual(runner.predicted, ["audio-a", "audio-a", "audio-a"])

    def test_empty_prediction_is_retried_then_recorded_as_success(
        self,
    ) -> None:
        from common import adk_ranking

        runner = _FakeRunner(["", ""])

        new_entries, final_cache = adk_ranking.run_source_group_predictions_adk(
            [_row("audio-a", 1)],
            [],
            runner,
            prompt_fp="prompt-fp",
            context_policy_fp="context-policy-fp",
            created_at="2026-06-03T00:00:00Z",
        )

        self.assertEqual(new_entries[0].prediction_text, "")
        self.assertEqual(new_entries[0].error, "")
        self.assertEqual(final_cache["audio-a"].prediction_text, "")

    def test_retry_replays_only_bounded_prior_successes(self) -> None:
        from common import adk_ranking

        rows = [_row(f"audio-{index}", index) for index in range(1, 37)]
        outputs = [f"prediction-{index}" for index in range(1, 36)]
        outputs.extend([RuntimeError("timeout"), "prediction-36"])
        runner = _FakeRunner(outputs)

        adk_ranking.run_source_group_predictions_adk(
            rows,
            [],
            runner,
            prompt_fp="prompt-fp",
            context_policy_fp="context-policy-fp",
            created_at="2026-06-03T00:00:00Z",
            num_recent_events=60,
        )

        self.assertEqual(
            runner.appended,
            [
                (f"audio-{index}", f"prediction-{index}")
                for index in range(6, 36)
            ],
        )

    def test_cached_failure_is_skipped_unless_retry_errors(self) -> None:
        from common import adk_ranking, ranking

        row = _row("audio-a", 1)
        failed = _entry(
            ranking,
            "audio-a",
            "",
            row_index=1,
            context_fp=ranking.context_fingerprint([]),
            error="timeout",
        )

        skip_runner = _FakeRunner(["should not run"])
        new_entries, final_cache = adk_ranking.run_source_group_predictions_adk(
            [row],
            [failed],
            skip_runner,
            prompt_fp="prompt-fp",
            context_policy_fp="context-policy-fp",
            created_at="2026-06-03T00:00:00Z",
            retry_errors=False,
        )

        self.assertEqual(new_entries, [])
        self.assertEqual(final_cache["audio-a"], failed)
        self.assertEqual(skip_runner.predicted, [])

        retry_runner = _FakeRunner(["copy"])
        retry_entries, retry_cache = (
            adk_ranking.run_source_group_predictions_adk(
                [row],
                [failed],
                retry_runner,
                prompt_fp="prompt-fp",
                context_policy_fp="context-policy-fp",
                created_at="2026-06-03T00:00:00Z",
                retry_errors=True,
            )
        )

        self.assertEqual(retry_entries[0].prediction_text, "copy")
        self.assertEqual(retry_cache["audio-a"].prediction_text, "copy")


if __name__ == "__main__":
    unittest.main()
