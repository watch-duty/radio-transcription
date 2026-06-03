"""Tests for Phase 2 contextual Gemini ranking prediction helpers."""

from __future__ import annotations

import json
import unittest


def _row(
    audio_segment_id: str,
    row_index: int,
    text: str = "GOLDEN SHOULD NOT APPEAR",
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
    context_fp: str = "context-fp",
) -> object:
    return ranking.PredictionCacheEntry(
        audio_segment_id=audio_segment_id,
        prediction_text=prediction_text,
        model_id="gemini-3.5-flash",
        prompt_fingerprint="prompt-fp",
        context_policy_fingerprint="context-policy-fp",
        num_recent_events=1000,
        context_fingerprint=context_fp,
        source_group="source-a",
        row_index=row_index,
        model_ready_audio_uri=f"gs://bucket/{audio_segment_id}.flac",
        created_at="2026-06-02T00:00:00Z",
        error="",
    )


class _FakeGenerateContentConfig:
    def __init__(self, **kwargs: object) -> None:
        self.kwargs = kwargs


class _FakeTypes:
    GenerateContentConfig = _FakeGenerateContentConfig

    class HttpOptions:
        def __init__(self, **kwargs: object) -> None:
            self.kwargs = kwargs


class _FakeResponse:
    def __init__(self, text: str) -> None:
        self.text = text


class _FakeModels:
    def __init__(self, response_text: str = "generated prediction") -> None:
        self.calls: list[dict[str, object]] = []
        self.response_text = response_text

    def generate_content(self, **kwargs: object) -> _FakeResponse:
        self.calls.append(kwargs)
        return _FakeResponse(self.response_text)


class _FakeClient:
    def __init__(self, response_text: str = "generated prediction") -> None:
        self.models = _FakeModels(response_text=response_text)


class _RecordingRunner:
    def __init__(self) -> None:
        self.calls: list[tuple[str, list[str]]] = []

    def predict_one(
        self,
        row: dict[str, object],
        context_entries: list[object],
    ) -> str:
        self.calls.append(
            (
                str(row["audio_segment_id"]),
                [entry.prediction_text for entry in context_entries],
            )
        )
        return f"generated {row['audio_segment_id']}"


class _FailingRunner:
    model_id = "gemini-3.5-flash"

    def __init__(self) -> None:
        self.calls: list[tuple[str, list[str]]] = []

    def predict_one(
        self,
        row: dict[str, object],
        context_entries: list[object],
    ) -> str:
        audio_segment_id = str(row["audio_segment_id"])
        self.calls.append(
            (
                audio_segment_id,
                [entry.prediction_text for entry in context_entries],
            )
        )
        if audio_segment_id == "audio-b":
            raise RuntimeError("transient model failure")
        return f"generated {audio_segment_id}"


class TestContextualContents(unittest.TestCase):
    def test_build_contextual_contents_uses_prior_prediction_turns(
        self,
    ) -> None:
        from common import gemini_ranking
        from common import prompts
        from common import ranking

        context_entries = [
            _entry(ranking, "audio-a", "prior prediction", row_index=1)
        ]

        contents = gemini_ranking.build_contextual_contents(
            _row("audio-b", 2),
            context_entries,
        )

        self.assertEqual(contents[0]["role"], "user")
        self.assertEqual(
            contents[0]["parts"][0]["file_data"]["file_uri"],
            "gs://bucket/audio-a.flac",
        )
        self.assertEqual(
            contents[0]["parts"][0]["file_data"]["mime_type"],
            "audio/flac",
        )
        self.assertEqual(
            contents[1],
            {"role": "model", "parts": [{"text": "prior prediction"}]},
        )
        self.assertEqual(contents[-1]["role"], "user")
        self.assertEqual(
            contents[-1]["parts"][0]["text"],
            prompts.GEMINI_TRANSCRIBE_USER_PROMPT,
        )
        self.assertEqual(
            contents[-1]["parts"][1]["file_data"]["file_uri"],
            "gs://bucket/audio-b.flac",
        )

    def test_golden_reference_text_never_appears_in_contents(self) -> None:
        from common import gemini_ranking

        contents = gemini_ranking.build_contextual_contents(
            _row("audio-b", 2, text="GOLDEN SHOULD NOT APPEAR"),
            [],
        )

        serialized = json.dumps(contents)
        self.assertNotIn("GOLDEN SHOULD NOT APPEAR", serialized)


class TestGeminiRankingRunner(unittest.TestCase):
    def setUp(self) -> None:
        from common import gemini_ranking

        self.original_types = gemini_ranking.types
        gemini_ranking.types = _FakeTypes

    def tearDown(self) -> None:
        from common import gemini_ranking

        gemini_ranking.types = self.original_types

    def test_extract_response_text_strips_or_returns_empty(self) -> None:
        from common import gemini_ranking

        self.assertEqual(
            gemini_ranking.extract_response_text(_FakeResponse(" copy \n")),
            "copy",
        )
        self.assertEqual(gemini_ranking.extract_response_text(object()), "")

    def test_runner_calls_default_full_model_and_generation_config(
        self,
    ) -> None:
        from common import gemini_ranking
        from common import prompts

        client = _FakeClient(response_text="generated text")
        runner = gemini_ranking.GeminiRankingRunner(client)

        result = runner.predict_one(_row("audio-a", 1), [])

        self.assertEqual(result, "generated text")
        self.assertEqual(len(client.models.calls), 1)
        call = client.models.calls[0]
        self.assertEqual(call["model"], "gemini-3.5-flash")
        self.assertEqual(call["contents"][0]["role"], "user")
        config = call["config"]
        self.assertEqual(
            config.kwargs["system_instruction"],
            prompts.GEMINI_TRANSCRIBE_SYSTEM_PROMPT,
        )
        self.assertEqual(config.kwargs["temperature"], 0.0)
        self.assertEqual(config.kwargs["max_output_tokens"], 512)

    def test_default_preflight_model_constant(self) -> None:
        from common import gemini_ranking

        self.assertEqual(
            gemini_ranking.DEFAULT_PREFLIGHT_MODEL,
            "gemini-3.1-pro-preview",
        )

    def test_import_without_vertex_extra_defers_failure(self) -> None:
        from common import gemini_ranking

        self.assertTrue(callable(gemini_ranking.build_contextual_contents))

    def test_new_vertex_client_passes_timeout_http_options(self) -> None:
        from common import gemini_ranking

        captured: dict[str, object] = {}

        class FakeGenai:
            @staticmethod
            def Client(**kwargs: object) -> object:
                captured.update(kwargs)
                return object()

        original_genai = gemini_ranking.genai
        try:
            gemini_ranking.genai = FakeGenai
            client = gemini_ranking.new_vertex_client(
                "test-project",
                "global",
                timeout_ms=120000,
            )
        finally:
            gemini_ranking.genai = original_genai

        self.assertIsNotNone(client)
        self.assertEqual(captured["vertexai"], True)
        self.assertEqual(captured["project"], "test-project")
        self.assertEqual(captured["location"], "global")
        self.assertEqual(
            captured["http_options"].kwargs,
            {"timeout": 120000},
        )


class TestSourceGroupPredictionRun(unittest.TestCase):
    def test_compatible_cache_hit_avoids_model_call(self) -> None:
        from common import gemini_ranking
        from common import ranking

        context_fp = ranking.context_fingerprint([])
        cache_entry = _entry(
            ranking,
            "audio-a",
            "cached prediction",
            context_fp=context_fp,
        )
        runner = _RecordingRunner()

        new_entries, final_cache = gemini_ranking.run_source_group_predictions(
            [_row("audio-a", 1)],
            {"audio-a": cache_entry},
            runner,
            prompt_fp="prompt-fp",
            context_policy_fp="context-policy-fp",
            num_recent_events=1000,
            created_at="2026-06-02T00:00:00Z",
        )

        self.assertEqual(new_entries, [])
        self.assertEqual(final_cache["audio-a"], cache_entry)
        self.assertEqual(runner.calls, [])

    def test_generated_prediction_feeds_later_same_source_context(
        self,
    ) -> None:
        from common import gemini_ranking

        runner = _RecordingRunner()

        new_entries, final_cache = gemini_ranking.run_source_group_predictions(
            [_row("audio-a", 1), _row("audio-b", 2)],
            {},
            runner,
            prompt_fp="prompt-fp",
            context_policy_fp="context-policy-fp",
            num_recent_events=1000,
            created_at="2026-06-02T00:00:00Z",
        )

        self.assertEqual(len(new_entries), 2)
        self.assertEqual(runner.calls[0], ("audio-a", []))
        self.assertEqual(runner.calls[1], ("audio-b", ["generated audio-a"]))
        self.assertIn("audio-a", final_cache)
        self.assertIn("audio-b", final_cache)

    def test_prediction_error_records_cache_entry_and_continues(
        self,
    ) -> None:
        from common import gemini_ranking

        runner = _FailingRunner()

        new_entries, final_cache = gemini_ranking.run_source_group_predictions(
            [_row("audio-a", 1), _row("audio-b", 2), _row("audio-c", 3)],
            {},
            runner,
            prompt_fp="prompt-fp",
            context_policy_fp="context-policy-fp",
            num_recent_events=1000,
            created_at="2026-06-02T00:00:00Z",
        )

        self.assertEqual(len(new_entries), 3)
        self.assertEqual(final_cache["audio-b"].prediction_text, "")
        self.assertIn("RuntimeError", final_cache["audio-b"].error)
        self.assertEqual(
            runner.calls,
            [
                ("audio-a", []),
                ("audio-b", ["generated audio-a"]),
                ("audio-c", ["generated audio-a"]),
            ],
        )


if __name__ == "__main__":
    unittest.main()
