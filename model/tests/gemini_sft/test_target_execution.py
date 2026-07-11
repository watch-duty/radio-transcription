"""Tests for Gemini SFT target execution."""

from __future__ import annotations

import asyncio
import json
import pathlib
import shutil
import unittest
import unittest.mock

import fake_gcs
from common.gemini import context, eval_artifacts, request_identity, vertex
from gemini_sft import config, target_execution


def _identity(
    *,
    model: str = "projects/p/locations/us-central1/endpoints/123",
    audio_uris: list[str] | None = None,
    system_prompt: str = "system",
    user_prompt: str = "user",
    histories: list[list[context.ContextTurn]] | None = None,
) -> dict:
    kwargs = {
        "target_label": "checkpoint_6",
        "model": model,
        "eval_manifest_uri": "gs://data/eval.jsonl",
        "audio_uris": audio_uris or ["gs://audio/1.flac", "gs://audio/2.flac"],
        "system_prompt": system_prompt,
        "user_prompt": user_prompt,
        "prior_context_count": 8,
        "prior_context_mode": "text_turns",
    }
    if histories is not None:
        kwargs["histories"] = histories
    return request_identity.build_gemini_eval_request_identity(**kwargs)


def _metadata(identity: dict) -> str:
    return (
        json.dumps(
            {
                "request_identity_hash": request_identity.request_identity_hash(
                    identity
                ),
                "request_identity": identity,
            },
            sort_keys=True,
        )
        + "\n"
    )


def _line_count(path: pathlib.Path) -> int:
    if not path.exists():
        return 0
    return len(path.read_text(encoding="utf-8").splitlines())


class TestTargetBackendResolver(unittest.TestCase):
    def test_publisher_model_defaults_to_batch(self) -> None:
        backend = target_execution.resolve_target_backend(
            config.EvalModelTarget(label="base", model="gemini-3.1-flash-lite"),
            config.EvalExecutionConfig(),
        )

        self.assertEqual(backend, "batch")

    def test_endpoint_resource_defaults_to_online(self) -> None:
        backend = target_execution.resolve_target_backend(
            config.EvalModelTarget(
                label="checkpoint_6",
                model="projects/p/locations/us-central1/endpoints/123",
            ),
            config.EvalExecutionConfig(),
        )

        self.assertEqual(backend, "online")

    def test_forced_backend_overrides_model_shape(self) -> None:
        publisher = config.EvalModelTarget(
            label="base",
            model="gemini-3.1-flash-lite",
        )
        endpoint = config.EvalModelTarget(
            label="checkpoint_6",
            model="projects/p/locations/us-central1/endpoints/123",
        )

        self.assertEqual(
            target_execution.resolve_target_backend(
                publisher,
                config.EvalExecutionConfig(backend="online"),
            ),
            "online",
        )
        self.assertEqual(
            target_execution.resolve_target_backend(
                endpoint,
                config.EvalExecutionConfig(backend="batch"),
            ),
            "batch",
        )


class TestOnlineRequestIdentity(unittest.TestCase):
    def test_prediction_uris_are_under_eval_label(self) -> None:
        prefix = "gs://bucket/runs/run-1/"
        paths = eval_artifacts.eval_target_artifact_paths(
            prefix, "checkpoint_6"
        )

        self.assertEqual(
            eval_artifacts.online_prediction_uri(prefix, "checkpoint_6"),
            paths.online_predictions_uri,
        )
        self.assertEqual(
            eval_artifacts.online_prediction_metadata_uri(
                prefix, "checkpoint_6"
            ),
            paths.online_metadata_uri,
        )

    def test_hash_is_stable_for_key_order(self) -> None:
        identity = _identity()
        reordered = {key: identity[key] for key in reversed(identity)}

        self.assertEqual(
            request_identity.request_identity_hash(identity),
            request_identity.request_identity_hash(reordered),
        )

    def test_hash_changes_for_request_content(self) -> None:
        base = request_identity.request_identity_hash(_identity())

        self.assertNotEqual(
            base,
            request_identity.request_identity_hash(
                _identity(system_prompt="different")
            ),
        )
        self.assertNotEqual(
            base,
            request_identity.request_identity_hash(
                _identity(model="gemini-3.1-flash-lite")
            ),
        )
        self.assertNotEqual(
            base,
            request_identity.request_identity_hash(
                _identity(audio_uris=["gs://audio/2.flac", "gs://audio/1.flac"])
            ),
        )

    def test_hash_changes_when_prior_transcript_changes(self) -> None:
        first = [
            [context.ContextTurn("gs://audio/prior.flac", "alpha")],
            [],
        ]
        second = [
            [context.ContextTurn("gs://audio/prior.flac", "bravo")],
            [],
        ]

        self.assertNotEqual(
            request_identity.request_identity_hash(_identity(histories=first)),
            request_identity.request_identity_hash(_identity(histories=second)),
        )

    def test_prefix_identity_rejects_changed_existing_request(self) -> None:
        stored = _identity(
            audio_uris=["gs://audio/1.flac"],
            histories=[[]],
        )
        requested = _identity(
            audio_uris=["gs://audio/1.flac", "gs://audio/2.flac"],
            histories=[
                [
                    context.ContextTurn(
                        "gs://audio/new-prior.flac",
                        "changed transcript",
                    )
                ],
                [],
            ],
        )

        with self.assertRaisesRegex(ValueError, "request identity mismatch"):
            request_identity.validate_prefix_identity(
                stored,
                requested,
                "request identity mismatch",
            )

    def test_identity_excludes_operational_settings(self) -> None:
        identity = _identity()

        self.assertNotIn("concurrency", identity)
        self.assertNotIn("max_retries", identity)


class TestOnlinePredictionResume(unittest.TestCase):
    def setUp(self) -> None:
        self.storage = fake_gcs.FakeStorageClient()
        self.predictions_uri = eval_artifacts.online_prediction_uri(
            "gs://bucket/run", "checkpoint_6"
        )
        self.metadata_uri = eval_artifacts.online_prediction_metadata_uri(
            "gs://bucket/run", "checkpoint_6"
        )
        self.local_dir = pathlib.Path(self.id().replace(".", "_"))
        self.addCleanup(shutil.rmtree, self.local_dir, ignore_errors=True)

    def _load(self, identity: dict):
        return target_execution.load_existing_online_predictions(
            storage_client=self.storage,
            predictions_uri=self.predictions_uri,
            metadata_uri=self.metadata_uri,
            local_predictions_path=self.local_dir / "online_predictions.jsonl",
            local_metadata_path=self.local_dir / "online_predictions.meta.json",
            request_identity=identity,
        )

    def test_exact_resume_returns_successful_rows_and_error_count(self) -> None:
        identity = _identity()
        self.storage.put(
            self.predictions_uri,
            (
                '{"audio_filepath":"gs://audio/1.flac",'
                '"pred_text":"one","error":null}\n'
                '{"audio_filepath":"gs://audio/2.flac",'
                '"pred_text":"","error":"empty response"}\n'
            ),
        )
        self.storage.put(self.metadata_uri, _metadata(identity))

        state = self._load(identity)

        self.assertEqual(set(state.rows_by_audio_uri), {"gs://audio/1.flac"})
        self.assertEqual(state.error_count, 1)

    def test_prediction_without_metadata_fails_before_paid_calls(self) -> None:
        self.storage.put(
            self.predictions_uri,
            '{"audio_filepath":"gs://audio/1.flac","pred_text":"one"}\n',
        )

        with self.assertRaisesRegex(
            ValueError, "online prediction metadata missing"
        ):
            self._load(_identity())

    def test_prompt_mismatch_fails(self) -> None:
        self.storage.put(
            self.predictions_uri,
            '{"audio_filepath":"gs://audio/1.flac","pred_text":"one"}\n',
        )
        self.storage.put(self.metadata_uri, _metadata(_identity()))

        with self.assertRaisesRegex(
            ValueError, "online prediction request identity mismatch"
        ):
            self._load(_identity(system_prompt="new prompt"))

    def test_non_prefix_audio_order_fails(self) -> None:
        old_identity = _identity(
            audio_uris=["gs://audio/2.flac", "gs://audio/1.flac"]
        )
        self.storage.put(
            self.predictions_uri,
            '{"audio_filepath":"gs://audio/2.flac","pred_text":"two"}\n',
        )
        self.storage.put(self.metadata_uri, _metadata(old_identity))

        with self.assertRaisesRegex(
            ValueError, "online prediction request identity mismatch"
        ):
            self._load(_identity())

    def test_smoke_prefix_reuse_returns_existing_prefix_rows(self) -> None:
        smoke_identity = _identity(audio_uris=["gs://audio/1.flac"])
        full_identity = _identity(
            audio_uris=[
                "gs://audio/1.flac",
                "gs://audio/2.flac",
                "gs://audio/3.flac",
            ]
        )
        self.storage.put(
            self.predictions_uri,
            '{"audio_filepath":"gs://audio/1.flac","pred_text":"one"}\n',
        )
        self.storage.put(self.metadata_uri, _metadata(smoke_identity))

        state = self._load(full_identity)

        self.assertEqual(list(state.rows_by_audio_uri), ["gs://audio/1.flac"])

    def test_malformed_prediction_row_is_skipped_on_resume(self) -> None:
        identity = _identity()
        self.storage.put(
            self.predictions_uri,
            (
                '{"audio_filepath":"gs://audio/1.flac",'
                '"pred_text":"one","error":null}\n'
                '{"audio_filepath":"gs://audio/2.flac","pred_text":\n'
            ),
        )
        self.storage.put(self.metadata_uri, _metadata(identity))

        state = self._load(identity)

        self.assertEqual(
            state.rows_by_audio_uri,
            {
                "gs://audio/1.flac": {
                    "audio_filepath": "gs://audio/1.flac",
                    "pred_text": "one",
                    "error": None,
                }
            },
        )

    def test_non_object_prediction_row_is_skipped_on_resume(self) -> None:
        identity = _identity()
        self.storage.put(
            self.predictions_uri,
            (
                '["not", "an", "object"]\n'
                '{"audio_filepath":"gs://audio/1.flac",'
                '"pred_text":"one","error":null}\n'
            ),
        )
        self.storage.put(self.metadata_uri, _metadata(identity))

        state = self._load(identity)

        self.assertEqual(
            state.rows_by_audio_uri,
            {
                "gs://audio/1.flac": {
                    "audio_filepath": "gs://audio/1.flac",
                    "pred_text": "one",
                    "error": None,
                }
            },
        )

    def test_absent_gcs_predictions_clear_stale_local_mirror(self) -> None:
        local_predictions = self.local_dir / "online_predictions.jsonl"
        local_predictions.parent.mkdir(parents=True)
        local_predictions.write_text(
            '{"audio_filepath":"gs://audio/stale.flac","pred_text":"stale"}\n',
            encoding="utf-8",
        )

        state = self._load(_identity())

        self.assertEqual(state.rows_by_audio_uri, {})
        self.assertFalse(local_predictions.exists())

    def test_smoke_metadata_rejects_rows_outside_stored_identity(self) -> None:
        smoke_identity = _identity(audio_uris=["gs://audio/1.flac"])
        full_identity = _identity(
            audio_uris=["gs://audio/1.flac", "gs://audio/2.flac"]
        )
        self.storage.put(
            self.predictions_uri,
            '{"audio_filepath":"gs://audio/2.flac","pred_text":"two"}\n',
        )
        self.storage.put(self.metadata_uri, _metadata(smoke_identity))

        with self.assertRaisesRegex(
            ValueError, "online prediction request identity mismatch"
        ):
            self._load(full_identity)


class TestRunOnlineTargetInference(unittest.TestCase):
    def setUp(self) -> None:
        self.storage = fake_gcs.FakeStorageClient()
        self.local_dir = pathlib.Path(self.id().replace(".", "_"))
        self.addCleanup(shutil.rmtree, self.local_dir, ignore_errors=True)

    @unittest.mock.patch("gemini_sft.target_execution.vertex.types")
    @unittest.mock.patch("gemini_sft.target_execution.vertex.genai")
    def test_rejects_duplicate_audio_uris_before_paid_calls(
        self, mock_genai, mock_types
    ) -> None:
        mock_types.GenerateContentConfig.side_effect = lambda **kwargs: kwargs

        with self.assertRaisesRegex(ValueError, "duplicate audio_uri"):
            asyncio.run(
                target_execution.run_online_target_inference(
                    storage_client=self.storage,
                    run_gcs_prefix="gs://bucket/run",
                    project="project",
                    default_location="us-central1",
                    target_label="checkpoint_6",
                    target_model=(
                        "projects/p/locations/us-central1/endpoints/123"
                    ),
                    audio_uris=["gs://audio/1.flac", "gs://audio/1.flac"],
                    histories=[[], []],
                    system_prompt="system",
                    user_prompt="user",
                    prior_context_count=8,
                    prior_context_mode="text_turns",
                    eval_manifest_uri="gs://data/eval.jsonl",
                    local_dir=self.local_dir,
                    concurrency=2,
                    max_retries=1,
                )
            )

        mock_genai.Client.assert_not_called()

    @unittest.mock.patch("gemini_sft.target_execution.vertex.types")
    @unittest.mock.patch("gemini_sft.target_execution.vertex.genai")
    def test_runs_with_shared_request_builder_and_records_empty_prediction(
        self, mock_genai, mock_types
    ) -> None:
        class Response:
            def __init__(self, text: str | None) -> None:
                self.text = text

        async def generate_content(**kwargs):
            calls.append(kwargs)
            if kwargs["contents"][-1]["parts"][-1]["fileData"][
                "fileUri"
            ].endswith("1.flac"):
                return Response("recognized")
            return Response("")

        calls = []
        mock_client = unittest.mock.MagicMock()
        mock_client.aio.models.generate_content = generate_content
        mock_genai.Client.return_value = mock_client
        mock_types.HttpRetryOptions.side_effect = lambda **kwargs: kwargs
        mock_types.HttpOptions.side_effect = lambda **kwargs: kwargs
        mock_types.GenerateContentConfig.side_effect = lambda **kwargs: kwargs

        result = asyncio.run(
            target_execution.run_online_target_inference(
                storage_client=self.storage,
                run_gcs_prefix="gs://bucket/run",
                project="project",
                default_location="us-central1",
                target_label="checkpoint_6",
                target_model="projects/p/locations/us-central1/endpoints/123",
                audio_uris=["gs://audio/1.flac", "gs://audio/2.flac"],
                histories=[
                    [
                        context.ContextTurn(
                            audio_uri="gs://audio/0.flac", text="prior"
                        )
                    ],
                    [],
                ],
                system_prompt="system",
                user_prompt="user",
                prior_context_count=8,
                prior_context_mode="text_turns",
                eval_manifest_uri="gs://data/eval.jsonl",
                local_dir=self.local_dir,
                concurrency=2,
                max_retries=3,
            )
        )

        mock_genai.Client.assert_called_once_with(
            vertexai=True,
            project="project",
            location="us-central1",
        )
        self.assertEqual(result["gs://audio/1.flac"], "recognized")
        self.assertEqual(result["gs://audio/2.flac"], "")
        self.assertEqual(result.error_count, 0)
        self.assertTrue(self.storage.has(result.online_predictions_uri))
        self.assertTrue(self.storage.has(result.metadata_uri))
        self.assertEqual(len(calls), 2)
        self.assertIn(
            {"text": "prior"},
            calls[0]["contents"][1]["parts"],
        )
        self.assertEqual(
            calls[0]["config"]["safety_settings"], vertex.GEMINI_SAFETY_SETTINGS
        )
        self.assertEqual(
            calls[0]["config"]["http_options"]["retry_options"],
            {
                "attempts": 3,
                "initial_delay": 2.0,
                "max_delay": 60.0,
                "exp_base": 2.0,
                "jitter": 1.0,
                "http_status_codes": [408, 429, 500, 502, 503, 504],
            },
        )
        self.assertIn(
            '"error": null', self.storage.get(result.online_predictions_uri)
        )

    @unittest.mock.patch("gemini_sft.target_execution.vertex.types")
    @unittest.mock.patch("gemini_sft.target_execution.vertex.genai")
    def test_each_online_config_uses_its_exact_request_payload(
        self, mock_genai, mock_types
    ) -> None:
        class Response:
            text = "recognized"

        request_payloads = {
            "gs://audio/1.flac": {
                "request": {
                    "contents": [
                        {
                            "role": "user",
                            "parts": [
                                {
                                    "fileData": {
                                        "fileUri": "gs://audio/1.flac",
                                        "mimeType": "audio/flac",
                                    }
                                }
                            ],
                        }
                    ],
                    "systemInstruction": {
                        "role": "system",
                        "parts": [{"text": "system one"}],
                    },
                    "generationConfig": {
                        "temperature": 0.1,
                        "max_output_tokens": 111,
                    },
                    "safetySettings": [
                        {
                            "category": "HARM_CATEGORY_HARASSMENT",
                            "threshold": "BLOCK_LOW_AND_ABOVE",
                        }
                    ],
                }
            },
            "gs://audio/2.flac": {
                "request": {
                    "contents": [
                        {
                            "role": "user",
                            "parts": [
                                {
                                    "fileData": {
                                        "fileUri": "gs://audio/2.flac",
                                        "mimeType": "audio/flac",
                                    }
                                }
                            ],
                        }
                    ],
                    "systemInstruction": {
                        "role": "system",
                        "parts": [{"text": "system two"}],
                    },
                    "generationConfig": {
                        "temperature": 0.2,
                        "max_output_tokens": 222,
                    },
                    "safetySettings": [
                        {
                            "category": "HARM_CATEGORY_HATE_SPEECH",
                            "threshold": "BLOCK_MEDIUM_AND_ABOVE",
                        }
                    ],
                }
            },
        }
        calls = []

        async def generate_content(**kwargs):
            calls.append(kwargs)
            return Response()

        def build_request(audio_uri: str, **_: object) -> dict:
            return request_payloads[audio_uri]

        mock_client = unittest.mock.MagicMock()
        mock_client.aio.models.generate_content = generate_content
        mock_genai.Client.return_value = mock_client
        mock_types.HttpRetryOptions.side_effect = lambda **kwargs: kwargs
        retry_http_options = unittest.mock.sentinel.retry_http_options
        mock_types.HttpOptions.return_value = retry_http_options
        mock_types.GenerateContentConfig.side_effect = lambda **kwargs: kwargs

        with unittest.mock.patch.object(
            target_execution.vertex,
            "build_request",
            side_effect=build_request,
        ):
            asyncio.run(
                target_execution.run_online_target_inference(
                    storage_client=self.storage,
                    run_gcs_prefix="gs://bucket/run",
                    project="project",
                    default_location="us-central1",
                    target_label="checkpoint_6",
                    target_model=(
                        "projects/p/locations/us-central1/endpoints/123"
                    ),
                    audio_uris=list(request_payloads),
                    histories=[[], []],
                    system_prompt="shared system",
                    user_prompt="shared user",
                    prior_context_count=8,
                    prior_context_mode="text_turns",
                    eval_manifest_uri="gs://data/eval.jsonl",
                    local_dir=self.local_dir,
                    concurrency=2,
                    max_retries=3,
                )
            )

        self.assertEqual(mock_types.GenerateContentConfig.call_count, 2)
        mock_types.HttpRetryOptions.assert_called_once()
        mock_types.HttpOptions.assert_called_once()
        calls_by_audio_uri = {
            call["contents"][-1]["parts"][-1]["fileData"]["fileUri"]: call
            for call in calls
        }
        for audio_uri, wrapped_payload in request_payloads.items():
            request = wrapped_payload["request"]
            self.assertEqual(
                calls_by_audio_uri[audio_uri]["config"],
                {
                    "system_instruction": request["systemInstruction"],
                    **request["generationConfig"],
                    "safety_settings": request["safetySettings"],
                    "http_options": retry_http_options,
                },
            )

    @unittest.mock.patch("gemini_sft.target_execution.vertex.types")
    @unittest.mock.patch("gemini_sft.target_execution.vertex.genai")
    def test_worker_pool_schedules_at_most_concurrency_workers(
        self, mock_genai, mock_types
    ) -> None:
        class Response:
            text = "recognized"

        async def generate_content(**kwargs):
            return Response()

        gather_argument_counts: list[int] = []
        original_gather = asyncio.gather

        async def tracking_gather(*awaitables, **kwargs):
            gather_argument_counts.append(len(awaitables))
            return await original_gather(*awaitables, **kwargs)

        mock_client = unittest.mock.MagicMock()
        mock_client.aio.models.generate_content = generate_content
        mock_genai.Client.return_value = mock_client
        mock_types.GenerateContentConfig.side_effect = lambda **kwargs: kwargs

        with unittest.mock.patch(
            "gemini_sft.target_execution.asyncio.gather",
            tracking_gather,
        ):
            asyncio.run(
                target_execution.run_online_target_inference(
                    storage_client=self.storage,
                    run_gcs_prefix="gs://bucket/run",
                    project="project",
                    default_location="us-central1",
                    target_label="checkpoint_6",
                    target_model=(
                        "projects/p/locations/us-central1/endpoints/123"
                    ),
                    audio_uris=[
                        f"gs://audio/{index}.flac" for index in range(5)
                    ],
                    histories=[[] for _ in range(5)],
                    system_prompt="system",
                    user_prompt="user",
                    prior_context_count=8,
                    prior_context_mode="text_turns",
                    eval_manifest_uri="gs://data/eval.jsonl",
                    local_dir=self.local_dir,
                    concurrency=2,
                    max_retries=1,
                )
            )

        self.assertEqual(gather_argument_counts, [2])

    @unittest.mock.patch("gemini_sft.target_execution.vertex.types")
    @unittest.mock.patch("gemini_sft.target_execution.vertex.genai")
    def test_worker_pool_starts_next_request_before_straggler_finishes(
        self, mock_genai, mock_types
    ) -> None:
        class Response:
            text = "recognized"

        async def run_scenario() -> None:
            slow_started = asyncio.Event()
            third_started = asyncio.Event()
            release_slow = asyncio.Event()

            async def generate_content(**kwargs):
                audio_uri = kwargs["contents"][-1]["parts"][-1]["fileData"][
                    "fileUri"
                ]
                if audio_uri.endswith("0.flac"):
                    slow_started.set()
                    await release_slow.wait()
                elif audio_uri.endswith("2.flac"):
                    third_started.set()
                return Response()

            mock_client = unittest.mock.MagicMock()
            mock_client.aio.models.generate_content = generate_content
            mock_genai.Client.return_value = mock_client
            generate_config = mock_types.GenerateContentConfig
            generate_config.side_effect = lambda **kwargs: kwargs

            async def observe_start_order() -> bool:
                try:
                    await asyncio.wait_for(slow_started.wait(), timeout=1.0)
                    try:
                        await asyncio.wait_for(
                            third_started.wait(), timeout=1.0
                        )
                    except TimeoutError:
                        return False
                    else:
                        return True
                finally:
                    release_slow.set()

            _, third_started_before_release = await asyncio.gather(
                target_execution.run_online_target_inference(
                    storage_client=self.storage,
                    run_gcs_prefix="gs://bucket/run",
                    project="project",
                    default_location="us-central1",
                    target_label="checkpoint_6",
                    target_model=(
                        "projects/p/locations/us-central1/endpoints/123"
                    ),
                    audio_uris=[
                        f"gs://audio/{index}.flac" for index in range(3)
                    ],
                    histories=[[] for _ in range(3)],
                    system_prompt="system",
                    user_prompt="user",
                    prior_context_count=8,
                    prior_context_mode="text_turns",
                    eval_manifest_uri="gs://data/eval.jsonl",
                    local_dir=self.local_dir,
                    concurrency=2,
                    max_retries=1,
                ),
                observe_start_order(),
            )
            self.assertTrue(third_started_before_release)

        asyncio.run(run_scenario())

    @unittest.mock.patch("gemini_sft.target_execution.vertex.types")
    @unittest.mock.patch("gemini_sft.target_execution.vertex.genai")
    def test_uploads_run_in_thread_pool(self, mock_genai, mock_types) -> None:
        class Response:
            text = "recognized"

        async def generate_content(**kwargs):
            return Response()

        to_thread_calls = []

        async def fake_to_thread(fn, *args, **kwargs):
            to_thread_calls.append((fn, args, kwargs))
            return fn(*args, **kwargs)

        mock_client = unittest.mock.MagicMock()
        mock_client.aio.models.generate_content = generate_content
        mock_genai.Client.return_value = mock_client
        mock_types.GenerateContentConfig.side_effect = lambda **kwargs: kwargs

        with unittest.mock.patch(
            "gemini_sft.target_execution.asyncio.to_thread",
            fake_to_thread,
        ):
            asyncio.run(
                target_execution.run_online_target_inference(
                    storage_client=self.storage,
                    run_gcs_prefix="gs://bucket/run",
                    project="project",
                    default_location="us-central1",
                    target_label="checkpoint_6",
                    target_model=(
                        "projects/p/locations/us-central1/endpoints/123"
                    ),
                    audio_uris=["gs://audio/1.flac"],
                    histories=[[]],
                    system_prompt="system",
                    user_prompt="user",
                    prior_context_count=8,
                    prior_context_mode="text_turns",
                    eval_manifest_uri="gs://data/eval.jsonl",
                    local_dir=self.local_dir,
                    concurrency=1,
                    max_retries=1,
                )
            )

        self.assertGreaterEqual(
            sum(
                1
                for fn, _, _ in to_thread_calls
                if fn is target_execution.gcs_utils.upload_local_file
            ),
            1,
        )
        self.assertGreaterEqual(
            sum(
                1
                for fn, _, _ in to_thread_calls
                if fn is target_execution.gcs_utils.upload_text
            ),
            1,
        )

    @unittest.mock.patch("gemini_sft.target_execution.vertex.types")
    @unittest.mock.patch("gemini_sft.target_execution.vertex.genai")
    def test_periodic_snapshot_uploads_completed_rows(
        self, mock_genai, mock_types
    ) -> None:
        class Response:
            text = "recognized"

        async def generate_content(**kwargs):
            return Response()

        snapshots: list[str] = []

        async def fake_periodic_upload(**kwargs):
            snapshots.append(kwargs["snapshot"])

        mock_client = unittest.mock.MagicMock()
        mock_client.aio.models.generate_content = generate_content
        mock_genai.Client.return_value = mock_client
        mock_types.GenerateContentConfig.side_effect = lambda **kwargs: kwargs

        with (
            unittest.mock.patch(
                "gemini_sft.target_execution.ONLINE_SYNC_EVERY", 1
            ),
            unittest.mock.patch(
                "gemini_sft.target_execution."
                "_upload_periodic_prediction_snapshot",
                fake_periodic_upload,
            ),
        ):
            asyncio.run(
                target_execution.run_online_target_inference(
                    storage_client=self.storage,
                    run_gcs_prefix="gs://bucket/run",
                    project="project",
                    default_location="us-central1",
                    target_label="checkpoint_6",
                    target_model=(
                        "projects/p/locations/us-central1/endpoints/123"
                    ),
                    audio_uris=["gs://audio/1.flac"],
                    histories=[[]],
                    system_prompt="system",
                    user_prompt="user",
                    prior_context_count=8,
                    prior_context_mode="text_turns",
                    eval_manifest_uri="gs://data/eval.jsonl",
                    local_dir=self.local_dir,
                    concurrency=1,
                    max_retries=1,
                )
            )

        self.assertEqual(len(snapshots), 1)
        rows = [
            json.loads(line)
            for line in snapshots[0].splitlines()
            if line.strip()
        ]
        self.assertEqual(
            rows,
            [
                {
                    "audio_filepath": "gs://audio/1.flac",
                    "error": None,
                    "model": "projects/p/locations/us-central1/endpoints/123",
                    "pred_text": "recognized",
                    "target_label": "checkpoint_6",
                }
            ],
        )

    @unittest.mock.patch("gemini_sft.target_execution.vertex.types")
    @unittest.mock.patch("gemini_sft.target_execution.vertex.genai")
    def test_periodic_uploads_are_serialized_without_holding_append_lock(
        self, mock_genai, mock_types
    ) -> None:
        class Response:
            text = "recognized"

        async def generate_content(**kwargs):
            return Response()

        async def run_scenario() -> None:
            upload_started = asyncio.Event()
            second_upload_started = asyncio.Event()
            release_upload = asyncio.Event()
            prediction_upload_calls = 0
            first_snapshot_line_count: int | None = None

            async def fake_upload(*args, **kwargs):
                nonlocal first_snapshot_line_count, prediction_upload_calls
                gcs_uri = str(args[2])
                if not gcs_uri.endswith("online_predictions.jsonl"):
                    return
                prediction_upload_calls += 1
                if prediction_upload_calls == 1:
                    first_snapshot_line_count = len(str(args[1]).splitlines())
                    upload_started.set()
                    await release_upload.wait()
                elif prediction_upload_calls == 2:
                    second_upload_started.set()

            async def wait_for_two_rows(path: pathlib.Path) -> None:
                while True:
                    line_count = await asyncio.to_thread(_line_count, path)
                    if line_count >= 2:
                        return
                    await asyncio.sleep(0)

            with (
                unittest.mock.patch(
                    "gemini_sft.target_execution.ONLINE_SYNC_EVERY", 1
                ),
                unittest.mock.patch(
                    "gemini_sft.target_execution._upload_text_async",
                    fake_upload,
                ),
            ):
                task = asyncio.create_task(
                    target_execution.run_online_target_inference(
                        storage_client=self.storage,
                        run_gcs_prefix="gs://bucket/run",
                        project="project",
                        default_location="us-central1",
                        target_label="checkpoint_6",
                        target_model=(
                            "projects/p/locations/us-central1/endpoints/123"
                        ),
                        audio_uris=["gs://audio/1.flac", "gs://audio/2.flac"],
                        histories=[[], []],
                        system_prompt="system",
                        user_prompt="user",
                        prior_context_count=8,
                        prior_context_mode="text_turns",
                        eval_manifest_uri="gs://data/eval.jsonl",
                        local_dir=self.local_dir,
                        concurrency=2,
                        max_retries=1,
                    )
                )
                try:
                    await asyncio.wait_for(upload_started.wait(), timeout=1.0)
                    predictions_path = (
                        self.local_dir
                        / "checkpoint_6"
                        / "online_predictions.jsonl"
                    )
                    await asyncio.wait_for(
                        wait_for_two_rows(predictions_path), timeout=1.0
                    )
                    await asyncio.sleep(0)
                    self.assertFalse(second_upload_started.is_set())
                finally:
                    release_upload.set()
                    await task
            self.assertEqual(first_snapshot_line_count, 1)

        mock_client = unittest.mock.MagicMock()
        mock_client.aio.models.generate_content = generate_content
        mock_genai.Client.return_value = mock_client
        mock_types.GenerateContentConfig.side_effect = lambda **kwargs: kwargs

        asyncio.run(run_scenario())

    @unittest.mock.patch("gemini_sft.target_execution.vertex.types")
    @unittest.mock.patch("gemini_sft.target_execution.vertex.genai")
    def test_periodic_upload_failure_does_not_abort_generation(
        self, mock_genai, mock_types
    ) -> None:
        class Response:
            text = "recognized"

        async def generate_content(**kwargs):
            return Response()

        prediction_uploads: list[str] = []

        async def flaky_upload(storage_client, text: str, gcs_uri: str) -> None:
            if not gcs_uri.endswith("online_predictions.jsonl"):
                return
            prediction_uploads.append(text)
            if len(prediction_uploads) == 1:
                msg = "temporary upload failure"
                raise RuntimeError(msg)

        mock_client = unittest.mock.MagicMock()
        mock_client.aio.models.generate_content = generate_content
        mock_genai.Client.return_value = mock_client
        mock_types.GenerateContentConfig.side_effect = lambda **kwargs: kwargs

        with (
            unittest.mock.patch(
                "gemini_sft.target_execution.ONLINE_SYNC_EVERY", 1
            ),
            unittest.mock.patch(
                "gemini_sft.target_execution._upload_text_async",
                flaky_upload,
            ),
            self.assertLogs(
                "gemini_sft.target_execution", level="WARNING"
            ) as logs,
        ):
            result = asyncio.run(
                target_execution.run_online_target_inference(
                    storage_client=self.storage,
                    run_gcs_prefix="gs://bucket/run",
                    project="project",
                    default_location="us-central1",
                    target_label="checkpoint_6",
                    target_model=(
                        "projects/p/locations/us-central1/endpoints/123"
                    ),
                    audio_uris=["gs://audio/1.flac"],
                    histories=[[]],
                    system_prompt="system",
                    user_prompt="user",
                    prior_context_count=8,
                    prior_context_mode="text_turns",
                    eval_manifest_uri="gs://data/eval.jsonl",
                    local_dir=self.local_dir,
                    concurrency=1,
                    max_retries=1,
                )
            )

        self.assertEqual(result["gs://audio/1.flac"], "recognized")
        self.assertEqual(len(prediction_uploads), 2)
        self.assertIn("temporary upload failure", "\n".join(logs.output))

    @unittest.mock.patch("gemini_sft.target_execution.vertex.genai")
    def test_safe_resume_skips_existing_rows(self, mock_genai) -> None:
        identity = _identity()
        predictions_uri = eval_artifacts.online_prediction_uri(
            "gs://bucket/run", "checkpoint_6"
        )
        metadata_uri = eval_artifacts.online_prediction_metadata_uri(
            "gs://bucket/run", "checkpoint_6"
        )
        self.storage.put(
            predictions_uri,
            (
                '{"audio_filepath":"gs://audio/1.flac",'
                '"pred_text":"one","error":null}\n'
                '{"audio_filepath":"gs://audio/2.flac",'
                '"pred_text":"two","error":null}\n'
            ),
        )
        self.storage.put(metadata_uri, _metadata(identity))

        result = asyncio.run(
            target_execution.run_online_target_inference(
                storage_client=self.storage,
                run_gcs_prefix="gs://bucket/run",
                project="project",
                default_location="us-central1",
                target_label="checkpoint_6",
                target_model="projects/p/locations/us-central1/endpoints/123",
                audio_uris=["gs://audio/1.flac", "gs://audio/2.flac"],
                histories=[[], []],
                system_prompt="system",
                user_prompt="user",
                prior_context_count=8,
                prior_context_mode="text_turns",
                eval_manifest_uri="gs://data/eval.jsonl",
                local_dir=self.local_dir,
                concurrency=2,
                max_retries=1,
            )
        )

        mock_genai.Client.assert_not_called()
        self.assertEqual(
            dict(result),
            {
                "gs://audio/1.flac": "one",
                "gs://audio/2.flac": "two",
            },
        )

    @unittest.mock.patch("gemini_sft.target_execution.vertex.types")
    @unittest.mock.patch("gemini_sft.target_execution.vertex.genai")
    def test_resume_retries_cached_error_rows(
        self, mock_genai, mock_types
    ) -> None:
        identity = _identity()
        predictions_uri = eval_artifacts.online_prediction_uri(
            "gs://bucket/run", "checkpoint_6"
        )
        metadata_uri = eval_artifacts.online_prediction_metadata_uri(
            "gs://bucket/run", "checkpoint_6"
        )
        self.storage.put(
            predictions_uri,
            (
                '{"audio_filepath":"gs://audio/1.flac",'
                '"pred_text":"one","error":null}\n'
                '{"audio_filepath":"gs://audio/2.flac",'
                '"pred_text":"","error":"TimeoutError: timed out"}\n'
            ),
        )
        self.storage.put(metadata_uri, _metadata(identity))

        class Response:
            text = "two retried"

        calls = []

        async def generate_content(**kwargs):
            calls.append(
                kwargs["contents"][-1]["parts"][-1]["fileData"]["fileUri"]
            )
            return Response()

        mock_client = unittest.mock.MagicMock()
        mock_client.aio.models.generate_content = generate_content
        mock_genai.Client.return_value = mock_client
        mock_types.GenerateContentConfig.side_effect = lambda **kwargs: kwargs

        result = asyncio.run(
            target_execution.run_online_target_inference(
                storage_client=self.storage,
                run_gcs_prefix="gs://bucket/run",
                project="project",
                default_location="us-central1",
                target_label="checkpoint_6",
                target_model="projects/p/locations/us-central1/endpoints/123",
                audio_uris=["gs://audio/1.flac", "gs://audio/2.flac"],
                histories=[[], []],
                system_prompt="system",
                user_prompt="user",
                prior_context_count=8,
                prior_context_mode="text_turns",
                eval_manifest_uri="gs://data/eval.jsonl",
                local_dir=self.local_dir,
                concurrency=2,
                max_retries=1,
            )
        )

        self.assertEqual(calls, ["gs://audio/2.flac"])
        self.assertEqual(
            dict(result),
            {
                "gs://audio/1.flac": "one",
                "gs://audio/2.flac": "two retried",
            },
        )
        self.assertEqual(result.error_count, 0)

    @unittest.mock.patch("gemini_sft.target_execution.vertex.types")
    @unittest.mock.patch("gemini_sft.target_execution.vertex.genai")
    def test_unresolved_online_errors_are_not_predictions(
        self, mock_genai, mock_types
    ) -> None:
        class Response:
            text = "one"

        async def generate_content(**kwargs):
            audio_uri = kwargs["contents"][-1]["parts"][-1]["fileData"][
                "fileUri"
            ]
            if audio_uri.endswith("2.flac"):
                msg = "temporary outage"
                raise RuntimeError(msg)
            return Response()

        mock_client = unittest.mock.MagicMock()
        mock_client.aio.models.generate_content = generate_content
        mock_genai.Client.return_value = mock_client
        mock_types.GenerateContentConfig.side_effect = lambda **kwargs: kwargs

        result = asyncio.run(
            target_execution.run_online_target_inference(
                storage_client=self.storage,
                run_gcs_prefix="gs://bucket/run",
                project="project",
                default_location="us-central1",
                target_label="checkpoint_6",
                target_model="projects/p/locations/us-central1/endpoints/123",
                audio_uris=["gs://audio/1.flac", "gs://audio/2.flac"],
                histories=[[], []],
                system_prompt="system",
                user_prompt="user",
                prior_context_count=8,
                prior_context_mode="text_turns",
                eval_manifest_uri="gs://data/eval.jsonl",
                local_dir=self.local_dir,
                concurrency=2,
                max_retries=1,
            )
        )

        self.assertEqual(dict(result), {"gs://audio/1.flac": "one"})
        self.assertEqual(result.error_count, 1)
        raw_rows = [
            json.loads(line)
            for line in self.storage.get(
                result.online_predictions_uri
            ).splitlines()
        ]
        self.assertEqual(
            {row["audio_filepath"] for row in raw_rows},
            set(_identity()["audio_uris"]),
        )
        self.assertTrue(
            any(
                row["audio_filepath"].endswith("2.flac") and row["error"]
                for row in raw_rows
            )
        )


class TestGenerateResponse(unittest.TestCase):
    def test_empty_text_is_successful_prediction(self) -> None:
        class Response:
            text = ""

        class Models:
            async def generate_content(self, **kwargs):
                return Response()

        client = unittest.mock.MagicMock()
        client.aio.models = Models()

        prediction, error = asyncio.run(
            target_execution._generate_response(
                client=client,
                model_id="projects/p/locations/us-central1/endpoints/123",
                contents=[],
                config={},
            )
        )

        self.assertEqual(prediction, "")
        self.assertIsNone(error)

    def test_response_text_exception_is_captured_after_one_sdk_call(
        self,
    ) -> None:
        class Response:
            @property
            def text(self) -> str:
                msg = "response contains no candidates"
                raise ValueError(msg)

        class Models:
            def __init__(self) -> None:
                self.calls = 0

            async def generate_content(self, **kwargs):
                self.calls += 1
                return Response()

        models = Models()
        client = unittest.mock.MagicMock()
        client.aio.models = models

        prediction, error = asyncio.run(
            target_execution._generate_response(
                client=client,
                model_id="projects/p/locations/us-central1/endpoints/123",
                contents=[],
                config={},
            )
        )

        self.assertEqual(prediction, "")
        self.assertEqual(
            error,
            "ValueError: response contains no candidates",
        )
        self.assertEqual(models.calls, 1)


if __name__ == "__main__":
    unittest.main()
