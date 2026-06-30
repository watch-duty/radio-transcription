from __future__ import annotations

# ruff: noqa: E402
import asyncio
import json
import sys
import unittest
import unittest.mock
from pathlib import Path

_SRC_DIR = str(Path(__file__).resolve().parents[2] / "src")
if _SRC_DIR not in sys.path:
    sys.path.insert(0, _SRC_DIR)

from common.gemini.context import ContextTurn
from common.gemini.eval_artifacts import (
    eval_target_artifact_paths,
)
from common.gemini.request_identity import (
    build_gemini_eval_request_identity,
    request_identity_hash,
)
from common.gemini.vertex import (
    GEMINI_SAFETY_SETTINGS,
)
from fake_gcs import FakeStorageClient
from gemini_sft.config import EvalExecutionConfig, EvalModelTarget
from gemini_sft.target_execution import (
    _generate_with_retries,
    load_existing_online_predictions,
    online_prediction_metadata_uri,
    online_prediction_uri,
    resolve_target_backend,
    run_online_target_inference,
    upload_local_file,
    upload_text,
)


def _identity(
    *,
    model: str = "projects/p/locations/us-central1/endpoints/123",
    audio_uris: list[str] | None = None,
    system_prompt: str = "system",
    user_prompt: str = "user",
) -> dict:
    return build_gemini_eval_request_identity(
        target_label="checkpoint_6",
        model=model,
        eval_manifest_uri="gs://data/eval.jsonl",
        audio_uris=audio_uris or ["gs://audio/1.flac", "gs://audio/2.flac"],
        system_prompt=system_prompt,
        user_prompt=user_prompt,
        prior_context_count=8,
        prior_context_mode="text_turns",
    )


def _metadata(identity: dict) -> str:
    return (
        json.dumps(
            {
                "request_identity_hash": request_identity_hash(identity),
                "request_identity": identity,
            },
            sort_keys=True,
        )
        + "\n"
    )


def _line_count(path: Path) -> int:
    if not path.exists():
        return 0
    return len(path.read_text(encoding="utf-8").splitlines())


class TestTargetBackendResolver(unittest.TestCase):
    def test_publisher_model_defaults_to_batch(self) -> None:
        backend = resolve_target_backend(
            EvalModelTarget(label="base", model="gemini-3.1-flash-lite"),
            EvalExecutionConfig(),
        )

        self.assertEqual(backend, "batch")

    def test_endpoint_resource_defaults_to_online(self) -> None:
        backend = resolve_target_backend(
            EvalModelTarget(
                label="checkpoint_6",
                model="projects/p/locations/us-central1/endpoints/123",
            ),
            EvalExecutionConfig(),
        )

        self.assertEqual(backend, "online")

    def test_forced_backend_overrides_model_shape(self) -> None:
        publisher = EvalModelTarget(
            label="base",
            model="gemini-3.1-flash-lite",
        )
        endpoint = EvalModelTarget(
            label="checkpoint_6",
            model="projects/p/locations/us-central1/endpoints/123",
        )

        self.assertEqual(
            resolve_target_backend(
                publisher,
                EvalExecutionConfig(backend="online"),
            ),
            "online",
        )
        self.assertEqual(
            resolve_target_backend(
                endpoint,
                EvalExecutionConfig(backend="batch"),
            ),
            "batch",
        )


class TestOnlineRequestIdentity(unittest.TestCase):
    def test_prediction_uris_are_under_eval_label(self) -> None:
        prefix = "gs://bucket/runs/run-1/"
        paths = eval_target_artifact_paths(prefix, "checkpoint_6")

        self.assertEqual(
            online_prediction_uri(prefix, "checkpoint_6"),
            paths.online_predictions_uri,
        )
        self.assertEqual(
            online_prediction_metadata_uri(prefix, "checkpoint_6"),
            paths.online_metadata_uri,
        )

    def test_hash_is_stable_for_key_order(self) -> None:
        identity = _identity()
        reordered = {key: identity[key] for key in reversed(identity)}

        self.assertEqual(
            request_identity_hash(identity),
            request_identity_hash(reordered),
        )

    def test_hash_changes_for_request_content(self) -> None:
        base = request_identity_hash(_identity())

        self.assertNotEqual(
            base,
            request_identity_hash(_identity(system_prompt="different")),
        )
        self.assertNotEqual(
            base,
            request_identity_hash(_identity(model="gemini-3.1-flash-lite")),
        )
        self.assertNotEqual(
            base,
            request_identity_hash(
                _identity(audio_uris=["gs://audio/2.flac", "gs://audio/1.flac"])
            ),
        )

    def test_identity_excludes_operational_settings(self) -> None:
        identity = _identity()

        self.assertNotIn("concurrency", identity)
        self.assertNotIn("max_retries", identity)


class TestOnlinePredictionResume(unittest.TestCase):
    def setUp(self) -> None:
        self.storage = FakeStorageClient()
        self.predictions_uri = online_prediction_uri(
            "gs://bucket/run", "checkpoint_6"
        )
        self.metadata_uri = online_prediction_metadata_uri(
            "gs://bucket/run", "checkpoint_6"
        )
        self.local_dir = Path(self.id().replace(".", "_"))
        self.addCleanup(
            lambda: __import__("shutil").rmtree(
                self.local_dir, ignore_errors=True
            )
        )

    def _load(self, identity: dict):
        return load_existing_online_predictions(
            storage_client=self.storage,
            predictions_uri=self.predictions_uri,
            metadata_uri=self.metadata_uri,
            local_predictions_path=self.local_dir / "online_predictions.jsonl",
            local_metadata_path=self.local_dir / "online_predictions.meta.json",
            request_identity=identity,
        )

    def test_exact_resume_returns_completed_rows_and_error_count(self) -> None:
        identity = _identity()
        self.storage.put(
            self.predictions_uri,
            (
                '{"audio_filepath":"gs://audio/1.flac","pred_text":"one","error":null}\n'
                '{"audio_filepath":"gs://audio/2.flac","pred_text":"","error":"empty response"}\n'
            ),
        )
        self.storage.put(self.metadata_uri, _metadata(identity))

        state = self._load(identity)

        self.assertEqual(
            set(state.rows_by_audio_uri), set(identity["audio_uris"])
        )
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
                '{"audio_filepath":"gs://audio/1.flac","pred_text":"one","error":null}\n'
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
        self.storage = FakeStorageClient()
        self.local_dir = Path(self.id().replace(".", "_"))
        self.addCleanup(
            lambda: __import__("shutil").rmtree(
                self.local_dir, ignore_errors=True
            )
        )

    @unittest.mock.patch("gemini_sft.target_execution.types")
    @unittest.mock.patch("gemini_sft.target_execution.genai")
    def test_rejects_duplicate_audio_uris_before_paid_calls(
        self, mock_genai, mock_types
    ) -> None:
        mock_types.GenerateContentConfig.side_effect = lambda **kwargs: kwargs

        with self.assertRaisesRegex(ValueError, "duplicate audio_uri"):
            asyncio.run(
                run_online_target_inference(
                    storage_client=self.storage,
                    run_gcs_prefix="gs://bucket/run",
                    project="project",
                    default_location="us-central1",
                    target_label="checkpoint_6",
                    target_model="projects/p/locations/us-central1/endpoints/123",
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

    @unittest.mock.patch("gemini_sft.target_execution.types")
    @unittest.mock.patch("gemini_sft.target_execution.genai")
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
        mock_types.GenerateContentConfig.side_effect = lambda **kwargs: kwargs

        result = asyncio.run(
            run_online_target_inference(
                storage_client=self.storage,
                run_gcs_prefix="gs://bucket/run",
                project="project",
                default_location="us-central1",
                target_label="checkpoint_6",
                target_model="projects/p/locations/us-central1/endpoints/123",
                audio_uris=["gs://audio/1.flac", "gs://audio/2.flac"],
                histories=[
                    [ContextTurn(audio_uri="gs://audio/0.flac", text="prior")],
                    [],
                ],
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
            calls[0]["config"]["safety_settings"], GEMINI_SAFETY_SETTINGS
        )
        self.assertIn(
            '"error": null', self.storage.get(result.online_predictions_uri)
        )

    @unittest.mock.patch("gemini_sft.target_execution.types")
    @unittest.mock.patch("gemini_sft.target_execution.genai")
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
                run_online_target_inference(
                    storage_client=self.storage,
                    run_gcs_prefix="gs://bucket/run",
                    project="project",
                    default_location="us-central1",
                    target_label="checkpoint_6",
                    target_model="projects/p/locations/us-central1/endpoints/123",
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
            sum(1 for fn, _, _ in to_thread_calls if fn is upload_local_file),
            1,
        )
        self.assertGreaterEqual(
            sum(1 for fn, _, _ in to_thread_calls if fn is upload_text),
            1,
        )

    @unittest.mock.patch("gemini_sft.target_execution.types")
    @unittest.mock.patch("gemini_sft.target_execution.genai")
    def test_periodic_upload_does_not_hold_append_lock(
        self, mock_genai, mock_types
    ) -> None:
        class Response:
            text = "recognized"

        async def generate_content(**kwargs):
            return Response()

        async def run_scenario() -> None:
            upload_started = asyncio.Event()
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

            async def wait_for_two_rows(path: Path) -> None:
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
                    run_online_target_inference(
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
                finally:
                    release_upload.set()
                    await task
            self.assertEqual(first_snapshot_line_count, 1)

        mock_client = unittest.mock.MagicMock()
        mock_client.aio.models.generate_content = generate_content
        mock_genai.Client.return_value = mock_client
        mock_types.GenerateContentConfig.side_effect = lambda **kwargs: kwargs

        asyncio.run(run_scenario())

    @unittest.mock.patch("gemini_sft.target_execution.genai")
    def test_safe_resume_skips_existing_rows(self, mock_genai) -> None:
        identity = _identity()
        predictions_uri = online_prediction_uri(
            "gs://bucket/run", "checkpoint_6"
        )
        metadata_uri = online_prediction_metadata_uri(
            "gs://bucket/run", "checkpoint_6"
        )
        self.storage.put(
            predictions_uri,
            (
                '{"audio_filepath":"gs://audio/1.flac","pred_text":"one","error":null}\n'
                '{"audio_filepath":"gs://audio/2.flac","pred_text":"two","error":null}\n'
            ),
        )
        self.storage.put(metadata_uri, _metadata(identity))

        result = asyncio.run(
            run_online_target_inference(
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


class TestGenerateWithRetries(unittest.TestCase):
    def test_empty_text_is_successful_prediction(self) -> None:
        class Response:
            text = ""

        class Models:
            async def generate_content(self, **kwargs):
                return Response()

        client = unittest.mock.MagicMock()
        client.aio.models = Models()

        prediction, error = asyncio.run(
            _generate_with_retries(
                client=client,
                model_id="projects/p/locations/us-central1/endpoints/123",
                contents=[],
                config={},
                max_retries=3,
            )
        )

        self.assertEqual(prediction, "")
        self.assertIsNone(error)

    def test_response_text_exception_is_returned_as_standard_error(
        self,
    ) -> None:
        class Response:
            @property
            def text(self) -> str:
                msg = "response contains no candidates"
                raise ValueError(msg)

        class Models:
            async def generate_content(self, **kwargs):
                return Response()

        client = unittest.mock.MagicMock()
        client.aio.models = Models()

        prediction, error = asyncio.run(
            _generate_with_retries(
                client=client,
                model_id="projects/p/locations/us-central1/endpoints/123",
                contents=[],
                config={},
                max_retries=1,
            )
        )

        self.assertEqual(prediction, "")
        self.assertEqual(
            error,
            "ValueError: response contains no candidates",
        )


if __name__ == "__main__":
    unittest.main()
