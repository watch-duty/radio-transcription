from __future__ import annotations

import json
import pathlib
import tempfile
import unittest

import fake_gcs
import sft_eval_fixtures
from common.gemini import batch, context, eval_artifacts, vertex


class TestGeminiBatchInference(unittest.TestCase):
    def test_build_batch_jsonl_uploads_canonical_requests(self) -> None:
        storage = fake_gcs.FakeStorageClient()
        with tempfile.TemporaryDirectory() as tmp_s:
            input_uri, output_uri = batch.build_batch_jsonl(
                storage_client=storage,
                run_gcs_prefix="gs://bucket/sft/runs/run-a",
                label="base",
                audio_uris=["gs://audio/a.flac"],
                system_prompt="sys",
                user_prompt="user",
                tmp_dir=pathlib.Path(tmp_s),
            )

        self.assertEqual(
            input_uri,
            sft_eval_fixtures.batch_input_uri("gs://bucket/sft/runs/run-a"),
        )
        self.assertEqual(
            output_uri,
            sft_eval_fixtures.batch_output_uri("gs://bucket/sft/runs/run-a"),
        )
        rows = [
            json.loads(line)
            for line in storage.get(input_uri).splitlines()
            if line.strip()
        ]
        current_audio_part = next(
            part
            for part in rows[0]["request"]["contents"][0]["parts"]
            if "fileData" in part
        )
        self.assertEqual(
            current_audio_part["fileData"]["fileUri"], "gs://audio/a.flac"
        )

    def test_build_batch_jsonl_uploads_text_turn_context_requests(self) -> None:
        storage = fake_gcs.FakeStorageClient()
        user_prompt = "user"
        with tempfile.TemporaryDirectory() as tmp_s:
            input_uri, _ = batch.build_batch_jsonl(
                storage_client=storage,
                run_gcs_prefix="gs://bucket/sft/runs/run-a",
                label="base",
                audio_uris=["gs://audio/current.flac"],
                system_prompt="sys",
                user_prompt=user_prompt,
                histories=[
                    [
                        context.ContextTurn(
                            "gs://audio/prior.flac", "prior transcript"
                        )
                    ]
                ],
                tmp_dir=pathlib.Path(tmp_s),
            )

        rows = [
            json.loads(line)
            for line in storage.get(input_uri).splitlines()
            if line.strip()
        ]
        contents = rows[0]["request"]["contents"]
        self.assertEqual(
            [turn["role"] for turn in contents],
            ["user", "model", "user"],
        )
        audio_parts = []
        for turn in contents:
            for part in turn["parts"]:
                if "fileData" in part:
                    audio_parts.append(part)
        self.assertEqual(len(audio_parts), 1)
        self.assertEqual(
            audio_parts[0]["fileData"]["fileUri"],
            "gs://audio/current.flac",
        )
        self.assertEqual(
            contents[0]["parts"][0]["text"],
            user_prompt,
        )
        self.assertEqual(
            contents[1]["parts"][0]["text"],
            "prior transcript",
        )
        self.assertEqual(
            contents[2]["parts"][0]["text"],
            user_prompt,
        )

    def test_build_batch_jsonl_rejects_duplicate_audio_uris(self) -> None:
        storage = fake_gcs.FakeStorageClient()
        with tempfile.TemporaryDirectory() as tmp_s:
            with self.assertRaisesRegex(ValueError, "duplicate audio_uri"):
                batch.build_batch_jsonl(
                    storage_client=storage,
                    run_gcs_prefix="gs://bucket/sft/runs/run-a",
                    label="base",
                    audio_uris=[
                        "gs://audio/a.flac",
                        "gs://audio/a.flac",
                    ],
                    system_prompt="sys",
                    user_prompt="user",
                    tmp_dir=pathlib.Path(tmp_s),
                )

        self.assertFalse(
            storage.has(
                sft_eval_fixtures.batch_input_uri("gs://bucket/sft/runs/run-a")
            )
        )

    def test_run_batch_audio_inference_matches_payload_to_context_mode(
        self,
    ) -> None:
        storage = fake_gcs.FakeStorageClient()
        run_gcs_prefix = "gs://bucket/sft/runs/run-a"
        output_uri = sft_eval_fixtures.batch_output_uri(run_gcs_prefix)
        current_audio_uri = "gs://audio/current.flac"
        history = [
            context.ContextTurn("gs://audio/prior.flac", "prior transcript")
        ]

        def submit_batch(**_: object) -> str:
            storage.put(
                f"{output_uri}predictions.jsonl",
                sft_eval_fixtures.vertex_batch_output(current_audio_uri, "copy")
                + "\n",
            )
            return output_uri

        preds = batch.run_batch_audio_inference(
            storage_client=storage,
            run_gcs_prefix=run_gcs_prefix,
            gcp_project="project",
            location="us-central1",
            model_id="gemini-3.1-flash-lite",
            label="base",
            audio_uris=[current_audio_uri],
            system_prompt="sys",
            user_prompt="user",
            prior_context_count=1,
            prior_context_mode="guarded_transcript_block",
            eval_manifest_uri="gs://data/eval.jsonl",
            histories=[history],
            submit_fn=submit_batch,
        )

        self.assertIsNotNone(preds)
        metadata = json.loads(
            storage.get(
                eval_artifacts.batch_prediction_metadata_uri(
                    run_gcs_prefix, "base"
                )
            )
        )
        stored_mode = metadata["request_identity"]["prior_context_mode"]
        self.assertEqual(stored_mode, "guarded_transcript_block")
        uploaded_request = json.loads(
            storage.get(sft_eval_fixtures.batch_input_uri(run_gcs_prefix))
        )
        expected_request = vertex.build_request(
            current_audio_uri,
            system_prompt="sys",
            user_prompt="user",
            history=history,
            history_mode=stored_mode,
        )
        self.assertEqual(uploaded_request, expected_request)

    def test_run_batch_audio_inference_returns_predictions_and_output_uri(
        self,
    ) -> None:
        storage = fake_gcs.FakeStorageClient()
        output_uri = sft_eval_fixtures.batch_output_uri(
            "gs://bucket/sft/runs/run-a"
        )
        storage.put(
            f"{output_uri}predictions.jsonl",
            sft_eval_fixtures.vertex_batch_output("gs://audio/a.flac", "copy")
            + "\n",
        )
        sft_eval_fixtures.put_batch_metadata(
            storage,
            run_gcs_prefix="gs://bucket/sft/runs/run-a",
        )

        preds = batch.run_batch_audio_inference(
            storage_client=storage,
            run_gcs_prefix="gs://bucket/sft/runs/run-a",
            gcp_project="project",
            location="us-central1",
            model_id="gemini-3.1-flash-lite",
            label="base",
            audio_uris=["gs://audio/a.flac"],
            system_prompt="sys",
            user_prompt="user",
            **sft_eval_fixtures.batch_identity_kwargs(),
            submit_fn=lambda **_: output_uri,
        )

        self.assertIsNotNone(preds)
        assert preds is not None
        self.assertEqual(preds, {"gs://audio/a.flac": "copy"})
        self.assertEqual(preds.output_uri, output_uri)

    def test_run_batch_audio_inference_reuses_existing_predictions(
        self,
    ) -> None:
        storage = fake_gcs.FakeStorageClient()
        output_uri = sft_eval_fixtures.batch_output_uri(
            "gs://bucket/sft/runs/run-a"
        )
        storage.put(
            f"{output_uri}prediction-model-1/predictions.jsonl",
            sft_eval_fixtures.vertex_batch_output("gs://audio/a.flac", "copy")
            + "\n",
        )
        sft_eval_fixtures.put_batch_metadata(
            storage,
            run_gcs_prefix="gs://bucket/sft/runs/run-a",
        )
        calls: list[dict[str, object]] = []

        preds = batch.run_batch_audio_inference(
            storage_client=storage,
            run_gcs_prefix="gs://bucket/sft/runs/run-a",
            gcp_project="project",
            location="us-central1",
            model_id="gemini-3.1-flash-lite",
            label="base",
            audio_uris=["gs://audio/a.flac"],
            system_prompt="sys",
            user_prompt="user",
            **sft_eval_fixtures.batch_identity_kwargs(),
            submit_fn=lambda **kwargs: calls.append(kwargs) or output_uri,
        )

        self.assertIsNotNone(preds)
        assert preds is not None
        self.assertEqual(preds, {"gs://audio/a.flac": "copy"})
        self.assertEqual(preds.output_uri, output_uri)
        self.assertEqual(calls, [])

    def test_reuse_rejects_duplicate_prediction_uri_across_blobs(self) -> None:
        storage = fake_gcs.FakeStorageClient()
        run_gcs_prefix = "gs://bucket/sft/runs/run-a"
        output_uri = sft_eval_fixtures.batch_output_uri(run_gcs_prefix)
        audio_uri = "gs://audio/a.flac"
        storage.put(
            f"{output_uri}job-1/predictions.jsonl",
            sft_eval_fixtures.vertex_batch_output(audio_uri, "first") + "\n",
        )
        storage.put(
            f"{output_uri}job-2/predictions.jsonl",
            sft_eval_fixtures.vertex_batch_output(audio_uri, "second") + "\n",
        )
        sft_eval_fixtures.put_batch_metadata(
            storage,
            run_gcs_prefix=run_gcs_prefix,
        )

        preds = batch.run_batch_audio_inference(
            storage_client=storage,
            run_gcs_prefix=run_gcs_prefix,
            gcp_project="project",
            location="us-central1",
            model_id="gemini-3.1-flash-lite",
            label="base",
            audio_uris=[audio_uri],
            system_prompt="sys",
            user_prompt="user",
            **sft_eval_fixtures.batch_identity_kwargs(),
        )

        self.assertIsNone(preds)

    def test_run_batch_audio_inference_reuses_matching_history(self) -> None:
        storage = fake_gcs.FakeStorageClient()
        run_gcs_prefix = "gs://bucket/sft/runs/run-a"
        output_uri = sft_eval_fixtures.batch_output_uri(run_gcs_prefix)
        audio_uri = "gs://audio/a.flac"
        histories = [
            [
                context.ContextTurn(
                    "gs://audio/prior.flac", "matching transcript"
                )
            ]
        ]
        storage.put(
            f"{output_uri}predictions.jsonl",
            sft_eval_fixtures.vertex_batch_output(audio_uri, "copy") + "\n",
        )
        sft_eval_fixtures.put_batch_metadata(
            storage,
            run_gcs_prefix=run_gcs_prefix,
            audio_uris=[audio_uri],
            prior_context_count=1,
            histories=histories,
        )
        calls: list[dict[str, object]] = []

        preds = batch.run_batch_audio_inference(
            storage_client=storage,
            run_gcs_prefix=run_gcs_prefix,
            gcp_project="project",
            location="us-central1",
            model_id="gemini-3.1-flash-lite",
            label="base",
            audio_uris=[audio_uri],
            system_prompt="sys",
            user_prompt="user",
            prior_context_count=1,
            prior_context_mode="text_turns",
            eval_manifest_uri="gs://data/eval.jsonl",
            histories=histories,
            submit_fn=lambda **kwargs: calls.append(kwargs) or output_uri,
        )

        self.assertEqual(preds, {audio_uri: "copy"})
        self.assertEqual(calls, [])

    def test_run_batch_audio_inference_rejects_changed_history(self) -> None:
        storage = fake_gcs.FakeStorageClient()
        run_gcs_prefix = "gs://bucket/sft/runs/run-a"
        output_uri = sft_eval_fixtures.batch_output_uri(run_gcs_prefix)
        input_uri = sft_eval_fixtures.batch_input_uri(run_gcs_prefix)
        audio_uri = "gs://audio/a.flac"
        original_input = '{"request":"original"}\n'
        storage.put(input_uri, original_input)
        storage.put(
            f"{output_uri}predictions.jsonl",
            sft_eval_fixtures.vertex_batch_output(audio_uri, "copy") + "\n",
        )
        sft_eval_fixtures.put_batch_metadata(
            storage,
            run_gcs_prefix=run_gcs_prefix,
            audio_uris=[audio_uri],
            prior_context_count=1,
            histories=[
                [context.ContextTurn("gs://audio/prior.flac", "old transcript")]
            ],
        )
        calls: list[dict[str, object]] = []

        with self.assertRaisesRegex(
            ValueError,
            "batch prediction request identity mismatch",
        ):
            batch.run_batch_audio_inference(
                storage_client=storage,
                run_gcs_prefix=run_gcs_prefix,
                gcp_project="project",
                location="us-central1",
                model_id="gemini-3.1-flash-lite",
                label="base",
                audio_uris=[audio_uri],
                system_prompt="sys",
                user_prompt="user",
                prior_context_count=1,
                prior_context_mode="text_turns",
                eval_manifest_uri="gs://data/eval.jsonl",
                histories=[
                    [
                        context.ContextTurn(
                            "gs://audio/prior.flac", "new transcript"
                        )
                    ]
                ],
                submit_fn=lambda **kwargs: calls.append(kwargs) or output_uri,
            )

        self.assertEqual(calls, [])
        self.assertEqual(storage.get(input_uri), original_input)

    def test_run_batch_audio_inference_does_not_mark_failed_submit_reusable(
        self,
    ) -> None:
        storage = fake_gcs.FakeStorageClient()
        run_gcs_prefix = "gs://bucket/sft/runs/run-a"
        output_uri = sft_eval_fixtures.batch_output_uri(run_gcs_prefix)

        def fail_after_partial_output(**_: object) -> str:
            storage.put(
                f"{output_uri}prediction-model-1/predictions.jsonl",
                sft_eval_fixtures.vertex_batch_output(
                    "gs://audio/a.flac", "partial"
                )
                + "\n",
            )
            msg = "batch failed"
            raise RuntimeError(msg)

        preds = batch.run_batch_audio_inference(
            storage_client=storage,
            run_gcs_prefix=run_gcs_prefix,
            gcp_project="project",
            location="us-central1",
            model_id="gemini-3.1-flash-lite",
            label="base",
            audio_uris=["gs://audio/a.flac"],
            system_prompt="sys",
            user_prompt="user",
            **sft_eval_fixtures.batch_identity_kwargs(),
            submit_fn=fail_after_partial_output,
        )

        self.assertIsNone(preds)
        self.assertFalse(
            storage.has(
                eval_artifacts.batch_prediction_metadata_uri(
                    run_gcs_prefix, "base"
                )
            )
        )

    def test_new_batch_rejects_extra_uri_before_marking_reusable(self) -> None:
        storage = fake_gcs.FakeStorageClient()
        run_gcs_prefix = "gs://bucket/sft/runs/run-a"
        output_uri = sft_eval_fixtures.batch_output_uri(run_gcs_prefix)

        def submit_with_extra_uri(**_: object) -> str:
            storage.put(
                f"{output_uri}predictions.jsonl",
                sft_eval_fixtures.vertex_batch_output(
                    "gs://audio/other.flac", "other"
                )
                + "\n",
            )
            return output_uri

        preds = batch.run_batch_audio_inference(
            storage_client=storage,
            run_gcs_prefix=run_gcs_prefix,
            gcp_project="project",
            location="us-central1",
            model_id="gemini-3.1-flash-lite",
            label="base",
            audio_uris=["gs://audio/a.flac"],
            system_prompt="sys",
            user_prompt="user",
            **sft_eval_fixtures.batch_identity_kwargs(),
            submit_fn=submit_with_extra_uri,
        )

        self.assertIsNone(preds)
        self.assertFalse(
            storage.has(
                eval_artifacts.batch_prediction_metadata_uri(
                    run_gcs_prefix, "base"
                )
            )
        )

    def test_run_batch_audio_inference_rejects_duplicate_audio_uris(
        self,
    ) -> None:
        calls: list[dict[str, object]] = []

        preds = batch.run_batch_audio_inference(
            storage_client=fake_gcs.FakeStorageClient(),
            run_gcs_prefix="gs://bucket/sft/runs/run-a",
            gcp_project="project",
            location="us-central1",
            model_id="gemini-3.1-flash-lite",
            label="base",
            audio_uris=["gs://audio/a.flac", "gs://audio/a.flac"],
            system_prompt="sys",
            user_prompt="user",
            **sft_eval_fixtures.batch_identity_kwargs(),
            submit_fn=lambda **kwargs: calls.append(kwargs) or "",
        )

        self.assertIsNone(preds)
        self.assertEqual(calls, [])

    def test_run_batch_audio_inference_rejects_extra_prediction_uri(
        self,
    ) -> None:
        storage = fake_gcs.FakeStorageClient()
        output_uri = sft_eval_fixtures.batch_output_uri(
            "gs://bucket/sft/runs/run-a"
        )
        storage.put(
            f"{output_uri}predictions.jsonl",
            sft_eval_fixtures.vertex_batch_output(
                "gs://audio/other.flac", "other"
            )
            + "\n",
        )
        sft_eval_fixtures.put_batch_metadata(
            storage,
            run_gcs_prefix="gs://bucket/sft/runs/run-a",
        )

        preds = batch.run_batch_audio_inference(
            storage_client=storage,
            run_gcs_prefix="gs://bucket/sft/runs/run-a",
            gcp_project="project",
            location="us-central1",
            model_id="gemini-3.1-flash-lite",
            label="base",
            audio_uris=["gs://audio/a.flac"],
            system_prompt="sys",
            user_prompt="user",
            **sft_eval_fixtures.batch_identity_kwargs(),
            submit_fn=lambda **_: output_uri,
        )

        self.assertIsNone(preds)


if __name__ == "__main__":
    unittest.main()
