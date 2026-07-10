from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from common.gemini import vertex
from common.gemini.batch import (
    build_batch_jsonl,
    run_batch_audio_inference,
)
from common.gemini.context import ContextTurn
from common.gemini.eval_artifacts import batch_prediction_metadata_uri
from fake_gcs import FakeStorageClient
from sft_eval_fixtures import (
    batch_identity_kwargs,
    batch_input_uri,
    batch_output_uri,
    put_batch_metadata,
    vertex_batch_output,
)


class TestGeminiBatchInference(unittest.TestCase):
    def test_build_batch_jsonl_uploads_canonical_requests(self) -> None:
        storage = FakeStorageClient()
        with tempfile.TemporaryDirectory() as tmp_s:
            input_uri, output_uri = build_batch_jsonl(
                storage_client=storage,
                run_gcs_prefix="gs://bucket/sft/runs/run-a",
                label="base",
                audio_uris=["gs://audio/a.flac"],
                system_prompt="sys",
                user_prompt="user",
                tmp_dir=Path(tmp_s),
            )

        self.assertEqual(
            input_uri, batch_input_uri("gs://bucket/sft/runs/run-a")
        )
        self.assertEqual(
            output_uri, batch_output_uri("gs://bucket/sft/runs/run-a")
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
        storage = FakeStorageClient()
        user_prompt = "user"
        with tempfile.TemporaryDirectory() as tmp_s:
            input_uri, _ = build_batch_jsonl(
                storage_client=storage,
                run_gcs_prefix="gs://bucket/sft/runs/run-a",
                label="base",
                audio_uris=["gs://audio/current.flac"],
                system_prompt="sys",
                user_prompt=user_prompt,
                histories=[
                    [ContextTurn("gs://audio/prior.flac", "prior transcript")]
                ],
                tmp_dir=Path(tmp_s),
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
        audio_parts = [
            part
            for turn in contents
            for part in turn["parts"]
            if "fileData" in part
        ]
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
        storage = FakeStorageClient()
        with tempfile.TemporaryDirectory() as tmp_s:
            with self.assertRaisesRegex(ValueError, "duplicate audio_uri"):
                build_batch_jsonl(
                    storage_client=storage,
                    run_gcs_prefix="gs://bucket/sft/runs/run-a",
                    label="base",
                    audio_uris=[
                        "gs://audio/a.flac",
                        "gs://audio/a.flac",
                    ],
                    system_prompt="sys",
                    user_prompt="user",
                    tmp_dir=Path(tmp_s),
                )

        self.assertFalse(
            storage.has(batch_input_uri("gs://bucket/sft/runs/run-a"))
        )

    def test_run_batch_audio_inference_matches_payload_to_context_mode(
        self,
    ) -> None:
        storage = FakeStorageClient()
        run_gcs_prefix = "gs://bucket/sft/runs/run-a"
        output_uri = batch_output_uri(run_gcs_prefix)
        current_audio_uri = "gs://audio/current.flac"
        history = [ContextTurn("gs://audio/prior.flac", "prior transcript")]

        def submit_batch(**_: object) -> str:
            storage.put(
                f"{output_uri}predictions.jsonl",
                vertex_batch_output(current_audio_uri, "copy") + "\n",
            )
            return output_uri

        preds = run_batch_audio_inference(
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
            storage.get(batch_prediction_metadata_uri(run_gcs_prefix, "base"))
        )
        stored_mode = metadata["request_identity"]["prior_context_mode"]
        self.assertEqual(stored_mode, "guarded_transcript_block")
        uploaded_request = json.loads(
            storage.get(batch_input_uri(run_gcs_prefix))
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
        storage = FakeStorageClient()
        output_uri = batch_output_uri("gs://bucket/sft/runs/run-a")
        storage.put(
            f"{output_uri}predictions.jsonl",
            vertex_batch_output("gs://audio/a.flac", "copy") + "\n",
        )
        put_batch_metadata(
            storage,
            run_gcs_prefix="gs://bucket/sft/runs/run-a",
        )

        preds = run_batch_audio_inference(
            storage_client=storage,
            run_gcs_prefix="gs://bucket/sft/runs/run-a",
            gcp_project="project",
            location="us-central1",
            model_id="gemini-3.1-flash-lite",
            label="base",
            audio_uris=["gs://audio/a.flac"],
            system_prompt="sys",
            user_prompt="user",
            **batch_identity_kwargs(),
            submit_fn=lambda **_: output_uri,
        )

        self.assertIsNotNone(preds)
        assert preds is not None
        self.assertEqual(preds, {"gs://audio/a.flac": "copy"})
        self.assertEqual(preds.output_uri, output_uri)

    def test_run_batch_audio_inference_reuses_existing_predictions(
        self,
    ) -> None:
        storage = FakeStorageClient()
        output_uri = batch_output_uri("gs://bucket/sft/runs/run-a")
        storage.put(
            f"{output_uri}prediction-model-1/predictions.jsonl",
            vertex_batch_output("gs://audio/a.flac", "copy") + "\n",
        )
        put_batch_metadata(
            storage,
            run_gcs_prefix="gs://bucket/sft/runs/run-a",
        )
        calls: list[dict[str, object]] = []

        preds = run_batch_audio_inference(
            storage_client=storage,
            run_gcs_prefix="gs://bucket/sft/runs/run-a",
            gcp_project="project",
            location="us-central1",
            model_id="gemini-3.1-flash-lite",
            label="base",
            audio_uris=["gs://audio/a.flac"],
            system_prompt="sys",
            user_prompt="user",
            **batch_identity_kwargs(),
            submit_fn=lambda **kwargs: calls.append(kwargs) or output_uri,
        )

        self.assertIsNotNone(preds)
        assert preds is not None
        self.assertEqual(preds, {"gs://audio/a.flac": "copy"})
        self.assertEqual(preds.output_uri, output_uri)
        self.assertEqual(calls, [])

    def test_run_batch_audio_inference_does_not_mark_failed_submit_reusable(
        self,
    ) -> None:
        storage = FakeStorageClient()
        run_gcs_prefix = "gs://bucket/sft/runs/run-a"
        output_uri = batch_output_uri(run_gcs_prefix)

        def fail_after_partial_output(**_: object) -> str:
            storage.put(
                f"{output_uri}prediction-model-1/predictions.jsonl",
                vertex_batch_output("gs://audio/a.flac", "partial") + "\n",
            )
            msg = "batch failed"
            raise RuntimeError(msg)

        preds = run_batch_audio_inference(
            storage_client=storage,
            run_gcs_prefix=run_gcs_prefix,
            gcp_project="project",
            location="us-central1",
            model_id="gemini-3.1-flash-lite",
            label="base",
            audio_uris=["gs://audio/a.flac"],
            system_prompt="sys",
            user_prompt="user",
            **batch_identity_kwargs(),
            submit_fn=fail_after_partial_output,
        )

        self.assertIsNone(preds)
        self.assertFalse(
            storage.has(batch_prediction_metadata_uri(run_gcs_prefix, "base"))
        )

    def test_run_batch_audio_inference_rejects_duplicate_audio_uris(
        self,
    ) -> None:
        calls: list[dict[str, object]] = []

        preds = run_batch_audio_inference(
            storage_client=FakeStorageClient(),
            run_gcs_prefix="gs://bucket/sft/runs/run-a",
            gcp_project="project",
            location="us-central1",
            model_id="gemini-3.1-flash-lite",
            label="base",
            audio_uris=["gs://audio/a.flac", "gs://audio/a.flac"],
            system_prompt="sys",
            user_prompt="user",
            **batch_identity_kwargs(),
            submit_fn=lambda **kwargs: calls.append(kwargs) or "",
        )

        self.assertIsNone(preds)
        self.assertEqual(calls, [])

    def test_run_batch_audio_inference_rejects_extra_prediction_uri(
        self,
    ) -> None:
        storage = FakeStorageClient()
        output_uri = batch_output_uri("gs://bucket/sft/runs/run-a")
        storage.put(
            f"{output_uri}predictions.jsonl",
            vertex_batch_output("gs://audio/other.flac", "other") + "\n",
        )
        put_batch_metadata(
            storage,
            run_gcs_prefix="gs://bucket/sft/runs/run-a",
        )

        preds = run_batch_audio_inference(
            storage_client=storage,
            run_gcs_prefix="gs://bucket/sft/runs/run-a",
            gcp_project="project",
            location="us-central1",
            model_id="gemini-3.1-flash-lite",
            label="base",
            audio_uris=["gs://audio/a.flac"],
            system_prompt="sys",
            user_prompt="user",
            **batch_identity_kwargs(),
            submit_fn=lambda **_: output_uri,
        )

        self.assertIsNone(preds)


if __name__ == "__main__":
    unittest.main()
