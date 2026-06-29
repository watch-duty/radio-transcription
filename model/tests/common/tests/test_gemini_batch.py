from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from common.gemini import request_identity
from common.gemini.context import ContextTurn
from common.gemini.batch import (
    batch_prediction_metadata_uri,
    build_batch_jsonl,
    run_batch_audio_inference,
)
from common.gemini.vertex import GEMINI_GENERATION_CONFIG, GEMINI_SAFETY_SETTINGS
from fake_gcs import FakeStorageClient


def _vertex_output(audio_uri: str, text: str) -> str:
    return json.dumps(
        {
            "request": {
                "contents": [
                    {
                        "parts": [
                            {
                                "fileData": {
                                    "fileUri": audio_uri,
                                }
                            }
                        ]
                    }
                ]
            },
            "response": {
                "candidates": [{"content": {"parts": [{"text": text}]}}]
            },
        }
    )


def _batch_identity_kwargs() -> dict[str, object]:
    return {
        "prior_context_count": 0,
        "prior_context_mode": "text_turns",
        "eval_manifest_uri": "gs://data/eval.jsonl",
    }


def _put_batch_metadata(
    storage: FakeStorageClient,
    *,
    run_gcs_prefix: str = "gs://bucket/sft/runs/run-a",
    label: str = "base",
    model: str = "gemini-3.1-flash-lite",
    audio_uris: list[str] | None = None,
    system_prompt: str = "sys",
    user_prompt: str = "user",
) -> None:
    identity = request_identity.build_request_identity(
        target_label=label,
        model=model,
        eval_manifest_uri=str(_batch_identity_kwargs()["eval_manifest_uri"]),
        audio_uris=audio_uris or ["gs://audio/a.flac"],
        system_prompt=system_prompt,
        user_prompt=user_prompt,
        prior_context_count=int(_batch_identity_kwargs()["prior_context_count"]),
        prior_context_mode=str(_batch_identity_kwargs()["prior_context_mode"]),
        generation_config=GEMINI_GENERATION_CONFIG,
        safety_settings=GEMINI_SAFETY_SETTINGS,
    )
    storage.put(
        batch_prediction_metadata_uri(run_gcs_prefix, label),
        json.dumps(request_identity.metadata_payload(identity), sort_keys=True)
        + "\n",
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
            input_uri, "gs://bucket/sft/runs/run-a/evals/base/input.jsonl"
        )
        self.assertEqual(
            output_uri, "gs://bucket/sft/runs/run-a/evals/base/output/"
        )
        rows = [
            json.loads(line)
            for line in storage.get(input_uri).splitlines()
            if line.strip()
        ]
        current_audio_part = next(
            part
            for part in rows[0]["request"]["contents"][0]["parts"]
            if "file_data" in part
        )
        self.assertEqual(
            current_audio_part["file_data"]["file_uri"], "gs://audio/a.flac"
        )

    def test_build_batch_jsonl_uploads_context_requests(self) -> None:
        storage = FakeStorageClient()
        with tempfile.TemporaryDirectory() as tmp_s:
            input_uri, _ = build_batch_jsonl(
                storage_client=storage,
                run_gcs_prefix="gs://bucket/sft/runs/run-a",
                label="base",
                audio_uris=["gs://audio/current.flac"],
                system_prompt="sys",
                user_prompt="user",
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
        self.assertEqual(
            contents[0]["parts"][0]["file_data"]["file_uri"],
            "gs://audio/prior.flac",
        )
        self.assertEqual(
            contents[1]["parts"][0]["text"],
            "prior transcript",
        )
        self.assertEqual(
            contents[2]["parts"][1]["file_data"]["file_uri"],
            "gs://audio/current.flac",
        )

    def test_run_batch_audio_inference_returns_predictions_and_output_uri(
        self,
    ) -> None:
        storage = FakeStorageClient()
        output_uri = "gs://bucket/sft/runs/run-a/evals/base/output/"
        storage.put(
            f"{output_uri}predictions.jsonl",
            _vertex_output("gs://audio/a.flac", "copy") + "\n",
        )
        _put_batch_metadata(storage)

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
            **_batch_identity_kwargs(),
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
        output_uri = "gs://bucket/sft/runs/run-a/evals/base/output/"
        storage.put(
            f"{output_uri}prediction-model-1/predictions.jsonl",
            _vertex_output("gs://audio/a.flac", "copy") + "\n",
        )
        _put_batch_metadata(storage)
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
            **_batch_identity_kwargs(),
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
        output_uri = f"{run_gcs_prefix}/evals/base/output/"

        def fail_after_partial_output(**_: object) -> str:
            storage.put(
                f"{output_uri}prediction-model-1/predictions.jsonl",
                _vertex_output("gs://audio/a.flac", "partial") + "\n",
            )
            raise RuntimeError("batch failed")

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
            **_batch_identity_kwargs(),
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
            **_batch_identity_kwargs(),
            submit_fn=lambda **kwargs: calls.append(kwargs) or "",
        )

        self.assertIsNone(preds)
        self.assertEqual(calls, [])

    def test_run_batch_audio_inference_rejects_extra_prediction_uri(
        self,
    ) -> None:
        storage = FakeStorageClient()
        output_uri = "gs://bucket/sft/runs/run-a/evals/base/output/"
        storage.put(
            f"{output_uri}predictions.jsonl",
            _vertex_output("gs://audio/other.flac", "other") + "\n",
        )
        _put_batch_metadata(storage)

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
            **_batch_identity_kwargs(),
            submit_fn=lambda **_: output_uri,
        )

        self.assertIsNone(preds)


if __name__ == "__main__":
    unittest.main()
