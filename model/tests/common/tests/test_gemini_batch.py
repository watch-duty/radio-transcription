from __future__ import annotations

import json
import pathlib
import tempfile
import unittest
import unittest.mock

import fake_gcs
import sft_eval_fixtures
from common.gemini import (
    batch,
    eval_artifacts,
    request_identity,
)


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
        sft_eval_fixtures.put_batch_reuse_artifacts(
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
        run_gcs_prefix = "gs://bucket/sft/runs/run-a"
        input_uri = sft_eval_fixtures.batch_input_uri(run_gcs_prefix)
        output_uri = sft_eval_fixtures.batch_output_uri(run_gcs_prefix)
        original_input = '{"request":"existing"}\n'
        storage.put(input_uri, original_input)
        storage.put(
            f"{output_uri}prediction-model-1/predictions.jsonl",
            sft_eval_fixtures.vertex_batch_output("gs://audio/a.flac", "copy")
            + "\n",
        )
        sft_eval_fixtures.put_batch_reuse_artifacts(
            storage,
            run_gcs_prefix=run_gcs_prefix,
        )
        calls: list[dict[str, object]] = []

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
            submit_fn=lambda **kwargs: calls.append(kwargs) or output_uri,
        )

        self.assertIsNotNone(preds)
        assert preds is not None
        self.assertEqual(preds, {"gs://audio/a.flac": "copy"})
        self.assertEqual(preds.output_uri, output_uri)
        self.assertEqual(calls, [])
        self.assertEqual(storage.get(input_uri), original_input)
        self.assertNotIn(input_uri, storage.uploads)

    def test_completed_reuse_requires_input_and_job_metadata(self) -> None:
        run_gcs_prefix = "gs://bucket/sft/runs/run-a"
        for missing_path_attribute in (
            "input_uri",
            "batch_job_metadata_uri",
        ):
            with self.subTest(missing_path_attribute=missing_path_attribute):
                storage = fake_gcs.FakeStorageClient()
                paths = eval_artifacts.eval_target_artifact_paths(
                    run_gcs_prefix, "base"
                )
                storage.put(
                    f"{paths.output_uri}predictions.jsonl",
                    sft_eval_fixtures.vertex_batch_output(
                        "gs://audio/a.flac", "copy"
                    )
                    + "\n",
                )
                sft_eval_fixtures.put_batch_reuse_artifacts(
                    storage,
                    run_gcs_prefix=run_gcs_prefix,
                )
                missing_uri = getattr(paths, missing_path_attribute)
                storage.delete(missing_uri)
                submit = unittest.mock.Mock(return_value=paths.output_uri)

                with self.assertRaisesRegex(
                    ValueError,
                    "completed batch prediction artifacts missing",
                ):
                    batch.run_batch_audio_inference(
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
                        submit_fn=submit,
                    )

                submit.assert_not_called()

    def test_completed_reuse_validates_job_request_identity(self) -> None:
        storage = fake_gcs.FakeStorageClient()
        run_gcs_prefix = "gs://bucket/sft/runs/run-a"
        paths = eval_artifacts.eval_target_artifact_paths(
            run_gcs_prefix, "base"
        )
        storage.put(
            f"{paths.output_uri}predictions.jsonl",
            sft_eval_fixtures.vertex_batch_output("gs://audio/a.flac", "copy")
            + "\n",
        )
        sft_eval_fixtures.put_batch_reuse_artifacts(
            storage,
            run_gcs_prefix=run_gcs_prefix,
        )
        job_metadata = json.loads(storage.get(paths.batch_job_metadata_uri))
        stale_identity = job_metadata["request_identity"]
        stale_identity["system_prompt"] = "stale"
        job_metadata["request_identity_hash"] = (
            request_identity.request_identity_hash(stale_identity)
        )
        storage.put(
            paths.batch_job_metadata_uri,
            json.dumps(job_metadata, sort_keys=True) + "\n",
        )

        with self.assertRaisesRegex(
            ValueError,
            "batch job request identity mismatch",
        ):
            batch.run_batch_audio_inference(
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
            )

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
        sft_eval_fixtures.put_batch_reuse_artifacts(
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

    def test_timeout_resumes_job_without_submit_or_input_rewrite(self) -> None:
        storage = fake_gcs.FakeStorageClient()
        run_gcs_prefix = "gs://bucket/sft/runs/run-a"
        paths = eval_artifacts.eval_target_artifact_paths(
            run_gcs_prefix, "base"
        )
        job_name = "projects/p/locations/us/batchPredictionJobs/123"
        prediction_uri = f"{paths.output_uri}job/predictions.jsonl"

        def submit_then_timeout(**kwargs: object) -> str:
            callback = kwargs.get("on_submitted")
            if callback is not None:
                if not callable(callback):
                    self.fail("on_submitted must be callable")
                callback(job_name)
            storage.put(
                prediction_uri,
                json.dumps({"status": {"code": 13}}) + "\n",
            )
            raise TimeoutError

        first_result = batch.run_batch_audio_inference(
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
            submit_fn=submit_then_timeout,
        )

        self.assertIsNone(first_result)
        self.assertTrue(storage.has(paths.batch_job_metadata_uri))
        job_metadata = json.loads(storage.get(paths.batch_job_metadata_uri))
        self.assertEqual(job_metadata["job_name"], job_name)
        self.assertIn("request_identity", job_metadata)
        self.assertFalse(storage.has(paths.batch_metadata_uri))
        original_input = storage.get(paths.input_uri)
        input_uploads = storage.uploads.count(paths.input_uri)
        poll_calls: list[dict[str, object]] = []

        def poll_saved_job(**kwargs: object) -> str:
            poll_calls.append(kwargs)
            storage.put(
                prediction_uri,
                sft_eval_fixtures.vertex_batch_output(
                    "gs://audio/a.flac", "copy"
                )
                + "\n",
            )
            return paths.output_uri

        def unexpected_submit(**_: object) -> str:
            self.fail("saved batch job must be polled without resubmission")

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
            submit_fn=unexpected_submit,
            poll_fn=poll_saved_job,
        )

        self.assertEqual(preds, {"gs://audio/a.flac": "copy"})
        self.assertEqual(
            poll_calls,
            [
                {
                    "name": job_name,
                    "project": "project",
                    "location": "us-central1",
                    "output_uri": paths.output_uri,
                }
            ],
        )
        self.assertEqual(storage.get(paths.input_uri), original_input)
        self.assertEqual(storage.uploads.count(paths.input_uri), input_uploads)
        self.assertTrue(storage.has(paths.batch_metadata_uri))
        self.assertTrue(storage.has(paths.batch_job_metadata_uri))

    def test_saved_job_identity_mismatch_fails_before_submit_or_poll(
        self,
    ) -> None:
        storage = fake_gcs.FakeStorageClient()
        run_gcs_prefix = "gs://bucket/sft/runs/run-a"
        paths = eval_artifacts.eval_target_artifact_paths(
            run_gcs_prefix, "base"
        )
        stale_identity = request_identity.build_gemini_eval_request_identity(
            target_label="base",
            model="gemini-3.1-flash-lite",
            eval_manifest_uri="gs://data/eval.jsonl",
            audio_uris=["gs://audio/a.flac"],
            system_prompt="stale system prompt",
            user_prompt="user",
            prior_context_count=0,
            prior_context_mode="text_turns",
        )
        job_metadata = request_identity.metadata_payload(stale_identity)
        job_metadata["job_name"] = (
            "projects/p/locations/us/batchPredictionJobs/123"
        )
        storage.put(
            paths.batch_job_metadata_uri,
            json.dumps(job_metadata, sort_keys=True) + "\n",
        )
        calls: list[str] = []

        def unexpected_submit(**_: object) -> str:
            calls.append("submit")
            return paths.output_uri

        def unexpected_poll(**_: object) -> str:
            calls.append("poll")
            return paths.output_uri

        with self.assertRaisesRegex(
            ValueError,
            "batch job request identity mismatch",
        ):
            batch.run_batch_audio_inference(
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
                submit_fn=unexpected_submit,
                poll_fn=unexpected_poll,
            )

        self.assertEqual(calls, [])
        self.assertFalse(storage.has(paths.input_uri))

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

    def test_new_batch_rejects_zero_successful_predictions(
        self,
    ) -> None:
        storage = fake_gcs.FakeStorageClient()
        run_gcs_prefix = "gs://bucket/sft/runs/run-a"
        paths = eval_artifacts.eval_target_artifact_paths(
            run_gcs_prefix, "base"
        )

        def submit_with_only_error_rows(**_: object) -> str:
            storage.put(
                f"{paths.output_uri}predictions.jsonl",
                json.dumps({"status": {"code": 13}}) + "\n",
            )
            return paths.output_uri

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
            submit_fn=submit_with_only_error_rows,
        )

        self.assertIsNone(preds)
        self.assertFalse(storage.has(paths.batch_metadata_uri))

    def test_new_batch_rejects_duplicate_uri_within_one_shard(self) -> None:
        storage = fake_gcs.FakeStorageClient()
        run_gcs_prefix = "gs://bucket/sft/runs/run-a"
        paths = eval_artifacts.eval_target_artifact_paths(
            run_gcs_prefix, "base"
        )
        audio_uri = "gs://audio/a.flac"

        def submit_with_duplicate_rows(**_: object) -> str:
            rows = [
                sft_eval_fixtures.vertex_batch_output(audio_uri, "first"),
                sft_eval_fixtures.vertex_batch_output(audio_uri, "second"),
            ]
            storage.put(
                f"{paths.output_uri}predictions.jsonl",
                "\n".join(rows) + "\n",
            )
            return paths.output_uri

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
            submit_fn=submit_with_duplicate_rows,
        )

        self.assertIsNone(preds)
        self.assertFalse(storage.has(paths.batch_metadata_uri))

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
        sft_eval_fixtures.put_batch_reuse_artifacts(
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
