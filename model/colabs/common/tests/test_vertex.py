"""Tests for common.vertex — no real GCP calls (all genai.Client mocked).

PR3 splits submit_tuning_job (submit + return job.name) and poll_tuning_job
(re-fetch by name, poll to terminal, return endpoint). The batch output URI
bug (cur.dest as object not str) is also fixed in PR3.
"""

import builtins
import importlib.util
from pathlib import Path
import unittest
import unittest.mock


def _make_mock_client(
    state: str = "JOB_STATE_SUCCEEDED",
    endpoint: str = "projects/p/locations/l/endpoints/e",
    job_name: str = "projects/p/locations/l/tuningJobs/123",
):
    mock_job = unittest.mock.MagicMock()
    mock_job.name = job_name
    mock_tuned = unittest.mock.MagicMock()
    mock_tuned.endpoint = endpoint
    mock_job.tuned_model = mock_tuned
    mock_cur = unittest.mock.MagicMock()
    mock_cur.state.name = state
    mock_cur.tuned_model.endpoint = endpoint
    mock_client = unittest.mock.MagicMock()
    mock_client.tunings.tune.return_value = mock_job
    mock_client.tunings.get.return_value = mock_cur
    return mock_client


class TestSubmitTuningJob(unittest.TestCase):
    """Tests for common.vertex.submit_tuning_job (PR3: submit-only, returns job.name)."""

    @unittest.mock.patch("common.vertex.genai")
    def test_defaults_to_gemini_31_flash_lite(self, mock_genai):
        """submit_tuning_job defaults to the current supervised-tuning model."""
        mock_client = _make_mock_client()
        mock_genai.Client.return_value = mock_client
        from common.vertex import submit_tuning_job

        submit_tuning_job(
            train_uri="gs://b/train.jsonl",
            display_name="test",
            project="p",
            location="us-central1",
        )

        call_kwargs = mock_client.tunings.tune.call_args.kwargs
        self.assertEqual(call_kwargs["base_model"], "gemini-3.1-flash-lite")

    @unittest.mock.patch("common.vertex.genai")
    def test_returns_job_name_not_endpoint(self, mock_genai):
        """PR3: submit_tuning_job now returns job.name (str), not endpoint."""
        mock_genai.Client.return_value = _make_mock_client()
        from common.vertex import submit_tuning_job

        result = submit_tuning_job(
            train_uri="gs://b/train.jsonl",
            display_name="test",
            project="p",
            location="us-central1",
        )
        # Must be job.name ("projects/p/.../tuningJobs/123"), not an endpoint
        self.assertIn("tuningJobs", result)
        self.assertNotIn("endpoints", result)

    @unittest.mock.patch("common.vertex.genai")
    def test_submit_returns_string(self, mock_genai):
        mock_genai.Client.return_value = _make_mock_client()
        from common.vertex import submit_tuning_job

        result = submit_tuning_job(
            train_uri="gs://bucket/train.jsonl",
            display_name="test-display",
            project="test-project",
            location="us-central1",
        )
        self.assertIsInstance(result, str)

    @unittest.mock.patch("common.vertex.genai")
    def test_submit_does_not_poll(self, mock_genai):
        """PR3: submit_tuning_job must NOT call tunings.get (no polling)."""
        mock_genai.Client.return_value = _make_mock_client()
        from common.vertex import submit_tuning_job

        submit_tuning_job(
            train_uri="gs://b/train.jsonl",
            display_name="test",
            project="p",
            location="us-central1",
        )
        mock_genai.Client.return_value.tunings.get.assert_not_called()

    @unittest.mock.patch("common.vertex.types")
    @unittest.mock.patch("common.vertex.genai")
    def test_wires_validation_dataset_when_val_uri_provided(
        self, mock_genai, mock_types
    ):
        mock_client = _make_mock_client()
        mock_genai.Client.return_value = mock_client
        mock_types.TuningDataset.return_value = "train-dataset"
        mock_types.TuningValidationDataset.return_value = "val-dataset"
        mock_types.CreateTuningJobConfig.side_effect = lambda **kwargs: kwargs
        from common.vertex import submit_tuning_job

        submit_tuning_job(
            train_uri="gs://b/train.jsonl",
            display_name="test",
            project="p",
            location="us-central1",
            val_uri="gs://b/val.jsonl",
        )
        call_kwargs = mock_client.tunings.tune.call_args.kwargs
        self.assertEqual(call_kwargs["training_dataset"], "train-dataset")
        self.assertEqual(
            call_kwargs["config"]["validation_dataset"], "val-dataset"
        )
        mock_types.TuningDataset.assert_called_once_with(
            gcs_uri="gs://b/train.jsonl"
        )
        mock_types.TuningValidationDataset.assert_called_once_with(
            gcs_uri="gs://b/val.jsonl"
        )


class TestPollTuningJob(unittest.TestCase):
    """Tests for common.vertex.poll_tuning_job (new in PR3)."""

    @unittest.mock.patch("common.vertex.genai")
    def test_poll_returns_endpoint(self, mock_genai):
        """poll_tuning_job polls and returns endpoint."""
        mock_genai.Client.return_value = _make_mock_client()
        from common.vertex import poll_tuning_job

        result = poll_tuning_job(
            name="projects/p/locations/l/tuningJobs/123",
            project="p",
            location="us-central1",
        )
        self.assertIn("endpoints", result)

    @unittest.mock.patch("common.vertex.genai")
    def test_poll_raises_on_failed_state(self, mock_genai):
        mock_genai.Client.return_value = _make_mock_client(
            state="JOB_STATE_FAILED"
        )
        from common.vertex import poll_tuning_job

        with self.assertRaises(RuntimeError):
            poll_tuning_job(
                name="projects/p/locations/l/tuningJobs/1",
                project="p",
                location="us-central1",
            )

    @unittest.mock.patch("common.vertex.genai")
    def test_poll_raises_on_cancelled_state(self, mock_genai):
        mock_genai.Client.return_value = _make_mock_client(
            state="JOB_STATE_CANCELLED"
        )
        from common.vertex import poll_tuning_job

        with self.assertRaises(RuntimeError):
            poll_tuning_job(
                name="projects/p/locations/l/tuningJobs/1",
                project="p",
                location="us-central1",
            )

    @unittest.mock.patch("common.vertex.genai")
    def test_poll_calls_tunings_get(self, mock_genai):
        """poll_tuning_job must call tunings.get to re-fetch by name."""
        mock_client = _make_mock_client()
        mock_genai.Client.return_value = mock_client
        from common.vertex import poll_tuning_job

        poll_tuning_job(
            name="projects/p/locations/l/tuningJobs/123",
            project="p",
            location="us-central1",
        )
        mock_client.tunings.get.assert_called_once_with(
            name="projects/p/locations/l/tuningJobs/123"
        )

    @unittest.mock.patch("common.vertex.genai")
    def test_poll_raises_timeout_when_never_terminal(self, mock_genai):
        """poll_tuning_job raises TimeoutError if no terminal state within timeout_hours."""
        mock_genai.Client.return_value = _make_mock_client(
            state="JOB_STATE_RUNNING"
        )
        from common.vertex import poll_tuning_job

        with self.assertRaises(TimeoutError):
            poll_tuning_job(
                name="projects/p/locations/l/tuningJobs/123",
                project="p",
                location="us-central1",
                poll_interval=0,
                timeout_hours=0,
            )

    @unittest.mock.patch("common.vertex.time.sleep")
    @unittest.mock.patch("common.vertex.genai")
    def test_poll_retries_transient_get_error(self, mock_genai, mock_sleep):
        """poll_tuning_job retries a transient tunings.get failure."""
        mock_client = _make_mock_client()
        success = mock_client.tunings.get.return_value
        mock_client.tunings.get.side_effect = [
            RuntimeError("temporary network failure"),
            success,
        ]
        mock_genai.Client.return_value = mock_client
        from common.vertex import poll_tuning_job

        result = poll_tuning_job(
            name="projects/p/locations/l/tuningJobs/123",
            project="p",
            location="us-central1",
        )

        self.assertIn("endpoints", result)
        self.assertEqual(mock_client.tunings.get.call_count, 2)
        mock_sleep.assert_called_once()


class TestAdapterEnum(unittest.TestCase):
    """Guard against silent miskeys in _ADAPTER_ENUM."""

    def test_enum_contains_required_sizes(self):
        from common.vertex import _ADAPTER_ENUM

        for key in ("ONE", "FOUR", "EIGHT", "SIXTEEN"):
            self.assertIn(key, _ADAPTER_ENUM, f"Missing adapter size: {key}")

    def test_enum_values_have_adapter_size_prefix(self):
        from common.vertex import _ADAPTER_ENUM

        for val in _ADAPTER_ENUM.values():
            self.assertTrue(val.startswith("ADAPTER_SIZE_"), val)


class TestImportGuard(unittest.TestCase):
    """Test that ImportError is raised when [vertex] extra is absent."""

    def test_patch_targets_exist_when_vertex_extra_missing(self):
        vertex_path = Path(__file__).resolve().parents[1] / "vertex.py"
        spec = importlib.util.spec_from_file_location(
            "common_vertex_without_genai_for_test", vertex_path
        )
        self.assertIsNotNone(spec)
        self.assertIsNotNone(spec.loader)
        module = importlib.util.module_from_spec(spec)
        real_import = builtins.__import__

        def _raise_for_google_genai(
            name, globals=None, locals=None, fromlist=(), level=0
        ):
            if (name == "google" and "genai" in fromlist) or (
                name == "google.genai"
            ):
                raise ImportError("No module named google.genai")
            return real_import(name, globals, locals, fromlist, level)

        with unittest.mock.patch(
            "builtins.__import__", new=_raise_for_google_genai
        ):
            spec.loader.exec_module(module)

        self.assertIsNotNone(module._VERTEX_MISSING)
        self.assertIsNone(module.genai)
        self.assertIsNone(module.types)

    def test_require_vertex_raises_when_missing(self):
        import common.vertex as vmod

        orig = vmod._VERTEX_MISSING
        try:
            vmod._VERTEX_MISSING = ImportError("test")
            with self.assertRaises(ImportError):
                vmod._require_vertex()
        finally:
            vmod._VERTEX_MISSING = orig


class TestBuildRequest(unittest.TestCase):
    """Tests for common.vertex.build_request — no GCP calls, pure dict construction."""

    def setUp(self):
        from common.vertex import (
            GEMINI_GENERATION_CONFIG,
            GEMINI_SAFETY_SETTINGS,
            build_request,
        )

        self.build_request = build_request
        self.default_gen_config = GEMINI_GENERATION_CONFIG
        self.default_safety = GEMINI_SAFETY_SETTINGS

    def test_return_shape(self):
        """build_request returns the canonical nested dict shape."""
        result = self.build_request(
            "gs://bucket/audio.flac",
            system_prompt="System.",
            user_prompt="Transcribe.",
        )
        req = result["request"]
        self.assertIn("contents", req)
        self.assertIn("system_instruction", req)
        self.assertIn("generation_config", req)
        self.assertIn("safety_settings", req)
        part = req["contents"][0]["parts"][0]
        self.assertEqual(
            part["file_data"]["file_uri"], "gs://bucket/audio.flac"
        )
        self.assertEqual(part["file_data"]["mime_type"], "audio/flac")

    def test_default_generation_config(self):
        """Default generation_config has temperature 0.0 and max_output_tokens 512."""
        result = self.build_request(
            "gs://bucket/audio.flac",
            system_prompt="S",
            user_prompt="U",
        )
        gen_cfg = result["request"]["generation_config"]
        self.assertEqual(gen_cfg["temperature"], 0.0)
        self.assertEqual(gen_cfg["max_output_tokens"], 512)

    def test_generation_config_is_copied(self):
        """generation_config.copy() is used — mutating the result leaves the default intact."""
        result = self.build_request(
            "gs://bucket/audio.flac",
            system_prompt="S",
            user_prompt="U",
        )
        result["request"]["generation_config"]["extra_key"] = (
            "should_not_propagate"
        )
        self.assertNotIn("extra_key", self.default_gen_config)

    def test_default_safety_settings_four_block_none(self):
        """Default safety_settings has 4 BLOCK_NONE entries."""
        result = self.build_request(
            "gs://bucket/audio.flac",
            system_prompt="S",
            user_prompt="U",
        )
        safety = result["request"]["safety_settings"]
        self.assertEqual(len(safety), 4)
        for entry in safety:
            self.assertEqual(entry["threshold"], "BLOCK_NONE")

    def test_safety_settings_is_copied(self):
        """safety_settings is shallow-copied — mutating the result leaves the default intact."""
        result = self.build_request(
            "gs://bucket/audio.flac",
            system_prompt="S",
            user_prompt="U",
        )
        result["request"]["safety_settings"].pop()
        self.assertEqual(len(self.default_safety), 4)

    def test_system_prompt_stripped(self):
        """system_prompt is stripped before embedding."""
        result = self.build_request(
            "gs://bucket/audio.flac",
            system_prompt="  Leading space.  ",
            user_prompt="U",
        )
        parts = result["request"]["system_instruction"]["parts"]
        self.assertEqual(parts[0]["text"], "Leading space.")

    def test_custom_generation_config_override(self):
        """Caller can pass a custom generation_config."""
        custom_cfg = {"temperature": 0.5, "max_output_tokens": 256}
        result = self.build_request(
            "gs://bucket/audio.flac",
            system_prompt="S",
            user_prompt="U",
            generation_config=custom_cfg,
        )
        self.assertEqual(
            result["request"]["generation_config"]["temperature"], 0.5
        )


class TestSubmitBatchInferenceOutputUri(unittest.TestCase):
    """PR3 bug fix: cur.dest.gcs_uri replaces getattr(cur, 'dest', ...)."""

    @unittest.mock.patch("common.vertex.genai")
    def test_returns_dest_gcs_uri(self, mock_genai):
        """submit_batch_inference returns cur.dest.gcs_uri, not BatchJobDestination object."""
        mock_dest = unittest.mock.MagicMock()
        mock_dest.gcs_uri = "gs://bucket/output/"
        mock_batch_job = unittest.mock.MagicMock()
        mock_batch_job.name = "projects/p/locations/l/batchPredictionJobs/1"
        mock_cur = unittest.mock.MagicMock()
        mock_cur.state.name = "JOB_STATE_SUCCEEDED"
        mock_cur.dest = mock_dest
        mock_client = unittest.mock.MagicMock()
        mock_client.batches.create.return_value = mock_batch_job
        mock_client.batches.get.return_value = mock_cur
        mock_genai.Client.return_value = mock_client

        from common.vertex import submit_batch_inference

        result = submit_batch_inference(
            input_uri="gs://bucket/input.jsonl",
            output_uri="gs://bucket/output/",
            model="gemini-2.5-flash",
            project="p",
            location="us-central1",
        )
        # Must be the string gcs_uri, not the mock object
        self.assertEqual(result, "gs://bucket/output/")

    @unittest.mock.patch("common.vertex.genai")
    def test_falls_back_to_output_uri_when_dest_is_none(self, mock_genai):
        """submit_batch_inference uses output_uri fallback when cur.dest is None."""
        mock_batch_job = unittest.mock.MagicMock()
        mock_batch_job.name = "projects/p/locations/l/batchPredictionJobs/1"
        mock_cur = unittest.mock.MagicMock()
        mock_cur.state.name = "JOB_STATE_SUCCEEDED"
        mock_cur.dest = None
        mock_client = unittest.mock.MagicMock()
        mock_client.batches.create.return_value = mock_batch_job
        mock_client.batches.get.return_value = mock_cur
        mock_genai.Client.return_value = mock_client

        from common.vertex import submit_batch_inference

        with self.assertLogs("common.vertex", level="WARNING") as logs:
            result = submit_batch_inference(
                input_uri="gs://bucket/input.jsonl",
                output_uri="gs://bucket/fallback/",
                model="gemini-2.5-flash",
                project="p",
                location="us-central1",
            )

        self.assertEqual(result, "gs://bucket/fallback/")
        self.assertIn(
            "Batch job returned no destination", "\n".join(logs.output)
        )

    @unittest.mock.patch("common.vertex.time.sleep")
    @unittest.mock.patch("common.vertex.genai")
    def test_batch_poll_retries_transient_get_error(
        self, mock_genai, mock_sleep
    ):
        """submit_batch_inference retries a transient batches.get failure."""
        mock_dest = unittest.mock.MagicMock()
        mock_dest.gcs_uri = "gs://bucket/output/"
        mock_batch_job = unittest.mock.MagicMock()
        mock_batch_job.name = "projects/p/locations/l/batchPredictionJobs/1"
        mock_cur = unittest.mock.MagicMock()
        mock_cur.state.name = "JOB_STATE_SUCCEEDED"
        mock_cur.dest = mock_dest
        mock_client = unittest.mock.MagicMock()
        mock_client.batches.create.return_value = mock_batch_job
        mock_client.batches.get.side_effect = [
            RuntimeError("temporary network failure"),
            mock_cur,
        ]
        mock_genai.Client.return_value = mock_client

        from common.vertex import submit_batch_inference

        result = submit_batch_inference(
            input_uri="gs://bucket/input.jsonl",
            output_uri="gs://bucket/output/",
            model="gemini-2.5-flash",
            project="p",
            location="us-central1",
        )

        self.assertEqual(result, "gs://bucket/output/")
        self.assertEqual(mock_client.batches.get.call_count, 2)
        mock_sleep.assert_called_once()


if __name__ == "__main__":
    unittest.main()
