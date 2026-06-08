"""Tests for common.gemini.vertex — no real GCP calls (all genai.Client mocked)."""

import builtins
import importlib.util
import json
import unittest
import unittest.mock
from pathlib import Path

import common.gemini.vertex as vmod
from common.gemini.vertex import (
    _ADAPTER_ENUM,
    GEMINI_GENERATION_CONFIG,
    GEMINI_SAFETY_SETTINGS,
    build_request,
    parse_batch_output,
    poll_tuning_job,
    submit_batch_inference,
    submit_tuning_job,
)


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
    """Tests for common.gemini.vertex.submit_tuning_job."""

    @unittest.mock.patch("common.gemini.vertex.genai")
    def test_defaults_to_gemini_31_flash_lite(self, mock_genai) -> None:
        """submit_tuning_job defaults to the current supervised-tuning model."""
        mock_client = _make_mock_client()
        mock_genai.Client.return_value = mock_client

        submit_tuning_job(
            train_uri="gs://b/train.jsonl",
            display_name="test",
            project="p",
            location="us-central1",
        )

        call_kwargs = mock_client.tunings.tune.call_args.kwargs
        self.assertEqual(call_kwargs["base_model"], "gemini-3.1-flash-lite")

    @unittest.mock.patch("common.gemini.vertex.genai")
    def test_returns_job_name_not_endpoint(self, mock_genai) -> None:
        """submit_tuning_job returns job.name (str), not endpoint."""
        mock_genai.Client.return_value = _make_mock_client()

        result = submit_tuning_job(
            train_uri="gs://b/train.jsonl",
            display_name="test",
            project="p",
            location="us-central1",
        )
        # Must be job.name ("projects/p/.../tuningJobs/123"), not an endpoint
        self.assertIn("tuningJobs", result)
        self.assertNotIn("endpoints", result)

    @unittest.mock.patch("common.gemini.vertex.genai")
    def test_submit_returns_string(self, mock_genai) -> None:
        mock_genai.Client.return_value = _make_mock_client()

        result = submit_tuning_job(
            train_uri="gs://bucket/train.jsonl",
            display_name="test-display",
            project="test-project",
            location="us-central1",
        )
        self.assertIsInstance(result, str)

    @unittest.mock.patch("common.gemini.vertex.genai")
    def test_submit_does_not_poll(self, mock_genai) -> None:
        """submit_tuning_job must NOT call tunings.get (no polling)."""
        mock_genai.Client.return_value = _make_mock_client()

        submit_tuning_job(
            train_uri="gs://b/train.jsonl",
            display_name="test",
            project="p",
            location="us-central1",
        )
        mock_genai.Client.return_value.tunings.get.assert_not_called()

    @unittest.mock.patch("common.gemini.vertex.types")
    @unittest.mock.patch("common.gemini.vertex.genai")
    def test_wires_validation_dataset_when_val_uri_provided(
        self, mock_genai, mock_types
    ) -> None:
        mock_client = _make_mock_client()
        mock_genai.Client.return_value = mock_client
        mock_types.TuningDataset.return_value = "train-dataset"
        mock_types.TuningValidationDataset.return_value = "val-dataset"
        mock_types.CreateTuningJobConfig.side_effect = lambda **kwargs: kwargs

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

    @unittest.mock.patch("common.gemini.vertex.types")
    @unittest.mock.patch("common.gemini.vertex.genai")
    def test_wires_adapter_size_two(self, mock_genai, mock_types) -> None:
        """Adapter size TWO maps to the google-genai enum string."""
        mock_client = _make_mock_client()
        mock_genai.Client.return_value = mock_client
        mock_types.TuningDataset.return_value = "train-dataset"
        mock_types.CreateTuningJobConfig.side_effect = lambda **kwargs: kwargs

        submit_tuning_job(
            train_uri="gs://b/train.jsonl",
            display_name="test",
            project="p",
            location="us-central1",
            adapter_size="TWO",
        )

        call_kwargs = mock_client.tunings.tune.call_args.kwargs
        self.assertEqual(
            call_kwargs["config"]["adapter_size"], "ADAPTER_SIZE_TWO"
        )


class TestPollTuningJob(unittest.TestCase):
    """Tests for common.gemini.vertex.poll_tuning_job."""

    @unittest.mock.patch("common.gemini.vertex.genai")
    def test_poll_returns_endpoint(self, mock_genai) -> None:
        """poll_tuning_job polls and returns endpoint."""
        mock_genai.Client.return_value = _make_mock_client()

        result = poll_tuning_job(
            name="projects/p/locations/l/tuningJobs/123",
            project="p",
            location="us-central1",
        )
        self.assertIn("endpoints", result)

    @unittest.mock.patch("common.gemini.vertex.genai")
    def test_poll_raises_on_failed_state(self, mock_genai) -> None:
        mock_genai.Client.return_value = _make_mock_client(
            state="JOB_STATE_FAILED"
        )

        with self.assertRaises(RuntimeError):
            poll_tuning_job(
                name="projects/p/locations/l/tuningJobs/1",
                project="p",
                location="us-central1",
            )

    @unittest.mock.patch("common.gemini.vertex.genai")
    def test_poll_raises_on_cancelled_state(self, mock_genai) -> None:
        mock_genai.Client.return_value = _make_mock_client(
            state="JOB_STATE_CANCELLED"
        )

        with self.assertRaises(RuntimeError):
            poll_tuning_job(
                name="projects/p/locations/l/tuningJobs/1",
                project="p",
                location="us-central1",
            )

    @unittest.mock.patch("common.gemini.vertex.genai")
    def test_poll_calls_tunings_get(self, mock_genai) -> None:
        """poll_tuning_job must call tunings.get to re-fetch by name."""
        mock_client = _make_mock_client()
        mock_genai.Client.return_value = mock_client

        poll_tuning_job(
            name="projects/p/locations/l/tuningJobs/123",
            project="p",
            location="us-central1",
        )
        mock_client.tunings.get.assert_called_once_with(
            name="projects/p/locations/l/tuningJobs/123"
        )

    @unittest.mock.patch("common.gemini.vertex.genai")
    def test_poll_raises_timeout_when_never_terminal(self, mock_genai) -> None:
        """poll_tuning_job raises TimeoutError if no terminal state within timeout_hours."""
        mock_genai.Client.return_value = _make_mock_client(
            state="JOB_STATE_RUNNING"
        )

        with self.assertRaises(TimeoutError):
            poll_tuning_job(
                name="projects/p/locations/l/tuningJobs/123",
                project="p",
                location="us-central1",
                poll_interval=0,
                timeout_hours=0,
            )

    @unittest.mock.patch("common.gemini.vertex.time.sleep")
    @unittest.mock.patch("common.gemini.vertex.genai")
    def test_poll_retries_transient_get_error(
        self, mock_genai, mock_sleep
    ) -> None:
        """poll_tuning_job retries a transient tunings.get failure."""
        mock_client = _make_mock_client()
        success = mock_client.tunings.get.return_value
        mock_client.tunings.get.side_effect = [
            RuntimeError("temporary network failure"),
            success,
        ]
        mock_genai.Client.return_value = mock_client

        result = poll_tuning_job(
            name="projects/p/locations/l/tuningJobs/123",
            project="p",
            location="us-central1",
        )

        self.assertIn("endpoints", result)
        self.assertEqual(mock_client.tunings.get.call_count, 2)
        mock_sleep.assert_called_once()

    @unittest.mock.patch("common.gemini.vertex.genai")
    def test_poll_raises_clear_error_when_endpoint_missing(
        self, mock_genai
    ) -> None:
        """Succeeded tuning jobs should expose a tuned_model endpoint."""
        mock_client = _make_mock_client()
        mock_client.tunings.get.return_value.tuned_model = None
        mock_genai.Client.return_value = mock_client

        with self.assertRaisesRegex(
            RuntimeError, "succeeded but returned no tuned_model.endpoint"
        ):
            poll_tuning_job(
                name="projects/p/locations/l/tuningJobs/123",
                project="p",
                location="us-central1",
            )


class TestAdapterEnum(unittest.TestCase):
    """Guard against silent miskeys in _ADAPTER_ENUM."""

    def test_enum_contains_required_sizes(self) -> None:
        for key in ("ONE", "TWO", "FOUR", "EIGHT", "SIXTEEN"):
            self.assertIn(key, _ADAPTER_ENUM, f"Missing adapter size: {key}")

    def test_enum_maps_two(self) -> None:
        self.assertEqual(_ADAPTER_ENUM["TWO"], "ADAPTER_SIZE_TWO")

    def test_enum_values_have_adapter_size_prefix(self) -> None:
        for val in _ADAPTER_ENUM.values():
            self.assertTrue(val.startswith("ADAPTER_SIZE_"), val)


class TestImportGuard(unittest.TestCase):
    """Test that ImportError is raised when [vertex] extra is absent."""

    def test_patch_targets_exist_when_vertex_extra_missing(self) -> None:
        vertex_path = (
            Path(__file__).resolve().parents[3]
            / "src"
            / "common"
            / "gemini"
            / "vertex.py"
        )
        spec = importlib.util.spec_from_file_location(
            "common_vertex_without_genai_for_test", vertex_path
        )
        self.assertIsNotNone(spec)
        self.assertIsNotNone(spec.loader)
        module = importlib.util.module_from_spec(spec)
        real_import = builtins.__import__

        def _raise_for_google_genai(
            name, globals_=None, locals_=None, fromlist=(), level=0
        ):
            if (name == "google" and "genai" in fromlist) or (
                name == "google.genai"
            ):
                msg = "No module named google.genai"
                raise ImportError(msg)
            return real_import(name, globals_, locals_, fromlist, level)

        with unittest.mock.patch(
            "builtins.__import__", new=_raise_for_google_genai
        ):
            spec.loader.exec_module(module)

        self.assertIsNotNone(module._VERTEX_MISSING)
        self.assertIsNone(module.genai)
        self.assertIsNone(module.types)

    def test_require_vertex_raises_when_missing(self) -> None:
        orig = vmod._VERTEX_MISSING
        try:
            vmod._VERTEX_MISSING = ImportError("test")
            with self.assertRaises(ImportError):
                vmod._require_vertex()
        finally:
            vmod._VERTEX_MISSING = orig


class TestBuildRequest(unittest.TestCase):
    """Tests for common.gemini.vertex.build_request — no GCP calls, pure dict construction."""

    def setUp(self) -> None:
        self.build_request = build_request
        self.default_gen_config = GEMINI_GENERATION_CONFIG
        self.default_safety = GEMINI_SAFETY_SETTINGS

    def test_return_shape(self) -> None:
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

    def test_default_generation_config(self) -> None:
        """Default generation_config has temperature 0.0 and max_output_tokens 512."""
        result = self.build_request(
            "gs://bucket/audio.flac",
            system_prompt="S",
            user_prompt="U",
        )
        gen_cfg = result["request"]["generation_config"]
        self.assertEqual(gen_cfg["temperature"], 0.0)
        self.assertEqual(gen_cfg["max_output_tokens"], 512)

    def test_generation_config_is_copied(self) -> None:
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

    def test_default_safety_settings_four_block_none(self) -> None:
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

    def test_safety_settings_is_copied(self) -> None:
        """safety_settings is shallow-copied — mutating the result leaves the default intact."""
        result = self.build_request(
            "gs://bucket/audio.flac",
            system_prompt="S",
            user_prompt="U",
        )
        result["request"]["safety_settings"].pop()
        self.assertEqual(len(self.default_safety), 4)

    def test_system_prompt_preserved_exactly(self) -> None:
        """system_prompt is embedded without normalization."""
        result = self.build_request(
            "gs://bucket/audio.flac",
            system_prompt="  Leading space.  ",
            user_prompt="U",
        )
        parts = result["request"]["system_instruction"]["parts"]
        self.assertEqual(parts[0]["text"], "  Leading space.  ")

    def test_custom_generation_config_override(self) -> None:
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


class TestParseBatchOutput(unittest.TestCase):
    def test_parses_camel_case_request_echo(self) -> None:
        output = {
            "request": {
                "contents": [
                    {
                        "parts": [
                            {
                                "fileData": {
                                    "fileUri": "gs://bucket/a.flac",
                                }
                            }
                        ]
                    }
                ]
            },
            "response": {
                "candidates": [{"content": {"parts": [{"text": "engine 41"}]}}]
            },
        }

        self.assertEqual(
            parse_batch_output(__import__("json").dumps(output)),
            {"gs://bucket/a.flac": "engine 41"},
        )

    def test_parses_snake_case_request_echo(self) -> None:
        output = {
            "request": {
                "contents": [
                    {
                        "parts": [
                            {
                                "file_data": {
                                    "file_uri": "gs://bucket/a.flac",
                                }
                            }
                        ]
                    }
                ]
            },
            "response": {
                "candidates": [{"content": {"parts": [{"text": "engine 41"}]}}]
            },
        }

        self.assertEqual(
            parse_batch_output(json.dumps(output)),
            {"gs://bucket/a.flac": "engine 41"},
        )

    def test_skips_status_and_malformed_rows(self) -> None:
        lines = [
            "{bad json",
            json.dumps({"status": {"code": 13}, "request": {}}),
            json.dumps({"request": {}, "response": {}}),
        ]

        self.assertEqual(parse_batch_output("\n".join(lines)), {})


class TestSubmitBatchInferenceOutputUri(unittest.TestCase):
    """Batch inference returns the destination GCS URI."""

    @unittest.mock.patch("common.gemini.vertex.genai")
    def test_returns_dest_gcs_uri(self, mock_genai) -> None:
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

        result = submit_batch_inference(
            input_uri="gs://bucket/input.jsonl",
            output_uri="gs://bucket/output/",
            model="gemini-2.5-flash",
            project="p",
            location="us-central1",
        )
        # Must be the string gcs_uri, not the mock object
        self.assertEqual(result, "gs://bucket/output/")

    @unittest.mock.patch("common.gemini.vertex.genai")
    def test_falls_back_to_output_uri_when_dest_is_none(
        self, mock_genai
    ) -> None:
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

        with self.assertLogs("common.gemini.vertex", level="WARNING") as logs:
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

    @unittest.mock.patch("common.gemini.vertex.time.sleep")
    @unittest.mock.patch("common.gemini.vertex.genai")
    def test_batch_poll_retries_transient_get_error(
        self, mock_genai, mock_sleep
    ) -> None:
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

    @unittest.mock.patch("common.gemini.vertex.genai")
    def test_batch_uses_full_model_resource_location(self, mock_genai) -> None:
        """Full tuned endpoint resources choose their own Vertex location."""
        mock_dest = unittest.mock.MagicMock()
        mock_dest.gcs_uri = "gs://bucket/output/"
        mock_batch_job = unittest.mock.MagicMock()
        mock_batch_job.name = "projects/p/locations/us/batchPredictionJobs/1"
        mock_cur = unittest.mock.MagicMock()
        mock_cur.state.name = "JOB_STATE_SUCCEEDED"
        mock_cur.dest = mock_dest
        mock_client = unittest.mock.MagicMock()
        mock_client.batches.create.return_value = mock_batch_job
        mock_client.batches.get.return_value = mock_cur
        mock_genai.Client.return_value = mock_client

        submit_batch_inference(
            input_uri="gs://bucket/input.jsonl",
            output_uri="gs://bucket/output/",
            model="projects/p/locations/us/endpoints/123",
            project="p",
            location="us-central1",
        )

        mock_genai.Client.assert_called_once_with(
            vertexai=True, project="p", location="us"
        )


if __name__ == "__main__":
    unittest.main()
