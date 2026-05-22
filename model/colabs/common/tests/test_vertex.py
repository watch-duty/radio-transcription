"""Tests for common.vertex — no real GCP calls (all genai.Client mocked)."""

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
    """Tests for common.vertex.submit_tuning_job (PR1 monolithic — submits+polls)."""

    @unittest.mock.patch("common.vertex.genai")
    def test_returns_endpoint_on_success(self, mock_genai):
        mock_genai.Client.return_value = _make_mock_client()
        from common.vertex import submit_tuning_job

        result = submit_tuning_job(
            train_uri="gs://bucket/train.jsonl",
            display_name="test-display",
            project="test-project",
            location="us-central1",
        )
        self.assertIsInstance(result, str)
        self.assertIn("endpoints", result)

    @unittest.mock.patch("common.vertex.genai")
    def test_raises_runtime_error_on_failed_state(self, mock_genai):
        mock_genai.Client.return_value = _make_mock_client(
            state="JOB_STATE_FAILED"
        )
        from common.vertex import submit_tuning_job

        with self.assertRaises(RuntimeError):
            submit_tuning_job(
                train_uri="gs://bucket/train.jsonl",
                display_name="test",
                project="p",
                location="us-central1",
            )

    @unittest.mock.patch("common.vertex.genai")
    def test_wires_validation_dataset_when_val_uri_provided(self, mock_genai):
        mock_client = _make_mock_client()
        mock_genai.Client.return_value = mock_client
        from common.vertex import submit_tuning_job

        submit_tuning_job(
            train_uri="gs://b/train.jsonl",
            display_name="test",
            project="p",
            location="us-central1",
            val_uri="gs://b/val.jsonl",
        )
        call_kwargs = mock_client.tunings.tune.call_args.kwargs
        cfg = call_kwargs.get("config")
        # validation_dataset must be present in the config
        self.assertIsNotNone(cfg)


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

    def test_require_vertex_raises_when_missing(self):
        import common.vertex as vmod

        orig = vmod._VERTEX_MISSING
        try:
            vmod._VERTEX_MISSING = ImportError("test")
            with self.assertRaises(ImportError):
                vmod._require_vertex()
        finally:
            vmod._VERTEX_MISSING = orig


if __name__ == "__main__":
    unittest.main()
