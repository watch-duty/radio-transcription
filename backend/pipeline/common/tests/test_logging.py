import logging
from unittest import TestCase, mock

from backend.pipeline.common.logging import setup_logging


class TestLogging(TestCase):
    def setUp(self) -> None:
        setup_logging.cache_clear()

    @mock.patch("backend.pipeline.common.logging.trace.set_tracer_provider")
    @mock.patch("backend.pipeline.common.logging.CloudTraceSpanExporter")
    @mock.patch("backend.pipeline.common.logging.cloud_logging")
    def test_setup_logging_gcp(
        self, mock_cloud_logging, mock_exporter, mock_set_provider
    ) -> None:
        # Mock cloud_logging to look like the real library
        mock_client_inst = mock.Mock()
        mock_cloud_logging.Client.return_value = mock_client_inst

        # Explicitly mock is_gcp_env to return True to trigger the GCP logging path
        with mock.patch(
            "backend.pipeline.common.logging.is_gcp_env", return_value=True
        ):
            with mock.patch.dict(
                "os.environ", {"GOOGLE_CLOUD_PROJECT": "test-project"}
            ):
                # First call should initialize cloud logging and tracing
                setup_logging()
                mock_cloud_logging.Client.assert_called_once()
                mock_client_inst.setup_logging.assert_called_once()
                mock_exporter.assert_called_once_with(project_id="test-project")
                mock_set_provider.assert_called_once()

                # Second call should do nothing (idempotency due to cache)
                setup_logging()
                mock_cloud_logging.Client.assert_called_once()
                mock_client_inst.setup_logging.assert_called_once()
                mock_exporter.assert_called_once()
                mock_set_provider.assert_called_once()

    @mock.patch("logging.basicConfig")
    def test_setup_logging_local(self, mock_basic_config) -> None:
        # Mock is_gcp_env to return False to trigger local logging path
        with mock.patch(
            "backend.pipeline.common.logging.is_gcp_env", return_value=False
        ):
            # First call should initialize local logging
            setup_logging()
            mock_basic_config.assert_called_once()
            _args, kwargs = mock_basic_config.call_args
            self.assertEqual(kwargs["level"], logging.INFO)
            self.assertTrue(kwargs["force"])

            # Second call should do nothing (idempotency)
            setup_logging()
            mock_basic_config.assert_called_once()
