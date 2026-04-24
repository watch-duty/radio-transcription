from unittest import TestCase, mock

from backend.pipeline.common.logging import setup_logging


class TestLogging(TestCase):
    def setUp(self) -> None:
        setup_logging.cache_clear()

    @mock.patch("backend.pipeline.common.logging.cloud_logging")
    def test_setup_logging_gcp(self, mock_cloud_logging) -> None:
        # Mock cloud_logging to look like the real library
        mock_client_inst = mock.Mock()
        mock_cloud_logging.Client.return_value = mock_client_inst

        # Explicitly mock is_gcp_env to return True to trigger the GCP logging path
        with mock.patch(
            "backend.pipeline.common.logging.is_gcp_env", return_value=True
        ):
            # First call should initialize cloud logging
            setup_logging()
            mock_cloud_logging.Client.assert_called_once()
            mock_client_inst.setup_logging.assert_called_once()

            # Second call should do nothing (idempotency)
            setup_logging()
            mock_cloud_logging.Client.assert_called_once()
            mock_client_inst.setup_logging.assert_called_once()
