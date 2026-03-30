import logging
import os
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

        with mock.patch.dict(os.environ, {"K_SERVICE": "1"}):
            # First call should initialize cloud logging
            setup_logging()
            mock_cloud_logging.Client.assert_called_once()
            mock_client_inst.setup_logging.assert_called_once()

            # Second call should do nothing (idempotency)
            setup_logging()
            mock_cloud_logging.Client.assert_called_once()
            mock_client_inst.setup_logging.assert_called_once()

    @mock.patch("logging.basicConfig")
    def test_setup_logging_local(self, mock_basic_config) -> None:
        # Ensure serverless env vars are not set
        with mock.patch.dict(os.environ, {}, clear=True):
            # First call should initialize local logging
            setup_logging()
            mock_basic_config.assert_called_once()
            _args, kwargs = mock_basic_config.call_args
            self.assertEqual(kwargs["level"], logging.INFO)
            self.assertTrue(kwargs["force"])

            # Second call should do nothing (idempotency)
            setup_logging()
            mock_basic_config.assert_called_once()
