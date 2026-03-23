import logging
import os
from unittest import TestCase, mock

from backend.pipeline.common.logging import _STATE, setup_logging


class TestLogging(TestCase):
    def setUp(self) -> None:
        # Reset the global flag before each test to ensure they are isolated
        _STATE["initialized"] = False

    @mock.patch("backend.pipeline.common.logging._cloud_logging")
    @mock.patch("logging.basicConfig")
    def test_setup_logging_cloud_fallback(
        self, mock_basic_config, mock_cloud_logging
    ) -> None:
        # Mock _cloud_logging to be None (not installed)
        with mock.patch("backend.pipeline.common.logging._cloud_logging", None):
            with mock.patch.dict(os.environ, {}, clear=True):
                # First call should fall back to basicConfig
                setup_logging()
                mock_basic_config.assert_called_once()
                self.assertEqual(
                    mock_basic_config.call_args[1]["level"], logging.INFO
                )

                # Second call should do nothing (idempotency)
                setup_logging()
                mock_basic_config.assert_called_once()

    @mock.patch("backend.pipeline.common.logging._cloud_logging")
    def test_setup_logging_cloud_installed(self, mock_cloud_logging) -> None:
        # Mock _cloud_logging to look like the real library
        mock_client_inst = mock.Mock()
        mock_cloud_logging.Client.return_value = mock_client_inst

        # Ensure it's not None
        with mock.patch(
            "backend.pipeline.common.logging._cloud_logging", mock_cloud_logging
        ):
            with mock.patch.dict(os.environ, {}, clear=True):
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
        # Set LOCAL_DEV to 1
        with mock.patch.dict(os.environ, {"LOCAL_DEV": "1"}):
            # First call should initialize local logging
            setup_logging()
            mock_basic_config.assert_called_once()
            _args, kwargs = mock_basic_config.call_args
            self.assertEqual(kwargs["level"], logging.INFO)
            self.assertTrue(kwargs["force"])

            # Second call should do nothing (idempotency)
            setup_logging()
            mock_basic_config.assert_called_once()
