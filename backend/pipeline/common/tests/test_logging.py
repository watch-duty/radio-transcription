import logging
import os
from unittest import TestCase, mock

from backend.pipeline.common.logging import setup_logging


class TestLogging(TestCase):
    @mock.patch("google.cloud.logging.Client")
    def test_setup_logging_cloud(self, mock_client) -> None:
        # Test cloud logging setup
        # Ensure LOCAL_DEV is not set
        with mock.patch.dict(os.environ, {}, clear=False):
            if "LOCAL_DEV" in os.environ:
                del os.environ["LOCAL_DEV"]
            setup_logging()

        mock_client.assert_called_once()
        mock_client().setup_logging.assert_called_once()

    @mock.patch("logging.basicConfig")
    def test_setup_logging_local(self, mock_basic_config) -> None:
        # Test local logging setup
        # Set LOCAL_DEV to 1
        with mock.patch.dict(os.environ, {"LOCAL_DEV": "1"}):
            setup_logging()

        mock_basic_config.assert_called_once()
        _args, kwargs = mock_basic_config.call_args
        self.assertEqual(kwargs["level"], logging.INFO)
        self.assertTrue(kwargs["force"])
