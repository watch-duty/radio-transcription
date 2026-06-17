import importlib
import unittest
from unittest import mock

from backend.pipeline.ingestion.oldest_feed_publisher import (
    main as oldest_feed_publisher_main,
)


class TestTracingInitialization(unittest.TestCase):
    def test_tracing_not_initialized_on_import(self) -> None:
        """Verifies that setup_logging is not called during module import."""
        with mock.patch(
            "backend.pipeline.common.log_helper.setup_logging"
        ) as mock_setup_logging:
            importlib.reload(oldest_feed_publisher_main)
            mock_setup_logging.assert_not_called()

    @mock.patch(
        "backend.pipeline.ingestion.oldest_feed_publisher.main.setup_logging"
    )
    def test_tracing_initialized_on_handler(
        self,
        mock_setup_logging: mock.MagicMock,
    ) -> None:
        """Verifies setup_logging is called when the HTTP handler runs."""
        mock_request = mock.MagicMock()
        with mock.patch(
            "backend.pipeline.ingestion.oldest_feed_publisher.main._loop.run_until_complete"
        ):
            oldest_feed_publisher_main.oldest_feed_publisher(mock_request)
            mock_setup_logging.assert_called_once()
