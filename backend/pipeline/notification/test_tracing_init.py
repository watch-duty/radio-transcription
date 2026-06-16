import importlib
import unittest
from unittest import mock

from backend.pipeline.notification import send_notification


class TestTracingInitialization(unittest.TestCase):
    def test_tracing_not_initialized_on_import(self) -> None:
        """Verifies that setup_tracing is not called during module import."""
        with mock.patch(
            "backend.pipeline.notification.send_notification.setup_tracing"
        ) as mock_setup_tracing:
            importlib.reload(send_notification)
            mock_setup_tracing.assert_not_called()

    @mock.patch("backend.pipeline.notification.send_notification.setup_tracing")
    def test_tracing_initialized_on_cloud_event(
        self,
        mock_setup_tracing: mock.MagicMock,
    ) -> None:
        """Verifies setup_tracing is called when the cloud event handler runs."""
        mock_event = mock.MagicMock()
        # Mock event.data to return empty dict so parse_cloud_event returns None and exits early
        mock_event.data = {}
        send_notification.send_notification(mock_event)
        mock_setup_tracing.assert_called_once_with(
            service_name="notification-service", use_batch=False
        )
