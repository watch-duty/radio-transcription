import importlib
import unittest
from unittest import mock

from backend.pipeline.normalization import main as normalization_main


class TestTracingInitialization(unittest.TestCase):
    def test_tracing_not_initialized_on_import(self) -> None:
        """Verifies that setup_tracing is not called during module import."""
        with mock.patch(
            "backend.pipeline.normalization.main.setup_tracing"
        ) as mock_setup_tracing:
            importlib.reload(normalization_main)
            mock_setup_tracing.assert_not_called()

    @mock.patch("backend.pipeline.normalization.main.setup_tracing")
    @mock.patch("backend.pipeline.normalization.main.container")
    def test_tracing_initialized_on_cloud_event(
        self,
        mock_container: mock.MagicMock,
        mock_setup_tracing: mock.MagicMock,
    ) -> None:
        """Verifies setup_tracing is called when the cloud event handler runs."""
        mock_event = mock.MagicMock()
        normalization_main.normalize_claim_check(mock_event)
        mock_setup_tracing.assert_called_once_with(
            service_name="normalization-service",
            use_batch=False,
            setup_metrics=True,
        )
