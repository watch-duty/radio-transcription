import importlib
import unittest
from unittest import mock

from backend.pipeline.transcription import main as transcription_main


class TestTracingInitialization(unittest.TestCase):
    def test_tracing_not_initialized_on_import(self) -> None:
        """Verifies that setup_tracing is not called during module import."""
        with mock.patch(
            "backend.pipeline.transcription.main.setup_tracing"
        ) as mock_setup_tracing:
            importlib.reload(transcription_main)
            mock_setup_tracing.assert_not_called()

    @mock.patch("backend.pipeline.transcription.main.setup_tracing")
    @mock.patch("backend.pipeline.transcription.main.container")
    def test_tracing_initialized_on_cloud_event(
        self,
        mock_container: mock.MagicMock,
        mock_setup_tracing: mock.MagicMock,
    ) -> None:
        """Verifies setup_tracing is called when the cloud event handler runs."""
        mock_event = mock.MagicMock()
        transcription_main.transcribe_claim_check(mock_event)
        mock_setup_tracing.assert_called_once_with(
            service_name="transcription-service", use_batch=False
        )
