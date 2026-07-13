import unittest
from unittest import mock

from fastapi import HTTPException, status

from backend.pipeline.transcription import main as transcription_main
from backend.pipeline.transcription.transcribers.gemini import (
    GeminiTransientTranscriptionError,
)


class TestTranscriptionMain(unittest.IsolatedAsyncioTestCase):
    @mock.patch("backend.pipeline.transcription.main.setup_tracing")
    async def test_transcribe_claim_check_raises_503_on_transient_error(
        self, mock_setup_tracing: mock.MagicMock
    ) -> None:
        """Verifies that a transient error results in an HTTPException 503."""
        mock_processor = mock.AsyncMock()
        mock_processor.process_event.side_effect = (
            GeminiTransientTranscriptionError("Incomplete response from Gemini")
        )

        mock_request = mock.MagicMock()
        mock_request.app.state.processor = mock_processor
        mock_event = {"message": {"data": "dGVzdA=="}}

        with self.assertRaises(HTTPException) as context:
            await transcription_main.transcribe_claim_check(
                mock_event, mock_request
            )

        self.assertEqual(
            context.exception.status_code, status.HTTP_503_SERVICE_UNAVAILABLE
        )
        self.assertIn(
            "Transient error processing message", context.exception.detail
        )

    @mock.patch("backend.pipeline.transcription.main.setup_tracing")
    async def test_transcribe_claim_check_raises_original_error_on_non_transient(
        self, mock_setup_tracing: mock.MagicMock
    ) -> None:
        """Verifies that non-transient errors are re-raised unmodified."""
        mock_processor = mock.AsyncMock()
        mock_processor.process_event.side_effect = ValueError("Some real bug")

        mock_request = mock.MagicMock()
        mock_request.app.state.processor = mock_processor
        mock_event = {"message": {"data": "dGVzdA=="}}

        with self.assertRaises(ValueError) as context:
            await transcription_main.transcribe_claim_check(
                mock_event, mock_request
            )

        self.assertEqual(str(context.exception), "Some real bug")
