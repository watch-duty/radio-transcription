"""Mock implementation of SpeechClient for local testing."""

from typing import Any
from unittest.mock import MagicMock


def get_mock_speech_client() -> Any:
    """Returns a MagicMock configured to act like SpeechClient."""
    mock_client = MagicMock()
    mock_response = MagicMock()
    mock_result = MagicMock()
    mock_result.alternatives = [
        MagicMock(transcript="Mock transcript from MockSpeechClient")
    ]
    mock_response.results = [mock_result]
    mock_client.recognize.return_value = mock_response
    return mock_client
