"""Mock implementation of SpeechClient for local testing."""

from typing import Any

from google.cloud.speech_v2 import SpeechClient


class MockAlternative:
    def __init__(self, transcript: str) -> None:
        self.transcript = transcript


class MockResult:
    def __init__(self, transcript: str) -> None:
        self.alternatives = [MockAlternative(transcript)]


class MockResponse:
    def __init__(self, transcript: str) -> None:
        self.results = [MockResult(transcript)]


class MockSpeechClient(SpeechClient):
    """Mock SpeechClient that inherits from SpeechClient to satisfy type checker."""

    def __init__(self) -> None:
        # Do not call super().__init__() to avoid requiring credentials/options
        pass

    def recognize(
        self,
        request: Any = None,
        *,
        recognizer: Any = None,
        config: Any = None,
        config_mask: Any = None,
        content: Any = None,
        uri: Any = None,
        retry: Any = None,
        timeout: Any = None,
        metadata: Any = (),
    ) -> Any:
        return MockResponse("Mock transcript from MockSpeechClient")


def get_mock_speech_client() -> MockSpeechClient:
    return MockSpeechClient()
