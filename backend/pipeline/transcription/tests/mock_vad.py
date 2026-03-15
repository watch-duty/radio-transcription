"""Module containing a mock VAD implementation for testing."""

import json

from backend.pipeline.transcription.vads import VoiceActivityDetector


class MockVadPlugin(VoiceActivityDetector):
    """
    A stateless, fake Voice Activity Detector for use in unit tests.
    Does not rely on C++ or ONNX dependencies.
    Configured via JSON to consistently return True (speech) or False (silence).
    """

    def __init__(self) -> None:
        self._returns_speech = True

    def setup(self, config_json: str) -> None:
        """
        Parses test configuration.
        Example: '{"returns": false}' forces the mock to always return False.
        """
        config = json.loads(config_json) if config_json else {}
        self._returns_speech = config.get("returns", True)

    def evaluate(self, audio_data: bytes, sample_rate: int) -> bool:
        """Immediately returns the configured boolean regardless of audio bytes."""
        return self._returns_speech
