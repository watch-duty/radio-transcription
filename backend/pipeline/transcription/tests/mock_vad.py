"""Module containing a mock VAD implementation for testing."""

import json

from backend.pipeline.transcription.vads import VoiceActivityDetector


class MockVadPlugin(VoiceActivityDetector):
    """A test-only implementation of VoiceActivityDetector that deterministically returns speech flags based on configuration."""

    def __init__(self) -> None:
        self._returns_speech = True

    def setup(self, config_json: str) -> None:
        """Parses the mocked JSON config to determine whether subsequent evaluations should return True or False."""
        config = json.loads(config_json) if config_json else {}
        self._returns_speech = config.get("returns", True)

    def evaluate(self, audio_data: bytes, sample_rate: int) -> bool:

        return self._returns_speech
