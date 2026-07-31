"""Mock transcriber implementation for offline/local testing."""

from backend.pipeline.common.utils import ConfigBase
from backend.pipeline.transcription.transcribers import base


class MockConfig(ConfigBase):
    """Configuration schema for the Mock Transcriber."""

    default_transcript: str = (
        "This is a mock transcription of the radio transmission."
    )
    transcripts: list[str] | None = None


class MockTranscriber(base.Transcriber):
    """A mock transcriber for offline/local testing that does not call external APIs."""

    def __init__(self, config: MockConfig) -> None:
        """Initializes the MockTranscriber with configuration."""
        self.config = config
        self.index = 0

    def setup(self) -> None:
        """Setup hook to reset index."""
        self.index = 0

    async def transcribe(
        self,
        *,
        audio_data: bytes | None = None,
        uri: str | None = None,
        duration_ms: int,
        context: base.TranscriptionContext | None = None,
    ) -> str | None:
        """Mock transcription implementation returns static or rotating mock transcripts."""
        # If a sequence of transcripts is provided, return them in rotation
        if self.config.transcripts:
            transcript = self.config.transcripts[
                self.index % len(self.config.transcripts)
            ]
            self.index += 1
            return transcript

        # Otherwise return the default static transcript
        return self.config.default_transcript
