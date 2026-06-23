"""Abstract Base Class for pluggable transcription services."""

import abc


class Transcriber(abc.ABC):
    """Abstract interface for handling audio transcription.

    Allows hot-swapping different external transcription APIs or models.
    """

    @abc.abstractmethod
    def setup(self) -> None:
        """Called once per Beam worker upon initialization.

        Use this to spin up clients, establish connections, or load models
        that cannot be pickled/serialized across processes.
        """

    @abc.abstractmethod
    async def transcribe(
        self,
        *,
        audio_data: bytes | None = None,
        uri: str | None = None,
        duration_ms: int,
    ) -> str | None:
        """Transcribes the audio payload either via raw bytes or a GCS URI reference and returns the text transcript.

        Args:
            audio_data: The raw audio payload bytes.
            uri: GCS URI reference of the audio file.
            duration_ms: Duration of the audio file in milliseconds.

        Returns:
            The transcribed text, or None if unintelligible or empty.
        """
