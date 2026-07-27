"""Abstract Base Class for pluggable transcription services."""

import abc
import dataclasses


@dataclasses.dataclass(frozen=True, slots=True)
class TranscriptionContext:
    """Segment metadata available to transcriber diagnostics.

    Attributes:
        segment_id: Stable identifier for the segment being transcribed.
    """

    segment_id: str


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
        context: TranscriptionContext | None = None,
    ) -> str | None:
        """Transcribes the audio payload either via raw bytes or a GCS URI
        reference and returns the text transcript.

        Args:
            audio_data: The raw audio payload bytes.
            uri: GCS URI reference of the audio file.
            duration_ms: Duration of the audio file in milliseconds.
            context: Optional segment metadata for request diagnostics.

        Returns:
            The transcribed text, or None if unintelligible or empty.
        """

    async def close(self) -> None:
        """Optional cleanup hook for closing client connections."""
        return
