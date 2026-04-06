import dataclasses
import datetime


@dataclasses.dataclass(frozen=True)
class CapturedChunk:
    """A single captured audio chunk yielded by a capture function.

    Attributes:
        audio_bytes: The raw audio file bytes for this segment.
        chunk_start_time: The UTC timestamp of the beginning of the audio window.
        chunk_end_time: The UTC timestamp of the end of the audio window.
    """

    audio_bytes: bytes
    chunk_start_time: datetime.datetime
    chunk_end_time: datetime.datetime
