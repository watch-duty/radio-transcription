# Python file defining data models for the ingestion pipeline.
import dataclasses
import datetime
from typing import NamedTuple


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


class CollectorEntry(NamedTuple):
    """Registry entry describing how to locate and invoke a collector.

    Attributes:
        module_path: Fully-qualified Python module path of the collector.
        func_name: Name of the capture function within that module.
        url_base: Base URL passed to the capture function.
    """

    module_path: str
    func_name: str
    url_base: str
