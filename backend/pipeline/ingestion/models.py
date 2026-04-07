# Python file defining data models for the ingestion pipeline.
from __future__ import annotations

import dataclasses
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import asyncio
    import datetime
    from collections.abc import AsyncIterator, Callable

    from backend.pipeline.storage.feed_store import LeasedFeed


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


if TYPE_CHECKING:
    # 3-arg collector: (feed, shutdown_event, url_base) -> AsyncIterator[CapturedChunk]
    CollectorFn = Callable[
        [LeasedFeed, asyncio.Event, str],
        AsyncIterator[CapturedChunk],
    ]
