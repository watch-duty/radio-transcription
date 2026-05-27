from __future__ import annotations

import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from backend.pipeline.storage.audio_segment_store import AudioSegmentStore

    from .models import AudioSegment, AudioSegmentCreate

logger = logging.getLogger(__name__)


class AudioSegmentService:
    """Service for managing audio segments, handling interactions with AudioSegmentStore."""

    def __init__(self, store: AudioSegmentStore) -> None:
        self._store = store

    async def list_audio_segments(
        self, feed_ids: list[str] | None = None
    ) -> list[AudioSegment]:
        """Lists all audio segments, optionally filtered by feed IDs."""
        return await self._store.list_audio_segments(feed_ids)

    async def bulk_add_audio_segments(
        self, segments: list[AudioSegmentCreate]
    ) -> int:
        """Idempotently saves a batch of audio segments in bulk."""
        return await self._store.bulk_add_audio_segments(segments)
