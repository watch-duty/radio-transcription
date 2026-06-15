from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from backend.pipeline.storage.audio_segment_store import SortOrder

from .models import AudioSegmentHistogramResponse, ListAudioSegmentsResponse

if TYPE_CHECKING:
    import datetime

    from backend.pipeline.storage.audio_segment_store import AudioSegmentStore

    from .models import (
        Annotation,
        AnnotationCreate,
        AudioSegment,
        AudioSegmentCreate,
    )

logger = logging.getLogger(__name__)


class AudioSegmentService:
    """Service for managing audio segments, handling interactions with AudioSegmentStore."""

    def __init__(self, store: AudioSegmentStore) -> None:
        self._store = store

    async def list_audio_segments(
        self,
        feed_ids: list[str] | None = None,
        limit: int = 100,
        next_token: str | None = None,
        start_time: datetime.datetime | None = None,
        end_time: datetime.datetime | None = None,
        order: SortOrder = SortOrder.DESC,
        *,
        is_alert: bool | None = None,
    ) -> ListAudioSegmentsResponse:
        """Lists all audio segments with annotations and pagination."""
        result = await self._store.list_audio_segments(
            feed_ids=feed_ids,
            limit=limit,
            next_token=next_token,
            start_time=start_time,
            end_time=end_time,
            order=order,
            is_alert=is_alert,
        )
        return ListAudioSegmentsResponse(
            segments=result.segments, next_token=result.next_token
        )

    async def get_audio_segment_histogram(
        self,
        start_time: datetime.datetime,
        end_time: datetime.datetime,
        feed_ids: list[str] | None = None,
        buckets: int = 288,
        *,
        is_alert: bool | None = None,
    ) -> AudioSegmentHistogramResponse:
        """Returns a bucketed clip-density histogram over a time window."""
        result = await self._store.get_audio_segment_histogram(
            start_time=start_time,
            end_time=end_time,
            feed_ids=feed_ids,
            buckets=buckets,
            is_alert=is_alert,
        )
        return AudioSegmentHistogramResponse(buckets=result)

    async def create_audio_segment(
        self, segment: AudioSegmentCreate
    ) -> AudioSegment:
        """Saves a single audio segment using the store."""
        return await self._store.create_audio_segment(
            segment_id=segment.id,
            feed_id=segment.feed_id,
            classification=segment.classification,
            start_timestamp=segment.start_timestamp,
            end_timestamp=segment.end_timestamp,
            source_audio_uris=segment.source_audio_uris,
            canonical_audio_uri=segment.canonical_audio_uri,
            start_audio_offset=segment.start_audio_offset,
            end_audio_offset=segment.end_audio_offset,
            playback_audio_uri=segment.playback_audio_uri,
            external_audio_segment_id=segment.external_audio_segment_id,
            missing_prior_context=segment.missing_prior_context,
            missing_post_context=segment.missing_post_context,
        )

    async def add_annotation(
        self, segment_id: str, annotation: AnnotationCreate
    ) -> Annotation:
        """Adds an annotation to an audio segment."""
        return await self._store.add_annotation(
            segment_id=segment_id,
            annotation_type=annotation.type,
            data=annotation.data.model_dump(),
        )
