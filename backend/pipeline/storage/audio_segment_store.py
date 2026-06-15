from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from typing import TYPE_CHECKING

import asyncpg
from pydantic import TypeAdapter

from backend.pipeline.storage import audio_segment_queries
from backend.pipeline.storage.pagination_utils import (
    SortOrder,
    decode_cursor,
    get_paginated_results,
)
from backend.services.audio_segments.models import (
    Annotation,
    AnnotationType,
    AudioClassification,
    AudioSegment,
    HistogramBucket,
)

if TYPE_CHECKING:
    import datetime

annotation_adapter = TypeAdapter(Annotation)


@dataclass
class PaginatedAudioSegments:
    segments: list[AudioSegment]
    next_token: str | None


class AudioSegmentStore:
    """Storage layer for audio segments and annotations against AlloyDB."""

    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    def _prepare_annotation(self, row_dict: dict) -> dict:
        """Prepare an annotation dictionary for Pydantic validation."""
        if row_dict.get("audio_segment_id"):
            row_dict["audio_segment_id"] = str(row_dict["audio_segment_id"])

        data = row_dict.get("data")
        if isinstance(data, str):
            row_dict["data"] = json.loads(data)

        return row_dict

    def _prepare_audio_segment(self, row: asyncpg.Record) -> dict:
        """Prepare a database row for Pydantic validation as an AudioSegment."""
        data = dict(row)
        if data.get("id"):
            data["id"] = str(data["id"])
        if data.get("feed_id"):
            data["feed_id"] = str(data["feed_id"])

        annotations = data.get("annotations")
        if annotations:
            if isinstance(annotations, str):
                annotations = json.loads(annotations)
            data["annotations"] = [
                self._prepare_annotation(ann) for ann in annotations
            ]

        return data

    async def add_annotation(
        self, segment_id: str, annotation_type: AnnotationType, data: dict
    ) -> Annotation:
        """Add an annotation to an audio segment."""
        try:
            uid = uuid.UUID(segment_id)
        except ValueError as e:
            msg = f"Invalid segment_id UUID: {segment_id}"
            raise ValueError(msg) from e

        data_json = json.dumps(data)

        try:
            row = await self._pool.fetchrow(
                audio_segment_queries.ADD_ANNOTATION_SQL,
                uid,
                annotation_type,
                data_json,
            )
        except asyncpg.exceptions.ForeignKeyViolationError as e:
            msg = f"Audio segment {segment_id} does not exist."
            raise ValueError(msg) from e

        if row is None:
            msg = f"Unable to add annotation for segment {segment_id}."
            raise ValueError(msg)

        return annotation_adapter.validate_python(
            self._prepare_annotation(dict(row))
        )

    async def create_audio_segment(
        self,
        segment_id: str,
        feed_id: str,
        classification: AudioClassification,
        start_timestamp: datetime.datetime,
        end_timestamp: datetime.datetime,
        source_audio_uris: list[str],
        canonical_audio_uri: str | None = None,
        start_audio_offset: datetime.timedelta | None = None,
        end_audio_offset: datetime.timedelta | None = None,
        playback_audio_uri: str | None = None,
        external_audio_segment_id: str | None = None,
        *,
        missing_prior_context: bool,
        missing_post_context: bool,
    ) -> AudioSegment:
        """Create a new audio segment."""
        try:
            segment_uuid = uuid.UUID(segment_id)
        except ValueError as e:
            msg = f"Invalid segment_id UUID: {segment_id}"
            raise ValueError(msg) from e

        try:
            feed_uuid = uuid.UUID(feed_id)
        except ValueError as e:
            msg = f"Invalid feed_id UUID: {feed_id}"
            raise ValueError(msg) from e

        try:
            row = await self._pool.fetchrow(
                audio_segment_queries.CREATE_AUDIO_SEGMENT_SQL,
                segment_uuid,
                feed_uuid,
                classification,
                start_timestamp,
                end_timestamp,
                missing_prior_context,
                missing_post_context,
                source_audio_uris,
                canonical_audio_uri,
                start_audio_offset,
                end_audio_offset,
                playback_audio_uri,
                external_audio_segment_id,
            )
        except asyncpg.exceptions.ForeignKeyViolationError as e:
            msg = f"Feed {feed_id} does not exist."
            raise ValueError(msg) from e

        if row is None:
            msg = "Unable to create audio segment."
            raise ValueError(msg)

        return AudioSegment.model_validate(self._prepare_audio_segment(row))

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
    ) -> PaginatedAudioSegments:
        """List all audio segments bundled with their annotations."""
        feed_uuids = None
        if feed_ids:
            try:
                feed_uuids = [uuid.UUID(fid) for fid in feed_ids]
            except ValueError as e:
                msg = f"Invalid feed_id UUID in list: {feed_ids}"
                raise ValueError(msg) from e

        cursor_ts = None
        cursor_uid = None
        if next_token:
            cursor_ts, cursor_uid = decode_cursor(next_token)

        is_asc = order == SortOrder.ASC
        query = (
            audio_segment_queries.LIST_AUDIO_SEGMENTS_ASC_SQL
            if is_asc
            else audio_segment_queries.LIST_AUDIO_SEGMENTS_DESC_SQL
        )

        rows = await self._pool.fetch(
            query,
            feed_uuids,
            cursor_ts,
            cursor_uid,
            start_time,
            end_time,
            is_alert,
            limit + 1,
        )

        rows, new_next_token = get_paginated_results(
            rows, limit, "end_timestamp", "id"
        )

        segments = [
            AudioSegment.model_validate(self._prepare_audio_segment(row))
            for row in rows
        ]

        return PaginatedAudioSegments(segments, new_next_token)

    async def get_audio_segment_histogram(
        self,
        start_time: datetime.datetime,
        end_time: datetime.datetime,
        feed_ids: list[str] | None = None,
        buckets: int = 288,
        *,
        is_alert: bool | None = None,
    ) -> list[HistogramBucket]:
        """Clip-density histogram over [start_time, end_time) by start_timestamp.

        The window is split into `buckets` equal-width bins. Empty bins are
        omitted. Each bucket's is_alert is true if any clip in it is an alert.
        An inverted window matches no rows.
        """
        if buckets < 1:
            msg = f"buckets must be >= 1, got {buckets}"
            raise ValueError(msg)

        feed_uuids = None
        if feed_ids:
            try:
                feed_uuids = [uuid.UUID(fid) for fid in feed_ids]
            except ValueError as e:
                msg = f"Invalid feed_id UUID in list: {feed_ids}"
                raise ValueError(msg) from e

        rows = await self._pool.fetch(
            audio_segment_queries.AUDIO_SEGMENT_HISTOGRAM_SQL,
            feed_uuids,
            start_time,
            end_time,
            buckets,
            is_alert,
        )

        bucket_width = (end_time - start_time) / buckets
        return [
            HistogramBucket(
                bucket_start=start_time
                + bucket_width * (row["bucket_index"] - 1),
                count=row["count"],
                is_alert=bool(row["is_alert"]),
            )
            for row in rows
        ]
