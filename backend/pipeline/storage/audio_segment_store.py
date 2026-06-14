from __future__ import annotations

import datetime
import json
import uuid
from dataclasses import dataclass

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
    AudioSegmentSummary,
)

annotation_adapter = TypeAdapter(Annotation)

MAX_SUMMARY_WINDOW = datetime.timedelta(days=7)
MAX_SUMMARY_ROWS = 50_000


@dataclass
class PaginatedAudioSegments:
    segments: list[AudioSegment]
    next_token: str | None


@dataclass
class AudioSegmentSummaryWindow:
    summaries: list[AudioSegmentSummary]
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

    async def get_audio_segment(self, segment_id: str) -> AudioSegment | None:
        """Fetch a single audio segment with its annotations, or None."""
        try:
            seg_uuid = uuid.UUID(segment_id)
        except ValueError as e:
            msg = f"Invalid segment_id UUID: {segment_id}"
            raise ValueError(msg) from e

        row = await self._pool.fetchrow(
            audio_segment_queries.GET_AUDIO_SEGMENT_BY_ID_SQL, seg_uuid
        )
        if row is None:
            return None

        return AudioSegment.model_validate(self._prepare_audio_segment(row))

    async def list_audio_segments(
        self,
        feed_ids: list[str] | None = None,
        limit: int = 100,
        next_token: str | None = None,
        start_time: datetime.datetime | None = None,
        end_time: datetime.datetime | None = None,
        order: SortOrder = SortOrder.DESC,
        ids: list[str] | None = None,
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

        id_uuids = None
        if ids:
            try:
                id_uuids = [uuid.UUID(i) for i in ids]
            except ValueError as e:
                msg = f"Invalid id UUID in list: {ids}"
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

        # Specific-id hydration returns all rows: LIMIT NULL, no pagination.
        fetch_limit = None if id_uuids else limit + 1
        rows = await self._pool.fetch(
            query,
            feed_uuids,
            cursor_ts,
            cursor_uid,
            start_time,
            end_time,
            is_alert,
            id_uuids,
            fetch_limit,
        )

        if id_uuids:
            new_next_token = None
        else:
            rows, new_next_token = get_paginated_results(
                rows, limit, "end_timestamp", "id"
            )

        segments = [
            AudioSegment.model_validate(self._prepare_audio_segment(row))
            for row in rows
        ]

        return PaginatedAudioSegments(segments, new_next_token)

    async def list_audio_segment_summaries(
        self,
        start_time: datetime.datetime,
        end_time: datetime.datetime,
        feed_ids: list[str] | None = None,
        next_token: str | None = None,
        *,
        is_alert: bool | None = None,
    ) -> AudioSegmentSummaryWindow:
        """Fetch lightweight summaries in a window.

        Row-capped; a non-null next_token resumes from the last returned row.
        """
        # Normalize naive inputs to UTC; mixing aware/naive raises TypeError.
        if start_time.tzinfo is None:
            start_time = start_time.replace(tzinfo=datetime.UTC)
        if end_time.tzinfo is None:
            end_time = end_time.replace(tzinfo=datetime.UTC)

        if end_time < start_time:
            msg = "end_time must not be before start_time."
            raise ValueError(msg)
        if end_time - start_time > MAX_SUMMARY_WINDOW:
            max_days = MAX_SUMMARY_WINDOW.days
            msg = f"Requested window exceeds the maximum of {max_days} days."
            raise ValueError(msg)

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

        rows = await self._pool.fetch(
            audio_segment_queries.LIST_AUDIO_SEGMENT_SUMMARIES_IN_WINDOW_SQL,
            feed_uuids,
            cursor_ts,
            cursor_uid,
            start_time,
            end_time,
            is_alert,
            MAX_SUMMARY_ROWS + 1,
        )

        # Non-null token = window overflowed; keysets the last returned row.
        rows, new_next_token = get_paginated_results(
            rows, MAX_SUMMARY_ROWS, "end_timestamp", "id"
        )

        summaries = [
            AudioSegmentSummary.model_validate(self._prepare_audio_segment(row))
            for row in rows
        ]

        return AudioSegmentSummaryWindow(summaries, new_next_token)
