from __future__ import annotations

import base64
import datetime
import json
import uuid
from dataclasses import dataclass
from enum import StrEnum

import asyncpg
from pydantic import TypeAdapter

from backend.pipeline.storage import audio_segment_queries
from backend.services.audio_segments.models import (
    Annotation,
    AnnotationType,
    AudioClassification,
    AudioSegment,
)

annotation_adapter = TypeAdapter(Annotation)


class SortOrder(StrEnum):
    ASC = "asc"
    DESC = "desc"


@dataclass
class PaginatedAudioSegments:
    segments: list[AudioSegment]
    next_token: str | None


class AudioSegmentStore:
    """Storage layer for audio segments and annotations against AlloyDB."""

    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    def _decode_cursor(
        self, next_token: str
    ) -> tuple[datetime.datetime, uuid.UUID]:
        """Decode a base64 pagination token into a timestamp and UUID."""
        try:
            decoded = base64.b64decode(next_token).decode("utf-8")
            ts_str, uid_str = decoded.split("|")
            return datetime.datetime.fromisoformat(ts_str), uuid.UUID(uid_str)
        except Exception as e:
            msg = f"Invalid next_token: {e}"
            raise ValueError(msg)

    def _encode_cursor(self, ts: datetime.datetime, uid: uuid.UUID) -> str:
        """Encode a timestamp and UUID into a base64 pagination token."""
        token_str = f"{ts.isoformat()}|{uid}"
        return base64.b64encode(token_str.encode("utf-8")).decode("utf-8")

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
        feed_id: str,
        classification: AudioClassification,
        start_timestamp: datetime.datetime,
        end_timestamp: datetime.datetime,
        source_audio_uris: list[str],
        canonical_audio_uri: str | None = None,
        start_audio_offset: datetime.timedelta | None = None,
        end_audio_offset: datetime.timedelta | None = None,
        playback_audio_uri: str | None = None,
        *,
        missing_prior_context: bool,
        missing_post_context: bool,
        segment_id: uuid.UUID | None = None,
    ) -> AudioSegment:
        """Create a new audio segment."""
        try:
            feed_uuid = uuid.UUID(feed_id)
        except ValueError as e:
            msg = f"Invalid feed_id UUID: {feed_id}"
            raise ValueError(msg) from e

        if segment_id is None:
            segment_id = uuid.uuid4()
        row = await self._pool.fetchrow(
            audio_segment_queries.CREATE_AUDIO_SEGMENT_SQL,
            segment_id,
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
        )

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
        has_alert: bool | None = None,
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
            cursor_ts, cursor_uid = self._decode_cursor(next_token)

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
            has_alert,
            limit + 1,
        )

        has_more = len(rows) > limit
        if has_more:
            rows = rows[:limit]
            last_row = rows[-1]
            new_next_token = self._encode_cursor(
                last_row["end_timestamp"], last_row["id"]
            )
        else:
            new_next_token = None

        segments = [
            AudioSegment.model_validate(self._prepare_audio_segment(row))
            for row in rows
        ]

        return PaginatedAudioSegments(segments, new_next_token)
