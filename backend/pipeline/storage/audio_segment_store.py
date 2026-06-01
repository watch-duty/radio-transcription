from __future__ import annotations

import json
import uuid
from typing import TYPE_CHECKING

import asyncpg

if TYPE_CHECKING:
    import datetime

from pydantic import TypeAdapter

from backend.pipeline.storage import audio_segment_queries
from backend.services.audio_segments.models import (
    Annotation,
    AnnotationType,
    AudioClassification,
    AudioSegment,
)

annotation_adapter = TypeAdapter(Annotation)


class AudioSegmentStore:
    """Storage layer for audio segments and annotations against AlloyDB."""

    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    def _prepare_annotation_row(self, row_dict: dict) -> dict:
        """Prepare an annotation dictionary for Pydantic validation."""
        if row_dict.get("audio_segment_id"):
            row_dict["audio_segment_id"] = str(row_dict["audio_segment_id"])

        data = row_dict.get("data")
        if isinstance(data, str):
            row_dict["data"] = json.loads(data)

        return row_dict

    def _prepare_audio_segment_row(self, row: asyncpg.Record) -> dict:
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
                self._prepare_annotation_row(ann) for ann in annotations
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
            self._prepare_annotation_row(dict(row))
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
    ) -> AudioSegment:
        """Create a new audio segment."""
        try:
            feed_uuid = uuid.UUID(feed_id)
        except ValueError as e:
            msg = f"Invalid feed_id UUID: {feed_id}"
            raise ValueError(msg) from e

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

        return AudioSegment.model_validate(self._prepare_audio_segment_row(row))

    async def list_audio_segments(
        self, feed_ids: list[str] | None = None
    ) -> list[AudioSegment]:
        """List all audio segments bundled with their annotations."""
        feed_uuids = None
        if feed_ids:
            try:
                feed_uuids = [uuid.UUID(fid) for fid in feed_ids]
            except ValueError as e:
                msg = f"Invalid feed_id UUID in list: {feed_ids}"
                raise ValueError(msg) from e

        rows = await self._pool.fetch(
            audio_segment_queries.LIST_AUDIO_SEGMENTS_SQL,
            feed_uuids,
        )

        return [
            AudioSegment.model_validate(self._prepare_audio_segment_row(row))
            for row in rows
        ]
