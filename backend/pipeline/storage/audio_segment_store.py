from __future__ import annotations

import json
import uuid
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import asyncpg

from pydantic import TypeAdapter

from backend.pipeline.storage import audio_segment_queries
from backend.services.audio_segments.models import (
    Annotation,
    AnnotationType,
    AudioSegment,
    AudioSegmentCreate,
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
        if isinstance(row_dict.get("data"), str):
            try:
                row_dict["data"] = json.loads(row_dict["data"])
            except ValueError:
                pass
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
                try:
                    annotations = json.loads(annotations)
                except ValueError:
                    pass
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

        row = await self._pool.fetchrow(
            audio_segment_queries.ADD_ANNOTATION_SQL,
            uid,
            annotation_type,
            data_json,
        )

        if row is None:
            msg = f"Unable to add annotation for segment {segment_id}."
            raise ValueError(msg)

        return annotation_adapter.validate_python(
            self._prepare_annotation_row(dict(row))
        )

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

    async def bulk_add_audio_segments(
        self, segments: list[AudioSegmentCreate]
    ) -> int:
        """Idempotently add multiple audio segments using ON CONFLICT DO NOTHING."""
        if not segments:
            return 0

        params = []
        for seg in segments:
            try:
                seg_id = uuid.UUID(seg.id)
            except ValueError as e:
                msg = f"Invalid segment id UUID: {seg.id}"
                raise ValueError(msg) from e

            try:
                feed_id = uuid.UUID(seg.feed_id)
            except ValueError as e:
                msg = f"Invalid feed id UUID: {seg.feed_id}"
                raise ValueError(msg) from e

            params.append(
                (
                    seg_id,
                    feed_id,
                    seg.classification.value,
                    seg.start_timestamp,
                    seg.end_timestamp,
                    seg.missing_prior_context,
                    seg.missing_post_context,
                    seg.source_audio_uris,
                    seg.canonical_audio_uri,
                    seg.start_audio_offset,
                    seg.end_audio_offset,
                    seg.playback_audio_uri,
                )
            )

        # executemany returns standard result command tag like "INSERT 0 10", but we return length
        await self._pool.executemany(
            audio_segment_queries.BULK_ADD_AUDIO_SEGMENTS_SQL,
            params,
        )
        return len(segments)
