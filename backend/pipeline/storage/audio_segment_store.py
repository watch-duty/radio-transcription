from __future__ import annotations

import json
import uuid
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import asyncpg

from backend.services.audio_segments.models import (
    Annotation,
    AnnotationType,
    AudioSegment,
)

from . import audio_segment_queries


class AudioSegmentStore:
    """Storage layer for audio segments and annotations against AlloyDB."""

    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    def _prepare_row(self, row: asyncpg.Record) -> dict:
        """Prepare a database row for Pydantic validation."""
        data = dict(row)
        if data.get("id"):
            data["id"] = str(data["id"])
        if data.get("feed_id"):
            data["feed_id"] = str(data["feed_id"])

        # Handle annotations if present
        annotations = data.get("annotations")
        if annotations:
            if isinstance(annotations, str):
                annotations = json.loads(annotations)

            # Convert UUIDs to strings in annotations
            for ann in annotations:
                if ann.get("audio_segment_id"):
                    ann["audio_segment_id"] = str(ann["audio_segment_id"])

            data["annotations"] = annotations

        return data

    async def add_annotation(
        self, segment_id: str, type_str: AnnotationType, data: dict
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
            type_str,
            data_json,
        )

        if row is None:
            msg = f"Unable to add annotation for segment {segment_id}."
            raise ValueError(msg)

        row_dict = dict(row)
        if isinstance(row_dict.get("data"), str):
            row_dict["data"] = json.loads(row_dict["data"])

        # Ensure audio_segment_id is converted to string
        if row_dict.get("audio_segment_id"):
            row_dict["audio_segment_id"] = str(row_dict["audio_segment_id"])

        return Annotation(**row_dict)

    async def list_audio_segments(self) -> list[AudioSegment]:
        """List all audio segments bundled with their annotations."""
        rows = await self._pool.fetch(
            audio_segment_queries.LIST_AUDIO_SEGMENTS_SQL
        )

        segments = []
        for row in rows:
            prepared = self._prepare_row(row)
            segments.append(AudioSegment(**prepared))

        return segments
