from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from google.protobuf import json_format

from backend.pipeline.schema_types.evaluated_transcribed_audio_pb2 import (
    EvaluatedTranscribedAudio,
)

if TYPE_CHECKING:
    from backend.pipeline.storage.transcript_store import TranscriptStore

logger = logging.getLogger(__name__)


class TranscriptService:
    """Service for managing transcripts, handling interaction with the data from the TranscriptStore and outputting it in the proper format for the Transcripts API."""

    def __init__(self, store: TranscriptStore) -> None:
        self._store = store

    async def create_transcript(self, data: dict[str, Any]) -> dict[str, Any]:
        """Creates a transcript from JSON data."""
        msg = EvaluatedTranscribedAudio()
        try:
            json_format.ParseDict(data, msg, ignore_unknown_fields=True)
        except json_format.ParseError as e:
            logger.exception("Failed to parse transcript JSON")
            error_msg = f"Invalid transcript data: {e}"
            raise ValueError(error_msg) from e

        created_msg = await self._store.create_transcript(msg)

        return json_format.MessageToDict(
            created_msg, preserving_proto_field_name=True
        )

    async def get_transcript(
        self, transmission_id: str
    ) -> dict[str, Any] | None:
        """Fetches a transcript by transmission ID."""
        msg = await self._store.get_transcript(transmission_id)
        if not msg:
            return None
        return json_format.MessageToDict(msg, preserving_proto_field_name=True)

    async def list_transcripts(self) -> list[dict[str, Any]]:
        """Lists all transcripts."""
        msgs = await self._store.list_transcripts()
        return [
            json_format.MessageToDict(m, preserving_proto_field_name=True)
            for m in msgs
        ]

    async def list_transcripts_by_feed_id(
        self, feed_id: str
    ) -> list[dict[str, Any]]:
        """Lists transcripts filtered by feed ID."""
        msgs = await self._store.list_transcripts_by_feed_id(feed_id)
        return [
            json_format.MessageToDict(m, preserving_proto_field_name=True)
            for m in msgs
        ]

    async def delete_transcript(self, transmission_id: str) -> bool:
        """Deletes a transcript by transmission ID."""
        return await self._store.delete_transcript(transmission_id)
