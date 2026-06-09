from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from google.protobuf import json_format

from backend.pipeline.schema_types.evaluated_transcribed_audio_pb2 import (
    EvaluatedTranscribedAudio,
)
from backend.pipeline.storage.transcript_store import SortOrder, TranscriptStore

from .models import ListTranscriptsResponse, Transcript

if TYPE_CHECKING:
    import datetime

logger = logging.getLogger(__name__)


class TranscriptService:
    """Service for managing transcripts, handling interaction with the data from the TranscriptStore and outputting it in the proper format for the Transcripts API."""

    def __init__(self, store: TranscriptStore) -> None:
        self._store = store

    async def create_transcript(self, transcript: Transcript) -> Transcript:
        """Creates a transcript from a Transcript object."""
        msg = EvaluatedTranscribedAudio()
        try:
            # Use exclude_unset=True to avoid sending defaults for fields not provided
            data = transcript.model_dump(mode="json", exclude_unset=True)
            # The API carries text_match spans as a flat list, but the proto
            # wraps them in a TextMatchAnnotation message (a oneof can't hold a
            # repeated field), so re-nest before parsing into the proto.
            for annotation in data.get("rule_annotations", {}).values():
                if isinstance(annotation.get("text_match"), list):
                    annotation["text_match"] = {
                        "spans": annotation["text_match"]
                    }
            json_format.ParseDict(data, msg, ignore_unknown_fields=True)
        except json_format.ParseError as e:
            logger.exception("Failed to parse transcript JSON")
            error_msg = f"Invalid transcript data: {e}"
            raise ValueError(error_msg) from e

        created_msg = await self._store.create_transcript(msg)
        return _to_transcript_model(created_msg)

    async def get_transcript(self, segment_id: str) -> Transcript | None:
        """Fetches a transcript by segment ID."""
        msg = await self._store.get_transcript(segment_id)
        if not msg:
            return None
        return _to_transcript_model(msg)

    async def list_transcripts(
        self,
        limit: int = 100,
        next_token: str | None = None,
        start_time: datetime.datetime | None = None,
        end_time: datetime.datetime | None = None,
        order: SortOrder | str = SortOrder.DESC,
        *,
        is_alert: bool | None = None,
    ) -> ListTranscriptsResponse:
        """Lists all transcripts with pagination and time window."""
        result = await self._store.list_transcripts(
            limit=limit,
            next_token=next_token,
            start_time=start_time,
            end_time=end_time,
            order=order,
            is_alert=is_alert,
        )
        return ListTranscriptsResponse(
            transcripts=[_to_transcript_model(m) for m in result.transcripts],
            next_token=result.next_token,
        )

    async def list_transcripts_by_feed_id(
        self,
        feed_id: str,
        limit: int = 100,
        next_token: str | None = None,
        start_time: datetime.datetime | None = None,
        end_time: datetime.datetime | None = None,
        order: SortOrder | str = SortOrder.DESC,
        *,
        is_alert: bool | None = None,
    ) -> ListTranscriptsResponse:
        """Lists transcripts filtered by feed ID with pagination and time window."""
        result = await self._store.list_transcripts_by_feed_id(
            feed_id,
            limit=limit,
            next_token=next_token,
            start_time=start_time,
            end_time=end_time,
            order=order,
            is_alert=is_alert,
        )
        return ListTranscriptsResponse(
            transcripts=[_to_transcript_model(m) for m in result.transcripts],
            next_token=result.next_token,
        )

    async def delete_transcript(self, segment_id: str) -> bool:
        """Deletes a transcript by segment ID."""
        return await self._store.delete_transcript(segment_id)


def _to_transcript_model(msg: EvaluatedTranscribedAudio) -> Transcript:
    data = json_format.MessageToDict(msg, preserving_proto_field_name=True)
    return Transcript(**data)
