from __future__ import annotations

import uuid

import asyncpg
import asyncpg.exceptions

from backend.pipeline.common.exceptions import AlreadyExistsError
from backend.pipeline.schema_types.evaluated_transcribed_audio_pb2 import (
    EvaluatedTranscribedAudio,
)

from . import transcript_queries


class TranscriptStore:
    """
    Storage layer for evaluated transmissions/transcripts against AlloyDB.

    Provides atomic SQL operations for Creating, Reading, and Deleting transcripts.
    Uses the EvaluatedTranscribedAudio protobuf message as the data model.
    """

    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    def _row_to_proto(self, row: asyncpg.Record) -> EvaluatedTranscribedAudio:
        """
        Convert a database row (asyncpg.Record) to EvaluatedTranscribedAudio.

        In the future, we may consider storing the proto directly instead, but for now
        we'll maintain them as separate types.
        """
        msg = EvaluatedTranscribedAudio()
        msg.transmission_id = str(row["transmission_id"])
        msg.feed_id = str(row["feed_id"])
        msg.transcript = row["transcript"]

        if row["start_timestamp"]:
            msg.start_timestamp.FromDatetime(row["start_timestamp"])
        if row["end_timestamp"]:
            msg.end_timestamp.FromDatetime(row["end_timestamp"])

        msg.missing_prior_context = row["missing_prior_context"]
        msg.missing_post_context = row["missing_post_context"]

        if row["source_audio_uris"]:
            msg.source_audio_uris.extend(row["source_audio_uris"])

        if row["canonical_audio_uri"]:
            msg.canonical_audio_uri = row["canonical_audio_uri"]

        if row["start_audio_offset"]:
            msg.start_audio_offset.FromTimedelta(row["start_audio_offset"])
        if row["end_audio_offset"]:
            msg.end_audio_offset.FromTimedelta(row["end_audio_offset"])

        if row["evaluation_decisions"]:
            msg.evaluation_decisions.extend(row["evaluation_decisions"])

        return msg

    async def create_transcript(
        self, transcript: EvaluatedTranscribedAudio
    ) -> EvaluatedTranscribedAudio:
        """Stores a new transcript record."""
        try:
            transmission_id = uuid.UUID(transcript.transmission_id)
        except ValueError as e:
            msg = f"Invalid transmission_id UUID: {transcript.transmission_id}"
            raise ValueError(msg) from e

        try:
            feed_id = uuid.UUID(transcript.feed_id)
        except ValueError as e:
            msg = f"Invalid feed_id UUID: {transcript.feed_id}"
            raise ValueError(msg) from e

        start_ts = transcript.start_timestamp.ToDatetime()
        end_ts = transcript.end_timestamp.ToDatetime()

        start_offset = None
        if transcript.HasField("start_audio_offset"):
            start_offset = transcript.start_audio_offset.ToTimedelta()

        end_offset = None
        if transcript.HasField("end_audio_offset"):
            end_offset = transcript.end_audio_offset.ToTimedelta()

        try:
            row = await self._pool.fetchrow(
                transcript_queries.CREATE_TRANSCRIPT_SQL,
                transmission_id,
                feed_id,
                transcript.transcript,
                start_ts,
                end_ts,
                transcript.missing_prior_context,
                transcript.missing_post_context,
                list(transcript.source_audio_uris),
                transcript.canonical_audio_uri or None,
                start_offset,
                end_offset,
                list(transcript.evaluation_decisions),
            )
        except asyncpg.exceptions.UniqueViolationError as e:
            raise AlreadyExistsError(str(transmission_id)) from e

        if row is None:
            msg = f"Unable to create transcript for transmission {transmission_id}."
            raise ValueError(msg)

        return self._row_to_proto(row)

    async def get_transcript(
        self, transmission_id: str
    ) -> EvaluatedTranscribedAudio | None:
        """Fetch a specific transcript by transmission ID."""
        try:
            uid = uuid.UUID(transmission_id)
        except ValueError:
            return None

        row = await self._pool.fetchrow(
            transcript_queries.GET_TRANSCRIPT_SQL, uid
        )
        if row is None:
            return None

        return self._row_to_proto(row)

    async def list_transcripts_by_feed_id(
        self, feed_id: str
    ) -> list[EvaluatedTranscribedAudio]:
        """Lists all transcripts for a specific feed ID."""
        try:
            uid = uuid.UUID(feed_id)
        except ValueError:
            return []

        rows = await self._pool.fetch(
            transcript_queries.GET_TRANSCRIPTS_BY_FEED_SQL, uid
        )
        return [self._row_to_proto(row) for row in rows]

    async def list_transcripts(self) -> list[EvaluatedTranscribedAudio]:
        """Lists all transcripts ordered by end_timestamp descending."""
        rows = await self._pool.fetch(transcript_queries.LIST_TRANSCRIPTS_SQL)
        return [self._row_to_proto(row) for row in rows]

    async def delete_transcript(self, transmission_id: str) -> bool:
        """Deletes a transcript."""
        try:
            uid = uuid.UUID(transmission_id)
        except ValueError:
            return False

        result = await self._pool.execute(
            transcript_queries.DELETE_TRANSCRIPT_SQL, uid
        )
        return result == "DELETE 1"
