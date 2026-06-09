from __future__ import annotations

import base64
import datetime
import unittest
import uuid

import asyncpg.exceptions

from backend.pipeline.common.exceptions import AlreadyExistsError
from backend.pipeline.schema_types.evaluated_transcribed_audio_pb2 import (
    EvaluatedTranscribedAudio,
)
from backend.pipeline.storage.tests.connection_util import make_mock_pool
from backend.pipeline.storage.transcript_store import TranscriptStore

_SEGMENT_ID = uuid.UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
_FEED_ID = uuid.UUID("bbbbbbbb-cccc-dddd-eeee-ffffffffffff")

_TRANSCRIPT_ROW = {
    "segment_id": _SEGMENT_ID,
    "feed_id": _FEED_ID,
    "transcript": "Hello world",
    "start_timestamp": datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC),
    "end_timestamp": datetime.datetime(2026, 1, 1, 0, 1, tzinfo=datetime.UTC),
    "missing_prior_context": False,
    "missing_post_context": False,
    "source_audio_uris": ["gs://bucket/audio1.ogg"],
    "canonical_audio_uri": "gs://bucket/canonical.ogg",
    "start_audio_offset": datetime.timedelta(seconds=5),
    "end_audio_offset": datetime.timedelta(seconds=10),
    "evaluation_decisions": ["rule-1"],
    "playback_audio_uri": None,
    "evaluation_errors": [],
    "created_at": datetime.datetime(2026, 1, 1, 0, 2, tzinfo=datetime.UTC),
}


class BaseTranscriptStoreTest(unittest.IsolatedAsyncioTestCase):
    """Base class for TranscriptStore tests with a shared prepopulated mock pool."""

    def setUp(self) -> None:
        super().setUp()
        self.pool = make_mock_pool(
            fetchrow_result=_TRANSCRIPT_ROW, fetch_result=[_TRANSCRIPT_ROW]
        )
        self.store = TranscriptStore(self.pool)


class TestCreateTranscript(BaseTranscriptStoreTest):
    """Tests for TranscriptStore.create_transcript."""

    async def test_creates_successfully(self) -> None:
        """Verifies creating a transcript returns the created transcript."""
        msg = EvaluatedTranscribedAudio()
        msg.segment_id = str(_SEGMENT_ID)
        msg.feed_id = str(_FEED_ID)
        msg.transcript = "Hello world"
        msg.start_timestamp.FromDatetime(
            datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC)
        )
        msg.end_timestamp.FromDatetime(
            datetime.datetime(2026, 1, 1, 0, 1, tzinfo=datetime.UTC)
        )
        msg.source_audio_uris.append("gs://bucket/audio1.ogg")
        msg.canonical_audio_uri = "gs://bucket/canonical.ogg"
        msg.start_audio_offset.FromTimedelta(datetime.timedelta(seconds=5))
        msg.end_audio_offset.FromTimedelta(datetime.timedelta(seconds=10))
        msg.evaluation_decisions.append("rule-1")

        result = await self.store.create_transcript(msg)

        self.assertEqual(result.segment_id, str(_SEGMENT_ID))
        self.assertEqual(result.feed_id, str(_FEED_ID))
        self.assertEqual(result.transcript, "Hello world")
        self.assertEqual(len(result.source_audio_uris), 1)
        self.assertEqual(result.source_audio_uris[0], "gs://bucket/audio1.ogg")
        self.assertEqual(
            result.canonical_audio_uri, "gs://bucket/canonical.ogg"
        )
        self.assertEqual(len(result.evaluation_decisions), 1)
        self.assertEqual(result.evaluation_decisions[0], "rule-1")

    async def test_raises_if_row_is_none(self) -> None:
        """Verifies it raises ValueError if no row returned (though unexpected)."""
        self.pool.fetchrow.return_value = None

        msg = EvaluatedTranscribedAudio()
        msg.segment_id = str(_SEGMENT_ID)
        msg.feed_id = str(_FEED_ID)

        with self.assertRaises(ValueError) as cm:
            await self.store.create_transcript(msg)

        self.assertIn(
            "Unable to create transcript for segment", str(cm.exception)
        )

    async def test_raises_invalid_segment_id(self) -> None:
        """Verifies it raises ValueError for invalid segment_id UUID."""
        msg = EvaluatedTranscribedAudio()
        msg.segment_id = "invalid-uuid"
        msg.feed_id = str(_FEED_ID)

        with self.assertRaises(ValueError) as cm:
            await self.store.create_transcript(msg)

        self.assertIn("Invalid segment_id UUID", str(cm.exception))

    async def test_raises_invalid_feed_id(self) -> None:
        """Verifies it raises ValueError for invalid feed_id UUID."""
        msg = EvaluatedTranscribedAudio()
        msg.segment_id = str(_SEGMENT_ID)
        msg.feed_id = "invalid-uuid"

        with self.assertRaises(ValueError) as cm:
            await self.store.create_transcript(msg)

        self.assertIn("Invalid feed_id UUID", str(cm.exception))

    async def test_raises_already_exists_error_on_duplicate(self) -> None:
        """Verifies it raises AlreadyExistsError for duplicate segment_id."""
        self.pool.fetchrow.side_effect = (
            asyncpg.exceptions.UniqueViolationError()
        )

        msg = EvaluatedTranscribedAudio()
        msg.segment_id = str(_SEGMENT_ID)
        msg.feed_id = str(_FEED_ID)

        with self.assertRaises(AlreadyExistsError) as cm:
            await self.store.create_transcript(msg)

        self.assertIn("already exists", str(cm.exception))


class TestGetTranscript(BaseTranscriptStoreTest):
    """Tests for TranscriptStore.get_transcript."""

    async def test_returns_transcript_if_found(self) -> None:
        """Verify fetching valid segment ID returns proto."""
        result = await self.store.get_transcript(str(_SEGMENT_ID))

        self.assertIsNotNone(result)
        assert result is not None
        self.assertEqual(result.segment_id, str(_SEGMENT_ID))

    async def test_returns_none_if_not_found(self) -> None:
        """Verify fetching non-existent segment ID returns None."""
        self.pool.fetchrow.return_value = None

        result = await self.store.get_transcript(str(_SEGMENT_ID))

        self.assertIsNone(result)

    async def test_returns_none_for_invalid_id(self) -> None:
        """Verify fetching with invalid UUID format returns None."""
        result = await self.store.get_transcript("not-a-uuid")

        self.assertIsNone(result)


class TestListTranscriptsByFeedId(BaseTranscriptStoreTest):
    """Tests for TranscriptStore.list_transcripts_by_feed_id."""

    async def test_returns_list_if_found(self) -> None:
        """Verify listing for a feed ID returns list of protos."""
        result = await self.store.list_transcripts_by_feed_id(str(_FEED_ID))

        self.assertEqual(len(result.transcripts), 1)
        self.assertEqual(result.transcripts[0].segment_id, str(_SEGMENT_ID))
        self.assertIsNone(result.next_token)

    async def test_returns_empty_list_for_invalid_id(self) -> None:
        """Verify invalid feed ID returns empty list."""
        result = await self.store.list_transcripts_by_feed_id("not-a-uuid")

        self.assertEqual(result.transcripts, [])
        self.assertIsNone(result.next_token)

    async def test_list_with_limit(self) -> None:
        """Verify listing with limit returns restricted list."""
        self.pool.fetch.return_value = [_TRANSCRIPT_ROW] * 2

        result = await self.store.list_transcripts_by_feed_id(
            str(_FEED_ID), limit=1
        )

        self.assertEqual(len(result.transcripts), 1)
        self.assertIsNotNone(result.next_token)

    async def test_list_with_next_token(self) -> None:
        """Verify listing with next_token parses token and queries correctly."""
        self.pool.fetch.return_value = [_TRANSCRIPT_ROW]

        token_str = f"{datetime.datetime(2026, 1, 1, 0, 1, tzinfo=datetime.UTC).isoformat()}|{_SEGMENT_ID}"
        token = base64.b64encode(token_str.encode("utf-8")).decode("utf-8")

        result = await self.store.list_transcripts_by_feed_id(
            str(_FEED_ID), next_token=token
        )

        self.assertEqual(len(result.transcripts), 1)
        self.assertIsNone(result.next_token)

        # Verify fetch was called with cursor values
        self.pool.fetch.assert_called_once()
        args = self.pool.fetch.call_args[0]
        self.assertEqual(
            args[2], datetime.datetime(2026, 1, 1, 0, 1, tzinfo=datetime.UTC)
        )
        self.assertEqual(args[3], _SEGMENT_ID)

    async def test_list_with_time_window(self) -> None:
        """Verify listing with time window passes arguments to query."""
        self.pool.fetch.return_value = [_TRANSCRIPT_ROW]

        start = datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC)
        end = datetime.datetime(2026, 1, 2, tzinfo=datetime.UTC)

        result = await self.store.list_transcripts_by_feed_id(
            str(_FEED_ID), start_time=start, end_time=end
        )

        self.assertEqual(len(result.transcripts), 1)

        # Verify fetch was called with time window
        self.pool.fetch.assert_called_once()
        args = self.pool.fetch.call_args[0]
        # Params are [query, uid, cursor_ts, cursor_uid, start_time, end_time, is_alert, limit+1]
        self.assertEqual(args[4], start)
        self.assertEqual(args[5], end)

    async def test_list_with_is_alert(self) -> None:
        """Verify listing with is_alert=True passes parameter to query."""
        self.pool.fetch.return_value = [_TRANSCRIPT_ROW]

        result = await self.store.list_transcripts_by_feed_id(
            str(_FEED_ID), is_alert=True
        )

        self.assertEqual(len(result.transcripts), 1)

        self.pool.fetch.assert_called_once()
        args = self.pool.fetch.call_args[0]
        self.assertTrue(args[6])


class TestListTranscripts(BaseTranscriptStoreTest):
    """Tests for TranscriptStore.list_transcripts."""

    async def test_returns_all_transcripts(self) -> None:
        """Verify listing all transcripts returns a list."""
        result = await self.store.list_transcripts()

        self.assertEqual(len(result.transcripts), 1)
        self.assertEqual(result.transcripts[0].segment_id, str(_SEGMENT_ID))
        self.assertIsNone(result.next_token)

    async def test_list_with_is_alert(self) -> None:
        """Verify listing with is_alert=True passes parameter to query."""
        self.pool.fetch.return_value = [_TRANSCRIPT_ROW]

        result = await self.store.list_transcripts(is_alert=True)

        self.assertEqual(len(result.transcripts), 1)

        self.pool.fetch.assert_called_once()
        args = self.pool.fetch.call_args[0]
        # Params are [query, cursor_ts, cursor_uid, start_time, end_time, is_alert, limit+1]
        self.assertTrue(args[5])


class TestDeleteTranscript(BaseTranscriptStoreTest):
    """Tests for TranscriptStore.delete_transcript."""

    async def test_deletes_successfully(self) -> None:
        """Verify successful deletion returns True."""
        self.pool.execute.return_value = "DELETE 1"

        result = await self.store.delete_transcript(str(_SEGMENT_ID))

        self.assertTrue(result)

    async def test_returns_false_if_no_row_deleted(self) -> None:
        """Verify deletion of non-existent row returns False."""
        result = await self.store.delete_transcript(str(_SEGMENT_ID))

        self.assertFalse(result)

    async def test_returns_false_for_invalid_id(self) -> None:
        """Verify invalid UUID returns False without hitting DB."""
        result = await self.store.delete_transcript("not-a-uuid")

        self.assertFalse(result)


if __name__ == "__main__":
    unittest.main()
