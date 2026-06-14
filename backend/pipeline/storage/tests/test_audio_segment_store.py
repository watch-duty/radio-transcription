from __future__ import annotations

import datetime
import unittest
import uuid

import asyncpg

from backend.pipeline.storage import audio_segment_queries
from backend.pipeline.storage.audio_segment_store import (
    AudioSegmentStore,
)
from backend.pipeline.storage.pagination_utils import (
    decode_cursor,
    encode_cursor,
)
from backend.pipeline.storage.tests.connection_util import make_mock_pool
from backend.services.audio_segments.models import (
    AnnotationType,
    AudioClassification,
)


class IsUUID:
    def __eq__(self, uuid_val: object) -> bool:
        return isinstance(uuid_val, uuid.UUID)


_SEGMENT_ID = uuid.UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
_FEED_ID = uuid.UUID("bbbbbbbb-cccc-dddd-eeee-ffffffffffff")

_ANNOTATION_ROW = {
    "audio_segment_id": _SEGMENT_ID,
    "type": "TRANSCRIPT",
    "data": {"text": "hello", "errors": []},
    "created_at": datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC),
    "updated_at": datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC),
}

_AUDIO_SEGMENT_ROW = {
    "id": _SEGMENT_ID,
    "feed_id": _FEED_ID,
    "classification": "SPEECH",
    "start_timestamp": datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC),
    "end_timestamp": datetime.datetime(2026, 1, 1, 0, 1, tzinfo=datetime.UTC),
    "missing_prior_context": False,
    "missing_post_context": False,
    "source_audio_uris": ["gs://bucket/audio1.ogg"],
    "canonical_audio_uri": "gs://bucket/canonical.ogg",
    "start_audio_offset": datetime.timedelta(seconds=5),
    "end_audio_offset": datetime.timedelta(seconds=10),
    "playback_audio_uri": None,
    "created_at": datetime.datetime(2026, 1, 1, 0, 2, tzinfo=datetime.UTC),
    "annotations": [
        {
            "audio_segment_id": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
            "type": "TRANSCRIPT",
            "data": {"text": "hello", "errors": []},
            "created_at": "2026-01-01T00:00:00Z",
            "updated_at": "2026-01-01T00:00:00Z",
        }
    ],
}


_SUMMARY_ROW = {
    "id": _SEGMENT_ID,
    "feed_id": _FEED_ID,
    "start_timestamp": datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC),
    "end_timestamp": datetime.datetime(2026, 1, 1, 0, 1, tzinfo=datetime.UTC),
    "classification": "SPEECH",
    "is_alert": False,
}


class TestAudioSegmentStore(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.pool = make_mock_pool(
            fetchrow_result=_ANNOTATION_ROW, fetch_result=[_AUDIO_SEGMENT_ROW]
        )
        self.store = AudioSegmentStore(self.pool)

    async def test_add_annotation_success(self) -> None:
        result = await self.store.add_annotation(
            str(_SEGMENT_ID),
            AnnotationType.TRANSCRIPT,
            {"text": "hello", "errors": []},
        )

        self.assertEqual(result.audio_segment_id, str(_SEGMENT_ID))
        self.assertEqual(result.type, "TRANSCRIPT")
        self.assertEqual(
            result.data.model_dump(), {"text": "hello", "errors": []}
        )

    async def test_add_evaluation_annotation_success(self) -> None:
        eval_row = {
            "audio_segment_id": _SEGMENT_ID,
            "type": "EVALUATION",
            "data": {"decisions": ["rule-1"], "errors": []},
            "created_at": datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC),
            "updated_at": datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC),
        }
        self.pool.fetchrow.return_value = eval_row

        result = await self.store.add_annotation(
            str(_SEGMENT_ID),
            AnnotationType.EVALUATION,
            {"decisions": ["rule-1"], "errors": []},
        )

        self.assertEqual(result.audio_segment_id, str(_SEGMENT_ID))
        self.assertEqual(result.type, "EVALUATION")
        self.assertEqual(
            result.data.model_dump(), {"decisions": ["rule-1"], "errors": []}
        )

    async def test_add_annotation_invalid_uuid(self) -> None:
        with self.assertRaises(ValueError) as cm:
            await self.store.add_annotation(
                "invalid-uuid", AnnotationType.TRANSCRIPT, {"text": "hello"}
            )
        self.assertIn("Invalid segment_id UUID", str(cm.exception))

    async def test_add_annotation_segment_not_found(self) -> None:
        self.pool.fetchrow.side_effect = (
            asyncpg.exceptions.ForeignKeyViolationError()
        )
        with self.assertRaises(ValueError) as cm:
            await self.store.add_annotation(
                str(_SEGMENT_ID), AnnotationType.TRANSCRIPT, {"text": "hello"}
            )
        self.assertIn("does not exist", str(cm.exception))

    async def test_create_audio_segment_success(self) -> None:
        new_row = _AUDIO_SEGMENT_ROW.copy()
        new_row.pop("annotations", None)
        self.pool.fetchrow.return_value = new_row

        result = await self.store.create_audio_segment(
            segment_id=str(_SEGMENT_ID),
            feed_id=str(_FEED_ID),
            classification=AudioClassification.SPEECH,
            start_timestamp=datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC),
            end_timestamp=datetime.datetime(
                2026, 1, 1, 0, 1, tzinfo=datetime.UTC
            ),
            source_audio_uris=["gs://bucket/audio1.ogg"],
            canonical_audio_uri="gs://bucket/canonical.ogg",
            start_audio_offset=datetime.timedelta(seconds=5),
            end_audio_offset=datetime.timedelta(seconds=10),
            playback_audio_uri=None,
            missing_prior_context=False,
            missing_post_context=False,
        )

        self.assertEqual(result.id, str(_SEGMENT_ID))
        self.assertEqual(result.feed_id, str(_FEED_ID))
        missing_prior_context = False
        missing_post_context = False
        self.pool.fetchrow.assert_called_once_with(
            audio_segment_queries.CREATE_AUDIO_SEGMENT_SQL,
            _SEGMENT_ID,
            _FEED_ID,
            AudioClassification.SPEECH,
            datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC),
            datetime.datetime(2026, 1, 1, 0, 1, tzinfo=datetime.UTC),
            missing_prior_context,
            missing_post_context,
            ["gs://bucket/audio1.ogg"],
            "gs://bucket/canonical.ogg",
            datetime.timedelta(seconds=5),
            datetime.timedelta(seconds=10),
            None,
            None,
        )

    async def test_create_audio_segment_invalid_segment_id(self) -> None:
        with self.assertRaises(ValueError) as cm:
            await self.store.create_audio_segment(
                segment_id="invalid-uuid",
                feed_id=str(_FEED_ID),
                classification=AudioClassification.SPEECH,
                start_timestamp=datetime.datetime(
                    2026, 1, 1, tzinfo=datetime.UTC
                ),
                end_timestamp=datetime.datetime(
                    2026, 1, 1, 0, 1, tzinfo=datetime.UTC
                ),
                source_audio_uris=["gs://bucket/audio1.ogg"],
                missing_prior_context=False,
                missing_post_context=False,
            )
        self.assertIn("Invalid segment_id UUID", str(cm.exception))

    async def test_create_audio_segment_invalid_feed_id(self) -> None:
        with self.assertRaises(ValueError) as cm:
            await self.store.create_audio_segment(
                segment_id=str(_SEGMENT_ID),
                feed_id="invalid-uuid",
                classification=AudioClassification.SPEECH,
                start_timestamp=datetime.datetime(
                    2026, 1, 1, tzinfo=datetime.UTC
                ),
                end_timestamp=datetime.datetime(
                    2026, 1, 1, 0, 1, tzinfo=datetime.UTC
                ),
                source_audio_uris=["gs://bucket/audio1.ogg"],
                missing_prior_context=False,
                missing_post_context=False,
            )
        self.assertIn("Invalid feed_id UUID", str(cm.exception))

    async def test_create_audio_segment_foreign_key_violation(self) -> None:
        self.pool.fetchrow.side_effect = (
            asyncpg.exceptions.ForeignKeyViolationError()
        )
        with self.assertRaises(ValueError) as cm:
            await self.store.create_audio_segment(
                segment_id=str(_SEGMENT_ID),
                feed_id=str(_FEED_ID),
                classification=AudioClassification.SPEECH,
                start_timestamp=datetime.datetime(
                    2026, 1, 1, tzinfo=datetime.UTC
                ),
                end_timestamp=datetime.datetime(
                    2026, 1, 1, 0, 1, tzinfo=datetime.UTC
                ),
                source_audio_uris=["gs://bucket/audio1.ogg"],
                missing_prior_context=False,
                missing_post_context=False,
            )
        self.assertIn("does not exist", str(cm.exception))

    async def test_list_audio_segments(self) -> None:
        result = await self.store.list_audio_segments()

        self.assertEqual(len(result.segments), 1)
        self.assertEqual(result.segments[0].id, str(_SEGMENT_ID))
        self.pool.fetch.assert_called_once_with(
            audio_segment_queries.LIST_AUDIO_SEGMENTS_DESC_SQL,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            101,
        )

    async def test_list_audio_segments_with_feed_id(self) -> None:
        result = await self.store.list_audio_segments([str(_FEED_ID)])

        self.assertEqual(len(result.segments), 1)
        self.pool.fetch.assert_called_once_with(
            audio_segment_queries.LIST_AUDIO_SEGMENTS_DESC_SQL,
            [_FEED_ID],
            None,
            None,
            None,
            None,
            None,
            None,
            101,
        )

    async def test_list_audio_segments_with_ids(self) -> None:
        result = await self.store.list_audio_segments(ids=[str(_SEGMENT_ID)])

        self.assertEqual(len(result.segments), 1)
        # An id request ignores limit/pagination: LIMIT NULL, no next_token.
        self.assertIsNone(result.next_token)
        self.pool.fetch.assert_called_once_with(
            audio_segment_queries.LIST_AUDIO_SEGMENTS_DESC_SQL,
            None,
            None,
            None,
            None,
            None,
            None,
            [_SEGMENT_ID],
            None,
        )

    async def test_list_audio_segments_invalid_feed_id(self) -> None:
        with self.assertRaises(ValueError) as cm:
            await self.store.list_audio_segments(["invalid-uuid"])
        self.assertIn("Invalid feed_id UUID in list", str(cm.exception))

    async def test_list_audio_segments_invalid_id(self) -> None:
        with self.assertRaises(ValueError) as cm:
            await self.store.list_audio_segments(ids=["invalid-uuid"])
        self.assertIn("Invalid id UUID in list", str(cm.exception))

    async def test_list_audio_segment_summaries(self) -> None:
        self.pool.fetch.return_value = [_SUMMARY_ROW]
        start = datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC)
        end = datetime.datetime(2026, 1, 2, tzinfo=datetime.UTC)

        result = await self.store.list_audio_segment_summaries(
            start_time=start, end_time=end
        )

        self.assertIsNone(result.next_token)
        self.assertEqual(len(result.summaries), 1)
        self.assertEqual(result.summaries[0].id, str(_SEGMENT_ID))
        self.assertFalse(result.summaries[0].is_alert)
        self.pool.fetch.assert_called_once_with(
            audio_segment_queries.LIST_AUDIO_SEGMENT_SUMMARIES_IN_WINDOW_SQL,
            None,
            None,
            None,
            start,
            end,
            None,
            101,
        )

    async def test_list_audio_segment_summaries_feed_and_alert(self) -> None:
        self.pool.fetch.return_value = [_SUMMARY_ROW]
        start = datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC)
        end = datetime.datetime(2026, 1, 2, tzinfo=datetime.UTC)

        is_alert = True
        await self.store.list_audio_segment_summaries(
            start_time=start,
            end_time=end,
            feed_ids=[str(_FEED_ID)],
            is_alert=is_alert,
        )

        self.pool.fetch.assert_called_once_with(
            audio_segment_queries.LIST_AUDIO_SEGMENT_SUMMARIES_IN_WINDOW_SQL,
            [_FEED_ID],
            None,
            None,
            start,
            end,
            is_alert,
            101,
        )

    async def test_list_audio_segment_summaries_resume(self) -> None:
        self.pool.fetch.return_value = [_SUMMARY_ROW]
        start = datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC)
        end = datetime.datetime(2026, 1, 2, tzinfo=datetime.UTC)
        cursor_ts = datetime.datetime(2026, 1, 1, 12, tzinfo=datetime.UTC)
        token = encode_cursor(cursor_ts, _SEGMENT_ID)

        await self.store.list_audio_segment_summaries(
            start_time=start, end_time=end, next_token=token
        )

        self.pool.fetch.assert_called_once_with(
            audio_segment_queries.LIST_AUDIO_SEGMENT_SUMMARIES_IN_WINDOW_SQL,
            None,
            cursor_ts,
            _SEGMENT_ID,
            start,
            end,
            None,
            101,
        )

    async def test_list_audio_segment_summaries_end_before_start(self) -> None:
        start = datetime.datetime(2026, 1, 2, tzinfo=datetime.UTC)
        end = datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC)
        with self.assertRaises(ValueError) as cm:
            await self.store.list_audio_segment_summaries(
                start_time=start, end_time=end
            )
        self.assertIn("must not be before", str(cm.exception))
        self.pool.fetch.assert_not_called()

    async def test_list_audio_segment_summaries_naive_start_time(self) -> None:
        # Naive start vs aware end must not raise TypeError.
        self.pool.fetch.return_value = [_SUMMARY_ROW]
        naive_start = datetime.datetime(2026, 1, 1)
        aware_end = datetime.datetime(2026, 1, 2, tzinfo=datetime.UTC)

        result = await self.store.list_audio_segment_summaries(
            start_time=naive_start, end_time=aware_end
        )

        self.assertEqual(len(result.summaries), 1)

    async def test_list_audio_segment_summaries_invalid_feed_id(self) -> None:
        start = datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC)
        end = datetime.datetime(2026, 1, 2, tzinfo=datetime.UTC)
        with self.assertRaises(ValueError) as cm:
            await self.store.list_audio_segment_summaries(
                start_time=start, end_time=end, feed_ids=["invalid-uuid"]
            )
        self.assertIn("Invalid feed_id UUID in list", str(cm.exception))

    async def test_list_audio_segment_summaries_full_page_returns_token(
        self,
    ) -> None:
        start = datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC)
        end = datetime.datetime(2026, 1, 2, tzinfo=datetime.UTC)
        # limit+1 rows fetched => a next page exists.
        self.pool.fetch.return_value = [_SUMMARY_ROW] * 3
        result = await self.store.list_audio_segment_summaries(
            start_time=start, end_time=end, limit=2
        )

        assert result.next_token is not None
        self.assertEqual(len(result.summaries), 2)
        cursor_ts, cursor_uid = decode_cursor(result.next_token)
        self.assertEqual(cursor_ts, _SUMMARY_ROW["end_timestamp"])
        self.assertEqual(cursor_uid, _SEGMENT_ID)

    def test_summaries_keyset_order_and_resume_stay_coupled(self) -> None:
        """Guards keyset determinism across same-timestamp ties.

        end_timestamp is not unique, so the sort needs the unique id as a
        tiebreaker and the resume predicate must skip strictly past the
        cursor's (end_timestamp, id) in that same direction. If the ORDER BY
        and the predicate ever drift apart, tied rows can straddle a page
        boundary and be dropped or duplicated. The cursor packs those same
        two columns (asserted in the overflow test above).
        """
        sql = audio_segment_queries.LIST_AUDIO_SEGMENT_SUMMARIES_IN_WINDOW_SQL
        # Total order: timestamp, then unique id tiebreaker, both DESC.
        self.assertIn("ORDER BY s.end_timestamp DESC, s.id DESC", sql)
        # Resume must match that DESC order: strictly-less comparators on the
        # same (end_timestamp, id) the cursor binds to $2/$3.
        self.assertIn(
            "s.end_timestamp < $2 OR (s.end_timestamp = $2 AND s.id < $3)",
            sql,
        )


if __name__ == "__main__":
    unittest.main()
