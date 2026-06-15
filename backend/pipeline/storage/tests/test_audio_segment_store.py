from __future__ import annotations

import datetime
import unittest
import uuid

import asyncpg

from backend.pipeline.storage import audio_segment_queries
from backend.pipeline.storage.audio_segment_store import AudioSegmentStore
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
            101,
        )

    async def test_list_audio_segments_invalid_feed_id(self) -> None:
        with self.assertRaises(ValueError) as cm:
            await self.store.list_audio_segments(["invalid-uuid"])
        self.assertIn("Invalid feed_id UUID in list", str(cm.exception))

    async def test_get_audio_segment_histogram(self) -> None:
        start = datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC)
        end = datetime.datetime(2026, 1, 1, 1, tzinfo=datetime.UTC)
        self.pool.fetch.return_value = [
            {"bucket_index": 1, "count": 3, "is_alert": False},
            {"bucket_index": 3, "count": 1, "is_alert": True},
        ]

        result = await self.store.get_audio_segment_histogram(
            start_time=start,
            end_time=end,
            feed_ids=[str(_FEED_ID)],
            buckets=12,
            is_alert=None,
        )

        # 1h / 12 buckets = 5min width; bucket index 1 -> start, 3 -> +10min.
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0].bucket_start, start)
        self.assertEqual(result[0].count, 3)
        self.assertFalse(result[0].is_alert)
        self.assertEqual(
            result[1].bucket_start,
            start + datetime.timedelta(minutes=10),
        )
        self.assertTrue(result[1].is_alert)
        self.pool.fetch.assert_called_once_with(
            audio_segment_queries.AUDIO_SEGMENT_HISTOGRAM_SQL,
            [_FEED_ID],
            start,
            end,
            12,
            None,
        )

    async def test_get_audio_segment_histogram_invalid_buckets(self) -> None:
        with self.assertRaises(ValueError) as cm:
            await self.store.get_audio_segment_histogram(
                start_time=datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC),
                end_time=datetime.datetime(2026, 1, 1, 1, tzinfo=datetime.UTC),
                buckets=0,
            )
        self.assertIn("buckets must be >= 1", str(cm.exception))

    async def test_get_audio_segment_histogram_invalid_feed_id(self) -> None:
        with self.assertRaises(ValueError) as cm:
            await self.store.get_audio_segment_histogram(
                start_time=datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC),
                end_time=datetime.datetime(2026, 1, 1, 1, tzinfo=datetime.UTC),
                feed_ids=["invalid-uuid"],
            )
        self.assertIn("Invalid feed_id UUID in list", str(cm.exception))


if __name__ == "__main__":
    unittest.main()
