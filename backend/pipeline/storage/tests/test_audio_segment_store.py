from __future__ import annotations

import datetime
import unittest
import uuid

from backend.pipeline.storage import audio_segment_queries
from backend.pipeline.storage.audio_segment_store import AudioSegmentStore
from backend.pipeline.storage.tests.connection_util import make_mock_pool
from backend.services.audio_segments.models import (
    AnnotationType,
    AudioClassification,
    AudioSegmentCreate,
)

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
    "classification": "SPEECH_DETECTED",
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

    async def test_list_audio_segments(self) -> None:
        result = await self.store.list_audio_segments()

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].id, str(_SEGMENT_ID))
        self.pool.fetch.assert_called_once_with(
            audio_segment_queries.LIST_AUDIO_SEGMENTS_SQL, None
        )

    async def test_list_audio_segments_with_feed_id(self) -> None:
        result = await self.store.list_audio_segments([str(_FEED_ID)])

        self.assertEqual(len(result), 1)
        self.pool.fetch.assert_called_once_with(
            audio_segment_queries.LIST_AUDIO_SEGMENTS_SQL, [_FEED_ID]
        )

    async def test_list_audio_segments_invalid_feed_id(self) -> None:
        with self.assertRaises(ValueError) as cm:
            await self.store.list_audio_segments(["invalid-uuid"])
        self.assertIn("Invalid feed_id UUID in list", str(cm.exception))

    async def test_bulk_add_audio_segments_success(self) -> None:
        segment1 = AudioSegmentCreate(
            id=str(_SEGMENT_ID),
            feed_id=str(_FEED_ID),
            classification=AudioClassification.SPEECH_DETECTED,
            start_timestamp=datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC),
            end_timestamp=datetime.datetime(
                2026, 1, 1, 0, 1, tzinfo=datetime.UTC
            ),
            missing_prior_context=False,
            missing_post_context=False,
            source_audio_uris=["gs://bucket/audio1.ogg"],
            canonical_audio_uri="gs://bucket/canonical.ogg",
            start_audio_offset=datetime.timedelta(seconds=5),
            end_audio_offset=datetime.timedelta(seconds=10),
            playback_audio_uri=None,
        )

        # Simulate the row being returned (i.e., the insert succeeded)
        conn = self.pool.acquire.return_value.__aenter__.return_value
        conn.fetchrow.return_value = {"id": _SEGMENT_ID}

        result = await self.store.bulk_add_audio_segments([segment1])

        self.assertEqual(result, 1)
        conn.fetchrow.assert_called_once()
        call_args = conn.fetchrow.call_args
        self.assertEqual(
            call_args[0][0], audio_segment_queries.BULK_ADD_AUDIO_SEGMENTS_SQL
        )
        self.assertEqual(call_args[0][1], _SEGMENT_ID)
        self.assertEqual(call_args[0][2], _FEED_ID)
        self.assertEqual(call_args[0][3], "SPEECH_DETECTED")

    async def test_bulk_add_audio_segments_duplicate_returns_zero(
        self,
    ) -> None:
        segment1 = AudioSegmentCreate(
            id=str(_SEGMENT_ID),
            feed_id=str(_FEED_ID),
            classification=AudioClassification.SPEECH_DETECTED,
            start_timestamp=datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC),
            end_timestamp=datetime.datetime(
                2026, 1, 1, 0, 1, tzinfo=datetime.UTC
            ),
            missing_prior_context=False,
            missing_post_context=False,
            source_audio_uris=[],
            canonical_audio_uri=None,
            start_audio_offset=None,
            end_audio_offset=None,
            playback_audio_uri=None,
        )

        # Simulate ON CONFLICT DO NOTHING: fetchrow returns None
        conn = self.pool.acquire.return_value.__aenter__.return_value
        conn.fetchrow.return_value = None

        result = await self.store.bulk_add_audio_segments([segment1])

        self.assertEqual(result, 0)


if __name__ == "__main__":
    unittest.main()
