from __future__ import annotations

import datetime
import unittest
import uuid
from unittest import mock

from backend.pipeline.storage.audio_segment_store import AudioSegmentStore
from backend.services.audio_segments.models import (
    AnnotationType,
)

_SEGMENT_ID = uuid.UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
_FEED_ID = uuid.UUID("bbbbbbbb-cccc-dddd-eeee-ffffffffffff")

_ANNOTATION_ROW = {
    "audio_segment_id": _SEGMENT_ID,
    "type": "TRANSCRIPT",
    "data": '{"text": "hello"}',
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
    "annotations": '[{"audio_segment_id": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee", "type": "TRANSCRIPT", "data": {"text": "hello"}, "created_at": "2026-01-01T00:00:00Z", "updated_at": "2026-01-01T00:00:00Z"}]',
}


def _make_mock_pool(
    *,
    fetchrow_result: dict | None = None,
    execute_result: str = "UPDATE 0",
    fetch_result: list | None = None,
) -> mock.AsyncMock:
    """Create a mock asyncpg.Pool with the given return values."""
    pool = mock.AsyncMock()
    pool.fetchrow.return_value = fetchrow_result
    pool.execute.return_value = execute_result
    pool.fetch.return_value = fetch_result or []
    return pool


class TestAudioSegmentStore(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.pool = _make_mock_pool(
            fetchrow_result=_ANNOTATION_ROW, fetch_result=[_AUDIO_SEGMENT_ROW]
        )
        self.store = AudioSegmentStore(self.pool)

    async def test_add_annotation_success(self) -> None:
        result = await self.store.add_annotation(
            str(_SEGMENT_ID), AnnotationType.TRANSCRIPT, {"text": "hello"}
        )

        self.assertEqual(result.audio_segment_id, str(_SEGMENT_ID))
        self.assertEqual(result.type, "TRANSCRIPT")
        self.assertEqual(result.data, {"text": "hello"})

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
        self.assertEqual(len(result[0].annotations), 1)
        self.assertEqual(result[0].annotations[0].type, "TRANSCRIPT")


if __name__ == "__main__":
    unittest.main()
