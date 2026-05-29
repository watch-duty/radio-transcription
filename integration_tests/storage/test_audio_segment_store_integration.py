from __future__ import annotations

import datetime
import uuid
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import asyncpg

import pytest

from backend.pipeline.storage.audio_segment_store import AudioSegmentStore
from backend.services.audio_segments.models import (
    AnnotationType,
    AudioClassification,
)
from integration_tests.storage.storage_feed_util import create_feed


@pytest.fixture
async def store(db_pool: asyncpg.Pool) -> AudioSegmentStore:
    """Provides an AudioSegmentStore instance with a clean database."""
    await db_pool.execute("TRUNCATE feeds CASCADE")
    return AudioSegmentStore(db_pool)


async def test_create_and_list_audio_segments(
    db_pool: asyncpg.Pool, store: AudioSegmentStore
) -> None:
    feed_id = await create_feed(db_pool)

    start_time = datetime.datetime(2026, 1, 1, 10, 0, 0, tzinfo=datetime.UTC)
    end_time = datetime.datetime(2026, 1, 1, 10, 1, 0, tzinfo=datetime.UTC)

    # 1. Create
    segment = await store.create_audio_segment(
        feed_id=str(feed_id),
        classification=AudioClassification.SPEECH_DETECTED,
        start_timestamp=start_time,
        end_timestamp=end_time,
        source_audio_uris=["gs://bucket/audio1.ogg"],
        missing_prior_context=False,
        missing_post_context=False,
    )

    assert segment.id is not None
    assert segment.feed_id == str(feed_id)
    assert segment.classification == AudioClassification.SPEECH_DETECTED
    assert segment.start_timestamp == start_time
    assert segment.end_timestamp == end_time
    assert segment.source_audio_uris == ["gs://bucket/audio1.ogg"]

    # 2. List
    segments = await store.list_audio_segments()
    assert len(segments) == 1
    assert segments[0].id == segment.id

    # 3. List with feed filter
    segments_filtered = await store.list_audio_segments(feed_ids=[str(feed_id)])
    assert len(segments_filtered) == 1
    assert segments_filtered[0].id == segment.id

    # List with non-existent feed
    segments_empty = await store.list_audio_segments(
        feed_ids=[str(uuid.uuid4())]
    )
    assert len(segments_empty) == 0


async def test_add_annotation(
    db_pool: asyncpg.Pool, store: AudioSegmentStore
) -> None:
    feed_id = await create_feed(db_pool)

    start_time = datetime.datetime(2026, 1, 1, 10, 0, 0, tzinfo=datetime.UTC)
    end_time = datetime.datetime(2026, 1, 1, 10, 1, 0, tzinfo=datetime.UTC)

    segment = await store.create_audio_segment(
        feed_id=str(feed_id),
        classification=AudioClassification.SPEECH_DETECTED,
        start_timestamp=start_time,
        end_timestamp=end_time,
        source_audio_uris=["gs://bucket/audio1.ogg"],
        missing_prior_context=False,
        missing_post_context=False,
    )

    # Add annotation
    annotation_data = {"text": "hello world", "errors": []}
    annotation = await store.add_annotation(
        segment_id=segment.id,
        annotation_type=AnnotationType.TRANSCRIPT,
        data=annotation_data,
    )

    assert annotation.audio_segment_id == segment.id
    assert annotation.type == AnnotationType.TRANSCRIPT
    assert annotation.data.model_dump() == annotation_data

    # Verify annotation is included when listing segments
    segments = await store.list_audio_segments()
    assert len(segments) == 1
    assert len(segments[0].annotations) == 1
    assert segments[0].annotations[0].type == AnnotationType.TRANSCRIPT
    assert segments[0].annotations[0].data.model_dump() == annotation_data
