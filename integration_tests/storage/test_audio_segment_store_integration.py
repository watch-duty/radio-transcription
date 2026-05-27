from __future__ import annotations

import datetime
import uuid
from typing import TYPE_CHECKING

import pytest

from backend.pipeline.storage.audio_segment_store import AudioSegmentStore
from backend.services.audio_segments.models import (
    AnnotationType,
    AudioClassification,
    AudioSegmentCreate,
    TranscriptAnnotation,
)

if TYPE_CHECKING:
    import asyncpg


@pytest.fixture
async def store(db_pool: asyncpg.Pool) -> AudioSegmentStore:
    """Provides an AudioSegmentStore instance with a clean database."""
    await db_pool.execute("TRUNCATE feeds CASCADE")
    return AudioSegmentStore(db_pool)


async def _insert_test_feed(pool: asyncpg.Pool, name: str) -> uuid.UUID:
    feed_id = await pool.fetchval(
        "INSERT INTO feeds (name, source_type, status) "
        "VALUES ($1, 'bcfy_feeds', 'active'::feed_status) "
        "RETURNING id",
        name,
    )
    await pool.execute(
        "INSERT INTO feed_properties (feed_id, source_feed_id, external_id, source_type) "
        "VALUES ($1::uuid, $2, $3, 'bcfy_feeds')",
        str(feed_id),
        f"src-{feed_id}",
        f"ext-{feed_id}",
    )
    return feed_id


@pytest.mark.asyncio
async def test_audio_segment_store_lifecycle_integration(
    db_pool: asyncpg.Pool, store: AudioSegmentStore
) -> None:
    feed_id = await _insert_test_feed(db_pool, "test-feed")

    segment_id = uuid.uuid4()
    segment = AudioSegmentCreate(
        id=str(segment_id),
        feed_id=str(feed_id),
        classification=AudioClassification.SPEECH_DETECTED,
        start_timestamp=datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC),
        end_timestamp=datetime.datetime(2026, 1, 1, 0, 1, tzinfo=datetime.UTC),
        missing_prior_context=False,
        missing_post_context=False,
        source_audio_uris=["gs://bucket/audio1.ogg"],
        canonical_audio_uri="gs://bucket/canonical.ogg",
        start_audio_offset=datetime.timedelta(seconds=5),
        end_audio_offset=datetime.timedelta(seconds=10),
        playback_audio_uri=None,
    )

    # 1. Idempotently Add a batch with that segment
    inserted = await store.bulk_add_audio_segments([segment])
    assert inserted == 1

    # 2. Add an annotation
    annotation = await store.add_annotation(
        str(segment_id),
        AnnotationType.TRANSCRIPT,
        {"text": "integration test transcript", "errors": []},
    )
    assert annotation.audio_segment_id == str(segment_id)
    assert annotation.type == "TRANSCRIPT"
    assert isinstance(annotation, TranscriptAnnotation)
    assert annotation.data.text == "integration test transcript"

    # 3. List segments and verify it returned correctly
    list_res = await store.list_audio_segments()
    assert len(list_res) >= 1
    found = [s for s in list_res if s.id == str(segment_id)]
    assert len(found) == 1
    assert found[0].feed_id == str(feed_id)
    assert found[0].classification == AudioClassification.SPEECH_DETECTED
    assert len(found[0].annotations) == 1
    ann = found[0].annotations[0]
    assert ann.type == AnnotationType.TRANSCRIPT
    assert isinstance(ann, TranscriptAnnotation)
    assert ann.data.text == "integration test transcript"

    # 4. Filter list by feed_id
    list_feed = await store.list_audio_segments([str(feed_id)])
    assert len(list_feed) == 1

    # 5. Idempotent POST: post duplicate segment checks that no error is thrown
    inserted_dup = await store.bulk_add_audio_segments([segment])
    assert inserted_dup == 1
