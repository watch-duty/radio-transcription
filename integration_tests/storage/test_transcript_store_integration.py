from __future__ import annotations

import datetime
import uuid
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import asyncpg

import pytest

from backend.pipeline.storage.transcript_store import TranscriptStore
from integration_tests.storage.storage_feed_util import create_feed


@pytest.fixture
async def store(db_pool: asyncpg.Pool) -> TranscriptStore:
    """Provides a TranscriptStore instance with a clean database."""
    await db_pool.execute("TRUNCATE feeds CASCADE")
    return TranscriptStore(db_pool)


async def _insert_transcript(
    pool: asyncpg.Pool,
    segment_id: uuid.UUID,
    feed_id: uuid.UUID,
    end_timestamp: datetime.datetime,
    transcript: str = "Test transcript",
) -> None:
    start_timestamp = end_timestamp - datetime.timedelta(seconds=10)
    await pool.execute(
        """
        INSERT INTO transcripts (segment_id, feed_id, transcript, start_timestamp, end_timestamp, created_at)
        VALUES ($1, $2, $3, $4, $5, NOW())
        """,
        str(segment_id),
        str(feed_id),
        transcript,
        start_timestamp,
        end_timestamp,
    )


async def test_list_transcripts_pagination(
    db_pool: asyncpg.Pool, store: TranscriptStore
) -> None:
    feed_id = await create_feed(db_pool)

    # Insert 3 transcripts with different timestamps
    t1 = datetime.datetime(2026, 1, 1, 10, 0, 0, tzinfo=datetime.UTC)
    t2 = datetime.datetime(2026, 1, 1, 10, 1, 0, tzinfo=datetime.UTC)
    t3 = datetime.datetime(2026, 1, 1, 10, 2, 0, tzinfo=datetime.UTC)

    await _insert_transcript(db_pool, uuid.uuid4(), feed_id, t1)
    await _insert_transcript(db_pool, uuid.uuid4(), feed_id, t2)
    await _insert_transcript(db_pool, uuid.uuid4(), feed_id, t3)

    # Page 1: Limit 2
    result = await store.list_transcripts_by_feed_id(str(feed_id), limit=2)
    assert len(result.transcripts) == 2
    assert result.next_token is not None

    # Results should be ordered by end_timestamp DESC
    assert result.transcripts[0].end_timestamp.ToDatetime() == t3.replace(
        tzinfo=None
    )
    assert result.transcripts[1].end_timestamp.ToDatetime() == t2.replace(
        tzinfo=None
    )

    # Page 2: Use next_token
    result2 = await store.list_transcripts_by_feed_id(
        str(feed_id), limit=2, next_token=result.next_token
    )
    assert len(result2.transcripts) == 1
    assert result2.transcripts[0].end_timestamp.ToDatetime() == t1.replace(
        tzinfo=None
    )
    assert result2.next_token is None


async def test_list_transcripts_ascending(
    db_pool: asyncpg.Pool, store: TranscriptStore
) -> None:
    feed_id = await create_feed(db_pool)

    t1 = datetime.datetime(2026, 1, 1, 10, 0, 0, tzinfo=datetime.UTC)
    t2 = datetime.datetime(2026, 1, 1, 10, 1, 0, tzinfo=datetime.UTC)
    t3 = datetime.datetime(2026, 1, 1, 10, 2, 0, tzinfo=datetime.UTC)

    await _insert_transcript(db_pool, uuid.uuid4(), feed_id, t1)
    await _insert_transcript(db_pool, uuid.uuid4(), feed_id, t2)
    await _insert_transcript(db_pool, uuid.uuid4(), feed_id, t3)

    result = await store.list_transcripts_by_feed_id(
        str(feed_id), limit=2, order="asc"
    )
    assert len(result.transcripts) == 2
    assert result.next_token is not None

    assert result.transcripts[0].end_timestamp.ToDatetime() == t1.replace(
        tzinfo=None
    )
    assert result.transcripts[1].end_timestamp.ToDatetime() == t2.replace(
        tzinfo=None
    )

    result2 = await store.list_transcripts_by_feed_id(
        str(feed_id), limit=2, next_token=result.next_token, order="asc"
    )
    assert len(result2.transcripts) == 1
    assert result2.transcripts[0].end_timestamp.ToDatetime() == t3.replace(
        tzinfo=None
    )
    assert result2.next_token is None


async def test_list_transcripts_time_window(
    db_pool: asyncpg.Pool, store: TranscriptStore
) -> None:
    feed_id = await create_feed(db_pool)

    t1 = datetime.datetime(2026, 1, 1, 10, 0, 0, tzinfo=datetime.UTC)
    t2 = datetime.datetime(2026, 1, 2, 10, 0, 0, tzinfo=datetime.UTC)
    t3 = datetime.datetime(2026, 1, 3, 10, 0, 0, tzinfo=datetime.UTC)

    await _insert_transcript(db_pool, uuid.uuid4(), feed_id, t1)
    await _insert_transcript(db_pool, uuid.uuid4(), feed_id, t2)
    await _insert_transcript(db_pool, uuid.uuid4(), feed_id, t3)

    # Filter by time window
    start = datetime.datetime(2026, 1, 1, 12, 0, 0, tzinfo=datetime.UTC)
    end = datetime.datetime(2026, 1, 2, 12, 0, 0, tzinfo=datetime.UTC)

    result = await store.list_transcripts_by_feed_id(
        str(feed_id), start_time=start, end_time=end
    )

    assert len(result.transcripts) == 1
    assert result.transcripts[0].end_timestamp.ToDatetime() == t2.replace(
        tzinfo=None
    )
