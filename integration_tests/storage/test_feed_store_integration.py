from __future__ import annotations

import uuid

import asyncpg
import pytest

from backend.pipeline.storage.feed_store import FeedStore


@pytest.fixture
async def store(db_pool: asyncpg.Pool) -> FeedStore:
    """Provides a FeedStore instance with a clean database."""
    await db_pool.execute("TRUNCATE feeds CASCADE")
    return FeedStore(db_pool)


# -- Helpers ----------------------------------------------------------


async def _insert_feed(
    pool: asyncpg.Pool,
    name: str,
    source_type: str = "bcfy_feeds",
    *,
    status: str = "unclaimed",
    failure_count: int = 0,
    worker_id: uuid.UUID | None = None,
    last_heartbeat_age_seconds: int | None = None,
    stream_url: str | None = None,
) -> uuid.UUID:
    """Insert a feed row and optionally an icecast properties row."""
    heartbeat_expr = "NULL"
    if last_heartbeat_age_seconds is not None:
        heartbeat_expr = f"NOW() - INTERVAL '{last_heartbeat_age_seconds} seconds'"

    feed_id = await pool.fetchval(
        f"INSERT INTO feeds (name, source_type, status, failure_count,"
        f" worker_id, last_heartbeat)"
        f" VALUES ($1, $2, $3::feed_status, $4, $5::uuid, {heartbeat_expr})"
        f" RETURNING id",
        name,
        source_type,
        status,
        failure_count,
        str(worker_id) if worker_id else None,
    )

    if stream_url is not None:
        await pool.execute(
            "INSERT INTO feed_properties_icecast (feed_id, stream_url) "
            "VALUES ($1::uuid, $2)",
            str(feed_id),
            stream_url,
        )

    return feed_id


async def _get_feed_status(pool: asyncpg.Pool, feed_id: uuid.UUID) -> dict:
    """Read a feed row back from the database."""
    row = await pool.fetchrow(
        "SELECT status, failure_count, worker_id, fencing_token FROM feeds WHERE id = $1::uuid",
        str(feed_id),
    )
    if row is None:
        msg = "Expected a row from query"
        raise AssertionError(msg)
    return dict(row)


# -- Tests: lease_feed ------------------------------------------------


async def test_lease_returns_feed_with_icecast_properties(
    db_pool: asyncpg.Pool, store: FeedStore
) -> None:
    """Leased feed includes stream_url from LEFT JOIN."""
    worker = uuid.uuid4()
    await _insert_feed(
        db_pool,
        "Icecast Feed",
        stream_url="http://stream.example.com/live",
    )

    result = await store.lease_feed(worker)

    assert result is not None
    assert result["name"] == "Icecast Feed"
    assert result["source_type"] == "bcfy_feeds"
    assert result["stream_url"] == "http://stream.example.com/live"
    assert result["fencing_token"] == 1


async def test_lease_returns_feed_without_icecast_properties(
    db_pool: asyncpg.Pool, store: FeedStore
) -> None:
    """Non-icecast feed has stream_url=None."""
    worker = uuid.uuid4()
    await _insert_feed(db_pool, "API Feed", source_type="bcfy_calls")

    result = await store.lease_feed(worker)

    assert result is not None
    assert result["name"] == "API Feed"
    assert result["stream_url"] is None
    assert result["fencing_token"] == 1


async def test_lease_returns_none_when_no_feeds(store: FeedStore) -> None:
    """Empty database returns None."""
    result = await store.lease_feed(uuid.uuid4())
    assert result is None


async def test_lease_prioritizes_unclaimed_over_failing(
    db_pool: asyncpg.Pool, store: FeedStore
) -> None:
    """Unclaimed feeds are leased before failing feeds."""
    worker = uuid.uuid4()
    await _insert_feed(
        db_pool,
        "Failing Feed",
        status="failing",
        failure_count=1,
        last_heartbeat_age_seconds=120,
    )
    await _insert_feed(db_pool, "Unclaimed Feed")

    result = await store.lease_feed(worker)

    assert result is not None
    assert result["name"] == "Unclaimed Feed"


async def test_lease_skips_active_feeds_with_fresh_heartbeat(
    db_pool: asyncpg.Pool, store: FeedStore
) -> None:
    """Active feed with a recent heartbeat is not available."""
    worker = uuid.uuid4()
    other_worker = uuid.uuid4()
    await _insert_feed(
        db_pool,
        "Active Feed",
        status="active",
        worker_id=other_worker,
        last_heartbeat_age_seconds=10,
    )

    result = await store.lease_feed(worker)

    assert result is None


async def test_lease_reclaims_stale_active_feed(
    db_pool: asyncpg.Pool, store: FeedStore
) -> None:
    """Active feed with heartbeat older than 60s becomes available."""
    worker = uuid.uuid4()
    old_worker = uuid.uuid4()
    await _insert_feed(
        db_pool,
        "Stale Feed",
        status="active",
        worker_id=old_worker,
        last_heartbeat_age_seconds=120,
    )

    result = await store.lease_feed(worker)

    assert result is not None
    assert result["name"] == "Stale Feed"


async def test_concurrent_leases_get_different_feeds(
    db_pool: asyncpg.Pool, store: FeedStore
) -> None:
    """Two sequential leases return different feeds (SKIP LOCKED)."""
    await _insert_feed(db_pool, "Feed A")
    await _insert_feed(db_pool, "Feed B")

    result_a = await store.lease_feed(uuid.uuid4())
    result_b = await store.lease_feed(uuid.uuid4())

    assert result_a is not None
    assert result_b is not None
    assert result_a["id"] != result_b["id"]


# -- Tests: update_feed_progress --------------------------------------


async def test_progress_update_succeeds_with_correct_worker(
    db_pool: asyncpg.Pool, store: FeedStore
) -> None:
    """Fenced update returns True and resets failure_count."""
    worker = uuid.uuid4()
    feed_id = await _insert_feed(
        db_pool,
        "My Feed",
        status="active",
        worker_id=worker,
        last_heartbeat_age_seconds=10,
        failure_count=1,
    )

    result = await store.update_feed_progress(
        feed_id,
        worker,
        "gs://bucket/path/file.ogg",
        0,
    )

    assert result is True
    row = await _get_feed_status(db_pool, feed_id)
    assert row["failure_count"] == 0


async def test_progress_update_fails_with_wrong_worker(
    db_pool: asyncpg.Pool, store: FeedStore
) -> None:
    """Wrong worker_id returns False (lease lost)."""
    worker = uuid.uuid4()
    feed_id = await _insert_feed(
        db_pool,
        "My Feed",
        status="active",
        worker_id=worker,
        last_heartbeat_age_seconds=10,
    )

    result = await store.update_feed_progress(
        feed_id,
        uuid.uuid4(),
        "gs://bucket/path/file.ogg",
        0,
    )

    assert result is False


# -- Tests: report_feed_failure ---------------------------------------


async def test_failure_sets_status_to_failing(
    db_pool: asyncpg.Pool, store: FeedStore
) -> None:
    """First failure transitions to 'failing' and releases the lease."""
    worker = uuid.uuid4()
    feed_id = await _insert_feed(
        db_pool,
        "My Feed",
        status="active",
        worker_id=worker,
        last_heartbeat_age_seconds=10,
    )

    await store.report_feed_failure(feed_id, worker, 0)

    row = await _get_feed_status(db_pool, feed_id)
    assert row["status"] == "failing"
    assert row["failure_count"] == 1
    assert row["worker_id"] is None


async def test_failure_escalation_to_quarantine(
    db_pool: asyncpg.Pool, store: FeedStore
) -> None:
    """Third failure transitions to 'quarantined'."""
    worker = uuid.uuid4()
    feed_id = await _insert_feed(
        db_pool,
        "My Feed",
        status="active",
        worker_id=worker,
        last_heartbeat_age_seconds=10,
        failure_count=2,
    )

    await store.report_feed_failure(feed_id, worker, 0)

    row = await _get_feed_status(db_pool, feed_id)
    assert row["status"] == "quarantined"
    assert row["failure_count"] == 3


# -- Tests: renew_heartbeats_batch_diagnostic --------------------------


async def test_diagnostic_renew_returns_all_owned_feeds(
    db_pool: asyncpg.Pool, store: FeedStore
) -> None:
    """Owned feeds are returned with renewed=True and correct diagnostics."""
    worker = uuid.uuid4()
    feed_a = await _insert_feed(
        db_pool,
        "Feed A",
        status="active",
        worker_id=worker,
        last_heartbeat_age_seconds=30,
    )
    feed_b = await _insert_feed(
        db_pool,
        "Feed B",
        status="active",
        worker_id=worker,
        last_heartbeat_age_seconds=30,
    )

    results = await store.renew_heartbeats_batch_diagnostic(
        [feed_a, feed_b],
        worker,
    )

    assert len(results) == 2
    by_id = {r["id"]: r for r in results}
    assert by_id[feed_a]["renewed"] is True
    assert by_id[feed_b]["renewed"] is True
    assert by_id[feed_a]["current_worker"] == worker
    assert by_id[feed_a]["current_status"] == "active"


async def test_diagnostic_renew_stolen_feed(
    db_pool: asyncpg.Pool, store: FeedStore
) -> None:
    """Stolen feed returns renewed=False with the thief's worker_id."""
    worker = uuid.uuid4()
    other_worker = uuid.uuid4()
    owned_feed = await _insert_feed(
        db_pool,
        "Owned Feed",
        status="active",
        worker_id=worker,
        last_heartbeat_age_seconds=30,
    )
    stolen_feed = await _insert_feed(
        db_pool,
        "Stolen Feed",
        status="active",
        worker_id=other_worker,
        last_heartbeat_age_seconds=30,
    )

    results = await store.renew_heartbeats_batch_diagnostic(
        [owned_feed, stolen_feed],
        worker,
    )

    by_id = {r["id"]: r for r in results}
    assert by_id[owned_feed]["renewed"] is True
    assert by_id[stolen_feed]["renewed"] is False
    assert by_id[stolen_feed]["current_worker"] == other_worker


async def test_diagnostic_renew_quarantined_feed(
    db_pool: asyncpg.Pool, store: FeedStore
) -> None:
    """Quarantined feed returns renewed=False with quarantined status."""
    worker = uuid.uuid4()
    feed_id = await _insert_feed(
        db_pool,
        "Quarantined Feed",
        status="quarantined",
        worker_id=None,
        failure_count=3,
    )

    results = await store.renew_heartbeats_batch_diagnostic(
        [feed_id],
        worker,
    )

    assert len(results) == 1
    assert results[0]["renewed"] is False
    assert results[0]["current_status"] == "quarantined"
    assert results[0]["current_worker"] is None


async def test_diagnostic_renew_empty_input(store: FeedStore) -> None:
    """Empty input returns empty list without hitting the database."""
    results = await store.renew_heartbeats_batch_diagnostic(
        [],
        uuid.uuid4(),
    )

    assert results == []


# -- Tests: release_feed ----------------------------------------------


async def test_release_feed_succeeds(db_pool: asyncpg.Pool, store: FeedStore) -> None:
    """Release returns True and resets the feed to unclaimed."""
    worker = uuid.uuid4()
    feed_id = await _insert_feed(
        db_pool,
        "My Feed",
        status="active",
        worker_id=worker,
        last_heartbeat_age_seconds=10,
    )

    result = await store.release_feed(feed_id, worker, 0)

    assert result is True
    row = await _get_feed_status(db_pool, feed_id)
    assert row["status"] == "unclaimed"
    assert row["worker_id"] is None


async def test_release_feed_fails_with_wrong_worker(
    db_pool: asyncpg.Pool, store: FeedStore
) -> None:
    """Release returns False when a different worker owns the feed."""
    worker = uuid.uuid4()
    feed_id = await _insert_feed(
        db_pool,
        "My Feed",
        status="active",
        worker_id=worker,
        last_heartbeat_age_seconds=10,
    )

    result = await store.release_feed(feed_id, uuid.uuid4(), 0)

    assert result is False


# -- Tests: fencing_token ------------------------------------------------


async def test_fencing_token_increments_on_each_lease(
    db_pool: asyncpg.Pool, store: FeedStore
) -> None:
    """Re-leasing a feed increments the fencing token."""
    worker = uuid.uuid4()
    await _insert_feed(db_pool, "My Feed")

    result1 = await store.lease_feed(worker)
    assert result1 is not None
    assert result1["fencing_token"] == 1

    await store.release_feed(result1["id"], worker, 1)

    result2 = await store.lease_feed(worker)
    assert result2 is not None
    assert result2["fencing_token"] == 2


async def test_progress_update_fails_with_wrong_fencing_token(
    db_pool: asyncpg.Pool, store: FeedStore
) -> None:
    """Correct worker_id but wrong fencing_token returns False."""
    worker = uuid.uuid4()
    feed_id = await _insert_feed(
        db_pool,
        "My Feed",
        status="active",
        worker_id=worker,
        last_heartbeat_age_seconds=10,
    )

    result = await store.update_feed_progress(
        feed_id,
        worker,
        "gs://bucket/path/file.ogg",
        999,  # wrong fencing_token
    )

    assert result is False


async def test_release_feed_fails_with_wrong_fencing_token(
    db_pool: asyncpg.Pool, store: FeedStore
) -> None:
    """Correct worker_id but wrong fencing_token returns False."""
    worker = uuid.uuid4()
    feed_id = await _insert_feed(
        db_pool,
        "My Feed",
        status="active",
        worker_id=worker,
        last_heartbeat_age_seconds=10,
    )

    result = await store.release_feed(feed_id, worker, 999)

    assert result is False


async def test_report_feed_failure_fails_with_wrong_fencing_token(
    db_pool: asyncpg.Pool, store: FeedStore
) -> None:
    """Correct worker_id but wrong fencing_token returns False."""
    worker = uuid.uuid4()
    feed_id = await _insert_feed(
        db_pool,
        "My Feed",
        status="active",
        worker_id=worker,
        last_heartbeat_age_seconds=10,
    )

    result = await store.report_feed_failure(feed_id, worker, 999)

    assert result is False
    # Verify feed state unchanged (failure was rejected)
    row = await _get_feed_status(db_pool, feed_id)
    assert row["status"] == "active"
    assert row["failure_count"] == 0
