from __future__ import annotations

import datetime
import uuid
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import asyncpg

import pytest

from backend.pipeline.storage.feed_store import FeedStore, SourceType


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
    source_feed_id: str | None = None,
    external_id: str = "ext_default",
) -> uuid.UUID:
    """Insert a feed row and its properties row."""
    heartbeat_expr = "NULL"
    if last_heartbeat_age_seconds is not None:
        heartbeat_expr = (
            f"NOW() - INTERVAL '{last_heartbeat_age_seconds} seconds'"
        )

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

    # Ensure unique source_feed_id if not provided
    if source_feed_id is None:
        source_feed_id = f"src_{uuid.uuid4().hex[:8]}"

    await pool.execute(
        "INSERT INTO feed_properties (feed_id, source_feed_id, external_id, source_type) "
        "VALUES ($1::uuid, $2, $3, $4)",
        str(feed_id),
        source_feed_id,
        external_id,
        source_type,
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


async def test_lease_returns_feed_with_feed_properties(
    db_pool: asyncpg.Pool, store: FeedStore
) -> None:
    """Leased feed includes source_feed_id from LEFT JOIN."""
    worker = uuid.uuid4()
    await _insert_feed(
        db_pool,
        "Icecast Feed",
        source_feed_id="123",
        external_id="ext-123",
    )

    result = await store.lease_feed(worker)

    assert result is not None
    assert result["name"] == "Icecast Feed"
    assert result["source_type"] == SourceType.BCFY_FEEDS
    assert result["source_feed_id"] == "123"
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


# -- Tests: acquire_feeds_batch (per-type CTE) -----------------------


async def test_primary_cte_respects_per_type_limits(
    db_pool: asyncpg.Pool, store: FeedStore
) -> None:
    """Each branch's LIMIT bounds the rows returned of that source_type."""
    for i in range(3):
        await _insert_feed(db_pool, f"bf-{i}", source_type="bcfy_feeds")
        await _insert_feed(db_pool, f"bc-{i}", source_type="bcfy_calls")
        await _insert_feed(db_pool, f"om-{i}", source_type="openmhz")

    worker = uuid.uuid4()
    result = await store.acquire_feeds_batch(
        worker, ramp_pct=100,
        limit_bcfy_feeds=2,
        limit_bcfy_calls=1,
        limit_openmhz=3,
    )

    counts: dict[SourceType, int] = {t: 0 for t in SourceType}
    for lease in result:
        counts[lease["source_type"]] += 1
    assert counts[SourceType.BCFY_FEEDS] == 2
    assert counts[SourceType.BCFY_CALLS] == 1
    assert counts[SourceType.OPENMHZ] == 3


async def test_primary_cte_limit_zero_skips_type(
    db_pool: asyncpg.Pool, store: FeedStore
) -> None:
    """A per-branch LIMIT of 0 returns zero rows of that source_type."""
    await _insert_feed(db_pool, "bf-0", source_type="bcfy_feeds")
    await _insert_feed(db_pool, "bf-1", source_type="bcfy_feeds")
    await _insert_feed(db_pool, "bc-0", source_type="bcfy_calls")

    worker = uuid.uuid4()
    result = await store.acquire_feeds_batch(
        worker, ramp_pct=100,
        limit_bcfy_feeds=0,
        limit_bcfy_calls=10,
        limit_openmhz=10,
    )

    assert all(lease["source_type"] != SourceType.BCFY_FEEDS for lease in result)
    assert any(lease["source_type"] == SourceType.BCFY_CALLS for lease in result)


async def test_primary_cte_ramp_zero_returns_empty(
    db_pool: asyncpg.Pool, store: FeedStore
) -> None:
    """ramp_pct=0 disables every bucket — no rows regardless of LIMITs."""
    await _insert_feed(db_pool, "bf-0", source_type="bcfy_feeds")
    await _insert_feed(db_pool, "bc-0", source_type="bcfy_calls")

    worker = uuid.uuid4()
    result = await store.acquire_feeds_batch(
        worker, ramp_pct=0,
        limit_bcfy_feeds=10,
        limit_bcfy_calls=10,
        limit_openmhz=10,
    )

    assert result == []


async def test_primary_cte_sets_status_to_active(
    db_pool: asyncpg.Pool, store: FeedStore
) -> None:
    """The outer UPDATE transitions unclaimed → active, bumps fencing_token."""
    feed_id = await _insert_feed(db_pool, "bf-0", source_type="bcfy_feeds")
    worker = uuid.uuid4()

    result = await store.acquire_feeds_batch(
        worker, ramp_pct=100,
        limit_bcfy_feeds=10,
        limit_bcfy_calls=10,
        limit_openmhz=10,
    )

    assert len(result) == 1
    assert result[0]["id"] == feed_id
    row = await _get_feed_status(db_pool, feed_id)
    assert row["status"] == "active"
    assert row["worker_id"] == worker
    assert row["fencing_token"] >= 1


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
        None,
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
        None,
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
    """Fifth failure transitions to 'quarantined'."""
    worker = uuid.uuid4()
    feed_id = await _insert_feed(
        db_pool,
        "My Feed",
        status="active",
        worker_id=worker,
        last_heartbeat_age_seconds=10,
        failure_count=4,
    )

    await store.report_feed_failure(feed_id, worker, 0)

    row = await _get_feed_status(db_pool, feed_id)
    assert row["status"] == "quarantined"
    assert row["failure_count"] == 5


async def test_failing_feed_not_leased_before_retry_after(
    db_pool: asyncpg.Pool, store: FeedStore
) -> None:
    """Feed with retry_after in the future is not returned by acquire."""
    worker = uuid.uuid4()
    feed_id = await _insert_feed(
        db_pool,
        "Backoff Feed",
        status="active",
        worker_id=worker,
        last_heartbeat_age_seconds=10,
    )
    await store.report_feed_failure(feed_id, worker, 0)
    result = await store.lease_feed(uuid.uuid4())
    assert result is None


async def test_failing_feed_leased_after_retry_after_expires(
    db_pool: asyncpg.Pool, store: FeedStore
) -> None:
    """Feed is returned by acquire after retry_after passes."""
    worker = uuid.uuid4()
    feed_id = await _insert_feed(
        db_pool,
        "Backoff Feed",
        status="active",
        worker_id=worker,
        last_heartbeat_age_seconds=10,
    )
    await store.report_feed_failure(feed_id, worker, 0)
    await db_pool.execute(
        "UPDATE feeds SET retry_after = NOW() - INTERVAL '1 second'"
        " WHERE id = $1",
        feed_id,
    )
    result = await store.lease_feed(uuid.uuid4())
    assert result is not None
    assert result["id"] == feed_id


async def test_lease_preserves_failure_count(
    db_pool: asyncpg.Pool, store: FeedStore
) -> None:
    """Leasing clears retry_after but does NOT reset failure_count."""
    worker = uuid.uuid4()
    feed_id = await _insert_feed(
        db_pool,
        "Preserve Feed",
        status="active",
        worker_id=worker,
        last_heartbeat_age_seconds=10,
    )
    await store.report_feed_failure(feed_id, worker, 0)
    await db_pool.execute(
        "UPDATE feeds SET retry_after = NOW() - INTERVAL '1 second'"
        " WHERE id = $1",
        feed_id,
    )
    new_worker = uuid.uuid4()
    result = await store.lease_feed(new_worker)
    assert result is not None
    row = await db_pool.fetchrow(
        "SELECT failure_count, retry_after FROM feeds WHERE id = $1",
        feed_id,
    )
    assert row["failure_count"] == 1
    assert row["retry_after"] is None


async def test_successful_processing_resets_failure_count(
    db_pool: asyncpg.Pool, store: FeedStore
) -> None:
    """update_feed_progress resets failure_count to 0."""
    worker = uuid.uuid4()
    feed_id = await _insert_feed(
        db_pool,
        "Progress Feed",
        status="active",
        worker_id=worker,
        last_heartbeat_age_seconds=10,
    )
    await store.report_feed_failure(feed_id, worker, 0)
    await db_pool.execute(
        "UPDATE feeds SET retry_after = NOW() - INTERVAL '1 second'"
        " WHERE id = $1",
        feed_id,
    )
    new_worker = uuid.uuid4()
    result = await store.lease_feed(new_worker)
    assert result is not None
    await store.update_feed_progress(
        result["id"],
        new_worker,
        "chunk_001.flac",
        result["fencing_token"],
        None,
    )
    row = await db_pool.fetchrow(
        "SELECT failure_count FROM feeds WHERE id = $1",
        feed_id,
    )
    assert row["failure_count"] == 0


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


async def test_release_feed_succeeds(
    db_pool: asyncpg.Pool, store: FeedStore
) -> None:
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
        None,
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

    assert result is None
    # Verify feed state unchanged (failure was rejected)
    row = await _get_feed_status(db_pool, feed_id)
    assert row["status"] == "active"
    assert row["failure_count"] == 0


# -- Tests: last_bookmark_time ------------------------------------------------


async def test_last_bookmark_time_round_trips_through_lease(
    db_pool: asyncpg.Pool, store: FeedStore
) -> None:
    """Bookmark set via update_feed_progress survives release and re-lease."""
    worker = uuid.uuid4()
    await _insert_feed(db_pool, "Bookmark Feed")

    # Lease the feed.
    result1 = await store.lease_feed(worker)
    assert result1 is not None
    assert result1["last_bookmark_time"] is None

    # Record a bookmark.
    bookmark = datetime.datetime(2026, 3, 30, 12, 0, 0, tzinfo=datetime.UTC)
    ok = await store.update_feed_progress(
        result1["id"],
        worker,
        "chunk_001.flac",
        result1["fencing_token"],
        bookmark,
    )
    assert ok is True

    # Release and re-lease.
    await store.release_feed(result1["id"], worker, result1["fencing_token"])
    result2 = await store.lease_feed(uuid.uuid4())

    assert result2 is not None
    assert result2["id"] == result1["id"]
    assert result2["last_bookmark_time"] == bookmark


# -- Tests: create_feed ------------------------------------------------


async def test_create_feed_succeeds(
    db_pool: asyncpg.Pool, store: FeedStore
) -> None:
    """create_feed atomically creates a feed and its properties."""
    feed = await store.create_feed(
        name="New Integration Feed",
        source_type="bcfy_feeds",
        source_feed_id="src_123",
        external_id="ext_123",
    )

    assert feed is not None
    assert feed["name"] == "New Integration Feed"
    assert feed["source_type"] == SourceType.BCFY_FEEDS
    assert feed["source_feed_id"] == "src_123"
    assert feed["external_id"] == "ext_123"

    # Verify in DB
    row = await db_pool.fetchrow(
        "SELECT f.name, fp.source_feed_id, fp.external_id "
        "FROM feeds f "
        "JOIN feed_properties fp ON f.id = fp.feed_id "
        "WHERE f.id = $1",
        feed["id"],
    )
    assert row is not None
    assert row["name"] == "New Integration Feed"
    assert row["source_feed_id"] == "src_123"
    assert row["external_id"] == "ext_123"


# -- Tests: get_feed --------------------------------------------------


async def test_get_feed_returns_feed(
    db_pool: asyncpg.Pool, store: FeedStore
) -> None:
    """get_feed retrieves a specific feed by ID."""
    feed_id = await _insert_feed(
        db_pool,
        "Get Feed Test",
        source_feed_id="src_get",
        external_id="ext_get",
    )

    feed = await store.get_feed(feed_id)

    assert feed is not None
    assert feed["id"] == feed_id
    assert feed["name"] == "Get Feed Test"
    assert feed["source_feed_id"] == "src_get"
    assert feed["external_id"] == "ext_get"


async def test_get_feed_returns_none_if_not_found(store: FeedStore) -> None:
    """get_feed returns None for non-existent ID."""
    feed = await store.get_feed(uuid.uuid4())
    assert feed is None


# -- Tests: list_feeds ------------------------------------------------


async def test_list_feeds_returns_all_feeds(
    db_pool: asyncpg.Pool, store: FeedStore
) -> None:
    """list_feeds retrieves all feeds ordered by created_at DESC."""
    feed_id_a = await _insert_feed(
        db_pool, "Feed A", source_feed_id="src_a", external_id="ext_a"
    )
    feed_id_b = await _insert_feed(
        db_pool, "Feed B", source_feed_id="src_b", external_id="ext_b"
    )

    feeds = await store.list_feeds()

    assert len(feeds) >= 2
    # The most recently created should be first
    assert feeds[0]["id"] == feed_id_b
    assert feeds[0]["source_feed_id"] == "src_b"
    assert feeds[0]["external_id"] == "ext_b"

    assert feeds[1]["id"] == feed_id_a
    assert feeds[1]["source_feed_id"] == "src_a"
    assert feeds[1]["external_id"] == "ext_a"


# -- Tests: delete_feed ------------------------------------------------


async def test_delete_feed_succeeds(
    db_pool: asyncpg.Pool, store: FeedStore
) -> None:
    """delete_feed deletes the feed and returns True."""
    feed_id = await _insert_feed(db_pool, "Delete Test Feed")

    result = await store.delete_feed(feed_id)

    assert result is True
    # Verify deleted
    row = await db_pool.fetchrow("SELECT 1 FROM feeds WHERE id = $1", feed_id)
    assert row is None


async def test_delete_feed_returns_false_if_not_found(store: FeedStore) -> None:
    """delete_feed returns False for non-existent ID."""
    result = await store.delete_feed(uuid.uuid4())
    assert result is False


# -- Tests: reset_feed ------------------------------------------------


async def test_reset_feed_succeeds(
    db_pool: asyncpg.Pool, store: FeedStore
) -> None:
    """reset_feed sets status to unclaimed, failure_count to 0, and clears worker_id."""
    worker = uuid.uuid4()
    feed_id = await _insert_feed(
        db_pool,
        "Quarantined Feed",
        status="quarantined",
        failure_count=5,
        worker_id=worker,
        last_heartbeat_age_seconds=1000,
    )

    feed = await store.reset_feed(feed_id)

    assert feed is not None
    assert feed["id"] == feed_id
    assert feed["status"] == "unclaimed"
    assert feed["failure_count"] == 0
    assert feed["worker_id"] is None

    # Verify DB state
    row = await _get_feed_status(db_pool, feed_id)
    assert row["status"] == "unclaimed"
    assert row["failure_count"] == 0
    assert row["worker_id"] is None


async def test_reset_feed_returns_none_if_not_found(store: FeedStore) -> None:
    """reset_feed returns None for non-existent ID."""
    result = await store.reset_feed(uuid.uuid4())
    assert result is None
