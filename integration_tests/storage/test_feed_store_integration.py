from __future__ import annotations

import asyncio
import datetime
import json
import uuid
from typing import cast

import asyncpg
import pytest

from backend.pipeline.common.exceptions import (
    FeedNameAlreadyExistsError,
    FeedStateConflictError,
)
from backend.pipeline.storage.feed_store import (
    FeedStatus,
    FeedStatusReason,
    FeedStore,
    SourceType,
)

_TEST_ACTOR_ID = "service_account:gcp:123456789012345678901"
_INVALID_ACTOR_ID = "system actor"


@pytest.fixture
async def store(db_pool: asyncpg.Pool) -> FeedStore:
    """Provides a FeedStore instance with a clean database."""
    await db_pool.execute("TRUNCATE feed_audit_events, feeds CASCADE")
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
        "INSERT INTO feed_properties (feed_id, source_feed_id, source_type) "
        "VALUES ($1::uuid, $2, $3)",
        str(feed_id),
        source_feed_id,
        source_type,
    )

    return feed_id


async def _get_feed_status(pool: asyncpg.Pool, feed_id: uuid.UUID) -> dict:
    """Read a feed row back from the database."""
    row = await _get_feed_diagnostics(pool, feed_id)
    return {
        key: row[key]
        for key in ("status", "failure_count", "worker_id", "fencing_token")
    }


async def _get_feed_diagnostics(
    pool: asyncpg.Pool,
    feed_id: uuid.UUID,
) -> dict:
    """Read lifecycle and diagnostic feed fields from the database."""
    row = await pool.fetchrow(
        "SELECT status, failure_count, worker_id, fencing_token,"
        " retry_after, status_reason, status_reason_updated_at,"
        " status_reason_detail,"
        " last_processed_filename, last_bookmark_time"
        " FROM feeds WHERE id = $1::uuid",
        str(feed_id),
    )
    if row is None:
        msg = "Expected a row from query"
        raise AssertionError(msg)
    return dict(row)


async def _fetch_audit_events(
    pool: asyncpg.Pool,
    feed_id: uuid.UUID,
) -> list[asyncpg.Record]:
    """Return audit rows for one feed in deterministic timeline order."""
    return await pool.fetch(
        "SELECT id, action, actor_id, feed_revision, occurred_at,"
        " before_values, after_values"
        " FROM feed_audit_events"
        " WHERE feed_id = $1"
        " ORDER BY feed_revision, occurred_at, id",
        feed_id,
    )


async def _report_feed_failure_for_test(
    store: FeedStore,
    pool: asyncpg.Pool,
    feed_id: uuid.UUID,
    worker_id: uuid.UUID,
    fencing_token: int = 0,
    *,
    reason: str | None = None,
    status_reason: FeedStatusReason | None = None,
) -> str | None:
    """Call report_feed_failure with DB-derived audit state."""
    return await store.report_feed_failure(
        feed_id,
        worker_id,
        fencing_token,
        actor_id=_TEST_ACTOR_ID,
        reason=reason,
        status_reason=status_reason,
    )


async def _get_feed_audit_revision(
    pool: asyncpg.Pool,
    feed_id: uuid.UUID,
) -> int | None:
    """Return the feed-local audit revision stored on the current feed row."""
    return await pool.fetchval(
        "SELECT audit_revision FROM feeds WHERE id = $1",
        feed_id,
    )


def _decode_json_object(value: object) -> dict[str, object]:
    """Decode asyncpg JSONB results across default codec variants."""
    if isinstance(value, str):
        decoded = json.loads(value)
    else:
        decoded = value
    if not isinstance(decoded, dict):
        msg = f"Expected JSON object, got {type(decoded).__name__}"
        raise TypeError(msg)
    return cast("dict[str, object]", decoded)


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
        worker,
        limits={
            SourceType.BCFY_FEEDS: 2,
            SourceType.BCFY_CALLS: 1,
            SourceType.OPENMHZ: 3,
        },
    )

    counts: dict[SourceType, int] = dict.fromkeys(SourceType, 0)
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
        worker,
        limits={
            SourceType.BCFY_FEEDS: 0,
            SourceType.BCFY_CALLS: 10,
            SourceType.OPENMHZ: 10,
        },
    )

    assert all(
        lease["source_type"] != SourceType.BCFY_FEEDS for lease in result
    )
    assert any(
        lease["source_type"] == SourceType.BCFY_CALLS for lease in result
    )


async def test_primary_cte_sets_status_to_active(
    db_pool: asyncpg.Pool, store: FeedStore
) -> None:
    """The outer UPDATE transitions unclaimed → active, bumps fencing_token."""
    feed_id = await _insert_feed(db_pool, "bf-0", source_type="bcfy_feeds")
    worker = uuid.uuid4()

    result = await store.acquire_feeds_batch(
        worker,
        limits={
            SourceType.BCFY_FEEDS: 10,
            SourceType.BCFY_CALLS: 10,
            SourceType.OPENMHZ: 10,
        },
    )

    assert len(result) == 1
    assert result[0]["id"] == feed_id
    row = await _get_feed_status(db_pool, feed_id)
    assert row["status"] == "active"
    assert row["worker_id"] == worker
    assert row["fencing_token"] >= 1


# -- Tests: acquire_feeds_recovery source_type filter ---------------


async def test_recovery_excludes_non_claim_source_types(
    db_pool: asyncpg.Pool,
) -> None:
    """Recovery sweep must not return rows whose source_type is outside claim_types.

    Regression: prior to the source_type filter, recovery would scoop up
    failing-retryable or active-abandoned rows of ANY source_type — including
    ECHO (served by a separate cloud function, never VM-leased) and any
    type the worker is not configured to claim. This breaks the per-type
    memory-budget invariant the primary path enforces structurally.
    """
    await db_pool.execute("TRUNCATE feeds CASCADE")
    # Worker only claims bcfy_feeds.
    store = FeedStore(db_pool, claim_types=[SourceType.BCFY_FEEDS])
    worker = uuid.uuid4()

    # An ECHO row in failing-retryable state with retry_after in the past.
    echo_id = await _insert_feed(
        db_pool,
        "echo-failing",
        source_type="echo",
        status="failing",
        failure_count=1,
        last_heartbeat_age_seconds=120,
    )
    await db_pool.execute(
        "UPDATE feeds SET retry_after = NOW() - INTERVAL '10 seconds'"
        " WHERE id = $1",
        echo_id,
    )
    # An openmhz row in active-abandoned state — also outside claim_types.
    other_worker = uuid.uuid4()
    await _insert_feed(
        db_pool,
        "openmhz-abandoned",
        source_type="openmhz",
        status="active",
        worker_id=other_worker,
        last_heartbeat_age_seconds=120,
    )
    # A bcfy_feeds row in failing-retryable — this one IS a valid target.
    bcfy_id = await _insert_feed(
        db_pool,
        "bcfy-failing",
        source_type="bcfy_feeds",
        status="failing",
        failure_count=1,
        last_heartbeat_age_seconds=120,
    )
    await db_pool.execute(
        "UPDATE feeds SET retry_after = NOW() - INTERVAL '10 seconds'"
        " WHERE id = $1",
        bcfy_id,
    )

    # Store only claims BCFY_FEEDS, so its generated recovery SQL only
    # has a bcfy_feeds_recovery branch. Echo and openmhz can't be
    # touched by the recovery sweep at all.
    result = await store.acquire_feeds_recovery(
        worker,
        abandonment_window_sec=60.0,
        limits={SourceType.BCFY_FEEDS: 10},
    )

    returned_ids = {lease["id"] for lease in result}
    assert echo_id not in returned_ids, "ECHO must never be claim-recovered"
    # Confirm only bcfy_feeds came back. The openmhz row would be eligible
    # for active-abandoned recovery (heartbeat_age=120s > abandonment=60s),
    # so its exclusion proves the claim_types subset is doing the work,
    # not lock ownership or staleness.
    assert returned_ids == {bcfy_id}
    assert all(
        lease["source_type"] == SourceType.BCFY_FEEDS for lease in result
    )


async def test_recovery_with_full_claim_types_picks_up_all(
    db_pool: asyncpg.Pool,
) -> None:
    """With the production claim_types set, recovery still works for all three."""
    await db_pool.execute("TRUNCATE feeds CASCADE")
    store = FeedStore(
        db_pool,
        claim_types=[
            SourceType.BCFY_FEEDS,
            SourceType.BCFY_CALLS,
            SourceType.OPENMHZ,
        ],
    )
    worker = uuid.uuid4()

    ids: dict[str, uuid.UUID] = {}
    for source_type in ("bcfy_feeds", "bcfy_calls", "openmhz"):
        fid = await _insert_feed(
            db_pool,
            f"{source_type}-failing",
            source_type=source_type,
            status="failing",
            failure_count=1,
            last_heartbeat_age_seconds=120,
        )
        await db_pool.execute(
            "UPDATE feeds SET retry_after = NOW() - INTERVAL '10 seconds'"
            " WHERE id = $1",
            fid,
        )
        ids[source_type] = fid

    result = await store.acquire_feeds_recovery(
        worker,
        abandonment_window_sec=60.0,
        limits={
            SourceType.BCFY_FEEDS: 10,
            SourceType.BCFY_CALLS: 10,
            SourceType.OPENMHZ: 10,
        },
    )

    assert {lease["id"] for lease in result} == set(ids.values())


async def test_recovery_respects_per_type_caps(
    db_pool: asyncpg.Pool,
) -> None:
    """Recovery's per-type LIMIT bounds rows per branch, not just total.

    Regression: prior to per-type LIMITs in recovery, a single aggregate
    LIMIT could push held > cap when the at-cap type happened to be
    failing-retryable. Example: cap_bcfy_feeds=240, held=200, slack=50;
    primary returns 0 (all unclaimed bcfy_feeds are failing-retryable);
    recovery with limit=50 would scoop 50 bcfy_feeds → held=250>cap.
    With per-type LIMITs, the bcfy_feeds branch's LIMIT is bounded by
    `cap - held = 40`, so recovery returns at most 40 of that type.
    """
    await db_pool.execute("TRUNCATE feeds CASCADE")
    store = FeedStore(
        db_pool,
        claim_types=[
            SourceType.BCFY_FEEDS,
            SourceType.BCFY_CALLS,
            SourceType.OPENMHZ,
        ],
    )
    worker = uuid.uuid4()

    # Insert 10 failing-retryable bcfy_feeds; we'll ask for at most 3.
    bcfy_ids = []
    for i in range(10):
        fid = await _insert_feed(
            db_pool,
            f"bcfy-failing-{i}",
            source_type="bcfy_feeds",
            status="failing",
            failure_count=1,
            last_heartbeat_age_seconds=120,
        )
        await db_pool.execute(
            "UPDATE feeds SET retry_after = NOW() - INTERVAL '10 seconds'"
            " WHERE id = $1",
            fid,
        )
        bcfy_ids.append(fid)

    result = await store.acquire_feeds_recovery(
        worker,
        abandonment_window_sec=60.0,
        # Per-type LIMITs strictly bound each branch; bcfy_feeds capped at 3.
        limits={
            SourceType.BCFY_FEEDS: 3,
            SourceType.BCFY_CALLS: 100,
            SourceType.OPENMHZ: 100,
        },
    )

    bcfy_returned = [
        lease
        for lease in result
        if lease["source_type"] == SourceType.BCFY_FEEDS
    ]
    assert len(bcfy_returned) == 3, (
        f"recovery exceeded bcfy_feeds limit=3, got {len(bcfy_returned)}"
    )


# -- Tests: count_held_by_type ---------------------------------------


async def test_count_held_by_type_groups_only_active_owned_rows(
    db_pool: asyncpg.Pool, store: FeedStore
) -> None:
    """Only active rows owned by ``worker_id`` count, grouped by source_type."""
    worker = uuid.uuid4()
    other_worker = uuid.uuid4()

    # Owned + active: should count.
    await _insert_feed(
        db_pool,
        "owned-bf-1",
        source_type="bcfy_feeds",
        status="active",
        worker_id=worker,
        last_heartbeat_age_seconds=10,
    )
    await _insert_feed(
        db_pool,
        "owned-bf-2",
        source_type="bcfy_feeds",
        status="active",
        worker_id=worker,
        last_heartbeat_age_seconds=10,
    )
    await _insert_feed(
        db_pool,
        "owned-bc-1",
        source_type="bcfy_calls",
        status="active",
        worker_id=worker,
        last_heartbeat_age_seconds=10,
    )
    # Owned + active openmhz: zero rows expected → key still present with 0.

    # Other worker, active: must NOT count for our worker.
    await _insert_feed(
        db_pool,
        "other-bf",
        source_type="bcfy_feeds",
        status="active",
        worker_id=other_worker,
        last_heartbeat_age_seconds=10,
    )
    # Owned but failing: must NOT count (status != active).
    await _insert_feed(
        db_pool,
        "owned-failing",
        source_type="bcfy_feeds",
        status="failing",
        worker_id=None,
        failure_count=1,
        last_heartbeat_age_seconds=120,
    )
    # Unclaimed: must NOT count.
    await _insert_feed(db_pool, "unclaimed", source_type="bcfy_calls")

    result = await store.count_held_by_type(worker)

    assert result[SourceType.BCFY_FEEDS] == 2
    assert result[SourceType.BCFY_CALLS] == 1
    assert result[SourceType.OPENMHZ] == 0
    # ECHO is always returned with 0 — Echo feeds are served by a separate
    # cloud function and never leased by this worker.
    assert result[SourceType.ECHO] == 0


async def test_count_held_by_type_empty_when_worker_holds_nothing(
    db_pool: asyncpg.Pool, store: FeedStore
) -> None:
    """Worker with no active leases gets all-zero dict (every type keyed)."""
    other_worker = uuid.uuid4()
    await _insert_feed(
        db_pool,
        "active-other",
        source_type="bcfy_feeds",
        status="active",
        worker_id=other_worker,
        last_heartbeat_age_seconds=10,
    )

    result = await store.count_held_by_type(uuid.uuid4())

    assert set(result.keys()) == set(SourceType)
    assert all(v == 0 for v in result.values())


# -- Tests: last_heartbeat write hygiene (HB-01) ---------------------


async def _read_last_heartbeat(
    pool: asyncpg.Pool, feed_id: uuid.UUID
) -> datetime.datetime | None:
    row = await pool.fetchrow(
        "SELECT last_heartbeat FROM feeds WHERE id = $1::uuid",
        str(feed_id),
    )
    assert row is not None
    return row["last_heartbeat"]


async def test_update_progress_does_not_touch_last_heartbeat(
    db_pool: asyncpg.Pool, store: FeedStore
) -> None:
    """UPDATE_PROGRESS_SQL no longer writes last_heartbeat (HB-01)."""
    worker = uuid.uuid4()
    feed_id = await _insert_feed(
        db_pool,
        "Progress Feed",
        status="active",
        worker_id=worker,
        last_heartbeat_age_seconds=30,
    )
    before = await _read_last_heartbeat(db_pool, feed_id)

    ok = await store.update_feed_progress(
        feed_id,
        worker,
        "gs://bucket/file.flac",
        fencing_token=0,
        last_bookmark_time=None,
        actor_id=_TEST_ACTOR_ID,
    )

    assert ok is True
    after = await _read_last_heartbeat(db_pool, feed_id)
    assert after == before, (
        "last_heartbeat must be unchanged by progress writes"
    )


async def test_report_failure_does_not_touch_last_heartbeat(
    db_pool: asyncpg.Pool, store: FeedStore
) -> None:
    """REPORT_FAILURE_SQL no longer writes last_heartbeat (HB-01)."""
    worker = uuid.uuid4()
    feed_id = await _insert_feed(
        db_pool,
        "Failing Feed",
        status="active",
        worker_id=worker,
        last_heartbeat_age_seconds=30,
    )
    before = await _read_last_heartbeat(db_pool, feed_id)

    result = await _report_feed_failure_for_test(
        store,
        db_pool,
        feed_id,
        worker,
        fencing_token=0,
    )

    assert result is not None
    after = await _read_last_heartbeat(db_pool, feed_id)
    assert after == before, "last_heartbeat must be unchanged by failure writes"


# -- Tests: heartbeat skip-if-recent (HB-02) -------------------------


async def test_heartbeat_skipped_when_recent(
    db_pool: asyncpg.Pool, store: FeedStore
) -> None:
    """A feed with last_heartbeat <15 s ago is NOT renewed; caller still owns it."""
    worker = uuid.uuid4()
    feed_id = await _insert_feed(
        db_pool,
        "Recent Feed",
        status="active",
        worker_id=worker,
        last_heartbeat_age_seconds=5,
    )
    before = await _read_last_heartbeat(db_pool, feed_id)

    results = await store.renew_heartbeats_batch_diagnostic([feed_id], worker)

    assert len(results) == 1
    assert results[0]["current_worker"] == worker
    assert results[0]["renewed"] is False, "fresh heartbeat must skip renewal"
    after = await _read_last_heartbeat(db_pool, feed_id)
    assert after == before


async def test_heartbeat_renewed_when_stale(
    db_pool: asyncpg.Pool, store: FeedStore
) -> None:
    """A feed with last_heartbeat >15 s ago IS renewed, last_heartbeat advances."""
    worker = uuid.uuid4()
    feed_id = await _insert_feed(
        db_pool,
        "Stale Feed",
        status="active",
        worker_id=worker,
        last_heartbeat_age_seconds=30,
    )
    before = await _read_last_heartbeat(db_pool, feed_id)

    results = await store.renew_heartbeats_batch_diagnostic([feed_id], worker)

    assert results[0]["renewed"] is True
    after = await _read_last_heartbeat(db_pool, feed_id)
    assert after is not None and before is not None
    assert after > before, "stale heartbeat must advance"


async def test_heartbeat_null_is_renewed(
    db_pool: asyncpg.Pool, store: FeedStore
) -> None:
    """NULL-safe branch: last_heartbeat IS NULL is eligible for renewal."""
    worker = uuid.uuid4()
    feed_id = await _insert_feed(
        db_pool,
        "Null Heartbeat Feed",
        status="active",
        worker_id=worker,
        last_heartbeat_age_seconds=None,  # yields NULL
    )

    results = await store.renew_heartbeats_batch_diagnostic([feed_id], worker)

    assert results[0]["renewed"] is True
    after = await _read_last_heartbeat(db_pool, feed_id)
    assert after is not None


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
        actor_id=_TEST_ACTOR_ID,
    )

    assert result is True
    row = await _get_feed_status(db_pool, feed_id)
    assert row["failure_count"] == 0


async def test_progress_clears_stale_status_reason_and_stamps_clear_time(
    db_pool: asyncpg.Pool,
    store: FeedStore,
) -> None:
    """Successful progress clears a stale canonical reason with a clear timestamp."""
    old_reason_ts = datetime.datetime(2026, 5, 29, 12, 0, tzinfo=datetime.UTC)
    worker = uuid.uuid4()
    feed_id = await _insert_feed(
        db_pool,
        "Progress Reason Feed",
        status="active",
        worker_id=worker,
        last_heartbeat_age_seconds=10,
        failure_count=1,
    )
    await db_pool.execute(
        "UPDATE feeds SET status_reason = $1,"
        " status_reason_updated_at = $2 WHERE id = $3",
        FeedStatusReason.SOURCE_OFFLINE.value,
        old_reason_ts,
        feed_id,
    )

    result = await store.update_feed_progress(
        feed_id,
        worker,
        "gs://bucket/path/file.ogg",
        0,
        None,
        actor_id=_TEST_ACTOR_ID,
    )

    assert result is True
    row = await _get_feed_diagnostics(db_pool, feed_id)
    assert row["status"] == "active"
    assert row["failure_count"] == 0
    assert row["status_reason"] is None
    status_reason_updated_at = row["status_reason_updated_at"]
    assert status_reason_updated_at > old_reason_ts


async def test_progress_with_null_status_reason_leaves_reason_timestamp_unchanged(
    db_pool: asyncpg.Pool,
    store: FeedStore,
) -> None:
    """Successful progress does not churn reason timestamps without a stale reason."""
    old_reason_ts = datetime.datetime(2026, 5, 29, 12, 0, tzinfo=datetime.UTC)
    worker = uuid.uuid4()
    feed_id = await _insert_feed(
        db_pool,
        "Progress Null Reason Feed",
        status="active",
        worker_id=worker,
        last_heartbeat_age_seconds=10,
        failure_count=1,
    )
    await db_pool.execute(
        "UPDATE feeds SET status_reason = NULL,"
        " status_reason_updated_at = $1 WHERE id = $2",
        old_reason_ts,
        feed_id,
    )

    result = await store.update_feed_progress(
        feed_id,
        worker,
        "gs://bucket/path/file.ogg",
        0,
        None,
        actor_id=_TEST_ACTOR_ID,
    )

    assert result is True
    row = await _get_feed_diagnostics(db_pool, feed_id)
    assert row["status_reason"] is None
    status_reason_updated_at = row["status_reason_updated_at"]
    assert status_reason_updated_at == old_reason_ts


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
        actor_id=_TEST_ACTOR_ID,
    )

    assert result is False


async def test_progress_update_succeeds_for_deactivated_owned_feed(
    db_pool: asyncpg.Pool, store: FeedStore
) -> None:
    """An in-flight bookmark write may finish after admin deactivation."""
    worker = uuid.uuid4()
    old_reason_ts = datetime.datetime(2026, 5, 29, 12, 0, tzinfo=datetime.UTC)
    feed_id = await _insert_feed(
        db_pool,
        "Deactivated In Flight Feed",
        status="deactivated",
        worker_id=worker,
        last_heartbeat_age_seconds=10,
        failure_count=2,
    )
    await db_pool.execute(
        "UPDATE feeds SET status_reason = $1,"
        " status_reason_updated_at = $2 WHERE id = $3",
        FeedStatusReason.SOURCE_UNREACHABLE.value,
        old_reason_ts,
        feed_id,
    )

    result = await store.update_feed_progress(
        feed_id,
        worker,
        "gs://bucket/path/deactivated.ogg",
        0,
        None,
        actor_id=_TEST_ACTOR_ID,
    )

    assert result is True
    row = await _get_feed_diagnostics(db_pool, feed_id)
    assert row["status"] == "deactivated"
    assert row["failure_count"] == 0
    assert row["last_processed_filename"] == "gs://bucket/path/deactivated.ogg"
    assert row["worker_id"] == worker
    assert row["status_reason"] is None
    assert row["status_reason_updated_at"] > old_reason_ts


# -- Tests: record_source_observation ----------------------------------


async def test_postgresql_greatest_ignores_null_bookmark_arguments(
    db_pool: asyncpg.Pool,
) -> None:
    """AlloyDB/PostgreSQL GREATEST keeps the non-NULL timestamp argument."""
    bookmark = datetime.datetime(2026, 6, 8, 12, 0, tzinfo=datetime.UTC)

    row = await db_pool.fetchrow(
        """
        SELECT
            GREATEST(
                NULL::TIMESTAMP WITH TIME ZONE,
                NULL::TIMESTAMP WITH TIME ZONE
            ) AS both_null,
            GREATEST(
                $1::TIMESTAMP WITH TIME ZONE,
                NULL::TIMESTAMP WITH TIME ZONE
            ) AS left_value,
            GREATEST(
                NULL::TIMESTAMP WITH TIME ZONE,
                $1::TIMESTAMP WITH TIME ZONE
            ) AS right_value
        """,
        bookmark,
    )

    assert row["both_null"] is None
    assert row["left_value"] == bookmark
    assert row["right_value"] == bookmark


@pytest.mark.parametrize(
    ("existing_bookmark", "resume_position", "expected_bookmark"),
    [
        pytest.param(None, None, None, id="both-null"),
        pytest.param(
            None,
            datetime.datetime(2026, 6, 8, 12, 0, tzinfo=datetime.UTC),
            datetime.datetime(2026, 6, 8, 12, 0, tzinfo=datetime.UTC),
            id="existing-null",
        ),
        pytest.param(
            datetime.datetime(2026, 6, 8, 12, 0, tzinfo=datetime.UTC),
            None,
            datetime.datetime(2026, 6, 8, 12, 0, tzinfo=datetime.UTC),
            id="resume-null",
        ),
        pytest.param(
            datetime.datetime(2026, 6, 8, 12, 0, tzinfo=datetime.UTC),
            datetime.datetime(2026, 6, 8, 11, 59, tzinfo=datetime.UTC),
            datetime.datetime(2026, 6, 8, 12, 0, tzinfo=datetime.UTC),
            id="resume-older",
        ),
        pytest.param(
            datetime.datetime(2026, 6, 8, 12, 0, tzinfo=datetime.UTC),
            datetime.datetime(2026, 6, 8, 12, 1, tzinfo=datetime.UTC),
            datetime.datetime(2026, 6, 8, 12, 1, tzinfo=datetime.UTC),
            id="resume-newer",
        ),
    ],
)
async def test_source_observation_updates_bookmark_monotonically(
    db_pool: asyncpg.Pool,
    store: FeedStore,
    existing_bookmark: datetime.datetime | None,
    resume_position: datetime.datetime | None,
    expected_bookmark: datetime.datetime | None,
) -> None:
    """Observation writes clear stale failure state without rewinding bookmark."""
    worker = uuid.uuid4()
    old_reason_ts = datetime.datetime(2026, 6, 8, 10, 0, tzinfo=datetime.UTC)
    feed_id = await _insert_feed(
        db_pool,
        f"Observation Bookmark {uuid.uuid4()}",
        status="active",
        worker_id=worker,
        last_heartbeat_age_seconds=10,
        failure_count=2,
    )
    await db_pool.execute(
        "UPDATE feeds SET last_bookmark_time = $1,"
        " status_reason = $2,"
        " status_reason_updated_at = $3"
        " WHERE id = $4",
        existing_bookmark,
        FeedStatusReason.SOURCE_UNREACHABLE.value,
        old_reason_ts,
        feed_id,
    )

    result = await store.record_source_observation(
        feed_id,
        worker,
        0,
        resume_position,
        actor_id=_TEST_ACTOR_ID,
    )

    assert result["recorded"] is True
    row = await _get_feed_diagnostics(db_pool, feed_id)
    assert row["status"] == "active"
    assert row["failure_count"] == 0
    assert row["status_reason"] is None
    assert row["status_reason_updated_at"] > old_reason_ts
    assert row["last_bookmark_time"] == expected_bookmark


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

    await _report_feed_failure_for_test(store, db_pool, feed_id, worker, 0)

    row = await _get_feed_status(db_pool, feed_id)
    assert row["status"] == "failing"
    assert row["failure_count"] == 1
    assert row["worker_id"] is None


async def test_failure_records_canonical_reason_and_timestamp(
    db_pool: asyncpg.Pool,
    store: FeedStore,
) -> None:
    """Failure writes the explicit canonical reason separately from raw detail."""
    worker = uuid.uuid4()
    feed_id = await _insert_feed(
        db_pool,
        "Source Offline Feed",
        status="active",
        worker_id=worker,
        last_heartbeat_age_seconds=10,
    )

    await _report_feed_failure_for_test(
        store,
        db_pool,
        feed_id,
        worker,
        0,
        reason="raw_404",
        status_reason=FeedStatusReason.SOURCE_OFFLINE,
    )

    row = await _get_feed_diagnostics(db_pool, feed_id)
    assert row["status"] == "failing"
    assert row["failure_count"] == 1
    assert row["worker_id"] is None
    assert row["status_reason"] == "source_offline"
    assert row["status_reason_updated_at"] is not None
    assert row["status_reason_detail"] == "raw_404"


async def test_failure_without_status_reason_records_unexpected_fallback(
    db_pool: asyncpg.Pool,
    store: FeedStore,
) -> None:
    """Omitted status_reason stores the default unexpected-error reason."""
    worker = uuid.uuid4()
    feed_id = await _insert_feed(
        db_pool,
        "Untyped Failure Feed",
        status="active",
        worker_id=worker,
        last_heartbeat_age_seconds=10,
    )

    await _report_feed_failure_for_test(
        store,
        db_pool,
        feed_id,
        worker,
        0,
        reason="raw_untyped",
    )

    row = await _get_feed_diagnostics(db_pool, feed_id)
    assert row["status_reason"] == "system_unexpected_error"
    assert row["status_reason_updated_at"] is not None


async def test_failure_update_fails_after_deactivation_without_rewriting_reason(
    db_pool: asyncpg.Pool,
    store: FeedStore,
) -> None:
    """A stale worker cannot rewrite diagnostics after operator deactivation."""
    old_reason_ts = datetime.datetime(2026, 5, 29, 12, 0, tzinfo=datetime.UTC)
    worker = uuid.uuid4()
    feed_id = await _insert_feed(
        db_pool,
        "Failure Deactivated Feed",
        status="active",
        worker_id=worker,
        last_heartbeat_age_seconds=10,
        failure_count=2,
    )
    await db_pool.execute(
        "UPDATE feeds SET status_reason = $1,"
        " status_reason_updated_at = $2 WHERE id = $3",
        FeedStatusReason.SOURCE_UNREACHABLE.value,
        old_reason_ts,
        feed_id,
    )
    assert await store.deactivate_feed(feed_id, actor_id=_TEST_ACTOR_ID) is True

    result = await _report_feed_failure_for_test(
        store,
        db_pool,
        feed_id,
        worker,
        0,
        reason="raw",
        status_reason=FeedStatusReason.SYSTEM_PIPELINE_ERROR,
    )

    assert result is None
    row = await _get_feed_diagnostics(db_pool, feed_id)
    assert row["status"] == "deactivated"
    assert row["failure_count"] == 2
    assert row["worker_id"] == worker
    assert row["status_reason"] == "source_unreachable"
    assert row["status_reason_updated_at"] == old_reason_ts


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

    await _report_feed_failure_for_test(store, db_pool, feed_id, worker, 0)

    row = await _get_feed_status(db_pool, feed_id)
    assert row["status"] == "quarantined"
    assert row["failure_count"] == 5


async def test_failure_preserves_deactivated_feed(
    db_pool: asyncpg.Pool, store: FeedStore
) -> None:
    """Failure reporting must not move an admin-deactivated feed."""
    worker = uuid.uuid4()
    feed_id = await _insert_feed(
        db_pool,
        "Deactivated Failure Feed",
        status="deactivated",
        worker_id=worker,
        last_heartbeat_age_seconds=10,
        failure_count=2,
    )

    result = await _report_feed_failure_for_test(
        store,
        db_pool,
        feed_id,
        worker,
        0,
    )

    assert result is None
    row = await _get_feed_status(db_pool, feed_id)
    assert row["status"] == "deactivated"
    assert row["failure_count"] == 2
    assert row["worker_id"] == worker


async def test_quarantine_preserves_diagnostic_detail_separately_from_canonical_reason(
    db_pool: asyncpg.Pool,
    store: FeedStore,
) -> None:
    """Quarantine keeps diagnostic detail separate from canonical reason."""
    worker = uuid.uuid4()
    feed_id = await _insert_feed(
        db_pool,
        "Collector Error Feed",
        status="active",
        worker_id=worker,
        last_heartbeat_age_seconds=10,
        failure_count=4,
    )

    await _report_feed_failure_for_test(
        store,
        db_pool,
        feed_id,
        worker,
        0,
        reason="ffmpeg_exit_1",
        status_reason=FeedStatusReason.SYSTEM_COLLECTOR_ERROR,
    )

    row = await _get_feed_diagnostics(db_pool, feed_id)
    assert row["status"] == "quarantined"
    assert row["failure_count"] == 5
    assert row["status_reason_detail"] == "ffmpeg_exit_1"
    assert row["status_reason"] == "system_collector_error"
    assert row["status_reason_updated_at"] is not None


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
    await _report_feed_failure_for_test(store, db_pool, feed_id, worker, 0)
    leased = await store.acquire_feeds_recovery(
        uuid.uuid4(),
        abandonment_window_sec=60.0,
        limits={SourceType.BCFY_FEEDS: 1},
    )
    assert leased == []


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
    await _report_feed_failure_for_test(store, db_pool, feed_id, worker, 0)
    await db_pool.execute(
        "UPDATE feeds SET retry_after = NOW() - INTERVAL '1 second'"
        " WHERE id = $1",
        feed_id,
    )
    leased = await store.acquire_feeds_recovery(
        uuid.uuid4(),
        abandonment_window_sec=60.0,
        limits={SourceType.BCFY_FEEDS: 1},
    )
    assert len(leased) == 1
    assert leased[0]["id"] == feed_id


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
    await _report_feed_failure_for_test(store, db_pool, feed_id, worker, 0)
    await db_pool.execute(
        "UPDATE feeds SET retry_after = NOW() - INTERVAL '1 second'"
        " WHERE id = $1",
        feed_id,
    )
    new_worker = uuid.uuid4()
    leased = await store.acquire_feeds_recovery(
        new_worker,
        abandonment_window_sec=60.0,
        limits={SourceType.BCFY_FEEDS: 1},
    )
    assert len(leased) == 1
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
    await _report_feed_failure_for_test(store, db_pool, feed_id, worker, 0)
    await db_pool.execute(
        "UPDATE feeds SET retry_after = NOW() - INTERVAL '1 second'"
        " WHERE id = $1",
        feed_id,
    )
    new_worker = uuid.uuid4()
    leased = await store.acquire_feeds_recovery(
        new_worker,
        abandonment_window_sec=60.0,
        limits={SourceType.BCFY_FEEDS: 1},
    )
    assert len(leased) == 1
    result = leased[0]
    await store.update_feed_progress(
        result["id"],
        new_worker,
        "chunk_001.flac",
        result["fencing_token"],
        None,
        actor_id=_TEST_ACTOR_ID,
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


async def test_release_feed_preserves_deactivated_feed(
    db_pool: asyncpg.Pool, store: FeedStore
) -> None:
    """Normal task release must not make a deactivated feed claimable."""
    worker = uuid.uuid4()
    feed_id = await _insert_feed(
        db_pool,
        "Deactivated Release Feed",
        status="deactivated",
        worker_id=worker,
        last_heartbeat_age_seconds=10,
    )

    result = await store.release_feed(feed_id, worker, 0)

    assert result is False
    row = await _get_feed_status(db_pool, feed_id)
    assert row["status"] == "deactivated"
    assert row["worker_id"] == worker


async def test_batch_release_preserves_deactivated_feeds(
    db_pool: asyncpg.Pool, store: FeedStore
) -> None:
    """Shutdown batch release skips deactivated rows owned by the worker."""
    worker = uuid.uuid4()
    active_id = await _insert_feed(
        db_pool,
        "Active Batch Release Feed",
        status="active",
        worker_id=worker,
        last_heartbeat_age_seconds=10,
    )
    deactivated_id = await _insert_feed(
        db_pool,
        "Deactivated Batch Release Feed",
        status="deactivated",
        worker_id=worker,
        last_heartbeat_age_seconds=10,
    )

    result = await store.release_feeds_batch(worker)

    assert result == 1
    active_row = await _get_feed_status(db_pool, active_id)
    assert active_row["status"] == "unclaimed"
    assert active_row["worker_id"] is None
    deactivated_row = await _get_feed_status(db_pool, deactivated_id)
    assert deactivated_row["status"] == "deactivated"
    assert deactivated_row["worker_id"] == worker


# -- Tests: fencing_token ------------------------------------------------


async def test_fencing_token_increments_on_each_lease(
    db_pool: asyncpg.Pool, store: FeedStore
) -> None:
    """Re-leasing a feed increments the fencing token."""
    worker = uuid.uuid4()
    await _insert_feed(db_pool, "My Feed")

    leased1 = await store.acquire_feeds_batch(
        worker,
        limits={SourceType.BCFY_FEEDS: 1},
    )
    assert len(leased1) == 1
    result1 = leased1[0]
    assert result1["fencing_token"] == 1

    await store.release_feed(result1["id"], worker, 1)

    leased2 = await store.acquire_feeds_batch(
        worker,
        limits={SourceType.BCFY_FEEDS: 1},
    )
    assert len(leased2) == 1
    result2 = leased2[0]
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
        actor_id=_TEST_ACTOR_ID,
    )

    assert result is False


async def test_progress_update_fails_with_wrong_fencing_token_leaves_reason_fields(
    db_pool: asyncpg.Pool,
    store: FeedStore,
) -> None:
    """A fenced-out progress write leaves diagnostic reason fields unchanged."""
    old_reason_ts = datetime.datetime(2026, 5, 29, 12, 0, tzinfo=datetime.UTC)
    worker = uuid.uuid4()
    feed_id = await _insert_feed(
        db_pool,
        "Progress Fenced Reason Feed",
        status="active",
        worker_id=worker,
        last_heartbeat_age_seconds=10,
    )
    await db_pool.execute(
        "UPDATE feeds SET status_reason = $1,"
        " status_reason_updated_at = $2 WHERE id = $3",
        FeedStatusReason.SOURCE_UNREACHABLE.value,
        old_reason_ts,
        feed_id,
    )

    result = await store.update_feed_progress(
        feed_id,
        worker,
        "gs://bucket/path/file.ogg",
        999,
        None,
        actor_id=_TEST_ACTOR_ID,
    )

    assert result is False
    row = await _get_feed_diagnostics(db_pool, feed_id)
    assert row["status_reason"] == "source_unreachable"
    status_reason_updated_at = row["status_reason_updated_at"]
    assert status_reason_updated_at == old_reason_ts


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

    result = await _report_feed_failure_for_test(
        store,
        db_pool,
        feed_id,
        worker,
        999,
    )

    assert result is None
    # Verify feed state unchanged (failure was rejected)
    row = await _get_feed_status(db_pool, feed_id)
    assert row["status"] == "active"
    assert row["failure_count"] == 0


async def test_report_feed_failure_fails_with_wrong_fencing_token_leaves_reason_fields(
    db_pool: asyncpg.Pool,
    store: FeedStore,
) -> None:
    """A fenced-out failure write leaves diagnostic reason fields unchanged."""
    old_reason_ts = datetime.datetime(2026, 5, 29, 12, 0, tzinfo=datetime.UTC)
    worker = uuid.uuid4()
    feed_id = await _insert_feed(
        db_pool,
        "Failure Fenced Reason Feed",
        status="active",
        worker_id=worker,
        last_heartbeat_age_seconds=10,
    )
    await db_pool.execute(
        "UPDATE feeds SET status_reason = $1,"
        " status_reason_updated_at = $2 WHERE id = $3",
        FeedStatusReason.SOURCE_UNREACHABLE.value,
        old_reason_ts,
        feed_id,
    )

    result = await _report_feed_failure_for_test(
        store,
        db_pool,
        feed_id,
        worker,
        999,
        reason="raw",
        status_reason=FeedStatusReason.SYSTEM_PIPELINE_ERROR,
    )

    assert result is None
    row = await _get_feed_diagnostics(db_pool, feed_id)
    assert row["status"] == "active"
    assert row["failure_count"] == 0
    assert row["status_reason"] == "source_unreachable"
    status_reason_updated_at = row["status_reason_updated_at"]
    assert status_reason_updated_at == old_reason_ts


# -- Tests: last_bookmark_time ------------------------------------------------


async def test_last_bookmark_time_round_trips_through_lease(
    db_pool: asyncpg.Pool, store: FeedStore
) -> None:
    """Bookmark set via update_feed_progress survives release and re-lease."""
    worker = uuid.uuid4()
    await _insert_feed(db_pool, "Bookmark Feed")

    # Lease the feed.
    leased1 = await store.acquire_feeds_batch(
        worker,
        limits={SourceType.BCFY_FEEDS: 1},
    )
    assert len(leased1) == 1
    result1 = leased1[0]
    assert result1["last_bookmark_time"] is None

    # Record a bookmark.
    bookmark = datetime.datetime(2026, 3, 30, 12, 0, 0, tzinfo=datetime.UTC)
    ok = await store.update_feed_progress(
        result1["id"],
        worker,
        "chunk_001.flac",
        result1["fencing_token"],
        bookmark,
        actor_id=_TEST_ACTOR_ID,
    )
    assert ok is True

    # Release and re-lease.
    await store.release_feed(result1["id"], worker, result1["fencing_token"])
    leased2 = await store.acquire_feeds_batch(
        uuid.uuid4(),
        limits={SourceType.BCFY_FEEDS: 1},
    )
    assert len(leased2) == 1
    result2 = leased2[0]

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
        actor_id=_TEST_ACTOR_ID,
    )

    assert feed is not None
    assert feed["name"] == "New Integration Feed"
    assert feed["source_type"] == SourceType.BCFY_FEEDS
    assert feed["source_feed_id"] == "src_123"

    # Verify in DB
    row = await db_pool.fetchrow(
        "SELECT f.name, fp.source_feed_id "
        "FROM feeds f "
        "JOIN feed_properties fp ON f.id = fp.feed_id "
        "WHERE f.id = $1",
        feed["id"],
    )
    assert row is not None
    assert row["name"] == "New Integration Feed"
    assert row["source_feed_id"] == "src_123"

    audit_rows = await _fetch_audit_events(db_pool, feed["id"])
    assert len(audit_rows) == 1
    audit_row = audit_rows[0]
    before_values = _decode_json_object(audit_row["before_values"])
    after_values = _decode_json_object(audit_row["after_values"])
    assert audit_row["action"] == "feed.created"
    assert audit_row["actor_id"] == _TEST_ACTOR_ID
    assert audit_row["feed_revision"] == 1
    assert before_values == {}
    assert after_values["name"] == "New Integration Feed"
    assert after_values["source_type"] == "bcfy_feeds"
    assert after_values["status"] == "unclaimed"
    assert after_values["source_feed_id"] == "src_123"
    assert after_values["tags"] == []
    assert "worker_id" not in after_values
    assert "last_heartbeat" not in after_values


async def test_create_feed_allows_duplicate_source_feed_id(
    db_pool: asyncpg.Pool, store: FeedStore
) -> None:
    """create_feed permits multiple feeds to share the same source_type and source_feed_id."""
    feed1 = await store.create_feed(
        name="Feed Instance 1",
        source_type="bcfy_feeds",
        source_feed_id="shared_src_999",
        actor_id=_TEST_ACTOR_ID,
    )
    feed2 = await store.create_feed(
        name="Feed Instance 2",
        source_type="bcfy_feeds",
        source_feed_id="shared_src_999",
        actor_id=_TEST_ACTOR_ID,
    )

    assert feed1["id"] != feed2["id"]
    assert feed1["source_feed_id"] == "shared_src_999"
    assert feed2["source_feed_id"] == "shared_src_999"


async def test_update_feed_succeeds(
    db_pool: asyncpg.Pool, store: FeedStore
) -> None:
    """update_feed updates feed metadata."""
    feed = await store.create_feed(
        name="Original Name",
        source_type="bcfy_feeds",
        source_feed_id="src_123",
        actor_id=_TEST_ACTOR_ID,
    )

    updated_feed = await store.update_feed(
        feed_id=feed["id"],
        name="Updated Name",
        actor_id=_TEST_ACTOR_ID,
    )

    assert updated_feed is not None
    assert updated_feed["name"] == "Updated Name"
    assert updated_feed["source_feed_id"] == "src_123"

    # Verify in DB
    row = await db_pool.fetchrow(
        "SELECT f.name, fp.source_feed_id "
        "FROM feeds f "
        "JOIN feed_properties fp ON f.id = fp.feed_id "
        "WHERE f.id = $1",
        feed["id"],
    )
    assert row is not None
    assert row["name"] == "Updated Name"
    assert row["source_feed_id"] == "src_123"

    audit_rows = await _fetch_audit_events(db_pool, feed["id"])
    assert [row["action"] for row in audit_rows] == [
        "feed.created",
        "feed.updated",
    ]
    assert [row["feed_revision"] for row in audit_rows] == [1, 2]
    audit_row = audit_rows[1]
    before_values = _decode_json_object(audit_row["before_values"])
    after_values = _decode_json_object(audit_row["after_values"])
    assert audit_row["actor_id"] == _TEST_ACTOR_ID
    assert before_values["name"] == "Original Name"
    assert before_values["tags"] == []
    assert after_values["name"] == "Updated Name"
    assert after_values["source_feed_id"] == "src_123"
    assert "worker_id" not in before_values
    assert "last_heartbeat" not in after_values


async def test_update_feed_no_op_does_not_write_audit_event(
    db_pool: asyncpg.Pool, store: FeedStore
) -> None:
    """No-op metadata saves return the feed without advancing audit history."""
    tags = [{"key": "county", "value": "Fulton"}]
    feed = await store.create_feed(
        name="Original Name",
        source_type="bcfy_feeds",
        source_feed_id="src_123",
        tags=tags,
        actor_id=_TEST_ACTOR_ID,
    )
    revision_before = await _get_feed_audit_revision(db_pool, feed["id"])

    updated_feed = await store.update_feed(
        feed_id=feed["id"],
        name="Original Name",
        tags=tags,
        actor_id=_TEST_ACTOR_ID,
    )

    assert updated_feed is not None
    assert updated_feed["name"] == "Original Name"
    assert updated_feed["tags"] == tags
    assert (
        await _get_feed_audit_revision(db_pool, feed["id"]) == revision_before
    )
    audit_rows = await _fetch_audit_events(db_pool, feed["id"])
    assert [row["action"] for row in audit_rows] == ["feed.created"]
    assert [row["feed_revision"] for row in audit_rows] == [1]


async def test_update_feed_already_exists(
    db_pool: asyncpg.Pool, store: FeedStore
) -> None:
    """update_feed raises FeedNameAlreadyExistsError when name conflicts."""
    await store.create_feed(
        name="Existing Feed",
        source_type="bcfy_feeds",
        source_feed_id="src_456",
        actor_id=_TEST_ACTOR_ID,
    )

    feed = await store.create_feed(
        name="Original Name",
        source_type="bcfy_feeds",
        source_feed_id="src_123",
        actor_id=_TEST_ACTOR_ID,
    )

    with pytest.raises(FeedNameAlreadyExistsError):
        await store.update_feed(
            feed_id=feed["id"],
            name="Existing Feed",
            actor_id=_TEST_ACTOR_ID,
        )


# -- Tests: audited transaction rollback ------------------------------


async def test_create_feed_audit_failure_rolls_back_feed_and_revision(
    db_pool: asyncpg.Pool,
    store: FeedStore,
) -> None:
    """A rejected audit actor rolls back the attempted feed creation."""
    name = f"Rollback Create {uuid.uuid4()}"
    source_feed_id = f"rollback-{uuid.uuid4()}"
    with pytest.raises(asyncpg.CheckViolationError):
        await store.create_feed(
            name=name,
            source_type="bcfy_feeds",
            source_feed_id=source_feed_id,
            actor_id=_INVALID_ACTOR_ID,
        )

    feed_row = await db_pool.fetchrow(
        "SELECT f.id FROM feeds f"
        " JOIN feed_properties fp ON fp.feed_id = f.id"
        " WHERE f.name = $1 AND fp.source_feed_id = $2",
        name,
        source_feed_id,
    )
    audit_rows = await db_pool.fetch(
        "SELECT 1 FROM feed_audit_events"
        " WHERE before_values->>'name' = $1"
        " OR after_values->>'name' = $1",
        name,
    )

    assert feed_row is None
    assert audit_rows == []


async def test_update_feed_audit_failure_rolls_back_state_and_revision(
    db_pool: asyncpg.Pool,
    store: FeedStore,
) -> None:
    """A rejected audit actor leaves an existing feed unchanged."""
    feed = await store.create_feed(
        name=f"Rollback Update Original {uuid.uuid4()}",
        source_type="bcfy_feeds",
        source_feed_id=f"rollback-update-{uuid.uuid4()}",
        tags=[{"key": "phase", "value": "before"}],
        actor_id=_TEST_ACTOR_ID,
    )
    revision_before = await _get_feed_audit_revision(db_pool, feed["id"])

    with pytest.raises(asyncpg.CheckViolationError):
        await store.update_feed(
            feed_id=feed["id"],
            name=f"Rollback Update Failed {uuid.uuid4()}",
            tags=[{"key": "phase", "value": "after"}],
            actor_id=_INVALID_ACTOR_ID,
        )

    row = await db_pool.fetchrow(
        "SELECT f.name, fp.tags FROM feeds f"
        " JOIN feed_properties fp ON fp.feed_id = f.id"
        " WHERE f.id = $1",
        feed["id"],
    )
    audit_rows = await _fetch_audit_events(db_pool, feed["id"])
    revision_after = await _get_feed_audit_revision(db_pool, feed["id"])

    assert row is not None
    assert row["name"] == feed["name"]
    assert json.loads(row["tags"]) == [{"key": "phase", "value": "before"}]
    assert [event["action"] for event in audit_rows] == ["feed.created"]
    assert revision_after == revision_before


@pytest.mark.parametrize("actor_id", ["", "service: "])
async def test_invalid_actor_values_are_not_rewritten_by_storage(
    db_pool: asyncpg.Pool,
    store: FeedStore,
    actor_id: str,
) -> None:
    """Storage passes invalid actor values through to DB rejection."""
    feed = await store.create_feed(
        name=f"Invalid Actor {uuid.uuid4()}",
        source_type="bcfy_feeds",
        source_feed_id=f"invalid-actor-{uuid.uuid4()}",
        actor_id=_TEST_ACTOR_ID,
    )

    with pytest.raises(asyncpg.CheckViolationError):
        await store.deactivate_feed(feed["id"], actor_id=actor_id)

    row = await _get_feed_status(db_pool, feed["id"])
    audit_rows = await _fetch_audit_events(db_pool, feed["id"])

    assert row["status"] == "unclaimed"
    assert [event["action"] for event in audit_rows] == ["feed.created"]
    assert all(event["actor_id"] != actor_id for event in audit_rows)
    assert all(event["action"] != "feed.deactivated" for event in audit_rows)


# -- Tests: audited concurrent ordering -------------------------------


async def test_concurrent_same_feed_updates_allocate_unique_revisions(
    db_pool: asyncpg.Pool,
    store: FeedStore,
) -> None:
    """Concurrent same-feed updates remain uniquely revision-orderable."""
    feed = await store.create_feed(
        name=f"Concurrent Original {uuid.uuid4()}",
        source_type="bcfy_feeds",
        source_feed_id=f"concurrent-{uuid.uuid4()}",
        tags=[{"key": "phase", "value": "original"}],
        actor_id=_TEST_ACTOR_ID,
    )
    update_inputs: tuple[tuple[str, list[dict[str, str]]], ...] = (
        (
            f"Concurrent First {uuid.uuid4()}",
            [{"key": "phase", "value": "first"}],
        ),
        (
            f"Concurrent Second {uuid.uuid4()}",
            [{"key": "phase", "value": "second"}],
        ),
    )

    results = await asyncio.gather(
        *[
            store.update_feed(
                feed_id=feed["id"],
                name=name,
                tags=tags,
                actor_id=_TEST_ACTOR_ID,
            )
            for name, tags in update_inputs
        ],
        return_exceptions=True,
    )

    assert all(not isinstance(result, BaseException) for result in results)
    assert all(result is not None for result in results)

    audit_rows = await _fetch_audit_events(db_pool, feed["id"])
    feed_revisions = [row["feed_revision"] for row in audit_rows]
    update_events = [
        row for row in audit_rows if row["action"] == "feed.updated"
    ]
    duplicate_revision_count = await db_pool.fetchval(
        "SELECT COUNT(*) FROM ("
        " SELECT feed_revision FROM feed_audit_events"
        " WHERE feed_id = $1"
        " GROUP BY feed_revision HAVING COUNT(*) > 1"
        ") duplicates",
        feed["id"],
    )

    assert [row["action"] for row in audit_rows].count("feed.created") == 1
    assert len(update_events) == 2
    assert len(feed_revisions) == len(set(feed_revisions))
    assert feed_revisions == list(range(1, len(feed_revisions) + 1))
    assert duplicate_revision_count == 0
    assert await _get_feed_audit_revision(db_pool, feed["id"]) == 3
    assert {
        _decode_json_object(row["after_values"])["name"]
        for row in update_events
    } == {name for name, _tags in update_inputs}


# -- Tests: audited runtime lifecycle events -------------------------


async def test_first_abnormal_failure_writes_failure_reported_audit_event(
    db_pool: asyncpg.Pool,
    store: FeedStore,
) -> None:
    """A new failing status/reason combination emits feed.failure_reported."""
    worker = uuid.uuid4()
    feed_id = await _insert_feed(
        db_pool,
        "Runtime Failure Audit",
        status="active",
        worker_id=worker,
    )

    result = await _report_feed_failure_for_test(
        store,
        db_pool,
        feed_id,
        worker,
        status_reason=FeedStatusReason.SOURCE_UNREACHABLE,
        reason="provider timeout",
    )

    audit_rows = await _fetch_audit_events(db_pool, feed_id)
    assert result == "failing"
    assert len(audit_rows) == 1
    audit_row = audit_rows[0]
    before_values = _decode_json_object(audit_row["before_values"])
    after_values = _decode_json_object(audit_row["after_values"])
    assert audit_row["action"] == "feed.failure_reported"
    assert audit_row["actor_id"] == _TEST_ACTOR_ID
    assert audit_row["feed_revision"] == 1
    assert before_values["status"] == "active"
    assert before_values["failure_count"] == 0
    assert after_values["status"] == "failing"
    assert after_values["failure_count"] == 1
    assert after_values["status_reason"] == "source_unreachable"
    assert after_values["status_reason_detail"] == "provider timeout"


async def test_same_status_reason_failure_retry_writes_no_audit_event(
    db_pool: asyncpg.Pool,
    store: FeedStore,
) -> None:
    """Repeating the same failing status/reason combination stays quiet."""
    worker = uuid.uuid4()
    feed_id = await _insert_feed(
        db_pool,
        "Runtime Same Failure Audit",
        status="active",
        worker_id=worker,
        failure_count=1,
    )
    await db_pool.execute(
        "UPDATE feeds SET status_reason = $1 WHERE id = $2",
        FeedStatusReason.SOURCE_UNREACHABLE.value,
        feed_id,
    )

    result = await _report_feed_failure_for_test(
        store,
        db_pool,
        feed_id,
        worker,
        status_reason=FeedStatusReason.SOURCE_UNREACHABLE,
        reason="new detail",
    )

    assert result == "failing"
    assert await _fetch_audit_events(db_pool, feed_id) == []


async def test_threshold_crossing_writes_quarantined_audit_event(
    db_pool: asyncpg.Pool,
    store: FeedStore,
) -> None:
    """Crossing the failure threshold emits feed.quarantined."""
    worker = uuid.uuid4()
    feed_id = await _insert_feed(
        db_pool,
        "Runtime Quarantine Audit",
        status="active",
        worker_id=worker,
        failure_count=4,
    )
    await db_pool.execute(
        "UPDATE feeds SET status_reason = $1 WHERE id = $2",
        FeedStatusReason.SOURCE_UNREACHABLE.value,
        feed_id,
    )

    result = await _report_feed_failure_for_test(
        store,
        db_pool,
        feed_id,
        worker,
        status_reason=FeedStatusReason.SOURCE_UNREACHABLE,
        reason="fifth failure",
    )

    audit_rows = await _fetch_audit_events(db_pool, feed_id)
    assert result == "quarantined"
    assert len(audit_rows) == 1
    audit_row = audit_rows[0]
    after_values = _decode_json_object(audit_row["after_values"])
    assert audit_row["action"] == "feed.quarantined"
    assert audit_row["feed_revision"] == 1
    assert after_values["status"] == "quarantined"
    assert after_values["failure_count"] == 5
    assert after_values["status_reason"] == "source_unreachable"


async def test_successful_progress_writes_recovered_audit_event(
    db_pool: asyncpg.Pool,
    store: FeedStore,
) -> None:
    """Successful progress from an abnormal status/reason emits feed.recovered."""
    worker = uuid.uuid4()
    feed_id = await _insert_feed(
        db_pool,
        "Runtime Recovery Audit",
        status="active",
        worker_id=worker,
        failure_count=2,
    )
    await db_pool.execute(
        "UPDATE feeds SET status_reason = $1 WHERE id = $2",
        FeedStatusReason.SOURCE_UNREACHABLE.value,
        feed_id,
    )

    result = await store.update_feed_progress(
        feed_id,
        worker,
        "gs://bucket/recovered.flac",
        0,
        None,
        actor_id=_TEST_ACTOR_ID,
    )

    audit_rows = await _fetch_audit_events(db_pool, feed_id)
    assert result is True
    assert len(audit_rows) == 1
    audit_row = audit_rows[0]
    before_values = _decode_json_object(audit_row["before_values"])
    after_values = _decode_json_object(audit_row["after_values"])
    assert audit_row["action"] == "feed.recovered"
    assert audit_row["feed_revision"] == 1
    assert before_values["failure_count"] == 2
    assert before_values["status_reason"] == "source_unreachable"
    assert after_values["status"] == "active"
    assert after_values["failure_count"] == 0
    assert after_values["status_reason"] is None


# -- Tests: get_feed --------------------------------------------------


async def test_get_feed_returns_feed(
    db_pool: asyncpg.Pool, store: FeedStore
) -> None:
    """get_feed retrieves a specific feed by ID."""
    feed_id = await _insert_feed(
        db_pool,
        "Get Feed Test",
        source_feed_id="src_get",
    )

    feed = await store.get_feed(feed_id)

    assert feed is not None
    assert feed["id"] == feed_id
    assert feed["name"] == "Get Feed Test"
    assert feed["source_feed_id"] == "src_get"


async def test_get_feed_returns_none_if_not_found(store: FeedStore) -> None:
    """get_feed returns None for non-existent ID."""
    feed = await store.get_feed(uuid.uuid4())
    assert feed is None


async def test_get_feed_projects_last_speech_segment_timestamp(
    db_pool: asyncpg.Pool, store: FeedStore
) -> None:
    """get_feed and list_feeds project the correct last_speech_segment_timestamp."""
    feed_id = await _insert_feed(
        db_pool,
        "Speech Timestamp Feed",
        source_feed_id="src_speech_ts",
    )

    # 1. No segments: should be None
    feed = await store.get_feed(feed_id)
    assert feed is not None
    assert feed["last_speech_segment_timestamp"] is None

    # 2. Insert non-speech segment: should still be None
    t1 = datetime.datetime(2026, 6, 16, 12, 0, 0, tzinfo=datetime.UTC)
    await db_pool.execute(
        "INSERT INTO audio_segments (id, feed_id, classification, start_timestamp, end_timestamp, created_at) "
        "VALUES ($1, $2, 'OTHER'::audio_classification, $3, $4, NOW())",
        uuid.uuid4(),
        feed_id,
        t1 - datetime.timedelta(seconds=10),
        t1,
    )
    feed = await store.get_feed(feed_id)
    assert feed is not None
    assert feed["last_speech_segment_timestamp"] is None

    # 3. Insert speech segment at T2: should return T2
    t2 = datetime.datetime(2026, 6, 16, 13, 0, 0, tzinfo=datetime.UTC)
    await db_pool.execute(
        "INSERT INTO audio_segments (id, feed_id, classification, start_timestamp, end_timestamp, created_at) "
        "VALUES ($1, $2, 'SPEECH'::audio_classification, $3, $4, NOW())",
        uuid.uuid4(),
        feed_id,
        t2 - datetime.timedelta(seconds=10),
        t2,
    )
    feed = await store.get_feed(feed_id)
    assert feed is not None
    assert feed["last_speech_segment_timestamp"] == t2

    # 4. Insert another speech segment at a later T3: should return T3
    t3 = datetime.datetime(2026, 6, 16, 14, 0, 0, tzinfo=datetime.UTC)
    seg_id_3 = uuid.UUID("33333333-3333-3333-3333-333333333333")
    await db_pool.execute(
        "INSERT INTO audio_segments (id, feed_id, classification, start_timestamp, end_timestamp, created_at) "
        "VALUES ($1, $2, 'SPEECH'::audio_classification, $3, $4, NOW())",
        seg_id_3,
        feed_id,
        t3 - datetime.timedelta(seconds=10),
        t3,
    )
    feed = await store.get_feed(feed_id)
    assert feed is not None
    assert feed["last_speech_segment_timestamp"] == t3

    # 5. Insert another speech segment at same T3, but with a smaller ID to verify ordering: should still return T3
    seg_id_4 = uuid.UUID("22222222-2222-2222-2222-222222222222")
    await db_pool.execute(
        "INSERT INTO audio_segments (id, feed_id, classification, start_timestamp, end_timestamp, created_at) "
        "VALUES ($1, $2, 'SPEECH'::audio_classification, $3, $4, NOW())",
        seg_id_4,
        feed_id,
        t3 - datetime.timedelta(seconds=10),
        t3,
    )

    feed = await store.get_feed(feed_id)
    assert feed is not None
    assert feed["last_speech_segment_timestamp"] == t3

    # Verify that listing feeds also correctly projects the timestamp
    list_res = await store.list_feeds(limit=10)
    matching_feed = next(f for f in list_res.feeds if f["id"] == feed_id)
    assert matching_feed["last_speech_segment_timestamp"] == t3


# -- Tests: list_feeds ------------------------------------------------


async def test_list_feeds_returns_all_feeds_ordered_desc(
    db_pool: asyncpg.Pool, store: FeedStore
) -> None:
    """list_feeds retrieves all feeds ordered by created_at DESC."""
    feed_id_a = await _insert_feed(db_pool, "Feed A", source_feed_id="src_a")
    feed_id_b = await _insert_feed(db_pool, "Feed B", source_feed_id="src_b")

    result = await store.list_feeds()

    feeds = result.feeds
    assert len(feeds) == 2
    # The most recently created should be first
    assert feeds[0]["id"] == feed_id_b
    assert feeds[0]["source_feed_id"] == "src_b"

    assert feeds[1]["id"] == feed_id_a
    assert feeds[1]["source_feed_id"] == "src_a"

    assert result.next_token is None


async def test_list_feeds_limit_and_pagination(
    db_pool: asyncpg.Pool, store: FeedStore
) -> None:
    """list_feeds supports limit and next_token keyset pagination."""
    feed_id_a = await _insert_feed(db_pool, "Feed A", source_feed_id="src_a")
    await asyncio.sleep(0.01)  # Ensure distinct created_at timestamps
    feed_id_b = await _insert_feed(db_pool, "Feed B", source_feed_id="src_b")

    # Page 1
    page1 = await store.list_feeds(limit=1)
    assert len(page1.feeds) == 1
    assert page1.feeds[0]["id"] == feed_id_b
    assert page1.next_token is not None

    # Page 2 using next_token
    page2 = await store.list_feeds(limit=1, next_token=page1.next_token)
    assert len(page2.feeds) == 1
    assert page2.feeds[0]["id"] == feed_id_a
    assert page2.next_token is None


async def test_list_feeds_filter_by_source_type(
    db_pool: asyncpg.Pool, store: FeedStore
) -> None:
    """list_feeds filters results by source_types."""
    feed_id_a = await _insert_feed(
        db_pool, "Feed A", source_type="bcfy_feeds", source_feed_id="src_a"
    )
    feed_id_b = await _insert_feed(
        db_pool, "Feed B", source_type="openmhz", source_feed_id="src_b"
    )

    # Filter by bcfy_feeds
    bcfy_result = await store.list_feeds(
        source_types=[SourceType.BCFY_FEEDS],
    )
    assert len(bcfy_result.feeds) == 1
    assert bcfy_result.feeds[0]["id"] == feed_id_a

    # Filter by openmhz
    openmhz_result = await store.list_feeds(
        source_types=[SourceType.OPENMHZ],
    )
    assert len(openmhz_result.feeds) == 1
    assert openmhz_result.feeds[0]["id"] == feed_id_b

    # Filter by both
    both_result = await store.list_feeds(
        source_types=[SourceType.BCFY_FEEDS, SourceType.OPENMHZ],
    )
    assert len(both_result.feeds) == 2


async def test_list_feeds_filter_by_status(
    db_pool: asyncpg.Pool, store: FeedStore
) -> None:
    """list_feeds filters results by statuses."""
    feed_id_a = await _insert_feed(db_pool, "Feed A", source_feed_id="src_a")
    # Modify the status of Feed A to active
    await db_pool.execute(
        "UPDATE feeds SET status = $1 WHERE id = $2",
        "active",
        feed_id_a,
    )

    feed_id_b = await _insert_feed(db_pool, "Feed B", source_feed_id="src_b")

    # Filter by active status
    active_result = await store.list_feeds(statuses=[FeedStatus.ACTIVE])
    assert len(active_result.feeds) == 1
    assert active_result.feeds[0]["id"] == feed_id_a

    # Filter by unclaimed status
    unclaimed_result = await store.list_feeds(
        statuses=[FeedStatus.UNCLAIMED],
    )
    assert len(unclaimed_result.feeds) == 1
    assert unclaimed_result.feeds[0]["id"] == feed_id_b


async def test_list_feeds_filter_by_tags(store: FeedStore) -> None:
    """list_feeds filters results by tag containment."""
    feed_a = await store.create_feed(
        name="Feed A",
        source_type="bcfy_feeds",
        source_feed_id="src_a",
        tags=[{"key": "region", "value": "West"}],
        actor_id=_TEST_ACTOR_ID,
    )
    feed_b = await store.create_feed(
        name="Feed B",
        source_type="bcfy_feeds",
        source_feed_id="src_b",
        tags=[{"key": "region", "value": "East"}],
        actor_id=_TEST_ACTOR_ID,
    )

    # Filter matching tag
    result_west = await store.list_feeds(
        tags=[{"key": "region", "value": "West"}]
    )
    assert len(result_west.feeds) == 1
    assert result_west.feeds[0]["id"] == feed_a["id"]

    # Filter other tag
    result_east = await store.list_feeds(
        tags=[{"key": "region", "value": "East"}]
    )
    assert len(result_east.feeds) == 1
    assert result_east.feeds[0]["id"] == feed_b["id"]

    # Filter non-matching tag
    result_none = await store.list_feeds(
        tags=[{"key": "region", "value": "South"}]
    )
    assert len(result_none.feeds) == 0


# -- Tests: deactivate_feed -------------------------------------------


async def test_deactivate_feed_succeeds(
    db_pool: asyncpg.Pool,
    store: FeedStore,
) -> None:
    """deactivate_feed persists state and one feed.deactivated audit row."""
    feed_id = await _insert_feed(
        db_pool,
        "Deactivate Audit Feed",
        status="active",
        source_feed_id=f"deactivate-{uuid.uuid4()}",
    )

    result = await store.deactivate_feed(feed_id, actor_id=_TEST_ACTOR_ID)

    assert result is True
    row = await _get_feed_status(db_pool, feed_id)
    audit_rows = await _fetch_audit_events(db_pool, feed_id)
    assert row["status"] == "deactivated"
    assert len(audit_rows) == 1
    audit_row = audit_rows[0]
    before_values = _decode_json_object(audit_row["before_values"])
    after_values = _decode_json_object(audit_row["after_values"])
    assert audit_row["action"] == "feed.deactivated"
    assert audit_row["actor_id"] == _TEST_ACTOR_ID
    assert audit_row["feed_revision"] == 1
    assert before_values["status"] == "active"
    assert after_values["status"] == "deactivated"
    source_feed_id = cast("str", before_values["source_feed_id"])
    assert source_feed_id.startswith("deactivate-")
    assert after_values["tags"] == []
    assert "worker_id" not in before_values
    assert "last_heartbeat" not in after_values


async def test_deactivate_feed_is_idempotent_without_duplicate_audit(
    db_pool: asyncpg.Pool,
    store: FeedStore,
) -> None:
    """Repeated deactivation keeps existing state without another audit row."""
    feed_id = await _insert_feed(
        db_pool,
        "Deactivate Idempotent Audit Feed",
        status="active",
        source_feed_id=f"deactivate-idempotent-{uuid.uuid4()}",
    )

    first_result = await store.deactivate_feed(feed_id, actor_id=_TEST_ACTOR_ID)
    revision_after_first = await _get_feed_audit_revision(db_pool, feed_id)
    second_result = await store.deactivate_feed(
        feed_id, actor_id=_TEST_ACTOR_ID
    )

    row = await _get_feed_status(db_pool, feed_id)
    audit_rows = await _fetch_audit_events(db_pool, feed_id)
    revision_after_second = await _get_feed_audit_revision(db_pool, feed_id)

    assert first_result is True
    assert second_result is True
    assert row["status"] == "deactivated"
    assert revision_after_first == 1
    assert revision_after_second == revision_after_first
    assert [audit_row["action"] for audit_row in audit_rows] == [
        "feed.deactivated"
    ]


# -- Tests: delete_feed ------------------------------------------------


async def test_delete_feed_succeeds(
    db_pool: asyncpg.Pool, store: FeedStore
) -> None:
    """delete_feed hard deletes the feed along with transcripts, audio segments, and annotations."""
    # 1. Insert a feed
    feed_id = await _insert_feed(db_pool, "Hard Delete Test Feed")

    # 2. Insert a transcript for the feed
    segment_id = uuid.uuid4()
    await db_pool.execute(
        """
        INSERT INTO transcripts (segment_id, feed_id, transcript, start_timestamp, end_timestamp, created_at)
        VALUES ($1, $2, $3, NOW() - INTERVAL '10 seconds', NOW(), NOW())
        """,
        str(segment_id),
        str(feed_id),
        "Test transcript to be deleted",
    )

    # 3. Insert an audio segment for the feed
    segment_id = uuid.uuid4()
    await db_pool.execute(
        """
        INSERT INTO audio_segments (id, feed_id, classification, start_timestamp, end_timestamp, created_at)
        VALUES ($1, $2, 'SPEECH', NOW() - INTERVAL '10 seconds', NOW(), NOW())
        """,
        str(segment_id),
        str(feed_id),
    )

    # 4. Insert an annotation for the audio segment
    await db_pool.execute(
        """
        INSERT INTO annotations (audio_segment_id, type, data, created_at)
        VALUES ($1, 'TRANSCRIPT', '{"text": "hello world"}', NOW())
        """,
        str(segment_id),
    )

    # 5. Perform the delete_feed call
    result = await store.delete_feed(feed_id, actor_id=_TEST_ACTOR_ID)

    assert result is True

    # 6. Verify the feed is deleted from feeds table
    row = await db_pool.fetchrow("SELECT 1 FROM feeds WHERE id = $1", feed_id)
    assert row is None

    # 7. Verify feed properties are deleted (cascade)
    row_props = await db_pool.fetchrow(
        "SELECT 1 FROM feed_properties WHERE feed_id = $1", feed_id
    )
    assert row_props is None

    # 8. Verify transcripts are deleted (CTE)
    row_trans = await db_pool.fetchrow(
        "SELECT 1 FROM transcripts WHERE feed_id = $1", feed_id
    )
    assert row_trans is None

    # 9. Verify audio segments are deleted (CTE)
    row_segs = await db_pool.fetchrow(
        "SELECT 1 FROM audio_segments WHERE feed_id = $1", feed_id
    )
    assert row_segs is None

    # 10. Verify annotations are deleted (cascade)
    row_annots = await db_pool.fetchrow(
        "SELECT 1 FROM annotations WHERE audio_segment_id = $1", segment_id
    )
    assert row_annots is None

    # 11. Verify delete audit history survives hard delete.
    audit_rows = await _fetch_audit_events(db_pool, feed_id)
    assert len(audit_rows) == 1
    audit_row = audit_rows[0]
    before_values = _decode_json_object(audit_row["before_values"])
    after_values = _decode_json_object(audit_row["after_values"])
    assert audit_row["action"] == "feed.deleted"
    assert audit_row["actor_id"] == _TEST_ACTOR_ID
    assert audit_row["feed_revision"] == 1
    assert before_values["id"] == str(feed_id)
    assert before_values["name"] == "Hard Delete Test Feed"
    source_feed_id = cast("str", before_values["source_feed_id"])
    assert source_feed_id.startswith("src_")
    assert before_values["tags"] == []
    assert after_values == {}
    assert "worker_id" not in before_values
    assert "last_heartbeat" not in before_values


async def test_delete_feed_returns_false_if_not_found(store: FeedStore) -> None:
    """delete_feed returns False for a non-existent feed ID."""
    result = await store.delete_feed(uuid.uuid4(), actor_id=_TEST_ACTOR_ID)
    assert result is False


async def test_delete_feed_active_feed_raises_conflict_without_audit(
    db_pool: asyncpg.Pool,
    store: FeedStore,
) -> None:
    """delete_feed returns a state conflict for active feeds."""
    feed_id = await _insert_feed(
        db_pool,
        "Active Delete Conflict Feed",
        status="active",
    )

    with pytest.raises(FeedStateConflictError, match="cannot be deleted"):
        await store.delete_feed(feed_id, actor_id=_TEST_ACTOR_ID)

    row = await db_pool.fetchrow(
        "SELECT status FROM feeds WHERE id = $1", feed_id
    )
    assert row is not None
    assert row["status"] == "active"
    audit_rows = await _fetch_audit_events(db_pool, feed_id)
    assert audit_rows == []


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

    feed = await store.reset_feed(feed_id, actor_id=_TEST_ACTOR_ID)

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

    audit_rows = await _fetch_audit_events(db_pool, feed_id)
    assert len(audit_rows) == 1
    audit_row = audit_rows[0]
    before_values = _decode_json_object(audit_row["before_values"])
    after_values = _decode_json_object(audit_row["after_values"])
    assert audit_row["action"] == "feed.reset"
    assert audit_row["actor_id"] == _TEST_ACTOR_ID
    assert audit_row["feed_revision"] == 1
    assert before_values["status"] == "quarantined"
    assert before_values["failure_count"] == 5
    assert after_values["status"] == "unclaimed"
    assert after_values["failure_count"] == 0
    assert after_values["tags"] == []
    assert "worker_id" not in before_values
    assert "last_heartbeat" not in after_values


async def test_reset_clears_stale_status_reason_with_clear_timestamp(
    db_pool: asyncpg.Pool,
    store: FeedStore,
) -> None:
    """Reset clears stale canonical reason."""
    old_reason_ts = datetime.datetime(2026, 5, 29, 12, 0, tzinfo=datetime.UTC)
    worker = uuid.uuid4()
    feed_id = await _insert_feed(
        db_pool,
        "Reason Reset Feed",
        status="quarantined",
        failure_count=5,
        worker_id=worker,
        last_heartbeat_age_seconds=1000,
    )
    await db_pool.execute(
        "UPDATE feeds SET status_reason_detail = $1, status_reason = $2,"
        " status_reason_updated_at = $3 WHERE id = $4",
        "raw outage",
        FeedStatusReason.SOURCE_UNREACHABLE.value,
        old_reason_ts,
        feed_id,
    )

    feed = await store.reset_feed(feed_id, actor_id=_TEST_ACTOR_ID)

    assert feed is not None
    assert feed["status"] == "unclaimed"
    assert feed["failure_count"] == 0
    assert feed["worker_id"] is None
    assert feed["status_reason"] is None
    status_reason_updated_at = feed["status_reason_updated_at"]
    assert status_reason_updated_at is not None
    assert status_reason_updated_at > old_reason_ts


async def test_reset_clears_status_reason_detail_in_row_and_audit(
    db_pool: asyncpg.Pool,
    store: FeedStore,
) -> None:
    """Reset clears stale diagnostic detail from state and audit snapshot."""
    feed_id = await _insert_feed(
        db_pool,
        "Detail Reset Feed",
        status="quarantined",
        failure_count=5,
    )
    await db_pool.execute(
        "UPDATE feeds SET status_reason = $1,"
        " status_reason_detail = $2 WHERE id = $3",
        FeedStatusReason.SOURCE_UNREACHABLE.value,
        "provider timeout from previous quarantine",
        feed_id,
    )

    feed = await store.reset_feed(feed_id, actor_id=_TEST_ACTOR_ID)

    assert feed is not None
    assert feed["status_reason"] is None
    row = await db_pool.fetchrow(
        "SELECT status_reason, status_reason_detail FROM feeds WHERE id = $1",
        feed_id,
    )
    assert row is not None
    assert row["status_reason"] is None
    assert row["status_reason_detail"] is None

    audit_rows = await _fetch_audit_events(db_pool, feed_id)
    assert len(audit_rows) == 1
    audit_row = audit_rows[0]
    before_values = _decode_json_object(audit_row["before_values"])
    after_values = _decode_json_object(audit_row["after_values"])
    assert audit_row["action"] == "feed.reset"
    assert (
        before_values["status_reason_detail"]
        == "provider timeout from previous quarantine"
    )
    assert after_values["status_reason_detail"] is None


async def test_reset_with_null_status_reason_leaves_reason_timestamp_unchanged(
    db_pool: asyncpg.Pool,
    store: FeedStore,
) -> None:
    """Reset keeps the reason timestamp stable when no canonical reason exists."""
    old_reason_ts = datetime.datetime(2026, 5, 29, 12, 0, tzinfo=datetime.UTC)
    feed_id = await _insert_feed(
        db_pool,
        "Null Reason Reset Feed",
        status="quarantined",
        failure_count=5,
        last_heartbeat_age_seconds=1000,
    )
    await db_pool.execute(
        "UPDATE feeds SET status_reason = NULL,"
        " status_reason_updated_at = $1 WHERE id = $2",
        old_reason_ts,
        feed_id,
    )

    feed = await store.reset_feed(feed_id, actor_id=_TEST_ACTOR_ID)

    assert feed is not None
    assert feed["status_reason"] is None
    status_reason_updated_at = feed["status_reason_updated_at"]
    assert status_reason_updated_at == old_reason_ts


async def test_reset_feed_returns_none_if_not_found(store: FeedStore) -> None:
    """reset_feed returns None for non-existent ID."""
    result = await store.reset_feed(uuid.uuid4(), actor_id=_TEST_ACTOR_ID)
    assert result is None


async def test_reset_feed_active_feed_raises_conflict_without_audit(
    db_pool: asyncpg.Pool,
    store: FeedStore,
) -> None:
    """reset_feed returns a state conflict for active feeds."""
    feed_id = await _insert_feed(
        db_pool,
        "Active Reset Conflict Feed",
        status="active",
    )

    with pytest.raises(FeedStateConflictError, match="cannot be reset"):
        await store.reset_feed(feed_id, actor_id=_TEST_ACTOR_ID)

    row = await db_pool.fetchrow(
        "SELECT status, audit_revision FROM feeds WHERE id = $1",
        feed_id,
    )
    assert row is not None
    assert row["status"] == "active"
    assert row["audit_revision"] == 0
    audit_rows = await _fetch_audit_events(db_pool, feed_id)
    assert audit_rows == []


# -- Tests: list_feed_history_records ---------------------------------


async def test_list_feed_history_records_decodes_jsonb(
    db_pool: asyncpg.Pool,
    store: FeedStore,
) -> None:
    """list_feed_history_records returns decoded JSONB fields (dicts)."""
    feed_id = await _insert_feed(
        db_pool,
        "History JSONB Decodes Feed",
        status="unclaimed",
    )

    # Trigger some event that writes to feed_audit_events.
    # Deactivating the feed should create an event.
    success = await store.deactivate_feed(feed_id, actor_id=_TEST_ACTOR_ID)
    assert success is True

    # Retrieve the audit events using the store method.
    result = await store.list_feed_history_records(feed_id)

    assert len(result.audit_events) == 1
    event = result.audit_events[0]
    assert event["action"] == "feed.deactivated"
    assert event["actor_id"] == _TEST_ACTOR_ID

    # These must be dicts, not raw strings
    assert isinstance(event["before_values"], dict)
    assert isinstance(event["after_values"], dict)

    # And check content
    assert event["before_values"]["status"] == "unclaimed"
    assert event["after_values"]["status"] == "deactivated"
