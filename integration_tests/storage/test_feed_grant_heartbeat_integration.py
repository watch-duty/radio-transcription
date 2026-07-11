"""External-PostgreSQL proof for exact Feed grant heartbeats.

This focused module never starts a local container or requests the shared
AlloyDB fixture. Every Feed identity is unique and remains in the prepared CI
database so the tests make no delete or truncate assumptions.
"""

from __future__ import annotations

import datetime
import os
import typing
import uuid

import asyncpg
import pytest
import pytest_asyncio

from backend.pipeline.storage import feed_store

if typing.TYPE_CHECKING:
    import collections.abc


pytestmark = pytest.mark.asyncio(loop_scope="module")

_SUPPORTED_POSTGRES_MAJORS = frozenset({"15", "16", "17"})
_OWNER_ID = uuid.UUID("11111111-2222-3333-4444-555555555555")
_OTHER_OWNER_ID = uuid.UUID("22222222-3333-4444-5555-666666666666")


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def ingestion_feed_pool() -> collections.abc.AsyncIterator[asyncpg.Pool]:
    """Connect only to the explicitly supplied PostgreSQL service."""
    if os.environ.get("PYTEST_XDIST_WORKER"):
        pytest.fail("the Feed heartbeat PostgreSQL module must run with -n 0")

    dsn = os.environ.get("INGESTION_FEED_TEST_DSN")
    required = (
        os.environ.get("INGESTION_FEED_EXTERNAL_POSTGRES_REQUIRED") == "1"
    )
    if not dsn:
        if required:
            pytest.fail(
                "INGESTION_FEED_TEST_DSN is required for this PostgreSQL gate"
            )
        pytest.skip("external PostgreSQL DSN is not configured")

    expected_major = os.environ.get("EXPECTED_POSTGRES_MAJOR")
    if required and expected_major is None:
        pytest.fail(
            "EXPECTED_POSTGRES_MAJOR is required for this PostgreSQL gate"
        )
    if (
        expected_major is not None
        and expected_major not in _SUPPORTED_POSTGRES_MAJORS
    ):
        pytest.fail(
            f"EXPECTED_POSTGRES_MAJOR {expected_major!r} is not supported; "
            f"expected one of {sorted(_SUPPORTED_POSTGRES_MAJORS)!r}"
        )

    pool = await asyncpg.create_pool(
        dsn=dsn,
        min_size=2,
        max_size=4,
        statement_cache_size=0,
    )
    try:
        if expected_major is not None:
            version_num = await pool.fetchval("SHOW server_version_num")
            actual_major = int(version_num) // 10000
            assert actual_major == int(expected_major), (
                "external PostgreSQL server major does not match the matrix"
            )
        yield pool
    finally:
        await pool.close()


async def _insert_feed(
    pool: asyncpg.Pool,
    *,
    status: str = "active",
    worker_id: uuid.UUID | None = _OWNER_ID,
    fencing_token: int = 7,
    last_heartbeat: datetime.datetime | None = None,
    failure_count: int = 0,
    status_reason: str | None = None,
) -> uuid.UUID:
    """Insert one unique Feed fixture without cleanup assumptions."""
    feed_id = uuid.uuid4()
    await pool.execute(
        """
        INSERT INTO public.feeds (
            id,
            name,
            source_type,
            status,
            worker_id,
            fencing_token,
            last_heartbeat,
            failure_count,
            status_reason
        ) VALUES (
            $1,
            $2,
            'bcfy_calls',
            $3::public.feed_status,
            $4,
            $5,
            $6,
            $7,
            $8
        )
        """,
        feed_id,
        f"feed-grant-heartbeat-{uuid.uuid4().hex}",
        status,
        worker_id,
        fencing_token,
        last_heartbeat,
        failure_count,
        status_reason,
    )
    return feed_id


async def _fetch_feed(pool: asyncpg.Pool, feed_id: uuid.UUID) -> asyncpg.Record:
    """Load the state the heartbeat query is permitted to observe."""
    row = await pool.fetchrow(
        """
        SELECT
            id,
            status::text AS status,
            worker_id,
            fencing_token,
            last_heartbeat,
            failure_count,
            status_reason
        FROM public.feeds
        WHERE id = $1
        """,
        feed_id,
    )
    assert row is not None
    return row


async def test_exact_feed_grant_heartbeats_are_ordered_and_fenced(
    ingestion_feed_pool: asyncpg.Pool,
) -> None:
    """Prove exact predicates, no-op semantics, and caller correlation."""
    now = datetime.datetime.now(datetime.UTC)
    stale_heartbeat = now - datetime.timedelta(minutes=2)

    stale_id = await _insert_feed(
        ingestion_feed_pool,
        last_heartbeat=stale_heartbeat,
    )
    null_id = await _insert_feed(ingestion_feed_pool, last_heartbeat=None)
    fresh_id = await _insert_feed(
        ingestion_feed_pool,
        last_heartbeat=now,
    )
    owner_mismatch_id = await _insert_feed(
        ingestion_feed_pool,
        worker_id=_OTHER_OWNER_ID,
        last_heartbeat=stale_heartbeat,
    )
    fence_mismatch_id = await _insert_feed(
        ingestion_feed_pool,
        fencing_token=8,
        last_heartbeat=stale_heartbeat,
    )
    administrative_id = await _insert_feed(
        ingestion_feed_pool,
        status="deactivated",
        last_heartbeat=stale_heartbeat,
        failure_count=2,
        status_reason="source_offline",
    )
    missing_id = uuid.uuid4()

    rejected_ids = (
        fresh_id,
        owner_mismatch_id,
        fence_mismatch_id,
        administrative_id,
    )
    before = {
        feed_id: dict(await _fetch_feed(ingestion_feed_pool, feed_id))
        for feed_id in rejected_ids
    }

    grants = (
        feed_store.FeedGrant(fence_mismatch_id, _OWNER_ID, 7),
        feed_store.FeedGrant(missing_id, _OWNER_ID, 7),
        feed_store.FeedGrant(fresh_id, _OWNER_ID, 7),
        feed_store.FeedGrant(null_id, _OWNER_ID, 7),
        feed_store.FeedGrant(administrative_id, _OWNER_ID, 7),
        feed_store.FeedGrant(owner_mismatch_id, _OWNER_ID, 7),
        feed_store.FeedGrant(stale_id, _OWNER_ID, 7),
    )
    store = feed_store.FeedStore(ingestion_feed_pool)

    results = await store.renew_grant_heartbeats(grants)

    assert tuple(result.grant for result in results) == grants
    assert tuple(result.disposition for result in results) == (
        feed_store.FeedGrantOperationDisposition.FENCE_MISMATCH,
        feed_store.FeedGrantOperationDisposition.MISSING,
        feed_store.FeedGrantOperationDisposition.ACCEPTED_NOOP,
        feed_store.FeedGrantOperationDisposition.APPLIED,
        feed_store.FeedGrantOperationDisposition.STATUS_INELIGIBLE,
        feed_store.FeedGrantOperationDisposition.OWNER_MISMATCH,
        feed_store.FeedGrantOperationDisposition.APPLIED,
    )
    assert results[1].snapshot is None
    assert results[4].snapshot is not None
    assert results[4].snapshot.failure_count == 2
    assert (
        results[4].snapshot.status_reason
        is feed_store.FeedStatusReason.SOURCE_OFFLINE
    )

    for feed_id in rejected_ids:
        assert (
            dict(await _fetch_feed(ingestion_feed_pool, feed_id))
            == before[feed_id]
        )

    null_after = await _fetch_feed(ingestion_feed_pool, null_id)
    stale_after = await _fetch_feed(ingestion_feed_pool, stale_id)
    assert null_after["last_heartbeat"] is not None
    assert stale_after["last_heartbeat"] > stale_heartbeat
