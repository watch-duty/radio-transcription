"""Real-PostgreSQL proofs for the fenced ingestion Lease lifecycle.

The generic runtime is not wired to this storage surface yet. These tests run
the dormant API against every supported PostgreSQL major so SQL syntax,
locking, fencing, and lifecycle behavior are proven independently of mocks.
Lease identities are permanent tombstones, so every test uses a unique key.
"""

from __future__ import annotations

import asyncio
import datetime
import os
import typing
import uuid

import asyncpg
import pytest
import pytest_asyncio

from backend.pipeline.storage import feed_store, ingestion_lease_store

if typing.TYPE_CHECKING:
    import collections.abc

pytestmark = pytest.mark.asyncio(loop_scope="module")

_SUPPORTED_POSTGRES_MAJORS = frozenset({"15", "16", "17"})
_TIMEOUT_SECONDS = 10.0
_SOURCE_TYPE = feed_store.SourceType.BCFY_CALLS


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def ingestion_lease_pool() -> collections.abc.AsyncIterator[asyncpg.Pool]:
    """Connect to the explicitly supplied external PostgreSQL service."""
    dsn = os.environ.get("INGESTION_LEASE_TEST_DSN")
    required = (
        os.environ.get("INGESTION_LEASE_EXTERNAL_POSTGRES_REQUIRED") == "1"
    )
    if not dsn:
        if required:
            pytest.fail(
                "INGESTION_LEASE_TEST_DSN is required for this PostgreSQL gate"
            )
        pytest.skip("external PostgreSQL DSN is not configured")
    if os.environ.get("PYTEST_XDIST_WORKER"):
        pytest.fail("the ingestion Lease PostgreSQL module must run serially")

    pool = await asyncpg.create_pool(
        dsn=dsn,
        min_size=4,
        max_size=10,
        statement_cache_size=0,
    )
    try:
        expected_major = os.environ.get("EXPECTED_POSTGRES_MAJOR")
        if expected_major is not None:
            if expected_major not in _SUPPORTED_POSTGRES_MAJORS:
                pytest.fail(
                    f"EXPECTED_POSTGRES_MAJOR {expected_major!r} is not "
                    "supported; expected one of "
                    f"{sorted(_SUPPORTED_POSTGRES_MAJORS)!r}"
                )
            version_num = await pool.fetchval("SHOW server_version_num")
            actual_major = int(version_num) // 10000
            assert actual_major == int(expected_major), (
                "external PostgreSQL server major does not match the matrix"
            )
        yield pool
    finally:
        await pool.close()


def _unique_digits() -> str:
    """Return a unique numeric-text external identity."""
    return str(uuid.uuid4().int)


async def _insert_lease(
    pool: asyncpg.Pool,
    sid: str,
    *,
    status: str = "unclaimed",
    owner_worker_id: uuid.UUID | None = None,
    fencing_token: int = 0,
    last_heartbeat: datetime.datetime | None = None,
    failure_count: int = 0,
    retry_after: datetime.datetime | None = None,
    status_reason: str | None = None,
    membership_revision: int = 0,
) -> None:
    """Insert one permanent, unique Lease fixture."""
    if status == "active" and last_heartbeat is None:
        last_heartbeat = datetime.datetime.now(datetime.UTC)
    await pool.execute(
        """
        INSERT INTO public.ingestion_leases (
            source_type,
            lease_key,
            status,
            worker_id,
            fencing_token,
            last_heartbeat,
            failure_count,
            retry_after,
            status_reason,
            status_reason_detail,
            membership_revision
        ) VALUES (
            'bcfy_calls',
            $1,
            $2::public.feed_status,
            $3,
            $4,
            $5,
            $6,
            $7,
            $8::TEXT,
            CASE WHEN $8 IS NULL THEN NULL ELSE 'fixture failure' END,
            $9
        )
        """,
        sid,
        status,
        owner_worker_id,
        fencing_token,
        last_heartbeat,
        failure_count,
        retry_after,
        status_reason,
        membership_revision,
    )


async def _insert_member(
    pool: asyncpg.Pool,
    sid: str,
    group_id: str,
    *,
    status: str = "active",
) -> uuid.UUID:
    """Insert one Feed and maintained Calls membership tuple.

    Args:
        pool: Database pool used to insert the fixture rows.
        sid: Textual Broadcastify Calls system identifier.
        group_id: Textual Broadcastify Calls talkgroup identifier.
        status: Initial Feed lifecycle status.

    Returns:
        The generated Feed identifier.
    """
    feed_id = uuid.uuid4()
    source_feed_id = f"{sid}-{group_id}"
    failing = status == "failing"
    now = datetime.datetime.now(datetime.UTC)
    async with pool.acquire() as connection:
        async with connection.transaction(isolation="read_committed"):
            await connection.execute(
                """
                INSERT INTO public.feeds (
                    id,
                    name,
                    source_type,
                    status,
                    failure_count,
                    retry_after,
                    status_reason,
                    status_reason_detail,
                    status_reason_updated_at
                ) VALUES (
                    $1,
                    $2,
                    'bcfy_calls',
                    $3::public.feed_status,
                    $4,
                    $5,
                    $6::TEXT,
                    $7,
                    $8
                )
                """,
                feed_id,
                f"Lease integration {uuid.uuid4().hex}",
                status,
                1 if failing else 0,
                now + datetime.timedelta(minutes=1) if failing else None,
                "source_unreachable" if failing else None,
                "fixture failure" if failing else None,
                now if failing else None,
            )
            await connection.execute(
                """
                INSERT INTO public.feed_properties (
                    feed_id,
                    source_feed_id,
                    source_type,
                    bcfy_calls_sid,
                    bcfy_calls_group_id,
                    bcfy_calls_is_trunked
                ) VALUES ($1, $2, 'bcfy_calls', $3, $4, TRUE)
                """,
                feed_id,
                source_feed_id,
                sid,
                group_id,
            )
    return feed_id


async def _claim_exact(
    store: ingestion_lease_store.IngestionLeaseStore,
    sid: str,
    owner_worker_id: uuid.UUID,
) -> ingestion_lease_store.LeaseGrant:
    claims = await store.claim_unclaimed(
        _SOURCE_TYPE,
        owner_worker_id,
        1000,
    )
    matching = [claim.grant for claim in claims if claim.grant.lease_key == sid]
    assert len(matching) == 1
    return matching[0]


async def _fetch_lease(pool: asyncpg.Pool, sid: str) -> asyncpg.Record:
    row = await pool.fetchrow(
        """
        SELECT
            status::text AS status,
            worker_id,
            fencing_token,
            last_heartbeat,
            failure_count,
            retry_after,
            status_reason,
            status_reason_detail,
            membership_revision,
            updated_at
        FROM public.ingestion_leases
        WHERE source_type = 'bcfy_calls' AND lease_key = $1
        """,
        sid,
    )
    assert row is not None
    return row


async def _cancel_tasks(*tasks: asyncio.Task[object] | None) -> None:
    pending = [task for task in tasks if task is not None and not task.done()]
    for task in pending:
        task.cancel()
    if pending:
        await asyncio.gather(*pending, return_exceptions=True)


async def test_competing_primary_claims_increment_fence_once(
    ingestion_lease_pool: asyncpg.Pool,
) -> None:
    sid = _unique_digits()
    await _insert_lease(ingestion_lease_pool, sid, fencing_token=41)
    store = ingestion_lease_store.IngestionLeaseStore(ingestion_lease_pool)
    barrier = asyncio.Barrier(3)

    async def _race(
        owner: uuid.UUID,
    ) -> tuple[ingestion_lease_store.LeaseClaim, ...]:
        await barrier.wait()
        return await store.claim_unclaimed(_SOURCE_TYPE, owner, 1)

    first = asyncio.create_task(_race(uuid.uuid4()))
    second = asyncio.create_task(_race(uuid.uuid4()))
    try:
        await barrier.wait()
        results = await asyncio.wait_for(
            asyncio.gather(first, second),
            timeout=_TIMEOUT_SECONDS,
        )
    finally:
        await _cancel_tasks(first, second)

    claims = [claim for result in results for claim in result]
    assert len(claims) == 1
    assert claims[0].grant.lease_key == sid
    assert claims[0].grant.fencing_token == 42
    durable = await _fetch_lease(ingestion_lease_pool, sid)
    assert durable["status"] == "active"
    assert durable["fencing_token"] == 42
    assert durable["worker_id"] == claims[0].grant.owner_worker_id


async def test_recovery_prioritizes_due_failure_then_stale_active(
    ingestion_lease_pool: asyncpg.Pool,
) -> None:
    now = datetime.datetime.now(datetime.UTC)
    due_sid = _unique_digits()
    stale_sid = _unique_digits()
    await _insert_lease(
        ingestion_lease_pool,
        due_sid,
        status="failing",
        fencing_token=7,
        failure_count=3,
        retry_after=now - datetime.timedelta(seconds=1),
    )
    await _insert_lease(
        ingestion_lease_pool,
        stale_sid,
        status="active",
        owner_worker_id=uuid.uuid4(),
        fencing_token=11,
        last_heartbeat=now - datetime.timedelta(minutes=2),
    )
    store = ingestion_lease_store.IngestionLeaseStore(ingestion_lease_pool)

    due_first = await store.claim_recoverable(
        _SOURCE_TYPE,
        uuid.uuid4(),
        1,
        datetime.timedelta(seconds=30),
    )
    assert [claim.grant.lease_key for claim in due_first] == [due_sid]
    assert due_first[0].grant.fencing_token == 8
    assert due_first[0].snapshot.failure_count == 3

    stale_second = await store.claim_recoverable(
        _SOURCE_TYPE,
        uuid.uuid4(),
        1,
        datetime.timedelta(seconds=30),
    )
    assert [claim.grant.lease_key for claim in stale_second] == [stale_sid]
    assert stale_second[0].grant.fencing_token == 12


async def test_exact_heartbeat_rejects_stale_fence_without_writing(
    ingestion_lease_pool: asyncpg.Pool,
) -> None:
    sid = _unique_digits()
    owner = uuid.uuid4()
    await _insert_lease(
        ingestion_lease_pool,
        sid,
        status="active",
        owner_worker_id=owner,
        fencing_token=19,
        last_heartbeat=(
            datetime.datetime.now(datetime.UTC) - datetime.timedelta(minutes=2)
        ),
    )
    store = ingestion_lease_store.IngestionLeaseStore(ingestion_lease_pool)
    grant = ingestion_lease_store.LeaseGrant(
        source_type=_SOURCE_TYPE,
        lease_key=sid,
        owner_worker_id=owner,
        fencing_token=19,
    )

    renewed = await store.renew_heartbeats((grant,))
    assert renewed[0].disposition is (
        ingestion_lease_store.LeaseOperationDisposition.APPLIED
    )
    after_renewal = await _fetch_lease(ingestion_lease_pool, sid)

    stale = ingestion_lease_store.LeaseGrant(
        source_type=_SOURCE_TYPE,
        lease_key=sid,
        owner_worker_id=owner,
        fencing_token=18,
    )
    rejected = await store.renew_heartbeats((stale,))
    assert rejected[0].disposition is (
        ingestion_lease_store.LeaseOperationDisposition.FENCE_MISMATCH
    )
    after_rejection = await _fetch_lease(ingestion_lease_pool, sid)
    assert after_rejection["last_heartbeat"] == after_renewal["last_heartbeat"]
    assert after_rejection["updated_at"] == after_renewal["updated_at"]


async def test_neutral_release_and_reclaim_invalidate_old_grant(
    ingestion_lease_pool: asyncpg.Pool,
) -> None:
    sid = _unique_digits()
    owner = uuid.uuid4()
    retry_after = datetime.datetime.now(datetime.UTC) + datetime.timedelta(
        minutes=5
    )
    await _insert_lease(
        ingestion_lease_pool,
        sid,
        status="active",
        owner_worker_id=owner,
        fencing_token=41,
        last_heartbeat=(
            datetime.datetime.now(datetime.UTC) - datetime.timedelta(minutes=1)
        ),
        failure_count=3,
        retry_after=retry_after,
        status_reason="source_unreachable",
        membership_revision=9,
    )
    store = ingestion_lease_store.IngestionLeaseStore(ingestion_lease_pool)
    old_grant = ingestion_lease_store.LeaseGrant(
        source_type=_SOURCE_TYPE,
        lease_key=sid,
        owner_worker_id=owner,
        fencing_token=41,
    )

    released = await store.release(
        old_grant,
        ingestion_lease_store.LeaseReleaseCause.SHUTDOWN,
    )
    assert released.disposition is (
        ingestion_lease_store.LeaseOperationDisposition.APPLIED
    )
    after_release = await _fetch_lease(ingestion_lease_pool, sid)
    assert after_release["status"] == "unclaimed"
    assert after_release["worker_id"] is None
    assert after_release["last_heartbeat"] is None
    assert after_release["fencing_token"] == 41
    assert after_release["failure_count"] == 3
    assert after_release["retry_after"] == retry_after
    assert after_release["status_reason"] == "source_unreachable"
    assert after_release["status_reason_detail"] == "fixture failure"
    assert after_release["membership_revision"] == 9

    new_grant = await _claim_exact(store, sid, owner)
    assert new_grant.fencing_token == 42

    stale_heartbeat = await store.renew_heartbeats((old_grant,))
    assert stale_heartbeat[0].disposition is (
        ingestion_lease_store.LeaseOperationDisposition.FENCE_MISMATCH
    )
    stale_release = await store.release(old_grant)
    assert stale_release.disposition is (
        ingestion_lease_store.LeaseOperationDisposition.FENCE_MISMATCH
    )
    durable = await _fetch_lease(ingestion_lease_pool, sid)
    assert durable["status"] == "active"
    assert durable["worker_id"] == owner
    assert durable["fencing_token"] == 42


async def test_budgeted_failure_releases_owner_and_preserves_authority(
    ingestion_lease_pool: asyncpg.Pool,
) -> None:
    sid = _unique_digits()
    owner = uuid.uuid4()
    await _insert_lease(
        ingestion_lease_pool,
        sid,
        status="active",
        owner_worker_id=owner,
        fencing_token=31,
        failure_count=2,
        membership_revision=9,
    )
    before = await _fetch_lease(ingestion_lease_pool, sid)
    database_before = await ingestion_lease_pool.fetchval("SELECT NOW()")
    store = ingestion_lease_store.IngestionLeaseStore(ingestion_lease_pool)
    grant = ingestion_lease_store.LeaseGrant(
        _SOURCE_TYPE,
        sid,
        owner,
        31,
    )

    result = await store.finalize_failure(
        grant,
        ingestion_lease_store.BudgetedFailure(
            failure_threshold=5,
            backoff_base_sec=2,
            backoff_max_sec=30,
        ),
        feed_store.FeedStatusReason.SOURCE_UNREACHABLE,
        actor_id="integration_test",
        reason="provider timeout",
    )

    database_after = await ingestion_lease_pool.fetchval("SELECT NOW()")
    durable = await _fetch_lease(ingestion_lease_pool, sid)
    assert result == ingestion_lease_store.LeaseFailureResult(
        ingestion_lease_store.LeaseOperationDisposition.APPLIED,
        feed_store.FeedStatus.FAILING,
    )
    assert durable["status"] == "failing"
    assert durable["worker_id"] is None
    assert durable["last_heartbeat"] is None
    assert durable["fencing_token"] == 31
    assert durable["failure_count"] == 3
    assert durable["membership_revision"] == 9
    assert durable["status_reason"] == "source_unreachable"
    assert durable["status_reason_detail"] == "provider timeout"
    assert durable["updated_at"] >= before["updated_at"]
    assert (
        database_before + datetime.timedelta(seconds=8)
        <= durable["retry_after"]
    )
    assert durable["retry_after"] <= database_after + datetime.timedelta(
        seconds=18
    )


async def test_budgeted_failure_quarantines_at_threshold(
    ingestion_lease_pool: asyncpg.Pool,
) -> None:
    sid = _unique_digits()
    owner = uuid.uuid4()
    await _insert_lease(
        ingestion_lease_pool,
        sid,
        status="active",
        owner_worker_id=owner,
        fencing_token=12,
        failure_count=4,
        membership_revision=3,
    )
    store = ingestion_lease_store.IngestionLeaseStore(ingestion_lease_pool)
    grant = ingestion_lease_store.LeaseGrant(_SOURCE_TYPE, sid, owner, 12)

    result = await store.finalize_failure(
        grant,
        ingestion_lease_store.BudgetedFailure(failure_threshold=5),
        feed_store.FeedStatusReason.SYSTEM_AUTHENTICATION_FAILED,
        actor_id="integration_test",
    )

    durable = await _fetch_lease(ingestion_lease_pool, sid)
    assert result.final_status is feed_store.FeedStatus.QUARANTINED
    assert durable["status"] == "quarantined"
    assert durable["worker_id"] is None
    assert durable["last_heartbeat"] is None
    assert durable["failure_count"] == 5
    assert durable["retry_after"] is None
    assert durable["fencing_token"] == 12
    assert durable["membership_revision"] == 3


async def test_non_budgeted_failure_resets_budget_and_never_quarantines(
    ingestion_lease_pool: asyncpg.Pool,
) -> None:
    sid = _unique_digits()
    owner = uuid.uuid4()
    retry_after = datetime.datetime.now(datetime.UTC) + datetime.timedelta(
        minutes=5
    )
    await _insert_lease(
        ingestion_lease_pool,
        sid,
        status="active",
        owner_worker_id=owner,
        fencing_token=18,
        failure_count=99,
        membership_revision=8,
    )
    store = ingestion_lease_store.IngestionLeaseStore(ingestion_lease_pool)
    grant = ingestion_lease_store.LeaseGrant(_SOURCE_TYPE, sid, owner, 18)

    result = await store.finalize_failure(
        grant,
        ingestion_lease_store.NonBudgetedFailure(retry_after),
        feed_store.FeedStatusReason.SYSTEM_PIPELINE_ERROR,
        actor_id="integration_test",
        reason="storage unavailable",
    )

    durable = await _fetch_lease(ingestion_lease_pool, sid)
    assert result.final_status is feed_store.FeedStatus.FAILING
    assert durable["status"] == "failing"
    assert durable["worker_id"] is None
    assert durable["last_heartbeat"] is None
    assert durable["failure_count"] == 0
    assert durable["retry_after"] == retry_after
    assert durable["status_reason"] == "system_pipeline_error"
    assert durable["status_reason_detail"] == "storage unavailable"
    assert durable["fencing_token"] == 18
    assert durable["membership_revision"] == 8


async def test_stale_failure_grant_is_rejected_without_writing(
    ingestion_lease_pool: asyncpg.Pool,
) -> None:
    sid = _unique_digits()
    owner = uuid.uuid4()
    await _insert_lease(
        ingestion_lease_pool,
        sid,
        status="active",
        owner_worker_id=owner,
        fencing_token=21,
        failure_count=2,
        membership_revision=6,
    )
    before = await _fetch_lease(ingestion_lease_pool, sid)
    store = ingestion_lease_store.IngestionLeaseStore(ingestion_lease_pool)
    stale = ingestion_lease_store.LeaseGrant(_SOURCE_TYPE, sid, owner, 20)

    result = await store.finalize_failure(
        stale,
        ingestion_lease_store.BudgetedFailure(),
        feed_store.FeedStatusReason.SOURCE_UNREACHABLE,
        actor_id="integration_test",
    )

    after = await _fetch_lease(ingestion_lease_pool, sid)
    assert result == ingestion_lease_store.LeaseFailureResult(
        ingestion_lease_store.LeaseOperationDisposition.FENCE_MISMATCH,
        None,
    )
    assert dict(after) == dict(before)


async def test_concurrent_failure_and_release_only_one_applies(
    ingestion_lease_pool: asyncpg.Pool,
) -> None:
    sid = _unique_digits()
    owner = uuid.uuid4()
    await _insert_lease(
        ingestion_lease_pool,
        sid,
        status="active",
        owner_worker_id=owner,
        fencing_token=40,
    )
    store = ingestion_lease_store.IngestionLeaseStore(ingestion_lease_pool)
    grant = ingestion_lease_store.LeaseGrant(_SOURCE_TYPE, sid, owner, 40)
    barrier = asyncio.Barrier(3)

    async def _fail() -> ingestion_lease_store.LeaseFailureResult:
        await barrier.wait()
        return await store.finalize_failure(
            grant,
            ingestion_lease_store.BudgetedFailure(),
            feed_store.FeedStatusReason.SOURCE_UNREACHABLE,
            actor_id="integration_test",
        )

    async def _release() -> ingestion_lease_store.LeaseOperationResult:
        await barrier.wait()
        return await store.release(grant)

    failure_task = asyncio.create_task(_fail())
    release_task = asyncio.create_task(_release())
    try:
        await barrier.wait()
        failure, release = await asyncio.wait_for(
            asyncio.gather(failure_task, release_task),
            timeout=_TIMEOUT_SECONDS,
        )
    finally:
        await _cancel_tasks(failure_task, release_task)

    assert {
        failure.disposition,
        release.disposition,
    } == {
        ingestion_lease_store.LeaseOperationDisposition.APPLIED,
        ingestion_lease_store.LeaseOperationDisposition.STATUS_INELIGIBLE,
    }
    durable = await _fetch_lease(ingestion_lease_pool, sid)
    assert durable["status"] in {"failing", "unclaimed"}
    assert durable["worker_id"] is None
    assert durable["last_heartbeat"] is None
    assert durable["fencing_token"] == 40


async def test_membership_snapshot_refresh_and_revision_fail_closed(
    ingestion_lease_pool: asyncpg.Pool,
) -> None:
    sid = f"00{_unique_digits()}"
    await _insert_lease(
        ingestion_lease_pool,
        sid,
        membership_revision=1,
    )
    store = ingestion_lease_store.IngestionLeaseStore(ingestion_lease_pool)
    grant = await _claim_exact(store, sid, uuid.uuid4())
    expected_ids = {
        "00045": await _insert_member(
            ingestion_lease_pool,
            sid,
            "00045",
        ),
        "00046": await _insert_member(
            ingestion_lease_pool,
            sid,
            "00046",
            status="failing",
        ),
        "00047": await _insert_member(
            ingestion_lease_pool,
            sid,
            "00047",
            status="deactivated",
        ),
    }

    snapshot = await store.load_membership(grant)

    assert isinstance(snapshot, ingestion_lease_store.MembershipSnapshot)
    assert snapshot.grant == grant
    assert snapshot.membership_revision == 1
    assert [member.identity.group_id for member in snapshot.members] == [
        "00045",
        "00046",
    ]
    assert [member.identity.feed_id for member in snapshot.members] == [
        expected_ids["00045"],
        expected_ids["00046"],
    ]
    assert all(member.identity.sid == sid for member in snapshot.members)

    async with ingestion_lease_pool.acquire() as blocker:
        transaction = blocker.transaction(isolation="read_committed")
        await transaction.start()
        try:
            await blocker.execute(
                "LOCK TABLE public.feed_properties IN ACCESS EXCLUSIVE MODE"
            )
            unchanged = await asyncio.wait_for(
                store.refresh_membership(grant, known_revision=1),
                timeout=_TIMEOUT_SECONDS,
            )
        finally:
            await transaction.rollback()

    assert unchanged == ingestion_lease_store.MembershipUnchanged(grant, 1)

    async with ingestion_lease_pool.acquire() as connection:
        async with connection.transaction(isolation="read_committed"):
            await connection.execute(
                """
                UPDATE public.ingestion_leases
                SET membership_revision = 2,
                    updated_at = NOW()
                WHERE source_type = 'bcfy_calls' AND lease_key = $1
                """,
                sid,
            )
            await connection.execute(
                """
                UPDATE public.feeds
                SET status = 'active'::public.feed_status
                WHERE id = $1
                """,
                expected_ids["00047"],
            )
    changed = await store.refresh_membership(grant, known_revision=1)
    assert isinstance(changed, ingestion_lease_store.MembershipSnapshot)
    assert changed.membership_revision == 2
    assert [member.identity.group_id for member in changed.members] == [
        "00045",
        "00046",
        "00047",
    ]

    regressed = await store.refresh_membership(grant, known_revision=3)
    assert isinstance(
        regressed,
        ingestion_lease_store.MembershipInvariantViolation,
    )
    assert "revision regressed" in regressed.detail

    released = await store.release(grant)
    assert released.disposition is (
        ingestion_lease_store.LeaseOperationDisposition.APPLIED
    )
    successor = await _claim_exact(store, sid, uuid.uuid4())
    assert successor.fencing_token > grant.fencing_token
    stale = await store.refresh_membership(grant, known_revision=2)
    assert isinstance(stale, ingestion_lease_store.GrantRejected)
    assert stale.reason is (
        ingestion_lease_store.GrantRejectionReason.OWNER_MISMATCH
    )


async def test_property_free_membership_load_is_explicitly_invalid(
    ingestion_lease_pool: asyncpg.Pool,
) -> None:
    sid = _unique_digits()
    await _insert_lease(ingestion_lease_pool, sid)
    store = ingestion_lease_store.IngestionLeaseStore(ingestion_lease_pool)
    grant = await _claim_exact(store, sid, uuid.uuid4())

    result = await store.load_membership(grant)

    assert isinstance(
        result,
        ingestion_lease_store.MembershipInvariantViolation,
    )
    assert "no structurally valid membership rows" in result.detail
