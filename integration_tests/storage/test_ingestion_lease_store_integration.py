"""PostgreSQL concurrency proofs for the fenced ingestion Lease store.

This module deliberately owns an external PostgreSQL fixture.  It must not
request the shared AlloyDB Omni fixtures because CI runs it against explicit
supported PostgreSQL services.  Lease identities are permanent tombstones, so
every test creates unique identities and leaves them in place.
"""

from __future__ import annotations

import asyncio
import datetime
import os
import typing
import uuid
from unittest import mock

import asyncpg
import pytest
import pytest_asyncio

from backend.pipeline.storage import feed_store, ingestion_lease_store

if typing.TYPE_CHECKING:
    import collections.abc

pytestmark = pytest.mark.asyncio(loop_scope="module")

_ACTOR_ID = "service_account:gcp:ingestion-lease-integration"
_BASE_CURSOR = datetime.datetime(2026, 7, 10, 12, 0, tzinfo=datetime.UTC)
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


def _forged_member_for_sid(
    feed_id: uuid.UUID,
    sid: str,
    group_id: str,
) -> ingestion_lease_store.LeaseMemberIdentity:
    """Construct an unsealed identity only for negative forgery proofs."""
    return ingestion_lease_store.LeaseMemberIdentity(
        feed_id=feed_id,
        source_type=_SOURCE_TYPE,
        source_feed_id=f"{sid}-{group_id}",
        sid=sid,
        group_id=group_id,
    )


def _issued_member_for_grant(
    grant: ingestion_lease_store.LeaseGrant,
    feed_id: uuid.UUID,
    group_id: str,
) -> ingestion_lease_store.LeaseMemberIdentity:
    """Issue a fixture identity with the same capability as its exact grant."""
    return ingestion_lease_store._issue_member_identity(
        grant,
        feed_id=feed_id,
        source_type=grant.source_type,
        source_feed_id=f"{grant.lease_key}-{group_id}",
        sid=grant.lease_key,
        group_id=group_id,
    )


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
    audit_revision: int = 0,
    membership_revision: int = 0,
) -> None:
    """Insert one permanent, unique Lease fixture."""
    if status == "active" and last_heartbeat is None:
        last_heartbeat = datetime.datetime.now(datetime.UTC)
    status_reason_updated_at = (
        datetime.datetime.now(datetime.UTC)
        if status_reason is not None
        else None
    )
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
            unclaimed_since,
            status_reason,
            status_reason_detail,
            status_reason_updated_at,
            audit_revision,
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
            CASE WHEN $2 = 'unclaimed' THEN NOW() ELSE NULL END,
            $8::TEXT,
            CASE WHEN $8 IS NULL THEN NULL ELSE 'fixture failure' END,
            $9,
            $10,
            $11
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
        status_reason_updated_at,
        audit_revision,
        membership_revision,
    )


async def _insert_member(
    pool: asyncpg.Pool,
    grant: ingestion_lease_store.LeaseGrant,
    *,
    status: str = "active",
    cursor: datetime.datetime | None = None,
    failure_count: int = 0,
    retry_after: datetime.datetime | None = None,
    status_reason: str | None = None,
    audit_revision: int = 0,
    group_id: str | None = None,
) -> ingestion_lease_store.LeaseMemberIdentity:
    """Insert one unique Feed and maintained Calls membership tuple."""
    sid = grant.lease_key
    feed_id = uuid.uuid4()
    if group_id is None:
        group_id = _unique_digits()
    source_feed_id = f"{sid}-{group_id}"
    status_reason_updated_at = (
        datetime.datetime.now(datetime.UTC)
        if status_reason is not None
        else None
    )
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
                    last_bookmark_time,
                    retry_after,
                    status_reason,
                    status_reason_detail,
                    status_reason_updated_at,
                    audit_revision
                ) VALUES (
                    $1,
                    $2,
                    'bcfy_calls',
                    $3::public.feed_status,
                    $4,
                    $5,
                    $6,
                    $7::TEXT,
                    CASE WHEN $7 IS NULL THEN NULL ELSE 'fixture failure' END,
                    $8,
                    $9
                )
                """,
                feed_id,
                f"Lease integration {uuid.uuid4().hex}",
                status,
                failure_count,
                cursor,
                retry_after,
                status_reason,
                status_reason_updated_at,
                audit_revision,
            )
            restored_cursor = await connection.fetchrow(
                """
                UPDATE public.feeds
                SET last_bookmark_time = $2
                WHERE id = $1
                RETURNING last_bookmark_time
                """,
                feed_id,
                cursor,
            )
            assert restored_cursor is not None
            assert restored_cursor["last_bookmark_time"] == cursor
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
    return _issued_member_for_grant(grant, feed_id, group_id)


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


async def _recover_exact(
    store: ingestion_lease_store.IngestionLeaseStore,
    sid: str,
    owner_worker_id: uuid.UUID,
) -> ingestion_lease_store.LeaseGrant:
    claims = await store.claim_recoverable(
        _SOURCE_TYPE,
        owner_worker_id,
        1000,
        datetime.timedelta(seconds=30),
    )
    matching = [claim.grant for claim in claims if claim.grant.lease_key == sid]
    assert len(matching) == 1
    return matching[0]


def _progress(
    member: ingestion_lease_store.LeaseMemberIdentity,
    cursor: datetime.datetime | None,
    *,
    path: str | None = None,
) -> ingestion_lease_store.AdmittedAudioProgress:
    return ingestion_lease_store.AdmittedAudioProgress(
        member=member,
        last_processed_filename=(
            path or f"gs://lease-integration/{uuid.uuid4().hex}.flac"
        ),
        cursor=cursor,
    )


def _observation(
    member: ingestion_lease_store.LeaseMemberIdentity,
    cursor: datetime.datetime | None,
) -> ingestion_lease_store.SourceObservation:
    return ingestion_lease_store.SourceObservation(member=member, cursor=cursor)


def _failure(
    member: ingestion_lease_store.LeaseMemberIdentity,
    cursor: datetime.datetime | None,
    *,
    budgeted: bool = True,
) -> ingestion_lease_store.FeedFailureTransition:
    if budgeted:
        action: ingestion_lease_store.LeaseFailureAction = (
            ingestion_lease_store.BudgetedFailure()
        )
        status_reason = feed_store.FeedStatusReason.SYSTEM_CONFIGURATION_INVALID
    else:
        action = ingestion_lease_store.NonBudgetedFailure(
            datetime.datetime.now(datetime.UTC) + datetime.timedelta(minutes=7)
        )
        status_reason = feed_store.FeedStatusReason.SYSTEM_PIPELINE_ERROR
    return ingestion_lease_store.FeedFailureTransition(
        member=member,
        action=action,
        status_reason=status_reason,
        reason="integration boundary failure",
        completion_cursor=cursor,
        charge_mode=(
            ingestion_lease_store.FeedFailureChargeMode.ON_CURSOR_ADVANCE
        ),
    )


def _batch(
    *mutations: ingestion_lease_store.ChildMutation,
    lease_effect: ingestion_lease_store.LeaseEffect | None = None,
) -> ingestion_lease_store.ChildMutationBatch:
    if lease_effect is None:
        lease_effect = ingestion_lease_store.NoLeaseEffect()
    return ingestion_lease_store.ChildMutationBatch(
        mutations=tuple(mutations),
        lease_effect=lease_effect,
    )


async def _commit(
    store: ingestion_lease_store.IngestionLeaseStore,
    grant: ingestion_lease_store.LeaseGrant,
    batch: ingestion_lease_store.ChildMutationBatch,
) -> ingestion_lease_store.GrantRejected | ingestion_lease_store.BatchCommitted:
    return await asyncio.wait_for(
        store.commit_child_mutations(grant, batch, actor_id=_ACTOR_ID),
        timeout=_TIMEOUT_SECONDS,
    )


async def _fetch_lease(pool: asyncpg.Pool, sid: str) -> asyncpg.Record:
    row = await pool.fetchrow(
        """
        SELECT
            source_type,
            lease_key,
            status::text AS status,
            worker_id,
            fencing_token,
            last_heartbeat,
            failure_count,
            retry_after,
            unclaimed_since,
            status_reason,
            status_reason_detail,
            status_reason_updated_at,
            audit_revision,
            membership_revision,
            created_at,
            updated_at
        FROM public.ingestion_leases
        WHERE source_type = 'bcfy_calls' AND lease_key = $1
        """,
        sid,
    )
    assert row is not None
    return row


async def _fetch_feed(pool: asyncpg.Pool, feed_id: uuid.UUID) -> asyncpg.Record:
    row = await pool.fetchrow(
        """
        SELECT
            id,
            name,
            source_type,
            status::text AS status,
            worker_id,
            fencing_token,
            last_heartbeat,
            last_processed_filename,
            last_bookmark_time,
            failure_count,
            retry_after,
            unclaimed_since,
            quarantine_reason,
            status_reason,
            status_reason_detail,
            status_reason_updated_at,
            audit_revision,
            created_at
        FROM public.feeds
        WHERE id = $1
        """,
        feed_id,
    )
    assert row is not None
    return row


async def _audit_rows(
    pool: asyncpg.Pool,
    feed_id: uuid.UUID,
) -> list[asyncpg.Record]:
    return await pool.fetch(
        """
        SELECT
            action,
            feed_revision,
            before_values,
            after_values,
            (before_values ->> 'status') AS before_status,
            (after_values ->> 'status') AS after_status,
            (before_values ->> 'failure_count')::integer
                AS before_failure_count,
            (after_values ->> 'failure_count')::integer
                AS after_failure_count
        FROM public.feed_audit_events
        WHERE feed_id = $1
        ORDER BY feed_revision
        """,
        feed_id,
    )


async def _wait_for_blocked_query(
    pool: asyncpg.Pool,
    query_fragment: str,
    *,
    minimum: int = 1,
) -> None:
    """Wait until PostgreSQL reports a bounded number of lock waiters."""

    async def _poll() -> None:
        for _attempt in range(1000):
            waiter_count = await pool.fetchval(
                """
                SELECT COUNT(*)
                FROM pg_catalog.pg_stat_activity AS activity
                WHERE activity.datname = current_database()
                  AND activity.pid <> pg_backend_pid()
                  AND activity.wait_event_type = 'Lock'
                  AND pg_catalog.strpos(activity.query, $1) > 0
                """,
                query_fragment,
            )
            if waiter_count >= minimum:
                return
            await asyncio.sleep(0.01)
        message = (
            f"PostgreSQL did not expose {minimum} blocked query or queries"
        )
        raise AssertionError(message)

    await asyncio.wait_for(_poll(), timeout=_TIMEOUT_SECONDS)


async def _cancel_tasks(*tasks: asyncio.Task[object] | None) -> None:
    pending = [task for task in tasks if task is not None and not task.done()]
    for task in pending:
        task.cancel()
    if pending:
        await asyncio.gather(*pending, return_exceptions=True)


async def _assert_old_grant_rejected(
    pool: asyncpg.Pool,
    store: ingestion_lease_store.IngestionLeaseStore,
    old_grant: ingestion_lease_store.LeaseGrant,
    member: ingestion_lease_store.LeaseMemberIdentity,
) -> None:
    lease_before = dict(await _fetch_lease(pool, old_grant.lease_key))
    feed_before = dict(await _fetch_feed(pool, member.feed_id))
    finalized_failure = await store.finalize_failure(
        old_grant,
        ingestion_lease_store.BudgetedFailure(),
        feed_store.FeedStatusReason.SYSTEM_CONFIGURATION_INVALID,
        actor_id=_ACTOR_ID,
        reason="stale grant must not finalize failure",
    )
    lease_after_finalized_failure = dict(
        await _fetch_lease(pool, old_grant.lease_key)
    )
    heartbeat = await store.renew_heartbeats((old_grant,))
    release = await store.release(old_grant)
    child = await _commit(
        store,
        old_grant,
        _batch(_observation(member, _BASE_CURSOR + datetime.timedelta(days=1))),
    )
    lease_after = dict(await _fetch_lease(pool, old_grant.lease_key))
    feed_after = dict(await _fetch_feed(pool, member.feed_id))

    assert heartbeat[0].disposition is not (
        ingestion_lease_store.LeaseOperationDisposition.APPLIED
    )
    assert release.disposition is not (
        ingestion_lease_store.LeaseOperationDisposition.APPLIED
    )
    assert finalized_failure.disposition is not (
        ingestion_lease_store.LeaseOperationDisposition.APPLIED
    )
    assert (
        finalized_failure.effect
        is ingestion_lease_store.LeaseFailureEffect.NONE
    )
    assert finalized_failure.before_snapshot is not None
    assert finalized_failure.after_snapshot is not None
    assert finalized_failure.before_snapshot == finalized_failure.after_snapshot
    assert lease_after_finalized_failure == lease_before
    assert isinstance(child, ingestion_lease_store.GrantRejected)
    assert lease_after == lease_before
    assert feed_after == feed_before


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


@pytest.mark.parametrize(
    "transition",
    [
        "neutral_release_reclaim",
        "finalized_failure_recovery",
        "stale_active_recovery",
        "same_worker_reacquisition",
    ],
)
async def test_generation_changes_reject_every_old_grant(
    ingestion_lease_pool: asyncpg.Pool,
    transition: str,
) -> None:
    sid = _unique_digits()
    await _insert_lease(ingestion_lease_pool, sid)
    store = ingestion_lease_store.IngestionLeaseStore(ingestion_lease_pool)
    first_owner = uuid.uuid4()
    old_grant = await _claim_exact(store, sid, first_owner)
    member = await _insert_member(ingestion_lease_pool, old_grant)

    if transition in {"neutral_release_reclaim", "same_worker_reacquisition"}:
        released = await store.release(old_grant)
        assert released.disposition is (
            ingestion_lease_store.LeaseOperationDisposition.APPLIED
        )
        next_owner = (
            first_owner
            if transition == "same_worker_reacquisition"
            else uuid.uuid4()
        )
        new_grant = await _claim_exact(store, sid, next_owner)
    elif transition == "finalized_failure_recovery":
        failed = await store.finalize_failure(
            old_grant,
            ingestion_lease_store.NonBudgetedFailure(
                datetime.datetime.now(datetime.UTC)
                - datetime.timedelta(seconds=1)
            ),
            feed_store.FeedStatusReason.SOURCE_UNREACHABLE,
            actor_id=_ACTOR_ID,
        )
        assert failed.disposition is (
            ingestion_lease_store.LeaseOperationDisposition.APPLIED
        )
        new_grant = await _recover_exact(store, sid, uuid.uuid4())
    else:
        await ingestion_lease_pool.execute(
            """
            UPDATE public.ingestion_leases
            SET last_heartbeat = NOW() - INTERVAL '2 minutes'
            WHERE source_type = 'bcfy_calls' AND lease_key = $1
            """,
            sid,
        )
        new_grant = await _recover_exact(store, sid, uuid.uuid4())

    assert new_grant.fencing_token > old_grant.fencing_token
    assert new_grant.fencing_token == old_grant.fencing_token + 1
    await _assert_old_grant_rejected(
        ingestion_lease_pool,
        store,
        old_grant,
        member,
    )


async def test_claim_order_and_property_free_claim(
    ingestion_lease_pool: asyncpg.Pool,
) -> None:
    store = ingestion_lease_store.IngestionLeaseStore(ingestion_lease_pool)
    now = datetime.datetime.now(datetime.UTC)

    primary_sid = _unique_digits()
    recovery_sid = _unique_digits()
    await _insert_lease(ingestion_lease_pool, primary_sid)
    await _insert_lease(
        ingestion_lease_pool,
        recovery_sid,
        status="failing",
        retry_after=now - datetime.timedelta(seconds=1),
    )
    primary = await store.claim_unclaimed(_SOURCE_TYPE, uuid.uuid4(), 1)
    assert [claim.grant.lease_key for claim in primary] == [primary_sid]
    recovered = await store.claim_recoverable(
        _SOURCE_TYPE,
        uuid.uuid4(),
        1,
        datetime.timedelta(seconds=30),
    )
    assert [claim.grant.lease_key for claim in recovered] == [recovery_sid]

    due_sid = _unique_digits()
    stale_sid = _unique_digits()
    await _insert_lease(
        ingestion_lease_pool,
        due_sid,
        status="failing",
        retry_after=now - datetime.timedelta(seconds=1),
    )
    await _insert_lease(
        ingestion_lease_pool,
        stale_sid,
        status="active",
        owner_worker_id=uuid.uuid4(),
        last_heartbeat=now - datetime.timedelta(minutes=2),
    )
    due_first = await store.claim_recoverable(
        _SOURCE_TYPE,
        uuid.uuid4(),
        1,
        datetime.timedelta(seconds=30),
    )
    assert [claim.grant.lease_key for claim in due_first] == [due_sid]
    stale_second = await store.claim_recoverable(
        _SOURCE_TYPE,
        uuid.uuid4(),
        1,
        datetime.timedelta(seconds=30),
    )
    assert [claim.grant.lease_key for claim in stale_second] == [stale_sid]

    identity_root = _unique_digits()
    low_sid = f"{identity_root}01"
    high_sid = f"{identity_root}02"
    await _insert_lease(ingestion_lease_pool, high_sid)
    await _insert_lease(ingestion_lease_pool, low_sid)
    ordered = await store.claim_unclaimed(_SOURCE_TYPE, uuid.uuid4(), 2)
    assert [claim.grant.lease_key for claim in ordered] == [low_sid, high_sid]

    property_free_sid = _unique_digits()
    await _insert_lease(ingestion_lease_pool, property_free_sid)
    property_free_grant = await _claim_exact(
        store,
        property_free_sid,
        uuid.uuid4(),
    )
    membership = await store.load_membership(property_free_grant)
    assert isinstance(
        membership,
        ingestion_lease_store.MembershipInvariantViolation,
    )
    assert (
        membership.reason
        is ingestion_lease_store.MembershipInvariantReason.EMPTY
    )


@pytest.mark.parametrize(
    "scenario",
    [
        "child_first_release",
        "transition_first_release",
        "transition_first_finalized_failure",
    ],
)
async def test_child_and_grant_transition_linearize_both_orders(  # noqa: PLR0915
    ingestion_lease_pool: asyncpg.Pool,
    scenario: str,
) -> None:
    sid = _unique_digits()
    await _insert_lease(ingestion_lease_pool, sid)
    store = ingestion_lease_store.IngestionLeaseStore(ingestion_lease_pool)
    old_grant = await _claim_exact(store, sid, uuid.uuid4())
    member = await _insert_member(ingestion_lease_pool, old_grant)
    target_cursor = _BASE_CURSOR + datetime.timedelta(minutes=1)
    child_task: asyncio.Task[object] | None = None
    transition_task: asyncio.Task[object] | None = None

    if scenario == "child_first_release":
        async with ingestion_lease_pool.acquire() as blocker:
            transaction = blocker.transaction(isolation="read_committed")
            transaction_open = False
            try:
                await transaction.start()
                transaction_open = True
                await blocker.fetchval(
                    "SELECT id FROM public.feeds WHERE id = $1 FOR UPDATE",
                    member.feed_id,
                )
                child_task = asyncio.create_task(
                    _commit(
                        store,
                        old_grant,
                        _batch(_progress(member, target_cursor)),
                    )
                )
                await _wait_for_blocked_query(
                    ingestion_lease_pool,
                    "FROM public.feeds",
                )
                transition_task = asyncio.create_task(store.release(old_grant))
                await _wait_for_blocked_query(
                    ingestion_lease_pool,
                    "FROM public.ingestion_leases",
                )
                await transaction.commit()
                transaction_open = False
                child_result, transition_result = await asyncio.wait_for(
                    asyncio.gather(child_task, transition_task),
                    timeout=_TIMEOUT_SECONDS,
                )
            finally:
                if transaction_open:
                    await transaction.rollback()
                await _cancel_tasks(child_task, transition_task)

        assert isinstance(child_result, ingestion_lease_store.BatchCommitted)
        assert child_result.children[0].disposition is (
            ingestion_lease_store.ChildDisposition.APPLIED
        )
        assert isinstance(
            transition_result,
            ingestion_lease_store.LeaseOperationResult,
        )
        assert transition_result.disposition is (
            ingestion_lease_store.LeaseOperationDisposition.APPLIED
        )
        new_grant = await _claim_exact(store, sid, uuid.uuid4())
    else:
        async with ingestion_lease_pool.acquire() as blocker:
            transaction = blocker.transaction(isolation="read_committed")
            transaction_open = False
            try:
                await transaction.start()
                transaction_open = True
                await blocker.fetchval(
                    """
                    SELECT lease_key
                    FROM public.ingestion_leases
                    WHERE source_type = 'bcfy_calls' AND lease_key = $1
                    FOR UPDATE
                    """,
                    sid,
                )
                if scenario == "transition_first_release":
                    transition_task = asyncio.create_task(
                        store.release(old_grant)
                    )
                else:
                    transition_task = asyncio.create_task(
                        store.finalize_failure(
                            old_grant,
                            ingestion_lease_store.NonBudgetedFailure(
                                datetime.datetime.now(datetime.UTC)
                                - datetime.timedelta(seconds=1)
                            ),
                            feed_store.FeedStatusReason.SOURCE_UNREACHABLE,
                            actor_id=_ACTOR_ID,
                        )
                    )
                await _wait_for_blocked_query(
                    ingestion_lease_pool,
                    "FROM public.ingestion_leases",
                )
                child_task = asyncio.create_task(
                    _commit(
                        store,
                        old_grant,
                        _batch(_progress(member, target_cursor)),
                    )
                )
                await _wait_for_blocked_query(
                    ingestion_lease_pool,
                    "FROM public.ingestion_leases",
                    minimum=2,
                )
                await transaction.commit()
                transaction_open = False
                transition_result, child_result = await asyncio.wait_for(
                    asyncio.gather(transition_task, child_task),
                    timeout=_TIMEOUT_SECONDS,
                )
            finally:
                if transaction_open:
                    await transaction.rollback()
                await _cancel_tasks(child_task, transition_task)

        assert isinstance(child_result, ingestion_lease_store.GrantRejected)
        durable_child = await _fetch_feed(
            ingestion_lease_pool,
            member.feed_id,
        )
        assert durable_child["last_bookmark_time"] is None
        if scenario == "transition_first_release":
            assert isinstance(
                transition_result,
                ingestion_lease_store.LeaseOperationResult,
            )
            assert transition_result.disposition is (
                ingestion_lease_store.LeaseOperationDisposition.APPLIED
            )
            new_grant = await _claim_exact(store, sid, uuid.uuid4())
        else:
            assert isinstance(
                transition_result,
                ingestion_lease_store.LeaseFailureResult,
            )
            assert transition_result.disposition is (
                ingestion_lease_store.LeaseOperationDisposition.APPLIED
            )
            new_grant = await _recover_exact(store, sid, uuid.uuid4())

    assert new_grant.fencing_token == old_grant.fencing_token + 1
    durable_before_stale_retry = await _fetch_feed(
        ingestion_lease_pool,
        member.feed_id,
    )
    stale_retry = await _commit(
        store,
        old_grant,
        _batch(
            _progress(
                member,
                target_cursor + datetime.timedelta(minutes=1),
            )
        ),
    )
    assert isinstance(stale_retry, ingestion_lease_store.GrantRejected)
    assert dict(
        await _fetch_feed(ingestion_lease_pool, member.feed_id)
    ) == dict(durable_before_stale_retry)


async def test_reverse_overlapping_feed_sets_do_not_deadlock(  # noqa: PLR0915
    ingestion_lease_pool: asyncpg.Pool,
) -> None:
    sid = _unique_digits()
    await _insert_lease(ingestion_lease_pool, sid)
    store = ingestion_lease_store.IngestionLeaseStore(ingestion_lease_pool)
    grant = await _claim_exact(store, sid, uuid.uuid4())
    first_member = await _insert_member(ingestion_lease_pool, grant)
    second_member = await _insert_member(ingestion_lease_pool, grant)
    ordered_members = sorted(
        (first_member, second_member),
        key=lambda member: member.feed_id.int,
    )
    low_member, high_member = ordered_members

    forged_sid = _unique_digits()
    await _insert_lease(ingestion_lease_pool, forged_sid)
    forged_grant = await _claim_exact(store, forged_sid, uuid.uuid4())
    forged_member = _forged_member_for_sid(
        low_member.feed_id,
        forged_sid,
        low_member.group_id,
    )
    forged_lease_before = dict(
        await _fetch_lease(ingestion_lease_pool, forged_sid)
    )
    feed_before_forgery = dict(
        await _fetch_feed(ingestion_lease_pool, low_member.feed_id)
    )
    audit_before_forgery = await _audit_rows(
        ingestion_lease_pool,
        low_member.feed_id,
    )
    checkout_forbidden_pool = mock.Mock(spec=asyncpg.Pool)
    forged_store = ingestion_lease_store.IngestionLeaseStore(
        typing.cast("asyncpg.Pool", checkout_forbidden_pool)
    )
    with pytest.raises(
        ValueError,
        match="binding was not issued by authoritative membership loading",
    ):
        await _commit(
            forged_store,
            forged_grant,
            _batch(_progress(forged_member, _BASE_CURSOR)),
        )
    checkout_forbidden_pool.acquire.assert_not_called()
    assert (
        dict(await _fetch_lease(ingestion_lease_pool, forged_sid))
        == forged_lease_before
    )
    assert (
        dict(await _fetch_feed(ingestion_lease_pool, low_member.feed_id))
        == feed_before_forgery
    )
    assert (
        await _audit_rows(ingestion_lease_pool, low_member.feed_id)
        == audit_before_forgery
    )

    first_cursor = _BASE_CURSOR + datetime.timedelta(minutes=1)
    second_cursor = _BASE_CURSOR + datetime.timedelta(minutes=2)
    first_task: asyncio.Task[object] | None = None
    second_task: asyncio.Task[object] | None = None
    async with ingestion_lease_pool.acquire() as blocker:
        transaction = blocker.transaction(isolation="read_committed")
        transaction_open = False
        try:
            await transaction.start()
            transaction_open = True
            await blocker.fetchval(
                "SELECT id FROM public.feeds WHERE id = $1 FOR UPDATE",
                low_member.feed_id,
            )
            first_task = asyncio.create_task(
                _commit(
                    store,
                    grant,
                    _batch(
                        _progress(high_member, first_cursor),
                        _progress(low_member, first_cursor),
                    ),
                )
            )
            await _wait_for_blocked_query(
                ingestion_lease_pool,
                "FROM public.feeds",
            )

            async with ingestion_lease_pool.acquire() as verifier:
                async with verifier.transaction(isolation="read_committed"):
                    lockable_high_feed = await verifier.fetchval(
                        """
                        SELECT id
                        FROM public.feeds
                        WHERE id = $1
                        FOR NO KEY UPDATE NOWAIT
                        """,
                        high_member.feed_id,
                    )
                    assert lockable_high_feed == high_member.feed_id

            second_task = asyncio.create_task(
                _commit(
                    store,
                    grant,
                    _batch(
                        _progress(low_member, second_cursor),
                        _progress(high_member, second_cursor),
                    ),
                )
            )
            await _wait_for_blocked_query(
                ingestion_lease_pool,
                "FROM public.ingestion_leases",
            )
            await transaction.commit()
            transaction_open = False
            first_result, second_result = await asyncio.wait_for(
                asyncio.gather(first_task, second_task),
                timeout=_TIMEOUT_SECONDS,
            )
        finally:
            if transaction_open:
                await transaction.rollback()
            await _cancel_tasks(first_task, second_task)

    assert isinstance(first_result, ingestion_lease_store.BatchCommitted)
    assert isinstance(second_result, ingestion_lease_store.BatchCommitted)
    assert [child.feed_id for child in first_result.children] == [
        high_member.feed_id,
        low_member.feed_id,
    ]
    assert [child.feed_id for child in second_result.children] == [
        low_member.feed_id,
        high_member.feed_id,
    ]
    assert [child.cursor_effect for child in first_result.children] == [
        ingestion_lease_store.CursorEffect.INITIALIZED,
        ingestion_lease_store.CursorEffect.INITIALIZED,
    ]
    assert [child.cursor_effect for child in second_result.children] == [
        ingestion_lease_store.CursorEffect.ADVANCED,
        ingestion_lease_store.CursorEffect.ADVANCED,
    ]
    assert all(
        child.disposition is ingestion_lease_store.ChildDisposition.APPLIED
        for child in first_result.children + second_result.children
    )
    for member in ordered_members:
        row = await _fetch_feed(ingestion_lease_pool, member.feed_id)
        assert row["last_bookmark_time"] == second_cursor


async def test_selective_child_dispositions_commit_valid_siblings(
    ingestion_lease_pool: asyncpg.Pool,
) -> None:
    sid = _unique_digits()
    await _insert_lease(ingestion_lease_pool, sid)
    store = ingestion_lease_store.IngestionLeaseStore(ingestion_lease_pool)
    grant = await _claim_exact(store, sid, uuid.uuid4())
    progress_member = await _insert_member(ingestion_lease_pool, grant)
    observation_member = await _insert_member(ingestion_lease_pool, grant)
    deactivated_member = await _insert_member(
        ingestion_lease_pool,
        grant,
        status="deactivated",
    )
    quarantined_member = await _insert_member(
        ingestion_lease_pool,
        grant,
        status="quarantined",
    )
    unclaimed_member = await _insert_member(
        ingestion_lease_pool,
        grant,
        status="unclaimed",
    )
    missing_member = _issued_member_for_grant(
        grant,
        uuid.uuid4(),
        _unique_digits(),
    )
    cursor = _BASE_CURSOR + datetime.timedelta(minutes=3)

    result = await _commit(
        store,
        grant,
        _batch(
            _progress(progress_member, cursor),
            _observation(observation_member, cursor),
            _progress(deactivated_member, cursor),
            _observation(missing_member, cursor),
            _observation(quarantined_member, cursor),
            _failure(unclaimed_member, cursor),
        ),
    )

    assert isinstance(result, ingestion_lease_store.BatchCommitted)
    assert [child.disposition for child in result.children] == [
        ingestion_lease_store.ChildDisposition.APPLIED,
        ingestion_lease_store.ChildDisposition.APPLIED,
        ingestion_lease_store.ChildDisposition.APPLIED_AFTER_DEACTIVATION,
        ingestion_lease_store.ChildDisposition.MISSING,
        ingestion_lease_store.ChildDisposition.STATUS_INELIGIBLE,
        ingestion_lease_store.ChildDisposition.STATUS_INELIGIBLE,
    ]
    assert (await _fetch_feed(ingestion_lease_pool, progress_member.feed_id))[
        "last_bookmark_time"
    ] == cursor
    assert (
        await _fetch_feed(ingestion_lease_pool, observation_member.feed_id)
    )["last_bookmark_time"] == cursor
    deactivated_row = await _fetch_feed(
        ingestion_lease_pool,
        deactivated_member.feed_id,
    )
    assert deactivated_row["status"] == "deactivated"
    assert deactivated_row["last_bookmark_time"] == cursor
    assert (
        await _fetch_feed(ingestion_lease_pool, quarantined_member.feed_id)
    )["last_bookmark_time"] is None
    assert (await _fetch_feed(ingestion_lease_pool, unclaimed_member.feed_id))[
        "failure_count"
    ] == 0


async def _deactivate_with_lease_lock(
    pool: asyncpg.Pool,
    sid: str,
    feed_id: uuid.UUID,
) -> None:
    """Model the required administrative Lease-to-Feed lock order."""
    async with pool.acquire() as connection:
        async with connection.transaction(isolation="read_committed"):
            await connection.fetchval(
                """
                SELECT lease_key
                FROM public.ingestion_leases
                WHERE source_type = 'bcfy_calls' AND lease_key = $1
                FOR NO KEY UPDATE
                """,
                sid,
            )
            await connection.execute(
                """
                UPDATE public.feeds
                SET status = 'deactivated'::public.feed_status
                WHERE id = $1
                """,
                feed_id,
            )


async def test_deactivation_races_and_revision_is_not_fence(  # noqa: PLR0915
    ingestion_lease_pool: asyncpg.Pool,
) -> None:
    store = ingestion_lease_store.IngestionLeaseStore(ingestion_lease_pool)
    target_cursor = _BASE_CURSOR + datetime.timedelta(minutes=4)

    for transition_first in (False, True):
        sid = _unique_digits()
        await _insert_lease(ingestion_lease_pool, sid)
        grant = await _claim_exact(store, sid, uuid.uuid4())
        member = await _insert_member(ingestion_lease_pool, grant)
        child_task: asyncio.Task[object] | None = None
        admin_task: asyncio.Task[object] | None = None

        if transition_first:
            async with ingestion_lease_pool.acquire() as blocker:
                transaction = blocker.transaction(isolation="read_committed")
                transaction_open = False
                try:
                    await transaction.start()
                    transaction_open = True
                    await blocker.fetchval(
                        """
                        SELECT lease_key
                        FROM public.ingestion_leases
                        WHERE source_type = 'bcfy_calls' AND lease_key = $1
                        FOR UPDATE
                        """,
                        sid,
                    )
                    await blocker.execute(
                        """
                        UPDATE public.feeds
                        SET status = 'deactivated'::public.feed_status
                        WHERE id = $1
                        """,
                        member.feed_id,
                    )
                    child_task = asyncio.create_task(
                        _commit(
                            store,
                            grant,
                            _batch(_progress(member, target_cursor)),
                        )
                    )
                    await _wait_for_blocked_query(
                        ingestion_lease_pool,
                        "FROM public.ingestion_leases",
                    )
                    await transaction.commit()
                    transaction_open = False
                    child_result = await asyncio.wait_for(
                        child_task,
                        timeout=_TIMEOUT_SECONDS,
                    )
                finally:
                    if transaction_open:
                        await transaction.rollback()
                    await _cancel_tasks(child_task)
            assert isinstance(
                child_result,
                ingestion_lease_store.BatchCommitted,
            )
            assert child_result.children[0].disposition is (
                ingestion_lease_store.ChildDisposition.APPLIED_AFTER_DEACTIVATION
            )
        else:
            async with ingestion_lease_pool.acquire() as blocker:
                transaction = blocker.transaction(isolation="read_committed")
                transaction_open = False
                try:
                    await transaction.start()
                    transaction_open = True
                    await blocker.fetchval(
                        "SELECT id FROM public.feeds WHERE id = $1 FOR UPDATE",
                        member.feed_id,
                    )
                    child_task = asyncio.create_task(
                        _commit(
                            store,
                            grant,
                            _batch(_progress(member, target_cursor)),
                        )
                    )
                    await _wait_for_blocked_query(
                        ingestion_lease_pool,
                        "FROM public.feeds",
                    )
                    admin_task = asyncio.create_task(
                        _deactivate_with_lease_lock(
                            ingestion_lease_pool,
                            sid,
                            member.feed_id,
                        )
                    )
                    await _wait_for_blocked_query(
                        ingestion_lease_pool,
                        "FROM public.ingestion_leases",
                    )
                    await transaction.commit()
                    transaction_open = False
                    child_result, _admin_result = await asyncio.wait_for(
                        asyncio.gather(child_task, admin_task),
                        timeout=_TIMEOUT_SECONDS,
                    )
                finally:
                    if transaction_open:
                        await transaction.rollback()
                    await _cancel_tasks(child_task, admin_task)
            assert isinstance(
                child_result,
                ingestion_lease_store.BatchCommitted,
            )
            assert child_result.children[0].disposition is (
                ingestion_lease_store.ChildDisposition.APPLIED
            )

        deactivated_row = await _fetch_feed(
            ingestion_lease_pool,
            member.feed_id,
        )
        assert deactivated_row["status"] == "deactivated"
        assert deactivated_row["last_bookmark_time"] == target_cursor
        before_rejections = dict(deactivated_row)
        observation = await _commit(
            store,
            grant,
            _batch(
                _observation(
                    member,
                    target_cursor + datetime.timedelta(minutes=1),
                )
            ),
        )
        failure = await _commit(
            store,
            grant,
            _batch(
                _failure(
                    member,
                    target_cursor + datetime.timedelta(minutes=1),
                )
            ),
        )
        assert isinstance(observation, ingestion_lease_store.BatchCommitted)
        assert isinstance(failure, ingestion_lease_store.BatchCommitted)
        assert observation.children[0].disposition is (
            ingestion_lease_store.ChildDisposition.STATUS_INELIGIBLE
        )
        assert failure.children[0].disposition is (
            ingestion_lease_store.ChildDisposition.STATUS_INELIGIBLE
        )
        assert (
            dict(await _fetch_feed(ingestion_lease_pool, member.feed_id))
            == before_rejections
        )

    revision_sid = _unique_digits()
    await _insert_lease(ingestion_lease_pool, revision_sid)
    revision_grant = await _claim_exact(store, revision_sid, uuid.uuid4())
    revision_member = await _insert_member(
        ingestion_lease_pool,
        revision_grant,
    )
    before_snapshot = await store.load_membership(revision_grant)
    assert isinstance(
        before_snapshot,
        ingestion_lease_store.MembershipSnapshot,
    )

    revision_task: asyncio.Task[object] | None = None
    async with ingestion_lease_pool.acquire() as blocker:
        transaction = blocker.transaction(isolation="read_committed")
        transaction_open = False
        try:
            await transaction.start()
            transaction_open = True
            await blocker.fetchval(
                """
                SELECT lease_key
                FROM public.ingestion_leases
                WHERE source_type = 'bcfy_calls' AND lease_key = $1
                FOR UPDATE
                """,
                revision_sid,
            )
            await blocker.execute(
                """
                UPDATE public.ingestion_leases
                SET membership_revision = membership_revision + 1,
                    updated_at = NOW()
                WHERE source_type = 'bcfy_calls' AND lease_key = $1
                """,
                revision_sid,
            )
            revision_task = asyncio.create_task(
                _commit(
                    store,
                    revision_grant,
                    _batch(_progress(revision_member, target_cursor)),
                )
            )
            await _wait_for_blocked_query(
                ingestion_lease_pool,
                "FROM public.ingestion_leases",
            )
            await transaction.commit()
            transaction_open = False
            revision_result = await asyncio.wait_for(
                revision_task,
                timeout=_TIMEOUT_SECONDS,
            )
        finally:
            if transaction_open:
                await transaction.rollback()
            await _cancel_tasks(revision_task)

    assert isinstance(revision_result, ingestion_lease_store.BatchCommitted)
    assert revision_result.children[0].disposition is (
        ingestion_lease_store.ChildDisposition.APPLIED
    )
    after_snapshot = await store.load_membership(revision_grant)
    assert isinstance(after_snapshot, ingestion_lease_store.MembershipSnapshot)
    assert after_snapshot.grant == before_snapshot.grant
    assert (
        after_snapshot.membership_revision
        == before_snapshot.membership_revision + 1
    )


def _expected_cursor(
    current: datetime.datetime | None,
    requested: datetime.datetime | None,
) -> datetime.datetime | None:
    if current is None:
        return requested
    if requested is None:
        return current
    return max(current, requested)


@pytest.mark.parametrize("operation", ["progress", "observation"])
@pytest.mark.parametrize(
    ("current", "requested", "expected_effect"),
    [
        pytest.param(
            None,
            _BASE_CURSOR,
            ingestion_lease_store.CursorEffect.INITIALIZED,
            id="initialize",
        ),
        pytest.param(
            _BASE_CURSOR,
            _BASE_CURSOR + datetime.timedelta(seconds=1),
            ingestion_lease_store.CursorEffect.ADVANCED,
            id="advance",
        ),
        pytest.param(
            _BASE_CURSOR,
            _BASE_CURSOR,
            ingestion_lease_store.CursorEffect.EQUAL,
            id="equal",
        ),
        pytest.param(
            _BASE_CURSOR,
            _BASE_CURSOR - datetime.timedelta(seconds=1),
            ingestion_lease_store.CursorEffect.REGRESSIVE,
            id="regressive",
        ),
        pytest.param(
            _BASE_CURSOR,
            None,
            ingestion_lease_store.CursorEffect.ABSENT,
            id="absent",
        ),
    ],
)
async def test_cursor_matrix_is_independent_from_lifecycle(
    ingestion_lease_pool: asyncpg.Pool,
    operation: str,
    current: datetime.datetime | None,
    requested: datetime.datetime | None,
    expected_effect: ingestion_lease_store.CursorEffect,
) -> None:
    sid = _unique_digits()
    await _insert_lease(ingestion_lease_pool, sid)
    store = ingestion_lease_store.IngestionLeaseStore(ingestion_lease_pool)
    grant = await _claim_exact(store, sid, uuid.uuid4())
    clean_member = await _insert_member(
        ingestion_lease_pool,
        grant,
        cursor=current,
    )
    failing_member = await _insert_member(
        ingestion_lease_pool,
        grant,
        status="failing",
        cursor=current,
        failure_count=2,
        retry_after=datetime.datetime.now(datetime.UTC)
        + datetime.timedelta(minutes=1),
        status_reason=feed_store.FeedStatusReason.SOURCE_UNREACHABLE.value,
    )
    deactivated_member = await _insert_member(
        ingestion_lease_pool,
        grant,
        status="deactivated",
        cursor=current,
        failure_count=2,
        retry_after=datetime.datetime.now(datetime.UTC)
        + datetime.timedelta(minutes=1),
        status_reason=feed_store.FeedStatusReason.SOURCE_UNREACHABLE.value,
    )

    def _command(
        member: ingestion_lease_store.LeaseMemberIdentity,
    ) -> ingestion_lease_store.ChildMutation:
        if operation == "progress":
            return _progress(member, requested)
        return _observation(member, requested)

    clean = await _commit(store, grant, _batch(_command(clean_member)))
    failing = await _commit(store, grant, _batch(_command(failing_member)))
    deactivated = await _commit(
        store,
        grant,
        _batch(_command(deactivated_member)),
    )

    assert isinstance(clean, ingestion_lease_store.BatchCommitted)
    assert isinstance(failing, ingestion_lease_store.BatchCommitted)
    assert isinstance(deactivated, ingestion_lease_store.BatchCommitted)
    assert clean.children[0].cursor_effect is expected_effect
    assert failing.children[0].cursor_effect is expected_effect
    assert deactivated.children[0].cursor_effect is expected_effect

    expected_cursor = _expected_cursor(current, requested)
    clean_row = await _fetch_feed(
        ingestion_lease_pool,
        clean_member.feed_id,
    )
    failing_row = await _fetch_feed(
        ingestion_lease_pool,
        failing_member.feed_id,
    )
    deactivated_row = await _fetch_feed(
        ingestion_lease_pool,
        deactivated_member.feed_id,
    )
    assert clean_row["last_bookmark_time"] == expected_cursor
    assert failing_row["last_bookmark_time"] == expected_cursor
    assert failing_row["status"] == "active"
    assert failing_row["failure_count"] == 0
    assert failing.children[0].lifecycle_effect is (
        ingestion_lease_store.LifecycleEffect.RECOVERED
    )
    assert (
        len(await _audit_rows(ingestion_lease_pool, failing_member.feed_id))
        == 1
    )

    if operation == "progress":
        assert deactivated.children[0].disposition is (
            ingestion_lease_store.ChildDisposition.APPLIED_AFTER_DEACTIVATION
        )
        assert deactivated.children[0].lifecycle_effect is (
            ingestion_lease_store.LifecycleEffect.CLEARED_WHILE_DEACTIVATED
        )
        assert deactivated_row["status"] == "deactivated"
        assert deactivated_row["failure_count"] == 0
        assert deactivated_row["last_bookmark_time"] == expected_cursor
    else:
        assert deactivated.children[0].disposition is (
            ingestion_lease_store.ChildDisposition.STATUS_INELIGIBLE
        )
        assert deactivated.children[0].lifecycle_effect is (
            ingestion_lease_store.LifecycleEffect.NONE
        )
        assert deactivated_row["status"] == "deactivated"
        assert deactivated_row["failure_count"] == 2
        assert deactivated_row["last_bookmark_time"] == current


async def test_budgeted_failure_threshold_quarantines_retained_count(
    ingestion_lease_pool: asyncpg.Pool,
) -> None:
    sid = _unique_digits()
    await _insert_lease(ingestion_lease_pool, sid)
    store = ingestion_lease_store.IngestionLeaseStore(ingestion_lease_pool)
    grant = await _claim_exact(store, sid, uuid.uuid4())
    cursor = _BASE_CURSOR + datetime.timedelta(minutes=5)
    member = await _insert_member(
        ingestion_lease_pool,
        grant,
        status="failing",
        cursor=_BASE_CURSOR,
        failure_count=4,
        retry_after=datetime.datetime.now(datetime.UTC)
        + datetime.timedelta(minutes=1),
        status_reason=feed_store.FeedStatusReason.SOURCE_UNREACHABLE.value,
    )
    lease_before = dict(await _fetch_lease(ingestion_lease_pool, sid))

    result = await _commit(
        store,
        grant,
        _batch(_failure(member, cursor, budgeted=True)),
    )
    feed_after = dict(await _fetch_feed(ingestion_lease_pool, member.feed_id))
    lease_after = dict(await _fetch_lease(ingestion_lease_pool, sid))
    events_after = await _audit_rows(
        ingestion_lease_pool,
        member.feed_id,
    )

    assert isinstance(result, ingestion_lease_store.BatchCommitted)
    assert result.children[0].disposition is (
        ingestion_lease_store.ChildDisposition.APPLIED
    )
    assert result.children[0].cursor_effect is (
        ingestion_lease_store.CursorEffect.ADVANCED
    )
    assert result.children[0].lifecycle_effect is (
        ingestion_lease_store.LifecycleEffect.QUARANTINED
    )
    assert feed_after["status"] == "quarantined"
    assert feed_after["failure_count"] == 5
    assert feed_after["retry_after"] is None
    assert feed_after["audit_revision"] == 1
    assert feed_after["status_reason"] == (
        feed_store.FeedStatusReason.SYSTEM_CONFIGURATION_INVALID.value
    )
    assert lease_after == lease_before
    assert len(events_after) == 1
    assert events_after[0]["action"] == "feed.quarantined"
    assert events_after[0]["feed_revision"] == 1
    assert events_after[0]["before_status"] == "failing"
    assert events_after[0]["before_failure_count"] == 4
    assert events_after[0]["after_status"] == "quarantined"
    assert events_after[0]["after_failure_count"] == 5


@pytest.mark.parametrize(
    "boundary_kind",
    [
        "nonterminal_budgeted_failure",
        "retained_non_budgeted_failure",
        "finalized_recovery",
    ],
)
async def test_boundary_retry_does_not_double_charge(  # noqa: PLR0915
    ingestion_lease_pool: asyncpg.Pool,
    boundary_kind: str,
) -> None:
    sid = _unique_digits()
    initial_lease_failure_count = (
        2 if boundary_kind == "finalized_recovery" else 0
    )
    initial_lease_reason = (
        feed_store.FeedStatusReason.SOURCE_UNREACHABLE.value
        if boundary_kind == "finalized_recovery"
        else None
    )
    await _insert_lease(
        ingestion_lease_pool,
        sid,
        failure_count=initial_lease_failure_count,
        status_reason=initial_lease_reason,
    )
    store = ingestion_lease_store.IngestionLeaseStore(ingestion_lease_pool)
    grant = await _claim_exact(store, sid, uuid.uuid4())
    cursor = _BASE_CURSOR + datetime.timedelta(minutes=5)
    requested_retry_after: datetime.datetime | None = None

    if boundary_kind == "finalized_recovery":
        member = await _insert_member(
            ingestion_lease_pool,
            grant,
            status="failing",
            cursor=_BASE_CURSOR,
            failure_count=2,
            retry_after=datetime.datetime.now(datetime.UTC)
            + datetime.timedelta(minutes=1),
            status_reason=(
                feed_store.FeedStatusReason.SOURCE_UNREACHABLE.value
            ),
        )
        batch = _batch(
            _observation(member, cursor),
            lease_effect=ingestion_lease_store.FinalizeLeaseRecovery(),
        )
    else:
        initial_feed_failure_count = (
            0 if boundary_kind == "nonterminal_budgeted_failure" else 4
        )
        member = await _insert_member(
            ingestion_lease_pool,
            grant,
            status="failing",
            cursor=_BASE_CURSOR,
            failure_count=initial_feed_failure_count,
            retry_after=datetime.datetime.now(datetime.UTC)
            + datetime.timedelta(minutes=1),
            status_reason=(
                feed_store.FeedStatusReason.SOURCE_UNREACHABLE.value
            ),
        )
        failure = _failure(
            member,
            cursor,
            budgeted=boundary_kind == "nonterminal_budgeted_failure",
        )
        if isinstance(
            failure.action,
            ingestion_lease_store.NonBudgetedFailure,
        ):
            requested_retry_after = failure.action.retry_after
        batch = _batch(
            failure,
        )

    first = await _commit(store, grant, batch)
    feed_after_first = dict(
        await _fetch_feed(ingestion_lease_pool, member.feed_id)
    )
    lease_after_first = dict(await _fetch_lease(ingestion_lease_pool, sid))
    events_after_first = await _audit_rows(
        ingestion_lease_pool,
        member.feed_id,
    )
    second = await _commit(store, grant, batch)
    feed_after_second = dict(
        await _fetch_feed(ingestion_lease_pool, member.feed_id)
    )
    lease_after_second = dict(await _fetch_lease(ingestion_lease_pool, sid))
    events_after_second = await _audit_rows(
        ingestion_lease_pool,
        member.feed_id,
    )

    assert isinstance(first, ingestion_lease_store.BatchCommitted)
    assert isinstance(second, ingestion_lease_store.BatchCommitted)
    assert feed_after_second == feed_after_first
    assert lease_after_second == lease_after_first
    assert events_after_second == events_after_first
    assert len(events_after_first) == 1
    assert second.children[0].cursor_effect is (
        ingestion_lease_store.CursorEffect.EQUAL
    )
    if boundary_kind == "finalized_recovery":
        assert first.lease_effect.effect is (
            ingestion_lease_store.LeaseLifecycleEffect.RECOVERED
        )
        assert second.lease_effect.effect is (
            ingestion_lease_store.LeaseLifecycleEffect.NONE
        )
        assert feed_after_first["failure_count"] == 0
        assert lease_after_first["failure_count"] == 0
    else:
        assert first.children[0].disposition is (
            ingestion_lease_store.ChildDisposition.APPLIED
        )
        assert feed_after_first["audit_revision"] == 1
        assert [event["feed_revision"] for event in events_after_first] == [1]
        assert events_after_first[0]["before_status"] == "failing"
        assert (
            events_after_first[0]["before_failure_count"]
            == initial_feed_failure_count
        )
        assert lease_after_first["audit_revision"] == 0
        assert second.children[0].disposition is (
            ingestion_lease_store.ChildDisposition.ACCEPTED_NOOP
        )
        if boundary_kind == "nonterminal_budgeted_failure":
            assert first.children[0].lifecycle_effect is (
                ingestion_lease_store.LifecycleEffect.FAILURE_RECORDED
            )
            assert feed_after_first["status"] == "failing"
            assert feed_after_first["failure_count"] == 1
            assert feed_after_first["retry_after"] is not None
            assert events_after_first[0]["after_status"] == "failing"
            assert events_after_first[0]["after_failure_count"] == 1
            assert feed_after_first["status_reason"] == (
                feed_store.FeedStatusReason.SYSTEM_CONFIGURATION_INVALID.value
            )
            assert [event["action"] for event in events_after_first] == [
                "feed.failure_reported"
            ]
        else:
            assert first.children[0].lifecycle_effect is (
                ingestion_lease_store.LifecycleEffect.FAILURE_RECORDED
            )
            assert requested_retry_after is not None
            assert feed_after_first["status"] == "failing"
            assert feed_after_first["failure_count"] == 0
            assert feed_after_first["retry_after"] == requested_retry_after
            assert events_after_first[0]["after_status"] == "failing"
            assert events_after_first[0]["after_failure_count"] == 0
            assert feed_after_first["status_reason"] == (
                feed_store.FeedStatusReason.SYSTEM_PIPELINE_ERROR.value
            )
            assert [event["action"] for event in events_after_first] == [
                "feed.failure_reported"
            ]


async def _insert_conflicting_audit_event(
    pool: asyncpg.Pool,
    feed_id: uuid.UUID,
) -> None:
    await pool.execute(
        """
        INSERT INTO public.feed_audit_events (
            feed_id,
            action,
            actor_id,
            feed_revision,
            before_values,
            after_values
        ) VALUES (
            $1,
            'feed.failure_reported',
            $2,
            1,
            '{}'::jsonb,
            '{}'::jsonb
        )
        """,
        feed_id,
        _ACTOR_ID,
    )


async def test_audit_failure_rolls_back_batch_and_lease_effect(
    ingestion_lease_pool: asyncpg.Pool,
) -> None:
    sid = _unique_digits()
    await _insert_lease(
        ingestion_lease_pool,
        sid,
        failure_count=2,
        status_reason=feed_store.FeedStatusReason.SOURCE_UNREACHABLE.value,
    )
    store = ingestion_lease_store.IngestionLeaseStore(ingestion_lease_pool)
    grant = await _claim_exact(store, sid, uuid.uuid4())
    audited_member = await _insert_member(
        ingestion_lease_pool,
        grant,
        status="failing",
        failure_count=2,
        status_reason=feed_store.FeedStatusReason.SOURCE_UNREACHABLE.value,
    )
    sibling_member = await _insert_member(ingestion_lease_pool, grant)
    await _insert_conflicting_audit_event(
        ingestion_lease_pool,
        audited_member.feed_id,
    )
    lease_before = dict(await _fetch_lease(ingestion_lease_pool, sid))
    audited_before = dict(
        await _fetch_feed(ingestion_lease_pool, audited_member.feed_id)
    )
    sibling_before = dict(
        await _fetch_feed(ingestion_lease_pool, sibling_member.feed_id)
    )

    with mock.patch(
        "backend.pipeline.storage.ingestion_lease_store."
        "feed_change_notifications.emit_feed_change_notification"
    ) as emit:
        with pytest.raises(asyncpg.UniqueViolationError):
            await _commit(
                store,
                grant,
                _batch(
                    _progress(audited_member, _BASE_CURSOR),
                    _progress(sibling_member, _BASE_CURSOR),
                    lease_effect=(
                        ingestion_lease_store.FinalizeLeaseRecovery()
                    ),
                ),
            )

    emit.assert_not_called()
    assert dict(await _fetch_lease(ingestion_lease_pool, sid)) == lease_before
    assert (
        dict(await _fetch_feed(ingestion_lease_pool, audited_member.feed_id))
        == audited_before
    )
    assert (
        dict(await _fetch_feed(ingestion_lease_pool, sibling_member.feed_id))
        == sibling_before
    )
    assert (
        len(await _audit_rows(ingestion_lease_pool, audited_member.feed_id))
        == 1
    )


async def test_cancellation_while_blocked_rolls_back_without_notification(  # noqa: PLR0915
    ingestion_lease_pool: asyncpg.Pool,
) -> None:
    sid = _unique_digits()
    retry_after = datetime.datetime.now(datetime.UTC) + datetime.timedelta(
        minutes=1
    )
    await _insert_lease(
        ingestion_lease_pool,
        sid,
        failure_count=2,
        retry_after=retry_after,
        status_reason=feed_store.FeedStatusReason.SOURCE_UNREACHABLE.value,
        audit_revision=4,
    )
    store = ingestion_lease_store.IngestionLeaseStore(ingestion_lease_pool)
    grant = await _claim_exact(store, sid, uuid.uuid4())
    member = await _insert_member(
        ingestion_lease_pool,
        grant,
        status="failing",
        failure_count=2,
        retry_after=retry_after,
        status_reason=feed_store.FeedStatusReason.SOURCE_UNREACHABLE.value,
        audit_revision=3,
    )
    lease_before = dict(await _fetch_lease(ingestion_lease_pool, sid))
    feed_before = dict(await _fetch_feed(ingestion_lease_pool, member.feed_id))
    audit_before = await _audit_rows(ingestion_lease_pool, member.feed_id)
    assert lease_before["failure_count"] == 2
    assert lease_before["audit_revision"] == 4
    assert feed_before["status"] == "failing"
    assert feed_before["failure_count"] == 2
    assert feed_before["audit_revision"] == 3
    assert audit_before == []
    child_task: asyncio.Task[object] | None = None
    lease_effect_applied = asyncio.Event()
    original_apply_lease_effect = store._apply_lease_effect

    async def _observe_lease_effect(
        connection: asyncpg.Connection,
        effect_grant: ingestion_lease_store.LeaseGrant,
        effect: ingestion_lease_store.LeaseEffect,
        before_snapshot: ingestion_lease_store.LeaseSnapshot,
    ) -> ingestion_lease_store.LeaseLifecycleResult:
        result = await original_apply_lease_effect(
            connection,
            effect_grant,
            effect,
            before_snapshot,
        )
        assert result.effect is (
            ingestion_lease_store.LeaseLifecycleEffect.RECOVERED
        )
        lease_effect_applied.set()
        return result

    async with ingestion_lease_pool.acquire() as blocker:
        transaction = blocker.transaction(isolation="read_committed")
        transaction_open = False
        try:
            await transaction.start()
            transaction_open = True
            await blocker.execute(
                "LOCK TABLE public.feed_properties IN ACCESS EXCLUSIVE MODE"
            )
            with (
                mock.patch.object(
                    store,
                    "_apply_lease_effect",
                    side_effect=_observe_lease_effect,
                ) as apply_lease_effect,
                mock.patch(
                    "backend.pipeline.storage.ingestion_lease_store."
                    "feed_change_notifications.emit_feed_change_notification"
                ) as emit,
            ):
                child_task = asyncio.create_task(
                    _commit(
                        store,
                        grant,
                        _batch(
                            _progress(member, _BASE_CURSOR),
                            lease_effect=(
                                ingestion_lease_store.FinalizeLeaseRecovery()
                            ),
                        ),
                    )
                )
                await _wait_for_blocked_query(
                    ingestion_lease_pool,
                    "FROM public.feed_properties",
                )
                await asyncio.wait_for(
                    lease_effect_applied.wait(),
                    timeout=_TIMEOUT_SECONDS,
                )
                apply_lease_effect.assert_awaited_once()
                blocked_backend_xid = await ingestion_lease_pool.fetchval(
                    """
                    SELECT activity.backend_xid::text
                    FROM pg_catalog.pg_stat_activity AS activity
                    WHERE activity.datname = current_database()
                      AND activity.pid <> pg_backend_pid()
                      AND activity.wait_event_type = 'Lock'
                      AND pg_catalog.strpos(
                          activity.query,
                          'FROM public.feed_properties'
                      ) > 0
                    LIMIT 1
                    """
                )
                assert blocked_backend_xid is not None

                child_task.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await child_task

                remaining_waiters = await ingestion_lease_pool.fetchval(
                    """
                    SELECT COUNT(*)
                    FROM pg_catalog.pg_stat_activity AS activity
                    WHERE activity.datname = current_database()
                      AND activity.pid <> pg_backend_pid()
                      AND activity.wait_event_type = 'Lock'
                      AND pg_catalog.strpos(
                          activity.query,
                          'FROM public.feed_properties'
                      ) > 0
                    """
                )
                assert remaining_waiters == 0
                async with ingestion_lease_pool.acquire() as verifier:
                    async with verifier.transaction(isolation="read_committed"):
                        await verifier.execute("SET LOCAL lock_timeout = '2s'")
                        locked_sid = await verifier.fetchval(
                            """
                            SELECT lease_key
                            FROM public.ingestion_leases
                            WHERE source_type = 'bcfy_calls'
                              AND lease_key = $1
                            FOR NO KEY UPDATE
                            """,
                            sid,
                        )
                        assert locked_sid == sid
                        locked_feed_id = await verifier.fetchval(
                            """
                            SELECT id
                            FROM public.feeds
                            WHERE id = $1
                            FOR NO KEY UPDATE
                            """,
                            member.feed_id,
                        )
                        assert locked_feed_id == member.feed_id

                assert (
                    dict(await _fetch_lease(ingestion_lease_pool, sid))
                    == lease_before
                )
                assert (
                    dict(
                        await _fetch_feed(
                            ingestion_lease_pool,
                            member.feed_id,
                        )
                    )
                    == feed_before
                )
                assert (
                    await _audit_rows(
                        ingestion_lease_pool,
                        member.feed_id,
                    )
                    == audit_before
                )
                emit.assert_not_called()
        finally:
            if transaction_open:
                await transaction.rollback()
            await _cancel_tasks(child_task)

    async with ingestion_lease_pool.acquire() as verifier:
        async with verifier.transaction(isolation="read_committed"):
            await verifier.execute("SET LOCAL lock_timeout = '2s'")
            await verifier.execute(
                "LOCK TABLE public.feed_properties IN ACCESS EXCLUSIVE MODE"
            )

    assert dict(await _fetch_lease(ingestion_lease_pool, sid)) == lease_before
    assert (
        dict(await _fetch_feed(ingestion_lease_pool, member.feed_id))
        == feed_before
    )
    assert (
        await _audit_rows(ingestion_lease_pool, member.feed_id) == audit_before
    )


async def test_notification_runs_after_commit_visibility(
    ingestion_lease_pool: asyncpg.Pool,
) -> None:
    sid = _unique_digits()
    await _insert_lease(ingestion_lease_pool, sid)
    store = ingestion_lease_store.IngestionLeaseStore(ingestion_lease_pool)
    grant = await _claim_exact(store, sid, uuid.uuid4())
    member = await _insert_member(
        ingestion_lease_pool,
        grant,
        status="failing",
        failure_count=2,
        status_reason=feed_store.FeedStatusReason.SOURCE_UNREACHABLE.value,
    )
    observed_tasks: list[asyncio.Task[tuple[str, int, int]]] = []

    async with ingestion_lease_pool.acquire() as observer:

        async def _observe_committed() -> tuple[str, int, int]:
            row = await observer.fetchrow(
                """
                SELECT
                    feeds.status::text AS status,
                    feeds.audit_revision,
                    COUNT(feed_audit_events.id)::integer AS event_count
                FROM public.feeds
                LEFT JOIN public.feed_audit_events
                  ON feed_audit_events.feed_id = feeds.id
                WHERE feeds.id = $1
                GROUP BY feeds.id
                """,
                member.feed_id,
            )
            assert row is not None
            return (
                row["status"],
                row["audit_revision"],
                row["event_count"],
            )

        def _notification_hook(_payload: object) -> None:
            observed_tasks.append(asyncio.create_task(_observe_committed()))

        with mock.patch(
            "backend.pipeline.storage.ingestion_lease_store."
            "feed_change_notifications.emit_feed_change_notification",
            side_effect=_notification_hook,
        ) as emit:
            committed = await _commit(
                store,
                grant,
                _batch(_progress(member, _BASE_CURSOR)),
            )
            assert isinstance(
                committed,
                ingestion_lease_store.BatchCommitted,
            )
            assert len(observed_tasks) == 1
            observed = await asyncio.wait_for(
                observed_tasks[0],
                timeout=_TIMEOUT_SECONDS,
            )
            assert observed == ("active", 1, 1)

            rollback_member = await _insert_member(
                ingestion_lease_pool,
                grant,
                status="failing",
                failure_count=2,
                status_reason=(
                    feed_store.FeedStatusReason.SOURCE_UNREACHABLE.value
                ),
            )
            await _insert_conflicting_audit_event(
                ingestion_lease_pool,
                rollback_member.feed_id,
            )
            with pytest.raises(asyncpg.UniqueViolationError):
                await _commit(
                    store,
                    grant,
                    _batch(_progress(rollback_member, _BASE_CURSOR)),
                )

        emit.assert_called_once()
        assert len(observed_tasks) == 1


async def test_clean_paths_do_not_touch_feed_properties(
    ingestion_lease_pool: asyncpg.Pool,
) -> None:
    sid = _unique_digits()
    await _insert_lease(ingestion_lease_pool, sid)
    store = ingestion_lease_store.IngestionLeaseStore(ingestion_lease_pool)
    grant = await _claim_exact(store, sid, uuid.uuid4())
    progress_member = await _insert_member(ingestion_lease_pool, grant)
    observation_member = await _insert_member(ingestion_lease_pool, grant)
    recovery_member = await _insert_member(
        ingestion_lease_pool,
        grant,
        status="failing",
        failure_count=2,
        status_reason=feed_store.FeedStatusReason.SOURCE_UNREACHABLE.value,
    )
    clean_cursor = _BASE_CURSOR + datetime.timedelta(minutes=6)

    async with ingestion_lease_pool.acquire() as blocker:
        transaction = blocker.transaction(isolation="read_committed")
        transaction_open = False
        try:
            await transaction.start()
            transaction_open = True
            await blocker.execute(
                "LOCK TABLE public.feed_properties IN ACCESS EXCLUSIVE MODE"
            )
            clean_result = await asyncio.wait_for(
                store.commit_child_mutations(
                    grant,
                    _batch(
                        _progress(progress_member, clean_cursor),
                        _observation(observation_member, clean_cursor),
                    ),
                    actor_id=_ACTOR_ID,
                ),
                timeout=_TIMEOUT_SECONDS,
            )
            await transaction.commit()
            transaction_open = False
        finally:
            if transaction_open:
                await transaction.rollback()

    assert isinstance(clean_result, ingestion_lease_store.BatchCommitted)
    assert [child.disposition for child in clean_result.children] == [
        ingestion_lease_store.ChildDisposition.APPLIED,
        ingestion_lease_store.ChildDisposition.APPLIED,
    ]
    assert (
        await _audit_rows(
            ingestion_lease_pool,
            progress_member.feed_id,
        )
        == []
    )
    assert (
        await _audit_rows(
            ingestion_lease_pool,
            observation_member.feed_id,
        )
        == []
    )

    recovery = await _commit(
        store,
        grant,
        _batch(_progress(recovery_member, clean_cursor)),
    )
    assert isinstance(recovery, ingestion_lease_store.BatchCommitted)
    assert recovery.children[0].lifecycle_effect is (
        ingestion_lease_store.LifecycleEffect.RECOVERED
    )
    events = await _audit_rows(
        ingestion_lease_pool,
        recovery_member.feed_id,
    )
    assert [event["action"] for event in events] == ["feed.recovered"]
