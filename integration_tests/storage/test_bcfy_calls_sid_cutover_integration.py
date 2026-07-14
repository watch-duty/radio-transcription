"""External-PostgreSQL proofs for the controlled Calls SID handoff."""

from __future__ import annotations

import asyncio
import datetime
import os
import pathlib
import shutil
import subprocess
import typing
import uuid

import asyncpg
import pytest
import pytest_asyncio

from backend.pipeline.storage import feed_store, ingestion_lease_store

if typing.TYPE_CHECKING:
    import collections.abc

pytestmark = pytest.mark.asyncio(loop_scope="module")

_ACTOR_ID = "service_account:gcp:phase7-cutover-integration"
_OPERATION_ROOT = pathlib.Path(
    "terraform/modules/alloydb/sql/operations/bcfy_calls_sid"
)
_SUPPORTED_POSTGRES_MAJORS = frozenset({"15", "16", "17"})
_SOURCE_TYPE = feed_store.SourceType.BCFY_CALLS


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def cutover_pool() -> collections.abc.AsyncIterator[asyncpg.Pool]:
    """Connect only to a disposable database supplied by the serial gate."""
    dsn = os.environ.get("INGESTION_CUTOVER_TEST_DSN")
    required = (
        os.environ.get("INGESTION_CUTOVER_EXTERNAL_POSTGRES_REQUIRED") == "1"
    )
    if not dsn:
        if required:
            pytest.fail("INGESTION_CUTOVER_TEST_DSN is required")
        pytest.skip("external PostgreSQL cutover DSN is not configured")
    if os.environ.get("PYTEST_XDIST_WORKER"):
        pytest.fail("the cutover PostgreSQL module must run serially")

    pool = await asyncpg.create_pool(
        dsn=dsn,
        min_size=1,
        max_size=4,
        statement_cache_size=0,
    )
    try:
        expected_major = os.environ.get("EXPECTED_POSTGRES_MAJOR")
        if expected_major not in _SUPPORTED_POSTGRES_MAJORS:
            pytest.fail("EXPECTED_POSTGRES_MAJOR must be 15, 16, or 17")
        version_num = await pool.fetchval("SHOW server_version_num")
        assert int(version_num) // 10000 == int(expected_major)
        yield pool
    finally:
        await pool.close()


async def _run_operation(
    filename: str,
    *,
    variables: dict[str, str] | None = None,
    succeeds: bool,
) -> subprocess.CompletedProcess[str]:
    """Execute one checked-in operation through its production psql seam."""
    psql = shutil.which("psql")
    if psql is None:
        pytest.fail("psql is required by the external PostgreSQL gate")
    dsn = os.environ["INGESTION_CUTOVER_TEST_DSN"]
    command = [
        psql,
        "-X",
        "-v",
        "ON_ERROR_STOP=1",
        "-d",
        dsn,
    ]
    for key, value in sorted((variables or {}).items()):
        command.extend(("-v", f"{key}={value}"))
    command.extend(("-f", str(_OPERATION_ROOT / filename)))
    completed = await asyncio.to_thread(
        subprocess.run,
        command,
        capture_output=True,
        check=False,
        text=True,
        timeout=30,
    )
    if succeeds and completed.returncode != 0:
        pytest.fail(
            f"{filename} failed:\n{completed.stdout}\n{completed.stderr}"
        )
    if not succeeds and completed.returncode == 0:
        pytest.fail(f"{filename} unexpectedly succeeded")
    return completed


async def _lease_rows(pool: asyncpg.Pool) -> list[dict[str, object]]:
    rows = await pool.fetch(
        """
        SELECT leases.*
        FROM public.ingestion_leases AS leases
        WHERE source_type = 'bcfy_calls'
        ORDER BY lease_key
        """
    )
    return [dict(row) for row in rows]


async def _child_rows(pool: asyncpg.Pool) -> list[dict[str, object]]:
    rows = await pool.fetch(
        """
        SELECT feeds.*
        FROM public.feeds AS feeds
        JOIN public.feed_properties AS properties
          ON properties.feed_id = feeds.id
        WHERE properties.source_type = 'bcfy_calls'
          AND properties.bcfy_calls_is_trunked IS TRUE
        ORDER BY feeds.id
        """
    )
    return [dict(row) for row in rows]


async def test_reduced_schema_executes_real_runtime_lifecycle(
    cutover_pool: asyncpg.Pool,
) -> None:
    """Migration 038 upgrades reduced 031 for every Lease lifecycle path."""
    sid = str(uuid.uuid4().int)
    await cutover_pool.execute(
        """
        INSERT INTO public.ingestion_leases (
            source_type,
            lease_key,
            status,
            unclaimed_since
        ) VALUES (
            'bcfy_calls',
            $1,
            'unclaimed'::public.feed_status,
            NOW()
        )
        """,
        sid,
    )
    store = ingestion_lease_store.IngestionLeaseStore(cutover_pool)
    owner = uuid.uuid4()
    claims = await store.claim_unclaimed(_SOURCE_TYPE, owner, 1000)
    claim = next(item for item in claims if item.grant.lease_key == sid)

    heartbeat = await store.renew_heartbeats((claim.grant,))
    assert heartbeat[0].disposition is (
        ingestion_lease_store.LeaseOperationDisposition.APPLIED
    )
    release = await store.release(claim.grant)
    assert release.disposition is (
        ingestion_lease_store.LeaseOperationDisposition.APPLIED
    )

    reclaimed = await store.claim_unclaimed(_SOURCE_TYPE, uuid.uuid4(), 1000)
    second = next(item for item in reclaimed if item.grant.lease_key == sid)
    budgeted = await store.finalize_failure(
        second.grant,
        ingestion_lease_store.BudgetedFailure(),
        feed_store.FeedStatusReason.SYSTEM_CONFIGURATION_INVALID,
        actor_id=_ACTOR_ID,
        reason="reduced-schema budgeted failure proof",
    )
    assert budgeted.disposition is (
        ingestion_lease_store.LeaseOperationDisposition.APPLIED
    )
    assert budgeted.after_snapshot is not None
    assert budgeted.after_snapshot.failure_count == 1
    await cutover_pool.execute(
        """
        UPDATE public.ingestion_leases
        SET retry_after = NOW() - INTERVAL '1 second'
        WHERE source_type = 'bcfy_calls' AND lease_key = $1
        """,
        sid,
    )
    recovered_budgeted = await store.claim_recoverable(
        _SOURCE_TYPE,
        uuid.uuid4(),
        1000,
        datetime.timedelta(seconds=30),
    )
    third = next(
        item for item in recovered_budgeted if item.grant.lease_key == sid
    )

    failed = await store.finalize_failure(
        third.grant,
        ingestion_lease_store.NonBudgetedFailure(
            datetime.datetime.now(datetime.UTC) - datetime.timedelta(seconds=1)
        ),
        feed_store.FeedStatusReason.SOURCE_UNREACHABLE,
        actor_id=_ACTOR_ID,
        reason="reduced-schema lifecycle proof",
    )
    assert failed.disposition is (
        ingestion_lease_store.LeaseOperationDisposition.APPLIED
    )
    assert failed.after_snapshot is not None
    assert failed.after_snapshot.failure_count == 0
    assert failed.after_snapshot.status is feed_store.FeedStatus.FAILING

    recovered = await store.claim_recoverable(
        _SOURCE_TYPE,
        uuid.uuid4(),
        1000,
        datetime.timedelta(seconds=30),
    )
    fourth = next(item for item in recovered if item.grant.lease_key == sid)
    assert third.grant.fencing_token == second.grant.fencing_token + 1
    assert fourth.grant.fencing_token == third.grant.fencing_token + 1


async def _seed_reviewed_membership(pool: asyncpg.Pool) -> None:
    legacy_worker = uuid.uuid4()
    rows: list[tuple[object, ...]] = []
    ordinal = 0
    for sid_index in range(19):
        sid = str(7000 + sid_index)
        feed_count = 9 if sid_index < 2 else 8
        for group_index in range(feed_count):
            group_id = str(1000 + group_index)
            status = "active"
            failure_count = 0
            status_reason = None
            if sid_index == 0 and group_index == 0:
                status = "failing"
                failure_count = 2
                status_reason = "source_unreachable"
            elif sid_index == 1 and group_index == 0:
                status = "quarantined"
                failure_count = 5
                status_reason = "system_configuration_invalid"
            feed_id = uuid.UUID(int=ordinal + 1)
            rows.append(
                (
                    feed_id,
                    f"Phase 7 cutover fixture {ordinal:03d}",
                    status,
                    legacy_worker if status == "active" else None,
                    failure_count,
                    status_reason,
                    ordinal + 10,
                    sid,
                    group_id,
                )
            )
            ordinal += 1
    assert len(rows) == 154
    rows.append(
        (
            uuid.UUID(int=ordinal + 1),
            "Phase 7 cutover fixture deactivated",
            "deactivated",
            None,
            4,
            "source_unreachable",
            ordinal + 10,
            "7000",
            "1999",
        )
    )

    async with pool.acquire() as connection:
        async with connection.transaction():
            await connection.executemany(
                """
                INSERT INTO public.feeds (
                    id,
                    name,
                    source_type,
                    status,
                    worker_id,
                    last_heartbeat,
                    last_processed_filename,
                    last_bookmark_time,
                    failure_count,
                    retry_after,
                    status_reason,
                    status_reason_detail,
                    status_reason_updated_at,
                    audit_revision,
                    fencing_token
                ) VALUES (
                    $1,
                    $2,
                    'bcfy_calls',
                    $3::public.feed_status,
                    $4,
                    CASE WHEN $4::UUID IS NULL THEN NULL ELSE NOW() END,
                    'gs://phase7-fixture/' || $1::TEXT || '.flac',
                    TIMESTAMPTZ '2026-07-13 00:00:00+00'
                        + ($7 * INTERVAL '1 second'),
                    $5,
                    CASE WHEN $3 = 'failing'
                         THEN TIMESTAMPTZ '2026-07-14 00:00:00+00'
                         ELSE NULL END,
                    $6::TEXT,
                    CASE WHEN $6::TEXT IS NULL
                         THEN NULL ELSE 'preserved fixture reason' END,
                    CASE WHEN $6::TEXT IS NULL THEN NULL ELSE NOW() END,
                    $5,
                    $7
                )
                """,
                [row[:7] for row in rows],
            )
            await connection.executemany(
                """
                INSERT INTO public.feed_properties (
                    feed_id,
                    source_feed_id,
                    source_type,
                    bcfy_calls_sid,
                    bcfy_calls_group_id,
                    bcfy_calls_is_trunked
                ) VALUES (
                    $1,
                    $2 || '-' || $3,
                    'bcfy_calls',
                    $2,
                    $3,
                    TRUE
                )
                """,
                [(row[0], row[7], row[8]) for row in rows],
            )


async def _manifest_digest(pool: asyncpg.Pool) -> str:
    digest = await pool.fetchval(
        """
        SELECT pg_catalog.encode(
            pg_catalog.sha256(
                pg_catalog.convert_to(
                    pg_catalog.string_agg(
                        properties.feed_id::TEXT || '|' ||
                        properties.bcfy_calls_sid || '|' ||
                        properties.bcfy_calls_group_id,
                        E'\\n' ORDER BY
                            properties.bcfy_calls_sid,
                            properties.bcfy_calls_group_id,
                            properties.feed_id
                    ),
                    'UTF8'
                )
            ),
            'hex'
        )
        FROM public.feed_properties AS properties
        JOIN public.feeds AS feeds ON feeds.id = properties.feed_id
        WHERE properties.source_type = 'bcfy_calls'
          AND feeds.source_type = 'bcfy_calls'
          AND properties.bcfy_calls_is_trunked IS TRUE
          AND feeds.status <> 'deactivated'::public.feed_status
        """
    )
    assert isinstance(digest, str)
    return digest


def _without(
    row: dict[str, object],
    fields: set[str],
) -> dict[str, object]:
    """Return a row without fields one operation may intentionally change."""
    return {key: value for key, value in row.items() if key not in fields}


async def _prove_projection_and_preseed(pool: asyncpg.Pool) -> str:
    """Prove independent completeness failure and replay-safe pre-seeding."""
    await _seed_reviewed_membership(pool)
    incomplete_feed_id = uuid.UUID(int=3)
    await pool.execute(
        """
        UPDATE public.feed_properties
        SET bcfy_calls_sid = NULL,
            bcfy_calls_group_id = NULL,
            bcfy_calls_is_trunked = NULL
        WHERE feed_id = $1
        """,
        incomplete_feed_id,
    )
    await _run_operation("001_preseed.sql", succeeds=False)
    assert (
        await pool.fetchval("SELECT COUNT(*) FROM public.ingestion_leases") == 0
    )
    await pool.execute(
        """
        UPDATE public.feed_properties
        SET bcfy_calls_sid = '7000',
            bcfy_calls_group_id = '1002',
            bcfy_calls_is_trunked = TRUE
        WHERE feed_id = $1
        """,
        incomplete_feed_id,
    )

    await _run_operation("001_preseed.sql", succeeds=True)
    preseed_rows = await _lease_rows(pool)
    assert len(preseed_rows) == 19
    assert {row["status"] for row in preseed_rows} == {"deactivated"}
    await _run_operation("001_preseed.sql", succeeds=True)
    assert await _lease_rows(pool) == preseed_rows
    return await _manifest_digest(pool)


async def _prove_activation_rejections(
    pool: asyncpg.Pool,
    digest: str,
) -> dict[str, str]:
    """Prove wrong review evidence and fence overflow leave no partial state."""
    await pool.execute(
        """
        UPDATE public.feeds
        SET worker_id = NULL,
            status = 'unclaimed'::public.feed_status,
            unclaimed_since = NOW()
        WHERE source_type = 'bcfy_calls'
          AND status = 'active'::public.feed_status
        """
    )
    await pool.execute(
        """
        UPDATE public.feeds
        SET status = 'active'::public.feed_status,
            worker_id = $2,
            last_heartbeat = NOW() - INTERVAL '2 minutes'
        WHERE id = $1
        """,
        uuid.UUID(int=5),
        uuid.UUID(int=999),
    )
    await pool.execute(
        """
        UPDATE public.ingestion_leases
        SET fencing_token = 1000
        WHERE source_type = 'bcfy_calls' AND lease_key = '7000'
        """
    )

    before_bad_digest_leases = await _lease_rows(pool)
    before_bad_digest_children = await _child_rows(pool)
    activation_variables = {
        "process_absence_confirmed": "CONFIRMED",
        "reviewed_manifest_digest": "0" * 64,
        "reviewed_sid_count": "19",
    }
    await _run_operation(
        "002_activate.sql",
        variables=activation_variables,
        succeeds=False,
    )
    assert await _lease_rows(pool) == before_bad_digest_leases
    assert await _child_rows(pool) == before_bad_digest_children

    overflow_feed_id = uuid.UUID(int=4)
    await pool.execute(
        "UPDATE public.feeds SET fencing_token = $2 WHERE id = $1",
        overflow_feed_id,
        9223372036854775807,
    )
    activation_variables["reviewed_manifest_digest"] = digest
    before_overflow_leases = await _lease_rows(pool)
    before_overflow_children = await _child_rows(pool)
    await _run_operation(
        "002_activate.sql",
        variables=activation_variables,
        succeeds=False,
    )
    assert await _lease_rows(pool) == before_overflow_leases
    assert await _child_rows(pool) == before_overflow_children
    await pool.execute(
        "UPDATE public.feeds SET fencing_token = 900 WHERE id = $1",
        overflow_feed_id,
    )

    await pool.execute(
        """
        CREATE FUNCTION public.phase7_corrupt_activation_postflight()
        RETURNS TRIGGER
        LANGUAGE plpgsql
        AS $trigger$
        BEGIN
            IF OLD.status = 'unclaimed'::public.feed_status
               AND NEW.status = 'active'::public.feed_status THEN
                NEW.failure_count := OLD.failure_count + 1;
            END IF;
            RETURN NEW;
        END
        $trigger$;
        CREATE TRIGGER phase7_corrupt_activation_postflight
        BEFORE UPDATE OF status ON public.feeds
        FOR EACH ROW
        EXECUTE FUNCTION public.phase7_corrupt_activation_postflight();
        """
    )
    before_postflight_leases = await _lease_rows(pool)
    before_postflight_children = await _child_rows(pool)
    await _run_operation(
        "002_activate.sql",
        variables=activation_variables,
        succeeds=False,
    )
    assert await _lease_rows(pool) == before_postflight_leases
    assert await _child_rows(pool) == before_postflight_children
    await pool.execute(
        """
        DROP TRIGGER phase7_corrupt_activation_postflight ON public.feeds;
        DROP FUNCTION public.phase7_corrupt_activation_postflight();
        """
    )
    return activation_variables


async def _activate_and_prove_preservation(
    pool: asyncpg.Pool,
    activation_variables: dict[str, str],
) -> None:
    """Activate all reviewed SIDs and prove monotonic parent authority."""
    child_before_activation = await _child_rows(pool)
    await _run_operation(
        "002_activate.sql",
        variables=activation_variables,
        succeeds=True,
    )
    leases_after_activation = await pool.fetch(
        """
        SELECT
            leases.lease_key,
            leases.status::TEXT AS status,
            leases.worker_id,
            leases.fencing_token,
            leases.membership_revision,
            MAX(feeds.fencing_token) AS maximum_child_fence
        FROM public.ingestion_leases AS leases
        JOIN public.feed_properties AS properties
          ON properties.source_type = leases.source_type
         AND properties.bcfy_calls_sid = leases.lease_key
         AND properties.bcfy_calls_is_trunked IS TRUE
        JOIN public.feeds AS feeds ON feeds.id = properties.feed_id
        WHERE leases.source_type = 'bcfy_calls'
        GROUP BY leases.source_type, leases.lease_key
        ORDER BY leases.lease_key
        """
    )
    assert len(leases_after_activation) == 19
    assert all(row["status"] == "unclaimed" for row in leases_after_activation)
    assert all(row["worker_id"] is None for row in leases_after_activation)
    assert all(
        row["fencing_token"] > row["maximum_child_fence"]
        for row in leases_after_activation
    )
    assert all(
        row["membership_revision"] == 1 for row in leases_after_activation
    )
    assert leases_after_activation[0]["fencing_token"] == 1000

    child_after_activation = await _child_rows(pool)
    stale_active = next(
        row for row in child_after_activation if row["id"] == uuid.UUID(int=5)
    )
    assert stale_active["status"] == "active"
    assert stale_active["worker_id"] is None
    assert stale_active["last_heartbeat"] is None
    allowed_child_changes = {
        "status",
        "worker_id",
        "last_heartbeat",
        "unclaimed_since",
    }
    for before, after in zip(
        child_before_activation,
        child_after_activation,
        strict=True,
    ):
        assert _without(before, allowed_child_changes) == _without(
            after,
            allowed_child_changes,
        )


async def _prove_generic_runtime_and_inverse(pool: asyncpg.Pool) -> None:
    """Use ordinary runtime APIs, then prove the exact healthy-child inverse."""
    store = ingestion_lease_store.IngestionLeaseStore(pool)
    claims = await store.claim_unclaimed(_SOURCE_TYPE, uuid.uuid4(), 19)
    assert len(claims) == 19
    target = next(
        item.grant for item in claims if item.grant.lease_key == "7000"
    )
    membership = await store.load_membership(target)
    assert isinstance(membership, ingestion_lease_store.MembershipSnapshot)
    assert membership.members
    mutation = ingestion_lease_store.SourceObservation(
        member=membership.members[0].identity,
        cursor=datetime.datetime(2026, 7, 14, tzinfo=datetime.UTC),
    )
    committed = await store.commit_child_mutations(
        target,
        ingestion_lease_store.ChildMutationBatch(
            mutations=(mutation,),
            lease_effect=ingestion_lease_store.NoLeaseEffect(),
        ),
        actor_id=_ACTOR_ID,
    )
    assert isinstance(committed, ingestion_lease_store.BatchCommitted)
    for claim in claims:
        released = await store.release(claim.grant)
        assert released.disposition is (
            ingestion_lease_store.LeaseOperationDisposition.APPLIED
        )

    leases_before_rollback = await _lease_rows(pool)
    children_before_rollback = await _child_rows(pool)
    await _run_operation(
        "003_rollback_children.sql",
        variables={"process_absence_confirmed": "CONFIRMED"},
        succeeds=True,
    )
    assert await _lease_rows(pool) == leases_before_rollback
    children_after_rollback = await _child_rows(pool)
    for before, after in zip(
        children_before_rollback,
        children_after_rollback,
        strict=True,
    ):
        if before["status"] == "active":
            assert after["status"] == "unclaimed"
        else:
            assert after["status"] == before["status"]
        allowed_changes = {"status", "unclaimed_since"}
        assert _without(before, allowed_changes) == _without(
            after,
            allowed_changes,
        )

    await _run_operation("004_verify.sql", succeeds=True)


async def test_controlled_handoff_and_inverse_are_atomic(
    cutover_pool: asyncpg.Pool,
) -> None:
    """Prove projection gates, replay, activation, runtime, and rollback."""
    digest = await _prove_projection_and_preseed(cutover_pool)
    activation_variables = await _prove_activation_rejections(
        cutover_pool,
        digest,
    )
    await _activate_and_prove_preservation(
        cutover_pool,
        activation_variables,
    )
    await _prove_generic_runtime_and_inverse(cutover_pool)
