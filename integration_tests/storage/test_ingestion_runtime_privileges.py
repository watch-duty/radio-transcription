"""Real PostgreSQL proofs for the dedicated ingestion runtime role."""

from __future__ import annotations

import dataclasses
import datetime
import os
import uuid
from typing import TYPE_CHECKING

import asyncpg
import pytest
import pytest_asyncio

from backend.pipeline.storage import feed_store, ingestion_lease_store

if TYPE_CHECKING:
    import collections.abc

pytestmark = pytest.mark.asyncio(loop_scope="module")

_ADMIN_DSN_ENV = "INGESTION_RUNTIME_ADMIN_DSN"
_RUNTIME_DSN_ENV = "INGESTION_RUNTIME_TEST_DSN"
_RUNTIME_ROLE_ENV = "INGESTION_RUNTIME_ROLE"
_REQUIRED_ENV = "INGESTION_RUNTIME_EXTERNAL_POSTGRES_REQUIRED"
_SOURCE_TYPE = feed_store.SourceType.BCFY_CALLS
_ACTOR_ID = "service_account:gcp:ingestion-runtime-privilege-test"
_CURSOR = datetime.datetime(2026, 7, 10, 12, 0, tzinfo=datetime.UTC)
_EXPECTED_TABLE_PRIVILEGES = {
    "ingestion_leases": frozenset({"SELECT", "UPDATE"}),
    "feeds": frozenset({"SELECT", "UPDATE"}),
    "feed_properties": frozenset({"SELECT"}),
    "feed_audit_events": frozenset({"SELECT", "INSERT"}),
}


@dataclasses.dataclass(frozen=True, slots=True)
class _PrivilegeFixtures:
    """Unique rows and objects created only by the admin setup pool."""

    sid: str
    group_id: str
    lease_owner: uuid.UUID
    member_feed_id: uuid.UUID
    legacy_feed_id: uuid.UUID
    legacy_worker_id: uuid.UUID
    future_table: str
    sequence: str
    function: str


def _configured_value(name: str) -> str:
    value = os.environ.get(name)
    if value:
        return value
    if os.environ.get(_REQUIRED_ENV) == "1":
        pytest.fail(f"{name} is required for this PostgreSQL gate")
    pytest.skip(f"{name} is not configured")


def _dsn(name: str) -> str:
    return _configured_value(name)


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def admin_pool() -> collections.abc.AsyncIterator[asyncpg.Pool]:
    """Create the fixture-only administrator pool."""
    if os.environ.get("PYTEST_XDIST_WORKER"):
        pytest.fail("the ingestion runtime privilege module must run serially")
    pool = await asyncpg.create_pool(
        dsn=_dsn(_ADMIN_DSN_ENV),
        min_size=1,
        max_size=2,
        statement_cache_size=0,
    )
    try:
        yield pool
    finally:
        await pool.close()


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def runtime_pool() -> collections.abc.AsyncIterator[asyncpg.Pool]:
    """Create the limited runtime pool used by every production operation."""
    pool = await asyncpg.create_pool(
        dsn=_dsn(_RUNTIME_DSN_ENV),
        min_size=2,
        max_size=4,
        statement_cache_size=0,
    )
    try:
        yield pool
    finally:
        await pool.close()


async def _role_name(pool: asyncpg.Pool) -> str:
    role = await pool.fetchval("SELECT current_user")
    assert isinstance(role, str)
    return role


async def _assert_admin_connection(
    connection: asyncpg.Connection,
    runtime_role: str,
) -> None:
    current_role = await connection.fetchval("SELECT current_user")
    assert current_role != runtime_role, (
        "fixture construction must use the admin pool"
    )


def _fixture_identifier(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex}"


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def privilege_fixtures(
    admin_pool: asyncpg.Pool,
) -> collections.abc.AsyncIterator[_PrivilegeFixtures]:
    """Create and remove unique fixtures exclusively through the admin pool."""
    runtime_role = _configured_value(_RUNTIME_ROLE_ENV)
    sid = str(uuid.uuid4().int)
    group_id = str(uuid.uuid4().int)
    fixtures = _PrivilegeFixtures(
        sid=sid,
        group_id=group_id,
        lease_owner=uuid.uuid4(),
        member_feed_id=uuid.uuid4(),
        legacy_feed_id=uuid.uuid4(),
        legacy_worker_id=uuid.uuid4(),
        future_table=_fixture_identifier("runtime_future_table"),
        sequence=_fixture_identifier("runtime_sequence"),
        function=_fixture_identifier("runtime_function"),
    )

    async with admin_pool.acquire() as connection:
        await _assert_admin_connection(connection, runtime_role)
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
                    status_reason_updated_at,
                    audit_revision
                ) VALUES (
                    $1,
                    $2,
                    'bcfy_calls',
                    'failing'::public.feed_status,
                    1,
                    NOW() + INTERVAL '5 minutes',
                    'source_unreachable',
                    'admin fixture failure',
                    NOW(),
                    0
                )
                """,
                fixtures.member_feed_id,
                f"Runtime privilege member {uuid.uuid4().hex}",
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
                fixtures.member_feed_id,
                f"{sid}-{group_id}",
                sid,
                group_id,
            )
            await connection.execute(
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
            await connection.execute(
                """
                INSERT INTO public.feeds (
                    id,
                    name,
                    source_type,
                    status,
                    worker_id,
                    last_heartbeat,
                    fencing_token
                ) VALUES (
                    $1,
                    $2,
                    'bcfy_calls',
                    'active'::public.feed_status,
                    $3,
                    NOW(),
                    11
                )
                """,
                fixtures.legacy_feed_id,
                f"Runtime privilege legacy {uuid.uuid4().hex}",
                fixtures.legacy_worker_id,
            )
            await connection.execute(
                f"CREATE TABLE public.{fixtures.future_table} "
                "(id integer PRIMARY KEY)"
            )
            await connection.execute(
                f"CREATE SEQUENCE public.{fixtures.sequence}"
            )
            await connection.execute(
                f"CREATE FUNCTION public.{fixtures.function}() "
                "RETURNS integer LANGUAGE sql IMMUTABLE AS 'SELECT 1'"
            )

    try:
        yield fixtures
    finally:
        async with admin_pool.acquire() as connection:
            await _assert_admin_connection(connection, runtime_role)
            async with connection.transaction(isolation="read_committed"):
                await connection.execute(
                    "DELETE FROM public.feed_audit_events WHERE feed_id = $1",
                    fixtures.member_feed_id,
                )
                await connection.execute(
                    "DELETE FROM public.feeds WHERE id = ANY($1::uuid[])",
                    [fixtures.member_feed_id, fixtures.legacy_feed_id],
                )
                await connection.execute(
                    """
                    UPDATE public.ingestion_leases
                    SET status = 'deactivated'::public.feed_status,
                        worker_id = NULL,
                        last_heartbeat = NULL,
                        retry_after = NULL,
                        unclaimed_since = NULL,
                        updated_at = NOW()
                    WHERE source_type = 'bcfy_calls' AND lease_key = $1
                    """,
                    fixtures.sid,
                )
                await connection.execute(
                    f"DROP FUNCTION IF EXISTS public.{fixtures.function}()"
                )
                await connection.execute(
                    f"DROP SEQUENCE IF EXISTS public.{fixtures.sequence}"
                )
                await connection.execute(
                    f"DROP TABLE IF EXISTS public.{fixtures.future_table}"
                )


async def test_runtime_login_inherits_only_the_dedicated_group(
    admin_pool: asyncpg.Pool,
    runtime_pool: asyncpg.Pool,
) -> None:
    """Prove pool identities and the complete safe role-attribute boundary."""
    admin_role = await _role_name(admin_pool)
    runtime_role = await _role_name(runtime_pool)
    assert admin_role != runtime_role
    assert runtime_role == _configured_value(_RUNTIME_ROLE_ENV)

    row = await admin_pool.fetchrow(
        """
        SELECT
            rolcanlogin,
            rolsuper,
            rolinherit,
            rolcreaterole,
            rolcreatedb,
            rolreplication,
            rolbypassrls
        FROM pg_catalog.pg_roles
        WHERE rolname = $1
        """,
        runtime_role,
    )
    assert row is not None
    assert dict(row) == {
        "rolcanlogin": True,
        "rolsuper": False,
        "rolinherit": True,
        "rolcreaterole": False,
        "rolcreatedb": False,
        "rolreplication": False,
        "rolbypassrls": False,
    }
    memberships = await admin_pool.fetch(
        """
        SELECT parent.rolname, membership.admin_option
        FROM pg_catalog.pg_auth_members AS membership
        JOIN pg_catalog.pg_roles AS parent
          ON parent.oid = membership.roleid
        JOIN pg_catalog.pg_roles AS member
          ON member.oid = membership.member
        WHERE member.rolname = $1
        ORDER BY parent.rolname
        """,
        runtime_role,
    )
    assert [(row["rolname"], row["admin_option"]) for row in memberships] == [
        ("app_ingestion_runtime", False)
    ]
    assert (
        await admin_pool.fetchval(
            "SELECT pg_catalog.pg_has_role($1, 'postgres', 'MEMBER')",
            runtime_role,
        )
        is False
    )
    server_version = int(await admin_pool.fetchval("SHOW server_version_num"))
    if server_version >= 160000:
        assert (
            await admin_pool.fetchval(
                "SELECT pg_catalog.pg_has_role($1, 'postgres', 'SET')",
                runtime_role,
            )
            is False
        )


async def test_runtime_effective_privileges_are_the_exact_contract(
    admin_pool: asyncpg.Pool,
    runtime_pool: asyncpg.Pool,
    privilege_fixtures: _PrivilegeFixtures,
) -> None:
    """Inspect effective direct, inherited, owner, and PUBLIC privileges."""
    del privilege_fixtures
    runtime_role = await _role_name(runtime_pool)
    server_version = int(await admin_pool.fetchval("SHOW server_version_num"))
    table_privileges = [
        "SELECT",
        "INSERT",
        "UPDATE",
        "DELETE",
        "TRUNCATE",
        "REFERENCES",
        "TRIGGER",
    ]
    if server_version >= 170000:
        table_privileges.append("MAINTAIN")

    rows = await admin_pool.fetch(
        """
        SELECT
            relation.relname,
            privilege.name,
            pg_catalog.has_table_privilege(
                $1,
                relation.oid,
                privilege.name
            ) AS allowed,
            pg_catalog.has_table_privilege(
                $1,
                relation.oid,
                privilege.name || ' WITH GRANT OPTION'
            ) AS grantable
        FROM pg_catalog.pg_class AS relation
        JOIN pg_catalog.pg_namespace AS namespace
          ON namespace.oid = relation.relnamespace
        CROSS JOIN unnest($2::text[]) AS privilege(name)
        WHERE namespace.nspname = 'public'
          AND relation.relkind IN ('r', 'p', 'v', 'm', 'f')
        ORDER BY relation.relname, privilege.name
        """,
        runtime_role,
        table_privileges,
    )
    allowed = {(row["relname"], row["name"]) for row in rows if row["allowed"]}
    expected = {
        (table, privilege)
        for table, privileges in _EXPECTED_TABLE_PRIVILEGES.items()
        for privilege in privileges
    }
    assert allowed == expected
    assert not any(row["grantable"] for row in rows)

    column_privileges = ["SELECT", "INSERT", "UPDATE", "REFERENCES"]
    column_rows = await admin_pool.fetch(
        """
        SELECT
            relation.relname,
            attribute.attname,
            privilege.name,
            pg_catalog.has_column_privilege(
                $1,
                relation.oid,
                attribute.attnum,
                privilege.name
            ) AS allowed,
            pg_catalog.has_column_privilege(
                $1,
                relation.oid,
                attribute.attnum,
                privilege.name || ' WITH GRANT OPTION'
            ) AS grantable
        FROM pg_catalog.pg_attribute AS attribute
        JOIN pg_catalog.pg_class AS relation
          ON relation.oid = attribute.attrelid
        JOIN pg_catalog.pg_namespace AS namespace
          ON namespace.oid = relation.relnamespace
        CROSS JOIN unnest($2::text[]) AS privilege(name)
        WHERE namespace.nspname = 'public'
          AND relation.relkind IN ('r', 'p', 'v', 'm', 'f')
          AND attribute.attnum > 0
          AND NOT attribute.attisdropped
        ORDER BY relation.relname, attribute.attnum, privilege.name
        """,
        runtime_role,
        column_privileges,
    )
    for column_row in column_rows:
        expected_allowed = column_row["name"] in (
            _EXPECTED_TABLE_PRIVILEGES.get(
                column_row["relname"],
                frozenset(),
            )
        )
        assert column_row["allowed"] is expected_allowed
        assert column_row["grantable"] is False

    assert (
        await admin_pool.fetchval(
            """
        SELECT NOT pg_catalog.bool_or(
            pg_catalog.has_sequence_privilege(
                $1,
                relation.oid,
                privilege.name
            )
        )
        FROM pg_catalog.pg_class AS relation
        JOIN pg_catalog.pg_namespace AS namespace
          ON namespace.oid = relation.relnamespace
        CROSS JOIN unnest(
            ARRAY['USAGE', 'SELECT', 'UPDATE']
        ) AS privilege(name)
        WHERE namespace.nspname = 'public'
          AND relation.relkind = 'S'
        """,
            runtime_role,
        )
        is True
    )
    assert (
        await admin_pool.fetchval(
            """
        SELECT NOT pg_catalog.bool_or(
            pg_catalog.has_function_privilege($1, procedure.oid, 'EXECUTE')
        )
        FROM pg_catalog.pg_proc AS procedure
        JOIN pg_catalog.pg_namespace AS namespace
          ON namespace.oid = procedure.pronamespace
        WHERE namespace.nspname = 'public'
        """,
            runtime_role,
        )
        is True
    )

    type_rows = await admin_pool.fetch(
        """
        SELECT
            type.typname,
            pg_catalog.has_type_privilege($1, type.oid, 'USAGE') AS allowed
        FROM pg_catalog.pg_type AS type
        JOIN pg_catalog.pg_namespace AS namespace
          ON namespace.oid = type.typnamespace
        LEFT JOIN pg_catalog.pg_class AS type_relation
          ON type_relation.oid = type.typrelid
        WHERE namespace.nspname = 'public'
          AND type.typelem = 0
          AND (
              (
                  type.typrelid = 0
                  AND type.typtype IN ('b', 'd', 'e', 'm', 'r')
              )
              OR (
                  type.typtype = 'c'
                  AND type_relation.relkind = 'c'
              )
          )
        ORDER BY type.typname
        """,
        runtime_role,
    )
    assert {row["typname"] for row in type_rows if row["allowed"]} == {
        "feed_status"
    }
    assert (
        await admin_pool.fetchval(
            "SELECT pg_catalog.has_database_privilege($1, current_database(), 'CONNECT')",
            runtime_role,
        )
        is True
    )
    assert (
        await admin_pool.fetchval(
            "SELECT pg_catalog.has_database_privilege($1, current_database(), 'CONNECT WITH GRANT OPTION')",
            runtime_role,
        )
        is False
    )
    assert (
        await admin_pool.fetchval(
            "SELECT pg_catalog.has_database_privilege($1, current_database(), 'CREATE')",
            runtime_role,
        )
        is False
    )
    assert (
        await admin_pool.fetchval(
            "SELECT pg_catalog.has_database_privilege($1, current_database(), 'TEMPORARY')",
            runtime_role,
        )
        is False
    )
    assert (
        await admin_pool.fetchval(
            "SELECT pg_catalog.has_schema_privilege($1, 'public', 'USAGE')",
            runtime_role,
        )
        is True
    )
    assert (
        await admin_pool.fetchval(
            "SELECT pg_catalog.has_schema_privilege($1, 'public', 'USAGE WITH GRANT OPTION')",
            runtime_role,
        )
        is False
    )
    assert (
        await admin_pool.fetchval(
            "SELECT pg_catalog.has_schema_privilege($1, 'public', 'CREATE')",
            runtime_role,
        )
        is False
    )
    assert (
        await admin_pool.fetchval(
            "SELECT pg_catalog.has_type_privilege($1, 'public.feed_status', 'USAGE WITH GRANT OPTION')",
            runtime_role,
        )
        is False
    )

    owned_objects = await admin_pool.fetchval(
        """
        SELECT
            (SELECT pg_catalog.count(*)
             FROM pg_catalog.pg_class
             WHERE relowner = role.oid)
          + (SELECT pg_catalog.count(*)
             FROM pg_catalog.pg_proc
             WHERE proowner = role.oid)
          + (SELECT pg_catalog.count(*)
             FROM pg_catalog.pg_type
             WHERE typowner = role.oid)
        FROM pg_catalog.pg_roles AS role
        WHERE role.rolname = $1
        """,
        runtime_role,
    )
    assert owned_objects == 0


async def test_real_legacy_and_lease_stores_run_through_runtime_pool(
    runtime_pool: asyncpg.Pool,
    privilege_fixtures: _PrivilegeFixtures,
) -> None:
    """Exercise the production legacy and fenced Lease store SQL."""
    legacy_store = feed_store.FeedStore(runtime_pool)
    released_legacy = await legacy_store.release_feed(
        privilege_fixtures.legacy_feed_id,
        privilege_fixtures.legacy_worker_id,
        11,
    )
    assert released_legacy is True

    lease_store = ingestion_lease_store.IngestionLeaseStore(runtime_pool)
    claims = await lease_store.claim_unclaimed(
        _SOURCE_TYPE,
        privilege_fixtures.lease_owner,
        1000,
    )
    matching = [
        claim
        for claim in claims
        if claim.grant.lease_key == privilege_fixtures.sid
    ]
    assert len(matching) == 1
    grant = matching[0].grant

    membership = await lease_store.load_membership(grant)
    assert isinstance(membership, ingestion_lease_store.MembershipSnapshot)
    assert [member.identity.feed_id for member in membership.members] == [
        privilege_fixtures.member_feed_id
    ]
    member = membership.members[0].identity
    batch = ingestion_lease_store.ChildMutationBatch(
        mutations=(
            ingestion_lease_store.AdmittedAudioProgress(
                member=member,
                last_processed_filename=(
                    f"gs://runtime-privilege/{uuid.uuid4().hex}.flac"
                ),
                cursor=_CURSOR,
            ),
        ),
        lease_effect=ingestion_lease_store.NoLeaseEffect(),
    )
    committed = await lease_store.commit_child_mutations(
        grant,
        batch,
        actor_id=_ACTOR_ID,
    )
    assert isinstance(committed, ingestion_lease_store.BatchCommitted)
    assert committed.children[0].disposition is (
        ingestion_lease_store.ChildDisposition.APPLIED
    )
    assert committed.children[0].lifecycle_effect is (
        ingestion_lease_store.LifecycleEffect.RECOVERED
    )

    audit = await runtime_pool.fetchrow(
        """
        SELECT action, actor_id, feed_revision
        FROM public.feed_audit_events
        WHERE feed_id = $1
        """,
        privilege_fixtures.member_feed_id,
    )
    assert audit is not None
    assert dict(audit) == {
        "action": "feed.recovered",
        "actor_id": _ACTOR_ID,
        "feed_revision": 1,
    }
    released = await lease_store.release(grant)
    assert released.disposition is (
        ingestion_lease_store.LeaseOperationDisposition.APPLIED
    )


async def _assert_denied(
    runtime_pool: asyncpg.Pool,
    statement: str,
) -> None:
    with pytest.raises(asyncpg.InsufficientPrivilegeError):
        await runtime_pool.execute(statement)


async def _public_has_direct_select_on_feeds(
    runtime_pool: asyncpg.Pool,
) -> bool:
    """Return whether PUBLIC has a direct SELECT ACL on ``feeds``."""
    result = await runtime_pool.fetchval(
        """
        SELECT COALESCE(
            pg_catalog.bool_or(
                acl.grantee = 0
                AND acl.privilege_type = 'SELECT'
            ),
            FALSE
        )
        FROM pg_catalog.pg_class AS relation
        JOIN pg_catalog.pg_namespace AS namespace
          ON namespace.oid = relation.relnamespace
        LEFT JOIN LATERAL pg_catalog.aclexplode(relation.relacl) AS acl
          ON TRUE
        WHERE namespace.nspname = 'public'
          AND relation.relname = 'feeds'
        """
    )
    assert isinstance(result, bool)
    return result


async def test_every_forbidden_runtime_action_is_denied(
    runtime_pool: asyncpg.Pool,
    privilege_fixtures: _PrivilegeFixtures,
) -> None:
    """Probe forbidden DML, DDL, ACL, role, sequence, and function actions."""
    statements = (
        "INSERT INTO public.ingestion_leases DEFAULT VALUES",
        "DELETE FROM public.ingestion_leases WHERE FALSE",
        "TRUNCATE TABLE public.ingestion_leases",
        "INSERT INTO public.feeds DEFAULT VALUES",
        "DELETE FROM public.feeds WHERE FALSE",
        "TRUNCATE TABLE public.feeds",
        "INSERT INTO public.feed_properties DEFAULT VALUES",
        "UPDATE public.feed_properties SET source_feed_id = source_feed_id WHERE FALSE",
        "DELETE FROM public.feed_properties WHERE FALSE",
        "TRUNCATE TABLE public.feed_properties",
        "UPDATE public.feed_audit_events SET action = action WHERE FALSE",
        "DELETE FROM public.feed_audit_events WHERE FALSE",
        "TRUNCATE TABLE public.feed_audit_events",
        "SELECT * FROM public.source_types",
        "SELECT slug FROM public.source_types",
        f"SELECT * FROM public.{privilege_fixtures.future_table}",
        f"INSERT INTO public.{privilege_fixtures.future_table} VALUES (1)",
        f"SELECT nextval('public.{privilege_fixtures.sequence}')",
        f"SELECT public.{privilege_fixtures.function}()",
        f"ALTER TABLE public.{privilege_fixtures.future_table} "
        "ADD COLUMN runtime_must_not_add integer",
        f"DROP TABLE public.{privilege_fixtures.future_table}",
        f"CREATE TABLE public.{_fixture_identifier('runtime_denied')} "
        "(id integer)",
        f"CREATE TEMPORARY TABLE {_fixture_identifier('runtime_temp_denied')} "
        "(id integer)",
        f"CREATE SCHEMA {_fixture_identifier('runtime_schema_denied')}",
        "SET ROLE postgres",
        f"CREATE ROLE {_fixture_identifier('runtime_role_denied')}",
    )
    for statement in statements:
        await _assert_denied(runtime_pool, statement)

    # PostgreSQL reports a warning, not an error, when a role without grant
    # options tries to grant a privilege it merely holds. Prove the operation
    # is ineffective by comparing the direct PUBLIC ACL before and after it.
    assert await _public_has_direct_select_on_feeds(runtime_pool) is False
    await runtime_pool.execute("GRANT SELECT ON public.feeds TO PUBLIC")
    assert await _public_has_direct_select_on_feeds(runtime_pool) is False
