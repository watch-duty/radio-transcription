"""Real PostgreSQL proofs for the dedicated ingestion runtime role."""

from __future__ import annotations

import dataclasses
import datetime
import os
import re
import typing
import uuid

import asyncpg
import pytest
import pytest_asyncio

from backend.pipeline.storage import feed_store, ingestion_lease_store

if typing.TYPE_CHECKING:
    import collections.abc

pytestmark = pytest.mark.asyncio(loop_scope="module")

_ADMIN_DSN_ENV = "INGESTION_RUNTIME_ADMIN_DSN"
_RUNTIME_DSN_ENV = "INGESTION_RUNTIME_TEST_DSN"
_LEGACY_DSN_ENV = "INGESTION_LEGACY_TEST_DSN"
_RUNTIME_ROLE_ENV = "INGESTION_RUNTIME_ROLE"
_LEGACY_ROLE_ENV = "INGESTION_LEGACY_ROLE"
_LEGACY_PARENT_ROLE_ENV = "INGESTION_LEGACY_PARENT_ROLE"
_CREATOR_ROLE_ENV = "INGESTION_RUNTIME_CREATOR_ROLE"
_LARGE_OBJECT_FIXTURE_ENV = "INGESTION_LARGE_OBJECT_FIXTURE"
_LEGACY_LO_COMPAT_FIXTURE_ENV = "INGESTION_LEGACY_LO_COMPAT_FIXTURE"
_REQUIRED_ENV = "INGESTION_RUNTIME_EXTERNAL_POSTGRES_REQUIRED"
_SOURCE_TYPE = feed_store.SourceType.BCFY_CALLS
_ACTOR_ID = "service_account:gcp:ingestion-runtime-privilege-test"
_LEGACY_ACTOR_ID = "service_account:gcp:legacy-ingestion-compatibility-test"
_CURSOR = datetime.datetime(2026, 7, 10, 12, 0, tzinfo=datetime.UTC)
_EXPECTED_TABLE_PRIVILEGES = {
    "ingestion_leases": frozenset({"SELECT", "UPDATE"}),
    "feeds": frozenset({"SELECT", "UPDATE"}),
    "feed_properties": frozenset({"SELECT"}),
    "feed_audit_events": frozenset({"SELECT", "INSERT"}),
}
_EXPECTED_LEGACY_TABLE_PRIVILEGES = {
    "feeds": frozenset({"SELECT", "UPDATE"}),
    # FeedStore's audited progress query locks the joined property row.
    "feed_properties": frozenset({"SELECT", "UPDATE"}),
    "feed_audit_events": frozenset({"SELECT", "INSERT"}),
}
_LARGE_OBJECT_CREATION_SIGNATURES = frozenset(
    {
        "lo_create(oid)",
        "lo_creat(integer)",
        "lo_from_bytea(oid,bytea)",
        "lo_import(text)",
        "lo_import(text,oid)",
    }
)
_EXPECTED_LEGACY_LARGE_OBJECT_CREATION_SIGNATURES = frozenset(
    {
        "lo_create(oid)",
        "lo_creat(integer)",
        "lo_from_bytea(oid,bytea)",
        "lo_import(text)",
    }
)
_LARGE_OBJECT_CREATION_PROBES = (
    "SELECT pg_catalog.lo_create(0)",
    "SELECT pg_catalog.lo_creat(0)",
    "SELECT pg_catalog.lo_from_bytea(0, decode('00', 'hex'))",
    "SELECT pg_catalog.lo_import('/etc/hosts')",
    ("SELECT pg_catalog.lo_import('/etc/hosts', 4000000000::pg_catalog.oid)"),
)


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
    future_type: str


@dataclasses.dataclass(frozen=True, slots=True)
class _EffectiveRolePrivileges:
    """Effective public-schema capabilities for one limited identity."""

    tables: frozenset[tuple[str, str]]
    table_grant_options: frozenset[tuple[str, str]]
    columns: frozenset[tuple[str, str, str]]
    column_grant_options: frozenset[tuple[str, str, str]]
    available_columns: frozenset[tuple[str, str]]
    sequences: frozenset[tuple[str, str]]
    routines: frozenset[str]
    types: frozenset[str]


def _configured_value(name: str) -> str:
    """Return a configured gate value or skip/fail with a precise reason."""
    value = os.environ.get(name)
    if value:
        return value
    if os.environ.get(_REQUIRED_ENV) == "1":
        pytest.fail(f"{name} is required for this PostgreSQL gate")
    pytest.skip(f"{name} is not configured")


def _dsn(name: str) -> str:
    """Return a required PostgreSQL DSN for a named environment variable."""
    return _configured_value(name)


def _configured_identifier(name: str) -> str:
    """Return a required simple PostgreSQL identifier."""
    value = _configured_value(name)
    if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value) is None:
        pytest.fail(f"{name} must be a simple PostgreSQL identifier")
    return value


def _configured_oid(name: str) -> int:
    """Return a required positive PostgreSQL object identifier."""
    value = _configured_value(name)
    if re.fullmatch(r"[0-9]+", value) is None or int(value) <= 0:
        pytest.fail(f"{name} must be a positive PostgreSQL OID")
    return int(value)


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def admin_pool() -> collections.abc.AsyncIterator[asyncpg.Pool]:
    """Create the fixture-only administrator pool.

    Yields:
        A bounded administrator connection pool.
    """
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
    """Create the limited runtime pool used by every production operation.

    Yields:
        A bounded connection pool authenticated as the runtime identity.
    """
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


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def legacy_pool() -> collections.abc.AsyncIterator[asyncpg.Pool]:
    """Create the limited compatibility pool for legacy Feed operations.

    Yields:
        A bounded connection pool authenticated as the legacy identity.
    """
    pool = await asyncpg.create_pool(
        dsn=_dsn(_LEGACY_DSN_ENV),
        min_size=2,
        max_size=4,
        statement_cache_size=0,
    )
    try:
        yield pool
    finally:
        await pool.close()


async def _role_name(pool: asyncpg.Pool) -> str:
    """Return the authenticated PostgreSQL role for a pool.

    Args:
        pool: Pool whose authenticated identity is inspected.

    Returns:
        The current PostgreSQL role name.
    """
    role = await pool.fetchval("SELECT current_user")
    assert isinstance(role, str)
    return role


async def _large_object_creation_privileges(
    admin_pool: asyncpg.Pool,
    role: str,
) -> list[asyncpg.Record]:
    """Inspect rights on all five server-side large-object creators.

    Args:
        admin_pool: Administrator pool used only for catalog inspection.
        role: PostgreSQL role whose effective rights are inspected.

    Returns:
        Creator signatures with execute and grant-option decisions.
    """
    return await admin_pool.fetch(
        """
        SELECT
            procedure.oid::pg_catalog.regprocedure::text AS signature,
            pg_catalog.has_function_privilege(
                $1,
                procedure.oid,
                'EXECUTE'
            ) AS allowed,
            pg_catalog.has_function_privilege(
                $1,
                procedure.oid,
                'EXECUTE WITH GRANT OPTION'
            ) AS grantable
        FROM pg_catalog.pg_proc AS procedure
        WHERE procedure.oid IN (
            'pg_catalog.lo_create(oid)'::pg_catalog.regprocedure,
            'pg_catalog.lo_creat(integer)'::pg_catalog.regprocedure,
            'pg_catalog.lo_from_bytea(oid, bytea)'::pg_catalog.regprocedure,
            'pg_catalog.lo_import(text)'::pg_catalog.regprocedure,
            'pg_catalog.lo_import(text, oid)'::pg_catalog.regprocedure
        )
        ORDER BY procedure.oid::pg_catalog.regprocedure::text
        """,
        role,
    )


async def _large_object_capabilities(
    admin_pool: asyncpg.Pool,
    role: str,
) -> list[asyncpg.Record]:
    """Inspect owner, read, write, and grant paths for large objects.

    Args:
        admin_pool: Administrator pool used only for catalog inspection.
        role: PostgreSQL role whose effective rights are inspected.

    Returns:
        One capability record per large object and privilege.
    """
    server_version = int(await admin_pool.fetchval("SHOW server_version_num"))
    set_role_membership_mode = "SET" if server_version >= 160000 else "MEMBER"
    return await admin_pool.fetch(
        """
        WITH role_identity AS (
            SELECT oid
            FROM pg_catalog.pg_roles
            WHERE rolname = $1
        ), database AS (
            SELECT oid
            FROM pg_catalog.pg_database
            WHERE datname = pg_catalog.current_database()
        ), catalog_candidate AS (
            SELECT
                substring(
                    configuration.setting
                    FROM pg_catalog.strpos(configuration.setting, '=') + 1
                )::boolean AS enabled,
                CASE
                    WHEN role_setting.setrole = role_identity.oid
                     AND role_setting.setdatabase = database.oid THEN 4
                    WHEN role_setting.setrole = role_identity.oid
                     AND role_setting.setdatabase = 0::oid THEN 3
                    WHEN role_setting.setrole = 0::oid
                     AND role_setting.setdatabase = database.oid THEN 2
                    ELSE 1
                END AS priority
            FROM role_identity
            CROSS JOIN database
            JOIN pg_catalog.pg_db_role_setting AS role_setting
              ON role_setting.setrole IN (0::oid, role_identity.oid)
             AND role_setting.setdatabase IN (0::oid, database.oid)
            CROSS JOIN LATERAL unnest(role_setting.setconfig)
              AS configuration(setting)
            WHERE pg_catalog.split_part(configuration.setting, '=', 1) =
                  'lo_compat_privileges'
        ), effective_lo_compat AS (
            SELECT COALESCE(
                (
                    SELECT enabled
                    FROM catalog_candidate
                    ORDER BY priority DESC
                    LIMIT 1
                ),
                pg_catalog.current_setting('lo_compat_privileges')::boolean
            ) AS enabled
        )
        SELECT
            large_object.oid,
            privilege.name,
            pg_catalog.pg_has_role(
                $1,
                large_object.lomowner,
                'USAGE'
            ) OR pg_catalog.pg_has_role(
                $1,
                large_object.lomowner,
                $2
            ) AS owner_capability,
            effective_lo_compat.enabled OR COALESCE(
                pg_catalog.bool_or(
                    acl.privilege_type = privilege.name
                    AND CASE
                        WHEN acl.grantee = 0::oid THEN TRUE
                        ELSE pg_catalog.pg_has_role(
                            $1,
                            acl.grantee,
                            'USAGE'
                        ) OR pg_catalog.pg_has_role(
                            $1,
                            acl.grantee,
                            $2
                        )
                    END
                ),
                FALSE
            ) AS allowed,
            COALESCE(
                pg_catalog.bool_or(
                    acl.privilege_type = privilege.name
                    AND acl.is_grantable
                    AND CASE
                        WHEN acl.grantee = 0::oid THEN TRUE
                        ELSE pg_catalog.pg_has_role(
                            $1,
                            acl.grantee,
                            'USAGE'
                        ) OR pg_catalog.pg_has_role(
                            $1,
                            acl.grantee,
                            $2
                        )
                    END
                ),
                FALSE
            ) AS grantable
        FROM pg_catalog.pg_largeobject_metadata AS large_object
        CROSS JOIN effective_lo_compat
        CROSS JOIN (VALUES ('SELECT'), ('UPDATE')) AS privilege(name)
        LEFT JOIN LATERAL pg_catalog.aclexplode(
            COALESCE(
                large_object.lomacl,
                pg_catalog.acldefault('L'::"char", large_object.lomowner)
            )
        ) AS acl ON TRUE
        GROUP BY
            large_object.oid,
            large_object.lomowner,
            privilege.name,
            effective_lo_compat.enabled
        ORDER BY large_object.oid, privilege.name
        """,
        role,
        set_role_membership_mode,
    )


async def _assert_no_large_object_capabilities(
    admin_pool: asyncpg.Pool,
    role: str,
) -> None:
    """Assert that a role has no capability on any large object.

    Args:
        admin_pool: Administrator pool used only for catalog inspection.
        role: PostgreSQL role expected to have no capability.
    """
    capabilities = await _large_object_capabilities(admin_pool, role)
    assert capabilities, "CI must provide an existing large object"
    assert not any(
        row["owner_capability"] or row["allowed"] or row["grantable"]
        for row in capabilities
    )


async def _assert_exact_legacy_large_object_capabilities(
    admin_pool: asyncpg.Pool,
    role: str,
) -> None:
    """Assert the legacy compatibility setting retained exact LO access.

    Args:
        admin_pool: Administrator pool used only for catalog inspection.
        role: Preserved legacy PostgreSQL role.
    """
    capabilities = await _large_object_capabilities(admin_pool, role)
    assert capabilities, "CI must provide an existing large object"
    expected_fixtures = {
        _configured_oid(_LARGE_OBJECT_FIXTURE_ENV),
        _configured_oid(_LEGACY_LO_COMPAT_FIXTURE_ENV),
    }
    assert expected_fixtures <= {row["oid"] for row in capabilities}
    for row in capabilities:
        assert row["owner_capability"] is False
        assert row["grantable"] is False
        assert row["allowed"] is True


async def _effective_role_privileges(
    admin_pool: asyncpg.Pool,
    role: str,
) -> _EffectiveRolePrivileges:
    """Return deterministic public-schema privilege sets for one role.

    Args:
        admin_pool: Administrator pool used only for catalog inspection.
        role: PostgreSQL role whose effective rights are inspected.

    Returns:
        Canonical effective privilege sets across public-schema surfaces.
    """
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

    table_rows = await admin_pool.fetch(
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
        role,
        table_privileges,
    )
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
        CROSS JOIN unnest(
            ARRAY['SELECT', 'INSERT', 'UPDATE', 'REFERENCES']
        ) AS privilege(name)
        WHERE namespace.nspname = 'public'
          AND relation.relkind IN ('r', 'p', 'v', 'm', 'f')
          AND attribute.attnum > 0
          AND NOT attribute.attisdropped
        ORDER BY relation.relname, attribute.attnum, privilege.name
        """,
        role,
    )
    sequence_rows = await admin_pool.fetch(
        """
        SELECT relation.relname, privilege.name
        FROM pg_catalog.pg_class AS relation
        JOIN pg_catalog.pg_namespace AS namespace
          ON namespace.oid = relation.relnamespace
        CROSS JOIN unnest(
            ARRAY['USAGE', 'SELECT', 'UPDATE']
        ) AS privilege(name)
        WHERE namespace.nspname = 'public'
          AND relation.relkind = 'S'
          AND pg_catalog.has_sequence_privilege(
              $1,
              relation.oid,
              privilege.name
          )
        ORDER BY relation.relname, privilege.name
        """,
        role,
    )
    routine_rows = await admin_pool.fetch(
        """
        SELECT procedure.oid::pg_catalog.regprocedure::text AS signature
        FROM pg_catalog.pg_proc AS procedure
        JOIN pg_catalog.pg_namespace AS namespace
          ON namespace.oid = procedure.pronamespace
        WHERE namespace.nspname = 'public'
          AND pg_catalog.has_function_privilege($1, procedure.oid, 'EXECUTE')
        ORDER BY signature
        """,
        role,
    )
    type_rows = await admin_pool.fetch(
        """
        SELECT type.typname
        FROM pg_catalog.pg_type AS type
        JOIN pg_catalog.pg_namespace AS namespace
          ON namespace.oid = type.typnamespace
        LEFT JOIN pg_catalog.pg_class AS type_relation
          ON type_relation.oid = type.typrelid
        WHERE namespace.nspname = 'public'
          AND type.typelem = 0
          AND (
              (type.typrelid = 0 AND type.typtype IN ('b', 'd', 'e', 'm', 'r'))
              OR (type.typtype = 'c' AND type_relation.relkind = 'c')
          )
          AND pg_catalog.has_type_privilege($1, type.oid, 'USAGE')
        ORDER BY type.typname
        """,
        role,
    )
    return _EffectiveRolePrivileges(
        tables=frozenset(
            (row["relname"], row["name"])
            for row in table_rows
            if row["allowed"]
        ),
        table_grant_options=frozenset(
            (row["relname"], row["name"])
            for row in table_rows
            if row["grantable"]
        ),
        columns=frozenset(
            (row["relname"], row["attname"], row["name"])
            for row in column_rows
            if row["allowed"]
        ),
        column_grant_options=frozenset(
            (row["relname"], row["attname"], row["name"])
            for row in column_rows
            if row["grantable"]
        ),
        available_columns=frozenset(
            (row["relname"], row["attname"]) for row in column_rows
        ),
        sequences=frozenset(
            (row["relname"], row["name"]) for row in sequence_rows
        ),
        routines=frozenset(row["signature"] for row in routine_rows),
        types=frozenset(row["typname"] for row in type_rows),
    )


async def _assert_admin_connection(
    connection: asyncpg.Connection,
    limited_roles: frozenset[str],
) -> None:
    current_role = await connection.fetchval("SELECT current_user")
    assert current_role not in limited_roles, (
        "fixture construction must use the admin pool"
    )


def _fixture_identifier(prefix: str) -> str:
    """Return a unique simple PostgreSQL identifier with a stable prefix."""
    return f"{prefix}_{uuid.uuid4().hex}"


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def _privilege_fixture_guard(
    admin_pool: asyncpg.Pool,
) -> collections.abc.AsyncIterator[_PrivilegeFixtures]:
    """Arm durable cleanup before the dependent fixture starts setup.

    Yields:
        Unique fixture identities whose cleanup is already armed.
    """
    runtime_role = _configured_identifier(_RUNTIME_ROLE_ENV)
    legacy_role = _configured_identifier(_LEGACY_ROLE_ENV)
    limited_roles = frozenset({runtime_role, legacy_role})
    fixtures = _PrivilegeFixtures(
        sid=str(uuid.uuid4().int),
        group_id=str(uuid.uuid4().int),
        lease_owner=uuid.uuid4(),
        member_feed_id=uuid.uuid4(),
        legacy_feed_id=uuid.uuid4(),
        legacy_worker_id=uuid.uuid4(),
        future_table=_fixture_identifier("runtime_future_table"),
        sequence=_fixture_identifier("runtime_sequence"),
        function=_fixture_identifier("runtime_function"),
        future_type=_fixture_identifier("runtime_type"),
    )

    try:
        yield fixtures
    finally:
        async with admin_pool.acquire() as connection:
            await _assert_admin_connection(connection, limited_roles)

            # Tombstone the Lease first in its own transaction. If later object
            # cleanup fails, the unique candidate can never remain claimable.
            async with connection.transaction(isolation="read_committed"):
                lease_before = await connection.fetchrow(
                    """
                    SELECT fencing_token
                    FROM public.ingestion_leases
                    WHERE source_type = 'bcfy_calls' AND lease_key = $1
                    """,
                    fixtures.sid,
                )
                if lease_before is not None:
                    update_result = await connection.execute(
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
                    assert update_result == "UPDATE 1"
                    tombstone = await connection.fetchrow(
                        """
                        SELECT
                            source_type,
                            lease_key,
                            status::text AS status,
                            worker_id,
                            last_heartbeat,
                            retry_after,
                            unclaimed_since,
                            fencing_token
                        FROM public.ingestion_leases
                        WHERE source_type = 'bcfy_calls' AND lease_key = $1
                        """,
                        fixtures.sid,
                    )
                    assert tombstone is not None
                    assert tuple(tombstone.values()) == (
                        "bcfy_calls",
                        fixtures.sid,
                        "deactivated",
                        None,
                        None,
                        None,
                        None,
                        lease_before["fencing_token"],
                    ), (
                        "fixture cleanup must retain the permanent Lease tombstone"
                    )

            async with connection.transaction(isolation="read_committed"):
                await connection.execute(
                    "DELETE FROM public.feed_audit_events "
                    "WHERE feed_id = ANY($1::uuid[])",
                    [fixtures.member_feed_id, fixtures.legacy_feed_id],
                )
                await connection.execute(
                    "DELETE FROM public.feeds WHERE id = ANY($1::uuid[])",
                    [fixtures.member_feed_id, fixtures.legacy_feed_id],
                )
                await connection.execute(
                    f"DROP TYPE IF EXISTS public.{fixtures.future_type}"
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


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def privilege_fixtures(
    admin_pool: asyncpg.Pool,
    _privilege_fixture_guard: _PrivilegeFixtures,
) -> collections.abc.AsyncIterator[_PrivilegeFixtures]:
    """Create unique fixtures exclusively through the admin setup pool.

    Yields:
        Fully materialized database fixtures for limited-role tests.
    """
    runtime_role = _configured_identifier(_RUNTIME_ROLE_ENV)
    legacy_role = _configured_identifier(_LEGACY_ROLE_ENV)
    creator_role = _configured_identifier(_CREATOR_ROLE_ENV)
    limited_roles = frozenset({runtime_role, legacy_role})
    fixtures = _privilege_fixture_guard
    sid = fixtures.sid
    group_id = fixtures.group_id

    async with admin_pool.acquire() as connection:
        await _assert_admin_connection(connection, limited_roles)
        async with connection.transaction(isolation="read_committed"):
            lease_count = await connection.fetchval(
                "SELECT pg_catalog.count(*) FROM public.ingestion_leases"
            )
            if lease_count != 0:
                pytest.fail(
                    "Lease table must be empty before the privilege suite; "
                    f"found {lease_count} rows"
                )
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
                    failure_count,
                    status_reason,
                    status_reason_detail,
                    status_reason_updated_at,
                    audit_revision,
                    unclaimed_since
                ) VALUES (
                    $1,
                    $2,
                    'bcfy_calls',
                    'unclaimed'::public.feed_status,
                    1,
                    'source_unreachable',
                    'legacy compatibility fixture failure',
                    NOW(),
                    0,
                    NOW()
                )
                """,
                fixtures.legacy_feed_id,
                f"Runtime privilege legacy {uuid.uuid4().hex}",
            )
            await connection.execute(
                """
                INSERT INTO public.feed_properties (
                    feed_id,
                    source_feed_id,
                    source_type
                ) VALUES ($1, $2, 'bcfy_calls')
                """,
                fixtures.legacy_feed_id,
                f"legacy-{uuid.uuid4().hex}",
            )
            await connection.execute(f"SET LOCAL ROLE {creator_role}")
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
            await connection.execute(
                f"CREATE TYPE public.{fixtures.future_type} AS ENUM ('fixture')"
            )

        eligible_leases = await connection.fetch(
            """
            SELECT lease_key, fencing_token
            FROM public.ingestion_leases
            WHERE source_type = 'bcfy_calls'
              AND status = 'unclaimed'::public.feed_status
            ORDER BY lease_key
            """
        )
        assert [tuple(row.values()) for row in eligible_leases] == [(sid, 0)]

    yield fixtures


async def test_limited_login_identities_are_isolated(
    admin_pool: asyncpg.Pool,
    runtime_pool: asyncpg.Pool,
    legacy_pool: asyncpg.Pool,
) -> None:
    """Prove pool identities and the complete safe role-attribute boundary."""
    admin_role = await _role_name(admin_pool)
    runtime_role = await _role_name(runtime_pool)
    legacy_role = await _role_name(legacy_pool)
    assert len({admin_role, runtime_role, legacy_role}) == 3
    assert runtime_role == _configured_value(_RUNTIME_ROLE_ENV)
    assert legacy_role == _configured_value(_LEGACY_ROLE_ENV)
    assert (
        await runtime_pool.fetchval(
            "SELECT pg_catalog.current_setting('session_replication_role')"
        )
        == "origin"
    )
    assert (
        await legacy_pool.fetchval(
            "SELECT pg_catalog.current_setting('session_replication_role')"
        )
        == "replica"
    )
    assert (
        await runtime_pool.fetchval(
            "SELECT pg_catalog.current_setting('lo_compat_privileges')"
        )
        == "off"
    )
    assert (
        await legacy_pool.fetchval(
            "SELECT pg_catalog.current_setting('lo_compat_privileges')"
        )
        == "on"
    )

    runtime_creation_privileges = await _large_object_creation_privileges(
        admin_pool,
        runtime_role,
    )
    assert {row["signature"] for row in runtime_creation_privileges} == (
        _LARGE_OBJECT_CREATION_SIGNATURES
    )
    assert not any(row["allowed"] for row in runtime_creation_privileges)
    assert not any(row["grantable"] for row in runtime_creation_privileges)

    legacy_creation_privileges = await _large_object_creation_privileges(
        admin_pool,
        legacy_role,
    )
    assert {row["signature"] for row in legacy_creation_privileges} == (
        _LARGE_OBJECT_CREATION_SIGNATURES
    )
    assert {
        row["signature"] for row in legacy_creation_privileges if row["allowed"]
    } == _EXPECTED_LEGACY_LARGE_OBJECT_CREATION_SIGNATURES
    assert not any(row["grantable"] for row in legacy_creation_privileges)

    async with legacy_pool.acquire() as connection:
        transaction = connection.transaction()
        await transaction.start()
        try:
            compat_fixture = _configured_oid(_LEGACY_LO_COMPAT_FIXTURE_ENV)
            assert (
                await connection.fetchval(
                    "SELECT pg_catalog.lo_get($1::oid)",
                    compat_fixture,
                )
                == b"\x01"
            )
            await connection.execute(
                "SELECT pg_catalog.lo_put($1::oid, 0, decode('03', 'hex'))",
                compat_fixture,
            )
        finally:
            await transaction.rollback()

    admin_creation_privileges = await _large_object_creation_privileges(
        admin_pool,
        admin_role,
    )
    assert {row["signature"] for row in admin_creation_privileges} == (
        _LARGE_OBJECT_CREATION_SIGNATURES
    )
    assert all(row["allowed"] for row in admin_creation_privileges)
    assert all(row["grantable"] for row in admin_creation_privileges)

    large_object_fixture = _configured_oid(_LARGE_OBJECT_FIXTURE_ENV)
    large_object_owner = await admin_pool.fetchval(
        """
        SELECT owner.rolname
        FROM pg_catalog.pg_largeobject_metadata AS large_object
        JOIN pg_catalog.pg_roles AS owner ON owner.oid = large_object.lomowner
        WHERE large_object.oid = $1::oid
        """,
        large_object_fixture,
    )
    assert large_object_owner == admin_role

    rows = await admin_pool.fetch(
        """
        SELECT
            rolname,
            rolcanlogin,
            rolsuper,
            rolinherit,
            rolcreaterole,
            rolcreatedb,
            rolreplication,
            rolbypassrls
        FROM pg_catalog.pg_roles
        WHERE rolname = ANY($1::text[])
        ORDER BY rolname
        """,
        [runtime_role, legacy_role],
    )
    assert {row["rolname"] for row in rows} == {runtime_role, legacy_role}
    for row in rows:
        assert dict(row) == {
            "rolname": row["rolname"],
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
    legacy_parent_role = _configured_identifier(_LEGACY_PARENT_ROLE_ENV)
    legacy_memberships = await admin_pool.fetch(
        """
        SELECT
            parent.rolname,
            parent.rolcanlogin,
            parent.rolsuper,
            parent.rolcreatedb,
            parent.rolcreaterole,
            membership.admin_option
        FROM pg_catalog.pg_auth_members AS membership
        JOIN pg_catalog.pg_roles AS parent ON parent.oid = membership.roleid
        JOIN pg_catalog.pg_roles AS member ON member.oid = membership.member
        WHERE member.rolname = $1
        ORDER BY parent.rolname
        """,
        legacy_role,
    )
    assert [dict(row) for row in legacy_memberships] == [
        {
            "rolname": legacy_parent_role,
            "rolcanlogin": False,
            "rolsuper": False,
            "rolcreatedb": True,
            "rolcreaterole": True,
            "admin_option": False,
        }
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
    effective = await _effective_role_privileges(admin_pool, runtime_role)
    expected_tables = frozenset(
        (table, privilege)
        for table, privileges in _EXPECTED_TABLE_PRIVILEGES.items()
        for privilege in privileges
    )
    assert effective.tables == expected_tables
    assert not effective.table_grant_options

    expected_columns = frozenset(
        (table, column, privilege)
        for table, column in effective.available_columns
        for privilege in _EXPECTED_TABLE_PRIVILEGES.get(table, frozenset())
    )
    assert effective.columns == expected_columns
    assert not effective.column_grant_options

    assert not effective.sequences
    assert not effective.routines

    assert effective.types == {"feed_status"}
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

    direct_parameter_grants = await admin_pool.fetchval(
        """
        SELECT pg_catalog.count(*)
        FROM pg_catalog.pg_parameter_acl AS parameter_acl
        CROSS JOIN LATERAL pg_catalog.aclexplode(parameter_acl.paracl) AS acl
        WHERE acl.grantee IN (
            0::oid,
            (SELECT oid FROM pg_catalog.pg_roles WHERE rolname = $1),
            (SELECT oid FROM pg_catalog.pg_roles
             WHERE rolname = 'app_ingestion_runtime')
        )
        """,
        runtime_role,
    )
    assert direct_parameter_grants == 0

    unsafe_parameter_grants = await admin_pool.fetchval(
        """
        SELECT pg_catalog.count(*)
        FROM pg_catalog.pg_settings AS setting
        WHERE (
            (
                setting.context IN ('superuser', 'superuser-backend')
                OR setting.name = 'session_replication_role'
            )
            AND pg_catalog.has_parameter_privilege($1, setting.name, 'SET')
        ) OR pg_catalog.has_parameter_privilege(
            $1,
            setting.name,
            'ALTER SYSTEM'
        )
        """,
        runtime_role,
    )
    assert unsafe_parameter_grants == 0

    await _assert_no_large_object_capabilities(admin_pool, runtime_role)

    owned_objects = await admin_pool.fetchval(
        """
        SELECT pg_catalog.count(*)
        FROM pg_catalog.pg_shdepend AS dependency
        JOIN pg_catalog.pg_roles AS owner
          ON owner.oid = dependency.refobjid
        WHERE dependency.refclassid =
              'pg_catalog.pg_authid'::pg_catalog.regclass
          AND dependency.deptype = 'o'
          AND owner.rolname IN ($1, 'app_ingestion_runtime')
        """,
        runtime_role,
    )
    assert owned_objects == 0


async def test_legacy_feed_store_runs_through_separate_limited_pool(
    admin_pool: asyncpg.Pool,
    legacy_pool: asyncpg.Pool,
    privilege_fixtures: _PrivilegeFixtures,
) -> None:
    """Prove the minimal legacy Feed claim/progress/audit compatibility path."""
    legacy_role = await _role_name(legacy_pool)
    effective = await _effective_role_privileges(admin_pool, legacy_role)
    expected_tables = frozenset(
        (table, privilege)
        for table, privileges in _EXPECTED_LEGACY_TABLE_PRIVILEGES.items()
        for privilege in privileges
    )
    assert effective.tables == expected_tables
    assert not effective.table_grant_options

    expected_columns = frozenset(
        (table, column, privilege)
        for table, column in effective.available_columns
        for privilege in _EXPECTED_LEGACY_TABLE_PRIVILEGES.get(
            table,
            frozenset(),
        )
    )
    assert effective.columns == expected_columns
    assert not effective.column_grant_options

    assert (
        await admin_pool.fetchval(
            "SELECT pg_catalog.has_schema_privilege($1, 'public', 'CREATE')",
            legacy_role,
        )
        is True
    )
    assert (
        await admin_pool.fetchval(
            "SELECT pg_catalog.has_database_privilege($1, current_database(), 'CONNECT')",
            legacy_role,
        )
        is True
    )
    assert (
        await admin_pool.fetchval(
            "SELECT pg_catalog.has_database_privilege($1, current_database(), 'CONNECT WITH GRANT OPTION')",
            legacy_role,
        )
        is False
    )
    for database_privilege in ("CREATE", "TEMPORARY"):
        assert (
            await admin_pool.fetchval(
                "SELECT pg_catalog.has_database_privilege($1, current_database(), $2)",
                legacy_role,
                database_privilege,
            )
            is True
        )
    assert (
        await admin_pool.fetchval(
            "SELECT pg_catalog.has_schema_privilege($1, 'public', 'USAGE')",
            legacy_role,
        )
        is True
    )
    assert (
        await admin_pool.fetchval(
            "SELECT pg_catalog.has_schema_privilege($1, 'public', 'USAGE WITH GRANT OPTION')",
            legacy_role,
        )
        is False
    )
    assert (
        await admin_pool.fetchval(
            "SELECT pg_catalog.has_type_privilege($1, 'public.feed_status', 'USAGE')",
            legacy_role,
        )
        is True
    )
    assert (
        await admin_pool.fetchval(
            "SELECT pg_catalog.has_type_privilege($1, 'public.feed_status', 'USAGE WITH GRANT OPTION')",
            legacy_role,
        )
        is False
    )
    assert not effective.sequences
    assert not effective.routines
    assert effective.types == {"feed_status"}

    unsafe_parameter_grants = await admin_pool.fetchval(
        """
        WITH privileged_parameters AS (
            SELECT parameter_acl.parname
            FROM pg_catalog.pg_parameter_acl AS parameter_acl
            UNION
            SELECT 'session_replication_role'::text
            UNION
            SELECT 'lo_compat_privileges'::text
        )
        SELECT pg_catalog.count(*)
        FROM privileged_parameters AS parameter
        CROSS JOIN (VALUES ('SET'), ('ALTER SYSTEM')) AS access(privilege)
        WHERE pg_catalog.has_parameter_privilege(
            $1,
            parameter.parname,
            access.privilege
        )
        """,
        legacy_role,
    )
    assert unsafe_parameter_grants == 1

    await _assert_exact_legacy_large_object_capabilities(
        admin_pool,
        legacy_role,
    )

    eligible = await admin_pool.fetch(
        """
        SELECT id, fencing_token
        FROM public.feeds
        WHERE source_type = 'bcfy_calls'
          AND status = 'unclaimed'::public.feed_status
        ORDER BY id
        """
    )
    assert [(row["id"], row["fencing_token"]) for row in eligible] == [
        (privilege_fixtures.legacy_feed_id, 0)
    ]

    legacy_store = feed_store.FeedStore(
        legacy_pool,
        claim_types=[_SOURCE_TYPE],
    )
    leased_feeds = await legacy_store.acquire_feeds_batch(
        privilege_fixtures.legacy_worker_id,
        {_SOURCE_TYPE: 1},
    )
    assert len(leased_feeds) == 1
    leased = leased_feeds[0]
    assert leased["id"] == privilege_fixtures.legacy_feed_id
    assert leased["source_type"] is _SOURCE_TYPE
    assert leased["fencing_token"] == 1

    updated = await legacy_store.update_feed_progress(
        privilege_fixtures.legacy_feed_id,
        privilege_fixtures.legacy_worker_id,
        f"gs://legacy-compatibility/{uuid.uuid4().hex}.flac",
        leased["fencing_token"],
        _CURSOR,
        actor_id=_LEGACY_ACTOR_ID,
    )
    assert updated is True
    history = await legacy_store.list_feed_history_records(
        privilege_fixtures.legacy_feed_id,
        limit=1,
    )
    assert history.total == 1
    assert len(history.audit_events) == 1
    assert history.audit_events[0]["action"] == "feed.recovered"
    assert history.audit_events[0]["actor_id"] == _LEGACY_ACTOR_ID
    assert history.audit_events[0]["feed_revision"] == 1

    released = await legacy_store.release_feed(
        privilege_fixtures.legacy_feed_id,
        privilege_fixtures.legacy_worker_id,
        leased["fencing_token"],
    )
    assert released is True


async def test_real_lease_store_runs_through_runtime_pool(
    admin_pool: asyncpg.Pool,
    runtime_pool: asyncpg.Pool,
    privilege_fixtures: _PrivilegeFixtures,
) -> None:
    """Exercise one exact fenced Lease without touching unrelated candidates."""
    eligible = await admin_pool.fetch(
        """
        SELECT lease_key, fencing_token
        FROM public.ingestion_leases
        WHERE source_type = 'bcfy_calls'
          AND status = 'unclaimed'::public.feed_status
        ORDER BY lease_key
        """
    )
    assert [(row["lease_key"], row["fencing_token"]) for row in eligible] == [
        (privilege_fixtures.sid, 0)
    ]

    lease_store = ingestion_lease_store.IngestionLeaseStore(runtime_pool)
    claims = await lease_store.claim_unclaimed(
        _SOURCE_TYPE,
        privilege_fixtures.lease_owner,
        1,
    )
    assert len(claims) == 1
    grant = claims[0].grant
    assert grant == ingestion_lease_store.LeaseGrant(
        source_type=_SOURCE_TYPE,
        lease_key=privilege_fixtures.sid,
        owner_worker_id=privilege_fixtures.lease_owner,
        fencing_token=1,
    )
    assert claims[0].snapshot.status is feed_store.FeedStatus.ACTIVE

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
    durable_tombstone = await runtime_pool.fetchrow(
        """
        SELECT
            status::text AS status,
            worker_id,
            fencing_token
        FROM public.ingestion_leases
        WHERE source_type = 'bcfy_calls' AND lease_key = $1
        """,
        privilege_fixtures.sid,
    )
    assert durable_tombstone is not None
    assert tuple(durable_tombstone.values()) == ("unclaimed", None, 1)


async def _assert_denied(
    runtime_pool: asyncpg.Pool,
    statement: str,
) -> None:
    with pytest.raises(asyncpg.InsufficientPrivilegeError):
        await runtime_pool.execute(statement)


async def _assert_large_object_creation_denied(
    admin_pool: asyncpg.Pool,
    runtime_pool: asyncpg.Pool,
    statement: str,
) -> None:
    """Roll back every creator probe and defensively unlink any survivor."""
    unexpected_oid: int | None = None
    denied = False
    try:
        async with runtime_pool.acquire() as connection:
            transaction = connection.transaction()
            await transaction.start()
            try:
                try:
                    unexpected_oid = await connection.fetchval(statement)
                except asyncpg.InsufficientPrivilegeError:
                    denied = True
            finally:
                await transaction.rollback()
    finally:
        if unexpected_oid is not None:
            await admin_pool.fetch(
                """
                SELECT pg_catalog.lo_unlink(large_object.oid)
                FROM pg_catalog.pg_largeobject_metadata AS large_object
                WHERE large_object.oid = $1::oid
                """,
                unexpected_oid,
            )
    assert denied, f"large-object creator unexpectedly succeeded: {statement}"


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
    admin_pool: asyncpg.Pool,
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
        "SET session_replication_role = replica",
        "SET lo_compat_privileges = on",
        "SET ROLE postgres",
        f"CREATE ROLE {_fixture_identifier('runtime_role_denied')}",
    )
    for statement in statements:
        await _assert_denied(runtime_pool, statement)

    large_objects_before = {
        row["oid"]
        for row in await admin_pool.fetch(
            "SELECT oid FROM pg_catalog.pg_largeobject_metadata ORDER BY oid"
        )
    }
    for statement in _LARGE_OBJECT_CREATION_PROBES:
        await _assert_large_object_creation_denied(
            admin_pool,
            runtime_pool,
            statement,
        )
    large_objects_after = {
        row["oid"]
        for row in await admin_pool.fetch(
            "SELECT oid FROM pg_catalog.pg_largeobject_metadata ORDER BY oid"
        )
    }
    assert large_objects_after == large_objects_before

    # PostgreSQL reports a warning, not an error, when a role without grant
    # options tries to grant a privilege it merely holds. Prove the operation
    # is ineffective by comparing the direct PUBLIC ACL before and after it.
    assert await _public_has_direct_select_on_feeds(runtime_pool) is False
    await runtime_pool.execute("GRANT SELECT ON public.feeds TO PUBLIC")
    assert await _public_has_direct_select_on_feeds(runtime_pool) is False
