"""Integration tests for DB-01: idle_in_transaction_session_timeout GUC.

Verifies the server-side `idle_in_transaction_session_timeout = 30 s` GUC is
applied to every asyncpg connection produced by `create_pool`, on both the
data pool (default sizing) and the heartbeat pool sizing (1/1) per D-06.

Verification approach (D-07): `SHOW idle_in_transaction_session_timeout`
against a connection acquired from each pool, asserting the returned value
is `"30s"`. Faster than a real-timeout test (~1 s vs 30 s) and proves the
GUC is applied — the actual timeout firing is PostgreSQL's own well-tested
behavior.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from backend.pipeline.storage.connection import close_pool, create_pool

if TYPE_CHECKING:
    import asyncpg


async def test_data_pool_carries_idle_timeout_guc(
    db_pool: asyncpg.Pool,
) -> None:
    """Default-sized pool: every connection has 30s idle-in-txn timeout."""
    async with db_pool.acquire() as conn:
        value = await conn.fetchval(
            "SHOW idle_in_transaction_session_timeout",
        )
    assert value == "30s", (
        f"Expected '30s' from SHOW; got {value!r}. "
        "DB-01 GUC not applied to data pool — check create_pool kwargs."
    )


async def test_heartbeat_pool_carries_idle_timeout_guc(
    postgres_container: dict[str, Any],
) -> None:
    """Heartbeat-sized pool (1/1) flows through the same factory per D-06."""
    # Heartbeat pool sizing per D-06: min_size=1, max_size=1.
    pool = await create_pool(
        host=postgres_container["host"],
        port=postgres_container["port"],
        user=postgres_container["user"],
        password=postgres_container["password"],
        db_name=postgres_container["database"],
        min_size=1,
        max_size=1,
    )
    try:
        async with pool.acquire() as conn:
            value = await conn.fetchval(
                "SHOW idle_in_transaction_session_timeout",
            )
        assert value == "30s", (
            f"Expected '30s' from SHOW on heartbeat pool; got {value!r}."
        )
    finally:
        await close_pool(pool)


async def test_pg_settings_records_30000_ms(
    db_pool: asyncpg.Pool,
) -> None:
    """pg_settings.setting holds raw 30000 (ms) — string per Pitfall 10."""
    async with db_pool.acquire() as conn:
        setting = await conn.fetchval(
            "SELECT setting FROM pg_settings "
            "WHERE name = 'idle_in_transaction_session_timeout'",
        )
    assert setting == "30000", (
        f"Expected pg_settings.setting='30000'; got {setting!r}. "
        "If '0', the GUC value didn't apply; if int-typed, asyncpg "
        "wire-encode silently dropped it (Pitfall 10)."
    )
