from __future__ import annotations

from unittest import mock


def make_mock_pool(
    *,
    fetchrow_result: dict | None = None,
    execute_result: str = "UPDATE 0",
    fetch_result: list | None = None,
) -> mock.AsyncMock:
    """Create a mock asyncpg.Pool with the given return values."""
    pool = mock.AsyncMock()
    pool.fetchrow.return_value = fetchrow_result
    pool.execute.return_value = execute_result
    pool.fetch.return_value = fetch_result or []

    # Set up a connection mock returned by pool.acquire() used as an async context manager.
    # pool.acquire() must return a sync object with __aenter__/__aexit__, not a coroutine.
    conn = mock.AsyncMock()
    conn.fetchrow.return_value = fetchrow_result
    conn.execute.return_value = execute_result
    conn.fetch.return_value = fetch_result or []

    acquire_cm = mock.MagicMock()
    acquire_cm.__aenter__ = mock.AsyncMock(return_value=conn)
    acquire_cm.__aexit__ = mock.AsyncMock(return_value=False)
    pool.acquire = mock.MagicMock(return_value=acquire_cm)

    # Set up conn.transaction() as a sync-callable async context manager
    transaction_cm = mock.MagicMock()
    transaction_cm.__aenter__ = mock.AsyncMock(return_value=None)
    transaction_cm.__aexit__ = mock.AsyncMock(return_value=False)
    conn.transaction = mock.MagicMock(return_value=transaction_cm)

    return pool
