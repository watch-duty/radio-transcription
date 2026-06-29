from __future__ import annotations

from unittest import mock


def _async_context_manager(result: object | None = None) -> mock.MagicMock:
    """Create an inspectable async context manager mock."""
    context = mock.MagicMock()
    context.__aenter__ = mock.AsyncMock(return_value=result)
    context.__aexit__ = mock.AsyncMock(return_value=None)
    return context


def make_mock_pool(
    *,
    fetchrow_result: dict | None = None,
    execute_result: str = "UPDATE 0",
    fetch_result: list | None = None,
    fetchval_result: int = 0,
    transaction: bool = False,
) -> mock.AsyncMock:
    """Create a mock asyncpg.Pool with the given return values."""
    pool = mock.AsyncMock()
    pool.fetchrow.return_value = fetchrow_result
    pool.execute.return_value = execute_result
    pool.fetch.return_value = fetch_result or []
    pool.fetchval.return_value = fetchval_result

    if transaction:
        connection = mock.AsyncMock()
        connection.fetchrow.return_value = fetchrow_result
        connection.execute.return_value = execute_result
        connection.fetch.return_value = fetch_result or []
        connection.fetchval.return_value = fetchval_result

        transaction_context = _async_context_manager()
        connection.transaction = mock.Mock(return_value=transaction_context)

        acquire_context = _async_context_manager(connection)
        pool.acquire = mock.Mock(return_value=acquire_context)

        pool.acquired_connection = connection
        pool.acquire_context = acquire_context
        pool.transaction_context = transaction_context

    return pool
