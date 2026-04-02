from __future__ import annotations

import asyncio
import logging
from typing import Any, cast

import asyncpg
import psycopg
from psycopg.rows import dict_row

from .settings import AlloyDBSettings


async def create_pool(
    host: str,
    user: str,
    db_name: str,
    password: str = "",
    port: int = 6432,
    min_size: int = 5,
    max_size: int = 5,
    command_timeout: float | None = None,
    timeout: float | None = None,  # noqa: ASYNC109
    max_inactive_connection_lifetime: float | None = None,
) -> asyncpg.Pool:
    """
    Create an asyncpg connection pool to the AlloyDB instance.

    Connects directly via private IP on the VPC. The pool manages
    connection lifecycle, checkout, and release automatically.

    Args:
        host: AlloyDB instance private IP or hostname.
        user: Database username.
        db_name: Target database name.
        password: Database password.
        port: Database port (default 6432, AlloyDB managed pooling).
        min_size: Minimum number of connections in the pool.
        max_size: Maximum number of connections in the pool.
        command_timeout: Query execution timeout in seconds.
        timeout: TCP connection timeout in seconds.
        max_inactive_connection_lifetime: Seconds before idle connections are
            closed. Useful for Cloud Run where CPU freezes between requests
            cause TCP connections to go stale.

    Returns:
        An asyncpg connection pool.

    Raises:
        TimeoutError: If connection cannot be established within timeout.
        ConnectionError: If connection fails for other reasons.

    """
    kwargs: dict = {
        "host": host,
        "port": port,
        "user": user,
        "password": password,
        "database": db_name,
        "min_size": min_size,
        "max_size": max_size,
        "statement_cache_size": 0,  # Required for PgBouncer transaction-mode pooling.
    }
    if command_timeout is not None:
        kwargs["command_timeout"] = command_timeout
    if timeout is not None:
        kwargs["timeout"] = timeout
    if max_inactive_connection_lifetime is not None:
        kwargs["max_inactive_connection_lifetime"] = (
            max_inactive_connection_lifetime
        )

    try:
        return await asyncpg.create_pool(**kwargs)
    except TimeoutError as e:
        msg = (
            f"Failed to connect to AlloyDB at {host}:{port} within {timeout}s. "
            "If running locally, ensure AlloyDB Auth Proxy is running."
        )
        raise TimeoutError(msg) from e
    except Exception as e:
        msg = (
            f"Failed to connect to AlloyDB: {e}. "
            f"Check credentials and network connectivity to {host}:{port}"
        )
        raise ConnectionError(msg) from e


async def close_pool(pool: asyncpg.Pool) -> None:
    """Close an asyncpg connection pool."""
    await pool.close()


def connect_db(
    settings: AlloyDBSettings | None = None,
) -> psycopg.Connection[dict[str, Any]]:
    """Open a sync psycopg connection to AlloyDB via pgBouncer.

    No pool needed when the caller handles at most one request at a time
    (e.g. Cloud Run with concurrency=1). pgBouncer provides server-side
    pooling.

    If *settings* is ``None``, an :class:`AlloyDBSettings` is constructed
    from environment variables.
    """
    if settings is None:
        settings = AlloyDBSettings()
    return cast(
        "psycopg.Connection[dict[str, Any]]",
        psycopg.connect(
            host=settings.host,
            port=settings.port,
            user=settings.user,
            password=settings.password,
            dbname=settings.db_name,
            autocommit=True,
            row_factory=cast("Any", dict_row),
        ),
    )


async def create_pool_from_settings(
    settings: AlloyDBSettings | None = None,
) -> asyncpg.Pool:
    """
    Create an asyncpg connection pool using an AlloyDBSettings object.

    If no settings object is provided, it defaults to loading from environment variables.
    """
    if settings is None:
        settings = AlloyDBSettings()

    return await create_pool(
        host=settings.host,
        port=settings.port,
        user=settings.user,
        db_name=settings.db_name,
        password=settings.password,
        min_size=settings.pool_min_size,
        max_size=settings.pool_max_size,
        command_timeout=settings.command_timeout_sec,
        timeout=settings.connect_timeout_sec,
    )


_logger = logging.getLogger(__name__)


async def create_pool_with_retry(
    settings: AlloyDBSettings | None = None,
    max_attempts: int = 5,
    base_delay: float = 2.0,
) -> asyncpg.Pool:
    """
    Create an asyncpg connection pool with exponential backoff retry.

    Retries on transient connection failures, which can occur during Cloud Run
    cold starts when multiple services spin up simultaneously and briefly
    saturate the AlloyDB managed connection pooler.

    Args:
        settings: AlloyDB connection settings. Defaults to env vars.
        max_attempts: Maximum number of connection attempts.
        base_delay: Initial retry delay in seconds (doubles each attempt).
    """
    last_exc: Exception | None = None
    for attempt in range(1, max_attempts + 1):
        try:
            return await create_pool_from_settings(settings)
        except (TimeoutError, ConnectionError, OSError) as exc:
            last_exc = exc
            if attempt == max_attempts:
                break
            delay = base_delay * (2 ** (attempt - 1))
            _logger.warning(
                "AlloyDB connection attempt %d/%d failed (%s). Retrying in %.1fs.",
                attempt,
                max_attempts,
                exc,
                delay,
            )
            await asyncio.sleep(delay)
    raise RuntimeError(
        f"Failed to connect to AlloyDB after {max_attempts} attempts."
    ) from last_exc
