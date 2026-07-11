import logging

import asyncpg
from tenacity import (
    RetryCallState,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from backend.pipeline.storage.settings import AlloyDBSettings

_logger = logging.getLogger(__name__)


async def create_pool(
    host: str,
    user: str,
    db_name: str,
    password: str = "",
    port: int = 6432,
    min_size: int = 8,
    max_size: int = 8,
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
        # Required for PgBouncer transaction-mode pooling.
        "statement_cache_size": 0,
        # DB-01: `idle_in_transaction_session_timeout = 30000` (30s) is enforced
        # server-side via AlloyDB cluster `database_flags`, NOT via asyncpg
        # `server_settings` — see radio-transcription-deployment/terraform/
        # modules/storage/main.tf. We connect through AlloyDB managed pooling
        # (PgBouncer in transaction mode, `pool_mode = "transaction"`) on port
        # 6432; per-connection SET GUCs would be RESET between transactions
        # (PITFALLS.md Pitfall 10), so a `server_settings` here would be
        # redundant with the cluster flag and only effective for the first
        # transaction on a freshly-acquired connection.
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


async def create_pool_from_settings(
    settings: AlloyDBSettings | None = None,
) -> asyncpg.Pool:
    """
    Create an asyncpg connection pool using an AlloyDBSettings object.

    If no settings object is provided, it defaults to loading from environment
    variables.
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


def _log_retry(retry_state: RetryCallState) -> None:
    exc = retry_state.outcome.exception() if retry_state.outcome else None
    _logger.warning(
        "AlloyDB connection attempt %d failed (%s). Retrying...",
        retry_state.attempt_number,
        exc,
        exc_info=exc,
    )


@retry(
    retry=retry_if_exception_type((TimeoutError, ConnectionError, OSError)),
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=2, max=30),
    before_sleep=_log_retry,
    reraise=True,
)
async def create_pool_with_retry(
    settings: AlloyDBSettings | None = None,
) -> asyncpg.Pool:
    """
    Create an asyncpg connection pool with exponential backoff retry.

    Retries on transient connection failures (up to 5 attempts, starting at 2s
    and doubling up to 30s). Intended for Cloud Run cold starts where multiple
    services spinning up simultaneously can briefly saturate the AlloyDB
    managed connection pooler.

    Args:
        settings: AlloyDB connection settings. Defaults to env vars.
    """
    return await create_pool_from_settings(settings)
