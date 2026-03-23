import asyncpg

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
