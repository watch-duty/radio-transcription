import os

import asyncpg


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


async def create_pool_from_env() -> asyncpg.Pool:
    """
    Create an asyncpg connection pool using standard environment variables.

    Reads configuration from:
    - ALLOYDB_HOST (default: localhost)
    - ALLOYDB_PORT (default: 6432)
    - ALLOYDB_USER (default: postgres)
    - ALLOYDB_DB (default: postgres)
    - ALLOYDB_PASSWORD (default: "")
    - DB_POOL_MIN_SIZE (default: 5)
    - DB_POOL_MAX_SIZE (default: 5)
    - DB_COMMAND_TIMEOUT_SEC (default: 30.0)
    - DB_CONNECT_TIMEOUT_SEC (default: 10.0)
    """
    host = os.environ.get("ALLOYDB_HOST", "localhost")
    port = int(os.environ.get("ALLOYDB_PORT", "6432"))
    user = os.environ.get("ALLOYDB_USER", "postgres")
    db_name = os.environ.get("ALLOYDB_DB", "postgres")
    password = os.environ.get("ALLOYDB_PASSWORD", "")

    min_size = int(os.environ.get("DB_POOL_MIN_SIZE", "5"))
    max_size = int(os.environ.get("DB_POOL_MAX_SIZE", "5"))

    command_timeout = float(os.environ.get("DB_COMMAND_TIMEOUT_SEC", "30.0"))
    timeout = float(os.environ.get("DB_CONNECT_TIMEOUT_SEC", "10.0"))

    return await create_pool(
        host=host,
        port=port,
        user=user,
        db_name=db_name,
        password=password,
        min_size=min_size,
        max_size=max_size,
        command_timeout=command_timeout,
        timeout=timeout,
    )
