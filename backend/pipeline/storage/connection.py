from __future__ import annotations

import asyncpg


async def create_pool(  # noqa: PLR0913
    host: str,
    user: str,
    db_name: str,
    password: str = "",
    port: int = 5432,
    min_size: int = 10,
    max_size: int = 10,
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
        port: Database port (default 5432).
        min_size: Minimum number of connections in the pool.
        max_size: Maximum number of connections in the pool.
        command_timeout: Query execution timeout in seconds.
        timeout: TCP connection timeout in seconds.

    Returns:
        An asyncpg connection pool.

    """
    kwargs: dict = {
        "host": host,
        "port": port,
        "user": user,
        "password": password,
        "database": db_name,
        "min_size": min_size,
        "max_size": max_size,
    }
    if command_timeout is not None:
        kwargs["command_timeout"] = command_timeout
    if timeout is not None:
        kwargs["timeout"] = timeout
    return await asyncpg.create_pool(**kwargs)


async def close_pool(pool: asyncpg.Pool) -> None:
    """Close an asyncpg connection pool."""
    await pool.close()
