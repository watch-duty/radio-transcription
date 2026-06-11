"""
Integration tests configurations and fixtures.

⚠️ IMPORTANT: All tests in this directory MUST be written to support parallel execution
(via `pytest -n auto`). Do not use hardcoded or shared IDs, feeds, or channels that could
cause collisions between concurrent test runs. Always use `uuid.uuid4()` for unique names.
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Generator

import asyncpg
import docker
import pytest
from testcontainers.postgres import PostgresContainer

from backend.pipeline.common.test_schema_helper import async_apply_test_schema
from backend.pipeline.storage.connection import create_pool


def _docker_available() -> bool:
    try:
        docker.from_env().ping()
    except Exception:
        return False
    return True


@pytest.fixture(scope="session")
def postgres_container() -> Generator[dict[str, Any]]:
    """Start AlloyDB Omni container and apply schema once per session."""
    if not _docker_available():
        pytest.skip("Docker is not available")

    container = PostgresContainer(
        image="google/alloydbomni:15",
        username="postgres",
        password="postgres",
        dbname="postgres",
        driver=None,
    )
    container.with_command("postgres -c 'max_connections=100'")
    container.start()

    host = container.get_container_host_ip()
    port = int(container.get_exposed_port(5432))

    async def _setup_schema() -> None:
        conn = await asyncpg.connect(
            host=host,
            port=port,
            user="postgres",
            password="postgres",
            database="postgres",
        )
        await async_apply_test_schema(conn)
        await conn.close()

    # Use a fresh event loop for the setup since we're in a session fixture
    # which may run before any test event loop is started.

    asyncio.run(_setup_schema())

    yield {
        "host": host,
        "port": port,
        "user": "postgres",
        "password": "postgres",
        "database": "postgres",
    }

    container.stop()


@pytest.fixture
async def db_pool(
    postgres_container: dict[str, Any],
) -> AsyncIterator[asyncpg.Pool]:
    """Create an asyncpg pool for a single test."""
    pool = await create_pool(
        host=postgres_container["host"],
        port=postgres_container["port"],
        user=postgres_container["user"],
        password=postgres_container["password"],
        db_name=postgres_container["database"],
        min_size=2,
        max_size=5,
    )

    yield pool

    await pool.close()
