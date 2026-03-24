from __future__ import annotations

import asyncio
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Generator

import asyncpg
import docker
import pytest
from testcontainers.postgres import PostgresContainer

from backend.pipeline.storage.connection import create_pool

_REPO_ROOT = Path(__file__).resolve().parents[2]
_SQL_DIR = (
    _REPO_ROOT / "terraform" / "modules" / "alloydb" / "sql" / "ingestion"
)


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
        # Apply schema files in order
        sql_files = sorted(_SQL_DIR.glob("*.sql"))
        for sql_file in sql_files:
            await conn.execute(sql_file.read_text())
        await conn.close()

    # Use a fresh event loop for the setup
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
