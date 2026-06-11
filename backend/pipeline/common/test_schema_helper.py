"""Helper for executing idempotent SQL migration schemas in integration test databases."""

from __future__ import annotations

from pathlib import Path
from typing import Any

_REPO_ROOT = Path(__file__).resolve().parents[3]
_SQL_DIR = (
    _REPO_ROOT / "terraform" / "modules" / "alloydb" / "sql" / "ingestion"
)


async def async_apply_test_schema(conn: Any) -> None:
    """Applies all ingestion SQL migration files in filename order using asyncpg."""
    sql_files = sorted(
        (f for f in _SQL_DIR.glob("*.sql") if "pg_cron" not in f.name),
        key=lambda f: f.name,
    )
    for sql_file in sql_files:
        content = sql_file.read_text()
        if content.startswith("-- AUTOCOMMIT"):
            for statement in content.split(";"):
                if statement.strip():
                    await conn.execute(statement.strip())
        else:
            await conn.execute(content)


def sync_apply_test_schema(conn: Any) -> None:
    """Applies all ingestion SQL migration files in filename order using psycopg."""
    sql_files = sorted(
        (f for f in _SQL_DIR.glob("*.sql") if "pg_cron" not in f.name),
        key=lambda f: f.name,
    )
    for sql_file in sql_files:
        content = sql_file.read_text()
        if content.startswith("-- AUTOCOMMIT"):
            for statement in content.split(";"):
                if statement.strip():
                    conn.execute(statement.strip().encode())
        else:
            conn.execute(content.encode())
