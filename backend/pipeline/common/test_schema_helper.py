"""Helper for executing idempotent SQL migration schemas in integration test databases."""

from __future__ import annotations

from pathlib import Path
from typing import Any

_REPO_ROOT = Path(__file__).resolve().parents[3]
_SQL_DIR = (
    _REPO_ROOT / "terraform" / "modules" / "alloydb" / "sql" / "ingestion"
)


def _has_sql(text: str) -> bool:
    """Returns True if ``text`` contains anything other than ``--`` comments.

    Disabled migrations keep their statements commented out.
    psql tolerates a comment-only query, but asyncpg raises on the EmptyQueryResponse.
    """
    return any(
        line.strip() and not line.lstrip().startswith("--")
        for line in text.splitlines()
    )


def _iter_statements(content: str) -> list[str]:
    """Splits file content into the chunks to execute, dropping comment-only ones."""
    if content.startswith("-- AUTOCOMMIT"):
        chunks = [s.strip() for s in content.split(";")]
    else:
        chunks = [content]
    return [chunk for chunk in chunks if _has_sql(chunk)]


def _sql_files() -> list[Path]:
    return sorted(
        (f for f in _SQL_DIR.glob("*.sql") if "pg_cron" not in f.name),
        key=lambda f: f.name,
    )


async def async_apply_test_schema(conn: Any) -> None:
    """Applies all ingestion SQL migration files in filename order using asyncpg."""
    for sql_file in _sql_files():
        for statement in _iter_statements(sql_file.read_text()):
            await conn.execute(statement)


def sync_apply_test_schema(conn: Any) -> None:
    """Applies all ingestion SQL migration files in filename order using psycopg."""
    for sql_file in _sql_files():
        for statement in _iter_statements(sql_file.read_text()):
            conn.execute(statement.encode())
