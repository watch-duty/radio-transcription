"""Synchronous database connection for event-driven Cloud Run services.

Uses psycopg (sync) rather than asyncpg because the echo collector runs
inside Cloud Functions Framework, which expects a synchronous handler.
Each call opens a fresh connection; pgBouncer provides server-side pooling.
"""

import psycopg
from psycopg.rows import dict_row

from backend.pipeline.storage.settings import AlloyDBSettings


def connect_db(
    settings: AlloyDBSettings | None = None,
) -> psycopg.Connection:
    """Open a sync psycopg connection to AlloyDB via pgBouncer.

    No pool needed when the caller handles at most one request at a time
    (e.g. Cloud Run with concurrency=1). pgBouncer provides server-side
    pooling.

    If *settings* is ``None``, an :class:`AlloyDBSettings` is constructed
    from environment variables.
    """
    if settings is None:
        settings = AlloyDBSettings()
    return psycopg.connect(  # type: ignore[return-value]
        host=settings.host,
        port=settings.port,
        user=settings.user,
        password=settings.password,
        dbname=settings.db_name,
        autocommit=True,
        row_factory=dict_row,  # type: ignore[arg-type]
    )
