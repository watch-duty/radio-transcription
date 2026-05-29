from __future__ import annotations

import uuid
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import asyncpg


async def create_feed(pool: asyncpg.Pool) -> uuid.UUID:
    """Creates a dummy feed directly in the database for testing."""
    feed_id = uuid.uuid4()
    await pool.execute(
        "INSERT INTO feeds (id, name, source_type) VALUES ($1, $2, $3)",
        feed_id,
        "Test Feed",
        "bcfy_feeds",
    )
    return feed_id
