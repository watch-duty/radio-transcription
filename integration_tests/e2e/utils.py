import asyncio
import os
import uuid
from collections.abc import Generator

import asyncpg
import pytest
import requests

FEEDS_API_HOST = os.environ.get("FEEDS_API_HOST", "localhost:8089")


@pytest.fixture(name="test_feed")
def create_test_feed() -> Generator[tuple[str, str]]:
    """Fixture to create a temporary feed for testing."""
    feed_name = f"integration-test-feed-{uuid.uuid4()}"
    payload = {
        "name": feed_name,
        "source_type": "bcfy_feeds",
        "source_feed_id": f"src-{uuid.uuid4()}",
        "external_id": f"ext-{uuid.uuid4()}",
    }

    url = f"http://{FEEDS_API_HOST}/v1/feeds"
    response = requests.post(url, json=payload, timeout=10)
    response.raise_for_status()

    feed_id = response.json().get("id", "")
    if not feed_id:
        msg = "Feed ID not returned by API"
        raise ValueError(msg)

    try:
        yield feed_id, feed_name
    finally:
        # Clean up transcripts via DB
        _conn_kwargs = {
            "host": os.environ.get("ALLOYDB_HOST", "postgres"),
            "port": int(os.environ.get("ALLOYDB_PORT", "5432")),
            "user": os.environ.get("ALLOYDB_USER", "postgres"),
            "password": os.environ.get("ALLOYDB_PASSWORD", "postgres"),
            "database": os.environ.get("ALLOYDB_DB", "postgres"),
        }

        async def _cleanup_db() -> None:
            conn = await asyncpg.connect(**_conn_kwargs)
            await conn.execute(
                "DELETE FROM transcripts WHERE feed_id = $1::uuid", feed_id
            )
            await conn.close()

        asyncio.run(_cleanup_db())

        # Delete feed via API
        del_url = f"http://{FEEDS_API_HOST}/v1/feeds/{feed_id}"
        del_response = requests.delete(del_url, timeout=10)
        del_response.raise_for_status()
