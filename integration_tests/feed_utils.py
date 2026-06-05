import asyncio
import os
import uuid
from collections.abc import Generator

import asyncpg
import pytest
import requests

FEEDS_API_HOST = os.environ.get("FEEDS_API_HOST", "localhost:8089")


def _create_and_cleanup_feed(
    payload: dict[str, str],
) -> Generator[tuple[str, str]]:
    """Helper to create a feed via API and clean up after test."""
    url = f"http://{FEEDS_API_HOST}/v1/feeds"
    response = requests.post(url, json=payload, timeout=10)
    response.raise_for_status()

    feed_id = response.json().get("id", "")
    if not feed_id:
        msg = "Feed ID not returned by API"
        raise ValueError(msg)

    try:
        yield feed_id, payload["name"]
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
                "DELETE FROM audio_segments WHERE feed_id = $1::uuid", feed_id
            )
            await conn.execute(
                "DELETE FROM transcripts WHERE feed_id = $1::uuid", feed_id
            )
            await conn.close()

        asyncio.run(_cleanup_db())

        # Delete feed via API
        del_url = f"http://{FEEDS_API_HOST}/v1/feeds/{feed_id}/deactivate"
        del_response = requests.post(del_url, timeout=10)
        del_response.raise_for_status()


@pytest.fixture(name="test_bcfy_feed")
def create_test_bcfy_feed() -> Generator[tuple[str, str]]:
    """Fixture to create a temporary BCFY feed for testing.

    Yields:
        tuple[str, str]: A tuple containing (feed_id, source_feed_id).
    """
    feed_name = f"integration-test-feed-{uuid.uuid4()}"
    payload = {
        "name": feed_name,
        "source_type": "bcfy_feeds",
        "source_feed_id": str(uuid.uuid4().fields[0]),
    }
    yield from _create_and_cleanup_feed(payload)


@pytest.fixture(name="test_polling_feed")
def create_test_polling_feed() -> Generator[tuple[str, str]]:
    """Fixture to create a temporary polling feed for testing.

    Yields:
        tuple[str, str]: A tuple containing (feed_id, feed_name).
    """
    feed_name = f"integration-test-polling-feed-{uuid.uuid4()}"
    payload = {
        "name": feed_name,
        "source_type": "bcfy_calls",
        "source_feed_id": f"{uuid.uuid4().fields[0]}-{uuid.uuid4().fields[1]}",
    }
    yield from _create_and_cleanup_feed(payload)


@pytest.fixture(name="test_echo_feed")
def create_test_echo_feed() -> Generator[tuple[str, str]]:
    """Fixture to create a temporary echo feed for testing.

    Yields:
        tuple[str, str]: A tuple containing (feed_id, source_feed_id).
    """
    feed_name = f"integration-test-echo-feed-{uuid.uuid4()}"
    payload = {
        "name": feed_name,
        "source_type": "echo",
        "source_feed_id": f"src-{uuid.uuid4()}",
    }
    gen = _create_and_cleanup_feed(payload)
    feed_id, _ = next(gen)
    try:
        yield feed_id, payload["source_feed_id"]
    finally:
        try:
            next(gen)
        except StopIteration:
            pass
