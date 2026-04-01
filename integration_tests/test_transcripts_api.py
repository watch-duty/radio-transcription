"""Integration tests for the Transcripts API."""

import os
import uuid
from collections.abc import AsyncIterator

import asyncpg
import httpx
import pytest

TRANSCRIPTS_API_HOST = os.environ.get("TRANSCRIPTS_API_HOST", "localhost:8087")


async def _get_db_connection() -> asyncpg.Connection:
    return await asyncpg.connect(
        host=os.environ.get("ALLOYDB_HOST", "postgres"),
        port=int(os.environ.get("ALLOYDB_PORT", "5432")),
        user=os.environ.get("ALLOYDB_USER", "postgres"),
        password=os.environ.get("ALLOYDB_PASSWORD", "postgres"),
        database=os.environ.get("ALLOYDB_DB", "postgres"),
    )


@pytest.fixture
async def api_client() -> AsyncIterator[httpx.AsyncClient]:
    """Fixture that yields an httpx.AsyncClient with base_url."""
    async with httpx.AsyncClient(
        base_url=f"http://{TRANSCRIPTS_API_HOST}/v1"
    ) as client:
        yield client


@pytest.fixture
async def test_feed() -> AsyncIterator[str]:
    """Feed set up, which are required to associated with transcripts."""
    conn = await _get_db_connection()
    feed_name = f"integration-test-feed-{uuid.uuid4()}"
    feed_id = await conn.fetchval(
        "INSERT INTO feeds (name, source_type) VALUES ($1, $2) RETURNING id",
        feed_name,
        "bcfy_feeds",
    )
    feed_id_str = str(feed_id)
    try:
        yield feed_id_str
    finally:
        await conn.execute("DELETE FROM feeds WHERE id = $1::uuid", feed_id_str)
        await conn.close()


@pytest.mark.asyncio
async def test_transcripts_api(
    api_client: httpx.AsyncClient, test_feed: str
) -> None:
    transmission_id = str(uuid.uuid4())
    transcript_text = "Hello integration test for transcripts API"

    payload = {
        "feed_id": test_feed,
        "transmission_id": transmission_id,
        "transcript": transcript_text,
    }

    # 1. Create transcript
    response = await api_client.post("/transcripts", json=payload, timeout=10.0)
    assert response.status_code == 201, f"Failed to create: {response.text}"
    created_data = response.json()
    assert created_data["transmission_id"] == transmission_id
    assert created_data["transcript"] == transcript_text

    # 2. List transcripts and verify it's there
    response = await api_client.get("/transcripts", timeout=10.0)
    assert response.status_code == 200, f"Failed to list: {response.text}"
    transcripts_list = response.json()
    found = any(
        item["transmission_id"] == transmission_id for item in transcripts_list
    )
    assert found, f"Created transcript {transmission_id} not found in listing"

    # 3. Get specific transcript
    response = await api_client.get(
        f"/transcripts/{transmission_id}", timeout=10.0
    )
    assert response.status_code == 200, f"Failed to get: {response.text}"
    get_data = response.json()
    assert get_data["transmission_id"] == transmission_id

    # 4. Delete transcript
    response = await api_client.delete(
        f"/transcripts/{transmission_id}", timeout=10.0
    )
    assert response.status_code == 204, f"Failed to delete: {response.text}"

    # 5. Verify deletion
    response = await api_client.get(
        f"/transcripts/{transmission_id}", timeout=10.0
    )
    assert response.status_code == 404, (
        f"Expected 404 after delete, got {response.status_code}"
    )
