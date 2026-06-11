"""Integration tests for the Transcripts API."""

import os
import uuid
from collections.abc import AsyncIterator

import asyncpg
import httpx
import pytest

from integration_tests.feed_utils import create_test_bcfy_feed  # noqa: F401

TRANSCRIPTS_API_HOST = os.environ.get("TRANSCRIPTS_API_HOST", "localhost:8087")


async def _get_db_connection() -> asyncpg.Connection:
    return await asyncpg.connect(
        host=os.environ.get("ALLOYDB_HOST", "postgres"),
        port=int(os.environ.get("ALLOYDB_PORT", "5432")),
        user=os.environ.get("ALLOYDB_USER", "postgres"),
        password=os.environ.get("ALLOYDB_PASSWORD", "postgres"),
        database=os.environ.get("ALLOYDB_DB", "postgres"),
    )


@pytest.fixture(name="api_client")
async def create_api_client() -> AsyncIterator[httpx.AsyncClient]:
    """Sets up client for requests."""
    async with httpx.AsyncClient(
        base_url=f"http://{TRANSCRIPTS_API_HOST}/v1"
    ) as client:
        yield client


@pytest.mark.asyncio
async def test_transcripts_api(
    api_client: httpx.AsyncClient, test_bcfy_feed: tuple[str, str]
) -> None:
    segment_id = str(uuid.uuid4())
    transcript_text = "Hello integration test for transcripts API"

    feed_id, _ = test_bcfy_feed
    payload = {
        "feed_id": feed_id,
        "segment_id": segment_id,
        "transcript": transcript_text,
    }

    # 1. Create transcript
    response = await api_client.post("/transcripts", json=payload, timeout=10.0)
    assert response.status_code == 201, f"Failed to create: {response.text}"
    created_data = response.json()
    assert created_data["segment_id"] == segment_id
    assert created_data["transcript"] == transcript_text

    # 2. List transcripts and verify it's there
    response = await api_client.get("/transcripts", timeout=10.0)
    assert response.status_code == 200, f"Failed to list: {response.text}"
    data = response.json()
    assert "transcripts" in data
    found = any(
        item["segment_id"] == segment_id for item in data["transcripts"]
    )
    assert found, f"Created transcript {segment_id} not found in listing"

    # 3. Get specific transcript
    response = await api_client.get(f"/transcripts/{segment_id}", timeout=10.0)
    assert response.status_code == 200, f"Failed to get: {response.text}"
    get_data = response.json()
    assert get_data["segment_id"] == segment_id

    # 4. Delete transcript
    response = await api_client.delete(
        f"/transcripts/{segment_id}", timeout=10.0
    )
    assert response.status_code == 204, f"Failed to delete: {response.text}"

    # 5. Verify deletion
    response = await api_client.get(f"/transcripts/{segment_id}", timeout=10.0)
    assert response.status_code == 404, (
        f"Expected 404 after delete, got {response.status_code}"
    )


@pytest.mark.asyncio
async def test_transcripts_api_duplicate_idempotent(
    api_client: httpx.AsyncClient, test_bcfy_feed: tuple[str, str]
) -> None:
    """Verify that creating a transcript with a duplicate segment_id is idempotent and returns 201."""
    segment_id = str(uuid.uuid4())
    transcript_text = "Hello integration test for duplicate conflict"

    feed_id, _ = test_bcfy_feed
    payload = {
        "feed_id": feed_id,
        "segment_id": segment_id,
        "transcript": transcript_text,
    }

    # 1. Create transcript
    response = await api_client.post("/transcripts", json=payload, timeout=10.0)
    assert response.status_code == 201, f"Failed to create: {response.text}"

    # 2. Attempt to create duplicate
    response = await api_client.post("/transcripts", json=payload, timeout=10.0)
    assert response.status_code == 201, (
        f"Expected 201 Created, got {response.status_code}: {response.text}"
    )

    # 3. Cleanup
    response = await api_client.delete(
        f"/transcripts/{segment_id}", timeout=10.0
    )
    assert response.status_code == 204, f"Failed to delete: {response.text}"
