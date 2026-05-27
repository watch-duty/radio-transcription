"""Integration tests for the Audio Segments API."""

import os
import uuid
from collections.abc import AsyncIterator

import httpx
import pytest

from integration_tests.feed_utils import create_test_bcfy_feed  # noqa: F401

AUDIO_SEGMENTS_API_HOST = os.environ.get(
    "AUDIO_SEGMENTS_API_HOST", "localhost:8091"
)


@pytest.fixture(name="api_client")
async def create_api_client() -> AsyncIterator[httpx.AsyncClient]:
    """Sets up client for requests."""
    async with httpx.AsyncClient(
        base_url=f"http://{AUDIO_SEGMENTS_API_HOST}/v1"
    ) as client:
        yield client


@pytest.mark.asyncio
async def test_audio_segments_api_routes(
    api_client: httpx.AsyncClient, test_bcfy_feed: tuple[str, str]
) -> None:
    feed_id, _ = test_bcfy_feed
    segment_id = str(uuid.uuid4())

    payload = {
        "audio_segments": [
            {
                "id": segment_id,
                "feed_id": feed_id,
                "classification": "SPEECH_DETECTED",
                "start_timestamp": "2026-01-01T00:00:00Z",
                "end_timestamp": "2026-01-01T00:01:00Z",
                "missing_prior_context": False,
                "missing_post_context": False,
                "source_audio_uris": ["gs://bucket/audio1.ogg"],
                "canonical_audio_uri": "gs://bucket/canonical.ogg",
                "start_audio_offset": "PT5S",
                "end_audio_offset": "PT10S",
                "playback_audio_uri": None,
            }
        ]
    }

    # 1. Bulk add audio segments
    response = await api_client.post(
        "/audio_segments", json=payload, timeout=10.0
    )
    assert response.status_code == 201, f"Failed to bulk add: {response.text}"
    added_data = response.json()
    assert added_data["inserted_count"] == 1

    # 2. List audio segments and verification (filtering by feed_id)
    response = await api_client.get(
        "/audio_segments", params={"feed_ids": [feed_id]}, timeout=10.0
    )
    assert response.status_code == 200, f"Failed to list: {response.text}"
    data = response.json()
    assert len(data) >= 1
    found = any(item["id"] == segment_id for item in data)
    assert found, f"Created segment {segment_id} not found in /audio_segments"

    # 3. Idempotency Check: Post duplicate segment
    response = await api_client.post(
        "/audio_segments", json=payload, timeout=10.0
    )
    assert response.status_code == 201, (
        f"Failed bulk add retry: {response.text}"
    )
    added_data = response.json()
    assert added_data["inserted_count"] == 0  # Duplicate segment, nothing inserted
