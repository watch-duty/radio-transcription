"""Integration tests for the Audio Segments API."""

import os
import uuid
from collections.abc import AsyncIterator
from pathlib import Path

import httpx
import pytest

from integration_tests.feed_utils import create_test_bcfy_feed  # noqa: F401


def get_audio_segments_api_host() -> str:
    """Return the Audio Segments API host for the current environment."""
    configured_host = os.environ.get("AUDIO_SEGMENTS_API_HOST")
    if configured_host:
        return configured_host

    if Path("/.dockerenv").exists():
        return "audio-segments-api:8091"

    return "localhost:8091"


@pytest.fixture(name="api_client")
async def create_api_client() -> AsyncIterator[httpx.AsyncClient]:
    """Sets up client for requests."""
    async with httpx.AsyncClient(
        base_url=f"http://{get_audio_segments_api_host()}/v1"
    ) as client:
        yield client


@pytest.mark.asyncio
async def test_audio_segments_api_routes(
    api_client: httpx.AsyncClient, test_bcfy_feed: tuple[str, str]
) -> None:
    feed_id, _ = test_bcfy_feed
    segment_id = str(uuid.uuid4())
    segment_data = {
        "id": segment_id,
        "feed_id": feed_id,
        "classification": "SPEECH_DETECTED",
        "start_timestamp": "2026-01-01T10:00:00Z",
        "end_timestamp": "2026-01-01T10:01:00Z",
        "missing_prior_context": False,
        "missing_post_context": False,
        "source_audio_uris": ["gs://bucket/audio1.ogg"],
        "canonical_audio_uri": "gs://bucket/canonical.ogg",
        "start_audio_offset": "PT5S",
        "end_audio_offset": "PT10S",
        "playback_audio_uri": None,
    }

    # 1. POST: Create an audio segment
    post_res = await api_client.post(
        "/audio_segments", json=segment_data, timeout=10.0
    )
    assert post_res.status_code == 201, f"Failed to create: {post_res.text}"
    created_segment = post_res.json()
    assert created_segment["id"] == segment_id
    assert created_segment["feed_id"] == feed_id
    assert created_segment["classification"] == "SPEECH_DETECTED"
    assert created_segment["start_timestamp"].startswith("2026-01-01T10:00:00")
    assert created_segment["source_audio_uris"] == ["gs://bucket/audio1.ogg"]

    segment_id = created_segment["id"]

    # 2. GET: List audio segments and verify (filtering by feed_id)
    get_res = await api_client.get(
        "/audio_segments", params={"feed_ids": [feed_id]}, timeout=10.0
    )
    assert get_res.status_code == 200, f"Failed to list: {get_res.text}"
    data = get_res.json()
    assert len(data["segments"]) >= 1
    found = any(item["id"] == segment_id for item in data["segments"])
    assert found, f"Created segment {segment_id} not found in /audio_segments"

    # 3. POST: Add an annotation to the audio segment
    annotation_data = {
        "type": "TRANSCRIPT",
        "data": {"text": "hello integration test", "errors": []},
    }
    ann_res = await api_client.post(
        f"/audio_segments/{segment_id}/annotations",
        json=annotation_data,
        timeout=10.0,
    )
    assert ann_res.status_code == 201, (
        f"Failed to add annotation: {ann_res.text}"
    )
    ann = ann_res.json()
    assert ann["audio_segment_id"] == segment_id
    assert ann["type"] == "TRANSCRIPT"
    assert ann["data"]["text"] == "hello integration test"

    # 4. GET: List audio segments to confirm annotation is retrieved
    get_res2 = await api_client.get(
        "/audio_segments", params={"feed_ids": [feed_id]}, timeout=10.0
    )
    assert get_res2.status_code == 200
    data2 = get_res2.json()
    target_segment = next(
        item for item in data2["segments"] if item["id"] == segment_id
    )
    assert len(target_segment["annotations"]) >= 1
    first_ann = target_segment["annotations"][0]
    assert first_ann["type"] == "TRANSCRIPT"
    assert first_ann["data"]["text"] == "hello integration test"
