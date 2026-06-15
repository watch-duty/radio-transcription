"""Integration tests for the Audio Segments API."""

import os
import uuid
from collections.abc import AsyncIterator
from pathlib import Path

import httpx
import pytest

from integration_tests.api import test_feeds_api
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


@pytest.fixture(name="proxy_client")
async def create_proxy_client() -> AsyncIterator[httpx.AsyncClient]:
    """Sets up proxy client for frontend-api requests."""
    async with httpx.AsyncClient(
        base_url=f"http://{os.environ.get('FRONTEND_API_HOST', 'localhost:8088')}/api/v1",
        headers={"Authorization": f"Bearer {test_feeds_api.DUMMY_JWT}"},
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
        "classification": "SPEECH",
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
    assert created_segment["classification"] == "SPEECH"
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


@pytest.mark.asyncio
async def test_audio_segments_is_alert_filtering_direct(
    api_client: httpx.AsyncClient,
    test_bcfy_feed: tuple[str, str],
) -> None:
    feed_id, _ = test_bcfy_feed

    # Create segment 1 (has alert)
    seg1_id = str(uuid.uuid4())
    seg1_data = {
        "id": seg1_id,
        "feed_id": feed_id,
        "classification": "SPEECH",
        "start_timestamp": "2026-01-01T11:00:00Z",
        "end_timestamp": "2026-01-01T11:01:00Z",
        "missing_prior_context": False,
        "missing_post_context": False,
        "source_audio_uris": ["gs://bucket/audio1.ogg"],
    }
    res = await api_client.post("/audio_segments", json=seg1_data, timeout=10.0)
    assert res.status_code == 201

    ann1_data = {
        "type": "EVALUATION",
        "data": {"decisions": ["rule-abc"], "errors": []},
    }
    res = await api_client.post(
        f"/audio_segments/{seg1_id}/annotations", json=ann1_data, timeout=10.0
    )
    assert res.status_code == 201

    # Create segment 2 (no alert)
    seg2_id = str(uuid.uuid4())
    seg2_data = {
        "id": seg2_id,
        "feed_id": feed_id,
        "classification": "SPEECH",
        "start_timestamp": "2026-01-01T11:02:00Z",
        "end_timestamp": "2026-01-01T11:03:00Z",
        "missing_prior_context": False,
        "missing_post_context": False,
        "source_audio_uris": ["gs://bucket/audio2.ogg"],
    }
    res = await api_client.post("/audio_segments", json=seg2_data, timeout=10.0)
    assert res.status_code == 201

    ann2_data = {
        "type": "EVALUATION",
        "data": {"decisions": [], "errors": []},
    }
    res = await api_client.post(
        f"/audio_segments/{seg2_id}/annotations", json=ann2_data, timeout=10.0
    )
    assert res.status_code == 201

    # Test direct python api filtering with is_alert=true
    direct_res_true = await api_client.get(
        "/audio_segments",
        params={"feed_ids": [feed_id], "is_alert": True},
        timeout=10.0,
    )
    assert direct_res_true.status_code == 200, f"Failed: {direct_res_true.text}"
    data_true = direct_res_true.json()
    segments_true = [s["id"] for s in data_true["segments"]]
    assert seg1_id in segments_true
    assert seg2_id not in segments_true

    # Test direct python api filtering with is_alert=false
    direct_res_false = await api_client.get(
        "/audio_segments",
        params={"feed_ids": [feed_id], "is_alert": False},
        timeout=10.0,
    )
    assert direct_res_false.status_code == 200, (
        f"Failed: {direct_res_false.text}"
    )
    data_false = direct_res_false.json()
    segments_false = [s["id"] for s in data_false["segments"]]
    assert seg1_id not in segments_false
    assert seg2_id in segments_false


@pytest.mark.asyncio
async def test_audio_segments_is_alert_filtering_fe_proxy(
    api_client: httpx.AsyncClient,
    proxy_client: httpx.AsyncClient,
    test_bcfy_feed: tuple[str, str],
) -> None:
    feed_id, _ = test_bcfy_feed

    # Create segment 1 (has alert)
    seg1_id = str(uuid.uuid4())
    seg1_data = {
        "id": seg1_id,
        "feed_id": feed_id,
        "classification": "SPEECH",
        "start_timestamp": "2026-01-01T11:00:00Z",
        "end_timestamp": "2026-01-01T11:01:00Z",
        "missing_prior_context": False,
        "missing_post_context": False,
        "source_audio_uris": ["gs://bucket/audio1.ogg"],
    }
    res = await api_client.post("/audio_segments", json=seg1_data, timeout=10.0)
    assert res.status_code == 201

    ann1_data = {
        "type": "EVALUATION",
        "data": {"decisions": ["rule-abc"], "errors": []},
    }
    res = await api_client.post(
        f"/audio_segments/{seg1_id}/annotations", json=ann1_data, timeout=10.0
    )
    assert res.status_code == 201

    # Create segment 2 (no alert)
    seg2_id = str(uuid.uuid4())
    seg2_data = {
        "id": seg2_id,
        "feed_id": feed_id,
        "classification": "SPEECH",
        "start_timestamp": "2026-01-01T11:02:00Z",
        "end_timestamp": "2026-01-01T11:03:00Z",
        "missing_prior_context": False,
        "missing_post_context": False,
        "source_audio_uris": ["gs://bucket/audio2.ogg"],
    }
    res = await api_client.post("/audio_segments", json=seg2_data, timeout=10.0)
    assert res.status_code == 201

    ann2_data = {
        "type": "EVALUATION",
        "data": {"decisions": [], "errors": []},
    }
    res = await api_client.post(
        f"/audio_segments/{seg2_id}/annotations", json=ann2_data, timeout=10.0
    )
    assert res.status_code == 201

    # Test frontend gateway proxy api filtering with isAlert=true
    proxy_res_true = await proxy_client.get(
        f"/audioSegments/{feed_id}",
        params={"isAlert": True},
        timeout=10.0,
    )
    assert proxy_res_true.status_code == 200, f"Failed: {proxy_res_true.text}"
    proxy_data_true = proxy_res_true.json()
    proxy_segs_true = [s["id"] for s in proxy_data_true["segments"]]
    assert seg1_id in proxy_segs_true
    assert seg2_id not in proxy_segs_true

    # Test frontend gateway proxy api filtering with isAlert=false
    proxy_res_false = await proxy_client.get(
        f"/audioSegments/{feed_id}",
        params={"isAlert": False},
        timeout=10.0,
    )
    assert proxy_res_false.status_code == 200, f"Failed: {proxy_res_false.text}"
    proxy_data_false = proxy_res_false.json()
    proxy_segs_false = [s["id"] for s in proxy_data_false["segments"]]
    assert seg1_id not in proxy_segs_false
    assert seg2_id in proxy_segs_false


@pytest.mark.asyncio
async def test_audio_segments_pagination_direct(
    api_client: httpx.AsyncClient,
    test_bcfy_feed: tuple[str, str],
) -> None:
    feed_id, _ = test_bcfy_feed

    # Create 3 segments
    seg_ids = []
    for i in range(3):
        seg_id = str(uuid.uuid4())
        seg_ids.append(seg_id)
        seg_data = {
            "id": seg_id,
            "feed_id": feed_id,
            "classification": "SPEECH",
            "start_timestamp": f"2026-01-01T12:0{i}:00Z",
            "end_timestamp": f"2026-01-01T12:0{i + 1}:00Z",
            "missing_prior_context": False,
            "missing_post_context": False,
            "source_audio_uris": [f"gs://bucket/audio{i}.ogg"],
        }
        res = await api_client.post(
            "/audio_segments", json=seg_data, timeout=10.0
        )
        assert res.status_code == 201

    # Fetch page 1 (limit=2)
    # The default order is DESC, so it should return the latest created segments first (seg2, seg1)
    res_p1 = await api_client.get(
        "/audio_segments",
        params={"feed_ids": [feed_id], "limit": 2},
        timeout=10.0,
    )
    assert res_p1.status_code == 200
    data_p1 = res_p1.json()
    assert len(data_p1["segments"]) == 2
    assert data_p1["segments"][0]["id"] == seg_ids[2]
    assert data_p1["segments"][1]["id"] == seg_ids[1]
    assert data_p1.get("next_token") is not None

    # Fetch page 2 (with next_token, limit=2)
    res_p2 = await api_client.get(
        "/audio_segments",
        params={
            "feed_ids": [feed_id],
            "limit": 2,
            "next_token": data_p1["next_token"],
        },
        timeout=10.0,
    )
    assert res_p2.status_code == 200
    data_p2 = res_p2.json()
    assert len(data_p2["segments"]) == 1
    assert data_p2["segments"][0]["id"] == seg_ids[0]
    assert data_p2.get("next_token") is None


@pytest.mark.asyncio
async def test_audio_segments_pagination_fe_proxy(
    api_client: httpx.AsyncClient,
    proxy_client: httpx.AsyncClient,
    test_bcfy_feed: tuple[str, str],
) -> None:
    feed_id, _ = test_bcfy_feed

    # Create 3 segments (using api_client)
    seg_ids = []
    for i in range(3):
        seg_id = str(uuid.uuid4())
        seg_ids.append(seg_id)
        seg_data = {
            "id": seg_id,
            "feed_id": feed_id,
            "classification": "SPEECH",
            "start_timestamp": f"2026-01-01T12:0{i}:00Z",
            "end_timestamp": f"2026-01-01T12:0{i + 1}:00Z",
            "missing_prior_context": False,
            "missing_post_context": False,
            "source_audio_uris": [f"gs://bucket/audio{i}.ogg"],
        }
        res = await api_client.post(
            "/audio_segments", json=seg_data, timeout=10.0
        )
        assert res.status_code == 201

    # Fetch page 1 (limit=2) via proxy_client
    res_p1 = await proxy_client.get(
        f"/audioSegments/{feed_id}",
        params={"limit": 2},
        timeout=10.0,
    )
    assert res_p1.status_code == 200
    data_p1 = res_p1.json()
    assert len(data_p1["segments"]) == 2
    assert data_p1["segments"][0]["id"] == seg_ids[2]
    assert data_p1["segments"][1]["id"] == seg_ids[1]
    assert data_p1.get("nextToken") is not None

    # Fetch page 2 (with nextToken, limit=2) via proxy_client
    res_p2 = await proxy_client.get(
        f"/audioSegments/{feed_id}",
        params={
            "limit": 2,
            "nextToken": data_p1["nextToken"],
        },
        timeout=10.0,
    )
    assert res_p2.status_code == 200
    data_p2 = res_p2.json()
    assert len(data_p2["segments"]) == 1
    assert data_p2["segments"][0]["id"] == seg_ids[0]
    assert data_p2.get("nextToken") is None
