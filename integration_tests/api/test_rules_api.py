"""Integration tests for the Rules Frontend Proxy and Backend Rules APIs."""

import os
import uuid
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from pathlib import Path

import httpx
import pytest

from integration_tests.feed_utils import create_test_bcfy_feed  # noqa: F401
from integration_tests.test_utils import DUMMY_JWT


@pytest.fixture(name="proxy_client")
async def create_proxy_client() -> AsyncIterator[httpx.AsyncClient]:
    async with httpx.AsyncClient(
        base_url=f"http://{os.environ.get('FRONTEND_API_HOST', 'localhost:8088')}/api/v1",
        headers={"Authorization": f"Bearer {DUMMY_JWT}"},
    ) as client:
        yield client


def get_rules_api_host() -> str:
    """Return the Rules API host for the current environment."""
    configured_host = os.environ.get("RULES_API_HOST")
    if configured_host:
        return configured_host

    if Path("/.dockerenv").exists():
        return "rules-management:8086"

    return "localhost:8086"


def get_audio_segments_api_host() -> str:
    """Return the Audio Segments API host for the current environment."""
    configured_host = os.environ.get("AUDIO_SEGMENTS_API_HOST")
    if configured_host:
        return configured_host

    if Path("/.dockerenv").exists():
        return "audio-segments-api:8091"

    return "localhost:8091"


@pytest.fixture(name="audio_client")
async def create_audio_client() -> AsyncIterator[httpx.AsyncClient]:
    """Sets up client for audio segment requests."""
    async with httpx.AsyncClient(
        base_url=f"http://{get_audio_segments_api_host()}/v1"
    ) as client:
        yield client


@pytest.fixture(name="api_client")
async def create_api_client() -> AsyncIterator[httpx.AsyncClient]:
    """Sets up client for requests."""
    async with httpx.AsyncClient(
        base_url=f"http://{get_rules_api_host()}/v1"
    ) as client:
        yield client


@pytest.mark.asyncio
async def test_create_rule_proxy(
    proxy_client: httpx.AsyncClient,
) -> None:
    """Test POST /rules via frontend proxy."""
    rule_name = f"Test Rule {uuid.uuid4()}"
    payload = {
        "ruleName": rule_name,
        "description": "Integration test rule description",
        "isActive": True,
        "scope": {
            "level": "GLOBAL",
            "targetFeeds": [],
        },
        "conditions": {
            "evaluationType": "KEYWORD_MATCH",
            "operator": "ANY",
            "keywords": ["wildfire", "evacuation"],
            "caseSensitive": False,
        },
    }

    resp = await proxy_client.post("/rules", json=payload, timeout=10.0)
    assert resp.status_code in [200, 201], f"Create rule failed: {resp.text}"
    data = resp.json()

    rule_id = data["ruleId"]
    assert data["ruleName"] == rule_name
    assert data["isActive"] is True
    assert data["scope"]["level"] == "GLOBAL"
    assert data["conditions"]["evaluationType"] == "KEYWORD_MATCH"
    assert data["conditions"]["keywords"] == ["wildfire", "evacuation"]

    # Cleanup rule
    await proxy_client.delete(f"/rules/{rule_id}", timeout=10.0)


@pytest.mark.asyncio
async def test_list_rules_proxy(
    proxy_client: httpx.AsyncClient,
) -> None:
    """Test GET /rules via frontend proxy."""
    rule_name = f"List Test Rule {uuid.uuid4()}"
    payload = {
        "ruleName": rule_name,
        "description": "List test rule description",
        "isActive": True,
        "scope": {
            "level": "GLOBAL",
            "targetFeeds": [],
        },
        "conditions": {
            "evaluationType": "KEYWORD_MATCH",
            "operator": "ALL",
            "keywords": ["structure", "fire"],
            "caseSensitive": False,
        },
    }

    create_resp = await proxy_client.post("/rules", json=payload, timeout=10.0)
    assert create_resp.status_code in [200, 201], (
        f"Create rule failed: {create_resp.text}"
    )
    rule_id = create_resp.json()["ruleId"]

    # List all rules
    resp = await proxy_client.get("/rules", timeout=10.0)
    assert resp.status_code == 200, f"List rules failed: {resp.text}"
    data = resp.json()
    assert isinstance(data, list)
    rule_ids = [r["ruleId"] for r in data]
    assert rule_id in rule_ids

    # List rules with ruleIds query filter
    filtered_resp = await proxy_client.get(
        "/rules", params={"ruleIds": [rule_id]}, timeout=10.0
    )
    assert filtered_resp.status_code == 200
    filtered_data = filtered_resp.json()
    assert isinstance(filtered_data, list)
    assert any(r["ruleId"] == rule_id for r in filtered_data)

    # Cleanup
    await proxy_client.delete(f"/rules/{rule_id}", timeout=10.0)


@pytest.mark.asyncio
async def test_get_rule_proxy(
    proxy_client: httpx.AsyncClient,
) -> None:
    """Test GET /rules/{ruleId} via frontend proxy."""
    rule_name = f"Get Test Rule {uuid.uuid4()}"
    payload = {
        "ruleName": rule_name,
        "description": "Get rule description",
        "isActive": True,
        "scope": {
            "level": "GLOBAL",
            "targetFeeds": [],
        },
        "conditions": {
            "evaluationType": "KEYWORD_MATCH",
            "operator": "ANY",
            "keywords": ["medical", "rescue"],
            "caseSensitive": True,
        },
    }

    create_resp = await proxy_client.post("/rules", json=payload, timeout=10.0)
    assert create_resp.status_code in [200, 201], (
        f"Create rule failed: {create_resp.text}"
    )
    rule_id = create_resp.json()["ruleId"]

    get_resp = await proxy_client.get(f"/rules/{rule_id}", timeout=10.0)
    assert get_resp.status_code == 200, f"Get rule failed: {get_resp.text}"
    data = get_resp.json()

    assert data["ruleId"] == rule_id
    assert data["ruleName"] == rule_name
    assert data["conditions"]["caseSensitive"] is True

    # Cleanup
    await proxy_client.delete(f"/rules/{rule_id}", timeout=10.0)


@pytest.mark.asyncio
async def test_get_rule_not_found_proxy(
    proxy_client: httpx.AsyncClient,
) -> None:
    """Test GET /rules/{non_existent_id} via frontend proxy returns 404."""
    non_existent_id = str(uuid.uuid4())
    resp = await proxy_client.get(f"/rules/{non_existent_id}", timeout=10.0)
    assert resp.status_code == 404, (
        f"Expected 404, got {resp.status_code}: {resp.text}"
    )


@pytest.mark.asyncio
async def test_update_rule_proxy(
    proxy_client: httpx.AsyncClient,
) -> None:
    """Test PUT /rules/{ruleId} via frontend proxy."""
    rule_name = f"Initial Rule Name {uuid.uuid4()}"
    payload = {
        "ruleName": rule_name,
        "description": "Initial description",
        "isActive": True,
        "scope": {
            "level": "GLOBAL",
            "targetFeeds": [],
        },
        "conditions": {
            "evaluationType": "KEYWORD_MATCH",
            "operator": "ANY",
            "keywords": ["hazmat"],
            "caseSensitive": False,
        },
    }

    create_resp = await proxy_client.post("/rules", json=payload, timeout=10.0)
    assert create_resp.status_code in [200, 201], (
        f"Create rule failed: {create_resp.text}"
    )
    rule_id = create_resp.json()["ruleId"]

    updated_name = f"Updated Rule Name {uuid.uuid4()}"
    update_payload = {
        "ruleName": updated_name,
        "description": "Updated description text",
        "isActive": False,
    }

    update_resp = await proxy_client.put(
        f"/rules/{rule_id}", json=update_payload, timeout=10.0
    )
    assert update_resp.status_code == 200, (
        f"Update rule failed: {update_resp.text}"
    )
    data = update_resp.json()

    assert data["ruleId"] == rule_id
    assert data["ruleName"] == updated_name
    assert data["description"] == "Updated description text"
    assert data["isActive"] is False

    # Cleanup
    await proxy_client.delete(f"/rules/{rule_id}", timeout=10.0)


@pytest.mark.asyncio
async def test_delete_rule_proxy(
    proxy_client: httpx.AsyncClient,
) -> None:
    """Test DELETE /rules/{ruleId} via frontend proxy."""
    rule_name = f"Delete Test Rule {uuid.uuid4()}"
    payload = {
        "ruleName": rule_name,
        "description": "Rule to delete",
        "isActive": True,
        "scope": {
            "level": "GLOBAL",
            "targetFeeds": [],
        },
        "conditions": {
            "evaluationType": "KEYWORD_MATCH",
            "operator": "ANY",
            "keywords": ["alarm"],
            "caseSensitive": False,
        },
    }

    create_resp = await proxy_client.post("/rules", json=payload, timeout=10.0)
    assert create_resp.status_code in [200, 201], (
        f"Create rule failed: {create_resp.text}"
    )
    rule_id = create_resp.json()["ruleId"]

    del_resp = await proxy_client.delete(f"/rules/{rule_id}", timeout=10.0)
    assert del_resp.status_code == 204, f"Delete rule failed: {del_resp.text}"

    # Verify GET returns 404 after deletion
    get_resp = await proxy_client.get(f"/rules/{rule_id}", timeout=10.0)
    assert get_resp.status_code == 404


@pytest.mark.asyncio
async def test_dry_run_rule(
    api_client: httpx.AsyncClient,
    audio_client: httpx.AsyncClient,
    test_bcfy_feed: tuple[str, str],
) -> None:
    """Test evaluating a prospective rule via dry-run."""
    feed_id, _ = test_bcfy_feed

    # 1. Seed actual data into the database via the Audio Segments API
    segment_id = str(uuid.uuid4())
    now = datetime.now(UTC)
    start_ts = now.isoformat()
    end_ts = (now + timedelta(minutes=1)).isoformat()

    await audio_client.post(
        "/audio_segments",
        json={
            "id": segment_id,
            "feed_id": feed_id,
            "classification": "SPEECH",
            "start_timestamp": start_ts,
            "end_timestamp": end_ts,
            "missing_prior_context": False,
            "missing_post_context": False,
            "source_audio_uris": ["gs://bucket/audio1.ogg"],
            "canonical_audio_uri": "gs://bucket/canonical.ogg",
        },
    )

    # Add a transcript containing the keyword "fire"
    await audio_client.post(
        f"/audio_segments/{segment_id}/annotations",
        json={
            "type": "TRANSCRIPT",
            "data": {
                "text": "A massive fire is spreading rapidly on the ridge.",
                "errors": [],
            },
        },
    )

    # 2. Execute the dry-run against the real database
    rule_payload = {
        "rule": {
            "rule_name": "Integration Test Dry Run",
            "description": "Test rule for dry-run",
            "is_active": True,
            "scope": {"level": "GLOBAL"},
            "conditions": {
                "evaluation_type": "KEYWORD_MATCH",
                "operator": "ANY",
                "keywords": ["fire", "evacuate"],
                "case_sensitive": False,
            },
        },
        "max_examples": 10,
        "feed_ids": [feed_id],
    }

    response = await api_client.post(
        "/rules/dry-run", json=rule_payload, timeout=10.0
    )

    assert response.status_code == 200, f"Dry-run failed: {response.text}"

    data = response.json()
    assert data["hit_count"] >= 1, (
        "Expected at least 1 hit from the seeded segment"
    )
    assert data["total_evaluated"] >= 1
    assert len(data["examples"]) >= 1
    assert data["examples"][0]["audio_segment_id"] == segment_id
