"""Integration tests for the Rules Frontend Proxy API."""

import os
import uuid
from collections.abc import AsyncIterator

import httpx
import pytest

from integration_tests.test_utils import DUMMY_JWT


@pytest.fixture(name="proxy_client")
async def create_proxy_client() -> AsyncIterator[httpx.AsyncClient]:
    async with httpx.AsyncClient(
        base_url=f"http://{os.environ.get('FRONTEND_API_HOST', 'localhost:8088')}/api/v1",
        headers={"Authorization": f"Bearer {DUMMY_JWT}"},
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
