"""Integration tests for the Feeds API and Frontend Feeds Proxy API."""

import os
import uuid
from collections.abc import AsyncIterator

import httpx
import pytest
from google.cloud import storage

from integration_tests.feed_utils import create_test_bcfy_feed  # noqa: F401
from integration_tests.test_utils import DUMMY_JWT


@pytest.fixture(name="backend_client")
async def create_backend_client() -> AsyncIterator[httpx.AsyncClient]:
    async with httpx.AsyncClient(
        base_url=f"http://{os.environ.get('FEEDS_API_HOST', 'localhost:8089')}/v1"
    ) as client:
        yield client


@pytest.fixture(name="proxy_client")
async def create_proxy_client() -> AsyncIterator[httpx.AsyncClient]:
    async with httpx.AsyncClient(
        base_url=f"http://{os.environ.get('FRONTEND_API_HOST', 'localhost:8088')}/api/v1",
        headers={"Authorization": f"Bearer {DUMMY_JWT}"},
    ) as client:
        yield client


@pytest.fixture(name="gcs_client")
def create_gcs_client() -> storage.Client:
    return storage.Client()


@pytest.mark.asyncio
async def test_feeds_api_direct(
    backend_client: httpx.AsyncClient, test_bcfy_feed: tuple[str, str]
) -> None:
    feed_id, _ = test_bcfy_feed

    # Test Backend feeds-api directly
    backend_resp = await backend_client.get("/feeds", timeout=10.0)
    assert backend_resp.status_code == 200, (
        f"Backend failed: {backend_resp.text}"
    )
    backend_data = backend_resp.json()

    assert isinstance(backend_data, dict)
    assert "feeds" in backend_data
    assert isinstance(backend_data["feeds"], list)

    feed_ids = [feed["id"] for feed in backend_data["feeds"]]
    assert feed_id in feed_ids


@pytest.mark.asyncio
async def test_feeds_api_proxy(
    proxy_client: httpx.AsyncClient, test_bcfy_feed: tuple[str, str]
) -> None:
    feed_id, _ = test_bcfy_feed

    # Test Frontend Proxy feeds-api
    proxy_resp = await proxy_client.get("/feeds", timeout=10.0)
    assert proxy_resp.status_code == 200, f"Proxy failed: {proxy_resp.text}"
    proxy_data = proxy_resp.json()

    assert isinstance(proxy_data, dict)
    assert "feeds" in proxy_data
    assert isinstance(proxy_data["feeds"], list)

    feed_ids = [feed["id"] for feed in proxy_data["feeds"]]
    assert feed_id in feed_ids


@pytest.mark.asyncio
async def test_get_feed_proxy(
    proxy_client: httpx.AsyncClient, test_bcfy_feed: tuple[str, str]
) -> None:
    """Test GET /feeds/{feedId} via frontend proxy."""
    feed_id, feed_name = test_bcfy_feed

    resp = await proxy_client.get(f"/feeds/{feed_id}", timeout=10.0)
    assert resp.status_code == 200, f"Get feed failed: {resp.text}"
    data = resp.json()

    assert data["id"] == feed_id
    assert data["name"] == feed_name
    assert data["sourceType"] == "bcfy_feeds"
    assert "status" in data
    assert "sourceUrl" in data


@pytest.mark.asyncio
async def test_get_feed_not_found_proxy(
    proxy_client: httpx.AsyncClient,
) -> None:
    """Test GET /feeds/{non_existent_id} via frontend proxy returns 404."""
    non_existent_id = str(uuid.uuid4())
    resp = await proxy_client.get(f"/feeds/{non_existent_id}", timeout=10.0)
    assert resp.status_code == 404, f"Expected 404, got {resp.status_code}: {resp.text}"


@pytest.mark.asyncio
async def test_create_feed_proxy(
    proxy_client: httpx.AsyncClient,
) -> None:
    """Test POST /feeds via frontend proxy to create a new feed."""
    source_id = str(uuid.uuid4().fields[0])
    feed_name = f"Test Proxy Create Feed {uuid.uuid4()}"
    payload = {
        "name": feed_name,
        "sourceType": "bcfy_feeds",
        "sourceFeedId": source_id,
        "tags": [{"key": "env", "value": "test"}],
    }

    resp = await proxy_client.post("/feeds", json=payload, timeout=10.0)
    assert resp.status_code == 201, f"Create feed failed: {resp.text}"
    data = resp.json()

    created_id = data["id"]
    assert data["name"] == feed_name
    assert data["sourceType"] == "bcfy_feeds"
    assert data["sourceFeedId"] == source_id

    # Clean up by deactivating
    await proxy_client.post(f"/feeds/{created_id}/deactivate", timeout=10.0)


@pytest.mark.asyncio
async def test_update_feed_proxy(
    proxy_client: httpx.AsyncClient, test_bcfy_feed: tuple[str, str]
) -> None:
    """Test PUT /feeds/{feedId} via frontend proxy."""
    feed_id, _ = test_bcfy_feed
    updated_name = f"Updated Feed {uuid.uuid4()}"
    payload = {
        "name": updated_name,
        "tags": [{"key": "updated", "value": "true"}],
    }

    resp = await proxy_client.put(f"/feeds/{feed_id}", json=payload, timeout=10.0)
    assert resp.status_code == 200, f"Update feed failed: {resp.text}"
    data = resp.json()
    assert data["id"] == feed_id
    assert data["name"] == updated_name

    # Verify via GET
    get_resp = await proxy_client.get(f"/feeds/{feed_id}", timeout=10.0)
    assert get_resp.status_code == 200
    assert get_resp.json()["name"] == updated_name


@pytest.mark.asyncio
async def test_reset_feed_proxy(
    proxy_client: httpx.AsyncClient, test_bcfy_feed: tuple[str, str]
) -> None:
    """Test POST /feeds/{feedId}/reset via frontend proxy."""
    feed_id, _ = test_bcfy_feed

    resp = await proxy_client.post(f"/feeds/{feed_id}/reset", timeout=10.0)
    assert resp.status_code == 200, f"Reset feed failed: {resp.text}"
    data = resp.json()
    assert data["id"] == feed_id


@pytest.mark.asyncio
async def test_list_feed_history_proxy(
    backend_client: httpx.AsyncClient,
    proxy_client: httpx.AsyncClient,
    test_bcfy_feed: tuple[str, str],
) -> None:
    """Test GET /feeds/{feedId}/history via frontend proxy after performing an update."""
    feed_id, _ = test_bcfy_feed

    update_payload = {
        "name": f"Updated Name {uuid.uuid4()}",
        "tags": [],
    }
    update_resp = await proxy_client.put(f"/feeds/{feed_id}", json=update_payload, timeout=10.0)
    assert update_resp.status_code == 200

    feed_get = await backend_client.get(f"/feeds/{feed_id}", timeout=10.0)
    assert feed_get.status_code == 200, f"Backend get feed failed: {feed_get.text}"

    backend_history = await backend_client.get(f"/feeds/{feed_id}/history", timeout=10.0)
    assert backend_history.status_code == 200, f"Backend history failed: {backend_history.text}"

    resp = await proxy_client.get(
        f"/feeds/{feed_id}/history", params={"limit": 10}, timeout=10.0
    )
    assert resp.status_code == 200, f"List history failed: {resp.text}"
    data = resp.json()
    assert isinstance(data, dict)
    assert "historyEvents" in data
    assert isinstance(data["historyEvents"], list)


@pytest.mark.asyncio
async def test_deactivate_feed_proxy(
    proxy_client: httpx.AsyncClient,
) -> None:
    """Test POST /feeds/{feedId}/deactivate via frontend proxy."""
    source_id = str(uuid.uuid4().fields[0])
    feed_name = f"Test Deactivate Feed {uuid.uuid4()}"
    payload = {
        "name": feed_name,
        "sourceType": "bcfy_feeds",
        "sourceFeedId": source_id,
    }

    create_resp = await proxy_client.post("/feeds", json=payload, timeout=10.0)
    assert create_resp.status_code == 201
    feed_id = create_resp.json()["id"]

    deact_resp = await proxy_client.post(f"/feeds/{feed_id}/deactivate", timeout=10.0)
    assert deact_resp.status_code == 204, f"Deactivate failed: {deact_resp.text}"

    # Verify status changed to inactive (frontend representation of deactivated)
    get_resp = await proxy_client.get(f"/feeds/{feed_id}", timeout=10.0)
    assert get_resp.status_code == 200
    assert get_resp.json()["status"] in ["inactive", "DEACTIVATED"]


@pytest.mark.asyncio
async def test_delete_feed_proxy(
    proxy_client: httpx.AsyncClient,
) -> None:
    """Test DELETE /feeds/{feedId} via frontend proxy."""
    source_id = str(uuid.uuid4().fields[0])
    feed_name = f"Test Delete Feed {uuid.uuid4()}"
    payload = {
        "name": feed_name,
        "sourceType": "bcfy_feeds",
        "sourceFeedId": source_id,
    }

    create_resp = await proxy_client.post("/feeds", json=payload, timeout=10.0)
    assert create_resp.status_code == 201
    feed_id = create_resp.json()["id"]

    del_resp = await proxy_client.delete(f"/feeds/{feed_id}", timeout=10.0)
    assert del_resp.status_code == 204, f"Delete failed: {del_resp.text}"

    # Verify GET 404 after deletion
    get_resp = await proxy_client.get(f"/feeds/{feed_id}", timeout=10.0)
    assert get_resp.status_code == 404


@pytest.mark.asyncio
async def test_list_feeds_with_query_params_proxy(
    proxy_client: httpx.AsyncClient, test_bcfy_feed: tuple[str, str]
) -> None:
    """Test GET /feeds with query parameters via frontend proxy."""
    feed_id, _ = test_bcfy_feed

    resp = await proxy_client.get(
        "/feeds",
        params={
            "limit": 10,
            "order": "desc",
            "sourceTypes": "bcfy_feeds",
        },
        timeout=10.0,
    )
    assert resp.status_code == 200, f"List feeds query params failed: {resp.text}"
    data = resp.json()
    assert "feeds" in data
    feed_ids = [f["id"] for f in data["feeds"]]
    assert feed_id in feed_ids
