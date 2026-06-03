"""Integration tests for the Feeds API and Frontend Feeds Proxy API."""

import os
from collections.abc import AsyncIterator

import httpx
import pytest

from integration_tests.feed_utils import create_test_bcfy_feed  # noqa: F401

# Dummy JWT token for frontend-api authentication
# Basic paylod to pass auth req: {
# "sub": "1234567890",
# "email": "test@example.com",
# "email_verified": true
# }
DUMMY_JWT = (
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9."
    "eyJzdWIiOiIxMjM0NTY3ODkwIiwiZW1haWwiOiJ0ZXN0QGV4YW1wbGUuY29tIiwiZW1haWxfdmVyaWZpZWQiOnRydWV9."
    "signature"
)


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

    # Assert format matches the response_model in backend/services/feeds/main.py (list[Feed])
    assert isinstance(backend_data, list)

    # Assert that the created feed is returned in the list
    feed_ids = [feed["id"] for feed in backend_data]
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

    # The proxy returns Feed[] (list) because the backend returns a list
    assert isinstance(proxy_data, list)

    # Assert that the created feed is returned in the list
    feed_ids = [feed["id"] for feed in proxy_data]
    assert feed_id in feed_ids
