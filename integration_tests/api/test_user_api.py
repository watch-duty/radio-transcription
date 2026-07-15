"""Integration tests for Users Frontend Proxy API."""

import os
from collections.abc import AsyncIterator

import httpx
import pytest

from integration_tests.test_utils import DUMMY_JWT


@pytest.fixture(name="authenticated_proxy_client")
async def create_authenticated_proxy_client() -> AsyncIterator[
    httpx.AsyncClient
]:
    async with httpx.AsyncClient(
        base_url=f"http://{os.environ.get('FRONTEND_API_HOST', 'localhost:8088')}/api/v1",
        headers={"Authorization": f"Bearer {DUMMY_JWT}"},
    ) as client:
        yield client


@pytest.fixture(name="unauthenticated_proxy_client")
async def create_unauthenticated_proxy_client() -> AsyncIterator[
    httpx.AsyncClient
]:
    async with httpx.AsyncClient(
        base_url=f"http://{os.environ.get('FRONTEND_API_HOST', 'localhost:8088')}/api/v1",
    ) as client:
        yield client


@pytest.mark.asyncio
async def test_get_user_info_authenticated(
    authenticated_proxy_client: httpx.AsyncClient,
) -> None:
    """Test GET /users with valid JWT token."""
    resp = await authenticated_proxy_client.get("/users", timeout=10.0)
    assert resp.status_code == 200, f"Get user info failed: {resp.text}"
    data = resp.json()
    assert data["email"] == "test@example.com"
    assert "isAdmin" in data
    assert isinstance(data["isAdmin"], bool)


@pytest.mark.asyncio
async def test_get_user_info_unauthenticated(
    unauthenticated_proxy_client: httpx.AsyncClient,
) -> None:
    """Test GET /users without authentication token returns error (401 or 500)."""
    resp = await unauthenticated_proxy_client.get("/users", timeout=10.0)
    assert resp.status_code in [401, 500]
