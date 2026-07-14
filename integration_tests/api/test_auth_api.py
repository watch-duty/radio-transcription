"""Integration tests for Auth and Users Frontend Proxy API."""

import os
from collections.abc import AsyncIterator

import httpx
import pytest

from integration_tests.test_utils import DUMMY_JWT


@pytest.fixture(name="authenticated_proxy_client")
async def create_authenticated_proxy_client() -> AsyncIterator[httpx.AsyncClient]:
    async with httpx.AsyncClient(
        base_url=f"http://{os.environ.get('FRONTEND_API_HOST', 'localhost:8088')}/api/v1",
        headers={"Authorization": f"Bearer {DUMMY_JWT}"},
    ) as client:
        yield client


@pytest.fixture(name="unauthenticated_proxy_client")
async def create_unauthenticated_proxy_client() -> AsyncIterator[httpx.AsyncClient]:
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


@pytest.mark.asyncio
async def test_logout_proxy(
    authenticated_proxy_client: httpx.AsyncClient,
) -> None:
    """Test POST /auth/logout via proxy returns 204."""
    resp = await authenticated_proxy_client.post("/auth/logout", timeout=10.0)
    assert resp.status_code == 204


@pytest.mark.asyncio
async def test_refresh_session_no_cookie(
    unauthenticated_proxy_client: httpx.AsyncClient,
) -> None:
    """Test GET /auth/session without refresh_token cookie returns 401."""
    resp = await unauthenticated_proxy_client.get("/auth/session", timeout=10.0)
    assert resp.status_code == 401
    assert "No refresh token in session" in resp.json().get("message", "")


@pytest.mark.asyncio
async def test_google_login_invalid_code(
    unauthenticated_proxy_client: httpx.AsyncClient,
) -> None:
    """Test POST /auth/google with invalid OAuth code returns error (400 or 500)."""
    payload = {"code": "invalid_test_code_12345"}
    resp = await unauthenticated_proxy_client.post(
        "/auth/google", json=payload, timeout=10.0
    )
    assert resp.status_code in [400, 500]
