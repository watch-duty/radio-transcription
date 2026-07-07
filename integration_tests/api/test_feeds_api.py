"""Integration tests for the Feeds API and Frontend Feeds Proxy API."""

import base64
import json
import os
import uuid
from collections.abc import AsyncIterator

import httpx
import pytest
from google.cloud import storage

import httpx
import pytest

from integration_tests.feed_utils import create_test_bcfy_feed  # noqa: F401


def generate_dummy_jwt(payload: dict) -> str:
    header = {"alg": "HS256", "typ": "JWT"}

    def b64url(data: bytes) -> str:
        return base64.urlsafe_b64encode(data).decode("utf-8").rstrip("=")

    header_segment = b64url(
        json.dumps(header, separators=(",", ":")).encode("utf-8")
    )
    payload_segment = b64url(
        json.dumps(payload, separators=(",", ":")).encode("utf-8")
    )
    return f"{header_segment}.{payload_segment}.signature"


# Dummy JWT token for frontend-api authentication
DUMMY_JWT = generate_dummy_jwt(
    {"sub": "1234567890", "email": "test@example.com", "email_verified": True}
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

    # Assert format matches ListFeedsResponse
    assert isinstance(backend_data, dict)
    assert "feeds" in backend_data
    assert isinstance(backend_data["feeds"], list)

    # Assert that the created feed is returned in the list
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

    # The proxy returns ListFeedsResponse (dict)
    assert isinstance(proxy_data, dict)
    assert "feeds" in proxy_data
    assert isinstance(proxy_data["feeds"], list)

    # Assert that the created feed is returned in the list
    feed_ids = [feed["id"] for feed in proxy_data["feeds"]]
    assert feed_id in feed_ids


@pytest.fixture(name="gcs_client")
def create_gcs_client() -> storage.Client:
    return storage.Client()


@pytest.mark.asyncio
async def test_create_echo_feed_missing_directory(
    proxy_client: httpx.AsyncClient,
) -> None:
    """Test that creating an Echo feed fails if the GCS directory does not exist."""
    feed_id = str(uuid.uuid4())
    payload = {
        "name": f"Test Echo Feed {feed_id}",
        "sourceType": "echo",
        "sourceFeedId": f"missing-dir-{feed_id}",
    }

    resp = await proxy_client.post("/feeds", json=payload)
    assert resp.status_code == 400
    assert "No GCS directory found for Echo feed" in resp.json()["message"]


@pytest.mark.asyncio
async def test_create_echo_feed_success(
    proxy_client: httpx.AsyncClient,
    gcs_client: storage.Client,
) -> None:
    """Test that creating an Echo feed succeeds if the GCS directory exists."""
    bucket_name = os.environ.get("ECHO_RECORDINGS_BUCKET", "echo-recordings-test")
    bucket = gcs_client.bucket(bucket_name)

    feed_id = str(uuid.uuid4())
    source_feed_id = f"valid-echo-dir-{feed_id}"

    # Create a dummy blob to simulate the directory existence
    blob = bucket.blob(f"{source_feed_id}/.keep")
    blob.upload_from_string("")

    payload = {
        "name": f"Test Echo Feed {feed_id}",
        "sourceType": "echo",
        "sourceFeedId": source_feed_id,
    }

    resp = await proxy_client.post("/feeds", json=payload)
    assert resp.status_code == 201, resp.text
    
    data = resp.json()
    assert data["sourceType"] == "echo"
    assert data["sourceFeedId"] == source_feed_id
