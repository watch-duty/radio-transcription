import asyncio
import base64
import json
import logging
import os
from collections.abc import Callable
from pathlib import Path

import httpx

from integration_tests.utils import assert_eventually

logger = logging.getLogger(__name__)


def generate_dummy_jwt(payload: dict) -> str:
    """Generate a dummy JWT token for local/testing authentication."""
    header = {"alg": "HS256", "typ": "JWT"}

    def b64url(data: bytes) -> str:
        return base64.urlsafe_b64encode(data).decode("utf-8").rstrip("=")

    header_segment = b64url(json.dumps(header, separators=",:").encode("utf-8"))
    payload_segment = b64url(
        json.dumps(payload, separators=",:").encode("utf-8")
    )
    return f"{header_segment}.{payload_segment}.signature"


# Dummy JWT token for frontend-api authentication
DUMMY_JWT = generate_dummy_jwt(
    {"sub": "1234567890", "email": "test@example.com", "email_verified": True}
)


def get_audio_segments_api_url() -> str:
    """Returns the base API URL (e.g. http://host:port/v1) for the Audio Segments service."""
    url = os.environ.get("AUDIO_SEGMENTS_API_URL")
    if url:
        if url.endswith("/audio_segments"):
            url = url.removesuffix("/audio_segments")
        if not url.endswith("/v1"):
            url = f"{url.rstrip('/')}/v1"
        return url

    if Path("/.dockerenv").exists():
        host = "audio-segments-api:8091"
    else:
        host = "localhost:8091"
    return f"http://{host}/v1"


def verify_audio_segments_via_api(
    feed_id: str,
    matcher: Callable[[dict], bool],
    timeout_sec: float = 300.0,
) -> bool:
    """Polls /v1/audio_segments until a segment matching the condition is found."""
    return verify_multiple_audio_segments_via_api(
        feed_id=feed_id,
        matcher=matcher,
        min_count=1,
        timeout_sec=timeout_sec,
    )


def verify_multiple_audio_segments_via_api(
    feed_id: str,
    matcher: Callable[[dict], bool],
    min_count: int,
    timeout_sec: float = 300.0,
) -> bool:
    """Polls /v1/audio_segments until at least `min_count` segments matching the condition are found."""
    base_url = get_audio_segments_api_url()

    async def _check_api():
        async with httpx.AsyncClient(base_url=base_url) as client:
            res = await client.get(
                "/audio_segments", params={"feed_ids": [feed_id]}
            )
            if res.status_code != 200:
                return False
            data = res.json()
            matches = [
                segment
                for segment in data.get("segments", [])
                if matcher(segment)
            ]
            return len(matches) >= min_count

    def condition():
        try:
            return asyncio.run(_check_api())
        except Exception as e:
            logger.warning(f"API check failed: {e}")
            return False

    logger.info(
        f"Waiting for at least {min_count} matching audio segments via API..."
    )
    assert_eventually(
        condition,
        timeout_sec=timeout_sec,
        error_msg=f"Did not find {min_count} matching audio segments via API",
    )
    return True


def verify_notification_received(
    segment_id: str,
    timeout_sec: float = 70.0,
) -> bool:
    """Polls the mock server until a notification matching segment_id is found."""
    mock_host = os.environ.get("MOCK_SERVER_HOST", "localhost:8082")
    url = f"http://{mock_host}"

    async def _check():
        async with httpx.AsyncClient() as client:
            res = await client.get(url, timeout=5.0)
            if res.status_code == 200:
                data = res.json()
                return any(r.get("segmentId") == segment_id for r in data)
            return False

    def condition():
        try:
            return asyncio.run(_check())
        except Exception as e:
            logger.warning(f"Mock server check failed: {e}")
            return False

    logger.info(f"Waiting for notification matching segment {segment_id}...")
    assert_eventually(
        condition,
        timeout_sec=timeout_sec,
        error_msg=f"Did not find expected notification matching segment {segment_id}",
    )
    return True
