"""Common testing utilities for integration tests."""

import asyncio
import logging
import os
import time
from collections.abc import Callable

import asyncpg
import httpx
import pytest
from pathlib import Path


def assert_eventually(
    condition_func: Callable[[], bool],
    timeout_sec: float = 5.0,
    error_msg: str = "Condition not met within timeout",
) -> None:
    """Repeatedly polls a condition function until it returns True."""
    end_time = time.time() + timeout_sec
    while time.time() < end_time:
        if condition_func():
            return
        time.sleep(0.5)
    pytest.fail(error_msg)


logger = logging.getLogger(__name__)


def _get_db_conn_kwargs():
    return {
        "host": os.environ["ALLOYDB_HOST"],
        "port": int(os.environ["ALLOYDB_PORT"]),
        "user": os.environ["ALLOYDB_USER"],
        "password": os.environ["ALLOYDB_PASSWORD"],
        "database": os.environ["ALLOYDB_DB"],
    }


def verify_transcript_in_db(feed_id: str, timeout_sec: float = 300.0) -> bool:
    """Polls the database until a transcript for the given feed_id appears."""

    async def _check_db():
        conn = await asyncpg.connect(**_get_db_conn_kwargs())
        row = await conn.fetchrow(
            "SELECT * FROM transcripts WHERE feed_id = $1::uuid", feed_id
        )
        await conn.close()
        return row is not None

    def condition():
        try:
            return asyncio.run(_check_db())
        except Exception as e:
            logger.warning(f"DB check failed: {e}")
            return False

    logger.info(f"Waiting for transcript in DB for feed {feed_id}...")

    assert_eventually(
        condition,
        timeout_sec=timeout_sec,
        error_msg="Transcript not found in DB",
    )
    return True


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
    base_url = get_audio_segments_api_url()

    async def _check_api():
        async with httpx.AsyncClient(base_url=base_url) as client:
            res = await client.get("/audio_segments", params={"feed_ids": [feed_id]})
            if res.status_code != 200:
                return False
            data = res.json()
            return any(matcher(segment) for segment in data.get("segments", []))

    def condition():
        try:
            return asyncio.run(_check_api())
        except Exception as e:
            logger.warning(f"API check failed: {e}")
            return False

    logger.info("Waiting for matching audio segment via API...")
    assert_eventually(
        condition,
        timeout_sec=timeout_sec,
        error_msg="No matching audio segment found via API",
    )
    return True
