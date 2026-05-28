"""Integration tests for the Audio Segments API."""

import datetime
import os
import uuid
from collections.abc import AsyncIterator
from pathlib import Path

import asyncpg
import httpx
import pytest

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


@pytest.mark.asyncio
async def test_audio_segments_api_routes(
    api_client: httpx.AsyncClient, test_bcfy_feed: tuple[str, str]
) -> None:
    feed_id, _ = test_bcfy_feed
    segment_id = str(uuid.uuid4())

    # 1. Directly seed audio segments into the database using asyncpg
    _conn_kwargs = {
        "host": os.environ.get("ALLOYDB_HOST", "postgres"),
        "port": int(os.environ.get("ALLOYDB_PORT", "5432")),
        "user": os.environ.get("ALLOYDB_USER", "postgres"),
        "password": os.environ.get("ALLOYDB_PASSWORD", "postgres"),
        "database": os.environ.get("ALLOYDB_DB", "postgres"),
    }
    conn = await asyncpg.connect(**_conn_kwargs)
    try:
        is_missing_prior = False
        is_missing_post = False
        await conn.execute(
            """
            INSERT INTO audio_segments (
                id, feed_id, classification, start_timestamp, end_timestamp,
                missing_prior_context, missing_post_context, source_audio_uris,
                canonical_audio_uri, start_audio_offset, end_audio_offset, playback_audio_uri
            ) VALUES (
                $1::uuid, $2::uuid, 'SPEECH_DETECTED'::audio_classification, $3, $4,
                $5, $6, $7, $8, $9, $10, $11
            )
            """,
            segment_id,
            feed_id,
            datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC),
            datetime.datetime(2026, 1, 1, 0, 1, tzinfo=datetime.UTC),
            is_missing_prior,
            is_missing_post,
            ["gs://bucket/audio1.ogg"],
            "gs://bucket/canonical.ogg",
            datetime.timedelta(seconds=5),
            datetime.timedelta(seconds=10),
            None,
        )
    finally:
        await conn.close()

    # 2. List audio segments and verification (filtering by feed_id)
    response = await api_client.get(
        "/audio_segments", params={"feed_ids": [feed_id]}, timeout=10.0
    )
    assert response.status_code == 200, f"Failed to list: {response.text}"
    data = response.json()
    assert len(data) >= 1
    found = any(item["id"] == segment_id for item in data)
    assert found, f"Created segment {segment_id} not found in /audio_segments"
