import asyncio
import logging
import os
import uuid

import httpx

from integration_tests.test_utils import (
    verify_transcript_in_db,
)

logger = logging.getLogger(__name__)


async def _setup_test_feed() -> str:
    """Creates a feed via Feeds API with a unique name. Returns feed ID."""
    feeds_api_url = f"{os.environ['FEEDS_API_URL']}/v1/feeds"

    unique_name = f"mock-icecast-feed-{uuid.uuid4()}"
    payload = {
        "name": unique_name,
        "source_type": "bcfy_feeds",
        "source_feed_id": "test-feed",
        "external_id": "ext-test-feed",
    }

    async with httpx.AsyncClient() as client:
        response = await client.post(feeds_api_url, json=payload)
    response.raise_for_status()
    data = response.json()
    feed_id = data["id"]
    logger.info(
        f"Test feed created via API successfully with ID: {feed_id} and name: {unique_name}"
    )
    return feed_id


def test_ingestion_e2e_flow() -> None:
    """Tests that audio ingestion service picks up seeded feed and results in a transcript."""
    # Setup the feed in DB via API
    feed_id = asyncio.run(_setup_test_feed())

    # Wait for transcript
    verify_transcript_in_db(feed_id)
