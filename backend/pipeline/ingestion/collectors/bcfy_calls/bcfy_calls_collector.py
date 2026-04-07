from __future__ import annotations

import asyncio
import collections
import datetime
import logging
import os
from typing import TYPE_CHECKING
from urllib.parse import urljoin

import aiohttp
from google.cloud import secretmanager

from backend.pipeline.common.audio import convert_to_flac
from backend.pipeline.ingestion.models import CapturedChunk

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from backend.pipeline.storage.feed_store import LeasedFeed

logger = logging.getLogger(__name__)


async def capture_bcfy_calls(  # noqa: PLR0915
    feed: LeasedFeed, shutdown_event: asyncio.Event, url_base: str
) -> AsyncIterator[CapturedChunk]:
    """Capture audio chunks from Broadcastify Calls API.

    Args:
        feed: Leased feed containing source_feed_id.
        shutdown_event: Signals graceful shutdown request.
        url_base: The base URL to prepend (not used directly if URLs are full).
    """
    source_feed_id = feed.get("source_feed_id")
    feed_id = feed.get("id")
    if not source_feed_id:
        msg = f"Feed {feed_id} missing source_feed_id"
        raise ValueError(msg)

    project_id = os.getenv("GOOGLE_CLOUD_PROJECT")
    secret_id = os.getenv("BROADCASTIFY_JWT_SECRET_ID")
    if not project_id or not secret_id:
        msg = "GOOGLE_CLOUD_PROJECT and BROADCASTIFY_JWT_SECRET_ID must be set"
        raise RuntimeError(msg)

    # Read JWT from Secret Manager
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
    try:
        response = client.access_secret_version(request={"name": name})
        jwt_token = response.payload.data.decode("UTF-8")
    except Exception as e:
        logger.exception("Failed to access secret %s: %s", name, e)
        secret_access_error = f"Failed to access secret {name}"
        raise RuntimeError(secret_access_error)

    normalized_url_base = url_base if url_base.endswith("/") else f"{url_base}/"
    api_url = urljoin(normalized_url_base, source_feed_id.strip())
    headers = {"Authorization": f"Bearer {jwt_token.strip()}"}

    seen_urls = collections.deque(maxlen=1000)

    async with aiohttp.ClientSession() as session:
        while not shutdown_event.is_set():
            try:
                async with session.get(api_url, headers=headers) as resp:
                    if resp.status != 200:
                        logger.exception(
                            "API call failed with status %s", resp.status
                        )
                        await asyncio.sleep(5)
                        continue

                    data = await resp.json()

                    if not isinstance(data, list):
                        data = [data]

                    for result in data:
                        mp3_url = result.get("url")
                        if not mp3_url or mp3_url in seen_urls:
                            continue
                        seen_urls.append(mp3_url)

                        # Download MP3
                        try:
                            async with session.get(mp3_url) as audio_resp:
                                if audio_resp.status != 200:
                                    logger.error(
                                        "Failed to download audio from %s",
                                        mp3_url,
                                    )
                                    continue
                                audio_bytes = await audio_resp.read()
                        except Exception as e:
                            logger.exception(
                                "Error downloading audio from %s: %s",
                                mp3_url,
                                e,
                            )
                            continue

                        # Convert to FLAC
                        try:
                            flac_bytes = convert_to_flac(audio_bytes, "mp3")
                        except Exception as e:
                            logger.exception(
                                "Failed to convert audio to FLAC: %s", e
                            )
                            continue

                        start_ts = result.get("start_ts")
                        end_ts = result.get("end_ts")

                        chunk_start_time = (
                            datetime.datetime.fromtimestamp(
                                start_ts, datetime.UTC
                            )
                            if start_ts
                            else datetime.datetime.now(datetime.UTC)
                        )
                        chunk_end_time = (
                            datetime.datetime.fromtimestamp(
                                end_ts, datetime.UTC
                            )
                            if end_ts
                            else datetime.datetime.now(datetime.UTC)
                        )

                        yield CapturedChunk(
                            audio_bytes=flac_bytes,
                            chunk_start_time=chunk_start_time,
                            chunk_end_time=chunk_end_time,
                        )

                await asyncio.sleep(5)

            except Exception as e:
                logger.exception("Error in capture_bcfy_calls loop: %s", e)
                await asyncio.sleep(5)
