from __future__ import annotations

import asyncio
import collections
import datetime
import logging
import os
import random
from typing import TYPE_CHECKING, Any

import aiohttp
from google.cloud import secretmanager

from backend.pipeline.common.audio import convert_to_flac
from backend.pipeline.ingestion.models import CapturedChunk

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from backend.pipeline.storage.feed_store import LeasedFeed

logger = logging.getLogger(__name__)

_MAX_5XX_RETRIES = 3
_POLL_INTERVAL_SEC = 10.0
_API_TIMEOUT_SEC = 10.0
_AUDIO_TIMEOUT_SEC = 60.0
_MP3_DOWNLOAD_MAX_RETRIES = 3
_MP3_DOWNLOAD_BACKOFF_BASE_SEC = 1.0


class AuthError(Exception):
    """Raised when Broadcastify Calls API returns 401 or 403."""


async def _sleep_or_shutdown(shutdown: asyncio.Event, seconds: float) -> bool:
    """Sleep for *seconds*, returning ``True`` if interrupted by shutdown."""
    try:
        await asyncio.wait_for(shutdown.wait(), timeout=seconds)
    except TimeoutError:
        return False
    return True


def _get_jwt_token() -> str:
    """Fetch Broadcastify JWT token synchronously from Secret Manager."""
    project_id = os.getenv("GOOGLE_CLOUD_PROJECT")
    secret_id = os.getenv("BROADCASTIFY_JWT_SECRET_ID")
    if not project_id or not secret_id:
        msg = "GOOGLE_CLOUD_PROJECT and BROADCASTIFY_JWT_SECRET_ID must be set"
        raise RuntimeError(msg)

    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
    try:
        response = client.access_secret_version(request={"name": name})
        return response.payload.data.decode("UTF-8").strip()
    except Exception as e:
        logger.exception("Failed to access secret %s: %s", name, e)
        msg = f"Failed to access secret {name}"
        raise RuntimeError(msg) from e


def _raise_for_fatal_status(
    status: int, feed_id: object, source_feed_id: str
) -> None:
    """Raise RuntimeError or AuthError for HTTP status codes that are not retryable."""
    if status == 429:
        msg = f"Rate limited by Broadcastify Calls API (feed {feed_id})"
        raise RuntimeError(msg)
    if status in (401, 403):
        msg = (
            f"Auth failure {status} from Broadcastify Calls API"
            f" (feed {feed_id}): credentials are invalid"
        )
        raise AuthError(msg)
    if status == 404:
        msg = (
            f"Feed not found (404) from Broadcastify Calls API"
            f" (feed {feed_id}, source_feed_id {source_feed_id}):"
            " feed configuration is wrong"
        )
        raise RuntimeError(msg)


async def _fetch_calls(
    session: aiohttp.ClientSession,
    url: str,
    headers: dict[str, str],
    params: dict[str, Any],
    feed_id: object,
    source_feed_id: str,
    shutdown_event: asyncio.Event,
) -> dict[str, Any] | None:
    """Fetch audio calls from Broadcastify, handling retries for 5XX errors."""
    for attempt in range(_MAX_5XX_RETRIES + 1):
        if shutdown_event.is_set():
            return None

        async with session.get(url, headers=headers, params=params) as resp:
            _raise_for_fatal_status(resp.status, feed_id, source_feed_id)

            if 500 <= resp.status <= 599:
                if attempt < _MAX_5XX_RETRIES:
                    delay = (2**attempt) + random.uniform(0, 1)  # noqa: S311
                    logger.warning(
                        "5XX %s (feed %s), retry %d/%d in %.1fs",
                        resp.status,
                        feed_id,
                        attempt + 1,
                        _MAX_5XX_RETRIES,
                        delay,
                    )
                    if await _sleep_or_shutdown(shutdown_event, delay):
                        return None
                    continue

                logger.error(
                    "5XX %s (feed %s) after %d retries, giving up on this call",
                    resp.status,
                    feed_id,
                    _MAX_5XX_RETRIES,
                )
                return None

            if resp.status != 200:
                logger.error(
                    "API call failed with status %s for feed %s "
                    "(source_feed_id=%s, url=%s, params=%s)",
                    resp.status,
                    feed_id,
                    source_feed_id,
                    url,
                    params,
                )
                return None

            return await resp.json()

    return None


def _raise_if_429(status: int, mp3_url: str) -> None:
    """Raise RuntimeError if status is 429."""
    if status == 429:
        msg = f"CDN rate limit (429) downloading MP3: {mp3_url}"
        raise RuntimeError(msg)


async def _download_and_convert_audio(
    session: aiohttp.ClientSession,
    mp3_url: str,
    shutdown_event: asyncio.Event,
) -> bytes | None:
    """Download MP3 audio and convert it to FLAC bytes on a separate thread."""
    timeout = aiohttp.ClientTimeout(total=_AUDIO_TIMEOUT_SEC)
    for attempt in range(_MP3_DOWNLOAD_MAX_RETRIES + 1):
        try:
            async with session.get(mp3_url, timeout=timeout) as audio_resp:
                _raise_if_429(audio_resp.status, mp3_url)

                if 500 <= audio_resp.status <= 599:
                    if attempt < _MP3_DOWNLOAD_MAX_RETRIES:
                        delay = _MP3_DOWNLOAD_BACKOFF_BASE_SEC * (2**attempt)
                        logger.warning(
                            "5XX %s downloading audio"
                            " (attempt %d/%d, retry in %.1fs): %s",
                            audio_resp.status,
                            attempt + 1,
                            _MP3_DOWNLOAD_MAX_RETRIES,
                            delay,
                            mp3_url,
                        )
                        if await _sleep_or_shutdown(shutdown_event, delay):
                            return None
                        continue

                    logger.error(
                        "5XX %s downloading audio after %d retries,"
                        " skipping: %s",
                        audio_resp.status,
                        _MP3_DOWNLOAD_MAX_RETRIES,
                        mp3_url,
                    )
                    return None

                if audio_resp.status != 200:
                    logger.error(
                        "Failed to download audio from %s (status %d)",
                        mp3_url,
                        audio_resp.status,
                    )
                    return None

                audio_bytes = await audio_resp.read()
                return await asyncio.to_thread(
                    convert_to_flac, audio_bytes, "mp3"
                )
        except RuntimeError:
            raise
        except Exception:
            logger.exception("Error downloading audio from %s", mp3_url)
            return None
    return None


async def capture_bcfy_calls(  # noqa: PLR0915, PLR0912
    feed: LeasedFeed, shutdown_event: asyncio.Event, url_base: str
) -> AsyncIterator[CapturedChunk]:
    """Capture audio chunks from Broadcastify Calls API.

    Args:
        feed: Leased feed containing source_feed_id.
        shutdown_event: Signals graceful shutdown request.
        url_base: Full Broadcastify Calls API live endpoint URL to query.
            The function uses this URL directly after normalizing a
            trailing slash.
    """
    source_feed_id = feed.get("source_feed_id")
    feed_id = feed.get("id")
    last_bookmark_time = feed.get("last_bookmark_time")
    last_bookmark_time_unix = (
        int(last_bookmark_time.timestamp()) if last_bookmark_time else None
    )
    if not source_feed_id:
        msg = f"Feed {feed_id} missing source_feed_id"
        raise ValueError(msg)

    # Fetch token in a thread to prevent blocking the event loop at startup
    jwt_token = await asyncio.to_thread(_get_jwt_token)

    normalized_url_base = url_base if url_base.endswith("/") else f"{url_base}/"
    headers = {"Authorization": f"Bearer {jwt_token}"}

    seen_urls = collections.deque(maxlen=1000)

    async with aiohttp.ClientSession(
        timeout=aiohttp.ClientTimeout(total=_API_TIMEOUT_SEC)
    ) as session:
        while not shutdown_event.is_set():
            params: dict[str, Any] = {"groups": source_feed_id}
            if last_bookmark_time_unix is not None:
                params["pos"] = last_bookmark_time_unix

            try:
                bcfy_calls = await _fetch_calls(
                    session,
                    normalized_url_base,
                    headers,
                    params,
                    feed_id,
                    source_feed_id,
                    shutdown_event,
                )

                calls = []
                if isinstance(bcfy_calls, dict):
                    response_calls = bcfy_calls.get("calls")
                    if isinstance(response_calls, list):
                        calls = response_calls

                if calls:
                    for result in calls:
                        if shutdown_event.is_set():
                            break

                        mp3_url = result.get("url")
                        if not mp3_url or mp3_url in seen_urls:
                            continue

                        try:
                            flac_bytes = await _download_and_convert_audio(
                                session, mp3_url, shutdown_event
                            )
                        except Exception as e:
                            logger.exception(
                                "Failed to process audio for %s: %s", mp3_url, e
                            )
                            continue
                        if not flac_bytes:
                            continue

                        start_ts = result.get("start_ts")
                        end_ts = result.get("end_ts")
                        now = datetime.datetime.now(datetime.UTC)

                        chunk_start_time = (
                            datetime.datetime.fromtimestamp(
                                start_ts, datetime.UTC
                            )
                            if start_ts is not None
                            else now
                        )
                        chunk_end_time = (
                            datetime.datetime.fromtimestamp(
                                end_ts, datetime.UTC
                            )
                            if end_ts is not None
                            else now
                        )

                        yield CapturedChunk(
                            audio_bytes=flac_bytes,
                            chunk_start_time=chunk_start_time,
                            chunk_end_time=chunk_end_time,
                        )

                        # Only mark as seen and update pagination after a successful
                        # yield, confirming the chunk was handed off to the pipeline.
                        seen_urls.append(mp3_url)
                    # Update local last_bookmark_time_unix for pagination after processing all calls in the response, ensuring we don't skip any calls if an error occurs mid-page.
                    if bcfy_calls and "lastPos" in bcfy_calls:
                        last_bookmark_time_unix = bcfy_calls["lastPos"]

                # Wait before polling again, gracefully interruptible by shutdown
                await _sleep_or_shutdown(shutdown_event, _POLL_INTERVAL_SEC)

            except AuthError:
                logger.warning(
                    "Auth failure (401/403) for feed %s, refreshing token.",
                    feed_id,
                )
                try:
                    jwt_token = await asyncio.to_thread(_get_jwt_token)
                    headers["Authorization"] = f"Bearer {jwt_token}"
                except Exception as e:
                    logger.exception("Failed to refresh JWT token: %s", e)
                await _sleep_or_shutdown(shutdown_event, _POLL_INTERVAL_SEC)
            except RuntimeError:
                raise
            except Exception as e:
                logger.exception("Error in capture_bcfy_calls loop: %s", e)
                await _sleep_or_shutdown(shutdown_event, _POLL_INTERVAL_SEC)
