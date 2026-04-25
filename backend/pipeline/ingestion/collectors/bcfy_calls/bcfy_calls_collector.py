from __future__ import annotations

import asyncio
import collections
import datetime
import logging
import os
import random
import uuid
from pathlib import Path
from typing import TYPE_CHECKING, Any
from urllib.parse import urlparse

import aiohttp
from google.cloud import secretmanager

from backend.pipeline.ingestion.exceptions import SourceError
from backend.pipeline.ingestion.models import CapturedChunk
from backend.pipeline.ingestion.slo_contract import (
    EVENT_TYPE_CALL_AUTH_FAILURE,
    EVENT_TYPE_CALL_DOWNLOAD_FAILED,
)

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from backend.pipeline.storage.feed_store import LeasedFeed

logger = logging.getLogger(__name__)

_MAX_5XX_RETRIES = 3
_POLL_INTERVAL_SEC = 10.0
_API_TIMEOUT_SEC = 10.0
_AUDIO_TIMEOUT_SEC = 60.0
_AUDIO_FILE_DOWNLOAD_MAX_RETRIES = 3
_AUDIO_FILE_DOWNLOAD_BACKOFF_BASE_SEC = 1.0
_MAX_CONSECUTIVE_FAILURES = 10
_KNOWN_AUDIO_FORMATS = frozenset({"mp3", "m4a", "wav", "ogg", "aac", "flac"})


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


def _get_audio_format(url: str) -> str:
    """Infer the audio format from a URL's file extension.

    Args:
        url: The audio file URL (e.g., 'https://site.com/jake.mp3?v=1').

    Returns:
        The lowercase file extension without the leading dot,
        or 'mp3' if no valid extension is found.
    """
    # 1. Isolate the path from the URL (strips 'https://' and '?query=...')
    path = urlparse(url).path

    # 2. Extract the suffix (e.g., '.mp3'), drop the dot, and make lowercase
    ext = Path(path).suffix[1:].lower()

    # 3. Validate against known formats
    if ext in _KNOWN_AUDIO_FORMATS:
        return ext

    return "mp3"


def _raise_if_429(status: int, audio_url: str) -> None:
    """Raise RuntimeError if status is 429."""
    if status == 429:
        msg = f"CDN rate limit (429) downloading audio file: {audio_url}"
        raise RuntimeError(msg)


async def _download_audio(  # noqa: PLR0911
    session: aiohttp.ClientSession,
    audio_url: str,
    shutdown_event: asyncio.Event,
) -> bytes | None:
    """Download audio file."""
    timeout = aiohttp.ClientTimeout(total=_AUDIO_TIMEOUT_SEC)
    for attempt in range(_AUDIO_FILE_DOWNLOAD_MAX_RETRIES + 1):
        try:
            async with session.get(audio_url, timeout=timeout) as audio_resp:
                _raise_if_429(audio_resp.status, audio_url)

                if 500 <= audio_resp.status <= 599:
                    if attempt < _AUDIO_FILE_DOWNLOAD_MAX_RETRIES:
                        delay = _AUDIO_FILE_DOWNLOAD_BACKOFF_BASE_SEC * (
                            2**attempt
                        )
                        logger.warning(
                            "5XX %s downloading audio"
                            " (attempt %d/%d, retry in %.1fs): %s",
                            audio_resp.status,
                            attempt + 1,
                            _AUDIO_FILE_DOWNLOAD_MAX_RETRIES,
                            delay,
                            audio_url,
                        )
                        if await _sleep_or_shutdown(shutdown_event, delay):
                            return None
                        continue

                    logger.error(
                        "5XX %s downloading audio after %d retries,"
                        " skipping: %s",
                        audio_resp.status,
                        _AUDIO_FILE_DOWNLOAD_MAX_RETRIES,
                        audio_url,
                    )
                    return None

                if audio_resp.status != 200:
                    logger.error(
                        "Failed to download audio from %s (status %d)",
                        audio_url,
                        audio_resp.status,
                    )
                    return None

                return await audio_resp.read()
        except RuntimeError:
            raise
        except Exception as e:
            if attempt < _AUDIO_FILE_DOWNLOAD_MAX_RETRIES:
                delay = _AUDIO_FILE_DOWNLOAD_BACKOFF_BASE_SEC * (2**attempt)
                logger.warning(
                    "Network error downloading audio"
                    " (attempt %d/%d, retry in %.1fs): %s",
                    attempt + 1,
                    _AUDIO_FILE_DOWNLOAD_MAX_RETRIES,
                    delay,
                    audio_url,
                    exc_info=e,
                )
                if await _sleep_or_shutdown(shutdown_event, delay):
                    return None
                continue
            logger.exception(
                "Network error downloading audio after %d retries,"
                " skipping: %s",
                _AUDIO_FILE_DOWNLOAD_MAX_RETRIES,
                audio_url,
            )
            return None
    return None


def _extract_calls_from_response(
    bcfy_calls: dict[str, Any] | None,
) -> list[dict[str, Any]]:
    """Safely extract the calls list from the API response."""
    if not isinstance(bcfy_calls, dict):
        return []
    response_calls = bcfy_calls.get("calls")
    return response_calls if isinstance(response_calls, list) else []


async def _create_chunk_from_call(
    session: aiohttp.ClientSession,
    result: dict[str, Any],
    audio_url: str,
    shutdown_event: asyncio.Event,
    session_id: str,
    receipt_time: datetime.datetime,
) -> CapturedChunk | None:
    """Download audio for a single call and wrap it in a CapturedChunk."""
    try:
        audio_bytes = await _download_audio(session, audio_url, shutdown_event)
    except RuntimeError:
        raise
    except Exception as e:
        logger.exception("Failed to process audio for %s: %s", audio_url, e)
        return None

    if not audio_bytes:
        return None

    start_ts = result.get("start_ts")
    end_ts = result.get("end_ts")
    now = datetime.datetime.now(datetime.UTC)

    chunk_start_time = (
        datetime.datetime.fromtimestamp(start_ts, datetime.UTC)
        if start_ts is not None
        else now
    )
    chunk_end_time = (
        datetime.datetime.fromtimestamp(end_ts, datetime.UTC)
        if end_ts is not None
        else now
    )

    return CapturedChunk(
        audio_bytes=audio_bytes,
        chunk_start_time=chunk_start_time,
        chunk_end_time=chunk_end_time,
        session_id=session_id,
        receipt_time=receipt_time,
    )


async def _handle_loop_failure(
    feed_id: object,
    consecutive_failures: int,
    shutdown_event: asyncio.Event,
) -> int:
    """Increment failure count, raise if exceeded, and sleep."""
    consecutive_failures += 1
    if consecutive_failures >= _MAX_CONSECUTIVE_FAILURES:
        raise SourceError(reason="source_unreachable")
    await _sleep_or_shutdown(shutdown_event, _POLL_INTERVAL_SEC)
    return consecutive_failures


async def capture_bcfy_calls(  # noqa: PLR0912, PLR0915
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
    connection_session_id = str(uuid.uuid4())
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
    consecutive_failures = 0

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

                calls = _extract_calls_from_response(bcfy_calls)

                if calls:
                    for result in calls:
                        if shutdown_event.is_set():
                            break

                        # SLO: receipt_time stamp — bcfy_calls per-call iteration
                        receipt_time = datetime.datetime.now(datetime.UTC)

                        audio_url = result.get("url")
                        if not audio_url or audio_url in seen_urls:
                            continue

                        chunk = await _create_chunk_from_call(
                            session,
                            result,
                            audio_url,
                            shutdown_event,
                            connection_session_id,
                            receipt_time,
                        )
                        if not chunk:
                            if not shutdown_event.is_set():
                                # SLO: call_download_failed emit — bcfy_calls _create_chunk_from_call returned None
                                logger.warning(
                                    "Call download failed",
                                    extra={
                                        "json_fields": {
                                            "event_type": EVENT_TYPE_CALL_DOWNLOAD_FAILED,
                                            "feed_id": str(feed["id"]),
                                            "source_type": feed["source_type"],
                                        },
                                    },
                                )
                            continue

                        yield chunk

                        # Only mark as seen and update pagination after a successful
                        # yield, confirming the chunk was handed off to the pipeline.
                        seen_urls.append(audio_url)
                        # Reset consecutive failures on successful yield
                        consecutive_failures = 0
                # Update local last_bookmark_time_unix for pagination after processing all calls in the response, ensuring we don't skip any calls if an error occurs mid-page.
                if bcfy_calls and "lastPos" in bcfy_calls:
                    last_bookmark_time_unix = bcfy_calls["lastPos"]

                # Wait before polling again, gracefully interruptible by shutdown
                await _sleep_or_shutdown(shutdown_event, _POLL_INTERVAL_SEC)

            except AuthError:
                # Structured so it joins the other ingestion SLO/alert surfaces via
                # event_type; token is deliberately NOT logged (bearer tokens in logs
                # are a secrets-in-logs anti-pattern even when short-lived).
                logger.warning(
                    "Auth failure (401/403) for feed %s; refreshing token.",
                    feed_id,
                    extra={
                        "json_fields": {
                            "event_type": EVENT_TYPE_CALL_AUTH_FAILURE,
                            "feed_id": str(feed_id),
                            "source_type": feed["source_type"],
                        },
                    },
                )
                try:
                    jwt_token = await asyncio.to_thread(_get_jwt_token)
                    headers["Authorization"] = f"Bearer {jwt_token}"
                except Exception as e:
                    # Use warning, not exception — the SourceError handler in
                    # normalizer_runtime calls logger.exception on the chained
                    # SourceError, which already includes this exception's
                    # traceback via __cause__. Logging it here too duplicates
                    # the stack trace for every auth failure.
                    logger.warning("Failed to refresh JWT token: %s", e)
                    raise SourceError(reason="auth_failed") from e
                consecutive_failures = await _handle_loop_failure(
                    feed_id, consecutive_failures, shutdown_event
                )
            except RuntimeError:
                raise
            except Exception as e:
                logger.exception("Error in capture_bcfy_calls loop: %s", e)
                consecutive_failures = await _handle_loop_failure(
                    feed_id, consecutive_failures, shutdown_event
                )
