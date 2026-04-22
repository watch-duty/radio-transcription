from __future__ import annotations

import asyncio
import datetime
import logging
import os
import random
import uuid
from typing import TYPE_CHECKING

from curl_cffi.requests import AsyncSession

from backend.pipeline.ingestion.collectors.openmhz._ws_transport import (
    websocket_transport,
)
from backend.pipeline.ingestion.models import CapturedChunk
from backend.pipeline.ingestion.slo_contract import (
    EVENT_TYPE_CALL_DOWNLOAD_FAILED,
)

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from backend.pipeline.ingestion.collectors.openmhz._types import (
        TransportFactory,
    )
    from backend.pipeline.storage.feed_store import LeasedFeed

logger = logging.getLogger(__name__)

MAX_RECONNECT_FAILURES = 10
_DOWNLOAD_MAX_RETRIES = 3
_DOWNLOAD_BACKOFF_BASE_SEC = 1.0
_RECONNECT_BACKOFF_BASE_SEC = 1.0
_RECONNECT_BACKOFF_CAP_SEC = 30.0


def _get_transport(name: str) -> TransportFactory:
    """Resolve transport by name. Reads module attributes at call time."""
    if name == "websocket":
        return websocket_transport
    msg = f"Unknown OPENMHZ_TRANSPORT: {name!r}"
    raise ValueError(msg)


async def _sleep_or_shutdown(shutdown: asyncio.Event, seconds: float) -> bool:
    """Sleep for *seconds*, returning ``True`` if interrupted by shutdown."""
    try:
        await asyncio.wait_for(shutdown.wait(), timeout=seconds)
    except TimeoutError:
        return False
    else:
        return True


async def _download_m4a(
    session: AsyncSession,
    url: str,
    shutdown: asyncio.Event,
) -> bytes | None:
    """Download m4a from Wasabi S3 with retries.

    Returns audio bytes on success, ``None`` on failure.
    """
    for attempt in range(_DOWNLOAD_MAX_RETRIES):
        try:
            resp = await session.get(url, timeout=30.0)
            if resp.status_code == 200:
                return resp.content
            if 400 <= resp.status_code < 500:
                logger.warning(
                    "Download non-retryable %d: url=%s",
                    resp.status_code,
                    url,
                )
                return None
            logger.warning(
                "Download %d (attempt %d/%d): url=%s",
                resp.status_code,
                attempt + 1,
                _DOWNLOAD_MAX_RETRIES,
                url,
            )
        except Exception:
            logger.warning(
                "Download error (attempt %d/%d): url=%s",
                attempt + 1,
                _DOWNLOAD_MAX_RETRIES,
                url,
                exc_info=True,
            )
        if attempt < _DOWNLOAD_MAX_RETRIES - 1:
            if await _sleep_or_shutdown(
                shutdown, _DOWNLOAD_BACKOFF_BASE_SEC * (2**attempt)
            ):
                return None

    logger.warning("Download failed after retries: url=%s", url)
    return None


async def openmhz_collector(
    feed: LeasedFeed,
    shutdown_event: asyncio.Event,
    url_base: str,
) -> AsyncIterator[CapturedChunk]:
    """Capture OpenMHZ call recordings via WebSocket.

    Yields :class:`CapturedChunk` for each call received.

    Raises:
        ValueError: If ``source_feed_id`` is missing from the feed.
        RuntimeError: After ``MAX_RECONNECT_FAILURES`` consecutive
            transport failures.
    """
    source_feed_id = feed.get("source_feed_id")
    if not source_feed_id:
        msg = f"Feed {feed['id']} ({feed['name']}) missing source_feed_id"
        raise ValueError(msg)

    short_name = source_feed_id.strip()
    transport_name = os.getenv("OPENMHZ_TRANSPORT", "websocket")
    transport_factory = _get_transport(transport_name)

    consecutive_ws_failures = 0
    download_session = AsyncSession()

    try:
        while not shutdown_event.is_set():
            connection_session_id = str(uuid.uuid4())
            try:
                async with transport_factory(
                    short_name, url_base, shutdown_event
                ) as events:
                    async for call in events:
                        # SLO: receipt_time stamp — OpenMHZ WS event arrived
                        receipt_time = datetime.datetime.now(datetime.UTC)
                        consecutive_ws_failures = 0

                        if call.length_sec == 0:
                            continue

                        m4a_bytes = await _download_m4a(
                            download_session, call.url, shutdown_event
                        )
                        if m4a_bytes is None:
                            if not shutdown_event.is_set():
                                # SLO: call_download_failed emit — OpenMHZ _download_m4a returned None
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

                        logger.debug(
                            "Audio ready: short_name=%s call_id=%s "
                            "m4a_bytes=%d",
                            short_name,
                            call.id,
                            len(m4a_bytes),
                        )
                        yield CapturedChunk(
                            audio_bytes=m4a_bytes,
                            chunk_start_time=call.time,
                            chunk_end_time=call.time
                            + datetime.timedelta(seconds=call.length_sec),
                            session_id=connection_session_id,
                            receipt_time=receipt_time,
                        )
            except Exception:
                logger.warning(
                    "Transport error: short_name=%s",
                    short_name,
                    exc_info=True,
                )

            if shutdown_event.is_set():
                return

            consecutive_ws_failures += 1
            if consecutive_ws_failures >= MAX_RECONNECT_FAILURES:
                msg = (
                    f"WebSocket failed {consecutive_ws_failures} "
                    f"times consecutively for {short_name}"
                )
                logger.error(
                    "Escalating to runtime: short_name=%s "
                    "consecutive_failures=%d",
                    short_name,
                    consecutive_ws_failures,
                )
                raise RuntimeError(msg)

            backoff = min(
                _RECONNECT_BACKOFF_CAP_SEC,
                _RECONNECT_BACKOFF_BASE_SEC * (2**consecutive_ws_failures),
            ) + random.uniform(0, 1)  # noqa: S311 -- jitter, not crypto
            logger.info(
                "Reconnecting: short_name=%s attempt=%d backoff_sec=%.1f",
                short_name,
                consecutive_ws_failures,
                backoff,
            )
            if await _sleep_or_shutdown(shutdown_event, backoff):
                return
    finally:
        await download_session.close()
