from __future__ import annotations

import asyncio
import datetime
import logging
import os
import random
import re
import time
import uuid
from typing import TYPE_CHECKING, Any
from urllib.parse import urlparse

from curl_cffi.requests import AsyncSession

from backend.pipeline.ingestion.collectors import (
    control_flow,
    item_downloads,
    telemetry,
)
from backend.pipeline.ingestion.collectors.failure_classification import (
    ItemBatchOutcome,
    ItemFailure,
    collector_failure,
    missing_source_feed_id_failure,
)
from backend.pipeline.ingestion.collectors.openmhz._ws_transport import (
    OpenMHzTransportError,
    websocket_transport,
)
from backend.pipeline.ingestion.failure_classifiers import (
    http_status,
)
from backend.pipeline.ingestion.models import (
    CapturedChunk,
    CaptureEvent,
    CaptureResources,
    FeedFailure,
    SourceObservation,
)
from backend.pipeline.storage.feed_store import FeedStatusReason

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from backend.pipeline.ingestion.collectors.openmhz._types import (
        TransportFactory,
    )
    from backend.pipeline.storage.feed_store import LeasedFeed

logger = logging.getLogger(__name__)

MAX_RECONNECT_FAILURES = 10
MAX_ITEM_DOWNLOAD_FAILURES = 10
_DOWNLOAD_MAX_RETRIES = 3
_DOWNLOAD_BACKOFF_BASE_SEC = 1.0
_RECONNECT_BACKOFF_BASE_SEC = 1.0
_RECONNECT_BACKOFF_CAP_SEC = 30.0
_QUIET_CONNECTION_HEALTHY_MIN_SEC = 60.0
_WS_UPGRADE_STATUS_RE = re.compile(
    r"Refused WebSockets upgrade:\s*(\d{3})",
    re.IGNORECASE,
)
_OPENMHZ_MEDIA_HOSTS = frozenset({"media.openmhz.com", "media2.openmhz.com"})
_INVALID_OPENMHZ_MEDIA_URL_REASON = "invalid_openmhz_media_url"


def _get_transport(name: str) -> TransportFactory:
    """Resolve transport by name. Reads module attributes at call time."""
    if name == "websocket":
        return websocket_transport
    raise collector_failure(
        FeedStatusReason.SYSTEM_CONFIGURATION_INVALID,
        "invalid_openmhz_transport",
    )


def _transport_failure_from_exception(exc: Exception) -> FeedFailure | None:
    """Classify transport exceptions that carry terminal HTTP evidence."""
    exception_text = _exception_chain_text(exc)
    match = _WS_UPGRADE_STATUS_RE.search(exception_text)
    if match is None:
        return None

    status = int(match.group(1))
    status_reason = http_status.classify_http_status(
        status,
    )
    if status_reason is None:
        return None

    return collector_failure(
        status_reason,
        f"OpenMHz WebSocket upgrade failed with HTTP {status}; "
        f"{exception_text}",
    )


def _exception_chain_text(exc: BaseException) -> str:
    """Return exception text plus causes for bounded quarantine diagnostics."""
    parts: list[str] = []
    seen: set[int] = set()
    current: BaseException | None = exc
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        parts.append(f"{type(current).__name__}: {current}")
        current = current.__cause__ or current.__context__
    return "; ".join(parts)


def _is_openmhz_media_url(url: str) -> bool:
    """Return true only for expected OpenMHz-hosted media URLs."""
    try:
        parsed = urlparse(url)
        host = (parsed.hostname or "").lower()
        port = parsed.port
    except ValueError:
        return False
    return (
        parsed.scheme == "https"
        and host in _OPENMHZ_MEDIA_HOSTS
        and port in (None, 443)
    )


async def _get_m4a_or_cancel(
    session: AsyncSession,
    url: str,
    shutdown: asyncio.Event,
) -> Any:
    """Run one media GET, interrupting promptly when shutdown is signaled."""
    if shutdown.is_set():
        raise asyncio.CancelledError

    return await control_flow.await_or_cancel(
        session.get(url, timeout=30.0, allow_redirects=False),
        shutdown,
    )


async def _download_m4a(
    session: AsyncSession,
    url: str,
    shutdown: asyncio.Event,
) -> bytes | ItemFailure:
    """Download m4a from Wasabi S3 with retries.

    Returns audio bytes on success or a classified item failure on terminal
    failure. Shutdown interruption propagates as ``asyncio.CancelledError``.
    """
    if not _is_openmhz_media_url(url):
        logger.warning("Download invalid OpenMHz media URL")
        return ItemFailure(
            FeedStatusReason.SYSTEM_SOURCE_PAYLOAD_INVALID,
            _INVALID_OPENMHZ_MEDIA_URL_REASON,
        )

    last_status: int | None = None
    last_exception: Exception | None = None
    for attempt in range(_DOWNLOAD_MAX_RETRIES):
        try:
            resp = await _get_m4a_or_cancel(session, url, shutdown)
            last_status = resp.status_code
            last_exception = None
            if resp.status_code == 200:
                return resp.content
            if http_status.is_retryable_http_status(resp.status_code):
                logger.warning(
                    "Download retryable item HTTP status=%d attempt=%d/%d",
                    resp.status_code,
                    attempt + 1,
                    _DOWNLOAD_MAX_RETRIES,
                )
            else:
                logger.warning(
                    "Download non-retryable item HTTP status=%d",
                    resp.status_code,
                )
                return item_downloads.item_http_failure(resp.status_code)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            last_exception = exc
            last_status = None
            logger.warning(
                "Download error attempt=%d/%d",
                attempt + 1,
                _DOWNLOAD_MAX_RETRIES,
                exc_info=True,
            )
        if attempt < _DOWNLOAD_MAX_RETRIES - 1:
            await control_flow.sleep_or_cancel(
                shutdown, _DOWNLOAD_BACKOFF_BASE_SEC * (2**attempt)
            )

    logger.warning("Download failed after retries")
    if last_status is not None:
        return item_downloads.item_http_failure(last_status)
    return item_downloads.item_download_failed(last_exception)


async def openmhz_collector(  # noqa: PLR0912, PLR0915
    feed: LeasedFeed,
    shutdown_event: asyncio.Event,
    url_base: str,
    _resources: CaptureResources,
) -> AsyncIterator[CaptureEvent]:
    """Capture OpenMHZ call recordings via WebSocket.

    Yields :class:`CapturedChunk` for each call received. A dirty leased feed
    also yields :class:`SourceObservation` after a successful connection.

    Args:
        feed: Leased feed containing source_feed_id.
        shutdown_event: Signals graceful shutdown request.
        url_base: OpenMHZ API base URL.
        _resources: Runtime-owned CaptureResources. Accepted but unused
            (openmhz uses curl_cffi for HTTP, not the runtime aiohttp
            session).

    Raises:
        FeedFailure: If source configuration is invalid or persistent OpenMHz
            source failures prevent capture.
    """
    source_feed_id = feed.get("source_feed_id")
    short_name = source_feed_id.strip() if source_feed_id else ""
    if not short_name:
        logger.error(
            "Feed %s (%s) missing source_feed_id",
            feed["id"],
            feed["name"],
        )
        raise missing_source_feed_id_failure()

    transport_name = os.getenv("OPENMHZ_TRANSPORT", "websocket")
    transport_factory = _get_transport(transport_name)

    consecutive_ws_failures = 0
    # OpenMHz streams item events continuously, so there is no natural API page
    # or poll batch. Use a bounded consecutive item-failure window and reset it
    # only after a successful chunk yield.
    item_outcome = ItemBatchOutcome()
    item_failure_count = 0
    last_transport_failure: FeedFailure | None = None
    last_transport_exception: Exception | None = None
    download_session = AsyncSession()

    try:
        while not shutdown_event.is_set():
            connection_session_id = str(uuid.uuid4())
            connection_produced_chunk = False
            connection_started_at: float | None = None
            try:
                pending_item_failure: ItemFailure | None = None
                async with transport_factory(
                    short_name, url_base, shutdown_event
                ) as events:
                    connection_started_at = time.monotonic()
                    if (
                        feed["failure_count"] > 0
                        or feed["status_reason"] is not None
                    ):
                        yield SourceObservation()
                    async for call in events:
                        # SLO: receipt_time stamp — OpenMHZ WS event arrived
                        receipt_time = datetime.datetime.now(datetime.UTC)

                        if call.length_sec == 0:
                            continue

                        download_result = await _download_m4a(
                            download_session, call.url, shutdown_event
                        )
                        if shutdown_event.is_set():
                            return
                        if isinstance(download_result, ItemFailure):
                            item_outcome.record_attempt()
                            item_outcome.record_failure(download_result)
                            item_failure_count += 1
                            if not shutdown_event.is_set():
                                telemetry.emit_call_download_failed(
                                    logger,
                                    feed_id=feed["id"],
                                    source_type=feed["source_type"],
                                )
                            if item_failure_count >= MAX_ITEM_DOWNLOAD_FAILURES:
                                promoted = item_outcome.promoted_failure()
                                if promoted is not None:
                                    pending_item_failure = promoted
                                    break
                            continue
                        m4a_bytes = download_result

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
                            external_audio_segment_id=call.url,
                        )
                        connection_produced_chunk = True
                        consecutive_ws_failures = 0
                        item_outcome = ItemBatchOutcome()
                        item_failure_count = 0
                if pending_item_failure is not None:
                    raise collector_failure(
                        pending_item_failure.status_reason,
                        pending_item_failure.reason,
                    )
            except FeedFailure:
                raise
            except asyncio.CancelledError:
                raise
            except OpenMHzTransportError as exc:
                last_transport_exception = exc
                classified = _transport_failure_from_exception(exc)
                if classified is not None:
                    last_transport_failure = classified
                logger.warning(
                    "Transport error: short_name=%s",
                    short_name,
                    exc_info=True,
                )

            if shutdown_event.is_set():
                return

            healthy_quiet_connection = (
                connection_started_at is not None
                and time.monotonic() - connection_started_at
                >= _QUIET_CONNECTION_HEALTHY_MIN_SEC
            )
            if healthy_quiet_connection:
                consecutive_ws_failures = 0
                last_transport_failure = None
                last_transport_exception = None
            elif not connection_produced_chunk:
                consecutive_ws_failures += 1
            if consecutive_ws_failures >= MAX_RECONNECT_FAILURES:
                logger.error(
                    "Escalating to runtime: short_name=%s "
                    "consecutive_failures=%d",
                    short_name,
                    consecutive_ws_failures,
                )
                if last_transport_failure is not None:
                    raise last_transport_failure
                exception_context = ""
                if last_transport_exception is not None:
                    exception_context = "; " + _exception_chain_text(
                        last_transport_exception
                    )
                raise collector_failure(
                    FeedStatusReason.SOURCE_UNREACHABLE,
                    "OpenMHz transport reconnect exhausted "
                    f"after {MAX_RECONNECT_FAILURES} consecutive failures"
                    f"{exception_context}",
                )

            reconnect_attempt = max(consecutive_ws_failures, 1)
            backoff = min(
                _RECONNECT_BACKOFF_CAP_SEC,
                _RECONNECT_BACKOFF_BASE_SEC * (2**reconnect_attempt),
            ) + random.uniform(0, 1)  # noqa: S311 -- jitter, not crypto
            logger.info(
                "Reconnecting: short_name=%s attempt=%d backoff_sec=%.1f",
                short_name,
                reconnect_attempt,
                backoff,
            )
            await control_flow.sleep_or_cancel(shutdown_event, backoff)
    finally:
        await download_session.close()
