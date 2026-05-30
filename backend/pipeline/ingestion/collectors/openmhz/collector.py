from __future__ import annotations

import asyncio
import dataclasses
import datetime
import logging
import os
import random
import uuid
from typing import TYPE_CHECKING

from curl_cffi.requests import AsyncSession

from backend.pipeline.ingestion.collectors.failure_classification import (
    ItemBatchOutcome,
    ItemFailure,
    collector_failure,
    missing_source_feed_id_failure,
)
from backend.pipeline.ingestion.collectors.openmhz._ws_transport import (
    websocket_transport,
)
from backend.pipeline.ingestion.models import (
    CapturedChunk,
    CaptureResources,
    CollectorFailure,
)
from backend.pipeline.ingestion.slo_contract import (
    EVENT_TYPE_CALL_DOWNLOAD_FAILED,
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


@dataclasses.dataclass(frozen=True)
class _DownloadResult:
    audio_bytes: bytes | None = None
    failure: ItemFailure | None = None


def _get_transport(name: str) -> TransportFactory:
    """Resolve transport by name. Reads module attributes at call time."""
    if name == "websocket":
        return websocket_transport
    raise collector_failure(
        FeedStatusReason.SYSTEM_CONFIGURATION_INVALID,
        "invalid_openmhz_transport",
    )


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
) -> _DownloadResult:
    """Download m4a from Wasabi S3 with retries.

    Returns classified download result for eligible call recordings.
    """
    for attempt in range(_DOWNLOAD_MAX_RETRIES):
        try:
            resp = await session.get(url, timeout=30.0)
            if resp.status_code == 200:
                return _DownloadResult(audio_bytes=resp.content)
            if resp.status_code in {401, 403}:
                logger.warning(
                    "Download auth failure %d: url=%s",
                    resp.status_code,
                    url,
                )
                return _DownloadResult(
                    failure=ItemFailure(
                        FeedStatusReason.SYSTEM_AUTHENTICATION_FAILED,
                        f"item_http_{resp.status_code}",
                    )
                )
            if resp.status_code == 429:
                logger.warning("Download rate limited 429: url=%s", url)
                return _DownloadResult(
                    failure=ItemFailure(
                        FeedStatusReason.SOURCE_RATE_LIMITED,
                        "item_http_429",
                    )
                )
            if 400 <= resp.status_code < 500:
                logger.warning(
                    "Download non-retryable %d: url=%s",
                    resp.status_code,
                    url,
                )
                return _DownloadResult(
                    failure=ItemFailure(
                        FeedStatusReason.SOURCE_UNREACHABLE,
                        "item_download_failed",
                    )
                )
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
                return _DownloadResult()

    logger.warning("Download failed after retries: url=%s", url)
    return _DownloadResult(
        failure=ItemFailure(
            FeedStatusReason.SOURCE_UNREACHABLE,
            "item_download_failed",
        )
    )


def _normalize_download_result(
    result: _DownloadResult | bytes | None,
) -> _DownloadResult:
    """Normalize legacy test doubles into the typed download result."""
    if isinstance(result, _DownloadResult):
        return result
    if isinstance(result, bytes):
        return _DownloadResult(audio_bytes=result)
    return _DownloadResult()


def _raise_item_failure(failure: ItemFailure) -> None:
    """Raise a typed collector failure from an item streak result."""
    raise collector_failure(failure.status_reason, failure.reason)


async def openmhz_collector(  # noqa: PLR0912, PLR0915
    feed: LeasedFeed,
    shutdown_event: asyncio.Event,
    url_base: str,
    _resources: CaptureResources,
) -> AsyncIterator[CapturedChunk]:
    """Capture OpenMHZ call recordings via WebSocket.

    Yields :class:`CapturedChunk` for each call received.

    Args:
        feed: Leased feed containing source_feed_id.
        shutdown_event: Signals graceful shutdown request.
        url_base: OpenMHZ API base URL.
        _resources: Runtime-owned CaptureResources. Accepted but unused
            (openmhz uses curl_cffi for HTTP, not the runtime aiohttp
            session).

    Raises:
        CollectorFailure: If source configuration is invalid or persistent
            OpenMHz source failures prevent capture.
    """
    source_feed_id = feed.get("source_feed_id")
    if not source_feed_id:
        logger.error(
            "Feed %s (%s) missing source_feed_id",
            feed["id"],
            feed["name"],
        )
        raise missing_source_feed_id_failure()

    short_name = source_feed_id.strip()
    transport_name = os.getenv("OPENMHZ_TRANSPORT", "websocket")
    transport_factory = _get_transport(transport_name)

    consecutive_ws_failures = 0
    item_outcome = ItemBatchOutcome()
    item_failure_count = 0
    download_session = AsyncSession()

    try:
        while not shutdown_event.is_set():
            connection_session_id = str(uuid.uuid4())
            try:
                pending_item_failure: ItemFailure | None = None
                async with transport_factory(
                    short_name, url_base, shutdown_event
                ) as events:
                    async for call in events:
                        # SLO: receipt_time stamp — OpenMHZ WS event arrived
                        receipt_time = datetime.datetime.now(datetime.UTC)
                        consecutive_ws_failures = 0

                        if call.length_sec == 0:
                            continue

                        download_result = _normalize_download_result(
                            await _download_m4a(
                                download_session, call.url, shutdown_event
                            )
                        )
                        if download_result.failure is not None:
                            item_outcome.record_attempt()
                            item_outcome.record_failure(download_result.failure)
                            item_failure_count += 1
                            if not shutdown_event.is_set():
                                # SLO: call_download_failed emit — OpenMHZ _download_m4a returned a classified failure
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
                            if item_failure_count >= MAX_ITEM_DOWNLOAD_FAILURES:
                                promoted = item_outcome.promoted_failure()
                                if promoted is not None:
                                    pending_item_failure = promoted
                                    break
                            continue

                        m4a_bytes = download_result.audio_bytes
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
                        item_outcome = ItemBatchOutcome()
                        item_failure_count = 0
                if pending_item_failure is not None:
                    _raise_item_failure(pending_item_failure)
            except CollectorFailure:
                raise
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
                logger.error(
                    "Escalating to runtime: short_name=%s "
                    "consecutive_failures=%d",
                    short_name,
                    consecutive_ws_failures,
                )
                raise collector_failure(
                    FeedStatusReason.SOURCE_UNREACHABLE,
                    "source_unreachable",
                )

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
