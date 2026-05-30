from __future__ import annotations

import asyncio
import base64
import collections
import dataclasses
import datetime
import logging
import random
import uuid
from typing import TYPE_CHECKING, Any
from zoneinfo import ZoneInfo

from curl_cffi.requests import AsyncSession

from backend.pipeline.common.audio import get_audio_duration
from backend.pipeline.ingestion.collectors.failure_classification import (
    ItemBatchOutcome,
    ItemFailure,
    collector_failure,
    missing_source_feed_id_failure,
)
from backend.pipeline.ingestion.models import (
    AudioMimeType,
    CapturedChunk,
    CollectorFailure,
)
from backend.pipeline.ingestion.settings import _require_env
from backend.pipeline.ingestion.slo_contract import (
    EVENT_TYPE_CALL_DOWNLOAD_FAILED,
)
from backend.pipeline.storage.feed_store import FeedStatusReason

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from backend.pipeline.ingestion.models import CaptureResources
    from backend.pipeline.storage.feed_store import LeasedFeed

logger = logging.getLogger(__name__)

_DOWNLOAD_MAX_RETRIES = 3
_DOWNLOAD_BACKOFF_BASE_SEC = 1.0
_POLL_INTERVAL_SEC = 30.0
_MAX_CONSECUTIVE_FAILURES = 10


def _build_auth_headers() -> dict[str, str]:
    """Build HTTP Basic Authorization headers from env vars, raising if missing."""
    user = _require_env("FIRE_NOTIFICATIONS_USER")
    password = _require_env("FIRE_NOTIFICATIONS_PASSWORD")
    credentials = f"{user}:{password}"
    encoded = base64.b64encode(credentials.encode()).decode()
    return {"Authorization": f"Basic {encoded}"}


@dataclasses.dataclass(frozen=True)
class _DownloadResult:
    audio_bytes: bytes | None = None
    failure: ItemFailure | None = None


def _poll_status_failure(status: int) -> ItemFailure:
    """Classify Fire Notifications poll endpoint failures."""
    reason = f"fn_api_http_{status}"
    if status in {401, 403}:
        return ItemFailure(
            FeedStatusReason.SYSTEM_AUTHENTICATION_FAILED,
            reason,
        )
    if status == 429:
        return ItemFailure(FeedStatusReason.SOURCE_RATE_LIMITED, reason)
    if 400 <= status < 500:
        return ItemFailure(
            FeedStatusReason.SYSTEM_CONFIGURATION_INVALID,
            reason,
        )
    return ItemFailure(FeedStatusReason.SOURCE_UNREACHABLE, reason)


async def _sleep_or_shutdown(shutdown: asyncio.Event, seconds: float) -> bool:
    """Sleep for *seconds*, returning ``True`` if interrupted by shutdown."""
    try:
        await asyncio.wait_for(shutdown.wait(), timeout=seconds)
    except TimeoutError:
        return False
    else:
        return True


async def _download_audio(
    session: AsyncSession,
    url: str,
    shutdown: asyncio.Event,
) -> _DownloadResult:
    """Download audio file from S3 with retries."""
    # Note: We use a manual retry loop instead of 'tenacity' to easily
    # interrupt the backoff sleep when the shutdown event is set,
    # matching the pattern in bcfy_calls_collector.py.
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
    """Raise a typed collector failure from a file-list aggregation result."""
    raise collector_failure(failure.status_reason, failure.reason)


def _get_channel_timezone(channel_key: str) -> ZoneInfo:
    """Stub for resolving a channel's timezone.

    In a real implementation, this should look up the timezone based on the channel
    or fetch it from feed properties/tags. Defaulting to UTC for now.
    """
    return ZoneInfo("UTC")


def _parse_filename_timestamp(
    filename: str, channel_key: str
) -> datetime.datetime:
    """Extract and localize timestamp from FN filename.

    Expected format: CHANNELNAME YYYY-MM-DD HH-MM-SS.mp3
    """
    # Remove .mp3
    base = filename.removesuffix(".mp3")
    # Split from right to get date and time
    parts = base.split(" ")
    if len(parts) < 3:
        msg = f"Unexpected filename format: {filename}"
        raise ValueError(msg)

    date_str = parts[-2]
    time_str = parts[-1]

    # Parse naive datetime
    dt_naive = datetime.datetime.strptime(
        f"{date_str} {time_str}", "%Y-%m-%d %H-%M-%S"
    )

    # Localize based on channel timezone and convert to UTC
    tz = _get_channel_timezone(channel_key)
    dt_aware = dt_naive.replace(tzinfo=tz)
    return dt_aware.astimezone(datetime.UTC)


async def _process_file_list(
    files: list[dict[str, Any]],
    session: AsyncSession,
    shutdown_event: asyncio.Event,
    connection_session_id: str,
    feed: LeasedFeed,
    processed_uuids: collections.deque[str],
    source_feed_id: str,
    s3_base_url: str,
) -> AsyncIterator[CapturedChunk]:
    """Filter, sort and process audio files, yielding CapturedChunks."""
    # Filter for files and sort by name to process chronologically
    audio_files = [
        f
        for f in files
        if f.get("type") == "file" and f.get("name", "").endswith(".mp3")
    ]
    audio_files.sort(key=lambda x: x.get("name", ""))

    outcome = ItemBatchOutcome()

    for f in audio_files:
        if shutdown_event.is_set():
            break

        file_uuid = f.get("uuid")
        if not file_uuid or file_uuid in processed_uuids:
            continue

        filename = f.get("name")
        if not filename:
            continue
        try:
            start_time = _parse_filename_timestamp(filename, source_feed_id)
        except ValueError:
            logger.warning(
                "Failed to parse timestamp from filename: %s", filename
            )
            continue

        last_bookmark_time = feed.get("last_bookmark_time")
        if last_bookmark_time is not None and start_time <= last_bookmark_time:
            continue

        # Download the actual audio
        s3_url = f"{s3_base_url.rstrip('/')}/{file_uuid}.mp3"
        receipt_time = datetime.datetime.now(datetime.UTC)

        outcome.record_attempt()
        download_result = _normalize_download_result(
            await _download_audio(session, s3_url, shutdown_event)
        )
        if download_result.failure is not None:
            outcome.record_failure(download_result.failure)
            if not shutdown_event.is_set():
                logger.warning(
                    "FN Audio download failed",
                    extra={
                        "json_fields": {
                            "event_type": EVENT_TYPE_CALL_DOWNLOAD_FAILED,
                            "feed_id": str(feed["id"]),
                            "source_type": feed["source_type"],
                        },
                    },
                )
            continue

        mp3_bytes = download_result.audio_bytes
        if mp3_bytes is None:
            if not shutdown_event.is_set():
                logger.warning(
                    "FN Audio download failed",
                    extra={
                        "json_fields": {
                            "event_type": EVENT_TYPE_CALL_DOWNLOAD_FAILED,
                            "feed_id": str(feed["id"]),
                            "source_type": feed["source_type"],
                        },
                    },
                )
            continue

        try:
            # to_thread: get_audio_duration shells out to ffprobe — keep it off the event loop.
            duration_ms = await asyncio.to_thread(get_audio_duration, mp3_bytes)
        except Exception:
            logger.warning(
                "Failed to compute duration for uuid=%s",
                file_uuid,
                exc_info=True,
            )
            outcome.record_failure(
                ItemFailure(
                    FeedStatusReason.SYSTEM_COLLECTOR_ERROR,
                    "duration_probe_failed",
                )
            )
            continue

        end_time = start_time + datetime.timedelta(milliseconds=duration_ms)

        logger.debug(
            "FN Audio ready: source_feed_id=%s uuid=%s size=%d duration_ms=%d",
            source_feed_id,
            file_uuid,
            len(mp3_bytes),
            duration_ms,
        )
        yield CapturedChunk(
            audio_bytes=mp3_bytes,
            chunk_start_time=start_time,
            chunk_end_time=end_time,
            session_id=connection_session_id,
            receipt_time=receipt_time,
            mime_type=AudioMimeType.MPEG,
            resume_position=end_time,
        )
        # Only mark as processed after a successful yield, confirming
        # the chunk was handed off to the pipeline.
        processed_uuids.append(file_uuid)
        outcome.record_chunk_produced()

    promoted = outcome.promoted_failure()
    if promoted is not None:
        _raise_item_failure(promoted)


async def fire_notifications_collector(  # noqa: PLR0912, PLR0915
    feed: LeasedFeed,
    shutdown_event: asyncio.Event,
    url_base: str,
    _resources: CaptureResources,
) -> AsyncIterator[CapturedChunk]:
    """Capture Fire Notifications audio via HTTP Polling.

    Yields :class:`CapturedChunk` for each new MP3 file found.
    """
    try:
        s3_base_url = _require_env("FIRE_NOTIFICATIONS_S3_BASE")
    except ValueError as e:
        raise collector_failure(
            FeedStatusReason.SYSTEM_CONFIGURATION_INVALID,
            "missing_fire_notifications_s3_base",
        ) from e
    try:
        headers = _build_auth_headers()
    except ValueError as e:
        raise collector_failure(
            FeedStatusReason.SYSTEM_CONFIGURATION_INVALID,
            "missing_fire_notifications_auth_config",
        ) from e

    source_feed_id = feed.get("source_feed_id")
    if not source_feed_id:
        logger.error(
            "Feed %s (%s) missing source_feed_id",
            feed["id"],
            feed["name"],
        )
        raise missing_source_feed_id_failure()

    # source_feed_id is e.g. RECORDINGS/SAN-JOSE-DISP
    # Ensure no double slashes if url_base ends with /
    if not url_base.endswith("/"):
        url_base += "/"
    poll_url = f"{url_base}{source_feed_id}"

    # Track UUIDs we've already ingested to prevent duplicates.
    # We use a deque with maxlen to prevent unbounded memory growth.
    processed_uuids: collections.deque[str] = collections.deque(maxlen=1000)
    consecutive_failures = 0
    connection_session_id = str(uuid.uuid4())

    session = AsyncSession()

    try:
        while not shutdown_event.is_set():
            poll_ok = False
            poll_failure: ItemFailure | None = None

            try:
                # Poll the API
                resp = await session.get(
                    poll_url, headers=headers, timeout=10.0
                )
                if resp.status_code == 200:
                    data = resp.json()
                    files = data.get("files", [])

                    async for chunk in _process_file_list(
                        files,
                        session,
                        shutdown_event,
                        connection_session_id,
                        feed,
                        processed_uuids,
                        source_feed_id,
                        s3_base_url,
                    ):
                        yield chunk
                    poll_ok = True
                else:
                    poll_failure = _poll_status_failure(resp.status_code)
                    logger.warning(
                        "FN API returned %d: %s", resp.status_code, poll_url
                    )
            except CollectorFailure:
                raise
            except Exception:
                poll_failure = ItemFailure(
                    FeedStatusReason.SOURCE_UNREACHABLE,
                    "source_unreachable",
                )
                logger.warning(
                    "FN API poll error: %s",
                    poll_url,
                    exc_info=True,
                )

            if poll_ok:
                consecutive_failures = 0
            else:
                consecutive_failures += 1
                if consecutive_failures >= _MAX_CONSECUTIVE_FAILURES:
                    failure = poll_failure or ItemFailure(
                        FeedStatusReason.SOURCE_UNREACHABLE,
                        "source_unreachable",
                    )
                    raise collector_failure(
                        failure.status_reason,
                        failure.reason,
                    )

            # Sleep before next poll, with a small jitter
            jitter = random.uniform(0, 5.0)  # noqa: S311
            if await _sleep_or_shutdown(
                shutdown_event, _POLL_INTERVAL_SEC + jitter
            ):
                return

    finally:
        await session.close()
