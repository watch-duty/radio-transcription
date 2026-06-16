from __future__ import annotations

import asyncio
import base64
import collections
import datetime
import logging
import random
import uuid
from typing import TYPE_CHECKING, Any
from zoneinfo import ZoneInfo

from backend.pipeline.common.audio import get_audio_duration
from backend.pipeline.ingestion.collectors import aiohttp_requests
from backend.pipeline.ingestion import quarantine_reason
from backend.pipeline.ingestion.collectors import (
    control_flow,
    failure_classification,
    item_downloads,
    payloads,
    telemetry,
)
from backend.pipeline.ingestion.collectors.failure_classification import (
    ItemBatchOutcome,
    ItemFailure,
    collector_failure,
    missing_source_feed_id_failure,
)
from backend.pipeline.ingestion.failure_classifiers import (
    http_status,
)
from backend.pipeline.ingestion.models import (
    AudioMimeType,
    CapturedChunk,
    CaptureEvent,
    FeedFailure,
    SourceObservation,
)
from backend.pipeline.ingestion.settings import _require_env
from backend.pipeline.storage import feed_store

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    import aiohttp

    from backend.pipeline.ingestion.models import CaptureResources
    from backend.pipeline.storage.feed_store import LeasedFeed

logger = logging.getLogger(__name__)

_DOWNLOAD_MAX_RETRIES = 3
_DOWNLOAD_BACKOFF_BASE_SEC = 1.0
_POLL_INTERVAL_SEC = 30.0
_MAX_CONSECUTIVE_FAILURES = 10

# The poll/list endpoint is configuration-owned: terminal 4xx responses usually
# mean our channel/path/auth setup is wrong. Per-MP3 download URLs use the
# default item policy instead because one stale object should not blame the feed.
_FN_POLL_HTTP_POLICY = http_status.HTTPStatusPolicy(
    exact=http_status.DEFAULT_HTTP_STATUS_POLICY.exact,
    default_4xx=(feed_store.FeedStatusReason.SYSTEM_CONFIGURATION_INVALID),
    default_5xx=feed_store.FeedStatusReason.SOURCE_UNREACHABLE,
    default_other_failure=feed_store.FeedStatusReason.SOURCE_UNREACHABLE,
)


def _classify_poll_status(
    status: int,
) -> failure_classification.FailureInfo:
    """Classify a terminal Fire Notifications poll status."""
    status_reason = http_status.classify_http_status(
        status,
        policy=_FN_POLL_HTTP_POLICY,
    )
    if status_reason is not None:
        return failure_classification.FailureInfo(
            status_reason,
            f"fn_api_http_{status}",
        )
    return failure_classification.FailureInfo(
        feed_store.FeedStatusReason.SOURCE_UNREACHABLE,
        "source_unreachable",
    )


def _build_auth_headers() -> dict[str, str]:
    """Build HTTP Basic Authorization headers from env vars, raising if missing."""
    user = _require_env("FIRE_NOTIFICATIONS_USER")
    password = _require_env("FIRE_NOTIFICATIONS_PASSWORD")
    credentials = f"{user}:{password}"
    encoded = base64.b64encode(credentials.encode()).decode()
    return {"Authorization": f"Basic {encoded}"}


async def _download_audio(
    session: aiohttp.ClientSession,
    url: str,
    shutdown: asyncio.Event,
) -> bytes | ItemFailure:
    """Download audio file from S3 with retries."""
    result = await aiohttp_requests.download_item_media(
        session,
        url,
        shutdown,
        retry_config=aiohttp_requests.RetryConfig(
            timeout_sec=30.0,
            max_attempts=_DOWNLOAD_MAX_RETRIES,
            base_delay_sec=_DOWNLOAD_BACKOFF_BASE_SEC,
            sleep_func=control_flow.sleep_or_cancel,
        ),
        log_label="Fire Notifications item audio",
    )
    if isinstance(result, aiohttp_requests.DownloadedItem):
        return result.content
    return result


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
    session: aiohttp.ClientSession,
    shutdown_event: asyncio.Event,
    connection_session_id: str,
    feed: LeasedFeed,
    processed_uuids: collections.deque[str],
    source_feed_id: str,
    s3_base_url: str,
    outcome: ItemBatchOutcome,
) -> AsyncIterator[CapturedChunk]:
    """Filter, sort and process audio files, yielding CapturedChunks."""
    # Filter for files and sort by name to process chronologically
    audio_files = [
        f
        for f in files
        if f.get("type") == "file" and f.get("name", "").endswith(".mp3")
    ]
    audio_files.sort(key=lambda x: x.get("name", ""))

    # A Fire Notifications file-list response is the observation boundary:
    # all eligible attempted MP3s failing is meaningful, but isolated stale or
    # corrupt files should not mark the feed unhealthy.
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
        audio_result = await _download_audio(session, s3_url, shutdown_event)
        if isinstance(audio_result, ItemFailure):
            outcome.record_failure(audio_result)
            if not shutdown_event.is_set():
                telemetry.emit_call_download_failed(
                    logger,
                    feed_id=feed["id"],
                    source_type=feed["source_type"],
                )
            continue
        mp3_bytes = audio_result

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
                    feed_store.FeedStatusReason.SYSTEM_COLLECTOR_ERROR,
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
            resume_position=start_time,
            external_audio_segment_id=f"{file_uuid}|{filename}",
        )
        # Only mark as processed after a successful yield, confirming
        # the chunk was handed off to the pipeline.
        processed_uuids.append(file_uuid)
        feed["last_bookmark_time"] = start_time
        outcome.record_chunk_produced()

    if shutdown_event.is_set():
        return

    promoted = outcome.promoted_failure()
    if promoted is not None:
        raise collector_failure(promoted.status_reason, promoted.reason)


async def _fetch_file_list(
    session: aiohttp.ClientSession,
    url: str,
    headers: dict[str, str],
    feed_id: object,
    shutdown_event: asyncio.Event,
) -> dict[str, Any]:
    """Fetch the Fire Notifications file list with request-level retries."""

    def validate_payload(payload: object) -> dict[str, Any]:
        if not isinstance(payload, dict):
            raise ValueError("Payload must be a dictionary")
        return payload

    return await aiohttp_requests.fetch_json_with_retries(
        session,
        url,
        shutdown_event,
        retry_config=aiohttp_requests.RetryConfig(
            timeout_sec=10.0,
            max_attempts=3,
            base_delay_sec=1.0,
            sleep_func=control_flow.sleep_or_cancel,
        ),
        headers=headers,
        log_label=f"Fire Notifications API feed {feed_id}",
        reason_prefix="fn_api_http",
        status_policy=_FN_POLL_HTTP_POLICY,
        validate_payload=validate_payload,
        invalid_payload_status_reason=feed_store.FeedStatusReason.SYSTEM_COLLECTOR_ERROR,
        invalid_payload_reason="fn_api_payload_malformed",
        transport_status_reason=feed_store.FeedStatusReason.SOURCE_UNREACHABLE,
        transport_reason="source_unreachable",
    )


async def fire_notifications_collector(  # noqa: PLR0912, PLR0915
    feed: LeasedFeed,
    shutdown_event: asyncio.Event,
    url_base: str,
    resources: CaptureResources,
) -> AsyncIterator[CaptureEvent]:
    """Capture Fire Notifications audio via HTTP Polling.

    Yields :class:`CapturedChunk` for each new MP3 file found, and
    :class:`SourceObservation` for successful empty/skipped-only file listings.
    """
    try:
        s3_base_url = _require_env("FIRE_NOTIFICATIONS_S3_BASE")
    except ValueError as e:
        raise collector_failure(
            feed_store.FeedStatusReason.SYSTEM_CONFIGURATION_INVALID,
            "missing_fire_notifications_s3_base",
        ) from e
    try:
        headers = _build_auth_headers()
    except ValueError as e:
        raise collector_failure(
            feed_store.FeedStatusReason.SYSTEM_CONFIGURATION_INVALID,
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
    last_poll_failure = failure_classification.FailureInfo(
        feed_store.FeedStatusReason.SOURCE_UNREACHABLE,
        "source_unreachable",
    )
    connection_session_id = str(uuid.uuid4())

    session = resources.http_session

    # Stagger initial polling requests to prevent thundering herd
    startup_delay = random.uniform(0, _POLL_INTERVAL_SEC)
    await control_flow.sleep_or_cancel(shutdown_event, startup_delay)

    while not shutdown_event.is_set():
        poll_ok = False

        try:
            # Poll the API
            data = await _fetch_file_list(
                session,
                poll_url,
                headers,
                feed["id"],
                shutdown_event,
            )
            files = payloads.extract_optional_item_list(
                data,
                "files",
                malformed_reason="fn_api_payload_malformed",
            )

            if files == []:
                yield SourceObservation()
            else:
                outcome = ItemBatchOutcome()
                async for chunk in _process_file_list(
                    files,
                    session,
                    shutdown_event,
                    connection_session_id,
                    feed,
                    processed_uuids,
                    source_feed_id,
                    s3_base_url,
                    outcome,
                ):
                    yield chunk
                is_skipped_only_listing_while_running = (
                    outcome.attempted_count == 0
                    and not outcome.chunk_produced
                    and not shutdown_event.is_set()
                )
                if is_skipped_only_listing_while_running:
                    yield SourceObservation()
            poll_ok = True
        except FeedFailure as exc:
            last_poll_failure = failure_classification.FailureInfo(
                exc.status_reason,
                exc.reason or "unknown_failure",
            )
            logger.warning(
                "FN API error feed %s: %s",
                feed["id"],
                exc,
            )
        except Exception as exc:
            last_poll_failure = failure_classification.FailureInfo(
                feed_store.FeedStatusReason.SOURCE_UNREACHABLE,
                f"source_unreachable: {quarantine_reason.exception_text(exc)}",
            )
            logger.warning(
                "FN API unexpected poll error feed %s",
                feed["id"],
                exc_info=True,
            )

        if poll_ok:
            consecutive_failures = 0
        else:
            consecutive_failures += 1
            if consecutive_failures >= _MAX_CONSECUTIVE_FAILURES:
                raise collector_failure(
                    last_poll_failure.status_reason,
                    last_poll_failure.reason,
                )

        # Sleep before next poll, with a small jitter
        if shutdown_event.is_set():
            return
        jitter = random.uniform(0, 5.0)  # noqa: S311
        await control_flow.sleep_or_cancel(
            shutdown_event, _POLL_INTERVAL_SEC + jitter
        )
