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

from curl_cffi.requests import AsyncSession

from backend.pipeline.common.audio import get_audio_duration
from backend.pipeline.ingestion import failure_policy, quarantine_reason
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
    policy_evidence_for_status_reason,
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
    session: AsyncSession,
    url: str,
    shutdown: asyncio.Event,
) -> bytes | ItemFailure:
    """Download audio file from S3 with retries."""
    # Note: We use a manual retry loop instead of 'tenacity' to easily
    # interrupt the backoff sleep when the shutdown event is set,
    # matching the pattern in bcfy_calls_collector.py.
    last_status: int | None = None
    last_exception: Exception | None = None
    for attempt in range(_DOWNLOAD_MAX_RETRIES):
        try:
            resp = await session.get(url, timeout=30.0)
            last_status = resp.status_code
            last_exception = None
            if resp.status_code == 200:
                return resp.content
            if http_status.is_retryable_http_status(resp.status_code):
                logger.warning(
                    "Download retryable %d (attempt %d/%d): url=%s",
                    resp.status_code,
                    attempt + 1,
                    _DOWNLOAD_MAX_RETRIES,
                    url,
                )
            else:
                logger.warning(
                    "Download non-retryable %d: url=%s",
                    resp.status_code,
                    url,
                )
                return item_downloads.item_http_failure(resp.status_code)
        except Exception as exc:
            last_exception = exc
            last_status = None
            logger.warning(
                "Download error (attempt %d/%d): url=%s",
                attempt + 1,
                _DOWNLOAD_MAX_RETRIES,
                url,
                exc_info=True,
            )
        if attempt < _DOWNLOAD_MAX_RETRIES - 1:
            await control_flow.sleep_or_cancel(
                shutdown, _DOWNLOAD_BACKOFF_BASE_SEC * (2**attempt)
            )

    logger.warning("Download failed after retries: url=%s", url)
    if last_status is not None:
        return item_downloads.item_http_failure(last_status)
    return item_downloads.item_download_failed(last_exception)


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
            resume_position=end_time,
            external_audio_segment_id=f"{file_uuid}|{filename}",
        )
        # Only mark as processed after a successful yield, confirming
        # the chunk was handed off to the pipeline.
        processed_uuids.append(file_uuid)
        outcome.record_chunk_produced()

    if shutdown_event.is_set():
        return

    promoted = outcome.promoted_failure()
    if promoted is not None:
        raise collector_failure(
            promoted.status_reason,
            promoted.reason,
            policy_evidence=policy_evidence_for_status_reason(
                promoted.status_reason,
                failure_scope=failure_policy.FailureScope.ITEM,
                endpoint_kind=failure_policy.EndpointKind.FIRE_POLL,
            ),
        )


async def fire_notifications_collector(  # noqa: PLR0912, PLR0915
    feed: LeasedFeed,
    shutdown_event: asyncio.Event,
    url_base: str,
    _resources: CaptureResources,
) -> AsyncIterator[CaptureEvent]:
    """Capture Fire Notifications audio via HTTP Polling.

    Yields :class:`CapturedChunk` for each new MP3 file found, and
    :class:`SourceObservation` for successful empty/skipped-only file listings.
    """
    try:
        s3_base_url = _require_env("FIRE_NOTIFICATIONS_S3_BASE")
    except ValueError as e:
        raise collector_failure(
            feed_store.FeedStatusReason.SYSTEM_RUNTIME_CONFIGURATION_INVALID,
            "missing_fire_notifications_s3_base",
            policy_evidence=failure_policy.FailurePolicyEvidence(
                owner_scope=failure_policy.OwnerScope.SOURCE_CLASS,
                failure_scope=failure_policy.FailureScope.FEED,
                endpoint_kind=failure_policy.EndpointKind.FIRE_POLL,
            ),
        ) from e
    try:
        headers = _build_auth_headers()
    except ValueError as e:
        raise collector_failure(
            feed_store.FeedStatusReason.SYSTEM_RUNTIME_CONFIGURATION_INVALID,
            "missing_fire_notifications_auth_config",
            policy_evidence=failure_policy.FailurePolicyEvidence(
                owner_scope=failure_policy.OwnerScope.SOURCE_CLASS,
                failure_scope=failure_policy.FailureScope.FEED,
                endpoint_kind=failure_policy.EndpointKind.FIRE_POLL,
            ),
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

    session = AsyncSession()

    try:
        while not shutdown_event.is_set():
            poll_ok = False

            try:
                # Poll the API
                resp = await session.get(
                    poll_url, headers=headers, timeout=10.0
                )
                if resp.status_code == 200:
                    try:
                        data = resp.json()
                    except ValueError as exc:
                        status_reason = (
                            feed_store.FeedStatusReason
                            .SYSTEM_SOURCE_PAYLOAD_INVALID
                        )
                        raise collector_failure(
                            status_reason,
                            "fn_api_payload_malformed: "
                            f"{quarantine_reason.exception_text(exc)}",
                            policy_evidence=policy_evidence_for_status_reason(
                                status_reason,
                                failure_scope=(
                                    failure_policy.FailureScope.OBSERVATION
                                ),
                                endpoint_kind=failure_policy.EndpointKind.FIRE_POLL,
                            ),
                        ) from exc
                    files = payloads.extract_optional_item_list(
                        data,
                        "files",
                        malformed_reason="fn_api_payload_malformed",
                        failure_scope=failure_policy.FailureScope.OBSERVATION,
                        endpoint_kind=failure_policy.EndpointKind.FIRE_POLL,
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
                else:
                    last_poll_failure = _classify_poll_status(resp.status_code)
                    logger.warning(
                        "FN API returned %d: %s", resp.status_code, poll_url
                    )
            except FeedFailure:
                raise
            except Exception as exc:
                last_poll_failure = failure_classification.FailureInfo(
                    feed_store.FeedStatusReason.SOURCE_UNREACHABLE,
                    "source_unreachable: "
                    f"{quarantine_reason.exception_text(exc)}",
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
                    raise collector_failure(
                        last_poll_failure.status_reason,
                        last_poll_failure.reason,
                        policy_evidence=policy_evidence_for_status_reason(
                            last_poll_failure.status_reason,
                            failure_scope=(
                                failure_policy.FailureScope.OBSERVATION
                            ),
                            endpoint_kind=failure_policy.EndpointKind.FIRE_POLL,
                        ),
                    )

            # Sleep before next poll, with a small jitter
            if shutdown_event.is_set():
                return
            jitter = random.uniform(0, 5.0)  # noqa: S311
            await control_flow.sleep_or_cancel(
                shutdown_event, _POLL_INTERVAL_SEC + jitter
            )

    finally:
        await session.close()
