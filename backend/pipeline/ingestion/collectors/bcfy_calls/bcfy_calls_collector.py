from __future__ import annotations

# This compatibility adapter deliberately delegates through provider internals
# while preserving the legacy collector's test seams during the migration.
# ruff: noqa: SLF001
import asyncio
import collections
import dataclasses
import datetime
import logging
import math
import uuid
from typing import TYPE_CHECKING, Any

from google.cloud import secretmanager

from backend.pipeline.ingestion.collectors import (
    control_flow,
    item_downloads,
    payloads,
    telemetry,
)
from backend.pipeline.ingestion.collectors.bcfy_calls import provider
from backend.pipeline.ingestion.collectors.failure_classification import (
    ItemBatchOutcome,
    ItemFailure,
    collector_failure,
    missing_source_feed_id_failure,
)
from backend.pipeline.ingestion.models import (
    AudioMimeType,
    CapturedChunk,
    CaptureEvent,
    CaptureResources,
    FeedFailure,
    SourceObservation,
)
from backend.pipeline.ingestion.slo_contract import EVENT_TYPE_CALL_AUTH_FAILURE
from backend.pipeline.storage.feed_store import FeedStatusReason

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Mapping

    import aiohttp

    from backend.pipeline.storage.feed_store import LeasedFeed

logger = logging.getLogger(__name__)

_POLL_INTERVAL_SEC = 10.0
_MAX_CONSECUTIVE_FAILURES = 10
_CALLS_API_MAX_ATTEMPTS = provider._CALLS_API_MAX_ATTEMPTS
_AUDIO_TIMEOUT_SEC = provider._AUDIO_TIMEOUT_SEC
_AUDIO_FILE_DOWNLOAD_MAX_ATTEMPTS = provider._AUDIO_FILE_DOWNLOAD_MAX_ATTEMPTS
_AUDIO_FILE_DOWNLOAD_BACKOFF_BASE_SEC = (
    provider._AUDIO_FILE_DOWNLOAD_BACKOFF_BASE_SEC
)
_TRANSIENT_CALLS_API_FAILURES = frozenset(
    {
        FeedStatusReason.SOURCE_RATE_LIMITED,
        FeedStatusReason.SOURCE_UNREACHABLE,
    }
)

_CALLS_API_HTTP_POLICY = provider._CALLS_API_HTTP_POLICY
_JwtCacheState = provider._JwtCacheState
_jwt_state = provider._jwt_state


@dataclasses.dataclass(frozen=True)
class _CallChunkResult:
    chunk: CapturedChunk | None = None
    failure: ItemFailure | None = None


def _get_jwt_token() -> str:
    """Forward the existing test seam to the shared provider."""
    return provider._get_jwt_token(
        _client_factory=secretmanager.SecretManagerServiceClient
    )


def _get_jwt_lock() -> asyncio.Lock:
    """Forward the existing test seam to the shared provider."""
    return provider._get_jwt_lock()


def _reset_jwt_cache_for_tests() -> None:
    """Reset the one provider-owned process-wide JWT cache."""
    provider._reset_jwt_cache_for_tests()


async def _get_shared_jwt_token(
    *,
    force_refresh: bool = False,
    stale_token: str | None = None,
) -> str:
    """Forward the existing test seam to the shared provider."""
    return await provider._get_shared_jwt_token(
        force_refresh=force_refresh,
        stale_token=stale_token,
        _token_fetcher=_get_jwt_token,
    )


async def _get_shared_jwt_token_with_retry(
    shutdown_event: asyncio.Event,
    *,
    force_refresh: bool = False,
    stale_token: str | None = None,
) -> str | None:
    """Forward the existing test seam to the shared provider."""
    return await provider._get_shared_jwt_token_with_retry(
        shutdown_event,
        force_refresh=force_refresh,
        stale_token=stale_token,
        _token_fetcher=_get_jwt_token,
    )


def _log_calls_api_response_invalid() -> None:
    """Forward the existing test seam to the shared provider."""
    provider._log_calls_api_response_invalid()


def _validate_calls_api_payload(
    payload: object,
) -> dict[str, Any]:
    """Forward the existing test seam to the shared provider."""
    validated = provider._validate_calls_api_payload(payload)
    return dict(validated)


async def _fetch_calls(
    session: aiohttp.ClientSession,
    url: str,
    headers: Mapping[str, str],
    params: Mapping[str, Any],
    shutdown_event: asyncio.Event,
) -> dict[str, Any]:
    """Forward the existing test seam to the shared provider."""
    result = await provider._fetch_calls(
        session,
        url,
        headers,
        params,
        shutdown_event,
    )
    return dict(result)


async def _download_audio(
    session: aiohttp.ClientSession,
    audio_url: str,
    shutdown_event: asyncio.Event,
    out_headers: dict[str, str] | None = None,
) -> bytes | ItemFailure:
    """Forward the existing test seam to the shared provider."""
    return await provider._download_audio(
        session,
        audio_url,
        shutdown_event,
        out_headers,
    )


def _extract_calls_from_response(
    bcfy_calls: dict[str, Any] | None,
) -> list[dict[str, Any]]:
    """Safely extract the calls list from the API response."""
    if bcfy_calls is None:
        return []
    return payloads.extract_optional_item_list(
        bcfy_calls,
        "calls",
        malformed_reason="calls_api_payload_malformed",
    )


def _last_pos_to_resume_position(
    bcfy_calls: dict[str, Any] | None,
) -> datetime.datetime | None:
    """Convert a Broadcastify Calls ``lastPos`` cursor to UTC datetime."""
    if not bcfy_calls or bcfy_calls.get("lastPos") is None:
        return None
    try:
        return datetime.datetime.fromtimestamp(
            int(float(bcfy_calls["lastPos"])),
            datetime.UTC,
        )
    except (TypeError, ValueError, OSError, OverflowError):
        logger.warning(
            "bcfy_calls response contained invalid lastPos",
            extra={
                "json_fields": {"event_type": "bcfy_calls_invalid_last_pos"}
            },
        )
        return None


async def _create_chunk_from_call(
    session: aiohttp.ClientSession,
    result: dict[str, Any],
    audio_url: str,
    shutdown_event: asyncio.Event,
    session_id: str,
    receipt_time: datetime.datetime,
    out_headers: dict[str, str] | None = None,
    calls_provider: provider.CallsProviderClient | None = None,
) -> _CallChunkResult:
    """Download audio for a single call and wrap it in a CapturedChunk."""
    out_h = out_headers if out_headers is not None else {}
    try:
        if calls_provider is None:
            audio_result = await _download_audio(
                session,
                audio_url,
                shutdown_event,
                out_h,
            )
        else:
            audio_result = await calls_provider.download_audio(
                audio_url,
                shutdown_event=shutdown_event,
                out_headers=out_h,
            )
    except asyncio.CancelledError:
        raise
    except Exception as error:
        # Do not attach the exception: transport errors may contain signed URLs.
        logger.error(  # noqa: TRY400
            "Failed to process Broadcastify Calls audio"
        )
        error_text = str(error)
        includes_sensitive_url = (
            audio_url in error_text
            or "://" in error_text
            or "signature=" in error_text
        )
        failure = item_downloads.item_download_failed(
            None if includes_sensitive_url else error
        )
        return _CallChunkResult(failure=failure)

    if isinstance(audio_result, ItemFailure):
        return _CallChunkResult(failure=audio_result)

    if not audio_result:
        return _CallChunkResult(
            failure=ItemFailure(
                FeedStatusReason.SYSTEM_COLLECTOR_ERROR,
                "item_download_failed",
            )
        )

    audio_bytes = audio_result

    now = datetime.datetime.now(datetime.UTC)
    chunk_start_time = _provider_timestamp(result.get("start_ts")) or now
    chunk_end_time = _provider_timestamp(result.get("end_ts")) or now

    # Resume cursor: the call's own API index time `ts`. On a mid-page crash
    # between two calls sharing a `ts` second the later one is not re-fetched
    # (strict `ts > pos`) -- an accepted, bounded data-loss case.
    resume_position = _provider_timestamp(result.get("ts"))
    if resume_position is None:
        logger.warning(
            "bcfy_calls call missing or invalid 'ts' (API pagination key) "
            "-- resume cursor falls back to chunk_end_time",
            extra={"json_fields": {"event_type": "bcfy_calls_missing_ts"}},
        )

    content_type = out_h.get("content-type")
    mime_type = AudioMimeType.from_string(content_type)

    return _CallChunkResult(
        chunk=CapturedChunk(
            audio_bytes=audio_bytes,
            chunk_start_time=chunk_start_time,
            chunk_end_time=chunk_end_time,
            session_id=session_id,
            receipt_time=receipt_time,
            mime_type=mime_type,
            resume_position=resume_position,
            external_audio_segment_id=audio_url,
        )
    )


def _raise_item_failure(failure: ItemFailure) -> None:
    """Raise a typed collector failure from an item aggregation result."""
    raise collector_failure(
        failure.status_reason,
        failure.reason,
    )


async def _handle_loop_failure(
    feed_id: object,
    consecutive_failures: int,
    shutdown_event: asyncio.Event,
    failure: ItemFailure,
) -> int:
    """Increment failure count, raise if exceeded, and sleep."""
    del feed_id
    consecutive_failures += 1
    if consecutive_failures >= _MAX_CONSECUTIVE_FAILURES:
        raise collector_failure(
            failure.status_reason,
            failure.reason,
        )
    await control_flow.sleep_or_cancel(shutdown_event, _POLL_INTERVAL_SEC)
    return consecutive_failures


async def capture_bcfy_calls(  # noqa: PLR0912, PLR0915
    feed: LeasedFeed,
    shutdown_event: asyncio.Event,
    url_base: str,
    resources: CaptureResources,
) -> AsyncIterator[CaptureEvent]:
    """Capture Broadcastify Calls audio and no-audio observations.

    Yields :class:`CapturedChunk` for processed call audio and
    :class:`SourceObservation` for successful empty/skipped-only API pages.

    Args:
        feed: Leased feed containing source_feed_id.
        shutdown_event: Signals graceful shutdown request.
        url_base: Full Broadcastify Calls API live endpoint URL to query.
            The function uses this URL directly after normalizing a
            trailing slash.
        resources: Runtime-owned CaptureResources. The http_session is
            the runtime-owned aiohttp.ClientSession created in
            CollectorRuntime._main(); per HTTP-01, the collector
            reuses it instead of constructing a new session
            per poll. Lifecycle is owned by the runtime — do not close.
    """
    connection_session_id = str(uuid.uuid4())
    source_feed_id = feed.get("source_feed_id")
    feed_id = feed.get("id")
    feed_name = feed.get("name")
    last_bookmark_time = feed.get("last_bookmark_time")
    last_bookmark_time_unix = (
        int(last_bookmark_time.timestamp()) if last_bookmark_time else None
    )
    if not source_feed_id:
        logger.error(
            "Feed %s (%s) missing source_feed_id",
            feed_id,
            feed_name,
        )
        raise missing_source_feed_id_failure()

    seen_urls = collections.deque(maxlen=1000)
    consecutive_failures = 0

    # HTTP-01: reuse runtime-owned session (D-04, D-05).
    # The runtime owns lifecycle; do NOT close here.
    session = resources.http_session
    calls_provider = provider.CallsProviderClient(
        session,
        url_base,
        _token_loader=_get_shared_jwt_token_with_retry,
        _json_fetcher=_fetch_calls,
        _media_downloader=_download_audio,
    )
    while not shutdown_event.is_set():
        try:
            try:
                page = await calls_provider.fetch_group_page(
                    source_feed_id,
                    last_bookmark_time_unix,
                    shutdown_event=shutdown_event,
                )
            except provider._TokenLoadStopped:
                return
            except FeedFailure as e:
                if e.status_reason not in _TRANSIENT_CALLS_API_FAILURES:
                    raise
                consecutive_failures = await _handle_loop_failure(
                    feed_id,
                    consecutive_failures,
                    shutdown_event,
                    ItemFailure(e.status_reason, e.reason),
                )
                continue

            bcfy_calls = dict(page.payload)

            calls = _extract_calls_from_response(bcfy_calls)

            if calls:
                # One Broadcastify Calls API response page is the observation
                # boundary: isolated media failures are skipped, but an entire
                # page of failed attempted call items is promoted.
                outcome = ItemBatchOutcome()
                # Sort the page by the API index time `ts` so the per-call
                # resume cursor advances monotonically; data-loss is then
                # bounded to the accepted tie case (calls sharing a `ts`).
                calls.sort(key=_provider_timestamp_sort_key)
                for result in calls:
                    if shutdown_event.is_set():
                        break

                    # SLO: receipt_time stamp — bcfy_calls per-call iteration
                    receipt_time = datetime.datetime.now(datetime.UTC)

                    audio_url = result.get("url")
                    if not audio_url or audio_url in seen_urls:
                        continue

                    outcome.record_attempt()
                    call_result = await _create_chunk_from_call(
                        session,
                        result,
                        audio_url,
                        shutdown_event,
                        connection_session_id,
                        receipt_time,
                        calls_provider=calls_provider,
                    )
                    if call_result.failure is not None:
                        outcome.record_failure(call_result.failure)

                    chunk = call_result.chunk
                    if not chunk:
                        if not shutdown_event.is_set():
                            telemetry.emit_call_download_failed(
                                logger,
                                feed_id=feed["id"],
                                source_type=feed["source_type"],
                            )
                        continue

                    yield chunk
                    outcome.record_chunk_produced()

                    # Only mark as seen and update pagination after a successful
                    # yield, confirming the chunk was handed off to the pipeline.
                    seen_urls.append(audio_url)
                    # Reset consecutive failures on successful yield
                    consecutive_failures = 0
                if shutdown_event.is_set():
                    return
                promoted = outcome.promoted_failure()
                if promoted is not None:
                    _raise_item_failure(promoted)
                is_skipped_only_page_while_running = (
                    outcome.attempted_count == 0
                    and not outcome.chunk_produced
                    and not shutdown_event.is_set()
                )
                if is_skipped_only_page_while_running:
                    consecutive_failures = 0
                    yield SourceObservation(
                        resume_position=_last_pos_to_resume_position(
                            bcfy_calls
                        ),
                    )
            elif bcfy_calls is not None:
                consecutive_failures = 0
                yield SourceObservation(
                    resume_position=_last_pos_to_resume_position(bcfy_calls),
                )
            # Update last_bookmark_time_unix for pagination AFTER processing
            # all calls in the response — ensures we don't skip any calls if
            # an error occurs mid-page.
            if bcfy_calls and "lastPos" in bcfy_calls:
                last_bookmark_time_unix = page.last_pos

            # Wait before polling again, gracefully interruptible by shutdown
            if shutdown_event.is_set():
                return
            await control_flow.sleep_or_cancel(
                shutdown_event, _POLL_INTERVAL_SEC
            )

        except FeedFailure as e:
            if (
                e.status_reason
                is not FeedStatusReason.SYSTEM_AUTHENTICATION_FAILED
            ):
                raise
            logger.warning(
                "Auth failure for feed %s; refreshing token.",
                feed_id,
                extra={
                    "json_fields": {
                        "event_type": EVENT_TYPE_CALL_AUTH_FAILURE,
                        "feed_id": str(feed_id),
                        "source_type": feed["source_type"],
                    },
                },
            )
            consecutive_failures = await _handle_loop_failure(
                feed_id,
                consecutive_failures,
                shutdown_event,
                ItemFailure(e.status_reason, e.reason),
            )
        except Exception as e:
            logger.exception("Error in capture_bcfy_calls loop: %s", e)
            consecutive_failures = await _handle_loop_failure(
                feed_id,
                consecutive_failures,
                shutdown_event,
                ItemFailure(
                    FeedStatusReason.SOURCE_UNREACHABLE,
                    "source_unreachable",
                ),
            )
