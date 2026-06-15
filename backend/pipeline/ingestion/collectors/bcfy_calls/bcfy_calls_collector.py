from __future__ import annotations

import asyncio
import collections
import dataclasses
import datetime
import logging
import os
import uuid
from typing import TYPE_CHECKING, Any, cast

from google.cloud import secretmanager

from backend.pipeline.ingestion import failure_policy
from backend.pipeline.ingestion.collectors import (
    aiohttp_requests,
    control_flow,
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
    CaptureResources,
    FeedFailure,
    SourceObservation,
)
from backend.pipeline.ingestion.slo_contract import (
    EVENT_TYPE_BCFY_JWT_FETCH_FAILED,
    EVENT_TYPE_CALL_AUTH_FAILURE,
)
from backend.pipeline.storage.feed_store import FeedStatusReason

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    import aiohttp

    from backend.pipeline.storage.feed_store import LeasedFeed

logger = logging.getLogger(__name__)

_CALLS_API_MAX_ATTEMPTS = 4
_POLL_INTERVAL_SEC = 10.0
_AUDIO_TIMEOUT_SEC = 60.0
_AUDIO_FILE_DOWNLOAD_MAX_ATTEMPTS = 4
_AUDIO_FILE_DOWNLOAD_BACKOFF_BASE_SEC = 1.0
_MAX_CONSECUTIVE_FAILURES = 10
_TRANSIENT_CALLS_API_FAILURES = frozenset(
    {
        FeedStatusReason.SOURCE_RATE_LIMITED,
        FeedStatusReason.SOURCE_UNREACHABLE,
    }
)

# This policy is only for the Calls API/metadata endpoint. A 404 here means
# the configured source group/feed id is not valid for the API, unlike an item
# media URL 404 that may be a stale object and should stay item-scoped.
_CALLS_API_HTTP_POLICY = http_status.HTTPStatusPolicy(
    exact={
        **http_status.DEFAULT_HTTP_STATUS_POLICY.exact,
        404: FeedStatusReason.SYSTEM_CONFIGURATION_INVALID,
    },
)


class _JwtCacheState:
    token: str | None
    refresh_task: asyncio.Task[str] | None
    lock: asyncio.Lock | None
    lock_loop: asyncio.AbstractEventLoop | None

    def __init__(self) -> None:
        self.token = None
        self.refresh_task = None
        self.lock = None
        self.lock_loop = None


_jwt_state = _JwtCacheState()


@dataclasses.dataclass(frozen=True)
class _CallChunkResult:
    chunk: CapturedChunk | None = None
    failure: ItemFailure | None = None


def _get_jwt_token() -> str:
    """Fetch Broadcastify JWT token synchronously from Secret Manager."""
    # Currently there is no implicit redirection support for the Secret Manager SDK to another endpoint.
    mock_token = os.getenv("MOCK_JWT_TOKEN")
    if mock_token:
        return mock_token

    project_id = os.getenv("GOOGLE_CLOUD_PROJECT")
    secret_id = os.getenv("BROADCASTIFY_JWT_SECRET_ID")
    if not project_id or not secret_id:
        raise collector_failure(
            FeedStatusReason.SYSTEM_RUNTIME_CONFIGURATION_INVALID,
            "calls_jwt_config_missing",
            policy_evidence=failure_policy.FailurePolicyEvidence(
                owner_scope=failure_policy.OwnerScope.SOURCE_CLASS,
                failure_scope=failure_policy.FailureScope.FEED,
                endpoint_kind=failure_policy.EndpointKind.CALLS_API,
            ),
        )

    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
    try:
        response = client.access_secret_version(request={"name": name})
        return response.payload.data.decode("UTF-8").strip()
    except Exception as e:
        logger.exception("Failed to access secret %s: %s", name, e)
        raise collector_failure(
            FeedStatusReason.SYSTEM_CREDENTIAL_ACCESS_FAILED,
            "calls_jwt_secret_access_failed",
            policy_evidence=failure_policy.FailurePolicyEvidence(
                owner_scope=failure_policy.OwnerScope.CREDENTIAL_SCOPE,
                failure_scope=failure_policy.FailureScope.FEED,
                endpoint_kind=failure_policy.EndpointKind.CALLS_API,
            ),
        ) from e


def _get_jwt_lock() -> asyncio.Lock:
    """Return a lazy lock tied to the current running loop."""
    loop = asyncio.get_running_loop()
    loop_changed = (
        _jwt_state.lock_loop is not None and _jwt_state.lock_loop is not loop
    )
    if loop_changed:
        _jwt_state.refresh_task = None
    if _jwt_state.lock is None or loop_changed:
        _jwt_state.lock = asyncio.Lock()
        _jwt_state.lock_loop = loop
    return _jwt_state.lock


def _reset_jwt_cache_for_tests() -> None:
    """Reset module-level JWT cache state for unit tests."""
    _jwt_state.token = None
    _jwt_state.refresh_task = None
    _jwt_state.lock = None
    _jwt_state.lock_loop = None


async def _get_shared_jwt_token(
    *,
    force_refresh: bool = False,
    stale_token: str | None = None,
) -> str:
    """Fetch the shared Broadcastify JWT with cooperative async singleflight."""
    if _jwt_state.token is not None and not force_refresh:
        return _jwt_state.token

    lock = _get_jwt_lock()
    async with lock:
        if _jwt_state.token is not None and not force_refresh:
            return _jwt_state.token
        if (
            force_refresh
            and stale_token is not None
            and _jwt_state.token is not None
            and _jwt_state.token != stale_token
        ):
            return _jwt_state.token
        if _jwt_state.refresh_task is None:
            _jwt_state.refresh_task = asyncio.create_task(
                asyncio.to_thread(_get_jwt_token)
            )
        task = _jwt_state.refresh_task

    try:
        token = await asyncio.shield(task)
    except Exception as e:
        should_log = False
        async with lock:
            if _jwt_state.refresh_task is task:
                _jwt_state.refresh_task = None
                should_log = True
        is_config_failure = isinstance(e, FeedFailure) and e.status_reason in (
            FeedStatusReason.SYSTEM_CONFIGURATION_INVALID,
            FeedStatusReason.SYSTEM_RUNTIME_CONFIGURATION_INVALID,
        )
        if should_log and not is_config_failure:
            logger.warning(
                "Failed to fetch Broadcastify JWT token from Secret Manager",
                exc_info=e,
                extra={
                    "json_fields": {
                        "event_type": EVENT_TYPE_BCFY_JWT_FETCH_FAILED,
                    },
                },
            )
        raise

    async with lock:
        if _jwt_state.refresh_task is task:
            _jwt_state.token = token
            _jwt_state.refresh_task = None
            return token
        if _jwt_state.token is not None:
            return _jwt_state.token
        return token


async def _get_shared_jwt_token_with_retry(
    feed_id: object,
    shutdown_event: asyncio.Event,
    *,
    force_refresh: bool = False,
    stale_token: str | None = None,
) -> str | None:
    """Retry transient shared JWT fetch failures without releasing the lease."""
    jwt_failures = 0
    while not shutdown_event.is_set():
        try:
            return await _get_shared_jwt_token(
                force_refresh=force_refresh,
                stale_token=stale_token,
            )
        except FeedFailure as e:
            if e.status_reason in (
                FeedStatusReason.SYSTEM_CONFIGURATION_INVALID,
                FeedStatusReason.SYSTEM_RUNTIME_CONFIGURATION_INVALID,
            ):
                raise
            jwt_failures = await _handle_loop_failure(
                feed_id,
                jwt_failures,
                shutdown_event,
                ItemFailure(e.status_reason, e.reason),
            )
        except Exception:
            jwt_failures = await _handle_loop_failure(
                feed_id,
                jwt_failures,
                shutdown_event,
                ItemFailure(
                    FeedStatusReason.SYSTEM_CREDENTIAL_ACCESS_FAILED,
                    "calls_jwt_secret_access_failed",
                ),
            )
    return None


def _log_calls_api_response_invalid(feed_id: object) -> None:
    """Log invalid successful Calls API payloads before classification."""
    logger.error(
        "Invalid Broadcastify Calls API response payload (feed %s)",
        feed_id,
    )


def _validate_calls_api_payload(
    payload: object,
    feed_id: object,
) -> dict[str, Any]:
    """Validate the Calls API payload shape before processing."""
    if not isinstance(payload, dict):
        _log_calls_api_response_invalid(feed_id)
        msg = "payload must be an object"
        raise TypeError(msg)
    calls_payload = cast("dict[str, Any]", payload)
    response_calls = calls_payload.get("calls", [])
    if not isinstance(response_calls, list):
        _log_calls_api_response_invalid(feed_id)
        msg = "calls field must be a list"
        raise TypeError(msg)
    return calls_payload


async def _fetch_calls(
    session: aiohttp.ClientSession,
    url: str,
    headers: dict[str, str],
    params: dict[str, Any],
    feed_id: object,
    shutdown_event: asyncio.Event,
) -> dict[str, Any]:
    """Fetch one Broadcastify Calls API page with request-level retries."""

    def validate_payload(payload: object) -> dict[str, Any]:
        return _validate_calls_api_payload(payload, feed_id)

    return await aiohttp_requests.fetch_json_with_retries(
        session,
        url,
        shutdown_event,
        retry_config=aiohttp_requests.RetryConfig(
            timeout_sec=10.0,
            max_attempts=_CALLS_API_MAX_ATTEMPTS,
            base_delay_sec=1.0,
            jitter_max_sec=1.0,
            sleep_func=control_flow.sleep_or_cancel,
        ),
        headers=headers,
        params=params,
        log_label=f"Calls API feed {feed_id}",
        reason_prefix="calls_api_http",
        status_policy=_CALLS_API_HTTP_POLICY,
        validate_payload=validate_payload,
        invalid_payload_status_reason=(
            FeedStatusReason.SYSTEM_SOURCE_PAYLOAD_INVALID
        ),
        invalid_payload_reason="calls_api_response_invalid",
        transport_status_reason=FeedStatusReason.SOURCE_UNREACHABLE,
        transport_reason="calls_api_http_transport_failed",
        failure_scope=failure_policy.FailureScope.FEED,
        endpoint_kind=failure_policy.EndpointKind.CALLS_API,
    )


async def _download_audio(
    session: aiohttp.ClientSession,
    audio_url: str,
    shutdown_event: asyncio.Event,
    out_headers: dict[str, str] | None = None,
) -> bytes | ItemFailure:
    """Download audio file."""
    result = await aiohttp_requests.download_item_media(
        session,
        audio_url,
        shutdown_event,
        retry_config=aiohttp_requests.RetryConfig(
            timeout_sec=_AUDIO_TIMEOUT_SEC,
            max_attempts=_AUDIO_FILE_DOWNLOAD_MAX_ATTEMPTS,
            base_delay_sec=_AUDIO_FILE_DOWNLOAD_BACKOFF_BASE_SEC,
            sleep_func=control_flow.sleep_or_cancel,
        ),
        log_label="Broadcastify Calls item audio",
    )
    if isinstance(result, aiohttp_requests.DownloadedItem):
        if out_headers is not None:
            out_headers.update(result.headers)
        return result.content
    return result


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
        failure_scope=failure_policy.FailureScope.OBSERVATION,
        endpoint_kind=failure_policy.EndpointKind.CALLS_API,
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
) -> _CallChunkResult:
    """Download audio for a single call and wrap it in a CapturedChunk."""
    out_h = out_headers if out_headers is not None else {}
    try:
        audio_result = await _download_audio(
            session, audio_url, shutdown_event, out_h
        )
    except asyncio.CancelledError:
        raise
    except Exception as e:
        logger.exception("Failed to process audio for %s: %s", audio_url, e)
        return _CallChunkResult(failure=item_downloads.item_download_failed(e))

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

    # Resume cursor: the call's own API index time `ts`. On a mid-page crash
    # between two calls sharing a `ts` second the later one is not re-fetched
    # (strict `ts > pos`) -- an accepted, bounded data-loss case.
    ts = result.get("ts")
    if ts is not None:
        resume_position = datetime.datetime.fromtimestamp(ts, datetime.UTC)
    else:
        resume_position = None
        logger.warning(
            "bcfy_calls call missing 'ts' (API pagination key) -- resume "
            "cursor falls back to chunk_end_time",
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
        policy_evidence=policy_evidence_for_status_reason(
            failure.status_reason,
            failure_scope=failure_policy.FailureScope.ITEM,
            endpoint_kind=failure_policy.EndpointKind.CALLS_MEDIA,
        ),
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
            policy_evidence=policy_evidence_for_status_reason(
                failure.status_reason,
                failure_scope=failure_policy.FailureScope.ITEM,
                endpoint_kind=failure_policy.EndpointKind.CALLS_MEDIA,
            ),
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

    jwt_token = await _get_shared_jwt_token_with_retry(feed_id, shutdown_event)
    if jwt_token is None:
        return

    normalized_url_base = url_base if url_base.endswith("/") else f"{url_base}/"
    headers = {"Authorization": f"Bearer {jwt_token}"}

    seen_urls = collections.deque(maxlen=1000)
    consecutive_failures = 0

    # HTTP-01: reuse runtime-owned session (D-04, D-05).
    # The runtime owns lifecycle; do NOT close here.
    session = resources.http_session
    while not shutdown_event.is_set():
        params: dict[str, Any] = {"groups": source_feed_id}
        if last_bookmark_time_unix is not None:
            params["pos"] = last_bookmark_time_unix

        try:
            try:
                fetch_result = await _fetch_calls(
                    session,
                    normalized_url_base,
                    headers,
                    params,
                    feed_id,
                    shutdown_event,
                )
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

            bcfy_calls = fetch_result

            calls = _extract_calls_from_response(bcfy_calls)

            if calls:
                # One Broadcastify Calls API response page is the observation
                # boundary: isolated media failures are skipped, but an entire
                # page of failed attempted call items is promoted.
                outcome = ItemBatchOutcome()
                # Sort the page by the API index time `ts` so the per-call
                # resume cursor advances monotonically; data-loss is then
                # bounded to the accepted tie case (calls sharing a `ts`).
                calls.sort(key=lambda c: c.get("ts") or 0)
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
                last_bookmark_time_unix = bcfy_calls["lastPos"]

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
            refreshed_token = await _get_shared_jwt_token_with_retry(
                feed_id,
                shutdown_event,
                force_refresh=True,
                stale_token=jwt_token,
            )
            if refreshed_token is None:
                return
            jwt_token = refreshed_token
            headers["Authorization"] = f"Bearer {jwt_token}"
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
