"""Shared Broadcastify Calls credential, metadata, and media provider."""

from __future__ import annotations

import asyncio
import collections.abc
import dataclasses
import logging
import os
import typing

import aiohttp
from google.cloud import secretmanager

from backend.pipeline.ingestion.collectors import (
    aiohttp_requests,
    control_flow,
)
from backend.pipeline.ingestion.collectors.failure_classification import (
    ItemFailure,
    collector_failure,
)
from backend.pipeline.ingestion.failure_classifiers import http_status
from backend.pipeline.ingestion.models import FeedFailure
from backend.pipeline.ingestion.slo_contract import (
    EVENT_TYPE_BCFY_JWT_FETCH_FAILED,
)
from backend.pipeline.storage import feed_store

if typing.TYPE_CHECKING:
    import datetime
    import types

logger = logging.getLogger(__name__)


_CALLS_API_MAX_ATTEMPTS = 4
_CALLS_API_TIMEOUT_SEC = 10.0
_CALLS_API_BACKOFF_BASE_SEC = 1.0
_CALLS_API_JITTER_MAX_SEC = 1.0
_AUDIO_TIMEOUT_SEC = 60.0
_AUDIO_FILE_DOWNLOAD_MAX_ATTEMPTS = 4
_AUDIO_FILE_DOWNLOAD_BACKOFF_BASE_SEC = 1.0
_JWT_MAX_CONSECUTIVE_FAILURES = 10
_JWT_RETRY_INTERVAL_SEC = 10.0


# This policy is only for the Calls API/metadata endpoint. A 404 here means
# the configured selector is invalid, unlike an item media URL 404.
_CALLS_API_HTTP_POLICY = http_status.HTTPStatusPolicy(
    exact={
        **http_status.DEFAULT_HTTP_STATUS_POLICY.exact,
        404: feed_store.FeedStatusReason.SYSTEM_SOURCE_CONFIGURATION_INVALID,
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


@dataclasses.dataclass(frozen=True, slots=True)
class CallsPageEnvelope:
    """Validated metadata envelope with raw independently handled items."""

    payload: collections.abc.Mapping[str, object] = dataclasses.field(
        repr=False
    )
    calls: tuple[object, ...] = dataclasses.field(repr=False)
    last_pos: object | None = dataclasses.field(repr=False)
    http_attempt_count: int
    response_byte_count: int
    response_row_count: int
    response_distinct_audio_url_count: int | None = None

    def __post_init__(self) -> None:
        for name in (
            "http_attempt_count",
            "response_byte_count",
            "response_row_count",
        ):
            value = getattr(self, name)
            if (
                isinstance(value, bool)
                or not isinstance(value, int)
                or value < 0
            ):
                msg = f"{name} must be a nonnegative integer"
                raise ValueError(msg)
        if self.response_row_count != len(self.calls):
            msg = "response_row_count must equal the validated calls count"
            raise ValueError(msg)
        distinct_count = self.response_distinct_audio_url_count
        if distinct_count is not None and (
            isinstance(distinct_count, bool)
            or not isinstance(distinct_count, int)
            or not 0 <= distinct_count <= self.response_row_count
        ):
            msg = (
                "response_distinct_audio_url_count must be a nonnegative "
                "integer no greater than response_row_count"
            )
            raise ValueError(msg)


type _TokenFetcher = collections.abc.Callable[[], str]
type _TokenLoader = collections.abc.Callable[..., typing.Awaitable[str | None]]
type _JsonFetcher = collections.abc.Callable[
    [
        aiohttp.ClientSession,
        str,
        collections.abc.Mapping[str, str],
        collections.abc.Mapping[str, object],
        object,
        asyncio.Event,
    ],
    typing.Awaitable[collections.abc.Mapping[str, object]],
]
type _JsonEvidenceFetcher = collections.abc.Callable[
    [
        aiohttp.ClientSession,
        str,
        collections.abc.Mapping[str, str],
        collections.abc.Mapping[str, object],
        object,
        asyncio.Event,
        aiohttp_requests.HttpAttemptObserver | None,
    ],
    typing.Awaitable[
        aiohttp_requests.JsonFetchEvidence[collections.abc.Mapping[str, object]]
    ],
]
type _MediaDownloader = collections.abc.Callable[
    [
        aiohttp.ClientSession,
        str,
        asyncio.Event,
        dict[str, str] | None,
    ],
    typing.Awaitable[bytes | ItemFailure],
]


class _TokenLoadStopped(asyncio.CancelledError):
    """Signal that credential loading stopped because shutdown won."""


class _RedactedMediaRequestContext:
    """Hide signed URLs carried by aiohttp transport exceptions."""

    def __init__(self, context: typing.Any) -> None:
        self._context = context

    async def __aenter__(self) -> typing.Any:
        try:
            return await self._context.__aenter__()
        except (aiohttp.ClientError, TimeoutError):
            raise aiohttp.ClientError from None

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: types.TracebackType | None,
    ) -> bool | None:
        try:
            suppressed = await self._context.__aexit__(
                exc_type,
                exc,
                traceback,
            )
        except (aiohttp.ClientError, TimeoutError):
            raise aiohttp.ClientError from None
        if suppressed:
            return True
        if isinstance(exc, (aiohttp.ClientError, TimeoutError)):
            raise aiohttp.ClientError from None
        return False


class _RedactedMediaSession:
    """Delegate requests while sanitizing transport exceptions only."""

    def __init__(self, session: aiohttp.ClientSession) -> None:
        self._session = session

    def get(
        self,
        url: str,
        *,
        timeout: aiohttp.ClientTimeout,
    ) -> _RedactedMediaRequestContext:
        try:
            context = self._session.get(url, timeout=timeout)
        except (aiohttp.ClientError, TimeoutError):
            raise aiohttp.ClientError from None
        return _RedactedMediaRequestContext(context)


def _get_jwt_token(
    *,
    _client_factory: collections.abc.Callable[[], typing.Any] | None = None,
) -> str:
    """Fetch the Broadcastify JWT synchronously from Secret Manager."""
    mock_token = os.getenv("MOCK_JWT_TOKEN")
    if mock_token:
        return mock_token

    project_id = os.getenv("GOOGLE_CLOUD_PROJECT")
    secret_id = os.getenv("BROADCASTIFY_JWT_SECRET_ID")
    if not project_id or not secret_id:
        raise collector_failure(
            feed_store.FeedStatusReason.SYSTEM_RUNTIME_CONFIGURATION_INVALID,
            "calls_jwt_config_missing",
        )

    client_factory = _client_factory or secretmanager.SecretManagerServiceClient
    client = client_factory()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
    try:
        response = client.access_secret_version(request={"name": name})
        return response.payload.data.decode("UTF-8").strip()
    except Exception as error:
        # Do not attach the exception: SDK errors may contain credential data.
        logger.error(  # noqa: TRY400
            "Failed to access Broadcastify JWT secret"
        )
        raise collector_failure(
            feed_store.FeedStatusReason.SYSTEM_CREDENTIAL_ACCESS_FAILED,
            "calls_jwt_secret_access_failed",
        ) from error


def _get_jwt_lock() -> asyncio.Lock:
    """Return one lazy cache lock tied to the current running loop."""
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
    """Reset process-wide JWT state for deterministic unit tests."""
    _jwt_state.token = None
    _jwt_state.refresh_task = None
    _jwt_state.lock = None
    _jwt_state.lock_loop = None


async def _get_shared_jwt_token(
    *,
    force_refresh: bool = False,
    stale_token: str | None = None,
    _token_fetcher: _TokenFetcher | None = None,
) -> str:
    """Fetch the process-wide JWT with cooperative async singleflight."""
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
            token_fetcher = _token_fetcher or _get_jwt_token
            _jwt_state.refresh_task = asyncio.create_task(
                asyncio.to_thread(token_fetcher)
            )
        task = _jwt_state.refresh_task

    try:
        token = await asyncio.shield(task)
    except Exception as error:
        should_log = False
        async with lock:
            if _jwt_state.refresh_task is task:
                _jwt_state.refresh_task = None
                should_log = True
        is_config_failure = isinstance(error, FeedFailure) and (
            error.status_reason
            in (
                feed_store.FeedStatusReason.SYSTEM_CONFIGURATION_INVALID,
                feed_store.FeedStatusReason.SYSTEM_RUNTIME_CONFIGURATION_INVALID,
            )
        )
        if should_log and not is_config_failure:
            logger.warning(
                "Failed to fetch Broadcastify JWT token from Secret Manager",
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
    subject_id: object,
    shutdown_event: asyncio.Event,
    *,
    force_refresh: bool = False,
    stale_token: str | None = None,
    _token_fetcher: _TokenFetcher | None = None,
) -> str | None:
    """Retry transient shared JWT access without releasing authority."""
    del subject_id
    failures = 0
    while not shutdown_event.is_set():
        try:
            return await _get_shared_jwt_token(
                force_refresh=force_refresh,
                stale_token=stale_token,
                _token_fetcher=_token_fetcher,
            )
        except FeedFailure as error:
            if error.status_reason in (
                feed_store.FeedStatusReason.SYSTEM_CONFIGURATION_INVALID,
                feed_store.FeedStatusReason.SYSTEM_RUNTIME_CONFIGURATION_INVALID,
            ):
                raise
            failure = ItemFailure(error.status_reason, error.reason)
        except Exception:
            failure = ItemFailure(
                feed_store.FeedStatusReason.SYSTEM_CREDENTIAL_ACCESS_FAILED,
                "calls_jwt_secret_access_failed",
            )

        failures += 1
        if failures >= _JWT_MAX_CONSECUTIVE_FAILURES:
            raise collector_failure(failure.status_reason, failure.reason)
        await control_flow.sleep_or_cancel(
            shutdown_event,
            _JWT_RETRY_INTERVAL_SEC,
        )
    return None


def _log_calls_api_response_invalid(subject_id: object) -> None:
    """Log invalid successful Calls metadata without response contents."""
    logger.error(
        "Invalid Broadcastify Calls API response payload (subject %s)",
        subject_id,
    )


def _validate_calls_api_payload(
    payload: object,
    subject_id: object,
) -> collections.abc.Mapping[str, object]:
    """Validate only the Calls metadata envelope shape."""
    if not isinstance(payload, collections.abc.Mapping):
        _log_calls_api_response_invalid(subject_id)
        msg = "payload must be an object"
        raise TypeError(msg)
    validated = typing.cast("collections.abc.Mapping[str, object]", payload)
    calls = validated.get("calls", [])
    if not isinstance(calls, list):
        _log_calls_api_response_invalid(subject_id)
        msg = "calls field must be a list"
        raise TypeError(msg)
    return validated


async def _fetch_calls_with_evidence(
    session: aiohttp.ClientSession,
    url: str,
    headers: collections.abc.Mapping[str, str],
    params: collections.abc.Mapping[str, object],
    subject_id: object,
    shutdown_event: asyncio.Event,
    attempt_observer: aiohttp_requests.HttpAttemptObserver | None = None,
) -> aiohttp_requests.JsonFetchEvidence[collections.abc.Mapping[str, object]]:
    """Fetch one Calls page with transport-owned success evidence."""

    def validate_payload(
        payload: object,
    ) -> collections.abc.Mapping[str, object]:
        return _validate_calls_api_payload(payload, subject_id)

    return await aiohttp_requests.fetch_json_with_retries_evidence(
        session,
        url,
        shutdown_event,
        retry_config=aiohttp_requests.RetryConfig(
            timeout_sec=_CALLS_API_TIMEOUT_SEC,
            max_attempts=_CALLS_API_MAX_ATTEMPTS,
            base_delay_sec=_CALLS_API_BACKOFF_BASE_SEC,
            jitter_max_sec=_CALLS_API_JITTER_MAX_SEC,
            sleep_func=control_flow.sleep_or_cancel,
        ),
        headers=headers,
        params=params,
        log_label=f"Calls API subject {subject_id}",
        reason_prefix="calls_api_http",
        status_policy=_CALLS_API_HTTP_POLICY,
        validate_payload=validate_payload,
        invalid_payload_status_reason=(
            feed_store.FeedStatusReason.SYSTEM_SOURCE_PAYLOAD_INVALID
        ),
        invalid_payload_reason="calls_api_response_invalid",
        transport_status_reason=(
            feed_store.FeedStatusReason.SOURCE_UNREACHABLE
        ),
        transport_reason="calls_api_http_transport_failed",
        attempt_observer=attempt_observer,
    )


async def _fetch_calls(
    session: aiohttp.ClientSession,
    url: str,
    headers: collections.abc.Mapping[str, str],
    params: collections.abc.Mapping[str, object],
    subject_id: object,
    shutdown_event: asyncio.Event,
) -> collections.abc.Mapping[str, object]:
    """Fetch one Calls page through the legacy payload-only contract."""
    evidence = await _fetch_calls_with_evidence(
        session,
        url,
        headers,
        params,
        subject_id,
        shutdown_event,
    )
    return evidence.payload


async def _adapt_payload_json_fetcher(
    fetcher: _JsonFetcher,
    session: aiohttp.ClientSession,
    url: str,
    headers: collections.abc.Mapping[str, str],
    params: collections.abc.Mapping[str, object],
    subject_id: object,
    shutdown_event: asyncio.Event,
    attempt_observer: aiohttp_requests.HttpAttemptObserver | None,
) -> aiohttp_requests.JsonFetchEvidence[collections.abc.Mapping[str, object]]:
    """Adapt an injected legacy fetcher without inferring transport facts."""
    del attempt_observer
    payload = await fetcher(
        session,
        url,
        headers,
        params,
        subject_id,
        shutdown_event,
    )
    return aiohttp_requests.JsonFetchEvidence(
        payload=payload,
        http_attempt_count=0,
        response_byte_count=0,
    )


async def _download_audio(
    session: aiohttp.ClientSession,
    audio_url: str,
    shutdown_event: asyncio.Event,
    out_headers: dict[str, str] | None = None,
    *,
    attempt_observer: aiohttp_requests.HttpAttemptObserver | None = None,
) -> bytes | ItemFailure:
    """Download one Calls item with the shared media retry policy."""
    redacted_session = typing.cast(
        "aiohttp.ClientSession",
        _RedactedMediaSession(session),
    )
    result = await aiohttp_requests.download_item_media(
        redacted_session,
        audio_url,
        shutdown_event,
        retry_config=aiohttp_requests.RetryConfig(
            timeout_sec=_AUDIO_TIMEOUT_SEC,
            max_attempts=_AUDIO_FILE_DOWNLOAD_MAX_ATTEMPTS,
            base_delay_sec=_AUDIO_FILE_DOWNLOAD_BACKOFF_BASE_SEC,
            sleep_func=control_flow.sleep_or_cancel,
        ),
        log_label="Broadcastify Calls item audio",
        attempt_observer=attempt_observer,
    )
    if isinstance(result, aiohttp_requests.DownloadedItem):
        if out_headers is not None:
            out_headers.update(result.headers)
        return result.content
    return result


def _calls_page_envelope(
    payload: object,
    subject_id: object,
    *,
    http_attempt_count: int,
    response_byte_count: int,
) -> CallsPageEnvelope:
    validated = _validate_calls_api_payload(payload, subject_id)
    calls = typing.cast("list[object]", validated.get("calls", []))
    distinct_audio_urls: set[str] = set()
    for call in calls:
        if not isinstance(call, collections.abc.Mapping):
            continue
        call_mapping = typing.cast(
            "collections.abc.Mapping[str, object]",
            call,
        )
        audio_url = call_mapping.get("url")
        if isinstance(audio_url, str) and audio_url:
            distinct_audio_urls.add(audio_url)
    return CallsPageEnvelope(
        payload=validated,
        calls=tuple(calls),
        last_pos=validated.get("lastPos"),
        http_attempt_count=http_attempt_count,
        response_byte_count=response_byte_count,
        response_row_count=len(calls),
        response_distinct_audio_url_count=len(distinct_audio_urls),
    )


class CallsProviderClient:
    """Lightweight Calls client over one runtime-owned HTTP session."""

    def __init__(
        self,
        session: aiohttp.ClientSession,
        live_endpoint_url: str,
        *,
        _token_loader: _TokenLoader | None = None,
        _json_fetcher: _JsonFetcher | None = None,
        _media_downloader: _MediaDownloader | None = None,
    ) -> None:
        self._session = session
        self._live_endpoint_url = (
            live_endpoint_url
            if live_endpoint_url.endswith("/")
            else f"{live_endpoint_url}/"
        )
        self._token_loader = _token_loader or _get_shared_jwt_token_with_retry
        self._legacy_json_fetcher = _json_fetcher
        self._json_evidence_fetcher: _JsonEvidenceFetcher = (
            _fetch_calls_with_evidence
        )
        self._legacy_media_downloader = _media_downloader

    async def fetch_group_page(
        self,
        group_id: str,
        pos: object | None,
        *,
        subject_id: object,
        shutdown_event: asyncio.Event,
    ) -> CallsPageEnvelope:
        """Fetch one legacy inclusive groups page."""
        if not group_id:
            msg = "group_id must be a nonempty string"
            raise ValueError(msg)
        return await self._fetch_page(
            group_id=group_id,
            sid=None,
            pos=pos,
            subject_id=subject_id,
            shutdown_event=shutdown_event,
        )

    async def fetch_sid_page(
        self,
        sid: str,
        pos: datetime.datetime | None,
        *,
        subject_id: object,
        shutdown_event: asyncio.Event,
        attempt_observer: aiohttp_requests.HttpAttemptObserver | None = None,
    ) -> CallsPageEnvelope:
        """Fetch one SID page with an integer timestamp position."""
        if not sid:
            msg = "sid must be a nonempty string"
            raise ValueError(msg)
        timestamp = int(pos.timestamp()) if pos is not None else None
        return await self._fetch_page(
            group_id=None,
            sid=sid,
            pos=timestamp,
            subject_id=subject_id,
            shutdown_event=shutdown_event,
            attempt_observer=attempt_observer,
        )

    async def _fetch_page(
        self,
        *,
        group_id: str | None,
        sid: str | None,
        pos: object | None,
        subject_id: object,
        shutdown_event: asyncio.Event,
        attempt_observer: aiohttp_requests.HttpAttemptObserver | None = None,
    ) -> CallsPageEnvelope:
        """Fetch one page after enforcing selector exclusivity."""
        if (group_id is None) == (sid is None):
            msg = "exactly one of group_id or sid is required"
            raise ValueError(msg)
        params: dict[str, object] = (
            {"groups": group_id} if group_id is not None else {"sid": sid}
        )
        if pos is not None:
            params["pos"] = pos

        token = await self._token_loader(subject_id, shutdown_event)
        if token is None:
            raise _TokenLoadStopped
        headers = {"Authorization": f"Bearer {token}"}
        try:
            if self._legacy_json_fetcher is None:
                evidence = await self._json_evidence_fetcher(
                    self._session,
                    self._live_endpoint_url,
                    headers,
                    params,
                    subject_id,
                    shutdown_event,
                    attempt_observer,
                )
            else:
                evidence = await _adapt_payload_json_fetcher(
                    self._legacy_json_fetcher,
                    self._session,
                    self._live_endpoint_url,
                    headers,
                    params,
                    subject_id,
                    shutdown_event,
                    attempt_observer,
                )
        except FeedFailure as error:
            if (
                error.status_reason
                is feed_store.FeedStatusReason.SYSTEM_AUTHENTICATION_FAILED
            ):
                refreshed = await self._token_loader(
                    subject_id,
                    shutdown_event,
                    force_refresh=True,
                    stale_token=token,
                )
                if refreshed is None:
                    raise _TokenLoadStopped from error
            raise
        return _calls_page_envelope(
            evidence.payload,
            subject_id,
            http_attempt_count=evidence.http_attempt_count,
            response_byte_count=evidence.response_byte_count,
        )

    async def download_audio(
        self,
        audio_url: str,
        *,
        shutdown_event: asyncio.Event,
        out_headers: dict[str, str] | None = None,
        attempt_observer: aiohttp_requests.HttpAttemptObserver | None = None,
    ) -> bytes | ItemFailure:
        """Download one item without taking ownership of the session."""
        if self._legacy_media_downloader is not None:
            return await self._legacy_media_downloader(
                self._session,
                audio_url,
                shutdown_event,
                out_headers,
            )
        return await _download_audio(
            self._session,
            audio_url,
            shutdown_event,
            out_headers,
            attempt_observer=attempt_observer,
        )
