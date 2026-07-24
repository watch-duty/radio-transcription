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

from backend.pipeline.ingestion import models, slo_contract
from backend.pipeline.ingestion.collectors import (
    aiohttp_requests,
    control_flow,
    failure_classification,
)
from backend.pipeline.ingestion.failure_classifiers import http_status
from backend.pipeline.storage import feed_store

if typing.TYPE_CHECKING:
    import datetime

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
    """Validated metadata envelope with raw independently handled items.

    Attributes:
        payload: Validated provider response mapping.
        calls: Raw call items preserved without per-item validation.
        last_pos: Provider cursor value from ``lastPos``, when present.
    """

    payload: collections.abc.Mapping[str, object]
    calls: tuple[object, ...]
    last_pos: object | None


type _TokenFetcher = collections.abc.Callable[[], str]
type _TokenLoader = collections.abc.Callable[..., typing.Awaitable[str | None]]
type _JsonFetcher = collections.abc.Callable[
    [
        aiohttp.ClientSession,
        str,
        dict[str, str],
        dict[str, object],
        asyncio.Event,
    ],
    typing.Awaitable[collections.abc.Mapping[str, object]],
]
type _MediaDownloader = collections.abc.Callable[
    [
        aiohttp.ClientSession,
        str,
        asyncio.Event,
        dict[str, str] | None,
    ],
    typing.Awaitable[bytes | failure_classification.ItemFailure],
]


class TokenLoadStopped(asyncio.CancelledError):
    """Signal that credential loading stopped because shutdown won."""


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
        raise failure_classification.collector_failure(
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
        logger.exception("Failed to access secret %s: %s", name, error)
        raise failure_classification.collector_failure(
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
        is_config_failure = isinstance(error, models.FeedFailure) and (
            error.status_reason
            in (
                feed_store.FeedStatusReason.SYSTEM_CONFIGURATION_INVALID,
                feed_store.FeedStatusReason.SYSTEM_RUNTIME_CONFIGURATION_INVALID,
            )
        )
        if should_log and not is_config_failure:
            logger.warning(
                "Failed to fetch Broadcastify JWT token from Secret Manager",
                exc_info=error,
                extra={
                    "json_fields": {
                        "event_type": (
                            slo_contract.EVENT_TYPE_BCFY_JWT_FETCH_FAILED
                        ),
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
    shutdown_event: asyncio.Event,
    *,
    force_refresh: bool = False,
    stale_token: str | None = None,
    _token_fetcher: _TokenFetcher | None = None,
) -> str | None:
    """Retry transient shared JWT access without releasing authority."""
    failures = 0
    while not shutdown_event.is_set():
        try:
            return await _get_shared_jwt_token(
                force_refresh=force_refresh,
                stale_token=stale_token,
                _token_fetcher=_token_fetcher,
            )
        except models.FeedFailure as error:
            if error.status_reason in (
                feed_store.FeedStatusReason.SYSTEM_CONFIGURATION_INVALID,
                feed_store.FeedStatusReason.SYSTEM_RUNTIME_CONFIGURATION_INVALID,
            ):
                raise
            failure = failure_classification.ItemFailure(
                error.status_reason,
                error.reason,
            )
        except Exception:
            failure = failure_classification.ItemFailure(
                feed_store.FeedStatusReason.SYSTEM_CREDENTIAL_ACCESS_FAILED,
                "calls_jwt_secret_access_failed",
            )

        failures += 1
        if failures >= _JWT_MAX_CONSECUTIVE_FAILURES:
            raise failure_classification.collector_failure(
                failure.status_reason,
                failure.reason,
            )
        await control_flow.sleep_or_cancel(
            shutdown_event,
            _JWT_RETRY_INTERVAL_SEC,
        )
    return None


def _log_calls_api_response_invalid() -> None:
    """Log invalid successful Calls metadata without response contents."""
    logger.error("Invalid Broadcastify Calls API response payload")


def _validate_calls_api_payload(
    payload: object,
) -> collections.abc.Mapping[str, object]:
    """Validate only the Calls metadata envelope shape."""
    if not isinstance(payload, dict):
        _log_calls_api_response_invalid()
        msg = "payload must be an object"
        raise TypeError(msg)
    validated = typing.cast("dict[str, object]", payload)
    calls = validated.get("calls", [])
    if not isinstance(calls, list):
        _log_calls_api_response_invalid()
        msg = "calls field must be a list"
        raise TypeError(msg)
    return validated


async def _fetch_calls(
    session: aiohttp.ClientSession,
    url: str,
    headers: dict[str, str],
    params: dict[str, object],
    shutdown_event: asyncio.Event,
) -> collections.abc.Mapping[str, object]:
    """Fetch one Calls metadata page with the shared request policy."""
    selector_label = next(
        (
            f"{selector}={params[selector]}"
            for selector in ("groups", "sid")
            if selector in params
        ),
        "unknown selector",
    )

    return await aiohttp_requests.fetch_json_with_retries(
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
        log_label=f"Calls API {selector_label}",
        reason_prefix="calls_api_http",
        status_policy=_CALLS_API_HTTP_POLICY,
        validate_payload=_validate_calls_api_payload,
        invalid_payload_status_reason=(
            feed_store.FeedStatusReason.SYSTEM_SOURCE_PAYLOAD_INVALID
        ),
        invalid_payload_reason="calls_api_response_invalid",
        transport_status_reason=(
            feed_store.FeedStatusReason.SOURCE_UNREACHABLE
        ),
        transport_reason="calls_api_http_transport_failed",
    )


async def _download_audio(
    session: aiohttp.ClientSession,
    audio_url: str,
    shutdown_event: asyncio.Event,
    out_headers: dict[str, str] | None = None,
) -> bytes | failure_classification.ItemFailure:
    """Download one Calls item with the shared media retry policy."""
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


def _calls_page_envelope(
    payload: object,
) -> CallsPageEnvelope:
    validated = _validate_calls_api_payload(payload)
    calls = typing.cast("list[object]", validated.get("calls", []))
    return CallsPageEnvelope(
        payload=validated,
        calls=tuple(calls),
        last_pos=validated.get("lastPos"),
    )


class CallsProviderClient:
    """Lightweight Calls client over one runtime-owned HTTP session."""

    def __init__(
        self,
        session: aiohttp.ClientSession,
        live_endpoint_url: str,
        *,
        on_authentication_failure: collections.abc.Callable[[], None]
        | None = None,
        _token_loader: _TokenLoader | None = None,
        _json_fetcher: _JsonFetcher | None = None,
        _media_downloader: _MediaDownloader | None = None,
    ) -> None:
        """Initialize a client over a caller-owned HTTP session.

        Args:
            session: Runtime-owned session used for every provider request.
            live_endpoint_url: Broadcastify Calls live endpoint URL.
            on_authentication_failure: Optional source-context observer invoked
                before a forced credential refresh.
            _token_loader: Internal credential-loader test seam.
            _json_fetcher: Internal metadata-fetch test seam.
            _media_downloader: Internal media-download test seam.
        """
        self._session = session
        self._live_endpoint_url = (
            live_endpoint_url
            if live_endpoint_url.endswith("/")
            else f"{live_endpoint_url}/"
        )
        self._on_authentication_failure = on_authentication_failure
        self._token_loader = _token_loader or _get_shared_jwt_token_with_retry
        self._json_fetcher = _json_fetcher or _fetch_calls
        self._media_downloader = _media_downloader or _download_audio

    async def fetch_group_page(
        self,
        source_feed_id: str,
        pos: object | None,
        *,
        shutdown_event: asyncio.Event,
    ) -> CallsPageEnvelope:
        """Fetch one legacy inclusive groups page.

        Args:
            source_feed_id: Nonempty Broadcastify group selector.
            pos: Inclusive upstream cursor, or ``None`` for the first page.
            shutdown_event: Signals cooperative request cancellation.

        Returns:
            Validated page metadata and raw call items.

        Raises:
            ValueError: If ``source_feed_id`` is empty.
            FeedFailure: If credential or metadata retrieval fails.
            asyncio.CancelledError: If shutdown interrupts the request.
        """
        if not source_feed_id:
            msg = "source_feed_id must be a nonempty string"
            raise ValueError(msg)
        return await self._fetch_page(
            selector_name="groups",
            selector_value=source_feed_id,
            pos=pos,
            shutdown_event=shutdown_event,
        )

    async def fetch_sid_page(
        self,
        sid: str,
        pos: datetime.datetime | None,
        *,
        shutdown_event: asyncio.Event,
    ) -> CallsPageEnvelope:
        """Fetch one SID page with an integer timestamp position.

        Args:
            sid: Nonempty Broadcastify system selector.
            pos: UTC-aware inclusive cursor, or ``None`` for the first page.
            shutdown_event: Signals cooperative request cancellation.

        Returns:
            Validated page metadata and raw call items.

        Raises:
            ValueError: If ``sid`` is empty.
            FeedFailure: If credential or metadata retrieval fails.
            asyncio.CancelledError: If shutdown interrupts the request.
        """
        if not sid:
            msg = "sid must be a nonempty string"
            raise ValueError(msg)
        timestamp = int(pos.timestamp()) if pos is not None else None
        return await self._fetch_page(
            selector_name="sid",
            selector_value=sid,
            pos=timestamp,
            shutdown_event=shutdown_event,
        )

    async def _fetch_page(
        self,
        *,
        selector_name: typing.Literal["groups", "sid"],
        selector_value: str,
        pos: object | None,
        shutdown_event: asyncio.Event,
    ) -> CallsPageEnvelope:
        """Fetch one page for an already-selected provider query mode."""
        params: dict[str, object] = {selector_name: selector_value}
        if pos is not None:
            params["pos"] = pos

        token = await self._token_loader(shutdown_event)
        if token is None:
            raise TokenLoadStopped
        headers = {"Authorization": f"Bearer {token}"}
        try:
            payload = await self._json_fetcher(
                self._session,
                self._live_endpoint_url,
                headers,
                params,
                shutdown_event,
            )
        except models.FeedFailure as error:
            if (
                error.status_reason
                is feed_store.FeedStatusReason.SYSTEM_AUTHENTICATION_FAILED
            ):
                if self._on_authentication_failure is not None:
                    self._on_authentication_failure()
                refreshed = await self._token_loader(
                    shutdown_event,
                    force_refresh=True,
                    stale_token=token,
                )
                if refreshed is None:
                    raise TokenLoadStopped from error
            raise
        return _calls_page_envelope(payload)

    async def download_audio(
        self,
        audio_url: str,
        *,
        shutdown_event: asyncio.Event,
        out_headers: dict[str, str] | None = None,
    ) -> bytes | failure_classification.ItemFailure:
        """Download one item without taking ownership of the session.

        Args:
            audio_url: Provider media URL for one call.
            shutdown_event: Signals cooperative request cancellation.
            out_headers: Optional mapping populated with response headers.

        Returns:
            Downloaded audio bytes or a classified item failure.

        Raises:
            asyncio.CancelledError: If shutdown interrupts the request.
        """
        return await self._media_downloader(
            self._session,
            audio_url,
            shutdown_event,
            out_headers,
        )
