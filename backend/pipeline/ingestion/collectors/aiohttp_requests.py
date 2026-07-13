"""Shared aiohttp request helpers for VM collectors."""

from __future__ import annotations

import asyncio
import collections.abc
import dataclasses
import enum
import json
import logging
import random
import re
from collections.abc import Awaitable, Callable, Mapping
from typing import Any, TypeVar, cast

import aiohttp

from backend.pipeline.ingestion import models, status_reason_detail
from backend.pipeline.ingestion.collectors import control_flow
from backend.pipeline.ingestion.collectors.failure_classification import (
    ItemFailure,
    collector_failure,
)
from backend.pipeline.ingestion.failure_classifiers import (
    http_status,
)
from backend.pipeline.storage import feed_store

logger = logging.getLogger(__name__)

_JSON = TypeVar("_JSON")
type SleepFunc = Callable[[asyncio.Event, float], Awaitable[None]]


class HttpAttemptKind(enum.StrEnum):
    """Closed operation kinds exposed to HTTP attempt observers."""

    JSON = "json"
    MEDIA = "media"


type HttpAttemptObserver = Callable[[HttpAttemptKind, int], None]


@dataclasses.dataclass(frozen=True, slots=True)
class JsonFetchEvidence[PayloadT]:
    """Validated JSON plus transport-owned scalar success evidence."""

    payload: PayloadT = dataclasses.field(repr=False)
    http_attempt_count: int
    response_byte_count: int

    def __post_init__(self) -> None:
        _require_nonnegative_int(
            self.http_attempt_count,
            name="http_attempt_count",
        )
        _require_nonnegative_int(
            self.response_byte_count,
            name="response_byte_count",
        )


_JSON_CONTENT_TYPE_RE = re.compile(r"^application/(?:[\w.+-]+?\+)?json")


def _require_nonnegative_int(value: object, *, name: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        msg = f"{name} must be a nonnegative integer"
        raise ValueError(msg)


def _observe_http_attempt(
    observer: HttpAttemptObserver | None,
    kind: HttpAttemptKind,
    ordinal: int,
) -> None:
    """Notify an invocation-local observer without changing HTTP behavior."""
    if observer is None:
        return
    try:
        observer(kind, ordinal)
    except (KeyboardInterrupt, SystemExit):
        raise
    except BaseException:
        # This is an isolation boundary. Observers cannot fail or cancel the
        # request owner, and their exception detail must not reach logs.
        return


@dataclasses.dataclass(frozen=True)
class DownloadedItem:
    """A successfully downloaded item body and response headers."""

    content: bytes
    headers: Mapping[str, str]


@dataclasses.dataclass(frozen=True)
class RetryConfig:
    """Mechanical retry settings shared by aiohttp request helpers."""

    timeout_sec: float
    max_attempts: int
    base_delay_sec: float = 1.0
    jitter_max_sec: float = 1.0
    sleep_func: SleepFunc = control_flow.sleep_or_cancel


def _validate_retry_config(retry_config: RetryConfig) -> None:
    if retry_config.max_attempts < 1:
        msg = "retry_config.max_attempts must be >= 1"
        raise ValueError(msg)


def _retry_delay(
    attempt: int,
    *,
    headers: object = None,
    base_delay_sec: float,
    jitter_max_sec: float,
) -> float:
    """Return retry delay, honoring a numeric Retry-After header."""
    retry_after = None
    if isinstance(headers, collections.abc.Mapping):
        header_map = cast("Mapping[object, object]", headers)
        retry_after = header_map.get("Retry-After")
    retry_after_text = (
        str(retry_after).strip() if retry_after is not None else ""
    )
    if retry_after_text.isdigit():
        return float(retry_after_text)

    return (base_delay_sec * (2**attempt)) + random.uniform(  # noqa: S311
        0,
        jitter_max_sec,
    )


def _is_retryable_status(status: int) -> bool:
    """Return whether HTTP status should be retried before classification."""
    return http_status.is_retryable_http_status(status)


def _has_attempt_remaining(attempt: int, retry_config: RetryConfig) -> bool:
    return attempt < retry_config.max_attempts - 1


async def _sleep_for_retry(
    shutdown: asyncio.Event,
    retry_config: RetryConfig,
    *,
    attempt: int,
    log_label: str,
    message: str,
    headers: object = None,
) -> None:
    delay = _retry_delay(
        attempt,
        headers=headers,
        base_delay_sec=retry_config.base_delay_sec,
        jitter_max_sec=retry_config.jitter_max_sec,
    )
    logger.warning(
        "%s %s (attempt %d/%d, retry in %.1fs)",
        log_label,
        message,
        attempt + 1,
        retry_config.max_attempts,
        delay,
    )
    await retry_config.sleep_func(shutdown, delay)


def _headers_dict(headers: object) -> dict[str, str]:
    """Return a plain lower-case single-value header dict.

    Multi-value headers are intentionally not preserved; callers currently use
    this only for single-value response metadata such as ``content-type``.
    Keys are normalized to lower-case because plain ``dict`` does not preserve
    aiohttp's case-insensitive header lookup semantics.
    """
    if not isinstance(headers, collections.abc.Mapping):
        return {}
    header_map = cast("Mapping[object, object]", headers)
    return {
        key.lower(): value
        for key, value in header_map.items()
        if isinstance(key, str) and isinstance(value, str)
    }


def _decode_json_body(
    response: aiohttp.ClientResponse,
    body: bytes,
) -> object:
    """Decode captured bytes with ``ClientResponse.json`` compatibility."""
    content_type = response.headers.get("Content-Type", "").lower()
    if _JSON_CONTENT_TYPE_RE.match(content_type) is None:
        raise aiohttp.ContentTypeError(
            response.request_info,
            response.history,
            status=response.status,
            message=(
                "Attempt to decode JSON with unexpected mimetype: "
                f"{content_type}"
            ),
            headers=response.headers,
        )

    stripped = body.strip()
    if not stripped:
        return None
    return json.loads(stripped.decode(response.get_encoding()))


async def _drain_error_response(response: aiohttp.ClientResponse) -> None:
    """Best-effort drain without replacing an already-observed HTTP status."""
    try:
        await response.read()
    except (aiohttp.ClientError, TimeoutError):
        # The response context will discard an unreadable body. Status and
        # headers remain authoritative for retry and failure classification.
        return


async def fetch_json_with_retries_evidence(  # noqa: UP047
    session: aiohttp.ClientSession,
    url: str,
    shutdown: asyncio.Event,
    *,
    retry_config: RetryConfig,
    headers: Mapping[str, str] | None = None,
    params: Mapping[str, Any] | None = None,
    log_label: str,
    reason_prefix: str,
    status_policy: http_status.HTTPStatusPolicy,
    validate_payload: Callable[[object], _JSON],
    invalid_payload_status_reason: feed_store.FeedStatusReason,
    invalid_payload_reason: str,
    transport_status_reason: feed_store.FeedStatusReason,
    transport_reason: str,
    attempt_observer: HttpAttemptObserver | None = None,
) -> JsonFetchEvidence[_JSON]:
    """Fetch validated JSON with actual attempts and accepted body bytes.

    Shutdown interruption raises ``asyncio.CancelledError``.
    Terminal feed-scoped failures raise ``FeedFailure``.
    """
    _validate_retry_config(retry_config)
    timeout = aiohttp.ClientTimeout(total=retry_config.timeout_sec)
    for attempt in range(retry_config.max_attempts):
        if shutdown.is_set():
            raise asyncio.CancelledError

        ordinal = attempt + 1
        _observe_http_attempt(
            attempt_observer,
            HttpAttemptKind.JSON,
            ordinal,
        )
        try:
            async with session.get(
                url,
                headers=headers,
                params=params,
                timeout=timeout,
            ) as response:
                if response.status == 200:
                    try:
                        body = await response.read()
                        payload = _decode_json_body(response, body)
                        validated = validate_payload(payload)
                        return JsonFetchEvidence(
                            payload=validated,
                            http_attempt_count=ordinal,
                            response_byte_count=len(body),
                        )
                    except (
                        aiohttp.ContentTypeError,
                        TypeError,
                        ValueError,
                    ) as exc:
                        raise collector_failure(
                            invalid_payload_status_reason,
                            f"{invalid_payload_reason}: "
                            f"{status_reason_detail.exception_text(exc)}",
                        ) from exc

                await _drain_error_response(response)
                if _is_retryable_status(
                    response.status
                ) and _has_attempt_remaining(attempt, retry_config):
                    await _sleep_for_retry(
                        shutdown,
                        retry_config,
                        attempt=attempt,
                        log_label=log_label,
                        message=f"returned {response.status}",
                        headers=response.headers,
                    )
                    continue

                status_reason = http_status.classify_http_status(
                    response.status,
                    policy=status_policy,
                )
                raise collector_failure(
                    status_reason
                    or feed_store.FeedStatusReason.SYSTEM_COLLECTOR_ERROR,
                    f"{reason_prefix}_{response.status}",
                )
        except models.FeedFailure:
            raise
        except (aiohttp.ClientError, TimeoutError) as exc:
            if _has_attempt_remaining(attempt, retry_config):
                await _sleep_for_retry(
                    shutdown,
                    retry_config,
                    attempt=attempt,
                    log_label=log_label,
                    message=(
                        "transport error: "
                        f"{status_reason_detail.exception_text(exc)}"
                    ),
                )
                continue
            raise collector_failure(
                transport_status_reason,
                f"{transport_reason}: {status_reason_detail.exception_text(exc)}",
            ) from exc

    msg = "unreachable retry loop exit"
    raise RuntimeError(msg)


async def fetch_json_with_retries(  # noqa: UP047
    session: aiohttp.ClientSession,
    url: str,
    shutdown: asyncio.Event,
    *,
    retry_config: RetryConfig,
    headers: Mapping[str, str] | None = None,
    params: Mapping[str, Any] | None = None,
    log_label: str,
    reason_prefix: str,
    status_policy: http_status.HTTPStatusPolicy,
    validate_payload: Callable[[object], _JSON],
    invalid_payload_status_reason: feed_store.FeedStatusReason,
    invalid_payload_reason: str,
    transport_status_reason: feed_store.FeedStatusReason,
    transport_reason: str,
    attempt_observer: HttpAttemptObserver | None = None,
) -> _JSON:
    """Fetch validated JSON through the payload-only compatibility API."""
    evidence = await fetch_json_with_retries_evidence(
        session,
        url,
        shutdown,
        retry_config=retry_config,
        headers=headers,
        params=params,
        log_label=log_label,
        reason_prefix=reason_prefix,
        status_policy=status_policy,
        validate_payload=validate_payload,
        invalid_payload_status_reason=invalid_payload_status_reason,
        invalid_payload_reason=invalid_payload_reason,
        transport_status_reason=transport_status_reason,
        transport_reason=transport_reason,
        attempt_observer=attempt_observer,
    )
    return evidence.payload


async def download_item_media(
    session: aiohttp.ClientSession,
    url: str,
    shutdown: asyncio.Event,
    *,
    retry_config: RetryConfig,
    log_label: str,
    reason_prefix: str = "item_http",
    status_policy: http_status.HTTPStatusPolicy = (
        http_status.DEFAULT_HTTP_STATUS_POLICY
    ),
    fallback_status_reason: feed_store.FeedStatusReason = (
        feed_store.FeedStatusReason.SYSTEM_COLLECTOR_ERROR
    ),
    fallback_reason: str = "item_download_failed",
    transport_status_reason: feed_store.FeedStatusReason = (
        feed_store.FeedStatusReason.SOURCE_UNREACHABLE
    ),
    transport_reason: str = "item_download_failed",
    attempt_observer: HttpAttemptObserver | None = None,
) -> DownloadedItem | ItemFailure:
    """Download item media with retry and item-scoped classification.

    Shutdown interruption raises ``asyncio.CancelledError``.
    """
    _validate_retry_config(retry_config)
    timeout = aiohttp.ClientTimeout(total=retry_config.timeout_sec)

    for attempt in range(retry_config.max_attempts):
        if shutdown.is_set():
            raise asyncio.CancelledError

        _observe_http_attempt(
            attempt_observer,
            HttpAttemptKind.MEDIA,
            attempt + 1,
        )
        try:
            async with session.get(url, timeout=timeout) as response:
                if response.status == 200:
                    content = await response.read()
                    if not content:
                        return ItemFailure(
                            fallback_status_reason,
                            fallback_reason,
                        )
                    return DownloadedItem(
                        content=content,
                        headers=_headers_dict(
                            getattr(response, "headers", None)
                        ),
                    )

                await _drain_error_response(response)
                if _is_retryable_status(
                    response.status
                ) and _has_attempt_remaining(attempt, retry_config):
                    await _sleep_for_retry(
                        shutdown,
                        retry_config,
                        attempt=attempt,
                        log_label=log_label,
                        message=f"returned {response.status}",
                        headers=response.headers,
                    )
                    continue

                status_reason = http_status.classify_http_status(
                    response.status,
                    policy=status_policy,
                )
                return ItemFailure(
                    status_reason or fallback_status_reason,
                    f"{reason_prefix}_{response.status}",
                )
        except (aiohttp.ClientError, TimeoutError) as exc:
            if _has_attempt_remaining(attempt, retry_config):
                await _sleep_for_retry(
                    shutdown,
                    retry_config,
                    attempt=attempt,
                    log_label=log_label,
                    message=(
                        "transport error: "
                        f"{status_reason_detail.exception_text(exc)}"
                    ),
                )
                continue
            return ItemFailure(
                transport_status_reason,
                f"{transport_reason}: {status_reason_detail.exception_text(exc)}",
            )

    msg = "unreachable retry loop exit"
    raise RuntimeError(msg)
