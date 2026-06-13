"""Shared aiohttp request helpers for VM collectors."""

from __future__ import annotations

import asyncio
import collections.abc
import dataclasses
import logging
import random
from collections.abc import Awaitable, Callable, Mapping
from typing import Any, TypeVar, cast

import aiohttp

from backend.pipeline.ingestion import models
from backend.pipeline.ingestion.collectors import (
    control_flow,
    failure_classification,
)
from backend.pipeline.ingestion.collectors.failure_classification import (
    ItemFailure,
    collector_failure,
    format_exception_context,
)
from backend.pipeline.ingestion.collectors.failure_classifiers import (
    http_status,
)
from backend.pipeline.storage import feed_store

logger = logging.getLogger(__name__)

_JSON = TypeVar("_JSON")
_RETRYABLE_STATUSES = frozenset({408, 429})
type SleepFunc = Callable[[asyncio.Event, float], Awaitable[None]]


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
    retry_after_text = str(retry_after) if retry_after is not None else ""
    if retry_after_text.isdigit():
        return float(retry_after_text)

    return (base_delay_sec * (2**attempt)) + random.uniform(  # noqa: S311
        0,
        jitter_max_sec,
    )


def _is_retryable_status(status: int) -> bool:
    """Return whether HTTP status should be retried before classification."""
    return status in _RETRYABLE_STATUSES or 500 <= status <= 599


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


def _classification_for_status(
    status: int,
    *,
    reason_prefix: str,
    policy: http_status.HTTPStatusPolicy,
    fallback_status_reason: feed_store.FeedStatusReason,
    fallback_reason: str,
) -> failure_classification.FailureInfo:
    """Classify status or return the caller's bounded fallback."""
    status_reason = http_status.classify_http_status(
        status,
        policy=policy,
    )
    if status_reason is not None:
        return failure_classification.FailureInfo(
            status_reason,
            f"{reason_prefix}_{status}",
        )
    return failure_classification.FailureInfo(
        fallback_status_reason,
        fallback_reason,
    )


def _headers_dict(headers: object) -> dict[str, str]:
    """Return a plain header dict from aiohttp headers or sparse test fakes."""
    if not isinstance(headers, collections.abc.Mapping):
        return {}
    header_map = cast("Mapping[object, object]", headers)
    return {
        key: value
        for key, value in header_map.items()
        if isinstance(key, str) and isinstance(value, str)
    }


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
) -> _JSON:
    """Fetch and validate JSON for a feed-scoped endpoint.

    Shutdown interruption raises ``asyncio.CancelledError``.
    Terminal feed-scoped failures raise ``FeedFailure``.
    """
    _validate_retry_config(retry_config)
    timeout = aiohttp.ClientTimeout(total=retry_config.timeout_sec)
    for attempt in range(retry_config.max_attempts):
        if shutdown.is_set():
            raise asyncio.CancelledError

        try:
            async with session.get(
                url,
                headers=headers,
                params=params,
                timeout=timeout,
            ) as response:
                if response.status == 200:
                    try:
                        payload = await response.json()
                        return validate_payload(payload)
                    except (
                        aiohttp.ContentTypeError,
                        TypeError,
                        ValueError,
                    ) as exc:
                        raise collector_failure(
                            invalid_payload_status_reason,
                            format_exception_context(
                                invalid_payload_reason,
                                exc,
                            ),
                        )

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

                classification = _classification_for_status(
                    response.status,
                    reason_prefix=reason_prefix,
                    policy=status_policy,
                    fallback_status_reason=(
                        feed_store.FeedStatusReason.SYSTEM_COLLECTOR_ERROR
                    ),
                    fallback_reason=f"{reason_prefix}_{response.status}",
                )
                raise collector_failure(
                    classification.status_reason,
                    classification.reason,
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
                    message=format_exception_context("transport error", exc),
                )
                continue
            raise collector_failure(
                transport_status_reason,
                format_exception_context(transport_reason, exc),
            )

    msg = "unreachable retry loop exit"
    raise RuntimeError(msg)


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
) -> DownloadedItem | ItemFailure:
    """Download item media with retry and item-scoped classification.

    Shutdown interruption raises ``asyncio.CancelledError``.
    """
    _validate_retry_config(retry_config)
    timeout = aiohttp.ClientTimeout(total=retry_config.timeout_sec)

    for attempt in range(retry_config.max_attempts):
        if shutdown.is_set():
            raise asyncio.CancelledError

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

                classification = _classification_for_status(
                    response.status,
                    reason_prefix=reason_prefix,
                    policy=status_policy,
                    fallback_status_reason=fallback_status_reason,
                    fallback_reason=f"{reason_prefix}_{response.status}",
                )
                return ItemFailure.from_info(classification)
        except (aiohttp.ClientError, TimeoutError) as exc:
            if _has_attempt_remaining(attempt, retry_config):
                await _sleep_for_retry(
                    shutdown,
                    retry_config,
                    attempt=attempt,
                    log_label=log_label,
                    message=format_exception_context("transport error", exc),
                )
                continue
            return ItemFailure(
                transport_status_reason,
                format_exception_context(transport_reason, exc),
            )

    msg = "unreachable retry loop exit"
    raise RuntimeError(msg)
