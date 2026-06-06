"""Shared aiohttp request helpers for VM collectors."""

from __future__ import annotations

import asyncio
import collections.abc
import dataclasses
import logging
import random
from typing import TYPE_CHECKING, Any, TypeVar, cast

import aiohttp

from backend.pipeline.ingestion import models
from backend.pipeline.ingestion.collectors import failure_classification
from backend.pipeline.ingestion.collectors.failure_classification import (
    ItemFailure,
    collector_failure,
)
from backend.pipeline.ingestion.collectors.failure_classifiers import (
    http_status,
)
from backend.pipeline.storage import feed_store

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Mapping

logger = logging.getLogger(__name__)

_JSON = TypeVar("_JSON")
_RETRYABLE_STATUSES = frozenset({408, 429})


@dataclasses.dataclass(frozen=True)
class DownloadedItem:
    """A successfully downloaded item body and response headers."""

    content: bytes
    headers: Mapping[str, str]


async def sleep_or_shutdown(
    shutdown: asyncio.Event,
    seconds: float,
) -> bool:
    """Sleep for *seconds*, returning ``True`` if interrupted by shutdown."""
    try:
        await asyncio.wait_for(shutdown.wait(), timeout=seconds)
    except TimeoutError:
        return False
    return True


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


def _classification_for_status(
    status: int,
    *,
    reason_prefix: str,
    policy: http_status.HTTPStatusPolicy,
    fallback_status_reason: feed_store.FeedStatusReason,
    fallback_reason: str,
) -> failure_classification.FailureClassification:
    """Classify status or return the caller's bounded fallback."""
    classification = http_status.classify_http_status(
        status,
        reason_prefix=reason_prefix,
        policy=policy,
    )
    if classification is not None:
        return classification
    return failure_classification.FailureClassification(
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
    headers: Mapping[str, str] | None = None,
    params: Mapping[str, Any] | None = None,
    timeout_sec: float,
    max_attempts: int,
    log_label: str,
    reason_prefix: str,
    status_policy: http_status.HTTPStatusPolicy,
    validate_payload: Callable[[object], _JSON],
    invalid_payload_status_reason: feed_store.FeedStatusReason,
    invalid_payload_reason: str,
    transport_status_reason: feed_store.FeedStatusReason,
    transport_reason: str,
    retry_base_delay_sec: float = 1.0,
    retry_jitter_max_sec: float = 0.0,
    sleep_func: Callable[
        [asyncio.Event, float],
        Awaitable[bool],
    ] = sleep_or_shutdown,
) -> _JSON | None:
    """Fetch and validate JSON for a feed-scoped endpoint.

    Returns ``None`` only when shutdown interrupts the request or retry sleep.
    Terminal feed-scoped failures raise ``FeedFailure``.
    """
    timeout = aiohttp.ClientTimeout(total=timeout_sec)
    for attempt in range(max_attempts):
        if shutdown.is_set():
            return None

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
                    ):
                        if attempt < max_attempts - 1:
                            delay = _retry_delay(
                                attempt,
                                base_delay_sec=retry_base_delay_sec,
                                jitter_max_sec=retry_jitter_max_sec,
                            )
                            logger.warning(
                                "%s invalid JSON/payload"
                                " (attempt %d/%d, retry in %.1fs)",
                                log_label,
                                attempt + 1,
                                max_attempts,
                                delay,
                                exc_info=True,
                            )
                            if await sleep_func(shutdown, delay):
                                return None
                            continue
                        raise collector_failure(
                            invalid_payload_status_reason,
                            invalid_payload_reason,
                        )

                if (
                    _is_retryable_status(response.status)
                    and attempt < max_attempts - 1
                ):
                    delay = _retry_delay(
                        attempt,
                        headers=response.headers,
                        base_delay_sec=retry_base_delay_sec,
                        jitter_max_sec=retry_jitter_max_sec,
                    )
                    logger.warning(
                        "%s returned %d (attempt %d/%d, retry in %.1fs)",
                        log_label,
                        response.status,
                        attempt + 1,
                        max_attempts,
                        delay,
                    )
                    if await sleep_func(shutdown, delay):
                        return None
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
        except (aiohttp.ClientError, TimeoutError):
            if attempt < max_attempts - 1:
                delay = _retry_delay(
                    attempt,
                    base_delay_sec=retry_base_delay_sec,
                    jitter_max_sec=retry_jitter_max_sec,
                )
                logger.warning(
                    "%s transport error (attempt %d/%d, retry in %.1fs)",
                    log_label,
                    attempt + 1,
                    max_attempts,
                    delay,
                    exc_info=True,
                )
                if await sleep_func(shutdown, delay):
                    return None
                continue
            raise collector_failure(
                transport_status_reason,
                transport_reason,
            )

    return None


async def download_item_media(  # noqa: PLR0911
    session: aiohttp.ClientSession,
    url: str,
    shutdown: asyncio.Event,
    *,
    timeout_sec: float,
    max_attempts: int,
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
    retry_base_delay_sec: float = 1.0,
    retry_jitter_max_sec: float = 0.0,
    sleep_func: Callable[
        [asyncio.Event, float],
        Awaitable[bool],
    ] = sleep_or_shutdown,
) -> DownloadedItem | ItemFailure | None:
    """Download item media with retry and item-scoped classification.

    Returns ``None`` only for shutdown interruption.
    """
    timeout = aiohttp.ClientTimeout(total=timeout_sec)
    last_status: int | None = None

    for attempt in range(max_attempts):
        if shutdown.is_set():
            return None

        try:
            async with session.get(url, timeout=timeout) as response:
                last_status = response.status
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

                if (
                    _is_retryable_status(response.status)
                    and attempt < max_attempts - 1
                ):
                    delay = _retry_delay(
                        attempt,
                        headers=response.headers,
                        base_delay_sec=retry_base_delay_sec,
                        jitter_max_sec=retry_jitter_max_sec,
                    )
                    logger.warning(
                        "%s returned %d (attempt %d/%d, retry in %.1fs)",
                        log_label,
                        response.status,
                        attempt + 1,
                        max_attempts,
                        delay,
                    )
                    if await sleep_func(shutdown, delay):
                        return None
                    continue

                classification = _classification_for_status(
                    response.status,
                    reason_prefix=reason_prefix,
                    policy=status_policy,
                    fallback_status_reason=fallback_status_reason,
                    fallback_reason=fallback_reason,
                )
                return ItemFailure.from_classification(classification)
        except (aiohttp.ClientError, TimeoutError):
            if attempt < max_attempts - 1:
                delay = _retry_delay(
                    attempt,
                    base_delay_sec=retry_base_delay_sec,
                    jitter_max_sec=retry_jitter_max_sec,
                )
                logger.warning(
                    "%s transport error (attempt %d/%d, retry in %.1fs)",
                    log_label,
                    attempt + 1,
                    max_attempts,
                    delay,
                    exc_info=True,
                )
                if await sleep_func(shutdown, delay):
                    return None
                continue
            return ItemFailure(
                transport_status_reason,
                transport_reason,
            )

    if last_status is None:
        return ItemFailure(transport_status_reason, transport_reason)

    classification = _classification_for_status(
        last_status,
        reason_prefix=reason_prefix,
        policy=status_policy,
        fallback_status_reason=fallback_status_reason,
        fallback_reason=fallback_reason,
    )
    return ItemFailure.from_classification(classification)
