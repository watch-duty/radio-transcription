"""Tenacity-based HTTP retry helper for ingestion collectors.

Usage::

    from backend.pipeline.ingestion.collectors._retry import (
        _ServerError,
        retry_http_op,
    )

    async def _attempt() -> bytes | None:
        async with session.get(url) as resp:
            if 500 <= resp.status <= 599:
                raise _ServerError(f"5XX {resp.status}")
            return await resp.read()

    result = await retry_http_op(_attempt, shutdown_event)
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from tenacity import (
    AsyncRetrying,
    RetryCallState,
    RetryError,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
    wait_random,
)

from backend.pipeline.ingestion.collectors import _utils

if TYPE_CHECKING:
    import asyncio
    from collections.abc import Awaitable, Callable

logger = logging.getLogger(__name__)


class _ServerError(Exception):
    """Raised inside an operation to signal a retryable server-side error."""


class _ShutdownInterrupt(BaseException):
    """Raised during tenacity sleep to signal graceful shutdown."""


async def retry_http_op[T](
    fn: Callable[[], Awaitable[T | None]],
    shutdown: asyncio.Event,
    *,
    max_retries: int = 3,
    backoff_base_sec: float = 1.0,
    backoff_cap_sec: float = 60.0,
    jitter: float = 0.0,
    operation_name: str = "operation",
) -> T | None:
    """Retry an async HTTP operation with exponential backoff.

    Calls ``fn()`` repeatedly until it returns a value, retries are
    exhausted, or ``shutdown`` fires.  ``fn`` signals retryable failures by
    raising :class:`_ServerError`; any other exception propagates
    immediately without retry.

    Args:
        fn: Zero-argument async callable returning ``T`` or ``None``.
            Return ``None`` to signal a non-retryable silent failure.
            Raise :class:`_ServerError` for retryable failures.
            Raise any other exception for non-retryable propagation.
        shutdown: Fires when a graceful shutdown is requested.
        max_retries: Maximum number of retry attempts
            (total calls = ``max_retries + 1``).
        backoff_base_sec: Base delay for exponential backoff (seconds).
        backoff_cap_sec: Maximum backoff delay cap (seconds).
        jitter: Maximum extra seconds of random jitter per delay.
        operation_name: Label used in log messages.

    Returns:
        The return value of ``fn`` on success, or ``None`` on retry
        exhaustion or shutdown interruption.

    Raises:
        Any exception raised by ``fn`` that is not :class:`_ServerError`.
    """

    async def _sleep(seconds: float) -> None:
        interrupted = await _utils._sleep_or_shutdown(  # noqa: SLF001
            shutdown, seconds
        )
        if interrupted:
            raise _ShutdownInterrupt

    def _log_retry(retry_state: RetryCallState) -> None:
        exc = retry_state.outcome.exception() if retry_state.outcome else None
        sleep_secs = (
            retry_state.next_action.sleep
            if retry_state.next_action
            else 0.0
        )
        remaining = max_retries - retry_state.attempt_number
        logger.warning(
            "%s attempt %d failed (%s), retrying in %.2fs (%d left)",
            operation_name,
            retry_state.attempt_number,
            exc,
            sleep_secs,
            remaining,
        )

    wait = wait_exponential(
        multiplier=backoff_base_sec,
        min=backoff_base_sec,
        max=backoff_cap_sec,
    )
    if jitter > 0:
        wait = wait + wait_random(0, jitter)

    retryer = AsyncRetrying(
        stop=stop_after_attempt(max_retries + 1),
        wait=wait,
        retry=retry_if_exception_type(_ServerError),
        sleep=_sleep,
        before_sleep=_log_retry,
        reraise=False,
    )

    try:
        return await retryer(fn)
    except _ShutdownInterrupt:
        logger.debug("%s interrupted by shutdown", operation_name)
        return None
    except RetryError:
        logger.warning(
            "%s failed after %d attempts",
            operation_name,
            max_retries + 1,
        )
        return None
