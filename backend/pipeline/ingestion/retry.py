from __future__ import annotations

import asyncio
import logging
import random
from typing import TYPE_CHECKING, TypeVar

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

logger = logging.getLogger(__name__)

T = TypeVar("T")


class LeaseExpiredError(Exception):
    """Raised when a retry loop detects heartbeat loss."""


async def retry_with_lease_check(
    fn: Callable[..., Awaitable[T]],
    *args: object,
    lease_lost: asyncio.Event,
    shutdown: asyncio.Event,
    max_retries: int = 3,
    base_delay_sec: float = 0.5,
    max_delay_sec: float = 8.0,
    retryable: tuple[type[Exception], ...] = (Exception,),
    operation_name: str = "operation",
) -> T:
    """
    Retry an async callable, aborting immediately if the lease is lost.

    Control flow per attempt:
    1. Check lease_lost → raise LeaseExpiredError
    2. Check shutdown → raise CancelledError
    3. Call fn(*args) → return on success
    4. On retryable exception: jittered backoff, racing against both
       lease_lost and shutdown for interruptibility
    5. On non-retryable or max retries exhausted: re-raise original

    Args:
        fn: Async callable to invoke.
        *args: Positional arguments forwarded to *fn*.
        lease_lost: Monotonic event set when heartbeat is uncertain.
        shutdown: Event set on SIGTERM for graceful shutdown.
        max_retries: Maximum number of retry attempts (total calls = max_retries + 1).
        base_delay_sec: Base delay for exponential backoff.
        max_delay_sec: Cap on backoff delay.
        retryable: Exception types eligible for retry.
        operation_name: Label for log messages.

    Returns:
        The return value of *fn* on success.

    Raises:
        LeaseExpiredError: If lease_lost is set before or during retry.

    """
    last_exception: Exception | None = None

    for attempt in range(max_retries + 1):
        if lease_lost.is_set():
            msg = f"Heartbeat lost before {operation_name} attempt {attempt}"
            raise LeaseExpiredError(msg)
        if shutdown.is_set():
            raise asyncio.CancelledError

        try:
            return await fn(*args)
        except Exception as exc:
            last_exception = exc

            if not isinstance(exc, retryable):
                raise

            remaining = max_retries - attempt
            if remaining <= 0:
                logger.warning(
                    "%s failed after %d attempts: %s",
                    operation_name,
                    attempt + 1,
                    exc,
                )
                raise

            delay = random.uniform(  # noqa: S311
                0,
                min(max_delay_sec, base_delay_sec * 2**attempt),
            )
            logger.info(
                "%s attempt %d failed (%s), retrying in %.2fs (%d left)",
                operation_name,
                attempt + 1,
                exc,
                delay,
                remaining,
            )

            # Race backoff against both lease_lost and shutdown to maintain
            # the runtime's SIGTERM-interruptibility invariant.
            lease_task = asyncio.create_task(lease_lost.wait())
            shutdown_task = asyncio.create_task(shutdown.wait())
            try:
                _done, _pending = await asyncio.wait(
                    [lease_task, shutdown_task],
                    timeout=delay,
                    return_when=asyncio.FIRST_COMPLETED,
                )
            finally:
                for t in (lease_task, shutdown_task):
                    t.cancel()
                    try:
                        await t
                    except asyncio.CancelledError:
                        pass

            if lease_lost.is_set():
                msg = f"Heartbeat lost during {operation_name} backoff"
                raise LeaseExpiredError(msg) from last_exception
            if shutdown.is_set():
                raise asyncio.CancelledError

    # Unreachable — loop always returns or raises — but satisfies type checker.
    msg = f"{operation_name} retry logic reached unreachable state"
    raise RuntimeError(msg)
