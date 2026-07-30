from __future__ import annotations

import asyncio
import collections.abc  # noqa: TC003 - public hints resolve at runtime.
import dataclasses
import logging
import random

logger = logging.getLogger(__name__)


class LeaseExpiredError(Exception):
    """Raised when a retry loop observes confirmed lease loss."""


@dataclasses.dataclass(frozen=True, slots=True)
class IssuedOperationSettlement[ResultT]:
    """Definitive issued-operation outcome and deferred cancellation.

    Exactly one outcome branch applies: ``failure is None`` means ``result``
    contains the operation result, which may itself be ``None``. Otherwise,
    ``failure`` contains the operation failure and ``result`` is ``None``.

    Attributes:
        result: The definitive operation result on success.
        failure: The definitive operation failure, including an operation's
            own cancellation.
        cancellation: The first exact caller cancellation deferred while the
            issued operation settled.
    """

    result: ResultT | None
    failure: BaseException | None
    cancellation: asyncio.CancelledError | None


async def _await_issued_operation[ResultT](
    awaitable: collections.abc.Awaitable[ResultT],
) -> ResultT:
    return await awaitable


async def settle_issued_operation[ResultT](
    awaitable: collections.abc.Awaitable[ResultT],
) -> IssuedOperationSettlement[ResultT]:
    """Settle issued async work without losing its definitive outcome.

    Caller cancellation is deferred while the child task settles. Repeated
    cancellation requests do not reach the child, and the first exact
    ``CancelledError`` remains available for the caller to propagate after it
    applies the operation outcome. A cancellation originating in the child is
    returned as the operation failure, not mistaken for caller cancellation.

    Args:
        awaitable: Issued async work whose outcome must become definitive.

    Returns:
        The definitive result or failure plus any deferred caller
        cancellation.
    """
    operation = asyncio.create_task(_await_issued_operation(awaitable))
    cancellation: asyncio.CancelledError | None = None

    while not operation.done():
        try:
            await asyncio.wait((operation,))
        except asyncio.CancelledError as error:
            if cancellation is None:
                cancellation = error

    try:
        result = operation.result()
    except BaseException as error:
        return IssuedOperationSettlement(
            result=None,
            failure=error,
            cancellation=cancellation,
        )
    return IssuedOperationSettlement(
        result=result,
        failure=None,
        cancellation=cancellation,
    )


# TODO: https://linear.app/watchduty/issue/GOO-566/ - Move retry callers to a
# RetryConfig + coroutine-factory API so keyword args and type checking survive.
async def retry_with_lease_check[T](
    fn: collections.abc.Callable[..., collections.abc.Awaitable[T]],
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
        lease_lost: Monotonic event set when lease loss is confirmed.
        shutdown: Event set on SIGTERM for graceful shutdown.
        max_retries: Maximum number of retry attempts (total calls =
            max_retries + 1).
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
            msg = f"Lease lost before {operation_name} attempt {attempt}"
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
                await asyncio.gather(
                    lease_task,
                    shutdown_task,
                    return_exceptions=True,
                )

            if lease_lost.is_set():
                msg = f"Lease lost during {operation_name} backoff"
                raise LeaseExpiredError(msg) from last_exception
            if shutdown.is_set():
                raise asyncio.CancelledError

    # Unreachable — loop always returns or raises — but satisfies type checker.
    msg = f"{operation_name} retry logic reached unreachable state"
    raise RuntimeError(msg)
