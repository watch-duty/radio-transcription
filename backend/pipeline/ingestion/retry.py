from __future__ import annotations

import asyncio
import dataclasses
import logging
import random
import typing
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

logger = logging.getLogger(__name__)


class LeaseExpiredError(Exception):
    """Raised when a retry loop observes confirmed lease loss."""


class CommittedAwaitableCancelled(BaseException):
    """An already-started committed operation cancelled internally.

    Caller cancellation is intentionally represented on
    :class:`CommittedAwaitableSettlement` instead.  This exception therefore
    means the child operation itself ended cancelled and its external outcome
    cannot be inferred from the awaitable.
    """


@dataclasses.dataclass(frozen=True, slots=True)
class CommittedAwaitableSettlement[ResultT]:
    """Definitive child outcome plus the first deferred cancellation."""

    result: ResultT | None
    failure: BaseException | None = None
    cancellation: asyncio.CancelledError | None = None

    def unwrap(self) -> ResultT:
        """Return the child value or re-raise its exact failure."""
        if self.failure is not None:
            raise self.failure
        return typing.cast("ResultT", self.result)


async def settle_committed_awaitable[ResultT](
    awaitable: typing.Awaitable[ResultT],
) -> CommittedAwaitableSettlement[ResultT]:
    """Wait for an already-started operation despite caller cancellation.

    The first exact ``CancelledError`` object is remembered while the child is
    shielded to a definitive result.  Repeated cancellation cannot replace
    that evidence.  The caller decides how to hand off or retain the result
    before re-raising the remembered exception.
    """
    task = asyncio.ensure_future(awaitable)
    cancellation: asyncio.CancelledError | None = None
    while True:
        try:
            result = await asyncio.shield(task)
        except asyncio.CancelledError as exc:
            if task.done():
                if task.cancelled():
                    message = (
                        "committed operation was cancelled with unknown outcome"
                    )
                    raise CommittedAwaitableCancelled(message) from exc
                try:
                    result = task.result()
                except BaseException as failure:
                    return CommittedAwaitableSettlement(
                        None,
                        failure=failure,
                        cancellation=cancellation or exc,
                    )
                return CommittedAwaitableSettlement(
                    result,
                    cancellation=cancellation or exc,
                )
            if cancellation is None:
                cancellation = exc
            continue
        except BaseException as exc:
            return CommittedAwaitableSettlement(
                None,
                failure=exc,
                cancellation=cancellation,
            )
        return CommittedAwaitableSettlement(
            result,
            cancellation=cancellation,
        )


# TODO: https://linear.app/watchduty/issue/GOO-566/ - Move retry callers to a
# RetryConfig + coroutine-factory API so keyword args and type checking survive.
async def retry_with_lease_check[T](  # noqa: PLR0912
    fn: Callable[..., Awaitable[T]],
    *args: object,
    lease_lost: asyncio.Event,
    shutdown: asyncio.Event,
    max_retries: int = 3,
    base_delay_sec: float = 0.5,
    max_delay_sec: float = 8.0,
    retryable: tuple[type[Exception], ...] = (Exception,),
    operation_name: str = "operation",
    retry_state_changed: Callable[[bool], None] | None = None,
    attempt_observer: Callable[[int], None] | None = None,
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
        retry_state_changed: Optional non-awaiting callback that reports the
            exact bounded-backoff interval.
        attempt_observer: Optional non-awaiting callback invoked immediately
            before each actual call with its one-based attempt number.

    Returns:
        The return value of *fn* on success.

    Raises:
        LeaseExpiredError: If lease_lost is set before or during retry.

    """
    last_exception: Exception | None = None
    retrying = False

    for attempt in range(max_retries + 1):
        if lease_lost.is_set():
            msg = f"Lease lost before {operation_name} attempt {attempt}"
            raise LeaseExpiredError(msg)
        if shutdown.is_set():
            raise asyncio.CancelledError

        if attempt_observer is not None:
            attempt_observer(attempt + 1)
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

            if retry_state_changed is not None:
                retry_state_changed(True)  # noqa: FBT003
            retrying = True

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
                if retrying:
                    if retry_state_changed is not None:
                        retry_state_changed(False)  # noqa: FBT003
                    retrying = False

            if lease_lost.is_set():
                msg = f"Lease lost during {operation_name} backoff"
                raise LeaseExpiredError(msg) from last_exception
            if shutdown.is_set():
                raise asyncio.CancelledError

    # Unreachable — loop always returns or raises — but satisfies type checker.
    msg = f"{operation_name} retry logic reached unreachable state"
    raise RuntimeError(msg)
