"""Shared collector control-flow helpers."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Awaitable


async def sleep_or_cancel(
    shutdown: asyncio.Event,
    seconds: float,
) -> None:
    """Sleep until timeout, or propagate shutdown as cancellation."""
    try:
        await asyncio.wait_for(shutdown.wait(), timeout=seconds)
    except TimeoutError:
        return

    raise asyncio.CancelledError


async def await_or_cancel[T](
    awaitable: Awaitable[T],
    shutdown: asyncio.Event,
) -> T:
    """Await an operation, cancelling it if shutdown wins.

    If the operation and shutdown complete in the same event-loop turn, a
    successful operation result wins so resource-producing calls can be returned
    to their caller and cleaned up normally.
    """
    operation_task = asyncio.ensure_future(awaitable)
    if shutdown.is_set():
        operation_task.cancel()
        await asyncio.gather(operation_task, return_exceptions=True)
        raise asyncio.CancelledError

    shutdown_task = asyncio.create_task(shutdown.wait())
    tasks = {operation_task, shutdown_task}

    try:
        done, pending = await asyncio.wait(
            tasks,
            return_when=asyncio.FIRST_COMPLETED,
        )
    except asyncio.CancelledError:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        raise

    if pending:
        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)

    if operation_task in done and not operation_task.cancelled():
        if operation_task.exception() is None:
            return operation_task.result()

    if shutdown_task in done and shutdown_task.result():
        raise asyncio.CancelledError

    return await operation_task
