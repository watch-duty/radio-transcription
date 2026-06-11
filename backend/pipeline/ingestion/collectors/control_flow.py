"""Shared collector control-flow helpers."""

from __future__ import annotations

import asyncio


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
