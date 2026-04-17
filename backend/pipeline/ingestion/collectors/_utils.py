"""Shared async utilities for ingestion collectors."""

from __future__ import annotations

import asyncio


async def _sleep_or_shutdown(
    shutdown: asyncio.Event, seconds: float
) -> bool:
    """Sleep for *seconds*, returning ``True`` if interrupted by shutdown."""
    try:
        await asyncio.wait_for(shutdown.wait(), timeout=seconds)
    except TimeoutError:
        return False
    return True
