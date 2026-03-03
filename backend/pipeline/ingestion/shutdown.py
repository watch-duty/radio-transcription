"""Graceful shutdown signal handling for asyncio-based ingestion workers."""

from __future__ import annotations

import asyncio
import logging
import signal

logger = logging.getLogger(__name__)


def setup_shutdown_signals(loop: asyncio.AbstractEventLoop) -> asyncio.Event:
    """
    Register SIGTERM/SIGINT handlers that set an :class:`asyncio.Event`.

    The returned event can be awaited by the leasing loop to trigger a
    graceful drain of in-flight work before the process exits.

    Args:
        loop: The running event loop on which to install signal handlers.

    Returns:
        An :class:`asyncio.Event` that is set when a shutdown signal arrives.

    """
    event = asyncio.Event()

    def handler(sig: signal.Signals) -> None:
        if event.is_set():
            return
        logger.info("Received %s, initiating graceful shutdown", sig.name)
        event.set()

    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, handler, sig)

    return event
