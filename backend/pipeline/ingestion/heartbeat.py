from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING

from backend.pipeline.storage.feed_store import FeedStore

if TYPE_CHECKING:
    import uuid
    from collections.abc import Callable

    import psycopg

logger = logging.getLogger(__name__)


class HeartbeatMonitor:
    """
    Background async task that periodically renews database heartbeats.

    For every feed registered with this monitor, the heartbeat loop calls
    ``FeedStore.renew_heartbeat`` once per interval.  If the renewal indicates
    a fence violation (another worker stole the lease), the corresponding
    ``asyncio.Task`` is cancelled immediately to prevent split-brain ingestion.

    Args:
        conn_factory: Callable that returns a new ``psycopg.Connection``.
            A fresh connection is created and closed on every loop iteration
            to avoid holding open sessions between heartbeat cycles.
        worker_id: UUID of the worker that owns the leased feeds.
        interval_sec: Seconds to sleep between heartbeat cycles (default 15).

    """

    def __init__(
        self,
        conn_factory: Callable[[], psycopg.Connection],
        worker_id: uuid.UUID,
        interval_sec: float = 15.0,
    ) -> None:
        self._conn_factory = conn_factory
        self._worker_id = worker_id
        self._interval = interval_sec
        self._feeds: dict[uuid.UUID, asyncio.Task] = {}
        self._task: asyncio.Task | None = None

    # -- Public API -------------------------------------------------------

    def register(self, feed_id: uuid.UUID, task: asyncio.Task) -> None:
        """Add a feed and its processing task to the heartbeat registry."""
        self._feeds[feed_id] = task

    def unregister(self, feed_id: uuid.UUID) -> None:
        """Remove a feed from the heartbeat registry."""
        self._feeds.pop(feed_id, None)

    def start(self) -> None:
        """Launch the heartbeat background task on the running event loop."""
        if self._task is not None and not self._task.done():
            msg = "HeartbeatMonitor is already running"
            raise RuntimeError(msg)
        self._task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        """Cancel the heartbeat background task and wait for it to finish."""
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

    # -- Internal ---------------------------------------------------------

    def _renew_all(
        self,
        feed_ids: list[uuid.UUID],
    ) -> dict[uuid.UUID, bool | Exception]:
        """
        Renew heartbeats for *feed_ids* using a scoped DB session.

        This synchronous helper runs inside ``asyncio.to_thread`` so it
        never blocks the event loop.  A single connection is opened for
        the entire batch and closed in a ``finally`` block.
        """
        conn = self._conn_factory()
        results: dict[uuid.UUID, bool | Exception] = {}
        try:
            store = FeedStore(conn)
            for feed_id in feed_ids:
                try:
                    results[feed_id] = store.renew_heartbeat(
                        feed_id,
                        self._worker_id,
                    )
                except Exception as exc:
                    results[feed_id] = exc
        finally:
            conn.close()
        return results

    async def _run(self) -> None:
        """Main heartbeat loop."""
        while True:
            # --- Completed-task cleanup (before renewals) ----------------
            snapshot = dict(self._feeds)
            for feed_id, task in snapshot.items():
                if task.done():
                    self._feeds.pop(feed_id, None)
                    if not task.cancelled():
                        exc = task.exception()
                        if exc is not None:
                            logger.error(
                                "Feed processing task crashed for feed %s",
                                feed_id,
                                exc_info=exc,
                            )

            # --- Renew heartbeats ----------------------------------------
            snapshot = dict(self._feeds)
            feed_ids = list(snapshot.keys())

            if feed_ids:
                try:
                    results = await asyncio.to_thread(
                        self._renew_all,
                        feed_ids,
                    )
                except Exception:
                    logger.exception("Heartbeat renewal batch failed")
                else:
                    for feed_id in feed_ids:
                        result = results.get(feed_id)
                        if isinstance(result, Exception):
                            logger.exception(
                                "Failed to renew heartbeat for feed %s",
                                feed_id,
                                exc_info=result,
                            )
                        elif result is False:
                            logger.warning(
                                "Fence violation for feed %s; cancelling task",
                                feed_id,
                            )
                            stale_task = snapshot[feed_id]
                            stale_task.cancel()
                            # Only evict if the registry still holds the same
                            # task.  A concurrent register() may have already
                            # replaced it with a fresh task for a new lease.
                            if self._feeds.get(feed_id) is stale_task:
                                del self._feeds[feed_id]

            # --- Sleep until next cycle ----------------------------------
            await asyncio.sleep(self._interval)
