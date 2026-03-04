from __future__ import annotations

import asyncio
import concurrent.futures
import logging
import os
import signal
import threading
import time
from typing import TYPE_CHECKING

import asyncpg

from backend.pipeline.ingestion.gcs import close_client, upload_audio
from backend.pipeline.storage.connection import close_pool, create_pool
from backend.pipeline.storage.feed_store import FeedStore

if TYPE_CHECKING:
    import uuid
    from collections.abc import AsyncIterator, Callable

    from backend.pipeline.ingestion.settings import NormalizerSettings
    from backend.pipeline.storage.feed_store import LeasedFeed

    CaptureFn = Callable[[LeasedFeed, asyncio.Event], AsyncIterator[bytes]]

logger = logging.getLogger(__name__)


class NormalizerRuntime:
    """
    Generic runtime orchestrator for a fleet of async feed-processing tasks.

    Manages the full lifecycle of feed ingestion: batch-leasing feeds from
    the database, spawning an asyncio Task per feed, renewing heartbeats via
    an OS daemon thread (immune to event loop starvation), and detecting fence
    violations (immediate ``os._exit(1)`` on any lost lease).

    Normalizer authors provide a single ``capture_fn`` async generator that
    yields audio chunks. The runtime handles everything else.

    Args:
        capture_fn: Async generator factory ``(feed, shutdown_event) -> AsyncIterator[bytes]``.
        settings: Runtime configuration. Defaults to ``NormalizerSettings()``.

    """

    def __init__(
        self,
        capture_fn: CaptureFn,
        settings: NormalizerSettings | None = None,
    ) -> None:
        if settings is None:
            from backend.pipeline.ingestion.settings import NormalizerSettings  # noqa: I001, PLC0415

            settings = NormalizerSettings()
        self._capture_fn = capture_fn
        self._settings = settings
        self._thread_stop = threading.Event()
        self._shutdown: asyncio.Event | None = None
        self._pool: asyncpg.Pool | None = None
        self._heartbeat_pool: asyncpg.Pool | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._feed_tasks: dict[uuid.UUID, asyncio.Task] = {}
        self._releasing_feeds: set[uuid.UUID] = set()
        self._heartbeat_thread: threading.Thread | None = None
        self._store: FeedStore | None = None
        self._heartbeat_store: FeedStore | None = None

    # -- Entry point ------------------------------------------------------

    def run(self) -> None:
        """
        Start the runtime. Blocks until shutdown completes.

        Sets up logging, then delegates to the async ``_main`` coroutine.
        """
        logging.basicConfig(
            format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
            level=logging.INFO,
        )
        logger.info(
            "Starting NormalizerRuntime worker_id=%s max_feeds=%d",
            self._settings.worker_id,
            self._settings.max_feeds_per_worker,
        )
        asyncio.run(self._main())

    # -- Async core -------------------------------------------------------

    async def _main(self) -> None:
        """Top-level async entry: setup, run leasing loop, then shutdown."""
        self._loop = asyncio.get_running_loop()
        self._shutdown = asyncio.Event()

        def _on_signal(sig: signal.Signals) -> None:
            if not self._shutdown.is_set():
                logger.info(
                    "Received %s -- initiating graceful shutdown", sig.name,
                )
                self._shutdown.set()
                self._thread_stop.set()

        for sig in (signal.SIGTERM, signal.SIGINT):
            self._loop.add_signal_handler(sig, _on_signal, sig)

        s = self._settings
        self._pool = await create_pool(
            host=s.db_host,
            user=s.db_user,
            db_name=s.db_name,
            password=s.db_password,
            port=s.db_port,
            min_size=s.pool_min_size,
            max_size=s.pool_max_size,
            command_timeout=s.db_command_timeout_sec,
            timeout=s.db_connect_timeout_sec,
        )
        self._store = FeedStore(self._pool)

        self._heartbeat_pool = await asyncpg.create_pool(
            host=s.db_host,
            port=s.db_port,
            user=s.db_user,
            password=s.db_password,
            database=s.db_name,
            min_size=1,
            max_size=1,
            command_timeout=s.db_command_timeout_sec,
            timeout=s.db_connect_timeout_sec,
        )
        self._heartbeat_store = FeedStore(self._heartbeat_pool)

        self._heartbeat_thread = threading.Thread(
            target=self._heartbeat_loop,
            daemon=True,
            name="heartbeat",
        )
        self._heartbeat_thread.start()

        try:
            await self._leasing_loop()
        finally:
            await self._shutdown_sequence()

    async def _sleep_or_shutdown(self, seconds: float) -> bool:
        """
        Wait for *seconds* or until shutdown is signalled.

        Returns:
            ``True`` if shutdown was signalled, ``False`` if the timeout
            elapsed normally.

        """
        try:
            await asyncio.wait_for(self._shutdown.wait(), timeout=seconds)
        except TimeoutError:
            return False
        return True

    # -- Leasing ----------------------------------------------------------

    async def _leasing_loop(self) -> None:
        """Continuously lease feeds in batches and spawn processing tasks."""
        while True:
            self._reap_completed_tasks()

            try:
                capacity = (
                    self._settings.max_feeds_per_worker - len(self._feed_tasks)
                )
                if capacity > 0:
                    leases = await self._store.acquire_feeds_batch(
                        self._settings.worker_id,
                        self._settings.abandonment_window_sec,
                        limit=capacity,
                    )
                    for lease in leases:
                        task = asyncio.create_task(
                            self._process_feed(lease),
                            name=f"feed-{lease['name']}",
                        )
                        self._feed_tasks[lease["id"]] = task
                    if leases:
                        logger.info(
                            "Acquired %d feeds (%d/%d active)",
                            len(leases),
                            len(self._feed_tasks),
                            self._settings.max_feeds_per_worker,
                        )
            except Exception:
                logger.exception(
                    "Lease acquisition failed -- will retry next cycle",
                )

            if await self._sleep_or_shutdown(
                self._settings.lease_poll_interval_sec,
            ):
                return

    def _reap_completed_tasks(self) -> None:
        """Remove completed tasks and consume their exceptions."""
        for feed_id in [
            fid for fid, t in self._feed_tasks.items() if t.done()
        ]:
            task = self._feed_tasks.pop(feed_id)
            try:
                exc = task.exception()
            except asyncio.CancelledError:
                pass  # normal — task was cancelled by shutdown
            else:
                if exc is not None:
                    logger.warning(
                        "Feed task %s failed: %s", task.get_name(), exc,
                    )

    # -- Per-feed pipeline ------------------------------------------------

    async def _process_feed(self, feed: LeasedFeed) -> None:
        """
        Run the capture-upload-bookmark pipeline for a single feed.

        Iterates the capture generator, uploads each chunk to GCS, and
        bookmarks progress with fence violation detection.
        """
        chunk_seq = 0
        worker_id = self._settings.worker_id

        try:
            async for audio_chunk in self._capture_fn(
                feed, self._shutdown,
            ):
                gcs_path = await upload_audio(
                    audio_chunk,
                    feed,
                    self._settings.final_staging_bucket,
                    chunk_seq,
                )
                chunk_seq += 1

                ok = await self._store.update_feed_progress(
                    feed["id"], worker_id, gcs_path,
                )
                if not ok:
                    logger.critical(
                        "Fence violation on bookmark for feed %s "
                        "-- terminating",
                        feed["name"],
                    )
                    logging.shutdown()
                    os._exit(1)

                if self._shutdown.is_set():
                    logger.info(
                        "Shutdown -- stopping feed %s cleanly after "
                        "chunk %d",
                        feed["name"],
                        chunk_seq,
                    )
                    return

        except asyncio.CancelledError:
            logger.info("Feed %s task cancelled", feed["name"])
            return

        except Exception:
            logger.exception("Error processing feed %s", feed["name"])
            self._releasing_feeds.add(feed["id"])
            try:
                await self._store.report_feed_failure(
                    feed["id"],
                    worker_id,
                    self._settings.failure_threshold,
                )
            except Exception:
                logger.exception(
                    "Failed to record failure for feed %s", feed["name"],
                )
            finally:
                self._releasing_feeds.discard(feed["id"])
            return

        # Normal completion (capture generator exhausted)
        self._releasing_feeds.add(feed["id"])
        try:
            await self._store.release_feed(feed["id"], worker_id)
        except Exception:
            logger.exception("Failed to release feed %s", feed["name"])
        finally:
            self._releasing_feeds.discard(feed["id"])

    # -- Heartbeat OS thread ----------------------------------------------

    def _heartbeat_loop(self) -> None:
        """
        OS daemon thread: schedule heartbeat cycle and enforce stall timeout.

        Uses a monotonic ticker for consistent heartbeat period regardless
        of cycle duration.
        """
        interval = self._settings.heartbeat_interval_sec
        next_tick = time.monotonic() + interval

        while not self._thread_stop.is_set():
            sleep_time = max(0.0, next_tick - time.monotonic())
            if self._thread_stop.wait(timeout=sleep_time):
                break

            try:
                future = asyncio.run_coroutine_threadsafe(
                    self._heartbeat_cycle(), self._loop,
                )
                future.result(
                    timeout=self._settings.heartbeat_stall_timeout_sec,
                )
            except concurrent.futures.TimeoutError:
                logger.critical(
                    "Event loop stall -- heartbeat did not complete "
                    "in %ds, terminating",
                    self._settings.heartbeat_stall_timeout_sec,
                )
                logging.shutdown()
                os._exit(1)
            except Exception:
                logger.exception("Heartbeat renewal error")

            next_tick += interval

    async def _heartbeat_cycle(self) -> None:
        """
        Renew heartbeats and detect fence violations.

        Runs entirely on the event loop thread — all dict/task access is
        thread-safe.
        """
        active = dict(self._feed_tasks.items())
        if not active:
            return

        renewed_ids = await self._heartbeat_store.renew_heartbeats_batch(
            list(active.keys()), self._settings.worker_id,
        )

        lost_ids = {
            fid
            for fid in active
            if fid not in renewed_ids
            and not active[fid].done()
            and fid not in self._releasing_feeds
        }
        if not lost_ids:
            logger.debug(
                "Heartbeat renewed for %d feeds", len(renewed_ids),
            )
            return

        logger.critical(
            "Heartbeat fence violation -- %d feed(s) lost: %s. "
            "Terminating.",
            len(lost_ids),
            ", ".join(str(fid) for fid in lost_ids),
        )
        logging.shutdown()
        os._exit(1)

    # -- Shutdown sequence ------------------------------------------------

    async def _shutdown_sequence(self) -> None:
        """
        Orderly teardown: cancel feed tasks, stop heartbeat thread,
        close GCS client and database pools.
        """
        logger.info(
            "Shutting down -- %d active feed tasks", len(self._feed_tasks),
        )
        self._thread_stop.set()

        if (
            self._heartbeat_thread is not None
            and self._heartbeat_thread.is_alive()
        ):
            await asyncio.to_thread(self._heartbeat_thread.join, timeout=5)

        # Cancel all feed tasks
        for task in self._feed_tasks.values():
            task.cancel()
        if self._feed_tasks:
            await asyncio.wait(
                self._feed_tasks.values(),
                timeout=self._settings.graceful_shutdown_timeout_sec,
            )

        await close_client()

        if self._heartbeat_pool is not None:
            try:
                await self._heartbeat_pool.close()
            except Exception:  # noqa: S110
                pass  # expected if heartbeat cycle was mid-query

        if self._pool is not None:
            await close_pool(self._pool)

        logger.info("Shutdown complete")
