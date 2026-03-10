import asyncio
import concurrent.futures
import datetime
import logging
import os
import signal
import threading
import time
import uuid
from collections.abc import AsyncIterator, Callable

import asyncpg
from google.cloud import pubsub_v1

from backend.pipeline.ingestion.settings import NormalizerSettings

from backend.pipeline.ingestion.gcs import close_client, upload_audio
from backend.pipeline.schema_types.raw_audio_chunk_pb2 import AudioChunk
from backend.pipeline.storage.connection import close_pool, create_pool
from backend.pipeline.storage.feed_store import FeedStore, LeasedFeed

FeedID = uuid.UUID
CaptureFn = Callable[[LeasedFeed, asyncio.Event], AsyncIterator[bytes]]

logger = logging.getLogger(__name__)

publisher = pubsub_v1.PublisherClient()


class NormalizerRuntime:
    """
    Generic runtime orchestrator for a fleet of async feed-processing tasks.

    Manages the full lifecycle of feed ingestion: batch-leasing feeds from
    the database, spawning an asyncio Task per feed, renewing heartbeats via
    an OS daemon thread (immune to event loop starvation), and detecting fence
    violations (immediate ``os._exit(1)`` on any lost lease).

    Normalizer authors provide a single ``capture_fn`` async generator that
    yields audio chunks. The runtime handles everything else.

    Design target: 250 concurrent feeds per GCE instance on a Managed
    Instance Group (MIG). The Stream Capturer MIG scales horizontally based
    on Stream Utilization % (number of active streams per instance), preventing
    data loss by ensuring fleet size is proportional to the stateful workload.
    Composition over inheritance — the capture function is passed in, not subclassed.

    INVARIANT: ``asyncio.sleep()`` and ``time.sleep()`` must not appear
    anywhere in this file. All waits use ``_sleep_or_shutdown`` or
    ``Event.wait(timeout=)`` so every wait point is interruptible by
    SIGTERM for prompt graceful shutdown.

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
        # threading.Event (not asyncio.Event) — the heartbeat OS thread
        # can't use asyncio primitives; it needs a thread-safe signal.
        self._thread_stop = threading.Event()
        # These fields are initialized in _main() before any method reads them.
        # Typed without None so the type checker doesn't require narrowing at
        # every usage site.  Accessing before _main() is a programming error.
        # asyncio.Event must be created inside a running event loop.
        self._shutdown: asyncio.Event = None  # type: ignore # set in _main()
        self._data_pool: asyncpg.Pool = None  # type: ignore # set in _main()
        # Dedicated 1-connection pool for heartbeat (control-plane / data-plane
        # separation). Prevents 250 bookmark/upload ops on the main pool from
        # starving heartbeat queries, which would cause false stall detection.
        self._heartbeat_pool: asyncpg.Pool = None  # type: ignore # set in _main()
        self._loop: asyncio.AbstractEventLoop = None  # type: ignore # set in _main()
        self._feed_tasks: dict[FeedID, asyncio.Task] = {}
        # Tracks feeds currently mid-await on release_feed/record_failure.
        # Without this, the heartbeat would see worker_id=NULL (set by the DB)
        # while the task is not yet .done(), misinterpreting the intentional
        # release as a fence violation and triggering os._exit(1).
        self._releasing_feeds: set[FeedID] = set()
        self._heartbeat_thread: threading.Thread | None = None
        self._store: FeedStore = None  # type: ignore # set in _main()
        self._heartbeat_store: FeedStore = None  # type: ignore # set in _main()

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
                    "Received %s -- initiating graceful shutdown",
                    sig.name,
                )
                self._shutdown.set()
                self._thread_stop.set()

        # Sets BOTH _shutdown (asyncio, for feed tasks) and _thread_stop
        # (threading, for heartbeat thread) to prevent the heartbeat from
        # firing a spurious os._exit(1) during the shutdown window.
        for sig in (signal.SIGTERM, signal.SIGINT):
            self._loop.add_signal_handler(sig, _on_signal, sig)

        s = self._settings
        # command_timeout: bounds query execution on established connections.
        # timeout (connect): bounds TCP handshake — without it, a VPC subnet
        # silently dropping packets hangs connect() for 2+ min (Linux TCP
        # SYN-ACK timeout), starving the pool of connections.
        self._data_pool = await create_pool(
            host=s.db_host,
            user=s.db_user,
            db_name=s.db_name,
            password=s.db_password,
            port=s.db_port,
            min_size=s.db_pool_min_size,
            max_size=s.db_pool_max_size,
            command_timeout=s.db_command_timeout_sec,
            timeout=s.db_connect_timeout_sec,
        )
        self._store = FeedStore(self._data_pool)

        # Dedicated 1-connection pool ensures heartbeat queries never queue
        # behind 250 bookmark/upload operations on the main pool. Without
        # this, pool contention causes false stall-timeout kills.
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
            statement_cache_size=0,
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

        This is the ONLY permitted wait mechanism in this file.
        ``asyncio.sleep()`` and ``time.sleep()`` must not appear anywhere —
        all waits must be interruptible for prompt SIGTERM response.

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
        """
        Continuously lease feeds in batches and spawn processing tasks.

        Uses ``acquire_feeds_batch`` with ``FOR UPDATE SKIP LOCKED`` for
        single-roundtrip batch acquisition. On startup with 250 empty slots
        this issues ~3 queries (at batch limit 100) instead of 250 individual
        ``LIMIT 1`` queries. During a 10-instance scale-up, this reduces
        AlloyDB contention from 2,500 serialized lock acquisitions to ~30.

        Reap latency (accepted trade-off): completed tasks are only reaped
        at the top of each iteration, so a dead task leaves a slot vacant
        for up to ``lease_poll_interval_sec - 1`` seconds. At 250 feeds,
        running at 249 for ~14s is negligible.
        """
        while True:
            self._reap_completed_tasks()

            # try/except: a transient DB error (connection reset, brief
            # AlloyDB maintenance) must not kill a worker with 200+ healthy
            # feed tasks. Existing tasks continue uninterrupted; the leasing
            # loop simply retries next cycle. During a full DB outage the
            # worker survives — mass os._exit(1) across the MIG would cause
            # a thundering herd cold-start on recovery. Since the DB is down,
            # no other worker can steal leases either.
            try:
                capacity = self._settings.max_feeds_per_worker - len(self._feed_tasks)
                if capacity > 0:
                    logger.info(
                        "Attempting to acquire up to %d feeds (%d/%d active)",
                        capacity,
                        len(self._feed_tasks),
                        self._settings.max_feeds_per_worker,
                    )
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
                    "Lease acquisition failed -- will retry in %.1fs",
                    self._settings.lease_poll_interval_sec,
                )

            if await self._sleep_or_shutdown(
                self._settings.lease_poll_interval_sec,
            ):
                return

    def _reap_completed_tasks(self) -> None:
        """
        Remove completed tasks and consume their exceptions.

        Must call ``task.exception()`` to retrieve the stored exception —
        without this, asyncio logs a "Task exception was never retrieved"
        warning at garbage collection time. ``.exception()`` on a cancelled
        task raises ``CancelledError``, hence the guard.
        """
        for feed_id in [fid for fid, t in self._feed_tasks.items() if t.done()]:
            task = self._feed_tasks.pop(feed_id)
            try:
                exc = task.exception()
            except asyncio.CancelledError:
                pass  # normal — task was cancelled by shutdown
            else:
                # Observability only — _process_feed already called
                # record_failure before the task exited.
                if exc is not None:
                    logger.error(
                        "Feed task %s failed: %s",
                        task.get_name(),
                        exc,
                        exc_info=exc,
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
                feed,
                self._shutdown,
            ):
                gcs_uri = await upload_audio(
                    audio_chunk,
                    feed,
                    self._settings.final_staging_bucket,
                    chunk_seq,
                )
                audio_chunk_msg = AudioChunk(gcs_uri=gcs_uri)
                now = datetime.datetime.now(tz=datetime.UTC)
                audio_chunk_msg.start_timestamp.FromDatetime(now)
                publisher.publish(
                    self._settings.pubsub_topic_path,
                    audio_chunk_msg.SerializeToString(),
                    feed_id=str(feed["id"]),
                )
                chunk_seq += 1

                ok = await self._store.update_feed_progress(
                    feed["id"],
                    worker_id,
                    gcs_uri,
                )
                if not ok:
                    # If the batched heartbeat is healthy (renewing every 15s),
                    # leases cannot be stolen within the 60s abandonment window.
                    # A stolen lease means the heartbeat mechanism itself failed
                    # systemically — cancelling one task while 249 others may
                    # also be compromised is unsafe. os._exit(1) is deliberate.
                    # MIG auto-heals within seconds; total downtime per feed:
                    # ~60s (abandonment window) + ~5s (instance restart).
                    logger.critical(
                        "Fence violation on bookmark for feed %s -- terminating",
                        feed["name"],
                    )
                    # logging.shutdown() flushes background logging threads
                    # (e.g. Cloud Logging's CloudLoggingHandler) that os._exit
                    # would bypass. This fence violation log is the only
                    # post-mortem evidence of split-brain.
                    logging.shutdown()
                    os._exit(1)

                if self._shutdown.is_set():
                    # Return without calling release_feed — _shutdown_sequence
                    # handles all task cancellation and the 60s abandonment
                    # window is the safety net if batch release fails.
                    logger.info(
                        "Shutdown -- stopping feed %s cleanly after chunk %d",
                        feed["name"],
                        chunk_seq,
                    )
                    return

        except asyncio.CancelledError:
            logger.info("Feed %s task cancelled", feed["name"])
            return

        except Exception:
            logger.exception("Error processing feed %s", feed["name"])
            # SAFETY: _releasing_feeds invariant — add BEFORE the first await
            # that drops the lease (report_feed_failure sets worker_id=NULL).
            self._releasing_feeds.add(feed["id"])
            try:
                await self._store.report_feed_failure(
                    feed["id"],
                    worker_id,
                    self._settings.feed_failure_threshold,
                )
            except Exception:
                # 60s abandonment window is the safety net if this fails.
                logger.exception(
                    "Failed to record failure for feed %s",
                    feed["name"],
                )
            finally:
                # SAFETY: No await between discard() and return. This ensures
                # _heartbeat_cycle cannot observe a state where the feed is
                # absent from _releasing_feeds but the task is not yet .done().
                self._releasing_feeds.discard(feed["id"])
            return

        # Normal completion (capture generator exhausted)
        # SAFETY: same _releasing_feeds invariant as above.
        self._releasing_feeds.add(feed["id"])
        try:
            await self._store.release_feed(feed["id"], worker_id)
        except Exception:
            # 60s abandonment window is the safety net.
            logger.exception("Failed to release feed %s", feed["name"])
        finally:
            # SAFETY: No await between discard() and return.
            self._releasing_feeds.discard(feed["id"])

    # -- Heartbeat OS thread ----------------------------------------------

    def _heartbeat_loop(self) -> None:
        """
        OS daemon thread: schedule heartbeat cycle and enforce stall timeout.

        Runs in a daemon OS thread (not an asyncio task) so it stays immune
        to event loop starvation — if the loop is blocked by a misbehaving
        capture function, this thread still fires and detects the stall.

        Uses a monotonic ticker for consistent heartbeat period regardless
        of cycle duration. Without this, the effective interval is
        ``heartbeat_interval + cycle_duration``. Under moderate DB load
        (5s cycles), heartbeats would fire every 20s instead of 15s. Two
        consecutive slow cycles could exceed the 60s abandonment window,
        triggering unnecessary ``os._exit(1)``.
        """
        interval = self._settings.heartbeat_interval_sec
        next_tick = time.monotonic() + interval

        while not self._thread_stop.is_set():
            sleep_time = max(0.0, next_tick - time.monotonic())
            if self._thread_stop.wait(timeout=sleep_time):
                break

            try:
                future = asyncio.run_coroutine_threadsafe(
                    self._heartbeat_cycle(),
                    self._loop,
                )
                future.result(
                    timeout=self._settings.heartbeat_stall_timeout_sec,
                )
            except concurrent.futures.TimeoutError:
                # CRITICAL: must be concurrent.futures.TimeoutError, NOT the
                # built-in TimeoutError. In Python 3.11+, asyncio.TimeoutError
                # aliases built-in TimeoutError — a DIFFERENT class from
                # concurrent.futures.TimeoutError. A bare "except TimeoutError"
                # would miss it, silently defeating the stall watchdog.
                logger.critical(
                    "Event loop stall -- heartbeat did not complete "
                    "in %ds, terminating",
                    self._settings.heartbeat_stall_timeout_sec,
                )
                logging.shutdown()  # flush before os._exit bypasses handlers
                os._exit(1)
            except Exception:
                # Transient DB error — log and retry. Don't kill: mass
                # os._exit(1) across the MIG during a DB outage would cause
                # a thundering herd cold-start on recovery.
                logger.exception("Heartbeat renewal error")

            # Advance ticker. If cycle took longer than one interval, the next
            # sleep_time clamps to 0 — fires immediately to catch up.
            next_tick += interval

    async def _heartbeat_cycle(self) -> None:
        """
        Renew heartbeats and detect fence violations.

        Runs entirely on the event loop thread — all dict/task access is
        thread-safe. Direct access from the OS heartbeat thread would risk
        ``RuntimeError: dictionary changed size during iteration`` and
        violates asyncio's thread-safety guarantees for ``task.done()``.

        Batched renewal reduces heartbeat DB load from ~17 qps
        (250 individual queries / 15s) to ~0.07 qps (1 query / 15s).
        """
        active = dict(self._feed_tasks.items())
        if not active:
            return

        renewed_ids = await self._heartbeat_store.renew_heartbeats_batch(
            list(active.keys()),
            self._settings.worker_id,
        )

        # A feed missing from renewed_ids is NOT a violation if:
        #   - active[fid].done(): task finished between snapshot and DB response
        #   - fid in _releasing_feeds: task is mid-await on release_feed /
        #     record_failure — DB has set worker_id=NULL but task hasn't returned.
        #     Without this filter, every normal release would trigger os._exit(1).
        # Any remaining feed is a true fence violation: another worker stole the
        # lease while this task was still running unaware.  Because the heartbeat
        # is batched, a single stolen lease implies a systemic failure (the
        # heartbeat mechanism itself broke), so we terminate the entire process
        # rather than cancelling one task while 249 others may also be compromised.
        lost_ids = {
            fid
            for fid in active
            if fid not in renewed_ids
            and not active[fid].done()
            and fid not in self._releasing_feeds
        }
        if not lost_ids:
            logger.debug(
                "Heartbeat renewed for %d feeds",
                len(renewed_ids),
            )
            return

        logger.critical(
            "Heartbeat fence violation -- %d feed(s) lost: %s. Terminating.",
            len(lost_ids),
            ", ".join(str(fid) for fid in lost_ids),
        )
        logging.shutdown()  # flush before os._exit bypasses handlers
        os._exit(1)

    # -- Shutdown sequence ------------------------------------------------

    async def _shutdown_sequence(self) -> None:
        """
        Orderly teardown: cancel feed tasks, stop heartbeat thread,
        close GCS client and database pools.
        """
        logger.info(
            "Shutting down -- %d active feed tasks",
            len(self._feed_tasks),
        )
        # Stop heartbeat FIRST to prevent it from seeing released feeds as
        # fence violations during the teardown window.
        self._thread_stop.set()

        # asyncio.to_thread avoids blocking the event loop during join().
        # Without this, .join() blocks the event loop thread, but the
        # heartbeat thread may be waiting for _heartbeat_cycle() to run ON
        # that event loop — a deadlock that wastes shutdown budget until
        # join() times out.
        if self._heartbeat_thread is not None and self._heartbeat_thread.is_alive():
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

        # Heartbeat pool close may raise if the heartbeat cycle was
        # mid-query when we stopped the thread — harmless during shutdown.
        if self._heartbeat_pool is not None:
            try:
                await self._heartbeat_pool.close()
            except Exception:  # noqa: S110
                pass

        if self._data_pool is not None:
            await close_pool(self._data_pool)

        logger.info("Shutdown complete")
