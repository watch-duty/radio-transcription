import asyncio
import concurrent.futures
import datetime
import logging
import os
import pathlib
import random
import signal
import threading
import time
import uuid
from collections.abc import AsyncIterator, Callable

import aiohttp
import asyncpg
import uvloop
from aiohttp import web
from google.api_core import exceptions as google_exceptions
from google.cloud.pubsub_v1.publisher import exceptions as pubsub_exceptions
from opentelemetry import trace

from backend.pipeline.common import gcp_helper
from backend.pipeline.common.clients import gcs_client, pubsub_client
from backend.pipeline.common.log_helper import setup_asyncio_logging
from backend.pipeline.common.tracing_utils import setup_tracing
from backend.pipeline.ingestion import (
    failure_policy,
    health_server,
    quarantine_reason,
    quarantine_telemetry,
)
from backend.pipeline.ingestion.failure_classifiers import pubsub
from backend.pipeline.ingestion.health_server import HealthState
from backend.pipeline.ingestion.models import (
    AudioMimeType,
    CapturedChunk,
    CaptureEvent,
    CaptureResources,
    FeedFailure,
    SourceObservation,
)
from backend.pipeline.ingestion.retry import (
    LeaseExpiredError,
    retry_with_lease_check,
)
from backend.pipeline.ingestion.router import resolve_topic_path
from backend.pipeline.ingestion.settings import CollectorSettings
from backend.pipeline.ingestion.slo_contract import EVENT_TYPE_CHUNK_INGESTED
from backend.pipeline.storage.connection import (
    close_pool,
    create_pool_with_retry,
)
from backend.pipeline.storage.feed_store import (
    FeedStatusReason,
    FeedStore,
    HeartbeatResult,
    LeasedFeed,
    SourceType,
)

FeedID = uuid.UUID
# 3-arg interface: route_capturer binds url_base internally and forwards
# the runtime-owned CaptureResources (currently just http_session) to the
# underlying 4-arg CollectorFn.
# See CollectorFn in models.py for the 4-arg raw collector signature.
CaptureFn = Callable[
    [LeasedFeed, asyncio.Event, CaptureResources],
    AsyncIterator[CaptureEvent],
]
logger = logging.getLogger(__name__)

_PIPELINE_GCS_UPLOAD_FAILED = "gcs_upload_failed"
_PIPELINE_BOOKMARK_WRITE_FAILED = "bookmark_write_failed"
_NON_BUDGETED_RETRY_MIN_SEC = 5 * 60
_NON_BUDGETED_RETRY_MAX_SEC = 15 * 60


class _PipelineFailure(Exception):
    """Post-capture runtime side-effect failure with a stable stage tag."""

    def __init__(
        self,
        reason: str,
        *,
        status_reason: FeedStatusReason,
    ) -> None:
        super().__init__(reason)
        self.reason = reason
        self.status_reason = status_reason


class CollectorRuntime:
    """
    Generic runtime orchestrator for a fleet of async feed-processing tasks.

    Manages the full lifecycle of feed ingestion: batch-leasing feeds from
    the database, spawning an asyncio Task per feed, renewing heartbeats via
    an OS daemon thread (immune to event loop starvation), and detecting fence
    violations (immediate ``os._exit(1)`` on any lost lease).

    Collector authors provide a single ``capture_fn`` async generator that
    yields audio chunks. The runtime handles everything else.

    Design target: 250 concurrent feeds per GCE instance on a Managed
    Instance Group (MIG). The Stream Capturer MIG scales horizontally based
    on Stream Utilization % (number of active streams per instance), preventing
    data loss by ensuring fleet size is proportional to the stateful workload.
    Composition over inheritance — the capture function is passed in, not subclassed.

    INVARIANT: ``asyncio.sleep()`` and ``time.sleep()`` must not appear
    anywhere in this file EXCEPT the single ``asyncio.sleep(0.25)``
    SSL-teardown idiom in ``_shutdown_sequence`` (HTTP-01 / aiohttp
    Pitfall 12 — see comment at that site). Every other wait point uses
    ``_sleep_or_shutdown`` or ``Event.wait(timeout=)`` so it is
    interruptible by SIGTERM for prompt graceful shutdown. The
    teardown sleep runs after ``_shutdown`` is already set, so
    interruptibility is moot at that point.

    Args:
        capture_fn: Async generator factory ``(feed, shutdown_event) -> AsyncIterator[CaptureEvent]``.
        settings: Runtime configuration. Defaults to ``CollectorSettings()``.

    """

    def __init__(
        self,
        capture_fn: CaptureFn,
        settings: CollectorSettings | None = None,
    ) -> None:
        if settings is None:
            from backend.pipeline.ingestion.settings import CollectorSettings  # noqa: I001, PLC0415

            settings = CollectorSettings()
        self._capture_fn = capture_fn
        self._collector_settings = settings
        # threading.Event (not asyncio.Event) — the heartbeat OS thread
        # can't use asyncio primitives; it needs a thread-safe signal.
        self._thread_stop = threading.Event()
        # These fields are initialized in _main() before any method reads them.
        # Typed without None so the type checker doesn't require narrowing at
        # every usage site.  Accessing before _main() is a programming error.
        # asyncio.Event must be created inside a running event loop.
        self._shutdown: asyncio.Event = None  # type: ignore # set in _main()
        # Monotonic (set-only, never cleared) signal: once this process has
        # confirmed lease loss, retry loops must stop permanently for this
        # runtime epoch.
        self._lease_lost: asyncio.Event = None  # type: ignore # set in _main()
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
        # aiohttp ClientSession is constructed in _main() because it
        # requires a running event loop. Lifecycle: session is closed in
        # _shutdown_sequence after _gcs_client.close() with a 250ms
        # SSL-teardown sleep (Pitfall 12).
        self._http_session: aiohttp.ClientSession = None  # type: ignore # set in _main()
        # _paused_for_memory: threading.Event (NOT bare bool) — read by the
        # asyncio leasing loop, set/cleared by the watchdog OS thread. See
        # PITFALLS.md Pitfall 20 — bool atomicity is a CPython implementation
        # detail; threading.Event is a contract.
        self._paused_for_memory = threading.Event()
        self._rss_watchdog_thread: threading.Thread | None = None
        self._capture_resources: CaptureResources = None  # type: ignore # set in _main()
        # Size the aiohttp connection pool to match the feed concurrency so
        # GCS uploads are never queued waiting for a free connection slot.
        # Without this, the default limit of 100 means ~150 uploads always
        # queue at the 250-feed target, adding latency and event-loop overhead.
        self._gcs_client = gcs_client.GcsClient(
            max_connections=self._collector_settings.max_feeds_per_worker,
        )
        self._pubsub_client = pubsub_client.PubSubClient()
        # Shared state published to the /healthz handler. feed_tasks is held
        # by reference to _feed_tasks so len() reflects live leasing state.
        self._health_state = HealthState(feed_tasks=self._feed_tasks)
        self._health_runner: web.AppRunner | None = None

    # -- Entry point ------------------------------------------------------

    def run(self) -> None:
        """
        Start the runtime. Blocks until shutdown completes.

        Uses uvloop as the event loop implementation. uvloop is a
        drop-in asyncio replacement built on libuv (C extension) that
        reduces per-callback scheduling overhead, which matters at 250
        concurrent feed tasks each generating frequent I/O completions.
        """
        logger.info(
            "Starting CollectorRuntime worker_id=%s max_feeds=%d",
            self._collector_settings.worker_id,
            self._collector_settings.max_feeds_per_worker,
        )
        setup_tracing(service_name="ingestion-service", is_ingestion=True)
        asyncio.run(self._main(), loop_factory=uvloop.new_event_loop)

    # -- Async core -------------------------------------------------------

    async def _main(self) -> None:
        """Top-level async entry: setup, run leasing loop, then shutdown."""
        self._loop = asyncio.get_running_loop()
        setup_asyncio_logging(self._loop)
        self._shutdown = asyncio.Event()
        self._lease_lost = asyncio.Event()

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

        settings = self._collector_settings
        # command_timeout: bounds query execution on established connections.
        # timeout (connect): bounds TCP handshake — without it, a VPC subnet
        # silently dropping packets hangs connect() for 2+ min (Linux TCP
        # SYN-ACK timeout), starving the pool of connections.
        self._data_pool = await create_pool_with_retry(settings.db)
        self._store = FeedStore(
            self._data_pool,
            claim_types=list(settings.caps.keys()),
        )

        # Dedicated 1-connection pool ensures heartbeat queries never queue
        # behind 250 bookmark/upload operations on the main pool. Without
        # this, pool contention causes false stall-timeout kills.
        hb_settings = settings.db.replace(pool_min_size=1, pool_max_size=1)
        self._heartbeat_pool = await create_pool_with_retry(hb_settings)
        # Heartbeat store doesn't claim feeds (only renews + counts), but
        # we pass the same claim_types for symmetry — keeps both stores
        # generated from the same set so a future divergence between
        # caps.keys() and the FeedStore default (all SourceType minus
        # ECHO) doesn't silently change the heartbeat store's
        # never-called acquire SQL out of sync with the data store's.
        self._heartbeat_store = FeedStore(
            self._heartbeat_pool,
            claim_types=list(settings.caps.keys()),
        )

        self._heartbeat_thread = threading.Thread(
            target=self._heartbeat_loop,
            daemon=True,
            name="heartbeat",
        )
        self._heartbeat_thread.start()

        # WATCHDOG-01: cgroup-aware self-RSS daemon thread (Phase 4).
        # Resolved limit drives whether the thread starts at all (D-22):
        # if cgroup limit is "max" or v1 sentinel, _resolve_* returns None
        # and we must NOT start the thread (the leasing loop's
        # _paused_for_memory.is_set() check trivially returns False forever
        # — acceptable, since the watchdog cannot do its job without a
        # limit). Joined in _shutdown_sequence immediately after the
        # heartbeat thread join and BEFORE feed-task cancellation
        # (PITFALLS Pitfall 1).
        watchdog_limit = self._resolve_container_memory_bytes()
        if watchdog_limit is None:
            logger.warning(
                "RSS watchdog disabled — no cgroup memory limit detected",
            )
        else:
            self._rss_watchdog_thread = threading.Thread(
                target=self._rss_watchdog_loop,
                args=(watchdog_limit,),
                daemon=True,
                name="rss-watchdog",
            )
            self._rss_watchdog_thread.start()

        quarantine_telemetry.configure(settings.google_cloud_project)

        # try/finally wraps session creation, capture_resources, and
        # health_server.start so an exception in any of those triggers
        # _shutdown_sequence — closes the aiohttp session and joins
        # heartbeat/watchdog threads cleanly (each guarded by None/is_alive
        # checks so partial init is safe).
        try:
            # HTTP-01: runtime-owned aiohttp.ClientSession. Constructed
            # inside _main() because the connector requires a running event
            # loop. TCPConnector kwargs per research SUMMARY.md
            # (limit_per_host=64 absorbs activation bursts;
            # ttl_dns_cache=300 survives cold-start herd; keepalive_timeout=75
            # is > 60s poll cadence). NO enable_cleanup_closed — ignored on
            # Python 3.13.1+ (aiohttp 3.13.5 docs). Session is closed in
            # _shutdown_sequence AFTER _gcs_client.close(), followed by
            # await asyncio.sleep(0.25) for SSL teardown (Pitfall 12).
            self._http_session = aiohttp.ClientSession(
                connector=aiohttp.TCPConnector(
                    limit=0,
                    limit_per_host=64,
                    ttl_dns_cache=300,
                    keepalive_timeout=75,
                ),
                timeout=aiohttp.ClientTimeout(total=30, connect=10),
            )
            self._capture_resources = CaptureResources(
                http_session=self._http_session,
            )

            # Start /healthz HTTP server. Runs on the same event loop as the
            # leasing/heartbeat coroutines — this is load-bearing: if the
            # loop is wedged, the handler stops responding and GCP
            # autohealing takes over.
            self._health_runner = await health_server.start(
                settings,
                self._health_state,
            )

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

    # cgroup memory paths -- frozen at class level so the path objects
    # are constructed once at import time, not per call. Cached as
    # pathlib.Path so each poll is just `.read_text()` + `int()`.
    _CGROUP_V2_LIMIT_PATH = pathlib.Path("/sys/fs/cgroup/memory.max")
    _CGROUP_V2_USAGE_PATH = pathlib.Path("/sys/fs/cgroup/memory.current")
    _CGROUP_V1_LIMIT_PATH = pathlib.Path(
        "/sys/fs/cgroup/memory/memory.limit_in_bytes",
    )
    _CGROUP_V1_USAGE_PATH = pathlib.Path(
        "/sys/fs/cgroup/memory/memory.usage_in_bytes",
    )
    # cgroup v1 "unbounded" sentinel (~ 2**63). PITFALLS.md Pitfall 2.
    _CGROUP_V1_UNBOUNDED_SENTINEL_THRESHOLD = 2**62

    @staticmethod
    def _resolve_container_memory_bytes() -> int | None:  # noqa: PLR0911
        """
        Return the container memory limit in bytes, or None if unbounded.

        Priority order (D-01 / D-04):
          1. cgroup v2 unified hierarchy (/sys/fs/cgroup/memory.max).
             Literal "max" -> None (PITFALLS.md Pitfall 2 -- DO NOT
             fall back to psutil.virtual_memory(); host-namespaced).
          2. cgroup v1 fallback (/sys/fs/cgroup/memory/memory.limit_in_bytes).
             Values >= 2**62 are the v1 "unbounded" sentinel -> None.
          3. None on OSError, malformed/non-integer content, or a
             non-positive limit (a 0 limit would cause ZeroDivisionError
             in the watchdog ratio computation).

        Caller MUST treat None as "watchdog disabled" and emit a
        WARNING per D-22 -- never fall back to host-namespaced memory.
        """
        # cgroup v2 (unified hierarchy; expected on COS / GKE / Cloud Run)
        try:
            raw = CollectorRuntime._CGROUP_V2_LIMIT_PATH.read_text().strip()
        except OSError:
            pass
        else:
            if raw == "max":
                return None
            try:
                val = int(raw)
            except ValueError:
                return None
            return val if val > 0 else None
        # cgroup v1 fallback (legacy hosts; should not be reachable on COS)
        try:
            raw = CollectorRuntime._CGROUP_V1_LIMIT_PATH.read_text().strip()
        except OSError:
            return None
        try:
            val = int(raw)
        except ValueError:
            return None
        if (
            val >= CollectorRuntime._CGROUP_V1_UNBOUNDED_SENTINEL_THRESHOLD
            or val <= 0
        ):
            return None
        return val

    @staticmethod
    def _resolve_container_memory_usage_bytes() -> int | None:
        """
        Return current container memory usage in bytes, or None on read error.

        Tries cgroup v2 (/sys/fs/cgroup/memory.current) first, falls back
        to cgroup v1 (/sys/fs/cgroup/memory/memory.usage_in_bytes). Per
        D-04, the "max"/sentinel handling lives in
        _resolve_container_memory_bytes -- this helper only sees numeric
        values once the limit phase has decided the watchdog should run.

        Returns None if BOTH paths raise OSError. The caller (the
        watchdog body in Plan 04-02) defensively pauses on None per
        CONTEXT.md "Specifics": "if read_text raises OSError mid-run,
        set _paused_for_memory defensively and emit a single ERROR log".
        """
        try:
            return int(
                CollectorRuntime._CGROUP_V2_USAGE_PATH.read_text().strip(),
            )
        except (OSError, ValueError):
            pass
        try:
            return int(
                CollectorRuntime._CGROUP_V1_USAGE_PATH.read_text().strip(),
            )
        except (OSError, ValueError):
            return None

    # -- Leasing ----------------------------------------------------------

    async def _leasing_loop(self) -> None:  # noqa: PLR0912
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
            # Memory back-pressure (WATCHDOG-01 / D-26..D-28): if RSS
            # crossed the pause threshold, don't claim more feeds. Existing
            # leases continue running (held leases are renewed by the
            # heartbeat — pause means "stop claiming MORE", not "abandon
            # current ones"). Use _sleep_or_shutdown so SIGTERM still
            # interrupts the pause — PITFALLS.md Pitfall 5 ("never bare
            # asyncio.sleep").
            #
            # Reap completed tasks BEFORE the pause check so a completed task
            # that raised an exception has its error retrieved promptly even
            # while the watchdog is paused — otherwise asyncio emits "Task
            # exception was never retrieved" warnings and tasks linger in
            # _feed_tasks until pause clears.
            self._reap_completed_tasks()

            if self._paused_for_memory.is_set():
                if await self._sleep_or_shutdown(
                    self._collector_settings.rss_watchdog_poll_interval_sec,
                ):
                    return
                continue

            # try/except: a transient DB error (connection reset, brief
            # AlloyDB maintenance) must not kill a worker with 200+ healthy
            # feed tasks. Existing tasks continue uninterrupted; the leasing
            # loop simply retries next cycle. During a full DB outage the
            # worker survives — mass os._exit(1) across the MIG would cause
            # a thundering herd cold-start on recovery. Since the DB is down,
            # no other worker can steal leases either.
            try:
                total_slack = (
                    self._collector_settings.max_feeds_per_worker
                    - len(self._feed_tasks)
                )
                if total_slack > 0:
                    s = self._collector_settings
                    caps = s.caps
                    # Pull the authoritative per-type held count from the
                    # DB before apportioning. See FeedStore.count_held_by_type
                    # for rationale: the previous in-memory counter leaked
                    # twice during PR review cycles (orphan-on-running +
                    # orphan-on-done), both silent O(N) drifts. DB-truth
                    # per cycle is structurally drift-proof and costs one
                    # extra round-trip per lease_poll_interval_sec.
                    held = await self._store.count_held_by_type(s.worker_id)
                    # Apportion total_slack across the per-type CTE
                    # branches (see _calculate_branch_limits for the
                    # algorithm + rationale). Extracted to a pure helper
                    # so the allocation math is unit-testable without the
                    # surrounding asyncio loop.
                    limits = self._calculate_branch_limits(
                        total_slack,
                        caps,
                        held,
                    )
                    logger.info(
                        "Attempting to acquire feeds "
                        "(slack=%d, caps=%s, held=%s, limits=%s, total_ask=%d)",
                        total_slack,
                        {t.value: v for t, v in caps.items()},
                        {t.value: v for t, v in held.items()},
                        {t.value: v for t, v in limits.items()},
                        sum(limits.values()),
                    )
                    primary = await self._store.acquire_feeds_batch(
                        s.worker_id,
                        limits,
                    )
                    leases: list[LeasedFeed] = list(primary)
                    # When the primary per-type CTE underfills (caps bound
                    # the ask below remaining slack, or there simply aren't
                    # enough unclaimed feeds of the right types), try the
                    # recovery path to sweep up failing-retryable +
                    # active-abandoned rows. The pg_cron sweep reclaims most
                    # active-abandoned rows at 30 s cadence, but this path
                    # still earns its keep for failing-retryable and for
                    # reclaiming slack before the next sweep tick.
                    if len(primary) < total_slack:
                        # Recovery must respect the SAME per-type caps
                        # the primary path enforced. Without re-running
                        # the apportion, recovery could push held > cap
                        # for a type whose primary path returned 0
                        # (e.g. all unclaimed of that type are
                        # failing-retryable, not unclaimed). Compute a
                        # fresh per-type limit dict from `held + primary
                        # acquired-by-type` and the remaining slack.
                        primary_by_type: dict[SourceType, int] = dict.fromkeys(
                            caps, 0
                        )
                        for lease in primary:
                            t = lease["source_type"]
                            if t in primary_by_type:
                                primary_by_type[t] += 1
                        held_after_primary = {
                            t: held.get(t, 0) + primary_by_type[t] for t in caps
                        }
                        recovery_limits = self._calculate_branch_limits(
                            total_slack - len(primary),
                            caps,
                            held_after_primary,
                        )
                        recovery = await self._store.acquire_feeds_recovery(
                            s.worker_id,
                            s.abandonment_window_sec,
                            recovery_limits,
                        )
                        leases.extend(recovery)

                    for lease in leases:
                        existing = self._feed_tasks.get(lease["id"])
                        if existing is not None:
                            # Whether or not `existing` is already .done(),
                            # it's about to be overwritten in _feed_tasks
                            # below and become unreachable from the
                            # reaper's iteration over
                            # self._feed_tasks.items(). We still need to
                            # consume the old task's exception — the
                            # reaper won't see it after the overwrite.
                            # Branches:
                            #   - not done: still running with an outdated
                            #     fencing token; if left alive it would
                            #     eventually fail a fenced write and call
                            #     os._exit(1). Cancel it (the
                            #     CancelledError path in _process_feed
                            #     exits cleanly without DB writes).
                            #   - done: task already exited between our
                            #     last reap call and acquire_feeds_batch
                            #     returning. Exception-consumption cleanup
                            #     is still required for the same reason.
                            if not existing.done():
                                logger.warning(
                                    "Cancelling orphaned task for feed %s "
                                    "(re-leased with token %d)",
                                    lease["name"],
                                    lease["fencing_token"],
                                )
                                existing.cancel()
                            else:
                                logger.info(
                                    "Reaping already-done task for feed %s "
                                    "inline during re-lease (reaper won't see it "
                                    "after overwrite)",
                                    lease["name"],
                                )
                            # Consume the task's exception so asyncio
                            # doesn't log "Task exception was never
                            # retrieved" at GC time. For an already-done
                            # task add_done_callback fires synchronously;
                            # for one we just cancelled it fires after
                            # cancel propagates. Either way the reaper
                            # won't get to this task (it's unreachable).
                            existing.add_done_callback(
                                self._consume_orphan_exception,
                            )
                        task = asyncio.create_task(
                            self._process_feed(lease),
                            name=f"feed-{lease['name']}",
                        )
                        self._feed_tasks[lease["id"]] = task
                    if leases:
                        logger.info(
                            "Acquired %d feeds (primary=%d, recovery=%d) "
                            "— %d/%d active",
                            len(leases),
                            len(primary),
                            len(leases) - len(primary),
                            len(self._feed_tasks),
                            self._collector_settings.max_feeds_per_worker,
                        )
            except Exception:
                logger.exception(
                    "Lease acquisition failed -- will retry in %.1fs",
                    self._collector_settings.lease_poll_interval_sec,
                )

            if await self._sleep_or_shutdown(
                self._collector_settings.lease_poll_interval_sec,
            ):
                return

    def _reap_completed_tasks(self) -> None:
        """Remove completed tasks and consume their exceptions.

        Must call ``task.exception()`` to retrieve the stored exception --
        without this, asyncio logs a "Task exception was never retrieved"
        warning at garbage collection time. ``.exception()`` on a cancelled
        task raises ``CancelledError``, hence the guard.

        The policy-routing handler in ``_process_feed`` is the primary
        fault-response path. The reaper exists to drain
        ``task.exception()`` so asyncio does not emit
        "Task exception was never retrieved" warnings, and to log meta-bugs
        where the catch handler itself raised -- which would indicate the
        DB write or telemetry call inside the catch arm crashed. Such
        meta-bugs surface as ERROR logs here; the lease abandonment window
        is the recovery safety net (D-04, v1.1).
        """
        for feed_id in [fid for fid, t in self._feed_tasks.items() if t.done()]:
            task = self._feed_tasks.pop(feed_id)
            try:
                exc = task.exception()
            except asyncio.CancelledError:
                pass  # normal -- task was cancelled by shutdown
            else:
                if exc is not None:
                    logger.error(
                        "Feed task %s failed: %s",
                        task.get_name(),
                        exc,
                        exc_info=exc,
                    )

    @staticmethod
    def _calculate_branch_limits(
        total_slack: int,
        caps: dict[SourceType, int],
        held: dict[SourceType, int],
    ) -> dict[SourceType, int]:
        """Water-filling apportion of total_slack across per-type branches.

        Per-type caps bound each branch individually (16.9 MiB/feed x 240
        bcfy_feeds rows is one worker's memory ceiling for that type). But
        the claim query is a UNION ALL across three branches, so bounding
        each branch by total_slack independently would let the SUM exceed
        total_slack — e.g. at cold start with max_feeds_per_worker=250,
        three branches of 250 each would return 740 feeds in one call,
        blowing past the worker budget by 3x.

        Algorithm: sort by ascending headroom, give each branch
        min(headroom, fair_share) where fair_share is
        `remaining_slack // remaining_branches`. Guarantees
        sum(limits.values()) <= total_slack, redistributes slack that a
        capped branch couldn't absorb to the branches behind it, and
        avoids starving any type. O(N log N) on a 3-element dict —
        effectively O(1). max() on per-type headroom is defensive against
        a negative-held accounting bug (which would otherwise make
        cap - held > cap).

        Args:
            total_slack: max_feeds_per_worker - len(_feed_tasks).
            caps: per-SourceType env-var-driven cap ceilings.
            held: DB-derived per-type active-lease count for this worker
                (see FeedStore.count_held_by_type). May contain extra
                keys beyond ``caps`` (e.g. ECHO always 0) — they are
                ignored because the dict comprehension iterates ``caps``.
                Keys in ``caps`` that are absent from ``held`` are
                treated as 0; this keeps the function safe against any
                future caller that passes a sparse dict.

        Returns:
            Dict mapping each SourceType in `caps` to its computed LIMIT.
            Sum of values is <= total_slack and each value is >= 0.
        """
        # Clamp headroom at the cap, not just at zero: if `held` somehow
        # went negative (double-decrement accounting bug), max(0, cap -
        # held) > cap and would let a branch's LIMIT exceed the memory
        # ceiling the cap represents. min(cap, ...) restores the "never
        # exceeds cap" invariant regardless of held's sign.
        headroom: dict[SourceType, int] = {
            t: min(caps[t], max(0, caps[t] - held.get(t, 0))) for t in caps
        }
        limits: dict[SourceType, int] = dict.fromkeys(caps, 0)
        # Sort types by ascending headroom so tight branches are sized
        # first; the loop's integer-division share naturally widens for
        # later branches as preceding branches consume less than their
        # "fair" slice.
        sorted_types = sorted(caps, key=lambda t: headroom[t])
        remaining_slack = total_slack
        remaining_branches = len(sorted_types)
        for t in sorted_types:
            # share recomputes per iteration: when an earlier branch is
            # headroom-limited (limits[t] < share), the savings flow into
            # the next branch's share automatically. Integer-division
            # remainders likewise compound forward, so the last branch's
            # share == remaining_slack and absorbs everything that fits.
            # Any remaining_slack > 0 at end of loop implies every branch
            # ended at headroom — there's nowhere to redistribute to.
            share = (
                remaining_slack // remaining_branches
                if remaining_branches > 0
                else 0
            )
            limits[t] = min(headroom[t], share)
            remaining_slack -= limits[t]
            remaining_branches -= 1
        return limits

    @staticmethod
    def _consume_orphan_exception(task: asyncio.Task) -> None:
        """Consume the exception from an orphaned (cancelled-and-replaced) task.

        Called via ``add_done_callback`` on tasks that _leasing_loop
        cancels when it re-leases a feed it already holds. The reaper
        normally calls ``task.exception()`` on completed tasks to prevent
        the "Task exception was never retrieved" warning at GC time, but
        orphaned tasks aren't in _feed_tasks anymore, so the reaper can't
        reach them. This callback does that job instead.
        """
        try:
            exc = task.exception()
        except asyncio.CancelledError:
            pass  # expected path for the re-lease cancel race
        else:
            if exc is not None:
                logger.error(
                    "Orphaned feed task %s exited with error: %s",
                    task.get_name(),
                    exc,
                    exc_info=exc,
                )

    # -- Per-feed pipeline ------------------------------------------------

    def _get_pubsub_topic_path(self, feed: LeasedFeed) -> str:
        """Determines the Pub/Sub topic path based on the feed source type."""
        return resolve_topic_path(feed["source_type"], self._collector_settings)

    @staticmethod
    def _non_budgeted_retry_after() -> datetime.datetime:
        """Return the retry time for non-budgeted failure lanes."""
        jitter_sec = random.uniform(  # noqa: S311 -- scheduling jitter
            _NON_BUDGETED_RETRY_MIN_SEC,
            _NON_BUDGETED_RETRY_MAX_SEC,
        )
        return datetime.datetime.now(datetime.UTC) + datetime.timedelta(
            seconds=jitter_sec,
        )

    def _emit_policy_decision(
        self,
        feed: LeasedFeed,
        *,
        reason: str,
        status_reason: FeedStatusReason,
        retry_after: datetime.datetime | None = None,
        replay_missing: bool = False,
        data_gap_known: bool = False,
    ) -> None:
        """Emit the canonical policy-decision telemetry event."""
        action = failure_policy.classify_failure_policy(status_reason)
        payload: dict[str, object] = {
            "event_type": "feed_failure_policy_decision",
            "feed_id": str(feed["id"]),
            "source_type": str(feed["source_type"]),
            "reason": reason,
            "status_reason": status_reason.value,
            "replay_missing": replay_missing,
            "data_gap_known": data_gap_known,
            "executed_action": action.value,
        }
        if retry_after is not None:
            payload["retry_after"] = retry_after.isoformat()
        logger.info(
            "Feed failure policy decision", extra={"json_fields": payload}
        )

    def _emit_post_bookmark_publish_failure(
        self,
        feed: LeasedFeed,
        *,
        reason: str,
        status_reason: FeedStatusReason,
    ) -> None:
        """Emit explicit evidence that capture/bookmark succeeded but publish did not."""
        action = failure_policy.classify_failure_policy(status_reason)
        payload: dict[str, object] = {
            "event_type": "post_bookmark_publish_failure",
            "feed_id": str(feed["id"]),
            "source_type": str(feed["source_type"]),
            "reason": reason,
            "status_reason": status_reason.value,
            "replay_missing": True,
            "data_gap_known": True,
            "executed_action": action.value,
        }
        logger.error(
            "Post-bookmark publish failure",
            extra={"json_fields": payload},
        )

    async def _process_captured_chunk(
        self,
        feed: LeasedFeed,
        captured_chunk: CapturedChunk,
        seq_for_chunk: int,
        worker_id: uuid.UUID,
        fencing_token: int,
        topic_path: str,
        chunk_extension: str,
        chunk_content_type: str,
    ) -> None:
        """Run post-capture upload, bookmark, publish, and SLO logging."""
        settings = self._collector_settings
        try:
            gcs_uri = await retry_with_lease_check(
                gcp_helper.upload_staged_audio,
                self._gcs_client,
                captured_chunk.audio_bytes,
                feed,
                settings.audio_staging_bucket,
                seq_for_chunk,
                fencing_token,
                chunk_extension,
                chunk_content_type,
                lease_lost=self._lease_lost,
                shutdown=self._shutdown,
                max_retries=settings.gcs_upload_max_retries,
                base_delay_sec=settings.gcs_upload_retry_base_delay_sec,
                max_delay_sec=settings.gcs_upload_retry_max_delay_sec,
                retryable=(
                    aiohttp.ClientError,
                    asyncio.TimeoutError,
                    OSError,
                ),
                operation_name="GCS upload",
            )
        except (asyncio.CancelledError, LeaseExpiredError):
            raise
        except Exception as exc:
            raise _PipelineFailure(
                _PIPELINE_GCS_UPLOAD_FAILED,
                status_reason=FeedStatusReason.SYSTEM_PIPELINE_ERROR,
            ) from exc

        duration_ms = int(
            (
                captured_chunk.chunk_end_time - captured_chunk.chunk_start_time
            ).total_seconds()
            * 1000
        )
        try:
            ok = await retry_with_lease_check(
                self._store.update_feed_progress,
                feed["id"],
                worker_id,
                gcs_uri,
                fencing_token,
                # bcfy_calls supplies the API `ts` resume cursor;
                # stream collectors leave it None -> end_ts fallback.
                captured_chunk.resume_position or captured_chunk.chunk_end_time,
                lease_lost=self._lease_lost,
                shutdown=self._shutdown,
                max_retries=settings.bookmark_max_retries,
                base_delay_sec=settings.bookmark_retry_base_delay_sec,
                max_delay_sec=settings.bookmark_retry_max_delay_sec,
                retryable=(
                    asyncpg.PostgresConnectionError,
                    asyncpg.InterfaceError,
                    OSError,
                ),
                operation_name="bookmark write",
            )
        except (asyncio.CancelledError, LeaseExpiredError):
            raise
        except Exception as exc:
            raise _PipelineFailure(
                _PIPELINE_BOOKMARK_WRITE_FAILED,
                status_reason=FeedStatusReason.SYSTEM_PIPELINE_ERROR,
            ) from exc

        if not ok:
            # If the batched heartbeat is healthy (renewing every 15s),
            # leases cannot be stolen within the 60s abandonment window.
            # A stolen lease means the heartbeat mechanism itself failed
            # systemically -- cancelling one task while 249 others may
            # also be compromised is unsafe. os._exit(1) is deliberate.
            # MIG auto-heals within seconds; total downtime per feed:
            # ~60s (abandonment window) + ~5s (instance restart).
            logger.critical(
                "Fence violation on bookmark for feed %s -- terminating",
                feed["name"],
            )
            self._lease_lost.set()
            # logging.shutdown() flushes background logging threads
            # (e.g. Cloud Logging's CloudLoggingHandler) that os._exit
            # would bypass.
            logging.shutdown()
            os._exit(1)

        try:
            message_id = await retry_with_lease_check(
                gcp_helper.publish_audio_chunk,
                self._pubsub_client,
                topic_path,
                str(feed["id"]),
                feed["name"],
                gcs_uri,
                captured_chunk.session_id,
                captured_chunk.chunk_start_time,
                duration_ms,
                feed["source_type"],
                captured_chunk.external_audio_segment_id,
                lease_lost=self._lease_lost,
                shutdown=self._shutdown,
                max_retries=settings.pubsub_publish_max_retries,
                base_delay_sec=settings.pubsub_publish_retry_base_delay_sec,
                max_delay_sec=settings.pubsub_publish_retry_max_delay_sec,
                retryable=(
                    google_exceptions.Aborted,
                    google_exceptions.DeadlineExceeded,
                    google_exceptions.InternalServerError,
                    google_exceptions.ResourceExhausted,
                    google_exceptions.ServiceUnavailable,
                    google_exceptions.Unknown,
                    google_exceptions.Cancelled,
                    pubsub_exceptions.PublishToPausedOrderingKeyException,
                ),
                operation_name="Pub/Sub publish",
            )
        except (asyncio.CancelledError, LeaseExpiredError):
            raise
        except Exception as exc:
            raise _PipelineFailure(
                pubsub.publish_failure_reason(exc),
                status_reason=(
                    FeedStatusReason.PIPELINE_PUBLISH_AFTER_BOOKMARK_FAILED
                ),
            ) from exc

        logger.info(
            "Published message %s for feed %s",
            message_id,
            feed["name"],
        )

        # SLO: chunk_ingested emit -- strictly after publish success.
        chunk_ingested_payload: dict[str, object] = {
            "event_type": EVENT_TYPE_CHUNK_INGESTED,
            "feed_id": str(feed["id"]),
            "source_type": feed["source_type"],
        }
        if captured_chunk.receipt_time is not None:
            raw_latency_sec = (
                datetime.datetime.now(datetime.UTC)
                - captured_chunk.receipt_time
            ).total_seconds()
            chunk_ingested_payload["processing_latency_sec"] = max(
                0.0, round(raw_latency_sec, 2)
            )
            if raw_latency_sec < 0:
                chunk_ingested_payload["latency_clamped"] = True
        logger.info(
            "Chunk ingested",
            extra={"json_fields": chunk_ingested_payload},
        )

    async def _record_feed_failure(
        self,
        feed: LeasedFeed,
        worker_id: uuid.UUID,
        fencing_token: int,
        *,
        reason: str,
        status_reason: FeedStatusReason,
        replay_missing: bool = False,
        data_gap_known: bool = False,
    ) -> None:
        """Persist a classified feed failure through the fenced store path."""
        self._emit_policy_decision(
            feed,
            reason=reason,
            status_reason=status_reason,
            replay_missing=replay_missing,
            data_gap_known=data_gap_known,
        )
        extra: dict[str, object] = {
            "json_fields": {
                "feed_id": str(feed["id"]),
                "source_type": str(feed["source_type"]),
                "reason": reason,
                "status_reason": status_reason.value,
            },
        }
        if status_reason in (
            FeedStatusReason.SOURCE_OFFLINE,
            FeedStatusReason.SOURCE_UNREACHABLE,
            FeedStatusReason.SOURCE_RATE_LIMITED,
        ):
            # External operational Gotchas (e.g. Icecast 404 stream missing, CDN bans) are environment observations.
            # Log them as a warning without attaching a noisy stack traceback or spawning permanent Stackdriver Error Groups.
            logger.warning(
                "Feed source processing observation: feed=%s reason=%s status_reason=%s",
                feed["name"],
                reason,
                status_reason.value,
                extra=extra,
            )
        else:
            # Actionable Watch Duty internal system Gotchas (e.g. collector bugs, DB/auth failures) warrant an ERROR traceback.
            logger.exception(
                "Feed internal processing error: feed=%s reason=%s status_reason=%s",
                feed["name"],
                reason,
                status_reason.value,
                extra=extra,
            )
        # SAFETY: _releasing_feeds invariant -- add BEFORE the first await
        # that drops the lease (report_feed_failure sets worker_id=NULL).
        self._releasing_feeds.add(feed["id"])
        try:
            status = await self._store.report_feed_failure(
                feed["id"],
                worker_id,
                fencing_token,
                self._collector_settings.feed_failure_threshold,
                reason=reason,
                status_reason=status_reason,
            )
            if status == "quarantined":
                await quarantine_telemetry.emit_quarantine_event(
                    feed_id=str(feed["id"]),
                    feed_name=feed["name"],
                    source_type=str(feed["source_type"]),
                    reason=reason,
                    status_reason=status_reason.value,
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

    async def _record_non_budgeted_failure(
        self,
        feed: LeasedFeed,
        worker_id: uuid.UUID,
        fencing_token: int,
        *,
        reason: str,
        status_reason: FeedStatusReason,
        replay_missing: bool = False,
        data_gap_known: bool = False,
    ) -> None:
        """Release a failure that must not consume the feed quarantine budget."""
        retry_after = self._non_budgeted_retry_after()
        self._emit_policy_decision(
            feed,
            reason=reason,
            status_reason=status_reason,
            retry_after=retry_after,
            replay_missing=replay_missing,
            data_gap_known=data_gap_known,
        )
        if replay_missing and data_gap_known:
            self._emit_post_bookmark_publish_failure(
                feed,
                reason=reason,
                status_reason=status_reason,
            )
        if status_reason.value.startswith("source_"):
            logger.info(
                "Feed source failure suppressed from quarantine budget: "
                "feed=%s reason=%s",
                feed["name"],
                reason,
            )
        else:
            logger.exception(
                "Feed processing error suppressed from quarantine budget: "
                "feed=%s reason=%s",
                feed["name"],
                reason,
            )
        # SAFETY: _releasing_feeds invariant -- add BEFORE the first await
        # that drops the lease (release_non_budgeted_failure sets
        # worker_id=NULL).
        self._releasing_feeds.add(feed["id"])
        try:
            await self._store.release_non_budgeted_failure(
                feed["id"],
                worker_id,
                fencing_token,
                retry_after=retry_after,
                status_reason=status_reason,
            )
        except Exception:
            # 60s abandonment window is the safety net if this fails.
            logger.exception(
                "Failed to record non-budgeted failure for feed %s",
                feed["name"],
            )
        finally:
            # SAFETY: No await between discard() and return.
            self._releasing_feeds.discard(feed["id"])

    @staticmethod
    def _leased_feed_has_failure_state(feed: LeasedFeed) -> bool:
        """Return whether a leased feed carries persisted failure state."""
        return feed["failure_count"] > 0 or feed["status_reason"] is not None

    @staticmethod
    def _advance_local_bookmark(
        feed: LeasedFeed,
        resume_position: datetime.datetime | None,
    ) -> None:
        """Keep the local lease copy monotonic with observation SQL."""
        if resume_position is None:
            return
        current_bookmark = feed["last_bookmark_time"]
        if current_bookmark is None or resume_position > current_bookmark:
            feed["last_bookmark_time"] = resume_position

    async def _process_source_observation(
        self,
        feed: LeasedFeed,
        observation: SourceObservation,
        worker_id: uuid.UUID,
        fencing_token: int,
    ) -> bool:
        """Record a non-audio source success when persisted state needs it.

        Returns ``True`` when processing may continue, ``False`` when the task
        should stop because the row is no longer owned by this active lease.
        """
        if (
            not self._leased_feed_has_failure_state(feed)
            and observation.resume_position is None
        ):
            return True

        try:
            result = await retry_with_lease_check(
                self._store.record_source_observation,
                feed["id"],
                worker_id,
                fencing_token,
                observation.resume_position,
                lease_lost=self._lease_lost,
                shutdown=self._shutdown,
                max_retries=self._collector_settings.bookmark_max_retries,
                base_delay_sec=(
                    self._collector_settings.bookmark_retry_base_delay_sec
                ),
                max_delay_sec=(
                    self._collector_settings.bookmark_retry_max_delay_sec
                ),
                retryable=(
                    asyncpg.PostgresConnectionError,
                    asyncpg.InterfaceError,
                    OSError,
                ),
                operation_name="source observation write",
            )
        except (asyncio.CancelledError, LeaseExpiredError):
            raise
        except Exception:
            logger.exception(
                "Failed to record source observation for feed %s",
                feed["name"],
            )
            return True

        if result["recorded"]:
            feed["failure_count"] = 0
            feed["status_reason"] = None
            self._advance_local_bookmark(feed, observation.resume_position)
            return True

        current_status = result["current_status"]
        current_worker = result["current_worker"]
        current_fencing_token = result["current_fencing_token"]
        if current_status == "active" and (
            current_worker != worker_id
            or current_fencing_token != fencing_token
        ):
            logger.critical(
                "Fence violation on source observation for feed %s -- terminating",
                feed["name"],
            )
            logging.shutdown()
            os._exit(1)

        logger.info(
            "Source observation ignored because feed is no longer active "
            "under this lease",
            extra={
                "json_fields": {
                    "feed_id": str(feed["id"]),
                    "current_status": current_status,
                    "current_worker": (
                        str(current_worker)
                        if current_worker is not None
                        else None
                    ),
                    "current_fencing_token": current_fencing_token,
                },
            },
        )
        return False

    async def _process_feed(  # noqa: PLR0911, PLR0912, PLR0915
        self,
        feed: LeasedFeed,
    ) -> None:
        """
        Run the capture-upload-bookmark pipeline for a single feed.

        Iterates the capture generator, uploads each chunk to GCS, and
        bookmarks progress with fence violation detection.
        """
        if (
            not feed.get("id")
            or not str(feed["id"]).strip()
            or str(feed["id"]).strip() == "None"
        ):
            msg = f"Leased feed '{feed.get('name')}' missing required id"
            logger.error(msg)
            raise ValueError(msg)

        chunk_seq = 0
        worker_id = self._collector_settings.worker_id
        fencing_token = feed["fencing_token"]
        _fallback_session_id: str | None = None
        topic_path = self._get_pubsub_topic_path(feed)

        if feed["source_type"] == SourceType.OPENMHZ:
            extension = "m4a"
            content_type = "audio/mp4"
        elif feed["source_type"] in (
            SourceType.ECHO,
            SourceType.FIRE_NOTIFICATIONS,
        ):
            extension = "mp3"
            content_type = "audio/mpeg"
        elif feed["source_type"] in (
            SourceType.BCFY_FEEDS,
            SourceType.BCFY_CALLS,
        ):
            extension = "flac"
            content_type = "audio/flac"
        else:
            msg = f"Unhandled source type: {feed['source_type']}"
            raise ValueError(msg)

        try:
            async for capture_event in self._capture_fn(
                feed,
                self._shutdown,
                self._capture_resources,
            ):
                if isinstance(capture_event, SourceObservation):
                    should_continue = await self._process_source_observation(
                        feed,
                        capture_event,
                        worker_id,
                        fencing_token,
                    )
                    if not should_continue:
                        return
                    if self._shutdown.is_set():
                        return
                    continue

                if not isinstance(capture_event, CapturedChunk):
                    msg = (
                        f"Collector yielded "
                        f"{type(capture_event).__name__}, "
                        f"expected CapturedChunk or SourceObservation"
                    )
                    raise TypeError(msg)  # noqa: TRY301
                captured_chunk = capture_event
                tracer = trace.get_tracer(__name__)
                with tracer.start_as_current_span("process_captured_chunk"):
                    chunk_extension = extension
                    chunk_content_type = content_type

                    if captured_chunk.mime_type is not None:
                        mime_map = {
                            AudioMimeType.MPEG: ("mp3", "audio/mpeg"),
                            AudioMimeType.AAC: ("aac", "audio/aac"),
                            AudioMimeType.WAV: ("wav", "audio/x-wav"),
                            AudioMimeType.FLAC: ("flac", "audio/flac"),
                            AudioMimeType.MP4: ("m4a", "audio/mp4"),
                            AudioMimeType.OGG: ("ogg", "audio/ogg"),
                        }
                        if captured_chunk.mime_type in mime_map:
                            chunk_extension, chunk_content_type = mime_map[
                                captured_chunk.mime_type
                            ]

                    # Bump chunk_seq up-front for every captured chunk, success or
                    # failure. Reusing a seq across chunks is unsafe: combined with
                    # second-precision upload timestamps and the 412-treated-as-
                    # success path, a same-second retry could publish the next
                    # chunk's metadata pointing at the previous chunk's audio bytes.
                    seq_for_chunk = chunk_seq
                    chunk_seq += 1
                    await self._process_captured_chunk(
                        feed,
                        captured_chunk,
                        seq_for_chunk,
                        worker_id,
                        fencing_token,
                        topic_path,
                        chunk_extension,
                        chunk_content_type,
                    )

                    if self._shutdown.is_set():
                        # Return without calling release_feed -- _shutdown_sequence
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

        except LeaseExpiredError:
            # Confirmed lease loss. Do NOT attempt report_feed_failure:
            # fenced ownership is gone, so another worker owns recovery.
            logger.warning(
                "Lease lost for feed %s -- aborting without DB write",
                feed["name"],
            )
            return

        except FeedFailure as e:
            action = failure_policy.classify_failure_policy(e.status_reason)
            if (
                action
                is failure_policy.ExecutedAction.INCREMENT_FEED_FAILURE_BUDGET
            ):
                await self._record_feed_failure(
                    feed,
                    worker_id,
                    fencing_token,
                    reason=e.reason,
                    status_reason=e.status_reason,
                )
            else:
                await self._record_non_budgeted_failure(
                    feed,
                    worker_id,
                    fencing_token,
                    reason=e.reason,
                    status_reason=e.status_reason,
                )
            return

        except _PipelineFailure as e:
            action = failure_policy.classify_failure_policy(e.status_reason)
            replay_missing = (
                action
                is failure_policy.ExecutedAction.RECORD_POST_BOOKMARK_PUBLISH_GAP
            )
            if (
                action
                is failure_policy.ExecutedAction.INCREMENT_FEED_FAILURE_BUDGET
            ):
                await self._record_feed_failure(
                    feed,
                    worker_id,
                    fencing_token,
                    reason=e.reason,
                    status_reason=e.status_reason,
                    replay_missing=replay_missing,
                    data_gap_known=replay_missing,
                )
            else:
                await self._record_non_budgeted_failure(
                    feed,
                    worker_id,
                    fencing_token,
                    reason=e.reason,
                    status_reason=e.status_reason,
                    replay_missing=replay_missing,
                    data_gap_known=replay_missing,
                )
            return

        except Exception as e:
            # Transitional catch-all for bugs or untyped collector failures.
            # Source-specific attribution belongs in collectors that raise
            # FeedFailure; the runtime only records the explicit fallback.
            reason = quarantine_reason.exception_text(e)
            await self._record_non_budgeted_failure(
                feed,
                worker_id,
                fencing_token,
                reason=reason,
                status_reason=FeedStatusReason.SYSTEM_UNEXPECTED_ERROR,
            )
            return

        # Normal completion (capture generator exhausted)
        # SAFETY: same _releasing_feeds invariant as above.
        self._releasing_feeds.add(feed["id"])
        try:
            await self._store.release_feed(feed["id"], worker_id, fencing_token)
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
        interval = self._collector_settings.heartbeat_interval_sec
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
                    timeout=self._collector_settings.heartbeat_stall_timeout_sec,
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
                    self._collector_settings.heartbeat_stall_timeout_sec,
                )
                logging.shutdown()  # flush before os._exit bypasses handlers
                os._exit(1)
            except Exception:
                # Transient DB error -- log and retry. A failed renewal
                # attempt does not prove confirmed lease loss; fenced writes
                # and heartbeat diagnostics remain authoritative.
                logger.exception("Heartbeat renewal error")

            # Advance ticker. If cycle took longer than one interval, the next
            # sleep_time clamps to 0 — fires immediately to catch up.
            next_tick += interval

    def _rss_watchdog_loop(self, limit_bytes: int) -> None:  # noqa: PLR0912
        """
        OS daemon thread: poll cgroup RSS, gate _paused_for_memory, trip _shutdown.

        Mirrors the heartbeat-loop pattern (no asyncio primitives — this is
        an OS thread). After 3 consecutive samples >= pause_threshold, sets
        _paused_for_memory so the leasing loop stops claiming new feeds.
        After 3 consecutive samples >= exit_threshold, triggers graceful
        shutdown via _loop.call_soon_threadsafe(_shutdown.set) AND sets
        _thread_stop — the latter causes the next loop iteration to exit
        via the `while not self._thread_stop.is_set()` check, giving us
        the single-trip semantics required by PITFALLS Pitfall 1.

        NEVER calls os._exit(1) — that path is reserved for fence violations
        and the stall watchdog. Kernel OOM is the backstop for fast leaks
        (REQUIREMENTS WATCHDOG-01 explicit).

        Logging is transition-only (D-25): startup, pause-set, pause-clear,
        trip. No periodic-poll logging — at 2s polling, 1800 lines/hour
        would dominate Cloud Logging quota.
        """
        s = self._collector_settings
        poll_interval = s.rss_watchdog_poll_interval_sec
        pause_threshold = s.rss_watchdog_pause_threshold
        exit_threshold = s.rss_watchdog_exit_threshold
        # Hysteresis margin is hard-coded as pause_threshold - 0.10 per D-20
        # — exposing it as a setting invites the 5pp temptation (D-08).
        resume_threshold = pause_threshold - 0.10
        pause_consec_target = s.rss_watchdog_pause_consecutive_samples
        exit_consec_target = s.rss_watchdog_exit_consecutive_samples
        warmup_sec = s.rss_watchdog_warmup_sec
        watchdog_start = time.monotonic()

        pause_consec = 0
        exit_consec = 0
        # One-shot flag for "OSError reading cgroup mid-run" — defensive
        # pause + single ERROR log (CONTEXT.md "Specifics"). We do NOT
        # trip exit on read failures (heavier action than failure mode
        # warrants).
        usage_read_error_logged = False

        # D-21 startup log line (mandatory per ROADMAP SC#1).
        logger.info(
            "RSS watchdog limit = %d MB (cgroup detected)",
            limit_bytes // (1024 * 1024),
        )

        while not self._thread_stop.is_set():
            if self._thread_stop.wait(timeout=poll_interval):
                break

            # Warmup grace — D-11 + D-32: counters do NOT increment during
            # warmup. Orthogonal to the consecutive-sample debounce; both
            # protect against different failure modes.
            if (time.monotonic() - watchdog_start) < warmup_sec:
                continue

            usage = self._resolve_container_memory_usage_bytes()
            if usage is None:
                # Defensive pause + single ERROR log (CONTEXT.md "Specifics":
                # better to pause than to silently disable; do NOT trip exit).
                if not self._paused_for_memory.is_set():
                    self._paused_for_memory.set()
                if not usage_read_error_logged:
                    logger.error(
                        "RSS watchdog: failed to read cgroup memory.current "
                        "— defensively pausing claims",
                    )
                    usage_read_error_logged = True
                continue

            # A successful read clears the one-shot error-log gate so a
            # later transient failure logs again.
            usage_read_error_logged = False

            ratio = usage / limit_bytes

            # Exit-threshold debounce (D-09 / D-31).
            if ratio >= exit_threshold:
                exit_consec += 1
            else:
                exit_consec = 0

            if exit_consec >= exit_consec_target:
                logger.error(
                    "RSS watchdog: %.1f%% >= exit threshold for %d samples "
                    "— initiating graceful shutdown",
                    ratio * 100.0,
                    exit_consec,
                )
                # D-18: trip path sets BOTH _thread_stop (so the watchdog
                # exits its own loop on the next iteration) AND
                # _shutdown via call_soon_threadsafe (so the asyncio main
                # loop sees the shutdown event). NEVER os._exit(1) —
                # PITFALLS Pitfall 1 + REQUIREMENTS WATCHDOG-01.
                self._loop.call_soon_threadsafe(self._shutdown.set)
                self._thread_stop.set()
                # Single-trip: the next iteration's `while not
                # self._thread_stop.is_set()` will be False, exiting the
                # loop. No explicit guard needed here.
                continue

            # Pause-set debounce (D-09 / D-30).
            if ratio >= pause_threshold:
                pause_consec += 1
            else:
                pause_consec = 0

            if (
                pause_consec >= pause_consec_target
                and not self._paused_for_memory.is_set()
            ):
                self._paused_for_memory.set()
                logger.warning(
                    "RSS watchdog: %.1f%% >= pause threshold for %d samples "
                    "— pausing claims",
                    ratio * 100.0,
                    pause_consec,
                )

            # Pause-clear (D-10): single sample below the hysteresis floor.
            # No separate consecutive-sample counter — the 10pp band already
            # absorbs noise.
            if self._paused_for_memory.is_set() and ratio < resume_threshold:
                self._paused_for_memory.clear()
                logger.info(
                    "RSS watchdog: %.1f%% — resuming claims",
                    ratio * 100.0,
                )

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
        # Stamp at dispatch, NOT on DB success. /healthz gate 1 is a
        # local-process-health check (spec: "must not check AlloyDB
        # connectivity"); stamping post-DB would make a transient AlloyDB
        # outage age every worker's stamp in lockstep, causing the autohealer
        # to recreate the entire fleet simultaneously — a thundering herd
        # that makes recovery worse. Stamping here proves the event loop
        # dispatched the cycle, which is the true local-health signal.
        self._health_state.last_heartbeat_tick = time.monotonic()

        active = dict(self._feed_tasks.items())
        if not active:
            return

        results: list[
            HeartbeatResult
        ] = await self._heartbeat_store.renew_heartbeats_batch_diagnostic(
            list(active.keys()),
            self._collector_settings.worker_id,
        )

        # Retention signal is now DB-authoritative worker ownership, not
        # `renewed`. Skip-if-recent (feed_queries.py
        # RENEW_HEARTBEATS_BATCH_DIAGNOSTIC_SQL) short-circuits the UPDATE
        # when last_heartbeat is already <15 s fresh — those rows return
        # with renewed=False but current_worker=us. Using `renewed` as the
        # gate would then fire spurious fence-violation os._exit(1) on
        # every newly-claimed feed whose first heartbeat tick falls inside
        # the fresh window. The correct invariant is: a feed is retained
        # iff the DB row still shows our worker_id.
        #
        # Race tolerance: `current_worker` is a point-in-time read from
        # within a `SELECT ... FOR UPDATE` CTE, so it is consistent at
        # that transaction's boundary. Between this check and a later
        # fenced write by the task itself (progress / release / failure),
        # another party could in theory steal the lease — but that path
        # requires the pg_cron sweep (30 s cadence, 60 s abandonment
        # threshold) OR another worker's recovery-CTE to fire, neither
        # of which can happen while our last_heartbeat stays < 60 s old.
        # Fenced writes on the task side are the authoritative lease-lost
        # detection at write time; they call update_feed_progress /
        # release_feed / report_feed_failure / release_non_budgeted_failure
        # with WHERE worker_id=$1 AND fencing_token=$2 and return False if
        # our lease was taken. The
        # existing retry_with_lease_check wrapper + _lease_lost event
        # gracefully handles that outcome without depending on the
        # heartbeat's retained_ids observation.
        worker_id = self._collector_settings.worker_id
        retained_ids = {
            r["id"] for r in results if r["current_worker"] == worker_id
        }

        # Cancel tasks for feeds that have been deactivated.
        for r in results:
            if (
                r["current_status"] == "deactivated"
                and r["current_worker"] == worker_id
            ):
                fid = r["id"]
                if fid in active and not active[fid].done():
                    logger.info(
                        "Feed %s deactivated; cancelling task",
                        fid,
                        extra={
                            "json_fields": {
                                "event_type": "feed_deactivated_task_cancelled",
                                "feed_id": str(fid),
                            },
                        },
                    )
                    active[fid].cancel()

        # A feed missing from retained_ids is NOT a violation if:
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
            if fid not in retained_ids
            and not active[fid].done()
            and fid not in self._releasing_feeds
        }
        if not lost_ids:
            logger.debug(
                "Heartbeat retained %d feeds (%d actually written)",
                len(retained_ids),
                sum(1 for r in results if r["renewed"]),
            )
            return

        # Log diagnostic details for each lost feed before terminating.
        diag_by_id = {r["id"]: r for r in results}
        for fid in lost_ids:
            diag = diag_by_id.get(fid)
            if diag:
                logger.critical(
                    "Feed %s lost: current_worker=%s current_status=%s renewed=%s",
                    fid,
                    diag["current_worker"],
                    diag["current_status"],
                    diag["renewed"],
                )
            else:
                logger.critical(
                    "Feed %s lost: no DB row returned (deleted from database?)",
                    fid,
                )

        logger.critical(
            "Heartbeat fence violation -- %d feed(s) lost: %s. Terminating.",
            len(lost_ids),
            ", ".join(str(fid) for fid in lost_ids),
        )
        # Belt-and-suspenders: signal retry loops in the narrow window
        # before process death.
        self._lease_lost.set()
        logging.shutdown()  # flush before os._exit bypasses handlers
        os._exit(1)

    # -- Shutdown sequence ------------------------------------------------

    async def _shutdown_sequence(self) -> None:  # noqa: PLR0912
        """
        Orderly teardown: stop /healthz HTTP server, cancel feed tasks,
        stop heartbeat thread, close GCS client and database pools.
        """
        logger.info(
            "Shutting down -- %d active feed tasks",
            len(self._feed_tasks),
        )
        # Stop the /healthz listener early so external probes get a clean
        # connection refusal (port closed) rather than hanging on a socket
        # whose event loop is about to drain.
        # Wrapped in try/except so a cleanup failure here doesn't skip the
        # heartbeat-thread stop and lease release below — the process is
        # exiting anyway, but we still want to drop leases so another worker
        # can pick them up quickly.
        if self._health_runner is not None:
            try:
                await self._health_runner.cleanup()
            except Exception:
                logger.warning(
                    "Failed to cleanly shut down /healthz server — continuing",
                    exc_info=True,
                )

        # Stop the heartbeat thread before cancelling feed tasks: once feeds
        # are released the heartbeat would otherwise see them as fence
        # violations during the teardown window.
        self._thread_stop.set()

        # asyncio.to_thread avoids blocking the event loop during join().
        # Without this, .join() blocks the event loop thread, but the
        # heartbeat thread may be waiting for _heartbeat_cycle() to run ON
        # that event loop — a deadlock that wastes shutdown budget until
        # join() times out.
        if (
            self._heartbeat_thread is not None
            and self._heartbeat_thread.is_alive()
        ):
            await asyncio.to_thread(self._heartbeat_thread.join, timeout=5)

        # Stop watchdog thread AFTER heartbeat (so a watchdog trip doesn't
        # cause the heartbeat to observe in-flight release_feeds_batch as
        # fence violations) and BEFORE task cancellation (so a stuck
        # cgroup read can't race with closing pools).
        # 3s join (vs heartbeat's 5s): watchdog body has no DB I/O; only a
        # path-stuck-on-cgroup-read could legitimately stall, and that's
        # rare enough that 3s is generous.
        if (
            self._rss_watchdog_thread is not None
            and self._rss_watchdog_thread.is_alive()
        ):
            await asyncio.to_thread(self._rss_watchdog_thread.join, timeout=3)

        # Cancel all feed tasks
        for task in self._feed_tasks.values():
            task.cancel()
        if self._feed_tasks:
            _done, pending = await asyncio.wait(
                self._feed_tasks.values(),
                timeout=self._collector_settings.task_cancel_budget_sec,
            )
            if pending:
                # PITFALLS.md Pitfall 9: asyncio.wait does NOT cancel
                # pending tasks at timeout. The task.cancel() loop above
                # only requested cancellation; tasks that swallow
                # CancelledError or are mid-CPU-bound work are still
                # alive after the sub-timeout. We MUST explicitly
                # re-cancel and re-await with a short hard timeout, or
                # pending tasks will observe a NULL worker_id after
                # release_feeds_batch and trigger the heartbeat
                # fence-violation os._exit(1) (pre-existing invariant 1
                # in PITFALLS.md).
                #
                # Bounded log: count + sorted feed names only (NEVER
                # full task repr). On an 800-feed shutdown, formatting
                # `pending` directly expands to >1MB of Cloud Logging
                # output (Pitfall 9 critical detail).
                names = sorted(t.get_name() for t in pending)
                logger.warning(
                    "Sub-timeout: %d tasks still running after %ss — "
                    "explicitly cancelling and proceeding to release. "
                    "names=%s",
                    len(pending),
                    self._collector_settings.task_cancel_budget_sec,
                    names,
                )
                for task in pending:
                    task.cancel()
                # Hard 2s settle so cancellation actually propagates
                # before release_feeds_batch runs. Tasks that don't
                # honor cancellation within 2s are abandoned —
                # release_feeds_batch is the safety net.
                await asyncio.wait(pending, timeout=2.0)

        # We MUST release leases AFTER cancelling tasks and waiting for them to
        # exit! If we release leases while tasks are still running, they might
        # see their lease as NULL during a progress update and trigger an
        # accidental fence violation (os._exit(1)).
        #
        # Single WHERE worker_id = $1 UPDATE: catches every row the DB
        # thinks we own, including stragglers from earlier per-feed
        # release_feed failures. The per-type CTE + SKIP LOCKED on the
        # claim side makes concurrent polling after a fleet-wide UNCLAIMED
        # flip self-balancing across surviving workers, so the
        # batched+jittered stagger the scaling plan originally prescribed
        # is no longer needed.
        worker_id = self._collector_settings.worker_id
        logger.info(
            "Releasing all leases owned by worker %s",
            worker_id,
        )
        try:
            total_released = await self._store.release_feeds_batch(worker_id)
            logger.info("Released %d leases", total_released)
        except Exception:
            logger.exception(
                "Failed to release feeds on shutdown (safety net will recover)"
            )

        await self._pubsub_client.close()
        await self._gcs_client.close()

        # Close runtime-owned aiohttp session AFTER _gcs_client.close()
        # per shutdown-ordering invariant (PITFALLS.md Pitfall 4 — close
        # session after asyncio.wait of feed tasks; placing alongside
        # _gcs_client.close() satisfies that). The 250ms sleep gives SSL
        # transports time to flush so we don't emit
        # "ResourceWarning: unclosed transport" (PITFALLS.md Pitfall 12 —
        # enable_cleanup_closed is dead on Python 3.13.1+).
        if self._http_session is not None:
            await self._http_session.close()
            await asyncio.sleep(0.25)

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
