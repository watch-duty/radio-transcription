"""Generic runtime composition for Feed and SID ingestion grants."""

from __future__ import annotations

import asyncio
import collections.abc
import concurrent.futures
import datetime
import logging
import os
import random
import signal
import socket
import threading
import time
import uuid  # noqa: TC003 - runtime type-hint resolution
from pathlib import Path

import aiohttp
import asyncpg
import uvloop
from aiohttp import web

from backend.pipeline.common import gcp_helper, tracing_utils
from backend.pipeline.common.actor_identity import (
    resolve_runtime_service_actor_id,
)
from backend.pipeline.common.clients import gcs_client, pubsub_client
from backend.pipeline.common.log_helper import setup_asyncio_logging
from backend.pipeline.common.tracing_utils import setup_tracing
from backend.pipeline.ingestion import (
    audio_pipeline,
    failure_policy,
    feed_grant_control,
    grant_control,
    grant_supervisor,
    health_server,
    memory_watchdog,
    quarantine_telemetry,
    sid_grant_control,
    source_runtime_specs,
    status_reason_detail,
)
from backend.pipeline.ingestion.collectors.bcfy_calls import (
    pipeline as bcfy_calls_pipeline,
)
from backend.pipeline.ingestion.collectors.bcfy_calls import (
    provider as bcfy_calls_provider,
)
from backend.pipeline.ingestion.collectors.bcfy_calls import (
    sid_runner as bcfy_calls_sid_runner,
)
from backend.pipeline.ingestion.collectors.bcfy_calls import (
    work_pool as bcfy_calls_work_pool,
)
from backend.pipeline.ingestion.failure_classifiers import pubsub
from backend.pipeline.ingestion.health_server import HealthState
from backend.pipeline.ingestion.models import (
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
from backend.pipeline.storage import (
    feed_lifecycle,
    feed_store,
    ingestion_lease_store,
)
from backend.pipeline.storage.connection import (
    close_pool,
    create_pool_with_retry,
)
from backend.pipeline.storage.feed_store import (
    FeedStatusReason,
    FeedStore,
    LeasedFeed,
    SourceObservationResult,
    SourceType,
)

CaptureFn = collections.abc.Callable[
    [LeasedFeed, asyncio.Event, CaptureResources],
    collections.abc.AsyncIterator[CaptureEvent],
]

logger = logging.getLogger(__name__)

_PIPELINE_GCS_UPLOAD_FAILED = "gcs_upload_failed"
_PIPELINE_BOOKMARK_WRITE_FAILED = "bookmark_write_failed"
_NON_BUDGETED_RETRY_MIN_SEC = 5 * 60
_NON_BUDGETED_RETRY_MAX_SEC = 15 * 60
INGESTION_IO_MAX_WORKERS = 512
_UUID_INT_RANGE = 1 << 128
_HEARTBEAT_COMMAND_BUDGET_FRACTION = 0.8


def _bounded_jitter(max_sec: float) -> float:
    """Return bounded non-cryptographic scheduling jitter."""
    if max_sec <= 0.0:
        return 0.0
    return random.uniform(0.0, max_sec)  # noqa: S311


def _deterministic_startup_stagger(
    worker_id: uuid.UUID,
    max_sec: float,
) -> float:
    """Map a worker UUID into a stable delay in ``[0, max_sec)``."""
    if max_sec <= 0.0:
        return 0.0
    return (worker_id.int / _UUID_INT_RANGE) * max_sec


def _startup_pacing_delay(
    worker_id: uuid.UUID,
    startup_stagger_max_sec: float,
    startup_jitter_max_sec: float,
) -> tuple[float, float, float]:
    """Return deterministic, random, and total startup pacing delays."""
    deterministic_delay = _deterministic_startup_stagger(
        worker_id,
        startup_stagger_max_sec,
    )
    random_delay = _bounded_jitter(startup_jitter_max_sec)
    return deterministic_delay, random_delay, deterministic_delay + random_delay


def _lease_poll_sleep_seconds(
    lease_poll_interval_sec: float,
    lease_poll_jitter_max_sec: float,
) -> float:
    """Return the lease poll interval plus bounded scheduling jitter."""
    return lease_poll_interval_sec + _bounded_jitter(lease_poll_jitter_max_sec)


def _advance_heartbeat_tick(
    *,
    next_tick: float,
    interval: float,
    now: float,
) -> float:
    """Advance a heartbeat ticker without catch-up write storms."""
    advanced = next_tick + interval
    if now - advanced > interval:
        return now + interval
    return advanced


def _heartbeat_io_timeout_sec(settings: CollectorSettings) -> float:
    """Fit serial domain heartbeats inside the process stall watchdog.

    The dedicated heartbeat pool has one connection, so selected ownership
    domains renew serially. Reserve part of the watchdog for event-loop
    scheduling and result validation, then divide the remaining database
    budget equally across domains. Each domain spends one shared deadline
    across pool checkout or reconnect, query execution, and pool release.

    Args:
        settings: Validated collector and database configuration.

    Returns:
        A positive per-domain timeout bounded by the configured database
        timeout.
    """
    domain_count = len(settings.worker_profile.allocations)
    per_domain_budget = (
        settings.heartbeat_stall_timeout_sec
        * _HEARTBEAT_COMMAND_BUDGET_FRACTION
        / domain_count
    )
    configured_timeout = settings.db.command_timeout_sec
    if configured_timeout > 0.0:
        return min(configured_timeout, per_domain_budget)
    return per_domain_budget


class _PipelineFailure(Exception):
    """Post-capture side-effect failure with canonical evidence."""

    def __init__(
        self,
        reason: str,
        *,
        status_reason: FeedStatusReason,
    ) -> None:
        super().__init__(reason)
        self.reason = reason
        self.status_reason = status_reason


class _FeedRunner:
    """Adapt the existing Feed data plane to the common runner contract."""

    def __init__(self, runtime: CollectorRuntime) -> None:
        self._runtime = runtime

    async def run(
        self,
        grant: feed_store.FeedGrant,
        payload: LeasedFeed,
        context: grant_control.RunContext,
    ) -> grant_control.RunOutcome:
        """Run one exact Feed ownership generation.

        Args:
            grant: Exact Feed authority held by the supervisor.
            payload: Claimed Feed configuration for the runner.
            context: Supervisor-owned stop and authority-loss signals.

        Returns:
            The runner's closed completion, loss, or failure outcome.
        """
        return await self._runtime._process_feed(  # noqa: SLF001
            grant,
            payload,
            context,
        )


def _non_budgeted_retry_after() -> datetime.datetime:
    """Return the next retry time for a non-budgeted failure.

    Returns:
        A jittered UTC retry time within the configured runtime window.
    """
    jitter_sec = random.uniform(  # noqa: S311
        _NON_BUDGETED_RETRY_MIN_SEC,
        _NON_BUDGETED_RETRY_MAX_SEC,
    )
    return datetime.datetime.now(datetime.UTC) + datetime.timedelta(
        seconds=jitter_sec,
    )


def _leased_feed_has_failure_state(feed: LeasedFeed) -> bool:
    """Return whether a leased Feed carries durable failure evidence.

    Args:
        feed: Claimed Feed payload to inspect.

    Returns:
        Whether a source observation must clear existing failure state.
    """
    return feed["failure_count"] > 0 or feed["status_reason"] is not None


def _advance_local_bookmark(
    feed: LeasedFeed,
    resume_position: datetime.datetime | None,
) -> None:
    """Advance the in-memory Feed bookmark monotonically.

    Args:
        feed: Mutable claimed Feed payload mirrored by the runner.
        resume_position: Newly committed source position, if present.

    Returns:
        None.
    """
    if resume_position is None:
        return
    current = feed["last_bookmark_time"]
    if current is None or resume_position > current:
        feed["last_bookmark_time"] = resume_position


def _log_feed_failure(
    feed: LeasedFeed,
    status_reason: FeedStatusReason,
    reason: str,
) -> None:
    """Emit runner-level Feed failure evidence.

    Args:
        feed: Feed whose runner stopped.
        status_reason: Canonical failure classification.
        reason: Operator-facing diagnostic detail.

    Returns:
        None.
    """
    fields = {
        "event_type": "feed_runner_failed",
        "feed_id": str(feed["id"]),
        "source_type": feed["source_type"].value,
        "status_reason": status_reason.value,
        "reason": reason,
    }
    if status_reason.owner == "source":
        logger.warning(
            "Feed source processing failed: %s",
            reason,
            extra={"json_fields": fields},
        )
    else:
        logger.error(
            "Feed processing failed: %s",
            reason,
            extra={"json_fields": fields},
        )


def _feed_media_type(source_type: SourceType) -> tuple[str, str]:
    """Return the default staged-media representation for a source.

    Args:
        source_type: Feed source whose default media type is required.

    Returns:
        The staged file extension and HTTP content type.

    Raises:
        ValueError: The source type has no configured media representation.
    """
    if source_type is SourceType.OPENMHZ:
        return ("m4a", "audio/mp4")
    if source_type in (
        SourceType.ECHO,
        SourceType.FIRE_NOTIFICATIONS,
    ):
        return ("mp3", "audio/mpeg")
    if source_type in (
        SourceType.BCFY_FEEDS,
        SourceType.BCFY_CALLS,
    ):
        return ("flac", "audio/flac")
    msg = f"Unhandled source type: {source_type}"
    raise ValueError(msg)


class CollectorRuntime:
    """Run all selected ownership domains through one GrantSupervisor."""

    def __init__(
        self,
        capture_fn: CaptureFn,
        settings: CollectorSettings | None = None,
        runtime_actor_id: str | None = None,
    ) -> None:
        if settings is None:
            settings = CollectorSettings()
        self._capture_fn = capture_fn
        self._collector_settings = settings
        self._hostname = socket.gethostname()
        self._runtime_actor_id = (
            runtime_actor_id
            if runtime_actor_id is not None
            else resolve_runtime_service_actor_id()
        )
        self._failure_budget = failure_policy.ConsumeFailureBudget(
            failure_threshold=settings.feed_failure_threshold,
            backoff_base_sec=feed_lifecycle.DEFAULT_BACKOFF_BASE_SEC,
            backoff_max_sec=feed_lifecycle.DEFAULT_BACKOFF_MAX_SEC,
        )

        self._thread_stop = threading.Event()
        self._memory_watchdog = memory_watchdog.MemoryWatchdog(
            settings,
            self._thread_stop,
        )
        self._shutdown: asyncio.Event | None = None
        self._data_pool: asyncpg.Pool | None = None
        self._heartbeat_pool: asyncpg.Pool | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._heartbeat_thread: threading.Thread | None = None
        self._store: FeedStore | None = None
        self._heartbeat_store: FeedStore | None = None
        self._sid_data_store: (
            ingestion_lease_store.IngestionLeaseStore | None
        ) = None
        self._sid_heartbeat_store: (
            ingestion_lease_store.IngestionLeaseStore | None
        ) = None
        self._sid_calls_provider: (
            bcfy_calls_provider.CallsProviderClient | None
        ) = None
        self._work_pool: (
            bcfy_calls_work_pool.BcfyCallsWorkPool[
                bcfy_calls_pipeline.FeedBatch,
                bcfy_calls_pipeline.FeedBatchResult,
            ]
            | None
        ) = None
        self._supervisor: grant_supervisor.GrantSupervisor | None = None
        self._http_session: aiohttp.ClientSession | None = None
        self._capture_resources: CaptureResources | None = None
        self._gcs_client = gcs_client.GcsClient(
            max_connections=settings.max_feeds_per_worker,
        )
        self._pubsub_client = pubsub_client.PubSubClient()
        self._health_state = HealthState(
            active_feed_count=self._active_feed_count,
            active_sid_count=self._active_sid_count,
            integrity_failed=self._integrity_failed,
            bcfy_calls_authority_mode=(
                settings.bcfy_calls_authority_mode.value
            ),
        )
        self._health_runner: web.AppRunner | None = None

    def _active_feed_count(self) -> int:
        """Return the local active Feed count without storage I/O.

        Returns:
            Number of Feed grants currently supervised by this process.
        """
        supervisor = self._supervisor
        if supervisor is None:
            return 0
        return supervisor.active_count(grant_control.DomainId.FEED)

    def _active_sid_count(self) -> int:
        """Return the local active SID count without storage I/O.

        Returns:
            Number of SID grants currently supervised by this process.
        """
        supervisor = self._supervisor
        if supervisor is None:
            return 0
        return supervisor.active_count(grant_control.DomainId.SID)

    def _integrity_failed(self) -> bool:
        """Return whether the local supervisor has failed closed."""
        supervisor = self._supervisor
        return (
            supervisor is not None
            and supervisor.integrity_failure_event.is_set()
        )

    def run(self) -> None:
        """Start the runtime and block until ordered shutdown completes.

        Returns:
            None after the asynchronous runtime shuts down.
        """
        logger.info(
            "Starting CollectorRuntime worker_id=%s",
            self._collector_settings.worker_id,
        )
        setup_tracing(service_name="ingestion-service", is_ingestion=True)
        asyncio.run(self._main(), loop_factory=uvloop.new_event_loop)

    async def _emit_feed_quarantine(
        self,
        grant: feed_store.FeedGrant,
        payload: LeasedFeed,
        decision: failure_policy.FailurePersistencePlan,
    ) -> None:
        """Emit observational telemetry after durable quarantine.

        Args:
            grant: Exact Feed generation that was finalized.
            payload: Claimed Feed configuration used for telemetry identity.
            decision: Failure plan that produced quarantine.

        Returns:
            None after the quarantine event settles.
        """
        await quarantine_telemetry.emit_quarantine_event(
            feed_id=str(grant.feed_id),
            feed_name=payload["name"],
            source_type=payload["source_type"].value,
            reason=decision.reason or decision.status_reason.value,
            status_reason=decision.status_reason.value,
        )

    async def _compose_supervisor(self) -> None:
        """Create selected controls, runners, and the sole supervisor.

        Returns:
            None after every configured domain has been registered.

        Raises:
            RuntimeError: Required runtime resources are not initialized.
            ValueError: The worker profile selects an unsupported domain.
        """
        data_pool = self._data_pool
        heartbeat_pool = self._heartbeat_pool
        http_session = self._http_session
        capture_resources = self._capture_resources
        if (
            data_pool is None
            or heartbeat_pool is None
            or http_session is None
            or capture_resources is None
        ):
            msg = "runtime resources must exist before composition"
            raise RuntimeError(msg)

        settings = self._collector_settings
        abandonment = datetime.timedelta(
            seconds=settings.abandonment_window_sec
        )
        heartbeat_timeout_sec = _heartbeat_io_timeout_sec(settings)
        registrations: list[object] = []
        for allocation in settings.worker_profile.allocations:
            if allocation.domain_id is grant_control.DomainId.FEED:
                self._store = FeedStore(
                    data_pool,
                    claim_types=list(settings.feed_claim_caps),
                )
                self._heartbeat_store = FeedStore(
                    heartbeat_pool,
                    claim_types=list(settings.feed_claim_caps),
                    heartbeat_timeout_sec=heartbeat_timeout_sec,
                )
                control = feed_grant_control.FeedGrantControl(
                    self._store,
                    self._heartbeat_store,
                    settings.feed_claim_caps,
                    abandonment,
                    actor_id=self._runtime_actor_id,
                    on_quarantined=self._emit_feed_quarantine,
                )
                registrations.append(
                    grant_supervisor.RegisteredDomain(
                        domain_id=grant_control.DomainId.FEED,
                        control=control,
                        runner=_FeedRunner(self),
                    )
                )
                continue

            if allocation.domain_id is not grant_control.DomainId.SID:
                msg = f"Unsupported runtime domain {allocation.domain_id}"
                raise ValueError(msg)
            self._sid_data_store = ingestion_lease_store.IngestionLeaseStore(
                data_pool
            )
            self._sid_heartbeat_store = (
                ingestion_lease_store.IngestionLeaseStore(
                    heartbeat_pool,
                    heartbeat_timeout_sec=heartbeat_timeout_sec,
                )
            )
            self._sid_calls_provider = bcfy_calls_provider.CallsProviderClient(
                http_session,
                source_runtime_specs.url_base_for(SourceType.BCFY_CALLS),
            )
            executor = bcfy_calls_pipeline.BcfyCallsFeedBatchExecutor(
                calls_provider=self._sid_calls_provider,
                lease_store=self._sid_data_store,
                gcs_client=self._gcs_client,
                pubsub_client=self._pubsub_client,
                settings=settings,
                topic_path=resolve_topic_path(
                    SourceType.BCFY_CALLS,
                    settings,
                ),
                actor_id=self._runtime_actor_id,
            )
            work_pool = bcfy_calls_work_pool.BcfyCallsWorkPool(
                executor,
                concurrency=settings.bcfy_calls_work_concurrency,
                queue_capacity=settings.bcfy_calls_work_queue_capacity,
            )
            await work_pool.start()
            self._work_pool = work_pool
            runner = bcfy_calls_sid_runner.BcfyCallsSidRunner(
                self._sid_data_store,
                self._sid_calls_provider,
                work_pool,
                self._plan_failure,
                actor_id=self._runtime_actor_id,
            )
            control = sid_grant_control.SidGrantControl(
                self._sid_data_store,
                self._sid_heartbeat_store,
                SourceType.BCFY_CALLS,
                abandonment,
                actor_id=self._runtime_actor_id,
            )
            registrations.append(
                grant_supervisor.RegisteredDomain(
                    domain_id=grant_control.DomainId.SID,
                    control=control,
                    runner=runner,
                )
            )

        self._supervisor = grant_supervisor.GrantSupervisor(
            settings.worker_profile,
            registrations,
            finalize_concurrency=settings.db.pool_max_size,
            failure_planner=self._plan_terminal_failure,
        )

    async def _main(self) -> None:
        """Initialize resources, run admission, and shut down in order.

        Returns:
            None after ordered runtime shutdown.

        Raises:
            BaseException: Startup, supervision, or shutdown fails.
        """
        self._loop = asyncio.get_running_loop()
        self._loop.set_default_executor(
            concurrent.futures.ThreadPoolExecutor(
                max_workers=INGESTION_IO_MAX_WORKERS,
                thread_name_prefix="ingestion_io",
            )
        )
        setup_asyncio_logging(self._loop)
        self._shutdown = asyncio.Event()

        def on_signal(sig: signal.Signals) -> None:
            shutdown = self._shutdown
            if shutdown is not None and not shutdown.is_set():
                logger.info(
                    "Received %s -- initiating graceful shutdown",
                    sig.name,
                )
                shutdown.set()

        for sig in (signal.SIGTERM, signal.SIGINT):
            self._loop.add_signal_handler(sig, on_signal, sig)

        settings = self._collector_settings
        deterministic_delay, random_delay, startup_delay = (
            _startup_pacing_delay(
                settings.worker_id,
                settings.startup_stagger_max_sec,
                settings.startup_jitter_max_sec,
            )
        )
        startup_fields: dict[str, object] = {
            "event_type": "startup_pacing",
            "worker_id": str(settings.worker_id),
            "worker_index": settings.worker_index,
            "hostname": self._hostname,
            "deterministic_delay_sec": deterministic_delay,
            "random_delay_sec": random_delay,
            "total_delay_sec": startup_delay,
            "selected_domains": [
                allocation.domain_id.value
                for allocation in settings.worker_profile.allocations
            ],
            "bcfy_calls_authority_mode": (
                settings.bcfy_calls_authority_mode.value
            ),
            "process_id": os.getpid(),
        }
        logger.info(
            "Startup pacing before pool creation",
            extra={"json_fields": startup_fields},
        )
        if await self._sleep_or_shutdown(startup_delay):
            return

        try:
            self._data_pool = await create_pool_with_retry(settings.db)
            heartbeat_settings = settings.db.replace(
                pool_min_size=1,
                pool_max_size=1,
                command_timeout_sec=_heartbeat_io_timeout_sec(settings),
            )
            self._heartbeat_pool = await create_pool_with_retry(
                heartbeat_settings
            )
            self._memory_watchdog.start(self._loop, self._shutdown)
            quarantine_telemetry.configure(settings.google_cloud_project)

            self._http_session = aiohttp.ClientSession(
                connector=aiohttp.TCPConnector(
                    limit=0,
                    limit_per_host=64,
                    ttl_dns_cache=300,
                    keepalive_timeout=75,
                ),
                timeout=aiohttp.ClientTimeout(total=30, connect=10),
            )
            segment_dir = None
            if settings.segment_temp_dir:
                segment_dir = Path(settings.segment_temp_dir)
            self._capture_resources = CaptureResources(
                http_session=self._http_session,
                segment_temp_dir=segment_dir,
            )
            await self._compose_supervisor()

            self._heartbeat_thread = threading.Thread(
                target=self._heartbeat_loop,
                daemon=True,
                name="heartbeat",
            )
            self._heartbeat_thread.start()
            self._health_runner = await health_server.start(
                settings,
                self._health_state,
            )
            await self._leasing_loop()
        finally:
            await self._shutdown_sequence()
        self._raise_integrity_failure()

    async def _sleep_or_shutdown(self, seconds: float) -> bool:
        """Wait for timeout, shutdown, or fatal supervisor evidence.

        Args:
            seconds: Maximum number of seconds to wait.

        Returns:
            Whether process shutdown was requested.

        Raises:
            RuntimeError: Runtime shutdown state is not initialized.
            BaseException: The supervisor has surfaced an integrity failure.
        """
        shutdown = self._shutdown
        if shutdown is None:
            msg = "runtime shutdown event is not initialized"
            raise RuntimeError(msg)
        waits = [asyncio.create_task(shutdown.wait())]
        supervisor = self._supervisor
        if supervisor is not None:
            waits.append(
                asyncio.create_task(supervisor.integrity_failure_event.wait())
            )
        try:
            await asyncio.wait(
                waits,
                timeout=seconds,
                return_when=asyncio.FIRST_COMPLETED,
            )
        finally:
            for task in waits:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*waits, return_exceptions=True)
        self._raise_integrity_failure()
        return shutdown.is_set()

    def _raise_integrity_failure(self) -> None:
        """Raise the supervisor's first fail-closed integrity outcome.

        Returns:
            None when the supervisor has no integrity failure.

        Raises:
            BaseException: The supervisor recorded a fatal integrity outcome.
        """
        supervisor = self._supervisor
        if (
            supervisor is None
            or not supervisor.integrity_failure_event.is_set()
        ):
            return
        failure = supervisor.integrity_failure
        if failure is None:
            msg = "supervisor signalled integrity failure without evidence"
            raise grant_control.GrantControlIntegrityError(msg)
        raise failure

    def _plan_failure(
        self,
        status_reason: FeedStatusReason,
        reason: str | None,
    ) -> failure_policy.FailurePersistencePlan:
        """Apply the one shared Feed and SID failure policy.

        Args:
            status_reason: Canonical failure classification.
            reason: Optional operator-facing diagnostic detail.

        Returns:
            Materialized budgeted or non-budgeted persistence plan.
        """
        return failure_policy.plan_failure(
            status_reason,
            reason,
            budgeted=self._failure_budget,
            non_budgeted=lambda: failure_policy.RetryWithoutBudget(
                _non_budgeted_retry_after()
            ),
        )

    def _plan_terminal_failure(
        self,
        outcome: grant_control.RunFailed,
    ) -> failure_policy.FailurePersistencePlan:
        """Plan one supervisor terminal failure.

        Args:
            outcome: Closed runner failure evidence.

        Returns:
            Materialized failure persistence plan.
        """
        return self._plan_failure(outcome.status_reason, outcome.reason)

    async def _leasing_loop(self) -> None:
        """Run the sole supervisor admission cadence until shutdown.

        Returns:
            None when process shutdown is requested.

        Raises:
            RuntimeError: Runtime supervision is not initialized.
            BaseException: The supervisor surfaces an integrity failure.
        """
        shutdown = self._shutdown
        supervisor = self._supervisor
        if shutdown is None or supervisor is None:
            msg = "runtime must be composed before admission"
            raise RuntimeError(msg)

        while not shutdown.is_set():
            if self._memory_watchdog.is_paused():
                await supervisor.admit_cycle(
                    self._collector_settings.worker_id,
                    memory_paused=True,
                )
                wait_sec = (
                    self._collector_settings.rss_watchdog_poll_interval_sec
                )
            else:
                try:
                    await supervisor.admit_cycle(
                        self._collector_settings.worker_id
                    )
                except Exception:
                    self._raise_integrity_failure()
                    logger.exception(
                        "Grant admission failed -- retrying next cycle"
                    )
                wait_sec = _lease_poll_sleep_seconds(
                    self._collector_settings.lease_poll_interval_sec,
                    self._collector_settings.lease_poll_jitter_max_sec,
                )
            if await self._sleep_or_shutdown(wait_sec):
                return

    def _get_pubsub_topic_path(self, feed: LeasedFeed) -> str:
        """Return the configured Pub/Sub topic for a Feed.

        Args:
            feed: Claimed Feed whose source selects the topic.

        Returns:
            Fully qualified Pub/Sub topic path.
        """
        return resolve_topic_path(
            feed["source_type"],
            self._collector_settings,
        )

    async def _process_captured_chunk(
        self,
        feed: LeasedFeed,
        captured_chunk: CapturedChunk,
        sequence: int,
        grant: feed_store.FeedGrant,
        topic_path: str,
        extension: str,
        content_type: str,
        context: grant_control.RunContext,
    ) -> None:
        """Upload, fence progress, then fulfill the publish obligation.

        Args:
            feed: Claimed Feed receiving the audio chunk.
            captured_chunk: Captured audio and source timing metadata.
            sequence: Feed-local create-only object sequence.
            grant: Exact Feed authority fencing progress.
            topic_path: Downstream Pub/Sub topic.
            extension: Staged object file extension.
            content_type: Staged object HTTP content type.
            context: Supervisor-owned stop and authority-loss signals.

        Returns:
            None after upload, bookmark, and publication complete.

        Raises:
            LeaseExpiredError: Fenced progress rejects this grant.
            _PipelineFailure: A physical side effect exhausts retries.
            asyncio.CancelledError: The owning runner is cancelled.
        """
        settings = self._collector_settings
        # A yielded chunk may finish during cooperative stop/deactivation, but
        # confirmed authority loss must still interrupt pre-bookmark retries.
        # After the bookmark commits, publication is an unconditional
        # obligation.
        no_cooperative_stop = asyncio.Event()
        try:
            gcs_uri = await audio_pipeline.upload_staged_audio_with_retry(
                gcp_helper.upload_staged_audio,
                gcs_client=self._gcs_client,
                chunk=captured_chunk,
                feed=feed,
                settings=settings,
                sequence=sequence,
                fencing_token=grant.fencing_token,
                extension=extension,
                content_type=content_type,
                lease_lost=context.grant_lost,
                shutdown=no_cooperative_stop,
            )
        except (asyncio.CancelledError, LeaseExpiredError):
            raise
        except Exception as error:
            raise _PipelineFailure(
                _PIPELINE_GCS_UPLOAD_FAILED,
                status_reason=FeedStatusReason.SYSTEM_PIPELINE_ERROR,
            ) from error

        store = self._store
        if store is None:
            msg = "Feed store is not initialized"
            raise RuntimeError(msg)

        async def update_progress() -> bool:
            return await store.update_feed_progress(
                feed["id"],
                worker_id=grant.owner_worker_id,
                new_gcs_path=gcs_uri,
                fencing_token=grant.fencing_token,
                last_bookmark_time=(
                    captured_chunk.resume_position
                    or captured_chunk.chunk_end_time
                ),
                actor_id=self._runtime_actor_id,
            )

        try:
            applied = await retry_with_lease_check(
                update_progress,
                lease_lost=context.grant_lost,
                shutdown=no_cooperative_stop,
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
        except Exception as error:
            raise _PipelineFailure(
                _PIPELINE_BOOKMARK_WRITE_FAILED,
                status_reason=FeedStatusReason.SYSTEM_PIPELINE_ERROR,
            ) from error
        if not applied:
            context.grant_lost.set()
            msg = f"Fence violation on bookmark for feed {feed['name']}"
            raise LeaseExpiredError(msg)

        try:
            await audio_pipeline.publish_audio_chunk_after_bookmark(
                gcp_helper.publish_audio_chunk,
                pubsub_client=self._pubsub_client,
                topic_path=topic_path,
                feed_id=feed["id"],
                feed_name=feed["name"],
                source_type=feed["source_type"],
                gcs_uri=gcs_uri,
                chunk=captured_chunk,
                settings=settings,
                lease_lost=no_cooperative_stop,
                shutdown=no_cooperative_stop,
                event_logger=logger,
            )
        except asyncio.CancelledError:
            raise
        except Exception as error:
            raise _PipelineFailure(
                pubsub.publish_failure_reason(error),
                status_reason=(
                    FeedStatusReason.PIPELINE_PUBLISH_AFTER_BOOKMARK_FAILED
                ),
            ) from error

    async def _process_source_observation(
        self,
        feed: LeasedFeed,
        observation: SourceObservation,
        grant: feed_store.FeedGrant,
        context: grant_control.RunContext,
    ) -> None:
        """Persist a source observation or surface confirmed grant loss.

        Args:
            feed: Claimed Feed whose state may advance or recover.
            observation: Collector-provided source position.
            grant: Exact Feed authority fencing the mutation.
            context: Supervisor-owned stop and authority-loss signals.

        Returns:
            None after persistence or a bounded transient failure.

        Raises:
            LeaseExpiredError: Storage confirms that the grant was lost.
            asyncio.CancelledError: The owning runner is cancelled.
        """
        if (
            not _leased_feed_has_failure_state(feed)
            and observation.resume_position is None
        ):
            return
        store = self._store
        if store is None:
            msg = "Feed store is not initialized"
            raise RuntimeError(msg)

        async def record_observation() -> SourceObservationResult:
            return await store.record_source_observation(
                feed["id"],
                grant.owner_worker_id,
                grant.fencing_token,
                observation.resume_position,
                actor_id=self._runtime_actor_id,
            )

        try:
            result = await retry_with_lease_check(
                record_observation,
                lease_lost=context.grant_lost,
                shutdown=context.stop_requested,
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
            return

        if result["recorded"]:
            feed["failure_count"] = 0
            feed["status_reason"] = None
            _advance_local_bookmark(
                feed,
                observation.resume_position,
            )
            return

        context.grant_lost.set()
        current_worker = result["current_worker"]
        current_worker_id = None
        if current_worker is not None:
            current_worker_id = str(current_worker)
        logger.info(
            "Source observation rejected for feed %s",
            feed["name"],
            extra={
                "json_fields": {
                    "feed_id": str(feed["id"]),
                    "current_status": result["current_status"],
                    "current_worker": current_worker_id,
                    "current_fencing_token": result["current_fencing_token"],
                }
            },
        )
        msg = f"Source observation lost ownership for feed {feed['name']}"
        raise LeaseExpiredError(msg)

    async def _process_feed(  # noqa: PLR0911, PLR0912, PLR0915
        self,
        grant: feed_store.FeedGrant,
        feed: LeasedFeed,
        context: grant_control.RunContext,
    ) -> grant_control.RunOutcome:
        """Run one Feed capture pipeline under exact grant authority.

        Args:
            grant: Exact Feed authority held by the supervisor.
            feed: Claimed Feed configuration and mutable local cursor state.
            context: Supervisor-owned stop and authority-loss signals.

        Returns:
            Closed completion, loss, or classified failure evidence.

        Raises:
            grant_control.GrantControlIntegrityError: Grant and payload do not
                describe the same ownership generation.
            asyncio.CancelledError: The owning supervisor task is cancelled.
        """
        if (
            grant.feed_id != feed["id"]
            or grant.owner_worker_id != self._collector_settings.worker_id
            or grant.fencing_token != feed["fencing_token"]
        ):
            msg = "Feed grant does not match its runner payload"
            raise grant_control.GrantControlIntegrityError(msg)
        if context.grant_lost.is_set():
            return grant_control.RunLost()
        if context.stop_requested.is_set():
            return grant_control.RunCompleted()

        try:
            topic_path = self._get_pubsub_topic_path(feed)
            extension, content_type = _feed_media_type(feed["source_type"])
        except ValueError as error:
            reason = status_reason_detail.exception_text(error)
            status_reason = (
                FeedStatusReason.SYSTEM_RUNTIME_CONFIGURATION_INVALID
            )
            _log_feed_failure(feed, status_reason, reason)
            return grant_control.RunFailed(status_reason, reason)

        resources = self._capture_resources
        if resources is None:
            msg = "capture resources are not initialized"
            raise RuntimeError(msg)
        capture_iterator = self._capture_fn(
            feed,
            context.stop_requested,
            resources,
        )
        sequence = 0
        try:
            async for event in capture_iterator:
                if isinstance(event, SourceObservation):
                    await self._process_source_observation(
                        feed,
                        event,
                        grant,
                        context,
                    )
                    if context.grant_lost.is_set():
                        return grant_control.RunLost()
                    if context.stop_requested.is_set():
                        return grant_control.RunCompleted()
                    continue
                if not isinstance(event, CapturedChunk):
                    msg = (
                        f"Collector yielded {type(event).__name__}; expected "
                        "CapturedChunk or SourceObservation"
                    )
                    raise TypeError(msg)  # noqa: TRY301

                ingest_time_ms = str(
                    int(
                        (
                            event.receipt_time
                            or datetime.datetime.now(datetime.UTC)
                        ).timestamp()
                        * 1000
                    )
                )
                with tracing_utils.with_baggage_and_span(
                    {
                        "ingest_time_ms": ingest_time_ms,
                        "feed_type": feed["source_type"].value,
                    },
                    "process_captured_chunk",
                    __name__,
                ):
                    extension_for_chunk = extension
                    content_type_for_chunk = content_type
                    if event.mime_type is not None:
                        extension_for_chunk, content_type_for_chunk = (
                            audio_pipeline.staging_parameters(event.mime_type)
                        )
                    current_sequence = sequence
                    sequence += 1
                    await self._process_captured_chunk(
                        feed,
                        event,
                        current_sequence,
                        grant,
                        topic_path,
                        extension_for_chunk,
                        content_type_for_chunk,
                        context,
                    )
                if context.grant_lost.is_set():
                    return grant_control.RunLost()
                if context.stop_requested.is_set():
                    return grant_control.RunCompleted()
        except LeaseExpiredError:
            logger.warning("Grant lost for feed %s", feed["name"])
            return grant_control.RunLost()
        except FeedFailure as error:
            _log_feed_failure(
                feed,
                error.status_reason,
                error.reason,
            )
            return grant_control.RunFailed(
                error.status_reason,
                error.reason,
            )
        except _PipelineFailure as error:
            _log_feed_failure(
                feed,
                error.status_reason,
                error.reason,
            )
            return grant_control.RunFailed(
                error.status_reason,
                error.reason,
            )
        except asyncio.CancelledError:
            task = asyncio.current_task()
            if task is None or task.cancelling() > 0:
                raise
            if context.grant_lost.is_set():
                return grant_control.RunLost()
            if context.stop_requested.is_set():
                return grant_control.RunCompleted()
            raise
        except Exception as error:
            reason = status_reason_detail.exception_text(error)
            status_reason = FeedStatusReason.SYSTEM_UNEXPECTED_ERROR
            _log_feed_failure(feed, status_reason, reason)
            return grant_control.RunFailed(status_reason, reason)
        finally:
            close = getattr(capture_iterator, "aclose", None)
            if close is not None:
                await close()

        if context.grant_lost.is_set():
            return grant_control.RunLost()
        return grant_control.RunCompleted()

    def _heartbeat_loop(self) -> None:
        """Dispatch supervisor heartbeat cycles from the watchdog thread.

        Returns:
            None after thread stop or an unavailable event loop.
        """
        interval = self._collector_settings.heartbeat_interval_sec
        next_tick = time.monotonic() + interval
        while not self._thread_stop.is_set():
            wait_sec = max(0.0, next_tick - time.monotonic())
            if self._thread_stop.wait(timeout=wait_sec):
                return
            loop = self._loop
            if loop is None:
                logger.critical("Heartbeat loop started before event loop")
                return
            try:
                future = asyncio.run_coroutine_threadsafe(
                    self._heartbeat_cycle(),
                    loop,
                )
                future.result(
                    timeout=(
                        self._collector_settings.heartbeat_stall_timeout_sec
                    ),
                )
            except concurrent.futures.TimeoutError:
                logger.critical(
                    "Event loop stall -- heartbeat did not complete in %ds",
                    self._collector_settings.heartbeat_stall_timeout_sec,
                )
                logging.shutdown()
                os._exit(1)
            except Exception:
                logger.exception("Heartbeat renewal error")
            next_tick = _advance_heartbeat_tick(
                next_tick=next_tick,
                interval=interval,
                now=time.monotonic(),
            )

    async def _heartbeat_cycle(self) -> None:
        """Dispatch one common exact-grant heartbeat cycle.

        Returns:
            None after every registered domain settles its heartbeat batch.

        Raises:
            RuntimeError: The supervisor is not initialized.
            BaseException: A domain heartbeat cannot be safely correlated.
        """
        supervisor = self._supervisor
        if supervisor is None:
            msg = "supervisor is not initialized"
            raise RuntimeError(msg)
        await supervisor.heartbeat_cycle(
            lambda: setattr(
                self._health_state,
                "last_heartbeat_tick",
                time.monotonic(),
            )
        )

    async def _stop_heartbeat_supervision(self) -> None:
        """Stop and join the heartbeat OS thread.

        Returns:
            None once the thread has stopped or was never started.
        """
        self._thread_stop.set()
        thread = self._heartbeat_thread
        if thread is None or not thread.is_alive():
            return
        # GrantSupervisor owns the one external shutdown deadline around this
        # callback. Do not impose a shorter timeout while an in-flight heartbeat
        # is still bounded by the watchdog's stall timeout.
        await asyncio.to_thread(thread.join)

    async def _shutdown_sequence(self) -> None:
        """Drain grants before closing the queue and shared resources.

        Returns:
            None after all owned resources close in dependency order.

        Raises:
            grant_supervisor.SupervisorNotDrainedError: Owned work may still
                use shared runtime resources.
            BaseException: Resource cleanup fails after grants have drained.
        """
        if self._health_runner is not None:
            try:
                await self._health_runner.cleanup()
            except Exception:
                logger.warning(
                    "Failed to stop health server",
                    exc_info=True,
                )

        supervisor = self._supervisor
        try:
            if supervisor is None:
                await self._stop_heartbeat_supervision()
            else:
                await supervisor.shutdown(
                    cooperative_grace_sec=(
                        self._collector_settings.task_cancel_budget_sec
                    ),
                    external_stop_deadline_sec=(
                        self._collector_settings.graceful_shutdown_timeout_sec
                    ),
                    stop_heartbeat_supervision=(
                        self._stop_heartbeat_supervision
                    ),
                )
        except grant_supervisor.SupervisorNotDrainedError:
            self._thread_stop.set()
            await self._memory_watchdog.join(timeout_sec=3)
            logger.critical(
                "Supervisor did not drain; preserving shared resources"
            )
            raise

        await self._memory_watchdog.join(timeout_sec=3)
        if self._work_pool is not None:
            await self._work_pool.close()
        await self._pubsub_client.close()
        await self._gcs_client.close()

        if self._http_session is not None:
            await self._http_session.close()
            await asyncio.sleep(0.25)
        if self._heartbeat_pool is not None:
            try:
                await close_pool(self._heartbeat_pool)
            except Exception:
                # The heartbeat thread is already proven stopped, so this pool
                # cannot still protect work. Continue closing the independent
                # data pool, matching the legacy shutdown safety net.
                logger.warning(
                    "Failed to close heartbeat database pool",
                    exc_info=True,
                )
        if self._data_pool is not None:
            await close_pool(self._data_pool)
        logger.info("Shutdown complete")
