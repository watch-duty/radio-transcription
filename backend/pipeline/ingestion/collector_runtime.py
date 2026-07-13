from __future__ import annotations

import asyncio
import concurrent.futures
import dataclasses
import datetime
import logging
import os
import random
import signal
import socket
import threading
import time
import types
import typing
import uuid
from collections.abc import AsyncIterator, Callable, Mapping
from pathlib import Path

import aiohttp
import asyncpg
import uvloop
from aiohttp import web
from google.api_core import exceptions as google_exceptions
from google.cloud.pubsub_v1.publisher import exceptions as pubsub_exceptions

from backend.pipeline.common import gcp_helper, tracing_utils
from backend.pipeline.common.actor_identity import (
    resolve_runtime_service_actor_id,
)
from backend.pipeline.common.clients import gcs_client, pubsub_client
from backend.pipeline.common.log_helper import setup_asyncio_logging
from backend.pipeline.common.tracing_utils import setup_tracing
from backend.pipeline.ingestion import (
    failure_policy,
    feed_grant_control,
    feed_work_scheduler,
    grant_control,
    grant_supervisor,
    health_server,
    memory_watchdog,
    quarantine_telemetry,
    sid_grant_control,
    source_runtime_specs,
    status_reason_detail,
    worker_profiles,
)
from backend.pipeline.ingestion.collectors.bcfy_calls import (
    provider as bcfy_calls_provider,
)
from backend.pipeline.ingestion.collectors.bcfy_calls import (
    runtime_adapters as bcfy_calls_runtime_adapters,
)
from backend.pipeline.ingestion.collectors.bcfy_calls import (
    sid_processor as bcfy_calls_sid_processor,
)
from backend.pipeline.ingestion.collectors.bcfy_calls import (
    sid_runner as bcfy_calls_sid_runner,
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
from backend.pipeline.ingestion.slo_contract import EVENT_TYPE_CHUNK_INGESTED
from backend.pipeline.storage import feed_lifecycle, ingestion_lease_store
from backend.pipeline.storage.connection import (
    close_pool,
    create_pool_with_retry,
)
from backend.pipeline.storage.feed_store import (
    FeedGrant,
    FeedStatusReason,
    FeedStore,
    LeasedFeed,
    SourceObservationResult,
    SourceType,
)

if typing.TYPE_CHECKING:
    from backend.pipeline.ingestion.settings import CollectorSettings

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
INGESTION_IO_MAX_WORKERS = 512
_UUID_INT_RANGE = 1 << 128


class _CommittedMutationCancelled(BaseException):
    """A committed-stage child stopped without a knowable outcome."""


async def _settle_committed_awaitable[ResultT](
    awaitable: typing.Awaitable[ResultT],
) -> ResultT:
    """Defer caller cancellation until committed mutation has settled."""
    task = asyncio.ensure_future(awaitable)
    while True:
        try:
            return await asyncio.shield(task)
        except asyncio.CancelledError as exc:
            if not task.done():
                continue
            if task.cancelled():
                msg = "committed mutation was cancelled with unknown outcome"
                raise _CommittedMutationCancelled(msg) from exc
            return task.result()


def _bounded_jitter(max_sec: float) -> float:
    """Return bounded non-cryptographic scheduling jitter."""
    if max_sec <= 0.0:
        return 0.0
    return random.uniform(0.0, max_sec)  # noqa: S311 -- scheduling jitter


def _deterministic_startup_stagger(
    worker_id: uuid.UUID,
    max_sec: float,
) -> float:
    """Map a worker UUID into a stable delay in ``[0, max_sec)``."""
    if max_sec <= 0.0:
        return 0.0
    # UUIDs are already uniformly distributed over the 128-bit integer range.
    # Scaling directly keeps the delay stable without depending on a PRNG
    # algorithm for deterministic scheduling.
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
    """Advance the heartbeat ticker without creating catch-up write storms.

    One immediate catch-up tick is useful after a moderately slow cycle because
    it preserves the intended long-term heartbeat cadence. If the loop is more
    than one interval behind, reset from ``now`` so recovery does not spin
    back-to-back heartbeat DB writes while trying to replay missed ticks.
    """
    advanced = next_tick + interval
    if now - advanced > interval:
        return now + interval
    return advanced


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


def _is_leased_feed_payload(
    value: object,
) -> typing.TypeGuard[LeasedFeed]:
    """Validate every runtime field required by the legacy Feed runner."""
    if not isinstance(value, Mapping):
        return False
    payload = typing.cast("Mapping[str, object]", value)
    required_fields = {
        "id",
        "name",
        "source_type",
        "last_processed_filename",
        "last_bookmark_time",
        "fencing_token",
        "failure_count",
        "status_reason",
        "source_feed_id",
    }
    if not required_fields.issubset(payload):
        return False
    fencing_token = payload["fencing_token"]
    failure_count = payload["failure_count"]
    tags = payload.get("tags")
    if tags is not None:
        if not isinstance(tags, list):
            return False
        for tag in tags:
            if not isinstance(tag, dict):
                return False
            if not all(
                isinstance(key, str) and isinstance(item, str)
                for key, item in tag.items()
            ):
                return False
    return (
        isinstance(payload["id"], uuid.UUID)
        and isinstance(payload["name"], str)
        and isinstance(payload["source_type"], SourceType)
        and (
            payload["last_processed_filename"] is None
            or isinstance(payload["last_processed_filename"], str)
        )
        and (
            payload["last_bookmark_time"] is None
            or isinstance(payload["last_bookmark_time"], datetime.datetime)
        )
        and not isinstance(fencing_token, bool)
        and isinstance(fencing_token, int)
        and fencing_token >= 0
        and not isinstance(failure_count, bool)
        and isinstance(failure_count, int)
        and failure_count >= 0
        and (
            payload["status_reason"] is None
            or isinstance(payload["status_reason"], FeedStatusReason)
        )
        and (
            payload["source_feed_id"] is None
            or isinstance(payload["source_feed_id"], str)
        )
    )


class _FeedRunner:
    """Typed compatibility shell around the unchanged Feed data plane."""

    def __init__(self, runtime: CollectorRuntime) -> None:
        self._runtime = runtime

    async def run(
        self,
        grant: FeedGrant,
        payload: LeasedFeed,
        context: grant_control.RunContext,
    ) -> grant_control.RunOutcome:
        """Run one exact Feed generation until all its work is closed."""

        async def stop_capture_on_loss() -> None:
            await context.grant_lost.wait()
            context.stop_requested.set()

        loss_bridge = asyncio.create_task(stop_capture_on_loss())
        try:
            return await self._runtime._process_feed(  # noqa: SLF001
                grant,
                payload,
                context,
            )
        finally:
            loss_bridge.cancel()
            await asyncio.gather(loss_bridge, return_exceptions=True)


def _feed_authority(grant: FeedGrant) -> grant_supervisor.FeedAuthority:
    return grant_supervisor.FeedAuthority(grant.feed_id)


def _feed_owner(grant: FeedGrant) -> uuid.UUID:
    return grant.owner_worker_id


def _feed_fencing_token(grant: FeedGrant) -> int:
    return grant.fencing_token


def _is_lease_snapshot_payload(
    value: object,
) -> typing.TypeGuard[ingestion_lease_store.LeaseSnapshot]:
    return isinstance(value, ingestion_lease_store.LeaseSnapshot)


def _sid_authority(
    grant: ingestion_lease_store.LeaseGrant,
) -> grant_supervisor.SidAuthority:
    return grant_supervisor.SidAuthority(
        source_type=grant.source_type.value,
        lease_key=grant.lease_key,
    )


def _sid_owner(grant: ingestion_lease_store.LeaseGrant) -> uuid.UUID:
    return grant.owner_worker_id


def _sid_fencing_token(grant: ingestion_lease_store.LeaseGrant) -> int:
    return grant.fencing_token


class _UnconnectedCallExecutor:
    """Fail closed until Phase 6 wires the durable SID call pipeline."""

    async def execute(
        self,
        execution: feed_work_scheduler.CallExecution,
    ) -> typing.Never:
        del execution
        message = "Phase 5 call executor is unconnected"
        raise feed_work_scheduler.SchedulerIntegrityError(message)


def _create_sid_feed_work_scheduler(
    data_store: ingestion_lease_store.IngestionLeaseStore,
    actor_id: str,
) -> feed_work_scheduler.FeedWorkScheduler:
    """Create the sole SID scheduler with fenced quiet progress only."""
    return feed_work_scheduler.FeedWorkScheduler(
        _UnconnectedCallExecutor(),
        boundary_committer=(
            bcfy_calls_runtime_adapters.FencedBoundaryCommitter(
                data_store,
                actor_id=actor_id,
            )
        ),
    )


def _is_feed_work_scheduler(
    value: object,
) -> typing.TypeGuard[feed_work_scheduler.FeedWorkScheduler]:
    """Validate the narrow lifecycle contract accepted from test factories."""
    return (
        callable(getattr(value, "start", None))
        and callable(getattr(value, "open_lane", None))
        and callable(getattr(value, "close", None))
        and callable(getattr(value, "raise_if_failed", None))
        and isinstance(
            getattr(value, "integrity_failure_event", None),
            asyncio.Event,
        )
    )


@dataclasses.dataclass(frozen=True, slots=True)
class _SidRunnerResources:
    """One controlled SID runner's view of shared runtime resources."""

    data_store: ingestion_lease_store.IngestionLeaseStore
    heartbeat_store: ingestion_lease_store.IngestionLeaseStore
    calls_provider: bcfy_calls_provider.CallsProviderClient
    capture_resources: CaptureResources
    gcs_client: gcs_client.GcsClient
    pubsub_client: pubsub_client.PubSubClient
    health_state: HealthState
    shutdown: asyncio.Event
    feed_work_scheduler: feed_work_scheduler.FeedWorkScheduler


type _SidStoreFactory = Callable[
    [asyncpg.Pool],
    ingestion_lease_store.IngestionLeaseStore,
]
type _SidProviderFactory = Callable[
    [aiohttp.ClientSession, str],
    bcfy_calls_provider.CallsProviderClient,
]
type _SidRunnerFactory = Callable[
    [_SidRunnerResources],
    grant_control.GrantRunner[
        ingestion_lease_store.LeaseGrant,
        ingestion_lease_store.LeaseSnapshot,
    ],
]
type _SchedulerFactory = Callable[
    [ingestion_lease_store.IngestionLeaseStore, str],
    object,
]


class CollectorRuntime:
    """Run the exact-grant lifecycle for the selected worker profile.

    One :class:`GrantSupervisor` claims, heartbeats, runs, and finalizes every
    registered grant domain through the same lifecycle. Feed grants adapt the
    collector-provided ``capture_fn`` async generator; other domains provide
    their own grant runners. Capacity and enabled domains come from the
    immutable worker profile rather than from runtime-specific limits.

    Confirmed authority loss stops only the runner for that exact grant. An
    unknown heartbeat or control outcome fail-closes the affected domain and
    reaches the runtime through the supervisor's fatal-integrity channel; the
    runtime then performs ordered shutdown before propagating the failure.
    Only an event-loop stall that prevents heartbeat dispatch from completing
    uses immediate ``os._exit(1)`` termination.

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
        runtime_actor_id: Optional producer-owned audit actor. Defaults to the
            configured runtime service actor.
        sid_provider_factory: Narrow deterministic-test seam for the one
            shared SID Calls provider.
        scheduler_factory: Narrow deterministic-test seam for the one
            selected-SID process scheduler.

    """

    def __init__(
        self,
        capture_fn: CaptureFn,
        settings: CollectorSettings | None = None,
        runtime_actor_id: str | None = None,
        *,
        sid_store_factory: _SidStoreFactory | None = None,
        sid_provider_factory: _SidProviderFactory | None = None,
        sid_runner_factory: _SidRunnerFactory | None = None,
        scheduler_factory: _SchedulerFactory | None = None,
    ) -> None:
        if settings is None:
            from backend.pipeline.ingestion.settings import CollectorSettings  # noqa: I001, PLC0415

            settings = CollectorSettings()
        self._capture_fn = capture_fn
        self._collector_settings = settings
        self._profile_digest = worker_profiles.profile_digest(
            settings.worker_profile
        )
        self._sid_store_factory = sid_store_factory
        self._sid_provider_factory = (
            bcfy_calls_provider.CallsProviderClient
            if sid_provider_factory is None
            else sid_provider_factory
        )
        self._sid_runner_factory = sid_runner_factory
        self._scheduler_factory = (
            _create_sid_feed_work_scheduler
            if scheduler_factory is None
            else scheduler_factory
        )
        self._hostname = socket.gethostname()
        self._runtime_actor_id = (
            runtime_actor_id
            if runtime_actor_id is not None
            else resolve_runtime_service_actor_id()
        )
        # threading.Event (not asyncio.Event) — the heartbeat OS thread
        # and memory watchdog thread can't use asyncio primitives; they need a
        # shared thread-safe stop signal.
        self._thread_stop = threading.Event()
        self._memory_watchdog = memory_watchdog.MemoryWatchdog(
            settings,
            self._thread_stop,
        )
        # These fields are initialized in _main() before any method reads them.
        # Typed without None so the type checker doesn't require narrowing at
        # every usage site.  Accessing before _main() is a programming error.
        # asyncio.Event must be created inside a running event loop.
        self._shutdown: asyncio.Event = None  # type: ignore # set in _main()
        self._data_pool: asyncpg.Pool = None  # type: ignore # set in _main()
        # Dedicated 1-connection pool for heartbeat (control-plane / data-plane
        # separation). Prevents profile-sized data-plane work on the main pool
        # from starving heartbeat queries and causing false stall detection.
        self._heartbeat_pool: asyncpg.Pool = None  # type: ignore # set in _main()
        self._loop: asyncio.AbstractEventLoop = None  # type: ignore # set in _main()
        self._heartbeat_thread: threading.Thread | None = None
        self._store: FeedStore = None  # type: ignore # set in _main()
        self._heartbeat_store: FeedStore = None  # type: ignore # set in _main()
        self._sid_data_store: (
            ingestion_lease_store.IngestionLeaseStore | None
        ) = None
        self._sid_heartbeat_store: (
            ingestion_lease_store.IngestionLeaseStore | None
        ) = None
        self._sid_calls_provider: (
            bcfy_calls_provider.CallsProviderClient | None
        ) = None
        self._feed_work_scheduler: (
            feed_work_scheduler.FeedWorkScheduler | None
        ) = None
        self._supervisor: grant_supervisor.GrantSupervisor = None  # type: ignore # set in _main()
        self._feed_runner: _FeedRunner = None  # type: ignore # set in _main()
        # aiohttp ClientSession is constructed in _main() because it
        # requires a running event loop. Lifecycle: session is closed in
        # _shutdown_sequence after _gcs_client.close() with a 250ms
        # SSL-teardown sleep (Pitfall 12).
        self._http_session: aiohttp.ClientSession = None  # type: ignore # set in _main()
        self._capture_resources: CaptureResources = None  # type: ignore # set in _main()
        # Size the aiohttp connection pool to the profile's process-wide owned
        # cap so concurrent data-plane operations are not constrained by the
        # client's lower default connection limit.
        self._gcs_client = gcs_client.GcsClient(
            max_connections=(
                self._collector_settings.worker_profile.process_owned_cap
            ),
        )
        self._pubsub_client = pubsub_client.PubSubClient()
        self._health_state = HealthState(
            snapshot_provider=self._health_snapshot,
        )
        self._health_runner: web.AppRunner | None = None

    def _health_snapshot(self) -> grant_supervisor.SupervisorSnapshot:
        """Return one immutable local snapshot without control or DB I/O."""
        if self._supervisor is not None:
            return self._supervisor.snapshot()
        profile = self._collector_settings.worker_profile
        return grant_supervisor.SupervisorSnapshot(
            profile=profile.name,
            profile_digest=self._profile_digest,
            counts_by_domain=types.MappingProxyType(
                {
                    allocation.domain_id: grant_supervisor.GrantCount(
                        active=0,
                        retrying=0,
                        durable_failing=0,
                    )
                    for allocation in profile.allocations
                }
            ),
        )

    # -- Entry point ------------------------------------------------------

    def run(self) -> None:
        """
        Start the runtime. Blocks until shutdown completes.

        Uses uvloop as the event loop implementation. uvloop is a
        drop-in asyncio replacement built on libuv (C extension) that
        reduces per-callback scheduling overhead when many concurrent grant
        runners generate frequent I/O completions.
        """
        logger.info(
            "Starting CollectorRuntime worker_id=%s max_feeds=%d",
            self._collector_settings.worker_id,
            self._collector_settings.max_feeds_per_worker,
        )
        setup_tracing(service_name="ingestion-service", is_ingestion=True)
        asyncio.run(self._main(), loop_factory=uvloop.new_event_loop)

    async def _emit_feed_quarantine(
        self,
        grant: FeedGrant,
        payload: LeasedFeed,
        decision: grant_control.BudgetedFailureDecision,
    ) -> None:
        """Emit observational evidence after exact durable quarantine."""
        await quarantine_telemetry.emit_quarantine_event(
            feed_id=str(grant.feed_id),
            feed_name=payload["name"],
            source_type=payload["source_type"].value,
            reason=decision.reason or decision.status_reason.value,
            status_reason=decision.status_reason.value,
            profile=self._collector_settings.worker_profile.name,
            profile_digest=self._profile_digest,
            domain_id=grant_control.DomainId.FEED.value,
            authority_kind=worker_profiles.AuthorityKind.FEED.value,
        )

    def _sid_terminal_decision(
        self,
        outcome: grant_control.RunOutcome,
    ) -> grant_control.TerminalDecision:
        """Select one SID Lease terminal action exactly once."""
        if isinstance(outcome, grant_control.RunFailed):
            return self._failure_terminal_decision(
                outcome,
                authority_kind=worker_profiles.AuthorityKind.SID_LEASE,
                event_type="sid_failure_policy_decision",
            )
        if isinstance(outcome, grant_control.RunStopped):
            return grant_control.NeutralRelease(outcome.cause)
        return grant_control.NeutralRelease(grant_control.TerminalCause.NORMAL)

    async def _start_feed_work_scheduler(self) -> None:
        """Construct and start one shared selected-SID resource set."""
        selected_sid = any(
            allocation.domain_id is grant_control.DomainId.SID
            for allocation in self._collector_settings.worker_profile.allocations
        )
        if not selected_sid or self._feed_work_scheduler is not None:
            return
        if any(
            resource is not None
            for resource in (
                self._sid_data_store,
                self._sid_heartbeat_store,
                self._sid_calls_provider,
            )
        ):
            message = "partial SID resources exist without a scheduler"
            raise RuntimeError(message)
        store_factory = (
            self._sid_store_factory or ingestion_lease_store.IngestionLeaseStore
        )
        self._sid_data_store = store_factory(self._data_pool)
        self._sid_heartbeat_store = store_factory(self._heartbeat_pool)
        self._sid_calls_provider = self._sid_provider_factory(
            self._http_session,
            source_runtime_specs.url_base_for(SourceType.BCFY_CALLS),
        )
        if not callable(
            getattr(self._sid_calls_provider, "fetch_sid_page", None)
        ):
            message = "SID provider factory returned an invalid provider"
            raise TypeError(message)
        candidate = self._scheduler_factory(
            self._sid_data_store,
            self._runtime_actor_id,
        )
        if not _is_feed_work_scheduler(candidate):
            message = "scheduler factory returned an invalid scheduler"
            raise TypeError(message)
        self._feed_work_scheduler = candidate
        await candidate.start()

    def _compose_supervisor(self) -> None:
        """Construct only selected typed domains over the shared resources."""
        settings = self._collector_settings
        registrations: list[object] = []
        abandonment = datetime.timedelta(
            seconds=settings.abandonment_window_sec
        )
        for allocation in settings.worker_profile.allocations:
            if allocation.domain_id is grant_control.DomainId.FEED:
                self._store = FeedStore(
                    self._data_pool,
                    claim_types=list(settings.caps.keys()),
                )
                self._heartbeat_store = FeedStore(
                    self._heartbeat_pool,
                    claim_types=list(settings.caps.keys()),
                )
                feed_control = feed_grant_control.FeedGrantControl(
                    self._store,
                    self._heartbeat_store,
                    settings.caps,
                    abandonment,
                    on_quarantined=self._emit_feed_quarantine,
                )
                self._feed_runner = _FeedRunner(self)
                registrations.append(
                    grant_supervisor.RegisteredDomain(
                        domain_id=grant_control.DomainId.FEED,
                        authority_kind=worker_profiles.AuthorityKind.FEED,
                        grant_type=FeedGrant,
                        payload_validator=_is_leased_feed_payload,
                        authority_of=_feed_authority,
                        owner_of=_feed_owner,
                        fencing_token_of=_feed_fencing_token,
                        control=feed_control,
                        runner=self._feed_runner,
                        allocation=allocation,
                        terminal_decision_for=self._feed_terminal_decision,
                    )
                )
                continue

            if allocation.domain_id is not grant_control.DomainId.SID:
                msg = f"Unsupported runtime domain {allocation.domain_id}"
                raise ValueError(msg)
            scheduler = self._feed_work_scheduler
            if scheduler is None:
                msg = "Selected SID domain requires a started scheduler"
                raise RuntimeError(msg)
            data_store = self._sid_data_store
            heartbeat_store = self._sid_heartbeat_store
            calls_provider = self._sid_calls_provider
            if (
                data_store is None
                or heartbeat_store is None
                or calls_provider is None
            ):
                msg = "Selected SID domain requires shared SID resources"
                raise RuntimeError(msg)
            sid_control = sid_grant_control.SidGrantControl(
                data_store,
                heartbeat_store,
                SourceType.BCFY_CALLS,
                abandonment,
            )
            if self._sid_runner_factory is None:

                def processor_factory(
                    grant: ingestion_lease_store.LeaseGrant,
                    payload: ingestion_lease_store.LeaseSnapshot,
                    lane: feed_work_scheduler.GrantLane,
                    *,
                    _data_store: ingestion_lease_store.IngestionLeaseStore = data_store,
                    _calls_provider: bcfy_calls_provider.CallsProviderClient = calls_provider,
                ) -> bcfy_calls_sid_processor.BcfyCallsSidProcessor:
                    del payload
                    return bcfy_calls_sid_processor.BcfyCallsSidProcessor(
                        grant,
                        _data_store,
                        _calls_provider,
                        lane,
                        now=lambda: datetime.datetime.now(datetime.UTC),
                    )

                sid_runner: grant_control.GrantRunner[
                    ingestion_lease_store.LeaseGrant,
                    ingestion_lease_store.LeaseSnapshot,
                ] = bcfy_calls_sid_runner.BcfyCallsSidRunner(
                    scheduler,
                    processor_factory,
                )
            else:
                sid_runner = self._sid_runner_factory(
                    _SidRunnerResources(
                        data_store=data_store,
                        heartbeat_store=heartbeat_store,
                        calls_provider=calls_provider,
                        capture_resources=self._capture_resources,
                        gcs_client=self._gcs_client,
                        pubsub_client=self._pubsub_client,
                        health_state=self._health_state,
                        shutdown=self._shutdown,
                        feed_work_scheduler=scheduler,
                    )
                )
            if not callable(getattr(sid_runner, "run", None)):
                msg = "SID runner factory returned an invalid runner"
                raise TypeError(msg)
            registrations.append(
                grant_supervisor.RegisteredDomain(
                    domain_id=grant_control.DomainId.SID,
                    authority_kind=(worker_profiles.AuthorityKind.SID_LEASE),
                    grant_type=ingestion_lease_store.LeaseGrant,
                    payload_validator=_is_lease_snapshot_payload,
                    authority_of=_sid_authority,
                    owner_of=_sid_owner,
                    fencing_token_of=_sid_fencing_token,
                    control=sid_control,
                    runner=sid_runner,
                    allocation=allocation,
                    terminal_decision_for=self._sid_terminal_decision,
                )
            )

        self._supervisor = grant_supervisor.GrantSupervisor(
            settings.worker_profile,
            registrations,
            finalize_concurrency=settings.db.pool_max_size,
        )

    # -- Async core -------------------------------------------------------

    async def _main(self) -> None:
        """Top-level async entry: setup, run leasing loop, then shutdown."""
        self._loop = asyncio.get_running_loop()
        self._loop.set_default_executor(
            concurrent.futures.ThreadPoolExecutor(
                max_workers=INGESTION_IO_MAX_WORKERS,
                thread_name_prefix="ingestion_io",
            )
        )
        setup_asyncio_logging(self._loop)
        self._shutdown = asyncio.Event()

        def _on_signal(sig: signal.Signals) -> None:
            if not self._shutdown.is_set():
                logger.info(
                    "Received %s -- initiating graceful shutdown",
                    sig.name,
                )
                self._shutdown.set()

        # Heartbeats intentionally continue through cooperative runner drain;
        # the supervisor stops heartbeat supervision immediately before exact
        # release/finalization.
        for sig in (signal.SIGTERM, signal.SIGINT):
            self._loop.add_signal_handler(sig, _on_signal, sig)

        settings = self._collector_settings
        (
            deterministic_delay,
            random_delay,
            total_startup_delay,
        ) = _startup_pacing_delay(
            settings.worker_id,
            settings.startup_stagger_max_sec,
            settings.startup_jitter_max_sec,
        )
        startup_pacing_fields: dict[str, object] = {
            "event_type": "startup_pacing",
            "worker_id": str(settings.worker_id),
            "worker_index": settings.worker_index,
            "hostname": self._hostname,
            "deterministic_delay_sec": deterministic_delay,
            "random_delay_sec": random_delay,
            "total_delay_sec": total_startup_delay,
            "startup_stagger_max_sec": settings.startup_stagger_max_sec,
            "startup_jitter_max_sec": settings.startup_jitter_max_sec,
            "lease_admission_cycle_budget": settings.lease_admission_cycle_budget,
            "max_feeds_per_worker": settings.max_feeds_per_worker,
            "profile": settings.worker_profile.name,
            "profile_digest": self._profile_digest,
            "selected_domains": [
                allocation.domain_id.value
                for allocation in settings.worker_profile.allocations
            ],
            "process_id": os.getpid(),
        }
        logger.info(
            "Startup pacing before pool creation",
            extra={"json_fields": startup_pacing_fields},
        )
        if await self._sleep_or_shutdown(total_startup_delay):
            logger.info(
                "Startup pacing interrupted by shutdown",
                extra={
                    "json_fields": {
                        **startup_pacing_fields,
                        "event_type": "startup_pacing_interrupted",
                    },
                },
            )
            return
        logger.info(
            "Startup pacing complete",
            extra={
                "json_fields": {
                    **startup_pacing_fields,
                    "event_type": "startup_pacing_complete",
                },
            },
        )

        # command_timeout: bounds query execution on established connections.
        # timeout (connect): bounds TCP handshake — without it, a VPC subnet
        # silently dropping packets hangs connect() for 2+ min (Linux TCP
        # SYN-ACK timeout), starving the pool of connections.
        self._data_pool = await create_pool_with_retry(settings.db)

        # Dedicated 1-connection pool ensures heartbeat queries never queue
        # behind profile-sized data-plane operations on the main pool. Without
        # this, pool contention causes false stall-timeout kills.
        hb_settings = settings.db.replace(pool_min_size=1, pool_max_size=1)
        self._heartbeat_pool = await create_pool_with_retry(hb_settings)

        # WATCHDOG-01: cgroup-aware memory daemon thread. The watchdog owns
        # cgroup detection and disabled-mode logging; the runtime only wires
        # lifecycle signals.
        self._memory_watchdog.start(self._loop, self._shutdown)

        quarantine_telemetry.configure(settings.google_cloud_project)

        # try/finally wraps session creation, capture_resources, and
        # health_server.start so an exception in any of those triggers
        # _shutdown_sequence — closes the aiohttp session and joins
        # heartbeat/watchdog threads cleanly.
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
            segment_dir = (
                Path(settings.segment_temp_dir)
                if settings.segment_temp_dir
                else None
            )
            self._capture_resources = CaptureResources(
                http_session=self._http_session,
                segment_temp_dir=segment_dir,
            )

            await self._start_feed_work_scheduler()
            self._compose_supervisor()

            # Start the sole heartbeat thread only after the supervisor and
            # every selected typed registration are fully constructed.
            self._heartbeat_thread = threading.Thread(
                target=self._heartbeat_loop,
                daemon=True,
                name="heartbeat",
            )
            self._heartbeat_thread.start()

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
        self._raise_integrity_failure()

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
        waits = [asyncio.create_task(self._shutdown.wait())]
        supervisor = self._supervisor
        if supervisor is not None:
            waits.append(
                asyncio.create_task(supervisor.integrity_failure_event.wait())
            )
        scheduler = self._feed_work_scheduler
        if scheduler is not None:
            waits.append(
                asyncio.create_task(scheduler.integrity_failure_event.wait())
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
        return self._shutdown.is_set()

    def _raise_scheduler_integrity_failure(self) -> None:
        """Raise the selected scheduler's first fatal evidence, if any."""
        scheduler = self._feed_work_scheduler
        if scheduler is not None and scheduler.integrity_failure_event.is_set():
            scheduler.raise_if_failed()
            msg = "scheduler signalled integrity failure without evidence"
            raise grant_control.GrantControlIntegrityError(msg)

    def _raise_integrity_failure(self) -> None:
        """Raise deterministic process-integrity evidence for restart."""
        self._raise_scheduler_integrity_failure()
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

    def _failure_terminal_decision(
        self,
        outcome: grant_control.RunFailed,
        *,
        authority_kind: worker_profiles.AuthorityKind,
        event_type: str,
    ) -> grant_control.TerminalDecision:
        """Apply one shared direct metadata failure policy."""
        action = failure_policy.classify_failure_policy(outcome.status_reason)
        post_bookmark_gap = (
            outcome.status_reason
            is FeedStatusReason.PIPELINE_PUBLISH_AFTER_BOOKMARK_FAILED
        )
        fields: dict[str, object] = {
            "event_type": event_type,
            "authority_kind": authority_kind.value,
            "status_reason": outcome.status_reason.value,
            "reason": outcome.reason,
            "executed_action": action.value,
            "replay_missing": post_bookmark_gap,
            "data_gap_known": post_bookmark_gap,
        }
        log_message = (
            "Feed failure policy decision"
            if authority_kind is worker_profiles.AuthorityKind.FEED
            else "SID failure policy decision"
        )
        if (
            action
            is failure_policy.ExecutedAction.INCREMENT_FEED_FAILURE_BUDGET
        ):
            logger.info(
                log_message,
                extra={"json_fields": fields},
            )
            return grant_control.BudgetedFailureDecision(
                failure_threshold=(
                    self._collector_settings.feed_failure_threshold
                ),
                backoff_base_sec=feed_lifecycle.DEFAULT_BACKOFF_BASE_SEC,
                backoff_max_sec=feed_lifecycle.DEFAULT_BACKOFF_MAX_SEC,
                status_reason=outcome.status_reason,
                actor_id=self._runtime_actor_id,
                reason=outcome.reason,
            )
        retry_after = self._non_budgeted_retry_after()
        fields["retry_after"] = retry_after.isoformat()
        logger.info(
            log_message,
            extra={"json_fields": fields},
        )
        if (
            authority_kind is worker_profiles.AuthorityKind.FEED
            and post_bookmark_gap
        ):
            logger.error(
                "Post-bookmark publish failure",
                extra={
                    "json_fields": {
                        **fields,
                        "event_type": "post_bookmark_publish_failure",
                    }
                },
            )
        return grant_control.NonBudgetedFailureDecision(
            retry_after=retry_after,
            status_reason=outcome.status_reason,
            actor_id=self._runtime_actor_id,
            reason=outcome.reason,
        )

    def _feed_terminal_decision(
        self,
        outcome: grant_control.RunOutcome,
    ) -> grant_control.TerminalDecision:
        """Select one legacy-compatible terminal action exactly once."""
        if isinstance(outcome, grant_control.RunFailed):
            return self._failure_terminal_decision(
                outcome,
                authority_kind=worker_profiles.AuthorityKind.FEED,
                event_type="feed_failure_policy_decision",
            )
        if isinstance(outcome, grant_control.RunStopped):
            return grant_control.NeutralRelease(outcome.cause)
        return grant_control.NeutralRelease(grant_control.TerminalCause.NORMAL)

    # -- Leasing ----------------------------------------------------------
    async def _leasing_loop(self) -> None:
        """Run the one supervisor admission cadence until shutdown."""
        while not self._shutdown.is_set():
            if self._memory_watchdog.is_paused():
                wait_sec = (
                    self._collector_settings.rss_watchdog_poll_interval_sec
                )
            else:
                try:
                    await self._supervisor.admit_cycle(
                        self._collector_settings.worker_id
                    )
                except Exception:
                    self._raise_integrity_failure()
                    logger.exception(
                        "Grant admission failed -- will retry in %.1fs",
                        self._collector_settings.lease_poll_interval_sec,
                    )
                wait_sec = _lease_poll_sleep_seconds(
                    self._collector_settings.lease_poll_interval_sec,
                    self._collector_settings.lease_poll_jitter_max_sec,
                )
            if await self._sleep_or_shutdown(wait_sec):
                return

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
        context: grant_control.RunContext,
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
                lease_lost=context.grant_lost,
                shutdown=context.stop_requested,
                max_retries=settings.gcs_upload_max_retries,
                base_delay_sec=settings.gcs_upload_retry_base_delay_sec,
                max_delay_sec=settings.gcs_upload_retry_max_delay_sec,
                retryable=(
                    aiohttp.ClientError,
                    asyncio.TimeoutError,
                    OSError,
                ),
                operation_name="GCS upload",
                retry_state_changed=context.set_retrying,
            )
        except (asyncio.CancelledError, LeaseExpiredError):
            raise
        except Exception as exc:
            raise _PipelineFailure(
                _PIPELINE_GCS_UPLOAD_FAILED,
                status_reason=FeedStatusReason.SYSTEM_PIPELINE_ERROR,
            ) from exc

        async def _update_progress() -> bool:
            return await self._store.update_feed_progress(
                feed["id"],
                worker_id=worker_id,
                new_gcs_path=gcs_uri,
                fencing_token=fencing_token,
                # bcfy_calls supplies the API `ts` resume cursor;
                # stream collectors leave it None -> end_ts fallback.
                last_bookmark_time=(
                    captured_chunk.resume_position
                    or captured_chunk.chunk_end_time
                ),
                actor_id=self._runtime_actor_id,
            )

        duration_ms = int(
            (
                captured_chunk.chunk_end_time - captured_chunk.chunk_start_time
            ).total_seconds()
            * 1000
        )
        try:
            ok = await retry_with_lease_check(
                _update_progress,
                lease_lost=context.grant_lost,
                shutdown=context.stop_requested,
                max_retries=settings.bookmark_max_retries,
                base_delay_sec=settings.bookmark_retry_base_delay_sec,
                max_delay_sec=settings.bookmark_retry_max_delay_sec,
                retryable=(
                    asyncpg.PostgresConnectionError,
                    asyncpg.InterfaceError,
                    OSError,
                ),
                operation_name="bookmark write",
                retry_state_changed=context.set_retrying,
            )
        except (asyncio.CancelledError, LeaseExpiredError):
            raise
        except Exception as exc:
            raise _PipelineFailure(
                _PIPELINE_BOOKMARK_WRITE_FAILED,
                status_reason=FeedStatusReason.SYSTEM_PIPELINE_ERROR,
            ) from exc

        if not ok:
            context.grant_lost.set()
            msg = f"Fence violation on bookmark for feed {feed['name']}"
            raise LeaseExpiredError(msg)

        try:
            committed_stage_signal = asyncio.Event()
            message_id = await _settle_committed_awaitable(
                retry_with_lease_check(
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
                    lease_lost=committed_stage_signal,
                    shutdown=committed_stage_signal,
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
                    retry_state_changed=context.set_retrying,
                )
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
        if captured_chunk.stream_interval_lag_sec is not None:
            chunk_ingested_payload["stream_interval_lag_sec"] = round(
                captured_chunk.stream_interval_lag_sec, 2
            )
        logger.info(
            "Chunk ingested",
            extra={"json_fields": chunk_ingested_payload},
        )

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
        context: grant_control.RunContext,
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

        async def _record_observation() -> SourceObservationResult:
            return await self._store.record_source_observation(
                feed["id"],
                worker_id,
                fencing_token,
                observation.resume_position,
                actor_id=self._runtime_actor_id,
            )

        try:
            result = await retry_with_lease_check(
                _record_observation,
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
                retry_state_changed=context.set_retrying,
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
            context.grant_lost.set()
            msg = (
                f"Fence violation on source observation for feed {feed['name']}"
            )
            raise LeaseExpiredError(msg)

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
        context.grant_lost.set()
        msg = f"Source observation lost ownership for feed {feed['name']}"
        raise LeaseExpiredError(msg)

    @staticmethod
    def _log_feed_failure(
        feed: LeasedFeed,
        *,
        reason: str,
        status_reason: FeedStatusReason,
    ) -> None:
        """Log runner evidence without selecting or persisting a policy."""
        extra: dict[str, object] = {
            "json_fields": {
                "event_type": "feed_runner_failed",
                "feed_id": str(feed["id"]),
                "source_type": str(feed["source_type"]),
                "reason": reason,
                "status_reason": status_reason.value,
            }
        }
        if status_reason.owner == "source":
            logger.warning(
                "Feed source processing observation: "
                "feed=%s reason=%s status_reason=%s",
                feed["name"],
                reason,
                status_reason.value,
                extra=extra,
            )
            return
        logger.exception(
            "Feed internal processing error: "
            "feed=%s reason=%s status_reason=%s",
            feed["name"],
            reason,
            status_reason.value,
            extra=extra,
        )

    async def _process_feed(  # noqa: PLR0911, PLR0912, PLR0915
        self,
        grant: FeedGrant,
        feed: LeasedFeed,
        context: grant_control.RunContext,
    ) -> grant_control.RunOutcome:
        """
        Run the capture-upload-bookmark pipeline for a single feed.

        Iterates the capture generator, uploads each chunk to GCS, and
        bookmarks progress with fence violation detection.
        """
        if (
            grant.feed_id != feed["id"]
            or grant.owner_worker_id != self._collector_settings.worker_id
            or grant.fencing_token != feed["fencing_token"]
        ):
            msg = "Feed grant does not match its validated runner payload"
            raise grant_control.GrantControlIntegrityError(msg)
        if context.grant_lost.is_set():
            return grant_control.RunLost()
        if context.stop_requested.is_set():
            return grant_control.RunStopped(
                grant_control.TerminalCause.SHUTDOWN
            )

        chunk_seq = 0
        worker_id = grant.owner_worker_id
        fencing_token = grant.fencing_token
        try:
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
                reason = f"Unhandled source type: {feed['source_type']}"
                status_reason = (
                    FeedStatusReason.SYSTEM_RUNTIME_CONFIGURATION_INVALID
                )
                self._log_feed_failure(
                    feed,
                    reason=reason,
                    status_reason=status_reason,
                )
                return grant_control.RunFailed(status_reason, reason)
        except ValueError as exc:
            reason = status_reason_detail.exception_text(exc)
            status_reason = (
                FeedStatusReason.SYSTEM_RUNTIME_CONFIGURATION_INVALID
            )
            self._log_feed_failure(
                feed,
                reason=reason,
                status_reason=status_reason,
            )
            return grant_control.RunFailed(status_reason, reason)

        capture_iterator: AsyncIterator[CaptureEvent] | None = None
        try:
            capture_iterator = self._capture_fn(
                feed,
                context.stop_requested,
                self._capture_resources,
            )
            async for capture_event in capture_iterator:
                if isinstance(capture_event, SourceObservation):
                    should_continue = await self._process_source_observation(
                        feed,
                        capture_event,
                        worker_id,
                        fencing_token,
                        context,
                    )
                    if not should_continue:
                        return grant_control.RunStopped(
                            grant_control.TerminalCause.PLANNED_DRAIN
                        )
                    if context.stop_requested.is_set():
                        return grant_control.RunStopped(
                            grant_control.TerminalCause.SHUTDOWN
                        )
                    continue

                if not isinstance(capture_event, CapturedChunk):
                    msg = (
                        f"Collector yielded "
                        f"{type(capture_event).__name__}, "
                        f"expected CapturedChunk or SourceObservation"
                    )
                    raise TypeError(msg)  # noqa: TRY301
                captured_chunk = capture_event
                ingest_time_ms = str(
                    int(
                        (
                            captured_chunk.receipt_time
                            or datetime.datetime.now(datetime.UTC)
                        ).timestamp()
                        * 1000
                    )
                )
                with tracing_utils.with_baggage_and_span(
                    {
                        "ingest_time_ms": ingest_time_ms,
                        "feed_type": str(feed["source_type"]),
                    },
                    "process_captured_chunk",
                    __name__,
                ):
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
                        context,
                    )

                    if context.stop_requested.is_set():
                        logger.info(
                            "Shutdown -- stopping feed %s cleanly after chunk %d",
                            feed["name"],
                            chunk_seq,
                        )
                        return grant_control.RunStopped(
                            grant_control.TerminalCause.SHUTDOWN
                        )

        except asyncio.CancelledError:
            logger.info("Feed %s task cancelled", feed["name"])
            return grant_control.RunStopped(
                grant_control.TerminalCause.CANCELLATION
            )

        except LeaseExpiredError:
            logger.warning(
                "Lease lost for feed %s -- aborting without DB write",
                feed["name"],
            )
            return grant_control.RunLost()

        except FeedFailure as e:
            self._log_feed_failure(
                feed,
                reason=e.reason,
                status_reason=e.status_reason,
            )
            return grant_control.RunFailed(e.status_reason, e.reason)

        except _PipelineFailure as e:
            self._log_feed_failure(
                feed,
                reason=e.reason,
                status_reason=e.status_reason,
            )
            return grant_control.RunFailed(e.status_reason, e.reason)

        except Exception as e:
            # Transitional catch-all for bugs or untyped collector failures.
            # Source-specific attribution belongs in collectors that raise
            # FeedFailure; the runtime only records the explicit fallback.
            reason = status_reason_detail.exception_text(e)
            status_reason = FeedStatusReason.SYSTEM_UNEXPECTED_ERROR
            self._log_feed_failure(
                feed,
                reason=reason,
                status_reason=status_reason,
            )
            return grant_control.RunFailed(status_reason, reason)
        finally:
            if capture_iterator is not None:
                aclose = getattr(capture_iterator, "aclose", None)
                if not callable(aclose):
                    msg = "capture iterator must support acknowledged aclose"
                    raise grant_control.GrantControlIntegrityError(msg)
                close = typing.cast(
                    "typing.Callable[[], typing.Awaitable[None]]",
                    aclose,
                )
                await close()

        if context.grant_lost.is_set():
            return grant_control.RunLost()
        if context.stop_requested.is_set():
            return grant_control.RunStopped(
                grant_control.TerminalCause.SHUTDOWN
            )
        return grant_control.RunCompleted()

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
        causing avoidable grant abandonment and recovery churn.
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
                # GrantSupervisor owns authority decisions: it fail-closes
                # control errors and signals the runtime's fatal-integrity
                # channel. This branch records only an unexpected exception
                # that escaped that contract; the main loop remains the owner
                # of ordered shutdown and fatal propagation.
                logger.exception("Heartbeat renewal error")

            # Allow one immediate catch-up tick, but reset large drift so a
            # slow recovery window cannot spin heartbeat DB writes.
            next_tick = _advance_heartbeat_tick(
                next_tick=next_tick,
                interval=interval,
                now=time.monotonic(),
            )

    async def _heartbeat_cycle(self) -> None:
        """Dispatch one common exact-grant heartbeat cycle."""
        await self._supervisor.heartbeat_cycle(
            lambda: setattr(
                self._health_state,
                "last_heartbeat_tick",
                time.monotonic(),
            )
        )

    # -- Shutdown sequence ------------------------------------------------

    async def _stop_heartbeat_supervision(self) -> None:
        """Stop and join the sole heartbeat OS thread."""
        self._thread_stop.set()
        if (
            self._heartbeat_thread is not None
            and self._heartbeat_thread.is_alive()
        ):
            await asyncio.to_thread(self._heartbeat_thread.join, timeout=5)
            if self._heartbeat_thread.is_alive():
                logger.critical(
                    "Heartbeat thread did not stop; preserving exact grants "
                    "and shared resources for process-exit recovery"
                )
                msg = "heartbeat thread did not stop"
                raise RuntimeError(msg)

    async def _shutdown_sequence(self) -> None:
        """Drain exact grants before finalization and shared-resource close."""
        if self._health_runner is not None:
            try:
                await self._health_runner.cleanup()
            except Exception:
                logger.warning(
                    "Failed to cleanly shut down /healthz server — continuing",
                    exc_info=True,
                )

        shutdown_result: grant_supervisor.ShutdownResult | None = None
        if self._supervisor is not None:
            external_wait_sec = max(
                0.0,
                self._collector_settings.graceful_shutdown_timeout_sec
                - self._collector_settings.task_cancel_budget_sec,
            )
            shutdown_result = await self._supervisor.shutdown(
                cooperative_grace_sec=(
                    self._collector_settings.task_cancel_budget_sec
                ),
                external_stop_deadline_sec=external_wait_sec,
                stop_heartbeat_supervision=(self._stop_heartbeat_supervision),
            )
        else:
            await self._stop_heartbeat_supervision()

        await self._memory_watchdog.join(timeout_sec=3)

        if shutdown_result is not None and shutdown_result.undrained:
            logger.critical(
                "Shutdown left %d mutation-capable grant runner(s); "
                "leaving grants and shared resources for stale recovery",
                shutdown_result.undrained,
            )
            return

        scheduler = self._feed_work_scheduler
        if scheduler is not None:
            scheduler_result = await scheduler.close()
            if isinstance(scheduler_result, feed_work_scheduler.Undrained):
                self._raise_scheduler_integrity_failure()
                msg = "process scheduler did not drain"
                raise grant_control.GrantControlIntegrityError(msg)
            if scheduler_result is not None:
                msg = "process scheduler returned an invalid close result"
                raise grant_control.GrantControlIntegrityError(msg)
            self._raise_scheduler_integrity_failure()

        await self._pubsub_client.close()
        await self._gcs_client.close()

        if self._http_session is not None:
            await self._http_session.close()
            # aiohttp requires a short event-loop turn for SSL transports to
            # finish after session closure (HTTP-01 / Pitfall 12).
            await asyncio.sleep(0.25)

        if self._heartbeat_pool is not None:
            try:
                await self._heartbeat_pool.close()
            except Exception:  # noqa: S110
                pass

        if self._data_pool is not None:
            await close_pool(self._data_pool)

        logger.info("Shutdown complete")
