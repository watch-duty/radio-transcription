"""Process scheduler and exact-grant streaming page facade."""

# Private sibling modules deliberately compose the scheduler's closed core.
# ruff: noqa: SLF001

from __future__ import annotations

import asyncio
import collections.abc
import dataclasses
import enum
import math
import time
import typing

from backend.pipeline.ingestion.collectors.bcfy_calls import cursor_policy
from backend.pipeline.ingestion.feed_work_scheduler import (
    _boundaries,
    _shard,
    _types,
)
from backend.pipeline.storage import feed_store, ingestion_lease_store

if typing.TYPE_CHECKING:
    import uuid

_T = typing.TypeVar("_T")
_MAX_FINITE_SECONDS = float.fromhex("0x1.fffffffffffffp+1023")


class SchedulerIntegrityError(RuntimeError):
    """The process scheduler can no longer prove safe admission."""


class _LaneClosedError(RuntimeError):
    """An exact lane closed before page coverage linearized."""


def _raise_cancelled() -> typing.Never:
    raise asyncio.CancelledError


class _CloseStrength(enum.IntEnum):
    """Monotonic exact-lane lifecycle strength."""

    OPEN = 0
    DRAINING = 1
    CANCELLING = 2


@dataclasses.dataclass(frozen=True, slots=True)
class _ReadOnlySignal:
    """Read-only projection retaining one object-identical runner Event."""

    _event: asyncio.Event

    def is_set(self) -> bool:
        return self._event.is_set()

    async def wait(self) -> bool:
        return await self._event.wait()


@dataclasses.dataclass(slots=True)
class _PageEvidenceAccumulator:
    """Mutable scalars plus only the page's live work and queue stamps."""

    grant: ingestion_lease_store.LeaseGrant
    page_sequence: int
    monotonic: typing.Callable[[], float] = dataclasses.field(repr=False)
    worker_utilization_denominator: int
    admitted_record_count: int = 0
    admitted_cohort_count: int = 0
    terminal_record_count: int = 0
    replay_blocked_record_count: int = 0
    total_queue_wait_seconds: float = 0.0
    maximum_queue_wait_seconds: float = 0.0
    oldest_queue_age_seconds: float = 0.0
    pressure_wait_count: int = 0
    pressure_wait_seconds: float = 0.0
    maximum_held_count: int = 0
    maximum_queue_depth: int = 0
    maximum_worker_utilization_numerator: int = 0
    early_flush_attempt_count: int = 0
    final_flush_attempt_count: int = 0
    total_flush_latency_seconds: float = 0.0
    maximum_flush_latency_seconds: float = 0.0
    fence_rejection_count: int = 0
    member_rejection_count: int = 0
    _queued_at: dict[_types.CohortRecordIdentity, float] = dataclasses.field(
        default_factory=dict,
        repr=False,
    )
    _live_identities: dict[int, _types.CohortRecordIdentity] = (
        dataclasses.field(default_factory=dict, repr=False)
    )
    _boundary_records: dict[int, _types._BoundaryRecord] = dataclasses.field(
        default_factory=dict,
        repr=False,
    )
    _boundary_capture_open: bool = dataclasses.field(default=True, repr=False)
    _boundary_capture_complete: bool | None = dataclasses.field(
        default=None,
        repr=False,
    )
    _last_now: float | None = dataclasses.field(default=None, repr=False)

    @staticmethod
    def _bounded_duration(started: float, finished: float) -> float:
        elapsed = finished - started
        if elapsed <= 0.0:
            return 0.0
        if not math.isfinite(elapsed):
            return _MAX_FINITE_SECONDS
        return elapsed

    @staticmethod
    def _bounded_sum(current: float, increment: float) -> float:
        total = current + increment
        if not math.isfinite(total):
            return _MAX_FINITE_SECONDS
        return total

    def now(self) -> float:
        """Read one contained, nondecreasing finite clock sample."""
        try:
            raw = self.monotonic()
        except BaseException:
            if self._last_now is None:
                self._last_now = 0.0
            return self._last_now
        if isinstance(raw, bool) or not isinstance(raw, (int, float)):
            if self._last_now is None:
                self._last_now = 0.0
            return self._last_now
        try:
            sampled = float(raw)
        except (OverflowError, ValueError):
            sampled = math.nan
        if not math.isfinite(sampled):
            if self._last_now is None:
                self._last_now = 0.0
            return self._last_now
        if self._last_now is None or sampled > self._last_now:
            self._last_now = sampled
        return self._last_now

    def _observe_oldest(self, now: float) -> None:
        if not self._queued_at:
            return
        oldest = self._bounded_duration(min(self._queued_at.values()), now)
        self.oldest_queue_age_seconds = max(
            self.oldest_queue_age_seconds,
            oldest,
        )

    def _close_queue_residence(
        self,
        first_identity: _types.CohortRecordIdentity,
        now: float,
    ) -> None:
        admitted_at = self._queued_at.pop(first_identity, None)
        if admitted_at is None:
            return
        elapsed = self._bounded_duration(admitted_at, now)
        self.total_queue_wait_seconds = self._bounded_sum(
            self.total_queue_wait_seconds,
            elapsed,
        )
        self.maximum_queue_wait_seconds = max(
            self.maximum_queue_wait_seconds,
            elapsed,
        )

    def observe_registration(
        self,
        identities: tuple[_types.CohortRecordIdentity, ...],
    ) -> None:
        first_identity = identities[0]
        now = self.now()
        self._observe_oldest(now)
        if first_identity in self._queued_at:
            return
        for identity in identities:
            self._live_identities[id(identity)] = identity
        self._queued_at[first_identity] = now
        self.admitted_record_count += len(identities)
        self.admitted_cohort_count += 1

    def observe_boundary_registration(
        self,
        record: _types._BoundaryRecord,
    ) -> None:
        """Retain only an exact live boundary identity for scalar sampling."""
        if self.page_sequence not in (
            record.provisional_page_sequence,
            record.promotion_page_sequence,
        ):
            return
        self._boundary_records[id(record)] = record

    def tracks_identity(
        self,
        identity: _types.CohortRecordIdentity,
    ) -> bool:
        """Return whether ``identity`` is object-identical page admission."""
        return self._live_identities.get(id(identity)) is identity

    def tracks_boundary(self, record: _types._BoundaryRecord) -> bool:
        """Return whether ``record`` is an object-identical page boundary."""
        return self._boundary_records.get(id(record)) is record and (
            self.page_sequence
            in (
                record.provisional_page_sequence,
                record.promotion_page_sequence,
                record.aborted_page_sequence,
            )
        )

    @property
    def boundary_capture_open(self) -> bool:
        """Return whether a new boundary attempt may capture this page."""
        return self._boundary_capture_open

    @property
    def boundary_capture_complete(self) -> bool:
        """Return whether every captured attempt settled before closure."""
        return self._boundary_capture_complete is True

    def close_boundary_capture(self, *, complete: bool) -> None:
        """Close this page to new boundary evidence after quiescence."""
        if not self._boundary_capture_open:
            return
        self._boundary_capture_open = False
        self._boundary_capture_complete = complete

    def observe_dispatch(
        self,
        first_identity: _types.CohortRecordIdentity,
    ) -> None:
        now = self.now()
        self._observe_oldest(now)
        self._close_queue_residence(first_identity, now)

    def observe_terminal(
        self,
        record_count: int,
        *,
        member_rejected: bool,
    ) -> None:
        self.terminal_record_count += record_count
        if member_rejected:
            self.member_rejection_count += record_count

    def observe_neutralization(
        self,
        first_identities: tuple[_types.CohortRecordIdentity, ...],
        record_count: int,
        *,
        replay_blocked: bool,
        member_rejected: bool,
    ) -> None:
        now = self.now()
        self._observe_oldest(now)
        for first_identity in first_identities:
            self._close_queue_residence(first_identity, now)
        self.terminal_record_count += record_count
        if replay_blocked:
            self.replay_blocked_record_count += record_count
        if member_rejected:
            self.member_rejection_count += record_count

    def observe_unadmitted(
        self,
        record_count: int,
        *,
        replay_blocked: bool,
        member_rejected: bool,
    ) -> None:
        if replay_blocked:
            self.replay_blocked_record_count += record_count
        if member_rejected:
            self.member_rejection_count += record_count

    def observe_pressure_wait(self, started: float) -> None:
        elapsed = self._bounded_duration(started, self.now())
        self.pressure_wait_count += 1
        self.pressure_wait_seconds = self._bounded_sum(
            self.pressure_wait_seconds,
            elapsed,
        )

    def observe_sample(
        self,
        *,
        held: int,
        queue_depth: int,
        active_workers: int,
        queued_first_identities: tuple[
            _types.CohortRecordIdentity,
            ...,
        ],
        live_identities: tuple[_types.CohortRecordIdentity, ...],
        live_boundary_records: tuple[_types._BoundaryRecord, ...],
    ) -> None:
        now = self.now()
        self._observe_oldest(now)
        live_queue_identity_ids = {
            id(identity) for identity in queued_first_identities
        }
        for first_identity in tuple(self._queued_at):
            if id(first_identity) not in live_queue_identity_ids:
                self._close_queue_residence(first_identity, now)
        self._live_identities = {
            id(identity): identity for identity in live_identities
        }
        self._boundary_records = {
            id(record): record for record in live_boundary_records
        }
        self.maximum_held_count = max(self.maximum_held_count, held)
        self.maximum_queue_depth = max(
            self.maximum_queue_depth,
            queue_depth,
        )
        self.maximum_worker_utilization_numerator = max(
            self.maximum_worker_utilization_numerator,
            active_workers,
        )

    def observe_flush_attempt(
        self,
        *,
        final_logical: bool,
        latency_seconds: float,
    ) -> None:
        if final_logical:
            self.final_flush_attempt_count += 1
        else:
            self.early_flush_attempt_count += 1
        latency = (
            latency_seconds
            if math.isfinite(latency_seconds) and latency_seconds >= 0.0
            else 0.0
        )
        self.total_flush_latency_seconds = self._bounded_sum(
            self.total_flush_latency_seconds,
            latency,
        )
        self.maximum_flush_latency_seconds = max(
            self.maximum_flush_latency_seconds,
            latency,
        )

    def observe_fence_rejection(self) -> None:
        self.fence_rejection_count += 1

    def observe_member_rejections(
        self,
        selected: _boundaries._SelectedBoundaryBatch,
        dispositions: tuple[_types.BoundaryDisposition, ...],
    ) -> None:
        self.member_rejection_count += sum(
            disposition is _types.BoundaryDisposition.MEMBER_REJECTED
            and self.tracks_boundary(record)
            for (_shard_value, record), disposition in zip(
                selected,
                dispositions,
                strict=True,
            )
        )

    def freeze(self) -> _types.SchedulerPageEvidence:
        """Return one immutable view without retaining completed history."""
        now = self.now()
        self._observe_oldest(now)
        live_residences = tuple(
            self._bounded_duration(admitted_at, now)
            for admitted_at in self._queued_at.values()
        )
        live_total = 0.0
        for elapsed in live_residences:
            live_total = self._bounded_sum(live_total, elapsed)
        total_queue_wait = self._bounded_sum(
            self.total_queue_wait_seconds,
            live_total,
        )
        maximum_queue_wait = max(
            self.maximum_queue_wait_seconds,
            max(live_residences, default=0.0),
        )
        return _types.SchedulerPageEvidence(
            admitted_record_count=self.admitted_record_count,
            admitted_cohort_count=self.admitted_cohort_count,
            terminal_record_count=self.terminal_record_count,
            replay_blocked_record_count=self.replay_blocked_record_count,
            total_queue_wait_seconds=total_queue_wait,
            maximum_queue_wait_seconds=maximum_queue_wait,
            oldest_queue_age_seconds=self.oldest_queue_age_seconds,
            pressure_encountered=self.pressure_wait_count > 0,
            pressure_wait_count=self.pressure_wait_count,
            pressure_wait_seconds=self.pressure_wait_seconds,
            maximum_held_count=self.maximum_held_count,
            maximum_queue_depth=self.maximum_queue_depth,
            maximum_worker_utilization_numerator=(
                self.maximum_worker_utilization_numerator
            ),
            worker_utilization_denominator=(
                self.worker_utilization_denominator
            ),
            early_flush_attempt_count=self.early_flush_attempt_count,
            final_flush_attempt_count=self.final_flush_attempt_count,
            total_flush_latency_seconds=self.total_flush_latency_seconds,
            maximum_flush_latency_seconds=self.maximum_flush_latency_seconds,
            fence_rejection_count=self.fence_rejection_count,
            member_rejection_count=self.member_rejection_count,
        )


class _ObservedCallExecutor:
    """Record actual dispatch immediately before delegating execution."""

    def __init__(
        self,
        delegate: _types.CallExecutor,
        dispatch_observer: typing.Callable[[_types.CohortExecution], None],
    ) -> None:
        self._delegate = delegate
        self._dispatch_observer = dispatch_observer

    def __getattr__(self, name: str) -> object:
        """Preserve private deterministic-test inspection of the delegate."""
        return getattr(self._delegate, name)

    async def execute(
        self,
        execution: _types.CohortExecution,
    ) -> _types._ExecutorOutcome:
        try:
            self._dispatch_observer(execution)
        except BaseException:  # noqa: S110 - evidence is observational
            pass
        return await self._delegate.execute(execution)


@dataclasses.dataclass(slots=True)
class _PageBarrier:
    """Bounded admission and structural terminal state for one page."""

    grant: ingestion_lease_store.LeaseGrant
    page_sequence: int
    evidence: _PageEvidenceAccumulator
    evidence_observer: (
        typing.Callable[[_types.SchedulerPageEvidence], None] | None
    ) = None
    evidence_published: bool = False
    current_source_order: int | None = None
    pulled: int = 0
    registered: int = 0
    localized: int = 0
    total_records: int = 0
    pulled_records: int = 0
    registered_records: int = 0
    locally_rejected_records: int = 0
    terminalized_registered_records: int = 0
    registered_cohorts: dict[
        _types.CohortRecordIdentity,
        tuple[_types.CohortRecordIdentity, ...],
    ] = dataclasses.field(default_factory=dict)
    terminal_facts: dict[
        _types.CohortRecordIdentity,
        _types.CohortTerminalFacts,
    ] = dataclasses.field(default_factory=dict)
    neutralized_identities: set[_types.CohortRecordIdentity] = (
        dataclasses.field(default_factory=set)
    )
    unresolved_replay_feed_ids: set[uuid.UUID] = dataclasses.field(
        default_factory=set
    )
    replay_blocked_feed_ids: set[uuid.UUID] = dataclasses.field(
        default_factory=set
    )
    locally_retired_members: list[ingestion_lease_store.LeaseMemberIdentity] = (
        dataclasses.field(default_factory=list)
    )
    abort_reason: str | None = None
    uncertainty: _types._FinalPageUncertainty | None = None
    changed: asyncio.Event = dataclasses.field(default_factory=asyncio.Event)


@dataclasses.dataclass(frozen=True, slots=True)
class _PageSnapshot:
    """Payload-free projection of one transient page barrier."""

    grant: ingestion_lease_store.LeaseGrant
    page_sequence: int
    current_source_order: int | None
    pulled: int
    registered: int
    localized: int
    total_records: int
    pulled_records: int
    registered_records: int
    locally_rejected_records: int
    terminalized_registered_records: int


@dataclasses.dataclass(frozen=True, slots=True)
class _LaneSnapshot:
    """Bounded exact-lane coordination state for deterministic tests."""

    grant: ingestion_lease_store.LeaseGrant
    next_page_sequence: int
    page: _PageSnapshot | None
    closing: bool
    closed: bool


@dataclasses.dataclass(frozen=True, slots=True)
class _SchedulerSnapshot:
    """Bounded process scheduler state without payload or receipt history."""

    shards: tuple[_types._ShardSnapshot, ...]
    held: int
    lane_count: int
    registered_worker_tasks: int
    registered_flusher_tasks: int
    started: bool
    closing: bool
    closed: bool
    fatal: bool


def _require_member_retirements(
    context: _types.PageFinalizationContext,
    result: _types._AcceptedFinalPage,
    *,
    boundary_rejected_members: tuple[
        ingestion_lease_store.LeaseMemberIdentity,
        ...,
    ],
) -> tuple[ingestion_lease_store.LeaseMemberIdentity, ...]:
    """Correlate accepted retirement evidence to current-page members."""
    current_members = [
        boundary.member for boundary in context.candidate_boundaries
    ]
    for facts in context.cohort_terminal_facts:
        current_members.extend(
            record.identity.member for record in facts.records
        )
    current_members.extend(context.locally_retired_members)
    retirements = result.member_retirements
    if any(
        not any(member is current for current in current_members)
        for member in retirements
    ):
        message = "member retirement is not exact current-page evidence"
        raise _types.CohortIntegrityError(message)
    required = (
        *boundary_rejected_members,
        *context.locally_retired_members,
    )
    if any(
        not any(member is retirement for retirement in retirements)
        for member in required
    ):
        message = "accepted result omitted exact member retirement"
        raise _types.CohortIntegrityError(message)
    return retirements


class _DefaultPageFinalizer:
    """Compatibility finalizer until the source adapter is injected."""

    async def finalize_page(
        self,
        context: _types.PageFinalizationContext,
    ) -> _types.FinalPageResult:
        boundaries = tuple(
            _types.BoundaryResult(
                boundary,
                _types.BoundaryDisposition.COMMITTED,
            )
            for boundary in context.candidate_boundaries
        )
        pending = tuple(
            record.identity
            for facts in context.cohort_terminal_facts
            for record in facts.records
            if record.closure_state
            is _types.CohortRecordClosureState.FINAL_CLOSURE_PENDING
        )
        resolutions = []
        for identity in pending:
            if any(
                identity.member is member
                for member in context.locally_retired_members
            ):
                basis = (
                    _types.FinalRecordReleaseBasis.ACCEPTED_MEMBER_RETIREMENT
                )
            elif type(context.candidate) is (
                cursor_policy.NoProgressPageCandidate
            ):
                basis = _types.FinalRecordReleaseBasis.ACCEPTED_NO_PROGRESS
            elif identity.feed_id in context.unresolved_replay_feed_ids:
                basis = _types.FinalRecordReleaseBasis.ACCEPTED_REPLAYABLE_FEED
            else:
                return _types.FinalPageOutcomeUnknown(
                    context.grant,
                    context.page_sequence,
                    context.candidate,
                )
            resolutions.append(
                _types.FinalRecordClosureResolution(
                    identity,
                    _types.CohortRecordClosureState.REPLAY_SAFE_RELEASE,
                    basis,
                )
            )
        accepted = {
            "grant": context.grant,
            "page_sequence": context.page_sequence,
            "candidate": context.candidate,
            "boundary_results": boundaries,
            "final_closure_resolutions": tuple(resolutions),
            "source_evidence": None,
            "member_retirements": tuple(
                sorted(
                    context.locally_retired_members,
                    key=lambda member: (
                        member.feed_id.int,
                        member.source_feed_id,
                    ),
                )
            ),
        }
        if type(context.candidate) is cursor_policy.NoProgressPageCandidate:
            return _types.FinalPageNoProgress(**accepted)
        if context.unresolved_replay_feed_ids:
            return _types.FinalPageReplayable(**accepted)
        return _types.FinalPageCovered(**accepted)


class FeedWorkScheduler:
    """One process-wide owner of fixed Feed-affine scheduler shards."""

    def __init__(
        self,
        executor: _types.CallExecutor,
        *,
        boundary_committer: _types.BoundaryCommitter | None = None,
        page_finalizer: _types.PageFinalizer | None = None,
        _limits: _types._SchedulerLimits = _types._PRODUCTION_LIMITS,
        _boundary_batch_size: int = _boundaries._BOUNDARY_BATCH_SIZE,
        _monotonic: typing.Callable[[], float] = time.monotonic,
    ) -> None:
        """Create one scheduler with immutable production or test limits.

        Args:
            executor: Private full-pipeline adapter for fixed workers.
            boundary_committer: Closed exact-grant persistence seam. Phase 4
                uses the deterministic default until Phase 5 wires storage.
            page_finalizer: Closed source-page finalization seam. The
                deterministic default accepts structurally settled pages.
            _limits: Validated deterministic-test limits. Production callers
                use the fixed default.
            _monotonic: Monotonic clock seam for deterministic evidence tests.
        """
        if not callable(getattr(executor, "execute", None)):
            message = "executor must provide async execute(record)"
            raise TypeError(message)
        if not callable(_monotonic):
            message = "_monotonic must be callable"
            raise TypeError(message)
        if boundary_committer is None:
            boundary_committer = _boundaries._DefaultBoundaryCommitter()
        if not callable(getattr(boundary_committer, "commit", None)):
            message = "boundary_committer must provide async commit"
            raise TypeError(message)
        if page_finalizer is None:
            page_finalizer = _DefaultPageFinalizer()
        if not callable(getattr(page_finalizer, "finalize_page", None)):
            message = "page_finalizer must provide async finalize_page"
            raise TypeError(message)
        _types._require_positive_integer(
            _boundary_batch_size,
            "_boundary_batch_size",
        )
        self._limits = _limits
        self._monotonic = _monotonic
        self._boundary_committer = boundary_committer
        self._page_finalizer = page_finalizer
        self._boundary_batch_size = _boundary_batch_size
        self._finalization_lock = asyncio.Lock()
        self._lanes: dict[
            ingestion_lease_store.LeaseGrant,
            GrantLane,
        ] = {}
        self._highest_fence: dict[tuple[feed_store.SourceType, str], int] = {}
        self._closing_grants: set[ingestion_lease_store.LeaseGrant] = set()
        self._abandonment: dict[
            ingestion_lease_store.LeaseGrant,
            BaseException,
        ] = {}
        self._fatal: BaseException | None = None
        self._fatal_event = asyncio.Event()
        self._fatal_propagation_task: asyncio.Task[None] | None = None
        observed_executor = _ObservedCallExecutor(
            executor,
            self._observe_page_dispatch,
        )
        self._shards = tuple(
            _shard._Shard(
                index,
                observed_executor,
                limits=_limits,
                outcome_observer=self._observe_outcome,
                grant_is_closing=self._closing_grants.__contains__,
                fatal_observer=self._observe_fatal,
                global_fatal=lambda: self._fatal,
                abandonment_for=self._abandonment.get,
                boundary_ready_observer=self._observe_boundary_ready,
                closing_settlement=self._settlement_for_closing,
                page_registration_observer=(self._observe_page_registration),
                page_terminal_observer=self._observe_page_terminal,
                page_neutralization_observer=(
                    self._observe_page_neutralization
                ),
            )
            for index in range(_limits.shard_count)
        )
        self._lifecycle_lock = asyncio.Lock()
        self._close_task: asyncio.Task[_types.Undrained | None] | None = None
        self._started = False
        self._closing = False
        self._closed = False

    async def start(self) -> None:
        """Start every shard's fixed workers exactly once."""
        async with self._lifecycle_lock:
            if self._closing or self._closed:
                message = "cannot start a closing scheduler"
                raise RuntimeError(message)
            self._raise_fatal()
            if self._started:
                return
            for shard in self._shards:
                await shard.start()
            self._started = True

    @property
    def integrity_failure_event(self) -> asyncio.Event:
        """Return the monotonic signal for process scheduler failure."""
        return self._fatal_event

    def raise_if_failed(self) -> None:
        """Raise public scheduler integrity evidence after the signal wakes."""
        self._raise_fatal()

    def open_lane(
        self,
        grant: ingestion_lease_store.LeaseGrant,
        *,
        stop_requested: asyncio.Event,
        grant_lost: asyncio.Event,
    ) -> GrantLane:
        """Bind one fresh live lane to one complete immutable Lease grant.

        Args:
            grant: Complete exact Lease ownership generation.
            stop_requested: Runner-owned monotonic graceful-stop event.
            grant_lost: Runner-owned monotonic complete-grant-loss event.

        Returns:
            Fresh lane scoped to ``grant``.

        Raises:
            RuntimeError: The scheduler is not open for new lanes.
            ValueError: The exact grant already owns a live lane.
        """
        self._raise_fatal()
        if not self._started:
            message = "scheduler must be started before opening a lane"
            raise RuntimeError(message)
        if self._closing or self._closed:
            message = "scheduler is closing"
            raise RuntimeError(message)
        if grant in self._lanes:
            message = "exact grant already has a lane"
            raise ValueError(message)
        slot = self._lease_slot(grant)
        highest = self._highest_fence.get(slot)
        if highest is not None and grant.fencing_token <= highest:
            message = "grant is not newer than the lane slot history"
            raise ValueError(message)
        self._prune_closed_slot(grant)
        signals = _types.LaneSignalView(
            grant=grant,
            stop_requested=_ReadOnlySignal(stop_requested),
            grant_lost=_ReadOnlySignal(grant_lost),
        )
        lane = GrantLane(self, grant, signals)
        self._lanes[grant] = lane
        self._highest_fence[slot] = grant.fencing_token
        return lane

    async def close(self) -> _types.Undrained | None:
        """Close all exact lanes and then their settled fixed workers."""
        task = self._request_close()
        try:
            return await asyncio.shield(task)
        except asyncio.CancelledError:
            failure = _shard._ShardUndrainedError(
                "scheduler close was cancelled before worker settlement"
            )
            lanes = tuple(self._lanes.values())
            for lane in lanes:
                self._publish_abandonment(lane.grant, failure)
            for lane in lanes:
                await lane._boundary_coordinator.abandon(failure)
                await self._abandon_exact_cancellations(
                    lane.grant,
                    failure,
                )
            raise

    def _request_close(self) -> asyncio.Task[_types.Undrained | None]:
        """Publish scheduler shutdown synchronously and share one task."""
        self._closing = True
        for lane in self._lanes.values():
            lane._request_close(_types.LaneCloseReason.SCHEDULER_SHUTDOWN)
        if self._close_task is None:
            self._close_task = asyncio.create_task(
                self._coordinate_close(),
                name="feed-work-scheduler-close",
            )
        return self._close_task

    async def _coordinate_close(self) -> _types.Undrained | None:
        """Settle every live exact lane before stopping fixed shard workers.

        Returns:
            ``None`` after all lanes, held records, and shard workers close;
            otherwise conservative ``Undrained`` evidence.

        Each lane receives synchronous shutdown intent before this coroutine
        awaits its shared close task. Any uncertain lane or shard prevents the
        scheduler from publishing a closed result.
        """
        lane_tasks = tuple(
            lane._request_close(_types.LaneCloseReason.SCHEDULER_SHUTDOWN)
            for lane in self._lanes.values()
        )
        results = []
        for task in lane_tasks:
            results.append(await asyncio.shield(task))
        if any(isinstance(result, _types.Undrained) for result in results):
            return _types.Undrained(
                None,
                _types.LaneCloseReason.SCHEDULER_SHUTDOWN,
            )
        try:
            self._raise_fatal()
            await self._wait_for_idle()
            for shard in self._shards:
                await shard.close()
        except (
            SchedulerIntegrityError,
            _shard._ShardFatalError,
            _shard._ShardUndrainedError,
        ):
            return _types.Undrained(
                None,
                _types.LaneCloseReason.SCHEDULER_SHUTDOWN,
            )
        self._closed = True
        return None

    async def _snapshot(self) -> _SchedulerSnapshot:
        shards = []
        for shard in self._shards:
            shards.append(await shard.snapshot())
        snapshots = tuple(shards)
        registered = 0
        for snapshot in snapshots:
            for worker in snapshot.workers:
                registered += worker.task_registered
        registered_flushers = sum(
            not lane._boundary_coordinator.task.done()
            for lane in self._lanes.values()
        )
        return _SchedulerSnapshot(
            shards=snapshots,
            held=sum(snapshot.held for snapshot in snapshots),
            lane_count=len(self._lanes),
            registered_worker_tasks=registered,
            registered_flusher_tasks=registered_flushers,
            started=self._started,
            closing=self._closing,
            closed=self._closed,
            fatal=any(snapshot.fatal for snapshot in snapshots),
        )

    async def _wait_for_idle(self) -> None:
        for shard in self._shards:
            await shard.wait_for_held(0)

    async def _purge_page(
        self,
        grant: ingestion_lease_store.LeaseGrant,
        page_sequence: int,
        *,
        preserve_final_pending: bool = False,
    ) -> None:
        async with self._finalization_lock:
            for shard in self._shards:
                await shard.purge_page(
                    grant,
                    page_sequence,
                    preserve_final_pending=preserve_final_pending,
                )

    async def _abort_boundary_page(
        self,
        grant: ingestion_lease_store.LeaseGrant,
        page_sequence: int,
    ) -> None:
        for shard in self._shards:
            await shard.abort_boundary_page(grant, page_sequence)

    async def _promote_boundary_page(
        self,
        grant: ingestion_lease_store.LeaseGrant,
        page_sequence: int,
    ) -> None:
        for shard in self._shards:
            await shard.promote_boundary_page(grant, page_sequence)

    async def _seal_boundary_page(
        self,
        grant: ingestion_lease_store.LeaseGrant,
        page_sequence: int,
    ) -> None:
        """Irrevocably seal every prepared shard after receipt issuance."""
        for shard in self._shards:
            await shard.seal_boundary_page(grant, page_sequence)

    def _group_final_resolutions(
        self,
        resolutions: tuple[_types.FinalRecordClosureResolution, ...],
    ) -> dict[
        _shard._Shard,
        tuple[_types.FinalRecordClosureResolution, ...],
    ]:
        by_shard: dict[
            _shard._Shard,
            list[_types.FinalRecordClosureResolution],
        ] = {}
        for resolution in resolutions:
            shard = self._shard_for(resolution.identity.feed_id)
            by_shard.setdefault(shard, []).append(resolution)
        return {shard: tuple(values) for shard, values in by_shard.items()}

    @staticmethod
    async def _validate_final_pending(
        by_shard: dict[
            _shard._Shard,
            tuple[_types.FinalRecordClosureResolution, ...],
        ],
    ) -> None:
        for shard, values in by_shard.items():
            await shard.validate_final_pending(values)

    @staticmethod
    async def _resolve_final_pending(
        by_shard: dict[
            _shard._Shard,
            tuple[_types.FinalRecordClosureResolution, ...],
        ],
    ) -> None:
        for shard, values in by_shard.items():
            await shard.resolve_final_pending(values)

    async def _mark_page_finalization_uncertain(
        self,
        grant: ingestion_lease_store.LeaseGrant,
        page_sequence: int,
        uncertainty: _types._FinalPageUncertainty,
    ) -> None:
        for shard in self._shards:
            await shard.mark_page_finalization_uncertain(
                grant,
                page_sequence,
                uncertainty,
            )

    async def _replay_blocked_feed_ids(
        self,
        grant: ingestion_lease_store.LeaseGrant,
        page_sequence: int,
    ) -> tuple[uuid.UUID, ...]:
        feed_ids: set[uuid.UUID] = set()
        for shard in self._shards:
            feed_ids.update(
                await shard.replay_blocked_feed_ids(grant, page_sequence)
            )
        return tuple(sorted(feed_ids, key=lambda value: value.int))

    async def _clear_replay_barriers(
        self,
        grant: ingestion_lease_store.LeaseGrant,
        page_sequence: int,
        feed_ids: tuple[uuid.UUID, ...],
    ) -> None:
        for shard in self._shards:
            await shard.clear_replay_barriers(
                grant,
                page_sequence,
                feed_ids,
            )

    async def _purge_exact(
        self,
        grant: ingestion_lease_store.LeaseGrant,
        settlement: _types.CallSettlement,
    ) -> None:
        unknown_pages: set[int] = set()
        for shard in self._shards:
            unknown_pages.update(await shard.unknown_retained_pages(grant))
        preserved = frozenset(unknown_pages)
        for shard in self._shards:
            await shard.purge_exact(
                grant,
                settlement=settlement,
                preserve_final_pending_pages=preserved,
            )

    async def _wait_exact_empty(
        self,
        grant: ingestion_lease_store.LeaseGrant,
    ) -> None:
        for shard in self._shards:
            if await shard.unknown_retained_pages(grant):
                message = "exact grant retains outcome-unknown work"
                raise _shard._ShardUndrainedError(message)
        for shard in self._shards:
            await shard.wait_exact_empty(grant)

    async def _cancel_exact(
        self,
        grant: ingestion_lease_store.LeaseGrant,
    ) -> None:
        pending = []
        for shard in self._shards:
            pending.append((shard, await shard.request_cancel_exact(grant)))
        for shard, requests in pending:
            await shard.settle_cancellations(requests)

    async def _abandon_exact_cancellations(
        self,
        grant: ingestion_lease_store.LeaseGrant,
        failure: BaseException,
    ) -> None:
        for shard in self._shards:
            await shard.abandon_exact_cancellations(grant, failure)

    async def _purge_feed(
        self,
        grant: ingestion_lease_store.LeaseGrant,
        feed_id: uuid.UUID,
    ) -> None:
        await self._shard_for(feed_id).purge_feed(grant, feed_id)

    async def _wake_admission(self) -> None:
        for shard in self._shards:
            await shard.wake_waiters()

    def _shard_for(self, feed_id: uuid.UUID) -> _shard._Shard:
        index = _types._shard_index(feed_id, self._limits)
        return self._shards[index]

    def _sample_page_evidence(
        self,
        evidence: _PageEvidenceAccumulator,
    ) -> None:
        """Sample instantaneous scalar load without retaining a snapshot."""
        try:
            live_identities = tuple(
                record.identity
                for shard in self._shards
                for record in shard._records.values()
                if evidence.tracks_identity(record.identity)
            )
            live_boundary_records = tuple(
                record
                for shard in self._shards
                for record in (
                    *shard._pending_boundaries.values(),
                    *shard._flushing_boundaries.values(),
                )
                if evidence.tracks_boundary(record)
            )
            held = len(live_identities) + len(live_boundary_records)
            queued_first_identities = tuple(
                cohort.identities[0]
                for shard in self._shards
                for queue in shard._feed_queues.values()
                for cohort in queue
                if evidence.tracks_identity(cohort.identities[0])
            )
            queue_depth = sum(
                len(cohort.records)
                for shard in self._shards
                for queue in shard._feed_queues.values()
                for cohort in queue
                if evidence.tracks_identity(cohort.identities[0])
            )
            active_workers = sum(
                slot.active_cohort is not None
                and evidence.tracks_identity(slot.active_cohort.identities[0])
                for shard in self._shards
                for slot in shard._workers
            )
            evidence.observe_sample(
                held=held,
                queue_depth=queue_depth,
                active_workers=active_workers,
                queued_first_identities=queued_first_identities,
                live_identities=live_identities,
                live_boundary_records=live_boundary_records,
            )
        except BaseException:  # noqa: S110 - evidence is observational
            pass

    def _observe_page_dispatch(
        self,
        execution: _types.CohortExecution,
    ) -> None:
        """Observe the exact active-worker transition before delegation."""
        first_identity = execution.calls[0].identity
        lane = self._lanes.get(first_identity.grant)
        if lane is not None:
            lane._observe_page_dispatch(first_identity)

    def _observe_outcome(
        self,
        record: _types._CallRecord,
        outcome: _types._ExecutorOutcome,
    ) -> None:
        """Project terminal authority loss into its exact lane."""
        lane = self._lanes.get(record.grant)
        if lane is None:
            return
        if isinstance(outcome, _types._ExecutorAuthorityLost):
            lane._request_close(_types.LaneCloseReason.AUTHORITY_LOSS)

    def _observe_page_registration(
        self,
        cohort: _types._CohortRecord,
    ) -> None:
        lane = self._lanes.get(cohort.grant)
        if lane is not None:
            lane._observe_page_registration(cohort)

    def _observe_page_terminal(
        self,
        cohort: _types._CohortRecord,
        outcome: _types._ExecutorOutcome | None,
        failure: BaseException | None,
    ) -> None:
        lane = self._lanes.get(cohort.grant)
        if lane is not None:
            lane._observe_page_terminal(cohort, outcome, failure)

    def _observe_page_neutralization(
        self,
        records: tuple[_types._CallRecord, ...],
        *,
        replay_blocked: bool,
    ) -> None:
        by_lane: dict[GrantLane, list[_types._CallRecord]] = {}
        for record in records:
            lane = self._lanes.get(record.grant)
            if lane is not None:
                by_lane.setdefault(lane, []).append(record)
        for lane, exact_records in by_lane.items():
            lane._observe_page_neutralization(
                tuple(exact_records),
                replay_blocked=replay_blocked,
            )

    def _observe_boundary_ready(
        self,
        grant: ingestion_lease_store.LeaseGrant,
    ) -> None:
        """Coalesce a shard-ready notification into its exact lane Event."""
        lane = self._lanes.get(grant)
        if lane is not None:
            lane._boundary_coordinator.notify_ready()

    def _settlement_for_closing(
        self,
        grant: ingestion_lease_store.LeaseGrant,
    ) -> _types.CallSettlement:
        lane = self._lanes.get(grant)
        if (
            lane is not None
            and lane._close_reason is _types.LaneCloseReason.AUTHORITY_LOSS
        ):
            return _types.CallSettlement.AUTHORITY_LOST
        return _types.CallSettlement.ABORTED

    def _publish_grant_close(
        self,
        grant: ingestion_lease_store.LeaseGrant,
    ) -> None:
        """Make close visible to fixed-worker dispatch synchronously."""
        self._closing_grants.add(grant)

    def _publish_abandonment(
        self,
        grant: ingestion_lease_store.LeaseGrant,
        failure: BaseException,
    ) -> None:
        """Retain an external deadline winner until intent registration."""
        self._abandonment.setdefault(grant, failure)

    def _clear_abandonment(
        self,
        grant: ingestion_lease_store.LeaseGrant,
    ) -> None:
        self._abandonment.pop(grant, None)

    def _prune_closed_slot(
        self,
        successor: ingestion_lease_store.LeaseGrant,
    ) -> None:
        slot = self._lease_slot(successor)
        for grant, lane in tuple(self._lanes.items()):
            if (
                grant != successor
                and self._lease_slot(grant) == slot
                and lane._closed
            ):
                del self._lanes[grant]
                self._closing_grants.discard(grant)

    def _lane_closed(self, lane: GrantLane) -> None:
        """Remove successfully closed lanes from the live registries."""
        if self._lanes.get(lane.grant) is lane:
            self._lanes.pop(lane.grant)
        self._closing_grants.discard(lane.grant)

    @staticmethod
    def _lease_slot(
        grant: ingestion_lease_store.LeaseGrant,
    ) -> tuple[feed_store.SourceType, str]:
        return grant.source_type, grant.lease_key

    def _observe_fatal(self, failure: BaseException) -> None:
        """Publish first integrity failure and wake every shard once."""
        if self._fatal is not None:
            return
        self._fatal = failure
        self._fatal_event.set()
        self._fatal_propagation_task = asyncio.create_task(
            self._propagate_fatal(failure),
            name="feed-work-scheduler-fatal-propagation",
        )

    async def _propagate_fatal(self, failure: BaseException) -> None:
        for shard in self._shards:
            await shard.propagate_fatal(failure)

    def _raise_fatal(self) -> None:
        if self._fatal is not None:
            message = "scheduler shard integrity failed"
            raise SchedulerIntegrityError(message) from self._fatal
        for shard in self._shards:
            failure = shard.fatal_failure
            if failure is not None:
                message = "scheduler shard integrity failed"
                raise SchedulerIntegrityError(message) from failure


class GrantLane:
    """One exact-grant lane hiding admission, shards, and cleanup."""

    def __init__(
        self,
        scheduler: FeedWorkScheduler,
        grant: ingestion_lease_store.LeaseGrant,
        signals: _types.LaneSignalView,
    ) -> None:
        self._scheduler = scheduler
        self._grant = grant
        self._signals = signals
        self._admission_lock = asyncio.Lock()
        self._state_lock = asyncio.Lock()
        self._closing_event = asyncio.Event()
        self._close_changed = asyncio.Event()
        self._next_page_sequence = 0
        self._page: _PageBarrier | None = None
        self._page_settlement_task: asyncio.Task[object] | None = None
        self._close_strength = _CloseStrength.OPEN
        self._close_reason = _types.LaneCloseReason.PLANNED_DRAIN
        self._close_task: (
            asyncio.Task[_types.LaneClosed | _types.Undrained] | None
        ) = None
        self._closing = False
        self._closed = False
        self._boundary_coordinator = _boundaries._BoundaryCoordinator(
            grant,
            scheduler._shards,
            scheduler._boundary_committer,
            authority_lost=lambda: self._request_close(
                _types.LaneCloseReason.AUTHORITY_LOSS
            ),
            fatal_observer=scheduler._observe_fatal,
            monotonic=scheduler._monotonic,
            evidence_sink_for=self._boundary_evidence_sink,
            batch_size=scheduler._boundary_batch_size,
        )

    @property
    def grant(self) -> ingestion_lease_store.LeaseGrant:
        """Return the complete immutable grant bound to this lane."""
        return self._grant

    @property
    def signals(self) -> _types.LaneSignalView:
        """Return the object-identical scheduler-owned read-only signals."""
        return self._signals

    def _boundary_evidence_sink(
        self,
        selected: _boundaries._SelectedBoundaryBatch,
        logical: bool,  # noqa: FBT001 - private callback protocol
    ) -> _PageEvidenceAccumulator | None:
        """Capture the exact page owner before external boundary mutation."""
        page = self._page
        if page is None or not page.evidence.boundary_capture_open:
            return None
        if logical or any(
            page.evidence.tracks_boundary(record)
            for _shard_value, record in selected
        ):
            return page.evidence
        return None

    def _wrap_settlement_observer(
        self,
        evidence: _PageEvidenceAccumulator,
        observer: typing.Callable[[_types.CallSettlement], None] | None,
    ) -> typing.Callable[[_types.CallSettlement], None]:
        """Close physically released queue residence before user callbacks."""

        def observe(settlement: _types.CallSettlement) -> None:
            self._scheduler._sample_page_evidence(evidence)
            if observer is not None:
                observer(settlement)

        return observe

    @staticmethod
    def _time_pressured_submission(
        page: _PageBarrier,
        cohort: _types.CohortSubmission,
        started: float,
    ) -> tuple[_types.CohortSubmission, typing.Callable[[], None]]:
        """End pressure timing immediately before the admission hook runs."""
        finished = False

        def finish() -> None:
            nonlocal finished
            if finished:
                return
            finished = True
            try:
                page.evidence.observe_pressure_wait(started)
            except BaseException:  # noqa: S110 - evidence is observational
                pass

        def admission_hook(
            identities: tuple[_types.CohortRecordIdentity, ...],
        ) -> None:
            finish()
            cohort.admission_hook(identities)

        observed = _types.CohortSubmission(
            member=cohort.member,
            feed_id=cohort.feed_id,
            cohort_timestamp=cohort.cohort_timestamp,
            calls=cohort.calls,
            admission_hook=admission_hook,
        )
        object.__setattr__(
            observed,
            "_admission_state",
            cohort._admission_state,
        )
        return observed, finish

    async def purge_feed(self, feed_id: uuid.UUID) -> None:
        """Purge queued work after an authoritative membership refresh."""
        self._raise_closed_locked()
        try:
            await self._scheduler._purge_feed(
                self._grant,
                feed_id,
            )
        except _shard._ShardFatalError as exc:
            message = "scheduler shard integrity failed"
            raise SchedulerIntegrityError(message) from exc.failure

    async def close(
        self,
        reason: _types.LaneCloseReason = (_types.LaneCloseReason.PLANNED_DRAIN),
    ) -> _types.LaneClosed | _types.Undrained:
        """Close this exact grant with monotonic shared coordination."""
        task = self._request_close(reason)
        try:
            return await asyncio.shield(task)
        except asyncio.CancelledError:
            failure = _shard._ShardUndrainedError(
                "lane close was cancelled before worker settlement"
            )
            self._scheduler._publish_abandonment(
                self._grant,
                failure,
            )
            await self._boundary_coordinator.abandon(failure)
            await self._scheduler._abandon_exact_cancellations(
                self._grant,
                failure,
            )
            raise

    async def cover_page(  # noqa: PLR0912, PLR0915
        self,
        *,
        calls: collections.abc.Iterable[_types.CohortSubmission],
        boundaries: collections.abc.Iterable[_types.BoundaryWork],
        candidate: cursor_policy.PageCandidate,
        evidence_observer: (
            typing.Callable[[_types.SchedulerPageEvidence], None] | None
        ) = None,
    ) -> _types.SettledPage | _types.Undrained:
        """Incrementally cover one source-order call page.

        Args:
            calls: Exact cohorts in frozen provider source order.
            boundaries: Single-pass trailing Feed completion boundaries.
            candidate: Exact cursor candidate awaiting bounded coverage.
            evidence_observer: Optional contained terminal-failure observer.

        Returns:
            Private sealed receipt consumable only by ``LeaseCursor``.

        Raises:
            CursorIntegrityError: Candidate authority or sequence is wrong.
            RuntimeError: The lane or scheduler is closing or failed.
        """
        if evidence_observer is not None and not callable(evidence_observer):
            message = "evidence_observer must be callable or None"
            raise TypeError(message)
        async with self._admission_lock:
            await self._validate_candidate(candidate)
            cohorts = tuple(calls)
            page_boundaries = tuple(boundaries)
            total_records = self._validate_cohort_page(
                cohorts,
                candidate.page_sequence,
            )
            no_progress = (
                type(candidate) is cursor_policy.NoProgressPageCandidate
            )
            if no_progress:
                self._require_no_progress_boundaries_empty(page_boundaries)
                page_boundaries = ()
            page = await self._begin_page(
                candidate.page_sequence,
                total_records,
                evidence_observer=evidence_observer,
            )
            try:
                source_order = 0
                replay_blocked_feeds: set[uuid.UUID] = set()
                for cohort in cohorts:
                    record_count = len(cohort.calls)
                    await self._mark_pulled(
                        source_order,
                        record_count,
                        call_records=True,
                    )
                    works = tuple(
                        _types._CallWork(
                            feed_id=cohort.feed_id,
                            grant=self._grant,
                            member=cohort.member,
                            source_order=source_order + offset,
                            cohort_timestamp=cohort.cohort_timestamp,
                            payload=submission.payload,
                            page_sequence=candidate.page_sequence,
                            settlement_observer=(
                                self._wrap_settlement_observer(
                                    page.evidence,
                                    submission.settlement_observer,
                                )
                            ),
                        )
                        for offset, submission in enumerate(cohort.calls)
                    )
                    shard = self._scheduler._shard_for(cohort.feed_id)
                    pressure_started = (
                        page.evidence.now()
                        if self._cohort_admission_is_pressured(
                            shard,
                            cohort.feed_id,
                            candidate.page_sequence,
                            record_count,
                        )
                        else None
                    )
                    admitted_cohort = cohort
                    finish_pressure: typing.Callable[[], None] | None = None
                    if pressure_started is not None:
                        admitted_cohort, finish_pressure = (
                            self._time_pressured_submission(
                                page,
                                cohort,
                                pressure_started,
                            )
                        )
                    try:
                        try:
                            await shard.admit_cohort(
                                admitted_cohort,
                                works,
                                self._signals,
                                abort_event=self._closing_event,
                            )
                        finally:
                            if finish_pressure is not None:
                                finish_pressure()
                            self._scheduler._sample_page_evidence(page.evidence)
                    except _shard._ReplayBlockedError:
                        self._notify_unadmitted_cohort(
                            cohort,
                            _types.CallSettlement.REPLAY_BLOCKED,
                        )
                        replay_blocked_feeds.add(cohort.feed_id)
                        await self._mark_localized(
                            source_order,
                            record_count,
                            call_records=True,
                            replay_blocked_feed=cohort.feed_id,
                        )
                        source_order += record_count
                        continue
                    except _shard._AdmissionAbortedError as exc:
                        message = "lane closed during page admission"
                        raise _LaneClosedError(message) from exc
                    except _shard._ShardFatalError as exc:
                        message = "scheduler shard integrity failed"
                        raise SchedulerIntegrityError(message) from exc.failure
                    await self._mark_registered(
                        source_order,
                        record_count,
                    )
                    source_order += record_count
                for boundary in page_boundaries:
                    self._require_boundary(boundary)
                    source_order = await self._next_source_order()
                    await self._mark_pulled(
                        source_order,
                        1,
                        call_records=False,
                    )
                    shard = self._scheduler._shard_for(boundary.feed_id)
                    if boundary.feed_id in replay_blocked_feeds or (
                        await shard.is_replay_blocked(
                            self._grant,
                            candidate.page_sequence,
                            boundary.feed_id,
                        )
                    ):
                        await self._mark_localized(
                            source_order,
                            1,
                            call_records=False,
                        )
                        continue
                    boundary_input = _types._BoundaryInput(
                        boundary=boundary,
                        grant=self._grant,
                        source_order=source_order,
                        page_sequence=candidate.page_sequence,
                    )
                    pressure_started = (
                        page.evidence.now()
                        if self._boundary_admission_is_pressured(
                            shard,
                            boundary,
                            candidate.page_sequence,
                        )
                        else None
                    )
                    try:
                        try:
                            boundary_record = await shard.admit_boundary(
                                boundary_input,
                                abort_event=self._closing_event,
                                pressure_relief=(
                                    self._boundary_coordinator.request_relief
                                ),
                            )
                            page.evidence.observe_boundary_registration(
                                boundary_record
                            )
                        finally:
                            if pressure_started is not None:
                                page.evidence.observe_pressure_wait(
                                    pressure_started
                                )
                            self._scheduler._sample_page_evidence(page.evidence)
                    except _shard._ReplayBlockedError:
                        replay_blocked_feeds.add(boundary.feed_id)
                        await self._mark_localized(
                            source_order,
                            1,
                            call_records=False,
                            replay_blocked_feed=boundary.feed_id,
                        )
                        continue
                    except _shard._BoundaryReliefRetryableError:
                        message = "boundary pressure relief is retryable"
                        raise RuntimeError(message) from None
                    except _boundaries._BoundaryAuthorityLostError as exc:
                        message = "exact grant lost boundary authority"
                        raise _LaneClosedError(message) from exc
                    except _boundaries._BoundaryCoordinatorError as exc:
                        self._scheduler._raise_fatal()
                        message = "boundary coordinator integrity failed"
                        raise SchedulerIntegrityError(message) from exc
                    except _shard._AdmissionAbortedError as exc:
                        message = "lane closed during boundary admission"
                        raise _LaneClosedError(message) from exc
                    except _shard._ShardFatalError as exc:
                        message = "scheduler shard integrity failed"
                        raise SchedulerIntegrityError(message) from exc.failure
                    await self._mark_registered(source_order, 1)
                page_state = await self._await_terminal_barrier()
                if page_state == "uncertain":
                    uncertainty = await self._page_uncertainty()
                    await self._scheduler._mark_page_finalization_uncertain(
                        self._grant,
                        candidate.page_sequence,
                        uncertainty,
                    )
                    undrained = _types.Undrained(
                        self._grant,
                        _types.LaneCloseReason.PLANNED_DRAIN,
                    )
                    (
                        evidence_complete,
                        cancelled,
                    ) = await self._settle_uncertain_page_evidence(page)
                    self._publish_page_evidence(
                        page,
                        complete=evidence_complete,
                    )
                    if cancelled:
                        _raise_cancelled()
                    return undrained
                if page_state != "finalizable":
                    message = f"page candidate aborted by {page_state}"
                    raise RuntimeError(message)  # noqa: TRY301
                try:
                    final_result = (
                        await self._boundary_coordinator.request_final()
                    )
                except _boundaries._BoundaryAuthorityLostError as exc:
                    message = "exact grant lost boundary authority"
                    raise _LaneClosedError(message) from exc
                except _boundaries._BoundaryCoordinatorError as exc:
                    self._scheduler._raise_fatal()
                    message = "boundary coordinator integrity failed"
                    raise SchedulerIntegrityError(message) from exc
                self._require_final_boundary_settlement(final_result)
                if not no_progress:
                    await self._scheduler._promote_boundary_page(
                        self._grant,
                        candidate.page_sequence,
                    )
                context = await self._build_finalization_context(
                    candidate,
                    page_boundaries,
                )
                page_settlement = await self._finalize_page(
                    candidate,
                    context,
                )
                if isinstance(page_settlement, _types.Undrained):
                    (
                        evidence_complete,
                        cancelled,
                    ) = await self._settle_uncertain_page_evidence(page)
                    self._publish_page_evidence(
                        page,
                        complete=evidence_complete,
                    )
                    if cancelled:
                        _raise_cancelled()
                    return page_settlement
            except asyncio.CancelledError:
                if page.uncertainty is not None:
                    if not page.evidence_published:
                        (
                            evidence_complete,
                            _cancelled,
                        ) = await self._settle_uncertain_page_evidence(page)
                        self._publish_page_evidence(
                            page,
                            complete=evidence_complete,
                        )
                    raise
                await self._abort_and_publish_page(page)
                raise
            except BaseException:
                await self._abort_and_publish_page(page)
                raise
            return page_settlement

    async def _snapshot(self) -> _LaneSnapshot:
        async with self._state_lock:
            page = self._page
            page_snapshot = (
                None
                if page is None
                else _PageSnapshot(
                    grant=page.grant,
                    page_sequence=page.page_sequence,
                    current_source_order=page.current_source_order,
                    pulled=page.pulled,
                    registered=page.registered,
                    localized=page.localized,
                    total_records=page.total_records,
                    pulled_records=page.pulled_records,
                    registered_records=page.registered_records,
                    locally_rejected_records=(page.locally_rejected_records),
                    terminalized_registered_records=(
                        page.terminalized_registered_records
                    ),
                )
            )
            return _LaneSnapshot(
                grant=self._grant,
                next_page_sequence=self._next_page_sequence,
                page=page_snapshot,
                closing=self._closing,
                closed=self._closed,
            )

    async def _validate_candidate(
        self,
        candidate: cursor_policy.PageCandidate,
    ) -> None:
        async with self._state_lock:
            self._raise_closed_locked()
            self._validate_candidate_locked(candidate)

    async def _begin_page(
        self,
        page_sequence: int,
        total_records: int,
        *,
        evidence_observer: (
            typing.Callable[[_types.SchedulerPageEvidence], None] | None
        ),
    ) -> _PageBarrier:
        async with self._state_lock:
            self._raise_closed_locked()
            if self._page is not None:
                message = "lane already owns a live page"
                raise RuntimeError(message)
            page = _PageBarrier(
                grant=self._grant,
                page_sequence=page_sequence,
                evidence=_PageEvidenceAccumulator(
                    grant=self._grant,
                    page_sequence=page_sequence,
                    monotonic=self._scheduler._monotonic,
                    worker_utilization_denominator=(
                        self._scheduler._limits.shard_count
                        * self._scheduler._limits.workers_per_shard
                    ),
                ),
                evidence_observer=evidence_observer,
                total_records=total_records,
            )
            self._page = page
        self._scheduler._sample_page_evidence(page.evidence)
        return page

    def _cohort_admission_is_pressured(
        self,
        shard: _shard._Shard,
        feed_id: uuid.UUID,
        page_sequence: int,
        record_count: int,
    ) -> bool:
        if (
            self._closing_event.is_set()
            or self._scheduler._fatal is not None
            or shard._fatal is not None
            or shard._stopping
            or shard._closed
            or (self._grant, page_sequence, feed_id) in shard._replay_blocks
        ):
            return False
        return (
            shard._pressure_paused
            or shard._held + record_count > shard._limits.capacity
        )

    def _boundary_admission_is_pressured(
        self,
        shard: _shard._Shard,
        boundary: _types.BoundaryWork,
        page_sequence: int,
    ) -> bool:
        scope = (self._grant, boundary.feed_id)
        if (
            self._closing_event.is_set()
            or self._scheduler._fatal is not None
            or shard._fatal is not None
            or shard._stopping
            or shard._closed
            or (
                self._grant,
                page_sequence,
                boundary.feed_id,
            )
            in shard._replay_blocks
            or scope in shard._pending_boundaries
        ):
            return False
        return shard._pressure_paused or shard._held >= shard._limits.capacity

    def _publish_page_evidence(
        self,
        page: _PageBarrier,
        *,
        complete: bool = True,
    ) -> None:
        """Publish one contained immutable failure/unknown observation."""
        if page.evidence_published:
            return
        page.evidence_published = True
        observer = page.evidence_observer
        page.evidence_observer = None
        if not complete:
            return
        self._scheduler._sample_page_evidence(page.evidence)
        try:
            evidence = page.evidence.freeze()
        except BaseException:
            return
        if observer is None:
            return
        try:
            observer(evidence)
        except BaseException:  # noqa: S110 - user observer is contained
            pass

    async def _mark_pulled(
        self,
        source_order: int,
        record_count: int,
        *,
        call_records: bool,
    ) -> None:
        _types._require_positive_integer(record_count, "record_count")
        async with self._state_lock:
            self._raise_closed_locked()
            page = self._require_page_locked()
            if page.current_source_order is not None:
                message = "cannot pull past the current source record"
                raise RuntimeError(message)
            page.current_source_order = source_order
            page.pulled += record_count
            if call_records:
                page.pulled_records += record_count
                if page.pulled_records > page.total_records:
                    message = "page pulled more calls than it declared"
                    raise RuntimeError(message)

    async def _next_source_order(self) -> int:
        async with self._state_lock:
            page = self._require_page_locked()
            if page.current_source_order is not None:
                message = "cannot inspect the next source record while blocked"
                raise RuntimeError(message)
            return page.pulled

    async def _mark_registered(
        self,
        source_order: int,
        record_count: int,
    ) -> None:
        _types._require_positive_integer(record_count, "record_count")
        async with self._state_lock:
            page = self._require_page_locked()
            if page.current_source_order is None:
                if (
                    source_order != page.pulled - record_count
                    or page.pulled != page.registered + page.localized
                ):
                    message = "source record lost its barrier transition"
                    raise RuntimeError(message)
                return
            if page.current_source_order != source_order:
                message = "registered source record does not match barrier"
                raise RuntimeError(message)
            page.current_source_order = None
            page.registered += record_count

    async def _mark_localized(
        self,
        source_order: int,
        record_count: int,
        *,
        call_records: bool,
        replay_blocked_feed: uuid.UUID | None = None,
    ) -> None:
        _types._require_positive_integer(record_count, "record_count")
        async with self._state_lock:
            page = self._require_page_locked()
            if page.current_source_order != source_order:
                message = "localized source record does not match barrier"
                raise RuntimeError(message)
            page.current_source_order = None
            page.localized += record_count
            if call_records:
                page.locally_rejected_records += record_count
            if replay_blocked_feed is not None:
                page.replay_blocked_feed_ids.add(replay_blocked_feed)
            page.changed.set()
            try:
                page.evidence.observe_unadmitted(
                    record_count,
                    replay_blocked=(
                        call_records and replay_blocked_feed is not None
                    ),
                    member_rejected=False,
                )
            except BaseException:  # noqa: S110 - evidence is observational
                pass
        self._scheduler._sample_page_evidence(page.evidence)

    def _localize_tag(
        self,
        page_sequence: int,
        source_order: int,
    ) -> None:
        page = self._page
        if page is None or page.page_sequence != page_sequence:
            return
        if page.current_source_order == source_order:
            page.current_source_order = None
            page.localized += 1
            return
        if source_order >= page.pulled:
            message = "membership evidence precedes its source pull"
            raise RuntimeError(message)
        if page.registered <= 0:
            message = "membership evidence has no registered record"
            raise RuntimeError(message)
        page.registered -= 1
        page.localized += 1

    @staticmethod
    def _append_retired_member(
        page: _PageBarrier,
        member: ingestion_lease_store.LeaseMemberIdentity,
    ) -> None:
        if not any(
            retained is member for retained in page.locally_retired_members
        ):
            page.locally_retired_members.append(member)

    @staticmethod
    def _mark_page_uncertain(
        page: _PageBarrier,
        uncertainty: _types._FinalPageUncertainty,
    ) -> None:
        if page.uncertainty is None:
            page.uncertainty = uncertainty
        elif page.uncertainty is not uncertainty:
            page.uncertainty = _types._FinalPageUncertainty.INTEGRITY_FAILURE
        page.changed.set()

    def _observe_page_registration(
        self,
        cohort: _types._CohortRecord,
    ) -> None:
        page = self._page
        if page is None or page.page_sequence != cohort.page_sequence:
            return
        identities = cohort.identities
        first = identities[0]
        if (
            cohort.grant != page.grant
            or first in page.registered_cohorts
            or any(
                identity.grant != page.grant
                or identity.page_sequence != page.page_sequence
                for identity in identities
            )
        ):
            self._mark_page_uncertain(
                page,
                _types._FinalPageUncertainty.INTEGRITY_FAILURE,
            )
            return
        page.registered_cohorts[first] = identities
        page.registered_records += len(identities)
        try:
            page.evidence.observe_registration(identities)
        except BaseException:  # noqa: S110 - evidence is observational
            pass
        self._scheduler._sample_page_evidence(page.evidence)
        page.changed.set()

    def _observe_page_dispatch(
        self,
        first_identity: _types.CohortRecordIdentity,
    ) -> None:
        page = self._page
        if (
            page is None
            or first_identity.grant != page.grant
            or first_identity.page_sequence != page.page_sequence
            or first_identity not in page.registered_cohorts
        ):
            return
        try:
            page.evidence.observe_dispatch(first_identity)
        except BaseException:  # noqa: S110 - evidence is observational
            pass
        self._scheduler._sample_page_evidence(page.evidence)

    def _observe_page_terminal(
        self,
        cohort: _types._CohortRecord,
        outcome: _types._ExecutorOutcome | None,
        failure: BaseException | None,
    ) -> None:
        page = self._page
        if page is None or page.page_sequence != cohort.page_sequence:
            return
        self._scheduler._sample_page_evidence(page.evidence)
        if failure is not None or outcome is None:
            self._mark_page_uncertain(
                page,
                _types._FinalPageUncertainty.INTEGRITY_FAILURE,
            )
            return
        identities = page.registered_cohorts.get(cohort.identities[0])
        if identities is None or any(
            expected is not actual
            for expected, actual in zip(
                identities,
                cohort.identities,
                strict=True,
            )
        ):
            self._mark_page_uncertain(
                page,
                _types._FinalPageUncertainty.INTEGRITY_FAILURE,
            )
            return
        if isinstance(
            outcome,
            (
                _types._ExecutorIntegrityFailure,
                _types._ExecutorOutcomeUnknown,
            ),
        ):
            uncertainty = (
                _types._FinalPageUncertainty.INTEGRITY_FAILURE
                if isinstance(outcome, _types._ExecutorIntegrityFailure)
                else _types._FinalPageUncertainty.OUTCOME_UNKNOWN
            )
            self._mark_page_uncertain(page, uncertainty)
            return
        fact_outcomes = (
            _types._ExecutorCompleted,
            _types._ExecutorFinalClosurePending,
            _types._ExecutorReplayableDirectFailure,
        )
        if isinstance(outcome, fact_outcomes):
            page.terminal_facts[identities[0]] = outcome.facts
        else:
            page.neutralized_identities.update(identities)
        if isinstance(outcome, _types._ExecutorReplayableDirectFailure):
            page.unresolved_replay_feed_ids.add(cohort.feed_id)
            page.replay_blocked_feed_ids.add(cohort.feed_id)
        if isinstance(outcome, _types._ExecutorMembershipRejected):
            self._append_retired_member(
                page,
                cohort.records[0].identity.member,
            )
        if isinstance(
            outcome,
            (
                _types._ExecutorStopped,
                _types._ExecutorRetryable,
                _types._ExecutorAuthorityLost,
            ),
        ):
            page.abort_reason = outcome.facts.disposition.value
        page.terminalized_registered_records += len(identities)
        try:
            page.evidence.observe_terminal(
                len(identities),
                member_rejected=isinstance(
                    outcome,
                    _types._ExecutorMembershipRejected,
                ),
            )
        except BaseException:  # noqa: S110 - evidence is observational
            pass
        if not self._terminal_accounting_is_valid(page):
            self._mark_page_uncertain(
                page,
                _types._FinalPageUncertainty.INTEGRITY_FAILURE,
            )
        page.changed.set()

    def _observe_page_neutralization(
        self,
        records: tuple[_types._CallRecord, ...],
        *,
        replay_blocked: bool,
    ) -> None:
        page = self._page
        if page is None:
            return
        fact_identities = {
            record.identity
            for facts in page.terminal_facts.values()
            for record in facts.records
        }
        added = 0
        neutralized_firsts: list[_types.CohortRecordIdentity] = []
        for record in records:
            identity = record.identity
            if (
                identity.grant != page.grant
                or identity.page_sequence != page.page_sequence
            ):
                continue
            first_identity = next(
                (
                    first
                    for first, identities in page.registered_cohorts.items()
                    if any(exact is identity for exact in identities)
                ),
                None,
            )
            if first_identity is None:
                self._mark_page_uncertain(
                    page,
                    _types._FinalPageUncertainty.INTEGRITY_FAILURE,
                )
                continue
            if (
                identity in fact_identities
                or identity in page.neutralized_identities
            ):
                continue
            page.neutralized_identities.add(identity)
            added += 1
            if not any(
                retained is first_identity for retained in neutralized_firsts
            ):
                neutralized_firsts.append(first_identity)
            if replay_blocked:
                page.replay_blocked_feed_ids.add(identity.feed_id)
        page.terminalized_registered_records += added
        if added:
            try:
                page.evidence.observe_neutralization(
                    tuple(neutralized_firsts),
                    added,
                    replay_blocked=replay_blocked,
                    member_rejected=False,
                )
            except BaseException:  # noqa: S110 - evidence is observational
                pass
        self._scheduler._sample_page_evidence(page.evidence)
        if not replay_blocked and added:
            page.abort_reason = "precommit_cancellation"
        if not self._terminal_accounting_is_valid(page):
            self._mark_page_uncertain(
                page,
                _types._FinalPageUncertainty.INTEGRITY_FAILURE,
            )
        page.changed.set()

    @staticmethod
    def _terminal_accounting_is_valid(page: _PageBarrier) -> bool:
        fact_records = sum(
            len(facts.records) for facts in page.terminal_facts.values()
        )
        return (
            page.terminalized_registered_records
            == fact_records + len(page.neutralized_identities)
            and page.terminalized_registered_records <= page.registered_records
        )

    async def _await_terminal_barrier(self) -> str:
        abort_neutralization_started = False
        while True:
            neutralize_abort = False
            async with self._state_lock:
                page = self._require_page_locked()
                if page.uncertainty is not None:
                    return "uncertain"
                conserved = (
                    page.pulled_records
                    == page.registered_records + page.locally_rejected_records
                )
                terminal = (
                    page.terminalized_registered_records
                    == page.registered_records
                )
                if (
                    page.pulled_records == page.total_records
                    and conserved
                    and terminal
                    and self._terminal_accounting_is_valid(page)
                ):
                    if page.abort_reason is not None:
                        return page.abort_reason
                    return "finalizable"
                if (
                    page.abort_reason is not None
                    and not abort_neutralization_started
                ):
                    abort_neutralization_started = True
                    neutralize_abort = True
                page.changed.clear()
                changed = page.changed
            if neutralize_abort:
                await self._scheduler._purge_page(
                    self._grant,
                    page.page_sequence,
                    preserve_final_pending=True,
                )
            else:
                await changed.wait()

    async def _build_finalization_context(
        self,
        candidate: cursor_policy.PageCandidate,
        boundaries: tuple[_types.BoundaryWork, ...],
    ) -> _types.PageFinalizationContext:
        replay_blocked = await self._scheduler._replay_blocked_feed_ids(
            self._grant,
            candidate.page_sequence,
        )
        async with self._state_lock:
            page = self._require_page_locked()
            locally_rejected_feed_ids = {
                member.feed_id for member in page.locally_retired_members
            }
            accepted_boundaries = tuple(
                boundary
                for boundary in boundaries
                if boundary.feed_id not in replay_blocked
                and boundary.feed_id not in locally_rejected_feed_ids
            )
            facts = tuple(
                page.terminal_facts[key]
                for key in sorted(
                    page.terminal_facts,
                    key=lambda identity: (
                        identity.feed_id.int,
                        identity.source_order,
                        identity.local_sequence,
                    ),
                )
            )
            page.replay_blocked_feed_ids.update(replay_blocked)
            retired_members = tuple(
                sorted(
                    page.locally_retired_members,
                    key=lambda member: (
                        member.feed_id.int,
                        member.source_feed_id,
                    ),
                )
            )
            return _types.PageFinalizationContext(
                grant=self._grant,
                page_sequence=candidate.page_sequence,
                candidate=candidate,
                cohort_terminal_facts=facts,
                unresolved_replay_feed_ids=tuple(
                    sorted(
                        page.unresolved_replay_feed_ids,
                        key=lambda value: value.int,
                    )
                ),
                locally_retired_members=retired_members,
                replay_blocked_feed_ids=tuple(
                    sorted(
                        page.replay_blocked_feed_ids,
                        key=lambda value: value.int,
                    )
                ),
                candidate_boundaries=accepted_boundaries,
            )

    async def _page_uncertainty(self) -> _types._FinalPageUncertainty:
        async with self._state_lock:
            page = self._require_page_locked()
            if page.uncertainty is None:
                message = "page lacks retained uncertainty"
                raise RuntimeError(message)
            return page.uncertainty

    async def _finalize_page(
        self,
        candidate: cursor_policy.PageCandidate,
        context: _types.PageFinalizationContext,
    ) -> _types.SettledPage | _types.Undrained:
        while True:
            try:
                result = await _boundaries._settle_final_page(
                    self._scheduler._page_finalizer.finalize_page(context)
                )
            except BaseException:
                return await self._retain_uncertain_page(
                    candidate.page_sequence,
                    _types._FinalPageUncertainty.OUTCOME_UNKNOWN,
                )
            try:
                accepted = self._validate_final_result(context, result)
            except BaseException:
                return await self._retain_uncertain_page(
                    candidate.page_sequence,
                    _types._FinalPageUncertainty.INTEGRITY_FAILURE,
                )
            if type(result) is not _types.FinalPageRetryable:
                break
            await asyncio.sleep(0)
        if type(result) is _types.FinalPageOutcomeUnknown:
            return await self._retain_uncertain_page(
                candidate.page_sequence,
                _types._FinalPageUncertainty.OUTCOME_UNKNOWN,
            )
        if type(result) is _types.FinalPageGrantRejected:
            page = self._page
            if page is not None:
                try:
                    page.evidence.observe_fence_rejection()
                except BaseException:  # noqa: S110 - evidence is observational
                    pass
            self._request_close(_types.LaneCloseReason.AUTHORITY_LOSS)
            message = "page finalizer rejected the exact grant"
            raise _LaneClosedError(message)
        if accepted is None:
            return await self._retain_uncertain_page(
                candidate.page_sequence,
                _types._FinalPageUncertainty.INTEGRITY_FAILURE,
            )
        transition = self._start_page_settlement(
            self._accept_final_page(candidate, context, accepted),
            name=(
                "feed-work-page-accept-"
                f"{self._grant.lease_key}-{candidate.page_sequence}"
            ),
        )
        try:
            return await self._settle_page_task(transition)
        except _LaneClosedError:
            raise
        except BaseException:
            return await self._retain_uncertain_page(
                candidate.page_sequence,
                _types._FinalPageUncertainty.INTEGRITY_FAILURE,
            )

    async def _accept_final_page(
        self,
        candidate: cursor_policy.PageCandidate,
        context: _types.PageFinalizationContext,
        accepted: _types._AcceptedFinalPage,
    ) -> _types.SettledPage:
        """Validate and settle one accepted page as an owned transition."""
        by_shard = self._scheduler._group_final_resolutions(
            accepted.final_closure_resolutions
        )
        async with self._state_lock:
            page = self._require_page_locked()
            if page.page_sequence != candidate.page_sequence:
                message = "page evidence crossed final candidate sequence"
                raise RuntimeError(message)
            evidence_accumulator = page.evidence
        evidence_complete = (
            await self._boundary_coordinator.settle_page_evidence(
                evidence_accumulator
            )
        )
        if not evidence_complete:
            message = "page evidence capture was abandoned before settlement"
            raise _boundaries._BoundaryCoordinatorError(message)
        async with self._scheduler._finalization_lock:
            await self._scheduler._validate_final_pending(by_shard)
            lease_settlement, scheduler_evidence = await self._cover(
                candidate,
                accepted,
            )
            await self._scheduler._resolve_final_pending(by_shard)
            await self._scheduler._clear_replay_barriers(
                self._grant,
                candidate.page_sequence,
                context.replay_blocked_feed_ids,
            )
            await self._scheduler._seal_boundary_page(
                self._grant,
                candidate.page_sequence,
            )
        return _types.SettledPage(
            candidate=candidate,
            lease_settlement=lease_settlement,
            final_closure_resolutions=(accepted.final_closure_resolutions),
            scheduler_evidence=scheduler_evidence,
            source_evidence=accepted.source_evidence,
        )

    async def _retain_uncertain_page(
        self,
        page_sequence: int,
        uncertainty: _types._FinalPageUncertainty,
    ) -> _types.Undrained:
        async with self._state_lock:
            page = self._require_page_locked()
            self._mark_page_uncertain(page, uncertainty)
        await self._scheduler._mark_page_finalization_uncertain(
            self._grant,
            page_sequence,
            uncertainty,
        )
        return _types.Undrained(
            self._grant,
            _types.LaneCloseReason.PLANNED_DRAIN,
        )

    def _validate_final_result(
        self,
        context: _types.PageFinalizationContext,
        result: object,
    ) -> _types._AcceptedFinalPage | None:
        result_types = {
            _types.FinalPageCovered,
            _types.FinalPageNoProgress,
            _types.FinalPageReplayable,
            _types.FinalPageRetryable,
            _types.FinalPageGrantRejected,
            _types.FinalPageOutcomeUnknown,
        }
        if type(result) not in result_types:
            message = "page finalizer returned outside closed vocabulary"
            raise _types.CohortIntegrityError(message)
        correlated = typing.cast("_types.FinalPageResult", result)
        if (
            correlated.grant != context.grant
            or correlated.page_sequence != context.page_sequence
            or correlated.candidate is not context.candidate
        ):
            message = "page final result crossed exact context"
            raise _types.CohortIntegrityError(message)
        if not isinstance(correlated, _types._AcceptedFinalPage):
            return None
        self._require_accepted_disposition(context, correlated)
        rejected_members = self._require_boundary_results(
            context,
            correlated,
        )
        member_retirements = _require_member_retirements(
            context,
            correlated,
            boundary_rejected_members=rejected_members,
        )
        self._require_final_resolutions(
            context,
            correlated,
            member_retirements=member_retirements,
        )
        return correlated

    @staticmethod
    def _require_accepted_disposition(
        context: _types.PageFinalizationContext,
        result: _types._AcceptedFinalPage,
    ) -> None:
        candidate = context.candidate
        if type(result) is _types.FinalPageCovered and (
            type(candidate) is not cursor_policy.PageCursorCandidate
            or context.unresolved_replay_feed_ids
        ):
            message = "covered result disagrees with page replay state"
            raise _types.CohortIntegrityError(message)
        if (
            type(result) is _types.FinalPageNoProgress
            and type(candidate) is not cursor_policy.NoProgressPageCandidate
        ):
            message = "no-progress result crossed candidate kind"
            raise _types.CohortIntegrityError(message)
        if type(result) is _types.FinalPageReplayable and (
            type(candidate) is not cursor_policy.PageCursorCandidate
            or not context.unresolved_replay_feed_ids
        ):
            message = "replayable result lacks exact unresolved Feed"
            raise _types.CohortIntegrityError(message)

    @staticmethod
    def _require_boundary_results(
        context: _types.PageFinalizationContext,
        result: _types._AcceptedFinalPage,
    ) -> tuple[ingestion_lease_store.LeaseMemberIdentity, ...]:
        if len(result.boundary_results) != len(context.candidate_boundaries):
            message = "final boundary result cardinality changed"
            raise _types.CohortIntegrityError(message)
        rejected_members = []
        for boundary, correlated in zip(
            context.candidate_boundaries,
            result.boundary_results,
            strict=True,
        ):
            valid_dispositions = {
                _types.BoundaryDisposition.COMMITTED,
                _types.BoundaryDisposition.MEMBER_REJECTED,
            }
            if (
                correlated.boundary is not boundary
                or correlated.disposition not in valid_dispositions
            ):
                message = "final boundary result lost exact correlation"
                raise _types.CohortIntegrityError(message)
            if correlated.disposition is (
                _types.BoundaryDisposition.MEMBER_REJECTED
            ):
                rejected_members.append(boundary.member)
        return tuple(rejected_members)

    @staticmethod
    def _require_final_resolutions(
        context: _types.PageFinalizationContext,
        result: _types._AcceptedFinalPage,
        *,
        member_retirements: tuple[
            ingestion_lease_store.LeaseMemberIdentity,
            ...,
        ],
    ) -> None:
        pending = tuple(
            record.identity
            for facts in context.cohort_terminal_facts
            for record in facts.records
            if record.closure_state
            is _types.CohortRecordClosureState.FINAL_CLOSURE_PENDING
        )
        resolutions = result.final_closure_resolutions
        if len(resolutions) != len(pending) or any(
            resolution.identity is not identity
            for resolution, identity in zip(
                resolutions,
                pending,
                strict=True,
            )
        ):
            message = "final resolutions do not atomically cover pending work"
            raise _types.CohortIntegrityError(message)
        for resolution in resolutions:
            identity = resolution.identity
            basis = resolution.release_basis
            member_retired = any(
                identity.member is member for member in member_retirements
            )
            if member_retired:
                if (
                    resolution.closure_state
                    is _types.CohortRecordClosureState.REPLAY_SAFE_RELEASE
                    and basis
                    is _types.FinalRecordReleaseBasis.ACCEPTED_MEMBER_RETIREMENT
                ):
                    continue
                message = "retired member retained non-retirement work"
                raise _types.CohortIntegrityError(message)
            if (
                resolution.closure_state
                is _types.CohortRecordClosureState.DURABLY_CLOSED
            ):
                continue
            if (
                basis is _types.FinalRecordReleaseBasis.ACCEPTED_NO_PROGRESS
                and type(result) is _types.FinalPageNoProgress
            ):
                continue
            if (
                basis is _types.FinalRecordReleaseBasis.ACCEPTED_REPLAYABLE_FEED
                and type(result) is _types.FinalPageReplayable
                and identity.feed_id in context.unresolved_replay_feed_ids
            ):
                continue
            if (
                basis
                is _types.FinalRecordReleaseBasis.ACCEPTED_MEMBER_RETIREMENT
                and any(
                    identity.member is member for member in member_retirements
                )
            ):
                continue
            message = "final replay-safe resolution lacks exact acceptance"
            raise _types.CohortIntegrityError(message)

    async def _cover(
        self,
        candidate: cursor_policy.PageCandidate,
        result: _types._AcceptedFinalPage,
    ) -> tuple[
        cursor_policy.PageSettlement,
        _types.SchedulerPageEvidence,
    ]:
        async with self._state_lock:
            self._raise_closed_locked()
            self._validate_candidate_locked(candidate)
            page = self._require_page_locked()
            if page.current_source_order is not None:
                message = "cannot cover a partially admitted source record"
                raise RuntimeError(message)
            if (
                page.pulled_records
                != page.registered_records + page.locally_rejected_records
                or page.terminalized_registered_records
                != page.registered_records
                or not self._terminal_accounting_is_valid(page)
            ):
                message = "page terminal conservation is incomplete"
                raise RuntimeError(message)
            if type(result) is _types.FinalPageCovered:
                settlement = cursor_policy._issue_covered_page(
                    typing.cast(
                        "cursor_policy.PageCursorCandidate",
                        candidate,
                    )
                )
            elif type(result) is _types.FinalPageNoProgress:
                settlement = cursor_policy._issue_no_progress_page(
                    typing.cast(
                        "cursor_policy.NoProgressPageCandidate",
                        candidate,
                    )
                )
            elif type(result) is _types.FinalPageReplayable:
                settlement = cursor_policy._issue_replayable_page(
                    typing.cast(
                        "cursor_policy.PageCursorCandidate",
                        candidate,
                    )
                )
            else:
                message = "result is not an accepted final disposition"
                raise cursor_policy.CursorIntegrityError(message)
            self._scheduler._sample_page_evidence(page.evidence)
            scheduler_evidence = page.evidence.freeze()
            self._next_page_sequence += 1
            self._page = None
            return settlement, scheduler_evidence

    async def _abort_page(self, page: _PageBarrier) -> bool:
        await self._scheduler._abort_boundary_page(
            self._grant,
            page.page_sequence,
        )
        evidence_complete = (
            await self._boundary_coordinator.settle_page_evidence(page.evidence)
        )
        await self._scheduler._purge_page(self._grant, page.page_sequence)
        async with self._state_lock:
            if self._page is page:
                self._page = None
        return evidence_complete

    async def _abort_and_publish_page(self, page: _PageBarrier) -> None:
        """Settle owned abort cleanup and publish only complete evidence."""
        cleanup = self._start_page_settlement(
            self._abort_page(page),
            name=(
                "feed-work-page-abort-"
                f"{self._grant.lease_key}-{page.page_sequence}"
            ),
        )
        evidence_complete = False
        try:
            evidence_complete = await self._settle_page_task(cleanup)
        except BaseException as cleanup_failure:
            self._scheduler._observe_fatal(cleanup_failure)
        self._publish_page_evidence(
            page,
            complete=evidence_complete,
        )

    async def _settle_uncertain_page_evidence(
        self,
        page: _PageBarrier,
    ) -> tuple[bool, bool]:
        """Settle started evidence while remembering caller cancellation."""
        task = self._start_page_settlement(
            self._boundary_coordinator.settle_page_evidence(page.evidence),
            name=(
                "feed-work-page-evidence-"
                f"{self._grant.lease_key}-{page.page_sequence}"
            ),
        )
        cancelled = False
        try:
            while True:
                try:
                    return await asyncio.shield(task), cancelled
                except asyncio.CancelledError:
                    cancelled = True
                    if task.done():
                        return task.result(), cancelled
        finally:
            if self._page_settlement_task is task:
                self._page_settlement_task = None

    def _start_page_settlement(
        self,
        coroutine: collections.abc.Coroutine[object, object, _T],
        *,
        name: str,
    ) -> asyncio.Task[_T]:
        """Register the lane's one bounded page finalization task.

        Args:
            coroutine: Abort or successful-seal work to own until settlement.
            name: Diagnostic asyncio task name.

        Returns:
            The newly registered task.

        Raises:
            RuntimeError: Another page finalization task is still registered.
        """
        if self._page_settlement_task is not None:
            message = "lane already owns a page settlement task"
            raise RuntimeError(message)
        task = asyncio.create_task(coroutine, name=name)
        self._page_settlement_task = task
        return task

    async def _settle_page_task(self, task: asyncio.Task[_T]) -> _T:
        """Keep caller cancellation pending until owned page work settles.

        Args:
            task: The lane-owned abort or seal task.

        Repeated cancellation cannot detach the bounded task. The caller's
        original exception remains active and is re-raised by ``cover_page``.
        """
        try:
            while True:
                try:
                    return await asyncio.shield(task)
                except asyncio.CancelledError:
                    if task.done():
                        return task.result()
                    continue
        finally:
            if self._page_settlement_task is task:
                self._page_settlement_task = None

    def _request_close(
        self,
        reason: _types.LaneCloseReason,
    ) -> asyncio.Task[_types.LaneClosed | _types.Undrained]:
        """Publish/strengthen close before returning an awaitable task."""
        requested = (
            _CloseStrength.DRAINING
            if reason is _types.LaneCloseReason.PLANNED_DRAIN
            else _CloseStrength.CANCELLING
        )
        if self._close_task is not None and self._close_task.done():
            return self._close_task
        if requested > self._close_strength:
            self._close_strength = requested
            self._close_reason = reason
            self._close_changed.set()
        elif (
            requested == self._close_strength
            and reason is _types.LaneCloseReason.AUTHORITY_LOSS
        ):
            self._close_reason = reason
            self._close_changed.set()
        self._scheduler._publish_grant_close(self._grant)
        self._closing = True
        self._closing_event.set()
        if self._close_task is None:
            self._close_task = asyncio.create_task(
                self._coordinate_close(),
                name=(
                    "feed-work-lane-close-"
                    f"{self._grant.lease_key}-{self._grant.fencing_token}"
                ),
            )
        return self._close_task

    async def _coordinate_close(
        self,
    ) -> _types.LaneClosed | _types.Undrained:
        """Purge, drain or cancel, then publish one strongest result."""
        try:
            await self._scheduler._wake_admission()
            async with self._scheduler._finalization_lock:
                await self._boundary_coordinator.close()
                await self._scheduler._purge_exact(
                    self._grant,
                    self._scheduler._settlement_for_closing(self._grant),
                )
            while True:
                if self._close_strength is _CloseStrength.CANCELLING:
                    await self._scheduler._cancel_exact(self._grant)
                    self._scheduler._raise_fatal()
                    await self._scheduler._wait_exact_empty(self._grant)
                else:
                    drained = await self._wait_for_drain_or_upgrade()
                    if not drained:
                        continue
                if self._close_strength is _CloseStrength.CANCELLING:
                    self._scheduler._raise_fatal()
                self._scheduler._raise_fatal()
                self._scheduler._clear_abandonment(self._grant)
                async with self._admission_lock:
                    async with self._state_lock:
                        self._closed = True
                        self._page = None
                self._scheduler._lane_closed(self)
                return _types.LaneClosed(
                    self._grant,
                    self._close_reason,
                )
        except (
            SchedulerIntegrityError,
            _shard._ShardFatalError,
            _shard._ShardUndrainedError,
        ):
            return _types.Undrained(
                self._grant,
                self._close_reason,
            )

    async def _wait_for_drain_or_upgrade(self) -> bool:
        """Wait for exact emptiness unless a stronger close wins."""
        self._close_changed.clear()
        if self._close_strength is _CloseStrength.CANCELLING:
            return False
        drain = asyncio.create_task(
            self._scheduler._wait_exact_empty(self._grant)
        )
        upgrade = asyncio.create_task(self._close_changed.wait())
        try:
            await asyncio.wait(
                (drain, upgrade),
                return_when=asyncio.FIRST_COMPLETED,
            )
            if self._close_strength is _CloseStrength.CANCELLING:
                return False
            await drain
            return True
        finally:
            pending = tuple(
                task for task in (drain, upgrade) if not task.done()
            )
            for task in pending:
                task.cancel()
            await asyncio.gather(drain, upgrade, return_exceptions=True)

    def _validate_candidate_locked(
        self,
        candidate: cursor_policy.PageCandidate,
    ) -> None:
        if type(candidate) not in (
            cursor_policy.PageCursorCandidate,
            cursor_policy.NoProgressPageCandidate,
        ):
            message = "candidate must be an exact PageCandidate"
            raise cursor_policy.CursorIntegrityError(message)
        if candidate.grant != self._grant:
            message = "candidate grant does not match the exact lane"
            raise cursor_policy.CursorIntegrityError(message)
        if candidate.page_sequence != self._next_page_sequence:
            message = "candidate is not the lane's exact next sequence"
            raise cursor_policy.CursorIntegrityError(message)

    def _raise_closed_locked(self) -> None:
        self._scheduler._raise_fatal()
        if self._closing or self._closed or self._closing_event.is_set():
            message = "exact grant lane is closing"
            raise _LaneClosedError(message)

    def _require_page_locked(self) -> _PageBarrier:
        page = self._page
        if page is None:
            message = "lane has no live page"
            raise RuntimeError(message)
        return page

    def _validate_cohort_page(
        self,
        cohorts: tuple[_types.CohortSubmission, ...],
        page_sequence: int,
    ) -> int:
        """Validate every cohort/member/payload before page mutation."""
        _types._require_nonnegative_integer(page_sequence, "page_sequence")
        total_records = 0
        for cohort in cohorts:
            if type(cohort) is not _types.CohortSubmission:
                message = "calls must contain exact CohortSubmission values"
                raise TypeError(message)
            if len(cohort.calls) > self._scheduler._limits.capacity:
                message = "cohort exceeds the hard shard capacity"
                raise ValueError(message)
            ingestion_lease_store._require_member_binding(
                self._grant,
                cohort.member,
            )
            if cohort.member.feed_id != cohort.feed_id:
                message = "cohort member and Feed disagree"
                raise _types.CohortIntegrityError(message)
            if (
                cohort._admission_state.status
                is not _types._AdmissionStatus.UNUSED
            ):
                message = "cohort admission hook is not unused"
                raise _types.CohortIntegrityError(message)
            for submission in cohort.calls:
                if submission.feed_id != cohort.feed_id:
                    message = "call Feed does not match cohort Feed"
                    raise _types.CohortIntegrityError(message)
                if submission.source_timestamp != cohort.cohort_timestamp:
                    message = "call timestamp does not match cohort timestamp"
                    raise _types.CohortIntegrityError(message)
                payload_member = self._payload_member(submission.payload)
                ingestion_lease_store._require_member_binding(
                    self._grant,
                    payload_member,
                )
                if payload_member is not cohort.member:
                    message = "call payload member crossed cohort identity"
                    raise _types.CohortIntegrityError(message)
            total_records += len(cohort.calls)
        return total_records

    @staticmethod
    def _payload_member(
        payload: object,
    ) -> ingestion_lease_store.LeaseMemberIdentity:
        candidate = getattr(payload, "member", None)
        if isinstance(candidate, ingestion_lease_store.LeaseMember):
            candidate = candidate.identity
        if isinstance(payload, collections.abc.Mapping):
            mapping = typing.cast(
                "collections.abc.Mapping[object, object]",
                payload,
            )
            candidate = mapping.get("member", candidate)
            if isinstance(candidate, ingestion_lease_store.LeaseMember):
                candidate = candidate.identity
        if not isinstance(
            candidate,
            ingestion_lease_store.LeaseMemberIdentity,
        ):
            message = "call payload must carry an issued member"
            raise _types.CohortIntegrityError(message)
        return candidate

    @staticmethod
    def _require_boundary(
        boundary: object,
    ) -> None:
        if type(boundary) is not _types.BoundaryWork:
            message = "boundaries must contain exact BoundaryWork values"
            raise TypeError(message)

    @staticmethod
    def _require_no_progress_boundaries_empty(
        boundaries: collections.abc.Iterable[_types.BoundaryWork],
    ) -> None:
        iterator = iter(boundaries)
        try:
            next(iterator)
        except StopIteration:
            return
        message = "no-progress pages must not contain boundaries"
        raise cursor_policy.CursorIntegrityError(message)

    @staticmethod
    def _require_final_boundary_settlement(
        result: _types._BoundaryPressureResult,
    ) -> None:
        if result is _types._BoundaryPressureResult.RETRYABLE:
            message = "final logical boundary flush is retryable"
            raise RuntimeError(message)

    def _notify_unadmitted_settlement(
        self,
        submission: _types.CallSubmission,
        settlement: _types.CallSettlement,
    ) -> None:
        observer = submission.settlement_observer
        if observer is None:
            return
        try:
            observer(settlement)
        except BaseException as exc:
            self._scheduler._observe_fatal(exc)
            message = "call settlement observer failed"
            raise SchedulerIntegrityError(message) from exc

    def _notify_unadmitted_cohort(
        self,
        cohort: _types.CohortSubmission,
        settlement: _types.CallSettlement,
    ) -> None:
        failure: BaseException | None = None
        for submission in cohort.calls:
            try:
                self._notify_unadmitted_settlement(
                    submission,
                    settlement,
                )
            except BaseException as exc:
                if failure is None:
                    failure = exc
        if failure is not None:
            raise failure
