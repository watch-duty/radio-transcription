"""Authoritative bounded state for one Feed-affine scheduler shard."""

# Private sibling modules intentionally share the scheduler's closed internals.
# ruff: noqa: SLF001

from __future__ import annotations

import asyncio
import collections
import dataclasses
import typing
import uuid

from backend.pipeline.ingestion.feed_work_scheduler import _types
from backend.pipeline.storage import ingestion_lease_store


class _ShardClosedError(RuntimeError):
    """Admission reached a shard that no longer accepts work."""


class _FeedRetiredError(RuntimeError):
    """Admission reached a Feed retired from this shard."""


class _AdmissionAbortedError(RuntimeError):
    """Admission observed its exact lane closing before registration."""


class _ShardFatalError(RuntimeError):
    """Admission or coordination observed persistent integrity failure."""

    def __init__(self, failure: BaseException) -> None:
        super().__init__("shard integrity failed")
        self.failure = failure


class _ShardUndrainedError(RuntimeError):
    """A close or replacement was attempted while work remained owned."""


class _UnexpectedWorkerCancellation(RuntimeError):
    """A worker was cancelled without registered cancellation intent."""


class _InvalidExecutorOutcome(RuntimeError):
    """An executor returned outside the closed outcome vocabulary."""


class _BoundaryReliefRetryableError(RuntimeError):
    """A pressure generation settled retryably and admission must abort."""


@dataclasses.dataclass(slots=True)
class _WorkerSlot:
    slot_id: int
    task: asyncio.Task[None] | None = None
    active_record: _types._CallRecord | None = None
    cancellation_sequence: int | None = None
    cancel_expected: bool = False
    abandoned: bool = False


@dataclasses.dataclass(frozen=True, slots=True)
class _CancellationRequest:
    """Exact registered worker cancellation awaiting settlement."""

    slot_id: int
    local_sequence: int
    task: asyncio.Task[None]


_TERMINAL_EXECUTOR_OUTCOMES = (
    _types._ExecutorCompleted,
    _types._ExecutorRetryable,
    _types._ExecutorAuthorityLost,
    _types._ExecutorMembershipRejected,
)


class _Shard:
    """One lock-protected held-token state machine with fixed workers."""

    def __init__(  # noqa: PLR0915
        self,
        shard_id: int,
        executor: _types.CallExecutor,
        *,
        limits: _types._SchedulerLimits = _types._PRODUCTION_LIMITS,
        outcome_observer: typing.Callable[
            [
                _types._CallRecord,
                _types._ExecutorOutcome,
                _types._RetireFeedResult | None,
            ],
            None,
        ]
        | None = None,
        grant_is_closing: typing.Callable[
            [ingestion_lease_store.LeaseGrant],
            bool,
        ]
        | None = None,
        fatal_observer: typing.Callable[[BaseException], None] | None = None,
        global_fatal: typing.Callable[[], BaseException | None] | None = None,
        abandonment_for: typing.Callable[
            [ingestion_lease_store.LeaseGrant],
            BaseException | None,
        ]
        | None = None,
        boundary_ready_observer: typing.Callable[
            [ingestion_lease_store.LeaseGrant],
            None,
        ]
        | None = None,
        closing_settlement: typing.Callable[
            [ingestion_lease_store.LeaseGrant],
            _types.CallSettlement,
        ]
        | None = None,
    ) -> None:
        """Create one authoritative shard with injected coordination seams.

        Args:
            shard_id: Stable index within the scheduler's immutable shards.
            executor: Full-pipeline adapter used by fixed worker slots.
            limits: Validated capacity and worker limits.
            outcome_observer: Optional terminal membership/loss observer.
            grant_is_closing: Optional exact-grant dispatch rejection check.
            fatal_observer: Optional process-level failure publisher.
            global_fatal: Optional process-level failure reader.
            abandonment_for: Optional external cancellation evidence reader.
            boundary_ready_observer: Optional exact-grant flusher notifier.
            closing_settlement: Optional exact-grant close outcome mapper.

        Raises:
            TypeError: An identifier, limit, or injected seam has wrong type.
            ValueError: ``shard_id`` is outside the configured shard count.
        """
        if isinstance(shard_id, bool) or not isinstance(shard_id, int):
            message = "shard_id must be an integer"
            raise TypeError(message)
        if not isinstance(limits, _types._SchedulerLimits):
            message = "limits must be _SchedulerLimits"
            raise TypeError(message)
        if shard_id < 0 or shard_id >= limits.shard_count:
            message = "shard_id is outside configured shard_count"
            raise ValueError(message)
        if not callable(getattr(executor, "execute", None)):
            message = "executor must provide async execute(record)"
            raise TypeError(message)
        if outcome_observer is not None and not callable(outcome_observer):
            message = "outcome_observer must be callable or None"
            raise TypeError(message)
        if grant_is_closing is not None and not callable(grant_is_closing):
            message = "grant_is_closing must be callable or None"
            raise TypeError(message)
        if fatal_observer is not None and not callable(fatal_observer):
            message = "fatal_observer must be callable or None"
            raise TypeError(message)
        if global_fatal is not None and not callable(global_fatal):
            message = "global_fatal must be callable or None"
            raise TypeError(message)
        if abandonment_for is not None and not callable(abandonment_for):
            message = "abandonment_for must be callable or None"
            raise TypeError(message)
        if boundary_ready_observer is not None and not callable(
            boundary_ready_observer
        ):
            message = "boundary_ready_observer must be callable or None"
            raise TypeError(message)
        if closing_settlement is not None and not callable(closing_settlement):
            message = "closing_settlement must be callable or None"
            raise TypeError(message)

        self.shard_id = shard_id
        self._executor = executor
        self._outcome_observer = outcome_observer
        self._grant_is_closing = grant_is_closing
        self._fatal_observer = fatal_observer
        self._global_fatal = global_fatal
        self._abandonment_for = abandonment_for
        self._boundary_ready_observer = boundary_ready_observer
        self._closing_settlement = closing_settlement
        self._limits = limits
        self._lock = asyncio.Lock()
        self._work_ready = asyncio.Condition(self._lock)
        self._capacity_changed = asyncio.Condition(self._lock)
        self._fatal_event = asyncio.Event()
        self._worker_changed = asyncio.Event()

        self._held = 0
        self._pressure_paused = False
        self._next_sequence = 0
        self._feed_queues: dict[
            uuid.UUID,
            collections.deque[_types._CallRecord],
        ] = {}
        self._ready: collections.deque[uuid.UUID] = collections.deque()
        self._ready_members: set[uuid.UUID] = set()
        self._records: dict[int, _types._CallRecord] = {}
        self._active_by_feed: dict[uuid.UUID, _types._CallRecord] = {}
        self._active_boundaries: dict[
            uuid.UUID,
            _types._BoundaryRecord,
        ] = {}
        self._retired_scopes: set[
            tuple[ingestion_lease_store.LeaseGrant, uuid.UUID]
        ] = set()
        self._pending_boundaries: dict[
            tuple[ingestion_lease_store.LeaseGrant, uuid.UUID],
            _types._BoundaryRecord,
        ] = {}
        self._flushing_boundaries: dict[
            int,
            _types._BoundaryRecord,
        ] = {}
        self._capacity_waiters = 0

        self._workers = [
            _WorkerSlot(slot_id)
            for slot_id in range(self._limits.workers_per_shard)
        ]
        self._started = False
        self._stopping = False
        self._closed = False
        self._admission_open = True
        self._fatal: BaseException | None = None

    @property
    def fatal_failure(self) -> BaseException | None:
        """Return the first persistent integrity failure, if any."""
        return self._fatal

    async def start(self) -> None:
        """Register exactly the configured fixed worker tasks once."""
        async with self._lock:
            self._raise_fatal_locked()
            if self._closed or self._stopping:
                message = "cannot start a closing shard"
                raise _ShardClosedError(message)
            if self._started:
                return
            self._started = True
            for slot in self._workers:
                self._spawn_worker_locked(slot)

    async def admit(
        self,
        work: _types._CallWork,
        *,
        abort_event: asyncio.Event | None = None,
    ) -> _types._CallRecord:
        """Atomically register one capacity-owning source record."""
        if not isinstance(work, _types._CallWork):
            message = "work must be _CallWork"
            raise TypeError(message)
        if abort_event is not None and not isinstance(
            abort_event,
            asyncio.Event,
        ):
            message = "abort_event must be an asyncio.Event"
            raise TypeError(message)

        async with self._capacity_changed:
            while True:
                self._raise_admission_error_locked(work, abort_event)
                if (
                    not self._pressure_paused
                    and self._held < self._limits.capacity
                ):
                    record = _types._CallRecord(
                        work=work,
                        local_sequence=self._next_sequence,
                    )
                    self._next_sequence += 1
                    self._records[record.local_sequence] = record
                    queue = self._feed_queues.setdefault(
                        record.feed_id,
                        collections.deque(),
                    )
                    queue.append(record)
                    self._held += 1
                    if self._held >= self._limits.high_water:
                        self._pressure_paused = True
                    if (
                        record.feed_id not in self._active_by_feed
                        and record.feed_id not in self._active_boundaries
                    ):
                        self._ensure_ready_locked(record.feed_id)
                    self._check_conservation_locked()
                    self._work_ready.notify_all()
                    return record

                self._capacity_waiters += 1
                self._capacity_changed.notify_all()
                try:
                    await self._capacity_changed.wait()
                finally:
                    self._capacity_waiters -= 1

    async def admit_boundary(  # noqa: PLR0912
        self,
        boundary_input: _types._BoundaryInput,
        *,
        abort_event: asyncio.Event,
        pressure_relief: typing.Callable[
            [],
            typing.Awaitable[_types._BoundaryPressureResult],
        ],
    ) -> _types._BoundaryRecord:
        """Count or coalesce one current trailing boundary incrementally."""
        if not isinstance(boundary_input, _types._BoundaryInput):
            message = "boundary_input must be a _BoundaryInput"
            raise TypeError(message)
        if not isinstance(abort_event, asyncio.Event):
            message = "abort_event must be an asyncio.Event"
            raise TypeError(message)
        if not callable(pressure_relief):
            message = "pressure_relief must be callable"
            raise TypeError(message)

        relief_requested = False
        while True:
            request_relief = False
            async with self._capacity_changed:
                self._raise_boundary_admission_error_locked(
                    boundary_input,
                    abort_event,
                )
                scope = (
                    boundary_input.grant,
                    boundary_input.boundary.feed_id,
                )
                pending = self._pending_boundaries.get(scope)
                if pending is not None:
                    self._coalesce_boundary_locked(pending, boundary_input)
                    self._check_conservation_locked()
                    if self._boundary_is_ready_locked(pending):
                        self._notify_boundary_ready_locked(pending.grant)
                    return pending
                if (
                    not self._pressure_paused
                    and self._held < self._limits.capacity
                ):
                    record = _types._BoundaryRecord(
                        grant=boundary_input.grant,
                        member=boundary_input.boundary.member,
                        local_sequence=self._next_sequence,
                        source_order=boundary_input.source_order,
                        created_page_sequence=(boundary_input.page_sequence),
                        target=boundary_input.boundary.target,
                        stable_target=None,
                        provisional_page_sequence=(
                            boundary_input.page_sequence
                        ),
                        provisional_count=1,
                        state=_types._RecordState.PENDING_BOUNDARY,
                    )
                    self._next_sequence += 1
                    self._pending_boundaries[scope] = record
                    self._held += 1
                    if self._held >= self._limits.high_water:
                        self._pressure_paused = True
                    self._capacity_changed.notify_all()
                    self._check_conservation_locked()
                    if self._boundary_is_ready_locked(record):
                        self._notify_boundary_ready_locked(record.grant)
                    return record
                if not relief_requested:
                    request_relief = True
                else:
                    self._capacity_waiters += 1
                    self._capacity_changed.notify_all()
                    try:
                        await self._capacity_changed.wait()
                    finally:
                        self._capacity_waiters -= 1
            if request_relief:
                relief_requested = True
                result = await pressure_relief()
                if result is _types._BoundaryPressureResult.RETRYABLE:
                    message = "boundary pressure relief settled retryably"
                    raise _BoundaryReliefRetryableError(message)

    async def select_boundary_batch(
        self,
        grant: ingestion_lease_store.LeaseGrant,
        limit: int,
        *,
        include_suspended: bool = False,
    ) -> tuple[_types._BoundaryRecord, ...]:
        """Detach a bounded ready exact-grant prefix under the shard lock."""
        if not isinstance(grant, ingestion_lease_store.LeaseGrant):
            message = "grant must be a LeaseGrant"
            raise TypeError(message)
        _types._require_positive_integer(limit, "limit")
        if not isinstance(include_suspended, bool):
            message = "include_suspended must be a boolean"
            raise TypeError(message)
        async with self._lock:
            self._raise_fatal_locked()
            selected = []
            candidates = sorted(
                self._pending_boundaries.items(),
                key=lambda item: item[1].local_sequence,
            )
            for scope, record in candidates:
                if len(selected) >= limit:
                    break
                if record.grant != grant:
                    continue
                if not self._boundary_is_ready_locked(
                    record,
                    include_suspended=include_suspended,
                ):
                    continue
                del self._pending_boundaries[scope]
                record.state = _types._RecordState.FLUSHING_BOUNDARY
                record.retry_suspended = False
                self._flushing_boundaries[record.local_sequence] = record
                self._active_boundaries[record.feed_id] = record
                selected.append(record)
            self._check_conservation_locked()
            return tuple(selected)

    async def apply_boundary_results(
        self,
        results: tuple[
            tuple[_types._BoundaryRecord, _types.BoundaryDisposition],
            ...,
        ],
    ) -> bool:
        """Settle one already-validated immutable batch exactly once."""
        retryable = False
        released_calls = []
        async with self._lock:
            for record, disposition in results:
                current = self._flushing_boundaries.get(record.local_sequence)
                if current is not record:
                    message = "flushing boundary identity changed"
                    raise RuntimeError(message)
                if self._active_boundaries.get(record.feed_id) is not record:
                    message = "boundary Feed ownership changed"
                    raise RuntimeError(message)
                del self._flushing_boundaries[record.local_sequence]
                del self._active_boundaries[record.feed_id]
                if disposition is _types.BoundaryDisposition.RETRYABLE:
                    retryable = True
                    self._restore_retryable_boundary_locked(record)
                else:
                    self._held -= 1
                    if (
                        disposition
                        is _types.BoundaryDisposition.MEMBER_REJECTED
                    ):
                        retirement = self._retire_feed_locked(
                            record.grant,
                            record.feed_id,
                        )
                        released_calls.extend(retirement.released_call_records)
                    self._ready_call_or_boundary_locked(record.feed_id)
                    self._after_release_locked(1)
            self._check_conservation_locked()
        await self._notify_settlements(
            tuple(released_calls),
            _types.CallSettlement.MEMBERSHIP_REJECTED,
        )
        return retryable

    async def discard_boundary_batch(
        self,
        records: tuple[_types._BoundaryRecord, ...],
    ) -> None:
        """Release a settled exact-fence-rejected immutable batch."""
        async with self._lock:
            released = 0
            for record in records:
                if (
                    self._flushing_boundaries.get(record.local_sequence)
                    is not record
                ):
                    message = "rejected boundary identity changed"
                    raise RuntimeError(message)
                del self._flushing_boundaries[record.local_sequence]
                self._active_boundaries.pop(record.feed_id, None)
                self._held -= 1
                released += 1
                self._ready_call_or_boundary_locked(record.feed_id)
            self._after_release_locked(released)
            self._check_conservation_locked()

    async def restore_boundary_batch(
        self,
        records: tuple[_types._BoundaryRecord, ...],
    ) -> None:
        """Restore a whole transiently failed batch without item outcomes."""
        async with self._lock:
            for record in records:
                if (
                    self._flushing_boundaries.get(record.local_sequence)
                    is not record
                ):
                    message = "retryable boundary identity changed"
                    raise RuntimeError(message)
                if self._active_boundaries.get(record.feed_id) is not record:
                    message = "retryable boundary Feed ownership changed"
                    raise RuntimeError(message)
                del self._flushing_boundaries[record.local_sequence]
                del self._active_boundaries[record.feed_id]
                self._restore_retryable_boundary_locked(record)
            self._check_conservation_locked()

    async def promote_boundary_page(
        self,
        grant: ingestion_lease_store.LeaseGrant,
        page_sequence: int,
    ) -> None:
        """Prepare current-page contributions without losing provenance."""
        async with self._lock:
            for record in self._pending_boundaries.values():
                if (
                    record.grant == grant
                    and record.provisional_page_sequence == page_sequence
                ):
                    prepared = record.promotion_page_sequence
                    if prepared not in (None, page_sequence):
                        message = "boundary promotion crossed live pages"
                        raise RuntimeError(message)
                    record.promotion_rollback_target = record.stable_target
                    record.stable_target = record.target
                    record.provisional_page_sequence = None
                    record.provisional_count = 0
                    record.promotion_page_sequence = page_sequence
            self._check_conservation_locked()

    async def seal_boundary_page(
        self,
        grant: ingestion_lease_store.LeaseGrant,
        page_sequence: int,
    ) -> None:
        """Stabilize prepared contributions after coverage has won."""
        async with self._lock:
            for record in self._pending_boundaries.values():
                if record.grant != grant:
                    continue
                if record.promotion_page_sequence == page_sequence:
                    if record.provisional_page_sequence is not None:
                        message = "promoted boundary retained provisional state"
                        raise RuntimeError(message)
                    record.promotion_page_sequence = None
                    record.promotion_rollback_target = None
                    continue
                if record.provisional_page_sequence == page_sequence:
                    message = "boundary was not prepared before sealing"
                    raise RuntimeError(message)
            self._check_conservation_locked()

    async def abort_boundary_page(
        self,
        grant: ingestion_lease_store.LeaseGrant,
        page_sequence: int,
    ) -> None:
        """Roll back only a still-pending current-page contribution."""
        async with self._lock:
            released = 0
            for scope, record in tuple(self._pending_boundaries.items()):
                if record.grant != grant:
                    continue
                provisional = record.provisional_page_sequence == page_sequence
                promoted = record.promotion_page_sequence == page_sequence
                if not provisional and not promoted:
                    continue
                rollback_target = (
                    record.promotion_rollback_target
                    if promoted
                    else record.stable_target
                )
                if rollback_target is None:
                    del self._pending_boundaries[scope]
                    self._held -= 1
                    released += 1
                else:
                    record.target = rollback_target
                    record.stable_target = rollback_target
                    record.provisional_page_sequence = None
                    record.provisional_count = 0
                    record.promotion_page_sequence = None
                    record.promotion_rollback_target = None
            for record in self._flushing_boundaries.values():
                if record.grant == grant and page_sequence in (
                    record.provisional_page_sequence,
                    record.promotion_page_sequence,
                ):
                    record.aborted_page_sequence = page_sequence
            self._after_release_locked(released)
            self._check_conservation_locked()

    async def has_ready_boundary(
        self,
        grant: ingestion_lease_store.LeaseGrant,
    ) -> bool:
        """Return whether this shard retains another ready exact boundary."""
        async with self._lock:
            return any(
                record.grant == grant and self._boundary_is_ready_locked(record)
                for record in self._pending_boundaries.values()
            )

    async def snapshot(self) -> _types._ShardSnapshot:
        """Return a payload-free bounded state projection."""
        async with self._lock:
            self._check_conservation_locked()
            queued_sequences = set()
            for queue in self._feed_queues.values():
                for record in queue:
                    queued_sequences.add(record.local_sequence)
            active_slots = {
                slot.active_record.local_sequence: slot.slot_id
                for slot in self._workers
                if slot.active_record is not None
            }
            records = tuple(
                _types._RecordSnapshot(
                    local_sequence=record.local_sequence,
                    feed_id=record.feed_id,
                    grant=record.grant,
                    source_order=record.work.source_order,
                    page_sequence=record.work.page_sequence,
                    state=(
                        _types._RecordState.QUEUED
                        if record.local_sequence in queued_sequences
                        else _types._RecordState.ACTIVE
                    ),
                    worker_slot=active_slots.get(record.local_sequence),
                )
                for record in sorted(
                    self._records.values(),
                    key=lambda value: value.local_sequence,
                )
            )
            workers = tuple(
                _types._WorkerSnapshot(
                    slot_id=slot.slot_id,
                    task_registered=slot.task is not None,
                    task_done=(
                        slot.task.done() if slot.task is not None else False
                    ),
                    active_sequence=(
                        slot.active_record.local_sequence
                        if slot.active_record is not None
                        else None
                    ),
                    cancellation_sequence=slot.cancellation_sequence,
                )
                for slot in self._workers
            )
            boundaries = tuple(
                _types._BoundarySnapshot(
                    local_sequence=record.local_sequence,
                    feed_id=record.feed_id,
                    grant=record.grant,
                    source_order=record.source_order,
                    created_page_sequence=record.created_page_sequence,
                    target=record.target,
                    stable_target=record.stable_target,
                    provisional_page_sequence=(
                        record.provisional_page_sequence
                    ),
                    provisional_count=record.provisional_count,
                    state=record.state,
                    retry_suspended=record.retry_suspended,
                )
                for record in sorted(
                    (
                        *self._pending_boundaries.values(),
                        *self._flushing_boundaries.values(),
                    ),
                    key=lambda value: value.local_sequence,
                )
            )
            return _types._ShardSnapshot(
                held=self._held,
                queued_calls=len(queued_sequences),
                active_calls=len(self._active_by_feed),
                pending_boundaries=len(self._pending_boundaries),
                flushing_boundaries=len(self._flushing_boundaries),
                pressure_paused=self._pressure_paused,
                ready_feeds=tuple(self._ready),
                ready_members=frozenset(self._ready_members),
                active_feeds=frozenset(
                    {*self._active_by_feed, *self._active_boundaries}
                ),
                records=records,
                boundaries=boundaries,
                workers=workers,
                retired_scopes=frozenset(self._retired_scopes),
                admission_open=self._admission_open,
                fatal=self._fatal is not None,
            )

    async def _take_next(
        self,
        slot_id: int,
        *,
        wait: bool = True,
    ) -> _types._CallRecord | None:
        """Move one fair FIFO record from queued to active count-neutrally."""
        slot = self._require_slot(slot_id)
        while True:
            released: _types._CallRecord | None = None
            async with self._work_ready:
                while True:
                    if (
                        self._fatal is not None
                        or self._global_fatal_failure() is not None
                        or self._stopping
                    ):
                        return None
                    if slot.active_record is not None:
                        message = "worker slot already owns an active record"
                        raise RuntimeError(message)
                    if self._ready:
                        feed_id = self._ready.popleft()
                        self._ready_members.remove(feed_id)
                        if feed_id in self._active_by_feed:
                            message = "ready Feed already owns an active record"
                            raise RuntimeError(message)
                        queue = self._feed_queues[feed_id]
                        record = queue.popleft()
                        if not queue:
                            del self._feed_queues[feed_id]
                        if (
                            self._grant_is_closing is not None
                            and self._grant_is_closing(record.grant)
                        ):
                            del self._records[record.local_sequence]
                            self._held -= 1
                            if feed_id in self._feed_queues:
                                self._ensure_ready_locked(feed_id)
                            self._after_release_locked(1)
                            self._check_conservation_locked()
                            released = record
                            break
                        self._active_by_feed[feed_id] = record
                        slot.active_record = record
                        self._check_conservation_locked()
                        return record
                    if not wait:
                        return None
                    await self._work_ready.wait()
            if released is not None:
                await self._notify_settlements(
                    (released,),
                    self._settlement_for_closing(released.grant),
                )

    async def _terminalize(
        self,
        slot_id: int,
        record: _types._CallRecord,
        outcome: _types._ExecutorOutcome,
    ) -> None:
        """Release one settled active record without retaining its outcome."""
        if not isinstance(outcome, _TERMINAL_EXECUTOR_OUTCOMES):
            message = "outcome is not scheduling-terminal"
            raise TypeError(message)
        retirement = None
        async with self._lock:
            released = self._terminalize_locked(slot_id, record)
            if not released:
                message = "active record no longer belongs to worker slot"
                raise RuntimeError(message)
            if isinstance(outcome, _types._ExecutorMembershipRejected):
                retirement = self._retire_feed_locked(
                    record.grant,
                    record.feed_id,
                )
        await self._notify_settlements(
            (record,),
            self._settlement_for_outcome(outcome),
        )
        if retirement is not None:
            await self._notify_settlements(
                retirement.released_call_records,
                _types.CallSettlement.MEMBERSHIP_REJECTED,
            )
        if (
            isinstance(
                outcome,
                (
                    _types._ExecutorAuthorityLost,
                    _types._ExecutorMembershipRejected,
                ),
            )
            and self._outcome_observer is not None
        ):
            self._outcome_observer(record, outcome, retirement)

    async def purge_exact(
        self,
        grant: ingestion_lease_store.LeaseGrant,
        *,
        settlement: _types.CallSettlement = _types.CallSettlement.ABORTED,
    ) -> _types._PurgeResult:
        """Release queued records matching the complete grant only."""
        if not isinstance(grant, ingestion_lease_store.LeaseGrant):
            message = "grant must be a LeaseGrant"
            raise TypeError(message)
        if not isinstance(settlement, _types.CallSettlement):
            message = "settlement must be a CallSettlement"
            raise TypeError(message)
        released_records = []
        async with self._lock:
            released: list[int] = []
            for feed_id, queue in tuple(self._feed_queues.items()):
                kept: collections.deque[_types._CallRecord] = (
                    collections.deque()
                )
                for record in queue:
                    if record.grant == grant:
                        released.append(record.local_sequence)
                        released_records.append(record)
                        del self._records[record.local_sequence]
                        self._held -= 1
                    else:
                        kept.append(record)
                if kept:
                    self._feed_queues[feed_id] = kept
                else:
                    del self._feed_queues[feed_id]
                    self._remove_ready_locked(feed_id)
            active = tuple(
                sorted(
                    record.local_sequence
                    for record in (
                        *self._active_by_feed.values(),
                        *self._flushing_boundaries.values(),
                    )
                    if record.grant == grant
                )
            )
            for scope, record in tuple(self._pending_boundaries.items()):
                if record.grant != grant:
                    continue
                del self._pending_boundaries[scope]
                released.append(record.local_sequence)
                self._held -= 1
            self._after_release_locked(len(released))
            self._check_conservation_locked()
            result = _types._PurgeResult(
                released_sequences=tuple(sorted(released)),
                active_sequences=active,
            )
        await self._notify_settlements(tuple(released_records), settlement)
        return result

    async def purge_page(
        self,
        grant: ingestion_lease_store.LeaseGrant,
        page_sequence: int,
    ) -> _types._PurgeResult:
        """Release only queued calls from one exact grant and page."""
        if not isinstance(grant, ingestion_lease_store.LeaseGrant):
            message = "grant must be a LeaseGrant"
            raise TypeError(message)
        _types._require_nonnegative_integer(page_sequence, "page_sequence")
        released_records = []
        async with self._lock:
            released: list[int] = []
            for feed_id, queue in tuple(self._feed_queues.items()):
                kept: collections.deque[_types._CallRecord] = (
                    collections.deque()
                )
                for record in queue:
                    if (
                        record.grant == grant
                        and record.work.page_sequence == page_sequence
                    ):
                        released.append(record.local_sequence)
                        released_records.append(record)
                        del self._records[record.local_sequence]
                        self._held -= 1
                    else:
                        kept.append(record)
                if kept:
                    self._feed_queues[feed_id] = kept
                else:
                    del self._feed_queues[feed_id]
                    self._remove_ready_locked(feed_id)
            active = tuple(
                sorted(
                    record.local_sequence
                    for record in self._active_by_feed.values()
                    if record.grant == grant
                    and record.work.page_sequence == page_sequence
                )
            )
            for scope, record in tuple(self._pending_boundaries.items()):
                if record.grant != grant or page_sequence not in (
                    record.provisional_page_sequence,
                    record.promotion_page_sequence,
                ):
                    continue
                rollback_target = (
                    record.promotion_rollback_target
                    if record.promotion_page_sequence == page_sequence
                    else record.stable_target
                )
                if rollback_target is not None:
                    continue
                del self._pending_boundaries[scope]
                released.append(record.local_sequence)
                self._held -= 1
            active = tuple(
                sorted(
                    (
                        *active,
                        *(
                            record.local_sequence
                            for record in self._flushing_boundaries.values()
                            if record.grant == grant
                            and page_sequence
                            in (
                                record.provisional_page_sequence,
                                record.promotion_page_sequence,
                            )
                        ),
                    )
                )
            )
            self._after_release_locked(len(released))
            self._check_conservation_locked()
            result = _types._PurgeResult(
                released_sequences=tuple(sorted(released)),
                active_sequences=active,
            )
        await self._notify_settlements(
            tuple(released_records),
            _types.CallSettlement.ABORTED,
        )
        return result

    async def retire_feed(
        self,
        grant: ingestion_lease_store.LeaseGrant,
        feed_id: uuid.UUID,
    ) -> _types._RetireFeedResult:
        """Reject one exact grant/Feed and purge only its queued calls."""
        if not isinstance(grant, ingestion_lease_store.LeaseGrant):
            message = "grant must be a LeaseGrant"
            raise TypeError(message)
        if not isinstance(feed_id, uuid.UUID):
            message = "feed_id must be a UUID"
            raise TypeError(message)
        async with self._lock:
            result = self._retire_feed_locked(grant, feed_id)
        await self._notify_settlements(
            result.released_call_records,
            _types.CallSettlement.MEMBERSHIP_REJECTED,
        )
        return result

    async def forget_retired_grant(
        self,
        grant: ingestion_lease_store.LeaseGrant,
    ) -> None:
        """Drop Feed-removal history after exact mutation closure."""
        if not isinstance(grant, ingestion_lease_store.LeaseGrant):
            message = "grant must be a LeaseGrant"
            raise TypeError(message)
        async with self._lock:
            self._retired_scopes = {
                scope for scope in self._retired_scopes if scope[0] != grant
            }

    def _retire_feed_locked(
        self,
        grant: ingestion_lease_store.LeaseGrant,
        feed_id: uuid.UUID,
    ) -> _types._RetireFeedResult:
        """Retire one exact scope while the shard lock is held."""
        self._retired_scopes.add((grant, feed_id))
        queue = self._feed_queues.get(feed_id, collections.deque())
        kept: collections.deque[_types._CallRecord] = collections.deque()
        released = []
        released_call_records = []
        released_calls = []
        released_boundaries = []
        for record in queue:
            if record.grant == grant:
                released.append(record.local_sequence)
                released_call_records.append(record)
                released_calls.append(
                    (record.work.page_sequence, record.work.source_order)
                )
                del self._records[record.local_sequence]
                self._held -= 1
            else:
                kept.append(record)
        if kept:
            self._feed_queues[feed_id] = kept
        else:
            self._feed_queues.pop(feed_id, None)
            self._remove_ready_locked(feed_id)
        scope = (grant, feed_id)
        pending_boundary = self._pending_boundaries.pop(scope, None)
        if pending_boundary is not None:
            released.append(pending_boundary.local_sequence)
            if pending_boundary.provisional_page_sequence is not None:
                released_boundaries.append(
                    (
                        pending_boundary.provisional_page_sequence,
                        pending_boundary.provisional_count,
                    )
                )
            self._held -= 1
        active = self._active_by_feed.get(feed_id)
        active_sequence = (
            active.local_sequence
            if active is not None and active.grant == grant
            else None
        )
        active_boundary = self._active_boundaries.get(feed_id)
        if active_boundary is not None and active_boundary.grant == grant:
            active_sequence = active_boundary.local_sequence
        self._after_release_locked(len(released))
        self._capacity_changed.notify_all()
        self._work_ready.notify_all()
        self._check_conservation_locked()
        return _types._RetireFeedResult(
            released_sequences=tuple(released),
            released_call_records=tuple(released_call_records),
            released_calls=tuple(released_calls),
            released_boundaries=tuple(released_boundaries),
            active_sequence=active_sequence,
        )

    async def request_cancel_exact(
        self,
        grant: ingestion_lease_store.LeaseGrant,
    ) -> tuple[_CancellationRequest, ...]:
        """Mark exact active ownership before cancelling fixed workers."""
        if not isinstance(grant, ingestion_lease_store.LeaseGrant):
            message = "grant must be a LeaseGrant"
            raise TypeError(message)
        async with self._lock:
            requests = []
            abandonment = self._abandonment_failure(grant)
            for slot in self._workers:
                record = slot.active_record
                task = slot.task
                if (
                    record is None
                    or record.grant != grant
                    or task is None
                    or task.done()
                ):
                    continue
                slot.cancel_expected = True
                slot.cancellation_sequence = record.local_sequence
                slot.abandoned = abandonment is not None
                requests.append(
                    _CancellationRequest(
                        slot_id=slot.slot_id,
                        local_sequence=record.local_sequence,
                        task=task,
                    )
                )
            if requests and abandonment is not None:
                self._mark_fatal_locked(abandonment)
        for request in requests:
            request.task.cancel()
        return tuple(requests)

    async def cancel_active_exact(
        self,
        grant: ingestion_lease_store.LeaseGrant,
    ) -> tuple[int, ...]:
        """Settle expected exact cancellations before reusing worker slots."""
        requests = await self.request_cancel_exact(grant)
        return await self.settle_cancellations(requests)

    async def settle_cancellations(
        self,
        requests: tuple[_CancellationRequest, ...],
    ) -> tuple[int, ...]:
        """Await already-issued cancellation requests before slot reuse."""
        settled = []
        for request in requests:
            while not request.task.done() and self._fatal is None:
                self._worker_changed.clear()
                if request.task.done() or self._fatal is not None:
                    break
                await self._worker_changed.wait()
            if not request.task.done():
                continue
            try:
                await request.task
            except asyncio.CancelledError:
                pass
            except BaseException as exc:
                await self._mark_fatal(exc)
            await self._replace_cancelled_worker(request)
            settled.append(request.slot_id)
        return tuple(settled)

    async def abandon_cancellation(
        self,
        slot_id: int,
        failure: BaseException,
    ) -> None:
        """Fail closed when an intentionally cancelled worker stays live."""
        if not isinstance(failure, BaseException):
            message = "failure must be a BaseException"
            raise TypeError(message)
        slot = self._require_slot(slot_id)
        async with self._lock:
            if (
                not slot.cancel_expected
                or slot.cancellation_sequence is None
                or slot.task is None
                or slot.task.done()
            ):
                message = "worker slot has no unsettled cancellation"
                raise RuntimeError(message)
            slot.abandoned = True
            self._mark_fatal_locked(failure)

    async def abandon_exact_cancellations(
        self,
        grant: ingestion_lease_store.LeaseGrant,
        failure: BaseException,
    ) -> int:
        """Fail closed for every live unsettled cancellation of a grant."""
        if not isinstance(grant, ingestion_lease_store.LeaseGrant):
            message = "grant must be a LeaseGrant"
            raise TypeError(message)
        if not isinstance(failure, BaseException):
            message = "failure must be a BaseException"
            raise TypeError(message)
        abandoned = 0
        async with self._lock:
            for slot in self._workers:
                record = slot.active_record
                task = slot.task
                if (
                    record is None
                    or record.grant != grant
                    or not slot.cancel_expected
                    or slot.cancellation_sequence != record.local_sequence
                    or task is None
                    or task.done()
                ):
                    continue
                slot.abandoned = True
                abandoned += 1
            if abandoned:
                self._mark_fatal_locked(failure)
        return abandoned

    async def wait_for_capacity_waiters(self, minimum: int) -> None:
        """Wait for a bounded producer-wait count in deterministic tests."""
        if isinstance(minimum, bool) or not isinstance(minimum, int):
            message = "minimum must be an integer"
            raise TypeError(message)
        if minimum < 0:
            message = "minimum must be nonnegative"
            raise ValueError(message)
        async with self._capacity_changed:
            await self._capacity_changed.wait_for(
                lambda: (
                    self._capacity_waiters >= minimum or self._fatal is not None
                )
            )
            self._raise_fatal_locked()

    async def wait_for_held(self, expected: int) -> None:
        """Wait for an exact held count without polling or wall-clock sleeps."""
        if isinstance(expected, bool) or not isinstance(expected, int):
            message = "expected must be an integer"
            raise TypeError(message)
        if expected < 0 or expected > self._limits.capacity:
            message = "expected is outside shard capacity"
            raise ValueError(message)
        async with self._capacity_changed:
            await self._capacity_changed.wait_for(
                lambda: self._held == expected or self._fatal is not None
            )
            self._raise_fatal_locked()

    async def wait_exact_empty(
        self,
        grant: ingestion_lease_store.LeaseGrant,
    ) -> None:
        """Wait until no counted record retains one complete grant."""
        if not isinstance(grant, ingestion_lease_store.LeaseGrant):
            message = "grant must be a LeaseGrant"
            raise TypeError(message)
        async with self._capacity_changed:
            await self._capacity_changed.wait_for(
                lambda: (
                    not any(
                        record.grant == grant
                        for record in (
                            *self._records.values(),
                            *self._pending_boundaries.values(),
                            *self._flushing_boundaries.values(),
                        )
                    )
                    or self._fatal is not None
                )
            )
            self._raise_fatal_locked()

    async def wait_for_fatal(self) -> None:
        """Wait until first persistent scheduler-integrity evidence exists."""
        await self._fatal_event.wait()

    async def propagate_fatal(self, failure: BaseException) -> None:
        """Fail this shard from scheduler-global integrity evidence."""
        if not isinstance(failure, BaseException):
            message = "failure must be a BaseException"
            raise TypeError(message)
        async with self._lock:
            self._mark_fatal_locked(failure, observe=False)

    async def wake_waiters(self) -> None:
        """Recheck work and admission predicates after lane state changes."""
        async with self._lock:
            self._work_ready.notify_all()
            self._capacity_changed.notify_all()

    async def close(self) -> None:
        """Stop fixed workers only after the shard is provably quiescent."""
        async with self._lock:
            if self._closed:
                return
            self._raise_fatal_locked()
            if self._held != 0:
                message = "cannot close a shard with held work"
                raise _ShardUndrainedError(message)
            self._admission_open = False
            self._stopping = True
            self._work_ready.notify_all()
            self._capacity_changed.notify_all()
            tasks = tuple(
                slot.task for slot in self._workers if slot.task is not None
            )
        if tasks:
            await asyncio.gather(*tasks)
        async with self._lock:
            self._closed = True

    def _spawn_worker_locked(self, slot: _WorkerSlot) -> None:
        old_task = slot.task
        if old_task is not None and not old_task.done():
            message = "worker replacement would create a transient extra task"
            raise RuntimeError(message)
        task = asyncio.create_task(
            self._worker_main(slot),
            name=f"feed-work-shard-{self.shard_id}-worker-{slot.slot_id}",
        )
        slot.task = task
        task.add_done_callback(
            lambda completed, worker_slot=slot: self._worker_done(
                worker_slot,
                completed,
            )
        )

    async def _worker_main(  # noqa: PLR0912
        self,
        slot: _WorkerSlot,
    ) -> None:
        """Run one fixed slot through dequeue, execute, and terminalize.

        Args:
            slot: Permanently registered worker slot owned by this task.

        Cancellation releases active work only when exact cancellation intent
        was registered first. Unexpected cancellation or execution failure is
        published as persistent shard-integrity evidence before task exit.
        """
        try:
            while True:
                record = await self._take_next(slot.slot_id)
                if record is None:
                    return
                try:
                    outcome = await self._executor.execute(record.execution())
                    expected, abandoned = await self._cancellation_state(
                        slot,
                        record,
                    )
                    if abandoned:
                        return
                    if isinstance(outcome, _types._ExecutorIntegrityFailure):
                        await self._mark_fatal(outcome.failure)
                        return
                    if not isinstance(outcome, _TERMINAL_EXECUTOR_OUTCOMES):
                        failure = _InvalidExecutorOutcome(
                            "executor returned an invalid outcome"
                        )
                        await self._mark_fatal(failure)
                        return
                    if expected:
                        failure = _ShardUndrainedError(
                            "executor swallowed registered cancellation"
                        )
                        await self._mark_fatal(failure)
                        return
                    await self._terminalize(
                        slot.slot_id,
                        record,
                        outcome,
                    )
                except asyncio.CancelledError:
                    expected, abandoned = await self._cancellation_state(
                        slot,
                        record,
                    )
                    if expected and not abandoned:
                        await self._terminalize_cancelled_if_active(
                            slot,
                            record,
                        )
                    elif not abandoned:
                        failure = _UnexpectedWorkerCancellation(
                            "worker cancelled without registered intent"
                        )
                        await self._mark_fatal(failure)
                    raise
                except BaseException as exc:
                    await self._mark_fatal(exc)
                    raise
        except asyncio.CancelledError:
            expected, _abandoned = await self._slot_cancel_state(slot)
            if not expected and not self._stopping:
                failure = _UnexpectedWorkerCancellation(
                    "idle worker cancelled without registered intent"
                )
                await self._mark_fatal(failure)
            raise
        except BaseException as exc:
            await self._mark_fatal(exc)
            raise

    async def _terminalize_cancelled_if_active(
        self,
        slot: _WorkerSlot,
        record: _types._CallRecord,
    ) -> None:
        async with self._lock:
            released = self._terminalize_locked(slot.slot_id, record)
        if released:
            await self._notify_settlements(
                (record,),
                self._settlement_for_closing(record.grant),
            )

    def _terminalize_locked(
        self,
        slot_id: int,
        record: _types._CallRecord,
    ) -> bool:
        slot = self._require_slot(slot_id)
        current = slot.active_record
        if current is not record:
            return False
        if self._active_by_feed.get(record.feed_id) is not record:
            message = "Feed and worker active ownership disagree"
            raise RuntimeError(message)
        slot.active_record = None
        del self._active_by_feed[record.feed_id]
        del self._records[record.local_sequence]
        self._held -= 1
        if self._fatal is None:
            self._ready_call_or_boundary_locked(record.feed_id)
        self._after_release_locked(1)
        self._check_conservation_locked()
        return True

    async def _replace_cancelled_worker(
        self,
        request: _CancellationRequest,
    ) -> None:
        async with self._lock:
            slot = self._require_slot(request.slot_id)
            if slot.task is not request.task or not request.task.done():
                message = "old worker has not settled"
                raise _ShardUndrainedError(message)
            if slot.active_record is not None:
                failure = _ShardUndrainedError(
                    "cancelled worker retained active ownership"
                )
                self._mark_fatal_locked(failure)
                return
            if self._fatal is not None or self._stopping:
                return
            slot.cancel_expected = False
            slot.cancellation_sequence = None
            slot.abandoned = False
            self._spawn_worker_locked(slot)

    async def _cancellation_state(
        self,
        slot: _WorkerSlot,
        record: _types._CallRecord,
    ) -> tuple[bool, bool]:
        async with self._lock:
            expected = (
                slot.cancel_expected
                and slot.cancellation_sequence == record.local_sequence
            )
            return expected, slot.abandoned

    async def _slot_cancel_state(
        self,
        slot: _WorkerSlot,
    ) -> tuple[bool, bool]:
        async with self._lock:
            return slot.cancel_expected, slot.abandoned

    async def _mark_fatal(self, failure: BaseException) -> None:
        async with self._lock:
            self._mark_fatal_locked(failure)

    def _mark_fatal_locked(
        self,
        failure: BaseException,
        *,
        observe: bool = True,
    ) -> None:
        self._admission_open = False
        first = self._fatal is None
        if self._fatal is None:
            self._fatal = failure
            self._fatal_event.set()
        if first and observe and self._fatal_observer is not None:
            self._fatal_observer(failure)
        self._worker_changed.set()
        self._work_ready.notify_all()
        self._capacity_changed.notify_all()

    def _worker_done(
        self,
        slot: _WorkerSlot,
        task: asyncio.Task[None],
    ) -> None:
        if slot.task is not task:
            return
        self._worker_changed.set()
        try:
            task.exception()
        except asyncio.CancelledError:
            pass

    def _ensure_ready_locked(self, feed_id: uuid.UUID) -> None:
        if feed_id in self._ready_members:
            return
        if feed_id in self._active_boundaries:
            return
        if feed_id not in self._feed_queues:
            message = "cannot ready a Feed without queued calls"
            raise RuntimeError(message)
        self._ready.append(feed_id)
        self._ready_members.add(feed_id)

    def _remove_ready_locked(self, feed_id: uuid.UUID) -> None:
        if feed_id not in self._ready_members:
            return
        self._ready_members.remove(feed_id)
        self._ready.remove(feed_id)

    def _coalesce_boundary_locked(
        self,
        record: _types._BoundaryRecord,
        boundary_input: _types._BoundaryInput,
    ) -> None:
        if record.state is not _types._RecordState.PENDING_BOUNDARY:
            message = "only a pending boundary may coalesce"
            raise RuntimeError(message)
        page_sequence = boundary_input.page_sequence
        if record.promotion_page_sequence not in (None, page_sequence):
            record.promotion_page_sequence = None
            record.promotion_rollback_target = None
        if record.provisional_page_sequence not in (None, page_sequence):
            message = "pending boundary retained another live page"
            raise RuntimeError(message)
        offered = boundary_input.boundary.target
        stable = record.stable_target
        if record.provisional_page_sequence is None:
            if stable is not None and offered <= stable:
                return
            record.provisional_page_sequence = page_sequence
            record.provisional_count = 1
        else:
            record.provisional_count += 1
        record.target = max(record.target, offered)

    def _restore_retryable_boundary_locked(
        self,
        record: _types._BoundaryRecord,
    ) -> None:
        """Restore or discard a retryable detached boundary under the lock.

        Args:
            record: Immutable-during-I/O record whose retryable result settled.

        Raises:
            RuntimeError: Restoration would cross page provenance or lose the
                stable rollback target.

        An aborted page rolls back before any merge. Otherwise a concurrent
        pending tail is coalesced without adding a second held permit.
        """
        page_sequence = (
            record.provisional_page_sequence
            if record.provisional_page_sequence is not None
            else record.promotion_page_sequence
        )
        aborted = (
            record.aborted_page_sequence is not None
            and record.aborted_page_sequence == page_sequence
        )
        rollback_target = (
            record.promotion_rollback_target
            if record.promotion_page_sequence == page_sequence
            else record.stable_target
        )
        if aborted and rollback_target is None:
            self._held -= 1
            self._after_release_locked(1)
            self._ready_call_or_boundary_locked(record.feed_id)
            return
        if aborted:
            if rollback_target is None:
                message = "aborted stable boundary lost its target"
                raise RuntimeError(message)
            record.target = rollback_target
            record.stable_target = rollback_target
            record.provisional_page_sequence = None
            record.provisional_count = 0
            record.promotion_page_sequence = None
            record.promotion_rollback_target = None
        record.aborted_page_sequence = None
        record.state = _types._RecordState.PENDING_BOUNDARY
        record.retry_suspended = True
        scope = (record.grant, record.feed_id)
        pending = self._pending_boundaries.pop(scope, None)
        if pending is not None:
            provisional_pages = {
                page
                for page in (
                    record.provisional_page_sequence,
                    pending.provisional_page_sequence,
                )
                if page is not None
            }
            if len(provisional_pages) > 1:
                message = "retryable merge crossed live boundary pages"
                raise RuntimeError(message)
            stable_targets = tuple(
                target
                for target in (record.stable_target, pending.stable_target)
                if target is not None
            )
            record.stable_target = (
                max(stable_targets) if stable_targets else None
            )
            record.target = max(record.target, pending.target)
            record.provisional_page_sequence = next(
                iter(provisional_pages),
                None,
            )
            record.provisional_count += pending.provisional_count
            self._held -= 1
            self._after_release_locked(1)
        self._pending_boundaries[scope] = record
        if (
            record.feed_id not in self._active_by_feed
            and record.feed_id not in self._active_boundaries
            and record.feed_id in self._feed_queues
        ):
            self._ensure_ready_locked(record.feed_id)

    def _ready_call_or_boundary_locked(self, feed_id: uuid.UUID) -> None:
        if (
            feed_id in self._active_by_feed
            or feed_id in self._active_boundaries
        ):
            return
        if feed_id in self._feed_queues:
            self._ensure_ready_locked(feed_id)
            return
        for record in self._pending_boundaries.values():
            if record.feed_id == feed_id and self._boundary_is_ready_locked(
                record
            ):
                self._notify_boundary_ready_locked(record.grant)

    def _boundary_is_ready_locked(
        self,
        record: _types._BoundaryRecord,
        *,
        include_suspended: bool = False,
    ) -> bool:
        return (
            record.state is _types._RecordState.PENDING_BOUNDARY
            and (include_suspended or not record.retry_suspended)
            and record.feed_id not in self._feed_queues
            and record.feed_id not in self._active_by_feed
            and record.feed_id not in self._active_boundaries
            and (record.grant, record.feed_id) not in self._retired_scopes
            and not self._is_grant_closing(record.grant)
        )

    def _notify_boundary_ready_locked(
        self,
        grant: ingestion_lease_store.LeaseGrant,
    ) -> None:
        if self._boundary_ready_observer is not None:
            self._boundary_ready_observer(grant)

    async def _notify_settlements(
        self,
        records: tuple[_types._CallRecord, ...],
        settlement: _types.CallSettlement,
    ) -> None:
        """Invoke every released record callback outside the shard lock."""
        if not isinstance(settlement, _types.CallSettlement):
            message = "settlement must be a CallSettlement"
            raise TypeError(message)
        failure: BaseException | None = None
        for record in records:
            observer = record.work.settlement_observer
            if observer is None:
                continue
            try:
                observer(settlement)
            except BaseException as exc:
                if failure is None:
                    failure = exc
        if failure is not None:
            await self._mark_fatal(failure)

    @staticmethod
    def _settlement_for_outcome(
        outcome: _types._ExecutorOutcome,
    ) -> _types.CallSettlement:
        if isinstance(outcome, _types._ExecutorCompleted):
            return _types.CallSettlement.COMPLETED
        if isinstance(outcome, _types._ExecutorRetryable):
            return _types.CallSettlement.RETRYABLE
        if isinstance(outcome, _types._ExecutorAuthorityLost):
            return _types.CallSettlement.AUTHORITY_LOST
        if isinstance(outcome, _types._ExecutorMembershipRejected):
            return _types.CallSettlement.MEMBERSHIP_REJECTED
        message = "executor outcome is not scheduling-terminal"
        raise TypeError(message)

    def _settlement_for_closing(
        self,
        grant: ingestion_lease_store.LeaseGrant,
    ) -> _types.CallSettlement:
        if self._closing_settlement is None:
            return _types.CallSettlement.ABORTED
        settlement = self._closing_settlement(grant)
        if not isinstance(settlement, _types.CallSettlement):
            message = "closing settlement callback returned an invalid value"
            raise TypeError(message)
        return settlement

    def _is_grant_closing(
        self,
        grant: ingestion_lease_store.LeaseGrant,
    ) -> bool:
        return self._grant_is_closing is not None and self._grant_is_closing(
            grant
        )

    def _after_release_locked(self, released_count: int) -> None:
        if released_count <= 0:
            return
        if self._pressure_paused and self._held <= self._limits.resume_at:
            self._pressure_paused = False
        self._capacity_changed.notify_all()
        self._work_ready.notify_all()

    def _raise_admission_error_locked(
        self,
        work: _types._CallWork,
        abort_event: asyncio.Event | None,
    ) -> None:
        self._raise_fatal_locked()
        if abort_event is not None and abort_event.is_set():
            message = "exact lane admission was aborted"
            raise _AdmissionAbortedError(message)
        if not self._admission_open or self._stopping or self._closed:
            message = "shard admission is closed"
            raise _ShardClosedError(message)
        if (work.grant, work.feed_id) in self._retired_scopes:
            message = "Feed is retired from this shard"
            raise _FeedRetiredError(message)

    def _raise_boundary_admission_error_locked(
        self,
        boundary_input: _types._BoundaryInput,
        abort_event: asyncio.Event,
    ) -> None:
        self._raise_fatal_locked()
        if abort_event.is_set():
            message = "exact lane admission was aborted"
            raise _AdmissionAbortedError(message)
        if not self._admission_open or self._stopping or self._closed:
            message = "shard admission is closed"
            raise _ShardClosedError(message)
        scope = (boundary_input.grant, boundary_input.boundary.feed_id)
        if scope in self._retired_scopes:
            message = "Feed is retired from this shard"
            raise _FeedRetiredError(message)

    def _raise_fatal_locked(self) -> None:
        failure = self._fatal or self._global_fatal_failure()
        if failure is not None:
            raise _ShardFatalError(failure) from failure

    def _global_fatal_failure(self) -> BaseException | None:
        if self._global_fatal is None:
            return None
        return self._global_fatal()

    def _abandonment_failure(
        self,
        grant: ingestion_lease_store.LeaseGrant,
    ) -> BaseException | None:
        if self._abandonment_for is None:
            return None
        return self._abandonment_for(grant)

    def _require_slot(self, slot_id: int) -> _WorkerSlot:
        if isinstance(slot_id, bool) or not isinstance(slot_id, int):
            message = "slot_id must be an integer"
            raise TypeError(message)
        if slot_id < 0 or slot_id >= len(self._workers):
            message = "slot_id is outside the fixed worker registry"
            raise ValueError(message)
        return self._workers[slot_id]

    def _check_conservation_locked(self) -> None:
        """Validate counted capacity and exclusive ownership under the lock.

        Raises:
            RuntimeError: Held capacity, ready membership, Feed ownership,
                record state, or fixed-worker ownership is inconsistent.

        Callers treat any failure as scheduler-integrity evidence; this method
        never repairs uncertain state or releases capacity speculatively.
        """
        queued = sum(len(queue) for queue in self._feed_queues.values())
        active = len(self._active_by_feed)
        conserved = (
            queued
            + active
            + len(self._pending_boundaries)
            + len(self._flushing_boundaries)
        )
        if self._held != conserved:
            message = "held conservation equation failed"
            raise RuntimeError(message)
        if self._held < 0 or self._held > self._limits.capacity:
            message = "held is outside the hard shard capacity"
            raise RuntimeError(message)
        if len(self._ready) != len(self._ready_members):
            message = "ready ring contains duplicate membership"
            raise RuntimeError(message)
        if set(self._ready) != self._ready_members:
            message = "ready ring and membership set disagree"
            raise RuntimeError(message)
        if set(self._active_by_feed) & self._ready_members:
            message = "active Feed also appears in the ready ring"
            raise RuntimeError(message)
        if set(self._active_boundaries) & self._ready_members:
            message = "boundary-owned Feed also appears in the ready ring"
            raise RuntimeError(message)
        if set(self._active_by_feed) & set(self._active_boundaries):
            message = "Feed has both call and boundary ownership"
            raise RuntimeError(message)
        if set(self._active_boundaries) != {
            record.feed_id for record in self._flushing_boundaries.values()
        }:
            message = "boundary Feed ownership disagrees with flushing state"
            raise RuntimeError(message)
        if any(
            record.state is not _types._RecordState.PENDING_BOUNDARY
            for record in self._pending_boundaries.values()
        ):
            message = "pending boundary map contains a detached record"
            raise RuntimeError(message)
        if any(
            record.state is not _types._RecordState.FLUSHING_BOUNDARY
            for record in self._flushing_boundaries.values()
        ):
            message = "flushing boundary map contains a pending record"
            raise RuntimeError(message)
        active_sequences = {
            record.local_sequence for record in self._active_by_feed.values()
        }
        slot_sequences = {
            slot.active_record.local_sequence
            for slot in self._workers
            if slot.active_record is not None
        }
        if active_sequences != slot_sequences:
            message = "Feed and fixed-worker ownership disagree"
            raise RuntimeError(message)
