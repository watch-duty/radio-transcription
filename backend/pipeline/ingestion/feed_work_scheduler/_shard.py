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


def _raise_cancelled() -> typing.Never:
    raise asyncio.CancelledError


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

    def __init__(
        self,
        shard_id: int,
        executor: _types.CallExecutor,
        *,
        limits: _types._SchedulerLimits = _types._PRODUCTION_LIMITS,
    ) -> None:
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

        self.shard_id = shard_id
        self._executor = executor
        self._limits = limits
        self._lock = asyncio.Lock()
        self._work_ready = asyncio.Condition(self._lock)
        self._capacity_changed = asyncio.Condition(self._lock)
        self._fatal_event = asyncio.Event()

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
        self._retired_feeds: set[uuid.UUID] = set()
        self._pending_boundaries = 0
        self._flushing_boundaries = 0
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
                    if record.feed_id not in self._active_by_feed:
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

    async def snapshot(self) -> _types._ShardSnapshot:
        """Return a payload-free bounded state projection."""
        async with self._lock:
            self._check_conservation_locked()
            queued_sequences = {
                record.local_sequence
                for queue in self._feed_queues.values()
                for record in queue
            }
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
            return _types._ShardSnapshot(
                held=self._held,
                queued_calls=len(queued_sequences),
                active_calls=len(self._active_by_feed),
                pending_boundaries=self._pending_boundaries,
                flushing_boundaries=self._flushing_boundaries,
                pressure_paused=self._pressure_paused,
                ready_feeds=tuple(self._ready),
                ready_members=frozenset(self._ready_members),
                active_feeds=frozenset(self._active_by_feed),
                records=records,
                workers=workers,
                retired_feeds=frozenset(self._retired_feeds),
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
        async with self._work_ready:
            while True:
                if self._fatal is not None or self._stopping:
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
                    self._active_by_feed[feed_id] = record
                    slot.active_record = record
                    self._check_conservation_locked()
                    return record
                if not wait:
                    return None
                await self._work_ready.wait()

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
        async with self._lock:
            released = self._terminalize_locked(slot_id, record)
            if not released:
                message = "active record no longer belongs to worker slot"
                raise RuntimeError(message)

    async def purge_exact(
        self,
        grant: ingestion_lease_store.LeaseGrant,
    ) -> _types._PurgeResult:
        """Release queued records matching the complete grant only."""
        if not isinstance(grant, ingestion_lease_store.LeaseGrant):
            message = "grant must be a LeaseGrant"
            raise TypeError(message)
        async with self._lock:
            released: list[int] = []
            for feed_id, queue in tuple(self._feed_queues.items()):
                kept: collections.deque[_types._CallRecord] = (
                    collections.deque()
                )
                for record in queue:
                    if record.grant == grant:
                        released.append(record.local_sequence)
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
                )
            )
            self._after_release_locked(len(released))
            self._check_conservation_locked()
            return _types._PurgeResult(
                released_sequences=tuple(sorted(released)),
                active_sequences=active,
            )

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
            self._after_release_locked(len(released))
            self._check_conservation_locked()
            return _types._PurgeResult(
                released_sequences=tuple(sorted(released)),
                active_sequences=active,
            )

    async def retire_feed(self, feed_id: uuid.UUID) -> _types._RetireFeedResult:
        """Reject one Feed and safely purge only its queued calls."""
        if not isinstance(feed_id, uuid.UUID):
            message = "feed_id must be a UUID"
            raise TypeError(message)
        async with self._lock:
            self._retired_feeds.add(feed_id)
            queue = self._feed_queues.pop(feed_id, collections.deque())
            released = tuple(record.local_sequence for record in queue)
            for record in queue:
                del self._records[record.local_sequence]
                self._held -= 1
            self._remove_ready_locked(feed_id)
            active = self._active_by_feed.get(feed_id)
            self._after_release_locked(len(released))
            self._check_conservation_locked()
            return _types._RetireFeedResult(
                released_sequences=released,
                active_sequence=(
                    active.local_sequence if active is not None else None
                ),
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
                requests.append(
                    _CancellationRequest(
                        slot_id=slot.slot_id,
                        local_sequence=record.local_sequence,
                        task=task,
                    )
                )
        for request in requests:
            request.task.cancel()
        return tuple(requests)

    async def cancel_active_exact(
        self,
        grant: ingestion_lease_store.LeaseGrant,
    ) -> tuple[int, ...]:
        """Settle expected exact cancellations before reusing worker slots."""
        requests = await self.request_cancel_exact(grant)
        for request in requests:
            try:
                await request.task
            except asyncio.CancelledError:
                pass
            except BaseException as exc:
                await self._mark_fatal(exc)
            await self._replace_cancelled_worker(request)
        return tuple(request.slot_id for request in requests)

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

    async def wait_for_fatal(self) -> None:
        """Wait until first persistent scheduler-integrity evidence exists."""
        await self._fatal_event.wait()

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
        try:
            while True:
                record = await self._take_next(slot.slot_id)
                if record is None:
                    return
                try:
                    outcome = await self._executor.execute(record)
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
                    await self._terminalize(
                        slot.slot_id,
                        record,
                        outcome,
                    )
                    if expected:
                        _raise_cancelled()
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
            self._terminalize_locked(slot.slot_id, record)

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
        if (
            record.feed_id in self._feed_queues
            and record.feed_id not in self._retired_feeds
            and self._fatal is None
        ):
            self._ensure_ready_locked(record.feed_id)
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

    def _mark_fatal_locked(self, failure: BaseException) -> None:
        self._admission_open = False
        if self._fatal is None:
            self._fatal = failure
            self._fatal_event.set()
        self._work_ready.notify_all()
        self._capacity_changed.notify_all()

    def _worker_done(
        self,
        slot: _WorkerSlot,
        task: asyncio.Task[None],
    ) -> None:
        del self
        if slot.task is not task:
            return
        try:
            task.exception()
        except asyncio.CancelledError:
            pass

    def _ensure_ready_locked(self, feed_id: uuid.UUID) -> None:
        if feed_id in self._ready_members:
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
        if work.feed_id in self._retired_feeds:
            message = "Feed is retired from this shard"
            raise _FeedRetiredError(message)

    def _raise_fatal_locked(self) -> None:
        if self._fatal is not None:
            raise _ShardFatalError(self._fatal) from self._fatal

    def _require_slot(self, slot_id: int) -> _WorkerSlot:
        if isinstance(slot_id, bool) or not isinstance(slot_id, int):
            message = "slot_id must be an integer"
            raise TypeError(message)
        if slot_id < 0 or slot_id >= len(self._workers):
            message = "slot_id is outside the fixed worker registry"
            raise ValueError(message)
        return self._workers[slot_id]

    def _check_conservation_locked(self) -> None:
        queued = sum(len(queue) for queue in self._feed_queues.values())
        active = len(self._active_by_feed)
        conserved = (
            queued
            + active
            + self._pending_boundaries
            + self._flushing_boundaries
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
