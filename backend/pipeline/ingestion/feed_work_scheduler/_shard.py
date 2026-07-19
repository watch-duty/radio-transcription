"""Authoritative bounded state for one Feed-affine scheduler shard."""

# Private sibling modules intentionally share the scheduler's closed internals.
# ruff: noqa: SLF001

from __future__ import annotations

import asyncio
import collections
import dataclasses
import typing

from backend.pipeline.ingestion.feed_work_scheduler import _types

if typing.TYPE_CHECKING:
    import uuid

    from backend.pipeline.storage import ingestion_lease_store


class _ShardClosedError(RuntimeError):
    """Admission reached a shard that no longer accepts work."""


class _ShardFatalError(RuntimeError):
    """Admission or coordination observed persistent integrity failure."""

    def __init__(self, failure: BaseException) -> None:
        super().__init__("shard integrity failed")
        self.failure = failure


class _ShardUndrainedError(RuntimeError):
    """A close or replacement was attempted while work remained owned."""


class _UnexpectedWorkerCancellation(RuntimeError):
    """A worker was cancelled without registered cancellation intent."""


def _raise_cancelled() -> typing.Never:
    raise asyncio.CancelledError


@dataclasses.dataclass(slots=True)
class _WorkerSlot:
    slot_id: int
    task: asyncio.Task[None] | None = None
    active_record: _types._CallRecord | None = None
    cancellation_sequence: int | None = None
    abandoned: bool = False


@dataclasses.dataclass(frozen=True, slots=True)
class _CancellationRequest:
    """Exact registered worker cancellation awaiting settlement."""

    slot_id: int
    task: asyncio.Task[None]


class _Shard:
    """One lock-protected held-token state machine with fixed workers."""

    def __init__(
        self,
        shard_id: int,
        executor: _types.CallExecutor,
        *,
        limits: _types._SchedulerLimits = _types._PRODUCTION_LIMITS,
        fatal_observer: typing.Callable[[BaseException], None] | None = None,
    ) -> None:
        if isinstance(shard_id, bool):
            message = "shard_id must be an integer"
            raise TypeError(message)
        if shard_id < 0 or shard_id >= limits.shard_count:
            message = "shard_id is outside configured shard_count"
            raise ValueError(message)

        self.shard_id = shard_id
        self._executor = executor
        self._limits = limits
        self._fatal_observer = fatal_observer
        self._lock = asyncio.Lock()
        self._work_ready = asyncio.Condition(self._lock)
        self._capacity_changed = asyncio.Condition(self._lock)

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

        self._workers = [
            _WorkerSlot(slot_id)
            for slot_id in range(self._limits.workers_per_shard)
        ]
        self._started = False
        self._stopping = False
        self._closed = False
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

    async def admit(self, work: _types._CallWork) -> _types._CallRecord:
        """Atomically register one capacity-owning source record."""
        async with self._capacity_changed:
            while True:
                self._raise_admission_error_locked()
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

                await self._capacity_changed.wait()

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
    ) -> None:
        """Release one settled active record."""
        async with self._lock:
            released = self._terminalize_locked(slot_id, record)
            if not released:
                message = "active record no longer belongs to worker slot"
                raise RuntimeError(message)

    async def purge_exact(
        self,
        grant: ingestion_lease_store.LeaseGrant,
    ) -> None:
        """Release queued records matching the complete grant only."""
        async with self._lock:
            released_count = 0
            for feed_id, queue in tuple(self._feed_queues.items()):
                kept: collections.deque[_types._CallRecord] = (
                    collections.deque()
                )
                for record in queue:
                    if record.grant == grant:
                        del self._records[record.local_sequence]
                        self._held -= 1
                        released_count += 1
                    else:
                        kept.append(record)
                if kept:
                    self._feed_queues[feed_id] = kept
                else:
                    del self._feed_queues[feed_id]
                    self._remove_ready_locked(feed_id)
            self._after_release_locked(released_count)
            self._check_conservation_locked()

    async def request_cancel_exact(
        self,
        grant: ingestion_lease_store.LeaseGrant,
    ) -> tuple[_CancellationRequest, ...]:
        """Mark exact active ownership before cancelling fixed workers."""
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
                    or slot.cancellation_sequence is not None
                ):
                    continue
                slot.cancellation_sequence = record.local_sequence
                requests.append(
                    _CancellationRequest(
                        slot_id=slot.slot_id,
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
        """Settle exact cancellations or fail closed when interrupted."""
        requests = await self.request_cancel_exact(grant)
        try:
            for request in requests:
                await asyncio.wait((request.task,))
                if not request.task.cancelled():
                    failure = request.task.exception()
                    if failure is not None:
                        await self._mark_fatal(failure)
                await self._replace_cancelled_worker(request)
        except asyncio.CancelledError as cancellation:
            failure = _ShardUndrainedError(
                "cancellation settlement was interrupted"
            )
            try:
                await self._abandon_interrupted_cancellations(
                    requests,
                    failure,
                )
            except Exception as cleanup_failure:
                raise cancellation from cleanup_failure
            raise
        return tuple(request.slot_id for request in requests)

    async def _abandon_interrupted_cancellations(
        self,
        requests: tuple[_CancellationRequest, ...],
        failure: BaseException,
    ) -> None:
        """Abandon workers whose cancelled caller cannot settle them."""
        async with self._lock:
            for request in requests:
                slot = self._require_slot(request.slot_id)
                if (
                    slot.task is request.task
                    and not request.task.done()
                    and slot.cancellation_sequence is not None
                ):
                    slot.abandoned = True
            self._mark_fatal_locked(failure)

    async def abandon_cancellation(
        self,
        slot_id: int,
        failure: BaseException,
    ) -> None:
        """Fail closed when an intentionally cancelled worker stays live."""
        slot = self._require_slot(slot_id)
        async with self._lock:
            if (
                slot.cancellation_sequence is None
                or slot.task is None
                or slot.task.done()
            ):
                message = "worker slot has no unsettled cancellation"
                raise RuntimeError(message)
            slot.abandoned = True
            self._mark_fatal_locked(failure)

    async def wait_for_held(self, expected: int) -> None:
        """Wait for the shard to drain without polling or wall-clock sleeps."""
        if isinstance(expected, bool):
            message = "expected must be an integer"
            raise TypeError(message)
        if expected < 0 or expected > self._limits.capacity:
            message = "expected is outside shard capacity"
            raise ValueError(message)
        if expected != 0:
            message = "wait_for_held only supports draining to zero"
            raise ValueError(message)
        async with self._capacity_changed:
            await self._capacity_changed.wait_for(
                lambda: self._held == expected or self._fatal is not None
            )
            self._raise_fatal_locked()

    async def close(self) -> None:
        """Stop fixed workers only after the shard is provably quiescent."""
        async with self._lock:
            if self._closed:
                return
            self._raise_fatal_locked()
            if self._held != 0:
                message = "cannot close a shard with held work"
                raise _ShardUndrainedError(message)
            self._stopping = True
            self._work_ready.notify_all()
            self._capacity_changed.notify_all()
            tasks = tuple(
                slot.task for slot in self._workers if slot.task is not None
            )
        if tasks:
            await asyncio.wait(tasks)
            for task in tasks:
                if not task.cancelled():
                    task.result()
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

    async def _worker_main(
        self,
        slot: _WorkerSlot,
    ) -> None:
        try:
            while True:
                record = await self._take_next(slot.slot_id)
                if record is None:
                    return
                try:
                    await self._executor.execute(record)
                    expected, abandoned = await self._cancellation_state(
                        slot,
                        record,
                    )
                    if abandoned:
                        return
                    await self._terminalize(
                        slot.slot_id,
                        record,
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
        if record.feed_id in self._feed_queues and self._fatal is None:
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
            slot.cancellation_sequence = None
            slot.abandoned = False
            self._spawn_worker_locked(slot)

    async def _cancellation_state(
        self,
        slot: _WorkerSlot,
        record: _types._CallRecord,
    ) -> tuple[bool, bool]:
        async with self._lock:
            expected = slot.cancellation_sequence == record.local_sequence
            return expected, slot.abandoned

    async def _slot_cancel_state(
        self,
        slot: _WorkerSlot,
    ) -> tuple[bool, bool]:
        async with self._lock:
            return slot.cancellation_sequence is not None, slot.abandoned

    async def _mark_fatal(self, failure: BaseException) -> None:
        async with self._lock:
            self._mark_fatal_locked(failure)

    def _mark_fatal_locked(self, failure: BaseException) -> None:
        first_failure = self._fatal is None
        if first_failure:
            self._fatal = failure
        self._work_ready.notify_all()
        self._capacity_changed.notify_all()
        if first_failure and self._fatal_observer is not None:
            self._fatal_observer(failure)

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

    def _raise_admission_error_locked(self) -> None:
        self._raise_fatal_locked()
        if self._stopping or self._closed:
            message = "shard admission is closed"
            raise _ShardClosedError(message)

    def _raise_fatal_locked(self) -> None:
        if self._fatal is not None:
            raise _ShardFatalError(self._fatal) from self._fatal

    def _require_slot(self, slot_id: int) -> _WorkerSlot:
        if isinstance(slot_id, bool):
            message = "slot_id must be an integer"
            raise TypeError(message)
        if slot_id < 0 or slot_id >= len(self._workers):
            message = "slot_id is outside the fixed worker registry"
            raise ValueError(message)
        return self._workers[slot_id]

    def _check_conservation_locked(self) -> None:
        queued = sum(len(queue) for queue in self._feed_queues.values())
        active = len(self._active_by_feed)
        conserved = queued + active
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
        if set(self._records) != (
            {
                record.local_sequence
                for queue in self._feed_queues.values()
                for record in queue
            }
            | active_sequences
        ):
            message = "record registry and ownership disagree"
            raise RuntimeError(message)
