"""Process scheduler and exact-grant streaming page facade."""

# Private sibling modules deliberately compose the scheduler's closed core.
# ruff: noqa: SLF001

from __future__ import annotations

import asyncio
import dataclasses
import typing

from backend.pipeline.ingestion.collectors.bcfy_calls import cursor_policy
from backend.pipeline.ingestion.feed_work_scheduler import _shard, _types
from backend.pipeline.storage import ingestion_lease_store

if typing.TYPE_CHECKING:
    import collections.abc
    import uuid


class _SchedulerIntegrityError(RuntimeError):
    """The process scheduler can no longer prove safe admission."""


class _LaneClosedError(RuntimeError):
    """An exact lane closed before page coverage linearized."""


@dataclasses.dataclass(slots=True)
class _PageBarrier:
    """O(1) state for the one page currently entering a lane."""

    grant: ingestion_lease_store.LeaseGrant
    page_sequence: int
    current_source_order: int | None = None
    pulled: int = 0
    registered: int = 0
    localized: int = 0


@dataclasses.dataclass(frozen=True, slots=True)
class _PageSnapshot:
    """Payload-free projection of one transient page barrier."""

    grant: ingestion_lease_store.LeaseGrant
    page_sequence: int
    current_source_order: int | None
    pulled: int
    registered: int
    localized: int


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
    started: bool
    closing: bool
    closed: bool
    fatal: bool


class FeedWorkScheduler:
    """One process-wide owner of fixed Feed-affine scheduler shards."""

    def __init__(
        self,
        executor: _types.CallExecutor,
        *,
        _limits: _types._SchedulerLimits = _types._PRODUCTION_LIMITS,
    ) -> None:
        """Create one scheduler with immutable production or test limits.

        Args:
            executor: Private full-pipeline adapter for fixed workers.
            _limits: Validated deterministic-test limits. Production callers
                use the fixed default.
        """
        if not isinstance(_limits, _types._SchedulerLimits):
            message = "_limits must be _SchedulerLimits"
            raise TypeError(message)
        self._limits = _limits
        self._shards = tuple(
            _shard._Shard(index, executor, limits=_limits)
            for index in range(_limits.shard_count)
        )
        self._lifecycle_lock = asyncio.Lock()
        self._lanes: dict[
            ingestion_lease_store.LeaseGrant,
            GrantLane,
        ] = {}
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

    def open_lane(
        self,
        grant: ingestion_lease_store.LeaseGrant,
    ) -> GrantLane:
        """Bind one fresh live lane to one complete immutable Lease grant.

        Args:
            grant: Complete exact Lease ownership generation.

        Returns:
            Fresh lane scoped to ``grant``.

        Raises:
            TypeError: ``grant`` is not a complete ``LeaseGrant``.
            RuntimeError: The scheduler is not open for new lanes.
            ValueError: The exact grant already owns a live lane.
        """
        if not isinstance(grant, ingestion_lease_store.LeaseGrant):
            message = "grant must be a LeaseGrant"
            raise TypeError(message)
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
        lane = GrantLane(self, grant)
        self._lanes[grant] = lane
        return lane

    async def close(self) -> None:
        """Close all lanes, then stop only quiescent fixed workers."""
        async with self._lifecycle_lock:
            if self._closed:
                return
            self._closing = True
            lanes = tuple(self._lanes.values())
            for lane in lanes:
                lane._publish_close()

        for lane in lanes:
            await lane._purge_for_scheduler_close()
        await self._wait_for_idle()
        self._raise_fatal()
        for shard in self._shards:
            await shard.close()
        for lane in lanes:
            await lane._finish_closed()
        async with self._lifecycle_lock:
            self._lanes.clear()
            self._closed = True

    async def _snapshot(self) -> _SchedulerSnapshot:
        shards = []
        for shard in self._shards:
            shards.append(await shard.snapshot())
        snapshots = tuple(shards)
        registered = sum(
            worker.task_registered
            for snapshot in snapshots
            for worker in snapshot.workers
        )
        return _SchedulerSnapshot(
            shards=snapshots,
            held=sum(snapshot.held for snapshot in snapshots),
            lane_count=len(self._lanes),
            registered_worker_tasks=registered,
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
    ) -> None:
        for shard in self._shards:
            await shard.purge_page(grant, page_sequence)

    async def _purge_exact(
        self,
        grant: ingestion_lease_store.LeaseGrant,
    ) -> None:
        for shard in self._shards:
            await shard.purge_exact(grant)

    async def _wake_admission(self) -> None:
        for shard in self._shards:
            await shard.wake_waiters()

    def _shard_for(self, feed_id: uuid.UUID) -> _shard._Shard:
        index = _types._shard_index(feed_id, self._limits)
        return self._shards[index]

    def _raise_fatal(self) -> None:
        for shard in self._shards:
            failure = shard.fatal_failure
            if failure is not None:
                message = "scheduler shard integrity failed"
                raise _SchedulerIntegrityError(message) from failure


class GrantLane:
    """One exact-grant lane hiding admission, shards, and cleanup."""

    def __init__(
        self,
        scheduler: FeedWorkScheduler,
        grant: ingestion_lease_store.LeaseGrant,
    ) -> None:
        self._scheduler = scheduler
        self._grant = grant
        self._admission_lock = asyncio.Lock()
        self._state_lock = asyncio.Lock()
        self._closing_event = asyncio.Event()
        self._next_page_sequence = 0
        self._page: _PageBarrier | None = None
        self._closing = False
        self._closed = False

    @property
    def grant(self) -> ingestion_lease_store.LeaseGrant:
        """Return the complete immutable grant bound to this lane."""
        return self._grant

    async def cover_page(
        self,
        *,
        calls: collections.abc.Iterable[_types.CallSubmission],
        boundaries: collections.abc.Iterable[object],
        candidate: cursor_policy.PageCursorCandidate,
    ) -> cursor_policy._CoveredPage:
        """Incrementally cover one source-order call page.

        Args:
            calls: Single-pass call submissions in provider source order.
            boundaries: Trailing boundary inputs. Plan 04-04 installs them;
                any non-empty input currently fails before call admission.
            candidate: Exact cursor candidate awaiting bounded coverage.

        Returns:
            Private sealed receipt consumable only by ``LeaseCursor``.

        Raises:
            CursorIntegrityError: Candidate authority or sequence is wrong.
            NotImplementedError: A boundary is offered before Plan 04-04.
            RuntimeError: The lane or scheduler is closing or failed.
        """
        async with self._admission_lock:
            await self._validate_candidate(candidate)
            self._require_empty_boundaries(boundaries)
            await self._begin_page(candidate.page_sequence)
            try:
                for source_order, submission in enumerate(calls):
                    self._require_submission(submission)
                    await self._mark_pulled(source_order)
                    work = _types._CallWork(
                        feed_id=submission.feed_id,
                        grant=self._grant,
                        source_order=source_order,
                        source_timestamp=submission.source_timestamp,
                        payload=submission.payload,
                        page_sequence=candidate.page_sequence,
                    )
                    shard = self._scheduler._shard_for(submission.feed_id)
                    try:
                        await shard.admit(
                            work,
                            abort_event=self._closing_event,
                        )
                    except _shard._AdmissionAbortedError as exc:
                        message = "lane closed during page admission"
                        raise _LaneClosedError(message) from exc
                    await self._mark_registered(source_order)
                return await self._cover(candidate)
            except BaseException:
                await self._abort_page(candidate.page_sequence)
                raise

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
        candidate: cursor_policy.PageCursorCandidate,
    ) -> None:
        async with self._state_lock:
            self._raise_closed_locked()
            self._validate_candidate_locked(candidate)

    async def _begin_page(self, page_sequence: int) -> None:
        async with self._state_lock:
            self._raise_closed_locked()
            if self._page is not None:
                message = "lane already owns a live page"
                raise RuntimeError(message)
            self._page = _PageBarrier(
                grant=self._grant,
                page_sequence=page_sequence,
            )

    async def _mark_pulled(self, source_order: int) -> None:
        async with self._state_lock:
            self._raise_closed_locked()
            page = self._require_page_locked()
            if page.current_source_order is not None:
                message = "cannot pull past the current source record"
                raise RuntimeError(message)
            page.current_source_order = source_order
            page.pulled += 1

    async def _mark_registered(self, source_order: int) -> None:
        async with self._state_lock:
            page = self._require_page_locked()
            if page.current_source_order != source_order:
                message = "registered source record does not match barrier"
                raise RuntimeError(message)
            page.current_source_order = None
            page.registered += 1

    async def _cover(
        self,
        candidate: cursor_policy.PageCursorCandidate,
    ) -> cursor_policy._CoveredPage:
        async with self._state_lock:
            self._raise_closed_locked()
            self._validate_candidate_locked(candidate)
            page = self._require_page_locked()
            if page.current_source_order is not None:
                message = "cannot cover a partially admitted source record"
                raise RuntimeError(message)
            if page.pulled != page.registered + page.localized:
                message = "not every pulled record owns bounded coverage"
                raise RuntimeError(message)
            receipt = cursor_policy._issue_covered_page(candidate)
            self._next_page_sequence += 1
            self._page = None
            return receipt

    async def _abort_page(self, page_sequence: int) -> None:
        await self._scheduler._purge_page(self._grant, page_sequence)
        async with self._state_lock:
            if (
                self._page is not None
                and self._page.page_sequence == page_sequence
            ):
                self._page = None

    def _publish_close(self) -> None:
        self._closing = True
        self._closing_event.set()

    async def _purge_for_scheduler_close(self) -> None:
        await self._scheduler._wake_admission()
        await self._scheduler._purge_exact(self._grant)

    async def _finish_closed(self) -> None:
        async with self._state_lock:
            self._closing = True
            self._closed = True
            self._page = None

    def _validate_candidate_locked(
        self,
        candidate: cursor_policy.PageCursorCandidate,
    ) -> None:
        if type(candidate) is not cursor_policy.PageCursorCandidate:
            message = "candidate must be an exact PageCursorCandidate"
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

    @staticmethod
    def _require_submission(submission: object) -> None:
        if not isinstance(submission, _types.CallSubmission):
            message = "calls must contain CallSubmission values"
            raise TypeError(message)

    @staticmethod
    def _require_empty_boundaries(
        boundaries: collections.abc.Iterable[object],
    ) -> None:
        iterator = iter(boundaries)
        try:
            next(iterator)
        except StopIteration:
            return
        message = "boundary scheduling is installed in Plan 04-04"
        raise NotImplementedError(message)
