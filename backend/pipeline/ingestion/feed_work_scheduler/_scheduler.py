"""Process scheduler and exact-grant page-admission lanes."""

# Private sibling modules deliberately compose the scheduler's closed core.
# ruff: noqa: SLF001

from __future__ import annotations

import asyncio
import enum
import typing

from backend.pipeline.ingestion.collectors.bcfy_calls import cursor_policy
from backend.pipeline.ingestion.feed_work_scheduler import _shard, _types

if typing.TYPE_CHECKING:
    import collections.abc
    import uuid

    from backend.pipeline.storage import feed_store, ingestion_lease_store


class SchedulerIntegrityError(RuntimeError):
    """The process scheduler can no longer prove safe admission."""


class _LaneClosedError(RuntimeError):
    """An exact lane closed before page coverage linearized."""


class _CloseStrength(enum.IntEnum):
    """Monotonic exact-lane lifecycle strength."""

    OPEN = 0
    DRAINING = 1
    CANCELLING = 2


class FeedWorkScheduler:
    """One process-wide owner of fixed Feed-affine scheduler shards."""

    def __init__(
        self,
        executor: _types.CallExecutor,
        *,
        _limits: _types._SchedulerLimits = _types._PRODUCTION_LIMITS,
    ) -> None:
        self._limits = _limits
        self._lanes: dict[
            ingestion_lease_store.LeaseGrant,
            GrantLane,
        ] = {}
        self._highest_fence: dict[
            tuple[feed_store.SourceType, str],
            int,
        ] = {}
        self._closing_grants: set[ingestion_lease_store.LeaseGrant] = set()
        self._fatal: BaseException | None = None
        self._fatal_event = asyncio.Event()
        self._fatal_propagation_task: asyncio.Task[None] | None = None
        self._shards = tuple(
            _shard._Shard(
                index,
                executor,
                limits=_limits,
                fatal_observer=self._observe_fatal,
                grant_is_closing=self._closing_grants.__contains__,
            )
            for index in range(_limits.shard_count)
        )
        self._close_task: asyncio.Task[_types.Undrained | None] | None = None
        self._started = False
        self._closing = False
        self._closed = False

    @property
    def integrity_failure_event(self) -> asyncio.Event:
        """Return the process-wide persistent integrity-failure signal."""
        return self._fatal_event

    async def start(self) -> None:
        """Start every shard's fixed workers exactly once."""
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
        """Open one lane for a strictly newer Lease generation."""
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

        highest = self._highest_fence.get(grant.unit_key)
        if highest is not None and grant.fencing_token <= highest:
            message = "grant is not newer than the lane slot history"
            raise ValueError(message)

        for existing_grant, lane in tuple(self._lanes.items()):
            if existing_grant.unit_key == grant.unit_key:
                lane._request_close(_types.LaneCloseReason.AUTHORITY_LOSS)

        lane = GrantLane(self, grant)
        self._lanes[grant] = lane
        self._highest_fence[grant.unit_key] = grant.fencing_token
        return lane

    async def close(self) -> _types.Undrained | None:
        """Close all exact lanes and then their settled fixed workers."""
        return await asyncio.shield(self._request_close())

    def _request_close(self) -> asyncio.Task[_types.Undrained | None]:
        self._closing = True
        if self._close_task is None:
            lane_tasks = tuple(
                lane._request_close(_types.LaneCloseReason.SCHEDULER_SHUTDOWN)
                for lane in self._lanes.values()
            )
            self._close_task = asyncio.create_task(
                self._coordinate_close(lane_tasks),
                name="feed-work-scheduler-close",
            )
        return self._close_task

    async def _coordinate_close(
        self,
        lane_tasks: tuple[
            asyncio.Task[_types.LaneClosed | _types.Undrained],
            ...,
        ],
    ) -> _types.Undrained | None:
        results = await asyncio.gather(
            *(asyncio.shield(task) for task in lane_tasks)
        )
        if any(isinstance(result, _types.Undrained) for result in results):
            return _types.Undrained(
                None,
                _types.LaneCloseReason.SCHEDULER_SHUTDOWN,
            )
        try:
            self._raise_fatal()
            for shard in self._shards:
                await shard.wait_for_held(0)
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

    async def _purge_feed(
        self,
        grant: ingestion_lease_store.LeaseGrant,
        feed_id: uuid.UUID,
    ) -> None:
        await self._shard_for(feed_id).purge_feed(grant, feed_id)

    async def _wait_exact_empty(
        self,
        grant: ingestion_lease_store.LeaseGrant,
    ) -> None:
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

    async def _wake_admission(self) -> None:
        for shard in self._shards:
            await shard.wake_waiters()

    def _shard_for(self, feed_id: uuid.UUID) -> _shard._Shard:
        index = _types._shard_index(feed_id, self._limits)
        return self._shards[index]

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

    def _lane_closed(self, lane: GrantLane) -> None:
        if self._lanes.get(lane.grant) is lane:
            del self._lanes[lane.grant]
        self._closing_grants.discard(lane.grant)


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
        self._closing_event = asyncio.Event()
        self._close_changed = asyncio.Event()
        self._next_page_sequence = 0
        self._close_strength = _CloseStrength.OPEN
        self._close_reason = _types.LaneCloseReason.PLANNED_DRAIN
        self._close_task: (
            asyncio.Task[_types.LaneClosed | _types.Undrained] | None
        ) = None
        self._closed = False

    @property
    def grant(self) -> ingestion_lease_store.LeaseGrant:
        """Return the complete immutable grant bound to this lane."""
        return self._grant

    async def cover_page(
        self,
        calls: collections.abc.Iterable[_types.CallSubmission],
        candidate: cursor_policy.PageCursorCandidate,
    ) -> cursor_policy._CoveredPage:
        """Admit every call before issuing one exact cursor receipt."""
        async with self._admission_lock:
            self._require_open()
            self._validate_candidate(candidate)
            admitted = False
            try:
                for submission in calls:
                    self._require_open()
                    work = _types._CallWork(
                        feed_id=submission.feed_id,
                        grant=self._grant,
                        cohort_timestamp=submission.source_timestamp,
                        payload=submission.payload,
                        page_sequence=candidate.page_sequence,
                    )
                    try:
                        await self._scheduler._shard_for(
                            submission.feed_id
                        ).admit(
                            work,
                            abort_event=self._closing_event,
                        )
                    except _shard._AdmissionAbortedError as exc:
                        message = "lane closed during page admission"
                        raise _LaneClosedError(message) from exc
                    except _shard._ShardFatalError as exc:
                        message = "scheduler shard integrity failed"
                        raise SchedulerIntegrityError(message) from exc.failure
                    admitted = True
                self._require_open()
            except BaseException:
                await self._scheduler._purge_page(
                    self._grant,
                    candidate.page_sequence,
                )
                if admitted and not self._closing_event.is_set():
                    self._request_close(_types.LaneCloseReason.AUTHORITY_LOSS)
                raise
            receipt = cursor_policy._issue_covered_page(candidate)
            self._next_page_sequence += 1
            return receipt

    async def purge_feed(self, feed_id: uuid.UUID) -> None:
        """Drop queued work after membership refresh; active work may finish."""
        async with self._admission_lock:
            self._require_open()
            try:
                await self._scheduler._purge_feed(self._grant, feed_id)
            except _shard._ShardFatalError as exc:
                message = "scheduler shard integrity failed"
                raise SchedulerIntegrityError(message) from exc.failure

    async def close(
        self,
        reason: _types.LaneCloseReason = _types.LaneCloseReason.PLANNED_DRAIN,
    ) -> _types.LaneClosed | _types.Undrained:
        """Close this exact grant with monotonic shared coordination."""
        return await asyncio.shield(self._request_close(reason))

    def _request_close(
        self,
        reason: _types.LaneCloseReason,
    ) -> asyncio.Task[_types.LaneClosed | _types.Undrained]:
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
        self._scheduler._closing_grants.add(self._grant)
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
        try:
            await self._scheduler._wake_admission()
            await self._scheduler._purge_exact(self._grant)
            while True:
                if self._close_strength is _CloseStrength.CANCELLING:
                    await self._scheduler._cancel_exact(self._grant)
                    await self._scheduler._wait_exact_empty(self._grant)
                    break
                if await self._wait_for_drain_or_upgrade():
                    break
            self._scheduler._raise_fatal()
        except (
            SchedulerIntegrityError,
            _shard._ShardFatalError,
            _shard._ShardUndrainedError,
        ):
            return _types.Undrained(self._grant, self._close_reason)
        self._closed = True
        self._scheduler._lane_closed(self)
        return _types.LaneClosed(self._grant, self._close_reason)

    async def _wait_for_drain_or_upgrade(self) -> bool:
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

    def _validate_candidate(
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

    def _require_open(self) -> None:
        self._scheduler._raise_fatal()
        if self._closing_event.is_set() or self._closed:
            message = "exact grant lane is closing"
            raise _LaneClosedError(message)
