"""Process scheduler and exact-grant streaming page facade."""

# Private sibling modules deliberately compose the scheduler's closed core.
# ruff: noqa: SLF001

from __future__ import annotations

import asyncio
import dataclasses
import enum
import typing
import uuid

from backend.pipeline.ingestion.collectors.bcfy_calls import cursor_policy
from backend.pipeline.ingestion.feed_work_scheduler import (
    _boundaries,
    _shard,
    _types,
)
from backend.pipeline.storage import feed_store, ingestion_lease_store

if typing.TYPE_CHECKING:
    import collections.abc


class SchedulerIntegrityError(RuntimeError):
    """The process scheduler can no longer prove safe admission."""


class _LaneClosedError(RuntimeError):
    """An exact lane closed before page coverage linearized."""


class _CloseStrength(enum.IntEnum):
    """Monotonic exact-lane lifecycle strength."""

    OPEN = 0
    DRAINING = 1
    CANCELLING = 2


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
    registered_flusher_tasks: int
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
        boundary_committer: _types.BoundaryCommitter | None = None,
        _limits: _types._SchedulerLimits = _types._PRODUCTION_LIMITS,
        _boundary_batch_size: int = _boundaries._BOUNDARY_BATCH_SIZE,
    ) -> None:
        """Create one scheduler with immutable production or test limits.

        Args:
            executor: Private full-pipeline adapter for fixed workers.
            boundary_committer: Closed exact-grant persistence seam. Phase 4
                uses the deterministic default until Phase 5 wires storage.
            _limits: Validated deterministic-test limits. Production callers
                use the fixed default.
        """
        if not isinstance(_limits, _types._SchedulerLimits):
            message = "_limits must be _SchedulerLimits"
            raise TypeError(message)
        if boundary_committer is None:
            boundary_committer = _boundaries._DefaultBoundaryCommitter()
        if not callable(getattr(boundary_committer, "commit", None)):
            message = "boundary_committer must provide async commit"
            raise TypeError(message)
        _types._require_positive_integer(
            _boundary_batch_size,
            "_boundary_batch_size",
        )
        self._limits = _limits
        self._boundary_committer = boundary_committer
        self._boundary_batch_size = _boundary_batch_size
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
        self._shards = tuple(
            _shard._Shard(
                index,
                executor,
                limits=_limits,
                outcome_observer=self._observe_outcome,
                grant_is_closing=self._closing_grants.__contains__,
                fatal_observer=self._observe_fatal,
                global_fatal=lambda: self._fatal,
                abandonment_for=self._abandonment.get,
                boundary_ready_observer=self._observe_boundary_ready,
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
        slot = self._lease_slot(grant)
        highest = self._highest_fence.get(slot)
        if highest is not None and grant.fencing_token <= highest:
            message = "grant is not newer than the lane slot history"
            raise ValueError(message)
        self._prune_closed_slot(grant)
        lane = GrantLane(self, grant)
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
        registered = sum(
            worker.task_registered
            for snapshot in snapshots
            for worker in snapshot.workers
        )
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
    ) -> None:
        for shard in self._shards:
            await shard.purge_page(grant, page_sequence)

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

    async def _purge_exact(
        self,
        grant: ingestion_lease_store.LeaseGrant,
    ) -> None:
        for shard in self._shards:
            await shard.purge_exact(grant)

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

    async def _abandon_exact_cancellations(
        self,
        grant: ingestion_lease_store.LeaseGrant,
        failure: BaseException,
    ) -> None:
        for shard in self._shards:
            await shard.abandon_exact_cancellations(grant, failure)

    async def _retire_feed(
        self,
        grant: ingestion_lease_store.LeaseGrant,
        feed_id: uuid.UUID,
    ) -> _types._RetireFeedResult:
        return await self._shard_for(feed_id).retire_feed(grant, feed_id)

    async def _forget_retired_grant(
        self,
        grant: ingestion_lease_store.LeaseGrant,
    ) -> None:
        for shard in self._shards:
            await shard.forget_retired_grant(grant)

    async def _wake_admission(self) -> None:
        for shard in self._shards:
            await shard.wake_waiters()

    def _shard_for(self, feed_id: uuid.UUID) -> _shard._Shard:
        index = _types._shard_index(feed_id, self._limits)
        return self._shards[index]

    def _observe_outcome(
        self,
        record: _types._CallRecord,
        outcome: _types._ExecutorOutcome,
        retirement: _types._RetireFeedResult | None,
    ) -> None:
        """Project terminal membership/loss evidence into its exact lane."""
        lane = self._lanes.get(record.grant)
        if lane is None:
            return
        if isinstance(outcome, _types._ExecutorAuthorityLost):
            lane._request_close(_types.LaneCloseReason.AUTHORITY_LOSS)
        elif isinstance(outcome, _types._ExecutorMembershipRejected):
            if retirement is None:
                message = "membership outcome omitted retirement evidence"
                raise RuntimeError(message)
            lane._observe_membership(record, retirement)

    def _observe_boundary_ready(
        self,
        grant: ingestion_lease_store.LeaseGrant,
    ) -> None:
        """Coalesce a shard-ready notification into its exact lane Event."""
        lane = self._lanes.get(grant)
        if lane is not None:
            lane._boundary_coordinator.notify_ready()

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
        """Release superseded closed-lane/task history by Lease slot."""
        slot = self._lease_slot(lane.grant)
        successor_exists = any(
            other is not lane and self._lease_slot(grant) == slot
            for grant, other in self._lanes.items()
        )
        if successor_exists:
            self._lanes.pop(lane.grant, None)
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
    ) -> None:
        self._scheduler = scheduler
        self._grant = grant
        self._admission_lock = asyncio.Lock()
        self._state_lock = asyncio.Lock()
        self._closing_event = asyncio.Event()
        self._close_changed = asyncio.Event()
        self._next_page_sequence = 0
        self._page: _PageBarrier | None = None
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
            batch_size=scheduler._boundary_batch_size,
        )

    @property
    def grant(self) -> ingestion_lease_store.LeaseGrant:
        """Return the complete immutable grant bound to this lane."""
        return self._grant

    async def remove_feed(self, feed_id: uuid.UUID) -> _types.FeedRemoved:
        """Retire one Feed only from this lane's complete grant."""
        if not isinstance(feed_id, uuid.UUID):
            message = "feed_id must be a UUID"
            raise TypeError(message)
        self._raise_closed_locked()
        try:
            result = await self._scheduler._retire_feed(
                self._grant,
                feed_id,
            )
        except _shard._ShardFatalError as exc:
            message = "scheduler shard integrity failed"
            raise SchedulerIntegrityError(message) from exc.failure
        await self._localize_released(result.released_calls)
        return _types.FeedRemoved(
            grant=self._grant,
            feed_id=feed_id,
            released_count=len(result.released_sequences),
            active_retained=result.active_sequence is not None,
        )

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
        calls: collections.abc.Iterable[_types.CallSubmission],
        boundaries: collections.abc.Iterable[_types.BoundaryWork],
        candidate: cursor_policy.PageCursorCandidate,
    ) -> cursor_policy._CoveredPage:
        """Incrementally cover one source-order call page.

        Args:
            calls: Single-pass call submissions in provider source order.
            boundaries: Single-pass trailing Feed completion boundaries.
            candidate: Exact cursor candidate awaiting bounded coverage.

        Returns:
            Private sealed receipt consumable only by ``LeaseCursor``.

        Raises:
            CursorIntegrityError: Candidate authority or sequence is wrong.
            RuntimeError: The lane or scheduler is closing or failed.
        """
        async with self._admission_lock:
            await self._validate_candidate(candidate)
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
                    except _shard._FeedRetiredError:
                        await self._mark_localized(source_order)
                        continue
                    except _shard._AdmissionAbortedError as exc:
                        message = "lane closed during page admission"
                        raise _LaneClosedError(message) from exc
                    except _shard._ShardFatalError as exc:
                        message = "scheduler shard integrity failed"
                        raise SchedulerIntegrityError(message) from exc.failure
                    await self._mark_registered(source_order)
                for boundary in boundaries:
                    self._require_boundary(boundary)
                    source_order = await self._next_source_order()
                    await self._mark_pulled(source_order)
                    boundary_input = _types._BoundaryInput(
                        boundary=boundary,
                        grant=self._grant,
                        source_order=source_order,
                        page_sequence=candidate.page_sequence,
                    )
                    shard = self._scheduler._shard_for(boundary.feed_id)
                    try:
                        await shard.admit_boundary(
                            boundary_input,
                            abort_event=self._closing_event,
                            pressure_relief=(
                                self._boundary_coordinator.request_relief
                            ),
                        )
                    except _shard._FeedRetiredError:
                        await self._mark_localized(source_order)
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
                    await self._mark_registered(source_order)
                try:
                    await self._boundary_coordinator.request_final()
                except _boundaries._BoundaryAuthorityLostError as exc:
                    message = "exact grant lost boundary authority"
                    raise _LaneClosedError(message) from exc
                except _boundaries._BoundaryCoordinatorError as exc:
                    self._scheduler._raise_fatal()
                    message = "boundary coordinator integrity failed"
                    raise SchedulerIntegrityError(message) from exc
                await self._scheduler._promote_boundary_page(
                    self._grant,
                    candidate.page_sequence,
                )
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

    async def _next_source_order(self) -> int:
        async with self._state_lock:
            page = self._require_page_locked()
            if page.current_source_order is not None:
                message = "cannot inspect the next source record while blocked"
                raise RuntimeError(message)
            return page.pulled

    async def _mark_registered(self, source_order: int) -> None:
        async with self._state_lock:
            page = self._require_page_locked()
            if page.current_source_order is None:
                if (
                    source_order != page.pulled - 1
                    or page.pulled != page.registered + page.localized
                ):
                    message = "source record lost its barrier transition"
                    raise RuntimeError(message)
                return
            if page.current_source_order != source_order:
                message = "registered source record does not match barrier"
                raise RuntimeError(message)
            page.current_source_order = None
            page.registered += 1

    async def _mark_localized(self, source_order: int) -> None:
        async with self._state_lock:
            page = self._require_page_locked()
            if page.current_source_order != source_order:
                message = "localized source record does not match barrier"
                raise RuntimeError(message)
            page.current_source_order = None
            page.localized += 1

    async def _localize_released(
        self,
        released_calls: tuple[tuple[int, int], ...],
    ) -> None:
        async with self._state_lock:
            for page_sequence, source_order in released_calls:
                self._localize_tag(page_sequence, source_order)

    def _observe_membership(
        self,
        record: _types._CallRecord,
        retirement: _types._RetireFeedResult,
    ) -> None:
        """Localize a terminal membership outcome without another task."""
        self._localize_tag(
            record.work.page_sequence,
            record.work.source_order,
        )
        for page_sequence, source_order in retirement.released_calls:
            self._localize_tag(page_sequence, source_order)

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
        await self._scheduler._abort_boundary_page(
            self._grant,
            page_sequence,
        )
        await self._scheduler._purge_page(self._grant, page_sequence)
        async with self._state_lock:
            if (
                self._page is not None
                and self._page.page_sequence == page_sequence
            ):
                self._page = None

    def _request_close(
        self,
        reason: _types.LaneCloseReason,
    ) -> asyncio.Task[_types.LaneClosed | _types.Undrained]:
        """Publish/strengthen close before returning an awaitable task."""
        if not isinstance(reason, _types.LaneCloseReason):
            message = "reason must be a LaneCloseReason"
            raise TypeError(message)
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
            await self._boundary_coordinator.close()
            await self._scheduler._purge_exact(self._grant)
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
                await self._scheduler._forget_retired_grant(self._grant)
                self._scheduler._raise_fatal()
                self._scheduler._clear_abandonment(self._grant)
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
    def _require_boundary(
        boundary: object,
    ) -> None:
        if type(boundary) is not _types.BoundaryWork:
            message = "boundaries must contain exact BoundaryWork values"
            raise TypeError(message)
