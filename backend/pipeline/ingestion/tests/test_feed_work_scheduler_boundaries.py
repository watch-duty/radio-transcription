"""Event-gated boundary ordering and exact-committer contract tests."""

from __future__ import annotations

import asyncio
import collections
import datetime
import importlib
import typing
import unittest
import uuid

from backend.pipeline.ingestion import feed_work_scheduler
from backend.pipeline.ingestion.collectors.bcfy_calls import cursor_policy
from backend.pipeline.storage import feed_store, ingestion_lease_store

_OWNER_ID = uuid.UUID("11111111-2222-3333-4444-555555555555")
_SOURCE_TIME = datetime.datetime(2026, 7, 12, 12, 0, tzinfo=datetime.UTC)


def _types() -> typing.Any:
    return importlib.import_module(
        "backend.pipeline.ingestion.feed_work_scheduler._types"
    )


def _limits(
    *,
    shard_count: int = 1,
    capacity: int = 8,
    workers: int = 1,
    high_water: int = 8,
    resume_at: int = 4,
) -> object:
    return _types()._SchedulerLimits(
        shard_count=shard_count,
        capacity=capacity,
        workers_per_shard=workers,
        high_water=high_water,
        resume_at=resume_at,
    )


def _grant(
    *,
    lease_key: str = "150",
    fencing_token: int = 1,
) -> ingestion_lease_store.LeaseGrant:
    return ingestion_lease_store.LeaseGrant(
        source_type=feed_store.SourceType.BCFY_CALLS,
        lease_key=lease_key,
        owner_worker_id=_OWNER_ID,
        fencing_token=fencing_token,
    )


def _open_lane(
    scheduler: feed_work_scheduler.FeedWorkScheduler,
    grant: ingestion_lease_store.LeaseGrant,
) -> feed_work_scheduler.GrantLane:
    return scheduler.open_lane(
        grant,
        stop_requested=asyncio.Event(),
        grant_lost=asyncio.Event(),
    )


def _call(
    feed_id: uuid.UUID,
    source_order: int,
    *,
    grant: ingestion_lease_store.LeaseGrant | None = None,
) -> object:
    exact_grant = _grant() if grant is None else grant
    member = _member(feed_id, grant=exact_grant)
    timestamp = _SOURCE_TIME + datetime.timedelta(seconds=source_order)
    call = feed_work_scheduler.CallSubmission(
        feed_id=feed_id,
        source_timestamp=timestamp,
        payload={"source_order": source_order, "member": member},
    )
    return feed_work_scheduler.CohortSubmission(
        member=member,
        feed_id=feed_id,
        cohort_timestamp=timestamp,
        calls=(call,),
        admission_hook=lambda _identities: None,
    )


def _member(
    feed_id: uuid.UUID,
    *,
    grant: ingestion_lease_store.LeaseGrant | None = None,
) -> ingestion_lease_store.LeaseMemberIdentity:
    exact_grant = _grant() if grant is None else grant
    return ingestion_lease_store._issue_member_identity(
        exact_grant,
        feed_id=feed_id,
        source_type=feed_store.SourceType.BCFY_CALLS,
        source_feed_id=f"{exact_grant.lease_key}-{feed_id.int}",
        sid=exact_grant.lease_key,
        group_id=str(feed_id.int),
    )


def _boundary(
    feed_id: uuid.UUID,
    seconds: int,
    *,
    grant: ingestion_lease_store.LeaseGrant | None = None,
) -> object:
    return _types().BoundaryWork(
        member=_member(feed_id, grant=grant),
        target=_SOURCE_TIME + datetime.timedelta(seconds=seconds),
    )


def _terminal_facts(
    execution: feed_work_scheduler.CohortExecution,
) -> feed_work_scheduler.CohortTerminalFacts:
    return feed_work_scheduler.CohortTerminalFacts(
        records=tuple(
            feed_work_scheduler.CohortRecordTerminalFact(
                identity=call.identity,
                participated=True,
                closure_state=(
                    feed_work_scheduler.CohortRecordClosureState.DURABLY_CLOSED
                ),
                full_pipeline_completed=True,
                terminal_reason=(
                    feed_work_scheduler.CohortRecordTerminalReason.FULL_PIPELINE
                ),
            )
            for call in execution.calls
        ),
        disposition=feed_work_scheduler.CohortTerminalDisposition.SETTLED,
    )


def _completed(
    execution: feed_work_scheduler.CohortExecution,
) -> feed_work_scheduler.CallCompleted:
    return feed_work_scheduler.CallCompleted(_terminal_facts(execution))


def _outcome_unknown(
    execution: feed_work_scheduler.CohortExecution,
) -> feed_work_scheduler.CallOutcomeUnknown:
    return feed_work_scheduler.CallOutcomeUnknown(
        feed_work_scheduler.CohortTerminalFacts(
            records=tuple(
                feed_work_scheduler.CohortRecordTerminalFact(
                    identity=call.identity,
                    participated=True,
                    closure_state=(
                        feed_work_scheduler.CohortRecordClosureState.OUTCOME_UNKNOWN
                    ),
                    full_pipeline_completed=False,
                    terminal_reason=(
                        feed_work_scheduler.CohortRecordTerminalReason.OUTCOME_UNKNOWN
                    ),
                )
                for call in execution.calls
            ),
            disposition=(
                feed_work_scheduler.CohortTerminalDisposition.OUTCOME_UNKNOWN
            ),
        )
    )


class _ImmediateExecutor:
    async def execute(self, record: object) -> object:
        execution = typing.cast(
            "feed_work_scheduler.CohortExecution",
            record,
        )
        return _completed(execution)


class _OutcomeUnknownExecutor:
    async def execute(self, record: object) -> object:
        return _outcome_unknown(
            typing.cast("feed_work_scheduler.CohortExecution", record)
        )


class _GateExecutor:
    def __init__(self) -> None:
        self.entered = asyncio.Event()
        self.release = asyncio.Event()
        self.calls = 0

    async def execute(self, record: object) -> object:
        execution = typing.cast(
            "feed_work_scheduler.CohortExecution",
            record,
        )
        self.calls += 1
        self.entered.set()
        await self.release.wait()
        return _completed(execution)


class _ControlledCommitter:
    """Closed fake with optional gates and caller-correlated outcomes."""

    def __init__(self) -> None:
        self.calls: list[tuple[object, tuple[object, ...], bool]] = []
        self.changed = asyncio.Event()
        self.block_nonempty = False
        self.block_final_number: int | None = None
        self.release = asyncio.Event()
        self.final_calls = 0
        self.dispositions: dict[uuid.UUID, object] = {}
        self.override_result: object | None = None
        self.inspect: typing.Callable[[], None] | None = None
        self.rejected_grants: set[object] = set()
        self.scripted_dispositions: collections.deque[object] = (
            collections.deque()
        )
        self.scripted_batch_results: collections.deque[object] = (
            collections.deque()
        )

    async def commit(
        self,
        grant: object,
        boundaries: tuple[object, ...],
        *,
        final_logical: bool,
    ) -> object:
        if self.inspect is not None:
            self.inspect()
        self.calls.append((grant, boundaries, final_logical))
        if final_logical:
            self.final_calls += 1
        self.changed.set()
        should_block = self.block_nonempty and bool(boundaries)
        should_block = should_block or (
            final_logical
            and self.block_final_number is not None
            and self.final_calls == self.block_final_number
        )
        if should_block:
            await self.release.wait()
        if self.override_result is not None:
            return self.override_result
        scheduler_types = _types()
        if grant in self.rejected_grants:
            return scheduler_types.BoundaryGrantRejected()
        if self.scripted_batch_results:
            return self.scripted_batch_results.popleft()
        scripted = (
            self.scripted_dispositions.popleft()
            if boundaries and self.scripted_dispositions
            else None
        )
        return scheduler_types.BoundaryBatchCommitted(
            tuple(
                scheduler_types.BoundaryResult(
                    boundary,
                    scripted
                    or self.dispositions.get(
                        boundary.feed_id,
                        scheduler_types.BoundaryDisposition.COMMITTED,
                    ),
                )
                for boundary in boundaries
            )
        )

    async def wait_for_calls(self, count: int) -> None:
        while len(self.calls) < count:
            self.changed.clear()
            if len(self.calls) >= count:
                return
            await asyncio.wait_for(self.changed.wait(), timeout=1)


class _ControlledClock:
    """Deterministic monotonic clock advanced only by a test owner."""

    def __init__(self) -> None:
        self.value = 0.0

    def __call__(self) -> float:
        return self.value

    def advance(self, seconds: float) -> None:
        self.value += seconds


class _TimedCommitter:
    """Gate each actual attempt so tests own its exact elapsed time."""

    def __init__(self) -> None:
        self.calls: list[tuple[object, tuple[object, ...], bool]] = []
        self.changed = asyncio.Event()
        self.releases: dict[int, asyncio.Event] = {}
        self.results: collections.deque[object] = collections.deque()
        self.member_rejected_feeds: set[uuid.UUID] = set()

    async def commit(
        self,
        grant: object,
        boundaries: tuple[object, ...],
        *,
        final_logical: bool,
    ) -> object:
        call_index = len(self.calls)
        self.calls.append((grant, boundaries, final_logical))
        release = self.releases.setdefault(call_index, asyncio.Event())
        self.changed.set()
        await release.wait()
        if self.results:
            return self.results.popleft()
        scheduler_types = _types()
        return scheduler_types.BoundaryBatchCommitted(
            tuple(
                scheduler_types.BoundaryResult(
                    boundary,
                    (
                        scheduler_types.BoundaryDisposition.MEMBER_REJECTED
                        if boundary.feed_id in self.member_rejected_feeds
                        else scheduler_types.BoundaryDisposition.COMMITTED
                    ),
                )
                for boundary in boundaries
            )
        )

    async def wait_for_calls(self, count: int) -> None:
        while len(self.calls) < count:
            self.changed.clear()
            if len(self.calls) >= count:
                return
            await asyncio.wait_for(self.changed.wait(), timeout=1)

    def release(self, call_index: int) -> None:
        self.releases.setdefault(call_index, asyncio.Event()).set()


class _TracingBoundaries:
    def __init__(self, boundaries: typing.Iterable[object]) -> None:
        self._boundaries = iter(boundaries)
        self.pulled: list[uuid.UUID] = []

    def __iter__(self) -> _TracingBoundaries:
        return self

    def __next__(self) -> object:
        boundary = next(self._boundaries)
        self.pulled.append(boundary.feed_id)
        return boundary


class TestBoundaryOrdering(unittest.IsolatedAsyncioTestCase):
    async def test_lane_flushing_owns_one_event_without_audio_slot(
        self,
    ) -> None:
        scheduler = feed_work_scheduler.FeedWorkScheduler(
            _ImmediateExecutor(),
            boundary_committer=_ControlledCommitter(),
            _limits=_limits(workers=2),
        )
        await scheduler.start()
        lane = _open_lane(scheduler, _grant())
        coordinator = lane._boundary_coordinator

        snapshot = await scheduler._snapshot()
        self.assertEqual(snapshot.registered_worker_tasks, 2)
        self.assertEqual(snapshot.registered_flusher_tasks, 1)
        self.assertIsInstance(coordinator.signal, asyncio.Event)
        self.assertFalse(coordinator.task.done())
        self.assertFalse(hasattr(coordinator, "_queue"))
        self.assertFalse(hasattr(coordinator, "_mailbox"))
        self.assertFalse(hasattr(coordinator, "_history"))

        await lane.close()
        await scheduler.close()

    async def test_call_pressure_blocks_boundary_stream_pull(self) -> None:
        executor = _GateExecutor()
        committer = _ControlledCommitter()
        scheduler = feed_work_scheduler.FeedWorkScheduler(
            executor,
            boundary_committer=committer,
            _limits=_limits(
                capacity=3,
                workers=1,
                high_water=1,
                resume_at=0,
            ),
        )
        await scheduler.start()
        grant = _grant()
        lane = _open_lane(scheduler, grant)
        feed_ids = (uuid.UUID(int=1), uuid.UUID(int=2))
        boundaries = _TracingBoundaries((_boundary(feed_ids[0], 10),))
        candidate = cursor_policy.LeaseCursor(grant, pos=None).prepare(
            _SOURCE_TIME
        )
        coverage = asyncio.create_task(
            lane.cover_page(
                calls=(_call(feed_ids[index], index) for index in range(2)),
                boundaries=boundaries,
                candidate=candidate,
            )
        )
        try:
            await scheduler._shards[0].wait_for_capacity_waiters(1)
            self.assertEqual(boundaries.pulled, [feed_ids[0]])
            self.assertEqual(committer.calls, [])
            executor.release.set()
            await asyncio.wait_for(coverage, timeout=1)
            self.assertEqual(boundaries.pulled, [feed_ids[0]])
        finally:
            executor.release.set()
            if not coverage.done():
                coverage.cancel()
                await asyncio.gather(coverage, return_exceptions=True)
            await scheduler._wait_for_idle()
            await scheduler.close()


class TestBoundaryEvidence(unittest.IsolatedAsyncioTestCase):
    async def test_unknown_evidence_waits_for_inflight_early_flush(
        self,
    ) -> None:
        clock = _ControlledClock()
        committer = _TimedCommitter()
        observed: list[feed_work_scheduler.SchedulerPageEvidence] = []
        scheduler = feed_work_scheduler.FeedWorkScheduler(
            _OutcomeUnknownExecutor(),
            boundary_committer=committer,
            _limits=_limits(),
            _monotonic=clock,
        )
        await scheduler.start()
        grant = _grant()
        lane = _open_lane(scheduler, grant)
        coverage = asyncio.create_task(
            lane.cover_page(
                calls=(_call(uuid.UUID(int=1), 0, grant=grant),),
                boundaries=(_boundary(uuid.UUID(int=2), 1, grant=grant),),
                candidate=cursor_policy.LeaseCursor(
                    grant,
                    pos=None,
                ).prepare(_SOURCE_TIME),
                evidence_observer=observed.append,
            )
        )

        try:
            await committer.wait_for_calls(1)
            self.assertFalse(committer.calls[0][2])
            self.assertFalse(coverage.done())
            clock.advance(2.0)
            committer.release(0)
            self.assertIsInstance(
                await asyncio.wait_for(coverage, timeout=1),
                feed_work_scheduler.Undrained,
            )

            self.assertEqual(len(observed), 1)
            self.assertEqual(observed[0].early_flush_attempt_count, 1)
            self.assertEqual(observed[0].total_flush_latency_seconds, 2.0)
            self.assertEqual((await scheduler._snapshot()).held, 1)
            self.assertIsNone(lane._page.evidence_observer)
        finally:
            committer.release(0)
            await scheduler.close()

    async def test_unknown_cancellation_retains_page_after_evidence_settlement(
        self,
    ) -> None:
        clock = _ControlledClock()
        committer = _TimedCommitter()
        observed: list[feed_work_scheduler.SchedulerPageEvidence] = []
        scheduler = feed_work_scheduler.FeedWorkScheduler(
            _OutcomeUnknownExecutor(),
            boundary_committer=committer,
            _limits=_limits(),
            _monotonic=clock,
        )
        await scheduler.start()
        grant = _grant()
        lane = _open_lane(scheduler, grant)
        coverage = asyncio.create_task(
            lane.cover_page(
                calls=(_call(uuid.UUID(int=1), 0, grant=grant),),
                boundaries=(_boundary(uuid.UUID(int=2), 1, grant=grant),),
                candidate=cursor_policy.LeaseCursor(
                    grant,
                    pos=None,
                ).prepare(_SOURCE_TIME),
                evidence_observer=observed.append,
            )
        )

        try:
            await committer.wait_for_calls(1)
            for _unused in range(100):
                if (
                    lane._page is not None
                    and lane._page.uncertainty is not None
                ):
                    break
                await asyncio.sleep(0)
            self.assertIsNotNone(lane._page)
            self.assertIsNotNone(lane._page.uncertainty)

            coverage.cancel()
            await asyncio.sleep(0)
            self.assertFalse(coverage.done())
            clock.advance(2.0)
            committer.release(0)
            with self.assertRaises(asyncio.CancelledError):
                await asyncio.wait_for(coverage, timeout=1)

            self.assertEqual(len(observed), 1)
            self.assertEqual(observed[0].early_flush_attempt_count, 1)
            self.assertEqual(observed[0].total_flush_latency_seconds, 2.0)
            self.assertEqual((await scheduler._snapshot()).held, 1)
            self.assertIsNotNone((await lane._snapshot()).page)
            self.assertIsNone(lane._page.evidence_observer)
        finally:
            committer.release(0)
            await scheduler.close()

    async def test_evidence_cutoff_does_not_start_post_final_followup_flush(
        self,
    ) -> None:
        clock = _ControlledClock()
        committer = _TimedCommitter()
        scheduler = feed_work_scheduler.FeedWorkScheduler(
            _ImmediateExecutor(),
            boundary_committer=committer,
            _limits=_limits(),
            _boundary_batch_size=1,
            _monotonic=clock,
        )
        await scheduler.start()
        grant = _grant()
        lane = _open_lane(scheduler, grant)
        ready_check_entered = asyncio.Event()
        release_ready_check = asyncio.Event()
        has_ready_boundary = lane._boundary_coordinator._has_ready_boundary

        async def gated_has_ready_boundary() -> bool:
            ready_check_entered.set()
            await release_ready_check.wait()
            return await has_ready_boundary()

        lane._boundary_coordinator._has_ready_boundary = (
            gated_has_ready_boundary
        )
        coverage = asyncio.create_task(
            lane.cover_page(
                calls=(),
                boundaries=tuple(
                    _boundary(uuid.UUID(int=index), index, grant=grant)
                    for index in range(1, 4)
                ),
                candidate=cursor_policy.LeaseCursor(
                    grant,
                    pos=None,
                ).prepare(_SOURCE_TIME),
            )
        )

        try:
            await committer.wait_for_calls(1)
            self.assertTrue(committer.calls[0][2])
            clock.advance(1.0)
            committer.release(0)
            await asyncio.wait_for(ready_check_entered.wait(), timeout=1)
            for _unused in range(100):
                if coverage.done():
                    break
                await asyncio.sleep(0)
            self.assertTrue(coverage.done())
            evidence = coverage.result().scheduler_evidence

            self.assertEqual(len(committer.calls), 1)
            self.assertEqual(evidence.early_flush_attempt_count, 0)
            self.assertEqual(evidence.final_flush_attempt_count, 1)
            self.assertEqual(evidence.total_flush_latency_seconds, 1.0)
            self.assertEqual(evidence.maximum_flush_latency_seconds, 1.0)

            release_ready_check.set()
            await committer.wait_for_calls(2)
            self.assertFalse(committer.calls[1][2])
            committer.release(1)
            await committer.wait_for_calls(3)
            self.assertFalse(committer.calls[2][2])
            committer.release(2)
            await asyncio.wait_for(scheduler._wait_for_idle(), timeout=1)
            self.assertEqual(evidence.early_flush_attempt_count, 0)
        finally:
            release_ready_check.set()
            for index in range(3):
                committer.release(index)
            if not coverage.done():
                coverage.cancel()
                await asyncio.gather(coverage, return_exceptions=True)
            await scheduler.close()

    async def test_rolled_back_stable_boundary_is_not_page_evidence(
        self,
    ) -> None:
        clock = _ControlledClock()
        committer = _TimedCommitter()
        scheduler = feed_work_scheduler.FeedWorkScheduler(
            _ImmediateExecutor(),
            boundary_committer=committer,
            _limits=_limits(),
            _monotonic=clock,
        )
        await scheduler.start()
        grant = _grant()
        lane = _open_lane(scheduler, grant)
        coordinator = lane._boundary_coordinator
        page = await lane._begin_page(1, 0, evidence_observer=None)
        shard = scheduler._shards[0]
        async with coordinator._generation_changed:
            coordinator._evidence_cutoff = True
        record = await shard.admit_boundary(
            _types()._BoundaryInput(
                _boundary(uuid.UUID(int=1), 2, grant=grant),
                grant,
                0,
                1,
            ),
            abort_event=asyncio.Event(),
            pressure_relief=coordinator.request_relief,
        )
        page.evidence.observe_boundary_registration(record)
        record.stable_target = _SOURCE_TIME
        await shard.abort_boundary_page(grant, 1)

        try:
            async with coordinator._generation_changed:
                coordinator._evidence_cutoff = False
                coordinator._signal.set()
                coordinator._generation_changed.notify_all()
            await committer.wait_for_calls(1)
            clock.advance(2.0)
            committer.release(0)
            await asyncio.wait_for(scheduler._wait_for_idle(), timeout=1)
            await coordinator.settle_page_evidence(page.evidence)
            evidence = page.evidence.freeze()

            self.assertEqual(record.target, _SOURCE_TIME)
            self.assertEqual(evidence.early_flush_attempt_count, 0)
            self.assertEqual(evidence.total_flush_latency_seconds, 0.0)
        finally:
            committer.release(0)
            await lane._abort_page(page)
            await scheduler.close()

    async def test_relief_adopts_inflight_old_boundary_evidence(self) -> None:
        clock = _ControlledClock()
        committer = _TimedCommitter()
        scheduler = feed_work_scheduler.FeedWorkScheduler(
            _ImmediateExecutor(),
            boundary_committer=committer,
            _limits=_limits(),
            _monotonic=clock,
        )
        await scheduler.start()
        grant = _grant()
        lane = _open_lane(scheduler, grant)
        page = await lane._begin_page(1, 0, evidence_observer=None)
        coordinator = lane._boundary_coordinator
        shard = scheduler._shards[0]
        await shard.admit_boundary(
            _types()._BoundaryInput(
                _boundary(uuid.UUID(int=1), 1, grant=grant),
                grant,
                0,
                0,
            ),
            abort_event=asyncio.Event(),
            pressure_relief=coordinator.request_relief,
        )
        await shard.promote_boundary_page(grant, 0)
        await shard.seal_boundary_page(grant, 0)
        coordinator.notify_ready()

        relief: asyncio.Task[object] | None = None
        try:
            await committer.wait_for_calls(1)
            relief = asyncio.create_task(coordinator.request_relief())
            for _unused in range(100):
                if coordinator.requested_generation == 1:
                    break
                await asyncio.sleep(0)
            self.assertEqual(coordinator.requested_generation, 1)
            clock.advance(3.0)
            committer.release(0)
            await asyncio.wait_for(relief, timeout=1)
            await asyncio.wait_for(
                coordinator.settle_page_evidence(page.evidence),
                timeout=1,
            )
            evidence = page.evidence.freeze()

            self.assertEqual(len(committer.calls), 1)
            self.assertEqual(evidence.early_flush_attempt_count, 1)
            self.assertEqual(evidence.total_flush_latency_seconds, 3.0)
        finally:
            committer.release(0)
            if relief is not None and not relief.done():
                relief.cancel()
                await asyncio.gather(relief, return_exceptions=True)
            await lane._abort_page(page)
            await scheduler.close()

    async def test_empty_pressure_relief_attempt_belongs_to_page_evidence(
        self,
    ) -> None:
        clock = _ControlledClock()
        committer = _TimedCommitter()
        executor = _GateExecutor()
        observed: list[feed_work_scheduler.SchedulerPageEvidence] = []
        scheduler = feed_work_scheduler.FeedWorkScheduler(
            executor,
            boundary_committer=committer,
            _limits=_limits(
                capacity=1,
                workers=1,
                high_water=1,
                resume_at=0,
            ),
            _monotonic=clock,
        )
        await scheduler.start()
        grant = _grant()
        lane = _open_lane(scheduler, grant)
        coverage = asyncio.create_task(
            lane.cover_page(
                calls=(_call(uuid.UUID(int=1), 0, grant=grant),),
                boundaries=(_boundary(uuid.UUID(int=2), 1, grant=grant),),
                candidate=cursor_policy.LeaseCursor(
                    grant,
                    pos=None,
                ).prepare(_SOURCE_TIME),
                evidence_observer=observed.append,
            )
        )

        try:
            await committer.wait_for_calls(1)
            self.assertEqual(committer.calls[0], (grant, (), False))
            clock.advance(2.0)
            committer.release(0)
            await scheduler._shards[0].wait_for_capacity_waiters(1)
            coverage.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await coverage

            self.assertEqual(len(observed), 1)
            self.assertEqual(observed[0].early_flush_attempt_count, 1)
            self.assertEqual(observed[0].total_flush_latency_seconds, 2.0)
            self.assertEqual(observed[0].pressure_wait_seconds, 2.0)
        finally:
            committer.release(0)
            executor.release.set()
            await scheduler._wait_for_idle()
            await scheduler.close()

    async def test_evidence_times_early_and_final_flush_attempts(self) -> None:
        clock = _ControlledClock()
        committer = _TimedCommitter()
        rejected_feed = uuid.UUID(int=1)
        committer.member_rejected_feeds.add(rejected_feed)
        scheduler = feed_work_scheduler.FeedWorkScheduler(
            _ImmediateExecutor(),
            boundary_committer=committer,
            _limits=_limits(
                capacity=2,
                workers=1,
                high_water=1,
                resume_at=0,
            ),
            _monotonic=clock,
        )
        await scheduler.start()
        grant = _grant()
        lane = _open_lane(scheduler, grant)
        coverage = asyncio.create_task(
            lane.cover_page(
                calls=(),
                boundaries=(
                    _boundary(rejected_feed, 1, grant=grant),
                    _boundary(uuid.UUID(int=2), 2, grant=grant),
                ),
                candidate=cursor_policy.LeaseCursor(
                    grant,
                    pos=None,
                ).prepare(_SOURCE_TIME),
            )
        )

        try:
            await committer.wait_for_calls(1)
            self.assertFalse(committer.calls[0][2])
            clock.advance(2.0)
            committer.release(0)
            await committer.wait_for_calls(2)
            self.assertTrue(committer.calls[1][2])
            clock.advance(3.0)
            committer.release(1)
            evidence = (
                await asyncio.wait_for(coverage, timeout=1)
            ).scheduler_evidence

            self.assertEqual(evidence.early_flush_attempt_count, 1)
            self.assertEqual(evidence.final_flush_attempt_count, 1)
            self.assertEqual(evidence.total_flush_latency_seconds, 5.0)
            self.assertEqual(evidence.maximum_flush_latency_seconds, 3.0)
            self.assertEqual(evidence.member_rejection_count, 1)
            self.assertEqual(evidence.fence_rejection_count, 0)
            self.assertTrue(evidence.pressure_encountered)
            self.assertEqual(evidence.pressure_wait_count, 1)
            self.assertEqual(evidence.pressure_wait_seconds, 2.0)
            self.assertEqual(evidence.maximum_held_count, 1)
        finally:
            committer.release(0)
            committer.release(1)
            if not coverage.done():
                coverage.cancel()
                await asyncio.gather(coverage, return_exceptions=True)
            await scheduler.close()

    async def test_retryable_flush_latency_evidence_precedes_failure(
        self,
    ) -> None:
        clock = _ControlledClock()
        committer = _TimedCommitter()
        committer.results.append(_types().BoundaryBatchRetryable())
        observed: list[feed_work_scheduler.SchedulerPageEvidence] = []
        scheduler = feed_work_scheduler.FeedWorkScheduler(
            _ImmediateExecutor(),
            boundary_committer=committer,
            _limits=_limits(),
            _monotonic=clock,
        )
        await scheduler.start()
        grant = _grant()
        lane = _open_lane(scheduler, grant)
        coverage = asyncio.create_task(
            lane.cover_page(
                calls=(),
                boundaries=(),
                candidate=cursor_policy.LeaseCursor(
                    grant,
                    pos=None,
                ).prepare(_SOURCE_TIME),
                evidence_observer=observed.append,
            )
        )

        try:
            await committer.wait_for_calls(1)
            clock.advance(1.5)
            committer.release(0)
            with self.assertRaisesRegex(RuntimeError, "retryable"):
                await asyncio.wait_for(coverage, timeout=1)

            self.assertEqual(len(observed), 1)
            evidence = observed[0]
            self.assertEqual(evidence.early_flush_attempt_count, 0)
            self.assertEqual(evidence.final_flush_attempt_count, 1)
            self.assertEqual(evidence.total_flush_latency_seconds, 1.5)
            self.assertEqual(evidence.maximum_flush_latency_seconds, 1.5)
            self.assertEqual(evidence.fence_rejection_count, 0)
        finally:
            committer.release(0)
            await asyncio.gather(coverage, return_exceptions=True)
            await scheduler.close()

    async def test_fence_rejection_is_counted_before_authority_failure(
        self,
    ) -> None:
        clock = _ControlledClock()
        committer = _TimedCommitter()
        committer.results.append(_types().BoundaryGrantRejected())
        observed: list[feed_work_scheduler.SchedulerPageEvidence] = []
        scheduler = feed_work_scheduler.FeedWorkScheduler(
            _ImmediateExecutor(),
            boundary_committer=committer,
            _limits=_limits(),
            _monotonic=clock,
        )
        await scheduler.start()
        grant = _grant()
        lane = _open_lane(scheduler, grant)
        coverage = asyncio.create_task(
            lane.cover_page(
                calls=(),
                boundaries=(),
                candidate=cursor_policy.LeaseCursor(
                    grant,
                    pos=None,
                ).prepare(_SOURCE_TIME),
                evidence_observer=observed.append,
            )
        )

        try:
            await committer.wait_for_calls(1)
            clock.advance(2.5)
            committer.release(0)
            with self.assertRaises(RuntimeError):
                await asyncio.wait_for(coverage, timeout=1)

            self.assertEqual(len(observed), 1)
            evidence = observed[0]
            self.assertEqual(evidence.final_flush_attempt_count, 1)
            self.assertEqual(evidence.total_flush_latency_seconds, 2.5)
            self.assertEqual(evidence.fence_rejection_count, 1)
            self.assertEqual(evidence.member_rejection_count, 0)
            self.assertIsInstance(
                await lane.close(
                    feed_work_scheduler.LaneCloseReason.AUTHORITY_LOSS
                ),
                feed_work_scheduler.LaneClosed,
            )
        finally:
            committer.release(0)
            await asyncio.gather(coverage, return_exceptions=True)
            await scheduler.close()


class TestBoundaryPageFinalization(unittest.IsolatedAsyncioTestCase):
    async def _cancel_after_promotion(
        self,
        promotion_count: int,
    ) -> None:
        shard_count = 3
        executor = _GateExecutor()
        committer = _ControlledCommitter()
        scheduler = feed_work_scheduler.FeedWorkScheduler(
            typing.cast("typing.Any", executor),
            boundary_committer=typing.cast("typing.Any", committer),
            _limits=typing.cast(
                "typing.Any",
                _limits(
                    shard_count=shard_count,
                    capacity=12,
                    workers=1,
                    high_water=12,
                    resume_at=6,
                ),
            ),
        )
        await scheduler.start()
        grant = _grant()
        lane = _open_lane(scheduler, grant)
        cursor = cursor_policy.LeaseCursor(grant, pos=None)
        candidate = cursor.prepare(_SOURCE_TIME)
        feeds = tuple(uuid.UUID(int=index + 3) for index in range(shard_count))
        promotion_reached = asyncio.Event()
        allow_promotion = asyncio.Event()
        promoted = 0

        def gate_promotion(
            shard: typing.Any,
        ) -> typing.Callable[
            [ingestion_lease_store.LeaseGrant, int],
            typing.Awaitable[None],
        ]:
            original = shard.promote_boundary_page

            async def promote(
                exact_grant: ingestion_lease_store.LeaseGrant,
                page_sequence: int,
            ) -> None:
                nonlocal promoted
                await original(exact_grant, page_sequence)
                promoted += 1
                if promoted == promotion_count:
                    promotion_reached.set()
                    await allow_promotion.wait()

            return promote

        for shard in scheduler._shards:
            typing.cast(
                "typing.Any", shard
            ).promote_boundary_page = gate_promotion(shard)

        calls = []
        for feed_id in feeds:
            for source_order in range(2):
                calls.append(_call(feed_id, source_order))
        coverage = asyncio.create_task(
            lane.cover_page(
                calls=typing.cast("typing.Any", tuple(calls)),
                boundaries=typing.cast(
                    "typing.Any",
                    tuple(_boundary(feed_id, 10) for feed_id in feeds),
                ),
                candidate=candidate,
            )
        )
        try:
            await asyncio.wait_for(executor.entered.wait(), timeout=1)
            executor.release.set()
            await asyncio.wait_for(promotion_reached.wait(), timeout=1)
            coverage.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await coverage

            snapshot = await scheduler._snapshot()
            self.assertEqual(snapshot.held, 0)
            self.assertTrue(
                all(shard.queued_calls == 0 for shard in snapshot.shards)
            )
            self.assertTrue(
                all(shard.pending_boundaries == 0 for shard in snapshot.shards)
            )
            self.assertTrue(
                all(shard.flushing_boundaries == 0 for shard in snapshot.shards)
            )
            self.assertIsNone((await lane._snapshot()).page)
            self.assertIs(cursor.outstanding_candidate, candidate)

            await scheduler._wait_for_idle()
        finally:
            allow_promotion.set()
            executor.release.set()
            if not coverage.done():
                coverage.cancel()
                await asyncio.gather(coverage, return_exceptions=True)
            await scheduler.close()

    async def test_cancel_after_each_partial_promotion_rolls_back_page(
        self,
    ) -> None:
        for promotion_count in (1, 2):
            with self.subTest(promotion_count=promotion_count):
                await self._cancel_after_promotion(promotion_count)

    async def test_cancel_after_final_promotion_before_receipt_rolls_back(
        self,
    ) -> None:
        await self._cancel_after_promotion(3)

    async def test_receipt_winner_returns_sealed_receipt(self) -> None:
        committer = _ControlledCommitter()
        scheduler = feed_work_scheduler.FeedWorkScheduler(
            typing.cast("typing.Any", _ImmediateExecutor()),
            boundary_committer=typing.cast("typing.Any", committer),
            _limits=typing.cast("typing.Any", _limits()),
        )
        await scheduler.start()
        grant = _grant()
        lane = _open_lane(scheduler, grant)
        cursor = cursor_policy.LeaseCursor(grant, pos=None)
        candidate = cursor.prepare(_SOURCE_TIME)
        final_promotion = asyncio.Event()
        allow_receipt = asyncio.Event()
        promote = scheduler._promote_boundary_page

        async def gated_promote(
            exact_grant: ingestion_lease_store.LeaseGrant,
            page_sequence: int,
        ) -> None:
            await promote(exact_grant, page_sequence)
            final_promotion.set()
            await allow_receipt.wait()

        typing.cast(
            "typing.Any", scheduler
        )._promote_boundary_page = gated_promote
        coverage = asyncio.create_task(
            lane.cover_page(
                calls=(),
                boundaries=typing.cast(
                    "typing.Any",
                    (_boundary(uuid.UUID(int=1), 10),),
                ),
                candidate=candidate,
            )
        )
        try:
            await asyncio.wait_for(final_promotion.wait(), timeout=1)
            allow_receipt.set()
            receipt = await coverage
            coverage.cancel()

            self.assertEqual(
                cursor.accept(receipt.lease_settlement),
                _SOURCE_TIME,
            )
            self.assertIsNone((await lane._snapshot()).page)
            self.assertEqual((await scheduler._snapshot()).held, 0)
            committed = []
            for _grant_value, batch, _final in committer.calls:
                committed.extend(batch)
            self.assertEqual(
                [
                    (boundary.feed_id, boundary.target)
                    for boundary in typing.cast("typing.Any", committed)
                ],
                [
                    (
                        uuid.UUID(int=1),
                        _SOURCE_TIME + datetime.timedelta(seconds=10),
                    )
                ],
            )
        finally:
            allow_receipt.set()
            await scheduler.close()

    async def test_cross_page_coalesce_commit_is_shielded_on_cancel(
        self,
    ) -> None:
        executor = _GateExecutor()
        committer = _ControlledCommitter()
        committer.block_final_number = 2
        scheduler = feed_work_scheduler.FeedWorkScheduler(
            executor,
            boundary_committer=committer,
            _limits=_limits(),
        )
        await scheduler.start()
        grant = _grant()
        lane = _open_lane(scheduler, grant)
        feed_id = uuid.UUID(int=1)
        cursor = cursor_policy.LeaseCursor(grant, pos=None)
        first_coverage = asyncio.create_task(
            lane.cover_page(
                calls=(_call(feed_id, 0),),
                boundaries=(_boundary(feed_id, 10),),
                candidate=cursor.prepare(_SOURCE_TIME),
            )
        )
        await asyncio.wait_for(executor.entered.wait(), timeout=1)
        executor.release.set()
        first = await asyncio.wait_for(first_coverage, timeout=1)
        cursor.accept(first.lease_settlement)

        second_candidate = cursor.prepare(
            _SOURCE_TIME + datetime.timedelta(seconds=1)
        )
        second = asyncio.create_task(
            lane.cover_page(
                calls=(_call(feed_id, 0),),
                boundaries=(
                    _boundary(feed_id, 5),
                    _boundary(feed_id, 20),
                ),
                candidate=second_candidate,
            )
        )
        try:
            await committer.wait_for_calls(2)
            snapshot = await scheduler._shards[0].snapshot()
            self.assertEqual(snapshot.held, 1)
            self.assertEqual(snapshot.pending_boundaries, 0)
            self.assertEqual(snapshot.flushing_boundaries, 1)
            self.assertEqual(len(snapshot.boundaries), 1)
            flushing = snapshot.boundaries[0]
            self.assertEqual(
                flushing.target,
                _SOURCE_TIME + datetime.timedelta(seconds=20),
            )

            second.cancel()
            await asyncio.sleep(0)
            self.assertFalse(second.done())
            committer.release.set()
            with self.assertRaises(asyncio.CancelledError):
                await second
            self.assertEqual((await scheduler._snapshot()).held, 0)
            self.assertIs(cursor.outstanding_candidate, second_candidate)
        finally:
            committer.release.set()
            executor.release.set()
            await scheduler._wait_for_idle()
            await scheduler.close()

    async def test_flushing_target_is_immutable_across_sequential_pages(
        self,
    ) -> None:
        executor = _GateExecutor()
        committer = _ControlledCommitter()
        committer.block_nonempty = True
        scheduler = feed_work_scheduler.FeedWorkScheduler(
            executor,
            boundary_committer=committer,
            _limits=_limits(),
        )
        await scheduler.start()
        grant = _grant()
        lane = _open_lane(scheduler, grant)
        feed_id = uuid.UUID(int=1)
        cursor = cursor_policy.LeaseCursor(grant, pos=None)
        first_coverage = asyncio.create_task(
            lane.cover_page(
                calls=(_call(feed_id, 0),),
                boundaries=(_boundary(feed_id, 10),),
                candidate=cursor.prepare(_SOURCE_TIME),
            )
        )
        await asyncio.wait_for(executor.entered.wait(), timeout=1)
        executor.release.set()
        await committer.wait_for_calls(1)
        try:
            snapshot = await scheduler._shards[0].snapshot()
            self.assertEqual(snapshot.flushing_boundaries, 1)
            self.assertEqual(snapshot.pending_boundaries, 0)
            self.assertEqual(
                [boundary.target for boundary in snapshot.boundaries],
                [_SOURCE_TIME + datetime.timedelta(seconds=10)],
            )
            self.assertEqual(snapshot.active_calls, 0)

            committer.release.set()
            first = await asyncio.wait_for(first_coverage, timeout=1)
            cursor.accept(first.lease_settlement)
            second = await asyncio.wait_for(
                lane.cover_page(
                    calls=(),
                    boundaries=(_boundary(feed_id, 20),),
                    candidate=cursor.prepare(
                        _SOURCE_TIME + datetime.timedelta(seconds=1)
                    ),
                ),
                timeout=1,
            )
            cursor.accept(second.lease_settlement)
            committed_targets = [
                boundary.target
                for _exact_grant, batch, _final in committer.calls
                for boundary in batch
            ]
            self.assertEqual(
                committed_targets,
                [
                    _SOURCE_TIME + datetime.timedelta(seconds=10),
                    _SOURCE_TIME + datetime.timedelta(seconds=20),
                ],
            )
        finally:
            committer.release.set()
            await asyncio.gather(first_coverage, return_exceptions=True)
            await scheduler._wait_for_idle()
            await scheduler.close()


class TestBoundaryOutcomes(unittest.IsolatedAsyncioTestCase):
    async def test_fence_rejection_closes_only_old_exact_grant(self) -> None:
        committer = _ControlledCommitter()
        scheduler = feed_work_scheduler.FeedWorkScheduler(
            _ImmediateExecutor(),
            boundary_committer=committer,
            _limits=_limits(),
        )
        await scheduler.start()
        old = _grant(fencing_token=1)
        successor = _grant(fencing_token=2)
        old_lane = _open_lane(scheduler, old)
        successor_lane = _open_lane(scheduler, successor)
        committer.rejected_grants.add(old)
        old_cursor = cursor_policy.LeaseCursor(old, pos=None)
        old_candidate = old_cursor.prepare(_SOURCE_TIME)

        with self.assertRaises(RuntimeError) as raised:
            await old_lane.cover_page(
                calls=(),
                boundaries=(_boundary(uuid.UUID(int=1), 1),),
                candidate=old_candidate,
            )

        self.assertNotIsInstance(
            raised.exception,
            feed_work_scheduler.SchedulerIntegrityError,
        )
        self.assertIs(old_cursor.outstanding_candidate, old_candidate)
        self.assertEqual(
            await asyncio.wait_for(
                old_lane.close(
                    feed_work_scheduler.LaneCloseReason.AUTHORITY_LOSS
                ),
                timeout=1,
            ),
            feed_work_scheduler.LaneClosed(
                old,
                feed_work_scheduler.LaneCloseReason.AUTHORITY_LOSS,
            ),
        )
        self.assertFalse((await scheduler._snapshot()).fatal)

        successor_cursor = cursor_policy.LeaseCursor(successor, pos=None)
        receipt = await successor_lane.cover_page(
            calls=(),
            boundaries=(_boundary(uuid.UUID(int=1), 2, grant=successor),),
            candidate=successor_cursor.prepare(_SOURCE_TIME),
        )
        self.assertEqual(
            successor_cursor.accept(receipt.lease_settlement),
            _SOURCE_TIME,
        )
        await asyncio.wait_for(scheduler._wait_for_idle(), timeout=1)
        self.assertIn(successor, (call[0] for call in committer.calls))
        self.assertFalse((await scheduler._snapshot()).fatal)
        await scheduler.close()

    async def test_committer_runs_unlocked_and_member_rejection_is_local(
        self,
    ) -> None:
        committer = _ControlledCommitter()
        scheduler_types = _types()
        removed_feed = uuid.UUID(int=1)
        sibling_feed = uuid.UUID(int=2)
        committer.dispositions[removed_feed] = (
            scheduler_types.BoundaryDisposition.MEMBER_REJECTED
        )
        scheduler = feed_work_scheduler.FeedWorkScheduler(
            _ImmediateExecutor(),
            boundary_committer=committer,
            _limits=_limits(),
        )
        committer.inspect = lambda: self.assertFalse(
            any(shard._lock.locked() for shard in scheduler._shards)
        )
        await scheduler.start()
        grant = _grant()
        lane = _open_lane(scheduler, grant)
        cursor = cursor_policy.LeaseCursor(grant, pos=None)
        try:
            receipt = await lane.cover_page(
                calls=(),
                boundaries=(
                    _boundary(removed_feed, 10),
                    _boundary(sibling_feed, 10),
                ),
                candidate=cursor.prepare(_SOURCE_TIME),
            )
            cursor.accept(receipt.lease_settlement)
            self.assertEqual((await scheduler._snapshot()).held, 0)

            later = await lane.cover_page(
                calls=(),
                boundaries=(
                    _boundary(removed_feed, 20),
                    _boundary(sibling_feed, 20),
                ),
                candidate=cursor.prepare(
                    _SOURCE_TIME + datetime.timedelta(seconds=1)
                ),
            )
            cursor.accept(later.lease_settlement)
            committed_feeds = set()
            for _grant_value, batch, _final in committer.calls:
                for boundary in batch:
                    committed_feeds.add(
                        typing.cast("typing.Any", boundary).feed_id
                    )
            self.assertIn(sibling_feed, committed_feeds)
            self.assertFalse((await scheduler._snapshot()).fatal)
        finally:
            await scheduler.close()

    async def test_malformed_correlation_fails_before_terminalization(
        self,
    ) -> None:
        committer = _ControlledCommitter()
        scheduler_types = _types()
        offered = _boundary(uuid.UUID(int=1), 10)
        crossed = _boundary(uuid.UUID(int=2), 10)
        committer.override_result = scheduler_types.BoundaryBatchCommitted(
            (
                scheduler_types.BoundaryResult(
                    crossed,
                    scheduler_types.BoundaryDisposition.COMMITTED,
                ),
            )
        )
        scheduler = feed_work_scheduler.FeedWorkScheduler(
            _ImmediateExecutor(),
            boundary_committer=committer,
            _limits=_limits(),
        )
        await scheduler.start()
        grant = _grant()
        lane = _open_lane(scheduler, grant)
        candidate = cursor_policy.LeaseCursor(grant, pos=None).prepare(
            _SOURCE_TIME
        )

        with self.assertRaises(feed_work_scheduler.SchedulerIntegrityError):
            await lane.cover_page(
                calls=(),
                boundaries=(offered,),
                candidate=candidate,
            )

        snapshot = await scheduler._shards[0].snapshot()
        self.assertTrue(snapshot.fatal)
        self.assertEqual(snapshot.held, 1)
        self.assertEqual(snapshot.flushing_boundaries, 1)
        self.assertIsInstance(
            await scheduler.close(),
            feed_work_scheduler.Undrained,
        )


class TestNoProgressPages(unittest.IsolatedAsyncioTestCase):
    async def test_no_progress_calls_are_bounded_with_one_empty_final_flush(
        self,
    ) -> None:
        committer = _ControlledCommitter()
        executor = _GateExecutor()
        scheduler = feed_work_scheduler.FeedWorkScheduler(
            executor,
            boundary_committer=committer,
            _limits=_limits(),
        )
        await scheduler.start()
        grant = _grant()
        lane = _open_lane(scheduler, grant)
        cursor = cursor_policy.LeaseCursor(grant, pos=_SOURCE_TIME)
        candidate = cursor.prepare_no_progress()
        coverage = asyncio.create_task(
            lane.cover_page(
                calls=(_call(uuid.UUID(int=1), 0),),
                boundaries=(),
                candidate=candidate,
            )
        )
        try:
            await asyncio.wait_for(executor.entered.wait(), timeout=1)
            executor.release.set()
            settlement = await asyncio.wait_for(coverage, timeout=1)

            self.assertNotIsInstance(
                settlement.lease_settlement,
                cursor_policy._CoveredPage,
            )
            self.assertEqual(committer.calls, [(grant, (), True)])
            self.assertEqual(committer.final_calls, 1)
            self.assertEqual((await scheduler._snapshot()).held, 0)
            original_pos = cursor.pos
            self.assertIsNone(
                cursor.accept_no_progress(settlement.lease_settlement)
            )
            self.assertIs(cursor.pos, original_pos)
            self.assertEqual(cursor.next_page_sequence, 1)
        finally:
            executor.release.set()
            await scheduler._wait_for_idle()
            await scheduler.close()

    async def test_no_progress_rejects_boundary_before_admitting_calls(
        self,
    ) -> None:
        committer = _ControlledCommitter()
        executor = _GateExecutor()
        scheduler = feed_work_scheduler.FeedWorkScheduler(
            executor,
            boundary_committer=committer,
            _limits=_limits(),
        )
        await scheduler.start()
        grant = _grant()
        lane = _open_lane(scheduler, grant)
        cursor = cursor_policy.LeaseCursor(grant, pos=_SOURCE_TIME)
        candidate = cursor.prepare_no_progress()
        boundaries = _TracingBoundaries((_boundary(uuid.UUID(int=1), 1),))

        try:
            with self.assertRaisesRegex(
                cursor_policy.CursorIntegrityError,
                "no-progress.*boundaries",
            ):
                await lane.cover_page(
                    calls=(_call(uuid.UUID(int=2), 0),),
                    boundaries=boundaries,
                    candidate=candidate,
                )

            self.assertEqual(boundaries.pulled, [uuid.UUID(int=1)])
            self.assertEqual(executor.calls, 0)
            self.assertEqual(committer.calls, [])
            self.assertIs(cursor.outstanding_candidate, candidate)
            self.assertEqual((await scheduler._snapshot()).held, 0)
        finally:
            await scheduler.close()

    async def test_empty_final_batch_retryable_aborts_then_explicit_retry(
        self,
    ) -> None:
        scheduler_types = _types()
        committer = _ControlledCommitter()
        committer.scripted_batch_results.append(
            scheduler_types.BoundaryBatchRetryable()
        )
        scheduler = feed_work_scheduler.FeedWorkScheduler(
            _ImmediateExecutor(),
            boundary_committer=committer,
            _limits=_limits(),
        )
        await scheduler.start()
        grant = _grant()
        lane = _open_lane(scheduler, grant)
        cursor = cursor_policy.LeaseCursor(grant, pos=_SOURCE_TIME)
        candidate = cursor.prepare_no_progress()

        try:
            with self.assertRaisesRegex(RuntimeError, "retryable"):
                await lane.cover_page(
                    calls=(),
                    boundaries=(),
                    candidate=candidate,
                )

            self.assertIs(cursor.outstanding_candidate, candidate)
            self.assertEqual((await lane._snapshot()).next_page_sequence, 0)
            self.assertEqual(committer.calls, [(grant, (), True)])

            settlement = await lane.cover_page(
                calls=(),
                boundaries=(),
                candidate=candidate,
            )
            cursor.accept_no_progress(settlement.lease_settlement)

            self.assertEqual(committer.calls, [(grant, (), True)] * 2)
            self.assertEqual(committer.final_calls, 2)
            self.assertEqual(cursor.next_page_sequence, 1)
            self.assertEqual(cursor.pos, _SOURCE_TIME)
        finally:
            await scheduler.close()

    async def test_no_progress_cancel_during_admission_returns_no_settlement(
        self,
    ) -> None:
        committer = _ControlledCommitter()
        executor = _GateExecutor()
        scheduler = feed_work_scheduler.FeedWorkScheduler(
            executor,
            boundary_committer=committer,
            _limits=_limits(
                capacity=2,
                workers=1,
                high_water=1,
                resume_at=0,
            ),
        )
        await scheduler.start()
        grant = _grant()
        lane = _open_lane(scheduler, grant)
        cursor = cursor_policy.LeaseCursor(grant, pos=_SOURCE_TIME)
        candidate = cursor.prepare_no_progress()
        coverage = asyncio.create_task(
            lane.cover_page(
                calls=(
                    _call(uuid.UUID(int=1), 0),
                    _call(uuid.UUID(int=2), 1),
                ),
                boundaries=(),
                candidate=candidate,
            )
        )
        try:
            await scheduler._shards[0].wait_for_capacity_waiters(1)
            coverage.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await coverage

            self.assertIs(cursor.outstanding_candidate, candidate)
            self.assertEqual(committer.final_calls, 0)
            lane_snapshot = await lane._snapshot()
            self.assertEqual(lane_snapshot.next_page_sequence, 0)
            self.assertIsNone(lane_snapshot.page)
        finally:
            executor.release.set()
            await scheduler._wait_for_idle()
            await scheduler.close()

    async def test_no_progress_cancel_during_final_flush_waits_for_evidence(
        self,
    ) -> None:
        committer = _ControlledCommitter()
        committer.block_final_number = 1
        clock = _ControlledClock()
        scheduler = feed_work_scheduler.FeedWorkScheduler(
            _ImmediateExecutor(),
            boundary_committer=committer,
            _limits=_limits(),
            _monotonic=clock,
        )
        await scheduler.start()
        grant = _grant()
        lane = _open_lane(scheduler, grant)
        cursor = cursor_policy.LeaseCursor(grant, pos=_SOURCE_TIME)
        candidate = cursor.prepare_no_progress()
        observed: list[feed_work_scheduler.SchedulerPageEvidence] = []
        coverage = asyncio.create_task(
            lane.cover_page(
                calls=(),
                boundaries=(),
                candidate=candidate,
                evidence_observer=observed.append,
            )
        )
        try:
            await committer.wait_for_calls(1)
            coverage.cancel()
            for _unused in range(5):
                await asyncio.sleep(0)
            self.assertFalse(coverage.done())
            clock.advance(2.0)
            committer.release.set()
            with self.assertRaises(asyncio.CancelledError):
                await coverage

            self.assertIs(cursor.outstanding_candidate, candidate)
            self.assertEqual(committer.calls, [(grant, (), True)])
            self.assertEqual((await lane._snapshot()).next_page_sequence, 0)
            self.assertEqual(len(observed), 1)
            self.assertEqual(observed[0].final_flush_attempt_count, 1)
            self.assertEqual(observed[0].total_flush_latency_seconds, 2.0)
            self.assertEqual(observed[0].maximum_flush_latency_seconds, 2.0)
        finally:
            committer.release.set()
            await lane.close()
            await scheduler.close()

    async def test_no_progress_cancel_before_seal_settles_owned_transition(
        self,
    ) -> None:
        committer = _ControlledCommitter()
        scheduler = feed_work_scheduler.FeedWorkScheduler(
            _ImmediateExecutor(),
            boundary_committer=committer,
            _limits=_limits(),
        )
        await scheduler.start()
        grant = _grant()
        lane = _open_lane(scheduler, grant)
        cursor = cursor_policy.LeaseCursor(grant, pos=_SOURCE_TIME)
        candidate = cursor.prepare_no_progress()
        seal_entered = asyncio.Event()
        allow_seal = asyncio.Event()
        cover = lane._cover

        async def gated_cover(
            offered: cursor_policy.PageCandidate,
            result: object,
        ) -> cursor_policy.PageSettlement:
            seal_entered.set()
            await allow_seal.wait()
            return await cover(offered, result)

        lane._cover = gated_cover
        coverage = asyncio.create_task(
            lane.cover_page(
                calls=(),
                boundaries=(),
                candidate=candidate,
            )
        )
        try:
            await asyncio.wait_for(seal_entered.wait(), timeout=1)
            coverage.cancel()
            allow_seal.set()
            settled = await asyncio.wait_for(coverage, timeout=1)

            cursor.accept_no_progress(settled.lease_settlement)
            self.assertIsNone(cursor.outstanding_candidate)
            self.assertEqual(committer.calls, [(grant, (), True)])
            lane_snapshot = await lane._snapshot()
            self.assertEqual(lane_snapshot.next_page_sequence, 1)
            self.assertIsNone(lane_snapshot.page)
        finally:
            allow_seal.set()
            await scheduler.close()

    async def test_equal_position_remains_progress_with_quiet_boundary(
        self,
    ) -> None:
        committer = _ControlledCommitter()
        scheduler = feed_work_scheduler.FeedWorkScheduler(
            _ImmediateExecutor(),
            boundary_committer=committer,
            _limits=_limits(),
        )
        await scheduler.start()
        grant = _grant()
        lane = _open_lane(scheduler, grant)
        cursor = cursor_policy.LeaseCursor(grant, pos=_SOURCE_TIME)

        try:
            receipt = await lane.cover_page(
                calls=(),
                boundaries=(_boundary(uuid.UUID(int=1), 0),),
                candidate=cursor.prepare(_SOURCE_TIME),
            )

            self.assertIsInstance(
                receipt.lease_settlement,
                cursor_policy._CoveredPage,
            )
            self.assertEqual(
                cursor.accept(receipt.lease_settlement),
                _SOURCE_TIME,
            )
            self.assertEqual(committer.final_calls, 1)
            committed = tuple(
                boundary
                for _exact_grant, boundaries, _final in committer.calls
                for boundary in boundaries
            )
            self.assertEqual(len(committed), 1)
            self.assertEqual(committed[0].target, _SOURCE_TIME)
        finally:
            await scheduler.close()


class TestBoundaryLiveness(unittest.IsolatedAsyncioTestCase):
    async def test_501_boundaries_flush_prefixes_then_one_final(self) -> None:
        committer = _ControlledCommitter()
        committer.block_nonempty = True
        scheduler = feed_work_scheduler.FeedWorkScheduler(
            _ImmediateExecutor(),
            boundary_committer=committer,
            _limits=_limits(
                shard_count=8,
                capacity=500,
                workers=4,
                high_water=400,
                resume_at=299,
            ),
            _boundary_batch_size=100,
        )
        await scheduler.start()
        grant = _grant()
        lane = _open_lane(scheduler, grant)
        relief_requested = asyncio.Event()
        request_relief = lane._boundary_coordinator.request_relief

        async def observe_relief() -> object:
            relief_requested.set()
            return await request_relief()

        lane._boundary_coordinator.request_relief = observe_relief
        cursor = cursor_policy.LeaseCursor(grant, pos=None)
        feed_ids = tuple(uuid.UUID(int=(index + 1) * 8) for index in range(501))
        boundaries = _TracingBoundaries(
            _boundary(feed_id, index + 1)
            for index, feed_id in enumerate(feed_ids)
        )
        coverage = asyncio.create_task(
            lane.cover_page(
                calls=(),
                boundaries=boundaries,
                candidate=cursor.prepare(_SOURCE_TIME),
            )
        )
        try:
            await committer.wait_for_calls(1)
            await asyncio.wait_for(relief_requested.wait(), timeout=1)
            self.assertEqual(
                lane._boundary_coordinator.requested_generation,
                1,
            )
            snapshot = await scheduler._shards[0].snapshot()
            self.assertEqual(snapshot.held, 400)
            self.assertEqual(boundaries.pulled, list(feed_ids))
            self.assertFalse(coverage.done())

            committer.release.set()
            receipt = await asyncio.wait_for(coverage, timeout=2)
            self.assertEqual(
                cursor.accept(receipt.lease_settlement),
                _SOURCE_TIME,
            )
            self.assertEqual(boundaries.pulled, list(feed_ids))
            self.assertEqual(committer.final_calls, 1)
            self.assertGreater(
                sum(
                    not final for _grant_value, _batch, final in committer.calls
                ),
                1,
            )
            self.assertTrue(
                all(
                    len(batch) <= 100
                    for _grant_value, batch, _final in committer.calls
                )
            )
            await asyncio.wait_for(scheduler._wait_for_idle(), timeout=1)
            self.assertEqual((await scheduler._snapshot()).held, 0)
        finally:
            committer.release.set()
            if not coverage.done():
                coverage.cancel()
                await asyncio.gather(coverage, return_exceptions=True)
            await lane.close()
            await scheduler.close()

    async def test_empty_page_still_awaits_final_logical_commit(self) -> None:
        committer = _ControlledCommitter()
        scheduler = feed_work_scheduler.FeedWorkScheduler(
            _ImmediateExecutor(),
            boundary_committer=committer,
            _limits=_limits(),
        )
        await scheduler.start()
        grant = _grant()
        lane = _open_lane(scheduler, grant)
        cursor = cursor_policy.LeaseCursor(grant, pos=None)

        receipt = await lane.cover_page(
            calls=(),
            boundaries=(),
            candidate=cursor.prepare(_SOURCE_TIME),
        )

        self.assertEqual(
            cursor.accept(receipt.lease_settlement),
            _SOURCE_TIME,
        )
        self.assertEqual(committer.calls, [(grant, (), True)])
        await lane.close()
        await scheduler.close()

    async def test_retryable_pressure_attempt_aborts_then_replay_succeeds(
        self,
    ) -> None:
        scheduler_types = _types()
        committer = _ControlledCommitter()
        committer.block_nonempty = True
        committer.scripted_dispositions.append(
            scheduler_types.BoundaryDisposition.RETRYABLE
        )
        scheduler = feed_work_scheduler.FeedWorkScheduler(
            _ImmediateExecutor(),
            boundary_committer=committer,
            _limits=_limits(
                capacity=500,
                workers=4,
                high_water=400,
                resume_at=299,
            ),
        )
        await scheduler.start()
        grant = _grant()
        lane = _open_lane(scheduler, grant)
        relief_requested = asyncio.Event()
        request_relief = lane._boundary_coordinator.request_relief

        async def observe_relief() -> object:
            relief_requested.set()
            return await request_relief()

        lane._boundary_coordinator.request_relief = observe_relief
        cursor = cursor_policy.LeaseCursor(grant, pos=None)
        candidate = cursor.prepare(_SOURCE_TIME)
        feed_ids = tuple(uuid.UUID(int=index + 1) for index in range(401))
        coverage = asyncio.create_task(
            lane.cover_page(
                calls=(),
                boundaries=(
                    _boundary(feed_id, index + 1)
                    for index, feed_id in enumerate(feed_ids)
                ),
                candidate=candidate,
            )
        )
        try:
            await committer.wait_for_calls(1)
            await asyncio.wait_for(relief_requested.wait(), timeout=1)
            self.assertEqual(
                lane._boundary_coordinator.requested_generation,
                1,
            )
            self.assertEqual((await scheduler._snapshot()).held, 400)
            committer.release.set()

            with self.assertRaisesRegex(RuntimeError, "retryable"):
                await asyncio.wait_for(coverage, timeout=1)
            self.assertIs(cursor.outstanding_candidate, candidate)
            self.assertEqual((await scheduler._snapshot()).held, 0)
            nonempty_attempts = [
                batch
                for _grant_value, batch, _final in committer.calls
                if batch
            ]
            self.assertEqual(len(nonempty_attempts), 1)

            replay = await asyncio.wait_for(
                lane.cover_page(
                    calls=(),
                    boundaries=(
                        _boundary(feed_id, index + 1)
                        for index, feed_id in enumerate(feed_ids)
                    ),
                    candidate=candidate,
                ),
                timeout=2,
            )
            self.assertEqual(
                cursor.accept(replay.lease_settlement),
                _SOURCE_TIME,
            )
            await asyncio.wait_for(scheduler._wait_for_idle(), timeout=1)
            self.assertEqual((await scheduler._snapshot()).held, 0)
        finally:
            committer.release.set()
            if not coverage.done():
                coverage.cancel()
                await asyncio.gather(coverage, return_exceptions=True)
            await lane.close()
            await scheduler.close()

    async def test_flusher_cancel_after_commit_start_returns_no_receipt(
        self,
    ) -> None:
        committer = _ControlledCommitter()
        committer.block_nonempty = True
        scheduler = feed_work_scheduler.FeedWorkScheduler(
            _ImmediateExecutor(),
            boundary_committer=committer,
            _limits=_limits(),
        )
        await scheduler.start()
        grant = _grant()
        lane = _open_lane(scheduler, grant)
        cursor = cursor_policy.LeaseCursor(grant, pos=None)
        candidate = cursor.prepare(_SOURCE_TIME)
        coverage = asyncio.create_task(
            lane.cover_page(
                calls=(),
                boundaries=(_boundary(uuid.UUID(int=1), 1),),
                candidate=candidate,
            )
        )
        await committer.wait_for_calls(1)
        self.assertEqual((await scheduler._snapshot()).held, 1)

        lane._boundary_coordinator.task.cancel()
        committer.release.set()

        with self.assertRaises(feed_work_scheduler.SchedulerIntegrityError):
            await asyncio.wait_for(coverage, timeout=1)
        self.assertIs(cursor.outstanding_candidate, candidate)
        snapshot = await scheduler._snapshot()
        self.assertTrue(snapshot.fatal)
        self.assertEqual(snapshot.held, 0)
        self.assertIsInstance(
            await scheduler.close(),
            feed_work_scheduler.Undrained,
        )

    async def test_never_settling_commit_suppresses_incomplete_evidence(
        self,
    ) -> None:
        committer = _ControlledCommitter()
        committer.block_nonempty = True
        observed: list[feed_work_scheduler.SchedulerPageEvidence] = []
        scheduler = feed_work_scheduler.FeedWorkScheduler(
            _ImmediateExecutor(),
            boundary_committer=committer,
            _limits=_limits(),
        )
        await scheduler.start()
        grant = _grant()
        lane = _open_lane(scheduler, grant)
        cursor = cursor_policy.LeaseCursor(grant, pos=None)
        candidate = cursor.prepare(_SOURCE_TIME)
        coverage = asyncio.create_task(
            lane.cover_page(
                calls=(),
                boundaries=(_boundary(uuid.UUID(int=1), 1),),
                candidate=candidate,
                evidence_observer=observed.append,
            )
        )
        await committer.wait_for_calls(1)
        coordinator_task = lane._boundary_coordinator.task
        close = asyncio.create_task(
            lane.close(feed_work_scheduler.LaneCloseReason.AUTHORITY_LOSS)
        )
        await asyncio.wait_for(lane._closing_event.wait(), timeout=1)

        close.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await close
        with self.assertRaises(feed_work_scheduler.SchedulerIntegrityError):
            await asyncio.wait_for(coverage, timeout=1)

        result = await asyncio.wait_for(
            lane.close(feed_work_scheduler.LaneCloseReason.AUTHORITY_LOSS),
            timeout=1,
        )
        self.assertIsInstance(result, feed_work_scheduler.Undrained)
        snapshot = await scheduler._snapshot()
        self.assertTrue(snapshot.fatal)
        self.assertEqual(snapshot.held, 1)
        self.assertEqual(observed, [])
        self.assertIs(lane._boundary_coordinator.task, coordinator_task)
        self.assertFalse(coordinator_task.done())
        self.assertIsInstance(
            await scheduler.close(),
            feed_work_scheduler.Undrained,
        )

        committer.release.set()
        await asyncio.wait_for(coordinator_task, timeout=1)
        self.assertEqual((await scheduler._snapshot()).held, 0)
        self.assertEqual(observed, [])


if __name__ == "__main__":
    unittest.main()
