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


def _call(feed_id: uuid.UUID, source_order: int) -> object:
    return feed_work_scheduler.CallSubmission(
        feed_id=feed_id,
        source_timestamp=(
            _SOURCE_TIME + datetime.timedelta(seconds=source_order)
        ),
        payload={"source_order": source_order},
    )


def _boundary(feed_id: uuid.UUID, seconds: int) -> object:
    return _types().BoundaryWork(
        feed_id=feed_id,
        target=_SOURCE_TIME + datetime.timedelta(seconds=seconds),
    )


class _ImmediateExecutor:
    async def execute(self, record: object) -> object:
        del record
        return feed_work_scheduler.CallCompleted()


class _GateExecutor:
    def __init__(self) -> None:
        self.entered = asyncio.Event()
        self.release = asyncio.Event()
        self.calls = 0

    async def execute(self, record: object) -> object:
        del record
        self.calls += 1
        self.entered.set()
        await self.release.wait()
        return feed_work_scheduler.CallCompleted()


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
        lane = scheduler.open_lane(_grant())
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
        lane = scheduler.open_lane(grant)
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
            self.assertEqual(boundaries.pulled, [])
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
        lane = scheduler.open_lane(grant)
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
            await asyncio.wait_for(promotion_reached.wait(), timeout=1)
            coverage.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await coverage

            snapshot = await scheduler._snapshot()
            self.assertEqual(snapshot.held, shard_count)
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

            executor.release.set()
            await scheduler._wait_for_idle()
            self.assertTrue(
                all(
                    not batch for _grant_value, batch, _final in committer.calls
                )
            )
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
        lane = scheduler.open_lane(grant)
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

            self.assertEqual(cursor.accept(receipt), _SOURCE_TIME)
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

    async def test_cross_page_pending_coalesce_rolls_back_on_abort(
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
        lane = scheduler.open_lane(grant)
        feed_id = uuid.UUID(int=1)
        cursor = cursor_policy.LeaseCursor(grant, pos=None)
        first = await asyncio.wait_for(
            lane.cover_page(
                calls=(_call(feed_id, 0),),
                boundaries=(_boundary(feed_id, 10),),
                candidate=cursor.prepare(_SOURCE_TIME),
            ),
            timeout=1,
        )
        cursor.accept(first)

        second = asyncio.create_task(
            lane.cover_page(
                calls=(_call(feed_id, 0),),
                boundaries=(
                    _boundary(feed_id, 5),
                    _boundary(feed_id, 20),
                ),
                candidate=cursor.prepare(
                    _SOURCE_TIME + datetime.timedelta(seconds=1)
                ),
            )
        )
        try:
            await committer.wait_for_calls(2)
            snapshot = await scheduler._shards[0].snapshot()
            self.assertEqual(snapshot.held, 3)
            self.assertEqual(snapshot.pending_boundaries, 1)
            self.assertEqual(len(snapshot.boundaries), 1)
            pending = snapshot.boundaries[0]
            self.assertEqual(
                pending.stable_target,
                _SOURCE_TIME + datetime.timedelta(seconds=10),
            )
            self.assertEqual(
                pending.target,
                _SOURCE_TIME + datetime.timedelta(seconds=20),
            )
            self.assertEqual(pending.provisional_page_sequence, 1)

            second.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await second
            rolled_back = (await scheduler._shards[0].snapshot()).boundaries[0]
            self.assertEqual(
                rolled_back.target,
                _SOURCE_TIME + datetime.timedelta(seconds=10),
            )
            self.assertIsNone(rolled_back.provisional_page_sequence)
            self.assertEqual((await scheduler._snapshot()).held, 2)
        finally:
            committer.release.set()
            executor.release.set()
            await scheduler._wait_for_idle()
            await scheduler.close()

    async def test_flushing_target_is_immutable_and_later_target_is_pending(
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
        lane = scheduler.open_lane(grant)
        feed_id = uuid.UUID(int=1)
        cursor = cursor_policy.LeaseCursor(grant, pos=None)
        first = await asyncio.wait_for(
            lane.cover_page(
                calls=(_call(feed_id, 0),),
                boundaries=(_boundary(feed_id, 10),),
                candidate=cursor.prepare(_SOURCE_TIME),
            ),
            timeout=1,
        )
        cursor.accept(first)
        self.assertEqual(committer.calls, [(grant, (), True)])
        executor.release.set()
        await committer.wait_for_calls(2)

        second = asyncio.create_task(
            lane.cover_page(
                calls=(),
                boundaries=(_boundary(feed_id, 20),),
                candidate=cursor.prepare(
                    _SOURCE_TIME + datetime.timedelta(seconds=1)
                ),
            )
        )
        try:
            await scheduler._shards[0].wait_for_held(2)
            snapshot = await scheduler._shards[0].snapshot()
            self.assertEqual(snapshot.flushing_boundaries, 1)
            self.assertEqual(snapshot.pending_boundaries, 1)
            self.assertEqual(
                [boundary.target for boundary in snapshot.boundaries],
                [
                    _SOURCE_TIME + datetime.timedelta(seconds=10),
                    _SOURCE_TIME + datetime.timedelta(seconds=20),
                ],
            )
            self.assertEqual(snapshot.active_calls, 0)
        finally:
            committer.release.set()
            await asyncio.wait_for(second, timeout=1)
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
        old_lane = scheduler.open_lane(old)
        successor_lane = scheduler.open_lane(successor)
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
            boundaries=(_boundary(uuid.UUID(int=1), 2),),
            candidate=successor_cursor.prepare(_SOURCE_TIME),
        )
        self.assertEqual(successor_cursor.accept(receipt), _SOURCE_TIME)
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
        lane = scheduler.open_lane(grant)
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
            cursor.accept(receipt)
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
            cursor.accept(later)
            committed_feeds = {
                boundary.feed_id
                for _grant_value, batch, _final in committer.calls
                for boundary in batch
            }
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
        lane = scheduler.open_lane(grant)
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
        lane = scheduler.open_lane(grant)
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
            self.assertEqual(boundaries.pulled, list(feed_ids[:401]))
            self.assertFalse(coverage.done())

            committer.release.set()
            receipt = await asyncio.wait_for(coverage, timeout=2)
            self.assertEqual(cursor.accept(receipt), _SOURCE_TIME)
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
        lane = scheduler.open_lane(grant)
        cursor = cursor_policy.LeaseCursor(grant, pos=None)

        receipt = await lane.cover_page(
            calls=(),
            boundaries=(),
            candidate=cursor.prepare(_SOURCE_TIME),
        )

        self.assertEqual(cursor.accept(receipt), _SOURCE_TIME)
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
        lane = scheduler.open_lane(grant)
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
            self.assertEqual(cursor.accept(replay), _SOURCE_TIME)
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
        lane = scheduler.open_lane(grant)
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

    async def test_never_settling_committed_close_is_undrained(self) -> None:
        committer = _ControlledCommitter()
        committer.block_nonempty = True
        scheduler = feed_work_scheduler.FeedWorkScheduler(
            _ImmediateExecutor(),
            boundary_committer=committer,
            _limits=_limits(),
        )
        await scheduler.start()
        grant = _grant()
        lane = scheduler.open_lane(grant)
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
        self.assertIs(lane._boundary_coordinator.task, coordinator_task)
        self.assertFalse(coordinator_task.done())
        self.assertIsInstance(
            await scheduler.close(),
            feed_work_scheduler.Undrained,
        )

        committer.release.set()
        await asyncio.wait_for(coordinator_task, timeout=1)
        self.assertEqual((await scheduler._snapshot()).held, 0)


if __name__ == "__main__":
    unittest.main()
