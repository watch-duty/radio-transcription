"""Event-gated boundary ordering and exact-committer contract tests."""

from __future__ import annotations

import asyncio
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
    capacity: int = 8,
    workers: int = 1,
    high_water: int = 8,
    resume_at: int = 4,
) -> object:
    return _types()._SchedulerLimits(
        shard_count=1,
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
        return scheduler_types.BoundaryBatchCommitted(
            tuple(
                scheduler_types.BoundaryResult(
                    boundary,
                    self.dispositions.get(
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
        first = await lane.cover_page(
            calls=(_call(feed_id, 0),),
            boundaries=(_boundary(feed_id, 10),),
            candidate=cursor.prepare(_SOURCE_TIME),
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
        first = await lane.cover_page(
            calls=(_call(feed_id, 0),),
            boundaries=(_boundary(feed_id, 10),),
            candidate=cursor.prepare(_SOURCE_TIME),
        )
        cursor.accept(first)
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


if __name__ == "__main__":
    unittest.main()
