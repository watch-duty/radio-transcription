"""Controlled public-interface tests for bounded Feed work scheduling."""

from __future__ import annotations

import asyncio
import dataclasses
import datetime
import importlib
import typing
import unittest
import uuid

from backend.pipeline.ingestion import feed_work_scheduler
from backend.pipeline.ingestion.collectors.bcfy_calls import cursor_policy
from backend.pipeline.storage import feed_store, ingestion_lease_store

_OWNER_ID = uuid.UUID("11111111-2222-3333-4444-555555555555")
_OTHER_OWNER_ID = uuid.UUID("22222222-3333-4444-5555-666666666666")
_SOURCE_TIME = datetime.datetime(2026, 7, 12, 12, 0, tzinfo=datetime.UTC)


def _scheduler_types() -> typing.Any:
    return importlib.import_module(
        "backend.pipeline.ingestion.feed_work_scheduler._types"
    )


def _grant(
    *,
    lease_key: str = "150",
    owner_worker_id: uuid.UUID = _OWNER_ID,
    fencing_token: int = 1,
) -> ingestion_lease_store.LeaseGrant:
    return ingestion_lease_store.LeaseGrant(
        source_type=feed_store.SourceType.BCFY_CALLS,
        lease_key=lease_key,
        owner_worker_id=owner_worker_id,
        fencing_token=fencing_token,
    )


def _submission(
    feed_id: uuid.UUID,
    source_order: int,
) -> object:
    return feed_work_scheduler.CallSubmission(
        feed_id=feed_id,
        source_timestamp=(
            _SOURCE_TIME + datetime.timedelta(seconds=source_order)
        ),
        payload={"source_order": source_order},
    )


class _ImmediateExecutor:
    def __init__(self) -> None:
        self.sequences: list[int] = []

    async def execute(self, record: object) -> object:
        self.sequences.append(record.local_sequence)
        return feed_work_scheduler.CallCompleted()


class _GateExecutor:
    """Event-gated executor with no timing or task-global assertions."""

    def __init__(self) -> None:
        self.started: list[int] = []
        self.changed = asyncio.Event()
        self._release: dict[int, asyncio.Event] = {}
        self._released = 0
        self._release_all = False

    async def execute(self, record: object) -> object:
        sequence = record.local_sequence
        event = self._release.setdefault(sequence, asyncio.Event())
        self.started.append(sequence)
        self.changed.set()
        if not self._release_all:
            await event.wait()
        return feed_work_scheduler.CallCompleted()

    async def wait_for_started(self, count: int) -> None:
        while len(self.started) < count:
            self.changed.clear()
            if len(self.started) >= count:
                return
            await asyncio.wait_for(self.changed.wait(), timeout=1)

    async def release_completions(self, count: int) -> None:
        target = self._released + count
        while self._released < target:
            await self.wait_for_started(self._released + 1)
            sequence = self.started[self._released]
            self._release[sequence].set()
            self._released += 1

    def release(self, sequence: int) -> None:
        self._release.setdefault(sequence, asyncio.Event()).set()

    def release_all(self) -> None:
        self._release_all = True
        for event in self._release.values():
            event.set()


class _DelayedCancellationExecutor:
    """Executor that exposes cancellation and an explicit settle winner."""

    def __init__(self, *, swallow: bool = False) -> None:
        self.swallow = swallow
        self.entered = asyncio.Event()
        self.cancellation_seen = asyncio.Event()
        self.settle = asyncio.Event()
        self.changed = asyncio.Event()
        self.entered_count = 0
        self.cancellation_count = 0

    async def execute(self, record: object) -> object:
        del record
        self.entered_count += 1
        self.entered.set()
        self.changed.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            self.cancellation_count += 1
            self.cancellation_seen.set()
            self.changed.set()
            if self.swallow:
                task = asyncio.current_task()
                if task is None:
                    message = "executor must run in a Task"
                    raise RuntimeError(message)
                task.uncancel()
            await self.settle.wait()
            if self.swallow:
                return feed_work_scheduler.CallCompleted()
            raise

    async def wait_for_counts(self, entered: int, cancelled: int) -> None:
        while (
            self.entered_count < entered or self.cancellation_count < cancelled
        ):
            self.changed.clear()
            if (
                self.entered_count >= entered
                and self.cancellation_count >= cancelled
            ):
                return
            await asyncio.wait_for(self.changed.wait(), timeout=1)


class _GatedOutcomeExecutor:
    """Returns one controlled closed outcome after page coverage."""

    def __init__(self, outcome: object) -> None:
        self.outcome = outcome
        self.entered = asyncio.Event()
        self.release = asyncio.Event()
        self.calls = 0

    async def execute(self, record: object) -> object:
        del record
        self.calls += 1
        self.entered.set()
        await self.release.wait()
        return self.outcome


class _CrossShardFailureExecutor:
    """Fails one shard while retaining controlled work on another."""

    def __init__(self, failing_feed: uuid.UUID) -> None:
        self.failing_feed = failing_feed
        self.failing_entered = asyncio.Event()
        self.healthy_entered = asyncio.Event()
        self.release_failure = asyncio.Event()
        self.release_healthy = asyncio.Event()

    async def execute(self, record: object) -> object:
        if record.feed_id == self.failing_feed:
            self.failing_entered.set()
            await self.release_failure.wait()
            message = "unexpected executor failure"
            raise RuntimeError(message)
        self.healthy_entered.set()
        await self.release_healthy.wait()
        return feed_work_scheduler.CallCompleted()


class _TracingCalls:
    """Single-pass source iterator with deterministic pull observation."""

    def __init__(self, values: typing.Iterable[object]) -> None:
        self._iterator = iter(values)
        self.pulled: list[int] = []
        self.changed = asyncio.Event()

    def __iter__(self) -> _TracingCalls:
        return self

    def __next__(self) -> object:
        value = next(self._iterator)
        source_order = typing.cast("dict[str, int]", value.payload)[
            "source_order"
        ]
        self.pulled.append(source_order)
        self.changed.set()
        return value

    async def wait_for_pulled(self, count: int) -> None:
        while len(self.pulled) < count:
            self.changed.clear()
            if len(self.pulled) >= count:
                return
            await asyncio.wait_for(self.changed.wait(), timeout=1)


class TestFeedWorkScheduler(unittest.IsolatedAsyncioTestCase):
    """Exact lanes, incremental coverage, and bounded task contracts."""

    async def test_public_exports_are_narrow_and_immutable(self) -> None:
        expected = {
            "CallAuthorityLost",
            "CallCompleted",
            "CallIntegrityFailure",
            "CallMembershipRejected",
            "CallRetryable",
            "CallSubmission",
            "FeedRemoved",
            "FeedWorkScheduler",
            "GrantLane",
            "LaneCloseReason",
            "LaneClosed",
            "SchedulerIntegrityError",
            "Undrained",
        }
        forbidden_fragments = {
            "Adapter",
            "Barrier",
            "Flusher",
            "Future",
            "Permit",
            "Receipt",
            "Shard",
            "Slot",
            "Worker",
        }

        self.assertEqual(set(feed_work_scheduler.__all__), expected)
        for exported in feed_work_scheduler.__all__:
            with self.subTest(exported=exported):
                self.assertTrue(hasattr(feed_work_scheduler, exported))
                self.assertTrue(
                    all(
                        fragment not in exported
                        for fragment in forbidden_fragments
                    )
                )

        submission = _submission(uuid.UUID(int=8), 0)
        self.assertFalse(hasattr(submission, "__dict__"))
        self.assertFalse(hasattr(submission, "grant"))
        self.assertFalse(hasattr(submission, "page_sequence"))
        grant = _grant()
        results = (
            feed_work_scheduler.LaneClosed(
                grant,
                feed_work_scheduler.LaneCloseReason.PLANNED_DRAIN,
            ),
            feed_work_scheduler.Undrained(
                grant,
                feed_work_scheduler.LaneCloseReason.AUTHORITY_LOSS,
            ),
            feed_work_scheduler.FeedRemoved(
                grant=grant,
                feed_id=uuid.UUID(int=8),
                released_count=0,
                active_retained=False,
            ),
        )
        for result in results:
            with self.subTest(result=type(result).__name__):
                self.assertTrue(dataclasses.is_dataclass(result))
                self.assertFalse(hasattr(result, "__dict__"))

    async def test_start_is_idempotent_and_opens_one_exact_lane(self) -> None:
        scheduler = feed_work_scheduler.FeedWorkScheduler(_ImmediateExecutor())
        await scheduler.start()
        await scheduler.start()
        grant = _grant()
        lane = scheduler.open_lane(grant)
        try:
            snapshot = await scheduler._snapshot()
            self.assertEqual(len(snapshot.shards), 8)
            self.assertEqual(snapshot.registered_worker_tasks, 32)
            self.assertEqual(snapshot.lane_count, 1)
            self.assertIs(lane.grant, grant)
            self.assertIsInstance(lane, feed_work_scheduler.GrantLane)
            with self.assertRaisesRegex(ValueError, "already has a lane"):
                scheduler.open_lane(grant)
        finally:
            await scheduler.close()

    async def test_page_validates_before_pull_and_returns_sealed_receipt(
        self,
    ) -> None:
        executor = _ImmediateExecutor()
        scheduler = feed_work_scheduler.FeedWorkScheduler(executor)
        await scheduler.start()
        grant = _grant()
        lane = scheduler.open_lane(grant)
        cursor = cursor_policy.LeaseCursor(grant, pos=None)
        candidate = cursor.prepare(_SOURCE_TIME)
        calls = _TracingCalls(
            _submission(uuid.UUID(int=(index + 1) * 8), index)
            for index in range(3)
        )
        try:
            receipt = await lane.cover_page(
                calls=calls,
                boundaries=(),
                candidate=candidate,
            )

            self.assertEqual(calls.pulled, [0, 1, 2])
            self.assertEqual(cursor.accept(receipt), _SOURCE_TIME)
            lane_snapshot = await lane._snapshot()
            self.assertEqual(lane_snapshot.next_page_sequence, 1)
            self.assertIsNone(lane_snapshot.page)
            await scheduler._wait_for_idle()
            self.assertEqual(executor.sequences, [0, 1, 2])

            wrong_grant = _grant(fencing_token=2)
            wrong_cursor = cursor_policy.LeaseCursor(
                wrong_grant,
                pos=None,
            )
            wrong_candidate = wrong_cursor.prepare(_SOURCE_TIME)
            untouched = _TracingCalls((_submission(uuid.UUID(int=8), 0),))
            with self.assertRaisesRegex(
                cursor_policy.CursorIntegrityError,
                "grant",
            ):
                await lane.cover_page(
                    calls=untouched,
                    boundaries=(),
                    candidate=wrong_candidate,
                )
            self.assertEqual(untouched.pulled, [])
        finally:
            await scheduler.close()

    async def test_nonempty_boundary_fails_before_call_admission(self) -> None:
        scheduler = feed_work_scheduler.FeedWorkScheduler(_ImmediateExecutor())
        await scheduler.start()
        grant = _grant()
        lane = scheduler.open_lane(grant)
        candidate = cursor_policy.LeaseCursor(
            grant,
            pos=None,
        ).prepare(_SOURCE_TIME)
        calls = _TracingCalls((_submission(uuid.UUID(int=8), 0),))
        try:
            with self.assertRaisesRegex(
                NotImplementedError,
                "boundary",
            ):
                await lane.cover_page(
                    calls=calls,
                    boundaries=(object(),),
                    candidate=candidate,
                )
            self.assertEqual(calls.pulled, [])
            self.assertEqual((await scheduler._snapshot()).held, 0)
        finally:
            await scheduler.close()

    async def test_hysteresis_preserves_source_order_until_299(
        self,
    ) -> None:
        executor = _GateExecutor()
        scheduler = feed_work_scheduler.FeedWorkScheduler(executor)
        await scheduler.start()
        grant = _grant()
        lane = scheduler.open_lane(grant)
        cursor = cursor_policy.LeaseCursor(grant, pos=None)
        candidate = cursor.prepare(_SOURCE_TIME)
        calls = _TracingCalls(
            _submission(uuid.UUID(int=(index + 1) * 8), index)
            for index in range(402)
        )
        coverage = asyncio.create_task(
            lane.cover_page(
                calls=calls,
                boundaries=(),
                candidate=candidate,
            )
        )
        try:
            await scheduler._shards[0].wait_for_capacity_waiters(1)
            snapshot = await scheduler._snapshot()
            self.assertEqual(snapshot.shards[0].held, 400)
            self.assertTrue(snapshot.shards[0].pressure_paused)
            self.assertEqual(calls.pulled, list(range(401)))
            self.assertFalse(coverage.done())

            await executor.release_completions(100)
            await scheduler._shards[0].wait_for_held(300)
            self.assertEqual(calls.pulled, list(range(401)))
            self.assertFalse(coverage.done())

            await executor.release_completions(1)
            await calls.wait_for_pulled(402)
            receipt = await asyncio.wait_for(coverage, timeout=1)
            self.assertEqual(calls.pulled, list(range(402)))
            self.assertEqual(cursor.accept(receipt), _SOURCE_TIME)
        finally:
            if not coverage.done():
                coverage.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await coverage
            executor.release_all()
            await scheduler._wait_for_idle()
            await scheduler.close()

    async def test_partial_cancel_returns_no_receipt_and_purges_page_queue(
        self,
    ) -> None:
        scheduler_types = _scheduler_types()
        limits = scheduler_types._SchedulerLimits(
            shard_count=1,
            capacity=5,
            workers_per_shard=1,
            high_water=4,
            resume_at=2,
        )
        executor = _GateExecutor()
        scheduler = feed_work_scheduler.FeedWorkScheduler(
            executor,
            _limits=limits,
        )
        await scheduler.start()
        grant = _grant()
        lane = scheduler.open_lane(grant)
        cursor = cursor_policy.LeaseCursor(grant, pos=None)
        candidate = cursor.prepare(_SOURCE_TIME)
        calls = _TracingCalls(
            _submission(uuid.UUID(int=index + 1), index) for index in range(6)
        )
        coverage = asyncio.create_task(
            lane.cover_page(
                calls=calls,
                boundaries=(),
                candidate=candidate,
            )
        )
        try:
            await scheduler._shards[0].wait_for_capacity_waiters(1)
            self.assertEqual(calls.pulled, list(range(5)))
            self.assertEqual((await scheduler._snapshot()).held, 4)

            coverage.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await coverage

            snapshot = await scheduler._snapshot()
            self.assertEqual(snapshot.held, 1)
            self.assertEqual(snapshot.shards[0].queued_calls, 0)
            self.assertEqual(snapshot.shards[0].active_calls, 1)
            self.assertIs(cursor.outstanding_candidate, candidate)
            lane_snapshot = await lane._snapshot()
            self.assertEqual(lane_snapshot.next_page_sequence, 0)
            self.assertIsNone(lane_snapshot.page)
        finally:
            executor.release_all()
            await scheduler._wait_for_idle()
            await scheduler.close()

    async def test_only_one_page_pulls_from_a_lane_at_a_time(self) -> None:
        scheduler_types = _scheduler_types()
        limits = scheduler_types._SchedulerLimits(
            shard_count=1,
            capacity=4,
            workers_per_shard=1,
            high_water=2,
            resume_at=0,
        )
        executor = _GateExecutor()
        scheduler = feed_work_scheduler.FeedWorkScheduler(
            executor,
            _limits=limits,
        )
        await scheduler.start()
        grant = _grant()
        lane = scheduler.open_lane(grant)
        first_candidate = cursor_policy.LeaseCursor(
            grant,
            pos=None,
        ).prepare(_SOURCE_TIME)
        second_candidate = cursor_policy.LeaseCursor(
            grant,
            pos=None,
        ).prepare(_SOURCE_TIME)
        first_calls = _TracingCalls(
            _submission(uuid.UUID(int=index + 1), index) for index in range(3)
        )
        second_calls = _TracingCalls((_submission(uuid.UUID(int=10), 0),))
        first = asyncio.create_task(
            lane.cover_page(
                calls=first_calls,
                boundaries=(),
                candidate=first_candidate,
            )
        )
        second: asyncio.Task[object] | None = None
        try:
            await scheduler._shards[0].wait_for_capacity_waiters(1)
            second = asyncio.create_task(
                lane.cover_page(
                    calls=second_calls,
                    boundaries=(),
                    candidate=second_candidate,
                )
            )
            yielded = asyncio.Event()
            asyncio.get_running_loop().call_soon(yielded.set)
            await yielded.wait()

            self.assertEqual(first_calls.pulled, [0, 1, 2])
            self.assertEqual(second_calls.pulled, [])
            self.assertFalse(second.done())
        finally:
            if second is not None:
                second.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await second
            first.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await first
            executor.release_all()
            await scheduler._wait_for_idle()
            await scheduler.close()

    async def test_exact_drain_preserves_successor_and_sibling_work(
        self,
    ) -> None:
        scheduler_types = _scheduler_types()
        limits = scheduler_types._SchedulerLimits(
            shard_count=1,
            capacity=8,
            workers_per_shard=2,
            high_water=8,
            resume_at=4,
        )
        executor = _GateExecutor()
        scheduler = feed_work_scheduler.FeedWorkScheduler(
            executor,
            _limits=limits,
        )
        await scheduler.start()
        old = _grant(fencing_token=7)
        successor = _grant(fencing_token=8)
        sibling = _grant(lease_key="151", fencing_token=7)
        old_lane = scheduler.open_lane(old)
        successor_lane = scheduler.open_lane(successor)
        sibling_lane = scheduler.open_lane(sibling)
        shared_feed = uuid.UUID(int=1)
        await old_lane.cover_page(
            calls=(
                _submission(shared_feed, 0),
                _submission(shared_feed, 1),
            ),
            boundaries=(),
            candidate=cursor_policy.LeaseCursor(old, pos=None).prepare(
                _SOURCE_TIME
            ),
        )
        await successor_lane.cover_page(
            calls=(_submission(shared_feed, 0),),
            boundaries=(),
            candidate=cursor_policy.LeaseCursor(
                successor,
                pos=None,
            ).prepare(_SOURCE_TIME),
        )
        await sibling_lane.cover_page(
            calls=(_submission(uuid.UUID(int=2), 0),),
            boundaries=(),
            candidate=cursor_policy.LeaseCursor(
                sibling,
                pos=None,
            ).prepare(_SOURCE_TIME),
        )
        await executor.wait_for_started(2)
        close = asyncio.create_task(
            old_lane.close(feed_work_scheduler.LaneCloseReason.PLANNED_DRAIN)
        )
        try:
            await asyncio.wait_for(old_lane._closing_event.wait(), timeout=1)
            snapshot = await old_lane._snapshot()
            self.assertTrue(snapshot.closing)
            self.assertFalse(snapshot.closed)
            self.assertFalse(close.done())
            await scheduler._shards[0].wait_for_held(3)
            records = (await scheduler._snapshot()).shards[0].records
            self.assertEqual(
                tuple(record.grant for record in records),
                (old, successor, sibling),
            )

            executor.release(0)
            result = await asyncio.wait_for(close, timeout=1)
            self.assertEqual(
                result,
                feed_work_scheduler.LaneClosed(
                    old,
                    feed_work_scheduler.LaneCloseReason.PLANNED_DRAIN,
                ),
            )
            self.assertEqual(
                await old_lane.close(
                    feed_work_scheduler.LaneCloseReason.PLANNED_DRAIN
                ),
                result,
            )
            with self.assertRaisesRegex(ValueError, "not newer"):
                scheduler.open_lane(old)
            remaining = (await scheduler._snapshot()).shards[0].records
            self.assertEqual(
                {record.grant for record in remaining},
                {successor, sibling},
            )
            self.assertEqual((await scheduler._snapshot()).lane_count, 2)
            self.assertNotIn(old, scheduler._closing_grants)
        finally:
            executor.release_all()
            await scheduler._wait_for_idle()
            await scheduler.close()

    async def test_drain_upgrades_to_loss_before_any_close_result(
        self,
    ) -> None:
        scheduler_types = _scheduler_types()
        limits = scheduler_types._SchedulerLimits(
            shard_count=1,
            capacity=4,
            workers_per_shard=1,
            high_water=4,
            resume_at=2,
        )
        executor = _DelayedCancellationExecutor()
        scheduler = feed_work_scheduler.FeedWorkScheduler(
            executor,
            _limits=limits,
        )
        await scheduler.start()
        grant = _grant()
        lane = scheduler.open_lane(grant)
        await lane.cover_page(
            calls=(_submission(uuid.UUID(int=1), 0),),
            boundaries=(),
            candidate=cursor_policy.LeaseCursor(grant, pos=None).prepare(
                _SOURCE_TIME
            ),
        )
        await asyncio.wait_for(executor.entered.wait(), timeout=1)
        old_worker = scheduler._shards[0]._workers[0].task
        drain = asyncio.create_task(
            lane.close(feed_work_scheduler.LaneCloseReason.PLANNED_DRAIN)
        )
        await asyncio.wait_for(lane._closing_event.wait(), timeout=1)
        loss = asyncio.create_task(
            lane.close(feed_work_scheduler.LaneCloseReason.AUTHORITY_LOSS)
        )
        await asyncio.wait_for(executor.cancellation_seen.wait(), timeout=1)
        self.assertFalse(drain.done())
        self.assertFalse(loss.done())
        self.assertEqual((await scheduler._snapshot()).held, 1)

        executor.settle.set()
        drain_result, loss_result = await asyncio.wait_for(
            asyncio.gather(drain, loss),
            timeout=1,
        )
        expected = feed_work_scheduler.LaneClosed(
            grant,
            feed_work_scheduler.LaneCloseReason.AUTHORITY_LOSS,
        )
        self.assertEqual(drain_result, expected)
        self.assertEqual(loss_result, expected)
        snapshot = await scheduler._snapshot()
        self.assertEqual(snapshot.held, 0)
        replacement = scheduler._shards[0]._workers[0].task
        self.assertIsNot(replacement, old_worker)
        self.assertTrue(old_worker.done())
        self.assertFalse(replacement.done())
        await scheduler.close()

    async def test_loss_requests_every_shard_before_cleanup_settles(
        self,
    ) -> None:
        scheduler_types = _scheduler_types()
        limits = scheduler_types._SchedulerLimits(
            shard_count=2,
            capacity=4,
            workers_per_shard=1,
            high_water=4,
            resume_at=2,
        )
        executor = _DelayedCancellationExecutor()
        scheduler = feed_work_scheduler.FeedWorkScheduler(
            executor,
            _limits=limits,
        )
        await scheduler.start()
        grant = _grant()
        lane = scheduler.open_lane(grant)
        await lane.cover_page(
            calls=(
                _submission(uuid.UUID(int=1), 0),
                _submission(uuid.UUID(int=2), 1),
            ),
            boundaries=(),
            candidate=cursor_policy.LeaseCursor(grant, pos=None).prepare(
                _SOURCE_TIME
            ),
        )
        await executor.wait_for_counts(2, 0)
        close = asyncio.create_task(
            lane.close(feed_work_scheduler.LaneCloseReason.AUTHORITY_LOSS)
        )

        await executor.wait_for_counts(2, 2)

        self.assertFalse(close.done())
        self.assertEqual((await scheduler._snapshot()).held, 2)
        executor.settle.set()
        self.assertEqual(
            await asyncio.wait_for(close, timeout=1),
            feed_work_scheduler.LaneClosed(
                grant,
                feed_work_scheduler.LaneCloseReason.AUTHORITY_LOSS,
            ),
        )
        self.assertEqual((await scheduler._snapshot()).held, 0)
        await scheduler.close()

    async def test_feed_removal_localizes_blocked_and_queued_records(  # noqa: PLR0915
        self,
    ) -> None:
        scheduler_types = _scheduler_types()
        limits = scheduler_types._SchedulerLimits(
            shard_count=1,
            capacity=4,
            workers_per_shard=1,
            high_water=2,
            resume_at=1,
        )
        executor = _GateExecutor()
        scheduler = feed_work_scheduler.FeedWorkScheduler(
            executor,
            _limits=limits,
        )
        await scheduler.start()
        grant = _grant(fencing_token=7)
        successor = _grant(fencing_token=8)
        lane = scheduler.open_lane(grant)
        successor_lane = scheduler.open_lane(successor)
        removed_feed = uuid.UUID(int=1)
        sibling_feed = uuid.UUID(int=2)
        later_sibling_feed = uuid.UUID(int=3)
        cursor = cursor_policy.LeaseCursor(grant, pos=None)
        calls = _TracingCalls(
            (
                _submission(removed_feed, 0),
                _submission(removed_feed, 1),
                _submission(removed_feed, 2),
                _submission(sibling_feed, 3),
                _submission(later_sibling_feed, 4),
            )
        )
        coverage = asyncio.create_task(
            lane.cover_page(
                calls=calls,
                boundaries=(),
                candidate=cursor.prepare(_SOURCE_TIME),
            )
        )
        try:
            await scheduler._shards[0].wait_for_capacity_waiters(1)
            self.assertEqual(calls.pulled, [0, 1, 2])
            removed = await lane.remove_feed(removed_feed)
            await calls.wait_for_pulled(5)
            await scheduler._shards[0].wait_for_capacity_waiters(1)
            barrier = (await lane._snapshot()).page
            self.assertIsNotNone(barrier)
            self.assertEqual(barrier.pulled, 5)
            self.assertEqual(barrier.registered, 2)
            self.assertEqual(barrier.localized, 2)
            self.assertEqual(barrier.current_source_order, 4)
            self.assertFalse(coverage.done())
            executor.release(0)
            receipt = await asyncio.wait_for(coverage, timeout=1)

            self.assertEqual(
                removed,
                feed_work_scheduler.FeedRemoved(
                    grant=grant,
                    feed_id=removed_feed,
                    released_count=1,
                    active_retained=True,
                ),
            )
            self.assertEqual(calls.pulled, [0, 1, 2, 3, 4])
            self.assertEqual(cursor.accept(receipt), _SOURCE_TIME)
            snapshot = await scheduler._snapshot()
            self.assertEqual(snapshot.held, 2)
            self.assertEqual(
                tuple(
                    record.source_order for record in snapshot.shards[0].records
                ),
                (3, 4),
            )

            next_candidate = cursor.prepare(
                _SOURCE_TIME + datetime.timedelta(seconds=1)
            )
            removed_only = await lane.cover_page(
                calls=(_submission(removed_feed, 0),),
                boundaries=(),
                candidate=next_candidate,
            )
            self.assertEqual(
                cursor.accept(removed_only),
                _SOURCE_TIME + datetime.timedelta(seconds=1),
            )
            self.assertEqual((await scheduler._snapshot()).held, 2)

            executor.release_all()
            await scheduler._wait_for_idle()
            successor_cursor = cursor_policy.LeaseCursor(
                successor,
                pos=None,
            )
            successor_receipt = await successor_lane.cover_page(
                calls=(_submission(removed_feed, 0),),
                boundaries=(),
                candidate=successor_cursor.prepare(_SOURCE_TIME),
            )
            self.assertEqual(
                successor_cursor.accept(successor_receipt),
                _SOURCE_TIME,
            )
            await scheduler._wait_for_idle()
        finally:
            if not coverage.done():
                coverage.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await coverage
            executor.release_all()
            await scheduler._wait_for_idle()
            await scheduler.close()

    async def test_typed_membership_rejection_retires_only_that_feed(
        self,
    ) -> None:
        executor = _GatedOutcomeExecutor(
            feed_work_scheduler.CallMembershipRejected()
        )
        scheduler = feed_work_scheduler.FeedWorkScheduler(executor)
        await scheduler.start()
        grant = _grant()
        lane = scheduler.open_lane(grant)
        removed_feed = uuid.UUID(int=8)
        sibling_feed = uuid.UUID(int=16)
        cursor = cursor_policy.LeaseCursor(grant, pos=None)
        first = await lane.cover_page(
            calls=(_submission(removed_feed, 0),),
            boundaries=(),
            candidate=cursor.prepare(_SOURCE_TIME),
        )
        cursor.accept(first)
        await asyncio.wait_for(executor.entered.wait(), timeout=1)
        executor.release.set()
        await scheduler._wait_for_idle()

        second = await lane.cover_page(
            calls=(
                _submission(removed_feed, 0),
                _submission(sibling_feed, 1),
            ),
            boundaries=(),
            candidate=cursor.prepare(
                _SOURCE_TIME + datetime.timedelta(seconds=1)
            ),
        )
        cursor.accept(second)
        await scheduler._wait_for_idle()
        self.assertEqual(executor.calls, 2)
        self.assertFalse((await scheduler._snapshot()).fatal)
        self.assertFalse((await lane._snapshot()).closing)
        await scheduler.close()

    async def test_typed_authority_loss_closes_its_exact_lane(self) -> None:
        executor = _GatedOutcomeExecutor(
            feed_work_scheduler.CallAuthorityLost()
        )
        scheduler = feed_work_scheduler.FeedWorkScheduler(executor)
        await scheduler.start()
        grant = _grant()
        lane = scheduler.open_lane(grant)
        cursor = cursor_policy.LeaseCursor(grant, pos=None)
        receipt = await lane.cover_page(
            calls=(_submission(uuid.UUID(int=8), 0),),
            boundaries=(),
            candidate=cursor.prepare(_SOURCE_TIME),
        )
        cursor.accept(receipt)
        await asyncio.wait_for(executor.entered.wait(), timeout=1)
        executor.release.set()
        await asyncio.wait_for(lane._closing_event.wait(), timeout=1)

        result = await lane.close(
            feed_work_scheduler.LaneCloseReason.AUTHORITY_LOSS
        )

        self.assertEqual(
            result,
            feed_work_scheduler.LaneClosed(
                grant,
                feed_work_scheduler.LaneCloseReason.AUTHORITY_LOSS,
            ),
        )
        self.assertEqual((await scheduler._snapshot()).held, 0)
        await scheduler.close()

    async def test_swallowed_cancellation_is_undrained_without_reuse(
        self,
    ) -> None:
        scheduler_types = _scheduler_types()
        limits = scheduler_types._SchedulerLimits(
            shard_count=1,
            capacity=2,
            workers_per_shard=1,
            high_water=1,
            resume_at=0,
        )
        executor = _DelayedCancellationExecutor(swallow=True)
        scheduler = feed_work_scheduler.FeedWorkScheduler(
            executor,
            _limits=limits,
        )
        await scheduler.start()
        grant = _grant()
        lane = scheduler.open_lane(grant)
        await lane.cover_page(
            calls=(_submission(uuid.UUID(int=1), 0),),
            boundaries=(),
            candidate=cursor_policy.LeaseCursor(grant, pos=None).prepare(
                _SOURCE_TIME
            ),
        )
        await asyncio.wait_for(executor.entered.wait(), timeout=1)
        worker = scheduler._shards[0]._workers[0].task
        cancel_entered = asyncio.Event()
        allow_cancel = asyncio.Event()
        cancel_exact = scheduler._cancel_exact

        async def gated_cancel_exact(
            exact_grant: ingestion_lease_store.LeaseGrant,
        ) -> None:
            cancel_entered.set()
            await allow_cancel.wait()
            await cancel_exact(exact_grant)

        scheduler._cancel_exact = gated_cancel_exact
        close = asyncio.create_task(
            lane.close(feed_work_scheduler.LaneCloseReason.AUTHORITY_LOSS)
        )
        await asyncio.wait_for(cancel_entered.wait(), timeout=1)
        self.assertFalse(executor.cancellation_seen.is_set())
        close.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await close
        allow_cancel.set()
        await asyncio.wait_for(executor.cancellation_seen.wait(), timeout=1)

        result = await asyncio.wait_for(
            lane.close(feed_work_scheduler.LaneCloseReason.AUTHORITY_LOSS),
            timeout=1,
        )
        self.assertEqual(
            result,
            feed_work_scheduler.Undrained(
                grant,
                feed_work_scheduler.LaneCloseReason.AUTHORITY_LOSS,
            ),
        )
        snapshot = await scheduler._snapshot()
        self.assertTrue(snapshot.fatal)
        self.assertEqual(snapshot.held, 1)
        self.assertIs(scheduler._shards[0]._workers[0].task, worker)
        self.assertFalse(worker.done())

        scheduler_result = await scheduler.close()
        self.assertIsInstance(
            scheduler_result,
            feed_work_scheduler.Undrained,
        )
        self.assertFalse((await scheduler._snapshot()).closed)
        executor.settle.set()
        await asyncio.wait_for(worker, timeout=1)
        self.assertEqual((await scheduler._snapshot()).held, 1)

    async def test_unexpected_worker_failure_reaches_every_lane(self) -> None:
        scheduler_types = _scheduler_types()
        limits = scheduler_types._SchedulerLimits(
            shard_count=2,
            capacity=2,
            workers_per_shard=1,
            high_water=1,
            resume_at=0,
        )
        failing_feed = uuid.UUID(int=2)
        healthy_feed = uuid.UUID(int=1)
        executor = _CrossShardFailureExecutor(failing_feed)
        scheduler = feed_work_scheduler.FeedWorkScheduler(
            executor,
            _limits=limits,
        )
        await scheduler.start()
        first_grant = _grant()
        sibling_grant = _grant(lease_key="151")
        first_lane = scheduler.open_lane(first_grant)
        sibling_lane = scheduler.open_lane(sibling_grant)
        first_cursor = cursor_policy.LeaseCursor(first_grant, pos=None)
        first_cursor.accept(
            await first_lane.cover_page(
                calls=(_submission(failing_feed, 0),),
                boundaries=(),
                candidate=first_cursor.prepare(_SOURCE_TIME),
            )
        )
        await asyncio.wait_for(executor.failing_entered.wait(), timeout=1)
        sibling_cursor = cursor_policy.LeaseCursor(sibling_grant, pos=None)
        sibling_cursor.accept(
            await sibling_lane.cover_page(
                calls=(_submission(healthy_feed, 0),),
                boundaries=(),
                candidate=sibling_cursor.prepare(_SOURCE_TIME),
            )
        )
        await asyncio.wait_for(executor.healthy_entered.wait(), timeout=1)
        blocked = asyncio.create_task(
            sibling_lane.cover_page(
                calls=(_submission(uuid.UUID(int=3), 0),),
                boundaries=(),
                candidate=sibling_cursor.prepare(
                    _SOURCE_TIME + datetime.timedelta(seconds=1)
                ),
            )
        )
        await scheduler._shards[1].wait_for_capacity_waiters(1)
        executor.release_failure.set()
        await scheduler._shards[0].wait_for_fatal()

        with self.assertRaises(feed_work_scheduler.SchedulerIntegrityError):
            await blocked
        with self.assertRaises(feed_work_scheduler.SchedulerIntegrityError):
            await first_lane.cover_page(
                calls=(),
                boundaries=(),
                candidate=first_cursor.prepare(
                    _SOURCE_TIME + datetime.timedelta(seconds=1)
                ),
            )
        healthy_worker = scheduler._shards[1]._workers[0].task
        executor.release_healthy.set()
        await asyncio.wait_for(healthy_worker, timeout=1)
        self.assertIsInstance(
            await first_lane.close(
                feed_work_scheduler.LaneCloseReason.PLANNED_DRAIN
            ),
            feed_work_scheduler.Undrained,
        )
        self.assertIsInstance(
            await sibling_lane.close(
                feed_work_scheduler.LaneCloseReason.PLANNED_DRAIN
            ),
            feed_work_scheduler.Undrained,
        )
        self.assertIsInstance(
            await scheduler.close(),
            feed_work_scheduler.Undrained,
        )
        snapshot = await scheduler._snapshot()
        self.assertFalse(snapshot.closed)
        self.assertEqual(snapshot.held, 1)

    async def test_close_before_coverage_returns_no_receipt(self) -> None:
        scheduler_types = _scheduler_types()
        limits = scheduler_types._SchedulerLimits(
            shard_count=1,
            capacity=5,
            workers_per_shard=1,
            high_water=4,
            resume_at=2,
        )
        executor = _GateExecutor()
        scheduler = feed_work_scheduler.FeedWorkScheduler(
            executor,
            _limits=limits,
        )
        await scheduler.start()
        grant = _grant()
        lane = scheduler.open_lane(grant)
        cursor = cursor_policy.LeaseCursor(grant, pos=None)
        candidate = cursor.prepare(_SOURCE_TIME)
        coverage = asyncio.create_task(
            lane.cover_page(
                calls=(
                    _submission(uuid.UUID(int=index + 1), index)
                    for index in range(6)
                ),
                boundaries=(),
                candidate=candidate,
            )
        )
        close: asyncio.Task[object] | None = None
        try:
            await scheduler._shards[0].wait_for_capacity_waiters(1)
            close = asyncio.create_task(
                lane.close(feed_work_scheduler.LaneCloseReason.PLANNED_DRAIN)
            )
            with self.assertRaisesRegex(RuntimeError, "closed"):
                await coverage

            snapshot = await scheduler._snapshot()
            self.assertTrue((await lane._snapshot()).closing)
            self.assertEqual(snapshot.held, 1)
            self.assertEqual(snapshot.shards[0].queued_calls, 0)
            self.assertIs(cursor.outstanding_candidate, candidate)
            self.assertFalse(close.done())
        finally:
            executor.release_all()
            if close is not None:
                await asyncio.wait_for(close, timeout=1)
            await scheduler.close()


if __name__ == "__main__":
    unittest.main()
