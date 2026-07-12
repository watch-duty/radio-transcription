"""Controlled public-interface tests for bounded Feed work scheduling."""

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

    def release_all(self) -> None:
        self._release_all = True
        for event in self._release.values():
            event.set()


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
            "FeedWorkScheduler",
            "GrantLane",
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


if __name__ == "__main__":
    unittest.main()
