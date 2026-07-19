"""Focused contracts for exact-grant Feed work scheduling."""

from __future__ import annotations

import asyncio
import datetime
import typing
import unittest
import uuid

from backend.pipeline.ingestion import feed_work_scheduler
from backend.pipeline.ingestion.collectors.bcfy_calls import cursor_policy
from backend.pipeline.ingestion.feed_work_scheduler import _scheduler, _types
from backend.pipeline.storage import feed_store, ingestion_lease_store

if typing.TYPE_CHECKING:
    import collections.abc

_OWNER_ID = uuid.UUID("11111111-2222-3333-4444-555555555555")
_FEED_IDS = tuple(uuid.UUID(int=value) for value in range(1, 5))
_START = datetime.datetime(2026, 7, 12, 12, 0, tzinfo=datetime.UTC)
_NEXT = _START + datetime.timedelta(seconds=10)


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


def _limits(
    *,
    shards: int = 1,
    capacity: int = 4,
    workers: int = 1,
) -> _types._SchedulerLimits:
    return _types._SchedulerLimits(
        shard_count=shards,
        capacity=capacity,
        workers_per_shard=workers,
        high_water=capacity,
        resume_at=max(0, capacity - 1),
    )


def _call(
    feed_id: uuid.UUID,
    payload: object,
) -> feed_work_scheduler.CallSubmission:
    return feed_work_scheduler.CallSubmission(
        feed_id=feed_id,
        source_timestamp=_START,
        payload=payload,
    )


def _candidate(
    grant: ingestion_lease_store.LeaseGrant,
) -> tuple[cursor_policy.LeaseCursor, cursor_policy.PageCursorCandidate]:
    cursor = cursor_policy.LeaseCursor(grant, pos=_START)
    return cursor, cursor.prepare(_NEXT)


class _GateExecutor:
    def __init__(self) -> None:
        self.started: list[object] = []
        self.changed = asyncio.Event()
        self._releases: dict[object, asyncio.Event] = {}

    async def execute(self, record: _types._CallRecord) -> None:
        payload = record.work.payload
        self.started.append(payload)
        self._releases.setdefault(payload, asyncio.Event())
        self.changed.set()
        await self._releases[payload].wait()

    async def wait_started(self, count: int) -> None:
        while len(self.started) < count:
            self.changed.clear()
            if len(self.started) >= count:
                return
            await asyncio.wait_for(self.changed.wait(), timeout=1)

    def release(self, payload: object) -> None:
        self._releases.setdefault(payload, asyncio.Event()).set()


class _CancellationExecutor:
    def __init__(self) -> None:
        self.entered = asyncio.Event()
        self.cancelled = asyncio.Event()

    async def execute(self, record: _types._CallRecord) -> None:
        del record
        self.entered.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            self.cancelled.set()
            raise


class _ImmediateExecutor:
    def __init__(self) -> None:
        self.payloads: list[object] = []
        self.changed = asyncio.Event()

    async def execute(self, record: _types._CallRecord) -> None:
        self.payloads.append(record.work.payload)
        self.changed.set()

    async def wait_completed(self, count: int) -> None:
        while len(self.payloads) < count:
            self.changed.clear()
            if len(self.payloads) >= count:
                return
            await asyncio.wait_for(self.changed.wait(), timeout=1)


class _FailingExecutor:
    async def execute(self, record: _types._CallRecord) -> None:
        del record
        message = "executor integrity failed"
        raise RuntimeError(message)


class TestExactGrantScheduler(unittest.IsolatedAsyncioTestCase):
    async def test_page_receipt_proves_bounded_admission_not_completion(
        self,
    ) -> None:
        executor = _GateExecutor()
        scheduler = feed_work_scheduler.FeedWorkScheduler(
            executor,
            _limits=_limits(capacity=2),
        )
        await scheduler.start()
        grant = _grant()
        lane = scheduler.open_lane(grant)
        cursor, candidate = _candidate(grant)

        receipt = await lane.cover_page(
            (
                _call(_FEED_IDS[0], "first"),
                _call(_FEED_IDS[1], "second"),
            ),
            candidate,
        )
        self.assertEqual(cursor.accept(receipt), _NEXT)

        await executor.wait_started(1)
        self.assertEqual(executor.started, ["first"])
        executor.release("first")
        await executor.wait_started(2)
        executor.release("second")

        result = await lane.close()
        self.assertEqual(
            result,
            feed_work_scheduler.LaneClosed(
                grant,
                feed_work_scheduler.LaneCloseReason.PLANNED_DRAIN,
            ),
        )
        self.assertIsNone(await scheduler.close())

    async def test_new_fence_closes_predecessor_and_rejects_stale_grants(
        self,
    ) -> None:
        scheduler = feed_work_scheduler.FeedWorkScheduler(
            _ImmediateExecutor(),
            _limits=_limits(),
        )
        await scheduler.start()
        predecessor = _grant(fencing_token=7)
        old_lane = scheduler.open_lane(predecessor)

        with self.assertRaisesRegex(ValueError, "already"):
            scheduler.open_lane(predecessor)

        successor = _grant(fencing_token=8)
        new_lane = scheduler.open_lane(successor)
        old_result = await old_lane.close()
        self.assertEqual(
            old_result,
            feed_work_scheduler.LaneClosed(
                predecessor,
                feed_work_scheduler.LaneCloseReason.AUTHORITY_LOSS,
            ),
        )

        with self.assertRaisesRegex(ValueError, "newer"):
            scheduler.open_lane(_grant(fencing_token=6))

        await new_lane.close()
        self.assertIsNone(await scheduler.close())

    async def test_candidate_must_match_lane_and_exact_page_sequence(
        self,
    ) -> None:
        scheduler = feed_work_scheduler.FeedWorkScheduler(
            _ImmediateExecutor(),
            _limits=_limits(),
        )
        await scheduler.start()
        grant = _grant()
        lane = scheduler.open_lane(grant)
        _other_cursor, wrong = _candidate(_grant(lease_key="151"))

        with self.assertRaisesRegex(
            cursor_policy.CursorIntegrityError,
            "grant",
        ):
            await lane.cover_page((), wrong)

        cursor, first = _candidate(grant)
        receipt = await lane.cover_page((), first)
        cursor.accept(receipt)
        with self.assertRaisesRegex(
            cursor_policy.CursorIntegrityError,
            "sequence",
        ):
            await lane.cover_page((), first)

        await lane.close()
        self.assertIsNone(await scheduler.close())

    async def test_purge_feed_drops_queued_work_but_keeps_active_work(
        self,
    ) -> None:
        executor = _GateExecutor()
        scheduler = feed_work_scheduler.FeedWorkScheduler(
            executor,
            _limits=_limits(capacity=3),
        )
        await scheduler.start()
        grant = _grant()
        lane = scheduler.open_lane(grant)
        _cursor, candidate = _candidate(grant)

        await lane.cover_page(
            (
                _call(_FEED_IDS[0], "active"),
                _call(_FEED_IDS[0], "removed"),
                _call(_FEED_IDS[1], "sibling"),
            ),
            candidate,
        )
        await executor.wait_started(1)
        await lane.purge_feed(_FEED_IDS[0])
        executor.release("active")
        await executor.wait_started(2)

        self.assertEqual(executor.started, ["active", "sibling"])
        executor.release("sibling")
        await lane.close()
        self.assertIsNone(await scheduler.close())

    async def test_planned_close_purges_queue_and_drains_active_work(
        self,
    ) -> None:
        executor = _GateExecutor()
        scheduler = feed_work_scheduler.FeedWorkScheduler(
            executor,
            _limits=_limits(capacity=2),
        )
        await scheduler.start()
        grant = _grant()
        lane = scheduler.open_lane(grant)
        _cursor, candidate = _candidate(grant)
        await lane.cover_page(
            (
                _call(_FEED_IDS[0], "active"),
                _call(_FEED_IDS[1], "queued"),
            ),
            candidate,
        )
        await executor.wait_started(1)

        closing = asyncio.create_task(lane.close())
        await asyncio.sleep(0)
        self.assertFalse(closing.done())
        executor.release("active")
        result = await closing

        self.assertEqual(executor.started, ["active"])
        self.assertEqual(
            result.reason,
            feed_work_scheduler.LaneCloseReason.PLANNED_DRAIN,
        )
        self.assertIsNone(await scheduler.close())

    async def test_authority_loss_cancels_active_work(self) -> None:
        executor = _CancellationExecutor()
        scheduler = feed_work_scheduler.FeedWorkScheduler(
            executor,
            _limits=_limits(),
        )
        await scheduler.start()
        grant = _grant()
        lane = scheduler.open_lane(grant)
        _cursor, candidate = _candidate(grant)
        await lane.cover_page((_call(_FEED_IDS[0], "active"),), candidate)
        await asyncio.wait_for(executor.entered.wait(), timeout=1)

        result = await lane.close(
            feed_work_scheduler.LaneCloseReason.AUTHORITY_LOSS
        )

        self.assertTrue(executor.cancelled.is_set())
        self.assertEqual(
            result,
            feed_work_scheduler.LaneClosed(
                grant,
                feed_work_scheduler.LaneCloseReason.AUTHORITY_LOSS,
            ),
        )
        self.assertIsNone(await scheduler.close())

    async def test_lane_close_wakes_capacity_blocked_page_admission(
        self,
    ) -> None:
        executor = _CancellationExecutor()
        scheduler = feed_work_scheduler.FeedWorkScheduler(
            executor,
            _limits=_limits(capacity=1),
        )
        await scheduler.start()
        grant = _grant()
        lane = scheduler.open_lane(grant)
        _cursor, candidate = _candidate(grant)
        covering = asyncio.create_task(
            lane.cover_page(
                (
                    _call(_FEED_IDS[0], "active"),
                    _call(_FEED_IDS[1], "blocked"),
                ),
                candidate,
            )
        )
        await asyncio.wait_for(executor.entered.wait(), timeout=1)

        closed = await lane.close(
            feed_work_scheduler.LaneCloseReason.AUTHORITY_LOSS
        )
        with self.assertRaises(_scheduler._LaneClosedError):
            await covering

        self.assertIsInstance(closed, feed_work_scheduler.LaneClosed)
        self.assertTrue(executor.cancelled.is_set())
        self.assertIsNone(await scheduler.close())

    async def test_partial_source_failure_closes_admitted_page(
        self,
    ) -> None:
        executor = _CancellationExecutor()
        scheduler = feed_work_scheduler.FeedWorkScheduler(
            executor,
            _limits=_limits(),
        )
        await scheduler.start()
        grant = _grant()
        lane = scheduler.open_lane(grant)
        _cursor, candidate = _candidate(grant)

        def broken_page() -> collections.abc.Iterator[
            feed_work_scheduler.CallSubmission
        ]:
            yield _call(_FEED_IDS[0], "active")
            message = "source iterator failed"
            raise RuntimeError(message)

        with self.assertRaisesRegex(RuntimeError, "source iterator failed"):
            await lane.cover_page(broken_page(), candidate)

        result = await lane.close()
        self.assertEqual(
            result.reason,
            feed_work_scheduler.LaneCloseReason.AUTHORITY_LOSS,
        )
        self.assertIsNone(await scheduler.close())

    async def test_executor_failure_stops_process_wide_admission(self) -> None:
        scheduler = feed_work_scheduler.FeedWorkScheduler(
            _FailingExecutor(),
            _limits=_limits(shards=2),
        )
        await scheduler.start()
        first_grant = _grant(lease_key="150")
        first_lane = scheduler.open_lane(first_grant)
        _cursor, candidate = _candidate(first_grant)
        await first_lane.cover_page(
            (_call(_FEED_IDS[0], "failure"),),
            candidate,
        )

        await asyncio.wait_for(
            scheduler.integrity_failure_event.wait(),
            timeout=1,
        )
        with self.assertRaises(feed_work_scheduler.SchedulerIntegrityError):
            scheduler.open_lane(_grant(lease_key="151"))

        self.assertEqual(
            await scheduler.close(),
            feed_work_scheduler.Undrained(
                None,
                feed_work_scheduler.LaneCloseReason.SCHEDULER_SHUTDOWN,
            ),
        )

    async def test_scheduler_shutdown_closes_all_open_lanes(self) -> None:
        executor = _ImmediateExecutor()
        scheduler = feed_work_scheduler.FeedWorkScheduler(
            executor,
            _limits=_limits(),
        )
        await scheduler.start()
        first_grant = _grant(lease_key="150")
        second_grant = _grant(lease_key="151")
        first = scheduler.open_lane(first_grant)
        second = scheduler.open_lane(second_grant)
        _first_cursor, first_candidate = _candidate(first_grant)
        _second_cursor, second_candidate = _candidate(second_grant)
        await first.cover_page(
            (_call(_FEED_IDS[0], "first"),),
            first_candidate,
        )
        await second.cover_page(
            (_call(_FEED_IDS[1], "second"),),
            second_candidate,
        )
        await executor.wait_completed(2)

        self.assertIsNone(await scheduler.close())
        with self.assertRaisesRegex(RuntimeError, "closing"):
            scheduler.open_lane(_grant(lease_key="152"))


if __name__ == "__main__":
    unittest.main()
