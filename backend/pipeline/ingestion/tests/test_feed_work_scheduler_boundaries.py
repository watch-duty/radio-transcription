"""Focused contracts for ordered, counted page-boundary settlement."""

from __future__ import annotations

import asyncio
import collections
import datetime
import unittest
import uuid

from backend.pipeline.ingestion import feed_work_scheduler
from backend.pipeline.ingestion.collectors.bcfy_calls import cursor_policy
from backend.pipeline.ingestion.feed_work_scheduler import _scheduler, _types
from backend.pipeline.storage import feed_store, ingestion_lease_store

_OWNER_ID = uuid.UUID("11111111-2222-3333-4444-555555555555")
_START = datetime.datetime(2026, 7, 12, 12, 0, tzinfo=datetime.UTC)
_NEXT = _START + datetime.timedelta(seconds=10)


def _grant(
    *,
    fencing_token: int = 1,
) -> ingestion_lease_store.LeaseGrant:
    return ingestion_lease_store.LeaseGrant(
        source_type=feed_store.SourceType.BCFY_CALLS,
        lease_key="150",
        owner_worker_id=_OWNER_ID,
        fencing_token=fencing_token,
    )


def _member(value: int) -> ingestion_lease_store.LeaseMemberIdentity:
    return ingestion_lease_store.LeaseMemberIdentity(
        feed_id=uuid.UUID(int=value),
        source_type=feed_store.SourceType.BCFY_CALLS,
        source_feed_id=f"150-{value}",
    )


def _call(
    member: ingestion_lease_store.LeaseMemberIdentity,
    payload: object,
) -> feed_work_scheduler.CallSubmission:
    return feed_work_scheduler.CallSubmission(
        feed_id=member.feed_id,
        source_timestamp=_START,
        payload=payload,
    )


def _boundary(
    member: ingestion_lease_store.LeaseMemberIdentity,
    target: datetime.datetime = _NEXT,
) -> feed_work_scheduler.BoundaryWork:
    return feed_work_scheduler.BoundaryWork(member, target)


def _limits(
    *,
    capacity: int = 4,
) -> _types._SchedulerLimits:
    return _types._SchedulerLimits(
        shard_count=1,
        capacity=capacity,
        workers_per_shard=1,
        high_water=capacity,
        resume_at=max(0, capacity - 1),
    )


def _cursor(
    grant: ingestion_lease_store.LeaseGrant,
    *,
    pos: datetime.datetime = _START,
) -> cursor_policy.LeaseCursor:
    return cursor_policy.LeaseCursor(grant, pos=pos)


class _ImmediateExecutor:
    async def execute(self, record: _types._CallRecord) -> None:
        del record


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


class _ScriptedCommitter:
    def __init__(
        self,
        *modes: str,
        gated: bool = False,
    ) -> None:
        self._modes = collections.deque(modes)
        self.calls: list[
            tuple[
                ingestion_lease_store.LeaseGrant,
                tuple[feed_work_scheduler.BoundaryWork, ...],
                bool,
            ]
        ] = []
        self.changed = asyncio.Event()
        self.entered = asyncio.Event()
        self.release = asyncio.Event()
        if not gated:
            self.release.set()

    async def commit(
        self,
        grant: ingestion_lease_store.LeaseGrant,
        boundaries: tuple[feed_work_scheduler.BoundaryWork, ...],
        *,
        final_logical: bool,
    ) -> feed_work_scheduler.BoundaryCommitResult:
        self.calls.append((grant, boundaries, final_logical))
        self.changed.set()
        self.entered.set()
        await self.release.wait()
        mode = self._modes.popleft() if self._modes else "committed"
        if mode == "retryable":
            return feed_work_scheduler.BoundaryBatchRetryable()
        if mode == "rejected":
            return feed_work_scheduler.BoundaryGrantRejected()
        if mode == "malformed":
            return feed_work_scheduler.BoundaryBatchCommitted(())
        disposition = (
            feed_work_scheduler.BoundaryDisposition.MEMBER_REJECTED
            if mode == "member_rejected"
            else feed_work_scheduler.BoundaryDisposition.COMMITTED
        )
        return feed_work_scheduler.BoundaryBatchCommitted(
            tuple(
                feed_work_scheduler.BoundaryResult(
                    boundary,
                    disposition,
                )
                for boundary in boundaries
            )
        )

    async def wait_calls(self, count: int) -> None:
        while len(self.calls) < count:
            self.changed.clear()
            if len(self.calls) >= count:
                return
            await asyncio.wait_for(self.changed.wait(), timeout=1)


class TestBoundarySettlement(unittest.IsolatedAsyncioTestCase):
    async def test_empty_page_still_performs_one_final_logical_attempt(
        self,
    ) -> None:
        committer = _ScriptedCommitter()
        scheduler = feed_work_scheduler.FeedWorkScheduler(
            _ImmediateExecutor(),
            committer,
            _limits=_limits(),
        )
        await scheduler.start()
        grant = _grant()
        lane = scheduler.open_lane(grant)
        cursor = _cursor(grant)
        candidate = cursor.prepare(_NEXT)

        receipt = await lane.cover_page((), candidate)

        self.assertEqual(cursor.accept(receipt), _NEXT)
        self.assertEqual(committer.calls, [(grant, (), True)])
        await lane.close()
        self.assertIsNone(await scheduler.close())

    async def test_later_call_stays_ahead_of_pending_boundary(self) -> None:
        executor = _GateExecutor()
        committer = _ScriptedCommitter()
        scheduler = feed_work_scheduler.FeedWorkScheduler(
            executor,
            committer,
            _limits=_limits(),
        )
        await scheduler.start()
        grant = _grant()
        lane = scheduler.open_lane(grant)
        member = _member(1)
        cursor = _cursor(grant)

        first = cursor.prepare(_NEXT)
        cursor.accept(
            await lane.cover_page(
                (_call(member, "first"),),
                first,
                (_boundary(member),),
            )
        )
        await executor.wait_started(1)
        self.assertFalse(
            any(boundaries for _, boundaries, _ in committer.calls)
        )

        second_pos = _NEXT + datetime.timedelta(seconds=10)
        second = cursor.prepare(second_pos)
        cursor.accept(
            await lane.cover_page(
                (_call(member, "second"),),
                second,
            )
        )
        executor.release("first")
        await executor.wait_started(2)
        self.assertEqual(executor.started, ["first", "second"])
        self.assertFalse(
            any(boundaries for _, boundaries, _ in committer.calls)
        )

        executor.release("second")
        await committer.wait_calls(3)
        committed = [
            boundaries for _, boundaries, _ in committer.calls if boundaries
        ]
        self.assertEqual(committed, [(_boundary(member),)])

        await lane.close()
        self.assertIsNone(await scheduler.close())

    async def test_retryable_boundary_coalesces_to_later_target(
        self,
    ) -> None:
        committer = _ScriptedCommitter("retryable", "committed")
        scheduler = feed_work_scheduler.FeedWorkScheduler(
            _ImmediateExecutor(),
            committer,
            _limits=_limits(),
        )
        await scheduler.start()
        grant = _grant()
        lane = scheduler.open_lane(grant)
        member = _member(1)
        cursor = _cursor(grant)

        first = cursor.prepare(_NEXT)
        cursor.accept(
            await lane.cover_page(
                (),
                first,
                (_boundary(member, _NEXT),),
            )
        )
        later = _NEXT + datetime.timedelta(seconds=10)
        second = cursor.prepare(later)
        cursor.accept(
            await lane.cover_page(
                (),
                second,
                (_boundary(member, later),),
            )
        )

        targets = [
            boundaries[0].target
            for _, boundaries, _ in committer.calls
            if boundaries
        ]
        self.assertEqual(targets, [_NEXT, later])
        await lane.close()
        self.assertIsNone(await scheduler.close())

    async def test_retryable_pressure_aborts_once_and_can_replay(
        self,
    ) -> None:
        committer = _ScriptedCommitter("retryable")
        scheduler = feed_work_scheduler.FeedWorkScheduler(
            _ImmediateExecutor(),
            committer,
            _limits=_limits(capacity=1),
            _boundary_batch_size=1,
        )
        await scheduler.start()
        grant = _grant()
        lane = scheduler.open_lane(grant)
        cursor = _cursor(grant)
        candidate = cursor.prepare(_NEXT)
        boundaries = (_boundary(_member(1)), _boundary(_member(2)))

        with self.assertRaisesRegex(RuntimeError, "pressure relief"):
            await lane.cover_page((), candidate, boundaries)
        self.assertEqual(len(committer.calls), 1)
        self.assertIs(cursor.outstanding_candidate, candidate)

        receipt = await lane.cover_page((), candidate, boundaries)
        self.assertEqual(cursor.accept(receipt), _NEXT)
        await lane.close()
        self.assertIsNone(await scheduler.close())

    async def test_oversized_boundary_page_makes_prefix_progress(
        self,
    ) -> None:
        committer = _ScriptedCommitter()
        scheduler = feed_work_scheduler.FeedWorkScheduler(
            _ImmediateExecutor(),
            committer,
            _limits=_limits(capacity=3),
            _boundary_batch_size=2,
        )
        await scheduler.start()
        grant = _grant()
        lane = scheduler.open_lane(grant)
        cursor = _cursor(grant)
        candidate = cursor.prepare(_NEXT)
        boundaries = tuple(_boundary(_member(value)) for value in range(1, 7))

        cursor.accept(await lane.cover_page((), candidate, boundaries))
        await committer.wait_calls(3)

        committed_feed_ids = {
            boundary.feed_id
            for _, batch, _ in committer.calls
            for boundary in batch
        }
        self.assertEqual(
            committed_feed_ids,
            {boundary.feed_id for boundary in boundaries},
        )
        self.assertEqual(
            sum(final for _, _, final in committer.calls),
            1,
        )
        await lane.close()
        self.assertIsNone(await scheduler.close())

    async def test_member_rejection_is_terminal_without_losing_lane(
        self,
    ) -> None:
        committer = _ScriptedCommitter("member_rejected")
        scheduler = feed_work_scheduler.FeedWorkScheduler(
            _ImmediateExecutor(),
            committer,
            _limits=_limits(),
        )
        await scheduler.start()
        grant = _grant()
        lane = scheduler.open_lane(grant)
        cursor = _cursor(grant)

        first = cursor.prepare(_NEXT)
        cursor.accept(
            await lane.cover_page(
                (),
                first,
                (_boundary(_member(1)),),
            )
        )
        later = _NEXT + datetime.timedelta(seconds=10)
        second = cursor.prepare(later)
        cursor.accept(await lane.cover_page((), second))

        self.assertFalse(scheduler.integrity_failure_event.is_set())
        await lane.close()
        self.assertIsNone(await scheduler.close())

    async def test_fence_rejection_closes_only_rejected_lane(self) -> None:
        committer = _ScriptedCommitter("rejected")
        scheduler = feed_work_scheduler.FeedWorkScheduler(
            _ImmediateExecutor(),
            committer,
            _limits=_limits(),
        )
        await scheduler.start()
        old_grant = _grant(fencing_token=1)
        old_lane = scheduler.open_lane(old_grant)
        old_cursor = _cursor(old_grant)

        with self.assertRaises(_scheduler._LaneClosedError):
            await old_lane.cover_page(
                (),
                old_cursor.prepare(_NEXT),
                (_boundary(_member(1)),),
            )
        closed = await old_lane.close()
        self.assertEqual(
            closed.reason,
            feed_work_scheduler.LaneCloseReason.AUTHORITY_LOSS,
        )

        successor = _grant(fencing_token=2)
        new_lane = scheduler.open_lane(successor)
        new_cursor = _cursor(successor)
        new_cursor.accept(
            await new_lane.cover_page((), new_cursor.prepare(_NEXT))
        )
        await new_lane.close()
        self.assertIsNone(await scheduler.close())

    async def test_malformed_committer_result_fails_process_closed(
        self,
    ) -> None:
        committer = _ScriptedCommitter("malformed")
        scheduler = feed_work_scheduler.FeedWorkScheduler(
            _ImmediateExecutor(),
            committer,
            _limits=_limits(),
        )
        await scheduler.start()
        grant = _grant()
        lane = scheduler.open_lane(grant)
        cursor = _cursor(grant)

        with self.assertRaises(feed_work_scheduler.SchedulerIntegrityError):
            await lane.cover_page(
                (),
                cursor.prepare(_NEXT),
                (_boundary(_member(1)),),
            )
        self.assertTrue(scheduler.integrity_failure_event.is_set())
        self.assertEqual(
            await scheduler.close(),
            feed_work_scheduler.Undrained(
                None,
                feed_work_scheduler.LaneCloseReason.SCHEDULER_SHUTDOWN,
            ),
        )

    async def test_cancelled_page_waits_for_begun_commit_to_settle(
        self,
    ) -> None:
        committer = _ScriptedCommitter(gated=True)
        scheduler = feed_work_scheduler.FeedWorkScheduler(
            _ImmediateExecutor(),
            committer,
            _limits=_limits(),
        )
        await scheduler.start()
        grant = _grant()
        lane = scheduler.open_lane(grant)
        cursor = _cursor(grant)
        covering = asyncio.create_task(
            lane.cover_page(
                (),
                cursor.prepare(_NEXT),
                (_boundary(_member(1)),),
            )
        )
        await asyncio.wait_for(committer.entered.wait(), timeout=1)

        covering.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await covering
        closing = lane._close_task
        if closing is None:
            message = "partial page cancellation did not close its lane"
            raise AssertionError(message)
        self.assertFalse(closing.done())

        committer.release.set()
        closed = await closing
        self.assertIsInstance(closed, feed_work_scheduler.LaneClosed)
        self.assertIsNone(await scheduler.close())

    async def test_close_during_retryable_commit_does_not_lose_wakeup(
        self,
    ) -> None:
        committer = _ScriptedCommitter("retryable", gated=True)
        scheduler = feed_work_scheduler.FeedWorkScheduler(
            _ImmediateExecutor(),
            committer,
            _limits=_limits(),
        )
        await scheduler.start()
        grant = _grant()
        lane = scheduler.open_lane(grant)
        cursor = _cursor(grant)
        covering = asyncio.create_task(
            lane.cover_page(
                (),
                cursor.prepare(_NEXT),
                (_boundary(_member(1)),),
            )
        )
        await asyncio.wait_for(committer.entered.wait(), timeout=1)
        closing = asyncio.create_task(
            lane.close(feed_work_scheduler.LaneCloseReason.AUTHORITY_LOSS)
        )

        committer.release.set()
        with self.assertRaises(_scheduler._LaneClosedError):
            await covering
        closed = await closing

        self.assertIsInstance(closed, feed_work_scheduler.LaneClosed)
        self.assertIsNone(await scheduler.close())


if __name__ == "__main__":
    unittest.main()
