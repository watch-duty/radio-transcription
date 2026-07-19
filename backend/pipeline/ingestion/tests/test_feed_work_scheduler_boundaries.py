"""Functional boundary-flush contracts for Feed work scheduling."""

from __future__ import annotations

import asyncio
import datetime
import unittest
import uuid

from backend.pipeline.ingestion import feed_work_scheduler
from backend.pipeline.ingestion.collectors.bcfy_calls import cursor_policy
from backend.pipeline.ingestion.feed_work_scheduler import _types
from backend.pipeline.storage import feed_store, ingestion_lease_store

_OWNER_ID = uuid.UUID("11111111-2222-3333-4444-555555555555")
_SOURCE_TIME = datetime.datetime(2026, 7, 12, 12, 0, tzinfo=datetime.UTC)


def _limits() -> _types._SchedulerLimits:
    return _types._SchedulerLimits(
        shard_count=1,
        capacity=8,
        workers_per_shard=1,
        high_water=8,
        resume_at=4,
    )


def _grant() -> ingestion_lease_store.LeaseGrant:
    return ingestion_lease_store.LeaseGrant(
        source_type=feed_store.SourceType.BCFY_CALLS,
        lease_key="150",
        owner_worker_id=_OWNER_ID,
        fencing_token=1,
    )


def _member(
    grant: ingestion_lease_store.LeaseGrant,
    feed_id: uuid.UUID,
) -> ingestion_lease_store.LeaseMemberIdentity:
    return ingestion_lease_store._issue_member_identity(
        grant,
        feed_id=feed_id,
        source_type=feed_store.SourceType.BCFY_CALLS,
        source_feed_id=f"{grant.lease_key}-{feed_id.int}",
    )


def _require_settled(
    result: feed_work_scheduler.SettledPage | feed_work_scheduler.Undrained,
) -> feed_work_scheduler.SettledPage:
    if not isinstance(result, feed_work_scheduler.SettledPage):
        message = "expected a settled page"
        raise TypeError(message)
    return result


def _require_covered(
    result: feed_work_scheduler.SettledPage | feed_work_scheduler.Undrained,
) -> cursor_policy._CoveredPage:
    settlement = _require_settled(result).lease_settlement
    if not isinstance(settlement, cursor_policy._CoveredPage):
        message = "expected a covered page settlement"
        raise TypeError(message)
    return settlement


def _require_no_progress(
    result: feed_work_scheduler.SettledPage | feed_work_scheduler.Undrained,
) -> cursor_policy._NoProgressPageSettled:
    settlement = _require_settled(result).lease_settlement
    if not isinstance(settlement, cursor_policy._NoProgressPageSettled):
        message = "expected a no-progress page settlement"
        raise TypeError(message)
    return settlement


class _UnusedExecutor:
    async def execute(
        self,
        execution: feed_work_scheduler.CohortExecution,
    ) -> feed_work_scheduler.CohortTerminalFacts:
        del execution
        message = "quiet pages must not execute call work"
        raise AssertionError(message)


class _RecordingCommitter:
    def __init__(self) -> None:
        self.calls: list[
            tuple[
                ingestion_lease_store.LeaseGrant,
                tuple[feed_work_scheduler.BoundaryWork, ...],
                bool,
            ]
        ] = []

    async def commit(
        self,
        grant: ingestion_lease_store.LeaseGrant,
        boundaries: tuple[feed_work_scheduler.BoundaryWork, ...],
        *,
        final_logical: bool,
    ) -> _types.BoundaryCommitResult:
        self.calls.append((grant, boundaries, final_logical))
        return feed_work_scheduler.BoundaryBatchCommitted(
            tuple(
                feed_work_scheduler.BoundaryResult(
                    boundary,
                    feed_work_scheduler.BoundaryDisposition.COMMITTED,
                )
                for boundary in boundaries
            )
        )


class TestBoundaryFlushContracts(unittest.IsolatedAsyncioTestCase):
    async def test_empty_page_performs_one_final_logical_flush(self) -> None:
        committer = _RecordingCommitter()
        scheduler = feed_work_scheduler.FeedWorkScheduler(
            _UnusedExecutor(),
            boundary_committer=committer,
            _limits=_limits(),
        )
        await scheduler.start()
        grant = _grant()
        lane = scheduler.open_lane(
            grant,
            stop_requested=asyncio.Event(),
            grant_lost=asyncio.Event(),
        )
        cursor = cursor_policy.LeaseCursor(grant, pos=None)

        try:
            result = await asyncio.wait_for(
                lane.cover_page(
                    calls=(),
                    boundaries=(),
                    candidate=cursor.prepare(_SOURCE_TIME),
                ),
                timeout=2,
            )

            self.assertEqual(cursor.accept(_require_covered(result)), _SOURCE_TIME)
            self.assertEqual(committer.calls, [(grant, (), True)])
        finally:
            await scheduler.close()

    async def test_quiet_feed_boundary_is_committed_and_finally_flushed(
        self,
    ) -> None:
        committer = _RecordingCommitter()
        scheduler = feed_work_scheduler.FeedWorkScheduler(
            _UnusedExecutor(),
            boundary_committer=committer,
            _limits=_limits(),
        )
        await scheduler.start()
        grant = _grant()
        lane = scheduler.open_lane(
            grant,
            stop_requested=asyncio.Event(),
            grant_lost=asyncio.Event(),
        )
        cursor = cursor_policy.LeaseCursor(grant, pos=_SOURCE_TIME)
        boundary = feed_work_scheduler.BoundaryWork(
            member=_member(grant, uuid.UUID(int=1)),
            target=_SOURCE_TIME,
        )

        try:
            result = await asyncio.wait_for(
                lane.cover_page(
                    calls=(),
                    boundaries=(boundary,),
                    candidate=cursor.prepare(_SOURCE_TIME),
                ),
                timeout=2,
            )

            cursor.accept(_require_covered(result))
            committed = tuple(
                item
                for _exact_grant, batch, _final_logical in committer.calls
                for item in batch
            )
            self.assertEqual(committed, (boundary,))
            self.assertEqual(
                sum(final for _grant_value, _batch, final in committer.calls),
                1,
            )
        finally:
            await scheduler.close()

    async def test_no_progress_page_flushes_without_advancing_cursor(
        self,
    ) -> None:
        committer = _RecordingCommitter()
        scheduler = feed_work_scheduler.FeedWorkScheduler(
            _UnusedExecutor(),
            boundary_committer=committer,
            _limits=_limits(),
        )
        await scheduler.start()
        grant = _grant()
        lane = scheduler.open_lane(
            grant,
            stop_requested=asyncio.Event(),
            grant_lost=asyncio.Event(),
        )
        cursor = cursor_policy.LeaseCursor(grant, pos=_SOURCE_TIME)

        try:
            result = await asyncio.wait_for(
                lane.cover_page(
                    calls=(),
                    boundaries=(),
                    candidate=cursor.prepare_no_progress(),
                ),
                timeout=2,
            )

            cursor.accept_no_progress(_require_no_progress(result))
            self.assertEqual(cursor.pos, _SOURCE_TIME)
            self.assertEqual(cursor.next_page_sequence, 1)
            self.assertEqual(committer.calls, [(grant, (), True)])
        finally:
            await scheduler.close()
