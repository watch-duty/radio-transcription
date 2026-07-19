"""Public behavioral contracts for bounded Feed work scheduling."""

from __future__ import annotations

import asyncio
import datetime
import typing
import unittest
import uuid

from backend.pipeline.ingestion import feed_work_scheduler
from backend.pipeline.ingestion.collectors.bcfy_calls import cursor_policy
from backend.pipeline.ingestion.feed_work_scheduler import _types
from backend.pipeline.storage import feed_store, ingestion_lease_store

if typing.TYPE_CHECKING:
    import collections.abc

_OWNER_ID = uuid.UUID("11111111-2222-3333-4444-555555555555")
_SOURCE_TIME = datetime.datetime(2026, 7, 12, 12, 0, tzinfo=datetime.UTC)


def _limits(*, workers: int = 2) -> _types._SchedulerLimits:
    return _types._SchedulerLimits(
        shard_count=1,
        capacity=8,
        workers_per_shard=workers,
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
        sid=grant.lease_key,
        group_id=str(feed_id.int),
    )


def _cohort(
    grant: ingestion_lease_store.LeaseGrant,
    feed_id: uuid.UUID,
    *,
    timestamp: datetime.datetime,
    payloads: tuple[object, ...],
    admission_hook: (
        collections.abc.Callable[
            [tuple[feed_work_scheduler.CohortRecordIdentity, ...]],
            None,
        ]
        | None
    ) = None,
    settlement_observer: (
        collections.abc.Callable[
            [feed_work_scheduler.CallSettlement],
            None,
        ]
        | None
    ) = None,
) -> feed_work_scheduler.CohortSubmission:
    member = _member(grant, feed_id)
    return feed_work_scheduler.CohortSubmission(
        member=member,
        feed_id=feed_id,
        cohort_timestamp=timestamp,
        calls=tuple(
            feed_work_scheduler.CallSubmission(
                feed_id=feed_id,
                source_timestamp=timestamp,
                payload={"value": payload, "member": member},
                settlement_observer=settlement_observer,
            )
            for payload in payloads
        ),
        admission_hook=(
            (lambda _identities: None)
            if admission_hook is None
            else admission_hook
        ),
    )


def _settled_facts(
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


class _ImmediateExecutor:
    def __init__(self) -> None:
        self.executions: list[feed_work_scheduler.CohortExecution] = []

    async def execute(
        self,
        execution: feed_work_scheduler.CohortExecution,
    ) -> feed_work_scheduler.CohortTerminalFacts:
        self.executions.append(execution)
        return _settled_facts(execution)


class _GatedExecutor:
    def __init__(self) -> None:
        self.started = {
            0: asyncio.Event(),
            1: asyncio.Event(),
        }
        self.release = {
            0: asyncio.Event(),
            1: asyncio.Event(),
        }
        self.timeline: list[tuple[str, int]] = []

    async def execute(
        self,
        execution: feed_work_scheduler.CohortExecution,
    ) -> feed_work_scheduler.CohortTerminalFacts:
        payload = typing.cast("dict[str, object]", execution.calls[0].payload)
        sequence = typing.cast("int", payload["value"])
        self.timeline.append(("start", sequence))
        self.started[sequence].set()
        await self.release[sequence].wait()
        self.timeline.append(("finish", sequence))
        return _settled_facts(execution)

    def release_all(self) -> None:
        for event in self.release.values():
            event.set()


class _IntegrityFailureExecutor:
    async def execute(
        self,
        execution: feed_work_scheduler.CohortExecution,
    ) -> feed_work_scheduler.CohortTerminalFacts:
        del execution
        message = "executor rejected cohort integrity"
        raise feed_work_scheduler.CohortIntegrityError(message)


class _CancellationIntegrityExecutor:
    def __init__(self) -> None:
        self.entered = asyncio.Event()

    async def execute(
        self,
        execution: feed_work_scheduler.CohortExecution,
    ) -> feed_work_scheduler.CohortTerminalFacts:
        self.entered.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            failure = feed_work_scheduler.CohortIntegrityError(
                "commit evidence became malformed during cancellation"
            )
            facts = feed_work_scheduler.CohortTerminalFacts(
                records=tuple(
                    feed_work_scheduler.CohortRecordTerminalFact(
                        identity=call.identity,
                        participated=True,
                        closure_state=(
                            feed_work_scheduler.CohortRecordClosureState.OUTCOME_UNKNOWN
                        ),
                        full_pipeline_completed=False,
                        terminal_reason=(
                            feed_work_scheduler.CohortRecordTerminalReason.INTEGRITY_FAILURE
                        ),
                    )
                    for call in execution.calls
                ),
                disposition=(
                    feed_work_scheduler.CohortTerminalDisposition.INTEGRITY_FAILURE
                ),
            )
            execution.retention.retain(
                feed_work_scheduler.OutcomeUnknownRetentionRequest(
                    feed_work_scheduler.OutcomeUnknownCause.COMMIT,
                    facts,
                    failure,
                )
            )
            raise
        message = "unset cancellation gate unexpectedly completed"
        raise AssertionError(message)


class TestFeedWorkScheduler(unittest.IsolatedAsyncioTestCase):
    async def test_page_preserves_exact_admission_identity_and_settles(
        self,
    ) -> None:
        executor = _ImmediateExecutor()
        scheduler = feed_work_scheduler.FeedWorkScheduler(
            executor,
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
        admitted: list[
            tuple[feed_work_scheduler.CohortRecordIdentity, ...]
        ] = []
        settlements: list[feed_work_scheduler.CallSettlement] = []
        cohort = _cohort(
            grant,
            uuid.UUID(int=1),
            timestamp=_SOURCE_TIME,
            payloads=("first", "second"),
            admission_hook=admitted.append,
            settlement_observer=settlements.append,
        )

        try:
            result = await asyncio.wait_for(
                lane.cover_page(
                    calls=(cohort,),
                    boundaries=(),
                    candidate=cursor.prepare(_SOURCE_TIME),
                ),
                timeout=2,
            )

            self.assertEqual(cursor.accept(_require_covered(result)), _SOURCE_TIME)
            self.assertEqual(len(admitted), 1)
            execution_identities = tuple(
                call.identity for call in executor.executions[0].calls
            )
            self.assertTrue(
                all(
                    admitted_identity is execution_identity
                    for admitted_identity, execution_identity in zip(
                        admitted[0],
                        execution_identities,
                        strict=True,
                    )
                )
            )
            self.assertEqual(
                tuple(identity.local_sequence for identity in admitted[0]),
                (0, 1),
            )
            self.assertEqual(
                settlements,
                [feed_work_scheduler.CallSettlement.COMPLETED] * 2,
            )
            self.assertIsInstance(
                await lane.close(),
                feed_work_scheduler.LaneClosed,
            )
        finally:
            await scheduler.close()

    async def test_same_feed_cohorts_execute_fifo(self) -> None:
        executor = _GatedExecutor()
        scheduler = feed_work_scheduler.FeedWorkScheduler(
            executor,
            _limits=_limits(workers=2),
        )
        await scheduler.start()
        grant = _grant()
        lane = scheduler.open_lane(
            grant,
            stop_requested=asyncio.Event(),
            grant_lost=asyncio.Event(),
        )
        cursor = cursor_policy.LeaseCursor(grant, pos=None)
        feed_id = uuid.UUID(int=1)
        coverage = asyncio.create_task(
            lane.cover_page(
                calls=(
                    _cohort(
                        grant,
                        feed_id,
                        timestamp=_SOURCE_TIME,
                        payloads=(0,),
                    ),
                    _cohort(
                        grant,
                        feed_id,
                        timestamp=_SOURCE_TIME + datetime.timedelta(seconds=1),
                        payloads=(1,),
                    ),
                ),
                boundaries=(),
                candidate=cursor.prepare(_SOURCE_TIME),
            )
        )

        try:
            await asyncio.wait_for(executor.started[0].wait(), timeout=1)
            with self.assertRaises(TimeoutError):
                await asyncio.wait_for(executor.started[1].wait(), timeout=0.05)
            executor.release[0].set()
            await asyncio.wait_for(executor.started[1].wait(), timeout=1)
            executor.release[1].set()
            result = await asyncio.wait_for(coverage, timeout=2)

            cursor.accept(_require_covered(result))
            self.assertEqual(
                executor.timeline,
                [
                    ("start", 0),
                    ("finish", 0),
                    ("start", 1),
                    ("finish", 1),
                ],
            )
        finally:
            executor.release_all()
            if not coverage.done():
                coverage.cancel()
                await asyncio.gather(coverage, return_exceptions=True)
            await scheduler.close()

    async def test_distinct_feeds_can_execute_concurrently(self) -> None:
        executor = _GatedExecutor()
        scheduler = feed_work_scheduler.FeedWorkScheduler(
            executor,
            _limits=_limits(workers=2),
        )
        await scheduler.start()
        grant = _grant()
        lane = scheduler.open_lane(
            grant,
            stop_requested=asyncio.Event(),
            grant_lost=asyncio.Event(),
        )
        cursor = cursor_policy.LeaseCursor(grant, pos=None)
        coverage = asyncio.create_task(
            lane.cover_page(
                calls=tuple(
                    _cohort(
                        grant,
                        uuid.UUID(int=sequence + 1),
                        timestamp=(
                            _SOURCE_TIME
                            + datetime.timedelta(seconds=sequence)
                        ),
                        payloads=(sequence,),
                    )
                    for sequence in range(2)
                ),
                boundaries=(),
                candidate=cursor.prepare(_SOURCE_TIME),
            )
        )

        try:
            await asyncio.wait_for(
                asyncio.gather(
                    executor.started[0].wait(),
                    executor.started[1].wait(),
                ),
                timeout=1,
            )
            self.assertNotIn(("finish", 0), executor.timeline)
            self.assertNotIn(("finish", 1), executor.timeline)
            executor.release_all()
            result = await asyncio.wait_for(coverage, timeout=2)
            cursor.accept(_require_covered(result))
        finally:
            executor.release_all()
            if not coverage.done():
                coverage.cancel()
                await asyncio.gather(coverage, return_exceptions=True)
            await scheduler.close()

    async def test_executor_integrity_failure_is_scheduler_fatal(self) -> None:
        scheduler = feed_work_scheduler.FeedWorkScheduler(
            _IntegrityFailureExecutor(),
            _limits=_limits(workers=1),
        )
        await scheduler.start()
        grant = _grant()
        lane = scheduler.open_lane(
            grant,
            stop_requested=asyncio.Event(),
            grant_lost=asyncio.Event(),
        )
        cursor = cursor_policy.LeaseCursor(grant, pos=None)
        coverage = asyncio.create_task(
            lane.cover_page(
                calls=(
                    _cohort(
                        grant,
                        uuid.UUID(int=1),
                        timestamp=_SOURCE_TIME,
                        payloads=("call",),
                    ),
                ),
                boundaries=(),
                candidate=cursor.prepare(_SOURCE_TIME),
            )
        )

        try:
            await asyncio.wait_for(
                scheduler.integrity_failure_event.wait(),
                timeout=1,
            )
            with self.assertRaises(
                feed_work_scheduler.SchedulerIntegrityError
            ) as raised:
                scheduler.raise_if_failed()
            self.assertIsInstance(
                raised.exception.__cause__,
                feed_work_scheduler.CohortIntegrityError,
            )
            await asyncio.wait_for(
                asyncio.gather(coverage, return_exceptions=True),
                timeout=1,
            )
        finally:
            if not coverage.done():
                coverage.cancel()
                await asyncio.gather(coverage, return_exceptions=True)
            self.assertIsInstance(
                await scheduler.close(),
                feed_work_scheduler.Undrained,
            )

    async def test_cancelled_integrity_retention_is_scheduler_fatal(
        self,
    ) -> None:
        executor = _CancellationIntegrityExecutor()
        scheduler = feed_work_scheduler.FeedWorkScheduler(
            executor,
            _limits=_limits(workers=1),
        )
        await scheduler.start()
        grant = _grant()
        lane = scheduler.open_lane(
            grant,
            stop_requested=asyncio.Event(),
            grant_lost=asyncio.Event(),
        )
        cursor = cursor_policy.LeaseCursor(grant, pos=None)
        coverage = asyncio.create_task(
            lane.cover_page(
                calls=(
                    _cohort(
                        grant,
                        uuid.UUID(int=1),
                        timestamp=_SOURCE_TIME,
                        payloads=("call",),
                    ),
                ),
                boundaries=(),
                candidate=cursor.prepare(_SOURCE_TIME),
            )
        )

        try:
            await asyncio.wait_for(executor.entered.wait(), timeout=1)
            close = asyncio.create_task(
                lane.close(
                    feed_work_scheduler.LaneCloseReason.AUTHORITY_LOSS
                )
            )
            await asyncio.wait_for(
                scheduler.integrity_failure_event.wait(),
                timeout=1,
            )
            with self.assertRaises(
                feed_work_scheduler.SchedulerIntegrityError
            ) as raised:
                scheduler.raise_if_failed()
            self.assertIsInstance(
                raised.exception.__cause__,
                feed_work_scheduler.CohortIntegrityError,
            )
            self.assertIsInstance(
                await asyncio.wait_for(close, timeout=1),
                feed_work_scheduler.Undrained,
            )
            await asyncio.wait_for(
                asyncio.gather(coverage, return_exceptions=True),
                timeout=1,
            )
        finally:
            if not coverage.done():
                coverage.cancel()
                await asyncio.gather(coverage, return_exceptions=True)
            self.assertIsInstance(
                await scheduler.close(),
                feed_work_scheduler.Undrained,
            )
