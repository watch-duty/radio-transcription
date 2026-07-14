"""Controlled exact-lane lifecycle tests for the Broadcastify SID runner."""

from __future__ import annotations

import asyncio
import datetime
import importlib
import inspect
import typing
import unittest
import unittest.mock
import uuid

from backend.pipeline.ingestion import (
    feed_work_scheduler,
    grant_control,
    sid_grant_control,
)
from backend.pipeline.ingestion.collectors.bcfy_calls import sid_processor
from backend.pipeline.storage import feed_store, ingestion_lease_store

_OWNER_ID = uuid.UUID("11111111-2222-3333-4444-555555555555")
_NOW = datetime.datetime(2026, 7, 12, 12, 0, tzinfo=datetime.UTC)
_SID_FAILURE_CASES = (
    (
        "provider-exhaustion",
        feed_store.FeedStatusReason.SYSTEM_SOURCE_PAYLOAD_INVALID,
        "invalid provider envelope",
    ),
    (
        "membership-invariant",
        feed_store.FeedStatusReason.SYSTEM_SOURCE_CONFIGURATION_INVALID,
        "bcfy_calls_sid_membership_invalid",
    ),
    (
        "logical-failure-threshold",
        feed_store.FeedStatusReason.SOURCE_RATE_LIMITED,
        "provider rate limit",
    ),
    (
        "promoted-page",
        feed_store.FeedStatusReason.SYSTEM_SOURCE_PAYLOAD_INVALID,
        "terminal item skip",
    ),
)


def _sid_runner_module():
    return importlib.import_module(
        "backend.pipeline.ingestion.collectors.bcfy_calls.sid_runner"
    )


def _grant(
    lease_key: str = "150",
    *,
    fencing_token: int = 1,
) -> ingestion_lease_store.LeaseGrant:
    return ingestion_lease_store.LeaseGrant(
        source_type=feed_store.SourceType.BCFY_CALLS,
        lease_key=lease_key,
        owner_worker_id=_OWNER_ID,
        fencing_token=fencing_token,
    )


def _snapshot() -> sid_grant_control.SidClaimPayload:
    return sid_grant_control.SidClaimPayload(
        ingestion_lease_store.LeaseSnapshot(
            status=feed_store.FeedStatus.ACTIVE,
            last_heartbeat=_NOW,
            failure_count=0,
            retry_after=None,
            status_reason=None,
            status_reason_detail=None,
            status_reason_updated_at=None,
            audit_revision=1,
            membership_revision=1,
            updated_at=_NOW,
        ),
        grant_control.ClaimMode.PRIMARY,
    )


class _TrackedEvent(asyncio.Event):
    """Event exposing deterministic waiter-settlement evidence."""

    def __init__(self) -> None:
        super().__init__()
        self.active_waiters = 0
        self.completed_waiters = 0
        self.cancelled_waiters = 0
        self.wait_entered = asyncio.Event()

    async def wait(self) -> typing.Literal[True]:
        self.active_waiters += 1
        self.wait_entered.set()
        try:
            result = await super().wait()
        except asyncio.CancelledError:
            self.cancelled_waiters += 1
            raise
        else:
            self.completed_waiters += 1
            return result
        finally:
            self.active_waiters -= 1


class _ControlledLane:
    """One fake exact lane with an Event-gated monotonic close result."""

    def __init__(
        self,
        scheduler: _ControlledScheduler,
        grant: ingestion_lease_store.LeaseGrant,
        stop_requested: asyncio.Event,
        grant_lost: asyncio.Event,
    ) -> None:
        self.scheduler = scheduler
        self.grant = grant
        self.signals = feed_work_scheduler.LaneSignalView(
            grant,
            stop_requested,
            grant_lost,
        )
        self.close_reasons: list[feed_work_scheduler.LaneCloseReason] = []
        self.close_entered = asyncio.Event()
        self.release_close = asyncio.Event()
        self.active_closers = 0
        self.strongest_reason = (
            feed_work_scheduler.LaneCloseReason.PLANNED_DRAIN
        )
        if not scheduler.block_close:
            self.release_close.set()

    async def close(
        self,
        reason: feed_work_scheduler.LaneCloseReason,
    ) -> object:
        self.close_reasons.append(reason)
        if reason is feed_work_scheduler.LaneCloseReason.AUTHORITY_LOSS or (
            reason is feed_work_scheduler.LaneCloseReason.SCHEDULER_SHUTDOWN
            and self.strongest_reason
            is feed_work_scheduler.LaneCloseReason.PLANNED_DRAIN
        ):
            self.strongest_reason = reason
        self.active_closers += 1
        self.close_entered.set()
        self.scheduler.close_calls += 1
        if self.scheduler.close_calls >= 2:
            self.scheduler.two_closes_entered.set()
        try:
            await self.release_close.wait()
            if self.scheduler.result_mode == "undrained":
                return feed_work_scheduler.Undrained(
                    self.grant,
                    self.strongest_reason,
                )
            if self.scheduler.result_mode == "invalid":
                return object()
            if self.scheduler.result_mode == "wrong_grant":
                return feed_work_scheduler.LaneClosed(
                    _grant("999", fencing_token=99),
                    self.strongest_reason,
                )
            return feed_work_scheduler.LaneClosed(
                self.grant,
                self.strongest_reason,
            )
        finally:
            self.active_closers -= 1


class _ControlledScheduler:
    """Fake process scheduler exposing only the runner-facing contract."""

    def __init__(
        self,
        *,
        block_close: bool = False,
        result_mode: str = "closed",
    ) -> None:
        self.block_close = block_close
        self.result_mode = result_mode
        self.integrity_failure_event = _TrackedEvent()
        self.failure: Exception | None = None
        self.opened: list[_ControlledLane] = []
        self.opened_signals: list[tuple[asyncio.Event, asyncio.Event]] = []
        self.lane_opened = asyncio.Event()
        self.two_lanes_opened = asyncio.Event()
        self.close_calls = 0
        self.two_closes_entered = asyncio.Event()

    def open_lane(
        self,
        grant: ingestion_lease_store.LeaseGrant,
        *,
        stop_requested: asyncio.Event,
        grant_lost: asyncio.Event,
    ) -> _ControlledLane:
        lane = _ControlledLane(
            self,
            grant,
            stop_requested,
            grant_lost,
        )
        self.opened.append(lane)
        self.opened_signals.append((stop_requested, grant_lost))
        self.lane_opened.set()
        if len(self.opened) >= 2:
            self.two_lanes_opened.set()
        return lane

    def fail(self, failure: Exception) -> None:
        self.failure = failure
        self.integrity_failure_event.set()

    def raise_if_failed(self) -> None:
        if self.failure is not None:
            message = "controlled scheduler failure"
            raise feed_work_scheduler.SchedulerIntegrityError(
                message
            ) from self.failure


class _ControlledProcessor:
    """One Event-gated processor exposing exact settlement evidence."""

    def __init__(self, terminal: BaseException | None = None) -> None:
        self.terminal = terminal
        self.started = asyncio.Event()
        self.release = asyncio.Event()
        self.work_stop: asyncio.Event | None = None
        self.signals: feed_work_scheduler.LaneSignalView | None = None
        self.cancellation_handoff: (
            sid_processor.ProcessorCancellationHandoff | None
        ) = None
        self.cancelled = False
        self.settled = asyncio.Event()

    async def run(
        self,
        work_stop: asyncio.Event,
        *,
        signals: feed_work_scheduler.LaneSignalView,
        cancellation_handoff: sid_processor.ProcessorCancellationHandoff,
    ) -> None:
        self.signals = signals
        self.cancellation_handoff = cancellation_handoff
        self.work_stop = work_stop
        self.started.set()
        try:
            await self.release.wait()
            if self.terminal is not None:
                raise self.terminal
        except asyncio.CancelledError:
            self.cancelled = True
            raise
        finally:
            self.settled.set()


class _ControlledProcessorFactory:
    """Create exactly one scripted processor per exact runner invocation."""

    def __init__(self, *processors: _ControlledProcessor) -> None:
        self._script = list(processors)
        self.calls: list[
            tuple[
                ingestion_lease_store.LeaseGrant,
                sid_grant_control.SidClaimPayload,
                _ControlledLane,
            ]
        ] = []
        self.processors: list[_ControlledProcessor] = []

    def __call__(
        self,
        grant: ingestion_lease_store.LeaseGrant,
        payload: sid_grant_control.SidClaimPayload,
        lane: _ControlledLane,
    ) -> _ControlledProcessor:
        if lane not in lane.scheduler.opened:
            message = "processor was constructed before its lane opened"
            raise AssertionError(message)
        processor = (
            self._script.pop(0) if self._script else _ControlledProcessor()
        )
        self.calls.append((grant, payload, lane))
        self.processors.append(processor)
        return processor


def _runner(
    sid_runner: typing.Any,
    scheduler: _ControlledScheduler,
    *processors: _ControlledProcessor,
) -> tuple[typing.Any, _ControlledProcessorFactory]:
    factory = _ControlledProcessorFactory(*processors)
    return sid_runner.BcfyCallsSidRunner(scheduler, factory), factory


def _context() -> tuple[
    grant_control.RunContext,
    _TrackedEvent,
    _TrackedEvent,
]:
    stop = _TrackedEvent()
    loss = _TrackedEvent()
    return (
        grant_control.RunContext(
            stop_requested=stop,
            grant_lost=loss,
            set_retrying=lambda _retrying: None,
        ),
        stop,
        loss,
    )


class TestBcfyCallsSidRunner(unittest.IsolatedAsyncioTestCase):
    """Prove one shared runner acknowledges only settled exact lanes."""

    def assert_waiters_settled(
        self,
        scheduler: _ControlledScheduler,
        *events: _TrackedEvent,
    ) -> None:
        for event in (*events, scheduler.integrity_failure_event):
            self.assertEqual(event.active_waiters, 0)
        for lane in scheduler.opened:
            self.assertEqual(lane.active_closers, 0)

    async def test_crossed_payload_type_fails_before_lane_open(self) -> None:
        sid_runner = _sid_runner_module()
        scheduler = _ControlledScheduler()
        runner, factory = _runner(sid_runner, scheduler)
        context, _stop, _loss = _context()
        wrong_payload = _snapshot().snapshot

        with self.assertRaises(TypeError):
            await runner.run(
                _grant(),
                typing.cast(
                    "sid_grant_control.SidClaimPayload",
                    wrong_payload,
                ),
                context,
            )

        self.assertEqual(scheduler.opened, [])
        self.assertEqual(factory.calls, [])

    async def test_concurrent_runs_open_distinct_invocation_local_lanes(
        self,
    ) -> None:
        sid_runner = _sid_runner_module()
        scheduler = _ControlledScheduler(block_close=True)
        runner, factory = _runner(sid_runner, scheduler)
        first_context, first_stop, first_loss = _context()
        second_context, second_stop, second_loss = _context()
        first_grant = _grant("150", fencing_token=1)
        second_grant = _grant("151", fencing_token=2)
        first_payload = _snapshot()
        second_payload = _snapshot()

        first = asyncio.create_task(
            runner.run(first_grant, first_payload, first_context)
        )
        second = asyncio.create_task(
            runner.run(second_grant, second_payload, second_context)
        )
        await asyncio.wait_for(scheduler.two_lanes_opened.wait(), timeout=1)

        self.assertEqual(
            [lane.grant for lane in scheduler.opened],
            [first_grant, second_grant],
        )
        self.assertEqual(
            scheduler.opened_signals,
            [(first_stop, first_loss), (second_stop, second_loss)],
        )
        self.assertIsNot(scheduler.opened[0], scheduler.opened[1])
        self.assertEqual(runner.__slots__, ("_processor_factory", "_scheduler"))
        self.assertFalse(hasattr(runner, "current_lane"))
        self.assertFalse(hasattr(runner, "_current_lane"))
        self.assertEqual(len(factory.calls), 2)
        self.assertIs(factory.calls[0][1], first_payload)
        self.assertIs(factory.calls[1][1], second_payload)
        self.assertEqual(
            [call[2] for call in factory.calls],
            scheduler.opened,
        )
        self.assertIsNot(
            factory.processors[0].work_stop,
            factory.processors[1].work_stop,
        )

        first_stop.set()
        second_stop.set()
        await asyncio.wait_for(
            scheduler.two_closes_entered.wait(),
            timeout=1,
        )
        for lane in scheduler.opened:
            processor = factory.processors[scheduler.opened.index(lane)]
            self.assertTrue(processor.work_stop.is_set())
            self.assertTrue(processor.cancelled)
            self.assertTrue(processor.settled.is_set())
            lane.release_close.set()
        outcomes = await asyncio.gather(first, second)

        self.assertEqual(
            outcomes,
            [
                grant_control.RunStopped(
                    grant_control.TerminalCause.PLANNED_DRAIN
                ),
                grant_control.RunStopped(
                    grant_control.TerminalCause.PLANNED_DRAIN
                ),
            ],
        )
        self.assert_waiters_settled(
            scheduler,
            first_stop,
            first_loss,
            second_stop,
            second_loss,
        )

    async def test_stop_and_loss_return_only_after_exact_lane_settlement(
        self,
    ) -> None:
        sid_runner = _sid_runner_module()
        cases = (
            (
                "stop",
                feed_work_scheduler.LaneCloseReason.PLANNED_DRAIN,
                grant_control.RunStopped(
                    grant_control.TerminalCause.PLANNED_DRAIN
                ),
            ),
            (
                "loss",
                feed_work_scheduler.LaneCloseReason.AUTHORITY_LOSS,
                grant_control.RunLost(),
            ),
        )
        for signal, reason, expected in cases:
            with self.subTest(signal=signal):
                scheduler = _ControlledScheduler(block_close=True)
                processor = _ControlledProcessor()
                runner, _ = _runner(sid_runner, scheduler, processor)
                context, stop, loss = _context()
                task = asyncio.create_task(
                    runner.run(_grant(signal), _snapshot(), context)
                )
                await asyncio.wait_for(scheduler.lane_opened.wait(), timeout=1)
                event = stop if signal == "stop" else loss
                event.set()
                lane = scheduler.opened[0]
                await asyncio.wait_for(lane.close_entered.wait(), timeout=1)

                self.assertFalse(task.done())
                self.assertEqual(lane.close_reasons, [reason])
                self.assertTrue(processor.work_stop.is_set())
                self.assertTrue(processor.cancelled)
                self.assertTrue(processor.settled.is_set())
                lane.release_close.set()
                self.assertEqual(await task, expected)
                self.assert_waiters_settled(scheduler, stop, loss)

    async def test_stop_escalates_to_loss_before_acknowledgement(self) -> None:
        sid_runner = _sid_runner_module()
        scheduler = _ControlledScheduler(block_close=True)
        runner, _ = _runner(sid_runner, scheduler)
        context, stop, loss = _context()
        task = asyncio.create_task(runner.run(_grant(), _snapshot(), context))
        await asyncio.wait_for(scheduler.lane_opened.wait(), timeout=1)
        lane = scheduler.opened[0]

        stop.set()
        await asyncio.wait_for(lane.close_entered.wait(), timeout=1)
        self.assertEqual(
            lane.close_reasons,
            [feed_work_scheduler.LaneCloseReason.PLANNED_DRAIN],
        )
        loss.set()
        await asyncio.wait_for(
            scheduler.two_closes_entered.wait(),
            timeout=1,
        )
        self.assertEqual(
            lane.close_reasons,
            [
                feed_work_scheduler.LaneCloseReason.PLANNED_DRAIN,
                feed_work_scheduler.LaneCloseReason.AUTHORITY_LOSS,
            ],
        )
        lane.release_close.set()

        self.assertEqual(await task, grant_control.RunLost())
        self.assert_waiters_settled(scheduler, stop, loss)

    async def test_simultaneous_loss_precedes_stop_and_integrity_precedes_both(
        self,
    ) -> None:
        sid_runner = _sid_runner_module()
        scheduler = _ControlledScheduler()
        runner, _ = _runner(sid_runner, scheduler)
        context, stop, loss = _context()
        stop.set()
        loss.set()

        outcome = await runner.run(_grant(), _snapshot(), context)

        self.assertEqual(outcome, grant_control.RunLost())
        self.assertEqual(
            scheduler.opened[0].close_reasons,
            [feed_work_scheduler.LaneCloseReason.AUTHORITY_LOSS],
        )
        self.assert_waiters_settled(scheduler, stop, loss)

        fatal_scheduler = _ControlledScheduler()
        fatal_scheduler.fail(RuntimeError("worker failed"))
        fatal_runner, _ = _runner(sid_runner, fatal_scheduler)
        fatal_context, fatal_stop, fatal_loss = _context()
        fatal_stop.set()
        fatal_loss.set()
        with self.assertRaises(sid_runner.SidRunnerIntegrityError) as raised:
            await fatal_runner.run(_grant("fatal"), _snapshot(), fatal_context)

        self.assertIsInstance(
            raised.exception.__cause__,
            feed_work_scheduler.SchedulerIntegrityError,
        )
        self.assertEqual(
            fatal_scheduler.opened[0].close_reasons,
            [feed_work_scheduler.LaneCloseReason.AUTHORITY_LOSS],
        )
        self.assert_waiters_settled(
            fatal_scheduler,
            fatal_stop,
            fatal_loss,
        )

    async def test_scheduler_fatal_closes_strongly_and_returns_no_outcome(
        self,
    ) -> None:
        sid_runner = _sid_runner_module()
        scheduler = _ControlledScheduler(block_close=True)
        runner, _ = _runner(sid_runner, scheduler)
        context, stop, loss = _context()
        task = asyncio.create_task(runner.run(_grant(), _snapshot(), context))
        await asyncio.wait_for(scheduler.lane_opened.wait(), timeout=1)
        lane = scheduler.opened[0]

        scheduler.fail(RuntimeError("fixed worker failed"))
        await asyncio.wait_for(lane.close_entered.wait(), timeout=1)
        self.assertFalse(task.done())
        self.assertEqual(
            lane.close_reasons,
            [feed_work_scheduler.LaneCloseReason.SCHEDULER_SHUTDOWN],
        )
        lane.release_close.set()

        with self.assertRaises(sid_runner.SidRunnerIntegrityError):
            await task
        self.assert_waiters_settled(scheduler, stop, loss)

    async def test_typed_processor_outcomes_settle_before_exact_close(
        self,
    ) -> None:
        sid_runner = _sid_runner_module()
        rejection = ingestion_lease_store.GrantRejected(
            ingestion_lease_store.GrantRejectionReason.MISSING,
            None,
        )
        cases = (
            (
                sid_processor.SidProcessorFailure(
                    feed_store.FeedStatusReason.SYSTEM_SOURCE_PAYLOAD_INVALID,
                    "bcfy_calls_sid_payload_invalid",
                ),
                feed_work_scheduler.LaneCloseReason.PLANNED_DRAIN,
                grant_control.RunFailed(
                    feed_store.FeedStatusReason.SYSTEM_SOURCE_PAYLOAD_INVALID,
                    "bcfy_calls_sid_payload_invalid",
                ),
            ),
            (
                sid_processor.SidProcessorAuthorityLost(rejection),
                feed_work_scheduler.LaneCloseReason.AUTHORITY_LOSS,
                grant_control.RunLost(),
            ),
            (
                sid_processor.SidProcessorPlannedDrain(uuid.UUID(int=1)),
                feed_work_scheduler.LaneCloseReason.PLANNED_DRAIN,
                grant_control.RunStopped(
                    grant_control.TerminalCause.PLANNED_DRAIN
                ),
            ),
        )
        for terminal, reason, expected in cases:
            with self.subTest(terminal=type(terminal).__name__):
                scheduler = _ControlledScheduler(block_close=True)
                processor = _ControlledProcessor(terminal)
                runner, _ = _runner(sid_runner, scheduler, processor)
                context, stop, loss = _context()
                task = asyncio.create_task(
                    runner.run(_grant(), _snapshot(), context)
                )
                await asyncio.wait_for(processor.started.wait(), timeout=1)
                processor.release.set()
                lane = scheduler.opened[0]
                await asyncio.wait_for(lane.close_entered.wait(), timeout=1)

                self.assertTrue(processor.work_stop.is_set())
                self.assertTrue(processor.settled.is_set())
                self.assertFalse(processor.cancelled)
                self.assertFalse(task.done())
                self.assertEqual(lane.close_reasons, [reason])
                lane.release_close.set()

                self.assertEqual(await task, expected)
                self.assert_waiters_settled(scheduler, stop, loss)

    async def test_all_failure_sources_map_to_generic_run_failed(
        self,
    ) -> None:
        sid_runner = _sid_runner_module()
        for case_id, status_reason, reason in _SID_FAILURE_CASES:
            with self.subTest(case=case_id):
                scheduler = _ControlledScheduler(block_close=True)
                processor = _ControlledProcessor(
                    sid_processor.SidProcessorFailure(status_reason, reason)
                )
                runner, _ = _runner(sid_runner, scheduler, processor)
                context, stop, loss = _context()
                task = asyncio.create_task(
                    runner.run(_grant(case_id), _snapshot(), context)
                )
                await asyncio.wait_for(processor.started.wait(), timeout=1)
                processor.release.set()
                lane = scheduler.opened[0]
                await asyncio.wait_for(lane.close_entered.wait(), timeout=1)

                self.assertTrue(processor.settled.is_set())
                self.assertFalse(task.done())
                self.assertEqual(
                    lane.close_reasons,
                    [feed_work_scheduler.LaneCloseReason.PLANNED_DRAIN],
                )
                lane.release_close.set()

                self.assertEqual(
                    await task,
                    grant_control.RunFailed(status_reason, reason),
                )
                self.assert_waiters_settled(scheduler, stop, loss)

    async def test_monotonic_signals_precede_late_processor_failure(
        self,
    ) -> None:
        sid_runner = _sid_runner_module()
        cases = (
            (
                "stop",
                grant_control.RunStopped(
                    grant_control.TerminalCause.PLANNED_DRAIN
                ),
            ),
            ("loss", grant_control.RunLost()),
        )
        for signal, expected in cases:
            with self.subTest(signal=signal):
                scheduler = _ControlledScheduler()
                processor = _ControlledProcessor(
                    sid_processor.SidProcessorFailure(
                        feed_store.FeedStatusReason.SOURCE_UNREACHABLE,
                        "late metadata failure",
                    )
                )
                runner, _ = _runner(sid_runner, scheduler, processor)
                context, stop, loss = _context()
                task = asyncio.create_task(
                    runner.run(_grant(signal), _snapshot(), context)
                )
                await asyncio.wait_for(processor.started.wait(), timeout=1)
                processor.release.set()
                if signal == "stop":
                    stop.set()
                else:
                    stop.set()
                    loss.set()

                self.assertEqual(await task, expected)
                self.assert_waiters_settled(scheduler, stop, loss)

        scheduler = _ControlledScheduler()
        processor = _ControlledProcessor(
            sid_processor.SidProcessorFailure(
                feed_store.FeedStatusReason.SOURCE_UNREACHABLE,
                "late metadata failure",
            )
        )
        runner, _ = _runner(sid_runner, scheduler, processor)
        context, stop, loss = _context()
        task = asyncio.create_task(runner.run(_grant(), _snapshot(), context))
        await asyncio.wait_for(processor.started.wait(), timeout=1)
        processor.release.set()
        stop.set()
        loss.set()
        scheduler.fail(RuntimeError("scheduler failed"))

        with self.assertRaises(sid_runner.SidRunnerIntegrityError):
            await task
        self.assert_waiters_settled(scheduler, stop, loss)

    async def test_normal_return_and_unknown_processor_failure_are_integrity(
        self,
    ) -> None:
        sid_runner = _sid_runner_module()
        for terminal in (None, RuntimeError("unexpected processor failure")):
            with self.subTest(terminal=repr(terminal)):
                scheduler = _ControlledScheduler()
                processor = _ControlledProcessor(terminal)
                runner, _ = _runner(sid_runner, scheduler, processor)
                context, stop, loss = _context()
                task = asyncio.create_task(
                    runner.run(_grant(), _snapshot(), context)
                )
                await asyncio.wait_for(processor.started.wait(), timeout=1)
                processor.release.set()

                with self.assertRaises(sid_runner.SidRunnerIntegrityError):
                    await task
                self.assertEqual(
                    scheduler.opened[0].close_reasons,
                    [feed_work_scheduler.LaneCloseReason.SCHEDULER_SHUTDOWN],
                )
                self.assert_waiters_settled(scheduler, stop, loss)

    async def test_undrained_and_invalid_close_results_are_integrity_failures(
        self,
    ) -> None:
        sid_runner = _sid_runner_module()
        for result_mode in ("undrained", "invalid", "wrong_grant"):
            with self.subTest(result_mode=result_mode):
                scheduler = _ControlledScheduler(result_mode=result_mode)
                runner, _ = _runner(sid_runner, scheduler)
                context, stop, loss = _context()
                stop.set()

                with self.assertRaises(sid_runner.SidRunnerIntegrityError):
                    await runner.run(_grant(result_mode), _snapshot(), context)

                self.assert_waiters_settled(scheduler, stop, loss)

    async def test_processor_undrained_is_sticky_runner_integrity(self) -> None:
        sid_runner = _sid_runner_module()
        grant = _grant()
        terminal = sid_processor.SidProcessorUndrained(
            feed_work_scheduler.Undrained(
                grant,
                feed_work_scheduler.LaneCloseReason.PLANNED_DRAIN,
            )
        )
        scheduler = _ControlledScheduler()
        processor = _ControlledProcessor(terminal)
        runner, _ = _runner(sid_runner, scheduler, processor)
        context, stop, loss = _context()
        task = asyncio.create_task(runner.run(grant, _snapshot(), context))
        await asyncio.wait_for(processor.started.wait(), timeout=1)
        processor.release.set()

        with self.assertRaises(sid_runner.SidRunnerIntegrityError) as raised:
            await task

        self.assertIs(raised.exception.__cause__, terminal)
        self.assertEqual(
            scheduler.opened[0].close_reasons,
            [feed_work_scheduler.LaneCloseReason.SCHEDULER_SHUTDOWN],
        )
        self.assert_waiters_settled(scheduler, stop, loss)

    async def test_raw_cancellation_cleans_lane_but_preserves_cancelled_error(
        self,
    ) -> None:
        sid_runner = _sid_runner_module()
        scheduler = _ControlledScheduler(block_close=True)
        processor = _ControlledProcessor()
        runner, _ = _runner(sid_runner, scheduler, processor)
        context, stop, loss = _context()
        task = asyncio.create_task(runner.run(_grant(), _snapshot(), context))
        await asyncio.wait_for(scheduler.lane_opened.wait(), timeout=1)
        lane = scheduler.opened[0]
        await asyncio.wait_for(stop.wait_entered.wait(), timeout=1)
        await asyncio.wait_for(loss.wait_entered.wait(), timeout=1)
        await asyncio.wait_for(
            scheduler.integrity_failure_event.wait_entered.wait(),
            timeout=1,
        )

        task.cancel()
        await asyncio.wait_for(lane.close_entered.wait(), timeout=1)
        self.assertEqual(
            lane.close_reasons,
            [feed_work_scheduler.LaneCloseReason.SCHEDULER_SHUTDOWN],
        )
        self.assertTrue(processor.work_stop.is_set())
        self.assertTrue(processor.cancelled)
        self.assertTrue(processor.settled.is_set())
        self.assertFalse(task.done())
        lane.release_close.set()

        with self.assertRaises(asyncio.CancelledError) as caught:
            await task
        handoff = processor.cancellation_handoff
        self.assertIsNotNone(handoff)
        assert handoff is not None
        self.assertIs(handoff.cancelled_error, caught.exception)
        self.assertIs(
            handoff.cause,
            sid_processor.RunnerCancellationCause.RAW_RUNNER_CANCELLATION,
        )
        self.assertGreaterEqual(stop.cancelled_waiters, 1)
        self.assertGreaterEqual(loss.cancelled_waiters, 1)
        self.assert_waiters_settled(scheduler, stop, loss)

    def test_raw_runner_cancellation_cause_requires_exact_lane_loss(
        self,
    ) -> None:
        sid_runner = _sid_runner_module()
        runner, _ = _runner(
            sid_runner,
            _ControlledScheduler(),
            _ControlledProcessor(),
        )
        grant = _grant()
        stop = asyncio.Event()
        loss = asyncio.Event()
        signals = feed_work_scheduler.LaneSignalView(grant, stop, loss)

        self.assertIs(
            runner._runner_cancellation_cause(signals),
            sid_processor.RunnerCancellationCause.RAW_RUNNER_CANCELLATION,
        )
        loss.set()
        self.assertIs(
            runner._runner_cancellation_cause(signals),
            sid_processor.RunnerCancellationCause.EXACT_GRANT_LOSS,
        )

    def test_runner_surface_owns_only_controlled_lane_lifecycle(self) -> None:
        sid_runner = _sid_runner_module()
        source = inspect.getsource(sid_runner)

        self.assertNotIn("TaskGroup", source)
        self.assertNotIn("asyncio.sleep", source)
        self.assertNotIn("cover_page(", source)
        self.assertNotIn("remove_feed(", source)
        self.assertNotIn("refresh_membership(", source)
        self.assertNotIn("fetch_sid_page(", source)
        self.assertNotIn("failure_policy", source)
        self.assertNotIn("http_session", source)
        self.assertNotIn("gcs_client", source)
        self.assertNotIn("pubsub_client", source)
        self.assertIsInstance(
            feed_work_scheduler.FeedWorkScheduler.integrity_failure_event,
            property,
        )
        self.assertTrue(
            callable(feed_work_scheduler.FeedWorkScheduler.raise_if_failed)
        )

    def test_processor_factory_is_required_and_validated(self) -> None:
        sid_runner = _sid_runner_module()
        scheduler = _ControlledScheduler()

        with self.assertRaises(TypeError):
            sid_runner.BcfyCallsSidRunner(scheduler)
        with self.assertRaises(TypeError):
            sid_runner.BcfyCallsSidRunner(scheduler, object())


if __name__ == "__main__":
    unittest.main()
