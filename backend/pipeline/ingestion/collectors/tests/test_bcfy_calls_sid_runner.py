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

import asyncpg

from backend.pipeline.ingestion import (
    feed_work_scheduler,
    grant_control,
)
from backend.pipeline.storage import feed_store, ingestion_lease_store

_OWNER_ID = uuid.UUID("11111111-2222-3333-4444-555555555555")
_NOW = datetime.datetime(2026, 7, 12, 12, 0, tzinfo=datetime.UTC)


def _sid_runner_module():
    return importlib.import_module(
        "backend.pipeline.ingestion.collectors.bcfy_calls.sid_runner"
    )


def _runtime_adapters_module():
    return importlib.import_module(
        "backend.pipeline.ingestion.collectors.bcfy_calls.runtime_adapters"
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


def _snapshot() -> ingestion_lease_store.LeaseSnapshot:
    return ingestion_lease_store.LeaseSnapshot(
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
    )


def _member(
    grant: ingestion_lease_store.LeaseGrant,
    feed_id: uuid.UUID,
    *,
    group_id: str,
) -> ingestion_lease_store.LeaseMemberIdentity:
    return ingestion_lease_store._issue_member_identity(
        grant,
        feed_id=feed_id,
        source_type=feed_store.SourceType.BCFY_CALLS,
        source_feed_id=f"{grant.lease_key}-{group_id}",
        sid=grant.lease_key,
        group_id=group_id,
    )


def _boundary(
    grant: ingestion_lease_store.LeaseGrant,
    feed_id: uuid.UUID,
    *,
    group_id: str,
    offset_seconds: int = 0,
) -> feed_work_scheduler.BoundaryWork:
    return feed_work_scheduler.BoundaryWork(
        member=_member(grant, feed_id, group_id=group_id),
        target=_NOW + datetime.timedelta(seconds=offset_seconds),
    )


def _child_result(
    feed_id: uuid.UUID,
    disposition: ingestion_lease_store.ChildDisposition,
) -> ingestion_lease_store.ChildMutationResult:
    return ingestion_lease_store.ChildMutationResult(
        feed_id=feed_id,
        disposition=disposition,
        cursor_effect=ingestion_lease_store.CursorEffect.ADVANCED,
        lifecycle_effect=ingestion_lease_store.LifecycleEffect.NONE,
    )


def _batch_committed(
    *children: ingestion_lease_store.ChildMutationResult,
) -> ingestion_lease_store.BatchCommitted:
    snapshot = _snapshot()
    return ingestion_lease_store.BatchCommitted(
        lease_effect=ingestion_lease_store.LeaseLifecycleResult(
            effect=ingestion_lease_store.LeaseLifecycleEffect.NONE,
            before_snapshot=snapshot,
            after_snapshot=snapshot,
        ),
        children=children,
    )


def _store_with_result(
    result: object,
) -> ingestion_lease_store.IngestionLeaseStore:
    store = unittest.mock.create_autospec(
        ingestion_lease_store.IngestionLeaseStore,
        instance=True,
    )
    store.commit_child_mutations = unittest.mock.AsyncMock(
        return_value=result
    )
    return typing.cast("ingestion_lease_store.IngestionLeaseStore", store)


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
    ) -> None:
        self.scheduler = scheduler
        self.grant = grant
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
        self.lane_opened = asyncio.Event()
        self.two_lanes_opened = asyncio.Event()
        self.close_calls = 0
        self.two_closes_entered = asyncio.Event()

    def open_lane(
        self,
        grant: ingestion_lease_store.LeaseGrant,
    ) -> _ControlledLane:
        lane = _ControlledLane(self, grant)
        self.opened.append(lane)
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

    async def test_concurrent_runs_open_distinct_invocation_local_lanes(
        self,
    ) -> None:
        sid_runner = _sid_runner_module()
        scheduler = _ControlledScheduler(block_close=True)
        runner = sid_runner.BcfyCallsSidRunner(scheduler)
        first_context, first_stop, first_loss = _context()
        second_context, second_stop, second_loss = _context()
        first_grant = _grant("150", fencing_token=1)
        second_grant = _grant("151", fencing_token=2)

        first = asyncio.create_task(
            runner.run(first_grant, _snapshot(), first_context)
        )
        second = asyncio.create_task(
            runner.run(second_grant, _snapshot(), second_context)
        )
        await asyncio.wait_for(scheduler.two_lanes_opened.wait(), timeout=1)

        self.assertEqual(
            [lane.grant for lane in scheduler.opened],
            [first_grant, second_grant],
        )
        self.assertIsNot(scheduler.opened[0], scheduler.opened[1])
        self.assertEqual(runner.__slots__, ("_scheduler",))
        self.assertFalse(hasattr(runner, "current_lane"))
        self.assertFalse(hasattr(runner, "_current_lane"))

        first_stop.set()
        second_stop.set()
        await asyncio.wait_for(
            scheduler.two_closes_entered.wait(),
            timeout=1,
        )
        for lane in scheduler.opened:
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
                runner = sid_runner.BcfyCallsSidRunner(scheduler)
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
                lane.release_close.set()
                self.assertEqual(await task, expected)
                self.assert_waiters_settled(scheduler, stop, loss)

    async def test_stop_escalates_to_loss_before_acknowledgement(self) -> None:
        sid_runner = _sid_runner_module()
        scheduler = _ControlledScheduler(block_close=True)
        runner = sid_runner.BcfyCallsSidRunner(scheduler)
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
        runner = sid_runner.BcfyCallsSidRunner(scheduler)
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
        fatal_runner = sid_runner.BcfyCallsSidRunner(fatal_scheduler)
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
        runner = sid_runner.BcfyCallsSidRunner(scheduler)
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

    async def test_undrained_and_invalid_close_results_are_integrity_failures(
        self,
    ) -> None:
        sid_runner = _sid_runner_module()
        for result_mode in ("undrained", "invalid", "wrong_grant"):
            with self.subTest(result_mode=result_mode):
                scheduler = _ControlledScheduler(result_mode=result_mode)
                runner = sid_runner.BcfyCallsSidRunner(scheduler)
                context, stop, loss = _context()
                stop.set()

                with self.assertRaises(sid_runner.SidRunnerIntegrityError):
                    await runner.run(_grant(result_mode), _snapshot(), context)

                self.assert_waiters_settled(scheduler, stop, loss)

    async def test_raw_cancellation_cleans_lane_but_preserves_cancelled_error(
        self,
    ) -> None:
        sid_runner = _sid_runner_module()
        scheduler = _ControlledScheduler(block_close=True)
        runner = sid_runner.BcfyCallsSidRunner(scheduler)
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
        self.assertFalse(task.done())
        lane.release_close.set()

        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertGreaterEqual(stop.cancelled_waiters, 1)
        self.assertGreaterEqual(loss.cancelled_waiters, 1)
        self.assert_waiters_settled(scheduler, stop, loss)

    def test_runner_surface_owns_only_controlled_lane_lifecycle(self) -> None:
        sid_runner = _sid_runner_module()
        source = inspect.getsource(sid_runner)

        self.assertNotIn("TaskGroup", source)
        self.assertNotIn("asyncio.sleep", source)
        self.assertNotIn("cover_page(", source)
        self.assertNotIn("remove_feed(", source)
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


class TestFencedBoundaryCommitter(unittest.IsolatedAsyncioTestCase):
    """Prove quiet boundaries make one exact fenced storage attempt."""

    async def test_nonempty_mutation_translation_preserves_exact_order(
        self,
    ) -> None:
        runtime_adapters = _runtime_adapters_module()
        grant = _grant()
        boundaries = tuple(
            _boundary(
                grant,
                uuid.UUID(int=index + 1),
                group_id=str(index + 1),
                offset_seconds=index,
            )
            for index in range(3)
        )
        dispositions = (
            ingestion_lease_store.ChildDisposition.APPLIED,
            ingestion_lease_store.ChildDisposition.APPLIED_AFTER_DEACTIVATION,
            ingestion_lease_store.ChildDisposition.ACCEPTED_NOOP,
        )
        store = _store_with_result(
            _batch_committed(
                *(
                    _child_result(boundary.feed_id, disposition)
                    for boundary, disposition in zip(
                        boundaries,
                        dispositions,
                        strict=True,
                    )
                )
            )
        )
        committer = runtime_adapters.FencedBoundaryCommitter(
            store,
            actor_id="service_account:gcp:collector",
        )

        result = await committer.commit(
            grant,
            boundaries,
            final_logical=False,
        )

        self.assertEqual(
            result,
            feed_work_scheduler.BoundaryBatchCommitted(
                tuple(
                    feed_work_scheduler.BoundaryResult(
                        boundary,
                        feed_work_scheduler.BoundaryDisposition.COMMITTED,
                    )
                    for boundary in boundaries
                )
            ),
        )
        store.commit_child_mutations.assert_awaited_once()
        call = store.commit_child_mutations.await_args
        self.assertEqual(call.args[0], grant)
        self.assertEqual(
            call.kwargs,
            {"actor_id": "service_account:gcp:collector"},
        )
        batch = call.args[1]
        self.assertIsInstance(
            batch.lease_effect,
            ingestion_lease_store.NoLeaseEffect,
        )
        self.assertEqual(
            tuple(type(mutation) for mutation in batch.mutations),
            (ingestion_lease_store.SourceObservation,) * 3,
        )
        self.assertEqual(
            tuple(mutation.member for mutation in batch.mutations),
            tuple(boundary.member for boundary in boundaries),
        )
        self.assertEqual(
            tuple(mutation.cursor for mutation in batch.mutations),
            tuple(boundary.target for boundary in boundaries),
        )
        store.load_membership.assert_not_called()
        store.refresh_membership.assert_not_called()

    async def test_empty_final_boundary_still_attempts_one_mutation(self) -> None:
        runtime_adapters = _runtime_adapters_module()
        grant = _grant()
        store = _store_with_result(_batch_committed())
        committer = runtime_adapters.FencedBoundaryCommitter(
            store,
            actor_id="service_account:gcp:collector",
        )

        result = await committer.commit(grant, (), final_logical=True)

        self.assertEqual(
            result,
            feed_work_scheduler.BoundaryBatchCommitted(()),
        )
        store.commit_child_mutations.assert_awaited_once()
        batch = store.commit_child_mutations.await_args.args[1]
        self.assertEqual(batch.mutations, ())
        self.assertIsInstance(
            batch.lease_effect,
            ingestion_lease_store.NoLeaseEffect,
        )

    async def test_exact_fence_rejection_is_batch_rejection(self) -> None:
        runtime_adapters = _runtime_adapters_module()
        store = _store_with_result(
            ingestion_lease_store.GrantRejected(
                ingestion_lease_store.GrantRejectionReason.MISSING,
                None,
            )
        )
        committer = runtime_adapters.FencedBoundaryCommitter(
            store,
            actor_id="service_account:gcp:collector",
        )

        result = await committer.commit(_grant(), (), final_logical=True)

        self.assertEqual(
            result,
            feed_work_scheduler.BoundaryGrantRejected(),
        )
        store.commit_child_mutations.assert_awaited_once()

    async def test_missing_and_ineligible_members_localize_in_order(
        self,
    ) -> None:
        runtime_adapters = _runtime_adapters_module()
        grant = _grant()
        boundaries = (
            _boundary(grant, uuid.UUID(int=1), group_id="1"),
            _boundary(grant, uuid.UUID(int=2), group_id="2"),
        )
        store = _store_with_result(
            _batch_committed(
                _child_result(
                    boundaries[0].feed_id,
                    ingestion_lease_store.ChildDisposition.MISSING,
                ),
                _child_result(
                    boundaries[1].feed_id,
                    ingestion_lease_store.ChildDisposition.STATUS_INELIGIBLE,
                ),
            )
        )
        committer = runtime_adapters.FencedBoundaryCommitter(
            store,
            actor_id="service_account:gcp:collector",
        )

        result = await committer.commit(
            grant,
            boundaries,
            final_logical=False,
        )

        self.assertEqual(
            tuple(item.boundary for item in result.results),
            boundaries,
        )
        self.assertEqual(
            tuple(item.disposition for item in result.results),
            (
                feed_work_scheduler.BoundaryDisposition.MEMBER_REJECTED,
                feed_work_scheduler.BoundaryDisposition.MEMBER_REJECTED,
            ),
        )

    async def test_transient_storage_failures_are_batch_retryable(self) -> None:
        runtime_adapters = _runtime_adapters_module()
        failures = (
            asyncpg.PostgresConnectionError("connection lost"),
            asyncpg.InterfaceError("connection unavailable"),
            OSError("socket failed"),
        )

        for failure in failures:
            with self.subTest(failure=type(failure).__name__):
                store = _store_with_result(object())
                store.commit_child_mutations.side_effect = failure
                committer = runtime_adapters.FencedBoundaryCommitter(
                    store,
                    actor_id="service_account:gcp:collector",
                )

                result = await committer.commit(
                    _grant(),
                    (),
                    final_logical=True,
                )

                self.assertEqual(
                    result,
                    feed_work_scheduler.BoundaryBatchRetryable(),
                )
                store.commit_child_mutations.assert_awaited_once()

    async def test_cancellation_and_nontransient_failures_propagate(
        self,
    ) -> None:
        runtime_adapters = _runtime_adapters_module()
        failures = (
            asyncio.CancelledError(),
            TypeError("programming error"),
            asyncpg.UniqueViolationError("constraint failed"),
        )

        for failure in failures:
            with self.subTest(failure=type(failure).__name__):
                store = _store_with_result(object())
                store.commit_child_mutations.side_effect = failure
                committer = runtime_adapters.FencedBoundaryCommitter(
                    store,
                    actor_id="service_account:gcp:collector",
                )

                with self.assertRaises(type(failure)):
                    await committer.commit(
                        _grant(),
                        (),
                        final_logical=True,
                    )

                store.commit_child_mutations.assert_awaited_once()

    async def test_malformed_mutation_correlation_is_integrity_failure(
        self,
    ) -> None:
        runtime_adapters = _runtime_adapters_module()
        grant = _grant()
        boundaries = (
            _boundary(grant, uuid.UUID(int=1), group_id="1"),
            _boundary(grant, uuid.UUID(int=2), group_id="2"),
        )
        cases = (
            _batch_committed(
                _child_result(
                    boundaries[0].feed_id,
                    ingestion_lease_store.ChildDisposition.APPLIED,
                )
            ),
            _batch_committed(
                _child_result(
                    boundaries[1].feed_id,
                    ingestion_lease_store.ChildDisposition.APPLIED,
                ),
                _child_result(
                    boundaries[0].feed_id,
                    ingestion_lease_store.ChildDisposition.APPLIED,
                ),
            ),
            _batch_committed(
                ingestion_lease_store.ChildMutationResult(
                    feed_id=boundaries[0].feed_id,
                    disposition=object(),  # type: ignore[arg-type]
                    cursor_effect=ingestion_lease_store.CursorEffect.ADVANCED,
                    lifecycle_effect=ingestion_lease_store.LifecycleEffect.NONE,
                ),
                _child_result(
                    boundaries[1].feed_id,
                    ingestion_lease_store.ChildDisposition.APPLIED,
                ),
            ),
        )

        for result in cases:
            with self.subTest(result=result):
                store = _store_with_result(result)
                committer = runtime_adapters.FencedBoundaryCommitter(
                    store,
                    actor_id="service_account:gcp:collector",
                )

                with self.assertRaises(
                    runtime_adapters.BoundaryAdapterIntegrityError
                ):
                    await committer.commit(
                        grant,
                        boundaries,
                        final_logical=False,
                    )

    async def test_boundary_input_shape_is_validated_before_mutation(
        self,
    ) -> None:
        runtime_adapters = _runtime_adapters_module()
        store = _store_with_result(_batch_committed())
        committer = runtime_adapters.FencedBoundaryCommitter(
            store,
            actor_id="service_account:gcp:collector",
        )

        with self.assertRaises(TypeError):
            await committer.commit(
                _grant(),
                [],  # type: ignore[arg-type]
                final_logical=True,
            )
        with self.assertRaises(TypeError):
            await committer.commit(
                _grant(),
                (object(),),  # type: ignore[arg-type]
                final_logical=True,
            )
        with self.assertRaises(TypeError):
            await committer.commit(
                _grant(),
                (),
                final_logical=1,  # type: ignore[arg-type]
            )

        store.commit_child_mutations.assert_not_awaited()


if __name__ == "__main__":
    unittest.main()
