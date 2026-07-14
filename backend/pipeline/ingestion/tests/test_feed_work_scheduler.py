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

type _ExecutorOutcome = (
    feed_work_scheduler.CallCompleted
    | feed_work_scheduler.CallFinalClosurePending
    | feed_work_scheduler.CallReplayableDirectFailure
    | feed_work_scheduler.CallRetryable
    | feed_work_scheduler.CallStopped
    | feed_work_scheduler.CallAuthorityLost
    | feed_work_scheduler.CallMembershipRejected
    | feed_work_scheduler.CallIntegrityFailure
    | feed_work_scheduler.CallOutcomeUnknown
)
type _FinalPageResult = (
    feed_work_scheduler.FinalPageCovered
    | feed_work_scheduler.FinalPageNoProgress
    | feed_work_scheduler.FinalPageReplayable
    | feed_work_scheduler.FinalPageRetryable
    | feed_work_scheduler.FinalPageGrantRejected
    | feed_work_scheduler.FinalPageOutcomeUnknown
)


def _require_not_none[T](value: T | None) -> T:
    if value is None:
        message = "test expected a non-None value"
        raise TypeError(message)
    return value


def _require_settled(
    result: feed_work_scheduler.SettledPage | feed_work_scheduler.Undrained,
) -> feed_work_scheduler.SettledPage:
    if not isinstance(result, feed_work_scheduler.SettledPage):
        message = "test expected a settled page"
        raise TypeError(message)
    return result


def _covered_settlement(
    result: feed_work_scheduler.SettledPage | feed_work_scheduler.Undrained,
) -> cursor_policy._CoveredPage:
    settlement = _require_settled(result).lease_settlement
    if not isinstance(settlement, cursor_policy._CoveredPage):
        message = "test expected a covered settlement"
        raise TypeError(message)
    return settlement


def _replayable_settlement(
    result: feed_work_scheduler.SettledPage | feed_work_scheduler.Undrained,
) -> cursor_policy._ReplayablePageSettled:
    settlement = _require_settled(result).lease_settlement
    if not isinstance(settlement, cursor_policy._ReplayablePageSettled):
        message = "test expected a replayable settlement"
        raise TypeError(message)
    return settlement


def _no_progress_settlement(
    result: feed_work_scheduler.SettledPage | feed_work_scheduler.Undrained,
) -> cursor_policy._NoProgressPageSettled:
    settlement = _require_settled(result).lease_settlement
    if not isinstance(settlement, cursor_policy._NoProgressPageSettled):
        message = "test expected a no-progress settlement"
        raise TypeError(message)
    return settlement


def _scheduler_types() -> typing.Any:
    return importlib.import_module(
        "backend.pipeline.ingestion.feed_work_scheduler._types"
    )


def _source_order(payload: object) -> int:
    if not isinstance(payload, dict):
        message = "test payload must be a dict"
        raise TypeError(message)
    source_order = typing.cast("dict[object, object]", payload).get(
        "source_order"
    )
    if not isinstance(source_order, int):
        message = "test payload source_order must be an int"
        raise TypeError(message)
    return source_order


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


def _open_lane(
    scheduler: feed_work_scheduler.FeedWorkScheduler,
    grant: ingestion_lease_store.LeaseGrant,
    *,
    stop_requested: asyncio.Event | None = None,
    grant_lost: asyncio.Event | None = None,
) -> feed_work_scheduler.GrantLane:
    return scheduler.open_lane(
        grant,
        stop_requested=(
            asyncio.Event() if stop_requested is None else stop_requested
        ),
        grant_lost=asyncio.Event() if grant_lost is None else grant_lost,
    )


def _submission(
    feed_id: uuid.UUID,
    source_order: int,
    *,
    grant: ingestion_lease_store.LeaseGrant | None = None,
    source_timestamp: datetime.datetime | None = _SOURCE_TIME,
    settlement_observer: typing.Callable[[object], None] | None = None,
    member: ingestion_lease_store.LeaseMemberIdentity | None = None,
) -> feed_work_scheduler.CohortSubmission:
    exact_grant = _grant() if grant is None else grant
    exact_member = _member(exact_grant, feed_id) if member is None else member
    timestamp = (
        None
        if source_timestamp is None
        else source_timestamp + datetime.timedelta(seconds=source_order)
    )
    call = feed_work_scheduler.CallSubmission(
        feed_id=feed_id,
        source_timestamp=timestamp,
        payload={"source_order": source_order, "member": exact_member},
        settlement_observer=settlement_observer,
    )
    return feed_work_scheduler.CohortSubmission(
        member=exact_member,
        feed_id=feed_id,
        cohort_timestamp=timestamp,
        calls=(call,),
        admission_hook=lambda _identities: None,
    )


def _cohort(
    feed_id: uuid.UUID,
    source_orders: tuple[int, ...],
    *,
    grant: ingestion_lease_store.LeaseGrant,
    cohort_timestamp: datetime.datetime | None,
    member: ingestion_lease_store.LeaseMemberIdentity | None = None,
    payload_member: ingestion_lease_store.LeaseMemberIdentity | None = None,
    admission_hook: typing.Callable[
        [tuple[feed_work_scheduler.CohortRecordIdentity, ...]],
        None,
    ]
    | None = None,
    settlement_observers: dict[
        int,
        typing.Callable[[object], None],
    ]
    | None = None,
) -> feed_work_scheduler.CohortSubmission:
    exact_member = _member(grant, feed_id) if member is None else member
    exact_payload_member = (
        exact_member if payload_member is None else payload_member
    )
    observers = {} if settlement_observers is None else settlement_observers
    calls = tuple(
        feed_work_scheduler.CallSubmission(
            feed_id=feed_id,
            source_timestamp=cohort_timestamp,
            payload={
                "source_order": source_order,
                "member": exact_payload_member,
            },
            settlement_observer=observers.get(source_order),
        )
        for source_order in source_orders
    )
    return feed_work_scheduler.CohortSubmission(
        member=exact_member,
        feed_id=feed_id,
        cohort_timestamp=cohort_timestamp,
        calls=calls,
        admission_hook=(
            (lambda _identities: None)
            if admission_hook is None
            else admission_hook
        ),
    )


def _terminal_facts(
    execution: feed_work_scheduler.CohortExecution,
    *,
    disposition: feed_work_scheduler.CohortTerminalDisposition = (
        feed_work_scheduler.CohortTerminalDisposition.SETTLED
    ),
    closure_state: feed_work_scheduler.CohortRecordClosureState = (
        feed_work_scheduler.CohortRecordClosureState.DURABLY_CLOSED
    ),
    reason: feed_work_scheduler.CohortRecordTerminalReason = (
        feed_work_scheduler.CohortRecordTerminalReason.FULL_PIPELINE
    ),
    participated: bool = True,
    item_failure: feed_work_scheduler.CohortItemFailureFact | None = None,
    direct_failure: feed_work_scheduler.CohortDirectFailureFact | None = None,
) -> feed_work_scheduler.CohortTerminalFacts:
    return feed_work_scheduler.CohortTerminalFacts(
        records=tuple(
            feed_work_scheduler.CohortRecordTerminalFact(
                identity=call.identity,
                participated=participated,
                closure_state=closure_state,
                full_pipeline_completed=(
                    reason
                    is feed_work_scheduler.CohortRecordTerminalReason.FULL_PIPELINE
                ),
                terminal_reason=reason,
                item_failure=item_failure,
                direct_failure=direct_failure,
            )
            for call in execution.calls
        ),
        disposition=disposition,
    )


def _completed(
    execution: feed_work_scheduler.CohortExecution,
) -> feed_work_scheduler.CallCompleted:
    return feed_work_scheduler.CallCompleted(_terminal_facts(execution))


def _retryable(
    execution: feed_work_scheduler.CohortExecution,
) -> feed_work_scheduler.CallRetryable:
    return feed_work_scheduler.CallRetryable(
        _terminal_facts(
            execution,
            disposition=feed_work_scheduler.CohortTerminalDisposition.RETRYABLE,
            closure_state=(
                feed_work_scheduler.CohortRecordClosureState.REPLAY_SAFE_RELEASE
            ),
            reason=feed_work_scheduler.CohortRecordTerminalReason.RETRYABLE,
            participated=False,
        )
    )


def _authority_lost(
    execution: feed_work_scheduler.CohortExecution,
) -> feed_work_scheduler.CallAuthorityLost:
    return feed_work_scheduler.CallAuthorityLost(
        _terminal_facts(
            execution,
            disposition=(
                feed_work_scheduler.CohortTerminalDisposition.AUTHORITY_LOST
            ),
            closure_state=(
                feed_work_scheduler.CohortRecordClosureState.REPLAY_SAFE_RELEASE
            ),
            reason=(
                feed_work_scheduler.CohortRecordTerminalReason.AUTHORITY_LOST
            ),
            participated=False,
        )
    )


def _membership_rejected(
    execution: feed_work_scheduler.CohortExecution,
) -> feed_work_scheduler.CallMembershipRejected:
    return feed_work_scheduler.CallMembershipRejected(
        _terminal_facts(
            execution,
            disposition=(
                feed_work_scheduler.CohortTerminalDisposition.MEMBERSHIP_REJECTED
            ),
            closure_state=(
                feed_work_scheduler.CohortRecordClosureState.REPLAY_SAFE_RELEASE
            ),
            reason=(
                feed_work_scheduler.CohortRecordTerminalReason.MEMBERSHIP_REJECTED
            ),
            participated=False,
        )
    )


def _replayable_direct(
    execution: feed_work_scheduler.CohortExecution,
) -> feed_work_scheduler.CallReplayableDirectFailure:
    direct = feed_work_scheduler.CohortDirectFailureFact(
        feed_store.FeedStatusReason.SYSTEM_PIPELINE_ERROR,
        "selected direct precommit failure",
    )
    return feed_work_scheduler.CallReplayableDirectFailure(
        _terminal_facts(
            execution,
            disposition=(
                feed_work_scheduler.CohortTerminalDisposition.REPLAYABLE_DIRECT
            ),
            closure_state=(
                feed_work_scheduler.CohortRecordClosureState.REPLAY_SAFE_RELEASE
            ),
            reason=(
                feed_work_scheduler.CohortRecordTerminalReason.REPLAYABLE_DIRECT
            ),
            participated=False,
            direct_failure=direct,
        )
    )


def _final_closure_pending(
    execution: feed_work_scheduler.CohortExecution,
) -> feed_work_scheduler.CallFinalClosurePending:
    item = feed_work_scheduler.CohortItemFailureFact(
        feed_store.FeedStatusReason.SYSTEM_SOURCE_PAYLOAD_INVALID,
        "terminal item skip",
    )
    return feed_work_scheduler.CallFinalClosurePending(
        _terminal_facts(
            execution,
            disposition=(
                feed_work_scheduler.CohortTerminalDisposition.FINAL_CLOSURE_PENDING
            ),
            closure_state=(
                feed_work_scheduler.CohortRecordClosureState.FINAL_CLOSURE_PENDING
            ),
            reason=(
                feed_work_scheduler.CohortRecordTerminalReason.TERMINAL_ITEM_SKIP
            ),
            item_failure=item,
        )
    )


def _stopped(
    execution: feed_work_scheduler.CohortExecution,
) -> feed_work_scheduler.CallStopped:
    return feed_work_scheduler.CallStopped(
        _terminal_facts(
            execution,
            disposition=feed_work_scheduler.CohortTerminalDisposition.STOPPED,
            closure_state=(
                feed_work_scheduler.CohortRecordClosureState.REPLAY_SAFE_RELEASE
            ),
            reason=feed_work_scheduler.CohortRecordTerminalReason.STOPPED,
            participated=False,
        )
    )


def _outcome_unknown(
    execution: feed_work_scheduler.CohortExecution,
) -> feed_work_scheduler.CallOutcomeUnknown:
    return feed_work_scheduler.CallOutcomeUnknown(
        _terminal_facts(
            execution,
            disposition=(
                feed_work_scheduler.CohortTerminalDisposition.OUTCOME_UNKNOWN
            ),
            closure_state=(
                feed_work_scheduler.CohortRecordClosureState.OUTCOME_UNKNOWN
            ),
            reason=(
                feed_work_scheduler.CohortRecordTerminalReason.OUTCOME_UNKNOWN
            ),
            participated=True,
        )
    )


class _ImmediateExecutor:
    def __init__(self) -> None:
        self.sequences: list[int] = []

    async def execute(
        self,
        execution: feed_work_scheduler.CohortExecution,
    ) -> _ExecutorOutcome:
        if not isinstance(execution, feed_work_scheduler.CohortExecution):
            message = "executor received a private scheduler record"
            raise TypeError(message)
        self.sequences.extend(
            _source_order(call.payload) for call in execution.calls
        )
        return _completed(execution)


class _GateExecutor:
    """Event-gated executor with no timing or task-global assertions."""

    def __init__(self) -> None:
        self.started: list[int] = []
        self.changed = asyncio.Event()
        self._release: dict[int, asyncio.Event] = {}
        self._released = 0
        self._release_all = False

    async def execute(
        self,
        execution: feed_work_scheduler.CohortExecution,
    ) -> _ExecutorOutcome:
        if not isinstance(execution, feed_work_scheduler.CohortExecution):
            message = "executor received a private scheduler record"
            raise TypeError(message)
        sequence = len(self.started)
        event = self._release.setdefault(sequence, asyncio.Event())
        self.started.append(sequence)
        self.changed.set()
        if not self._release_all:
            await event.wait()
        return _completed(execution)

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


class _ControlledClock:
    """Deterministic monotonic clock advanced only by a test owner."""

    def __init__(self) -> None:
        self.value = 0.0

    def __call__(self) -> float:
        return self.value

    def advance(self, seconds: float) -> None:
        self.value += seconds


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

    async def execute(
        self,
        execution: feed_work_scheduler.CohortExecution,
    ) -> _ExecutorOutcome:
        if not isinstance(execution, feed_work_scheduler.CohortExecution):
            message = "executor received a private scheduler record"
            raise TypeError(message)
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
                return _completed(execution)
            raise
        message = "an unset Event unexpectedly completed"
        raise AssertionError(message)

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

    def __init__(
        self,
        outcome_factory: typing.Callable[
            [feed_work_scheduler.CohortExecution],
            _ExecutorOutcome,
        ],
    ) -> None:
        self.outcome_factory = outcome_factory
        self.entered = asyncio.Event()
        self.release = asyncio.Event()
        self.calls = 0
        self.executions: list[feed_work_scheduler.CohortExecution] = []

    async def execute(
        self,
        execution: feed_work_scheduler.CohortExecution,
    ) -> _ExecutorOutcome:
        if not isinstance(execution, feed_work_scheduler.CohortExecution):
            message = "executor received a private scheduler record"
            raise TypeError(message)
        self.calls += 1
        self.executions.append(execution)
        self.entered.set()
        await self.release.wait()
        return self.outcome_factory(execution)


class _ReplayBarrierExecutor:
    """Gate the first Feed cohort while siblings remain runnable."""

    def __init__(self, failing_feed: uuid.UUID) -> None:
        self.failing_feed = failing_feed
        self.started: list[tuple[int, ...]] = []
        self.changed = asyncio.Event()
        self.failure_entered = asyncio.Event()
        self.release_failure = asyncio.Event()

    async def execute(
        self,
        execution: feed_work_scheduler.CohortExecution,
    ) -> _ExecutorOutcome:
        if not isinstance(execution, feed_work_scheduler.CohortExecution):
            message = "executor received a private scheduler record"
            raise TypeError(message)
        orders = tuple(_source_order(call.payload) for call in execution.calls)
        self.started.append(orders)
        self.changed.set()
        first = execution.calls[0]
        if first.feed_id == self.failing_feed and orders[0] == 0:
            self.failure_entered.set()
            await self.release_failure.wait()
            return _replayable_direct(execution)
        return _completed(execution)

    async def wait_for_started(self, count: int) -> None:
        while len(self.started) < count:
            self.changed.clear()
            if len(self.started) >= count:
                return
            await asyncio.wait_for(self.changed.wait(), timeout=1)


class _PageAbortExecutor:
    """Retain one final-pending cohort before settling another outcome."""

    def __init__(
        self,
        abort_factory: typing.Callable[
            [feed_work_scheduler.CohortExecution],
            _ExecutorOutcome,
        ],
    ) -> None:
        self.abort_factory = abort_factory
        self.release_abort = asyncio.Event()
        self.abort_entered = asyncio.Event()

    async def execute(
        self,
        execution: feed_work_scheduler.CohortExecution,
    ) -> _ExecutorOutcome:
        if not isinstance(execution, feed_work_scheduler.CohortExecution):
            message = "executor received a private scheduler record"
            raise TypeError(message)
        source_order = _source_order(execution.calls[0].payload)
        if source_order == 0:
            return _final_closure_pending(execution)
        self.abort_entered.set()
        await self.release_abort.wait()
        return self.abort_factory(execution)


class _CancellationCapabilityExecutor:
    """Use exactly one scheduler-minted cancellation capability."""

    def __init__(self, *, unknown: bool) -> None:
        self.unknown = unknown
        self.entered = asyncio.Event()
        self.handoff_complete = asyncio.Event()
        self.caught: asyncio.CancelledError | None = None
        self.reraised_same_object = False

    async def execute(
        self,
        execution: feed_work_scheduler.CohortExecution,
    ) -> _ExecutorOutcome:
        if not isinstance(execution, feed_work_scheduler.CohortExecution):
            message = "executor received a private scheduler record"
            raise TypeError(message)
        self.entered.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError as exc:
            self.caught = exc
            if self.unknown:
                facts = _outcome_unknown(execution).facts
                request = feed_work_scheduler.OutcomeUnknownRetentionRequest(
                    feed_work_scheduler.OutcomeUnknownCause.COMMIT,
                    facts,
                )
                execution.retention.retain(request)
                execution.retention.retain(request)
            else:
                outcome = _completed(execution)
                execution.cancellation_handoff.settle(outcome)
                execution.cancellation_handoff.settle(outcome)
            self.handoff_complete.set()
            try:
                raise
            except asyncio.CancelledError as propagated:
                self.reraised_same_object = propagated is exc
                raise
        message = "an unset Event unexpectedly completed"
        raise AssertionError(message)


class _HoldFirstThenCompleteExecutor:
    """Keep the first canceled-page cohort active during its retry."""

    def __init__(self) -> None:
        self.first_entered = asyncio.Event()
        self.release_first = asyncio.Event()
        self.calls = 0

    async def execute(
        self,
        execution: feed_work_scheduler.CohortExecution,
    ) -> _ExecutorOutcome:
        if not isinstance(execution, feed_work_scheduler.CohortExecution):
            message = "executor received a private scheduler record"
            raise TypeError(message)
        self.calls += 1
        if self.calls > 1:
            return _completed(execution)
        self.first_entered.set()
        await self.release_first.wait()
        return _completed(execution)


class _UnknownPageAbortExecutor:
    """Retain known-final and unknown work before a stopped sibling."""

    def __init__(self) -> None:
        self.abort_entered = asyncio.Event()
        self.release_abort = asyncio.Event()

    async def execute(
        self,
        execution: feed_work_scheduler.CohortExecution,
    ) -> _ExecutorOutcome:
        if not isinstance(execution, feed_work_scheduler.CohortExecution):
            message = "executor received a private scheduler record"
            raise TypeError(message)
        source_order = _source_order(execution.calls[0].payload)
        if source_order == 0:
            return _final_closure_pending(execution)
        if source_order == 1:
            return _outcome_unknown(execution)
        self.abort_entered.set()
        await self.release_abort.wait()
        return _stopped(execution)


class _AbortBeforeUnknownExecutor:
    """Return STOPPED before a later cohort reveals unknown outcome."""

    def __init__(self) -> None:
        self.stopped_returned = asyncio.Event()
        self.unknown_entered = asyncio.Event()
        self.release_unknown = asyncio.Event()

    async def execute(
        self,
        execution: feed_work_scheduler.CohortExecution,
    ) -> _ExecutorOutcome:
        if not isinstance(execution, feed_work_scheduler.CohortExecution):
            message = "executor received a private scheduler record"
            raise TypeError(message)
        source_order = _source_order(execution.calls[0].payload)
        if source_order == 0:
            return _final_closure_pending(execution)
        if source_order == 1:
            self.stopped_returned.set()
            return _stopped(execution)
        self.unknown_entered.set()
        await self.release_unknown.wait()
        return _outcome_unknown(execution)


class _FinalPendingWithGatedSiblingExecutor:
    """Hold a sibling after an earlier Feed becomes final-pending."""

    def __init__(self) -> None:
        self.pending_returned = asyncio.Event()
        self.sibling_entered = asyncio.Event()
        self.release_sibling = asyncio.Event()

    async def execute(
        self,
        execution: feed_work_scheduler.CohortExecution,
    ) -> _ExecutorOutcome:
        if not isinstance(execution, feed_work_scheduler.CohortExecution):
            message = "executor received a private scheduler record"
            raise TypeError(message)
        source_order = _source_order(execution.calls[0].payload)
        if source_order == 0:
            self.pending_returned.set()
            return _final_closure_pending(execution)
        self.sibling_entered.set()
        await self.release_sibling.wait()
        return _completed(execution)


class _CrossedTerminalFactsExecutor:
    """Return structurally legal facts for the wrong exact identity."""

    async def execute(
        self,
        execution: feed_work_scheduler.CohortExecution,
    ) -> _ExecutorOutcome:
        if not isinstance(execution, feed_work_scheduler.CohortExecution):
            message = "executor received a private scheduler record"
            raise TypeError(message)
        call = execution.calls[0]
        crossed = dataclasses.replace(
            call.identity,
            local_sequence=call.identity.local_sequence + 1,
        )
        facts = feed_work_scheduler.CohortTerminalFacts(
            records=(
                feed_work_scheduler.CohortRecordTerminalFact(
                    identity=crossed,
                    participated=True,
                    closure_state=(
                        feed_work_scheduler.CohortRecordClosureState.DURABLY_CLOSED
                    ),
                    full_pipeline_completed=True,
                    terminal_reason=(
                        feed_work_scheduler.CohortRecordTerminalReason.FULL_PIPELINE
                    ),
                ),
            ),
            disposition=feed_work_scheduler.CohortTerminalDisposition.SETTLED,
        )
        return feed_work_scheduler.CallCompleted(facts)


def _accepted_boundary_results(
    context: feed_work_scheduler.PageFinalizationContext,
) -> tuple[feed_work_scheduler.BoundaryResult, ...]:
    return tuple(
        feed_work_scheduler.BoundaryResult(
            boundary,
            feed_work_scheduler.BoundaryDisposition.COMMITTED,
        )
        for boundary in context.candidate_boundaries
    )


def _covered_final_page(
    context: feed_work_scheduler.PageFinalizationContext,
    *,
    resolutions: tuple[
        feed_work_scheduler.FinalRecordClosureResolution,
        ...,
    ] = (),
    source_evidence: object = None,
    member_retirements: tuple[
        ingestion_lease_store.LeaseMemberIdentity,
        ...,
    ]
    | None = None,
) -> feed_work_scheduler.FinalPageCovered:
    return feed_work_scheduler.FinalPageCovered(
        grant=context.grant,
        page_sequence=context.page_sequence,
        candidate=context.candidate,
        boundary_results=_accepted_boundary_results(context),
        final_closure_resolutions=resolutions,
        source_evidence=source_evidence,
        member_retirements=(
            context.locally_retired_members
            if member_retirements is None
            else member_retirements
        ),
    )


def _member_rejected_retirement_page(
    context: feed_work_scheduler.PageFinalizationContext,
) -> feed_work_scheduler.FinalPageCovered:
    """Accept exact boundary rejection with member-retirement release."""
    identity = context.cohort_terminal_facts[0].records[0].identity
    retirements = tuple(
        boundary.member for boundary in context.candidate_boundaries
    )
    return feed_work_scheduler.FinalPageCovered(
        grant=context.grant,
        page_sequence=context.page_sequence,
        candidate=context.candidate,
        boundary_results=tuple(
            feed_work_scheduler.BoundaryResult(
                boundary,
                feed_work_scheduler.BoundaryDisposition.MEMBER_REJECTED,
            )
            for boundary in context.candidate_boundaries
        ),
        final_closure_resolutions=(
            feed_work_scheduler.FinalRecordClosureResolution(
                identity=identity,
                closure_state=(
                    feed_work_scheduler.CohortRecordClosureState.REPLAY_SAFE_RELEASE
                ),
                release_basis=(
                    feed_work_scheduler.FinalRecordReleaseBasis.ACCEPTED_MEMBER_RETIREMENT
                ),
            ),
        ),
        source_evidence=None,
        member_retirements=retirements,
    )


def _member_rejected_durable_page(
    context: feed_work_scheduler.PageFinalizationContext,
) -> feed_work_scheduler.FinalPageCovered:
    """Return invalid durable closure for an exact rejected member."""
    identity = context.cohort_terminal_facts[0].records[0].identity
    retirements = tuple(
        boundary.member for boundary in context.candidate_boundaries
    )
    return feed_work_scheduler.FinalPageCovered(
        grant=context.grant,
        page_sequence=context.page_sequence,
        candidate=context.candidate,
        boundary_results=tuple(
            feed_work_scheduler.BoundaryResult(
                boundary,
                feed_work_scheduler.BoundaryDisposition.MEMBER_REJECTED,
            )
            for boundary in context.candidate_boundaries
        ),
        final_closure_resolutions=(
            feed_work_scheduler.FinalRecordClosureResolution(
                identity=identity,
                closure_state=(
                    feed_work_scheduler.CohortRecordClosureState.DURABLY_CLOSED
                ),
                release_basis=(
                    feed_work_scheduler.FinalRecordReleaseBasis.DURABLE_SOURCE_CLOSURE
                ),
            ),
        ),
        source_evidence=None,
        member_retirements=retirements,
    )


def _member_rejected_without_retirement_page(
    context: feed_work_scheduler.PageFinalizationContext,
) -> feed_work_scheduler.FinalPageCovered:
    """Omit required retirement evidence for a rejected boundary."""
    accepted = _member_rejected_retirement_page(context)
    return dataclasses.replace(accepted, member_retirements=())


class _ControlledPageFinalizer:
    """Capture one final context and settle only when explicitly released."""

    def __init__(
        self,
        result_factory: typing.Callable[
            [feed_work_scheduler.PageFinalizationContext],
            object,
        ] = _covered_final_page,
    ) -> None:
        self.result_factory = result_factory
        self.contexts: list[feed_work_scheduler.PageFinalizationContext] = []
        self.entered = asyncio.Event()
        self.release = asyncio.Event()

    async def finalize_page(
        self,
        context: feed_work_scheduler.PageFinalizationContext,
    ) -> _FinalPageResult:
        self.contexts.append(context)
        self.entered.set()
        await self.release.wait()
        result = self.result_factory(context)
        if not isinstance(
            result,
            (
                feed_work_scheduler.FinalPageCovered,
                feed_work_scheduler.FinalPageNoProgress,
                feed_work_scheduler.FinalPageReplayable,
                feed_work_scheduler.FinalPageRetryable,
                feed_work_scheduler.FinalPageGrantRejected,
                feed_work_scheduler.FinalPageOutcomeUnknown,
            ),
        ):
            message = "test finalizer returned an invalid result"
            raise TypeError(message)
        return result


class _NoCommitThenReplayableFinalizer:
    """Prove no commit once, then await accepted replayable settlement."""

    def __init__(self) -> None:
        self.contexts: list[feed_work_scheduler.PageFinalizationContext] = []
        self.retry_entered = asyncio.Event()
        self.release_retry = asyncio.Event()

    async def finalize_page(
        self,
        context: feed_work_scheduler.PageFinalizationContext,
    ) -> _FinalPageResult:
        self.contexts.append(context)
        if len(self.contexts) == 1:
            return feed_work_scheduler.FinalPageRetryable(
                grant=context.grant,
                page_sequence=context.page_sequence,
                candidate=context.candidate,
                commit_child_mutations_started=False,
                mutation_could_have_committed=False,
            )
        self.retry_entered.set()
        await self.release_retry.wait()
        return feed_work_scheduler.FinalPageReplayable(
            grant=context.grant,
            page_sequence=context.page_sequence,
            candidate=context.candidate,
            boundary_results=_accepted_boundary_results(context),
            final_closure_resolutions=(),
            source_evidence=None,
        )


class _PerOrderOutcomeExecutor:
    """Return one closed outcome factory for each cohort source order."""

    def __init__(
        self,
        factories: dict[
            int,
            typing.Callable[
                [feed_work_scheduler.CohortExecution],
                _ExecutorOutcome,
            ],
        ],
    ) -> None:
        self.factories = factories
        self.executed: list[int] = []

    async def execute(
        self,
        execution: feed_work_scheduler.CohortExecution,
    ) -> _ExecutorOutcome:
        if not isinstance(execution, feed_work_scheduler.CohortExecution):
            message = "executor received a private scheduler record"
            raise TypeError(message)
        source_order = execution.calls[0].identity.source_order
        self.executed.append(source_order)
        return self.factories[source_order](execution)


class _CapturingExecutor:
    def __init__(self) -> None:
        self.executions: list[feed_work_scheduler.CohortExecution] = []

    async def execute(
        self,
        execution: feed_work_scheduler.CohortExecution,
    ) -> _ExecutorOutcome:
        self.executions.append(execution)
        return _completed(execution)


class _CrossShardFailureExecutor:
    """Fails one shard while retaining controlled work on another."""

    def __init__(self, failing_feed: uuid.UUID) -> None:
        self.failing_feed = failing_feed
        self.failing_entered = asyncio.Event()
        self.healthy_entered = asyncio.Event()
        self.release_failure = asyncio.Event()
        self.release_healthy = asyncio.Event()

    async def execute(
        self,
        execution: feed_work_scheduler.CohortExecution,
    ) -> _ExecutorOutcome:
        if not isinstance(execution, feed_work_scheduler.CohortExecution):
            message = "executor received a private scheduler record"
            raise TypeError(message)
        if execution.calls[0].feed_id == self.failing_feed:
            self.failing_entered.set()
            await self.release_failure.wait()
            message = "unexpected executor failure"
            raise RuntimeError(message)
        self.healthy_entered.set()
        await self.release_healthy.wait()
        return _completed(execution)


class _TracingCalls:
    """Single-pass source iterator with deterministic pull observation."""

    def __init__(
        self,
        values: typing.Iterable[feed_work_scheduler.CohortSubmission],
    ) -> None:
        self._iterator = iter(values)
        self.pulled: list[int] = []
        self.changed = asyncio.Event()

    def __iter__(self) -> _TracingCalls:
        return self

    def __next__(self) -> feed_work_scheduler.CohortSubmission:
        value = next(self._iterator)
        source_order = typing.cast(
            "dict[str, object]",
            value.calls[0].payload,
        )["source_order"]
        self.pulled.append(typing.cast("int", source_order))
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

    async def test_scheduler_page_evidence_is_bounded_and_payload_free(
        self,
    ) -> None:
        evidence = feed_work_scheduler.SchedulerPageEvidence(
            admitted_record_count=3,
            admitted_cohort_count=2,
            terminal_record_count=3,
            replay_blocked_record_count=1,
            total_queue_wait_seconds=5.0,
            maximum_queue_wait_seconds=3.0,
            oldest_queue_age_seconds=2.0,
            pressure_encountered=True,
            pressure_wait_count=1,
            pressure_wait_seconds=4.0,
            maximum_held_count=4,
            maximum_queue_depth=3,
            maximum_worker_utilization_numerator=2,
            worker_utilization_denominator=4,
            early_flush_attempt_count=1,
            final_flush_attempt_count=1,
            total_flush_latency_seconds=7.0,
            maximum_flush_latency_seconds=5.0,
            fence_rejection_count=0,
            member_rejection_count=1,
        )

        self.assertFalse(hasattr(evidence, "__dict__"))
        self.assertTrue(
            all(
                isinstance(value, (bool, int, float))
                for value in dataclasses.asdict(evidence).values()
            )
        )
        self.assertNotIn("payload", repr(evidence).casefold())
        self.assertNotIn("url", repr(evidence).casefold())
        admitted_count_field = "admitted_record_count"
        with self.assertRaises((dataclasses.FrozenInstanceError, TypeError)):
            setattr(evidence, admitted_count_field, 4)

        invalid = (
            {"admitted_record_count": True},
            {"terminal_record_count": 4},
            {"total_queue_wait_seconds": float("nan")},
            {"maximum_queue_wait_seconds": float("inf")},
            {"oldest_queue_age_seconds": 4.0},
            {"pressure_encountered": False},
            {"pressure_wait_count": True},
            {"pressure_wait_count": -1, "pressure_encountered": False},
            {
                "total_queue_wait_seconds": 1.0,
                "maximum_queue_wait_seconds": 0.0,
                "oldest_queue_age_seconds": 0.0,
            },
            {"maximum_queue_depth": 5},
            {"maximum_worker_utilization_numerator": 5},
            {"total_flush_latency_seconds": 4.0},
            {
                "total_flush_latency_seconds": 1.0,
                "maximum_flush_latency_seconds": 0.0,
            },
        )
        for changes in invalid:
            with self.subTest(changes=changes):
                with self.assertRaises((TypeError, ValueError)):
                    dataclasses.replace(evidence, **changes)

        self.assertEqual(
            (
                _scheduler_types().PRODUCTION_SHARD_COUNT,
                _scheduler_types().PRODUCTION_SHARD_CAPACITY,
                _scheduler_types().PRODUCTION_WORKERS_PER_SHARD,
                _scheduler_types().PRODUCTION_HIGH_WATER,
                _scheduler_types().PRODUCTION_RESUME_AT,
            ),
            (8, 500, 4, 400, 299),
        )

    async def test_settled_page_evidence_has_exact_no_queue_zeroes(
        self,
    ) -> None:
        clock = _ControlledClock()
        scheduler = feed_work_scheduler.FeedWorkScheduler(
            _ImmediateExecutor(),
            _monotonic=clock,
        )
        await scheduler.start()
        grant = _grant()
        lane = _open_lane(scheduler, grant)
        cursor = cursor_policy.LeaseCursor(grant, pos=None)
        observed: list[feed_work_scheduler.SchedulerPageEvidence] = []

        try:
            settled = await lane.cover_page(
                calls=(),
                boundaries=(),
                candidate=cursor.prepare(_SOURCE_TIME),
                evidence_observer=observed.append,
            )

            self.assertIsInstance(settled, feed_work_scheduler.SettledPage)
            self.assertEqual(observed, [])
            evidence = _require_settled(settled).scheduler_evidence
            self.assertEqual(evidence.admitted_record_count, 0)
            self.assertEqual(evidence.admitted_cohort_count, 0)
            self.assertEqual(evidence.total_queue_wait_seconds, 0.0)
            self.assertEqual(evidence.maximum_queue_wait_seconds, 0.0)
            self.assertEqual(evidence.oldest_queue_age_seconds, 0.0)
            self.assertFalse(evidence.pressure_encountered)
            self.assertEqual(evidence.pressure_wait_count, 0)
            self.assertEqual(evidence.pressure_wait_seconds, 0.0)
            self.assertEqual(evidence.maximum_held_count, 0)
            self.assertEqual(evidence.maximum_queue_depth, 0)
            self.assertEqual(
                evidence.maximum_worker_utilization_numerator,
                0,
            )
            self.assertEqual(evidence.worker_utilization_denominator, 32)
            self.assertEqual(evidence.early_flush_attempt_count, 0)
            self.assertEqual(evidence.final_flush_attempt_count, 1)
            self.assertEqual(evidence.total_flush_latency_seconds, 0.0)
            self.assertEqual(evidence.maximum_flush_latency_seconds, 0.0)
            self.assertEqual(evidence.fence_rejection_count, 0)
            self.assertEqual(evidence.member_rejection_count, 0)
        finally:
            await scheduler.close()

    async def test_queue_wait_counts_multi_record_cohort_once_and_keeps_age(
        self,
    ) -> None:
        limits = _scheduler_types()._SchedulerLimits(
            shard_count=1,
            capacity=4,
            workers_per_shard=1,
            high_water=4,
            resume_at=2,
        )
        clock = _ControlledClock()
        executor = _GateExecutor()
        scheduler = feed_work_scheduler.FeedWorkScheduler(
            executor,
            _limits=limits,
            _monotonic=clock,
        )
        await scheduler.start()
        grant = _grant()
        lane = _open_lane(scheduler, grant)
        cursor = cursor_policy.LeaseCursor(grant, pos=None)
        coverage = asyncio.create_task(
            lane.cover_page(
                calls=(
                    _cohort(
                        uuid.UUID(int=1),
                        (0,),
                        grant=grant,
                        cohort_timestamp=_SOURCE_TIME,
                    ),
                    _cohort(
                        uuid.UUID(int=2),
                        (1, 2, 3),
                        grant=grant,
                        cohort_timestamp=_SOURCE_TIME,
                    ),
                ),
                boundaries=(),
                candidate=cursor.prepare(_SOURCE_TIME),
            )
        )

        try:
            await executor.wait_for_started(1)
            async with asyncio.timeout(1):
                while True:
                    page = (await lane._snapshot()).page
                    if page is not None and page.registered_records == 4:
                        break
                    await asyncio.sleep(0)
            clock.advance(3.0)
            await executor.release_completions(1)
            await executor.wait_for_started(2)
            executor.release_all()
            settled = await asyncio.wait_for(coverage, timeout=1)
            evidence = _require_settled(settled).scheduler_evidence

            self.assertEqual(evidence.admitted_record_count, 4)
            self.assertEqual(evidence.admitted_cohort_count, 2)
            self.assertEqual(evidence.terminal_record_count, 4)
            self.assertEqual(evidence.total_queue_wait_seconds, 3.0)
            self.assertEqual(evidence.maximum_queue_wait_seconds, 3.0)
            self.assertEqual(evidence.oldest_queue_age_seconds, 3.0)
            self.assertFalse(evidence.pressure_encountered)
            self.assertEqual(evidence.pressure_wait_count, 0)
            self.assertEqual(evidence.pressure_wait_seconds, 0.0)
            self.assertEqual(evidence.maximum_held_count, 4)
            self.assertEqual(evidence.maximum_queue_depth, 4)
            self.assertEqual(
                evidence.maximum_worker_utilization_numerator,
                1,
            )
            self.assertEqual(evidence.worker_utilization_denominator, 1)
        finally:
            executor.release_all()
            if not coverage.done():
                coverage.cancel()
                await asyncio.gather(coverage, return_exceptions=True)
            await scheduler.close()

    async def test_empty_page_evidence_excludes_sibling_lane_load(self) -> None:
        limits = _scheduler_types()._SchedulerLimits(
            shard_count=1,
            capacity=4,
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
        busy_grant = _grant(lease_key="150")
        empty_grant = _grant(lease_key="151")
        busy_lane = _open_lane(scheduler, busy_grant)
        empty_lane = _open_lane(scheduler, empty_grant)
        busy_coverage = asyncio.create_task(
            busy_lane.cover_page(
                calls=(
                    _submission(uuid.UUID(int=1), 0, grant=busy_grant),
                    _submission(uuid.UUID(int=2), 1, grant=busy_grant),
                ),
                boundaries=(),
                candidate=cursor_policy.LeaseCursor(
                    busy_grant,
                    pos=None,
                ).prepare(_SOURCE_TIME),
            )
        )

        try:
            await executor.wait_for_started(1)
            async with asyncio.timeout(1):
                while True:
                    page = (await busy_lane._snapshot()).page
                    if page is not None and page.registered_records == 2:
                        break
                    await asyncio.sleep(0)

            settled = await empty_lane.cover_page(
                calls=(),
                boundaries=(),
                candidate=cursor_policy.LeaseCursor(
                    empty_grant,
                    pos=None,
                ).prepare(_SOURCE_TIME),
            )
            evidence = _require_settled(settled).scheduler_evidence

            self.assertEqual(evidence.maximum_held_count, 0)
            self.assertEqual(evidence.maximum_queue_depth, 0)
            self.assertEqual(
                evidence.maximum_worker_utilization_numerator,
                0,
            )
            self.assertFalse(evidence.pressure_encountered)
        finally:
            busy_coverage.cancel()
            executor.release_all()
            await asyncio.gather(busy_coverage, return_exceptions=True)
            await scheduler.close()

    async def test_pressure_evidence_times_only_blocked_admission(self) -> None:
        limits = _scheduler_types()._SchedulerLimits(
            shard_count=1,
            capacity=2,
            workers_per_shard=1,
            high_water=1,
            resume_at=0,
        )
        clock = _ControlledClock()
        executor = _GateExecutor()
        scheduler = feed_work_scheduler.FeedWorkScheduler(
            executor,
            _limits=limits,
            _monotonic=clock,
        )
        await scheduler.start()
        grant = _grant()
        lane = _open_lane(scheduler, grant)
        coverage = asyncio.create_task(
            lane.cover_page(
                calls=(
                    _submission(uuid.UUID(int=1), 0, grant=grant),
                    _cohort(
                        uuid.UUID(int=2),
                        (1,),
                        grant=grant,
                        cohort_timestamp=(
                            _SOURCE_TIME + datetime.timedelta(seconds=1)
                        ),
                        admission_hook=lambda _identities: clock.advance(2.0),
                    ),
                ),
                boundaries=(),
                candidate=cursor_policy.LeaseCursor(
                    grant,
                    pos=None,
                ).prepare(_SOURCE_TIME),
            )
        )

        try:
            await scheduler._shards[0].wait_for_capacity_waiters(1)
            clock.advance(5.0)
            await executor.release_completions(1)
            await executor.wait_for_started(2)
            executor.release_all()
            evidence = _require_settled(
                await asyncio.wait_for(coverage, timeout=1)
            ).scheduler_evidence

            self.assertTrue(evidence.pressure_encountered)
            self.assertEqual(evidence.pressure_wait_count, 1)
            self.assertEqual(evidence.pressure_wait_seconds, 5.0)
            self.assertEqual(evidence.maximum_held_count, 1)
        finally:
            executor.release_all()
            if not coverage.done():
                coverage.cancel()
                await asyncio.gather(coverage, return_exceptions=True)
            await scheduler.close()

    async def test_typed_abort_evidence_observer_is_contained_and_reentrant(
        self,
    ) -> None:
        clock = _ControlledClock()
        executor = _GatedOutcomeExecutor(_stopped)
        scheduler = feed_work_scheduler.FeedWorkScheduler(
            executor,
            _monotonic=clock,
        )
        await scheduler.start()
        grant = _grant()
        lane = _open_lane(scheduler, grant)
        observed: list[feed_work_scheduler.SchedulerPageEvidence] = []

        def observe(
            evidence: feed_work_scheduler.SchedulerPageEvidence,
        ) -> None:
            observed.append(evidence)
            lane._request_close(
                feed_work_scheduler.LaneCloseReason.PLANNED_DRAIN
            )
            message = "contained evidence observer failure"
            raise RuntimeError(message)

        coverage = asyncio.create_task(
            lane.cover_page(
                calls=(_submission(uuid.UUID(int=8), 0, grant=grant),),
                boundaries=(),
                candidate=cursor_policy.LeaseCursor(
                    grant,
                    pos=None,
                ).prepare(_SOURCE_TIME),
                evidence_observer=observe,
            )
        )
        try:
            await asyncio.wait_for(executor.entered.wait(), timeout=1)
            clock.advance(2.0)
            executor.release.set()
            with self.assertRaisesRegex(RuntimeError, "stopped"):
                await asyncio.wait_for(coverage, timeout=1)

            self.assertEqual(len(observed), 1)
            self.assertEqual(observed[0].admitted_record_count, 1)
            self.assertEqual(observed[0].terminal_record_count, 1)
            self.assertEqual(observed[0].final_flush_attempt_count, 0)
        finally:
            executor.release.set()
            await asyncio.gather(coverage, return_exceptions=True)
            await scheduler.close()

    async def test_repeated_cancellation_observes_failure_evidence_once(
        self,
    ) -> None:
        limits = _scheduler_types()._SchedulerLimits(
            shard_count=1,
            capacity=2,
            workers_per_shard=1,
            high_water=1,
            resume_at=0,
        )
        clock = _ControlledClock()
        executor = _GateExecutor()
        scheduler = feed_work_scheduler.FeedWorkScheduler(
            executor,
            _limits=limits,
            _monotonic=clock,
        )
        await scheduler.start()
        grant = _grant()
        lane = _open_lane(scheduler, grant)
        observed: list[feed_work_scheduler.SchedulerPageEvidence] = []
        coverage = asyncio.create_task(
            lane.cover_page(
                calls=(
                    _submission(uuid.UUID(int=1), 0, grant=grant),
                    _submission(uuid.UUID(int=2), 1, grant=grant),
                ),
                boundaries=(),
                candidate=cursor_policy.LeaseCursor(
                    grant,
                    pos=None,
                ).prepare(_SOURCE_TIME),
                evidence_observer=observed.append,
            )
        )

        try:
            await scheduler._shards[0].wait_for_capacity_waiters(1)
            clock.advance(4.0)
            coverage.cancel()
            coverage.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await coverage

            self.assertEqual(len(observed), 1)
            self.assertTrue(observed[0].pressure_encountered)
            self.assertEqual(observed[0].pressure_wait_count, 1)
            self.assertEqual(observed[0].pressure_wait_seconds, 4.0)
        finally:
            executor.release_all()
            await scheduler._wait_for_idle()
            await scheduler.close()

    async def test_queue_wait_closes_before_purge_settlement_observer(
        self,
    ) -> None:
        limits = _scheduler_types()._SchedulerLimits(
            shard_count=1,
            capacity=2,
            workers_per_shard=1,
            high_water=2,
            resume_at=1,
        )
        clock = _ControlledClock()
        executor = _GateExecutor()
        scheduler = feed_work_scheduler.FeedWorkScheduler(
            executor,
            _limits=limits,
            _monotonic=clock,
        )
        await scheduler.start()
        grant = _grant()
        lane = _open_lane(scheduler, grant)
        observed: list[feed_work_scheduler.SchedulerPageEvidence] = []
        coverage = asyncio.create_task(
            lane.cover_page(
                calls=(
                    _submission(uuid.UUID(int=1), 0, grant=grant),
                    _submission(
                        uuid.UUID(int=2),
                        1,
                        grant=grant,
                        settlement_observer=lambda _settlement: clock.advance(
                            10.0
                        ),
                    ),
                ),
                boundaries=(),
                candidate=cursor_policy.LeaseCursor(
                    grant,
                    pos=None,
                ).prepare(_SOURCE_TIME),
                evidence_observer=observed.append,
            )
        )

        try:
            await executor.wait_for_started(1)
            async with asyncio.timeout(1):
                while True:
                    page = (await lane._snapshot()).page
                    if page is not None and page.registered_records == 2:
                        break
                    await asyncio.sleep(0)
            clock.advance(4.0)
            coverage.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await coverage

            self.assertEqual(len(observed), 1)
            self.assertEqual(observed[0].total_queue_wait_seconds, 4.0)
            self.assertEqual(observed[0].maximum_queue_wait_seconds, 4.0)
            self.assertEqual(observed[0].oldest_queue_age_seconds, 4.0)
        finally:
            executor.release_all()
            await scheduler._wait_for_idle()
            await scheduler.close()

    async def test_page_evidence_retry_excludes_active_same_sequence_work(
        self,
    ) -> None:
        executor = _HoldFirstThenCompleteExecutor()
        scheduler = feed_work_scheduler.FeedWorkScheduler(
            executor,
            _limits=_scheduler_types()._SchedulerLimits(
                shard_count=1,
                capacity=3,
                workers_per_shard=2,
                high_water=3,
                resume_at=2,
            ),
        )
        await scheduler.start()
        grant = _grant()
        lane = _open_lane(scheduler, grant)
        cursor = cursor_policy.LeaseCursor(grant, pos=None)
        candidate = cursor.prepare(_SOURCE_TIME)
        first = asyncio.create_task(
            lane.cover_page(
                calls=(_submission(uuid.UUID(int=1), 0, grant=grant),),
                boundaries=(),
                candidate=candidate,
            )
        )

        try:
            await asyncio.wait_for(executor.first_entered.wait(), timeout=1)
            first.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await first
            self.assertEqual((await scheduler._snapshot()).held, 1)

            settled = await lane.cover_page(
                calls=(_submission(uuid.UUID(int=2), 0, grant=grant),),
                boundaries=(),
                candidate=candidate,
            )

            evidence = _require_settled(settled).scheduler_evidence
            self.assertEqual(evidence.admitted_record_count, 1)
            self.assertEqual(evidence.maximum_held_count, 1)
            self.assertEqual(evidence.maximum_queue_depth, 1)
        finally:
            executor.release_first.set()
            await asyncio.gather(first, return_exceptions=True)
            await scheduler._wait_for_idle()
            await scheduler.close()

    async def test_public_exports_are_narrow_and_immutable(self) -> None:
        expected = {
            "CallAuthorityLost",
            "CallCompleted",
            "CallExecution",
            "CallFinalClosurePending",
            "CallIntegrityFailure",
            "CallMembershipRejected",
            "CallOutcomeUnknown",
            "CallReplayableDirectFailure",
            "CallRetryable",
            "CallSettlement",
            "CallStopped",
            "CallSubmission",
            "CohortCancellationHandoff",
            "CohortDirectFailureFact",
            "CohortExecution",
            "CohortIntegrityError",
            "CohortItemFailureFact",
            "CohortRecordClosureState",
            "CohortRecordIdentity",
            "CohortRecordTerminalFact",
            "CohortRecordTerminalReason",
            "CohortRetentionHandle",
            "CohortSubmission",
            "CohortTerminalDisposition",
            "CohortTerminalFacts",
            "BoundaryBatchCommitted",
            "BoundaryBatchRetryable",
            "BoundaryDisposition",
            "BoundaryGrantRejected",
            "BoundaryResult",
            "BoundaryWork",
            "FeedRemoved",
            "FeedWorkScheduler",
            "FinalPageCovered",
            "FinalPageGrantRejected",
            "FinalPageNoProgress",
            "FinalPageOutcomeUnknown",
            "FinalPageReplayable",
            "FinalPageRetryable",
            "FinalRecordClosureResolution",
            "FinalRecordReleaseBasis",
            "GrantLane",
            "LaneCloseReason",
            "LaneClosed",
            "LaneSignalView",
            "OutcomeUnknownCause",
            "OutcomeUnknownRetentionRequest",
            "PageFinalizationContext",
            "PageFinalizer",
            "SchedulerIntegrityError",
            "SchedulerPageEvidence",
            "SettledPage",
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
        self.assertTrue(hasattr(submission, "member"))
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

        member = _member(grant, uuid.UUID(int=8))
        identity = feed_work_scheduler.CohortRecordIdentity(
            grant=grant,
            member=member,
            page_sequence=0,
            feed_id=uuid.UUID(int=8),
            cohort_timestamp=None,
            source_order=0,
            local_sequence=0,
        )
        execution = feed_work_scheduler.CallExecution(
            identity=identity,
            payload=object(),
        )
        self.assertTrue(dataclasses.is_dataclass(execution))
        self.assertFalse(hasattr(execution, "__dict__"))
        timestamp_field = "source_timestamp"
        with self.assertRaises((dataclasses.FrozenInstanceError, TypeError)):
            setattr(execution, timestamp_field, _SOURCE_TIME)

    async def test_executor_receives_public_execution_with_none_timestamp(
        self,
    ) -> None:
        executor = _CapturingExecutor()
        scheduler = feed_work_scheduler.FeedWorkScheduler(executor)
        await scheduler.start()
        grant = _grant()
        lane = _open_lane(scheduler, grant)
        member = _member(grant, uuid.UUID(int=8))
        payload = {
            "source_order": 0,
            "url": "https://example.invalid/1",
            "member": member,
        }
        call = feed_work_scheduler.CallSubmission(
            feed_id=uuid.UUID(int=8),
            source_timestamp=None,
            payload=payload,
        )

        try:
            await lane.cover_page(
                calls=(
                    feed_work_scheduler.CohortSubmission(
                        member=member,
                        feed_id=uuid.UUID(int=8),
                        cohort_timestamp=None,
                        calls=(call,),
                        admission_hook=lambda _identities: None,
                    ),
                ),
                boundaries=(),
                candidate=cursor_policy.LeaseCursor(
                    grant,
                    pos=None,
                ).prepare(_SOURCE_TIME),
            )
            await scheduler._wait_for_idle()

            self.assertEqual(len(executor.executions), 1)
            cohort_execution = executor.executions[0]
            self.assertIsInstance(
                cohort_execution,
                feed_work_scheduler.CohortExecution,
            )
            execution = cohort_execution.calls[0]
            self.assertIs(execution.grant, grant)
            self.assertEqual(execution.feed_id, uuid.UUID(int=8))
            self.assertIsNone(execution.source_timestamp)
            self.assertIs(execution.payload, payload)
            self.assertFalse(hasattr(execution, "local_sequence"))
            self.assertFalse(hasattr(execution, "work"))
        finally:
            await scheduler.close()

    async def test_executor_outcomes_notify_once_outside_shard_locks(
        self,
    ) -> None:
        cases = (
            (
                _completed,
                feed_work_scheduler.CallSettlement.COMPLETED,
            ),
            (
                _retryable,
                feed_work_scheduler.CallSettlement.REPLAY_SAFE_RELEASE,
            ),
            (
                _authority_lost,
                feed_work_scheduler.CallSettlement.REPLAY_SAFE_RELEASE,
            ),
            (
                _membership_rejected,
                feed_work_scheduler.CallSettlement.REPLAY_SAFE_RELEASE,
            ),
        )

        def observer_for(
            scheduler: feed_work_scheduler.FeedWorkScheduler,
            observed: list[object],
            notified: asyncio.Event,
        ) -> typing.Callable[[object], None]:
            def observe(settlement: object) -> None:
                self.assertFalse(
                    any(shard._lock.locked() for shard in scheduler._shards)
                )
                observed.append(settlement)
                notified.set()

            return observe

        for case_index, (outcome_factory, expected) in enumerate(cases):
            with self.subTest(outcome=outcome_factory.__name__):
                executor = _GatedOutcomeExecutor(outcome_factory)
                scheduler = feed_work_scheduler.FeedWorkScheduler(executor)
                await scheduler.start()
                grant = _grant(lease_key=str(150 + case_index))
                lane = _open_lane(scheduler, grant)
                observed: list[object] = []
                notified = asyncio.Event()

                coverage = asyncio.create_task(
                    lane.cover_page(
                        calls=(
                            _submission(
                                uuid.UUID(int=8),
                                0,
                                grant=grant,
                                settlement_observer=observer_for(
                                    scheduler,
                                    observed,
                                    notified,
                                ),
                            ),
                        ),
                        boundaries=(),
                        candidate=cursor_policy.LeaseCursor(
                            grant,
                            pos=None,
                        ).prepare(_SOURCE_TIME),
                    )
                )
                await asyncio.wait_for(executor.entered.wait(), timeout=1)
                executor.release.set()
                await asyncio.wait_for(notified.wait(), timeout=1)

                self.assertEqual(observed, [expected])
                if (
                    expected
                    is feed_work_scheduler.CallSettlement.AUTHORITY_LOST
                ):
                    await lane.close(
                        feed_work_scheduler.LaneCloseReason.AUTHORITY_LOSS
                    )
                await asyncio.gather(coverage, return_exceptions=True)
                await scheduler.close()

    async def test_observer_can_reenter_lane_and_failure_is_integrity(
        self,
    ) -> None:
        executor = _GatedOutcomeExecutor(_completed)
        scheduler = feed_work_scheduler.FeedWorkScheduler(executor)
        await scheduler.start()
        grant = _grant()
        lane = _open_lane(scheduler, grant)
        reentered = asyncio.Event()

        def close_from_observer(settlement: object) -> None:
            self.assertIs(
                settlement,
                feed_work_scheduler.CallSettlement.COMPLETED,
            )
            self.assertFalse(
                any(shard._lock.locked() for shard in scheduler._shards)
            )
            lane._request_close(
                feed_work_scheduler.LaneCloseReason.PLANNED_DRAIN
            )
            reentered.set()

        coverage = asyncio.create_task(
            lane.cover_page(
                calls=(
                    _submission(
                        uuid.UUID(int=8),
                        0,
                        grant=grant,
                        settlement_observer=close_from_observer,
                    ),
                ),
                boundaries=(),
                candidate=cursor_policy.LeaseCursor(
                    grant,
                    pos=None,
                ).prepare(_SOURCE_TIME),
            )
        )
        await asyncio.wait_for(executor.entered.wait(), timeout=1)
        executor.release.set()
        await asyncio.wait_for(reentered.wait(), timeout=1)
        await asyncio.gather(coverage, return_exceptions=True)
        await lane.close()
        await scheduler.close()

        failed_scheduler = feed_work_scheduler.FeedWorkScheduler(
            _ImmediateExecutor()
        )
        await failed_scheduler.start()
        failed_grant = _grant(lease_key="151")
        failed_lane = _open_lane(failed_scheduler, failed_grant)

        def fail_observer(settlement: object) -> None:
            del settlement
            message = "observer failed"
            raise RuntimeError(message)

        coverage = asyncio.create_task(
            failed_lane.cover_page(
                calls=(
                    _submission(
                        uuid.UUID(int=8),
                        0,
                        grant=failed_grant,
                        settlement_observer=fail_observer,
                    ),
                ),
                boundaries=(),
                candidate=cursor_policy.LeaseCursor(
                    failed_grant,
                    pos=None,
                ).prepare(_SOURCE_TIME),
            )
        )
        await asyncio.wait_for(
            failed_scheduler.integrity_failure_event.wait(),
            timeout=1,
        )
        await asyncio.gather(coverage, return_exceptions=True)
        with self.assertRaises(feed_work_scheduler.SchedulerIntegrityError):
            failed_scheduler.raise_if_failed()
        self.assertIsInstance(
            await failed_scheduler.close(),
            feed_work_scheduler.Undrained,
        )

    async def test_queued_abort_removal_and_loss_notify_exactly_once(
        self,
    ) -> None:
        scheduler_types = _scheduler_types()
        limits = scheduler_types._SchedulerLimits(
            shard_count=1,
            capacity=4,
            workers_per_shard=1,
            high_water=2,
            resume_at=0,
        )

        abort_executor = _GateExecutor()
        abort_scheduler = feed_work_scheduler.FeedWorkScheduler(
            abort_executor,
            _limits=limits,
        )
        await abort_scheduler.start()
        abort_grant = _grant(lease_key="180")
        abort_lane = _open_lane(abort_scheduler, abort_grant)
        abort_observed: dict[int, list[object]] = {0: [], 1: [], 2: []}
        abort_coverage = asyncio.create_task(
            abort_lane.cover_page(
                calls=(
                    _submission(
                        uuid.UUID(int=1),
                        source_order,
                        grant=abort_grant,
                        settlement_observer=abort_observed[source_order].append,
                    )
                    for source_order in range(3)
                ),
                boundaries=(),
                candidate=cursor_policy.LeaseCursor(
                    abort_grant,
                    pos=None,
                ).prepare(_SOURCE_TIME),
            )
        )
        await abort_scheduler._shards[0].wait_for_capacity_waiters(1)
        abort_coverage.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await abort_coverage
        self.assertEqual(
            abort_observed[1],
            [feed_work_scheduler.CallSettlement.ABORTED],
        )
        self.assertEqual(abort_observed[2], [])
        abort_executor.release_all()
        await abort_scheduler._wait_for_idle()
        self.assertEqual(
            abort_observed[0],
            [feed_work_scheduler.CallSettlement.COMPLETED],
        )
        await abort_scheduler.close()

        remove_executor = _GateExecutor()
        remove_scheduler = feed_work_scheduler.FeedWorkScheduler(
            remove_executor,
            _limits=limits,
        )
        await remove_scheduler.start()
        remove_grant = _grant(lease_key="181")
        remove_lane = _open_lane(remove_scheduler, remove_grant)
        remove_observed: dict[int, list[object]] = {0: [], 1: []}
        removed_feed = uuid.UUID(int=1)
        remove_coverage = asyncio.create_task(
            remove_lane.cover_page(
                calls=(
                    _submission(
                        removed_feed,
                        source_order,
                        grant=remove_grant,
                        settlement_observer=(
                            remove_observed[source_order].append
                        ),
                    )
                    for source_order in range(2)
                ),
                boundaries=(),
                candidate=cursor_policy.LeaseCursor(
                    remove_grant,
                    pos=None,
                ).prepare(_SOURCE_TIME),
            )
        )
        await remove_executor.wait_for_started(1)
        await remove_lane.remove_feed(removed_feed)
        self.assertEqual(
            remove_observed[1],
            [feed_work_scheduler.CallSettlement.MEMBERSHIP_REJECTED],
        )
        remove_executor.release_all()
        await remove_scheduler._wait_for_idle()
        await asyncio.gather(remove_coverage, return_exceptions=True)
        self.assertEqual(
            remove_observed[0],
            [feed_work_scheduler.CallSettlement.COMPLETED],
        )
        await remove_scheduler.close()

        loss_executor = _DelayedCancellationExecutor()
        loss_scheduler = feed_work_scheduler.FeedWorkScheduler(
            loss_executor,
            _limits=limits,
        )
        await loss_scheduler.start()
        loss_grant = _grant(lease_key="182")
        loss_lane = _open_lane(loss_scheduler, loss_grant)
        loss_observed: dict[int, list[object]] = {0: [], 1: []}
        loss_coverage = asyncio.create_task(
            loss_lane.cover_page(
                calls=(
                    _submission(
                        uuid.UUID(int=1),
                        source_order,
                        grant=loss_grant,
                        settlement_observer=loss_observed[source_order].append,
                    )
                    for source_order in range(2)
                ),
                boundaries=(),
                candidate=cursor_policy.LeaseCursor(
                    loss_grant,
                    pos=None,
                ).prepare(_SOURCE_TIME),
            )
        )
        await asyncio.wait_for(loss_executor.entered.wait(), timeout=1)
        loss_close = asyncio.create_task(
            loss_lane.close(feed_work_scheduler.LaneCloseReason.AUTHORITY_LOSS)
        )
        await asyncio.wait_for(
            loss_executor.cancellation_seen.wait(), timeout=1
        )
        loss_executor.settle.set()
        await asyncio.wait_for(loss_close, timeout=1)
        await asyncio.gather(loss_coverage, return_exceptions=True)
        self.assertEqual(
            loss_observed,
            {
                0: [feed_work_scheduler.CallSettlement.REPLAY_SAFE_RELEASE],
                1: [feed_work_scheduler.CallSettlement.AUTHORITY_LOST],
            },
        )
        await loss_scheduler.close()

    async def test_start_is_idempotent_and_opens_one_exact_lane(self) -> None:
        scheduler = feed_work_scheduler.FeedWorkScheduler(_ImmediateExecutor())
        await scheduler.start()
        await scheduler.start()
        grant = _grant()
        lane = _open_lane(scheduler, grant)
        try:
            snapshot = await scheduler._snapshot()
            self.assertEqual(len(snapshot.shards), 8)
            self.assertEqual(snapshot.registered_worker_tasks, 32)
            self.assertEqual(snapshot.lane_count, 1)
            self.assertIs(lane.grant, grant)
            self.assertIsInstance(lane, feed_work_scheduler.GrantLane)
            with self.assertRaisesRegex(ValueError, "already has a lane"):
                _open_lane(scheduler, grant)
        finally:
            await scheduler.close()

    async def test_page_validates_before_pull_and_returns_sealed_receipt(
        self,
    ) -> None:
        executor = _ImmediateExecutor()
        scheduler = feed_work_scheduler.FeedWorkScheduler(executor)
        await scheduler.start()
        grant = _grant()
        lane = _open_lane(scheduler, grant)
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
            self.assertEqual(
                cursor.accept(_covered_settlement(receipt)),
                _SOURCE_TIME,
            )
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

    async def test_invalid_boundary_fails_after_call_stream(self) -> None:
        scheduler = feed_work_scheduler.FeedWorkScheduler(_ImmediateExecutor())
        await scheduler.start()
        grant = _grant()
        lane = _open_lane(scheduler, grant)
        candidate = cursor_policy.LeaseCursor(
            grant,
            pos=None,
        ).prepare(_SOURCE_TIME)
        calls = _TracingCalls((_submission(uuid.UUID(int=8), 0),))
        try:
            with self.assertRaisesRegex(
                TypeError,
                "BoundaryWork",
            ):
                await lane.cover_page(
                    calls=calls,
                    boundaries=typing.cast(
                        "tuple[feed_work_scheduler.BoundaryWork, ...]",
                        (object(),),
                    ),
                    candidate=candidate,
                )
            self.assertEqual(calls.pulled, [0])
            await scheduler._wait_for_idle()
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
        lane = _open_lane(scheduler, grant)
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
            self.assertEqual(calls.pulled, list(range(402)))
            self.assertFalse(coverage.done())

            await executor.release_completions(100)
            await scheduler._shards[0].wait_for_held(300)
            self.assertEqual(calls.pulled, list(range(402)))
            self.assertFalse(coverage.done())

            await executor.release_completions(1)
            await calls.wait_for_pulled(402)
            executor.release_all()
            receipt = await asyncio.wait_for(coverage, timeout=1)
            self.assertEqual(calls.pulled, list(range(402)))
            self.assertEqual(
                cursor.accept(_covered_settlement(receipt)),
                _SOURCE_TIME,
            )
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
        lane = _open_lane(scheduler, grant)
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
            self.assertEqual(calls.pulled, list(range(6)))
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

    async def test_repeated_cancel_waits_for_bounded_page_cleanup(
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
            typing.cast("typing.Any", executor),
            _limits=limits,
        )
        await scheduler.start()
        grant = _grant()
        lane = _open_lane(scheduler, grant)
        cursor = cursor_policy.LeaseCursor(grant, pos=None)
        candidate = cursor.prepare(_SOURCE_TIME)
        abort_entered = asyncio.Event()
        allow_abort = asyncio.Event()
        abort_page = lane._abort_page

        async def gated_abort(page_sequence: typing.Any) -> None:
            abort_entered.set()
            await allow_abort.wait()
            await abort_page(page_sequence)

        typing.cast("typing.Any", lane)._abort_page = gated_abort
        coverage = asyncio.create_task(
            lane.cover_page(
                calls=typing.cast(
                    "typing.Any",
                    (
                        _submission(uuid.UUID(int=index + 1), index)
                        for index in range(6)
                    ),
                ),
                boundaries=(),
                candidate=candidate,
            )
        )
        try:
            await scheduler._shards[0].wait_for_capacity_waiters(1)
            coverage.cancel()
            await asyncio.wait_for(abort_entered.wait(), timeout=1)

            coverage.cancel()
            yielded = asyncio.Event()
            asyncio.get_running_loop().call_soon(yielded.set)
            await yielded.wait()
            self.assertFalse(coverage.done())

            allow_abort.set()
            with self.assertRaises(asyncio.CancelledError):
                await coverage

            snapshot = await scheduler._snapshot()
            self.assertEqual(snapshot.held, 1)
            self.assertEqual(snapshot.shards[0].queued_calls, 0)
            self.assertEqual(snapshot.shards[0].active_calls, 1)
            self.assertIsNone((await lane._snapshot()).page)
            self.assertIs(cursor.outstanding_candidate, candidate)
        finally:
            allow_abort.set()
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
        lane = _open_lane(scheduler, grant)
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
        old_lane = _open_lane(scheduler, old)
        successor_lane = _open_lane(scheduler, successor)
        sibling_lane = _open_lane(scheduler, sibling)
        shared_feed = uuid.UUID(int=1)
        old_coverage = asyncio.create_task(
            old_lane.cover_page(
                calls=(
                    _submission(shared_feed, 0, grant=old),
                    _submission(shared_feed, 1, grant=old),
                ),
                boundaries=(),
                candidate=cursor_policy.LeaseCursor(old, pos=None).prepare(
                    _SOURCE_TIME
                ),
            ),
            name="old-coverage",
        )
        successor_coverage = asyncio.create_task(
            successor_lane.cover_page(
                calls=(_submission(shared_feed, 0, grant=successor),),
                boundaries=(),
                candidate=cursor_policy.LeaseCursor(
                    successor,
                    pos=None,
                ).prepare(_SOURCE_TIME),
            ),
            name="successor-coverage",
        )
        sibling_coverage = asyncio.create_task(
            sibling_lane.cover_page(
                calls=(_submission(uuid.UUID(int=2), 0, grant=sibling),),
                boundaries=(),
                candidate=cursor_policy.LeaseCursor(
                    sibling,
                    pos=None,
                ).prepare(_SOURCE_TIME),
            ),
            name="sibling-coverage",
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
            records = (await scheduler._snapshot()).shards[0].records
            self.assertIn(successor, {record.grant for record in records})
            self.assertIn(sibling, {record.grant for record in records})
            self.assertIn(old, {record.grant for record in records})

            executor.release_all()
            old_page, successor_page, sibling_page = await asyncio.gather(
                old_coverage,
                successor_coverage,
                sibling_coverage,
                return_exceptions=True,
            )
            self.assertIsInstance(old_page, RuntimeError)
            self.assertIsInstance(
                successor_page,
                feed_work_scheduler.SettledPage,
            )
            self.assertIsInstance(
                sibling_page,
                feed_work_scheduler.SettledPage,
            )
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
                _open_lane(scheduler, old)
            remaining = (await scheduler._snapshot()).shards[0].records
            self.assertEqual(remaining, ())
            self.assertEqual((await scheduler._snapshot()).lane_count, 2)
            self.assertNotIn(old, scheduler._closing_grants)
        finally:
            executor.release_all()
            await asyncio.wait_for(scheduler._wait_for_idle(), timeout=1)
            coverage_tasks = {
                old_coverage,
                successor_coverage,
                sibling_coverage,
            }
            await asyncio.gather(
                *coverage_tasks,
                return_exceptions=True,
            )
            await asyncio.wait_for(scheduler.close(), timeout=1)

    async def test_distinct_slot_churn_retains_only_scalar_fences(
        self,
    ) -> None:
        scheduler = feed_work_scheduler.FeedWorkScheduler(
            typing.cast("typing.Any", _ImmediateExecutor())
        )
        await scheduler.start()
        grants = tuple(
            _grant(lease_key=str(150 + index), fencing_token=index + 1)
            for index in range(3)
        )

        try:
            for grant in grants:
                lane = _open_lane(scheduler, grant)
                coordinator = lane._boundary_coordinator
                self.assertEqual((await scheduler._snapshot()).lane_count, 1)

                result = await lane.close()

                self.assertEqual(
                    result,
                    feed_work_scheduler.LaneClosed(
                        grant,
                        feed_work_scheduler.LaneCloseReason.PLANNED_DRAIN,
                    ),
                )
                self.assertTrue(coordinator.task.done())
                self.assertEqual((await scheduler._snapshot()).lane_count, 0)
                self.assertNotIn(grant, scheduler._lanes)
                self.assertNotIn(grant, scheduler._closing_grants)

            self.assertEqual(scheduler._lanes, {})
            self.assertEqual(scheduler._closing_grants, set())
            self.assertEqual(
                scheduler._highest_fence,
                {
                    (grant.source_type, grant.lease_key): grant.fencing_token
                    for grant in grants
                },
            )
            self.assertTrue(
                all(
                    isinstance(fence, int)
                    for fence in scheduler._highest_fence.values()
                )
            )
        finally:
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
        lane = _open_lane(scheduler, grant)
        coverage = asyncio.create_task(
            lane.cover_page(
                calls=(_submission(uuid.UUID(int=1), 0),),
                boundaries=(),
                candidate=cursor_policy.LeaseCursor(grant, pos=None).prepare(
                    _SOURCE_TIME
                ),
            )
        )
        await asyncio.wait_for(executor.entered.wait(), timeout=1)
        old_worker = _require_not_none(scheduler._shards[0]._workers[0].task)
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
        replacement = _require_not_none(scheduler._shards[0]._workers[0].task)
        self.assertIsNot(replacement, old_worker)
        self.assertTrue(old_worker.done())
        self.assertFalse(replacement.done())
        with self.assertRaisesRegex(
            RuntimeError,
            "page candidate aborted by precommit_cancellation",
        ):
            await asyncio.wait_for(coverage, timeout=1)
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
        lane = _open_lane(scheduler, grant)
        coverage = asyncio.create_task(
            lane.cover_page(
                calls=(
                    _submission(uuid.UUID(int=1), 0),
                    _submission(uuid.UUID(int=2), 1),
                ),
                boundaries=(),
                candidate=cursor_policy.LeaseCursor(grant, pos=None).prepare(
                    _SOURCE_TIME
                ),
            )
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
        await asyncio.gather(coverage, return_exceptions=True)
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
        lane = _open_lane(scheduler, grant)
        successor_lane = _open_lane(scheduler, successor)
        removed_feed = uuid.UUID(int=1)
        sibling_feed = uuid.UUID(int=2)
        later_sibling_feed = uuid.UUID(int=3)
        cursor = cursor_policy.LeaseCursor(grant, pos=None)
        removed_member = _member(grant, removed_feed)
        calls = _TracingCalls(
            (
                _submission(
                    removed_feed,
                    0,
                    grant=grant,
                    member=removed_member,
                ),
                _submission(
                    removed_feed,
                    1,
                    grant=grant,
                    member=removed_member,
                ),
                _submission(
                    removed_feed,
                    2,
                    grant=grant,
                    member=removed_member,
                ),
                _submission(sibling_feed, 3, grant=grant),
                _submission(later_sibling_feed, 4, grant=grant),
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
            self.assertEqual(calls.pulled, [0, 1, 2, 3, 4])
            removed = await lane.remove_feed(removed_feed)
            await calls.wait_for_pulled(5)
            async with asyncio.timeout(1):
                while True:
                    barrier = (await lane._snapshot()).page
                    if barrier is not None and barrier.pulled == 5:
                        break
                    await asyncio.sleep(0)
            self.assertIsNotNone(barrier)
            self.assertEqual(barrier.pulled, 5)
            self.assertEqual(barrier.registered, 3)
            self.assertEqual(barrier.localized, 1)
            self.assertEqual(barrier.current_source_order, 4)
            self.assertFalse(coverage.done())
            executor.release_all()
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
            self.assertEqual(
                cursor.accept(_covered_settlement(receipt)),
                _SOURCE_TIME,
            )
            snapshot = await scheduler._snapshot()
            self.assertEqual(snapshot.held, 0)

            next_candidate = cursor.prepare(
                _SOURCE_TIME + datetime.timedelta(seconds=1)
            )
            removed_only = await lane.cover_page(
                calls=(_submission(removed_feed, 0, grant=grant),),
                boundaries=(),
                candidate=next_candidate,
            )
            self.assertEqual(
                cursor.accept(_covered_settlement(removed_only)),
                _SOURCE_TIME + datetime.timedelta(seconds=1),
            )
            self.assertEqual((await scheduler._snapshot()).held, 0)

            executor.release_all()
            await scheduler._wait_for_idle()
            successor_cursor = cursor_policy.LeaseCursor(
                successor,
                pos=None,
            )
            successor_receipt = await successor_lane.cover_page(
                calls=(_submission(removed_feed, 0, grant=successor),),
                boundaries=(),
                candidate=successor_cursor.prepare(_SOURCE_TIME),
            )
            self.assertEqual(
                successor_cursor.accept(_covered_settlement(successor_receipt)),
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
        executor = _GatedOutcomeExecutor(_membership_rejected)
        scheduler = feed_work_scheduler.FeedWorkScheduler(executor)
        await scheduler.start()
        grant = _grant()
        lane = _open_lane(scheduler, grant)
        removed_feed = uuid.UUID(int=8)
        sibling_feed = uuid.UUID(int=16)
        cursor = cursor_policy.LeaseCursor(grant, pos=None)
        first_coverage = asyncio.create_task(
            lane.cover_page(
                calls=(_submission(removed_feed, 0),),
                boundaries=(),
                candidate=cursor.prepare(_SOURCE_TIME),
            )
        )
        await asyncio.wait_for(executor.entered.wait(), timeout=1)
        executor.release.set()
        first = await asyncio.wait_for(first_coverage, timeout=1)
        cursor.accept(_covered_settlement(first))

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
        cursor.accept(_covered_settlement(second))
        await scheduler._wait_for_idle()
        self.assertEqual(executor.calls, 2)
        self.assertFalse((await scheduler._snapshot()).fatal)
        self.assertFalse((await lane._snapshot()).closing)
        await scheduler.close()

    async def test_typed_authority_loss_closes_its_exact_lane(self) -> None:
        executor = _GatedOutcomeExecutor(_authority_lost)
        scheduler = feed_work_scheduler.FeedWorkScheduler(executor)
        await scheduler.start()
        grant = _grant()
        lane = _open_lane(scheduler, grant)
        cursor = cursor_policy.LeaseCursor(grant, pos=None)
        coverage = asyncio.create_task(
            lane.cover_page(
                calls=(_submission(uuid.UUID(int=8), 0),),
                boundaries=(),
                candidate=cursor.prepare(_SOURCE_TIME),
            )
        )
        await asyncio.wait_for(executor.entered.wait(), timeout=1)
        executor.release.set()
        await asyncio.wait_for(lane._closing_event.wait(), timeout=1)
        await asyncio.gather(coverage, return_exceptions=True)

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
        lane = _open_lane(scheduler, grant)
        coverage = asyncio.create_task(
            lane.cover_page(
                calls=(_submission(uuid.UUID(int=1), 0),),
                boundaries=(),
                candidate=cursor_policy.LeaseCursor(grant, pos=None).prepare(
                    _SOURCE_TIME
                ),
            )
        )
        await asyncio.wait_for(executor.entered.wait(), timeout=1)
        worker = _require_not_none(scheduler._shards[0]._workers[0].task)
        cancel_entered = asyncio.Event()
        allow_cancel = asyncio.Event()
        cancel_exact = scheduler._cancel_exact

        async def gated_cancel_exact(
            exact_grant: ingestion_lease_store.LeaseGrant,
        ) -> None:
            cancel_entered.set()
            await allow_cancel.wait()
            await cancel_exact(exact_grant)

        cancel_method = "_cancel_exact"
        setattr(scheduler, cancel_method, gated_cancel_exact)
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
        await asyncio.gather(coverage, return_exceptions=True)

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
        first_lane = _open_lane(scheduler, first_grant)
        sibling_lane = _open_lane(scheduler, sibling_grant)
        first_cursor = cursor_policy.LeaseCursor(first_grant, pos=None)
        first_candidate = first_cursor.prepare(_SOURCE_TIME)
        first_coverage = asyncio.create_task(
            first_lane.cover_page(
                calls=(_submission(failing_feed, 0, grant=first_grant),),
                boundaries=(),
                candidate=first_candidate,
            )
        )
        await asyncio.wait_for(executor.failing_entered.wait(), timeout=1)
        sibling_cursor = cursor_policy.LeaseCursor(sibling_grant, pos=None)
        sibling_coverage = asyncio.create_task(
            sibling_lane.cover_page(
                calls=(_submission(healthy_feed, 0, grant=sibling_grant),),
                boundaries=(),
                candidate=sibling_cursor.prepare(_SOURCE_TIME),
            )
        )
        await asyncio.wait_for(executor.healthy_entered.wait(), timeout=1)
        executor.release_failure.set()
        await asyncio.wait_for(
            scheduler.integrity_failure_event.wait(),
            timeout=1,
        )
        self.assertTrue(scheduler.integrity_failure_event.is_set())
        with self.assertRaises(
            feed_work_scheduler.SchedulerIntegrityError
        ) as raised:
            scheduler.raise_if_failed()
        self.assertIsInstance(raised.exception.__cause__, RuntimeError)

        healthy_worker = _require_not_none(
            scheduler._shards[1]._workers[0].task
        )
        executor.release_healthy.set()
        await asyncio.wait_for(healthy_worker, timeout=1)
        _, pending = await asyncio.wait(
            {first_coverage, sibling_coverage},
            timeout=1,
        )
        self.assertEqual(
            pending,
            set(),
            tuple(
                (
                    task.get_name(),
                    tuple(
                        (frame.f_code.co_name, frame.f_lineno)
                        for frame in task.get_stack()
                    ),
                )
                for task in pending
            ),
        )
        page_results = await asyncio.gather(
            first_coverage,
            sibling_coverage,
            return_exceptions=True,
        )
        self.assertTrue(
            all(
                isinstance(
                    result,
                    (
                        feed_work_scheduler.SchedulerIntegrityError,
                        feed_work_scheduler.Undrained,
                    ),
                )
                for result in page_results
            )
        )
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
        lane = _open_lane(scheduler, grant)
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

    async def test_atomic_cohort_record_identity_and_lane_signal_view(  # noqa: PLR0915
        self,
    ) -> None:
        limits = _scheduler_types()._SchedulerLimits(
            shard_count=1,
            capacity=5,
            workers_per_shard=1,
            high_water=5,
            resume_at=2,
        )
        executor = _GatedOutcomeExecutor(_completed)
        scheduler = feed_work_scheduler.FeedWorkScheduler(
            executor,
            _limits=limits,
        )
        await scheduler.start()
        grant = _grant()
        stop_requested = asyncio.Event()
        grant_lost = asyncio.Event()
        lane = _open_lane(
            scheduler,
            grant,
            stop_requested=stop_requested,
            grant_lost=grant_lost,
        )
        hook_calls: list[
            tuple[feed_work_scheduler.CohortRecordIdentity, ...]
        ] = []
        observed = {source_order: [] for source_order in range(3)}

        def admit(
            identities: tuple[
                feed_work_scheduler.CohortRecordIdentity,
                ...,
            ],
        ) -> None:
            self.assertEqual(scheduler._shards[0]._held, 0)
            self.assertEqual(scheduler._shards[0]._records, {})
            hook_calls.append(identities)

        cohort = _cohort(
            uuid.UUID(int=1),
            (0, 1, 2),
            grant=grant,
            cohort_timestamp=_SOURCE_TIME,
            admission_hook=admit,
            settlement_observers={
                source_order: observed[source_order].append
                for source_order in range(3)
            },
        )
        try:
            coverage = asyncio.create_task(
                lane.cover_page(
                    calls=(cohort,),
                    boundaries=(),
                    candidate=cursor_policy.LeaseCursor(
                        grant,
                        pos=None,
                    ).prepare(_SOURCE_TIME),
                )
            )
            await asyncio.wait_for(executor.entered.wait(), timeout=1)

            self.assertEqual(executor.calls, 1)
            execution = executor.executions[0]
            self.assertEqual(len(execution.calls), 3)
            self.assertEqual(len(hook_calls), 1)
            identities = hook_calls[0]
            self.assertEqual(
                identities,
                tuple(call.identity for call in execution.calls),
            )
            self.assertEqual(
                tuple(identity.local_sequence for identity in identities),
                (0, 1, 2),
            )
            self.assertEqual(
                tuple(identity.source_order for identity in identities),
                (0, 1, 2),
            )
            self.assertTrue(
                all(identity.member is cohort.member for identity in identities)
            )
            snapshot = await scheduler._snapshot()
            self.assertEqual(snapshot.held, 3)
            self.assertEqual(snapshot.shards[0].active_calls, 3)
            self.assertEqual(snapshot.shards[0].queued_calls, 0)
            self.assertTrue(
                all(
                    not hasattr(record, "payload")
                    and not hasattr(record, "signals")
                    and not hasattr(record, "facts")
                    for record in snapshot.shards[0].records
                )
            )

            signals = execution.signals
            self.assertIs(lane.signals, signals)
            self.assertIs(signals.grant, grant)
            self.assertFalse(hasattr(signals.stop_requested, "set"))
            self.assertFalse(hasattr(signals.stop_requested, "clear"))
            self.assertFalse(hasattr(signals, "retention"))
            self.assertFalse(signals.stop_requested.is_set())
            stop_requested.set()
            self.assertTrue(signals.stop_requested.is_set())
            self.assertTrue(await signals.stop_requested.wait())
            self.assertFalse(signals.grant_lost.is_set())
            grant_lost.set()
            self.assertTrue(await signals.grant_lost.wait())

            executor.release.set()
            await asyncio.wait_for(coverage, timeout=1)
            await scheduler._wait_for_idle()
            self.assertEqual(
                observed,
                {
                    source_order: [feed_work_scheduler.CallSettlement.COMPLETED]
                    for source_order in range(3)
                },
            )
        finally:
            executor.release.set()
            await scheduler.close()

    async def test_member_identity_validation_precedes_all_mutation(
        self,
    ) -> None:
        executor = _ImmediateExecutor()
        scheduler = feed_work_scheduler.FeedWorkScheduler(executor)
        await scheduler.start()
        grant = _grant(fencing_token=2)
        lane = _open_lane(scheduler, grant)
        feed_id = uuid.UUID(int=8)
        other_feed = uuid.UUID(int=16)
        exact_member = _member(grant, feed_id)
        predecessor_member = _member(_grant(fencing_token=1), feed_id)
        successor_member = _member(_grant(fencing_token=3), feed_id)
        other_member = _member(grant, other_feed)
        forged_member = ingestion_lease_store.LeaseMemberIdentity(
            feed_id=feed_id,
            source_type=feed_store.SourceType.BCFY_CALLS,
            source_feed_id="150-8",
            sid="150",
            group_id="8",
        )
        hook_calls: list[object] = []

        def hook(
            identities: tuple[
                feed_work_scheduler.CohortRecordIdentity,
                ...,
            ],
        ) -> None:
            hook_calls.append(identities)

        def manual(
            *,
            member: ingestion_lease_store.LeaseMemberIdentity,
            call_feed: uuid.UUID = feed_id,
            call_timestamp: datetime.datetime | None = _SOURCE_TIME,
            payload: object,
        ) -> feed_work_scheduler.CohortSubmission:
            return feed_work_scheduler.CohortSubmission(
                member=member,
                feed_id=feed_id,
                cohort_timestamp=_SOURCE_TIME,
                calls=(
                    feed_work_scheduler.CallSubmission(
                        feed_id=call_feed,
                        source_timestamp=call_timestamp,
                        payload=payload,
                    ),
                ),
                admission_hook=hook,
            )

        cases = {
            "forged": manual(
                member=forged_member,
                payload={"member": forged_member},
            ),
            "predecessor_grant": manual(
                member=predecessor_member,
                payload={"member": predecessor_member},
            ),
            "successor_grant": manual(
                member=successor_member,
                payload={"member": successor_member},
            ),
            "crossed_payload_member": manual(
                member=exact_member,
                payload={"member": other_member},
            ),
            "crossed_call_feed": manual(
                member=exact_member,
                call_feed=other_feed,
                payload={"member": exact_member},
            ),
            "crossed_timestamp": manual(
                member=exact_member,
                call_timestamp=_SOURCE_TIME + datetime.timedelta(seconds=1),
                payload={"member": exact_member},
            ),
            "missing_payload_member": manual(
                member=exact_member,
                payload={"source_order": 0},
            ),
        }

        try:
            for name, cohort in cases.items():
                with self.subTest(name=name):
                    with self.assertRaises(
                        (
                            TypeError,
                            ValueError,
                            feed_work_scheduler.CohortIntegrityError,
                        )
                    ):
                        await lane.cover_page(
                            calls=(cohort,),
                            boundaries=(),
                            candidate=cursor_policy.LeaseCursor(
                                grant,
                                pos=None,
                            ).prepare(_SOURCE_TIME),
                        )
                    lane_snapshot = await lane._snapshot()
                    self.assertEqual(lane_snapshot.next_page_sequence, 0)
                    self.assertIsNone(lane_snapshot.page)
                    self.assertEqual((await scheduler._snapshot()).held, 0)
                    self.assertEqual(executor.sequences, [])
                    self.assertEqual(hook_calls, [])

            with self.assertRaises(ValueError):
                feed_work_scheduler.CohortSubmission(
                    member=other_member,
                    feed_id=feed_id,
                    cohort_timestamp=_SOURCE_TIME,
                    calls=(
                        feed_work_scheduler.CallSubmission(
                            feed_id=feed_id,
                            source_timestamp=_SOURCE_TIME,
                            payload={"member": other_member},
                        ),
                    ),
                    admission_hook=hook,
                )
            with self.assertRaises(ValueError):
                _cohort(
                    feed_id,
                    (0, 1),
                    grant=grant,
                    cohort_timestamp=None,
                )
        finally:
            await scheduler.close()

    async def test_admission_hook_rollback_reentry_and_oversized_atomicity(  # noqa: PLR0915
        self,
    ) -> None:
        limits = _scheduler_types()._SchedulerLimits(
            shard_count=1,
            capacity=2,
            workers_per_shard=1,
            high_water=2,
            resume_at=0,
        )
        executor = _ImmediateExecutor()
        scheduler = feed_work_scheduler.FeedWorkScheduler(
            executor,
            _limits=limits,
        )
        await scheduler.start()
        grant = _grant()
        lane = _open_lane(scheduler, grant)
        draft: list[object] = []
        hook_calls = 0

        def rollback_hook(
            identities: tuple[
                feed_work_scheduler.CohortRecordIdentity,
                ...,
            ],
        ) -> None:
            nonlocal hook_calls
            hook_calls += 1
            self.assertEqual(scheduler._shards[0]._held, 0)
            draft.extend(identities)
            draft.clear()
            message = "ledger admission rejected"
            raise RuntimeError(message)

        failed = _cohort(
            uuid.UUID(int=1),
            (0, 1),
            grant=grant,
            cohort_timestamp=_SOURCE_TIME,
            admission_hook=rollback_hook,
        )
        try:
            with self.assertRaisesRegex(RuntimeError, "ledger admission"):
                await lane.cover_page(
                    calls=(failed,),
                    boundaries=(),
                    candidate=cursor_policy.LeaseCursor(
                        grant,
                        pos=None,
                    ).prepare(_SOURCE_TIME),
                )
            self.assertEqual(hook_calls, 1)
            self.assertEqual(draft, [])
            self.assertEqual((await scheduler._snapshot()).held, 0)
            self.assertEqual(scheduler._shards[0]._next_sequence, 0)
            self.assertIsNone((await lane._snapshot()).page)

            with self.assertRaises(feed_work_scheduler.CohortIntegrityError):
                await lane.cover_page(
                    calls=(failed,),
                    boundaries=(),
                    candidate=cursor_policy.LeaseCursor(
                        grant,
                        pos=None,
                    ).prepare(_SOURCE_TIME),
                )
            self.assertEqual(hook_calls, 1)

            small_calls = 0
            oversized_calls = 0

            def small_hook(
                _identities: tuple[
                    feed_work_scheduler.CohortRecordIdentity,
                    ...,
                ],
            ) -> None:
                nonlocal small_calls
                small_calls += 1

            def oversized_hook(
                _identities: tuple[
                    feed_work_scheduler.CohortRecordIdentity,
                    ...,
                ],
            ) -> None:
                nonlocal oversized_calls
                oversized_calls += 1

            small = _cohort(
                uuid.UUID(int=2),
                (0,),
                grant=grant,
                cohort_timestamp=_SOURCE_TIME,
                admission_hook=small_hook,
            )
            oversized = _cohort(
                uuid.UUID(int=3),
                (1, 2, 3),
                grant=grant,
                cohort_timestamp=_SOURCE_TIME + datetime.timedelta(seconds=1),
                admission_hook=oversized_hook,
            )
            with self.assertRaisesRegex(ValueError, "capacity"):
                await lane.cover_page(
                    calls=(small, oversized),
                    boundaries=(),
                    candidate=cursor_policy.LeaseCursor(
                        grant,
                        pos=None,
                    ).prepare(_SOURCE_TIME),
                )
            self.assertEqual((small_calls, oversized_calls), (0, 0))
            self.assertEqual(executor.sequences, [])
            self.assertEqual((await scheduler._snapshot()).held, 0)

            reentrant_holder: dict[
                str,
                feed_work_scheduler.CohortSubmission,
            ] = {}

            def reentrant_hook(
                _identities: tuple[
                    feed_work_scheduler.CohortRecordIdentity,
                    ...,
                ],
            ) -> None:
                reentrant_holder["cohort"]._begin_admission()

            reentrant = _cohort(
                uuid.UUID(int=4),
                (0,),
                grant=grant,
                cohort_timestamp=_SOURCE_TIME,
                admission_hook=reentrant_hook,
            )
            reentrant_holder["cohort"] = reentrant
            with self.assertRaises(feed_work_scheduler.CohortIntegrityError):
                await lane.cover_page(
                    calls=(reentrant,),
                    boundaries=(),
                    candidate=cursor_policy.LeaseCursor(
                        grant,
                        pos=None,
                    ).prepare(_SOURCE_TIME),
                )
            self.assertEqual((await scheduler._snapshot()).held, 0)
            self.assertEqual(scheduler._shards[0]._next_sequence, 0)
        finally:
            await scheduler.close()

    async def test_replay_block_purges_queued_cohort_but_sibling_completes(
        self,
    ) -> None:
        limits = _scheduler_types()._SchedulerLimits(
            shard_count=1,
            capacity=8,
            workers_per_shard=2,
            high_water=8,
            resume_at=4,
        )
        failed_feed = uuid.UUID(int=1)
        sibling_feed = uuid.UUID(int=2)
        clock = _ControlledClock()
        executor = _ReplayBarrierExecutor(failed_feed)
        scheduler = feed_work_scheduler.FeedWorkScheduler(
            executor,
            _limits=limits,
            _monotonic=clock,
        )
        await scheduler.start()
        grant = _grant()
        lane = _open_lane(scheduler, grant)
        observed = {source_order: [] for source_order in range(3)}
        try:
            coverage = asyncio.create_task(
                lane.cover_page(
                    calls=(
                        _cohort(
                            failed_feed,
                            (0,),
                            grant=grant,
                            cohort_timestamp=_SOURCE_TIME,
                            settlement_observers={0: observed[0].append},
                        ),
                        _cohort(
                            failed_feed,
                            (1,),
                            grant=grant,
                            cohort_timestamp=(
                                _SOURCE_TIME + datetime.timedelta(seconds=1)
                            ),
                            settlement_observers={1: observed[1].append},
                        ),
                        _cohort(
                            sibling_feed,
                            (2,),
                            grant=grant,
                            cohort_timestamp=_SOURCE_TIME,
                            settlement_observers={2: observed[2].append},
                        ),
                    ),
                    boundaries=(),
                    candidate=cursor_policy.LeaseCursor(
                        grant,
                        pos=None,
                    ).prepare(_SOURCE_TIME),
                )
            )
            await executor.wait_for_started(2)
            self.assertIn((0,), executor.started)
            self.assertIn((2,), executor.started)
            self.assertNotIn((1,), executor.started)
            clock.advance(6.0)
            executor.release_failure.set()
            settled = await asyncio.wait_for(coverage, timeout=1)
            await scheduler._wait_for_idle()

            self.assertEqual(
                observed,
                {
                    0: [feed_work_scheduler.CallSettlement.REPLAY_SAFE_RELEASE],
                    1: [feed_work_scheduler.CallSettlement.REPLAY_BLOCKED],
                    2: [feed_work_scheduler.CallSettlement.COMPLETED],
                },
            )
            self.assertEqual((await scheduler._snapshot()).held, 0)
            evidence = _require_settled(settled).scheduler_evidence
            self.assertEqual(evidence.admitted_record_count, 3)
            self.assertEqual(evidence.admitted_cohort_count, 3)
            self.assertEqual(evidence.terminal_record_count, 3)
            self.assertEqual(evidence.replay_blocked_record_count, 1)
            self.assertEqual(evidence.total_queue_wait_seconds, 6.0)
            self.assertEqual(evidence.maximum_queue_wait_seconds, 6.0)
            self.assertEqual(evidence.oldest_queue_age_seconds, 6.0)
        finally:
            executor.release_failure.set()
            await scheduler.close()

    async def test_later_arriving_replay_block_never_dispatches(
        self,
    ) -> None:
        limits = _scheduler_types()._SchedulerLimits(
            shard_count=1,
            capacity=2,
            workers_per_shard=2,
            high_water=2,
            resume_at=0,
        )
        failed_feed = uuid.UUID(int=1)
        sibling_feed = uuid.UUID(int=2)
        executor = _ReplayBarrierExecutor(failed_feed)
        scheduler = feed_work_scheduler.FeedWorkScheduler(
            executor,
            _limits=limits,
        )
        await scheduler.start()
        grant = _grant()
        lane = _open_lane(scheduler, grant)
        observed = {source_order: [] for source_order in range(3)}
        coverage = asyncio.create_task(
            lane.cover_page(
                calls=(
                    _cohort(
                        failed_feed,
                        (0,),
                        grant=grant,
                        cohort_timestamp=_SOURCE_TIME,
                        settlement_observers={0: observed[0].append},
                    ),
                    _cohort(
                        sibling_feed,
                        (2,),
                        grant=grant,
                        cohort_timestamp=_SOURCE_TIME,
                        settlement_observers={2: observed[2].append},
                    ),
                    _cohort(
                        failed_feed,
                        (1,),
                        grant=grant,
                        cohort_timestamp=(
                            _SOURCE_TIME + datetime.timedelta(seconds=1)
                        ),
                        settlement_observers={1: observed[1].append},
                    ),
                ),
                boundaries=(),
                candidate=cursor_policy.LeaseCursor(
                    grant,
                    pos=None,
                ).prepare(_SOURCE_TIME),
            )
        )
        try:
            await scheduler._shards[0].wait_for_capacity_waiters(1)
            await executor.wait_for_started(2)
            self.assertFalse(coverage.done())
            self.assertNotIn((1,), executor.started)
            executor.release_failure.set()
            await asyncio.wait_for(coverage, timeout=1)
            await scheduler._wait_for_idle()

            self.assertNotIn((1,), executor.started)
            self.assertEqual(
                observed,
                {
                    0: [feed_work_scheduler.CallSettlement.REPLAY_SAFE_RELEASE],
                    1: [feed_work_scheduler.CallSettlement.REPLAY_BLOCKED],
                    2: [feed_work_scheduler.CallSettlement.COMPLETED],
                },
            )
        finally:
            executor.release_failure.set()
            if not coverage.done():
                coverage.cancel()
                await asyncio.gather(coverage, return_exceptions=True)
            await scheduler.close()

    async def test_terminal_facts_closure_state_and_disposition_matrix(
        self,
    ) -> None:
        self.assertEqual(
            set(feed_work_scheduler.CohortRecordClosureState),
            {
                feed_work_scheduler.CohortRecordClosureState.DURABLY_CLOSED,
                feed_work_scheduler.CohortRecordClosureState.FINAL_CLOSURE_PENDING,
                feed_work_scheduler.CohortRecordClosureState.REPLAY_SAFE_RELEASE,
                feed_work_scheduler.CohortRecordClosureState.OUTCOME_UNKNOWN,
            },
        )
        self.assertEqual(len(feed_work_scheduler.CohortTerminalDisposition), 9)
        self.assertEqual(
            len(feed_work_scheduler.CohortRecordTerminalReason), 11
        )
        grant = _grant()
        member = _member(grant, uuid.UUID(int=1))
        identity = feed_work_scheduler.CohortRecordIdentity(
            grant=grant,
            member=member,
            page_sequence=0,
            feed_id=uuid.UUID(int=1),
            cohort_timestamp=_SOURCE_TIME,
            source_order=0,
            local_sequence=0,
        )
        item = feed_work_scheduler.CohortItemFailureFact(
            feed_store.FeedStatusReason.SYSTEM_SOURCE_PAYLOAD_INVALID,
            "  bad\nitem   payload  ",
        )
        self.assertEqual(item.detail, "bad item payload")
        capped = feed_work_scheduler.CohortItemFailureFact(
            feed_store.FeedStatusReason.SYSTEM_SOURCE_PAYLOAD_INVALID,
            "x" * 4096,
        )
        self.assertEqual(len(capped.detail), 2048)
        with self.assertRaises(ValueError):
            feed_work_scheduler.CohortDirectFailureFact(
                feed_store.FeedStatusReason.SYSTEM_PIPELINE_ERROR,
                "response at https://secret.invalid/audio",
            )
        direct = feed_work_scheduler.CohortDirectFailureFact(
            feed_store.FeedStatusReason.SYSTEM_PIPELINE_ERROR,
            "selected publication failure",
        )

        def facts(
            disposition: feed_work_scheduler.CohortTerminalDisposition,
            closure: feed_work_scheduler.CohortRecordClosureState,
            reason: feed_work_scheduler.CohortRecordTerminalReason,
            *,
            participated: bool,
            item_failure: feed_work_scheduler.CohortItemFailureFact
            | None = None,
            direct_failure: feed_work_scheduler.CohortDirectFailureFact
            | None = None,
        ) -> feed_work_scheduler.CohortTerminalFacts:
            return feed_work_scheduler.CohortTerminalFacts(
                records=(
                    feed_work_scheduler.CohortRecordTerminalFact(
                        identity=identity,
                        participated=participated,
                        closure_state=closure,
                        full_pipeline_completed=(
                            reason
                            is feed_work_scheduler.CohortRecordTerminalReason.FULL_PIPELINE
                        ),
                        terminal_reason=reason,
                        item_failure=item_failure,
                        direct_failure=direct_failure,
                    ),
                ),
                disposition=disposition,
            )

        completed_skip = facts(
            feed_work_scheduler.CohortTerminalDisposition.SETTLED,
            feed_work_scheduler.CohortRecordClosureState.DURABLY_CLOSED,
            feed_work_scheduler.CohortRecordTerminalReason.TERMINAL_ITEM_SKIP,
            participated=True,
            item_failure=item,
        )
        completed = feed_work_scheduler.CallCompleted(completed_skip)
        self.assertFalse(completed.facts.records[0].full_pipeline_completed)
        outcomes = (
            completed,
            feed_work_scheduler.CallFinalClosurePending(
                facts(
                    feed_work_scheduler.CohortTerminalDisposition.FINAL_CLOSURE_PENDING,
                    feed_work_scheduler.CohortRecordClosureState.FINAL_CLOSURE_PENDING,
                    feed_work_scheduler.CohortRecordTerminalReason.TERMINAL_ITEM_SKIP,
                    participated=True,
                    item_failure=item,
                )
            ),
            feed_work_scheduler.CallReplayableDirectFailure(
                facts(
                    feed_work_scheduler.CohortTerminalDisposition.REPLAYABLE_DIRECT,
                    feed_work_scheduler.CohortRecordClosureState.REPLAY_SAFE_RELEASE,
                    feed_work_scheduler.CohortRecordTerminalReason.REPLAYABLE_DIRECT,
                    participated=False,
                    direct_failure=direct,
                )
            ),
            feed_work_scheduler.CallRetryable(
                facts(
                    feed_work_scheduler.CohortTerminalDisposition.RETRYABLE,
                    feed_work_scheduler.CohortRecordClosureState.REPLAY_SAFE_RELEASE,
                    feed_work_scheduler.CohortRecordTerminalReason.RETRYABLE,
                    participated=False,
                )
            ),
            feed_work_scheduler.CallStopped(
                facts(
                    feed_work_scheduler.CohortTerminalDisposition.STOPPED,
                    feed_work_scheduler.CohortRecordClosureState.REPLAY_SAFE_RELEASE,
                    feed_work_scheduler.CohortRecordTerminalReason.STOPPED,
                    participated=False,
                )
            ),
            feed_work_scheduler.CallAuthorityLost(
                facts(
                    feed_work_scheduler.CohortTerminalDisposition.AUTHORITY_LOST,
                    feed_work_scheduler.CohortRecordClosureState.REPLAY_SAFE_RELEASE,
                    feed_work_scheduler.CohortRecordTerminalReason.AUTHORITY_LOST,
                    participated=False,
                )
            ),
            feed_work_scheduler.CallMembershipRejected(
                facts(
                    feed_work_scheduler.CohortTerminalDisposition.MEMBERSHIP_REJECTED,
                    feed_work_scheduler.CohortRecordClosureState.REPLAY_SAFE_RELEASE,
                    feed_work_scheduler.CohortRecordTerminalReason.MEMBERSHIP_REJECTED,
                    participated=False,
                )
            ),
            feed_work_scheduler.CallIntegrityFailure(
                facts(
                    feed_work_scheduler.CohortTerminalDisposition.INTEGRITY_FAILURE,
                    feed_work_scheduler.CohortRecordClosureState.OUTCOME_UNKNOWN,
                    feed_work_scheduler.CohortRecordTerminalReason.INTEGRITY_FAILURE,
                    participated=True,
                ),
                RuntimeError("malformed facts"),
            ),
            feed_work_scheduler.CallOutcomeUnknown(
                facts(
                    feed_work_scheduler.CohortTerminalDisposition.OUTCOME_UNKNOWN,
                    feed_work_scheduler.CohortRecordClosureState.OUTCOME_UNKNOWN,
                    feed_work_scheduler.CohortRecordTerminalReason.OUTCOME_UNKNOWN,
                    participated=True,
                )
            ),
        )
        self.assertEqual(
            {outcome.facts.disposition for outcome in outcomes},
            set(feed_work_scheduler.CohortTerminalDisposition),
        )
        with self.assertRaises(feed_work_scheduler.CohortIntegrityError):
            feed_work_scheduler.CohortRecordTerminalFact(
                identity=identity,
                participated=False,
                closure_state=(
                    feed_work_scheduler.CohortRecordClosureState.DURABLY_CLOSED
                ),
                full_pipeline_completed=False,
                terminal_reason=(
                    feed_work_scheduler.CohortRecordTerminalReason.PUBLICATION_ABANDONED
                ),
            )
        with self.assertRaises(feed_work_scheduler.CohortIntegrityError):
            feed_work_scheduler.CohortRecordTerminalFact(
                identity=identity,
                participated=True,
                closure_state=(
                    feed_work_scheduler.CohortRecordClosureState.FINAL_CLOSURE_PENDING
                ),
                full_pipeline_completed=False,
                terminal_reason=(
                    feed_work_scheduler.CohortRecordTerminalReason.TERMINAL_ITEM_SKIP
                ),
            )

    async def test_crossed_terminal_facts_retain_outcome_unknown(
        self,
    ) -> None:
        scheduler = feed_work_scheduler.FeedWorkScheduler(
            _CrossedTerminalFactsExecutor()
        )
        await scheduler.start()
        grant = _grant()
        lane = _open_lane(scheduler, grant)
        observed: list[object] = []
        result = await lane.cover_page(
            calls=(
                _cohort(
                    uuid.UUID(int=8),
                    (0,),
                    grant=grant,
                    cohort_timestamp=_SOURCE_TIME,
                    settlement_observers={0: observed.append},
                ),
            ),
            boundaries=(),
            candidate=cursor_policy.LeaseCursor(
                grant,
                pos=None,
            ).prepare(_SOURCE_TIME),
        )
        self.assertIsInstance(result, feed_work_scheduler.Undrained)
        async with asyncio.timeout(1):
            while True:
                snapshot = await scheduler._snapshot()
                if (
                    snapshot.shards[0].records[0].state.value
                    == "outcome_unknown"
                ):
                    break
                await asyncio.sleep(0)
        self.assertEqual(snapshot.held, 1)
        self.assertEqual(observed, [])
        self.assertFalse(snapshot.fatal)
        self.assertIsInstance(
            await asyncio.wait_for(
                lane.close(feed_work_scheduler.LaneCloseReason.AUTHORITY_LOSS),
                timeout=1,
            ),
            feed_work_scheduler.Undrained,
        )
        self.assertIsInstance(
            await scheduler.close(), feed_work_scheduler.Undrained
        )

    async def test_final_closure_pending_known_abort_matrix(self) -> None:
        cases = (_stopped, _retryable, _authority_lost)
        for abort_factory in cases:
            with self.subTest(outcome=abort_factory.__name__):
                limits = _scheduler_types()._SchedulerLimits(
                    shard_count=2,
                    capacity=4,
                    workers_per_shard=1,
                    high_water=4,
                    resume_at=2,
                )
                executor = _PageAbortExecutor(abort_factory)
                scheduler = feed_work_scheduler.FeedWorkScheduler(
                    executor,
                    _limits=limits,
                )
                await scheduler.start()
                grant = _grant()
                lane = _open_lane(scheduler, grant)
                observed = {0: [], 1: []}
                coverage = asyncio.create_task(
                    lane.cover_page(
                        calls=(
                            _cohort(
                                uuid.UUID(int=1),
                                (0,),
                                grant=grant,
                                cohort_timestamp=_SOURCE_TIME,
                                settlement_observers={0: observed[0].append},
                            ),
                            _cohort(
                                uuid.UUID(int=2),
                                (1,),
                                grant=grant,
                                cohort_timestamp=_SOURCE_TIME,
                                settlement_observers={1: observed[1].append},
                            ),
                        ),
                        boundaries=(),
                        candidate=cursor_policy.LeaseCursor(
                            grant,
                            pos=None,
                        ).prepare(_SOURCE_TIME),
                    )
                )
                await asyncio.wait_for(executor.abort_entered.wait(), timeout=1)
                async with asyncio.timeout(1):
                    while True:
                        snapshot = await scheduler._snapshot()
                        states = {
                            record.state.value
                            for shard in snapshot.shards
                            for record in shard.records
                        }
                        if "final_closure_pending" in states:
                            break
                        await asyncio.sleep(0)
                executor.release_abort.set()
                if abort_factory is _authority_lost:
                    await asyncio.wait_for(
                        lane._closing_event.wait(), timeout=1
                    )
                    await asyncio.wait_for(
                        lane.close(
                            feed_work_scheduler.LaneCloseReason.AUTHORITY_LOSS
                        ),
                        timeout=1,
                    )
                else:
                    await scheduler._wait_for_idle()

                self.assertEqual(
                    observed,
                    {
                        0: [
                            feed_work_scheduler.CallSettlement.REPLAY_SAFE_RELEASE
                        ],
                        1: [
                            feed_work_scheduler.CallSettlement.REPLAY_SAFE_RELEASE
                        ],
                    },
                )
                self.assertEqual((await scheduler._snapshot()).held, 0)
                await asyncio.gather(coverage, return_exceptions=True)
                await scheduler.close()

    async def test_precommit_cancellation_releases_final_pending_once(
        self,
    ) -> None:
        limits = _scheduler_types()._SchedulerLimits(
            shard_count=2,
            capacity=4,
            workers_per_shard=1,
            high_water=4,
            resume_at=2,
        )
        executor = _PageAbortExecutor(_completed)
        scheduler = feed_work_scheduler.FeedWorkScheduler(
            executor,
            _limits=limits,
        )
        await scheduler.start()
        grant = _grant()
        lane = _open_lane(scheduler, grant)
        observed = {0: [], 1: []}
        coverage = asyncio.create_task(
            lane.cover_page(
                calls=(
                    _cohort(
                        uuid.UUID(int=1),
                        (0,),
                        grant=grant,
                        cohort_timestamp=_SOURCE_TIME,
                        settlement_observers={0: observed[0].append},
                    ),
                    _cohort(
                        uuid.UUID(int=2),
                        (1,),
                        grant=grant,
                        cohort_timestamp=_SOURCE_TIME,
                        settlement_observers={1: observed[1].append},
                    ),
                ),
                boundaries=(),
                candidate=cursor_policy.LeaseCursor(
                    grant,
                    pos=None,
                ).prepare(_SOURCE_TIME),
            )
        )
        await asyncio.wait_for(executor.abort_entered.wait(), timeout=1)
        result = await asyncio.wait_for(
            lane.close(feed_work_scheduler.LaneCloseReason.AUTHORITY_LOSS),
            timeout=1,
        )
        self.assertIsInstance(result, feed_work_scheduler.LaneClosed)
        self.assertEqual(
            observed,
            {
                0: [feed_work_scheduler.CallSettlement.REPLAY_SAFE_RELEASE],
                1: [feed_work_scheduler.CallSettlement.REPLAY_SAFE_RELEASE],
            },
        )
        self.assertEqual((await scheduler._snapshot()).held, 0)
        await asyncio.gather(coverage, return_exceptions=True)
        await scheduler.close()

    async def test_outcome_unknown_suppresses_page_abort_cleanup(
        self,
    ) -> None:
        limits = _scheduler_types()._SchedulerLimits(
            shard_count=2,
            capacity=5,
            workers_per_shard=2,
            high_water=5,
            resume_at=2,
        )
        executor = _UnknownPageAbortExecutor()
        scheduler = feed_work_scheduler.FeedWorkScheduler(
            executor,
            _limits=limits,
        )
        await scheduler.start()
        grant = _grant()
        lane = _open_lane(scheduler, grant)
        observed = {0: [], 1: [], 2: []}
        stopped_settled = asyncio.Event()

        def observe_stopped(settlement: object) -> None:
            observed[2].append(settlement)
            stopped_settled.set()

        coverage = asyncio.create_task(
            lane.cover_page(
                calls=tuple(
                    _cohort(
                        uuid.UUID(int=source_order + 1),
                        (source_order,),
                        grant=grant,
                        cohort_timestamp=_SOURCE_TIME,
                        settlement_observers={
                            source_order: (
                                observe_stopped
                                if source_order == 2
                                else observed[source_order].append
                            )
                        },
                    )
                    for source_order in range(3)
                ),
                boundaries=(),
                candidate=cursor_policy.LeaseCursor(
                    grant,
                    pos=None,
                ).prepare(_SOURCE_TIME),
            )
        )
        await asyncio.wait_for(executor.abort_entered.wait(), timeout=1)
        async with asyncio.timeout(1):
            while True:
                snapshot = await scheduler._snapshot()
                states = [
                    record.state.value
                    for shard in snapshot.shards
                    for record in shard.records
                ]
                if (
                    "final_closure_pending" in states
                    and "outcome_unknown" in states
                ):
                    break
                await asyncio.sleep(0)
        executor.release_abort.set()
        self.assertIsInstance(
            await asyncio.wait_for(coverage, timeout=1),
            feed_work_scheduler.Undrained,
        )
        await asyncio.wait_for(stopped_settled.wait(), timeout=1)
        snapshot = await scheduler._snapshot()
        self.assertEqual(snapshot.held, 2)
        self.assertEqual(observed[0], [])
        self.assertEqual(observed[1], [])
        self.assertEqual(
            observed[2],
            [feed_work_scheduler.CallSettlement.REPLAY_SAFE_RELEASE],
        )
        self.assertIsInstance(
            await asyncio.wait_for(
                lane.close(feed_work_scheduler.LaneCloseReason.AUTHORITY_LOSS),
                timeout=1,
            ),
            feed_work_scheduler.Undrained,
        )
        self.assertEqual((await scheduler._snapshot()).held, 2)
        self.assertEqual(observed[0], [])
        self.assertIsInstance(
            await scheduler.close(), feed_work_scheduler.Undrained
        )

    async def test_cancellation_handoff_and_retention_handle_are_disjoint(
        self,
    ) -> None:
        for unknown in (False, True):
            with self.subTest(unknown=unknown):
                limits = _scheduler_types()._SchedulerLimits(
                    shard_count=1,
                    capacity=3,
                    workers_per_shard=1,
                    high_water=3,
                    resume_at=1,
                )
                executor = _CancellationCapabilityExecutor(unknown=unknown)
                scheduler = feed_work_scheduler.FeedWorkScheduler(
                    executor,
                    _limits=limits,
                )
                await scheduler.start()
                grant = _grant()
                lane = _open_lane(scheduler, grant)
                observed = {0: [], 1: []}
                coverage = asyncio.create_task(
                    lane.cover_page(
                        calls=(
                            _cohort(
                                uuid.UUID(int=1),
                                (0, 1),
                                grant=grant,
                                cohort_timestamp=_SOURCE_TIME,
                                settlement_observers={
                                    0: observed[0].append,
                                    1: observed[1].append,
                                },
                            ),
                        ),
                        boundaries=(),
                        candidate=cursor_policy.LeaseCursor(
                            grant,
                            pos=None,
                        ).prepare(_SOURCE_TIME),
                    )
                )
                await asyncio.wait_for(executor.entered.wait(), timeout=1)
                worker = scheduler._shards[0]._workers[0].task
                self.assertIsNotNone(worker)
                result = await asyncio.wait_for(
                    lane.close(
                        feed_work_scheduler.LaneCloseReason.AUTHORITY_LOSS
                    ),
                    timeout=1,
                )
                await asyncio.wait_for(
                    executor.handoff_complete.wait(),
                    timeout=1,
                )
                self.assertIsNotNone(executor.caught)
                with self.assertRaises(asyncio.CancelledError) as propagated:
                    await typing.cast("asyncio.Task[None]", worker)
                self.assertIsInstance(
                    propagated.exception, asyncio.CancelledError
                )
                self.assertTrue(executor.reraised_same_object)
                await asyncio.gather(coverage, return_exceptions=True)

                if unknown:
                    self.assertIsInstance(result, feed_work_scheduler.Undrained)
                    snapshot = await scheduler._snapshot()
                    self.assertEqual(snapshot.held, 2)
                    self.assertEqual(snapshot.shards[0].active_calls, 0)
                    self.assertTrue(
                        all(
                            record.state.value == "outcome_unknown"
                            for record in snapshot.shards[0].records
                        )
                    )
                    self.assertEqual(observed, {0: [], 1: []})
                    self.assertIsInstance(
                        await scheduler.close(),
                        feed_work_scheduler.Undrained,
                    )
                else:
                    self.assertIsInstance(
                        result, feed_work_scheduler.LaneClosed
                    )
                    self.assertEqual((await scheduler._snapshot()).held, 0)
                    self.assertEqual(
                        observed,
                        {
                            0: [feed_work_scheduler.CallSettlement.COMPLETED],
                            1: [feed_work_scheduler.CallSettlement.COMPLETED],
                        },
                    )
                    await scheduler.close()


class TestPageFinalization(unittest.IsolatedAsyncioTestCase):
    async def test_default_finalizer_prefers_exact_member_retirement(
        self,
    ) -> None:
        grant = _grant()
        feed_id = uuid.UUID(int=89)
        member = _member(grant, feed_id)
        candidate = cursor_policy.LeaseCursor(
            grant,
            pos=_SOURCE_TIME,
        ).prepare_no_progress()
        identity = feed_work_scheduler.CohortRecordIdentity(
            grant=grant,
            member=member,
            page_sequence=candidate.page_sequence,
            feed_id=feed_id,
            cohort_timestamp=None,
            source_order=0,
            local_sequence=0,
        )
        failure = feed_work_scheduler.CohortItemFailureFact(
            feed_store.FeedStatusReason.SYSTEM_SOURCE_PAYLOAD_INVALID,
            "terminal item skip",
        )
        facts = feed_work_scheduler.CohortTerminalFacts(
            records=(
                feed_work_scheduler.CohortRecordTerminalFact(
                    identity=identity,
                    participated=True,
                    closure_state=(
                        feed_work_scheduler.CohortRecordClosureState.FINAL_CLOSURE_PENDING
                    ),
                    full_pipeline_completed=False,
                    terminal_reason=(
                        feed_work_scheduler.CohortRecordTerminalReason.TERMINAL_ITEM_SKIP
                    ),
                    item_failure=failure,
                ),
            ),
            disposition=(
                feed_work_scheduler.CohortTerminalDisposition.FINAL_CLOSURE_PENDING
            ),
        )
        context = feed_work_scheduler.PageFinalizationContext(
            grant=grant,
            page_sequence=candidate.page_sequence,
            candidate=candidate,
            cohort_terminal_facts=(facts,),
            unresolved_replay_feed_ids=(),
            locally_retired_members=(member,),
            replay_blocked_feed_ids=(),
            candidate_boundaries=(),
        )
        scheduler_module = importlib.import_module(
            "backend.pipeline.ingestion.feed_work_scheduler._scheduler"
        )

        result = await scheduler_module._DefaultPageFinalizer().finalize_page(
            context
        )

        self.assertIs(type(result), feed_work_scheduler.FinalPageNoProgress)
        assert isinstance(result, feed_work_scheduler.FinalPageNoProgress)
        self.assertEqual(result.member_retirements, (member,))
        self.assertEqual(
            result.final_closure_resolutions[0].release_basis,
            feed_work_scheduler.FinalRecordReleaseBasis.ACCEPTED_MEMBER_RETIREMENT,
        )

    async def test_terminal_barrier_waits_for_exact_record_conservation(
        self,
    ) -> None:
        executor = _GatedOutcomeExecutor(_completed)
        finalizer = _ControlledPageFinalizer()
        scheduler = feed_work_scheduler.FeedWorkScheduler(
            executor,
            page_finalizer=finalizer,
        )
        await scheduler.start()
        grant = _grant()
        lane = _open_lane(scheduler, grant)
        cursor = cursor_policy.LeaseCursor(grant, pos=None)
        coverage = asyncio.create_task(
            lane.cover_page(
                calls=(
                    _cohort(
                        uuid.UUID(int=81),
                        (0, 1),
                        grant=grant,
                        cohort_timestamp=_SOURCE_TIME,
                    ),
                ),
                boundaries=(),
                candidate=cursor.prepare(_SOURCE_TIME),
            )
        )
        try:
            await asyncio.wait_for(executor.entered.wait(), timeout=1)
            barrier = _require_not_none((await lane._snapshot()).page)
            self.assertIsNotNone(barrier)
            self.assertEqual(barrier.pulled_records, 2)
            self.assertEqual(barrier.registered_records, 2)
            self.assertEqual(barrier.locally_rejected_records, 0)
            self.assertEqual(barrier.terminalized_registered_records, 0)
            self.assertFalse(finalizer.entered.is_set())
            self.assertFalse(coverage.done())

            executor.release.set()
            await asyncio.wait_for(finalizer.entered.wait(), timeout=1)
            context = finalizer.contexts[0]
            self.assertEqual(len(context.cohort_terminal_facts), 1)
            self.assertEqual(
                len(context.cohort_terminal_facts[0].records),
                2,
            )
            self.assertFalse(coverage.done())
            finalizer.release.set()
            receipt = await asyncio.wait_for(coverage, timeout=1)
            self.assertEqual(
                cursor.accept(_covered_settlement(receipt)),
                _SOURCE_TIME,
            )
            self.assertEqual((await scheduler._snapshot()).held, 0)
        finally:
            executor.release.set()
            finalizer.release.set()
            if not coverage.done():
                coverage.cancel()
                await asyncio.gather(coverage, return_exceptions=True)
            await scheduler.close()

    async def test_terminal_barrier_local_rejection_is_not_registration(
        self,
    ) -> None:
        finalizer = _ControlledPageFinalizer()
        scheduler = feed_work_scheduler.FeedWorkScheduler(
            _ImmediateExecutor(),
            page_finalizer=finalizer,
        )
        await scheduler.start()
        grant = _grant()
        lane = _open_lane(scheduler, grant)
        retired_feed = uuid.UUID(int=82)
        sibling_feed = uuid.UUID(int=83)
        await lane.remove_feed(retired_feed)
        retired_submission = _submission(retired_feed, 0, grant=grant)
        sibling_submission = _submission(sibling_feed, 1, grant=grant)
        cursor = cursor_policy.LeaseCursor(grant, pos=None)
        coverage = asyncio.create_task(
            lane.cover_page(
                calls=(
                    retired_submission,
                    sibling_submission,
                ),
                boundaries=(),
                candidate=cursor.prepare(_SOURCE_TIME),
            )
        )
        try:
            await asyncio.wait_for(finalizer.entered.wait(), timeout=1)
            barrier = _require_not_none((await lane._snapshot()).page)
            self.assertIsNotNone(barrier)
            self.assertEqual(barrier.pulled_records, 2)
            self.assertEqual(barrier.registered_records, 1)
            self.assertEqual(barrier.locally_rejected_records, 1)
            self.assertEqual(barrier.terminalized_registered_records, 1)
            context = finalizer.contexts[0]
            self.assertEqual(len(context.cohort_terminal_facts), 1)
            self.assertEqual(
                context.cohort_terminal_facts[0].records[0].identity.feed_id,
                sibling_feed,
            )
            self.assertEqual(len(context.locally_retired_members), 1)
            self.assertIs(
                context.locally_retired_members[0],
                retired_submission.member,
            )
            finalizer.release.set()
            receipt = await asyncio.wait_for(coverage, timeout=1)
            self.assertEqual(
                cursor.accept(_covered_settlement(receipt)),
                _SOURCE_TIME,
            )
        finally:
            finalizer.release.set()
            if not coverage.done():
                coverage.cancel()
                await asyncio.gather(coverage, return_exceptions=True)
            await scheduler.close()

    async def test_final_closure_pending_waits_for_atomic_resolution(
        self,
    ) -> None:
        observed: list[object] = []
        source_evidence = object()

        def resolve(
            context: feed_work_scheduler.PageFinalizationContext,
        ) -> object:
            pending = context.cohort_terminal_facts[0].records[0].identity
            resolution = feed_work_scheduler.FinalRecordClosureResolution(
                identity=pending,
                closure_state=(
                    feed_work_scheduler.CohortRecordClosureState.DURABLY_CLOSED
                ),
                release_basis=(
                    feed_work_scheduler.FinalRecordReleaseBasis.DURABLE_SOURCE_CLOSURE
                ),
            )
            return _covered_final_page(
                context,
                resolutions=(resolution,),
                source_evidence=source_evidence,
            )

        finalizer = _ControlledPageFinalizer(resolve)
        scheduler = feed_work_scheduler.FeedWorkScheduler(
            _GatedOutcomeExecutor(_final_closure_pending),
            page_finalizer=finalizer,
        )
        await scheduler.start()
        grant = _grant()
        lane = _open_lane(scheduler, grant)
        cursor = cursor_policy.LeaseCursor(grant, pos=None)
        coverage = asyncio.create_task(
            lane.cover_page(
                calls=(
                    _cohort(
                        uuid.UUID(int=84),
                        (0,),
                        grant=grant,
                        cohort_timestamp=None,
                        settlement_observers={0: observed.append},
                    ),
                ),
                boundaries=(),
                candidate=cursor.prepare(_SOURCE_TIME),
            )
        )
        executor = typing.cast(
            "_GatedOutcomeExecutor", scheduler._shards[0]._executor
        )
        try:
            await asyncio.wait_for(executor.entered.wait(), timeout=1)
            executor.release.set()
            await asyncio.wait_for(finalizer.entered.wait(), timeout=1)
            self.assertEqual(observed, [])
            self.assertEqual((await scheduler._snapshot()).held, 1)
            finalizer.release.set()
            receipt = await asyncio.wait_for(coverage, timeout=1)
            settled_receipt = _require_settled(receipt)
            self.assertEqual(
                cursor.accept(_covered_settlement(receipt)),
                _SOURCE_TIME,
            )
            self.assertIs(settled_receipt.source_evidence, source_evidence)
            self.assertEqual(len(settled_receipt.final_closure_resolutions), 1)
            self.assertEqual(
                observed,
                [feed_work_scheduler.CallSettlement.COMPLETED],
            )
            self.assertEqual((await scheduler._snapshot()).held, 0)
        finally:
            executor.release.set()
            finalizer.release.set()
            if not coverage.done():
                coverage.cancel()
                await asyncio.gather(coverage, return_exceptions=True)
            await scheduler.close()

    async def test_replayable_finalization_releases_same_feed_pending(
        self,
    ) -> None:
        feed_id = uuid.UUID(int=85)
        observed = {0: [], 1: []}

        def replay(
            context: feed_work_scheduler.PageFinalizationContext,
        ) -> object:
            pending = next(
                fact.identity
                for facts in context.cohort_terminal_facts
                for fact in facts.records
                if fact.closure_state
                is feed_work_scheduler.CohortRecordClosureState.FINAL_CLOSURE_PENDING
            )
            resolution = feed_work_scheduler.FinalRecordClosureResolution(
                identity=pending,
                closure_state=(
                    feed_work_scheduler.CohortRecordClosureState.REPLAY_SAFE_RELEASE
                ),
                release_basis=(
                    feed_work_scheduler.FinalRecordReleaseBasis.ACCEPTED_REPLAYABLE_FEED
                ),
            )
            return feed_work_scheduler.FinalPageReplayable(
                grant=context.grant,
                page_sequence=context.page_sequence,
                candidate=context.candidate,
                boundary_results=_accepted_boundary_results(context),
                final_closure_resolutions=(resolution,),
                source_evidence=None,
            )

        finalizer = _ControlledPageFinalizer(replay)
        executor = _PerOrderOutcomeExecutor(
            {0: _final_closure_pending, 1: _replayable_direct}
        )
        scheduler = feed_work_scheduler.FeedWorkScheduler(
            executor,
            page_finalizer=finalizer,
        )
        await scheduler.start()
        grant = _grant()
        lane = _open_lane(scheduler, grant)
        cursor = cursor_policy.LeaseCursor(grant, pos=None)
        coverage = asyncio.create_task(
            lane.cover_page(
                calls=(
                    _cohort(
                        feed_id,
                        (0,),
                        grant=grant,
                        cohort_timestamp=None,
                        settlement_observers={0: observed[0].append},
                    ),
                    _cohort(
                        feed_id,
                        (1,),
                        grant=grant,
                        cohort_timestamp=_SOURCE_TIME,
                        settlement_observers={1: observed[1].append},
                    ),
                ),
                boundaries=(),
                candidate=cursor.prepare(_SOURCE_TIME),
            )
        )
        try:
            await asyncio.wait_for(finalizer.entered.wait(), timeout=1)
            context = finalizer.contexts[0]
            self.assertEqual(executor.executed, [0, 1])
            self.assertEqual(context.unresolved_replay_feed_ids, (feed_id,))
            self.assertEqual(context.replay_blocked_feed_ids, (feed_id,))
            self.assertEqual(observed[0], [])
            finalizer.release.set()
            receipt = await asyncio.wait_for(coverage, timeout=1)
            cursor.accept_replayable(_replayable_settlement(receipt))
            self.assertEqual(
                observed,
                {
                    0: [feed_work_scheduler.CallSettlement.REPLAY_SAFE_RELEASE],
                    1: [feed_work_scheduler.CallSettlement.REPLAY_SAFE_RELEASE],
                },
            )
            self.assertFalse(
                await scheduler._shard_for(feed_id).is_replay_blocked(
                    grant,
                    0,
                    feed_id,
                )
            )
        finally:
            finalizer.release.set()
            if not coverage.done():
                coverage.cancel()
                await asyncio.gather(coverage, return_exceptions=True)
            await scheduler.close()

    async def test_final_closure_pending_malformed_resolution_is_atomic(
        self,
    ) -> None:
        observed = {0: [], 1: []}

        def omit_one(
            context: feed_work_scheduler.PageFinalizationContext,
        ) -> object:
            identity = context.cohort_terminal_facts[0].records[0].identity
            return _covered_final_page(
                context,
                resolutions=(
                    feed_work_scheduler.FinalRecordClosureResolution(
                        identity=identity,
                        closure_state=(
                            feed_work_scheduler.CohortRecordClosureState.DURABLY_CLOSED
                        ),
                        release_basis=(
                            feed_work_scheduler.FinalRecordReleaseBasis.DURABLE_SOURCE_CLOSURE
                        ),
                    ),
                ),
            )

        finalizer = _ControlledPageFinalizer(omit_one)
        scheduler = feed_work_scheduler.FeedWorkScheduler(
            _PerOrderOutcomeExecutor(
                {0: _final_closure_pending, 1: _final_closure_pending}
            ),
            page_finalizer=finalizer,
        )
        await scheduler.start()
        grant = _grant()
        lane = _open_lane(scheduler, grant)
        coverage = asyncio.create_task(
            lane.cover_page(
                calls=tuple(
                    _cohort(
                        uuid.UUID(int=90 + source_order),
                        (source_order,),
                        grant=grant,
                        cohort_timestamp=None,
                        settlement_observers={
                            source_order: observed[source_order].append
                        },
                    )
                    for source_order in range(2)
                ),
                boundaries=(),
                candidate=cursor_policy.LeaseCursor(
                    grant,
                    pos=None,
                ).prepare(_SOURCE_TIME),
            )
        )
        try:
            await asyncio.wait_for(finalizer.entered.wait(), timeout=1)
            self.assertEqual((await scheduler._snapshot()).held, 2)
            finalizer.release.set()
            self.assertIsInstance(
                await asyncio.wait_for(coverage, timeout=1),
                feed_work_scheduler.Undrained,
            )
            self.assertEqual(observed, {0: [], 1: []})
            self.assertEqual((await scheduler._snapshot()).held, 2)
            self.assertEqual(
                {
                    uncertainty
                    for shard in scheduler._shards
                    for uncertainty in shard._uncertain_final_pages.values()
                },
                {_scheduler_types()._FinalPageUncertainty.INTEGRITY_FAILURE},
            )
            self.assertIsInstance(
                await lane.close(
                    feed_work_scheduler.LaneCloseReason.AUTHORITY_LOSS
                ),
                feed_work_scheduler.Undrained,
            )
        finally:
            finalizer.release.set()
            if not coverage.done():
                coverage.cancel()
                await asyncio.gather(coverage, return_exceptions=True)
            await scheduler.close()

    async def test_final_closure_pending_cross_shard_validation_is_atomic(
        self,
    ) -> None:
        observed = {0: [], 1: []}

        def resolve_all(
            context: feed_work_scheduler.PageFinalizationContext,
        ) -> object:
            identities = tuple(
                record.identity
                for facts in context.cohort_terminal_facts
                for record in facts.records
            )
            return _covered_final_page(
                context,
                resolutions=tuple(
                    feed_work_scheduler.FinalRecordClosureResolution(
                        identity=identity,
                        closure_state=(
                            feed_work_scheduler.CohortRecordClosureState.DURABLY_CLOSED
                        ),
                        release_basis=(
                            feed_work_scheduler.FinalRecordReleaseBasis.DURABLE_SOURCE_CLOSURE
                        ),
                    )
                    for identity in identities
                ),
            )

        finalizer = _ControlledPageFinalizer(resolve_all)
        scheduler = feed_work_scheduler.FeedWorkScheduler(
            _PerOrderOutcomeExecutor(
                {0: _final_closure_pending, 1: _final_closure_pending}
            ),
            page_finalizer=finalizer,
        )
        await scheduler.start()
        grant = _grant()
        feeds = (uuid.UUID(int=107), uuid.UUID(int=108))
        self.assertIsNot(
            scheduler._shard_for(feeds[0]),
            scheduler._shard_for(feeds[1]),
        )
        lane = _open_lane(scheduler, grant)
        coverage = asyncio.create_task(
            lane.cover_page(
                calls=tuple(
                    _cohort(
                        feed_id,
                        (source_order,),
                        grant=grant,
                        cohort_timestamp=None,
                        settlement_observers={
                            source_order: observed[source_order].append
                        },
                    )
                    for source_order, feed_id in enumerate(feeds)
                ),
                boundaries=(),
                candidate=cursor_policy.LeaseCursor(
                    grant,
                    pos=None,
                ).prepare(_SOURCE_TIME),
            )
        )
        try:
            await asyncio.wait_for(finalizer.entered.wait(), timeout=1)
            rejecting_shard = scheduler._shard_for(feeds[1])

            async def reject_validation(
                resolutions: object,
            ) -> None:
                del resolutions
                message = "injected cross-shard ownership mismatch"
                raise feed_work_scheduler.CohortIntegrityError(message)

            validation_method = "validate_final_pending"
            setattr(rejecting_shard, validation_method, reject_validation)
            finalizer.release.set()
            self.assertIsInstance(
                await asyncio.wait_for(coverage, timeout=1),
                feed_work_scheduler.Undrained,
            )
            self.assertEqual(observed, {0: [], 1: []})
            self.assertEqual((await scheduler._snapshot()).held, 2)
            self.assertIsInstance(
                await lane.close(
                    feed_work_scheduler.LaneCloseReason.AUTHORITY_LOSS
                ),
                feed_work_scheduler.Undrained,
            )
        finally:
            finalizer.release.set()
            if not coverage.done():
                coverage.cancel()
                await asyncio.gather(coverage, return_exceptions=True)
            await scheduler.close()

    async def test_member_removal_resolves_only_exact_pending_member(
        self,
    ) -> None:
        retired_feed = uuid.UUID(int=92)
        sibling_feed = uuid.UUID(int=93)
        grant = _grant()
        retired_member = _member(grant, retired_feed)
        observed = {0: [], 1: [], 2: []}

        def accept_retirement(
            context: feed_work_scheduler.PageFinalizationContext,
        ) -> object:
            pending = next(
                record.identity
                for facts in context.cohort_terminal_facts
                for record in facts.records
                if record.closure_state
                is feed_work_scheduler.CohortRecordClosureState.FINAL_CLOSURE_PENDING
            )
            self.assertIs(context.locally_retired_members[0], retired_member)
            return _covered_final_page(
                context,
                resolutions=(
                    feed_work_scheduler.FinalRecordClosureResolution(
                        identity=pending,
                        closure_state=(
                            feed_work_scheduler.CohortRecordClosureState.REPLAY_SAFE_RELEASE
                        ),
                        release_basis=(
                            feed_work_scheduler.FinalRecordReleaseBasis.ACCEPTED_MEMBER_RETIREMENT
                        ),
                    ),
                ),
            )

        finalizer = _ControlledPageFinalizer(accept_retirement)
        scheduler = feed_work_scheduler.FeedWorkScheduler(
            _PerOrderOutcomeExecutor(
                {
                    0: _final_closure_pending,
                    1: _membership_rejected,
                    2: _completed,
                }
            ),
            page_finalizer=finalizer,
        )
        await scheduler.start()
        lane = _open_lane(scheduler, grant)
        cursor = cursor_policy.LeaseCursor(grant, pos=None)
        coverage = asyncio.create_task(
            lane.cover_page(
                calls=(
                    _cohort(
                        retired_feed,
                        (0,),
                        grant=grant,
                        cohort_timestamp=None,
                        member=retired_member,
                        payload_member=retired_member,
                        settlement_observers={0: observed[0].append},
                    ),
                    _cohort(
                        retired_feed,
                        (1,),
                        grant=grant,
                        cohort_timestamp=_SOURCE_TIME,
                        member=retired_member,
                        payload_member=retired_member,
                        settlement_observers={1: observed[1].append},
                    ),
                    _cohort(
                        sibling_feed,
                        (2,),
                        grant=grant,
                        cohort_timestamp=_SOURCE_TIME,
                        settlement_observers={2: observed[2].append},
                    ),
                ),
                boundaries=(),
                candidate=cursor.prepare(_SOURCE_TIME),
            )
        )
        try:
            await asyncio.wait_for(finalizer.entered.wait(), timeout=1)
            self.assertEqual(observed[0], [])
            finalizer.release.set()
            settled = await asyncio.wait_for(coverage, timeout=1)
            self.assertEqual(
                cursor.accept(_covered_settlement(settled)),
                _SOURCE_TIME,
            )
            self.assertEqual(
                observed,
                {
                    0: [feed_work_scheduler.CallSettlement.REPLAY_SAFE_RELEASE],
                    1: [feed_work_scheduler.CallSettlement.REPLAY_SAFE_RELEASE],
                    2: [feed_work_scheduler.CallSettlement.COMPLETED],
                },
            )
        finally:
            finalizer.release.set()
            if not coverage.done():
                coverage.cancel()
                await asyncio.gather(coverage, return_exceptions=True)
            await scheduler.close()

    async def test_member_removal_surfaces_final_pending_only_retirement(
        self,
    ) -> None:
        pending_feed = uuid.UUID(int=105)
        sibling_feed = uuid.UUID(int=106)
        grant = _grant()
        pending_member = _member(grant, pending_feed)
        observed = {0: [], 1: []}

        def accept_retirement(
            context: feed_work_scheduler.PageFinalizationContext,
        ) -> object:
            self.assertEqual(context.locally_retired_members, (pending_member,))
            identity = context.cohort_terminal_facts[0].records[0].identity
            return _covered_final_page(
                context,
                resolutions=(
                    feed_work_scheduler.FinalRecordClosureResolution(
                        identity=identity,
                        closure_state=(
                            feed_work_scheduler.CohortRecordClosureState.REPLAY_SAFE_RELEASE
                        ),
                        release_basis=(
                            feed_work_scheduler.FinalRecordReleaseBasis.ACCEPTED_MEMBER_RETIREMENT
                        ),
                    ),
                ),
            )

        executor = _FinalPendingWithGatedSiblingExecutor()
        finalizer = _ControlledPageFinalizer(accept_retirement)
        scheduler = feed_work_scheduler.FeedWorkScheduler(
            executor,
            page_finalizer=finalizer,
        )
        await scheduler.start()
        lane = _open_lane(scheduler, grant)
        cursor = cursor_policy.LeaseCursor(grant, pos=None)
        coverage = asyncio.create_task(
            lane.cover_page(
                calls=(
                    _cohort(
                        pending_feed,
                        (0,),
                        grant=grant,
                        cohort_timestamp=None,
                        member=pending_member,
                        payload_member=pending_member,
                        settlement_observers={0: observed[0].append},
                    ),
                    _cohort(
                        sibling_feed,
                        (1,),
                        grant=grant,
                        cohort_timestamp=_SOURCE_TIME,
                        settlement_observers={1: observed[1].append},
                    ),
                ),
                boundaries=(),
                candidate=cursor.prepare(_SOURCE_TIME),
            )
        )
        try:
            await asyncio.wait_for(executor.pending_returned.wait(), timeout=1)
            await asyncio.wait_for(executor.sibling_entered.wait(), timeout=1)
            pending_shard = scheduler._shard_for(pending_feed)

            async def wait_for_final_pending() -> None:
                while not pending_shard._final_pending:  # noqa: ASYNC110
                    await asyncio.sleep(0)

            await asyncio.wait_for(wait_for_final_pending(), timeout=1)
            removal = await lane.remove_feed(pending_feed)
            self.assertEqual(removal.released_count, 0)
            self.assertTrue(removal.active_retained)
            self.assertEqual(observed[0], [])

            executor.release_sibling.set()
            await asyncio.wait_for(finalizer.entered.wait(), timeout=1)
            finalizer.release.set()
            settled = await asyncio.wait_for(coverage, timeout=1)
            self.assertEqual(
                cursor.accept(_covered_settlement(settled)),
                _SOURCE_TIME,
            )
            self.assertEqual(
                observed,
                {
                    0: [feed_work_scheduler.CallSettlement.REPLAY_SAFE_RELEASE],
                    1: [feed_work_scheduler.CallSettlement.COMPLETED],
                },
            )
        finally:
            executor.release_sibling.set()
            finalizer.release.set()
            if not coverage.done():
                coverage.cancel()
                await asyncio.gather(coverage, return_exceptions=True)
            await scheduler.close()

    async def test_stopped_aborts_without_page_finalizer_or_receipt(
        self,
    ) -> None:
        finalizer = _ControlledPageFinalizer()
        executor = _GatedOutcomeExecutor(_stopped)
        scheduler = feed_work_scheduler.FeedWorkScheduler(
            executor,
            page_finalizer=finalizer,
        )
        await scheduler.start()
        grant = _grant()
        lane = _open_lane(scheduler, grant)
        cursor = cursor_policy.LeaseCursor(grant, pos=None)
        candidate = cursor.prepare(_SOURCE_TIME)
        coverage = asyncio.create_task(
            lane.cover_page(
                calls=(_submission(uuid.UUID(int=94), 0, grant=grant),),
                boundaries=(),
                candidate=candidate,
            )
        )
        try:
            await asyncio.wait_for(executor.entered.wait(), timeout=1)
            executor.release.set()
            with self.assertRaisesRegex(RuntimeError, "stopped"):
                await asyncio.wait_for(coverage, timeout=1)
            self.assertFalse(finalizer.entered.is_set())
            self.assertIs(cursor.outstanding_candidate, candidate)
            self.assertEqual((await scheduler._snapshot()).held, 0)
        finally:
            executor.release.set()
            finalizer.release.set()
            if not coverage.done():
                coverage.cancel()
                await asyncio.gather(coverage, return_exceptions=True)
            await scheduler.close()

    async def test_stopped_waits_for_later_outcome_unknown_before_cleanup(
        self,
    ) -> None:
        executor = _AbortBeforeUnknownExecutor()
        finalizer = _ControlledPageFinalizer()
        observed = {0: [], 1: [], 2: []}
        scheduler = feed_work_scheduler.FeedWorkScheduler(
            executor,
            page_finalizer=finalizer,
        )
        await scheduler.start()
        grant = _grant()
        lane = _open_lane(scheduler, grant)
        coverage = asyncio.create_task(
            lane.cover_page(
                calls=tuple(
                    _cohort(
                        uuid.UUID(int=100 + source_order),
                        (source_order,),
                        grant=grant,
                        cohort_timestamp=(
                            None if source_order == 0 else _SOURCE_TIME
                        ),
                        settlement_observers={
                            source_order: observed[source_order].append
                        },
                    )
                    for source_order in range(3)
                ),
                boundaries=(),
                candidate=cursor_policy.LeaseCursor(
                    grant,
                    pos=None,
                ).prepare(_SOURCE_TIME),
            )
        )
        try:
            await asyncio.wait_for(executor.stopped_returned.wait(), timeout=1)
            await asyncio.wait_for(executor.unknown_entered.wait(), timeout=1)
            await asyncio.sleep(0)
            self.assertFalse(coverage.done())
            self.assertEqual(observed[0], [])
            self.assertEqual((await scheduler._snapshot()).held, 2)

            executor.release_unknown.set()
            self.assertIsInstance(
                await asyncio.wait_for(coverage, timeout=1),
                feed_work_scheduler.Undrained,
            )
            self.assertFalse(finalizer.entered.is_set())
            self.assertEqual(observed[0], [])
            self.assertEqual(observed[2], [])
            self.assertEqual((await scheduler._snapshot()).held, 2)
            self.assertIsInstance(
                await lane.close(
                    feed_work_scheduler.LaneCloseReason.AUTHORITY_LOSS
                ),
                feed_work_scheduler.Undrained,
            )
        finally:
            executor.release_unknown.set()
            finalizer.release.set()
            if not coverage.done():
                coverage.cancel()
                await asyncio.gather(coverage, return_exceptions=True)
            await scheduler.close()

    async def test_page_final_cancellation_is_shielded_to_typed_result(
        self,
    ) -> None:
        finalizer = _ControlledPageFinalizer()
        scheduler = feed_work_scheduler.FeedWorkScheduler(
            _ImmediateExecutor(),
            page_finalizer=finalizer,
        )
        await scheduler.start()
        grant = _grant()
        lane = _open_lane(scheduler, grant)
        cursor = cursor_policy.LeaseCursor(grant, pos=None)
        coverage = asyncio.create_task(
            lane.cover_page(
                calls=(_submission(uuid.UUID(int=95), 0, grant=grant),),
                boundaries=(),
                candidate=cursor.prepare(_SOURCE_TIME),
            )
        )
        try:
            await asyncio.wait_for(finalizer.entered.wait(), timeout=1)
            coverage.cancel()
            coverage.cancel()
            finalizer.release.set()
            settled = await asyncio.wait_for(coverage, timeout=1)
            self.assertEqual(
                cursor.accept(_covered_settlement(settled)),
                _SOURCE_TIME,
            )
            self.assertEqual(len(finalizer.contexts), 1)
        finally:
            finalizer.release.set()
            if not coverage.done():
                coverage.cancel()
                await asyncio.gather(coverage, return_exceptions=True)
            await scheduler.close()

    async def test_no_progress_replay_releases_final_closure_pending(
        self,
    ) -> None:
        feed_id = uuid.UUID(int=96)
        observed = {0: [], 1: []}

        def no_progress(
            context: feed_work_scheduler.PageFinalizationContext,
        ) -> object:
            identity = context.cohort_terminal_facts[0].records[0].identity
            resolution = feed_work_scheduler.FinalRecordClosureResolution(
                identity=identity,
                closure_state=(
                    feed_work_scheduler.CohortRecordClosureState.REPLAY_SAFE_RELEASE
                ),
                release_basis=(
                    feed_work_scheduler.FinalRecordReleaseBasis.ACCEPTED_NO_PROGRESS
                ),
            )
            return feed_work_scheduler.FinalPageNoProgress(
                grant=context.grant,
                page_sequence=context.page_sequence,
                candidate=context.candidate,
                boundary_results=_accepted_boundary_results(context),
                final_closure_resolutions=(resolution,),
                source_evidence=None,
            )

        finalizer = _ControlledPageFinalizer(no_progress)
        scheduler = feed_work_scheduler.FeedWorkScheduler(
            _PerOrderOutcomeExecutor(
                {0: _final_closure_pending, 1: _replayable_direct}
            ),
            page_finalizer=finalizer,
        )
        await scheduler.start()
        grant = _grant()
        lane = _open_lane(scheduler, grant)
        cursor = cursor_policy.LeaseCursor(grant, pos=_SOURCE_TIME)
        coverage = asyncio.create_task(
            lane.cover_page(
                calls=(
                    _cohort(
                        feed_id,
                        (0,),
                        grant=grant,
                        cohort_timestamp=None,
                        settlement_observers={0: observed[0].append},
                    ),
                    _cohort(
                        feed_id,
                        (1,),
                        grant=grant,
                        cohort_timestamp=_SOURCE_TIME,
                        settlement_observers={1: observed[1].append},
                    ),
                ),
                boundaries=(),
                candidate=cursor.prepare_no_progress(),
            )
        )
        try:
            await asyncio.wait_for(finalizer.entered.wait(), timeout=1)
            self.assertEqual(observed[0], [])
            self.assertEqual(
                observed[1],
                [feed_work_scheduler.CallSettlement.REPLAY_SAFE_RELEASE],
            )
            finalizer.release.set()
            settled = await asyncio.wait_for(coverage, timeout=1)
            cursor.accept_no_progress(_no_progress_settlement(settled))
            self.assertEqual(
                observed,
                {
                    0: [feed_work_scheduler.CallSettlement.REPLAY_SAFE_RELEASE],
                    1: [feed_work_scheduler.CallSettlement.REPLAY_SAFE_RELEASE],
                },
            )
            self.assertEqual(cursor.pos, _SOURCE_TIME)
            self.assertFalse(
                await scheduler._shard_for(feed_id).is_replay_blocked(
                    grant,
                    0,
                    feed_id,
                )
            )
        finally:
            finalizer.release.set()
            if not coverage.done():
                coverage.cancel()
                await asyncio.gather(coverage, return_exceptions=True)
            await scheduler.close()

    async def test_no_commit_retryable_retries_only_final_generation(
        self,
    ) -> None:
        replay_feed = uuid.UUID(int=97)
        observed: list[object] = []
        finalizer = _NoCommitThenReplayableFinalizer()
        executor = _ReplayBarrierExecutor(replay_feed)
        scheduler = feed_work_scheduler.FeedWorkScheduler(
            executor,
            page_finalizer=finalizer,
        )
        await scheduler.start()
        grant = _grant()
        lane = _open_lane(scheduler, grant)
        cursor = cursor_policy.LeaseCursor(grant, pos=None)
        candidate = cursor.prepare(_SOURCE_TIME)
        coverage = asyncio.create_task(
            lane.cover_page(
                calls=(
                    _cohort(
                        replay_feed,
                        (0,),
                        grant=grant,
                        cohort_timestamp=_SOURCE_TIME,
                        settlement_observers={0: observed.append},
                    ),
                ),
                boundaries=(),
                candidate=candidate,
            )
        )
        await asyncio.wait_for(executor.failure_entered.wait(), timeout=1)
        executor.release_failure.set()
        await asyncio.wait_for(finalizer.retry_entered.wait(), timeout=1)
        self.assertFalse(coverage.done())
        self.assertIs(cursor.outstanding_candidate, candidate)
        self.assertEqual(executor.started, [(0,)])
        self.assertEqual(len(finalizer.contexts), 2)
        self.assertIs(finalizer.contexts[0], finalizer.contexts[1])
        self.assertTrue(
            await scheduler._shard_for(replay_feed).is_replay_blocked(
                grant,
                candidate.page_sequence,
                replay_feed,
            )
        )

        finalizer.release_retry.set()
        settled = await asyncio.wait_for(coverage, timeout=1)
        cursor.accept_replayable(_replayable_settlement(settled))
        self.assertEqual(
            observed,
            [feed_work_scheduler.CallSettlement.REPLAY_SAFE_RELEASE],
        )
        self.assertFalse(
            await scheduler._shard_for(replay_feed).is_replay_blocked(
                grant,
                candidate.page_sequence,
                replay_feed,
            )
        )
        self.assertEqual((await scheduler._snapshot()).held, 0)
        await scheduler.close()

    async def test_covered_clears_only_preexisting_page_replay_barrier(
        self,
    ) -> None:
        replay_feed = uuid.UUID(int=103)
        sibling_feed = uuid.UUID(int=104)
        observed: list[object] = []

        def cover(
            context: feed_work_scheduler.PageFinalizationContext,
        ) -> object:
            self.assertEqual(context.unresolved_replay_feed_ids, ())
            self.assertEqual(
                context.replay_blocked_feed_ids,
                (replay_feed,),
            )
            return _covered_final_page(context)

        executor = _CapturingExecutor()
        finalizer = _ControlledPageFinalizer(cover)
        finalizer.release.set()
        scheduler = feed_work_scheduler.FeedWorkScheduler(
            executor,
            page_finalizer=finalizer,
        )
        await scheduler.start()
        grant = _grant()
        lane = _open_lane(scheduler, grant)
        shard = scheduler._shard_for(replay_feed)
        async with shard._lock:
            shard._replay_blocks.update(
                {
                    (grant, 0, replay_feed),
                    (grant, 1, replay_feed),
                }
            )
        cursor = cursor_policy.LeaseCursor(grant, pos=None)
        settled = await lane.cover_page(
            calls=(
                _cohort(
                    replay_feed,
                    (0,),
                    grant=grant,
                    cohort_timestamp=_SOURCE_TIME,
                    settlement_observers={0: observed.append},
                ),
                _cohort(
                    sibling_feed,
                    (1,),
                    grant=grant,
                    cohort_timestamp=_SOURCE_TIME,
                ),
            ),
            boundaries=(),
            candidate=cursor.prepare(_SOURCE_TIME),
        )
        self.assertEqual(
            cursor.accept(_covered_settlement(settled)),
            _SOURCE_TIME,
        )
        self.assertEqual(
            observed,
            [feed_work_scheduler.CallSettlement.REPLAY_BLOCKED],
        )
        self.assertEqual(len(executor.executions), 1)
        execution = executor.executions[0]
        self.assertEqual(execution.calls[0].feed_id, sibling_feed)
        self.assertFalse(await shard.is_replay_blocked(grant, 0, replay_feed))
        self.assertTrue(await shard.is_replay_blocked(grant, 1, replay_feed))
        await scheduler.close()

    async def test_page_final_outcome_unknown_retains_pending_undrained(
        self,
    ) -> None:
        feed_id = uuid.UUID(int=98)
        observed = {0: [], 1: []}

        def unknown(
            context: feed_work_scheduler.PageFinalizationContext,
        ) -> object:
            return feed_work_scheduler.FinalPageOutcomeUnknown(
                context.grant,
                context.page_sequence,
                context.candidate,
            )

        finalizer = _ControlledPageFinalizer(unknown)
        executor = _PerOrderOutcomeExecutor(
            {0: _final_closure_pending, 1: _replayable_direct}
        )
        scheduler = feed_work_scheduler.FeedWorkScheduler(
            executor,
            page_finalizer=finalizer,
        )
        await scheduler.start()
        grant = _grant()
        lane = _open_lane(scheduler, grant)
        evidence_observed: list[feed_work_scheduler.SchedulerPageEvidence] = []
        coverage = asyncio.create_task(
            lane.cover_page(
                calls=(
                    _cohort(
                        feed_id,
                        (0,),
                        grant=grant,
                        cohort_timestamp=None,
                        settlement_observers={0: observed[0].append},
                    ),
                    _cohort(
                        feed_id,
                        (1,),
                        grant=grant,
                        cohort_timestamp=_SOURCE_TIME,
                        settlement_observers={1: observed[1].append},
                    ),
                ),
                boundaries=(),
                candidate=cursor_policy.LeaseCursor(
                    grant,
                    pos=None,
                ).prepare(_SOURCE_TIME),
                evidence_observer=evidence_observed.append,
            )
        )
        try:
            await asyncio.wait_for(finalizer.entered.wait(), timeout=1)
            finalizer.release.set()
            self.assertIsInstance(
                await asyncio.wait_for(coverage, timeout=1),
                feed_work_scheduler.Undrained,
            )
            self.assertEqual(
                observed,
                {
                    0: [],
                    1: [feed_work_scheduler.CallSettlement.REPLAY_SAFE_RELEASE],
                },
            )
            self.assertEqual((await scheduler._snapshot()).held, 1)
            self.assertEqual(len(evidence_observed), 1)
            self.assertEqual(
                evidence_observed[0].admitted_record_count,
                2,
            )
            self.assertEqual(evidence_observed[0].terminal_record_count, 2)
            self.assertEqual(
                {
                    uncertainty
                    for shard in scheduler._shards
                    for uncertainty in shard._uncertain_final_pages.values()
                },
                {_scheduler_types()._FinalPageUncertainty.OUTCOME_UNKNOWN},
            )
            self.assertTrue(
                await scheduler._shard_for(feed_id).is_replay_blocked(
                    grant,
                    0,
                    feed_id,
                )
            )
            self.assertEqual(len(evidence_observed), 1)
            self.assertIsInstance(
                await lane.close(
                    feed_work_scheduler.LaneCloseReason.AUTHORITY_LOSS
                ),
                feed_work_scheduler.Undrained,
            )
            self.assertTrue(
                await scheduler._shard_for(feed_id).is_replay_blocked(
                    grant,
                    0,
                    feed_id,
                )
            )
        finally:
            finalizer.release.set()
            if not coverage.done():
                coverage.cancel()
                await asyncio.gather(coverage, return_exceptions=True)
            await scheduler.close()

    async def test_outcome_unknown_never_invokes_page_finalizer(
        self,
    ) -> None:
        finalizer = _ControlledPageFinalizer()
        executor = _GatedOutcomeExecutor(_outcome_unknown)
        scheduler = feed_work_scheduler.FeedWorkScheduler(
            executor,
            page_finalizer=finalizer,
        )
        await scheduler.start()
        grant = _grant()
        lane = _open_lane(scheduler, grant)
        coverage = asyncio.create_task(
            lane.cover_page(
                calls=(_submission(uuid.UUID(int=86), 0, grant=grant),),
                boundaries=(),
                candidate=cursor_policy.LeaseCursor(
                    grant,
                    pos=None,
                ).prepare(_SOURCE_TIME),
            )
        )
        try:
            await asyncio.wait_for(executor.entered.wait(), timeout=1)
            executor.release.set()
            result = await asyncio.wait_for(coverage, timeout=1)
            self.assertIsInstance(result, feed_work_scheduler.Undrained)
            self.assertFalse(finalizer.entered.is_set())
            self.assertEqual((await scheduler._snapshot()).held, 1)
            self.assertIsInstance(
                await lane.close(
                    feed_work_scheduler.LaneCloseReason.AUTHORITY_LOSS
                ),
                feed_work_scheduler.Undrained,
            )
        finally:
            executor.release.set()
            finalizer.release.set()
            if not coverage.done():
                coverage.cancel()
                await asyncio.gather(coverage, return_exceptions=True)
            await scheduler.close()

    async def test_member_rejected_boundary_accepts_exact_retirement_release(
        self,
    ) -> None:
        feed_id = uuid.UUID(int=151)
        grant = _grant()
        member = _member(grant, feed_id)
        observed = []

        finalizer = _ControlledPageFinalizer(_member_rejected_retirement_page)
        finalizer.release.set()
        scheduler = feed_work_scheduler.FeedWorkScheduler(
            _GatedOutcomeExecutor(_final_closure_pending),
            page_finalizer=finalizer,
        )
        await scheduler.start()
        lane = _open_lane(scheduler, grant)
        cursor = cursor_policy.LeaseCursor(grant, pos=None)
        coverage = asyncio.create_task(
            lane.cover_page(
                calls=(
                    _cohort(
                        feed_id,
                        (0,),
                        grant=grant,
                        cohort_timestamp=None,
                        member=member,
                        payload_member=member,
                        settlement_observers={0: observed.append},
                    ),
                ),
                boundaries=(
                    feed_work_scheduler.BoundaryWork(member, _SOURCE_TIME),
                ),
                candidate=cursor.prepare(_SOURCE_TIME),
            )
        )
        executor = typing.cast(
            "_GatedOutcomeExecutor",
            scheduler._shards[0]._executor,
        )
        try:
            await asyncio.wait_for(executor.entered.wait(), timeout=1)
            executor.release.set()
            settled = await asyncio.wait_for(coverage, timeout=1)
            self.assertIsInstance(settled, feed_work_scheduler.SettledPage)
            assert isinstance(settled, feed_work_scheduler.SettledPage)
            self.assertEqual(
                cursor.accept(_covered_settlement(settled)),
                _SOURCE_TIME,
            )
            self.assertEqual(
                observed,
                [feed_work_scheduler.CallSettlement.REPLAY_SAFE_RELEASE],
            )
            self.assertTrue(await lane.is_feed_retired(feed_id))
            self.assertEqual((await scheduler._snapshot()).held, 0)
        finally:
            executor.release.set()
            if not coverage.done():
                coverage.cancel()
                await asyncio.gather(coverage, return_exceptions=True)
            await scheduler.close()

    async def test_member_rejected_boundary_rejects_durable_resolution(
        self,
    ) -> None:
        feed_id = uuid.UUID(int=152)
        grant = _grant()
        member = _member(grant, feed_id)
        observed = []

        finalizer = _ControlledPageFinalizer(_member_rejected_durable_page)
        finalizer.release.set()
        scheduler = feed_work_scheduler.FeedWorkScheduler(
            _GatedOutcomeExecutor(_final_closure_pending),
            page_finalizer=finalizer,
        )
        await scheduler.start()
        lane = _open_lane(scheduler, grant)
        coverage = asyncio.create_task(
            lane.cover_page(
                calls=(
                    _cohort(
                        feed_id,
                        (0,),
                        grant=grant,
                        cohort_timestamp=None,
                        member=member,
                        payload_member=member,
                        settlement_observers={0: observed.append},
                    ),
                ),
                boundaries=(
                    feed_work_scheduler.BoundaryWork(member, _SOURCE_TIME),
                ),
                candidate=cursor_policy.LeaseCursor(
                    grant,
                    pos=None,
                ).prepare(_SOURCE_TIME),
            )
        )
        executor = typing.cast(
            "_GatedOutcomeExecutor",
            scheduler._shards[0]._executor,
        )
        try:
            await asyncio.wait_for(executor.entered.wait(), timeout=1)
            executor.release.set()
            self.assertIsInstance(
                await asyncio.wait_for(coverage, timeout=1),
                feed_work_scheduler.Undrained,
            )
            self.assertEqual(observed, [])
            self.assertFalse(await lane.is_feed_retired(feed_id))
            self.assertEqual((await scheduler._snapshot()).held, 1)
        finally:
            executor.release.set()
            if not coverage.done():
                coverage.cancel()
                await asyncio.gather(coverage, return_exceptions=True)
            await scheduler.close()

    def test_member_retirement_value_rejects_invalid_authority_and_order(
        self,
    ) -> None:
        grant = _grant()
        first = _member(grant, uuid.UUID(int=161))
        second = _member(grant, uuid.UUID(int=162))
        duplicate_feed = _member(grant, first.feed_id)
        crossed = _member(_grant(lease_key="999"), uuid.UUID(int=163))
        forged = ingestion_lease_store.LeaseMemberIdentity(
            feed_id=uuid.UUID(int=167),
            source_type=feed_store.SourceType.BCFY_CALLS,
            source_feed_id="150-167",
            sid="150",
            group_id="167",
        )
        candidate = cursor_policy.LeaseCursor(grant, pos=None).prepare(
            _SOURCE_TIME
        )
        cases = (
            ("duplicate_member", (first, first)),
            ("duplicate_feed", (first, duplicate_feed)),
            ("nondeterministic_order", (second, first)),
            ("cross_grant", (crossed,)),
            ("forged_member", (forged,)),
        )
        for case_id, retirements in cases:
            with self.subTest(case_id=case_id):
                with self.assertRaises(
                    feed_work_scheduler.CohortIntegrityError
                ):
                    feed_work_scheduler.FinalPageCovered(
                        grant=grant,
                        page_sequence=candidate.page_sequence,
                        candidate=candidate,
                        boundary_results=(),
                        final_closure_resolutions=(),
                        source_evidence=None,
                        member_retirements=retirements,
                    )

    async def test_member_rejected_boundary_omission_retains_exact_page(
        self,
    ) -> None:
        feed_id = uuid.UUID(int=164)
        grant = _grant()
        member = _member(grant, feed_id)
        finalizer = _ControlledPageFinalizer(
            _member_rejected_without_retirement_page
        )
        finalizer.release.set()
        scheduler = feed_work_scheduler.FeedWorkScheduler(
            _GatedOutcomeExecutor(_final_closure_pending),
            page_finalizer=finalizer,
        )
        await scheduler.start()
        lane = _open_lane(scheduler, grant)
        coverage = asyncio.create_task(
            lane.cover_page(
                calls=(
                    _cohort(
                        feed_id,
                        (0,),
                        grant=grant,
                        cohort_timestamp=None,
                        member=member,
                        payload_member=member,
                    ),
                ),
                boundaries=(
                    feed_work_scheduler.BoundaryWork(member, _SOURCE_TIME),
                ),
                candidate=cursor_policy.LeaseCursor(
                    grant,
                    pos=None,
                ).prepare(_SOURCE_TIME),
            )
        )
        executor = typing.cast(
            "_GatedOutcomeExecutor",
            scheduler._shards[0]._executor,
        )
        try:
            await asyncio.wait_for(executor.entered.wait(), timeout=1)
            executor.release.set()
            self.assertIsInstance(
                await asyncio.wait_for(coverage, timeout=1),
                feed_work_scheduler.Undrained,
            )
            self.assertFalse(await lane.is_feed_retired(feed_id))
            self.assertEqual((await scheduler._snapshot()).held, 1)
        finally:
            executor.release.set()
            if not coverage.done():
                coverage.cancel()
                await asyncio.gather(coverage, return_exceptions=True)
            await scheduler.close()

    async def test_retirement_rejects_same_grant_noncurrent_page_member(
        self,
    ) -> None:
        grant = _grant()
        current_feed = uuid.UUID(int=165)
        current_member = _member(grant, current_feed)
        foreign_member = _member(grant, uuid.UUID(int=166))

        def retire_foreign_member(
            context: feed_work_scheduler.PageFinalizationContext,
        ) -> feed_work_scheduler.FinalPageCovered:
            return _covered_final_page(
                context,
                member_retirements=(foreign_member,),
            )

        finalizer = _ControlledPageFinalizer(retire_foreign_member)
        finalizer.release.set()
        scheduler = feed_work_scheduler.FeedWorkScheduler(
            _ImmediateExecutor(),
            page_finalizer=finalizer,
        )
        await scheduler.start()
        lane = _open_lane(scheduler, grant)
        try:
            result = await lane.cover_page(
                calls=(
                    _submission(
                        current_feed,
                        0,
                        grant=grant,
                        member=current_member,
                    ),
                ),
                boundaries=(),
                candidate=cursor_policy.LeaseCursor(
                    grant,
                    pos=None,
                ).prepare(_SOURCE_TIME),
            )

            self.assertIsInstance(result, feed_work_scheduler.Undrained)
            self.assertFalse(await lane.is_feed_retired(current_feed))
            self.assertFalse(await lane.is_feed_retired(foreign_member.feed_id))
        finally:
            await scheduler.close()


if __name__ == "__main__":
    unittest.main()
