"""Deterministic contracts for one exact-grant Calls SID processor."""

from __future__ import annotations

# The processor deliberately keeps its page algorithms private and deep.
import asyncio
import collections
import dataclasses
import datetime
import inspect
import typing
import unittest
import uuid
from unittest import mock

from backend.pipeline.ingestion import (
    failure_policy,
    feed_work_scheduler,
    grant_control,
    models,
    sid_grant_control,
)
from backend.pipeline.ingestion.collectors import aiohttp_requests
from backend.pipeline.ingestion.collectors.bcfy_calls import (
    boundary_verdict,
    cursor_policy,
    provider,
    runtime_adapters,
    sid_processor,
    telemetry,
)
from backend.pipeline.ingestion.collectors.bcfy_calls import (
    pipeline as cohort_pipeline,
)
from backend.pipeline.storage import feed_store, ingestion_lease_store

_OWNER_ID = uuid.UUID("11111111-2222-3333-4444-555555555555")
_FEED_A = uuid.UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
_FEED_B = uuid.UUID("bbbbbbbb-cccc-dddd-eeee-ffffffffffff")
_FEED_C = uuid.UUID("cccccccc-dddd-eeee-ffff-000000000000")
_FEED_D = uuid.UUID("dddddddd-eeee-ffff-0000-111111111111")
_FEED_E = uuid.UUID("eeeeeeee-ffff-0000-1111-222222222222")
_NOW = datetime.datetime(2026, 7, 13, 1, 0, tzinfo=datetime.UTC)
_GRANT = ingestion_lease_store.LeaseGrant(
    feed_store.SourceType.BCFY_CALLS,
    "00123",
    _OWNER_ID,
    7,
)


def _claim_payload(
    mode: grant_control.ClaimMode = grant_control.ClaimMode.PRIMARY,
) -> sid_grant_control.SidClaimPayload:
    return sid_grant_control.SidClaimPayload(
        mode,
    )


def _batch_committed(
    *children: ingestion_lease_store.ChildMutationResult,
) -> ingestion_lease_store.BatchCommitted:
    return ingestion_lease_store.BatchCommitted(
        lease_effect=ingestion_lease_store.LeaseLifecycleResult(
            effect=ingestion_lease_store.LeaseLifecycleEffect.NONE,
        ),
        children=children,
    )


def _member(
    feed_id: uuid.UUID,
    source_feed_id: str,
    *,
    cursor: datetime.datetime | None = _NOW,
) -> ingestion_lease_store.LeaseMember:
    sid, group_id = source_feed_id.split("-", maxsplit=1)
    identity = ingestion_lease_store._issue_member_identity(
        _GRANT,
        feed_id=feed_id,
        source_type=feed_store.SourceType.BCFY_CALLS,
        source_feed_id=source_feed_id,
        sid=sid,
        group_id=group_id,
    )
    return ingestion_lease_store.LeaseMember(
        identity=identity,
        name=f"Feed {source_feed_id}",
        last_bookmark_time=cursor,
    )


def _snapshot(
    revision: int,
    *members: ingestion_lease_store.LeaseMember,
) -> ingestion_lease_store.MembershipSnapshot:
    return ingestion_lease_store.MembershipSnapshot(
        grant=_GRANT,
        membership_revision=revision,
        members=members,
        excluded_count=0,
    )


class _ScriptedMembershipStore:
    """Return immutable scripted refresh outcomes and record revisions."""

    def __init__(
        self,
        *results: object,
        trace: list[str] | None = None,
    ) -> None:
        self.results = collections.deque(results)
        self.trace = trace
        self.calls: list[
            tuple[ingestion_lease_store.LeaseGrant, int | None]
        ] = []

    async def refresh_membership(
        self,
        grant: ingestion_lease_store.LeaseGrant,
        *,
        known_revision: int | None,
    ) -> ingestion_lease_store.MembershipRefreshResult:
        if self.trace is not None:
            self.trace.append("refresh")
        self.calls.append((grant, known_revision))
        result = self.results.popleft()
        if isinstance(result, _AsyncGate):
            result = await result.wait()
        if isinstance(result, BaseException):
            raise result
        if not isinstance(
            result,
            (
                ingestion_lease_store.MembershipSnapshot,
                ingestion_lease_store.MembershipUnchanged,
                ingestion_lease_store.GrantRejected,
                ingestion_lease_store.MembershipInvariantViolation,
            ),
        ):
            message = "scripted membership result has an invalid type"
            raise TypeError(message)
        return result


class _ScriptedProvider:
    """Return complete provider envelopes without network or credentials."""

    def __init__(
        self,
        *pages: object,
        trace: list[str] | None = None,
    ) -> None:
        self.pages = collections.deque(pages)
        self.trace = trace
        self.calls: list[
            tuple[str, datetime.datetime | None, object, asyncio.Event]
        ] = []

    async def fetch_sid_page(
        self,
        sid: str,
        pos: datetime.datetime | None,
        *,
        subject_id: object,
        shutdown_event: asyncio.Event,
        attempt_observer: (aiohttp_requests.HttpAttemptObserver | None) = None,
    ) -> provider.CallsPageEnvelope:
        if self.trace is not None:
            self.trace.append("fetch")
        self.calls.append((sid, pos, subject_id, shutdown_event))
        if attempt_observer is not None:
            attempt_observer(aiohttp_requests.HttpAttemptKind.JSON, 1)
        result = self.pages.popleft()
        if isinstance(result, _AsyncGate):
            result = await result.wait()
        if isinstance(result, BaseException):
            raise result
        if not isinstance(result, provider.CallsPageEnvelope):
            message = "scripted provider result must be a page"
            raise TypeError(message)
        return result


class _AsyncGate:
    """Expose one awaited stage and release it deterministically."""

    def __init__(self, result: object) -> None:
        self.entered = asyncio.Event()
        self.release = asyncio.Event()
        self.result = result

    async def wait(self) -> object:
        self.entered.set()
        await self.release.wait()
        return self.result


class _SecondPendingInsertFails(dict[str, uuid.UUID]):
    """Raise before the second pending URL insert and permit exact rollback.

    Attributes:
        insertion_count: Number of attempted ``__setitem__`` operations.
    """

    def __init__(self) -> None:
        super().__init__()
        self.insertion_count = 0

    def __setitem__(self, key: str, value: uuid.UUID) -> None:
        self.insertion_count += 1
        if self.insertion_count == 2:
            message = "second pending insert failed"
            raise RuntimeError(message)
        super().__setitem__(key, value)


class _UnexpectedExecutor:
    """Fail if scheduler work executes after an admission rejection."""

    async def execute(
        self,
        execution: feed_work_scheduler.CohortExecution,
    ) -> feed_work_scheduler.CallCompleted:
        del execution
        message = "rejected cohort unexpectedly executed"
        raise AssertionError(message)


def _resolve_successful_execution(
    execution: feed_work_scheduler.CohortExecution,
) -> feed_work_scheduler.CallCompleted:
    """Resolve real producer obligations and return exact durable facts."""
    records = []
    for call in execution.calls:
        payload = typing.cast(
            "cohort_pipeline.ScheduledCohortPayload",
            call.payload,
        )
        matching = tuple(
            entry
            for entry in payload.entries
            if entry.source_order == call.identity.source_order
        )
        if len(matching) != 1:
            message = "execution identity lost its payload entry"
            raise AssertionError(message)
        entry = matching[0]
        ledger = sid_processor.gap_ledger._owner_for(entry.obligation)
        ledger.resolve_success(entry.obligation, call.identity)
        records.append(
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
        )
    return feed_work_scheduler.CallCompleted(
        feed_work_scheduler.CohortTerminalFacts(
            tuple(records),
            feed_work_scheduler.CohortTerminalDisposition.SETTLED,
        )
    )


class _SuccessfulExecutor:
    """Complete real processor payloads through their page-owned ledgers."""

    async def execute(
        self,
        execution: feed_work_scheduler.CohortExecution,
    ) -> feed_work_scheduler.CallCompleted:
        return _resolve_successful_execution(execution)


class _FinalPendingSkipExecutor:
    """Return an exact missing-ts skip awaiting final page resolution."""

    async def execute(
        self,
        execution: feed_work_scheduler.CohortExecution,
    ) -> feed_work_scheduler.CallFinalClosurePending:
        failure = feed_work_scheduler.CohortItemFailureFact(
            feed_store.FeedStatusReason.SOURCE_UNREACHABLE,
            "missing source audio",
        )
        records = []
        for call in execution.calls:
            payload = typing.cast(
                "cohort_pipeline.ScheduledCohortPayload",
                call.payload,
            )
            entry = next(
                item
                for item in payload.entries
                if item.source_order == call.identity.source_order
            )
            ledger = sid_processor.gap_ledger._owner_for(entry.obligation)
            ledger.mark_terminal_skip(
                entry.obligation,
                call.identity,
                failure,
                1,
            )
            records.append(
                feed_work_scheduler.CohortRecordTerminalFact(
                    identity=call.identity,
                    participated=True,
                    closure_state=(
                        feed_work_scheduler.CohortRecordClosureState.FINAL_CLOSURE_PENDING
                    ),
                    full_pipeline_completed=False,
                    terminal_reason=(
                        feed_work_scheduler.CohortRecordTerminalReason.TERMINAL_ITEM_SKIP
                    ),
                    item_failure=failure,
                )
            )
        return feed_work_scheduler.CallFinalClosurePending(
            feed_work_scheduler.CohortTerminalFacts(
                tuple(records),
                feed_work_scheduler.CohortTerminalDisposition.FINAL_CLOSURE_PENDING,
            )
        )


class _ProcessorReplayBarrierExecutor:
    """Hold t1, complete its sibling, and fail if same-Feed t2 executes.

    Attributes:
        started: Source-order tuples that reached physical execution.
        first_entered: Set when the replayable t1 starts.
        sibling_completed: Set after the other Feed resolves durably.
        release_first: Gate allowing t1 to return replayable evidence.
    """

    def __init__(self, replay_feed_id: uuid.UUID) -> None:
        self.replay_feed_id = replay_feed_id
        self.started: list[tuple[int, ...]] = []
        self.first_entered = asyncio.Event()
        self.sibling_completed = asyncio.Event()
        self.release_first = asyncio.Event()

    async def execute(
        self,
        execution: feed_work_scheduler.CohortExecution,
    ) -> (
        feed_work_scheduler.CallCompleted
        | feed_work_scheduler.CallReplayableDirectFailure
    ):
        orders = tuple(call.identity.source_order for call in execution.calls)
        self.started.append(orders)
        feed_id = execution.calls[0].identity.feed_id
        if feed_id != self.replay_feed_id:
            completed = _resolve_successful_execution(execution)
            self.sibling_completed.set()
            return completed
        if orders != (0,):
            message = "same-Feed t2 crossed the replay barrier"
            raise AssertionError(message)
        self.first_entered.set()
        await self.release_first.wait()
        records = tuple(
            feed_work_scheduler.CohortRecordTerminalFact(
                identity=call.identity,
                participated=True,
                closure_state=(
                    feed_work_scheduler.CohortRecordClosureState.REPLAY_SAFE_RELEASE
                ),
                full_pipeline_completed=False,
                terminal_reason=(
                    feed_work_scheduler.CohortRecordTerminalReason.REPLAYABLE_DIRECT
                ),
                direct_failure=feed_work_scheduler.CohortDirectFailureFact(
                    feed_store.FeedStatusReason.SYSTEM_PIPELINE_ERROR,
                    "selected direct precommit failure",
                ),
            )
            for call in execution.calls
        )
        return feed_work_scheduler.CallReplayableDirectFailure(
            feed_work_scheduler.CohortTerminalFacts(
                records,
                feed_work_scheduler.CohortTerminalDisposition.REPLAYABLE_DIRECT,
            )
        )


class _ProcessorReplayPageFinalizer:
    """Accept a real replayable scheduler page with exact source evidence.

    Attributes:
        contexts: Finalization contexts received from the real scheduler.
    """

    def __init__(self) -> None:
        self.contexts: list[feed_work_scheduler.PageFinalizationContext] = []

    async def finalize_page(
        self,
        context: feed_work_scheduler.PageFinalizationContext,
    ) -> feed_work_scheduler.FinalPageReplayable:
        self.contexts.append(context)
        if type(context.candidate) is not cursor_policy.PageCursorCandidate:
            message = "replay finalizer requires a progress candidate"
            raise AssertionError(message)
        verdict = boundary_verdict.decide_page_verdict(
            runtime_adapters._page_verdict_input(context)
        )
        caps = tuple(
            typing.cast("typing.Any", _closure_cap(context.candidate, boundary))
            for boundary in context.candidate_boundaries
        )
        return feed_work_scheduler.FinalPageReplayable(
            grant=context.grant,
            page_sequence=context.page_sequence,
            candidate=context.candidate,
            boundary_results=tuple(
                feed_work_scheduler.BoundaryResult(
                    boundary,
                    feed_work_scheduler.BoundaryDisposition.COMMITTED,
                )
                for boundary in context.candidate_boundaries
            ),
            final_closure_resolutions=(),
            source_evidence=runtime_adapters.PageFinalizationEvidence(
                verdict=verdict,
                closure_caps=caps,
                quarantined_feed_ids=(),
                item_to_feed_promotion_count=(
                    verdict.item_to_feed_promotion_count
                ),
            ),
            member_retirements=context.locally_retired_members,
        )


class _RecordingLane:
    """Record exact coverage/retirement without downstream work."""

    def __init__(
        self,
        *cover_actions: object,
        trace: list[str] | None = None,
    ) -> None:
        self.removed: list[uuid.UUID] = []
        self.retired: set[uuid.UUID] = set()
        self.trace = trace
        self.cover_actions = collections.deque(cover_actions)
        self.covers: list[
            tuple[
                tuple[feed_work_scheduler.CohortSubmission, ...],
                tuple[feed_work_scheduler.BoundaryWork, ...],
                cursor_policy.PageCandidate,
            ]
        ] = []

    async def remove_feed(
        self,
        feed_id: uuid.UUID,
    ) -> feed_work_scheduler.FeedRemoved:
        self.removed.append(feed_id)
        self.retired.add(feed_id)
        return feed_work_scheduler.FeedRemoved(
            grant=_GRANT,
            feed_id=feed_id,
            released_count=0,
            active_retained=True,
        )

    async def is_feed_retired(self, feed_id: uuid.UUID) -> bool:
        return feed_id in self.retired

    async def cover_page(  # noqa: PLR0912
        self,
        *,
        calls: typing.Iterable[feed_work_scheduler.CohortSubmission],
        boundaries: typing.Iterable[feed_work_scheduler.BoundaryWork],
        candidate: cursor_policy.PageCandidate,
        evidence_observer: (
            typing.Callable[[feed_work_scheduler.SchedulerPageEvidence], None]
            | None
        ) = None,
    ) -> feed_work_scheduler.SettledPage | feed_work_scheduler.Undrained:
        if self.trace is not None:
            self.trace.append("cover")
        call_values = tuple(calls)
        boundary_values = tuple(boundaries)
        self.covers.append((call_values, boundary_values, candidate))
        if self.cover_actions:
            action = self.cover_actions.popleft()
            if isinstance(action, _AsyncGate):
                action = await action.wait()
            if callable(action):
                cover_action = typing.cast(
                    "typing.Callable[[tuple[feed_work_scheduler.CohortSubmission, ...], tuple[feed_work_scheduler.BoundaryWork, ...], cursor_policy.PageCandidate], object]",
                    action,
                )
                action = cover_action(call_values, boundary_values, candidate)
                if inspect.isawaitable(action):
                    action = await action
            if isinstance(action, BaseException):
                for cohort in call_values:
                    for call in cohort.calls:
                        if call.settlement_observer is not None:
                            call.settlement_observer(
                                feed_work_scheduler.CallSettlement.ABORTED
                            )
                raise action
            if isinstance(
                action,
                (
                    feed_work_scheduler.SettledPage,
                    feed_work_scheduler.Undrained,
                ),
            ):
                if evidence_observer is not None and isinstance(
                    action, feed_work_scheduler.SettledPage
                ):
                    evidence_observer(action.scheduler_evidence)
                return action
            if action is not None:
                message = "unknown scripted cover action"
                raise AssertionError(message)
        settled = _settled_page(call_values, boundary_values, candidate)
        if evidence_observer is not None:
            evidence_observer(settled.scheduler_evidence)
        return settled


class _RecordingWait:
    """Record interruptible waits without sleeping."""

    def __init__(self, *, trace: list[str] | None = None) -> None:
        self.calls: list[tuple[asyncio.Event, float]] = []
        self.trace = trace

    async def __call__(
        self,
        stop_requested: asyncio.Event,
        seconds: float,
    ) -> None:
        if self.trace is not None:
            self.trace.append("wait")
        self.calls.append((stop_requested, seconds))


class _StoppingWait(_RecordingWait):
    """Stop after an exact count while preserving every wait duration."""

    def __init__(
        self,
        *,
        stop_after: int,
        trace: list[str] | None = None,
    ) -> None:
        super().__init__(trace=trace)
        self.stop_after = stop_after

    async def __call__(
        self,
        stop_requested: asyncio.Event,
        seconds: float,
    ) -> None:
        await super().__call__(stop_requested, seconds)
        if len(self.calls) == self.stop_after:
            stop_requested.set()


class _BlockingWait(_RecordingWait):
    """Wait until the supplied stop signal requests cancellation."""

    def __init__(self) -> None:
        super().__init__()
        self.entered = asyncio.Event()

    async def __call__(
        self,
        stop_requested: asyncio.Event,
        seconds: float,
    ) -> None:
        await super().__call__(stop_requested, seconds)
        self.entered.set()
        await stop_requested.wait()
        raise asyncio.CancelledError


def _processor(
    store: _ScriptedMembershipStore,
    *,
    lane: _RecordingLane | None = None,
    calls_provider: _ScriptedProvider | None = None,
    wait: _RecordingWait | None = None,
    session_id_factory: typing.Callable[[], str] | None = None,
    monotonic: typing.Callable[[], float] | None = None,
    poll_emitter: (
        typing.Callable[[telemetry.SidPollEvidence], None] | None
    ) = None,
    replay_floor_emitter: (
        typing.Callable[[telemetry.ReplayWindowTruncatedEvent], None] | None
    ) = None,
    claim_payload: sid_grant_control.SidClaimPayload | None = None,
    now: typing.Callable[[], datetime.datetime] | None = None,
) -> tuple[sid_processor.BcfyCallsSidProcessor, _RecordingLane]:
    selected_lane = lane or _RecordingLane()
    processor = sid_processor.BcfyCallsSidProcessor(
        _GRANT,
        claim_payload or _claim_payload(),
        store,
        calls_provider or _ScriptedProvider(),
        selected_lane,
        now=now or (lambda: _NOW),
        monotonic=monotonic or (lambda: 1.0),
        poll_emitter=poll_emitter or telemetry.emit_sid_poll,
        replay_floor_emitter=(
            replay_floor_emitter or telemetry.emit_replay_window_truncated
        ),
        wait=wait or _RecordingWait(),
        session_id_factory=(
            session_id_factory or sid_processor._new_session_id
        ),
    )
    return processor, selected_lane


def _raw_call(
    group_id: object,
    url: object,
    ts: object = 1_783_908_001,
) -> dict[str, object]:
    result = {"groupId": group_id, "url": url}
    if ts is not ...:
        result["ts"] = ts
    return result


def _page(
    *calls: object,
    last_pos: object | None,
) -> provider.CallsPageEnvelope:
    payload: dict[str, object] = {"calls": list(calls)}
    if last_pos is not None:
        payload["lastPos"] = last_pos
    distinct_urls = {
        call.get("url")
        for call in calls
        if isinstance(call, collections.abc.Mapping)
        and isinstance(call.get("url"), str)
        and call.get("url")
    }
    return provider.CallsPageEnvelope(
        payload=payload,
        calls=calls,
        last_pos=last_pos,
        http_attempt_count=1,
        response_byte_count=0,
        response_row_count=len(calls),
        response_distinct_audio_url_count=len(distinct_urls),
    )


def _prepared_work(
    processor: sid_processor.BcfyCallsSidProcessor,
    calls: typing.Iterable[object],
    *,
    replay_feed_ids: frozenset[uuid.UUID] = frozenset(),
    frozen_routes: (
        typing.Mapping[str, sid_processor._MemberState] | None
    ) = None,
) -> sid_processor._PreparedPageWork:
    lease_cursor = processor._lease_cursor
    if lease_cursor is None:
        message = "processor membership must be initialized"
        raise AssertionError(message)
    candidate = cursor_policy.LeaseCursor(
        _GRANT,
        pos=lease_cursor.pos,
    ).prepare_no_progress()
    return processor._prepare_page_work(
        calls,
        candidate=candidate,
        replay_feed_ids=replay_feed_ids,
        frozen_routes=frozen_routes,
    )


def _prepared_calls(
    processor: sid_processor.BcfyCallsSidProcessor,
    calls: typing.Iterable[object],
    *,
    replay_feed_ids: frozenset[uuid.UUID] = frozenset(),
    frozen_routes: (
        typing.Mapping[str, sid_processor._MemberState] | None
    ) = None,
) -> tuple[feed_work_scheduler.CallSubmission, ...]:
    prepared = _prepared_work(
        processor,
        calls,
        replay_feed_ids=replay_feed_ids,
        frozen_routes=frozen_routes,
    )
    calls_by_cohort = []
    for cohort in prepared.cohorts:
        calls_by_cohort.extend(cohort.calls)
    return tuple(calls_by_cohort)


def _admit_prepared(
    prepared: sid_processor._PreparedPageWork,
) -> tuple[
    tuple[
        feed_work_scheduler.CallSubmission,
        feed_work_scheduler.CohortRecordIdentity,
        cohort_pipeline.ScheduledCohortEntry,
    ],
    ...,
]:
    admitted = []
    source_order = 0
    local_sequence = 0
    for cohort in prepared.cohorts:
        identities = tuple(
            feed_work_scheduler.CohortRecordIdentity(
                grant=_GRANT,
                member=cohort.member,
                page_sequence=prepared.ledger.page_sequence,
                feed_id=cohort.feed_id,
                cohort_timestamp=cohort.cohort_timestamp,
                source_order=source_order + offset,
                local_sequence=local_sequence + offset,
            )
            for offset, _call in enumerate(cohort.calls)
        )
        source_order += len(cohort.calls)
        local_sequence += len(cohort.calls)
        cohort.admission_hook(identities)
        payload = typing.cast(
            "cohort_pipeline.ScheduledCohortPayload",
            cohort.calls[0].payload,
        )
        admitted.extend(
            zip(cohort.calls, identities, payload.entries, strict=True)
        )
    return tuple(admitted)


def _complete_admitted(
    admitted: tuple[
        feed_work_scheduler.CallSubmission,
        feed_work_scheduler.CohortRecordIdentity,
        cohort_pipeline.ScheduledCohortEntry,
    ],
) -> None:
    call, identity, entry = admitted
    ledger = sid_processor.gap_ledger._owner_for(entry.obligation)
    ledger.resolve_success(entry.obligation, identity)
    assert call.settlement_observer is not None
    call.settlement_observer(feed_work_scheduler.CallSettlement.COMPLETED)


def _submission_urls(
    calls: typing.Iterable[feed_work_scheduler.CallSubmission],
) -> list[str]:
    offsets: dict[int, int] = {}
    urls = []
    for call in calls:
        payload = typing.cast(
            "cohort_pipeline.ScheduledCohortPayload",
            call.payload,
        )
        key = id(payload)
        offset = offsets.get(key, 0)
        urls.append(payload.entries[offset].audio_url)
        offsets[key] = offset + 1
    return urls


def _scheduler_evidence(
    *,
    record_count: int,
    cohort_count: int,
) -> feed_work_scheduler.SchedulerPageEvidence:
    return feed_work_scheduler.SchedulerPageEvidence(
        admitted_record_count=record_count,
        admitted_cohort_count=cohort_count,
        terminal_record_count=record_count,
        replay_blocked_record_count=0,
        total_queue_wait_seconds=0.0,
        maximum_queue_wait_seconds=0.0,
        oldest_queue_age_seconds=0.0,
        pressure_encountered=False,
        pressure_wait_count=0,
        pressure_wait_seconds=0.0,
        maximum_held_count=record_count,
        maximum_queue_depth=record_count,
        maximum_worker_utilization_numerator=0,
        worker_utilization_denominator=1,
        early_flush_attempt_count=0,
        final_flush_attempt_count=1,
        total_flush_latency_seconds=0.0,
        maximum_flush_latency_seconds=0.0,
        fence_rejection_count=0,
        member_rejection_count=0,
    )


def _closure_cap(
    candidate: cursor_policy.PageCursorCandidate,
    boundary: feed_work_scheduler.BoundaryWork,
) -> runtime_adapters._FeedClosureCap:
    result = ingestion_lease_store.ChildMutationResult(
        feed_id=boundary.feed_id,
        disposition=ingestion_lease_store.ChildDisposition.APPLIED,
        cursor_effect=ingestion_lease_store.CursorEffect.ADVANCED,
        lifecycle_effect=ingestion_lease_store.LifecycleEffect.NONE,
    )
    seal = runtime_adapters._FeedClosureCapSeal(
        grant=candidate.grant,
        page_sequence=candidate.page_sequence,
        feed_id=boundary.feed_id,
        accepted_through=candidate.last_pos,
        requested_target=candidate.last_pos,
        command_kind=runtime_adapters._FinalChildCommandKind.OBSERVATION,
        result=result,
        _construction_key=runtime_adapters._FEED_CLOSURE_CAP_CONSTRUCTION_KEY,
    )
    return runtime_adapters._FeedClosureCap(
        grant=candidate.grant,
        page_sequence=candidate.page_sequence,
        feed_id=boundary.feed_id,
        accepted_through=candidate.last_pos,
        requested_target=candidate.last_pos,
        command_kind=runtime_adapters._FinalChildCommandKind.OBSERVATION,
        result=result,
        _seal=seal,
    )


def _settled_page(
    cohorts: tuple[feed_work_scheduler.CohortSubmission, ...],
    boundaries: tuple[feed_work_scheduler.BoundaryWork, ...],
    candidate: cursor_policy.PageCandidate,
) -> feed_work_scheduler.SettledPage:
    if type(candidate) not in {
        cursor_policy.PageCursorCandidate,
        cursor_policy.NoProgressPageCandidate,
    }:
        message = "unexpected candidate"
        raise AssertionError(message)
    typed_candidate = candidate
    identities = []
    terminal_facts = []
    local_sequence = 0
    for cohort in cohorts:
        cohort_identities = tuple(
            feed_work_scheduler.CohortRecordIdentity(
                grant=_GRANT,
                member=cohort.member,
                page_sequence=typed_candidate.page_sequence,
                feed_id=cohort.feed_id,
                cohort_timestamp=cohort.cohort_timestamp,
                source_order=len(identities) + offset,
                local_sequence=local_sequence + offset,
            )
            for offset, _call in enumerate(cohort.calls)
        )
        local_sequence += len(cohort.calls)
        cohort.admission_hook(cohort_identities)
        records = []
        payload = typing.cast(
            "cohort_pipeline.ScheduledCohortPayload",
            cohort.calls[0].payload,
        )
        for identity, call, entry in zip(
            cohort_identities,
            cohort.calls,
            payload.entries,
            strict=True,
        ):
            ledger = sid_processor.gap_ledger._owner_for(entry.obligation)
            ledger.resolve_success(entry.obligation, identity)
            if call.settlement_observer is not None:
                call.settlement_observer(
                    feed_work_scheduler.CallSettlement.COMPLETED
                )
            records.append(
                feed_work_scheduler.CohortRecordTerminalFact(
                    identity=identity,
                    participated=True,
                    closure_state=(
                        feed_work_scheduler.CohortRecordClosureState.DURABLY_CLOSED
                    ),
                    full_pipeline_completed=True,
                    terminal_reason=(
                        feed_work_scheduler.CohortRecordTerminalReason.FULL_PIPELINE
                    ),
                )
            )
        identities.extend(cohort_identities)
        terminal_facts.append(
            feed_work_scheduler.CohortTerminalFacts(
                records=tuple(records),
                disposition=feed_work_scheduler.CohortTerminalDisposition.SETTLED,
            )
        )
    members_by_id = {}
    for cohort in cohorts:
        members_by_id[cohort.member.feed_id] = cohort.member
    for boundary in boundaries:
        members_by_id[boundary.member.feed_id] = boundary.member
    verdict = boundary_verdict.decide_page_verdict(
        boundary_verdict.PageVerdictInput(
            grant=_GRANT,
            page_sequence=typed_candidate.page_sequence,
            members=tuple(
                members_by_id[key]
                for key in sorted(members_by_id, key=lambda value: value.int)
            ),
            record_identities=tuple(
                sorted(
                    identities,
                    key=lambda value: (
                        value.feed_id.int,
                        value.source_order,
                        value.local_sequence,
                    ),
                )
            ),
            cohort_terminal_facts=tuple(
                sorted(
                    terminal_facts,
                    key=lambda value: (
                        value.records[0].identity.feed_id.int,
                        value.records[0].identity.source_order,
                    ),
                )
            ),
            child_recovery_candidates=(),
            lease_recovery_candidate=False,
        )
    )
    caps = (
        tuple(
            typing.cast("typing.Any", _closure_cap(typed_candidate, boundary))
            for boundary in sorted(
                boundaries,
                key=lambda value: value.feed_id.int,
            )
        )
        if type(typed_candidate) is cursor_policy.PageCursorCandidate
        else ()
    )
    source_evidence = runtime_adapters.PageFinalizationEvidence(
        verdict=verdict,
        closure_caps=caps,
        quarantined_feed_ids=(),
        item_to_feed_promotion_count=verdict.item_to_feed_promotion_count,
    )
    if isinstance(typed_candidate, cursor_policy.PageCursorCandidate):
        lease_settlement = cursor_policy._issue_covered_page(typed_candidate)
    else:
        assert isinstance(
            typed_candidate,
            cursor_policy.NoProgressPageCandidate,
        )
        lease_settlement = cursor_policy._issue_no_progress_page(
            typed_candidate
        )
    return feed_work_scheduler.SettledPage(
        candidate=typed_candidate,
        lease_settlement=lease_settlement,
        final_closure_resolutions=(),
        scheduler_evidence=_scheduler_evidence(
            record_count=len(identities),
            cohort_count=len(cohorts),
        ),
        source_evidence=source_evidence,
    )


def _quarantined_settled_page(
    cohorts: tuple[feed_work_scheduler.CohortSubmission, ...],
    boundaries: tuple[feed_work_scheduler.BoundaryWork, ...],
    candidate: cursor_policy.PageCandidate,
) -> feed_work_scheduler.SettledPage:
    settled = _settled_page(cohorts, boundaries, candidate)
    evidence = typing.cast(
        "runtime_adapters.PageFinalizationEvidence",
        settled.source_evidence,
    )
    return dataclasses.replace(
        settled,
        source_evidence=dataclasses.replace(
            evidence,
            quarantined_feed_ids=(_FEED_A,),
        ),
    )


def _promoted_settled_page(
    cohorts: tuple[feed_work_scheduler.CohortSubmission, ...],
    boundaries: tuple[feed_work_scheduler.BoundaryWork, ...],
    candidate: cursor_policy.PageCandidate,
) -> feed_work_scheduler.SettledPage:
    settled = _settled_page(cohorts, boundaries, candidate)
    assert isinstance(candidate, cursor_policy.PageCursorCandidate)
    identities = []
    facts = []
    source_order = 0
    for cohort in cohorts:
        records = []
        for offset, _call in enumerate(cohort.calls):
            identity = feed_work_scheduler.CohortRecordIdentity(
                grant=_GRANT,
                member=cohort.member,
                page_sequence=candidate.page_sequence,
                feed_id=cohort.feed_id,
                cohort_timestamp=cohort.cohort_timestamp,
                source_order=source_order + offset,
                local_sequence=source_order + offset,
            )
            identities.append(identity)
            records.append(
                feed_work_scheduler.CohortRecordTerminalFact(
                    identity=identity,
                    participated=True,
                    closure_state=(
                        feed_work_scheduler.CohortRecordClosureState.DURABLY_CLOSED
                    ),
                    full_pipeline_completed=False,
                    terminal_reason=(
                        feed_work_scheduler.CohortRecordTerminalReason.TERMINAL_ITEM_SKIP
                    ),
                    item_failure=feed_work_scheduler.CohortItemFailureFact(
                        feed_store.FeedStatusReason.SYSTEM_SOURCE_PAYLOAD_INVALID,
                        "terminal item skip",
                    ),
                )
            )
        source_order += len(cohort.calls)
        facts.append(
            feed_work_scheduler.CohortTerminalFacts(
                tuple(records),
                feed_work_scheduler.CohortTerminalDisposition.SETTLED,
            )
        )
    members = tuple(
        cohort.member
        for cohort in sorted(
            cohorts,
            key=lambda value: value.feed_id.int,
        )
    )
    verdict = boundary_verdict.decide_page_verdict(
        boundary_verdict.PageVerdictInput(
            grant=_GRANT,
            page_sequence=candidate.page_sequence,
            members=members,
            record_identities=tuple(
                sorted(
                    identities,
                    key=lambda value: (
                        value.feed_id.int,
                        value.source_order,
                    ),
                )
            ),
            cohort_terminal_facts=tuple(
                sorted(
                    facts,
                    key=lambda value: (
                        value.records[0].identity.feed_id.int,
                        value.records[0].identity.source_order,
                    ),
                )
            ),
            child_recovery_candidates=(),
            lease_recovery_candidate=False,
        )
    )
    evidence = typing.cast(
        "runtime_adapters.PageFinalizationEvidence",
        settled.source_evidence,
    )
    return dataclasses.replace(
        settled,
        source_evidence=dataclasses.replace(
            evidence,
            verdict=verdict,
            item_to_feed_promotion_count=(verdict.item_to_feed_promotion_count),
        ),
    )


def _replayable_settled_page(
    cohorts: tuple[feed_work_scheduler.CohortSubmission, ...],
    boundaries: tuple[feed_work_scheduler.BoundaryWork, ...],
    candidate: cursor_policy.PageCandidate,
) -> feed_work_scheduler.SettledPage:
    del boundaries
    if type(candidate) is not cursor_policy.PageCursorCandidate:
        message = "replayable test settlement requires a progress candidate"
        raise AssertionError(message)
    identities = []
    terminal_facts = []
    source_order = 0
    for cohort in cohorts:
        cohort_identities = tuple(
            feed_work_scheduler.CohortRecordIdentity(
                grant=_GRANT,
                member=cohort.member,
                page_sequence=candidate.page_sequence,
                feed_id=cohort.feed_id,
                cohort_timestamp=cohort.cohort_timestamp,
                source_order=source_order + offset,
                local_sequence=source_order + offset,
            )
            for offset, _call in enumerate(cohort.calls)
        )
        source_order += len(cohort.calls)
        cohort.admission_hook(cohort_identities)
        records = []
        for identity, call in zip(
            cohort_identities,
            cohort.calls,
            strict=True,
        ):
            if call.settlement_observer is not None:
                call.settlement_observer(
                    feed_work_scheduler.CallSettlement.RETRYABLE
                )
            records.append(
                feed_work_scheduler.CohortRecordTerminalFact(
                    identity=identity,
                    participated=True,
                    closure_state=(
                        feed_work_scheduler.CohortRecordClosureState.REPLAY_SAFE_RELEASE
                    ),
                    full_pipeline_completed=False,
                    terminal_reason=(
                        feed_work_scheduler.CohortRecordTerminalReason.REPLAYABLE_DIRECT
                    ),
                    direct_failure=feed_work_scheduler.CohortDirectFailureFact(
                        feed_store.FeedStatusReason.SYSTEM_PIPELINE_ERROR,
                        "selected direct precommit failure",
                    ),
                )
            )
        identities.extend(cohort_identities)
        terminal_facts.append(
            feed_work_scheduler.CohortTerminalFacts(
                tuple(records),
                feed_work_scheduler.CohortTerminalDisposition.REPLAYABLE_DIRECT,
            )
        )
    members = tuple(
        {cohort.member.feed_id: cohort.member for cohort in cohorts}[feed_id]
        for feed_id in sorted(
            {cohort.member.feed_id for cohort in cohorts},
            key=lambda value: value.int,
        )
    )
    verdict = boundary_verdict.decide_page_verdict(
        boundary_verdict.PageVerdictInput(
            grant=_GRANT,
            page_sequence=candidate.page_sequence,
            members=members,
            record_identities=tuple(
                sorted(
                    identities,
                    key=lambda value: (
                        value.feed_id.int,
                        value.source_order,
                    ),
                )
            ),
            cohort_terminal_facts=tuple(
                sorted(
                    terminal_facts,
                    key=lambda value: (
                        value.records[0].identity.feed_id.int,
                        value.records[0].identity.source_order,
                    ),
                )
            ),
            child_recovery_candidates=(),
            lease_recovery_candidate=False,
        )
    )
    return feed_work_scheduler.SettledPage(
        candidate=candidate,
        lease_settlement=cursor_policy._issue_replayable_page(candidate),
        final_closure_resolutions=(),
        scheduler_evidence=_scheduler_evidence(
            record_count=len(identities),
            cohort_count=len(cohorts),
        ),
        source_evidence=runtime_adapters.PageFinalizationEvidence(
            verdict=verdict,
            closure_caps=(),
            quarantined_feed_ids=(),
            item_to_feed_promotion_count=(verdict.item_to_feed_promotion_count),
        ),
    )


class TestBcfyCallsSidProcessor(unittest.IsolatedAsyncioTestCase):
    """Exact routing, snapshot, and URL-state contracts."""

    async def test_claim_mode_payload_identity_survives_membership_refresh(
        self,
    ) -> None:
        payload = _claim_payload(grant_control.ClaimMode.RECOVERY)
        processor, _ = _processor(
            _ScriptedMembershipStore(
                _snapshot(4, _member(_FEED_A, "00123-00045"))
            ),
            claim_payload=payload,
        )

        await processor._refresh_membership()

        self.assertIs(processor._claim_payload, payload)
        self.assertIs(
            processor._claim_payload.claim_mode,
            grant_control.ClaimMode.RECOVERY,
        )

    async def test_claim_mode_alone_selects_bootstrap_or_recovery_floor_cause(
        self,
    ) -> None:
        old_cursor = _NOW - datetime.timedelta(minutes=20)
        cases = (
            (
                sid_grant_control.SidClaimPayload(
                    grant_control.ClaimMode.PRIMARY,
                ),
                cursor_policy.ReplayFloorCause.BOOTSTRAP,
            ),
            (
                sid_grant_control.SidClaimPayload(
                    grant_control.ClaimMode.RECOVERY,
                ),
                cursor_policy.ReplayFloorCause.RECOVERY,
            ),
        )

        for payload, expected_cause in cases:
            with self.subTest(claim_mode=payload.claim_mode.value):
                emitted: list[telemetry.ReplayWindowTruncatedEvent] = []
                processor, _ = _processor(
                    _ScriptedMembershipStore(
                        _snapshot(
                            4,
                            _member(
                                _FEED_A,
                                "00123-00045",
                                cursor=old_cursor,
                            ),
                        )
                    ),
                    claim_payload=payload,
                    replay_floor_emitter=emitted.append,
                )

                await processor._refresh_membership()
                request = processor._select_request_position()

                self.assertEqual(
                    request.pos,
                    _NOW - datetime.timedelta(minutes=5),
                )
                self.assertEqual(len(emitted), 1)
                self.assertIs(
                    emitted[0].floor_reached.cause,
                    expected_cause,
                )

    async def test_all_null_bootstrap_omits_pos_without_floor_event(
        self,
    ) -> None:
        emitted: list[telemetry.ReplayWindowTruncatedEvent] = []
        processor, _ = _processor(
            _ScriptedMembershipStore(
                _snapshot(
                    4,
                    _member(_FEED_A, "00123-00045", cursor=None),
                )
            ),
            replay_floor_emitter=emitted.append,
        )

        await processor._refresh_membership()
        request = processor._select_request_position()

        self.assertIsNone(request.pos)
        self.assertEqual(emitted, [])

    async def test_replay_floor_emitter_failure_does_not_change_bootstrap(
        self,
    ) -> None:
        old_cursor = _NOW - datetime.timedelta(minutes=20)

        def fail(_event: telemetry.ReplayWindowTruncatedEvent) -> None:
            message = "telemetry unavailable"
            raise RuntimeError(message)

        processor, _ = _processor(
            _ScriptedMembershipStore(
                _snapshot(
                    4,
                    _member(
                        _FEED_A,
                        "00123-00045",
                        cursor=old_cursor,
                    ),
                )
            ),
            replay_floor_emitter=fail,
        )

        await processor._refresh_membership()
        request = processor._select_request_position()

        floor = _NOW - datetime.timedelta(minutes=5)
        self.assertEqual(request.pos, floor)
        assert processor._lease_cursor is not None
        self.assertEqual(processor._lease_cursor.pos, floor)

    async def test_bootstrap_rechecks_floor_at_request_boundary(self) -> None:
        requested = _NOW - datetime.timedelta(minutes=4, seconds=59)
        observed_times = iter((_NOW, _NOW + datetime.timedelta(seconds=2)))
        emitted: list[telemetry.ReplayWindowTruncatedEvent] = []
        processor, _ = _processor(
            _ScriptedMembershipStore(
                _snapshot(
                    4,
                    _member(
                        _FEED_A,
                        "00123-00045",
                        cursor=requested,
                    ),
                )
            ),
            now=lambda: next(observed_times),
            replay_floor_emitter=emitted.append,
        )

        await processor._refresh_membership()
        request = processor._select_request_position()

        request_floor = (
            _NOW
            - datetime.timedelta(minutes=5)
            + (datetime.timedelta(seconds=2))
        )
        self.assertEqual(request.pos, request_floor)
        self.assertEqual(len(emitted), 1)
        self.assertIs(
            emitted[0].floor_reached.cause,
            cursor_policy.ReplayFloorCause.BOOTSTRAP,
        )
        self.assertEqual(emitted[0].floor_reached.lost_duration_seconds, 1.0)

    async def test_snapshot_initial_and_unchanged_reuse_identity(self) -> None:
        initial = _snapshot(4, _member(_FEED_A, "00123-00045"))
        store = _ScriptedMembershipStore(
            initial,
            ingestion_lease_store.MembershipUnchanged(_GRANT, 4),
        )
        processor, _ = _processor(store)

        await processor._refresh_membership()
        frozen_snapshot = processor._snapshot
        frozen_routes = processor._members_by_source
        await processor._refresh_membership()

        self.assertIs(processor._snapshot, frozen_snapshot)
        self.assertIs(processor._members_by_source, frozen_routes)
        self.assertEqual(store.calls, [(_GRANT, None), (_GRANT, 4)])
        self.assertIs(
            processor._member_state(_FEED_A).route_state,
            sid_processor.MemberRouteState.NORMAL,
        )

    async def test_snapshot_page_keeps_frozen_routes_until_next_page(
        self,
    ) -> None:
        member_a = _member(_FEED_A, "00123-00045")
        member_b = _member(_FEED_B, "00123-00046")
        store = _ScriptedMembershipStore(
            _snapshot(4, member_a),
            _snapshot(5, member_a, member_b),
        )
        processor, _ = _processor(store)
        await processor._refresh_membership()
        old_page = _prepared_calls(
            processor, [_raw_call("00123-00046", "https://audio/new")]
        )

        await processor._refresh_membership()

        self.assertEqual(list(old_page), [])
        next_page = list(
            _prepared_calls(
                processor, [_raw_call("00123-00046", "https://audio/new")]
            )
        )
        self.assertEqual([call.feed_id for call in next_page], [_FEED_B])

    async def test_snapshot_removal_and_reactivation_plan_drain(self) -> None:
        member_a = _member(_FEED_A, "00123-00045")
        member_b = _member(_FEED_B, "00123-00046")
        store = _ScriptedMembershipStore(
            _snapshot(4, member_a, member_b),
            _snapshot(5, member_a),
            _snapshot(6, member_a, member_b),
        )
        processor, lane = _processor(store)
        await processor._refresh_membership()
        prepared = _prepared_work(
            processor,
            [_raw_call("00123-00046", "https://audio/retired")],
        )
        pending = _admit_prepared(prepared)[0]

        await processor._refresh_membership()

        self.assertEqual(lane.removed, [_FEED_B])
        self.assertNotIn("https://audio/retired", processor._pending_by_url)
        _complete_admitted(pending)
        self.assertNotIn("https://audio/retired", processor._recent_urls)
        with self.assertRaises(sid_processor.SidProcessorPlannedDrain):
            await processor._refresh_membership()

    async def test_route_uses_exact_leading_zero_source_key(self) -> None:
        store = _ScriptedMembershipStore(
            _snapshot(4, _member(_FEED_A, "00123-00045"))
        )
        processor, _ = _processor(store)
        await processor._refresh_membership()

        submissions = list(
            _prepared_calls(
                processor,
                [
                    _raw_call("123-45", "https://audio/normalized"),
                    _raw_call(12345, "https://audio/integer"),
                    _raw_call("00123-00046", "https://audio/unknown"),
                    _raw_call("00123-00045", "https://audio/exact"),
                ],
            )
        )

        self.assertEqual(len(submissions), 1)
        self.assertEqual(submissions[0].feed_id, _FEED_A)
        payload = submissions[0].payload
        self.assertIsInstance(
            payload,
            cohort_pipeline.ScheduledCohortPayload,
        )
        assert isinstance(payload, cohort_pipeline.ScheduledCohortPayload)
        self.assertEqual(payload.entries[0].audio_url, "https://audio/exact")

    async def test_malformed_siblings_are_item_local(self) -> None:
        store = _ScriptedMembershipStore(
            _snapshot(4, _member(_FEED_A, "00123-00045"))
        )
        processor, _ = _processor(store)
        await processor._refresh_membership()

        calls: list[object] = [
            None,
            [],
            {"groupId": "00123-00045", "url": ""},
            _raw_call("00123-00045", "https://audio/bool", ts=True),
            _raw_call("00123-00045", "https://audio/nan", float("nan")),
            _raw_call("00123-00045", "https://audio/string", "123"),
            _raw_call("00123-00045", "https://audio/valid"),
        ]

        submissions = list(_prepared_calls(processor, calls))

        self.assertEqual(len(submissions), 1)
        self.assertEqual(
            _submission_urls(submissions)[0],
            "https://audio/valid",
        )

    async def test_route_missing_ts_and_inclusive_coverage(self) -> None:
        cursor = datetime.datetime.fromtimestamp(1_783_908_001, datetime.UTC)
        store = _ScriptedMembershipStore(
            _snapshot(
                4,
                _member(_FEED_A, "00123-00045", cursor=cursor),
            )
        )
        processor, _ = _processor(store)
        await processor._refresh_membership()

        with self.assertLogs(
            "backend.pipeline.ingestion.collectors.bcfy_calls.sid_processor",
            level="WARNING",
        ) as captured:
            submissions = list(
                _prepared_calls(
                    processor,
                    [
                        _raw_call(
                            "00123-00045",
                            "https://audio/missing",
                            ...,
                        ),
                        _raw_call(
                            "00123-00045",
                            "https://audio/equal",
                            1_783_908_001,
                        ),
                        _raw_call(
                            "00123-00045",
                            "https://audio/newer",
                            1_783_908_002,
                        ),
                    ],
                )
            )

        self.assertEqual(
            _submission_urls(submissions),
            ["https://audio/missing", "https://audio/newer"],
        )
        self.assertIsNone(submissions[0].source_timestamp)
        self.assertEqual(
            captured.records[0].json_fields["event_type"],
            "bcfy_calls_missing_ts",
        )

    async def test_route_equal_ts_distinct_urls_keep_source_order(self) -> None:
        store = _ScriptedMembershipStore(
            _snapshot(
                4,
                _member(
                    _FEED_A,
                    "00123-00045",
                    cursor=datetime.datetime.fromtimestamp(0, datetime.UTC),
                ),
            )
        )
        processor, _ = _processor(store)
        await processor._refresh_membership()

        submissions = list(
            _prepared_calls(
                processor,
                [
                    _raw_call("00123-00045", "https://audio/later", 20),
                    _raw_call("00123-00045", "https://audio/equal-a", 10),
                    _raw_call("00123-00045", "https://audio/equal-b", 10),
                ],
            )
        )

        self.assertEqual(
            _submission_urls(submissions),
            [
                "https://audio/equal-a",
                "https://audio/equal-b",
                "https://audio/later",
            ],
        )

    async def test_dedup_pending_and_grant_wide_url_identity(self) -> None:
        store = _ScriptedMembershipStore(
            _snapshot(
                4,
                _member(_FEED_A, "00123-00045"),
                _member(_FEED_B, "00123-00046"),
            )
        )
        processor, _ = _processor(store)
        await processor._refresh_membership()

        prepared = _prepared_work(
            processor,
            [
                _raw_call("00123-00045", "https://audio/shared"),
                _raw_call("00123-00046", "https://audio/shared"),
            ],
        )
        submissions = _admit_prepared(prepared)

        self.assertEqual(len(submissions), 1)
        self.assertEqual(
            processor._pending_by_url,
            {"https://audio/shared": _FEED_A},
        )

    async def test_dedup_all_settlement_outcomes(self) -> None:
        store = _ScriptedMembershipStore(
            _snapshot(4, _member(_FEED_A, "00123-00045"))
        )
        processor, _ = _processor(store)
        await processor._refresh_membership()

        for settlement in feed_work_scheduler.CallSettlement:
            with self.subTest(settlement=settlement.value):
                url = f"https://audio/{settlement.value}"
                prepared = _prepared_work(
                    processor,
                    [_raw_call("00123-00045", url)],
                )
                admitted = _admit_prepared(prepared)[0]
                submission = admitted[0]
                self.assertEqual(processor._pending_by_url[url], _FEED_A)

                assert submission.settlement_observer is not None
                if settlement is feed_work_scheduler.CallSettlement.COMPLETED:
                    _complete_admitted(admitted)
                else:
                    submission.settlement_observer(settlement)

                self.assertNotIn(url, processor._pending_by_url)
                if settlement is feed_work_scheduler.CallSettlement.COMPLETED:
                    self.assertIn(url, processor._recent_urls)
                    self.assertEqual(
                        list(
                            _prepared_calls(
                                processor, [_raw_call("00123-00045", url)]
                            )
                        ),
                        [],
                    )
                else:
                    self.assertNotIn(url, processor._recent_urls)

    async def test_dedup_recent_cap_evicts_exactly_one_thousand(self) -> None:
        store = _ScriptedMembershipStore(
            _snapshot(4, _member(_FEED_A, "00123-00045"))
        )
        processor, _ = _processor(store)
        await processor._refresh_membership()

        for index in range(1_001):
            prepared = _prepared_work(
                processor,
                [
                    _raw_call(
                        "00123-00045",
                        f"https://audio/{index}",
                        1_783_908_100 + index,
                    )
                ],
            )
            _complete_admitted(_admit_prepared(prepared)[0])

        self.assertEqual(len(processor._recent_order), 1_000)
        self.assertEqual(len(processor._recent_urls), 1_000)
        self.assertNotIn("https://audio/0", processor._recent_urls)
        self.assertIn("https://audio/1", processor._recent_urls)
        self.assertIn("https://audio/1000", processor._recent_urls)

    async def test_dedup_feed_cleanup_late_completion_and_close(self) -> None:
        member_a = _member(_FEED_A, "00123-00045")
        member_b = _member(_FEED_B, "00123-00046")
        store = _ScriptedMembershipStore(
            _snapshot(4, member_a, member_b),
            _snapshot(5, member_b),
        )
        processor, lane = _processor(store)
        await processor._refresh_membership()
        prepared = _prepared_work(
            processor,
            [
                _raw_call("00123-00045", "https://audio/a"),
                _raw_call("00123-00046", "https://audio/b"),
            ],
        )
        calls = _admit_prepared(prepared)

        await processor._refresh_membership()

        self.assertEqual(lane.removed, [_FEED_A])
        self.assertEqual(
            processor._pending_by_url,
            {"https://audio/b": _FEED_B},
        )
        _complete_admitted(calls[0])
        self.assertNotIn("https://audio/a", processor._recent_urls)

        processor.close()

        self.assertEqual(processor._pending_by_url, {})
        self.assertEqual(processor._recent_urls, set())
        self.assertEqual(list(processor._recent_order), [])
        self.assertEqual(processor._session_by_member, {})
        self.assertEqual(processor._member_by_session, {})
        self.assertEqual(processor._members_by_source, {})

    async def test_adopt_null_member_on_valid_empty_page_then_route(
        self,
    ) -> None:
        store = _ScriptedMembershipStore(
            _snapshot(
                4,
                _member(_FEED_A, "00123-00045", cursor=None),
            )
        )
        processor, lane = _processor(store)
        await processor._refresh_membership()
        request = processor._select_request_position()
        target = _NOW + datetime.timedelta(seconds=10)

        await processor._settle_page(
            _page(
                _raw_call("00123-00045", "https://audio/adoption"),
                last_pos=target.timestamp(),
            ),
            request=request,
        )

        calls, boundaries, _ = lane.covers[0]
        self.assertEqual(calls, ())
        self.assertEqual(
            boundaries,
            (
                feed_work_scheduler.BoundaryWork(
                    processor._member_state(_FEED_A).member.identity,
                    target,
                ),
            ),
        )
        self.assertIs(
            processor._member_state(_FEED_A).route_state,
            sid_processor.MemberRouteState.NORMAL,
        )
        routed = list(
            _prepared_calls(
                processor,
                [
                    _raw_call(
                        "00123-00045",
                        "https://audio/following-poll",
                        target.timestamp() + 1,
                    )
                ],
            )
        )
        self.assertEqual(len(routed), 1)

    async def test_adopt_new_null_member_classification(self) -> None:
        member_a = _member(
            _FEED_A,
            "00123-00045",
            cursor=_NOW - datetime.timedelta(seconds=30),
        )
        member_b = _member(_FEED_B, "00123-00046", cursor=None)
        store = _ScriptedMembershipStore(
            _snapshot(4, member_a),
            _snapshot(5, member_a, member_b),
        )
        processor, _ = _processor(store)
        await processor._refresh_membership()

        await processor._refresh_membership()

        self.assertIs(
            processor._member_state(_FEED_B).route_state,
            sid_processor.MemberRouteState.ADOPTING,
        )

    async def test_adopt_missing_malformed_and_regressive_last_pos_retained(
        self,
    ) -> None:
        cases = (
            None,
            "malformed",
            (_NOW - datetime.timedelta(minutes=1)).timestamp(),
        )
        for last_pos in cases:
            with self.subTest(last_pos=last_pos):
                member_a = _member(
                    _FEED_A,
                    "00123-00045",
                    cursor=_NOW - datetime.timedelta(seconds=30),
                )
                member_b = _member(_FEED_B, "00123-00046", cursor=None)
                store = _ScriptedMembershipStore(
                    _snapshot(4, member_a),
                    _snapshot(5, member_a, member_b),
                )
                processor, lane = _processor(store)
                await processor._refresh_membership()
                await processor._refresh_membership()
                original_cursor = processor._lease_cursor.pos

                await processor._settle_page(
                    _page(last_pos=last_pos),
                    request=processor._select_request_position(),
                )

                _, boundaries, candidate = lane.covers[0]
                self.assertEqual(boundaries, ())
                self.assertIsInstance(
                    candidate,
                    sid_processor.cursor_policy.NoProgressPageCandidate,
                )
                self.assertEqual(processor._lease_cursor.pos, original_cursor)
                self.assertIs(
                    processor._member_state(_FEED_B).route_state,
                    sid_processor.MemberRouteState.ADOPTING,
                )

    async def test_adopt_aborted_coverage_retains_state(self) -> None:
        lane = _RecordingLane(RuntimeError("cover aborted"))
        store = _ScriptedMembershipStore(
            _snapshot(
                4,
                _member(_FEED_A, "00123-00045", cursor=None),
            )
        )
        processor, _ = _processor(store, lane=lane)
        await processor._refresh_membership()

        with self.assertRaisesRegex(RuntimeError, "cover aborted"):
            await processor._settle_page(
                _page(last_pos=_NOW.timestamp()),
                request=processor._select_request_position(),
            )

        self.assertIs(
            processor._member_state(_FEED_A).route_state,
            sid_processor.MemberRouteState.ADOPTING,
        )
        self.assertIsNotNone(
            processor._lease_cursor.outstanding_candidate,
        )

    async def test_replay_classifies_older_equal_newer_and_null_joins(
        self,
    ) -> None:
        current = _NOW - datetime.timedelta(seconds=30)
        member_a = _member(_FEED_A, "00123-00045", cursor=current)
        store = _ScriptedMembershipStore(
            _snapshot(4, member_a),
            _snapshot(
                5,
                member_a,
                _member(
                    _FEED_B,
                    "00123-00046",
                    cursor=current - datetime.timedelta(seconds=1),
                ),
                _member(_FEED_C, "00123-00047", cursor=current),
                _member(
                    _FEED_D,
                    "00123-00048",
                    cursor=current + datetime.timedelta(seconds=1),
                ),
                _member(_FEED_E, "00123-00049", cursor=None),
            ),
        )
        processor, _ = _processor(store)
        await processor._refresh_membership()

        await processor._refresh_membership()

        self.assertIs(
            processor._member_state(_FEED_B).route_state,
            sid_processor.MemberRouteState.REPLAY_PENDING,
        )
        self.assertIs(
            processor._member_state(_FEED_C).route_state,
            sid_processor.MemberRouteState.NORMAL,
        )
        self.assertIs(
            processor._member_state(_FEED_D).route_state,
            sid_processor.MemberRouteState.NORMAL,
        )
        self.assertIs(
            processor._member_state(_FEED_E).route_state,
            sid_processor.MemberRouteState.ADOPTING,
        )

    async def test_replay_initial_floor_does_not_mark_initial_member(
        self,
    ) -> None:
        old_cursor = _NOW - datetime.timedelta(minutes=30)
        store = _ScriptedMembershipStore(
            _snapshot(
                4,
                _member(_FEED_A, "00123-00045", cursor=old_cursor),
            )
        )
        processor, _ = _processor(store)

        await processor._refresh_membership()

        self.assertEqual(
            processor._lease_cursor.pos,
            _NOW - datetime.timedelta(minutes=5),
        )
        self.assertIs(
            processor._member_state(_FEED_A).route_state,
            sid_processor.MemberRouteState.NORMAL,
        )

    async def test_replay_coalesces_minimum_with_five_minute_clamp(
        self,
    ) -> None:
        current = _NOW - datetime.timedelta(seconds=30)
        member_a = _member(_FEED_A, "00123-00045", cursor=current)
        store = _ScriptedMembershipStore(
            _snapshot(4, member_a),
            _snapshot(
                5,
                member_a,
                _member(
                    _FEED_B,
                    "00123-00046",
                    cursor=_NOW - datetime.timedelta(minutes=20),
                ),
                _member(
                    _FEED_C,
                    "00123-00047",
                    cursor=_NOW - datetime.timedelta(minutes=2),
                ),
            ),
        )
        emitted: list[telemetry.ReplayWindowTruncatedEvent] = []
        processor, _ = _processor(
            store,
            replay_floor_emitter=emitted.append,
        )
        await processor._refresh_membership()
        await processor._refresh_membership()
        before_cursor = processor._lease_cursor.pos

        request = processor._select_request_position()

        self.assertEqual(
            request.pos,
            _NOW - datetime.timedelta(minutes=5),
        )
        self.assertEqual(request.replay_feed_ids, frozenset({_FEED_B, _FEED_C}))
        self.assertEqual(processor._lease_cursor.pos, before_cursor)
        self.assertEqual(len(emitted), 1)
        evidence = emitted[0].floor_reached
        self.assertIs(
            evidence.cause,
            cursor_policy.ReplayFloorCause.REPLAY_OVERRIDE,
        )
        self.assertEqual(
            evidence.requested_start,
            _NOW - datetime.timedelta(minutes=20),
        )
        self.assertEqual(evidence.lost_duration_seconds, 900.0)

    async def test_replay_valid_override_routes_all_and_consumes_state(
        self,
    ) -> None:
        current = _NOW - datetime.timedelta(seconds=30)
        member_a = _member(_FEED_A, "00123-00045", cursor=current)
        member_b = _member(
            _FEED_B,
            "00123-00046",
            cursor=_NOW - datetime.timedelta(minutes=2),
        )
        store = _ScriptedMembershipStore(
            _snapshot(4, member_a),
            _snapshot(5, member_a, member_b),
        )
        processor, lane = _processor(store)
        await processor._refresh_membership()
        await processor._refresh_membership()
        target = _NOW + datetime.timedelta(seconds=10)
        request = processor._select_request_position()

        await processor._settle_page(
            _page(
                _raw_call(
                    "00123-00045",
                    "https://audio/normal-on-override",
                    current.timestamp() + 1,
                ),
                _raw_call(
                    "00123-00046",
                    "https://audio/replay",
                    (_NOW - datetime.timedelta(minutes=1)).timestamp(),
                ),
                last_pos=target.timestamp(),
            ),
            request=request,
        )

        calls, boundaries, _ = lane.covers[0]
        self.assertEqual(
            [call.feed_id for call in calls],
            [_FEED_B, _FEED_A],
        )
        self.assertEqual(
            {boundary.feed_id for boundary in boundaries},
            {_FEED_A, _FEED_B},
        )
        self.assertEqual(processor._lease_cursor.pos, target)
        self.assertIs(
            processor._member_state(_FEED_B).route_state,
            sid_processor.MemberRouteState.NORMAL,
        )

    async def test_replay_no_progress_retains_override_and_cursor(self) -> None:
        current = _NOW - datetime.timedelta(seconds=30)
        member_a = _member(_FEED_A, "00123-00045", cursor=current)
        member_b = _member(
            _FEED_B,
            "00123-00046",
            cursor=_NOW - datetime.timedelta(minutes=2),
        )
        store = _ScriptedMembershipStore(
            _snapshot(4, member_a),
            _snapshot(5, member_a, member_b),
        )
        processor, lane = _processor(store)
        await processor._refresh_membership()
        await processor._refresh_membership()
        request = processor._select_request_position()
        before_cursor = processor._lease_cursor.pos

        await processor._settle_page(
            _page(
                _raw_call(
                    "00123-00046",
                    "https://audio/no-progress-replay",
                    (_NOW - datetime.timedelta(minutes=1)).timestamp(),
                ),
                last_pos="invalid",
            ),
            request=request,
        )

        calls, boundaries, _ = lane.covers[0]
        self.assertEqual([call.feed_id for call in calls], [_FEED_B])
        self.assertEqual(boundaries, ())
        self.assertEqual(processor._lease_cursor.pos, before_cursor)
        self.assertIs(
            processor._member_state(_FEED_B).route_state,
            sid_processor.MemberRouteState.REPLAY_PENDING,
        )
        self.assertEqual(
            processor._select_request_position().pos,
            request.pos,
        )

    async def test_replay_aborted_override_retains_state_and_unwinds_url(
        self,
    ) -> None:
        current = _NOW - datetime.timedelta(seconds=30)
        member_a = _member(_FEED_A, "00123-00045", cursor=current)
        member_b = _member(
            _FEED_B,
            "00123-00046",
            cursor=_NOW - datetime.timedelta(minutes=2),
        )
        lane = _RecordingLane(RuntimeError("override aborted"))
        store = _ScriptedMembershipStore(
            _snapshot(4, member_a),
            _snapshot(5, member_a, member_b),
        )
        processor, _ = _processor(store, lane=lane)
        await processor._refresh_membership()
        await processor._refresh_membership()

        with self.assertRaisesRegex(RuntimeError, "override aborted"):
            await processor._settle_page(
                _page(
                    _raw_call(
                        "00123-00046",
                        "https://audio/aborted-override",
                        (_NOW - datetime.timedelta(minutes=1)).timestamp(),
                    ),
                    last_pos=_NOW.timestamp(),
                ),
                request=processor._select_request_position(),
            )

        self.assertIs(
            processor._member_state(_FEED_B).route_state,
            sid_processor.MemberRouteState.REPLAY_PENDING,
        )
        self.assertNotIn(
            "https://audio/aborted-override",
            processor._pending_by_url,
        )

    async def test_adopt_null_sibling_on_valid_replay_override(self) -> None:
        current = _NOW - datetime.timedelta(seconds=30)
        member_a = _member(_FEED_A, "00123-00045", cursor=current)
        member_b = _member(
            _FEED_B,
            "00123-00046",
            cursor=_NOW - datetime.timedelta(minutes=2),
        )
        member_c = _member(_FEED_C, "00123-00047", cursor=None)
        store = _ScriptedMembershipStore(
            _snapshot(4, member_a),
            _snapshot(5, member_a, member_b, member_c),
        )
        processor, lane = _processor(store)
        await processor._refresh_membership()
        await processor._refresh_membership()
        target = _NOW + datetime.timedelta(seconds=1)

        await processor._settle_page(
            _page(
                _raw_call(
                    "00123-00047",
                    "https://audio/ignored-adoption",
                    target.timestamp(),
                ),
                last_pos=target.timestamp(),
            ),
            request=processor._select_request_position(),
        )

        calls, boundaries, _ = lane.covers[0]
        self.assertEqual(calls, ())
        self.assertEqual(
            {boundary.feed_id for boundary in boundaries},
            {_FEED_A, _FEED_B, _FEED_C},
        )
        self.assertIs(
            processor._member_state(_FEED_C).route_state,
            sid_processor.MemberRouteState.NORMAL,
        )

    async def test_last_pos_float_string_equal_uses_progress_path(self) -> None:
        cursor = _NOW - datetime.timedelta(seconds=30)
        store = _ScriptedMembershipStore(
            _snapshot(4, _member(_FEED_A, "00123-00045", cursor=cursor))
        )
        processor, lane = _processor(store)
        await processor._refresh_membership()

        await processor._settle_page(
            _page(last_pos=f"{cursor.timestamp():.1f}"),
            request=processor._select_request_position(),
        )

        _, boundaries, candidate = lane.covers[0]
        self.assertIsInstance(
            candidate,
            sid_processor.cursor_policy.PageCursorCandidate,
        )
        self.assertEqual([boundary.target for boundary in boundaries], [cursor])
        self.assertEqual(processor._lease_cursor.pos, cursor)

    async def test_no_progress_routes_calls_without_boundaries_or_cursor(
        self,
    ) -> None:
        cursor = _NOW - datetime.timedelta(seconds=30)
        store = _ScriptedMembershipStore(
            _snapshot(4, _member(_FEED_A, "00123-00045", cursor=cursor))
        )
        processor, lane = _processor(store)
        await processor._refresh_membership()

        await processor._settle_page(
            _page(
                _raw_call(
                    "00123-00045",
                    "https://audio/no-progress",
                    cursor.timestamp() + 1,
                ),
                last_pos=None,
            ),
            request=processor._select_request_position(),
        )

        calls, boundaries, candidate = lane.covers[0]
        self.assertEqual([call.feed_id for call in calls], [_FEED_A])
        self.assertEqual(boundaries, ())
        self.assertIsInstance(
            candidate,
            sid_processor.cursor_policy.NoProgressPageCandidate,
        )
        self.assertEqual(processor._lease_cursor.pos, cursor)
        self.assertEqual(processor._lease_cursor.next_page_sequence, 1)

    async def test_cadence_immediate_refresh_fetch_cover_wait_order(
        self,
    ) -> None:
        trace: list[str] = []
        stop_requested = asyncio.Event()
        store = _ScriptedMembershipStore(
            _snapshot(4, _member(_FEED_A, "00123-00045")),
            trace=trace,
        )
        calls_provider = _ScriptedProvider(
            _page(last_pos=_NOW.timestamp()),
            trace=trace,
        )
        lane = _RecordingLane(trace=trace)
        wait = _StoppingWait(stop_after=1, trace=trace)
        processor, _ = _processor(
            store,
            lane=lane,
            calls_provider=calls_provider,
            wait=wait,
        )

        await processor.run(stop_requested)

        self.assertEqual(trace, ["refresh", "fetch", "cover", "wait"])
        self.assertEqual(store.calls, [(_GRANT, None)])
        self.assertEqual(len(calls_provider.calls), 1)
        self.assertEqual(calls_provider.calls[0][0], "00123")
        self.assertEqual(calls_provider.calls[0][2], "00123")
        self.assertIs(calls_provider.calls[0][3], stop_requested)
        self.assertEqual(wait.calls, [(stop_requested, 10.0)])

    async def test_overlap_pressure_cover_blocks_fetch_and_cadence(
        self,
    ) -> None:
        cover_gate = _AsyncGate(None)
        stop_requested = asyncio.Event()
        store = _ScriptedMembershipStore(
            _snapshot(4, _member(_FEED_A, "00123-00045")),
            ingestion_lease_store.MembershipUnchanged(_GRANT, 4),
        )
        calls_provider = _ScriptedProvider(
            _page(last_pos=_NOW.timestamp()),
            _page(last_pos=(_NOW + datetime.timedelta(seconds=1)).timestamp()),
        )
        lane = _RecordingLane(cover_gate)
        wait = _StoppingWait(stop_after=1)
        processor, _ = _processor(
            store,
            lane=lane,
            calls_provider=calls_provider,
            wait=wait,
        )

        task = asyncio.create_task(processor.run(stop_requested))
        await cover_gate.entered.wait()
        self.assertEqual(len(store.calls), 1)
        self.assertEqual(len(calls_provider.calls), 1)
        self.assertEqual(wait.calls, [])
        self.assertEqual(processor._consecutive_failures, 0)

        cover_gate.release.set()
        await task

        self.assertEqual(len(calls_provider.calls), 1)
        self.assertEqual(wait.calls, [(stop_requested, 10.0)])

    async def test_pressure_is_local_to_each_exact_processor(self) -> None:
        pressure_gate = _AsyncGate(None)
        first_stop = asyncio.Event()
        second_stop = asyncio.Event()
        first_provider = _ScriptedProvider(_page(last_pos=_NOW.timestamp()))
        second_provider = _ScriptedProvider(_page(last_pos=_NOW.timestamp()))
        first, _ = _processor(
            _ScriptedMembershipStore(
                _snapshot(4, _member(_FEED_A, "00123-00045"))
            ),
            lane=_RecordingLane(pressure_gate),
            calls_provider=first_provider,
            wait=_StoppingWait(stop_after=1),
        )
        second, _ = _processor(
            _ScriptedMembershipStore(
                _snapshot(4, _member(_FEED_B, "00123-00046"))
            ),
            calls_provider=second_provider,
            wait=_StoppingWait(stop_after=1),
        )

        first_task = asyncio.create_task(first.run(first_stop))
        await pressure_gate.entered.wait()
        await second.run(second_stop)

        self.assertEqual(len(first_provider.calls), 1)
        self.assertEqual(len(second_provider.calls), 1)
        self.assertFalse(first_task.done())

        pressure_gate.release.set()
        await first_task

    async def test_pressure_lane_error_is_not_membership_retry_evidence(
        self,
    ) -> None:
        stop_requested = asyncio.Event()
        calls_provider = _ScriptedProvider(_page(last_pos=_NOW.timestamp()))
        wait = _RecordingWait()
        processor, _ = _processor(
            _ScriptedMembershipStore(
                _snapshot(4, _member(_FEED_A, "00123-00045"))
            ),
            lane=_RecordingLane(OSError("lane pressure failed")),
            calls_provider=calls_provider,
            wait=wait,
        )

        with self.assertRaisesRegex(OSError, "lane pressure failed"):
            await processor.run(stop_requested)

        self.assertEqual(len(calls_provider.calls), 1)
        self.assertEqual(wait.calls, [])
        self.assertEqual(processor._consecutive_failures, 0)

    async def test_pressure_removal_error_is_not_store_uncertainty(
        self,
    ) -> None:
        class _RemovalFailureLane(_RecordingLane):
            async def remove_feed(
                self,
                feed_id: uuid.UUID,
            ) -> feed_work_scheduler.FeedRemoved:
                del feed_id
                message = "lane removal failed"
                raise OSError(message)

        member_a = _member(_FEED_A, "00123-00045")
        member_b = _member(_FEED_B, "00123-00046")
        wait = _RecordingWait()
        processor, _ = _processor(
            _ScriptedMembershipStore(
                _snapshot(4, member_a),
                _snapshot(5, member_b),
            ),
            lane=_RemovalFailureLane(),
            calls_provider=_ScriptedProvider(_page(last_pos=_NOW.timestamp())),
            wait=wait,
        )

        with self.assertRaisesRegex(OSError, "lane removal failed"):
            await processor.run(asyncio.Event())

        self.assertEqual(len(wait.calls), 1)
        self.assertEqual(processor._consecutive_failures, 0)

    async def test_cadence_revision_during_fetch_applies_next_poll(
        self,
    ) -> None:
        fetch_gate = _AsyncGate(
            _page(
                _raw_call(
                    "00123-00046",
                    "https://audio/not-yet-a-member",
                    _NOW.timestamp() + 0.5,
                ),
                last_pos=_NOW.timestamp() + 1,
            )
        )
        member_a = _member(_FEED_A, "00123-00045")
        member_b = _member(_FEED_B, "00123-00046")
        store = _ScriptedMembershipStore(
            _snapshot(4, member_a),
            _snapshot(5, member_a, member_b),
        )
        calls_provider = _ScriptedProvider(
            fetch_gate,
            _page(
                _raw_call(
                    "00123-00046",
                    "https://audio/member-on-next-poll",
                    _NOW.timestamp() + 1.5,
                ),
                last_pos=_NOW.timestamp() + 2,
            ),
        )
        lane = _RecordingLane()
        wait = _StoppingWait(stop_after=2)
        stop_requested = asyncio.Event()
        processor, _ = _processor(
            store,
            lane=lane,
            calls_provider=calls_provider,
            wait=wait,
        )

        task = asyncio.create_task(processor.run(stop_requested))
        await fetch_gate.entered.wait()
        self.assertEqual(store.calls, [(_GRANT, None)])
        fetch_gate.release.set()
        await task

        self.assertEqual(store.calls, [(_GRANT, None), (_GRANT, 4)])
        self.assertEqual([len(calls) for calls, _, _ in lane.covers], [0, 1])
        self.assertEqual(lane.covers[1][0][0].feed_id, _FEED_B)
        self.assertEqual(len(calls_provider.calls), 2)

    async def test_cadence_success_resets_nine_logical_failures(self) -> None:
        stop_requested = asyncio.Event()
        wait = _StoppingWait(stop_after=1)
        processor, _ = _processor(
            _ScriptedMembershipStore(
                _snapshot(4, _member(_FEED_A, "00123-00045"))
            ),
            calls_provider=_ScriptedProvider(_page(last_pos=_NOW.timestamp())),
            wait=wait,
        )
        processor._consecutive_failures = 9

        await processor.run(stop_requested)

        self.assertEqual(processor._consecutive_failures, 0)
        self.assertEqual(wait.calls, [(stop_requested, 10.0)])

    async def test_cadence_tenth_transient_preserves_failure(self) -> None:
        stop_requested = asyncio.Event()
        calls_provider = _ScriptedProvider(
            models.FeedFailure(
                feed_store.FeedStatusReason.SOURCE_RATE_LIMITED,
                "provider rate limit",
            )
        )
        wait = _RecordingWait()
        processor, _ = _processor(
            _ScriptedMembershipStore(
                _snapshot(4, _member(_FEED_A, "00123-00045"))
            ),
            calls_provider=calls_provider,
            wait=wait,
        )
        processor._consecutive_failures = 9

        with self.assertRaises(sid_processor.SidProcessorFailure) as caught:
            await processor.run(stop_requested)

        self.assertIs(
            caught.exception.status_reason,
            feed_store.FeedStatusReason.SOURCE_RATE_LIMITED,
        )
        self.assertEqual(caught.exception.reason, "provider rate limit")
        self.assertEqual(wait.calls, [])

    async def test_cadence_authentication_failure_waits_below_limit(
        self,
    ) -> None:
        stop_requested = asyncio.Event()
        wait = _StoppingWait(stop_after=1)
        processor, _ = _processor(
            _ScriptedMembershipStore(
                _snapshot(4, _member(_FEED_A, "00123-00045"))
            ),
            calls_provider=_ScriptedProvider(
                models.FeedFailure(
                    feed_store.FeedStatusReason.SYSTEM_AUTHENTICATION_FAILED,
                    "provider authentication refresh failed",
                )
            ),
            wait=wait,
        )

        await processor.run(stop_requested)

        self.assertEqual(processor._consecutive_failures, 1)
        self.assertEqual(wait.calls, [(stop_requested, 10.0)])

    async def test_cadence_nontransient_provider_failure_is_immediate(
        self,
    ) -> None:
        wait = _RecordingWait()
        processor, _ = _processor(
            _ScriptedMembershipStore(
                _snapshot(4, _member(_FEED_A, "00123-00045"))
            ),
            calls_provider=_ScriptedProvider(
                models.FeedFailure(
                    feed_store.FeedStatusReason.SYSTEM_SOURCE_PAYLOAD_INVALID,
                    "invalid provider envelope",
                )
            ),
            wait=wait,
        )

        with self.assertRaises(sid_processor.SidProcessorFailure) as caught:
            await processor.run(asyncio.Event())

        self.assertIs(
            caught.exception.status_reason,
            feed_store.FeedStatusReason.SYSTEM_SOURCE_PAYLOAD_INVALID,
        )
        self.assertEqual(caught.exception.reason, "invalid provider envelope")
        self.assertEqual(wait.calls, [])

    async def test_membership_invariant_failure_preserves_typed_terminal(
        self,
    ) -> None:
        calls_provider = _ScriptedProvider()
        wait = _RecordingWait()
        processor, _ = _processor(
            _ScriptedMembershipStore(
                ingestion_lease_store.MembershipInvariantViolation(
                    _GRANT,
                    ingestion_lease_store.MembershipInvariantReason.DUPLICATE_ROUTING_KEY,
                    "duplicate group route",
                )
            ),
            calls_provider=calls_provider,
            wait=wait,
        )

        with self.assertRaises(sid_processor.SidProcessorFailure) as caught:
            await processor.run(asyncio.Event())

        self.assertIs(
            caught.exception.status_reason,
            feed_store.FeedStatusReason.SYSTEM_SOURCE_CONFIGURATION_INVALID,
        )
        self.assertEqual(
            caught.exception.reason,
            "bcfy_calls_sid_membership_invalid",
        )
        self.assertEqual(calls_provider.calls, [])
        self.assertEqual(wait.calls, [])

    async def test_cadence_membership_uncertainty_skips_fetch_then_recovers(
        self,
    ) -> None:
        stop_requested = asyncio.Event()
        calls_provider = _ScriptedProvider(_page(last_pos=_NOW.timestamp()))
        wait = _StoppingWait(stop_after=2)
        store = _ScriptedMembershipStore(
            TimeoutError("membership timed out"),
            _snapshot(4, _member(_FEED_A, "00123-00045")),
        )
        processor, _ = _processor(
            store,
            calls_provider=calls_provider,
            wait=wait,
        )

        await processor.run(stop_requested)

        self.assertEqual(store.calls, [(_GRANT, None), (_GRANT, None)])
        self.assertEqual(len(calls_provider.calls), 1)
        self.assertEqual(wait.calls, [(stop_requested, 10.0)] * 2)
        self.assertEqual(processor._consecutive_failures, 0)

    async def test_cadence_grant_rejection_has_no_fetch_or_wait(self) -> None:
        calls_provider = _ScriptedProvider()
        wait = _RecordingWait()
        rejection = ingestion_lease_store.GrantRejected(
            ingestion_lease_store.GrantRejectionReason.MISSING,
        )
        processor, _ = _processor(
            _ScriptedMembershipStore(rejection),
            calls_provider=calls_provider,
            wait=wait,
        )

        with self.assertRaises(sid_processor.SidProcessorAuthorityLost):
            await processor.run(asyncio.Event())

        self.assertEqual(calls_provider.calls, [])
        self.assertEqual(wait.calls, [])

    async def test_cadence_reactivation_planned_drain_precedes_fetch(
        self,
    ) -> None:
        member_a = _member(_FEED_A, "00123-00045")
        member_b = _member(_FEED_B, "00123-00046")
        calls_provider = _ScriptedProvider(
            _page(last_pos=_NOW.timestamp()),
            _page(last_pos=_NOW.timestamp()),
        )
        store = _ScriptedMembershipStore(
            _snapshot(4, member_a),
            _snapshot(5, member_b),
            _snapshot(6, member_a, member_b),
        )
        processor, _ = _processor(
            store,
            calls_provider=calls_provider,
            wait=_RecordingWait(),
        )

        with self.assertRaises(sid_processor.SidProcessorPlannedDrain):
            await processor.run(asyncio.Event())

        self.assertEqual(len(store.calls), 3)
        self.assertEqual(len(calls_provider.calls), 2)

    async def test_stop_before_refresh_and_after_blocked_refresh(self) -> None:
        stop_requested = asyncio.Event()
        stop_requested.set()
        store = _ScriptedMembershipStore()
        calls_provider = _ScriptedProvider()
        processor, _ = _processor(store, calls_provider=calls_provider)

        await processor.run(stop_requested)

        self.assertEqual(store.calls, [])
        self.assertEqual(calls_provider.calls, [])

        stop_requested = asyncio.Event()
        refresh_gate = _AsyncGate(_snapshot(4, _member(_FEED_A, "00123-00045")))
        store = _ScriptedMembershipStore(refresh_gate)
        processor, _ = _processor(store, calls_provider=calls_provider)
        task = asyncio.create_task(processor.run(stop_requested))
        await refresh_gate.entered.wait()
        stop_requested.set()
        refresh_gate.release.set()
        await task

        self.assertEqual(len(store.calls), 1)
        self.assertEqual(calls_provider.calls, [])

    async def test_candidate_first_cohort_grouping_and_atomic_admission(
        self,
    ) -> None:
        sessions = []

        def session_id() -> str:
            sessions.append("stable-session")
            return sessions[-1]

        processor, _ = _processor(
            _ScriptedMembershipStore(
                _snapshot(
                    4,
                    _member(
                        _FEED_A,
                        "00123-00045",
                        cursor=datetime.datetime.fromtimestamp(
                            0,
                            datetime.UTC,
                        ),
                    ),
                )
            ),
            session_id_factory=session_id,
        )
        await processor._refresh_membership()
        assert processor._lease_cursor is not None
        candidate = processor._lease_cursor.prepare(_NOW)

        prepared = processor._prepare_page_work(
            [
                _raw_call("00123-00045", "https://audio/t10-a", 10),
                _raw_call("00123-00045", "https://audio/missing-a", ...),
                _raw_call("00123-00045", "https://audio/t10-b", 10),
                _raw_call("00123-00045", "https://audio/missing-b", ...),
                _raw_call("00123-00045", "https://audio/t20", 20),
                _raw_call("00123-00045", "https://audio/t10-b", 10),
            ],
            candidate=candidate,
        )

        self.assertEqual(prepared.ledger.page_sequence, candidate.page_sequence)
        self.assertEqual(
            [len(cohort.calls) for cohort in prepared.cohorts], [1, 1, 2, 1]
        )
        self.assertEqual(
            [cohort.cohort_timestamp for cohort in prepared.cohorts],
            [
                None,
                None,
                datetime.datetime.fromtimestamp(10, datetime.UTC),
                datetime.datetime.fromtimestamp(20, datetime.UTC),
            ],
        )
        self.assertEqual(
            [entry.source_order for entry in prepared.entries],
            [0, 1, 2, 3, 4],
        )
        self.assertEqual(
            [entry.audio_url for entry in prepared.entries],
            [
                "https://audio/missing-a",
                "https://audio/missing-b",
                "https://audio/t10-a",
                "https://audio/t10-b",
                "https://audio/t20",
            ],
        )
        self.assertEqual(sessions, ["stable-session"])
        self.assertEqual(
            {
                typing.cast(
                    "cohort_pipeline.ScheduledCohortPayload",
                    cohort.calls[0].payload,
                ).session_id
                for cohort in prepared.cohorts
            },
            {"stable-session"},
        )
        self.assertEqual(processor._pending_by_url, {})

        cohort = prepared.cohorts[2]
        identities = tuple(
            feed_work_scheduler.CohortRecordIdentity(
                grant=_GRANT,
                member=cohort.member,
                page_sequence=candidate.page_sequence,
                feed_id=cohort.feed_id,
                cohort_timestamp=cohort.cohort_timestamp,
                source_order=2 + offset,
                local_sequence=offset,
            )
            for offset, _call in enumerate(cohort.calls)
        )
        processor._pending_by_url["https://audio/t10-b"] = _FEED_A
        with self.assertRaises(feed_work_scheduler.CohortIntegrityError):
            cohort.admission_hook(identities)
        payload = typing.cast(
            "cohort_pipeline.ScheduledCohortPayload",
            cohort.calls[0].payload,
        )
        self.assertTrue(
            all(
                prepared.ledger.state(entry.obligation)
                is sid_processor.gap_ledger.MissingCallState.DRAFT
                for entry in payload.entries
            )
        )
        processor._pending_by_url.clear()

        cohort.admission_hook(identities)
        self.assertEqual(
            processor._pending_by_url,
            {
                "https://audio/t10-a": _FEED_A,
                "https://audio/t10-b": _FEED_A,
            },
        )
        for call in cohort.calls:
            assert call.settlement_observer is not None
            call.settlement_observer(
                feed_work_scheduler.CallSettlement.RETRYABLE
            )
        self.assertEqual(processor._pending_by_url, {})

    async def test_second_pending_insert_rolls_back_before_ledger_admission(
        self,
    ) -> None:
        processor, _ = _processor(
            _ScriptedMembershipStore(
                _snapshot(4, _member(_FEED_A, "00123-00045"))
            )
        )
        await processor._refresh_membership()
        assert processor._lease_cursor is not None
        candidate = processor._lease_cursor.prepare(
            _NOW + datetime.timedelta(seconds=30)
        )
        prepared = processor._prepare_page_work(
            (
                _raw_call(
                    "00123-00045",
                    "https://audio/first",
                    _NOW.timestamp() + 1,
                ),
                _raw_call(
                    "00123-00045",
                    "https://audio/second",
                    _NOW.timestamp() + 1,
                ),
            ),
            candidate=candidate,
        )
        pending = _SecondPendingInsertFails()
        processor._pending_by_url = pending
        scheduler = feed_work_scheduler.FeedWorkScheduler(_UnexpectedExecutor())
        await scheduler.start()
        lane = scheduler.open_lane(
            _GRANT,
            stop_requested=asyncio.Event(),
            grant_lost=asyncio.Event(),
        )
        try:
            with self.assertRaisesRegex(
                RuntimeError,
                "second pending insert failed",
            ):
                await lane.cover_page(
                    calls=prepared.cohorts,
                    boundaries=(),
                    candidate=candidate,
                )

            self.assertEqual(pending, {})
            self.assertEqual(pending.insertion_count, 2)
            self.assertTrue(
                all(
                    prepared.ledger.state(entry.obligation)
                    is sid_processor.gap_ledger.MissingCallState.DRAFT
                    for entry in prepared.entries
                )
            )
            self.assertEqual((await scheduler._snapshot()).held, 0)
            lane_snapshot = await lane._snapshot()
            self.assertEqual(lane_snapshot.next_page_sequence, 0)
            self.assertIsNone(lane_snapshot.page)
        finally:
            await scheduler.close()

    async def test_ledger_rejection_rolls_back_all_staged_pending_urls(
        self,
    ) -> None:
        processor, _ = _processor(
            _ScriptedMembershipStore(
                _snapshot(4, _member(_FEED_A, "00123-00045"))
            )
        )
        await processor._refresh_membership()
        assert processor._lease_cursor is not None
        candidate = processor._lease_cursor.prepare(
            _NOW + datetime.timedelta(seconds=30)
        )
        prepared = processor._prepare_page_work(
            (
                _raw_call(
                    "00123-00045",
                    "https://audio/released",
                    _NOW.timestamp() + 1,
                ),
            ),
            candidate=candidate,
        )
        call = prepared.cohorts[0].calls[0]
        assert call.settlement_observer is not None
        call.settlement_observer(feed_work_scheduler.CallSettlement.RETRYABLE)
        scheduler = feed_work_scheduler.FeedWorkScheduler(_UnexpectedExecutor())
        await scheduler.start()
        lane = scheduler.open_lane(
            _GRANT,
            stop_requested=asyncio.Event(),
            grant_lost=asyncio.Event(),
        )
        try:
            with self.assertRaises(
                sid_processor.gap_ledger.MissingCallIntegrityError
            ):
                await lane.cover_page(
                    calls=prepared.cohorts,
                    boundaries=(),
                    candidate=candidate,
                )

            self.assertEqual(processor._pending_by_url, {})
            self.assertIs(
                prepared.ledger.state(prepared.entries[0].obligation),
                sid_processor.gap_ledger.MissingCallState.REPLAY_RELEASED,
            )
            self.assertEqual((await scheduler._snapshot()).held, 0)
            lane_snapshot = await lane._snapshot()
            self.assertEqual(lane_snapshot.next_page_sequence, 0)
            self.assertIsNone(lane_snapshot.page)
        finally:
            await scheduler.close()

    async def test_stable_session_collision_fails_before_lane_mutation(
        self,
    ) -> None:
        factory_calls = 0

        def collide() -> str:
            nonlocal factory_calls
            factory_calls += 1
            return "same-session"

        processor, lane = _processor(
            _ScriptedMembershipStore(
                _snapshot(
                    4,
                    _member(_FEED_A, "00123-00045"),
                    _member(_FEED_B, "00123-00046"),
                )
            ),
            session_id_factory=collide,
        )
        await processor._refresh_membership()
        assert processor._lease_cursor is not None
        candidate = processor._lease_cursor.prepare(_NOW)

        with self.assertRaises(feed_work_scheduler.CohortIntegrityError):
            processor._prepare_page_work(
                [
                    _raw_call("00123-00045", "https://audio/a"),
                    _raw_call("00123-00046", "https://audio/b"),
                ],
                candidate=candidate,
            )

        self.assertEqual(factory_calls, 2)
        self.assertEqual(lane.covers, [])
        self.assertEqual(processor._pending_by_url, {})

    async def test_replayable_page_consumes_sequence_without_moving_pos(
        self,
    ) -> None:
        initial = _NOW - datetime.timedelta(seconds=30)
        lane = _RecordingLane(_replayable_settled_page)
        processor, _ = _processor(
            _ScriptedMembershipStore(
                _snapshot(
                    4,
                    _member(_FEED_A, "00123-00045", cursor=initial),
                )
            ),
            lane=lane,
        )
        await processor._refresh_membership()

        await processor._settle_page(
            _page(
                _raw_call(
                    "00123-00045",
                    "https://audio/replay",
                    initial.timestamp() + 1,
                ),
                last_pos=_NOW.timestamp(),
            ),
            request=processor._select_request_position(),
        )

        assert processor._lease_cursor is not None
        self.assertEqual(processor._lease_cursor.pos, initial)
        self.assertEqual(processor._lease_cursor.next_page_sequence, 1)
        self.assertIsNone(processor._lease_cursor.outstanding_candidate)
        self.assertEqual(processor._select_request_position().pos, initial)
        self.assertEqual(processor._pending_by_url, {})
        self.assertEqual(processor._recent_urls, set())
        self.assertEqual(len(lane.covers), 1)

    async def test_replayable_candidate_aging_uses_overload_floor_once(
        self,
    ) -> None:
        initial = _NOW - datetime.timedelta(seconds=30)
        observed_now = [_NOW]
        emitted: list[telemetry.ReplayWindowTruncatedEvent] = []
        lane = _RecordingLane(_replayable_settled_page)
        processor, _ = _processor(
            _ScriptedMembershipStore(
                _snapshot(
                    4,
                    _member(_FEED_A, "00123-00045", cursor=initial),
                )
            ),
            lane=lane,
            now=lambda: observed_now[0],
            replay_floor_emitter=emitted.append,
        )
        await processor._refresh_membership()
        first_request = processor._select_request_position()
        self.assertEqual(emitted, [])

        await processor._settle_page(
            _page(last_pos=_NOW.timestamp()),
            request=first_request,
        )
        observed_now[0] = _NOW + datetime.timedelta(minutes=10)

        overload_request = processor._select_request_position()

        self.assertEqual(
            overload_request.pos,
            _NOW + datetime.timedelta(minutes=5),
        )
        self.assertEqual(len(emitted), 1)
        evidence = emitted[0].floor_reached
        self.assertIs(
            evidence.cause,
            cursor_policy.ReplayFloorCause.OVERLOAD,
        )
        self.assertEqual(evidence.requested_start, initial)
        self.assertEqual(evidence.lost_duration_seconds, 330.0)
        assert processor._lease_cursor is not None
        self.assertEqual(processor._lease_cursor.pos, initial)
        self.assertEqual(processor._lease_cursor.next_page_sequence, 1)

    async def test_real_scheduler_replay_barrier_blocks_t2_and_settles_page(
        self,
    ) -> None:
        member_a = _member(_FEED_A, "00123-00045")
        member_b = _member(_FEED_B, "00123-00046")
        membership_store = _ScriptedMembershipStore(
            _snapshot(4, member_a, member_b)
        )
        executor = _ProcessorReplayBarrierExecutor(_FEED_A)
        finalizer = _ProcessorReplayPageFinalizer()
        scheduler = feed_work_scheduler.FeedWorkScheduler(
            executor,
            page_finalizer=finalizer,
        )
        await scheduler.start()
        lane = scheduler.open_lane(
            _GRANT,
            stop_requested=asyncio.Event(),
            grant_lost=asyncio.Event(),
        )
        processor = sid_processor.BcfyCallsSidProcessor(
            _GRANT,
            _claim_payload(),
            membership_store,
            _ScriptedProvider(),
            lane,
            now=lambda: _NOW,
        )
        await processor._refresh_membership()
        target = _NOW + datetime.timedelta(seconds=30)
        settlement = asyncio.create_task(
            processor._settle_page(
                _page(
                    _raw_call(
                        "00123-00045",
                        "https://audio/a-t1",
                        _NOW.timestamp() + 1,
                    ),
                    _raw_call(
                        "00123-00045",
                        "https://audio/a-t2",
                        _NOW.timestamp() + 2,
                    ),
                    _raw_call(
                        "00123-00046",
                        "https://audio/b-t1",
                        _NOW.timestamp() + 1,
                    ),
                    last_pos=target.timestamp(),
                ),
                request=processor._select_request_position(),
            )
        )
        try:
            await asyncio.wait_for(executor.first_entered.wait(), timeout=1)
            await asyncio.wait_for(
                executor.sibling_completed.wait(),
                timeout=1,
            )
            self.assertIn((0,), executor.started)
            self.assertIn((1,), executor.started)
            self.assertNotIn((2,), executor.started)
            executor.release_first.set()
            await asyncio.wait_for(settlement, timeout=1)

            self.assertNotIn((2,), executor.started)
            self.assertEqual(len(finalizer.contexts), 1)
            context = finalizer.contexts[0]
            self.assertEqual(
                context.unresolved_replay_feed_ids,
                (_FEED_A,),
            )
            self.assertEqual(context.replay_blocked_feed_ids, (_FEED_A,))
            assert processor._lease_cursor is not None
            self.assertEqual(processor._lease_cursor.pos, _NOW)
            self.assertEqual(processor._lease_cursor.next_page_sequence, 1)
            self.assertEqual(processor._select_request_position().pos, _NOW)
            self.assertEqual(processor._pending_by_url, {})
            self.assertNotIn("https://audio/a-t1", processor._recent_urls)
            self.assertNotIn("https://audio/a-t2", processor._recent_urls)
            self.assertIn("https://audio/b-t1", processor._recent_urls)
            self.assertEqual((await scheduler._snapshot()).held, 0)
        finally:
            executor.release_first.set()
            if not settlement.done():
                settlement.cancel()
                await asyncio.gather(settlement, return_exceptions=True)
            await scheduler.close()

    async def test_no_progress_nonboundary_rejection_tombstones_exact_feed(
        self,
    ) -> None:
        member_a = _member(_FEED_A, "00123-00045")
        member_b = _member(_FEED_B, "00123-00046")
        membership_store = _ScriptedMembershipStore(
            _snapshot(4, member_a, member_b),
            ingestion_lease_store.MembershipUnchanged(_GRANT, 4),
        )
        store = mock.create_autospec(
            ingestion_lease_store.IngestionLeaseStore,
            instance=True,
        )
        store.commit_child_mutations = mock.AsyncMock(
            return_value=_batch_committed(
                ingestion_lease_store.ChildMutationResult(
                    feed_id=_FEED_A,
                    disposition=(
                        ingestion_lease_store.ChildDisposition.STATUS_INELIGIBLE
                    ),
                    cursor_effect=ingestion_lease_store.CursorEffect.ABSENT,
                    lifecycle_effect=ingestion_lease_store.LifecycleEffect.NONE,
                )
            )
        )
        finalizer = runtime_adapters.FencedPageFinalizer(
            store,
            actor_id="service_account:gcp:sid-processor-tests",
            budgeted_failure=failure_policy.ConsumeFailureBudget(7, 15, 600),
            boundary_settled_utc=lambda: _NOW,
        )
        scheduler = feed_work_scheduler.FeedWorkScheduler(
            _SuccessfulExecutor(),
            page_finalizer=finalizer,
        )
        await scheduler.start()
        lane = scheduler.open_lane(
            _GRANT,
            stop_requested=asyncio.Event(),
            grant_lost=asyncio.Event(),
        )
        processor = sid_processor.BcfyCallsSidProcessor(
            _GRANT,
            _claim_payload(),
            membership_store,
            _ScriptedProvider(),
            lane,
            now=lambda: _NOW,
        )
        await processor._refresh_membership()
        try:
            with mock.patch.object(
                sid_processor.gap_ledger.telemetry,
                "emit_missing_call",
            ) as emit_missing_call:
                await processor._settle_page(
                    _page(
                        _raw_call(
                            "00123-00045",
                            "https://audio/no-progress",
                            ...,
                        ),
                        last_pos=None,
                    ),
                    request=processor._select_request_position(),
                )

            emit_missing_call.assert_not_called()
            self.assertTrue(await lane.is_feed_retired(_FEED_A))
            self.assertFalse(await lane.is_feed_retired(_FEED_B))
            self.assertEqual(set(processor._members_by_id), {_FEED_B})
            self.assertEqual(
                set(processor._members_by_source),
                {"00123-00046"},
            )
            self.assertIn(_FEED_A, processor._retired_feed_ids)
            assert processor._lease_cursor is not None
            self.assertEqual(processor._lease_cursor.pos, _NOW)
            self.assertEqual(processor._lease_cursor.next_page_sequence, 1)
            commit_call = store.commit_child_mutations.await_args
            assert commit_call is not None
            batch = commit_call.args[1]
            self.assertEqual(
                batch.mutations,
                (
                    ingestion_lease_store.SourceObservation(
                        member_a.identity, None
                    ),
                ),
            )

            await processor._refresh_membership()
            prepared = _prepared_work(
                processor,
                (
                    _raw_call(
                        "00123-00045",
                        "https://audio/retired-next-poll",
                        _NOW.timestamp() + 1,
                    ),
                ),
            )
            self.assertEqual(prepared.cohorts, ())
            self.assertEqual(processor._pending_by_url, {})
            self.assertEqual((await scheduler._snapshot()).held, 0)
        finally:
            await scheduler.close()

    async def test_no_progress_pending_skip_member_rejection_drains_page(
        self,
    ) -> None:
        member = _member(_FEED_A, "00123-00045")
        store = mock.create_autospec(
            ingestion_lease_store.IngestionLeaseStore,
            instance=True,
        )
        store.commit_child_mutations = mock.AsyncMock(
            return_value=_batch_committed(
                ingestion_lease_store.ChildMutationResult(
                    feed_id=_FEED_A,
                    disposition=(
                        ingestion_lease_store.ChildDisposition.STATUS_INELIGIBLE
                    ),
                    cursor_effect=ingestion_lease_store.CursorEffect.ABSENT,
                    lifecycle_effect=ingestion_lease_store.LifecycleEffect.NONE,
                )
            )
        )
        finalizer = runtime_adapters.FencedPageFinalizer(
            store,
            actor_id="service_account:gcp:sid-processor-tests",
            budgeted_failure=failure_policy.ConsumeFailureBudget(7, 15, 600),
            boundary_settled_utc=lambda: _NOW,
        )
        scheduler = feed_work_scheduler.FeedWorkScheduler(
            _FinalPendingSkipExecutor(),
            page_finalizer=finalizer,
        )
        await scheduler.start()
        lane = scheduler.open_lane(
            _GRANT,
            stop_requested=asyncio.Event(),
            grant_lost=asyncio.Event(),
        )
        processor = sid_processor.BcfyCallsSidProcessor(
            _GRANT,
            _claim_payload(),
            _ScriptedMembershipStore(_snapshot(4, member)),
            _ScriptedProvider(),
            lane,
            now=lambda: _NOW,
        )
        await processor._refresh_membership()
        try:
            with mock.patch.object(
                sid_processor.gap_ledger.telemetry,
                "emit_missing_call",
            ) as emit_missing_call:
                await processor._settle_page(
                    _page(
                        _raw_call(
                            "00123-00045",
                            "https://audio/no-progress-skip",
                            ...,
                        ),
                        last_pos=None,
                    ),
                    request=processor._select_request_position(),
                )

            emit_missing_call.assert_not_called()
            self.assertTrue(await lane.is_feed_retired(_FEED_A))
            self.assertEqual(processor._members_by_id, {})
            self.assertEqual(processor._pending_by_url, {})
            self.assertIsNone(processor._active_page_ledger)
            assert processor._lease_cursor is not None
            self.assertEqual(processor._lease_cursor.pos, _NOW)
            self.assertEqual(processor._lease_cursor.next_page_sequence, 1)
            self.assertEqual((await scheduler._snapshot()).held, 0)
        finally:
            await scheduler.close()

    async def test_undrained_page_retains_candidate_and_is_not_resubmitted(
        self,
    ) -> None:
        lane = _RecordingLane(
            feed_work_scheduler.Undrained(
                _GRANT,
                feed_work_scheduler.LaneCloseReason.PLANNED_DRAIN,
            )
        )
        processor, _ = _processor(
            _ScriptedMembershipStore(
                _snapshot(4, _member(_FEED_A, "00123-00045"))
            ),
            lane=lane,
        )
        await processor._refresh_membership()

        with self.assertRaises(sid_processor.SidProcessorUndrained):
            await processor._settle_page(
                _page(
                    _raw_call("00123-00045", "https://audio/unknown"),
                    last_pos=(_NOW + datetime.timedelta(seconds=1)).timestamp(),
                ),
                request=processor._select_request_position(),
            )

        assert processor._lease_cursor is not None
        self.assertIsNotNone(processor._lease_cursor.outstanding_candidate)
        self.assertIsNotNone(processor._active_page_ledger)
        self.assertEqual(len(lane.covers), 1)
        with self.assertRaises(cursor_policy.CursorIntegrityError):
            await processor._settle_page(
                _page(
                    last_pos=(_NOW + datetime.timedelta(seconds=2)).timestamp()
                ),
                request=processor._select_request_position(),
            )
        self.assertEqual(len(lane.covers), 1)

    async def test_quarantine_tombstone_survives_unchanged_membership(
        self,
    ) -> None:
        member = _member(_FEED_A, "00123-00045")
        lane = _RecordingLane(_quarantined_settled_page)
        processor, _ = _processor(
            _ScriptedMembershipStore(
                _snapshot(4, member),
                ingestion_lease_store.MembershipUnchanged(_GRANT, 4),
                _snapshot(5, member),
            ),
            lane=lane,
        )
        await processor._refresh_membership()
        await processor._settle_page(
            _page(last_pos=(_NOW + datetime.timedelta(seconds=1)).timestamp()),
            request=processor._select_request_position(),
        )

        self.assertEqual(lane.removed, [_FEED_A])
        self.assertEqual(processor._members_by_id, {})
        self.assertIn(_FEED_A, processor._retired_feed_ids)
        await processor._refresh_membership()
        self.assertEqual(processor._members_by_id, {})
        with self.assertRaises(sid_processor.SidProcessorPlannedDrain):
            await processor._refresh_membership()

    async def test_unanimous_page_failure_promotes_only_after_page_close(
        self,
    ) -> None:
        lane = _RecordingLane(_promoted_settled_page)
        processor, _ = _processor(
            _ScriptedMembershipStore(
                _snapshot(
                    4,
                    _member(_FEED_A, "00123-00045"),
                    _member(_FEED_B, "00123-00046"),
                )
            ),
            lane=lane,
        )
        await processor._refresh_membership()
        target = _NOW + datetime.timedelta(seconds=1)

        with self.assertRaises(sid_processor.SidProcessorFailure) as raised:
            await processor._settle_page(
                _page(
                    _raw_call("00123-00045", "https://audio/a"),
                    _raw_call("00123-00046", "https://audio/b"),
                    last_pos=target.timestamp(),
                ),
                request=processor._select_request_position(),
            )

        self.assertIs(
            raised.exception.status_reason,
            feed_store.FeedStatusReason.SYSTEM_SOURCE_PAYLOAD_INVALID,
        )
        self.assertEqual(raised.exception.reason, "terminal item skip")
        assert processor._lease_cursor is not None
        self.assertEqual(processor._lease_cursor.pos, target)
        self.assertIsNone(processor._active_page_ledger)
        self.assertEqual(processor._pending_by_url, {})

    async def test_stop_cancellation_during_fetch_and_cover_is_preserved(
        self,
    ) -> None:
        for stage in ("fetch", "cover"):
            with self.subTest(stage=stage):
                stop_requested = asyncio.Event()
                cancellation_gate = _AsyncGate(asyncio.CancelledError())
                calls_provider = _ScriptedProvider(
                    cancellation_gate
                    if stage == "fetch"
                    else _page(last_pos=_NOW.timestamp())
                )
                lane = _RecordingLane(
                    cancellation_gate if stage == "cover" else None
                )
                if stage == "fetch":
                    lane = _RecordingLane()
                processor, _ = _processor(
                    _ScriptedMembershipStore(
                        _snapshot(4, _member(_FEED_A, "00123-00045"))
                    ),
                    lane=lane,
                    calls_provider=calls_provider,
                )
                task = asyncio.create_task(processor.run(stop_requested))
                await cancellation_gate.entered.wait()
                stop_requested.set()
                cancellation_gate.release.set()

                with self.assertRaises(asyncio.CancelledError):
                    await task

                self.assertEqual(processor._pending_by_url, {})

    async def test_stop_wait_cancels_promptly_and_clears_local_state(
        self,
    ) -> None:
        stop_requested = asyncio.Event()
        wait = _BlockingWait()
        processor, _ = _processor(
            _ScriptedMembershipStore(
                _snapshot(4, _member(_FEED_A, "00123-00045"))
            ),
            calls_provider=_ScriptedProvider(_page(last_pos=_NOW.timestamp())),
            wait=wait,
        )

        task = asyncio.create_task(processor.run(stop_requested))
        await wait.entered.wait()
        stop_requested.set()
        with self.assertRaises(asyncio.CancelledError):
            await task

        self.assertEqual(processor._members_by_id, {})
        self.assertEqual(processor._pending_by_url, {})
        self.assertIsNone(processor._snapshot)

    async def test_cadence_source_has_no_gate_ticker_or_runtime_ownership(
        self,
    ) -> None:
        run_source = inspect.getsource(sid_processor.BcfyCallsSidProcessor.run)
        module_source = inspect.getsource(sid_processor)

        for forbidden in (
            "5.0",
            "monotonic",
            "deadline",
            "create_task",
            "set_retrying",
            "last_attempt",
            "missed_tick",
        ):
            self.assertNotIn(forbidden, run_source)
        for forbidden in (
            "ClientSession",
            ".claim(",
            ".heartbeat(",
            ".finalize(",
            "pubsub",
            "gcs",
        ):
            self.assertNotIn(forbidden, module_source)


class TestSidPollTelemetryWiring(unittest.IsolatedAsyncioTestCase):
    """Exercise one emitted event at each logical-poll owner boundary."""

    async def test_sid_poll_success_uses_actual_owner_evidence(self) -> None:
        events: list[telemetry.SidPollEvidence] = []
        stop_requested = asyncio.Event()
        processor, _ = _processor(
            _ScriptedMembershipStore(
                _snapshot(4, _member(_FEED_A, "00123-00045"))
            ),
            calls_provider=_ScriptedProvider(_page(last_pos=_NOW.timestamp())),
            wait=_StoppingWait(stop_after=1),
            poll_emitter=events.append,
        )

        await processor.run(stop_requested)

        self.assertEqual(len(events), 1)
        event = events[0]
        self.assertIs(event.outcome, telemetry.SidPollOutcome.SUCCESS)
        self.assertTrue(event.provider_observed)
        self.assertEqual(event.http_attempt_count, 1)
        self.assertEqual(event.response_row_count, 0)
        self.assertEqual(event.response_distinct_audio_url_count, 0)
        self.assertEqual(
            event.response_last_pos_state,
            telemetry.SidPollResponseLastPosState.VALID,
        )
        self.assertEqual(event.membership_revision, 4)
        self.assertEqual(event.oldest_queue_age_seconds, 0.0)

    async def test_sid_poll_membership_uncertainty_emits_before_provider(
        self,
    ) -> None:
        events: list[telemetry.SidPollEvidence] = []
        calls_provider = _ScriptedProvider()
        processor, _ = _processor(
            _ScriptedMembershipStore(TimeoutError("membership timed out")),
            calls_provider=calls_provider,
            wait=_StoppingWait(stop_after=1),
            poll_emitter=events.append,
        )

        await processor.run(asyncio.Event())

        self.assertEqual(len(events), 1)
        event = events[0]
        self.assertIs(
            event.outcome,
            telemetry.SidPollOutcome.MEMBERSHIP_UNCERTAIN,
        )
        self.assertFalse(event.provider_observed)
        self.assertEqual(event.http_attempt_count, 0)
        self.assertIsNone(event.response_row_count)
        self.assertIsNone(event.response_byte_count)
        self.assertIsNone(event.response_distinct_audio_url_count)
        self.assertIsNone(event.response_last_pos)
        self.assertEqual(calls_provider.calls, [])

    async def test_sid_poll_membership_invalid_emits_once_without_provider(
        self,
    ) -> None:
        events: list[telemetry.SidPollEvidence] = []
        calls_provider = _ScriptedProvider()
        processor, _ = _processor(
            _ScriptedMembershipStore(
                ingestion_lease_store.MembershipInvariantViolation(
                    _GRANT,
                    ingestion_lease_store.MembershipInvariantReason.DUPLICATE_ROUTING_KEY,
                    "duplicate group route",
                )
            ),
            calls_provider=calls_provider,
            poll_emitter=events.append,
        )

        with self.assertRaises(sid_processor.SidProcessorFailure):
            await processor.run(asyncio.Event())

        self.assertEqual(len(events), 1)
        self.assertIs(
            events[0].outcome,
            telemetry.SidPollOutcome.MEMBERSHIP_INVALID,
        )
        self.assertFalse(events[0].provider_observed)
        self.assertEqual(events[0].http_attempt_count, 0)
        self.assertIsNone(events[0].response_distinct_audio_url_count)
        self.assertEqual(calls_provider.calls, [])

    async def test_sid_poll_provider_typed_failure_keeps_actual_attempt(
        self,
    ) -> None:
        events: list[telemetry.SidPollEvidence] = []
        processor, _ = _processor(
            _ScriptedMembershipStore(
                _snapshot(4, _member(_FEED_A, "00123-00045"))
            ),
            calls_provider=_ScriptedProvider(
                models.FeedFailure(
                    feed_store.FeedStatusReason.SYSTEM_SOURCE_PAYLOAD_INVALID,
                    "invalid provider envelope",
                )
            ),
            poll_emitter=events.append,
        )

        with self.assertRaises(sid_processor.SidProcessorFailure):
            await processor.run(asyncio.Event())

        self.assertEqual(len(events), 1)
        self.assertIs(
            events[0].outcome,
            telemetry.SidPollOutcome.PROVIDER_FAILED,
        )
        self.assertFalse(events[0].provider_observed)
        self.assertEqual(events[0].http_attempt_count, 1)
        self.assertIsNone(events[0].response_distinct_audio_url_count)

    async def test_sid_poll_blocked_settlement_emits_nothing_early(
        self,
    ) -> None:
        events: list[telemetry.SidPollEvidence] = []
        gate = _AsyncGate(None)
        lane = _RecordingLane(gate)
        processor, _ = _processor(
            _ScriptedMembershipStore(
                _snapshot(4, _member(_FEED_A, "00123-00045"))
            ),
            lane=lane,
            calls_provider=_ScriptedProvider(
                _page(
                    _raw_call("00123-00045", "https://audio/one"),
                    last_pos=_NOW.timestamp(),
                )
            ),
            wait=_StoppingWait(stop_after=1),
            poll_emitter=events.append,
        )
        task = asyncio.create_task(processor.run(asyncio.Event()))

        await gate.entered.wait()
        self.assertEqual(events, [])
        gate.release.set()
        await task

        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].admitted_record_count, 1)
        self.assertEqual(events[0].matched_call_count, 1)
        self.assertEqual(events[0].response_distinct_audio_url_count, 1)

    async def test_sid_poll_prestarted_stop_and_between_poll_stop_add_none(
        self,
    ) -> None:
        prestarted_events: list[telemetry.SidPollEvidence] = []
        already_stopped = asyncio.Event()
        already_stopped.set()
        processor, _ = _processor(
            _ScriptedMembershipStore(),
            poll_emitter=prestarted_events.append,
        )

        await processor.run(already_stopped)

        self.assertEqual(prestarted_events, [])

        between_events: list[telemetry.SidPollEvidence] = []
        wait = _BlockingWait()
        between_processor, _ = _processor(
            _ScriptedMembershipStore(
                _snapshot(4, _member(_FEED_A, "00123-00045"))
            ),
            calls_provider=_ScriptedProvider(_page(last_pos=_NOW.timestamp())),
            wait=wait,
            poll_emitter=between_events.append,
        )
        stop_requested = asyncio.Event()
        task = asyncio.create_task(between_processor.run(stop_requested))
        await wait.entered.wait()
        self.assertEqual(len(between_events), 1)
        stop_requested.set()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertEqual(len(between_events), 1)


if __name__ == "__main__":
    unittest.main()
