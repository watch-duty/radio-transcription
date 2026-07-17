"""One-attempt storage adapter tests for Broadcastify Calls cohorts."""

from __future__ import annotations

import asyncio
import dataclasses
import datetime
import inspect
import typing
import unittest
import uuid
from unittest import mock

import asyncpg

from backend.pipeline.ingestion import failure_policy, feed_work_scheduler
from backend.pipeline.ingestion.collectors.bcfy_calls import (
    boundary_verdict,
    cursor_policy,
    runtime_adapters,
)
from backend.pipeline.storage import feed_store, ingestion_lease_store

_ACTOR_ID = "service_account:gcp:collector"
_OWNER_ID = uuid.UUID("11111111-2222-3333-4444-555555555555")
_NOW = datetime.datetime(2026, 7, 12, 12, 0, tzinfo=datetime.UTC)


class _MockIngestionLeaseStore(ingestion_lease_store.IngestionLeaseStore):
    """Autospecced store whose awaited methods retain mock assertions."""

    commit_child_mutations: mock.AsyncMock
    load_membership: mock.AsyncMock
    refresh_membership: mock.AsyncMock


def _page_evidence(
    result: (
        feed_work_scheduler.FinalPageCovered
        | feed_work_scheduler.FinalPageNoProgress
        | feed_work_scheduler.FinalPageReplayable
    ),
) -> runtime_adapters.PageFinalizationEvidence:
    evidence = result.source_evidence
    if not isinstance(evidence, runtime_adapters.PageFinalizationEvidence):
        message = "test expected page-finalization evidence"
        raise TypeError(message)
    return evidence


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
    disposition: ingestion_lease_store.ChildDisposition = (
        ingestion_lease_store.ChildDisposition.APPLIED
    ),
    *,
    cursor_effect: ingestion_lease_store.CursorEffect = (
        ingestion_lease_store.CursorEffect.ADVANCED
    ),
    lifecycle_effect: ingestion_lease_store.LifecycleEffect = (
        ingestion_lease_store.LifecycleEffect.NONE
    ),
) -> ingestion_lease_store.ChildMutationResult:
    return ingestion_lease_store.ChildMutationResult(
        feed_id=feed_id,
        disposition=disposition,
        cursor_effect=cursor_effect,
        lifecycle_effect=lifecycle_effect,
    )


def _batch_committed(
    *children: ingestion_lease_store.ChildMutationResult,
    lease_effect: ingestion_lease_store.LeaseLifecycleEffect = (
        ingestion_lease_store.LeaseLifecycleEffect.NONE
    ),
) -> ingestion_lease_store.BatchCommitted:
    return ingestion_lease_store.BatchCommitted(
        lease_effect=ingestion_lease_store.LeaseLifecycleResult(
            effect=lease_effect,
        ),
        children=children,
    )


def _batch_with_malformed_lease_effect(
    *children: ingestion_lease_store.ChildMutationResult,
) -> ingestion_lease_store.BatchCommitted:
    lease_effect = ingestion_lease_store.LeaseLifecycleResult(
        effect=ingestion_lease_store.LeaseLifecycleEffect.NONE,
    )
    object.__setattr__(lease_effect, "effect", object())
    return ingestion_lease_store.BatchCommitted(
        lease_effect=lease_effect,
        children=children,
    )


def _record_identity(
    grant: ingestion_lease_store.LeaseGrant,
    member: ingestion_lease_store.LeaseMemberIdentity,
    source_order: int,
    *,
    cohort_timestamp: datetime.datetime | None = _NOW,
) -> feed_work_scheduler.CohortRecordIdentity:
    return feed_work_scheduler.CohortRecordIdentity(
        grant=grant,
        member=member,
        page_sequence=0,
        feed_id=member.feed_id,
        cohort_timestamp=cohort_timestamp,
        source_order=source_order,
        local_sequence=source_order,
    )


def _terminal_record(
    identity: feed_work_scheduler.CohortRecordIdentity,
    reason: feed_work_scheduler.CohortRecordTerminalReason,
    *,
    closure_state: feed_work_scheduler.CohortRecordClosureState | None = None,
    status_reason: feed_store.FeedStatusReason = (
        feed_store.FeedStatusReason.SOURCE_UNREACHABLE
    ),
    detail: str = "bounded failure detail",
) -> feed_work_scheduler.CohortRecordTerminalFact:
    if closure_state is None:
        if reason in {
            feed_work_scheduler.CohortRecordTerminalReason.FULL_PIPELINE,
            feed_work_scheduler.CohortRecordTerminalReason.TERMINAL_ITEM_SKIP,
            feed_work_scheduler.CohortRecordTerminalReason.PUBLICATION_EXHAUSTED,
        }:
            closure_state = (
                feed_work_scheduler.CohortRecordClosureState.DURABLY_CLOSED
            )
        else:
            closure_state = (
                feed_work_scheduler.CohortRecordClosureState.REPLAY_SAFE_RELEASE
            )
    item_failure = None
    if reason is (
        feed_work_scheduler.CohortRecordTerminalReason.TERMINAL_ITEM_SKIP
    ):
        item_failure = feed_work_scheduler.CohortItemFailureFact(
            status_reason,
            detail,
        )
    direct_failure = None
    if reason in {
        feed_work_scheduler.CohortRecordTerminalReason.REPLAYABLE_DIRECT,
        feed_work_scheduler.CohortRecordTerminalReason.PUBLICATION_EXHAUSTED,
    }:
        direct_failure = feed_work_scheduler.CohortDirectFailureFact(
            status_reason,
            detail,
        )
    return feed_work_scheduler.CohortRecordTerminalFact(
        identity=identity,
        participated=True,
        closure_state=closure_state,
        full_pipeline_completed=(
            reason
            is feed_work_scheduler.CohortRecordTerminalReason.FULL_PIPELINE
        ),
        terminal_reason=reason,
        item_failure=item_failure,
        direct_failure=direct_failure,
    )


def _terminal_facts(
    *records: feed_work_scheduler.CohortRecordTerminalFact,
    disposition: feed_work_scheduler.CohortTerminalDisposition,
) -> feed_work_scheduler.CohortTerminalFacts:
    return feed_work_scheduler.CohortTerminalFacts(records, disposition)


def _page_context(
    grant: ingestion_lease_store.LeaseGrant,
    *,
    facts: tuple[feed_work_scheduler.CohortTerminalFacts, ...],
    boundaries: tuple[feed_work_scheduler.BoundaryWork, ...] = (),
    unresolved_replay_feed_ids: tuple[uuid.UUID, ...] = (),
    locally_retired_members: tuple[
        ingestion_lease_store.LeaseMemberIdentity,
        ...,
    ] = (),
    no_progress: bool = False,
) -> feed_work_scheduler.PageFinalizationContext:
    cursor = cursor_policy.LeaseCursor(grant, pos=None)
    candidate = (
        cursor.prepare_no_progress()
        if no_progress
        else cursor.prepare(_NOW + datetime.timedelta(seconds=30))
    )
    return feed_work_scheduler.PageFinalizationContext(
        grant=grant,
        page_sequence=0,
        candidate=candidate,
        cohort_terminal_facts=facts,
        unresolved_replay_feed_ids=unresolved_replay_feed_ids,
        locally_retired_members=locally_retired_members,
        replay_blocked_feed_ids=(),
        candidate_boundaries=boundaries,
    )


def _store_with_result(
    result: object,
) -> _MockIngestionLeaseStore:
    store = mock.create_autospec(
        ingestion_lease_store.IngestionLeaseStore,
        instance=True,
    )
    store.commit_child_mutations = mock.AsyncMock(return_value=result)
    return typing.cast("_MockIngestionLeaseStore", store)


class _NoStartStore:
    """Fake whose ordinary method proves failure before an awaitable exists."""

    def __init__(self) -> None:
        self.calls = 0

    def commit_child_mutations(self, *_args: object, **_kwargs: object) -> None:
        self.calls += 1
        raise runtime_adapters.BoundaryCommitNotStarted


class _AcknowledgementLost(RuntimeError):
    """Test-only signal for a result lost after a commit may have happened."""


class TestFencedBoundaryCommitter(unittest.IsolatedAsyncioTestCase):
    """Prove boundaries make one lifecycle-neutral fenced storage attempt."""

    async def test_boundary_committer_nonempty_is_neutral_and_ordered(
        self,
    ) -> None:
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

        for final_logical in (False, True):
            with self.subTest(final_logical=final_logical):
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
                    actor_id=_ACTOR_ID,
                )

                result = await committer.commit(
                    grant,
                    boundaries,
                    final_logical=final_logical,
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
                self.assertEqual(call.kwargs, {"actor_id": _ACTOR_ID})
                batch = call.args[1]
                self.assertIs(
                    type(batch.lease_effect),
                    ingestion_lease_store.NoLeaseEffect,
                )
                self.assertEqual(
                    tuple(type(mutation) for mutation in batch.mutations),
                    (ingestion_lease_store.ClosedCohortProgress,) * 3,
                )
                self.assertEqual(
                    tuple(mutation.member for mutation in batch.mutations),
                    tuple(boundary.member for boundary in boundaries),
                )
                self.assertEqual(
                    tuple(
                        mutation.last_processed_filename
                        for mutation in batch.mutations
                    ),
                    (None,) * 3,
                )
                self.assertEqual(
                    tuple(mutation.cursor for mutation in batch.mutations),
                    tuple(boundary.target for boundary in boundaries),
                )
                store.load_membership.assert_not_called()
                store.refresh_membership.assert_not_called()

    async def test_boundary_committer_empty_is_still_one_neutral_attempt(
        self,
    ) -> None:
        for final_logical in (False, True):
            with self.subTest(final_logical=final_logical):
                store = _store_with_result(_batch_committed())
                committer = runtime_adapters.FencedBoundaryCommitter(
                    store,
                    actor_id=_ACTOR_ID,
                )

                result = await committer.commit(
                    _grant(),
                    (),
                    final_logical=final_logical,
                )

                self.assertEqual(
                    result,
                    feed_work_scheduler.BoundaryBatchCommitted(()),
                )
                store.commit_child_mutations.assert_awaited_once()
                batch = store.commit_child_mutations.await_args.args[1]
                self.assertEqual(batch.mutations, ())
                self.assertIs(
                    type(batch.lease_effect),
                    ingestion_lease_store.NoLeaseEffect,
                )

    async def test_boundary_committer_stale_grant_is_global_rejection(
        self,
    ) -> None:
        store = _store_with_result(
            ingestion_lease_store.GrantRejected(
                ingestion_lease_store.GrantRejectionReason.MISSING,
            )
        )
        committer = runtime_adapters.FencedBoundaryCommitter(
            store,
            actor_id=_ACTOR_ID,
        )

        result = await committer.commit(_grant(), (), final_logical=True)

        self.assertEqual(result, feed_work_scheduler.BoundaryGrantRejected())
        store.commit_child_mutations.assert_awaited_once()

    async def test_boundary_committer_member_rejection_is_local_and_ordered(
        self,
    ) -> None:
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
            actor_id=_ACTOR_ID,
        )

        result = await committer.commit(
            grant,
            boundaries,
            final_logical=False,
        )

        assert isinstance(
            result,
            feed_work_scheduler.BoundaryBatchCommitted,
        )
        self.assertEqual(
            tuple(item.boundary for item in result.results),
            boundaries,
        )
        self.assertEqual(
            tuple(item.disposition for item in result.results),
            (feed_work_scheduler.BoundaryDisposition.MEMBER_REJECTED,) * 2,
        )

    async def test_boundary_committer_definitive_no_start_is_retryable(
        self,
    ) -> None:
        store = _NoStartStore()
        committer = runtime_adapters.FencedBoundaryCommitter(
            typing.cast("ingestion_lease_store.IngestionLeaseStore", store),
            actor_id=_ACTOR_ID,
        )

        result = await committer.commit(_grant(), (), final_logical=True)

        self.assertEqual(
            result,
            feed_work_scheduler.BoundaryBatchRetryable(),
        )
        self.assertEqual(store.calls, 1)

    async def test_boundary_committer_outcome_unknown_transport_propagates(
        self,
    ) -> None:
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
                    actor_id=_ACTOR_ID,
                )

                with self.assertRaises(type(failure)) as raised:
                    await committer.commit(
                        _grant(),
                        (),
                        final_logical=True,
                    )

                self.assertIs(raised.exception, failure)
                store.commit_child_mutations.assert_awaited_once()

    async def test_boundary_committer_outcome_unknown_control_propagates(
        self,
    ) -> None:
        failures = (
            asyncio.CancelledError(),
            _AcknowledgementLost("commit acknowledgement lost"),
            runtime_adapters.BoundaryCommitNotStarted(
                "raised only after await began"
            ),
        )

        for failure in failures:
            with self.subTest(failure=type(failure).__name__):
                store = _store_with_result(object())
                store.commit_child_mutations.side_effect = failure
                committer = runtime_adapters.FencedBoundaryCommitter(
                    store,
                    actor_id=_ACTOR_ID,
                )

                with self.assertRaises(type(failure)) as raised:
                    await committer.commit(
                        _grant(),
                        (),
                        final_logical=True,
                    )

                self.assertIs(raised.exception, failure)
                store.commit_child_mutations.assert_awaited_once()

    async def test_boundary_committer_malformed_results_are_outcome_unknown(
        self,
    ) -> None:
        grant = _grant()
        boundaries = (
            _boundary(grant, uuid.UUID(int=1), group_id="1"),
            _boundary(grant, uuid.UUID(int=2), group_id="2"),
        )
        malformed = (
            object(),
            _batch_committed(_child_result(boundaries[0].feed_id)),
            _batch_committed(
                _child_result(boundaries[1].feed_id),
                _child_result(boundaries[0].feed_id),
            ),
            _batch_committed(
                ingestion_lease_store.ChildMutationResult(
                    feed_id=boundaries[0].feed_id,
                    disposition=typing.cast(
                        "ingestion_lease_store.ChildDisposition",
                        object(),
                    ),
                    cursor_effect=ingestion_lease_store.CursorEffect.ADVANCED,
                    lifecycle_effect=ingestion_lease_store.LifecycleEffect.NONE,
                ),
                _child_result(boundaries[1].feed_id),
            ),
            _batch_committed(
                _child_result(
                    boundaries[0].feed_id,
                    lifecycle_effect=(
                        ingestion_lease_store.LifecycleEffect.RECOVERED
                    ),
                ),
                _child_result(boundaries[1].feed_id),
            ),
            _batch_committed(
                _child_result(boundaries[0].feed_id),
                _child_result(boundaries[1].feed_id),
                lease_effect=(
                    ingestion_lease_store.LeaseLifecycleEffect.RECOVERED
                ),
            ),
            _batch_with_malformed_lease_effect(
                _child_result(boundaries[0].feed_id),
                _child_result(boundaries[1].feed_id),
            ),
        )

        for case_index, result in enumerate(malformed):
            with self.subTest(
                case_index=case_index,
                result_type=type(result).__name__,
            ):
                store = _store_with_result(result)
                committer = runtime_adapters.FencedBoundaryCommitter(
                    store,
                    actor_id=_ACTOR_ID,
                )

                with self.assertRaises(
                    runtime_adapters.BoundaryAdapterIntegrityError
                ):
                    await committer.commit(
                        grant,
                        boundaries,
                        final_logical=False,
                    )

                store.commit_child_mutations.assert_awaited_once()


class TestFencedPageFinalizer(unittest.IsolatedAsyncioTestCase):
    """Prove source lifecycle finalization is exact and once-only."""

    def test_finalizer_requires_an_explicit_exact_budgeted_policy(self) -> None:
        signature = inspect.signature(runtime_adapters.FencedPageFinalizer)
        policy_parameter = signature.parameters["budgeted_failure"]
        self.assertIs(policy_parameter.default, inspect.Parameter.empty)
        settled_parameter = signature.parameters["boundary_settled_utc"]
        self.assertIs(settled_parameter.default, inspect.Parameter.empty)

        store = _store_with_result(_batch_committed())
        with self.assertRaisesRegex(TypeError, "exact ConsumeFailureBudget"):
            runtime_adapters.FencedPageFinalizer(
                store,
                actor_id=_ACTOR_ID,
                budgeted_failure=typing.cast(
                    "failure_policy.ConsumeFailureBudget",
                    object(),
                ),
                boundary_settled_utc=lambda: _NOW,
            )

    async def test_mixed_success_and_replayable_feed_is_one_transaction(
        self,
    ) -> None:
        grant = _grant()
        successful_member = _member(
            grant,
            uuid.UUID(int=101),
            group_id="101",
        )
        replayable_member = _member(
            grant,
            uuid.UUID(int=202),
            group_id="202",
        )
        successful = _terminal_facts(
            _terminal_record(
                _record_identity(grant, successful_member, 0),
                feed_work_scheduler.CohortRecordTerminalReason.FULL_PIPELINE,
            ),
            disposition=feed_work_scheduler.CohortTerminalDisposition.SETTLED,
        )
        replayable = _terminal_facts(
            _terminal_record(
                _record_identity(
                    grant,
                    replayable_member,
                    1,
                    cohort_timestamp=_NOW + datetime.timedelta(seconds=1),
                ),
                feed_work_scheduler.CohortRecordTerminalReason.REPLAYABLE_DIRECT,
            ),
            disposition=(
                feed_work_scheduler.CohortTerminalDisposition.REPLAYABLE_DIRECT
            ),
        )
        boundary = feed_work_scheduler.BoundaryWork(
            successful_member,
            _NOW + datetime.timedelta(seconds=30),
        )
        context = _page_context(
            grant,
            facts=(successful, replayable),
            boundaries=(boundary,),
            unresolved_replay_feed_ids=(replayable_member.feed_id,),
        )
        store = _store_with_result(
            _batch_committed(
                _child_result(
                    successful_member.feed_id,
                    cursor_effect=ingestion_lease_store.CursorEffect.ADVANCED,
                    lifecycle_effect=(
                        ingestion_lease_store.LifecycleEffect.RECOVERED
                    ),
                ),
                _child_result(
                    replayable_member.feed_id,
                    cursor_effect=ingestion_lease_store.CursorEffect.ABSENT,
                    lifecycle_effect=(
                        ingestion_lease_store.LifecycleEffect.FAILURE_RECORDED
                    ),
                ),
                lease_effect=(
                    ingestion_lease_store.LeaseLifecycleEffect.RECOVERED
                ),
            )
        )
        policy = failure_policy.ConsumeFailureBudget(7, 15, 600)
        finalizer = runtime_adapters.FencedPageFinalizer(
            store,
            actor_id=_ACTOR_ID,
            budgeted_failure=policy,
            boundary_settled_utc=lambda: _NOW,
        )

        result = await finalizer.finalize_page(context)

        self.assertIs(type(result), feed_work_scheduler.FinalPageReplayable)
        store.commit_child_mutations.assert_awaited_once()
        call = store.commit_child_mutations.await_args
        self.assertEqual(call.args[0], grant)
        self.assertEqual(call.kwargs, {"actor_id": _ACTOR_ID})
        batch = call.args[1]
        self.assertIs(
            type(batch.lease_effect),
            ingestion_lease_store.FinalizeLeaseRecovery,
        )
        self.assertEqual(len(batch.mutations), 2)
        observation, failure = batch.mutations
        self.assertEqual(
            observation,
            ingestion_lease_store.SourceObservation(
                successful_member,
                boundary.target,
            ),
        )
        self.assertIs(
            type(failure), ingestion_lease_store.FeedFailureTransition
        )
        self.assertIs(failure.member, replayable_member)
        self.assertIsNone(failure.completion_cursor)
        self.assertIs(
            failure.charge_mode,
            ingestion_lease_store.FeedFailureChargeMode.ONE_SHOT,
        )
        self.assertEqual(
            failure.action,
            ingestion_lease_store.NonBudgetedFailure(_NOW),
        )
        self.assertFalse(
            failure_policy.consumes_failure_budget(failure.status_reason)
        )
        assert isinstance(result, feed_work_scheduler.FinalPageReplayable)
        self.assertEqual(
            result.boundary_results,
            (
                feed_work_scheduler.BoundaryResult(
                    boundary,
                    feed_work_scheduler.BoundaryDisposition.COMMITTED,
                ),
            ),
        )
        evidence = _page_evidence(result)
        self.assertIs(
            type(evidence),
            runtime_adapters.PageFinalizationEvidence,
        )
        self.assertEqual(evidence.quarantined_feed_ids, ())
        self.assertEqual(evidence.item_to_feed_promotion_count, 0)
        self.assertEqual(len(evidence.closure_caps), 1)
        self.assertEqual(
            runtime_adapters.feed_closure_cap_feed_id(evidence.closure_caps[0]),
            successful_member.feed_id,
        )

    async def test_no_progress_recovers_participant_without_cursor(
        self,
    ) -> None:
        grant = _grant()
        member = _member(grant, uuid.UUID(int=303), group_id="303")
        facts = _terminal_facts(
            _terminal_record(
                _record_identity(grant, member, 0),
                feed_work_scheduler.CohortRecordTerminalReason.FULL_PIPELINE,
            ),
            disposition=feed_work_scheduler.CohortTerminalDisposition.SETTLED,
        )
        context = _page_context(
            grant,
            facts=(facts,),
            no_progress=True,
        )
        store = _store_with_result(
            _batch_committed(
                _child_result(
                    member.feed_id,
                    cursor_effect=ingestion_lease_store.CursorEffect.ABSENT,
                    lifecycle_effect=(
                        ingestion_lease_store.LifecycleEffect.RECOVERED
                    ),
                )
            )
        )
        finalizer = runtime_adapters.FencedPageFinalizer(
            store,
            actor_id=_ACTOR_ID,
            budgeted_failure=failure_policy.ConsumeFailureBudget(7, 15, 600),
            boundary_settled_utc=lambda: _NOW,
        )

        result = await finalizer.finalize_page(context)

        self.assertIs(type(result), feed_work_scheduler.FinalPageNoProgress)
        batch = store.commit_child_mutations.await_args.args[1]
        self.assertEqual(
            batch.mutations,
            (ingestion_lease_store.SourceObservation(member, None),),
        )
        assert isinstance(result, feed_work_scheduler.FinalPageNoProgress)
        self.assertEqual(result.boundary_results, ())
        self.assertEqual(_page_evidence(result).closure_caps, ())

    async def test_no_progress_rejected_observation_reports_exact_retirement(
        self,
    ) -> None:
        grant = _grant()
        member = _member(grant, uuid.UUID(int=304), group_id="304")
        facts = _terminal_facts(
            _terminal_record(
                _record_identity(grant, member, 0, cohort_timestamp=None),
                feed_work_scheduler.CohortRecordTerminalReason.FULL_PIPELINE,
            ),
            disposition=feed_work_scheduler.CohortTerminalDisposition.SETTLED,
        )
        context = _page_context(
            grant,
            facts=(facts,),
            no_progress=True,
        )
        store = _store_with_result(
            _batch_committed(
                _child_result(
                    member.feed_id,
                    ingestion_lease_store.ChildDisposition.STATUS_INELIGIBLE,
                    cursor_effect=ingestion_lease_store.CursorEffect.ABSENT,
                )
            )
        )
        finalizer = runtime_adapters.FencedPageFinalizer(
            store,
            actor_id=_ACTOR_ID,
            budgeted_failure=failure_policy.ConsumeFailureBudget(7, 15, 600),
            boundary_settled_utc=lambda: _NOW,
        )

        result = await finalizer.finalize_page(context)

        self.assertIs(type(result), feed_work_scheduler.FinalPageNoProgress)
        assert isinstance(result, feed_work_scheduler.FinalPageNoProgress)
        self.assertEqual(result.boundary_results, ())
        self.assertEqual(result.final_closure_resolutions, ())
        self.assertEqual(result.member_retirements, (member,))
        batch = store.commit_child_mutations.await_args.args[1]
        self.assertEqual(
            batch.mutations,
            (ingestion_lease_store.SourceObservation(member, None),),
        )

    async def test_no_progress_rejected_pending_skip_prefers_retirement(
        self,
    ) -> None:
        grant = _grant()
        member = _member(grant, uuid.UUID(int=305), group_id="305")
        identity = _record_identity(
            grant,
            member,
            0,
            cohort_timestamp=None,
        )
        facts = _terminal_facts(
            _terminal_record(
                identity,
                feed_work_scheduler.CohortRecordTerminalReason.TERMINAL_ITEM_SKIP,
                closure_state=(
                    feed_work_scheduler.CohortRecordClosureState.FINAL_CLOSURE_PENDING
                ),
            ),
            disposition=(
                feed_work_scheduler.CohortTerminalDisposition.FINAL_CLOSURE_PENDING
            ),
        )
        context = _page_context(
            grant,
            facts=(facts,),
            no_progress=True,
        )
        store = _store_with_result(
            _batch_committed(
                _child_result(
                    member.feed_id,
                    ingestion_lease_store.ChildDisposition.STATUS_INELIGIBLE,
                    cursor_effect=ingestion_lease_store.CursorEffect.ABSENT,
                )
            )
        )
        finalizer = runtime_adapters.FencedPageFinalizer(
            store,
            actor_id=_ACTOR_ID,
            budgeted_failure=failure_policy.ConsumeFailureBudget(7, 15, 600),
            boundary_settled_utc=lambda: _NOW,
        )

        result = await finalizer.finalize_page(context)

        assert isinstance(result, feed_work_scheduler.FinalPageNoProgress)
        self.assertEqual(result.member_retirements, (member,))
        self.assertEqual(
            result.final_closure_resolutions,
            (
                feed_work_scheduler.FinalRecordClosureResolution(
                    identity,
                    feed_work_scheduler.CohortRecordClosureState.REPLAY_SAFE_RELEASE,
                    feed_work_scheduler.FinalRecordReleaseBasis.ACCEPTED_MEMBER_RETIREMENT,
                ),
            ),
        )

    async def test_mutated_closure_cap_is_rejected_as_forged(self) -> None:
        grant = _grant()
        member = _member(grant, uuid.UUID(int=404), group_id="404")
        facts = _terminal_facts(
            _terminal_record(
                _record_identity(grant, member, 0),
                feed_work_scheduler.CohortRecordTerminalReason.FULL_PIPELINE,
            ),
            disposition=feed_work_scheduler.CohortTerminalDisposition.SETTLED,
        )
        target = _NOW + datetime.timedelta(seconds=30)
        context = _page_context(
            grant,
            facts=(facts,),
            boundaries=(feed_work_scheduler.BoundaryWork(member, target),),
        )
        store = _store_with_result(
            _batch_committed(
                _child_result(
                    member.feed_id,
                    cursor_effect=ingestion_lease_store.CursorEffect.ADVANCED,
                )
            )
        )
        finalizer = runtime_adapters.FencedPageFinalizer(
            store,
            actor_id=_ACTOR_ID,
            budgeted_failure=failure_policy.ConsumeFailureBudget(7, 15, 600),
            boundary_settled_utc=lambda: _NOW,
        )
        result = await finalizer.finalize_page(context)
        assert isinstance(result, feed_work_scheduler.FinalPageCovered)
        cap = _page_evidence(result).closure_caps[0]
        object.__setattr__(cap, "feed_id", uuid.UUID(int=405))

        with self.assertRaises(runtime_adapters.BoundaryAdapterIntegrityError):
            runtime_adapters.feed_closure_cap_feed_id(cap)

    async def test_failure_policy_identity_and_settlement_time_are_exact(
        self,
    ) -> None:
        grant = _grant()
        budgeted_member = _member(
            grant,
            uuid.UUID(int=501),
            group_id="501",
        )
        nonbudgeted_member = _member(
            grant,
            uuid.UUID(int=502),
            group_id="502",
        )
        successful_member = _member(
            grant,
            uuid.UUID(int=503),
            group_id="503",
        )
        facts = tuple(
            _terminal_facts(
                _terminal_record(
                    _record_identity(
                        grant,
                        member,
                        source_order,
                        cohort_timestamp=(
                            _NOW + datetime.timedelta(seconds=source_order)
                        ),
                    ),
                    reason,
                    status_reason=status_reason,
                ),
                disposition=feed_work_scheduler.CohortTerminalDisposition.SETTLED,
            )
            for source_order, member, reason, status_reason in (
                (
                    0,
                    budgeted_member,
                    feed_work_scheduler.CohortRecordTerminalReason.PUBLICATION_EXHAUSTED,
                    feed_store.FeedStatusReason.SYSTEM_CONFIGURATION_INVALID,
                ),
                (
                    1,
                    nonbudgeted_member,
                    feed_work_scheduler.CohortRecordTerminalReason.PUBLICATION_EXHAUSTED,
                    feed_store.FeedStatusReason.PIPELINE_PUBLISH_AFTER_BOOKMARK_FAILED,
                ),
                (
                    2,
                    successful_member,
                    feed_work_scheduler.CohortRecordTerminalReason.FULL_PIPELINE,
                    feed_store.FeedStatusReason.SOURCE_UNREACHABLE,
                ),
            )
        )
        target = _NOW + datetime.timedelta(seconds=30)
        boundaries = tuple(
            feed_work_scheduler.BoundaryWork(member, target)
            for member in (
                budgeted_member,
                nonbudgeted_member,
                successful_member,
            )
        )
        context = _page_context(
            grant,
            facts=facts,
            boundaries=boundaries,
        )
        store = _store_with_result(
            _batch_committed(
                _child_result(
                    budgeted_member.feed_id,
                    lifecycle_effect=(
                        ingestion_lease_store.LifecycleEffect.FAILURE_RECORDED
                    ),
                ),
                _child_result(
                    nonbudgeted_member.feed_id,
                    lifecycle_effect=(
                        ingestion_lease_store.LifecycleEffect.FAILURE_RECORDED
                    ),
                ),
                _child_result(successful_member.feed_id),
            )
        )
        policy = failure_policy.ConsumeFailureBudget(9, 17, 701)
        clock = mock.Mock(return_value=_NOW)
        finalizer = runtime_adapters.FencedPageFinalizer(
            store,
            actor_id=_ACTOR_ID,
            budgeted_failure=policy,
            boundary_settled_utc=clock,
        )

        result = await finalizer.finalize_page(context)

        self.assertIs(type(result), feed_work_scheduler.FinalPageCovered)
        self.assertIs(finalizer.budgeted_failure, policy)
        clock.assert_called_once_with()
        batch = store.commit_child_mutations.await_args.args[1]
        budgeted, nonbudgeted, observation = batch.mutations
        self.assertEqual(
            budgeted.action,
            ingestion_lease_store.BudgetedFailure(9, 17, 701),
        )
        self.assertEqual(
            nonbudgeted.action,
            ingestion_lease_store.NonBudgetedFailure(_NOW),
        )
        self.assertIs(
            type(observation),
            ingestion_lease_store.SourceObservation,
        )
        assert isinstance(result, feed_work_scheduler.FinalPageCovered)
        self.assertEqual(
            _page_evidence(result).item_to_feed_promotion_count,
            0,
        )
        self.assertEqual(
            tuple(
                runtime_adapters.feed_closure_cap_feed_id(cap)
                for cap in _page_evidence(result).closure_caps
            ),
            tuple(
                sorted(
                    (
                        budgeted_member.feed_id,
                        nonbudgeted_member.feed_id,
                        successful_member.feed_id,
                    ),
                    key=lambda value: value.int,
                )
            ),
        )

    async def test_every_durable_cursor_effect_mints_only_own_feed_cap(
        self,
    ) -> None:
        cases = (
            (
                ingestion_lease_store.CursorEffect.INITIALIZED,
                ingestion_lease_store.ChildDisposition.APPLIED,
                True,
            ),
            (
                ingestion_lease_store.CursorEffect.ADVANCED,
                ingestion_lease_store.ChildDisposition.APPLIED_AFTER_DEACTIVATION,
                True,
            ),
            (
                ingestion_lease_store.CursorEffect.EQUAL,
                ingestion_lease_store.ChildDisposition.ACCEPTED_NOOP,
                True,
            ),
            (
                ingestion_lease_store.CursorEffect.REGRESSIVE,
                ingestion_lease_store.ChildDisposition.APPLIED,
                True,
            ),
            (
                ingestion_lease_store.CursorEffect.ABSENT,
                ingestion_lease_store.ChildDisposition.APPLIED,
                False,
            ),
            (
                ingestion_lease_store.CursorEffect.ADVANCED,
                ingestion_lease_store.ChildDisposition.MISSING,
                False,
            ),
            (
                ingestion_lease_store.CursorEffect.ADVANCED,
                ingestion_lease_store.ChildDisposition.STATUS_INELIGIBLE,
                False,
            ),
        )
        for index, (cursor_effect, disposition, expected_cap) in enumerate(
            cases,
            start=1,
        ):
            with self.subTest(
                cursor_effect=cursor_effect.value,
                disposition=disposition.value,
            ):
                grant = _grant(fencing_token=index)
                member = _member(
                    grant,
                    uuid.UUID(int=600 + index),
                    group_id=str(600 + index),
                )
                facts = _terminal_facts(
                    _terminal_record(
                        _record_identity(grant, member, 0),
                        feed_work_scheduler.CohortRecordTerminalReason.FULL_PIPELINE,
                    ),
                    disposition=(
                        feed_work_scheduler.CohortTerminalDisposition.SETTLED
                    ),
                )
                target = _NOW + datetime.timedelta(seconds=30)
                context = _page_context(
                    grant,
                    facts=(facts,),
                    boundaries=(
                        feed_work_scheduler.BoundaryWork(member, target),
                    ),
                )
                store = _store_with_result(
                    _batch_committed(
                        _child_result(
                            member.feed_id,
                            disposition,
                            cursor_effect=cursor_effect,
                        )
                    )
                )
                finalizer = runtime_adapters.FencedPageFinalizer(
                    store,
                    actor_id=_ACTOR_ID,
                    budgeted_failure=(
                        failure_policy.ConsumeFailureBudget(7, 15, 600)
                    ),
                    boundary_settled_utc=lambda: _NOW,
                )

                result = await finalizer.finalize_page(context)

                assert isinstance(result, feed_work_scheduler.FinalPageCovered)
                caps = _page_evidence(result).closure_caps
                self.assertEqual(bool(caps), expected_cap)
                if caps:
                    self.assertTrue(
                        runtime_adapters.feed_closure_cap_covers(
                            caps[0],
                            grant=grant,
                            page_sequence=0,
                            feed_id=member.feed_id,
                            requested_target=target,
                        )
                    )
                    self.assertFalse(
                        runtime_adapters.feed_closure_cap_covers(
                            caps[0],
                            grant=dataclasses.replace(
                                grant,
                                fencing_token=grant.fencing_token + 1,
                            ),
                            page_sequence=0,
                            feed_id=member.feed_id,
                            requested_target=target,
                        )
                    )
                    self.assertFalse(
                        runtime_adapters.feed_closure_cap_covers(
                            caps[0],
                            grant=grant,
                            page_sequence=1,
                            feed_id=member.feed_id,
                            requested_target=target,
                        )
                    )
                    self.assertFalse(
                        runtime_adapters.feed_closure_cap_covers(
                            caps[0],
                            grant=grant,
                            page_sequence=0,
                            feed_id=uuid.UUID(int=9999),
                            requested_target=target,
                        )
                    )

    async def test_promoted_page_preserves_children_and_parent_failure(
        self,
    ) -> None:
        grant = _grant()
        members = tuple(
            _member(grant, uuid.UUID(int=value), group_id=str(value))
            for value in (701, 702)
        )
        facts = tuple(
            _terminal_facts(
                _terminal_record(
                    _record_identity(
                        grant,
                        member,
                        source_order,
                        cohort_timestamp=(
                            _NOW + datetime.timedelta(seconds=source_order)
                        ),
                    ),
                    feed_work_scheduler.CohortRecordTerminalReason.TERMINAL_ITEM_SKIP,
                ),
                disposition=feed_work_scheduler.CohortTerminalDisposition.SETTLED,
            )
            for source_order, member in enumerate(members)
        )
        target = _NOW + datetime.timedelta(seconds=30)
        context = _page_context(
            grant,
            facts=facts,
            boundaries=tuple(
                feed_work_scheduler.BoundaryWork(member, target)
                for member in members
            ),
        )
        store = _store_with_result(
            _batch_committed(
                *(_child_result(member.feed_id) for member in members)
            )
        )
        finalizer = runtime_adapters.FencedPageFinalizer(
            store,
            actor_id=_ACTOR_ID,
            budgeted_failure=failure_policy.ConsumeFailureBudget(7, 15, 600),
            boundary_settled_utc=lambda: _NOW,
        )

        result = await finalizer.finalize_page(context)

        self.assertIs(type(result), feed_work_scheduler.FinalPageCovered)
        batch = store.commit_child_mutations.await_args.args[1]
        self.assertIs(
            type(batch.lease_effect), ingestion_lease_store.NoLeaseEffect
        )
        self.assertEqual(
            tuple(type(mutation) for mutation in batch.mutations),
            (ingestion_lease_store.ClosedCohortProgress,) * 2,
        )
        assert isinstance(result, feed_work_scheduler.FinalPageCovered)
        evidence = _page_evidence(result)
        self.assertIs(
            evidence.verdict.selected_scope,
            boundary_verdict.NewlyChargedScope.LEASE_ONLY,
        )
        self.assertEqual(evidence.item_to_feed_promotion_count, 2)
        self.assertEqual(len(evidence.closure_caps), 2)

    async def test_final_pending_uses_exact_durable_or_replay_basis(
        self,
    ) -> None:
        grant = _grant()
        pending_member = _member(
            grant,
            uuid.UUID(int=801),
            group_id="801",
        )
        sibling = _member(grant, uuid.UUID(int=802), group_id="802")
        pending_identity = _record_identity(
            grant,
            pending_member,
            0,
            cohort_timestamp=None,
        )
        pending = _terminal_facts(
            _terminal_record(
                pending_identity,
                feed_work_scheduler.CohortRecordTerminalReason.TERMINAL_ITEM_SKIP,
                closure_state=(
                    feed_work_scheduler.CohortRecordClosureState.FINAL_CLOSURE_PENDING
                ),
            ),
            disposition=(
                feed_work_scheduler.CohortTerminalDisposition.FINAL_CLOSURE_PENDING
            ),
        )
        successful = _terminal_facts(
            _terminal_record(
                _record_identity(
                    grant,
                    sibling,
                    1,
                    cohort_timestamp=_NOW + datetime.timedelta(seconds=1),
                ),
                feed_work_scheduler.CohortRecordTerminalReason.FULL_PIPELINE,
            ),
            disposition=feed_work_scheduler.CohortTerminalDisposition.SETTLED,
        )
        target = _NOW + datetime.timedelta(seconds=30)
        context = _page_context(
            grant,
            facts=(pending, successful),
            boundaries=(
                feed_work_scheduler.BoundaryWork(pending_member, target),
                feed_work_scheduler.BoundaryWork(sibling, target),
            ),
        )
        store = _store_with_result(
            _batch_committed(
                _child_result(
                    pending_member.feed_id,
                    lifecycle_effect=(
                        ingestion_lease_store.LifecycleEffect.FAILURE_RECORDED
                    ),
                ),
                _child_result(sibling.feed_id),
            )
        )
        finalizer = runtime_adapters.FencedPageFinalizer(
            store,
            actor_id=_ACTOR_ID,
            budgeted_failure=failure_policy.ConsumeFailureBudget(7, 15, 600),
            boundary_settled_utc=lambda: _NOW,
        )

        result = await finalizer.finalize_page(context)

        assert isinstance(result, feed_work_scheduler.FinalPageCovered)
        self.assertEqual(
            result.final_closure_resolutions,
            (
                feed_work_scheduler.FinalRecordClosureResolution(
                    pending_identity,
                    feed_work_scheduler.CohortRecordClosureState.DURABLY_CLOSED,
                    feed_work_scheduler.FinalRecordReleaseBasis.DURABLE_SOURCE_CLOSURE,
                ),
            ),
        )
        replay_identity = _record_identity(
            grant,
            pending_member,
            2,
            cohort_timestamp=_NOW + datetime.timedelta(seconds=2),
        )
        replayable = _terminal_facts(
            _terminal_record(
                replay_identity,
                feed_work_scheduler.CohortRecordTerminalReason.REPLAYABLE_DIRECT,
            ),
            disposition=(
                feed_work_scheduler.CohortTerminalDisposition.REPLAYABLE_DIRECT
            ),
        )
        replay_context = _page_context(
            grant,
            facts=(pending, replayable, successful),
            boundaries=(feed_work_scheduler.BoundaryWork(sibling, target),),
            unresolved_replay_feed_ids=(pending_member.feed_id,),
        )
        replay_store = _store_with_result(
            _batch_committed(
                _child_result(sibling.feed_id),
                _child_result(
                    pending_member.feed_id,
                    cursor_effect=ingestion_lease_store.CursorEffect.ABSENT,
                    lifecycle_effect=(
                        ingestion_lease_store.LifecycleEffect.FAILURE_RECORDED
                    ),
                ),
            )
        )
        replay_finalizer = runtime_adapters.FencedPageFinalizer(
            replay_store,
            actor_id=_ACTOR_ID,
            budgeted_failure=failure_policy.ConsumeFailureBudget(7, 15, 600),
            boundary_settled_utc=lambda: _NOW,
        )

        replay_result = await replay_finalizer.finalize_page(replay_context)

        assert isinstance(
            replay_result,
            feed_work_scheduler.FinalPageReplayable,
        )
        self.assertEqual(
            tuple(
                runtime_adapters.feed_closure_cap_feed_id(cap)
                for cap in _page_evidence(replay_result).closure_caps
            ),
            (sibling.feed_id,),
        )
        self.assertEqual(
            replay_result.final_closure_resolutions,
            (
                feed_work_scheduler.FinalRecordClosureResolution(
                    pending_identity,
                    feed_work_scheduler.CohortRecordClosureState.REPLAY_SAFE_RELEASE,
                    feed_work_scheduler.FinalRecordReleaseBasis.ACCEPTED_REPLAYABLE_FEED,
                ),
            ),
        )

    async def test_exact_local_retirement_releases_only_its_pending_member(
        self,
    ) -> None:
        grant = _grant()
        retired = _member(grant, uuid.UUID(int=901), group_id="901")
        sibling = _member(grant, uuid.UUID(int=902), group_id="902")
        pending_identity = _record_identity(
            grant,
            retired,
            0,
            cohort_timestamp=None,
        )
        pending = _terminal_facts(
            _terminal_record(
                pending_identity,
                feed_work_scheduler.CohortRecordTerminalReason.TERMINAL_ITEM_SKIP,
                closure_state=(
                    feed_work_scheduler.CohortRecordClosureState.FINAL_CLOSURE_PENDING
                ),
            ),
            disposition=(
                feed_work_scheduler.CohortTerminalDisposition.FINAL_CLOSURE_PENDING
            ),
        )
        successful = _terminal_facts(
            _terminal_record(
                _record_identity(grant, sibling, 1),
                feed_work_scheduler.CohortRecordTerminalReason.FULL_PIPELINE,
            ),
            disposition=feed_work_scheduler.CohortTerminalDisposition.SETTLED,
        )
        target = _NOW + datetime.timedelta(seconds=30)
        context = _page_context(
            grant,
            facts=(pending, successful),
            boundaries=(feed_work_scheduler.BoundaryWork(sibling, target),),
            locally_retired_members=(retired,),
        )
        store = _store_with_result(
            _batch_committed(_child_result(sibling.feed_id))
        )
        finalizer = runtime_adapters.FencedPageFinalizer(
            store,
            actor_id=_ACTOR_ID,
            budgeted_failure=failure_policy.ConsumeFailureBudget(7, 15, 600),
            boundary_settled_utc=lambda: _NOW,
        )

        result = await finalizer.finalize_page(context)

        assert isinstance(result, feed_work_scheduler.FinalPageCovered)
        self.assertEqual(
            result.final_closure_resolutions,
            (
                feed_work_scheduler.FinalRecordClosureResolution(
                    pending_identity,
                    feed_work_scheduler.CohortRecordClosureState.REPLAY_SAFE_RELEASE,
                    feed_work_scheduler.FinalRecordReleaseBasis.ACCEPTED_MEMBER_RETIREMENT,
                ),
            ),
        )
        self.assertEqual(
            tuple(
                runtime_adapters.feed_closure_cap_feed_id(cap)
                for cap in _page_evidence(result).closure_caps
            ),
            (sibling.feed_id,),
        )

    async def test_quarantine_and_one_attempt_failure_outcomes_are_typed(
        self,
    ) -> None:
        grant = _grant()
        failed = _member(grant, uuid.UUID(int=1001), group_id="1001")
        facts = _terminal_facts(
            _terminal_record(
                _record_identity(grant, failed, 0),
                feed_work_scheduler.CohortRecordTerminalReason.PUBLICATION_EXHAUSTED,
                status_reason=(
                    feed_store.FeedStatusReason.SYSTEM_CONFIGURATION_INVALID
                ),
            ),
            disposition=feed_work_scheduler.CohortTerminalDisposition.SETTLED,
        )
        target = _NOW + datetime.timedelta(seconds=30)
        context = _page_context(
            grant,
            facts=(facts,),
            boundaries=(feed_work_scheduler.BoundaryWork(failed, target),),
        )
        store = _store_with_result(
            _batch_committed(
                _child_result(
                    failed.feed_id,
                    lifecycle_effect=(
                        ingestion_lease_store.LifecycleEffect.QUARANTINED
                    ),
                )
            )
        )
        finalizer = runtime_adapters.FencedPageFinalizer(
            store,
            actor_id=_ACTOR_ID,
            budgeted_failure=failure_policy.ConsumeFailureBudget(7, 15, 600),
            boundary_settled_utc=lambda: _NOW,
        )

        result = await finalizer.finalize_page(context)

        assert isinstance(result, feed_work_scheduler.FinalPageCovered)
        self.assertEqual(
            _page_evidence(result).quarantined_feed_ids,
            (failed.feed_id,),
        )

        no_start = _NoStartStore()
        retryable = await runtime_adapters.FencedPageFinalizer(
            typing.cast("ingestion_lease_store.IngestionLeaseStore", no_start),
            actor_id=_ACTOR_ID,
            budgeted_failure=failure_policy.ConsumeFailureBudget(7, 15, 600),
            boundary_settled_utc=lambda: _NOW,
        ).finalize_page(context)
        self.assertIs(type(retryable), feed_work_scheduler.FinalPageRetryable)
        self.assertEqual(no_start.calls, 1)

        uncertain_store = _store_with_result(object())
        uncertain_store.commit_child_mutations.side_effect = OSError(
            "connection lost"
        )
        unknown = await runtime_adapters.FencedPageFinalizer(
            uncertain_store,
            actor_id=_ACTOR_ID,
            budgeted_failure=failure_policy.ConsumeFailureBudget(7, 15, 600),
            boundary_settled_utc=lambda: _NOW,
        ).finalize_page(context)
        self.assertIs(
            type(unknown),
            feed_work_scheduler.FinalPageOutcomeUnknown,
        )
        uncertain_store.commit_child_mutations.assert_awaited_once()

        cancelled = asyncio.CancelledError()
        cancelled_store = _store_with_result(object())
        cancelled_store.commit_child_mutations.side_effect = cancelled
        with self.assertRaises(asyncio.CancelledError) as raised:
            await runtime_adapters.FencedPageFinalizer(
                cancelled_store,
                actor_id=_ACTOR_ID,
                budgeted_failure=(
                    failure_policy.ConsumeFailureBudget(7, 15, 600)
                ),
                boundary_settled_utc=lambda: _NOW,
            ).finalize_page(context)
        self.assertIs(raised.exception, cancelled)

    async def test_finalizer_rejects_malformed_lease_effect(
        self,
    ) -> None:
        grant = _grant()
        member = _member(grant, uuid.UUID(int=1101), group_id="1101")
        facts = _terminal_facts(
            _terminal_record(
                _record_identity(grant, member, 0),
                feed_work_scheduler.CohortRecordTerminalReason.FULL_PIPELINE,
            ),
            disposition=feed_work_scheduler.CohortTerminalDisposition.SETTLED,
        )
        target = _NOW + datetime.timedelta(seconds=30)
        context = _page_context(
            grant,
            facts=(facts,),
            boundaries=(feed_work_scheduler.BoundaryWork(member, target),),
        )
        store = _store_with_result(
            _batch_with_malformed_lease_effect(
                _child_result(member.feed_id),
            )
        )
        finalizer = runtime_adapters.FencedPageFinalizer(
            store,
            actor_id=_ACTOR_ID,
            budgeted_failure=failure_policy.ConsumeFailureBudget(7, 15, 600),
            boundary_settled_utc=lambda: _NOW,
        )

        result = await finalizer.finalize_page(context)

        self.assertIs(
            type(result),
            feed_work_scheduler.FinalPageOutcomeUnknown,
        )
        store.commit_child_mutations.assert_awaited_once()

    async def test_local_member_rejection_releases_only_its_pending_record(
        self,
    ) -> None:
        grant = _grant()
        member = _member(grant, uuid.UUID(int=1201), group_id="1201")
        identity = _record_identity(
            grant,
            member,
            0,
            cohort_timestamp=None,
        )
        facts = _terminal_facts(
            _terminal_record(
                identity,
                feed_work_scheduler.CohortRecordTerminalReason.TERMINAL_ITEM_SKIP,
                closure_state=(
                    feed_work_scheduler.CohortRecordClosureState.FINAL_CLOSURE_PENDING
                ),
            ),
            disposition=(
                feed_work_scheduler.CohortTerminalDisposition.FINAL_CLOSURE_PENDING
            ),
        )
        target = _NOW + datetime.timedelta(seconds=30)
        boundary = feed_work_scheduler.BoundaryWork(member, target)
        context = _page_context(
            grant,
            facts=(facts,),
            boundaries=(boundary,),
        )
        store = _store_with_result(
            _batch_committed(
                _child_result(
                    member.feed_id,
                    ingestion_lease_store.ChildDisposition.STATUS_INELIGIBLE,
                    cursor_effect=ingestion_lease_store.CursorEffect.EQUAL,
                )
            )
        )
        finalizer = runtime_adapters.FencedPageFinalizer(
            store,
            actor_id=_ACTOR_ID,
            budgeted_failure=failure_policy.ConsumeFailureBudget(7, 15, 600),
            boundary_settled_utc=lambda: _NOW,
        )

        result = await finalizer.finalize_page(context)

        assert isinstance(result, feed_work_scheduler.FinalPageCovered)
        self.assertEqual(
            result.boundary_results,
            (
                feed_work_scheduler.BoundaryResult(
                    boundary,
                    feed_work_scheduler.BoundaryDisposition.MEMBER_REJECTED,
                ),
            ),
        )
        self.assertEqual(_page_evidence(result).closure_caps, ())
        self.assertEqual(
            result.final_closure_resolutions,
            (
                feed_work_scheduler.FinalRecordClosureResolution(
                    identity,
                    feed_work_scheduler.CohortRecordClosureState.REPLAY_SAFE_RELEASE,
                    feed_work_scheduler.FinalRecordReleaseBasis.ACCEPTED_MEMBER_RETIREMENT,
                ),
            ),
        )
        self.assertEqual(result.member_retirements, (member,))

    async def test_finalizer_correlation_failures_are_outcome_unknown_once(
        self,
    ) -> None:
        grant = _grant()
        member = _member(grant, uuid.UUID(int=1301), group_id="1301")
        facts = _terminal_facts(
            _terminal_record(
                _record_identity(grant, member, 0),
                feed_work_scheduler.CohortRecordTerminalReason.FULL_PIPELINE,
            ),
            disposition=feed_work_scheduler.CohortTerminalDisposition.SETTLED,
        )
        target = _NOW + datetime.timedelta(seconds=30)
        context = _page_context(
            grant,
            facts=(facts,),
            boundaries=(feed_work_scheduler.BoundaryWork(member, target),),
        )
        malformed = (
            _batch_committed(),
            _batch_committed(
                _child_result(member.feed_id),
                _child_result(member.feed_id),
            ),
            _batch_committed(_child_result(uuid.UUID(int=1302))),
        )
        for case_index, committed in enumerate(malformed):
            with self.subTest(
                case_index=case_index,
                child_count=len(committed.children),
            ):
                store = _store_with_result(committed)
                finalizer = runtime_adapters.FencedPageFinalizer(
                    store,
                    actor_id=_ACTOR_ID,
                    budgeted_failure=(
                        failure_policy.ConsumeFailureBudget(7, 15, 600)
                    ),
                    boundary_settled_utc=lambda: _NOW,
                )

                result = await finalizer.finalize_page(context)

                self.assertIs(
                    type(result),
                    feed_work_scheduler.FinalPageOutcomeUnknown,
                )
                store.commit_child_mutations.assert_awaited_once()

        rejection_store = _store_with_result(
            ingestion_lease_store.GrantRejected(
                ingestion_lease_store.GrantRejectionReason.MISSING,
            )
        )
        rejected = await runtime_adapters.FencedPageFinalizer(
            rejection_store,
            actor_id=_ACTOR_ID,
            budgeted_failure=failure_policy.ConsumeFailureBudget(7, 15, 600),
            boundary_settled_utc=lambda: _NOW,
        ).finalize_page(context)
        self.assertIs(
            type(rejected),
            feed_work_scheduler.FinalPageGrantRejected,
        )
        rejection_store.commit_child_mutations.assert_awaited_once()

    async def test_invalid_settlement_time_fails_before_storage_checkout(
        self,
    ) -> None:
        grant = _grant()
        member = _member(grant, uuid.UUID(int=1401), group_id="1401")
        facts = _terminal_facts(
            _terminal_record(
                _record_identity(grant, member, 0),
                feed_work_scheduler.CohortRecordTerminalReason.FULL_PIPELINE,
            ),
            disposition=feed_work_scheduler.CohortTerminalDisposition.SETTLED,
        )
        context = _page_context(grant, facts=(facts,), no_progress=True)
        store = _store_with_result(_batch_committed())
        finalizer = runtime_adapters.FencedPageFinalizer(
            store,
            actor_id=_ACTOR_ID,
            budgeted_failure=failure_policy.ConsumeFailureBudget(7, 15, 600),
            boundary_settled_utc=lambda: _NOW.replace(tzinfo=None),
        )

        with self.assertRaisesRegex(ValueError, "UTC-aware"):
            await finalizer.finalize_page(context)

        store.commit_child_mutations.assert_not_awaited()


class TestPhysicalCohortCommitter(unittest.IsolatedAsyncioTestCase):
    """Prove direct cohorts preserve their exact optional durability fields."""

    async def test_physical_cohort_path_cursor_and_both_are_exact_neutral(
        self,
    ) -> None:
        grant = _grant()
        member = _member(grant, uuid.UUID(int=90), group_id="90")
        cases = (
            ("gs://bucket/path-only.ogg", None),
            (None, _NOW),
            ("gs://bucket/both.ogg", _NOW),
        )

        for path, cursor in cases:
            for final_logical in (False, True):
                with self.subTest(
                    path=path,
                    has_cursor=cursor is not None,
                    final_logical=final_logical,
                ):
                    commit = runtime_adapters.PhysicalCohortCommit(
                        member,
                        path,
                        cursor,
                    )
                    store = _store_with_result(
                        _batch_committed(
                            _child_result(
                                member.feed_id,
                                cursor_effect=(
                                    ingestion_lease_store.CursorEffect.ABSENT
                                    if cursor is None
                                    else ingestion_lease_store.CursorEffect.ADVANCED
                                ),
                            )
                        )
                    )
                    committer = runtime_adapters.FencedBoundaryCommitter(
                        store,
                        actor_id=_ACTOR_ID,
                    )

                    result = await committer.commit_cohort(
                        grant,
                        commit,
                        final_logical=final_logical,
                    )

                    self.assertEqual(
                        result,
                        runtime_adapters.PhysicalCohortResult(
                            commit,
                            feed_work_scheduler.BoundaryDisposition.COMMITTED,
                        ),
                    )
                    store.commit_child_mutations.assert_awaited_once()
                    call = store.commit_child_mutations.await_args
                    self.assertEqual(call.args[0], grant)
                    self.assertEqual(call.kwargs, {"actor_id": _ACTOR_ID})
                    batch = call.args[1]
                    self.assertIs(
                        type(batch.lease_effect),
                        ingestion_lease_store.NoLeaseEffect,
                    )
                    self.assertEqual(
                        batch.mutations,
                        (
                            ingestion_lease_store.ClosedCohortProgress(
                                member,
                                path,
                                cursor,
                            ),
                        ),
                    )

    async def test_physical_cohort_member_rejection_stays_local(self) -> None:
        grant = _grant()
        member = _member(grant, uuid.UUID(int=91), group_id="91")
        commit = runtime_adapters.PhysicalCohortCommit(member, None, _NOW)
        store = _store_with_result(
            _batch_committed(
                _child_result(
                    member.feed_id,
                    ingestion_lease_store.ChildDisposition.MISSING,
                )
            )
        )
        committer = runtime_adapters.FencedBoundaryCommitter(
            store,
            actor_id=_ACTOR_ID,
        )

        result = await committer.commit_cohort(
            grant,
            commit,
            final_logical=False,
        )

        self.assertEqual(
            result,
            runtime_adapters.PhysicalCohortResult(
                commit,
                feed_work_scheduler.BoundaryDisposition.MEMBER_REJECTED,
            ),
        )

    async def test_physical_cohort_all_null_is_rejected_before_io(self) -> None:
        grant = _grant()
        member = _member(grant, uuid.UUID(int=92), group_id="92")
        store = _store_with_result(_batch_committed())
        runtime_adapters.FencedBoundaryCommitter(store, actor_id=_ACTOR_ID)

        with self.assertRaisesRegex(ValueError, "requires a path or cursor"):
            runtime_adapters.PhysicalCohortCommit(member, None, None)

        store.commit_child_mutations.assert_not_awaited()

    def test_physical_cohort_value_is_frozen(self) -> None:
        grant = _grant()
        member = _member(grant, uuid.UUID(int=93), group_id="93")
        commit = runtime_adapters.PhysicalCohortCommit(member, None, _NOW)

        cursor_field = "cursor"
        with self.assertRaises(dataclasses.FrozenInstanceError):
            setattr(commit, cursor_field, None)

        with self.assertRaises(TypeError):
            runtime_adapters.PhysicalCohortResult(
                commit,
                typing.cast(
                    "feed_work_scheduler.BoundaryDisposition",
                    "committed",
                ),
            )
        with self.assertRaises(ValueError):
            runtime_adapters.PhysicalCohortResult(
                commit,
                feed_work_scheduler.BoundaryDisposition.RETRYABLE,
            )


if __name__ == "__main__":
    unittest.main()
