"""Contract matrix for the pure Broadcastify Calls page verdict."""

from __future__ import annotations

import dataclasses
import datetime
import inspect
import typing
import unittest
import uuid
from unittest import mock

from backend.pipeline.ingestion import failure_policy, feed_work_scheduler
from backend.pipeline.ingestion.collectors import failure_classification
from backend.pipeline.ingestion.collectors.bcfy_calls import boundary_verdict
from backend.pipeline.storage import feed_store, ingestion_lease_store

_OWNER_ID = uuid.UUID("11111111-2222-3333-4444-555555555555")
_SOURCE_TIME = datetime.datetime(2026, 7, 13, 12, 0, tzinfo=datetime.UTC)
_PAGE_SEQUENCE = 7


def _grant(
    *,
    lease_key: str = "150",
    fencing_token: int = 11,
) -> ingestion_lease_store.LeaseGrant:
    return ingestion_lease_store.LeaseGrant(
        source_type=feed_store.SourceType.BCFY_CALLS,
        lease_key=lease_key,
        owner_worker_id=_OWNER_ID,
        fencing_token=fencing_token,
    )


def _assign_attribute(
    target: object,
    field_name: str,
    value: object,
) -> None:
    setattr(target, field_name, value)


def _member(
    grant: ingestion_lease_store.LeaseGrant,
    feed_number: int,
) -> ingestion_lease_store.LeaseMemberIdentity:
    feed_id = uuid.UUID(int=feed_number)
    group_id = str(feed_number)
    return ingestion_lease_store._issue_member_identity(
        grant,
        feed_id=feed_id,
        source_type=feed_store.SourceType.BCFY_CALLS,
        source_feed_id=f"{grant.lease_key}-{group_id}",
        sid=grant.lease_key,
        group_id=group_id,
    )


def _identity(
    grant: ingestion_lease_store.LeaseGrant,
    member: ingestion_lease_store.LeaseMemberIdentity,
    source_order: int,
    *,
    cohort_timestamp: datetime.datetime | None = _SOURCE_TIME,
    local_sequence: int | None = None,
    page_sequence: int = _PAGE_SEQUENCE,
) -> feed_work_scheduler.CohortRecordIdentity:
    return feed_work_scheduler.CohortRecordIdentity(
        grant=grant,
        member=member,
        page_sequence=page_sequence,
        feed_id=member.feed_id,
        cohort_timestamp=cohort_timestamp,
        local_sequence=(
            source_order if local_sequence is None else local_sequence
        ),
    )


def _record(
    identity: feed_work_scheduler.CohortRecordIdentity,
    reason: feed_work_scheduler.CohortRecordTerminalReason,
    *,
    closure_state: feed_work_scheduler.CohortRecordClosureState | None = None,
    participated: bool | None = None,
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
            feed_work_scheduler.CohortRecordTerminalReason.PUBLICATION_ABANDONED,
        }:
            closure_state = (
                feed_work_scheduler.CohortRecordClosureState.DURABLY_CLOSED
            )
        elif reason in {
            feed_work_scheduler.CohortRecordTerminalReason.INTEGRITY_FAILURE,
            feed_work_scheduler.CohortRecordTerminalReason.OUTCOME_UNKNOWN,
        }:
            closure_state = (
                feed_work_scheduler.CohortRecordClosureState.OUTCOME_UNKNOWN
            )
        else:
            closure_state = (
                feed_work_scheduler.CohortRecordClosureState.REPLAY_SAFE_RELEASE
            )
    if participated is None:
        participated = reason in {
            feed_work_scheduler.CohortRecordTerminalReason.FULL_PIPELINE,
            feed_work_scheduler.CohortRecordTerminalReason.TERMINAL_ITEM_SKIP,
            feed_work_scheduler.CohortRecordTerminalReason.PUBLICATION_EXHAUSTED,
            feed_work_scheduler.CohortRecordTerminalReason.PUBLICATION_ABANDONED,
            feed_work_scheduler.CohortRecordTerminalReason.REPLAYABLE_DIRECT,
        }
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


def _facts(
    records: tuple[feed_work_scheduler.CohortRecordTerminalFact, ...],
    disposition: feed_work_scheduler.CohortTerminalDisposition,
) -> feed_work_scheduler.CohortTerminalFacts:
    return feed_work_scheduler.CohortTerminalFacts(records, disposition)


def _settled(
    record: feed_work_scheduler.CohortRecordTerminalFact,
) -> feed_work_scheduler.CohortTerminalFacts:
    return _facts(
        (record,),
        feed_work_scheduler.CohortTerminalDisposition.SETTLED,
    )


def _page(
    grant: ingestion_lease_store.LeaseGrant,
    members: tuple[ingestion_lease_store.LeaseMemberIdentity, ...],
    cohorts: tuple[feed_work_scheduler.CohortTerminalFacts, ...],
    *,
    record_identities: tuple[
        feed_work_scheduler.CohortRecordIdentity,
        ...,
    ]
    | None = None,
    child_recovery_candidates: tuple[
        ingestion_lease_store.LeaseMemberIdentity,
        ...,
    ] = (),
    lease_recovery_candidate: bool = False,
    page_sequence: int = _PAGE_SEQUENCE,
) -> boundary_verdict.PageVerdictInput:
    identities = (
        tuple(
            record.identity for cohort in cohorts for record in cohort.records
        )
        if record_identities is None
        else record_identities
    )
    return boundary_verdict.PageVerdictInput(
        grant=grant,
        page_sequence=page_sequence,
        members=members,
        record_identities=identities,
        cohort_terminal_facts=cohorts,
        child_recovery_candidates=child_recovery_candidates,
        lease_recovery_candidate=lease_recovery_candidate,
    )


def _one_record_feed(
    grant: ingestion_lease_store.LeaseGrant,
    member: ingestion_lease_store.LeaseMemberIdentity,
    source_order: int,
    reason: feed_work_scheduler.CohortRecordTerminalReason,
    *,
    status_reason: feed_store.FeedStatusReason = (
        feed_store.FeedStatusReason.SOURCE_UNREACHABLE
    ),
    detail: str = "bounded failure detail",
) -> feed_work_scheduler.CohortTerminalFacts:
    timestamp = _SOURCE_TIME + datetime.timedelta(seconds=source_order)
    return _settled(
        _record(
            _identity(
                grant,
                member,
                source_order,
                cohort_timestamp=timestamp,
            ),
            reason,
            status_reason=status_reason,
            detail=detail,
        )
    )


class TestVerdictPromotionMatrix(unittest.TestCase):
    """D-10 through D-18 participation and promotion truth table."""

    def test_verdict_promotion_truth_table_zero_one_two_many(self) -> None:
        cases = (
            (0, 0, boundary_verdict.NewlyChargedScope.FAILED_FEEDS),
            (1, 1, boundary_verdict.NewlyChargedScope.FAILED_FEEDS),
            (2, 2, boundary_verdict.NewlyChargedScope.LEASE_ONLY),
            (4, 4, boundary_verdict.NewlyChargedScope.LEASE_ONLY),
        )
        for participant_count, failed_count, expected_scope in cases:
            with self.subTest(
                participant_count=participant_count,
                failed_count=failed_count,
            ):
                grant = _grant()
                members = tuple(
                    _member(grant, index + 1)
                    for index in range(participant_count)
                )
                cohorts = tuple(
                    _one_record_feed(
                        grant,
                        member,
                        index,
                        feed_work_scheduler.CohortRecordTerminalReason.TERMINAL_ITEM_SKIP,
                    )
                    for index, member in enumerate(members)
                )

                verdict = boundary_verdict.decide_page_verdict(
                    _page(grant, members, cohorts)
                )

                self.assertEqual(
                    verdict.participating_feed_count,
                    participant_count,
                )
                self.assertEqual(verdict.failed_feed_count, failed_count)
                self.assertEqual(verdict.selected_scope, expected_scope)
                self.assertEqual(
                    verdict.item_to_feed_promotion_count,
                    failed_count,
                )

    def test_quiet_feed_is_neutral_to_promotion_denominator(self) -> None:
        grant = _grant()
        first = _member(grant, 1)
        second = _member(grant, 2)
        quiet = _member(grant, 3)
        cohorts = (
            _one_record_feed(
                grant,
                first,
                0,
                feed_work_scheduler.CohortRecordTerminalReason.TERMINAL_ITEM_SKIP,
            ),
            _one_record_feed(
                grant,
                second,
                1,
                feed_work_scheduler.CohortRecordTerminalReason.TERMINAL_ITEM_SKIP,
            ),
        )

        verdict = boundary_verdict.decide_page_verdict(
            _page(grant, (quiet, second, first), cohorts)
        )

        self.assertEqual(verdict.participating_feed_count, 2)
        self.assertEqual(verdict.failed_feed_count, 2)
        self.assertEqual(
            verdict.selected_scope,
            boundary_verdict.NewlyChargedScope.LEASE_ONLY,
        )
        quiet_facts = verdict.feed_facts[2]
        self.assertEqual(quiet_facts.member, quiet)
        self.assertFalse(quiet_facts.participated)
        self.assertEqual(quiet_facts.record_facts, ())
        self.assertIsNone(quiet_facts.item_failure)
        self.assertEqual(quiet_facts.direct_failures, ())

    def test_participating_success_suppresses_lease_promotion(self) -> None:
        grant = _grant()
        successful = _member(grant, 1)
        failed = _member(grant, 2)
        cohorts = (
            _one_record_feed(
                grant,
                successful,
                0,
                feed_work_scheduler.CohortRecordTerminalReason.FULL_PIPELINE,
            ),
            _one_record_feed(
                grant,
                failed,
                1,
                feed_work_scheduler.CohortRecordTerminalReason.TERMINAL_ITEM_SKIP,
            ),
        )

        verdict = boundary_verdict.decide_page_verdict(
            _page(grant, (successful, failed), cohorts)
        )

        self.assertEqual(verdict.participating_feed_count, 2)
        self.assertEqual(verdict.successful_feed_count, 1)
        self.assertEqual(verdict.failed_feed_count, 1)
        self.assertEqual(
            verdict.selected_scope,
            boundary_verdict.NewlyChargedScope.FAILED_FEEDS,
        )
        self.assertIsNone(verdict.lease_failure)

    def test_partial_full_success_does_not_erase_later_direct_failure(
        self,
    ) -> None:
        grant = _grant()
        first = _member(grant, 1)
        second = _member(grant, 2)
        cohorts = (
            _one_record_feed(
                grant,
                first,
                0,
                feed_work_scheduler.CohortRecordTerminalReason.FULL_PIPELINE,
            ),
            _one_record_feed(
                grant,
                first,
                1,
                feed_work_scheduler.CohortRecordTerminalReason.PUBLICATION_EXHAUSTED,
                status_reason=(
                    feed_store.FeedStatusReason.PIPELINE_PUBLISH_AFTER_BOOKMARK_FAILED
                ),
                detail="later publication exhausted",
            ),
            _one_record_feed(
                grant,
                second,
                2,
                feed_work_scheduler.CohortRecordTerminalReason.TERMINAL_ITEM_SKIP,
            ),
        )

        verdict = boundary_verdict.decide_page_verdict(
            _page(grant, (first, second), cohorts)
        )

        self.assertEqual(verdict.successful_feed_ids, (first.feed_id,))
        self.assertEqual(verdict.failed_feed_count, 2)
        self.assertEqual(
            verdict.failed_feeds[0].origin,
            boundary_verdict.FeedFailureOrigin.DIRECT_PIPELINE,
        )
        self.assertEqual(
            verdict.selected_scope,
            boundary_verdict.NewlyChargedScope.FAILED_FEEDS,
        )
        self.assertEqual(verdict.item_to_feed_promotion_count, 1)

    def test_common_status_different_detail_promotes_common_reason(
        self,
    ) -> None:
        grant = _grant()
        first = _member(grant, 1)
        second = _member(grant, 2)
        status = feed_store.FeedStatusReason.SOURCE_RATE_LIMITED
        cohorts = (
            _one_record_feed(
                grant,
                first,
                0,
                feed_work_scheduler.CohortRecordTerminalReason.TERMINAL_ITEM_SKIP,
                status_reason=status,
                detail="first bounded detail",
            ),
            _one_record_feed(
                grant,
                second,
                1,
                feed_work_scheduler.CohortRecordTerminalReason.TERMINAL_ITEM_SKIP,
                status_reason=status,
                detail="second unrelated detail",
            ),
        )

        verdict = boundary_verdict.decide_page_verdict(
            _page(grant, (second, first), cohorts)
        )

        self.assertEqual(
            verdict.selected_scope,
            boundary_verdict.NewlyChargedScope.LEASE_ONLY,
        )
        self.assertIsNotNone(verdict.lease_failure)
        lease_failure = typing.cast(
            "boundary_verdict.BoundaryFailure",
            verdict.lease_failure,
        )
        self.assertEqual(lease_failure.status_reason, status)
        self.assertEqual(lease_failure.detail, "first bounded detail")

    def test_mixed_feed_failures_ignore_identical_detail(self) -> None:
        grant = _grant()
        first = _member(grant, 1)
        second = _member(grant, 2)
        detail = "identical free form detail"
        cohorts = (
            _one_record_feed(
                grant,
                first,
                0,
                feed_work_scheduler.CohortRecordTerminalReason.TERMINAL_ITEM_SKIP,
                status_reason=feed_store.FeedStatusReason.SOURCE_UNREACHABLE,
                detail=detail,
            ),
            _one_record_feed(
                grant,
                second,
                1,
                feed_work_scheduler.CohortRecordTerminalReason.TERMINAL_ITEM_SKIP,
                status_reason=feed_store.FeedStatusReason.SOURCE_RATE_LIMITED,
                detail=detail,
            ),
        )

        verdict = boundary_verdict.decide_page_verdict(
            _page(grant, (first, second), cohorts)
        )

        self.assertIsNotNone(verdict.lease_failure)
        lease_failure = typing.cast(
            "boundary_verdict.BoundaryFailure",
            verdict.lease_failure,
        )
        self.assertEqual(
            lease_failure.status_reason,
            feed_store.FeedStatusReason.SYSTEM_COLLECTOR_ERROR,
        )
        self.assertEqual(
            lease_failure.detail,
            boundary_verdict.MIXED_FEED_FAILURE_REASON,
        )


class TestItemFailureAggregation(unittest.TestCase):
    """Existing ItemBatchOutcome remains the sole item promotion owner."""

    def test_every_item_failure_fact_uses_item_batch_outcome_adapter(
        self,
    ) -> None:
        grant = _grant()
        member = _member(grant, 1)
        identities = (
            _identity(grant, member, 0),
            _identity(grant, member, 1),
        )
        records = (
            _record(
                identities[0],
                feed_work_scheduler.CohortRecordTerminalReason.TERMINAL_ITEM_SKIP,
                detail="first detail",
            ),
            _record(
                identities[1],
                feed_work_scheduler.CohortRecordTerminalReason.TERMINAL_ITEM_SKIP,
                detail="second detail",
            ),
        )
        cohorts = (
            _facts(
                records,
                feed_work_scheduler.CohortTerminalDisposition.SETTLED,
            ),
        )
        seen: list[failure_classification.ItemFailure] = []
        original = failure_classification.ItemBatchOutcome.record_failure

        def record_failure(
            instance: failure_classification.ItemBatchOutcome,
            failure: failure_classification.ItemFailure,
        ) -> None:
            seen.append(failure)
            original(instance, failure)

        with mock.patch.object(
            failure_classification.ItemBatchOutcome,
            "record_failure",
            new=record_failure,
        ):
            verdict = boundary_verdict.decide_page_verdict(
                _page(grant, (member,), cohorts)
            )

        self.assertEqual(
            seen,
            [
                failure_classification.ItemFailure(
                    feed_store.FeedStatusReason.SOURCE_UNREACHABLE,
                    "first detail",
                ),
                failure_classification.ItemFailure(
                    feed_store.FeedStatusReason.SOURCE_UNREACHABLE,
                    "second detail",
                ),
            ],
        )
        self.assertEqual(
            verdict.failed_feeds[0].failure.status_reason,
            feed_store.FeedStatusReason.SOURCE_UNREACHABLE,
        )
        self.assertEqual(
            verdict.failed_feeds[0].failure.detail,
            "first detail",
        )
        self.assertEqual(verdict.item_to_feed_promotion_count, 1)

    def test_mixed_item_failure_statuses_use_exact_existing_reason(
        self,
    ) -> None:
        grant = _grant()
        member = _member(grant, 1)
        records = (
            _record(
                _identity(grant, member, 0),
                feed_work_scheduler.CohortRecordTerminalReason.TERMINAL_ITEM_SKIP,
                status_reason=feed_store.FeedStatusReason.SOURCE_UNREACHABLE,
                detail="same detail",
            ),
            _record(
                _identity(grant, member, 1),
                feed_work_scheduler.CohortRecordTerminalReason.TERMINAL_ITEM_SKIP,
                status_reason=feed_store.FeedStatusReason.SOURCE_RATE_LIMITED,
                detail="same detail",
            ),
        )
        cohorts = (
            _facts(
                records,
                feed_work_scheduler.CohortTerminalDisposition.SETTLED,
            ),
        )

        verdict = boundary_verdict.decide_page_verdict(
            _page(grant, (member,), cohorts)
        )

        failure = verdict.failed_feeds[0].failure
        self.assertEqual(
            failure.status_reason,
            feed_store.FeedStatusReason.SYSTEM_COLLECTOR_ERROR,
        )
        self.assertEqual(
            failure.detail,
            failure_classification.MIXED_ITEM_FAILURE_REASON,
        )
        self.assertEqual(verdict.item_to_feed_promotion_count, 1)

    def test_isolated_item_failure_with_full_success_stays_item_local(
        self,
    ) -> None:
        grant = _grant()
        member = _member(grant, 1)
        records = (
            _record(
                _identity(grant, member, 0),
                feed_work_scheduler.CohortRecordTerminalReason.TERMINAL_ITEM_SKIP,
            ),
            _record(
                _identity(grant, member, 1),
                feed_work_scheduler.CohortRecordTerminalReason.FULL_PIPELINE,
            ),
        )
        cohorts = (
            _facts(
                records,
                feed_work_scheduler.CohortTerminalDisposition.SETTLED,
            ),
        )

        verdict = boundary_verdict.decide_page_verdict(
            _page(grant, (member,), cohorts)
        )

        self.assertEqual(verdict.failed_feeds, ())
        self.assertEqual(verdict.successful_feed_ids, (member.feed_id,))
        self.assertEqual(verdict.item_to_feed_promotion_count, 0)


class TestDirectFailureAggregation(unittest.TestCase):
    """Direct pipeline failures dominate item-only promotion deterministically."""

    def test_two_direct_failures_common_status_use_frozen_first_detail(
        self,
    ) -> None:
        grant = _grant()
        member = _member(grant, 1)
        status = (
            feed_store.FeedStatusReason.PIPELINE_PUBLISH_AFTER_BOOKMARK_FAILED
        )
        cohorts = (
            _one_record_feed(
                grant,
                member,
                0,
                feed_work_scheduler.CohortRecordTerminalReason.PUBLICATION_EXHAUSTED,
                status_reason=status,
                detail="first publication",
            ),
            _one_record_feed(
                grant,
                member,
                1,
                feed_work_scheduler.CohortRecordTerminalReason.PUBLICATION_EXHAUSTED,
                status_reason=status,
                detail="second publication",
            ),
        )

        verdict = boundary_verdict.decide_page_verdict(
            _page(grant, (member,), cohorts)
        )

        feed = verdict.feed_facts[0]
        self.assertEqual(len(feed.direct_failures), 2)
        self.assertEqual(
            tuple(
                evidence.identity.local_sequence
                for evidence in feed.direct_failures
            ),
            (0, 1),
        )
        self.assertIsNotNone(feed.selected_failure)
        selected_failure = typing.cast(
            "boundary_verdict.BoundaryFailure",
            feed.selected_failure,
        )
        self.assertEqual(selected_failure.status_reason, status)
        self.assertEqual(selected_failure.detail, "first publication")
        self.assertEqual(verdict.failed_feed_count, 1)
        self.assertEqual(verdict.item_to_feed_promotion_count, 0)

    def test_two_direct_failures_mixed_status_ignore_identical_detail(
        self,
    ) -> None:
        grant = _grant()
        member = _member(grant, 1)
        detail = "identical detail"
        cohorts = (
            _one_record_feed(
                grant,
                member,
                0,
                feed_work_scheduler.CohortRecordTerminalReason.PUBLICATION_EXHAUSTED,
                status_reason=(
                    feed_store.FeedStatusReason.PIPELINE_PUBLISH_AFTER_BOOKMARK_FAILED
                ),
                detail=detail,
            ),
            _one_record_feed(
                grant,
                member,
                1,
                feed_work_scheduler.CohortRecordTerminalReason.PUBLICATION_EXHAUSTED,
                status_reason=feed_store.FeedStatusReason.SYSTEM_PIPELINE_ERROR,
                detail=detail,
            ),
        )

        verdict = boundary_verdict.decide_page_verdict(
            _page(grant, (member,), cohorts)
        )

        failure = verdict.failed_feeds[0].failure
        self.assertEqual(
            failure.status_reason,
            feed_store.FeedStatusReason.SYSTEM_COLLECTOR_ERROR,
        )
        self.assertEqual(
            failure.detail,
            boundary_verdict.MIXED_DIRECT_FAILURE_REASON,
        )
        self.assertEqual(verdict.item_to_feed_promotion_count, 0)

    def test_direct_failure_overrides_item_aggregation_and_count(self) -> None:
        grant = _grant()
        member = _member(grant, 1)
        cohorts = (
            _one_record_feed(
                grant,
                member,
                0,
                feed_work_scheduler.CohortRecordTerminalReason.TERMINAL_ITEM_SKIP,
                status_reason=feed_store.FeedStatusReason.SOURCE_UNREACHABLE,
            ),
            _one_record_feed(
                grant,
                member,
                1,
                feed_work_scheduler.CohortRecordTerminalReason.PUBLICATION_EXHAUSTED,
                status_reason=feed_store.FeedStatusReason.SYSTEM_PIPELINE_ERROR,
                detail="direct pipeline failure",
            ),
        )

        verdict = boundary_verdict.decide_page_verdict(
            _page(grant, (member,), cohorts)
        )

        self.assertEqual(
            verdict.failed_feeds[0].origin,
            boundary_verdict.FeedFailureOrigin.DIRECT_PIPELINE,
        )
        self.assertEqual(
            verdict.failed_feeds[0].failure.status_reason,
            feed_store.FeedStatusReason.SYSTEM_PIPELINE_ERROR,
        )
        self.assertEqual(verdict.item_to_feed_promotion_count, 0)

    def test_duplicate_precommit_direct_stage_key_is_rejected(self) -> None:
        grant = _grant()
        member = _member(grant, 1)
        timestamp = _SOURCE_TIME
        records = tuple(
            _record(
                _identity(
                    grant,
                    member,
                    source_order,
                    cohort_timestamp=timestamp,
                ),
                feed_work_scheduler.CohortRecordTerminalReason.REPLAYABLE_DIRECT,
                status_reason=feed_store.FeedStatusReason.SYSTEM_PIPELINE_ERROR,
            )
            for source_order in (0, 1)
        )
        cohorts = (
            _facts(
                records,
                feed_work_scheduler.CohortTerminalDisposition.REPLAYABLE_DIRECT,
            ),
        )
        page = _page(grant, (member,), cohorts)

        with self.assertRaisesRegex(
            boundary_verdict.BoundaryVerdictIntegrityError,
            "duplicate direct-failure",
        ):
            boundary_verdict.decide_page_verdict(page)

    def test_one_precommit_direct_stage_is_bounded_by_record_facts(
        self,
    ) -> None:
        grant = _grant()
        member = _member(grant, 1)
        records = (
            _record(
                _identity(grant, member, 0),
                feed_work_scheduler.CohortRecordTerminalReason.REPLAYABLE_DIRECT,
                status_reason=feed_store.FeedStatusReason.SYSTEM_PIPELINE_ERROR,
            ),
            _record(
                _identity(grant, member, 1),
                feed_work_scheduler.CohortRecordTerminalReason.RETRYABLE,
                participated=False,
            ),
        )
        cohorts = (
            _facts(
                records,
                feed_work_scheduler.CohortTerminalDisposition.REPLAYABLE_DIRECT,
            ),
        )

        verdict = boundary_verdict.decide_page_verdict(
            _page(grant, (member,), cohorts)
        )

        self.assertEqual(verdict.participating_feed_count, 1)
        self.assertEqual(verdict.failed_feed_count, 1)
        self.assertEqual(len(verdict.feed_facts[0].direct_failures), 1)
        self.assertTrue(verdict.feed_facts[0].replayable_record_observed)
        self.assertEqual(verdict.item_to_feed_promotion_count, 0)

    def test_precommit_direct_accepts_attempted_retryable_sibling(
        self,
    ) -> None:
        grant = _grant()
        member = _member(grant, 1)
        records = (
            _record(
                _identity(grant, member, 0),
                feed_work_scheduler.CohortRecordTerminalReason.RETRYABLE,
                participated=True,
            ),
            _record(
                _identity(grant, member, 1),
                feed_work_scheduler.CohortRecordTerminalReason.REPLAYABLE_DIRECT,
                status_reason=feed_store.FeedStatusReason.SYSTEM_PIPELINE_ERROR,
            ),
        )
        cohorts = (
            _facts(
                records,
                feed_work_scheduler.CohortTerminalDisposition.REPLAYABLE_DIRECT,
            ),
        )

        verdict = boundary_verdict.decide_page_verdict(
            _page(grant, (member,), cohorts)
        )

        self.assertEqual(verdict.feed_facts[0].attempted_count, 2)
        self.assertEqual(len(verdict.feed_facts[0].direct_failures), 1)
        self.assertEqual(verdict.failed_feed_count, 1)
        self.assertEqual(verdict.item_to_feed_promotion_count, 0)


class TestRecoveryDecisions(unittest.TestCase):
    """Recovery is explicit and independent from the newly charged scope."""

    def test_prior_child_and_lease_recovery_candidates_are_selected(
        self,
    ) -> None:
        grant = _grant()
        quiet = _member(grant, 1)
        successful = _member(grant, 2)
        failed = _member(grant, 3)
        cohorts = (
            _one_record_feed(
                grant,
                successful,
                0,
                feed_work_scheduler.CohortRecordTerminalReason.FULL_PIPELINE,
            ),
            _one_record_feed(
                grant,
                failed,
                1,
                feed_work_scheduler.CohortRecordTerminalReason.TERMINAL_ITEM_SKIP,
            ),
        )

        verdict = boundary_verdict.decide_page_verdict(
            _page(
                grant,
                (failed, quiet, successful),
                cohorts,
                child_recovery_candidates=(successful, failed, quiet),
                lease_recovery_candidate=True,
            )
        )

        self.assertEqual(
            tuple(
                member.feed_id for member in verdict.child_recovery_candidates
            ),
            (quiet.feed_id, successful.feed_id),
        )
        self.assertTrue(verdict.lease_recovery_candidate)
        self.assertFalse(verdict.preserved_lease_recovery)
        self.assertEqual(verdict.preserved_failed_children, ())

    def test_promoted_failed_children_and_lease_recovery_are_preserved(
        self,
    ) -> None:
        grant = _grant()
        first = _member(grant, 1)
        second = _member(grant, 2)
        cohorts = (
            _one_record_feed(
                grant,
                first,
                0,
                feed_work_scheduler.CohortRecordTerminalReason.TERMINAL_ITEM_SKIP,
            ),
            _one_record_feed(
                grant,
                second,
                1,
                feed_work_scheduler.CohortRecordTerminalReason.TERMINAL_ITEM_SKIP,
            ),
        )

        verdict = boundary_verdict.decide_page_verdict(
            _page(
                grant,
                (second, first),
                cohorts,
                child_recovery_candidates=(second, first),
                lease_recovery_candidate=True,
            )
        )

        self.assertEqual(verdict.child_recovery_candidates, ())
        self.assertEqual(
            tuple(
                member.feed_id for member in verdict.preserved_failed_children
            ),
            (first.feed_id, second.feed_id),
        )
        self.assertEqual(verdict.charged_feed_failures, ())
        self.assertFalse(verdict.lease_recovery_candidate)
        self.assertTrue(verdict.preserved_lease_recovery)

    def test_recovery_candidate_with_later_direct_failure_is_not_cleared(
        self,
    ) -> None:
        grant = _grant()
        member = _member(grant, 1)
        cohorts = (
            _one_record_feed(
                grant,
                member,
                0,
                feed_work_scheduler.CohortRecordTerminalReason.FULL_PIPELINE,
            ),
            _one_record_feed(
                grant,
                member,
                1,
                feed_work_scheduler.CohortRecordTerminalReason.PUBLICATION_EXHAUSTED,
                status_reason=feed_store.FeedStatusReason.SYSTEM_PIPELINE_ERROR,
            ),
        )

        verdict = boundary_verdict.decide_page_verdict(
            _page(
                grant,
                (member,),
                cohorts,
                child_recovery_candidates=(member,),
            )
        )

        self.assertEqual(verdict.successful_feed_ids, (member.feed_id,))
        self.assertEqual(verdict.failed_feed_count, 1)
        self.assertEqual(verdict.child_recovery_candidates, ())


class TestFinalMutationPlan(unittest.TestCase):
    """Pure final child matrix preserves the selected lifecycle scope."""

    def test_direct_failures_aggregate_to_one_cursor_bearing_transition(
        self,
    ) -> None:
        grant = _grant()
        failed = _member(grant, 901)
        quiet = _member(grant, 902)
        cohorts = (
            _one_record_feed(
                grant,
                failed,
                0,
                feed_work_scheduler.CohortRecordTerminalReason.FULL_PIPELINE,
            ),
            _one_record_feed(
                grant,
                failed,
                1,
                feed_work_scheduler.CohortRecordTerminalReason.PUBLICATION_EXHAUSTED,
                status_reason=(
                    feed_store.FeedStatusReason.SYSTEM_CONFIGURATION_INVALID
                ),
                detail="first direct failure",
            ),
            _one_record_feed(
                grant,
                failed,
                2,
                feed_work_scheduler.CohortRecordTerminalReason.PUBLICATION_EXHAUSTED,
                status_reason=(
                    feed_store.FeedStatusReason.SYSTEM_CONFIGURATION_INVALID
                ),
                detail="second direct failure",
            ),
        )
        verdict = boundary_verdict.decide_page_verdict(
            _page(
                grant,
                (failed, quiet),
                cohorts,
                child_recovery_candidates=(failed, quiet),
                lease_recovery_candidate=True,
            )
        )
        target = _SOURCE_TIME + datetime.timedelta(minutes=1)
        boundaries = tuple(
            feed_work_scheduler.BoundaryWork(member, target)
            for member in (failed, quiet)
        )
        policy = failure_policy.ConsumeFailureBudget(9, 17, 701)

        plan = boundary_verdict.plan_final_mutations(
            verdict,
            candidate_boundaries=boundaries,
            unresolved_replay_feed_ids=(),
            replay_blocked_feed_ids=(),
            locally_retired_members=(),
            requested_target=target,
            boundary_settled_utc=_SOURCE_TIME,
            budgeted_failure=policy,
        )

        self.assertEqual(len(plan.commands), 2)
        failure, observation = (command.mutation for command in plan.commands)
        self.assertIs(
            type(failure), ingestion_lease_store.FeedFailureTransition
        )
        assert isinstance(failure, ingestion_lease_store.FeedFailureTransition)
        self.assertIs(failure.member, failed)
        self.assertEqual(
            failure.action,
            ingestion_lease_store.BudgetedFailure(9, 17, 701),
        )
        self.assertEqual(failure.reason, "first direct failure")
        self.assertEqual(failure.completion_cursor, target)
        self.assertIs(
            failure.charge_mode,
            ingestion_lease_store.FeedFailureChargeMode.ONE_SHOT,
        )
        self.assertEqual(
            observation,
            ingestion_lease_store.SourceObservation(quiet, target),
        )
        self.assertIs(
            type(plan.lease_effect),
            ingestion_lease_store.FinalizeLeaseRecovery,
        )

    def test_no_progress_observes_only_successful_participants(self) -> None:
        grant = _grant()
        successful = _member(grant, 951)
        quiet = _member(grant, 952)
        verdict = boundary_verdict.decide_page_verdict(
            _page(
                grant,
                (successful, quiet),
                (
                    _one_record_feed(
                        grant,
                        successful,
                        0,
                        feed_work_scheduler.CohortRecordTerminalReason.FULL_PIPELINE,
                    ),
                ),
                child_recovery_candidates=(successful, quiet),
                lease_recovery_candidate=True,
            )
        )

        plan = boundary_verdict.plan_final_mutations(
            verdict,
            candidate_boundaries=(),
            unresolved_replay_feed_ids=(),
            replay_blocked_feed_ids=(),
            locally_retired_members=(),
            requested_target=None,
            boundary_settled_utc=_SOURCE_TIME,
            budgeted_failure=failure_policy.ConsumeFailureBudget(7, 15, 600),
        )

        self.assertEqual(
            tuple(command.mutation for command in plan.commands),
            (ingestion_lease_store.SourceObservation(successful, None),),
        )
        self.assertEqual(plan.boundary_command_count, 0)

    def test_promoted_page_uses_only_neutral_failed_child_progress(
        self,
    ) -> None:
        grant = _grant()
        failed_a = _member(grant, 1001)
        failed_b = _member(grant, 1002)
        quiet = _member(grant, 1003)
        cohorts = (
            _one_record_feed(
                grant,
                failed_a,
                0,
                feed_work_scheduler.CohortRecordTerminalReason.TERMINAL_ITEM_SKIP,
            ),
            _one_record_feed(
                grant,
                failed_b,
                1,
                feed_work_scheduler.CohortRecordTerminalReason.TERMINAL_ITEM_SKIP,
            ),
        )
        verdict = boundary_verdict.decide_page_verdict(
            _page(
                grant,
                (failed_a, failed_b, quiet),
                cohorts,
                child_recovery_candidates=(failed_a, failed_b, quiet),
                lease_recovery_candidate=True,
            )
        )
        target = _SOURCE_TIME + datetime.timedelta(minutes=1)
        boundaries = tuple(
            feed_work_scheduler.BoundaryWork(member, target)
            for member in (failed_a, failed_b, quiet)
        )

        plan = boundary_verdict.plan_final_mutations(
            verdict,
            candidate_boundaries=boundaries,
            unresolved_replay_feed_ids=(),
            replay_blocked_feed_ids=(),
            locally_retired_members=(),
            requested_target=target,
            boundary_settled_utc=_SOURCE_TIME,
            budgeted_failure=failure_policy.ConsumeFailureBudget(7, 15, 600),
        )

        self.assertIs(
            type(plan.lease_effect),
            ingestion_lease_store.NoLeaseEffect,
        )
        self.assertEqual(
            tuple(type(command.mutation) for command in plan.commands),
            (ingestion_lease_store.ClosedCohortProgress,) * 3,
        )
        self.assertEqual(
            tuple(command.mutation.member for command in plan.commands),
            (failed_a, failed_b, quiet),
        )


class TestFinalizableFactValidation(unittest.TestCase):
    """Only exact complete predecessor facts can reach verdict policy."""

    def test_every_executor_outcome_fact_disposition_is_closed(self) -> None:
        grant = _grant()
        accepted = []
        rejected = []
        for index, disposition in enumerate(
            feed_work_scheduler.CohortTerminalDisposition
        ):
            member = _member(grant, index + 1)
            identity = _identity(
                grant,
                member,
                index,
                cohort_timestamp=(
                    _SOURCE_TIME + datetime.timedelta(seconds=index)
                ),
            )
            reason = {
                feed_work_scheduler.CohortTerminalDisposition.SETTLED: (
                    feed_work_scheduler.CohortRecordTerminalReason.FULL_PIPELINE
                ),
                feed_work_scheduler.CohortTerminalDisposition.FINAL_CLOSURE_PENDING: (
                    feed_work_scheduler.CohortRecordTerminalReason.TERMINAL_ITEM_SKIP
                ),
                feed_work_scheduler.CohortTerminalDisposition.REPLAYABLE_DIRECT: (
                    feed_work_scheduler.CohortRecordTerminalReason.REPLAYABLE_DIRECT
                ),
                feed_work_scheduler.CohortTerminalDisposition.RETRYABLE: (
                    feed_work_scheduler.CohortRecordTerminalReason.RETRYABLE
                ),
                feed_work_scheduler.CohortTerminalDisposition.STOPPED: (
                    feed_work_scheduler.CohortRecordTerminalReason.STOPPED
                ),
                feed_work_scheduler.CohortTerminalDisposition.AUTHORITY_LOST: (
                    feed_work_scheduler.CohortRecordTerminalReason.AUTHORITY_LOST
                ),
                feed_work_scheduler.CohortTerminalDisposition.MEMBERSHIP_REJECTED: (
                    feed_work_scheduler.CohortRecordTerminalReason.MEMBERSHIP_REJECTED
                ),
                feed_work_scheduler.CohortTerminalDisposition.INTEGRITY_FAILURE: (
                    feed_work_scheduler.CohortRecordTerminalReason.INTEGRITY_FAILURE
                ),
                feed_work_scheduler.CohortTerminalDisposition.OUTCOME_UNKNOWN: (
                    feed_work_scheduler.CohortRecordTerminalReason.OUTCOME_UNKNOWN
                ),
            }[disposition]
            closure_state = (
                feed_work_scheduler.CohortRecordClosureState.DURABLY_CLOSED
            )
            participated = True
            status_reason = feed_store.FeedStatusReason.SOURCE_UNREACHABLE
            if disposition is (
                feed_work_scheduler.CohortTerminalDisposition.SETTLED
            ):
                reason = (
                    feed_work_scheduler.CohortRecordTerminalReason.FULL_PIPELINE
                )
            elif disposition is (
                feed_work_scheduler.CohortTerminalDisposition.FINAL_CLOSURE_PENDING
            ):
                reason = (
                    feed_work_scheduler.CohortRecordTerminalReason.TERMINAL_ITEM_SKIP
                )
                closure_state = (
                    feed_work_scheduler.CohortRecordClosureState.FINAL_CLOSURE_PENDING
                )
            elif disposition in {
                feed_work_scheduler.CohortTerminalDisposition.INTEGRITY_FAILURE,
                feed_work_scheduler.CohortTerminalDisposition.OUTCOME_UNKNOWN,
            }:
                closure_state = (
                    feed_work_scheduler.CohortRecordClosureState.OUTCOME_UNKNOWN
                )
            else:
                closure_state = (
                    feed_work_scheduler.CohortRecordClosureState.REPLAY_SAFE_RELEASE
                )
                participated = False
                if disposition is (
                    feed_work_scheduler.CohortTerminalDisposition.REPLAYABLE_DIRECT
                ):
                    participated = True
                    status_reason = (
                        feed_store.FeedStatusReason.SYSTEM_PIPELINE_ERROR
                    )
            outcome = _facts(
                (
                    _record(
                        identity,
                        reason,
                        closure_state=closure_state,
                        participated=participated,
                        status_reason=status_reason,
                    ),
                ),
                disposition,
            )
            try:
                page = _page(grant, (member,), (outcome,))
            except boundary_verdict.BoundaryVerdictIntegrityError:
                rejected.append(disposition)
            else:
                accepted.append(disposition)
                boundary_verdict.decide_page_verdict(page)

        self.assertEqual(
            set(accepted),
            {
                feed_work_scheduler.CohortTerminalDisposition.SETTLED,
                feed_work_scheduler.CohortTerminalDisposition.FINAL_CLOSURE_PENDING,
                feed_work_scheduler.CohortTerminalDisposition.REPLAYABLE_DIRECT,
            },
        )
        self.assertEqual(
            set(rejected),
            set(feed_work_scheduler.CohortTerminalDisposition) - set(accepted),
        )

    def test_every_terminal_reason_is_accepted_or_neutral_rejected(
        self,
    ) -> None:
        accepted_reasons = {
            feed_work_scheduler.CohortRecordTerminalReason.FULL_PIPELINE,
            feed_work_scheduler.CohortRecordTerminalReason.TERMINAL_ITEM_SKIP,
            feed_work_scheduler.CohortRecordTerminalReason.PUBLICATION_EXHAUSTED,
            feed_work_scheduler.CohortRecordTerminalReason.REPLAYABLE_DIRECT,
        }
        observed_accepted = set()
        observed_rejected = set()
        for index, reason in enumerate(
            feed_work_scheduler.CohortRecordTerminalReason
        ):
            grant = _grant(fencing_token=index + 1)
            member = _member(grant, 1)
            identity = _identity(grant, member, index)
            if reason in {
                feed_work_scheduler.CohortRecordTerminalReason.FULL_PIPELINE,
                feed_work_scheduler.CohortRecordTerminalReason.TERMINAL_ITEM_SKIP,
                feed_work_scheduler.CohortRecordTerminalReason.PUBLICATION_EXHAUSTED,
                feed_work_scheduler.CohortRecordTerminalReason.PUBLICATION_ABANDONED,
            }:
                disposition = (
                    feed_work_scheduler.CohortTerminalDisposition.SETTLED
                )
            elif reason is (
                feed_work_scheduler.CohortRecordTerminalReason.REPLAYABLE_DIRECT
            ):
                disposition = feed_work_scheduler.CohortTerminalDisposition.REPLAYABLE_DIRECT
            else:
                disposition = {
                    feed_work_scheduler.CohortRecordTerminalReason.RETRYABLE: (
                        feed_work_scheduler.CohortTerminalDisposition.RETRYABLE
                    ),
                    feed_work_scheduler.CohortRecordTerminalReason.STOPPED: (
                        feed_work_scheduler.CohortTerminalDisposition.STOPPED
                    ),
                    feed_work_scheduler.CohortRecordTerminalReason.AUTHORITY_LOST: (
                        feed_work_scheduler.CohortTerminalDisposition.AUTHORITY_LOST
                    ),
                    feed_work_scheduler.CohortRecordTerminalReason.MEMBERSHIP_REJECTED: (
                        feed_work_scheduler.CohortTerminalDisposition.MEMBERSHIP_REJECTED
                    ),
                    feed_work_scheduler.CohortRecordTerminalReason.INTEGRITY_FAILURE: (
                        feed_work_scheduler.CohortTerminalDisposition.INTEGRITY_FAILURE
                    ),
                    feed_work_scheduler.CohortRecordTerminalReason.OUTCOME_UNKNOWN: (
                        feed_work_scheduler.CohortTerminalDisposition.OUTCOME_UNKNOWN
                    ),
                }[reason]
            facts = _facts(
                (
                    _record(
                        identity,
                        reason,
                        participated=(
                            True
                            if reason
                            in {
                                feed_work_scheduler.CohortRecordTerminalReason.INTEGRITY_FAILURE,
                                feed_work_scheduler.CohortRecordTerminalReason.OUTCOME_UNKNOWN,
                            }
                            else None
                        ),
                    ),
                ),
                disposition,
            )
            try:
                page = _page(grant, (member,), (facts,))
            except boundary_verdict.BoundaryVerdictIntegrityError:
                observed_rejected.add(reason)
            else:
                observed_accepted.add(reason)
                boundary_verdict.decide_page_verdict(page)

        self.assertEqual(observed_accepted, accepted_reasons)
        self.assertEqual(
            observed_rejected,
            set(feed_work_scheduler.CohortRecordTerminalReason)
            - accepted_reasons,
        )

    def test_replay_blocked_quiet_and_membership_rejection_are_neutral(
        self,
    ) -> None:
        grant = _grant()
        quiet = _member(grant, 1)
        verdict = boundary_verdict.decide_page_verdict(
            _page(grant, (quiet,), ())
        )
        self.assertEqual(verdict.participating_feed_count, 0)
        self.assertEqual(verdict.failed_feed_count, 0)
        self.assertEqual(verdict.item_to_feed_promotion_count, 0)
        self.assertNotIn(
            "replay_blocked",
            tuple(field.name for field in dataclasses.fields(verdict)),
        )

        identity = _identity(grant, quiet, 0)
        membership = _facts(
            (
                _record(
                    identity,
                    feed_work_scheduler.CohortRecordTerminalReason.MEMBERSHIP_REJECTED,
                ),
            ),
            feed_work_scheduler.CohortTerminalDisposition.MEMBERSHIP_REJECTED,
        )
        with self.assertRaisesRegex(
            boundary_verdict.BoundaryVerdictIntegrityError,
            "not finalizable",
        ):
            _page(grant, (quiet,), (membership,))

    def test_missing_terminal_fact_is_rejected(self) -> None:
        grant = _grant()
        member = _member(grant, 1)
        first = _identity(grant, member, 0)
        second = _identity(grant, member, 1)
        cohorts = (
            _settled(
                _record(
                    first,
                    feed_work_scheduler.CohortRecordTerminalReason.FULL_PIPELINE,
                )
            ),
        )

        with self.assertRaisesRegex(
            boundary_verdict.BoundaryVerdictIntegrityError,
            "cover every admitted record",
        ):
            _page(
                grant,
                (member,),
                cohorts,
                record_identities=(first, second),
            )

    def test_forged_equal_record_identity_is_rejected(self) -> None:
        grant = _grant()
        member = _member(grant, 1)
        expected = _identity(grant, member, 0)
        forged = dataclasses.replace(expected)
        cohorts = (
            _settled(
                _record(
                    forged,
                    feed_work_scheduler.CohortRecordTerminalReason.FULL_PIPELINE,
                )
            ),
        )

        with self.assertRaisesRegex(
            boundary_verdict.BoundaryVerdictIntegrityError,
            "missing, forged, or out of order",
        ):
            _page(
                grant,
                (member,),
                cohorts,
                record_identities=(expected,),
            )

    def test_cross_page_grant_and_issued_member_mismatch_are_rejected(
        self,
    ) -> None:
        grant = _grant()
        successor = _grant(fencing_token=12)
        member = _member(grant, 1)
        distinct_member = _member(grant, 1)
        cases = (
            _identity(grant, member, 0, page_sequence=8),
            _identity(successor, _member(successor, 1), 0),
            _identity(grant, distinct_member, 0),
        )

        for case_index, identity in enumerate(cases):
            with self.subTest(case_index=case_index):
                cohorts = (
                    _settled(
                        _record(
                            identity,
                            feed_work_scheduler.CohortRecordTerminalReason.FULL_PIPELINE,
                        )
                    ),
                )
                with self.assertRaises(
                    boundary_verdict.BoundaryVerdictIntegrityError
                ):
                    _page(grant, (member,), cohorts)

    def test_unissued_member_binding_is_rejected(self) -> None:
        grant = _grant()
        forged = ingestion_lease_store.LeaseMemberIdentity(
            feed_id=uuid.UUID(int=1),
            source_type=feed_store.SourceType.BCFY_CALLS,
            source_feed_id="150-1",
            sid="150",
            group_id="1",
        )

        with self.assertRaisesRegex(
            boundary_verdict.BoundaryVerdictIntegrityError,
            "not issued",
        ):
            _page(grant, (forged,), ())

    def test_disposition_mismatched_terminal_fact_is_rejected(self) -> None:
        grant = _grant()
        member = _member(grant, 1)
        record = _record(
            _identity(grant, member, 0),
            feed_work_scheduler.CohortRecordTerminalReason.REPLAYABLE_DIRECT,
            status_reason=feed_store.FeedStatusReason.SYSTEM_PIPELINE_ERROR,
        )
        with self.assertRaises(feed_work_scheduler.CohortIntegrityError):
            _facts(
                (record,),
                feed_work_scheduler.CohortTerminalDisposition.SETTLED,
            )

    def test_out_of_order_terminal_cohorts_are_rejected(self) -> None:
        grant = _grant()
        first = _member(grant, 1)
        second = _member(grant, 2)
        first_facts = _one_record_feed(
            grant,
            first,
            0,
            feed_work_scheduler.CohortRecordTerminalReason.FULL_PIPELINE,
        )
        second_facts = _one_record_feed(
            grant,
            second,
            1,
            feed_work_scheduler.CohortRecordTerminalReason.FULL_PIPELINE,
        )
        expected = (
            first_facts.records[0].identity,
            second_facts.records[0].identity,
        )

        with self.assertRaisesRegex(
            boundary_verdict.BoundaryVerdictIntegrityError,
            "terminal cohorts are not",
        ):
            _page(
                grant,
                (first, second),
                (second_facts, first_facts),
                record_identities=expected,
            )

    def test_verdict_missing_timestamp_keeps_frozen_source_order(self) -> None:
        grant = _grant()
        member = _member(grant, 1)
        missing_timestamp = _settled(
            _record(
                _identity(
                    grant,
                    member,
                    0,
                    cohort_timestamp=None,
                ),
                feed_work_scheduler.CohortRecordTerminalReason.FULL_PIPELINE,
            )
        )
        timestamped = _settled(
            _record(
                _identity(
                    grant,
                    member,
                    1,
                    cohort_timestamp=_SOURCE_TIME,
                ),
                feed_work_scheduler.CohortRecordTerminalReason.FULL_PIPELINE,
            )
        )

        verdict = boundary_verdict.decide_page_verdict(
            _page(
                grant,
                (member,),
                (missing_timestamp, timestamped),
            )
        )

        self.assertEqual(
            tuple(
                fact.identity.local_sequence
                for fact in verdict.feed_facts[0].record_facts
            ),
            (0, 1),
        )

    def test_final_feed_uuid_and_direct_order_are_deterministic(self) -> None:
        grant = _grant()
        first = _member(grant, 1)
        second = _member(grant, 2)
        cohorts = (
            _one_record_feed(
                grant,
                first,
                0,
                feed_work_scheduler.CohortRecordTerminalReason.PUBLICATION_EXHAUSTED,
                status_reason=feed_store.FeedStatusReason.SYSTEM_PIPELINE_ERROR,
                detail="first",
            ),
            _one_record_feed(
                grant,
                first,
                1,
                feed_work_scheduler.CohortRecordTerminalReason.PUBLICATION_EXHAUSTED,
                status_reason=feed_store.FeedStatusReason.SYSTEM_PIPELINE_ERROR,
                detail="second",
            ),
            _one_record_feed(
                grant,
                second,
                2,
                feed_work_scheduler.CohortRecordTerminalReason.TERMINAL_ITEM_SKIP,
            ),
        )

        verdict = boundary_verdict.decide_page_verdict(
            _page(grant, (second, first), cohorts)
        )

        self.assertEqual(
            tuple(feed.member.feed_id for feed in verdict.feed_facts),
            (first.feed_id, second.feed_id),
        )
        self.assertEqual(
            tuple(decision.member.feed_id for decision in verdict.failed_feeds),
            (first.feed_id, second.feed_id),
        )
        self.assertEqual(
            tuple(
                evidence.identity.local_sequence
                for evidence in verdict.feed_facts[0].direct_failures
            ),
            (0, 1),
        )


class TestPureVerdictContract(unittest.TestCase):
    """The verdict has no I/O, logging, mutable state, or timing inputs."""

    def test_verdict_is_frozen_slotted_and_repeatably_pure(self) -> None:
        grant = _grant()
        member = _member(grant, 1)
        cohorts = (
            _one_record_feed(
                grant,
                member,
                0,
                feed_work_scheduler.CohortRecordTerminalReason.FULL_PIPELINE,
            ),
        )
        page = _page(
            grant,
            (member,),
            cohorts,
            child_recovery_candidates=(member,),
            lease_recovery_candidate=True,
        )

        first = boundary_verdict.decide_page_verdict(page)
        second = boundary_verdict.decide_page_verdict(page)

        self.assertEqual(first, second)
        self.assertFalse(hasattr(first, "__dict__"))
        self.assertFalse(hasattr(first.feed_facts[0], "__dict__"))
        with self.assertRaises(dataclasses.FrozenInstanceError):
            _assign_attribute(first, "failed_feed_count", 99)

    def test_verdict_module_has_no_io_logging_or_mutable_policy_state(
        self,
    ) -> None:
        source = inspect.getsource(boundary_verdict)
        self.assertNotIn("import logging", source)
        self.assertNotIn("async def ", source)
        self.assertNotIn("open(", source)
        self.assertNotIn("global ", source)
        policy_globals = {
            name: value
            for name, value in vars(boundary_verdict).items()
            if name.startswith("_")
            and not name.startswith("__")
            and not inspect.ismodule(value)
            and not inspect.isclass(value)
            and not inspect.isfunction(value)
        }
        self.assertFalse(
            any(
                isinstance(value, (list, dict, set))
                for value in policy_globals.values()
            )
        )


if __name__ == "__main__":
    unittest.main()
