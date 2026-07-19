"""Stable domain-contract tests for Feed-affine scheduling."""

from __future__ import annotations

import datetime
import uuid

import pytest

from backend.pipeline.ingestion import feed_work_scheduler
from backend.pipeline.ingestion.feed_work_scheduler import _types
from backend.pipeline.storage import feed_store, ingestion_lease_store

_OWNER_ID = uuid.UUID("11111111-2222-3333-4444-555555555555")
_FEED_ID = uuid.UUID("00000000-0000-0000-0000-000000000001")
_SOURCE_TIME = datetime.datetime(2026, 7, 12, 12, 0, tzinfo=datetime.UTC)


def _identity() -> feed_work_scheduler.CohortRecordIdentity:
    grant = ingestion_lease_store.LeaseGrant(
        source_type=feed_store.SourceType.BCFY_CALLS,
        lease_key="150",
        owner_worker_id=_OWNER_ID,
        fencing_token=1,
    )
    member = ingestion_lease_store._issue_member_identity(
        grant,
        feed_id=_FEED_ID,
        source_type=feed_store.SourceType.BCFY_CALLS,
        source_feed_id="150-1",
    )
    return feed_work_scheduler.CohortRecordIdentity(
        grant=grant,
        member=member,
        page_sequence=0,
        feed_id=_FEED_ID,
        cohort_timestamp=_SOURCE_TIME,
        local_sequence=0,
    )


def test_production_scheduler_limits_are_exact() -> None:
    assert _types.PRODUCTION_SHARD_COUNT == 8
    assert _types.PRODUCTION_SHARD_CAPACITY == 500
    assert _types.PRODUCTION_WORKERS_PER_SHARD == 4
    assert _types.PRODUCTION_HIGH_WATER == 400
    assert _types.PRODUCTION_RESUME_AT == 299


def test_record_identity_uses_only_functional_ordering_state() -> None:
    identity = _identity()

    assert identity.local_sequence == 0
    assert not hasattr(identity, "source_order")


def test_terminal_facts_are_the_complete_executor_result() -> None:
    identity = _identity()
    facts = feed_work_scheduler.CohortTerminalFacts(
        records=(
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
            ),
        ),
        disposition=feed_work_scheduler.CohortTerminalDisposition.SETTLED,
    )

    assert facts.identities == (identity,)
    assert not hasattr(feed_work_scheduler, "CallCompleted")


def test_disposition_rejects_incompatible_record_closure() -> None:
    identity = _identity()
    replay_record = feed_work_scheduler.CohortRecordTerminalFact(
        identity=identity,
        participated=False,
        closure_state=(
            feed_work_scheduler.CohortRecordClosureState.REPLAY_SAFE_RELEASE
        ),
        full_pipeline_completed=False,
        terminal_reason=(
            feed_work_scheduler.CohortRecordTerminalReason.RETRYABLE
        ),
    )

    with pytest.raises(feed_work_scheduler.CohortIntegrityError):
        feed_work_scheduler.CohortTerminalFacts(
            records=(replay_record,),
            disposition=feed_work_scheduler.CohortTerminalDisposition.SETTLED,
        )
