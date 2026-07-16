"""Exact obligation and emission tests for missing Broadcastify calls."""

from __future__ import annotations

import dataclasses
import datetime
import logging
import uuid
from unittest import mock

import pytest

from backend.pipeline.ingestion import feed_work_scheduler, slo_contract
from backend.pipeline.ingestion.collectors.bcfy_calls import (
    boundary_verdict,
    cursor_policy,
    gap_ledger,
    runtime_adapters,
    telemetry,
)
from backend.pipeline.storage import feed_store, ingestion_lease_store

_NOW = datetime.datetime(2026, 7, 13, 12, 0, tzinfo=datetime.UTC)
_TARGET = _NOW + datetime.timedelta(seconds=30)
_WORKER_ID = uuid.UUID("00000000-0000-0000-0000-000000001001")
_FEED_A = uuid.UUID("00000000-0000-0000-0000-000000001002")
_FEED_B = uuid.UUID("00000000-0000-0000-0000-000000001003")
_URL = "https://audio.example.invalid/call.flac?signature=secret"


def _grant() -> ingestion_lease_store.LeaseGrant:
    return ingestion_lease_store.LeaseGrant(
        feed_store.SourceType.BCFY_CALLS,
        "123",
        _WORKER_ID,
        10,
    )


def _member(
    grant: ingestion_lease_store.LeaseGrant,
    *,
    feed_id: uuid.UUID = _FEED_A,
    group_id: str = "45",
) -> ingestion_lease_store.LeaseMember:
    identity = ingestion_lease_store._issue_member_identity(
        grant,
        feed_id=feed_id,
        source_type=feed_store.SourceType.BCFY_CALLS,
        source_feed_id=f"123-{group_id}",
        sid="123",
        group_id=group_id,
    )
    return ingestion_lease_store.LeaseMember(
        identity=identity,
        name=f"Calls Feed {group_id}",
        last_bookmark_time=None,
    )


def _identity(
    grant: ingestion_lease_store.LeaseGrant,
    member: ingestion_lease_store.LeaseMember,
    *,
    source_order: int = 0,
    local_sequence: int = 80,
    timestamp: datetime.datetime | None = _NOW,
) -> feed_work_scheduler.CohortRecordIdentity:
    return feed_work_scheduler.CohortRecordIdentity(
        grant,
        member.identity,
        page_sequence=0,
        feed_id=member.identity.feed_id,
        cohort_timestamp=timestamp,
        source_order=source_order,
        local_sequence=local_sequence,
    )


def _draft(
    ledger: gap_ledger.MissingCallLedger,
    member: ingestion_lease_store.LeaseMember,
    *,
    source_order: int = 0,
    timestamp: datetime.datetime | None = _NOW,
    audio_url: str = _URL,
) -> gap_ledger.MissingCallObligation:
    return ledger.draft(
        member,
        audio_url=audio_url,
        provider_ts=timestamp,
        source_order=source_order,
    )


def _scheduler_evidence(
    record_count: int = 1,
) -> feed_work_scheduler.SchedulerPageEvidence:
    return feed_work_scheduler.SchedulerPageEvidence(
        admitted_record_count=record_count,
        admitted_cohort_count=int(record_count > 0),
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
        maximum_worker_utilization_numerator=1,
        worker_utilization_denominator=32,
        early_flush_attempt_count=0,
        final_flush_attempt_count=1,
        total_flush_latency_seconds=0.0,
        maximum_flush_latency_seconds=0.0,
        fence_rejection_count=0,
        member_rejection_count=0,
    )


def _verdict(
    grant: ingestion_lease_store.LeaseGrant,
    member: ingestion_lease_store.LeaseMember,
) -> boundary_verdict.PageVerdict:
    return boundary_verdict.decide_page_verdict(
        boundary_verdict.PageVerdictInput(
            grant=grant,
            page_sequence=0,
            members=(member.identity,),
            record_identities=(),
            cohort_terminal_facts=(),
            child_recovery_candidates=(member.identity,),
            lease_recovery_candidate=True,
        )
    )


def _closure_cap(
    grant: ingestion_lease_store.LeaseGrant,
    member: ingestion_lease_store.LeaseMember,
) -> runtime_adapters._FeedClosureCap:
    result = ingestion_lease_store.ChildMutationResult(
        feed_id=member.identity.feed_id,
        disposition=ingestion_lease_store.ChildDisposition.APPLIED,
        cursor_effect=ingestion_lease_store.CursorEffect.ADVANCED,
        lifecycle_effect=ingestion_lease_store.LifecycleEffect.NONE,
    )
    kind = runtime_adapters._FinalChildCommandKind.OBSERVATION
    seal = runtime_adapters._FeedClosureCapSeal(
        grant=grant,
        page_sequence=0,
        feed_id=member.identity.feed_id,
        accepted_through=_TARGET,
        requested_target=_TARGET,
        command_kind=kind,
        result=result,
        _construction_key=(runtime_adapters._FEED_CLOSURE_CAP_CONSTRUCTION_KEY),
    )
    return runtime_adapters._FeedClosureCap(
        grant=grant,
        page_sequence=0,
        feed_id=member.identity.feed_id,
        accepted_through=_TARGET,
        requested_target=_TARGET,
        command_kind=kind,
        result=result,
        _seal=seal,
    )


def _settled_page(
    grant: ingestion_lease_store.LeaseGrant,
    member: ingestion_lease_store.LeaseMember,
    resolution: feed_work_scheduler.FinalRecordClosureResolution,
    *,
    cap: runtime_adapters._FeedClosureCap | None = None,
    no_progress: bool = False,
    replayable: bool = False,
    record_count: int = 1,
) -> feed_work_scheduler.SettledPage:
    cursor = cursor_policy.LeaseCursor(grant, pos=None)
    if no_progress:
        candidate = cursor.prepare_no_progress()
        settlement = cursor_policy._issue_no_progress_page(candidate)
    else:
        candidate = cursor.prepare(_TARGET)
        settlement = (
            cursor_policy._issue_replayable_page(candidate)
            if replayable
            else cursor_policy._issue_covered_page(candidate)
        )
    source_evidence = runtime_adapters.PageFinalizationEvidence(
        verdict=_verdict(grant, member),
        closure_caps=(() if cap is None else (cap,)),
        quarantined_feed_ids=(),
        item_to_feed_promotion_count=0,
    )
    return feed_work_scheduler.SettledPage(
        candidate=candidate,
        lease_settlement=settlement,
        final_closure_resolutions=(resolution,),
        scheduler_evidence=_scheduler_evidence(record_count),
        source_evidence=source_evidence,
    )


def _observe_final_resolution(
    ledger: gap_ledger.MissingCallLedger,
    obligation: gap_ledger.MissingCallObligation,
    resolution: feed_work_scheduler.FinalRecordClosureResolution,
) -> None:
    settlement = (
        feed_work_scheduler.CallSettlement.COMPLETED
        if resolution.closure_state
        is feed_work_scheduler.CohortRecordClosureState.DURABLY_CLOSED
        else feed_work_scheduler.CallSettlement.REPLAY_SAFE_RELEASE
    )
    ledger.observe_settlement(obligation, settlement)


def test_admission_is_atomic_and_binds_the_exact_identity_tuple() -> None:
    grant = _grant()
    member = _member(grant)
    ledger = gap_ledger.MissingCallLedger(grant, 0)
    first = _draft(ledger, member)
    second = _draft(
        ledger,
        member,
        source_order=1,
        audio_url=f"{_URL}-2",
    )
    first_identity = _identity(grant, member)
    crossed = _identity(grant, member, source_order=2, local_sequence=81)

    with pytest.raises(gap_ledger.MissingCallIntegrityError):
        ledger.admit((first, second), (first_identity, crossed))

    assert ledger.state(first) is gap_ledger.MissingCallState.DRAFT
    assert ledger.state(second) is gap_ledger.MissingCallState.DRAFT

    second_identity = _identity(
        grant,
        member,
        source_order=1,
        local_sequence=81,
    )
    ledger.admit((first, second), (first_identity, second_identity))

    assert ledger.state(first) is gap_ledger.MissingCallState.ADMITTED
    assert ledger.state(second) is gap_ledger.MissingCallState.ADMITTED
    with pytest.raises(gap_ledger.MissingCallIntegrityError):
        ledger.admit((first,), (first_identity,))


def test_local_sequence_is_shard_local_but_unique_within_one_feed() -> None:
    grant = _grant()
    member_a = _member(grant)
    member_b = _member(grant, feed_id=_FEED_B, group_id="46")
    ledger = gap_ledger.MissingCallLedger(grant, 0)
    first = _draft(ledger, member_a)
    sibling = _draft(
        ledger,
        member_b,
        source_order=1,
        audio_url=f"{_URL}-sibling",
    )
    duplicate = _draft(
        ledger,
        member_a,
        source_order=2,
        audio_url=f"{_URL}-duplicate",
    )

    ledger.admit((first,), (_identity(grant, member_a),))
    ledger.admit(
        (sibling,),
        (_identity(grant, member_b, source_order=1),),
    )

    assert ledger.state(first) is gap_ledger.MissingCallState.ADMITTED
    assert ledger.state(sibling) is gap_ledger.MissingCallState.ADMITTED
    with pytest.raises(gap_ledger.MissingCallIntegrityError):
        ledger.admit(
            (duplicate,),
            (_identity(grant, member_a, source_order=2),),
        )
    assert ledger.state(duplicate) is gap_ledger.MissingCallState.DRAFT


def test_cross_ledger_handle_and_equal_but_replaced_identity_fail_closed() -> (
    None
):
    grant = _grant()
    member = _member(grant)
    first_ledger = gap_ledger.MissingCallLedger(grant, 0)
    second_ledger = gap_ledger.MissingCallLedger(grant, 0)
    obligation = _draft(first_ledger, member)
    identity = _identity(grant, member)

    with pytest.raises(gap_ledger.MissingCallIntegrityError):
        second_ledger.admit((obligation,), (identity,))

    first_ledger.admit((obligation,), (identity,))
    with pytest.raises(gap_ledger.MissingCallIntegrityError):
        first_ledger.resolve_success(
            obligation,
            dataclasses.replace(identity),
        )
    assert first_ledger.state(obligation) is (
        gap_ledger.MissingCallState.ADMITTED
    )


def test_timestamped_skip_emits_once_only_after_exact_physical_cursor() -> None:
    grant = _grant()
    member = _member(grant)
    ledger = gap_ledger.MissingCallLedger(grant, 0)
    obligation = _draft(ledger, member)
    identity = _identity(grant, member)
    ledger.admit((obligation,), (identity,))
    failure = feed_work_scheduler.CohortItemFailureFact(
        feed_store.FeedStatusReason.SOURCE_UNREACHABLE,
        "item_download_failed",
    )

    with mock.patch.object(telemetry.logger, "log") as emit:
        ledger.mark_terminal_skip(obligation, identity, failure, 3)
        assert ledger.state(obligation) is gap_ledger.MissingCallState.PENDING
        emit.assert_not_called()

        with pytest.raises(gap_ledger.MissingCallIntegrityError):
            ledger.close_timestamped_skip(
                obligation,
                identity,
                _NOW - datetime.timedelta(microseconds=1),
            )
        emit.assert_not_called()

        ledger.close_timestamped_skip(obligation, identity, _NOW)

        assert ledger.state(obligation) is gap_ledger.MissingCallState.EMITTED
        emit.assert_called_once()
        level, message = emit.call_args.args
        fields = emit.call_args.kwargs["extra"]["json_fields"]
        assert level == logging.WARNING
        assert message == "Broadcastify Calls missing call"
        assert fields == {
            "audio_url": _URL,
            "attempt_count": 3,
            "event_type": "bcfy_calls_missing_call",
            "feed_id": str(_FEED_A),
            "feed_name": "Calls Feed 45",
            "gap_stage": "terminal_item_skip",
            "group_id": "45",
            "provider_ts": _NOW.isoformat(),
            "reason": "item_download_failed",
            "schema_version": 1,
            "sid": "123",
            "source_feed_id": "123-45",
            "source_type": "bcfy_calls",
            "status_reason": "source_unreachable",
        }
        with pytest.raises(gap_ledger.MissingCallIntegrityError):
            ledger.close_timestamped_skip(obligation, identity, _NOW)
        emit.assert_called_once()


def test_missing_timestamp_closes_only_with_own_feed_cap_and_resolution() -> (
    None
):
    grant = _grant()
    member = _member(grant)
    ledger = gap_ledger.MissingCallLedger(grant, 0)
    obligation = _draft(ledger, member, timestamp=None)
    identity = _identity(grant, member, timestamp=None)
    ledger.admit((obligation,), (identity,))
    ledger.mark_terminal_skip(
        obligation,
        identity,
        feed_work_scheduler.CohortItemFailureFact(
            feed_store.FeedStatusReason.SOURCE_UNREACHABLE,
            "item_download_failed",
        ),
        1,
    )
    resolution = feed_work_scheduler.FinalRecordClosureResolution(
        identity,
        feed_work_scheduler.CohortRecordClosureState.DURABLY_CLOSED,
        feed_work_scheduler.FinalRecordReleaseBasis.DURABLE_SOURCE_CLOSURE,
    )

    with mock.patch.object(telemetry.logger, "log") as emit:
        _observe_final_resolution(ledger, obligation, resolution)
        assert ledger.state(obligation) is (
            gap_ledger.MissingCallState.FINAL_CLOSURE_PENDING
        )
        emit.assert_not_called()
        ledger.close_page(
            _settled_page(
                grant,
                member,
                resolution,
                cap=_closure_cap(grant, member),
            )
        )

    assert ledger.state(obligation) is gap_ledger.MissingCallState.EMITTED
    emit.assert_called_once()
    assert emit.call_args.kwargs["extra"]["json_fields"]["provider_ts"] is (
        None
    )
    with pytest.raises(gap_ledger.MissingCallIntegrityError):
        ledger.close_page(
            _settled_page(
                grant,
                member,
                resolution,
                cap=_closure_cap(grant, member),
            )
        )


def test_quiet_sibling_cap_is_valid_but_cannot_replace_own_feed_cap() -> None:
    grant = _grant()
    member = _member(grant)
    quiet_sibling = _member(grant, feed_id=_FEED_B, group_id="46")
    ledger = gap_ledger.MissingCallLedger(grant, 0)
    obligation = _draft(ledger, member, timestamp=None)
    identity = _identity(grant, member, timestamp=None)
    ledger.admit((obligation,), (identity,))
    ledger.mark_terminal_skip(
        obligation,
        identity,
        feed_work_scheduler.CohortItemFailureFact(
            feed_store.FeedStatusReason.SOURCE_UNREACHABLE,
            "item_download_failed",
        ),
        1,
    )
    resolution = feed_work_scheduler.FinalRecordClosureResolution(
        identity,
        feed_work_scheduler.CohortRecordClosureState.DURABLY_CLOSED,
        feed_work_scheduler.FinalRecordReleaseBasis.DURABLE_SOURCE_CLOSURE,
    )
    _observe_final_resolution(ledger, obligation, resolution)
    page = _settled_page(
        grant,
        member,
        resolution,
        cap=_closure_cap(grant, member),
    )
    source_evidence = page.source_evidence
    assert isinstance(
        source_evidence,
        runtime_adapters.PageFinalizationEvidence,
    )
    page = dataclasses.replace(
        page,
        source_evidence=dataclasses.replace(
            source_evidence,
            closure_caps=(
                _closure_cap(grant, member),
                _closure_cap(grant, quiet_sibling),
            ),
        ),
    )

    with mock.patch.object(telemetry.logger, "log") as emit:
        ledger.close_page(page)

    assert ledger.state(obligation) is gap_ledger.MissingCallState.EMITTED
    emit.assert_called_once()


def test_unadmitted_replay_release_closes_with_zero_scheduler_records() -> None:
    grant = _grant()
    member = _member(grant)
    ledger = gap_ledger.MissingCallLedger(grant, 0)
    obligation = _draft(ledger, member, timestamp=None)
    ledger.observe_settlement(
        obligation,
        feed_work_scheduler.CallSettlement.REPLAY_BLOCKED,
    )
    identity = _identity(grant, member, timestamp=None)
    placeholder = feed_work_scheduler.FinalRecordClosureResolution(
        identity,
        feed_work_scheduler.CohortRecordClosureState.REPLAY_SAFE_RELEASE,
        feed_work_scheduler.FinalRecordReleaseBasis.ACCEPTED_NO_PROGRESS,
    )
    page = dataclasses.replace(
        _settled_page(
            grant,
            member,
            placeholder,
            no_progress=True,
        ),
        final_closure_resolutions=(),
        scheduler_evidence=_scheduler_evidence(0),
    )

    ledger.close_page(page)

    assert ledger.state(obligation) is (
        gap_ledger.MissingCallState.REPLAY_RELEASED
    )


def test_page_close_requires_exact_scheduler_record_conservation() -> None:
    grant = _grant()
    member = _member(grant)
    ledger = gap_ledger.MissingCallLedger(grant, 0)
    obligation = _draft(ledger, member, timestamp=None)
    identity = _identity(grant, member, timestamp=None)
    ledger.admit((obligation,), (identity,))
    ledger.mark_terminal_skip(
        obligation,
        identity,
        feed_work_scheduler.CohortItemFailureFact(
            feed_store.FeedStatusReason.SOURCE_UNREACHABLE,
            "item_download_failed",
        ),
        1,
    )
    resolution = feed_work_scheduler.FinalRecordClosureResolution(
        identity,
        feed_work_scheduler.CohortRecordClosureState.DURABLY_CLOSED,
        feed_work_scheduler.FinalRecordReleaseBasis.DURABLE_SOURCE_CLOSURE,
    )
    _observe_final_resolution(ledger, obligation, resolution)

    with pytest.raises(
        gap_ledger.MissingCallIntegrityError,
        match="record counts disagree",
    ):
        ledger.close_page(
            _settled_page(
                grant,
                member,
                resolution,
                cap=_closure_cap(grant, member),
                record_count=2,
            )
        )

    assert ledger.state(obligation) is (
        gap_ledger.MissingCallState.FINAL_CLOSURE_PENDING
    )


@pytest.mark.parametrize(
    ("basis", "settlement_kind"),
    [
        (
            feed_work_scheduler.FinalRecordReleaseBasis.ACCEPTED_NO_PROGRESS,
            "no_progress",
        ),
        (
            feed_work_scheduler.FinalRecordReleaseBasis.ACCEPTED_REPLAYABLE_FEED,
            "replayable",
        ),
        (
            feed_work_scheduler.FinalRecordReleaseBasis.ACCEPTED_MEMBER_RETIREMENT,
            "covered",
        ),
    ],
    ids=("accepted-no-progress", "accepted-replay", "member-retirement"),
)
def test_cap_free_final_release_emits_no_event(
    basis: feed_work_scheduler.FinalRecordReleaseBasis,
    settlement_kind: str,
) -> None:
    grant = _grant()
    member = _member(grant)
    ledger = gap_ledger.MissingCallLedger(grant, 0)
    obligation = _draft(ledger, member, timestamp=None)
    identity = _identity(grant, member, timestamp=None)
    ledger.admit((obligation,), (identity,))
    ledger.mark_terminal_skip(
        obligation,
        identity,
        feed_work_scheduler.CohortItemFailureFact(
            feed_store.FeedStatusReason.SOURCE_UNREACHABLE,
            "item_download_failed",
        ),
        1,
    )
    resolution = feed_work_scheduler.FinalRecordClosureResolution(
        identity,
        feed_work_scheduler.CohortRecordClosureState.REPLAY_SAFE_RELEASE,
        basis,
    )

    with mock.patch.object(telemetry.logger, "log") as emit:
        _observe_final_resolution(ledger, obligation, resolution)
        ledger.close_page(
            _settled_page(
                grant,
                member,
                resolution,
                no_progress=settlement_kind == "no_progress",
                replayable=settlement_kind == "replayable",
            )
        )

    assert ledger.state(obligation) is (
        gap_ledger.MissingCallState.REPLAY_RELEASED
    )
    emit.assert_not_called()


def test_wrong_release_basis_and_crossed_resolution_leave_page_open() -> None:
    grant = _grant()
    member = _member(grant)
    ledger = gap_ledger.MissingCallLedger(grant, 0)
    obligation = _draft(ledger, member, timestamp=None)
    identity = _identity(grant, member, timestamp=None)
    ledger.admit((obligation,), (identity,))
    ledger.mark_terminal_skip(
        obligation,
        identity,
        feed_work_scheduler.CohortItemFailureFact(
            feed_store.FeedStatusReason.SOURCE_UNREACHABLE,
            "item_download_failed",
        ),
        1,
    )
    wrong_basis = feed_work_scheduler.FinalRecordClosureResolution(
        identity,
        feed_work_scheduler.CohortRecordClosureState.REPLAY_SAFE_RELEASE,
        feed_work_scheduler.FinalRecordReleaseBasis.ACCEPTED_NO_PROGRESS,
    )

    with pytest.raises(gap_ledger.MissingCallIntegrityError):
        ledger.close_page(
            _settled_page(
                grant,
                member,
                wrong_basis,
                replayable=True,
            )
        )
    assert ledger.state(obligation) is (
        gap_ledger.MissingCallState.FINAL_CLOSURE_PENDING
    )

    crossed = dataclasses.replace(
        wrong_basis,
        identity=dataclasses.replace(identity),
    )
    with pytest.raises(gap_ledger.MissingCallIntegrityError):
        ledger.close_page(
            _settled_page(
                grant,
                member,
                crossed,
                no_progress=True,
            )
        )

    retirement_without_release = feed_work_scheduler.FinalRecordClosureResolution(
        identity,
        feed_work_scheduler.CohortRecordClosureState.REPLAY_SAFE_RELEASE,
        feed_work_scheduler.FinalRecordReleaseBasis.ACCEPTED_MEMBER_RETIREMENT,
    )
    with pytest.raises(
        gap_ledger.MissingCallIntegrityError,
        match="lacks its exact physical release",
    ):
        ledger.close_page(
            _settled_page(
                grant,
                member,
                retirement_without_release,
                no_progress=True,
            )
        )


@pytest.mark.parametrize(
    ("settlement", "initial_state"),
    [
        (feed_work_scheduler.CallSettlement.ABORTED, "draft"),
        (feed_work_scheduler.CallSettlement.RETRYABLE, "admitted"),
        (
            feed_work_scheduler.CallSettlement.REPLAY_SAFE_RELEASE,
            "final_pending",
        ),
        (feed_work_scheduler.CallSettlement.REPLAY_BLOCKED, "admitted"),
    ],
    ids=(
        "unadmitted-abort",
        "precommit-retry",
        "final-pending-member-retirement",
        "replay-blocked",
    ),
)
def test_known_replay_observer_releases_without_event(
    settlement: feed_work_scheduler.CallSettlement,
    initial_state: str,
) -> None:
    grant = _grant()
    member = _member(grant)
    ledger = gap_ledger.MissingCallLedger(grant, 0)
    obligation = _draft(ledger, member, timestamp=None)
    identity = _identity(grant, member, timestamp=None)
    if initial_state != "draft":
        ledger.admit((obligation,), (identity,))
    if initial_state == "final_pending":
        ledger.mark_terminal_skip(
            obligation,
            identity,
            feed_work_scheduler.CohortItemFailureFact(
                feed_store.FeedStatusReason.SOURCE_UNREACHABLE,
                "item_download_failed",
            ),
            1,
        )

    with mock.patch.object(telemetry.logger, "log") as emit:
        ledger.observe_settlement(obligation, settlement)

    assert ledger.state(obligation) is (
        gap_ledger.MissingCallState.REPLAY_RELEASED
    )
    emit.assert_not_called()
    with pytest.raises(
        gap_ledger.MissingCallIntegrityError,
        match="already observed",
    ):
        ledger.observe_settlement(obligation, settlement)


def test_success_publication_gap_abandonment_and_unknown_are_disjoint() -> None:
    grant = _grant()
    member = _member(grant)
    ledger = gap_ledger.MissingCallLedger(grant, 0)
    obligations = tuple(
        _draft(
            ledger,
            member,
            source_order=index,
            audio_url=f"{_URL}-{index}",
        )
        for index in range(4)
    )
    identities = tuple(
        _identity(
            grant,
            member,
            source_order=index,
            local_sequence=80 + index,
        )
        for index in range(4)
    )
    ledger.admit(obligations, identities)

    with mock.patch.object(telemetry.logger, "log") as emit:
        ledger.resolve_success(obligations[0], identities[0])
        ledger.emit_publication_exhausted(
            obligations[1],
            identities[1],
            feed_work_scheduler.CohortDirectFailureFact(
                feed_store.FeedStatusReason.PIPELINE_PUBLISH_AFTER_BOOKMARK_FAILED,
                "pubsub_unavailable",
            ),
            3,
        )
        ledger.emit_publication_abandoned(obligations[2], identities[2])
        ledger.retain_unknown(obligations[3], identities[3])

    assert tuple(ledger.state(value) for value in obligations) == (
        gap_ledger.MissingCallState.RESOLVED,
        gap_ledger.MissingCallState.EMITTED,
        gap_ledger.MissingCallState.EMITTED,
        gap_ledger.MissingCallState.UNKNOWN_RETAINED,
    )
    assert [call.args[0] for call in emit.call_args_list] == [
        logging.ERROR,
        logging.ERROR,
    ]
    assert [
        call.kwargs["extra"]["json_fields"]["attempt_count"]
        for call in emit.call_args_list
    ] == [3, 0]


def test_logger_failure_is_contained_after_irreversible_state_release() -> None:
    grant = _grant()
    member = _member(grant)
    ledger = gap_ledger.MissingCallLedger(grant, 0)
    obligation = _draft(ledger, member)
    identity = _identity(grant, member)
    ledger.admit((obligation,), (identity,))

    with mock.patch.object(
        telemetry.logger,
        "log",
        side_effect=RuntimeError("logger unavailable"),
    ):
        ledger.emit_publication_abandoned(obligation, identity)

    assert ledger.state(obligation) is gap_ledger.MissingCallState.EMITTED


def test_bounded_fields_and_attempts_fail_before_transition() -> None:
    grant = _grant()
    member = _member(grant)
    ledger = gap_ledger.MissingCallLedger(grant, 0)
    with pytest.raises(ValueError, match="audio_url"):
        _draft(
            ledger,
            member,
            audio_url="x" * (gap_ledger.MISSING_CALL_AUDIO_URL_MAX_LENGTH + 1),
        )

    obligation = _draft(ledger, member)
    identity = _identity(grant, member)
    ledger.admit((obligation,), (identity,))
    with pytest.raises(ValueError, match="attempt_count"):
        ledger.emit_publication_exhausted(
            obligation,
            identity,
            feed_work_scheduler.CohortDirectFailureFact(
                feed_store.FeedStatusReason.PIPELINE_PUBLISH_AFTER_BOOKMARK_FAILED,
                "publish_failed",
            ),
            gap_ledger.MISSING_CALL_ATTEMPT_COUNT_MAX + 1,
        )
    assert ledger.state(obligation) is (gap_ledger.MissingCallState.ADMITTED)

    oversized_failure = feed_work_scheduler.CohortDirectFailureFact(
        feed_store.FeedStatusReason.PIPELINE_PUBLISH_AFTER_BOOKMARK_FAILED,
        "publish_failed",
    )
    object.__setattr__(
        oversized_failure,
        "detail",
        "x" * (slo_contract.BCFY_CALLS_MISSING_CALL_REASON_MAX_LENGTH + 1),
    )
    with pytest.raises(ValueError, match=r"failure\.detail"):
        ledger.emit_publication_exhausted(
            obligation,
            identity,
            oversized_failure,
            1,
        )
    assert ledger.state(obligation) is (gap_ledger.MissingCallState.ADMITTED)


def test_terminal_contract_values_never_contain_url_or_obligation() -> None:
    fields = {
        field.name
        for field in dataclasses.fields(
            feed_work_scheduler.CohortRecordTerminalFact
        )
    }
    retention_fields = {
        field.name
        for field in dataclasses.fields(
            feed_work_scheduler.OutcomeUnknownRetentionRequest
        )
    }

    assert "audio_url" not in fields
    assert "obligation" not in fields
    assert "audio_url" not in retention_fields
    assert "obligation" not in retention_fields
    assert "AcceptedLastPos" not in vars(gap_ledger)


def test_obligation_repr_does_not_leak_signed_url() -> None:
    grant = _grant()
    member = _member(grant)
    ledger = gap_ledger.MissingCallLedger(grant, 0)
    obligation = _draft(ledger, member)

    assert _URL not in repr(obligation)
    assert "signature=secret" not in repr(obligation)
    assert isinstance(obligation, gap_ledger.MissingCallObligation)


def test_success_cap_cannot_close_replayable_sibling_feed() -> None:
    grant = _grant()
    member_a = _member(grant)
    member_b = _member(grant, feed_id=_FEED_B, group_id="46")
    ledger = gap_ledger.MissingCallLedger(grant, 0)
    obligation_a = _draft(ledger, member_a)
    obligation_b = _draft(
        ledger,
        member_b,
        source_order=1,
        timestamp=None,
        audio_url=f"{_URL}-b",
    )
    identity_a = _identity(grant, member_a)
    identity_b = _identity(
        grant,
        member_b,
        source_order=1,
        local_sequence=81,
        timestamp=None,
    )
    ledger.admit(
        (obligation_a, obligation_b),
        (identity_a, identity_b),
    )
    ledger.resolve_success(obligation_a, identity_a)
    ledger.mark_terminal_skip(
        obligation_b,
        identity_b,
        feed_work_scheduler.CohortItemFailureFact(
            feed_store.FeedStatusReason.SOURCE_UNREACHABLE,
            "item_download_failed",
        ),
        1,
    )
    resolution = feed_work_scheduler.FinalRecordClosureResolution(
        identity_b,
        feed_work_scheduler.CohortRecordClosureState.REPLAY_SAFE_RELEASE,
        feed_work_scheduler.FinalRecordReleaseBasis.ACCEPTED_REPLAYABLE_FEED,
    )
    cursor = cursor_policy.LeaseCursor(grant, pos=None)
    candidate = cursor.prepare(_TARGET)
    verdict = boundary_verdict.decide_page_verdict(
        boundary_verdict.PageVerdictInput(
            grant=grant,
            page_sequence=0,
            members=(member_a.identity, member_b.identity),
            record_identities=(),
            cohort_terminal_facts=(),
            child_recovery_candidates=(member_a.identity, member_b.identity),
            lease_recovery_candidate=True,
        )
    )
    source_evidence = runtime_adapters.PageFinalizationEvidence(
        verdict=verdict,
        closure_caps=(_closure_cap(grant, member_a),),
        quarantined_feed_ids=(),
        item_to_feed_promotion_count=0,
    )
    settled = feed_work_scheduler.SettledPage(
        candidate=candidate,
        lease_settlement=cursor_policy._issue_replayable_page(candidate),
        final_closure_resolutions=(resolution,),
        scheduler_evidence=_scheduler_evidence(2),
        source_evidence=source_evidence,
    )

    with mock.patch.object(telemetry.logger, "log") as emit:
        ledger.observe_settlement(
            obligation_a,
            feed_work_scheduler.CallSettlement.COMPLETED,
        )
        _observe_final_resolution(ledger, obligation_b, resolution)
        ledger.close_page(settled)

    assert ledger.state(obligation_a) is gap_ledger.MissingCallState.RESOLVED
    assert ledger.state(obligation_b) is (
        gap_ledger.MissingCallState.REPLAY_RELEASED
    )
    emit.assert_not_called()


def test_later_same_feed_replay_releases_earlier_missing_ts_skip() -> None:
    grant = _grant()
    member = _member(grant)
    ledger = gap_ledger.MissingCallLedger(grant, 0)
    earlier = _draft(ledger, member, timestamp=None)
    later = _draft(
        ledger,
        member,
        source_order=1,
        audio_url=f"{_URL}-later",
    )
    earlier_identity = _identity(grant, member, timestamp=None)
    later_identity = _identity(
        grant,
        member,
        source_order=1,
        local_sequence=81,
    )
    ledger.admit((earlier, later), (earlier_identity, later_identity))
    ledger.mark_terminal_skip(
        earlier,
        earlier_identity,
        feed_work_scheduler.CohortItemFailureFact(
            feed_store.FeedStatusReason.SOURCE_UNREACHABLE,
            "item_download_failed",
        ),
        1,
    )
    ledger.observe_settlement(
        later,
        feed_work_scheduler.CallSettlement.REPLAY_SAFE_RELEASE,
    )
    resolution = feed_work_scheduler.FinalRecordClosureResolution(
        earlier_identity,
        feed_work_scheduler.CohortRecordClosureState.REPLAY_SAFE_RELEASE,
        feed_work_scheduler.FinalRecordReleaseBasis.ACCEPTED_REPLAYABLE_FEED,
    )

    with mock.patch.object(telemetry.logger, "log") as emit:
        _observe_final_resolution(ledger, earlier, resolution)
        ledger.close_page(
            _settled_page(
                grant,
                member,
                resolution,
                replayable=True,
                record_count=2,
            )
        )

    assert ledger.state(earlier) is gap_ledger.MissingCallState.REPLAY_RELEASED
    assert ledger.state(later) is gap_ledger.MissingCallState.REPLAY_RELEASED
    emit.assert_not_called()


def test_crossed_cap_and_extra_resolution_are_rejected_atomically() -> None:
    grant = _grant()
    member = _member(grant)
    sibling = _member(grant, feed_id=_FEED_B, group_id="46")
    ledger = gap_ledger.MissingCallLedger(grant, 0)
    obligation = _draft(ledger, member, timestamp=None)
    identity = _identity(grant, member, timestamp=None)
    ledger.admit((obligation,), (identity,))
    ledger.mark_terminal_skip(
        obligation,
        identity,
        feed_work_scheduler.CohortItemFailureFact(
            feed_store.FeedStatusReason.SOURCE_UNREACHABLE,
            "item_download_failed",
        ),
        1,
    )
    durable = feed_work_scheduler.FinalRecordClosureResolution(
        identity,
        feed_work_scheduler.CohortRecordClosureState.DURABLY_CLOSED,
        feed_work_scheduler.FinalRecordReleaseBasis.DURABLE_SOURCE_CLOSURE,
    )

    with pytest.raises(gap_ledger.MissingCallIntegrityError):
        ledger.close_page(
            _settled_page(
                grant,
                member,
                durable,
                cap=_closure_cap(grant, sibling),
            )
        )
    assert ledger.state(obligation) is (
        gap_ledger.MissingCallState.FINAL_CLOSURE_PENDING
    )

    extra = dataclasses.replace(durable, identity=dataclasses.replace(identity))
    page = _settled_page(
        grant,
        member,
        durable,
        cap=_closure_cap(grant, member),
    )
    object.__setattr__(
        page,
        "final_closure_resolutions",
        (durable, extra),
    )
    with pytest.raises(gap_ledger.MissingCallIntegrityError):
        ledger.close_page(page)
    assert ledger.state(obligation) is (
        gap_ledger.MissingCallState.FINAL_CLOSURE_PENDING
    )


def test_unknown_retention_blocks_page_close_and_cannot_be_replay_released() -> (
    None
):
    grant = _grant()
    member = _member(grant)
    ledger = gap_ledger.MissingCallLedger(grant, 0)
    obligation = _draft(ledger, member, timestamp=None)
    identity = _identity(grant, member, timestamp=None)
    ledger.admit((obligation,), (identity,))
    ledger.retain_unknown(obligation, identity)
    resolution = feed_work_scheduler.FinalRecordClosureResolution(
        identity,
        feed_work_scheduler.CohortRecordClosureState.REPLAY_SAFE_RELEASE,
        feed_work_scheduler.FinalRecordReleaseBasis.ACCEPTED_NO_PROGRESS,
    )

    with pytest.raises(gap_ledger.MissingCallIntegrityError):
        ledger.close_page(
            _settled_page(
                grant,
                member,
                resolution,
                no_progress=True,
            )
        )
    with pytest.raises(gap_ledger.MissingCallIntegrityError):
        ledger.observe_settlement(
            obligation,
            feed_work_scheduler.CallSettlement.REPLAY_SAFE_RELEASE,
        )
    assert ledger.state(obligation) is (
        gap_ledger.MissingCallState.UNKNOWN_RETAINED
    )
