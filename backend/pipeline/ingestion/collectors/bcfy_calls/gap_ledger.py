"""Bounded replay-safe missing-call obligations for one Calls page."""

from __future__ import annotations

import dataclasses
import datetime
import enum
from typing import TYPE_CHECKING

from backend.pipeline.ingestion import feed_work_scheduler, slo_contract
from backend.pipeline.ingestion.collectors.bcfy_calls import (
    cursor_policy,
    runtime_adapters,
    telemetry,
)
from backend.pipeline.storage import feed_store, ingestion_lease_store

if TYPE_CHECKING:
    import uuid

__all__ = [
    "MISSING_CALL_ATTEMPT_COUNT_MAX",
    "MISSING_CALL_AUDIO_URL_MAX_LENGTH",
    "MissingCallIntegrityError",
    "MissingCallLedger",
    "MissingCallObligation",
    "MissingCallState",
]

MISSING_CALL_AUDIO_URL_MAX_LENGTH = (
    slo_contract.BCFY_CALLS_MISSING_CALL_AUDIO_URL_MAX_LENGTH
)
MISSING_CALL_ATTEMPT_COUNT_MAX = (
    slo_contract.BCFY_CALLS_MISSING_CALL_ATTEMPT_COUNT_MAX
)

_OBLIGATION_CONSTRUCTION_KEY = object()


class MissingCallIntegrityError(RuntimeError):
    """Missing-call evidence crossed its exact page or record authority."""


class MissingCallState(enum.StrEnum):
    """Closed lifecycle of one page-local missing-call obligation."""

    DRAFT = "draft"
    ADMITTED = "admitted"
    RESOLVED = "resolved"
    PENDING = "pending"
    FINAL_CLOSURE_PENDING = "final_closure_pending"
    EMITTED = "emitted"
    REPLAY_RELEASED = "replay_released"
    UNKNOWN_RETAINED = "unknown_retained"


@dataclasses.dataclass(frozen=True, slots=True, init=False, repr=False)
class MissingCallObligation:
    """Sealed capability naming one ledger-owned record without its URL."""

    _owner: MissingCallLedger
    _slot: int
    _seal: object

    def __init__(
        self,
        owner: MissingCallLedger,
        slot: int,
        construction_key: object,
    ) -> None:
        if construction_key is not _OBLIGATION_CONSTRUCTION_KEY:
            message = "missing-call obligations require their ledger issuer"
            raise MissingCallIntegrityError(message)
        object.__setattr__(self, "_owner", owner)
        object.__setattr__(self, "_slot", slot)
        object.__setattr__(self, "_seal", construction_key)

    def __repr__(self) -> str:
        return f"<MissingCallObligation slot={self._slot}>"


@dataclasses.dataclass(slots=True)
class _ObligationRecord:
    """Private bounded event data and transition state."""

    handle: MissingCallObligation
    member: ingestion_lease_store.LeaseMember
    feed_name: str
    audio_url: str = dataclasses.field(repr=False)
    provider_ts: datetime.datetime | None
    source_order: int
    state: MissingCallState = MissingCallState.DRAFT
    identity: feed_work_scheduler.CohortRecordIdentity | None = None
    gap_stage: telemetry.MissingCallStage | None = None
    status_reason: feed_store.FeedStatusReason | None = None
    reason: str | None = None
    attempt_count: int | None = None
    requires_final_resolution: bool = False
    settlement_observed: feed_work_scheduler.CallSettlement | None = None


@dataclasses.dataclass(frozen=True, slots=True)
class _ClosurePlan:
    """Prevalidated atomic state transition for one final record."""

    record: _ObligationRecord
    closure_state: feed_work_scheduler.CohortRecordClosureState
    event: telemetry.MissingCallEvent | None


_REPLAY_SETTLEMENTS = frozenset(
    {
        feed_work_scheduler.CallSettlement.RETRYABLE,
        feed_work_scheduler.CallSettlement.AUTHORITY_LOST,
        feed_work_scheduler.CallSettlement.MEMBERSHIP_REJECTED,
        feed_work_scheduler.CallSettlement.ABORTED,
        feed_work_scheduler.CallSettlement.REPLAY_SAFE_RELEASE,
        feed_work_scheduler.CallSettlement.REPLAY_BLOCKED,
    }
)


class MissingCallLedger:
    """Own every bounded missing-call obligation for one candidate page."""

    __slots__ = (
        "_closed",
        "_grant",
        "_local_sequences",
        "_page_sequence",
        "_records",
        "_source_orders",
    )

    def __init__(
        self,
        grant: ingestion_lease_store.LeaseGrant,
        page_sequence: int,
    ) -> None:
        if not isinstance(grant, ingestion_lease_store.LeaseGrant):
            message = "grant must be a LeaseGrant"
            raise TypeError(message)
        _require_nonnegative_integer(page_sequence, "page_sequence")
        if grant.source_type is not feed_store.SourceType.BCFY_CALLS:
            message = "missing-call ledger requires a Calls grant"
            raise ValueError(message)
        self._grant = grant
        self._page_sequence = page_sequence
        self._records: list[_ObligationRecord] = []
        self._source_orders: set[int] = set()
        self._local_sequences: set[tuple[uuid.UUID, int]] = set()
        self._closed = False

    @property
    def grant(self) -> ingestion_lease_store.LeaseGrant:
        """Return the exact complete grant bound to this page ledger."""
        return self._grant

    @property
    def page_sequence(self) -> int:
        """Return the exact grant-local page sequence."""
        return self._page_sequence

    def draft(
        self,
        member: ingestion_lease_store.LeaseMember,
        *,
        audio_url: str,
        provider_ts: datetime.datetime | None,
        source_order: int,
    ) -> MissingCallObligation:
        """Create one unbound draft before scheduler admission."""
        self._require_open()
        if not isinstance(member, ingestion_lease_store.LeaseMember):
            message = "member must be a LeaseMember"
            raise TypeError(message)
        try:
            ingestion_lease_store._require_member_binding(  # noqa: SLF001
                self._grant,
                member.identity,
            )
        except (TypeError, ValueError) as exc:
            message = "draft member crossed complete grant authority"
            raise MissingCallIntegrityError(message) from exc
        feed_name = _require_bounded_string(
            member.name,
            "member.name",
            slo_contract.BCFY_CALLS_MISSING_CALL_IDENTITY_MAX_LENGTH,
        )
        for name, value in (
            ("member.sid", member.identity.sid),
            ("member.source_feed_id", member.identity.source_feed_id),
            ("member.group_id", member.identity.group_id),
            ("member.source_type", member.identity.source_type.value),
        ):
            _require_bounded_string(
                value,
                name,
                slo_contract.BCFY_CALLS_MISSING_CALL_IDENTITY_MAX_LENGTH,
            )
        _require_bounded_string(
            audio_url,
            "audio_url",
            MISSING_CALL_AUDIO_URL_MAX_LENGTH,
        )
        _require_utc_timestamp(provider_ts, "provider_ts")
        _require_nonnegative_integer(source_order, "source_order")
        if source_order in self._source_orders:
            message = "draft source_order is already owned by this page"
            raise MissingCallIntegrityError(message)

        slot = len(self._records)
        handle = MissingCallObligation(
            self,
            slot,
            _OBLIGATION_CONSTRUCTION_KEY,
        )
        self._records.append(
            _ObligationRecord(
                handle=handle,
                member=member,
                feed_name=feed_name,
                audio_url=audio_url,
                provider_ts=provider_ts,
                source_order=source_order,
            )
        )
        self._source_orders.add(source_order)
        return handle

    def admit(
        self,
        obligations: tuple[MissingCallObligation, ...],
        identities: tuple[feed_work_scheduler.CohortRecordIdentity, ...],
    ) -> None:
        """Atomically bind one admitted cohort's exact ordered identities."""
        self._require_open()
        if not isinstance(obligations, tuple) or not obligations:
            message = "obligations must be a nonempty immutable tuple"
            raise ValueError(message)
        if not isinstance(identities, tuple) or len(identities) != len(
            obligations
        ):
            message = "admission identities must exactly cover obligations"
            raise MissingCallIntegrityError(message)
        if len({id(value) for value in obligations}) != len(obligations):
            message = "admission repeats an obligation handle"
            raise MissingCallIntegrityError(message)
        if any(
            type(value) is not feed_work_scheduler.CohortRecordIdentity
            for value in identities
        ):
            message = "admission requires exact record identities"
            raise TypeError(message)
        if len({id(value) for value in identities}) != len(identities):
            message = "admission repeats a record identity object"
            raise MissingCallIntegrityError(message)
        source_orders = tuple(
            self._require_record(value).source_order for value in obligations
        )
        if source_orders != tuple(sorted(source_orders)):
            message = "admission obligations crossed provider source order"
            raise MissingCallIntegrityError(message)
        local_sequences = tuple(value.local_sequence for value in identities)
        if local_sequences != tuple(
            range(local_sequences[0], local_sequences[0] + len(identities))
        ):
            message = "admission local_sequence values are not consecutive"
            raise MissingCallIntegrityError(message)

        pending: list[
            tuple[_ObligationRecord, feed_work_scheduler.CohortRecordIdentity]
        ] = []
        proposed_local_sequences: set[tuple[uuid.UUID, int]] = set()
        for obligation, identity in zip(
            obligations,
            identities,
            strict=True,
        ):
            record = self._require_record(obligation)
            if record.state is not MissingCallState.DRAFT:
                message = "admission obligation is not a fresh draft"
                raise MissingCallIntegrityError(message)
            self._validate_identity(record, identity, admitted=False)
            scheduler_slot = (identity.feed_id, identity.local_sequence)
            if scheduler_slot in self._local_sequences or (
                scheduler_slot in proposed_local_sequences
            ):
                message = "admission Feed/local_sequence is already bound"
                raise MissingCallIntegrityError(message)
            proposed_local_sequences.add(scheduler_slot)
            pending.append((record, identity))

        for record, identity in pending:
            record.identity = identity
            record.state = MissingCallState.ADMITTED
        self._local_sequences.update(proposed_local_sequences)

    def state(self, obligation: MissingCallObligation) -> MissingCallState:
        """Return one exact obligation's current closed state."""
        return self._require_record(obligation).state

    def resolve_success(
        self,
        obligation: MissingCallObligation,
        identity: feed_work_scheduler.CohortRecordIdentity,
    ) -> None:
        """Resolve one fully published call without gap evidence."""
        record = self._require_admitted_record(obligation, identity)
        self._require_state(record, MissingCallState.ADMITTED)
        record.state = MissingCallState.RESOLVED

    def mark_terminal_skip(
        self,
        obligation: MissingCallObligation,
        identity: feed_work_scheduler.CohortRecordIdentity,
        failure: feed_work_scheduler.CohortItemFailureFact,
        attempt_count: int,
    ) -> None:
        """Hold one terminal source/audio skip until exact durable closure."""
        record = self._require_admitted_record(obligation, identity)
        self._require_state(record, MissingCallState.ADMITTED)
        if type(failure) is not feed_work_scheduler.CohortItemFailureFact:
            message = "failure must be an exact CohortItemFailureFact"
            raise TypeError(message)
        _require_attempt_count(attempt_count, require_attempt=True)
        reason = _require_bounded_string(
            failure.detail,
            "failure.detail",
            slo_contract.BCFY_CALLS_MISSING_CALL_REASON_MAX_LENGTH,
        )
        record.gap_stage = telemetry.MissingCallStage.TERMINAL_ITEM_SKIP
        record.status_reason = failure.status_reason
        record.reason = reason
        record.attempt_count = attempt_count
        record.state = (
            MissingCallState.FINAL_CLOSURE_PENDING
            if record.provider_ts is None
            else MissingCallState.PENDING
        )
        record.requires_final_resolution = record.provider_ts is None

    def close_timestamped_skip(
        self,
        obligation: MissingCallObligation,
        identity: feed_work_scheduler.CohortRecordIdentity,
        accepted_cursor: datetime.datetime,
    ) -> None:
        """Emit a pending timestamped skip after its neutral commit crosses."""
        record = self._require_admitted_record(obligation, identity)
        self._require_state(record, MissingCallState.PENDING)
        _require_utc_timestamp(accepted_cursor, "accepted_cursor")
        if record.provider_ts is None or accepted_cursor < record.provider_ts:
            message = "physical cursor did not cross this terminal skip"
            raise MissingCallIntegrityError(message)
        self._emit(record)

    def emit_publication_exhausted(
        self,
        obligation: MissingCallObligation,
        identity: feed_work_scheduler.CohortRecordIdentity,
        failure: feed_work_scheduler.CohortDirectFailureFact,
        attempt_count: int,
    ) -> None:
        """Emit one immediate post-bookmark publication gap."""
        record = self._require_admitted_record(obligation, identity)
        self._require_state(record, MissingCallState.ADMITTED)
        if type(failure) is not feed_work_scheduler.CohortDirectFailureFact:
            message = "failure must be an exact CohortDirectFailureFact"
            raise TypeError(message)
        _require_attempt_count(attempt_count, require_attempt=True)
        reason = _require_bounded_string(
            failure.detail,
            "failure.detail",
            slo_contract.BCFY_CALLS_MISSING_CALL_REASON_MAX_LENGTH,
        )
        record.gap_stage = telemetry.MissingCallStage.PUBLICATION_EXHAUSTED
        record.status_reason = failure.status_reason
        record.reason = reason
        record.attempt_count = attempt_count
        self._emit(record)

    def emit_publication_abandoned(
        self,
        obligation: MissingCallObligation,
        identity: feed_work_scheduler.CohortRecordIdentity,
    ) -> None:
        """Emit one deliberately unattempted committed publication gap."""
        record = self._require_admitted_record(obligation, identity)
        self._require_state(record, MissingCallState.ADMITTED)
        record.gap_stage = telemetry.MissingCallStage.PUBLICATION_ABANDONED
        record.status_reason = (
            feed_store.FeedStatusReason.PIPELINE_PUBLISH_AFTER_BOOKMARK_FAILED
        )
        record.reason = "publication_abandoned"
        record.attempt_count = 0
        self._emit(record)

    def retain_unknown(
        self,
        obligation: MissingCallObligation,
        identity: feed_work_scheduler.CohortRecordIdentity,
    ) -> None:
        """Retain nonterminal URL reachability without observer or emission."""
        record = self._require_admitted_record(obligation, identity)
        if record.state is MissingCallState.UNKNOWN_RETAINED:
            return
        if record.state in {
            MissingCallState.RESOLVED,
            MissingCallState.EMITTED,
            MissingCallState.REPLAY_RELEASED,
        }:
            return
        if record.state not in {
            MissingCallState.ADMITTED,
            MissingCallState.PENDING,
            MissingCallState.FINAL_CLOSURE_PENDING,
        }:
            message = "obligation cannot retain unknown from this state"
            raise MissingCallIntegrityError(message)
        record.state = MissingCallState.UNKNOWN_RETAINED

    def observe_settlement(
        self,
        obligation: MissingCallObligation,
        settlement: feed_work_scheduler.CallSettlement,
    ) -> None:
        """Apply one exact scheduler release callback without side channels."""
        record = self._require_record(obligation)
        if not isinstance(settlement, feed_work_scheduler.CallSettlement):
            message = "settlement must be a CallSettlement"
            raise TypeError(message)
        if record.settlement_observed is not None:
            message = "scheduler settlement was already observed"
            raise MissingCallIntegrityError(message)
        if record.state is MissingCallState.FINAL_CLOSURE_PENDING:
            if settlement is feed_work_scheduler.CallSettlement.COMPLETED:
                record.settlement_observed = settlement
                return
            if settlement is (
                feed_work_scheduler.CallSettlement.REPLAY_SAFE_RELEASE
            ):
                self._release_record(record)
                record.settlement_observed = settlement
                return
            message = "final-pending record received a nonfinal settlement"
            raise MissingCallIntegrityError(message)
        if settlement in _REPLAY_SETTLEMENTS:
            self._release_record(record)
            record.settlement_observed = settlement
            return
        if settlement is not feed_work_scheduler.CallSettlement.COMPLETED:
            message = "settlement is outside the closed observer contract"
            raise MissingCallIntegrityError(message)
        if record.state not in {
            MissingCallState.RESOLVED,
            MissingCallState.EMITTED,
        }:
            message = "completed settlement preceded obligation closure"
            raise MissingCallIntegrityError(message)
        record.settlement_observed = settlement

    def close_page(self, settled_page: feed_work_scheduler.SettledPage) -> None:
        """Atomically close every final-pending record from exact evidence."""
        self._require_open()
        source_evidence = self._validate_page_evidence(settled_page)
        pending = self._validate_terminal_records()
        planned = self._plan_final_closures(
            settled_page,
            source_evidence,
            pending,
        )
        to_emit = tuple(
            planned_closure.event
            for planned_closure in planned
            if planned_closure.event is not None
        )
        self._closed = True
        for planned_closure in planned:
            planned_closure.record.state = (
                MissingCallState.EMITTED
                if planned_closure.closure_state
                is feed_work_scheduler.CohortRecordClosureState.DURABLY_CLOSED
                else MissingCallState.REPLAY_RELEASED
            )
        for event in to_emit:
            telemetry.emit_missing_call(event)

    def _validate_page_evidence(
        self,
        settled_page: feed_work_scheduler.SettledPage,
    ) -> runtime_adapters.PageFinalizationEvidence:
        """Validate the exact page, source, and scheduler conservation proof."""
        if type(settled_page) is not feed_work_scheduler.SettledPage:
            message = "settled_page must be an exact SettledPage"
            raise TypeError(message)
        if (
            settled_page.grant != self._grant
            or settled_page.page_sequence != self._page_sequence
        ):
            message = "settled page crossed ledger grant or page"
            raise MissingCallIntegrityError(message)
        if type(settled_page.source_evidence) is not (
            runtime_adapters.PageFinalizationEvidence
        ):
            message = "settled page lacks exact Calls source evidence"
            raise MissingCallIntegrityError(message)
        source_evidence = settled_page.source_evidence
        if (
            source_evidence.verdict.grant != self._grant
            or source_evidence.verdict.page_sequence != self._page_sequence
        ):
            message = "Calls source evidence crossed ledger grant or page"
            raise MissingCallIntegrityError(message)
        if type(settled_page.scheduler_evidence) is not (
            feed_work_scheduler.SchedulerPageEvidence
        ):
            message = "settled page lacks exact scheduler evidence"
            raise MissingCallIntegrityError(message)
        scheduler_evidence = settled_page.scheduler_evidence
        admitted_record_count = sum(
            record.identity is not None for record in self._records
        )
        if (
            scheduler_evidence.admitted_record_count != admitted_record_count
            or scheduler_evidence.terminal_record_count != admitted_record_count
        ):
            message = "page ledger and scheduler record counts disagree"
            raise MissingCallIntegrityError(message)
        return source_evidence

    def _validate_terminal_records(self) -> tuple[_ObligationRecord, ...]:
        """Validate observer/state agreement and return final records."""
        unresolved_states = {
            MissingCallState.DRAFT,
            MissingCallState.ADMITTED,
            MissingCallState.PENDING,
            MissingCallState.UNKNOWN_RETAINED,
        }
        if any(record.state in unresolved_states for record in self._records):
            message = "page close encountered unresolved nonfinal obligation"
            raise MissingCallIntegrityError(message)
        for record in self._records:
            if record.requires_final_resolution:
                continue
            if record.state in {
                MissingCallState.RESOLVED,
                MissingCallState.EMITTED,
            } and record.settlement_observed is not (
                feed_work_scheduler.CallSettlement.COMPLETED
            ):
                message = "durable record lacks its scheduler release"
                raise MissingCallIntegrityError(message)
            if (
                record.state is MissingCallState.REPLAY_RELEASED
                and record.settlement_observed not in _REPLAY_SETTLEMENTS
            ):
                message = "replay record lacks its scheduler release"
                raise MissingCallIntegrityError(message)
        pending = tuple(
            record
            for record in self._records
            if record.requires_final_resolution
        )
        if any(
            record.state
            not in {
                MissingCallState.FINAL_CLOSURE_PENDING,
                MissingCallState.REPLAY_RELEASED,
            }
            for record in pending
        ):
            message = "final-resolution obligation crossed its pending state"
            raise MissingCallIntegrityError(message)
        return pending

    def _plan_final_closures(
        self,
        settled_page: feed_work_scheduler.SettledPage,
        source_evidence: runtime_adapters.PageFinalizationEvidence,
        pending: tuple[_ObligationRecord, ...],
    ) -> tuple[_ClosurePlan, ...]:
        """Prebuild every final transition without mutating ledger state."""
        resolutions = settled_page.final_closure_resolutions
        if len(resolutions) != len(pending):
            message = "final resolutions do not exactly cover pending records"
            raise MissingCallIntegrityError(message)
        pending_by_identity = {
            id(record.identity): record
            for record in pending
            if record.identity is not None
        }
        cap_by_feed = self._validate_caps(
            settled_page,
            source_evidence.closure_caps,
        )
        planned = []
        seen_resolution_identities: set[int] = set()
        for resolution in resolutions:
            planned.append(
                self._plan_final_resolution(
                    settled_page,
                    resolution,
                    pending_by_identity,
                    seen_resolution_identities,
                    cap_by_feed,
                )
            )
        if len(seen_resolution_identities) != len(pending_by_identity):
            message = "final resolution coverage is partial"
            raise MissingCallIntegrityError(message)
        return tuple(planned)

    def _plan_final_resolution(
        self,
        settled_page: feed_work_scheduler.SettledPage,
        resolution: feed_work_scheduler.FinalRecordClosureResolution,
        pending_by_identity: dict[int, _ObligationRecord],
        seen_resolution_identities: set[int],
        cap_by_feed: dict[uuid.UUID, object],
    ) -> _ClosurePlan:
        """Correlate and prevalidate one exact final resolution."""
        identity_key = id(resolution.identity)
        record = pending_by_identity.get(identity_key)
        if (
            record is None
            or record.identity is not resolution.identity
            or identity_key in seen_resolution_identities
        ):
            message = "final resolution is extra, crossed, or duplicated"
            raise MissingCallIntegrityError(message)
        seen_resolution_identities.add(identity_key)
        self._validate_identity(record, resolution.identity, admitted=True)
        self._validate_resolution(
            settled_page,
            resolution,
            has_cap=resolution.identity.feed_id in cap_by_feed,
        )
        durable = resolution.closure_state is (
            feed_work_scheduler.CohortRecordClosureState.DURABLY_CLOSED
        )
        expected_settlement = (
            feed_work_scheduler.CallSettlement.COMPLETED
            if durable
            else feed_work_scheduler.CallSettlement.REPLAY_SAFE_RELEASE
        )
        if record.settlement_observed is not expected_settlement:
            message = "final resolution lacks its exact physical release"
            raise MissingCallIntegrityError(message)
        if durable != (record.state is MissingCallState.FINAL_CLOSURE_PENDING):
            message = "final resolution disagrees with observer state"
            raise MissingCallIntegrityError(message)
        return _ClosurePlan(
            record,
            resolution.closure_state,
            self._event(record) if durable else None,
        )

    def _validate_caps(
        self,
        settled_page: feed_work_scheduler.SettledPage,
        caps: tuple[object, ...],
    ) -> dict[uuid.UUID, object]:
        if not isinstance(caps, tuple):
            message = "closure caps must be an immutable tuple"
            raise TypeError(message)
        candidate = settled_page.candidate
        if type(candidate) is cursor_policy.NoProgressPageCandidate:
            if caps:
                message = "no-progress settlement cannot carry closure caps"
                raise MissingCallIntegrityError(message)
            return {}
        if type(candidate) is not cursor_policy.PageCursorCandidate:
            message = "settled candidate is outside the closed contract"
            raise MissingCallIntegrityError(message)

        cap_by_feed: dict[uuid.UUID, object] = {}
        for cap in caps:
            try:
                feed_id = runtime_adapters.feed_closure_cap_feed_id(cap)
                covered = runtime_adapters.feed_closure_cap_covers(
                    cap,
                    grant=self._grant,
                    page_sequence=self._page_sequence,
                    feed_id=feed_id,
                    requested_target=candidate.last_pos,
                )
            except (
                TypeError,
                ValueError,
                runtime_adapters.BoundaryAdapterIntegrityError,
            ) as exc:
                message = "closure cap is forged or crossed"
                raise MissingCallIntegrityError(message) from exc
            if not covered:
                message = "closure cap crossed page target"
                raise MissingCallIntegrityError(message)
            if feed_id in cap_by_feed:
                message = "closure caps repeat a Feed"
                raise MissingCallIntegrityError(message)
            cap_by_feed[feed_id] = cap
        return cap_by_feed

    @staticmethod
    def _validate_resolution(
        settled_page: feed_work_scheduler.SettledPage,
        resolution: feed_work_scheduler.FinalRecordClosureResolution,
        *,
        has_cap: bool,
    ) -> None:
        if type(resolution) is not (
            feed_work_scheduler.FinalRecordClosureResolution
        ):
            message = "resolution must be an exact final-record value"
            raise TypeError(message)
        closure = feed_work_scheduler.CohortRecordClosureState
        basis = feed_work_scheduler.FinalRecordReleaseBasis
        if resolution.closure_state is closure.DURABLY_CLOSED:
            if not has_cap or resolution.release_basis is not (
                basis.DURABLE_SOURCE_CLOSURE
            ):
                message = "durable resolution lacks its own Feed cap"
                raise MissingCallIntegrityError(message)
            return
        if resolution.closure_state is not closure.REPLAY_SAFE_RELEASE:
            message = "final resolution is neither durable nor replay-safe"
            raise MissingCallIntegrityError(message)
        if has_cap:
            message = "replay resolution disagrees with own-Feed durability"
            raise MissingCallIntegrityError(message)
        settlement = settled_page.lease_settlement
        if resolution.release_basis is basis.ACCEPTED_NO_PROGRESS:
            valid = type(settlement) is cursor_policy._NoProgressPageSettled  # noqa: SLF001
        elif resolution.release_basis is basis.ACCEPTED_REPLAYABLE_FEED:
            valid = type(settlement) is cursor_policy._ReplayablePageSettled  # noqa: SLF001
        else:
            valid = (
                resolution.release_basis is basis.ACCEPTED_MEMBER_RETIREMENT
                and type(settlement)
                in {
                    cursor_policy._CoveredPage,  # noqa: SLF001
                    cursor_policy._NoProgressPageSettled,  # noqa: SLF001
                    cursor_policy._ReplayablePageSettled,  # noqa: SLF001
                }
            )
        if not valid:
            message = "cap-free resolution uses the wrong accepted basis"
            raise MissingCallIntegrityError(message)

    def _require_open(self) -> None:
        if self._closed:
            message = "missing-call page ledger is already closed"
            raise MissingCallIntegrityError(message)

    def _require_record(
        self,
        obligation: MissingCallObligation,
    ) -> _ObligationRecord:
        if type(obligation) is not MissingCallObligation:
            message = "obligation must be an exact sealed handle"
            raise TypeError(message)
        if (
            obligation._owner is not self  # noqa: SLF001
            or obligation._seal is not _OBLIGATION_CONSTRUCTION_KEY  # noqa: SLF001
            or isinstance(obligation._slot, bool)  # noqa: SLF001
            or not isinstance(obligation._slot, int)  # noqa: SLF001
            or obligation._slot < 0  # noqa: SLF001
            or obligation._slot >= len(self._records)  # noqa: SLF001
        ):
            message = "obligation crossed or forged its ledger authority"
            raise MissingCallIntegrityError(message)
        record = self._records[obligation._slot]  # noqa: SLF001
        if record.handle is not obligation:
            message = "obligation handle crossed its original slot"
            raise MissingCallIntegrityError(message)
        return record

    def _require_admitted_record(
        self,
        obligation: MissingCallObligation,
        identity: feed_work_scheduler.CohortRecordIdentity,
    ) -> _ObligationRecord:
        record = self._require_record(obligation)
        self._validate_identity(record, identity, admitted=True)
        return record

    def _validate_identity(
        self,
        record: _ObligationRecord,
        identity: feed_work_scheduler.CohortRecordIdentity,
        *,
        admitted: bool,
    ) -> None:
        if type(identity) is not feed_work_scheduler.CohortRecordIdentity:
            message = "identity must be an exact CohortRecordIdentity"
            raise TypeError(message)
        try:
            ingestion_lease_store._require_member_binding(  # noqa: SLF001
                self._grant,
                identity.member,
            )
        except (TypeError, ValueError) as exc:
            message = "record identity member lost its issued binding"
            raise MissingCallIntegrityError(message) from exc
        if admitted and record.identity is not identity:
            message = "record identity object crossed its admission binding"
            raise MissingCallIntegrityError(message)
        if (
            identity.grant != self._grant
            or identity.page_sequence != self._page_sequence
            or identity.member is not record.member.identity
            or identity.feed_id != record.member.identity.feed_id
            or identity.source_order != record.source_order
            or identity.cohort_timestamp != record.provider_ts
        ):
            message = "record identity crossed grant/member/Feed/page/order"
            raise MissingCallIntegrityError(message)

    @staticmethod
    def _require_state(
        record: _ObligationRecord,
        expected: MissingCallState,
    ) -> None:
        if record.state is not expected:
            message = (
                f"obligation state {record.state.value} cannot transition "
                f"from required {expected.value}"
            )
            raise MissingCallIntegrityError(message)

    @staticmethod
    def _release_record(record: _ObligationRecord) -> None:
        if record.state not in {
            MissingCallState.DRAFT,
            MissingCallState.ADMITTED,
            MissingCallState.PENDING,
            MissingCallState.FINAL_CLOSURE_PENDING,
        }:
            message = "obligation is not replay-releasable"
            raise MissingCallIntegrityError(message)
        record.state = MissingCallState.REPLAY_RELEASED

    @staticmethod
    def _event(record: _ObligationRecord) -> telemetry.MissingCallEvent:
        if (
            record.gap_stage is None
            or record.status_reason is None
            or record.reason is None
            or record.attempt_count is None
        ):
            message = "irreversible obligation lacks complete event evidence"
            raise MissingCallIntegrityError(message)
        identity = record.member.identity
        return telemetry.MissingCallEvent(
            audio_url=record.audio_url,
            sid=identity.sid,
            feed_id=str(identity.feed_id),
            feed_name=record.feed_name,
            source_type=identity.source_type.value,
            source_feed_id=identity.source_feed_id,
            group_id=identity.group_id,
            provider_ts=(
                record.provider_ts.isoformat()
                if record.provider_ts is not None
                else None
            ),
            gap_stage=record.gap_stage,
            status_reason=record.status_reason.value,
            reason=record.reason,
            attempt_count=record.attempt_count,
        )

    def _emit(self, record: _ObligationRecord) -> None:
        event = self._event(record)
        record.state = MissingCallState.EMITTED
        telemetry.emit_missing_call(event)


def _owner_for(obligation: MissingCallObligation) -> MissingCallLedger:
    if type(obligation) is not MissingCallObligation:
        message = "obligation must be an exact sealed handle"
        raise TypeError(message)
    owner = obligation._owner  # noqa: SLF001
    owner._require_record(obligation)  # noqa: SLF001
    return owner


def _validate_scheduled_entry(
    obligation: MissingCallObligation,
    *,
    member: ingestion_lease_store.LeaseMember,
    source_order: int,
    audio_url: str,
    provider_ts: datetime.datetime | None,
) -> MissingCallLedger:
    owner = _owner_for(obligation)
    record = owner._require_record(obligation)  # noqa: SLF001
    if (
        record.member is not member
        or record.source_order != source_order
        or record.audio_url != audio_url
        or record.provider_ts != provider_ts
    ):
        message = "scheduled entry crossed its draft obligation"
        raise MissingCallIntegrityError(message)
    return owner


def _validate_admitted_entry(
    obligation: MissingCallObligation,
    identity: feed_work_scheduler.CohortRecordIdentity,
) -> MissingCallLedger:
    owner = _owner_for(obligation)
    owner._require_admitted_record(obligation, identity)  # noqa: SLF001
    return owner


def _require_nonnegative_integer(value: object, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        message = f"{name} must be an integer"
        raise TypeError(message)
    if value < 0:
        message = f"{name} must be nonnegative"
        raise ValueError(message)
    return value


def _require_bounded_string(
    value: object,
    name: str,
    maximum: int,
) -> str:
    if not isinstance(value, str):
        message = f"{name} must be a string"
        raise TypeError(message)
    if not value or len(value) > maximum:
        message = f"{name} must be nonempty and at most {maximum} chars"
        raise ValueError(message)
    return value


def _require_utc_timestamp(value: object, name: str) -> None:
    if value is None:
        return
    if not isinstance(value, datetime.datetime):
        message = f"{name} must be a datetime or None"
        raise TypeError(message)
    if value.utcoffset() != datetime.timedelta(0):
        message = f"{name} must be UTC-aware"
        raise ValueError(message)


def _require_attempt_count(
    value: object,
    *,
    require_attempt: bool,
) -> int:
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or value < int(require_attempt)
        or value > MISSING_CALL_ATTEMPT_COUNT_MAX
    ):
        message = "attempt_count is outside the bounded event contract"
        raise ValueError(message)
    return value
