"""Closed immutable vocabulary for bounded Feed-affine scheduling."""

from __future__ import annotations

import dataclasses
import datetime
import enum
import math
import typing

from backend.pipeline.ingestion.collectors.bcfy_calls import cursor_policy
from backend.pipeline.storage import (
    feed_store,
    ingestion_lease_store,
    status_reason_detail,
)

if typing.TYPE_CHECKING:
    import uuid

PRODUCTION_SHARD_COUNT = 8
PRODUCTION_SHARD_CAPACITY = 500
PRODUCTION_WORKERS_PER_SHARD = 4
PRODUCTION_HIGH_WATER = 400
PRODUCTION_RESUME_AT = 299


def _require_positive_integer(value: int, name: str) -> int:
    if isinstance(value, bool):
        message = f"{name} must be an integer"
        raise TypeError(message)
    if value <= 0:
        message = f"{name} must be positive"
        raise ValueError(message)
    return value


def _require_nonnegative_integer(value: int, name: str) -> int:
    if isinstance(value, bool):
        message = f"{name} must be an integer"
        raise TypeError(message)
    if value < 0:
        message = f"{name} must be nonnegative"
        raise ValueError(message)
    return value


def _require_nonnegative_finite_number(
    value: float,
    name: str,
) -> float:
    if isinstance(value, bool):
        message = f"{name} must be a number"
        raise TypeError(message)
    try:
        numeric = float(value)
    except OverflowError:
        numeric = math.inf
    if not math.isfinite(numeric) or numeric < 0:
        message = f"{name} must be finite and nonnegative"
        raise ValueError(message)
    return numeric


@dataclasses.dataclass(frozen=True, slots=True)
class _SchedulerLimits:
    """Validated fixed sizes used by production and deterministic tests."""

    shard_count: int
    capacity: int
    workers_per_shard: int
    high_water: int
    resume_at: int

    def __post_init__(self) -> None:
        _require_positive_integer(self.shard_count, "shard_count")
        _require_positive_integer(self.capacity, "capacity")
        _require_positive_integer(
            self.workers_per_shard,
            "workers_per_shard",
        )
        _require_positive_integer(self.high_water, "high_water")
        _require_nonnegative_integer(self.resume_at, "resume_at")
        if self.high_water > self.capacity:
            message = "high_water must not exceed capacity"
            raise ValueError(message)
        if self.resume_at >= self.high_water:
            message = "resume_at must be lower than high_water"
            raise ValueError(message)


_PRODUCTION_LIMITS = _SchedulerLimits(
    shard_count=PRODUCTION_SHARD_COUNT,
    capacity=PRODUCTION_SHARD_CAPACITY,
    workers_per_shard=PRODUCTION_WORKERS_PER_SHARD,
    high_water=PRODUCTION_HIGH_WATER,
    resume_at=PRODUCTION_RESUME_AT,
)


def _shard_index(
    feed_id: uuid.UUID,
    limits: _SchedulerLimits = _PRODUCTION_LIMITS,
) -> int:
    """Return stable UUID affinity for production or validated test limits."""
    return feed_id.int % limits.shard_count


class CohortIntegrityError(RuntimeError):
    """A cohort capability, identity, or terminal fact is inconsistent."""


class ExecutionSignal(typing.Protocol):
    """Read-only projection of one runner-owned monotonic signal."""

    def is_set(self) -> bool:
        """Return whether the underlying signal is set."""
        ...

    async def wait(self) -> bool:
        """Wait for the underlying signal without mutation authority."""
        ...


@dataclasses.dataclass(frozen=True, slots=True)
class LaneSignalView:
    """One scheduler-owned read-only signal view bound to an exact lane."""

    grant: ingestion_lease_store.LeaseGrant
    stop_requested: ExecutionSignal
    grant_lost: ExecutionSignal


@dataclasses.dataclass(frozen=True, slots=True)
class CohortRecordIdentity:
    """Complete immutable identity for one counted cohort record."""

    grant: ingestion_lease_store.LeaseGrant
    member: ingestion_lease_store.LeaseMemberIdentity
    page_sequence: int
    feed_id: uuid.UUID
    cohort_timestamp: datetime.datetime | None
    source_order: int
    local_sequence: int

    def __post_init__(self) -> None:
        if self.member.feed_id != self.feed_id:
            message = "member Feed does not match record Feed"
            raise ValueError(message)
        _require_nonnegative_integer(self.page_sequence, "page_sequence")
        _require_nonnegative_integer(self.source_order, "source_order")
        _require_nonnegative_integer(self.local_sequence, "local_sequence")
        _require_utc_timestamp(self.cohort_timestamp, "cohort_timestamp")


@dataclasses.dataclass(frozen=True, slots=True)
class CallExecution:
    """Stable exact-identity call view passed across the executor boundary."""

    identity: CohortRecordIdentity
    payload: object

    @property
    def grant(self) -> ingestion_lease_store.LeaseGrant:
        """Return the predecessor-owned complete grant identity."""
        return self.identity.grant

    @property
    def feed_id(self) -> uuid.UUID:
        """Return the predecessor-owned Feed identity."""
        return self.identity.feed_id

    @property
    def source_timestamp(self) -> datetime.datetime | None:
        """Return the exact normalized cohort timestamp."""
        return self.identity.cohort_timestamp


class CallSettlement(enum.StrEnum):
    """Closed terminal or release notification for one submitted call."""

    COMPLETED = "completed"
    RETRYABLE = "retryable"
    AUTHORITY_LOST = "authority_lost"
    MEMBERSHIP_REJECTED = "membership_rejected"
    ABORTED = "aborted"
    REPLAY_SAFE_RELEASE = "replay_safe_release"
    REPLAY_BLOCKED = "replay_blocked"


@dataclasses.dataclass(frozen=True, slots=True)
class CallSubmission:
    """One opaque source-order call offered to an exact grant lane.

    Attributes:
        feed_id: Authoritative Feed identity used for stable shard affinity.
        source_timestamp: Source metadata retained without ordering authority.
        payload: Opaque adapter input retained only by its counted record.
    """

    feed_id: uuid.UUID
    source_timestamp: datetime.datetime | None
    payload: object
    settlement_observer: typing.Callable[[CallSettlement], None] | None = None

    def __post_init__(self) -> None:
        _require_utc_timestamp(self.source_timestamp, "source_timestamp")


class _AdmissionStatus(enum.StrEnum):
    UNUSED = "unused"
    INVOKING = "invoking"
    ACCEPTED = "accepted"
    FAILED = "failed"


@dataclasses.dataclass(slots=True)
class _AdmissionState:
    status: _AdmissionStatus = _AdmissionStatus.UNUSED


@dataclasses.dataclass(frozen=True, slots=True)
class CohortSubmission:
    """One exact Feed/page/timestamp cohort offered atomically."""

    member: ingestion_lease_store.LeaseMemberIdentity
    feed_id: uuid.UUID
    cohort_timestamp: datetime.datetime | None
    calls: tuple[CallSubmission, ...]
    admission_hook: typing.Callable[
        [tuple[CohortRecordIdentity, ...]],
        None,
    ]
    _admission_state: _AdmissionState = dataclasses.field(
        default_factory=_AdmissionState,
        init=False,
        repr=False,
        compare=False,
    )

    def __post_init__(self) -> None:
        if self.member.feed_id != self.feed_id:
            message = "cohort member Feed does not match cohort Feed"
            raise ValueError(message)
        _require_utc_timestamp(self.cohort_timestamp, "cohort_timestamp")
        if not self.calls:
            message = "calls must be a nonempty immutable tuple"
            raise ValueError(message)
        if self.cohort_timestamp is None and len(self.calls) != 1:
            message = "a missing-timestamp cohort must be a singleton"
            raise ValueError(message)

    def _begin_admission(self) -> None:
        if self._admission_state.status is not _AdmissionStatus.UNUSED:
            message = "cohort admission hook is not unused"
            raise CohortIntegrityError(message)
        self._admission_state.status = _AdmissionStatus.INVOKING

    def _accept_admission(self) -> None:
        if self._admission_state.status is not _AdmissionStatus.INVOKING:
            message = "cohort admission hook did not enter once"
            raise CohortIntegrityError(message)
        self._admission_state.status = _AdmissionStatus.ACCEPTED

    def _fail_admission(self) -> None:
        self._admission_state.status = _AdmissionStatus.FAILED


def _require_utc_timestamp(
    value: datetime.datetime | None,
    name: str,
) -> None:
    if value is None:
        return
    if value.utcoffset() != datetime.timedelta(0):
        message = f"{name} must be UTC-aware"
        raise ValueError(message)


@dataclasses.dataclass(frozen=True, slots=True)
class BoundaryWork:
    """One trailing Feed boundary offered after a page's call stream.

    Attributes:
        member: Store-issued member identity retained through persistence.
        target: Validated UTC completion boundary that may only coalesce up.
    """

    member: ingestion_lease_store.LeaseMemberIdentity
    target: datetime.datetime

    def __post_init__(self) -> None:
        if self.target.utcoffset() != datetime.timedelta(0):
            message = "target must be UTC-aware"
            raise ValueError(message)

    @property
    def feed_id(self) -> uuid.UUID:
        """Return the member's authoritative Feed identity cheaply."""
        return self.member.feed_id


class BoundaryDisposition(enum.StrEnum):
    """Closed caller-correlated outcome for one boundary target."""

    COMMITTED = "committed"
    MEMBER_REJECTED = "member_rejected"
    RETRYABLE = "retryable"


@dataclasses.dataclass(frozen=True, slots=True)
class BoundaryResult:
    """One disposition correlated to the caller's immutable target."""

    boundary: BoundaryWork
    disposition: BoundaryDisposition


@dataclasses.dataclass(frozen=True, slots=True)
class BoundaryBatchCommitted:
    """Caller-ordered settled results beneath an accepted exact grant."""

    results: tuple[BoundaryResult, ...]


@dataclasses.dataclass(frozen=True, slots=True)
class BoundaryGrantRejected:
    """The committer rejected the complete exact grant or fence."""


@dataclasses.dataclass(frozen=True, slots=True)
class BoundaryBatchRetryable:
    """One whole physical batch attempt settled transiently."""


type BoundaryCommitResult = (
    BoundaryBatchCommitted | BoundaryBatchRetryable | BoundaryGrantRejected
)


class BoundaryCommitter(typing.Protocol):
    """Narrow exact-grant seam for deterministic boundary settlement."""

    async def commit(
        self,
        grant: ingestion_lease_store.LeaseGrant,
        boundaries: tuple[BoundaryWork, ...],
        *,
        final_logical: bool,
    ) -> BoundaryCommitResult:
        """Attempt one immutable physical or final logical batch."""
        ...


@dataclasses.dataclass(frozen=True, slots=True)
class _CallWork:
    """One source-order call submission before local shard registration."""

    feed_id: uuid.UUID
    grant: ingestion_lease_store.LeaseGrant
    member: ingestion_lease_store.LeaseMemberIdentity
    source_order: int
    cohort_timestamp: datetime.datetime | None
    payload: object
    page_sequence: int
    settlement_observer: typing.Callable[[CallSettlement], None] | None = None

    def __post_init__(self) -> None:
        if self.member.feed_id != self.feed_id:
            message = "member Feed does not match work Feed"
            raise ValueError(message)
        _require_nonnegative_integer(self.source_order, "source_order")
        _require_utc_timestamp(self.cohort_timestamp, "cohort_timestamp")
        _require_nonnegative_integer(self.page_sequence, "page_sequence")


@dataclasses.dataclass(frozen=True, slots=True)
class _CallRecord:
    """One distinct counted call with its scheduler-local identity."""

    work: _CallWork
    identity: CohortRecordIdentity

    def __post_init__(self) -> None:
        expected = (
            self.work.grant,
            self.work.member,
            self.work.page_sequence,
            self.work.feed_id,
            self.work.cohort_timestamp,
            self.work.source_order,
        )
        actual = (
            self.identity.grant,
            self.identity.member,
            self.identity.page_sequence,
            self.identity.feed_id,
            self.identity.cohort_timestamp,
            self.identity.source_order,
        )
        if actual != expected or self.identity.member is not self.work.member:
            message = "record identity does not match predecessor work"
            raise ValueError(message)

    @property
    def local_sequence(self) -> int:
        return self.identity.local_sequence

    @property
    def feed_id(self) -> uuid.UUID:
        return self.work.feed_id

    @property
    def grant(self) -> ingestion_lease_store.LeaseGrant:
        return self.work.grant

    def execution(self) -> CallExecution:
        """Project the private record into its stable executor value."""
        return CallExecution(
            identity=self.identity,
            payload=self.work.payload,
        )


@dataclasses.dataclass(slots=True)
class _CohortControl:
    """One active cohort's bounded synchronous capability state."""

    identities: tuple[CohortRecordIdentity, ...]
    active: bool = False
    retention_request: OutcomeUnknownRetentionRequest | None = None
    known_handoff: object | None = None
    integrity_failure: BaseException | None = None
    retained_outcome: object | None = None


@dataclasses.dataclass(frozen=True, slots=True)
class _CohortRecord:
    """One Feed FIFO unit retaining N distinct counted records."""

    records: tuple[_CallRecord, ...]
    control: _CohortControl
    signals: LaneSignalView

    def __post_init__(self) -> None:
        if not self.records:
            message = "records must be a nonempty immutable tuple"
            raise ValueError(message)
        first = self.records[0]
        identities = tuple(record.identity for record in self.records)
        if self.control.identities != identities:
            message = "cohort control identities do not match records"
            raise ValueError(message)
        if self.signals.grant != first.grant:
            message = "cohort signals crossed grant identity"
            raise ValueError(message)
        if any(
            record.grant != first.grant
            or record.feed_id != first.feed_id
            or record.identity.page_sequence != first.identity.page_sequence
            or record.identity.cohort_timestamp
            != first.identity.cohort_timestamp
            or record.identity.member is not first.identity.member
            for record in self.records
        ):
            message = "cohort records crossed identity"
            raise ValueError(message)

    @property
    def feed_id(self) -> uuid.UUID:
        return self.records[0].feed_id

    @property
    def grant(self) -> ingestion_lease_store.LeaseGrant:
        return self.records[0].grant

    @property
    def page_sequence(self) -> int:
        return self.records[0].identity.page_sequence

    @property
    def identities(self) -> tuple[CohortRecordIdentity, ...]:
        return self.control.identities

    @property
    def local_sequence(self) -> int:
        """Return the first sequence for private one-unit diagnostics."""
        return self.records[0].local_sequence

    @property
    def work(self) -> _CallWork:
        """Return the first work value for one-unit compatibility tests."""
        return self.records[0].work


@dataclasses.dataclass(frozen=True, slots=True)
class _BoundaryInput:
    """One page-local boundary before counted shard registration."""

    boundary: BoundaryWork
    grant: ingestion_lease_store.LeaseGrant
    source_order: int
    page_sequence: int

    def __post_init__(self) -> None:
        _require_nonnegative_integer(self.source_order, "source_order")
        _require_nonnegative_integer(self.page_sequence, "page_sequence")


@dataclasses.dataclass(slots=True)
class _BoundaryRecord:
    """One counted pending or immutable detached boundary observation."""

    grant: ingestion_lease_store.LeaseGrant
    member: ingestion_lease_store.LeaseMemberIdentity
    local_sequence: int
    source_order: int
    created_page_sequence: int
    target: datetime.datetime
    stable_target: datetime.datetime | None
    provisional_page_sequence: int | None
    provisional_count: int
    state: _RecordState
    aborted_page_sequence: int | None = None
    retry_suspended: bool = False
    promotion_page_sequence: int | None = None
    promotion_rollback_target: datetime.datetime | None = None

    @property
    def feed_id(self) -> uuid.UUID:
        return self.member.feed_id

    def detached_work(self) -> BoundaryWork:
        """Return the immutable target passed across the I/O boundary."""
        return BoundaryWork(self.member, self.target)


class _BoundaryPressureResult(enum.StrEnum):
    """Bounded result returned to one pressure-blocked page episode."""

    COMPLETED = "completed"
    RETRYABLE = "retryable"


class CohortRecordClosureState(enum.StrEnum):
    """Closed physical-closure state for one cohort record."""

    DURABLY_CLOSED = "durably_closed"
    FINAL_CLOSURE_PENDING = "final_closure_pending"
    REPLAY_SAFE_RELEASE = "replay_safe_release"
    OUTCOME_UNKNOWN = "outcome_unknown"


class CohortRecordTerminalReason(enum.StrEnum):
    """Closed predecessor-owned reason for one record terminal fact."""

    FULL_PIPELINE = "full_pipeline"
    TERMINAL_ITEM_SKIP = "terminal_item_skip"
    PUBLICATION_EXHAUSTED = "publication_exhausted"
    PUBLICATION_ABANDONED = "publication_abandoned"
    REPLAYABLE_DIRECT = "replayable_direct"
    STOPPED = "stopped"
    AUTHORITY_LOST = "authority_lost"
    MEMBERSHIP_REJECTED = "membership_rejected"
    RETRYABLE = "retryable"
    INTEGRITY_FAILURE = "integrity_failure"
    OUTCOME_UNKNOWN = "outcome_unknown"


class CohortTerminalDisposition(enum.StrEnum):
    """Closed whole-cohort disposition carried by every executor outcome."""

    SETTLED = "settled"
    FINAL_CLOSURE_PENDING = "final_closure_pending"
    REPLAYABLE_DIRECT = "replayable_direct"
    RETRYABLE = "retryable"
    STOPPED = "stopped"
    AUTHORITY_LOST = "authority_lost"
    MEMBERSHIP_REJECTED = "membership_rejected"
    INTEGRITY_FAILURE = "integrity_failure"
    OUTCOME_UNKNOWN = "outcome_unknown"


def _normalize_failure_detail(value: str) -> str:
    normalized = " ".join(value.split())
    lowered = normalized.casefold()
    if any(
        forbidden in lowered
        for forbidden in (
            "http://",
            "https://",
            "authorization",
            "token=",
        )
    ):
        message = "failure detail contains forbidden sensitive material"
        raise ValueError(message)
    return status_reason_detail.cap_status_reason_detail_for_storage(normalized)


@dataclasses.dataclass(frozen=True, slots=True)
class CohortItemFailureFact:
    """Bounded canonical evidence for one terminal item skip."""

    status_reason: feed_store.FeedStatusReason
    detail: str

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "detail",
            _normalize_failure_detail(self.detail),
        )


@dataclasses.dataclass(frozen=True, slots=True)
class CohortDirectFailureFact:
    """Bounded selected direct pipeline-failure evidence."""

    status_reason: feed_store.FeedStatusReason
    detail: str

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "detail",
            _normalize_failure_detail(self.detail),
        )


@dataclasses.dataclass(frozen=True, slots=True)
class CohortRecordTerminalFact:
    """One exact identity's closed immutable execution fact."""

    identity: CohortRecordIdentity
    participated: bool
    closure_state: CohortRecordClosureState
    full_pipeline_completed: bool
    terminal_reason: CohortRecordTerminalReason
    item_failure: CohortItemFailureFact | None = None
    direct_failure: CohortDirectFailureFact | None = None

    def __post_init__(self) -> None:
        self._validate_implications()

    def _validate_implications(self) -> None:
        closure_reasons = {
            CohortRecordClosureState.DURABLY_CLOSED: {
                CohortRecordTerminalReason.FULL_PIPELINE,
                CohortRecordTerminalReason.TERMINAL_ITEM_SKIP,
                CohortRecordTerminalReason.PUBLICATION_EXHAUSTED,
                CohortRecordTerminalReason.PUBLICATION_ABANDONED,
            },
            CohortRecordClosureState.FINAL_CLOSURE_PENDING: {
                CohortRecordTerminalReason.TERMINAL_ITEM_SKIP,
            },
            CohortRecordClosureState.REPLAY_SAFE_RELEASE: {
                CohortRecordTerminalReason.REPLAYABLE_DIRECT,
                CohortRecordTerminalReason.STOPPED,
                CohortRecordTerminalReason.AUTHORITY_LOST,
                CohortRecordTerminalReason.MEMBERSHIP_REJECTED,
                CohortRecordTerminalReason.RETRYABLE,
            },
            CohortRecordClosureState.OUTCOME_UNKNOWN: {
                CohortRecordTerminalReason.INTEGRITY_FAILURE,
                CohortRecordTerminalReason.OUTCOME_UNKNOWN,
            },
        }
        if self.terminal_reason not in closure_reasons[self.closure_state]:
            message = "record closure state and terminal reason disagree"
            raise CohortIntegrityError(message)
        if self.full_pipeline_completed != (
            self.terminal_reason is CohortRecordTerminalReason.FULL_PIPELINE
        ):
            message = "full pipeline flag and terminal reason disagree"
            raise CohortIntegrityError(message)
        if self.full_pipeline_completed and (
            not self.participated
            or self.closure_state is not CohortRecordClosureState.DURABLY_CLOSED
        ):
            message = "full pipeline requires durable participation"
            raise CohortIntegrityError(message)
        if (
            self.closure_state is CohortRecordClosureState.DURABLY_CLOSED
            and not self.participated
        ):
            message = "durable closure requires participation"
            raise CohortIntegrityError(message)
        item_skip = (
            self.terminal_reason
            is CohortRecordTerminalReason.TERMINAL_ITEM_SKIP
        )
        if (self.item_failure is not None) != item_skip:
            message = "item failure must exist exactly for terminal item skip"
            raise CohortIntegrityError(message)
        direct_reason = self.terminal_reason in {
            CohortRecordTerminalReason.REPLAYABLE_DIRECT,
            CohortRecordTerminalReason.PUBLICATION_EXHAUSTED,
        }
        if (self.direct_failure is not None) != direct_reason:
            message = "direct failure and selected terminal reason disagree"
            raise CohortIntegrityError(message)
        if item_skip and not self.participated:
            message = "terminal item skip requires participation"
            raise CohortIntegrityError(message)


@dataclasses.dataclass(frozen=True, slots=True)
class CohortTerminalFacts:
    """Ordered exact facts and one closed whole-cohort disposition."""

    records: tuple[CohortRecordTerminalFact, ...]
    disposition: CohortTerminalDisposition

    def __post_init__(self) -> None:
        if not self.records:
            message = "records must be a nonempty immutable tuple"
            raise ValueError(message)
        identities = tuple(record.identity for record in self.records)
        if len(set(identities)) != len(identities):
            message = "terminal facts contain duplicate record identities"
            raise CohortIntegrityError(message)
        if tuple(identity.source_order for identity in identities) != tuple(
            sorted(identity.source_order for identity in identities)
        ):
            message = "terminal facts are not in source order"
            raise CohortIntegrityError(message)
        if tuple(identity.local_sequence for identity in identities) != tuple(
            sorted(identity.local_sequence for identity in identities)
        ):
            message = "terminal facts are not in local-sequence order"
            raise CohortIntegrityError(message)

    @property
    def identities(self) -> tuple[CohortRecordIdentity, ...]:
        """Return the exact predecessor-owned identity tuple."""
        return tuple(record.identity for record in self.records)


class FinalRecordReleaseBasis(enum.StrEnum):
    """Closed generic proof authorizing one final-record transition."""

    DURABLE_SOURCE_CLOSURE = "durable_source_closure"
    ACCEPTED_NO_PROGRESS = "accepted_no_progress"
    ACCEPTED_REPLAYABLE_FEED = "accepted_replayable_feed"
    ACCEPTED_MEMBER_RETIREMENT = "accepted_member_retirement"


@dataclasses.dataclass(frozen=True, slots=True)
class FinalRecordClosureResolution:
    """Exact generic closure for one held final-pending record."""

    identity: CohortRecordIdentity
    closure_state: CohortRecordClosureState
    release_basis: FinalRecordReleaseBasis

    def __post_init__(self) -> None:
        if self.closure_state not in {
            CohortRecordClosureState.DURABLY_CLOSED,
            CohortRecordClosureState.REPLAY_SAFE_RELEASE,
        }:
            message = "final closure must be durable or replay-safe"
            raise CohortIntegrityError(message)
        if (self.closure_state is CohortRecordClosureState.DURABLY_CLOSED) != (
            self.release_basis is FinalRecordReleaseBasis.DURABLE_SOURCE_CLOSURE
        ):
            message = "final closure state and release basis disagree"
            raise CohortIntegrityError(message)


def _require_uuid_tuple(
    value: tuple[uuid.UUID, ...],
    name: str,
) -> tuple[uuid.UUID, ...]:
    if len(set(value)) != len(value):
        message = f"{name} must not contain duplicates"
        raise CohortIntegrityError(message)
    if value != tuple(sorted(value, key=lambda item: item.int)):
        message = f"{name} must be in deterministic UUID order"
        raise CohortIntegrityError(message)
    return value


def _require_member_retirement_tuple(
    grant: ingestion_lease_store.LeaseGrant,
    value: tuple[ingestion_lease_store.LeaseMemberIdentity, ...],
) -> tuple[ingestion_lease_store.LeaseMemberIdentity, ...]:
    """Validate deterministic accepted retirement authority for one grant."""
    members = value
    if len({id(member) for member in members}) != len(members) or len(
        {member.feed_id for member in members}
    ) != len(members):
        message = "member_retirements must not repeat a member or Feed"
        raise CohortIntegrityError(message)
    expected = tuple(
        sorted(
            members,
            key=lambda member: (
                member.feed_id.int,
                member.source_feed_id,
            ),
        )
    )
    if members != expected:
        message = "member_retirements must be in deterministic Feed order"
        raise CohortIntegrityError(message)
    for member in members:
        try:
            ingestion_lease_store._require_member_binding(  # noqa: SLF001
                grant,
                member,
            )
        except (TypeError, ValueError) as exc:
            message = "member retirement crossed complete grant"
            raise CohortIntegrityError(message) from exc
    return members


@dataclasses.dataclass(frozen=True, slots=True)
class PageFinalizationContext:
    """Bounded exact evidence supplied to one source page finalizer."""

    grant: ingestion_lease_store.LeaseGrant
    page_sequence: int
    candidate: cursor_policy.PageCandidate
    cohort_terminal_facts: tuple[CohortTerminalFacts, ...]
    unresolved_replay_feed_ids: tuple[uuid.UUID, ...]
    locally_retired_members: tuple[
        ingestion_lease_store.LeaseMemberIdentity,
        ...,
    ]
    replay_blocked_feed_ids: tuple[uuid.UUID, ...]
    candidate_boundaries: tuple[BoundaryWork, ...]

    def __post_init__(self) -> None:
        _require_nonnegative_integer(self.page_sequence, "page_sequence")
        if type(self.candidate) not in {
            cursor_policy.PageCursorCandidate,
            cursor_policy.NoProgressPageCandidate,
        }:
            message = "candidate must be an exact PageCandidate"
            raise TypeError(message)
        if (
            self.candidate.grant != self.grant
            or self.candidate.page_sequence != self.page_sequence
        ):
            message = "candidate crossed finalization context"
            raise CohortIntegrityError(message)
        self._validate_facts()
        replay = _require_uuid_tuple(
            self.unresolved_replay_feed_ids,
            "unresolved_replay_feed_ids",
        )
        _require_uuid_tuple(
            self.replay_blocked_feed_ids,
            "replay_blocked_feed_ids",
        )
        replay_facts = {
            record.identity.feed_id
            for facts in self.cohort_terminal_facts
            for record in facts.records
            if record.terminal_reason
            is CohortRecordTerminalReason.REPLAYABLE_DIRECT
        }
        if set(replay) != replay_facts:
            message = "unresolved replay Feeds crossed terminal facts"
            raise CohortIntegrityError(message)
        if len({id(member) for member in self.locally_retired_members}) != len(
            self.locally_retired_members
        ):
            message = "locally_retired_members contains duplicate identity"
            raise CohortIntegrityError(message)
        for member in self.locally_retired_members:
            try:
                ingestion_lease_store._require_member_binding(  # noqa: SLF001
                    self.grant,
                    member,
                )
            except (TypeError, ValueError) as exc:
                message = "locally retired member crossed complete grant"
                raise CohortIntegrityError(message) from exc
        for boundary in self.candidate_boundaries:
            try:
                ingestion_lease_store._require_member_binding(  # noqa: SLF001
                    self.grant,
                    boundary.member,
                )
            except (TypeError, ValueError) as exc:
                message = "candidate boundary crossed complete grant"
                raise CohortIntegrityError(message) from exc

    def _validate_facts(self) -> None:
        seen: set[CohortRecordIdentity] = set()
        order = []
        for facts in self.cohort_terminal_facts:
            identities = facts.identities
            if any(
                identity.grant != self.grant
                or identity.page_sequence != self.page_sequence
                for identity in identities
            ):
                message = "terminal facts crossed finalization page"
                raise CohortIntegrityError(message)
            if seen.intersection(identities):
                message = "terminal facts duplicate registered identity"
                raise CohortIntegrityError(message)
            seen.update(identities)
            first = identities[0]
            order.append(
                (
                    first.feed_id.int,
                    first.source_order,
                    first.local_sequence,
                )
            )
        if order != sorted(order):
            message = "cohort terminal facts are not deterministic"
            raise CohortIntegrityError(message)


@dataclasses.dataclass(frozen=True, slots=True)
class _AcceptedFinalPage:
    """Shared exact fields for one accepted final-page disposition.

    Attributes:
        grant: Complete Lease authority for this page.
        page_sequence: Exact grant-local page sequence.
        candidate: Object-identical cursor candidate accepted by the source.
        boundary_results: Ordered results for every submitted page boundary.
        final_closure_resolutions: Complete ordered final-pending resolution.
        source_evidence: Source-owned accepted transaction evidence.
        member_retirements: Deterministic exact issued members whose accepted
            child commands were rejected or already retired on this page.
    """

    grant: ingestion_lease_store.LeaseGrant
    page_sequence: int
    candidate: cursor_policy.PageCandidate
    boundary_results: tuple[BoundaryResult, ...]
    final_closure_resolutions: tuple[FinalRecordClosureResolution, ...]
    source_evidence: object
    member_retirements: tuple[
        ingestion_lease_store.LeaseMemberIdentity,
        ...,
    ] = ()

    def __post_init__(self) -> None:
        _require_final_result_identity(
            self.grant,
            self.page_sequence,
            self.candidate,
        )
        _require_member_retirement_tuple(self.grant, self.member_retirements)


@dataclasses.dataclass(frozen=True, slots=True)
class FinalPageCovered(_AcceptedFinalPage):
    """Accepted advancing or equal progress for one exact candidate."""


@dataclasses.dataclass(frozen=True, slots=True)
class FinalPageNoProgress(_AcceptedFinalPage):
    """Accepted source lifecycle for one unusable-lastPos candidate."""


@dataclasses.dataclass(frozen=True, slots=True)
class FinalPageReplayable(_AcceptedFinalPage):
    """Accepted source lifecycle while exact Feed work remains replayable."""


def _require_final_result_identity(
    grant: object,
    page_sequence: object,
    candidate: object,
) -> None:
    if not isinstance(grant, ingestion_lease_store.LeaseGrant):
        message = "grant must be a LeaseGrant"
        raise TypeError(message)
    if isinstance(page_sequence, bool) or not isinstance(page_sequence, int):
        message = "page_sequence must be an integer"
        raise TypeError(message)
    exact_sequence = _require_nonnegative_integer(
        page_sequence,
        "page_sequence",
    )
    if type(candidate) not in {
        cursor_policy.PageCursorCandidate,
        cursor_policy.NoProgressPageCandidate,
    }:
        message = "candidate must be an exact PageCandidate"
        raise TypeError(message)
    exact_candidate = typing.cast("cursor_policy.PageCandidate", candidate)
    if (
        exact_candidate.grant != grant
        or exact_candidate.page_sequence != exact_sequence
    ):
        message = "final result crossed candidate identity"
        raise CohortIntegrityError(message)


@dataclasses.dataclass(frozen=True, slots=True)
class FinalPageRetryable:
    """Definitive proof that the final mutation never started."""

    grant: ingestion_lease_store.LeaseGrant
    page_sequence: int
    candidate: cursor_policy.PageCandidate
    commit_child_mutations_started: bool
    mutation_could_have_committed: bool

    def __post_init__(self) -> None:
        _require_final_result_identity(
            self.grant,
            self.page_sequence,
            self.candidate,
        )
        if (
            self.commit_child_mutations_started
            or self.mutation_could_have_committed
        ):
            message = "retryable final page lacks definitive no-commit proof"
            raise CohortIntegrityError(message)


@dataclasses.dataclass(frozen=True, slots=True)
class FinalPageGrantRejected:
    """The finalizer rejected this complete exact grant."""

    grant: ingestion_lease_store.LeaseGrant
    page_sequence: int
    candidate: cursor_policy.PageCandidate

    def __post_init__(self) -> None:
        _require_final_result_identity(
            self.grant,
            self.page_sequence,
            self.candidate,
        )


@dataclasses.dataclass(frozen=True, slots=True)
class FinalPageOutcomeUnknown:
    """The final mutation may have committed and must not be resubmitted."""

    grant: ingestion_lease_store.LeaseGrant
    page_sequence: int
    candidate: cursor_policy.PageCandidate

    def __post_init__(self) -> None:
        _require_final_result_identity(
            self.grant,
            self.page_sequence,
            self.candidate,
        )


class _FinalPageUncertainty(enum.StrEnum):
    """Disjoint retained reason for an unaccepted final-page transition."""

    OUTCOME_UNKNOWN = "outcome_unknown"
    INTEGRITY_FAILURE = "integrity_failure"


type FinalPageResult = (
    FinalPageCovered
    | FinalPageNoProgress
    | FinalPageReplayable
    | FinalPageRetryable
    | FinalPageGrantRejected
    | FinalPageOutcomeUnknown
)


class PageFinalizer(typing.Protocol):
    """One exact, once-only source lifecycle finalization seam."""

    async def finalize_page(
        self,
        context: PageFinalizationContext,
    ) -> FinalPageResult:
        """Return one closed final disposition for ``context``."""
        ...


@dataclasses.dataclass(frozen=True, slots=True)
class SchedulerPageEvidence:
    """Bounded scalar evidence captured by one page's scheduling owners.

    Attributes:
        admitted_record_count: Call records registered by this exact page.
        admitted_cohort_count: Call cohorts registered by this exact page.
        terminal_record_count: Registered records with a terminal disposition.
        replay_blocked_record_count: Records rejected by replay protection.
        total_queue_wait_seconds: Sum of completed and live cohort residence.
        maximum_queue_wait_seconds: Longest observed cohort residence.
        oldest_queue_age_seconds: Oldest observed live cohort residence.
        pressure_encountered: Whether admission waited under pressure.
        pressure_wait_count: Number of distinct pressure waits.
        pressure_wait_seconds: Total time spent blocked under pressure.
        maximum_held_count: Peak exact-page call and boundary records held.
        maximum_queue_depth: Peak exact-page queued call-record count.
        maximum_worker_utilization_numerator: Peak exact-page active workers.
        worker_utilization_denominator: Fixed scheduler worker count.
        early_flush_attempt_count: Exact-page non-final boundary attempts.
        final_flush_attempt_count: Exact-page final logical attempts.
        total_flush_latency_seconds: Sum of exact-page flush latencies.
        maximum_flush_latency_seconds: Longest exact-page flush latency.
        fence_rejection_count: Exact-grant boundary/final fence rejections.
        member_rejection_count: Call and boundary member rejections.
    """

    admitted_record_count: int
    admitted_cohort_count: int
    terminal_record_count: int
    replay_blocked_record_count: int
    total_queue_wait_seconds: float
    maximum_queue_wait_seconds: float
    oldest_queue_age_seconds: float
    pressure_encountered: bool
    pressure_wait_count: int
    pressure_wait_seconds: float
    maximum_held_count: int
    maximum_queue_depth: int
    maximum_worker_utilization_numerator: int
    worker_utilization_denominator: int
    early_flush_attempt_count: int
    final_flush_attempt_count: int
    total_flush_latency_seconds: float
    maximum_flush_latency_seconds: float
    fence_rejection_count: int
    member_rejection_count: int

    def __post_init__(self) -> None:  # noqa: PLR0912
        integer_fields = (
            ("admitted_record_count", self.admitted_record_count),
            ("admitted_cohort_count", self.admitted_cohort_count),
            ("terminal_record_count", self.terminal_record_count),
            ("replay_blocked_record_count", self.replay_blocked_record_count),
            ("maximum_held_count", self.maximum_held_count),
            ("maximum_queue_depth", self.maximum_queue_depth),
            (
                "maximum_worker_utilization_numerator",
                self.maximum_worker_utilization_numerator,
            ),
            ("pressure_wait_count", self.pressure_wait_count),
            ("early_flush_attempt_count", self.early_flush_attempt_count),
            ("final_flush_attempt_count", self.final_flush_attempt_count),
            ("fence_rejection_count", self.fence_rejection_count),
            ("member_rejection_count", self.member_rejection_count),
        )
        for name, value in integer_fields:
            _require_nonnegative_integer(value, name)
        _require_positive_integer(
            self.worker_utilization_denominator,
            "worker_utilization_denominator",
        )
        duration_fields = (
            ("total_queue_wait_seconds", self.total_queue_wait_seconds),
            ("maximum_queue_wait_seconds", self.maximum_queue_wait_seconds),
            ("oldest_queue_age_seconds", self.oldest_queue_age_seconds),
            ("pressure_wait_seconds", self.pressure_wait_seconds),
            ("total_flush_latency_seconds", self.total_flush_latency_seconds),
            (
                "maximum_flush_latency_seconds",
                self.maximum_flush_latency_seconds,
            ),
        )
        for name, value in duration_fields:
            _require_nonnegative_finite_number(value, name)
        if self.admitted_cohort_count > self.admitted_record_count:
            message = "admitted cohorts cannot exceed admitted records"
            raise ValueError(message)
        if self.terminal_record_count > self.admitted_record_count:
            message = "terminal records cannot exceed admitted records"
            raise ValueError(message)
        if (
            self.oldest_queue_age_seconds > self.maximum_queue_wait_seconds
            or self.maximum_queue_wait_seconds > self.total_queue_wait_seconds
        ):
            message = "queue age and residence totals are inconsistent"
            raise ValueError(message)
        if (self.total_queue_wait_seconds == 0.0) != (
            self.maximum_queue_wait_seconds == 0.0
        ):
            message = "queue residence total and maximum must zero together"
            raise ValueError(message)
        if self.admitted_cohort_count == 0 and any(
            value != 0.0
            for value in (
                self.total_queue_wait_seconds,
                self.maximum_queue_wait_seconds,
                self.oldest_queue_age_seconds,
            )
        ):
            message = "queue timing requires an admitted cohort"
            raise ValueError(message)
        encountered = self.pressure_wait_count > 0
        if self.pressure_encountered is not encountered:
            message = "pressure encounter and wait count disagree"
            raise ValueError(message)
        if self.pressure_wait_count == 0 and self.pressure_wait_seconds != 0.0:
            message = "pressure time requires an observed pressure wait"
            raise ValueError(message)
        if self.maximum_queue_depth > self.maximum_held_count:
            message = "queue depth cannot exceed held work"
            raise ValueError(message)
        if (
            self.maximum_worker_utilization_numerator
            > self.worker_utilization_denominator
        ):
            message = "worker utilization exceeds its fixed denominator"
            raise ValueError(message)
        flush_attempt_count = (
            self.early_flush_attempt_count + self.final_flush_attempt_count
        )
        if flush_attempt_count == 0 and any(
            value != 0.0
            for value in (
                self.total_flush_latency_seconds,
                self.maximum_flush_latency_seconds,
            )
        ):
            message = "flush latency requires an actual flush attempt"
            raise ValueError(message)
        if (
            self.maximum_flush_latency_seconds
            > self.total_flush_latency_seconds
        ):
            message = "maximum flush latency exceeds total latency"
            raise ValueError(message)
        if (self.total_flush_latency_seconds == 0.0) != (
            self.maximum_flush_latency_seconds == 0.0
        ):
            message = "flush latency total and maximum must zero together"
            raise ValueError(message)


@dataclasses.dataclass(frozen=True, slots=True)
class SettledPage:
    """Accepted page settlement plus exact scheduler and source evidence."""

    candidate: cursor_policy.PageCandidate
    lease_settlement: cursor_policy.PageSettlement
    final_closure_resolutions: tuple[FinalRecordClosureResolution, ...]
    scheduler_evidence: SchedulerPageEvidence
    source_evidence: object

    def __post_init__(self) -> None:
        settlement_types = {
            cursor_policy._CoveredPage,  # noqa: SLF001
            cursor_policy._NoProgressPageSettled,  # noqa: SLF001
            cursor_policy._ReplayablePageSettled,  # noqa: SLF001
        }
        if type(self.candidate) not in {
            cursor_policy.PageCursorCandidate,
            cursor_policy.NoProgressPageCandidate,
        }:
            message = "candidate must be an exact PageCandidate"
            raise TypeError(message)
        if type(self.lease_settlement) not in settlement_types:
            message = "lease_settlement must be privately issued"
            raise TypeError(message)
        if (
            self.lease_settlement.grant != self.candidate.grant
            or self.lease_settlement.page_sequence
            != self.candidate.page_sequence
            or cursor_policy._read_seal(  # noqa: SLF001
                self.lease_settlement
            )
            is not cursor_policy._read_seal(self.candidate)  # noqa: SLF001
        ):
            message = "settled page crossed exact candidate identity"
            raise CohortIntegrityError(message)

    @property
    def grant(self) -> ingestion_lease_store.LeaseGrant:
        """Return the complete grant carried by the exact candidate."""
        return self.candidate.grant

    @property
    def page_sequence(self) -> int:
        """Return the candidate's exact grant-local page sequence."""
        return self.candidate.page_sequence


class OutcomeUnknownCause(enum.StrEnum):
    """Closed external operation whose result may have become durable."""

    COMMIT = "commit"
    PUBLICATION = "publication"


@dataclasses.dataclass(frozen=True, slots=True)
class OutcomeUnknownRetentionRequest:
    """One exact sticky unknown/integrity request for an active cohort."""

    cause: OutcomeUnknownCause
    terminal_facts: CohortTerminalFacts
    failure: BaseException | None = None

    def __post_init__(self) -> None:
        disposition = self.terminal_facts.disposition
        if disposition not in {
            CohortTerminalDisposition.INTEGRITY_FAILURE,
            CohortTerminalDisposition.OUTCOME_UNKNOWN,
        }:
            message = "retention requires unknown or integrity facts"
            raise CohortIntegrityError(message)
        if disposition is CohortTerminalDisposition.INTEGRITY_FAILURE:
            if not isinstance(self.failure, BaseException):
                message = "integrity retention requires its exact failure"
                raise TypeError(message)
        elif self.failure is not None:
            message = "outcome-unknown retention cannot carry a failure"
            raise CohortIntegrityError(message)


_CAPABILITY_SEAL = object()


@dataclasses.dataclass(frozen=True, slots=True)
class CohortRetentionHandle:
    """Scheduler-minted complete-identity sticky-retention capability."""

    identities: tuple[CohortRecordIdentity, ...]
    _callback: typing.Callable[[OutcomeUnknownRetentionRequest], None]
    _seal: object = dataclasses.field(repr=False, compare=False)

    def __post_init__(self) -> None:
        if self._seal is not _CAPABILITY_SEAL:
            message = "retention handle was not scheduler-minted"
            raise CohortIntegrityError(message)

    def retain(self, request: OutcomeUnknownRetentionRequest) -> None:
        """Retain this exact active cohort for uncertainty or integrity."""
        self._callback(request)


@dataclasses.dataclass(frozen=True, slots=True)
class CohortCancellationHandoff:
    """Scheduler-minted complete-identity definitive-cancel capability."""

    identities: tuple[CohortRecordIdentity, ...]
    _callback: typing.Callable[[object], None]
    _seal: object = dataclasses.field(repr=False, compare=False)

    def __post_init__(self) -> None:
        if self._seal is not _CAPABILITY_SEAL:
            message = "cancellation handoff was not scheduler-minted"
            raise CohortIntegrityError(message)

    def settle(self, outcome: object) -> None:
        """Submit one definitive exact known outcome synchronously."""
        self._callback(outcome)


def _issue_retention_handle(
    identities: tuple[CohortRecordIdentity, ...],
    callback: typing.Callable[[OutcomeUnknownRetentionRequest], None],
) -> CohortRetentionHandle:
    return CohortRetentionHandle(identities, callback, _CAPABILITY_SEAL)


def _issue_cancellation_handoff(
    identities: tuple[CohortRecordIdentity, ...],
    callback: typing.Callable[[object], None],
) -> CohortCancellationHandoff:
    return CohortCancellationHandoff(identities, callback, _CAPABILITY_SEAL)


@dataclasses.dataclass(frozen=True, slots=True)
class CohortExecution:
    """One atomic Feed execution retaining N distinct record identities."""

    calls: tuple[CallExecution, ...]
    signals: LaneSignalView
    retention: CohortRetentionHandle
    cancellation_handoff: CohortCancellationHandoff

    def __post_init__(self) -> None:
        if not self.calls:
            message = "calls must be a nonempty immutable tuple"
            raise ValueError(message)
        identities = tuple(call.identity for call in self.calls)
        if identities != self.retention.identities or identities != (
            self.cancellation_handoff.identities
        ):
            message = "cohort execution capabilities crossed identity"
            raise CohortIntegrityError(message)


def _require_outcome_facts(
    facts: CohortTerminalFacts,
    disposition: CohortTerminalDisposition,
) -> CohortTerminalFacts:
    if facts.disposition is not disposition:
        message = "outcome and terminal disposition disagree"
        raise CohortIntegrityError(message)
    return facts


@dataclasses.dataclass(frozen=True, slots=True)
class CallCompleted:
    """Every record is structurally settled and durably closed."""

    facts: CohortTerminalFacts

    def __post_init__(self) -> None:
        facts = _require_outcome_facts(
            self.facts,
            CohortTerminalDisposition.SETTLED,
        )
        if any(
            record.closure_state is not CohortRecordClosureState.DURABLY_CLOSED
            for record in facts.records
        ):
            message = "completed outcome requires durable closure"
            raise CohortIntegrityError(message)


@dataclasses.dataclass(frozen=True, slots=True)
class CallFinalClosurePending:
    """At least one structurally settled record awaits source finalization."""

    facts: CohortTerminalFacts

    def __post_init__(self) -> None:
        facts = _require_outcome_facts(
            self.facts,
            CohortTerminalDisposition.FINAL_CLOSURE_PENDING,
        )
        states = {record.closure_state for record in facts.records}
        if (
            CohortRecordClosureState.FINAL_CLOSURE_PENDING not in states
            or not states
            <= {
                CohortRecordClosureState.DURABLY_CLOSED,
                CohortRecordClosureState.FINAL_CLOSURE_PENDING,
            }
        ):
            message = "final-pending outcome has invalid closure states"
            raise CohortIntegrityError(message)


@dataclasses.dataclass(frozen=True, slots=True)
class CallReplayableDirectFailure:
    """A definitive direct precommit failure replay-releases the cohort."""

    facts: CohortTerminalFacts

    def __post_init__(self) -> None:
        facts = _require_outcome_facts(
            self.facts,
            CohortTerminalDisposition.REPLAYABLE_DIRECT,
        )
        _require_all_replay_safe(facts)
        if not any(
            record.terminal_reason
            is CohortRecordTerminalReason.REPLAYABLE_DIRECT
            for record in facts.records
        ):
            message = "replayable outcome lacks direct failure evidence"
            raise CohortIntegrityError(message)


@dataclasses.dataclass(frozen=True, slots=True)
class CallRetryable:
    """Definitive no-start/precommit evidence replay-releases the cohort."""

    facts: CohortTerminalFacts

    def __post_init__(self) -> None:
        facts = _require_outcome_facts(
            self.facts,
            CohortTerminalDisposition.RETRYABLE,
        )
        _require_all_replay_safe(facts)
        if any(
            record.terminal_reason is not CohortRecordTerminalReason.RETRYABLE
            or record.participated
            for record in facts.records
        ):
            message = "retryable outcome lacks definitive precommit proof"
            raise CohortIntegrityError(message)


@dataclasses.dataclass(frozen=True, slots=True)
class CallStopped:
    """Graceful explicit stop with durable or replay-safe record facts."""

    facts: CohortTerminalFacts

    def __post_init__(self) -> None:
        facts = _require_outcome_facts(
            self.facts,
            CohortTerminalDisposition.STOPPED,
        )
        _require_abort_mix(facts, CohortRecordTerminalReason.STOPPED)


@dataclasses.dataclass(frozen=True, slots=True)
class CallAuthorityLost:
    """Confirmed complete-grant loss with exact terminal facts."""

    facts: CohortTerminalFacts

    def __post_init__(self) -> None:
        facts = _require_outcome_facts(
            self.facts,
            CohortTerminalDisposition.AUTHORITY_LOST,
        )
        _require_abort_mix(
            facts,
            CohortRecordTerminalReason.AUTHORITY_LOST,
        )


@dataclasses.dataclass(frozen=True, slots=True)
class CallMembershipRejected:
    """Exact member rejection replay-releases only its cohort."""

    facts: CohortTerminalFacts

    def __post_init__(self) -> None:
        facts = _require_outcome_facts(
            self.facts,
            CohortTerminalDisposition.MEMBERSHIP_REJECTED,
        )
        _require_all_replay_safe(facts)
        if any(
            record.terminal_reason
            is not CohortRecordTerminalReason.MEMBERSHIP_REJECTED
            for record in facts.records
        ):
            message = "membership outcome contains another reason"
            raise CohortIntegrityError(message)


@dataclasses.dataclass(frozen=True, slots=True)
class CallIntegrityFailure:
    """Malformed evidence retains this cohort with integrity disposition."""

    facts: CohortTerminalFacts
    failure: BaseException

    def __post_init__(self) -> None:
        facts = _require_outcome_facts(
            self.facts,
            CohortTerminalDisposition.INTEGRITY_FAILURE,
        )
        _require_all_unknown(
            facts,
            CohortRecordTerminalReason.INTEGRITY_FAILURE,
        )
        if not isinstance(self.failure, BaseException):
            message = "failure must be a BaseException"
            raise TypeError(message)


@dataclasses.dataclass(frozen=True, slots=True)
class CallOutcomeUnknown:
    """External commit/publication outcome is unknown and retained."""

    facts: CohortTerminalFacts

    def __post_init__(self) -> None:
        facts = _require_outcome_facts(
            self.facts,
            CohortTerminalDisposition.OUTCOME_UNKNOWN,
        )
        _require_all_unknown(
            facts,
            CohortRecordTerminalReason.OUTCOME_UNKNOWN,
        )


def _require_all_replay_safe(facts: CohortTerminalFacts) -> None:
    if any(
        record.closure_state is not CohortRecordClosureState.REPLAY_SAFE_RELEASE
        for record in facts.records
    ):
        message = "outcome requires replay-safe closure"
        raise CohortIntegrityError(message)


def _require_abort_mix(
    facts: CohortTerminalFacts,
    reason: CohortRecordTerminalReason,
) -> None:
    allowed = {
        CohortRecordClosureState.DURABLY_CLOSED,
        CohortRecordClosureState.REPLAY_SAFE_RELEASE,
    }
    if any(record.closure_state not in allowed for record in facts.records):
        message = "abort outcome contains retained or final-pending state"
        raise CohortIntegrityError(message)
    matching_reasons = {
        reason,
        CohortRecordTerminalReason.PUBLICATION_ABANDONED,
    }
    if not any(
        record.terminal_reason in matching_reasons for record in facts.records
    ):
        message = "abort outcome lacks matching terminal reason"
        raise CohortIntegrityError(message)


def _require_all_unknown(
    facts: CohortTerminalFacts,
    reason: CohortRecordTerminalReason,
) -> None:
    if any(
        record.closure_state is not CohortRecordClosureState.OUTCOME_UNKNOWN
        or record.terminal_reason is not reason
        for record in facts.records
    ):
        message = "retained outcome has crossed unknown evidence"
        raise CohortIntegrityError(message)


class LaneCloseReason(enum.StrEnum):
    """Closed reasons accepted by one exact grant lane."""

    PLANNED_DRAIN = "planned_drain"
    AUTHORITY_LOSS = "authority_loss"
    SCHEDULER_SHUTDOWN = "scheduler_shutdown"


@dataclasses.dataclass(frozen=True, slots=True)
class LaneClosed:
    """Proof that one exact lane reached mutation closure.

    Attributes:
        grant: Complete immutable Lease generation that is now closed.
        reason: Strongest close reason settled by the lane coordinator.
    """

    grant: ingestion_lease_store.LeaseGrant
    reason: LaneCloseReason


@dataclasses.dataclass(frozen=True, slots=True)
class Undrained:
    """Conservative result when mutation closure cannot be proved.

    Attributes:
        grant: Unsettled exact Lease generation, or ``None`` for process-wide
            scheduler shutdown.
        reason: Strongest close reason requested before uncertainty remained.
    """

    grant: ingestion_lease_store.LeaseGrant | None
    reason: LaneCloseReason


_ExecutorCompleted = CallCompleted
_ExecutorFinalClosurePending = CallFinalClosurePending
_ExecutorReplayableDirectFailure = CallReplayableDirectFailure
_ExecutorRetryable = CallRetryable
_ExecutorStopped = CallStopped
_ExecutorAuthorityLost = CallAuthorityLost
_ExecutorMembershipRejected = CallMembershipRejected
_ExecutorIntegrityFailure = CallIntegrityFailure
_ExecutorOutcomeUnknown = CallOutcomeUnknown


type _ExecutorOutcome = (
    CallCompleted
    | CallFinalClosurePending
    | CallReplayableDirectFailure
    | CallRetryable
    | CallStopped
    | CallAuthorityLost
    | CallMembershipRejected
    | CallIntegrityFailure
    | CallOutcomeUnknown
)


class CallExecutor(typing.Protocol):
    """Narrow injected seam awaited only by an unlocked fixed worker."""

    async def execute(self, execution: CohortExecution) -> _ExecutorOutcome:
        """Settle one already-registered cohort through a closed outcome."""
        ...


class _RecordState(enum.StrEnum):
    """Counted locations in the shard conservation equation."""

    QUEUED = "queued"
    ACTIVE = "active"
    FINAL_CLOSURE_PENDING = "final_closure_pending"
    OUTCOME_UNKNOWN = "outcome_unknown"
    PENDING_BOUNDARY = "pending_boundary"
    FLUSHING_BOUNDARY = "flushing_boundary"


@dataclasses.dataclass(frozen=True, slots=True)
class _RecordSnapshot:
    """Payload-free bounded identity for one counted call record."""

    local_sequence: int
    feed_id: uuid.UUID
    grant: ingestion_lease_store.LeaseGrant
    source_order: int
    page_sequence: int
    state: _RecordState
    worker_slot: int | None


@dataclasses.dataclass(frozen=True, slots=True)
class _BoundarySnapshot:
    """Payload-free projection of one counted boundary record."""

    local_sequence: int
    feed_id: uuid.UUID
    grant: ingestion_lease_store.LeaseGrant
    source_order: int
    created_page_sequence: int
    target: datetime.datetime
    stable_target: datetime.datetime | None
    provisional_page_sequence: int | None
    provisional_count: int
    state: _RecordState
    retry_suspended: bool


@dataclasses.dataclass(frozen=True, slots=True)
class _WorkerSnapshot:
    """Bounded ownership evidence for one fixed worker slot."""

    slot_id: int
    task_registered: bool
    task_done: bool
    active_sequence: int | None
    cancellation_sequence: int | None


@dataclasses.dataclass(frozen=True, slots=True)
class _ShardSnapshot:
    """Read-only bounded projection of authoritative shard state."""

    held: int
    queued_calls: int
    active_calls: int
    pending_boundaries: int
    flushing_boundaries: int
    pressure_paused: bool
    ready_feeds: tuple[uuid.UUID, ...]
    ready_members: frozenset[uuid.UUID]
    active_feeds: frozenset[uuid.UUID]
    records: tuple[_RecordSnapshot, ...]
    boundaries: tuple[_BoundarySnapshot, ...]
    workers: tuple[_WorkerSnapshot, ...]
    admission_open: bool
    fatal: bool


@dataclasses.dataclass(frozen=True, slots=True)
class _PurgeResult:
    """Exact bounded-scan result without retaining completed outcomes."""

    released_sequences: tuple[int, ...]
    active_sequences: tuple[int, ...]
