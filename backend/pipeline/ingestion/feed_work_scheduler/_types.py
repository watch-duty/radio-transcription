"""Closed immutable vocabulary for bounded Feed-affine scheduling."""

from __future__ import annotations

import dataclasses
import datetime
import enum
import typing
import uuid

from backend.pipeline.storage import (
    feed_store,
    ingestion_lease_store,
    status_reason_detail,
)

PRODUCTION_SHARD_COUNT = 8
PRODUCTION_SHARD_CAPACITY = 500
PRODUCTION_WORKERS_PER_SHARD = 4
PRODUCTION_HIGH_WATER = 400
PRODUCTION_RESUME_AT = 299


def _require_positive_integer(value: object, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        message = f"{name} must be an integer"
        raise TypeError(message)
    if value <= 0:
        message = f"{name} must be positive"
        raise ValueError(message)
    return value


def _require_nonnegative_integer(value: object, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        message = f"{name} must be an integer"
        raise TypeError(message)
    if value < 0:
        message = f"{name} must be nonnegative"
        raise ValueError(message)
    return value


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
    if not isinstance(feed_id, uuid.UUID):
        message = "feed_id must be a UUID"
        raise TypeError(message)
    if not isinstance(limits, _SchedulerLimits):
        message = "limits must be _SchedulerLimits"
        raise TypeError(message)
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

    def __post_init__(self) -> None:
        if not isinstance(self.grant, ingestion_lease_store.LeaseGrant):
            message = "grant must be a LeaseGrant"
            raise TypeError(message)
        for name, signal in (
            ("stop_requested", self.stop_requested),
            ("grant_lost", self.grant_lost),
        ):
            if not callable(getattr(signal, "is_set", None)) or not callable(
                getattr(signal, "wait", None)
            ):
                message = f"{name} must be a read-only execution signal"
                raise TypeError(message)


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
        if not isinstance(self.grant, ingestion_lease_store.LeaseGrant):
            message = "grant must be a LeaseGrant"
            raise TypeError(message)
        if not isinstance(
            self.member,
            ingestion_lease_store.LeaseMemberIdentity,
        ):
            message = "member must be a LeaseMemberIdentity"
            raise TypeError(message)
        if not isinstance(self.feed_id, uuid.UUID):
            message = "feed_id must be a UUID"
            raise TypeError(message)
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

    def __post_init__(self) -> None:
        if type(self.identity) is not CohortRecordIdentity:
            message = "identity must be an exact CohortRecordIdentity"
            raise TypeError(message)

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
        if not isinstance(self.feed_id, uuid.UUID):
            message = "feed_id must be a UUID"
            raise TypeError(message)
        if self.source_timestamp is not None and not isinstance(
            self.source_timestamp,
            datetime.datetime,
        ):
            message = "source_timestamp must be a datetime or None"
            raise TypeError(message)
        _require_utc_timestamp(self.source_timestamp, "source_timestamp")
        if self.settlement_observer is not None and not callable(
            self.settlement_observer
        ):
            message = "settlement_observer must be callable or None"
            raise TypeError(message)


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
        if not isinstance(
            self.member,
            ingestion_lease_store.LeaseMemberIdentity,
        ):
            message = "member must be a LeaseMemberIdentity"
            raise TypeError(message)
        if not isinstance(self.feed_id, uuid.UUID):
            message = "feed_id must be a UUID"
            raise TypeError(message)
        if self.member.feed_id != self.feed_id:
            message = "cohort member Feed does not match cohort Feed"
            raise ValueError(message)
        _require_utc_timestamp(self.cohort_timestamp, "cohort_timestamp")
        if not isinstance(self.calls, tuple) or not self.calls:
            message = "calls must be a nonempty immutable tuple"
            raise ValueError(message)
        if any(type(call) is not CallSubmission for call in self.calls):
            message = "calls must contain exact CallSubmission values"
            raise TypeError(message)
        if self.cohort_timestamp is None and len(self.calls) != 1:
            message = "a missing-timestamp cohort must be a singleton"
            raise ValueError(message)
        if not callable(self.admission_hook):
            message = "admission_hook must be callable"
            raise TypeError(message)

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


def _require_utc_timestamp(value: object, name: str) -> None:
    if value is None:
        return
    if not isinstance(value, datetime.datetime):
        message = f"{name} must be a datetime or None"
        raise TypeError(message)
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
        if not isinstance(
            self.member,
            ingestion_lease_store.LeaseMemberIdentity,
        ):
            message = "member must be a LeaseMemberIdentity"
            raise TypeError(message)
        if not isinstance(self.target, datetime.datetime):
            message = "target must be a datetime"
            raise TypeError(message)
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

    def __post_init__(self) -> None:
        if type(self.boundary) is not BoundaryWork:
            message = "boundary must be an exact BoundaryWork"
            raise TypeError(message)
        if not isinstance(self.disposition, BoundaryDisposition):
            message = "disposition must be a BoundaryDisposition"
            raise TypeError(message)


@dataclasses.dataclass(frozen=True, slots=True)
class BoundaryBatchCommitted:
    """Caller-ordered settled results beneath an accepted exact grant."""

    results: tuple[BoundaryResult, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.results, tuple):
            message = "results must be an immutable tuple"
            raise TypeError(message)


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
        if not isinstance(self.feed_id, uuid.UUID):
            message = "feed_id must be a UUID"
            raise TypeError(message)
        if not isinstance(self.grant, ingestion_lease_store.LeaseGrant):
            message = "grant must be a LeaseGrant"
            raise TypeError(message)
        if not isinstance(
            self.member,
            ingestion_lease_store.LeaseMemberIdentity,
        ):
            message = "member must be a LeaseMemberIdentity"
            raise TypeError(message)
        if self.member.feed_id != self.feed_id:
            message = "member Feed does not match work Feed"
            raise ValueError(message)
        _require_nonnegative_integer(self.source_order, "source_order")
        _require_utc_timestamp(self.cohort_timestamp, "cohort_timestamp")
        _require_nonnegative_integer(self.page_sequence, "page_sequence")
        if self.settlement_observer is not None and not callable(
            self.settlement_observer
        ):
            message = "settlement_observer must be callable or None"
            raise TypeError(message)


@dataclasses.dataclass(frozen=True, slots=True)
class _CallRecord:
    """One distinct counted call with its scheduler-local identity."""

    work: _CallWork
    identity: CohortRecordIdentity

    def __post_init__(self) -> None:
        if not isinstance(self.work, _CallWork):
            message = "work must be _CallWork"
            raise TypeError(message)
        if type(self.identity) is not CohortRecordIdentity:
            message = "identity must be an exact CohortRecordIdentity"
            raise TypeError(message)
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
        if not isinstance(self.records, tuple) or not self.records:
            message = "records must be a nonempty immutable tuple"
            raise ValueError(message)
        if any(type(record) is not _CallRecord for record in self.records):
            message = "records must contain exact _CallRecord values"
            raise TypeError(message)
        first = self.records[0]
        identities = tuple(record.identity for record in self.records)
        if self.control.identities != identities:
            message = "cohort control identities do not match records"
            raise ValueError(message)
        if type(self.signals) is not LaneSignalView:
            message = "signals must be an exact LaneSignalView"
            raise TypeError(message)
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
        if type(self.boundary) is not BoundaryWork:
            message = "boundary must be an exact BoundaryWork"
            raise TypeError(message)
        if not isinstance(self.grant, ingestion_lease_store.LeaseGrant):
            message = "grant must be a LeaseGrant"
            raise TypeError(message)
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


def _normalize_failure_detail(value: object) -> str:
    if not isinstance(value, str):
        message = "failure detail must be a string"
        raise TypeError(message)
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
    return status_reason_detail.cap_status_reason_detail_for_storage(
        normalized
    )


@dataclasses.dataclass(frozen=True, slots=True)
class CohortItemFailureFact:
    """Bounded canonical evidence for one terminal item skip."""

    status_reason: feed_store.FeedStatusReason
    detail: str

    def __post_init__(self) -> None:
        if not isinstance(self.status_reason, feed_store.FeedStatusReason):
            message = "status_reason must be a FeedStatusReason"
            raise TypeError(message)
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
        if not isinstance(self.status_reason, feed_store.FeedStatusReason):
            message = "status_reason must be a FeedStatusReason"
            raise TypeError(message)
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
        if type(self.identity) is not CohortRecordIdentity:
            message = "identity must be an exact CohortRecordIdentity"
            raise TypeError(message)
        if not isinstance(self.participated, bool):
            message = "participated must be a bool"
            raise TypeError(message)
        if not isinstance(self.closure_state, CohortRecordClosureState):
            message = "closure_state must be a CohortRecordClosureState"
            raise TypeError(message)
        if not isinstance(self.full_pipeline_completed, bool):
            message = "full_pipeline_completed must be a bool"
            raise TypeError(message)
        if not isinstance(self.terminal_reason, CohortRecordTerminalReason):
            message = "terminal_reason must be a CohortRecordTerminalReason"
            raise TypeError(message)
        if self.item_failure is not None and type(
            self.item_failure
        ) is not CohortItemFailureFact:
            message = "item_failure must be a CohortItemFailureFact or None"
            raise TypeError(message)
        if self.direct_failure is not None and type(
            self.direct_failure
        ) is not CohortDirectFailureFact:
            message = "direct_failure must be a CohortDirectFailureFact or None"
            raise TypeError(message)
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
            or self.closure_state
            is not CohortRecordClosureState.DURABLY_CLOSED
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
        if not isinstance(self.records, tuple) or not self.records:
            message = "records must be a nonempty immutable tuple"
            raise ValueError(message)
        if any(
            type(record) is not CohortRecordTerminalFact
            for record in self.records
        ):
            message = (
                "records must contain exact CohortRecordTerminalFact values"
            )
            raise TypeError(message)
        if not isinstance(self.disposition, CohortTerminalDisposition):
            message = "disposition must be a CohortTerminalDisposition"
            raise TypeError(message)
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


class OutcomeUnknownCause(enum.StrEnum):
    """Closed external operation whose result may have become durable."""

    COMMIT = "commit"
    PUBLICATION = "publication"


@dataclasses.dataclass(frozen=True, slots=True)
class OutcomeUnknownRetentionRequest:
    """One exact sticky retention request for an active cohort."""

    cause: OutcomeUnknownCause
    terminal_facts: CohortTerminalFacts

    def __post_init__(self) -> None:
        if not isinstance(self.cause, OutcomeUnknownCause):
            message = "cause must be an OutcomeUnknownCause"
            raise TypeError(message)
        if type(self.terminal_facts) is not CohortTerminalFacts:
            message = "terminal_facts must be exact CohortTerminalFacts"
            raise TypeError(message)
        if (
            self.terminal_facts.disposition
            is not CohortTerminalDisposition.OUTCOME_UNKNOWN
        ):
            message = "retention requires OUTCOME_UNKNOWN facts"
            raise CohortIntegrityError(message)


_CAPABILITY_SEAL = object()


@dataclasses.dataclass(frozen=True, slots=True)
class CohortRetentionHandle:
    """Scheduler-minted complete-identity outcome-unknown capability."""

    identities: tuple[CohortRecordIdentity, ...]
    _callback: typing.Callable[[OutcomeUnknownRetentionRequest], None]
    _seal: object = dataclasses.field(repr=False, compare=False)

    def __post_init__(self) -> None:
        if self._seal is not _CAPABILITY_SEAL:
            message = "retention handle was not scheduler-minted"
            raise CohortIntegrityError(message)

    def retain(self, request: OutcomeUnknownRetentionRequest) -> None:
        """Retain this exact active cohort for one unknown result."""
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
        if not isinstance(self.calls, tuple) or not self.calls:
            message = "calls must be a nonempty immutable tuple"
            raise ValueError(message)
        if any(type(call) is not CallExecution for call in self.calls):
            message = "calls must contain exact CallExecution values"
            raise TypeError(message)
        if type(self.signals) is not LaneSignalView:
            message = "signals must be an exact LaneSignalView"
            raise TypeError(message)
        identities = tuple(call.identity for call in self.calls)
        if identities != self.retention.identities or identities != (
            self.cancellation_handoff.identities
        ):
            message = "cohort execution capabilities crossed identity"
            raise CohortIntegrityError(message)


def _require_outcome_facts(
    facts: object,
    disposition: CohortTerminalDisposition,
) -> CohortTerminalFacts:
    if type(facts) is not CohortTerminalFacts:
        message = "facts must be exact CohortTerminalFacts"
        raise TypeError(message)
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
            record.closure_state
            is not CohortRecordClosureState.DURABLY_CLOSED
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
        record.closure_state
        is not CohortRecordClosureState.REPLAY_SAFE_RELEASE
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

    def __post_init__(self) -> None:
        if not isinstance(self.grant, ingestion_lease_store.LeaseGrant):
            message = "grant must be a LeaseGrant"
            raise TypeError(message)
        if not isinstance(self.reason, LaneCloseReason):
            message = "reason must be a LaneCloseReason"
            raise TypeError(message)


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

    def __post_init__(self) -> None:
        if self.grant is not None and not isinstance(
            self.grant,
            ingestion_lease_store.LeaseGrant,
        ):
            message = "grant must be a LeaseGrant or None"
            raise TypeError(message)
        if not isinstance(self.reason, LaneCloseReason):
            message = "reason must be a LaneCloseReason"
            raise TypeError(message)


@dataclasses.dataclass(frozen=True, slots=True)
class FeedRemoved:
    """Evidence-neutral result for one exact lane-local Feed removal.

    Attributes:
        grant: Complete immutable Lease generation scoped by the removal.
        feed_id: Feed retired only from that exact grant.
        released_count: Queued calls and pending boundaries safely released.
        active_retained: Whether admitted active work remains to settle.
    """

    grant: ingestion_lease_store.LeaseGrant
    feed_id: uuid.UUID
    released_count: int
    active_retained: bool

    def __post_init__(self) -> None:
        if not isinstance(self.grant, ingestion_lease_store.LeaseGrant):
            message = "grant must be a LeaseGrant"
            raise TypeError(message)
        if not isinstance(self.feed_id, uuid.UUID):
            message = "feed_id must be a UUID"
            raise TypeError(message)
        _require_nonnegative_integer(self.released_count, "released_count")
        if not isinstance(self.active_retained, bool):
            message = "active_retained must be a bool"
            raise TypeError(message)


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
    retired_scopes: frozenset[
        tuple[ingestion_lease_store.LeaseGrant, uuid.UUID]
    ]
    admission_open: bool
    fatal: bool


@dataclasses.dataclass(frozen=True, slots=True)
class _PurgeResult:
    """Exact bounded-scan result without retaining completed outcomes."""

    released_sequences: tuple[int, ...]
    active_sequences: tuple[int, ...]


@dataclasses.dataclass(frozen=True, slots=True)
class _RetireFeedResult:
    """Localized Feed retirement result for later lane coordination."""

    released_sequences: tuple[int, ...]
    released_call_records: tuple[_CallRecord, ...]
    released_calls: tuple[tuple[int, int], ...]
    released_boundaries: tuple[tuple[int, int], ...]
    active_sequence: int | None
