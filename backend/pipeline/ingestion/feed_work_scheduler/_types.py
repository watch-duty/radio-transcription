"""Closed immutable vocabulary for bounded Feed-affine scheduling."""

from __future__ import annotations

import dataclasses
import datetime
import enum
import typing
import uuid

from backend.pipeline.storage import ingestion_lease_store

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


@dataclasses.dataclass(frozen=True, slots=True)
class _CallWork:
    """One source-order call submission before local shard registration."""

    feed_id: uuid.UUID
    grant: ingestion_lease_store.LeaseGrant
    source_order: int
    source_timestamp: datetime.datetime
    payload: object
    page_sequence: int

    def __post_init__(self) -> None:
        if not isinstance(self.feed_id, uuid.UUID):
            message = "feed_id must be a UUID"
            raise TypeError(message)
        if not isinstance(self.grant, ingestion_lease_store.LeaseGrant):
            message = "grant must be a LeaseGrant"
            raise TypeError(message)
        _require_nonnegative_integer(self.source_order, "source_order")
        if not isinstance(self.source_timestamp, datetime.datetime):
            message = "source_timestamp must be a datetime"
            raise TypeError(message)
        _require_nonnegative_integer(self.page_sequence, "page_sequence")


@dataclasses.dataclass(frozen=True, slots=True)
class _CallRecord:
    """One distinct counted call with its scheduler-local identity."""

    work: _CallWork
    local_sequence: int

    def __post_init__(self) -> None:
        if not isinstance(self.work, _CallWork):
            message = "work must be _CallWork"
            raise TypeError(message)
        _require_nonnegative_integer(self.local_sequence, "local_sequence")

    @property
    def feed_id(self) -> uuid.UUID:
        return self.work.feed_id

    @property
    def grant(self) -> ingestion_lease_store.LeaseGrant:
        return self.work.grant


@dataclasses.dataclass(frozen=True, slots=True)
class _ExecutorCompleted:
    """The call pipeline settled in a scheduling-terminal state."""


@dataclasses.dataclass(frozen=True, slots=True)
class _ExecutorRetryable:
    """The adapter settled with retryable evidence for an outer policy."""


@dataclasses.dataclass(frozen=True, slots=True)
class _ExecutorAuthorityLost:
    """The adapter confirmed loss of the record's exact Lease authority."""


@dataclasses.dataclass(frozen=True, slots=True)
class _ExecutorMembershipRejected:
    """The adapter rejected only the record's Feed membership."""


@dataclasses.dataclass(frozen=True, slots=True)
class _ExecutorIntegrityFailure:
    """The adapter settled with scheduler-integrity failure evidence."""

    failure: BaseException

    def __post_init__(self) -> None:
        if not isinstance(self.failure, BaseException):
            message = "failure must be a BaseException"
            raise TypeError(message)


type _ExecutorOutcome = (
    _ExecutorCompleted
    | _ExecutorRetryable
    | _ExecutorAuthorityLost
    | _ExecutorMembershipRejected
    | _ExecutorIntegrityFailure
)


class CallExecutor(typing.Protocol):
    """Narrow injected seam awaited only by an unlocked fixed worker."""

    async def execute(self, record: _CallRecord) -> _ExecutorOutcome:
        """Settle one already-registered call through a closed outcome."""
        ...


class _RecordState(enum.StrEnum):
    """Counted locations in the shard conservation equation."""

    QUEUED = "queued"
    ACTIVE = "active"
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
    workers: tuple[_WorkerSnapshot, ...]
    retired_feeds: frozenset[uuid.UUID]
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
    active_sequence: int | None
