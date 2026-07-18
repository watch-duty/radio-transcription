"""Closed immutable vocabulary for bounded Feed-affine scheduling."""

from __future__ import annotations

import dataclasses
import typing

if typing.TYPE_CHECKING:
    import datetime
    import uuid

    from backend.pipeline.storage import ingestion_lease_store

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


@dataclasses.dataclass(frozen=True, slots=True)
class _CallWork:
    """One call submission before local shard registration."""

    feed_id: uuid.UUID
    grant: ingestion_lease_store.LeaseGrant
    cohort_timestamp: datetime.datetime | None
    payload: object
    page_sequence: int

    def __post_init__(self) -> None:
        _require_nonnegative_integer(self.page_sequence, "page_sequence")


@dataclasses.dataclass(frozen=True, slots=True)
class _CallRecord:
    """One distinct counted call with its scheduler-local identity."""

    work: _CallWork
    local_sequence: int

    def __post_init__(self) -> None:
        _require_nonnegative_integer(self.local_sequence, "local_sequence")

    @property
    def feed_id(self) -> uuid.UUID:
        return self.work.feed_id

    @property
    def grant(self) -> ingestion_lease_store.LeaseGrant:
        return self.work.grant


class CallExecutor(typing.Protocol):
    """Narrow injected seam awaited only by an unlocked fixed worker."""

    async def execute(self, record: _CallRecord) -> None:
        """Settle one already-registered call or raise on integrity failure."""
        ...
