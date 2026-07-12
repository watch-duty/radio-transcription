"""Closed generic vocabulary for supervising typed ingestion grants."""

from __future__ import annotations

import dataclasses
import enum
import typing

if typing.TYPE_CHECKING:
    import asyncio
    import datetime
    import uuid

    from backend.pipeline.storage import feed_store


class DomainId(enum.StrEnum):
    """Static durable-authority domains understood by the runtime."""

    FEED = "feed"
    SID = "sid"


class ClaimMode(enum.StrEnum):
    """Closed primary-then-recovery admission modes."""

    PRIMARY = "primary"
    RECOVERY = "recovery"


class HeartbeatDisposition(enum.StrEnum):
    """Common exact-grant heartbeat meanings."""

    RETAINED = "retained"
    ADMINISTRATIVE_STOP = "administrative_stop"
    LOST = "lost"
    UNAVAILABLE = "unavailable"


class FinalizeDisposition(enum.StrEnum):
    """Common terminal storage outcomes."""

    APPLIED = "applied"
    ACCEPTED_NOOP = "accepted_noop"
    LOST = "lost"
    UNAVAILABLE = "unavailable"


class TerminalCause(enum.StrEnum):
    """Neutral reasons an exact grant runner stopped."""

    NORMAL = "normal"
    SHUTDOWN = "shutdown"
    PLANNED_DRAIN = "planned_drain"
    CANCELLATION = "cancellation"


class GrantControlIntegrityError(RuntimeError):
    """Raised when a typed store response cannot be correlated exactly."""


@dataclasses.dataclass(frozen=True, slots=True)
class LifecycleEvidence:
    """Only common projection of mutable durable lifecycle state."""

    durable_failing: bool

    def __post_init__(self) -> None:
        if not isinstance(self.durable_failing, bool):
            msg = "durable_failing must be a bool"
            raise TypeError(msg)


@dataclasses.dataclass(frozen=True, slots=True)
class ClaimedGrant[GrantT, PayloadT]:
    """One newly claimed typed grant and its separate runner payload."""

    grant: GrantT
    payload: PayloadT
    lifecycle: LifecycleEvidence


@dataclasses.dataclass(frozen=True, slots=True)
class GrantHeartbeat[GrantT]:
    """One caller-correlated common heartbeat result."""

    grant: GrantT
    disposition: HeartbeatDisposition
    lifecycle: LifecycleEvidence | None


@dataclasses.dataclass(frozen=True, slots=True)
class FinalizeResult[GrantT]:
    """One exact grant finalization result."""

    grant: GrantT
    disposition: FinalizeDisposition
    lifecycle: LifecycleEvidence | None


@dataclasses.dataclass(frozen=True, slots=True)
class NeutralRelease:
    """Selected neutral exact-grant release."""

    cause: TerminalCause

    def __post_init__(self) -> None:
        if not isinstance(self.cause, TerminalCause):
            msg = "cause must be a TerminalCause"
            raise TypeError(msg)


@dataclasses.dataclass(frozen=True, slots=True)
class BudgetedFailureDecision:
    """Already-selected failure action that consumes a durable budget."""

    failure_threshold: int
    backoff_base_sec: int
    backoff_max_sec: int
    status_reason: feed_store.FeedStatusReason
    actor_id: str
    reason: str | None


@dataclasses.dataclass(frozen=True, slots=True)
class NonBudgetedFailureDecision:
    """Already-selected retryable failure outside the durable budget."""

    retry_after: datetime.datetime
    status_reason: feed_store.FeedStatusReason
    actor_id: str
    reason: str | None


type TerminalDecision = (
    NeutralRelease | BudgetedFailureDecision | NonBudgetedFailureDecision
)


@dataclasses.dataclass(frozen=True, slots=True)
class RunCompleted:
    """Runner completed normally."""


@dataclasses.dataclass(frozen=True, slots=True)
class RunStopped:
    """Runner stopped for one neutral terminal cause."""

    cause: TerminalCause


@dataclasses.dataclass(frozen=True, slots=True)
class RunLost:
    """Runner observed confirmed exact-grant loss."""


@dataclasses.dataclass(frozen=True, slots=True)
class RunFailed:
    """Runner exhausted local work with canonical failure evidence."""

    status_reason: feed_store.FeedStatusReason
    reason: str | None


type RunOutcome = RunCompleted | RunStopped | RunLost | RunFailed


@dataclasses.dataclass(frozen=True, slots=True)
class RunContext:
    """Supervisor-owned signals and retry-state callback for one runner."""

    stop_requested: asyncio.Event
    grant_lost: asyncio.Event
    set_retrying: typing.Callable[[bool], None]


class GrantControl[GrantT, PayloadT](typing.Protocol):
    """Small typed storage-control seam shared by registered domains."""

    async def claim(
        self,
        mode: ClaimMode,
        owner_worker_id: uuid.UUID,
        limit: int,
    ) -> tuple[ClaimedGrant[GrantT, PayloadT], ...]:
        """Claim one closed admission mode up to ``limit`` grants."""
        ...

    async def heartbeat(
        self,
        grants: typing.Sequence[GrantT],
    ) -> tuple[GrantHeartbeat[GrantT], ...]:
        """Heartbeat complete grants with exact caller correlation."""
        ...

    async def finalize(
        self,
        grant: GrantT,
        payload: PayloadT,
        terminal: TerminalDecision,
    ) -> FinalizeResult[GrantT]:
        """Execute one selected action with its exact validated payload."""
        ...


class GrantRunner[GrantT, PayloadT](typing.Protocol):
    """Typed work runner that owns no claim, heartbeat, or finalization."""

    async def run(
        self,
        grant: GrantT,
        payload: PayloadT,
        context: RunContext,
    ) -> RunOutcome:
        """Run work for one exact grant until a closed outcome."""
        ...
