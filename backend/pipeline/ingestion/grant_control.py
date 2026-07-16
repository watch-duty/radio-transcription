"""Closed generic vocabulary for supervising typed ingestion grants."""

from __future__ import annotations

import dataclasses
import enum
import typing

from backend.pipeline.ingestion import failure_policy

if typing.TYPE_CHECKING:
    import asyncio
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
    INELIGIBLE = "ineligible"
    LOST = "lost"


class FinalizeDisposition(enum.StrEnum):
    """Common terminal storage outcomes."""

    APPLIED = "applied"
    LOST = "lost"


class GrantControlIntegrityError(RuntimeError):
    """Raised when a typed store response cannot be correlated exactly."""


@dataclasses.dataclass(frozen=True, slots=True)
class ClaimedGrant[GrantT, PayloadT]:
    """One newly claimed typed grant and its separate runner payload.

    Attributes:
        grant: Complete immutable authority for the ownership generation.
        payload: Domain-specific data passed unchanged to the runner.
    """

    grant: GrantT
    payload: PayloadT


@dataclasses.dataclass(frozen=True, slots=True)
class GrantHeartbeat[GrantT]:
    """One caller-correlated common heartbeat result.

    Attributes:
        grant: Original grant associated with the storage result.
        disposition: Domain-neutral meaning of the heartbeat result.
    """

    grant: GrantT
    disposition: HeartbeatDisposition


@dataclasses.dataclass(frozen=True, slots=True)
class FinalizeResult[GrantT]:
    """One exact grant finalization result.

    Attributes:
        grant: Exact grant submitted for finalization.
        disposition: Domain-neutral meaning of the storage result.
    """

    grant: GrantT
    disposition: FinalizeDisposition


@dataclasses.dataclass(frozen=True, slots=True)
class NeutralRelease:
    """Selected neutral exact-grant release."""


type TerminalDecision = NeutralRelease | failure_policy.FailurePersistencePlan


@dataclasses.dataclass(frozen=True, slots=True)
class RunCompleted:
    """Runner completed normally."""


@dataclasses.dataclass(frozen=True, slots=True)
class RunLost:
    """Runner observed confirmed exact-grant loss."""


@dataclasses.dataclass(frozen=True, slots=True)
class RunFailed:
    """Runner exhausted local work with canonical failure evidence.

    Attributes:
        status_reason: Canonical failure classification.
        reason: Optional bounded operator-facing detail.
    """

    status_reason: feed_store.FeedStatusReason
    reason: str | None


type RunOutcome = RunCompleted | RunLost | RunFailed


@dataclasses.dataclass(frozen=True, slots=True)
class RunContext:
    """Supervisor-owned stop and authority-loss signals for one runner.

    Attributes:
        stop_requested: Set when the runner should stop cooperatively.
        grant_lost: Set when exact durable authority has been lost.
    """

    stop_requested: asyncio.Event
    grant_lost: asyncio.Event


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
