"""Closed generic vocabulary for supervising typed ingestion grants."""

from __future__ import annotations

# Runtime imports keep typing.get_type_hints() resolvable.
import asyncio  # noqa: TC003
import dataclasses
import enum
import typing

from backend.pipeline.ingestion import failure_policy
from backend.pipeline.storage import feed_store  # noqa: TC001

if typing.TYPE_CHECKING:
    import uuid


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
        reason: Optional diagnostic detail. The storage seam applies its
            durable length cap.
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
        """Claim one closed admission mode up to ``limit`` grants.

        Args:
            mode: Primary or recovery admission mode.
            owner_worker_id: Worker that will own returned grants.
            limit: Maximum number of grants to claim.

        Returns:
            Caller-ordered claims with their domain-specific runner payloads.

        Raises:
            GrantControlIntegrityError: A store response violates the control
                contract.
        """
        ...

    async def heartbeat(
        self,
        grants: typing.Sequence[GrantT],
    ) -> tuple[GrantHeartbeat[GrantT], ...]:
        """Heartbeat complete grants with exact caller correlation.

        Args:
            grants: Complete grants in caller correlation order.

        Returns:
            One caller-ordered disposition for every submitted grant.

        Raises:
            GrantControlIntegrityError: A store response cannot be correlated
                exactly.
        """
        ...

    async def finalize(
        self,
        grant: GrantT,
        payload: PayloadT,
        terminal: TerminalDecision,
    ) -> FinalizeResult[GrantT]:
        """Execute one selected action with its exact validated payload.

        Args:
            grant: Exact ownership generation to finalize.
            payload: Domain-specific payload returned with the claim.
            terminal: Selected neutral or failure persistence decision.

        Returns:
            Exact-grant finalization disposition.

        Raises:
            GrantControlIntegrityError: The payload or store response violates
                the control contract.
        """
        ...


class GrantRunner[GrantT, PayloadT](typing.Protocol):
    """Typed work runner that owns no claim, heartbeat, or finalization."""

    async def run(
        self,
        grant: GrantT,
        payload: PayloadT,
        context: RunContext,
    ) -> RunOutcome:
        """Run work for one exact grant until a closed outcome.

        Args:
            grant: Exact authority held while the runner is active.
            payload: Domain-specific claim payload.
            context: Supervisor-owned stop and authority-loss signals.

        Returns:
            One closed completion, loss, or failure outcome.
        """
        ...
