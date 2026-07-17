"""Public contracts for durable fenced ingestion Leases."""

from __future__ import annotations

import dataclasses
import datetime
import enum
import uuid

from backend.pipeline.storage import feed_lifecycle, feed_store

__all__ = (
    "AdmittedAudioProgress",
    "BatchCommitted",
    "BudgetedFailure",
    "ChildDisposition",
    "ChildMutation",
    "ChildMutationBatch",
    "ChildMutationResult",
    "ClosedCohortProgress",
    "FeedFailureTransition",
    "FinalizeLeaseRecovery",
    "GrantRejected",
    "GrantRejectionReason",
    "LeaseClaim",
    "LeaseEffect",
    "LeaseFailureAction",
    "LeaseFailureResult",
    "LeaseGrant",
    "LeaseHeartbeatResult",
    "LeaseMember",
    "LeaseMemberIdentity",
    "LeaseOperationDisposition",
    "LeaseOperationResult",
    "LeaseReleaseCause",
    "MembershipInvariantViolation",
    "MembershipRefreshResult",
    "MembershipSnapshot",
    "MembershipUnchanged",
    "NoLeaseEffect",
    "NonBudgetedFailure",
    "SourceObservation",
)


@dataclasses.dataclass(frozen=True, slots=True)
class LeaseGrant:
    """Complete immutable authority for one Lease ownership generation.

    Attributes:
        source_type: Ingestion source family that owns the Lease namespace.
        lease_key: Permanent source-local Lease identity, such as a SID.
        owner_worker_id: Worker authorized for this ownership generation.
        fencing_token: Monotonic generation that rejects zombie workers.
    """

    source_type: feed_store.SourceType
    lease_key: str
    owner_worker_id: uuid.UUID
    fencing_token: int

    @property
    def unit_key(self) -> tuple[feed_store.SourceType, str]:
        """Return the permanent identity within the Lease domain."""
        return (self.source_type, self.lease_key)

    def __post_init__(self) -> None:
        if not isinstance(self.source_type, feed_store.SourceType):
            msg = "source_type must be a SourceType"
            raise TypeError(msg)
        if not isinstance(self.lease_key, str):
            msg = "lease_key must be a string"
            raise TypeError(msg)
        if not self.lease_key.strip():
            msg = "lease_key must not be empty"
            raise ValueError(msg)
        if not isinstance(self.owner_worker_id, uuid.UUID):
            msg = "owner_worker_id must be a UUID"
            raise TypeError(msg)
        if isinstance(self.fencing_token, bool) or not isinstance(
            self.fencing_token,
            int,
        ):
            msg = "fencing_token must be an integer"
            raise TypeError(msg)
        if self.fencing_token < 0:
            msg = "fencing_token must be nonnegative"
            raise ValueError(msg)


@dataclasses.dataclass(frozen=True, slots=True)
class LeaseClaim:
    """A newly established complete Lease grant.

    Attributes:
        grant: Complete immutable authority established by the claim.
    """

    grant: LeaseGrant


class LeaseOperationDisposition(enum.StrEnum):
    """Closed outcome vocabulary for exact-grant control operations.

    Attributes:
        APPLIED: The requested durable mutation committed.
        MISSING: The permanent Lease identity does not exist.
        OWNER_MISMATCH: Another worker owns the current generation.
        FENCE_MISMATCH: The supplied ownership generation is stale.
        STATUS_INELIGIBLE: The Lease is not active for this operation.
    """

    APPLIED = "applied"
    MISSING = "missing"
    OWNER_MISMATCH = "owner_mismatch"
    FENCE_MISMATCH = "fence_mismatch"
    STATUS_INELIGIBLE = "status_ineligible"


@dataclasses.dataclass(frozen=True, slots=True)
class LeaseOperationResult:
    """Narrow result for one exact-grant Lease mutation.

    Attributes:
        disposition: Closed classification of the mutation attempt.
    """

    disposition: LeaseOperationDisposition


@dataclasses.dataclass(frozen=True, slots=True)
class LeaseHeartbeatResult:
    """Caller-correlated diagnostic result for heartbeat renewal.

    Attributes:
        grant: Original caller grant associated with this result.
        disposition: Closed classification of the heartbeat attempt.
    """

    grant: LeaseGrant
    disposition: LeaseOperationDisposition


class LeaseReleaseCause(enum.StrEnum):
    """Structured telemetry causes sharing one neutral release policy.

    Attributes:
        NORMAL: Ordinary completed ownership interval.
        SHUTDOWN: Worker shutdown released the Lease.
        REBALANCE: Whole-unit rebalancing released the Lease.
        CANCELLATION: Local task cancellation released the Lease.
        ABANDONMENT: Explicit local abandonment released the Lease.
    """

    NORMAL = "normal"
    SHUTDOWN = "shutdown"
    REBALANCE = "rebalance"
    CANCELLATION = "cancellation"
    ABANDONMENT = "abandonment"


class GrantRejectionReason(enum.StrEnum):
    """Closed reasons an exact active Lease grant can be rejected.

    Attributes:
        MISSING: The permanent Lease identity does not exist.
        OWNER_MISMATCH: Another worker owns the current generation.
        FENCE_MISMATCH: The supplied ownership generation is stale.
        STATUS_INELIGIBLE: The Lease is not active for this operation.
    """

    MISSING = "missing"
    OWNER_MISMATCH = "owner_mismatch"
    FENCE_MISMATCH = "fence_mismatch"
    STATUS_INELIGIBLE = "status_ineligible"


@dataclasses.dataclass(frozen=True, slots=True)
class GrantRejected:
    """Shared exact-grant rejection classification.

    Attributes:
        reason: Exact reason the complete grant was rejected.
    """

    reason: GrantRejectionReason


@dataclasses.dataclass(frozen=True, slots=True)
class BudgetedFailure:
    """Failure action that consumes the retained Lease failure budget.

    Attributes:
        failure_threshold: Retained count that transitions to quarantine.
        backoff_base_sec: Delay before exponential growth and jitter.
        backoff_max_sec: Upper bound on the exponential delay before jitter.
    """

    failure_threshold: int = feed_lifecycle.DEFAULT_FAILURE_THRESHOLD
    backoff_base_sec: int = feed_lifecycle.DEFAULT_BACKOFF_BASE_SEC
    backoff_max_sec: int = feed_lifecycle.DEFAULT_BACKOFF_MAX_SEC

    def __post_init__(self) -> None:
        for name, value in (
            ("failure_threshold", self.failure_threshold),
            ("backoff_base_sec", self.backoff_base_sec),
            ("backoff_max_sec", self.backoff_max_sec),
        ):
            if isinstance(value, bool):
                msg = f"{name} must be an integer"
                raise TypeError(msg)
            if value <= 0:
                msg = f"{name} must be positive"
                raise ValueError(msg)
        if self.backoff_base_sec > self.backoff_max_sec:
            msg = "backoff_base_sec must not exceed backoff_max_sec"
            raise ValueError(msg)


@dataclasses.dataclass(frozen=True, slots=True)
class NonBudgetedFailure:
    """Retryable failure action that cannot quarantine the Lease.

    Attributes:
        retry_after: Caller-selected UTC time when recovery becomes eligible.
    """

    retry_after: datetime.datetime

    def __post_init__(self) -> None:
        if self.retry_after.utcoffset() != datetime.timedelta(0):
            msg = "retry_after must be UTC-aware"
            raise ValueError(msg)


type LeaseFailureAction = BudgetedFailure | NonBudgetedFailure


def _status_reason_detail_storage_value(value: str | None) -> str | None:
    """Normalize an operator-facing lifecycle detail."""
    return feed_lifecycle.status_reason_detail_storage_value(value)


@dataclasses.dataclass(frozen=True, slots=True)
class LeaseFailureResult:
    """Narrow outcome of one exact-grant failure finalization.

    Attributes:
        disposition: Closed classification of the mutation attempt.
        final_status: Failing or quarantined after an applied mutation; absent
            when the exact grant was rejected.
    """

    disposition: LeaseOperationDisposition
    final_status: feed_store.FeedStatus | None

    def __post_init__(self) -> None:
        applied = self.disposition is LeaseOperationDisposition.APPLIED
        if applied:
            valid_final_status = self.final_status in (
                feed_store.FeedStatus.FAILING,
                feed_store.FeedStatus.QUARANTINED,
            )
            if valid_final_status:
                return
        elif self.final_status is None:
            return

        msg = "only an applied failure may return failing or quarantined status"
        raise ValueError(msg)


@dataclasses.dataclass(frozen=True, slots=True)
class LeaseMemberIdentity:
    """Immutable Feed/source/SID/group binding from membership loading.

    Attributes:
        feed_id: Permanent Feed UUID.
        source_type: Source family shared by the Feed and property row.
        source_feed_id: Canonical provider routing identity.
        sid: Textual Broadcastify system identity, including leading zeroes.
        group_id: Textual talkgroup identity, including leading zeroes.
    """

    feed_id: uuid.UUID
    source_type: feed_store.SourceType
    source_feed_id: str
    sid: str
    group_id: str


@dataclasses.dataclass(frozen=True, slots=True)
class AdmittedAudioProgress:
    """Successful progress for work admitted under the current grant.

    Attributes:
        member: Immutable child identity from the membership snapshot.
        last_processed_filename: Source path accepted as durable progress.
        cursor: Optional monotonic Feed cursor proposed by the caller.
    """

    member: LeaseMemberIdentity
    last_processed_filename: str
    cursor: datetime.datetime | None


@dataclasses.dataclass(frozen=True, slots=True)
class SourceObservation:
    """Successful source boundary observation for one immutable member.

    Attributes:
        member: Immutable child identity from the membership snapshot.
        cursor: Optional monotonic quiet-boundary cursor.
    """

    member: LeaseMemberIdentity
    cursor: datetime.datetime | None


@dataclasses.dataclass(frozen=True, slots=True)
class ClosedCohortProgress:
    """Lifecycle-neutral durability for one completed cohort.

    Attributes:
        member: Immutable child identity from the membership snapshot.
        last_processed_filename: Optional final path for the closed cohort.
        cursor: Optional monotonic cursor for the closed cohort.
    """

    member: LeaseMemberIdentity
    last_processed_filename: str | None
    cursor: datetime.datetime | None


@dataclasses.dataclass(frozen=True, slots=True)
class FeedFailureTransition:
    """One-shot caller-classified failure at an optional source boundary.

    Callers must not retry after an outcome-unknown transaction attempt.

    Attributes:
        member: Immutable child identity from the membership snapshot.
        action: Budgeted or non-budgeted durable failure policy.
        status_reason: Canonical abnormal lifecycle reason.
        reason: Optional operator-facing diagnostic detail.
        completion_cursor: Optional completed boundary cursor.
    """

    member: LeaseMemberIdentity
    action: LeaseFailureAction
    status_reason: feed_store.FeedStatusReason
    reason: str | None
    completion_cursor: datetime.datetime | None


type ChildMutation = (
    AdmittedAudioProgress
    | SourceObservation
    | ClosedCohortProgress
    | FeedFailureTransition
)


@dataclasses.dataclass(frozen=True, slots=True)
class NoLeaseEffect:
    """Explicit request to retain all current Lease lifecycle evidence."""


@dataclasses.dataclass(frozen=True, slots=True)
class FinalizeLeaseRecovery:
    """Clear retained Lease failure evidence after proven success."""


type LeaseEffect = NoLeaseEffect | FinalizeLeaseRecovery


@dataclasses.dataclass(frozen=True, slots=True)
class ChildMutationBatch:
    """Closed immutable child commands and their parent Lease effect.

    Attributes:
        mutations: Globally de-duplicated caller-ordered child commands.
        lease_effect: Requested lifecycle effect for the parent Lease.
    """

    mutations: tuple[ChildMutation, ...]
    lease_effect: LeaseEffect


class ChildDisposition(enum.StrEnum):
    """Actionable disposition of one requested child mutation.

    Attributes:
        COMMITTED: The requested durable postcondition is satisfied without
            newly quarantining the Feed.
        COMMITTED_AND_QUARANTINED: The requested durable postcondition is
            satisfied and the Feed crossed its quarantine threshold.
        REJECTED: The child is missing or no longer eligible for the command.
    """

    COMMITTED = "committed"
    COMMITTED_AND_QUARANTINED = "committed_and_quarantined"
    REJECTED = "rejected"


@dataclasses.dataclass(frozen=True, slots=True)
class ChildMutationResult:
    """Caller-correlated selective result for one Feed command.

    Attributes:
        feed_id: Permanent Feed UUID from the caller command.
        disposition: Whether the requested postcondition was committed.
    """

    feed_id: uuid.UUID
    disposition: ChildDisposition


@dataclasses.dataclass(frozen=True, slots=True)
class BatchCommitted:
    """Committed parent effect and actionable child results.

    Attributes:
        children: One selective result per command in caller order.
    """

    children: tuple[ChildMutationResult, ...]


@dataclasses.dataclass(frozen=True, slots=True)
class LeaseMember:
    """Eligible child Feed state attached to an immutable identity.

    Attributes:
        identity: Immutable routing identity loaded with this state.
        name: Canonical Feed display name frozen for publication.
        last_bookmark_time: Durable Feed progress cursor.
    """

    identity: LeaseMemberIdentity
    name: str
    last_bookmark_time: datetime.datetime | None


@dataclasses.dataclass(frozen=True, slots=True)
class MembershipSnapshot:
    """Authoritative eligible membership for one exact active grant.

    Attributes:
        grant: Complete Lease authority validated before the child read.
        membership_revision: Cache-invalidating parent revision.
        members: Eligible active or failing children in routing order.
    """

    grant: LeaseGrant
    membership_revision: int
    members: tuple[LeaseMember, ...]


@dataclasses.dataclass(frozen=True, slots=True)
class MembershipUnchanged:
    """Proof that one exact grant still has the caller's known revision.

    Attributes:
        grant: Complete Lease authority validated beneath the row lock.
        membership_revision: Revision equal to the caller's known value.
    """

    grant: LeaseGrant
    membership_revision: int


@dataclasses.dataclass(frozen=True, slots=True)
class MembershipInvariantViolation:
    """Fail-closed result for deterministically invalid Lease membership.

    Attributes:
        grant: Complete Lease authority associated with the failed load.
    """

    grant: LeaseGrant


type MembershipRefreshResult = (
    MembershipSnapshot
    | MembershipUnchanged
    | GrantRejected
    | MembershipInvariantViolation
)
