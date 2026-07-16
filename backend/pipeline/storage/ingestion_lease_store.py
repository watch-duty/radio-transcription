"""Typed storage boundary for durable fenced ingestion Leases."""

from __future__ import annotations

import dataclasses
import datetime
import enum
import json
import logging
import typing
import uuid

from backend.pipeline.storage import (
    feed_audit_sql,
    feed_change_notifications,
    feed_lifecycle,
    feed_store,
    ingestion_lease_queries,
)

if typing.TYPE_CHECKING:
    import collections.abc

    import asyncpg


logger = logging.getLogger(__name__)


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
    """A newly established complete Lease grant."""

    grant: LeaseGrant

    def __post_init__(self) -> None:
        _require_grant(self.grant)


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

    def __post_init__(self) -> None:
        if not isinstance(self.disposition, LeaseOperationDisposition):
            msg = "disposition must be a LeaseOperationDisposition"
            raise TypeError(msg)


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

    def __post_init__(self) -> None:
        if not isinstance(self.reason, GrantRejectionReason):
            msg = "reason must be a GrantRejectionReason"
            raise TypeError(msg)


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
            if isinstance(value, bool) or not isinstance(value, int):
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
        if not isinstance(self.retry_after, datetime.datetime):
            msg = "retry_after must be a datetime"
            raise TypeError(msg)
        if self.retry_after.utcoffset() != datetime.timedelta(0):
            msg = "retry_after must be UTC-aware"
            raise ValueError(msg)


type LeaseFailureAction = BudgetedFailure | NonBudgetedFailure


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
            valid_final_status = isinstance(
                self.final_status,
                feed_store.FeedStatus,
            ) and self.final_status in (
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


@dataclasses.dataclass(frozen=True, slots=True)
class _PlannedChildMutation:
    """Private, database-derived execution plan for one child command."""

    caller_ordinal: int
    mutation: ChildMutation
    before_row: collections.abc.Mapping | None
    disposition: ChildDisposition
    write_cursor: bool = False
    write_path: bool = False
    clear_lifecycle: bool = False
    audit_action: str | None = None

    @property
    def feed_id(self) -> uuid.UUID:
        """Return the command's immutable Feed UUID."""
        return self.mutation.member.feed_id

    @property
    def needs_update(self) -> bool:
        """Whether this command requires a write-hot Feed update."""
        return (
            self.write_cursor
            or self.write_path
            or self.clear_lifecycle
            or (
                isinstance(self.mutation, FeedFailureTransition)
                and self.disposition is not ChildDisposition.REJECTED
            )
        )


@dataclasses.dataclass(frozen=True, slots=True)
class _ChildAuditRowset:
    """Private parallel arrays for one rowset-safe Feed audit insert."""

    feed_ids: tuple[uuid.UUID, ...]
    actions: tuple[str, ...]
    actors: tuple[str, ...]
    revisions: tuple[int, ...]
    before_values: tuple[str, ...]
    after_values: tuple[str, ...]
    caller_ordinals: tuple[int, ...]


def _require_source_type(value: object) -> feed_store.SourceType:
    if not isinstance(value, feed_store.SourceType):
        msg = "source_type must be a SourceType"
        raise TypeError(msg)
    return value


def _require_owner_worker_id(value: object) -> uuid.UUID:
    if not isinstance(value, uuid.UUID):
        msg = "owner_worker_id must be a UUID"
        raise TypeError(msg)
    return value


def _require_limit(value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        msg = "limit must be an integer"
        raise TypeError(msg)
    if value < 0:
        msg = "limit must be nonnegative"
        raise ValueError(msg)
    return value


def _require_abandonment_after(value: object) -> datetime.timedelta:
    if not isinstance(value, datetime.timedelta):
        msg = "abandonment_after must be a timedelta"
        raise TypeError(msg)
    if value <= datetime.timedelta(0):
        msg = "abandonment_after must be positive"
        raise ValueError(msg)
    return value


def _require_grant(value: object) -> LeaseGrant:
    if not isinstance(value, LeaseGrant):
        msg = "grant must be a LeaseGrant"
        raise TypeError(msg)
    return value


def _require_failure_action(value: object) -> LeaseFailureAction:
    if type(value) not in (BudgetedFailure, NonBudgetedFailure):
        msg = "action must be BudgetedFailure or NonBudgetedFailure"
        raise TypeError(msg)
    return typing.cast("LeaseFailureAction", value)


def _require_status_reason(value: object) -> feed_store.FeedStatusReason:
    if not isinstance(value, feed_store.FeedStatusReason):
        msg = "status_reason must be a FeedStatusReason"
        raise TypeError(msg)
    return value


def _require_log_actor_id(value: object) -> str:
    """Validate an actor identity before adding it to structured log fields."""
    if not isinstance(value, str):
        msg = "actor_id must be a string"
        raise TypeError(msg)
    if not value or len(value) > 512 or any(char.isspace() for char in value):
        msg = (
            "actor_id must be nonempty, at most 512 chars, and whitespace-free"
        )
        raise ValueError(msg)
    return value


def _require_reason_detail(value: object) -> str | None:
    if value is not None and not isinstance(value, str):
        msg = "reason must be a string or None"
        raise TypeError(msg)
    return feed_lifecycle.status_reason_detail_storage_value(value)


def _require_known_membership_revision(value: object) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int):
        msg = "known_revision must be an integer or None"
        raise TypeError(msg)
    if value < 0:
        msg = "known_revision must be nonnegative"
        raise ValueError(msg)
    return value


def _require_actor_id(value: object) -> str:
    if not isinstance(value, str):
        msg = "actor_id must be a string"
        raise TypeError(msg)
    if not value or len(value) > 512 or any(char.isspace() for char in value):
        msg = (
            "actor_id must be nonempty, at most 512 chars, and whitespace-free"
        )
        raise ValueError(msg)
    return value


def _require_utc_cursor(
    value: object,
    *,
    field_name: str = "cursor",
) -> datetime.datetime | None:
    if value is None:
        return None
    if not isinstance(value, datetime.datetime):
        msg = f"{field_name} must be a datetime or None"
        raise TypeError(msg)
    if value.utcoffset() != datetime.timedelta(0):
        msg = f"{field_name} must be UTC-aware"
        raise ValueError(msg)
    return value


def _require_member_identity(
    grant: LeaseGrant,
    value: object,
) -> LeaseMemberIdentity:
    if not isinstance(value, LeaseMemberIdentity):
        msg = "child member must be a LeaseMemberIdentity"
        raise TypeError(msg)
    if not isinstance(value.feed_id, uuid.UUID):
        msg = "child member feed_id must be a UUID"
        raise TypeError(msg)
    if not isinstance(value.source_type, feed_store.SourceType):
        msg = "child member source_type must be a SourceType"
        raise TypeError(msg)
    for field_name, field_value in (
        ("sid", value.sid),
        ("group_id", value.group_id),
    ):
        if (
            not isinstance(field_value, str)
            or not field_value
            or not field_value.isascii()
            or not field_value.isdigit()
        ):
            msg = f"child member {field_name} must contain ASCII digits"
            raise ValueError(msg)
    if not isinstance(value.source_feed_id, str):
        msg = "child member source_feed_id must be a string"
        raise TypeError(msg)
    if value.source_type is not grant.source_type:
        msg = "child member source type does not match the Lease grant"
        raise ValueError(msg)
    if value.sid != grant.lease_key:
        msg = "child member SID does not match the Lease key"
        raise ValueError(msg)
    if value.source_feed_id != f"{value.sid}-{value.group_id}":
        msg = "child member source_feed_id does not match SID-group identity"
        raise ValueError(msg)
    return value


def _require_parallel_cardinality(
    rowset_name: str,
    *columns: collections.abc.Sequence[object],
) -> None:
    lengths = {len(column) for column in columns}
    if len(lengths) > 1:
        msg = f"{rowset_name} rowset columns have different cardinalities"
        raise ValueError(msg)


def _mutation_cursor(
    mutation: ChildMutation,
) -> datetime.datetime | None:
    """Return the cursor field shared semantically by child commands."""
    if isinstance(mutation, FeedFailureTransition):
        return mutation.completion_cursor
    return mutation.cursor


def _require_closed_cohort_progress(
    mutation: ClosedCohortProgress,
) -> None:
    """Validate lifecycle-neutral progress fields before pool checkout."""
    path = mutation.last_processed_filename
    if path is not None and not isinstance(path, str):
        msg = "closed cohort last_processed_filename must be a string or None"
        raise TypeError(msg)
    if isinstance(path, str) and not path.strip():
        msg = (
            "closed cohort last_processed_filename must be nonempty "
            "when present"
        )
        raise ValueError(msg)
    if path is None and mutation.cursor is None:
        msg = "closed cohort progress requires a path or cursor"
        raise ValueError(msg)


def _require_child_batch(
    grant: LeaseGrant,
    value: object,
) -> ChildMutationBatch:
    if type(value) is not ChildMutationBatch:
        msg = "batch must be a ChildMutationBatch"
        raise TypeError(msg)
    batch = value
    if not isinstance(batch.mutations, tuple):
        msg = "batch mutations must be an immutable tuple"
        raise TypeError(msg)
    if type(batch.lease_effect) not in (
        NoLeaseEffect,
        FinalizeLeaseRecovery,
    ):
        msg = "lease_effect must be NoLeaseEffect or FinalizeLeaseRecovery"
        raise TypeError(msg)

    seen_feed_ids: set[uuid.UUID] = set()
    progress_commands: list[AdmittedAudioProgress] = []
    observation_commands: list[SourceObservation] = []
    closed_cohort_commands: list[ClosedCohortProgress] = []
    failure_commands: list[FeedFailureTransition] = []
    for mutation in batch.mutations:
        if type(mutation) not in (
            AdmittedAudioProgress,
            SourceObservation,
            ClosedCohortProgress,
            FeedFailureTransition,
        ):
            msg = f"unsupported child mutation {type(mutation).__name__}"
            raise TypeError(msg)
        member = _require_member_identity(grant, mutation.member)
        if member.feed_id in seen_feed_ids:
            msg = f"duplicate Feed UUID {member.feed_id}"
            raise ValueError(msg)
        seen_feed_ids.add(member.feed_id)
        _require_utc_cursor(
            _mutation_cursor(mutation),
            field_name=(
                "completion_cursor"
                if isinstance(mutation, FeedFailureTransition)
                else "cursor"
            ),
        )
        if isinstance(mutation, AdmittedAudioProgress):
            if (
                not isinstance(mutation.last_processed_filename, str)
                or not mutation.last_processed_filename.strip()
            ):
                msg = "last_processed_filename must be nonempty"
                raise ValueError(msg)
            progress_commands.append(mutation)
        elif isinstance(mutation, SourceObservation):
            observation_commands.append(mutation)
        elif isinstance(mutation, ClosedCohortProgress):
            _require_closed_cohort_progress(mutation)
            closed_cohort_commands.append(mutation)
        else:
            _require_failure_action(mutation.action)
            _require_status_reason(mutation.status_reason)
            _require_reason_detail(mutation.reason)
            failure_commands.append(mutation)

    _require_parallel_cardinality(
        "admitted progress",
        tuple(command.member.feed_id for command in progress_commands),
        tuple(command.last_processed_filename for command in progress_commands),
        tuple(command.cursor for command in progress_commands),
    )
    _require_parallel_cardinality(
        "source observation",
        tuple(command.member.feed_id for command in observation_commands),
        tuple(command.cursor for command in observation_commands),
    )
    _require_parallel_cardinality(
        "closed cohort progress",
        tuple(command.member.feed_id for command in closed_cohort_commands),
        tuple(
            command.last_processed_filename
            for command in closed_cohort_commands
        ),
        tuple(command.cursor for command in closed_cohort_commands),
    )
    _require_parallel_cardinality(
        "Feed failure",
        tuple(command.member.feed_id for command in failure_commands),
        tuple(command.completion_cursor for command in failure_commands),
        tuple(command.action for command in failure_commands),
        tuple(command.status_reason for command in failure_commands),
        tuple(command.reason for command in failure_commands),
    )
    return batch


def _child_feed_id_from_row(row: collections.abc.Mapping) -> uuid.UUID:
    feed_id = row["id"]
    if not isinstance(feed_id, uuid.UUID):
        msg = "child Feed row contains a non-UUID id"
        raise TypeError(msg)
    return feed_id


def _child_source_type_from_row(
    row: collections.abc.Mapping,
) -> feed_store.SourceType:
    value = row["source_type"]
    try:
        return feed_store.SourceType(value)
    except ValueError as error:
        msg = f"Unknown child Feed source type {value!r}"
        raise ValueError(msg) from error


def _should_write_cursor(
    current: datetime.datetime | None,
    requested: datetime.datetime | None,
) -> bool:
    """Return whether ``requested`` advances the durable Feed cursor."""
    return requested is not None and (current is None or requested > current)


def _has_dirty_child_lifecycle(
    status: feed_store.FeedStatus,
    row: collections.abc.Mapping,
) -> bool:
    return (
        status is feed_store.FeedStatus.FAILING
        or row["failure_count"] != 0
        or row["retry_after"] is not None
        or row["status_reason"] is not None
        or row["status_reason_detail"] is not None
    )


def _select_recovery_audit_action(
    status: feed_store.FeedStatus,
    row: collections.abc.Mapping,
) -> str | None:
    effective_abnormal = status is feed_store.FeedStatus.FAILING or (
        status is feed_store.FeedStatus.ACTIVE
        and (row["failure_count"] > 0 or row["status_reason"] is not None)
    )
    return "feed.recovered" if effective_abnormal else None


def _select_failure_audit_action(
    row: collections.abc.Mapping,
    mutation: FeedFailureTransition,
    *,
    quarantined: bool,
) -> str | None:
    """Apply the canonical Feed failure/quarantine event rules."""
    if quarantined:
        return "feed.quarantined"
    status = _status_from_row(row)
    effective_prior_failing = status is feed_store.FeedStatus.FAILING or (
        status is feed_store.FeedStatus.ACTIVE
        and (row["failure_count"] > 0 or row["status_reason"] is not None)
    )
    if not effective_prior_failing or (
        _status_reason_from_row(row) is not mutation.status_reason
    ):
        return "feed.failure_reported"
    return None


def _effective_prior_status(
    row: collections.abc.Mapping,
) -> feed_store.FeedStatus:
    """Apply the canonical dirty-active compatibility interpretation."""
    status = _status_from_row(row)
    if status is feed_store.FeedStatus.ACTIVE and (
        row["failure_count"] > 0 or row["status_reason"] is not None
    ):
        return feed_store.FeedStatus.FAILING
    return status


def _confirmed_child_audit_action(
    plan: _PlannedChildMutation,
    after_row: collections.abc.Mapping,
) -> str | None:
    """Select a canonical action from the locked before and returned after."""
    if isinstance(plan.mutation, ClosedCohortProgress):
        if plan.audit_action is not None:
            msg = "closed cohort progress cannot request an audit action"
            raise ValueError(msg)
        return None
    before_row = plan.before_row
    if before_row is None:
        msg = "updated child lacks locked before state"
        raise ValueError(msg)
    before_status = _effective_prior_status(before_row)
    after_status = _status_from_row(after_row)
    if isinstance(plan.mutation, FeedFailureTransition):
        if (
            after_status is feed_store.FeedStatus.QUARANTINED
            and before_status is not feed_store.FeedStatus.QUARANTINED
            and after_row["failure_count"] > before_row["failure_count"]
        ):
            return "feed.quarantined"
        if after_status is feed_store.FeedStatus.FAILING and (
            before_status is not feed_store.FeedStatus.FAILING
            or _status_reason_from_row(before_row)
            != _status_reason_from_row(after_row)
        ):
            return "feed.failure_reported"
        return None
    if (
        before_status
        in (feed_store.FeedStatus.FAILING, feed_store.FeedStatus.QUARANTINED)
        and after_status
        not in (
            feed_store.FeedStatus.FAILING,
            feed_store.FeedStatus.QUARANTINED,
        )
        and after_row["status_reason"] is None
        and after_row["failure_count"] == 0
    ):
        return "feed.recovered"
    return None


def _json_compatible(value: object) -> object:
    if isinstance(value, datetime.datetime):
        return value.isoformat()
    if isinstance(value, uuid.UUID):
        return str(value)
    if isinstance(value, enum.Enum):
        return value.value
    msg = f"unsupported Feed audit JSON value {type(value).__name__}"
    raise TypeError(msg)


def _tags_from_property(value: object) -> object:
    if value is None:
        return []
    if isinstance(value, str):
        parsed = json.loads(value)
        if not isinstance(parsed, list):
            msg = "Feed audit tags must be a JSON array"
            raise TypeError(msg)
        return parsed
    if isinstance(value, list):
        return value
    msg = "Feed audit tags must be a JSON array"
    raise TypeError(msg)


def _child_audit_snapshot(
    row: collections.abc.Mapping,
    property_row: collections.abc.Mapping,
) -> str:
    source = dict(row)
    source["source_feed_id"] = property_row["source_feed_id"]
    source["tags"] = _tags_from_property(property_row["tags"])
    snapshot = {
        key: source[column]
        for key, column in feed_audit_sql.AUDITED_FEED_STATE_FIELDS
    }
    return json.dumps(
        snapshot,
        default=_json_compatible,
        separators=(",", ":"),
    )


def _audit_properties_by_id(
    expected_feed_ids: set[uuid.UUID],
    rows: collections.abc.Sequence[collections.abc.Mapping],
) -> dict[uuid.UUID, collections.abc.Mapping]:
    properties_by_id: dict[uuid.UUID, collections.abc.Mapping] = {}
    for row in rows:
        feed_id = row["feed_id"]
        if not isinstance(feed_id, uuid.UUID):
            msg = "Feed audit property row has a non-UUID id"
            raise TypeError(msg)
        if feed_id not in expected_feed_ids or feed_id in properties_by_id:
            msg = "Feed audit properties returned an unexpected row"
            raise ValueError(msg)
        properties_by_id[feed_id] = row
    if set(properties_by_id) != expected_feed_ids:
        msg = "Feed audit properties are missing an actual candidate"
        raise ValueError(msg)
    return properties_by_id


def _build_child_audit_rowset(
    candidates: collections.abc.Sequence[_PlannedChildMutation],
    updated_by_ordinal: dict[int, collections.abc.Mapping],
    properties_by_id: dict[uuid.UUID, collections.abc.Mapping],
    actor_id: str,
) -> _ChildAuditRowset:
    feed_ids: list[uuid.UUID] = []
    actions: list[str] = []
    revisions: list[int] = []
    before_values: list[str] = []
    after_values: list[str] = []
    caller_ordinals: list[int] = []
    for plan in sorted(candidates, key=lambda item: item.caller_ordinal):
        before_row = plan.before_row
        after_row = updated_by_ordinal.get(plan.caller_ordinal)
        if before_row is None or after_row is None:
            msg = "Feed audit candidate lacks before or after state"
            raise ValueError(msg)
        property_row = properties_by_id[plan.feed_id]
        if property_row["source_feed_id"] != (
            plan.mutation.member.source_feed_id
        ):
            msg = "Feed audit property identity does not match the member"
            raise ValueError(msg)
        before_revision = before_row["audit_revision"]
        after_revision = after_row["audit_revision"]
        if (
            not isinstance(before_revision, int)
            or not isinstance(after_revision, int)
            or after_revision != before_revision + 1
        ):
            msg = "Feed audit revision did not advance exactly once"
            raise ValueError(msg)
        feed_ids.append(plan.feed_id)
        actions.append(typing.cast("str", plan.audit_action))
        revisions.append(after_revision)
        before_values.append(_child_audit_snapshot(before_row, property_row))
        after_values.append(_child_audit_snapshot(after_row, property_row))
        caller_ordinals.append(plan.caller_ordinal)

    actors = [actor_id] * len(feed_ids)
    _require_parallel_cardinality(
        "child audit",
        feed_ids,
        actions,
        actors,
        revisions,
        before_values,
        after_values,
        caller_ordinals,
    )
    return _ChildAuditRowset(
        feed_ids=tuple(feed_ids),
        actions=tuple(actions),
        actors=tuple(actors),
        revisions=tuple(revisions),
        before_values=tuple(before_values),
        after_values=tuple(after_values),
        caller_ordinals=tuple(caller_ordinals),
    )


def _source_type_from_row(
    row: collections.abc.Mapping,
) -> feed_store.SourceType:
    value = row["source_type"]
    try:
        return feed_store.SourceType(value)
    except ValueError as error:
        msg = f"Unknown Lease source type {value!r}"
        raise ValueError(msg) from error


def _status_from_row(
    row: collections.abc.Mapping,
    prefix: str = "",
) -> feed_store.FeedStatus:
    value = row[f"{prefix}status"]
    try:
        return feed_store.FeedStatus(value)
    except ValueError as error:
        msg = f"Unknown Lease status {value!r}"
        raise ValueError(msg) from error


def _status_reason_from_row(
    row: collections.abc.Mapping,
    prefix: str = "",
) -> feed_store.FeedStatusReason | None:
    value = row[f"{prefix}status_reason"]
    if value is None:
        return None
    try:
        return feed_store.FeedStatusReason(value)
    except ValueError as error:
        msg = f"Unknown Lease status reason {value!r}"
        raise ValueError(msg) from error


def _failure_count_from_row(row: collections.abc.Mapping) -> int:
    """Return one validated durable Lease failure count."""
    value = row["failure_count"]
    if isinstance(value, bool) or not isinstance(value, int):
        msg = "Lease failure_count must be an integer"
        raise TypeError(msg)
    if value < 0:
        msg = "Lease failure_count must be nonnegative"
        raise ValueError(msg)
    return value


def _lifecycle_dirty_from_row(row: collections.abc.Mapping) -> bool:
    """Whether successful work must clear any retained lifecycle field."""
    return (
        _failure_count_from_row(row) != 0
        or row["status_reason"] is not None
        or row["retry_after"] is not None
        or row["status_reason_detail"] is not None
    )


def _membership_revision_from_row(
    row: collections.abc.Mapping,
) -> int:
    """Return a validated cache-invalidating membership revision."""
    value = row["membership_revision"]
    if isinstance(value, bool) or not isinstance(value, int):
        msg = "Lease membership_revision must be an integer"
        raise TypeError(msg)
    if value < 0:
        msg = "Lease membership_revision must be nonnegative"
        raise ValueError(msg)
    return value


def _claim_from_row(row: collections.abc.Mapping) -> LeaseClaim:
    grant = LeaseGrant(
        source_type=_source_type_from_row(row),
        lease_key=row["lease_key"],
        owner_worker_id=row["worker_id"],
        fencing_token=row["fencing_token"],
    )
    return LeaseClaim(grant=grant)


def _disposition_for_rejection(
    reason: GrantRejectionReason,
) -> LeaseOperationDisposition:
    return LeaseOperationDisposition(reason.value)


def _membership_identity_from_row(
    grant: LeaseGrant,
    row: collections.abc.Mapping,
) -> LeaseMemberIdentity | MembershipInvariantViolation:
    feed_id = row["feed_id"]
    source_feed_id = row["source_feed_id"]
    sid = row["sid"]
    group_id = row["group_id"]
    if (
        not isinstance(feed_id, uuid.UUID)
        or not isinstance(source_feed_id, str)
        or not source_feed_id
        or not isinstance(sid, str)
        or not sid
        or not sid.isascii()
        or not sid.isdigit()
        or not isinstance(group_id, str)
        or not group_id
        or not group_id.isascii()
        or not group_id.isdigit()
        or source_feed_id != f"{sid}-{group_id}"
        or sid != grant.lease_key
    ):
        return MembershipInvariantViolation(grant)

    property_source_raw = row["property_source_type"]
    feed_source_raw = row["feed_source_type"]
    if not isinstance(property_source_raw, str) or not isinstance(
        feed_source_raw,
        str,
    ):
        return MembershipInvariantViolation(grant)
    try:
        property_source = feed_store.SourceType(property_source_raw)
        feed_source = feed_store.SourceType(feed_source_raw)
    except ValueError as error:
        msg = "membership row contains an unknown source binding"
        raise ValueError(msg) from error
    if (
        property_source is not grant.source_type
        or feed_source is not property_source
    ):
        return MembershipInvariantViolation(grant)
    return LeaseMemberIdentity(
        feed_id=feed_id,
        source_type=property_source,
        source_feed_id=source_feed_id,
        sid=sid,
        group_id=group_id,
    )


def _member_from_row(
    identity: LeaseMemberIdentity,
    row: collections.abc.Mapping,
) -> LeaseMember:
    try:
        name = row["feed_name"]
    except KeyError as error:
        msg = "membership row is missing the canonical Feed name"
        raise ValueError(msg) from error
    if not isinstance(name, str):
        msg = "membership Feed name must be a string"
        raise TypeError(msg)
    if not name.strip():
        msg = "membership Feed name must not be blank"
        raise ValueError(msg)
    return LeaseMember(
        identity=identity,
        name=name,
        last_bookmark_time=row["last_bookmark_time"],
    )


def _locked_children_by_id(
    requested_feed_ids: set[uuid.UUID],
    rows: collections.abc.Sequence[collections.abc.Mapping],
) -> dict[uuid.UUID, collections.abc.Mapping]:
    by_id: dict[uuid.UUID, collections.abc.Mapping] = {}
    for row in rows:
        feed_id = _child_feed_id_from_row(row)
        if feed_id not in requested_feed_ids or feed_id in by_id:
            msg = "child Feed lock returned an unexpected or duplicate row"
            raise ValueError(msg)
        _child_source_type_from_row(row)
        _status_from_row(row)
        _status_reason_from_row(row)
        by_id[feed_id] = row
    return by_id


def _plan_non_failure_child_mutation(
    caller_ordinal: int,
    mutation: AdmittedAudioProgress | SourceObservation | ClosedCohortProgress,
    before_row: collections.abc.Mapping,
    status: feed_store.FeedStatus,
    *,
    write_cursor: bool,
) -> _PlannedChildMutation:
    """Plan progress or observation separately from failure charging."""
    if isinstance(mutation, ClosedCohortProgress):
        write_path = (
            mutation.last_processed_filename is not None
            and before_row["last_processed_filename"]
            != mutation.last_processed_filename
        )
        return _PlannedChildMutation(
            caller_ordinal=caller_ordinal,
            mutation=mutation,
            before_row=before_row,
            disposition=ChildDisposition.COMMITTED,
            write_cursor=write_cursor,
            write_path=write_path,
        )

    clear_lifecycle = _has_dirty_child_lifecycle(status, before_row)

    write_path = False
    if isinstance(mutation, AdmittedAudioProgress):
        write_path = write_cursor or (
            mutation.cursor is None
            and before_row["last_processed_filename"]
            != mutation.last_processed_filename
        )

    audit_action = (
        _select_recovery_audit_action(status, before_row)
        if clear_lifecycle and status is not feed_store.FeedStatus.DEACTIVATED
        else None
    )
    return _PlannedChildMutation(
        caller_ordinal=caller_ordinal,
        mutation=mutation,
        before_row=before_row,
        disposition=ChildDisposition.COMMITTED,
        write_cursor=write_cursor,
        write_path=write_path,
        clear_lifecycle=clear_lifecycle,
        audit_action=audit_action,
    )


def _plan_child_mutation(
    caller_ordinal: int,
    mutation: ChildMutation,
    before_row: collections.abc.Mapping | None,
) -> _PlannedChildMutation:
    if before_row is None:
        return _PlannedChildMutation(
            caller_ordinal=caller_ordinal,
            mutation=mutation,
            before_row=None,
            disposition=ChildDisposition.REJECTED,
        )

    status = _status_from_row(before_row)
    source_type = _child_source_type_from_row(before_row)
    write_cursor = _should_write_cursor(
        before_row["last_bookmark_time"],
        _mutation_cursor(mutation),
    )
    if source_type is not mutation.member.source_type:
        return _PlannedChildMutation(
            caller_ordinal=caller_ordinal,
            mutation=mutation,
            before_row=before_row,
            disposition=ChildDisposition.REJECTED,
        )

    is_progress = isinstance(
        mutation,
        (AdmittedAudioProgress, ClosedCohortProgress),
    )
    allowed_statuses = (
        (
            feed_store.FeedStatus.ACTIVE,
            feed_store.FeedStatus.FAILING,
            feed_store.FeedStatus.DEACTIVATED,
        )
        if is_progress
        else (
            feed_store.FeedStatus.ACTIVE,
            feed_store.FeedStatus.FAILING,
        )
    )
    if status not in allowed_statuses:
        return _PlannedChildMutation(
            caller_ordinal=caller_ordinal,
            mutation=mutation,
            before_row=before_row,
            disposition=ChildDisposition.REJECTED,
        )

    if isinstance(mutation, FeedFailureTransition):
        failure = mutation
        quarantined = isinstance(failure.action, BudgetedFailure) and (
            before_row["failure_count"] + 1 >= failure.action.failure_threshold
        )
        return _PlannedChildMutation(
            caller_ordinal=caller_ordinal,
            mutation=mutation,
            before_row=before_row,
            disposition=(
                ChildDisposition.COMMITTED_AND_QUARANTINED
                if quarantined
                else ChildDisposition.COMMITTED
            ),
            write_cursor=write_cursor,
            audit_action=_select_failure_audit_action(
                before_row,
                failure,
                quarantined=quarantined,
            ),
        )

    return _plan_non_failure_child_mutation(
        caller_ordinal,
        mutation,
        before_row,
        status,
        write_cursor=write_cursor,
    )


def _updated_children_by_ordinal(
    expected: collections.abc.Sequence[_PlannedChildMutation],
    rows: collections.abc.Sequence[collections.abc.Mapping],
) -> dict[int, collections.abc.Mapping]:
    expected_by_ordinal = {
        plan.caller_ordinal: plan.feed_id for plan in expected
    }
    rows_by_ordinal: dict[int, collections.abc.Mapping] = {}
    for row in rows:
        ordinal = row["caller_ordinal"]
        if (
            not isinstance(ordinal, int)
            or ordinal not in expected_by_ordinal
            or ordinal in rows_by_ordinal
            or _child_feed_id_from_row(row) != expected_by_ordinal[ordinal]
        ):
            msg = "child DML returned an unexpected or duplicate row"
            raise ValueError(msg)
        _child_source_type_from_row(row)
        _status_from_row(row)
        _status_reason_from_row(row)
        rows_by_ordinal[ordinal] = row
    if set(rows_by_ordinal) != set(expected_by_ordinal):
        msg = "child DML did not return every locked eligible Feed"
        raise ValueError(msg)
    return rows_by_ordinal


def _updated_neutral_children_by_ordinal(
    expected: collections.abc.Sequence[_PlannedChildMutation],
    rows: collections.abc.Sequence[collections.abc.Mapping],
) -> dict[int, collections.abc.Mapping]:
    """Correlate lifecycle-neutral DML without reading lifecycle columns."""
    expected_by_ordinal = {
        plan.caller_ordinal: plan.feed_id for plan in expected
    }
    rows_by_ordinal: dict[int, collections.abc.Mapping] = {}
    for row in rows:
        ordinal = row["caller_ordinal"]
        if (
            not isinstance(ordinal, int)
            or ordinal not in expected_by_ordinal
            or ordinal in rows_by_ordinal
            or _child_feed_id_from_row(row) != expected_by_ordinal[ordinal]
        ):
            msg = "neutral child DML returned an unexpected or duplicate row"
            raise ValueError(msg)
        rows_by_ordinal[ordinal] = row
    if set(rows_by_ordinal) != set(expected_by_ordinal):
        msg = "neutral child DML did not return every locked eligible Feed"
        raise ValueError(msg)
    return rows_by_ordinal


class IngestionLeaseStore:
    """Storage facade for complete-grant Lease control operations."""

    def __init__(self, pool: asyncpg.Pool) -> None:
        """Initialize the store over a managed asyncpg pool.

        Args:
            pool: Database pool used for one-shot Lease operations.
        """
        self._pool = pool

    def _grant_rejection(
        self,
        grant: LeaseGrant,
        row: collections.abc.Mapping | None,
    ) -> GrantRejected | None:
        """Classify a locked Lease row against a complete active grant."""
        reason = self._grant_rejection_reason(grant, row)
        if reason is None:
            return None
        return GrantRejected(reason)

    def _grant_rejection_reason(
        self,
        grant: LeaseGrant,
        row: collections.abc.Mapping | None,
    ) -> GrantRejectionReason | None:
        """Classify a locked Lease row without constructing a snapshot."""
        _require_grant(grant)
        if row is None:
            return GrantRejectionReason.MISSING

        source_type = _source_type_from_row(row)
        if source_type is not grant.source_type or row["lease_key"] != (
            grant.lease_key
        ):
            msg = "locked Lease identity did not match the requested identity"
            raise ValueError(msg)

        if _status_from_row(row) is not feed_store.FeedStatus.ACTIVE:
            return GrantRejectionReason.STATUS_INELIGIBLE
        if row["worker_id"] != grant.owner_worker_id:
            return GrantRejectionReason.OWNER_MISMATCH
        if row["fencing_token"] != grant.fencing_token:
            return GrantRejectionReason.FENCE_MISMATCH
        return None

    async def claim_unclaimed(
        self,
        source_type: feed_store.SourceType,
        owner_worker_id: uuid.UUID,
        limit: int,
    ) -> tuple[LeaseClaim, ...]:
        """Claim deterministic unclaimed Leases as new generations.

        Args:
            source_type: Source namespace to claim from.
            owner_worker_id: Worker that will own every returned grant.
            limit: Maximum number of Lease generations to establish.

        Returns:
            Claims ordered by permanent Lease identity.

        Raises:
            TypeError: An argument has the wrong runtime type.
            ValueError: The limit is negative or a returned row is invalid.
        """
        source_type = _require_source_type(source_type)
        owner_worker_id = _require_owner_worker_id(owner_worker_id)
        limit = _require_limit(limit)
        if limit == 0:
            return ()

        rows = await self._pool.fetch(
            ingestion_lease_queries.CLAIM_UNCLAIMED_LEASES_SQL,
            source_type.value,
            owner_worker_id,
            limit,
        )
        claims = tuple(_claim_from_row(row) for row in rows)
        if any(
            claim.grant.source_type is not source_type
            or claim.grant.owner_worker_id != owner_worker_id
            for claim in claims
        ):
            msg = "claim query returned an unexpected source type"
            raise ValueError(msg)
        return claims

    async def claim_recoverable(
        self,
        source_type: feed_store.SourceType,
        owner_worker_id: uuid.UUID,
        limit: int,
        abandonment_after: datetime.timedelta,
    ) -> tuple[LeaseClaim, ...]:
        """Claim due failing Leases before stale active Leases.

        Args:
            source_type: Source namespace to recover from.
            owner_worker_id: Worker that will own every returned grant.
            limit: Maximum number of Lease generations to establish.
            abandonment_after: Age at which an active heartbeat is stale.

        Returns:
            Claims ordered by permanent Lease identity after recovery priority.

        Raises:
            TypeError: An argument has the wrong runtime type.
            ValueError: A bound is invalid or a returned row is invalid.
        """
        source_type = _require_source_type(source_type)
        owner_worker_id = _require_owner_worker_id(owner_worker_id)
        limit = _require_limit(limit)
        abandonment_after = _require_abandonment_after(abandonment_after)
        if limit == 0:
            return ()

        rows = await self._pool.fetch(
            ingestion_lease_queries.CLAIM_RECOVERABLE_LEASES_SQL,
            source_type.value,
            owner_worker_id,
            limit,
            abandonment_after,
        )
        claims = tuple(_claim_from_row(row) for row in rows)
        if any(
            claim.grant.source_type is not source_type
            or claim.grant.owner_worker_id != owner_worker_id
            for claim in claims
        ):
            msg = "recovery query returned an unexpected source type"
            raise ValueError(msg)
        return claims

    async def renew_heartbeats(
        self,
        grants: collections.abc.Sequence[LeaseGrant],
    ) -> tuple[LeaseHeartbeatResult, ...]:
        """Renew exact active grants and preserve caller result order.

        Args:
            grants: Distinct complete grants to renew in one database call.

        Returns:
            One typed result per input grant in the original caller order.

        Raises:
            TypeError: An item is not a complete Lease grant.
            ValueError: The input repeats a permanent Lease identity.
            RuntimeError: An exact active row unexpectedly was not updated.
        """
        grants = tuple(grants)
        identities: set[tuple[feed_store.SourceType, str]] = set()
        for candidate in grants:
            grant = _require_grant(candidate)
            identity = (grant.source_type, grant.lease_key)
            if identity in identities:
                msg = f"duplicate Lease identity {identity!r}"
                raise ValueError(msg)
            identities.add(identity)
        if not grants:
            return ()

        ordered = sorted(
            enumerate(grants),
            key=lambda item: (
                item[1].source_type.value,
                item[1].lease_key,
            ),
        )
        rows = await self._pool.fetch(
            ingestion_lease_queries.RENEW_LEASE_HEARTBEATS_SQL,
            [grant.source_type.value for _, grant in ordered],
            [grant.lease_key for _, grant in ordered],
            [grant.owner_worker_id for _, grant in ordered],
            [grant.fencing_token for _, grant in ordered],
            [ordinal for ordinal, _ in ordered],
        )
        rows_by_ordinal = {row["caller_ordinal"]: row for row in rows}
        return tuple(
            self._heartbeat_result(
                grant,
                rows_by_ordinal.get(ordinal),
            )
            for ordinal, grant in enumerate(grants)
        )

    def _heartbeat_result(
        self,
        grant: LeaseGrant,
        row: collections.abc.Mapping | None,
    ) -> LeaseHeartbeatResult:
        if row is None or row["status"] is None:
            return LeaseHeartbeatResult(
                grant,
                LeaseOperationDisposition.MISSING,
            )
        rejection_reason = self._grant_rejection_reason(grant, row)
        if row["applied"]:
            if rejection_reason is not None:
                msg = "heartbeat updated a mismatched Lease grant"
                raise RuntimeError(msg)
            return LeaseHeartbeatResult(
                grant,
                LeaseOperationDisposition.APPLIED,
            )
        if rejection_reason is None:
            msg = "heartbeat did not update an exact active Lease grant"
            raise RuntimeError(msg)
        return LeaseHeartbeatResult(
            grant,
            _disposition_for_rejection(rejection_reason),
        )

    async def release(
        self,
        grant: LeaseGrant,
        cause: LeaseReleaseCause = LeaseReleaseCause.NORMAL,
    ) -> LeaseOperationResult:
        """Neutrally release one exact active grant.

        Args:
            grant: Complete active ownership generation to release.
            cause: Telemetry classification; storage policy is unchanged.

        Returns:
            Applied result or current typed rejection state.

        Raises:
            TypeError: The grant or cause has the wrong runtime type.
            RuntimeError: An exact active row unexpectedly was not updated.
        """
        grant = _require_grant(grant)
        if not isinstance(cause, LeaseReleaseCause):
            msg = "cause must be a LeaseReleaseCause"
            raise TypeError(msg)

        row = await self._pool.fetchrow(
            ingestion_lease_queries.RELEASE_LEASE_SQL,
            grant.source_type.value,
            grant.lease_key,
            grant.owner_worker_id,
            grant.fencing_token,
        )
        if row is None:
            return LeaseOperationResult(LeaseOperationDisposition.MISSING)
        if row["applied"]:
            rejection_reason = self._grant_rejection_reason(grant, row)
            if rejection_reason is not None:
                msg = "release updated a mismatched Lease grant"
                raise RuntimeError(msg)
            logger.info(
                "Ingestion Lease released",
                extra={
                    "source_type": grant.source_type.value,
                    "lease_key": grant.lease_key,
                    "owner_worker_id": str(grant.owner_worker_id),
                    "fencing_token": grant.fencing_token,
                    "release_cause": cause.value,
                },
            )
            return LeaseOperationResult(LeaseOperationDisposition.APPLIED)

        rejection = self._grant_rejection(grant, row)
        if rejection is None:
            msg = "release did not update an exact active Lease grant"
            raise RuntimeError(msg)
        return LeaseOperationResult(
            _disposition_for_rejection(rejection.reason)
        )

    async def finalize_failure(
        self,
        grant: LeaseGrant,
        action: LeaseFailureAction,
        status_reason: feed_store.FeedStatusReason,
        *,
        actor_id: str,
        reason: str | None = None,
    ) -> LeaseFailureResult:
        """Finalize one Lease failure through one exact active grant.

        The caller classifies the failure by choosing a closed action. Storage
        atomically releases ownership and applies that action without retrying
        an outcome-unknown database call.

        Args:
            grant: Complete active ownership generation to finalize.
            action: Budgeted or non-budgeted durable failure policy.
            status_reason: Canonical abnormal lifecycle reason.
            actor_id: Whitespace-free causal actor used in structured logs.
            reason: Optional operator-facing detail, bounded before persistence.

        Returns:
            A narrow applied effect or typed exact-grant rejection.

        Raises:
            TypeError: An argument has the wrong runtime type.
            ValueError: A bound is invalid or a database value is unknown.
            RuntimeError: The locked exact grant produced an impossible result.
        """
        grant = _require_grant(grant)
        action = _require_failure_action(action)
        status_reason = _require_status_reason(status_reason)
        actor_id = _require_log_actor_id(actor_id)
        detail = _require_reason_detail(reason)

        if isinstance(action, BudgetedFailure):
            query = ingestion_lease_queries.FINALIZE_BUDGETED_FAILURE_SQL
            parameters = (
                grant.source_type.value,
                grant.lease_key,
                grant.owner_worker_id,
                grant.fencing_token,
                action.failure_threshold,
                action.backoff_base_sec,
                action.backoff_max_sec,
                status_reason.value,
                detail,
            )
        else:
            query = ingestion_lease_queries.FINALIZE_NON_BUDGETED_FAILURE_SQL
            parameters = (
                grant.source_type.value,
                grant.lease_key,
                grant.owner_worker_id,
                grant.fencing_token,
                action.retry_after,
                status_reason.value,
                detail,
            )

        row = await self._pool.fetchrow(query, *parameters)
        result = self._failure_result(grant, row)
        if result.disposition is LeaseOperationDisposition.APPLIED:
            final_status = result.final_status
            if final_status is None:
                msg = "applied Lease failure is missing its final status"
                raise RuntimeError(msg)
            logger.warning(
                "Ingestion Lease failure finalized",
                extra={
                    "source_type": grant.source_type.value,
                    "lease_key": grant.lease_key,
                    "owner_worker_id": str(grant.owner_worker_id),
                    "fencing_token": grant.fencing_token,
                    "actor_id": actor_id,
                    "status_reason": status_reason.value,
                    "failure_action": type(action).__name__,
                    "final_status": final_status.value,
                },
            )
        return result

    def _failure_result(
        self,
        grant: LeaseGrant,
        row: collections.abc.Mapping | None,
    ) -> LeaseFailureResult:
        """Validate and convert one locked failure-finalization result.

        Args:
            grant: Complete authority used for the mutation attempt.
            row: Locked query result, or ``None`` when the Lease is missing.

        Returns:
            Exact-grant disposition and applied final status, if any.

        Raises:
            ValueError: Locked identity or returned status is unknown.
            RuntimeError: The query returned an internally inconsistent result.
        """
        if row is None:
            return LeaseFailureResult(
                LeaseOperationDisposition.MISSING,
                None,
            )

        rejection_reason = self._grant_rejection_reason(grant, row)
        if not row["applied"]:
            if row["final_status"] is not None:
                msg = "rejected Lease failure unexpectedly returned final state"
                raise RuntimeError(msg)
            if rejection_reason is None:
                msg = "failure did not update an exact active Lease grant"
                raise RuntimeError(msg)
            return LeaseFailureResult(
                _disposition_for_rejection(rejection_reason),
                None,
            )

        if rejection_reason is not None:
            msg = "failure updated a mismatched Lease grant"
            raise RuntimeError(msg)
        try:
            final_status = feed_store.FeedStatus(row["final_status"])
        except ValueError as error:
            msg = f"Unknown finalized Lease status {row['final_status']!r}"
            raise ValueError(msg) from error
        if final_status not in (
            feed_store.FeedStatus.FAILING,
            feed_store.FeedStatus.QUARANTINED,
        ):
            msg = (
                f"Failure finalized to ineligible status {final_status.value!r}"
            )
            raise RuntimeError(msg)
        return LeaseFailureResult(
            LeaseOperationDisposition.APPLIED,
            final_status,
        )

    async def load_membership(
        self,
        grant: LeaseGrant,
    ) -> GrantRejected | MembershipSnapshot | MembershipInvariantViolation:
        """Load one fail-closed authoritative Calls membership snapshot.

        Controlled administrative SQL owns immutable identity maintenance and
        revision increments. The revision returned here invalidates caches; it
        never participates in grant or lifecycle authorization.

        Args:
            grant: Complete immutable Lease ownership generation.

        Returns:
            A complete snapshot, exact-grant rejection, or structural
            invariant violation.

        Raises:
            TypeError: If the grant has the wrong runtime type.
            ValueError: If the source is unsupported or a row is invalid.
        """
        result = await self.refresh_membership(grant, known_revision=None)
        if isinstance(result, MembershipUnchanged):
            msg = "full membership load returned an unchanged result"
            raise TypeError(msg)
        return result

    async def refresh_membership(
        self,
        grant: LeaseGrant,
        *,
        known_revision: int | None,
    ) -> MembershipRefreshResult:
        """Conditionally load membership beneath one locked exact grant.

        Args:
            grant: Complete immutable Lease ownership generation.
            known_revision: Last authoritative revision, or None initially.

        Returns:
            A complete snapshot, unchanged proof, grant rejection, or
            structural invariant violation.

        Raises:
            TypeError: If the grant or known revision has the wrong type.
            ValueError: If the source or known revision is invalid.
        """
        grant = _require_grant(grant)
        known_revision = _require_known_membership_revision(known_revision)
        if grant.source_type is not feed_store.SourceType.BCFY_CALLS:
            msg = "membership loading supports only bcfy_calls Leases"
            raise ValueError(msg)

        async with self._pool.acquire() as connection:
            async with connection.transaction(isolation="read_committed"):
                lease_row = await connection.fetchrow(
                    ingestion_lease_queries.LOCK_LEASE_SQL,
                    grant.source_type.value,
                    grant.lease_key,
                )
                rejection = self._grant_rejection(grant, lease_row)
                if rejection is not None:
                    return rejection
                if lease_row is None:
                    msg = "accepted Lease grant lacks a locked row"
                    raise RuntimeError(msg)

                current_revision = _membership_revision_from_row(lease_row)
                if current_revision == known_revision:
                    return MembershipUnchanged(grant, current_revision)
                if (
                    known_revision is not None
                    and current_revision < known_revision
                ):
                    return MembershipInvariantViolation(grant)

                rows = await connection.fetch(
                    ingestion_lease_queries.LOAD_BCFY_CALLS_MEMBERSHIP_SQL,
                    grant.lease_key,
                )
                return self._membership_result(
                    grant,
                    current_revision,
                    rows,
                )

    def _membership_result(
        self,
        grant: LeaseGrant,
        membership_revision: int,
        rows: collections.abc.Sequence[collections.abc.Mapping],
    ) -> MembershipSnapshot | MembershipInvariantViolation:
        if not rows:
            return MembershipInvariantViolation(grant)

        members: list[LeaseMember] = []
        routing_keys: set[str] = set()
        for row in rows:
            identity = _membership_identity_from_row(grant, row)
            if isinstance(identity, MembershipInvariantViolation):
                return identity
            if identity.source_feed_id in routing_keys:
                return MembershipInvariantViolation(grant)
            routing_keys.add(identity.source_feed_id)
            member = _member_from_row(identity, row)
            status = _status_from_row(row)
            if status in (
                feed_store.FeedStatus.ACTIVE,
                feed_store.FeedStatus.FAILING,
            ):
                members.append(member)

        if not members:
            return MembershipInvariantViolation(grant)
        return MembershipSnapshot(
            grant=grant,
            membership_revision=membership_revision,
            members=tuple(members),
        )

    async def commit_child_mutations(
        self,
        grant: LeaseGrant,
        batch: ChildMutationBatch,
        *,
        actor_id: str,
    ) -> GrantRejected | BatchCommitted:
        """Commit one closed child batch beneath an exact active Lease.

        The method makes one explicit READ COMMITTED transaction attempt. It
        performs no storage retry and emits buffered Feed notifications only
        after both the transaction and acquired connection exit normally.

        Args:
            grant: Complete immutable Lease ownership generation.
            batch: Closed, globally de-duplicated child commands.
            actor_id: Durable audit actor identity.

        Returns:
            A batch-level grant rejection or caller-ordered committed results.

        Raises:
            TypeError: If a command uses an unsupported or malformed type.
            ValueError: If command identity, cursor, or rowset data is invalid.
        """
        grant = _require_grant(grant)
        actor_id = _require_actor_id(actor_id)
        batch = _require_child_batch(grant, batch)
        target_feed_ids = sorted(
            (mutation.member.feed_id for mutation in batch.mutations),
            key=lambda feed_id: feed_id.int,
        )
        notification_payloads: dict[int, object] = {}

        async with self._pool.acquire() as connection:
            async with connection.transaction(isolation="read_committed"):
                lease_row = await connection.fetchrow(
                    ingestion_lease_queries.LOCK_LEASE_SQL,
                    grant.source_type.value,
                    grant.lease_key,
                )
                rejection = self._grant_rejection(grant, lease_row)
                if rejection is not None:
                    return rejection
                if lease_row is None:
                    msg = "accepted Lease grant lacks a locked row"
                    raise ValueError(msg)
                locked_rows = []
                if target_feed_ids:
                    locked_rows = await connection.fetch(
                        ingestion_lease_queries.LOCK_CHILD_FEEDS_SQL,
                        target_feed_ids,
                    )
                locked_by_id = _locked_children_by_id(
                    set(target_feed_ids),
                    locked_rows,
                )
                plans = tuple(
                    _plan_child_mutation(
                        caller_ordinal,
                        mutation,
                        locked_by_id.get(mutation.member.feed_id),
                    )
                    for caller_ordinal, mutation in enumerate(batch.mutations)
                )

                updated_by_ordinal: dict[int, collections.abc.Mapping] = {}
                progress_updates = tuple(
                    plan
                    for plan in plans
                    if isinstance(plan.mutation, AdmittedAudioProgress)
                    and plan.needs_update
                )
                observation_updates = tuple(
                    plan
                    for plan in plans
                    if isinstance(plan.mutation, SourceObservation)
                    and plan.needs_update
                )
                closed_cohort_updates = tuple(
                    plan
                    for plan in plans
                    if isinstance(plan.mutation, ClosedCohortProgress)
                    and plan.needs_update
                )
                failure_updates = tuple(
                    plan
                    for plan in plans
                    if isinstance(plan.mutation, FeedFailureTransition)
                    and plan.needs_update
                )
                if progress_updates:
                    rows = await self._apply_admitted_progress(
                        connection,
                        progress_updates,
                    )
                    updated_by_ordinal.update(
                        _updated_children_by_ordinal(progress_updates, rows)
                    )
                if observation_updates:
                    rows = await self._apply_source_observations(
                        connection,
                        observation_updates,
                    )
                    updated_by_ordinal.update(
                        _updated_children_by_ordinal(observation_updates, rows)
                    )
                if closed_cohort_updates:
                    rows = await self._apply_closed_cohort_progress(
                        connection,
                        closed_cohort_updates,
                    )
                    updated_by_ordinal.update(
                        _updated_neutral_children_by_ordinal(
                            closed_cohort_updates,
                            rows,
                        )
                    )
                if failure_updates:
                    rows = await self._apply_feed_failures(
                        connection,
                        failure_updates,
                    )
                    updated_by_ordinal.update(
                        _updated_children_by_ordinal(failure_updates, rows)
                    )

                lease_recovered = await self._apply_lease_effect(
                    connection,
                    grant,
                    batch.lease_effect,
                    lease_row,
                )

                notification_payloads = await self._write_child_audits(
                    connection,
                    plans,
                    updated_by_ordinal,
                    actor_id,
                )
                children = tuple(
                    ChildMutationResult(
                        feed_id=plan.feed_id,
                        disposition=plan.disposition,
                    )
                    for plan in plans
                )
                committed = BatchCommitted(children=children)

        for caller_ordinal in sorted(notification_payloads):
            feed_change_notifications.emit_feed_change_notification(
                notification_payloads[caller_ordinal]
            )
        if lease_recovered:
            logger.info(
                "Ingestion Lease recovered after finalized child boundary",
                extra={
                    "source_type": grant.source_type.value,
                    "lease_key": grant.lease_key,
                    "owner_worker_id": str(grant.owner_worker_id),
                    "fencing_token": grant.fencing_token,
                },
            )
        return committed

    async def _apply_admitted_progress(
        self,
        connection: asyncpg.Connection,
        plans: tuple[_PlannedChildMutation, ...],
    ) -> collections.abc.Sequence[collections.abc.Mapping]:
        """Apply one static admitted-progress rowset."""
        mutations = [
            typing.cast("AdmittedAudioProgress", plan.mutation)
            for plan in plans
        ]
        feed_ids = [plan.feed_id for plan in plans]
        paths = [mutation.last_processed_filename for mutation in mutations]
        cursors = [mutation.cursor for mutation in mutations]
        write_cursors = [plan.write_cursor for plan in plans]
        write_paths = [plan.write_path for plan in plans]
        clear_lifecycle = [plan.clear_lifecycle for plan in plans]
        caller_ordinals = [plan.caller_ordinal for plan in plans]
        _require_parallel_cardinality(
            "admitted progress",
            feed_ids,
            paths,
            cursors,
            write_cursors,
            write_paths,
            clear_lifecycle,
            caller_ordinals,
        )
        return await connection.fetch(
            ingestion_lease_queries.APPLY_ADMITTED_PROGRESS_SQL,
            feed_ids,
            paths,
            cursors,
            write_cursors,
            write_paths,
            clear_lifecycle,
            caller_ordinals,
        )

    async def _apply_source_observations(
        self,
        connection: asyncpg.Connection,
        plans: tuple[_PlannedChildMutation, ...],
    ) -> collections.abc.Sequence[collections.abc.Mapping]:
        """Apply one static source-observation rowset."""
        mutations = [
            typing.cast("SourceObservation", plan.mutation) for plan in plans
        ]
        feed_ids = [plan.feed_id for plan in plans]
        cursors = [mutation.cursor for mutation in mutations]
        write_cursors = [plan.write_cursor for plan in plans]
        clear_lifecycle = [plan.clear_lifecycle for plan in plans]
        caller_ordinals = [plan.caller_ordinal for plan in plans]
        _require_parallel_cardinality(
            "source observation",
            feed_ids,
            cursors,
            write_cursors,
            clear_lifecycle,
            caller_ordinals,
        )
        return await connection.fetch(
            ingestion_lease_queries.APPLY_SOURCE_OBSERVATIONS_SQL,
            feed_ids,
            cursors,
            write_cursors,
            clear_lifecycle,
            caller_ordinals,
        )

    async def _apply_closed_cohort_progress(
        self,
        connection: asyncpg.Connection,
        plans: tuple[_PlannedChildMutation, ...],
    ) -> collections.abc.Sequence[collections.abc.Mapping]:
        """Apply one static lifecycle-neutral cohort progress rowset."""
        mutations = [
            typing.cast("ClosedCohortProgress", plan.mutation) for plan in plans
        ]
        feed_ids = [plan.feed_id for plan in plans]
        paths = [mutation.last_processed_filename for mutation in mutations]
        cursors = [mutation.cursor for mutation in mutations]
        write_cursors = [plan.write_cursor for plan in plans]
        write_paths = [plan.write_path for plan in plans]
        caller_ordinals = [plan.caller_ordinal for plan in plans]
        _require_parallel_cardinality(
            "closed cohort progress",
            feed_ids,
            paths,
            cursors,
            write_cursors,
            write_paths,
            caller_ordinals,
        )
        return await connection.fetch(
            ingestion_lease_queries.APPLY_CLOSED_COHORT_PROGRESS_SQL,
            feed_ids,
            paths,
            cursors,
            write_cursors,
            write_paths,
            caller_ordinals,
        )

    async def _apply_feed_failures(
        self,
        connection: asyncpg.Connection,
        plans: tuple[_PlannedChildMutation, ...],
    ) -> collections.abc.Sequence[collections.abc.Mapping]:
        """Apply one static one-shot Feed failure rowset."""
        mutations = [
            typing.cast("FeedFailureTransition", plan.mutation)
            for plan in plans
        ]
        actions = [mutation.action for mutation in mutations]
        feed_ids = [plan.feed_id for plan in plans]
        cursors = [mutation.completion_cursor for mutation in mutations]
        write_cursors = [plan.write_cursor for plan in plans]
        is_budgeted = [
            isinstance(action, BudgetedFailure) for action in actions
        ]
        thresholds = [
            action.failure_threshold
            if isinstance(action, BudgetedFailure)
            else feed_lifecycle.DEFAULT_FAILURE_THRESHOLD
            for action in actions
        ]
        backoff_maxima = [
            action.backoff_max_sec
            if isinstance(action, BudgetedFailure)
            else feed_lifecycle.DEFAULT_BACKOFF_MAX_SEC
            for action in actions
        ]
        backoff_bases = [
            action.backoff_base_sec
            if isinstance(action, BudgetedFailure)
            else feed_lifecycle.DEFAULT_BACKOFF_BASE_SEC
            for action in actions
        ]
        retry_times = [
            action.retry_after
            if isinstance(action, NonBudgetedFailure)
            else None
            for action in actions
        ]
        status_reasons = [
            mutation.status_reason.value for mutation in mutations
        ]
        reason_details = [
            _require_reason_detail(mutation.reason) for mutation in mutations
        ]
        caller_ordinals = [plan.caller_ordinal for plan in plans]
        _require_parallel_cardinality(
            "Feed failure",
            feed_ids,
            cursors,
            write_cursors,
            is_budgeted,
            thresholds,
            backoff_maxima,
            backoff_bases,
            retry_times,
            status_reasons,
            reason_details,
            caller_ordinals,
        )
        return await connection.fetch(
            ingestion_lease_queries.APPLY_FEED_FAILURES_SQL,
            feed_ids,
            cursors,
            write_cursors,
            is_budgeted,
            thresholds,
            backoff_maxima,
            backoff_bases,
            retry_times,
            status_reasons,
            reason_details,
            caller_ordinals,
        )

    async def _apply_lease_effect(
        self,
        connection: asyncpg.Connection,
        grant: LeaseGrant,
        effect: LeaseEffect,
        before_row: collections.abc.Mapping,
    ) -> bool:
        """Apply success-proven Lease recovery under the held exact grant."""
        if isinstance(effect, NoLeaseEffect):
            return False
        if not _lifecycle_dirty_from_row(before_row):
            return False

        before_revision = _membership_revision_from_row(before_row)

        row = await connection.fetchrow(
            ingestion_lease_queries.FINALIZE_LEASE_RECOVERY_SQL,
            grant.source_type.value,
            grant.lease_key,
            grant.owner_worker_id,
            grant.fencing_token,
        )
        if row is None:
            msg = "Lease recovery did not return the still-locked exact grant"
            raise ValueError(msg)
        if (
            _source_type_from_row(row) is not grant.source_type
            or row["lease_key"] != grant.lease_key
            or row["worker_id"] != grant.owner_worker_id
            or row["fencing_token"] != grant.fencing_token
        ):
            msg = "Lease recovery returned a different grant"
            raise ValueError(msg)
        if (
            _status_from_row(row) is not feed_store.FeedStatus.ACTIVE
            or _lifecycle_dirty_from_row(row)
            or _membership_revision_from_row(row) != before_revision
        ):
            msg = "Lease recovery returned inconsistent after state"
            raise ValueError(msg)
        return True

    async def _write_child_audits(
        self,
        connection: asyncpg.Connection,
        plans: tuple[_PlannedChildMutation, ...],
        updated_by_ordinal: dict[int, collections.abc.Mapping],
        actor_id: str,
    ) -> dict[int, object]:
        """Insert actual child audit candidates as one canonical rowset."""
        candidates: list[_PlannedChildMutation] = []
        for plan in plans:
            if not plan.needs_update:
                continue
            after_row = updated_by_ordinal.get(plan.caller_ordinal)
            if after_row is None:
                msg = "updated child lacks returned after state"
                raise ValueError(msg)
            confirmed_action = _confirmed_child_audit_action(plan, after_row)
            if confirmed_action != plan.audit_action:
                msg = "child DML returned an inconsistent audit transition"
                raise ValueError(msg)
            if confirmed_action is not None:
                candidates.append(
                    dataclasses.replace(plan, audit_action=confirmed_action)
                )
        if not candidates:
            return {}

        sorted_feed_ids = sorted(
            (plan.feed_id for plan in candidates),
            key=lambda feed_id: feed_id.int,
        )
        property_rows = await connection.fetch(
            ingestion_lease_queries.LOAD_CHILD_AUDIT_PROPERTIES_SQL,
            sorted_feed_ids,
        )
        properties_by_id = _audit_properties_by_id(
            set(sorted_feed_ids),
            property_rows,
        )
        rowset = _build_child_audit_rowset(
            tuple(candidates),
            updated_by_ordinal,
            properties_by_id,
            actor_id,
        )
        audit_rows = await connection.fetch(
            ingestion_lease_queries.INSERT_CHILD_AUDIT_EVENTS_SQL,
            rowset.feed_ids,
            rowset.actions,
            rowset.actors,
            rowset.revisions,
            rowset.before_values,
            rowset.after_values,
            rowset.caller_ordinals,
        )
        payloads: dict[int, object] = {}
        expected_ordinals = set(rowset.caller_ordinals)
        for row in audit_rows:
            caller_ordinal = row["caller_ordinal"]
            if (
                not isinstance(caller_ordinal, int)
                or caller_ordinal not in expected_ordinals
                or caller_ordinal in payloads
                or row["feed_audit_event"] is None
            ):
                msg = "Feed audit insert returned an unexpected row"
                raise ValueError(msg)
            payloads[caller_ordinal] = row["feed_audit_event"]
        if set(payloads) != expected_ordinals:
            msg = "Feed audit insert did not return every candidate"
            raise ValueError(msg)
        return payloads
