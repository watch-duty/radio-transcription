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
    """Complete immutable authority for one Lease ownership generation."""

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
class LeaseSnapshot:
    """Mutable Lease state observed separately from grant identity."""

    status: feed_store.FeedStatus
    last_heartbeat: datetime.datetime | None
    failure_count: int
    retry_after: datetime.datetime | None
    status_reason: feed_store.FeedStatusReason | None
    status_reason_detail: str | None
    status_reason_updated_at: datetime.datetime | None
    audit_revision: int
    membership_revision: int
    updated_at: datetime.datetime


@dataclasses.dataclass(frozen=True, slots=True)
class LeaseClaim:
    """A newly established grant and its mutable state snapshot."""

    grant: LeaseGrant
    snapshot: LeaseSnapshot


class LeaseOperationDisposition(enum.StrEnum):
    """Closed outcome vocabulary for exact-grant control operations."""

    APPLIED = "applied"
    ACCEPTED_NOOP = "accepted_noop"
    MISSING = "missing"
    OWNER_MISMATCH = "owner_mismatch"
    FENCE_MISMATCH = "fence_mismatch"
    STATUS_INELIGIBLE = "status_ineligible"


@dataclasses.dataclass(frozen=True, slots=True)
class LeaseOperationResult:
    """Diagnostic result for one exact-grant Lease mutation."""

    disposition: LeaseOperationDisposition
    snapshot: LeaseSnapshot | None


@dataclasses.dataclass(frozen=True, slots=True)
class LeaseHeartbeatResult:
    """Caller-correlated diagnostic result for heartbeat renewal."""

    grant: LeaseGrant
    disposition: LeaseOperationDisposition
    snapshot: LeaseSnapshot | None


class LeaseReleaseCause(enum.StrEnum):
    """Structured telemetry causes sharing one neutral release policy."""

    NORMAL = "normal"
    SHUTDOWN = "shutdown"
    REBALANCE = "rebalance"
    CANCELLATION = "cancellation"
    ABANDONMENT = "abandonment"


class GrantRejectionReason(enum.StrEnum):
    """Closed reasons an exact active Lease grant can be rejected."""

    MISSING = "missing"
    OWNER_MISMATCH = "owner_mismatch"
    FENCE_MISMATCH = "fence_mismatch"
    STATUS_INELIGIBLE = "status_ineligible"


@dataclasses.dataclass(frozen=True, slots=True)
class GrantRejected:
    """Shared exact-grant rejection with current locked state."""

    reason: GrantRejectionReason
    snapshot: LeaseSnapshot | None

    def __post_init__(self) -> None:
        missing = self.reason is GrantRejectionReason.MISSING
        if missing != (self.snapshot is None):
            msg = "only a missing Lease may have no rejection snapshot"
            raise ValueError(msg)


@dataclasses.dataclass(frozen=True, slots=True)
class BudgetedFailure:
    """Closed action for a failure that consumes the Lease budget."""

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
    """Closed action for retryable failure outside the Lease budget."""

    retry_after: datetime.datetime

    def __post_init__(self) -> None:
        if not isinstance(self.retry_after, datetime.datetime):
            msg = "retry_after must be a datetime"
            raise TypeError(msg)
        if self.retry_after.utcoffset() != datetime.timedelta(0):
            msg = "retry_after must be UTC-aware"
            raise ValueError(msg)


type LeaseFailureAction = BudgetedFailure | NonBudgetedFailure


class LeaseFailureEffect(enum.StrEnum):
    """Durable lifecycle effect of finalized Lease failure."""

    NONE = "none"
    FAILURE_RECORDED = "failure_recorded"
    QUARANTINED = "quarantined"


@dataclasses.dataclass(frozen=True, slots=True)
class LeaseFailureResult:
    """Before/after evidence for one finalized failure attempt."""

    disposition: LeaseOperationDisposition
    effect: LeaseFailureEffect
    before_snapshot: LeaseSnapshot | None
    after_snapshot: LeaseSnapshot | None

    def __post_init__(self) -> None:
        missing = self.disposition is LeaseOperationDisposition.MISSING
        if missing and (
            self.before_snapshot is not None or self.after_snapshot is not None
        ):
            msg = "failure snapshots may be absent only for a missing Lease"
            raise ValueError(msg)
        if not missing and (
            self.before_snapshot is None or self.after_snapshot is None
        ):
            msg = "present Lease failures require before and after snapshots"
            raise ValueError(msg)
        if (
            self.disposition is not LeaseOperationDisposition.APPLIED
            and self.effect is not LeaseFailureEffect.NONE
        ):
            msg = "a non-applied failure cannot report a lifecycle effect"
            raise ValueError(msg)


@dataclasses.dataclass(frozen=True, slots=True)
class LeaseMemberIdentity:
    """Immutable Feed/source/SID/group binding from membership loading."""

    feed_id: uuid.UUID
    source_type: feed_store.SourceType
    source_feed_id: str
    sid: str
    group_id: str


@dataclasses.dataclass(frozen=True, slots=True)
class AdmittedAudioProgress:
    """Successful progress for work admitted under the current grant."""

    member: LeaseMemberIdentity
    last_processed_filename: str
    cursor: datetime.datetime | None


@dataclasses.dataclass(frozen=True, slots=True)
class SourceObservation:
    """Successful source boundary observation for one immutable member."""

    member: LeaseMemberIdentity
    cursor: datetime.datetime | None


@dataclasses.dataclass(frozen=True, slots=True)
class FeedFailureTransition:
    """Caller-classified failure at an optional completed source boundary.

    A command without a completion cursor is a standalone one-shot mutation;
    callers must not retry it after an outcome-unknown transaction attempt.
    """

    member: LeaseMemberIdentity
    action: LeaseFailureAction
    status_reason: feed_store.FeedStatusReason
    reason: str | None
    completion_cursor: datetime.datetime | None


type ChildMutation = (
    AdmittedAudioProgress | SourceObservation | FeedFailureTransition
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
    """Closed immutable child commands and their parent Lease effect."""

    mutations: tuple[ChildMutation, ...]
    lease_effect: LeaseEffect


class ChildDisposition(enum.StrEnum):
    """Selective disposition of one requested child mutation."""

    APPLIED = "applied"
    APPLIED_AFTER_DEACTIVATION = "applied_after_deactivation"
    ACCEPTED_NOOP = "accepted_noop"
    MISSING = "missing"
    STATUS_INELIGIBLE = "status_ineligible"


class CursorEffect(enum.StrEnum):
    """Relationship between a requested and durable Feed cursor."""

    INITIALIZED = "initialized"
    ADVANCED = "advanced"
    EQUAL = "equal"
    REGRESSIVE = "regressive"
    ABSENT = "absent"


class LifecycleEffect(enum.StrEnum):
    """Independent Feed lifecycle effect of a child mutation."""

    NONE = "none"
    RECOVERED = "recovered"
    CLEARED_WHILE_DEACTIVATED = "cleared_while_deactivated"
    FAILURE_RECORDED = "failure_recorded"
    QUARANTINED = "quarantined"


class LeaseLifecycleEffect(enum.StrEnum):
    """Lifecycle effect applied to the still-owned parent Lease."""

    NONE = "none"
    RECOVERED = "recovered"


@dataclasses.dataclass(frozen=True, slots=True)
class ChildMutationResult:
    """Caller-correlated selective result for one Feed command."""

    feed_id: uuid.UUID
    disposition: ChildDisposition
    cursor_effect: CursorEffect
    lifecycle_effect: LifecycleEffect


@dataclasses.dataclass(frozen=True, slots=True)
class LeaseLifecycleResult:
    """Before/after evidence for the parent Lease effect."""

    effect: LeaseLifecycleEffect
    before_snapshot: LeaseSnapshot
    after_snapshot: LeaseSnapshot


@dataclasses.dataclass(frozen=True, slots=True)
class BatchCommitted:
    """Committed parent effect and caller-ordered child results."""

    lease_effect: LeaseLifecycleResult
    children: tuple[ChildMutationResult, ...]


@dataclasses.dataclass(frozen=True, slots=True)
class LeaseMember:
    """Eligible child Feed state attached to an immutable identity."""

    identity: LeaseMemberIdentity
    status: feed_store.FeedStatus
    last_processed_filename: str | None
    last_bookmark_time: datetime.datetime | None
    failure_count: int
    retry_after: datetime.datetime | None
    status_reason: feed_store.FeedStatusReason | None
    status_reason_detail: str | None
    audit_revision: int


@dataclasses.dataclass(frozen=True, slots=True)
class MembershipSnapshot:
    """Authoritative eligible membership for one exact active grant."""

    grant: LeaseGrant
    membership_revision: int
    members: tuple[LeaseMember, ...]
    excluded_count: int


class MembershipInvariantReason(enum.StrEnum):
    """Closed reasons an authoritative membership load can fail closed."""

    EMPTY = "empty"
    MISSING_IDENTITY = "missing_identity"
    SOURCE_MISMATCH = "source_mismatch"
    NO_ELIGIBLE_MEMBERS = "no_eligible_members"


@dataclasses.dataclass(frozen=True, slots=True)
class MembershipInvariantViolation:
    """Fail-closed result for structurally invalid Lease membership."""

    grant: LeaseGrant
    reason: MembershipInvariantReason
    detail: str


@dataclasses.dataclass(frozen=True, slots=True)
class _PlannedChildMutation:
    """Private, database-derived execution plan for one child command."""

    caller_ordinal: int
    mutation: ChildMutation
    before_row: collections.abc.Mapping | None
    disposition: ChildDisposition
    cursor_effect: CursorEffect
    lifecycle_effect: LifecycleEffect
    write_cursor: bool = False
    write_path: bool = False
    clear_lifecycle: bool = False
    charge_failure: bool = False
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
            or self.charge_failure
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


def _require_status_reason(value: object) -> feed_store.FeedStatusReason:
    if not isinstance(value, feed_store.FeedStatusReason):
        msg = "status_reason must be a FeedStatusReason"
        raise TypeError(msg)
    return value


def _require_failure_action(value: object) -> LeaseFailureAction:
    if type(value) not in (BudgetedFailure, NonBudgetedFailure):
        msg = "action must be BudgetedFailure or NonBudgetedFailure"
        raise TypeError(msg)
    return typing.cast("LeaseFailureAction", value)


def _require_reason_detail(value: object) -> str | None:
    if value is not None and not isinstance(value, str):
        msg = "reason must be a string or None"
        raise TypeError(msg)
    return feed_lifecycle.status_reason_detail_storage_value(value)


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


def _require_member_binding(
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
    failure_commands: list[FeedFailureTransition] = []
    for mutation in batch.mutations:
        if type(mutation) not in (
            AdmittedAudioProgress,
            SourceObservation,
            FeedFailureTransition,
        ):
            msg = f"unsupported child mutation {type(mutation).__name__}"
            raise TypeError(msg)
        member = _require_member_binding(grant, mutation.member)
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


def _cursor_effect(
    current: datetime.datetime | None,
    requested: datetime.datetime | None,
) -> CursorEffect:
    if requested is None:
        return CursorEffect.ABSENT
    if current is None:
        return CursorEffect.INITIALIZED
    if requested > current:
        return CursorEffect.ADVANCED
    if requested == current:
        return CursorEffect.EQUAL
    return CursorEffect.REGRESSIVE


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
    lifecycle_effect: LifecycleEffect,
) -> str | None:
    """Apply the canonical Feed failure/quarantine event rules."""
    if lifecycle_effect is LifecycleEffect.QUARANTINED:
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


def _snapshot_from_row(
    row: collections.abc.Mapping,
    prefix: str = "",
) -> LeaseSnapshot:
    return LeaseSnapshot(
        status=_status_from_row(row, prefix),
        last_heartbeat=row[f"{prefix}last_heartbeat"],
        failure_count=row[f"{prefix}failure_count"],
        retry_after=row[f"{prefix}retry_after"],
        status_reason=_status_reason_from_row(row, prefix),
        status_reason_detail=row[f"{prefix}status_reason_detail"],
        status_reason_updated_at=row[f"{prefix}status_reason_updated_at"],
        audit_revision=row[f"{prefix}audit_revision"],
        membership_revision=row[f"{prefix}membership_revision"],
        updated_at=row[f"{prefix}updated_at"],
    )


def _lease_has_dirty_lifecycle(snapshot: LeaseSnapshot) -> bool:
    """Whether a successful finalized boundary has Lease evidence to clear."""
    return (
        snapshot.failure_count != 0
        or snapshot.retry_after is not None
        or snapshot.status_reason is not None
        or snapshot.status_reason_detail is not None
        or snapshot.status_reason_updated_at is not None
    )


def _claim_from_row(row: collections.abc.Mapping) -> LeaseClaim:
    grant = LeaseGrant(
        source_type=_source_type_from_row(row),
        lease_key=row["lease_key"],
        owner_worker_id=row["worker_id"],
        fencing_token=row["fencing_token"],
    )
    return LeaseClaim(grant=grant, snapshot=_snapshot_from_row(row))


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
        return MembershipInvariantViolation(
            grant,
            MembershipInvariantReason.MISSING_IDENTITY,
            "membership row has a missing or inconsistent immutable identity",
        )

    property_source_raw = row["property_source_type"]
    feed_source_raw = row["feed_source_type"]
    if not isinstance(property_source_raw, str) or not isinstance(
        feed_source_raw,
        str,
    ):
        return MembershipInvariantViolation(
            grant,
            MembershipInvariantReason.MISSING_IDENTITY,
            "membership row is missing a joined source identity",
        )
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
        return MembershipInvariantViolation(
            grant,
            MembershipInvariantReason.SOURCE_MISMATCH,
            "Feed and property source bindings do not match the Lease",
        )
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
    return LeaseMember(
        identity=identity,
        status=_status_from_row(row),
        last_processed_filename=row["last_processed_filename"],
        last_bookmark_time=row["last_bookmark_time"],
        failure_count=row["failure_count"],
        retry_after=row["retry_after"],
        status_reason=_status_reason_from_row(row),
        status_reason_detail=row["status_reason_detail"],
        audit_revision=row["audit_revision"],
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
            disposition=ChildDisposition.MISSING,
            cursor_effect=CursorEffect.ABSENT,
            lifecycle_effect=LifecycleEffect.NONE,
        )

    status = _status_from_row(before_row)
    source_type = _child_source_type_from_row(before_row)
    cursor_effect = _cursor_effect(
        before_row["last_bookmark_time"],
        _mutation_cursor(mutation),
    )
    if source_type is not mutation.member.source_type:
        return _PlannedChildMutation(
            caller_ordinal=caller_ordinal,
            mutation=mutation,
            before_row=before_row,
            disposition=ChildDisposition.STATUS_INELIGIBLE,
            cursor_effect=cursor_effect,
            lifecycle_effect=LifecycleEffect.NONE,
        )

    is_progress = isinstance(mutation, AdmittedAudioProgress)
    is_failure = isinstance(mutation, FeedFailureTransition)
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
            disposition=ChildDisposition.STATUS_INELIGIBLE,
            cursor_effect=cursor_effect,
            lifecycle_effect=LifecycleEffect.NONE,
        )

    write_cursor = cursor_effect in (
        CursorEffect.INITIALIZED,
        CursorEffect.ADVANCED,
    )
    if is_failure:
        failure = typing.cast("FeedFailureTransition", mutation)
        charge_failure = failure.completion_cursor is None or write_cursor
        if not charge_failure:
            return _PlannedChildMutation(
                caller_ordinal=caller_ordinal,
                mutation=mutation,
                before_row=before_row,
                disposition=ChildDisposition.ACCEPTED_NOOP,
                cursor_effect=cursor_effect,
                lifecycle_effect=LifecycleEffect.NONE,
            )
        lifecycle_effect = LifecycleEffect.FAILURE_RECORDED
        if isinstance(failure.action, BudgetedFailure) and (
            before_row["failure_count"] + 1 >= failure.action.failure_threshold
        ):
            lifecycle_effect = LifecycleEffect.QUARANTINED
        return _PlannedChildMutation(
            caller_ordinal=caller_ordinal,
            mutation=mutation,
            before_row=before_row,
            disposition=ChildDisposition.APPLIED,
            cursor_effect=cursor_effect,
            lifecycle_effect=lifecycle_effect,
            write_cursor=write_cursor,
            charge_failure=True,
            audit_action=_select_failure_audit_action(
                before_row,
                failure,
                lifecycle_effect,
            ),
        )

    clear_lifecycle = _has_dirty_child_lifecycle(status, before_row)
    lifecycle_effect = LifecycleEffect.NONE
    if clear_lifecycle:
        lifecycle_effect = (
            LifecycleEffect.CLEARED_WHILE_DEACTIVATED
            if status is feed_store.FeedStatus.DEACTIVATED
            else LifecycleEffect.RECOVERED
        )

    write_path = False
    if isinstance(mutation, AdmittedAudioProgress):
        write_path = write_cursor or (
            mutation.cursor is None
            and before_row["last_processed_filename"]
            != mutation.last_processed_filename
        )

    needs_update = write_cursor or write_path or clear_lifecycle
    disposition = (
        ChildDisposition.APPLIED
        if needs_update
        else ChildDisposition.ACCEPTED_NOOP
    )
    if status is feed_store.FeedStatus.DEACTIVATED:
        disposition = ChildDisposition.APPLIED_AFTER_DEACTIVATION
    audit_action = None
    if lifecycle_effect is LifecycleEffect.RECOVERED:
        audit_action = _select_recovery_audit_action(status, before_row)
    return _PlannedChildMutation(
        caller_ordinal=caller_ordinal,
        mutation=mutation,
        before_row=before_row,
        disposition=disposition,
        cursor_effect=cursor_effect,
        lifecycle_effect=lifecycle_effect,
        write_cursor=write_cursor,
        write_path=write_path,
        clear_lifecycle=clear_lifecycle,
        audit_action=audit_action,
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


class IngestionLeaseStore:
    """Storage facade for complete-grant Lease control operations."""

    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    def _grant_rejection(
        self,
        grant: LeaseGrant,
        row: collections.abc.Mapping | None,
    ) -> GrantRejected | None:
        """Classify a locked Lease row against a complete active grant."""
        _require_grant(grant)
        if row is None:
            return GrantRejected(GrantRejectionReason.MISSING, None)

        source_type = _source_type_from_row(row)
        if source_type is not grant.source_type or row["lease_key"] != (
            grant.lease_key
        ):
            msg = "locked Lease identity did not match the requested identity"
            raise ValueError(msg)

        snapshot = _snapshot_from_row(row)
        if snapshot.status is not feed_store.FeedStatus.ACTIVE:
            return GrantRejected(
                GrantRejectionReason.STATUS_INELIGIBLE,
                snapshot,
            )
        if row["worker_id"] != grant.owner_worker_id:
            return GrantRejected(
                GrantRejectionReason.OWNER_MISMATCH,
                snapshot,
            )
        if row["fencing_token"] != grant.fencing_token:
            return GrantRejected(
                GrantRejectionReason.FENCE_MISMATCH,
                snapshot,
            )
        return None

    async def claim_unclaimed(
        self,
        source_type: feed_store.SourceType,
        owner_worker_id: uuid.UUID,
        limit: int,
    ) -> tuple[LeaseClaim, ...]:
        """Claim deterministic unclaimed Leases as new generations."""
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
        """Claim due failing Leases before stale active Leases."""
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
        """Renew exact active grants and preserve caller result order."""
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
                None,
            )
        if row["applied"]:
            return LeaseHeartbeatResult(
                grant,
                LeaseOperationDisposition.APPLIED,
                _snapshot_from_row(row),
            )
        rejection = self._grant_rejection(grant, row)
        if rejection is None:
            return LeaseHeartbeatResult(
                grant,
                LeaseOperationDisposition.ACCEPTED_NOOP,
                _snapshot_from_row(row),
            )
        return LeaseHeartbeatResult(
            grant,
            _disposition_for_rejection(rejection.reason),
            rejection.snapshot,
        )

    async def release(
        self,
        grant: LeaseGrant,
        cause: LeaseReleaseCause = LeaseReleaseCause.NORMAL,
    ) -> LeaseOperationResult:
        """Neutrally release one exact active grant."""
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
            return LeaseOperationResult(
                LeaseOperationDisposition.MISSING,
                None,
            )
        if row["applied"]:
            snapshot = _snapshot_from_row(row)
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
            return LeaseOperationResult(
                LeaseOperationDisposition.APPLIED,
                snapshot,
            )

        rejection = self._grant_rejection(grant, row)
        if rejection is None:
            return LeaseOperationResult(
                LeaseOperationDisposition.ACCEPTED_NOOP,
                _snapshot_from_row(row),
            )
        return LeaseOperationResult(
            _disposition_for_rejection(rejection.reason),
            rejection.snapshot,
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
        """Finalize one exhausted Lease failure through the exact grant."""
        grant = _require_grant(grant)
        action = _require_failure_action(action)
        status_reason = _require_status_reason(status_reason)
        actor_id = _require_actor_id(actor_id)
        detail = _require_reason_detail(reason)

        if isinstance(action, BudgetedFailure):
            query = ingestion_lease_queries.FINALIZE_BUDGETED_FAILURE_SQL
            parameters = (
                grant.source_type.value,
                grant.lease_key,
                grant.owner_worker_id,
                grant.fencing_token,
                action.failure_threshold,
                action.backoff_max_sec,
                action.backoff_base_sec,
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
                    "failure_effect": result.effect.value,
                },
            )
        return result

    def _failure_result(
        self,
        grant: LeaseGrant,
        row: collections.abc.Mapping | None,
    ) -> LeaseFailureResult:
        if row is None:
            return LeaseFailureResult(
                LeaseOperationDisposition.MISSING,
                LeaseFailureEffect.NONE,
                None,
                None,
            )

        before_snapshot = _snapshot_from_row(row)
        if not row["applied"]:
            rejection = self._grant_rejection(grant, row)
            disposition = (
                LeaseOperationDisposition.ACCEPTED_NOOP
                if rejection is None
                else _disposition_for_rejection(rejection.reason)
            )
            return LeaseFailureResult(
                disposition,
                LeaseFailureEffect.NONE,
                before_snapshot,
                before_snapshot,
            )

        after_snapshot = _snapshot_from_row(row, "after_")
        effect = LeaseFailureEffect.FAILURE_RECORDED
        if after_snapshot.status is feed_store.FeedStatus.QUARANTINED:
            effect = LeaseFailureEffect.QUARANTINED
        return LeaseFailureResult(
            LeaseOperationDisposition.APPLIED,
            effect,
            before_snapshot,
            after_snapshot,
        )

    async def load_membership(
        self,
        grant: LeaseGrant,
    ) -> GrantRejected | MembershipSnapshot | MembershipInvariantViolation:
        """Load one fail-closed authoritative Calls membership snapshot.

        Controlled administrative SQL owns immutable identity maintenance and
        revision increments. The revision returned here invalidates caches; it
        never participates in grant or lifecycle authorization.
        """
        grant = _require_grant(grant)
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

                snapshot = _snapshot_from_row(lease_row)
                rows = await connection.fetch(
                    ingestion_lease_queries.LOAD_BCFY_CALLS_MEMBERSHIP_SQL,
                    grant.lease_key,
                )
                return self._membership_result(
                    grant,
                    snapshot.membership_revision,
                    rows,
                )

    def _membership_result(
        self,
        grant: LeaseGrant,
        membership_revision: int,
        rows: collections.abc.Sequence[collections.abc.Mapping],
    ) -> MembershipSnapshot | MembershipInvariantViolation:
        if not rows:
            return MembershipInvariantViolation(
                grant,
                MembershipInvariantReason.EMPTY,
                "owned Lease has no structurally valid membership rows",
            )

        members: list[LeaseMember] = []
        excluded_count = 0
        for row in rows:
            identity = _membership_identity_from_row(grant, row)
            if isinstance(identity, MembershipInvariantViolation):
                return identity
            member = _member_from_row(identity, row)
            if member.status in (
                feed_store.FeedStatus.ACTIVE,
                feed_store.FeedStatus.FAILING,
            ):
                members.append(member)
            else:
                excluded_count += 1

        if not members:
            return MembershipInvariantViolation(
                grant,
                MembershipInvariantReason.NO_ELIGIBLE_MEMBERS,
                "owned Lease has no active or failing member",
            )
        return MembershipSnapshot(
            grant=grant,
            membership_revision=membership_revision,
            members=tuple(members),
            excluded_count=excluded_count,
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
                lease_before = _snapshot_from_row(lease_row)

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
                if failure_updates:
                    rows = await self._apply_feed_failures(
                        connection,
                        failure_updates,
                    )
                    updated_by_ordinal.update(
                        _updated_children_by_ordinal(failure_updates, rows)
                    )

                lease_lifecycle = await self._apply_lease_effect(
                    connection,
                    grant,
                    batch.lease_effect,
                    lease_before,
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
                        cursor_effect=plan.cursor_effect,
                        lifecycle_effect=plan.lifecycle_effect,
                    )
                    for plan in plans
                )
                committed = BatchCommitted(
                    lease_effect=lease_lifecycle,
                    children=children,
                )

        for caller_ordinal in sorted(notification_payloads):
            feed_change_notifications.emit_feed_change_notification(
                notification_payloads[caller_ordinal]
            )
        if committed.lease_effect.effect is LeaseLifecycleEffect.RECOVERED:
            logger.info(
                "Ingestion Lease recovered after finalized child boundary",
                extra={
                    "source_type": grant.source_type.value,
                    "lease_key": grant.lease_key,
                    "owner_worker_id": str(grant.owner_worker_id),
                    "fencing_token": grant.fencing_token,
                    "before_audit_revision": (
                        committed.lease_effect.before_snapshot.audit_revision
                    ),
                    "after_audit_revision": (
                        committed.lease_effect.after_snapshot.audit_revision
                    ),
                },
            )
        return committed

    async def _apply_admitted_progress(
        self,
        connection: asyncpg.Connection,
        plans: tuple[_PlannedChildMutation, ...],
    ) -> collections.abc.Sequence[collections.abc.Mapping]:
        """Apply one static admitted-progress rowset."""
        feed_ids = [plan.feed_id for plan in plans]
        paths = [
            typing.cast(
                "AdmittedAudioProgress",
                plan.mutation,
            ).last_processed_filename
            for plan in plans
        ]
        cursors = [plan.mutation.cursor for plan in plans]
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
        feed_ids = [plan.feed_id for plan in plans]
        cursors = [plan.mutation.cursor for plan in plans]
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

    async def _apply_feed_failures(
        self,
        connection: asyncpg.Connection,
        plans: tuple[_PlannedChildMutation, ...],
    ) -> collections.abc.Sequence[collections.abc.Mapping]:
        """Apply one static cursor-idempotent Feed failure rowset."""
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
        before_snapshot: LeaseSnapshot,
    ) -> LeaseLifecycleResult:
        """Apply success-proven Lease recovery under the held exact grant."""
        if isinstance(effect, NoLeaseEffect) or not _lease_has_dirty_lifecycle(
            before_snapshot
        ):
            return LeaseLifecycleResult(
                LeaseLifecycleEffect.NONE,
                before_snapshot,
                before_snapshot,
            )

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
        after_snapshot = _snapshot_from_row(row)
        if (
            after_snapshot.status is not feed_store.FeedStatus.ACTIVE
            or after_snapshot.last_heartbeat != before_snapshot.last_heartbeat
            or after_snapshot.failure_count != 0
            or after_snapshot.retry_after is not None
            or after_snapshot.status_reason is not None
            or after_snapshot.status_reason_detail is not None
            or after_snapshot.status_reason_updated_at is not None
            or after_snapshot.audit_revision
            != before_snapshot.audit_revision + 1
            or after_snapshot.membership_revision
            != before_snapshot.membership_revision
        ):
            msg = "Lease recovery returned inconsistent after state"
            raise ValueError(msg)
        return LeaseLifecycleResult(
            LeaseLifecycleEffect.RECOVERED,
            before_snapshot,
            after_snapshot,
        )

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
