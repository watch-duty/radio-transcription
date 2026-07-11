"""Typed storage boundary for durable fenced ingestion Leases."""

from __future__ import annotations

import dataclasses
import datetime
import enum
import logging
import typing
import uuid

from backend.pipeline.storage import (
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
    if not isinstance(value, (BudgetedFailure, NonBudgetedFailure)):
        msg = "action must be BudgetedFailure or NonBudgetedFailure"
        raise TypeError(msg)
    return value


def _require_reason_detail(value: object) -> str | None:
    if value is not None and not isinstance(value, str):
        msg = "reason must be a string or None"
        raise TypeError(msg)
    return feed_lifecycle.status_reason_detail_storage_value(value)


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
