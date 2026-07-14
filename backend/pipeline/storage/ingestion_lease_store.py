"""Typed storage boundary for durable fenced ingestion Leases."""

from __future__ import annotations

import dataclasses
import datetime
import enum
import logging
import typing
import uuid

from backend.pipeline.storage import (
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


def _source_type_from_row(
    row: collections.abc.Mapping,
) -> feed_store.SourceType:
    value = row["source_type"]
    try:
        return feed_store.SourceType(value)
    except ValueError as error:
        msg = f"Unknown Lease source type {value!r}"
        raise ValueError(msg) from error


def _status_from_row(row: collections.abc.Mapping) -> feed_store.FeedStatus:
    value = row["status"]
    try:
        return feed_store.FeedStatus(value)
    except ValueError as error:
        msg = f"Unknown Lease status {value!r}"
        raise ValueError(msg) from error


def _status_reason_from_row(
    row: collections.abc.Mapping,
) -> feed_store.FeedStatusReason | None:
    value = row["status_reason"]
    if value is None:
        return None
    try:
        return feed_store.FeedStatusReason(value)
    except ValueError as error:
        msg = f"Unknown Lease status reason {value!r}"
        raise ValueError(msg) from error


def _snapshot_from_row(row: collections.abc.Mapping) -> LeaseSnapshot:
    return LeaseSnapshot(
        status=_status_from_row(row),
        last_heartbeat=row["last_heartbeat"],
        failure_count=row["failure_count"],
        retry_after=row["retry_after"],
        status_reason=_status_reason_from_row(row),
        status_reason_detail=row["status_reason_detail"],
        status_reason_updated_at=row["status_reason_updated_at"],
        audit_revision=row["audit_revision"],
        membership_revision=row["membership_revision"],
        updated_at=row["updated_at"],
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
