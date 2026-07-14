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
class LeaseSnapshot:
    """Failure-policy state observed separately from grant identity.

    Attributes:
        status: Current durable lifecycle state.
        failure_count: Retained count used by budgeted failure policy.
        status_reason: Structured reason for the current lifecycle state.
    """

    status: feed_store.FeedStatus
    failure_count: int
    status_reason: feed_store.FeedStatusReason | None


@dataclasses.dataclass(frozen=True, slots=True)
class LeaseClaim:
    """A newly established grant and its mutable state snapshot.

    Attributes:
        grant: Complete authority for the newly claimed generation.
        snapshot: Mutable Lease state returned by the same claim write.
    """

    grant: LeaseGrant
    snapshot: LeaseSnapshot


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
    """Diagnostic result for one exact-grant Lease mutation.

    Attributes:
        disposition: Closed classification of the mutation attempt.
        snapshot: Current state, absent only when the Lease is missing.
    """

    disposition: LeaseOperationDisposition
    snapshot: LeaseSnapshot | None


@dataclasses.dataclass(frozen=True, slots=True)
class LeaseHeartbeatResult:
    """Caller-correlated outcome for heartbeat renewal.

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
    """Shared exact-grant rejection with current locked state.

    Attributes:
        reason: Exact reason the complete grant was rejected.
        snapshot: Locked current state, absent only for a missing Lease.
    """

    reason: GrantRejectionReason
    snapshot: LeaseSnapshot | None

    def __post_init__(self) -> None:
        missing = self.reason is GrantRejectionReason.MISSING
        if missing != (self.snapshot is None):
            msg = "only a missing Lease may have no rejection snapshot"
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
class LeaseMember:
    """Eligible child Feed state attached to an immutable identity.

    Attributes:
        identity: Immutable routing identity loaded with this state.
        status: Current Feed lifecycle state.
        last_bookmark_time: Durable Feed progress cursor.
        failure_count: Retained Feed failure count.
        retry_after: Earliest time a failing Feed may retry.
        status_reason: Canonical Feed lifecycle reason.
        status_reason_detail: Bounded operator-facing reason detail.
    """

    identity: LeaseMemberIdentity
    status: feed_store.FeedStatus
    last_bookmark_time: datetime.datetime | None
    failure_count: int
    retry_after: datetime.datetime | None
    status_reason: feed_store.FeedStatusReason | None
    status_reason_detail: str | None


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
    """Fail-closed result for structurally invalid Lease membership.

    Attributes:
        grant: Complete Lease authority associated with the failed load.
        detail: Bounded diagnostic explanation without row credentials.
    """

    grant: LeaseGrant
    detail: str


type MembershipRefreshResult = (
    MembershipSnapshot
    | MembershipUnchanged
    | GrantRejected
    | MembershipInvariantViolation
)


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
        failure_count=row["failure_count"],
        status_reason=_status_reason_from_row(row),
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
            "membership row is missing a joined source identity",
        )
    try:
        property_source = feed_store.SourceType(property_source_raw)
        feed_source = feed_store.SourceType(feed_source_raw)
    except ValueError:
        return MembershipInvariantViolation(
            grant,
            "membership row contains an unknown source binding",
        )
    if (
        property_source is not grant.source_type
        or feed_source is not property_source
    ):
        return MembershipInvariantViolation(
            grant,
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
        last_bookmark_time=row["last_bookmark_time"],
        failure_count=row["failure_count"],
        retry_after=row["retry_after"],
        status_reason=_status_reason_from_row(row),
        status_reason_detail=row["status_reason_detail"],
    )


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
        snapshot = None if row is None else _snapshot_from_row(row)
        return GrantRejected(reason, snapshot)

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
            msg = "release did not update an exact active Lease grant"
            raise RuntimeError(msg)
        return LeaseOperationResult(
            _disposition_for_rejection(rejection.reason),
            rejection.snapshot,
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

                snapshot = _snapshot_from_row(lease_row)
                current_revision = snapshot.membership_revision
                if current_revision == known_revision:
                    return MembershipUnchanged(grant, current_revision)
                if (
                    known_revision is not None
                    and current_revision < known_revision
                ):
                    return MembershipInvariantViolation(
                        grant,
                        "locked membership revision regressed below the "
                        "authoritative cached revision",
                    )

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
            return MembershipInvariantViolation(
                grant,
                "owned Lease has no structurally valid membership rows",
            )

        members: list[LeaseMember] = []
        routing_keys: set[str] = set()
        for row in rows:
            identity = _membership_identity_from_row(grant, row)
            if isinstance(identity, MembershipInvariantViolation):
                return identity
            if identity.source_feed_id in routing_keys:
                return MembershipInvariantViolation(
                    grant,
                    "membership contains a duplicate canonical routing key",
                )
            routing_keys.add(identity.source_feed_id)
            member = _member_from_row(identity, row)
            if member.status in (
                feed_store.FeedStatus.ACTIVE,
                feed_store.FeedStatus.FAILING,
            ):
                members.append(member)

        if not members:
            return MembershipInvariantViolation(
                grant,
                "owned Lease has no active or failing member",
            )
        return MembershipSnapshot(
            grant=grant,
            membership_revision=membership_revision,
            members=tuple(members),
        )
