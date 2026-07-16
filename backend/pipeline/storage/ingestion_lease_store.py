"""Typed storage boundary for durable fenced ingestion Leases."""

from __future__ import annotations

import datetime
import logging
import typing
import uuid

from backend.pipeline.storage import (
    _ingestion_lease_child_commit,
    feed_change_notifications,
    feed_store,
    ingestion_lease_contracts,
    ingestion_lease_queries,
)

if typing.TYPE_CHECKING:
    import collections.abc

    import asyncpg


logger = logging.getLogger(__name__)


LeaseGrant = ingestion_lease_contracts.LeaseGrant
LeaseClaim = ingestion_lease_contracts.LeaseClaim
LeaseOperationDisposition = ingestion_lease_contracts.LeaseOperationDisposition
LeaseOperationResult = ingestion_lease_contracts.LeaseOperationResult
LeaseHeartbeatResult = ingestion_lease_contracts.LeaseHeartbeatResult
LeaseReleaseCause = ingestion_lease_contracts.LeaseReleaseCause
GrantRejectionReason = ingestion_lease_contracts.GrantRejectionReason
GrantRejected = ingestion_lease_contracts.GrantRejected
BudgetedFailure = ingestion_lease_contracts.BudgetedFailure
NonBudgetedFailure = ingestion_lease_contracts.NonBudgetedFailure
LeaseFailureAction = ingestion_lease_contracts.LeaseFailureAction
LeaseFailureResult = ingestion_lease_contracts.LeaseFailureResult
LeaseMemberIdentity = ingestion_lease_contracts.LeaseMemberIdentity
AdmittedAudioProgress = ingestion_lease_contracts.AdmittedAudioProgress
SourceObservation = ingestion_lease_contracts.SourceObservation
ClosedCohortProgress = ingestion_lease_contracts.ClosedCohortProgress
FeedFailureTransition = ingestion_lease_contracts.FeedFailureTransition
ChildMutation = ingestion_lease_contracts.ChildMutation
NoLeaseEffect = ingestion_lease_contracts.NoLeaseEffect
FinalizeLeaseRecovery = ingestion_lease_contracts.FinalizeLeaseRecovery
LeaseEffect = ingestion_lease_contracts.LeaseEffect
ChildMutationBatch = ingestion_lease_contracts.ChildMutationBatch
ChildDisposition = ingestion_lease_contracts.ChildDisposition
ChildMutationResult = ingestion_lease_contracts.ChildMutationResult
BatchCommitted = ingestion_lease_contracts.BatchCommitted
LeaseMember = ingestion_lease_contracts.LeaseMember
MembershipSnapshot = ingestion_lease_contracts.MembershipSnapshot
MembershipUnchanged = ingestion_lease_contracts.MembershipUnchanged
MembershipInvariantViolation = (
    ingestion_lease_contracts.MembershipInvariantViolation
)
MembershipRefreshResult = ingestion_lease_contracts.MembershipRefreshResult

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
    "IngestionLeaseStore",
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
    """Decode immutable member identity with fail-closed domain checks.

    Args:
        grant: Exact Lease authority whose source and key own the membership.
        row: One authoritative membership query row.

    Returns:
        A decoded immutable identity, or ``MembershipInvariantViolation`` when
        Feed/source/SID/group values are malformed, unknown, or inconsistent.

    Raises:
        KeyError: The query row omits a required projected column.
    """
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
    except ValueError:
        return MembershipInvariantViolation(grant)
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
    """Decode canonical presentation fields for one validated member identity.

    Args:
        identity: Fail-closed structural identity decoded from the same row.
        row: Membership projection containing Feed name and progress cursor.

    Returns:
        The immutable Lease member exposed in a membership snapshot.

    Raises:
        TypeError: The canonical Feed name is not a string.
        ValueError: The canonical Feed name is missing or blank.
    """
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
            ValueError: The input repeats a permanent Lease identity.
            RuntimeError: An exact active row unexpectedly was not updated.
        """
        grants = tuple(grants)
        identities: set[tuple[feed_store.SourceType, str]] = set()
        for grant in grants:
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
            RuntimeError: An exact active row unexpectedly was not updated.
        """
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
        actor_id = _require_actor_id(actor_id)
        detail = ingestion_lease_contracts._status_reason_detail_storage_value(  # noqa: SLF001
            reason
        )

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
            TypeError: If a returned row has an invalid runtime type.
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
            TypeError: If the known revision has the wrong runtime type.
            ValueError: If the source or known revision is invalid.
        """
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
        """Decode one authoritative membership page under the held lock.

        Args:
            grant: Exact active Lease authority for the loaded page.
            membership_revision: Validated parent revision for the snapshot.
            rows: Authoritative membership rows in routing order.

        Returns:
            A frozen eligible snapshot, or ``MembershipInvariantViolation``
            for empty, duplicate, unknown, or inconsistent membership state.

        Raises:
            TypeError: A required canonical Feed value has an invalid type.
            ValueError: A required canonical Feed value is structurally
                corrupt rather than an expected membership invariant failure.
        """
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
            try:
                status = _status_from_row(row)
            except ValueError:
                return MembershipInvariantViolation(grant)
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
        actor_id = _require_actor_id(actor_id)
        prepared = _ingestion_lease_child_commit.prepare_child_commit(
            grant,
            batch,
            actor_id,
        )
        authority = prepared.grant

        async with self._pool.acquire() as connection:
            async with connection.transaction(isolation="read_committed"):
                lease_row = await connection.fetchrow(
                    ingestion_lease_queries.LOCK_LEASE_SQL,
                    authority.source_type.value,
                    authority.lease_key,
                )
                rejection = self._grant_rejection(authority, lease_row)
                if rejection is not None:
                    return rejection
                if lease_row is None:
                    msg = "accepted Lease grant lacks a locked row"
                    raise ValueError(msg)
                applied = (
                    await _ingestion_lease_child_commit.apply_child_mutations(
                        connection,
                        prepared,
                    )
                )
                lease_recovered = await self._apply_lease_effect(
                    connection,
                    authority,
                    prepared.lease_effect,
                    lease_row,
                )
                pending = (
                    await _ingestion_lease_child_commit.prepare_pending_effects(
                        connection,
                        applied,
                    )
                )

        for plan in applied.plans:
            payload = pending.notification_payloads.get(plan.feed_id)
            if payload is not None:
                feed_change_notifications.emit_feed_change_notification(payload)
        if lease_recovered:
            logger.info(
                "Ingestion Lease recovered after finalized child boundary",
                extra={
                    "source_type": authority.source_type.value,
                    "lease_key": authority.lease_key,
                    "owner_worker_id": str(authority.owner_worker_id),
                    "fencing_token": authority.fencing_token,
                },
            )
        return pending.committed

    async def _apply_lease_effect(
        self,
        connection: asyncpg.Connection,
        grant: LeaseGrant,
        effect: LeaseEffect,
        before_row: collections.abc.Mapping,
    ) -> bool:
        """Apply success-proven Lease recovery under the held exact grant.

        Args:
            connection: Transaction connection holding the exact Lease lock.
            grant: Complete immutable authority matched by that locked row.
            effect: Validated parent lifecycle effect requested by the batch.
            before_row: Locked Lease state used to detect meaningful recovery.

        Returns:
            ``True`` only when dirty lifecycle state was cleared in storage.

        Raises:
            TypeError: A returned Lease field has an invalid runtime type.
            ValueError: Recovery does not return the same grant or produces an
                inconsistent lifecycle or membership revision.
        """
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
