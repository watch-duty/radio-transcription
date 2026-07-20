"""Thin IngestionLeaseStore translation for SID grants."""

from __future__ import annotations

import datetime
import typing

from backend.pipeline.ingestion import failure_policy, grant_control
from backend.pipeline.storage import feed_store, ingestion_lease_store

if typing.TYPE_CHECKING:
    import uuid

_STATUS_INELIGIBLE = (
    ingestion_lease_store.LeaseOperationDisposition.STATUS_INELIGIBLE
)


def _finalize_disposition(
    disposition: ingestion_lease_store.LeaseOperationDisposition,
) -> grant_control.FinalizeDisposition:
    """Translate one authoritative Lease finalization disposition.

    Args:
        disposition: Storage-level exact-grant outcome.

    Returns:
        Domain-neutral applied or lost outcome.

    Raises:
        grant_control.GrantControlIntegrityError: ``disposition`` is unknown.
    """
    if disposition is ingestion_lease_store.LeaseOperationDisposition.APPLIED:
        return grant_control.FinalizeDisposition.APPLIED
    if disposition in (
        ingestion_lease_store.LeaseOperationDisposition.MISSING,
        ingestion_lease_store.LeaseOperationDisposition.OWNER_MISMATCH,
        ingestion_lease_store.LeaseOperationDisposition.FENCE_MISMATCH,
        ingestion_lease_store.LeaseOperationDisposition.STATUS_INELIGIBLE,
    ):
        return grant_control.FinalizeDisposition.LOST
    msg = "SID finalization returned an unknown disposition"
    raise grant_control.GrantControlIntegrityError(msg)


class SidGrantControl:
    """Translate generic grant operations to authoritative SID Lease stores."""

    domain_id = grant_control.DomainId.SID

    def __init__(
        self,
        data_store: ingestion_lease_store.IngestionLeaseStore,
        heartbeat_store: ingestion_lease_store.IngestionLeaseStore,
        source_type: feed_store.SourceType,
        abandonment_window: datetime.timedelta,
        *,
        actor_id: str,
    ) -> None:
        if abandonment_window <= datetime.timedelta(0):
            msg = "abandonment_window must be positive"
            raise ValueError(msg)
        if not actor_id.strip():
            msg = "actor_id must not be empty"
            raise ValueError(msg)
        self._data_store = data_store
        self._heartbeat_store = heartbeat_store
        self._source_type = source_type
        self._abandonment_window = abandonment_window
        self._actor_id = actor_id

    async def claim(
        self,
        mode: grant_control.ClaimMode,
        owner_worker_id: uuid.UUID,
        limit: int,
    ) -> tuple[
        grant_control.ClaimedGrant[
            ingestion_lease_store.LeaseGrant,
            grant_control.ClaimMode,
        ],
        ...,
    ]:
        """Map primary or recovery admission directly to the Lease store.

        Args:
            mode: Primary or recovery admission mode.
            owner_worker_id: Worker that will own returned grants.
            limit: Maximum total grants to return.

        Returns:
            Store-ordered exact Lease grants paired with ``mode``.

        Raises:
            TypeError: ``limit`` is a boolean.
            ValueError: ``limit`` is negative.
            grant_control.GrantControlIntegrityError: Claims exceed ``limit``,
                repeat authority, have an invalid type, or do not match the
                configured source and requested owner.
        """
        if isinstance(limit, bool):
            msg = "limit must be an integer"
            raise TypeError(msg)
        if limit < 0:
            msg = "limit must be nonnegative"
            raise ValueError(msg)
        if limit == 0:
            return ()

        if mode is grant_control.ClaimMode.PRIMARY:
            claims = await self._data_store.claim_unclaimed(
                self._source_type,
                owner_worker_id,
                limit,
            )
        else:
            claims = await self._data_store.claim_recoverable(
                self._source_type,
                owner_worker_id,
                limit,
                self._abandonment_window,
            )
        if len(claims) > limit:
            msg = "SID claim returned more grants than requested"
            raise grant_control.GrantControlIntegrityError(msg)

        translated = []
        seen: set[ingestion_lease_store.LeaseGrant] = set()
        for claim in claims:
            if claim.grant in seen:
                msg = "SID claim returned a duplicate authority"
                raise grant_control.GrantControlIntegrityError(msg)
            if (
                claim.grant.source_type is not self._source_type
                or claim.grant.owner_worker_id != owner_worker_id
            ):
                msg = "SID claim returned an unexpected grant identity"
                raise grant_control.GrantControlIntegrityError(msg)
            seen.add(claim.grant)
            translated.append(
                grant_control.ClaimedGrant(
                    grant=claim.grant,
                    payload=mode,
                )
            )
        return tuple(translated)

    async def heartbeat(
        self,
        grants: typing.Sequence[ingestion_lease_store.LeaseGrant],
    ) -> tuple[
        grant_control.GrantHeartbeat[ingestion_lease_store.LeaseGrant],
        ...,
    ]:
        """Translate exact Lease heartbeat results without losing order.

        Args:
            grants: Complete Lease grants in caller correlation order.

        Returns:
            One domain-neutral disposition for every submitted grant.

        Raises:
            grant_control.GrantControlIntegrityError: Results have invalid
                cardinality, identity, order, or disposition.
        """
        grants = tuple(grants)
        results = await self._heartbeat_store.renew_heartbeats(grants)
        if len(results) != len(grants):
            msg = "SID heartbeat result cardinality mismatch"
            raise grant_control.GrantControlIntegrityError(msg)

        translated = []
        seen: set[ingestion_lease_store.LeaseGrant] = set()
        for index, result in enumerate(results):
            if result.grant in seen or result.grant != grants[index]:
                msg = "SID heartbeat result identity or order mismatch"
                raise grant_control.GrantControlIntegrityError(msg)
            seen.add(result.grant)

            if (
                result.disposition
                is ingestion_lease_store.LeaseOperationDisposition.APPLIED
            ):
                disposition = grant_control.HeartbeatDisposition.RETAINED
            elif result.disposition is _STATUS_INELIGIBLE:
                disposition = grant_control.HeartbeatDisposition.INELIGIBLE
            elif result.disposition in (
                ingestion_lease_store.LeaseOperationDisposition.MISSING,
                ingestion_lease_store.LeaseOperationDisposition.OWNER_MISMATCH,
                ingestion_lease_store.LeaseOperationDisposition.FENCE_MISMATCH,
            ):
                disposition = grant_control.HeartbeatDisposition.LOST
            else:
                msg = "SID heartbeat returned an unknown disposition"
                raise grant_control.GrantControlIntegrityError(msg)

            translated.append(
                grant_control.GrantHeartbeat(
                    grant=result.grant,
                    disposition=disposition,
                )
            )
        return tuple(translated)

    async def finalize(
        self,
        grant: ingestion_lease_store.LeaseGrant,
        payload: grant_control.ClaimMode,
        terminal: grant_control.TerminalDecision,
    ) -> grant_control.FinalizeResult[ingestion_lease_store.LeaseGrant]:
        """Execute one exact Lease release or selected failure action.

        Args:
            grant: Exact Lease ownership generation to finalize.
            payload: Claim mode returned with the original claim.
            terminal: Neutral release or preclassified failure plan.

        Returns:
            Applied or lost exact-grant disposition.

        Raises:
            grant_control.GrantControlIntegrityError: Storage returns an
                invalid result or a non-budgeted failure quarantines.
        """
        if isinstance(terminal, grant_control.NeutralRelease):
            result = await self._data_store.release(grant)
            disposition = _finalize_disposition(result.disposition)
            return grant_control.FinalizeResult(grant, disposition)

        treatment = terminal.treatment
        if isinstance(treatment, failure_policy.ConsumeFailureBudget):
            action: ingestion_lease_store.LeaseFailureAction = (
                ingestion_lease_store.BudgetedFailure(
                    failure_threshold=treatment.failure_threshold,
                    backoff_base_sec=treatment.backoff_base_sec,
                    backoff_max_sec=treatment.backoff_max_sec,
                )
            )
        elif isinstance(treatment, failure_policy.RetryWithoutBudget):
            action = ingestion_lease_store.NonBudgetedFailure(
                treatment.retry_after
            )
        else:
            msg = "terminal must be a closed TerminalDecision"
            raise TypeError(msg)

        result = await self._data_store.finalize_failure(
            grant,
            action,
            terminal.status_reason,
            actor_id=self._actor_id,
            reason=terminal.reason,
        )
        if isinstance(treatment, failure_policy.RetryWithoutBudget) and (
            result.final_status is feed_store.FeedStatus.QUARANTINED
        ):
            msg = "non-budgeted SID failure cannot quarantine"
            raise grant_control.GrantControlIntegrityError(msg)
        disposition = _finalize_disposition(result.disposition)
        return grant_control.FinalizeResult(grant, disposition)
