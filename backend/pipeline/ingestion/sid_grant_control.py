"""Thin IngestionLeaseStore translation for SID grants."""

from __future__ import annotations

import datetime
import typing
import uuid

from backend.pipeline.ingestion import grant_control
from backend.pipeline.storage import feed_store, ingestion_lease_store

_STATUS_INELIGIBLE = (
    ingestion_lease_store.LeaseOperationDisposition.STATUS_INELIGIBLE
)
_RELEASE_CAUSES = {
    grant_control.TerminalCause.NORMAL: (
        ingestion_lease_store.LeaseReleaseCause.NORMAL
    ),
    grant_control.TerminalCause.SHUTDOWN: (
        ingestion_lease_store.LeaseReleaseCause.SHUTDOWN
    ),
    grant_control.TerminalCause.PLANNED_DRAIN: (
        ingestion_lease_store.LeaseReleaseCause.REBALANCE
    ),
    grant_control.TerminalCause.CANCELLATION: (
        ingestion_lease_store.LeaseReleaseCause.CANCELLATION
    ),
}


def _lifecycle(
    snapshot: ingestion_lease_store.LeaseSnapshot,
) -> grant_control.LifecycleEvidence:
    return grant_control.LifecycleEvidence(
        durable_failing=(
            snapshot.failure_count > 0 or snapshot.status_reason is not None
        )
    )


def _finalize_disposition(
    disposition: ingestion_lease_store.LeaseOperationDisposition,
) -> grant_control.FinalizeDisposition:
    if disposition is ingestion_lease_store.LeaseOperationDisposition.APPLIED:
        return grant_control.FinalizeDisposition.APPLIED
    if (
        disposition
        is ingestion_lease_store.LeaseOperationDisposition.ACCEPTED_NOOP
    ):
        return grant_control.FinalizeDisposition.ACCEPTED_NOOP
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

    def __init__(
        self,
        data_store: ingestion_lease_store.IngestionLeaseStore,
        heartbeat_store: ingestion_lease_store.IngestionLeaseStore,
        source_type: feed_store.SourceType,
        abandonment_window: datetime.timedelta,
    ) -> None:
        if not isinstance(source_type, feed_store.SourceType):
            msg = "source_type must be a SourceType"
            raise TypeError(msg)
        if not isinstance(abandonment_window, datetime.timedelta):
            msg = "abandonment_window must be a timedelta"
            raise TypeError(msg)
        if abandonment_window <= datetime.timedelta(0):
            msg = "abandonment_window must be positive"
            raise ValueError(msg)
        self._data_store = data_store
        self._heartbeat_store = heartbeat_store
        self._source_type = source_type
        self._abandonment_window = abandonment_window

    async def claim(
        self,
        mode: grant_control.ClaimMode,
        owner_worker_id: uuid.UUID,
        limit: int,
    ) -> tuple[
        grant_control.ClaimedGrant[
            ingestion_lease_store.LeaseGrant,
            ingestion_lease_store.LeaseSnapshot,
        ],
        ...,
    ]:
        """Map primary or recovery admission directly to the Lease store."""
        if not isinstance(mode, grant_control.ClaimMode):
            msg = "mode must be a ClaimMode"
            raise TypeError(msg)
        if not isinstance(owner_worker_id, uuid.UUID):
            msg = "owner_worker_id must be a UUID"
            raise TypeError(msg)
        if isinstance(limit, bool) or not isinstance(limit, int):
            msg = "limit must be an integer"
            raise TypeError(msg)
        if limit < 0:
            msg = "limit must be nonnegative"
            raise ValueError(msg)

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
            if not isinstance(claim, ingestion_lease_store.LeaseClaim):
                msg = "SID claim returned an invalid result type"
                raise grant_control.GrantControlIntegrityError(msg)
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
                    payload=claim.snapshot,
                    lifecycle=_lifecycle(claim.snapshot),
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
        """Translate exact Lease heartbeat results without losing order."""
        grants = tuple(grants)
        results = await self._heartbeat_store.renew_heartbeats(grants)
        if len(results) != len(grants):
            msg = "SID heartbeat result cardinality mismatch"
            raise grant_control.GrantControlIntegrityError(msg)

        translated = []
        seen: set[ingestion_lease_store.LeaseGrant] = set()
        for index, result in enumerate(results):
            if not isinstance(
                result, ingestion_lease_store.LeaseHeartbeatResult
            ):
                msg = "SID heartbeat returned an invalid result type"
                raise grant_control.GrantControlIntegrityError(msg)
            if result.grant in seen or result.grant != grants[index]:
                msg = "SID heartbeat result identity or order mismatch"
                raise grant_control.GrantControlIntegrityError(msg)
            seen.add(result.grant)

            lifecycle = None
            if result.disposition in (
                ingestion_lease_store.LeaseOperationDisposition.APPLIED,
                ingestion_lease_store.LeaseOperationDisposition.ACCEPTED_NOOP,
            ):
                if result.snapshot is None:
                    msg = "retained SID heartbeat is missing its snapshot"
                    raise grant_control.GrantControlIntegrityError(msg)
                disposition = grant_control.HeartbeatDisposition.RETAINED
                lifecycle = _lifecycle(result.snapshot)
            elif result.disposition is _STATUS_INELIGIBLE:
                disposition = (
                    grant_control.HeartbeatDisposition.ADMINISTRATIVE_STOP
                )
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
                    lifecycle=lifecycle,
                )
            )
        return tuple(translated)

    async def finalize(
        self,
        grant: ingestion_lease_store.LeaseGrant,
        payload: ingestion_lease_store.LeaseSnapshot,
        terminal: grant_control.TerminalDecision,
    ) -> grant_control.FinalizeResult[ingestion_lease_store.LeaseGrant]:
        """Execute one exact Lease release or selected failure action."""
        if not isinstance(grant, ingestion_lease_store.LeaseGrant):
            msg = "grant must be a LeaseGrant"
            raise TypeError(msg)
        if not isinstance(payload, ingestion_lease_store.LeaseSnapshot):
            msg = "payload must be a LeaseSnapshot"
            raise TypeError(msg)

        if isinstance(terminal, grant_control.NeutralRelease):
            result = await self._data_store.release(
                grant,
                cause=_RELEASE_CAUSES[terminal.cause],
            )
            if not isinstance(
                result, ingestion_lease_store.LeaseOperationResult
            ):
                msg = "SID release returned an invalid result type"
                raise grant_control.GrantControlIntegrityError(msg)
            disposition = _finalize_disposition(result.disposition)
            if (
                disposition
                in (
                    grant_control.FinalizeDisposition.APPLIED,
                    grant_control.FinalizeDisposition.ACCEPTED_NOOP,
                )
                and result.snapshot is None
            ):
                msg = "retained SID release is missing its snapshot"
                raise grant_control.GrantControlIntegrityError(msg)
            lifecycle = (
                _lifecycle(result.snapshot)
                if result.snapshot is not None
                and disposition is not grant_control.FinalizeDisposition.LOST
                else None
            )
            return grant_control.FinalizeResult(
                grant,
                disposition,
                lifecycle,
            )

        if isinstance(terminal, grant_control.BudgetedFailureDecision):
            action: ingestion_lease_store.LeaseFailureAction = (
                ingestion_lease_store.BudgetedFailure(
                    failure_threshold=terminal.failure_threshold,
                    backoff_base_sec=terminal.backoff_base_sec,
                    backoff_max_sec=terminal.backoff_max_sec,
                )
            )
        elif isinstance(terminal, grant_control.NonBudgetedFailureDecision):
            action = ingestion_lease_store.NonBudgetedFailure(
                terminal.retry_after
            )
        else:
            msg = "terminal must be a closed TerminalDecision"
            raise TypeError(msg)

        result = await self._data_store.finalize_failure(
            grant,
            action,
            terminal.status_reason,
            actor_id=terminal.actor_id,
            reason=terminal.reason,
        )
        if not isinstance(result, ingestion_lease_store.LeaseFailureResult):
            msg = "SID failure returned an invalid result type"
            raise grant_control.GrantControlIntegrityError(msg)
        if isinstance(terminal, grant_control.NonBudgetedFailureDecision) and (
            result.effect
            is ingestion_lease_store.LeaseFailureEffect.QUARANTINED
            or (
                result.disposition
                is ingestion_lease_store.LeaseOperationDisposition.APPLIED
                and result.after_snapshot is not None
                and result.after_snapshot.status
                is feed_store.FeedStatus.QUARANTINED
            )
        ):
            msg = "non-budgeted SID failure cannot quarantine"
            raise grant_control.GrantControlIntegrityError(msg)
        disposition = _finalize_disposition(result.disposition)
        lifecycle = None
        if disposition is not grant_control.FinalizeDisposition.LOST:
            snapshot = result.after_snapshot or result.before_snapshot
            if snapshot is None:
                msg = "retained SID failure result is missing its snapshot"
                raise grant_control.GrantControlIntegrityError(msg)
            lifecycle = _lifecycle(snapshot)
        return grant_control.FinalizeResult(grant, disposition, lifecycle)
