"""Thin IngestionLeaseStore translation for SID grants."""

from __future__ import annotations

import dataclasses
import datetime
import hmac
import json
import secrets
import typing
import uuid

from backend.pipeline.ingestion import grant_control
from backend.pipeline.storage import feed_store, ingestion_lease_store

_STATUS_INELIGIBLE = (
    ingestion_lease_store.LeaseOperationDisposition.STATUS_INELIGIBLE
)
_SID_CLAIM_BINDING_VERSION = "sid-claim-payload-v1"
_SID_CLAIM_BINDING_KEY = secrets.token_bytes(32)
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


@dataclasses.dataclass(frozen=True, slots=True)
class SidClaimPayload:
    """Source-minted immutable provenance for one SID claim.

    Attributes:
        claim_mode: Admission path that established this generation.
    """

    claim_mode: grant_control.ClaimMode
    _binding_proof: bytes = dataclasses.field(
        default=b"",
        init=False,
        repr=False,
        compare=False,
    )

    def __post_init__(self) -> None:
        if not isinstance(self.claim_mode, grant_control.ClaimMode):
            msg = "claim_mode must be a ClaimMode"
            raise TypeError(msg)


def _sid_claim_binding_proof(
    grant: ingestion_lease_store.LeaseGrant,
    payload: SidClaimPayload,
) -> bytes:
    """Bind exact claim provenance to one complete grant in this process."""
    message = json.dumps(
        (
            _SID_CLAIM_BINDING_VERSION,
            grant.source_type.value,
            grant.lease_key,
            str(grant.owner_worker_id),
            grant.fencing_token,
            payload.claim_mode.value,
            id(payload),
        ),
        ensure_ascii=True,
        separators=(",", ":"),
    ).encode("ascii")
    return hmac.digest(_SID_CLAIM_BINDING_KEY, message, "sha256")


def _lifecycle(
    *,
    durable_failing: bool,
) -> grant_control.LifecycleEvidence:
    return grant_control.LifecycleEvidence(durable_failing=durable_failing)


def _finalize_disposition(
    disposition: ingestion_lease_store.LeaseOperationDisposition,
) -> grant_control.FinalizeDisposition:
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


def _released_lifecycle(
    result: ingestion_lease_store.LeaseOperationResult,
    disposition: grant_control.FinalizeDisposition,
) -> grant_control.LifecycleEvidence | None:
    """Translate lifecycle evidence from one neutral release result."""
    if disposition is grant_control.FinalizeDisposition.APPLIED:
        if not isinstance(result.durable_failing, bool):
            msg = "retained SID release lacks lifecycle evidence"
            raise grant_control.GrantControlIntegrityError(msg)
        return _lifecycle(durable_failing=result.durable_failing)
    if result.durable_failing is not None:
        msg = "lost SID release returned lifecycle evidence"
        raise grant_control.GrantControlIntegrityError(msg)
    return None


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

    @staticmethod
    def _issue_claim_payload(
        grant: ingestion_lease_store.LeaseGrant,
        mode: grant_control.ClaimMode,
    ) -> SidClaimPayload:
        """Mint the sole source-owned immutable provenance object."""
        payload = SidClaimPayload(mode)
        object.__setattr__(
            payload,
            "_binding_proof",
            _sid_claim_binding_proof(grant, payload),
        )
        return payload

    async def claim(
        self,
        mode: grant_control.ClaimMode,
        owner_worker_id: uuid.UUID,
        limit: int,
    ) -> tuple[
        grant_control.ClaimedGrant[
            ingestion_lease_store.LeaseGrant,
            SidClaimPayload,
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
            payload = self._issue_claim_payload(
                claim.grant,
                mode,
            )
            translated.append(
                grant_control.ClaimedGrant(
                    grant=claim.grant,
                    payload=payload,
                    lifecycle=_lifecycle(durable_failing=claim.durable_failing),
                )
            )
        return tuple(translated)

    def _validate_claim_payload(
        self,
        grant: ingestion_lease_store.LeaseGrant,
        payload: SidClaimPayload,
    ) -> None:
        """Validate one exact source-issued claim correlation."""
        if (
            grant.source_type is not self._source_type
            or not hmac.compare_digest(
                payload._binding_proof,  # noqa: SLF001
                _sid_claim_binding_proof(grant, payload),
            )
        ):
            msg = "SID finalization payload crossed its complete grant"
            raise grant_control.GrantControlIntegrityError(msg)

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
            if (
                result.disposition
                is ingestion_lease_store.LeaseOperationDisposition.APPLIED
            ):
                disposition = grant_control.HeartbeatDisposition.RETAINED
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
        payload: SidClaimPayload,
        terminal: grant_control.TerminalDecision,
    ) -> grant_control.FinalizeResult[ingestion_lease_store.LeaseGrant]:
        """Execute one exact Lease release or selected failure action."""
        if not isinstance(grant, ingestion_lease_store.LeaseGrant):
            msg = "grant must be a LeaseGrant"
            raise TypeError(msg)
        if type(payload) is not SidClaimPayload:
            msg = "payload must be an exact SidClaimPayload"
            raise TypeError(msg)
        if not isinstance(
            terminal,
            (
                grant_control.NeutralRelease,
                grant_control.BudgetedFailureDecision,
                grant_control.NonBudgetedFailureDecision,
            ),
        ):
            msg = "terminal must be a closed TerminalDecision"
            raise TypeError(msg)
        self._validate_claim_payload(grant, payload)

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
            return grant_control.FinalizeResult(
                grant,
                disposition,
                _released_lifecycle(result, disposition),
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
            result.final_status is feed_store.FeedStatus.QUARANTINED
        ):
            msg = "non-budgeted SID failure cannot quarantine"
            raise grant_control.GrantControlIntegrityError(msg)
        disposition = _finalize_disposition(result.disposition)
        lifecycle = (
            grant_control.LifecycleEvidence(durable_failing=True)
            if disposition is grant_control.FinalizeDisposition.APPLIED
            else None
        )
        return grant_control.FinalizeResult(grant, disposition, lifecycle)
