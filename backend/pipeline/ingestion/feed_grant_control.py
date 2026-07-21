"""Thin FeedStore translation for the generic grant-control contract."""

from __future__ import annotations

import asyncio
import datetime
import logging
import types
import typing

from backend.pipeline.ingestion import (
    failure_policy,
    failure_telemetry,
    grant_control,
)
from backend.pipeline.storage import feed_store

if typing.TYPE_CHECKING:
    import uuid

logger = logging.getLogger(__name__)

_QUARANTINE_OBSERVER_TIMEOUT_SEC = 2.0

type QuarantineObserver = typing.Callable[
    [
        feed_store.FeedGrant,
        feed_store.LeasedFeed,
        failure_policy.FailurePersistencePlan,
    ],
    typing.Awaitable[None],
]


def _calculate_branch_limits(
    total_slack: int,
    caps: typing.Mapping[feed_store.SourceType, int],
    held: typing.Mapping[feed_store.SourceType, int],
) -> dict[feed_store.SourceType, int]:
    """Preserve the legacy deterministic water-fill allocation.

    Args:
        total_slack: Maximum grants available in this admission pass.
        caps: Per-source ownership ceilings.
        held: Current durable ownership counts by source.

    Returns:
        Per-source claim limits whose sum does not exceed ``total_slack``.
    """
    headroom = {
        source_type: min(
            caps[source_type],
            max(0, caps[source_type] - held.get(source_type, 0)),
        )
        for source_type in caps
    }
    limits: dict[feed_store.SourceType, int] = dict.fromkeys(caps, 0)
    sorted_types = sorted(caps, key=lambda source_type: headroom[source_type])
    remaining_slack = total_slack
    remaining_branches = len(sorted_types)
    for source_type in sorted_types:
        share = (
            remaining_slack // remaining_branches
            if remaining_branches > 0
            else 0
        )
        limits[source_type] = min(headroom[source_type], share)
        remaining_slack -= limits[source_type]
        remaining_branches -= 1
    return limits


class FeedGrantControl:
    """Translate generic grant operations to two authoritative Feed stores."""

    domain_id = grant_control.DomainId.FEED

    def __init__(
        self,
        data_store: feed_store.FeedStore,
        heartbeat_store: feed_store.FeedStore,
        source_caps: typing.Mapping[feed_store.SourceType, int],
        abandonment_window: datetime.timedelta,
        *,
        actor_id: str,
        on_quarantined: QuarantineObserver | None = None,
    ) -> None:
        caps: dict[feed_store.SourceType, int] = {}
        for source_type, cap in source_caps.items():
            if isinstance(cap, bool):
                msg = "source caps must be integers"
                raise TypeError(msg)
            if cap < 0:
                msg = "source caps must be nonnegative"
                raise ValueError(msg)
            caps[source_type] = cap
        if abandonment_window <= datetime.timedelta(0):
            msg = "abandonment_window must be positive"
            raise ValueError(msg)
        if not actor_id.strip():
            msg = "actor_id must not be empty"
            raise ValueError(msg)
        self._data_store = data_store
        self._heartbeat_store = heartbeat_store
        self._source_caps = types.MappingProxyType(caps)
        self._abandonment_window = abandonment_window
        self._actor_id = actor_id
        self._on_quarantined = on_quarantined

    async def _observe_quarantine(
        self,
        grant: feed_store.FeedGrant,
        payload: feed_store.LeasedFeed,
        terminal: failure_policy.FailurePersistencePlan,
    ) -> None:
        """Bound non-authoritative evidence after durable finalization.

        Args:
            grant: Exact Feed ownership generation that was finalized.
            payload: Original leased Feed payload.
            terminal: Applied budgeted failure plan.

        Returns:
            None after observation completes or is abandoned.
        """
        observer = self._on_quarantined
        if observer is None:
            return
        try:
            await asyncio.wait_for(
                observer(grant, payload, terminal),
                timeout=_QUARANTINE_OBSERVER_TIMEOUT_SEC,
            )
        except asyncio.CancelledError:
            logger.warning(
                "Quarantine observer cancelled after exact Feed finalization"
            )
        except TimeoutError:
            logger.warning(
                "Quarantine observer timed out after exact Feed finalization"
            )
        except Exception:
            logger.exception(
                "Quarantine observer failed after exact Feed finalization"
            )

    async def claim(
        self,
        mode: grant_control.ClaimMode,
        owner_worker_id: uuid.UUID,
        limit: int,
    ) -> tuple[
        grant_control.ClaimedGrant[
            feed_store.FeedGrant,
            feed_store.LeasedFeed,
        ],
        ...,
    ]:
        """Claim primary or recoverable Feeds through legacy water filling.

        Args:
            mode: Primary or recovery admission mode.
            owner_worker_id: Worker that will own returned grants.
            limit: Maximum total grants to return.

        Returns:
            Store-ordered exact Feed grants and unchanged leased payloads.

        Raises:
            TypeError: ``limit`` is a boolean.
            ValueError: ``limit`` is negative.
            grant_control.GrantControlIntegrityError: The store returns too
                many claims or repeats a Feed authority.
        """
        if isinstance(limit, bool):
            msg = "limit must be an integer"
            raise TypeError(msg)
        if limit < 0:
            msg = "limit must be nonnegative"
            raise ValueError(msg)
        if limit == 0:
            return ()

        held = await self._data_store.count_held_by_type(owner_worker_id)
        limits = _calculate_branch_limits(limit, self._source_caps, held)
        if mode is grant_control.ClaimMode.PRIMARY:
            payloads = await self._data_store.acquire_feeds_batch(
                owner_worker_id,
                limits,
            )
        else:
            payloads = await self._data_store.acquire_feeds_recovery(
                owner_worker_id,
                self._abandonment_window.total_seconds(),
                limits,
            )

        if len(payloads) > limit:
            msg = "Feed claim returned more grants than requested"
            raise grant_control.GrantControlIntegrityError(msg)
        claims = []
        seen: set[uuid.UUID] = set()
        for payload in payloads:
            feed_id = payload["id"]
            if feed_id in seen:
                msg = "Feed claim returned a duplicate authority"
                raise grant_control.GrantControlIntegrityError(msg)
            seen.add(feed_id)
            grant = feed_store.FeedGrant(
                feed_id=feed_id,
                owner_worker_id=owner_worker_id,
                fencing_token=payload["fencing_token"],
            )
            claims.append(
                grant_control.ClaimedGrant(
                    grant=grant,
                    payload=payload,
                )
            )
        return tuple(claims)

    async def heartbeat(
        self,
        grants: typing.Sequence[feed_store.FeedGrant],
    ) -> tuple[grant_control.GrantHeartbeat[feed_store.FeedGrant], ...]:
        """Translate exact Feed heartbeat results without losing order.

        Args:
            grants: Complete Feed grants in caller correlation order.

        Returns:
            One domain-neutral disposition for every submitted grant.

        Raises:
            grant_control.GrantControlIntegrityError: Results have invalid
                cardinality, identity, order, or disposition.
        """
        grants = tuple(grants)
        results = await self._heartbeat_store.renew_grant_heartbeats(grants)
        if len(results) != len(grants):
            msg = "Feed heartbeat result cardinality mismatch"
            raise grant_control.GrantControlIntegrityError(msg)

        translated = []
        seen: set[feed_store.FeedGrant] = set()
        for index, result in enumerate(results):
            if result.grant in seen or result.grant != grants[index]:
                msg = "Feed heartbeat result identity or order mismatch"
                raise grant_control.GrantControlIntegrityError(msg)
            seen.add(result.grant)

            if (
                result.disposition
                is feed_store.FeedGrantOperationDisposition.APPLIED
            ):
                disposition = grant_control.HeartbeatDisposition.RETAINED
            elif (
                result.disposition
                is feed_store.FeedGrantOperationDisposition.STATUS_INELIGIBLE
            ):
                disposition = grant_control.HeartbeatDisposition.INELIGIBLE
            elif result.disposition in (
                feed_store.FeedGrantOperationDisposition.MISSING,
                feed_store.FeedGrantOperationDisposition.OWNER_MISMATCH,
                feed_store.FeedGrantOperationDisposition.FENCE_MISMATCH,
            ):
                disposition = grant_control.HeartbeatDisposition.LOST
            else:
                msg = "Feed heartbeat returned an unknown disposition"
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
        grant: feed_store.FeedGrant,
        payload: feed_store.LeasedFeed,
        terminal: grant_control.TerminalDecision,
    ) -> grant_control.FinalizeResult[feed_store.FeedGrant]:
        """Execute one exact Feed release or selected failure action.

        Args:
            grant: Exact Feed ownership generation to finalize.
            payload: Original leased Feed payload paired with ``grant``.
            terminal: Neutral release or preclassified failure plan.

        Returns:
            Applied or lost exact-grant disposition.

        Raises:
            grant_control.GrantControlIntegrityError: The payload does not
                match the grant, or storage returns an invalid outcome.
            asyncio.CancelledError: Authoritative storage finalization is
                cancelled before its outcome is known.
        """
        if (
            payload["id"] != grant.feed_id
            or payload["fencing_token"] != grant.fencing_token
        ):
            msg = "Feed finalization payload does not match its grant"
            raise grant_control.GrantControlIntegrityError(msg)

        if isinstance(terminal, grant_control.NeutralRelease):
            applied = await self._data_store.release_feed(
                grant.feed_id,
                grant.owner_worker_id,
                grant.fencing_token,
            )
            disposition = (
                grant_control.FinalizeDisposition.APPLIED
                if applied
                else grant_control.FinalizeDisposition.LOST
            )
            return grant_control.FinalizeResult(grant, disposition)

        treatment = terminal.treatment
        if isinstance(treatment, failure_policy.ConsumeFailureBudget):
            status = await self._data_store.report_feed_failure(
                grant.feed_id,
                grant.owner_worker_id,
                grant.fencing_token,
                treatment.failure_threshold,
                treatment.backoff_base_sec,
                treatment.backoff_max_sec,
                actor_id=self._actor_id,
                reason=terminal.reason,
                status_reason=terminal.status_reason,
            )
        elif isinstance(treatment, failure_policy.RetryWithoutBudget):
            status = await self._data_store.release_non_budgeted_failure(
                grant.feed_id,
                grant.owner_worker_id,
                grant.fencing_token,
                retry_after=treatment.retry_after,
                status_reason=terminal.status_reason,
                actor_id=self._actor_id,
                reason=terminal.reason,
            )
        else:
            msg = "terminal must be a closed TerminalDecision"
            raise TypeError(msg)

        if status is None:
            return grant_control.FinalizeResult(
                grant,
                grant_control.FinalizeDisposition.LOST,
            )
        if status not in (
            feed_store.FeedStatus.FAILING.value,
            feed_store.FeedStatus.QUARANTINED.value,
        ):
            msg = f"Feed failure returned unexpected status {status!r}"
            raise grant_control.GrantControlIntegrityError(msg)
        if (
            isinstance(treatment, failure_policy.RetryWithoutBudget)
            and status == feed_store.FeedStatus.QUARANTINED.value
        ):
            msg = "Non-budgeted Feed finalization returned quarantined"
            raise grant_control.GrantControlIntegrityError(msg)
        failure_telemetry.emit_failure_policy_decision(
            logger,
            feed_id=payload["id"],
            source_type=payload["source_type"],
            plan=terminal,
        )
        if status == feed_store.FeedStatus.QUARANTINED.value and isinstance(
            treatment, failure_policy.ConsumeFailureBudget
        ):
            await self._observe_quarantine(grant, payload, terminal)
        return grant_control.FinalizeResult(
            grant,
            grant_control.FinalizeDisposition.APPLIED,
        )
