"""One-page-at-a-time Broadcastify Calls ingestion for an owned SID."""

from __future__ import annotations

import asyncio
import collections
import collections.abc
import datetime
import logging
import math
import typing
import uuid

from backend.pipeline.ingestion import (
    failure_policy,
    failure_telemetry,
    grant_control,
    models,
    retry,
)
from backend.pipeline.ingestion.collectors import failure_classification
from backend.pipeline.ingestion.collectors.bcfy_calls import (
    pipeline,
    provider,
    work_pool,
)
from backend.pipeline.storage import feed_store, ingestion_lease_store

logger = logging.getLogger(__name__)

_POLL_INTERVAL_SEC = 10.0
_MAX_CONSECUTIVE_FAILURES = 10
_RECENT_URL_LIMIT = 1_000
_MEMBERSHIP_INVALID = "bcfy_calls_sid_membership_invalid"
_MIXED_FEED_FAILURES = "mixed_feed_failures"
_POST_BOOKMARK_FAILURE = (
    feed_store.FeedStatusReason.PIPELINE_PUBLISH_AFTER_BOOKMARK_FAILED
)
_TRANSIENT_METADATA_FAILURES = frozenset(
    {
        feed_store.FeedStatusReason.SOURCE_RATE_LIMITED,
        feed_store.FeedStatusReason.SOURCE_UNREACHABLE,
        feed_store.FeedStatusReason.SYSTEM_AUTHENTICATION_FAILED,
    }
)

type FailurePlanner = collections.abc.Callable[
    [feed_store.FeedStatusReason, str | None],
    failure_policy.FailurePersistencePlan,
]
type _CommitResult = (
    ingestion_lease_store.BatchCommitted | ingestion_lease_store.GrantRejected
)


class _LeaseStore(typing.Protocol):
    async def load_membership(
        self,
        grant: ingestion_lease_store.LeaseGrant,
    ) -> (
        ingestion_lease_store.GrantRejected
        | ingestion_lease_store.MembershipSnapshot
        | ingestion_lease_store.MembershipInvariantViolation
    ):
        """Load the authoritative membership beneath an exact SID grant."""
        ...

    async def commit_child_mutations(
        self,
        grant: ingestion_lease_store.LeaseGrant,
        batch: ingestion_lease_store.ChildMutationBatch,
        *,
        actor_id: str,
    ) -> (
        ingestion_lease_store.GrantRejected
        | ingestion_lease_store.BatchCommitted
    ):
        """Commit one closed child batch beneath an exact SID grant."""
        ...


class _CallsProvider(typing.Protocol):
    async def fetch_sid_page(
        self,
        sid: str,
        pos: datetime.datetime | None,
        *,
        shutdown_event: asyncio.Event,
    ) -> provider.CallsPageEnvelope:
        """Fetch one Calls page for a SID."""
        ...


class _BatchPool(typing.Protocol):
    async def settle_batches(
        self,
        batches: collections.abc.Sequence[pipeline.FeedBatch],
    ) -> work_pool.SettledBatches[
        pipeline.FeedBatch,
        pipeline.FeedBatchResult,
    ]:
        """Admit and settle one caller-ordered group of Feed batches.

        Args:
            batches: Complete per-Feed batches for one SID page.

        Returns:
            Settled batch evidence, unexpected failure, and deferred caller
            cancellation.
        """
        ...


class _FeedState:
    """Grant-local publication and duplicate-suppression state for one Feed.

    Attributes:
        session_id: Publication session shared by this grant-local Feed run.
        next_sequence: Next publication sequence after settled Feed work.
        recent_committed_urls: Bounded set of recently committed source URLs.
    """

    def __init__(self) -> None:
        self.session_id = str(uuid.uuid4())
        self.next_sequence = 0
        self.recent_committed_urls: collections.OrderedDict[str, None] = (
            collections.OrderedDict()
        )

    def remember_urls(self, urls: collections.abc.Iterable[str]) -> None:
        """Remember committed URLs within a small grant-local window."""
        for url in urls:
            if url in self.recent_committed_urls:
                continue
            self.recent_committed_urls[url] = None
            if len(self.recent_committed_urls) > _RECENT_URL_LIMIT:
                self.recent_committed_urls.popitem(last=False)


def _prune_departed_states(
    states: dict[uuid.UUID, _FeedState],
    members: collections.abc.Iterable[ingestion_lease_store.LeaseMember],
) -> None:
    """Drop grant-local state for Feeds absent from current membership."""
    current_feed_ids = {member.identity.feed_id for member in members}
    for feed_id in states.keys() - current_feed_ids:
        del states[feed_id]


def _utc_timestamp(value: object) -> datetime.datetime | None:
    """Decode a finite provider timestamp at the external trust boundary."""
    if isinstance(value, bool) or not isinstance(value, (int, float, str)):
        return None
    try:
        numeric = float(value)
        if not math.isfinite(numeric):
            return None
        return datetime.datetime.fromtimestamp(numeric, datetime.UTC)
    except (TypeError, ValueError, OSError, OverflowError):
        return None


def _valid_page_boundary(
    raw_last_pos: object,
    requested_position: datetime.datetime | None,
    response_received_at: datetime.datetime,
) -> datetime.datetime | None:
    """Return a monotonic, non-future cursor from one provider response.

    Args:
        raw_last_pos: Untrusted ``lastPos`` value from Broadcastify.
        requested_position: Durable position used for the page request.
        response_received_at: Local UTC time when the response completed.

    Returns:
        A safe page boundary, or None when the provider value is invalid.
    """
    boundary = _utc_timestamp(raw_last_pos)
    if boundary is None:
        return None
    if boundary.timestamp() <= 0:
        return None
    if boundary > response_received_at:
        return None
    if requested_position is not None:
        issued_second = int(requested_position.timestamp())
        if int(boundary.timestamp()) < issued_second:
            return None
    return boundary


def _call_order(
    call: pipeline.CallWork,
) -> tuple[bool, datetime.datetime]:
    """Sort valid cursor times first while retaining stable malformed order."""
    timestamp = _utc_timestamp(call.payload.get("ts"))
    return (
        timestamp is None,
        timestamp or datetime.datetime.max.replace(tzinfo=datetime.UTC),
    )


def _storage_failure_action(
    plan: failure_policy.FailurePersistencePlan,
) -> ingestion_lease_store.LeaseFailureAction:
    treatment = plan.treatment
    if isinstance(treatment, failure_policy.ConsumeFailureBudget):
        return ingestion_lease_store.BudgetedFailure(
            failure_threshold=treatment.failure_threshold,
            backoff_base_sec=treatment.backoff_base_sec,
            backoff_max_sec=treatment.backoff_max_sec,
        )
    return ingestion_lease_store.NonBudgetedFailure(treatment.retry_after)


def _log_poll_settled(
    grant: ingestion_lease_store.LeaseGrant,
    *,
    status: str,
    error: str | None,
) -> None:
    """Emit the single operational settlement event for one initiated poll."""
    fields: dict[str, object] = {
        "event_type": "bcfy_calls_sid_poll_settled",
        "sid": grant.lease_key,
        "status": status,
    }
    if error is not None:
        fields["error"] = error
    logger.info(
        "Broadcastify Calls SID poll settled",
        extra={"json_fields": fields},
    )


def _promoted_sid_failure(
    results: collections.abc.Sequence[pipeline.FeedBatchResult],
) -> failure_classification.ItemFailure | None:
    """Select a parent failure for one unanimously failed multi-Feed page.

    Args:
        results: Definitive results for every call-bearing Feed on a complete
            SID page.

    Returns:
        The shared failure, a generic failure for mixed classifications, or
        ``None`` when the page retains Feed-local outcomes.
    """
    if len(results) < 2:
        return None

    failures: list[failure_classification.ItemFailure] = []
    for result in results:
        if result.published_count:
            return None
        terminal = result.terminal
        if not isinstance(terminal, failure_classification.ItemFailure):
            return None
        failures.append(terminal)

    first = failures[0]
    if all(
        failure.status_reason is first.status_reason for failure in failures
    ):
        return first
    return failure_classification.ItemFailure(
        feed_store.FeedStatusReason.SYSTEM_COLLECTOR_ERROR,
        _MIXED_FEED_FAILURES,
    )


def _raise_settlement_failure(
    failure: BaseException,
    cancellation: asyncio.CancelledError | None,
) -> typing.NoReturn:
    """Raise failed accepted work without misclassifying child cancellation."""
    if isinstance(failure, asyncio.CancelledError):
        message = "accepted SID Feed batch was cancelled unexpectedly"
        _raise_integrity_with_evidence(message, failure, cancellation)
    if cancellation is not None:
        raise failure from cancellation
    raise failure


def _raise_integrity_with_evidence(
    message: str,
    failure: BaseException,
    cancellation: asyncio.CancelledError | None,
) -> typing.NoReturn:
    """Raise integrity failure while retaining child and caller evidence."""
    if cancellation is None:
        raise grant_control.GrantControlIntegrityError(message) from failure
    try:
        _raise_deferred_failure(failure)
    except BaseException:
        raise grant_control.GrantControlIntegrityError(
            message
        ) from cancellation


def _raise_deferred_failure(failure: BaseException) -> typing.NoReturn:
    """Raise one retained child failure to establish exception context."""
    raise failure


def _raise_deferred_cancellation(
    cancellation: asyncio.CancelledError,
) -> typing.NoReturn:
    """Propagate the exact cancellation retained during settlement."""
    raise cancellation


class BcfyCallsSidRunner:
    """Run locally demultiplexed Calls pages under one exact SID grant."""

    def __init__(
        self,
        store: _LeaseStore,
        calls_provider: _CallsProvider,
        work_pool: _BatchPool,
        failure_planner: FailurePlanner,
        *,
        actor_id: str,
        poll_interval_sec: float = _POLL_INTERVAL_SEC,
        clock: collections.abc.Callable[[], datetime.datetime] | None = None,
    ) -> None:
        if poll_interval_sec < 0:
            msg = "poll_interval_sec must be nonnegative"
            raise ValueError(msg)
        self._store = store
        self._calls_provider = calls_provider
        self._work_pool = work_pool
        self._failure_planner = failure_planner
        self._actor_id = actor_id
        self._poll_interval_sec = poll_interval_sec
        self._clock = clock or (lambda: datetime.datetime.now(datetime.UTC))

    async def run(  # noqa: PLR0911, PLR0912, PLR0915
        self,
        grant: ingestion_lease_store.LeaseGrant,
        payload: grant_control.ClaimMode,
        context: grant_control.RunContext,
    ) -> grant_control.RunOutcome:
        """Poll, settle, and durably close pages until stopped or failed.

        Args:
            grant: Exact fenced SID ownership generation.
            payload: Claim mode supplied by the generic grant runtime.
            context: Cooperative stop and lost-authority signals.

        Returns:
            The terminal outcome consumed by the generic grant runtime.

        Raises:
            GrantControlIntegrityError: Durable page commit outcome is unknown.
            CancelledError: The runner task is cancelled outside a commit.
        """
        del payload
        states: dict[uuid.UUID, _FeedState] = {}
        consecutive_failures = 0

        while True:
            if context.grant_lost.is_set():
                return grant_control.RunLost()
            if context.stop_requested.is_set():
                return grant_control.RunCompleted()

            poll_started = self._clock()
            membership = await self._store.load_membership(grant)
            if isinstance(membership, ingestion_lease_store.GrantRejected):
                return grant_control.RunLost()
            if isinstance(
                membership,
                ingestion_lease_store.MembershipInvariantViolation,
            ):
                return grant_control.RunFailed(
                    feed_store.FeedStatusReason.SYSTEM_CONFIGURATION_INVALID,
                    _MEMBERSHIP_INVALID,
                )

            _prune_departed_states(states, membership.members)

            due_members = tuple(
                member
                for member in membership.members
                if member.retry_after is None
                or member.retry_after <= poll_started
            )
            if not due_members:
                await self._finish_poll_wait(poll_started, context)
                continue

            requested_position = min(
                (
                    member.last_bookmark_time
                    for member in due_members
                    if member.last_bookmark_time is not None
                ),
                default=None,
            )
            request_started = self._clock()
            try:
                page = await self._calls_provider.fetch_sid_page(
                    grant.lease_key,
                    requested_position,
                    shutdown_event=context.stop_requested,
                )
            except provider.TokenLoadStopped:
                _log_poll_settled(
                    grant,
                    status="stopped",
                    error=None,
                )
                return grant_control.RunCompleted()
            except models.FeedFailure as error:
                _log_poll_settled(
                    grant,
                    status="metadata_failed",
                    error=error.reason,
                )
                if error.status_reason not in _TRANSIENT_METADATA_FAILURES:
                    return grant_control.RunFailed(
                        error.status_reason,
                        error.reason,
                    )
                consecutive_failures += 1
                if consecutive_failures >= _MAX_CONSECUTIVE_FAILURES:
                    return grant_control.RunFailed(
                        error.status_reason,
                        error.reason,
                    )
                await self._finish_poll_wait(poll_started, context)
                continue
            except asyncio.CancelledError:
                _log_poll_settled(
                    grant,
                    status="cancelled",
                    error=None,
                )
                raise
            except Exception as error:
                _log_poll_settled(
                    grant,
                    status="integrity_failed",
                    error=type(error).__name__,
                )
                raise
            response_received_at = self._clock()

            consecutive_failures = 0
            poll_status = "integrity_failed"
            poll_error: str | None = None
            try:
                batches = self._route_page(
                    grant,
                    due_members,
                    page.calls,
                    states,
                )
                settlement = await self._work_pool.settle_batches(batches)
                result_by_feed = {
                    batch.member.identity.feed_id: result
                    for batch, result in settlement.results
                }
                for batch, result in settlement.results:
                    state = states[batch.member.identity.feed_id]
                    state.next_sequence = result.next_sequence
                    state.remember_urls(result.committed_urls)

                rejected = any(
                    isinstance(
                        result.terminal,
                        ingestion_lease_store.GrantRejected,
                    )
                    for _batch, result in settlement.results
                )
                if rejected:
                    if settlement.failure is not None:
                        _raise_settlement_failure(
                            settlement.failure,
                            settlement.cancellation,
                        )
                    poll_status = "grant_lost"
                    return grant_control.RunLost()

                complete = settlement.failure is None and len(
                    settlement.results
                ) == len(batches)
                promoted = (
                    _promoted_sid_failure(
                        tuple(result for _batch, result in settlement.results)
                    )
                    if complete
                    else None
                )

                boundary = _valid_page_boundary(
                    page.last_pos,
                    requested_position,
                    response_received_at,
                )
                if boundary is None:
                    logger.error(
                        "Broadcastify Calls SID response has invalid lastPos",
                        extra={
                            "json_fields": {
                                "event_type": (
                                    "bcfy_calls_sid_invalid_last_pos"
                                ),
                                "sid": grant.lease_key,
                            }
                        },
                    )

                mutation_result: (
                    ingestion_lease_store.BatchCommitted
                    | ingestion_lease_store.GrantRejected
                    | None
                ) = None
                commit_cancellation: asyncio.CancelledError | None = None
                if complete or settlement.results:
                    (
                        mutation_result,
                        commit_cancellation,
                    ) = await self._commit_page(
                        grant,
                        due_members,
                        frozenset(
                            batch.member.identity.feed_id for batch in batches
                        ),
                        result_by_feed,
                        boundary,
                        request_started,
                        complete=complete,
                        promoted=promoted,
                    )

                cancellation = settlement.cancellation or commit_cancellation
                if isinstance(
                    mutation_result,
                    ingestion_lease_store.GrantRejected,
                ):
                    poll_status = "grant_lost"
                    return grant_control.RunLost()

                if settlement.failure is not None:
                    _raise_settlement_failure(
                        settlement.failure,
                        cancellation,
                    )

                if promoted is not None:
                    poll_status = "sid_failed"
                    poll_error = promoted.reason
                    return grant_control.RunFailed(
                        promoted.status_reason,
                        promoted.reason,
                    )

                if cancellation is not None:
                    _raise_deferred_cancellation(cancellation)

                poll_status = "completed"
            except asyncio.CancelledError:
                poll_status = "cancelled"
                raise
            except Exception as error:
                poll_error = type(error).__name__
                raise
            finally:
                _log_poll_settled(
                    grant,
                    status=poll_status,
                    error=poll_error,
                )

            if context.grant_lost.is_set():
                return grant_control.RunLost()
            if context.stop_requested.is_set():
                return grant_control.RunCompleted()
            await self._finish_poll_wait(poll_started, context)

    def _route_page(
        self,
        grant: ingestion_lease_store.LeaseGrant,
        members: collections.abc.Sequence[ingestion_lease_store.LeaseMember],
        raw_calls: collections.abc.Sequence[object],
        states: dict[uuid.UUID, _FeedState],
    ) -> tuple[pipeline.FeedBatch, ...]:
        by_source_feed_id = {
            member.identity.source_feed_id: member for member in members
        }
        adopting_ids = frozenset(
            member.identity.feed_id
            for member in members
            if member.last_bookmark_time is None
        )
        calls_by_feed: dict[uuid.UUID, list[pipeline.CallWork]] = {}
        admitted_urls: dict[uuid.UUID, set[str]] = {}

        for raw_call in raw_calls:
            if not isinstance(raw_call, collections.abc.Mapping):
                logger.error("Ignoring malformed Broadcastify Calls item")
                continue
            call = typing.cast(
                "collections.abc.Mapping[str, object]",
                raw_call,
            )
            timestamp = _utc_timestamp(call.get("ts"))

            raw_source_feed_id = call.get("groupId")
            source_feed_id = None
            if isinstance(raw_source_feed_id, (str, int)) and not isinstance(
                raw_source_feed_id,
                bool,
            ):
                source_feed_id = str(raw_source_feed_id)
            member = (
                by_source_feed_id.get(source_feed_id)
                if source_feed_id is not None
                else None
            )
            if member is None or member.identity.feed_id in adopting_ids:
                continue
            audio_url = call.get("url")
            if not isinstance(audio_url, str) or not audio_url:
                logger.error("Ignoring Broadcastify Calls item without URL")
                continue
            state = states.setdefault(member.identity.feed_id, _FeedState())
            page_urls = admitted_urls.setdefault(
                member.identity.feed_id,
                set(),
            )
            if (
                audio_url in state.recent_committed_urls
                or audio_url in page_urls
            ):
                continue
            if (
                timestamp is not None
                and member.last_bookmark_time is not None
                and timestamp <= member.last_bookmark_time
            ):
                continue
            if timestamp is None:
                logger.error(
                    "Broadcastify Calls item has invalid ts; "
                    "progress will use chunk end time",
                    extra={
                        "json_fields": {
                            "event_type": "bcfy_calls_missing_ts",
                            "feed_id": str(member.identity.feed_id),
                            "url": audio_url,
                        }
                    },
                )
            calls_by_feed.setdefault(member.identity.feed_id, []).append(
                pipeline.CallWork(
                    payload=call,
                    audio_url=audio_url,
                )
            )
            page_urls.add(audio_url)

        batches = []
        for member in members:
            calls = calls_by_feed.get(member.identity.feed_id)
            if not calls:
                continue
            calls.sort(key=_call_order)
            state = states[member.identity.feed_id]
            batches.append(
                pipeline.FeedBatch(
                    grant=grant,
                    member=member,
                    session_id=state.session_id,
                    starting_sequence=state.next_sequence,
                    calls=tuple(calls),
                )
            )
        return tuple(batches)

    async def _commit_page(
        self,
        grant: ingestion_lease_store.LeaseGrant,
        due_members: collections.abc.Sequence[
            ingestion_lease_store.LeaseMember
        ],
        routed_feed_ids: frozenset[uuid.UUID],
        results: collections.abc.Mapping[
            uuid.UUID,
            pipeline.FeedBatchResult,
        ],
        boundary: datetime.datetime | None,
        adoption_boundary: datetime.datetime,
        *,
        complete: bool,
        promoted: failure_classification.ItemFailure | None,
    ) -> tuple[
        ingestion_lease_store.BatchCommitted
        | ingestion_lease_store.GrantRejected
        | None,
        asyncio.CancelledError | None,
    ]:
        """Commit only child evidence proven by the current page settlement.

        Complete ordinary pages may clear parent recovery state. Partial or
        SID-promoted pages retain it. Unresolved routed Feeds are omitted,
        while independently quiet or adopting Feeds may share a transaction
        already required by accepted results.

        Args:
            grant: Exact fenced SID ownership generation.
            due_members: Ingestible members included in this poll boundary.
            routed_feed_ids: Members for which the page contained work.
            results: Definitive per-Feed results keyed by immutable Feed UUID.
            boundary: Validated provider page boundary, if available.
            adoption_boundary: Local request time for newly adopted Feeds.
            complete: Whether every routed Feed batch settled definitively.
            promoted: Parent failure selected for a complete page, if any.

        Returns:
            The fenced mutation result, if a transaction was needed, plus the
            first caller cancellation deferred during that transaction.

        Raises:
            GrantControlIntegrityError: The issued transaction failed with an
                outcome that cannot safely be retried.
        """
        mutations: list[ingestion_lease_store.ChildMutation] = []
        failure_plans: dict[
            uuid.UUID,
            failure_policy.FailurePersistencePlan,
        ] = {}
        for member in due_members:
            feed_id = member.identity.feed_id
            result = results.get(feed_id)
            if result is None and feed_id in routed_feed_ids:
                continue
            terminal = result.terminal if result is not None else None
            if isinstance(
                terminal,
                failure_classification.ItemFailure,
            ):
                if promoted is not None:
                    if terminal.status_reason is _POST_BOOKMARK_FAILURE:
                        failure_telemetry.emit_post_bookmark_publish_failure(
                            logger,
                            feed_id=feed_id,
                            source_type=grant.source_type,
                            reason=(
                                terminal.reason or terminal.status_reason.value
                            ),
                        )
                    continue
                plan = self._failure_planner(
                    terminal.status_reason,
                    terminal.reason,
                )
                failure_plans[feed_id] = plan
                mutations.append(
                    ingestion_lease_store.FeedFailureTransition(
                        member=member.identity,
                        action=_storage_failure_action(plan),
                        status_reason=plan.status_reason,
                        reason=plan.reason,
                        completion_cursor=None,
                    )
                )
                continue
            observation = (
                adoption_boundary
                if member.last_bookmark_time is None
                else boundary
            )
            mutations.append(
                ingestion_lease_store.SourceObservation(
                    member.identity,
                    observation,
                )
            )

        lease_effect: ingestion_lease_store.LeaseEffect = (
            ingestion_lease_store.FinalizeLeaseRecovery()
            if complete and promoted is None
            else ingestion_lease_store.NoLeaseEffect()
        )
        if not mutations and isinstance(
            lease_effect,
            ingestion_lease_store.NoLeaseEffect,
        ):
            return (None, None)

        settlement = await retry.settle_issued_operation(
            self._store.commit_child_mutations(
                grant,
                ingestion_lease_store.ChildMutationBatch(
                    tuple(mutations),
                    lease_effect,
                ),
                actor_id=self._actor_id,
            )
        )
        if settlement.failure is not None:
            message = "SID page commit failed with outcome unknown"
            _raise_integrity_with_evidence(
                message,
                settlement.failure,
                settlement.cancellation,
            )

        committed = typing.cast(
            "_CommitResult",
            settlement.result,
        )
        if isinstance(committed, ingestion_lease_store.BatchCommitted):
            for child in committed.children:
                plan = failure_plans.get(child.feed_id)
                if (
                    plan is not None
                    and child.disposition
                    is not ingestion_lease_store.ChildDisposition.REJECTED
                ):
                    failure_telemetry.emit_failure_policy_decision(
                        logger,
                        feed_id=child.feed_id,
                        source_type=grant.source_type,
                        plan=plan,
                    )
        return (committed, settlement.cancellation)

    async def _finish_poll_wait(
        self,
        poll_started: datetime.datetime,
        context: grant_control.RunContext,
    ) -> None:
        """Wait out the poll cadence unless stop or authority loss arrives."""
        elapsed = (self._clock() - poll_started).total_seconds()
        remaining = max(0.0, self._poll_interval_sec - elapsed)
        if not remaining:
            return

        stop_wait = asyncio.create_task(context.stop_requested.wait())
        loss_wait = asyncio.create_task(context.grant_lost.wait())
        try:
            await asyncio.wait(
                (stop_wait, loss_wait),
                timeout=remaining,
                return_when=asyncio.FIRST_COMPLETED,
            )
        finally:
            stop_wait.cancel()
            loss_wait.cancel()
            await asyncio.gather(
                stop_wait,
                loss_wait,
                return_exceptions=True,
            )
