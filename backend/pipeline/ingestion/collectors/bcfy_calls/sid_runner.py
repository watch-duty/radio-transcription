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

from backend.pipeline.ingestion import failure_policy, grant_control
from backend.pipeline.ingestion.collectors.bcfy_calls import pipeline, provider
from backend.pipeline.ingestion.collectors.failure_classification import (
    ItemBatchOutcome,
    ItemFailure,
)
from backend.pipeline.ingestion.models import FeedFailure
from backend.pipeline.storage import feed_store, ingestion_lease_store

logger = logging.getLogger(__name__)

_POLL_INTERVAL_SEC = 10.0
_MAX_CONSECUTIVE_FAILURES = 10
_RECENT_URL_LIMIT = 1_000
_MEMBERSHIP_INVALID = "bcfy_calls_sid_membership_invalid"
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


class _BatchPool(typing.Protocol):
    async def submit(
        self,
        batch: pipeline.FeedBatch,
    ) -> asyncio.Future[pipeline.FeedBatchResult]:
        """Admit one Feed batch to the process-wide bounded pool."""
        ...


class _FeedState:
    """Grant-local publication and duplicate-suppression state for one Feed."""

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


def _utc_timestamp(value: object) -> datetime.datetime | None:
    """Decode a finite provider timestamp at the external trust boundary."""
    if isinstance(value, bool) or not isinstance(value, (int, float, str)):
        return None
    try:
        numeric = float(value)
        if not math.isfinite(numeric):
            return None
        return datetime.datetime.fromtimestamp(int(numeric), datetime.UTC)
    except (TypeError, ValueError, OSError, OverflowError):
        return None


def _valid_page_boundary(
    raw_last_pos: object,
    requested_position: datetime.datetime | None,
) -> datetime.datetime | None:
    boundary = _utc_timestamp(raw_last_pos)
    if boundary is None:
        return None
    if requested_position is None:
        return boundary
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


async def _settle_accepted(
    futures: collections.abc.Sequence[asyncio.Future[pipeline.FeedBatchResult]],
) -> tuple[pipeline.FeedBatchResult, ...]:
    """Settle every accepted batch before surfacing failure or cancellation."""
    if not futures:
        return ()

    settlement = asyncio.gather(*futures, return_exceptions=True)
    cancellation: asyncio.CancelledError | None = None
    while not settlement.done():
        try:
            await asyncio.shield(settlement)
        except asyncio.CancelledError as error:
            if cancellation is None:
                cancellation = error

    settled = settlement.result()
    first_error = next(
        (result for result in settled if isinstance(result, BaseException)),
        None,
    )
    if cancellation is not None:
        if first_error is not None:
            logger.error(
                "Accepted SID Feed batch failed during cancellation",
                exc_info=(
                    type(first_error),
                    first_error,
                    first_error.__traceback__,
                ),
            )
            raise cancellation from first_error
        raise cancellation
    if first_error is not None:
        raise first_error
    return typing.cast(
        "tuple[pipeline.FeedBatchResult, ...]",
        tuple(settled),
    )


class BcfyCallsSidRunner:
    """Run locally demultiplexed Calls pages under one exact SID grant."""

    def __init__(
        self,
        store: ingestion_lease_store.IngestionLeaseStore,
        calls_provider: provider.CallsProviderClient,
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
        """Poll, settle, and durably close pages until stopped or failed."""
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

            due_members = tuple(
                member
                for member in membership.members
                if member.retry_after is None
                or member.retry_after <= poll_started
            )
            if not due_members:
                await asyncio.sleep(self._poll_interval_sec)
                continue

            requested_position = min(
                (
                    member.last_bookmark_time
                    for member in due_members
                    if member.last_bookmark_time is not None
                ),
                default=None,
            )
            request_stop = asyncio.Event()
            try:
                page = await self._calls_provider.fetch_sid_page(
                    grant.lease_key,
                    requested_position,
                    shutdown_event=request_stop,
                )
            except provider.TokenLoadStopped:
                return grant_control.RunCompleted()
            except FeedFailure as error:
                self._log_poll_settled(
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
                await self._finish_poll_wait(poll_started)
                continue

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
                futures: list[asyncio.Future[pipeline.FeedBatchResult]] = []
                settlement_started = False
                try:
                    for batch in batches:
                        futures.append(await self._work_pool.submit(batch))
                    settlement_started = True
                    results = await _settle_accepted(futures)
                except asyncio.CancelledError as cancellation:
                    if not settlement_started and futures:
                        try:
                            await _settle_accepted(futures)
                        except asyncio.CancelledError:
                            pass
                        except BaseException as error:
                            logger.exception(
                                "Accepted SID Feed batch failed during "
                                "cancellation",
                            )
                            raise cancellation from error
                    raise
                except BaseException as admission_error:
                    if not settlement_started and futures:
                        try:
                            await _settle_accepted(futures)
                        except asyncio.CancelledError as cancellation:
                            raise cancellation from admission_error
                        except BaseException as error:
                            logger.exception(
                                "Accepted SID Feed batch failed after "
                                "admission stopped",
                            )
                            raise admission_error from error
                    raise

                result_by_feed = {
                    batch.member.identity.feed_id: result
                    for batch, result in zip(batches, results, strict=True)
                }
                for batch, result in zip(batches, results, strict=True):
                    state = states[batch.member.identity.feed_id]
                    state.next_sequence = result.next_sequence
                    state.remember_urls(result.committed_urls)
                    if isinstance(
                        result.terminal,
                        ingestion_lease_store.GrantRejected,
                    ):
                        poll_status = "grant_lost"
                        return grant_control.RunLost()

                boundary = _valid_page_boundary(
                    page.last_pos,
                    requested_position,
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
                promoted = self._promoted_parent_failure(results)
                try:
                    mutation_result = await self._commit_page(
                        grant,
                        due_members,
                        result_by_feed,
                        boundary,
                        promoted,
                    )
                except asyncio.CancelledError as error:
                    msg = "SID page commit cancellation left outcome unknown"
                    raise grant_control.GrantControlIntegrityError(
                        msg
                    ) from error
                except grant_control.GrantControlIntegrityError:
                    raise
                except Exception as error:
                    msg = "SID page commit failed with outcome unknown"
                    raise grant_control.GrantControlIntegrityError(
                        msg
                    ) from error

                if isinstance(
                    mutation_result,
                    ingestion_lease_store.GrantRejected,
                ):
                    poll_status = "grant_lost"
                    return grant_control.RunLost()

                if promoted is not None:
                    poll_status = "failed"
                    poll_error = promoted.reason
                    return grant_control.RunFailed(
                        promoted.status_reason,
                        promoted.reason,
                    )
                poll_status = "completed"
            except asyncio.CancelledError:
                poll_status = "cancelled"
                raise
            except Exception as error:
                poll_error = type(error).__name__
                raise
            finally:
                self._log_poll_settled(
                    grant,
                    status=poll_status,
                    error=poll_error,
                )

            if context.grant_lost.is_set():
                return grant_control.RunLost()
            if context.stop_requested.is_set():
                return grant_control.RunCompleted()
            await self._finish_poll_wait(poll_started)

    def _route_page(
        self,
        grant: ingestion_lease_store.LeaseGrant,
        members: collections.abc.Sequence[ingestion_lease_store.LeaseMember],
        raw_calls: collections.abc.Sequence[object],
        states: dict[uuid.UUID, _FeedState],
    ) -> tuple[pipeline.FeedBatch, ...]:
        by_group = {
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

            raw_group_id = call.get("groupId")
            group_id = (
                str(raw_group_id)
                if isinstance(raw_group_id, (str, int))
                and not isinstance(raw_group_id, bool)
                else None
            )
            member = by_group.get(group_id) if group_id is not None else None
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

    @staticmethod
    def _promoted_parent_failure(
        results: collections.abc.Sequence[pipeline.FeedBatchResult],
    ) -> ItemFailure | None:
        participants = [
            result for result in results if result.attempted_count > 0
        ]
        if len(participants) < 2:
            return None
        outcome = ItemBatchOutcome()
        for result in participants:
            outcome.record_attempt()
            if result.published_count > 0:
                outcome.record_chunk_produced()
            if isinstance(result.terminal, ItemFailure):
                outcome.record_failure(result.terminal)
        return outcome.promoted_failure()

    async def _commit_page(
        self,
        grant: ingestion_lease_store.LeaseGrant,
        due_members: collections.abc.Sequence[
            ingestion_lease_store.LeaseMember
        ],
        results: collections.abc.Mapping[
            uuid.UUID,
            pipeline.FeedBatchResult,
        ],
        boundary: datetime.datetime | None,
        promoted: ItemFailure | None,
    ) -> (
        ingestion_lease_store.BatchCommitted
        | ingestion_lease_store.GrantRejected
    ):
        mutations: list[ingestion_lease_store.ChildMutation] = []
        for member in due_members:
            feed_id = member.identity.feed_id
            result = results.get(feed_id)
            if promoted is not None and result is not None:
                continue
            terminal = result.terminal if result is not None else None
            if isinstance(terminal, ItemFailure):
                plan = self._failure_planner(
                    terminal.status_reason,
                    terminal.reason,
                )
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
            if boundary is not None:
                mutations.append(
                    ingestion_lease_store.SourceObservation(
                        member.identity,
                        boundary,
                    )
                )

        lease_effect: ingestion_lease_store.LeaseEffect
        if promoted is None:
            lease_effect = ingestion_lease_store.FinalizeLeaseRecovery()
        else:
            lease_effect = ingestion_lease_store.NoLeaseEffect()
        return await self._store.commit_child_mutations(
            grant,
            ingestion_lease_store.ChildMutationBatch(
                tuple(mutations),
                lease_effect,
            ),
            actor_id=self._actor_id,
        )

    async def _finish_poll_wait(
        self,
        poll_started: datetime.datetime,
    ) -> None:
        elapsed = (self._clock() - poll_started).total_seconds()
        await asyncio.sleep(max(0.0, self._poll_interval_sec - elapsed))

    @staticmethod
    def _log_poll_settled(
        grant: ingestion_lease_store.LeaseGrant,
        *,
        status: str,
        error: str | None,
    ) -> None:
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
