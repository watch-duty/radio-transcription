"""Exact-grant Broadcastify Calls SID acquisition and local routing."""

from __future__ import annotations

import asyncio
import collections
import collections.abc
import dataclasses
import datetime
import enum
import logging
import math
import time
import types
import typing
import uuid

import asyncpg

from backend.pipeline.ingestion import (
    feed_work_scheduler,
    grant_control,
    sid_grant_control,
)
from backend.pipeline.ingestion import models as ingestion_models
from backend.pipeline.ingestion.collectors import aiohttp_requests, control_flow
from backend.pipeline.ingestion.collectors.bcfy_calls import (
    boundary_verdict,
    cursor_policy,
    gap_ledger,
    provider,
    runtime_adapters,
    telemetry,
)
from backend.pipeline.ingestion.collectors.bcfy_calls import (
    pipeline as cohort_pipeline,
)
from backend.pipeline.storage import feed_store, ingestion_lease_store

logger = logging.getLogger(__name__)

_RECENT_URL_LIMIT = 1_000
_POLL_INTERVAL_SEC = 10.0
_MAX_CONSECUTIVE_FAILURES = 10
_TRANSIENT_PROVIDER_FAILURES = frozenset(
    {
        feed_store.FeedStatusReason.SOURCE_RATE_LIMITED,
        feed_store.FeedStatusReason.SOURCE_UNREACHABLE,
        feed_store.FeedStatusReason.SYSTEM_AUTHENTICATION_FAILED,
    }
)
_MEMBERSHIP_UNCERTAINTY_ERRORS = (
    asyncpg.PostgresConnectionError,
    asyncpg.InterfaceError,
    OSError,
)


class MemberRouteState(enum.StrEnum):
    """Closed grant-local routing state for one eligible Feed."""

    NORMAL = "normal"
    ADOPTING = "adopting"
    REPLAY_PENDING = "replay_pending"
    RETIRED = "retired"


class RunnerCancellationCause(enum.StrEnum):
    """Closed cause selected by the sole raw-runner cancellation writer."""

    RAW_RUNNER_CANCELLATION = "raw_runner_cancellation"
    EXACT_GRANT_LOSS = "exact_grant_loss"


@dataclasses.dataclass(slots=True)
class ProcessorCancellationHandoff:
    """Invocation-local once-only cancellation evidence for a processor."""

    grant: ingestion_lease_store.LeaseGrant
    _cause: RunnerCancellationCause | None = dataclasses.field(
        default=None,
        init=False,
        repr=False,
    )
    _cancelled_error: asyncio.CancelledError | None = dataclasses.field(
        default=None,
        init=False,
        repr=False,
    )

    def __post_init__(self) -> None:
        if not isinstance(self.grant, ingestion_lease_store.LeaseGrant):
            message = "grant must be a LeaseGrant"
            raise TypeError(message)

    @property
    def cause(self) -> RunnerCancellationCause | None:
        """Return the runner-selected cause without mutation authority."""
        return self._cause

    @property
    def cancelled_error(self) -> asyncio.CancelledError | None:
        """Return the first exact raw-runner cancellation object."""
        return self._cancelled_error

    def _record(
        self,
        cause: RunnerCancellationCause,
        cancelled_error: asyncio.CancelledError,
    ) -> None:
        """Record the first value; only the source runner invokes this."""
        if self._cause is None:
            self._cause = cause
            self._cancelled_error = cancelled_error
            return
        if self._cause is cause and self._cancelled_error is cancelled_error:
            return
        message = "processor cancellation handoff was crossed or replaced"
        raise feed_work_scheduler.CohortIntegrityError(message)


@dataclasses.dataclass(slots=True)
class _MemberState:
    """Mutable grant-local coverage and route state for one sealed member."""

    member: ingestion_lease_store.LeaseMember
    effective_cursor: datetime.datetime | None
    route_state: MemberRouteState

    @property
    def feed_id(self) -> uuid.UUID:
        """Return the authoritative immutable Feed UUID."""
        return self.member.identity.feed_id


class SidProcessorFailure(RuntimeError):
    """Typed terminal SID processor failure for runner classification."""

    def __init__(
        self,
        status_reason: feed_store.FeedStatusReason,
        reason: str,
    ) -> None:
        if not isinstance(status_reason, feed_store.FeedStatusReason):
            message = "status_reason must be a FeedStatusReason"
            raise TypeError(message)
        if not isinstance(reason, str) or not reason:
            message = "reason must be a nonempty string"
            raise ValueError(message)
        self.status_reason = status_reason
        self.reason = reason
        super().__init__(reason)


class SidProcessorAuthorityLost(RuntimeError):
    """The exact Lease grant no longer authorizes membership refresh."""

    def __init__(
        self,
        rejection: ingestion_lease_store.GrantRejected,
    ) -> None:
        if not isinstance(rejection, ingestion_lease_store.GrantRejected):
            message = "rejection must be a GrantRejected"
            raise TypeError(message)
        self.rejection = rejection
        super().__init__(f"bcfy_calls_sid_authority_lost:{rejection.reason}")


class SidProcessorPlannedDrain(RuntimeError):
    """A retired Feed UUID reappeared and requires a successor lane."""

    def __init__(self, feed_id: uuid.UUID) -> None:
        if not isinstance(feed_id, uuid.UUID):
            message = "feed_id must be a UUID"
            raise TypeError(message)
        self.feed_id = feed_id
        super().__init__("bcfy_calls_sid_member_reactivated")


class SidProcessorUndrained(RuntimeError):
    """The page retained uncertain work and cannot begin another poll.

    Attributes:
        result: Exact scheduler result retaining the page's work and permits.
    """

    def __init__(self, result: feed_work_scheduler.Undrained) -> None:
        if not isinstance(result, feed_work_scheduler.Undrained):
            message = "result must be an Undrained"
            raise TypeError(message)
        self.result = result
        super().__init__("bcfy_calls_sid_page_undrained")


class _MembershipRefreshUncertain(RuntimeError):
    """The store could not prove one authoritative refresh result."""


class _MembershipStore(typing.Protocol):
    """Narrow atomic membership refresh seam."""

    async def refresh_membership(
        self,
        grant: ingestion_lease_store.LeaseGrant,
        *,
        known_revision: int | None,
    ) -> ingestion_lease_store.MembershipRefreshResult:
        """Return unchanged proof or one complete exact-grant snapshot."""
        ...


class _CallsProvider(typing.Protocol):
    """Narrow shared Calls metadata provider seam."""

    async def fetch_sid_page(
        self,
        sid: str,
        pos: datetime.datetime | None,
        *,
        subject_id: object,
        shutdown_event: typing.Any,
        attempt_observer: (aiohttp_requests.HttpAttemptObserver | None) = None,
    ) -> provider.CallsPageEnvelope:
        """Fetch one validated top-level SID page."""
        ...


class _GrantLane(typing.Protocol):
    """Exact-grant scheduler operations used by the processor."""

    async def remove_feed(
        self,
        feed_id: uuid.UUID,
    ) -> feed_work_scheduler.FeedRemoved:
        """Retire one Feed without affecting siblings."""
        ...

    async def is_feed_retired(self, feed_id: uuid.UUID) -> bool:
        """Return exact lane-local retirement for one Feed.

        Args:
            feed_id: Immutable Feed UUID within this lane's complete grant.

        Returns:
            Whether the scheduler has retired this exact Feed scope.
        """
        ...

    async def cover_page(
        self,
        *,
        calls: collections.abc.Iterable[feed_work_scheduler.CohortSubmission],
        boundaries: collections.abc.Iterable[feed_work_scheduler.BoundaryWork],
        candidate: cursor_policy.PageCandidate,
        evidence_observer: (
            collections.abc.Callable[
                [feed_work_scheduler.SchedulerPageEvidence],
                None,
            ]
            | None
        ) = None,
    ) -> feed_work_scheduler.SettledPage | feed_work_scheduler.Undrained:
        """Settle one bounded progress or no-progress page."""
        ...


type _Now = collections.abc.Callable[[], datetime.datetime]
type _Monotonic = collections.abc.Callable[[], float]
type _PollEmitter = collections.abc.Callable[[telemetry.SidPollEvidence], None]
type _ReplayFloorEmitter = collections.abc.Callable[
    [telemetry.ReplayWindowTruncatedEvent],
    None,
]
type _SessionIdFactory = collections.abc.Callable[[], str]
type _Wait = collections.abc.Callable[
    [asyncio.Event, float],
    collections.abc.Awaitable[None],
]


@dataclasses.dataclass(frozen=True, slots=True)
class _ValidatedCall:
    """One independently validated provider item before route filters."""

    source_order: int
    state: _MemberState
    audio_url: str
    source_timestamp: datetime.datetime | None
    sort_timestamp: float
    raw_call: collections.abc.Mapping[str, object]


@dataclasses.dataclass(frozen=True, slots=True)
class _PageRequest:
    """Frozen request position and page membership participation."""

    pos: datetime.datetime | None
    replay_feed_ids: frozenset[uuid.UUID]
    snapshot: ingestion_lease_store.MembershipSnapshot
    routes: collections.abc.Mapping[str, _MemberState]


@dataclasses.dataclass(slots=True)
class _SidPollAccumulator:
    """Mutable owner-sourced scalars for one started logical poll.

    Attributes:
        grant: Complete immutable Lease generation described by this poll.
        started_monotonic: Owner clock sample defining logical-poll start.
    """

    grant: ingestion_lease_store.LeaseGrant
    started_monotonic: float
    membership_revision: int | None = None
    request_pos: int | None = None
    lease_cursor_lag_seconds: float | None = None
    maximum_feed_cursor_lag_seconds: float | None = None
    null_feed_cursor_count: int = 0
    provider_observed: bool = False
    http_attempt_count: int = 0
    response_row_count: int | None = None
    response_byte_count: int | None = None
    response_distinct_audio_url_count: int | None = None
    response_last_pos: int | None = None
    response_last_pos_state: telemetry.SidPollResponseLastPosState = (
        telemetry.SidPollResponseLastPosState.NOT_OBSERVED
    )
    matched_call_count: int = 0
    deduplicated_call_count: int = 0
    scheduler_evidence: feed_work_scheduler.SchedulerPageEvidence | None = None
    participating_feed_count: int = 0
    successful_feed_count: int = 0
    failed_feed_count: int = 0
    item_to_feed_promotion_count: int = 0
    lifecycle_scope: telemetry.SidPollLifecycleScope = (
        telemetry.SidPollLifecycleScope.NONE
    )
    lifecycle_reason: str | None = None
    _closed: bool = dataclasses.field(default=False, init=False, repr=False)

    def __post_init__(self) -> None:
        if not isinstance(self.grant, ingestion_lease_store.LeaseGrant):
            message = "grant must be a LeaseGrant"
            raise TypeError(message)
        if (
            isinstance(self.started_monotonic, bool)
            or not isinstance(self.started_monotonic, (int, float))
            or not math.isfinite(self.started_monotonic)
        ):
            message = "started_monotonic must be finite"
            raise ValueError(message)

    @property
    def closed(self) -> bool:
        """Return whether this exact accumulator has emitted or attempted."""
        return self._closed

    def observe_membership(
        self,
        snapshot: ingestion_lease_store.MembershipSnapshot,
        *,
        lease_pos: datetime.datetime | None,
        observed_at: datetime.datetime,
    ) -> None:
        """Capture revision and bounded Cursor lag from one exact snapshot."""
        if snapshot.grant != self.grant:
            message = "poll membership crossed complete grant"
            raise feed_work_scheduler.CohortIntegrityError(message)
        self.membership_revision = snapshot.membership_revision
        self.lease_cursor_lag_seconds = _cursor_lag_seconds(
            lease_pos,
            observed_at,
        )
        cursor_lags = []
        null_count = 0
        for member in snapshot.members:
            lag = _cursor_lag_seconds(
                member.last_bookmark_time,
                observed_at,
            )
            if lag is None:
                null_count += 1
            else:
                cursor_lags.append(lag)
        self.maximum_feed_cursor_lag_seconds = (
            max(cursor_lags) if cursor_lags else None
        )
        self.null_feed_cursor_count = null_count

    def observe_request(self, pos: datetime.datetime | None) -> None:
        """Capture the exact provider wire position without inventing one."""
        self.request_pos = None if pos is None else int(pos.timestamp())

    def observe_http_attempt(
        self,
        kind: aiohttp_requests.HttpAttemptKind,
        ordinal: int,
    ) -> None:
        """Capture actual JSON attempt ordinals from the transport owner."""
        if kind is not aiohttp_requests.HttpAttemptKind.JSON:
            return
        if isinstance(ordinal, bool):
            return
        self.http_attempt_count = max(self.http_attempt_count, ordinal)

    def observe_provider(self, page: provider.CallsPageEnvelope) -> None:
        """Capture immutable accepted-response evidence from the provider."""
        self.provider_observed = True
        self.http_attempt_count = page.http_attempt_count
        self.response_row_count = page.response_row_count
        self.response_byte_count = page.response_byte_count
        self.response_distinct_audio_url_count = (
            page.response_distinct_audio_url_count
        )

    def observe_response_boundary(
        self,
        state: telemetry.SidPollResponseLastPosState,
        value: datetime.datetime | None,
    ) -> None:
        """Capture one normalized boundary state, never its malformed input."""
        self.response_last_pos_state = state
        self.response_last_pos = (
            int(value.timestamp())
            if state is telemetry.SidPollResponseLastPosState.VALID
            and value is not None
            else None
        )

    def observe_routing(
        self,
        *,
        matched_call_count: int,
        deduplicated_call_count: int,
    ) -> None:
        """Capture processor-owned routing and exact-URL deduplication."""
        self.matched_call_count = matched_call_count
        self.deduplicated_call_count = deduplicated_call_count

    def observe_scheduler(
        self,
        evidence: feed_work_scheduler.SchedulerPageEvidence,
    ) -> None:
        """Capture the exact page-local scheduler evidence once."""
        if self.scheduler_evidence is not None and (
            self.scheduler_evidence is not evidence
            and self.scheduler_evidence != evidence
        ):
            message = "poll received crossed scheduler evidence"
            raise feed_work_scheduler.CohortIntegrityError(message)
        self.scheduler_evidence = evidence

    def observe_source_evidence(
        self,
        evidence: runtime_adapters.PageFinalizationEvidence,
    ) -> None:
        """Copy only finalizer-owned counts, scope, and canonical reason."""
        verdict = evidence.verdict
        self.participating_feed_count = verdict.participating_feed_count
        self.successful_feed_count = verdict.successful_feed_count
        self.failed_feed_count = verdict.failed_feed_count
        self.item_to_feed_promotion_count = (
            evidence.item_to_feed_promotion_count
        )
        if verdict.failed_feed_count == 0:
            return
        if (
            verdict.selected_scope
            is boundary_verdict.NewlyChargedScope.LEASE_ONLY
        ):
            failure = verdict.lease_failure
            if failure is None:
                message = "Lease-only verdict lacks exact failure evidence"
                raise feed_work_scheduler.CohortIntegrityError(message)
            self.lifecycle_scope = telemetry.SidPollLifecycleScope.LEASE_ONLY
            self.lifecycle_reason = failure.status_reason.value
            return
        self.lifecycle_scope = telemetry.SidPollLifecycleScope.FAILED_FEEDS
        reasons = {
            failure.failure.status_reason for failure in verdict.failed_feeds
        }
        selected_reason = (
            next(iter(reasons))
            if len(reasons) == 1
            else feed_store.FeedStatusReason.SYSTEM_COLLECTOR_ERROR
        )
        self.lifecycle_reason = selected_reason.value

    def close(
        self,
        outcome: telemetry.SidPollOutcome,
        *,
        finished_monotonic: float,
        emitter: _PollEmitter,
    ) -> telemetry.SidPollEvidence:
        """Freeze and emit this exact poll once.

        Raises:
            CohortIntegrityError: The accumulator was already closed.
            ValueError: The owner monotonic clock moved backwards.
        """
        if self._closed:
            message = "SID poll accumulator already closed"
            raise feed_work_scheduler.CohortIntegrityError(message)
        self._closed = True
        if (
            isinstance(finished_monotonic, bool)
            or not isinstance(finished_monotonic, (int, float))
            or not math.isfinite(finished_monotonic)
            or finished_monotonic < self.started_monotonic
        ):
            message = "finished_monotonic must not precede poll start"
            raise ValueError(message)
        event = self._event(
            outcome,
            duration_seconds=(finished_monotonic - self.started_monotonic),
        )
        try:
            emitter(event)
        except (KeyboardInterrupt, SystemExit):
            raise
        except BaseException:
            return event
        return event

    def _event(
        self,
        outcome: telemetry.SidPollOutcome,
        *,
        duration_seconds: float,
    ) -> telemetry.SidPollEvidence:
        scheduler = self.scheduler_evidence
        scheduler_fields = _scheduler_event_fields(scheduler)
        lifecycle_scope = self.lifecycle_scope
        lifecycle_reason = self.lifecycle_reason
        if outcome in {
            telemetry.SidPollOutcome.STOPPED,
            telemetry.SidPollOutcome.AUTHORITY_LOST,
        }:
            lifecycle_scope = telemetry.SidPollLifecycleScope.NONE
            lifecycle_reason = None
        return telemetry.SidPollEvidence(
            sid=self.grant.lease_key,
            source_type=self.grant.source_type.value,
            owner_worker_id=str(self.grant.owner_worker_id),
            fencing_token=self.grant.fencing_token,
            membership_revision=self.membership_revision,
            outcome=outcome,
            duration_seconds=duration_seconds,
            provider_observed=self.provider_observed,
            http_attempt_count=self.http_attempt_count,
            response_row_count=self.response_row_count,
            response_byte_count=self.response_byte_count,
            response_distinct_audio_url_count=(
                self.response_distinct_audio_url_count
            ),
            request_pos=self.request_pos,
            response_last_pos=self.response_last_pos,
            response_last_pos_state=self.response_last_pos_state,
            matched_call_count=self.matched_call_count,
            deduplicated_call_count=self.deduplicated_call_count,
            admitted_record_count=scheduler_fields.admitted_record_count,
            admitted_cohort_count=scheduler_fields.admitted_cohort_count,
            lease_cursor_lag_seconds=self.lease_cursor_lag_seconds,
            maximum_feed_cursor_lag_seconds=(
                self.maximum_feed_cursor_lag_seconds
            ),
            null_feed_cursor_count=self.null_feed_cursor_count,
            participating_feed_count=self.participating_feed_count,
            successful_feed_count=self.successful_feed_count,
            failed_feed_count=self.failed_feed_count,
            item_to_feed_promotion_count=(self.item_to_feed_promotion_count),
            lifecycle_scope=lifecycle_scope,
            lifecycle_reason=lifecycle_reason,
            total_queue_wait_seconds=(
                scheduler_fields.total_queue_wait_seconds
            ),
            maximum_queue_wait_seconds=(
                scheduler_fields.maximum_queue_wait_seconds
            ),
            oldest_queue_age_seconds=(
                scheduler_fields.oldest_queue_age_seconds
            ),
            pressure_encountered=scheduler_fields.pressure_encountered,
            pressure_wait_count=scheduler_fields.pressure_wait_count,
            pressure_wait_seconds=scheduler_fields.pressure_wait_seconds,
            maximum_held_count=scheduler_fields.maximum_held_count,
            maximum_queue_depth=scheduler_fields.maximum_queue_depth,
            maximum_worker_utilization_numerator=(
                scheduler_fields.maximum_worker_utilization_numerator
            ),
            worker_utilization_denominator=(
                scheduler_fields.worker_utilization_denominator
            ),
            early_flush_attempt_count=(
                scheduler_fields.early_flush_attempt_count
            ),
            final_flush_attempt_count=(
                scheduler_fields.final_flush_attempt_count
            ),
            total_flush_latency_seconds=(
                scheduler_fields.total_flush_latency_seconds
            ),
            maximum_flush_latency_seconds=(
                scheduler_fields.maximum_flush_latency_seconds
            ),
            fence_rejection_count=scheduler_fields.fence_rejection_count,
            membership_rejection_count=(
                scheduler_fields.member_rejection_count
            ),
            terminal_record_count=scheduler_fields.terminal_record_count,
            replay_blocked_record_count=(
                scheduler_fields.replay_blocked_record_count
            ),
        )


@dataclasses.dataclass(frozen=True, slots=True)
class _PollSchedulerFields:
    """Exact scalar projection of optional scheduler page evidence."""

    admitted_record_count: int = 0
    admitted_cohort_count: int = 0
    terminal_record_count: int = 0
    replay_blocked_record_count: int = 0
    total_queue_wait_seconds: float = 0.0
    maximum_queue_wait_seconds: float = 0.0
    oldest_queue_age_seconds: float = 0.0
    pressure_encountered: bool = False
    pressure_wait_count: int = 0
    pressure_wait_seconds: float = 0.0
    maximum_held_count: int = 0
    maximum_queue_depth: int = 0
    maximum_worker_utilization_numerator: int = 0
    worker_utilization_denominator: int = 0
    early_flush_attempt_count: int = 0
    final_flush_attempt_count: int = 0
    total_flush_latency_seconds: float = 0.0
    maximum_flush_latency_seconds: float = 0.0
    fence_rejection_count: int = 0
    member_rejection_count: int = 0


def _scheduler_event_fields(
    evidence: feed_work_scheduler.SchedulerPageEvidence | None,
) -> _PollSchedulerFields:
    """Copy scheduler-owner scalars without reconstructing from snapshots."""
    if evidence is None:
        return _PollSchedulerFields()
    return _PollSchedulerFields(
        admitted_record_count=evidence.admitted_record_count,
        admitted_cohort_count=evidence.admitted_cohort_count,
        terminal_record_count=evidence.terminal_record_count,
        replay_blocked_record_count=evidence.replay_blocked_record_count,
        total_queue_wait_seconds=evidence.total_queue_wait_seconds,
        maximum_queue_wait_seconds=evidence.maximum_queue_wait_seconds,
        oldest_queue_age_seconds=evidence.oldest_queue_age_seconds,
        pressure_encountered=evidence.pressure_encountered,
        pressure_wait_count=evidence.pressure_wait_count,
        pressure_wait_seconds=evidence.pressure_wait_seconds,
        maximum_held_count=evidence.maximum_held_count,
        maximum_queue_depth=evidence.maximum_queue_depth,
        maximum_worker_utilization_numerator=(
            evidence.maximum_worker_utilization_numerator
        ),
        worker_utilization_denominator=(
            evidence.worker_utilization_denominator
        ),
        early_flush_attempt_count=evidence.early_flush_attempt_count,
        final_flush_attempt_count=evidence.final_flush_attempt_count,
        total_flush_latency_seconds=evidence.total_flush_latency_seconds,
        maximum_flush_latency_seconds=evidence.maximum_flush_latency_seconds,
        fence_rejection_count=evidence.fence_rejection_count,
        member_rejection_count=evidence.member_rejection_count,
    )


def _cursor_lag_seconds(
    cursor: datetime.datetime | None,
    observed_at: datetime.datetime,
) -> float | None:
    """Return a nonnegative lag from one UTC owner observation."""
    observed = _require_utc_datetime(observed_at, field_name="observed_at")
    if cursor is None:
        return None
    value = _require_utc_datetime(cursor, field_name="cursor")
    return max(0.0, (observed - value).total_seconds())


@dataclasses.dataclass(frozen=True, slots=True)
class _PreparedPageWork:
    """One page ledger plus its exact immutable cohort stream."""

    ledger: gap_ledger.MissingCallLedger
    cohorts: tuple[feed_work_scheduler.CohortSubmission, ...]
    entries: tuple[cohort_pipeline.ScheduledCohortEntry, ...]
    entry_feed_ids: tuple[uuid.UUID, ...]
    matched_call_count: int
    deduplicated_call_count: int


def _page_obligations_are_released(prepared: _PreparedPageWork) -> bool:
    """Return whether known abort cleanup left no retained obligation."""
    closed = {
        gap_ledger.MissingCallState.RESOLVED,
        gap_ledger.MissingCallState.EMITTED,
        gap_ledger.MissingCallState.REPLAY_RELEASED,
    }
    return all(
        prepared.ledger.state(entry.obligation) in closed
        for entry in prepared.entries
    )


def _rollback_pending_urls(
    pending_by_url: collections.abc.MutableMapping[str, uuid.UUID],
    audio_urls: tuple[str, ...],
    feed_id: uuid.UUID,
) -> None:
    """Remove only this synchronous admission's exact staged URL bindings."""
    for audio_url in reversed(audio_urls):
        if pending_by_url.get(audio_url) == feed_id:
            del pending_by_url[audio_url]


def _new_session_id() -> str:
    """Return one bounded invocation-local connection session identifier."""
    return str(uuid.uuid4())


class BcfyCallsSidProcessor:
    """Own one exact grant's membership, cursor, routing, and URL state."""

    def __init__(
        self,
        grant: ingestion_lease_store.LeaseGrant,
        claim_payload: sid_grant_control.SidClaimPayload,
        membership_store: _MembershipStore,
        calls_provider: _CallsProvider,
        lane: _GrantLane,
        *,
        now: _Now,
        monotonic: _Monotonic = time.monotonic,
        poll_emitter: _PollEmitter = telemetry.emit_sid_poll,
        replay_floor_emitter: _ReplayFloorEmitter = (
            telemetry.emit_replay_window_truncated
        ),
        wait: _Wait = control_flow.sleep_or_cancel,
        session_id_factory: _SessionIdFactory = _new_session_id,
    ) -> None:
        if not isinstance(grant, ingestion_lease_store.LeaseGrant):
            message = "grant must be a LeaseGrant"
            raise TypeError(message)
        if grant.source_type is not feed_store.SourceType.BCFY_CALLS:
            message = "SID processor supports only bcfy_calls grants"
            raise ValueError(message)
        if type(claim_payload) is not sid_grant_control.SidClaimPayload:
            message = "claim_payload must be an exact SidClaimPayload"
            raise TypeError(message)
        for name, value in (
            ("now", now),
            ("monotonic", monotonic),
            ("poll_emitter", poll_emitter),
            ("replay_floor_emitter", replay_floor_emitter),
            ("wait", wait),
            ("session_id_factory", session_id_factory),
        ):
            if not callable(value):
                message = f"{name} must be callable"
                raise TypeError(message)
        self._grant = grant
        self._claim_payload = claim_payload
        self._membership_store = membership_store
        self._provider = calls_provider
        self._lane = lane
        self._now = now
        self._monotonic = monotonic
        self._poll_emitter = poll_emitter
        self._replay_floor_emitter = replay_floor_emitter
        self._wait = wait
        self._session_id_factory = session_id_factory
        self._lease_cursor: cursor_policy.LeaseCursor | None = None
        self._snapshot: ingestion_lease_store.MembershipSnapshot | None = None
        self._members_by_source: dict[str, _MemberState] = {}
        self._members_by_id: dict[uuid.UUID, _MemberState] = {}
        self._retired_feed_ids: set[uuid.UUID] = set()
        self._pending_by_url: dict[str, uuid.UUID] = {}
        self._recent_order: collections.deque[tuple[str, uuid.UUID]] = (
            collections.deque(maxlen=_RECENT_URL_LIMIT)
        )
        self._recent_urls: set[str] = set()
        self._session_by_member: dict[
            int,
            tuple[ingestion_lease_store.LeaseMemberIdentity, str],
        ] = {}
        self._member_by_session: dict[
            str,
            ingestion_lease_store.LeaseMemberIdentity,
        ] = {}
        self._active_page_ledger: gap_ledger.MissingCallLedger | None = None
        self._active_poll: _SidPollAccumulator | None = None
        self._initial_floor_cause: cursor_policy.ReplayFloorCause | None = None
        self._initial_requested_start: datetime.datetime | None = None
        self._initial_request_pending = False
        self._consecutive_failures = 0

    async def run(  # noqa: PLR0912, PLR0915
        self,
        stop_requested: asyncio.Event,
        *,
        signals: feed_work_scheduler.LaneSignalView | None = None,
        cancellation_handoff: ProcessorCancellationHandoff | None = None,
    ) -> None:
        """Run completion-paced logical polls with exact terminal controls."""
        if signals is None:
            signals = feed_work_scheduler.LaneSignalView(
                self._grant,
                stop_requested,
                asyncio.Event(),
            )
        if signals.grant != self._grant:
            message = "lane signals crossed processor grant"
            raise feed_work_scheduler.CohortIntegrityError(message)
        if cancellation_handoff is None:
            cancellation_handoff = ProcessorCancellationHandoff(self._grant)
        if cancellation_handoff.grant != self._grant:
            message = "cancellation handoff crossed processor grant"
            raise feed_work_scheduler.CohortIntegrityError(message)
        try:
            while not self._terminal_requested(stop_requested, signals):
                poll = self._begin_poll()
                retry_failure: (
                    tuple[feed_store.FeedStatusReason, str] | None
                ) = None
                try:
                    try:
                        snapshot = await self._refresh_membership()
                    except _MembershipRefreshUncertain:
                        self._finish_poll(
                            poll,
                            telemetry.SidPollOutcome.MEMBERSHIP_UNCERTAIN,
                        )
                        retry_failure = (
                            feed_store.FeedStatusReason.SYSTEM_COLLECTOR_ERROR,
                            "bcfy_calls_sid_membership_refresh_failed",
                        )
                    else:
                        lease_cursor = self._lease_cursor
                        if lease_cursor is None:
                            message = "membership refresh lost Lease Cursor"
                            raise RuntimeError(message)
                        poll.observe_membership(
                            snapshot,
                            lease_pos=lease_cursor.pos,
                            observed_at=_require_utc_datetime(
                                self._now(),
                                field_name="now",
                            ),
                        )
                        if self._terminal_requested(stop_requested, signals):
                            self._finish_poll(
                                poll,
                                self._terminal_poll_outcome(
                                    signals,
                                    cancellation_handoff,
                                ),
                            )
                            return
                        if not self._members_by_id:
                            self._finish_poll(
                                poll,
                                telemetry.SidPollOutcome.SUCCESS,
                            )
                        else:
                            request = self._select_request_position()
                            poll.observe_request(request.pos)
                            if self._terminal_requested(
                                stop_requested,
                                signals,
                            ):
                                self._finish_poll(
                                    poll,
                                    self._terminal_poll_outcome(
                                        signals,
                                        cancellation_handoff,
                                    ),
                                )
                                return
                            try:
                                page = await self._provider.fetch_sid_page(
                                    self._grant.lease_key,
                                    request.pos,
                                    subject_id=self._grant.lease_key,
                                    shutdown_event=stop_requested,
                                    attempt_observer=(
                                        poll.observe_http_attempt
                                    ),
                                )
                            except ingestion_models.FeedFailure as exc:
                                self._finish_poll(
                                    poll,
                                    telemetry.SidPollOutcome.PROVIDER_FAILED,
                                )
                                if (
                                    exc.status_reason
                                    not in _TRANSIENT_PROVIDER_FAILURES
                                ):
                                    raise SidProcessorFailure(
                                        exc.status_reason,
                                        exc.reason,
                                    ) from exc
                                retry_failure = (
                                    exc.status_reason,
                                    exc.reason,
                                )
                            else:
                                poll.observe_provider(page)
                                await self._settle_page(
                                    page,
                                    request=request,
                                    poll=poll,
                                )
                                self._consecutive_failures = 0
                                self._finish_poll(
                                    poll,
                                    telemetry.SidPollOutcome.SUCCESS,
                                )
                except asyncio.CancelledError:
                    if not poll.closed:
                        self._finish_poll(
                            poll,
                            self._terminal_poll_outcome(
                                signals,
                                cancellation_handoff,
                            ),
                        )
                    raise
                except SidProcessorAuthorityLost:
                    if not poll.closed:
                        self._finish_poll(
                            poll,
                            telemetry.SidPollOutcome.AUTHORITY_LOST,
                        )
                    raise
                except SidProcessorPlannedDrain:
                    if not poll.closed:
                        self._finish_poll(
                            poll,
                            telemetry.SidPollOutcome.STOPPED,
                        )
                    raise
                except SidProcessorFailure as exc:
                    if not poll.closed:
                        outcome = telemetry.SidPollOutcome.PAGE_FAILED
                        if exc.reason == "bcfy_calls_sid_membership_invalid":
                            outcome = (
                                telemetry.SidPollOutcome.MEMBERSHIP_INVALID
                            )
                        self._finish_poll(poll, outcome)
                    raise
                except BaseException:
                    if not poll.closed:
                        self._finish_poll(
                            poll,
                            telemetry.SidPollOutcome.PAGE_FAILED,
                        )
                    raise
                finally:
                    if poll.closed and self._active_poll is poll:
                        self._active_poll = None

                if self._terminal_requested(stop_requested, signals):
                    return
                if retry_failure is not None:
                    status_reason, reason = retry_failure
                    await self._record_logical_failure(
                        stop_requested,
                        status_reason,
                        reason,
                    )
                    continue
                await self._wait(stop_requested, _POLL_INTERVAL_SEC)
        finally:
            self.close()

    def _begin_poll(self) -> _SidPollAccumulator:
        """Create the sole accumulator immediately before membership work."""
        if self._active_poll is not None:
            message = "processor already owns a started logical poll"
            raise feed_work_scheduler.CohortIntegrityError(message)
        poll = _SidPollAccumulator(
            self._grant,
            self._monotonic(),
        )
        self._active_poll = poll
        return poll

    def _finish_poll(
        self,
        poll: _SidPollAccumulator,
        outcome: telemetry.SidPollOutcome,
    ) -> None:
        """Close one owner accumulator without changing source behavior."""
        if self._active_poll is not poll:
            return
        try:
            poll.close(
                outcome,
                finished_monotonic=self._monotonic(),
                emitter=self._poll_emitter,
            )
        except (KeyboardInterrupt, SystemExit):
            raise
        except BaseException:
            # Telemetry validation/emission remains behavior-neutral.
            return
        finally:
            self._active_poll = None

    @staticmethod
    def _terminal_requested(
        work_stop: asyncio.Event,
        signals: feed_work_scheduler.LaneSignalView,
    ) -> bool:
        """Return one monotonic pre-accumulator or in-poll terminal signal."""
        return (
            work_stop.is_set()
            or signals.stop_requested.is_set()
            or signals.grant_lost.is_set()
        )

    @staticmethod
    def _terminal_poll_outcome(
        signals: feed_work_scheduler.LaneSignalView,
        handoff: ProcessorCancellationHandoff,
    ) -> telemetry.SidPollOutcome:
        """Select authority loss only from exact lane/runner evidence."""
        if signals.grant_lost.is_set() or (
            handoff.cause is RunnerCancellationCause.EXACT_GRANT_LOSS
        ):
            return telemetry.SidPollOutcome.AUTHORITY_LOST
        return telemetry.SidPollOutcome.STOPPED

    async def _record_logical_failure(
        self,
        stop_requested: asyncio.Event,
        status_reason: feed_store.FeedStatusReason,
        reason: str,
    ) -> None:
        """Count one completed logical failure and pace retry or terminate."""
        self._consecutive_failures += 1
        if self._consecutive_failures >= _MAX_CONSECUTIVE_FAILURES:
            raise SidProcessorFailure(status_reason, reason)
        await self._wait(stop_requested, _POLL_INTERVAL_SEC)

    def _member_state(self, feed_id: uuid.UUID) -> _MemberState:
        """Return one current member state or fail closed."""
        try:
            return self._members_by_id[feed_id]
        except KeyError as exc:
            message = "Feed is not a current SID member"
            raise KeyError(message) from exc

    async def _refresh_membership(
        self,
    ) -> ingestion_lease_store.MembershipSnapshot:
        """Refresh once and atomically apply a changed complete snapshot."""
        known_revision = (
            None
            if self._snapshot is None
            else self._snapshot.membership_revision
        )
        try:
            result = await self._membership_store.refresh_membership(
                self._grant,
                known_revision=known_revision,
            )
        except _MEMBERSHIP_UNCERTAINTY_ERRORS as exc:
            raise _MembershipRefreshUncertain from exc
        if isinstance(result, ingestion_lease_store.GrantRejected):
            raise SidProcessorAuthorityLost(result)
        if isinstance(
            result,
            ingestion_lease_store.MembershipInvariantViolation,
        ):
            raise SidProcessorFailure(
                feed_store.FeedStatusReason.SYSTEM_SOURCE_CONFIGURATION_INVALID,
                "bcfy_calls_sid_membership_invalid",
            )
        if isinstance(result, ingestion_lease_store.MembershipUnchanged):
            if self._snapshot is None:
                raise SidProcessorFailure(
                    feed_store.FeedStatusReason.SYSTEM_SOURCE_CONFIGURATION_INVALID,
                    "bcfy_calls_sid_membership_invalid",
                )
            if (
                result.grant != self._grant
                or result.membership_revision
                != self._snapshot.membership_revision
            ):
                raise SidProcessorFailure(
                    feed_store.FeedStatusReason.SYSTEM_SOURCE_CONFIGURATION_INVALID,
                    "bcfy_calls_sid_membership_invalid",
                )
            return self._snapshot
        if not isinstance(result, ingestion_lease_store.MembershipSnapshot):
            message = "membership refresh returned an unknown result"
            raise TypeError(message)
        if result.grant != self._grant:
            raise SidProcessorFailure(
                feed_store.FeedStatusReason.SYSTEM_SOURCE_CONFIGURATION_INVALID,
                "bcfy_calls_sid_membership_invalid",
            )
        if self._snapshot is None:
            self._apply_initial_snapshot(result)
        else:
            await self._apply_changed_snapshot(result)
        return result

    def _apply_initial_snapshot(
        self,
        snapshot: ingestion_lease_store.MembershipSnapshot,
    ) -> None:
        """Bootstrap cursor and member states from one initial snapshot."""
        now = _require_utc_datetime(self._now(), field_name="now")
        initial_cause = {
            grant_control.ClaimMode.PRIMARY: (
                cursor_policy.ReplayFloorCause.BOOTSTRAP
            ),
            grant_control.ClaimMode.RECOVERY: (
                cursor_policy.ReplayFloorCause.RECOVERY
            ),
        }[self._claim_payload.claim_mode]
        decision = cursor_policy.bootstrap_cursor(
            (member.last_bookmark_time for member in snapshot.members),
            now=now,
            cause=initial_cause,
        )
        members_by_source: dict[str, _MemberState] = {}
        members_by_id: dict[uuid.UUID, _MemberState] = {}
        for member in snapshot.members:
            state = _MemberState(
                member=member,
                effective_cursor=member.last_bookmark_time,
                route_state=(
                    MemberRouteState.ADOPTING
                    if member.last_bookmark_time is None
                    else MemberRouteState.NORMAL
                ),
            )
            self._add_state(members_by_source, members_by_id, state)
        self._lease_cursor = cursor_policy.LeaseCursor(
            self._grant,
            pos=decision.pos,
        )
        self._snapshot = snapshot
        self._members_by_source = members_by_source
        self._members_by_id = members_by_id
        self._initial_floor_cause = initial_cause
        self._initial_requested_start = decision.durable_minimum
        self._initial_request_pending = True

    async def _apply_changed_snapshot(
        self,
        snapshot: ingestion_lease_store.MembershipSnapshot,
    ) -> None:
        """Diff one later complete snapshot by immutable Feed UUID."""
        current_snapshot = self._snapshot
        lease_cursor = self._lease_cursor
        if current_snapshot is None or lease_cursor is None:
            message = "changed membership requires initial cursor state"
            raise RuntimeError(message)
        if snapshot.membership_revision <= current_snapshot.membership_revision:
            raise SidProcessorFailure(
                feed_store.FeedStatusReason.SYSTEM_SOURCE_CONFIGURATION_INVALID,
                "bcfy_calls_sid_membership_invalid",
            )

        incoming = {
            member.identity.feed_id: member for member in snapshot.members
        }
        if len(incoming) != len(snapshot.members):
            raise SidProcessorFailure(
                feed_store.FeedStatusReason.SYSTEM_SOURCE_CONFIGURATION_INVALID,
                "bcfy_calls_sid_membership_invalid",
            )
        reactivated = self._retired_feed_ids.intersection(incoming)
        if reactivated:
            feed_id = min(reactivated, key=lambda item: item.int)
            raise SidProcessorPlannedDrain(feed_id)

        next_by_source: dict[str, _MemberState] = {}
        next_by_id: dict[uuid.UUID, _MemberState] = {}
        for feed_id, member in incoming.items():
            prior = self._members_by_id.get(feed_id)
            if prior is None:
                route_state = MemberRouteState.NORMAL
                durable_cursor = member.last_bookmark_time
                if durable_cursor is None:
                    route_state = MemberRouteState.ADOPTING
                elif (
                    lease_cursor.pos is not None
                    and durable_cursor < lease_cursor.pos
                ):
                    route_state = MemberRouteState.REPLAY_PENDING
                state = _MemberState(
                    member=member,
                    effective_cursor=durable_cursor,
                    route_state=route_state,
                )
            else:
                self._require_same_binding(prior.member, member)
                effective_cursor = _maximum_cursor(
                    prior.effective_cursor,
                    member.last_bookmark_time,
                )
                state = _MemberState(
                    member=member,
                    effective_cursor=effective_cursor,
                    route_state=prior.route_state,
                )
            self._add_state(next_by_source, next_by_id, state)

        removed = set(self._members_by_id).difference(incoming)
        for feed_id in sorted(removed, key=lambda item: item.int):
            prior = self._members_by_id[feed_id]
            await self._lane.remove_feed(feed_id)
            prior.route_state = MemberRouteState.RETIRED
            self._retired_feed_ids.add(feed_id)
            self._clear_feed_urls(feed_id)

        self._snapshot = snapshot
        self._members_by_source = next_by_source
        self._members_by_id = next_by_id

    def _add_state(
        self,
        by_source: dict[str, _MemberState],
        by_id: dict[uuid.UUID, _MemberState],
        state: _MemberState,
    ) -> None:
        """Add one unique exact route and immutable Feed UUID."""
        source_feed_id = state.member.identity.source_feed_id
        if source_feed_id in by_source or state.feed_id in by_id:
            raise SidProcessorFailure(
                feed_store.FeedStatusReason.SYSTEM_SOURCE_CONFIGURATION_INVALID,
                "bcfy_calls_sid_membership_invalid",
            )
        by_source[source_feed_id] = state
        by_id[state.feed_id] = state

    def _require_same_binding(
        self,
        prior: ingestion_lease_store.LeaseMember,
        current: ingestion_lease_store.LeaseMember,
    ) -> None:
        """Reject source/SID/group changes for one immutable Feed UUID."""
        prior_identity = prior.identity
        current_identity = current.identity
        if (
            prior_identity.source_type is not current_identity.source_type
            or prior_identity.source_feed_id != current_identity.source_feed_id
            or prior_identity.sid != current_identity.sid
            or prior_identity.group_id != current_identity.group_id
        ):
            raise SidProcessorFailure(
                feed_store.FeedStatusReason.SYSTEM_SOURCE_CONFIGURATION_INVALID,
                "bcfy_calls_sid_membership_invalid",
            )

    def _select_request_position(self) -> _PageRequest:
        """Freeze one normal or replay-override provider request position."""
        snapshot = self._snapshot
        lease_cursor = self._lease_cursor
        if snapshot is None or lease_cursor is None:
            message = "membership must be loaded before selecting SID pos"
            raise RuntimeError(message)

        replay_states = tuple(
            state
            for state in self._members_by_id.values()
            if state.route_state is MemberRouteState.REPLAY_PENDING
        )
        position = lease_cursor.pos
        replay_feed_ids: frozenset[uuid.UUID] = frozenset()
        floor_reached: cursor_policy.ReplayFloorReached | None = None
        if replay_states:
            # A replay override supersedes an initial request that has not yet
            # been issued; never emit stale bootstrap/recovery evidence later.
            self._initial_request_pending = False
            self._initial_floor_cause = None
            self._initial_requested_start = None
            replay_cursors = tuple(
                state.effective_cursor for state in replay_states
            )
            if any(cursor is None for cursor in replay_cursors):
                raise SidProcessorFailure(
                    feed_store.FeedStatusReason.SYSTEM_SOURCE_CONFIGURATION_INVALID,
                    "bcfy_calls_sid_membership_invalid",
                )
            typed_cursors = typing.cast(
                "tuple[datetime.datetime, ...]",
                replay_cursors,
            )
            floor_decision = cursor_policy.apply_replay_floor(
                min(typed_cursors),
                now=_require_utc_datetime(self._now(), field_name="now"),
                cause=cursor_policy.ReplayFloorCause.REPLAY_OVERRIDE,
            )
            position = floor_decision.selected_start
            floor_reached = floor_decision.floor_reached
            replay_feed_ids = frozenset(
                state.feed_id for state in replay_states
            )
        elif self._initial_request_pending:
            # Re-evaluate at the actual request boundary so even a cursor that
            # crosses the floor during refresh is clamped and reported once.
            initial_cause = self._initial_floor_cause
            if initial_cause is None:
                message = "initial request lost immutable floor provenance"
                raise feed_work_scheduler.CohortIntegrityError(message)
            floor_decision = cursor_policy.apply_replay_floor(
                self._initial_requested_start,
                now=_require_utc_datetime(self._now(), field_name="now"),
                cause=initial_cause,
            )
            position = floor_decision.selected_start
            floor_reached = floor_decision.floor_reached
            self._initial_request_pending = False
            self._initial_floor_cause = None
            self._initial_requested_start = None
        else:
            floor_decision = cursor_policy.apply_replay_floor(
                position,
                now=_require_utc_datetime(self._now(), field_name="now"),
                cause=cursor_policy.ReplayFloorCause.OVERLOAD,
            )
            position = floor_decision.selected_start
            floor_reached = floor_decision.floor_reached

        self._emit_replay_floor_reached(floor_reached)

        return _PageRequest(
            pos=position,
            replay_feed_ids=replay_feed_ids,
            snapshot=snapshot,
            routes=types.MappingProxyType(dict(self._members_by_source)),
        )

    def _emit_replay_floor_reached(
        self,
        evidence: cursor_policy.ReplayFloorReached | None,
    ) -> None:
        """Best-effort emission after one immutable cursor decision."""
        if evidence is None:
            return
        try:
            event = telemetry.ReplayWindowTruncatedEvent(self._grant, evidence)
            self._replay_floor_emitter(event)
        except (KeyboardInterrupt, SystemExit):
            raise
        except BaseException:
            # Operational evidence cannot mutate request or cursor authority.
            return

    async def _settle_page(  # noqa: PLR0912
        self,
        page: provider.CallsPageEnvelope,
        *,
        request: _PageRequest,
        poll: _SidPollAccumulator | None = None,
    ) -> None:
        """Settle one HTTP-success page as progress or no-progress."""
        lease_cursor = self._lease_cursor
        if lease_cursor is None:
            message = "membership must be loaded before page settlement"
            raise RuntimeError(message)
        if poll is None:
            # Preserve the narrow private test/diagnostic seam used to settle
            # an already-fetched page outside the logical polling loop. Such
            # calls collect owner evidence locally but never emit a poll.
            poll = _SidPollAccumulator(self._grant, self._monotonic())
        elif self._active_poll is not poll:
            message = "page settlement crossed its poll accumulator"
            raise feed_work_scheduler.CohortIntegrityError(message)

        last_pos, last_pos_state = _last_pos_to_datetime(page.last_pos)
        if (
            last_pos is not None
            and lease_cursor.pos is not None
            and last_pos < lease_cursor.pos
        ):
            _log_invalid_last_pos()
            last_pos = None
            last_pos_state = telemetry.SidPollResponseLastPosState.REGRESSIVE
        poll.observe_response_boundary(last_pos_state, last_pos)
        if last_pos is None:
            candidate = lease_cursor.prepare_no_progress()
            boundaries: tuple[feed_work_scheduler.BoundaryWork, ...] = ()
        else:
            candidate = lease_cursor.prepare(last_pos)
            boundaries = tuple(
                feed_work_scheduler.BoundaryWork(
                    state.member.identity,
                    last_pos,
                )
                for state in self._progress_participants(request)
            )

        prepared = self._prepare_page_work(
            page.calls,
            candidate=candidate,
            replay_feed_ids=request.replay_feed_ids,
            frozen_routes=request.routes,
        )
        poll.observe_routing(
            matched_call_count=prepared.matched_call_count,
            deduplicated_call_count=prepared.deduplicated_call_count,
        )
        if self._active_page_ledger is not None:
            message = "processor already retains an unsettled page ledger"
            raise feed_work_scheduler.CohortIntegrityError(message)
        self._active_page_ledger = prepared.ledger
        try:
            settled = await self._lane.cover_page(
                calls=prepared.cohorts,
                boundaries=boundaries,
                candidate=candidate,
                evidence_observer=poll.observe_scheduler,
            )
        except BaseException:
            if _page_obligations_are_released(prepared):
                self._active_page_ledger = None
            raise
        if isinstance(settled, feed_work_scheduler.Undrained):
            raise SidProcessorUndrained(settled)
        if type(settled) is not feed_work_scheduler.SettledPage:
            message = "SID lane returned outside the closed page contract"
            raise feed_work_scheduler.CohortIntegrityError(message)
        if (
            settled.candidate is not candidate
            or settled.grant != self._grant
            or settled.page_sequence != candidate.page_sequence
        ):
            message = "settled page crossed exact processor candidate"
            raise feed_work_scheduler.CohortIntegrityError(message)
        poll.observe_scheduler(settled.scheduler_evidence)
        source_evidence = settled.source_evidence
        if type(source_evidence) is not (
            runtime_adapters.PageFinalizationEvidence
        ):
            message = "settled page lacks exact Calls source evidence"
            raise feed_work_scheduler.CohortIntegrityError(message)
        poll.observe_source_evidence(source_evidence)
        await self._consume_source_evidence(
            settled,
            prepared=prepared,
        )

    def _progress_participants(
        self,
        request: _PageRequest,
    ) -> tuple[_MemberState, ...]:
        """Return frozen members authorized for valid page boundaries."""
        participants: list[_MemberState] = []
        for state in request.routes.values():
            if state.route_state in (
                MemberRouteState.NORMAL,
                MemberRouteState.ADOPTING,
            ) or (
                state.route_state is MemberRouteState.REPLAY_PENDING
                and state.feed_id in request.replay_feed_ids
            ):
                participants.append(state)
        return tuple(participants)

    def _prepare_page_work(
        self,
        raw_calls: collections.abc.Iterable[object],
        *,
        candidate: cursor_policy.PageCandidate,
        replay_feed_ids: frozenset[uuid.UUID] = frozenset(),
        frozen_routes: (
            collections.abc.Mapping[str, _MemberState] | None
        ) = None,
    ) -> _PreparedPageWork:
        """Freeze one candidate page into exact sorted timestamp cohorts.

        Args:
            raw_calls: Provider items in their frozen page order.
            candidate: Exact already-prepared cursor candidate for the page.
            replay_feed_ids: Feeds authorized by the current replay override.
            frozen_routes: Optional immutable routing snapshot for this fetch.

        Returns:
            One page ledger and deterministic per-Feed cohort stream. Cohort
            first-seen order is defined by the existing ``(ts, source_order)``
            sorted stream, preserving chronological Feed execution.

        Raises:
            TypeError: The candidate is outside the closed cursor vocabulary.
            CohortIntegrityError: Candidate authority crosses the processor.
        """
        if type(candidate) not in {
            cursor_policy.PageCursorCandidate,
            cursor_policy.NoProgressPageCandidate,
        }:
            message = "candidate must be an exact PageCandidate"
            raise TypeError(message)
        if candidate.grant != self._grant:
            message = "candidate crossed processor grant"
            raise feed_work_scheduler.CohortIntegrityError(message)
        page_routes = (
            dict(self._members_by_source)
            if frozen_routes is None
            else frozen_routes
        )
        validated: list[_ValidatedCall] = []
        for source_order, raw_call in enumerate(raw_calls):
            item = self._validate_call(
                source_order,
                raw_call,
                page_routes,
            )
            if item is not None:
                validated.append(item)
        validated.sort(
            key=lambda item: (item.sort_timestamp, item.source_order)
        )
        ledger = gap_ledger.MissingCallLedger(
            self._grant,
            candidate.page_sequence,
        )
        grouped: dict[
            tuple[uuid.UUID, datetime.datetime | None, int | None],
            list[_ValidatedCall],
        ] = {}
        seen_urls: set[str] = set()
        deduplicated_call_count = 0
        for item in validated:
            state = item.state
            eligible = state.route_state is MemberRouteState.NORMAL
            if state.route_state is MemberRouteState.REPLAY_PENDING:
                eligible = state.feed_id in replay_feed_ids
            if not eligible:
                continue
            if (
                item.source_timestamp is not None
                and state.effective_cursor is not None
                and item.source_timestamp <= state.effective_cursor
            ):
                continue
            if (
                item.audio_url in seen_urls
                or item.audio_url in self._pending_by_url
                or item.audio_url in self._recent_urls
            ):
                deduplicated_call_count += 1
                continue
            seen_urls.add(item.audio_url)
            singleton = (
                item.source_order if item.source_timestamp is None else None
            )
            key = (state.feed_id, item.source_timestamp, singleton)
            grouped.setdefault(key, []).append(item)

        cohorts = []
        page_entries = []
        entry_feed_ids = []
        next_source_order = 0
        for items in grouped.values():
            state = items[0].state
            entries = []
            for item in items:
                source_order = next_source_order
                next_source_order += 1
                obligation = ledger.draft(
                    state.member,
                    audio_url=item.audio_url,
                    provider_ts=item.source_timestamp,
                    source_order=source_order,
                )
                entry = cohort_pipeline.ScheduledCohortEntry(
                    source_order=source_order,
                    audio_url=item.audio_url,
                    raw_call=item.raw_call,
                    receipt_time=_require_utc_datetime(
                        self._now(),
                        field_name="now",
                    ),
                    obligation=obligation,
                )
                entries.append(entry)
                page_entries.append(entry)
                entry_feed_ids.append(state.feed_id)
            payload = cohort_pipeline.ScheduledCohortPayload(
                member=state.member,
                session_id=self._session_id(state.member.identity),
                entries=tuple(entries),
            )
            calls = tuple(
                feed_work_scheduler.CallSubmission(
                    feed_id=state.feed_id,
                    source_timestamp=item.source_timestamp,
                    payload=payload,
                    settlement_observer=self._settlement_observer(
                        payload,
                        entry,
                        state.feed_id,
                    ),
                )
                for item, entry in zip(items, entries, strict=True)
            )
            cohorts.append(
                feed_work_scheduler.CohortSubmission(
                    member=state.member.identity,
                    feed_id=state.feed_id,
                    cohort_timestamp=items[0].source_timestamp,
                    calls=calls,
                    admission_hook=self._admission_hook(
                        payload,
                        tuple(entry.audio_url for entry in entries),
                        state.feed_id,
                    ),
                )
            )
        return _PreparedPageWork(
            ledger=ledger,
            cohorts=tuple(cohorts),
            entries=tuple(page_entries),
            entry_feed_ids=tuple(entry_feed_ids),
            matched_call_count=len(validated),
            deduplicated_call_count=deduplicated_call_count,
        )

    def _validate_call(  # noqa: PLR0911
        self,
        source_order: int,
        raw_call: object,
        frozen_routes: collections.abc.Mapping[str, _MemberState],
    ) -> _ValidatedCall | None:
        """Validate one provider item without poisoning its siblings."""
        if not isinstance(raw_call, collections.abc.Mapping):
            return None
        call_mapping = typing.cast(
            "collections.abc.Mapping[str, object]",
            raw_call,
        )
        group_id = call_mapping.get("groupId")
        if not isinstance(group_id, str) or not group_id:
            return None
        state = frozen_routes.get(group_id)
        if state is None:
            return None
        audio_url = call_mapping.get("url")
        if not isinstance(audio_url, str) or not audio_url:
            return None

        timestamp_value = call_mapping.get("ts", _MISSING)
        source_timestamp: datetime.datetime | None
        sort_timestamp = 0.0
        if timestamp_value is _MISSING:
            source_timestamp = None
            logger.warning(
                "bcfy_calls call missing 'ts' (API pagination key)",
                extra={"json_fields": {"event_type": "bcfy_calls_missing_ts"}},
            )
        else:
            if (
                isinstance(timestamp_value, bool)
                or not isinstance(timestamp_value, (int, float))
                or not math.isfinite(timestamp_value)
            ):
                return None
            try:
                source_timestamp = datetime.datetime.fromtimestamp(
                    timestamp_value,
                    datetime.UTC,
                )
            except (OverflowError, OSError, ValueError):
                return None
            sort_timestamp = float(timestamp_value)

        return _ValidatedCall(
            source_order=source_order,
            state=state,
            audio_url=audio_url,
            source_timestamp=source_timestamp,
            sort_timestamp=sort_timestamp,
            raw_call=types.MappingProxyType(dict(call_mapping)),
        )

    def _settlement_observer(
        self,
        payload: cohort_pipeline.ScheduledCohortPayload,
        entry: cohort_pipeline.ScheduledCohortEntry,
        feed_id: uuid.UUID,
    ) -> collections.abc.Callable[[feed_work_scheduler.CallSettlement], None]:
        """Pair the exact ledger release with grant-local URL state."""
        ledger = gap_ledger._owner_for(entry.obligation)  # noqa: SLF001
        ledger_observer = payload.settlement_observer(entry.source_order)
        audio_url = entry.audio_url

        def observe(settlement: feed_work_scheduler.CallSettlement) -> None:
            ledger_observer(settlement)
            if self._pending_by_url.get(audio_url) != feed_id:
                return
            state = ledger.state(entry.obligation)
            if state is gap_ledger.MissingCallState.FINAL_CLOSURE_PENDING:
                return
            del self._pending_by_url[audio_url]
            if (
                state
                in {
                    gap_ledger.MissingCallState.RESOLVED,
                    gap_ledger.MissingCallState.EMITTED,
                }
                and feed_id not in self._retired_feed_ids
            ):
                self._append_recent(audio_url, feed_id)

        return observe

    def _admission_hook(
        self,
        payload: cohort_pipeline.ScheduledCohortPayload,
        audio_urls: tuple[str, ...],
        feed_id: uuid.UUID,
    ) -> collections.abc.Callable[
        [tuple[feed_work_scheduler.CohortRecordIdentity, ...]],
        None,
    ]:
        """Atomically bind exact identities and grant-local pending URLs.

        The synchronous URL staging is fully rolled back for either a mapping
        failure or ledger validation failure. ``payload.admission_hook`` is
        the final operation: its ledger validates the whole tuple before its
        non-failing state assignments, so success needs no compensating step.
        """

        def admit(
            identities: tuple[
                feed_work_scheduler.CohortRecordIdentity,
                ...,
            ],
        ) -> None:
            if any(
                audio_url in self._pending_by_url
                or audio_url in self._recent_urls
                for audio_url in audio_urls
            ):
                message = "cohort URL became ineligible before admission"
                raise feed_work_scheduler.CohortIntegrityError(message)
            try:
                for audio_url in audio_urls:
                    self._pending_by_url[audio_url] = feed_id
                payload.admission_hook(identities)
            except BaseException:
                _rollback_pending_urls(
                    self._pending_by_url,
                    audio_urls,
                    feed_id,
                )
                raise

        return admit

    def _session_id(
        self,
        member: ingestion_lease_store.LeaseMemberIdentity,
    ) -> str:
        """Return one stable invocation-local session for an issued member."""
        key = id(member)
        cached = self._session_by_member.get(key)
        if cached is not None:
            cached_member, session_id = cached
            if cached_member is not member:
                message = "member identity key was reused during invocation"
                raise feed_work_scheduler.CohortIntegrityError(message)
            return session_id
        session_id = self._session_id_factory()
        if not isinstance(session_id, str):
            message = "session_id_factory must return a string"
            raise feed_work_scheduler.CohortIntegrityError(message)
        prior = self._member_by_session.get(session_id)
        if prior is not None and prior is not member:
            message = "session_id_factory collided across issued members"
            raise feed_work_scheduler.CohortIntegrityError(message)
        self._session_by_member[key] = (member, session_id)
        self._member_by_session[session_id] = member
        return session_id

    async def _consume_source_evidence(
        self,
        settled: feed_work_scheduler.SettledPage,
        *,
        prepared: _PreparedPageWork,
    ) -> None:
        """Consume one accepted page exactly once, then apply local effects.

        Args:
            settled: Exact scheduler-owned accepted page result.
            prepared: Processor-owned ledger and cohort stream for that page.

        Raises:
            CohortIntegrityError: Returned page or source evidence crosses the
                processor's grant, candidate, cursor, or lifecycle authority.
            SidProcessorFailure: The accepted verdict promotes to Lease scope.
        """
        source_evidence = settled.source_evidence
        if type(source_evidence) is not (
            runtime_adapters.PageFinalizationEvidence
        ):
            message = "settled page lacks exact Calls source evidence"
            raise feed_work_scheduler.CohortIntegrityError(message)
        verdict = source_evidence.verdict
        if (
            verdict.grant != self._grant
            or verdict.page_sequence != settled.page_sequence
        ):
            message = "Calls source evidence crossed processor page"
            raise feed_work_scheduler.CohortIntegrityError(message)

        prepared.ledger.close_page(settled)
        self._finalize_page_urls(prepared)
        lease_cursor = self._lease_cursor
        if lease_cursor is None:
            message = "accepted page lost its Lease Cursor"
            raise RuntimeError(message)
        settlement = settled.lease_settlement
        if type(settlement) is cursor_policy._CoveredPage:  # noqa: SLF001
            lease_cursor.accept(settlement)
        elif type(settlement) is cursor_policy._NoProgressPageSettled:  # noqa: SLF001
            lease_cursor.accept_no_progress(settlement)
        elif type(settlement) is cursor_policy._ReplayablePageSettled:  # noqa: SLF001
            lease_cursor.accept_replayable(settlement)
        else:
            message = "settled page carries an unknown cursor settlement"
            raise feed_work_scheduler.CohortIntegrityError(message)

        self._apply_closure_caps(settled, source_evidence.closure_caps)
        await self._reconcile_lane_retirements()
        for feed_id in source_evidence.quarantined_feed_ids:
            await self._retire_local_feed(feed_id, remove_from_lane=True)
        self._active_page_ledger = None

        if (
            verdict.selected_scope
            is boundary_verdict.NewlyChargedScope.LEASE_ONLY
            and verdict.lease_failure is not None
        ):
            raise SidProcessorFailure(
                verdict.lease_failure.status_reason,
                verdict.lease_failure.detail,
            )

    def _apply_closure_caps(
        self,
        settled: feed_work_scheduler.SettledPage,
        caps: tuple[object, ...],
    ) -> None:
        """Advance only Feed routes proven through exact source caps."""
        candidate = settled.candidate
        if type(candidate) is cursor_policy.NoProgressPageCandidate:
            return
        if type(candidate) is not cursor_policy.PageCursorCandidate:
            message = "accepted page carries an unknown candidate"
            raise feed_work_scheduler.CohortIntegrityError(message)
        for cap in caps:
            try:
                feed_id = runtime_adapters.feed_closure_cap_feed_id(cap)
                covered = runtime_adapters.feed_closure_cap_covers(
                    cap,
                    grant=self._grant,
                    page_sequence=candidate.page_sequence,
                    feed_id=feed_id,
                    requested_target=candidate.last_pos,
                )
            except (
                TypeError,
                ValueError,
                runtime_adapters.BoundaryAdapterIntegrityError,
            ) as exc:
                message = "closure cap crossed processor authority"
                raise feed_work_scheduler.CohortIntegrityError(message) from exc
            if not covered:
                message = "closure cap does not cover processor candidate"
                raise feed_work_scheduler.CohortIntegrityError(message)
            state = self._members_by_id.get(feed_id)
            if state is None:
                continue
            state.effective_cursor = _maximum_cursor(
                state.effective_cursor,
                candidate.last_pos,
            )
            if state.route_state in {
                MemberRouteState.ADOPTING,
                MemberRouteState.REPLAY_PENDING,
            }:
                state.route_state = MemberRouteState.NORMAL

    async def _reconcile_lane_retirements(self) -> None:
        """Mirror scheduler-local exact Feed retirement into routing state."""
        for feed_id in tuple(self._members_by_id):
            if await self._lane.is_feed_retired(feed_id):
                await self._retire_local_feed(
                    feed_id,
                    remove_from_lane=False,
                )

    async def _retire_local_feed(
        self,
        feed_id: uuid.UUID,
        *,
        remove_from_lane: bool,
    ) -> None:
        """Tombstone one exact Feed and clear only its local work state."""
        if feed_id in self._retired_feed_ids:
            return
        if remove_from_lane:
            await self._lane.remove_feed(feed_id)
        state = self._members_by_id.pop(feed_id, None)
        if state is not None:
            source_feed_id = state.member.identity.source_feed_id
            if self._members_by_source.get(source_feed_id) is state:
                del self._members_by_source[source_feed_id]
            state.route_state = MemberRouteState.RETIRED
        self._retired_feed_ids.add(feed_id)
        self._clear_feed_urls(feed_id)

    def _finalize_page_urls(self, prepared: _PreparedPageWork) -> None:
        """Apply final close states that settle after scheduler observers."""
        for entry, feed_id in zip(
            prepared.entries,
            prepared.entry_feed_ids,
            strict=True,
        ):
            if self._pending_by_url.get(entry.audio_url) != feed_id:
                continue
            state = prepared.ledger.state(entry.obligation)
            if state is gap_ledger.MissingCallState.REPLAY_RELEASED:
                del self._pending_by_url[entry.audio_url]
            elif state in {
                gap_ledger.MissingCallState.RESOLVED,
                gap_ledger.MissingCallState.EMITTED,
            }:
                del self._pending_by_url[entry.audio_url]
                if feed_id not in self._retired_feed_ids:
                    self._append_recent(entry.audio_url, feed_id)

    def _append_recent(self, audio_url: str, feed_id: uuid.UUID) -> None:
        """Append one unique completion while synchronizing deque and set."""
        if audio_url in self._recent_urls:
            return
        if len(self._recent_order) == _RECENT_URL_LIMIT:
            evicted_url, _ = self._recent_order[0]
            self._recent_urls.remove(evicted_url)
        self._recent_order.append((audio_url, feed_id))
        self._recent_urls.add(audio_url)

    def _clear_feed_urls(self, feed_id: uuid.UUID) -> None:
        """Clear only one Feed's pending and recent exact-URL state."""
        for audio_url, owner_feed_id in tuple(self._pending_by_url.items()):
            if owner_feed_id == feed_id:
                del self._pending_by_url[audio_url]
        retained = (
            entry for entry in self._recent_order if entry[1] != feed_id
        )
        self._recent_order = collections.deque(
            retained,
            maxlen=_RECENT_URL_LIMIT,
        )
        self._recent_urls = {url for url, _ in self._recent_order}

    def close(self) -> None:
        """Discard all exact-grant local routing and deduplication state."""
        self._pending_by_url.clear()
        self._recent_order.clear()
        self._recent_urls.clear()
        self._session_by_member.clear()
        self._member_by_session.clear()
        self._members_by_source.clear()
        self._members_by_id.clear()
        self._retired_feed_ids.clear()
        self._snapshot = None
        self._lease_cursor = None
        self._active_poll = None


_MISSING = object()


def _last_pos_to_datetime(
    value: object | None,
) -> tuple[
    datetime.datetime | None,
    telemetry.SidPollResponseLastPosState,
]:
    """Parse Calls progress plus one closed normalized telemetry state."""
    if value is None:
        _log_invalid_last_pos()
        return None, telemetry.SidPollResponseLastPosState.MISSING
    if isinstance(value, bool):
        _log_invalid_last_pos()
        return None, telemetry.SidPollResponseLastPosState.INVALID
    try:
        numeric_value = float(typing.cast("typing.Any", value))
    except (TypeError, ValueError):
        _log_invalid_last_pos()
        return None, telemetry.SidPollResponseLastPosState.INVALID
    if not math.isfinite(numeric_value):
        _log_invalid_last_pos()
        return None, telemetry.SidPollResponseLastPosState.INVALID
    try:
        timestamp = int(numeric_value)
        return (
            datetime.datetime.fromtimestamp(timestamp, datetime.UTC),
            telemetry.SidPollResponseLastPosState.VALID,
        )
    except (OSError, OverflowError, ValueError):
        _log_invalid_last_pos()
        return None, telemetry.SidPollResponseLastPosState.INVALID


def _log_invalid_last_pos() -> None:
    """Emit the existing low-cardinality invalid progress diagnostic."""
    logger.warning(
        "bcfy_calls response contained invalid lastPos",
        extra={"json_fields": {"event_type": "bcfy_calls_invalid_last_pos"}},
    )


def _require_utc_datetime(
    value: object,
    *,
    field_name: str,
) -> datetime.datetime:
    if not isinstance(value, datetime.datetime):
        message = f"{field_name} must be a datetime"
        raise TypeError(message)
    if value.utcoffset() != datetime.timedelta(0):
        message = f"{field_name} must be UTC-aware"
        raise ValueError(message)
    return value


def _maximum_cursor(
    first: datetime.datetime | None,
    second: datetime.datetime | None,
) -> datetime.datetime | None:
    """Return the monotonic maximum of two optional UTC cursors."""
    if first is None:
        return second
    if second is None:
        return first
    return max(first, second)
