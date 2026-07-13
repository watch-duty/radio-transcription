"""Pure exact-fact policy for one finalized Broadcastify Calls page."""

from __future__ import annotations

import dataclasses
import enum
import typing

from backend.pipeline.ingestion import feed_work_scheduler
from backend.pipeline.ingestion.collectors import failure_classification
from backend.pipeline.storage import feed_store, ingestion_lease_store

if typing.TYPE_CHECKING:
    import datetime
    import uuid

MIXED_DIRECT_FAILURE_REASON = "mixed_direct_failures"
MIXED_FEED_FAILURE_REASON = "mixed_feed_failures"

_FINALIZABLE_DISPOSITIONS = frozenset(
    {
        feed_work_scheduler.CohortTerminalDisposition.SETTLED,
        feed_work_scheduler.CohortTerminalDisposition.FINAL_CLOSURE_PENDING,
        feed_work_scheduler.CohortTerminalDisposition.REPLAYABLE_DIRECT,
    }
)


class BoundaryVerdictIntegrityError(ValueError):
    """Exact page facts are incomplete, crossed, or structurally invalid."""


class NewlyChargedScope(enum.StrEnum):
    """The sole lifecycle scope newly charged by a finalized page."""

    LEASE_ONLY = "lease_only"
    FAILED_FEEDS = "failed_feeds"


class FeedFailureOrigin(enum.StrEnum):
    """Closed owner of one selected Feed-level failure."""

    ITEM_AGGREGATION = "item_aggregation"
    DIRECT_PIPELINE = "direct_pipeline"


@dataclasses.dataclass(frozen=True, slots=True)
class BoundaryFailure:
    """Canonical status and bounded diagnostic detail."""

    status_reason: feed_store.FeedStatusReason
    detail: str

    def __post_init__(self) -> None:
        if not isinstance(self.status_reason, feed_store.FeedStatusReason):
            message = "status_reason must be a FeedStatusReason"
            raise TypeError(message)
        if not isinstance(self.detail, str):
            message = "detail must be a string"
            raise TypeError(message)


@dataclasses.dataclass(frozen=True, slots=True)
class DirectFailureEvidence:
    """One identity-bound direct failure in deterministic page order."""

    identity: feed_work_scheduler.CohortRecordIdentity
    stage: feed_work_scheduler.CohortRecordTerminalReason
    failure: BoundaryFailure

    def __post_init__(self) -> None:
        if type(self.identity) is not feed_work_scheduler.CohortRecordIdentity:
            message = "identity must be an exact CohortRecordIdentity"
            raise TypeError(message)
        if self.stage not in {
            feed_work_scheduler.CohortRecordTerminalReason.REPLAYABLE_DIRECT,
            feed_work_scheduler.CohortRecordTerminalReason.PUBLICATION_EXHAUSTED,
        }:
            message = "direct failure stage is not a direct terminal reason"
            raise BoundaryVerdictIntegrityError(message)
        if type(self.failure) is not BoundaryFailure:
            message = "failure must be an exact BoundaryFailure"
            raise TypeError(message)


@dataclasses.dataclass(frozen=True, slots=True)
class FeedBoundaryFacts:
    """One Feed's immutable normalization of predecessor terminal facts."""

    member: ingestion_lease_store.LeaseMemberIdentity
    record_facts: tuple[
        feed_work_scheduler.CohortRecordTerminalFact,
        ...,
    ]
    participated: bool
    attempted_count: int
    usable_audio_observed: bool
    full_pipeline_completed: bool
    all_records_durably_closed: bool
    replayable_record_observed: bool
    item_failure: failure_classification.ItemFailure | None
    direct_failures: tuple[DirectFailureEvidence, ...]
    selected_failure: BoundaryFailure | None
    failure_origin: FeedFailureOrigin | None
    item_to_feed_promoted: bool

    def __post_init__(self) -> None:
        if not isinstance(
            self.member,
            ingestion_lease_store.LeaseMemberIdentity,
        ):
            message = "member must be a LeaseMemberIdentity"
            raise TypeError(message)
        if not isinstance(self.record_facts, tuple):
            message = "record_facts must be an immutable tuple"
            raise TypeError(message)
        _require_nonnegative_integer(self.attempted_count, "attempted_count")
        for name, value in (
            ("participated", self.participated),
            ("usable_audio_observed", self.usable_audio_observed),
            ("full_pipeline_completed", self.full_pipeline_completed),
            ("all_records_durably_closed", self.all_records_durably_closed),
            ("replayable_record_observed", self.replayable_record_observed),
            ("item_to_feed_promoted", self.item_to_feed_promoted),
        ):
            if not isinstance(value, bool):
                message = f"{name} must be a bool"
                raise TypeError(message)
        if self.participated != (self.attempted_count > 0):
            message = "participation and attempted count disagree"
            raise BoundaryVerdictIntegrityError(message)
        if self.item_to_feed_promoted != (
            self.item_failure is not None
            and self.failure_origin is FeedFailureOrigin.ITEM_AGGREGATION
        ):
            message = "item promotion and selected failure disagree"
            raise BoundaryVerdictIntegrityError(message)
        if (self.selected_failure is None) != (self.failure_origin is None):
            message = "selected failure and failure origin disagree"
            raise BoundaryVerdictIntegrityError(message)
        if self.failure_origin is FeedFailureOrigin.DIRECT_PIPELINE and not (
            self.direct_failures
        ):
            message = "direct failure origin requires direct evidence"
            raise BoundaryVerdictIntegrityError(message)


@dataclasses.dataclass(frozen=True, slots=True)
class FeedFailureDecision:
    """One failed Feed and its selected canonical lifecycle evidence."""

    member: ingestion_lease_store.LeaseMemberIdentity
    failure: BoundaryFailure
    origin: FeedFailureOrigin

    def __post_init__(self) -> None:
        if not isinstance(
            self.member,
            ingestion_lease_store.LeaseMemberIdentity,
        ):
            message = "member must be a LeaseMemberIdentity"
            raise TypeError(message)
        if type(self.failure) is not BoundaryFailure:
            message = "failure must be an exact BoundaryFailure"
            raise TypeError(message)
        if not isinstance(self.origin, FeedFailureOrigin):
            message = "origin must be a FeedFailureOrigin"
            raise TypeError(message)


@dataclasses.dataclass(frozen=True, slots=True)
class PageVerdictInput:
    """Frozen exact-grant/page facts eligible for final policy selection.

    Neutral aborts, member/loss outcomes, replay-blocked records, and unknown
    outcomes are deliberately not representable as finalizable evidence.
    ``record_identities`` is the scheduler-owned admitted record set. It keeps
    missing facts distinguishable from a genuinely quiet Feed.
    """

    grant: ingestion_lease_store.LeaseGrant
    page_sequence: int
    members: tuple[ingestion_lease_store.LeaseMemberIdentity, ...]
    record_identities: tuple[
        feed_work_scheduler.CohortRecordIdentity,
        ...,
    ]
    cohort_terminal_facts: tuple[
        feed_work_scheduler.CohortTerminalFacts,
        ...,
    ]
    child_recovery_candidates: tuple[
        ingestion_lease_store.LeaseMemberIdentity,
        ...,
    ]
    lease_recovery_candidate: bool

    def __post_init__(self) -> None:
        _validate_page_input(self)


@dataclasses.dataclass(frozen=True, slots=True)
class PageVerdict:
    """Pure finalized policy decision with one newly charged scope."""

    grant: ingestion_lease_store.LeaseGrant
    page_sequence: int
    feed_facts: tuple[FeedBoundaryFacts, ...]
    participating_feed_ids: tuple[uuid.UUID, ...]
    successful_feed_ids: tuple[uuid.UUID, ...]
    failed_feeds: tuple[FeedFailureDecision, ...]
    selected_scope: NewlyChargedScope
    lease_failure: BoundaryFailure | None
    charged_feed_failures: tuple[FeedFailureDecision, ...]
    child_recovery_candidates: tuple[
        ingestion_lease_store.LeaseMemberIdentity,
        ...,
    ]
    preserved_failed_children: tuple[
        ingestion_lease_store.LeaseMemberIdentity,
        ...,
    ]
    lease_recovery_candidate: bool
    preserved_lease_recovery: bool
    participating_feed_count: int
    successful_feed_count: int
    failed_feed_count: int
    item_to_feed_promotion_count: int

    def __post_init__(self) -> None:
        self._validate_counts()
        self._validate_scope()

    def _validate_counts(self) -> None:
        for name, value in (
            ("participating_feed_count", self.participating_feed_count),
            ("successful_feed_count", self.successful_feed_count),
            ("failed_feed_count", self.failed_feed_count),
            (
                "item_to_feed_promotion_count",
                self.item_to_feed_promotion_count,
            ),
        ):
            _require_nonnegative_integer(value, name)
        if not (
            self.item_to_feed_promotion_count
            <= self.failed_feed_count
            <= self.participating_feed_count
        ):
            message = "page verdict counts violate promotion bounds"
            raise BoundaryVerdictIntegrityError(message)
        if self.successful_feed_count > self.participating_feed_count:
            message = "successful Feed count exceeds participants"
            raise BoundaryVerdictIntegrityError(message)
        if self.participating_feed_count != len(self.participating_feed_ids):
            message = "participating Feed count is inconsistent"
            raise BoundaryVerdictIntegrityError(message)
        if self.successful_feed_count != len(self.successful_feed_ids):
            message = "successful Feed count is inconsistent"
            raise BoundaryVerdictIntegrityError(message)
        if self.failed_feed_count != len(self.failed_feeds):
            message = "failed Feed count is inconsistent"
            raise BoundaryVerdictIntegrityError(message)

    def _validate_scope(self) -> None:
        promoted = self.selected_scope is NewlyChargedScope.LEASE_ONLY
        if promoted != (self.lease_failure is not None):
            message = "Lease scope and selected Lease failure disagree"
            raise BoundaryVerdictIntegrityError(message)
        if promoted:
            if self.charged_feed_failures:
                message = "promoted verdict cannot charge child failures"
                raise BoundaryVerdictIntegrityError(message)
            if len(self.preserved_failed_children) != self.failed_feed_count:
                message = "promoted verdict must preserve every failed child"
                raise BoundaryVerdictIntegrityError(message)
        else:
            if self.charged_feed_failures != self.failed_feeds:
                message = "non-promoted verdict must charge failed Feeds"
                raise BoundaryVerdictIntegrityError(message)
            if self.preserved_failed_children:
                message = "non-promoted verdict cannot preserve failed Feeds"
                raise BoundaryVerdictIntegrityError(message)
        if self.lease_recovery_candidate and self.preserved_lease_recovery:
            message = "Lease recovery cannot be selected and preserved"
            raise BoundaryVerdictIntegrityError(message)


def _require_nonnegative_integer(value: object, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        message = f"{name} must be an integer"
        raise TypeError(message)
    if value < 0:
        message = f"{name} must be nonnegative"
        raise ValueError(message)
    return value


def _identity_order_key(
    identity: feed_work_scheduler.CohortRecordIdentity,
) -> tuple[int, int, int]:
    return (
        identity.feed_id.int,
        identity.source_order,
        identity.local_sequence,
    )


def _cohort_order_key(
    facts: feed_work_scheduler.CohortTerminalFacts,
) -> tuple[int, int, int]:
    return _identity_order_key(facts.records[0].identity)


def _require_member_binding(
    grant: ingestion_lease_store.LeaseGrant,
    member: object,
) -> ingestion_lease_store.LeaseMemberIdentity:
    try:
        # The store owns the process-local authoritative member seal.
        return ingestion_lease_store._require_member_binding(  # noqa: SLF001
            grant,
            member,
        )
    except (
        TypeError,
        ValueError,
        feed_work_scheduler.CohortIntegrityError,
    ) as error:
        raise BoundaryVerdictIntegrityError(str(error)) from error


def _validate_page_input(page: PageVerdictInput) -> None:
    if not isinstance(page.grant, ingestion_lease_store.LeaseGrant):
        message = "grant must be a LeaseGrant"
        raise TypeError(message)
    if page.grant.source_type is not feed_store.SourceType.BCFY_CALLS:
        message = "page verdict requires a Broadcastify Calls Lease"
        raise BoundaryVerdictIntegrityError(message)
    _require_nonnegative_integer(page.page_sequence, "page_sequence")
    if not isinstance(page.lease_recovery_candidate, bool):
        message = "lease_recovery_candidate must be a bool"
        raise TypeError(message)

    members_by_feed = _validate_members(page)
    _validate_record_identities(page, members_by_feed)
    _validate_terminal_facts(page, members_by_feed)
    _validate_recovery_candidates(page, members_by_feed)


def _validate_members(
    page: PageVerdictInput,
) -> dict[uuid.UUID, ingestion_lease_store.LeaseMemberIdentity]:
    if not isinstance(page.members, tuple):
        message = "members must be an immutable tuple"
        raise TypeError(message)
    members_by_feed: dict[
        uuid.UUID,
        ingestion_lease_store.LeaseMemberIdentity,
    ] = {}
    for candidate in page.members:
        member = _require_member_binding(page.grant, candidate)
        if member.feed_id in members_by_feed:
            message = f"duplicate page member {member.feed_id}"
            raise BoundaryVerdictIntegrityError(message)
        members_by_feed[member.feed_id] = member
    return members_by_feed


def _validate_record_identities(
    page: PageVerdictInput,
    members_by_feed: dict[
        uuid.UUID,
        ingestion_lease_store.LeaseMemberIdentity,
    ],
) -> None:
    if not isinstance(page.record_identities, tuple):
        message = "record_identities must be an immutable tuple"
        raise TypeError(message)
    if len(set(page.record_identities)) != len(page.record_identities):
        message = "record_identities contains duplicates"
        raise BoundaryVerdictIntegrityError(message)
    keys: list[tuple[int, int, int]] = []
    source_orders: set[int] = set()
    for identity in page.record_identities:
        if type(identity) is not feed_work_scheduler.CohortRecordIdentity:
            message = "record identity must be exact CohortRecordIdentity"
            raise TypeError(message)
        member = members_by_feed.get(identity.feed_id)
        if member is None:
            message = "record identity Feed is outside the page member set"
            raise BoundaryVerdictIntegrityError(message)
        if (
            identity.grant != page.grant
            or identity.page_sequence != page.page_sequence
            or identity.member is not member
        ):
            message = "record identity crossed grant, page, or member"
            raise BoundaryVerdictIntegrityError(message)
        if identity.source_order in source_orders:
            message = "record identities repeat a page source order"
            raise BoundaryVerdictIntegrityError(message)
        source_orders.add(identity.source_order)
        keys.append(_identity_order_key(identity))
    if keys != sorted(keys):
        message = "record identities are not in deterministic page order"
        raise BoundaryVerdictIntegrityError(message)


def _validate_terminal_facts(
    page: PageVerdictInput,
    members_by_feed: dict[
        uuid.UUID,
        ingestion_lease_store.LeaseMemberIdentity,
    ],
) -> None:
    if not isinstance(page.cohort_terminal_facts, tuple):
        message = "cohort_terminal_facts must be an immutable tuple"
        raise TypeError(message)

    cohort_keys: list[tuple[int, int, int]] = []
    cohort_identities: set[tuple[uuid.UUID, datetime.datetime | None, int]] = (
        set()
    )
    flattened_identities: list[feed_work_scheduler.CohortRecordIdentity] = []
    for facts in page.cohort_terminal_facts:
        if type(facts) is not feed_work_scheduler.CohortTerminalFacts:
            message = "terminal facts must be exact CohortTerminalFacts"
            raise TypeError(message)
        _revalidate_terminal_facts_value(facts)
        if facts.disposition not in _FINALIZABLE_DISPOSITIONS:
            message = (
                f"{facts.disposition.value} facts are not finalizable verdict "
                "input"
            )
            raise BoundaryVerdictIntegrityError(message)
        _validate_disposition(facts)
        first_identity = facts.records[0].identity
        member = members_by_feed.get(first_identity.feed_id)
        if member is None:
            message = "terminal fact Feed is outside the page member set"
            raise BoundaryVerdictIntegrityError(message)
        for record in facts.records:
            _validate_record_fact(record)
            identity = record.identity
            if (
                identity.grant != page.grant
                or identity.page_sequence != page.page_sequence
                or identity.feed_id != first_identity.feed_id
                or identity.cohort_timestamp != first_identity.cohort_timestamp
                or identity.member is not member
            ):
                message = "terminal facts crossed grant, page, Feed, or member"
                raise BoundaryVerdictIntegrityError(message)
        cohort_key = _cohort_order_key(facts)
        cohort_identity = (
            first_identity.feed_id,
            first_identity.cohort_timestamp,
            (
                first_identity.source_order
                if first_identity.cohort_timestamp is None
                else -1
            ),
        )
        if cohort_identity in cohort_identities:
            message = "terminal facts split or duplicate one exact cohort"
            raise BoundaryVerdictIntegrityError(message)
        cohort_identities.add(cohort_identity)
        cohort_keys.append(cohort_key)
        flattened_identities.extend(fact.identity for fact in facts.records)

    if cohort_keys != sorted(cohort_keys):
        message = "terminal cohorts are not in deterministic page order"
        raise BoundaryVerdictIntegrityError(message)
    if len(flattened_identities) != len(page.record_identities):
        message = "terminal facts do not cover every admitted record"
        raise BoundaryVerdictIntegrityError(message)
    if any(
        actual is not expected
        for actual, expected in zip(
            flattened_identities,
            page.record_identities,
            strict=True,
        )
    ):
        message = "terminal facts are missing, forged, or out of order"
        raise BoundaryVerdictIntegrityError(message)


def _revalidate_terminal_facts_value(
    facts: feed_work_scheduler.CohortTerminalFacts,
) -> None:
    try:
        validated = feed_work_scheduler.CohortTerminalFacts(
            records=facts.records,
            disposition=facts.disposition,
        )
    except (
        TypeError,
        ValueError,
        feed_work_scheduler.CohortIntegrityError,
    ) as error:
        raise BoundaryVerdictIntegrityError(str(error)) from error
    if validated != facts:
        message = "terminal facts changed after predecessor validation"
        raise BoundaryVerdictIntegrityError(message)


def _validate_record_fact(
    record: feed_work_scheduler.CohortRecordTerminalFact,
) -> None:
    if type(record) is not feed_work_scheduler.CohortRecordTerminalFact:
        message = "record fact must be exact CohortRecordTerminalFact"
        raise TypeError(message)
    try:
        validated = feed_work_scheduler.CohortRecordTerminalFact(
            identity=record.identity,
            participated=record.participated,
            closure_state=record.closure_state,
            full_pipeline_completed=record.full_pipeline_completed,
            terminal_reason=record.terminal_reason,
            item_failure=record.item_failure,
            direct_failure=record.direct_failure,
        )
    except (
        TypeError,
        ValueError,
        feed_work_scheduler.CohortIntegrityError,
    ) as error:
        raise BoundaryVerdictIntegrityError(str(error)) from error
    if validated != record:
        message = "record fact changed after predecessor validation"
        raise BoundaryVerdictIntegrityError(message)
    if (
        record.full_pipeline_completed
        or record.item_failure is not None
        or record.direct_failure is not None
    ) and not record.participated:
        message = "selected work evidence requires participation"
        raise BoundaryVerdictIntegrityError(message)
    _revalidate_failure_fact(record.item_failure)
    _revalidate_failure_fact(record.direct_failure)


def _revalidate_failure_fact(
    fact: (
        feed_work_scheduler.CohortItemFailureFact
        | feed_work_scheduler.CohortDirectFailureFact
        | None
    ),
) -> None:
    if fact is None:
        return
    fact_type = type(fact)
    if fact_type is feed_work_scheduler.CohortItemFailureFact:
        validated = feed_work_scheduler.CohortItemFailureFact(
            fact.status_reason,
            fact.detail,
        )
    elif fact_type is feed_work_scheduler.CohortDirectFailureFact:
        validated = feed_work_scheduler.CohortDirectFailureFact(
            fact.status_reason,
            fact.detail,
        )
    else:
        message = "failure evidence has an unsupported exact type"
        raise TypeError(message)
    if validated != fact:
        message = "failure evidence changed after predecessor validation"
        raise BoundaryVerdictIntegrityError(message)


def _validate_disposition(
    facts: feed_work_scheduler.CohortTerminalFacts,
) -> None:
    disposition = facts.disposition
    records = facts.records
    if disposition is feed_work_scheduler.CohortTerminalDisposition.SETTLED:
        allowed_reasons = {
            feed_work_scheduler.CohortRecordTerminalReason.FULL_PIPELINE,
            feed_work_scheduler.CohortRecordTerminalReason.TERMINAL_ITEM_SKIP,
            feed_work_scheduler.CohortRecordTerminalReason.PUBLICATION_EXHAUSTED,
        }
        if any(
            record.closure_state
            is not feed_work_scheduler.CohortRecordClosureState.DURABLY_CLOSED
            or record.terminal_reason not in allowed_reasons
            for record in records
        ):
            message = "SETTLED facts contain non-finalizable record evidence"
            raise BoundaryVerdictIntegrityError(message)
        return
    if disposition is (
        feed_work_scheduler.CohortTerminalDisposition.FINAL_CLOSURE_PENDING
    ):
        allowed_states = {
            feed_work_scheduler.CohortRecordClosureState.DURABLY_CLOSED,
            feed_work_scheduler.CohortRecordClosureState.FINAL_CLOSURE_PENDING,
        }
        allowed_reasons = {
            feed_work_scheduler.CohortRecordTerminalReason.FULL_PIPELINE,
            feed_work_scheduler.CohortRecordTerminalReason.TERMINAL_ITEM_SKIP,
            feed_work_scheduler.CohortRecordTerminalReason.PUBLICATION_EXHAUSTED,
        }
        final_pending = (
            feed_work_scheduler.CohortRecordClosureState.FINAL_CLOSURE_PENDING
        )
        if not any(
            record.closure_state is final_pending for record in records
        ) or any(
            record.closure_state not in allowed_states
            or record.terminal_reason not in allowed_reasons
            for record in records
        ):
            message = "FINAL_CLOSURE_PENDING facts disagree with records"
            raise BoundaryVerdictIntegrityError(message)
        return
    if disposition is (
        feed_work_scheduler.CohortTerminalDisposition.REPLAYABLE_DIRECT
    ):
        allowed_reasons = {
            feed_work_scheduler.CohortRecordTerminalReason.REPLAYABLE_DIRECT,
            feed_work_scheduler.CohortRecordTerminalReason.RETRYABLE,
        }
        replay_safe = (
            feed_work_scheduler.CohortRecordClosureState.REPLAY_SAFE_RELEASE
        )
        if not any(
            record.terminal_reason
            is feed_work_scheduler.CohortRecordTerminalReason.REPLAYABLE_DIRECT
            for record in records
        ) or any(
            record.closure_state is not replay_safe
            or record.terminal_reason not in allowed_reasons
            for record in records
        ):
            message = "REPLAYABLE_DIRECT facts disagree with records"
            raise BoundaryVerdictIntegrityError(message)
        return
    message = "neutral or unknown facts cannot enter final verdict policy"
    raise BoundaryVerdictIntegrityError(message)


def _validate_recovery_candidates(
    page: PageVerdictInput,
    members_by_feed: dict[
        uuid.UUID,
        ingestion_lease_store.LeaseMemberIdentity,
    ],
) -> None:
    if not isinstance(page.child_recovery_candidates, tuple):
        message = "child_recovery_candidates must be an immutable tuple"
        raise TypeError(message)
    seen: set[uuid.UUID] = set()
    for candidate in page.child_recovery_candidates:
        if not isinstance(
            candidate,
            ingestion_lease_store.LeaseMemberIdentity,
        ):
            message = "child recovery candidate must be a LeaseMemberIdentity"
            raise TypeError(message)
        member = members_by_feed.get(candidate.feed_id)
        if member is None or candidate is not member:
            message = "child recovery candidate crossed the page member set"
            raise BoundaryVerdictIntegrityError(message)
        if member.feed_id in seen:
            message = "child recovery candidates contain duplicates"
            raise BoundaryVerdictIntegrityError(message)
        seen.add(member.feed_id)


def _normalize_feed_facts(
    page: PageVerdictInput,
) -> tuple[FeedBoundaryFacts, ...]:
    records_by_feed: dict[
        uuid.UUID,
        list[
            tuple[
                int,
                feed_work_scheduler.CohortRecordTerminalFact,
            ]
        ],
    ] = {member.feed_id: [] for member in page.members}
    for cohort_order, facts in enumerate(page.cohort_terminal_facts):
        for record in facts.records:
            records_by_feed[record.identity.feed_id].append(
                (cohort_order, record)
            )

    return tuple(
        _normalize_one_feed(
            member,
            records_by_feed[member.feed_id],
        )
        for member in sorted(page.members, key=lambda item: item.feed_id.int)
    )


def _normalize_one_feed(
    member: ingestion_lease_store.LeaseMemberIdentity,
    ordered_records: list[
        tuple[
            int,
            feed_work_scheduler.CohortRecordTerminalFact,
        ]
    ],
) -> FeedBoundaryFacts:
    item_outcome = failure_classification.ItemBatchOutcome()
    direct_failures: list[DirectFailureEvidence] = []
    direct_keys: set[tuple[object, ...]] = set()
    record_facts: list[feed_work_scheduler.CohortRecordTerminalFact] = []
    full_pipeline_completed = False

    for cohort_order, record in ordered_records:
        record_facts.append(record)
        if record.participated:
            item_outcome.record_attempt()
        if record.item_failure is not None:
            item_outcome.record_failure(
                failure_classification.ItemFailure(
                    record.item_failure.status_reason,
                    record.item_failure.detail,
                )
            )
        if record.full_pipeline_completed or record.direct_failure is not None:
            item_outcome.record_chunk_produced()
        full_pipeline_completed = (
            full_pipeline_completed or record.full_pipeline_completed
        )
        if record.direct_failure is None:
            continue
        direct_key = _direct_failure_key(cohort_order, record)
        if direct_key in direct_keys:
            message = "duplicate direct-failure stage identity"
            raise BoundaryVerdictIntegrityError(message)
        direct_keys.add(direct_key)
        direct_failures.append(
            DirectFailureEvidence(
                identity=record.identity,
                stage=record.terminal_reason,
                failure=BoundaryFailure(
                    record.direct_failure.status_reason,
                    record.direct_failure.detail,
                ),
            )
        )

    if len(direct_failures) > len(record_facts):
        message = "direct failures exceed admitted record facts"
        raise BoundaryVerdictIntegrityError(message)
    item_failure = item_outcome.promoted_failure()
    direct_failure = _aggregate_direct_failures(direct_failures)
    if direct_failure is not None:
        selected_failure = direct_failure
        failure_origin = FeedFailureOrigin.DIRECT_PIPELINE
    elif item_failure is not None:
        selected_failure = BoundaryFailure(
            item_failure.status_reason,
            item_failure.reason,
        )
        failure_origin = FeedFailureOrigin.ITEM_AGGREGATION
    else:
        selected_failure = None
        failure_origin = None

    return FeedBoundaryFacts(
        member=member,
        record_facts=tuple(record_facts),
        participated=item_outcome.attempted_count > 0,
        attempted_count=item_outcome.attempted_count,
        usable_audio_observed=item_outcome.chunk_produced,
        full_pipeline_completed=full_pipeline_completed,
        all_records_durably_closed=all(
            record.closure_state
            is feed_work_scheduler.CohortRecordClosureState.DURABLY_CLOSED
            for record in record_facts
        ),
        replayable_record_observed=any(
            record.closure_state
            is feed_work_scheduler.CohortRecordClosureState.REPLAY_SAFE_RELEASE
            for record in record_facts
        ),
        item_failure=item_failure,
        direct_failures=tuple(direct_failures),
        selected_failure=selected_failure,
        failure_origin=failure_origin,
        item_to_feed_promoted=(
            item_failure is not None and direct_failure is None
        ),
    )


def _direct_failure_key(
    cohort_order: int,
    record: feed_work_scheduler.CohortRecordTerminalFact,
) -> tuple[object, ...]:
    stage = record.terminal_reason
    replayable_direct = (
        feed_work_scheduler.CohortRecordTerminalReason.REPLAYABLE_DIRECT
    )
    if stage is replayable_direct:
        return cohort_order, stage
    return cohort_order, record.identity.source_order, stage


def _aggregate_direct_failures(
    direct_failures: list[DirectFailureEvidence],
) -> BoundaryFailure | None:
    if not direct_failures:
        return None
    first = direct_failures[0].failure
    if all(
        evidence.failure.status_reason is first.status_reason
        for evidence in direct_failures
    ):
        return first
    return BoundaryFailure(
        feed_store.FeedStatusReason.SYSTEM_COLLECTOR_ERROR,
        MIXED_DIRECT_FAILURE_REASON,
    )


def _feed_failure_decisions(
    feeds: tuple[FeedBoundaryFacts, ...],
) -> tuple[FeedFailureDecision, ...]:
    failures: list[FeedFailureDecision] = []
    for feed in feeds:
        if feed.selected_failure is None:
            continue
        if feed.failure_origin is None or not feed.participated:
            message = "selected Feed failure lacks participating ownership"
            raise BoundaryVerdictIntegrityError(message)
        failures.append(
            FeedFailureDecision(
                member=feed.member,
                failure=feed.selected_failure,
                origin=feed.failure_origin,
            )
        )
    return tuple(failures)


def _aggregate_lease_failure(
    failures: tuple[FeedFailureDecision, ...],
) -> BoundaryFailure:
    first = failures[0].failure
    if all(
        decision.failure.status_reason is first.status_reason
        for decision in failures
    ):
        return first
    return BoundaryFailure(
        feed_store.FeedStatusReason.SYSTEM_COLLECTOR_ERROR,
        MIXED_FEED_FAILURE_REASON,
    )


def decide_page_verdict(page: PageVerdictInput) -> PageVerdict:
    """Select one pure Feed-or-Lease failure scope for finalizable facts.

    Args:
        page: Exact admitted identities, predecessor terminal facts, and prior
            lifecycle recovery candidates for one complete Lease page.

    Returns:
        An immutable deterministic page verdict.

    Raises:
        TypeError: ``page`` is not an exact ``PageVerdictInput``.
        BoundaryVerdictIntegrityError: Facts are missing, crossed, neutral,
            unknown, or otherwise structurally inconsistent.
    """
    if type(page) is not PageVerdictInput:
        message = "page must be an exact PageVerdictInput"
        raise TypeError(message)
    _validate_page_input(page)

    feeds = _normalize_feed_facts(page)
    participating = tuple(feed for feed in feeds if feed.participated)
    successful = tuple(
        feed for feed in participating if feed.full_pipeline_completed
    )
    failed = _feed_failure_decisions(feeds)
    promoted = (
        len(participating) >= 2
        and not successful
        and len(failed) == len(participating)
    )
    if promoted:
        selected_scope = NewlyChargedScope.LEASE_ONLY
        lease_failure = _aggregate_lease_failure(failed)
        charged_feed_failures: tuple[FeedFailureDecision, ...] = ()
        preserved_failed_children = tuple(
            decision.member for decision in failed
        )
    else:
        selected_scope = NewlyChargedScope.FAILED_FEEDS
        lease_failure = None
        charged_feed_failures = failed
        preserved_failed_children = ()

    failed_feed_ids = {decision.member.feed_id for decision in failed}
    child_recoveries = tuple(
        member
        for member in sorted(
            page.child_recovery_candidates,
            key=lambda item: item.feed_id.int,
        )
        if member.feed_id not in failed_feed_ids
    )
    item_to_feed_count = sum(feed.item_to_feed_promoted for feed in feeds)

    return PageVerdict(
        grant=page.grant,
        page_sequence=page.page_sequence,
        feed_facts=feeds,
        participating_feed_ids=tuple(
            feed.member.feed_id for feed in participating
        ),
        successful_feed_ids=tuple(feed.member.feed_id for feed in successful),
        failed_feeds=failed,
        selected_scope=selected_scope,
        lease_failure=lease_failure,
        charged_feed_failures=charged_feed_failures,
        child_recovery_candidates=child_recoveries,
        preserved_failed_children=preserved_failed_children,
        lease_recovery_candidate=(
            page.lease_recovery_candidate and not promoted
        ),
        preserved_lease_recovery=(page.lease_recovery_candidate and promoted),
        participating_feed_count=len(participating),
        successful_feed_count=len(successful),
        failed_feed_count=len(failed),
        item_to_feed_promotion_count=item_to_feed_count,
    )
