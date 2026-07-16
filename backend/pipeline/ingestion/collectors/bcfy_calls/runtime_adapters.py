"""Runtime adapters connecting bounded Calls work to fenced storage."""

from __future__ import annotations

import collections.abc
import dataclasses
import datetime
import enum
import typing
import uuid

from backend.pipeline.ingestion import feed_work_scheduler
from backend.pipeline.ingestion.collectors.bcfy_calls import (
    boundary_verdict,
    cursor_policy,
)
from backend.pipeline.storage import ingestion_lease_store

__all__ = [
    "BoundaryAdapterIntegrityError",
    "BoundaryCommitNotStarted",
    "FencedBoundaryCommitter",
    "FencedPageFinalizer",
    "PageFinalizationEvidence",
    "PhysicalCohortCommit",
    "PhysicalCohortCommitResult",
    "PhysicalCohortCommitter",
    "PhysicalCohortResult",
    "feed_closure_cap_covers",
    "feed_closure_cap_feed_id",
]


class BoundaryAdapterIntegrityError(RuntimeError):
    """A storage result cannot be correlated to its submitted boundaries."""


class BoundaryCommitNotStarted(RuntimeError):
    """Typed proof that no child-mutation attempt started.

    This exception is retry evidence only when raised synchronously while the
    store awaitable is being created. Once the awaitable exists, every failure
    is outcome-unknown to this adapter and propagates unchanged.
    """


@dataclasses.dataclass(frozen=True, slots=True)
class PhysicalCohortCommit:
    """Exact lifecycle-neutral durability requested by one direct cohort."""

    member: ingestion_lease_store.LeaseMemberIdentity
    last_processed_filename: str | None
    cursor: datetime.datetime | None

    def __post_init__(self) -> None:
        if not isinstance(
            self.member,
            ingestion_lease_store.LeaseMemberIdentity,
        ):
            message = "member must be a LeaseMemberIdentity"
            raise TypeError(message)
        path = self.last_processed_filename
        if path is not None and not isinstance(path, str):
            message = "last_processed_filename must be a string or None"
            raise TypeError(message)
        if isinstance(path, str) and not path.strip():
            message = "last_processed_filename must be nonempty when present"
            raise ValueError(message)
        cursor = self.cursor
        if cursor is not None and not isinstance(cursor, datetime.datetime):
            message = "cursor must be a datetime or None"
            raise TypeError(message)
        if isinstance(
            cursor, datetime.datetime
        ) and cursor.utcoffset() != datetime.timedelta(0):
            message = "cursor must be UTC-aware"
            raise ValueError(message)
        if path is None and cursor is None:
            message = "physical cohort commit requires a path or cursor"
            raise ValueError(message)


@dataclasses.dataclass(frozen=True, slots=True)
class PhysicalCohortResult:
    """One direct cohort result correlated to its exact submitted value."""

    commit: PhysicalCohortCommit
    disposition: feed_work_scheduler.BoundaryDisposition

    def __post_init__(self) -> None:
        if type(self.commit) is not PhysicalCohortCommit:
            message = "commit must be an exact PhysicalCohortCommit"
            raise TypeError(message)
        if not isinstance(
            self.disposition,
            feed_work_scheduler.BoundaryDisposition,
        ):
            message = "disposition must be a BoundaryDisposition"
            raise TypeError(message)
        if self.disposition not in (
            feed_work_scheduler.BoundaryDisposition.COMMITTED,
            feed_work_scheduler.BoundaryDisposition.MEMBER_REJECTED,
        ):
            message = "physical cohort disposition must be terminal"
            raise ValueError(message)


type PhysicalCohortCommitResult = (
    PhysicalCohortResult
    | feed_work_scheduler.BoundaryBatchRetryable
    | feed_work_scheduler.BoundaryGrantRejected
)


class PhysicalCohortCommitter(typing.Protocol):
    """Narrow direct-pipeline protocol for one exact physical cohort."""

    async def commit_cohort(
        self,
        grant: ingestion_lease_store.LeaseGrant,
        commit: PhysicalCohortCommit,
        *,
        final_logical: bool,
    ) -> PhysicalCohortCommitResult:
        """Attempt one physical cohort without lifecycle evidence."""
        ...


_COMMITTED_CHILD_DISPOSITIONS = frozenset(
    (
        ingestion_lease_store.ChildDisposition.APPLIED,
        ingestion_lease_store.ChildDisposition.APPLIED_AFTER_DEACTIVATION,
        ingestion_lease_store.ChildDisposition.ACCEPTED_NOOP,
    )
)
_REJECTED_CHILD_DISPOSITIONS = frozenset(
    (
        ingestion_lease_store.ChildDisposition.MISSING,
        ingestion_lease_store.ChildDisposition.STATUS_INELIGIBLE,
    )
)


class _FinalChildCommandKind(enum.StrEnum):
    """Closed source child-command kind bound into one closure cap."""

    OBSERVATION = "observation"
    FAILURE = "failure"
    NEUTRAL_PROGRESS = "neutral_progress"


_FEED_CLOSURE_CAP_CONSTRUCTION_KEY = object()


@dataclasses.dataclass(frozen=True, slots=True)
class _FeedClosureCapSeal:
    """Immutable original fields used to detect crossed cap mutation."""

    grant: ingestion_lease_store.LeaseGrant
    page_sequence: int
    feed_id: uuid.UUID
    accepted_through: datetime.datetime
    requested_target: datetime.datetime
    command_kind: _FinalChildCommandKind
    result: ingestion_lease_store.ChildMutationResult
    _construction_key: object = dataclasses.field(repr=False, compare=False)


@dataclasses.dataclass(frozen=True, slots=True)
class _FeedClosureCap:
    """Process-local exact-Feed authority issued after durable correlation."""

    grant: ingestion_lease_store.LeaseGrant
    page_sequence: int
    feed_id: uuid.UUID
    accepted_through: datetime.datetime
    requested_target: datetime.datetime
    command_kind: _FinalChildCommandKind
    result: ingestion_lease_store.ChildMutationResult
    _seal: _FeedClosureCapSeal = dataclasses.field(repr=False, compare=False)

    def __post_init__(self) -> None:
        _validate_feed_closure_cap_authority(self)
        _validate_feed_closure_cap_cursor(self)
        _validate_feed_closure_cap_result(self)


def _validate_feed_closure_cap_authority(cap: _FeedClosureCap) -> None:
    seal = cap._seal  # noqa: SLF001
    if type(seal) is not _FeedClosureCapSeal or (
        seal._construction_key  # noqa: SLF001
        is not _FEED_CLOSURE_CAP_CONSTRUCTION_KEY
    ):
        message = "Feed closure cap was not issued by the finalizer"
        raise BoundaryAdapterIntegrityError(message)
    if not isinstance(cap.grant, ingestion_lease_store.LeaseGrant):
        message = "closure cap grant must be a LeaseGrant"
        raise TypeError(message)
    if isinstance(cap.page_sequence, bool) or not isinstance(
        cap.page_sequence,
        int,
    ):
        message = "closure cap page_sequence must be an integer"
        raise TypeError(message)
    if cap.page_sequence < 0:
        message = "closure cap page_sequence must be nonnegative"
        raise ValueError(message)
    if not isinstance(cap.feed_id, uuid.UUID):
        message = "closure cap feed_id must be a UUID"
        raise TypeError(message)


def _validate_feed_closure_cap_cursor(cap: _FeedClosureCap) -> None:
    for name, value in (
        ("accepted_through", cap.accepted_through),
        ("requested_target", cap.requested_target),
    ):
        if not isinstance(value, datetime.datetime):
            message = f"closure cap {name} must be a datetime"
            raise TypeError(message)
        if value.utcoffset() != datetime.timedelta(0):
            message = f"closure cap {name} must be UTC-aware"
            raise ValueError(message)
    if cap.accepted_through < cap.requested_target:
        message = "closure cap does not cross its requested target"
        raise BoundaryAdapterIntegrityError(message)


def _validate_feed_closure_cap_result(cap: _FeedClosureCap) -> None:
    if not isinstance(cap.command_kind, _FinalChildCommandKind):
        message = "closure cap command_kind is invalid"
        raise TypeError(message)
    if type(cap.result) is not ingestion_lease_store.ChildMutationResult:
        message = "closure cap result must be exact ChildMutationResult"
        raise TypeError(message)
    if cap.result.feed_id != cap.feed_id:
        message = "closure cap result crossed Feed identity"
        raise BoundaryAdapterIntegrityError(message)
    seal = cap._seal  # noqa: SLF001
    original = (
        seal.grant,
        seal.page_sequence,
        seal.feed_id,
        seal.accepted_through,
        seal.requested_target,
        seal.command_kind,
        seal.result,
    )
    current = (
        cap.grant,
        cap.page_sequence,
        cap.feed_id,
        cap.accepted_through,
        cap.requested_target,
        cap.command_kind,
        cap.result,
    )
    if current != original or cap.result is not seal.result:
        message = "Feed closure cap fields crossed their private seal"
        raise BoundaryAdapterIntegrityError(message)


def _require_feed_closure_cap(value: object) -> _FeedClosureCap:
    if type(value) is not _FeedClosureCap:
        message = "value must be an exact Feed closure cap"
        raise TypeError(message)
    value.__post_init__()
    return value


def feed_closure_cap_feed_id(value: object) -> uuid.UUID:
    """Return the Feed UUID from one validated private closure cap."""
    return _require_feed_closure_cap(value).feed_id


def feed_closure_cap_covers(
    value: object,
    *,
    grant: ingestion_lease_store.LeaseGrant,
    page_sequence: int,
    feed_id: uuid.UUID,
    requested_target: datetime.datetime,
) -> bool:
    """Validate whether one private cap covers the exact requested scope."""
    cap = _require_feed_closure_cap(value)
    return (
        cap.grant == grant
        and cap.page_sequence == page_sequence
        and cap.feed_id == feed_id
        and cap.requested_target == requested_target
        and cap.accepted_through >= requested_target
    )


@dataclasses.dataclass(frozen=True, slots=True)
class PageFinalizationEvidence:
    """Bounded source evidence returned by one accepted final transaction."""

    verdict: boundary_verdict.PageVerdict
    closure_caps: tuple[_FeedClosureCap, ...]
    quarantined_feed_ids: tuple[uuid.UUID, ...]
    item_to_feed_promotion_count: int

    def __post_init__(self) -> None:
        if type(self.verdict) is not boundary_verdict.PageVerdict:
            message = "verdict must be an exact PageVerdict"
            raise TypeError(message)
        if not isinstance(self.closure_caps, tuple):
            message = "closure_caps must be an immutable tuple"
            raise TypeError(message)
        caps = tuple(
            _require_feed_closure_cap(cap) for cap in self.closure_caps
        )
        cap_feed_ids = tuple(cap.feed_id for cap in caps)
        if len(set(cap_feed_ids)) != len(cap_feed_ids):
            message = "closure_caps repeat a Feed"
            raise BoundaryAdapterIntegrityError(message)
        if cap_feed_ids != tuple(
            sorted(cap_feed_ids, key=lambda value: value.int)
        ):
            message = "closure_caps must be in deterministic Feed order"
            raise BoundaryAdapterIntegrityError(message)
        if not isinstance(self.quarantined_feed_ids, tuple) or any(
            not isinstance(feed_id, uuid.UUID)
            for feed_id in self.quarantined_feed_ids
        ):
            message = "quarantined_feed_ids must be an immutable UUID tuple"
            raise TypeError(message)
        if self.quarantined_feed_ids != tuple(
            sorted(set(self.quarantined_feed_ids), key=lambda value: value.int)
        ):
            message = "quarantined_feed_ids must be unique and ordered"
            raise BoundaryAdapterIntegrityError(message)
        count = self.item_to_feed_promotion_count
        if isinstance(count, bool) or not isinstance(count, int):
            message = "item_to_feed_promotion_count must be an integer"
            raise TypeError(message)
        if count != self.verdict.item_to_feed_promotion_count:
            message = "source evidence changed the verdict promotion count"
            raise BoundaryAdapterIntegrityError(message)


def _require_neutral_lease_result(value: object) -> None:
    """Reject malformed or contradictory lifecycle-neutral evidence."""
    if type(value) is not ingestion_lease_store.LeaseLifecycleResult:
        message = "neutral storage batch returned an invalid Lease result"
        raise BoundaryAdapterIntegrityError(message)
    if value.effect is not ingestion_lease_store.LeaseLifecycleEffect.NONE:
        message = "neutral storage batch returned contradictory evidence"
        raise BoundaryAdapterIntegrityError(message)


class FencedBoundaryCommitter:
    """Translate immutable boundaries into one exact-grant attempt."""

    __slots__ = ("_actor_id", "_store")

    def __init__(
        self,
        store: ingestion_lease_store.IngestionLeaseStore,
        *,
        actor_id: str,
    ) -> None:
        """Create the one-attempt adapter around an ingestion Lease store.

        Args:
            store: Store exposing the fenced child-mutation operation.
            actor_id: Stable audit actor forwarded to the storage boundary.
        """
        if not callable(getattr(store, "commit_child_mutations", None)):
            message = "store must provide async commit_child_mutations"
            raise TypeError(message)
        if not isinstance(actor_id, str):
            message = "actor_id must be a string"
            raise TypeError(message)
        if (
            not actor_id
            or len(actor_id) > 512
            or any(character.isspace() for character in actor_id)
        ):
            message = (
                "actor_id must be nonempty, at most 512 chars, and "
                "whitespace-free"
            )
            raise ValueError(message)
        self._store = store
        self._actor_id = actor_id

    async def commit(
        self,
        grant: ingestion_lease_store.LeaseGrant,
        boundaries: tuple[feed_work_scheduler.BoundaryWork, ...],
        *,
        final_logical: bool,
    ) -> (
        feed_work_scheduler.BoundaryBatchCommitted
        | feed_work_scheduler.BoundaryBatchRetryable
        | feed_work_scheduler.BoundaryGrantRejected
    ):
        """Make one fenced mutation attempt and return closed batch evidence."""
        if not isinstance(grant, ingestion_lease_store.LeaseGrant):
            message = "grant must be a LeaseGrant"
            raise TypeError(message)
        if not isinstance(boundaries, tuple):
            message = "boundaries must be an immutable tuple"
            raise TypeError(message)
        for boundary in boundaries:
            if type(boundary) is not feed_work_scheduler.BoundaryWork:
                message = "boundaries must contain exact BoundaryWork values"
                raise TypeError(message)
        if not isinstance(final_logical, bool):
            message = "final_logical must be a bool"
            raise TypeError(message)

        mutations = tuple(
            ingestion_lease_store.ClosedCohortProgress(
                member=boundary.member,
                last_processed_filename=None,
                cursor=boundary.target,
            )
            for boundary in boundaries
        )
        result = await self._commit_mutations(
            grant,
            mutations,
            tuple(boundary.feed_id for boundary in boundaries),
        )
        if isinstance(
            result,
            (
                feed_work_scheduler.BoundaryBatchRetryable,
                feed_work_scheduler.BoundaryGrantRejected,
            ),
        ):
            return result

        correlated = []
        for boundary, child in zip(
            boundaries,
            result,
            strict=True,
        ):
            disposition = self._map_disposition(child.disposition)
            correlated.append(
                feed_work_scheduler.BoundaryResult(
                    boundary=boundary,
                    disposition=disposition,
                )
            )
        return feed_work_scheduler.BoundaryBatchCommitted(tuple(correlated))

    async def commit_cohort(
        self,
        grant: ingestion_lease_store.LeaseGrant,
        commit: PhysicalCohortCommit,
        *,
        final_logical: bool,
    ) -> PhysicalCohortCommitResult:
        """Make one lifecycle-neutral attempt for a direct physical cohort."""
        if not isinstance(grant, ingestion_lease_store.LeaseGrant):
            message = "grant must be a LeaseGrant"
            raise TypeError(message)
        if type(commit) is not PhysicalCohortCommit:
            message = "commit must be an exact PhysicalCohortCommit"
            raise TypeError(message)
        if not isinstance(final_logical, bool):
            message = "final_logical must be a bool"
            raise TypeError(message)

        mutation = ingestion_lease_store.ClosedCohortProgress(
            member=commit.member,
            last_processed_filename=commit.last_processed_filename,
            cursor=commit.cursor,
        )
        result = await self._commit_mutations(
            grant,
            (mutation,),
            (commit.member.feed_id,),
        )
        if isinstance(
            result,
            (
                feed_work_scheduler.BoundaryBatchRetryable,
                feed_work_scheduler.BoundaryGrantRejected,
            ),
        ):
            return result
        return PhysicalCohortResult(
            commit,
            self._map_disposition(result[0].disposition),
        )

    async def _commit_mutations(
        self,
        grant: ingestion_lease_store.LeaseGrant,
        mutations: tuple[ingestion_lease_store.ClosedCohortProgress, ...],
        expected_feed_ids: tuple[object, ...],
    ) -> (
        tuple[ingestion_lease_store.ChildMutationResult, ...]
        | feed_work_scheduler.BoundaryBatchRetryable
        | feed_work_scheduler.BoundaryGrantRejected
    ):
        """Start once, then propagate every may-have-started failure."""
        batch = ingestion_lease_store.ChildMutationBatch(
            mutations=mutations,
            lease_effect=ingestion_lease_store.NoLeaseEffect(),
        )
        try:
            attempt = self._store.commit_child_mutations(
                grant,
                batch,
                actor_id=self._actor_id,
            )
        except BoundaryCommitNotStarted:
            return feed_work_scheduler.BoundaryBatchRetryable()
        if not isinstance(attempt, collections.abc.Awaitable):
            message = "storage did not return an awaitable child attempt"
            raise BoundaryAdapterIntegrityError(message)

        result = await attempt
        if type(result) is ingestion_lease_store.GrantRejected:
            return feed_work_scheduler.BoundaryGrantRejected()
        if type(result) is not ingestion_lease_store.BatchCommitted:
            message = "storage returned outside the closed batch vocabulary"
            raise BoundaryAdapterIntegrityError(message)
        _require_neutral_lease_result(result.lease_effect)
        if type(result.children) is not tuple:
            message = "storage child results must be an immutable tuple"
            raise BoundaryAdapterIntegrityError(message)
        if len(result.children) != len(expected_feed_ids):
            message = "storage child result count does not match commands"
            raise BoundaryAdapterIntegrityError(message)

        for expected_feed_id, child in zip(
            expected_feed_ids,
            result.children,
            strict=True,
        ):
            if type(child) is not ingestion_lease_store.ChildMutationResult:
                message = "storage returned an unknown child result"
                raise BoundaryAdapterIntegrityError(message)
            if child.feed_id != expected_feed_id:
                message = "storage child result order or Feed does not match"
                raise BoundaryAdapterIntegrityError(message)
            if not isinstance(
                child.cursor_effect,
                ingestion_lease_store.CursorEffect,
            ):
                message = "storage returned an unknown cursor effect"
                raise BoundaryAdapterIntegrityError(message)
            if (
                child.lifecycle_effect
                is not ingestion_lease_store.LifecycleEffect.NONE
            ):
                message = "neutral child returned a lifecycle effect"
                raise BoundaryAdapterIntegrityError(message)
        return result.children

    @staticmethod
    def _map_disposition(
        disposition: object,
    ) -> feed_work_scheduler.BoundaryDisposition:
        if not isinstance(
            disposition,
            ingestion_lease_store.ChildDisposition,
        ):
            message = "storage returned an unsupported child disposition"
            raise BoundaryAdapterIntegrityError(message)
        if disposition in _COMMITTED_CHILD_DISPOSITIONS:
            return feed_work_scheduler.BoundaryDisposition.COMMITTED
        if disposition in _REJECTED_CHILD_DISPOSITIONS:
            return feed_work_scheduler.BoundaryDisposition.MEMBER_REJECTED
        message = "storage returned an unsupported child disposition"
        raise BoundaryAdapterIntegrityError(message)


def _page_verdict_input(
    context: feed_work_scheduler.PageFinalizationContext,
) -> boundary_verdict.PageVerdictInput:
    """Project scheduler-owned exact facts into the pure source policy."""
    excluded_feed_ids = set(context.replay_blocked_feed_ids)
    excluded_feed_ids.update(
        member.feed_id for member in context.locally_retired_members
    )
    facts = tuple(
        cohort
        for cohort in context.cohort_terminal_facts
        if cohort.records[0].identity.feed_id not in excluded_feed_ids
    )
    members_by_id: dict[
        uuid.UUID,
        ingestion_lease_store.LeaseMemberIdentity,
    ] = {}
    for boundary in context.candidate_boundaries:
        members_by_id[boundary.feed_id] = boundary.member
    for cohort in facts:
        member = cohort.records[0].identity.member
        existing = members_by_id.get(member.feed_id)
        if existing is not None and existing is not member:
            message = "finalization facts crossed candidate member identity"
            raise BoundaryAdapterIntegrityError(message)
        members_by_id[member.feed_id] = member
    members = tuple(
        members_by_id[feed_id]
        for feed_id in sorted(members_by_id, key=lambda value: value.int)
    )
    identities = tuple(
        record.identity for cohort in facts for record in cohort.records
    )
    return boundary_verdict.PageVerdictInput(
        grant=context.grant,
        page_sequence=context.page_sequence,
        members=members,
        record_identities=identities,
        cohort_terminal_facts=facts,
        child_recovery_candidates=members,
        lease_recovery_candidate=True,
    )


def _requested_target(
    candidate: cursor_policy.PageCandidate,
) -> datetime.datetime | None:
    if type(candidate) is cursor_policy.PageCursorCandidate:
        return candidate.last_pos
    if type(candidate) is cursor_policy.NoProgressPageCandidate:
        return None
    message = "candidate must be an exact PageCandidate"
    raise TypeError(message)


def _require_boundary_settled_utc(value: object) -> datetime.datetime:
    if not isinstance(value, datetime.datetime):
        message = "boundary_settled_utc must return a datetime"
        raise TypeError(message)
    if value.utcoffset() != datetime.timedelta(0):
        message = "boundary_settled_utc must return a UTC-aware datetime"
        raise ValueError(message)
    return value


def _require_finalizer_policy(
    value: object,
) -> ingestion_lease_store.BudgetedFailure:
    if type(value) is not ingestion_lease_store.BudgetedFailure:
        message = "budgeted_failure must be an exact BudgetedFailure"
        raise TypeError(message)
    validated = ingestion_lease_store.BudgetedFailure(
        value.failure_threshold,
        value.backoff_base_sec,
        value.backoff_max_sec,
    )
    if validated != value:
        message = "budgeted_failure changed after validation"
        raise BoundaryAdapterIntegrityError(message)
    return value


def _final_child_command_kind(
    mutation: ingestion_lease_store.ChildMutation,
) -> _FinalChildCommandKind:
    if type(mutation) is ingestion_lease_store.SourceObservation:
        return _FinalChildCommandKind.OBSERVATION
    if type(mutation) is ingestion_lease_store.FeedFailureTransition:
        return _FinalChildCommandKind.FAILURE
    if type(mutation) is ingestion_lease_store.ClosedCohortProgress:
        return _FinalChildCommandKind.NEUTRAL_PROGRESS
    message = "final child mutation has an unsupported exact type"
    raise BoundaryAdapterIntegrityError(message)


def _final_child_cursor(
    mutation: ingestion_lease_store.ChildMutation,
) -> datetime.datetime | None:
    if type(mutation) is ingestion_lease_store.FeedFailureTransition:
        return mutation.completion_cursor
    if type(mutation) is ingestion_lease_store.SourceObservation:
        return mutation.cursor
    if type(mutation) is ingestion_lease_store.ClosedCohortProgress:
        return mutation.cursor
    message = "final child mutation has an unsupported cursor owner"
    raise BoundaryAdapterIntegrityError(message)


def _require_final_lease_result(
    plan: boundary_verdict.FinalMutationPlan,
    value: object,
) -> ingestion_lease_store.LeaseLifecycleResult:
    if type(value) is not ingestion_lease_store.LeaseLifecycleResult:
        message = "final storage batch returned an invalid Lease result"
        raise BoundaryAdapterIntegrityError(message)
    if not isinstance(
        value.effect,
        ingestion_lease_store.LeaseLifecycleEffect,
    ):
        message = "final storage batch returned malformed Lease evidence"
        raise BoundaryAdapterIntegrityError(message)
    if type(plan.lease_effect) is ingestion_lease_store.NoLeaseEffect and (
        value.effect is not ingestion_lease_store.LeaseLifecycleEffect.NONE
    ):
        message = "promoted finalization changed parent Lease lifecycle"
        raise BoundaryAdapterIntegrityError(message)
    if type(plan.lease_effect) is (
        ingestion_lease_store.FinalizeLeaseRecovery
    ) and value.effect not in {
        ingestion_lease_store.LeaseLifecycleEffect.NONE,
        ingestion_lease_store.LeaseLifecycleEffect.RECOVERED,
    }:
        message = "non-promoted finalization returned unknown Lease effect"
        raise BoundaryAdapterIntegrityError(message)
    return value


def _require_final_child_result(
    command: boundary_verdict.FinalMutationCommand,
    value: object,
) -> ingestion_lease_store.ChildMutationResult:
    if type(value) is not ingestion_lease_store.ChildMutationResult:
        message = "final storage batch returned an unknown child result"
        raise BoundaryAdapterIntegrityError(message)
    if value.feed_id != command.mutation.member.feed_id:
        message = "final storage child result crossed Feed or order"
        raise BoundaryAdapterIntegrityError(message)
    if not isinstance(
        value.disposition, ingestion_lease_store.ChildDisposition
    ):
        message = "final storage child disposition is invalid"
        raise BoundaryAdapterIntegrityError(message)
    if not isinstance(value.cursor_effect, ingestion_lease_store.CursorEffect):
        message = "final storage child cursor effect is invalid"
        raise BoundaryAdapterIntegrityError(message)
    if not isinstance(
        value.lifecycle_effect,
        ingestion_lease_store.LifecycleEffect,
    ):
        message = "final storage child lifecycle effect is invalid"
        raise BoundaryAdapterIntegrityError(message)
    accepted = value.disposition in _COMMITTED_CHILD_DISPOSITIONS
    rejected = value.disposition in _REJECTED_CHILD_DISPOSITIONS
    if not accepted and not rejected:
        message = "final storage child disposition is unsupported"
        raise BoundaryAdapterIntegrityError(message)
    if rejected and value.lifecycle_effect is not (
        ingestion_lease_store.LifecycleEffect.NONE
    ):
        message = "rejected child returned a lifecycle effect"
        raise BoundaryAdapterIntegrityError(message)
    mutation = command.mutation
    if type(mutation) is ingestion_lease_store.ClosedCohortProgress:
        allowed_effects = {ingestion_lease_store.LifecycleEffect.NONE}
    elif type(mutation) is ingestion_lease_store.SourceObservation:
        allowed_effects = {
            ingestion_lease_store.LifecycleEffect.NONE,
            ingestion_lease_store.LifecycleEffect.RECOVERED,
            ingestion_lease_store.LifecycleEffect.CLEARED_WHILE_DEACTIVATED,
        }
    else:
        allowed_effects = {
            ingestion_lease_store.LifecycleEffect.NONE,
            ingestion_lease_store.LifecycleEffect.FAILURE_RECORDED,
            ingestion_lease_store.LifecycleEffect.QUARANTINED,
        }
    if value.lifecycle_effect not in allowed_effects:
        message = "child lifecycle effect crossed its submitted command"
        raise BoundaryAdapterIntegrityError(message)
    return value


def _mint_feed_closure_caps(
    context: feed_work_scheduler.PageFinalizationContext,
    plan: boundary_verdict.FinalMutationPlan,
    results: tuple[ingestion_lease_store.ChildMutationResult, ...],
) -> tuple[_FeedClosureCap, ...]:
    target = plan.requested_target
    if target is None:
        return ()
    unresolved = set(context.unresolved_replay_feed_ids)
    blocked = set(context.replay_blocked_feed_ids)
    qualifying_effects = {
        ingestion_lease_store.CursorEffect.INITIALIZED,
        ingestion_lease_store.CursorEffect.ADVANCED,
        ingestion_lease_store.CursorEffect.EQUAL,
        ingestion_lease_store.CursorEffect.REGRESSIVE,
    }
    caps_by_feed: dict[uuid.UUID, _FeedClosureCap] = {}
    for command, result in zip(plan.commands, results, strict=True):
        feed_id = command.mutation.member.feed_id
        submitted_cursor = _final_child_cursor(command.mutation)
        if (
            submitted_cursor is None
            or submitted_cursor < target
            or feed_id in unresolved
            or feed_id in blocked
            or result.disposition not in _COMMITTED_CHILD_DISPOSITIONS
            or result.cursor_effect not in qualifying_effects
        ):
            continue
        cap = _FeedClosureCap(
            grant=context.grant,
            page_sequence=context.page_sequence,
            feed_id=feed_id,
            accepted_through=submitted_cursor,
            requested_target=target,
            command_kind=_final_child_command_kind(command.mutation),
            result=result,
            _seal=_FeedClosureCapSeal(
                grant=context.grant,
                page_sequence=context.page_sequence,
                feed_id=feed_id,
                accepted_through=submitted_cursor,
                requested_target=target,
                command_kind=_final_child_command_kind(command.mutation),
                result=result,
                _construction_key=_FEED_CLOSURE_CAP_CONSTRUCTION_KEY,
            ),
        )
        current = caps_by_feed.get(feed_id)
        if current is None or cap.accepted_through > current.accepted_through:
            caps_by_feed[feed_id] = cap
    return tuple(
        caps_by_feed[feed_id]
        for feed_id in sorted(caps_by_feed, key=lambda value: value.int)
    )


def _final_closure_resolutions(
    context: feed_work_scheduler.PageFinalizationContext,
    caps: tuple[_FeedClosureCap, ...],
    locally_rejected_members: tuple[
        ingestion_lease_store.LeaseMemberIdentity,
        ...,
    ],
) -> tuple[feed_work_scheduler.FinalRecordClosureResolution, ...] | None:
    """Resolve pending records with the most specific accepted authority."""
    closure = feed_work_scheduler.CohortRecordClosureState
    basis_kind = feed_work_scheduler.FinalRecordReleaseBasis
    caps_by_feed = {cap.feed_id: cap for cap in caps}
    unresolved = set(context.unresolved_replay_feed_ids)
    retired_members = {
        id(member): member for member in context.locally_retired_members
    }
    retired_members.update(
        {id(member): member for member in locally_rejected_members}
    )
    resolutions = []
    for facts in context.cohort_terminal_facts:
        for record in facts.records:
            if record.closure_state is not (
                feed_work_scheduler.CohortRecordClosureState.FINAL_CLOSURE_PENDING
            ):
                continue
            identity = record.identity
            if identity.feed_id in caps_by_feed:
                closure_state = closure.DURABLY_CLOSED
                basis = basis_kind.DURABLE_SOURCE_CLOSURE
            elif id(identity.member) in retired_members:
                closure_state = closure.REPLAY_SAFE_RELEASE
                basis = basis_kind.ACCEPTED_MEMBER_RETIREMENT
            elif type(context.candidate) is (
                cursor_policy.NoProgressPageCandidate
            ):
                closure_state = closure.REPLAY_SAFE_RELEASE
                basis = basis_kind.ACCEPTED_NO_PROGRESS
            elif identity.feed_id in unresolved:
                closure_state = closure.REPLAY_SAFE_RELEASE
                basis = basis_kind.ACCEPTED_REPLAYABLE_FEED
            else:
                return None
            resolutions.append(
                feed_work_scheduler.FinalRecordClosureResolution(
                    identity=identity,
                    closure_state=closure_state,
                    release_basis=basis,
                )
            )
    return tuple(resolutions)


def _accepted_member_retirements(
    context: feed_work_scheduler.PageFinalizationContext,
    locally_rejected_members: tuple[
        ingestion_lease_store.LeaseMemberIdentity,
        ...,
    ],
) -> tuple[ingestion_lease_store.LeaseMemberIdentity, ...]:
    """Return one deterministic exact member tuple for accepted retirement."""
    members_by_feed: dict[
        uuid.UUID,
        ingestion_lease_store.LeaseMemberIdentity,
    ] = {}
    for member in (
        *context.locally_retired_members,
        *locally_rejected_members,
    ):
        existing = members_by_feed.get(member.feed_id)
        if existing is not None and existing is not member:
            message = "accepted retirement crossed issued member identity"
            raise BoundaryAdapterIntegrityError(message)
        members_by_feed[member.feed_id] = member
    return tuple(
        members_by_feed[feed_id]
        for feed_id in sorted(members_by_feed, key=lambda value: value.int)
    )


def _final_outcome_unknown(
    context: feed_work_scheduler.PageFinalizationContext,
) -> feed_work_scheduler.FinalPageOutcomeUnknown:
    return feed_work_scheduler.FinalPageOutcomeUnknown(
        context.grant,
        context.page_sequence,
        context.candidate,
    )


type _FinalPageResult = (
    feed_work_scheduler.FinalPageCovered
    | feed_work_scheduler.FinalPageNoProgress
    | feed_work_scheduler.FinalPageReplayable
    | feed_work_scheduler.FinalPageRetryable
    | feed_work_scheduler.FinalPageGrantRejected
    | feed_work_scheduler.FinalPageOutcomeUnknown
)


async def _commit_final_batch_once(
    store: ingestion_lease_store.IngestionLeaseStore,
    actor_id: str,
    context: feed_work_scheduler.PageFinalizationContext,
    batch: ingestion_lease_store.ChildMutationBatch,
) -> ingestion_lease_store.BatchCommitted | _FinalPageResult:
    """Start once and classify only definitive pre-start versus uncertainty."""
    try:
        attempt = store.commit_child_mutations(
            context.grant,
            batch,
            actor_id=actor_id,
        )
    except BoundaryCommitNotStarted:
        return feed_work_scheduler.FinalPageRetryable(
            context.grant,
            context.page_sequence,
            context.candidate,
            commit_child_mutations_started=False,
            mutation_could_have_committed=False,
        )
    except Exception:
        return _final_outcome_unknown(context)

    committed: object = None
    if isinstance(attempt, collections.abc.Awaitable):
        try:
            committed = await attempt
        except Exception:
            committed = None
    if type(committed) is ingestion_lease_store.GrantRejected:
        return feed_work_scheduler.FinalPageGrantRejected(
            context.grant,
            context.page_sequence,
            context.candidate,
        )
    if type(committed) is ingestion_lease_store.BatchCommitted:
        return committed
    return _final_outcome_unknown(context)


class FencedPageFinalizer:
    """Commit one exact source lifecycle matrix for a finalized SID page."""

    __slots__ = (
        "_actor_id",
        "_boundary_settled_utc",
        "_budgeted_failure",
        "_store",
    )

    def __init__(
        self,
        store: ingestion_lease_store.IngestionLeaseStore,
        *,
        actor_id: str,
        budgeted_failure: ingestion_lease_store.BudgetedFailure,
        boundary_settled_utc: typing.Callable[[], datetime.datetime],
    ) -> None:
        """Create an exact one-attempt source page finalizer.

        Args:
            store: Existing deep fenced child-mutation owner.
            actor_id: Stable audit actor for the final transaction.
            budgeted_failure: Mandatory explicit runtime child policy.
            boundary_settled_utc: Injected UTC supplier called once per page.
        """
        if not callable(getattr(store, "commit_child_mutations", None)):
            message = "store must provide async commit_child_mutations"
            raise TypeError(message)
        if not isinstance(actor_id, str):
            message = "actor_id must be a string"
            raise TypeError(message)
        if (
            not actor_id
            or len(actor_id) > 512
            or any(character.isspace() for character in actor_id)
        ):
            message = (
                "actor_id must be nonempty, at most 512 chars, and "
                "whitespace-free"
            )
            raise ValueError(message)
        if not callable(boundary_settled_utc):
            message = "boundary_settled_utc must be callable"
            raise TypeError(message)
        self._store = store
        self._actor_id = actor_id
        self._budgeted_failure = _require_finalizer_policy(budgeted_failure)
        self._boundary_settled_utc = boundary_settled_utc

    @property
    def budgeted_failure(self) -> ingestion_lease_store.BudgetedFailure:
        """Return the exact runtime policy object retained by identity."""
        return self._budgeted_failure

    async def finalize_page(
        self,
        context: feed_work_scheduler.PageFinalizationContext,
    ) -> _FinalPageResult:
        """Attempt one exact final child transaction without resubmission."""
        if type(context) is not feed_work_scheduler.PageFinalizationContext:
            message = "context must be exact PageFinalizationContext"
            raise TypeError(message)
        settled_utc = _require_boundary_settled_utc(
            self._boundary_settled_utc()
        )
        verdict_input = _page_verdict_input(context)
        verdict = boundary_verdict.decide_page_verdict(verdict_input)
        plan = boundary_verdict.plan_final_mutations(
            verdict,
            candidate_boundaries=context.candidate_boundaries,
            unresolved_replay_feed_ids=context.unresolved_replay_feed_ids,
            replay_blocked_feed_ids=context.replay_blocked_feed_ids,
            locally_retired_members=context.locally_retired_members,
            requested_target=_requested_target(context.candidate),
            boundary_settled_utc=settled_utc,
            budgeted_failure=self._budgeted_failure,
        )
        batch = ingestion_lease_store.ChildMutationBatch(
            mutations=tuple(command.mutation for command in plan.commands),
            lease_effect=plan.lease_effect,
        )
        committed = await _commit_final_batch_once(
            self._store,
            self._actor_id,
            context,
            batch,
        )
        if type(committed) is not ingestion_lease_store.BatchCommitted:
            return typing.cast("_FinalPageResult", committed)
        try:
            return self._accepted_result(context, plan, committed)
        except (TypeError, ValueError, BoundaryAdapterIntegrityError):
            return _final_outcome_unknown(context)

    def _accepted_result(
        self,
        context: feed_work_scheduler.PageFinalizationContext,
        plan: boundary_verdict.FinalMutationPlan,
        committed: ingestion_lease_store.BatchCommitted,
    ) -> _FinalPageResult:
        _require_final_lease_result(plan, committed.lease_effect)
        if type(committed.children) is not tuple or len(
            committed.children
        ) != len(plan.commands):
            message = "final child result cardinality changed"
            raise BoundaryAdapterIntegrityError(message)
        results = tuple(
            _require_final_child_result(command, result)
            for command, result in zip(
                plan.commands,
                committed.children,
                strict=True,
            )
        )
        boundary_results = tuple(
            feed_work_scheduler.BoundaryResult(
                typing.cast(
                    "feed_work_scheduler.BoundaryWork",
                    command.boundary,
                ),
                (
                    feed_work_scheduler.BoundaryDisposition.COMMITTED
                    if result.disposition in _COMMITTED_CHILD_DISPOSITIONS
                    else feed_work_scheduler.BoundaryDisposition.MEMBER_REJECTED
                ),
            )
            for command, result in zip(
                plan.commands[: plan.boundary_command_count],
                results[: plan.boundary_command_count],
                strict=True,
            )
        )
        caps = _mint_feed_closure_caps(context, plan, results)
        locally_rejected_members = tuple(
            command.mutation.member
            for command, result in zip(plan.commands, results, strict=True)
            if result.disposition in _REJECTED_CHILD_DISPOSITIONS
        )
        resolutions = _final_closure_resolutions(
            context,
            caps,
            locally_rejected_members,
        )
        if resolutions is None:
            message = "pending final records lack exact closure evidence"
            raise BoundaryAdapterIntegrityError(message)
        quarantined = tuple(
            sorted(
                {
                    result.feed_id
                    for result in results
                    if result.lifecycle_effect
                    is ingestion_lease_store.LifecycleEffect.QUARANTINED
                },
                key=lambda value: value.int,
            )
        )
        evidence = PageFinalizationEvidence(
            verdict=plan.verdict,
            closure_caps=caps,
            quarantined_feed_ids=quarantined,
            item_to_feed_promotion_count=(
                plan.verdict.item_to_feed_promotion_count
            ),
        )
        accepted = {
            "grant": context.grant,
            "page_sequence": context.page_sequence,
            "candidate": context.candidate,
            "boundary_results": boundary_results,
            "final_closure_resolutions": resolutions,
            "source_evidence": evidence,
            "member_retirements": _accepted_member_retirements(
                context,
                locally_rejected_members,
            ),
        }
        if type(context.candidate) is cursor_policy.NoProgressPageCandidate:
            return feed_work_scheduler.FinalPageNoProgress(**accepted)
        if context.unresolved_replay_feed_ids:
            return feed_work_scheduler.FinalPageReplayable(**accepted)
        return feed_work_scheduler.FinalPageCovered(**accepted)
