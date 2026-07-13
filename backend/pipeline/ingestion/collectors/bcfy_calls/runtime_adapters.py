"""Runtime adapters connecting bounded Calls work to fenced storage."""

from __future__ import annotations

import collections.abc
import dataclasses
import datetime
import typing

from backend.pipeline.ingestion import feed_work_scheduler
from backend.pipeline.storage import ingestion_lease_store

__all__ = [
    "BoundaryAdapterIntegrityError",
    "BoundaryCommitNotStarted",
    "FencedBoundaryCommitter",
    "PhysicalCohortCommit",
    "PhysicalCohortCommitResult",
    "PhysicalCohortCommitter",
    "PhysicalCohortResult",
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


def _require_neutral_lease_result(value: object) -> None:
    """Reject malformed or contradictory lifecycle-neutral evidence."""
    if type(value) is not ingestion_lease_store.LeaseLifecycleResult:
        message = "neutral storage batch returned an invalid Lease result"
        raise BoundaryAdapterIntegrityError(message)
    if (
        value.effect is not ingestion_lease_store.LeaseLifecycleEffect.NONE
        or type(value.before_snapshot)
        is not ingestion_lease_store.LeaseSnapshot
        or type(value.after_snapshot) is not ingestion_lease_store.LeaseSnapshot
        or value.before_snapshot != value.after_snapshot
    ):
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
