"""Runtime adapters connecting bounded Calls work to fenced storage."""

from __future__ import annotations

import asyncpg

from backend.pipeline.ingestion import feed_work_scheduler
from backend.pipeline.storage import ingestion_lease_store

__all__ = [
    "BoundaryAdapterIntegrityError",
    "FencedBoundaryCommitter",
]


class BoundaryAdapterIntegrityError(RuntimeError):
    """A storage result cannot be correlated to its submitted boundaries."""


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


class FencedBoundaryCommitter:
    """Translate immutable quiet boundaries into one exact-grant attempt."""

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

        batch = ingestion_lease_store.ChildMutationBatch(
            mutations=tuple(
                ingestion_lease_store.SourceObservation(
                    member=boundary.member,
                    cursor=boundary.target,
                )
                for boundary in boundaries
            ),
            lease_effect=ingestion_lease_store.NoLeaseEffect(),
        )
        try:
            result = await self._store.commit_child_mutations(
                grant,
                batch,
                actor_id=self._actor_id,
            )
        except (
            asyncpg.PostgresConnectionError,
            asyncpg.InterfaceError,
            OSError,
        ):
            return feed_work_scheduler.BoundaryBatchRetryable()

        if type(result) is ingestion_lease_store.GrantRejected:
            return feed_work_scheduler.BoundaryGrantRejected()
        if type(result) is not ingestion_lease_store.BatchCommitted:
            message = "storage returned outside the closed batch vocabulary"
            raise BoundaryAdapterIntegrityError(message)
        if len(result.children) != len(boundaries):
            message = "storage child result count does not match boundaries"
            raise BoundaryAdapterIntegrityError(message)

        correlated = []
        for boundary, child in zip(
            boundaries,
            result.children,
            strict=True,
        ):
            if type(child) is not ingestion_lease_store.ChildMutationResult:
                message = "storage returned an unknown child result"
                raise BoundaryAdapterIntegrityError(message)
            if child.feed_id != boundary.feed_id:
                message = "storage child result order or Feed does not match"
                raise BoundaryAdapterIntegrityError(message)
            disposition = self._map_disposition(child.disposition)
            correlated.append(
                feed_work_scheduler.BoundaryResult(
                    boundary=boundary,
                    disposition=disposition,
                )
            )
        return feed_work_scheduler.BoundaryBatchCommitted(tuple(correlated))

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
