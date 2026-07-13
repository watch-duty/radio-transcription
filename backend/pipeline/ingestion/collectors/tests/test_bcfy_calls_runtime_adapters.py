"""One-attempt storage adapter tests for Broadcastify Calls cohorts."""

from __future__ import annotations

import asyncio
import dataclasses
import datetime
import typing
import unittest
import uuid
from unittest import mock

import asyncpg

from backend.pipeline.ingestion import feed_work_scheduler
from backend.pipeline.ingestion.collectors.bcfy_calls import runtime_adapters
from backend.pipeline.storage import feed_store, ingestion_lease_store

_ACTOR_ID = "service_account:gcp:collector"
_OWNER_ID = uuid.UUID("11111111-2222-3333-4444-555555555555")
_NOW = datetime.datetime(2026, 7, 12, 12, 0, tzinfo=datetime.UTC)


def _grant(
    lease_key: str = "150",
    *,
    fencing_token: int = 1,
) -> ingestion_lease_store.LeaseGrant:
    return ingestion_lease_store.LeaseGrant(
        source_type=feed_store.SourceType.BCFY_CALLS,
        lease_key=lease_key,
        owner_worker_id=_OWNER_ID,
        fencing_token=fencing_token,
    )


def _snapshot() -> ingestion_lease_store.LeaseSnapshot:
    return ingestion_lease_store.LeaseSnapshot(
        status=feed_store.FeedStatus.ACTIVE,
        last_heartbeat=_NOW,
        failure_count=0,
        retry_after=None,
        status_reason=None,
        status_reason_detail=None,
        status_reason_updated_at=None,
        audit_revision=1,
        membership_revision=1,
        updated_at=_NOW,
    )


def _member(
    grant: ingestion_lease_store.LeaseGrant,
    feed_id: uuid.UUID,
    *,
    group_id: str,
) -> ingestion_lease_store.LeaseMemberIdentity:
    return ingestion_lease_store._issue_member_identity(
        grant,
        feed_id=feed_id,
        source_type=feed_store.SourceType.BCFY_CALLS,
        source_feed_id=f"{grant.lease_key}-{group_id}",
        sid=grant.lease_key,
        group_id=group_id,
    )


def _boundary(
    grant: ingestion_lease_store.LeaseGrant,
    feed_id: uuid.UUID,
    *,
    group_id: str,
    offset_seconds: int = 0,
) -> feed_work_scheduler.BoundaryWork:
    return feed_work_scheduler.BoundaryWork(
        member=_member(grant, feed_id, group_id=group_id),
        target=_NOW + datetime.timedelta(seconds=offset_seconds),
    )


def _child_result(
    feed_id: uuid.UUID,
    disposition: ingestion_lease_store.ChildDisposition = (
        ingestion_lease_store.ChildDisposition.APPLIED
    ),
    *,
    cursor_effect: ingestion_lease_store.CursorEffect = (
        ingestion_lease_store.CursorEffect.ADVANCED
    ),
    lifecycle_effect: ingestion_lease_store.LifecycleEffect = (
        ingestion_lease_store.LifecycleEffect.NONE
    ),
) -> ingestion_lease_store.ChildMutationResult:
    return ingestion_lease_store.ChildMutationResult(
        feed_id=feed_id,
        disposition=disposition,
        cursor_effect=cursor_effect,
        lifecycle_effect=lifecycle_effect,
    )


def _batch_committed(
    *children: ingestion_lease_store.ChildMutationResult,
    lease_effect: ingestion_lease_store.LeaseLifecycleEffect = (
        ingestion_lease_store.LeaseLifecycleEffect.NONE
    ),
) -> ingestion_lease_store.BatchCommitted:
    snapshot = _snapshot()
    return ingestion_lease_store.BatchCommitted(
        lease_effect=ingestion_lease_store.LeaseLifecycleResult(
            effect=lease_effect,
            before_snapshot=snapshot,
            after_snapshot=snapshot,
        ),
        children=children,
    )


def _batch_with_changed_neutral_snapshot(
    *children: ingestion_lease_store.ChildMutationResult,
) -> ingestion_lease_store.BatchCommitted:
    snapshot = _snapshot()
    return ingestion_lease_store.BatchCommitted(
        lease_effect=ingestion_lease_store.LeaseLifecycleResult(
            effect=ingestion_lease_store.LeaseLifecycleEffect.NONE,
            before_snapshot=snapshot,
            after_snapshot=dataclasses.replace(snapshot, failure_count=1),
        ),
        children=children,
    )


def _store_with_result(
    result: object,
) -> ingestion_lease_store.IngestionLeaseStore:
    store = mock.create_autospec(
        ingestion_lease_store.IngestionLeaseStore,
        instance=True,
    )
    store.commit_child_mutations = mock.AsyncMock(return_value=result)
    return typing.cast("ingestion_lease_store.IngestionLeaseStore", store)


class _NoStartStore:
    """Fake whose ordinary method proves failure before an awaitable exists."""

    def __init__(self) -> None:
        self.calls = 0

    def commit_child_mutations(self, *_args: object, **_kwargs: object) -> None:
        self.calls += 1
        raise runtime_adapters.BoundaryCommitNotStarted


class _NonAwaitableStore:
    """Malformed fake returning without creating an async attempt."""

    def __init__(self) -> None:
        self.calls = 0

    def commit_child_mutations(
        self, *_args: object, **_kwargs: object
    ) -> object:
        self.calls += 1
        return object()


class _AcknowledgementLost(RuntimeError):
    """Test-only signal for a result lost after a commit may have happened."""


class TestFencedBoundaryCommitter(unittest.IsolatedAsyncioTestCase):
    """Prove boundaries make one lifecycle-neutral fenced storage attempt."""

    async def test_boundary_committer_nonempty_is_neutral_and_ordered(
        self,
    ) -> None:
        grant = _grant()
        boundaries = tuple(
            _boundary(
                grant,
                uuid.UUID(int=index + 1),
                group_id=str(index + 1),
                offset_seconds=index,
            )
            for index in range(3)
        )
        dispositions = (
            ingestion_lease_store.ChildDisposition.APPLIED,
            ingestion_lease_store.ChildDisposition.APPLIED_AFTER_DEACTIVATION,
            ingestion_lease_store.ChildDisposition.ACCEPTED_NOOP,
        )

        for final_logical in (False, True):
            with self.subTest(final_logical=final_logical):
                store = _store_with_result(
                    _batch_committed(
                        *(
                            _child_result(boundary.feed_id, disposition)
                            for boundary, disposition in zip(
                                boundaries,
                                dispositions,
                                strict=True,
                            )
                        )
                    )
                )
                committer = runtime_adapters.FencedBoundaryCommitter(
                    store,
                    actor_id=_ACTOR_ID,
                )

                result = await committer.commit(
                    grant,
                    boundaries,
                    final_logical=final_logical,
                )

                self.assertEqual(
                    result,
                    feed_work_scheduler.BoundaryBatchCommitted(
                        tuple(
                            feed_work_scheduler.BoundaryResult(
                                boundary,
                                feed_work_scheduler.BoundaryDisposition.COMMITTED,
                            )
                            for boundary in boundaries
                        )
                    ),
                )
                store.commit_child_mutations.assert_awaited_once()
                call = store.commit_child_mutations.await_args
                self.assertEqual(call.args[0], grant)
                self.assertEqual(call.kwargs, {"actor_id": _ACTOR_ID})
                batch = call.args[1]
                self.assertIs(
                    type(batch.lease_effect),
                    ingestion_lease_store.NoLeaseEffect,
                )
                self.assertEqual(
                    tuple(type(mutation) for mutation in batch.mutations),
                    (ingestion_lease_store.ClosedCohortProgress,) * 3,
                )
                self.assertEqual(
                    tuple(mutation.member for mutation in batch.mutations),
                    tuple(boundary.member for boundary in boundaries),
                )
                self.assertEqual(
                    tuple(
                        mutation.last_processed_filename
                        for mutation in batch.mutations
                    ),
                    (None,) * 3,
                )
                self.assertEqual(
                    tuple(mutation.cursor for mutation in batch.mutations),
                    tuple(boundary.target for boundary in boundaries),
                )
                store.load_membership.assert_not_called()
                store.refresh_membership.assert_not_called()

    async def test_boundary_committer_empty_is_still_one_neutral_attempt(
        self,
    ) -> None:
        for final_logical in (False, True):
            with self.subTest(final_logical=final_logical):
                store = _store_with_result(_batch_committed())
                committer = runtime_adapters.FencedBoundaryCommitter(
                    store,
                    actor_id=_ACTOR_ID,
                )

                result = await committer.commit(
                    _grant(),
                    (),
                    final_logical=final_logical,
                )

                self.assertEqual(
                    result,
                    feed_work_scheduler.BoundaryBatchCommitted(()),
                )
                store.commit_child_mutations.assert_awaited_once()
                batch = store.commit_child_mutations.await_args.args[1]
                self.assertEqual(batch.mutations, ())
                self.assertIs(
                    type(batch.lease_effect),
                    ingestion_lease_store.NoLeaseEffect,
                )

    async def test_boundary_committer_stale_grant_is_global_rejection(
        self,
    ) -> None:
        store = _store_with_result(
            ingestion_lease_store.GrantRejected(
                ingestion_lease_store.GrantRejectionReason.MISSING,
                None,
            )
        )
        committer = runtime_adapters.FencedBoundaryCommitter(
            store,
            actor_id=_ACTOR_ID,
        )

        result = await committer.commit(_grant(), (), final_logical=True)

        self.assertEqual(result, feed_work_scheduler.BoundaryGrantRejected())
        store.commit_child_mutations.assert_awaited_once()

    async def test_boundary_committer_member_rejection_is_local_and_ordered(
        self,
    ) -> None:
        grant = _grant()
        boundaries = (
            _boundary(grant, uuid.UUID(int=1), group_id="1"),
            _boundary(grant, uuid.UUID(int=2), group_id="2"),
        )
        store = _store_with_result(
            _batch_committed(
                _child_result(
                    boundaries[0].feed_id,
                    ingestion_lease_store.ChildDisposition.MISSING,
                ),
                _child_result(
                    boundaries[1].feed_id,
                    ingestion_lease_store.ChildDisposition.STATUS_INELIGIBLE,
                ),
            )
        )
        committer = runtime_adapters.FencedBoundaryCommitter(
            store,
            actor_id=_ACTOR_ID,
        )

        result = await committer.commit(
            grant,
            boundaries,
            final_logical=False,
        )

        assert isinstance(
            result,
            feed_work_scheduler.BoundaryBatchCommitted,
        )
        self.assertEqual(
            tuple(item.boundary for item in result.results),
            boundaries,
        )
        self.assertEqual(
            tuple(item.disposition for item in result.results),
            (feed_work_scheduler.BoundaryDisposition.MEMBER_REJECTED,) * 2,
        )

    async def test_boundary_committer_definitive_no_start_is_retryable(
        self,
    ) -> None:
        store = _NoStartStore()
        committer = runtime_adapters.FencedBoundaryCommitter(
            typing.cast("ingestion_lease_store.IngestionLeaseStore", store),
            actor_id=_ACTOR_ID,
        )

        result = await committer.commit(_grant(), (), final_logical=True)

        self.assertEqual(
            result,
            feed_work_scheduler.BoundaryBatchRetryable(),
        )
        self.assertEqual(store.calls, 1)

    async def test_boundary_committer_outcome_unknown_transport_propagates(
        self,
    ) -> None:
        failures = (
            asyncpg.PostgresConnectionError("connection lost"),
            asyncpg.InterfaceError("connection unavailable"),
            OSError("socket failed"),
        )

        for failure in failures:
            with self.subTest(failure=type(failure).__name__):
                store = _store_with_result(object())
                store.commit_child_mutations.side_effect = failure
                committer = runtime_adapters.FencedBoundaryCommitter(
                    store,
                    actor_id=_ACTOR_ID,
                )

                with self.assertRaises(type(failure)) as raised:
                    await committer.commit(
                        _grant(),
                        (),
                        final_logical=True,
                    )

                self.assertIs(raised.exception, failure)
                store.commit_child_mutations.assert_awaited_once()

    async def test_boundary_committer_outcome_unknown_control_propagates(
        self,
    ) -> None:
        failures = (
            asyncio.CancelledError(),
            _AcknowledgementLost("commit acknowledgement lost"),
            runtime_adapters.BoundaryCommitNotStarted(
                "raised only after await began"
            ),
        )

        for failure in failures:
            with self.subTest(failure=type(failure).__name__):
                store = _store_with_result(object())
                store.commit_child_mutations.side_effect = failure
                committer = runtime_adapters.FencedBoundaryCommitter(
                    store,
                    actor_id=_ACTOR_ID,
                )

                with self.assertRaises(type(failure)) as raised:
                    await committer.commit(
                        _grant(),
                        (),
                        final_logical=True,
                    )

                self.assertIs(raised.exception, failure)
                store.commit_child_mutations.assert_awaited_once()

    async def test_boundary_committer_malformed_results_are_outcome_unknown(
        self,
    ) -> None:
        grant = _grant()
        boundaries = (
            _boundary(grant, uuid.UUID(int=1), group_id="1"),
            _boundary(grant, uuid.UUID(int=2), group_id="2"),
        )
        malformed = (
            object(),
            dataclasses.replace(
                _batch_committed(
                    _child_result(boundaries[0].feed_id),
                    _child_result(boundaries[1].feed_id),
                ),
                children=[  # type: ignore[arg-type]
                    _child_result(boundaries[0].feed_id),
                    _child_result(boundaries[1].feed_id),
                ],
            ),
            _batch_committed(_child_result(boundaries[0].feed_id)),
            _batch_committed(
                _child_result(boundaries[1].feed_id),
                _child_result(boundaries[0].feed_id),
            ),
            _batch_committed(
                ingestion_lease_store.ChildMutationResult(
                    feed_id=boundaries[0].feed_id,
                    disposition=object(),  # type: ignore[arg-type]
                    cursor_effect=ingestion_lease_store.CursorEffect.ADVANCED,
                    lifecycle_effect=ingestion_lease_store.LifecycleEffect.NONE,
                ),
                _child_result(boundaries[1].feed_id),
            ),
            _batch_committed(
                ingestion_lease_store.ChildMutationResult(
                    feed_id=boundaries[0].feed_id,
                    disposition=ingestion_lease_store.ChildDisposition.APPLIED,
                    cursor_effect=object(),  # type: ignore[arg-type]
                    lifecycle_effect=ingestion_lease_store.LifecycleEffect.NONE,
                ),
                _child_result(boundaries[1].feed_id),
            ),
            _batch_committed(
                _child_result(
                    boundaries[0].feed_id,
                    lifecycle_effect=(
                        ingestion_lease_store.LifecycleEffect.RECOVERED
                    ),
                ),
                _child_result(boundaries[1].feed_id),
            ),
            _batch_committed(
                _child_result(boundaries[0].feed_id),
                _child_result(boundaries[1].feed_id),
                lease_effect=(
                    ingestion_lease_store.LeaseLifecycleEffect.RECOVERED
                ),
            ),
            _batch_with_changed_neutral_snapshot(
                _child_result(boundaries[0].feed_id),
                _child_result(boundaries[1].feed_id),
            ),
        )

        for result in malformed:
            with self.subTest(result=result):
                store = _store_with_result(result)
                committer = runtime_adapters.FencedBoundaryCommitter(
                    store,
                    actor_id=_ACTOR_ID,
                )

                with self.assertRaises(
                    runtime_adapters.BoundaryAdapterIntegrityError
                ):
                    await committer.commit(
                        grant,
                        boundaries,
                        final_logical=False,
                    )

                store.commit_child_mutations.assert_awaited_once()

        store = _NonAwaitableStore()
        committer = runtime_adapters.FencedBoundaryCommitter(
            typing.cast("ingestion_lease_store.IngestionLeaseStore", store),
            actor_id=_ACTOR_ID,
        )
        with self.assertRaises(runtime_adapters.BoundaryAdapterIntegrityError):
            await committer.commit(grant, boundaries, final_logical=False)
        self.assertEqual(store.calls, 1)

    async def test_boundary_committer_inputs_fail_before_mutation(self) -> None:
        store = _store_with_result(_batch_committed())
        committer = runtime_adapters.FencedBoundaryCommitter(
            store,
            actor_id=_ACTOR_ID,
        )

        with self.assertRaises(TypeError):
            await committer.commit(
                _grant(),
                [],  # type: ignore[arg-type]
                final_logical=True,
            )
        with self.assertRaises(TypeError):
            await committer.commit(
                _grant(),
                (object(),),  # type: ignore[arg-type]
                final_logical=True,
            )
        with self.assertRaises(TypeError):
            await committer.commit(
                _grant(),
                (),
                final_logical=1,  # type: ignore[arg-type]
            )

        store.commit_child_mutations.assert_not_awaited()


class TestPhysicalCohortCommitter(unittest.IsolatedAsyncioTestCase):
    """Prove direct cohorts preserve their exact optional durability fields."""

    async def test_physical_cohort_path_cursor_and_both_are_exact_neutral(
        self,
    ) -> None:
        grant = _grant()
        member = _member(grant, uuid.UUID(int=90), group_id="90")
        cases = (
            ("gs://bucket/path-only.ogg", None),
            (None, _NOW),
            ("gs://bucket/both.ogg", _NOW),
        )

        for path, cursor in cases:
            for final_logical in (False, True):
                with self.subTest(
                    path=path,
                    cursor=cursor,
                    final_logical=final_logical,
                ):
                    commit = runtime_adapters.PhysicalCohortCommit(
                        member,
                        path,
                        cursor,
                    )
                    store = _store_with_result(
                        _batch_committed(
                            _child_result(
                                member.feed_id,
                                cursor_effect=(
                                    ingestion_lease_store.CursorEffect.ABSENT
                                    if cursor is None
                                    else ingestion_lease_store.CursorEffect.ADVANCED
                                ),
                            )
                        )
                    )
                    committer = runtime_adapters.FencedBoundaryCommitter(
                        store,
                        actor_id=_ACTOR_ID,
                    )

                    result = await committer.commit_cohort(
                        grant,
                        commit,
                        final_logical=final_logical,
                    )

                    self.assertEqual(
                        result,
                        runtime_adapters.PhysicalCohortResult(
                            commit,
                            feed_work_scheduler.BoundaryDisposition.COMMITTED,
                        ),
                    )
                    store.commit_child_mutations.assert_awaited_once()
                    call = store.commit_child_mutations.await_args
                    self.assertEqual(call.args[0], grant)
                    self.assertEqual(call.kwargs, {"actor_id": _ACTOR_ID})
                    batch = call.args[1]
                    self.assertIs(
                        type(batch.lease_effect),
                        ingestion_lease_store.NoLeaseEffect,
                    )
                    self.assertEqual(
                        batch.mutations,
                        (
                            ingestion_lease_store.ClosedCohortProgress(
                                member,
                                path,
                                cursor,
                            ),
                        ),
                    )

    async def test_physical_cohort_member_rejection_stays_local(self) -> None:
        grant = _grant()
        member = _member(grant, uuid.UUID(int=91), group_id="91")
        commit = runtime_adapters.PhysicalCohortCommit(member, None, _NOW)
        store = _store_with_result(
            _batch_committed(
                _child_result(
                    member.feed_id,
                    ingestion_lease_store.ChildDisposition.MISSING,
                )
            )
        )
        committer = runtime_adapters.FencedBoundaryCommitter(
            store,
            actor_id=_ACTOR_ID,
        )

        result = await committer.commit_cohort(
            grant,
            commit,
            final_logical=False,
        )

        self.assertEqual(
            result,
            runtime_adapters.PhysicalCohortResult(
                commit,
                feed_work_scheduler.BoundaryDisposition.MEMBER_REJECTED,
            ),
        )

    async def test_physical_cohort_all_null_is_rejected_before_io(self) -> None:
        grant = _grant()
        member = _member(grant, uuid.UUID(int=92), group_id="92")
        store = _store_with_result(_batch_committed())
        runtime_adapters.FencedBoundaryCommitter(store, actor_id=_ACTOR_ID)

        with self.assertRaisesRegex(ValueError, "requires a path or cursor"):
            runtime_adapters.PhysicalCohortCommit(member, None, None)

        store.commit_child_mutations.assert_not_awaited()

    def test_physical_cohort_value_is_frozen(self) -> None:
        grant = _grant()
        member = _member(grant, uuid.UUID(int=93), group_id="93")
        commit = runtime_adapters.PhysicalCohortCommit(member, None, _NOW)

        with self.assertRaises(dataclasses.FrozenInstanceError):
            commit.cursor = None  # type: ignore[misc]

        with self.assertRaises(TypeError):
            runtime_adapters.PhysicalCohortResult(
                commit,
                "committed",  # type: ignore[arg-type]
            )
        with self.assertRaises(ValueError):
            runtime_adapters.PhysicalCohortResult(
                commit,
                feed_work_scheduler.BoundaryDisposition.RETRYABLE,
            )


if __name__ == "__main__":
    unittest.main()
