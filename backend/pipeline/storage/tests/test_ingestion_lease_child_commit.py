"""Unit tests for the private ingestion Lease child-commit capability."""

from __future__ import annotations

import datetime
import unittest
import uuid
from unittest import mock

from backend.pipeline.storage import (
    _ingestion_lease_child_commit as child_commit,
)
from backend.pipeline.storage import (
    feed_store,
)
from backend.pipeline.storage import (
    ingestion_lease_contracts as contracts,
)

_NOW = datetime.datetime(2026, 7, 10, 12, 0, tzinfo=datetime.UTC)
_OWNER_ID = uuid.UUID("11111111-2222-3333-4444-555555555555")


def _grant() -> contracts.LeaseGrant:
    return contracts.LeaseGrant(
        source_type=feed_store.SourceType.BCFY_CALLS,
        lease_key="123",
        owner_worker_id=_OWNER_ID,
        fencing_token=7,
    )


def _member(feed_id: uuid.UUID) -> contracts.LeaseMemberIdentity:
    return contracts.LeaseMemberIdentity(
        feed_id=feed_id,
        source_type=feed_store.SourceType.BCFY_CALLS,
        source_feed_id="123-45",
    )


def _child_row(
    feed_id: uuid.UUID,
    **overrides: object,
) -> dict[str, object]:
    row: dict[str, object] = {
        "id": feed_id,
        "name": f"Feed {feed_id}",
        "source_type": "bcfy_calls",
        "status": "active",
        "last_processed_filename": None,
        "last_bookmark_time": None,
        "failure_count": 0,
        "retry_after": None,
        "status_reason": None,
        "status_reason_detail": None,
        "status_reason_updated_at": None,
        "audit_revision": 0,
        "created_at": _NOW,
    }
    row.update(overrides)
    return row


def _progress(
    feed_id: uuid.UUID,
    *,
    cursor: datetime.datetime | None = _NOW,
    path: str = "gs://bucket/audio.flac",
) -> contracts.AdmittedAudioProgress:
    return contracts.AdmittedAudioProgress(_member(feed_id), path, cursor)


def _closed_cohort(
    feed_id: uuid.UUID,
    *,
    cursor: datetime.datetime | None = _NOW,
    path: str | None = "gs://bucket/cohort.flac",
) -> contracts.ClosedCohortProgress:
    return contracts.ClosedCohortProgress(_member(feed_id), path, cursor)


def _failure(
    feed_id: uuid.UUID,
    *,
    cursor: datetime.datetime | None = _NOW,
    reason: str | None = "boundary failed",
) -> contracts.FeedFailureTransition:
    return contracts.FeedFailureTransition(
        member=_member(feed_id),
        action=contracts.BudgetedFailure(),
        status_reason=(
            feed_store.FeedStatusReason.SYSTEM_CONFIGURATION_INVALID
        ),
        reason=reason,
        completion_cursor=cursor,
    )


def _plan(
    mutation: contracts.ChildMutation,
    before_row: dict[str, object] | None,
) -> child_commit._PlannedChildMutation:
    prepared = child_commit.prepare_child_commit(
        _grant(),
        contracts.ChildMutationBatch(
            mutations=(mutation,),
            lease_effect=contracts.NoLeaseEffect(),
        ),
        "service_account:gcp:collector",
    )
    before_state = (
        None
        if before_row is None
        else child_commit._locked_child_state_from_row(before_row)
    )
    return child_commit._plan_prepared_child_mutation(
        prepared.mutations[0],
        before_state,
    )


class TestChildPlanning(unittest.TestCase):
    """Tests for cursor, path, lifecycle, and failure planning."""

    def test_cursor_write_and_lifecycle_recovery_are_independent(self) -> None:
        feed_id = uuid.UUID("77777777-0000-0000-0000-000000000070")
        earlier = _NOW - datetime.timedelta(seconds=1)
        later = _NOW + datetime.timedelta(seconds=1)

        for case_index, (current, requested, write_cursor) in enumerate(
            (
                (None, _NOW, True),
                (_NOW, later, True),
                (_NOW, _NOW, False),
                (_NOW, earlier, False),
                (_NOW, None, False),
            )
        ):
            with self.subTest(case_index=case_index):
                clean = _plan(
                    contracts.SourceObservation(_member(feed_id), requested),
                    _child_row(feed_id, last_bookmark_time=current),
                )
                dirty = _plan(
                    contracts.SourceObservation(_member(feed_id), requested),
                    _child_row(
                        feed_id,
                        status="failing",
                        failure_count=2,
                        status_reason="source_unreachable",
                        last_bookmark_time=current,
                    ),
                )

                self.assertIs(clean.write_cursor, write_cursor)
                self.assertFalse(clean.clear_lifecycle)
                self.assertIs(
                    clean.disposition, contracts.ChildDisposition.COMMITTED
                )
                self.assertIs(dirty.write_cursor, write_cursor)
                self.assertTrue(dirty.clear_lifecycle)
                self.assertTrue(dirty.needs_update)

    def test_clean_deactivated_cursor_noops_remain_committed(self) -> None:
        feed_id = uuid.UUID("77777777-0000-0000-0000-000000000071")
        earlier = _NOW - datetime.timedelta(seconds=1)

        for case_index, requested in enumerate((_NOW, earlier, None)):
            with self.subTest(case_index=case_index):
                plan = _plan(
                    _progress(feed_id, cursor=requested),
                    _child_row(
                        feed_id,
                        status="deactivated",
                        last_bookmark_time=_NOW,
                        last_processed_filename="gs://bucket/audio.flac",
                    ),
                )

                self.assertFalse(plan.write_cursor)
                self.assertFalse(plan.needs_update)
                self.assertIs(
                    plan.disposition, contracts.ChildDisposition.COMMITTED
                )

    def test_closed_cohort_plans_path_and_cursor_independently(self) -> None:
        feed_id = uuid.UUID("77777777-0000-0000-0000-000000000142")
        earlier = _NOW - datetime.timedelta(seconds=1)
        later = _NOW + datetime.timedelta(seconds=1)
        path = "gs://bucket/cohort.flac"
        cases = (
            (
                _closed_cohort(feed_id, cursor=later, path=path),
                _child_row(feed_id, last_bookmark_time=_NOW),
                True,
                True,
            ),
            (
                _closed_cohort(feed_id, cursor=later, path=None),
                _child_row(feed_id, last_bookmark_time=_NOW),
                True,
                False,
            ),
            (
                _closed_cohort(feed_id, cursor=None, path=path),
                _child_row(feed_id, last_processed_filename="old"),
                False,
                True,
            ),
            (
                _closed_cohort(feed_id, cursor=_NOW, path=path),
                _child_row(feed_id, last_bookmark_time=_NOW),
                False,
                True,
            ),
            (
                _closed_cohort(feed_id, cursor=earlier, path=path),
                _child_row(feed_id, last_bookmark_time=_NOW),
                False,
                True,
            ),
            (
                _closed_cohort(feed_id, cursor=_NOW, path=path),
                _child_row(
                    feed_id,
                    last_bookmark_time=_NOW,
                    last_processed_filename=path,
                ),
                False,
                False,
            ),
        )

        for mutation, row, write_cursor, write_path in cases:
            with self.subTest(write_cursor=write_cursor, write_path=write_path):
                plan = _plan(mutation, row)

                self.assertEqual(plan.write_cursor, write_cursor)
                self.assertEqual(plan.write_path, write_path)
                self.assertFalse(plan.clear_lifecycle)
                self.assertIs(
                    plan.disposition, contracts.ChildDisposition.COMMITTED
                )

    def test_closed_cohort_preserves_dirty_lifecycle_for_eligible_statuses(
        self,
    ) -> None:
        for index, status in enumerate(
            (
                feed_store.FeedStatus.ACTIVE,
                feed_store.FeedStatus.FAILING,
                feed_store.FeedStatus.DEACTIVATED,
            )
        ):
            with self.subTest(status=status.value):
                feed_id = uuid.UUID(int=150 + index)
                plan = _plan(
                    _closed_cohort(feed_id, cursor=None),
                    _child_row(
                        feed_id,
                        status=status.value,
                        failure_count=4,
                        retry_after=_NOW,
                        status_reason="source_unreachable",
                        status_reason_detail="still dirty",
                        audit_revision=9,
                    ),
                )

                self.assertIs(
                    plan.disposition, contracts.ChildDisposition.COMMITTED
                )
                self.assertFalse(plan.clear_lifecycle)

    def test_failure_charge_is_independent_from_cursor_write(self) -> None:
        feed_id = uuid.UUID("99999999-0000-0000-0000-000000000089")
        earlier = _NOW - datetime.timedelta(seconds=1)
        later = _NOW + datetime.timedelta(seconds=1)

        for case_index, (current, requested, write_cursor) in enumerate(
            (
                (None, None, False),
                (_NOW, _NOW, False),
                (_NOW, earlier, False),
                (None, _NOW, True),
                (_NOW, later, True),
            )
        ):
            with self.subTest(case_index=case_index):
                plan = _plan(
                    _failure(feed_id, cursor=requested),
                    _child_row(feed_id, last_bookmark_time=current),
                )

                self.assertEqual(plan.write_cursor, write_cursor)
                self.assertTrue(plan.needs_update)
                self.assertIs(
                    plan.disposition, contracts.ChildDisposition.COMMITTED
                )


class TestChildBoundaries(unittest.IsolatedAsyncioTestCase):
    """Tests for prepared values and distinct SQL row projections."""

    async def test_failure_detail_is_normalized_once_before_checkout(
        self,
    ) -> None:
        feed_id = uuid.UUID("99999999-0000-0000-0000-000000000091")
        connection = mock.AsyncMock()
        connection.fetch.side_effect = [
            [_child_row(feed_id)],
            [
                _child_row(
                    feed_id,
                    status="failing",
                    failure_count=1,
                    status_reason="system_configuration_invalid",
                    status_reason_detail="normalized",
                    audit_revision=1,
                )
            ],
        ]
        batch = contracts.ChildMutationBatch(
            (_failure(feed_id, reason="raw detail"),),
            contracts.NoLeaseEffect(),
        )

        with mock.patch.object(
            contracts,
            "_status_reason_detail_storage_value",
            return_value="normalized",
        ) as normalize:
            prepared = child_commit.prepare_child_commit(
                _grant(),
                batch,
                "service_account:gcp:collector",
            )
            await child_commit.apply_child_mutations(connection, prepared)

        normalize.assert_called_once_with("raw detail")
        self.assertEqual(
            connection.fetch.await_args_list[1].args[10],
            ["normalized"],
        )

    def test_audited_return_projection_does_not_require_progress_fields(
        self,
    ) -> None:
        feed_id = uuid.UUID("99999999-0000-0000-0000-000000000092")
        row = _child_row(feed_id)
        del row["last_processed_filename"]
        del row["last_bookmark_time"]

        state = child_commit._audited_child_state_from_row(row)

        self.assertEqual(state.feed_id, feed_id)
        self.assertEqual(state.audit_revision, 0)

    def test_neutral_return_projection_requires_only_feed_id(self) -> None:
        feed_id = uuid.UUID("99999999-0000-0000-0000-000000000093")

        state = child_commit._neutral_child_state_from_row({"id": feed_id})

        self.assertEqual(state.feed_id, feed_id)
