"""Focused orchestration tests for the fenced ingestion Lease store."""

from __future__ import annotations

import datetime
import unittest
import uuid
from unittest import mock

from backend.pipeline.storage import (
    feed_store,
    ingestion_lease_queries,
    ingestion_lease_store,
)
from backend.pipeline.storage.tests import connection_util

_OWNER_ID = uuid.UUID("11111111-2222-3333-4444-555555555555")
_OTHER_OWNER_ID = uuid.UUID("22222222-3333-4444-5555-666666666666")
_NOW = datetime.datetime(2026, 7, 10, 12, 0, tzinfo=datetime.UTC)


def _grant(
    lease_key: str = "123",
    *,
    owner_id: uuid.UUID = _OWNER_ID,
    fencing_token: int = 7,
) -> ingestion_lease_store.LeaseGrant:
    return ingestion_lease_store.LeaseGrant(
        feed_store.SourceType.BCFY_CALLS,
        lease_key,
        owner_id,
        fencing_token,
    )


def _lease_row(**overrides: object) -> dict[str, object]:
    row: dict[str, object] = {
        "source_type": "bcfy_calls",
        "lease_key": "123",
        "status": "active",
        "worker_id": _OWNER_ID,
        "fencing_token": 7,
        "last_heartbeat": _NOW,
        "failure_count": 2,
        "retry_after": None,
        "status_reason": "source_unreachable",
        "status_reason_detail": "provider timeout",
        "status_reason_updated_at": _NOW,
        "audit_revision": 3,
        "membership_revision": 4,
        "updated_at": _NOW,
        "applied": False,
    }
    row.update(overrides)
    return row


def _failure_result_row(
    *,
    after_status: str = "failing",
    after_failure_count: int = 3,
    after_retry_after: datetime.datetime | None = _NOW,
    **overrides: object,
) -> dict[str, object]:
    applied = overrides.pop("applied", True)
    row = _lease_row(applied=applied, **overrides)
    row.update(
        {
            "after_status": after_status,
            "after_last_heartbeat": None,
            "after_failure_count": after_failure_count,
            "after_retry_after": after_retry_after,
            "after_status_reason": "source_unreachable",
            "after_status_reason_detail": "provider timeout",
            "after_status_reason_updated_at": _NOW,
            "after_audit_revision": 4,
            "after_membership_revision": 4,
            "after_updated_at": _NOW,
        }
    )
    return row


def _member_row(**overrides: object) -> dict[str, object]:
    row: dict[str, object] = {
        "feed_id": uuid.UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"),
        "property_source_type": "bcfy_calls",
        "feed_source_type": "bcfy_calls",
        "source_feed_id": "00123-00045",
        "sid": "00123",
        "group_id": "00045",
        "status": "active",
        "last_processed_filename": "gs://bucket/last.ogg",
        "last_bookmark_time": _NOW,
        "failure_count": 0,
        "retry_after": None,
        "status_reason": None,
        "status_reason_detail": None,
        "audit_revision": 2,
    }
    row.update(overrides)
    return row


class TestIngestionLeaseStoreValidation(unittest.IsolatedAsyncioTestCase):
    """Tests that malformed control input fails before pool checkout."""

    async def test_claim_rejects_invalid_source_owner_and_limit(self) -> None:
        pool = connection_util.make_mock_pool()
        store = ingestion_lease_store.IngestionLeaseStore(pool)

        invalid_calls = (
            store.claim_unclaimed("bcfy_calls", _OWNER_ID, 1),
            store.claim_unclaimed(
                feed_store.SourceType.BCFY_CALLS,
                "worker",
                1,
            ),
            store.claim_unclaimed(
                feed_store.SourceType.BCFY_CALLS,
                _OWNER_ID,
                -1,
            ),
            store.claim_unclaimed(
                feed_store.SourceType.BCFY_CALLS,
                _OWNER_ID,
                limit=True,
            ),
        )

        for call in invalid_calls:
            with self.assertRaises((TypeError, ValueError)):
                await call

        pool.fetch.assert_not_awaited()

    async def test_zero_claim_limit_returns_without_sql(self) -> None:
        pool = connection_util.make_mock_pool()
        store = ingestion_lease_store.IngestionLeaseStore(pool)

        result = await store.claim_unclaimed(
            feed_store.SourceType.BCFY_CALLS,
            _OWNER_ID,
            0,
        )

        self.assertEqual(result, ())
        pool.fetch.assert_not_awaited()

    async def test_recovery_rejects_nonpositive_abandonment(self) -> None:
        pool = connection_util.make_mock_pool()
        store = ingestion_lease_store.IngestionLeaseStore(pool)

        for case_index, abandonment_after in enumerate(
            (
                datetime.timedelta(0),
                datetime.timedelta(seconds=-1),
                "60 seconds",
            )
        ):
            with self.subTest(case_index=case_index):
                with self.assertRaises((TypeError, ValueError)):
                    await store.claim_recoverable(
                        feed_store.SourceType.BCFY_CALLS,
                        _OWNER_ID,
                        1,
                        abandonment_after,
                    )

        pool.fetch.assert_not_awaited()

    async def test_empty_heartbeat_input_returns_without_sql(self) -> None:
        pool = connection_util.make_mock_pool()
        store = ingestion_lease_store.IngestionLeaseStore(pool)

        result = await store.renew_heartbeats(())

        self.assertEqual(result, ())
        pool.fetch.assert_not_awaited()

    async def test_duplicate_heartbeat_identity_fails_before_sql(self) -> None:
        pool = connection_util.make_mock_pool()
        store = ingestion_lease_store.IngestionLeaseStore(pool)

        with self.assertRaisesRegex(ValueError, "duplicate Lease identity"):
            await store.renew_heartbeats((_grant(), _grant(fencing_token=8)))

        pool.fetch.assert_not_awaited()

    async def test_release_rejects_unknown_cause_before_sql(self) -> None:
        pool = connection_util.make_mock_pool()
        store = ingestion_lease_store.IngestionLeaseStore(pool)

        with self.assertRaises(TypeError):
            await store.release(_grant(), cause="shutdown")

        pool.fetchrow.assert_not_awaited()


class TestIngestionLeaseStoreClaims(unittest.IsolatedAsyncioTestCase):
    """Tests for strict typed claim conversion and one-shot execution."""

    async def test_primary_claim_returns_complete_grant_and_snapshot(
        self,
    ) -> None:
        pool = connection_util.make_mock_pool(
            fetch_result=[_lease_row(fencing_token=8, failure_count=0)]
        )
        store = ingestion_lease_store.IngestionLeaseStore(pool)

        result = await store.claim_unclaimed(
            feed_store.SourceType.BCFY_CALLS,
            _OWNER_ID,
            1,
        )

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].grant, _grant(fencing_token=8))
        self.assertEqual(result[0].snapshot.membership_revision, 4)
        pool.fetch.assert_awaited_once_with(
            ingestion_lease_queries.CLAIM_UNCLAIMED_LEASES_SQL,
            "bcfy_calls",
            _OWNER_ID,
            1,
        )

    async def test_same_worker_reclaim_returns_newer_generation(self) -> None:
        pool = connection_util.make_mock_pool(
            fetch_result=[_lease_row(fencing_token=8)]
        )
        store = ingestion_lease_store.IngestionLeaseStore(pool)
        old_grant = _grant(fencing_token=7)

        result = await store.claim_unclaimed(
            old_grant.source_type,
            old_grant.owner_worker_id,
            1,
        )

        self.assertGreater(
            result[0].grant.fencing_token, old_grant.fencing_token
        )
        self.assertEqual(
            result[0].grant.owner_worker_id,
            old_grant.owner_worker_id,
        )

    async def test_stale_active_recovery_returns_newer_generation(self) -> None:
        pool = connection_util.make_mock_pool(
            fetch_result=[_lease_row(fencing_token=9)]
        )
        store = ingestion_lease_store.IngestionLeaseStore(pool)
        old_grant = _grant(fencing_token=8)
        abandonment_after = datetime.timedelta(seconds=60)

        result = await store.claim_recoverable(
            old_grant.source_type,
            old_grant.owner_worker_id,
            1,
            abandonment_after,
        )

        self.assertGreater(
            result[0].grant.fencing_token, old_grant.fencing_token
        )
        pool.fetch.assert_awaited_once_with(
            ingestion_lease_queries.CLAIM_RECOVERABLE_LEASES_SQL,
            "bcfy_calls",
            _OWNER_ID,
            1,
            abandonment_after,
        )

    async def test_claim_conversion_rejects_unknown_database_values(
        self,
    ) -> None:
        cases = (
            {"source_type": "future_source"},
            {"status": "future_status"},
            {"status_reason": "future_reason"},
        )

        for overrides in cases:
            with self.subTest(overrides=overrides):
                pool = connection_util.make_mock_pool(
                    fetch_result=[_lease_row(**overrides)]
                )
                store = ingestion_lease_store.IngestionLeaseStore(pool)
                with self.assertRaises(ValueError):
                    await store.claim_unclaimed(
                        feed_store.SourceType.BCFY_CALLS,
                        _OWNER_ID,
                        1,
                    )

    async def test_database_exception_is_not_retried(self) -> None:
        pool = connection_util.make_mock_pool()
        pool.fetch.side_effect = RuntimeError("database unavailable")
        store = ingestion_lease_store.IngestionLeaseStore(pool)

        with self.assertRaisesRegex(RuntimeError, "database unavailable"):
            await store.claim_unclaimed(
                feed_store.SourceType.BCFY_CALLS,
                _OWNER_ID,
                1,
            )

        pool.fetch.assert_awaited_once()


class TestIngestionLeaseStoreHeartbeat(unittest.IsolatedAsyncioTestCase):
    """Tests for exact-grant heartbeat diagnostics."""

    async def test_results_follow_caller_order_after_database_scramble(
        self,
    ) -> None:
        first = _grant("200")
        second = _grant("100", fencing_token=8)
        rows = [
            _lease_row(
                lease_key="100",
                fencing_token=8,
                caller_ordinal=1,
                applied=True,
            ),
            _lease_row(
                lease_key="200",
                caller_ordinal=0,
                applied=True,
            ),
        ]
        pool = connection_util.make_mock_pool(fetch_result=rows)
        store = ingestion_lease_store.IngestionLeaseStore(pool)

        result = await store.renew_heartbeats((first, second))

        self.assertEqual([item.grant for item in result], [first, second])
        self.assertTrue(
            all(
                item.disposition
                is ingestion_lease_store.LeaseOperationDisposition.APPLIED
                for item in result
            )
        )
        args = pool.fetch.await_args.args
        self.assertEqual(args[1], ["bcfy_calls", "bcfy_calls"])
        self.assertEqual(args[2], ["100", "200"])
        self.assertEqual(args[5], [1, 0])

    async def test_stale_owner_token_status_and_missing_are_typed(self) -> None:
        cases = (
            (
                _lease_row(worker_id=_OTHER_OWNER_ID, caller_ordinal=0),
                ingestion_lease_store.LeaseOperationDisposition.OWNER_MISMATCH,
            ),
            (
                _lease_row(fencing_token=8, caller_ordinal=0),
                ingestion_lease_store.LeaseOperationDisposition.FENCE_MISMATCH,
            ),
            (
                _lease_row(status="failing", caller_ordinal=0),
                ingestion_lease_store.LeaseOperationDisposition.STATUS_INELIGIBLE,
            ),
            (
                {
                    "source_type": "bcfy_calls",
                    "lease_key": "123",
                    "caller_ordinal": 0,
                    "applied": False,
                    "worker_id": None,
                    "fencing_token": None,
                    "status": None,
                },
                ingestion_lease_store.LeaseOperationDisposition.MISSING,
            ),
        )

        for row, expected in cases:
            with self.subTest(expected=expected.value):
                pool = connection_util.make_mock_pool(fetch_result=[row])
                store = ingestion_lease_store.IngestionLeaseStore(pool)

                result = await store.renew_heartbeats((_grant(),))

                self.assertIs(result[0].disposition, expected)
                if (
                    expected
                    is ingestion_lease_store.LeaseOperationDisposition.MISSING
                ):
                    self.assertIsNone(result[0].snapshot)
                else:
                    self.assertIsNotNone(result[0].snapshot)


class TestSharedGrantRejection(unittest.TestCase):
    """Tests for nullable/current-state rejection semantics."""

    def setUp(self) -> None:
        self.store = ingestion_lease_store.IngestionLeaseStore(mock.AsyncMock())
        self.grant = _grant()

    def test_only_missing_has_no_snapshot(self) -> None:
        result = self.store._grant_rejection(self.grant, None)

        self.assertIsNotNone(result)
        assert result is not None
        self.assertIs(
            result.reason,
            ingestion_lease_store.GrantRejectionReason.MISSING,
        )
        self.assertIsNone(result.snapshot)

    def test_owner_fence_and_status_rejections_preserve_snapshot(self) -> None:
        cases = (
            (
                _lease_row(worker_id=_OTHER_OWNER_ID),
                ingestion_lease_store.GrantRejectionReason.OWNER_MISMATCH,
            ),
            (
                _lease_row(fencing_token=8),
                ingestion_lease_store.GrantRejectionReason.FENCE_MISMATCH,
            ),
            (
                _lease_row(status="unclaimed"),
                ingestion_lease_store.GrantRejectionReason.STATUS_INELIGIBLE,
            ),
        )

        for row, reason in cases:
            with self.subTest(reason=reason.value):
                result = self.store._grant_rejection(self.grant, row)
                self.assertIsNotNone(result)
                assert result is not None
                self.assertIs(result.reason, reason)
                self.assertIsInstance(
                    result.snapshot,
                    ingestion_lease_store.LeaseSnapshot,
                )
                self.assertEqual(result.snapshot.failure_count, 2)

    def test_exact_active_grant_is_not_rejected(self) -> None:
        self.assertIsNone(self.store._grant_rejection(self.grant, _lease_row()))


class TestIngestionLeaseStoreRelease(unittest.IsolatedAsyncioTestCase):
    """Tests for one neutral exact-grant release policy."""

    async def test_all_closed_causes_execute_identical_sql(self) -> None:
        observed_args = []

        for cause in ingestion_lease_store.LeaseReleaseCause:
            pool = connection_util.make_mock_pool(
                fetchrow_result=_lease_row(
                    status="unclaimed",
                    worker_id=None,
                    last_heartbeat=None,
                    applied=True,
                )
            )
            store = ingestion_lease_store.IngestionLeaseStore(pool)

            result = await store.release(_grant(), cause=cause)

            self.assertIs(
                result.disposition,
                ingestion_lease_store.LeaseOperationDisposition.APPLIED,
            )
            self.assertIsNone(result.snapshot.last_heartbeat)
            observed_args.append(pool.fetchrow.await_args.args)

        self.assertTrue(all(args == observed_args[0] for args in observed_args))
        self.assertIs(
            observed_args[0][0],
            ingestion_lease_queries.RELEASE_LEASE_SQL,
        )

    async def test_stale_release_returns_current_diagnostic_state(self) -> None:
        pool = connection_util.make_mock_pool(
            fetchrow_result=_lease_row(
                status="failing",
                worker_id=None,
                last_heartbeat=None,
            )
        )
        store = ingestion_lease_store.IngestionLeaseStore(pool)

        result = await store.release(
            _grant(),
            cause=ingestion_lease_store.LeaseReleaseCause.SHUTDOWN,
        )

        self.assertIs(
            result.disposition,
            ingestion_lease_store.LeaseOperationDisposition.STATUS_INELIGIBLE,
        )
        self.assertEqual(result.snapshot.status, feed_store.FeedStatus.FAILING)
        self.assertEqual(result.snapshot.failure_count, 2)

    async def test_missing_release_is_typed(self) -> None:
        pool = connection_util.make_mock_pool(fetchrow_result=None)
        store = ingestion_lease_store.IngestionLeaseStore(pool)

        result = await store.release(_grant())

        self.assertIs(
            result.disposition,
            ingestion_lease_store.LeaseOperationDisposition.MISSING,
        )
        self.assertIsNone(result.snapshot)

    async def test_database_exception_is_not_retried(self) -> None:
        pool = connection_util.make_mock_pool()
        pool.fetchrow.side_effect = RuntimeError("release failed")
        store = ingestion_lease_store.IngestionLeaseStore(pool)

        with self.assertRaisesRegex(RuntimeError, "release failed"):
            await store.release(_grant())

        pool.fetchrow.assert_awaited_once()


class TestLeaseFailureActionValidation(unittest.IsolatedAsyncioTestCase):
    """Tests for pre-checkout closed failure policy validation."""

    def test_budgeted_action_rejects_invalid_parameters(self) -> None:
        cases = (
            {"failure_threshold": 0},
            {"failure_threshold": True},
            {"backoff_base_sec": 0},
            {"backoff_max_sec": 0},
            {"backoff_base_sec": 20, "backoff_max_sec": 10},
        )

        for case_index, kwargs in enumerate(cases):
            with self.subTest(case_index=case_index):
                with self.assertRaises((TypeError, ValueError)):
                    ingestion_lease_store.BudgetedFailure(**kwargs)

    def test_non_budgeted_action_requires_utc_aware_retry(self) -> None:
        cases = (
            datetime.datetime(2026, 7, 10),
            datetime.datetime(
                2026,
                7,
                10,
                tzinfo=datetime.timezone(datetime.timedelta(hours=1)),
            ),
            "tomorrow",
        )

        for case_index, retry_after in enumerate(cases):
            with self.subTest(case_index=case_index):
                with self.assertRaises((TypeError, ValueError)):
                    ingestion_lease_store.NonBudgetedFailure(retry_after)

    async def test_finalize_rejects_invalid_actor_and_reason_before_pool(
        self,
    ) -> None:
        pool = connection_util.make_mock_pool()
        store = ingestion_lease_store.IngestionLeaseStore(pool)
        action = ingestion_lease_store.BudgetedFailure()
        invalid_actors = ("", "has space", "x" * 513)

        for actor_id in invalid_actors:
            with self.assertRaises(ValueError):
                await store.finalize_failure(
                    _grant(),
                    action,
                    feed_store.FeedStatusReason.SOURCE_UNREACHABLE,
                    actor_id=actor_id,
                )

        with self.assertRaises(TypeError):
            await store.finalize_failure(
                _grant(),
                action,
                "source_unreachable",
                actor_id="service_account:gcp:collector",
            )

        pool.fetchrow.assert_not_awaited()


class TestFinalizeLeaseFailure(unittest.IsolatedAsyncioTestCase):
    """Tests for one-shot exact-grant finalized failures."""

    async def test_budgeted_failure_returns_before_and_after_effect(
        self,
    ) -> None:
        pool = connection_util.make_mock_pool(
            fetchrow_result=_failure_result_row()
        )
        store = ingestion_lease_store.IngestionLeaseStore(pool)

        result = await store.finalize_failure(
            _grant(),
            ingestion_lease_store.BudgetedFailure(),
            feed_store.FeedStatusReason.SOURCE_UNREACHABLE,
            actor_id="service_account:gcp:collector",
            reason="provider timeout",
        )

        self.assertIs(
            result.disposition,
            ingestion_lease_store.LeaseOperationDisposition.APPLIED,
        )
        self.assertIs(
            result.effect,
            ingestion_lease_store.LeaseFailureEffect.FAILURE_RECORDED,
        )
        self.assertEqual(result.before_snapshot.failure_count, 2)
        self.assertEqual(result.after_snapshot.failure_count, 3)
        self.assertEqual(
            result.after_snapshot.status,
            feed_store.FeedStatus.FAILING,
        )
        args = pool.fetchrow.await_args.args
        self.assertIs(
            args[0],
            ingestion_lease_queries.FINALIZE_BUDGETED_FAILURE_SQL,
        )
        self.assertEqual(args[5:8], (5, 600, 15))
        self.assertEqual(args[8], "source_unreachable")
        self.assertEqual(args[9], "provider timeout")

    async def test_budgeted_failure_reports_quarantine_at_threshold(
        self,
    ) -> None:
        pool = connection_util.make_mock_pool(
            fetchrow_result=_failure_result_row(
                after_status="quarantined",
                after_failure_count=5,
                after_retry_after=None,
                failure_count=4,
            )
        )
        store = ingestion_lease_store.IngestionLeaseStore(pool)

        result = await store.finalize_failure(
            _grant(),
            ingestion_lease_store.BudgetedFailure(),
            feed_store.FeedStatusReason.SYSTEM_AUTHENTICATION_FAILED,
            actor_id="service_account:gcp:collector",
        )

        self.assertIs(
            result.effect,
            ingestion_lease_store.LeaseFailureEffect.QUARANTINED,
        )
        self.assertIsNone(result.after_snapshot.retry_after)

    async def test_non_budgeted_failure_resets_count_and_uses_retry_time(
        self,
    ) -> None:
        retry_after = _NOW + datetime.timedelta(minutes=8)
        pool = connection_util.make_mock_pool(
            fetchrow_result=_failure_result_row(
                after_failure_count=0,
                after_retry_after=retry_after,
            )
        )
        store = ingestion_lease_store.IngestionLeaseStore(pool)

        result = await store.finalize_failure(
            _grant(),
            ingestion_lease_store.NonBudgetedFailure(retry_after),
            feed_store.FeedStatusReason.SYSTEM_PIPELINE_ERROR,
            actor_id="service_account:gcp:collector",
        )

        self.assertEqual(result.after_snapshot.failure_count, 0)
        self.assertEqual(result.after_snapshot.retry_after, retry_after)
        args = pool.fetchrow.await_args.args
        self.assertIs(
            args[0],
            ingestion_lease_queries.FINALIZE_NON_BUDGETED_FAILURE_SQL,
        )
        self.assertEqual(args[5], retry_after)

    async def test_failure_detail_is_bounded_before_database_call(self) -> None:
        pool = connection_util.make_mock_pool(
            fetchrow_result=_failure_result_row()
        )
        store = ingestion_lease_store.IngestionLeaseStore(pool)

        await store.finalize_failure(
            _grant(),
            ingestion_lease_store.BudgetedFailure(),
            feed_store.FeedStatusReason.SOURCE_UNREACHABLE,
            actor_id="service_account:gcp:collector",
            reason="x" * 3000,
        )

        detail = pool.fetchrow.await_args.args[-1]
        self.assertEqual(len(detail), 2048)
        self.assertTrue(detail.endswith("[truncated]"))

    async def test_present_rejection_preserves_same_before_after_state(
        self,
    ) -> None:
        row = _failure_result_row(
            applied=False,
            worker_id=_OTHER_OWNER_ID,
        )
        for field in (
            "status",
            "last_heartbeat",
            "failure_count",
            "retry_after",
            "status_reason",
            "status_reason_detail",
            "status_reason_updated_at",
            "audit_revision",
            "membership_revision",
            "updated_at",
        ):
            row[f"after_{field}"] = row[field]
        pool = connection_util.make_mock_pool(fetchrow_result=row)
        store = ingestion_lease_store.IngestionLeaseStore(pool)

        result = await store.finalize_failure(
            _grant(),
            ingestion_lease_store.BudgetedFailure(),
            feed_store.FeedStatusReason.SOURCE_UNREACHABLE,
            actor_id="service_account:gcp:collector",
        )

        self.assertIs(
            result.disposition,
            ingestion_lease_store.LeaseOperationDisposition.OWNER_MISMATCH,
        )
        self.assertIs(
            result.effect, ingestion_lease_store.LeaseFailureEffect.NONE
        )
        self.assertEqual(result.before_snapshot, result.after_snapshot)

    async def test_missing_failure_has_nullable_before_and_after(self) -> None:
        pool = connection_util.make_mock_pool(fetchrow_result=None)
        store = ingestion_lease_store.IngestionLeaseStore(pool)

        result = await store.finalize_failure(
            _grant(),
            ingestion_lease_store.BudgetedFailure(),
            feed_store.FeedStatusReason.SOURCE_UNREACHABLE,
            actor_id="service_account:gcp:collector",
        )

        self.assertIs(
            result.disposition,
            ingestion_lease_store.LeaseOperationDisposition.MISSING,
        )
        self.assertIs(
            result.effect, ingestion_lease_store.LeaseFailureEffect.NONE
        )
        self.assertIsNone(result.before_snapshot)
        self.assertIsNone(result.after_snapshot)

    async def test_database_exception_is_never_retried(self) -> None:
        pool = connection_util.make_mock_pool()
        pool.fetchrow.side_effect = RuntimeError("outcome unknown")
        store = ingestion_lease_store.IngestionLeaseStore(pool)

        with self.assertRaisesRegex(RuntimeError, "outcome unknown"):
            await store.finalize_failure(
                _grant(),
                ingestion_lease_store.BudgetedFailure(),
                feed_store.FeedStatusReason.SOURCE_UNREACHABLE,
                actor_id="service_account:gcp:collector",
            )

        pool.fetchrow.assert_awaited_once()

    async def test_finalized_failure_makes_stale_release_ineligible(
        self,
    ) -> None:
        pool = connection_util.make_mock_pool()
        pool.fetchrow.side_effect = [
            _failure_result_row(),
            _lease_row(
                status="failing",
                worker_id=None,
                last_heartbeat=None,
                applied=False,
            ),
        ]
        store = ingestion_lease_store.IngestionLeaseStore(pool)

        failure = await store.finalize_failure(
            _grant(),
            ingestion_lease_store.BudgetedFailure(),
            feed_store.FeedStatusReason.SOURCE_UNREACHABLE,
            actor_id="service_account:gcp:collector",
        )
        release = await store.release(
            _grant(),
            cause=ingestion_lease_store.LeaseReleaseCause.SHUTDOWN,
        )

        self.assertIs(
            failure.disposition,
            ingestion_lease_store.LeaseOperationDisposition.APPLIED,
        )
        self.assertIs(
            release.disposition,
            ingestion_lease_store.LeaseOperationDisposition.STATUS_INELIGIBLE,
        )
        self.assertEqual(pool.fetchrow.await_count, 2)


class TestLoadMembership(unittest.IsolatedAsyncioTestCase):
    """Tests for fail-closed authoritative membership snapshots."""

    async def test_rejects_unsupported_source_before_checkout(self) -> None:
        pool = connection_util.make_mock_pool(transaction=True)
        store = ingestion_lease_store.IngestionLeaseStore(pool)
        grant = ingestion_lease_store.LeaseGrant(
            feed_store.SourceType.OPENMHZ,
            "123",
            _OWNER_ID,
            7,
        )

        with self.assertRaises(ValueError):
            await store.load_membership(grant)

        pool.acquire.assert_not_called()

    async def test_loads_members_after_exact_grant_lock_in_transaction(
        self,
    ) -> None:
        pool = connection_util.make_mock_pool(transaction=True)
        connection = pool.acquired_connection
        connection.fetchrow.return_value = _lease_row(lease_key="00123")
        connection.fetch.return_value = [
            _member_row(),
            _member_row(
                feed_id=uuid.UUID("bbbbbbbb-cccc-dddd-eeee-ffffffffffff"),
                source_feed_id="00123-00046",
                group_id="00046",
                status="failing",
            ),
            _member_row(
                feed_id=uuid.UUID("cccccccc-dddd-eeee-ffff-000000000000"),
                source_feed_id="00123-00047",
                group_id="00047",
                status="deactivated",
            ),
        ]
        store = ingestion_lease_store.IngestionLeaseStore(pool)

        result = await store.load_membership(_grant("00123"))

        self.assertIsInstance(
            result,
            ingestion_lease_store.MembershipSnapshot,
        )
        assert isinstance(result, ingestion_lease_store.MembershipSnapshot)
        self.assertEqual(result.membership_revision, 4)
        self.assertEqual(len(result.members), 2)
        self.assertEqual(result.excluded_count, 1)
        self.assertEqual(result.members[0].identity.sid, "00123")
        self.assertEqual(result.members[0].identity.group_id, "00045")
        self.assertEqual(
            result.members[0].identity.source_feed_id, "00123-00045"
        )
        connection.transaction.assert_called_once_with(
            isolation="read_committed"
        )
        self.assertIs(
            connection.fetchrow.await_args.args[0],
            ingestion_lease_queries.LOCK_LEASE_SQL,
        )
        self.assertIs(
            connection.fetch.await_args.args[0],
            ingestion_lease_queries.LOAD_BCFY_CALLS_MEMBERSHIP_SQL,
        )
        method_names = [item[0] for item in connection.method_calls]
        self.assertLess(
            method_names.index("fetchrow"),
            method_names.index("fetch"),
        )

    async def test_invalid_grants_return_shared_rejection_before_members(
        self,
    ) -> None:
        cases = (
            (
                _lease_row(worker_id=_OTHER_OWNER_ID),
                ingestion_lease_store.GrantRejectionReason.OWNER_MISMATCH,
            ),
            (
                _lease_row(fencing_token=8),
                ingestion_lease_store.GrantRejectionReason.FENCE_MISMATCH,
            ),
            (
                _lease_row(
                    status="unclaimed",
                    worker_id=None,
                    last_heartbeat=None,
                ),
                ingestion_lease_store.GrantRejectionReason.STATUS_INELIGIBLE,
            ),
            (None, ingestion_lease_store.GrantRejectionReason.MISSING),
        )

        for row, expected_reason in cases:
            with self.subTest(reason=expected_reason.value):
                pool = connection_util.make_mock_pool(transaction=True)
                connection = pool.acquired_connection
                connection.fetchrow.return_value = row
                store = ingestion_lease_store.IngestionLeaseStore(pool)

                result = await store.load_membership(_grant())

                self.assertIsInstance(
                    result,
                    ingestion_lease_store.GrantRejected,
                )
                assert isinstance(result, ingestion_lease_store.GrantRejected)
                self.assertIs(result.reason, expected_reason)
                if expected_reason is (
                    ingestion_lease_store.GrantRejectionReason.MISSING
                ):
                    self.assertIsNone(result.snapshot)
                else:
                    self.assertIsNotNone(result.snapshot)
                connection.fetch.assert_not_awaited()

    async def test_empty_and_no_eligible_members_fail_closed(self) -> None:
        cases = (
            (),
            (_member_row(status="deactivated"),),
        )

        for case_index, rows in enumerate(cases):
            with self.subTest(case_index=case_index):
                pool = connection_util.make_mock_pool(transaction=True)
                connection = pool.acquired_connection
                connection.fetchrow.return_value = _lease_row()
                connection.fetch.return_value = list(rows)
                store = ingestion_lease_store.IngestionLeaseStore(pool)

                result = await store.load_membership(_grant())

                self.assertIsInstance(
                    result,
                    ingestion_lease_store.MembershipInvariantViolation,
                )

    async def test_missing_identity_and_source_mismatch_fail_closed(
        self,
    ) -> None:
        cases = (
            _member_row(source_feed_id=None),
            _member_row(feed_source_type="openmhz"),
            _member_row(group_id=None),
            _member_row(feed_source_type=None),
            _member_row(feed_id=None),
            _member_row(sid="not-numeric"),
        )

        for case_index, row in enumerate(cases):
            with self.subTest(case_index=case_index):
                pool = connection_util.make_mock_pool(transaction=True)
                connection = pool.acquired_connection
                connection.fetchrow.return_value = _lease_row()
                connection.fetch.return_value = [row]
                store = ingestion_lease_store.IngestionLeaseStore(pool)

                result = await store.load_membership(_grant())

                self.assertIsInstance(
                    result,
                    ingestion_lease_store.MembershipInvariantViolation,
                )

    async def test_revision_changes_snapshot_but_not_grant_identity(
        self,
    ) -> None:
        pool = connection_util.make_mock_pool(transaction=True)
        connection = pool.acquired_connection
        connection.fetchrow.side_effect = [
            _lease_row(lease_key="00123", membership_revision=4),
            _lease_row(lease_key="00123", membership_revision=5),
        ]
        connection.fetch.side_effect = [[_member_row()], [_member_row()]]
        store = ingestion_lease_store.IngestionLeaseStore(pool)
        grant = _grant("00123")

        first = await store.load_membership(grant)
        second = await store.load_membership(grant)

        self.assertEqual(first.grant, second.grant)
        self.assertEqual(first.membership_revision, 4)
        self.assertEqual(second.membership_revision, 5)

    async def test_property_free_claim_can_fail_closed_on_membership_load(
        self,
    ) -> None:
        pool = connection_util.make_mock_pool(transaction=True)
        pool.fetch.return_value = [_lease_row(fencing_token=8)]
        connection = pool.acquired_connection
        connection.fetchrow.return_value = _lease_row(fencing_token=8)
        connection.fetch.return_value = []
        store = ingestion_lease_store.IngestionLeaseStore(pool)

        claims = await store.claim_unclaimed(
            feed_store.SourceType.BCFY_CALLS,
            _OWNER_ID,
            1,
        )
        result = await store.load_membership(claims[0].grant)

        self.assertIsInstance(
            result,
            ingestion_lease_store.MembershipInvariantViolation,
        )


if __name__ == "__main__":
    unittest.main()
