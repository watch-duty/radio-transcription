"""Focused orchestration tests for the fenced ingestion Lease store."""

from __future__ import annotations

import dataclasses
import datetime
import typing
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
        "failure_count": 2,
        "status_reason": "source_unreachable",
        "membership_revision": 4,
        "applied": False,
        "final_status": None,
    }
    row.update(overrides)
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
        "last_bookmark_time": _NOW,
        "failure_count": 0,
        "retry_after": None,
        "status_reason": None,
        "status_reason_detail": None,
    }
    row.update(overrides)
    return row


class TestIngestionLeaseStoreValidation(unittest.IsolatedAsyncioTestCase):
    """Tests that malformed control input fails before pool checkout."""

    async def test_claim_rejects_invalid_source_owner_and_limit(self) -> None:
        pool = connection_util.make_mock_pool()
        store = ingestion_lease_store.IngestionLeaseStore(pool)

        invalid_calls = (
            store.claim_unclaimed(
                "bcfy_calls",  # ty: ignore[invalid-argument-type]
                _OWNER_ID,
                1,
            ),
            store.claim_unclaimed(
                feed_store.SourceType.BCFY_CALLS,
                "worker",  # ty: ignore[invalid-argument-type]
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
                        abandonment_after,  # ty: ignore[invalid-argument-type]
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
            await store.release(
                _grant(),
                cause="shutdown",  # ty: ignore[invalid-argument-type]
            )

        pool.fetchrow.assert_not_awaited()

    def test_budgeted_failure_rejects_invalid_parameters(self) -> None:
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

    def test_non_budgeted_failure_requires_utc_aware_retry(self) -> None:
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
                    ingestion_lease_store.NonBudgetedFailure(
                        retry_after,  # ty: ignore[invalid-argument-type]
                    )

    async def test_finalize_failure_validates_before_pool_checkout(
        self,
    ) -> None:
        pool = connection_util.make_mock_pool()
        store = ingestion_lease_store.IngestionLeaseStore(pool)
        action = ingestion_lease_store.BudgetedFailure()
        status_reason = feed_store.FeedStatusReason.SOURCE_UNREACHABLE
        invalid_call_factories = (
            lambda: store.finalize_failure(
                "grant",  # ty: ignore[invalid-argument-type]
                action,
                status_reason,
                actor_id="collector",
            ),
            lambda: store.finalize_failure(
                _grant(),
                object(),  # ty: ignore[invalid-argument-type]
                status_reason,
                actor_id="collector",
            ),
            lambda: store.finalize_failure(
                _grant(),
                action,
                "source_unreachable",  # ty: ignore[invalid-argument-type]
                actor_id="collector",
            ),
            lambda: store.finalize_failure(
                _grant(),
                action,
                status_reason,
                actor_id="has space",
            ),
            lambda: store.finalize_failure(
                _grant(),
                action,
                status_reason,
                actor_id="collector",
                reason=123,  # ty: ignore[invalid-argument-type]
            ),
        )

        for case_index, make_call in enumerate(invalid_call_factories):
            with self.subTest(case_index=case_index):
                with self.assertRaises((TypeError, ValueError)):
                    await make_call()

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
        self.assertEqual(result[0].snapshot.failure_count, 0)
        self.assertIs(
            result[0].snapshot.status_reason,
            feed_store.FeedStatusReason.SOURCE_UNREACHABLE,
        )
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

    async def test_exact_nonapplied_heartbeat_fails_closed(self) -> None:
        pool = connection_util.make_mock_pool(
            fetch_result=[_lease_row(caller_ordinal=0, applied=False)]
        )
        store = ingestion_lease_store.IngestionLeaseStore(pool)

        with self.assertRaisesRegex(
            RuntimeError,
            "heartbeat did not update an exact active Lease grant",
        ):
            await store.renew_heartbeats((_grant(),))


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
                assert isinstance(
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
                    applied=True,
                )
            )
            store = ingestion_lease_store.IngestionLeaseStore(pool)

            result = await store.release(_grant(), cause=cause)

            self.assertIs(
                result.disposition,
                ingestion_lease_store.LeaseOperationDisposition.APPLIED,
            )
            assert result.snapshot is not None
            self.assertIs(
                result.snapshot.status, feed_store.FeedStatus.UNCLAIMED
            )
            self.assertEqual(result.snapshot.failure_count, 2)
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
        assert result.snapshot is not None
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

    async def test_exact_nonapplied_release_fails_closed(self) -> None:
        pool = connection_util.make_mock_pool(
            fetchrow_result=_lease_row(applied=False)
        )
        store = ingestion_lease_store.IngestionLeaseStore(pool)

        with self.assertRaisesRegex(
            RuntimeError,
            "release did not update an exact active Lease grant",
        ):
            await store.release(_grant())

    async def test_database_exception_is_not_retried(self) -> None:
        pool = connection_util.make_mock_pool()
        pool.fetchrow.side_effect = RuntimeError("release failed")
        store = ingestion_lease_store.IngestionLeaseStore(pool)

        with self.assertRaisesRegex(RuntimeError, "release failed"):
            await store.release(_grant())

        pool.fetchrow.assert_awaited_once()


class TestFinalizeLeaseFailure(unittest.IsolatedAsyncioTestCase):
    """Tests for one-shot exact-grant failure finalization."""

    async def test_budgeted_failure_records_final_status_and_parameters(
        self,
    ) -> None:
        pool = connection_util.make_mock_pool(
            fetchrow_result=_lease_row(
                applied=True,
                final_status="failing",
            )
        )
        store = ingestion_lease_store.IngestionLeaseStore(pool)

        result = await store.finalize_failure(
            _grant(),
            ingestion_lease_store.BudgetedFailure(),
            feed_store.FeedStatusReason.SOURCE_UNREACHABLE,
            actor_id="service_account:gcp:collector",
            reason="provider timeout",
        )

        self.assertEqual(
            result,
            ingestion_lease_store.LeaseFailureResult(
                ingestion_lease_store.LeaseOperationDisposition.APPLIED,
                feed_store.FeedStatus.FAILING,
            ),
        )
        pool.fetchrow.assert_awaited_once_with(
            ingestion_lease_queries.FINALIZE_BUDGETED_FAILURE_SQL,
            "bcfy_calls",
            "123",
            _OWNER_ID,
            7,
            5,
            15,
            600,
            "source_unreachable",
            "provider timeout",
        )

    async def test_budgeted_failure_reports_quarantine(self) -> None:
        pool = connection_util.make_mock_pool(
            fetchrow_result=_lease_row(
                applied=True,
                final_status="quarantined",
            )
        )
        store = ingestion_lease_store.IngestionLeaseStore(pool)

        result = await store.finalize_failure(
            _grant(),
            ingestion_lease_store.BudgetedFailure(),
            feed_store.FeedStatusReason.SOURCE_UNREACHABLE,
            actor_id="collector",
        )

        self.assertIs(
            result.final_status,
            feed_store.FeedStatus.QUARANTINED,
        )

    async def test_non_budgeted_failure_uses_caller_retry_and_resets_budget(
        self,
    ) -> None:
        retry_after = _NOW + datetime.timedelta(minutes=8)
        pool = connection_util.make_mock_pool(
            fetchrow_result=_lease_row(
                applied=True,
                final_status="failing",
            )
        )
        store = ingestion_lease_store.IngestionLeaseStore(pool)

        result = await store.finalize_failure(
            _grant(),
            ingestion_lease_store.NonBudgetedFailure(retry_after),
            feed_store.FeedStatusReason.SYSTEM_PIPELINE_ERROR,
            actor_id="collector",
        )

        self.assertIs(
            result.final_status,
            feed_store.FeedStatus.FAILING,
        )
        pool.fetchrow.assert_awaited_once_with(
            ingestion_lease_queries.FINALIZE_NON_BUDGETED_FAILURE_SQL,
            "bcfy_calls",
            "123",
            _OWNER_ID,
            7,
            retry_after,
            "system_pipeline_error",
            None,
        )

    async def test_failure_detail_is_bounded_before_database_call(self) -> None:
        pool = connection_util.make_mock_pool(
            fetchrow_result=_lease_row(
                applied=True,
                final_status="failing",
            )
        )
        store = ingestion_lease_store.IngestionLeaseStore(pool)

        await store.finalize_failure(
            _grant(),
            ingestion_lease_store.BudgetedFailure(),
            feed_store.FeedStatusReason.SOURCE_UNREACHABLE,
            actor_id="collector",
            reason="x" * 3000,
        )

        detail = pool.fetchrow.await_args.args[-1]
        self.assertEqual(len(detail), 2048)
        self.assertTrue(detail.endswith("[truncated]"))

    async def test_rejections_are_typed_without_final_status(self) -> None:
        cases = (
            (None, ingestion_lease_store.LeaseOperationDisposition.MISSING),
            (
                _lease_row(worker_id=_OTHER_OWNER_ID),
                ingestion_lease_store.LeaseOperationDisposition.OWNER_MISMATCH,
            ),
            (
                _lease_row(fencing_token=8),
                ingestion_lease_store.LeaseOperationDisposition.FENCE_MISMATCH,
            ),
            (
                _lease_row(status="failing", worker_id=None),
                ingestion_lease_store.LeaseOperationDisposition.STATUS_INELIGIBLE,
            ),
        )

        for case_index, (row, disposition) in enumerate(cases):
            with self.subTest(case_index=case_index):
                pool = connection_util.make_mock_pool(fetchrow_result=row)
                store = ingestion_lease_store.IngestionLeaseStore(pool)
                result = await store.finalize_failure(
                    _grant(),
                    ingestion_lease_store.BudgetedFailure(),
                    feed_store.FeedStatusReason.SOURCE_UNREACHABLE,
                    actor_id="collector",
                )

                self.assertIs(result.disposition, disposition)
                self.assertIsNone(result.final_status)

    async def test_applied_failure_log_uses_final_status(self) -> None:
        pool = connection_util.make_mock_pool(
            fetchrow_result=_lease_row(
                applied=True,
                final_status="failing",
            )
        )
        store = ingestion_lease_store.IngestionLeaseStore(pool)

        with mock.patch.object(ingestion_lease_store.logger, "warning") as log:
            await store.finalize_failure(
                _grant(),
                ingestion_lease_store.BudgetedFailure(),
                feed_store.FeedStatusReason.SOURCE_UNREACHABLE,
                actor_id="collector",
            )

        fields = log.call_args.kwargs["extra"]
        self.assertEqual(fields["final_status"], "failing")
        self.assertNotIn("failure_effect", fields)

    async def test_exact_nonapplied_or_mismatched_applied_result_fails_closed(
        self,
    ) -> None:
        cases = (
            _lease_row(),
            _lease_row(
                applied=True,
                final_status="failing",
                worker_id=_OTHER_OWNER_ID,
            ),
            _lease_row(applied=True, final_status="active"),
        )

        for case_index, row in enumerate(cases):
            with self.subTest(case_index=case_index):
                pool = connection_util.make_mock_pool(fetchrow_result=row)
                store = ingestion_lease_store.IngestionLeaseStore(pool)
                with self.assertRaises((RuntimeError, ValueError)):
                    await store.finalize_failure(
                        _grant(),
                        ingestion_lease_store.BudgetedFailure(),
                        feed_store.FeedStatusReason.SOURCE_UNREACHABLE,
                        actor_id="collector",
                    )

    async def test_database_exception_is_not_retried(self) -> None:
        pool = connection_util.make_mock_pool()
        pool.fetchrow.side_effect = RuntimeError("outcome unknown")
        store = ingestion_lease_store.IngestionLeaseStore(pool)

        with self.assertRaisesRegex(RuntimeError, "outcome unknown"):
            await store.finalize_failure(
                _grant(),
                ingestion_lease_store.BudgetedFailure(),
                feed_store.FeedStatusReason.SOURCE_UNREACHABLE,
                actor_id="collector",
            )

        pool.fetchrow.assert_awaited_once()


class TestLoadMembership(unittest.IsolatedAsyncioTestCase):
    """Tests for fail-closed authoritative membership snapshots."""

    def test_snapshot_contract_omits_excluded_count(self) -> None:
        fields = {
            field.name
            for field in dataclasses.fields(
                ingestion_lease_store.MembershipSnapshot
            )
        }

        self.assertNotIn("excluded_count", fields)

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
        self.assertEqual(result.members[0].identity.sid, "00123")
        self.assertEqual(result.members[0].identity.group_id, "00045")
        self.assertEqual(
            result.members[0].identity.source_feed_id,
            "00123-00045",
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
                connection.fetchrow.return_value = _lease_row(lease_key="00123")
                connection.fetch.return_value = list(rows)
                store = ingestion_lease_store.IngestionLeaseStore(pool)

                result = await store.load_membership(_grant("00123"))

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
            _member_row(feed_source_type="future_source"),
            _member_row(group_id=None),
            _member_row(feed_source_type=None),
            _member_row(feed_id=None),
            _member_row(sid="not-numeric"),
        )

        for case_index, row in enumerate(cases):
            with self.subTest(case_index=case_index):
                pool = connection_util.make_mock_pool(transaction=True)
                connection = pool.acquired_connection
                connection.fetchrow.return_value = _lease_row(lease_key="00123")
                connection.fetch.return_value = [row]
                store = ingestion_lease_store.IngestionLeaseStore(pool)

                result = await store.load_membership(_grant("00123"))

                self.assertIsInstance(
                    result,
                    ingestion_lease_store.MembershipInvariantViolation,
                )

    async def test_unknown_member_lifecycle_values_fail_closed(self) -> None:
        rows = (
            _member_row(status="future_status"),
            _member_row(status_reason="future_reason"),
        )

        for case_index, row in enumerate(rows):
            with self.subTest(case_index=case_index):
                pool = connection_util.make_mock_pool(transaction=True)
                connection = pool.acquired_connection
                connection.fetchrow.return_value = _lease_row(lease_key="00123")
                connection.fetch.return_value = [row]
                store = ingestion_lease_store.IngestionLeaseStore(pool)

                result = await store.load_membership(_grant("00123"))

                self.assertIsInstance(
                    result,
                    ingestion_lease_store.MembershipInvariantViolation,
                )
                assert isinstance(
                    result,
                    ingestion_lease_store.MembershipInvariantViolation,
                )
                self.assertIn("unknown lifecycle value", result.detail)

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

        assert isinstance(first, ingestion_lease_store.MembershipSnapshot)
        assert isinstance(second, ingestion_lease_store.MembershipSnapshot)
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


class TestRefreshMembership(unittest.IsolatedAsyncioTestCase):
    """Tests for atomic revision-aware exact-grant membership refresh."""

    def test_refresh_result_contract_is_closed_frozen_and_slotted(self) -> None:
        self.assertEqual(
            set(
                typing.get_args(
                    ingestion_lease_store.MembershipRefreshResult.__value__
                )
            ),
            {
                ingestion_lease_store.MembershipSnapshot,
                ingestion_lease_store.MembershipUnchanged,
                ingestion_lease_store.GrantRejected,
                ingestion_lease_store.MembershipInvariantViolation,
            },
        )
        self.assertEqual(
            {
                field.name
                for field in dataclasses.fields(
                    ingestion_lease_store.MembershipInvariantViolation
                )
            },
            {"grant", "detail"},
        )
        unchanged = ingestion_lease_store.MembershipUnchanged(_grant(), 4)
        self.assertTrue(hasattr(type(unchanged), "__slots__"))
        with self.assertRaises(dataclasses.FrozenInstanceError):
            typing.cast("typing.Any", unchanged).membership_revision = 5

    async def test_initial_refresh_loads_one_complete_snapshot(self) -> None:
        pool = connection_util.make_mock_pool(transaction=True)
        connection = pool.acquired_connection
        connection.fetchrow.return_value = _lease_row(
            lease_key="00123",
            membership_revision=4,
        )
        connection.fetch.return_value = [_member_row()]
        store = ingestion_lease_store.IngestionLeaseStore(pool)

        result = await store.refresh_membership(
            _grant("00123"),
            known_revision=None,
        )

        self.assertIsInstance(
            result,
            ingestion_lease_store.MembershipSnapshot,
        )
        assert isinstance(result, ingestion_lease_store.MembershipSnapshot)
        self.assertEqual(result.membership_revision, 4)
        self.assertEqual(len(result.members), 1)
        connection.fetchrow.assert_awaited_once_with(
            ingestion_lease_queries.LOCK_LEASE_SQL,
            "bcfy_calls",
            "00123",
        )
        connection.fetch.assert_awaited_once_with(
            ingestion_lease_queries.LOAD_BCFY_CALLS_MEMBERSHIP_SQL,
            "00123",
        )
        method_names = [item[0] for item in connection.method_calls]
        self.assertLess(
            method_names.index("fetchrow"),
            method_names.index("fetch"),
        )
        connection.transaction.assert_called_once_with(
            isolation="read_committed"
        )

    async def test_equal_revision_returns_unchanged_without_child_read(
        self,
    ) -> None:
        pool = connection_util.make_mock_pool(transaction=True)
        connection = pool.acquired_connection
        connection.fetchrow.return_value = _lease_row(membership_revision=4)
        store = ingestion_lease_store.IngestionLeaseStore(pool)

        result = await store.refresh_membership(
            _grant(),
            known_revision=4,
        )

        self.assertEqual(
            result,
            ingestion_lease_store.MembershipUnchanged(_grant(), 4),
        )
        connection.fetchrow.assert_awaited_once_with(
            ingestion_lease_queries.LOCK_LEASE_SQL,
            "bcfy_calls",
            "123",
        )
        connection.fetch.assert_not_awaited()

    async def test_higher_revision_reloads_once_and_regression_reads_no_child(
        self,
    ) -> None:
        pool = connection_util.make_mock_pool(transaction=True)
        connection = pool.acquired_connection
        connection.fetchrow.side_effect = [
            _lease_row(lease_key="00123", membership_revision=5),
            _lease_row(lease_key="00123", membership_revision=4),
        ]
        connection.fetch.return_value = [_member_row()]
        store = ingestion_lease_store.IngestionLeaseStore(pool)
        grant = _grant("00123")

        changed = await store.refresh_membership(
            grant,
            known_revision=4,
        )
        regressed = await store.refresh_membership(
            grant,
            known_revision=5,
        )

        assert isinstance(changed, ingestion_lease_store.MembershipSnapshot)
        self.assertEqual(changed.membership_revision, 5)
        assert isinstance(
            regressed,
            ingestion_lease_store.MembershipInvariantViolation,
        )
        self.assertIn(
            "revision regressed",
            regressed.detail,
        )
        connection.fetch.assert_awaited_once_with(
            ingestion_lease_queries.LOAD_BCFY_CALLS_MEMBERSHIP_SQL,
            "00123",
        )

    async def test_invalid_known_revisions_fail_before_checkout(self) -> None:
        invalid_revisions = (-1, True, 1.0, "4")

        for case_index, known_revision in enumerate(invalid_revisions):
            with self.subTest(case_index=case_index):
                pool = connection_util.make_mock_pool(transaction=True)
                store = ingestion_lease_store.IngestionLeaseStore(pool)

                with self.assertRaises((TypeError, ValueError)):
                    await store.refresh_membership(
                        _grant(),
                        known_revision=typing.cast("int", known_revision),
                    )

                pool.acquire.assert_not_called()

    async def test_exact_grant_rejection_reads_no_children(self) -> None:
        cases = (
            None,
            _lease_row(worker_id=_OTHER_OWNER_ID),
            _lease_row(fencing_token=8),
            _lease_row(status="unclaimed"),
        )

        for case_index, lease_row in enumerate(cases):
            with self.subTest(case_index=case_index):
                pool = connection_util.make_mock_pool(transaction=True)
                connection = pool.acquired_connection
                connection.fetchrow.return_value = lease_row
                store = ingestion_lease_store.IngestionLeaseStore(pool)

                result = await store.refresh_membership(
                    _grant(),
                    known_revision=4,
                )

                self.assertIsInstance(
                    result,
                    ingestion_lease_store.GrantRejected,
                )
                connection.fetch.assert_not_awaited()

    async def test_duplicate_exact_routing_key_fails_closed(self) -> None:
        pool = connection_util.make_mock_pool(transaction=True)
        connection = pool.acquired_connection
        connection.fetchrow.return_value = _lease_row(lease_key="00123")
        connection.fetch.return_value = [
            _member_row(),
            _member_row(
                feed_id=uuid.UUID("bbbbbbbb-cccc-dddd-eeee-ffffffffffff"),
            ),
        ]
        store = ingestion_lease_store.IngestionLeaseStore(pool)

        result = await store.refresh_membership(
            _grant("00123"),
            known_revision=None,
        )

        assert isinstance(
            result,
            ingestion_lease_store.MembershipInvariantViolation,
        )
        self.assertIn(
            "duplicate canonical routing key",
            result.detail,
        )

    async def test_textually_distinct_leading_zero_keys_remain_distinct(
        self,
    ) -> None:
        pool = connection_util.make_mock_pool(transaction=True)
        connection = pool.acquired_connection
        connection.fetchrow.return_value = _lease_row(lease_key="00123")
        connection.fetch.return_value = [
            _member_row(),
            _member_row(
                feed_id=uuid.UUID("bbbbbbbb-cccc-dddd-eeee-ffffffffffff"),
                source_feed_id="00123-000045",
                group_id="000045",
            ),
        ]
        store = ingestion_lease_store.IngestionLeaseStore(pool)

        result = await store.refresh_membership(
            _grant("00123"),
            known_revision=None,
        )

        assert isinstance(result, ingestion_lease_store.MembershipSnapshot)
        self.assertEqual(
            tuple(member.identity.source_feed_id for member in result.members),
            ("00123-00045", "00123-000045"),
        )

    async def test_snapshots_are_frozen_copies_of_each_loaded_page(
        self,
    ) -> None:
        first_row = _member_row()
        second_row = _member_row(
            feed_id=uuid.UUID("bbbbbbbb-cccc-dddd-eeee-ffffffffffff"),
            source_feed_id="00123-00046",
            group_id="00046",
        )
        first_page = [first_row]
        second_page = [second_row]
        pool = connection_util.make_mock_pool(transaction=True)
        connection = pool.acquired_connection
        connection.fetchrow.side_effect = [
            _lease_row(lease_key="00123", membership_revision=4),
            _lease_row(lease_key="00123", membership_revision=5),
        ]
        connection.fetch.side_effect = [first_page, second_page]
        store = ingestion_lease_store.IngestionLeaseStore(pool)
        grant = _grant("00123")

        first = await store.refresh_membership(
            grant,
            known_revision=None,
        )
        second = await store.refresh_membership(
            grant,
            known_revision=4,
        )
        first_row["source_feed_id"] = "mutated"
        second_row["source_feed_id"] = "mutated"
        first_page.append(second_row)
        second_page.clear()

        assert isinstance(first, ingestion_lease_store.MembershipSnapshot)
        assert isinstance(second, ingestion_lease_store.MembershipSnapshot)
        self.assertEqual(
            tuple(member.identity.source_feed_id for member in first.members),
            ("00123-00045",),
        )
        self.assertEqual(
            tuple(member.identity.source_feed_id for member in second.members),
            ("00123-00046",),
        )
        with self.assertRaises(dataclasses.FrozenInstanceError):
            typing.cast("typing.Any", first).members = ()


if __name__ == "__main__":
    unittest.main()
