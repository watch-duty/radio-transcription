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
            assert result.snapshot is not None
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


if __name__ == "__main__":
    unittest.main()
