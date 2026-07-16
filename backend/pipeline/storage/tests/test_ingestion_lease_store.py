"""Focused orchestration tests for the fenced ingestion Lease store."""

from __future__ import annotations

import asyncio
import dataclasses
import datetime
import typing
import unittest
import uuid
from unittest import mock

from backend.pipeline.ingestion import failure_policy
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
        "membership_revision": 4,
        "updated_at": _NOW,
        "applied": False,
        "final_status": None,
    }
    row.update(overrides)
    return row


def _member_row(**overrides: object) -> dict[str, object]:
    row: dict[str, object] = {
        "feed_id": uuid.UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"),
        "feed_name": "County Fire Dispatch",
        "property_source_type": "bcfy_calls",
        "feed_source_type": "bcfy_calls",
        "source_feed_id": "00123-00045",
        "sid": "00123",
        "group_id": "00045",
        "status": "active",
        "last_bookmark_time": _NOW,
    }
    row.update(overrides)
    return row


def _member_identity(
    feed_id: uuid.UUID,
    *,
    sid: str = "123",
    group_id: str = "45",
) -> ingestion_lease_store.LeaseMemberIdentity:
    return ingestion_lease_store.LeaseMemberIdentity(
        feed_id=feed_id,
        source_type=feed_store.SourceType.BCFY_CALLS,
        source_feed_id=f"{sid}-{group_id}",
        sid=sid,
        group_id=group_id,
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


def _audit_property_row(
    feed_id: uuid.UUID,
    **overrides: object,
) -> dict[str, object]:
    row: dict[str, object] = {
        "feed_id": feed_id,
        "source_feed_id": "123-45",
        "tags": [],
    }
    row.update(overrides)
    return row


def _child_audit_payload(
    feed_id: uuid.UUID,
    *,
    caller_ordinal: int,
) -> dict[str, object]:
    return {
        "caller_ordinal": caller_ordinal,
        "feed_audit_event": {
            "event_type": "radio_transcription.feed_change_notification",
            "schema_version": 1,
            "event_id": uuid.uuid4(),
            "action": "feed.recovered",
            "occurred_at": _NOW,
            "actor_id": "service_account:gcp:collector",
            "feed_id": feed_id,
            "feed_revision": 1,
            "before_values": {"status": "failing"},
            "after_values": {"status": "active"},
        },
    }


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

    async def test_primary_claim_returns_complete_grant(
        self,
    ) -> None:
        pool = connection_util.make_mock_pool(
            fetch_result=[_lease_row(fencing_token=8)]
        )
        store = ingestion_lease_store.IngestionLeaseStore(pool)

        result = await store.claim_unclaimed(
            feed_store.SourceType.BCFY_CALLS,
            _OWNER_ID,
            1,
        )

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].grant, _grant(fencing_token=8))
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
            {"lease_key": ""},
            {"worker_id": "not-a-uuid"},
            {"fencing_token": True},
        )

        for overrides in cases:
            with self.subTest(overrides=overrides):
                pool = connection_util.make_mock_pool(
                    fetch_result=[_lease_row(**overrides)]
                )
                store = ingestion_lease_store.IngestionLeaseStore(pool)
                with self.assertRaises((TypeError, ValueError)):
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
    """Tests for exact-grant rejection classification."""

    def setUp(self) -> None:
        self.store = ingestion_lease_store.IngestionLeaseStore(mock.AsyncMock())
        self.grant = _grant()

    def test_missing_is_classified_without_exposing_row_state(self) -> None:
        result = self.store._grant_rejection(self.grant, None)

        self.assertIsNotNone(result)
        assert result is not None
        self.assertIs(
            result.reason,
            ingestion_lease_store.GrantRejectionReason.MISSING,
        )

    def test_owner_fence_and_status_rejections_are_classified(self) -> None:
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

    def test_exact_active_grant_is_not_rejected(self) -> None:
        self.assertIsNone(self.store._grant_rejection(self.grant, _lease_row()))


class TestIngestionLeaseStoreRelease(unittest.IsolatedAsyncioTestCase):
    """Tests for one neutral exact-grant release policy."""

    async def test_all_closed_causes_execute_identical_sql(self) -> None:
        observed_args = []

        for cause in ingestion_lease_store.LeaseReleaseCause:
            pool = connection_util.make_mock_pool(
                fetchrow_result=_lease_row(
                    applied=True,
                )
            )
            store = ingestion_lease_store.IngestionLeaseStore(pool)

            result = await store.release(_grant(), cause=cause)

            self.assertIs(
                result.disposition,
                ingestion_lease_store.LeaseOperationDisposition.APPLIED,
            )
            observed_args.append(pool.fetchrow.await_args.args)

        self.assertTrue(all(args == observed_args[0] for args in observed_args))
        self.assertIs(
            observed_args[0][0],
            ingestion_lease_queries.RELEASE_LEASE_SQL,
        )

    async def test_stale_release_returns_only_rejection_disposition(
        self,
    ) -> None:
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

    async def test_missing_release_is_typed(self) -> None:
        pool = connection_util.make_mock_pool(fetchrow_result=None)
        store = ingestion_lease_store.IngestionLeaseStore(pool)

        result = await store.release(_grant())

        self.assertIs(
            result.disposition,
            ingestion_lease_store.LeaseOperationDisposition.MISSING,
        )

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
                    ingestion_lease_store.NonBudgetedFailure(
                        retry_after,  # ty: ignore[invalid-argument-type]
                    )

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
                "source_unreachable",  # ty: ignore[invalid-argument-type]
                actor_id="service_account:gcp:collector",
            )

        pool.fetchrow.assert_not_awaited()


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
        self.assertEqual(result.members[0].name, "County Fire Dispatch")
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

    async def test_membership_feed_name_rejects_missing_null_and_blank(
        self,
    ) -> None:
        invalid_rows = [
            _member_row(feed_name=None),
            _member_row(feed_name=7),
            _member_row(feed_name=""),
            _member_row(feed_name=" \t "),
        ]
        missing_name = _member_row()
        del missing_name["feed_name"]
        invalid_rows.append(missing_name)

        for case_index, row in enumerate(invalid_rows):
            with self.subTest(case_index=case_index):
                pool = connection_util.make_mock_pool(transaction=True)
                connection = pool.acquired_connection
                connection.fetchrow.return_value = _lease_row(lease_key="00123")
                connection.fetch.return_value = [row]
                store = ingestion_lease_store.IngestionLeaseStore(pool)

                with self.assertRaises((TypeError, ValueError)):
                    await store.load_membership(_grant("00123"))

                connection.fetch.assert_awaited_once_with(
                    ingestion_lease_queries.LOAD_BCFY_CALLS_MEMBERSHIP_SQL,
                    "00123",
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
        unchanged = ingestion_lease_store.MembershipUnchanged(_grant(), 4)
        self.assertTrue(hasattr(type(unchanged), "__slots__"))
        with self.assertRaises(dataclasses.FrozenInstanceError):
            unchanged.membership_revision = 5  # type: ignore[misc]  # ty: ignore[invalid-assignment]

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
        self.assertIs(
            regressed.reason,
            ingestion_lease_store.MembershipInvariantReason.REVISION_REGRESSION,
        )
        connection.fetch.assert_awaited_once_with(
            ingestion_lease_queries.LOAD_BCFY_CALLS_MEMBERSHIP_SQL,
            "00123",
        )

    async def test_invalid_known_revisions_fail_before_checkout(self) -> None:
        invalid_revisions = (-1, True, 1.0, "4")

        for known_revision in invalid_revisions:
            with self.subTest(known_revision=known_revision):
                pool = connection_util.make_mock_pool(transaction=True)
                store = ingestion_lease_store.IngestionLeaseStore(pool)

                with self.assertRaises((TypeError, ValueError)):
                    await store.refresh_membership(
                        _grant(),
                        known_revision=known_revision,  # ty: ignore[invalid-argument-type]
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
        self.assertIs(
            result.reason,
            ingestion_lease_store.MembershipInvariantReason.DUPLICATE_ROUTING_KEY,
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
            first.members = ()  # type: ignore[misc]  # ty: ignore[invalid-assignment]


class TestCommitChildMutations(unittest.IsolatedAsyncioTestCase):
    """Tests for the one-attempt fenced child transaction."""

    def _progress(
        self,
        feed_id: uuid.UUID,
        *,
        cursor: datetime.datetime | None = _NOW,
        path: str = "gs://bucket/audio.flac",
    ) -> ingestion_lease_store.AdmittedAudioProgress:
        return ingestion_lease_store.AdmittedAudioProgress(
            _member_identity(feed_id),
            path,
            cursor,
        )

    def _failure(
        self,
        feed_id: uuid.UUID,
        *,
        cursor: datetime.datetime | None = _NOW,
        action: ingestion_lease_store.LeaseFailureAction | None = None,
        charge_mode: ingestion_lease_store.FeedFailureChargeMode = (
            ingestion_lease_store.FeedFailureChargeMode.ON_CURSOR_ADVANCE
        ),
        status_reason: feed_store.FeedStatusReason = (
            feed_store.FeedStatusReason.SYSTEM_CONFIGURATION_INVALID
        ),
    ) -> ingestion_lease_store.FeedFailureTransition:
        if action is None:
            action = ingestion_lease_store.BudgetedFailure()
        return ingestion_lease_store.FeedFailureTransition(
            member=_member_identity(feed_id),
            action=action,
            status_reason=status_reason,
            reason="boundary failed",
            completion_cursor=cursor,
            charge_mode=charge_mode,
        )

    def _closed_cohort(
        self,
        feed_id: uuid.UUID,
        *,
        cursor: datetime.datetime | None = _NOW,
        path: str | None = "gs://bucket/cohort.flac",
    ) -> ingestion_lease_store.ClosedCohortProgress:
        return ingestion_lease_store.ClosedCohortProgress(
            member=_member_identity(feed_id),
            last_processed_filename=path,
            cursor=cursor,
        )

    async def test_precheckout_validation_rejects_bad_batch_shapes(
        self,
    ) -> None:
        feed_id = uuid.UUID("aaaaaaaa-0000-0000-0000-000000000001")
        malformed_member = dataclasses.replace(
            _member_identity(feed_id),
            source_feed_id="123-999",
        )
        invalid_batches = (
            ingestion_lease_store.ChildMutationBatch(
                (
                    self._progress(feed_id),
                    ingestion_lease_store.SourceObservation(
                        _member_identity(feed_id),
                        _NOW,
                    ),
                ),
                ingestion_lease_store.NoLeaseEffect(),
            ),
            ingestion_lease_store.ChildMutationBatch(
                (self._progress(feed_id), self._failure(feed_id)),
                ingestion_lease_store.NoLeaseEffect(),
            ),
            ingestion_lease_store.ChildMutationBatch(
                (
                    ingestion_lease_store.FeedFailureTransition(
                        member=_member_identity(feed_id),
                        action="budgeted",  # ty: ignore[invalid-argument-type]
                        status_reason=(
                            feed_store.FeedStatusReason.SOURCE_UNREACHABLE
                        ),
                        reason="failed",
                        completion_cursor=_NOW,
                        charge_mode=(
                            ingestion_lease_store.FeedFailureChargeMode.ON_CURSOR_ADVANCE
                        ),
                    ),
                ),
                ingestion_lease_store.NoLeaseEffect(),
            ),
            ingestion_lease_store.ChildMutationBatch(
                (
                    ingestion_lease_store.FeedFailureTransition(
                        member=_member_identity(feed_id),
                        action=ingestion_lease_store.BudgetedFailure(),
                        status_reason=(
                            feed_store.FeedStatusReason.SOURCE_UNREACHABLE
                        ),
                        reason="failed",
                        completion_cursor=_NOW,
                        charge_mode="one_shot",  # ty: ignore[invalid-argument-type]
                    ),
                ),
                ingestion_lease_store.NoLeaseEffect(),
            ),
            ingestion_lease_store.ChildMutationBatch(
                (
                    ingestion_lease_store.AdmittedAudioProgress(
                        malformed_member,
                        "gs://bucket/audio.flac",
                        _NOW,
                    ),
                ),
                ingestion_lease_store.NoLeaseEffect(),
            ),
            ingestion_lease_store.ChildMutationBatch(
                (
                    ingestion_lease_store.AdmittedAudioProgress(
                        _member_identity(feed_id),
                        "",
                        _NOW,
                    ),
                ),
                ingestion_lease_store.NoLeaseEffect(),
            ),
            ingestion_lease_store.ChildMutationBatch(
                (
                    ingestion_lease_store.SourceObservation(
                        _member_identity(feed_id),
                        datetime.datetime(2026, 7, 10),
                    ),
                ),
                ingestion_lease_store.NoLeaseEffect(),
            ),
            ingestion_lease_store.ChildMutationBatch(
                (self._closed_cohort(feed_id, cursor=None, path=None),),
                ingestion_lease_store.NoLeaseEffect(),
            ),
            ingestion_lease_store.ChildMutationBatch(
                (self._closed_cohort(feed_id, cursor=None, path=" \t"),),
                ingestion_lease_store.NoLeaseEffect(),
            ),
            ingestion_lease_store.ChildMutationBatch(
                (
                    self._closed_cohort(
                        feed_id,
                        cursor=datetime.datetime(2026, 7, 10),
                    ),
                ),
                ingestion_lease_store.NoLeaseEffect(),
            ),
        )

        for case_index, batch in enumerate(invalid_batches):
            with self.subTest(case_index=case_index):
                pool = connection_util.make_mock_pool(transaction=True)
                store = ingestion_lease_store.IngestionLeaseStore(pool)
                with self.assertRaises((TypeError, ValueError)):
                    await store.commit_child_mutations(
                        _grant(),
                        batch,
                        actor_id="service_account:gcp:collector",
                    )
                pool.acquire.assert_not_called()

        for actor_id in ("", "has space", "x" * 513):
            pool = connection_util.make_mock_pool(transaction=True)
            store = ingestion_lease_store.IngestionLeaseStore(pool)
            with self.assertRaises(ValueError):
                await store.commit_child_mutations(
                    _grant(),
                    ingestion_lease_store.ChildMutationBatch(
                        (self._progress(feed_id),),
                        ingestion_lease_store.NoLeaseEffect(),
                    ),
                    actor_id=actor_id,
                )
            pool.acquire.assert_not_called()

    async def test_closed_cohort_duplicate_feed_rejected_before_checkout(
        self,
    ) -> None:
        feed_id = uuid.UUID("aaaaaaaa-0000-0000-0000-000000000140")
        pool = connection_util.make_mock_pool(transaction=True)
        store = ingestion_lease_store.IngestionLeaseStore(pool)

        with self.assertRaisesRegex(ValueError, "duplicate Feed UUID"):
            await store.commit_child_mutations(
                _grant(),
                ingestion_lease_store.ChildMutationBatch(
                    (
                        self._closed_cohort(feed_id),
                        self._closed_cohort(
                            feed_id,
                            cursor=None,
                            path="gs://bucket/other.flac",
                        ),
                    ),
                    ingestion_lease_store.NoLeaseEffect(),
                ),
                actor_id="service_account:gcp:collector",
            )

        pool.acquire.assert_not_called()

    def test_cursor_and_lifecycle_effects_are_independent(self) -> None:
        feed_id = uuid.UUID("77777777-0000-0000-0000-000000000070")
        earlier = _NOW - datetime.timedelta(seconds=1)
        later = _NOW + datetime.timedelta(seconds=1)
        cursor_cases = (
            (None, _NOW, ingestion_lease_store.CursorEffect.INITIALIZED),
            (_NOW, later, ingestion_lease_store.CursorEffect.ADVANCED),
            (_NOW, _NOW, ingestion_lease_store.CursorEffect.EQUAL),
            (_NOW, earlier, ingestion_lease_store.CursorEffect.REGRESSIVE),
            (_NOW, None, ingestion_lease_store.CursorEffect.ABSENT),
        )

        for current, requested, expected in cursor_cases:
            with self.subTest(expected=expected.value):
                clean_plan = ingestion_lease_store._plan_child_mutation(
                    0,
                    ingestion_lease_store.SourceObservation(
                        _member_identity(feed_id),
                        requested,
                    ),
                    _child_row(feed_id, last_bookmark_time=current),
                )
                self.assertIs(clean_plan.cursor_effect, expected)
                self.assertIs(
                    clean_plan.lifecycle_effect,
                    ingestion_lease_store.LifecycleEffect.NONE,
                )

                failing_plan = ingestion_lease_store._plan_child_mutation(
                    0,
                    ingestion_lease_store.SourceObservation(
                        _member_identity(feed_id),
                        requested,
                    ),
                    _child_row(
                        feed_id,
                        status="failing",
                        failure_count=2,
                        status_reason="source_unreachable",
                        last_bookmark_time=current,
                    ),
                )
                self.assertIs(failing_plan.cursor_effect, expected)
                self.assertIs(
                    failing_plan.lifecycle_effect,
                    ingestion_lease_store.LifecycleEffect.RECOVERED,
                )
                self.assertTrue(failing_plan.needs_update)

    def test_clean_deactivated_cursor_noops_remain_accepted_noop(
        self,
    ) -> None:
        feed_id = uuid.UUID("77777777-0000-0000-0000-000000000071")
        earlier = _NOW - datetime.timedelta(seconds=1)
        cursor_cases = (
            (_NOW, ingestion_lease_store.CursorEffect.EQUAL),
            (earlier, ingestion_lease_store.CursorEffect.REGRESSIVE),
            (None, ingestion_lease_store.CursorEffect.ABSENT),
        )

        for requested, expected in cursor_cases:
            with self.subTest(expected=expected.value):
                plan = ingestion_lease_store._plan_child_mutation(
                    0,
                    self._progress(feed_id, cursor=requested),
                    _child_row(
                        feed_id,
                        status="deactivated",
                        last_bookmark_time=_NOW,
                        last_processed_filename="gs://bucket/audio.flac",
                    ),
                )

                self.assertIs(plan.cursor_effect, expected)
                self.assertFalse(plan.needs_update)
                self.assertIs(
                    plan.disposition,
                    ingestion_lease_store.ChildDisposition.ACCEPTED_NOOP,
                )
                self.assertIs(
                    plan.lifecycle_effect,
                    ingestion_lease_store.LifecycleEffect.NONE,
                )

    def test_closed_cohort_plans_path_and_cursor_independently(self) -> None:
        feed_id = uuid.UUID("77777777-0000-0000-0000-000000000142")
        earlier = _NOW - datetime.timedelta(seconds=1)
        later = _NOW + datetime.timedelta(seconds=1)
        path = "gs://bucket/cohort.flac"
        cases = (
            (
                self._closed_cohort(feed_id, cursor=later, path=path),
                _child_row(feed_id, last_bookmark_time=_NOW),
                ingestion_lease_store.CursorEffect.ADVANCED,
                True,
                True,
            ),
            (
                self._closed_cohort(feed_id, cursor=later, path=None),
                _child_row(feed_id, last_bookmark_time=_NOW),
                ingestion_lease_store.CursorEffect.ADVANCED,
                True,
                False,
            ),
            (
                self._closed_cohort(feed_id, cursor=None, path=path),
                _child_row(feed_id, last_processed_filename="old"),
                ingestion_lease_store.CursorEffect.ABSENT,
                False,
                True,
            ),
            (
                self._closed_cohort(feed_id, cursor=_NOW, path=path),
                _child_row(feed_id, last_bookmark_time=_NOW),
                ingestion_lease_store.CursorEffect.EQUAL,
                False,
                True,
            ),
            (
                self._closed_cohort(feed_id, cursor=earlier, path=path),
                _child_row(feed_id, last_bookmark_time=_NOW),
                ingestion_lease_store.CursorEffect.REGRESSIVE,
                False,
                True,
            ),
            (
                self._closed_cohort(feed_id, cursor=_NOW, path=path),
                _child_row(
                    feed_id,
                    last_bookmark_time=_NOW,
                    last_processed_filename=path,
                ),
                ingestion_lease_store.CursorEffect.EQUAL,
                False,
                False,
            ),
        )

        for mutation, row, cursor_effect, write_cursor, write_path in cases:
            with self.subTest(
                cursor_effect=cursor_effect.value,
                write_cursor=write_cursor,
                write_path=write_path,
            ):
                plan = ingestion_lease_store._plan_child_mutation(
                    0,
                    mutation,
                    row,
                )

                self.assertIs(plan.cursor_effect, cursor_effect)
                self.assertEqual(plan.write_cursor, write_cursor)
                self.assertEqual(plan.write_path, write_path)
                self.assertIs(
                    plan.lifecycle_effect,
                    ingestion_lease_store.LifecycleEffect.NONE,
                )
                self.assertFalse(plan.clear_lifecycle)
                self.assertIsNone(plan.audit_action)
                expected_disposition = (
                    ingestion_lease_store.ChildDisposition.APPLIED
                    if write_cursor or write_path
                    else ingestion_lease_store.ChildDisposition.ACCEPTED_NOOP
                )
                self.assertIs(plan.disposition, expected_disposition)

    def test_closed_cohort_allows_dirty_and_deactivated_members(self) -> None:
        statuses = (
            feed_store.FeedStatus.ACTIVE,
            feed_store.FeedStatus.FAILING,
            feed_store.FeedStatus.DEACTIVATED,
        )
        for index, status in enumerate(statuses):
            with self.subTest(status=status.value):
                feed_id = uuid.UUID(int=150 + index)
                plan = ingestion_lease_store._plan_child_mutation(
                    0,
                    self._closed_cohort(feed_id, cursor=None),
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

                expected = ingestion_lease_store.ChildDisposition.APPLIED
                if status is feed_store.FeedStatus.DEACTIVATED:
                    expected = ingestion_lease_store.ChildDisposition.APPLIED_AFTER_DEACTIVATION
                self.assertIs(plan.disposition, expected)
                self.assertIs(
                    plan.lifecycle_effect,
                    ingestion_lease_store.LifecycleEffect.NONE,
                )
                self.assertFalse(plan.clear_lifecycle)
                self.assertIsNone(plan.audit_action)

    async def test_closed_cohort_preserves_dirty_lifecycle_and_order(
        self,
    ) -> None:
        first_id = uuid.UUID("ffffffff-0000-0000-0000-000000000143")
        second_id = uuid.UUID("11111111-0000-0000-0000-000000000144")
        third_id = uuid.UUID("22222222-0000-0000-0000-000000000145")
        earlier = _NOW - datetime.timedelta(seconds=1)
        feed_ids = (first_id, second_id, third_id)
        paths = tuple(f"gs://bucket/{index}.flac" for index in range(3))
        pool = connection_util.make_mock_pool(transaction=True)
        connection = pool.acquired_connection
        connection.fetchrow.return_value = _lease_row()
        connection.fetch.side_effect = [
            [
                _child_row(
                    second_id,
                    status="failing",
                    last_bookmark_time=_NOW,
                    failure_count=3,
                    retry_after=_NOW,
                    status_reason="source_unreachable",
                    status_reason_detail="dirty failing",
                    audit_revision=8,
                ),
                _child_row(
                    third_id,
                    status="deactivated",
                    last_bookmark_time=_NOW,
                    failure_count=5,
                    retry_after=_NOW,
                    status_reason="system_pipeline_error",
                    status_reason_detail="dirty deactivated",
                    audit_revision=11,
                ),
                _child_row(
                    first_id,
                    status="active",
                    last_bookmark_time=_NOW,
                    failure_count=2,
                    retry_after=_NOW,
                    status_reason="source_unreachable",
                    status_reason_detail="dirty active",
                    audit_revision=6,
                ),
            ],
            [
                {
                    "caller_ordinal": 2,
                    "id": third_id,
                    "last_processed_filename": paths[2],
                    "last_bookmark_time": _NOW,
                },
                {
                    "caller_ordinal": 0,
                    "id": first_id,
                    "last_processed_filename": paths[0],
                    "last_bookmark_time": _NOW,
                },
                {
                    "caller_ordinal": 1,
                    "id": second_id,
                    "last_processed_filename": paths[1],
                    "last_bookmark_time": _NOW,
                },
            ],
        ]
        store = ingestion_lease_store.IngestionLeaseStore(pool)
        mutations = tuple(
            self._closed_cohort(
                feed_id,
                cursor=_NOW if index == 0 else earlier,
                path=paths[index],
            )
            for index, feed_id in enumerate(feed_ids)
        )

        with mock.patch(
            "backend.pipeline.storage.ingestion_lease_store."
            "feed_change_notifications.emit_feed_change_notification"
        ) as emit:
            result = await store.commit_child_mutations(
                _grant(),
                ingestion_lease_store.ChildMutationBatch(
                    mutations,
                    ingestion_lease_store.NoLeaseEffect(),
                ),
                actor_id="service_account:gcp:collector",
            )

        assert isinstance(result, ingestion_lease_store.BatchCommitted)
        self.assertEqual(
            tuple(child.feed_id for child in result.children),
            feed_ids,
        )
        self.assertEqual(
            tuple(child.lifecycle_effect for child in result.children),
            (ingestion_lease_store.LifecycleEffect.NONE,) * 3,
        )
        self.assertIs(
            result.children[2].disposition,
            ingestion_lease_store.ChildDisposition.APPLIED_AFTER_DEACTIVATION,
        )
        self.assertEqual(connection.fetch.await_count, 2)
        lock_args = connection.fetch.await_args_list[0].args
        self.assertEqual(lock_args[1], [second_id, third_id, first_id])
        neutral_args = connection.fetch.await_args_list[1].args
        self.assertIs(
            neutral_args[0],
            ingestion_lease_queries.APPLY_CLOSED_COHORT_PROGRESS_SQL,
        )
        self.assertEqual(neutral_args[1], list(feed_ids))
        self.assertEqual(neutral_args[2], list(paths))
        self.assertEqual(neutral_args[4], [False, False, False])
        self.assertEqual(neutral_args[5], [True, True, True])
        emit.assert_not_called()

    async def test_closed_cohort_stale_fence_reads_no_child(self) -> None:
        feed_id = uuid.UUID("aaaaaaaa-0000-0000-0000-000000000146")
        pool = connection_util.make_mock_pool(transaction=True)
        connection = pool.acquired_connection
        connection.fetchrow.return_value = _lease_row(fencing_token=8)
        store = ingestion_lease_store.IngestionLeaseStore(pool)

        result = await store.commit_child_mutations(
            _grant(),
            ingestion_lease_store.ChildMutationBatch(
                (self._closed_cohort(feed_id),),
                ingestion_lease_store.NoLeaseEffect(),
            ),
            actor_id="service_account:gcp:collector",
        )

        assert isinstance(result, ingestion_lease_store.GrantRejected)
        self.assertIs(
            result.reason,
            ingestion_lease_store.GrantRejectionReason.FENCE_MISMATCH,
        )
        connection.fetch.assert_not_awaited()

    async def test_closed_cohort_missing_and_ineligible_are_selective(
        self,
    ) -> None:
        missing_id = uuid.UUID("aaaaaaaa-0000-0000-0000-000000000147")
        quarantined_id = uuid.UUID("aaaaaaaa-0000-0000-0000-000000000148")
        pool = connection_util.make_mock_pool(transaction=True)
        connection = pool.acquired_connection
        connection.fetchrow.return_value = _lease_row()
        connection.fetch.return_value = [
            _child_row(quarantined_id, status="quarantined")
        ]
        store = ingestion_lease_store.IngestionLeaseStore(pool)

        result = await store.commit_child_mutations(
            _grant(),
            ingestion_lease_store.ChildMutationBatch(
                (
                    self._closed_cohort(missing_id),
                    self._closed_cohort(quarantined_id),
                ),
                ingestion_lease_store.NoLeaseEffect(),
            ),
            actor_id="service_account:gcp:collector",
        )

        assert isinstance(result, ingestion_lease_store.BatchCommitted)
        self.assertEqual(
            tuple(child.disposition for child in result.children),
            (
                ingestion_lease_store.ChildDisposition.MISSING,
                ingestion_lease_store.ChildDisposition.STATUS_INELIGIBLE,
            ),
        )
        self.assertEqual(connection.fetch.await_count, 1)

    async def test_closed_cohort_exact_noop_runs_no_dml(self) -> None:
        feed_id = uuid.UUID("aaaaaaaa-0000-0000-0000-000000000149")
        path = "gs://bucket/cohort.flac"
        pool = connection_util.make_mock_pool(transaction=True)
        connection = pool.acquired_connection
        connection.fetchrow.return_value = _lease_row()
        connection.fetch.return_value = [
            _child_row(
                feed_id,
                last_processed_filename=path,
                last_bookmark_time=_NOW,
            )
        ]
        store = ingestion_lease_store.IngestionLeaseStore(pool)

        result = await store.commit_child_mutations(
            _grant(),
            ingestion_lease_store.ChildMutationBatch(
                (self._closed_cohort(feed_id, path=path),),
                ingestion_lease_store.NoLeaseEffect(),
            ),
            actor_id="service_account:gcp:collector",
        )

        assert isinstance(result, ingestion_lease_store.BatchCommitted)
        self.assertIs(
            result.children[0].disposition,
            ingestion_lease_store.ChildDisposition.ACCEPTED_NOOP,
        )
        self.assertIs(
            result.children[0].cursor_effect,
            ingestion_lease_store.CursorEffect.EQUAL,
        )
        self.assertIs(
            result.children[0].lifecycle_effect,
            ingestion_lease_store.LifecycleEffect.NONE,
        )
        connection.fetch.assert_awaited_once()

    async def test_closed_cohort_database_error_rolls_back_once(self) -> None:
        feed_id = uuid.UUID("aaaaaaaa-0000-0000-0000-000000000150")
        pool = connection_util.make_mock_pool(transaction=True)
        connection = pool.acquired_connection
        connection.fetchrow.return_value = _lease_row()
        connection.fetch.side_effect = [
            [_child_row(feed_id)],
            RuntimeError("neutral progress failed"),
        ]
        store = ingestion_lease_store.IngestionLeaseStore(pool)

        with mock.patch(
            "backend.pipeline.storage.ingestion_lease_store."
            "feed_change_notifications.emit_feed_change_notification"
        ) as emit:
            with self.assertRaisesRegex(
                RuntimeError,
                "neutral progress failed",
            ):
                await store.commit_child_mutations(
                    _grant(),
                    ingestion_lease_store.ChildMutationBatch(
                        (self._closed_cohort(feed_id),),
                        ingestion_lease_store.NoLeaseEffect(),
                    ),
                    actor_id="service_account:gcp:collector",
                )

        self.assertEqual(connection.fetch.await_count, 2)
        exit_args = pool.transaction_context.__aexit__.await_args.args
        self.assertIs(exit_args[0], RuntimeError)
        emit.assert_not_called()

    async def test_source_observation_none_clears_lifecycle_without_cursor(
        self,
    ) -> None:
        feed_id = uuid.UUID("aaaaaaaa-0000-0000-0000-000000000151")
        payload_row = _child_audit_payload(feed_id, caller_ordinal=0)
        pool = connection_util.make_mock_pool(transaction=True)
        connection = pool.acquired_connection
        connection.fetchrow.return_value = _lease_row()
        connection.fetch.side_effect = [
            [
                _child_row(
                    feed_id,
                    status="failing",
                    last_bookmark_time=_NOW,
                    failure_count=2,
                    retry_after=_NOW,
                    status_reason="source_unreachable",
                    status_reason_detail="stale failure",
                    audit_revision=4,
                )
            ],
            [
                _child_row(
                    feed_id,
                    caller_ordinal=0,
                    status="active",
                    last_bookmark_time=_NOW,
                    failure_count=0,
                    retry_after=None,
                    status_reason=None,
                    status_reason_detail=None,
                    audit_revision=5,
                )
            ],
            [_audit_property_row(feed_id)],
            [payload_row],
        ]
        store = ingestion_lease_store.IngestionLeaseStore(pool)

        result = await store.commit_child_mutations(
            _grant(),
            ingestion_lease_store.ChildMutationBatch(
                (
                    ingestion_lease_store.SourceObservation(
                        _member_identity(feed_id),
                        None,
                    ),
                ),
                ingestion_lease_store.NoLeaseEffect(),
            ),
            actor_id="service_account:gcp:collector",
        )

        assert isinstance(result, ingestion_lease_store.BatchCommitted)
        self.assertIs(
            result.children[0].cursor_effect,
            ingestion_lease_store.CursorEffect.ABSENT,
        )
        self.assertIs(
            result.children[0].lifecycle_effect,
            ingestion_lease_store.LifecycleEffect.RECOVERED,
        )
        observation_args = connection.fetch.await_args_list[1].args
        self.assertIs(
            observation_args[0],
            ingestion_lease_queries.APPLY_SOURCE_OBSERVATIONS_SQL,
        )
        self.assertEqual(observation_args[2], [None])
        self.assertEqual(observation_args[3], [False])
        self.assertEqual(observation_args[4], [True])

    async def test_closed_cohort_fanout_is_constant_for_one_and_hundred(
        self,
    ) -> None:
        observed_fetch_counts = []
        for feed_count in (1, 100):
            feed_ids = [
                uuid.UUID(int=1000 + index) for index in range(feed_count)
            ]
            pool = connection_util.make_mock_pool(transaction=True)
            connection = pool.acquired_connection
            connection.fetchrow.return_value = _lease_row()
            connection.fetch.side_effect = [
                [_child_row(feed_id) for feed_id in reversed(feed_ids)],
                [
                    {
                        "caller_ordinal": index,
                        "id": feed_id,
                        "last_processed_filename": None,
                        "last_bookmark_time": _NOW,
                    }
                    for index, feed_id in enumerate(feed_ids)
                ],
            ]
            store = ingestion_lease_store.IngestionLeaseStore(pool)

            result = await store.commit_child_mutations(
                _grant(),
                ingestion_lease_store.ChildMutationBatch(
                    tuple(
                        self._closed_cohort(
                            feed_id,
                            cursor=_NOW,
                            path=None,
                        )
                        for feed_id in feed_ids
                    ),
                    ingestion_lease_store.NoLeaseEffect(),
                ),
                actor_id="service_account:gcp:collector",
            )

            assert isinstance(result, ingestion_lease_store.BatchCommitted)
            self.assertEqual(len(result.children), feed_count)
            observed_fetch_counts.append(connection.fetch.await_count)

        self.assertEqual(observed_fetch_counts, [2, 2])

    async def test_empty_batch_still_locks_and_validates_grant(self) -> None:
        pool = connection_util.make_mock_pool(transaction=True)
        connection = pool.acquired_connection
        connection.fetchrow.return_value = _lease_row()
        store = ingestion_lease_store.IngestionLeaseStore(pool)

        result = await store.commit_child_mutations(
            _grant(),
            ingestion_lease_store.ChildMutationBatch(
                (),
                ingestion_lease_store.NoLeaseEffect(),
            ),
            actor_id="service_account:gcp:collector",
        )

        assert isinstance(result, ingestion_lease_store.BatchCommitted)
        self.assertEqual(result.children, ())
        connection.fetchrow.assert_awaited_once_with(
            ingestion_lease_queries.LOCK_LEASE_SQL,
            "bcfy_calls",
            "123",
        )
        connection.fetch.assert_not_awaited()
        connection.transaction.assert_called_once_with(
            isolation="read_committed"
        )

    async def test_rejected_grant_executes_no_child_or_audit_query(
        self,
    ) -> None:
        cases = (
            (
                None,
                ingestion_lease_store.GrantRejectionReason.MISSING,
            ),
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
        feed_id = uuid.UUID("aaaaaaaa-0000-0000-0000-000000000002")

        for lease_row, reason in cases:
            with self.subTest(reason=reason.value):
                pool = connection_util.make_mock_pool(transaction=True)
                connection = pool.acquired_connection
                connection.fetchrow.return_value = lease_row
                store = ingestion_lease_store.IngestionLeaseStore(pool)

                result = await store.commit_child_mutations(
                    _grant(),
                    ingestion_lease_store.ChildMutationBatch(
                        (self._progress(feed_id),),
                        ingestion_lease_store.NoLeaseEffect(),
                    ),
                    actor_id="service_account:gcp:collector",
                )

                assert isinstance(
                    result,
                    ingestion_lease_store.GrantRejected,
                )
                self.assertIs(result.reason, reason)
                connection.fetch.assert_not_awaited()

    async def test_sorted_lock_and_scrambled_dml_return_caller_order(
        self,
    ) -> None:
        first_id = uuid.UUID("ffffffff-0000-0000-0000-000000000001")
        second_id = uuid.UUID("11111111-0000-0000-0000-000000000002")
        pool = connection_util.make_mock_pool(transaction=True)
        connection = pool.acquired_connection
        connection.fetchrow.return_value = _lease_row()
        connection.fetch.side_effect = [
            [_child_row(first_id), _child_row(second_id)],
            [
                _child_row(
                    second_id,
                    caller_ordinal=1,
                    last_processed_filename="gs://bucket/second.flac",
                    last_bookmark_time=_NOW,
                ),
                _child_row(
                    first_id,
                    caller_ordinal=0,
                    last_processed_filename="gs://bucket/first.flac",
                    last_bookmark_time=_NOW,
                ),
            ],
        ]
        store = ingestion_lease_store.IngestionLeaseStore(pool)
        batch = ingestion_lease_store.ChildMutationBatch(
            (
                self._progress(first_id, path="gs://bucket/first.flac"),
                self._progress(second_id, path="gs://bucket/second.flac"),
            ),
            ingestion_lease_store.NoLeaseEffect(),
        )

        result = await store.commit_child_mutations(
            _grant(),
            batch,
            actor_id="service_account:gcp:collector",
        )

        assert isinstance(result, ingestion_lease_store.BatchCommitted)
        self.assertEqual(
            [child.feed_id for child in result.children],
            [first_id, second_id],
        )
        lock_args = connection.fetch.await_args_list[0].args
        self.assertIs(
            lock_args[0], ingestion_lease_queries.LOCK_CHILD_FEEDS_SQL
        )
        self.assertEqual(lock_args[1], [second_id, first_id])
        self.assertTrue(
            all(
                child.disposition
                is ingestion_lease_store.ChildDisposition.APPLIED
                for child in result.children
            )
        )

    async def test_expected_missing_and_status_races_are_selective(
        self,
    ) -> None:
        valid_id = uuid.UUID("11111111-0000-0000-0000-000000000010")
        missing_id = uuid.UUID("22222222-0000-0000-0000-000000000020")
        quarantined_id = uuid.UUID("33333333-0000-0000-0000-000000000030")
        pool = connection_util.make_mock_pool(transaction=True)
        connection = pool.acquired_connection
        connection.fetchrow.return_value = _lease_row()
        connection.fetch.side_effect = [
            [
                _child_row(valid_id),
                _child_row(quarantined_id, status="quarantined"),
            ],
            [
                _child_row(
                    valid_id,
                    caller_ordinal=0,
                    last_bookmark_time=_NOW,
                    last_processed_filename="gs://bucket/audio.flac",
                )
            ],
        ]
        store = ingestion_lease_store.IngestionLeaseStore(pool)
        batch = ingestion_lease_store.ChildMutationBatch(
            (
                self._progress(valid_id),
                ingestion_lease_store.SourceObservation(
                    _member_identity(missing_id),
                    _NOW,
                ),
                ingestion_lease_store.SourceObservation(
                    _member_identity(quarantined_id),
                    _NOW,
                ),
            ),
            ingestion_lease_store.NoLeaseEffect(),
        )

        result = await store.commit_child_mutations(
            _grant(),
            batch,
            actor_id="service_account:gcp:collector",
        )

        assert isinstance(result, ingestion_lease_store.BatchCommitted)
        self.assertEqual(
            [child.disposition for child in result.children],
            [
                ingestion_lease_store.ChildDisposition.APPLIED,
                ingestion_lease_store.ChildDisposition.MISSING,
                ingestion_lease_store.ChildDisposition.STATUS_INELIGIBLE,
            ],
        )
        self.assertEqual(connection.fetch.await_count, 2)

    async def test_deactivated_progress_clears_without_audit(self) -> None:
        feed_id = uuid.UUID("44444444-0000-0000-0000-000000000040")
        pool = connection_util.make_mock_pool(transaction=True)
        connection = pool.acquired_connection
        connection.fetchrow.return_value = _lease_row()
        connection.fetch.side_effect = [
            [
                _child_row(
                    feed_id,
                    status="deactivated",
                    failure_count=2,
                    status_reason="source_unreachable",
                )
            ],
            [
                _child_row(
                    feed_id,
                    caller_ordinal=0,
                    status="deactivated",
                    last_bookmark_time=_NOW,
                    last_processed_filename="gs://bucket/audio.flac",
                    audit_revision=1,
                )
            ],
        ]
        store = ingestion_lease_store.IngestionLeaseStore(pool)

        result = await store.commit_child_mutations(
            _grant(),
            ingestion_lease_store.ChildMutationBatch(
                (self._progress(feed_id),),
                ingestion_lease_store.NoLeaseEffect(),
            ),
            actor_id="service_account:gcp:collector",
        )

        assert isinstance(result, ingestion_lease_store.BatchCommitted)
        child = result.children[0]
        self.assertIs(
            child.disposition,
            ingestion_lease_store.ChildDisposition.APPLIED_AFTER_DEACTIVATION,
        )
        self.assertIs(
            child.lifecycle_effect,
            ingestion_lease_store.LifecycleEffect.CLEARED_WHILE_DEACTIVATED,
        )
        self.assertEqual(connection.fetch.await_count, 2)

    async def test_recovery_audit_notifies_after_transaction_and_checkout(
        self,
    ) -> None:
        feed_id = uuid.UUID("55555555-0000-0000-0000-000000000050")
        payload_row = _child_audit_payload(feed_id, caller_ordinal=0)
        pool = connection_util.make_mock_pool(transaction=True)
        connection = pool.acquired_connection
        connection.fetchrow.return_value = _lease_row()
        connection.fetch.side_effect = [
            [
                _child_row(
                    feed_id,
                    status="failing",
                    failure_count=2,
                    status_reason="source_unreachable",
                )
            ],
            [
                _child_row(
                    feed_id,
                    caller_ordinal=0,
                    status="active",
                    last_bookmark_time=_NOW,
                    last_processed_filename="gs://bucket/audio.flac",
                    audit_revision=1,
                )
            ],
            [_audit_property_row(feed_id)],
            [payload_row],
        ]
        store = ingestion_lease_store.IngestionLeaseStore(pool)

        with mock.patch(
            "backend.pipeline.storage.ingestion_lease_store."
            "feed_change_notifications.emit_feed_change_notification"
        ) as emit:
            emit.side_effect = lambda _payload: (
                pool.transaction_context.__aexit__.assert_awaited_once(),
                pool.acquire_context.__aexit__.assert_awaited_once(),
            )
            result = await store.commit_child_mutations(
                _grant(),
                ingestion_lease_store.ChildMutationBatch(
                    (self._progress(feed_id),),
                    ingestion_lease_store.NoLeaseEffect(),
                ),
                actor_id="service_account:gcp:collector",
            )

        assert isinstance(result, ingestion_lease_store.BatchCommitted)
        self.assertIs(
            result.children[0].lifecycle_effect,
            ingestion_lease_store.LifecycleEffect.RECOVERED,
        )
        emit.assert_called_once_with(payload_row["feed_audit_event"])
        self.assertIs(
            connection.fetch.await_args_list[2].args[0],
            ingestion_lease_queries.LOAD_CHILD_AUDIT_PROPERTIES_SQL,
        )
        self.assertIs(
            connection.fetch.await_args_list[3].args[0],
            ingestion_lease_queries.INSERT_CHILD_AUDIT_EVENTS_SQL,
        )

    async def test_database_error_and_cancellation_escape_without_notification(
        self,
    ) -> None:
        feed_id = uuid.UUID("66666666-0000-0000-0000-000000000060")
        for error in (
            RuntimeError("database failed"),
            asyncio.CancelledError(),
        ):
            with self.subTest(error=type(error).__name__):
                pool = connection_util.make_mock_pool(transaction=True)
                connection = pool.acquired_connection
                connection.fetchrow.return_value = _lease_row()
                connection.fetch.side_effect = [
                    [_child_row(feed_id)],
                    error,
                ]
                store = ingestion_lease_store.IngestionLeaseStore(pool)

                with mock.patch(
                    "backend.pipeline.storage.ingestion_lease_store."
                    "feed_change_notifications."
                    "emit_feed_change_notification"
                ) as emit:
                    with self.assertRaises(type(error)):
                        await store.commit_child_mutations(
                            _grant(),
                            ingestion_lease_store.ChildMutationBatch(
                                (self._progress(feed_id),),
                                ingestion_lease_store.NoLeaseEffect(),
                            ),
                            actor_id="service_account:gcp:collector",
                        )

                emit.assert_not_called()
                self.assertEqual(connection.fetch.await_count, 2)
                exit_args = pool.transaction_context.__aexit__.await_args.args
                self.assertIs(exit_args[0], type(error))

    async def test_on_cursor_advance_absent_equal_cursor_and_regressive_noop(
        self,
    ) -> None:
        feed_id = uuid.UUID("88888888-0000-0000-0000-000000000080")
        for cursor, expected_effect in (
            (None, ingestion_lease_store.CursorEffect.ABSENT),
            (_NOW, ingestion_lease_store.CursorEffect.EQUAL),
            (
                _NOW - datetime.timedelta(seconds=1),
                ingestion_lease_store.CursorEffect.REGRESSIVE,
            ),
        ):
            with self.subTest(cursor_effect=expected_effect.value):
                pool = connection_util.make_mock_pool(transaction=True)
                connection = pool.acquired_connection
                connection.fetchrow.return_value = _lease_row()
                connection.fetch.return_value = [
                    _child_row(feed_id, last_bookmark_time=_NOW)
                ]
                store = ingestion_lease_store.IngestionLeaseStore(pool)

                result = await store.commit_child_mutations(
                    _grant(),
                    ingestion_lease_store.ChildMutationBatch(
                        (self._failure(feed_id, cursor=cursor),),
                        ingestion_lease_store.NoLeaseEffect(),
                    ),
                    actor_id="service_account:gcp:collector",
                )

                assert isinstance(result, ingestion_lease_store.BatchCommitted)
                child = result.children[0]
                self.assertIs(child.cursor_effect, expected_effect)
                self.assertIs(
                    child.disposition,
                    ingestion_lease_store.ChildDisposition.ACCEPTED_NOOP,
                )
                self.assertIs(
                    child.lifecycle_effect,
                    ingestion_lease_store.LifecycleEffect.NONE,
                )
                connection.fetch.assert_awaited_once()

    def test_one_shot_charge_is_independent_from_cursor_effect(self) -> None:
        feed_id = uuid.UUID("99999999-0000-0000-0000-000000000089")
        earlier = _NOW - datetime.timedelta(seconds=1)
        later = _NOW + datetime.timedelta(seconds=1)
        cases = (
            (None, None, ingestion_lease_store.CursorEffect.ABSENT, False),
            (_NOW, _NOW, ingestion_lease_store.CursorEffect.EQUAL, False),
            (
                _NOW,
                earlier,
                ingestion_lease_store.CursorEffect.REGRESSIVE,
                False,
            ),
            (
                None,
                _NOW,
                ingestion_lease_store.CursorEffect.INITIALIZED,
                True,
            ),
            (
                _NOW,
                later,
                ingestion_lease_store.CursorEffect.ADVANCED,
                True,
            ),
        )

        for current, requested, expected_effect, write_cursor in cases:
            with self.subTest(cursor_effect=expected_effect.value):
                plan = ingestion_lease_store._plan_child_mutation(
                    0,
                    self._failure(
                        feed_id,
                        cursor=requested,
                        charge_mode=(
                            ingestion_lease_store.FeedFailureChargeMode.ONE_SHOT
                        ),
                    ),
                    _child_row(feed_id, last_bookmark_time=current),
                )

                self.assertIs(plan.cursor_effect, expected_effect)
                self.assertTrue(plan.charge_failure)
                self.assertEqual(plan.write_cursor, write_cursor)
                self.assertIs(
                    plan.disposition,
                    ingestion_lease_store.ChildDisposition.APPLIED,
                )
                self.assertIs(
                    plan.lifecycle_effect,
                    ingestion_lease_store.LifecycleEffect.FAILURE_RECORDED,
                )

    async def test_explicit_second_one_shot_invocation_charges_again(
        self,
    ) -> None:
        feed_id = uuid.UUID("99999999-0000-0000-0000-000000000090")
        pool = connection_util.make_mock_pool(transaction=True)
        connection = pool.acquired_connection
        connection.fetchrow.return_value = _lease_row()
        connection.fetch.side_effect = [
            [
                _child_row(
                    feed_id,
                    status="failing",
                    failure_count=1,
                    status_reason="system_configuration_invalid",
                )
            ],
            [
                _child_row(
                    feed_id,
                    caller_ordinal=0,
                    status="failing",
                    failure_count=2,
                    status_reason="system_configuration_invalid",
                    audit_revision=1,
                )
            ],
            [
                _child_row(
                    feed_id,
                    status="failing",
                    failure_count=2,
                    status_reason="system_configuration_invalid",
                    audit_revision=1,
                )
            ],
            [
                _child_row(
                    feed_id,
                    caller_ordinal=0,
                    status="failing",
                    failure_count=3,
                    status_reason="system_configuration_invalid",
                    audit_revision=2,
                )
            ],
        ]
        store = ingestion_lease_store.IngestionLeaseStore(pool)
        batch = ingestion_lease_store.ChildMutationBatch(
            (
                self._failure(
                    feed_id,
                    cursor=None,
                    charge_mode=(
                        ingestion_lease_store.FeedFailureChargeMode.ONE_SHOT
                    ),
                ),
            ),
            ingestion_lease_store.NoLeaseEffect(),
        )
        first = await store.commit_child_mutations(
            _grant(), batch, actor_id="service_account:gcp:collector"
        )
        second = await store.commit_child_mutations(
            _grant(), batch, actor_id="service_account:gcp:collector"
        )

        assert isinstance(first, ingestion_lease_store.BatchCommitted)
        assert isinstance(second, ingestion_lease_store.BatchCommitted)
        self.assertEqual(
            tuple(
                result.children[0].lifecycle_effect
                for result in (first, second)
            ),
            (ingestion_lease_store.LifecycleEffect.FAILURE_RECORDED,) * 2,
        )
        self.assertEqual(connection.fetch.await_count, 4)
        for call_index in (1, 3):
            failure_args = connection.fetch.await_args_list[call_index].args
            self.assertIs(
                failure_args[0],
                ingestion_lease_queries.APPLY_FEED_FAILURES_SQL,
            )
            self.assertEqual(failure_args[3], [True])
            self.assertEqual(failure_args[4], [False])

    async def test_budgeted_failure_quarantines_and_audits_atomically(
        self,
    ) -> None:
        feed_id = uuid.UUID("aaaaaaaa-0000-0000-0000-000000000100")
        payload_row = _child_audit_payload(feed_id, caller_ordinal=0)
        pool = connection_util.make_mock_pool(transaction=True)
        connection = pool.acquired_connection
        connection.fetchrow.return_value = _lease_row()
        connection.fetch.side_effect = [
            [
                _child_row(
                    feed_id,
                    status="failing",
                    failure_count=4,
                    status_reason="system_configuration_invalid",
                )
            ],
            [
                _child_row(
                    feed_id,
                    caller_ordinal=0,
                    status="quarantined",
                    failure_count=5,
                    status_reason="system_configuration_invalid",
                    last_bookmark_time=_NOW,
                    audit_revision=1,
                )
            ],
            [_audit_property_row(feed_id)],
            [payload_row],
        ]
        store = ingestion_lease_store.IngestionLeaseStore(pool)

        result = await store.commit_child_mutations(
            _grant(),
            ingestion_lease_store.ChildMutationBatch(
                (self._failure(feed_id),),
                ingestion_lease_store.NoLeaseEffect(),
            ),
            actor_id="service_account:gcp:collector",
        )

        assert isinstance(result, ingestion_lease_store.BatchCommitted)
        self.assertIs(
            result.children[0].lifecycle_effect,
            ingestion_lease_store.LifecycleEffect.QUARANTINED,
        )
        self.assertEqual(connection.fetch.await_count, 4)

    async def test_non_budgeted_failure_resets_and_cannot_quarantine(
        self,
    ) -> None:
        feed_id = uuid.UUID("bbbbbbbb-0000-0000-0000-000000000110")
        retry_after = _NOW + datetime.timedelta(minutes=8)
        action = ingestion_lease_store.NonBudgetedFailure(retry_after)
        prior_cursor = _NOW - datetime.timedelta(seconds=1)
        pool = connection_util.make_mock_pool(transaction=True)
        connection = pool.acquired_connection
        connection.fetchrow.return_value = _lease_row()
        connection.fetch.side_effect = [
            [
                _child_row(
                    feed_id,
                    status="failing",
                    failure_count=4,
                    status_reason="system_pipeline_error",
                    last_bookmark_time=prior_cursor,
                )
            ],
            [
                _child_row(
                    feed_id,
                    caller_ordinal=0,
                    status="failing",
                    failure_count=0,
                    retry_after=retry_after,
                    status_reason="system_pipeline_error",
                    last_bookmark_time=_NOW,
                    audit_revision=1,
                )
            ],
        ]
        store = ingestion_lease_store.IngestionLeaseStore(pool)

        result = await store.commit_child_mutations(
            _grant(),
            ingestion_lease_store.ChildMutationBatch(
                (
                    self._failure(
                        feed_id,
                        action=action,
                        status_reason=(
                            feed_store.FeedStatusReason.SYSTEM_PIPELINE_ERROR
                        ),
                    ),
                ),
                ingestion_lease_store.NoLeaseEffect(),
            ),
            actor_id="service_account:gcp:collector",
        )

        assert isinstance(result, ingestion_lease_store.BatchCommitted)
        self.assertIs(
            result.children[0].lifecycle_effect,
            ingestion_lease_store.LifecycleEffect.FAILURE_RECORDED,
        )
        failure_args = connection.fetch.await_args_list[1].args
        self.assertEqual(failure_args[5], [False])
        self.assertEqual(failure_args[9], [retry_after])

    async def test_all_non_budgeted_reasons_reset_at_quarantine_threshold(
        self,
    ) -> None:
        retry_after = _NOW + datetime.timedelta(minutes=11)
        non_budgeted_reasons = tuple(
            reason
            for reason in feed_store.FeedStatusReason
            if failure_policy.classify_failure_policy(reason)
            is not failure_policy.ExecutedAction.INCREMENT_FEED_FAILURE_BUDGET
        )
        self.assertEqual(
            set(feed_store.FeedStatusReason) - set(non_budgeted_reasons),
            {
                feed_store.FeedStatusReason.SYSTEM_CONFIGURATION_INVALID,
                (
                    feed_store.FeedStatusReason.SYSTEM_RUNTIME_CONFIGURATION_INVALID
                ),
            },
        )

        for reason_index, status_reason in enumerate(non_budgeted_reasons):
            with self.subTest(status_reason=status_reason.value):
                feed_id = uuid.UUID(int=500 + reason_index)
                pool = connection_util.make_mock_pool(transaction=True)
                connection = pool.acquired_connection
                connection.fetchrow.return_value = _lease_row()
                connection.fetch.side_effect = [
                    [
                        _child_row(
                            feed_id,
                            status="failing",
                            failure_count=5,
                            status_reason=status_reason.value,
                            last_bookmark_time=_NOW,
                        )
                    ],
                    [
                        _child_row(
                            feed_id,
                            caller_ordinal=0,
                            status="failing",
                            failure_count=0,
                            retry_after=retry_after,
                            status_reason=status_reason.value,
                            last_bookmark_time=_NOW,
                            audit_revision=1,
                        )
                    ],
                ]
                store = ingestion_lease_store.IngestionLeaseStore(pool)

                result = await store.commit_child_mutations(
                    _grant(),
                    ingestion_lease_store.ChildMutationBatch(
                        (
                            self._failure(
                                feed_id,
                                cursor=_NOW,
                                action=ingestion_lease_store.NonBudgetedFailure(
                                    retry_after
                                ),
                                charge_mode=(
                                    ingestion_lease_store.FeedFailureChargeMode.ONE_SHOT
                                ),
                                status_reason=status_reason,
                            ),
                        ),
                        ingestion_lease_store.NoLeaseEffect(),
                    ),
                    actor_id="service_account:gcp:collector",
                )

                assert isinstance(
                    result,
                    ingestion_lease_store.BatchCommitted,
                )
                self.assertIs(
                    result.children[0].lifecycle_effect,
                    ingestion_lease_store.LifecycleEffect.FAILURE_RECORDED,
                )
                self.assertIsNot(
                    result.children[0].lifecycle_effect,
                    ingestion_lease_store.LifecycleEffect.QUARANTINED,
                )
                failure_args = connection.fetch.await_args_list[1].args
                self.assertEqual(failure_args[3], [True])
                self.assertEqual(failure_args[4], [False])
                self.assertEqual(failure_args[5], [False])
                self.assertEqual(failure_args[9], [retry_after])
                self.assertEqual(failure_args[10], [status_reason.value])

    async def test_one_shot_failure_and_neutral_closure_finalize_independently(
        self,
    ) -> None:
        closed_feed_id = uuid.UUID("bbbbbbbb-0000-0000-0000-000000000111")
        failed_feed_id = uuid.UUID("bbbbbbbb-0000-0000-0000-000000000112")
        before_lease = _lease_row(failure_count=2)
        after_lease = _lease_row(
            failure_count=0,
            retry_after=None,
            status_reason=None,
            status_reason_detail=None,
        )
        pool = connection_util.make_mock_pool(transaction=True)
        connection = pool.acquired_connection
        connection.fetchrow.side_effect = [before_lease, after_lease]
        connection.fetch.side_effect = [
            [
                _child_row(
                    failed_feed_id,
                    status="failing",
                    failure_count=1,
                    status_reason="system_configuration_invalid",
                    last_bookmark_time=_NOW,
                ),
                _child_row(
                    closed_feed_id,
                    last_bookmark_time=_NOW - datetime.timedelta(seconds=1),
                ),
            ],
            [
                {
                    "caller_ordinal": 0,
                    "id": closed_feed_id,
                    "last_processed_filename": "gs://bucket/cohort.flac",
                    "last_bookmark_time": _NOW,
                }
            ],
            [
                _child_row(
                    failed_feed_id,
                    caller_ordinal=1,
                    status="failing",
                    failure_count=2,
                    status_reason="system_configuration_invalid",
                    status_reason_detail="boundary failed",
                    last_bookmark_time=_NOW,
                    audit_revision=1,
                )
            ],
        ]
        store = ingestion_lease_store.IngestionLeaseStore(pool)

        result = await store.commit_child_mutations(
            _grant(),
            ingestion_lease_store.ChildMutationBatch(
                (
                    self._closed_cohort(closed_feed_id),
                    self._failure(
                        failed_feed_id,
                        cursor=_NOW,
                        charge_mode=(
                            ingestion_lease_store.FeedFailureChargeMode.ONE_SHOT
                        ),
                    ),
                ),
                ingestion_lease_store.FinalizeLeaseRecovery(),
            ),
            actor_id="service_account:gcp:collector",
        )

        assert isinstance(result, ingestion_lease_store.BatchCommitted)
        self.assertEqual(
            tuple(child.lifecycle_effect for child in result.children),
            (
                ingestion_lease_store.LifecycleEffect.NONE,
                ingestion_lease_store.LifecycleEffect.FAILURE_RECORDED,
            ),
        )
        self.assertIs(
            result.lease_effect.effect,
            ingestion_lease_store.LeaseLifecycleEffect.RECOVERED,
        )
        self.assertEqual(connection.fetch.await_count, 3)
        self.assertIs(
            connection.fetch.await_args_list[1].args[0],
            ingestion_lease_queries.APPLY_CLOSED_COHORT_PROGRESS_SQL,
        )
        failure_args = connection.fetch.await_args_list[2].args
        self.assertIs(
            failure_args[0],
            ingestion_lease_queries.APPLY_FEED_FAILURES_SQL,
        )
        self.assertEqual(failure_args[3], [True])
        self.assertEqual(failure_args[4], [False])
        self.assertIs(
            connection.fetchrow.await_args_list[1].args[0],
            ingestion_lease_queries.FINALIZE_LEASE_RECOVERY_SQL,
        )

    async def test_failure_rejects_ineligible_status_without_evidence(
        self,
    ) -> None:
        feed_id = uuid.UUID("cccccccc-0000-0000-0000-000000000120")
        for status in ("deactivated", "quarantined", "unclaimed"):
            with self.subTest(status=status):
                pool = connection_util.make_mock_pool(transaction=True)
                connection = pool.acquired_connection
                connection.fetchrow.return_value = _lease_row()
                connection.fetch.return_value = [
                    _child_row(feed_id, status=status)
                ]
                store = ingestion_lease_store.IngestionLeaseStore(pool)

                result = await store.commit_child_mutations(
                    _grant(),
                    ingestion_lease_store.ChildMutationBatch(
                        (self._failure(feed_id),),
                        ingestion_lease_store.NoLeaseEffect(),
                    ),
                    actor_id="service_account:gcp:collector",
                )

                assert isinstance(result, ingestion_lease_store.BatchCommitted)
                self.assertIs(
                    result.children[0].disposition,
                    ingestion_lease_store.ChildDisposition.STATUS_INELIGIBLE,
                )
                self.assertIs(
                    result.children[0].lifecycle_effect,
                    ingestion_lease_store.LifecycleEffect.NONE,
                )
                connection.fetch.assert_awaited_once()

    async def test_empty_batch_can_finalize_lease_recovery_once(self) -> None:
        before = _lease_row(
            failure_count=2,
            retry_after=_NOW,
            status_reason="source_unreachable",
            status_reason_detail="provider timeout",
        )
        after = _lease_row(
            failure_count=0,
            retry_after=None,
            status_reason=None,
            status_reason_detail=None,
        )
        pool = connection_util.make_mock_pool(transaction=True)
        connection = pool.acquired_connection
        connection.fetchrow.side_effect = [before, after]
        store = ingestion_lease_store.IngestionLeaseStore(pool)

        with mock.patch(
            "backend.pipeline.storage.ingestion_lease_store.logger.info"
        ) as log_recovery:
            log_recovery.side_effect = lambda *_args, **_kwargs: (
                pool.transaction_context.__aexit__.assert_awaited_once(),
                pool.acquire_context.__aexit__.assert_awaited_once(),
            )
            result = await store.commit_child_mutations(
                _grant(),
                ingestion_lease_store.ChildMutationBatch(
                    (),
                    ingestion_lease_store.FinalizeLeaseRecovery(),
                ),
                actor_id="service_account:gcp:collector",
            )

        assert isinstance(result, ingestion_lease_store.BatchCommitted)
        self.assertIs(
            result.lease_effect.effect,
            ingestion_lease_store.LeaseLifecycleEffect.RECOVERED,
        )
        self.assertIs(
            connection.fetchrow.await_args_list[1].args[0],
            ingestion_lease_queries.FINALIZE_LEASE_RECOVERY_SQL,
        )
        connection.fetch.assert_not_awaited()
        log_recovery.assert_called_once()

    async def test_each_raw_lifecycle_field_can_trigger_recovery(self) -> None:
        cases = (
            ("failure_count", {"failure_count": 1}),
            ("retry_after", {"retry_after": _NOW}),
            ("status_reason", {"status_reason": "source_unreachable"}),
            (
                "status_reason_detail",
                {"status_reason_detail": "provider timeout"},
            ),
        )

        for field_name, dirty_override in cases:
            with self.subTest(field_name=field_name):
                clean_lifecycle: dict[str, object] = {
                    "failure_count": 0,
                    "retry_after": None,
                    "status_reason": None,
                    "status_reason_detail": None,
                }
                before = _lease_row(
                    **(clean_lifecycle | dirty_override),
                )
                after = _lease_row(**clean_lifecycle)
                pool = connection_util.make_mock_pool(transaction=True)
                connection = pool.acquired_connection
                connection.fetchrow.side_effect = [before, after]
                store = ingestion_lease_store.IngestionLeaseStore(pool)

                result = await store.commit_child_mutations(
                    _grant(),
                    ingestion_lease_store.ChildMutationBatch(
                        (),
                        ingestion_lease_store.FinalizeLeaseRecovery(),
                    ),
                    actor_id="service_account:gcp:collector",
                )

                assert isinstance(result, ingestion_lease_store.BatchCommitted)
                self.assertIs(
                    result.lease_effect.effect,
                    ingestion_lease_store.LeaseLifecycleEffect.RECOVERED,
                )
                self.assertEqual(connection.fetchrow.await_count, 2)

    async def test_clean_finalize_retry_is_lease_lifecycle_noop(self) -> None:
        clean_lease = _lease_row(
            failure_count=0,
            retry_after=None,
            status_reason=None,
            status_reason_detail=None,
        )
        pool = connection_util.make_mock_pool(transaction=True)
        connection = pool.acquired_connection
        connection.fetchrow.return_value = clean_lease
        store = ingestion_lease_store.IngestionLeaseStore(pool)

        result = await store.commit_child_mutations(
            _grant(),
            ingestion_lease_store.ChildMutationBatch(
                (),
                ingestion_lease_store.FinalizeLeaseRecovery(),
            ),
            actor_id="service_account:gcp:collector",
        )

        assert isinstance(result, ingestion_lease_store.BatchCommitted)
        self.assertIs(
            result.lease_effect.effect,
            ingestion_lease_store.LeaseLifecycleEffect.NONE,
        )
        connection.fetchrow.assert_awaited_once()

    async def test_no_lease_effect_preserves_retained_failure_evidence(
        self,
    ) -> None:
        pool = connection_util.make_mock_pool(transaction=True)
        connection = pool.acquired_connection
        connection.fetchrow.return_value = _lease_row(failure_count=3)
        store = ingestion_lease_store.IngestionLeaseStore(pool)

        result = await store.commit_child_mutations(
            _grant(),
            ingestion_lease_store.ChildMutationBatch(
                (),
                ingestion_lease_store.NoLeaseEffect(),
            ),
            actor_id="service_account:gcp:collector",
        )

        assert isinstance(result, ingestion_lease_store.BatchCommitted)
        self.assertIs(
            result.lease_effect.effect,
            ingestion_lease_store.LeaseLifecycleEffect.NONE,
        )
        connection.fetchrow.assert_awaited_once()

    async def test_audit_failure_rolls_back_children_and_lease_recovery(
        self,
    ) -> None:
        feed_id = uuid.UUID("dddddddd-0000-0000-0000-000000000130")
        before_lease = _lease_row(failure_count=2)
        after_lease = _lease_row(
            failure_count=0,
            status_reason=None,
            status_reason_detail=None,
        )
        pool = connection_util.make_mock_pool(transaction=True)
        connection = pool.acquired_connection
        connection.fetchrow.side_effect = [before_lease, after_lease]
        connection.fetch.side_effect = [
            [
                _child_row(
                    feed_id,
                    status="failing",
                    failure_count=2,
                    status_reason="source_unreachable",
                )
            ],
            [
                _child_row(
                    feed_id,
                    caller_ordinal=0,
                    status="active",
                    failure_count=0,
                    status_reason=None,
                    last_bookmark_time=_NOW,
                    last_processed_filename="gs://bucket/audio.flac",
                    audit_revision=1,
                )
            ],
            RuntimeError("audit properties unavailable"),
        ]
        store = ingestion_lease_store.IngestionLeaseStore(pool)

        with mock.patch(
            "backend.pipeline.storage.ingestion_lease_store."
            "feed_change_notifications.emit_feed_change_notification"
        ) as emit:
            with self.assertRaisesRegex(
                RuntimeError,
                "audit properties unavailable",
            ):
                await store.commit_child_mutations(
                    _grant(),
                    ingestion_lease_store.ChildMutationBatch(
                        (self._progress(feed_id),),
                        ingestion_lease_store.FinalizeLeaseRecovery(),
                    ),
                    actor_id="service_account:gcp:collector",
                )

        emit.assert_not_called()
        self.assertIs(
            connection.fetchrow.await_args_list[1].args[0],
            ingestion_lease_queries.FINALIZE_LEASE_RECOVERY_SQL,
        )
        exit_args = pool.transaction_context.__aexit__.await_args.args
        self.assertIs(exit_args[0], RuntimeError)

    async def test_statement_fanout_is_independent_of_feed_count(self) -> None:
        observed_fetch_counts = []
        for feed_count in (1, 100):
            feed_ids = [uuid.UUID(int=index + 1) for index in range(feed_count)]
            pool = connection_util.make_mock_pool(transaction=True)
            connection = pool.acquired_connection
            connection.fetchrow.return_value = _lease_row()
            connection.fetch.side_effect = [
                [_child_row(feed_id) for feed_id in reversed(feed_ids)],
                [
                    _child_row(
                        feed_id,
                        caller_ordinal=index,
                        last_bookmark_time=_NOW,
                        last_processed_filename=f"gs://bucket/{index}.flac",
                    )
                    for index, feed_id in enumerate(feed_ids)
                ],
            ]
            store = ingestion_lease_store.IngestionLeaseStore(pool)

            result = await store.commit_child_mutations(
                _grant(),
                ingestion_lease_store.ChildMutationBatch(
                    tuple(
                        self._progress(
                            feed_id,
                            path=f"gs://bucket/{index}.flac",
                        )
                        for index, feed_id in enumerate(feed_ids)
                    ),
                    ingestion_lease_store.NoLeaseEffect(),
                ),
                actor_id="service_account:gcp:collector",
            )

            assert isinstance(result, ingestion_lease_store.BatchCommitted)
            self.assertEqual(len(result.children), feed_count)
            observed_fetch_counts.append(connection.fetch.await_count)

        self.assertEqual(observed_fetch_counts, [2, 2])


if __name__ == "__main__":
    unittest.main()
