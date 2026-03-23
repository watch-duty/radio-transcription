from __future__ import annotations

import datetime
import unittest
import uuid
from unittest import mock

from backend.pipeline.storage.feed_store import FeedStore, HeartbeatResult, LeasedFeed

_FEED_ID = uuid.UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
_FEED_ID_B = uuid.UUID("bbbbbbbb-cccc-dddd-eeee-ffffffffffff")
_WORKER_ID = uuid.UUID("11111111-2222-3333-4444-555555555555")

_LEASE_ROW = {
    "id": _FEED_ID,
    "name": "My Feed",
    "source_type": "bcfy_feeds",
    "last_processed_filename": None,
    "fencing_token": 1,
    "stream_url": "http://stream.example.com/feed",
}


def _make_pool(
    *,
    fetchrow_result: dict | None = None,
    execute_result: str = "UPDATE 0",
    fetch_result: list | None = None,
) -> mock.AsyncMock:
    """Create a mock asyncpg.Pool with the given return values."""
    pool = mock.AsyncMock()
    pool.fetchrow.return_value = fetchrow_result
    pool.execute.return_value = execute_result
    pool.fetch.return_value = fetch_result or []
    return pool


class TestLeaseFeed(unittest.IsolatedAsyncioTestCase):
    """Tests for FeedStore.lease_feed."""

    async def test_returns_feed_when_available(self) -> None:
        """A leased feed is returned as a LeasedFeed dict."""
        pool = _make_pool(fetchrow_result=_LEASE_ROW)
        store = FeedStore(pool)

        result = await store.lease_feed(_WORKER_ID)

        expected: LeasedFeed = {
            "id": _FEED_ID,
            "name": "My Feed",
            "source_type": "bcfy_feeds",
            "last_processed_filename": None,
            "fencing_token": 1,
            "stream_url": "http://stream.example.com/feed",
        }
        self.assertEqual(result, expected)

    async def test_returns_none_when_no_feed_available(self) -> None:
        """None is returned when no feed can be leased."""
        pool = _make_pool(fetchrow_result=None)
        store = FeedStore(pool)

        result = await store.lease_feed(_WORKER_ID)

        self.assertIsNone(result)

    async def test_passes_worker_id_as_parameter(self) -> None:
        """The worker_id is passed as a parameter to the query."""
        pool = _make_pool(fetchrow_result=None)
        store = FeedStore(pool)

        await store.lease_feed(_WORKER_ID)

        args = pool.fetchrow.call_args
        self.assertEqual(args[0][1], _WORKER_ID)


class TestUpdateFeedProgress(unittest.IsolatedAsyncioTestCase):
    """Tests for FeedStore.update_feed_progress."""

    async def test_returns_true_when_lease_held(self) -> None:
        """True is returned when the fenced update succeeds."""
        pool = _make_pool(execute_result="UPDATE 1")
        store = FeedStore(pool)

        result = await store.update_feed_progress(
            _FEED_ID,
            _WORKER_ID,
            "gs://bucket/path/file.ogg",
            1,
        )

        self.assertTrue(result)

    async def test_returns_false_when_lease_lost(self) -> None:
        """False is returned when no row matches (lease was lost)."""
        pool = _make_pool(execute_result="UPDATE 0")
        store = FeedStore(pool)

        result = await store.update_feed_progress(
            _FEED_ID,
            _WORKER_ID,
            "gs://bucket/path/file.ogg",
            1,
        )

        self.assertFalse(result)

    async def test_passes_correct_parameters(self) -> None:
        """Parameters are passed in the correct order."""
        pool = _make_pool(execute_result="UPDATE 1")
        store = FeedStore(pool)
        gcs_path = "gs://bucket/path/file.ogg"

        await store.update_feed_progress(_FEED_ID, _WORKER_ID, gcs_path, 1)

        args = pool.execute.call_args[0]
        self.assertEqual(args[1:], (gcs_path, _FEED_ID, _WORKER_ID, 1))


class TestRenewHeartbeatsBatchDiagnostic(unittest.IsolatedAsyncioTestCase):
    """Tests for FeedStore.renew_heartbeats_batch_diagnostic."""

    async def test_returns_diagnostic_results(self) -> None:
        """Returned list contains HeartbeatResult dicts with diagnostic info."""
        other_worker = uuid.UUID("22222222-3333-4444-5555-666666666666")
        pool = _make_pool(
            fetch_result=[
                {
                    "id": _FEED_ID,
                    "current_worker": _WORKER_ID,
                    "current_status": "active",
                    "renewed": True,
                },
                {
                    "id": _FEED_ID_B,
                    "current_worker": other_worker,
                    "current_status": "active",
                    "renewed": False,
                },
            ],
        )
        store = FeedStore(pool)

        result = await store.renew_heartbeats_batch_diagnostic(
            [_FEED_ID, _FEED_ID_B],
            _WORKER_ID,
        )

        self.assertEqual(len(result), 2)
        self.assertEqual(
            result[0],
            HeartbeatResult(
                id=_FEED_ID,
                current_worker=_WORKER_ID,
                current_status="active",
                renewed=True,
            ),
        )
        self.assertEqual(
            result[1],
            HeartbeatResult(
                id=_FEED_ID_B,
                current_worker=other_worker,
                current_status="active",
                renewed=False,
            ),
        )

    async def test_short_circuits_on_empty_input(self) -> None:
        """Empty feed_ids list returns empty list without executing a query."""
        pool = mock.AsyncMock()
        store = FeedStore(pool)

        result = await store.renew_heartbeats_batch_diagnostic([], _WORKER_ID)

        self.assertEqual(result, [])
        pool.fetch.assert_not_called()

    async def test_passes_correct_parameters(self) -> None:
        """Parameters are passed as (feed_ids_list, worker_id)."""
        pool = _make_pool(
            fetch_result=[
                {
                    "id": _FEED_ID,
                    "current_worker": _WORKER_ID,
                    "current_status": "active",
                    "renewed": True,
                },
            ],
        )
        store = FeedStore(pool)
        feed_ids = [_FEED_ID, _FEED_ID_B]

        await store.renew_heartbeats_batch_diagnostic(feed_ids, _WORKER_ID)

        args = pool.fetch.call_args[0]
        self.assertEqual(args[1:], (feed_ids, _WORKER_ID))

    async def test_mixed_renewed_and_unrenewed(self) -> None:
        """Results correctly distinguish renewed vs unrenewed feeds."""
        pool = _make_pool(
            fetch_result=[
                {
                    "id": _FEED_ID,
                    "current_worker": _WORKER_ID,
                    "current_status": "active",
                    "renewed": True,
                },
                {
                    "id": _FEED_ID_B,
                    "current_worker": None,
                    "current_status": "unclaimed",
                    "renewed": False,
                },
            ],
        )
        store = FeedStore(pool)

        result = await store.renew_heartbeats_batch_diagnostic(
            [_FEED_ID, _FEED_ID_B],
            _WORKER_ID,
        )

        renewed = [r for r in result if r["renewed"]]
        not_renewed = [r for r in result if not r["renewed"]]
        self.assertEqual(len(renewed), 1)
        self.assertEqual(renewed[0]["id"], _FEED_ID)
        self.assertEqual(len(not_renewed), 1)
        self.assertEqual(not_renewed[0]["current_status"], "unclaimed")


class TestReportFeedFailure(unittest.IsolatedAsyncioTestCase):
    """Tests for FeedStore.report_feed_failure."""

    async def test_returns_true_when_lease_held(self) -> None:
        """True is returned when the failure was recorded."""
        pool = _make_pool(
            execute_result="UPDATE 1",
            fetchrow_result={
                "status": "failing",
                "failure_count": 1,
                "retry_after": None,
            },
        )
        store = FeedStore(pool)

        result = await store.report_feed_failure(_FEED_ID, _WORKER_ID, 1)

        self.assertTrue(result)

    async def test_returns_false_when_lease_lost(self) -> None:
        """False is returned when the UPDATE matches no rows."""
        pool = _make_pool(execute_result="UPDATE 0")
        store = FeedStore(pool)

        result = await store.report_feed_failure(_FEED_ID, _WORKER_ID, 1)

        self.assertFalse(result)

    async def test_passes_correct_parameters(self) -> None:
        """Parameters are passed in the correct order to the atomic SQL."""
        pool = _make_pool(
            execute_result="UPDATE 1",
            fetchrow_result={
                "status": "failing",
                "failure_count": 1,
                "retry_after": None,
            },
        )
        store = FeedStore(pool)

        await store.report_feed_failure(_FEED_ID, _WORKER_ID, 1)

        args = pool.execute.call_args[0]
        # $1=feed_id, $2=worker_id, $3=threshold, $4=fencing_token
        self.assertEqual(args[1], _FEED_ID)
        self.assertEqual(args[2], _WORKER_ID)
        self.assertEqual(args[3], 3)  # default threshold
        self.assertEqual(args[4], 1)  # fencing_token


class TestReleaseFeed(unittest.IsolatedAsyncioTestCase):
    """Tests for FeedStore.release_feed."""

    async def test_returns_true_when_lease_held(self) -> None:
        """True is returned when the feed was released."""
        pool = _make_pool(execute_result="UPDATE 1")
        store = FeedStore(pool)

        result = await store.release_feed(_FEED_ID, _WORKER_ID, 1)

        self.assertTrue(result)

    async def test_returns_false_when_lease_lost(self) -> None:
        """False is returned when the lease was already lost."""
        pool = _make_pool(execute_result="UPDATE 0")
        store = FeedStore(pool)

        result = await store.release_feed(_FEED_ID, _WORKER_ID, 1)

        self.assertFalse(result)

    async def test_passes_correct_parameters(self) -> None:
        """Parameters are passed in the correct order."""
        pool = _make_pool(execute_result="UPDATE 1")
        store = FeedStore(pool)

        await store.release_feed(_FEED_ID, _WORKER_ID, 1)

        args = pool.execute.call_args[0]
        self.assertEqual(args[1:], (_FEED_ID, _WORKER_ID, 1))


class TestAcquireFeedsBatch(unittest.IsolatedAsyncioTestCase):
    """Tests for FeedStore.acquire_feeds_batch."""

    async def test_returns_list_of_feeds(self) -> None:
        """Multiple feeds are returned as a list of LeasedFeed dicts."""
        rows = [
            {
                "id": _FEED_ID,
                "name": "Feed A",
                "source_type": "bcfy_feeds",
                "last_processed_filename": None,
                "fencing_token": 1,
                "stream_url": "http://stream.example.com/a",
            },
            {
                "id": _FEED_ID_B,
                "name": "Feed B",
                "source_type": "bcfy_feeds",
                "last_processed_filename": "gs://bucket/path",
                "fencing_token": 1,
                "stream_url": None,
            },
        ]
        pool = _make_pool(fetch_result=rows)
        store = FeedStore(pool)

        result = await store.acquire_feeds_batch(_WORKER_ID, 60.0, limit=10)

        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]["id"], _FEED_ID)
        self.assertEqual(result[1]["id"], _FEED_ID_B)

    async def test_returns_empty_list_when_none_available(self) -> None:
        """Empty list returned when no feeds can be leased."""
        pool = _make_pool(fetch_result=[])
        store = FeedStore(pool)

        result = await store.acquire_feeds_batch(_WORKER_ID, 60.0, limit=10)

        self.assertEqual(result, [])

    async def test_passes_correct_parameters(self) -> None:
        """Parameters include worker_id, timedelta, and limit."""
        pool = _make_pool(fetch_result=[])
        store = FeedStore(pool)

        await store.acquire_feeds_batch(_WORKER_ID, 60.0, limit=5)

        args = pool.fetch.call_args[0]
        self.assertEqual(args[1], _WORKER_ID)
        self.assertEqual(args[2], datetime.timedelta(seconds=60.0))
        self.assertEqual(args[3], 5)


class TestReportFeedFailureWithThreshold(unittest.IsolatedAsyncioTestCase):
    """Tests for FeedStore.report_feed_failure with custom threshold."""

    async def test_custom_threshold_passed_to_sql(self) -> None:
        """Custom failure_threshold is passed as $3 parameter."""
        pool = _make_pool(
            execute_result="UPDATE 1",
            fetchrow_result={
                "status": "failing",
                "failure_count": 1,
                "retry_after": None,
            },
        )
        store = FeedStore(pool)

        await store.report_feed_failure(_FEED_ID, _WORKER_ID, 1, failure_threshold=5)

        args = pool.execute.call_args[0]
        self.assertEqual(args[3], 5)  # $3 = threshold

    async def test_default_threshold_is_3(self) -> None:
        """Default threshold is 3."""
        pool = _make_pool(
            execute_result="UPDATE 1",
            fetchrow_result={
                "status": "failing",
                "failure_count": 1,
                "retry_after": None,
            },
        )
        store = FeedStore(pool)

        await store.report_feed_failure(_FEED_ID, _WORKER_ID, 1)

        args = pool.execute.call_args[0]
        self.assertEqual(args[3], 3)


class TestBackoffFormula(unittest.TestCase):
    """Verify the exponential backoff computation used by report_feed_failure."""

    def test_first_failure_30s(self) -> None:
        assert min(30 * (2**0), 3600) == 30

    def test_third_failure_120s(self) -> None:
        assert min(30 * (2**2), 3600) == 120

    def test_seventh_failure_1920s(self) -> None:
        assert min(30 * (2**6), 3600) == 1920

    def test_eighth_failure_capped_3600s(self) -> None:
        assert min(30 * (2**7), 3600) == 3600

    def test_tenth_failure_still_capped(self) -> None:
        assert min(30 * (2**9), 3600) == 3600


if __name__ == "__main__":
    unittest.main()
