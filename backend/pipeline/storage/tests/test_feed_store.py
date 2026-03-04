from __future__ import annotations

import datetime
import unittest
import uuid
from unittest import mock

from backend.pipeline.storage.feed_store import FeedStore, LeasedFeed

_FEED_ID = uuid.UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
_FEED_ID_B = uuid.UUID("bbbbbbbb-cccc-dddd-eeee-ffffffffffff")
_WORKER_ID = uuid.UUID("11111111-2222-3333-4444-555555555555")

_LEASE_ROW = {
    "id": _FEED_ID,
    "name": "My Feed",
    "source_type": "bcfy_feeds",
    "last_processed_filename": None,
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
            _FEED_ID, _WORKER_ID, "gs://bucket/path/file.ogg",
        )

        self.assertTrue(result)

    async def test_returns_false_when_lease_lost(self) -> None:
        """False is returned when no row matches (lease was lost)."""
        pool = _make_pool(execute_result="UPDATE 0")
        store = FeedStore(pool)

        result = await store.update_feed_progress(
            _FEED_ID, _WORKER_ID, "gs://bucket/path/file.ogg",
        )

        self.assertFalse(result)

    async def test_passes_correct_parameters(self) -> None:
        """Parameters are passed in the correct order."""
        pool = _make_pool(execute_result="UPDATE 1")
        store = FeedStore(pool)
        gcs_path = "gs://bucket/path/file.ogg"

        await store.update_feed_progress(_FEED_ID, _WORKER_ID, gcs_path)

        args = pool.execute.call_args[0]
        self.assertEqual(args[1:], (gcs_path, _FEED_ID, _WORKER_ID))


class TestRenewHeartbeat(unittest.IsolatedAsyncioTestCase):
    """Tests for FeedStore.renew_heartbeat."""

    async def test_returns_true_when_lease_held(self) -> None:
        """True is returned when the heartbeat was renewed."""
        pool = _make_pool(execute_result="UPDATE 1")
        store = FeedStore(pool)

        result = await store.renew_heartbeat(_FEED_ID, _WORKER_ID)

        self.assertTrue(result)

    async def test_returns_false_when_lease_lost(self) -> None:
        """False is returned when the lease was lost (fence violation)."""
        pool = _make_pool(execute_result="UPDATE 0")
        store = FeedStore(pool)

        result = await store.renew_heartbeat(_FEED_ID, _WORKER_ID)

        self.assertFalse(result)

    async def test_passes_correct_parameters(self) -> None:
        """Parameters are passed in the correct order."""
        pool = _make_pool(execute_result="UPDATE 1")
        store = FeedStore(pool)

        await store.renew_heartbeat(_FEED_ID, _WORKER_ID)

        args = pool.execute.call_args[0]
        self.assertEqual(args[1:], (_FEED_ID, _WORKER_ID))


class TestRenewHeartbeatsBatch(unittest.IsolatedAsyncioTestCase):
    """Tests for FeedStore.renew_heartbeats_batch."""

    async def test_returns_renewed_ids(self) -> None:
        """Returned set contains the IDs from RETURNING rows."""
        pool = _make_pool(
            fetch_result=[{"id": _FEED_ID}, {"id": _FEED_ID_B}],
        )
        store = FeedStore(pool)

        result = await store.renew_heartbeats_batch(
            [_FEED_ID, _FEED_ID_B], _WORKER_ID,
        )

        self.assertEqual(result, {_FEED_ID, _FEED_ID_B})

    async def test_returns_empty_set_for_no_matches(self) -> None:
        """Empty set is returned when no rows match (all leases lost)."""
        pool = _make_pool(fetch_result=[])
        store = FeedStore(pool)

        result = await store.renew_heartbeats_batch([_FEED_ID], _WORKER_ID)

        self.assertEqual(result, set())

    async def test_short_circuits_on_empty_input(self) -> None:
        """Empty feed_ids list returns empty set without executing a query."""
        pool = mock.AsyncMock()
        store = FeedStore(pool)

        result = await store.renew_heartbeats_batch([], _WORKER_ID)

        self.assertEqual(result, set())
        pool.fetch.assert_not_called()

    async def test_passes_correct_parameters(self) -> None:
        """Parameters are passed as (feed_ids_list, worker_id)."""
        pool = _make_pool(fetch_result=[{"id": _FEED_ID}])
        store = FeedStore(pool)
        feed_ids = [_FEED_ID, _FEED_ID_B]

        await store.renew_heartbeats_batch(feed_ids, _WORKER_ID)

        args = pool.fetch.call_args[0]
        self.assertEqual(args[1:], (feed_ids, _WORKER_ID))


class TestReportFeedFailure(unittest.IsolatedAsyncioTestCase):
    """Tests for FeedStore.report_feed_failure."""

    async def test_returns_true_when_lease_held(self) -> None:
        """True is returned when the failure was recorded."""
        pool = _make_pool(execute_result="UPDATE 1")
        store = FeedStore(pool)

        result = await store.report_feed_failure(_FEED_ID, _WORKER_ID)

        self.assertTrue(result)

    async def test_returns_false_when_lease_lost(self) -> None:
        """False is returned when the lease was already lost."""
        pool = _make_pool(execute_result="UPDATE 0")
        store = FeedStore(pool)

        result = await store.report_feed_failure(_FEED_ID, _WORKER_ID)

        self.assertFalse(result)

    async def test_passes_correct_parameters(self) -> None:
        """Parameters are passed in the correct order."""
        pool = _make_pool(execute_result="UPDATE 1")
        store = FeedStore(pool)

        await store.report_feed_failure(_FEED_ID, _WORKER_ID)

        args = pool.execute.call_args[0]
        self.assertEqual(args[1:], (_FEED_ID, _WORKER_ID, 3))


class TestReleaseFeed(unittest.IsolatedAsyncioTestCase):
    """Tests for FeedStore.release_feed."""

    async def test_returns_true_when_lease_held(self) -> None:
        """True is returned when the feed was released."""
        pool = _make_pool(execute_result="UPDATE 1")
        store = FeedStore(pool)

        result = await store.release_feed(_FEED_ID, _WORKER_ID)

        self.assertTrue(result)

    async def test_returns_false_when_lease_lost(self) -> None:
        """False is returned when the lease was already lost."""
        pool = _make_pool(execute_result="UPDATE 0")
        store = FeedStore(pool)

        result = await store.release_feed(_FEED_ID, _WORKER_ID)

        self.assertFalse(result)

    async def test_passes_correct_parameters(self) -> None:
        """Parameters are passed in the correct order."""
        pool = _make_pool(execute_result="UPDATE 1")
        store = FeedStore(pool)

        await store.release_feed(_FEED_ID, _WORKER_ID)

        args = pool.execute.call_args[0]
        self.assertEqual(args[1:], (_FEED_ID, _WORKER_ID))


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
                "stream_url": "http://stream.example.com/a",
            },
            {
                "id": _FEED_ID_B,
                "name": "Feed B",
                "source_type": "bcfy_feeds",
                "last_processed_filename": "gs://bucket/path",
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

    async def test_passes_custom_threshold(self) -> None:
        """Custom failure_threshold is passed as query parameter."""
        pool = _make_pool(execute_result="UPDATE 1")
        store = FeedStore(pool)

        await store.report_feed_failure(_FEED_ID, _WORKER_ID, failure_threshold=5)

        args = pool.execute.call_args[0]
        self.assertEqual(args[3], 5)

    async def test_default_threshold_is_3(self) -> None:
        """Default threshold is 3 for backward compatibility."""
        pool = _make_pool(execute_result="UPDATE 1")
        store = FeedStore(pool)

        await store.report_feed_failure(_FEED_ID, _WORKER_ID)

        args = pool.execute.call_args[0]
        self.assertEqual(args[3], 3)


if __name__ == "__main__":
    unittest.main()
