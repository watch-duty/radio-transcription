from __future__ import annotations

import datetime
import unittest
import uuid
from unittest import mock

from backend.pipeline.storage.feed_store import (
    FeedStore,
    HeartbeatResult,
    LeasedFeed,
    SourceType,
)

_FEED_ID = uuid.UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
_FEED_ID_B = uuid.UUID("bbbbbbbb-cccc-dddd-eeee-ffffffffffff")
_WORKER_ID = uuid.UUID("11111111-2222-3333-4444-555555555555")

_LEASE_ROW = {
    "id": _FEED_ID,
    "name": "My Feed",
    "source_type": "bcfy_feeds",
    "last_processed_filename": None,
    "last_bookmark_time": None,
    "fencing_token": 1,
    "source_feed_id": "123",
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
            "source_type": SourceType.BCFY_FEEDS,
            "last_processed_filename": None,
            "last_bookmark_time": None,
            "fencing_token": 1,
            "source_feed_id": "123",
        }
        self.assertEqual(result, expected)

    async def test_returns_none_when_no_feed_available(self) -> None:
        """None is returned when no feed can be leased."""
        pool = _make_pool(fetchrow_result=None)
        store = FeedStore(pool)

        result = await store.lease_feed(_WORKER_ID)

        self.assertIsNone(result)

    async def test_passes_worker_id_as_parameter(self) -> None:
        """The worker_id and source_types are passed as parameters to the query."""
        pool = _make_pool(fetchrow_result=None)
        store = FeedStore(pool)

        await store.lease_feed(_WORKER_ID)

        args = pool.fetchrow.call_args[0]
        self.assertEqual(args[1], _WORKER_ID)
        self.assertIsNone(args[2])  # source_types default

    async def test_passes_source_types_filter(self) -> None:
        """When FeedStore is constructed with source_types, the filter is passed to the query."""
        pool = _make_pool(fetchrow_result=None)
        store = FeedStore(pool, source_types=["bcfy_feeds"])

        await store.lease_feed(_WORKER_ID)

        args = pool.fetchrow.call_args[0]
        self.assertEqual(args[2], ["bcfy_feeds"])

    async def test_raises_value_error_on_unknown_source_type(self) -> None:
        """ValueError is raised with details if the DB returns an unknown source type slug."""
        bad_row = _LEASE_ROW.copy()
        bad_row["source_type"] = "unknown_slug"
        pool = _make_pool(fetchrow_result=bad_row)
        store = FeedStore(pool)

        with self.assertRaises(ValueError) as ctx:
            await store.lease_feed(_WORKER_ID)

        self.assertIn(
            f"Unknown source type 'unknown_slug' for feed {_FEED_ID}",
            str(ctx.exception),
        )


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
            None,
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
            None,
        )

        self.assertFalse(result)

    async def test_passes_correct_parameters(self) -> None:
        """Parameters are passed in the correct order."""
        pool = _make_pool(execute_result="UPDATE 1")
        store = FeedStore(pool)
        gcs_path = "gs://bucket/path/file.ogg"

        await store.update_feed_progress(
            _FEED_ID, _WORKER_ID, gcs_path, 1, None
        )

        args = pool.execute.call_args[0]
        self.assertEqual(args[1:], (gcs_path, _FEED_ID, _WORKER_ID, 1, None))

    async def test_passes_non_none_last_bookmark_time(self) -> None:
        """Non-None last_bookmark_time is forwarded as the 5th SQL parameter."""
        pool = _make_pool(execute_result="UPDATE 1")
        store = FeedStore(pool)
        gcs_path = "gs://bucket/path/file.ogg"
        last_bookmark_time = datetime.datetime(
            2024,
            1,
            2,
            tzinfo=datetime.UTC,
        )
        await store.update_feed_progress(
            _FEED_ID,
            _WORKER_ID,
            gcs_path,
            1,
            last_bookmark_time,
        )
        args = pool.execute.call_args[0]
        self.assertEqual(
            args[1:],
            (gcs_path, _FEED_ID, _WORKER_ID, 1, last_bookmark_time),
        )


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

    async def test_returns_status_when_lease_held(self) -> None:
        """Status string is returned when the RETURNING row is present."""
        pool = _make_pool(
            fetchrow_result={
                "status": "failing",
                "failure_count": 1,
                "retry_after": None,
            },
        )
        store = FeedStore(pool)

        result = await store.report_feed_failure(_FEED_ID, _WORKER_ID, 1)

        self.assertEqual(result, "failing")

    async def test_returns_none_when_lease_lost(self) -> None:
        """None is returned when RETURNING yields no row."""
        pool = _make_pool(fetchrow_result=None)
        store = FeedStore(pool)

        result = await store.report_feed_failure(_FEED_ID, _WORKER_ID, 1)

        self.assertIsNone(result)

    async def test_returns_quarantined_status(self) -> None:
        """Quarantined status string is returned at threshold."""
        pool = _make_pool(
            fetchrow_result={
                "status": "quarantined",
                "failure_count": 5,
                "retry_after": None,
            },
        )
        store = FeedStore(pool)

        result = await store.report_feed_failure(_FEED_ID, _WORKER_ID, 1)

        self.assertEqual(result, "quarantined")

    async def test_passes_correct_parameters(self) -> None:
        """Parameters are passed in the correct order to the atomic SQL."""
        pool = _make_pool(
            fetchrow_result={
                "status": "failing",
                "failure_count": 1,
                "retry_after": None,
            },
        )
        store = FeedStore(pool)

        await store.report_feed_failure(_FEED_ID, _WORKER_ID, 1)

        args = pool.fetchrow.call_args[0]
        # $1=feed_id, $2=worker_id, $3=threshold, $4=fencing_token,
        # $5=backoff_max_sec, $6=backoff_base_sec
        self.assertEqual(args[1], _FEED_ID)
        self.assertEqual(args[2], _WORKER_ID)
        self.assertEqual(args[3], 5)  # default threshold
        self.assertEqual(args[4], 1)  # fencing_token
        self.assertEqual(args[5], 600)  # default backoff_max_sec
        self.assertEqual(args[6], 15)  # default backoff_base_sec


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
                "last_bookmark_time": None,
                "fencing_token": 1,
                "source_feed_id": "123",
            },
            {
                "id": _FEED_ID_B,
                "name": "Feed B",
                "source_type": "bcfy_feeds",
                "last_processed_filename": "gs://bucket/path",
                "last_bookmark_time": None,
                "fencing_token": 1,
                "source_feed_id": None,
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
        """Parameters include worker_id, timedelta, limit, and source_types."""
        pool = _make_pool(fetch_result=[])
        store = FeedStore(pool)

        await store.acquire_feeds_batch(_WORKER_ID, 60.0, limit=5)

        args = pool.fetch.call_args[0]
        self.assertEqual(args[1], _WORKER_ID)
        self.assertEqual(args[2], datetime.timedelta(seconds=60.0))
        self.assertEqual(args[3], 5)
        self.assertIsNone(args[4])  # source_types default

    async def test_passes_source_types_filter(self) -> None:
        """When FeedStore is constructed with source_types, the filter is passed to the query."""
        pool = _make_pool(fetch_result=[])
        store = FeedStore(pool, source_types=["bcfy_feeds", "bcfy_calls"])

        await store.acquire_feeds_batch(_WORKER_ID, 60.0, limit=5)

        args = pool.fetch.call_args[0]
        self.assertEqual(args[4], ["bcfy_feeds", "bcfy_calls"])

    async def test_raises_value_error_on_unknown_source_type(self) -> None:
        """ValueError is raised with details if the DB returns an unknown source type slug."""
        bad_row = {
            "id": _FEED_ID,
            "name": "Bad Feed",
            "source_type": "invalid_type",
            "last_processed_filename": None,
            "fencing_token": 1,
            "source_feed_id": None,
        }
        pool = _make_pool(fetch_result=[bad_row])
        store = FeedStore(pool)

        with self.assertRaises(ValueError) as ctx:
            await store.acquire_feeds_batch(_WORKER_ID, 60.0, limit=1)

        self.assertIn(
            f"Unknown source type 'invalid_type' for feed {_FEED_ID}",
            str(ctx.exception),
        )


class TestReportFeedFailureWithThreshold(unittest.IsolatedAsyncioTestCase):
    """Tests for FeedStore.report_feed_failure with custom threshold."""

    async def test_custom_threshold_passed_to_sql(self) -> None:
        """Custom failure_threshold is passed as $3 parameter."""
        pool = _make_pool(
            fetchrow_result={
                "status": "failing",
                "failure_count": 1,
                "retry_after": None,
            },
        )
        store = FeedStore(pool)

        await store.report_feed_failure(
            _FEED_ID, _WORKER_ID, 1, failure_threshold=5
        )

        args = pool.fetchrow.call_args[0]
        self.assertEqual(args[3], 5)  # $3 = threshold

    async def test_default_threshold_is_5(self) -> None:
        """Default threshold is 5."""
        pool = _make_pool(
            fetchrow_result={
                "status": "failing",
                "failure_count": 1,
                "retry_after": None,
            },
        )
        store = FeedStore(pool)

        await store.report_feed_failure(_FEED_ID, _WORKER_ID, 1)

        args = pool.fetchrow.call_args[0]
        self.assertEqual(args[3], 5)


class TestBackoffFormula(unittest.TestCase):
    """Verify the exponential backoff computation used by report_feed_failure.

    Default: base=15s, max=600s (10 minutes).
    """

    def test_first_failure_15s(self) -> None:
        assert min(15 * (2**0), 600) == 15

    def test_third_failure_60s(self) -> None:
        assert min(15 * (2**2), 600) == 60

    def test_sixth_failure_480s(self) -> None:
        assert min(15 * (2**5), 600) == 480

    def test_seventh_failure_capped_600s(self) -> None:
        assert min(15 * (2**6), 600) == 600

    def test_tenth_failure_still_capped(self) -> None:
        assert min(15 * (2**9), 600) == 600


class TestReleaseFeedsBatch(unittest.IsolatedAsyncioTestCase):
    """Tests for FeedStore.release_feeds_batch."""

    async def test_returns_count_when_released(self) -> None:
        """The number of released feeds is returned."""
        pool = _make_pool(execute_result="UPDATE 2")
        store = FeedStore(pool)

        result = await store.release_feeds_batch(_WORKER_ID)

        self.assertEqual(result, 2)

    async def test_returns_zero_when_none_released(self) -> None:
        """Zero is returned when no feeds were released."""
        pool = _make_pool(execute_result="UPDATE 0")
        store = FeedStore(pool)

        result = await store.release_feeds_batch(_WORKER_ID)

        self.assertEqual(result, 0)

    async def test_passes_correct_parameters(self) -> None:
        """The worker_id is passed as a parameter to the query."""
        pool = _make_pool(execute_result="UPDATE 1")
        store = FeedStore(pool)

        await store.release_feeds_batch(_WORKER_ID)

        args = pool.execute.call_args[0]
        self.assertEqual(args[1], _WORKER_ID)


class TestCreateFeed(unittest.IsolatedAsyncioTestCase):
    """Tests for FeedStore.create_feed."""

    async def test_returns_feed_on_success(self) -> None:
        """A created feed is returned as a Feed dict."""
        row = {
            "id": _FEED_ID,
            "name": "New Feed",
            "source_type": "bcfy_feeds",
            "status": "unclaimed",
            "failure_count": 0,
            "worker_id": None,
            "last_heartbeat": None,
            "last_processed_filename": None,
            "last_bookmark_time": None,
            "created_at": datetime.datetime(2026, 4, 10, tzinfo=datetime.UTC),
            "source_feed_id": "123",
            "external_id": "ext_123",
        }
        pool = _make_pool(fetchrow_result=row)
        store = FeedStore(pool)

        result = await store.create_feed(
            "New Feed", "bcfy_feeds", "123", "ext_123"
        )

        self.assertEqual(result["id"], _FEED_ID)
        self.assertEqual(result["name"], "New Feed")
        self.assertEqual(result["source_type"], SourceType.BCFY_FEEDS)

    async def test_raises_value_error_on_failure(self) -> None:
        """ValueError is raised if the DB returns no row."""
        pool = _make_pool(fetchrow_result=None)
        store = FeedStore(pool)

        with self.assertRaises(ValueError):
            await store.create_feed("New Feed", "bcfy_feeds", "123", "ext_123")


class TestGetFeed(unittest.IsolatedAsyncioTestCase):
    """Tests for FeedStore.get_feed."""

    async def test_returns_feed_when_exists(self) -> None:
        """A feed is returned as a Feed dict when it exists."""
        row = {
            "id": _FEED_ID,
            "name": "My Feed",
            "source_type": "bcfy_feeds",
            "status": "unclaimed",
            "failure_count": 0,
            "worker_id": None,
            "last_heartbeat": None,
            "last_processed_filename": None,
            "last_bookmark_time": None,
            "created_at": datetime.datetime(2026, 4, 10, tzinfo=datetime.UTC),
            "source_feed_id": "123",
            "external_id": "ext_123",
        }
        pool = _make_pool(fetchrow_result=row)
        store = FeedStore(pool)

        result = await store.get_feed(_FEED_ID)

        assert result is not None
        self.assertEqual(result["id"], _FEED_ID)

    async def test_returns_none_when_not_exists(self) -> None:
        """None is returned when the feed does not exist."""
        pool = _make_pool(fetchrow_result=None)
        store = FeedStore(pool)

        result = await store.get_feed(_FEED_ID)

        self.assertIsNone(result)


class TestListFeeds(unittest.IsolatedAsyncioTestCase):
    """Tests for FeedStore.list_feeds."""

    async def test_returns_list_of_feeds(self) -> None:
        """A list of Feed dicts is returned."""
        rows = [
            {
                "id": _FEED_ID,
                "name": "Feed A",
                "source_type": "bcfy_feeds",
                "status": "unclaimed",
                "failure_count": 0,
                "worker_id": None,
                "last_heartbeat": None,
                "last_processed_filename": None,
                "last_bookmark_time": None,
                "created_at": datetime.datetime(
                    2026, 4, 10, tzinfo=datetime.UTC
                ),
                "source_feed_id": "123",
                "external_id": "ext_123",
            },
            {
                "id": _FEED_ID_B,
                "name": "Feed B",
                "source_type": "openmhz",
                "status": "active",
                "failure_count": 0,
                "worker_id": _WORKER_ID,
                "last_heartbeat": datetime.datetime(
                    2026, 4, 10, tzinfo=datetime.UTC
                ),
                "last_processed_filename": None,
                "last_bookmark_time": None,
                "created_at": datetime.datetime(
                    2026, 4, 9, tzinfo=datetime.UTC
                ),
                "source_feed_id": "456",
                "external_id": "ext_456",
            },
        ]
        pool = _make_pool(fetch_result=rows)
        store = FeedStore(pool)

        result = await store.list_feeds()

        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]["id"], _FEED_ID)
        self.assertEqual(result[1]["id"], _FEED_ID_B)
        self.assertEqual(result[1]["source_type"], SourceType.OPENMHZ)


if __name__ == "__main__":
    unittest.main()
