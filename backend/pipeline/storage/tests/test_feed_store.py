from __future__ import annotations

import datetime
import json
import unittest
import uuid
from typing import cast
from unittest import mock

import asyncpg

from backend.pipeline.storage import feed_queries
from backend.pipeline.storage.feed_store import (
    FeedStore,
    HeartbeatResult,
    SourceType,
)

_FEED_ID = uuid.UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
_FEED_ID_B = uuid.UUID("bbbbbbbb-cccc-dddd-eeee-ffffffffffff")
_WORKER_ID = uuid.UUID("11111111-2222-3333-4444-555555555555")

_LEASE_ROW = {
    "id": _FEED_ID,
    "name": "My Feed",
    "external_id": "ext-id",
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


_DEFAULT_LIMITS: dict[SourceType, int] = {
    SourceType.BCFY_FEEDS: 10,
    SourceType.BCFY_CALLS: 10,
    SourceType.OPENMHZ: 10,
}


class TestAcquireFeedsBatch(unittest.IsolatedAsyncioTestCase):
    """Tests for FeedStore.acquire_feeds_batch."""

    async def test_returns_list_of_feeds(self) -> None:
        """Multiple feeds are returned as a list of LeasedFeed dicts."""
        rows = [
            {
                "id": _FEED_ID,
                "name": "Feed A",
                "external_id": "ext-id",
                "source_type": "bcfy_feeds",
                "last_processed_filename": None,
                "last_bookmark_time": None,
                "fencing_token": 1,
                "source_feed_id": "123",
            },
            {
                "id": _FEED_ID_B,
                "name": "Feed B",
                "external_id": "ext-id",
                "source_type": "bcfy_feeds",
                "last_processed_filename": "gs://bucket/path",
                "last_bookmark_time": None,
                "fencing_token": 1,
                "source_feed_id": None,
            },
        ]
        pool = _make_pool(fetch_result=rows)
        store = FeedStore(pool)

        result = await store.acquire_feeds_batch(_WORKER_ID, _DEFAULT_LIMITS)

        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]["id"], _FEED_ID)
        self.assertEqual(result[1]["id"], _FEED_ID_B)

    async def test_returns_empty_list_when_none_available(self) -> None:
        """Empty list returned when no feeds can be leased."""
        pool = _make_pool(fetch_result=[])
        store = FeedStore(pool)

        result = await store.acquire_feeds_batch(_WORKER_ID, _DEFAULT_LIMITS)

        self.assertEqual(result, [])

    async def test_passes_positional_in_claim_types_order(self) -> None:
        """Limits dict is unpacked in claim_types iteration order."""
        pool = _make_pool(fetch_result=[])
        store = FeedStore(
            pool,
            claim_types=[
                SourceType.BCFY_FEEDS,
                SourceType.BCFY_CALLS,
                SourceType.OPENMHZ,
            ],
        )

        await store.acquire_feeds_batch(
            _WORKER_ID,
            {
                SourceType.BCFY_FEEDS: 2,
                SourceType.BCFY_CALLS: 3,
                SourceType.OPENMHZ: 5,
            },
        )

        args = pool.fetch.call_args[0]
        # args[0] is the generated SQL string (not a constant identity check
        # anymore — the constant no longer exists).
        self.assertIsInstance(args[0], str)
        self.assertEqual(args[1], _WORKER_ID)
        self.assertEqual(args[2], 2)  # BCFY_FEEDS
        self.assertEqual(args[3], 3)  # BCFY_CALLS
        self.assertEqual(args[4], 5)  # OPENMHZ

    async def test_per_type_limit_zero_is_passed_through(self) -> None:
        """A branch's LIMIT of 0 reaches the SQL — DB enforces the skip."""
        pool = _make_pool(fetch_result=[])
        store = FeedStore(
            pool,
            claim_types=[
                SourceType.BCFY_FEEDS,
                SourceType.BCFY_CALLS,
                SourceType.OPENMHZ,
            ],
        )

        await store.acquire_feeds_batch(
            _WORKER_ID,
            {
                SourceType.BCFY_FEEDS: 0,
                SourceType.BCFY_CALLS: 10,
                SourceType.OPENMHZ: 10,
            },
        )

        args = pool.fetch.call_args[0]
        self.assertEqual(args[2], 0)

    async def test_absent_limit_key_treated_as_zero(self) -> None:
        """Types absent from limits dict pass 0 to the SQL — same effect as LIMIT 0."""
        pool = _make_pool(fetch_result=[])
        store = FeedStore(
            pool,
            claim_types=[
                SourceType.BCFY_FEEDS,
                SourceType.BCFY_CALLS,
                SourceType.OPENMHZ,
            ],
        )

        await store.acquire_feeds_batch(
            _WORKER_ID,
            {SourceType.BCFY_FEEDS: 5},
        )

        args = pool.fetch.call_args[0]
        self.assertEqual(args[2], 5)
        self.assertEqual(args[3], 0)
        self.assertEqual(args[4], 0)

    async def test_raises_on_unknown_limit_key(self) -> None:
        """A SourceType not in claim_types raises ValueError."""
        pool = _make_pool(fetch_result=[])
        # Default claim_types = SourceType minus ECHO. Construct a store
        # that only claims BCFY_FEEDS so OPENMHZ is unknown.
        store = FeedStore(pool, claim_types=[SourceType.BCFY_FEEDS])

        with self.assertRaises(ValueError) as ctx:
            await store.acquire_feeds_batch(
                _WORKER_ID,
                {SourceType.BCFY_FEEDS: 1, SourceType.OPENMHZ: 1},
            )
        self.assertIn("openmhz", str(ctx.exception))

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
            await store.acquire_feeds_batch(
                _WORKER_ID,
                {
                    SourceType.BCFY_FEEDS: 1,
                    SourceType.BCFY_CALLS: 1,
                    SourceType.OPENMHZ: 1,
                },
            )

        self.assertIn(
            f"Unknown source type 'invalid_type' for feed {_FEED_ID}",
            str(ctx.exception),
        )


class TestBuildAcquireFeedsBatchSql(unittest.TestCase):
    """Tests for build_acquire_feeds_batch_sql pure helper."""

    def test_one_branch_per_claim_type(self) -> None:
        sql = feed_queries.build_acquire_feeds_batch_sql(
            [SourceType.BCFY_FEEDS]
        )
        self.assertEqual(
            sql.count("AS MATERIALIZED ("), 2
        )  # 1 branch + claimed

    def test_three_branches_for_production_set(self) -> None:
        sql = feed_queries.build_acquire_feeds_batch_sql(
            [
                SourceType.BCFY_FEEDS,
                SourceType.BCFY_CALLS,
                SourceType.OPENMHZ,
            ]
        )
        self.assertEqual(
            sql.count("AS MATERIALIZED ("), 4
        )  # 3 branches + claimed

    def test_param_count_matches_claim_types(self) -> None:
        """N claim_types → LIMIT $2..$(1+N) appears in SQL."""
        sql = feed_queries.build_acquire_feeds_batch_sql(
            [
                SourceType.BCFY_FEEDS,
                SourceType.BCFY_CALLS,
                SourceType.OPENMHZ,
            ]
        )
        self.assertIn("LIMIT $2", sql)
        self.assertIn("LIMIT $3", sql)
        self.assertIn("LIMIT $4", sql)
        self.assertNotIn("LIMIT $5", sql)

    def test_source_type_literals_inlined(self) -> None:
        sql = feed_queries.build_acquire_feeds_batch_sql(
            [
                SourceType.BCFY_FEEDS,
                SourceType.BCFY_CALLS,
                SourceType.OPENMHZ,
            ]
        )
        self.assertIn("source_type = 'bcfy_feeds'", sql)
        self.assertIn("source_type = 'bcfy_calls'", sql)
        self.assertIn("source_type = 'openmhz'", sql)

    def test_deterministic(self) -> None:
        types = [SourceType.BCFY_FEEDS, SourceType.OPENMHZ]
        self.assertEqual(
            feed_queries.build_acquire_feeds_batch_sql(types),
            feed_queries.build_acquire_feeds_batch_sql(types),
        )

    def test_empty_claim_types_raises(self) -> None:
        with self.assertRaises(ValueError):
            feed_queries.build_acquire_feeds_batch_sql([])

    def test_byte_identical_to_golden_for_production_set(self) -> None:
        """Production claim_types order produces a known-good SQL string.

        Guards against accidental SQL formatting drift during future
        refactors. The SQL shape (whitespace, indentation, branch
        ordering, parameter numbering) is load-bearing — the planner
        chooses the (source_type, id) WHERE status='unclaimed' partial
        composite index based on the literal source_type per branch, and
        the DB-side prepared-statement cache keys on the SQL string.
        """
        expected = (
            "WITH\n"
            "    bcfy_feeds_claim AS MATERIALIZED (\n"
            "        SELECT id FROM feeds\n"
            "        WHERE source_type = 'bcfy_feeds' AND status = 'unclaimed'::feed_status\n"
            "        ORDER BY id\n"
            "        LIMIT $2\n"
            "        FOR NO KEY UPDATE SKIP LOCKED\n"
            "    ),\n"
            "    bcfy_calls_claim AS MATERIALIZED (\n"
            "        SELECT id FROM feeds\n"
            "        WHERE source_type = 'bcfy_calls' AND status = 'unclaimed'::feed_status\n"
            "        ORDER BY id\n"
            "        LIMIT $3\n"
            "        FOR NO KEY UPDATE SKIP LOCKED\n"
            "    ),\n"
            "    openmhz_claim AS MATERIALIZED (\n"
            "        SELECT id FROM feeds\n"
            "        WHERE source_type = 'openmhz' AND status = 'unclaimed'::feed_status\n"
            "        ORDER BY id\n"
            "        LIMIT $4\n"
            "        FOR NO KEY UPDATE SKIP LOCKED\n"
            "    ),\n"
            "    claimed AS MATERIALIZED (\n"
            "        SELECT id FROM bcfy_feeds_claim\n"
            "        UNION ALL\n"
            "        SELECT id FROM bcfy_calls_claim\n"
            "        UNION ALL\n"
            "        SELECT id FROM openmhz_claim\n"
            "    ),\n"
            "leased AS (\n"
            "    UPDATE feeds\n"
            "    SET status = 'active'::feed_status,\n"
            "        worker_id = $1,\n"
            "        fencing_token = fencing_token + 1,\n"
            "        last_heartbeat = NOW(),\n"
            "        retry_after = NULL\n"
            "    FROM claimed\n"
            "    WHERE feeds.id = claimed.id\n"
            "    RETURNING feeds.id, feeds.name, feeds.source_type,\n"
            "              feeds.last_processed_filename, feeds.last_bookmark_time,\n"
            "              feeds.fencing_token\n"
            ")\n"
            "SELECT leased.id, leased.name, leased.source_type,\n"
            "       leased.last_processed_filename, leased.last_bookmark_time,\n"
            "       leased.fencing_token, fpi.source_feed_id, fpi.external_id\n"
            "FROM leased\n"
            "JOIN feed_properties fpi ON fpi.feed_id = leased.id\n"
        )
        actual = feed_queries.build_acquire_feeds_batch_sql(
            [
                SourceType.BCFY_FEEDS,
                SourceType.BCFY_CALLS,
                SourceType.OPENMHZ,
            ]
        )
        self.assertEqual(actual, expected)


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


class TestRowToLeasedFeed(unittest.TestCase):
    """Tests for the shared row-to-LeasedFeed mapping helper."""

    def test_returns_leased_feed_from_valid_row(self) -> None:
        store = FeedStore(_make_pool())

        # asyncpg.Record exposes __getitem__ like a dict; tests pass a
        # dict literal that quacks like Record. Cast tells the type
        # checker we know what we're doing — runtime is unaffected.
        result = store._row_to_leased_feed(cast("asyncpg.Record", _LEASE_ROW))

        self.assertEqual(result["id"], _FEED_ID)
        self.assertEqual(result["name"], "My Feed")
        self.assertEqual(result["source_type"], SourceType.BCFY_FEEDS)
        self.assertEqual(result["fencing_token"], 1)

    def test_invalid_source_type_raises(self) -> None:
        bad_row = {**_LEASE_ROW, "source_type": "not_a_real_type"}
        store = FeedStore(_make_pool())

        with self.assertRaises(ValueError) as context:
            store._row_to_leased_feed(cast("asyncpg.Record", bad_row))

        self.assertIn(
            "Unknown source type 'not_a_real_type'", str(context.exception)
        )


class TestAcquireFeedsRecovery(unittest.IsolatedAsyncioTestCase):
    """Tests for FeedStore.acquire_feeds_recovery."""

    async def test_all_zero_limits_skip_pool(self) -> None:
        """All-zero limits dict returns [] without touching the pool."""
        pool = _make_pool()
        store = FeedStore(pool)

        # Build an all-zero dict over the store's claim_types only —
        # passing ECHO (not in the default claim_types) would be rejected
        # by the unknown-key validation regardless of value.
        zeros = dict.fromkeys(store._claim_types, 0)
        result = await store.acquire_feeds_recovery(_WORKER_ID, 60.0, zeros)

        self.assertEqual(result, [])
        pool.fetch.assert_not_called()

    async def test_empty_limits_dict_skips_pool(self) -> None:
        """Empty limits dict returns [] without touching the pool."""
        pool = _make_pool()
        store = FeedStore(pool)

        result = await store.acquire_feeds_recovery(_WORKER_ID, 60.0, {})

        self.assertEqual(result, [])
        pool.fetch.assert_not_called()

    async def test_passes_positional_in_claim_types_order(self) -> None:
        """worker_id, abandonment_interval, then per-type LIMITs in claim_types order."""
        pool = _make_pool(fetch_result=[])
        store = FeedStore(
            pool,
            claim_types=[
                SourceType.BCFY_FEEDS,
                SourceType.BCFY_CALLS,
                SourceType.OPENMHZ,
            ],
        )

        await store.acquire_feeds_recovery(
            _WORKER_ID,
            60.0,
            {
                SourceType.BCFY_FEEDS: 2,
                SourceType.BCFY_CALLS: 3,
                SourceType.OPENMHZ: 5,
            },
        )

        args = pool.fetch.call_args[0]
        # args[0] is the generated recovery SQL string.
        self.assertIsInstance(args[0], str)
        self.assertEqual(args[1], _WORKER_ID)
        self.assertEqual(args[2], datetime.timedelta(seconds=60.0))
        self.assertEqual(args[3], 2)  # BCFY_FEEDS recovery LIMIT
        self.assertEqual(args[4], 3)  # BCFY_CALLS recovery LIMIT
        self.assertEqual(args[5], 5)  # OPENMHZ recovery LIMIT

    async def test_absent_limit_key_treated_as_zero(self) -> None:
        """Types absent from limits dict pass 0 to the SQL."""
        pool = _make_pool(fetch_result=[])
        store = FeedStore(
            pool,
            claim_types=[
                SourceType.BCFY_FEEDS,
                SourceType.BCFY_CALLS,
                SourceType.OPENMHZ,
            ],
        )

        await store.acquire_feeds_recovery(
            _WORKER_ID,
            60.0,
            {SourceType.BCFY_FEEDS: 5},
        )

        args = pool.fetch.call_args[0]
        self.assertEqual(args[3], 5)
        self.assertEqual(args[4], 0)
        self.assertEqual(args[5], 0)

    async def test_raises_on_unknown_limit_key(self) -> None:
        """A SourceType not in claim_types raises ValueError."""
        pool = _make_pool(fetch_result=[])
        store = FeedStore(pool, claim_types=[SourceType.BCFY_FEEDS])

        with self.assertRaises(ValueError) as ctx:
            await store.acquire_feeds_recovery(
                _WORKER_ID,
                60.0,
                {SourceType.OPENMHZ: 1},
            )
        self.assertIn("openmhz", str(ctx.exception))

    async def test_returns_leased_feeds(self) -> None:
        """Rows are converted to LeasedFeed dicts via the shared helper."""
        pool = _make_pool(fetch_result=[_LEASE_ROW])
        store = FeedStore(pool)

        result = await store.acquire_feeds_recovery(
            _WORKER_ID,
            60.0,
            {SourceType.BCFY_FEEDS: 10},
        )

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["id"], _FEED_ID)


class TestBuildAcquireFeedsRecoverySql(unittest.TestCase):
    """Tests for build_acquire_feeds_recovery_sql pure helper."""

    def test_one_branch_per_claim_type(self) -> None:
        sql = feed_queries.build_acquire_feeds_recovery_sql(
            [SourceType.BCFY_FEEDS]
        )
        # 1 _recovery branch + 1 recovered = 2 MATERIALIZED.
        self.assertEqual(sql.count("AS MATERIALIZED ("), 2)
        self.assertIn("bcfy_feeds_recovery AS MATERIALIZED", sql)

    def test_three_branches_for_production_set(self) -> None:
        sql = feed_queries.build_acquire_feeds_recovery_sql(
            [
                SourceType.BCFY_FEEDS,
                SourceType.BCFY_CALLS,
                SourceType.OPENMHZ,
            ]
        )
        self.assertEqual(sql.count("AS MATERIALIZED ("), 4)

    def test_param_count_matches_claim_types(self) -> None:
        """N claim_types → LIMIT $3..$(2+N) appears in SQL."""
        sql = feed_queries.build_acquire_feeds_recovery_sql(
            [
                SourceType.BCFY_FEEDS,
                SourceType.BCFY_CALLS,
                SourceType.OPENMHZ,
            ]
        )
        self.assertIn("LIMIT $3", sql)
        self.assertIn("LIMIT $4", sql)
        self.assertIn("LIMIT $5", sql)
        self.assertNotIn("LIMIT $6", sql)
        # $2 is the abandonment interval.
        self.assertIn("$2::interval", sql)

    def test_each_branch_filters_failing_or_active_abandoned(self) -> None:
        sql = feed_queries.build_acquire_feeds_recovery_sql(
            [SourceType.BCFY_FEEDS]
        )
        self.assertIn("status = 'failing'::feed_status", sql)
        self.assertIn("status = 'active'::feed_status", sql)
        self.assertIn("retry_after IS NULL OR retry_after <= NOW()", sql)
        self.assertIn("last_heartbeat < NOW() - $2::interval", sql)

    def test_source_type_literals_inlined(self) -> None:
        sql = feed_queries.build_acquire_feeds_recovery_sql(
            [SourceType.BCFY_FEEDS, SourceType.OPENMHZ]
        )
        self.assertIn("source_type = 'bcfy_feeds'", sql)
        self.assertIn("source_type = 'openmhz'", sql)
        # bcfy_calls is absent — no branch generated, so no filter.
        self.assertNotIn("source_type = 'bcfy_calls'", sql)

    def test_empty_claim_types_raises(self) -> None:
        with self.assertRaises(ValueError):
            feed_queries.build_acquire_feeds_recovery_sql([])


class TestCountHeldByType(unittest.IsolatedAsyncioTestCase):
    """Tests for FeedStore.count_held_by_type."""

    async def test_returns_counts_for_returned_source_types(self) -> None:
        """Returned rows populate the corresponding SourceType keys."""
        pool = _make_pool(
            fetch_result=[
                {"source_type": "bcfy_feeds", "n": 12},
                {"source_type": "bcfy_calls", "n": 7},
            ],
        )
        store = FeedStore(pool)

        result = await store.count_held_by_type(_WORKER_ID)

        self.assertEqual(result[SourceType.BCFY_FEEDS], 12)
        self.assertEqual(result[SourceType.BCFY_CALLS], 7)

    async def test_returns_zeros_for_absent_source_types(self) -> None:
        """Every SourceType key is present in output, even if not in rows."""
        pool = _make_pool(
            fetch_result=[
                {"source_type": "bcfy_feeds", "n": 3},
            ],
        )
        store = FeedStore(pool)

        result = await store.count_held_by_type(_WORKER_ID)

        # Every SourceType is keyed, with 0 for unreturned types.
        for source_type in SourceType:
            self.assertIn(source_type, result)
        self.assertEqual(result[SourceType.BCFY_FEEDS], 3)
        self.assertEqual(result[SourceType.BCFY_CALLS], 0)
        self.assertEqual(result[SourceType.OPENMHZ], 0)
        self.assertEqual(result[SourceType.ECHO], 0)

    async def test_skips_unknown_source_type_rows(self) -> None:
        """Bogus source_type strings are silently skipped, not raised."""
        pool = _make_pool(
            fetch_result=[
                {"source_type": "bcfy_feeds", "n": 4},
                {"source_type": "future_type_not_in_enum", "n": 99},
            ],
        )
        store = FeedStore(pool)

        result = await store.count_held_by_type(_WORKER_ID)

        # The known type populates; the unknown row is dropped — output
        # contains only valid SourceType keys, all integer values.
        self.assertEqual(result[SourceType.BCFY_FEEDS], 4)
        for value in result.values():
            self.assertIsInstance(value, int)

    async def test_empty_db_result_returns_all_zeros(self) -> None:
        """No rows → dict has every SourceType mapped to 0."""
        pool = _make_pool(fetch_result=[])
        store = FeedStore(pool)

        result = await store.count_held_by_type(_WORKER_ID)

        self.assertEqual(set(result.keys()), set(SourceType))
        self.assertTrue(all(v == 0 for v in result.values()))

    async def test_passes_worker_id_as_param(self) -> None:
        """Worker ID is forwarded as the only SQL parameter."""
        pool = _make_pool(fetch_result=[])
        store = FeedStore(pool)

        await store.count_held_by_type(_WORKER_ID)

        args = pool.fetch.call_args[0]
        self.assertIs(args[0], feed_queries.COUNT_HELD_BY_TYPE_SQL)
        self.assertEqual(args[1], _WORKER_ID)


class TestReleaseFeedsBatch(unittest.IsolatedAsyncioTestCase):
    """Tests for FeedStore.release_feeds_batch."""

    async def test_passes_worker_id(self) -> None:
        pool = _make_pool(execute_result="UPDATE 2")
        store = FeedStore(pool)

        result = await store.release_feeds_batch(_WORKER_ID)

        self.assertEqual(result, 2)
        args = pool.execute.call_args[0]
        self.assertIs(args[0], feed_queries.RELEASE_FEEDS_BATCH_SQL)
        self.assertEqual(args[1], _WORKER_ID)

    async def test_parses_update_count(self) -> None:
        pool = _make_pool(execute_result="UPDATE 7")
        store = FeedStore(pool)

        result = await store.release_feeds_batch(_WORKER_ID)

        self.assertEqual(result, 7)

    async def test_returns_zero_for_unparseable_result(self) -> None:
        pool = _make_pool(execute_result="ROLLBACK")
        store = FeedStore(pool)

        result = await store.release_feeds_batch(_WORKER_ID)

        self.assertEqual(result, 0)


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

    async def test_create_feed_with_tags(self) -> None:
        """Tags are passed to the SQL and returned in the Feed."""
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
            "tags": '[{"key": "env", "value": "prod"}]',
        }
        pool = _make_pool(fetchrow_result=row)
        store = FeedStore(pool)

        tags = [{"key": "env", "value": "prod"}]
        result = await store.create_feed(
            "New Feed", "bcfy_feeds", "123", "ext_123", tags=tags
        )

        self.assertEqual(result["tags"], tags)
        args = pool.fetchrow.call_args[0]
        self.assertEqual(args[5], json.dumps(tags))

    async def test_create_feed_invalid_tags(self) -> None:
        """CheckViolationError is raised when DB constraint fails for invalid tags."""
        pool = _make_pool()
        pool.fetchrow.side_effect = asyncpg.CheckViolationError(
            "valid_tags_schema"
        )
        store = FeedStore(pool)

        tags = [{"invalid": "shape"}]
        with self.assertRaises(asyncpg.CheckViolationError):
            await store.create_feed(
                "New Feed", "bcfy_feeds", "123", "ext_123", tags=tags
            )

    async def test_raises_value_error_on_failure(self) -> None:
        """ValueError is raised if the DB returns no row."""
        pool = _make_pool(fetchrow_result=None)
        store = FeedStore(pool)

        with self.assertRaises(ValueError):
            await store.create_feed("New Feed", "bcfy_feeds", "123", "ext_123")

    async def test_create_feed_invalid_source_type(self) -> None:
        """ValueError is raised when an invalid source type is passed."""
        pool = _make_pool()
        store = FeedStore(pool)

        with self.assertRaises(ValueError) as cm:
            await store.create_feed(
                name="Test Feed",
                source_type="invalid_type",
                source_feed_id="src_123",
                external_id="ext_123",
            )
        self.assertIn("Invalid source type", str(cm.exception))


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

    async def test_get_feed_returns_tags(self) -> None:
        """Tags are returned in the Feed dict when they exist."""
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
            "tags": '[{"key": "county", "value": "Fulton"}]',
        }
        pool = _make_pool(fetchrow_result=row)
        store = FeedStore(pool)

        result = await store.get_feed(_FEED_ID)

        assert result is not None
        self.assertEqual(result["tags"], [{"key": "county", "value": "Fulton"}])

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

    async def test_list_feeds_returns_tags(self) -> None:
        """Tags are returned in the Feed dicts when they exist."""
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
                "tags": '[{"key": "county", "value": "Fulton"}]',
            },
        ]
        pool = _make_pool(fetch_result=rows)
        store = FeedStore(pool)

        result = await store.list_feeds()

        self.assertEqual(len(result), 1)
        self.assertEqual(
            result[0]["tags"], [{"key": "county", "value": "Fulton"}]
        )


class TestDeactivateFeed(unittest.IsolatedAsyncioTestCase):
    """Tests for FeedStore.deactivate_feed."""

    async def test_delete_succeeds(self) -> None:
        """True is returned when a feed is deactivated."""
        pool = _make_pool(execute_result="UPDATE 1")
        store = FeedStore(pool)

        result = await store.deactivate_feed(_FEED_ID)

        self.assertTrue(result)
        pool.execute.assert_called_once_with(
            feed_queries.DEACTIVATE_FEED_SQL, _FEED_ID
        )

    async def test_delete_fails_when_not_found(self) -> None:
        """False is returned when no feed is deactivated."""
        pool = _make_pool(execute_result="UPDATE 0")
        store = FeedStore(pool)

        result = await store.deactivate_feed(_FEED_ID)

        self.assertFalse(result)


class TestResetFeed(unittest.IsolatedAsyncioTestCase):
    """Tests for FeedStore.reset_feed."""

    async def test_reset_succeeds(self) -> None:
        """The feed is reset successfully."""
        row = {
            "id": _FEED_ID,
            "name": "My Feed",
            "source_type": "bcfy_feeds",
            "status": "quarantined",
            "failure_count": 5,
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

        result = await store.reset_feed(_FEED_ID)

        self.assertIsNotNone(result)
        pool.fetchrow.assert_called_once_with(
            feed_queries.RESET_FEED_SQL, _FEED_ID
        )

    async def test_reset_fails_when_not_found(self) -> None:
        """None is returned when no feed is found."""
        pool = _make_pool(fetchrow_result=None)
        store = FeedStore(pool)

        result = await store.reset_feed(_FEED_ID)

        self.assertIsNone(result)


if __name__ == "__main__":
    unittest.main()
