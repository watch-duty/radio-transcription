from __future__ import annotations

import asyncio
import unittest
import uuid
from pathlib import Path

import asyncpg
import docker
from testcontainers.postgres import PostgresContainer

from backend.pipeline.storage.connection import create_pool
from backend.pipeline.storage.feed_store import FeedStore

_REPO_ROOT = Path(__file__).resolve().parents[4]
_SQL_DIR = _REPO_ROOT / "terraform" / "modules" / "alloydb" / "sql" / "ingestion"


def _docker_available() -> bool:
    try:
        docker.from_env().ping()
    except Exception:
        return False
    return True


@unittest.skipUnless(_docker_available(), "Docker is not available")
class TestFeedStoreIntegration(unittest.IsolatedAsyncioTestCase):
    """Integration tests for FeedStore against AlloyDB Omni."""

    container: PostgresContainer
    pool: asyncpg.Pool

    @classmethod
    def setUpClass(cls) -> None:
        """Start AlloyDB Omni container and apply schema."""
        cls.container = PostgresContainer(
            image="google/alloydbomni:15",
            username="postgres",
            password="postgres",
            dbname="postgres",
            driver=None,
        )
        cls.container.start()

        # Apply schema using a temporary synchronous connection via asyncpg.
        cls._host = cls.container.get_container_host_ip()
        cls._port = int(cls.container.get_exposed_port(5432))

        async def _setup_schema() -> None:
            conn = await asyncpg.connect(
                host=cls._host,
                port=cls._port,
                user="postgres",
                password="postgres",
                database="postgres",
            )
            for sql_file in sorted(_SQL_DIR.glob("*.sql")):
                await conn.execute(sql_file.read_text())
            await conn.close()

        asyncio.run(_setup_schema())

    @classmethod
    def tearDownClass(cls) -> None:
        """Stop container."""
        cls.container.stop()

    async def asyncSetUp(self) -> None:
        """Create pool, truncate feeds, and set up store."""
        self.pool = await create_pool(
            host=self._host,
            port=self._port,
            user="postgres",
            password="postgres",
            db_name="postgres",
            min_size=2,
            max_size=5,
        )
        await self.pool.execute("TRUNCATE feeds CASCADE")
        self.store = FeedStore(self.pool)

    async def asyncTearDown(self) -> None:
        """Close pool."""
        await self.pool.close()

    # -- Helpers ----------------------------------------------------------

    async def _insert_feed(
        self,
        name: str,
        source_type: str = "bcfy_feeds",
        *,
        status: str = "unclaimed",
        failure_count: int = 0,
        worker_id: uuid.UUID | None = None,
        last_heartbeat_age_seconds: int | None = None,
        stream_url: str | None = None,
    ) -> uuid.UUID:
        """Insert a feed row and optionally an icecast properties row."""
        heartbeat_expr = "NULL"
        if last_heartbeat_age_seconds is not None:
            heartbeat_expr = f"NOW() - INTERVAL '{last_heartbeat_age_seconds} seconds'"

        feed_id = await self.pool.fetchval(
            f"INSERT INTO feeds (name, source_type, status, failure_count,"
            f" worker_id, last_heartbeat)"
            f" VALUES ($1, $2, $3::feed_status, $4, $5::uuid, {heartbeat_expr})"
            f" RETURNING id",
            name,
            source_type,
            status,
            failure_count,
            str(worker_id) if worker_id else None,
        )

        if stream_url is not None:
            await self.pool.execute(
                "INSERT INTO feed_properties_icecast (feed_id, stream_url) "
                "VALUES ($1::uuid, $2)",
                str(feed_id),
                stream_url,
            )

        return feed_id

    async def _get_feed_status(self, feed_id: uuid.UUID) -> dict:
        """Read a feed row back from the database."""
        row = await self.pool.fetchrow(
            "SELECT status, failure_count, worker_id, fencing_token FROM feeds WHERE id = $1::uuid",
            str(feed_id),
        )
        if row is None:
            msg = "Expected a row from query"
            raise AssertionError(msg)
        return dict(row)

    # -- Tests: lease_feed ------------------------------------------------

    async def test_lease_returns_feed_with_icecast_properties(self) -> None:
        """Leased feed includes stream_url from LEFT JOIN."""
        worker = uuid.uuid4()
        await self._insert_feed(
            "Icecast Feed",
            stream_url="http://stream.example.com/live",
        )

        result = await self.store.lease_feed(worker)

        if result is None:
            msg = "Expected a LeasedFeed, got None"
            raise AssertionError(msg)
        self.assertEqual(result["name"], "Icecast Feed")
        self.assertEqual(result["source_type"], "bcfy_feeds")
        self.assertEqual(
            result["stream_url"],
            "http://stream.example.com/live",
        )
        self.assertEqual(result["fencing_token"], 1)

    async def test_lease_returns_feed_without_icecast_properties(self) -> None:
        """Non-icecast feed has stream_url=None."""
        worker = uuid.uuid4()
        await self._insert_feed("API Feed", source_type="bcfy_calls")

        result = await self.store.lease_feed(worker)

        if result is None:
            msg = "Expected a LeasedFeed, got None"
            raise AssertionError(msg)
        self.assertEqual(result["name"], "API Feed")
        self.assertIsNone(result["stream_url"])
        self.assertEqual(result["fencing_token"], 1)

    async def test_lease_returns_none_when_no_feeds(self) -> None:
        """Empty database returns None."""
        result = await self.store.lease_feed(uuid.uuid4())
        self.assertIsNone(result)

    async def test_lease_prioritizes_unclaimed_over_failing(self) -> None:
        """Unclaimed feeds are leased before failing feeds."""
        worker = uuid.uuid4()
        await self._insert_feed(
            "Failing Feed",
            status="failing",
            failure_count=1,
            last_heartbeat_age_seconds=120,
        )
        await self._insert_feed("Unclaimed Feed")

        result = await self.store.lease_feed(worker)

        if result is None:
            msg = "Expected a LeasedFeed, got None"
            raise AssertionError(msg)
        self.assertEqual(result["name"], "Unclaimed Feed")

    async def test_lease_skips_active_feeds_with_fresh_heartbeat(self) -> None:
        """Active feed with a recent heartbeat is not available."""
        worker = uuid.uuid4()
        other_worker = uuid.uuid4()
        await self._insert_feed(
            "Active Feed",
            status="active",
            worker_id=other_worker,
            last_heartbeat_age_seconds=10,
        )

        result = await self.store.lease_feed(worker)

        self.assertIsNone(result)

    async def test_lease_reclaims_stale_active_feed(self) -> None:
        """Active feed with heartbeat older than 60s becomes available."""
        worker = uuid.uuid4()
        old_worker = uuid.uuid4()
        await self._insert_feed(
            "Stale Feed",
            status="active",
            worker_id=old_worker,
            last_heartbeat_age_seconds=120,
        )

        result = await self.store.lease_feed(worker)

        if result is None:
            msg = "Expected a LeasedFeed, got None"
            raise AssertionError(msg)
        self.assertEqual(result["name"], "Stale Feed")

    async def test_concurrent_leases_get_different_feeds(self) -> None:
        """Two sequential leases return different feeds (SKIP LOCKED)."""
        await self._insert_feed("Feed A")
        await self._insert_feed("Feed B")

        result_a = await self.store.lease_feed(uuid.uuid4())
        result_b = await self.store.lease_feed(uuid.uuid4())

        if result_a is None:
            msg = "Expected a LeasedFeed for result_a, got None"
            raise AssertionError(msg)
        if result_b is None:
            msg = "Expected a LeasedFeed for result_b, got None"
            raise AssertionError(msg)
        self.assertNotEqual(result_a["id"], result_b["id"])

    # -- Tests: update_feed_progress --------------------------------------

    async def test_progress_update_succeeds_with_correct_worker(self) -> None:
        """Fenced update returns True and resets failure_count."""
        worker = uuid.uuid4()
        feed_id = await self._insert_feed(
            "My Feed",
            status="active",
            worker_id=worker,
            last_heartbeat_age_seconds=10,
            failure_count=1,
        )

        result = await self.store.update_feed_progress(
            feed_id,
            worker,
            "gs://bucket/path/file.ogg",
            0,
        )

        self.assertTrue(result)
        row = await self._get_feed_status(feed_id)
        self.assertEqual(row["failure_count"], 0)

    async def test_progress_update_fails_with_wrong_worker(self) -> None:
        """Wrong worker_id returns False (lease lost)."""
        worker = uuid.uuid4()
        feed_id = await self._insert_feed(
            "My Feed",
            status="active",
            worker_id=worker,
            last_heartbeat_age_seconds=10,
        )

        result = await self.store.update_feed_progress(
            feed_id,
            uuid.uuid4(),
            "gs://bucket/path/file.ogg",
            0,
        )

        self.assertFalse(result)

    # -- Tests: report_feed_failure ---------------------------------------

    async def test_failure_sets_status_to_failing(self) -> None:
        """First failure transitions to 'failing' and releases the lease."""
        worker = uuid.uuid4()
        feed_id = await self._insert_feed(
            "My Feed",
            status="active",
            worker_id=worker,
            last_heartbeat_age_seconds=10,
        )

        await self.store.report_feed_failure(feed_id, worker, 0)

        row = await self._get_feed_status(feed_id)
        self.assertEqual(row["status"], "failing")
        self.assertEqual(row["failure_count"], 1)
        self.assertIsNone(row["worker_id"])

    async def test_failure_escalation_to_quarantine(self) -> None:
        """Third failure transitions to 'quarantined'."""
        worker = uuid.uuid4()
        feed_id = await self._insert_feed(
            "My Feed",
            status="active",
            worker_id=worker,
            last_heartbeat_age_seconds=10,
            failure_count=2,
        )

        await self.store.report_feed_failure(feed_id, worker, 0)

        row = await self._get_feed_status(feed_id)
        self.assertEqual(row["status"], "quarantined")
        self.assertEqual(row["failure_count"], 3)

    async def test_failing_feed_not_leased_before_retry_after(self) -> None:
        """Feed with retry_after in the future is not returned by acquire."""
        worker = uuid.uuid4()
        feed_id = await self._insert_feed(
            "Backoff Feed",
            status="active",
            worker_id=worker,
            last_heartbeat_age_seconds=10,
        )
        await self.store.report_feed_failure(feed_id, worker, 0)
        # retry_after is 30s in the future — feed should NOT be returned
        result = await self.store.lease_feed(uuid.uuid4())
        self.assertIsNone(result)

    async def test_failing_feed_leased_after_retry_after_expires(self) -> None:
        """Feed is returned by acquire after retry_after passes."""
        worker = uuid.uuid4()
        feed_id = await self._insert_feed(
            "Backoff Feed",
            status="active",
            worker_id=worker,
            last_heartbeat_age_seconds=10,
        )
        await self.store.report_feed_failure(feed_id, worker, 0)
        # Manually expire the backoff
        await self.pool.execute(
            "UPDATE feeds SET retry_after = NOW() - INTERVAL '1 second' WHERE id = $1",
            feed_id,
        )
        result = await self.store.lease_feed(uuid.uuid4())
        self.assertIsNotNone(result)
        self.assertEqual(result["id"], feed_id)

    async def test_lease_preserves_failure_count(self) -> None:
        """Leasing clears retry_after but does NOT reset failure_count."""
        worker = uuid.uuid4()
        feed_id = await self._insert_feed(
            "Reset Feed",
            status="active",
            worker_id=worker,
            last_heartbeat_age_seconds=10,
        )
        await self.store.report_feed_failure(feed_id, worker, 0)
        # Expire backoff so feed can be re-leased
        await self.pool.execute(
            "UPDATE feeds SET retry_after = NOW() - INTERVAL '1 second' WHERE id = $1",
            feed_id,
        )
        new_worker = uuid.uuid4()
        result = await self.store.lease_feed(new_worker)
        self.assertIsNotNone(result)
        row = await self.pool.fetchrow(
            "SELECT failure_count, retry_after FROM feeds WHERE id = $1",
            feed_id,
        )
        # failure_count preserved (only reset by successful processing)
        self.assertEqual(row["failure_count"], 1)
        # retry_after cleared on lease
        self.assertIsNone(row["retry_after"])

    async def test_successful_processing_resets_failure_count(self) -> None:
        """update_feed_progress resets failure_count to 0."""
        worker = uuid.uuid4()
        feed_id = await self._insert_feed(
            "Progress Feed",
            status="active",
            worker_id=worker,
            last_heartbeat_age_seconds=10,
        )
        await self.store.report_feed_failure(feed_id, worker, 0)
        # Expire backoff and re-lease
        await self.pool.execute(
            "UPDATE feeds SET retry_after = NOW() - INTERVAL '1 second' WHERE id = $1",
            feed_id,
        )
        new_worker = uuid.uuid4()
        result = await self.store.lease_feed(new_worker)
        self.assertIsNotNone(result)
        # Simulate successful processing
        await self.store.update_feed_progress(
            result["id"],
            new_worker,
            "chunk_001.flac",
            result["fencing_token"],
        )
        row = await self.pool.fetchrow(
            "SELECT failure_count FROM feeds WHERE id = $1",
            feed_id,
        )
        self.assertEqual(row["failure_count"], 0)

    # -- Tests: renew_heartbeats_batch_diagnostic --------------------------

    async def test_diagnostic_renew_returns_all_owned_feeds(self) -> None:
        """Owned feeds are returned with renewed=True and correct diagnostics."""
        worker = uuid.uuid4()
        feed_a = await self._insert_feed(
            "Feed A",
            status="active",
            worker_id=worker,
            last_heartbeat_age_seconds=30,
        )
        feed_b = await self._insert_feed(
            "Feed B",
            status="active",
            worker_id=worker,
            last_heartbeat_age_seconds=30,
        )

        results = await self.store.renew_heartbeats_batch_diagnostic(
            [feed_a, feed_b],
            worker,
        )

        self.assertEqual(len(results), 2)
        by_id = {r["id"]: r for r in results}
        self.assertTrue(by_id[feed_a]["renewed"])
        self.assertTrue(by_id[feed_b]["renewed"])
        self.assertEqual(by_id[feed_a]["current_worker"], worker)
        self.assertEqual(by_id[feed_a]["current_status"], "active")

    async def test_diagnostic_renew_stolen_feed(self) -> None:
        """Stolen feed returns renewed=False with the thief's worker_id."""
        worker = uuid.uuid4()
        other_worker = uuid.uuid4()
        owned_feed = await self._insert_feed(
            "Owned Feed",
            status="active",
            worker_id=worker,
            last_heartbeat_age_seconds=30,
        )
        stolen_feed = await self._insert_feed(
            "Stolen Feed",
            status="active",
            worker_id=other_worker,
            last_heartbeat_age_seconds=30,
        )

        results = await self.store.renew_heartbeats_batch_diagnostic(
            [owned_feed, stolen_feed],
            worker,
        )

        by_id = {r["id"]: r for r in results}
        self.assertTrue(by_id[owned_feed]["renewed"])
        self.assertFalse(by_id[stolen_feed]["renewed"])
        self.assertEqual(by_id[stolen_feed]["current_worker"], other_worker)

    async def test_diagnostic_renew_quarantined_feed(self) -> None:
        """Quarantined feed returns renewed=False with quarantined status."""
        worker = uuid.uuid4()
        feed_id = await self._insert_feed(
            "Quarantined Feed",
            status="quarantined",
            worker_id=None,
            failure_count=3,
        )

        results = await self.store.renew_heartbeats_batch_diagnostic(
            [feed_id],
            worker,
        )

        self.assertEqual(len(results), 1)
        self.assertFalse(results[0]["renewed"])
        self.assertEqual(results[0]["current_status"], "quarantined")
        self.assertIsNone(results[0]["current_worker"])

    async def test_diagnostic_renew_empty_input(self) -> None:
        """Empty input returns empty list without hitting the database."""
        results = await self.store.renew_heartbeats_batch_diagnostic(
            [],
            uuid.uuid4(),
        )

        self.assertEqual(results, [])

    # -- Tests: release_feed ----------------------------------------------

    async def test_release_feed_succeeds(self) -> None:
        """Release returns True and resets the feed to unclaimed."""
        worker = uuid.uuid4()
        feed_id = await self._insert_feed(
            "My Feed",
            status="active",
            worker_id=worker,
            last_heartbeat_age_seconds=10,
        )

        result = await self.store.release_feed(feed_id, worker, 0)

        self.assertTrue(result)
        row = await self._get_feed_status(feed_id)
        self.assertEqual(row["status"], "unclaimed")
        self.assertIsNone(row["worker_id"])

    async def test_release_feed_fails_with_wrong_worker(self) -> None:
        """Release returns False when a different worker owns the feed."""
        worker = uuid.uuid4()
        feed_id = await self._insert_feed(
            "My Feed",
            status="active",
            worker_id=worker,
            last_heartbeat_age_seconds=10,
        )

        result = await self.store.release_feed(feed_id, uuid.uuid4(), 0)

        self.assertFalse(result)

    # -- Tests: fencing_token ------------------------------------------------

    async def test_fencing_token_increments_on_each_lease(self) -> None:
        """Re-leasing a feed increments the fencing token."""
        worker = uuid.uuid4()
        await self._insert_feed("My Feed")

        result1 = await self.store.lease_feed(worker)
        assert result1 is not None
        self.assertEqual(result1["fencing_token"], 1)

        await self.store.release_feed(result1["id"], worker, 1)

        result2 = await self.store.lease_feed(worker)
        assert result2 is not None
        self.assertEqual(result2["fencing_token"], 2)

    async def test_progress_update_fails_with_wrong_fencing_token(self) -> None:
        """Correct worker_id but wrong fencing_token returns False."""
        worker = uuid.uuid4()
        feed_id = await self._insert_feed(
            "My Feed",
            status="active",
            worker_id=worker,
            last_heartbeat_age_seconds=10,
        )

        result = await self.store.update_feed_progress(
            feed_id,
            worker,
            "gs://bucket/path/file.ogg",
            999,  # wrong fencing_token
        )

        self.assertFalse(result)

    async def test_release_feed_fails_with_wrong_fencing_token(self) -> None:
        """Correct worker_id but wrong fencing_token returns False."""
        worker = uuid.uuid4()
        feed_id = await self._insert_feed(
            "My Feed",
            status="active",
            worker_id=worker,
            last_heartbeat_age_seconds=10,
        )

        result = await self.store.release_feed(feed_id, worker, 999)

        self.assertFalse(result)

    async def test_report_feed_failure_fails_with_wrong_fencing_token(self) -> None:
        """Correct worker_id but wrong fencing_token returns False."""
        worker = uuid.uuid4()
        feed_id = await self._insert_feed(
            "My Feed",
            status="active",
            worker_id=worker,
            last_heartbeat_age_seconds=10,
        )

        result = await self.store.report_feed_failure(feed_id, worker, 999)

        self.assertFalse(result)
        # Verify feed state unchanged (failure was rejected)
        row = await self._get_feed_status(feed_id)
        self.assertEqual(row["status"], "active")
        self.assertEqual(row["failure_count"], 0)


if __name__ == "__main__":
    unittest.main()
