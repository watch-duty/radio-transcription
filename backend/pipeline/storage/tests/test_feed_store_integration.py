from __future__ import annotations

import asyncio
import unittest
import uuid
from pathlib import Path

import asyncpg
import docker
from testcontainers.postgres import PostgresContainer

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
        self.pool = await asyncpg.create_pool(
            host=self._host,
            port=self._port,
            user="postgres",
            password="postgres",
            database="postgres",
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
            "SELECT status, failure_count, worker_id FROM feeds WHERE id = $1::uuid",
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

        await self.store.report_feed_failure(feed_id, worker)

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

        await self.store.report_feed_failure(feed_id, worker)

        row = await self._get_feed_status(feed_id)
        self.assertEqual(row["status"], "quarantined")
        self.assertEqual(row["failure_count"], 3)

    # -- Tests: renew_heartbeat -------------------------------------------

    async def test_renew_heartbeat_succeeds_for_active_feed(self) -> None:
        """Heartbeat renewal returns True and updates the timestamp."""
        worker = uuid.uuid4()
        feed_id = await self._insert_feed(
            "My Feed",
            status="active",
            worker_id=worker,
            last_heartbeat_age_seconds=30,
        )

        old_hb = await self.pool.fetchval(
            "SELECT last_heartbeat FROM feeds WHERE id = $1::uuid",
            str(feed_id),
        )

        result = await self.store.renew_heartbeat(feed_id, worker)

        self.assertTrue(result)

        new_hb = await self.pool.fetchval(
            "SELECT last_heartbeat FROM feeds WHERE id = $1::uuid",
            str(feed_id),
        )
        self.assertGreater(new_hb, old_hb)

    async def test_renew_heartbeat_returns_false_for_stolen_lease(self) -> None:
        """Renewal returns False when a different worker owns the feed."""
        owner = uuid.uuid4()
        impostor = uuid.uuid4()
        feed_id = await self._insert_feed(
            "Stolen Feed",
            status="active",
            worker_id=owner,
            last_heartbeat_age_seconds=10,
        )

        result = await self.store.renew_heartbeat(feed_id, impostor)

        self.assertFalse(result)

    # -- Tests: renew_heartbeats_batch ------------------------------------

    async def test_batch_renew_returns_all_active_feeds(self) -> None:
        """All feeds owned by the worker are returned in the renewed set."""
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

        result = await self.store.renew_heartbeats_batch([feed_a, feed_b], worker)

        self.assertEqual(result, {feed_a, feed_b})

    async def test_batch_renew_excludes_stolen_feeds(self) -> None:
        """Only feeds still owned by the worker are returned."""
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

        result = await self.store.renew_heartbeats_batch(
            [owned_feed, stolen_feed],
            worker,
        )

        self.assertEqual(result, {owned_feed})

    async def test_batch_renew_returns_empty_set_for_empty_input(self) -> None:
        """Empty input returns empty set without hitting the database."""
        result = await self.store.renew_heartbeats_batch([], uuid.uuid4())

        self.assertEqual(result, set())

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

        result = await self.store.release_feed(feed_id, worker)

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

        result = await self.store.release_feed(feed_id, uuid.uuid4())

        self.assertFalse(result)


if __name__ == "__main__":
    unittest.main()
