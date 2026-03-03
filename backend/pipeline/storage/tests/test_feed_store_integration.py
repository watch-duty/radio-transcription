from __future__ import annotations

import unittest
import uuid
from pathlib import Path

import docker
import psycopg
from psycopg import sql
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
class TestFeedStoreIntegration(unittest.TestCase):
    """Integration tests for FeedStore against AlloyDB Omni."""

    container: PostgresContainer
    conn: psycopg.Connection

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

        cls.conn = psycopg.connect(
            host=cls.container.get_container_host_ip(),
            port=int(cls.container.get_exposed_port(5432)),
            user="postgres",
            password="postgres",
            dbname="postgres",
        )

        # Apply schema files in filename order.
        cursor = cls.conn.cursor()
        for sql_file in sorted(_SQL_DIR.glob("*.sql")):
            cursor.execute(sql_file.read_text())  # type: ignore[no-matching-overload]
        cls.conn.commit()
        cursor.close()

    @classmethod
    def tearDownClass(cls) -> None:
        """Stop container and close connection."""
        cls.conn.close()
        cls.container.stop()

    def setUp(self) -> None:
        """Truncate feeds (cascades to icecast properties) before each test."""
        self.conn.rollback()
        cursor = self.conn.cursor()
        cursor.execute("TRUNCATE feeds CASCADE")
        self.conn.commit()
        cursor.close()
        self.store = FeedStore(self.conn)

    # -- Helpers ----------------------------------------------------------

    def _insert_feed(  # noqa: PLR0913
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
        cursor = self.conn.cursor()

        heartbeat_sql: sql.Composable = sql.SQL("NULL")
        if last_heartbeat_age_seconds is not None:
            heartbeat_sql = sql.SQL("NOW() - INTERVAL {}").format(
                sql.Literal(f"{last_heartbeat_age_seconds} seconds"),
            )

        query = sql.SQL(
            "INSERT INTO feeds (name, source_type, status, failure_count,"
            " worker_id, last_heartbeat)"
            " VALUES (%s, %s, %s::feed_status, %s, %s::uuid, {heartbeat})"
            " RETURNING id",
        ).format(heartbeat=heartbeat_sql)

        cursor.execute(
            query,
            (
                name,
                source_type,
                status,
                failure_count,
                str(worker_id) if worker_id else None,
            ),
        )
        row = cursor.fetchone()
        if row is None:
            msg = "INSERT did not return a row"
            raise AssertionError(msg)
        feed_id = row[0]

        if stream_url is not None:
            cursor.execute(
                "INSERT INTO feed_properties_icecast (feed_id, stream_url) "
                "VALUES (%s::uuid, %s)",
                (str(feed_id), stream_url),
            )

        self.conn.commit()
        cursor.close()
        return feed_id

    def _get_feed_status(self, feed_id: uuid.UUID) -> dict:
        """Read a feed row back from the database."""
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT status, failure_count, worker_id FROM feeds WHERE id = %s::uuid",
            (str(feed_id),),
        )
        row = cursor.fetchone()
        if row is None:
            msg = "Expected a row from query"
            raise AssertionError(msg)
        if cursor.description is None:
            msg = "No column description available"
            raise AssertionError(msg)
        columns = [d[0] for d in cursor.description]
        cursor.close()
        return dict(zip(columns, row, strict=True))

    # -- Tests: lease_feed ------------------------------------------------

    def test_lease_returns_feed_with_icecast_properties(self) -> None:
        """Leased feed includes stream_url from LEFT JOIN."""
        worker = uuid.uuid4()
        self._insert_feed(
            "Icecast Feed",
            stream_url="http://stream.example.com/live",
        )

        result = self.store.lease_feed(worker)

        if result is None:
            msg = "Expected a LeasedFeed, got None"
            raise AssertionError(msg)
        self.assertEqual(result["name"], "Icecast Feed")
        self.assertEqual(result["source_type"], "bcfy_feeds")
        self.assertEqual(
            result["stream_url"],
            "http://stream.example.com/live",
        )

    def test_lease_returns_feed_without_icecast_properties(self) -> None:
        """Non-icecast feed has stream_url=None."""
        worker = uuid.uuid4()
        self._insert_feed("API Feed", source_type="bcfy_calls")

        result = self.store.lease_feed(worker)

        if result is None:
            msg = "Expected a LeasedFeed, got None"
            raise AssertionError(msg)
        self.assertEqual(result["name"], "API Feed")
        self.assertIsNone(result["stream_url"])

    def test_lease_returns_none_when_no_feeds(self) -> None:
        """Empty database returns None."""
        result = self.store.lease_feed(uuid.uuid4())
        self.assertIsNone(result)

    def test_lease_prioritizes_unclaimed_over_failing(self) -> None:
        """Unclaimed feeds are leased before failing feeds."""
        worker = uuid.uuid4()
        self._insert_feed(
            "Failing Feed",
            status="failing",
            failure_count=1,
            last_heartbeat_age_seconds=120,
        )
        self._insert_feed("Unclaimed Feed")

        result = self.store.lease_feed(worker)

        if result is None:
            msg = "Expected a LeasedFeed, got None"
            raise AssertionError(msg)
        self.assertEqual(result["name"], "Unclaimed Feed")

    def test_lease_skips_active_feeds_with_fresh_heartbeat(self) -> None:
        """Active feed with a recent heartbeat is not available."""
        worker = uuid.uuid4()
        other_worker = uuid.uuid4()
        self._insert_feed(
            "Active Feed",
            status="active",
            worker_id=other_worker,
            last_heartbeat_age_seconds=10,
        )

        result = self.store.lease_feed(worker)

        self.assertIsNone(result)

    def test_lease_reclaims_stale_active_feed(self) -> None:
        """Active feed with heartbeat older than 60s becomes available."""
        worker = uuid.uuid4()
        old_worker = uuid.uuid4()
        self._insert_feed(
            "Stale Feed",
            status="active",
            worker_id=old_worker,
            last_heartbeat_age_seconds=120,
        )

        result = self.store.lease_feed(worker)

        if result is None:
            msg = "Expected a LeasedFeed, got None"
            raise AssertionError(msg)
        self.assertEqual(result["name"], "Stale Feed")

    def test_concurrent_leases_get_different_feeds(self) -> None:
        """Two sequential leases return different feeds (SKIP LOCKED)."""
        self._insert_feed("Feed A")
        self._insert_feed("Feed B")

        result_a = self.store.lease_feed(uuid.uuid4())
        result_b = self.store.lease_feed(uuid.uuid4())

        if result_a is None:
            msg = "Expected a LeasedFeed for result_a, got None"
            raise AssertionError(msg)
        if result_b is None:
            msg = "Expected a LeasedFeed for result_b, got None"
            raise AssertionError(msg)
        self.assertNotEqual(result_a["id"], result_b["id"])

    # -- Tests: update_feed_progress --------------------------------------

    def test_progress_update_succeeds_with_correct_worker(self) -> None:
        """Fenced update returns True and resets failure_count."""
        worker = uuid.uuid4()
        feed_id = self._insert_feed(
            "My Feed",
            status="active",
            worker_id=worker,
            last_heartbeat_age_seconds=10,
            failure_count=1,
        )

        result = self.store.update_feed_progress(
            feed_id,
            worker,
            "gs://bucket/path/file.ogg",
        )

        self.assertTrue(result)
        row = self._get_feed_status(feed_id)
        self.assertEqual(row["failure_count"], 0)

    def test_progress_update_fails_with_wrong_worker(self) -> None:
        """Wrong worker_id returns False (lease lost)."""
        worker = uuid.uuid4()
        feed_id = self._insert_feed(
            "My Feed",
            status="active",
            worker_id=worker,
            last_heartbeat_age_seconds=10,
        )

        result = self.store.update_feed_progress(
            feed_id,
            uuid.uuid4(),
            "gs://bucket/path/file.ogg",
        )

        self.assertFalse(result)

    # -- Tests: report_feed_failure ---------------------------------------

    def test_failure_sets_status_to_failing(self) -> None:
        """First failure transitions to 'failing' and releases the lease."""
        worker = uuid.uuid4()
        feed_id = self._insert_feed(
            "My Feed",
            status="active",
            worker_id=worker,
            last_heartbeat_age_seconds=10,
        )

        self.store.report_feed_failure(feed_id, worker)

        row = self._get_feed_status(feed_id)
        self.assertEqual(row["status"], "failing")
        self.assertEqual(row["failure_count"], 1)
        self.assertIsNone(row["worker_id"])

    def test_failure_escalation_to_quarantine(self) -> None:
        """Third failure transitions to 'quarantined'."""
        worker = uuid.uuid4()
        feed_id = self._insert_feed(
            "My Feed",
            status="active",
            worker_id=worker,
            last_heartbeat_age_seconds=10,
            failure_count=2,
        )

        self.store.report_feed_failure(feed_id, worker)

        row = self._get_feed_status(feed_id)
        self.assertEqual(row["status"], "quarantined")
        self.assertEqual(row["failure_count"], 3)

    # -- Tests: renew_heartbeat -------------------------------------------

    def test_renew_heartbeat_succeeds_for_active_feed(self) -> None:
        """Heartbeat renewal returns True and updates the timestamp."""
        worker = uuid.uuid4()
        feed_id = self._insert_feed(
            "My Feed",
            status="active",
            worker_id=worker,
            last_heartbeat_age_seconds=30,
        )

        # Capture the heartbeat before renewal.
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT last_heartbeat FROM feeds WHERE id = %s::uuid",
            (str(feed_id),),
        )
        old_hb = cursor.fetchone()[0]  # type: ignore[index]
        cursor.close()

        result = self.store.renew_heartbeat(feed_id, worker)

        self.assertTrue(result)

        # Verify the heartbeat was updated.
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT last_heartbeat FROM feeds WHERE id = %s::uuid",
            (str(feed_id),),
        )
        new_hb = cursor.fetchone()[0]  # type: ignore[index]
        cursor.close()
        self.assertGreater(new_hb, old_hb)

    def test_renew_heartbeat_returns_false_for_stolen_lease(self) -> None:
        """Renewal returns False when a different worker owns the feed."""
        owner = uuid.uuid4()
        impostor = uuid.uuid4()
        feed_id = self._insert_feed(
            "Stolen Feed",
            status="active",
            worker_id=owner,
            last_heartbeat_age_seconds=10,
        )

        result = self.store.renew_heartbeat(feed_id, impostor)

        self.assertFalse(result)

    # -- Tests: renew_heartbeats_batch ------------------------------------

    def test_batch_renew_returns_all_active_feeds(self) -> None:
        """All feeds owned by the worker are returned in the renewed set."""
        worker = uuid.uuid4()
        feed_a = self._insert_feed(
            "Feed A",
            status="active",
            worker_id=worker,
            last_heartbeat_age_seconds=30,
        )
        feed_b = self._insert_feed(
            "Feed B",
            status="active",
            worker_id=worker,
            last_heartbeat_age_seconds=30,
        )

        result = self.store.renew_heartbeats_batch([feed_a, feed_b], worker)

        self.assertEqual(result, {feed_a, feed_b})

    def test_batch_renew_excludes_stolen_feeds(self) -> None:
        """Only feeds still owned by the worker are returned."""
        worker = uuid.uuid4()
        other_worker = uuid.uuid4()
        owned_feed = self._insert_feed(
            "Owned Feed",
            status="active",
            worker_id=worker,
            last_heartbeat_age_seconds=30,
        )
        stolen_feed = self._insert_feed(
            "Stolen Feed",
            status="active",
            worker_id=other_worker,
            last_heartbeat_age_seconds=30,
        )

        result = self.store.renew_heartbeats_batch(
            [owned_feed, stolen_feed], worker,
        )

        self.assertEqual(result, {owned_feed})

    def test_batch_renew_returns_empty_set_for_empty_input(self) -> None:
        """Empty input returns empty set without hitting the database."""
        result = self.store.renew_heartbeats_batch([], uuid.uuid4())

        self.assertEqual(result, set())


if __name__ == "__main__":
    unittest.main()
