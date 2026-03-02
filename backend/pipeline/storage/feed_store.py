from __future__ import annotations

from typing import TYPE_CHECKING, TypedDict

from psycopg.rows import dict_row

if TYPE_CHECKING:
    import uuid

    import psycopg

_LEASE_FEED_SQL = """\
WITH available_feed AS (
    SELECT id
    FROM feeds
    WHERE status IN ('unclaimed'::feed_status, 'failing'::feed_status, 'active'::feed_status)
      AND (worker_id IS NULL OR last_heartbeat < NOW() - INTERVAL '60 seconds')
    ORDER BY (status = 'unclaimed'::feed_status) DESC,
             failure_count ASC,
             last_heartbeat ASC NULLS FIRST
    LIMIT 1
    FOR UPDATE SKIP LOCKED
),
leased AS (
    UPDATE feeds
    SET worker_id = %s,
        status = 'active'::feed_status,
        last_heartbeat = NOW()
    FROM available_feed
    WHERE feeds.id = available_feed.id
    RETURNING feeds.id, feeds.name, feeds.source_type,
              feeds.last_processed_filename
)
SELECT leased.id, leased.name, leased.source_type,
       leased.last_processed_filename, fpi.stream_url
FROM leased
LEFT JOIN feed_properties_icecast fpi ON fpi.feed_id = leased.id
"""

_UPDATE_PROGRESS_SQL = """\
UPDATE feeds
SET last_processed_filename = %s,
    last_heartbeat = NOW(),
    failure_count = 0
WHERE id = %s AND worker_id = %s
"""

_REPORT_FAILURE_SQL = """\
UPDATE feeds
SET status = CASE WHEN failure_count + 1 >= 3
                  THEN 'quarantined'::feed_status
                  ELSE 'failing'::feed_status END,
    failure_count = failure_count + 1,
    worker_id = NULL,
    last_heartbeat = NOW()
WHERE id = %s AND worker_id = %s
"""


class LeasedFeed(TypedDict):
    """Feed details returned after a successful lease acquisition."""

    id: uuid.UUID
    name: str
    source_type: str
    last_processed_filename: str | None
    stream_url: str | None


class FeedStore:
    """
    Storage layer for feed lifecycle operations against AlloyDB.

    Provides atomic SQL operations for the feed leasing mechanism:
    acquiring leases, bookmarking progress, and reporting failures.

    Args:
        conn: A psycopg connection to the AlloyDB instance.

    """

    def __init__(self, conn: psycopg.Connection) -> None:
        self._conn = conn

    def lease_feed(self, worker_id: uuid.UUID) -> LeasedFeed | None:
        """
        Atomically find, lock, and lease an available feed.

        Finds the highest-priority available feed (unclaimed or failing with a
        stale heartbeat), assigns it to the given worker, and returns the feed
        details including any Icecast-specific properties.

        Args:
            worker_id: UUID of the worker requesting the lease.

        Returns:
            A ``LeasedFeed`` dictionary if a feed was leased, or ``None`` if no
            feeds are available.

        """
        with self._conn.transaction():
            with self._conn.cursor(row_factory=dict_row) as cursor:
                cursor.execute(_LEASE_FEED_SQL, (worker_id,))
                row = cursor.fetchone()
                if row is None:
                    return None

                return LeasedFeed(
                    id=row["id"],
                    name=row["name"],
                    source_type=row["source_type"],
                    last_processed_filename=row["last_processed_filename"],
                    stream_url=row["stream_url"],
                )

    def update_feed_progress(
        self,
        feed_id: uuid.UUID,
        worker_id: uuid.UUID,
        new_gcs_path: str,
    ) -> bool:
        """
        Update the feed's bookmark and heartbeat after a successful write.

        This is a fenced operation — it only succeeds if the given worker still
        holds the lease. A ``False`` return indicates the lease was lost and the
        worker should stop processing this feed.

        Args:
            feed_id: UUID of the feed to update.
            worker_id: UUID of the worker holding the lease.
            new_gcs_path: The GCS object path of the last successfully written file.

        Returns:
            ``True`` if the update succeeded (lease still held), ``False`` if the
            lease was lost.

        """
        with self._conn.transaction():
            with self._conn.cursor() as cursor:
                cursor.execute(
                    _UPDATE_PROGRESS_SQL,
                    (new_gcs_path, feed_id, worker_id),
                )
                return cursor.rowcount > 0

    def report_feed_failure(
        self,
        feed_id: uuid.UUID,
        worker_id: uuid.UUID,
    ) -> bool:
        """
        Report a feed failure, incrementing the failure count.

        Releases the worker's lease and transitions the feed to ``'failing'``
        or ``'quarantined'`` (if the failure threshold of 3 is reached).

        This is a fenced operation — it only succeeds if the given worker still
        holds the lease.

        Args:
            feed_id: UUID of the feed that failed.
            worker_id: UUID of the worker reporting the failure.

        Returns:
            ``True`` if the failure was recorded, ``False`` if the lease was
            already lost.

        """
        with self._conn.transaction():
            with self._conn.cursor() as cursor:
                cursor.execute(
                    _REPORT_FAILURE_SQL,
                    (feed_id, worker_id),
                )
                return cursor.rowcount > 0
