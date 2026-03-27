from __future__ import annotations

import logging
from typing import TYPE_CHECKING, TypedDict

if TYPE_CHECKING:
    import uuid

    import asyncpg

    from backend.pipeline.schema_types.source_types_pb2 import SourceType

logger = logging.getLogger(__name__)


_LEASE_FEED_SQL = """\
WITH available_feed AS (
    SELECT id
    FROM feeds
    WHERE (
        status = 'unclaimed'::feed_status
        OR (status = 'failing'::feed_status AND (retry_after IS NULL OR retry_after <= NOW()))
        OR (status = 'active'::feed_status
            AND last_heartbeat < NOW() - INTERVAL '60 seconds')
    )
    ORDER BY (status = 'unclaimed'::feed_status) DESC,
             retry_after ASC NULLS FIRST,
             last_heartbeat ASC NULLS FIRST
    LIMIT 1
    FOR UPDATE SKIP LOCKED
),
leased AS (
    UPDATE feeds
    SET worker_id = $1,
        status = 'active'::feed_status,
        retry_after = NULL,
        last_heartbeat = NOW(),
        fencing_token = fencing_token + 1
    FROM available_feed
    WHERE feeds.id = available_feed.id
    RETURNING feeds.id, feeds.name, feeds.source_type, feeds.source_type_enum,
              feeds.last_processed_filename, feeds.fencing_token
)
SELECT leased.id, leased.name, leased.source_type, leased.source_type_enum,
       leased.last_processed_filename, leased.fencing_token, fpi.stream_url
FROM leased
LEFT JOIN feed_properties_icecast fpi ON fpi.feed_id = leased.id
"""

_UPDATE_PROGRESS_SQL = """\
UPDATE feeds
SET last_processed_filename = $1,
    last_heartbeat = NOW(),
    failure_count = 0
WHERE id = $2 AND worker_id = $3 AND fencing_token = $4
"""

# Heartbeat renewal deliberately does NOT check fencing_token.  The token is
# constant for the lifetime of a lease (only incremented on acquisition), so
# worker_id alone is sufficient here.  Adding per-feed tokens would require
# passing parallel arrays, adding complexity with no safety benefit.
_RENEW_HEARTBEATS_BATCH_DIAGNOSTIC_SQL = """\
WITH current_state AS (
    SELECT id, worker_id, status
    FROM feeds WHERE id = ANY($1::uuid[])
    FOR UPDATE
),
do_update AS (
    UPDATE feeds SET last_heartbeat = NOW()
    FROM current_state
    WHERE feeds.id = current_state.id AND current_state.worker_id = $2
    RETURNING feeds.id
)
SELECT
    current_state.id,
    current_state.worker_id AS current_worker,
    current_state.status::text AS current_status,
    (do_update.id IS NOT NULL) AS renewed
FROM current_state
LEFT JOIN do_update ON current_state.id = do_update.id;
"""

_RELEASE_FEED_SQL = """\
UPDATE feeds
SET worker_id = NULL,
    status = 'unclaimed'::feed_status,
    last_heartbeat = NOW()
WHERE id = $1 AND worker_id = $2 AND fencing_token = $3
"""

_RELEASE_FEEDS_BATCH_SQL = """\
UPDATE feeds
SET worker_id = NULL,
    status = 'unclaimed'::feed_status,
    last_heartbeat = NOW()
WHERE worker_id = $1 AND status = 'active'::feed_status
"""

_ACQUIRE_FEEDS_BATCH_SQL = """\
WITH available_feeds AS (
    SELECT id
    FROM feeds
    WHERE (
        status = 'unclaimed'::feed_status
        OR (status = 'failing'::feed_status AND (retry_after IS NULL OR retry_after <= NOW()))
        OR (status = 'active'::feed_status
            AND last_heartbeat < NOW() - $2::interval)
    )
    ORDER BY (status = 'unclaimed'::feed_status) DESC,
             retry_after ASC NULLS FIRST,
             last_heartbeat ASC NULLS FIRST
    LIMIT $3
    FOR UPDATE SKIP LOCKED
),
leased AS (
    UPDATE feeds
    SET worker_id = $1,
        status = 'active'::feed_status,
        retry_after = NULL,
        last_heartbeat = NOW(),
        fencing_token = fencing_token + 1
    FROM available_feeds
    WHERE feeds.id = available_feeds.id
    RETURNING feeds.id, feeds.name, feeds.source_type, feeds.source_type_enum,
              feeds.last_processed_filename, feeds.fencing_token
)
SELECT leased.id, leased.name, leased.source_type, leased.source_type_enum,
       leased.last_processed_filename, leased.fencing_token, fpi.stream_url
FROM leased
LEFT JOIN feed_properties_icecast fpi ON fpi.feed_id = leased.id
"""

# NOTE: $3 = failure_threshold, $4 = fencing_token,
#       $5 = backoff_max_sec, $6 = backoff_base_sec.
# Backoff formula: base * 2^(failure_count), capped at max, plus 0-10s jitter.
_REPORT_FAILURE_SQL = """\
UPDATE feeds
SET status = CASE WHEN failure_count + 1 >= $3
                  THEN 'quarantined'::feed_status
                  ELSE 'failing'::feed_status END,
    failure_count = failure_count + 1,
    worker_id = NULL,
    last_heartbeat = NOW(),
    retry_after = CASE WHEN failure_count + 1 < $3
                       THEN NOW() + LEAST($5 * INTERVAL '1 second',
                            $6 * INTERVAL '1 second' * POWER(2, failure_count))
                            + (RANDOM() * INTERVAL '10 seconds')
                       ELSE NULL END
WHERE id = $1 AND worker_id = $2 AND fencing_token = $4
RETURNING status::text, failure_count, retry_after
"""


class LeasedFeed(TypedDict):
    """Feed details returned after a successful lease acquisition."""

    id: uuid.UUID
    name: str
    source_type: SourceType
    last_processed_filename: str | None
    fencing_token: int
    stream_url: str | None


class HeartbeatResult(TypedDict):
    """Per-feed diagnostic info returned by diagnostic heartbeat renewal."""

    id: uuid.UUID
    current_worker: uuid.UUID | None
    current_status: str
    renewed: bool


class FeedStore:
    """
    Storage layer for feed lifecycle operations against AlloyDB.

    Provides atomic SQL operations for the feed leasing mechanism:
    acquiring leases, bookmarking progress, and reporting failures.

    Uses asyncpg pool-level methods for automatic connection checkout
    and release, enabling concurrent DB access from many feed tasks.

    Args:
        pool: An asyncpg connection pool to the AlloyDB instance.

    """

    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    async def lease_feed(self, worker_id: uuid.UUID) -> LeasedFeed | None:
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
        row = await self._pool.fetchrow(_LEASE_FEED_SQL, worker_id)
        if row is None:
            return None

        return LeasedFeed(
            id=row["id"],
            name=row["name"],
            source_type=row["source_type_enum"],
            last_processed_filename=row["last_processed_filename"],
            fencing_token=row["fencing_token"],
            stream_url=row["stream_url"],
        )

    async def update_feed_progress(
        self,
        feed_id: uuid.UUID,
        worker_id: uuid.UUID,
        new_gcs_path: str,
        fencing_token: int,
    ) -> bool:
        """
        Update the feed's bookmark and heartbeat after a successful write.

        This is a fenced operation — it only succeeds if the given worker still
        holds the lease AND the fencing token matches. A ``False`` return
        indicates the lease was lost and the worker should stop processing
        this feed.

        Args:
            feed_id: UUID of the feed to update.
            worker_id: UUID of the worker holding the lease.
            new_gcs_path: The GCS object path of the last successfully written file.
            fencing_token: The fencing token received at lease acquisition.

        Returns:
            ``True`` if the update succeeded (lease still held), ``False`` if the
            lease was lost.

        """
        result = await self._pool.execute(
            _UPDATE_PROGRESS_SQL,
            new_gcs_path,
            feed_id,
            worker_id,
            fencing_token,
        )
        return result == "UPDATE 1"

    async def renew_heartbeats_batch_diagnostic(
        self,
        feed_ids: list[uuid.UUID],
        worker_id: uuid.UUID,
    ) -> list[HeartbeatResult]:
        """
        Batch-renew heartbeats with per-feed diagnostic info.

        Uses an atomic CTE that locks rows, conditionally updates, and
        always returns per-feed state in a single round-trip. This enables
        the caller to log *why* a feed wasn't renewed (stolen by another
        worker, quarantined, etc.) before taking action.

        Args:
            feed_ids: List of feed UUIDs to renew.
            worker_id: UUID of the worker holding the leases.

        Returns:
            List of ``HeartbeatResult`` dicts, one per input feed_id.

        """
        if not feed_ids:
            return []
        rows = await self._pool.fetch(
            _RENEW_HEARTBEATS_BATCH_DIAGNOSTIC_SQL,
            feed_ids,
            worker_id,
        )
        return [
            HeartbeatResult(
                id=row["id"],
                current_worker=row["current_worker"],
                current_status=row["current_status"],
                renewed=row["renewed"],
            )
            for row in rows
        ]

    async def report_feed_failure(
        self,
        feed_id: uuid.UUID,
        worker_id: uuid.UUID,
        fencing_token: int,
        failure_threshold: int = 5,
        backoff_base_sec: int = 15,
        backoff_max_sec: int = 600,
    ) -> bool:
        """Report a feed failure with exponential backoff.

        Atomically increments ``failure_count``, computes ``retry_after``
        with exponential backoff + jitter, and transitions to
        ``'quarantined'`` if *failure_threshold* is reached.

        Backoff formula: ``min(backoff_base_sec * 2^failure_count,
        backoff_max_sec) + random(0-10s) jitter``.

        This is a fenced operation — it only succeeds if the given worker
        still holds the lease AND the fencing token matches.

        Args:
            feed_id: UUID of the feed that failed.
            worker_id: UUID of the worker reporting the failure.
            fencing_token: The fencing token received at lease acquisition.
            failure_threshold: Number of consecutive failures before
                quarantine.
            backoff_base_sec: Base delay in seconds for the first retry.
            backoff_max_sec: Maximum backoff cap in seconds.

        Returns:
            ``True`` if the failure was recorded, ``False`` if the lease was
            already lost.

        """
        row = await self._pool.fetchrow(
            _REPORT_FAILURE_SQL,
            feed_id,
            worker_id,
            failure_threshold,
            fencing_token,
            backoff_max_sec,
            backoff_base_sec,
        )
        if row is None:
            return False

        if row["status"] == "quarantined":
            logger.critical(
                "Feed quarantined",
                extra={
                    "feed_id": str(feed_id),
                    "failure_count": row["failure_count"],
                },
            )
        else:
            logger.info(
                "Feed failure recorded",
                extra={
                    "feed_id": str(feed_id),
                    "failure_count": row["failure_count"],
                    "retry_after": str(row["retry_after"]),
                },
            )
        return True

    async def release_feed(
        self,
        feed_id: uuid.UUID,
        worker_id: uuid.UUID,
        fencing_token: int,
    ) -> bool:
        """
        Release a feed lease, returning it to 'unclaimed' status.

        Used during graceful shutdown or when a capture function exits
        normally. The 60-second heartbeat safety net will eventually reclaim
        the feed if this call fails.

        This is a fenced operation — it only succeeds if the given worker
        still holds the lease AND the fencing token matches.

        Args:
            feed_id: UUID of the feed to release.
            worker_id: UUID of the worker releasing the lease.
            fencing_token: The fencing token received at lease acquisition.

        Returns:
            ``True`` if the lease was released, ``False`` if the lease was
            already lost.

        """
        result = await self._pool.execute(
            _RELEASE_FEED_SQL,
            feed_id,
            worker_id,
            fencing_token,
        )
        return result == "UPDATE 1"

    async def release_feeds_batch(self, worker_id: uuid.UUID) -> int:
        """
        Release all active leases held by this worker.

        Used during graceful shutdown to allow other workers to immediately claim
        the feeds without waiting for timeout expiration.

        Args:
            worker_id: UUID of the worker releasing leases.

        Returns:
            The number of feeds released.
        """
        result = await self._pool.execute(_RELEASE_FEEDS_BATCH_SQL, worker_id)
        if result.startswith("UPDATE "):
            return int(result.split()[1])
        return 0

    async def acquire_feeds_batch(
        self,
        worker_id: uuid.UUID,
        abandonment_window_sec: float,
        limit: int,
    ) -> list[LeasedFeed]:
        """
        Batch-acquire up to *limit* available feeds in a single query.

        Uses FOR UPDATE SKIP LOCKED to avoid contention with other workers.

        Args:
            worker_id: UUID of the worker requesting leases.
            abandonment_window_sec: Seconds before a heartbeat is considered stale.
            limit: Maximum number of feeds to acquire.

        Returns:
            List of ``LeasedFeed`` dicts (empty if none available).

        """
        import datetime  # noqa: PLC0415

        rows = await self._pool.fetch(
            _ACQUIRE_FEEDS_BATCH_SQL,
            worker_id,
            datetime.timedelta(seconds=abandonment_window_sec),
            limit,
        )
        return [
            LeasedFeed(
                id=row["id"],
                name=row["name"],
                source_type=row["source_type_enum"],
                last_processed_filename=row["last_processed_filename"],
                fencing_token=row["fencing_token"],
                stream_url=row["stream_url"],
            )
            for row in rows
        ]
