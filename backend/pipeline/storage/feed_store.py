from __future__ import annotations

import enum
import logging
from typing import TYPE_CHECKING, TypedDict

from backend.pipeline.storage.feed_queries import (
    ACQUIRE_FEEDS_RECOVERY_SQL,
    COUNT_HELD_BY_TYPE_SQL,
    CREATE_FEED_SQL,
    DELETE_FEED_SQL,
    GET_FEED_SQL,
    LEASE_FEED_SQL,
    LIST_FEEDS_SQL,
    RELEASE_FEED_SQL,
    RELEASE_FEEDS_BATCH_SQL,
    RENEW_HEARTBEATS_BATCH_DIAGNOSTIC_SQL,
    REPORT_FAILURE_SQL,
    RESET_FEED_SQL,
    UPDATE_PROGRESS_SQL,
    build_acquire_feeds_batch_sql,
)

if TYPE_CHECKING:
    import datetime
    import uuid
    from collections.abc import Sequence

    import asyncpg

logger = logging.getLogger(__name__)


class SourceType(enum.StrEnum):
    """Supported audio source types.

    Each value corresponds to a slug in the ``source_types`` database table.

    .. important::
        This enum **must** be kept in sync with the following SQL files:

        - ``terraform/modules/alloydb/sql/ingestion/002_source_types.sql``
        - ``terraform/modules/alloydb/sql/ingestion/006_seed_source_types.sql``

        When adding or renaming a source type, update both this enum and the
        SQL files together.
    """

    BCFY_FEEDS = "bcfy_feeds"
    BCFY_CALLS = "bcfy_calls"
    # Echo uses a separate cloud function for ingestion instead of VMs.
    ECHO = "echo"
    OPENMHZ = "openmhz"


class LeasedFeed(TypedDict):
    """Feed details returned after a successful lease acquisition."""

    id: uuid.UUID
    name: str
    external_id: str
    source_type: SourceType
    last_processed_filename: str | None
    last_bookmark_time: datetime.datetime | None
    fencing_token: int
    source_feed_id: str | None


class HeartbeatResult(TypedDict):
    """Per-feed diagnostic info returned by diagnostic heartbeat renewal."""

    id: uuid.UUID
    current_worker: uuid.UUID | None
    current_status: str
    renewed: bool


class Feed(TypedDict):
    """Full feed details."""

    id: uuid.UUID
    name: str
    source_type: SourceType
    status: str
    failure_count: int
    worker_id: uuid.UUID | None
    last_heartbeat: datetime.datetime | None
    last_processed_filename: str | None
    last_bookmark_time: datetime.datetime | None
    created_at: datetime.datetime
    source_feed_id: str | None
    external_id: str | None


class FeedStore:
    """
    Storage layer for feed lifecycle operations against AlloyDB.

    Provides atomic SQL operations for the feed leasing mechanism:
    acquiring leases, bookmarking progress, and reporting failures.

    Uses asyncpg pool-level methods for automatic connection checkout
    and release, enabling concurrent DB access from many feed tasks.

    Args:
        pool: An asyncpg connection pool to the AlloyDB instance.
        source_types: Optional list of source-type slugs to filter
            lease queries.  When set, only feeds whose ``source_type``
            matches one of the values will be leased.  ``None`` disables
            filtering (all types are eligible).
        claim_types: Optional ordered sequence of ``SourceType`` values
            this store will claim via ``acquire_feeds_batch``. The SQL
            is generated at construction time with one MATERIALIZED CTE
            per type. Defaults to every ``SourceType`` except ``ECHO``
            (Echo feeds are served by a separate cloud function and
            are never leased here). ``NormalizerRuntime`` passes
            ``list(settings.caps.keys())`` so the SQL shape and the
            runtime's per-type budgets are seeded from the same set.

    """

    def __init__(
        self,
        pool: asyncpg.Pool,
        source_types: list[str] | None = None,
        claim_types: Sequence[SourceType] | None = None,
    ) -> None:
        self._pool = pool
        self._source_types = source_types
        if claim_types is None:
            claim_types = [t for t in SourceType if t != SourceType.ECHO]
        self._claim_types: tuple[SourceType, ...] = tuple(claim_types)
        self._acquire_feeds_batch_sql = build_acquire_feeds_batch_sql(
            self._claim_types,
        )

    def _row_to_feed(self, row: asyncpg.Record) -> Feed:
        """Convert a database row to a Feed dict with validation."""
        try:
            source_type = SourceType(row["source_type"])
        except ValueError as e:
            msg = f"Unknown source type {row['source_type']!r} for feed {row['id']}"
            raise ValueError(msg) from e

        return Feed(
            id=row["id"],
            name=row["name"],
            source_type=source_type,
            status=row["status"],
            failure_count=row["failure_count"],
            worker_id=row["worker_id"],
            last_heartbeat=row["last_heartbeat"],
            last_processed_filename=row["last_processed_filename"],
            last_bookmark_time=row["last_bookmark_time"],
            created_at=row["created_at"],
            source_feed_id=row["source_feed_id"],
            external_id=row["external_id"],
        )

    def _row_to_leased_feed(self, row: asyncpg.Record) -> LeasedFeed:
        """Convert a claim/lease-path row to a LeasedFeed dict.

        Shared between lease_feed (single-row), acquire_feeds_batch (primary
        per-type CTE), and acquire_feeds_recovery (failing-retryable +
        active-abandoned). Keeping the mapping in one place ensures all three
        claim paths agree on how source_type is validated and which columns
        end up in the TypedDict.
        """
        try:
            source_type = SourceType(row["source_type"])
        except ValueError as e:
            msg = f"Unknown source type {row['source_type']!r} for feed {row['id']}"
            raise ValueError(msg) from e
        return LeasedFeed(
            id=row["id"],
            name=row["name"],
            external_id=row["external_id"],
            source_type=source_type,
            last_processed_filename=row["last_processed_filename"],
            last_bookmark_time=row["last_bookmark_time"],
            fencing_token=row["fencing_token"],
            source_feed_id=row["source_feed_id"],
        )

    async def lease_feed(self, worker_id: uuid.UUID) -> LeasedFeed | None:
        """
        Atomically find, lock, and lease an available feed.

        Finds the highest-priority available feed (unclaimed or failing with a
        stale heartbeat), assigns it to the given worker, and returns the feed
        details including source-specific properties from ``feed_properties``.

        Args:
            worker_id: UUID of the worker requesting the lease.

        Returns:
            A ``LeasedFeed`` dictionary if a feed was leased, or ``None`` if no
            feeds are available.

        """
        row = await self._pool.fetchrow(
            LEASE_FEED_SQL, worker_id, self._source_types
        )
        if row is None:
            return None
        return self._row_to_leased_feed(row)

    async def update_feed_progress(
        self,
        feed_id: uuid.UUID,
        worker_id: uuid.UUID,
        new_gcs_path: str,
        fencing_token: int,
        last_bookmark_time: datetime.datetime | None,
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
            last_bookmark_time: Timestamp bookmark for the last processed audio.

        Returns:
            ``True`` if the update succeeded (lease still held), ``False`` if the
            lease was lost.

        """
        result = await self._pool.execute(
            UPDATE_PROGRESS_SQL,
            new_gcs_path,
            feed_id,
            worker_id,
            fencing_token,
            last_bookmark_time,
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
            RENEW_HEARTBEATS_BATCH_DIAGNOSTIC_SQL,
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
    ) -> str | None:
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
            The new feed status (``'failing'`` or ``'quarantined'``) if
            the failure was recorded, or ``None`` if the lease was
            already lost.

        """
        row = await self._pool.fetchrow(
            REPORT_FAILURE_SQL,
            feed_id,
            worker_id,
            failure_threshold,
            fencing_token,
            backoff_max_sec,
            backoff_base_sec,
        )
        if row is None:
            return None

        status: str = row["status"]
        if status == "quarantined":
            logger.critical(
                "Feed failure threshold reached — status set to quarantined",
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
        return status

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
            RELEASE_FEED_SQL,
            feed_id,
            worker_id,
            fencing_token,
        )
        return result == "UPDATE 1"

    async def acquire_feeds_batch(
        self,
        worker_id: uuid.UUID,
        ramp_pct: int,
        limits: dict[SourceType, int],
    ) -> list[LeasedFeed]:
        """
        Batch-acquire unclaimed feeds via the per-type UNION ALL MATERIALIZED CTE.

        Each branch is independently capped by its per-type LIMIT so adversarial
        heap clustering (e.g. a batch of newly-added bcfy_feeds landing
        together) cannot hand a worker a memory-heavy mono-type batch.
        Passing 0 for a branch's LIMIT structurally skips that type — the
        branch's inner SELECT returns no rows and contributes nothing to the
        outer UPDATE.

        Recovery-path claims (failing-retryable + active-abandoned) are served
        by the separate ``acquire_feeds_recovery`` method, called when this
        primary path returns fewer rows than the worker's total slack.

        Args:
            worker_id: UUID of the worker requesting leases.
            ramp_pct: md5 ramp filter threshold (0-100). 100 = all eligible.
            limits: Per-type LIMIT keyed by ``SourceType``. Types absent
                from the dict are passed as 0 (their CTE branch returns
                no rows). Types present but not in this store's
                ``claim_types`` raise ``ValueError`` — the SQL was
                generated for a fixed set at construction.

        Returns:
            List of ``LeasedFeed`` dicts (empty if none available).
        """
        unknown = set(limits) - set(self._claim_types)
        if unknown:
            msg = (
                "limits contains source types not in this store's claim_types: "
                f"{sorted(t.value for t in unknown)}"
            )
            raise ValueError(msg)
        positional = [limits.get(t, 0) for t in self._claim_types]
        rows = await self._pool.fetch(
            self._acquire_feeds_batch_sql,
            worker_id,
            ramp_pct,
            *positional,
        )
        return [self._row_to_leased_feed(row) for row in rows]

    async def acquire_feeds_recovery(
        self,
        worker_id: uuid.UUID,
        abandonment_window_sec: float,
        ramp_pct: int,
        limit: int,
    ) -> list[LeasedFeed]:
        """Recovery-path claim: failing-retryable + active-abandoned.

        Called by the worker when the per-type primary CTE returns fewer
        rows than the worker's total slack. The query is filtered to this
        store's ``claim_types``, so feeds of types this worker doesn't
        claim (including ``ECHO``) are never returned. No per-type cap
        within ``claim_types`` — failing and abandoned volumes are small
        by construction, and the pg_cron sweep handles most
        active-abandoned reclamation out-of-band.

        Args:
            worker_id: UUID of the worker requesting leases.
            abandonment_window_sec: Seconds before a heartbeat is considered stale.
            ramp_pct: md5 ramp filter threshold (0-100). 100 = all eligible.
            limit: Maximum number of feeds to acquire.

        Returns:
            List of ``LeasedFeed`` dicts (empty if none available or
            ``limit <= 0``).
        """
        if limit <= 0:
            return []

        import datetime  # noqa: PLC0415

        rows = await self._pool.fetch(
            ACQUIRE_FEEDS_RECOVERY_SQL,
            worker_id,
            datetime.timedelta(seconds=abandonment_window_sec),
            ramp_pct,
            limit,
            [t.value for t in self._claim_types],
        )
        return [self._row_to_leased_feed(row) for row in rows]

    async def count_held_by_type(
        self,
        worker_id: uuid.UUID,
    ) -> dict[SourceType, int]:
        """Count active leases held by ``worker_id``, grouped by source_type.

        Authoritative per-cycle replacement for the worker's in-memory
        ``_held_by_type`` counter. The leasing loop calls this once per
        iteration and passes the result straight into
        ``_calculate_branch_limits``; the DB is the source of truth, so
        sweep-reclaims and cross-worker steals are reflected immediately
        without any in-process bookkeeping.

        The returned dict always contains an entry for every ``SourceType``
        value — types the worker doesn't currently hold map to 0. This
        lets the caller skip ``.get(t, 0)`` dances at use sites. ``ECHO``
        is always 0 in practice (Echo feeds are served by a separate
        cloud function, never leased by this worker) but the key is
        populated anyway so the invariant "every SourceType in the dict"
        holds.

        Unknown ``source_type`` strings returned by the DB are silently
        skipped. ``CREATE_FEED_SQL`` enforces referential integrity
        against ``source_types``, so this should never trigger in
        practice — the skip path is defensive against a hypothetical
        schema drift.

        Args:
            worker_id: UUID of the worker whose held leases to count.

        Returns:
            Dict mapping every ``SourceType`` to the count of active
            leases owned by this worker for that type (0 if none).
        """
        rows = await self._pool.fetch(COUNT_HELD_BY_TYPE_SQL, worker_id)
        counts: dict[SourceType, int] = dict.fromkeys(SourceType, 0)
        for row in rows:
            try:
                counts[SourceType(row["source_type"])] = row["n"]
            except ValueError:
                continue
        return counts

    async def release_feeds_batch(self, worker_id: uuid.UUID) -> int:
        """Release every active lease still owned by this worker.

        Primary use: ``_shutdown_sequence`` calls this once after
        cancelling all feed tasks. The cancelled tasks return without
        running their normal-completion ``release_feed`` (see the
        ``_process_feed`` shutdown branch), so this single
        ``WHERE worker_id = $1`` UPDATE is what actually flips them
        back to ``unclaimed``.

        Secondary defensive role: catches any straggler row where an
        earlier per-feed ``release_feed`` call failed mid-lifetime
        (transient DB error, task reaped before the finally block could
        retry) and the row was sitting in the DB with ``worker_id = us``
        until pg_cron reclaimed it.

        Args:
            worker_id: UUID of the worker releasing its leases.

        Returns:
            The number of feeds actually released.
        """
        result = await self._pool.execute(RELEASE_FEEDS_BATCH_SQL, worker_id)
        if result.startswith("UPDATE "):
            return int(result.split()[1])
        return 0

    async def create_feed(
        self,
        name: str,
        source_type: str | SourceType,
        source_feed_id: str,
        external_id: str,
    ) -> Feed:
        """Create a new feed record.

        Atomically creates a new feed in the `feeds` table and its corresponding
        properties in the `feed_properties` table.
        """
        if not source_feed_id:
            msg = "source_feed_id cannot be empty"
            raise ValueError(msg)
        if not external_id:
            msg = "external_id cannot be empty"
            raise ValueError(msg)

        # Validate SourceType
        if isinstance(source_type, str):
            try:
                SourceType(source_type)
            except ValueError as e:
                msg = f"Invalid source type {source_type!r}"
                raise ValueError(msg) from e
            source_type_str = source_type
        else:
            source_type_str = source_type.value

        row = await self._pool.fetchrow(
            CREATE_FEED_SQL,
            name,
            source_type_str,
            source_feed_id,
            external_id,
        )
        if row is None:
            msg = f"Failed to create feed {name}"
            raise ValueError(msg)

        return self._row_to_feed(row)

    async def get_feed(self, feed_id: uuid.UUID) -> Feed | None:
        """Fetch a specific feed by ID.

        Retrieves feed details including properties from `feed_properties`.
        """
        row = await self._pool.fetchrow(GET_FEED_SQL, feed_id)
        if row is None:
            return None

        return self._row_to_feed(row)

    async def list_feeds(self) -> list[Feed]:
        """List all feeds.

        Retrieves all feeds ordered by creation time descending.
        """
        rows = await self._pool.fetch(LIST_FEEDS_SQL)
        return [self._row_to_feed(row) for row in rows]

    async def delete_feed(self, feed_id: uuid.UUID) -> bool:
        """Delete a feed by ID.

        Returns True if a row was deleted, False otherwise.
        """
        result = await self._pool.execute(DELETE_FEED_SQL, feed_id)
        return result == "DELETE 1"

    async def reset_feed(self, feed_id: uuid.UUID) -> Feed | None:
        """Reset a feed to an unclaimed, unassigned state.

        Sets ``status = 'unclaimed'``, ``failure_count = 0``, clears
        ``worker_id``, and updates ``last_heartbeat`` for the given feed.
        Returns the updated feed, or ``None`` if no feed with that ID exists.

        Args:
            feed_id: UUID of the feed to reset.

        Returns:
            The updated ``Feed`` dict, or ``None`` if the feed was not found.

        """
        row = await self._pool.fetchrow(RESET_FEED_SQL, feed_id)
        if row is None:
            return None
        return self._row_to_feed(row)
