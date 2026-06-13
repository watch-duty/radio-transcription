"""Synchronous feed store for event-driven ingestion services.

Provides a sync counterpart to the async :class:`FeedStore` used by the
normalizer runtime.  Each method opens and closes its own database
connection via the injected *connect_db* factory, which is appropriate
when the caller handles at most one request at a time (e.g. Cloud Run
with ``concurrency=1`` behind pgBouncer).
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from backend.pipeline.ingestion import quarantine_reason

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    import uuid
    from collections.abc import Callable

    import psycopg

    from backend.pipeline.storage.feed_store import FeedStatusReason

# ---------------------------------------------------------------------------
# SQL — psycopg v3 uses %s params (not asyncpg's $1)
# ---------------------------------------------------------------------------

_RESOLVE_ECHO_FEED_SQL = """\
SELECT f.id, f.name, f.status, f.failure_count
FROM feeds f
JOIN feed_properties fp ON fp.feed_id = f.id
WHERE fp.source_feed_id = %s
AND fp.source_type = 'echo'
"""

_HEARTBEAT_SQL = """\
UPDATE feeds
SET last_heartbeat = NOW(),
    failure_count = CASE WHEN failure_count > 0 THEN 0 ELSE failure_count END,
    status = 'active'::feed_status,
    status_reason_updated_at = CASE
        WHEN status_reason IS NOT NULL THEN NOW()
        ELSE status_reason_updated_at
    END,
    status_reason = NULL
WHERE id = %s
"""

# Backoff formula: base * 2^(failure_count), capped at max, plus 0-10s jitter.
# Matches _REPORT_FAILURE_SQL in feed_store.py (minus worker_id/fencing_token).
_RECORD_FAILURE_SQL = """\
UPDATE feeds
SET status = CASE WHEN failure_count + 1 >= %s
                  THEN 'quarantined'::feed_status
                  ELSE 'failing'::feed_status END,
    failure_count = failure_count + 1,
    last_heartbeat = NOW(),
    retry_after = CASE WHEN failure_count + 1 < %s
                       THEN NOW() + LEAST(
                            %s * INTERVAL '1 second',
                            %s * INTERVAL '1 second' * POWER(2, failure_count)
                       ) + (RANDOM() * INTERVAL '10 seconds')
                       ELSE NULL END,
    quarantine_reason = CASE WHEN failure_count + 1 >= %s THEN COALESCE(%s, quarantine_reason) ELSE quarantine_reason END,
    status_reason = COALESCE(%s, 'system_unexpected_error'),
    status_reason_updated_at = NOW()
WHERE id = %s
"""


class SyncFeedStore:
    """Sync storage layer for feed lifecycle operations.

    Args:
        connect_db: Factory that returns a :class:`psycopg.Connection`
            configured with ``autocommit=True`` and ``dict_row``.
        failure_threshold: Number of consecutive failures before a feed
            is quarantined.
        base_backoff_sec: Base interval (seconds) for exponential retry
            backoff.
        max_backoff_sec: Cap (seconds) on the retry backoff interval.
    """

    def __init__(
        self,
        connect_db: Callable[[], psycopg.Connection[dict[str, Any]]],
        *,
        failure_threshold: int = 5,
        base_backoff_sec: int = 15,
        max_backoff_sec: int = 600,
    ) -> None:
        self._connect_db = connect_db
        self._failure_threshold = failure_threshold
        self._base_backoff_sec = base_backoff_sec
        self._max_backoff_sec = max_backoff_sec

    # ------------------------------------------------------------------
    # Source-specific resolution
    # ------------------------------------------------------------------

    def resolve_echo_feed(self, channel_name: str) -> dict[str, Any] | None:
        """Look up a feed by its Echo channel name.

        Returns the feed row ``{id, name, status, failure_count}`` or
        ``None`` if no feed is registered for *channel_name*.
        """
        with self._connect_db() as conn:
            return conn.execute(
                _RESOLVE_ECHO_FEED_SQL, (channel_name,)
            ).fetchone()

    # ------------------------------------------------------------------
    # Generic lifecycle operations
    # ------------------------------------------------------------------

    def record_heartbeat(self, feed_id: uuid.UUID) -> None:
        """Record a successful processing heartbeat.

        Marks status as ``active``, and resets ``failure_count`` if the
        feed was previously in a failing state.
        """
        with self._connect_db() as conn:
            conn.execute(_HEARTBEAT_SQL, (feed_id,))

    def record_failure(
        self,
        feed_id: uuid.UUID,
        *,
        reason: str | None = None,
        status_reason: FeedStatusReason | None = None,
    ) -> None:
        """Record a processing failure with exponential backoff.

        Increments ``failure_count`` and sets ``retry_after`` using the
        formula ``base * 2^failure_count`` (capped at ``max_backoff_sec``,
        plus 0-10 s jitter).  Quarantines the feed when the threshold is
        reached.
        """
        status_reason_value = status_reason.value if status_reason is not None else None  # fmt: skip
        stored_reason = (
            quarantine_reason.cap_quarantine_reason_for_storage(reason)
            if reason is not None
            else None
        )
        with self._connect_db() as conn:
            conn.execute(
                _RECORD_FAILURE_SQL,
                (
                    self._failure_threshold,
                    self._failure_threshold,
                    self._max_backoff_sec,
                    self._base_backoff_sec,
                    self._failure_threshold,
                    stored_reason,
                    status_reason_value,
                    feed_id,
                ),
            )
            logger.warning(
                "Feed failure recorded",
                extra={"feed_id": str(feed_id)},
            )
