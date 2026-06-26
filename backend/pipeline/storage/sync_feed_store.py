"""Synchronous feed store for event-driven ingestion services.

Provides a sync counterpart to the async :class:`FeedStore` used by the
normalizer runtime.  Each method opens and closes its own database
connection via the injected *connect_db* factory, which is appropriate
when the caller handles at most one request at a time (e.g. Cloud Run
with ``concurrency=1`` behind pgBouncer).
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, LiteralString, TypedDict, cast

from backend.pipeline.storage import (
    feed_lifecycle,
    feed_store,
    sync_feed_queries,
)

logger = logging.getLogger(__name__)
if TYPE_CHECKING:
    import datetime
    import uuid
    from collections.abc import Callable

    import psycopg


def _require_actor_id(actor_id: str | None) -> str:
    if actor_id is None:
        msg = "actor_id is required for audited feed lifecycle writes"
        raise ValueError(msg)
    return actor_id


class ResolvedEchoFeed(TypedDict):
    """Feed fields consumed by the Echo ingestion handler."""

    id: uuid.UUID
    name: str
    status: feed_store.FeedStatus
    created_at: datetime.datetime


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
        failure_threshold: int = feed_lifecycle.DEFAULT_FAILURE_THRESHOLD,
        base_backoff_sec: int = feed_lifecycle.DEFAULT_BACKOFF_BASE_SEC,
        max_backoff_sec: int = feed_lifecycle.DEFAULT_BACKOFF_MAX_SEC,
    ) -> None:
        self._connect_db = connect_db
        self._failure_threshold = failure_threshold
        self._base_backoff_sec = base_backoff_sec
        self._max_backoff_sec = max_backoff_sec

    # ------------------------------------------------------------------
    # Source-specific resolution
    # ------------------------------------------------------------------

    def resolve_echo_feed(self, channel_name: str) -> ResolvedEchoFeed | None:
        """Look up a feed by its Echo channel name.

        Returns the fields Echo processing consumes, or ``None`` if no feed is
        registered for *channel_name*.
        """
        with self._connect_db() as conn:
            row = conn.execute(
                sync_feed_queries.RESOLVE_ECHO_FEED_SQL, (channel_name,)
            ).fetchone()
        if row is None:
            return None
        try:
            status = feed_store.FeedStatus(row["status"])
        except ValueError as exc:
            msg = f"Unknown feed status {row['status']!r} for Echo feed"
            raise ValueError(msg) from exc
        return {
            "id": row["id"],
            "name": row["name"],
            "status": status,
            "created_at": row["created_at"],
        }

    # ------------------------------------------------------------------
    # Generic lifecycle operations
    # ------------------------------------------------------------------

    def record_heartbeat(
        self,
        feed_id: uuid.UUID,
        *,
        actor_id: str,
    ) -> None:
        """Record a successful processing heartbeat.

        Marks status as ``active``, and resets ``failure_count`` if the
        feed was previously in a failing state.
        """
        with self._connect_db() as conn:
            conn.execute(
                cast("LiteralString", sync_feed_queries.HEARTBEAT_SQL),
                (feed_id, _require_actor_id(actor_id)),
            )

    def record_failure(
        self,
        feed_id: uuid.UUID,
        *,
        actor_id: str,
        reason: str | None = None,
        status_reason: feed_store.FeedStatusReason | None = None,
    ) -> None:
        """Record a processing failure with exponential backoff.

        Increments ``failure_count`` and sets ``retry_after`` using the
        formula ``base * 2^failure_count`` (capped at ``max_backoff_sec``,
        plus 0-10 s jitter).  Quarantines the feed when the threshold is
        reached.
        """
        status_reason_value = feed_lifecycle.status_reason_storage_value(
            status_reason
        )
        status_reason_detail = (
            feed_lifecycle.status_reason_detail_storage_value(reason)
        )
        params = (
            feed_id,
            status_reason_value,
            self._failure_threshold,
            self._failure_threshold,
            self._max_backoff_sec,
            self._base_backoff_sec,
            status_reason_detail,
            _require_actor_id(actor_id),
        )
        with self._connect_db() as conn:
            conn.execute(
                cast("LiteralString", sync_feed_queries.RECORD_FAILURE_SQL),
                params,
            )
        logger.warning(
            "Feed failure recorded",
            extra={
                "feed_id": str(feed_id),
                "status_reason": status_reason_value,
                "reason": status_reason_detail,
            },
        )

    def record_non_budgeted_failure(
        self,
        feed_id: uuid.UUID,
        *,
        actor_id: str,
        status_reason: feed_store.FeedStatusReason,
        reason: str | None = None,
    ) -> None:
        """Record a visible failure without consuming quarantine budget.

        Echo receives source object notifications, so this sync path clears
        ``retry_after`` instead of scheduling DB-based retry.
        """
        params = (
            feed_id,
            status_reason.value,
            feed_lifecycle.status_reason_detail_storage_value(reason),
            _require_actor_id(actor_id),
        )
        with self._connect_db() as conn:
            conn.execute(
                cast(
                    "LiteralString",
                    sync_feed_queries.RECORD_NON_BUDGETED_FAILURE_SQL,
                ),
                params,
            )
        logger.info(
            "Non-budgeted feed failure recorded",
            extra={
                "feed_id": str(feed_id),
                "status_reason": status_reason.value,
            },
        )
