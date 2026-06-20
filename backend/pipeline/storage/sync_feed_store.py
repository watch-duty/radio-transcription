"""Synchronous feed store for event-driven ingestion services.

Provides a sync counterpart to the async :class:`FeedStore` used by the
normalizer runtime.  Each method opens and closes its own database
connection via the injected *connect_db* factory, which is appropriate
when the caller handles at most one request at a time (e.g. Cloud Run
with ``concurrency=1`` behind pgBouncer).
"""

from __future__ import annotations

import datetime
import enum
import json
import logging
import uuid
from typing import TYPE_CHECKING, Any, TypedDict, cast

from backend.pipeline.storage import (
    feed_lifecycle,
    feed_store,
    sync_feed_queries,
)

logger = logging.getLogger(__name__)
_RUNTIME_ABNORMAL_STATUSES = frozenset(
    {feed_store.FeedStatus.FAILING, feed_store.FeedStatus.QUARANTINED}
)

if TYPE_CHECKING:
    from collections.abc import Callable

    import psycopg


class ResolvedEchoFeed(TypedDict):
    """Feed fields consumed by the Echo ingestion handler."""

    id: uuid.UUID
    name: str
    status: feed_store.FeedStatus
    failure_count: int
    status_reason: feed_store.FeedStatusReason | None
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
            "failure_count": row["failure_count"],
            "status_reason": self._parse_status_reason(
                row["status_reason"],
                feed_id=row["id"],
            ),
            "created_at": row["created_at"],
        }

    # ------------------------------------------------------------------
    # Generic lifecycle operations
    # ------------------------------------------------------------------

    @staticmethod
    def _json_default(value: object) -> str:
        """Serialize database scalar values for audit JSON payloads."""
        if isinstance(value, uuid.UUID):
            return str(value)
        if isinstance(value, datetime.datetime):
            return value.isoformat()
        if isinstance(value, enum.StrEnum):
            return value.value
        msg = f"Object of type {type(value).__name__} is not JSON serializable"
        raise TypeError(msg)

    @staticmethod
    def _decode_json_array(value: object) -> list[dict[str, str]]:
        """Decode nullable JSON array values returned by psycopg."""
        if value is None:
            return []
        if isinstance(value, str):
            return cast("list[dict[str, str]]", json.loads(value))
        return cast("list[dict[str, str]]", value)

    @staticmethod
    def _parse_status_reason(
        raw: str | None,
        *,
        feed_id: object,
    ) -> feed_store.FeedStatusReason | None:
        """Parse nullable status-reason text from database rows."""
        if raw is None:
            return None
        try:
            return feed_store.FeedStatusReason(raw)
        except ValueError as exc:
            msg = f"Unknown status reason {raw!r} for feed {feed_id}"
            raise ValueError(msg) from exc

    @staticmethod
    def _parse_feed_status(
        raw: str,
        *,
        feed_id: object,
    ) -> feed_store.FeedStatus:
        """Parse feed lifecycle status text from database rows."""
        try:
            return feed_store.FeedStatus(raw)
        except ValueError as exc:
            msg = f"Unknown status {raw!r} for feed {feed_id}"
            raise ValueError(msg) from exc

    def _audit_snapshot(self, row: dict[str, Any]) -> dict[str, object]:
        """Serialize the maintained feed audit snapshot allowlist."""
        return {
            "id": self._json_default(row["id"]),
            "name": row["name"],
            "source_type": row["source_type"],
            "status": row["status"],
            "failure_count": row["failure_count"],
            "retry_after": row["retry_after"],
            "status_reason": row["status_reason"],
            "status_reason_updated_at": row["status_reason_updated_at"],
            "status_reason_detail": row["status_reason_detail"],
            "quarantine_reason": row["quarantine_reason"],
            "last_bookmark_time": row["last_bookmark_time"],
            "created_at": row["created_at"],
            "feed_properties.source_feed_id": row[
                "feed_properties.source_feed_id"
            ],
            "feed_properties.tags": self._decode_json_array(
                row["feed_properties.tags"]
            ),
        }

    def _allocate_feed_sequence(
        self,
        conn: psycopg.Connection[dict[str, Any]],
        feed_id: uuid.UUID,
    ) -> int:
        """Allocate the next per-feed audit sequence in the transaction."""
        row = conn.execute(
            sync_feed_queries.ALLOCATE_FEED_AUDIT_SEQUENCE_SQL,
            (feed_id,),
        ).fetchone()
        if row is None:
            msg = f"Failed to allocate audit sequence for feed {feed_id}"
            raise ValueError(msg)
        return int(row["feed_sequence"])

    def _insert_feed_audit_event(
        self,
        conn: psycopg.Connection[dict[str, Any]],
        *,
        action: str,
        actor_id: str,
        before_values: dict[str, object],
        after_values: dict[str, object],
        identity_row: dict[str, Any],
        metadata: dict[str, object] | None = None,
    ) -> None:
        """Insert one storage-owned feed audit event."""
        feed_id = identity_row["id"]
        if not isinstance(feed_id, uuid.UUID):
            msg = f"Invalid feed ID for audit event: {feed_id!r}"
            raise TypeError(msg)
        feed_sequence = self._allocate_feed_sequence(conn, feed_id)
        conn.execute(
            sync_feed_queries.INSERT_FEED_AUDIT_EVENT_SQL,
            (
                feed_id,
                action,
                actor_id,
                feed_sequence,
                json.dumps(
                    before_values,
                    default=self._json_default,
                    sort_keys=True,
                ),
                json.dumps(
                    after_values,
                    default=self._json_default,
                    sort_keys=True,
                ),
                json.dumps(metadata or {}, sort_keys=True),
            ),
        )

    @staticmethod
    def _mutation_rowcount(cursor: object) -> int:
        """Return psycopg rowcount for a lifecycle mutation cursor."""
        value = getattr(cursor, "rowcount", 0)
        return int(value) if isinstance(value, int) else 0

    def _runtime_prior_metadata(
        self,
        *,
        previous_status: feed_store.FeedStatus,
        previous_failure_count: int,
        previous_status_reason: feed_store.FeedStatusReason | None,
    ) -> dict[str, object]:
        """Return non-secret runtime prior-state metadata for audit rows."""
        return {
            "previous_status": previous_status.value,
            "previous_failure_count": previous_failure_count,
            "previous_status_reason": (
                previous_status_reason.value
                if previous_status_reason is not None
                else None
            ),
        }

    def _runtime_effective_prior_state(
        self,
        before_row: dict[str, Any],
        *,
        claimed_previous_status: feed_store.FeedStatus,
    ) -> tuple[
        feed_store.FeedStatus,
        int,
        feed_store.FeedStatusReason | None,
    ]:
        """Derive runtime prior state from the locked pre-mutation snapshot."""
        before_status = self._parse_feed_status(
            before_row["status"],
            feed_id=before_row["id"],
        )
        before_failure_count = before_row["failure_count"]
        before_status_reason = self._parse_status_reason(
            before_row["status_reason"],
            feed_id=before_row["id"],
        )
        claim_carried_failure_state = (
            before_status == feed_store.FeedStatus.ACTIVE
            and claimed_previous_status in _RUNTIME_ABNORMAL_STATUSES
            and (before_failure_count > 0 or before_status_reason is not None)
        )
        if claim_carried_failure_state:
            return (
                claimed_previous_status,
                before_failure_count,
                before_status_reason,
            )
        return before_status, before_failure_count, before_status_reason

    def _runtime_failure_audit_action(
        self,
        *,
        previous_status: feed_store.FeedStatus,
        previous_failure_count: int,
        previous_status_reason: feed_store.FeedStatusReason | None,
        after_row: dict[str, Any],
    ) -> str | None:
        """Return the runtime failure/quarantine event action, if any."""
        after_status = self._parse_feed_status(
            after_row["status"],
            feed_id=after_row["id"],
        )
        after_reason = self._parse_status_reason(
            after_row["status_reason"],
            feed_id=after_row["id"],
        )
        if after_status == feed_store.FeedStatus.QUARANTINED:
            if (
                previous_status != feed_store.FeedStatus.QUARANTINED
                and after_row["failure_count"] > previous_failure_count
            ):
                return "feed.quarantined"
            return None
        if after_status != feed_store.FeedStatus.FAILING:
            return None
        if previous_status != feed_store.FeedStatus.FAILING:
            return "feed.failure_reported"
        if previous_status_reason != after_reason:
            return "feed.failure_reported"
        return None

    def _runtime_recovery_audit_action(
        self,
        *,
        previous_status: feed_store.FeedStatus,
        after_row: dict[str, Any],
    ) -> str | None:
        """Return the runtime recovery event action, if any."""
        if previous_status not in _RUNTIME_ABNORMAL_STATUSES:
            return None
        after_status = self._parse_feed_status(
            after_row["status"],
            feed_id=after_row["id"],
        )
        if after_status in _RUNTIME_ABNORMAL_STATUSES:
            return None
        after_reason = self._parse_status_reason(
            after_row["status_reason"],
            feed_id=after_row["id"],
        )
        if after_reason is not None or after_row["failure_count"] != 0:
            return None
        return "feed.recovered"

    def _maybe_insert_runtime_audit_event(
        self,
        conn: psycopg.Connection[dict[str, Any]],
        *,
        action: str | None,
        actor_id: str,
        previous_status: feed_store.FeedStatus,
        previous_failure_count: int,
        previous_status_reason: feed_store.FeedStatusReason | None,
        before_row: dict[str, Any],
        after_row: dict[str, Any],
    ) -> None:
        """Insert a runtime audit event when action selection produced one."""
        if action is None:
            return
        self._insert_feed_audit_event(
            conn,
            action=action,
            actor_id=actor_id,
            before_values=self._audit_snapshot(before_row),
            after_values=self._audit_snapshot(after_row),
            identity_row=after_row,
            metadata=self._runtime_prior_metadata(
                previous_status=previous_status,
                previous_failure_count=previous_failure_count,
                previous_status_reason=previous_status_reason,
            ),
        )

    def record_heartbeat(
        self,
        feed_id: uuid.UUID,
        *,
        actor_id: str | None = None,
        previous_status: feed_store.FeedStatus | None = None,
        previous_failure_count: int = 0,
        previous_status_reason: feed_store.FeedStatusReason | None = None,
    ) -> None:
        """Record a successful processing heartbeat.

        Marks status as ``active``, and resets ``failure_count`` if the
        feed was previously in a failing state.
        """
        if actor_id is None or previous_status is None:
            with self._connect_db() as conn:
                conn.execute(sync_feed_queries.HEARTBEAT_SQL, (feed_id,))
            return

        with self._connect_db() as conn:
            with conn.transaction():
                before_row = conn.execute(
                    sync_feed_queries.GET_AUDIT_FEED_SNAPSHOT_SQL,
                    (feed_id,),
                ).fetchone()
                if before_row is None:
                    return
                cursor = conn.execute(
                    sync_feed_queries.HEARTBEAT_SQL,
                    (feed_id,),
                )
                if self._mutation_rowcount(cursor) != 1:
                    return
                after_row = conn.execute(
                    sync_feed_queries.GET_AUDIT_FEED_SNAPSHOT_SQL,
                    (feed_id,),
                ).fetchone()
                if after_row is None:
                    msg = (
                        "Failed to read sync recovery audit snapshot for "
                        f"feed {feed_id}"
                    )
                    raise ValueError(msg)
                (
                    effective_previous_status,
                    effective_previous_failure_count,
                    effective_previous_status_reason,
                ) = self._runtime_effective_prior_state(
                    before_row,
                    claimed_previous_status=previous_status,
                )
                action = self._runtime_recovery_audit_action(
                    previous_status=effective_previous_status,
                    after_row=after_row,
                )
                self._maybe_insert_runtime_audit_event(
                    conn,
                    action=action,
                    actor_id=actor_id,
                    previous_status=effective_previous_status,
                    previous_failure_count=effective_previous_failure_count,
                    previous_status_reason=effective_previous_status_reason,
                    before_row=before_row,
                    after_row=after_row,
                )

    def record_failure(
        self,
        feed_id: uuid.UUID,
        *,
        actor_id: str | None = None,
        previous_status: feed_store.FeedStatus | None = None,
        previous_failure_count: int = 0,
        previous_status_reason: feed_store.FeedStatusReason | None = None,
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
            self._failure_threshold,
            self._failure_threshold,
            self._max_backoff_sec,
            self._base_backoff_sec,
            status_reason_value,
            status_reason_detail,
            status_reason_value,
            feed_id,
        )
        if actor_id is None or previous_status is None:
            with self._connect_db() as conn:
                conn.execute(sync_feed_queries.RECORD_FAILURE_SQL, params)
                logger.warning(
                    "Feed failure recorded",
                    extra={"feed_id": str(feed_id)},
                )
            return

        with self._connect_db() as conn:
            with conn.transaction():
                before_row = conn.execute(
                    sync_feed_queries.GET_AUDIT_FEED_SNAPSHOT_SQL,
                    (feed_id,),
                ).fetchone()
                if before_row is None:
                    return
                cursor = conn.execute(
                    sync_feed_queries.RECORD_FAILURE_SQL,
                    params,
                )
                if self._mutation_rowcount(cursor) != 1:
                    return
                after_row = conn.execute(
                    sync_feed_queries.GET_AUDIT_FEED_SNAPSHOT_SQL,
                    (feed_id,),
                ).fetchone()
                if after_row is None:
                    msg = (
                        "Failed to read sync failure audit snapshot for "
                        f"feed {feed_id}"
                    )
                    raise ValueError(msg)
                (
                    effective_previous_status,
                    effective_previous_failure_count,
                    effective_previous_status_reason,
                ) = self._runtime_effective_prior_state(
                    before_row,
                    claimed_previous_status=previous_status,
                )
                action = self._runtime_failure_audit_action(
                    previous_status=effective_previous_status,
                    previous_failure_count=effective_previous_failure_count,
                    previous_status_reason=effective_previous_status_reason,
                    after_row=after_row,
                )
                self._maybe_insert_runtime_audit_event(
                    conn,
                    action=action,
                    actor_id=actor_id,
                    previous_status=effective_previous_status,
                    previous_failure_count=effective_previous_failure_count,
                    previous_status_reason=effective_previous_status_reason,
                    before_row=before_row,
                    after_row=after_row,
                )
            logger.warning(
                "Feed failure recorded",
                extra={"feed_id": str(feed_id)},
            )

    def record_non_budgeted_failure(
        self,
        feed_id: uuid.UUID,
        *,
        actor_id: str | None = None,
        previous_status: feed_store.FeedStatus | None = None,
        previous_failure_count: int = 0,
        previous_status_reason: feed_store.FeedStatusReason | None = None,
        status_reason: feed_store.FeedStatusReason,
        reason: str | None = None,
    ) -> None:
        """Record a visible failure without consuming quarantine budget.

        Echo receives source object notifications, so this sync path clears
        ``retry_after`` instead of scheduling DB-based retry. It also leaves
        ``quarantine_reason`` untouched because no quarantine threshold crossed.
        """
        params = (
            status_reason.value,
            feed_lifecycle.status_reason_detail_storage_value(reason),
            status_reason.value,
            feed_id,
        )
        if actor_id is None or previous_status is None:
            with self._connect_db() as conn:
                conn.execute(
                    sync_feed_queries.RECORD_NON_BUDGETED_FAILURE_SQL,
                    params,
                )
                logger.info(
                    "Non-budgeted feed failure recorded",
                    extra={
                        "feed_id": str(feed_id),
                        "status_reason": status_reason.value,
                    },
                )
            return

        with self._connect_db() as conn:
            with conn.transaction():
                before_row = conn.execute(
                    sync_feed_queries.GET_AUDIT_FEED_SNAPSHOT_SQL,
                    (feed_id,),
                ).fetchone()
                if before_row is None:
                    return
                cursor = conn.execute(
                    sync_feed_queries.RECORD_NON_BUDGETED_FAILURE_SQL,
                    params,
                )
                if self._mutation_rowcount(cursor) != 1:
                    return
                after_row = conn.execute(
                    sync_feed_queries.GET_AUDIT_FEED_SNAPSHOT_SQL,
                    (feed_id,),
                ).fetchone()
                if after_row is None:
                    msg = (
                        "Failed to read sync non-budgeted failure audit "
                        f"snapshot for feed {feed_id}"
                    )
                    raise ValueError(msg)
                (
                    effective_previous_status,
                    effective_previous_failure_count,
                    effective_previous_status_reason,
                ) = self._runtime_effective_prior_state(
                    before_row,
                    claimed_previous_status=previous_status,
                )
                action = self._runtime_failure_audit_action(
                    previous_status=effective_previous_status,
                    previous_failure_count=effective_previous_failure_count,
                    previous_status_reason=effective_previous_status_reason,
                    after_row=after_row,
                )
                self._maybe_insert_runtime_audit_event(
                    conn,
                    action=action,
                    actor_id=actor_id,
                    previous_status=effective_previous_status,
                    previous_failure_count=effective_previous_failure_count,
                    previous_status_reason=effective_previous_status_reason,
                    before_row=before_row,
                    after_row=after_row,
                )
            logger.info(
                "Non-budgeted feed failure recorded",
                extra={
                    "feed_id": str(feed_id),
                    "status_reason": status_reason.value,
                },
            )
