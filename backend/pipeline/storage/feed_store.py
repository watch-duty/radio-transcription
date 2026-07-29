from __future__ import annotations

import asyncio
import enum
import json
import logging
import uuid
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, NotRequired, TypedDict

import asyncpg
import asyncpg.exceptions

from backend.pipeline.common.exceptions import (
    FeedAlreadyExistsError,
    FeedNameAlreadyExistsError,
    FeedStateConflictError,
)
from backend.pipeline.storage import (
    connection,
    feed_change_notifications,
    feed_lifecycle,
    feed_queries,
    feed_sid_admin_queries,
    ingestion_lease_queries,
)
from backend.pipeline.storage.pagination_utils import (
    SortOrder,
    decode_cursor,
    decode_int_cursor,
    get_paginated_results,
)

if TYPE_CHECKING:
    import collections.abc
    import datetime

logger = logging.getLogger(__name__)

_CREATE_FEED_UNIQUE_CONSTRAINTS = frozenset(
    {
        "feeds_name_key",
        "idx_feed_properties_source_lookup",
    }
)
_UPDATE_FEED_UNIQUE_CONSTRAINTS = frozenset({"feeds_name_key"})


class SourceType(enum.StrEnum):
    """Supported audio source types.

    Each value corresponds to a slug in the ``source_types`` database table.

    .. important::
        Adding a claimable source type is a three-place change:

        1. **This enum** — add the new member.
        2. **DB seed** — add a row in
           ``terraform/modules/alloydb/sql/ingestion/002_source_types.sql``
           and ``006_seed_source_types.sql``.
        3. **Runtime source spec** — add an entry to
           ``backend.pipeline.ingestion.source_runtime_specs``. This
           registry drives ``CollectorSettings.feed_claim_caps``,
           ``FeedStore``'s generated acquire-batch SQL, topic routing metadata,
           URL base metadata, and the ``claim_types`` filter on the recovery
           path.
           **Skipping this step means VM workers
           will silently never claim feeds of the new type** — neither
           the primary CTE nor the recovery sweep will pick them up.

        Renames must touch both this enum and the DB seed in the same
        deploy.
    """

    # Continuous Icecast-protocol stream for Broadcastify feeds
    # (handled by icecast_collector.py). Currently the primary stream source
    # using the Icecast collector; feeds into Dataflow segmentation.
    # Note: Do not confuse with BCFY_CALLS (discrete REST polling collector).
    BCFY_FEEDS = "bcfy_feeds"
    # Discrete call REST polling API collector for Broadcastify Calls
    # (bcfy_calls_collector.py). Does NOT pass through Dataflow segmentation.
    BCFY_CALLS = "bcfy_calls"
    # Echo uses a separate cloud function for ingestion instead of VMs.
    ECHO = "echo"
    OPENMHZ = "openmhz"
    FIRE_NOTIFICATIONS = "fire_notifications"


class FeedStatus(enum.StrEnum):
    """Lifecycle status of a feed, stored in the ``feeds.status`` column."""

    # Eligible for leasing by any worker.
    UNCLAIMED = "unclaimed"
    # Currently leased by a worker.
    ACTIVE = "active"
    # Lease held but feed is experiencing errors; still eligible for
    # leasing and processing.
    FAILING = "failing"
    # Ineligible for leasing due to repeated failures; requires manual
    # triage and reset.
    QUARANTINED = "quarantined"
    # Permanently ineligible for leasing; used for feeds that are deleted
    # or retired but kept for historical/triage purposes.
    DEACTIVATED = "deactivated"


_FEED_STATUS_REASON_OWNERS = frozenset({"source", "system", "pipeline"})


def _status_reason_owner(status_reason: str) -> str:
    """Return the owner namespace encoded by a status-reason prefix."""
    owner, separator, _ = status_reason.partition("_")
    if not separator or owner not in _FEED_STATUS_REASON_OWNERS:
        msg = f"Unsupported status reason owner in {status_reason!r}"
        raise ValueError(msg)
    return owner


class FeedStatusReason(enum.StrEnum):
    """Canonical abnormal feed reason stored in ``feeds.status_reason``."""

    PIPELINE_PUBLISH_AFTER_BOOKMARK_FAILED = (
        "pipeline_publish_after_bookmark_failed"
    )
    SOURCE_OFFLINE = "source_offline"
    SOURCE_UNREACHABLE = "source_unreachable"
    SOURCE_RATE_LIMITED = "source_rate_limited"
    SYSTEM_AUTHENTICATION_FAILED = "system_authentication_failed"
    SYSTEM_CONFIGURATION_INVALID = "system_configuration_invalid"
    SYSTEM_SOURCE_CONFIGURATION_INVALID = "system_source_configuration_invalid"
    SYSTEM_RUNTIME_CONFIGURATION_INVALID = (
        "system_runtime_configuration_invalid"
    )
    SYSTEM_CREDENTIAL_ACCESS_FAILED = "system_credential_access_failed"
    SYSTEM_SOURCE_PAYLOAD_INVALID = "system_source_payload_invalid"
    SYSTEM_COLLECTOR_ERROR = "system_collector_error"
    SYSTEM_PIPELINE_ERROR = "system_pipeline_error"
    SYSTEM_UNEXPECTED_ERROR = "system_unexpected_error"

    @property
    def owner(self) -> str:
        """Coarse owner namespace encoded by the reason prefix."""
        return _status_reason_owner(self.value)


@dataclass(frozen=True, slots=True)
class FeedGrant:
    """Complete immutable authority for one Feed ownership generation.

    Attributes:
        feed_id: Permanent Feed identity.
        owner_worker_id: Worker authorized for this ownership generation.
        fencing_token: Monotonic generation that rejects stale workers.
    """

    feed_id: uuid.UUID
    owner_worker_id: uuid.UUID
    fencing_token: int

    @property
    def unit_key(self) -> uuid.UUID:
        """Return the permanent identity within the Feed domain."""
        return self.feed_id

    def __post_init__(self) -> None:
        if not isinstance(self.feed_id, uuid.UUID):
            msg = "feed_id must be a UUID"
            raise TypeError(msg)
        if not isinstance(self.owner_worker_id, uuid.UUID):
            msg = "owner_worker_id must be a UUID"
            raise TypeError(msg)
        if isinstance(self.fencing_token, bool) or not isinstance(
            self.fencing_token,
            int,
        ):
            msg = "fencing_token must be an integer"
            raise TypeError(msg)
        if self.fencing_token < 0:
            msg = "fencing_token must be nonnegative"
            raise ValueError(msg)


class FeedGrantOperationDisposition(enum.StrEnum):
    """Closed outcomes for exact-grant Feed heartbeat operations.

    Attributes:
        APPLIED: The exact active heartbeat request was accepted.
        MISSING: The permanent Feed identity does not exist.
        OWNER_MISMATCH: Another worker owns the current generation.
        FENCE_MISMATCH: The supplied ownership generation is stale.
        STATUS_INELIGIBLE: The Feed is not active for heartbeat renewal.
    """

    APPLIED = "applied"
    MISSING = "missing"
    OWNER_MISMATCH = "owner_mismatch"
    FENCE_MISMATCH = "fence_mismatch"
    STATUS_INELIGIBLE = "status_ineligible"


@dataclass(frozen=True, slots=True)
class FeedGrantHeartbeatResult:
    """Caller-correlated result for one exact Feed grant heartbeat.

    Attributes:
        grant: Original caller grant associated with this result.
        disposition: Closed classification of the heartbeat attempt.
    """

    grant: FeedGrant
    disposition: FeedGrantOperationDisposition


class LeasedFeed(TypedDict):
    """Feed details returned after a successful lease acquisition."""

    id: uuid.UUID
    name: str
    source_type: SourceType
    last_processed_filename: str | None
    last_bookmark_time: datetime.datetime | None
    fencing_token: int
    failure_count: int
    status_reason: FeedStatusReason | None
    source_feed_id: str | None
    tags: NotRequired[list[dict[str, str]] | None]


class SourceObservationResult(TypedDict):
    """Diagnostic result for recording a non-audio source observation."""

    id: uuid.UUID
    current_worker: uuid.UUID | None
    current_status: str | None
    current_fencing_token: int | None
    recorded: bool


class Feed(TypedDict):
    """Full feed details.

    ``status`` and its reason/heartbeat columns are the raw child
    lifecycle, used by admin action eligibility. The ``effective_*``
    fields project the parent ingestion Lease health for maintained SID
    members (and mirror the child for every other row); the display layer
    should use them. ``bcfy_calls_sid``/``lease_*`` expose the raw SID
    management metadata.
    """

    id: uuid.UUID
    name: str
    source_type: SourceType
    status: FeedStatus
    status_reason: FeedStatusReason | None
    status_reason_updated_at: datetime.datetime | None
    status_reason_detail: str | None
    failure_count: int
    worker_id: uuid.UUID | None
    last_heartbeat: datetime.datetime | None
    last_processed_filename: str | None
    last_bookmark_time: datetime.datetime | None
    created_at: datetime.datetime
    source_feed_id: str | None
    tags: list[dict[str, str]] | None
    last_speech_segment_timestamp: datetime.datetime | None
    bcfy_calls_sid: str | None
    lease_status: FeedStatus | None
    lease_last_heartbeat: datetime.datetime | None
    lease_status_reason: FeedStatusReason | None
    effective_status: FeedStatus
    effective_status_reason: FeedStatusReason | None
    effective_status_reason_detail: str | None
    effective_last_heartbeat: datetime.datetime | None


@dataclass
class PaginatedFeeds:
    feeds: list[Feed]
    next_token: str | None
    total: int


class FeedAuditEvent(TypedDict):
    id: uuid.UUID
    feed_id: uuid.UUID
    action: str
    actor_id: str
    occurred_at: datetime.datetime
    feed_revision: int
    before_values: dict
    after_values: dict


@dataclass
class PaginatedFeedAuditEvents:
    audit_events: list[FeedAuditEvent]
    next_token: str | None
    total: int


def _require_actor_id(actor_id: str | None) -> str:
    if actor_id is None:
        msg = "actor_id is required for audited feed lifecycle writes"
        raise ValueError(msg)
    return actor_id


def _feed_status_from_value(
    value: object,
    feed_id: object,
    field_name: str,
) -> FeedStatus:
    try:
        return FeedStatus(value)
    except ValueError as e:
        msg = f"Unknown {field_name} {value!r} for feed {feed_id}"
        raise ValueError(msg) from e


def _feed_status_reason_from_value(
    value: object,
    feed_id: object,
    field_name: str,
) -> FeedStatusReason | None:
    if value is None:
        return None
    try:
        return FeedStatusReason(value)
    except ValueError as e:
        msg = f"Unknown {field_name} {value!r} for feed {feed_id}"
        raise ValueError(msg) from e


def _parse_sid_source_feed_id(source_feed_id: str) -> tuple[str, str]:
    """Split a Calls ``source_feed_id`` into its canonical SID and group.

    The maintained membership constraint requires ``source_feed_id`` to be
    exactly ``'<sid>-<group_id>'`` with numeric ASCII components; parsing
    here keeps the store boundary safe for callers that bypass the API
    model validation.
    """
    sid, separator, group_id = source_feed_id.partition("-")
    if (
        separator != "-"
        or not sid
        or not sid.isascii()
        or not sid.isdigit()
        or not group_id
        or not group_id.isascii()
        or not group_id.isdigit()
    ):
        msg = (
            "bcfy_calls source_feed_id must be '<sid>-<group_id>' with "
            "numeric components"
        )
        raise ValueError(msg)
    return sid, group_id


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
            per type. Defaults to the source types owned through Feed
            grants. ``CollectorRuntime`` passes
            ``list(settings.feed_claim_caps)`` explicitly.
        heartbeat_timeout_sec: Optional total timeout for heartbeat pool
            checkout, query execution, and connection release.

    """

    def __init__(
        self,
        pool: asyncpg.Pool,
        claim_types: collections.abc.Sequence[SourceType] | None = None,
        *,
        heartbeat_timeout_sec: float | None = None,
    ) -> None:
        self._pool = pool
        self._heartbeat_timeout_sec = heartbeat_timeout_sec
        if claim_types is None:
            claim_types = [
                SourceType.BCFY_FEEDS,
                SourceType.OPENMHZ,
                SourceType.FIRE_NOTIFICATIONS,
            ]
        self._claim_types: tuple[SourceType, ...] = tuple(claim_types)
        self._acquire_feeds_batch_sql = (
            feed_queries.build_acquire_feeds_batch_sql(
                self._claim_types,
            )
        )
        self._acquire_feeds_recovery_sql = (
            feed_queries.build_acquire_feeds_recovery_sql(
                self._claim_types,
            )
        )

    def _row_to_feed(self, row: asyncpg.Record) -> Feed:
        """Convert a database row to a Feed dict with validation."""
        try:
            source_type = SourceType(row["source_type"])
        except ValueError as e:
            msg = (
                f"Unknown source type {row['source_type']!r} "
                f"for feed {row['id']}"
            )
            raise ValueError(msg) from e
        try:
            status = FeedStatus(row["status"])
        except ValueError as e:
            msg = f"Unknown status {row['status']!r} for feed {row['id']}"
            raise ValueError(msg) from e
        status_reason_raw = row["status_reason"]
        if status_reason_raw is None:
            status_reason = None
        else:
            try:
                status_reason = FeedStatusReason(status_reason_raw)
            except ValueError as e:
                msg = (
                    f"Unknown status reason {status_reason_raw!r} "
                    f"for feed {row['id']}"
                )
                raise ValueError(msg) from e

        tags = row.get("tags")
        if tags is not None:
            tags = json.loads(tags)

        # Read queries project lease-aware effective health; audited
        # mutation rows do not carry those columns and fall back to the
        # child's own lifecycle so one decoder serves both row shapes.
        feed_id = row["id"]
        effective_status_raw = row.get("effective_status")
        if effective_status_raw is None:
            effective_status = status
            effective_status_reason = status_reason
            effective_status_reason_detail = row["status_reason_detail"]
            effective_last_heartbeat = row["last_heartbeat"]
        else:
            effective_status = _feed_status_from_value(
                effective_status_raw,
                feed_id,
                "effective status",
            )
            effective_status_reason = _feed_status_reason_from_value(
                row.get("effective_status_reason"),
                feed_id,
                "effective status reason",
            )
            effective_status_reason_detail = row.get(
                "effective_status_reason_detail"
            )
            effective_last_heartbeat = row.get("effective_last_heartbeat")

        lease_status_raw = row.get("lease_status")
        lease_status = (
            None
            if lease_status_raw is None
            else _feed_status_from_value(
                lease_status_raw,
                feed_id,
                "lease status",
            )
        )

        return Feed(
            id=feed_id,
            name=row["name"],
            source_type=source_type,
            status=status,
            status_reason=status_reason,
            status_reason_updated_at=row["status_reason_updated_at"],
            status_reason_detail=row["status_reason_detail"],
            failure_count=row["failure_count"],
            worker_id=row["worker_id"],
            last_heartbeat=row["last_heartbeat"],
            last_processed_filename=row["last_processed_filename"],
            last_bookmark_time=row["last_bookmark_time"],
            created_at=row["created_at"],
            source_feed_id=row["source_feed_id"],
            tags=tags,
            last_speech_segment_timestamp=row["last_speech_segment_timestamp"],
            bcfy_calls_sid=row.get("bcfy_calls_sid"),
            lease_status=lease_status,
            lease_last_heartbeat=row.get("lease_last_heartbeat"),
            lease_status_reason=_feed_status_reason_from_value(
                row.get("lease_status_reason"),
                feed_id,
                "lease status reason",
            ),
            effective_status=effective_status,
            effective_status_reason=effective_status_reason,
            effective_status_reason_detail=effective_status_reason_detail,
            effective_last_heartbeat=effective_last_heartbeat,
        )

    @staticmethod
    def _is_expected_unique_violation(
        error: asyncpg.exceptions.UniqueViolationError,
        expected_constraints: frozenset[str],
    ) -> bool:
        constraint_name = getattr(error, "constraint_name", None)
        return constraint_name in expected_constraints

    def _row_to_leased_feed(self, row: asyncpg.Record) -> LeasedFeed:
        """Convert a claim/lease-path row to a LeasedFeed dict.

        Shared between acquire_feeds_batch (primary per-type CTE) and
        acquire_feeds_recovery (failing-retryable + active-abandoned).
        Keeping the mapping in one place ensures both claim paths agree
        on how source_type is validated and which columns end up in the
        TypedDict.
        """
        try:
            source_type = SourceType(row["source_type"])
        except ValueError as e:
            msg = (
                f"Unknown source type {row['source_type']!r} "
                f"for feed {row['id']}"
            )
            raise ValueError(msg) from e
        status_reason_raw = row["status_reason"]
        tags_raw = row.get("tags")
        tags = None
        if tags_raw is not None:
            tags = (
                json.loads(tags_raw) if isinstance(tags_raw, str) else tags_raw
            )

        return LeasedFeed(
            id=row["id"],
            name=row["name"],
            source_type=source_type,
            last_processed_filename=row["last_processed_filename"],
            last_bookmark_time=row["last_bookmark_time"],
            fencing_token=row["fencing_token"],
            failure_count=row["failure_count"],
            status_reason=(
                FeedStatusReason(status_reason_raw)
                if status_reason_raw is not None
                else None
            ),
            source_feed_id=row["source_feed_id"],
            tags=tags,
        )

    async def update_feed_progress(
        self,
        feed_id: uuid.UUID,
        worker_id: uuid.UUID,
        new_gcs_path: str,
        fencing_token: int,
        last_bookmark_time: datetime.datetime | None,
        *,
        actor_id: str,
    ) -> bool:
        """
        Update the feed's bookmark and heartbeat after a successful write.

        This is a fenced operation — it only succeeds if the given worker still
        holds the lease AND the fencing token matches. It intentionally remains
        allowed for deactivated rows so one in-flight bookmark can finish after
        an admin stop.

        Args:
            feed_id: UUID of the feed to update.
            worker_id: UUID of the worker holding the lease.
            new_gcs_path: The GCS object path of the last successfully written
                file.
            fencing_token: The fencing token received at lease acquisition.
            last_bookmark_time: Timestamp bookmark for the last processed audio.
            actor_id: Causal actor for audited runtime recovery.

        Returns:
            ``True`` if the update succeeded (lease still held), ``False`` if
            the lease was lost.

        """
        row = await self._pool.fetchrow(
            feed_queries.UPDATE_PROGRESS_SQL,
            new_gcs_path,
            feed_id,
            worker_id,
            fencing_token,
            last_bookmark_time,
            _require_actor_id(actor_id),
        )
        if row is None:
            return False
        feed_change_notifications.emit_feed_change_notification(
            row.get("feed_audit_event")
        )
        return True

    async def record_source_observation(
        self,
        feed_id: uuid.UUID,
        worker_id: uuid.UUID,
        fencing_token: int,
        resume_position: datetime.datetime | None,
        *,
        actor_id: str,
    ) -> SourceObservationResult:
        """Record a non-audio source success through a fenced diagnostic path.

        This clears stale failure state for an active leased feed without
        claiming audio progress. If *resume_position* is provided, it advances
        the source cursor in ``last_bookmark_time``.
        """
        row = await self._pool.fetchrow(
            feed_queries.RECORD_SOURCE_OBSERVATION_SQL,
            feed_id,
            worker_id,
            fencing_token,
            resume_position,
            _require_actor_id(actor_id),
        )
        if row is None:
            return SourceObservationResult(
                id=feed_id,
                current_worker=None,
                current_status=None,
                current_fencing_token=None,
                recorded=False,
            )
        result = SourceObservationResult(
            id=row["id"],
            current_worker=row["current_worker"],
            current_status=row["current_status"],
            current_fencing_token=row["current_fencing_token"],
            recorded=row["recorded"],
        )
        feed_change_notifications.emit_feed_change_notification(
            row.get("feed_audit_event")
        )
        return result

    async def renew_grant_heartbeats(
        self,
        grants: collections.abc.Sequence[FeedGrant],
    ) -> tuple[FeedGrantHeartbeatResult, ...]:
        """Renew complete active Feed grants in exact caller order.

        Args:
            grants: Complete Feed grants to renew. A Feed identity may appear
                at most once, even with a different owner or fence.

        Returns:
            One exact, caller-correlated result for every input grant.

        Raises:
            ValueError: If inputs repeat or results are malformed or
                miscorrelated.
        """
        grants = tuple(grants)
        feed_ids: set[uuid.UUID] = set()
        for grant in grants:
            if grant.feed_id in feed_ids:
                msg = f"duplicate Feed identity {grant.feed_id}"
                raise ValueError(msg)
            feed_ids.add(grant.feed_id)
        if not grants:
            return ()

        ordered = sorted(
            enumerate(grants),
            key=lambda item: item[1].feed_id.int,
        )
        rows = await connection.fetch_with_timeout_budget(
            self._pool,
            feed_queries.RENEW_GRANT_HEARTBEATS_SQL,
            [grant.feed_id for _, grant in ordered],
            [grant.owner_worker_id for _, grant in ordered],
            [grant.fencing_token for _, grant in ordered],
            [ordinal for ordinal, _ in ordered],
            timeout_sec=self._heartbeat_timeout_sec,
        )

        expected_by_ordinal = dict(enumerate(grants))
        rows_by_ordinal: dict[
            int,
            collections.abc.Mapping[str, object],
        ] = {}
        for row in rows:
            try:
                caller_ordinal = row["caller_ordinal"]
                returned_feed_id = row["feed_id"]
            except (KeyError, TypeError) as error:
                msg = "Feed heartbeat returned a malformed row"
                raise ValueError(msg) from error
            if (
                isinstance(caller_ordinal, bool)
                or not isinstance(caller_ordinal, int)
                or caller_ordinal not in expected_by_ordinal
                or caller_ordinal in rows_by_ordinal
                or returned_feed_id
                != expected_by_ordinal[caller_ordinal].feed_id
            ):
                msg = "Feed heartbeat returned an unexpected or duplicate row"
                raise ValueError(msg)
            rows_by_ordinal[caller_ordinal] = row

        if set(rows_by_ordinal) != set(expected_by_ordinal):
            msg = "Feed heartbeat did not return every caller input"
            raise ValueError(msg)

        return tuple(
            self._grant_heartbeat_result(
                grant,
                rows_by_ordinal[ordinal],
            )
            for ordinal, grant in enumerate(grants)
        )

    def _grant_heartbeat_result(
        self,
        grant: FeedGrant,
        row: collections.abc.Mapping[str, object],
    ) -> FeedGrantHeartbeatResult:
        """Classify one validated caller-correlated heartbeat row."""
        required_fields = (
            "status",
            "worker_id",
            "fencing_token",
        )
        if any(field not in row for field in required_fields):
            msg = "Feed heartbeat row is missing required state fields"
            raise ValueError(msg)

        status_raw = row.get("status")
        if status_raw is None:
            if any(
                row.get(field) is not None
                for field in ("worker_id", "fencing_token")
            ):
                msg = "missing Feed heartbeat row contains current state"
                raise ValueError(msg)
            return FeedGrantHeartbeatResult(
                grant,
                FeedGrantOperationDisposition.MISSING,
            )

        try:
            status = FeedStatus(status_raw)
        except (TypeError, ValueError) as error:
            msg = "Feed heartbeat row contains an invalid status"
            raise ValueError(msg) from error

        worker_id = row.get("worker_id")
        if worker_id is not None and not isinstance(worker_id, uuid.UUID):
            msg = "Feed heartbeat row contains an invalid worker_id"
            raise ValueError(msg)
        fencing_token = row.get("fencing_token")
        if (
            isinstance(fencing_token, bool)
            or not isinstance(fencing_token, int)
            or fencing_token < 0
        ):
            msg = "Feed heartbeat row contains an invalid fencing_token"
            raise ValueError(msg)

        if status is not FeedStatus.ACTIVE:
            disposition = FeedGrantOperationDisposition.STATUS_INELIGIBLE
        elif worker_id != grant.owner_worker_id:
            disposition = FeedGrantOperationDisposition.OWNER_MISMATCH
        elif fencing_token != grant.fencing_token:
            disposition = FeedGrantOperationDisposition.FENCE_MISMATCH
        else:
            disposition = FeedGrantOperationDisposition.APPLIED

        return FeedGrantHeartbeatResult(grant, disposition)

    async def report_feed_failure(
        self,
        feed_id: uuid.UUID,
        worker_id: uuid.UUID,
        fencing_token: int,
        failure_threshold: int = feed_lifecycle.DEFAULT_FAILURE_THRESHOLD,
        backoff_base_sec: int = feed_lifecycle.DEFAULT_BACKOFF_BASE_SEC,
        backoff_max_sec: int = feed_lifecycle.DEFAULT_BACKOFF_MAX_SEC,
        *,
        actor_id: str,
        reason: str | None = None,
        status_reason: FeedStatusReason | None = None,
    ) -> str | None:
        """Report a feed failure with exponential backoff.

        Atomically increments ``failure_count``, computes ``retry_after``
        with exponential backoff + jitter, and transitions to
        ``'quarantined'`` if *failure_threshold* is reached. The canonical
        *status_reason* is stored in ``feeds.status_reason``, and diagnostic
        *reason* text is stored in ``feeds.status_reason_detail``.

        Backoff formula: ``min(backoff_base_sec * 2^failure_count,
        backoff_max_sec) + random(0-10s) jitter``.

        This is a fenced operation — it only succeeds if the given worker
        still holds an active lease AND the fencing token matches. A
        deactivated feed is an administrative terminal state until reset, so
        failure reporting returns ``None`` instead of changing status.

        Args:
            feed_id: UUID of the feed that failed.
            worker_id: UUID of the worker reporting the failure.
            fencing_token: The fencing token received at lease acquisition.
            failure_threshold: Number of consecutive failures before
                quarantine.
            backoff_base_sec: Base delay in seconds for the first retry.
            backoff_max_sec: Maximum backoff cap in seconds.
            actor_id: Causal actor for audited runtime failure events.
            reason: Diagnostic failure text. Persisted to
                ``feeds.status_reason_detail`` after applying the storage
                boundary length cap.
            status_reason: Canonical reason code for the current abnormal
                feed condition. ``None`` lets SQL store the compatibility
                fallback ``system_unexpected_error``.

        Returns:
            The new feed status (``'failing'`` or ``'quarantined'``) if
            the failure was recorded, or ``None`` if the lease was
            already lost.

        """
        status_reason_value = feed_lifecycle.status_reason_storage_value(
            status_reason
        )
        status_reason_detail = (
            feed_lifecycle.status_reason_detail_storage_value(reason)
        )
        required_actor_id = _require_actor_id(actor_id)
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                feed_queries.REPORT_FAILURE_SQL,
                feed_id,
                worker_id,
                failure_threshold,
                fencing_token,
                backoff_max_sec,
                backoff_base_sec,
                status_reason_value,
                status_reason_detail,
                required_actor_id,
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
                    "reason": reason,
                },
            )
        feed_change_notifications.emit_feed_change_notification(
            row.get("feed_audit_event")
        )
        return status

    async def release_non_budgeted_failure(
        self,
        feed_id: uuid.UUID,
        worker_id: uuid.UUID,
        fencing_token: int,
        *,
        retry_after: datetime.datetime,
        status_reason: FeedStatusReason,
        actor_id: str,
        reason: str | None = None,
    ) -> str | None:
        """Release a non-feed-budgeted failure into retryable failing state.

        This fenced path is for failures that should not consume the feed
        quarantine budget: post-capture pipeline failures, unannotated
        collector failures, source-class incidents, and unknown evidence. It
        resets any previous consecutive feed budget and releases the lease for
        later retry.
        """
        required_actor_id = _require_actor_id(actor_id)
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                feed_queries.RELEASE_NON_BUDGETED_FAILURE_SQL,
                feed_id,
                worker_id,
                fencing_token,
                retry_after,
                status_reason.value,
                feed_lifecycle.status_reason_detail_storage_value(reason),
                required_actor_id,
            )
        if row is None:
            return None
        status: str = row["status"]
        feed_change_notifications.emit_feed_change_notification(
            row.get("feed_audit_event")
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
        still holds an active lease AND the fencing token matches. A
        deactivated feed is an administrative terminal state until reset, so
        release is a no-op for deactivated rows.

        Args:
            feed_id: UUID of the feed to release.
            worker_id: UUID of the worker releasing the lease.
            fencing_token: The fencing token received at lease acquisition.

        Returns:
            ``True`` if the lease was released, ``False`` if the lease was
            already lost.

        """
        result = await self._pool.execute(
            feed_queries.RELEASE_FEED_SQL,
            feed_id,
            worker_id,
            fencing_token,
        )
        return result == "UPDATE 1"

    async def acquire_feeds_batch(
        self,
        worker_id: uuid.UUID,
        limits: dict[SourceType, int],
    ) -> list[LeasedFeed]:
        """
        Batch-acquire unclaimed feeds via the per-type UNION ALL MATERIALIZED
        CTE.

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
            *positional,
        )
        return [self._row_to_leased_feed(row) for row in rows]

    async def acquire_feeds_recovery(
        self,
        worker_id: uuid.UUID,
        abandonment_window_sec: float,
        limits: dict[SourceType, int],
    ) -> list[LeasedFeed]:
        """Recovery-path claim: failing-retryable + active-abandoned.

        Called by the worker when the per-type primary CTE returns fewer
        rows than the worker's total slack. Mirrors ``acquire_feeds_batch``
        in shape: each branch has its own LIMIT keyed on ``SourceType``
        and its own SKIP LOCKED scope, so per-type caps are enforced
        structurally even when one type's primary path returns 0 and the
        recovery sweep would otherwise scoop up rows of an at-cap type.

        Args:
            worker_id: UUID of the worker requesting leases.
            abandonment_window_sec: Seconds before a heartbeat is considered
                stale.
            limits: Per-type recovery LIMIT keyed by ``SourceType``.
                Types absent from the dict are passed as 0 (their CTE
                branch returns no rows). Types present but not in this
                store's ``claim_types`` raise ``ValueError``.

        Returns:
            List of ``LeasedFeed`` dicts (empty if all limits are 0 or
            no eligible rows exist).
        """
        unknown = set(limits) - set(self._claim_types)
        if unknown:
            msg = (
                "limits contains source types not in this store's claim_types: "
                f"{sorted(t.value for t in unknown)}"
            )
            raise ValueError(msg)

        positional = [limits.get(t, 0) for t in self._claim_types]
        if not any(v > 0 for v in positional):
            # Every branch has LIMIT 0 — skip the round-trip.
            return []

        import datetime  # noqa: PLC0415

        rows = await self._pool.fetch(
            self._acquire_feeds_recovery_sql,
            worker_id,
            datetime.timedelta(seconds=abandonment_window_sec),
            *positional,
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
        skipped. ``feed_queries.CREATE_FEED_SQL`` enforces referential integrity
        against ``source_types``, so this should never trigger in
        practice — the skip path is defensive against a hypothetical
        schema drift.

        Args:
            worker_id: UUID of the worker whose held leases to count.

        Returns:
            Dict mapping every ``SourceType`` to the count of active
            leases owned by this worker for that type (0 if none).
        """
        rows = await self._pool.fetch(
            feed_queries.COUNT_HELD_BY_TYPE_SQL, worker_id
        )
        counts: dict[SourceType, int] = dict.fromkeys(SourceType, 0)
        for row in rows:
            try:
                counts[SourceType(row["source_type"])] = row["n"]
            except ValueError:
                continue
        return counts

    async def create_feed(
        self,
        name: str,
        source_type: str | SourceType,
        source_feed_id: str,
        tags: list[dict[str, str]] | None = None,
        *,
        actor_id: str,
    ) -> Feed:
        """Create a new feed record.

        Atomically creates a new feed in the `feeds` table and its corresponding
        properties in the `feed_properties` table.

        Broadcastify Calls feeds are created as maintained SID members: the
        canonical SID/group identity is persisted, the permanent parent
        Lease is created or locked, its membership revision advances, and
        the child is inserted as an enabled member with a NULL cursor — all
        in one transaction.
        """
        required_actor_id = _require_actor_id(actor_id)
        if not source_feed_id:
            msg = "source_feed_id cannot be empty"
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

        try:
            if source_type_str == SourceType.BCFY_CALLS.value:
                row = await self._create_sid_feed_row(
                    name,
                    source_feed_id,
                    tags,
                    required_actor_id,
                )
            else:
                async with self._pool.acquire() as conn:
                    row = await conn.fetchrow(
                        feed_queries.CREATE_FEED_SQL,
                        name,
                        source_type_str,
                        source_feed_id,
                        json.dumps(tags or []),
                        required_actor_id,
                    )
            if row is None:
                msg = f"Failed to create feed {name}"
                raise ValueError(msg)
        except asyncpg.exceptions.UniqueViolationError as e:
            if not self._is_expected_unique_violation(
                e,
                _CREATE_FEED_UNIQUE_CONSTRAINTS,
            ):
                raise
            logger.warning(
                "Feed already exists",
                extra={
                    "source_type": source_type_str,
                    "source_feed_id": source_feed_id,
                },
            )
            raise FeedAlreadyExistsError(source_type_str, source_feed_id) from e
        except asyncpg.exceptions.ForeignKeyViolationError as e:
            logger.warning(
                "Invalid source type provided",
                extra={
                    "source_type": source_type_str,
                },
            )
            msg = f"Invalid source type '{source_type_str}'"
            raise ValueError(msg) from e

        feed = self._row_to_feed(row)
        feed_change_notifications.emit_feed_change_notification(
            row.get("feed_audit_event")
        )
        return feed

    async def _create_sid_feed_row(
        self,
        name: str,
        source_feed_id: str,
        tags: list[dict[str, str]] | None,
        actor_id: str,
    ) -> asyncpg.Record | None:
        """Create one SID-managed Calls member beneath its locked parent.

        One transaction: insert an absent parent Lease as ``unclaimed``
        (fence 0, revision 1), or lock the existing parent and advance its
        revision (reactivating a ``deactivated`` parent to a clean
        ``unclaimed`` with a fresh failure budget, while preserving an
        active owner or failing backoff exactly), then insert the enabled
        child. Any uniqueness or audit failure rolls
        back the Lease insert/revision along with the Feed, so a retry
        cannot allocate a second revision or event.
        """
        sid, group_id = _parse_sid_source_feed_id(source_feed_id)
        async with self._pool.acquire() as conn:
            async with conn.transaction(isolation="read_committed"):
                inserted = await conn.fetchrow(
                    feed_sid_admin_queries.INSERT_UNCLAIMED_PARENT_LEASE_SQL,
                    sid,
                )
                lease_row = await conn.fetchrow(
                    ingestion_lease_queries.LOCK_LEASE_SQL,
                    SourceType.BCFY_CALLS.value,
                    sid,
                )
                if lease_row is None:
                    msg = f"bcfy_calls SID Lease {sid!r} is unexpectedly absent"
                    raise RuntimeError(msg)
                if inserted is None:
                    await conn.execute(
                        feed_sid_admin_queries.REGISTER_MEMBER_ON_EXISTING_LEASE_SQL,
                        sid,
                    )
                return await conn.fetchrow(
                    feed_sid_admin_queries.CREATE_SID_FEED_SQL,
                    name,
                    source_feed_id,
                    json.dumps(tags or []),
                    sid,
                    group_id,
                    actor_id,
                )

    async def _get_sid_lease_key(self, feed_id: uuid.UUID) -> str | None:
        """Resolve a maintained SID membership for admin routing, lock-free.

        Returns ``None`` for non-Calls feeds and legacy Calls rows whose
        maintained membership fields are NULL; those keep the legacy admin
        paths. The SID branch re-verifies this identity under the child
        lock, so a stale read here degrades to not-found, never to a
        different parent.
        """
        row = await self._pool.fetchrow(
            feed_sid_admin_queries.GET_SID_MEMBERSHIP_KEY_SQL,
            feed_id,
        )
        if row is None:
            return None
        return row.get("sid")

    async def _deactivate_sid_feed(
        self,
        feed_id: uuid.UUID,
        sid: str,
        actor_id: str,
    ) -> bool:
        """Deactivate one SID child using the parent-first lock order.

        The parent Lease row is locked before the child Feed, matching the
        worker's membership-refresh and child-commit order. If the
        permanent Lease row is absent (a configuration error surfaced by
        the health projection), the child statement still applies and the
        lease update simply matches no row.
        """
        async with self._pool.acquire() as conn:
            async with conn.transaction(isolation="read_committed"):
                await conn.fetchrow(
                    ingestion_lease_queries.LOCK_LEASE_SQL,
                    SourceType.BCFY_CALLS.value,
                    sid,
                )
                row = await conn.fetchrow(
                    feed_sid_admin_queries.DEACTIVATE_SID_CHILD_SQL,
                    feed_id,
                    sid,
                    actor_id,
                )
        if row is None:
            return False
        feed_change_notifications.emit_feed_change_notification(
            row.get("feed_audit_event")
        )
        return True

    async def _reset_sid_feed(
        self,
        feed_id: uuid.UUID,
        sid: str,
        actor_id: str,
    ) -> Feed | None:
        """Reset one SID child using the parent-first lock order.

        Supported for active and inactive parents; the parent branch is
        decided inside the SQL from the locked Lease row. An inactive
        parent is reset to clean ``unclaimed`` even when the child itself
        is already clean. Only when neither the child nor the parent needs
        a change is the reset an idempotent no-op with no audit event or
        revision bump.
        """
        async with self._pool.acquire() as conn:
            async with conn.transaction(isolation="read_committed"):
                await conn.fetchrow(
                    ingestion_lease_queries.LOCK_LEASE_SQL,
                    SourceType.BCFY_CALLS.value,
                    sid,
                )
                row = await conn.fetchrow(
                    feed_sid_admin_queries.RESET_SID_CHILD_SQL,
                    feed_id,
                    sid,
                    actor_id,
                )
        if row is None:
            return None
        feed = self._row_to_feed(row)
        feed_change_notifications.emit_feed_change_notification(
            row.get("feed_audit_event")
        )
        return feed

    async def update_feed(
        self,
        feed_id: uuid.UUID,
        name: str,
        tags: list[dict[str, str]] | None = None,
        *,
        actor_id: str,
    ) -> Feed | None:
        """Update an existing feed record.

        Updates the feed in the `feeds` table and its corresponding
        properties in the `feed_properties` table.
        """
        required_actor_id = _require_actor_id(actor_id)
        try:
            async with self._pool.acquire() as conn:
                row = await conn.fetchrow(
                    feed_queries.UPDATE_FEED_SQL,
                    feed_id,
                    name,
                    json.dumps(tags or []),
                    required_actor_id,
                )
            if row is None:
                return None
        except asyncpg.exceptions.UniqueViolationError as e:
            if not self._is_expected_unique_violation(
                e,
                _UPDATE_FEED_UNIQUE_CONSTRAINTS,
            ):
                raise
            logger.warning(
                "Feed update conflicts with existing feed name",
                extra={
                    "feed_name": name,
                },
            )
            raise FeedNameAlreadyExistsError(name) from e

        logger.info(
            "Feed updated successfully",
            extra={
                "feed_id": str(feed_id),
                "feed_name": name,
            },
        )
        feed = self._row_to_feed(row)
        feed_change_notifications.emit_feed_change_notification(
            row.get("feed_audit_event")
        )
        return feed

    async def get_feed(self, feed_id: uuid.UUID) -> Feed | None:
        """Fetch a specific feed by ID.

        Retrieves feed details including properties from `feed_properties`.
        """
        row = await self._pool.fetchrow(feed_queries.GET_FEED_SQL, feed_id)
        if row is None:
            return None

        return self._row_to_feed(row)

    async def list_feeds(
        self,
        *,
        limit: int = 100,
        next_token: str | None = None,
        order: SortOrder = SortOrder.DESC,
        source_types: list[SourceType] | None = None,
        statuses: list[FeedStatus] | None = None,
        tags: list[dict[str, str]] | None = None,
        name: str | None = None,
    ) -> PaginatedFeeds:
        """List all feeds with keyset pagination and optional filters.

        Retrieves feeds ordered by creation time, using timestamp+ID-based
        keyset pagination.
        """
        if limit < 1:
            msg = "limit must be >= 1"
            raise ValueError(msg)

        cursor_ts = None
        cursor_uid = None
        if next_token:
            cursor_ts, cursor_uid = decode_cursor(next_token)

        is_asc = order == SortOrder.ASC or order == "asc"
        query = (
            feed_queries.LIST_FEEDS_ASC_SQL
            if is_asc
            else feed_queries.LIST_FEEDS_DESC_SQL
        )

        tags_json = None
        if tags:
            tags_json = json.dumps(tags)

        rows_task = self._pool.fetch(
            query,
            cursor_ts,
            cursor_uid,
            source_types,
            statuses,
            tags_json,
            name,
            limit + 1,
        )
        total_task = self._pool.fetchval(
            feed_queries.COUNT_FEEDS_SQL,
            source_types,
            statuses,
            tags_json,
            name,
        )

        rows, total = await asyncio.gather(rows_task, total_task)

        rows, new_next_token = get_paginated_results(
            rows, limit, "created_at", "id"
        )

        feeds = [self._row_to_feed(row) for row in rows]
        return PaginatedFeeds(feeds, new_next_token, total)

    async def deactivate_feed(
        self,
        feed_id: uuid.UUID,
        *,
        actor_id: str,
    ) -> bool:
        """Deactivate a feed by ID.

        Deactivation is an administrative terminal state until reset. The
        active worker metadata is intentionally preserved so the heartbeat
        path can cancel any running task gracefully.

        Returns True if the feed exists. Already-deactivated feeds are treated
        as a no-op and do not create another audit event.

        SID-managed Calls members are deactivated beneath their locked
        parent Lease: the parent revision advances once, and when the final
        eligible member is removed the parent is deactivated and unowned;
        otherwise an active parent's authority is preserved exactly.
        """
        required_actor_id = _require_actor_id(actor_id)
        sid = await self._get_sid_lease_key(feed_id)
        if sid is not None:
            return await self._deactivate_sid_feed(
                feed_id,
                sid,
                required_actor_id,
            )
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                feed_queries.DEACTIVATE_FEED_SQL,
                feed_id,
                required_actor_id,
            )
        if row is None:
            return False
        feed_change_notifications.emit_feed_change_notification(
            row.get("feed_audit_event")
        )
        return True

    async def delete_feed(
        self,
        feed_id: uuid.UUID,
        *,
        actor_id: str,
    ) -> bool:
        """Hard delete a feed by ID.

        Deletes the feed itself, along with all referencing database entities
        (transcripts, audio segments, annotations, and feed properties).

        Returns True if the feed was successfully deleted, False otherwise.

        SID-managed Calls members are deleted in two transactions: a short
        parent-first membership detach (identical to deactivate, an
        idempotent no-op when already detached), then the hard cleanup
        below, which locks only the Feed and never holds the parent Lease
        lock. The permanent parent Lease row is never deleted, and a retry
        after a failed cleanup resumes without another membership revision.
        A detached delete therefore emits ``feed.deactivated`` (when the
        detach actually changed lifecycle) followed by ``feed.deleted``.
        """
        required_actor_id = _require_actor_id(actor_id)
        sid = await self._get_sid_lease_key(feed_id)
        if sid is not None:
            await self._deactivate_sid_feed(feed_id, sid, required_actor_id)
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                feed_queries.DELETE_FEED_SQL,
                feed_id,
                required_actor_id,
            )
        if row is None:
            return False
        if row["blocked_active"]:
            raise FeedStateConflictError(
                str(feed_id),
                "deleted",
                row["current_status"],
            )
        deleted = bool(row["deleted"])
        feed_change_notifications.emit_feed_change_notification(
            row.get("feed_audit_event")
        )
        return deleted

    async def reset_feed(
        self,
        feed_id: uuid.UUID,
        *,
        actor_id: str,
    ) -> Feed | None:
        """Reset a feed to an unclaimed, unassigned state.

        This is the explicit reactivation path for deactivated or quarantined
        feeds. Sets ``status = 'unclaimed'``, ``failure_count = 0``, clears
        ``worker_id``, and updates ``last_heartbeat`` for the given feed.
        Returns the updated feed, or ``None`` if no feed with that ID exists.

        Args:
            feed_id: UUID of the feed to reset.
            actor_id: Required audit actor ID for the reset event.

        Returns:
            The updated ``Feed`` dict, or ``None`` if the feed was not found.

        SID-managed Calls members take a dedicated parent-first reset that
        supports active and inactive parents: the child becomes an enabled
        member with a cleared cursor for page-boundary re-adoption, and the
        legacy active-feed conflict below does not apply. Legacy rows keep
        the existing behavior, including the active conflict.
        """
        required_actor_id = _require_actor_id(actor_id)
        sid = await self._get_sid_lease_key(feed_id)
        if sid is not None:
            return await self._reset_sid_feed(feed_id, sid, required_actor_id)
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                feed_queries.RESET_FEED_SQL,
                feed_id,
                required_actor_id,
            )
        if row is None:
            return None
        if row["blocked_active"]:
            raise FeedStateConflictError(
                str(feed_id),
                "reset",
                row["current_status"],
            )
        if row["id"] is None:
            return None
        feed = self._row_to_feed(row)
        feed_change_notifications.emit_feed_change_notification(
            row.get("feed_audit_event")
        )
        return feed

    async def list_feed_history_records(
        self,
        feed_id: uuid.UUID,
        *,
        limit: int = 100,
        next_token: str | None = None,
        order: SortOrder = SortOrder.DESC,
    ) -> PaginatedFeedAuditEvents:
        """List audit events for a feed with keyset pagination.

        Args:
            feed_id: UUID of the feed whose history to list.
            limit: Maximum number of events to return.
            next_token: Keyset pagination token for the next page.
            order: The sort order by occurred_at (ASC or DESC).

        Returns:
            A PaginatedFeedAuditEvents object containing history events,
            next_token, and total.

        Raises:
            ValueError: If limit is less than 1 or if next_token is invalid.
        """
        if limit < 1:
            msg = "limit must be >= 1"
            raise ValueError(msg)

        cursor_ts = None
        cursor_revision = None
        if next_token:
            cursor_ts, cursor_revision = decode_int_cursor(next_token)

        is_asc = order == SortOrder.ASC or order == "asc"
        query = (
            feed_queries.LIST_FEED_AUDIT_EVENTS_ASC_SQL
            if is_asc
            else feed_queries.LIST_FEED_AUDIT_EVENTS_DESC_SQL
        )

        rows_task = self._pool.fetch(
            query,
            feed_id,
            cursor_ts,
            cursor_revision,
            limit + 1,
        )
        total_task = self._pool.fetchval(
            feed_queries.COUNT_FEED_AUDIT_EVENTS_SQL,
            feed_id,
        )

        rows, total = await asyncio.gather(rows_task, total_task)

        rows, new_next_token = get_paginated_results(
            rows,
            limit,
            timestamp_key="occurred_at",
            id_key="feed_revision",
        )

        events = []
        for row in rows:
            before_values_raw = row["before_values"]
            before_values = (
                json.loads(before_values_raw)
                if isinstance(before_values_raw, str)
                else before_values_raw
            )
            after_values_raw = row["after_values"]
            after_values = (
                json.loads(after_values_raw)
                if isinstance(after_values_raw, str)
                else after_values_raw
            )
            events.append(
                FeedAuditEvent(
                    id=row["id"],
                    feed_id=row["feed_id"],
                    action=row["action"],
                    actor_id=row["actor_id"],
                    occurred_at=row["occurred_at"],
                    feed_revision=row["feed_revision"],
                    before_values=before_values,
                    after_values=after_values,
                )
            )
        return PaginatedFeedAuditEvents(events, new_next_token, total)

    async def get_feed_search_options(self) -> dict[str, list[Any]]:
        """Fetch precomputed search filter options for feeds."""
        async with self._pool.acquire() as conn:
            tag_rows = await conn.fetch(
                feed_queries.GET_FEED_SEARCH_OPTIONS_TAGS_SQL
            )
            st_rows = await conn.fetch(
                feed_queries.GET_FEED_SEARCH_OPTIONS_SOURCE_TYPES_SQL
            )
            status_rows = await conn.fetch(
                feed_queries.GET_FEED_SEARCH_OPTIONS_STATUSES_SQL
            )
            return {
                "source_types": [r["source_type"] for r in st_rows],
                "statuses": [r["status"] for r in status_rows],
                "tags": [
                    {"key": r["key"], "value": r["value"]} for r in tag_rows
                ],
            }
