# Phase 1: Audit Contract and Emission - Pattern Map

**Mapped:** 2026-06-26
**Files analyzed:** 10 new/modified files
**Analogs found:** 10 / 10

## File Classification

| New/Modified File | Role | Data Flow | Closest Analog | Match Quality |
|-------------------|------|-----------|----------------|---------------|
| `backend/pipeline/storage/feed_audit_sql.py` | utility | transform | `backend/pipeline/storage/feed_audit_sql.py` | exact |
| `backend/pipeline/storage/feed_audit_notifications.py` | utility | event-driven | `backend/pipeline/ingestion/quarantine_telemetry.py` | role-match |
| `backend/pipeline/storage/feed_queries.py` | config | CRUD + transform | `backend/pipeline/storage/feed_queries.py` | exact |
| `backend/pipeline/storage/sync_feed_queries.py` | config | CRUD + transform | `backend/pipeline/storage/sync_feed_queries.py` | exact |
| `backend/pipeline/storage/feed_store.py` | store | CRUD + request-response | `backend/pipeline/storage/feed_store.py` | exact |
| `backend/pipeline/storage/sync_feed_store.py` | store | event-driven + request-response | `backend/pipeline/storage/sync_feed_store.py` | exact |
| `backend/pipeline/storage/tests/test_feed_audit_notifications.py` | test | event-driven | `backend/pipeline/ingestion/tests/test_quarantine_telemetry.py` | role-match |
| `backend/pipeline/storage/tests/test_feed_query_contracts.py` | test | transform | `backend/pipeline/storage/tests/test_feed_query_contracts.py` | exact |
| `backend/pipeline/storage/tests/test_feed_store.py` | test | CRUD + request-response | `backend/pipeline/storage/tests/test_feed_store.py` | exact |
| `backend/pipeline/storage/tests/test_sync_feed_store.py` | test | event-driven + request-response | `backend/pipeline/storage/tests/test_sync_feed_store.py` | exact |

## Pattern Assignments

### `backend/pipeline/storage/feed_audit_sql.py` (utility, transform)

**Analog:** `backend/pipeline/storage/feed_audit_sql.py`

**Imports and module contract pattern** (lines 1-7):
```python
"""Shared SQL fragments for feed audit event writes."""

from __future__ import annotations

# ruff: noqa: S608

AUDITED_FEED_STATE_FIELDS = (
```

**Allowlisted JSONB snapshot pattern** (lines 23-33):
```python
def audit_snapshot_sql(alias: str) -> str:
    """Return a JSONB object expression for the maintained audit allowlist."""
    parts: list[str] = []
    for key, column in AUDITED_FEED_STATE_FIELDS:
        value = f"{alias}.{column}"
        if column in {"source_type", "status"}:
            value = f"{value}::text"
        if column == "tags":
            value = f"COALESCE({value}, '[]'::jsonb)"
        parts.append(f"        {key!r}, {value}")
    return "jsonb_build_object(\n" + ",\n".join(parts) + "\n    )"
```

**Shared audit insert CTE pattern** (lines 109-137):
```python
def insert_feed_audit_event_cte(
    *,
    feed_id_sql: str,
    action_sql: str,
    actor_id_sql: str,
    feed_revision_sql: str,
    before_values_sql: str,
    after_values_sql: str,
    from_sql: str,
    where_sql: str | None = None,
    returning_sql: str = "id",
) -> str:
    """Return write_audit CTE for the canonical feed audit insert."""
    where_clause = f"\n    WHERE {where_sql}" if where_sql else ""
    return f"""write_audit AS (
    INSERT INTO feed_audit_events (
        feed_id, action, actor_id, feed_revision,
        before_values, after_values
    )
    SELECT
        {feed_id_sql},
        {action_sql},
        {actor_id_sql},
        {feed_revision_sql},
{before_values_sql},
{after_values_sql}
    {from_sql}{where_clause}
    RETURNING {returning_sql}
)"""
```

**Apply:** Add the notification JSONB expression/helper here, not in each query.
Keep the SQL-building style explicit, f-string based, and covered by pure SQL
contract tests. The new helper should produce one flat JSONB object and be used
through `returning_sql` or a dedicated return-builder.

### `backend/pipeline/storage/feed_audit_notifications.py` (utility, event-driven)

**Analog:** `backend/pipeline/ingestion/quarantine_telemetry.py`

**Imports and logger pattern** (lines 11-23):
```python
from __future__ import annotations

import logging

from backend.pipeline.common.clients.monitoring_client import MonitoringClient
from backend.pipeline.ingestion.slo_contract import (
    EVENT_TYPE_FEED_QUARANTINED,
    METRIC_TYPE_QUARANTINE_EVENTS,
)

logger = logging.getLogger(__name__)
```

For the storage helper, copy the `from __future__` and `logging.getLogger`
shape, but do not import monitoring, Pub/Sub, webhook, or Cloud Logging clients.
The helper may need `json` and `collections.abc.Mapping` for driver return
normalization.

**Failure-isolated structured emit pattern** (lines 39-73):
```python
async def emit_quarantine_event(
    feed_id: str,
    feed_name: str,
    source_type: str,
    reason: str,
    status_reason: str,
) -> None:
    """Emit a quarantine event signal.  **Never raises.**

    1. Structured ERROR log (always, even when metrics are disabled).
       ``reason`` is interpolated into the message and included as
       ``json_fields.reason`` so on-callers see the failure cause in the
       Logs Explorer summary row without expanding the payload.
    """
    try:
        logger.error(
            "Feed quarantined: %s",
            reason,
            extra={
                "json_fields": {
                    "event_type": EVENT_TYPE_FEED_QUARANTINED,
                    "feed_id": feed_id,
                    "feed_name": feed_name,
                    "reason": reason,
                    "status_reason": status_reason,
                    "source_type": source_type,
                },
            },
        )
    except Exception:  # noqa: S110
        pass
```

**Apply:** Implement a synchronous `emit_feed_audit_notification(...) -> None`
with this same isolation boundary. It should no-op for `None`, normalize
`str` JSON only when necessary, pass a mapping to `extra={"json_fields": ...}`,
and catch local exceptions without re-raising.

**No-client / no-coupling boundary** (lines 75-94):
```python
    if _client is None:
        return

    try:
        # Cardinality: keep labels bounded; reason is unbounded and would fragment time series.
        await _client.write_time_series(
            metric_type=METRIC_TYPE_QUARANTINE_EVENTS,
            labels={
                "feed_id": feed_id,
                "feed_name": feed_name,
                "source_type": source_type,
            },
            value=1,
            resource_labels={"project_id": _client.project_id},
        )
    except Exception:
        try:
            logger.warning("Failed to emit quarantine metric", exc_info=True)
        except Exception:  # noqa: S110
            pass
```

**Apply:** Do not copy the metric client part into storage. The relevant pattern
is that optional side effects are separated from the core log and all failures
are isolated.

### `backend/pipeline/storage/feed_queries.py` (config, CRUD + transform)

**Analog:** `backend/pipeline/storage/feed_queries.py`

**Imports and shared audit SQL constants** (lines 1-22):
```python
"""SQL queries for async VM feed storage operations.

These queries use asyncpg ``$N`` parameters and VM lease fencing fields such as
``worker_id`` and ``fencing_token``. Keep lifecycle SQL explicit here so
quarantine-sensitive transitions remain locally auditable.
"""
# ruff: noqa: S608

from __future__ import annotations

from typing import TYPE_CHECKING

from backend.pipeline.storage import feed_audit_sql

if TYPE_CHECKING:
    from collections.abc import Sequence

    from backend.pipeline.storage.feed_store import SourceType


_AUDIT_BEFORE_SNAPSHOT_SQL = feed_audit_sql.audit_snapshot_sql("before_row")
_AUDIT_AFTER_SNAPSHOT_SQL = feed_audit_sql.audit_snapshot_sql("after_row")
```

**Audited recovery write pattern** (lines 24-76):
```python
UPDATE_PROGRESS_SQL = f"""\
WITH before_row AS (
    SELECT
{feed_audit_sql.audit_source_projection("f")}
    FROM feeds f
    JOIN feed_properties fp ON fp.feed_id = f.id
    WHERE f.id = $2 AND f.worker_id = $3 AND f.fencing_token = $4
    FOR UPDATE
),
updated AS (
    UPDATE feeds
    SET last_processed_filename = $1,
        last_bookmark_time = COALESCE($5, feeds.last_bookmark_time),
        audit_revision = CASE
            WHEN feeds.failure_count <> 0 OR feeds.status_reason IS NOT NULL THEN feeds.audit_revision + 1
            ELSE feeds.audit_revision
        END,
        failure_count = 0,
        status_reason = NULL
    FROM before_row
    WHERE feeds.id = before_row.id
    RETURNING feeds.*, feeds.audit_revision AS feed_revision
),
after_row AS (
    SELECT u.*, fp.source_feed_id, COALESCE(fp.tags, '[]'::jsonb) AS tags
    FROM updated u
    JOIN feed_properties fp ON fp.feed_id = u.id
),
{feed_audit_sql.recovery_audit_action_cte(before_alias="before_row")},
{
    feed_audit_sql.insert_feed_audit_event_cte(
        feed_id_sql="after_row.id",
        action_sql="audit_action.action",
        actor_id_sql="$6::text",
        feed_revision_sql="after_row.feed_revision",
        before_values_sql=_AUDIT_BEFORE_SNAPSHOT_SQL,
        after_values_sql=_AUDIT_AFTER_SNAPSHOT_SQL,
        from_sql=(
            "FROM before_row\n"
            "    JOIN after_row ON after_row.id = before_row.id\n"
            "    CROSS JOIN audit_action"
        ),
        where_sql="audit_action.action IS NOT NULL",
    )
}
SELECT after_row.*
FROM after_row
"""
```

**Apply:** Add `feed_audit_event` to the `write_audit` CTE return and expose it
from the final `SELECT` with `LEFT JOIN write_audit ON TRUE` where a successful
normal result may have no audit row.

**Audited failure suppression pattern** (lines 446-510):
```python
# Failure writes always update current failure state and advance
# feeds.audit_revision. feed_audit_events rows are intentionally suppressed for
# repeated failures with the same status/status_reason, so event revisions can
# have gaps.
REPORT_FAILURE_SQL = f"""\
...
{feed_audit_sql.failure_audit_action_cte()},
{
    feed_audit_sql.insert_feed_audit_event_cte(
        feed_id_sql="after_row.id",
        action_sql="audit_action.action",
        actor_id_sql="$9::text",
        feed_revision_sql="after_row.feed_revision",
        before_values_sql=_AUDIT_BEFORE_SNAPSHOT_SQL,
        after_values_sql=_AUDIT_AFTER_SNAPSHOT_SQL,
        from_sql=(
            "FROM before_row\n"
            "    JOIN after_row ON after_row.id = before_row.id\n"
            "    CROSS JOIN audit_action"
        ),
        where_sql="audit_action.action IS NOT NULL",
    )
}
SELECT after_row.*
FROM after_row
"""
```

**Apply:** Preserve the suppression semantics. Repeated failure SQL may return
a normal status row and `feed_audit_event = NULL`; Python should emit only when
the column is non-null.

**Create/update/admin action pattern** (lines 568-598, 827-899):
```python
CREATE_FEED_SQL = f"""\
WITH new_feed AS (
    INSERT INTO feeds (name, source_type, audit_revision)
    VALUES ($1, $2, 1)
    RETURNING feeds.*, feeds.audit_revision AS feed_revision
),
...
{
    feed_audit_sql.insert_feed_audit_event_cte(
        feed_id_sql="after_row.id",
        action_sql="'feed.created'",
        actor_id_sql="$5::text",
        feed_revision_sql="after_row.feed_revision",
        before_values_sql="        '{}'::jsonb",
        after_values_sql=_AUDIT_AFTER_SNAPSHOT_SQL,
        from_sql="FROM after_row",
    )
}
SELECT after_row.*
FROM after_row;
"""
```

```python
UPDATE_FEED_SQL = f"""\
...
change AS (
    SELECT
        before_row.*,
        (
            before_row.name IS DISTINCT FROM $2
            OR before_row.tags IS DISTINCT FROM $3::jsonb
        ) AS changed
    FROM before_row
),
...
{
    feed_audit_sql.insert_feed_audit_event_cte(
        feed_id_sql="updated_row.id",
        action_sql="'feed.updated'",
        actor_id_sql="$4::text",
        feed_revision_sql="updated_row.feed_revision",
        before_values_sql=_AUDIT_BEFORE_SNAPSHOT_SQL,
        after_values_sql=feed_audit_sql.audit_snapshot_sql("updated_row"),
        from_sql="FROM before_row\n    JOIN updated_row ON updated_row.id = before_row.id",
    )
}
SELECT result_row.*,
...
FROM result_row;
"""
```

**Apply:** Do not add extra database reads. The result row should carry the
nullable `feed_audit_event` column produced in the same SQL statement.

### `backend/pipeline/storage/sync_feed_queries.py` (config, CRUD + transform)

**Analog:** `backend/pipeline/storage/sync_feed_queries.py`

**Sync parameter style and actor CTE pattern** (lines 1-19):
```python
"""Synchronous feed lifecycle SQL queries.

psycopg v3 uses ``%s`` parameters, so these stay separate from the async
``feed_queries`` module which uses asyncpg's ``$1`` parameter style. Echo
lifecycle writes also rely on terminal-state guards instead of VM lease
fencing.
"""
# ruff: noqa: S608

from backend.pipeline.storage import feed_audit_sql

_AUDIT_BEFORE_SNAPSHOT_SQL = feed_audit_sql.audit_snapshot_sql("before_row")
_AUDIT_AFTER_SNAPSHOT_SQL = feed_audit_sql.audit_snapshot_sql("after_row")
_AUDIT_ACTOR_CTE_SQL = """audit_actor AS (
    SELECT %s::text AS actor_id
)"""
```

**Sync heartbeat/recovery audit pattern** (lines 30-86):
```python
HEARTBEAT_SQL = f"""\
WITH before_row AS (
    SELECT
{feed_audit_sql.audit_source_projection("f")}
    FROM feeds f
    JOIN feed_properties fp ON fp.feed_id = f.id
    WHERE f.id = %s
      AND f.status NOT IN ('quarantined'::feed_status, 'deactivated'::feed_status)
    FOR UPDATE
),
...
{feed_audit_sql.recovery_audit_action_cte(before_alias="before_row")},
{_AUDIT_ACTOR_CTE_SQL},
{
    feed_audit_sql.insert_feed_audit_event_cte(
        feed_id_sql="after_row.id",
        action_sql="audit_action.action",
        actor_id_sql="audit_actor.actor_id",
        feed_revision_sql="after_row.feed_revision",
        before_values_sql=_AUDIT_BEFORE_SNAPSHOT_SQL,
        after_values_sql=_AUDIT_AFTER_SNAPSHOT_SQL,
        from_sql=(
            "FROM before_row\n"
            "    JOIN after_row ON after_row.id = before_row.id\n"
            "    CROSS JOIN audit_action\n"
            "    CROSS JOIN audit_actor"
        ),
        where_sql="audit_action.action IS NOT NULL",
    )
}
SELECT after_row.*
FROM after_row
"""
```

**Sync failure audit pattern** (lines 90-157):
```python
# Failure writes always update current failure state and advance
# feeds.audit_revision. feed_audit_events rows are intentionally suppressed for
# repeated failures with the same status/status_reason, so event revisions can
# have gaps.
RECORD_FAILURE_SQL = f"""\
...
{feed_audit_sql.failure_audit_action_cte()},
{_AUDIT_ACTOR_CTE_SQL},
{
    feed_audit_sql.insert_feed_audit_event_cte(
        feed_id_sql="after_row.id",
        action_sql="audit_action.action",
        actor_id_sql="audit_actor.actor_id",
        feed_revision_sql="after_row.feed_revision",
        before_values_sql=_AUDIT_BEFORE_SNAPSHOT_SQL,
        after_values_sql=_AUDIT_AFTER_SNAPSHOT_SQL,
        from_sql=(
            "FROM before_row\n"
            "    JOIN after_row ON after_row.id = before_row.id\n"
            "    CROSS JOIN audit_action\n"
            "    CROSS JOIN audit_actor"
        ),
        where_sql="audit_action.action IS NOT NULL",
    )
}
SELECT after_row.*
FROM after_row
"""
```

**Apply:** Mirror async SQL payload return shape, but keep `%s` placeholders and
the sync-only `audit_actor` CTE.

### `backend/pipeline/storage/feed_store.py` (store, CRUD + request-response)

**Analog:** `backend/pipeline/storage/feed_store.py`

**Imports, logger, and storage dependency pattern** (lines 1-24, 30):
```python
from __future__ import annotations

import asyncio
import enum
import json
import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, TypedDict

import asyncpg
import asyncpg.exceptions

from backend.pipeline.common.exceptions import (
    FeedAlreadyExistsError,
    FeedNameAlreadyExistsError,
    FeedStateConflictError,
)
from backend.pipeline.storage import feed_lifecycle, feed_queries
...
logger = logging.getLogger(__name__)
```

**Actor validation pattern** (lines 192-196):
```python
def _require_actor_id(actor_id: str | None) -> str:
    if actor_id is None:
        msg = "actor_id is required for audited feed lifecycle writes"
        raise ValueError(msg)
    return actor_id
```

**Async audited fetchrow pattern** (lines 366-375):
```python
row = await self._pool.fetchrow(
    feed_queries.UPDATE_PROGRESS_SQL,
    new_gcs_path,
    feed_id,
    worker_id,
    fencing_token,
    last_bookmark_time,
    _require_actor_id(actor_id),
)
return row is not None
```

**Connection-scoped audited fetchrow pattern** (lines 512-548):
```python
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
else:
    logger.info(
        "Feed failure recorded",
        extra={
            "feed_id": str(feed_id),
            "failure_count": row["failure_count"],
            "retry_after": str(row["retry_after"]),
            "reason": reason,
        },
    )
return status
```

**Apply:** Call the shared notification helper after a non-null row is returned
and before returning the method result. Remove or suppress only duplicate
storage-layer failure summary logs if they describe the same audit-shaped event.

**Create/update store semantics** (lines 820-857, 873-905):
```python
try:
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
...
return self._row_to_feed(row)
```

```python
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
...
return self._row_to_feed(row)
```

**Apply:** Preserve exception translation and return semantics. For no-op update
rows, the helper should receive `row["feed_audit_event"]` and no-op when it is
`None`.

**Admin lifecycle diagnostics** (lines 997-1073):
```python
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
return bool(row["deleted"])
```

```python
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
return self._row_to_feed(row)
```

**Apply:** Add notification emission without breaking `blocked_active`,
`deleted`, `current_status`, or `id is None` checks.

### `backend/pipeline/storage/sync_feed_store.py` (store, event-driven + request-response)

**Analog:** `backend/pipeline/storage/sync_feed_store.py`

**Imports, logger, and dependency pattern** (lines 10-21):
```python
from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, LiteralString, TypedDict, cast

from backend.pipeline.storage import (
    feed_lifecycle,
    feed_store,
    sync_feed_queries,
)

logger = logging.getLogger(__name__)
```

**Existing sync row-consuming pattern** (lines 82-98):
```python
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
```

**Current discard-only audited writes to change** (lines 115-119, 152-164):
```python
with self._connect_db() as conn:
    conn.execute(
        cast("LiteralString", sync_feed_queries.HEARTBEAT_SQL),
        (feed_id, _require_actor_id(actor_id)),
    )
```

```python
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
```

**Apply:** For `record_heartbeat`, `record_failure`, and
`record_non_budgeted_failure`, copy the `execute(...).fetchone()` row-consuming
shape from `resolve_echo_feed`, call the shared helper with the nullable
`feed_audit_event`, and preserve public return type `None`. Remove duplicate
failure summary logs only when the audit notification covers them.

### `backend/pipeline/storage/tests/test_feed_audit_notifications.py` (test, event-driven)

**Analogs:** `backend/pipeline/ingestion/tests/test_quarantine_telemetry.py`,
`backend/pipeline/common/tests/test_tracing_utils.py`

**Structured log assertion pattern** (quarantine test lines 18-52):
```python
with self.assertLogs(
    "backend.pipeline.ingestion.quarantine_telemetry",
    level=logging.ERROR,
) as cm:
    await quarantine_telemetry.emit_quarantine_event(
        feed_id="abc-123",
        feed_name="Test Feed",
        source_type="bcfy_feeds",
        reason=reason,
        status_reason="system_unexpected_error",
    )

self.assertEqual(len(cm.records), 1)
record = cast("Any", cm.records[0])
self.assertEqual(record.json_fields["event_type"], "feed_quarantined")
self.assertEqual(record.json_fields["feed_id"], "abc-123")
```

**Never-raises logging failure pattern** (quarantine test lines 115-131):
```python
with mock.patch.object(
    quarantine_telemetry.logger,
    "error",
    side_effect=RuntimeError("logging broken"),
):
    # Must not raise.
    await quarantine_telemetry.emit_quarantine_event(
        feed_id="abc",
        feed_name="F",
        source_type="s",
        reason="r",
        status_reason="system_unexpected_error",
    )
```

**Mock logger call assertion pattern** (tracing test lines 152-178):
```python
@patch("backend.pipeline.common.log_helper.pipeline_metrics_logger")
def test_record_pipeline_stage(self, mock_logger) -> None:
    """Verifies that record_pipeline_stage emits a log with correct json_fields."""
    record_pipeline_stage("segmentation", "start")
    mock_logger.info.assert_called_once_with(
        "Pipeline stage recorded: segmentation -> start",
        extra={
            "json_fields": {
                "event_type": "pipeline_stage",
                "stage": "segmentation",
                "status": "start",
            }
        },
    )
```

**Apply:** Test `None` no-op, mapping payload, string JSON payload, invalid
payload/no-raise, logger failure/no-raise, and exact `event_type` plus
`schema_version` fields.

### `backend/pipeline/storage/tests/test_feed_query_contracts.py` (test, transform)

**Analog:** `backend/pipeline/storage/tests/test_feed_query_contracts.py`

**Pure SQL helper contract pattern** (lines 54-97):
```python
class TestFeedAuditEventSqlContract(unittest.TestCase):
    """Tests for the feed-local audit event insert contract."""

    def test_shared_insert_builder_renders_canonical_columns(self) -> None:
        sql = feed_audit_sql.insert_feed_audit_event_cte(
            feed_id_sql="after_row.id",
            action_sql="audit_action.action",
            actor_id_sql="$1::text",
            feed_revision_sql="after_row.feed_revision",
            before_values_sql=feed_audit_sql.audit_snapshot_sql("before_row"),
            after_values_sql=feed_audit_sql.audit_snapshot_sql("after_row"),
            from_sql="FROM before_row JOIN after_row ON after_row.id = before_row.id",
            where_sql="audit_action.action IS NOT NULL",
        )

        self.assertIn("write_audit AS", sql)
        self.assertIn("INSERT INTO feed_audit_events", sql)
        self.assertIn("feed_id, action, actor_id, feed_revision", sql)
        self.assertIn("before_values, after_values", sql)
        self.assertIn("RETURNING id", sql)
```

**Audited query coverage pattern** (lines 98-118):
```python
def test_audited_mutation_sql_embeds_audit_insert(self) -> None:
    audited_sql = (
        feed_queries.CREATE_FEED_SQL,
        feed_queries.UPDATE_FEED_SQL,
        feed_queries.DEACTIVATE_FEED_SQL,
        feed_queries.DELETE_FEED_SQL,
        feed_queries.RESET_FEED_SQL,
        feed_queries.UPDATE_PROGRESS_SQL,
        feed_queries.RECORD_SOURCE_OBSERVATION_SQL,
        feed_queries.REPORT_FAILURE_SQL,
        feed_queries.RELEASE_NON_BUDGETED_FAILURE_SQL,
        sync_feed_queries.HEARTBEAT_SQL,
        sync_feed_queries.RECORD_FAILURE_SQL,
        sync_feed_queries.RECORD_NON_BUDGETED_FAILURE_SQL,
    )

    for sql in audited_sql:
        stripped = _sql_without_comments(sql)
        self.assertIn("INSERT INTO feed_audit_events", stripped)
        self.assertIn("feed_revision", stripped)
```

**Apply:** Extend this file to assert `feed_audit_event` appears in every
audited final result contract and that the notification payload SQL contains
the flat key set: `event_type`, `schema_version`, `event_id`, `action`,
`occurred_at`, `actor_id`, `feed_id`, `feed_revision`, `before_values`,
`after_values`.

### `backend/pipeline/storage/tests/test_feed_store.py` (test, CRUD + request-response)

**Analog:** `backend/pipeline/storage/tests/test_feed_store.py`

**Imports and mock pool setup** (lines 1-35):
```python
from __future__ import annotations

import datetime
import inspect
import json
import pathlib
import re
import unittest
import uuid
from typing import Any, TypedDict, cast
from unittest import mock

import asyncpg
import yaml

from backend.pipeline.storage import (
    feed_audit_sql,
    feed_queries,
    feed_store,
    status_reason_detail,
)
from backend.pipeline.storage.feed_store import (
    FeedStatus,
    FeedStatusReason,
    FeedStore,
    HeartbeatResult,
    SourceType,
)
from backend.pipeline.storage.tests.connection_util import make_mock_pool
```

**Row fixture pattern** (lines 78-107):
```python
def _full_feed_row(**overrides: object) -> dict[str, object]:
    row: dict[str, object] = {
        "id": _FEED_ID,
        "name": "My Feed",
        "source_type": "bcfy_feeds",
        "status": "unclaimed",
        "status_reason": None,
        "status_reason_updated_at": None,
        "status_reason_detail": None,
        "failure_count": 0,
        "retry_after": None,
        "worker_id": None,
        "last_heartbeat": None,
        "last_processed_filename": None,
        "last_bookmark_time": None,
        "created_at": datetime.datetime(2026, 4, 10, tzinfo=datetime.UTC),
        "feed_revision": 1,
        "source_feed_id": "123",
        "tags": "[]",
        "last_speech_segment_timestamp": None,
    }
    row.update(overrides)
    return row
```

**Storage-boundary guard pattern** (lines 551-590):
```python
class TestFeedAuditStorageBoundary(unittest.TestCase):
    """Hardening checks for the storage-owned audit boundary."""

    def test_audited_mutations_require_explicit_keyword_actor(self) -> None:
        for method_name in (
            "create_feed",
            "update_feed",
            "deactivate_feed",
            "delete_feed",
            "reset_feed",
            "report_feed_failure",
            "release_non_budgeted_failure",
        ):
            with self.subTest(method_name=method_name):
                signature = inspect.signature(getattr(FeedStore, method_name))
                actor = signature.parameters.get("actor_id")

                self.assertIsNotNone(actor)
                assert actor is not None
                self.assertEqual(
                    actor.kind,
                    inspect.Parameter.KEYWORD_ONLY,
                )
                self.assertIs(actor.default, inspect.Parameter.empty)
```

**Async store audit SQL assertion pattern** (lines 2103-2135):
```python
async def test_create_feed_uses_combined_audit_sql(self) -> None:
    """Successful create uses one SQL statement that embeds feed.created."""
    tags = [{"key": "env", "value": "prod"}]
    row = _full_feed_row(
        name="Created Feed",
        tags='[{"key": "env", "value": "prod"}]',
        status_reason_detail="created detail",
    )
    pool = make_mock_pool(transaction=True)
    conn = pool.acquired_connection
    conn.fetchrow.return_value = row
    store = FeedStore(pool)

    result = await store.create_feed(
        "Created Feed",
        "bcfy_feeds",
        "123",
        tags=tags,
        actor_id=_FEEDS_SERVICE_ACTOR_ID,
    )

    self.assertEqual(result["name"], "Created Feed")
    self.assertEqual(
        conn.fetchrow.await_args_list[0].args[0],
        feed_queries.CREATE_FEED_SQL,
    )
    self.assertEqual(len(conn.fetchrow.await_args_list), 1)
    conn.fetchval.assert_not_awaited()
    conn.execute.assert_not_awaited()
    args = conn.fetchrow.await_args.args
    self.assertEqual(args[-1], _FEEDS_SERVICE_ACTOR_ID)
    self.assertIn("INSERT INTO feed_audit_events", args[0])
    self.assertIn("'feed.created'", args[0])
```

**No-op and missing-row preservation pattern** (lines 2182-2222):
```python
async def test_noop_update_returns_current_feed_without_audit(
    self,
) -> None:
    tags = [{"key": "env", "value": "prod"}]
    current = _full_feed_row(
        name="Same Feed",
        tags='[{"key": "env", "value": "prod"}]',
    )
    pool = make_mock_pool(transaction=True)
    conn = pool.acquired_connection
    conn.fetchrow.return_value = current
    store = FeedStore(pool)

    result = await store.update_feed(
        _FEED_ID,
        "Same Feed",
        tags=tags,
        actor_id=_FEEDS_SERVICE_ACTOR_ID,
    )

    assert result is not None
    self.assertEqual(result["name"], "Same Feed")
    conn.fetchrow.assert_awaited_once()
    conn.fetchval.assert_not_awaited()
    conn.execute.assert_not_awaited()
```

**Admin conflict preservation pattern** (lines 2609-2629, 2711-2728):
```python
async def test_active_feed_delete_raises_state_conflict(self) -> None:
    """Active feeds return a conflict marker instead of looking missing."""
    pool = make_mock_pool(transaction=True)
    conn = pool.acquired_connection
    conn.fetchrow.return_value = {
        "id": _FEED_ID,
        "blocked_active": True,
        "current_status": "active",
        "deleted": False,
    }
    store = FeedStore(pool)

    with self.assertRaisesRegex(
        FeedStateConflictError, "cannot be deleted"
    ):
        await store.delete_feed(
            _FEED_ID,
            actor_id=_FEEDS_SERVICE_ACTOR_ID,
        )
    conn.fetchval.assert_not_awaited()
```

**Apply:** Patch `feed_audit_notifications.emit_feed_audit_notification` in
store tests and assert it is called once with the returned `feed_audit_event`
for audit rows, called with `None` or not called for no-op/missing paths
according to the chosen helper-call convention, and that helper exceptions do
not change return semantics.

### `backend/pipeline/storage/tests/test_sync_feed_store.py` (test, event-driven + request-response)

**Analog:** `backend/pipeline/storage/tests/test_sync_feed_store.py`

**Mock sync connection pattern** (lines 20-42):
```python
def _make_store(
    mock_conn: MagicMock,
    *,
    failure_threshold: int = 5,
    base_backoff_sec: int = 15,
    max_backoff_sec: int = 600,
) -> SyncFeedStore:
    """Build a SyncFeedStore backed by a mock connection."""
    connect_db = MagicMock(return_value=mock_conn)
    return SyncFeedStore(
        connect_db,
        failure_threshold=failure_threshold,
        base_backoff_sec=base_backoff_sec,
        max_backoff_sec=max_backoff_sec,
    )


def _make_mock_conn() -> MagicMock:
    conn = MagicMock()
    conn.__enter__ = MagicMock(return_value=conn)
    conn.__exit__ = MagicMock(return_value=False)
    conn.execute.return_value.fetchone.return_value = None
    return conn
```

**Sync lifecycle assertion pattern** (lines 95-119):
```python
class TestRecordHeartbeat:
    def test_executes_heartbeat_sql(self) -> None:
        conn = _make_mock_conn()
        store = _make_store(conn)
        feed_id = uuid.uuid4()

        store.record_heartbeat(feed_id, actor_id=_ECHO_ACTOR_ID)

        conn.execute.assert_called_once()
        sql, params = conn.execute.call_args[0]
        assert (
            "status NOT IN ('quarantined'::feed_status, "
            "'deactivated'::feed_status)"
        ) in sql
        assert params == (feed_id, _ECHO_ACTOR_ID)
```

**Sync failure log pattern to revise** (lines 230-249):
```python
with patch(
    "backend.pipeline.storage.sync_feed_store.logger"
) as mock_logger:
    store.record_failure(
        feed_id,
        actor_id=_ECHO_ACTOR_ID,
        reason="echo_recording_download_failed",
        status_reason=FeedStatusReason.SYSTEM_COLLECTOR_ERROR,
    )

mock_logger.warning.assert_called_once()
extra = mock_logger.warning.call_args[1]["extra"]
assert extra["feed_id"] == str(feed_id)
assert extra["status_reason"] == "system_collector_error"
assert extra["reason"] == "echo_recording_download_failed"
```

**Sync audit SQL coverage pattern** (lines 328-346):
```python
class TestSyncAuditSql:
    def test_runtime_audit_insert_is_embedded_in_lifecycle_sql(self) -> None:
        for sql in (
            sync_feed_queries.HEARTBEAT_SQL,
            sync_feed_queries.RECORD_FAILURE_SQL,
            sync_feed_queries.RECORD_NON_BUDGETED_FAILURE_SQL,
        ):
            assert "INSERT INTO feed_audit_events" in sql
            assert "feed_revision" in sql
            assert "before_values" in sql
            assert "after_values" in sql

    def test_runtime_audit_actions_are_selected_in_sql(self) -> None:
        assert "THEN 'feed.recovered'" in sync_feed_queries.HEARTBEAT_SQL
        assert (
            "THEN 'feed.failure_reported'"
            in sync_feed_queries.RECORD_FAILURE_SQL
        )
        assert "THEN 'feed.quarantined'" in sync_feed_queries.RECORD_FAILURE_SQL
```

**Apply:** Update sync tests to expect `conn.execute(...).fetchone()`, assert
shared helper calls, and remove/replace duplicate `logger.warning` or
`logger.info` assertions when those logs are intentionally superseded by audit
notification emission.

## Shared Patterns

### Structured Logging

**Source:** `backend/pipeline/common/log_helper.py`
**Apply to:** `feed_audit_notifications.py`, notification helper tests

**`json_fields` logging shape** (lines 20-35):
```python
def record_pipeline_stage(stage: str, status: str = "start") -> None:
    """Records that an audio chunk has reached a stage/status in the pipeline.

    Emits a structured log to power Log-Based Metrics. This avoids data loss
    from scale-to-zero container destruction in Cloud Run / GCF.
    """
    pipeline_metrics_logger.info(
        f"Pipeline stage recorded: {stage} -> {status}",
        extra={
            "json_fields": {
                "event_type": "pipeline_stage",
                "stage": stage,
                "status": status,
            }
        },
    )
```

### Trace-Aware Formatter Compatibility

**Source:** `backend/pipeline/common/log_helper.py`
**Apply to:** Any helper emitting structured records

**Custom extra preservation pattern** (lines 185-200):
```python
# Extract custom extra attributes
log_record.update(
    {
        key: value
        for key, value in record.__dict__.items()
        if key not in RESERVED_ATTRS and not key.startswith("_")
    }
)

# Add trace info from OpenTelemetry
trace_attrs = get_trace_attributes()
if trace_attrs.get("trace"):
    log_record["logging.googleapis.com/trace"] = trace_attrs["trace"]
    log_record["logging.googleapis.com/spanId"] = trace_attrs["spanId"]

return json.dumps(log_record)
```

### Audit Boundary Ownership

**Source:** `backend/pipeline/storage/tests/test_feed_store.py`
**Apply to:** Storage tests and implementation boundaries

**Service layer must not build audit rows** (lines 581-590):
```python
def test_feeds_service_does_not_build_audit_rows_directly(self) -> None:
    for path in (
        pathlib.Path("backend/services/feeds/main.py"),
        pathlib.Path("backend/services/feeds/service.py"),
    ):
        with self.subTest(path=str(path)):
            text = path.read_text()

            self.assertNotIn("feed_audit_events", text)
            self.assertNotIn("_insert_feed_audit_event", text)
```

### Store Mocking

**Source:** `backend/pipeline/storage/tests/connection_util.py`
**Apply to:** `test_feed_store.py`

**Async pool and acquired connection pattern** (lines 14-45):
```python
def make_mock_pool(
    *,
    fetchrow_result: dict | None = None,
    execute_result: str = "UPDATE 0",
    fetch_result: list | None = None,
    fetchval_result: int = 0,
    transaction: bool = False,
) -> mock.AsyncMock:
    """Create a mock asyncpg.Pool with the given return values."""
    pool = mock.AsyncMock()
    pool.fetchrow.return_value = fetchrow_result
    pool.execute.return_value = execute_result
    pool.fetch.return_value = fetch_result or []
    pool.fetchval.return_value = fetchval_result

    if transaction:
        connection = mock.AsyncMock()
        connection.fetchrow.return_value = fetchrow_result
        connection.execute.return_value = execute_result
        connection.fetch.return_value = fetch_result or []
        connection.fetchval.return_value = fetchval_result
...
```

## No Analog Found

All planned files have close analogs in the codebase. Use the research guidance
only to refine exact helper names and SQL return expression signatures.

## Metadata

**Analog search scope:** `backend/pipeline/storage`, `backend/pipeline/common`,
`backend/pipeline/ingestion`
**Files scanned:** 179 files by `rg --files`, 167 Python files by `find`
**Project instructions loaded:** `AGENTS.md`, `.agents/instructions.md`,
`.github/instructions/PYTHON_STYLE.instructions.md`
**Project-local skills:** none found under `.codex/skills` or `.agents/skills`
**Pattern extraction date:** 2026-06-26
