# Phase 02: Transactional Storage Writes - Pattern Map

**Mapped:** 2026-06-19
**Files analyzed:** 10 implementation/documentation targets
**Analogs found:** 10 / 10

## File Classification

| New/Modified File | Role | Data Flow | Closest Analog | Match Quality |
|-------------------|------|-----------|----------------|---------------|
| `backend/pipeline/storage/feed_store.py` | store | CRUD + transactional writes | `backend/pipeline/storage/feed_store.py` current mutation methods | exact role, new transaction behavior |
| `backend/pipeline/storage/feed_queries.py` | query utility | CRUD + row-locking + transform | `backend/pipeline/storage/feed_queries.py` mutation and diagnostic CTEs | exact role |
| `backend/pipeline/storage/tests/connection_util.py` | test utility | asyncpg mock I/O | `backend/pipeline/storage/tests/connection_util.py` | exact role, extend for transactions |
| `backend/pipeline/storage/tests/test_feed_store.py` | test | CRUD + async storage mocks | existing mutation/failure tests in same file | exact role |
| `integration_tests/storage/test_feed_store_integration.py` | test | CRUD + database side effects | existing create/update/delete/reset integration tests | exact role |
| `backend/pipeline/storage/tests/test_feed_audit_contract.py` | test | migration/contract verification | existing Phase 1 audit contract tests | exact role |
| `documentation/feed-audit-events.md` | documentation | domain contract | current Feed Audit Event contract | exact role |
| `terraform/modules/alloydb/sql/ingestion/029_feed_audit_events.sql` | migration | schema constraints + indexes | current audit schema foundation | exact role |
| `backend/services/feeds/service.py` | service | request-response delegation | current thin `FeedService` delegation | exact role |
| `backend/services/feeds/tests/test_api.py` | test | request-response mocks | current FastAPI route tests | role-match |

## Pattern Assignments

### `backend/pipeline/storage/feed_store.py` (store, CRUD + transactional writes)

**Analog:** current `FeedStore` imports, mapping helpers, and mutation methods in `backend/pipeline/storage/feed_store.py`.

**Imports and exception pattern** (lines 3-38):
```python
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
)
from backend.pipeline.storage import quarantine_reason
from backend.pipeline.storage.feed_queries import (
    COUNT_FEEDS_SQL,
    COUNT_HELD_BY_TYPE_SQL,
    CREATE_FEED_SQL,
    DEACTIVATE_FEED_SQL,
    DELETE_FEED_SQL,
    GET_FEED_SQL,
    LIST_FEEDS_ASC_SQL,
    LIST_FEEDS_DESC_SQL,
    RECORD_SOURCE_OBSERVATION_SQL,
    RELEASE_FEED_SQL,
    RELEASE_FEEDS_BATCH_SQL,
    RELEASE_NON_BUDGETED_FAILURE_SQL,
    RENEW_HEARTBEATS_BATCH_DIAGNOSTIC_SQL,
    REPORT_FAILURE_SQL,
    RESET_FEED_SQL,
    UPDATE_FEED_SQL,
    UPDATE_PROGRESS_SQL,
    build_acquire_feeds_batch_sql,
    build_acquire_feeds_recovery_sql,
)
```

Add audit SQL constants to this same import block. Keep `asyncpg.exceptions.UniqueViolationError` and `ForeignKeyViolationError` handling local to the methods that can raise them.

**Row-to-domain mapping pattern** (lines 249-295):
```python
def _row_to_feed(self, row: asyncpg.Record) -> Feed:
    """Convert a database row to a Feed dict with validation."""
    try:
        source_type = SourceType(row["source_type"])
    except ValueError as e:
        msg = f"Unknown source type {row['source_type']!r} for feed {row['id']}"
        raise ValueError(msg) from e
    try:
        status = FeedStatus(row["status"])
    except ValueError as e:
        msg = f"Unknown status {row['status']!r} for feed {row['id']}"
        raise ValueError(msg) from e

    tags = row.get("tags")
    if tags is not None:
        tags = json.loads(tags)

    return Feed(
        id=row["id"],
        name=row["name"],
        source_type=source_type,
        status=status,
        status_reason=status_reason,
        status_reason_updated_at=row["status_reason_updated_at"],
        quarantine_reason=row["quarantine_reason"],
        failure_count=row["failure_count"],
        worker_id=row["worker_id"],
        last_heartbeat=row["last_heartbeat"],
        last_processed_filename=row["last_processed_filename"],
        last_bookmark_time=row["last_bookmark_time"],
        created_at=row["created_at"],
        source_feed_id=row["source_feed_id"],
        tags=tags,
        last_speech_segment_timestamp=row["last_speech_segment_timestamp"],
    )
```

Create a separate audit snapshot helper rather than overloading `_row_to_feed`. The audit helper should use the Phase 1 allowlist, include `status_reason_detail`, and exclude worker/heartbeat/fencing/noise fields.

**Current create mutation pattern** (lines 804-835):
```python
try:
    row = await self._pool.fetchrow(
        CREATE_FEED_SQL,
        name,
        source_type_str,
        source_feed_id,
        json.dumps(tags or []),
    )
except asyncpg.exceptions.UniqueViolationError as e:
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

if row is None:
    msg = f"Failed to create feed {name}"
    raise ValueError(msg)

return self._row_to_feed(row)
```

Refactor this into `async with self._pool.acquire() as conn:` and `async with conn.transaction():`, but keep the same validation, exception conversion, and return shape. `feed.created` should insert after `CREATE_FEED_SQL` returns the row and before transaction commit.

**Current update mutation pattern** (lines 848-874):
```python
try:
    row = await self._pool.fetchrow(
        UPDATE_FEED_SQL,
        feed_id,
        name,
        json.dumps(tags or []),
    )
except asyncpg.exceptions.UniqueViolationError as e:
    logger.warning(
        "Feed update conflicts with existing feed name",
        extra={
            "feed_name": name,
        },
    )
    raise FeedNameAlreadyExistsError(name) from e

if row is None:
    return None

logger.info(
    "Feed updated successfully",
    extra={
        "feed_id": str(feed_id),
        "feed_name": name,
    },
)
return self._row_to_feed(row)
```

Phase 2 should lock/read the before snapshot first, compare normalized stored `name` and `tags`, and suppress `feed.updated` when unchanged while still returning the current feed. Do not turn no-op into `None`.

**Current lifecycle mutation pattern** (lines 942-983):
```python
async def deactivate_feed(self, feed_id: uuid.UUID) -> bool:
    result = await self._pool.execute(DEACTIVATE_FEED_SQL, feed_id)
    return result == "UPDATE 1"

async def delete_feed(self, feed_id: uuid.UUID) -> bool:
    result = await self._pool.execute(DELETE_FEED_SQL, feed_id)
    return result == "DELETE 1"

async def reset_feed(self, feed_id: uuid.UUID) -> Feed | None:
    row = await self._pool.fetchrow(RESET_FEED_SQL, feed_id)
    if row is None:
        return None
    return self._row_to_feed(row)
```

Refactor these to transactional connection calls. Existing-feed paths need a locked before snapshot. `delete_feed` must insert `feed.deleted` before `DELETE_FEED_SQL`, while `feed_properties` still exists.

**Pool configuration constraint** (from `backend/pipeline/storage/connection.py` lines 57-75):
```python
kwargs: dict = {
    "host": host,
    "port": port,
    "user": user,
    "password": password,
    "database": db_name,
    "min_size": min_size,
    "max_size": max_size,
    "statement_cache_size": 0,  # Required for PgBouncer transaction-mode pooling.
    # DB-01: `idle_in_transaction_session_timeout = 30000` (30s) is enforced
    # server-side via AlloyDB cluster `database_flags`, NOT via asyncpg
    # `server_settings` -- see radio-transcription-deployment/terraform/
    # modules/storage/main.tf. We connect through AlloyDB managed pooling
    # (PgBouncer in transaction mode, `pool_mode = "transaction"`) on port
    # 6432; per-connection SET GUCs would be RESET between transactions
}
```

Keep audited transaction blocks short. Do not add session-level settings or long external work inside the transaction.

### `backend/pipeline/storage/feed_queries.py` (query utility, CRUD + row-locking)

**Analog:** current mutation SQL and CTE row-locking SQL in `backend/pipeline/storage/feed_queries.py`.

**Create shape** (lines 395-412):
```sql
WITH new_feed AS (
    INSERT INTO feeds (name, source_type)
    VALUES ($1, $2)
    RETURNING id, name, source_type, status, status_reason,
              status_reason_updated_at, failure_count, worker_id,
              last_heartbeat, last_processed_filename,
              last_bookmark_time, created_at, quarantine_reason
),
new_props AS (
    INSERT INTO feed_properties (feed_id, source_feed_id, source_type, tags)
    SELECT id, $3, source_type, $4 FROM new_feed
    RETURNING source_feed_id, tags
)
SELECT nf.*, np.source_feed_id, np.tags,
       NULL::timestamptz AS last_speech_segment_timestamp
FROM new_feed nf
JOIN new_props np ON TRUE;
```

Add `status_reason_detail` to new current-state projections used by audited mutations. Prefer explicit audit projection SQL over raw row dumps.

**Update/deactivate/delete/reset shape** (lines 485-564):
```sql
DEACTIVATE_FEED_SQL = """\
UPDATE feeds
SET status = 'deactivated'::feed_status
WHERE id = $1
"""

DELETE_FEED_SQL = """\
WITH deleted_audio_segments AS (
    DELETE FROM audio_segments
    WHERE feed_id = $1
),
deleted_transcripts AS (
    DELETE FROM transcripts
    WHERE feed_id = $1
)
DELETE FROM feeds
WHERE id = $1
"""

RESET_FEED_SQL = """\
WITH updated AS (
    UPDATE feeds
    SET status = 'unclaimed'::feed_status,
        failure_count = 0,
        worker_id = NULL,
        unclaimed_since = NOW(),
        quarantine_reason = NULL,
        last_heartbeat = NOW(),
        status_reason_updated_at = CASE
            WHEN status_reason IS NOT NULL THEN NOW()
            ELSE status_reason_updated_at
        END,
        status_reason = NULL
    WHERE id = $1
    RETURNING id, name, source_type, status, failure_count, worker_id,
              status_reason, status_reason_updated_at, last_heartbeat,
              last_processed_filename, last_bookmark_time, created_at,
              quarantine_reason
)
...
UPDATE_FEED_SQL = """\
WITH updated_feed AS (
    UPDATE feeds
    SET name = $2
    WHERE id = $1
    RETURNING id, name, source_type, status, status_reason,
              status_reason_updated_at, failure_count, worker_id,
              last_heartbeat, last_processed_filename,
              last_bookmark_time, created_at, quarantine_reason
),
updated_props AS (
    UPDATE feed_properties
    SET tags = $3
    WHERE feed_id = $1
    RETURNING source_feed_id, tags
)
...
```

Use these names as the mutation entry points. Add audit-specific SQL constants near them, for example locked snapshot select, sequence allocation, and audit insert.

**Row-locking analog** (lines 25-55):
```sql
WITH current_state AS (
    SELECT id, worker_id, status, fencing_token
    FROM feeds
    WHERE id = $1
    FOR UPDATE
),
do_update AS (
    UPDATE feeds
    SET failure_count = 0,
        last_bookmark_time = GREATEST(last_bookmark_time, $4),
        status_reason_updated_at = CASE
            WHEN status_reason IS NOT NULL THEN NOW()
            ELSE status_reason_updated_at
        END,
        status_reason = NULL
    FROM current_state
    WHERE feeds.id = current_state.id
      AND current_state.worker_id = $2
      AND current_state.fencing_token = $3
      AND current_state.status = 'active'::feed_status
    RETURNING feeds.id
)
SELECT ...
```

Use `FOR UPDATE` for exact feed snapshot reads before audited existing-feed mutations. Do not use `MAX(feed_sequence) + 1`.

**ON CONFLICT analog for sequence allocation** (from `backend/pipeline/storage/audio_segment_queries.py` lines 87-93):
```sql
INSERT INTO annotations (audio_segment_id, type, data)
VALUES ($1, $2, $3)
ON CONFLICT (audio_segment_id, type) DO UPDATE
SET audio_segment_id = annotations.audio_segment_id
RETURNING audio_segment_id, type, data, created_at, updated_at
```

Adapt this style to `feed_audit_event_sequences`:
```sql
INSERT INTO feed_audit_event_sequences (feed_id, next_sequence)
VALUES ($1, 2)
ON CONFLICT (feed_id) DO UPDATE
SET next_sequence = feed_audit_event_sequences.next_sequence + 1,
    updated_at = NOW()
RETURNING next_sequence - 1 AS feed_sequence
```

Run sequence allocation inside the same transaction as the state mutation and audit insert.

### `backend/pipeline/storage/tests/connection_util.py` (test utility, asyncpg mock I/O)

**Analog:** current mock pool helper.

**Existing helper** (lines 6-19):
```python
def make_mock_pool(
    *,
    fetchrow_result: dict | None = None,
    execute_result: str = "UPDATE 0",
    fetch_result: list | None = None,
    fetchval_result: int = 0,
) -> mock.AsyncMock:
    """Create a mock asyncpg.Pool with the given return values."""
    pool = mock.AsyncMock()
    pool.fetchrow.return_value = fetchrow_result
    pool.execute.return_value = execute_result
    pool.fetch.return_value = fetch_result or []
    pool.fetchval.return_value = fetchval_result
    return pool
```

Extend this helper rather than creating per-test ad hoc mocks. It needs an optional transaction-capable connection mock where `pool.acquire()` returns an async context manager and the connection exposes `fetchrow`, `fetchval`, `fetch`, `execute`, and `transaction()`.

### `backend/pipeline/storage/tests/test_feed_store.py` (test, CRUD + async storage mocks)

**Analog:** existing storage unit tests.

**Imports and row factory pattern** (lines 1-24 and 62-82):
```python
import datetime
import json
import pathlib
import re
import unittest
import uuid
from typing import cast
from unittest import mock

import asyncpg
import yaml

from backend.pipeline.storage import feed_queries, feed_store, quarantine_reason
from backend.pipeline.storage.feed_store import (
    FeedStatus,
    FeedStatusReason,
    FeedStore,
    HeartbeatResult,
    SourceType,
)
from backend.pipeline.storage.pagination_utils import encode_cursor
from backend.pipeline.storage.tests.connection_util import make_mock_pool

def _full_feed_row(**overrides: object) -> dict[str, object]:
    row: dict[str, object] = {
        "id": _FEED_ID,
        "name": "My Feed",
        "source_type": "bcfy_feeds",
        "status": "unclaimed",
        "status_reason": None,
        ...
    }
    row.update(overrides)
    return row
```

Extend `_full_feed_row` with `status_reason_detail` once storage projections include it. Keep unit tests in `unittest.IsolatedAsyncioTestCase`.

**Current create/update test style** (lines 1744-1775 and 1996-2060):
```python
class TestCreateFeed(unittest.IsolatedAsyncioTestCase):
    async def test_returns_feed_on_success(self) -> None:
        row = _full_feed_row(name="New Feed")
        pool = make_mock_pool(fetchrow_result=row)
        store = FeedStore(pool)

        result = await store.create_feed("New Feed", "bcfy_feeds", "123")

        self.assertEqual(result["id"], _FEED_ID)
        self.assertEqual(result["name"], "New Feed")
        self.assertEqual(result["source_type"], SourceType.BCFY_FEEDS)

class TestDeactivateFeed(unittest.IsolatedAsyncioTestCase):
    async def test_delete_succeeds(self) -> None:
        pool = make_mock_pool(execute_result="UPDATE 1")
        store = FeedStore(pool)

        result = await store.deactivate_feed(_FEED_ID)

        self.assertTrue(result)
        pool.execute.assert_called_once_with(
            feed_queries.DEACTIVATE_FEED_SQL, _FEED_ID
        )
```

Update these tests to pass `actor_id`. Add focused assertions for action, actor, sequence allocation, before/after JSON, and call ordering through the transaction connection mock.

**Parameter assertion pattern** (lines 882-904):
```python
await store.report_feed_failure(
    _FEED_ID,
    _WORKER_ID,
    1,
    reason="ffmpeg_exit_1",
    status_reason=FeedStatusReason.SYSTEM_COLLECTOR_ERROR,
)

args = pool.fetchrow.call_args[0]
self.assertIs(args[0], feed_queries.REPORT_FAILURE_SQL)
self.assertEqual(
    args[1:],
    (
        _FEED_ID,
        _WORKER_ID,
        5,
        1,
        600,
        15,
        "ffmpeg_exit_1",
        "system_collector_error",
    ),
)
```

Copy this pattern for audit insert argument checks. The assertion should prove storage callers pass only causal input (`actor_id`) and do not construct audit rows outside `FeedStore`.

### `integration_tests/storage/test_feed_store_integration.py` (test, CRUD + database side effects)

**Analog:** existing AlloyDB-backed storage integration tests.

**Fixture pattern** (lines 25-29):
```python
@pytest.fixture
async def store(db_pool: asyncpg.Pool) -> FeedStore:
    """Provides a FeedStore instance with a clean database."""
    await db_pool.execute("TRUNCATE feeds CASCADE")
    return FeedStore(db_pool)
```

Use this fixture for rollback and concurrency checks. Keep generated IDs unique because `integration_tests/conftest.py` lines 4-6 require parallel-safe tests.

**Create/update integration pattern** (lines 1605-1685):
```python
feed = await store.create_feed(
    name="New Integration Feed",
    source_type="bcfy_feeds",
    source_feed_id="src_123",
)

row = await db_pool.fetchrow(
    "SELECT f.name, fp.source_feed_id "
    "FROM feeds f "
    "JOIN feed_properties fp ON f.id = fp.feed_id "
    "WHERE f.id = $1",
    feed["id"],
)
assert row is not None
assert row["name"] == "New Integration Feed"
assert row["source_feed_id"] == "src_123"
```

Add audit row queries after each audited mutation. Verify one event per create, meaningful update, deactivate, reset, and delete.

**Hard-delete verification pattern** (lines 1960-2030):
```python
result = await store.delete_feed(feed_id)

assert result is True

row = await db_pool.fetchrow("SELECT 1 FROM feeds WHERE id = $1", feed_id)
assert row is None

row_props = await db_pool.fetchrow(
    "SELECT 1 FROM feed_properties WHERE feed_id = $1", feed_id
)
assert row_props is None
```

Extend this with an audit survival assertion: the `feed.deleted` row must still exist after `feeds` and `feed_properties` are gone, with full `before_values` and `{}` `after_values`.

**Reset diagnostics pattern** (lines 2071-2107):
```python
feed = await store.reset_feed(feed_id)

assert feed is not None
assert feed["status"] == "unclaimed"
assert feed["failure_count"] == 0
assert feed["worker_id"] is None
assert feed["status_reason"] is None
status_reason_updated_at = feed["status_reason_updated_at"]
assert status_reason_updated_at is not None
assert status_reason_updated_at > old_reason_ts
row = await _get_feed_diagnostics(db_pool, feed_id)
assert row["quarantine_reason"] is None
```

Use this pattern to verify reset before/after snapshots include diagnostic fields and cleared values.

### `backend/pipeline/storage/tests/test_feed_audit_contract.py` (test, migration/contract verification)

**Analog:** Phase 1 text-level contract tests.

**Current actor vocabulary test data** (lines 19-27):
```python
_ACTOR_STRINGS = (
    "user:google:",
    "user-email:",
    "service:",
    "system:",
    "job:",
    "gcp-sa:",
    "unknown:unknown",
)
```

Remove `"system:"` here before storage emits real audit rows.

**Constraint test pattern** (lines 116-172):
```python
def test_migration_defines_actor_and_action_constraints() -> None:
    text = _read(
        "terraform/modules/alloydb/sql/ingestion/029_feed_audit_events.sql"
    )
    sql = _sql_without_comments(text)

    for token in (
        "feed_audit_events_action_check",
        "feed_audit_events_actor_id_check",
        *_ACTIONS,
        *_ACTOR_STRINGS,
    ):
        assert token in sql

def test_migration_rejects_malformed_actor_id_suffixes() -> None:
    ...
    rejected_actor_ids = (
        "user:google:",
        "service:",
        "system:",
        "job:",
        "gcp-sa:",
        "user-email:",
    )
```

Update both positive and malformed suffix tests to drop `system:`. Add an explicit negative assertion that the contract and migration no longer accept `system:`.

### `documentation/feed-audit-events.md` (documentation, domain contract)

**Analog:** current domain contract.

**Action and actor vocabulary** (lines 44-88):
```markdown
The Feed Audit Event action vocabulary has exactly these action names:

- `feed.created`: a configured feed was created.
- `feed.updated`: meaningful feed configuration changed.
- `feed.deactivated`: a feed was intentionally deactivated.
- `feed.reset`: a feed was reset for future processing.
- `feed.deleted`: a feed was removed from current-state storage.
- `feed.failure_reported`: a non-terminal abnormal failure was persisted.
- `feed.quarantined`: a failure episode crossed the quarantine threshold.
- `feed.recovered`: successful activity cleared previously persisted abnormal
  state.

Canonical actor forms are:

- `user:google:<sub>` for a human admin identified by the Google subject claim.
- `user-email:<normalized_email>` when a human admin has no available Google
  subject claim.
- `service:<service_name>` for a semantic service actor.
- `system:<component_name>` for a semantic system component.
- `job:<job_name>` for scheduled or maintenance job actors.
- `gcp-sa:<service_account_email>` only when the authenticated workload
  principal is the best available actor and no semantic service, system, or job
  actor is known.
- `unknown:unknown` as the explicit fallback.
```

Remove `system:<component_name>` and revise the `gcp-sa` sentence so it says no semantic service or job actor is known.

**Snapshot allowlist** (lines 92-123):
```markdown
`before_values` and `after_values` are allowlisted JSON snapshots of meaningful
domain values before and after an audited mutation. They are not raw row dumps
and should exclude high-noise operational fields unless a later phase proves
they are needed.

`feed.deleted` uses normal `before_values` as the self-contained deleted-feed
snapshot. It does not get a delete-specific identity blob. It uses the same
maintained allowlist mechanism as other audit events, and audit history must
not depend on the current `feeds` row after hard delete.

The initial deletion snapshot allowlist is:

- `id`
- `name`
- `source_type`
- `status`
- `failure_count`
- `retry_after`
- `status_reason`
- `status_reason_updated_at`
- `status_reason_detail`
- `quarantine_reason`
- `last_bookmark_time`
- `created_at`
- `feed_properties.source_feed_id`
- `feed_properties.tags`
```

Planner should preserve this allowlist for all Phase 2 events, not just delete.

### `terraform/modules/alloydb/sql/ingestion/029_feed_audit_events.sql` (migration, schema constraints)

**Analog:** current audit schema foundation.

**Sequence and audit table foundation** (lines 24-49):
```sql
-- Per-feed sequence counter foundation for later transactional writers.
-- No feed foreign key is used because audit history must survive hard deletes.
CREATE TABLE IF NOT EXISTS feed_audit_event_sequences (
    feed_id       UUID PRIMARY KEY,
    next_sequence BIGINT NOT NULL DEFAULT 1,
    updated_at    TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

-- Durable audit rows for meaningful feed mutations.
CREATE TABLE IF NOT EXISTS feed_audit_events (
    id                   UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    feed_id              UUID NOT NULL,
    feed_name            VARCHAR(255) NOT NULL,
    source_type          TEXT NOT NULL REFERENCES source_types(slug),
    action               TEXT NOT NULL,
    actor_id             TEXT NOT NULL,
    occurred_at          TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    feed_sequence        BIGINT NOT NULL,
    status               feed_status,
    status_reason        TEXT,
    status_reason_detail TEXT,
    before_values        JSONB NOT NULL DEFAULT '{}'::jsonb,
    after_values         JSONB NOT NULL DEFAULT '{}'::jsonb,
    metadata             JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at           TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);
```

Do not add a cascading `feeds` foreign key. Audit rows must survive hard delete.

**Actor constraint branch to remove** (lines 120-128):
```sql
OR (
    actor_id LIKE 'system:%'
    AND substring(
        actor_id FROM char_length('system:') + 1
    ) <> ''
    AND substring(
        actor_id FROM char_length('system:') + 1
    ) !~ '[[:space:]]'
)
```

Remove this branch before real audit inserts ship.

**Ordering and JSON constraints** (lines 188-209):
```sql
ALTER TABLE feed_audit_events
    ADD CONSTRAINT feed_audit_events_feed_sequence_unique
    UNIQUE (feed_id, feed_sequence);

ALTER TABLE feed_audit_events
    ADD CONSTRAINT feed_audit_events_json_object_shape
    CHECK (
        jsonb_typeof(before_values) = 'object'
        AND jsonb_typeof(after_values) = 'object'
        AND jsonb_typeof(metadata) = 'object'
    );
```

Tests should assert audit writers provide JSON objects only and rely on the unique pair to detect sequence bugs.

### `backend/services/feeds/service.py` (service, request-response delegation)

**Analog:** current thin service layer.

**Current delegation pattern** (lines 27-57):
```python
async def create_feed(self, feed_in: FeedCreate) -> Feed:
    """Creates a new feed."""
    store_feed = await self._store.create_feed(
        name=feed_in.name,
        source_type=feed_in.source_type,
        source_feed_id=feed_in.source_feed_id,
        tags=[t.model_dump() for t in feed_in.tags]
        if feed_in.tags
        else None,
    )
    return Feed.model_validate(store_feed)

async def update_feed(
    self, feed_id: str, feed_in: FeedUpdate
) -> Feed | None:
    """Updates an existing feed."""
    try:
        uid = uuid.UUID(feed_id)
    except ValueError:
        return None

    store_feed = await self._store.update_feed(
        feed_id=uid,
        name=feed_in.name,
        tags=[t.model_dump() for t in feed_in.tags]
        if feed_in.tags
        else None,
    )
```

Add a module constant such as `_FEEDS_SERVICE_ACTOR_ID = "service:feeds-service"` and pass it to storage mutation methods. Do not read actor identity from request body in Phase 2.

**Lifecycle delegation/logging pattern** (lines 98-158):
```python
success = await self._store.deactivate_feed(uid)
if success:
    logger.info(
        "Feed deactivated",
        extra={
            "json_fields": {
                "event_type": "feed_deactivated",
                "feed_id": str(uid),
            },
        },
    )
return success
```

Preserve existing logs and return shapes. Only add the storage `actor_id` argument.

### `backend/services/feeds/tests/test_api.py` (test, request-response mocks)

**Analog:** existing FastAPI route tests.

**Route mock setup** (lines 30-42):
```python
class TestFeedsAPI(unittest.TestCase):
    def setUp(self) -> None:
        """Set up a test client and dependency overrides before each test."""
        self.mock_service = AsyncMock()
        app.state.feed_service = self.mock_service

        app.dependency_overrides[verify_oidc_token] = skip_auth
        self.client = TestClient(app)

    def tearDown(self) -> None:
        """Clean up after each test."""
        app.dependency_overrides.clear()
```

Use this file only if route-level expectations need updating. Phase 2 should not add actor fields to request bodies or responses.

**Lifecycle route assertion pattern** (lines 440-492):
```python
response = self.client.post(f"/v1/feeds/{feed_id}/deactivate")

self.assertEqual(response.status_code, status.HTTP_204_NO_CONTENT)
self.mock_service.deactivate_feed.assert_called_once_with(str(feed_id))

response = self.client.delete(f"/v1/feeds/{feed_id}")

self.assertEqual(response.status_code, status.HTTP_204_NO_CONTENT)
self.mock_service.delete_feed.assert_called_once_with(str(feed_id))

response = self.client.post(f"/v1/feeds/{feed_id}/reset")

self.assertEqual(response.status_code, status.HTTP_200_OK)
self.mock_service.reset_feed.assert_called_once_with(str(feed_id))
```

These assertions should remain unchanged if actor fallback stays inside `FeedService`.

## Shared Patterns

### Storage-Owned Audit Boundary

**Source:** `.planning/phases/02-transactional-storage-writes/02-CONTEXT.md` decisions D-01 through D-04.
**Apply to:** `FeedStore` mutation methods and `FeedService`.

- Require `actor_id` on `FeedStore.create_feed`, `update_feed`, `deactivate_feed`, `reset_feed`, and `delete_feed`.
- Keep audit insert helpers private to `backend/pipeline/storage/feed_store.py` or `feed_queries.py`.
- Service/runtime callers pass causal inputs only. They do not build `feed_audit_events` rows.

### Transaction Boundary

**Source:** existing storage pool style in `feed_store.py`; no local explicit `connection.transaction()` analog was found.
**Apply to:** all audited storage mutations.

Use this shape for implementation:
```python
async with self._pool.acquire() as conn:
    async with conn.transaction():
        before_row = await conn.fetchrow(GET_AUDIT_FEED_SNAPSHOT_SQL, feed_id)
        ...
        sequence = await conn.fetchval(ALLOCATE_FEED_AUDIT_SEQUENCE_SQL, feed_id)
        await conn.execute(INSERT_FEED_AUDIT_EVENT_SQL, ...)
```

Everything that changes current state, allocates sequence, and inserts audit history belongs inside the same transaction. Anything that returns `None` or `False` for not-found should avoid inserting audit rows.

### Row Locking

**Source:** `feed_queries.py` CTEs with `FOR UPDATE` at lines 25-55 and `FOR NO KEY UPDATE SKIP LOCKED` at lines 251-257.
**Apply to:** update, deactivate, reset, delete snapshot reads.

Use exact-feed `FOR UPDATE` for audited lifecycle writes. Do not copy leasing `SKIP LOCKED` semantics into admin mutations; admin mutations must serialize per feed rather than skip rows.

### Hard Delete Snapshot

**Source:** `feed_queries.DELETE_FEED_SQL` lines 491-502 and `integration_tests/storage/test_feed_store_integration.py` lines 1960-2030.
**Apply to:** `delete_feed`.

Read and lock the audit snapshot before `DELETE_FEED_SQL`. Insert `feed.deleted` before hard delete removes `feeds` and cascades `feed_properties`. Then assert audit row survival after `feeds`, `feed_properties`, `transcripts`, `audio_segments`, and `annotations` are gone.

### No-Op Update Suppression

**Source:** Phase 2 decisions D-05 through D-07 and current `update_feed` return shape in `feed_store.py` lines 864-874.
**Apply to:** `update_feed`.

Compare normalized stored `name` and `tags` against requested values. If unchanged, return the current feed and do not allocate sequence or insert `feed.updated`.

### Per-Feed Sequence Allocation

**Source:** `029_feed_audit_events.sql` sequence table lines 24-30 and unique constraint lines 188-193.
**Apply to:** all emitted audit rows.

Use `feed_audit_event_sequences` with `INSERT ... ON CONFLICT DO UPDATE ... RETURNING`. Never derive the next value from `MAX(feed_sequence) + 1`.

### Actor Vocabulary Cleanup

**Source:** `documentation/feed-audit-events.md` lines 67-79, `029_feed_audit_events.sql` lines 120-128, and `test_feed_audit_contract.py` lines 19-27 and 137-144.
**Apply to:** docs, migration SQL, contract tests.

Remove `system:` from accepted actor namespaces before audit writes emit rows. Keep `service:`, `job:`, `gcp-sa:`, `user:google:`, `user-email:`, and `unknown:unknown`.

### Phase 2 Service Actor

**Source:** `backend/services/feeds/service.py` delegation lines 27-57 and 98-158.
**Apply to:** all feeds-service mutation calls into `FeedStore`.

Pass `actor_id="service:feeds-service"` from `FeedService` for Phase 2. Trusted human actor propagation remains Phase 3.

### Test Safety

**Source:** `AGENTS.md` and `.agents/instructions.md` local test safety guidance.
**Apply to:** planner verification commands.

Prefer targeted unit tests for docs/storage mock changes. Use integration tests for rollback and concurrent ordering only as a focused, approval-gated or CI lane because they use Docker/Testcontainers.

## No Local Analog Found

| Pattern | Reason | Planner Guidance |
|---------|--------|------------------|
| Explicit asyncpg `connection.transaction()` inside storage methods | Repository search found no backend/storage `connection.transaction()` usage. Existing store methods use pool-level one-shot `fetchrow`, `execute`, `fetch`, and `fetchval`. | Implement the standard asyncpg acquired-connection transaction shape from Phase 2 research while preserving existing `FeedStore` validation, logging, and exception style. |
| Feed audit writer helper | `feed_audit_events` schema exists, but no writer code exists yet. | Keep helper private to storage. Add SQL constants for snapshot select, sequence allocation, and event insert in `feed_queries.py`; orchestrate them from `FeedStore`. |

## Risk Notes For Planner

- Row locking: lock existing-feed snapshots with `FOR UPDATE` before mutation. Do not use lease `SKIP LOCKED` behavior for admin lifecycle writes.
- Hard delete: capture `feed_properties.source_feed_id` and `feed_properties.tags` before `DELETE_FEED_SQL` cascades them away.
- No-op update: compare normalized stored values, not raw request text. Return the feed normally and suppress the audit insert.
- Sequence allocation: allocate through `feed_audit_event_sequences` in the same transaction. `MAX(feed_sequence) + 1` is explicitly out of bounds.
- Actor cleanup: remove `system:` from docs, SQL, and contract tests before inserting rows, or writes can start with a vocabulary the phase explicitly rejected.
- Integration coverage: unit mocks can prove call ordering and payloads, but rollback and concurrent per-feed ordering need database-backed tests.

## Metadata

**Analog search scope:** `.planning/`, `documentation/`, `terraform/modules/alloydb/sql/ingestion/`, `backend/pipeline/storage/`, `backend/services/feeds/`, `integration_tests/storage/`.
**Files scanned:** 24 source/planning/test files via required reads and targeted analog search.
**Pattern extraction date:** 2026-06-19
