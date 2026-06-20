# Phase 05: Retention and Verification Hardening - Pattern Map

**Mapped:** 2026-06-20
**Files analyzed:** 12
**Analogs found:** 12 / 12

## File Classification

| New/Modified File | Role | Data Flow | Closest Analog | Match Quality |
|-------------------|------|-----------|----------------|---------------|
| `terraform/modules/alloydb/sql/ingestion/031_feed_audit_event_retention.sql` | migration | batch | `terraform/modules/alloydb/sql/ingestion/019_feeds_pg_cron_jobs.sql` + `029_feed_audit_events.sql` | role-match |
| `terraform/modules/alloydb/sql/ingestion/032_feed_audit_events_pg_cron_retention.sql` | migration/config | batch/scheduled | `terraform/modules/alloydb/sql/ingestion/019_feeds_pg_cron_jobs.sql` | exact |
| `backend/pipeline/storage/tests/test_feed_audit_contract.py` | test | static-file contract | `backend/pipeline/storage/tests/test_feed_audit_contract.py` | exact |
| `integration_tests/storage/test_feed_store_integration.py` | test | CRUD/batch | `integration_tests/storage/test_feed_store_integration.py` | exact |
| `documentation/feed-audit-events.md` | documentation | contract | `documentation/feed-audit-events.md` | exact |
| `backend/pipeline/storage/tests/test_feed_store.py` | test | CRUD/event-driven | `backend/pipeline/storage/tests/test_feed_store.py` | exact |
| `backend/pipeline/storage/tests/test_sync_feed_store.py` | test | event-driven | `backend/pipeline/storage/tests/test_sync_feed_store.py` | exact |
| `backend/pipeline/storage/tests/test_feed_query_contracts.py` | test | static-file contract | `backend/pipeline/storage/tests/test_feed_query_contracts.py` | exact |
| `backend/pipeline/storage/tests/test_feed_lifecycle.py` | test | transform | `backend/pipeline/storage/tests/test_feed_lifecycle.py` | exact |
| `backend/services/feeds/tests/test_api.py` | test | request-response | `backend/services/feeds/tests/test_api.py` | exact |
| `backend/services/feeds/tests/test_service.py` | test | request-response | `backend/services/feeds/tests/test_service.py` | exact |
| `frontend/api/src/feeds/feedsController.test.ts` | test | request-response | `frontend/api/src/feeds/feedsController.test.ts` | exact |

## Pattern Assignments

### `terraform/modules/alloydb/sql/ingestion/031_feed_audit_event_retention.sql` (migration, batch)

**Analog:** `terraform/modules/alloydb/sql/ingestion/019_feeds_pg_cron_jobs.sql`, `029_feed_audit_events.sql`, `030_feed_audit_events_actor_constraint.sql`

**Schema target pattern** (`029_feed_audit_events.sql` lines 24-49, 206-213):

```sql
-- Per-feed sequence counter foundation for later transactional writers.
-- No feed foreign key is used because audit history must survive hard deletes.
CREATE TABLE IF NOT EXISTS feed_audit_event_sequences (
    feed_id       UUID PRIMARY KEY,
    next_sequence BIGINT NOT NULL DEFAULT 1,
    updated_at    TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS feed_audit_events (
    id                   UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    feed_id              UUID NOT NULL,
    occurred_at          TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    feed_sequence        BIGINT NOT NULL,
    created_at           TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_feed_audit_events_occurred_at
    ON feed_audit_events (occurred_at);
```

**Bounded batch pattern** (`019_feeds_pg_cron_jobs.sql` lines 49-67, 83-84):

```sql
-- AS MATERIALIZED is load-bearing: from PG 12 onward, unmarked CTEs
-- are inlined into the outer query when the planner judges it
-- profitable.
WITH abandoned AS MATERIALIZED (
    SELECT id, last_heartbeat FROM public.feeds
     WHERE status = 'active'::feed_status
       AND last_heartbeat < NOW() - INTERVAL '60 seconds'
     LIMIT 500
     FOR UPDATE SKIP LOCKED
)
UPDATE public.feeds
   SET status = 'unclaimed'::feed_status
  FROM abandoned
 WHERE feeds.id = abandoned.id;
```

Copy the same `AS MATERIALIZED` + `LIMIT` + `FOR UPDATE SKIP LOCKED` shape for expired audit rows, but use `DELETE FROM public.feed_audit_events events USING expired` and the locked cutoff `occurred_at < NOW() - INTERVAL '18 months'`.

**Sequence bookkeeping pattern** (`030_feed_audit_events_actor_constraint.sql` lines 75-84):

```sql
INSERT INTO feed_audit_event_sequences (feed_id, next_sequence)
SELECT feed_id, MAX(feed_sequence) + 1
FROM feed_audit_events
GROUP BY feed_id
ON CONFLICT (feed_id) DO UPDATE
SET next_sequence = GREATEST(
        feed_audit_event_sequences.next_sequence,
        EXCLUDED.next_sequence
    ),
    updated_at = NOW();
```

For Phase 5, do not recompute or renumber sequences. Use this only as the table/update idiom. Prune sequence rows with `NOT EXISTS` against both `feeds` and retained `feed_audit_events`.

---

### `terraform/modules/alloydb/sql/ingestion/032_feed_audit_events_pg_cron_retention.sql` (migration/config, batch/scheduled)

**Analog:** `terraform/modules/alloydb/sql/ingestion/019_feeds_pg_cron_jobs.sql`

**Filename and extension convention** (lines 10-20):

```sql
-- File-naming convention (load-bearing): any migration whose application
-- requires pg_cron must have "pg_cron" in its filename.
CREATE EXTENSION IF NOT EXISTS pg_cron;
```

**Named schedule pattern** (lines 101-105):

```sql
SELECT cron.schedule(
    'feeds-vac',
    '* * * * *',
    'VACUUM public.feeds'
);
```

Use a named job such as `feed-audit-events-retention`, a daily cron expression, and a command that calls the extension-free retention helper. Keep actual retention SQL out of the skipped `*pg_cron*` file when possible.

---

### `backend/pipeline/storage/tests/test_feed_audit_contract.py` (test, static-file contract)

**Analog:** `backend/pipeline/storage/tests/test_feed_audit_contract.py`

**Imports and helpers pattern** (lines 1-38):

```python
from __future__ import annotations

import pathlib
import re

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[4]

def _read(path: str) -> str:
    return (_REPO_ROOT / path).read_text(encoding="utf-8")

def _sql_without_comments(text: str) -> str:
    return re.sub(r"--.*$", "", text, flags=re.MULTILINE)

def _normalized_sql(text: str) -> str:
    return " ".join(_sql_without_comments(text).split())
```

**Contract token pattern** (lines 41-60):

```python
def test_documentation_defines_feed_audit_event_contract() -> None:
    text = _read("documentation/feed-audit-events.md")

    for token in (
        "feed_audit_events",
        "occurred_at",
        "feed_sequence",
        "18 months",
    ):
        assert token in text

    assert "system:" not in text
```

**Migration invariant pattern** (lines 76-115):

```python
def test_migration_defines_delete_safe_audit_schema() -> None:
    text = _read(
        "terraform/modules/alloydb/sql/ingestion/029_feed_audit_events.sql"
    )
    sql = _sql_without_comments(text)
    normalized = _normalized_sql(text)

    for token in (
        "CREATE TABLE IF NOT EXISTS feed_audit_events",
        "occurred_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()",
    ):
        assert token in normalized

    for pattern in (
        r"ON\s+DELETE\s+CASCADE",
        r"pg_cron",
    ):
        assert re.search(pattern, sql, flags=re.IGNORECASE) is None
```

Add retention tests here for:

- scheduler filename includes `pg_cron`
- scheduler uses `cron.schedule`
- retention cutoff uses `occurred_at`, not `created_at`
- retention interval is exactly `18 months`
- delete is bounded by `LIMIT`
- no `INSERT INTO feed_audit_events`
- no sequence renumbering or `UPDATE feed_audit_events SET feed_sequence`

---

### `integration_tests/storage/test_feed_store_integration.py` (test, CRUD/batch)

**Analog:** `integration_tests/storage/test_feed_store_integration.py`

**Fixture/import pattern** (lines 1-30):

```python
from __future__ import annotations

import asyncio
import datetime
import json
import uuid

import asyncpg
import pytest

from backend.pipeline.storage.feed_store import FeedStore

@pytest.fixture
async def store(db_pool: asyncpg.Pool) -> FeedStore:
    """Provides a FeedStore instance with a clean database."""
    await db_pool.execute("TRUNCATE feeds CASCADE")
    return FeedStore(db_pool)
```

**Audit helper pattern** (lines 109-133):

```python
async def _fetch_audit_events(
    pool: asyncpg.Pool,
    feed_id: uuid.UUID,
) -> list[asyncpg.Record]:
    """Return audit rows for one feed in deterministic timeline order."""
    return await pool.fetch(
        "SELECT id, action, actor_id, feed_sequence, occurred_at,"
        " status::text AS status, before_values, after_values"
        " FROM feed_audit_events"
        " WHERE feed_id = $1"
        " ORDER BY feed_sequence, occurred_at, id",
        feed_id,
    )

async def _get_audit_sequence_next(
    pool: asyncpg.Pool,
    feed_id: uuid.UUID,
) -> int | None:
    return await pool.fetchval(
        "SELECT next_sequence FROM feed_audit_event_sequences"
        " WHERE feed_id = $1",
        feed_id,
    )
```

**Rollback pattern** (lines 1796-1832):

```python
async def test_create_feed_audit_failure_rolls_back_feed_and_sequence(
    db_pool: asyncpg.Pool,
    store: FeedStore,
) -> None:
    sequence_count_before = await db_pool.fetchval(
        "SELECT COUNT(*) FROM feed_audit_event_sequences"
    )

    with pytest.raises(asyncpg.CheckViolationError):
        await store.create_feed(
            name=name,
            source_type="bcfy_feeds",
            source_feed_id=source_feed_id,
            actor_id=_INVALID_ACTOR_ID,
        )

    audit_rows = await db_pool.fetch(
        "SELECT 1 FROM feed_audit_events WHERE feed_name = $1",
        name,
    )
    sequence_count_after = await db_pool.fetchval(
        "SELECT COUNT(*) FROM feed_audit_event_sequences"
    )

    assert feed_row is None
    assert audit_rows == []
    assert sequence_count_after == sequence_count_before
```

**Concurrent ordering pattern** (lines 1902-1960):

```python
results = await asyncio.gather(
    *[
        store.update_feed(
            feed_id=feed["id"],
            name=name,
            tags=tags,
            actor_id=_TEST_ACTOR_ID,
        )
        for name, tags in update_inputs
    ],
    return_exceptions=True,
)

audit_rows = await _fetch_audit_events(db_pool, feed["id"])
feed_sequences = [row["feed_sequence"] for row in audit_rows]

assert len(feed_sequences) == len(set(feed_sequences))
assert feed_sequences == list(range(1, len(feed_sequences) + 1))
assert await _get_audit_sequence_next(db_pool, feed["id"]) == 4
```

**Delete-survival pattern** (lines 2328-2344):

```python
# Verify delete audit history survives hard delete.
audit_rows = await _fetch_audit_events(db_pool, feed_id)
assert len(audit_rows) == 1
audit_row = audit_rows[0]
before_values = _decode_json_object(audit_row["before_values"])
after_values = _decode_json_object(audit_row["after_values"])
assert audit_row["action"] == "feed.deleted"
assert audit_row["feed_sequence"] == 1
assert before_values["id"] == str(feed_id)
assert after_values == {}
assert "worker_id" not in before_values
assert "last_heartbeat" not in before_values
```

Add retention integration tests near the audited transaction/delete sections if the retention helper is extension-free. Use direct `INSERT INTO feed_audit_events` rows with one expired and one retained event, call the procedure, assert the retained row keeps its original `feed_sequence`, and assert live/deleted sequence rows follow D-09 through D-11.

---

### `documentation/feed-audit-events.md` (documentation, contract)

**Analog:** `documentation/feed-audit-events.md`

**Domain contract pattern** (lines 5-17):

```markdown
A Feed Audit Event is durable backend history for a meaningful feed mutation.
It answers what happened to a feed, when it happened, what changed, and which
causal actor produced the change.

Feed Audit Events are not full event sourcing. The current `feeds` row remains
the authoritative current feed state, and `feed_audit_events` is the append-only
audit history that storage writers, runtime paths, future Watch Duty delivery,
and admin timelines derive from.
```

**Retention section to update** (lines 184-190):

```markdown
## Retention

Feed Audit Events are retained for 18 months. Phase 5 owns retention
enforcement, including any scheduled deletion job or retention verification.

Phase 1 only defines the retention target and the fields future enforcement can
use. It does not add retention enforcement jobs.
```

After Phase 5, rewrite this section so enforcement is current: DB-owned `pg_cron`, `occurred_at < NOW() - INTERVAL '18 months'`, bounded daily batch, expected `feed_sequence` gaps, and safe pruning rules for `feed_audit_event_sequences`.

**Phase boundary pattern** (lines 194-207):

```markdown
The implemented v1 runtime boundary includes storage-owned audit events for
async collector and Echo/sync ingestion failure, quarantine, and recovery
outcomes.
Storage remains the only code boundary that inserts
`feed_audit_events`; runtime and Echo handlers pass causal actor and prior-state
inputs into storage instead of constructing audit rows directly.
```

Remove `retention enforcement jobs` from the "does not add" list once the scheduler migration exists.

---

### `backend/pipeline/storage/tests/test_feed_store.py` (test, CRUD/event-driven)

**Analog:** `backend/pipeline/storage/tests/test_feed_store.py`

**Imports/constants/helpers pattern** (lines 1-39, 95-113):

```python
from __future__ import annotations

import datetime
import json
import unittest
import uuid

from backend.pipeline.storage import feed_queries, feed_store, quarantine_reason
from backend.pipeline.storage.tests.connection_util import make_mock_pool

_FEEDS_SERVICE_ACTOR_ID = "service:feeds-service"
_COLLECTOR_RUNTIME_ACTOR_ID = "service:collector-runtime"

def _audit_snapshot_row(**overrides: object) -> dict[str, object]:
    row = _full_feed_row(**overrides)
    row["feed_properties.source_feed_id"] = row["source_feed_id"]
    row["feed_properties.tags"] = row["tags"]
    return row
```

**Admin CRUD event pattern** (lines 2606-2668, 2674-2741):

```python
async def test_writes_feed_created_audit_event(self) -> None:
    conn.fetchrow.side_effect = [row, snapshot]
    conn.fetchval.return_value = 1

    result = await store.create_feed(
        "Created Feed",
        "bcfy_feeds",
        "123",
        tags=tags,
        actor_id=_FEEDS_SERVICE_ACTOR_ID,
    )

    conn.fetchval.assert_awaited_once_with(
        feed_queries.ALLOCATE_FEED_AUDIT_SEQUENCE_SQL,
        _FEED_ID,
    )
    args = conn.execute.await_args.args
    self.assertEqual(args[0], feed_queries.INSERT_FEED_AUDIT_EVENT_SQL)
    self.assertEqual(
        args[1:10],
        (
            _FEED_ID,
            "Created Feed",
            "bcfy_feeds",
            "feed.created",
            _FEEDS_SERVICE_ACTOR_ID,
            1,
            "unclaimed",
            None,
            "created detail",
        ),
    )
    self.assertNotIn("worker_id", after_values)
    self.assertNotIn("last_heartbeat", after_values)
```

```python
async def test_meaningful_update_writes_feed_updated_audit_event(self) -> None:
    conn.fetchrow.side_effect = [before, updated_row, after]
    conn.fetchval.return_value = 2

    result = await store.update_feed(
        _FEED_ID,
        "Updated Feed",
        tags=tags,
        actor_id=_FEEDS_SERVICE_ACTOR_ID,
    )

    conn.fetchval.assert_awaited_once_with(
        feed_queries.ALLOCATE_FEED_AUDIT_SEQUENCE_SQL,
        _FEED_ID,
    )
    args = conn.execute.await_args.args
    self.assertEqual(args[0], feed_queries.INSERT_FEED_AUDIT_EVENT_SQL)
    self.assertEqual(
        args[1:10],
        (
            _FEED_ID,
            "Updated Feed",
            "bcfy_feeds",
            "feed.updated",
            _FEEDS_SERVICE_ACTOR_ID,
            2,
            "unclaimed",
            None,
            "after detail",
        ),
    )
```

**Runtime event/no-noise pattern** (lines 1466-1538, 1581-1613, 1762-1783):

```python
async def test_first_abnormal_failure_emits_failure_reported(self) -> None:
    conn.fetchrow.side_effect = [
        _audit_snapshot_row(status="active"),
        _failure_update_row(),
        _audit_snapshot_row(status="failing", failure_count=1),
    ]
    conn.fetchval.return_value = 7

    result = await store.report_feed_failure(
        _FEED_ID,
        _WORKER_ID,
        1,
        **_runtime_prior_kwargs(),
        reason="collector failed",
        status_reason=FeedStatusReason.SYSTEM_COLLECTOR_ERROR,
    )

    audit_args = _audit_insert_calls(conn)[0]
    self.assertEqual(audit_args[4], "feed.failure_reported")
```

```python
async def test_same_combo_retry_emits_no_event(self) -> None:
    result = await store.report_feed_failure(
        _FEED_ID,
        _WORKER_ID,
        1,
        **_runtime_prior_kwargs(
            previous_status=FeedStatus.FAILING,
            previous_failure_count=1,
            previous_status_reason=(
                FeedStatusReason.SYSTEM_COLLECTOR_ERROR
            ),
        ),
        reason="new detail only",
        status_reason=FeedStatusReason.SYSTEM_COLLECTOR_ERROR,
    )

    self.assertEqual(result, "failing")
    conn.fetchval.assert_not_awaited()
    self.assertEqual(_audit_insert_calls(conn), [])
```

```python
async def test_threshold_crossing_emits_only_quarantined(self) -> None:
    await store.report_feed_failure(
        _FEED_ID,
        _WORKER_ID,
        1,
        **_runtime_prior_kwargs(
            previous_status=FeedStatus.FAILING,
            previous_failure_count=4,
            previous_status_reason=FeedStatusReason.SOURCE_UNREACHABLE,
        ),
        status_reason=FeedStatusReason.SOURCE_UNREACHABLE,
    )

    audit_actions = [args[4] for args in _audit_insert_calls(conn)]
    self.assertEqual(audit_actions, ["feed.quarantined"])
```

```python
async def test_clean_progress_emits_no_event(self) -> None:
    result = await store.update_feed_progress(
        _FEED_ID,
        _WORKER_ID,
        "gs://bucket/path/file.ogg",
        1,
        None,
        **_runtime_prior_kwargs(previous_status=FeedStatus.ACTIVE),
    )

    self.assertTrue(result)
    conn.fetchval.assert_not_awaited()
    self.assertEqual(_audit_insert_calls(conn), [])
```

Phase 5 should reference these tests in the v1 verification gate. If adding explicit event coverage aggregation, copy the existing `make_mock_pool(transaction=True)`, `conn.fetchrow.side_effect`, `conn.fetchval`, and `_audit_insert_calls` style.

---

### `backend/pipeline/storage/tests/test_sync_feed_store.py` (test, event-driven)

**Analog:** `backend/pipeline/storage/tests/test_sync_feed_store.py`

**Sync transaction/helper pattern** (lines 55-105):

```python
def _make_transactional_conn(*cursors: MagicMock) -> MagicMock:
    conn = _make_mock_conn()
    conn.execute.side_effect = list(cursors)
    tx = MagicMock()
    tx.__enter__ = MagicMock(return_value=tx)
    tx.__exit__ = MagicMock(return_value=False)
    conn.transaction.return_value = tx
    return conn

def _audit_insert_calls(conn: MagicMock) -> list[tuple[object, ...]]:
    return [
        call.args
        for call in conn.execute.call_args_list
        if call.args[0] == sync_feed_queries.INSERT_FEED_AUDIT_EVENT_SQL
    ]
```

**Echo no-noise pattern** (lines 591-605, 607-639):

```python
def test_clean_heartbeat_emits_no_event(self) -> None:
    conn = _make_transactional_conn(
        _make_cursor(row=_audit_snapshot_row(id=feed_id)),
        _make_cursor(rowcount=1),
        _make_cursor(row=_audit_snapshot_row(id=feed_id)),
    )

    store.record_heartbeat(
        feed_id,
        **_sync_prior_kwargs(previous_status=FeedStatus.ACTIVE),
    )

    assert _audit_insert_calls(conn) == []
```

```python
def test_detail_only_heartbeat_clear_from_normal_emits_no_event(self) -> None:
    store.record_heartbeat(
        feed_id,
        **_sync_prior_kwargs(previous_status=FeedStatus.ACTIVE),
    )

    assert _audit_insert_calls(conn) == []
```

---

### `backend/pipeline/storage/tests/test_feed_query_contracts.py` (test, static-file contract)

**Analog:** `backend/pipeline/storage/tests/test_feed_query_contracts.py`

**No lease/status-reason coupling pattern** (lines 53-65):

```python
def test_claim_release_heartbeat_count_and_deactivate_do_not_reference_status_reason(
    self,
) -> None:
    lifecycle_sql = [
        feed_queries.RENEW_HEARTBEATS_BATCH_DIAGNOSTIC_SQL,
        feed_queries.RELEASE_FEED_SQL,
        feed_queries.RELEASE_FEEDS_BATCH_SQL,
        feed_queries.COUNT_HELD_BY_TYPE_SQL,
        feed_queries.DEACTIVATE_FEED_SQL,
    ]

    for sql in lifecycle_sql:
        self.assertNotIn("status_reason", _sql_without_comments(sql))
```

**Storage-owned insertion guard** (lines 599-613):

```python
def test_runtime_and_echo_sources_do_not_reference_audit_table(
    self,
) -> None:
    paths = (
        pathlib.Path("backend/pipeline/ingestion/collector_runtime.py"),
        pathlib.Path("backend/pipeline/ingestion/collectors/echo/main.py"),
    )

    for path in paths:
        with self.subTest(path=str(path)):
            text = path.read_text()
            self.assertNotIn("feed_audit_events", text)
            self.assertNotIn("INSERT INTO feed_audit_events", text)
```

If Phase 5 adds static guards for retention ownership, copy this path-loop style and assert no service/runtime file deletes from `feed_audit_events`.

---

### `backend/pipeline/storage/tests/test_feed_lifecycle.py` (test, transform)

**Analog:** `backend/pipeline/storage/tests/test_feed_lifecycle.py`

**Diagnostic detail sanitizer pattern** (lines 53-82):

```python
def test_status_reason_detail_storage_value_normalizes_whitespace() -> None:
    result = feed_lifecycle.status_reason_detail_storage_value(
        " provider\n\n timeout\twhile connecting "
    )

    assert result == "provider timeout while connecting"

def test_status_reason_detail_storage_value_redacts_credentials() -> None:
    result = feed_lifecycle.status_reason_detail_storage_value(
        "Authorization: Bearer abc.def password=hunter2 "
        "api_key=sk-testvalue123 secret='hidden'"
    )

    assert result is not None
    assert "abc.def" not in result
    assert "hunter2" not in result
    assert "sk-testvalue123" not in result
    assert "hidden" not in result
    assert result.count("[redacted]") >= 3

def test_status_reason_detail_storage_value_caps_reason() -> None:
    long_reason = "x" * (quarantine_reason.MAX_QUARANTINE_REASON_LENGTH + 1)

    result = feed_lifecycle.status_reason_detail_storage_value(long_reason)

    assert result is not None
    assert len(result) == quarantine_reason.MAX_QUARANTINE_REASON_LENGTH
    assert result.endswith("[truncated]")
```

Keep these in the v1 verification gate; retention must delete rows only, not redact or mutate retained payloads.

---

### `backend/services/feeds/tests/test_service.py` (test, request-response)

**Analog:** `backend/services/feeds/tests/test_service.py`

**Actor propagation pattern** (lines 34-82, 143-157):

```python
class TestFeedServiceAuditActor(unittest.IsolatedAsyncioTestCase):
    """Tests for feed service audit actor propagation."""

    def test_admin_mutation_methods_require_keyword_only_actor_id(self) -> None:
        for method_name in (
            "create_feed",
            "update_feed",
            "deactivate_feed",
            "delete_feed",
            "reset_feed",
        ):
            signature = inspect.signature(getattr(FeedService, method_name))
            actor = signature.parameters.get("actor_id")
            self.assertEqual(actor.kind, inspect.Parameter.KEYWORD_ONLY)
            self.assertIs(actor.default, inspect.Parameter.empty)

    async def test_delete_feed_passes_admin_actor_to_store(self) -> None:
        store.delete_feed.return_value = True
        service = FeedService(store)

        result = await service.delete_feed(str(_FEED_ID), actor_id=_ADMIN_ACTOR_ID)

        self.assertTrue(result)
        store.delete_feed.assert_awaited_once_with(
            _FEED_ID,
            actor_id=_ADMIN_ACTOR_ID,
        )
```

---

### `backend/services/feeds/tests/test_api.py` (test, request-response)

**Analog:** `backend/services/feeds/tests/test_api.py`

**Public compatibility pattern** (lines 81-87, 428-448, 689-703):

```python
def test_feed_model_exposes_status_reason_detail_not_quarantine_reason(
    self,
) -> None:
    """The feed API response uses canonical diagnostic detail only."""
    self.assertIn("status_reason_detail", Feed.model_fields)
    self.assertNotIn("quarantine_reason", Feed.model_fields)
```

```python
def test_get_feed_with_status_reason_detail(self) -> None:
    feed_id = uuid.uuid4()
    mock_feed = Feed(
        id=feed_id,
        name="Test Feed",
        source_type=SourceType.BCFY_FEEDS,
        source_feed_id="123",
        status=FeedStatus.FAILING,
        status_reason=FeedStatusReason.SOURCE_UNREACHABLE,
        status_reason_detail="provider timed out",
        last_heartbeat=None,
    )

    response = self.client.get(f"/v1/feeds/{feed_id}")

    data = response.json()
    self.assertEqual(data["status_reason_detail"], "provider timed out")
    self.assertNotIn("quarantine_reason", data)
```

```python
def test_delete_feed_success(self) -> None:
    response = self.client.delete(
        f"/v1/feeds/{feed_id}",
        headers=_ACTOR_HEADERS,
    )

    self.assertEqual(response.status_code, status.HTTP_204_NO_CONTENT)
    self.mock_service.delete_feed.assert_called_once_with(
        str(feed_id),
        actor_id=_ACTOR_ID,
    )
```

---

### `frontend/api/src/feeds/feedsController.test.ts` (test, request-response)

**Analog:** `frontend/api/src/feeds/feedsController.test.ts`

**Backend-to-frontend diagnostic detail mapping pattern** (lines 59-75):

```typescript
const mockBackendFeed = {
  id: 'feed_123',
  name: 'Test Feed',
  source_type: 'openmhz',
  source_feed_id: 'src_123',
  status: 'active',
  substatus: 'active',
  last_heartbeat: '2024-01-01T00:00:00Z',
  status_reason_detail: 'provider timeout',
};

const expectedFrontendFeed = {
  id: 'feed_123',
  name: 'Test Feed',
  sourceType: 'openmhz',
};
```

**Admin delete request pattern** (lines 591-605):

```typescript
describe('deleteFeed', () => {
  const mockAdminRequest = requestWithUser({ isAdmin: true, sub: adminSub });

  it('should return 204 on success', async () => {
    mockRequest.mockResolvedValueOnce({ status: 204 });

    const controller = new FeedsController();
    await controller.deleteFeed('feed_123', mockAdminRequest);

    expect(mockRequest).toHaveBeenCalledWith({
      url: 'http://feeds-api.example.com/feed_123',
      method: 'DELETE',
      headers: adminActorHeaders,
    });
  });
});
```

## Shared Patterns

### pg_cron Skip Convention

**Sources:** `backend/pipeline/common/test_schema_helper.py`, `local_dev/docker_postgres_init.sh`, `.github/workflows/ci.yml`, `terraform/modules/alloydb/main.tf`

**Apply to:** New migration naming and testability split.

`backend/pipeline/common/test_schema_helper.py` lines 14-27:

```python
async def async_apply_test_schema(conn: Any) -> None:
    """Applies all ingestion SQL migration files in filename order using asyncpg."""
    sql_files = sorted(
        (f for f in _SQL_DIR.glob("*.sql") if "pg_cron" not in f.name),
        key=lambda f: f.name,
    )
    for sql_file in sql_files:
        content = sql_file.read_text()
        await conn.execute(content)
```

`local_dev/docker_postgres_init.sh` lines 1-20:

```sh
# Applies SQL migrations from /sql in lexical order, skipping pg_cron files.
# pg_cron is production-only (AlloyDB alloydb.enable_pg_cron flag); the
# postgres:15-alpine image used by docker-compose doesn't ship the extension,
for f in /sql/*.sql; do
    name=$(basename "$f")
    case "$name" in
        *pg_cron*)
            echo "SKIP $name (pg_cron extension not available in docker-compose postgres)"
            continue
            ;;
    esac
    psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" -f "$f"
done
```

`.github/workflows/ci.yml` lines 366-379:

```yaml
run: |
  set -euo pipefail
  for f in *.sql; do
    case "$f" in
      *pg_cron*)
        echo "Skipping $f (pg_cron extension not installed in CI)";
        continue;;
    esac
    echo "Applying $f..."
    psql -v ON_ERROR_STOP=1 -f "$f"
  done
```

`terraform/modules/alloydb/main.tf` lines 98-117:

```hcl
# Applies the ingestion DDL (sql/ingestion/*.sql) to the AlloyDB instance ...
locals {
  schema_sql_files = var.apply_schema ? sort(fileset("${path.module}/sql/ingestion", "*.sql")) : []
  schema_sql_combined = join("\n", [
    for f in local.schema_sql_files : file("${path.module}/sql/ingestion/${f}")
  ])
}
```

### Storage-Owned Audit Insertions

**Sources:** `backend/pipeline/storage/feed_queries.py`, `backend/pipeline/storage/feed_store.py`

**Apply to:** Verification expectations, no runtime/service direct insertions, retention ownership.

`feed_queries.py` lines 469-507:

```python
ALLOCATE_FEED_AUDIT_SEQUENCE_SQL = """\
INSERT INTO feed_audit_event_sequences (feed_id, next_sequence)
VALUES ($1, 2)
ON CONFLICT (feed_id) DO UPDATE
SET next_sequence = feed_audit_event_sequences.next_sequence + 1,
    updated_at = NOW()
RETURNING next_sequence - 1 AS feed_sequence
"""

INSERT_FEED_AUDIT_EVENT_SQL = """\
INSERT INTO feed_audit_events (
    feed_id,
    feed_name,
    source_type,
    action,
    actor_id,
    feed_sequence,
    status,
    status_reason,
    status_reason_detail,
    before_values,
    after_values,
    metadata
)
VALUES (
    $1,
    $2,
    $3,
    $4,
    $5,
    $6,
    $7::feed_status,
    $8,
    $9,
    $10::jsonb,
    $11::jsonb,
    COALESCE($12::jsonb, '{}'::jsonb)
)
"""
```

`feed_store.py` lines 364-419:

```python
async def _allocate_feed_sequence(
    self,
    conn: asyncpg.Connection,
    feed_id: uuid.UUID,
) -> int:
    """Allocate the next per-feed audit sequence in the open transaction."""
    feed_sequence = await conn.fetchval(
        feed_queries.ALLOCATE_FEED_AUDIT_SEQUENCE_SQL,
        feed_id,
    )
    if feed_sequence is None:
        msg = f"Failed to allocate audit sequence for feed {feed_id}"
        raise ValueError(msg)
    return int(feed_sequence)

async def _insert_feed_audit_event(
    self,
    conn: asyncpg.Connection,
    *,
    action: str,
    actor_id: str,
    before_values: dict[str, object],
    after_values: dict[str, object],
    identity_row: asyncpg.Record,
    metadata: dict[str, object] | None = None,
) -> None:
    """Insert one storage-owned feed audit event."""
    feed_sequence = await self._allocate_feed_sequence(conn, feed_id)
    await conn.execute(
        feed_queries.INSERT_FEED_AUDIT_EVENT_SQL,
        feed_id,
        identity["feed_name"],
        identity["source_type"],
        action,
        actor_id,
        feed_sequence,
        identity["status"],
        identity["status_reason"],
        identity["status_reason_detail"],
        json.dumps(before_values, default=self._json_default, sort_keys=True),
        json.dumps(after_values, default=self._json_default, sort_keys=True),
        json.dumps(metadata or {}, sort_keys=True),
    )
```

### V1 Verification Gate Command Shape

**Source:** `05-RESEARCH.md`, `AGENTS.md`, `.agents/instructions.md`

**Apply to:** Phase 5 verification plan.

Use targeted low-resource tests locally, preferably with `safe-run`, and keep Testcontainers integration tests as CI/prepared-machine lanes unless explicitly approved.

```bash
safe-run -- uv run python -m pytest \
  backend/pipeline/storage/tests/test_feed_audit_contract.py \
  backend/pipeline/storage/tests/test_feed_lifecycle.py \
  backend/pipeline/storage/tests/test_feed_query_contracts.py \
  backend/pipeline/storage/tests/test_feed_store.py \
  backend/pipeline/storage/tests/test_sync_feed_store.py \
  backend/services/feeds/tests/test_api.py \
  backend/services/feeds/tests/test_service.py \
  -q
```

## No Analog Found

No Phase 5 file lacks a close codebase analog. The retention helper migration is not an exact existing file, but the codebase has strong partial analogs for bounded SQL batches, audit schema invariants, static contract tests, and DB-backed storage integration tests.

## Metadata

**Analog search scope:** `terraform/modules/alloydb/sql/ingestion/`, `backend/pipeline/storage/`, `backend/pipeline/storage/tests/`, `backend/services/feeds/tests/`, `frontend/api/src/feeds/`, `integration_tests/storage/`, `documentation/`, local/CI schema helpers.
**Files scanned:** 34 candidate files by `rg --files`, plus targeted line reads from 18 files.
**Pattern extraction date:** 2026-06-20
