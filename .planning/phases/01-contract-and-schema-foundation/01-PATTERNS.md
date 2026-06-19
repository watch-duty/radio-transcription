# Phase 1: Contract and Schema Foundation - Pattern Map

**Mapped:** 2026-06-19
**Files analyzed:** 6
**Analogs found:** 6 / 6

## File Classification

| New/Modified File | Role | Data Flow | Closest Analog | Match Quality |
|-------------------|------|-----------|----------------|---------------|
| `terraform/modules/alloydb/sql/ingestion/029_feed_audit_events.sql` | migration | batch DDL | `terraform/modules/alloydb/sql/ingestion/024_feeds_status_reason.sql`, `022_audio_segments_annotations.sql` | role-match |
| `terraform/modules/alloydb/sql/ci/hot_protection_check.sql` | test | batch validation | `terraform/modules/alloydb/sql/ci/hot_protection_check.sql` | exact |
| `documentation/feed-audit-events.md` | documentation | file-I/O | `.planning/PROJECT.md`, `CONTEXT.md` | role-match |
| `CONTEXT.md` | documentation | file-I/O | `CONTEXT.md` | exact |
| `backend/pipeline/storage/feed_audit.py` | utility | transform | `backend/pipeline/storage/quarantine_reason.py`, `backend/pipeline/storage/feed_store.py` | role-match |
| `backend/pipeline/storage/tests/test_feed_store.py` | test | file-I/O / transform | `backend/pipeline/storage/tests/test_feed_store.py` | exact |

## Pattern Assignments

### `terraform/modules/alloydb/sql/ingestion/029_feed_audit_events.sql` (migration, batch DDL)

**Analog:** `terraform/modules/alloydb/sql/ingestion/024_feeds_status_reason.sql`

**Migration numbering pattern:** Existing ingestion migrations are ordered SQL
files under `terraform/modules/alloydb/sql/ingestion/`. The current highest
file is `028_initialize_feed_bookmarks.sql`; Phase 1 should use
`029_feed_audit_events.sql`.

**Current feed schema pattern** (`003_feeds.sql` lines 1-15):

```sql
-- Idempotent: IF NOT EXISTS allows safe re-application during Terraform runs.
CREATE TABLE IF NOT EXISTS feeds (
    id                      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name                    VARCHAR(255) NOT NULL UNIQUE,
    source_type             TEXT NOT NULL REFERENCES source_types(slug),
    status                  feed_status NOT NULL DEFAULT 'unclaimed'::feed_status,
    failure_count           INT NOT NULL DEFAULT 0,

    -- Dynamic leasing & state columns
    worker_id               UUID,
    last_heartbeat          TIMESTAMP WITH TIME ZONE,
    last_processed_filename TEXT,

    created_at              TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);
```

Use these existing `feeds` row fields to choose the delete snapshot allowlist.
Do not include dynamic leasing fields by default: `worker_id`,
`last_heartbeat`, and `last_processed_filename` are explicitly operational
state.

**Nullable column migration pattern** (`024_feeds_status_reason.sql` lines 1-10):

```sql
-- Add canonical feed status reason fields for operator triage.
-- Existing rows remain NULL until application code records or clears a reason.
-- status_reason_updated_at records the last status-reason observation or clear.
-- Known values are enforced by application code.
-- No index is added because this phase has no status-reason query path.
ALTER TABLE feeds
    ADD COLUMN IF NOT EXISTS status_reason TEXT;

ALTER TABLE feeds
    ADD COLUMN IF NOT EXISTS status_reason_updated_at TIMESTAMP WITH TIME ZONE;
```

Copy this for `feeds.status_reason_detail`: comment the lifecycle, keep existing
rows `NULL`, use `ADD COLUMN IF NOT EXISTS`, and do not add an index on the
mutable current-state column.

**Diagnostic compatibility pattern** (`020_quarantine_reason.sql` lines 1-9):

```sql
-- Add quarantine_reason column to feeds for operator triage.
-- Populated by REPORT_FAILURE_SQL when a feed transitions to 'quarantined',
-- storing the reason string passed by the catch-all handler in
-- collector_runtime._process_feed (typically str(exc) or type(exc).__name__).
-- NULL when the feed has never been quarantined.
-- No CHECK constraint: reason strings are free-form.
-- No index: read on individual rows during triage, not in hot-path queries.
ALTER TABLE feeds
    ADD COLUMN IF NOT EXISTS quarantine_reason TEXT;
```

Mirror the free-text compatibility posture, but add the new Phase 1 length
constraint for `status_reason_detail` because the contract locks the 2048
character cap.

**Table and index pattern** (`022_audio_segments_annotations.sql` lines 16-31,
45-51):

```sql
-- Create audio_segments table with metadata matching legacy transcripts structure.
CREATE TABLE IF NOT EXISTS audio_segments (
    id                      UUID PRIMARY KEY,
    feed_id                 UUID NOT NULL REFERENCES feeds(id),
    classification          AUDIO_CLASSIFICATION NOT NULL,
    start_timestamp         TIMESTAMP WITH TIME ZONE NOT NULL,
    end_timestamp           TIMESTAMP WITH TIME ZONE NOT NULL,
    missing_prior_context   BOOLEAN NOT NULL DEFAULT FALSE,
    missing_post_context    BOOLEAN NOT NULL DEFAULT FALSE,
    source_audio_uris       TEXT[] NOT NULL DEFAULT '{}',
    canonical_audio_uri     TEXT,
    start_audio_offset      INTERVAL,
    end_audio_offset        INTERVAL,
    playback_audio_uri      TEXT,
    created_at              TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

-- Composite index on audio_segments for high-performance feed-based keyset pagination.
CREATE INDEX IF NOT EXISTS idx_audio_segments_feed_pagination
    ON audio_segments (feed_id, end_timestamp DESC, id DESC);

-- GIN index on data column to quickly query JSON parameters inside annotations.
CREATE INDEX IF NOT EXISTS idx_annotations_data
    ON annotations USING GIN (data);
```

Use `CREATE TABLE IF NOT EXISTS` and `CREATE INDEX IF NOT EXISTS` for
`feed_audit_events`. Prefer timeline-oriented indexes such as
`(feed_id, feed_sequence)` or `(feed_id, occurred_at DESC, id DESC)` and
retention-oriented `occurred_at`; do not index mutable `feeds` diagnostic
fields.

**Constraint guard pattern** (`021_feed_properties_tags.sql` lines 41-50):

```sql
-- Add constraint using the validation function if not exists.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.table_constraints
        WHERE table_name = 'feed_properties' AND constraint_name = 'valid_tags_schema'
    ) THEN
        ALTER TABLE feed_properties ADD CONSTRAINT valid_tags_schema CHECK (validate_feed_tags(tags));
    END IF;
END $$;
```

Use this `DO $$ BEGIN IF NOT EXISTS ... ALTER TABLE ADD CONSTRAINT ... END $$`
shape for idempotent checks such as `feeds_status_reason_detail_length`,
`feed_audit_events_actor_id_check`, and uniqueness constraints if `ALTER TABLE`
is used instead of inline table constraints.

**Delete-survival anti-pattern** (`012_feed_properties.sql` lines 1-6):

```sql
-- Idempotent: IF NOT EXISTS allows safe re-application during Terraform runs.
CREATE TABLE IF NOT EXISTS feed_properties (
    feed_id     UUID PRIMARY KEY REFERENCES feeds(id) ON DELETE CASCADE,
    source_feed_id  TEXT NOT NULL,
    external_id TEXT NOT NULL -- The ID used for mapping feed ID within application
);
```

Do not copy `ON DELETE CASCADE` for `feed_audit_events.feed_id`. Cascading is
appropriate for current-state child tables like `feed_properties`, but the Phase
1 contract requires audit rows to survive hard delete.

**Hard delete behavior to preserve** (`feed_queries.py` lines 491-502):

```python
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
```

The audit schema must not depend on the `feeds` row still existing after this
delete. Store `feed_id` as data plus event-time identity columns and
`before_values`.

**pg_cron naming convention to avoid in Phase 1** (`019_feeds_pg_cron_jobs.sql`
lines 10-20):

```sql
-- File-naming convention (load-bearing): any migration whose application
-- requires pg_cron must have "pg_cron" in its filename. The substring is
-- matched by (a) the CI guard job in .github/workflows/ci.yml, (b) the
-- docker-compose postgres init script in local_dev/docker_postgres_init.sh,
-- and (c) the integration-test fixtures under backend/**/tests/ and
-- integration_tests/**/conftest.py. Any of those contexts runs against a
-- vanilla postgres image that lacks the pg_cron extension, so they skip
-- files matching *pg_cron*. If a future migration breaks this convention,
-- local tests and docker-compose will crash at CREATE EXTENSION with no
-- useful hint about where to apply the fix.
CREATE EXTENSION IF NOT EXISTS pg_cron;
```

Phase 1 should document 18-month retention and may add an `occurred_at` index,
but should not add a pg_cron retention job in `029_feed_audit_events.sql`.
Retention enforcement is Phase 5.

---

### `terraform/modules/alloydb/sql/ci/hot_protection_check.sql` (test, batch validation)

**Analog:** `terraform/modules/alloydb/sql/ci/hot_protection_check.sql`

**Purpose pattern** (lines 1-10):

```sql
-- HOT-protection guard. Runs against the schema produced by applying all
-- files under sql/ingestion/*.sql to a fresh PostgreSQL 16 instance.
-- Returns one row per (index, column) pair that violates the HOT invariant
-- - CI fails the build if any row is returned.
--
-- Invariant: no index on the feeds table may reference a column that the
-- hot write path mutates, because PostgreSQL's Heap-Only Tuple optimization
-- is disabled for an UPDATE whenever any indexed column is modified. The
-- eight guarded columns below are all mutated at high frequency by claim,
-- heartbeat, progress, release, or failure paths.
```

If Phase 1 or the planner treats `feeds.status_reason_detail` as hot-path
mutated with failure/recovery writes, add it to the guarded column list and do
not create an index on it.

**Guarded column list pattern** (lines 33-50):

```sql
SELECT c.relname AS indexname, a.attname
  FROM pg_class t
  JOIN pg_index x ON x.indrelid = t.oid
  JOIN pg_class c ON c.oid = x.indexrelid
  JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(x.indkey)
 WHERE t.relname = 'feeds'
   AND t.relnamespace = 'public'::regnamespace
   AND a.attname IN (
       'last_heartbeat',
       'unclaimed_since',
       'worker_id',
       'fencing_token',
       'last_processed_filename',
       'last_bookmark_time',
       'failure_count',
       'retry_after'
   )
   AND c.relname <> 'idx_feeds_failing_retryable';
```

Keep the OID-based join structure. Only add column names to the `a.attname IN`
list; do not rewrite the query.

**CI validation pattern** (`.github/workflows/ci.yml` lines 304-371):

```yaml
# Applies the ingestion DDL (sql/ingestion/*.sql, excluding *pg_cron* files
# which require an extension not shipped with postgres:16-alpine) to a
# disposable Postgres 16 service, then runs the HOT-protection guard at
# sql/ci/hot_protection_check.sql. Fails the build if any index on the
# feeds table references a mutated hot-path column. This prevents future
# migrations from silently defeating HOT updates and bloating the table.
alloydb-hot-protection-check:
  needs: [setup]
  if: needs.setup.outputs.should_run == 'true' && needs.setup.outputs.requires_backend == 'true'
  runs-on: ubuntu-24.04
...
      for f in *.sql; do
        case "$f" in
          *pg_cron*)
            echo "Skipping $f (pg_cron extension not installed in CI)";
            continue;;
        esac
        echo "Applying $f..."
        psql -v ON_ERROR_STOP=1 -f "$f"
      done
...
      violations=$(psql -v ON_ERROR_STOP=1 -t -A \
        -f terraform/modules/alloydb/sql/ci/hot_protection_check.sql)
```

This is the main live schema validation path. Local planner verification should
prefer text-based tests and `git diff --check` unless a live DB check is
explicitly approved.

---

### `documentation/feed-audit-events.md` (documentation, file-I/O)

**Analog:** `.planning/PROJECT.md` and `CONTEXT.md`

**Domain-first opening pattern** (`.planning/PROJECT.md` lines 1-13):

```markdown
# Feed Audit Events V1

## What This Is

Feed Audit Events V1 adds durable, queryable history for meaningful feed
mutations in the radio transcription backend. It is for Watch Duty engineers
and future admin tooling that need to answer what happened to a feed, when it
happened, what changed, and whether the cause was a human action or system
runtime behavior.

This project is not full event sourcing. The current `feeds` row remains the
authoritative current-state model; the new work adds an append-only audit
history and a cleaner current diagnostic detail field.
```

Start the new document with domain meaning, not schema DDL. The storage table
is supporting detail.

**Scope and boundary pattern** (`.planning/PROJECT.md` lines 100-120):

```markdown
## Constraints

- **Brownfield architecture**: Preserve the existing current-state `feeds`
  model, storage-layer SQL patterns, and FastAPI service boundaries - the
  ingestion runtime already depends on current-state lease queries and fenced
  writes.
- **Database consistency**: Feed mutations and audit inserts must commit or
  roll back together - audit history is only useful if it cannot drift from
  successful state changes.
- **Compatibility**: Existing consumers of `quarantine_reason` must keep
  working during the v1 rollout - add `status_reason_detail` without removing
  the old field immediately.
- **Signal quality**: Do not audit routine heartbeat or lease churn by default
  - the audit table must stay understandable and affordable.
- **Retention**: Keep feed audit events for 18 months - this is the v1 product
  target and should be enforced, not just documented.
- **Security**: Do not persist secrets, tokens, raw credential-bearing
  exception strings, or unbounded provider responses in diagnostic detail -
  persisted reason text must be bounded and scrubbed where needed.
- **Delivery boundary**: WD backend delivery is a later phase - v1 schema should
  support it without introducing dispatcher state or webhook attempts yet.
```

For Phase 1, update the security wording to match locked decisions:
`status_reason_detail` preserves raw emitted detail with a 2048-character cap
and no redaction beyond length cap in Phase 1. Call out the tradeoff directly.

**Terminology pattern** (`CONTEXT.md` lines 238-280):

```markdown
### Status Reason

The current canonical abnormal-condition label for a feed. It is visible to
operators and is the v1 routing key for failure policy decisions.

### Status Reason Owner

The coarse ownership namespace encoded by a status reason prefix: `source`,
`system`, or `pipeline`. It identifies the layer that owns the abnormal
condition and is distinct from retry, quarantine, and logging policy.
...
### Quarantine Reason

The detailed diagnostic message persisted when a feed failure episode
crosses the quarantine threshold. It describes that threshold-crossing episode
for debugging; it is not the lifecycle owner label and does not summarize the
full failure budget history. It is not a stable machine-readable code and
should not drive control flow. Ingestion keeps the full useful diagnostic in
memory; storage caps it only at the database persistence boundary.
```

Use the same concise term format for `Feed Audit Event`, `Audit History`,
`Current Feed State`, `Status Reason Detail`, and `Actor ID`.

**Markdown location style** (`documentation/local-dev-mock-audio.md` lines 1-6):

```markdown
# Mock Audio Files Directory

The `local_dev/mock_audio` directory is mounted to the local `mock-audio-server` Docker container at `/data`.

When you run the system locally, the mock audio server will automatically serve audio files from subdirectories matching the feed's source type and source feed ID.
```

The only existing `documentation/` file is procedural local-dev documentation.
For `feed-audit-events.md`, prefer the project/domain style above and keep
schema snippets secondary.

---

### `CONTEXT.md` (documentation, file-I/O)

**Analog:** `CONTEXT.md`

**Insertion pattern:** Add new terms under `## Ingestion Context` near the
current `Status Reason` and `Quarantine Reason` entries. Keep headings as
`### Term Name`, followed by one or two short paragraphs. Avoid implementation
checklists in `CONTEXT.md`; put detailed field tables in
`documentation/feed-audit-events.md`.

**Existing term shape** (lines 238-280):

```markdown
### Status Reason

The current canonical abnormal-condition label for a feed. It is visible to
operators and is the v1 routing key for failure policy decisions.

### Status Reason Owner

The coarse ownership namespace encoded by a status reason prefix: `source`,
`system`, or `pipeline`. It identifies the layer that owns the abnormal
condition and is distinct from retry, quarantine, and logging policy.
...
### Quarantine Reason

The detailed diagnostic message persisted when a feed failure episode
crosses the quarantine threshold. It describes that threshold-crossing episode
for debugging; it is not the lifecycle owner label and does not summarize the
full failure budget history. It is not a stable machine-readable code and
should not drive control flow. Ingestion keeps the full useful diagnostic in
memory; storage caps it only at the database persistence boundary.
```

**Apply to:** New glossary terms should distinguish:

- current feed state: the authoritative `feeds` row
- audit history: append-only Feed Audit Events
- status reason: typed machine-readable abnormal reason
- status reason detail: bounded explanatory text, not control flow
- quarantine reason: legacy compatibility detail
- actor ID: required namespaced causal actor string

---

### `backend/pipeline/storage/feed_audit.py` (utility, transform)

**Analog:** `backend/pipeline/storage/quarantine_reason.py`

This file is optional for Phase 1. Create it only if the planner wants a
storage-adjacent contract helper for action constants, actor namespace checks,
snapshot allowlists, or a generalized diagnostic cap helper. Do not add storage
insert behavior here in Phase 1.

**Small helper pattern** (`quarantine_reason.py` lines 1-14):

```python
"""Storage-boundary helpers for feed quarantine reasons."""

from __future__ import annotations

MAX_QUARANTINE_REASON_LENGTH = 2048
_TRUNCATION_MARKER = " [truncated]"


def cap_quarantine_reason_for_storage(text: str) -> str:
    """Cap quarantine text while keeping a visible truncation marker."""
    if len(text) <= MAX_QUARANTINE_REASON_LENGTH:
        return text
    prefix_len = MAX_QUARANTINE_REASON_LENGTH - len(_TRUNCATION_MARKER)
    return f"{text[:prefix_len].rstrip()}{_TRUNCATION_MARKER}"
```

Copy this shape for `MAX_STATUS_REASON_DETAIL_LENGTH = 2048` if a new helper is
introduced. Keep it cap-only; Phase 1 explicitly does not add redaction.

**Import and enum pattern** (`feed_store.py` lines 1-17, 53-80):

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
)
from backend.pipeline.storage import quarantine_reason
...
class SourceType(enum.StrEnum):
    """Supported audio source types.

    Each value corresponds to a slug in the ``source_types`` database table.
```

Use `enum.StrEnum` for any Python action vocabulary constants. Keep comments
near multi-place contracts, as `SourceType` does for database seed and runtime
spec coupling.

**Validation/error pattern** (`feed_store.py` lines 104-140, 261-272):

```python
_FEED_STATUS_REASON_OWNERS = frozenset({"source", "system", "pipeline"})


def _status_reason_owner(status_reason: str) -> str:
    """Return the owner namespace encoded by a status-reason prefix."""
    owner, separator, _ = status_reason.partition("_")
    if not separator or owner not in _FEED_STATUS_REASON_OWNERS:
        msg = f"Unsupported status reason owner in {status_reason!r}"
        raise ValueError(msg)
    return owner
...
            try:
                status_reason = FeedStatusReason(status_reason_raw)
            except ValueError as e:
                msg = (
                    f"Unknown status reason {status_reason_raw!r} "
                    f"for feed {row['id']}"
                )
                raise ValueError(msg) from e
```

If actor/action validators are added, raise `ValueError` with explicit messages
and chain enum parse failures.

---

### `backend/pipeline/storage/tests/test_feed_store.py` (test, file-I/O / transform)

**Analog:** `backend/pipeline/storage/tests/test_feed_store.py`

Use text-level contract tests for SQL migration shape and helper behavior. This
keeps Phase 1 validation low-resource and avoids requiring a live database.

**Imports and constants pattern** (lines 1-24):

```python
from __future__ import annotations

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
```

If `feed_audit.py` is added, import it alongside `quarantine_reason`.

**Migration contract test pattern** (lines 91-121, 127-136):

```python
class TestStatusReasonMigrationContract(unittest.TestCase):
    """Contract tests for the Phase 1 status reason migration."""

    _MIGRATION = pathlib.Path(
        "terraform/modules/alloydb/sql/ingestion/024_feeds_status_reason.sql"
    )

    def test_adds_only_nullable_status_reason_columns(self) -> None:
        self.assertTrue(self._MIGRATION.exists())
        text = self._MIGRATION.read_text()
        sql = _sql_without_comments(text)

        column_defs = [
            (name.lower(), " ".join(definition.upper().split()))
            for name, definition in re.findall(
                r"ADD COLUMN IF NOT EXISTS\s+(\w+)\s+([^;]+);",
                sql,
                flags=re.IGNORECASE,
            )
        ]
        self.assertEqual(
            column_defs,
            [
                ("status_reason", "TEXT"),
                (
                    "status_reason_updated_at",
                    "TIMESTAMP WITH TIME ZONE",
                ),
            ],
        )
        self.assertNotIn("quarantine_reason", text.lower())
...
        for token in (
            "default",
            "update feeds",
            "check",
            "create index",
            "create type",
        ):
            self.assertNotIn(token, low_sql)
```

For `029_feed_audit_events.sql`, adapt this pattern to assert the table name,
required columns, `actor_id` check, `feed_sequence` uniqueness, JSONB snapshot
defaults, and no cascading FK to `feeds`.

For `status_reason_detail`, do not copy the old `self.assertNotIn("check", ...)`
expectation. Phase 1 should add a length check.

**Query projection pattern** (lines 326-337):

```python
def test_full_feed_queries_project_status_reason_fields(self) -> None:
    for sql in (
        feed_queries.CREATE_FEED_SQL,
        feed_queries.GET_FEED_SQL,
        feed_queries.LIST_FEEDS_DESC_SQL,
        feed_queries.LIST_FEEDS_ASC_SQL,
        feed_queries.RESET_FEED_SQL,
        feed_queries.UPDATE_FEED_SQL,
    ):
        self.assertRegex(sql, r"\bstatus_reason\b")
        self.assertRegex(sql, r"\bstatus_reason_updated_at\b")
```

Phase 1 should not require query projection updates unless it adds Python
storage shape changes. If `status_reason_detail` is only a database column in
Phase 1, leave query/API projection tests for Phase 3.

**Cap helper test pattern** (lines 929-953):

```python
async def test_caps_quarantine_reason_at_persistence_boundary(
    self,
) -> None:
    """Long reasons are capped only before database writes."""
    pool = make_mock_pool(
        fetchrow_result={
            "status": "failing",
            "failure_count": 1,
            "retry_after": None,
        },
    )
    store = FeedStore(pool)
    long_reason = "x" * (quarantine_reason.MAX_QUARANTINE_REASON_LENGTH + 1)

    await store.report_feed_failure(
        _FEED_ID,
        _WORKER_ID,
        1,
        reason=long_reason,
    )

    reason_arg = pool.fetchrow.call_args[0][-2]
    self.assertEqual(
        len(reason_arg),
        quarantine_reason.MAX_QUARANTINE_REASON_LENGTH,
```

If a `cap_status_reason_detail_for_storage` helper is added in Phase 1, create
a direct helper test rather than coupling it to write paths that are owned by
later phases.

**Sync parity cap pattern** (`test_sync_feed_store.py` lines 156-166):

```python
def test_caps_quarantine_reason_at_persistence_boundary(self) -> None:
    conn = _make_mock_conn()
    store = _make_store(conn)
    feed_id = uuid.uuid4()
    long_reason = "x" * (quarantine_reason.MAX_QUARANTINE_REASON_LENGTH + 1)

    store.record_failure(feed_id, reason=long_reason)

    reason_arg = conn.execute.call_args[0][1][5]
    assert len(reason_arg) == quarantine_reason.MAX_QUARANTINE_REASON_LENGTH
    assert reason_arg.endswith("[truncated]")
```

Use this only in later phases when sync and async writers both persist
`status_reason_detail`.

## Shared Patterns

### Ordered SQL Migration Replay

**Source:** `backend/pipeline/common/test_schema_helper.py`
**Apply to:** Any SQL migration under `terraform/modules/alloydb/sql/ingestion/`

```python
async def async_apply_test_schema(conn: Any) -> None:
    """Applies all ingestion SQL migration files in filename order using asyncpg."""
    sql_files = sorted(
        (f for f in _SQL_DIR.glob("*.sql") if "pg_cron" not in f.name),
        key=lambda f: f.name,
    )
    for sql_file in sql_files:
        content = sql_file.read_text()
        if content.startswith("-- AUTOCOMMIT"):
            for statement in content.split(";"):
                if statement.strip():
                    await conn.execute(statement.strip())
        else:
            await conn.execute(content)
```

`029_feed_audit_events.sql` must be safe to run in filename order and should not
depend on pg_cron.

### Test Schema Helper Unit Pattern

**Source:** `backend/pipeline/common/tests/test_schema_helper.py`
**Apply to:** Tests for migration replay behavior

```python
mock_file_normal.name = "001_normal.sql"
mock_file_normal.read_text.return_value = "CREATE TABLE a (id UUID);"

mock_file_autocommit.name = "027_autocommit.sql"
mock_file_autocommit.read_text.return_value = "-- AUTOCOMMIT\nALTER TYPE a ADD VALUE 'B';\nALTER TYPE a ADD VALUE 'C';"
...
# Verified normal files are executed as a single block
mock_conn.execute.assert_any_call("CREATE TABLE a (id UUID);")
# Verified autocommit files are split by semicolon and executed individually
```

No change is expected here for Phase 1 unless the migration introduces a new
special execution mode. It should not.

### Diagnostic Detail Cap

**Source:** `backend/pipeline/storage/quarantine_reason.py`
**Apply to:** `status_reason_detail` helper or documentation

```python
MAX_QUARANTINE_REASON_LENGTH = 2048
_TRUNCATION_MARKER = " [truncated]"


def cap_quarantine_reason_for_storage(text: str) -> str:
    """Cap quarantine text while keeping a visible truncation marker."""
    if len(text) <= MAX_QUARANTINE_REASON_LENGTH:
        return text
    prefix_len = MAX_QUARANTINE_REASON_LENGTH - len(_TRUNCATION_MARKER)
    return f"{text[:prefix_len].rstrip()}{_TRUNCATION_MARKER}"
```

Copy cap behavior, not the old field name. Document that Phase 1 preserves raw
capped detail and does not redact beyond length.

### Status Reason Current-State SQL

**Source:** `backend/pipeline/storage/feed_queries.py`
**Apply to:** Later writer phases; Phase 1 docs should reference the behavior

```python
REPORT_FAILURE_SQL = """\
UPDATE feeds
SET status = CASE WHEN failure_count + 1 >= $3
                  THEN 'quarantined'::feed_status
                  ELSE 'failing'::feed_status END,
    failure_count = failure_count + 1,
    worker_id = NULL,
    retry_after = CASE WHEN failure_count + 1 < $3
                       THEN NOW() + LEAST($5 * INTERVAL '1 second',
                            $6 * INTERVAL '1 second' * POWER(2, failure_count))
                            + (RANDOM() * INTERVAL '10 seconds')
                       ELSE NULL END,
    -- COALESCE protects against an edge call passing reason=None during
    -- the quarantine transition: keep the previously-recorded reason
    -- rather than overwriting it with NULL. A real reason still wins.
    quarantine_reason = CASE WHEN failure_count + 1 >= $3 THEN COALESCE($7, quarantine_reason) ELSE quarantine_reason END,
    status_reason = COALESCE($8, 'system_unexpected_error'),
    status_reason_updated_at = CASE
        WHEN status_reason IS DISTINCT FROM COALESCE($8, 'system_unexpected_error')
            THEN NOW()
        ELSE status_reason_updated_at
    END
```

The contract must keep `status_reason` as machine-readable control data and
`status_reason_detail` as explanatory text.

### Documentation Validation

**Source:** `AGENTS.md` and `.agents/instructions.md`
**Apply to:** Docs-only plans

For docs-only changes, use:

```bash
git diff --check
```

Do not run broad local pytest, Docker, component, API, or E2E lanes by default.
If Python text-level tests are added, keep them narrow, for example:

```bash
safe-run -- uv run python -m pytest backend/pipeline/storage/tests/test_feed_store.py -q
```

### No Live DB Local Default

**Source:** `.github/workflows/ci.yml` and `AGENTS.md`
**Apply to:** SQL validation planning

CI already applies ingestion SQL against Postgres 16 and runs the HOT guard.
Local Phase 1 checks should be text-based unless the user explicitly approves a
live DB or container-backed run.

## No Analog Found

| File | Role | Data Flow | Reason |
|------|------|-----------|--------|
| None | - | - | Every candidate file has at least a role-match analog. There is no exact existing append-only feed audit ledger, so planner should combine SQL table patterns with the Phase 1 research schema examples for action, actor, and sequence details. |

## Metadata

**Analog search scope:** `terraform/modules/alloydb/sql/ingestion/`,
`terraform/modules/alloydb/sql/ci/`, `documentation/`, `CONTEXT.md`,
`.planning/PROJECT.md`, `backend/pipeline/storage/`,
`backend/pipeline/common/`, `.github/workflows/`

**Files scanned:** 105 targeted files across SQL, storage, docs, common test
helpers, and CI workflow paths.

**Pattern extraction date:** 2026-06-19
