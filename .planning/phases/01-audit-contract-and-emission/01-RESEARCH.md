# Phase 1: Audit Contract and Emission - Research

**Researched:** 2026-06-26
**Domain:** Python storage-layer audit SQL, structured logging, asyncpg/psycopg
integration
**Confidence:** HIGH for Phase 1 storage/logging plan; MEDIUM for driver JSON
return normalization details because the implementation should tolerate both
string and mapping forms.

<user_constraints>
## User Constraints (from CONTEXT.md)

Source: `.planning/phases/01-audit-contract-and-emission/01-CONTEXT.md`
[VERIFIED: codebase grep]

### Locked Decisions

#### Phase Boundary

Phase 1 defines and emits the storage-boundary Feed Audit Notification log for every newly inserted `feed_audit_events` row. It does not route logs to Pub/Sub, call the Watch Duty webhook, add delivery state, add database polling, or change feed lifecycle semantics.

The implementation should treat `feed_audit_events` as the canonical audit ledger and structured logs as best-effort notification signals emitted after audited SQL returns the committed event payload.

#### Implementation Decisions

##### Audit Event Contract
- **D-01:** Emit one structured Feed Audit Notification for every newly inserted `feed_audit_events` row, including admin actions and ingestion/runtime lifecycle actions.
- **D-02:** Emit no notification when an audited SQL statement does not insert an audit row, such as no-op updates or suppressed repeated failure noise.
- **D-03:** Use `event_type="radio_transcription.feed_audit_notification"` and integer `schema_version=1`.
- **D-04:** Keep the v1 payload flat: `event_id`, `action`, `occurred_at`, `actor_id`, `feed_id`, `feed_revision`, `before_values`, and `after_values`, plus `event_type` and `schema_version`.
- **D-05:** Do not add extra fields solely for webhook readability. The Watch Duty endpoint currently requires `feed_id` and preserves unknown fields, so Phase 1 should support the agreed endpoint payload without inventing a broader event schema.

##### SQL and Result Shape
- **D-06:** Do not add extra database round trips. Audited SQL should return any notification payload in the same statement that writes the audit row.
- **D-07:** Return a single nullable JSONB column named `feed_audit_event` instead of many scalar `audit_*` columns. This avoids namespace confusion with `feeds.audit_revision` and future feed columns that may begin with `audit_`.
- **D-08:** Build the payload from database-returned audit row values, not request-local guesses, so notifications cannot describe a row that was not inserted.
- **D-09:** Avoid repeated transformation and encode/decode cycles. SQL may build the JSONB payload once; Python should parse only if the DB driver returns a JSON string, then pass the dict to logging with `extra={"json_fields": ...}`.

##### Emission Behavior
- **D-10:** Add a shared storage helper for notification preparation and structured log emission, reused by async `FeedStore` and sync `SyncFeedStore`.
- **D-11:** Notification emission failures must never affect ingestion, feed lifecycle writes, or audit row persistence. The helper should catch all local emission exceptions and avoid re-raising.
- **D-12:** Do not import Pub/Sub, webhook clients, Cloud Logging sink clients, or deployment-specific routing code into the feed storage path.
- **D-13:** Remove only storage-layer duplicate failure summary logs once the audit-shaped notification log covers the same event. Keep runtime policy logs, quarantine telemetry, admin/API logs, and unrelated operational logs.

##### Verification Boundary
- **D-14:** Phase 1 verification should focus on unit/query-contract tests and storage mock behavior. Integration or E2E tests are not required for the discussion outcome unless the plan finds a small targeted check with clear value.
- **D-15:** Tests should prove actual audit inserts produce one notification payload, suppressed/no-op audit paths produce no notification, helper failures are swallowed, and async/sync stores share the same emitter behavior.

### the agent's Discretion

The planner may choose exact helper names and SQL helper signatures, but should preserve the single-column `feed_audit_event` result shape and keep shared producer behavior out of routing/relay code.

### Deferred Ideas (OUT OF SCOPE)

- Cloud Logging sink, Pub/Sub topic/subscription, IAM, retry, and DLQ configuration belong to Phase 2.
- Cloud Run relay, Pub/Sub envelope parsing, WD webhook POST, and API key handling belong to Phase 3.
- Staging/prod rollout proof, operational dashboards, replay tooling, and runbooks belong to Phase 4 or later.
- Durable delivery, outbox tables, database polling, CDC, triggers, and direct webhook calls remain out of scope for this milestone unless requirements change.
</user_constraints>

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|------------------|
| AUDIT-01 | Every newly inserted `feed_audit_events` row emits exactly one best-effort structured Feed Audit Notification log. | Extend the shared audit insert CTE to return one `feed_audit_event` JSONB object and call one helper from every audited store method. [VERIFIED: `.planning/REQUIREMENTS.md`, `backend/pipeline/storage/feed_audit_sql.py`, `backend/pipeline/storage/feed_queries.py`, `backend/pipeline/storage/sync_feed_queries.py`] |
| AUDIT-02 | Feed state changes that do not insert a `feed_audit_events` row emit no Feed Audit Notification. | Preserve existing `where_sql="audit_action.action IS NOT NULL"` and no-op update paths; final result rows should expose `feed_audit_event=NULL` when `write_audit` is empty. [VERIFIED: `feed_audit_sql.py`, `feed_queries.py`, `sync_feed_queries.py`] |
| AUDIT-03 | Notification emission never raises to callers and never changes feed writes or audit persistence. | Put all normalization/logging in an isolation helper that catches local exceptions and returns without re-raising. [VERIFIED: `01-CONTEXT.md`, `AGENTS.md`, `.agents/instructions.md`] |
| AUDIT-04 | Async `FeedStore` and sync `SyncFeedStore` audited write paths use one shared notification helper. | Add one module under `backend/pipeline/storage/` and call it from async and sync stores; do not duplicate payload assembly in store methods. [VERIFIED: `feed_store.py`, `sync_feed_store.py`, `01-CONTEXT.md`] |
| AUDIT-05 | Storage SQL returns notification payload data from the same audited statement without an extra database round trip. | Join the `write_audit` CTE into the final `SELECT`; convert sync methods from `execute()` to row-consuming calls only where audited SQL can return a payload. [VERIFIED: `feed_queries.py`, `sync_feed_queries.py`, `sync_feed_store.py`] |
| PAYLOAD-01 | Each notification log includes `event_type="radio_transcription.feed_audit_notification"` and `schema_version=1`. | Build those two fields in the returned JSONB payload or final Python dict before `extra={"json_fields": ...}`. [VERIFIED: `01-CONTEXT.md`, `.planning/REQUIREMENTS.md`] |
| PAYLOAD-02 | Each payload is flat and includes `event_id`, `action`, `occurred_at`, `actor_id`, `feed_id`, `feed_revision`, `before_values`, and `after_values`. | Use `jsonb_build_object` from the inserted audit row fields; do not wrap the payload under a nested key. [VERIFIED: `029_feed_audit_events.sql`, `feed_audit_sql.py`, `01-CONTEXT.md`] |
| PAYLOAD-03 | Payload mirrors the existing feed audit snapshot allowlist and adds no raw request bodies, secrets, or webhook-only fields. | Reuse `audit_snapshot_sql()` and `AUDITED_FEED_STATE_FIELDS`; do not add fields outside that allowlist in Phase 1. [VERIFIED: `feed_audit_sql.py`, `.planning/REQUIREMENTS.md`] |
| PAYLOAD-04 | Payload construction avoids repeated JSON encode/decode cycles; producers pass structured dictionaries to logging. | SQL builds JSONB once; Python parses only if needed for driver output and passes a dict through `json_fields`. [VERIFIED: `01-CONTEXT.md`; CITED: Google Cloud Logging std-lib integration docs] |
</phase_requirements>

## Summary

Phase 1 should be planned as a narrow storage-layer producer change. The durable
event remains the existing `feed_audit_events` row, and the new behavior is one
best-effort structured log emitted from the storage boundary when audited SQL
returns a non-null `feed_audit_event` payload. [VERIFIED:
`.planning/PROJECT.md`, `.planning/ROADMAP.md`, `01-CONTEXT.md`]

The lowest-risk implementation is to extend
`backend/pipeline/storage/feed_audit_sql.py` with a reusable JSONB return
expression, update every audited async and sync SQL statement to expose one
nullable final column named `feed_audit_event`, and add one shared
`backend/pipeline/storage/feed_audit_notifications.py` helper that normalizes
the returned object and emits `extra={"json_fields": payload}`. [VERIFIED:
`feed_audit_sql.py`, `feed_queries.py`, `sync_feed_queries.py`,
`feed_store.py`, `sync_feed_store.py`; CITED: Google Cloud Logging std-lib
integration docs]

Do not plan any cloud routing, Pub/Sub publishing, Watch Duty calls, delivery
state, database polling, triggers, or lifecycle semantic changes in this
phase. Those are explicitly deferred to later phases or out of scope. [VERIFIED:
`01-CONTEXT.md`, `.planning/REQUIREMENTS.md`, `.planning/ROADMAP.md`]

**Primary recommendation:** Use a single SQL-returned `feed_audit_event` JSONB
payload plus one shared storage helper; emit after successful audited store
calls and swallow local logging failures. [VERIFIED: `01-CONTEXT.md`]

## Project Constraints (from AGENTS.md)

- Read and follow `.agents/instructions.md` before code changes or code review. [VERIFIED: `AGENTS.md`]
- Read Python and JS/TS style guides before code changes or review; Phase 1 is Python-only but the Python guide still applies. [VERIFIED: `.agents/instructions.md`, `.github/instructions/PYTHON_STYLE.instructions.md`]
- Default to targeted low-resource checks; do not run broad local E2E, API, component, Docker, testcontainers, or full integration-stack commands unless explicitly approved. [VERIFIED: `AGENTS.md`, `.agents/instructions.md`]
- For docs-only changes, use `git diff --check` instead of Python tests unless the user asks for tests. [VERIFIED: `AGENTS.md`]
- Prefer `safe-run -- <command>` for tests, builds, installs, browser/e2e runs, benchmarks, and other host-intensive commands. [VERIFIED: prompt AGENTS instructions; `safe-run -- uv --version` executed]
- Prefer the project's `mise` task runner for standard formatting, linting, and generation tasks. [VERIFIED: `.agents/instructions.md`, `.mise.toml`]
- Do not use `--no-verify` for commits; hooks are not to be bypassed. [VERIFIED: `.agents/instructions.md`]
- Python code uses absolute imports, `from __future__ import annotations` where modern annotations are used, 80-character formatting through Ruff, and `logger = logging.getLogger(__name__)`. [VERIFIED: `.github/instructions/PYTHON_STYLE.instructions.md`, `.planning/codebase/CONVENTIONS.md`]
- Catch-all `Exception` is acceptable only at an isolation point that records or suppresses safely; the notification helper is such an isolation point because AUDIT-03/D-11 require failure isolation. [VERIFIED: Python style guide, `01-CONTEXT.md`]
- Project-local skills directories `.codex/skills/` and `.agents/skills/` are absent; only `.agents/instructions.md` applies. [VERIFIED: `find radio-transcription/.codex/skills radio-transcription/.agents/skills ...`]

## Architectural Responsibility Map

| Capability | Primary Tier | Secondary Tier | Rationale |
|------------|--------------|----------------|-----------|
| Audit row insertion | Database / Storage | API / Backend | Existing SQL CTEs insert `feed_audit_events` in the same statement as feed mutations. [VERIFIED: `feed_queries.py`, `sync_feed_queries.py`] |
| Notification payload construction | Database / Storage | API / Backend | Locked decision D-08 requires DB-returned audit row values, and D-07 requires a JSONB result column. [VERIFIED: `01-CONTEXT.md`] |
| Structured log emission | API / Backend | Database / Storage | Python storage methods receive rows and emit standard logging records; SQL should not call logging or routing systems. [VERIFIED: `feed_store.py`, `sync_feed_store.py`, `log_helper.py`] |
| Failure isolation for emission | API / Backend | - | The shared helper owns local exception swallowing so callers keep existing return semantics. [VERIFIED: `01-CONTEXT.md`] |
| Cloud Logging sink routing | CDN / Static / Cloud Routing | - | Routing is Phase 2, not Phase 1. [VERIFIED: `.planning/ROADMAP.md`] |
| Watch Duty webhook delivery | API / Backend | - | Relay delivery is Phase 3, not Phase 1. [VERIFIED: `.planning/ROADMAP.md`] |

## Standard Stack

### Core

| Library / Tool | Version | Purpose | Why Standard |
|----------------|---------|---------|--------------|
| Python via `uv` | 3.13.2 in project environment | Backend storage and tests | Repo pins Python 3.13.2 and package bounds `>=3.13,<3.14`; `uv run` imported the active packages under Python 3.13.2. [VERIFIED: `.tool-versions`, `pyproject.toml`, `uv run python -c ...`] |
| `asyncpg` | 0.31.0 locked/imported | Async `FeedStore` database access | Existing async storage methods use asyncpg pool/connection methods. [VERIFIED: `uv.lock`, `uv run python -c ...`, `feed_store.py`] |
| `psycopg[binary]` | 3.3.3 locked/imported | Sync `SyncFeedStore` database access | Existing Echo sync store uses psycopg connection execution. [VERIFIED: `uv.lock`, `uv run python -c ...`, `sync_feed_store.py`] |
| `google-cloud-logging` | 3.15.0 locked/imported; docs latest observed 3.16.0 | Standard logging integration with Cloud Logging | Repo centralizes logging through `setup_logging()`, and official docs support JSON payloads via `extra={"json_fields": ...}`. [VERIFIED: `uv.lock`, `log_helper.py`; CITED: Google Cloud Logging docs] |
| Python `logging` | stdlib | Local and cloud structured log API | Existing repo uses module loggers and `json_fields` structured payloads in production code and tests. [VERIFIED: `log_helper.py`, `test_tracing_utils.py`, `test_quarantine_telemetry.py`] |
| PostgreSQL / AlloyDB SQL | existing schema | CTE mutation and JSONB payload construction | Existing audit writes use SQL CTEs and `jsonb_build_object` snapshots. [VERIFIED: `029_feed_audit_events.sql`, `feed_audit_sql.py`, `feed_queries.py`] |

### Supporting

| Library / Tool | Version | Purpose | When to Use |
|----------------|---------|---------|-------------|
| pytest | 9.0.3 locked/imported | Unit and contract tests | Use for focused storage/helper tests; avoid broad resource-heavy suites by default. [VERIFIED: `uv.lock`, `pyproject.toml`, `.agents/instructions.md`] |
| pytest-asyncio | 1.3.0 locked | Async store tests | Existing `unittest.IsolatedAsyncioTestCase` and pytest async mode cover async storage methods. [VERIFIED: `uv.lock`, `pyproject.toml`, `test_feed_store.py`] |
| pytest-xdist | 3.8.0 locked | Parallel unit test execution | Root pytest config uses `addopts="-n auto"`; use targeted commands to limit scope. [VERIFIED: `uv.lock`, `pyproject.toml`] |
| Ruff | 0.14.14 declared | Formatting and linting | Use if implementation changes Python code; docs-only research does not require it. [VERIFIED: `pyproject.toml`, `.mise.toml`] |
| `safe-run` | available | Host-stability wrapper | Prefix targeted tests/builds with `safe-run --` per repo instructions. [VERIFIED: `command -v safe-run`, `safe-run -- uv --version`] |

### Alternatives Considered

| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| SQL-returned `feed_audit_event` JSONB | Extra `SELECT` from `feed_audit_events` after write | Rejected by D-06/AUDIT-05 because it adds a database round trip and can drift from the single audited statement. [VERIFIED: `01-CONTEXT.md`, `.planning/REQUIREMENTS.md`] |
| Standard logging `json_fields` | Direct `cloud_logging.Client().logger(...).log_struct(...)` calls | Direct calls are supported by the library, but they add cloud-client coupling to storage and violate D-12's routing/client boundary. [CITED: Context7 `/googleapis/python-logging`; VERIFIED: `01-CONTEXT.md`] |
| Shared storage helper | Per-method payload/logging code | Rejected by AUDIT-04/D-10 because async and sync paths must share producer behavior. [VERIFIED: `01-CONTEXT.md`, `.planning/REQUIREMENTS.md`] |
| Phase 1 Pub/Sub publish | Cloud Logging route in Phase 2 | Pub/Sub clients in storage are out of scope and would couple feed writes to routing. [VERIFIED: `01-CONTEXT.md`, `.planning/ROADMAP.md`] |

**Installation:** No new dependency installation is required for Phase 1. Use
the existing uv workspace and lockfile. [VERIFIED: `pyproject.toml`, `uv.lock`]

```bash
cd radio-transcription
safe-run -- uv run python -m pytest backend/pipeline/storage/tests/test_feed_query_contracts.py -q
safe-run -- uv run python -m pytest backend/pipeline/storage/tests/test_feed_store.py -q
safe-run -- uv run python -m pytest backend/pipeline/storage/tests/test_sync_feed_store.py -q
```

**Version verification:** Package versions were verified from `uv.lock` and
local imports with `uv run python -c ...`; `google-cloud-logging` docs were
checked through Context7 and official Google docs. [VERIFIED: `uv.lock`; CITED:
Context7 `/googleapis/python-logging`, Google Cloud Logging docs]

## Architecture Patterns

### System Architecture Diagram

```text
Admin/BFF writes     VM collector writes      Echo sync writes
      |                    |                       |
      v                    v                       v
FeedStore async       FeedStore async         SyncFeedStore
      |                    |                       |
      +---------- audited SQL CTE statement -------+
                           |
                           v
             feeds mutation + feed_audit_events insert
                           |
                           v
             final row includes nullable feed_audit_event JSONB
                           |
                    feed_audit_notifications helper
                           |
                +----------+----------+
                |                     |
          json_fields log        no-op on NULL
                |
                v
     Cloud Logging ingestion only; no Phase 1 routing
```

The diagram reflects Phase 1 only: storage emits a structured log after the
audited statement returns; routing and delivery begin in later phases.
[VERIFIED: `01-CONTEXT.md`, `.planning/ROADMAP.md`]

### Recommended Project Structure

```text
backend/pipeline/storage/
├── feed_audit_sql.py                 # Add notification JSONB return builder.
├── feed_audit_notifications.py       # New shared normalize-and-emit helper.
├── feed_queries.py                   # Add nullable feed_audit_event to audited SELECTs.
├── sync_feed_queries.py              # Add nullable feed_audit_event to sync audited SELECTs.
├── feed_store.py                     # Call helper after row-returning audited writes.
├── sync_feed_store.py                # Fetch rows and call the same helper.
└── tests/
    ├── test_feed_audit_notifications.py
    ├── test_feed_query_contracts.py
    ├── test_feed_store.py
    └── test_sync_feed_store.py
```

This structure follows existing storage boundaries: SQL in query modules, row
behavior in stores, package-local tests under `backend/pipeline/storage/tests`.
[VERIFIED: `.planning/codebase/CONVENTIONS.md`, `.planning/codebase/TESTING.md`]

### Pattern 1: Shared JSONB Audit Return

**What:** Add a helper in `feed_audit_sql.py` that returns a SQL expression for
the flat notification object, and use it as `feed_audit_event` in
`write_audit RETURNING`. [VERIFIED: `feed_audit_sql.py`, `01-CONTEXT.md`]

**When to use:** Use it for every `insert_feed_audit_event_cte(...)` call in
async and sync audited lifecycle SQL. [VERIFIED: `feed_queries.py`,
`sync_feed_queries.py`]

**Example:**

```python
# Source: backend/pipeline/storage/feed_audit_sql.py and 01-CONTEXT.md
def feed_audit_event_payload_sql(alias: str = "feed_audit_events") -> str:
    """Return the v1 Feed Audit Notification JSONB payload expression."""
    return f"""jsonb_build_object(
        'event_type', 'radio_transcription.feed_audit_notification',
        'schema_version', 1,
        'event_id', {alias}.id,
        'action', {alias}.action,
        'occurred_at', {alias}.occurred_at,
        'actor_id', {alias}.actor_id,
        'feed_id', {alias}.feed_id,
        'feed_revision', {alias}.feed_revision,
        'before_values', {alias}.before_values,
        'after_values', {alias}.after_values
    )"""
```

### Pattern 2: Nullable Final Result Column

**What:** Join the `write_audit` CTE into each audited final `SELECT` and expose
one nullable `feed_audit_event` column to Python. [VERIFIED: `01-CONTEXT.md`,
`feed_queries.py`, `sync_feed_queries.py`]

**When to use:** Use `LEFT JOIN write_audit ON TRUE` for normal result rows so
suppressed paths return the existing row plus `NULL`. For `DELETE_FEED_SQL`,
allow the CTE to keep internal `feed_id` if child-delete CTEs still depend on
it, but expose only `feed_audit_event` from the final result. [VERIFIED:
`feed_queries.py`]

**Example:**

```sql
-- Source: backend/pipeline/storage/feed_queries.py pattern
SELECT after_row.*,
       write_audit.feed_audit_event
FROM after_row
LEFT JOIN write_audit ON TRUE;
```

### Pattern 3: Storage Helper as Failure Boundary

**What:** Centralize normalization and logging in one helper that no-ops on
`None`, parses only string JSON, emits `json_fields`, and catches local
exceptions. [VERIFIED: `01-CONTEXT.md`, `test_quarantine_telemetry.py`; CITED:
Google Cloud Logging docs]

**When to use:** Call after any audited storage row is returned and before
returning the public method result; do not call on methods that do not insert
audit rows. [VERIFIED: `feed_store.py`, `sync_feed_store.py`]

**Example:**

```python
# Source: 01-CONTEXT.md, Google Cloud Logging std-lib integration docs
import json
import logging
from collections.abc import Mapping
from typing import Any

logger = logging.getLogger(__name__)


def emit_feed_audit_notification(
    feed_audit_event: object | None,
) -> None:
    if feed_audit_event is None:
        return
    try:
        if isinstance(feed_audit_event, str):
            payload = json.loads(feed_audit_event)
        elif isinstance(feed_audit_event, Mapping):
            payload = dict(feed_audit_event)
        else:
            return
        logger.info(
            "Feed audit notification emitted",
            extra={"json_fields": payload},
        )
    except Exception:
        return
```

The planner should refine exact typing and optional diagnostic logging, but the
exception boundary must not re-raise. [VERIFIED: `01-CONTEXT.md`]

### Pattern 4: Sync Store Row Consumption

**What:** Convert `SyncFeedStore.record_heartbeat`, `record_failure`, and
`record_non_budgeted_failure` from discard-only `execute()` calls to
row-consuming calls that can inspect `feed_audit_event`. [VERIFIED:
`sync_feed_store.py`, `sync_feed_queries.py`]

**When to use:** Use only for the three sync audited methods; preserve public
return type `None`. [VERIFIED: `sync_feed_store.py`]

**Example:**

```python
# Source: backend/pipeline/storage/sync_feed_store.py existing connection style
with self._connect_db() as conn:
    row = conn.execute(sql, params).fetchone()
emit_feed_audit_notification(
    None if row is None else row.get("feed_audit_event")
)
```

### Anti-Patterns to Avoid

- **Building payloads from request or method parameters:** This can create
  phantom notifications that do not match the inserted audit row. Use the
  returned `write_audit` data. [VERIFIED: `01-CONTEXT.md`,
  `.planning/research/PITFALLS.md`]
- **Calling Pub/Sub, Watch Duty, or Cloud Logging client APIs from storage:**
  Those imports violate Phase 1 boundaries and D-12. [VERIFIED:
  `01-CONTEXT.md`]
- **Emitting failure summary logs and audit-shaped notification logs for the
  same storage event:** Remove only storage-layer duplicates once the new
  notification covers the event; keep runtime/admin telemetry. [VERIFIED:
  `01-CONTEXT.md`, `feed_store.py`, `sync_feed_store.py`]
- **Changing feed lifecycle return values to support logging:** Existing public
  store semantics must remain stable; the payload is an internal side effect.
  [VERIFIED: `.planning/ROADMAP.md`, `feed_store.py`, `sync_feed_store.py`]

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Structured log transport | Direct Cloud Logging client calls or custom JSON stdout formatting | Python standard logging with `extra={"json_fields": payload}` | Existing repo and Google docs support this path. [VERIFIED: `log_helper.py`; CITED: Google docs] |
| Payload allowlist | New ad hoc dict from request/local method args | Existing `audit_snapshot_sql()` and `feed_audit_events` inserted row | Prevents raw request bodies and keeps payload tied to DB truth. [VERIFIED: `feed_audit_sql.py`, `029_feed_audit_events.sql`] |
| Async/sync duplicate emitters | Separate store-specific logging blocks | One `feed_audit_notifications.py` helper | AUDIT-04/D-10 require one helper. [VERIFIED: `01-CONTEXT.md`] |
| Delivery state | Outbox table, polling cursor, retry table, or `LISTEN/NOTIFY` | Nothing in Phase 1; later phases use Cloud Logging/Pub/Sub/relay | Delivery state is explicitly out of scope. [VERIFIED: `.planning/REQUIREMENTS.md`, `01-CONTEXT.md`] |
| JSON encode/decode loop | `json.dumps` before logging, then sink/relay parse cycles | Pass a structured dict to `json_fields`; parse only if driver returns a string | PAYLOAD-04 and Google docs support structured JSON payloads. [VERIFIED: `.planning/REQUIREMENTS.md`; CITED: Google docs] |

**Key insight:** Phase 1 is a producer contract, not a delivery system. The
planner should optimize for correct audit-boundary emission and failure
isolation, not cloud routing completeness. [VERIFIED: `.planning/ROADMAP.md`,
`01-CONTEXT.md`]

## Common Pitfalls

### Pitfall 1: Emitting For State Changes Without Audit Rows

**What goes wrong:** No-op updates, repeated failure noise, or already
deactivated feeds produce notification logs even though no `feed_audit_events`
row was inserted. [VERIFIED: `01-CONTEXT.md`, `feed_queries.py`]

**Why it happens:** The final feed result row can exist even when `write_audit`
is empty. [VERIFIED: `UPDATE_FEED_SQL`, `DEACTIVATE_FEED_SQL`]

**How to avoid:** Make `emit_feed_audit_notification(None)` a no-op and drive
emission only from the nullable `feed_audit_event` column. [VERIFIED:
`01-CONTEXT.md`]

**Warning signs:** Tests assert method success but do not assert emitted log
count; SQL final `SELECT` cannot produce `NULL AS feed_audit_event` for
suppressed paths. [VERIFIED: `test_feed_store.py`, `test_sync_feed_store.py`]

### Pitfall 2: Sync Store Keeps Discarding Result Rows

**What goes wrong:** Echo sync audited writes insert audit rows but never emit
notifications because the sync store still calls `execute()` and ignores rows.
[VERIFIED: `sync_feed_store.py`, `sync_feed_queries.py`]

**Why it happens:** Existing sync methods have `None` public return values and
previously did not need query output. [VERIFIED: `sync_feed_store.py`]

**How to avoid:** Fetch one row from each sync audited SQL statement, call the
shared helper, and preserve public method return behavior. [VERIFIED:
`01-CONTEXT.md`, `sync_feed_store.py`]

**Warning signs:** Tests only check `conn.execute.assert_called_once()` and do
not verify `fetchone()` or helper invocation. [VERIFIED:
`test_sync_feed_store.py`]

### Pitfall 3: Breaking Delete/Reset/Deactivate Result Semantics

**What goes wrong:** Adding `feed_audit_event` accidentally hides
`blocked_active`, `deleted`, or `current_status` fields used by store logic.
[VERIFIED: `feed_store.py`, `feed_queries.py`]

**Why it happens:** Admin lifecycle SQL returns diagnostic result rows that are
not all normal feed rows. [VERIFIED: `DEACTIVATE_FEED_SQL`,
`DELETE_FEED_SQL`, `RESET_FEED_SQL`]

**How to avoid:** Add the nullable payload column without removing existing
diagnostic fields; emit before returning but after conflict/missing handling is
preserved. [VERIFIED: `feed_store.py`]

**Warning signs:** Active delete/reset tests fail, or `_row_to_feed()` receives
rows with missing feed columns. [VERIFIED: `test_feed_store.py`]

### Pitfall 4: Duplicate JSON Transformation

**What goes wrong:** SQL builds JSONB, Python serializes it to a string, and the
log handler or relay parses it again. [VERIFIED: `.planning/REQUIREMENTS.md`]

**Why it happens:** Existing feed store code already serializes tags for SQL,
which can make another `json.dumps` feel natural. [VERIFIED: `feed_store.py`]

**How to avoid:** Treat `feed_audit_event` as the payload object; only
`json.loads` if a driver returns a JSON string; pass a dict to `json_fields`.
[VERIFIED: `01-CONTEXT.md`; CITED: Google docs]

**Warning signs:** Helper accepts only `str`, or tests assert a JSON string in
`record.json_fields` instead of a dict. [VERIFIED: `test_quarantine_telemetry.py`]

### Pitfall 5: Storage Logging Failure Coupling

**What goes wrong:** A local logger/mock failure makes feed writes fail despite
the database mutation succeeding. [VERIFIED: `01-CONTEXT.md`]

**Why it happens:** Logging is usually treated as harmless, so exceptions are
not isolated. [VERIFIED: `test_quarantine_telemetry.py`]

**How to avoid:** Test a patched emitter logger that raises and verify store
methods still return their normal values. [VERIFIED: `01-CONTEXT.md`,
`test_quarantine_telemetry.py`]

**Warning signs:** The helper has no broad isolation boundary, or store tests
expect logging exceptions. [VERIFIED: `01-CONTEXT.md`]

## Code Examples

### SQL Return Builder

```python
# Source: backend/pipeline/storage/feed_audit_sql.py pattern
def feed_audit_event_returning_sql() -> str:
    return (
        "id, feed_id, "
        f"{feed_audit_event_payload_sql()} AS feed_audit_event"
    )
```

For most audited queries, only `feed_audit_event` needs to be consumed outside
the CTE. `DELETE_FEED_SQL` still needs internal `feed_id` for child-delete CTEs,
so the CTE may return both internally while the final result exposes the single
payload column. [VERIFIED: `feed_queries.py`, `01-CONTEXT.md`]

### Async Store Hook

```python
# Source: backend/pipeline/storage/feed_store.py existing method shape
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
feed_audit_notifications.emit_feed_audit_notification(
    row["feed_audit_event"]
)
return self._row_to_feed(row)
```

The planner should decide whether to emit before or after `_row_to_feed(row)`;
the important contract is that helper failures are swallowed and store return
semantics stay unchanged. [VERIFIED: `01-CONTEXT.md`, `feed_store.py`]

### Log Assertion

```python
# Source: backend/pipeline/ingestion/tests/test_quarantine_telemetry.py pattern
with self.assertLogs(
    "backend.pipeline.storage.feed_audit_notifications",
    level=logging.INFO,
) as cm:
    emit_feed_audit_notification(payload)

record = cm.records[0]
self.assertEqual(
    record.json_fields["event_type"],
    "radio_transcription.feed_audit_notification",
)
self.assertEqual(record.json_fields["schema_version"], 1)
```

Existing tests inspect `record.json_fields` when asserting structured log
payloads. [VERIFIED: `test_quarantine_telemetry.py`,
`test_tracing_utils.py`]

## State of the Art

| Old Approach | Current Approach | When Changed / Decided | Impact |
|--------------|------------------|------------------------|--------|
| Durable audit rows only, no notification producer | Add best-effort structured log for each newly inserted audit row | Phase 1 context on 2026-06-26 | Storage now exposes committed audit payloads to logging without delivery state. [VERIFIED: `01-CONTEXT.md`, `.planning/STATE.md`] |
| Multiple scalar `audit_*` columns considered | One nullable JSONB column `feed_audit_event` | D-07 in Phase 1 context | Avoids namespace collisions with `feeds.audit_revision` and future audit-prefixed feed columns. [VERIFIED: `01-CONTEXT.md`] |
| Alert notification package handles evaluated transcript alerts | Feed audit notification helper under storage only | Phase 1 boundary | Avoids segment-centric alert semantics in audit events. [VERIFIED: `.planning/research/PITFALLS.md`, `backend/pipeline/notification/`] |
| Custom delivery/outbox ideas | Deferred Cloud Logging/Pub/Sub/relay in later phases | Roadmap 2026-06-26 | Phase 1 stays focused on contract and emission. [VERIFIED: `.planning/ROADMAP.md`] |

**Deprecated/outdated for this phase:**
- `event_type="feed_audit_event"` and `schema_version="feed_audit_event.v1"` from earlier milestone-level stack research are superseded by locked Phase 1 decisions. [VERIFIED: `.planning/research/STACK.md`, `01-CONTEXT.md`]
- Direct Pub/Sub publishing from storage is out of scope for Phase 1. [VERIFIED: `01-CONTEXT.md`]
- Database polling, triggers, CDC, `LISTEN/NOTIFY`, outbox payload tables, and direct Watch Duty calls are out of scope for v1. [VERIFIED: `.planning/REQUIREMENTS.md`]

## Environment Availability

| Dependency | Required By | Available | Version | Fallback |
|------------|-------------|-----------|---------|----------|
| `uv` | Running project Python and targeted pytest | yes | Host `uv 0.11.2`; project pin `0.9.28` | Use `mise` if toolchain bootstrap is needed. [VERIFIED: `uv --version`, `.tool-versions`] |
| Python project runtime | Storage tests and implementation | yes through `uv run` | 3.13.2 | Do not use host `python3` 3.12.13 for repo tests. [VERIFIED: `uv run python -c ...`, `python3 --version`] |
| `safe-run` | Host-stable test execution | yes | command available | Run very narrow commands directly only if `safe-run` is unavailable. [VERIFIED: `command -v safe-run`, `safe-run -- uv --version`] |
| pytest stack | Targeted unit/contract tests | yes | pytest 9.0.3, pytest-asyncio 1.3.0, pytest-xdist 3.8.0 | None needed for Phase 1. [VERIFIED: `uv.lock`, `uv run python -c ...`] |
| Knowledge graph | Graph context discovery | no | graphify disabled; graph file absent | Use direct codebase grep and planning docs. [VERIFIED: `graphify status`, `ls .planning/graphs/graph.json`] |

**Missing dependencies with no fallback:** None for Phase 1 planning. [VERIFIED:
environment probes above]

**Missing dependencies with fallback:** Knowledge graph is disabled; direct
source inspection was used instead. [VERIFIED: graphify status]

## Security Domain

Security enforcement is considered enabled because
`workflow.security_enforcement` is absent rather than explicitly `false` in
`.planning/config.json`. [VERIFIED: `.planning/config.json`; VERIFIED: GSD
research instructions]

### Applicable ASVS Categories

| ASVS Category | Applies | Standard Control |
|---------------|---------|------------------|
| V2 Authentication | no new auth in Phase 1 | Preserve committed `actor_id` from `feed_audit_events`; do not trust new caller fields. [VERIFIED: `029_feed_audit_events.sql`, `01-CONTEXT.md`] |
| V3 Session Management | no | Phase 1 does not touch browser or service sessions. [VERIFIED: `.planning/ROADMAP.md`] |
| V4 Access Control | indirectly | Do not widen storage paths or expose new APIs; feed services keep existing actor propagation controls. [VERIFIED: `.planning/codebase/CONCERNS.md`] |
| V5 Input Validation | yes | Use SQL snapshot allowlist and DB constraints; no raw request bodies or secrets in payload. [VERIFIED: `feed_audit_sql.py`, `029_feed_audit_events.sql`, `.planning/REQUIREMENTS.md`] |
| V6 Cryptography | no | No signing, keys, or webhook auth in Phase 1; those are relay/delivery phases. [VERIFIED: `.planning/ROADMAP.md`] |

### Known Threat Patterns for This Stack

| Pattern | STRIDE | Standard Mitigation |
|---------|--------|---------------------|
| Spoofed notification actor fields | Spoofing | Build payload from inserted audit row `actor_id`, not request-local or caller-supplied notification fields. [VERIFIED: `01-CONTEXT.md`, `029_feed_audit_events.sql`] |
| Sensitive data leakage through logs | Information Disclosure | Reuse `AUDITED_FEED_STATE_FIELDS`; do not include raw request bodies, secrets, credentials, or webhook-only enrichment. [VERIFIED: `feed_audit_sql.py`, `.planning/REQUIREMENTS.md`] |
| Feed write denial through logging failure | Denial of Service | Swallow local notification helper failures and never re-raise to store callers. [VERIFIED: `01-CONTEXT.md`] |
| Duplicate or phantom event generation | Repudiation / Integrity | Emit only from non-null DB-returned `feed_audit_event` values. [VERIFIED: `01-CONTEXT.md`] |

## Assumptions Log

All claims in this research were verified against local files, command output,
Context7, or official Google documentation. No `[ASSUMED]` claims are intended.

| # | Claim | Section | Risk if Wrong |
|---|-------|---------|---------------|
| - | None | - | - |

## Open Questions (RESOLVED)

1. **Should helper failures produce a fallback diagnostic log?**
   - What we know: D-11 requires swallowing local emission exceptions. [VERIFIED: `01-CONTEXT.md`]
   - RESOLVED: The Phase 1 helper should be a minimal no-raise helper. It may attempt one best-effort diagnostic log inside a nested suppression block, but fallback diagnostics are not required and must never be observable by callers. Tests should prove emitter exceptions are swallowed. [VERIFIED: Python style guide catch-at-isolation guidance, `01-CONTEXT.md` D-11]

2. **Should store methods emit before or after row conversion?**
   - What we know: The payload is independent of `_row_to_feed()` and should not change return semantics. [VERIFIED: `feed_store.py`, `01-CONTEXT.md`]
   - RESOLVED: Store methods should emit after method-specific missing/conflict checks and after required row validation for normal feed-returning methods. For methods that return only booleans or `None`, emit after the row has been fetched and any blocked-state logic has been applied. Preserve existing public return semantics, and keep helper failures swallowed. [VERIFIED: `feed_store.py`, `sync_feed_store.py`, `01-CONTEXT.md`]

## Sources

### Primary (HIGH Confidence)

- `.planning/phases/01-audit-contract-and-emission/01-CONTEXT.md` - locked Phase 1 boundary, payload, SQL return shape, helper, and verification decisions.
- `.planning/REQUIREMENTS.md` - AUDIT-01..05 and PAYLOAD-01..04.
- `.planning/ROADMAP.md` - Phase 1 goal, success criteria, and later phase boundaries.
- `.planning/PROJECT.md` - project constraints and v1 non-critical-path scope.
- `backend/pipeline/storage/feed_audit_sql.py` - audit snapshot allowlist and audit insert CTE helper.
- `backend/pipeline/storage/feed_queries.py` - async audited mutation SQL.
- `backend/pipeline/storage/sync_feed_queries.py` - sync audited mutation SQL.
- `backend/pipeline/storage/feed_store.py` - async store integration points and return semantics.
- `backend/pipeline/storage/sync_feed_store.py` - sync store integration points and current row-discard behavior.
- `backend/pipeline/common/log_helper.py` - standard structured logging setup.
- `backend/pipeline/storage/tests/test_feed_query_contracts.py`, `test_feed_store.py`, `test_sync_feed_store.py` - current storage test patterns.
- `terraform/modules/alloydb/sql/ingestion/029_feed_audit_events.sql` - audit table schema, actions, constraints, and indexes.
- `AGENTS.md`, `.agents/instructions.md`, `.github/instructions/PYTHON_STYLE.instructions.md` - project directives and local test safety.

### Secondary (MEDIUM Confidence)

- Context7 `/googleapis/python-logging` - confirmed the Python client supports structured log entries and standard logging integration.
- Google Cloud Logging Python stdlib integration docs - `https://docs.cloud.google.com/python/docs/reference/logging/latest/std-lib-integration`; confirms `extra={"json_fields": data_dict}` writes JSON payloads and page was last updated 2026-06-03.
- Google Cloud Logging changelog - `https://docs.cloud.google.com/python/docs/reference/logging/latest/changelog`; confirms latest observed docs version 3.16.0 and `json_fields` support history.

### Tertiary (LOW Confidence)

- None used.

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH - versions and tools were verified from `uv.lock`,
  `.tool-versions`, and local imports.
- Architecture: HIGH - Phase 1 boundaries and code seams are directly visible in
  local storage SQL/store modules.
- Pitfalls: HIGH - pitfalls are tied to concrete current code paths and locked
  context decisions.
- Driver JSON normalization: MEDIUM - planner should require tolerant helper
  behavior instead of relying on one driver return type.

**Research date:** 2026-06-26
**Valid until:** 2026-07-26 for local storage patterns; re-check Google Cloud
Logging docs before changing logging API assumptions.

**Validation Architecture:** Omitted because
`workflow.nyquist_validation=false` in `.planning/config.json`. [VERIFIED:
`.planning/config.json`]
