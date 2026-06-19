# Phase 2: Transactional Storage Writes - Research

**Researched:** 2026-06-19
**Domain:** async Python storage writes, PostgreSQL/AlloyDB transactions, feed audit event persistence
**Confidence:** HIGH

<user_constraints>
## User Constraints (from CONTEXT.md)

Copied verbatim from `.planning/phases/02-transactional-storage-writes/02-CONTEXT.md`. [VERIFIED: .planning/phases/02-transactional-storage-writes/02-CONTEXT.md]

### Locked Decisions

## Implementation Decisions

### Storage Method Contract

- **D-01:** Audited `FeedStore` mutation methods require an explicit
  `actor_id`. Do not make `actor_id` optional in storage.
- **D-02:** Use the existing mutation methods as the audited paths for Phase 2
  (`create_feed`, `update_feed`, `deactivate_feed`, `reset_feed`,
  `delete_feed`). Do not add parallel `*_with_audit` variants that could drift
  from unaudited versions.
- **D-03:** `FeedStore` owns Feed Audit Event creation. Service and runtime
  callers pass causal inputs such as `actor_id`, but they must not build or
  insert audit rows directly.
- **D-04:** A current-state mutation and its audit row must commit or roll back
  as one database transaction. Existing pool-level one-shot calls can be
  refactored to explicit connection transactions where needed.

### Meaningful Update Detection

- **D-05:** `update_feed` must suppress `feed.updated` when no meaningful
  allowlisted value changes.
- **D-06:** A no-op update still returns the current feed normally. It must not
  return a falsey "not found" style result or force an API behavior change.
- **D-07:** For Phase 2, "meaningful update" means a change to the values this
  storage method controls and the audit allowlist tracks, currently feed name
  and tags. Compare normalized stored values, not raw request text.

### Snapshot Granularity

- **D-08:** Use full maintained allowlisted snapshots for all Phase 2 audited
  events, not changed-field-only payloads.
- **D-09:** `feed.created` uses `before_values = {}` and `after_values` as the
  full allowlisted snapshot after creation.
- **D-10:** `feed.updated`, `feed.deactivated`, and `feed.reset` use full
  allowlisted snapshots in both `before_values` and `after_values`.
- **D-11:** `feed.deleted` uses the full allowlisted snapshot in
  `before_values` and `after_values = {}`. This snapshot is the self-contained
  deleted-feed record and must be captured before the current row and cascading
  `feed_properties` row are removed.
- **D-12:** The maintained snapshot allowlist should follow the Phase 1
  contract: meaningful feed row fields plus `feed_properties.source_feed_id`
  and `feed_properties.tags`, excluding noisy worker/heartbeat lease fields by
  default.
- **D-13:** Full snapshots are still allowlisted domain snapshots. They are not
  raw unrestricted row dumps, and they must not introduce secrets or high-noise
  scheduler fields.

### Actor Vocabulary And Fallbacks

- **D-14:** Until Phase 3 wires trusted admin identity from the BFF/service
  boundary, feeds-service API mutations pass `service:feeds-service` as the
  required `actor_id`.
- **D-15:** Do not use `user:null`, `user:`, empty suffixes, or other fake user
  actors. If a trusted human `sub` exists, use `user:google:<sub>`. If only a
  trusted email exists, use `user-email:<normalized_email>`. If neither exists,
  use `unknown:unknown` only as an explicit rare fallback or reject the admin
  mutation in the later service-boundary phase.
- **D-16:** Remove the `system:` actor prefix from the v1 contract before any
  audit rows are emitted. It overlaps with clearer categories and is likely to
  become vague. Keep `service:`, `job:`, `gcp-sa:`, `user:google:`,
  `user-email:`, and `unknown:unknown`.
- **D-17:** `gcp-sa:<service_account_email>` remains reserved for cases where
  the authenticated GCP workload principal is the only known origin and no
  semantic service/job actor is available.

### Per-Feed Ordering And Transactionality

- **D-18:** Allocate `feed_sequence` inside the same database transaction as
  the audited mutation and audit insert.
- **D-19:** Use `feed_audit_event_sequences` as the sequence allocator with
  row locking or atomic upsert/update semantics.
- **D-20:** Do not compute the next sequence from
  `MAX(feed_sequence) + 1`; that shape is race-prone under concurrent
  mutations.

### Verification Expectations

- **D-21:** Phase 2 tests must prove the storage methods write the expected
  audit action, actor, sequence, and full before/after snapshots for create,
  update, deactivate, reset, and delete.
- **D-22:** Tests must prove no audit row is left behind when the state
  mutation fails or rolls back.
- **D-23:** Tests must prove no-op `update_feed` returns the feed normally and
  suppresses `feed.updated`.

### the agent's Discretion

The user delegated exact helper names, SQL layout, and practical test split to
the agent as long as the decisions above are preserved. Prefer the existing
storage style and keep the implementation localized to `FeedStore`,
`feed_queries`, focused tests, and contract docs/schema cleanup for the actor
vocabulary.

### Deferred Ideas (OUT OF SCOPE)

- Trusted admin actor forwarding from BFF to feeds service belongs to Phase 3.
- Runtime failure, quarantine, recovery, Echo/sync coverage, and no-lease-churn
  behavior belong to Phase 4.
- Retention enforcement and final database-level verification hardening belong
  to Phase 5.
- Admin timeline read APIs, UI, and Watch Duty backend webhook delivery remain
  out of scope for v1.
</user_constraints>

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|------------------|
| AUD-04 | Audit history preserves the meaningful values before and after each audited change. [VERIFIED: .planning/REQUIREMENTS.md] | Use full allowlisted snapshots in `before_values` and `after_values`; do not use raw row dumps or changed-field-only payloads. [VERIFIED: 02-CONTEXT.md; VERIFIED: documentation/feed-audit-events.md] |
| EVT-01 | Feed creation emits one audit event. [VERIFIED: .planning/REQUIREMENTS.md] | Extend existing `FeedStore.create_feed` to require `actor_id`, run inside an asyncpg transaction, and insert `feed.created` after the feed/properties row exists but before commit. [VERIFIED: backend/pipeline/storage/feed_store.py; CITED: https://magicstack.github.io/asyncpg/current/usage.html] |
| EVT-02 | Meaningful feed configuration changes emit audit events. [VERIFIED: .planning/REQUIREMENTS.md] | Compare normalized stored `name` and `tags`; emit `feed.updated` only when those allowlisted values change. [VERIFIED: 02-CONTEXT.md; VERIFIED: backend/pipeline/storage/feed_queries.py] |
| EVT-03 | Feed deactivation emits one audit event. [VERIFIED: .planning/REQUIREMENTS.md] | Extend `deactivate_feed` to lock/read the current row, update status, and insert `feed.deactivated` in the same transaction. [VERIFIED: backend/pipeline/storage/feed_store.py; CITED: https://www.postgresql.org/docs/15/explicit-locking.html] |
| EVT-04 | Feed reset emits one audit event. [VERIFIED: .planning/REQUIREMENTS.md] | Extend `reset_feed` to capture before/after snapshots around `RESET_FEED_SQL` and insert `feed.reset`. [VERIFIED: backend/pipeline/storage/feed_queries.py; VERIFIED: 02-CONTEXT.md] |
| EVT-05 | Feed deletion emits one audit event before the feed is removed from current-state storage. [VERIFIED: .planning/REQUIREMENTS.md] | Lock/read the feed and `feed_properties` snapshot first, insert `feed.deleted`, then execute the hard delete inside the same transaction. [VERIFIED: 02-CONTEXT.md; VERIFIED: terraform/modules/alloydb/sql/ingestion/012_feed_properties.sql] |
| CON-01 | A successful audited feed mutation and its audit event commit together. [VERIFIED: .planning/REQUIREMENTS.md] | Use `async with pool.acquire()` plus `async with connection.transaction()`; asyncpg commits on successful exit. [CITED: https://magicstack.github.io/asyncpg/current/usage.html] |
| CON-02 | A failed or rolled-back feed mutation does not leave behind an audit event. [VERIFIED: .planning/REQUIREMENTS.md] | Keep sequence allocation, state mutation, and audit insert inside one transaction; asyncpg rolls back the block on exceptions. [CITED: https://magicstack.github.io/asyncpg/current/usage.html] |
| CON-03 | Concurrent audited mutations for the same feed preserve a unique, deterministic per-feed order. [VERIFIED: .planning/REQUIREMENTS.md] | Lock the current feed row for existing-feed mutations and allocate `feed_sequence` through `feed_audit_event_sequences` using row locking or atomic upsert/update, backed by `UNIQUE (feed_id, feed_sequence)`. [VERIFIED: terraform/modules/alloydb/sql/ingestion/029_feed_audit_events.sql; CITED: https://www.postgresql.org/docs/15/sql-insert.html] |
| CON-04 | Audit event creation is owned by backend storage boundaries so service and runtime callers cannot accidentally create state/history drift. [VERIFIED: .planning/REQUIREMENTS.md] | Do not expose audit insert helpers above `FeedStore`; service callers pass only `actor_id`, with `service:feeds-service` fallback in Phase 2. [VERIFIED: 02-CONTEXT.md; VERIFIED: backend/services/feeds/service.py] |
</phase_requirements>

## Project Constraints (from AGENTS.md)

- Read `.agents/instructions.md` before code changes or code review; this research read it and only writes planning documentation. [VERIFIED: AGENTS.md; VERIFIED: .agents/instructions.md]
- Read Python and JS/TS style guides before code changes or code review; this research read both style guides for planner context. [VERIFIED: .agents/instructions.md; VERIFIED: .github/instructions/PYTHON_STYLE.instructions.md; VERIFIED: .github/instructions/JS_TS_STYLE.instructions.md]
- Default to targeted low-resource checks locally and avoid broad Docker, component, API, E2E, or unscoped pytest lanes unless explicitly approved. [VERIFIED: AGENTS.md; VERIFIED: .agents/instructions.md]
- For docs-only changes, use `git diff --check` instead of Python tests unless the user asks for tests. [VERIFIED: AGENTS.md; VERIFIED: .agents/instructions.md]
- Prefer `mise` for standard formatting, linting, generation, and tests, while respecting the local test safety rules. [VERIFIED: .agents/instructions.md]
- Do not bypass local pre-commit hooks with `--no-verify` when committing. [VERIFIED: .agents/instructions.md]
- Use `ctx7` for current library, framework, SDK, API, CLI, and cloud-service docs; this research used Context7 for asyncpg transaction documentation. [VERIFIED: prompt AGENTS.md; VERIFIED: Context7 CLI output]
- Use `safe-run -- <command>` for agent-run tests, builds, installs, browser/e2e runs, benchmarks, stress tests, and other commands that may consume substantial resources. [VERIFIED: prompt AGENTS.md]
- No project-defined skills were found under `.codex/skills` or `.agents/skills`. [VERIFIED: project skills discovery command]
- The project graph is unavailable because graphify is disabled, so no graph-derived relationships are used. [VERIFIED: graphify status command]
- Nyquist validation is disabled because `.planning/config.json` sets `workflow.nyquist_validation` to `false`; this research omits the Validation Architecture section. [VERIFIED: .planning/config.json]
- Security enforcement is enabled because `.planning/config.json` does not set `security_enforcement: false`; this research includes a Security Domain section. [VERIFIED: .planning/config.json]

## Summary

Phase 2 should refactor the existing `FeedStore` admin lifecycle mutation methods into storage-owned audited transactions rather than adding parallel audited methods. [VERIFIED: 02-CONTEXT.md; VERIFIED: backend/pipeline/storage/feed_store.py] The implementation hinge is replacing pool-level one-shot calls on `create_feed`, `update_feed`, `deactivate_feed`, `delete_feed`, and `reset_feed` with explicit asyncpg connection transactions, so the current-state mutation, sequence allocation, and `feed_audit_events` insert share one commit boundary. [VERIFIED: backend/pipeline/storage/feed_store.py; CITED: https://magicstack.github.io/asyncpg/current/usage.html]

The planner must schedule one required contract cleanup before real audit writes: remove the `system:` actor namespace from documentation, SQL constraints, and Phase 1 contract tests because Phase 2 decisions supersede the earlier actor vocabulary. [VERIFIED: 02-CONTEXT.md; VERIFIED: documentation/feed-audit-events.md; VERIFIED: terraform/modules/alloydb/sql/ingestion/029_feed_audit_events.sql; VERIFIED: backend/pipeline/storage/tests/test_feed_audit_contract.py] This is not optional because `actor_id` is constrained at the database layer and Phase 2 will start inserting real rows. [VERIFIED: terraform/modules/alloydb/sql/ingestion/029_feed_audit_events.sql; VERIFIED: 02-CONTEXT.md]

The safest design is a small storage-local audit helper layer: one snapshot selector/helper, one sequence allocator query, one audit insert query, and thin transaction wrappers inside the existing mutation methods. [VERIFIED: 02-CONTEXT.md; VERIFIED: backend/pipeline/storage/feed_queries.py] Mocks can prove method wiring and no-op behavior, but rollback and concurrent ordering require a focused storage integration test against the existing AlloyDB Omni/Testcontainers fixture or CI, because unit mocks cannot prove database transaction semantics. [VERIFIED: integration_tests/conftest.py; VERIFIED: .agents/instructions.md]

**Primary recommendation:** implement storage-owned audited transactions in `FeedStore`, use atomic `feed_audit_event_sequences` upsert/update for per-feed sequence allocation, build full allowlisted snapshots from audit-specific row projections, pass `service:feeds-service` from the feeds service for Phase 2, remove `system:` from the v1 actor contract, and add focused storage tests for event payloads, rollback, no-op update suppression, and concurrent ordering. [VERIFIED: 02-CONTEXT.md; VERIFIED: backend/services/feeds/service.py; CITED: https://www.postgresql.org/docs/15/sql-insert.html]

## Architectural Responsibility Map

| Capability | Primary Tier | Secondary Tier | Rationale |
|------------|--------------|----------------|-----------|
| Audited feed lifecycle writes | Database / Storage | API / Backend | `FeedStore` already owns feed lifecycle SQL, and Phase 2 locks audit creation to this boundary. [VERIFIED: backend/pipeline/storage/feed_store.py; VERIFIED: 02-CONTEXT.md] |
| Actor fallback for Phase 2 admin/service mutations | API / Backend | Database / Storage | Feeds service should pass `service:feeds-service` until Phase 3 forwards trusted human identity; storage requires but does not derive `actor_id`. [VERIFIED: 02-CONTEXT.md; VERIFIED: backend/services/feeds/service.py] |
| Per-feed audit ordering | Database / Storage | API / Backend | `feed_audit_event_sequences` and `UNIQUE (feed_id, feed_sequence)` are database-owned ordering primitives. [VERIFIED: terraform/modules/alloydb/sql/ingestion/029_feed_audit_events.sql] |
| Full allowlisted before/after snapshots | Database / Storage | API / Backend | Snapshot capture must happen at the storage boundary while the row is locked and before hard delete removes current state. [VERIFIED: 02-CONTEXT.md; VERIFIED: backend/pipeline/storage/feed_queries.py] |
| No-op update suppression | Database / Storage | API / Backend | Only storage can compare normalized stored values and decide whether to insert `feed.updated` without changing API behavior. [VERIFIED: 02-CONTEXT.md; VERIFIED: backend/pipeline/storage/feed_store.py] |
| Runtime failure/quarantine/recovery audit events | API / Backend | Database / Storage | Runtime events are explicitly deferred to Phase 4 and should not be planned in Phase 2. [VERIFIED: 02-CONTEXT.md; VERIFIED: .planning/ROADMAP.md] |
| Admin timeline/read APIs/UI | Frontend Server / Browser | API / Backend | Timeline and UI work are out of scope for v1/Phase 2. [VERIFIED: 02-CONTEXT.md; VERIFIED: .planning/ROADMAP.md] |

## Standard Stack

### Core

| Library / Tool | Version | Purpose | Why Standard |
|----------------|---------|---------|--------------|
| PostgreSQL-compatible AlloyDB | Production AlloyDB; integration fixture uses AlloyDB Omni 15. [VERIFIED: integration_tests/conftest.py] | Durable current-state rows, audit table, JSONB snapshots, row locks, and sequence allocator. [VERIFIED: terraform/modules/alloydb/sql/ingestion/029_feed_audit_events.sql; CITED: https://www.postgresql.org/docs/15/explicit-locking.html] | The repo already stores feeds in AlloyDB/PostgreSQL SQL tables and applies ordered SQL migrations. [VERIFIED: terraform/modules/alloydb/sql/ingestion; VERIFIED: terraform/modules/alloydb/main.tf] |
| `asyncpg` | Locked `0.31.0`; lower bound `>=0.29.0`. [VERIFIED: uv.lock; VERIFIED: pyproject.toml] | Async pool, explicit connection transactions, fetch/execute APIs for `FeedStore`. [VERIFIED: backend/pipeline/storage/feed_store.py; CITED: https://magicstack.github.io/asyncpg/current/usage.html] | Existing async feed storage uses asyncpg pool methods and `$1` parameterized SQL. [VERIFIED: backend/pipeline/storage/feed_store.py; VERIFIED: backend/pipeline/storage/feed_queries.py] |
| Python via `uv` | Project requires `>=3.13,<3.14`; `uv run python --version` returned Python 3.13.12. [VERIFIED: pyproject.toml; VERIFIED: environment probe] | Storage implementation and tests. [VERIFIED: backend/pipeline/storage/feed_store.py] | The backend/storage workspace is Python. [VERIFIED: pyproject.toml; VERIFIED: backend/pipeline/storage/feed_store.py] |
| Ordered ingestion SQL migrations | Current audit schema foundation is `029_feed_audit_events.sql`. [VERIFIED: terraform/modules/alloydb/sql/ingestion/029_feed_audit_events.sql] | Actor constraint cleanup if changing accepted actor namespaces. [VERIFIED: 02-CONTEXT.md; VERIFIED: terraform/modules/alloydb/sql/ingestion/029_feed_audit_events.sql] | Terraform and test schema helpers apply ingestion SQL files in sorted filename order. [VERIFIED: terraform/modules/alloydb/main.tf; VERIFIED: backend/pipeline/common/test_schema_helper.py] |
| pytest | Locked `9.0.3`; lower bound `>=9.0.2`. [VERIFIED: uv.lock; VERIFIED: pyproject.toml] | Focused storage/service tests for Phase 2. [VERIFIED: backend/pipeline/storage/tests/test_feed_store.py; VERIFIED: integration_tests/storage/test_feed_store_integration.py] | Existing backend tests use pytest plus unittest-style async test classes. [VERIFIED: .planning/codebase/TESTING.md] |

### Supporting

| Library / Tool | Version | Purpose | When to Use |
|----------------|---------|---------|-------------|
| `testcontainers[postgres]` | Locked `4.14.2`. [VERIFIED: uv.lock] | Focused integration coverage for rollback and concurrent sequence behavior. [VERIFIED: integration_tests/conftest.py] | Use in CI or with explicit local approval because Docker/testcontainers are resource-heavy. [VERIFIED: AGENTS.md; VERIFIED: .agents/instructions.md] |
| Docker | `docker info` returned server version `29.5.2`. [VERIFIED: environment probe] | Required by existing AlloyDB Omni/Testcontainers integration fixture. [VERIFIED: integration_tests/conftest.py] | Use only for targeted storage integration validation when approved or in CI. [VERIFIED: .agents/instructions.md] |
| `safe-run` | Available at `/home/shuojing/.local/bin/safe-run`. [VERIFIED: environment probe] | Host-stability wrapper for test/build commands. [VERIFIED: prompt AGENTS.md] | Prefix pytest/integration commands scheduled by agents. [VERIFIED: prompt AGENTS.md] |
| Pydantic | Locked `2.13.3`; lower bound `>=2.10.6`. [VERIFIED: uv.lock; VERIFIED: pyproject.toml] | Existing feeds API models remain the service response surface. [VERIFIED: backend/services/feeds/models.py] | Phase 2 should avoid taking over Phase 3 response compatibility unless unavoidable. [VERIFIED: .planning/ROADMAP.md; VERIFIED: 02-CONTEXT.md] |

### Alternatives Considered

| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| Existing `FeedStore` methods with required `actor_id` | Parallel `*_with_audit` methods | Rejected by locked decision D-02 because parallel paths can drift. [VERIFIED: 02-CONTEXT.md] |
| asyncpg explicit transactions | Pool-level one-shot calls | Pool-level calls autocommit each operation and cannot guarantee state plus audit rollback together. [VERIFIED: backend/pipeline/storage/feed_store.py; CITED: https://magicstack.github.io/asyncpg/current/usage.html] |
| `feed_audit_event_sequences` upsert/update | `MAX(feed_sequence) + 1` from audit rows | Rejected by D-20 and race-prone under concurrent writers. [VERIFIED: 02-CONTEXT.md] |
| Full allowlisted snapshots | Changed-field-only payloads | Rejected by D-08; full allowlisted snapshots are required for all Phase 2 events. [VERIFIED: 02-CONTEXT.md] |
| Storage-owned audit insert | Service-built audit rows | Rejected by D-03 and CON-04 because callers could create state/history drift. [VERIFIED: 02-CONTEXT.md; VERIFIED: .planning/REQUIREMENTS.md] |

**Installation:**

No new dependency installation is recommended for Phase 2. [VERIFIED: pyproject.toml; VERIFIED: uv.lock]

```bash
# Focused docs-only check for this research artifact.
git diff --check

# Focused unit checks planner should schedule after implementation.
safe-run -- uv run python -m pytest backend/pipeline/storage/tests/test_feed_store.py backend/pipeline/storage/tests/test_feed_audit_contract.py backend/services/feeds/tests/test_api.py -q

# Approval-gated or CI storage integration check for transaction/concurrency behavior.
safe-run -- uv run python -m pytest integration_tests/storage/test_feed_store_integration.py -q -n 0
```

**Version verification:** Python package versions were verified from `uv.lock`; current asyncpg transaction syntax was verified through Context7 `/websites/magicstack_github_io_asyncpg_current`; PostgreSQL SQL features were verified against official PostgreSQL 15 docs because the integration fixture uses AlloyDB Omni 15. [VERIFIED: uv.lock; VERIFIED: Context7 CLI output; VERIFIED: integration_tests/conftest.py; CITED: https://www.postgresql.org/docs/15/sql-insert.html]

## Architecture Patterns

### System Architecture Diagram

```text
Feeds API route / service method
    |
    | passes Phase 2 actor_id = "service:feeds-service"
    v
Existing FeedStore mutation method (required actor_id)
    |
    v
Acquire asyncpg connection
    |
    v
Open connection.transaction()
    |
    +--> existing-feed mutations: lock current feed + feed_properties row
    |        |
    |        +--> build before snapshot
    |        +--> suppress no-op update if normalized name/tags unchanged
    |
    +--> run current-state mutation SQL
    |        |
    |        +--> create/update/deactivate/reset return after state
    |        +--> delete: audit insert occurs before hard delete
    |
    +--> allocate feed_sequence from feed_audit_event_sequences
    |
    +--> insert feed_audit_events row with action, actor_id, sequence,
    |    full before_values, full after_values, and event identity columns
    |
    v
Transaction commit
    |
    v
Return existing FeedStore result shape to service/API

Exception anywhere in transaction
    |
    v
asyncpg transaction rollback: no partial feed mutation or audit row
```

This flow preserves existing FastAPI/service boundaries while moving transactionality into storage. [VERIFIED: backend/services/feeds/main.py; VERIFIED: backend/services/feeds/service.py; VERIFIED: backend/pipeline/storage/feed_store.py; CITED: https://magicstack.github.io/asyncpg/current/usage.html]

### Recommended Project Structure

```text
backend/pipeline/storage/
|-- feed_store.py             # Existing mutation methods, transaction orchestration, snapshot helper calls
|-- feed_queries.py           # Audit snapshot/select, sequence allocation, and audit insert SQL
`-- tests/
    |-- test_feed_store.py    # Focused unit tests and transaction mock extension
    `-- test_feed_audit_contract.py  # Actor vocabulary cleanup tests

backend/services/feeds/
|-- service.py                # Pass service:feeds-service actor_id in Phase 2
`-- tests/test_api.py         # Preserve route/service behavior

integration_tests/storage/
`-- test_feed_store_integration.py  # Focused rollback/concurrency checks, approval-gated locally

documentation/
`-- feed-audit-events.md      # Remove system: actor namespace before writes

terraform/modules/alloydb/sql/ingestion/
`-- 029_feed_audit_events.sql # Remove system: actor constraint branch or add follow-up migration if deployed
```

The structure keeps implementation localized to the storage boundary and focused tests, as requested by the Phase 2 context. [VERIFIED: 02-CONTEXT.md; VERIFIED: filesystem listing]

### Pattern 1: asyncpg Transaction Per Audited Mutation

**What:** Use one acquired connection and one explicit transaction for the current-state mutation, sequence allocation, and audit insert. [CITED: https://magicstack.github.io/asyncpg/current/usage.html]

**When to use:** Use for `create_feed`, `update_feed`, `deactivate_feed`, `delete_feed`, and `reset_feed`; do not use for heartbeat, lease, progress, or runtime failure paths in Phase 2. [VERIFIED: 02-CONTEXT.md; VERIFIED: backend/pipeline/storage/feed_store.py]

**Example:**

```python
# Source: asyncpg transaction docs and existing FeedStore style.
async with self._pool.acquire() as conn:
    async with conn.transaction():
        before_row = await conn.fetchrow(
            GET_AUDIT_FEED_SNAPSHOT_SQL,
            feed_id,
        )
        if before_row is None:
            return None

        after_row = await conn.fetchrow(
            RESET_FEED_SQL,
            feed_id,
        )
        if after_row is None:
            msg = f"Failed to reset feed {feed_id}"
            raise ValueError(msg)

        feed_sequence = await conn.fetchval(
            ALLOCATE_FEED_AUDIT_SEQUENCE_SQL,
            feed_id,
        )
        await conn.execute(
            INSERT_FEED_AUDIT_EVENT_SQL,
            feed_id,
            after_row["name"],
            after_row["source_type"],
            "feed.reset",
            actor_id,
            feed_sequence,
            json.dumps(_audit_snapshot(before_row)),
            json.dumps(_audit_snapshot(after_row)),
        )

return self._row_to_feed(after_row)
```

### Pattern 2: Atomic Sequence Allocation With Existing Counter Table

**What:** Allocate `feed_sequence` with `INSERT ... ON CONFLICT DO UPDATE ... RETURNING`, using the `feed_audit_event_sequences` primary key as the serialization point. [VERIFIED: terraform/modules/alloydb/sql/ingestion/029_feed_audit_events.sql; CITED: https://www.postgresql.org/docs/15/sql-insert.html]

**When to use:** Use once per emitted audit event, inside the same transaction as the feed mutation and audit insert. [VERIFIED: 02-CONTEXT.md]

**Example:**

```sql
-- Source: PostgreSQL 15 INSERT / ON CONFLICT docs and Phase 1 schema.
INSERT INTO feed_audit_event_sequences (feed_id, next_sequence)
VALUES ($1, 2)
ON CONFLICT (feed_id) DO UPDATE
SET next_sequence = feed_audit_event_sequences.next_sequence + 1,
    updated_at = NOW()
RETURNING next_sequence - 1 AS feed_sequence;
```

This returns `1` for the first event for a feed and returns the previous `next_sequence` for each later event. [CITED: https://www.postgresql.org/docs/15/sql-insert.html]

### Pattern 3: Audit-Specific Snapshot Projection

**What:** Use an audit snapshot query/helper that selects the full allowlist, including fields not currently present in the public `Feed` API model. [VERIFIED: documentation/feed-audit-events.md; VERIFIED: backend/services/feeds/models.py]

**When to use:** Use for every audited event. Use `{}` for create `before_values` and delete `after_values`. [VERIFIED: 02-CONTEXT.md]

**Example:**

```sql
-- Source: Phase 1 contract allowlist and PostgreSQL JSONB docs.
SELECT
    f.id,
    f.name,
    f.source_type,
    f.status::text AS status,
    f.failure_count,
    f.retry_after,
    f.status_reason,
    f.status_reason_updated_at,
    f.status_reason_detail,
    f.quarantine_reason,
    f.last_bookmark_time,
    f.created_at,
    fp.source_feed_id,
    fp.tags
FROM feeds f
JOIN feed_properties fp ON fp.feed_id = f.id
WHERE f.id = $1
FOR NO KEY UPDATE;
```

PostgreSQL row locks block concurrent updates/deletes on the selected rows until the transaction ends, and `FOR NO KEY UPDATE` is the row lock mode acquired by updates that do not change key columns. [CITED: https://www.postgresql.org/docs/15/explicit-locking.html]

### Pattern 4: No-Op Update Returns Current Feed Without Audit

**What:** Lock/read the current feed row, normalize stored tags/request tags to comparable Python lists, compare stored `name` and `tags`, and return the current feed when they are equal. [VERIFIED: 02-CONTEXT.md; VERIFIED: backend/pipeline/storage/feed_store.py]

**When to use:** Only `update_feed`; create/deactivate/reset/delete always emit one audit event when they succeed. [VERIFIED: 02-CONTEXT.md]

**Example:**

```python
# Source: Phase 2 D-05 through D-07 and existing _row_to_feed tag parsing.
before_snapshot = _audit_snapshot(before_row)
after_input = {
    "name": name,
    "tags": tags or [],
}
if (
    before_snapshot["name"] == after_input["name"]
    and before_snapshot["feed_properties.tags"] == after_input["tags"]
):
    return self._row_to_feed(before_row)
```

### Anti-Patterns to Avoid

- **Parallel audited methods:** Do not add `create_feed_with_audit` or similar variants because the user locked the existing methods as audited paths. [VERIFIED: 02-CONTEXT.md]
- **Autocommit audit insert after mutation:** Do not use separate pool-level `fetchrow`/`execute` calls for state and audit writes because asyncpg applies changes immediately outside an explicit transaction. [CITED: https://magicstack.github.io/asyncpg/current/usage.html]
- **`MAX(feed_sequence) + 1`:** Do not derive ordering from existing audit rows because D-20 rejects that race-prone shape. [VERIFIED: 02-CONTEXT.md]
- **Raw row dump snapshots:** Do not serialize unrestricted `feeds.*` and `feed_properties.*`; snapshots must be allowlisted and exclude lease/heartbeat noise. [VERIFIED: 02-CONTEXT.md; VERIFIED: documentation/feed-audit-events.md]
- **Using PostgreSQL 18-only `RETURNING WITH (OLD AS ...)`:** Do not rely on PostgreSQL 18 syntax because the integration fixture is AlloyDB Omni 15 and PostgreSQL 15 docs show the older `RETURNING` form. [VERIFIED: integration_tests/conftest.py; CITED: https://www.postgresql.org/docs/15/sql-insert.html; CITED: https://www.postgresql.org/docs/current/sql-insert.html]

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Transaction management | Custom rollback flags or manual compensating deletes | asyncpg `connection.transaction()` | asyncpg provides commit/rollback context-manager semantics. [CITED: https://magicstack.github.io/asyncpg/current/usage.html] |
| Per-feed ordering | Python counters, `MAX(feed_sequence)+1`, or service-side sequence state | `feed_audit_event_sequences` atomic upsert/update | The database serializes conflicting upserts and already has a uniqueness backstop. [VERIFIED: terraform/modules/alloydb/sql/ingestion/029_feed_audit_events.sql; CITED: https://www.postgresql.org/docs/15/sql-insert.html] |
| Snapshot shape | Ad hoc raw row serialization | Storage-local allowlist helper | The contract requires maintained allowlisted domain snapshots. [VERIFIED: 02-CONTEXT.md; VERIFIED: documentation/feed-audit-events.md] |
| Actor attribution | Request-body actor fields or nullable fake users | Required `actor_id` parameter and Phase 2 `service:feeds-service` fallback | Trusted human forwarding is deferred and actor spoofing must be avoided. [VERIFIED: 02-CONTEXT.md] |
| Rollback/concurrency proof | Pure mock-only assertions | Existing AlloyDB Omni/Testcontainers storage integration fixture | Database transaction and row-lock behavior cannot be proven by mocks alone. [VERIFIED: integration_tests/conftest.py; CITED: https://www.postgresql.org/docs/15/explicit-locking.html] |

**Key insight:** Phase 2 is not an audit event emitter sprinkled through callers; it is a transaction boundary change in storage. [VERIFIED: 02-CONTEXT.md]

## Runtime State Inventory

This phase includes actor vocabulary cleanup before real audit rows are emitted, so runtime state risk is limited but should be called out for planning. [VERIFIED: 02-CONTEXT.md]

| Category | Items Found | Action Required |
|----------|-------------|------------------|
| Stored data | Repository code has no existing writer for `feed_audit_events`, but a live database could theoretically contain manually inserted rows with `actor_id LIKE 'system:%'`. [VERIFIED: rg audit writer search; ASSUMED] | Add a migration/precheck plan for removing `system:` from allowed actor constraints; confirm no live `system:%` audit rows before tightening if production schema has already been applied. [VERIFIED: terraform/modules/alloydb/sql/ingestion/029_feed_audit_events.sql] |
| Live service config | No external service config writes audit actors in Phase 2; trusted admin actor forwarding is deferred. [VERIFIED: 02-CONTEXT.md] | None for Phase 2. [VERIFIED: 02-CONTEXT.md] |
| OS-registered state | No OS-level registrations were found or implicated by feed audit actor vocabulary. [VERIFIED: repository search scope] | None. [VERIFIED: repository search scope] |
| Secrets/env vars | No secret or env var name is changed by Phase 2 actor fallback; `service:feeds-service` is a literal service actor. [VERIFIED: 02-CONTEXT.md] | None. [VERIFIED: 02-CONTEXT.md] |
| Build artifacts | No generated artifact currently contains audit writer code; OpenAPI/response exposure of diagnostic detail is Phase 3. [VERIFIED: backend/services/feeds/models.py; VERIFIED: .planning/ROADMAP.md] | Do not run frontend/protobuf generation for Phase 2 unless implementation unexpectedly changes public contracts. [VERIFIED: .planning/ROADMAP.md] |

## Common Pitfalls

### Pitfall 1: Transaction Split Between Mutation And Audit

**What goes wrong:** A feed row changes but the audit insert fails, or an audit row persists after a failed mutation. [VERIFIED: .planning/REQUIREMENTS.md]

**Why it happens:** Existing `FeedStore` lifecycle methods use pool-level one-shot calls, and asyncpg autocommits outside an explicit transaction block. [VERIFIED: backend/pipeline/storage/feed_store.py; CITED: https://magicstack.github.io/asyncpg/current/usage.html]

**How to avoid:** Acquire one connection and use one `connection.transaction()` block for the state mutation, sequence allocation, and audit insert. [CITED: https://magicstack.github.io/asyncpg/current/usage.html]

**Warning signs:** Code calls `self._pool.fetchrow(...)` for the mutation and later `self._pool.execute(...)` for audit insertion. [VERIFIED: backend/pipeline/storage/feed_store.py]

### Pitfall 2: Deletion Snapshot Captured Too Late

**What goes wrong:** `feed.deleted` loses `feed_properties.source_feed_id` or `tags` because `feed_properties` cascades after the `feeds` row is deleted. [VERIFIED: terraform/modules/alloydb/sql/ingestion/012_feed_properties.sql; VERIFIED: 02-CONTEXT.md]

**Why it happens:** `feed_properties.feed_id` has `ON DELETE CASCADE`, and current hard delete removes the feed row after deleting audio/transcript dependencies. [VERIFIED: terraform/modules/alloydb/sql/ingestion/012_feed_properties.sql; VERIFIED: backend/pipeline/storage/feed_queries.py]

**How to avoid:** Lock/read the allowlisted snapshot before running `DELETE_FEED_SQL`, insert `feed.deleted`, then delete inside the same transaction. [VERIFIED: 02-CONTEXT.md]

**Warning signs:** Delete audit code builds `before_values` from a post-delete `SELECT` or from only the feed ID. [VERIFIED: 02-CONTEXT.md]

### Pitfall 3: No-Op Update Changes API Behavior

**What goes wrong:** A no-op update returns `None` or 404 because the SQL is changed to only return rows when values differ. [VERIFIED: 02-CONTEXT.md]

**Why it happens:** `UPDATE ... WHERE value IS DISTINCT FROM ... RETURNING` is tempting but conflates "no meaningful change" with "not found". [VERIFIED: 02-CONTEXT.md]

**How to avoid:** Read/lock the current row first, compare normalized `name` and `tags`, and return the current feed without audit if unchanged. [VERIFIED: 02-CONTEXT.md]

**Warning signs:** Tests for no-op update assert 404 or `None`, or service/API tests change status code for unchanged updates. [VERIFIED: backend/services/feeds/tests/test_api.py]

### Pitfall 4: Actor Vocabulary Drift

**What goes wrong:** Storage emits `service:feeds-service` but SQL/tests/docs still accept or require `system:`. [VERIFIED: 02-CONTEXT.md; VERIFIED: backend/pipeline/storage/tests/test_feed_audit_contract.py]

**Why it happens:** Phase 1 allowed `system:<component_name>`, while Phase 2 decisions removed it before real audit rows are emitted. [VERIFIED: documentation/feed-audit-events.md; VERIFIED: 02-CONTEXT.md]

**How to avoid:** Plan a contract/schema/test cleanup before enabling audited writes. [VERIFIED: 02-CONTEXT.md]

**Warning signs:** `_ACTOR_STRINGS` or SQL constraint branches still include `system:` after Phase 2 implementation. [VERIFIED: backend/pipeline/storage/tests/test_feed_audit_contract.py; VERIFIED: terraform/modules/alloydb/sql/ingestion/029_feed_audit_events.sql]

### Pitfall 5: Snapshot Helper Uses Public Feed Model

**What goes wrong:** Audit snapshots omit `retry_after` or `status_reason_detail` because `Feed`/API response models do not currently expose them. [VERIFIED: documentation/feed-audit-events.md; VERIFIED: backend/services/feeds/models.py]

**Why it happens:** Phase 3 owns API compatibility and diagnostic-detail exposure, while Phase 2 needs storage-only audit snapshot values. [VERIFIED: .planning/ROADMAP.md; VERIFIED: 02-CONTEXT.md]

**How to avoid:** Use an audit-specific row projection/helper rather than relying on the public `Feed` Pydantic model or current `Feed` TypedDict alone. [VERIFIED: backend/services/feeds/models.py; VERIFIED: backend/pipeline/storage/feed_store.py]

**Warning signs:** `before_values`/`after_values` are built from `Feed.model_dump()` or from `_row_to_feed()` output only. [VERIFIED: backend/services/feeds/models.py; VERIFIED: backend/pipeline/storage/feed_store.py]

### Pitfall 6: Deadlock-Prone Lock Order

**What goes wrong:** Concurrent update/reset/delete operations block each other or deadlock under load. [CITED: https://www.postgresql.org/docs/15/explicit-locking.html]

**Why it happens:** Different methods acquire locks in inconsistent order, such as sequence row first in one method and feed row first in another. [CITED: https://www.postgresql.org/docs/15/explicit-locking.html]

**How to avoid:** For existing-feed mutations, lock/read the feed snapshot first, then allocate sequence, then insert audit; keep that order consistent. [VERIFIED: 02-CONTEXT.md; CITED: https://www.postgresql.org/docs/15/explicit-locking.html]

**Warning signs:** One method calls `ALLOCATE_FEED_AUDIT_SEQUENCE_SQL` before reading/locking the feed row while another locks the feed first. [VERIFIED: 02-CONTEXT.md]

## Code Examples

Verified patterns from official or repository sources follow. [VERIFIED: repository files; CITED: official docs]

### Transaction Boundary

```python
# Source: https://magicstack.github.io/asyncpg/current/usage.html
async with pool.acquire() as connection:
    async with connection.transaction():
        await connection.execute(
            "INSERT INTO feed_audit_events (...) VALUES (...)",
        )
```

### Sequence Allocator

```sql
-- Source: https://www.postgresql.org/docs/15/sql-insert.html
INSERT INTO feed_audit_event_sequences (feed_id, next_sequence)
VALUES ($1, 2)
ON CONFLICT (feed_id) DO UPDATE
SET next_sequence = feed_audit_event_sequences.next_sequence + 1,
    updated_at = NOW()
RETURNING next_sequence - 1 AS feed_sequence;
```

### Audit Insert Skeleton

```sql
-- Source: terraform/modules/alloydb/sql/ingestion/029_feed_audit_events.sql
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
);
```

### Delete Flow

```python
# Source: Phase 2 D-11 and current DELETE_FEED_SQL behavior.
async with self._pool.acquire() as conn:
    async with conn.transaction():
        before_row = await conn.fetchrow(
            GET_AUDIT_FEED_SNAPSHOT_SQL,
            feed_id,
        )
        if before_row is None:
            return False

        feed_sequence = await conn.fetchval(
            ALLOCATE_FEED_AUDIT_SEQUENCE_SQL,
            feed_id,
        )
        await _insert_audit_event(
            conn,
            action="feed.deleted",
            actor_id=actor_id,
            feed_sequence=feed_sequence,
            before_values=_audit_snapshot(before_row),
            after_values={},
        )
        result = await conn.execute(DELETE_FEED_SQL, feed_id)
        if result != "DELETE 1":
            msg = f"Failed to delete feed {feed_id}"
            raise ValueError(msg)
return True
```

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| Pool-level one-shot `fetchrow`/`execute` for each feed mutation. [VERIFIED: backend/pipeline/storage/feed_store.py] | Explicit asyncpg connection transaction per audited mutation. [CITED: https://magicstack.github.io/asyncpg/current/usage.html; VERIFIED: 02-CONTEXT.md] | Phase 2. [VERIFIED: .planning/ROADMAP.md] | Required for state and audit rows to commit/roll back together. [VERIFIED: .planning/REQUIREMENTS.md] |
| Phase 1 actor namespace includes `system:`. [VERIFIED: documentation/feed-audit-events.md; VERIFIED: terraform/modules/alloydb/sql/ingestion/029_feed_audit_events.sql] | Phase 2 removes `system:` and keeps `service:`, `job:`, `gcp-sa:`, `user:google:`, `user-email:`, and `unknown:unknown`. [VERIFIED: 02-CONTEXT.md] | Phase 2 context restart on 2026-06-19. [VERIFIED: 02-CONTEXT.md] | Contract/schema/tests must be cleaned before emitting rows. [VERIFIED: 02-CONTEXT.md] |
| Audit schema foundation only, no repository writer. [VERIFIED: rg audit writer search; VERIFIED: terraform/modules/alloydb/sql/ingestion/029_feed_audit_events.sql] | Storage-owned writer inside existing `FeedStore` methods. [VERIFIED: 02-CONTEXT.md] | Phase 2. [VERIFIED: .planning/ROADMAP.md] | Service/runtime callers no longer decide audit row shape. [VERIFIED: .planning/REQUIREMENTS.md] |
| Changed-field-only snapshots considered during discussion. [VERIFIED: 02-CONTEXT.md] | Full allowlisted snapshots for all Phase 2 lifecycle events. [VERIFIED: 02-CONTEXT.md] | Phase 2 restarted context. [VERIFIED: 02-CONTEXT.md] | Planner must include shared snapshot helper and event-specific `{}` sides. [VERIFIED: 02-CONTEXT.md] |
| PostgreSQL current docs include newer `RETURNING WITH (OLD AS ..., NEW AS ...)` syntax. [CITED: https://www.postgresql.org/docs/current/sql-insert.html] | Use PostgreSQL 15-compatible SQL because local integration uses AlloyDB Omni 15. [VERIFIED: integration_tests/conftest.py; CITED: https://www.postgresql.org/docs/15/sql-insert.html] | Compatibility constraint exists now. [VERIFIED: integration_tests/conftest.py] | Avoid PG18-only syntax in migration/query plans. [CITED: https://www.postgresql.org/docs/15/sql-insert.html] |

**Deprecated/outdated:**

- `system:` actor namespace is outdated for v1 before audit emission and should be removed from docs, SQL constraints, and contract tests in Phase 2. [VERIFIED: 02-CONTEXT.md; VERIFIED: documentation/feed-audit-events.md; VERIFIED: backend/pipeline/storage/tests/test_feed_audit_contract.py]
- `MAX(feed_sequence)+1` is explicitly disallowed for Phase 2 ordering. [VERIFIED: 02-CONTEXT.md]
- Broad local Docker/component/E2E validation is not the default execution path for agents in this repo. [VERIFIED: AGENTS.md; VERIFIED: .agents/instructions.md]

## Assumptions Log

| # | Claim | Section | Risk if Wrong |
|---|-------|---------|---------------|
| A1 | Live production database contents were not queried during planning; repository evidence shows no writer, but manually inserted `feed_audit_events.actor_id LIKE 'system:%'` rows remain theoretically possible. [ASSUMED] | Runtime State Inventory | Resolved by requiring a fail-closed schema migration that prechecks legacy `system:%` rows before replacing the stale actor constraint. |

## Open Questions (RESOLVED)

1. **RESOLVED: Has `029_feed_audit_events.sql` already been applied to any shared/live database with manual audit rows?** [ASSUMED]
   - What we know: The repository has no implemented audit writer yet. [VERIFIED: rg audit writer search]
   - Decision: Phase 2 does not depend on proving live databases are empty. Plan 01 must add a follow-up migration after `029_feed_audit_events.sql` that prechecks for legacy `actor_id LIKE 'system:%'` rows, raises an explicit operator-facing exception if any exist, and otherwise drops/recreates `feed_audit_events_actor_id_check` without the `system:%` branch. [VERIFIED: terraform/modules/alloydb/sql/ingestion/029_feed_audit_events.sql; VERIFIED: .planning/phases/02-transactional-storage-writes/02-01-PLAN.md]
   - Consequence: Fresh schemas and already-applied schemas converge on the same accepted actor vocabulary before Phase 2 storage writers emit audit rows. A shared database with manual `system:%` rows fails closed instead of silently retaining the stale accepting constraint. [VERIFIED: 02-CONTEXT.md]

2. **RESOLVED: Should `status_reason_detail` enter the `Feed` TypedDict in Phase 2 or remain audit-only until Phase 3?** [VERIFIED: backend/pipeline/storage/feed_store.py; VERIFIED: backend/services/feeds/models.py]
   - What we know: Snapshot allowlist includes `status_reason_detail`, but the current public feeds service model does not expose it. [VERIFIED: documentation/feed-audit-events.md; VERIFIED: backend/services/feeds/models.py]
   - Decision: Keep `status_reason_detail` audit-only in Phase 2. Add it to audit-specific snapshot SQL/helper output, but do not require adding it to the public `Feed` TypedDict or feeds-service response models until Phase 3 compatibility work. [VERIFIED: .planning/ROADMAP.md; VERIFIED: .planning/phases/02-transactional-storage-writes/02-01-PLAN.md]
   - Consequence: Phase 2 can persist full audit snapshots without expanding API response compatibility scope. Phase 3 remains responsible for exposing canonical diagnostic detail to existing clients. [VERIFIED: .planning/ROADMAP.md]

## Environment Availability

| Dependency | Required By | Available | Version | Fallback |
|------------|-------------|-----------|---------|----------|
| Python via raw `python3` | Ad hoc commands | yes, wrong major for project | 3.12.13 [VERIFIED: environment probe] | Use `uv run python`, which resolved Python 3.13.12. [VERIFIED: environment probe] |
| `uv` | Backend tests and Python environment | yes | 0.11.2 installed; repo pins 0.9.28. [VERIFIED: environment probe; VERIFIED: .tool-versions] | Use repo/mise-managed tooling where possible; `uv run python` already resolves Python 3.13. [VERIFIED: environment probe] |
| `safe-run` | Host-stable test execution | yes | path `/home/shuojing/.local/bin/safe-run`. [VERIFIED: environment probe] | None needed. [VERIFIED: environment probe] |
| Node.js / npx | Context7 CLI docs lookup | yes | Node v22.22.2, npx 10.9.7; repo pins Node 22.14.0. [VERIFIED: environment probe; VERIFIED: .tool-versions] | None needed for research; use pinned tools for frontend work. [VERIFIED: .tool-versions] |
| `mise` | Standard project tasks | yes | 2026.3.18 installed; newer available warning shown. [VERIFIED: environment probe] | Direct targeted `uv run ...` commands are acceptable when scoped. [VERIFIED: .agents/instructions.md] |
| Docker | Storage integration tests | yes | Server 29.5.2. [VERIFIED: environment probe] | Prefer CI or explicit approval before local testcontainer execution. [VERIFIED: .agents/instructions.md] |
| Testcontainers / AlloyDB Omni | Storage rollback/concurrency integration tests | package locked; container image used by fixture | `testcontainers` 4.14.2; `google/alloydbomni:15`. [VERIFIED: uv.lock; VERIFIED: integration_tests/conftest.py] | Unit tests can cover wiring but not database-level concurrency. [VERIFIED: integration_tests/conftest.py] |

**Missing dependencies with no fallback:** None found for planning or targeted Phase 2 validation. [VERIFIED: environment probes]

**Missing dependencies with fallback:** Raw `python3` is 3.12.13 while the project requires Python 3.13, but `uv run python` resolves Python 3.13.12. [VERIFIED: environment probe; VERIFIED: pyproject.toml]

## Security Domain

### Applicable ASVS Categories

| ASVS Category | Applies | Standard Control |
|---------------|---------|------------------|
| V2 Authentication | no for Phase 2 storage writes | Trusted admin identity forwarding is Phase 3; Phase 2 uses service actor fallback. [VERIFIED: 02-CONTEXT.md; VERIFIED: .planning/ROADMAP.md] |
| V3 Session Management | no | No browser/session behavior changes are in scope. [VERIFIED: 02-CONTEXT.md] |
| V4 Access Control | yes | Do not accept actor IDs from untrusted request bodies; service passes `service:feeds-service` until Phase 3 trusted forwarding. [VERIFIED: 02-CONTEXT.md; VERIFIED: backend/services/feeds/service.py] |
| V5 Input Validation | yes | Enforce actor namespace constraints, JSON object shape, and allowlisted snapshots. [VERIFIED: terraform/modules/alloydb/sql/ingestion/029_feed_audit_events.sql; VERIFIED: documentation/feed-audit-events.md] |
| V6 Cryptography | no | No cryptographic signing, hashing, or secret storage changes are in Phase 2. [VERIFIED: 02-CONTEXT.md] |

### Known Threat Patterns for Storage Audit Writes

| Pattern | STRIDE | Standard Mitigation |
|---------|--------|---------------------|
| Actor spoofing through request payloads | Spoofing | Storage requires explicit `actor_id`, services pass only trusted causal input, and Phase 2 uses fixed `service:feeds-service`. [VERIFIED: 02-CONTEXT.md] |
| State/history drift | Tampering | State mutation and audit insert occur inside one asyncpg transaction. [CITED: https://magicstack.github.io/asyncpg/current/usage.html; VERIFIED: .planning/REQUIREMENTS.md] |
| Lost delete context | Repudiation | Capture full allowlisted `before_values` before hard delete removes the current row and cascades `feed_properties`. [VERIFIED: 02-CONTEXT.md; VERIFIED: terraform/modules/alloydb/sql/ingestion/012_feed_properties.sql] |
| Sensitive data in snapshots | Information Disclosure | Use the maintained allowlist and exclude raw unrestricted rows, secrets, credential-bearing details, and high-noise lease fields. [VERIFIED: 02-CONTEXT.md; VERIFIED: documentation/feed-audit-events.md] |
| Concurrent sequence collision | Tampering | Allocate through `feed_audit_event_sequences` and keep `UNIQUE (feed_id, feed_sequence)`. [VERIFIED: terraform/modules/alloydb/sql/ingestion/029_feed_audit_events.sql; CITED: https://www.postgresql.org/docs/15/sql-insert.html] |
| Local resource exhaustion from validation | Denial of Service | Use narrow `safe-run` commands and avoid unapproved broad Docker/E2E/component tests. [VERIFIED: AGENTS.md; VERIFIED: .agents/instructions.md] |

## Sources

### Primary (HIGH confidence)

- `.planning/phases/02-transactional-storage-writes/02-CONTEXT.md` - locked Phase 2 decisions, deferred scope, verification expectations. [VERIFIED: file read]
- `.planning/REQUIREMENTS.md` - AUD/EVT/CON requirements and traceability. [VERIFIED: file read]
- `.planning/ROADMAP.md` - Phase 2 goal, dependencies, success criteria, future phase boundaries. [VERIFIED: file read]
- `.planning/STATE.md` - Phase 1 completion and Phase 2 current status. [VERIFIED: file read]
- `documentation/feed-audit-events.md` - Phase 1 Feed Audit Event contract and snapshot allowlist. [VERIFIED: file read]
- `terraform/modules/alloydb/sql/ingestion/029_feed_audit_events.sql` - audit table, sequence table, constraints, indexes, actor constraint. [VERIFIED: file read]
- `backend/pipeline/storage/feed_store.py` - existing async `FeedStore` mutation methods and mapping helpers. [VERIFIED: file read]
- `backend/pipeline/storage/feed_queries.py` - current mutation SQL and hard delete shape. [VERIFIED: file read]
- `backend/pipeline/storage/tests/test_feed_store.py` - existing storage unit test style and mutation tests. [VERIFIED: file read]
- `backend/pipeline/storage/tests/test_feed_audit_contract.py` - Phase 1 contract tests requiring actor vocabulary cleanup. [VERIFIED: file read]
- `backend/services/feeds/service.py` and `backend/services/feeds/main.py` - feeds service/API boundary. [VERIFIED: file read]
- Context7 `/websites/magicstack_github_io_asyncpg_current` - asyncpg pool and transaction docs. [VERIFIED: Context7 CLI output]
- PostgreSQL 15 official docs - `INSERT ... ON CONFLICT`, `RETURNING`, row locks, JSONB construction. [CITED: https://www.postgresql.org/docs/15/sql-insert.html; CITED: https://www.postgresql.org/docs/15/explicit-locking.html; CITED: https://www.postgresql.org/docs/15/functions-json.html]

### Secondary (MEDIUM confidence)

- PostgreSQL current official docs - used only to identify syntax that should not be used when targeting the repo's PostgreSQL 15-compatible fixture. [CITED: https://www.postgresql.org/docs/current/sql-insert.html]
- `.planning/codebase/CONCERNS.md` and `.planning/codebase/TESTING.md` - mapped storage fragility and testing conventions. [VERIFIED: file read]
- `.agents/instructions.md`, `AGENTS.md`, and prompt AGENTS instructions - local test safety, ctx7 usage, safe-run guidance, commit rules. [VERIFIED: file read; VERIFIED: prompt]

### Tertiary (LOW confidence)

- Live production database state for manually inserted `feed_audit_events` rows was not checked. [ASSUMED]

## Metadata

**Confidence breakdown:**

- Standard stack: HIGH - Existing dependencies and versions are verified from repo files, `uv.lock`, environment probes, and official asyncpg/PostgreSQL docs. [VERIFIED: uv.lock; VERIFIED: pyproject.toml; CITED: https://magicstack.github.io/asyncpg/current/usage.html]
- Architecture: HIGH - Phase 2 decisions explicitly assign audit creation to `FeedStore`, and code inspection confirms the current mutation/service boundaries. [VERIFIED: 02-CONTEXT.md; VERIFIED: backend/pipeline/storage/feed_store.py; VERIFIED: backend/services/feeds/service.py]
- Pitfalls: HIGH - Pitfalls come from locked decisions, current code shape, existing schema constraints, and official transaction/locking docs. [VERIFIED: 02-CONTEXT.md; VERIFIED: terraform/modules/alloydb/sql/ingestion/029_feed_audit_events.sql; CITED: https://www.postgresql.org/docs/15/explicit-locking.html]
- Runtime state: MEDIUM - Repository state is verified, but live database contents are not inspected. [VERIFIED: rg audit writer search; ASSUMED]

**Research date:** 2026-06-19
**Valid until:** 2026-07-19
