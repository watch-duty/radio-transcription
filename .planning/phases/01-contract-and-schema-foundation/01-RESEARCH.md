# Phase 1: Contract and Schema Foundation - Research

**Researched:** 2026-06-19
**Domain:** Feed audit event domain contract and PostgreSQL/AlloyDB schema foundation
**Confidence:** HIGH

<user_constraints>
## User Constraints (from CONTEXT.md)

Copied verbatim from `.planning/phases/01-contract-and-schema-foundation/01-CONTEXT.md`. [VERIFIED: .planning/phases/01-contract-and-schema-foundation/01-CONTEXT.md]

### Locked Decisions

## Implementation Decisions

### Delete Identity

- **D-01:** Do not create a delete-specific identity blob for
  `feed.deleted`. The event's `before_values` is the self-contained deletion
  snapshot.
- **D-02:** `feed.deleted.before_values` should use the same maintained
  allowlist mechanism as other audit events.
- **D-03:** The deletion snapshot allowlist should be derived from `feeds` row
  fields or a long-term-maintainable subset of that row.
- **D-04:** Exclude noisy worker/heartbeat lease fields from the default delete
  snapshot unless a later phase explicitly proves they are needed.
- **D-05:** Audit history must not rely on the deleted `feeds` row continuing to
  exist and must not use a cascading FK that removes audit events on feed
  delete.

### Actor ID

- **D-06:** Store one required `actor_id` string on every Feed Audit Event.
  Do not add separate `actor_type` or `actor_display` columns in v1.
- **D-07:** `actor_id` must be namespaced and stable enough for filtering:
  `<namespace>:<stable-id>`.
- **D-08:** For human admin actions, prefer `user:google:<sub>` using the
  Google subject claim already present on the BFF `GoogleUser`.
- **D-09:** Use `user-email:<normalized_email>` only as a fallback when a Google
  subject is unavailable.
- **D-10:** Use semantic non-human actor IDs for system-originated events:
  `service:<service_name>`, `system:<component_name>`, and `job:<job_name>`.
- **D-11:** Reserve `gcp-sa:<service_account_email>` for cases where only the
  authenticated workload principal is known and no semantic service/system actor
  can be determined.
- **D-12:** Use `unknown:unknown` as an explicit fallback; it should be rare and
  visible in tests/monitoring later.
- **D-13:** For admin actions, `actor_id` should represent the causal human
  actor, not the BFF or feeds-service transport service account. Trusted actor
  forwarding from BFF to FastAPI is a later phase detail, but the Phase 1
  contract must support it.

### Diagnostic Detail

- **D-14:** `status_reason_detail` follows current `quarantine_reason` behavior
  for persisted text in v1: preserve the emitted detail text and cap its length.
- **D-15:** Do not require redaction or transformation of
  `status_reason_detail` in Phase 1 beyond the length cap.
- **D-16:** Record the security tradeoff explicitly: raw capped detail is easier
  to implement and preserves debugging value, but it can persist sensitive text
  if upstream failure strings contain it. Later hardening can add redaction as a
  contract revision.
- **D-17:** `status_reason` remains the typed machine-readable reason;
  `status_reason_detail` is explanatory text and must not become control flow.

### Contract Documentation

- **D-18:** Write the Phase 1 documentation as a domain contract first, with
  storage schema details second.
- **D-19:** The contract must define Feed Audit Event meaning, action
  vocabulary, actor ID vocabulary, before/after semantics,
  `status_reason_detail`, retention policy, and v1 boundaries.
- **D-20:** Storage columns, indexes, migration names, and table layout are
  supporting details. They should be documented enough for implementation, but
  future WD delivery/admin timeline consumers should not have to reverse
  engineer domain meaning from the table schema.

### the agent's Discretion

The user chose high-level contract semantics and delegated exact allowlist
membership to maintainability. Downstream planning may choose the initial
`feeds` field allowlist, sequence allocator mechanism, migration file names,
and schema constraints as long as the decisions above are preserved.

### Deferred Ideas (OUT OF SCOPE)

None — discussion stayed within phase scope.
</user_constraints>

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|------------------|
| AUD-02 | Each audited event identifies the affected feed even when the current feed row is later deleted. [VERIFIED: .planning/REQUIREMENTS.md] | Use `feed_audit_events.feed_id` plus event-time feed identity columns and deletion `before_values`; do not use a cascading FK to `feeds`. [VERIFIED: 01-CONTEXT.md; CITED: https://www.postgresql.org/docs/current/ddl-constraints.html] |
| AUD-03 | Each audited event records when the event occurred and has a stable per-feed ordering that future timelines can use. [VERIFIED: .planning/REQUIREMENTS.md] | Use `occurred_at TIMESTAMP WITH TIME ZONE`, `feed_sequence BIGINT`, and `UNIQUE (feed_id, feed_sequence)`; prefer an atomic counter table for later writers. [CITED: https://www.postgresql.org/docs/current/sql-createtable.html; CITED: https://www.postgresql.org/docs/current/sql-altertable.html] |
| DIAG-01 | Current feed state includes a canonical bounded diagnostic detail field that can explain abnormal status for both quarantine and non-quarantine failures. [VERIFIED: .planning/REQUIREMENTS.md] | Add nullable `feeds.status_reason_detail TEXT` with the same 2048-character cap behavior as current quarantine reason; no redaction beyond length cap in Phase 1. [VERIFIED: backend/pipeline/storage/quarantine_reason.py; VERIFIED: 01-CONTEXT.md] |
| ACT-01 | Each audit event attributes the cause to a human admin, service component, runtime worker, scheduled job, or explicit unknown actor. [VERIFIED: .planning/REQUIREMENTS.md] | Store one required namespaced `actor_id` and document the allowed namespaces. [VERIFIED: 01-CONTEXT.md] |
| DOC-01 | Repository documentation defines the Feed Audit Event concept, action vocabulary, actor vocabulary, diagnostic-detail semantics, retention policy, and v1 boundaries. [VERIFIED: .planning/REQUIREMENTS.md] | Add a domain-first contract document, then schema details. [VERIFIED: 01-CONTEXT.md] |
| DOC-02 | The contract is written so future Watch Duty backend delivery and admin timeline work can derive consumer payloads without changing the v1 audit meaning. [VERIFIED: .planning/REQUIREMENTS.md] | Define canonical domain fields independent of webhook delivery or UI read APIs. [VERIFIED: .planning/ROADMAP.md; VERIFIED: 01-CONTEXT.md] |
| DOC-03 | Repository terminology distinguishes current feed state, audit history, typed status reasons, diagnostic detail, and the legacy quarantine reason compatibility alias. [VERIFIED: .planning/REQUIREMENTS.md] | Extend the repo terminology around `status_reason`, `status_reason_detail`, `quarantine_reason`, and Feed Audit Events. [VERIFIED: CONTEXT.md; VERIFIED: 01-CONTEXT.md] |
</phase_requirements>

## Project Constraints (from AGENTS.md)

- Read `.agents/instructions.md` before code changes or code review; this research read it and only writes planning documentation. [VERIFIED: AGENTS.md; VERIFIED: .agents/instructions.md]
- Use targeted low-resource checks locally and avoid broad Docker, component, API, E2E, or unscoped pytest lanes unless explicitly approved. [VERIFIED: AGENTS.md; VERIFIED: .agents/instructions.md]
- For docs-only changes, use `git diff --check` rather than Python tests unless the user asks for tests. [VERIFIED: AGENTS.md; VERIFIED: .agents/instructions.md]
- Prefer `mise` for standard formatting, linting, generation, and test tasks, but this worktree's `.mise.toml` is currently untrusted until `mise trust` is run. [VERIFIED: .agents/instructions.md; VERIFIED: environment probe]
- Do not bypass local pre-commit hooks with `--no-verify` when committing. [VERIFIED: .agents/instructions.md]
- Use `ctx7` for current library, framework, SDK, API, CLI, and cloud-service docs; this research used Context7 for PostgreSQL current docs. [VERIFIED: prompt AGENTS.md; VERIFIED: Context7 CLI output]
- Use `safe-run -- <command>` for agent-run tests, builds, installs, browser/e2e runs, benchmarks, stress tests, and other potentially heavy commands. [VERIFIED: prompt AGENTS.md]
- No project-defined skills were found under `.agents/skills` or `.codex/skills`. [VERIFIED: project skills discovery command]
- Security domain is enabled because `.planning/config.json` does not set `security_enforcement: false`; include security guidance. [VERIFIED: .planning/config.json]
- Nyquist validation is disabled because `.planning/config.json` sets `workflow.nyquist_validation` to `false`; omit the Validation Architecture section. [VERIFIED: .planning/config.json]

## Summary

Phase 1 should lock a backend-owned domain contract and database foundation, not implement runtime audit emission, admin timeline APIs, Watch Duty delivery, or service/BFF compatibility work. [VERIFIED: .planning/ROADMAP.md; VERIFIED: 01-CONTEXT.md] The table name is `feed_audit_events`, the current-state row remains `feeds`, and the schema must preserve enough event-time feed identity to remain useful after hard delete. [VERIFIED: 01-CONTEXT.md; VERIFIED: .planning/ROADMAP.md]

The strongest plan is to add one ordered ingestion SQL migration for `feeds.status_reason_detail`, `feed_audit_events`, minimal timeline/retention indexes, and a per-feed sequence foundation, plus a domain-first documentation file that defines event meaning before storage details. [VERIFIED: terraform/modules/alloydb/sql/ingestion/003_feeds.sql; VERIFIED: terraform/modules/alloydb/sql/ingestion/024_feeds_status_reason.sql; VERIFIED: terraform/modules/alloydb/sql/ingestion/028_initialize_feed_bookmarks.sql; CITED: https://www.postgresql.org/docs/current/sql-createtable.html] The migration should stay idempotent, should not index mutable `feeds` diagnostic fields, and should not add a foreign key that can delete or block audit history when `feeds` rows are hard-deleted. [VERIFIED: terraform/modules/alloydb/sql/ci/hot_protection_check.sql; VERIFIED: backend/pipeline/storage/feed_queries.py; CITED: https://www.postgresql.org/docs/current/ddl-constraints.html]

**Primary recommendation:** create a domain contract at `documentation/feed-audit-events.md`, add schema migration `029_feed_audit_events.sql`, define `status_reason_detail` with a 2048-character cap, define one required namespaced `actor_id`, add `feed_sequence` plus uniqueness and an atomic-counter foundation, and leave storage writes/API/runtime/retention enforcement to later mapped phases. [VERIFIED: .planning/ROADMAP.md; VERIFIED: 01-CONTEXT.md; VERIFIED: backend/pipeline/storage/quarantine_reason.py]

## Architectural Responsibility Map

| Capability | Primary Tier | Secondary Tier | Rationale |
|------------|--------------|----------------|-----------|
| Feed Audit Event domain meaning | API / Backend | Database / Storage | The audit event is a backend domain contract consumed by storage writers and future backend delivery/timeline work. [VERIFIED: .planning/ROADMAP.md; VERIFIED: .planning/codebase/ARCHITECTURE.md] |
| `feed_audit_events` table | Database / Storage | API / Backend | AlloyDB/PostgreSQL owns durable rows, constraints, JSONB snapshots, and ordering indexes. [VERIFIED: terraform/modules/alloydb/sql/ingestion; CITED: https://www.postgresql.org/docs/current/datatype-json.html] |
| Per-feed ordering | Database / Storage | API / Backend | The database should enforce `UNIQUE (feed_id, feed_sequence)` while later store code allocates sequence values atomically. [CITED: https://www.postgresql.org/docs/current/sql-altertable.html; CITED: https://www.postgresql.org/docs/current/applevel-consistency.html] |
| Current diagnostic detail | Database / Storage | API / Backend | `feeds.status_reason_detail` belongs on current feed state; Phase 3 can expose it through FastAPI/BFF responses. [VERIFIED: .planning/ROADMAP.md; VERIFIED: backend/services/feeds/models.py] |
| Actor vocabulary | API / Backend | Frontend Server (BFF) | The domain stores one `actor_id`; future admin actor derivation starts at the BFF user and is passed through trusted backend boundaries later. [VERIFIED: frontend/common/src/types/auth.ts; VERIFIED: backend/pipeline/common/auth.py; VERIFIED: 01-CONTEXT.md] |
| Delete-survival identity | Database / Storage | API / Backend | Hard delete removes the current `feeds` row and `feed_properties`; event rows must carry event-time identity and deletion `before_values`. [VERIFIED: backend/pipeline/storage/feed_queries.py; VERIFIED: terraform/modules/alloydb/sql/ingestion/012_feed_properties.sql] |
| Future Watch Duty/admin payload derivation | API / Backend | Database / Storage | Consumer payloads should derive from the domain event contract, not from webhook-specific storage or UI-specific fields. [VERIFIED: .planning/REQUIREMENTS.md; VERIFIED: 01-CONTEXT.md] |

## Standard Stack

### Core

| Library / Tool | Version | Purpose | Why Standard |
|----------------|---------|---------|--------------|
| PostgreSQL-compatible AlloyDB | Production AlloyDB; tests use AlloyDB Omni 15 and CI Postgres 16. [VERIFIED: integration_tests/conftest.py; VERIFIED: .github/workflows/ci.yml] | Durable schema for `feeds`, `feed_audit_events`, JSONB snapshots, timestamps, constraints, and retention index. [VERIFIED: terraform/modules/alloydb/sql/ingestion; CITED: https://www.postgresql.org/docs/current/datatype-json.html] | Current feed state already lives in AlloyDB and feed mutations are SQL-first. [VERIFIED: terraform/modules/alloydb/sql/ingestion/003_feeds.sql; VERIFIED: backend/pipeline/storage/feed_queries.py] |
| Ordered idempotent SQL migrations | Existing numbered files end at `028_initialize_feed_bookmarks.sql`. [VERIFIED: filesystem listing; VERIFIED: terraform/modules/alloydb/sql/ingestion/028_initialize_feed_bookmarks.sql] | Add `029_feed_audit_events.sql`. [VERIFIED: filesystem listing] | The repo already applies ordered ingestion SQL in CI and test schema helpers. [VERIFIED: backend/pipeline/common/test_schema_helper.py; VERIFIED: .github/workflows/ci.yml] |
| Python | `>=3.13,<3.14`. [VERIFIED: pyproject.toml] | Storage-adjacent contract enums/helpers if code is included. [VERIFIED: backend/pipeline/storage/feed_store.py] | Existing feed storage and services are Python. [VERIFIED: backend/pipeline/storage/feed_store.py; VERIFIED: backend/services/feeds/main.py] |
| `asyncpg` | Locked `0.31.0`; lower bound `>=0.29.0`. [VERIFIED: uv.lock; VERIFIED: pyproject.toml] | Later async storage writes in `FeedStore`. [VERIFIED: backend/pipeline/storage/feed_store.py] | Existing async feed store uses asyncpg pool methods and `$1` parameters. [VERIFIED: backend/pipeline/storage/feed_store.py; VERIFIED: backend/pipeline/storage/feed_queries.py] |
| `psycopg[binary]` | Locked `3.3.3`; lower bound `>=3.2.0`. [VERIFIED: uv.lock; VERIFIED: pyproject.toml] | Later sync Echo parity in `SyncFeedStore`. [VERIFIED: backend/pipeline/storage/sync_feed_store.py] | Existing Echo storage path uses psycopg v3 `%s` parameters. [VERIFIED: backend/pipeline/storage/sync_feed_store.py] |

### Supporting

| Library / Tool | Version | Purpose | When to Use |
|----------------|---------|---------|-------------|
| Pydantic | Locked `2.13.3`; lower bound `>=2.10.6`. [VERIFIED: uv.lock; VERIFIED: backend/services/feeds/pyproject.toml] | Later feed response compatibility for `status_reason_detail`. [VERIFIED: backend/services/feeds/models.py; VERIFIED: .planning/ROADMAP.md] | Phase 3 owns API exposure, but Phase 1 docs should name the compatibility boundary. [VERIFIED: .planning/ROADMAP.md] |
| pytest | Locked `9.0.3`; lower bound `>=9.0.2`. [VERIFIED: uv.lock; VERIFIED: pyproject.toml] | Targeted unit/schema checks in later execution. [VERIFIED: .planning/codebase/TESTING.md] | Use narrow commands only; avoid broad local lanes by default. [VERIFIED: AGENTS.md; VERIFIED: .agents/instructions.md] |
| testcontainers | Locked `4.14.2`. [VERIFIED: uv.lock] | Storage component tests against AlloyDB Omni when explicitly approved. [VERIFIED: integration_tests/conftest.py] | Use for Phase 2/5 SQL behavior; Phase 1 research/docs should not start containers. [VERIFIED: AGENTS.md; VERIFIED: .planning/ROADMAP.md] |
| `safe-run` | Available at `/home/shuojing/.local/bin/safe-run`. [VERIFIED: environment probe] | Host-stability wrapper for heavy commands. [VERIFIED: prompt AGENTS.md] | Prefix tests/builds/install commands that may consume substantial resources. [VERIFIED: prompt AGENTS.md] |

### Alternatives Considered

| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| `feed_audit_events` AlloyDB table | Cloud Logging or app logs | Logs are not the durable, transactionally related domain data requested by the roadmap. [VERIFIED: .planning/PROJECT.md; CITED: https://cheatsheetseries.owasp.org/cheatsheets/Logging_Cheat_Sheet.html] |
| Domain audit contract | Webhook/outbox payload schema | The prior discussion locked `feed_audit_events` as audit foundation, not delivery state. [VERIFIED: 01-CONTEXT.md] |
| Ordered SQL migrations | Alembic or another migration framework | The repo already uses idempotent ordered SQL for AlloyDB and replay in tests; adding a second migration system creates process split. [VERIFIED: terraform/modules/alloydb/sql/ingestion; VERIFIED: backend/pipeline/common/test_schema_helper.py] |
| Required namespaced `actor_id` | Separate `actor_type` and display columns | The user locked one required `actor_id` and rejected separate actor columns for v1. [VERIFIED: 01-CONTEXT.md] |
| Atomic counter foundation | Naive `MAX(feed_sequence)+1` under concurrent writers | PostgreSQL docs recommend explicit locks for application-level consistency; a unique constraint is a backstop, not a race-free allocator. [CITED: https://www.postgresql.org/docs/current/applevel-consistency.html; CITED: https://www.postgresql.org/docs/current/sql-altertable.html] |

**Installation:**

No new dependency installation is recommended for Phase 1. [VERIFIED: pyproject.toml; VERIFIED: uv.lock]

```bash
# Docs/schema-only local check
git diff --check

# Targeted backend checks if code/schema tests are added later
safe-run -- uv run python -m pytest backend/pipeline/storage/tests/test_feed_store.py backend/services/feeds/tests/test_api.py -q
```

**Version verification:** Python package versions were verified from `uv.lock`; PostgreSQL schema behavior was verified through Context7 `/websites/postgresql_current` docs; no npm package version is required for Phase 1. [VERIFIED: uv.lock; VERIFIED: Context7 CLI output]

## Architecture Patterns

### System Architecture Diagram

```text
Phase 1 authoring
    |
    v
Domain contract documentation
    | defines action vocabulary, actor_id vocabulary,
    | before/after semantics, status_reason_detail, retention policy
    v
Ordered SQL migration 029_feed_audit_events.sql
    |-- feeds.status_reason_detail (current-state detail)
    |-- feed_audit_events (append-only audit history)
    |-- feed_sequence uniqueness and sequence-counter foundation
    |-- minimal indexes for future per-feed timelines and retention
    v
Future phases
    |-- Phase 2 storage writers allocate sequence and insert events atomically
    |-- Phase 3 API/BFF exposes diagnostic detail and forwards human actor
    |-- Phase 4 runtime emits failure/quarantine/recovery events
    `-- Phase 5 retention job and behavioral verification
```

This data flow follows the roadmap phase order and keeps Phase 1 as contract/schema foundation only. [VERIFIED: .planning/ROADMAP.md; VERIFIED: 01-CONTEXT.md]

### Recommended Project Structure

```text
documentation/
`-- feed-audit-events.md        # Domain contract first, schema second

backend/pipeline/storage/
`-- feed_audit.py               # Optional StrEnum/constants/helpers only; no inserts yet

terraform/modules/alloydb/sql/ingestion/
`-- 029_feed_audit_events.sql   # status_reason_detail + audit table + sequence foundation
```

The documentation path matches the repo's existing `documentation/` area, and the SQL path matches existing ingestion migrations. [VERIFIED: filesystem listing; VERIFIED: terraform/modules/alloydb/sql/ingestion]

### Pattern 1: Domain-First Contract

**What:** Define the Feed Audit Event meaning, action names, actor ID namespaces, diagnostic detail semantics, snapshot allowlist, retention policy, and v1 boundaries before listing table columns. [VERIFIED: 01-CONTEXT.md]

**When to use:** Use for Phase 1 documentation and any future consumer-facing contract reference. [VERIFIED: .planning/ROADMAP.md]

**Example:**

```markdown
<!-- Source: 01-CONTEXT.md decisions D-18 through D-20 -->
## Feed Audit Event

A Feed Audit Event is durable backend history for a meaningful feed mutation.
It is not the current-state source of truth and is not a Watch Duty delivery
attempt record.

Required concepts:
- action: one value from the Feed Audit Event action vocabulary
- actor_id: one required namespaced actor value
- occurred_at: when the domain event occurred
- feed_sequence: stable per-feed ordering for timelines
- before_values / after_values: allowlisted domain snapshots
```

### Pattern 2: Deletion-Safe Audit Schema

**What:** Store affected feed identity in the audit row and deletion `before_values` rather than relying on the current `feeds` row. [VERIFIED: 01-CONTEXT.md; VERIFIED: backend/pipeline/storage/feed_queries.py]

**When to use:** Use for `feed_audit_events` DDL and the contract section describing hard delete. [VERIFIED: .planning/ROADMAP.md]

**Example:**

```sql
-- Source: PostgreSQL current docs for CREATE TABLE / constraints;
-- source: 01-CONTEXT.md D-05 for no cascading feed FK.
CREATE TABLE IF NOT EXISTS feed_audit_events (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    feed_id UUID NOT NULL,
    feed_sequence BIGINT NOT NULL,
    action TEXT NOT NULL,
    occurred_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    actor_id TEXT NOT NULL,
    feed_name TEXT,
    source_type TEXT,
    source_feed_id TEXT,
    status TEXT,
    status_reason TEXT,
    status_reason_detail TEXT,
    before_values JSONB NOT NULL DEFAULT '{}'::jsonb,
    after_values JSONB NOT NULL DEFAULT '{}'::jsonb,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    CONSTRAINT feed_audit_events_feed_sequence_unique
        UNIQUE (feed_id, feed_sequence)
);
```

Do not add `REFERENCES feeds(id) ON DELETE CASCADE`; PostgreSQL supports cascade behavior, and the user explicitly rejected audit deletion on feed delete. [CITED: https://www.postgresql.org/docs/current/ddl-constraints.html; VERIFIED: 01-CONTEXT.md]

### Pattern 3: Stable Per-Feed Sequence Foundation

**What:** Store `feed_sequence` on each event and enforce uniqueness per feed; add a counter table or equivalent allocator foundation so later storage writers do not depend on racy `MAX+1`. [VERIFIED: .planning/ROADMAP.md; CITED: https://www.postgresql.org/docs/current/applevel-consistency.html]

**When to use:** Use in the Phase 1 migration if the planner wants the allocator schema locked before Phase 2 writers. [VERIFIED: 01-CONTEXT.md]

**Example:**

```sql
-- Source: PostgreSQL current docs for UNIQUE constraints and app-level locking.
CREATE TABLE IF NOT EXISTS feed_audit_event_sequences (
    feed_id UUID PRIMARY KEY,
    next_sequence BIGINT NOT NULL DEFAULT 1,
    CHECK (next_sequence >= 1)
);

ALTER TABLE feed_audit_events
    ADD CONSTRAINT feed_audit_events_feed_sequence_unique
    UNIQUE (feed_id, feed_sequence);
```

Later Phase 2 writer SQL should allocate with an atomic `INSERT ... ON CONFLICT DO UPDATE ... RETURNING` CTE or equivalent single-transaction mechanism. [CITED: https://www.postgresql.org/docs/current/applevel-consistency.html]

### Pattern 4: Bounded Current Diagnostic Detail

**What:** Add `feeds.status_reason_detail TEXT` with a length check and use the same 2048-character cap behavior as current quarantine text. [VERIFIED: backend/pipeline/storage/quarantine_reason.py; VERIFIED: 01-CONTEXT.md]

**When to use:** Use in Phase 1 migration and contract docs; writer lifecycle and API exposure are later phases. [VERIFIED: .planning/ROADMAP.md]

**Example:**

```sql
-- Source: existing quarantine_reason cap is 2048 characters.
ALTER TABLE feeds
    ADD COLUMN IF NOT EXISTS status_reason_detail TEXT;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
          FROM pg_constraint
         WHERE conname = 'feeds_status_reason_detail_length'
    ) THEN
        ALTER TABLE feeds
            ADD CONSTRAINT feeds_status_reason_detail_length
            CHECK (
                status_reason_detail IS NULL
                OR length(status_reason_detail) <= 2048
            );
    END IF;
END $$;
```

Do not add an index on `feeds.status_reason_detail` because this phase has no current-state diagnostic query path and the repo guards hot feed updates from mutable-column indexes. [VERIFIED: terraform/modules/alloydb/sql/ci/hot_protection_check.sql; VERIFIED: terraform/modules/alloydb/sql/ingestion/024_feeds_status_reason.sql]

### Anti-Patterns to Avoid

- **Combined delivery outbox:** Do not mix delivery attempts, retries, signatures, or webhook state into `feed_audit_events`; those are deferred v2 delivery concerns. [VERIFIED: .planning/REQUIREMENTS.md; VERIFIED: 01-CONTEXT.md]
- **Delete-specific identity blob:** Do not add a special `deleted_feed_identity` field; use normal event identity plus the maintained `before_values` snapshot. [VERIFIED: 01-CONTEXT.md]
- **Separate `actor_type` or display columns:** Do not revive older research sketches with actor type columns; the locked contract is one required `actor_id`. [VERIFIED: 01-CONTEXT.md; VERIFIED: .planning/research/ARCHITECTURE.md]
- **Generic trigger audit:** Do not use `AFTER UPDATE ON feeds` triggers because they cannot reliably encode actor ID, semantic action names, or lease-churn exclusions. [VERIFIED: .planning/codebase/ARCHITECTURE.md; VERIFIED: 01-CONTEXT.md]
- **API/UI work:** Do not add admin timeline read endpoints, UI, or Watch Duty delivery in Phase 1. [VERIFIED: .planning/ROADMAP.md; VERIFIED: 01-CONTEXT.md]

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Schema migrations | A new migration framework or ad hoc SQL runner | Existing ordered SQL files under `terraform/modules/alloydb/sql/ingestion/` | The repo already applies and replays these files in CI/test helpers. [VERIFIED: backend/pipeline/common/test_schema_helper.py; VERIFIED: .github/workflows/ci.yml] |
| Audit event storage | Logs, Cloud Logging, or webhook payload tables | `feed_audit_events` in AlloyDB/PostgreSQL | The project needs durable queryable domain history. [VERIFIED: .planning/PROJECT.md; CITED: https://cheatsheetseries.owasp.org/cheatsheets/Logging_Cheat_Sheet.html] |
| JSON snapshot serialization | Ad hoc text blobs | PostgreSQL `JSONB` columns | PostgreSQL supports `jsonb` storage and JSONB indexes if future query paths need them. [CITED: https://www.postgresql.org/docs/current/datatype-json.html] |
| Actor parsing | Separate v1 actor taxonomy columns | Required namespaced `actor_id` | The user locked one string actor model. [VERIFIED: 01-CONTEXT.md] |
| Diagnostic detail bounding | New per-call string slicing | Generalized storage-bound helper based on `cap_quarantine_reason_for_storage` | The current cap behavior is already centralized at the storage boundary. [VERIFIED: backend/pipeline/storage/quarantine_reason.py] |
| Per-feed ordering | Naive concurrent `MAX(feed_sequence)+1` | Counter table/upsert plus `UNIQUE (feed_id, feed_sequence)` | PostgreSQL docs require explicit consistency controls for app-level invariants under concurrent writes. [CITED: https://www.postgresql.org/docs/current/applevel-consistency.html; CITED: https://www.postgresql.org/docs/current/sql-altertable.html] |

**Key insight:** Phase 1 should make the domain and database shape hard to misinterpret, while leaving behavior writers to later phases that can prove transactionality and concurrency with focused tests. [VERIFIED: .planning/ROADMAP.md; VERIFIED: .planning/REQUIREMENTS.md]

## Runtime State Inventory

| Category | Items Found | Action Required |
|----------|-------------|-----------------|
| Stored data | Existing `feeds` rows have `status_reason`, `status_reason_updated_at`, and `quarantine_reason`; no code/schema occurrence of `status_reason_detail` or `feed_audit_events` exists outside planning docs. [VERIFIED: terraform/modules/alloydb/sql/ingestion/020_quarantine_reason.sql; VERIFIED: terraform/modules/alloydb/sql/ingestion/024_feeds_status_reason.sql; VERIFIED: rg status_reason_detail/feed_audit_events] | Add nullable column/table without backfill; keep `quarantine_reason` compatibility; do not migrate existing text into audit history. [VERIFIED: 01-CONTEXT.md; VERIFIED: .planning/ROADMAP.md] |
| Live service config | None found for Phase 1; no existing service config references `feed_audit_events` or `status_reason_detail`. [VERIFIED: rg status_reason_detail/feed_audit_events] | No live-service config change in Phase 1. [VERIFIED: .planning/ROADMAP.md] |
| OS-registered state | None found; Phase 1 scope is repo documentation and SQL schema, not systemd/pm2/launchd/task registration. [VERIFIED: .planning/ROADMAP.md] | None. [VERIFIED: .planning/ROADMAP.md] |
| Secrets/env vars | No new secret or env var is required by the Phase 1 contract/schema work. [VERIFIED: .planning/ROADMAP.md; VERIFIED: pyproject.toml] | Do not add secret/env-var requirements. [VERIFIED: 01-CONTEXT.md] |
| Build artifacts | Generated protobuf and TSOA outputs are unrelated to Phase 1 if no API/proto changes are made. [VERIFIED: .planning/ROADMAP.md; VERIFIED: .planning/codebase/ARCHITECTURE.md] | None for contract docs and SQL migration; regenerate only if a later phase changes proto or TSOA controllers. [VERIFIED: .agents/instructions.md] |

## Common Pitfalls

### Pitfall 1: Cascading Away Delete History

**What goes wrong:** A foreign key from `feed_audit_events.feed_id` to `feeds.id` uses cascade or restrict semantics that either deletes audit rows or blocks hard delete. [VERIFIED: 01-CONTEXT.md; CITED: https://www.postgresql.org/docs/current/ddl-constraints.html]

**Why it happens:** The existing `delete_feed` SQL hard-deletes the feed row after deleting related audio/transcript rows, and `feed_properties` already cascades from `feeds`. [VERIFIED: backend/pipeline/storage/feed_queries.py; VERIFIED: terraform/modules/alloydb/sql/ingestion/012_feed_properties.sql]

**How to avoid:** Store `feed_id` as data without a cascading FK and preserve event-time identity plus deletion `before_values`. [VERIFIED: 01-CONTEXT.md]

**Warning signs:** DDL contains `REFERENCES feeds(id) ON DELETE CASCADE` or delete tests only assert current rows disappeared. [VERIFIED: 01-CONTEXT.md]

### Pitfall 2: Reintroducing Separate Actor Columns

**What goes wrong:** Schema or docs add `actor_type`, `actor_display`, or transport-service identity as canonical v1 fields. [VERIFIED: 01-CONTEXT.md]

**Why it happens:** Older project research sketched separate actor fields before the Phase 1 discussion locked a single `actor_id`. [VERIFIED: .planning/research/ARCHITECTURE.md; VERIFIED: 01-CONTEXT.md]

**How to avoid:** Document and enforce `actor_id` namespaces only: `user:google:<sub>`, fallback `user-email:<normalized_email>`, `service:<service_name>`, `system:<component_name>`, `job:<job_name>`, reserved `gcp-sa:<service_account_email>`, and `unknown:unknown`. [VERIFIED: 01-CONTEXT.md]

**Warning signs:** DDL has `actor_type`, docs describe actor display names, or admin actions use a service-account actor instead of the human user. [VERIFIED: 01-CONTEXT.md]

### Pitfall 3: Treating Diagnostic Detail as Control Flow

**What goes wrong:** Code or docs make `status_reason_detail` a policy key instead of explanatory text. [VERIFIED: 01-CONTEXT.md; VERIFIED: CONTEXT.md]

**Why it happens:** `quarantine_reason` currently stores raw details for debugging, and new detail fields can tempt branching on raw strings. [VERIFIED: backend/pipeline/storage/quarantine_reason.py; VERIFIED: CONTEXT.md]

**How to avoid:** Keep control flow on `status_reason`; define `status_reason_detail` as capped explanatory text only. [VERIFIED: 01-CONTEXT.md]

**Warning signs:** Tests assert behavior based on substring matching in `status_reason_detail`. [VERIFIED: CONTEXT.md]

### Pitfall 4: Over-Scoping Retention Enforcement Into Phase 1

**What goes wrong:** The planner adds pg_cron deletion jobs and retention behavioral tests in Phase 1 even though AUD-05 maps to Phase 5. [VERIFIED: .planning/REQUIREMENTS.md; VERIFIED: .planning/ROADMAP.md]

**Why it happens:** The Phase 1 contract must define retention policy, but enforcement is a later phase requirement. [VERIFIED: .planning/ROADMAP.md]

**How to avoid:** In Phase 1, document 18-month retention, store `occurred_at`, and add an `occurred_at` index if accepted as schema foundation; implement scheduled cleanup in Phase 5. [VERIFIED: .planning/ROADMAP.md; CITED: https://www.postgresql.org/docs/current/sql-createtable.html]

**Warning signs:** A new `*pg_cron*.sql` retention schedule appears in Phase 1 plan tasks. [VERIFIED: terraform/modules/alloydb/sql/ingestion/019_feeds_pg_cron_jobs.sql; VERIFIED: .planning/ROADMAP.md]

### Pitfall 5: Indexing Hot Current-State Diagnostic Fields

**What goes wrong:** New indexes on `feeds.status_reason_detail`, `status_reason`, or other mutable status fields degrade HOT update behavior. [VERIFIED: terraform/modules/alloydb/sql/ci/hot_protection_check.sql]

**Why it happens:** Future query thinking leaks into Phase 1 even though there is no current-state diagnostic query requirement. [VERIFIED: .planning/ROADMAP.md]

**How to avoid:** Index `feed_audit_events` for future timeline/retention, not mutable `feeds` diagnostic fields; optionally extend the HOT guard to include the new column. [VERIFIED: terraform/modules/alloydb/sql/ci/hot_protection_check.sql]

**Warning signs:** Migration adds `CREATE INDEX ... ON feeds (status_reason_detail)` or skips HOT guard review. [VERIFIED: terraform/modules/alloydb/sql/ci/hot_protection_check.sql]

## Code Examples

Verified patterns from official sources and repo code:

### PostgreSQL JSONB and Timeline Fields

```sql
-- Source: https://www.postgresql.org/docs/current/datatype-json.html
-- Source: https://www.postgresql.org/docs/current/sql-createtable.html
CREATE TABLE IF NOT EXISTS feed_audit_events (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    feed_id UUID NOT NULL,
    feed_sequence BIGINT NOT NULL,
    occurred_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    before_values JSONB NOT NULL DEFAULT '{}'::jsonb,
    after_values JSONB NOT NULL DEFAULT '{}'::jsonb,
    CONSTRAINT feed_audit_events_feed_sequence_unique
        UNIQUE (feed_id, feed_sequence)
);
```

### Actor ID Prefix Check

```sql
-- Source: 01-CONTEXT.md actor decisions.
ALTER TABLE feed_audit_events
    ADD CONSTRAINT feed_audit_events_actor_id_check
    CHECK (
        actor_id = 'unknown:unknown'
        OR actor_id LIKE 'user:google:%'
        OR actor_id LIKE 'user-email:%'
        OR actor_id LIKE 'service:%'
        OR actor_id LIKE 'system:%'
        OR actor_id LIKE 'job:%'
        OR actor_id LIKE 'gcp-sa:%'
    );
```

### Diagnostic Cap Helper Shape

```python
# Source: backend/pipeline/storage/quarantine_reason.py
MAX_STATUS_REASON_DETAIL_LENGTH = 2048
_TRUNCATION_MARKER = " [truncated]"

def cap_status_reason_detail_for_storage(text: str) -> str:
    if len(text) <= MAX_STATUS_REASON_DETAIL_LENGTH:
        return text
    prefix_len = MAX_STATUS_REASON_DETAIL_LENGTH - len(_TRUNCATION_MARKER)
    return f"{text[:prefix_len].rstrip()}{_TRUNCATION_MARKER}"
```

This helper mirrors current cap-only behavior; Phase 1 must not add redaction beyond the length cap because the user locked that decision. [VERIFIED: backend/pipeline/storage/quarantine_reason.py; VERIFIED: 01-CONTEXT.md]

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| `quarantine_reason` as the only persisted free-text diagnostic | `status_reason_detail` is the canonical bounded diagnostic detail, while `quarantine_reason` remains compatibility alias later. [VERIFIED: .planning/PROJECT.md; VERIFIED: 01-CONTEXT.md] | Feed Audit Events V1 planning on 2026-06-19. [VERIFIED: .planning/STATE.md; VERIFIED: 01-CONTEXT.md] | Phase 1 adds schema/contract; Phase 3 owns API compatibility. [VERIFIED: .planning/ROADMAP.md] |
| Separate actor field sketches in previous research | One required namespaced `actor_id`. [VERIFIED: 01-CONTEXT.md] | Phase 1 discussion on 2026-06-19. [VERIFIED: 01-CONTEXT.md] | Planner must avoid `actor_type`/`actor_display` columns. [VERIFIED: 01-CONTEXT.md] |
| Delivery/outbox-shaped audit work | Domain audit table `feed_audit_events`; delivery is later. [VERIFIED: 01-CONTEXT.md; VERIFIED: .planning/ROADMAP.md] | Phase 1 context gathered 2026-06-19. [VERIFIED: 01-CONTEXT.md] | Consumer payloads derive from domain events without changing v1 meaning. [VERIFIED: .planning/REQUIREMENTS.md] |
| Current row lookup for feed identity | Event-time identity and `before_values` survive hard delete. [VERIFIED: 01-CONTEXT.md] | Phase 1 context gathered 2026-06-19. [VERIFIED: 01-CONTEXT.md] | No cascading FK from audit events to `feeds`. [VERIFIED: 01-CONTEXT.md] |

**Deprecated/outdated:**

- Separate `actor_type` and `actor_display` in v1 are outdated by locked Phase 1 actor decisions. [VERIFIED: 01-CONTEXT.md; VERIFIED: .planning/research/ARCHITECTURE.md]
- Treating retention enforcement as Phase 1 work is outdated by the roadmap requirement mapping of AUD-05 to Phase 5. [VERIFIED: .planning/REQUIREMENTS.md; VERIFIED: .planning/ROADMAP.md]

## Assumptions Log

> List all claims tagged `[ASSUMED]` in this research. The planner and discuss-phase use this section to identify decisions that need user confirmation before execution.

| # | Claim | Section | Risk if Wrong |
|---|-------|---------|---------------|

**If this table is empty:** All claims in this research were verified or cited — no user confirmation needed.

## Open Questions

1. **Should `feed_audit_event_sequences` be created in Phase 1 or deferred to Phase 2 storage writes?**
   - What we know: Phase 1 must support stable per-feed ordering, and the user delegated sequence allocator mechanism to the planner/researcher. [VERIFIED: .planning/ROADMAP.md; VERIFIED: 01-CONTEXT.md]
   - What's unclear: The roadmap does not explicitly require the allocator table itself in Phase 1, only the contract/schema support. [VERIFIED: .planning/ROADMAP.md]
   - Recommendation: Add the counter table in Phase 1 because it is schema foundation and prevents Phase 2 from inventing a weaker allocator. [CITED: https://www.postgresql.org/docs/current/applevel-consistency.html]

2. **Should Phase 1 touch FastAPI/Pydantic feed response models?**
   - What we know: Phase 1 success criteria say the current feed schema exposes `status_reason_detail`; Phase 3 success criteria separately say API responses expose it. [VERIFIED: .planning/ROADMAP.md]
   - What's unclear: "Schema exposes" can mean database schema only or database plus backend typed feed shape. [VERIFIED: .planning/ROADMAP.md]
   - Recommendation: In Phase 1, add database column and storage/domain contract only; plan API response fields in Phase 3 to match requirement ownership. [VERIFIED: .planning/ROADMAP.md]

## Environment Availability

| Dependency | Required By | Available | Version | Fallback |
|------------|-------------|-----------|---------|----------|
| `safe-run` | Agent-run tests/builds/install commands | yes [VERIFIED: environment probe] | path `/home/shuojing/.local/bin/safe-run` [VERIFIED: environment probe] | Use direct commands only for lightweight reads/docs checks. [VERIFIED: prompt AGENTS.md] |
| Python | Backend/storage tooling | partial [VERIFIED: environment probe] | global `python3` is 3.12.13; project requires 3.13.2 / `>=3.13,<3.14`. [VERIFIED: environment probe; VERIFIED: .tool-versions; VERIFIED: pyproject.toml] | Use trusted `mise` or `uv`-managed environment before running Python tests. [VERIFIED: .tool-versions] |
| `uv` | Python dependency/test runner | yes [VERIFIED: environment probe] | global `uv 0.11.2`; `.tool-versions` pins 0.9.28. [VERIFIED: environment probe; VERIFIED: .tool-versions] | Prefer `mise` once trusted; otherwise note version drift. [VERIFIED: .tool-versions] |
| `mise` | Standard repo task runner | installed but untrusted in this worktree [VERIFIED: environment probe] | `2026.3.18`; repo config requires `mise trust`. [VERIFIED: environment probe] | Run `mise trust` manually or use direct narrow commands. [VERIFIED: environment probe; VERIFIED: .agents/instructions.md] |
| Node/npm/npx | Context7 and possible later BFF tests | yes [VERIFIED: environment probe] | Node `v22.22.2`, npm/npx `10.9.7`; repo pins Node `22.14.0`. [VERIFIED: environment probe; VERIFIED: .tool-versions] | Use `mise` after trust for exact pin if frontend tests are needed. [VERIFIED: .tool-versions] |
| Docker | Storage component tests | yes [VERIFIED: docker info] | Docker Engine `29.5.2`; server reachable. [VERIFIED: docker info] | Avoid component tests unless explicitly approved. [VERIFIED: AGENTS.md; VERIFIED: .agents/instructions.md] |
| `psql` | Manual local SQL checks | no [VERIFIED: environment probe] | not installed on PATH. [VERIFIED: environment probe] | Use CI HOT guard, Docker container psql, or test helpers when approved. [VERIFIED: .github/workflows/ci.yml; VERIFIED: backend/pipeline/common/test_schema_helper.py] |
| Terraform | Infrastructure workflows | version drift [VERIFIED: environment probe] | global `1.15.0`; repo pins `1.14.5`. [VERIFIED: environment probe; VERIFIED: .tool-versions] | Use `mise` after trust for exact pin; Phase 1 schema file edits do not require Terraform execution. [VERIFIED: .tool-versions] |

**Missing dependencies with no fallback:**

- None for writing Phase 1 research/docs/schema plans. [VERIFIED: environment probe]

**Missing dependencies with fallback:**

- `psql` is missing; use CI, Docker-backed psql, or test schema helpers if SQL execution is needed. [VERIFIED: environment probe; VERIFIED: .github/workflows/ci.yml]
- `mise` config is untrusted; run `mise trust` before relying on repo task aliases. [VERIFIED: environment probe]

## Security Domain

### Applicable ASVS Categories

OWASP ASVS 5.0.0 is the latest stable ASVS release according to the OWASP project page. [CITED: https://owasp.org/www-project-application-security-verification-standard/] The GSD template category names below are used as planning buckets, while the source standard should be referenced with versioned identifiers when implementation writes security requirements. [CITED: https://owasp.org/www-project-application-security-verification-standard/]

| ASVS Category | Applies | Standard Control |
|---------------|---------|------------------|
| V2 Authentication | yes | Store admin-caused events with `actor_id` derived from verified Google subject where available, not from request body fields. [VERIFIED: 01-CONTEXT.md; VERIFIED: frontend/common/src/types/auth.ts] |
| V3 Session Management | no | Phase 1 does not change sessions or browser auth state. [VERIFIED: .planning/ROADMAP.md] |
| V4 Access Control | yes later | Phase 1 is write-only; docs must state future timeline reads need authorization before exposing diagnostic detail. [VERIFIED: .planning/ROADMAP.md; CITED: https://owasp.org/Top10/2021/A09_2021-Security_Logging_and_Monitoring_Failures/] |
| V5 Input Validation | yes | Use action vocabulary, actor namespace checks, JSON snapshot allowlists, and diagnostic length caps. [VERIFIED: 01-CONTEXT.md; VERIFIED: backend/pipeline/storage/quarantine_reason.py] |
| V6 Cryptography | no new crypto | Do not hand-roll signatures or encryption in Phase 1; WD delivery signatures are deferred. [VERIFIED: .planning/REQUIREMENTS.md; VERIFIED: .planning/ROADMAP.md] |

### Known Threat Patterns for Feed Audit Schema

| Pattern | STRIDE | Standard Mitigation |
|---------|--------|---------------------|
| Forged audit actor | Spoofing | Contract requires `actor_id` from trusted auth context later; Phase 1 should reject request-body actors in docs. [VERIFIED: 01-CONTEXT.md; VERIFIED: .planning/codebase/CONCERNS.md] |
| Sensitive text retained in diagnostics | Information Disclosure | Phase 1 must document the locked raw-cap tradeoff and the 2048 cap; redaction is not required until later hardening. [VERIFIED: 01-CONTEXT.md; VERIFIED: backend/pipeline/storage/quarantine_reason.py] |
| Audit loss on feed delete | Tampering / Repudiation | Do not cascade audit rows with `feeds`; store event-time feed identity. [VERIFIED: 01-CONTEXT.md; CITED: https://www.postgresql.org/docs/current/ddl-constraints.html] |
| SQL injection in action/detail writes | Tampering | Later writers should keep existing parameterized asyncpg/psycopg SQL patterns. [VERIFIED: backend/pipeline/storage/feed_queries.py; VERIFIED: backend/pipeline/storage/sync_feed_store.py] |
| Log/audit injection in future readers | Tampering | Store structured columns/JSONB and define consumer escaping in future read API work. [CITED: https://owasp.org/Top10/2021/A09_2021-Security_Logging_and_Monitoring_Failures/] |

## Sources

### Primary (HIGH confidence)

- `.planning/phases/01-contract-and-schema-foundation/01-CONTEXT.md` - locked decisions for delete identity, actor ID, diagnostic detail, and contract docs. [VERIFIED]
- `.planning/REQUIREMENTS.md` - Phase 1 requirement IDs and later-phase ownership. [VERIFIED]
- `.planning/ROADMAP.md` - Phase 1 scope and success criteria. [VERIFIED]
- `.planning/STATE.md` - current planning state and dates. [VERIFIED]
- `AGENTS.md` and `.agents/instructions.md` - local agent/test safety constraints. [VERIFIED]
- `.planning/config.json` - `commit_docs`, search flags, and `workflow.nyquist_validation=false`. [VERIFIED]
- `terraform/modules/alloydb/sql/ingestion/*.sql` - current feed schema, migration ordering, pg_cron filename convention, and HOT-safe indexes. [VERIFIED]
- `terraform/modules/alloydb/sql/ci/hot_protection_check.sql` - HOT-protection guard. [VERIFIED]
- `backend/pipeline/storage/feed_store.py`, `feed_queries.py`, `sync_feed_store.py`, and `quarantine_reason.py` - storage patterns, hard delete SQL, current status reason behavior, and 2048-character cap. [VERIFIED]
- `backend/pipeline/common/test_schema_helper.py` and `.github/workflows/ci.yml` - migration replay and pg_cron skip behavior. [VERIFIED]
- Context7 `/websites/postgresql_current` - PostgreSQL current docs for constraints, JSONB, timestamps, unique constraints, sequences/locking topics. [VERIFIED: Context7 CLI]

### Secondary (MEDIUM confidence)

- PostgreSQL current documentation: `ddl-constraints`, `datatype-json`, `sql-createtable`, `sql-altertable`, `sql-select`, and `applevel-consistency`. [CITED: https://www.postgresql.org/docs/current/ddl-constraints.html]
- OWASP ASVS project page - latest stable ASVS 5.0.0 and versioned requirement reference guidance. [CITED: https://owasp.org/www-project-application-security-verification-standard/]
- OWASP Logging Cheat Sheet and OWASP Top 10 A09 - application logging/audit trail guidance and sensitive logging risks. [CITED: https://cheatsheetseries.owasp.org/cheatsheets/Logging_Cheat_Sheet.html; CITED: https://owasp.org/Top10/2021/A09_2021-Security_Logging_and_Monitoring_Failures/]

### Tertiary (LOW confidence)

- None.

## Metadata

**Confidence breakdown:**

- Standard stack: HIGH - based on repo lockfiles, codebase migrations, and Context7 PostgreSQL docs. [VERIFIED: uv.lock; VERIFIED: terraform/modules/alloydb/sql/ingestion; VERIFIED: Context7 CLI]
- Architecture: HIGH - based on Phase 1 context, roadmap boundaries, and existing feed storage architecture. [VERIFIED: 01-CONTEXT.md; VERIFIED: .planning/ROADMAP.md; VERIFIED: .planning/codebase/ARCHITECTURE.md]
- Pitfalls: HIGH - based on existing hard-delete SQL, HOT guard, actor-boundary concerns, and locked user decisions. [VERIFIED: backend/pipeline/storage/feed_queries.py; VERIFIED: terraform/modules/alloydb/sql/ci/hot_protection_check.sql; VERIFIED: .planning/codebase/CONCERNS.md; VERIFIED: 01-CONTEXT.md]

**Research date:** 2026-06-19
**Valid until:** 2026-07-19 for repo-specific planning; re-check PostgreSQL/OWASP docs before security-sensitive implementation if delayed. [CITED: https://owasp.org/www-project-application-security-verification-standard/]
