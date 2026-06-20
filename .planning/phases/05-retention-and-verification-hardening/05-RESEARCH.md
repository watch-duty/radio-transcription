# Phase 5: Retention and Verification Hardening - Research

**Researched:** 2026-06-20
**Domain:** AlloyDB/PostgreSQL retention jobs, feed audit contract verification, targeted pytest/Testcontainers validation
**Confidence:** HIGH

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions
## Implementation Decisions

### Retention Mechanism

- **D-01:** Enforce audit retention inside AlloyDB with a DB-owned `pg_cron`
  scheduled job. Add a new ingestion SQL migration whose filename includes
  `pg_cron`, preserving the repository's existing local/CI skip convention for
  Postgres environments without the extension.
- **D-02:** Retention uses `feed_audit_events.occurred_at` as the expiry field.
  The cutoff is `occurred_at < NOW() - INTERVAL '18 months'`, matching the
  event's domain time rather than insert time.
- **D-03:** Run cleanup daily with a bounded delete batch. Do not use an
  unbounded delete that could remove a large paused-backlog in one transaction.
- **D-04:** Backlog catch-up deletes one bounded batch per daily run. Do not
  loop inside one cron invocation until the backlog is gone, and do not switch
  to high-frequency catch-up as part of v1.

### Retention Side Effects

- **D-05:** Retention deletes expired `feed_audit_events` rows only. It must
  not archive rows, rewrite/redact payloads, or create synthetic summary,
  tombstone, or baseline audit events.
- **D-06:** Retention may leave gaps in `feed_sequence` for retained rows. This
  is expected and acceptable; `feed_sequence` values are immutable audit
  labels and must not be renumbered after old rows expire.
- **D-07:** Retained timelines simply start at the oldest non-expired event.
  Consumers should not infer that the first retained `feed_sequence` is the
  first event that ever happened for that feed.

### Sequence Cleanup

- **D-08:** Do not remove `feed_sequence` or `feed_audit_event_sequences` in
  Phase 5. The current contract and tests use them to satisfy stable per-feed
  ordering and concurrent ordering requirements.
- **D-09:** Prune `feed_audit_event_sequences` rows only when both conditions
  are true: the corresponding feed no longer exists in `feeds`, and there are
  no retained `feed_audit_events` rows for that feed.
- **D-10:** Keep sequence rows for live feeds even if all of that feed's older
  audit rows have expired, because future events still need the next
  transactionally allocated `feed_sequence`.
- **D-11:** Keep sequence rows for deleted feeds while any retained audit event
  remains, so retained history and ordering metadata age out together.

### Verification Boundary

- **D-12:** User discussion focused on retention, side effects, and sequence
  cleanup. The planner may choose the exact verification split, but it must
  preserve the Phase 5 roadmap requirements: retention behavior, event
  coverage, rollback/concurrency, diagnostic-detail lifecycle, public API
  compatibility, delete survival, and no-lease-churn behavior.
- **D-13:** Prefer existing verification lanes and patterns: static SQL
  contract tests for migration invariants, focused storage/service/runtime unit
  tests for behavior, and Testcontainers-backed storage integration tests where
  real database transaction or retention semantics matter.

### the agent's Discretion

- Choose the exact daily cron expression, conservative batch size, SQL helper
  shape, and migration number/name, provided the file name includes `pg_cron`
  and the delete is bounded.
- Choose whether retention cleanup uses one statement or a small set of
  statements, provided expired audit rows are deleted before or together with
  safe pruning of orphaned sequence rows.
- Choose the exact automated-test grouping and command set. Keep Docker or
  Testcontainers lanes aligned with repository safety rules and document any
  human/CI-only verification that cannot run locally.

### Deferred Ideas (OUT OF SCOPE)
## Deferred Ideas

- Revisit in a future phase whether explicit per-feed `feed_sequence` can be
  removed from the audit contract and replaced by client-side ordering on
  `occurred_at` plus event ID. This is a contract/schema redesign and should
  not be done in Phase 5 retention hardening.
</user_constraints>

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|------------------|
| AUD-05 | Audit rows are retained for 18 months and expired only by the approved retention mechanism. | Use a DB-owned pg_cron schedule that calls bounded retention SQL using `occurred_at < NOW() - INTERVAL '18 months'`; static SQL contract tests should guard the filename, cutoff, bounded delete, and absence of synthetic inserts. [VERIFIED: .planning/REQUIREMENTS.md; VERIFIED: .planning/phases/05-retention-and-verification-hardening/05-CONTEXT.md; CITED: https://github.com/citusdata/pg_cron] |
| VER-01 | Automated tests verify audit events for feed create, update, deactivate, reset, delete, failure, quarantine, and recovery paths. | Existing storage, sync, and integration tests already cover most events; Phase 5 should add an explicit v1 event coverage gate so those paths cannot regress silently. [VERIFIED: backend/pipeline/storage/tests/test_feed_store.py; VERIFIED: backend/pipeline/storage/tests/test_sync_feed_store.py; VERIFIED: integration_tests/storage/test_feed_store_integration.py] |
| VER-02 | Automated tests verify transaction rollback behavior and concurrent per-feed event ordering. | Existing Testcontainers storage tests already cover rollback on audit constraint failure and concurrent same-feed sequence allocation; Phase 5 should keep these in the required verification command set. [VERIFIED: integration_tests/storage/test_feed_store_integration.py:1796; VERIFIED: integration_tests/storage/test_feed_store_integration.py:1902] |
| VER-03 | Automated tests verify diagnostic-detail lifecycle, public API migration away from `quarantine_reason`, and secret/detail bounding behavior. | Existing lifecycle, SQL contract, service, and API tests cover sanitizer behavior, public model shape, and actor/detail propagation; Phase 5 should group them into the v1 gate. [VERIFIED: backend/pipeline/storage/tests/test_feed_lifecycle.py:53; VERIFIED: backend/services/feeds/tests/test_api.py:81; VERIFIED: backend/services/feeds/tests/test_service.py:58] |
| VER-04 | Automated tests verify delete-survival and retention behavior. | Existing delete integration verifies audit rows survive hard delete; retention behavior still needs new DB-retention tests and static pg_cron migration tests. [VERIFIED: integration_tests/storage/test_feed_store_integration.py:2256; VERIFIED: backend/pipeline/storage/tests/test_feed_audit_contract.py] |
| VER-05 | Automated tests verify that lease churn and clean heartbeat or progress paths do not emit default audit events. | Existing async/sync storage tests cover clean progress, clean source observation, clean heartbeat, and runtime/Echo ownership guards; Phase 5 should make those tests part of the v1 gate. [VERIFIED: backend/pipeline/storage/tests/test_feed_store.py:1762; VERIFIED: backend/pipeline/storage/tests/test_sync_feed_store.py:591; VERIFIED: backend/pipeline/storage/tests/test_feed_query_contracts.py:601] |
</phase_requirements>

## Summary

Phase 5 should be planned as a hardening phase, not a feature-expansion phase: the only new mechanism is retention enforcement for `feed_audit_events`, and the rest of the work is organizing existing tests into an explicit v1 behavioral contract gate. [VERIFIED: .planning/phases/05-retention-and-verification-hardening/05-CONTEXT.md; VERIFIED: .planning/REQUIREMENTS.md]

Use a DB-owned retention path with `pg_cron` scheduling and bounded deletes keyed on `feed_audit_events.occurred_at`; this matches the locked Phase 5 decisions and the existing production-only `*pg_cron*` migration convention. [VERIFIED: .planning/phases/05-retention-and-verification-hardening/05-CONTEXT.md; VERIFIED: terraform/modules/alloydb/sql/ingestion/019_feeds_pg_cron_jobs.sql; CITED: https://docs.cloud.google.com/alloydb/docs/reference/extensions]

For testability, the best planning shape is a pure SQL retention procedure or function that can be exercised in Testcontainers plus a separate required `*pg_cron*` scheduler migration that calls it in AlloyDB. [VERIFIED: backend/pipeline/common/test_schema_helper.py; VERIFIED: integration_tests/conftest.py; CITED: https://github.com/citusdata/pg_cron; INFERRED] This preserves the local/CI skip rule for extension-dependent SQL while still letting retention semantics be tested against a real Postgres-compatible database. [VERIFIED: backend/pipeline/common/test_schema_helper.py; VERIFIED: local_dev/docker_postgres_init.sh; VERIFIED: .github/workflows/ci.yml:358]

**Primary recommendation:** Plan two work streams: retention SQL/scheduler plus a v1 verification gate that composes existing focused tests and adds missing retention-specific assertions. [VERIFIED: .planning/phases/05-retention-and-verification-hardening/05-CONTEXT.md; INFERRED]

## Project Constraints (from AGENTS.md)

- Read `.agents/instructions.md` before code changes or code review; it requires reading Python/JS style guides for code changes, but this research task only read the workspace instruction file because no implementation change is being made. [VERIFIED: AGENTS.md; VERIFIED: .agents/instructions.md]
- Default to targeted low-resource local checks; do not run broad local E2E, API, component, Docker, Testcontainers, or full integration-stack commands without explicit user confirmation. [VERIFIED: AGENTS.md; VERIFIED: .agents/instructions.md]
- For docs-only changes, use `git diff --check` instead of Python tests unless the user asks for tests. [VERIFIED: AGENTS.md]
- Prefer GitHub Actions for full E2E/resource-stack validation. [VERIFIED: AGENTS.md; VERIFIED: .agents/instructions.md]
- Use `safe-run -- <command>` for agent-run tests, builds, installs, browser/e2e runs, benchmarks, stress tests, and other potentially resource-heavy commands. [VERIFIED: user-provided AGENTS.md instructions]
- Prefer `mise` tasks for standard formatting, linting, generation, and tests, while still following local test safety rules. [VERIFIED: .agents/instructions.md]
- Do not use `--no-verify` for commits. [VERIFIED: .agents/instructions.md]
- The GSD workflow block says direct repo edits should happen through a GSD workflow unless explicitly bypassed; this file is itself a GSD research artifact requested by the phase workflow. [VERIFIED: AGENTS.md; VERIFIED: user objective]
- No project skill `SKILL.md` files were found under `.codex/skills` or `.agents/skills`; only `.agents/instructions.md` exists. [VERIFIED: rg --files .agents .codex]

## Architectural Responsibility Map

| Capability | Primary Tier | Secondary Tier | Rationale |
|------------|--------------|----------------|-----------|
| Audit retention scheduling | Database / Storage | Infrastructure | The locked mechanism is a DB-owned AlloyDB `pg_cron` job, and ingestion SQL migrations are the repo's schema deployment surface. [VERIFIED: 05-CONTEXT.md; VERIFIED: terraform/modules/alloydb/main.tf:98; CITED: https://docs.cloud.google.com/alloydb/docs/reference/extensions] |
| Expired audit-row deletion | Database / Storage | Test layer | The rows live in `feed_audit_events`, and retention must delete persisted rows without app/runtime audit insert paths. [VERIFIED: terraform/modules/alloydb/sql/ingestion/029_feed_audit_events.sql; VERIFIED: 05-CONTEXT.md] |
| Sequence-row pruning | Database / Storage | Test layer | The sequence table is storage-owned ordering metadata and must be pruned only after both current feed and retained audit rows are gone. [VERIFIED: terraform/modules/alloydb/sql/ingestion/029_feed_audit_events.sql; VERIFIED: 05-CONTEXT.md] |
| Admin/feed lifecycle event verification | Storage and API service | BFF/frontend for compatibility checks | Feed audit events are inserted by storage methods, while Phase 3 moved public diagnostic compatibility through backend service and BFF/frontend contracts. [VERIFIED: backend/pipeline/storage/feed_store.py:379; VERIFIED: backend/services/feeds/tests/test_api.py:81; VERIFIED: frontend/api/src/feeds/feedsController.ts:152] |
| Runtime/Echo event verification | Storage layer | Ingestion runtime / Echo handler | Runtime and Echo pass actor/prior-state inputs to storage; storage owns audit-row insertion. [VERIFIED: backend/pipeline/storage/feed_store.py:920; VERIFIED: backend/pipeline/storage/sync_feed_store.py:215; VERIFIED: .planning/phases/04-runtime-event-integration/04-VERIFICATION.md] |
| No-noise verification | Storage layer | Ingestion runtime tests | Clean progress/heartbeat/release paths must avoid allocating feed audit sequences or inserting audit rows. [VERIFIED: backend/pipeline/storage/tests/test_feed_store.py:1762; VERIFIED: backend/pipeline/storage/tests/test_sync_feed_store.py:591; VERIFIED: backend/pipeline/storage/tests/test_feed_query_contracts.py:53] |

## Standard Stack

### Core

| Library / Tool | Version | Purpose | Why Standard |
|----------------|---------|---------|--------------|
| AlloyDB for PostgreSQL | Managed PostgreSQL-compatible service | Production database and schema execution target. | Existing Terraform module applies ingestion SQL to AlloyDB, and Google documents `pg_cron` as a supported extension when `alloydb.enable_pg_cron=on`. [VERIFIED: terraform/modules/alloydb/main.tf:98; CITED: https://docs.cloud.google.com/alloydb/docs/reference/extensions] |
| `pg_cron` | Managed extension, repo does not pin extension version | Database-owned scheduled retention job. | Phase 5 locks retention to AlloyDB `pg_cron`; official pg_cron docs support named scheduled SQL commands and stored procedure calls. [VERIFIED: 05-CONTEXT.md; VERIFIED: ctx7 /citusdata/pg_cron; CITED: https://github.com/citusdata/pg_cron] |
| SQL migrations in `terraform/modules/alloydb/sql/ingestion/` | Lexical file order | Schema, procedures, and cron job registration. | Terraform uploads and applies sorted ingestion SQL files, and local/CI helpers apply the same files while skipping `*pg_cron*`. [VERIFIED: terraform/modules/alloydb/main.tf:115; VERIFIED: local_dev/docker_postgres_init.sh; VERIFIED: backend/pipeline/common/test_schema_helper.py] |
| Python `pytest` | 9.0.3 locked in `uv.lock` | Focused unit, contract, and integration tests. | Existing backend tests use pytest/unittest patterns and root pytest config. [VERIFIED: uv.lock; VERIFIED: uv run python import; VERIFIED: pyproject.toml] |
| `asyncpg` | 0.31.0 locked in `uv.lock` | Async FeedStore and Testcontainers storage integration. | `FeedStore` and integration tests use asyncpg pools and transactions. [VERIFIED: uv.lock; VERIFIED: uv run python import; VERIFIED: backend/pipeline/storage/feed_store.py] |
| `psycopg[binary]` | 3.3.3 locked in `uv.lock` | Sync Echo storage path. | `SyncFeedStore` uses psycopg-style sync transactions and SQL helpers. [VERIFIED: uv.lock; VERIFIED: uv run python import; VERIFIED: backend/pipeline/storage/sync_feed_store.py] |

### Supporting

| Library / Tool | Version | Purpose | When to Use |
|----------------|---------|---------|-------------|
| `testcontainers[postgres]` | 4.14.2 locked in `uv.lock` | Testcontainers-backed storage integration tests with AlloyDB Omni image. | Use for retention procedure semantics, rollback, concurrent ordering, and delete-survival checks when a prepared local or CI environment is available. [VERIFIED: uv.lock; VERIFIED: uv run python importlib.metadata; VERIFIED: integration_tests/conftest.py] |
| Docker | Engine 29.5.2 locally; Python SDK 7.1.0 locked | Runs Testcontainers storage database. | Use only when explicitly approved locally or in CI because repo instructions classify these lanes as resource-heavy. [VERIFIED: docker --version; VERIFIED: docker info; VERIFIED: uv.lock; VERIFIED: AGENTS.md] |
| `safe-run` | Installed, no version output | Host-stability wrapper for agent-run commands. | Use around test/build commands that may consume significant resources. [VERIFIED: command -v safe-run; VERIFIED: user-provided AGENTS.md instructions] |
| `mise` | 2026.3.18 locally; repo `.tool-versions` pins toolchain expectations | Task runner for standard test/lint/generation commands. | Use `mise run test:unit` for backend unit tests and `mise run test:component:feeds` only with approval/prepared environment. [VERIFIED: mise --version; VERIFIED: .mise.toml:264] |
| `uv` | 0.11.2 locally; `uv run python` resolves Python 3.13.2 | Python environment and command runner. | Use `uv run` or `mise` commands instead of system `python3`, because system `python3` is 3.12.13 while the project requires Python 3.13. [VERIFIED: python3 --version; VERIFIED: uv run python --version; VERIFIED: pyproject.toml] |

### Alternatives Considered

| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| DB-owned `pg_cron` job | Cloud Scheduler plus Cloud Run/worker cleanup | Rejected for Phase 5 because D-01 locks DB-owned `pg_cron`, and adding app/runtime cleanup expands the operational surface. [VERIFIED: 05-CONTEXT.md] |
| Bounded batch delete | Unbounded `DELETE FROM feed_audit_events WHERE occurred_at < ...` | Rejected because D-03 requires bounded deletion to avoid one large transaction during backlog cleanup. [VERIFIED: 05-CONTEXT.md] |
| Immutable sequence labels | Renumbering retained `feed_sequence` values after expiry | Rejected because D-06 says gaps after retention are expected and sequence labels must remain immutable. [VERIFIED: 05-CONTEXT.md] |
| Static-only retention tests | Real DB procedure tests plus static scheduler tests | Static-only tests miss SQL semantics; a pure retention procedure/function enables Testcontainers coverage while preserving `*pg_cron*` skip for extension-dependent scheduling. [VERIFIED: backend/pipeline/common/test_schema_helper.py; VERIFIED: integration_tests/conftest.py; INFERRED] |

**Installation:**

```bash
# No new package install is expected for Phase 5.
# Existing dependencies are locked in uv.lock and package metadata.
```

**Version verification:**

```bash
uv run python --version
uv run python -c "import pytest, asyncpg, psycopg; print(pytest.__version__, asyncpg.__version__, psycopg.__version__)"
uv run python -c "from importlib.metadata import version; print(version('testcontainers'), version('docker'))"
docker --version
```

These commands verified Python 3.13.2 under `uv run`, pytest 9.0.3, asyncpg 0.31.0, psycopg 3.3.3, testcontainers 4.14.2, Docker SDK 7.1.0, and Docker Engine 29.5.2. [VERIFIED: command output]

## Architecture Patterns

### System Architecture Diagram

```text
-----------------------------+
| Ingestion SQL migrations   |
| sql/ingestion/*.sql        |
+-------------+---------------+
              |
              v
+-----------------------------+       +------------------------------+
| Pure retention SQL helper   |       | pg_cron scheduler migration  |
| CALL prune_feed_audit...    |<------| cron.schedule(job, daily,    |
| no pg_cron dependency       |       |   'CALL helper()')           |
+-------------+---------------+       +------------------------------+
              |
              v
+-----------------------------+
| feed_audit_events           |
| delete expired rows by      |
| occurred_at, bounded batch  |
+-------------+---------------+
              |
              v
+-----------------------------+
| feed_audit_event_sequences  |
| prune only orphan sequence  |
| rows with no feed and no    |
| retained audit events       |
+-------------+---------------+
              |
              v
+-----------------------------+
| Verification gate           |
| static SQL contract tests   |
| storage unit tests          |
| Testcontainers retention    |
| tests in CI/prepared env    |
+-----------------------------+
```

This diagram reflects the recommended split between extension-free retention semantics and extension-dependent scheduling. [VERIFIED: local_dev/docker_postgres_init.sh; VERIFIED: backend/pipeline/common/test_schema_helper.py; VERIFIED: 05-CONTEXT.md; INFERRED]

### Recommended Project Structure

```text
terraform/modules/alloydb/sql/ingestion/
├── 031_feed_audit_event_retention.sql          # pure retention procedure/function, extension-free
└── 032_feed_audit_events_pg_cron_retention.sql # CREATE EXTENSION + cron.schedule daily job

backend/pipeline/storage/tests/
├── test_feed_audit_contract.py                 # static migration/doc invariants
├── test_feed_store.py                          # storage unit event/no-noise gate
├── test_sync_feed_store.py                     # Echo/sync event/no-noise gate
├── test_feed_query_contracts.py                # SQL ownership/no-noise/static guards
└── test_feed_lifecycle.py                      # diagnostic detail sanitizer bounds

integration_tests/storage/
└── test_feed_store_integration.py              # rollback, concurrency, delete-survival, retention procedure behavior
```

The proposed `031` and `032` numbers are recommendations based on the current highest ingestion migration `030_feed_audit_events_actor_constraint.sql`; the planner may choose exact names as long as the scheduler file includes `pg_cron`. [VERIFIED: terraform/modules/alloydb/sql/ingestion/030_feed_audit_events_actor_constraint.sql; VERIFIED: 05-CONTEXT.md; INFERRED]

### Pattern 1: Extension-Free Retention Procedure

**What:** Put the actual bounded delete and safe sequence pruning in SQL that does not require the `pg_cron` extension. [VERIFIED: backend/pipeline/common/test_schema_helper.py; INFERRED]

**When to use:** Use when retention behavior must be executable in Testcontainers/local CI while scheduler registration remains production-only. [VERIFIED: integration_tests/conftest.py; VERIFIED: local_dev/docker_postgres_init.sh; INFERRED]

**Example:**

```sql
-- Source: repository schema + Phase 5 decisions.
-- The exact function/procedure name and batch size are planner discretion.
CREATE OR REPLACE PROCEDURE public.prune_feed_audit_events_retention()
LANGUAGE plpgsql
AS $$
BEGIN
    WITH expired AS MATERIALIZED (
        SELECT id
        FROM public.feed_audit_events
        WHERE occurred_at < NOW() - INTERVAL '18 months'
        ORDER BY occurred_at, id
        LIMIT 10000
        FOR UPDATE SKIP LOCKED
    )
    DELETE FROM public.feed_audit_events events
    USING expired
    WHERE events.id = expired.id;

    WITH orphaned_sequences AS MATERIALIZED (
        SELECT sequences.feed_id
        FROM public.feed_audit_event_sequences sequences
        WHERE NOT EXISTS (
            SELECT 1
            FROM public.feeds feeds
            WHERE feeds.id = sequences.feed_id
        )
          AND NOT EXISTS (
            SELECT 1
            FROM public.feed_audit_events events
            WHERE events.feed_id = sequences.feed_id
        )
        ORDER BY sequences.feed_id
        LIMIT 10000
        FOR UPDATE SKIP LOCKED
    )
    DELETE FROM public.feed_audit_event_sequences sequences
    USING orphaned_sequences
    WHERE sequences.feed_id = orphaned_sequences.feed_id;
END;
$$;
```

The important invariants are the `occurred_at` cutoff, the 18-month interval, a bounded batch, no audit-row inserts, no sequence renumbering, and sequence pruning only after both the feed and retained audit rows are absent. [VERIFIED: 05-CONTEXT.md; VERIFIED: terraform/modules/alloydb/sql/ingestion/029_feed_audit_events.sql]

### Pattern 2: pg_cron Scheduler Migration

**What:** Register a named daily `pg_cron` job from a migration file whose filename includes `pg_cron`. [VERIFIED: terraform/modules/alloydb/sql/ingestion/019_feeds_pg_cron_jobs.sql; VERIFIED: local_dev/docker_postgres_init.sh]

**When to use:** Use for AlloyDB production scheduling only; local docker-compose, HOT guard CI, and Testcontainers schema helpers skip `*pg_cron*` files. [VERIFIED: local_dev/docker_postgres_init.sh; VERIFIED: .github/workflows/ci.yml:358; VERIFIED: backend/pipeline/common/test_schema_helper.py]

**Example:**

```sql
-- Source: pg_cron docs and existing repo migration style.
CREATE EXTENSION IF NOT EXISTS pg_cron;

SELECT cron.schedule(
    'feed-audit-events-retention',
    '15 3 * * *',
    'CALL public.prune_feed_audit_events_retention()'
);
```

Official pg_cron documentation supports `cron.schedule(job_name, schedule, command)` and examples include scheduling direct SQL deletes and stored procedure calls. [VERIFIED: ctx7 /citusdata/pg_cron; CITED: https://github.com/citusdata/pg_cron]

### Pattern 3: v1 Verification Gate by Existing Test Lanes

**What:** Build the Phase 5 verification gate by naming targeted existing tests plus new retention tests, instead of creating a broad unscoped suite. [VERIFIED: AGENTS.md; VERIFIED: .planning/codebase/TESTING.md; VERIFIED: 05-CONTEXT.md]

**When to use:** Use for all Phase 5 local verification; keep Testcontainers lanes CI/prepared-machine only unless explicitly approved. [VERIFIED: AGENTS.md; VERIFIED: .agents/instructions.md]

**Recommended command set:**

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

This command stays in low-resource backend unit/contract territory and avoids local Testcontainers unless separately approved. [VERIFIED: AGENTS.md; VERIFIED: .mise.toml:264; INFERRED]

### Anti-Patterns to Avoid

- **Scheduler-only retention SQL:** If the only copy of the retention SQL lives inside a skipped `*pg_cron*` migration, local/Testcontainers retention behavior cannot be executed directly. [VERIFIED: backend/pipeline/common/test_schema_helper.py; VERIFIED: local_dev/docker_postgres_init.sh; INFERRED]
- **Unbounded delete:** A plain delete with no batch limit violates D-03 and risks a large transaction after retention backlog pauses. [VERIFIED: 05-CONTEXT.md]
- **Deleting sequence rows for live feeds:** Future events for live feeds still need the transactionally allocated next sequence. [VERIFIED: 05-CONTEXT.md; VERIFIED: backend/pipeline/storage/feed_queries.py:469]
- **Renumbering retained events:** Retention gaps in `feed_sequence` are expected and must not be "fixed" by rewriting audit history. [VERIFIED: 05-CONTEXT.md]
- **Runtime or service direct audit inserts:** Storage is the only audit-row writer, and static guards already enforce that runtime/Echo sources do not reference `feed_audit_events`. [VERIFIED: backend/pipeline/storage/feed_store.py:379; VERIFIED: backend/pipeline/storage/tests/test_feed_query_contracts.py:601]

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Scheduled retention | App worker, shell cron, Cloud Scheduler cleanup endpoint | AlloyDB `pg_cron` scheduled SQL | Phase 5 locks DB-owned `pg_cron`, and pg_cron officially schedules SQL/procedure commands inside PostgreSQL-compatible databases. [VERIFIED: 05-CONTEXT.md; CITED: https://github.com/citusdata/pg_cron] |
| Retention batch limiting | Custom Python pagination loop | SQL CTE selecting a bounded batch, then `DELETE ... USING` | The retention job must delete one bounded batch per daily run and should not loop until empty. [VERIFIED: 05-CONTEXT.md; INFERRED] |
| Per-feed sequence recovery after retention | Sequence renumbering or `MAX(feed_sequence)+1` recomputation | Existing `feed_audit_event_sequences` allocator | The sequence allocator is already transactionally used by storage, and Phase 5 forbids removing or renumbering sequences. [VERIFIED: backend/pipeline/storage/feed_queries.py:469; VERIFIED: 05-CONTEXT.md] |
| Secret/detail filtering | New one-off retention redaction job | Existing storage-boundary sanitizer and delete-only retention | Retention must delete expired audit rows only; secret bounding already belongs to diagnostic detail persistence tests. [VERIFIED: 05-CONTEXT.md; VERIFIED: backend/pipeline/storage/feed_lifecycle.py:62] |
| Full v1 verification | New broad E2E suite | Existing targeted storage/service/runtime tests plus retention additions | The repo warns against broad local E2E/Docker lanes, and Phase 5 is contract hardening around already-built behavior. [VERIFIED: AGENTS.md; VERIFIED: .planning/phases/04-runtime-event-integration/04-VERIFICATION.md] |

**Key insight:** The hard part is not building a deletion loop; it is preserving the audit contract while proving that only the approved DB-owned mechanism can expire rows and that sequence metadata ages out safely. [VERIFIED: 05-CONTEXT.md; INFERRED]

## Common Pitfalls

### Pitfall 1: Putting All Retention SQL In a `*pg_cron*` File

**What goes wrong:** The migration is skipped by local docker-compose, HOT guard CI, and Testcontainers schema helpers, so retention semantics get only text-level validation. [VERIFIED: local_dev/docker_postgres_init.sh; VERIFIED: .github/workflows/ci.yml:358; VERIFIED: backend/pipeline/common/test_schema_helper.py]

**Why it happens:** The repo intentionally skips `*pg_cron*` files because vanilla Postgres images lack the extension. [VERIFIED: local_dev/docker_postgres_init.sh]

**How to avoid:** Put extension-free retention semantics in an ordinary migration and schedule that helper from a separate `*pg_cron*` migration. [INFERRED]

**Warning signs:** No integration test can execute the production retention SQL without enabling `pg_cron`. [INFERRED]

### Pitfall 2: Using `created_at` Instead of `occurred_at`

**What goes wrong:** Expiry follows insert time rather than domain event time, violating D-02. [VERIFIED: 05-CONTEXT.md]

**Why it happens:** The audit schema has both `occurred_at` and `created_at`, and `created_at` can look like the operational timestamp. [VERIFIED: terraform/modules/alloydb/sql/ingestion/029_feed_audit_events.sql]

**How to avoid:** Static contract tests should assert the retention SQL contains `occurred_at < NOW() - INTERVAL '18 months'` and does not use `created_at` as the cutoff. [VERIFIED: 05-CONTEXT.md; INFERRED]

**Warning signs:** Retention SQL mentions `created_at` in its `WHERE` clause. [INFERRED]

### Pitfall 3: Sequence Pruning That Breaks Future Live-Feed Events

**What goes wrong:** A live feed whose old audit rows expired loses its sequence row, causing future events to restart sequence allocation at `1` or collide. [VERIFIED: 05-CONTEXT.md; VERIFIED: backend/pipeline/storage/feed_queries.py:469]

**Why it happens:** It is tempting to delete sequence rows whenever no audit events remain. [INFERRED]

**How to avoid:** Prune `feed_audit_event_sequences` only when no `feeds` row exists and no retained `feed_audit_events` row exists. [VERIFIED: 05-CONTEXT.md]

**Warning signs:** Sequence cleanup SQL lacks both `NOT EXISTS (SELECT 1 FROM feeds ...)` and `NOT EXISTS (SELECT 1 FROM feed_audit_events ...)`. [INFERRED]

### Pitfall 4: Treating Retention Gaps as Data Corruption

**What goes wrong:** Tests or future timeline logic assume the first retained event has `feed_sequence = 1`. [VERIFIED: 05-CONTEXT.md]

**Why it happens:** Existing concurrent ordering tests assert contiguous sequences before retention, but Phase 5 retention explicitly permits gaps after expiry. [VERIFIED: integration_tests/storage/test_feed_store_integration.py:1902; VERIFIED: 05-CONTEXT.md]

**How to avoid:** Add a retention test where an expired early event is deleted while a later retained event keeps its original sequence. [INFERRED]

**Warning signs:** A test recomputes retained event sequences as `range(1, n + 1)` after retention. [INFERRED]

### Pitfall 5: Running Resource-Heavy Tests by Default

**What goes wrong:** Local Docker/Testcontainers or broad integration commands can violate repo safety rules and exhaust the developer machine. [VERIFIED: AGENTS.md; VERIFIED: .agents/instructions.md]

**Why it happens:** The root pytest config uses `-n auto`, and component tests start Testcontainers. [VERIFIED: pyproject.toml; VERIFIED: integration_tests/conftest.py]

**How to avoid:** Plan low-resource unit/contract commands separately from CI/prepared-machine component commands. [VERIFIED: AGENTS.md; INFERRED]

**Warning signs:** A Phase 5 task says to run `uv run pytest integration_tests/` locally without explicit approval. [VERIFIED: AGENTS.md; INFERRED]

## Code Examples

Verified patterns from official and repository sources:

### Schedule a Named pg_cron Job

```sql
-- Source: ctx7 /citusdata/pg_cron and existing 019_feeds_pg_cron_jobs.sql.
SELECT cron.schedule(
    'feed-audit-events-retention',
    '15 3 * * *',
    'CALL public.prune_feed_audit_events_retention()'
);
```

`cron.schedule` accepts a named job, cron expression, and SQL command, and official examples schedule both direct deletes and procedure calls. [VERIFIED: ctx7 /citusdata/pg_cron; CITED: https://github.com/citusdata/pg_cron]

### Static Retention Contract Test Shape

```python
# Source: backend/pipeline/storage/tests/test_feed_audit_contract.py pattern.
def test_retention_migration_uses_pg_cron_and_bounded_occurred_at_cutoff() -> None:
    path = _REPO_ROOT / "terraform/modules/alloydb/sql/ingestion/032_feed_audit_events_pg_cron_retention.sql"
    text = path.read_text(encoding="utf-8")
    normalized = _normalized_sql(text)

    assert "pg_cron" in path.name
    assert "cron.schedule" in normalized
    assert "feed-audit-events-retention" in normalized
    assert "18 months" in normalized
    assert "occurred_at" in normalized
    assert "LIMIT" in normalized
    assert "INSERT INTO feed_audit_events" not in normalized
```

This mirrors the repository's existing static contract test style for migration/documentation invariants. [VERIFIED: backend/pipeline/storage/tests/test_feed_audit_contract.py]

### Retention Integration Test Shape

```python
# Source: integration_tests/storage/test_feed_store_integration.py helpers.
async def test_retention_prunes_expired_events_and_safe_orphan_sequences(
    db_pool: asyncpg.Pool,
) -> None:
    feed_id = await _insert_feed(db_pool, "Retention Feed")
    await db_pool.execute(
        """
        INSERT INTO feed_audit_event_sequences (feed_id, next_sequence)
        VALUES ($1, 4)
        ON CONFLICT (feed_id) DO UPDATE SET next_sequence = EXCLUDED.next_sequence
        """,
        feed_id,
    )
    await db_pool.execute(
        """
        INSERT INTO feed_audit_events
            (feed_id, feed_name, source_type, action, actor_id,
             occurred_at, feed_sequence, before_values, after_values)
        VALUES
            ($1, 'Retention Feed', 'bcfy_feeds', 'feed.created',
             'service:feeds-service', NOW() - INTERVAL '19 months',
             1, '{}'::jsonb, '{}'::jsonb),
            ($1, 'Retention Feed', 'bcfy_feeds', 'feed.updated',
             'service:feeds-service', NOW() - INTERVAL '17 months',
             2, '{}'::jsonb, '{}'::jsonb)
        """,
        feed_id,
    )

    await db_pool.execute("CALL public.prune_feed_audit_events_retention()")

    rows = await _fetch_audit_events(db_pool, feed_id)
    assert [row["feed_sequence"] for row in rows] == [2]
    assert await _get_audit_sequence_next(db_pool, feed_id) == 4
```

The retained sequence remaining at `2` verifies that retention does not renumber surviving events. [VERIFIED: 05-CONTEXT.md; INFERRED]

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| Retention target documented but unenforced | DB-owned `pg_cron` retention job | Phase 5 scope | Planner must add schema-level enforcement, not only documentation. [VERIFIED: documentation/feed-audit-events.md; VERIFIED: 05-CONTEXT.md] |
| Runtime retention cleanup by application code | AlloyDB scheduled SQL | Phase 5 locked decision | Avoids service/runtime paths becoming audit deletion owners. [VERIFIED: 05-CONTEXT.md] |
| Deriving per-feed order from current audit rows | `feed_audit_event_sequences` allocator | Phase 2 decision | Retention can delete old rows without rewriting future sequence allocation. [VERIFIED: .planning/phases/02-transactional-storage-writes/02-CONTEXT.md; VERIFIED: backend/pipeline/storage/feed_queries.py:469] |
| Public diagnostic detail via `quarantine_reason` | Canonical `status_reason_detail` | Phase 3 and Phase 4 | Phase 5 verification should assert public API migration and storage sanitizer coverage, not reintroduce alias behavior. [VERIFIED: .planning/phases/03-service-and-compatibility-surface/03-CONTEXT.md; VERIFIED: backend/services/feeds/tests/test_api.py:81] |
| Runtime/Echo audit behavior as future work | Runtime/Echo failure, quarantine, recovery, and no-noise semantics implemented | Phase 4 | Phase 5 should verify the built behavior rather than redefine event semantics. [VERIFIED: .planning/phases/04-runtime-event-integration/04-VERIFICATION.md] |

**Deprecated/outdated:**

- `quarantine_reason` as public canonical diagnostic detail is outdated for new public code; Phase 3 moved public contracts to `status_reason_detail`. [VERIFIED: .planning/phases/03-service-and-compatibility-surface/03-CONTEXT.md; VERIFIED: backend/services/feeds/tests/test_api.py:81]
- `system:` actor namespace is outdated; Phase 2 removed it before audit rows were emitted. [VERIFIED: .planning/STATE.md; VERIFIED: terraform/modules/alloydb/sql/ingestion/030_feed_audit_events_actor_constraint.sql]
- One-file scheduler-only retention is not recommended because it weakens executable retention verification under the repo's `*pg_cron*` skip convention. [VERIFIED: backend/pipeline/common/test_schema_helper.py; INFERRED]

## Assumptions Log

| # | Claim | Section | Risk if Wrong |
|---|-------|---------|---------------|
| A1 | A two-migration shape, with an extension-free retention helper plus a `*pg_cron*` scheduler migration, is the best way to satisfy both D-01 and D-13. | Summary, Architecture Patterns | If the team wants exactly one migration, retention semantics may need to be static-tested only locally or require an AlloyDB-enabled CI lane. |
| A2 | The v1 retention batch size is fixed at `LIMIT 10000`; static tests should assert this exact constant. | Code Examples, Open Questions (RESOLVED) | If production audit volume is much larger or rows are large, a future phase can tune from production metrics without changing Phase 5's locked plan. |

## Open Questions (RESOLVED)

1. **Exact retention batch size**
   - What we know: D-03 requires a bounded batch and D-04 requires one bounded batch per daily run. [VERIFIED: 05-CONTEXT.md]
   - Resolution: Phase 5 uses `LIMIT 10000` exactly in the extension-free retention procedure and asserts that token in static tests. This value is the approved v1 constant; production-metric tuning is out of scope for this phase.

2. **Whether to add an AlloyDB-enabled cron execution lane**
   - What we know: local/CI vanilla Postgres intentionally skips `*pg_cron*` migrations. [VERIFIED: local_dev/docker_postgres_init.sh; VERIFIED: .github/workflows/ci.yml:358; VERIFIED: backend/pipeline/common/test_schema_helper.py]
   - Resolution: Scheduler verification is a required prepared AlloyDB or CI/prepared-machine lane. The lane must pass before the scheduler checkpoint is marked complete; if it is not run or does not pass, execution must leave an explicit pending UAT item and must not count the checkpoint as complete.

## Environment Availability

| Dependency | Required By | Available | Version | Fallback |
|------------|-------------|-----------|---------|----------|
| `uv run python` | Backend unit/contract tests | yes | Python 3.13.2 | Use `mise run` tasks that call `uv`. [VERIFIED: uv run python --version; VERIFIED: .mise.toml] |
| System `python3` | Direct Python commands | yes, wrong major/minor | 3.12.13 | Do not use for project tests; use `uv run python`. [VERIFIED: python3 --version; VERIFIED: pyproject.toml] |
| `uv` | Python env/test runner | yes | 0.11.2 | `mise` can wrap repo tasks, but local uv differs from `.tool-versions` expectation. [VERIFIED: uv --version; VERIFIED: .tool-versions] |
| `mise` | Standard task runner | yes | 2026.3.18 | Direct `uv run` targeted commands. [VERIFIED: mise --version; VERIFIED: .mise.toml] |
| Docker engine | Testcontainers storage integration | yes | 29.5.2 | Prefer CI or explicit local approval for Testcontainers lanes. [VERIFIED: docker --version; VERIFIED: docker info --format; VERIFIED: AGENTS.md] |
| `psql` CLI | Manual SQL checks / CI-like HOT guard locally | no | - | Use Testcontainers through pytest or GitHub Actions; CI service image includes `psql` in the workflow context. [VERIFIED: psql --version; VERIFIED: .github/workflows/ci.yml:358] |
| `safe-run` | Host-stability wrapper | yes | no version output | Use it as command prefix for tests/builds; no fallback needed for simple reads. [VERIFIED: command -v safe-run; VERIFIED: user-provided AGENTS.md instructions] |
| Node/Yarn | Frontend/BFF compatibility tests if included | yes | Node 22.22.2, Yarn 1.22.22 | Phase 5 likely only needs existing BFF/unit tests if planner includes them. [VERIFIED: node --version; VERIFIED: yarn --version; VERIFIED: frontend/api/src/feeds/feedsController.test.ts] |

**Missing dependencies with no fallback:**

- None for low-resource Phase 5 research and unit/contract planning. [VERIFIED: environment probes]

**Missing dependencies with fallback:**

- `psql` is missing locally; use pytest/Testcontainers for DB semantics or GitHub Actions for CI SQL guards. [VERIFIED: psql --version; VERIFIED: integration_tests/conftest.py; VERIFIED: .github/workflows/ci.yml:358]

## Security Domain

### Applicable ASVS Categories

| ASVS Category | Applies | Standard Control |
|---------------|---------|------------------|
| V2 Authentication | no new auth in Phase 5 | Keep existing trusted actor tests in the v1 gate; do not add retention APIs. [VERIFIED: 05-CONTEXT.md; VERIFIED: backend/services/feeds/tests/test_service.py:37] |
| V3 Session Management | no | Phase 5 does not change sessions or frontend auth flows. [VERIFIED: 05-CONTEXT.md] |
| V4 Access Control | yes, indirectly | Retention should be DB-owned scheduled maintenance, not a public service endpoint. [VERIFIED: 05-CONTEXT.md; CITED: https://github.com/citusdata/pg_cron] |
| V5 Input Validation | yes | Diagnostic details remain bounded and sanitized at the storage boundary; tests should stay in the v1 gate. [VERIFIED: backend/pipeline/storage/feed_lifecycle.py:62; VERIFIED: backend/pipeline/storage/tests/test_feed_lifecycle.py:61] |
| V6 Cryptography | no new crypto | Do not hand-roll crypto; Phase 5 does not add webhook signatures or delivery. [VERIFIED: .planning/REQUIREMENTS.md] |
| V7 Error Handling and Logging | yes | Do not persist raw credential-bearing exception text; diagnostic sanitizer tests already cover common credential patterns. [VERIFIED: backend/pipeline/storage/feed_lifecycle.py:16; VERIFIED: backend/pipeline/storage/tests/test_feed_lifecycle.py:61] |
| V14 Configuration | yes | AlloyDB `pg_cron` requires the `alloydb.enable_pg_cron` flag before `CREATE EXTENSION`. [CITED: https://docs.cloud.google.com/alloydb/docs/reference/alloydb-flags; VERIFIED: terraform/modules/alloydb/sql/ingestion/019_feeds_pg_cron_jobs.sql] |

### Known Threat Patterns for Feed Audit Retention

| Pattern | STRIDE | Standard Mitigation |
|---------|--------|---------------------|
| Unauthorized deletion of audit rows through app code | Tampering / Repudiation | Keep deletion in DB-owned retention SQL and avoid public/admin retention endpoints. [VERIFIED: 05-CONTEXT.md; INFERRED] |
| Secret retention in diagnostic detail | Information Disclosure | Keep storage-boundary sanitizer and cap tests in the v1 verification gate. [VERIFIED: backend/pipeline/storage/feed_lifecycle.py:62; VERIFIED: backend/pipeline/storage/tests/test_feed_lifecycle.py:61] |
| Large retention backlog causing long locks | Denial of Service | Use one bounded delete batch per daily cron run. [VERIFIED: 05-CONTEXT.md] |
| Audit ordering ambiguity after expiry | Repudiation | Preserve immutable `feed_sequence` labels and document expected gaps. [VERIFIED: 05-CONTEXT.md] |
| Scheduler misconfiguration | Reliability / Tampering | Static tests should assert job name, schedule, command, filename convention, and 18-month cutoff. [VERIFIED: terraform/modules/alloydb/sql/ingestion/019_feeds_pg_cron_jobs.sql; VERIFIED: backend/pipeline/storage/tests/test_feed_audit_contract.py; INFERRED] |

## Sources

### Primary (HIGH confidence)

- `.planning/phases/05-retention-and-verification-hardening/05-CONTEXT.md` - locked Phase 5 retention and verification decisions.
- `.planning/REQUIREMENTS.md` - AUD-05 and VER-01 through VER-05 requirement text.
- `.planning/STATE.md` - current milestone state and prior decisions.
- `AGENTS.md` and `.agents/instructions.md` - local test safety, GSD, and workflow constraints.
- `terraform/modules/alloydb/sql/ingestion/019_feeds_pg_cron_jobs.sql` - existing pg_cron migration pattern and skip convention.
- `terraform/modules/alloydb/sql/ingestion/029_feed_audit_events.sql` - audit table, sequence table, `occurred_at`, indexes, and constraints.
- `terraform/modules/alloydb/sql/ingestion/030_feed_audit_events_actor_constraint.sql` - current actor constraint and sequence backfill pattern.
- `backend/pipeline/common/test_schema_helper.py` - test schema helper that skips `*pg_cron*`.
- `local_dev/docker_postgres_init.sh` and `.github/workflows/ci.yml` - local/CI skip behavior for pg_cron migrations.
- `backend/pipeline/storage/feed_store.py`, `feed_queries.py`, and `sync_feed_store.py` - storage-owned audit insertion and runtime/Echo action gates.
- `backend/pipeline/storage/tests/test_feed_audit_contract.py`, `test_feed_store.py`, `test_sync_feed_store.py`, `test_feed_query_contracts.py`, and `test_feed_lifecycle.py` - existing contract/unit coverage patterns.
- `integration_tests/storage/test_feed_store_integration.py` and `integration_tests/conftest.py` - Testcontainers storage semantics and existing rollback/concurrency/delete-survival coverage.
- Context7 `/citusdata/pg_cron` - pg_cron scheduling syntax and examples.
- Google AlloyDB flags and supported extensions docs - `alloydb.enable_pg_cron` requirement and pg_cron support.

### Secondary (MEDIUM confidence)

- `documentation/feed-audit-events.md` - domain contract, with retention text still describing Phase 5 as future enforcement before this phase.
- `.planning/codebase/TESTING.md`, `.planning/codebase/ARCHITECTURE.md`, and `.planning/codebase/CONCERNS.md` - codebase maps and known concerns generated on 2026-06-19.
- `.planning/phases/04-runtime-event-integration/04-VERIFICATION.md` - verified Phase 4 behavior and remaining Echo Docker/Testcontainers UAT.

### Tertiary (LOW confidence)

- None. All major claims were checked against local files, current command output, Context7, or official Google/pg_cron documentation.

## Metadata

**Confidence breakdown:**

- Standard stack: HIGH - versions were verified from `uv.lock`, `uv run` imports, and local tool probes; pg_cron behavior was checked with Context7 and official docs.
- Architecture: HIGH - responsibility boundaries come from locked Phase 5 decisions and existing repo schema/test patterns.
- Pitfalls: HIGH - pitfalls are derived from explicit repo skip conventions, existing tests, and locked retention decisions.

**Research date:** 2026-06-20
**Valid until:** 2026-07-20 for repo-local patterns; re-check AlloyDB/pg_cron docs before changing extension configuration.
