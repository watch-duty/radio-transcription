# Stack Research

**Domain:** Feed Audit Events V1 in the existing radio-transcription backend
**Researched:** 2026-06-19
**Confidence:** HIGH

## Recommended Stack

### Core Technologies

| Technology | Version | Purpose | Why Recommended | Confidence |
|------------|---------|---------|-----------------|------------|
| Python | >=3.13,<3.14 | Backend storage, ingestion runtime, FastAPI feed service | The root backend workspace already targets Python 3.13 and all feed mutation paths are Python. Keep audit writes in the same language/runtime as `FeedStore`, `SyncFeedStore`, and `backend/services/feeds`. | HIGH |
| AlloyDB / PostgreSQL | AlloyDB Omni 15 in component tests; production AlloyDB via Terraform | Durable audit ledger, current `feeds.status_reason_detail`, retention | Feed current state already lives in AlloyDB and existing feed mutations are SQL-first. Audit events must commit with feed mutations, so the database is the correct durability boundary. | HIGH |
| asyncpg | >=0.29.0 | Async storage access for VM ingestion and FastAPI service paths | `FeedStore` uses asyncpg pools and parameterized SQL constants today. Extend that pattern for audit inserts rather than introducing another ORM or client. | HIGH |
| psycopg v3 | >=3.2.0 | Sync storage access for Echo ingestion | Echo bypasses the VM runtime and uses `SyncFeedStore`; V1 failure/recovery audit coverage must include this sync path. | HIGH |
| FastAPI + Pydantic | FastAPI >=0.110.0, Pydantic >=2.10.6 | Feed service compatibility for `status_reason_detail` | `backend/services/feeds` already exposes feed CRUD/status fields through Pydantic response models. Add the compatibility field here; do not add audit read APIs in V1. | HIGH |
| TypeScript BFF shared types | Node 22, TypeScript 6, Express 5, tsoa 7 alpha | Preserve browser/BFF feed contract compatibility | Existing UI/BFF consumes `quarantineReason` and `statusReason` through `frontend/common` and `frontend/api/src/feeds`. Add `statusReasonDetail` only if compatibility responses need it; do not build timeline UI. | HIGH |
| Terraform-managed SQL migrations | Existing `terraform/modules/alloydb/sql/ingestion/*.sql` | Schema changes and pg_cron retention schedule | Production schema application already uploads ordered SQL files to GCS and runs a Cloud Run Job with `psql`. Reuse this exactly. | HIGH |

### Storage And Migration Mechanisms

| Mechanism | Recommended Use | Specific Files / Directories | Rationale | Confidence |
|-----------|-----------------|------------------------------|-----------|------------|
| Ordered ingestion SQL migration | Add `029_feed_audit_events.sql` for table, counters, `status_reason_detail`, indexes, and any retention helper SQL that must be testable without pg_cron | `terraform/modules/alloydb/sql/ingestion/` | The existing migration system is filename-ordered, idempotent SQL. The next unambiguous number after `028_initialize_feed_bookmarks.sql` is `029`. | HIGH |
| Separate pg_cron migration | Add `030_feed_audit_events_pg_cron.sql` for the scheduled 18-month retention job | `terraform/modules/alloydb/sql/ingestion/` | Files containing `pg_cron` are intentionally skipped in local Docker, integration-test schema helpers, and CI hot-protection setup because vanilla Postgres lacks the extension. Keep that convention. | HIGH |
| Durable audit table | Create `feed_audit_events` in AlloyDB with immutable rows | `terraform/modules/alloydb/sql/ingestion/029_feed_audit_events.sql` | Project context explicitly chooses durable domain audit data, not logs or webhook payloads. | HIGH |
| Per-feed sequence counter table | Add a small `feed_audit_event_counters(feed_id uuid primary key, next_sequence bigint not null)` or equivalent allocator | Same migration plus SQL CTEs in storage queries | Do not compute sequence with `MAX(sequence)+1`; concurrent admin/runtime mutations can race. An `INSERT ... ON CONFLICT DO UPDATE ... RETURNING next_sequence - 1` CTE gives atomic per-feed sequence allocation. | HIGH |
| No FK from audit rows to `feeds` | Store `feed_id uuid not null` as data, not as a cascading foreign key | `feed_audit_events` DDL | `delete_feed` is audit-worthy and currently hard-deletes the feed row. A cascading FK would erase the history V1 is adding. | HIGH |
| JSONB value snapshots | Store `before_values jsonb` and `after_values jsonb` | `feed_audit_events` DDL and storage SQL | The project requirements ask for before/after values, and existing SQL already returns joined feed/property state. JSONB avoids schema churn for every feed field while preserving queryability. | HIGH |
| Bounded diagnostic detail | Add `feeds.status_reason_detail text` and keep `feeds.quarantine_reason` populated as a compatibility alias for one release | `029_feed_audit_events.sql`, `backend/pipeline/storage/feed_store.py`, `backend/pipeline/storage/sync_feed_store.py` | Existing `quarantine_reason` is a quarantine-only detail string. V1 needs a canonical current diagnostic detail that also covers non-quarantine failures. | HIGH |
| Retention by occurred time | Delete audit rows older than 18 months using a scheduled pg_cron job | `030_feed_audit_events_pg_cron.sql` | Retention is an active V1 requirement, not just documentation. Put production enforcement in the same pg_cron mechanism already used for feed maintenance. | HIGH |

Recommended `feed_audit_events` shape:

```sql
CREATE TABLE IF NOT EXISTS feed_audit_events (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    feed_id UUID NOT NULL,
    sequence BIGINT NOT NULL,
    action TEXT NOT NULL,
    occurred_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    actor_type TEXT NOT NULL,
    actor_id TEXT,
    source_type TEXT,
    source_feed_id TEXT,
    status TEXT,
    status_reason TEXT,
    status_reason_detail TEXT,
    before_values JSONB NOT NULL DEFAULT '{}'::jsonb,
    after_values JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    UNIQUE (feed_id, sequence)
);
```

Use indexes for the expected future timeline and retention paths:

- `(feed_id, sequence DESC)` or `(feed_id, occurred_at DESC, sequence DESC)` for per-feed history.
- `(occurred_at)` for retention deletes.
- Avoid indexes on hot `feeds` columns. Adding unindexed `feeds.status_reason_detail` should not affect the existing HOT-protection guard.

### Storage Layer

| Mechanism | Recommended Use | Specific Files / Directories | Rationale | Confidence |
|-----------|-----------------|------------------------------|-----------|------------|
| `FeedStore` | Keep async feed audit writes in the existing store layer | `backend/pipeline/storage/feed_store.py`, `backend/pipeline/storage/feed_queries.py` | The existing service and VM runtime already mutate feeds only through `FeedStore`; this is the right transaction boundary for create, update, deactivate, reset, delete, failure, quarantine, and recovery. | HIGH |
| `SyncFeedStore` | Add parallel audit writes for Echo heartbeat/recovery and failure/quarantine | `backend/pipeline/storage/sync_feed_store.py` | Echo is an event-driven Cloud Function path and does not call async `FeedStore`. Missing this path would leave feed failures and recoveries incomplete. | HIGH |
| Single-statement CTEs or explicit transactions | Write feed mutation and audit insert atomically | `CREATE_FEED_SQL`, `UPDATE_FEED_SQL`, `DEACTIVATE_FEED_SQL`, `DELETE_FEED_SQL`, `RESET_FEED_SQL`, `REPORT_FAILURE_SQL`, `RELEASE_NON_BUDGETED_FAILURE_SQL`, `UPDATE_PROGRESS_SQL`, `RECORD_SOURCE_OBSERVATION_SQL`, sync SQL constants | Existing SQL is mostly single-statement CTE style. Preserve that where possible; use explicit `asyncpg` / `psycopg` transactions only when the CTE becomes too opaque. | HIGH |
| Domain action enum | Add a Python `StrEnum` or constants for action names | Prefer `backend/pipeline/storage/feed_store.py` or a small storage-adjacent module | Actions should be domain events such as `feed.created`, `feed.updated`, `feed.deactivated`, `feed.reset`, `feed.deleted`, `feed.failure_reported`, `feed.quarantined`, and `feed.recovered`; do not encode HTTP route names. | HIGH |
| Storage-bound detail cap | Use the existing storage-boundary cap pattern, generalized for `status_reason_detail` | Existing: `backend/pipeline/storage/quarantine_reason.py`; likely add a neutral helper or alias | The repo already caps `quarantine_reason` at persistence time, not in collectors. Keep that behavior for the new canonical detail and keep raw unbounded provider bodies out. | HIGH |
| Recovery events | Emit `feed.recovered` when dirty failure state is cleared by successful progress, source observation, reset, or Echo heartbeat | `UPDATE_PROGRESS_SQL`, `RECORD_SOURCE_OBSERVATION_SQL`, `RESET_FEED_SQL`, `_HEARTBEAT_SQL` | Existing code clears `status_reason` and failure counts on these paths. Audit should record the meaningful recovery, not routine lease release. | HIGH |

Implementation guidance:

- Capture `before_values` from the locked row before mutation and `after_values`
  from the updated row in the same statement/transaction.
- For `report_feed_failure`, emit exactly one audit event per successful
  failure record: `feed.quarantined` when the threshold is crossed, otherwise
  `feed.failure_reported`. This matches the project decision not to double-log
  failure plus quarantine for the same outcome.
- For non-budgeted failures, still emit `feed.failure_reported`; those are
  visible operational failures even though they do not consume quarantine
  budget.
- Do not audit routine lease churn: `acquire_feeds_batch`,
  `acquire_feeds_recovery`, `release_feed`, `release_feeds_batch`, and
  heartbeat renewal should remain noise-free unless they clear a dirty status
  reason as a recovery.

### API And Actor Mechanisms

| Mechanism | Recommended Use | Specific Files / Directories | Rationale | Confidence |
|-----------|-----------------|------------------------------|-----------|------------|
| Existing feed FastAPI service | Add `status_reason_detail` to feed responses and service models | `backend/services/feeds/models.py`, `backend/services/feeds/service.py`, `backend/services/feeds/main.py` | Existing consumers already get feed status fields through this service. Keep compatibility there. | HIGH |
| Existing BFF feed controller | Forward admin actor context for create/update/deactivate/reset/delete and map `status_reason_detail` if surfaced | `frontend/api/src/feeds/feedsController.ts` | The BFF has `request.user.email`; backend services currently see a service ID token. Use this existing boundary rather than new auth infrastructure. | MEDIUM |
| Internal actor headers | Add narrowly scoped headers such as `X-WD-Actor-Type` and `X-WD-Actor-Id` from the BFF service call | `frontend/api/src/feeds/feedsController.ts`, `backend/services/feeds/main.py` | This is the smallest change to capture human actor identity. Only trust headers set by the authenticated BFF/service-to-service path, not browser-supplied raw headers. | MEDIUM |
| System actor constants | Use explicit actor values for runtime paths | `backend/pipeline/ingestion/collector_runtime.py`, `backend/pipeline/ingestion/collectors/echo/main.py` | Runtime failures and recoveries are system-generated. Store them as `actor_type='system'` with stable actor IDs such as `ingestion-runtime` or `echo-ingestion`. | HIGH |
| No audit read API in V1 | Do not add `/feed-audit-events` endpoints yet | N/A | Project scope says admin timeline read APIs and frontend UI are deferred. V1 is write-only durable data plus feed compatibility field. | HIGH |

### Test Mechanisms

| Test Type | Recommended Use | Specific Files / Commands | Rationale | Confidence |
|-----------|-----------------|---------------------------|-----------|------------|
| Backend unit tests | Validate SQL shape, action selection, sequence allocation parameters, detail cap, response model fields | `backend/pipeline/storage/tests/test_feed_store.py`, `backend/pipeline/storage/tests/test_sync_feed_store.py`, `backend/services/feeds/tests/test_api.py`; run `safe-run -- uv run python -m pytest backend/pipeline/storage/tests/test_feed_store.py backend/pipeline/storage/tests/test_sync_feed_store.py backend/services/feeds/tests/test_api.py -q` | Existing unit tests already inspect feed SQL and mock stores/API. This is the lowest-cost regression net. | HIGH |
| Ingestion runtime unit tests | Validate failure/quarantine/recovery hooks do not double-write and do not audit lease churn | `backend/pipeline/ingestion/tests/test_collector_runtime.py` and collector failure tests as needed | Runtime owns policy routing and is where many non-admin audit events originate. | HIGH |
| Storage component tests | Validate transactional behavior against AlloyDB Omni | `integration_tests/storage/test_feed_store_integration.py`; run only when approved: `safe-run -- mise run test:component:feeds` | Existing component tests apply migrations and exercise actual SQL. Needed for before/after JSONB, hard delete audit preservation, retention helper SQL, and sequence concurrency. | HIGH |
| API/BFF unit tests | Validate `status_reason_detail` compatibility and actor header forwarding | `frontend/api/src/feeds/feedsController.test.ts`; run `safe-run -- yarn --cwd frontend/api test --watch=false frontend/api/src/feeds/feedsController.test.ts` | BFF is the human actor source for admin feed mutations. | MEDIUM |
| UI service tests | Only update if `statusReasonDetail` is surfaced to browser types | `frontend/transcription-ui/src/service/listFeeds.test.ts`, `getFeed.test.ts`, create/update/reset/deactivate/delete tests as applicable | Do not add UI behavior, but keep type mapping stable if the field is exposed. | MEDIUM |
| Docs-only validation | Use diff whitespace check | `git diff --check` | Repo instructions say docs-only work should avoid broad test lanes. | HIGH |

Do not proactively run unscoped `uv run pytest`, `mise run test:component`,
`mise run test:api`, `mise run test:e2e`, or Docker Compose E2E locally.
Those lanes are explicitly resource-heavy in `AGENTS.md`.

### Documentation Mechanisms

| Mechanism | Recommended Use | Specific Files / Directories | Rationale | Confidence |
|-----------|-----------------|------------------------------|-----------|------------|
| Project terminology | Add Feed Audit Event, status reason detail, action vocabulary, actor vocabulary | `CONTEXT.md` | This repo already centralizes feed/status/quarantine terms here. Future WD delivery/admin timeline phases need stable vocabulary. | HIGH |
| Collector/runtime guide | Update only for failure/recovery event behavior and detail-field semantics | `backend/pipeline/ingestion/collectors/README.md`, `backend/pipeline/README.md` | This guide is the source for failure classification and runtime ownership. It should state that audit writes are runtime/store-owned, not collector-owned. | HIGH |
| Migration comments | Document retention and pg_cron filename requirement in SQL files | New SQL migrations under `terraform/modules/alloydb/sql/ingestion/` | Existing migrations use comments to capture operational invariants. Retention and pg_cron skip behavior are load-bearing. | HIGH |
| No product/UI docs in V1 | Do not create admin timeline docs yet | N/A | Timeline reads and frontend UI are out of scope for this milestone. | HIGH |

## Installation

No new dependency installation is recommended.

Use existing repo commands:

```bash
# Targeted backend/unit validation
safe-run -- uv run python -m pytest backend/pipeline/storage/tests/test_feed_store.py backend/pipeline/storage/tests/test_sync_feed_store.py backend/services/feeds/tests/test_api.py -q

# Standard backend unit lane, still low-resource compared with component/e2e
safe-run -- mise run test:unit

# Storage component lane only when explicitly approved
safe-run -- mise run test:component:feeds

# Frontend API controller tests if actor/status field mapping changes
safe-run -- yarn --cwd frontend/api test --watch=false frontend/api/src/feeds/feedsController.test.ts

# Docs-only / planning-only validation
git diff --check
```

## Alternatives Considered

| Recommended | Alternative | Why Not | Confidence |
|-------------|-------------|---------|------------|
| AlloyDB table `feed_audit_events` | Cloud Logging / log-based audit | Logs are short-lived, not transactionally tied to feed mutations, and cannot reliably back future admin timeline queries. | HIGH |
| Domain audit data | Store future webhook payloads as canonical rows | Project context explicitly warns against HTTP-shaped canonical storage; future delivery should derive payloads from durable domain events. | HIGH |
| Existing SQL migration workflow | Alembic or another migration framework | The repo already has ordered idempotent SQL applied by Terraform/Cloud Run and replayed in tests. A new migration tool would add process split-brain. | HIGH |
| Existing `FeedStore` / `SyncFeedStore` | SQLAlchemy ORM or separate repository layer | Current feed behavior is hand-written SQL with asyncpg/psycopg. ORM adoption would be broad and unnecessary for scoped audit writes. | HIGH |
| Per-feed sequence counter table | `MAX(sequence)+1` in `feed_audit_events` | `MAX+1` races under concurrent admin/runtime mutations. Use an atomic upsert counter. | HIGH |
| Store-owned audit writes | Database triggers | Triggers would struggle to capture actor identity, domain action names, and carefully bounded diagnostic detail. Existing store methods already know the action and context. | MEDIUM |
| Write-only V1 | New admin timeline read API/UI | Project scope explicitly defers timeline read APIs and frontend UI. Adding them would expand roadmap scope. | HIGH |
| Current FastAPI/BFF auth boundary with actor forwarding | New auth service or browser direct backend calls | Existing architecture routes browser calls through the BFF and backend services validate service OIDC. Actor forwarding is sufficient for V1 admin mutations. | MEDIUM |

## What NOT to Use

| Avoid | Why | Use Instead | Confidence |
|-------|-----|-------------|------------|
| Pub/Sub, Eventarc, or webhook workers for V1 audit persistence | V1 is durable write-only audit data, not delivery. Async dispatch introduces retries, outbox state, and failure modes outside scope. | Synchronous transactional AlloyDB insert with the feed mutation. | HIGH |
| Full event sourcing | The project explicitly keeps `feeds` as current-state source of truth. | Append-only audit ledger plus current `feeds` row. | HIGH |
| Cascading FK from audit events to `feeds` | Hard delete would remove the audit history. | Store feed identity fields in audit rows without cascade. | HIGH |
| Auditing lease acquire/release/heartbeat churn | It would flood the ledger with scheduler mechanics and obscure meaningful operator events. | Audit meaningful feed mutations and dirty-state recoveries only. | HIGH |
| Unbounded exception/provider text | Security and storage risk; project constraints forbid secrets/tokens/raw credential-bearing strings. | Storage-bound cap and scrubbed, diagnostic-only `status_reason_detail`. | HIGH |
| New database or cache | Adds operational surface and cannot improve atomicity over the existing feed transaction. | AlloyDB. | HIGH |
| New protobuf contract | Feed Audit Events V1 is not a pipeline Pub/Sub message contract. | SQL schema and Python/TypeScript service models where needed. | HIGH |
| Broad local Docker/E2E tests by default | Repo instructions identify these as resource-heavy. | Targeted unit tests and opt-in `test:component:feeds`. | HIGH |

## Stack Patterns by Variant

**Admin feed mutations (`create`, `update`, `deactivate`, `reset`, `delete`):**

- Use BFF `request.user.email` as the human actor.
- Forward actor context to the FastAPI feed service through internal headers on the service-to-service request.
- In `FeedService` / `FeedStore`, pass actor context into the store method and insert the audit row in the same SQL statement/transaction as the mutation.
- For `delete_feed`, insert audit data before deleting dependent/current rows and preserve feed identity fields in the audit event.

**VM ingestion failures and recoveries:**

- Use `CollectorRuntime` policy routes as the source of audit actions.
- Emit `feed.failure_reported` for successful budgeted and non-budgeted failure stores.
- Emit `feed.quarantined` instead of a separate failure event when `report_feed_failure` returns `quarantined`.
- Emit `feed.recovered` when successful progress/source observation clears dirty failure state.
- Do not emit audit events for ordinary active/unclaimed lease transitions.

**Echo ingestion:**

- Update `SyncFeedStore.record_failure` to write `feed.failure_reported` or `feed.quarantined` with `actor_type='system'`.
- Update `SyncFeedStore.record_heartbeat` to emit `feed.recovered` only when it clears existing dirty failure state.
- Keep Echo parity explicit because it does not use async `FeedStore`.

**Retention:**

- Put retention helper/query in non-pg_cron SQL if it needs component-test coverage.
- Put the production schedule in `*_pg_cron.sql` so local Docker and CI skip behavior remains intact.
- Use bounded delete batches if expected volume could be large.

## Version Compatibility

| Package / Tool | Compatible With | Notes | Confidence |
|----------------|-----------------|-------|------------|
| Python >=3.13,<3.14 | Root backend and feeds service packages | Use existing syntax/style and Ruff target `py313`. | HIGH |
| asyncpg >=0.29.0 | `FeedStore` async queries | Use `$1`-style parameters and async pool methods. | HIGH |
| psycopg[binary] >=3.2.0 | `SyncFeedStore` Echo queries | Use `%s` parameters and explicit connection scopes. | HIGH |
| FastAPI >=0.110.0 + Pydantic >=2.10.6 | Feed service models | Add fields to Pydantic models with snake_case backend JSON. | HIGH |
| Node 22 + TypeScript 6 | BFF/shared types | Keep `@transcription/common` type mapping and TSOA controller tests aligned. | HIGH |
| pg_cron | Production AlloyDB only | Migrations requiring it must include `pg_cron` in the filename; tests/local Docker skip those files. | HIGH |

## Sources

- `.planning/PROJECT.md` - Feed Audit Events V1 requirements, scope, and key decisions.
- `.planning/codebase/STACK.md` - existing runtime, package manager, frameworks, and test tools.
- `.planning/codebase/ARCHITECTURE.md` - feed service/store boundaries and ingestion runtime ownership.
- `.planning/codebase/CONVENTIONS.md` - Python/TypeScript naming, style, and error handling conventions.
- `.planning/codebase/TESTING.md` - test organization and safe command guidance.
- `backend/pipeline/storage/feed_store.py` and `backend/pipeline/storage/feed_queries.py` - async feed mutation SQL and status reason handling.
- `backend/pipeline/storage/sync_feed_store.py` - Echo sync feed mutation path.
- `backend/pipeline/ingestion/collector_runtime.py` and `backend/pipeline/ingestion/failure_policy.py` - failure, quarantine, non-budgeted retry, and recovery decision points.
- `backend/services/feeds/*` - FastAPI feed API and Pydantic response models.
- `frontend/api/src/feeds/feedsController.ts` and `frontend/common/src/types/feeds.ts` - BFF/shared feed contract and user actor availability.
- `terraform/modules/alloydb/main.tf` and `terraform/modules/alloydb/sql/ingestion/*` - production migration mechanism and current feed schema.
- `backend/pipeline/common/test_schema_helper.py`, `integration_tests/conftest.py`, `local_dev/docker_postgres_init.sh`, `.github/workflows/ci.yml` - schema replay, pg_cron skip behavior, and HOT-protection guard.
- `CONTEXT.md`, `backend/pipeline/README.md`, `backend/pipeline/ingestion/collectors/README.md` - repository terminology and failure-classification docs.

---
*Stack research for: Feed Audit Events V1*
*Researched: 2026-06-19*
