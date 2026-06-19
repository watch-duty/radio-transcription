# Architecture Research

**Domain:** Durable feed audit events for the existing current-state feed model
**Researched:** 2026-06-19
**Confidence:** HIGH

## Recommended Architecture

Feed Audit Events V1 should add an append-only `feed_audit_events` ledger beside the existing `feeds` current-state table. The `feeds` row remains authoritative for current lifecycle, lease, failure, and diagnostic state. Audit events explain successful meaningful mutations after they happen; they must not become the replay source for current state and must not replace the lease/fencing model.

### System Overview

```text
Admin UI / BFF
    |
    v
FastAPI feed service
`backend/services/feeds/*`
    |
    v
FeedService supplies actor context
    |
    v
FeedStore audited mutation methods
`backend/pipeline/storage/feed_store.py`
    |
    +--> current state: `feeds`, `feed_properties`
    |
    `--> append-only history: `feed_audit_events`

VM ingestion runtime
`backend/pipeline/ingestion/collector_runtime.py`
    |
    v
Fenced FeedStore failure/recovery writes
    |
    +--> current state update
    `--> audit event in same SQL mutation

Echo ingestion runtime
`backend/pipeline/ingestion/collectors/echo/main.py`
    |
    v
SyncFeedStore audited heartbeat/failure writes
    |
    +--> current state update
    `--> audit event in same sync SQL mutation
```

### Core Decision

Use a store-owned audit ledger, not full event sourcing.

`feeds` remains the source of current truth for:

- claimability and leasing: `status`, `worker_id`, `fencing_token`, `last_heartbeat`
- failure policy: `failure_count`, `retry_after`, `status_reason`
- operator diagnostics: new `status_reason_detail`, compatibility `quarantine_reason`
- source progress: `last_processed_filename`, `last_bookmark_time`

`feed_audit_events` becomes the source of durable mutation history for later operator timelines and webhook delivery. Future readers consume this table; current runtime code never reconstructs `feeds` from events.

## Component Boundaries

| Component | Responsibility | Write Ownership |
|-----------|----------------|-----------------|
| AlloyDB migrations | Add `status_reason_detail`, `feed_audit_events`, indexes, and retention job | Schema only |
| `FeedStore` | Own async feed mutations and audit inserts for VM runtime and FastAPI service | Primary writer for `feeds` and `feed_audit_events` |
| `SyncFeedStore` | Own Echo heartbeat/failure current-state writes and matching audit inserts | Secondary writer for Echo only |
| `FeedService` | Validate service inputs, derive/pass actor context, return API models | No direct SQL |
| FastAPI feed routes | Accept admin mutations, pass actor context, preserve existing response compatibility | No direct SQL |
| Ingestion runtime | Classify failure/recovery paths and call existing store methods | No direct audit writes outside store |
| BFF/API proxy | Preserve admin gate and forward end-user actor context for human actions | No direct AlloyDB writes |
| Docs/types | Define durable event contract and compatibility fields | No writes |

The audit table should have exactly two application write owners in V1: `FeedStore` and `SyncFeedStore`. Do not let collectors, FastAPI route handlers, BFF controllers, or future webhook dispatch code insert audit rows directly. Those layers can pass actor/cause metadata into store methods, but the store must keep current-state mutation and audit insert atomic.

## Database Schema

### `feeds.status_reason_detail`

Add a nullable current diagnostic detail field:

```sql
ALTER TABLE feeds
    ADD COLUMN IF NOT EXISTS status_reason_detail TEXT;

ALTER TABLE feeds
    ADD CONSTRAINT feeds_status_reason_detail_length
    CHECK (
        status_reason_detail IS NULL
        OR length(status_reason_detail) <= 2048
    );
```

Recommended behavior:

- `status_reason` stays the canonical bounded code.
- `status_reason_detail` becomes the canonical human diagnostic detail for the current abnormal condition.
- `quarantine_reason` remains populated as a compatibility alias for one release, especially when a feed enters `quarantined`.
- Recovery/reset clears `status_reason`, `status_reason_detail`, and the compatibility `quarantine_reason`.
- The storage-boundary cap should move from quarantine-only naming to a general status-detail helper while preserving `MAX_QUARANTINE_REASON_LENGTH = 2048` compatibility.

Do not add an index on `feeds.status_reason_detail`. Also extend `terraform/modules/alloydb/sql/ci/hot_protection_check.sql` to guard this column, and preferably `status_reason` / `status_reason_updated_at`, against future feed indexes. These fields are mutated on failure and recovery paths; indexing them would make future hot-path changes easy to get wrong.

### `feed_audit_events`

Recommended V1 table:

```sql
CREATE TABLE IF NOT EXISTS feed_audit_events (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    feed_id UUID NOT NULL,
    feed_sequence BIGINT NOT NULL,
    action TEXT NOT NULL,
    occurred_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    actor_type TEXT NOT NULL,
    actor_id TEXT,
    actor_context JSONB NOT NULL DEFAULT '{}'::jsonb,
    source_type TEXT,
    feed_name TEXT,
    source_feed_id TEXT,
    status TEXT,
    status_reason TEXT,
    status_reason_detail TEXT,
    before_values JSONB NOT NULL DEFAULT '{}'::jsonb,
    after_values JSONB NOT NULL DEFAULT '{}'::jsonb,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    CONSTRAINT feed_audit_events_sequence_unique
        UNIQUE (feed_id, feed_sequence),
    CONSTRAINT feed_audit_events_detail_length
        CHECK (
            status_reason_detail IS NULL
            OR length(status_reason_detail) <= 2048
        ),
    CONSTRAINT feed_audit_events_action_check
        CHECK (action IN (
            'feed.created',
            'feed.updated',
            'feed.deactivated',
            'feed.reset',
            'feed.deleted',
            'feed.failure_reported',
            'feed.quarantined',
            'feed.recovered'
        )),
    CONSTRAINT feed_audit_events_actor_type_check
        CHECK (actor_type IN ('user', 'runtime', 'system'))
);
```

Do not add a foreign key from `feed_audit_events.feed_id` to `feeds.id` in V1. Current `delete_feed` is a hard delete of the feed row and related data. A cascade would delete the audit event; a restrict FK would block existing delete semantics. Preserve `feed_id`, `feed_name`, `source_type`, and `source_feed_id` as event data so a deletion remains explainable after the current row is gone.

Recommended indexes:

```sql
CREATE INDEX IF NOT EXISTS feed_audit_events_feed_timeline_idx
    ON feed_audit_events (feed_id, occurred_at DESC, id DESC);

CREATE INDEX IF NOT EXISTS feed_audit_events_retention_idx
    ON feed_audit_events (occurred_at);
```

Avoid action/status/source indexes in V1 because there is no read API yet. Add those only when a real query needs them. Audit-table indexes do not affect HOT updates on `feeds`, but they still add write cost on every audited mutation.

## Event Contract

| Action | Writer Path | Before / After Values | Notes |
|--------|-------------|-----------------------|-------|
| `feed.created` | `FeedStore.create_feed` | before `{}`, after feed snapshot | Sequence is always `1` for a new feed. |
| `feed.updated` | `FeedStore.update_feed` | changed config fields, especially `name`, `tags` | Do not emit for no-op full updates. |
| `feed.deactivated` | `FeedStore.deactivate_feed` | lifecycle fields | Keep external success for existing already-deactivated feeds, but emit only on actual transition. |
| `feed.reset` | `FeedStore.reset_feed` | lifecycle, failure, diagnostic fields | Human recovery action; clear detail fields. |
| `feed.deleted` | `FeedStore.delete_feed` | full safe feed snapshot, after `{}` or `{"deleted": true}` | Insert event before deleting current rows. |
| `feed.failure_reported` | `FeedStore.report_feed_failure`, `release_non_budgeted_failure`, `SyncFeedStore.record_failure` | status, failure count, retry, reason fields | Covers budgeted and non-budgeted persisted failures. |
| `feed.quarantined` | same call as threshold-crossing failure | same fields as failure | Emit this instead of a duplicate `feed.failure_reported` for the threshold-crossing mutation. |
| `feed.recovered` | `update_feed_progress`, `record_source_observation`, `SyncFeedStore.record_heartbeat` | dirty failure state before, clean state after | Emit only when previous row had failure state/detail. Do not audit routine progress or heartbeat. |

Every event should include:

- `feed_id`, `feed_sequence`, `occurred_at`
- `action`
- `actor_type`, `actor_id`, `actor_context`
- feed identity snapshot: `feed_name`, `source_type`, `source_feed_id`
- current outcome snapshot: `status`, `status_reason`, `status_reason_detail`
- bounded `before_values` / `after_values`

Do not store secrets, raw credential-bearing exception strings, provider response bodies, or unbounded payloads. Keep diagnostic detail behind the same 2048-character cap used for quarantine text today.

## Data Flow

### Human Feed Mutation

```text
React UI
  -> BFF feed controller verifies admin
  -> BFF forwards trusted actor context
  -> FastAPI feed route calls FeedService
  -> FeedService calls FeedStore with actor context
  -> FeedStore runs one audited mutation
      -> locks/updates current feed state
      -> computes next per-feed sequence
      -> inserts feed_audit_events row
      -> returns existing API response
```

FastAPI feed routes currently depend on `verify_oidc_token` at app level and do not pass user claims into `FeedService`. V1 should change feed mutation routes to accept explicit auth dependency output, mirroring the rules service pattern, and/or accept trusted actor headers from the BFF. The backend should not blindly trust arbitrary actor headers from direct callers; the safe boundary is "BFF authenticated the user and backend authenticated the BFF/service caller."

### Runtime Failure Or Quarantine

```text
Collector raises FeedFailure or runtime raises _PipelineFailure
  -> CollectorRuntime classifies policy action
  -> _releasing_feeds is marked before the awaited store write
  -> FeedStore.report_feed_failure or release_non_budgeted_failure
      -> fenced WHERE worker_id + fencing_token + active status
      -> updates current failure state
      -> writes exactly one audit event
      -> returns status/action diagnostics
  -> quarantine telemetry emits only after store success and only for quarantined
```

Preserve the existing `_releasing_feeds` invariant. Do not add a second post-mutation audit await in the runtime. The audited mutation should remain a single awaited store operation so the heartbeat loop does not observe a feed as lost between current-state mutation and audit insert.

### Runtime Recovery

```text
Successful chunk progress or source observation
  -> FeedStore update clears stale failure state
  -> if previous row was dirty:
       insert feed.recovered
     else:
       no audit event
```

This records meaningful recovery without polluting the ledger with ordinary heartbeat, lease, and bookmark churn.

### Echo Runtime

```text
Echo GCS finalize event
  -> SyncFeedStore.resolve_echo_feed
  -> successful publish calls SyncFeedStore.record_heartbeat
      -> writes feed.recovered only if dirty
  -> failure calls SyncFeedStore.record_failure
      -> writes feed.failure_reported or feed.quarantined
```

Echo is not leased by the VM runtime, so V1 must update `SyncFeedStore` separately. Otherwise Echo failures and recoveries will be absent from the audit ledger.

## Storage Pattern

Prefer audited SQL CTE mutations over "update first, insert audit later" Python code.

The existing feed store mostly uses single-statement CTEs and `pool.fetchrow`/`pool.execute`, which is a good fit for PgBouncer transaction-mode pooling and the current test style. V1 should keep each meaningful mutation atomic in one SQL statement where practical:

```sql
WITH before_state AS MATERIALIZED (
    SELECT ...
    FROM feeds f
    JOIN feed_properties fp ON fp.feed_id = f.id
    WHERE f.id = $1
    FOR UPDATE
),
updated AS (
    UPDATE feeds
       SET ...
      FROM before_state
     WHERE feeds.id = before_state.id
     RETURNING ...
),
next_sequence AS (
    SELECT COALESCE(MAX(feed_sequence), 0) + 1 AS value
    FROM feed_audit_events
    WHERE feed_id = $1
),
audit AS (
    INSERT INTO feed_audit_events (...)
    SELECT ..., next_sequence.value, ...
    FROM before_state, updated, next_sequence
    WHERE meaningful_change_predicate
    RETURNING id
)
SELECT ...;
```

The row lock on `feeds` serializes concurrent mutations for the same feed before computing `MAX(feed_sequence) + 1`. The unique `(feed_id, feed_sequence)` constraint remains the backstop. Create and delete paths should also lock or own the feed row while computing the sequence. Do not introduce an independent audit-writer call that can succeed after a current-state mutation has failed or fail after current state has committed.

For complex delete SQL, insert the audit event from a locked `before_state` CTE before deleting `audio_segments`, `transcripts`, and `feeds`. Because the audit row has no FK to `feeds`, it survives the hard delete.

## Recommended Project Structure

```text
backend/pipeline/storage/
|-- feed_audit.py              # action/actor dataclasses, safe JSON helpers
|-- feed_audit_queries.py      # shared SQL fragments/constants only if they reduce duplication
|-- feed_queries.py            # audited feed mutation SQL remains close to feed mutations
|-- feed_store.py              # async audited store methods
`-- sync_feed_store.py         # Echo audited sync methods

backend/services/feeds/
|-- models.py                  # expose status_reason_detail while keeping quarantine_reason
|-- service.py                 # pass actor context to store methods
`-- main.py                    # derive actor context from auth/BFF boundary

terraform/modules/alloydb/sql/ingestion/
|-- 029_feed_audit_events.sql
`-- 030_feed_audit_events_retention_pg_cron.sql

documentation/
`-- feed-audit-events.md       # durable event contract and compatibility policy
```

Use `feed_audit.py` for small typed helpers only. Keep the authoritative mutation logic in the feed store and query modules so reviewers can reason about current-state and audit changes together.

## Retention

V1 requires 18-month retention. Enforce it with a pg_cron migration whose filename contains `pg_cron`, matching the existing migration convention:

```sql
SELECT cron.schedule(
    'feed-audit-events-retention',
    '0 9 * * *',
    $$
    WITH expired AS MATERIALIZED (
        SELECT id
        FROM public.feed_audit_events
        WHERE occurred_at < NOW() - INTERVAL '18 months'
        ORDER BY occurred_at
        LIMIT 5000
        FOR UPDATE SKIP LOCKED
    )
    DELETE FROM public.feed_audit_events e
    USING expired
    WHERE e.id = expired.id;
    $$
);
```

Keep this separate from the table/index migration so local tests and CI continue to skip `*pg_cron*` files on vanilla Postgres, as they already do for `019_feeds_pg_cron_jobs.sql`.

## Build Order And Dependencies

1. **Schema foundation**
   - Add `status_reason_detail`.
   - Add `feed_audit_events`, constraints, and minimal indexes.
   - Add retention pg_cron migration.
   - Extend HOT guard for the new current-state diagnostic field.

2. **Storage contract helpers**
   - Add feed audit action/actor helpers.
   - Convert `FeedStore` create/update/deactivate/reset/delete to audited mutations.
   - Convert failure/quarantine/recovery paths in `FeedStore`.
   - Convert Echo heartbeat/failure paths in `SyncFeedStore`.

3. **Service/runtime integration**
   - Pass actor context from FastAPI feed service to storage methods.
   - Pass runtime actor context from `CollectorRuntime` and Echo.
   - Preserve existing external API status codes and response shapes.

4. **Compatibility surface**
   - Expose `status_reason_detail` in backend models.
   - Keep `quarantine_reason` in responses for one release.
   - Update BFF/shared TypeScript types to carry both fields.
   - Keep UI reading `quarantineReason` until a later UI cleanup can switch to `statusReasonDetail`.

5. **Documentation and tests**
   - Document event contract and terminology.
   - Add migration-contract tests, store unit tests, sync-store tests, and integration transactionality tests.
   - Update OpenAPI/frontend enum/type contract tests as needed.

This order matters: storage cannot write audit events until schema exists; services should not pass actor context until storage accepts it; docs should land with the contract once schema and actions are stable.

## Risk Controls

### Leasing And Fencing

- Keep audit writes inside the same fenced SQL as `report_feed_failure`, `release_non_budgeted_failure`, `update_feed_progress`, and `record_source_observation`.
- Do not audit `acquire_feeds_batch`, `acquire_feeds_recovery`, `renew_heartbeats_batch_diagnostic`, `release_feed`, or `release_feeds_batch`.
- Do not move `_releasing_feeds.add(...)` later in `CollectorRuntime`; it must still happen before the awaited store call that clears worker ownership.
- Continue returning `None`/false when fenced writes lose ownership. A failed fence must not insert an audit event.

### HOT-Safe Feed Writes

- Add no new `feeds` indexes for audit.
- Keep `status_reason_detail` nullable, with no default and no backfill.
- Add audit indexes only on `feed_audit_events`.
- Run and maintain `.github/workflows/ci.yml` HOT-protection guard when changing feed indexes or mutated feed columns.
- If a future query needs `feeds.status_reason_detail`, build a separate read path or audit-table query instead of indexing the hot current-state row.

### Compatibility

- Backend API should return both `status_reason_detail` and `quarantine_reason` for one release.
- Existing `quarantine_reason` consumers must keep working.
- New code should write/read `status_reason_detail` as canonical detail; `quarantine_reason` is a compatibility alias, not a control-flow field.
- BFF should map unknown future `status_reason` values to `unknown`, as it does today, but detail text should pass through as bounded display text.

### Delete Durability

- Do not use `ON DELETE CASCADE` from audit events to feeds.
- Insert `feed.deleted` before hard delete.
- Store enough feed identity in the event row to inspect deletion after `feeds` and `feed_properties` are gone.

## Anti-Patterns To Avoid

### Full Event Sourcing

**What:** Rebuild `feeds` from `feed_audit_events` or make runtime leasing consult the audit ledger.
**Why bad:** Existing lease, heartbeat, recovery, and failure paths are current-state SQL with HOT-sensitive performance constraints.
**Instead:** Keep `feeds` current-state authoritative and write audit events as transactionally consistent history.

### Post-Commit Audit Insert

**What:** Call a generic `insert_audit_event()` after a feed mutation succeeds.
**Why bad:** A second write can fail independently, leaving current state without history.
**Instead:** Use audited CTE mutations or an explicit single transaction on one connection when a CTE is not practical.

### Auditing Lease Churn

**What:** Emit events for every claim, heartbeat, release, and progress update.
**Why bad:** It creates high-volume scheduler noise and hides the useful lifecycle story.
**Instead:** Emit only meaningful mutation events and recovery events when dirty state is cleared.

### Feed FK On Audit Rows

**What:** Add a strict or cascading FK from `feed_audit_events.feed_id` to `feeds.id`.
**Why bad:** Current hard delete either fails or removes the deletion history.
**Instead:** Store feed identity snapshots in the audit row and let application-owned writes enforce validity.

## Test Strategy

| Area | Tests |
|------|-------|
| Schema | Migration-contract test for nullable/detail fields, audit table constraints, no `feeds` index, pg_cron filename convention |
| HOT safety | Existing HOT guard plus expanded guarded column list |
| Async store | Unit tests asserting audited SQL writes event only on successful/meaningful mutation |
| Integration | AlloyDB-backed tests proving current-state mutation and audit insert commit/rollback together |
| Sync store | Echo `record_failure` and dirty `record_heartbeat` audit coverage |
| Runtime | `CollectorRuntime` tests for failure, quarantine, and recovery events without lease invariant regressions |
| API compatibility | Backend model/API tests for both `status_reason_detail` and `quarantine_reason` |
| BFF/types | Shared type and controller mapping tests for both fields and actor forwarding |

## Sources

- `.planning/PROJECT.md`
- `.planning/codebase/ARCHITECTURE.md`
- `.planning/codebase/STRUCTURE.md`
- `.planning/codebase/CONCERNS.md`
- `backend/pipeline/storage/feed_store.py`
- `backend/pipeline/storage/feed_queries.py`
- `backend/pipeline/storage/sync_feed_store.py`
- `backend/pipeline/ingestion/collector_runtime.py`
- `backend/pipeline/ingestion/models.py`
- `backend/pipeline/ingestion/collectors/echo/main.py`
- `backend/pipeline/ingestion/collectors/README.md`
- `backend/services/feeds/main.py`
- `backend/services/feeds/service.py`
- `backend/services/feeds/models.py`
- `frontend/api/src/feeds/feedsController.ts`
- `frontend/common/src/types/feeds.ts`
- `terraform/modules/alloydb/sql/ingestion/003_feeds.sql`
- `terraform/modules/alloydb/sql/ingestion/017_feeds_hot_storage_tuning.sql`
- `terraform/modules/alloydb/sql/ingestion/018_feeds_hot_indexes.sql`
- `terraform/modules/alloydb/sql/ingestion/019_feeds_pg_cron_jobs.sql`
- `terraform/modules/alloydb/sql/ingestion/020_quarantine_reason.sql`
- `terraform/modules/alloydb/sql/ingestion/024_feeds_status_reason.sql`
- `terraform/modules/alloydb/sql/ci/hot_protection_check.sql`
- `backend/pipeline/common/test_schema_helper.py`

---
*Architecture research for: Feed Audit Events V1*
*Researched: 2026-06-19*
