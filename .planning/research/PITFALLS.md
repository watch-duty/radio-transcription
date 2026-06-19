# Pitfalls Research

**Domain:** Feed Audit Events V1 in the radio-transcription ingestion/runtime/storage code
**Researched:** 2026-06-19
**Confidence:** HIGH for repo-specific risks based on `.planning` context and source inspection; MEDIUM for proposed phase names because the roadmap is not written yet.

## Recommended Phase Names Used Below

The roadmap can rename these, but the prevention work should stay grouped this way:

1. **Contract and Schema** - durable event table, action vocabulary, per-feed sequence, current `status_reason_detail`, retention DDL, and event contract docs.
2. **Transactional Storage Writes** - atomic feed mutation plus audit insert in `FeedStore` and delete/reset/update/create SQL.
3. **Service and Compatibility Surface** - FastAPI/BFF actor propagation, existing feed response compatibility, generated OpenAPI/shared type updates.
4. **Runtime Event Integration** - ingestion failure/quarantine/recovery event emission, `SyncFeedStore`/Echo parity, and exclusion of lease churn.
5. **Retention and Verification** - pg_cron retention, HOT/query checks, storage integration tests, concurrency tests, and docs verification.

## Critical Pitfalls

### Pitfall 1: Writing Audit Events Outside the Feed Mutation Transaction

**What goes wrong:**
The feed row changes but the audit row is missing, or the audit row commits even though the feed mutation later fails. This is the highest-risk failure because the new feature's purpose is durable reconstruction of what happened.

**Why it happens:**
Existing service methods in `backend/services/feeds/service.py` already log after `deactivate_feed`, `delete_feed`, and `reset_feed`; it is tempting to add audit writes there. Existing store methods mostly execute one SQL statement through `pool.fetchrow()` or `pool.execute()`, so adding a second call after the existing SQL creates drift unless both calls are explicitly transactional.

**How to avoid:**
Put audit insertion behind store-layer methods, not service-layer logging. Prefer one data-modifying CTE per mutation that captures `before`, performs the mutation, inserts exactly one audit event, and returns the updated row. If a single CTE is too awkward, use `async with conn.transaction()` around both statements and add rollback tests. For `SyncFeedStore`, account for `connect_db(..., autocommit=True)` by using a single SQL statement or changing the connection/transaction behavior deliberately.

**Warning signs:**
- A service method calls `store.update_*()` and then separately calls `store.insert_audit_event()`.
- Tests assert only that an audit event exists after success, but never inject audit insert failure and verify the feed row rolls back.
- Audit insert helpers accept arbitrary feed IDs without seeing the same locked row that is being mutated.

**Phase to address:**
Phase 2: Transactional Storage Writes.

---

### Pitfall 2: Losing Per-Feed Sequence Integrity Under Concurrency

**What goes wrong:**
Two concurrent operations on the same feed produce duplicate sequence numbers, gaps that cannot be explained, or events ordered differently from the committed feed states.

**Why it happens:**
The requirement asks for a per-feed sequence. A naive `SELECT max(sequence) + 1` is unsafe unless all writers serialize on the same feed row or another per-feed lock. The current system has concurrent VM workers, heartbeat/recovery paths, admin mutation endpoints, and an Echo Cloud Run path.

**How to avoid:**
Add `UNIQUE (feed_id, sequence)` and generate the sequence inside the same transaction that locks/mutates the feed row. For feed mutations, the current row update can be the serialization point if every audited write locks the row first. For delete, capture and lock the row before removing it. Add a concurrency storage test with parallel update/reset/failure attempts on the same feed and assert strictly increasing sequences.

**Warning signs:**
- Sequence is computed in Python before the SQL mutation.
- Sequence uses a global database sequence and roadmap still claims "per-feed sequence".
- No unique constraint exists on `(feed_id, sequence)`.
- Delete audit event generation happens after the feed row is gone.

**Phase to address:**
Phase 1 for constraints and schema; Phase 2 for transactional allocation; Phase 5 for concurrency verification.

---

### Pitfall 3: Letting Lease Churn Pollute the Audit Ledger

**What goes wrong:**
The audit table fills with `active`, `unclaimed`, heartbeat, lease release, and worker handoff records. The history becomes noisy, expensive, and unhelpful for operators.

**Why it happens:**
The `feeds.status` column mixes product lifecycle state with runtime scheduling state. `active` and `unclaimed` are lease mechanics; `failing`, `quarantined`, `deactivated`, and explicit admin edits are closer to operator-facing history. The runtime touches lease state frequently through `acquire_feeds_batch`, `renew_heartbeats_batch_diagnostic`, `release_feed`, `release_feeds_batch`, and pg_cron abandoned-lease sweep.

**How to avoid:**
Use explicit audit calls only in meaningful mutation paths. Do not add a database trigger on all `feeds` updates. Write negative tests proving no audit event is emitted for claim, heartbeat, release, shutdown batch release, clean bookmark progress, or abandoned-lease sweep.

**Warning signs:**
- A generic `AFTER UPDATE ON feeds` trigger appears in the migration.
- Audit action names include `feed.active`, `feed.unclaimed`, `heartbeat`, or `lease_released`.
- Local tests assert events for every `status` change instead of the project-approved meaningful actions.

**Phase to address:**
Phase 1 for action contract; Phase 4 for runtime integration tests.

---

### Pitfall 4: Missing Recovery Events Hidden in Success Paths

**What goes wrong:**
Failures and quarantines are audited, but recovery is not. Operators see a feed fail and later observe a clean current state with no durable event explaining when it recovered.

**Why it happens:**
Recovery is not a dedicated method today. It is implicit in successful work:
- `UPDATE_PROGRESS_SQL` clears `failure_count` and `status_reason` after a successful chunk bookmark.
- `RECORD_SOURCE_OBSERVATION_SQL` clears stale failure state after a non-audio successful source observation.
- `SyncFeedStore._HEARTBEAT_SQL` for Echo sets `status = 'active'`, clears `status_reason`, and resets failure count.
- `RESET_FEED_SQL` is an explicit admin recovery/reactivation path and clears both canonical and raw diagnostic fields.

**How to avoid:**
Treat `feed.recovered` as conditional: emit it only when the pre-mutation row had `failure_count > 0`, `status_reason IS NOT NULL`, `status = 'failing'`, or another persisted abnormal state that the success path clears. Do not emit recovery for clean progress, clean observation, or routine Echo heartbeat. Add tests for all three implicit recovery paths plus explicit reset.

**Warning signs:**
- The implementation only modifies `report_feed_failure` and `reset_feed`.
- Recovery event tests cover only `/reset`.
- Every successful chunk creates `feed.recovered`, even when the feed was already clean.

**Phase to address:**
Phase 4: Runtime Event Integration.

---

### Pitfall 5: Missing the Echo `SyncFeedStore` Path

**What goes wrong:**
Most feed sources produce audit history, but Echo feed failures and recoveries do not. The system then has source-type-specific blind spots.

**Why it happens:**
Most feed lifecycle writes use async `FeedStore`, but Echo ingestion uses `backend/pipeline/storage/sync_feed_store.py` from `backend/pipeline/ingestion/collectors/echo/main.py`. It has separate SQL for `record_heartbeat` and `record_failure`, uses psycopg, and connects with autocommit.

**How to avoid:**
Update `SyncFeedStore` in the same phase as the async runtime failure paths. Keep action semantics aligned with `FeedStore`: failure/quarantine/recovery events, bounded detail, canonical status reason, and no event for skipped unknown/deactivated/quarantined Echo notifications. Add unit tests in `backend/pipeline/storage/tests/test_sync_feed_store.py` and integration coverage in Echo tests if feasible.

**Warning signs:**
- New audit helper imports only appear in `feed_store.py`.
- Echo tests still assert only `record_heartbeat` and `record_failure`, with no audit assertions.
- `feed_audit_events` has no rows for `source_type = 'echo'` after a simulated Echo failure/recovery.

**Phase to address:**
Phase 4: Runtime Event Integration.

---

### Pitfall 6: Hard Delete Removes the Only Identity Needed for History

**What goes wrong:**
Deleting a feed either deletes its audit history through cascade, blocks deletion because of an audit foreign key, or leaves a `feed.deleted` event without enough feed identity to interpret it later.

**Why it happens:**
`DELETE_FEED_SQL` currently deletes audio segments and transcripts, then deletes the feed row. `feed_properties` cascades away with the feed. If `feed_audit_events.feed_id` uses a normal cascading foreign key to `feeds`, durable history is incompatible with hard delete.

**How to avoid:**
Make audit history survive feed deletion. Store denormalized identity fields needed for forensic use, such as `feed_id`, `feed_name`, `source_type`, and `source_feed_id`, directly on the audit event or in event values. Avoid `ON DELETE CASCADE` from audit events to `feeds`; either omit the FK or use a non-cascading design that does not block delete. Insert `feed.deleted` from a CTE that captures feed and feed_properties before the delete.

**Warning signs:**
- `feed_audit_events` references `feeds(id) ON DELETE CASCADE`.
- The delete audit event has only `feed_id` and no name/source snapshot.
- Tests verify the feed is gone but do not verify the delete event remains.

**Phase to address:**
Phase 1 for schema shape; Phase 2 for delete SQL; Phase 5 for storage integration verification.

---

### Pitfall 7: Duplicating Failure and Quarantine Events for One Threshold Crossing

**What goes wrong:**
When `failure_count + 1` reaches the threshold, the ledger records both `feed.failure_reported` and `feed.quarantined` for the same SQL update. Operators see two events for one outcome and downstream delivery later has to guess which one is canonical.

**Why it happens:**
`report_feed_failure` returns only the resulting status string today. The project decision says "Emit one `feed.quarantined` event when threshold crossing occurs" and "Treat all persisted non-quarantine failures as audit-worthy." That means the event action must be chosen from the final transition, not blindly emitted before checking the result.

**How to avoid:**
In the same SQL operation, choose `feed.quarantined` when the row transitions to quarantined; otherwise choose `feed.failure_reported`. Include `failure_count`, `retry_after`, `status_reason`, and bounded detail in values. `release_non_budgeted_failure` should create a non-quarantine failure event but never a quarantine event.

**Warning signs:**
- Tests expect two audit rows when `report_feed_failure(... threshold reached ...)` runs once.
- Quarantine telemetry and audit event action are coupled so closely that changing one changes the other.
- `release_non_budgeted_failure` can emit `feed.quarantined`.

**Phase to address:**
Phase 4: Runtime Event Integration.

---

### Pitfall 8: Treating Audit Events as Future Webhook Payloads

**What goes wrong:**
The table bakes in HTTP delivery fields, receiver-specific payload shape, retry state, or signature assumptions. Later Watch Duty backend delivery either duplicates the ledger or needs a schema rewrite.

**Why it happens:**
Linear context includes downstream WD backend delivery, but this project explicitly says v1 is write-only durable domain history and not webhook delivery.

**How to avoid:**
Store domain audit event data: feed identity, per-feed sequence, action, occurred time, actor, status reason, bounded detail, before_values, and after_values. Keep dispatcher state, delivery attempts, response codes, signatures, and receiver payload versions out of v1. Document how future delivery derives webhook payloads from durable events.

**Warning signs:**
- Table columns include `webhook_url`, `attempt_count`, `signature`, `delivered_at`, or `http_status`.
- Action names are external endpoint verbs instead of domain actions.
- Contract docs describe receiver retries even though delivery is out of scope.

**Phase to address:**
Phase 1: Contract and Schema.

---

### Pitfall 9: Losing Actor Attribution at the Service Boundary

**What goes wrong:**
Admin-created, updated, deactivated, reset, and deleted events are recorded as anonymous system events, so the audit history cannot answer "who changed this feed?"

**Why it happens:**
Current FastAPI feed handlers depend on `verify_oidc_token`, but service methods do not accept an actor. The BFF has `request.user.isAdmin`, but backend services currently receive only the feed payload. Existing logs in `FeedService` record event type and feed ID, not actor.

**How to avoid:**
Extend the backend service boundary to derive actor from verified auth context, not from client-supplied request body fields. Pass actor into `FeedService` and `FeedStore` mutation methods. Use a clear system actor for runtime writes, such as `system:ingestion` plus worker ID where useful. If BFF actor forwarding is needed, only trust it when the backend can verify the caller boundary.

**Warning signs:**
- API request models add an `actor` field that browsers can set.
- Store methods default `actor = "unknown"` for admin endpoints.
- Audit rows for admin operations do not include email/sub or an explicit actor type.

**Phase to address:**
Phase 3 for API/service propagation; Phase 4 for runtime actor values.

---

### Pitfall 10: Breaking `quarantine_reason` Compatibility While Adding `status_reason_detail`

**What goes wrong:**
Existing UI/BFF paths lose the displayed diagnostic reason, or old consumers keep reading stale `quarantine_reason` while new code writes only `status_reason_detail`.

**Why it happens:**
The repo currently exposes `quarantine_reason` through FastAPI, BFF conversion, shared frontend types, and UI status tooltips. The new project requires `status_reason_detail` as canonical current diagnostic detail while keeping `quarantine_reason` populated as a compatibility alias for one release.

**How to avoid:**
Add nullable `feeds.status_reason_detail` without removing `quarantine_reason`. During the compatibility window, write bounded detail to both fields where quarantine compatibility matters, and clear both on reset/recovery. Update full-feed SQL projections, Pydantic models, BFF `FeedBackend`, shared frontend types, conversion tests, and OpenAPI. Document when the alias can be removed.

**Warning signs:**
- `quarantine_reason` disappears from `backend/services/feeds/models.py` or `frontend/common/src/types/feeds.ts` in v1.
- `RESET_FEED_SQL` clears only one diagnostic field.
- `FeedStatusIndicator` tests are updated only for the new field and stop checking the old field.

**Phase to address:**
Phase 1 for migration; Phase 3 for API/BFF/frontend compatibility; Phase 4 for runtime writes.

---

### Pitfall 11: Persisting Unbounded or Secret-Bearing Diagnostic Detail

**What goes wrong:**
Audit rows store tokens, credential-bearing exception text, full provider responses, or unbounded error blobs. Retention then preserves sensitive or noisy data for 18 months.

**Why it happens:**
Current `quarantine_reason` is capped at 2048 characters but is not sanitized or redacted. Existing tests explicitly assert there is no sanitizer for quarantine reason. The new project adds a more general `status_reason_detail`, increasing the chance that raw exception strings get persisted beyond quarantine.

**How to avoid:**
Create a storage-boundary helper for `status_reason_detail` that bounds length and redacts obvious secrets/tokens/URLs where needed. Keep stable `status_reason` enum values as the grouping key and store detail only as operator context. Add tests with long strings, multiline exceptions, bearer/API-key-shaped text, and provider error payloads.

**Warning signs:**
- New detail code reuses `exception_text()` directly without a redaction boundary.
- Tests only cover length caps.
- `before_values` or `after_values` include source credentials, env vars, request headers, or raw external API responses.

**Phase to address:**
Phase 1 for helper/contract; Phase 4 for runtime use; Phase 5 for verification.

---

### Pitfall 12: Incorrect Before/After Values for Multi-Table Feed Mutations

**What goes wrong:**
An update event says the name changed but omits tag changes, source identity, or diagnostic fields. A create/delete event lacks enough state to reconstruct what changed.

**Why it happens:**
Feed data spans `feeds` and `feed_properties`. `CREATE_FEED_SQL` and `UPDATE_FEED_SQL` already use CTEs across both tables, and list/get queries join them. A narrow audit implementation that only reads `feeds` misses `source_feed_id` and tags.

**How to avoid:**
Define an audit snapshot projection that covers the meaningful feed fields across both tables: name, source_type, source_feed_id, tags, status, status_reason, status_reason_detail/quarantine compatibility, failure_count, retry_after where relevant, and timestamps needed for interpretation. Capture before and after from the same locked transaction. Keep `before_values` and `after_values` as bounded JSON objects, not full row dumps.

**Warning signs:**
- `before_values` never includes tags.
- Create and delete events use different field names than update/reset/failure events.
- Tests only update `name`, not tags or diagnostic fields.

**Phase to address:**
Phase 1 for snapshot contract; Phase 2 for SQL implementation.

---

### Pitfall 13: Retention Is Documented But Not Enforced

**What goes wrong:**
Audit history grows forever, increasing storage costs and making future timeline queries slower. The roadmap claims 18-month retention but no system enforces it.

**Why it happens:**
Existing pg_cron work is production-specific and skipped by CI/local/test fixtures when filenames contain `pg_cron`. It is easy to add a table and defer cleanup because v1 has no read API.

**How to avoid:**
Ship retention in v1. Put table/index DDL in a normal ingestion migration and schedule retention in a separate `*pg_cron*.sql` migration so local/CI skip behavior remains intact. Also create a testable cleanup SQL/function or store helper that component tests can call without pg_cron. Retention should delete rows older than 18 months by `occurred_at`, preferably in bounded batches if volume can grow.

**Warning signs:**
- `PITFALLS.md` or project docs mention retention but no SQL migration schedules or exposes cleanup.
- The retention migration requires pg_cron but its filename does not include `pg_cron`.
- Tests never create old audit rows and verify deletion.

**Phase to address:**
Phase 5: Retention and Verification.

---

### Pitfall 14: Defeating HOT-Safe Feed Storage Tuning

**What goes wrong:**
Feed lease, heartbeat, progress, and failure paths slow down or bloat the `feeds` table because a new index or trigger touches high-churn columns.

**Why it happens:**
The repo has explicit HOT-protection tuning for `feeds`, including fillfactor, partial indexes, and a CI guard against indexes on hot columns. Feed Audit Events V1 adds new current-state fields and may tempt new indexes on diagnostics or generic triggers on all feed updates.

**How to avoid:**
Do not index mutable current diagnostic fields unless there is a v1 query requirement. Keep audit indexes on `feed_audit_events`, not on hot `feeds` columns. Avoid generic triggers that run for heartbeat/lease/progress updates. If a new mutable feed column becomes hot, update `terraform/modules/alloydb/sql/ci/hot_protection_check.sql` intentionally.

**Warning signs:**
- Migration adds an index on `feeds.status_reason_detail`, `failure_count`, `worker_id`, `last_heartbeat`, or `retry_after` without a measured query need.
- Audit implementation uses `AFTER UPDATE ON feeds` trigger.
- HOT-protection CI is bypassed or not considered for the migration.

**Phase to address:**
Phase 1 for schema review; Phase 5 for HOT/query verification.

---

### Pitfall 15: Misclassifying Failure Policy Actions

**What goes wrong:**
Source outages burn quarantine budget, unknown runtime bugs get classified as source problems, or post-bookmark publish gaps look like normal retryable failures. The audit trail then explains the wrong cause.

**Why it happens:**
The runtime routes failures through `FeedStatusReason` and `failure_policy.classify_failure_policy`. Only some reasons increment the feed failure budget. `PIPELINE_PUBLISH_AFTER_BOOKMARK_FAILED` is special because capture/bookmark succeeded but publish failed, creating a known replay gap.

**How to avoid:**
Use `FeedStatusReason` and `ExecutedAction` as audit inputs. Store both the domain action (`feed.failure_reported`, `feed.quarantined`, `feed.recovered`) and enough context to understand policy (`status_reason`, retry_after, failure_count, replay_missing/data_gap_known if retained in event values). Do not branch on freeform detail text.

**Warning signs:**
- Audit action is selected by substring matching `reason`.
- Tests do not include `PIPELINE_PUBLISH_AFTER_BOOKMARK_FAILED`.
- Non-budgeted failures increment `failure_count` in audit values even though SQL resets it to zero.

**Phase to address:**
Phase 4: Runtime Event Integration.

---

### Pitfall 16: Trusting Logs or Telemetry as the Audit Source

**What goes wrong:**
The system appears to have an audit trail in local testing because logs exist, but durable database history is incomplete. Logs expire and are not shaped for future admin timeline queries.

**Why it happens:**
The repo already emits structured quarantine telemetry and service logs such as `feed_deactivated` and `feed_reset`. These are useful operational signals but not durable audit records.

**How to avoid:**
Keep logs/metrics as observability only. The acceptance criteria for each mutation should query `feed_audit_events`, not logs. Quarantine telemetry should remain unchanged unless explicitly required; audit events should be persisted independently and transactionally.

**Warning signs:**
- Tests use `assertLogs` as the only proof of audit behavior.
- Code reuses `quarantine_telemetry.emit_quarantine_event` as the audit writer.
- Audit docs say "look in Cloud Logging" for feed history.

**Phase to address:**
Phase 2 for durable storage; Phase 4 for runtime separation; Phase 5 for verification.

---

### Pitfall 17: Migration Ordering and Test Schema Drift

**What goes wrong:**
Production schema, local Docker schema, and integration-test schema diverge. A migration passes locally but fails in CI, or a pg_cron migration breaks test database setup.

**Why it happens:**
Ingestion migrations are applied lexically. Existing files include duplicate numeric prefixes (`025_*`), and pg_cron files are skipped by CI/local/test helpers based on filename substring. Test schemas use `async_apply_test_schema()` and `sync_apply_test_schema()` to apply every non-pg_cron SQL file.

**How to avoid:**
Use the next unique migration prefix and idempotent DDL. Keep pg_cron scheduling in a filename containing `pg_cron`. After adding migrations, run a low-resource schema/HOT check or targeted storage tests rather than only unit SQL string tests. Update test helpers only if the migration convention changes, which should be avoided.

**Warning signs:**
- A new retention migration calls `CREATE EXTENSION pg_cron` but the filename lacks `pg_cron`.
- SQL relies on one migration file applying before another with the same prefix and ambiguous name order.
- Integration tests fail during schema setup rather than in the audit behavior assertions.

**Phase to address:**
Phase 1 for migration structure; Phase 5 for schema verification.

---

### Pitfall 18: Under-Testing With Mocks Only

**What goes wrong:**
SQL string tests pass, but real AlloyDB/Postgres behavior fails for transactions, JSONB, CTE row locking, cascade/delete survival, retention, or concurrency.

**Why it happens:**
The repo has many good mocked unit tests for store methods and API behavior, but storage correctness depends on actual database semantics. The integration tests are resource-heavy, so agents may avoid them entirely.

**How to avoid:**
Use the repo's pattern: unit tests for SQL shape and narrow component tests for database semantics. Add targeted integration tests for create/update/delete/reset/failure audit rows, rollback on audit insert failure, audit survival after delete, per-feed sequence uniqueness, conditional recovery, and retention cleanup. Follow `AGENTS.md`: do not run broad local E2E/component suites by default; use targeted low-resource commands or CI for heavy validation.

**Warning signs:**
- Only `make_mock_pool()` tests exist for audit writes.
- No test inserts old audit rows.
- No test runs concurrent operations against the same feed.
- Verification plan says `mise run test:e2e` even though the change is storage-scoped and local broad tests are discouraged.

**Phase to address:**
Phase 5: Retention and Verification.

## Technical Debt Patterns

| Shortcut | Immediate Benefit | Long-term Cost | When Acceptable |
|----------|-------------------|----------------|-----------------|
| Add audit writes in `FeedService` after store calls | Small diff, easy actor access | State/history drift on partial failure; duplicates logic outside storage boundary | Never for durable audit writes |
| Use an `AFTER UPDATE` trigger on `feeds` | Captures many mutations quickly | Audits lease churn, hurts hot paths, hard to filter correctly | Never for v1 |
| Store only `feed_id` in events | Simple schema | Delete events become unreadable after hard delete; source/name context lost | Never for delete-capable history |
| Use raw exception strings as detail | Fast implementation | Secret/PII retention and noisy unbounded data | Only after central bounding/redaction |
| Skip Echo/`SyncFeedStore` until later | Reduces first implementation scope | Source-type-specific history gaps | Only if explicitly documented as out of scope, which conflicts with current v1 scope |
| Rely on Cloud Logging for audit | No schema work | Logs expire and cannot power admin timeline queries | Never for this project |
| Add retention docs without cleanup SQL | Faster v1 demo | Unbounded storage growth and missed requirement | Never; retention is active v1 scope |

## Integration Gotchas

| Integration | Common Mistake | Correct Approach |
|-------------|----------------|------------------|
| `FeedStore` async SQL | Append an audit insert after existing `fetchrow()` | Make feed mutation and audit insert one transaction/CTE |
| `SyncFeedStore` Echo path | Forget it because most code uses async `FeedStore` | Add sync audit behavior and tests alongside async runtime work |
| FastAPI feed service | Let clients submit `actor` | Derive actor from verified auth context and pass it downward |
| BFF/shared frontend types | Rename or remove `quarantineReason` immediately | Add `statusReasonDetail` while preserving `quarantineReason` alias for one release |
| AlloyDB migrations | Put pg_cron retention in a normal migration filename | Put schedule in `*pg_cron*.sql` and keep core table DDL testable without pg_cron |
| Quarantine telemetry | Reuse telemetry emit as audit persistence | Keep telemetry as logs/metrics; audit writes go to AlloyDB |

## Performance Traps

| Trap | Symptoms | Prevention | When It Breaks |
|------|----------|------------|----------------|
| Auditing lease churn | Audit table grows rapidly with low-value rows | Explicit event sites only; no generic feed update trigger | Immediately at normal heartbeat/lease volume |
| Indexing mutable `feeds` diagnostics | HOT-protection CI failures or production table bloat | Keep indexes on `feed_audit_events`; avoid new hot `feeds` indexes | As soon as heartbeat/progress/failure paths touch indexed columns |
| Unbounded retention delete | Retention job locks or deletes huge batches | Delete by `occurred_at` in bounded batches or a measured query shape | After 18 months of accumulated events or during backlogged cron |
| Future timeline query unindexed | Later admin UI scans all events | Add v1 indexes for `(feed_id, sequence)` and retention by `occurred_at`; defer other indexes until read API | When GOO-574/admin timeline arrives |
| Storing full row dumps | Large JSONB rows and expensive history | Store bounded meaningful before/after values only | With frequent updates or large tag/detail values |

## Security Mistakes

| Mistake | Risk | Prevention |
|---------|------|------------|
| Persisting raw credential/provider errors | Secrets or PII retained for 18 months | Central bounded/redacted `status_reason_detail` helper |
| Accepting actor from request body | Forged audit identity | Derive actor from verified OIDC/BFF trust boundary |
| Recording source URLs or headers in values | Tokenized URLs or auth headers leak into audit | Whitelist fields in before/after snapshots |
| Treating BFF auth weaknesses as solved by audit | Audit records may faithfully record spoofed identity | Do not weaken current auth boundary; prefer backend-verified identity |
| Giving audit table broad read access | Internal diagnostic detail exposed too widely | Keep v1 write-only and document future read authorization requirements |

## "Looks Done But Isn't" Checklist

- [ ] **Schema:** `feed_audit_events` exists, has `(feed_id, sequence)` uniqueness, stores feed identity snapshots, and does not cascade-delete with `feeds`.
- [ ] **Actions:** The action vocabulary covers create, update, deactivate, reset, delete, failure_reported, quarantined, and recovered without lease churn actions.
- [ ] **Transactions:** Every feed mutation and its audit insert commits or rolls back together.
- [ ] **Delete:** Hard delete writes a durable delete event before removing the feed and the audit event remains after the feed is gone.
- [ ] **Recovery:** Dirty success paths emit `feed.recovered`; clean progress/heartbeat paths do not.
- [ ] **Echo:** `SyncFeedStore.record_heartbeat` and `record_failure` have audit parity with async runtime paths.
- [ ] **Compatibility:** `status_reason_detail` is canonical, `quarantine_reason` remains populated/cleared as a compatibility alias for one release, and existing UI/BFF tests still pass.
- [ ] **Actor:** Admin actions use verified user identity; runtime actions use explicit system actor values.
- [ ] **Retention:** 18-month cleanup is implemented and testable without relying solely on production pg_cron.
- [ ] **Verification:** Targeted storage integration tests cover rollback, delete survival, sequence concurrency, failure/quarantine, recovery, and retention.

## Recovery Strategies

| Pitfall | Recovery Cost | Recovery Steps |
|---------|---------------|----------------|
| Non-transactional audit drift | HIGH | Stop rollout, identify drift window, compare feed updated timestamps/logs against audit rows, backfill only factual events that can be proven, then move audit writes into storage transactions |
| Duplicate sequence values | HIGH | Add/repair unique constraint, sort by occurred_at/id to reconstruct ordering where possible, reassign sequences in a maintenance transaction, add concurrency test |
| Audit cascade on hard delete | HIGH | Change FK design, restore from backup if needed, add delete-survival test before re-enabling hard deletes |
| Lease churn audited | MEDIUM | Delete noisy action rows if safe, remove trigger/generic writer, add negative tests around lease/heartbeat/release paths |
| Missing Echo history | MEDIUM | Add `SyncFeedStore` audit writes, backfill only events supported by durable current state/logs if needed, mark unverifiable gaps honestly |
| Unbounded sensitive detail | HIGH | Stop writes, redact in place where policy allows, rotate exposed credentials if necessary, ship bounded/redacted helper and tests |
| Retention omitted | MEDIUM | Add retention migration/job, run one-time cleanup in bounded batches, monitor row counts and duration |

## Pitfall-to-Phase Mapping

| Pitfall | Prevention Phase | Verification |
|---------|------------------|--------------|
| Non-transactional audit writes | Phase 2 | Inject audit insert failure and verify feed mutation rollback |
| Sequence races | Phases 1, 2, 5 | Unique constraint plus concurrent same-feed mutation test |
| Lease churn noise | Phases 1, 4 | Negative tests for claim, heartbeat, release, batch release, clean progress |
| Hidden recovery paths | Phase 4 | Tests for `UPDATE_PROGRESS_SQL`, `RECORD_SOURCE_OBSERVATION_SQL`, Echo heartbeat, and reset |
| Missing Echo path | Phase 4 | `test_sync_feed_store.py` plus Echo handler/integration assertions |
| Delete loses history | Phases 1, 2, 5 | Storage integration test verifies audit remains after `delete_feed` |
| Duplicate failure/quarantine event | Phase 4 | Threshold test asserts exactly one `feed.quarantined` event |
| Webhook-shaped canonical schema | Phase 1 | Contract doc review: no delivery attempt/webhook columns in v1 table |
| Actor missing or forged | Phase 3 | API/service tests derive actor from auth context; body actor ignored/rejected |
| Compatibility break | Phases 1, 3, 4 | API/BFF/frontend tests cover both current detail and `quarantineReason` alias |
| Sensitive/unbounded detail | Phases 1, 4, 5 | Unit tests for length cap and redaction cases |
| Multi-table before/after gaps | Phases 1, 2 | Update tests cover name and tags; delete event includes feed_properties snapshot |
| Retention not enforced | Phase 5 | Old-row cleanup test plus pg_cron migration filename check |
| HOT-safe regression | Phases 1, 5 | HOT-protection CI guard and migration review |
| Failure policy misclassification | Phase 4 | Runtime tests cover budgeted, non-budgeted, and post-bookmark publish failure |
| Logs mistaken for audit | Phases 2, 4, 5 | Tests query `feed_audit_events`, not only `assertLogs` |
| Migration/test drift | Phases 1, 5 | Apply non-pg_cron migrations in test schema and run targeted storage tests |
| Mock-only verification | Phase 5 | Add component tests for DB-specific behavior |

## Sources

- `.planning/PROJECT.md` - Feed Audit Events V1 scope, constraints, action requirements, retention, compatibility, and out-of-scope delivery boundary.
- `.planning/codebase/ARCHITECTURE.md` - storage/service/runtime boundaries, ingestion runtime responsibilities, Echo direct segmented source path, anti-patterns.
- `.planning/codebase/CONCERNS.md` - known risks around `FeedStore` SQL/HOT tuning, runtime lease invariants, API/BFF contract drift, security/logging hygiene, and testing gaps.
- `.planning/codebase/TESTING.md` - pytest/Vitest/testcontainers practices and local test-safety constraints.
- `backend/pipeline/storage/feed_store.py` and `backend/pipeline/storage/feed_queries.py` - current feed mutation, failure, reset, delete, lease, and recovery SQL behavior.
- `backend/pipeline/storage/sync_feed_store.py` and `backend/pipeline/ingestion/collectors/echo/main.py` - Echo-specific sync feed mutation path.
- `backend/pipeline/ingestion/collector_runtime.py`, `failure_policy.py`, `models.py`, and `quarantine_telemetry.py` - runtime failure policy, recovery clearing, quarantine telemetry, and lease-churn boundaries.
- `backend/services/feeds/*` and `frontend/api/src/feeds/feedsController.ts` - API/service/BFF compatibility and actor-boundary surfaces.
- `terraform/modules/alloydb/sql/ingestion/*.sql` and `terraform/modules/alloydb/sql/ci/hot_protection_check.sql` - migration ordering, pg_cron convention, HOT-safe feed storage constraints.

---
*Pitfalls research for: Feed Audit Events V1*
*Researched: 2026-06-19*
