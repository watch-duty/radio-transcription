# Project Research Summary

**Project:** Feed Audit Events V1
**Domain:** Durable backend audit ledger for radio transcription feed mutations
**Researched:** 2026-06-19
**Confidence:** HIGH

## Executive Summary

Feed Audit Events V1 should add a durable, queryable, append-only audit ledger for meaningful feed mutations in the existing radio transcription backend. Experts would build this as application-owned domain audit data beside the existing current-state model, not as Cloud Logging, not as webhook payload storage, and not as full event sourcing. The current `feeds` row must remain authoritative for lease, failure, retry, diagnostic, and progress state; `feed_audit_events` explains successful meaningful changes for future admin timeline and Watch Duty delivery work.

The recommended approach is to add `feeds.status_reason_detail`, a new `feed_audit_events` table, a per-feed sequence allocator, bounded before/after JSON snapshots, actor attribution, and 18-month retention using the repo's existing AlloyDB SQL migration and pg_cron conventions. Audit writes should be owned only by `FeedStore` and `SyncFeedStore`, with each feed mutation and matching audit insert committed atomically in the same SQL statement or explicit transaction. V1 should expose compatibility for the current feed response fields but should not add audit read APIs, admin UI, webhook delivery, or delivery-attempt state.

The main risks are state/history drift from post-commit audit writes, sequence races under concurrent admin/runtime mutations, noisy lease-churn events, missing Echo or implicit recovery paths, forged or missing actor attribution, sensitive diagnostic detail retention, and breaking `quarantine_reason` compatibility. Mitigate them by putting audit writes at the storage boundary, using an atomic per-feed sequence strategy, explicitly allowlisting audited actions and snapshot fields, preserving the BFF/backend trust boundary for actors, bounding and scrubbing detail text, keeping `quarantine_reason` populated for one release, and verifying with focused storage/component tests rather than broad E2E runs.

## Key Findings

### Recommended Stack

The stack should stay inside the existing backend architecture: Python storage/runtime code, AlloyDB/PostgreSQL for durable state, asyncpg/psycopg for async and Echo sync paths, FastAPI/Pydantic for feed service compatibility, TypeScript BFF/shared types for existing UI contracts, and Terraform-managed SQL migrations for schema and pg_cron retention. No new dependency or database is justified for V1.

**Core technologies:**
- Python `>=3.13,<3.14`: implement storage/runtime changes in the existing backend runtime.
- AlloyDB / PostgreSQL: store durable audit rows and current `status_reason_detail` at the same consistency boundary as `feeds`.
- asyncpg `>=0.29.0`: extend async `FeedStore` SQL patterns for VM ingestion and FastAPI service mutations.
- psycopg v3 `>=3.2.0`: add parity for Echo `SyncFeedStore` heartbeat/failure writes.
- FastAPI `>=0.110.0` + Pydantic `>=2.10.6`: preserve feed API response compatibility and expose `status_reason_detail`.
- Node 22 + TypeScript 6 BFF/shared types: forward admin actor context and keep frontend feed contracts stable.
- Terraform-managed SQL migrations: add ordered ingestion migrations for table/detail fields and separate `*pg_cron*.sql` retention schedule.

Critical stack requirements:
- Add normal schema migration after `028_initialize_feed_bookmarks.sql`, likely `029_feed_audit_events.sql`.
- Put pg_cron scheduling in a separate filename containing `pg_cron`, likely `030_feed_audit_events_retention_pg_cron.sql`.
- Prefer an atomic per-feed sequence counter table or equivalent allocator over naive `MAX(sequence)+1`; row-lock-plus-`MAX` is acceptable only if every audited writer is proven to serialize on the same feed row.
- Do not add a cascading FK from audit rows to `feeds`; delete history must survive hard deletes.
- Do not index new hot current-state diagnostic columns on `feeds`.

### Expected Features

V1 is a backend durability and contract milestone. It should create reliable domain audit data first, then let future roadmap phases build read APIs, UI, and delivery on top of that contract.

**Must have (table stakes):**
- Append-only `feed_audit_events` table with `feed_id`, per-feed sequence, action, occurred time, actor, status/reason/detail, and bounded before/after JSON.
- Stable action taxonomy: `feed.created`, `feed.updated`, `feed.deactivated`, `feed.reset`, `feed.deleted`, `feed.failure_reported`, `feed.quarantined`, and `feed.recovered`.
- Transactional feed mutation plus audit insert for create, update, deactivate, reset, delete, budgeted/non-budgeted failure, quarantine threshold crossing, and recovery-by-success.
- Actor attribution for human admin actions and system/runtime actions.
- `feeds.status_reason_detail` as canonical bounded diagnostic detail, with `quarantine_reason` kept populated as a compatibility alias for one release.
- Deletion-safe snapshots that preserve feed identity after hard delete.
- 18-month audit retention enforcement.
- Contract documentation and focused tests for storage, service/API compatibility, runtime failure/quarantine/recovery, delete survival, and retention.

**Should have (competitive / v1.x useful):**
- Internal query/helper for verification if direct SQL assertions become brittle during rollout.
- Cheap reliable metadata where already available, such as source component, trace/request ID, or worker context.
- Narrow monitoring for event volume, failure storms, and retention-job impact.

**Defer (v2+):**
- Watch Duty backend webhook delivery, signatures, retries, idempotency, and delivery-attempt state.
- Admin timeline read APIs and frontend UI.
- Consumer-specific webhook payload mappers.
- Historical synthetic baseline/backfill events.
- Tamper-evident/WORM compliance hardening.
- Cross-domain audit ledgers for rules, transcripts, audio segments, notifications, or auth.
- Replay/rebuild of current feed state from audit events.

### Architecture Approach

Use a store-owned audit ledger beside the existing current-state `feeds` model. The only application write owners for `feed_audit_events` in V1 should be `FeedStore` and `SyncFeedStore`. Service, runtime, BFF, and collector layers may pass actor/cause metadata downward, but they should not insert audit rows directly. This keeps current-state mutation, before/after capture, sequence allocation, and audit insertion at one atomic boundary.

**Major components:**
1. AlloyDB migrations - add `status_reason_detail`, `feed_audit_events`, constraints, indexes, retention schedule, and HOT guard updates.
2. `FeedStore` - own async audited mutations for admin and VM runtime feed paths.
3. `SyncFeedStore` - own Echo failure and heartbeat/recovery audit parity.
4. `FeedService` and FastAPI routes - derive/pass actor context and preserve existing response semantics.
5. BFF/API proxy - enforce admin gate and forward trusted end-user actor context.
6. Ingestion runtime - classify failure/recovery outcomes and call store methods without separate audit writes.
7. Docs/types/tests - define the event contract, compatibility policy, and verification net.

Key patterns:
- Prefer data-modifying CTEs or explicit single-connection transactions.
- Capture before and after snapshots from locked rows in the same transaction.
- Emit audit events only for meaningful domain mutations, not lease acquire/release/heartbeat churn.
- Emit one `feed.quarantined` outcome event for a threshold-crossing failure, not both failure and quarantine events.
- Emit `feed.recovered` only when successful progress/source observation/Echo heartbeat/reset clears persisted dirty state.
- Whitelist snapshot fields and cap/redact diagnostic detail at the storage boundary.

### Critical Pitfalls

1. **Post-commit audit writes** - avoid by making store mutations and audit inserts one CTE/transaction, with rollback tests.
2. **Per-feed sequence races** - avoid with a unique `(feed_id, sequence)` constraint and an atomic per-feed allocator or proven same-row serialization.
3. **Lease churn pollution** - avoid generic `AFTER UPDATE` triggers and add negative tests for claim, heartbeat, release, clean progress, and abandoned-lease paths.
4. **Hidden recovery gaps** - audit recovery when dirty failure state is cleared by successful progress, source observation, Echo heartbeat, or reset; do not audit clean success.
5. **Missing Echo parity** - update `SyncFeedStore.record_failure` and dirty `record_heartbeat` alongside async runtime paths.
6. **Delete loses history** - omit cascading feed FK and insert `feed.deleted` with identity snapshot before hard delete.
7. **Actor attribution gaps or forgery** - derive actor from verified service/BFF auth context, not request body fields.
8. **Compatibility break** - keep `quarantine_reason` available while adding canonical `status_reason_detail`.
9. **Sensitive/unbounded detail** - centralize length cap and redaction before persistence.
10. **Retention only documented** - ship cleanup SQL/pg_cron and a testable cleanup path in V1.

## Implications for Roadmap

Based on research, suggested phase structure:

### Phase 1: Contract and Schema Foundation

**Rationale:** Every later phase depends on agreed event vocabulary, data shape, migration layout, sequence semantics, detail compatibility, and retention design.
**Delivers:** Event contract docs; `status_reason_detail`; `feed_audit_events`; action/actor constraints; feed identity fields; before/after field allowlist; per-feed sequence allocator; minimal indexes; pg_cron migration stub/schedule; HOT guard updates.
**Addresses:** Append-only audit table, action taxonomy, per-feed ordering, bounded detail, deletion-safe schema, 18-month retention foundation.
**Avoids:** Webhook-shaped canonical storage, cascading audit deletion, hot `feeds` indexes, lease-churn action vocabulary, unbounded diagnostic detail.

### Phase 2: Transactional Storage Writes

**Rationale:** The feature's correctness depends on current-state mutations and audit rows committing together; storage must be stable before service/runtime callers rely on it.
**Delivers:** Audited `FeedStore` create/update/deactivate/reset/delete mutations; before/after snapshots across `feeds` and `feed_properties`; no-op suppression; delete-survival behavior; rollback-safe audit insertion; sequence allocation in the mutation transaction.
**Addresses:** Transactional mutation plus audit insert, deletion-safe snapshots, multi-table before/after values, per-feed sequence integrity.
**Avoids:** Service-layer post-commit inserts, sequence races, missing delete identity, logs mistaken for audit data, mock-only confidence.

### Phase 3: Service and Compatibility Surface

**Rationale:** Admin actor attribution and response compatibility cross the FastAPI/BFF/shared-type boundary and should be handled after storage accepts explicit actor context.
**Delivers:** FastAPI feed route/service actor propagation; trusted BFF actor forwarding where needed; Pydantic response fields; `status_reason_detail` and `quarantine_reason` compatibility; TypeScript shared/BFF mapping tests; OpenAPI/type updates as applicable.
**Addresses:** Actor attribution, current feed diagnostic compatibility, existing UI/BFF contract preservation.
**Avoids:** Forged request-body actors, anonymous admin events, broken `quarantineReason` UI behavior, backend/frontend contract drift.

### Phase 4: Runtime Event Integration

**Rationale:** Runtime failure, quarantine, recovery, and Echo semantics are the highest-risk behavior paths and should land after storage and actor contracts are ready.
**Delivers:** Runtime actor constants; audited budgeted and non-budgeted failures; single `feed.quarantined` event on threshold crossing; conditional `feed.recovered` for dirty success paths; `SyncFeedStore` Echo failure/heartbeat parity; negative tests for lease churn.
**Addresses:** Failure/quarantine/recovery event semantics, Echo coverage, signal quality, system actor values.
**Avoids:** Duplicate failure/quarantine events, hidden recovery gaps, source-type blind spots, auditing lease mechanics, failure-policy misclassification.

### Phase 5: Retention, Verification, and Documentation Hardening

**Rationale:** V1 is not complete until retention is enforced and database-specific guarantees are proven against real SQL behavior.
**Delivers:** Retention cleanup implementation and pg_cron schedule; testable cleanup path; migration schema checks; HOT checks; focused component tests for rollback, delete survival, concurrency, JSONB snapshots, recovery, failure/quarantine, and retention; final contract/terminology docs.
**Addresses:** 18-month enforcement, DB transaction/concurrency confidence, future roadmap readiness.
**Avoids:** Documentation-only retention, schema/test drift, broad expensive local test lanes, under-testing with mocks only.

### Phase Ordering Rationale

- Schema and contract must come first because all storage, service, runtime, and future consumer work depends on action names, field semantics, sequence strategy, and detail compatibility.
- Storage should precede service/runtime integration because it is the atomicity boundary and prevents higher layers from growing unsafe audit write paths.
- Actor/compatibility work belongs between storage and runtime so admin mutations get real identity without coupling runtime system events to BFF concerns.
- Runtime/Echo work should be separate because it has distinct failure-policy semantics, lease-invariant risks, and negative-test needs.
- Retention and component verification come last as a hardening phase, but retention schema decisions must be designed in Phase 1.

### Research Flags

Phases likely needing deeper research during planning:
- **Phase 3:** Confirm the exact trusted actor propagation mechanism between BFF and FastAPI service, including whether end-user identity is available from verified OIDC claims or must be forwarded through internal headers.
- **Phase 4:** Inspect current failure-policy edge cases during planning, especially non-budgeted failures, post-bookmark publish gaps, Echo autocommit behavior, and recovery clearing predicates.
- **Phase 5:** Validate pg_cron retention details and expected row volume if the roadmap wants bounded batch sizing or operational monitoring beyond the researched default.

Phases with standard patterns (skip research-phase unless implementation discovers drift):
- **Phase 1:** Migration shape, pg_cron filename convention, no-HOT-index rule, and write-only contract are well documented in the repo.
- **Phase 2:** Store-owned SQL/CTE mutation pattern is established, though implementation planning must be careful.
- **Phase 5 test selection:** The targeted test lanes are documented; planning should choose commands rather than re-researching the test framework.

## Confidence Assessment

| Area | Confidence | Notes |
|------|------------|-------|
| Stack | HIGH | Based on existing repo stack, migration workflow, storage clients, service frameworks, and test commands. No new dependency is needed. |
| Features | HIGH | V1 scope is explicit in `.planning/PROJECT.md`; future consumer ordering is MEDIUM because WD delivery/admin UI requirements are deferred. |
| Architecture | HIGH | Store/service/runtime boundaries are repo-specific and consistent across research; only sequence allocation implementation needs a final design choice. |
| Pitfalls | HIGH | Risks are grounded in source inspection, current SQL/runtime behavior, migration conventions, and known test constraints. |

**Overall confidence:** HIGH

### Gaps to Address

- **Actor source of truth:** Decide whether FastAPI can derive the admin actor directly from verified auth context or must trust narrowly scoped BFF headers.
- **Sequence allocator design:** Prefer a counter table or equivalent atomic allocator; explicitly reject naive `MAX+1` unless every writer serializes under a feed-row lock and tests prove it.
- **Diagnostic redaction policy:** The cap is clear, but exact scrub patterns for bearer tokens, API keys, URLs, provider payloads, and multiline exceptions need implementation-level tests.
- **Retention volume assumptions:** The 18-month requirement and pg_cron pattern are clear; batch size and monitoring can be tuned when expected event volume is known.
- **Future read authorization:** V1 is write-only, but contract docs should flag that future timeline APIs must define authorization before exposing diagnostic detail.

## Sources

### Primary (HIGH confidence)

- `.planning/PROJECT.md` - Feed Audit Events V1 scope, constraints, active requirements, out-of-scope boundaries, and key decisions.
- `.planning/codebase/STACK.md` - existing runtime, package managers, frameworks, migration tooling, and test tools.
- `.planning/codebase/ARCHITECTURE.md` - feed service/store/runtime boundaries and anti-patterns.
- `.planning/codebase/CONVENTIONS.md` - Python/TypeScript naming, style, and error-handling conventions.
- `.planning/codebase/CONCERNS.md` - known risks around feed-store SQL, HOT tuning, runtime invariants, API/BFF drift, and testing gaps.
- `.planning/codebase/TESTING.md` - unit/component test organization and local test-safety guidance.
- `backend/pipeline/storage/feed_store.py` and `backend/pipeline/storage/feed_queries.py` - async feed mutation, failure, reset, delete, recovery, and diagnostic storage paths.
- `backend/pipeline/storage/sync_feed_store.py` - Echo sync heartbeat/failure path.
- `backend/pipeline/ingestion/collector_runtime.py`, `failure_policy.py`, `models.py`, and `quarantine_telemetry.py` - runtime failure policy, recovery clearing, quarantine telemetry, and lease boundaries.
- `backend/services/feeds/*` - FastAPI feed service/API models.
- `frontend/api/src/feeds/feedsController.ts` and `frontend/common/src/types/feeds.ts` - BFF/shared feed contract and admin user availability.
- `terraform/modules/alloydb/sql/ingestion/*.sql` and `terraform/modules/alloydb/sql/ci/hot_protection_check.sql` - migration ordering, pg_cron convention, current feed schema, and HOT-safe guardrails.

### Secondary (MEDIUM confidence)

- OWASP Logging Cheat Sheet - application-owned logs/audit records should be purpose-driven, contextual, and avoid sensitive data: https://cheatsheetseries.owasp.org/cheatsheets/Logging_Cheat_Sheet.html
- NIST CSRC Audit Log Glossary and NIST SP 800-92 - audit records commonly include chronological timestamps, service/user/system accounts, event/status/error codes, and enough context for analysis: https://csrc.nist.gov/glossary/term/audit_log and https://nvlpubs.nist.gov/nistpubs/legacy/SP/nistspecialpublication800-92.Pdf

### Tertiary (LOW confidence)

- None identified. Remaining uncertainty is implementation-specific, not source-quality related.

---
*Research completed: 2026-06-19*
*Ready for roadmap: yes*
