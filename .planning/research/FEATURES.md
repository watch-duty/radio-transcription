# Feature Research

**Domain:** Feed auditability for the radio transcription backend
**Researched:** 2026-06-19
**Confidence:** HIGH for v1 scope and codebase dependencies; MEDIUM for future consumer ordering

## Feature Landscape

Feed Audit Events V1 should establish a durable backend event ledger for meaningful feed mutations. It should not deliver webhooks, expose an admin timeline, or rebuild current feed state from events. The existing `feeds` row remains the current-state source of truth; the audit table is an append-only history that future WD/backend/admin consumers can read or deliver from later.

External audit-log guidance supports the same shape: application-owned audit data should capture enough context to reconstruct what happened, who or what caused it, where it happened, and when it happened, while deliberately excluding sensitive or excessive raw data.

### Table Stakes For V1

| Capability | Why V1 Requires It | Complexity | Dependencies | Notes |
|------------|--------------------|------------|--------------|-------|
| Append-only `feed_audit_events` table | Durable history is the core value. Logs and quarantine telemetry are not feed-history-shaped and do not survive as product data. | HIGH | AlloyDB migration, event contract, feed mutation paths | Store domain audit events, not HTTP delivery payloads. Retention cleanup is the only expected deletion path. |
| Stable event action taxonomy | Future consumers need one vocabulary for create, update, deactivate, reset, delete, failure, quarantine, and recovery events. | MEDIUM | Product decisions in `.planning/PROJECT.md`; existing `FeedStatus` and `FeedStatusReason` enums | Use canonical action names such as `feed.created`, `feed.updated`, `feed.deactivated`, `feed.reset`, `feed.deleted`, `feed.failure_reported`, `feed.quarantined`, and `feed.recovered`. |
| Write-only v1 contract documentation | The roadmap needs future WD delivery and admin timeline work to build from the same model without coupling to storage internals. | LOW | Event taxonomy, table schema | Document required fields, action semantics, versioning, retention, and out-of-scope consumers. Do not add read APIs in v1. |
| Transactional feed mutation plus audit insert | Audit history is unreliable if feed state commits without its event, or an event commits for a rolled-back mutation. | HIGH | Storage-layer transaction helpers; refactor of `create_feed`, `update_feed`, `deactivate_feed`, `delete_feed`, `reset_feed`, failure paths | Implement in `FeedStore` or a store-owned helper so service/runtime code cannot forget the audit write. |
| Per-feed sequence and deterministic ordering | Future timelines and WD delivery need stable ordering even when timestamps tie or events are created concurrently. | MEDIUM | Audit table schema, same transaction as feed row mutation | Add `(feed_id, sequence)` uniqueness. Generate sequence under the same transaction, preferably while locking the affected feed row or an equivalent per-feed sequence source. |
| Required event context fields | The audit row must answer what happened, when, who/what caused it, and what changed. | MEDIUM | Event contract, actor propagation, before/after capture | Minimum fields: event id, feed id, per-feed sequence, action, occurred_at, actor_type, actor_id or actor_label, status, status_reason, status_reason_detail, before_values, after_values, request/correlation id where available. |
| Bounded before/after values | Operators need direct "what changed" answers without reconstructing diffs from current state. | MEDIUM | Feed snapshot helpers; field allowlist | Capture only meaningful feed/config/status fields. Avoid worker heartbeat, lease owner churn, unbounded payloads, and secret-bearing fields. |
| Actor attribution model | Linear asks for who changed fields, and future admin tooling will be incomplete without human-vs-system attribution. | HIGH | Backend auth claims, BFF user identity, service method signatures, runtime actor constants | Store `actor_type` values such as `admin_user`, `system_runtime`, `system_service`, and `unknown`. For BFF-initiated admin mutations, propagate the authenticated user identity or a trusted internal actor header from the BFF to the backend service. |
| `status_reason_detail` migration | Current `quarantine_reason` only captures threshold-crossing quarantine detail; v1 needs canonical bounded diagnostic detail for any abnormal current feed state. | MEDIUM | AlloyDB migration, feed models, API compatibility, failure paths | Add `feeds.status_reason_detail`, keep `quarantine_reason` populated as a compatibility alias for one release, and clear both through recovery/reset semantics. |
| Diagnostic detail bounding and scrubbing | Persisted diagnostic detail can contain exception strings or provider responses. The project constraint forbids secrets, tokens, raw credential-bearing strings, and unbounded data. | MEDIUM | `quarantine_reason` helper replacement or extension; failure classifiers | Reuse the 2048-character cap as a baseline, but add explicit scrubbing before storage. Treat detail as display/debug text, never as policy input. |
| Failure/quarantine/recovery event semantics | The existing runtime has budgeted failures, non-budgeted failures, quarantine threshold crossings, and success paths that clear failure state. V1 must make these understandable. | HIGH | `report_feed_failure`, `release_non_budgeted_failure`, `update_feed_progress`, `record_source_observation`, recovery claim path | Persist non-quarantine failures as audit-worthy. When a failure crosses the threshold, emit one `feed.quarantined` outcome event rather than a duplicate `failure_reported` plus `quarantined` pair for the same mutation. Emit `feed.recovered` when persisted failure state is cleared by successful capture/source observation, not for every normal lease. |
| Deletion-safe audit snapshots | Current `delete_feed` hard-deletes feed-related rows. The audit record must survive and still identify what was deleted. | HIGH | Audit table FK strategy, before_values snapshot, delete transaction | Do not use a cascading FK that removes audit events. Include enough feed identity in the delete event, such as name, source type, source feed id, and tags. |
| Queryable physical design | V1 is write-only at the API layer, but the table must be shaped for future per-feed and time-window reads. | MEDIUM | Table schema, indexes, retention design | Add indexes for `(feed_id, sequence)`, `(feed_id, occurred_at DESC)`, and likely `(occurred_at)` for retention. Avoid indexes on hot `feeds` columns. |
| 18-month retention enforcement | Retention is an active project requirement, not documentation-only. | MEDIUM | Audit table schema, migration/job mechanism, retention tests | Use the existing ordered migration and pg_cron maintenance pattern where appropriate. Retention deletes old audit rows only; it must not affect current feed rows. |
| Focused automated verification | The change cuts across storage, service, ingestion runtime, compatibility fields, deletion, and retention. | MEDIUM | Unit/component tests, schema tests, runtime tests | Cover create/update/deactivate/reset/delete, failure/quarantine/recovery, actor fields, before/after fields, compatibility alias behavior, retention, and transaction rollback. |

### Deferred Capabilities For Future Phases

| Capability | Why Defer | Complexity | Dependencies | Future Trigger |
|------------|-----------|------------|--------------|----------------|
| WD/backend event delivery | Delivery workers, signatures, retries, idempotency, receiver integration, and attempt state are separate delivery concerns. | HIGH | Stable v1 event contract, event ordering, retention, likely outbox/delivery table | Start after v1 ledger is proven and WD payload requirements are finalized. |
| Admin timeline read API | V1 should be write-only. Read APIs need pagination, auth, filtering, response shape, and backend/BFF contract work. | HIGH | Audit table, query indexes, event contract | Start when admin UI or support workflows are ready to consume event history. |
| Admin timeline UI | A UI would expand this milestone into feed frontend delivery, explicitly out of scope. | HIGH | Read API, shared types, UX requirements | Start in the GOO-574/admin timeline phase. |
| Consumer-specific webhook payloads | Canonical audit data should not be shaped around one downstream HTTP consumer. | MEDIUM | WD/backend delivery design | Add derived payload mappers when a delivery phase starts. |
| Historical baseline/backfill events | Synthetic "existing feed initialized" events can mislead operators into believing something happened at v1 rollout time. | MEDIUM | Data migration policy, product decision on historical meaning | Only consider if consumers explicitly need a baseline marker, and label it as synthetic. |
| Tamper-evident or WORM compliance hardening | Append-only database history satisfies v1 product needs; cryptographic chains or immutable storage add operational overhead. | HIGH | Compliance requirements, security review | Consider only if regulatory or incident-response requirements demand it. |
| Cross-domain audit events | Rules, transcripts, audio segments, notification delivery, and auth events have different ownership and semantics. | HIGH | Separate domain requirements | Keep v1 feed-only; create separate audit projects for other domains. |
| Search, analytics, and export workflows | Useful later, but they require product questions about who searches, what filters matter, and how much data can be exported. | MEDIUM | Read API, authorization, data volume review | Start after admin/support consumers validate the core timeline. |
| Rich actor directory enrichment | Display names, groups, and user profile history can drift and create privacy surface area. | MEDIUM | Stable actor IDs in v1 events | Resolve display labels at read time in a future consumer if needed. |
| Replay or rebuild current feed state from audit events | That is event sourcing. The project explicitly preserves `feeds` as current state. | HIGH | Full architecture redesign | Do not pursue for this milestone. |

### Anti-Features To Deliberately Avoid

| Anti-Feature | Why Requested | Why Problematic | Complexity/Risk Avoided | Alternative |
|--------------|---------------|-----------------|-------------------------|-------------|
| Full feed event sourcing | It sounds like the purest audit model. | Existing lease, failure, and UI paths depend on `feeds` as current state. Rebuilding state from events would be a rewrite. | HIGH | Keep `feeds` authoritative and write append-only audit events alongside successful mutations. |
| Storing webhook payloads as canonical audit rows | Future WD delivery needs payloads. | Delivery payloads are consumer contracts, not the durable domain model. They will change independently. | HIGH | Store domain audit events and derive delivery payloads later. |
| Auditing routine `active`/`unclaimed` lease churn and heartbeats | It gives "complete" lifecycle visibility. | Worker scheduling noise would swamp meaningful feed history and increase table volume without operator value. | HIGH | Audit meaningful feed mutations and failure/recovery outcomes only. Keep lease telemetry in logs/metrics. |
| Synthetic baseline events for all existing feeds | It makes every feed have a first timeline row. | It creates misleading history at rollout time and can be mistaken for real create events. | MEDIUM | Start history when v1 is deployed; let missing earlier history mean "not captured before v1." |
| Unbounded raw exception/provider response storage | Engineers want maximum diagnostic detail. | It risks secrets, tokens, PII, large rows, noisy timelines, and retention cost. | HIGH | Store bounded, scrubbed `status_reason_detail` plus stable `status_reason` enums. |
| Diagnostic text as control flow | It is easy to branch on existing free-text failure detail. | Free-form detail changes with provider messages and exception text; it is not stable policy. | MEDIUM | Branch on typed status/action enums only. |
| Duplicating one threshold-crossing failure as both `feed.failure_reported` and `feed.quarantined` | It appears to preserve every step. | It creates double-counting and ambiguous timelines for one state mutation. | MEDIUM | Emit one outcome event: `feed.quarantined` for threshold crossing, `feed.failure_reported` for non-terminal persisted failures. |
| Cascading audit history on hard delete | It follows normal relational cleanup. | It destroys the audit trail exactly when deletion history matters most. | HIGH | Keep audit events independent from feed-row lifetime and store deleted feed identity in the event. |
| Mutable audit rows for corrections | It is tempting to patch wrong details. | Mutable history undermines auditability and makes consumer state hard to reason about. | MEDIUM | Add a correction/superseding event in a future phase if needed; retention purge is the only normal deletion. |
| Building read APIs, UI, and delivery in v1 | It gives immediate visible value. | It expands the milestone into multiple consumer products before the data contract is stable. | HIGH | Ship the write-only ledger and documented contract first. |
| Storing delivery attempts in `feed_audit_events` | Delivery needs observability too. | It mixes domain facts with transport attempts and complicates retention/query semantics. | MEDIUM | Use a separate future delivery/outbox table for attempts. |

## Feature Dependencies

```text
status_reason_detail migration
    -> diagnostic detail cap/scrub contract
    -> failure/quarantine/recovery event payloads

event action taxonomy
    -> feed_audit_events schema
    -> write-only contract docs
    -> future WD delivery
    -> future admin timeline read API/UI

feed_audit_events schema
    -> per-feed sequence and query indexes
    -> transactional mutation + audit insert
    -> deletion-safe audit snapshots
    -> 18-month retention enforcement

actor attribution model
    -> BFF/backend actor propagation for admin mutations
    -> create/update/deactivate/reset/delete audit events

existing failure policy and runtime paths
    -> failure_reported/quarantined/recovered semantics
    -> runtime-focused tests
```

### Dependency Notes

- **Diagnostic detail must land before failure event payloads:** `quarantine_reason` is too narrow for non-quarantine failures, while `status_reason_detail` is the canonical v1 detail field.
- **The event contract must precede implementation:** Storage, docs, tests, and future consumers all depend on the same action names and field semantics.
- **Transactional storage changes are the highest-risk engineering dependency:** The current store methods often issue one SQL statement per mutation. V1 needs transaction-scoped before snapshots, mutation, sequence allocation, and audit insert.
- **Actor attribution depends on request plumbing:** The backend service currently verifies service OIDC tokens, while the BFF holds the admin user's email/admin status. Human attribution requires explicit trusted propagation or a backend-visible end-user claim.
- **Recovery is not lease acquisition:** A retryable failing feed may be claimed as `active`, but the audit-worthy recovery is the successful clearing of persisted failure state, not the scheduling mechanics.
- **Delete events require pre-delete state:** Once `delete_feed` removes feed-related rows, the audit insert must already contain the identity and before-values needed for future investigations.

## MVP Definition

### Launch With (v1)

- [ ] `feed_audit_events` table with append-only event rows, per-feed sequence, action, occurred time, actor, status/reason/detail, and bounded before/after JSON.
- [ ] `status_reason_detail` on `feeds`, bounded and scrubbed, with `quarantine_reason` kept as a compatibility alias for one release.
- [ ] Transactional audit writes for create, update, deactivate, reset, delete, budgeted/non-budgeted failure, quarantine threshold crossing, and recovery-by-success.
- [ ] Minimal actor attribution for `admin_user`, `system_runtime`, and `system_service`, including BFF-to-backend propagation for admin mutations or an explicitly documented fallback if unavailable.
- [ ] Retention enforcement for audit rows older than 18 months.
- [ ] Contract documentation and focused tests for storage, service/API compatibility, ingestion failure/quarantine/recovery, deletion survival, and retention.

### Add After Validation (v1.x)

- [ ] Internal query helper or store method for phase-specific verification only, if direct SQL becomes too brittle during rollout.
- [ ] Additional event metadata such as source component, trace id, or request id where existing plumbing makes it cheap and reliable.
- [ ] Narrow event-volume monitoring for failure storms and retention-job impact.

### Future Consideration (v2+)

- [ ] WD/backend delivery with signed payloads, retries, idempotency, and delivery attempt state.
- [ ] Admin timeline read API and UI.
- [ ] Consumer-specific derived payload mappers.
- [ ] Tamper-evident storage or immutable archive if compliance requirements emerge.
- [ ] Cross-domain audit ledgers for rules, transcripts, audio segments, notifications, or auth.

## Feature Prioritization Matrix

| Feature | User Value | Implementation Cost | Priority |
|---------|------------|---------------------|----------|
| Append-only audit table | HIGH | HIGH | P1 |
| Event taxonomy and docs | HIGH | MEDIUM | P1 |
| Transactional mutation + audit insert | HIGH | HIGH | P1 |
| Per-feed ordering | HIGH | MEDIUM | P1 |
| Actor attribution | HIGH | HIGH | P1 |
| `status_reason_detail` migration | HIGH | MEDIUM | P1 |
| Failure/quarantine/recovery semantics | HIGH | HIGH | P1 |
| Deletion-safe snapshots | HIGH | HIGH | P1 |
| 18-month retention | MEDIUM | MEDIUM | P1 |
| Internal verification query helper | MEDIUM | LOW | P2 |
| Read API/admin timeline | HIGH | HIGH | P3 |
| WD/backend delivery | HIGH | HIGH | P3 |
| Historical backfill/baseline | LOW | MEDIUM | P3 |
| Tamper-evident archive | LOW | HIGH | P3 |

**Priority key:**
- P1: Required for v1 launch.
- P2: Helpful after the core ledger works, but not required for the public contract.
- P3: Future phase or deliberately out of v1 scope.

## Sources

- [HIGH] `.planning/PROJECT.md` - project scope, active requirements, out-of-scope boundaries, and key decisions for Feed Audit Events V1.
- [HIGH] `.planning/codebase/ARCHITECTURE.md` - store/service/runtime boundaries and anti-patterns such as diagnostic text as control flow.
- [HIGH] `.planning/codebase/CONCERNS.md` - fragile feed-store SQL, auth boundary, pg_cron/recovery concerns, and test gaps.
- [HIGH] `backend/pipeline/storage/feed_store.py` and `backend/pipeline/storage/feed_queries.py` - current feed mutation, failure, reset, delete, recovery, and diagnostic-detail storage paths.
- [HIGH] `backend/pipeline/ingestion/collector_runtime.py` and `backend/pipeline/ingestion/failure_policy.py` - runtime failure policy, quarantine telemetry, non-budgeted failures, and recovery-by-success behavior.
- [HIGH] `backend/services/feeds/main.py`, `backend/services/feeds/service.py`, `frontend/api/src/feeds/feedsController.ts`, and `frontend/api/src/authentication.ts` - admin mutation API surface and actor-attribution plumbing gap.
- [HIGH] OWASP Logging Cheat Sheet - application logging should be application-owned, purpose-driven, include enough event context for analysis, and exclude sensitive data: https://cheatsheetseries.owasp.org/cheatsheets/Logging_Cheat_Sheet.html
- [MEDIUM] NIST CSRC Audit Log Glossary and NIST SP 800-92 - audit logs are chronological records/documentary evidence, and audit records commonly include timestamps, event/status/error codes, service names, and user/system accounts: https://csrc.nist.gov/glossary/term/audit_log and https://nvlpubs.nist.gov/nistpubs/legacy/SP/nistspecialpublication800-92.Pdf

---
*Feature research for: Feed Audit Events V1*
*Researched: 2026-06-19*
