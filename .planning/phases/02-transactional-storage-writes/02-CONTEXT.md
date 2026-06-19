# Phase 2: Transactional Storage Writes - Context

**Gathered:** 2026-06-19
**Status:** Ready for planning

<domain>
## Phase Boundary

Phase 2 makes storage-owned admin/service feed mutations persist current-state
changes and their Feed Audit Events together. It covers feed create,
meaningful update, deactivate, reset, and hard delete. It does not cover
runtime failure, quarantine, recovery, Echo/sync ingestion, trusted admin actor
forwarding from the BFF, admin read APIs, Watch Duty delivery, UI work, or
audit retention.

This restarted context supersedes the earlier Phase 2 context in this same
directory. If there is any conflict, use this file. The biggest changed
decision is snapshot shape: Phase 2 now uses full allowlisted snapshots for all
audited storage lifecycle events, while suppressing no-op update events.

The current `feeds` and `feed_properties` rows remain the authoritative
current-state model. `feed_audit_events` is the durable append-only history
written by storage mutation methods in the same database transaction as the
state change.

</domain>

<decisions>
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

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Planning Context

- `.planning/PROJECT.md` - project scope, constraints, out-of-scope
  boundaries, and milestone-level decisions.
- `.planning/REQUIREMENTS.md` - Phase 2 requirements: AUD-04, EVT-01,
  EVT-02, EVT-03, EVT-04, EVT-05, CON-01, CON-02, CON-03, CON-04.
- `.planning/ROADMAP.md` - Phase 2 goal, dependencies, and success criteria.
- `.planning/STATE.md` - current milestone status and prior phase decisions.
- `.planning/phases/01-contract-and-schema-foundation/01-CONTEXT.md` -
  Phase 1 actor, deletion snapshot, diagnostic detail, and schema foundation
  decisions.
- `.planning/phases/01-contract-and-schema-foundation/01-VERIFICATION.md` -
  Phase 1 verification results and deferred follow-up context.

### Domain Contract And Schema

- `documentation/feed-audit-events.md` - canonical Feed Audit Event contract,
  action vocabulary, actor vocabulary, before/after semantics, and deletion
  snapshot allowlist.
- `terraform/modules/alloydb/sql/ingestion/029_feed_audit_events.sql` -
  current audit schema foundation, `feed_audit_events`,
  `feed_audit_event_sequences`, actor constraints, action constraints,
  sequence constraints, and JSON object shape checks.
- `terraform/modules/alloydb/sql/ingestion/003_feeds.sql` - base `feeds` row
  fields for snapshot allowlist decisions.
- `terraform/modules/alloydb/sql/ingestion/012_feed_properties.sql` - feed
  properties row and cascade relationship to deleted feeds.
- `terraform/modules/alloydb/sql/ingestion/013_feed_properties_source_type.sql`
  - source lookup uniqueness for feed properties.
- `terraform/modules/alloydb/sql/ingestion/020_quarantine_reason.sql` -
  current `quarantine_reason` compatibility field.
- `terraform/modules/alloydb/sql/ingestion/024_feeds_status_reason.sql` -
  canonical status reason fields.

### Storage And Service Code

- `backend/pipeline/storage/feed_store.py` - `FeedStore` mutation methods,
  current row-to-domain mapping, source/status enums, and storage ownership
  boundary.
- `backend/pipeline/storage/feed_queries.py` - SQL for create, update,
  deactivate, delete, reset, and existing failure paths.
- `backend/pipeline/storage/tests/test_feed_store.py` - established storage
  unit test style and feed mutation tests to extend.
- `backend/pipeline/storage/tests/test_feed_audit_contract.py` - Phase 1
  contract/schema tests that must be updated when removing `system:`.
- `backend/pipeline/storage/tests/connection_util.py` - mock pool helper used
  by storage tests.
- `backend/services/feeds/main.py` - FastAPI feeds endpoints and current auth
  dependency shape.
- `backend/services/feeds/service.py` - thin service layer delegating feed
  mutations to `FeedStore`.

### Auth Boundary Context

- `frontend/common/src/types/auth.ts` - BFF user shape containing `sub`,
  `email`, and `isAdmin`.
- `frontend/api/src/authentication.ts` - BFF authentication path that populates
  `request.user`.
- `frontend/api/src/feeds/feedsController.ts` - admin checks for feed mutation
  routes and current downstream calls to feeds service.
- `frontend/api/src/utils.ts` - downstream service client construction using
  Google ID token clients.
- `backend/pipeline/common/auth.py` - Python service OIDC verification helper
  currently used as a global dependency.

### Codebase Maps

- `.planning/codebase/ARCHITECTURE.md` - storage/service/runtime boundaries and
  current feed state architecture.
- `.planning/codebase/STACK.md` - Python, asyncpg, AlloyDB, FastAPI,
  migration, and test stack.
- `.planning/codebase/CONCERNS.md` - FeedStore fragility, HOT/index concerns,
  auth-boundary risks, and storage testing cautions.

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets

- `FeedStore._row_to_feed`: converts joined feed rows into the current `Feed`
  typed dict and already normalizes tags/status/source enum values.
- `feed_queries.CREATE_FEED_SQL`, `UPDATE_FEED_SQL`, `DEACTIVATE_FEED_SQL`,
  `DELETE_FEED_SQL`, and `RESET_FEED_SQL`: current mutation SQL entry points to
  wrap or revise for transactional audit writes.
- `feed_audit_event_sequences`: schema foundation for transactional per-feed
  sequence allocation.
- `make_mock_pool`: existing storage unit tests use async mocks; it may need a
  transaction-capable extension for Phase 2.

### Established Patterns

- FastAPI feed endpoints are thin and delegate through `FeedService` to
  `FeedStore`.
- `FeedStore` owns feed lifecycle SQL and is the right boundary for preventing
  state/history drift.
- Existing admin mutation calls do not yet carry trusted human actor identity
  into Python storage. Phase 2 requires `actor_id`; Phase 3 will wire trusted
  propagation.
- Current delete SQL manually deletes audio segments and transcripts before
  deleting `feeds`; `feed_properties` is removed by cascade. The audit snapshot
  must read feed properties before the hard delete.
- FeedStore SQL is a known fragile area because leasing depends on generated
  SQL shape, fencing tokens, `SKIP LOCKED`, and HOT-safe indexes. Phase 2
  should keep audit write changes out of heartbeat/lease churn paths.

### Integration Points

- Storage mutation methods: `create_feed`, `update_feed`, `deactivate_feed`,
  `delete_feed`, `reset_feed`.
- SQL module: add audit insert/sequence allocation statements near feed
  mutation SQL or a storage-owned helper query section.
- Contract cleanup: remove accepted `system:` prefix from docs, migration
  constraints, and Phase 1 contract tests before emitting real audit data.
- Service fallback: feed service calls should pass `service:feeds-service` until
  Phase 3 replaces it with trusted user actor forwarding where available.

</code_context>

<specifics>
## Specific Ideas

- User explicitly restarted the Phase 2 discussion and selected areas 1, 2,
  and 3 for reconsideration.
- User chose required `actor_id` for audited storage methods.
- User chose to suppress no-op `feed.updated` events while returning the feed
  normally.
- User chose full allowlisted snapshots for all Phase 2 event before/after
  values.
- Previously accepted actor clarification still applies: `service:feeds-service`
  is the Phase 2 fallback; do not emit fake nullable user actors; remove
  `system:` before real audit events are emitted.

</specifics>

<deferred>
## Deferred Ideas

- Trusted admin actor forwarding from BFF to feeds service belongs to Phase 3.
- Runtime failure, quarantine, recovery, Echo/sync coverage, and no-lease-churn
  behavior belong to Phase 4.
- Retention enforcement and final database-level verification hardening belong
  to Phase 5.
- Admin timeline read APIs, UI, and Watch Duty backend webhook delivery remain
  out of scope for v1.

</deferred>

---

*Phase: 2-Transactional Storage Writes*
*Context gathered: 2026-06-19*
