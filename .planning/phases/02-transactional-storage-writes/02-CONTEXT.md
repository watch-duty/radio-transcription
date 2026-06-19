# Phase 2: Transactional Storage Writes - Context

**Gathered:** 2026-06-19
**Status:** Ready for planning

<domain>
## Phase Boundary

Phase 2 makes storage-owned admin/service feed mutations persist current-state
changes and their Feed Audit Events together. It covers feed create, meaningful
update, deactivate, reset, and hard delete. It does not cover runtime failure,
quarantine, recovery, Echo/sync ingestion, trusted admin actor forwarding from
the BFF, admin read APIs, Watch Duty delivery, UI work, or audit retention.

The current `feeds` and `feed_properties` rows remain the authoritative
current-state model. `feed_audit_events` is the durable append-only history
written by storage mutation methods in the same database transaction as the
state change.

</domain>

<decisions>
## Implementation Decisions

### Audit Producer Boundary

- **D-01:** `FeedStore` owns Feed Audit Event creation for Phase 2 mutations.
  Service and runtime callers may pass causal inputs such as `actor_id`, but
  they must not build or insert audit rows directly.
- **D-02:** Implement audit writing through private storage helpers or tightly
  scoped storage-owned functions. The helper should be reused by audited
  `FeedStore` methods and hidden from service-layer call sites.
- **D-03:** A current-state mutation and its audit row must be committed or
  rolled back as one unit. Existing one-shot pool calls need to move to an
  explicit connection transaction where necessary.

### Snapshot Shape

- **D-04:** Use a hybrid allowlist model for `before_values` and
  `after_values`.
- **D-05:** `feed.created` uses `before_values = {}` and `after_values` as a
  full maintained allowlisted feed snapshot.
- **D-06:** `feed.deleted` uses a full maintained allowlisted feed snapshot in
  `before_values` and `after_values = {}`. This snapshot is the
  self-contained deleted-feed record and must be captured before the current
  row and cascading `feed_properties` row are removed.
- **D-07:** `feed.updated`, `feed.deactivated`, and `feed.reset` use changed
  allowlisted fields only, so timelines stay readable and avoid copying
  unchanged state into every event.
- **D-08:** The maintained snapshot allowlist includes meaningful feed row
  fields plus `feed_properties.source_feed_id` and `feed_properties.tags`.
  Default snapshots exclude noisy worker/heartbeat lease fields unless a later
  phase proves they are needed.

### Actor Handling

- **D-09:** Phase 2 storage methods accept an `actor_id` parameter for audited
  mutations.
- **D-10:** Until Phase 3 wires trusted admin identity from the BFF/service
  boundary, feeds-service API mutations use the deliberate fallback
  `service:feeds-service`.
- **D-11:** Do not use `user:null`, `user:`, empty suffixes, or other fake user
  actors. If a trusted human `sub` exists, use `user:google:<sub>`. If only a
  trusted email exists, use `user-email:<normalized_email>`. If neither exists,
  use `unknown:unknown` only as an explicit rare fallback or reject the admin
  mutation in the later service-boundary phase.
- **D-12:** Remove the `system:` actor prefix from the v1 contract before any
  audit rows are emitted. It overlaps with clearer categories and is likely to
  become a vague dumping ground. Keep `service:`, `job:`, `gcp-sa:`,
  `user:google:`, `user-email:`, and `unknown:unknown`.
- **D-13:** `gcp-sa:<service_account_email>` remains reserved for cases where
  the authenticated GCP workload principal is the only known origin and no
  semantic service/job actor is available.

### Per-Feed Ordering

- **D-14:** Allocate `feed_sequence` inside the same database transaction as
  the audited mutation and audit insert.
- **D-15:** Use `feed_audit_event_sequences` as the sequence allocator with
  row locking or atomic upsert/update semantics.
- **D-16:** Do not compute the next sequence from
  `MAX(feed_sequence) + 1`; that shape is race-prone under concurrent
  mutations.
- **D-17:** Concurrent audited mutations for the same feed must produce unique
  deterministic per-feed ordering without service or runtime callers managing
  ordering.

### Transaction And Test Scope

- **D-18:** Phase 2 tests must prove the storage methods write the expected
  audit action, actor, sequence, and before/after payload for create, update,
  deactivate, reset, and delete.
- **D-19:** Tests must prove no audit row is left behind when the state
  mutation fails or rolls back.
- **D-20:** Mocked storage tests are acceptable for method-level transaction
  and payload shape, but the plan should include the strongest practical
  database-level coverage available in the repo for sequence allocation and
  rollback semantics.

### the agent's Discretion

The user delegated exact helper names, SQL shape, and initial allowlist
implementation details to the agent as long as the decisions above are
preserved. Prefer the existing storage style and keep the implementation
localized to `FeedStore`, `feed_queries`, focused tests, and contract docs/schema
cleanup for the actor-prefix vocabulary.

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Planning Context

- `.planning/PROJECT.md` — project scope, constraints, out-of-scope boundaries,
  and milestone-level decisions.
- `.planning/REQUIREMENTS.md` — Phase 2 requirements: AUD-04, EVT-01, EVT-02,
  EVT-03, EVT-04, EVT-05, CON-01, CON-02, CON-03, CON-04.
- `.planning/ROADMAP.md` — Phase 2 goal, dependencies, and success criteria.
- `.planning/STATE.md` — current milestone status and prior phase decisions.
- `.planning/phases/01-contract-and-schema-foundation/01-CONTEXT.md` — Phase 1
  actor, deletion snapshot, diagnostic detail, and schema foundation decisions.
- `.planning/phases/01-contract-and-schema-foundation/01-VERIFICATION.md` —
  Phase 1 verification results and deferred follow-up context.

### Domain Contract And Schema

- `documentation/feed-audit-events.md` — canonical Feed Audit Event contract,
  action vocabulary, actor vocabulary, before/after semantics, and deletion
  snapshot allowlist.
- `terraform/modules/alloydb/sql/ingestion/029_feed_audit_events.sql` — current
  audit schema foundation, `feed_audit_events`, `feed_audit_event_sequences`,
  actor constraints, action constraints, sequence constraints, and JSON object
  shape checks.
- `terraform/modules/alloydb/sql/ingestion/003_feeds.sql` — base `feeds` row
  fields for snapshot allowlist decisions.
- `terraform/modules/alloydb/sql/ingestion/012_feed_properties.sql` — feed
  properties row and cascade relationship to deleted feeds.
- `terraform/modules/alloydb/sql/ingestion/013_feed_properties_source_type.sql`
  — source lookup uniqueness for feed properties.
- `terraform/modules/alloydb/sql/ingestion/020_quarantine_reason.sql` — current
  `quarantine_reason` compatibility field.
- `terraform/modules/alloydb/sql/ingestion/024_feeds_status_reason.sql` —
  canonical status reason fields.

### Storage And Service Code

- `backend/pipeline/storage/feed_store.py` — `FeedStore` mutation methods,
  current row-to-domain mapping, source/status enums, and storage ownership
  boundary.
- `backend/pipeline/storage/feed_queries.py` — SQL for create, update,
  deactivate, delete, reset, and existing failure paths.
- `backend/pipeline/storage/tests/test_feed_store.py` — established storage unit
  test style and feed mutation tests to extend.
- `backend/pipeline/storage/tests/test_feed_audit_contract.py` — Phase 1
  contract/schema tests that must be updated when removing `system:`.
- `backend/pipeline/storage/tests/connection_util.py` — mock pool helper used by
  storage tests.
- `backend/services/feeds/main.py` — FastAPI feeds endpoints and current auth
  dependency shape.
- `backend/services/feeds/service.py` — thin service layer delegating feed
  mutations to `FeedStore`.

### Auth Boundary Context

- `frontend/common/src/types/auth.ts` — BFF user shape containing `sub`,
  `email`, and `isAdmin`.
- `frontend/api/src/authentication.ts` — BFF authentication path that populates
  `request.user`.
- `frontend/api/src/feeds/feedsController.ts` — admin checks for feed mutation
  routes and current downstream calls to feeds service.
- `frontend/api/src/utils.ts` — downstream service client construction using
  Google ID token clients.
- `backend/pipeline/common/auth.py` — Python service OIDC verification helper
  currently used as a global dependency.

### Codebase Maps

- `.planning/codebase/ARCHITECTURE.md` — storage/service/runtime boundaries and
  current feed state architecture.
- `.planning/codebase/STACK.md` — Python, asyncpg, AlloyDB, FastAPI, migration,
  and test stack.
- `.planning/codebase/CONCERNS.md` — FeedStore fragility, HOT/index concerns,
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
  into Python storage. Phase 2 should accept `actor_id`; Phase 3 will wire
  trusted propagation.
- Current delete SQL manually deletes audio segments and transcripts before
  deleting `feeds`; `feed_properties` is removed by cascade. The audit snapshot
  must read feed properties before the hard delete.

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

- Treat "currently this request was user-triggered but the user ID was not
  forwarded" as `service:feeds-service` in Phase 2, not as `user:null` or
  `user:`.
- Use `user-email:<normalized_email>` only when a trusted email exists but a
  Google `sub` does not.
- Keep `unknown:unknown` explicit and rare. It is better than pretending a
  malformed user actor is valid.
- Tighten the actor vocabulary now because no audit rows exist yet.

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
