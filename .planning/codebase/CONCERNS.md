# Codebase Concerns

**Analysis Date:** 2026-06-14

## Tech Debt

**Feed quarantine budget is too coarse:**
- Issue: `FeedFailure`, `_PipelineFailure`, and unexpected runtime exceptions
  all route through `_record_feed_failure` and
  `FeedStore.report_feed_failure`.
- Files: `backend/pipeline/ingestion/collector_runtime.py`,
  `backend/pipeline/storage/feed_store.py`,
  `backend/pipeline/storage/feed_queries.py`.
- Impact: post-capture pipeline failures and non-feed-actionable source
  observations can consume the same persisted failure budget as true
  feed-actionable failures.
- Fix approach: introduce structured policy evidence and route non-quarantine
  cases through a non-budgeted release/suppression path.

**`quarantine_reason` is raw forensic text:**
- Issue: `quarantine_reason` stores bounded raw reason text on quarantine
  transitions.
- Files: `terraform/modules/alloydb/sql/ingestion/020_quarantine_reason.sql`,
  `backend/pipeline/storage/feed_queries.py`.
- Impact: parsing it for policy would be brittle and would encode old incident
  strings as behavior.
- Fix approach: use canonical structured fields for policy decisions and keep
  raw reason for investigation only.

**Generated/protobuf boundaries require manual refresh:**
- Issue: generated Python protobuf files are not the authoritative source and
  must be regenerated after proto changes.
- Files: `protos/`, `backend/pipeline/schema_types/`,
  `backend/pipeline/README.md`.
- Impact: schema and code can drift locally if generation is skipped.
- Fix approach: run `mise run generate:protos` after `.proto` edits and test
  producer/consumer compatibility.

## Known Bugs / Current Risk Areas

**Post-bookmark Pub/Sub publish failure creates a data gap:**
- Symptoms: `_process_captured_chunk` uploads GCS, writes feed progress, then
  publishes Pub/Sub. If publish fails after bookmark, the feed cursor has
  advanced but downstream processing did not receive the message.
- Files: `backend/pipeline/ingestion/collector_runtime.py`,
  `backend/pipeline/common/gcp_helper.py`.
- Workaround: current code records a pipeline failure against feed state.
- Root cause: no durable outbox/hold table for post-bookmark publish failures.
- Fix approach: v1 should suppress quarantine and record explicit telemetry;
  later work should add a replayable outbox or hold/replay lane.

**Paused ordering key handling can mask root cause:**
- Symptoms: `PublishToPausedOrderingKeyException` is caught, `resume_publish`
  is attempted, and the exception is re-raised into runtime failure handling.
- Files: `backend/pipeline/common/gcp_helper.py`.
- Impact: paused-key symptoms can appear as feed failures even though capture
  is healthy.
- Fix approach: classify as pipeline-owned, alert on publisher state/backlog,
  and avoid feed quarantine budget increments.

**Frontend status model is coarse:**
- Symptoms: `failing` and `quarantined` both map to UI `error`.
- Files: `frontend/common/src/utils/statusUtils.ts`,
  `frontend/common/src/types/feeds.ts`.
- Impact: using `failing` for non-budgeted suppressed retry is compatible but
  not semantically rich in UI.
- Fix approach: keep v1 behavior compatible; add richer status reason display
  later if operators need it.

## Security Considerations

**Source credentials and local env files:**
- Risk: source auth values, GCP settings, and local env values may exist in
  `.env` or `local_dev/LOCAL.env`.
- Current mitigation: docs and logs should mention env variable names only,
  never values.
- Recommendations: keep secret scanning in doc workflows and avoid copying raw
  request/URL/auth payloads into logs or `quarantine_reason`.

**Operator-facing raw reasons:**
- Risk: raw reason strings may accidentally include high-cardinality or
  sensitive detail if collectors bypass helper guidance.
- Current mitigation: `FeedFailure.reason` is bounded to 200 characters and
  collector docs forbid URLs, stderr blobs, tokens, IDs, request bodies, signed
  URLs, and secrets.
- Recommendations: prefer typed structured fields and stable reason tags over
  raw strings.

**API auth boundaries:**
- Risk: management APIs are protected by OIDC verification; frontend proxy and
  backend services must keep auth assumptions aligned.
- Current mitigation: FastAPI app dependencies use `verify_oidc_token`; frontend
  API uses Google auth/Jose libraries.
- Recommendations: when changing API routes, add auth tests or verify existing
  auth middleware still applies.

## Performance Bottlenecks

**Ingestion lease and heartbeat concurrency:**
- Problem: runtime targets up to hundreds of concurrent feed tasks per worker.
- Files: `backend/pipeline/ingestion/collector_runtime.py`,
  `backend/pipeline/storage/feed_queries.py`.
- Current mitigation: batch leasing, per-source caps, dedicated heartbeat pool,
  DB-truth held counts, cgroup RSS watchdog, and fenced writes.
- Safe modification: preserve heartbeat isolation, cancellation paths, and
  lease-lost invariants; add focused tests for allocation/release changes.

**Recovery query indexes:**
- Problem: recovery path may become expensive if failing/abandoned rows spike.
- Files: `backend/pipeline/storage/feed_queries.py`.
- Current note: code comments propose a future `idx_feeds_recovery` if recovery
  P99 exceeds 50 ms or pg_cron is paused.
- Safe modification: check HOT-protection guard and index side effects before
  adding indexes on frequently-mutated feed columns.

**Local validation cost:**
- Problem: broad test/lint/dev commands can invoke Docker containers,
  emulators, notebooks, Terraform, and frontend builds.
- Files: `AGENTS.md`, `.agents/instructions.md`, `.mise.toml`.
- Current mitigation: instructions require narrow local checks by default.
- Safe modification: use `safe-run --` and targeted tests.

## Fragile Areas

**Collector/runtime boundary:**
- Why fragile: collectors and runtime divide ownership of retries,
  classification, upload, publish, bookmarks, and failure state.
- Common failures: collectors raising feed-level failures for item-scoped
  issues, runtime inferring source semantics from raw strings, or pipeline
  failures mutating feed quarantine budget.
- Safe modification: read `backend/pipeline/ingestion/collectors/README.md`
  before changes and add tests at the boundary.

**Feed lifecycle SQL:**
- Why fragile: feed status, failure count, retry delay, worker ID, fencing
  token, heartbeat, and retry-after state interact in a few atomic SQL paths.
- Common failures: releasing a lease too early, incrementing failure count in a
  non-quarantine path, failing to clear stale status reason on progress, or
  making quarantined feeds claimable.
- Safe modification: add storage tests that assert final row state.

**Pub/Sub schema contracts:**
- Why fragile: schema validation failures happen at publish time and can be
  deterministic poison-message failures.
- Common failures: producer and topic schema revisions drift.
- Safe modification: validate serialization and schema compatibility before
  rollout; avoid hot-path retries for deterministic invalid payloads.

**Generated frontend API artifacts:**
- Why fragile: `frontend/api` uses tsoa-generated routes/specs.
- Common failures: changing controllers/models without regenerating routes or
  OpenAPI output.
- Safe modification: run package scripts such as `generate-routes`,
  `generate-spec`, and `verify-spec` when API contracts change.

## Dependencies at Risk

**Apache Beam / Python version coupling:**
- Risk: normalization package comments warn that Beam version and Docker base
  image should move together.
- Impact: dependency upgrades can break the normalization image or runtime.
- Migration plan: update `backend/pipeline/normalization/pyproject.toml` and
  `backend/pipeline/normalization/Dockerfile` together.

**Frontend major versions:**
- Risk: React 19, Vite 8, TypeScript 6, ESLint 10, and Material UI 9 are
  current-major dependencies where plugin compatibility can be strict.
- Impact: frontend lint/build failures after package upgrades.
- Migration plan: upgrade one package family at a time and run targeted
  package checks.

## Missing Critical Features

**Durable publish outbox / hold-replay lane:**
- Problem: no v1 durable replay path for post-bookmark publish gaps.
- Current workaround: suppress feed quarantine and emit explicit data-gap
  telemetry in the planned policy redesign.
- Blocks: guaranteed replay after downstream publish repair.
- Implementation complexity: medium to high, because it touches ordering,
  idempotency, storage, replay workers, and alerting.

**Fleet-wide source-class breakers:**
- Problem: current feed state is per-feed; shared source/auth/provider failures
  can fan out as many feed symptoms.
- Current workaround: planned v1 can emit policy intent without actual breaker
  state.
- Blocks: automatic suppression/canary behavior for source-class incidents.
- Implementation complexity: medium, likely requiring shared state and metrics.

**Persistent structured failure audit events:**
- Problem: v1 plan logs structured policy decisions but does not persist all
  structured policy fields in DB.
- Current workaround: logs and current-schema fields.
- Blocks: complete historical root-cause analytics from database alone.
- Implementation complexity: medium, requires event schema/storage and
  retention policy.

## Test Coverage Gaps To Watch

**Quarantine policy redesign:**
- Must test: pipeline failures do not call `report_feed_failure`, unannotated
  failures become telemetry gaps, source/offline/auth/rate-limit paths use a
  non-budgeted release, and real feed config failures still use the budgeted
  path.

**Post-bookmark publish gap telemetry:**
- Must test: logs include `replay_missing=true`, `data_gap_known=true`,
  `policy_intent=hold_for_replay`, and non-quarantine executed action.

**Frontend status reason visibility:**
- Risk: new backend `status_reason` values may need shared types/OpenAPI/UI
  updates even when the lifecycle `status` remains `failing`.

---

*Concerns audit: 2026-06-14*
*Update as issues are fixed or new ones are discovered*
