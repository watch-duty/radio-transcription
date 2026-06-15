# Phase 5: Producer And Runtime Routing Merge - Context

**Gathered:** 2026-06-15
**Status:** Ready for planning

<domain>
## Phase Boundary

Phase 5 merges the Phase 4 strict policy table into the current producer and
runtime paths. Current VM collectors should emit the split backend
`FeedStatusReason` values for known root causes, and `CollectorRuntime` should
execute `FailurePolicyDecision` consistently for both collector `FeedFailure`
and runtime `_PipelineFailure`.

This phase does not add a database migration, durable outbox/replay worker,
source-class breaker state, persistent audit table, new feed lifecycle status,
OpenAPI/frontend/generated compatibility updates, or Echo parity. Phase 6 owns
API/UI compatibility and final documentation synchronization.

</domain>

<decisions>
## Implementation Decisions

### Runtime Policy Execution

- **D-01:** `_PipelineFailure` must use the same policy branch shape as
  collector `FeedFailure`: classify `status_reason + policy_evidence`, then
  execute `_record_feed_failure(...)` only when `failure_policy.is_feed_quarantine(decision)`
  is true; otherwise execute `_record_non_budgeted_failure(...)`.
- **D-02:** Do not duplicate status-specific routing conditions in
  `collector_runtime.py`. Runtime may compute small execution flags from the
  returned `FailurePolicyDecision`, but the policy table remains the routing
  authority.
- **D-03:** `pipeline_publish_after_bookmark_failed` with Pub/Sub publish-stage
  pipeline evidence must call `FeedStore.report_feed_failure(...)` and respect
  the existing `feed_failure_threshold`. It must not call
  `release_non_budgeted_failure(...)` for that matching evidence row.
- **D-04:** GCS upload and bookmark-write `_PipelineFailure` events remain
  `system_pipeline_error` with pipeline evidence and must continue through
  `release_non_budgeted_failure(...)`.
- **D-05:** Non-budgeted source, broad collector, broad pipeline,
  credential-access, unexpected, and telemetry-gap routes should keep the
  current reset/release behavior: `status='failing'`, `failure_count=0`,
  bounded `retry_after`, no `quarantine_reason`, and no quarantine telemetry.
- **D-06:** Preserve post-bookmark data-gap visibility without keeping the old
  special event as routing authority. Preferred minimal implementation: allow
  `_record_feed_failure(...)` to emit the canonical policy-decision log with
  `replay_missing=True` and `data_gap_known=True` for Pub/Sub post-bookmark
  failures. The legacy `post_bookmark_publish_failure` event is not required
  for v1.1 tests.
- **D-07:** Quarantine telemetry remains threshold-driven: emit
  `feed_quarantined` only when `report_feed_failure(...)` returns
  `"quarantined"`, not merely when a budgeted failure is recorded.

### Producer Status Splits

- **D-08:** Keep `system_configuration_invalid` for feed-row or
  source-specific feed configuration problems, such as missing
  `source_feed_id`, invalid configured feed identifiers, invalid per-feed URLs,
  or invalid feed-selected transport.
- **D-09:** Use `system_runtime_configuration_invalid` for shared runtime,
  deployment, environment, or source-class configuration that retry will not
  repair. Current examples include missing Broadcastify Calls JWT project/secret
  env config and missing Fire Notifications runtime/auth env configuration.
- **D-10:** Use `system_credential_access_failed` when Watch Duty failed to
  retrieve/access internal credentials, not when the upstream provider rejected
  the credential. Current example: Broadcastify Calls Secret Manager access
  failure. This remains non-budgeted in Phase 5 so a future retry-forever flip
  is a one-row policy change.
- **D-11:** Keep `system_authentication_failed` for upstream/provider
  authentication or authorization rejection after the collector's retry/refresh
  policy. Current 401/403 API/media cases should remain auth failures unless
  the root cause is internal credential retrieval.
- **D-12:** Use `system_source_payload_invalid` when a successful source
  response violates the collector payload contract and repeating the same
  request is not expected to help. Current examples include Calls API malformed
  payload shape, Fire Notifications malformed JSON/payload shape, and
  source-provided invalid OpenMHz media URLs.
- **D-13:** Keep broad `system_collector_error` for ambiguous or mixed failures
  that still do not have a clear owner after direct evidence, including mixed
  all-items-failed aggregation and ffmpeg process failures without source-probe
  evidence.
- **D-14:** Prefer direct substitutions at the producer sites or narrow helper
  defaults where the helper contract is already source-payload-specific. Do not
  introduce a new abstraction unless it removes duplicated mapping logic.

### Verification Boundary

- **D-15:** Add RED/GREEN tests at the behavior boundary being changed:
  collector/helper tests for split producer mappings, runtime tests for
  `_PipelineFailure` budgeted versus non-budgeted store calls, and focused
  storage/runtime tests for reset/quarantine telemetry boundaries.
- **D-16:** Runtime tests should assert store method calls first:
  `report_feed_failure(...)` versus `release_non_budgeted_failure(...)`.
  Telemetry assertions are secondary and should only prove the operational
  record mirrors the routing decision.
- **D-17:** Phase 5 should update existing tests that currently assert the old
  non-budgeted Pub/Sub publish-after-bookmark behavior. Do not preserve those
  expectations by adding compatibility branches.
- **D-18:** Do not run or require broad local E2E/integration stacks for Phase
  5. Use targeted `safe-run -- uv run python -m pytest ... -q -n 0` commands
  for changed backend files.

### the agent's Discretion

The agent may choose the exact helper names, parametrization style, and whether
to group runtime changes into one helper or inline the existing `FeedFailure`
branch shape. Keep changes minimal and make future routing flips local to the
policy table whenever possible.

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Project Scope

- `.planning/PROJECT.md` — v1.1 core value, active milestone scope, and
  locked out-of-scope items.
- `.planning/REQUIREMENTS.md` — Phase 5 requirements: `RUN-11` through
  `RUN-16`, `TEST-13`, `TEST-14`, and `TEST-15`.
- `.planning/ROADMAP.md` — Phase 5 goal, success criteria, and plan split.

### Prior Phase Decisions

- `.planning/phases/02-runtime-routing-and-telemetry/02-CONTEXT.md` — Runtime
  ownership, telemetry-as-audit guidance, non-budgeted execution, and v1
  publish-gap context.
- `.planning/phases/03-verification-and-compatibility/03-CONTEXT.md` — Focused
  verification and compatibility boundary decisions.
- `.planning/phases/04-strict-policy-table-and-status-vocabulary/04-CONTEXT.md`
  — Explicit policy rows, split status vocabulary, and Phase 5 hand-off.
- `.planning/phases/04-strict-policy-table-and-status-vocabulary/04-VERIFICATION.md`
  — Verified Phase 4 behavior and known Phase 5/6 deferrals.

### Codebase Maps

- `.planning/codebase/ARCHITECTURE.md` — Collector/runtime/storage ownership
  boundaries.
- `.planning/codebase/CONCERNS.md` — Quarantine-budget mismatch, publish-gap
  risk, and fragile runtime/store areas.
- `.planning/codebase/TESTING.md` — Focused test expectations and local safety
  guidance.

### Source Files To Inspect

- `backend/pipeline/ingestion/failure_policy.py` — Phase 4 policy table and
  decision predicates; do not bypass this table in runtime.
- `backend/pipeline/ingestion/collector_runtime.py` — `_PipelineFailure`,
  `_process_captured_chunk(...)`, `_record_feed_failure(...)`,
  `_record_non_budgeted_failure(...)`, and `_process_feed(...)` catch arms.
- `backend/pipeline/ingestion/collectors/failure_classification.py` — owner
  scope mapping and evidence construction for split status reasons.
- `backend/pipeline/ingestion/collectors/bcfy_calls/bcfy_calls_collector.py` —
  Calls JWT config/access, Calls API payload validation, media item failures.
- `backend/pipeline/ingestion/collectors/fire_notifications/collector.py` —
  Fire runtime config, auth handling, poll payload validation, item promotion.
- `backend/pipeline/ingestion/collectors/icecast/icecast_collector.py` —
  stream auth/config, ffmpeg classification, and source probe behavior.
- `backend/pipeline/ingestion/collectors/openmhz/collector.py` — transport
  config, WebSocket upgrade failures, media URL payload validation, item
  promotion.
- `backend/pipeline/ingestion/collectors/aiohttp_requests.py` — JSON fetch and
  item download helper status defaults.
- `backend/pipeline/ingestion/collectors/payloads.py` — source payload shape
  helper that should emit `system_source_payload_invalid`.
- `backend/pipeline/ingestion/failure_classifiers/ffmpeg.py` — ffmpeg process
  fallback classification that should remain broad collector error unless
  source-probe evidence says otherwise.
- `backend/pipeline/ingestion/tests/test_collector_runtime.py` — runtime store
  call and telemetry tests, including old publish-gap expectations.
- `backend/pipeline/ingestion/collectors/tests/` — source-specific producer
  mapping tests to update.

### External Official Docs

- `https://docs.cloud.google.com/pubsub/docs/publish-topics-schema` — Pub/Sub
  schema publish failures return `INVALID_ARGUMENT`; retrying unchanged invalid
  messages is not expected to help.
- `https://docs.cloud.google.com/pubsub/docs/reference/error-codes` — Pub/Sub
  `INVALID_ARGUMENT` failures are request-invalid and fail again if retried.
- `https://docs.cloud.google.com/pubsub/docs/samples/pubsub-resume-publish-with-ordering-keys`
  — Ordering-key publish failures affect queued/future messages for the same
  key until publishing is resumed.

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets

- `failure_policy.classify_failure_policy(...)` and
  `failure_policy.is_feed_quarantine(...)` already provide the exact routing
  decision Phase 5 needs.
- `_record_feed_failure(...)` already handles thresholded
  `report_feed_failure(...)` plus quarantine telemetry on `"quarantined"`.
- `_record_non_budgeted_failure(...)` already handles retry-after,
  `release_non_budgeted_failure(...)`, and suppressed telemetry.
- `_pipeline_policy_evidence(...)` already creates pipeline evidence for GCS,
  bookmark, and Pub/Sub stages.
- `policy_evidence_for_status_reason(...)` lets producer tests validate
  status/evidence consistency after split enum substitutions.

### Established Patterns

- Collectors classify source-specific evidence; runtime executes side effects;
  policy remains pure.
- Feed state changes should be proven by store method calls and final storage
  state, not by log-only assertions.
- Item failures remain item-scoped until `ItemBatchOutcome.promoted_failure()`
  proves all attempted items failed.
- `quarantine_reason` remains forensic text only and must not drive routing.

### Integration Points

- `_process_feed(...)` currently branches correctly for collector
  `FeedFailure`, but `_PipelineFailure` still unconditionally uses the
  non-budgeted path.
- Existing runtime tests around
  `pipeline_publish_after_bookmark_failed` currently assert the old
  non-budgeted behavior and must become RED tests for the new v1.1 behavior.
- Producer tests already cover many old labels, making them the right place to
  update expectations for runtime config, credential access, and source payload
  splits.
- `backend/pipeline/ingestion/collectors/README.md` still states the v1
  non-budgeted publish-gap behavior; final doc synchronization belongs in Phase
  6 unless Phase 5 changes make the guide actively misleading for tests.

</code_context>

<specifics>
## Specific Ideas

- Runtime can add optional `replay_missing` and `data_gap_known` parameters to
  `_record_feed_failure(...)`, mirroring the existing non-budgeted helper
  signature without changing storage.
- The old special `post_bookmark_publish_failure` event should not be asserted
  by Phase 5 tests. If kept temporarily, it is supplemental observability only.
- Calls producer updates likely include:
  `calls_jwt_config_missing -> system_runtime_configuration_invalid`,
  `calls_jwt_secret_access_failed -> system_credential_access_failed`, and
  malformed Calls API payloads -> `system_source_payload_invalid`.
- Fire Notifications producer updates likely include missing shared env/auth
  config -> `system_runtime_configuration_invalid` and malformed poll payloads
  -> `system_source_payload_invalid`.
- OpenMHz producer updates likely include source-provided invalid media URLs ->
  `system_source_payload_invalid`; invalid selected transport remains
  feed-row/source-specific configuration invalid.
- ffmpeg process timeout/signal/exit fallback should remain
  `system_collector_error` unless same-endpoint probe evidence classifies a
  source condition.

</specifics>

<deferred>
## Deferred Ideas

- Durable publish outbox / hold-replay worker remains v2.
- Real source-class or credential breaker state remains v2.
- Persistent structured policy audit/event table remains v2.
- OpenAPI, generated API route metadata, shared frontend types, and UI labels
  remain Phase 6.
- Echo ingestion parity remains a follow-up outside this milestone.
- Full collector guide/doc synchronization remains Phase 6 unless a Phase 5
  test or implementation comment needs a small local correction.

</deferred>

---

*Phase: 5-Producer And Runtime Routing Merge*
*Context gathered: 2026-06-15*
