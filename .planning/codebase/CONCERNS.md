# Codebase Concerns

**Analysis Date:** 2026-06-26

## Tech Debt

**Evaluation processor failure policy is inconsistent:**
- Issue: `backend/pipeline/evaluation/processor.py` mixes permanent-drop and retry behavior inside one message path. Empty CloudEvent raw data and empty evaluation payloads log and return, while missing required fields raise `ValueError`, and publish failures bubble from `future.result()` without explicit classification.
- Files: `backend/pipeline/evaluation/processor.py`, `backend/pipeline/evaluation/tests/test_processor.py`
- Impact: malformed evaluation messages, annotation write failures, or empty evaluator outputs can be acknowledged without durable operator visibility, while other malformed inputs retry indefinitely.
- Fix approach: define a permanent/transient error policy in `backend/pipeline/evaluation/processor.py`, route permanent failures to a dead-letter or audit path, and add tests in `backend/pipeline/evaluation/tests/test_processor.py` for parse failure, empty payload, annotation failure, and publish failure outcomes.

**Gemini transcription lacks local retry and context integration:**
- Issue: `backend/pipeline/transcription/transcribers/gemini.py` calls `client.models.generate_content(...)` directly and contains TODOs for contextual prompting, retry behavior, and a fine-tuned model.
- Files: `backend/pipeline/transcription/transcribers/gemini.py`, `backend/pipeline/transcription/transcribers/prompts.py`
- Impact: transient GenAI/API failures rely on upstream retry behavior, prompt quality cannot use segment context, and model-selection work remains manual.
- Fix approach: add bounded retry/backoff around the Gemini request in `backend/pipeline/transcription/transcribers/gemini.py`, thread available segment/feed context into prompt creation in `backend/pipeline/transcription/transcribers/prompts.py`, and make model selection an explicit configuration option with tests.

**Fire Notifications ingestion has source-specific stubs:**
- Issue: `backend/pipeline/ingestion/collectors/fire_notifications/client.py` uses UTC as a placeholder channel timezone and de-duplicates files by timestamp with first-file-wins behavior when duplicate timestamped filenames appear.
- Files: `backend/pipeline/ingestion/collectors/fire_notifications/client.py`
- Impact: non-UTC channels can produce incorrect bookmark times, and duplicate timestamped files can be skipped without a durable record.
- Fix approach: add a feed/channel timezone mapping, include duplicate filename disambiguation in the collector state, and cover duplicate timestamps plus non-UTC feed behavior in collector tests under `backend/pipeline/ingestion/collectors/fire_notifications/`.

**Ingestion retry helper erases call signatures:**
- Issue: `backend/pipeline/ingestion/retry.py` accepts `fn` plus `*args: object`, and its TODO calls out a future `RetryConfig` plus coroutine-factory API.
- Files: `backend/pipeline/ingestion/retry.py`
- Impact: retry call sites lose static type checking for positional arguments and keyword arguments, making ingestion changes easier to wire incorrectly.
- Fix approach: replace the variadic helper with a typed retry configuration object and a zero-argument coroutine factory, then migrate ingestion call sites in `backend/pipeline/ingestion/`.

**Adding a claimable source type is a multi-place manual change:**
- Issue: `backend/pipeline/storage/feed_store.py` documents that a new claimable source type must update `SourceType`, seed SQL, and `backend.pipeline.ingestion.source_runtime_specs`; skipping the runtime spec means VM workers silently never claim the source.
- Files: `backend/pipeline/storage/feed_store.py`, `backend/pipeline/ingestion/source_runtime_specs.py`, `terraform/modules/alloydb/sql/ingestion/002_source_types.sql`, `terraform/modules/alloydb/sql/ingestion/006_seed_source_types.sql`
- Impact: source-type additions are fragile and can deploy successfully while leaving feeds permanently unclaimed.
- Fix approach: create a single source-type registry that drives enum validation, seed checks, runtime specs, and frontend/common typing, with a contract test that compares `backend/pipeline/storage/feed_store.py`, `backend/pipeline/ingestion/source_runtime_specs.py`, and the seed migrations.

**Feed hard-delete cleanup is incomplete:**
- Issue: `backend/pipeline/storage/feed_queries.py` marks hard-delete cleanup as TODO for remaining legacy transcript removal.
- Files: `backend/pipeline/storage/feed_queries.py`
- Impact: feed deletion semantics are not fully documented at the storage boundary, and legacy transcript or derived data cleanup can drift from the feed row deletion behavior.
- Fix approach: finish the hard-delete cleanup path in `backend/pipeline/storage/feed_queries.py`, document retained audit rows separately from removable derived data, and add delete-path coverage in `backend/pipeline/storage/tests/` and `integration_tests/storage/`.

**Audit actor identity can degrade to unresolved service actors:**
- Issue: `backend/pipeline/common/actor_identity.py` returns `service_account:gcp:unresolved` when GCP runtime is detected and `FEED_AUDIT_ACTOR_ID` is missing or malformed.
- Files: `backend/pipeline/common/actor_identity.py`, `CONTEXT.md`
- Impact: feed audit events remain writable, but service actor attribution loses precision and multiple misconfigured workloads collapse into one unresolved identity.
- Fix approach: keep the non-blocking write behavior, but add deployment validation and alerting for unresolved actor logs from `backend/pipeline/common/actor_identity.py`.

**Generated artifacts are easy to confuse with source of truth:**
- Issue: `.gitignore` excludes generated protobuf and TypeScript outputs, while local generated files can exist under `backend/pipeline/schema_types/` and frontend generated directories.
- Files: `.gitignore`, `.mise.toml`, `backend/pipeline/schema_types/`, `protos/`
- Impact: local generated outputs can mask stale source definitions during development, and changes must be made to `protos/` plus regeneration commands rather than ignored generated files.
- Fix approach: treat `protos/` as the editable source of truth, use `.mise.toml` generation tasks for regeneration, and avoid reviewing ignored generated files as durable implementation changes.

## Known Bugs

**Frontend auth accepts decoded bearer tokens without signature verification:**
- Symptoms: when gateway userinfo headers are absent, `frontend/api/src/authentication.ts` reads `Authorization` and uses `jose.decodeJwt<GoogleUser>(token)` rather than verifying signature, issuer, and audience.
- Files: `frontend/api/src/authentication.ts`, `frontend/api/src/authentication.test.ts`, `frontend/api/src/config.ts`
- Trigger: a request reaches the BFF without `x-apigateway-api-userinfo` or `x-endpoint-api-userinfo` and includes a syntactically valid bearer JWT payload.
- Workaround: keep the BFF strictly behind the authenticated gateway and configure `WORKSPACE_ADMIN_GROUP_EMAIL` so `frontend/api/src/config.ts` does not allow all users when no admin group is configured.

**Backend service auth can fail open when `IS_GCP` is absent:**
- Symptoms: `backend/pipeline/common/auth.py` returns a local-dev identity whenever `backend/pipeline/common/env.py` reports non-GCP, and `is_gcp_env()` only checks `IS_GCP == "true"`.
- Files: `backend/pipeline/common/auth.py`, `backend/pipeline/common/env.py`, `backend/services/feeds/main.py`, `backend/services/audio_segments/main.py`, `backend/services/rules/main.py`
- Trigger: a deployed service path runs without `IS_GCP=true` in the environment.
- Workaround: enforce `IS_GCP=true` in deployment configuration and keep Cloud Run/IAM ingress restrictions in place until `backend/pipeline/common/env.py` can fail closed for deployed runtimes.

**Docs endpoint selects the first API Gateway config:**
- Symptoms: `frontend/api/src/docs/docsController.ts` contains a TODO noting that it assumes a single API config and selects the first/latest config returned by the gateway API.
- Files: `frontend/api/src/docs/docsController.ts`
- Trigger: multiple API Gateway APIs or configs are present in the project/region.
- Workaround: keep only the intended API config visible to this endpoint, or verify returned docs manually before relying on `frontend/api/src/docs/docsController.ts`.

**Feed UI fetches every feed page for normal views:**
- Symptoms: `frontend/transcription-ui/src/service/listFeeds.ts` provides `listFeedsPage(...)` but `listFeeds(...)` loops through all pages, and feed configuration/search views call `listFeeds(...)` for main data plus all-feed tag discovery.
- Files: `frontend/transcription-ui/src/service/listFeeds.ts`, `frontend/transcription-ui/src/components/feeds/FeedConfigurationView.tsx`, `frontend/transcription-ui/src/components/feeds/FeedSearchView.tsx`, `frontend/transcription-ui/src/components/feeds/FeedConfigurationTable.tsx`, `frontend/transcription-ui/src/components/feeds/FeedTable.tsx`
- Trigger: a workspace with many feeds opens feed configuration or search views; the search view also refetches periodically.
- Workaround: reduce feed counts or filters operationally; the durable fix is server-backed pagination/infinite queries in `frontend/transcription-ui/src/service/listFeeds.ts` and the feed table components.

**Duplicate Fire Notifications filenames are dropped by timestamp map behavior:**
- Symptoms: `backend/pipeline/ingestion/collectors/fire_notifications/client.py` stores seen files by timestamp and keeps the first file when more than one filename parses to the same timestamp.
- Files: `backend/pipeline/ingestion/collectors/fire_notifications/client.py`
- Trigger: Fire Notifications exposes more than one audio file for the same parsed timestamp.
- Workaround: investigate feed-specific duplicate filename patterns and disable or isolate affected feeds until duplicate-aware collection is implemented in `backend/pipeline/ingestion/collectors/fire_notifications/client.py`.

## Security Considerations

**BFF JWT verification is not cryptographic in fallback mode:**
- Risk: `frontend/api/src/authentication.ts` decodes bearer JWTs without verifying signature, issuer, audience, or expiry when gateway identity headers are absent.
- Files: `frontend/api/src/authentication.ts`, `frontend/api/src/config.ts`, `frontend/api/src/index.ts`
- Current mitigation: API Gateway identity headers are preferred, and admin checks use `frontend/api/src/config.ts` when `WORKSPACE_ADMIN_GROUP_EMAIL` is configured.
- Recommendations: reject raw bearer fallback outside local development, or verify Google ID tokens with explicit issuer and audience in `frontend/api/src/authentication.ts`; make missing `WORKSPACE_ADMIN_GROUP_EMAIL` fail closed in production.

**Backend OIDC verification depends on an environment flag:**
- Risk: `backend/pipeline/common/auth.py` bypasses OIDC verification outside `IS_GCP=true`, and the Google token verification call does not pass an explicit audience.
- Files: `backend/pipeline/common/auth.py`, `backend/pipeline/common/env.py`, `backend/services/feeds/main.py`, `backend/services/audio_segments/main.py`, `backend/services/rules/main.py`
- Current mitigation: service apps register `verify_oidc_token` as a FastAPI dependency, and comments expect Cloud Run/GFE to provide primary protection.
- Recommendations: infer deployed runtime from Cloud Run metadata or fail closed by default, require an explicit service audience in `backend/pipeline/common/auth.py`, and add deployment tests that reject missing `IS_GCP` for service containers.

**Trusted actor headers rely on private ingress:**
- Risk: `backend/services/feeds/main.py` trusts `X-WD-Actor-Id` after format validation because the frontend BFF derives it in `frontend/api/src/feeds/actorHeaders.ts`.
- Files: `backend/services/feeds/main.py`, `frontend/api/src/feeds/actorHeaders.ts`
- Current mitigation: `backend/services/feeds/main.py` documents that admin mutation routes must stay private to the BFF service account and public ingress must strip `X-WD-Actor-Id`.
- Recommendations: enforce the private-ingress contract in Terraform/IAM, strip actor headers at public edges, and add an integration test that direct public calls cannot spoof `X-WD-Actor-Id`.

**Audit JSON snapshots are allowlisted but still carry operational metadata:**
- Risk: `backend/pipeline/storage/feed_audit_sql.py` stores configured feed state fields in `feed_audit_events.before_values` and `feed_audit_events.after_values`.
- Files: `backend/pipeline/storage/feed_audit_sql.py`, `terraform/modules/alloydb/sql/ingestion/029_feed_audit_events.sql`
- Current mitigation: `backend/pipeline/storage/feed_audit_sql.py` uses `AUDITED_FEED_STATE_FIELDS` rather than serializing arbitrary request bodies, and the database constrains actor id shape and JSON object shape.
- Recommendations: keep new audit fields behind an explicit allowlist review in `backend/pipeline/storage/feed_audit_sql.py`, and do not add credentials, access tokens, or raw external request payloads to audit snapshots.

## Performance Bottlenecks

**Feed tag and name filters lack matching search indexes:**
- Problem: `backend/pipeline/storage/feed_queries.py` filters with `fp.tags @> $jsonb` and `f.name ILIKE '%' || $name || '%'`, while current feed indexes focus on claim/retry/active paths.
- Files: `backend/pipeline/storage/feed_queries.py`, `terraform/modules/alloydb/sql/ingestion/018_feeds_hot_indexes.sql`, `terraform/modules/alloydb/sql/ingestion/021_feed_properties_tags.sql`
- Cause: tag containment and substring name search require GIN/trigram-style indexes or alternate search design to avoid scanning as feed count grows.
- Improvement path: add a GIN index for `feed_properties.tags`, add a trigram or normalized search index for `feeds.name`, and capture `EXPLAIN` coverage in `integration_tests/storage/test_feed_store_integration.py`.

**Recovery acquisition has a documented sort limit:**
- Problem: `backend/pipeline/storage/feed_queries.py` documents that active-abandoned recovery uses `idx_feeds_active` plus filtering and sort, and becomes expensive if pg_cron pauses or failure volume spikes.
- Files: `backend/pipeline/storage/feed_queries.py`, `terraform/modules/alloydb/sql/ci/hot_protection_check.sql`
- Cause: the active-abandoned branch does not have a dedicated `(retry_after, id)` recovery index covering active/failing rows.
- Improvement path: add the documented `idx_feeds_recovery` migration when production P99 exceeds the stated threshold, and update the HOT protection allowlist in `terraform/modules/alloydb/sql/ci/hot_protection_check.sql`.

**Audio segment alert filtering computes annotations per row:**
- Problem: `backend/pipeline/storage/audio_segment_queries.py` uses lateral aggregation over annotations to derive alert state, and alert filtering happens against the computed value.
- Files: `backend/pipeline/storage/audio_segment_queries.py`, `backend/pipeline/storage/audio_segment_store.py`, `terraform/modules/alloydb/sql/ingestion/022_audio_segments_annotations.sql`, `frontend/transcription-ui/src/hooks/useAudioSegments.ts`
- Cause: alert status is not denormalized onto `audio_segments`, and the annotation indexes support JSON/data access rather than direct alert pagination.
- Improvement path: add a denormalized or indexed alert projection for audio segments, keep annotation truth in `backend/pipeline/storage/audio_segment_queries.py`, and add query-plan tests for alert-only pagination.

**Feed UI polling multiplies backend list costs:**
- Problem: `frontend/transcription-ui/src/components/feeds/FeedSearchView.tsx` polls feed data while `frontend/transcription-ui/src/service/listFeeds.ts` can request every page for each query.
- Files: `frontend/transcription-ui/src/components/feeds/FeedSearchView.tsx`, `frontend/transcription-ui/src/service/listFeeds.ts`, `frontend/transcription-ui/src/components/feeds/FeedTable.tsx`
- Cause: client views perform all-page fetches and client-side filter/sort instead of using server pagination and incremental rendering.
- Improvement path: replace `listFeeds(...)` view usage with `listFeedsPage(...)` plus infinite query/page controls, and keep sorting/filtering parameters server-side in `backend/services/feeds/main.py` and `frontend/api/src/feeds/feedsController.ts`.

**Audit event history has no retention or partitioning policy:**
- Problem: `terraform/modules/alloydb/sql/ingestion/029_feed_audit_events.sql` creates append-only `feed_audit_events` indexes but no retention, archive, or partitioning strategy.
- Files: `terraform/modules/alloydb/sql/ingestion/029_feed_audit_events.sql`, `backend/pipeline/storage/feed_audit_sql.py`, `backend/pipeline/storage/feed_queries.py`
- Cause: every meaningful feed mutation can add durable JSON snapshots, and delete semantics intentionally keep audit rows after feed row deletion.
- Improvement path: define retention/export requirements in `CONTEXT.md`, add partitioning or archival once write volume requires it, and keep read queries bounded by feed/time indexes.

## Fragile Areas

**Collector runtime carries many concurrency invariants:**
- Files: `backend/pipeline/ingestion/collector_runtime.py`, `backend/pipeline/ingestion/tests/test_collector_runtime.py`
- Why fragile: `backend/pipeline/ingestion/collector_runtime.py` orchestrates feed leases, heartbeat/data thread pools, queue backpressure, release tracking, interruptible sleeps, and a target of many feeds per worker in one large module.
- Safe modification: keep `_releasing_feeds` updates before awaits on release paths, preserve separate heartbeat/data executor responsibilities, and cover behavior with `backend/pipeline/ingestion/tests/test_collector_runtime.py` before changing scheduler or shutdown logic.
- Test coverage: strong focused tests exist in `backend/pipeline/ingestion/tests/test_collector_runtime.py`, but broad local execution is resource-sensitive under the repo instructions in `AGENTS.md`.

**Stateful segmentation depends on timer and lease behavior:**
- Files: `backend/pipeline/segmentation/transforms/stateful.py`, `backend/pipeline/segmentation/state/stitcher_state.py`, `backend/pipeline/segmentation/tests/`
- Why fragile: `backend/pipeline/segmentation/transforms/stateful.py` documents Dataflow Windmill lease limits, self-chaining timers, chunk caps per bundle, state clearing, and trace-context handling as load-bearing behavior.
- Safe modification: keep timer self-chaining and max-chunks logic intact, preserve idle-state clearing, and adjust `backend/pipeline/segmentation/state/stitcher_state.py` with tests that include late chunks, out-of-order chunks, gaps, and flush boundaries.
- Test coverage: segmentation tests exist under `backend/pipeline/segmentation/tests/`, but changes that affect timers or state should include integration-style coverage for state/timer transitions.

**Feed store SQL is dynamic and audit-coupled:**
- Files: `backend/pipeline/storage/feed_queries.py`, `backend/pipeline/storage/feed_store.py`, `backend/pipeline/storage/feed_audit_sql.py`, `backend/pipeline/storage/tests/test_feed_query_contracts.py`, `integration_tests/storage/test_feed_store_integration.py`
- Why fragile: feed status mutations, claim paths, recovery paths, and audit writes are composed with raw SQL strings and CTEs across multiple helpers.
- Safe modification: update SQL fragments through `backend/pipeline/storage/feed_audit_sql.py` helpers when audit behavior changes, run contract tests in `backend/pipeline/storage/tests/test_feed_query_contracts.py`, and add integration coverage in `integration_tests/storage/test_feed_store_integration.py` for state transitions.
- Test coverage: storage tests are extensive, but query-plan and high-volume filter behavior require explicit integration/performance checks.

**Proto schemas carry rolling-upgrade compatibility fields:**
- Files: `protos/streaming_state.proto`, `protos/normalized_audio.proto`, `backend/pipeline/schema_types/`
- Why fragile: `protos/streaming_state.proto` contains deprecated fields kept for backward compatibility, and `protos/normalized_audio.proto` still carries fields planned for removal after database segment IDs fully cover lookups.
- Safe modification: reserve removed field numbers/names, regenerate outputs through `.mise.toml` tasks, and keep rolling-upgrade compatibility in `backend/pipeline/schema_types/` consumers until all live workers use the new schema.
- Test coverage: schema validation exists through project tooling, but removal of deprecated fields needs an explicit migration plan and compatibility tests for active Dataflow/worker state.

**Docs generation depends on cloud API shape and config ordering:**
- Files: `frontend/api/src/docs/docsController.ts`
- Why fragile: docs retrieval relies on API Gateway config listing behavior and local OpenAPI rewriting in one controller.
- Safe modification: add an explicit API/config selector to `frontend/api/src/docs/docsController.ts` before multiple configs are present, and keep tests focused on exact config selection and URL rewrite behavior.
- Test coverage: controller-level tests should include multiple config responses.

## Scaling Limits

**Collector VM feed capacity is explicitly bounded:**
- Current capacity: `backend/pipeline/ingestion/collector_runtime.py` documents a target scale of roughly 250 feeds per instance.
- Limit: heartbeat, queue, executor, and feed-lease work can saturate per worker before source-specific collectors or Pub/Sub publishing saturate.
- Scaling path: add worker instances and source-type caps through `backend/pipeline/ingestion/source_runtime_specs.py`, then measure heartbeat latency and release latency in `backend/pipeline/ingestion/collector_runtime.py`.

**Recovery sweep assumes small abandoned/failing volumes:**
- Current capacity: `backend/pipeline/storage/feed_queries.py` comments describe structurally small recovery volumes drained by pg_cron.
- Limit: paused pg_cron or a failure storm pushes recovery acquisition into expensive sort behavior.
- Scaling path: add the documented recovery index in `backend/pipeline/storage/feed_queries.py`, update `terraform/modules/alloydb/sql/ci/hot_protection_check.sql`, and monitor recovery acquisition P99.

**Frontend feed tables assume moderate feed counts:**
- Current capacity: `frontend/transcription-ui/src/service/listFeeds.ts` requests pages of 100 until completion.
- Limit: thousands of feeds create many API requests, repeated all-feed tag discovery queries, and client-side sorting/filtering costs in `frontend/transcription-ui/src/components/feeds/FeedTable.tsx`.
- Scaling path: adopt server pagination/infinite query in `frontend/transcription-ui/src/service/listFeeds.ts`, add a tag metadata endpoint, and virtualize large feed tables in `frontend/transcription-ui/src/components/feeds/FeedTable.tsx`.

**Audio segment polling has a hard page cap:**
- Current capacity: `frontend/transcription-ui/src/hooks/useAudioSegments.ts` caps polling iterations while fetching newer segment pages.
- Limit: high-volume feeds can exceed the polling cap and leave the UI behind until a full refresh or broader query catches up.
- Scaling path: add a streaming or cursor-based incremental segment API in `backend/services/audio_segments/main.py` and reduce client polling loops in `frontend/transcription-ui/src/hooks/useAudioSegments.ts`.

**Audit table growth is unbounded by schema:**
- Current capacity: `terraform/modules/alloydb/sql/ingestion/029_feed_audit_events.sql` adds indexes on feed/time, time, and actor.
- Limit: high mutation volume grows JSONB audit storage and indexes indefinitely.
- Scaling path: add retention/export/partition policy for `feed_audit_events` in `terraform/modules/alloydb/sql/ingestion/029_feed_audit_events.sql` once operational retention requirements are defined in `CONTEXT.md`.

## Dependencies at Risk

**Runtime security depends on deployment environment variables:**
- Risk: `backend/pipeline/common/env.py` treats `IS_GCP` as the runtime security switch, and `backend/pipeline/common/actor_identity.py` relies on `FEED_AUDIT_ACTOR_ID` for precise service actor attribution.
- Impact: missing `IS_GCP` can bypass backend auth, and missing `FEED_AUDIT_ACTOR_ID` makes audit actor identity unresolved.
- Migration plan: make deployed-runtime detection fail closed in `backend/pipeline/common/env.py`, validate required deployment env vars in Terraform/Cloud Run config, and alert from `backend/pipeline/common/actor_identity.py` unresolved actor logs.

**Google API Gateway docs integration depends on config ordering:**
- Risk: `frontend/api/src/docs/docsController.ts` assumes the relevant API Gateway config is first/latest.
- Impact: docs can be generated from the wrong API config when several configs exist.
- Migration plan: configure an explicit API/config identifier and query that target in `frontend/api/src/docs/docsController.ts`.

**Model scoring dependencies are pinned and excluded from main lint scope:**
- Risk: `model/pyproject.toml` pins research/scoring dependencies, and `pyproject.toml` excludes `model/colabs/**/*.py` and notebooks from the main Ruff rule set.
- Impact: model evaluation behavior and notebook helper code can drift separately from backend style and dependency update practices.
- Migration plan: keep model dependency updates isolated to `model/pyproject.toml`, run the notebook formatting/linting flow described in `ASR_CONTRIBUTING.md`, and add explicit regression data when scoring dependencies change.

**Proto generation depends on local tooling alignment:**
- Risk: `.mise.toml` owns protobuf generation tasks, while `.gitignore` ignores many generated outputs.
- Impact: contributors can run code against locally generated artifacts that are not part of the committed source state.
- Migration plan: document `protos/` as the only edited schema source, keep generated outputs reproducible from `.mise.toml`, and validate schema regeneration in CI rather than relying on local ignored files.

## Missing Critical Features

**Feed audit events are durable but not operator-visible through a read API:**
- Problem: audit storage exists in `terraform/modules/alloydb/sql/ingestion/029_feed_audit_events.sql` and write helpers exist in `backend/pipeline/storage/feed_audit_sql.py`, but feed service/proxy routes expose feed CRUD without an audit timeline endpoint.
- Blocks: operators cannot inspect feed mutation history from the existing API/UI even though `feed_audit_events` rows are written.
- Files: `terraform/modules/alloydb/sql/ingestion/029_feed_audit_events.sql`, `backend/pipeline/storage/feed_audit_sql.py`, `backend/services/feeds/main.py`, `frontend/api/src/feeds/feedsController.ts`, `CONTEXT.md`

**Feed audit notification delivery is best-effort/follow-up:**
- Problem: `CONTEXT.md` frames feed audit notifications and broader ops lifecycle delivery as follow-up rather than the system of record.
- Blocks: downstream systems cannot rely on notification delivery for complete feed audit history.
- Files: `CONTEXT.md`, `backend/pipeline/storage/feed_audit_sql.py`, `terraform/modules/alloydb/sql/ingestion/029_feed_audit_events.sql`

**Feed tag vocabulary is discovered through all-feed scans:**
- Problem: feed UI views derive tag filters by requesting all feeds through `frontend/transcription-ui/src/service/listFeeds.ts` instead of using a dedicated tag metadata endpoint.
- Blocks: scalable feed filtering and tag discovery for large workspaces.
- Files: `frontend/transcription-ui/src/service/listFeeds.ts`, `frontend/transcription-ui/src/components/feeds/FeedConfigurationView.tsx`, `frontend/transcription-ui/src/components/feeds/FeedSearchView.tsx`, `backend/services/feeds/main.py`

**Transcription prompt context is not wired:**
- Problem: `backend/pipeline/transcription/transcribers/gemini.py` and `backend/pipeline/transcription/transcribers/prompts.py` contain TODOs for dynamic/contextual prompts.
- Blocks: ASR quality improvements that depend on feed metadata, location, previous segment context, or source-specific terminology.
- Files: `backend/pipeline/transcription/transcribers/gemini.py`, `backend/pipeline/transcription/transcribers/prompts.py`

## Test Coverage Gaps

**Auth tests need forged-token rejection coverage:**
- What's not tested: the expected production behavior that raw or unsigned bearer JWT payloads are rejected when gateway identity headers are absent.
- Files: `frontend/api/src/authentication.ts`, `frontend/api/src/authentication.test.ts`
- Risk: the current `jose.decodeJwt` fallback can remain in place without a failing regression test.
- Priority: High

**Backend auth needs deployed-runtime fail-closed coverage:**
- What's not tested: service APIs rejecting requests when runtime config resembles deployment but `IS_GCP` is missing or false.
- Files: `backend/pipeline/common/auth.py`, `backend/pipeline/common/env.py`, `backend/services/feeds/main.py`, `backend/services/audio_segments/main.py`, `backend/services/rules/main.py`
- Risk: Cloud Run or service deployment misconfiguration can silently bypass OIDC verification.
- Priority: High

**Feed filter query plans need regression coverage:**
- What's not tested: high-volume query plans for tag containment, substring name search, count queries, and recovery acquisition.
- Files: `backend/pipeline/storage/feed_queries.py`, `backend/pipeline/storage/tests/test_feed_query_contracts.py`, `integration_tests/storage/test_feed_store_integration.py`, `terraform/modules/alloydb/sql/ingestion/021_feed_properties_tags.sql`
- Risk: feed-management latency can degrade without unit tests failing.
- Priority: Medium

**Audit read/delivery coverage is absent because the feature is absent:**
- What's not tested: feed audit timeline API behavior, pagination, authorization, and notification delivery semantics.
- Files: `backend/services/feeds/main.py`, `frontend/api/src/feeds/feedsController.ts`, `backend/pipeline/storage/feed_audit_sql.py`, `terraform/modules/alloydb/sql/ingestion/029_feed_audit_events.sql`
- Risk: future audit UI/API work can accidentally read incomplete histories, expose snapshots incorrectly, or omit authorization checks.
- Priority: High

**Evaluation processor needs explicit failure-semantics tests:**
- What's not tested: the intended ack/retry/dead-letter behavior for parse errors, empty payloads, evaluator failures, annotation write failures, and Pub/Sub publish failures.
- Files: `backend/pipeline/evaluation/processor.py`, `backend/pipeline/evaluation/tests/test_processor.py`
- Risk: small handler changes can change message acknowledgement semantics without detection.
- Priority: Medium

**Fire Notifications collector needs duplicate/timezone cases:**
- What's not tested: duplicate parsed filenames and non-UTC channel timestamp behavior.
- Files: `backend/pipeline/ingestion/collectors/fire_notifications/client.py`
- Risk: source-specific audio can be skipped or bookmarked incorrectly.
- Priority: Medium

**Local verification has host-stability constraints:**
- What's not tested: full repository test/build behavior during this mapping pass.
- Files: `AGENTS.md`, `.agents/instructions.md`, `pyproject.toml`, `.mise.toml`
- Risk: broad `pytest` execution uses project-level parallelism and can consume substantial host resources; docs-only changes should use static validation unless a focused test is required.
- Priority: Low

---

*Concerns audit: 2026-06-26*
