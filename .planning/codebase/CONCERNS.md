# Codebase Concerns

**Analysis Date:** 2026-05-24

## Tech Debt

**API gateway authentication is trust-by-comment:**
- Issue: `frontend/api/src/authentication.ts` calls `jwt.decode(...)` and casts the result to `GoogleUser`; it does not verify signature, issuer, audience, expiry, or `email_verified`.
- Files: `frontend/api/src/authentication.ts`, `frontend/api/src/auth/authController.ts`, `frontend/api/src/index.ts`
- Impact: Any route protected only by TSOA `@Security('google_id_token')` accepts any syntactically valid bearer JWT if API Gateway verification is bypassed, misconfigured, or exercised locally.
- Fix approach: Replace decode-only auth with `google-auth-library` token verification or an explicit API Gateway assertion contract. Validate `aud`, `iss`, `exp`, and `email_verified`, then add focused tests in `frontend/api/src/authentication.test.ts`.

**GCP environment flag controls service auth behavior:**
- Issue: `backend/pipeline/common/auth.py` returns a local-dev identity whenever `backend/pipeline/common/env.py` sees `IS_GCP != "true"`.
- Files: `backend/pipeline/common/auth.py`, `backend/pipeline/common/env.py`, `backend/services/feeds/main.py`, `backend/services/rules/main.py`, `backend/services/transcripts/main.py`
- Impact: A missing or misspelled deployment env var disables auth for FastAPI service APIs. The same verifier also omits an expected audience argument for `verify_oauth2_token(...)`.
- Fix approach: Make auth bypass explicit through a development-only env var such as `ALLOW_INSECURE_LOCAL_AUTH=true`, fail closed by default, and pass the expected Cloud Run audience to token verification.

**Source type definitions are duplicated across stacks:**
- Issue: Source type values live in Python enums, SQL seeds, ingestion caps, collector registry, frontend TypeScript types, API URL mapping, and normalization routing.
- Files: `backend/pipeline/storage/feed_store.py`, `backend/pipeline/ingestion/settings.py`, `backend/pipeline/ingestion/router.py`, `backend/pipeline/ingestion/main.py`, `frontend/common/src/types/feeds.ts`, `frontend/api/src/feeds/feedsController.ts`, `terraform/modules/alloydb/sql/ingestion/006_seed_source_types.sql`
- Impact: Adding a feed source can leave workers unable to claim feeds, UI unable to represent them, or URLs missing. `backend/pipeline/ingestion/main.py` guards only the collector/cap subset.
- Fix approach: Keep the startup invariant, and add a generated or shared manifest for source slugs consumed by Python, SQL seed generation, and TypeScript.

**Lint policy suppresses important safety and complexity signals:**
- Issue: `pyproject.toml` ignores broad categories including complexity (`C901`), TODOs (`FIX002`), catch-all exceptions (`BLE001`), unsafe pickle (`S301`), subprocess safety (`S603`/`S607`), hardcoded password warnings (`S106`), and request timeout warnings in tests.
- Files: `pyproject.toml`, `backend/pipeline/normalization/transforms/stateful.py`, `backend/pipeline/ingestion/normalizer_runtime.py`, `backend/pipeline/normalization/audio/audio_processor.py`
- Impact: Real issues blend into allowed style debt, especially in high-risk modules that run subprocesses, parse untrusted messages, and maintain long-lived worker state.
- Fix approach: Move ignores to narrow per-file exceptions with comments, and re-enable high-value rules for production code paths first.

**Evaluation processor has explicit unhandled failure TODOs:**
- Issue: `backend/pipeline/evaluation/processor.py` marks parse, evaluation, transcript write, and publish failure handling as TODOs, and publishes with `future.result()` without a timeout.
- Files: `backend/pipeline/evaluation/processor.py`, `backend/pipeline/evaluation/tests/test_processor.py`
- Impact: Partial processing can leave transcripts stored without alert publication, or flagged messages can block the function while waiting on Pub/Sub.
- Fix approach: Add explicit retry/dead-letter behavior for each stage and use bounded publish waits with clear error classification.

## Known Bugs

**Rule groups are accepted but never evaluated:**
- Symptoms: `RULE_GROUP` is part of the public model and API conversion path, but `BaseTextEvaluator._evaluate_rule(...)` returns `False` for group conditions.
- Files: `backend/pipeline/common/rules/models.py`, `frontend/common/src/types/rules.ts`, `frontend/api/src/rules/rulesController.ts`, `backend/pipeline/evaluation/rules_evaluation/evaluator.py`
- Trigger: Create an active group rule whose child rule matches a transcript.
- Workaround: Use flattened keyword or regex rules instead of `RULE_GROUP`.

**Normalization can emit placeholder GCS URIs:**
- Symptoms: When `canonical_audio_bucket` is unset and there is no single contributing URI fallback, `NormalizeAudioFn` emits `gs://test-bucket/placeholder.flac` and `gs://test-bucket/placeholder.m4a`.
- Files: `backend/pipeline/normalization/transforms/stateful.py`, `backend/pipeline/normalization/orchestration.py`, `backend/pipeline/transcription/processor.py`
- Trigger: Normalize a multi-chunk flush request without `canonical_audio_bucket`.
- Workaround: Ensure `canonical_audio_bucket` is always configured for deployed normalization jobs.

**Logout cookie clearing does not mirror cookie attributes:**
- Symptoms: `setRefreshTokenCookie(...)` sets `secure` and `sameSite`, but `logout(...)` calls `clearCookie('refresh_token')` without matching options.
- Files: `frontend/api/src/auth/authController.ts`, `frontend/api/src/auth/authController.test.ts`
- Trigger: Production cross-site cookie with `sameSite: 'none'` and `secure: true`.
- Workaround: Browser/session expiry eventually removes the cookie; manual cookie clearing may be needed when logout does not clear it.

**Docs endpoint assumes the first API Gateway API is the intended API:**
- Symptoms: `DocsController.getOpenApiSpec()` lists all Gateway APIs and uses `apis[0]`, then picks the latest config for that API.
- Files: `frontend/api/src/docs/docsController.ts`, `frontend/api/README.md`
- Trigger: A project contains more than one API Gateway API.
- Workaround: Keep only one API Gateway API in the project or verify the served docs manually.

**Transcript list limit is unbounded:**
- Symptoms: FastAPI accepts any `limit: int = 100`, TSOA only marks the query as integer, and storage sends `limit + 1` to SQL.
- Files: `backend/services/transcripts/main.py`, `backend/pipeline/storage/transcript_store.py`, `frontend/api/src/transcripts/transcriptsController.ts`
- Trigger: Request a very large or negative `limit`.
- Workaround: Callers use the default UI page size.

## Security Considerations

**Authentication lacks defense in depth:**
- Risk: Gateway-facing Node auth decodes tokens without verification, FastAPI service auth fails open outside `IS_GCP=true`, and service token verification does not pass an expected audience.
- Files: `frontend/api/src/authentication.ts`, `backend/pipeline/common/auth.py`, `backend/pipeline/common/env.py`
- Current mitigation: API Gateway and Cloud Run IAM are expected to enforce authentication before application code runs.
- Recommendations: Verify tokens in application code, fail closed on missing deployment auth config, and add auth tests that reject forged tokens and wrong audiences.

**Shared OAuth2Client mutates credentials across requests:**
- Risk: `frontend/api/src/auth/authController.ts` defines a module-level `OAuth2Client` and calls `client.setCredentials({ refresh_token })` during session refresh.
- Files: `frontend/api/src/auth/authController.ts`, `frontend/api/src/auth/authController.test.ts`
- Current mitigation: Requests are short-lived and tests use one request at a time.
- Recommendations: Instantiate an OAuth client per refresh request or use a non-mutating token refresh flow; add a concurrency test for two simultaneous refreshes.

**Sensitive payloads and upstream data are logged too broadly:**
- Risk: Notification sends log the full alert payload, including transcript text and audio URIs, and API proxy errors log raw upstream response data.
- Files: `backend/pipeline/notification/request_handler.py`, `backend/pipeline/notification/send_notification.py`, `frontend/api/src/utils.ts`
- Current mitigation: API keys are sent in headers and are not directly logged by the notification handler.
- Recommendations: Log only stable IDs, status codes, and redacted error summaries. Treat transcript text, signed URLs, and upstream response bodies as sensitive.

**Basic auth and source URLs can leak through process lists or logs:**
- Risk: Icecast credentials are encoded into an ffmpeg `-headers` argument, and collectors log full download/source URLs on failures.
- Files: `backend/pipeline/ingestion/collectors/icecast/icecast_collector.py`, `backend/pipeline/ingestion/collectors/bcfy_calls/bcfy_calls_collector.py`, `backend/pipeline/ingestion/collectors/openmhz/collector.py`, `backend/pipeline/ingestion/collectors/fire_notifications/collector.py`
- Current mitigation: Secrets are read from environment or Secret Manager rather than source files.
- Recommendations: Avoid full URL logging, redact query strings, and isolate ffmpeg processes so process args are not observable outside the container boundary.

**Environment files are present in the repo tree:**
- Risk: Secret-like configuration files exist and must not be read into committed documentation or logs.
- Files: `local_dev/LOCAL.env`, `frontend/api/.env.example`, `frontend/transcription-ui/.env.example`
- Current mitigation: Example files are separated from runtime secrets; contents are not included in this audit.
- Recommendations: Keep real secrets out of these files, add secret scanning in CI, and prefer generated local env files ignored by git.

**Authorization is coarse-grained after login:**
- Risk: Authenticated API users can call feed reset/deactivate and rule create/update/delete endpoints; no role or ownership checks appear in controllers.
- Files: `frontend/api/src/feeds/feedsController.ts`, `frontend/api/src/rules/rulesController.ts`, `backend/services/feeds/main.py`, `backend/services/rules/main.py`
- Current mitigation: Access is limited to authenticated users at the gateway/service layer.
- Recommendations: Add role-based authorization for operational actions and enforce owner/admin checks in the backend services.

## Performance Bottlenecks

**Feeds and rules list endpoints are unpaginated:**
- Problem: Feed listing returns all feeds, and rules listing returns all rules when no IDs are supplied.
- Files: `backend/services/feeds/main.py`, `backend/pipeline/storage/feed_store.py`, `backend/pipeline/storage/feed_queries.py`, `frontend/api/src/feeds/feedsController.ts`, `backend/pipeline/storage/rules_queries.py`
- Cause: `LIST_FEEDS_SQL` and `LIST_RULES_SQL` have no `LIMIT`, cursor, or search filters.
- Improvement path: Add keyset pagination and server-side filtering; keep frontend lists virtualized but do not rely on UI virtualization as the only scale control.

**Rule evaluation fetches and scans the full rule set:**
- Problem: `RemoteTextEvaluator` caches one full rule list for 60 seconds and evaluates applicable rules in process for every transcript.
- Files: `backend/pipeline/evaluation/rules_evaluation/evaluator.py`, `backend/pipeline/storage/rules_queries.py`
- Cause: Rules API has no active/global/feed-specific filtering endpoint for evaluation.
- Improvement path: Add a backend query for active rules by feed scope, compile/cache regexes safely, and invalidate cache on rule changes.

**User-controlled regex can block evaluation workers:**
- Problem: Regex rules are executed with Python `re.search(...)` against transcript text without compilation limits, timeout, or pattern validation.
- Files: `backend/pipeline/evaluation/rules_evaluation/evaluator.py`, `backend/pipeline/common/rules/models.py`, `backend/services/rules/main.py`
- Cause: The rules model accepts arbitrary regex strings and flags.
- Improvement path: Validate regex patterns at rule creation, use a timeout-capable regex engine or run regex evaluation under a strict deadline, and cap pattern/text sizes.

**Audio subprocesses have no execution timeout:**
- Problem: ffmpeg and ffprobe calls in normalization helpers run without a timeout.
- Files: `backend/pipeline/normalization/audio/audio_processor.py`, `backend/pipeline/common/audio.py`
- Cause: `subprocess.run(...)` calls capture output but do not set `timeout=...`.
- Improvement path: Add bounded subprocess timeouts, classify timeout errors for DLQ/quarantine, and cover corrupt or adversarial audio inputs in unit tests.

**Pub/Sub publishing can wait indefinitely:**
- Problem: Several publish paths wait on Pub/Sub futures without an explicit timeout.
- Files: `backend/pipeline/common/gcp_helper.py`, `backend/pipeline/evaluation/processor.py`, `backend/pipeline/transcription/processor.py`
- Cause: `future.result()` and `asyncio.wrap_future(...)` are used without a deadline.
- Improvement path: Use bounded waits and retry policies at call sites, then emit DLQ/quarantine records rather than holding worker capacity indefinitely.

**Recovery lease query has a documented index threshold:**
- Problem: The recovery path sorts active-abandoned rows without the optional recovery index when pg_cron stalls or failure volume spikes.
- Files: `backend/pipeline/storage/feed_queries.py`, `terraform/modules/alloydb/sql/ingestion/019_feeds_pg_cron_jobs.sql`, `terraform/modules/alloydb/sql/ci/hot_protection_check.sql`
- Cause: The code intentionally defers `idx_feeds_recovery` until recovery-path P99 warrants it.
- Improvement path: Monitor recovery-path P99 and add the documented partial index plus HOT-protection allow-list entry when the threshold is crossed.

## Fragile Areas

**NormalizerRuntime is a load-bearing state machine:**
- Files: `backend/pipeline/ingestion/normalizer_runtime.py`, `backend/pipeline/ingestion/tests/test_runtime.py`, `backend/pipeline/storage/feed_store.py`
- Why fragile: Heartbeats, lease fencing, RSS watchdog shutdown, feed task cancellation, and batch lease release are tightly coupled; the file is about 1.5k lines and uses `os._exit(1)` for fence violations.
- Safe modification: Preserve the shutdown ordering documented in `backend/pipeline/ingestion/normalizer_runtime.py`; add unit tests for any change to heartbeat, release, cancellation, or `_releasing_feeds`.
- Test coverage: Strong unit coverage exists in `backend/pipeline/ingestion/tests/test_runtime.py`; production-like integration behavior still depends on AlloyDB, Pub/Sub, GCS, and MIG semantics.

**Beam normalization state is complex and serialization-sensitive:**
- Files: `backend/pipeline/normalization/transforms/stateful.py`, `backend/pipeline/normalization/transforms/stitcher_engine.py`, `backend/pipeline/normalization/state/stitcher_state.py`, `backend/pipeline/normalization/common/coders.py`
- Why fragile: Continuous and segmented state machines share timers, Beam state, custom coders, `PickleCoder`, VAD, GCS, and subprocess audio processing.
- Safe modification: Change one state transition at a time and add focused tests under `backend/pipeline/normalization/tests/`; avoid changing state schema without coder migration tests.
- Test coverage: Broad unit coverage exists, but `backend/pipeline/normalization/tests/test_output.log` is a committed failure/debug artifact and should not be treated as a test oracle.

**Source collector routing must stay in sync with storage and deployment:**
- Files: `backend/pipeline/ingestion/router.py`, `backend/pipeline/ingestion/settings.py`, `backend/pipeline/storage/feed_store.py`, `terraform/modules/alloydb/sql/ingestion/006_seed_source_types.sql`, `frontend/common/src/types/feeds.ts`
- Why fragile: New source support spans SQL, Python runtime caps, per-source topics, collectors, frontend type unions, and API URL presentation.
- Safe modification: Update all source registries in one change, run the startup invariant tests, and add a source-specific collector contract test.
- Test coverage: `backend/pipeline/ingestion/main.py` checks collectors versus caps only; frontend and SQL drift need additional checks.

**Auth configuration spans gateway, API, service, and Cloud Run IAM:**
- Files: `frontend/api/src/config.ts`, `frontend/api/tsoa.json`, `frontend/api/openapi.yaml`, `backend/pipeline/common/auth.py`, `terraform/modules/cloud_function/main.tf`
- Why fragile: Missing OAuth config logs instead of failing for some variables, Node auth trusts Gateway verification, and Python auth trusts `IS_GCP`.
- Safe modification: Centralize required auth/deployment variables and fail closed during startup.
- Test coverage: Controller tests cover login/refresh happy paths but not forged JWT rejection, wrong audience rejection, or deployment env drift.

**Notification deduplication depends on warm global Redis state:**
- Files: `backend/pipeline/notification/send_notification.py`, `backend/pipeline/notification/notification_deduplication.py`, `backend/pipeline/common/storage/redis_service.py`
- Why fragile: The function initializes Redis-backed dedupe globally at import time and carries a TODO for local-dev Redis integration.
- Safe modification: Keep dedupe initialization observable and fail mode explicit; avoid adding per-invocation Redis clients without measuring cold-start cost.
- Test coverage: Unit tests cover dedupe behavior, but failure-mode tests for Redis outage versus notification delivery behavior should be expanded.

## Scaling Limits

**Ingestion worker capacity is capped by memory and per-source defaults:**
- Current capacity: `max_feeds_per_worker` defaults to 800, with per-source caps in `backend/pipeline/ingestion/settings.py`.
- Limit: Memory-heavy sources and upload concurrency can push workers into RSS pause or graceful shutdown.
- Scaling path: Tune per-source caps from production RSS metrics and keep GCS/aiohttp connection limits aligned with `max_feeds_per_worker`.

**AlloyDB leasing assumes pg_cron and low recovery backlog:**
- Current capacity: The abandoned-lease sweep reclaims at most 500 rows per 30 seconds in `terraform/modules/alloydb/sql/ingestion/019_feeds_pg_cron_jobs.sql`.
- Limit: Zonal outages, pg_cron failure, or failure storms create recovery backlogs where recovery query sorting becomes expensive.
- Scaling path: Monitor pg_cron job health, recovery P99, and oldest-unclaimed-feed age; add the documented recovery index when needed.

**Rules API and evaluator scale with total rule count:**
- Current capacity: `RemoteTextEvaluator` caches a single complete rule list for 60 seconds.
- Limit: Rule count growth increases rules API payload size and per-transcript CPU.
- Scaling path: Add active-feed-scoped rule queries and push filtering into `backend/pipeline/storage/rules_queries.py`.

**Frontend feed and rule views load whole collections:**
- Current capacity: UI fetches all feeds and all rules for views and transcript context.
- Limit: Large feed/rule counts increase API latency, browser memory, and render cost.
- Scaling path: Add API pagination/search and update `frontend/transcription-ui/src/service/listFeeds.ts` and `frontend/transcription-ui/src/service/listRules.ts` to query incrementally.

**Normalization cost is CPU and subprocess heavy:**
- Current capacity: VAD ONNX sessions are shared per Beam worker, but each normalization still runs DSP, VAD, ffmpeg exports, and GCS uploads.
- Limit: Long or malformed chunks consume worker CPU and may block on subprocesses without timeouts.
- Scaling path: Bound audio duration earlier, add subprocess deadlines, and track Dataflow worker CPU/memory per source type.

## Dependencies at Risk

**Alpha and leading-edge frontend stack:**
- Risk: `frontend/api/package.json` uses `tsoa` `^7.0.0-alpha.0`, TypeScript `^6.0.2`, Express `^5.2.1`, and Functions Framework `^5.0.2`.
- Impact: Generator output, middleware behavior, or type semantics can shift under dependency updates.
- Migration plan: Pin exact versions for gateway generation, review generated `frontend/api/openapi.yaml` diffs, and keep `frontend/api/src/**/*.test.ts` focused on generated route behavior.

**Python 3.13 runtime with native/ML/audio dependencies:**
- Risk: `pyproject.toml` requires Python 3.13 and includes Beam, onnxruntime, pedalboard, soundfile, uvloop, and Google SDK packages.
- Impact: Native wheels and runtime compatibility can block deploys or create subtle performance changes.
- Migration plan: Keep lockfile updates isolated, run unit tests plus a smoke build for Docker images that own native dependencies.

**curl-cffi transport depends on external protocol behavior:**
- Risk: OpenMHZ WebSocket and download paths use `curl_cffi` browser impersonation and custom Engine.IO/Socket.IO frame parsing.
- Impact: Upstream protocol or TLS fingerprint changes can break collection.
- Migration plan: Keep protocol parsing tests in `backend/pipeline/ingestion/collectors/tests/test_openmhz_ws_transport.py` and add captured-frame fixtures for upstream changes.

**ffmpeg and ffprobe are external runtime tools:**
- Risk: Audio processing code assumes ffmpeg/ffprobe availability and behavior outside Python dependency management.
- Impact: Container image drift can break ingestion, duration calculation, normalization export, or tests.
- Migration plan: Pin OS package versions in Dockerfiles and add image-level smoke checks for `ffmpeg` and `ffprobe`.

**pg_cron migration convention is load-bearing:**
- Risk: Migrations requiring pg_cron must include `pg_cron` in the filename so local/test contexts skip them.
- Impact: A wrongly named migration can fail local database setup and integration fixtures.
- Migration plan: Keep the filename guard in CI and add a migration linter for `CREATE EXTENSION pg_cron` usage.

## Missing Critical Features

**Rule group evaluation:**
- Problem: Group rules are part of the API contract but evaluation skips them.
- Blocks: Operators cannot express reusable rule combinations through `RULE_GROUP`.

**Role-based authorization:**
- Problem: Authenticated users are not differentiated for feed operations, rule mutation, docs access, or transcript access.
- Blocks: Least-privilege access for operational versus read-only users.

**Pagination and filtering for feeds/rules:**
- Problem: Feeds and rules APIs expose full-list endpoints only.
- Blocks: Large deployments with thousands of feeds or many rules.

**Per-feed timezone resolution for Fire Notifications:**
- Problem: `_get_channel_timezone(...)` is a stub that always returns UTC.
- Blocks: Correct timestamp localization for channels whose filenames encode local time outside UTC.

**Strict deployment configuration validation for auth and docs:**
- Problem: Some required API config values log errors rather than failing startup.
- Blocks: Fast diagnosis of broken auth/docs deployments.

## Test Coverage Gaps

**API authentication verification:**
- What's not tested: Forged JWT rejection, wrong issuer/audience rejection, unverified email rejection, and missing API Gateway verification.
- Files: `frontend/api/src/authentication.ts`, `frontend/api/src/auth/authController.test.ts`
- Risk: Auth regressions ship because controller tests mock Google token exchange but do not test TSOA auth behavior.
- Priority: High

**OAuth refresh concurrency and logout cookie clearing:**
- What's not tested: Concurrent refreshes against the module-level OAuth client and production cookie clearing with matching `secure`/`sameSite` attributes.
- Files: `frontend/api/src/auth/authController.ts`, `frontend/api/src/auth/authController.test.ts`
- Risk: Users can see intermittent refresh failures or logout cookies that survive.
- Priority: High

**Rule groups and regex safety:**
- What's not tested: `RULE_GROUP` evaluation semantics, cyclic group detection, invalid regex rejection, and catastrophic regex backtracking protection.
- Files: `backend/pipeline/evaluation/rules_evaluation/evaluator.py`, `backend/pipeline/common/rules/models.py`, `backend/pipeline/evaluation/tests/test_evaluator.py`
- Risk: Rules silently fail to trigger or hang evaluation workers.
- Priority: High

**Normalization URI fallback:**
- What's not tested: No-canonical-bucket multi-chunk behavior that emits placeholder `gs://test-bucket/...` URIs.
- Files: `backend/pipeline/normalization/transforms/stateful.py`, `backend/pipeline/normalization/tests/test_transforms.py`
- Risk: Production messages point transcription at non-existent placeholder objects.
- Priority: High

**Pagination and limit validation:**
- What's not tested: Negative or oversized transcript limits, feed list pagination, and rules list pagination.
- Files: `backend/services/transcripts/main.py`, `backend/services/feeds/main.py`, `backend/pipeline/storage/rules_store.py`, `frontend/api/src/transcripts/transcriptsController.test.ts`
- Risk: Large requests degrade database and API performance.
- Priority: Medium

---

*Concerns audit: 2026-05-24*
