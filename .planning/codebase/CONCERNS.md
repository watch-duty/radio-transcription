# Codebase Concerns

**Analysis Date:** 2026-05-27

## Tech Debt

**Broad lint and type-check exemptions:**
- Issue: The root Ruff profile ignores many rules that normally catch debt in production code, including TODO markers, complexity, f-string logging, blind exception handling, unsafe subprocess calls, hardcoded-password checks, and SQL-injection heuristics. Model notebooks and `model/colabs/**/*.py` ignore all Ruff rules, and `model/scripts/**.py` has a relaxed profile for missing annotations and high branch/statement counts.
- Files: `pyproject.toml:100`, `pyproject.toml:121`, `pyproject.toml:146`, `pyproject.toml:180`, `pyproject.toml:182`, `pyproject.toml:224`
- Impact: New debt can land without CI pressure, especially in model/SFT code and runtime-heavy backend paths where small correctness issues affect paid training, alerting, or ingestion reliability.
- Fix approach: Tighten ignores by subtree. Keep notebooks exempt, but require strict lint/type coverage for `model/scripts/sft/`, security-sensitive backend modules, and new Python files.

**Frontend API configuration fails inconsistently:**
- Issue: `frontend/api/src/config.ts` throws for backend service URLs but only logs missing `PROJECT_ID`, `API_PUBLIC_URL`, `GOOGLE_AUTH_CLIENT_ID`, and `GOOGLE_AUTH_CLIENT_SECRET`; the OAuth client is still constructed from possibly undefined values.
- Files: `frontend/api/src/config.ts:31`, `frontend/api/src/config.ts:39`, `frontend/api/src/auth/authController.ts:30`
- Impact: Misconfigured deployments can boot and fail only when auth/docs routes are exercised.
- Fix approach: Fail startup for required production values, and split truly optional local-dev values into an explicit local config path.

**API docs lookup assumes one API Gateway config:**
- Issue: `DocsController` uses the first API returned by API Gateway, then globally caches the derived OpenAPI document. The code comment records that API ID selection is not configurable.
- Files: `frontend/api/src/docs/docsController.ts:12`, `frontend/api/src/docs/docsController.ts:30`, `frontend/api/src/docs/docsController.ts:84`
- Impact: Projects with multiple API Gateway APIs can serve the wrong docs until process restart.
- Fix approach: Add an API ID/config ID env var, validate it at startup, and cache by explicit key with an invalidation path for deployments.

**SFT pipeline uses hardcoded cloud project and bucket constants:**
- Issue: Gemini SFT output locations are constants in the CLI rather than configuration, so running in another project/bucket requires source edits.
- Files: `model/scripts/sft/pipeline.py:33`, `model/scripts/sft/pipeline.py:34`, `model/scripts/sft/pipeline.py:35`
- Impact: SFT runs are coupled to one environment and are easy to mis-route during development or evaluation.
- Fix approach: Move project, bucket, and prefix to CLI flags/env vars persisted into `model/scripts/sft/results/<round-id>/config.json`.

## Known Bugs

**SFT tune/eval/all commands are exposed but stubbed:**
- Symptoms: `python pipeline.py tune`, `python pipeline.py eval`, and `python pipeline.py all` return exit code `1` with not-implemented messages.
- Files: `model/scripts/sft/pipeline.py:332`, `model/scripts/sft/pipeline.py:338`, `model/scripts/sft/pipeline.py:344`, `model/scripts/sft/README.md`
- Trigger: Running the documented non-build SFT commands.
- Workaround: Only `build` is functional; tune/eval must be implemented before paid SFT runs.

**Echo SFT dataset cannot build until manifest placeholders are filled:**
- Symptoms: The Echo registry leaves `train_manifest_uri` and `val_manifest_uri` empty, while `build` requires a non-empty split URI for the requested split.
- Files: `model/scripts/sft/datasets.toml:13`, `model/scripts/sft/datasets.toml:15`, `model/scripts/sft/datasets.toml:21`, `model/scripts/sft/pipeline.py:109`
- Trigger: Running `python pipeline.py build --datasets echo ...` before the cluster-split output is registered.
- Workaround: Populate the split manifest URIs before running `build`.

**Notification webhook failures can be acknowledged as successful function executions:**
- Symptoms: `RequestHandler.send_notification()` does not raise on non-2xx HTTP responses, and `send_notification()` catches request exceptions without re-raising. The Cloud Function module also disables Pub/Sub retries.
- Files: `backend/pipeline/notification/request_handler.py:46`, `backend/pipeline/notification/request_handler.py:55`, `backend/pipeline/notification/send_notification.py:172`, `backend/pipeline/notification/send_notification.py:174`, `terraform/modules/cloud_function/main.tf:47`
- Trigger: Downstream notification endpoint returns 400/401/429/500 after urllib3 retries are exhausted, or request setup fails.
- Workaround: Operational logs are the only signal; no automatic retry or DLQ path is present for the function invocation.

**Normalization can emit test-bucket placeholder audio URIs:**
- Symptoms: If no canonical audio bucket is configured and the request is not a single-source passthrough, `NormalizeAudioFn` emits `gs://test-bucket/placeholder.*` values. The transcription processor then sends `claim.canonical_audio_uri` to the transcriber.
- Files: `backend/pipeline/normalization/transforms/stateful.py:1047`, `backend/pipeline/normalization/transforms/stateful.py:1083`, `backend/pipeline/normalization/transforms/stateful.py:1106`, `backend/pipeline/transcription/processor.py:95`
- Trigger: Normalization runs without `--canonical_audio_bucket` for multi-chunk or derivative-output cases.
- Workaround: Configure `canonical_audio_bucket`; replace placeholders with a validation error or DLQ record.

**Evaluation processor has unhandled TODO paths for failure modes:**
- Symptoms: Parse, evaluation, transcript write, and alert publish failures are marked as TODO. `_parse_cloud_event()` only returns `None` for missing data; malformed base64/protobuf input can raise outside the explicit skip path.
- Files: `backend/pipeline/evaluation/processor.py:68`, `backend/pipeline/evaluation/processor.py:91`, `backend/pipeline/evaluation/processor.py:102`, `backend/pipeline/evaluation/processor.py:117`, `backend/pipeline/evaluation/processor.py:131`
- Trigger: Corrupt Pub/Sub data, rules service failure, Transcripts API failure, or Pub/Sub publish failure.
- Workaround: Function-level retries or logs handle the failure; no processor-local DLQ/quarantine behavior is implemented.

## Security Considerations

**Backend service auth depends on an env flag that is not set anywhere in repo config:**
- Risk: `verify_oidc_token()` returns a local-dev identity whenever `IS_GCP` is not exactly `"true"`. Repo-wide search shows only the check and comments, not Terraform/Docker config that sets the flag.
- Files: `backend/pipeline/common/auth.py:45`, `backend/pipeline/common/env.py:4`, `backend/pipeline/common/env.py:12`, `backend/pipeline/common/storage/redis_service.py:29`
- Current mitigation: The code expects Terraform to set `IS_GCP`, and some services sit behind Cloud Run/API Gateway.
- Recommendations: Invert the default so auth is enforced unless `LOCAL_DEV_AUTH_BYPASS=true`; add deployment tests that assert `IS_GCP` or the replacement flag is present for every service.

**Frontend API accepts decoded JWTs without local verification:**
- Risk: TSOA auth decodes the bearer token with `jwt.decode()` and trusts API Gateway to have verified signature, audience, issuer, and email state. If the Express app is reachable directly or gateway config drifts, any decodable JWT-like payload can satisfy controller security.
- Files: `frontend/api/src/authentication.ts:33`, `frontend/api/src/authentication.ts:34`, `frontend/api/src/authentication.ts:41`, `frontend/api/src/transcripts/transcriptsController.ts:75`, `frontend/api/src/feeds/feedsController.ts:145`, `frontend/api/src/rules/rulesController.ts:226`
- Current mitigation: Controllers use `@Security('google_id_token')` and API Gateway extensions.
- Recommendations: Verify Google ID tokens locally with explicit audience and issuer checks, and reject unverified email if required by product policy.

**No application-level authorization is visible after authentication:**
- Risk: Authenticated users can call feed, rule, transcript, and docs endpoints without role/scope checks in controller/service code. Rules creation records `created_by`, but update/delete/list paths do not enforce ownership or role.
- Files: `frontend/api/src/feeds/feedsController.ts:145`, `frontend/api/src/rules/rulesController.ts:226`, `backend/services/rules/main.py:42`, `backend/services/rules/main.py:86`, `backend/services/feeds/main.py:35`
- Current mitigation: Authentication is required on protected routes when auth is enabled.
- Recommendations: Add an authorization dependency/policy layer, enforce roles for mutating feed/rule operations, and test 403 cases.

**User-controlled regex rules can cause CPU exhaustion:**
- Risk: `RegexConditions.expression` accepts raw regex strings, and evaluation runs `re.search()` directly with no compile validation, timeout, or safe-regex engine. Catastrophic backtracking can block evaluation workers.
- Files: `backend/pipeline/common/rules/models.py:50`, `backend/pipeline/common/rules/models.py:56`, `backend/pipeline/evaluation/rules_evaluation/evaluator.py:62`, `backend/pipeline/evaluation/rules_evaluation/evaluator.py:64`
- Current mitigation: None detected in rule models or evaluator.
- Recommendations: Validate regexes on create/update, cap expression length, use a regex engine with timeouts, or execute regex evaluation in a bounded worker.

**Notification logs include full alert payloads:**
- Risk: Outbound notification payloads include transcript text and audio URIs, and `RequestHandler` logs the serialized payload plus response body.
- Files: `backend/pipeline/notification/request_handler.py:36`, `backend/pipeline/notification/request_handler.py:41`, `backend/pipeline/notification/send_notification.py:115`, `backend/pipeline/notification/send_notification.py:118`, `backend/pipeline/notification/send_notification.py:119`
- Current mitigation: The API key is sent as a header and is not explicitly logged.
- Recommendations: Log message IDs, feed IDs, status, and redacted summaries only; move payload logging behind a local debug flag.

**Browser audio playback assumes public GCS access:**
- Risk: The UI rewrites `gs://` URIs directly to `https://storage.googleapis.com/...`, which requires public-readable objects or permissive IAM/CORS for emergency audio.
- Files: `frontend/transcription-ui/src/utils/audioUtils.ts:4`, `frontend/transcription-ui/src/utils/audioUtils.ts:15`, `frontend/transcription-ui/src/utils/audioUtils.ts:17`, `terraform/modules/gcs_bucket/main.tf:1`
- Current mitigation: Not detected in UI code; bucket module supports CORS but does not define signed URL behavior.
- Recommendations: Serve short-lived signed URLs or proxy authenticated audio through the API.

**Operational token passed as CLI argument:**
- Risk: `bulk_import_feeds.py` requires `--token`, which exposes the bearer token to shell history and process listings on shared machines.
- Files: `backend/scripts/bulk_import_feeds.py:157`, `backend/scripts/bulk_import_feeds.py:160`, `backend/scripts/bulk_import_feeds.py:175`
- Current mitigation: None detected.
- Recommendations: Read tokens from stdin, keychain, or an env var; redact token-bearing command examples.

**Secret-like files are present in the repo tree:**
- Risk: Environment template/local files exist and must be treated as sensitive during future scans and commits.
- Files: `local_dev/LOCAL.env`, `frontend/api/.env.example`, `frontend/transcription-ui/.env.example`
- Current mitigation: Contents were not read for this audit.
- Recommendations: Keep real secrets out of these files, add secret scanning, and ensure local env files are ignored unless they are intentionally sanitized examples.

## Performance Bottlenecks

**ffmpeg/ffprobe subprocesses have no timeout in synchronous audio helpers:**
- Problem: Malformed or adversarial audio can wedge a worker thread/process while `subprocess.run()` waits indefinitely.
- Files: `backend/pipeline/normalization/audio/audio_processor.py:148`, `backend/pipeline/normalization/audio/audio_processor.py:233`, `backend/pipeline/normalization/audio/audio_processor.py:279`, `backend/pipeline/common/audio.py:29`
- Cause: `subprocess.run()` is called without `timeout=` in decode/export/duration paths.
- Improvement path: Add bounded timeouts, classify timeout as a retryable/DLQ error, and cover with tests for hung subprocess behavior.

**Feed recovery SQL has a documented sort/index limit:**
- Problem: Recovery-path claims can become expensive if failing or active-abandoned row volume spikes.
- Files: `backend/pipeline/storage/feed_queries.py:246`, `backend/pipeline/storage/feed_queries.py:251`, `backend/pipeline/storage/feed_queries.py:254`
- Cause: Active-abandoned recovery uses `idx_feeds_active` plus filtering/sorting instead of a dedicated `(retry_after, id)` recovery index.
- Improvement path: Track query P99 and add the documented `idx_feeds_recovery` migration when production load approaches the stated threshold.

**Notification HTTP client creates a connection pool per send:**
- Problem: Every notification constructs a new `PoolManager`, losing connection reuse.
- Files: `backend/pipeline/notification/request_handler.py:25`, `backend/pipeline/notification/request_handler.py:43`
- Cause: Pool manager lifecycle is per request rather than per warm function instance.
- Improvement path: Move `PoolManager` to `RequestHandler.__init__`, set request timeouts, and raise on non-2xx responses.

**Rules evaluation scales linearly over all active rules and regex cost:**
- Problem: Each evaluation fetches cached full rule sets and checks all global plus feed-specific rules in Python.
- Files: `backend/pipeline/evaluation/rules_evaluation/evaluator.py:82`, `backend/pipeline/evaluation/rules_evaluation/evaluator.py:97`, `backend/pipeline/evaluation/rules_evaluation/evaluator.py:179`, `backend/pipeline/evaluation/rules_evaluation/evaluator.py:201`
- Cause: No per-feed pre-indexing beyond in-memory organization, no compiled regex cache, and no rule query filtering by feed.
- Improvement path: Cache compiled rule plans by rule version, fetch only applicable rules where possible, and bound regex execution.

## Fragile Areas

**NormalizerRuntime lifecycle and lease fencing:**
- Files: `backend/pipeline/ingestion/normalizer_runtime.py`, `backend/pipeline/ingestion/tests/test_runtime.py`, `integration_tests/storage/test_feed_store_integration.py`
- Why fragile: A 1,483-line runtime combines asyncio tasks, OS threads, cgroup memory watchdogs, DB lease fencing, heartbeat diagnostics, and deliberate `os._exit(1)` paths.
- Safe modification: Change one invariant at a time, keep cancellation/heartbeat behavior covered in `backend/pipeline/ingestion/tests/test_runtime.py`, and run storage integration tests for lease/fencing changes.
- Test coverage: Good branch coverage exists, but integration tests can skip when Docker is unavailable via `integration_tests/conftest.py:35`.

**Beam stateful normalization transforms:**
- Files: `backend/pipeline/normalization/transforms/stateful.py`, `backend/pipeline/normalization/transforms/stitcher_engine.py`, `backend/pipeline/normalization/tests/test_transforms.py`
- Why fragile: Continuous and segmented stateful DoFns manage Beam state, event-time timers, processing-time timers, sequence buffers, VAD, GCS uploads, DLQ routing, and placeholder URI fallback in one large module.
- Safe modification: Prefer moving pure audio/timer decisions into tested helpers before changing Beam DoFn state. Run `backend/pipeline/normalization/tests/test_transforms.py` and audio processor tests for any timer/output change.
- Test coverage: Placeholder URI fallback and missing canonical bucket behavior are not directly covered by a focused regression test.

**FeedStore SQL generation and database indexes:**
- Files: `backend/pipeline/storage/feed_store.py`, `backend/pipeline/storage/feed_queries.py`, `backend/pipeline/storage/tests/test_feed_store.py`, `integration_tests/storage/test_feed_store_integration.py`
- Why fragile: Claim/recovery SQL is generated dynamically from `SourceType`, uses materialized CTEs and `SKIP LOCKED`, and depends on partial indexes plus HOT-protection CI.
- Safe modification: Treat SQL comments as operational constraints, keep enum values closed, and test generated SQL plus real AlloyDB Omni behavior.
- Test coverage: Unit and integration coverage is substantial, but Docker-gated integration skips reduce confidence on machines without Docker.

**Notification warm-instance singletons:**
- Files: `backend/pipeline/notification/send_notification.py`, `backend/pipeline/common/storage/redis_service.py`, `backend/pipeline/notification/notification_deduplication.py`
- Why fragile: Redis and request handler instances are created at import time. Env variables are also read at import time in `request_handler.py` and `redis_service.py`.
- Safe modification: Move env validation and client construction into explicit startup/factory code that tests can reset without module reloads.
- Test coverage: Existing notification tests cover success, duplicate suppression, and max-retry exception; they do not cover non-2xx responses or swallowed send failures.

**Model/SFT import-path coupling:**
- Files: `model/scripts/sft/pipeline.py`, `model/scripts/sft/adapters/gcs_manifest.py`, `model/pyproject.toml`, `pyproject.toml`
- Why fragile: SFT tests mutate `sys.path` to import `pipeline` and `common`, root `ty` excludes `model/`, and SFT scripts use a relaxed Ruff profile.
- Safe modification: Package `model/scripts/sft` as an importable module or add a small wrapper entry point; run `model/scripts/sft/tests/` and `model/colabs/common/tests/` together.
- Test coverage: Build/preflight contracts are covered, but tune/eval are stubs.

## Scaling Limits

**Ingestion worker scale target is bounded by per-process feed count:**
- Current capacity: `NormalizerRuntime` documents a design target of 250 concurrent feeds per worker.
- Limit: Event loop stalls, heartbeat delays, or DB pool contention can terminate the process via `os._exit(1)`.
- Scaling path: Scale horizontally with MIG capacity, keep `max_feeds_per_worker` aligned with memory watchdog thresholds, and preserve the dedicated heartbeat pool.
- Files: `backend/pipeline/ingestion/normalizer_runtime.py:61`, `backend/pipeline/ingestion/normalizer_runtime.py:73`, `backend/pipeline/ingestion/normalizer_runtime.py:1041`, `backend/pipeline/ingestion/settings.py`

**Cloud Function module serializes event processing by default:**
- Current capacity: The reusable Terraform module caps functions at one instance.
- Limit: Alert/evaluation/notification bursts queue behind `max_instance_count = 1`, and Pub/Sub retry is disabled for triggered functions.
- Scaling path: Make max instances and retry policy caller-configurable per function, and add DLQs for non-idempotent handlers.
- Files: `terraform/modules/cloud_function/main.tf:34`, `terraform/modules/cloud_function/main.tf:47`, `backend/pipeline/notification/send_notification.py`

**SFT preflight GCS reachability is thread-pool bounded:**
- Current capacity: Preflight uses 16 workers and 256-URI batches by default.
- Limit: Very large SFT manifests can spend significant wall time in GCS reachability checks, and the checks run only when `storage_client` is provided.
- Scaling path: Persist preflight reports, make workers/batch size explicit CLI flags, and shard very large manifests.
- Files: `model/scripts/sft/preflight.py:28`, `model/scripts/sft/preflight.py:168`, `model/scripts/sft/preflight.py:247`

## Dependencies at Risk

**NeMo text normalization is pinned and optional tests skip when missing:**
- Risk: `nemo_text_processing==1.1.0` is pinned because normalization changes alter WER, but golden scoring tests skip on a bare-core checkout.
- Impact: A dependency bump or missing optional extra can silently reduce scoring validation in common local/CI paths.
- Migration plan: Run `model/colabs/common/tests/test_scoring.py` in a dedicated scoring CI job with `common[scoring]` installed before changing scoring dependencies.
- Files: `model/pyproject.toml`, `model/colabs/common/scoring.py`, `model/colabs/common/tests/test_scoring.py`

**Frontend API uses an alpha TSOA release:**
- Risk: `tsoa` is pinned to an alpha major release in the API package.
- Impact: Route/spec generation behavior can change across alpha updates and break `frontend/api/openapi.yaml` or generated routes.
- Migration plan: Pin exact versions for generator packages and keep `yarn --cwd frontend/api verify-spec` in CI.
- Files: `frontend/api/package.json`, `frontend/api/tsoa.json`, `frontend/api/openapi.yaml`

**Container base images are tags rather than digests:**
- Risk: Dockerfiles use mutable tags such as `python:3.13-slim`, `node:22-slim`, `apache/beam_python3.13_sdk:2.73.0`, and `cos-stable` for VM images.
- Impact: Rebuilds can pick up changed base layers without code changes.
- Migration plan: Pin production images by digest, automate base-image refreshes, and scan images in CI.
- Files: `backend/pipeline/ingestion/Dockerfile`, `backend/services/feeds/Dockerfile`, `frontend/api/Dockerfile`, `backend/pipeline/normalization/Dockerfile`, `terraform/modules/container_mig/main.tf`

**System ffmpeg/ffprobe dependency is assumed at runtime:**
- Risk: Audio decode/export/duration paths call `ffmpeg` and `ffprobe` binaries directly.
- Impact: Missing, incompatible, or hanging binaries break normalization and duration extraction.
- Migration plan: Assert binary presence at startup, pin package versions in images where possible, and add timeout/error classification around every subprocess call.
- Files: `backend/pipeline/normalization/audio/audio_processor.py`, `backend/pipeline/common/audio.py`, `backend/pipeline/ingestion/collectors/icecast/icecast_collector.py`

## Missing Critical Features

**Gemini SFT paid run lifecycle is incomplete:**
- Problem: Build/preflight pieces exist, but tune, eval, and all-in-one execution are not implemented.
- Blocks: Submitting Vertex AI SFT jobs, enforcing cost confirmation, evaluating tuned models, and writing run ledgers.
- Files: `model/scripts/sft/pipeline.py:332`, `model/scripts/sft/pipeline.py:338`, `model/scripts/sft/pipeline.py:344`, `model/scripts/sft/README.md`

**SFT data split registration is incomplete:**
- Problem: Echo `train_manifest_uri` and `val_manifest_uri` are placeholders.
- Blocks: Building train/validation JSONL from registered datasets without manual registry edits.
- Files: `model/scripts/sft/datasets.toml:13`, `model/scripts/sft/datasets.toml:15`, `model/scripts/sft/datasets.toml:21`

**Rule groups are modeled but not evaluated:**
- Problem: `GroupConditions` exists in the rule model, but evaluator explicitly skips it and returns `False`.
- Blocks: Nested/composite rule behavior for alerts.
- Files: `backend/pipeline/common/rules/models.py:60`, `backend/pipeline/evaluation/rules_evaluation/evaluator.py:79`

**Notification DLQ/retry behavior is absent:**
- Problem: Failed notification sends are logged and swallowed, and the Terraform function trigger disables Pub/Sub retries.
- Blocks: Guaranteed alert delivery and operator replay of failed notifications.
- Files: `backend/pipeline/notification/send_notification.py:172`, `backend/pipeline/notification/send_notification.py:174`, `terraform/modules/cloud_function/main.tf:47`

**Application authorization policy is not implemented:**
- Problem: Authenticated identity is not mapped to roles/scopes/permissions in the frontend API or backend services.
- Blocks: Least-privilege administration for feed/rule mutations and transcript visibility.
- Files: `frontend/api/src/authentication.ts`, `frontend/api/src/feeds/feedsController.ts`, `frontend/api/src/rules/rulesController.ts`, `backend/services/rules/main.py`, `backend/services/feeds/main.py`

## Test Coverage Gaps

**Evaluation failure branches lack focused tests:**
- What's not tested: Malformed base64/protobuf input, evaluator exceptions, Transcripts API write failures, and Pub/Sub publish failures.
- Files: `backend/pipeline/evaluation/processor.py`, `backend/pipeline/evaluation/tests/test_processor.py`
- Risk: Alerting/evaluation failures can regress into silent drops or uncontrolled retries.
- Priority: High

**Notification delivery tests do not cover non-2xx or swallowed send failures:**
- What's not tested: HTTP 400/401/429/500 handling, missing endpoint/API key handling, and whether function-level failures should re-raise for retry/DLQ.
- Files: `backend/pipeline/notification/request_handler.py`, `backend/pipeline/notification/test_request_handler.py`, `backend/pipeline/notification/send_notification.py`, `backend/pipeline/notification/test_send_notification.py`
- Risk: Failed notifications can be acknowledged as successful.
- Priority: High

**Authentication tests bypass auth instead of verifying production checks:**
- What's not tested: `IS_GCP` behavior, audience validation, missing/invalid bearer tokens through FastAPI services, and TSOA JWT verification assumptions.
- Files: `backend/services/feeds/tests/test_api.py`, `backend/services/rules/tests/test_api.py`, `backend/services/transcripts/tests/test_api.py`, `backend/pipeline/common/auth.py`, `frontend/api/src/authentication.ts`
- Risk: Auth bypass or gateway drift can reach production routes unnoticed.
- Priority: High

**Regex rule safety is untested:**
- What's not tested: Regex validation, catastrophic backtracking protection, expression length caps, and invalid flag handling.
- Files: `backend/pipeline/common/rules/models.py`, `backend/pipeline/evaluation/rules_evaluation/evaluator.py`, `backend/pipeline/evaluation/tests/test_evaluator.py`, `backend/services/rules/tests/test_api.py`
- Risk: A bad rule can block evaluator CPU and delay alerts.
- Priority: High

**NormalizeAudioFn placeholder URI behavior lacks regression coverage:**
- What's not tested: Missing `canonical_audio_bucket` with multi-source buffers and the downstream transcription behavior for placeholder URIs.
- Files: `backend/pipeline/normalization/transforms/stateful.py`, `backend/pipeline/normalization/tests/test_transforms.py`, `backend/pipeline/transcription/processor.py`
- Risk: Test placeholder GCS paths can enter production Pub/Sub claims.
- Priority: Medium

**SFT preflight is not wired into the CLI run path:**
- What's not tested: `pipeline.py build` or future `tune` invoking `run_preflight()` as a hard gate before paid operations.
- Files: `model/scripts/sft/preflight.py`, `model/scripts/sft/pipeline.py`, `model/scripts/sft/tests/test_pipeline_build.py`
- Risk: Paid SFT runs can proceed without data-quality checks once tune is implemented unless the integration is explicitly tested.
- Priority: High

**Docker-gated integration coverage can skip silently on machines without Docker:**
- What's not tested: Real AlloyDB Omni lease/schema behavior when Docker is unavailable.
- Files: `integration_tests/conftest.py:35`, `integration_tests/storage/test_feed_store_integration.py`, `backend/pipeline/ingestion/collectors/tests/test_bcfy_calls_collector_integration.py`
- Risk: Local and CI environments without Docker miss the highest-value storage and collector integration checks.
- Priority: Medium

---

*Concerns audit: 2026-05-27*
