# Codebase Concerns

**Analysis Date:** 2026-04-21

Scope: focused on the ingestion pipeline (stream capturer fleet, collectors, credential rotation, storage layer). Transcription pipeline concerns are noted only where they intersect with ingestion.

---

## Tech Debt

**Unhandled failure paths in evaluation processor:**
- Issue: Four `TODO (GOO-245)` markers — parse failure, evaluation failure, write failure, and publish failure are all caught and logged but not routed anywhere (DLQ, retry, metric).
- Files: `backend/pipeline/evaluation/processor.py:59,69,80,95`
- Impact: Silent data loss on any downstream breakage; operators won't see the issue without log-grepping.
- Fix approach: Route to a DLQ topic (or emit a counter metric) in each handler. Mirrors how ingestion uses Pub/Sub ordering keys for per-feed backpressure.

**Notification service has hard-coded config and `GOO-320` TODO:**
- Issue: Two TODOs for local-dev Redis (GOO-173) and non-env-var duration (GOO-320).
- Files: `backend/pipeline/notification/send_notification.py:34,66`
- Impact: Tuning notification duration requires a code change; local dev diverges from prod.
- Fix approach: Resolve the Linear tickets; promote magic numbers to env vars.

**Collector dedup buffer is in-process and hard-coded:**
- Issue: `seen_urls = collections.deque(maxlen=1000)` per-feed in memory; on worker restart the buffer is lost and the next poll can re-yield recent calls (idempotent at the GCS layer thanks to token-qualified paths + `ifGenerationMatch=0`, but still wastes Pub/Sub messages and duration).
- Files: `backend/pipeline/ingestion/collectors/bcfy_calls/bcfy_calls_collector.py:350`
- Impact: Duplicate chunk publications for roughly one poll interval after every restart; downstream stitcher must dedup.
- Fix approach: Use `lastPos`/`start_ts` pagination (partially done on line 401) as the only source of truth and drop the in-process set.

**Ruff ignore list is very permissive (~90 rules globally ignored):**
- Issue: `pyproject.toml:80-163` disables ALL-selected rules broadly, including `DTZ005`, `G004` (f-string logging), `BLE001` (blind exception catches), `S603/S607` (subprocess), `COM812`.
- Files: `pyproject.toml`
- Impact: Lint no longer catches real bugs (naive datetimes, f-string log injection, subprocess shell traversal risk). New contributors can merge code that violates project patterns.
- Fix approach: Re-enable rules one at a time, starting with `BLE001` (replace blind `except Exception:` with specific types) and `DTZ005`/`DTZ006` (we already pass `tz=UTC` most places — the remaining violations are real bugs).

**`from backend.pipeline.ingestion.settings import NormalizerSettings  # noqa: I001, PLC0415` inside `__init__`:**
- Issue: Deferred import to avoid a circular-import at module load.
- Files: `backend/pipeline/ingestion/normalizer_runtime.py:80`
- Impact: Hides the real coupling; every fresh reader is confused by the inline import.
- Fix approach: Move shared settings into a lower-level module that both can import cleanly.

---

## Known Bugs

**Stale `last_bookmark_time_unix` pagination on mid-loop error:**
- Symptoms: If `_fetch_calls` succeeds but a subsequent download fails, `last_bookmark_time_unix` is updated from `lastPos` on every successful fetch even if only *some* calls were yielded. Next poll starts after the last page — any un-yielded calls from the failed download are skipped.
- Files: `backend/pipeline/ingestion/collectors/bcfy_calls/bcfy_calls_collector.py:401-402`
- Trigger: 5xx or network error on `_download_audio` after exhausting retries inside a page that `_fetch_calls` returned. The loop continues, and `lastPos` advances.
- Workaround: Acceptable data loss per Broadcastify's semantics (chunks are short and audio is best-effort), but this is not documented. Consider moving the `lastPos` update to after the inner `for call` loop breaks cleanly.

**`_get_audio_format` is defined but its return value is never used in the chunk path:**
- Symptoms: `capture_bcfy_calls` passes a fixed `"m4a"` / `"audio/mp4"` via the runtime's per-source switch (`normalizer_runtime.py:362`) — the per-URL extension detection in `bcfy_calls_collector.py:147` is dead code with tests.
- Files: `backend/pipeline/ingestion/collectors/bcfy_calls/bcfy_calls_collector.py:147-167`
- Trigger: Any mixed-format call feed would get a wrong `.m4a` extension silently.
- Workaround: None in the hot path. Either wire the detection into `CapturedChunk` (adding a `content_type` / `extension` field) or delete the helper.

**`_handle_loop_failure` raises `RuntimeError` *after* sleeping:**
- Symptoms: When consecutive failures >= 10, the function sleeps `_POLL_INTERVAL_SEC` (10s) **before** raising, adding 10s to worker failure recovery per feed failure.
- Files: `backend/pipeline/ingestion/collectors/bcfy_calls/bcfy_calls_collector.py:310-318`
- Trigger: Sustained auth or 5xx errors on a feed.
- Workaround: None needed — the raise path is taken only once per feed before the task exits and the feed is recorded-as-failed. But it delays quarantine telemetry by the sleep interval.

**Echo ingestion TOCTOU on feed status lookup:**
- Symptoms: `feed_store.resolve_echo_feed` reads status once; subsequent `record_heartbeat` / `record_failure` do not re-check quarantine status. An operator-triggered `deactivated` during handling still completes the upload+publish.
- Files: `backend/pipeline/ingestion/collectors/echo/main.py:107-180`
- Trigger: Operator deactivates a feed while a Cloud Run instance is mid-handler.
- Workaround: Accept the one-in-flight race — the next event drops. No fix recommended unless it becomes a problem.

---

## Security Considerations

**Broadcastify credentials pulled from plaintext env vars in the Cloud Function:**
- Risk: `BROADCASTIFY_USERNAME`, `BROADCASTIFY_PASSWORD`, `BROADCASTIFY_API_KEY`, `BROADCASTIFY_API_APP_ID`, `BROADCASTIFY_API_KEY_ID` are read via `os.environ` at import time. If the Cloud Function deployment YAML or Terraform output ever logs these, they surface in Cloud Logging.
- Files: `backend/pipeline/ingestion/broadcastify_credential_rotation/main.py:30-36`
- Current mitigation: Terraform presumably injects these via Secret Manager; credential rotation cycles them every run and destroys versions older than 6h (`cleanup_old_versions`, line 48).
- Recommendations: Switch from env-var injection to in-function Secret Manager reads (matches how `bcfy_calls_collector.py:57` reads the JWT). Centralises the secret-access audit trail in IAM.

**JWT logged in plaintext during auth-refresh failure:**
- Risk: `bcfy_calls_collector.py:408-412` logs `"Auth failure (401/403) for feed %s and token %s, refreshing token."` with the *failing* JWT. If Cloud Logging retention is long and log-based alerting surfaces this message, the token leaks.
- Files: `backend/pipeline/ingestion/collectors/bcfy_calls/bcfy_calls_collector.py:408-412`
- Current mitigation: Token expires after 1h (line 171 in rotation); retention risk window is bounded.
- Recommendations: Log only the JWT `kid` or a SHA256 prefix. Never log full Bearer tokens, expired or not.

**ffmpeg subprocess invoked with user-controlled URL:**
- Risk: `capture_icecast_stream` concatenates `source_feed_id` into the stream URL (`icecast_collector.py:127`). An attacker with DB write access (to `feed_properties.source_feed_id`) could inject `file://` paths, `&#` control chars, or `-fflags ...` injection into positional args. However, the value is passed to ffmpeg via `-i <url>` after URL normalization (`urljoin`), and `create_subprocess_exec` does NOT invoke a shell, so standard CLI flags are safe. `file://` injection via the URL would still let ffmpeg read local files.
- Files: `backend/pipeline/ingestion/collectors/icecast/icecast_collector.py:127,289-314`
- Current mitigation: Threat model assumes DB writes require authenticated operator access.
- Recommendations: Validate `source_feed_id` against an allowlist (alphanumeric + `-`) at the DB constraint level and at creation time (`feed_store.create_feed`, line 436 already checks emptiness — extend to format).

**`Authorization: Basic ...\r\n` header interpolated into ffmpeg `-headers`:**
- Risk: `_build_auth_header` passes `f"Authorization: Basic {encoded}\r\n"` to ffmpeg. If `BROADCASTIFY_PASSWORD` ever contained `\r\n` (not base64-encoded here — it's the password that gets base64-encoded, so CRLF in the password would be encoded safely). Current code is safe but fragile if the base64 step is ever refactored out.
- Files: `backend/pipeline/ingestion/collectors/icecast/icecast_collector.py:40-52`
- Current mitigation: `base64.b64encode(credentials.encode())` neutralises any CRLF.
- Recommendations: Add a comment marking the b64-encode as security-critical to avoid future "simplification" removing it.

**Secret Manager client created without explicit audience or impersonation check:**
- Risk: Default ADC is used (`secretmanager.SecretManagerServiceClient()`). If the runtime service account is ever over-granted (e.g. `roles/secretmanager.admin`), any code path on the VM can read arbitrary project secrets.
- Files: `backend/pipeline/ingestion/collectors/bcfy_calls/bcfy_calls_collector.py:57` and `broadcastify_credential_rotation/main.py:251`
- Current mitigation: Assumed IAM principle of least privilege at the Terraform layer.
- Recommendations: Audit the service account's IAM bindings — it should only have `roles/secretmanager.secretAccessor` scoped to the specific `BROADCASTIFY_JWT_SECRET_ID` resource.

---

## Performance Bottlenecks

**Per-feed `asyncio.to_thread` for filesystem reads on every 10s segment:**
- Problem: `icecast_collector.py:182,206` moves `read_bytes()` and `unlink()` to the default thread executor. At 250 feeds × one read per 10s = 25 thread submissions per second, plus an additional submission per unlink. The default executor is `ThreadPoolExecutor(max_workers=min(32, cpu_count + 4))` ≈ 8-12 workers on typical GCE instances.
- Files: `backend/pipeline/ingestion/collectors/icecast/icecast_collector.py:182,206`
- Cause: Thread-pool saturation would back-pressure reads and widen segment-to-pubsub latency. Current load suggests this is fine, but there's no explicit pool sizing.
- Improvement path: Either (a) set a named `ThreadPoolExecutor(max_workers=32)` via `loop.set_default_executor` at runtime startup, or (b) use `aiofiles` for true-async file I/O. Benchmark before deciding.

**JWT Secret Manager fetch is synchronous on hot path:**
- Problem: `_get_jwt_token` uses the synchronous `secretmanager` client, wrapped in `asyncio.to_thread`. On token-expiry 401, every feed refetches its own JWT (line 414), serialized through the same thread pool as the filesystem reads above.
- Files: `backend/pipeline/ingestion/collectors/bcfy_calls/bcfy_calls_collector.py:49-65,414`
- Cause: No shared/cached JWT across feed tasks — 250 feeds each hold their own JWT string and each refreshes independently.
- Improvement path: Share a single cached JWT via a module-level `asyncio.Lock` + expiry TTL, refreshed by the first feed to observe a 401. Would cut Secret Manager API load by ~250×.

**AlloyDB pool sizing is tiny at 5/5:**
- Problem: `AlloyDBSettings` defaults to `pool_min_size=5, pool_max_size=5`. At 250 feeds per worker, each bookmark write must queue for one of 5 connections. Query durations >20ms (transcription throughput work, not ingestion) would back up the main pool.
- Files: `backend/pipeline/storage/settings.py:39-48`
- Cause: Default tuned for low-traffic Cloud Run services; ingestion overrides via `ALLOYDB_POOL_MAX_SIZE` env var.
- Improvement path: Document the env-var override in `normalizer_runtime.py` header. Run load tests at the 250-feed target to validate pool sizing — expected per-worker burst is 250 / heartbeat_interval_sec = ~17 writes/sec, easily absorbed by 5-20 connections at sub-10ms write latency.

**Pub/Sub `ordering_key=feed_id` forces serial publish per feed:**
- Problem: Required for the stitcher to reassemble chunks in order, but serial publish means one slow publish on a feed blocks all its subsequent chunks.
- Files: `backend/pipeline/common/gcp_helper.py:271`
- Cause: Pub/Sub ordering is a strong constraint from the downstream consumer.
- Improvement path: None without redesigning the stitcher. Monitor publish latency p99 per feed and alert on tail.

**`asyncio.sleep(POLL_INTERVAL_SEC)` in icecast hot loop (0.25s):**
- Problem: Every capture task polls filesystem 4×/sec. At 250 feeds = 1,000 wake-ups/sec just for polling. uvloop handles this, but CPU cost scales linearly.
- Files: `backend/pipeline/ingestion/collectors/icecast/icecast_collector.py:248`
- Cause: No inotify/fsevents plumbing — polling is simple and robust but wastes cycles.
- Improvement path: `asyncio.add_reader`-style FS-event watching would cut CPU noticeably at the 250-feed target. Lower-risk: increase poll interval to 0.5s and rely on ffmpeg's segment-time (10s) being much larger. Empirically validate the current cost first (uvloop makes this likely a non-issue).

---

## Fragile Areas

**Heartbeat OS thread + asyncio race windows:**
- Files: `backend/pipeline/ingestion/normalizer_runtime.py:105-109,507-545,640-692`
- Why fragile: The `_releasing_feeds: set[FeedID]` guard is load-bearing. Every `_process_feed` exit path (normal exhaust, `LeaseExpiredError`, generic `Exception`) must add-to and remove-from the set at exactly the right moments — no `await` may intervene between `self._releasing_feeds.discard(feed["id"])` and `return`. If any future refactor adds an `await` between those lines, a race lets the heartbeat thread misread the state and trigger a spurious `os._exit(1)`, killing the whole worker and cancelling 249 healthy tasks.
- Safe modification: Before editing `_process_feed`, preserve the `# SAFETY: No await between discard() and return` invariant. Add a unit test that asserts `_releasing_feeds` is drained before return. Any new early-exit path must wrap the failure-recording block in the same `add/try/discard` pattern.
- Test coverage: Comprehensive at `backend/pipeline/ingestion/tests/test_runtime.py` (1204 lines) — the largest test file in the project. Check whether it explicitly covers the `_releasing_feeds` edge cases across all three exit paths.

**`os._exit(1)` bypasses asyncio cleanup AND Python finally blocks:**
- Files: `backend/pipeline/ingestion/normalizer_runtime.py:475,592,692`
- Why fragile: Three call sites. Each is preceded by `logging.shutdown()` to flush CloudLoggingHandler. If anyone adds a new call site and forgets `logging.shutdown()`, the fence-violation log line is lost and post-mortem analysis is blind.
- Safe modification: Add a wrapper `_fatal_exit(reason: str)` that does logger.critical + logging.shutdown + `os._exit(1)` atomically. All three current sites should call it. Prevents divergence.
- Test coverage: These paths are inherently hard to unit-test (the process would actually exit) — likely mocked. Verify the fixture doesn't mock away the `logging.shutdown()` check.

**`concurrent.futures.TimeoutError` vs builtin `TimeoutError` alias:**
- Files: `backend/pipeline/ingestion/normalizer_runtime.py:580-592`
- Why fragile: In Python 3.11+, `asyncio.TimeoutError` became an alias for builtin `TimeoutError`, but `concurrent.futures.TimeoutError` is a distinct class. A careless `except TimeoutError:` here would silently catch `concurrent.futures.TimeoutError` on Python ≤ 3.10 but miss it on 3.13 (the project's target). The existing inline comment (line 581-585) is the only guard against this regression.
- Safe modification: Add a `typing.assert_never` or explicit `isinstance` check to make the distinction mechanical.

**Icecast ffmpeg stderr drain task + finally-block cancellation:**
- Files: `backend/pipeline/ingestion/collectors/icecast/icecast_collector.py:250-258`
- Why fragile: If `drain_task` or `wait_task` cancellation throws an exception other than `asyncio.CancelledError`, the `_cleanup_ffmpeg_process` call that force-terminates ffmpeg is skipped, leaking the subprocess until shutdown cascades.
- Safe modification: Wrap each individual cleanup step in its own `try/except Exception`. The `contextlib.suppress(asyncio.CancelledError)` currently only catches `CancelledError`.

**PgBouncer transaction-mode `statement_cache_size=0`:**
- Files: `backend/pipeline/storage/connection.py:65`
- Why fragile: Required for PgBouncer transaction pooling (asyncpg prepared statements don't survive transaction boundaries when multiplexed). A subtle change to PgBouncer mode or a drop-in replacement would silently re-enable statement caching, leading to random `InvalidSqlStatementNameError` under load. The comment on line 65 is the only guard.
- Safe modification: Add a startup integration test that confirms the kwarg reaches asyncpg unchanged.

**`_lease_lost` event is set-only, never cleared:**
- Files: `backend/pipeline/ingestion/normalizer_runtime.py:93-97,600-601`
- Why fragile: Intentional (monotonic signal — once uncertain, stay uncertain). But if anyone adds a `self._lease_lost.clear()` call to "recover", every retry loop in every feed task will now silently accept stale leases. The invariant is captured only in the comment on line 93-96.
- Safe modification: Rename to `_lease_lost_once` to make the one-shot semantic explicit, or wrap in a `OneShotEvent` class that asserts on `.clear()`.

---

## Scaling Limits

**Per-instance feed capacity: 250:**
- Current capacity: `MAX_FEEDS_PER_WORKER=250` (`settings.py:43-47`). Design target cited throughout normalizer_runtime.py header.
- Limit: Event-loop throughput. uvloop (`normalizer_runtime.py:142`) is required at this scale. Above 250, scheduling overhead at `asyncio.wait_for` / `Event.wait` sites becomes non-trivial.
- Scaling path: Horizontal — Managed Instance Group scales on "Stream Utilization %". Pushing per-instance higher would require benchmarking event-loop latency under `uvloop.run` at 500+ tasks.

**AlloyDB connection budget vs. fleet size:**
- Current capacity: Per worker: main pool `max_size=5` (default) + heartbeat pool `max_size=1`. At N workers: 6N connections.
- Limit: AlloyDB's advertised 1000+ concurrent connections, but PgBouncer transaction-pooling is the real throughput determinant.
- Scaling path: Bump `ALLOYDB_POOL_MAX_SIZE` env var per worker. Heartbeat pool is intentionally kept at 1 to guarantee no queuing — do NOT raise this.

**Pub/Sub ordering-key cardinality:**
- Current capacity: One ordering key per feed_id. Pub/Sub limit is 1,000,000 ordering keys per topic.
- Limit: Well under Pub/Sub limits for foreseeable feed count.
- Scaling path: Not a concern.

**GCS aiohttp connection pool:**
- Current capacity: `max_connections=MAX_FEEDS_PER_WORKER=250` per worker (`normalizer_runtime.py:117-119`).
- Limit: Sized to eliminate upload queueing at design-target load.
- Scaling path: Increase proportionally if `MAX_FEEDS_PER_WORKER` is raised.

**Audio-chunk Pub/Sub message size:**
- Current capacity: Message carries `gs://` URI + metadata only, not audio bytes. Stays well under Pub/Sub's 10 MB limit.
- Limit: N/A.
- Scaling path: N/A.

---

## Dependencies at Risk

**`uvloop>=0.21.0` with no upper bound:**
- Risk: uvloop is a C extension replacement for asyncio's default event loop. A 0.22+ release with any API change could break `asyncio.run(..., loop_factory=uvloop.new_event_loop)` (`normalizer_runtime.py:142`).
- Impact: Catastrophic — worker fails to start. MIG autohealer would replace instances indefinitely.
- Migration plan: Pin to `uvloop>=0.21.0,<0.22.0` until manually tested. Add a startup smoke test that imports uvloop and constructs a loop.

**`curl_cffi>=0.9.1` (OpenMHZ transport only):**
- Risk: curl_cffi provides browser-TLS impersonation (`AsyncSession(impersonate="chrome")` — `_ws_transport.py:88`). OpenMHZ blocks non-browser TLS fingerprints, so any breaking change in curl_cffi's WebSocket API would silently break the OpenMHZ collector.
- Impact: OpenMHZ feeds stop ingesting. Other collectors unaffected.
- Migration plan: Pin explicitly; integration test `test_openmhz_ws_transport.py` catches breakage, but only if CI runs against the pinned version.

**`asyncpg>=0.29.0` + PgBouncer interaction:**
- Risk: asyncpg 0.30+ changed prepared-statement handling. Combined with `statement_cache_size=0` workaround (`connection.py:65`), future asyncpg versions could silently break PgBouncer compatibility.
- Impact: Feed leasing / heartbeat queries fail randomly under PgBouncer.
- Migration plan: Add an integration test (docker-compose PgBouncer + AlloyDB) that validates concurrent leasing under transaction pooling. Currently only `testcontainers[postgres]` is in dev deps (`pyproject.toml:55`).

**`functions-framework>=3.10.1`:**
- Risk: Used by both the Echo Cloud Run handler and the credential rotation Cloud Function. A breaking change in the framework's request/event signature cascades.
- Impact: Echo ingestion and credential rotation both break simultaneously.
- Migration plan: Pin, and add a CI step that does a dry-import of both handler modules.

**`apache-beam` upgrade (cited in commit log):**
- Risk: Recent commit `86c502b [ENG-ONLY] Update apache-beam to >=2.71.0 (#285)` — the transcription pipeline depends on Beam's DirectRunner. Tests `test_transforms.py` has `@unittest.skip("DirectRunner metrics validation is flaky")` (line 400).
- Impact: Flaky test means real regressions in transform metrics go undetected.
- Migration plan: Replace DirectRunner metric assertions with deterministic mocks; re-enable the skipped tests.

---

## Missing Critical Features

**SLO instrumentation not yet implemented:**
- Problem: The metrics `receipt_time`, `chunk_ingested`, `call_download_failed`, `active_feed_count` are referenced in planning docs but NOT emitted by the codebase today. Grep returns zero matches across the whole tree. The only custom Cloud Monitoring metric present is `custom.googleapis.com/feeds/quarantine_events` (`quarantine_telemetry.py:19`).
- Blocks: On-call engineers have no latency/throughput dashboards. Fleet-wide stalls can only be detected via `/healthz` kill-and-restart cycles.
- Note: WIP stash exists (`3bc8574 WIP: SLO instrumentation draft + autohealing terraform - preserved during replan`). Not yet merged.

**No DLQ for failed chunk publishes:**
- Problem: If `publish_audio_chunk` fails after retries exhaust (catastrophic Pub/Sub outage), the chunk is dropped. No retry queue, no spill-to-disk.
- Blocks: Durable "every capture eventually makes it to the transcriber" guarantee.
- Workaround: Rely on the 60s abandonment window to re-lease the feed; collector will re-yield the chunk from its pagination bookmark. Works only if the feed-store write also failed.

**No structured chunk-lifecycle tracing:**
- Problem: A chunk transits: collector yield → GCS upload → Pub/Sub publish → feed-progress bookmark. No correlation ID is propagated end-to-end. `session_id` (`models.py` via `CapturedChunk`) is the closest, but it's per-WebSocket-session, not per-chunk.
- Blocks: Root-causing "this chunk was lost between upload and publish" requires log-joining by timestamp + GCS URI.
- Fix approach: Add a `chunk_uuid` set at yield time, threaded through Pub/Sub attrs and log lines.

**No feed-source rate-limiting:**
- Problem: `bcfy_calls_collector.py` handles 429 by raising `RuntimeError` (line 73), which triggers the quarantine path after 10 failures. There is no adaptive backoff on 429; any rate-limit storm takes a feed offline for the consecutive-failure backoff.
- Blocks: Graceful degradation during Broadcastify API overload.
- Fix approach: On 429, apply longer jittered backoff (minutes, not seconds) *without* incrementing the consecutive-failure counter.

---

## Test Coverage Gaps

**`test_runtime.py` monster file (1204 lines):**
- What's not tested: Hard to say without reading it top-to-bottom, but any file at this size usually means scenarios that should be separate files are tangled. High-value check: do all three `os._exit(1)` call sites have dedicated tests that exercise the exact race windows (batched-heartbeat stolen lease, stall timeout, bookmark fence violation)?
- Files: `backend/pipeline/ingestion/tests/test_runtime.py`
- Risk: A race condition caught only in production manifests as `os._exit(1)` and whole-worker termination.
- Priority: High. Audit the test file; split into focused modules per invariant.

**No test for the `_releasing_feeds` no-await-between-discard-and-return invariant:**
- What's not tested: The SAFETY comments at `normalizer_runtime.py:528-532, 543-545` are load-bearing, but nothing enforces them. A careless refactor could insert an `await` without tripping CI.
- Files: `backend/pipeline/ingestion/normalizer_runtime.py` (invariant), `backend/pipeline/ingestion/tests/test_runtime.py` (would need new test)
- Risk: Silent reintroduction of the race that `_releasing_feeds` was designed to prevent. Manifests as spurious fence-violation `os._exit(1)` kills.
- Priority: High.

**Flaky DirectRunner tests are skipped:**
- What's not tested: Beam transform metrics assertions (`test_transforms.py:400`) and two additional skipped tests at lines 945, 1137.
- Files: `backend/pipeline/transcription/tests/test_transforms.py`
- Risk: Transcription pipeline transforms can regress their metrics-emission without anyone noticing.
- Priority: Medium (transcription, not ingestion).

**Ingestion tests count vs. lines of code:**
- What's not tested: `normalizer_runtime.py` (781 lines) : `test_runtime.py` (1204 lines) = healthy ratio. `bcfy_calls_collector.py` (427) : `test_bcfy_calls_collector.py` (1073) + integration (427) = thorough. `icecast_collector.py` (347) : `test_icecast_collector.py` (606) + integration (521) = thorough. The concerning outlier is `openmhz/collector.py` (196 lines) — its test file `test_openmhz_collector.py` size was not read, plus a transport integration test. Verify coverage % there.
- Files: `backend/pipeline/ingestion/collectors/openmhz/collector.py`
- Risk: OpenMHZ reconnect logic, backoff calculation, and download retry are all in `collector.py` with non-trivial control flow.
- Priority: Medium.

**Integration tests require Docker + ffmpeg:**
- What's not tested: CI environments without Docker skip `test_icecast_collector_integration.py`, `test_bcfy_calls_collector_integration.py`, `test_openmhz_collector_integration.py`, `test_echo_collector_integration.py`. `@unittest.skipUnless(_docker_available(), ...)` guards.
- Files: `backend/pipeline/ingestion/collectors/**/tests/*_integration.py`
- Risk: Integration regressions only caught by engineers running locally with Docker. CI-only PRs may pass while integration behaviour breaks.
- Priority: Medium. Either make Docker a hard CI requirement or mark these tests as required pre-merge.

**No test for quarantine auto-heal on heartbeat:**
- What's not tested: MEMORY notes "Feedback: Quarantine auto-heal — Heartbeat should auto-heal quarantined feeds". The `normalizer_runtime.py` heartbeat path does not reset `failure_count` or `status` — the Echo collector's `record_heartbeat` (line 180) does. For streaming/ingestion-VM sources the quarantine auto-heal is presumably implemented in SQL (`REPORT_FAILURE_SQL` / `ACQUIRE_FEEDS_BATCH_SQL`), but I did not verify.
- Files: `backend/pipeline/storage/feed_queries.py` (not opened), `backend/pipeline/storage/tests/test_feed_store.py`
- Risk: A quarantined feed that recovers upstream may stay stuck in `quarantined` status forever.
- Priority: Medium. Read `feed_queries.py` to confirm, then add explicit test if absent.

---

*Concerns audit: 2026-04-21*
