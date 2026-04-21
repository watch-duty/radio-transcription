# Ingestion SLO Instrumentation

## What This Is

Application-side instrumentation on the audio-ingestion pipeline that produces the signals the ops team's SLO monitoring stack consumes. Five Python-side additions — one dataclass field, three structured logs, one custom Cloud Monitoring metric — give the existing `/healthz` endpoint and the soon-to-land Terraform alerts (policies 1–10 in the SLO design) everything they need to detect processing failures, per-source latency regressions, instance-level crashes, and fleet-silent events.

## Core Value

**The emitted fields and metric type exactly match what the Terraform alert side filters on.** A typo in `custom.googleapis.com/ingestion/active_feed_count`, `event_type=chunk_ingested`, `event_type=call_download_failed`, or `jsonPayload.source_type` silently breaks the alerts downstream without any test failing in this repo. Producing those signals correctly — and having tests that pin the exact payload shapes so they can't drift — is the one thing that must work.

## Requirements

### Validated

<!-- Shipped and confirmed valuable — inferred from the codebase map. These are locked. -->

- ✓ **EXIST-01**: Regional MIG of n2-standard-4 VMs runs two `NormalizerRuntime` workers each, claiming feeds from AlloyDB via `SELECT ... FOR UPDATE SKIP LOCKED` — `backend/pipeline/ingestion/normalizer_runtime.py`
- ✓ **EXIST-02**: Three streaming/polling collectors (`bcfy_feeds`, `openmhz`, `bcfy_calls`) yield `CapturedChunk` into the runtime; fourth source `echo` runs on Cloud Run via Eventarc — `backend/pipeline/ingestion/collectors/{icecast,openmhz,bcfy_calls,echo}/`
- ✓ **EXIST-03**: Per-chunk pipeline: GCS upload → Pub/Sub publish → AlloyDB bookmark, each wrapped in `retry_with_lease_check` with fencing-token fence-violation detection (`os._exit(1)` on loss) — `backend/pipeline/ingestion/normalizer_runtime.py:_process_feed`
- ✓ **EXIST-04**: OS heartbeat thread + 45 s stall watchdog; `HealthState` + aiohttp `/healthz` endpoint already shipped (PR #247) with DB-decoupled staleness gate and intentional rejection of "zero feeds → 503" — `backend/pipeline/ingestion/health_server.py`
- ✓ **EXIST-05**: `feed_quarantined` structured log already emits `event_type`, `feed_id`, `source_type`, `feed_name` via `logger.error(msg, extra={...})` — `backend/pipeline/ingestion/quarantine_telemetry.py:49-57`

### Active

<!-- Current scope for this project. Hypotheses until shipped and validated. -->

- [ ] **RCPT-01**: `CapturedChunk` dataclass has a `receipt_time: datetime | None = None` field (UTC, tz-aware)
- [ ] **RCPT-02**: Icecast collector stamps `receipt_time` at the moment a segment is *finalized* (not filling) — before the `read_bytes()` call in `icecast_collector.capture_icecast_stream`
- [ ] **RCPT-03**: OpenMHZ collector stamps `receipt_time` at the moment a WS event arrives at the collector loop (first statement of the `async for call in events:` body)
- [ ] **RCPT-04**: `bcfy_calls` collector stamps `receipt_time` per-call iteration inside `for result in calls:` (not at the listing fetch)
- [ ] **LOG-01**: `chunk_ingested` structured INFO log emitted exactly once per chunk **after** GCS + Pub/Sub + bookmark all succeed, with `event_type`, `feed_id`, `source_type`, `processing_latency_sec` (rounded to 2 decimals; `null` when `receipt_time is None`) under `extra={...}`
- [ ] **LOG-02**: `call_download_failed` structured WARNING log emitted for openmhz + bcfy_calls on terminal download failure (retries exhausted), with `event_type`, `feed_id`, `source_type`. Emitted at the caller site; suppressed during shutdown. Not emitted on success-on-retry. `bcfy_feeds` never emits this.
- [ ] **METRIC-01**: `active_feed_count` custom metric published every 60 s as GAUGE INT64 to `custom.googleapis.com/ingestion/active_feed_count`, `gce_instance` monitored resource, `instance_id`/`zone` resolved from the GCE metadata server. No `feed_id` or `source_type` label (cardinality).
- [ ] **METRIC-02**: Metric reporter is a standalone asyncio task with injectable Cloud Monitoring client and sleep function (testable in isolation), survives transient API errors, disables gracefully on metadata failure in non-GCE environments.
- [ ] **HEALTHZ-01**: Regression test pins current `/healthz` behavior — 200 during startup grace, 200 with fresh heartbeat, 503 with stale heartbeat. Catches future accidental re-coupling to DB or removal of the DB-decoupled stamp.
- [ ] **VERIFY-01**: Pre-flight dry-run matches emitted strings to the Terraform-side alert filters: metric type exactly `custom.googleapis.com/ingestion/active_feed_count`, log event types exactly `chunk_ingested` / `call_download_failed` / `feed_quarantined`, logger path `backend.pipeline.ingestion.*`.
- [ ] **VERIFY-02**: Local smoke-test — boot a worker against a test AlloyDB + recorded upstream fixture; confirm all three log lines appear in stdout as parseable JSON with the expected fields.

### Out of Scope

<!-- Explicit boundaries. Includes reasoning to prevent re-adding. -->

- **Terraform alert policies / log-based metrics / notification channels** — the ops team owns the alerting side. This project only produces the signals.
- **Transcription / evaluation / notification pipeline SLO instrumentation** — deferred. Those services have their own (Cloud Run) observability stack; a separate project would cover end-to-end SLI work across them.
- **Cloud Run echo ingestion SLIs** — echo uses Cloud Run's built-in `request_count` and `request_latencies`, so Alert 3 (echo SLO) and Alert 6 (echo latency) don't need application-side instrumentation.
- **Refactoring `MonitoringClient` to unify with the new reporter** — quarantine events (global resource + feed_id labels) and active_feed_count (gce_instance resource + no feed_id) have fundamentally different usage shapes. Keeping them separate.
- **Autoscaling on `active_feed_count`** — the metric is designed to feed the autoscaler, but the autoscaler config is a separate Terraform-side decision.
- **Pre-existing `publish_audio_chunk` signature bug** — already fixed on main (PR landed before this project started).

## Context

### The codebase
- Python 3.13 / uv / ruff / pytest; monorepo with `backend/`, `frontend/transcription-ui/`, `terraform/`, `integration_tests/`. Full map in `.planning/codebase/`.
- Ingestion runtime targets 250 concurrent feeds per GCE instance; peak fleet ~12,000 feeds across ~25 VMs.
- Existing structured-log pattern: `logger.<level>(msg, extra={...})` — exemplified by `quarantine_telemetry.py`. Cloud Logging picks up the `extra` dict as `jsonPayload`.
- Existing `MonitoringClient` at `backend/pipeline/common/clients/monitoring_client.py` uses `global` resource with per-event labels — for quarantine events. Not reused for `active_feed_count` (different shape).

### The design driver
- The SLO design document specifies 10 alert policies (CRIT-severity, PagerDuty-routed) covering pipeline availability (SLI 1), processing success (SLI 2), and quarantine rate (SLI 3). The instrumentation here produces signals for alerts 1, 2, 5a–c, 7, 8; alerts 3, 4, 6, 9, 10 use either the Cloud Run built-ins or pre-existing logs.
- Cardinality constraint: Cloud Monitoring ~30 k time-series soft limit. Adding `feed_id` to a metric label at 12 k feeds × 25 VMs (~300 k series) silently drops data points. Labels here are `instance_id`-only.
- `/healthz` must NOT probe AlloyDB/GCS/Pub/Sub — a transient DB outage cascading to fleet-wide 503s would cause the MIG autohealer to replace every VM simultaneously.

### Recent history in this session
- Codebase map landed at `commit 2179d51` on branch `feat/slo-ingestion-instrumentation`.
- Prior WIP SLO draft (pre-mapping, from an earlier branch) is preserved at `stash@{0}`; may be cherry-picked for reference but not relied on.
- The `/healthz` + MIG autohealing was landed by PR #247 (`ae22f91`). That PR's author explicitly rejected the prompt's "zero-feeds → 503" gate with documented reasoning; this project respects that choice.

## Constraints

- **Cardinality**: Custom-metric labels must be `instance_id`-only. No `feed_id`, no `source_type` as metric labels. (Logs may include feed_id — different limit.)
- **`/healthz` purity**: No downstream-dependency probes. Local process liveness only.
- **Log schema stability**: `event_type` must be a top-level JSON field in all three structured logs. Payload field names must match the Terraform alert filters exactly.
- **No new runtime dependencies**: `google-cloud-monitoring`, `aiohttp`, `asyncio` already in the tree. Anything else should be justified.
- **Loop safety**: Metric reporter must not share pools/locks/queues with the feed-processing loop. A wedged reporter cannot block feed processing, and vice versa.
- **Shutdown semantics**: The reporter and `/healthz` server must respect SIGTERM cleanly — honor the existing `_sleep_or_shutdown` pattern and the shutdown-sequence ordering in `NormalizerRuntime._shutdown_sequence`.

## Key Decisions

| Decision | Rationale | Outcome |
|----------|-----------|---------|
| `/healthz` stays as currently implemented on main | PR #247 already decoupled it from AlloyDB and intentionally rejects the zero-feeds gate; Alert 8 handles that signal via the active-feed-count metric with a 3-min duration, which avoids fleet-wide crash loops during legitimate idle states | — Pending (verify via HEALTHZ-01 regression test) |
| `call_download_failed` emitted at the caller (collector loop), not inside `_download_*` helpers | Caller already has `feed_id` + `source_type` in scope; threading them through the pure download utilities would muddy their signature. Downside: bcfy_calls's `_create_chunk_from_call` returns None for ~2 reasons (download failure, empty bytes) — both are counted as `call_download_failed`. Acceptable per SLO semantics ("download didn't produce usable audio"). | — Pending |
| `active_feed_count` lives in a new `backend/pipeline/ingestion/metric_reporter.py` module, not as an extension of `MonitoringClient` | `MonitoringClient` is for per-event emits on the `global` resource with feed_id labels (quarantine). Active-feed-count is periodic, `gce_instance` resource, no feed_id. Separate modules keep each class's contract clear. | — Pending |
| Inject the Cloud Monitoring client and shutdown-sleep as constructor args on the reporter | Matches the prompt's explicit test requirement ("accept the monitoring client as a constructor argument so it can be stubbed in tests") and lets unit tests exercise the tick loop without real GCP or the full runtime. | — Pending |
| `bcfy_calls`'s receipt stamp is per-call iteration, not per-fetch | Prompt's explicit spec ("stamp at each call's iteration point, not at the listing fetch"). This gives per-call E2E latency rather than per-batch. | — Pending |

## Evolution

This document evolves at phase transitions and milestone boundaries.

**After each phase transition** (via `/gsd-transition`):
1. Requirements invalidated? → Move to Out of Scope with reason
2. Requirements validated? → Move to Validated with phase reference
3. New requirements emerged? → Add to Active
4. Decisions to log? → Add to Key Decisions
5. "What This Is" still accurate? → Update if drifted

**After each milestone** (via `/gsd-complete-milestone`):
1. Full review of all sections
2. Core Value check — still the right priority?
3. Audit Out of Scope — reasons still valid?
4. Update Context with current state

---
*Last updated: 2026-04-21 after initialization*
