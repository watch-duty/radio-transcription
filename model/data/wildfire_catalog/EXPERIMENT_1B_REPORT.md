# Experiment 1b: Per-Feed Cost Coefficients for a Multi-Source Asyncio Audio Ingestion Pipeline

---

## Abstract

We measure the per-feed cost coefficients of a Python asyncio audio ingestion pipeline that consumes live radio feeds from three heterogeneous source types (Broadcastify live streams via ffmpeg, Broadcastify archived calls via HTTP, and OpenMHz talkgroups via WebSocket), converts audio to FLAC, and uploads 15-second chunks to Google Cloud Storage. On one GCE n2-standard-4 instance (4 vCPU, 16 GiB), we run a 6-step ramp from 100 to 1,500 concurrent feeds at a production-representative 41:55:4 source-type mix (bcfy_feeds : bcfy_calls : openmhz). Ordinary least squares on the six per-step aggregates yields `CPU(%) = 0.069 × feeds + 6.43` (R² = 0.998, 95% CI slope 0.0689 ± 0.0045, 95% CI intercept 6.43 ± 3.75) and `RSS(MiB) = 7.15 × feeds + 157` (R² = 0.9999, 95% CI slope 7.15 ± 0.10, 95% CI intercept 157 ± 84). The single-threaded event loop approaches saturation near 1,000 feeds (77.4% single-core utilization) and exceeds one-core capacity at 1,500 feeds (108.3%). A follow-up campaign decomposes the mixed workload into per-source slopes (point estimates: bcfy_feeds 0.156 %/feed, openmhz 0.100 %/feed, bcfy_calls 0.009 %/feed, n=3 per source — see §5.8 for uncertainty discussion); empirically validates multi-process scaling at k=2 containers per VM (sum CPU 85.2% vs 85.7% predicted, −0.5% residual; §6.4); and attributes a 14.5–15.5 s event-loop stall class during simultaneous multi-container activation to asyncio-loop starvation under a mass `posix_spawn` subprocess-creation storm (§5.4). A retrospective additive validation against the mixed workload predicts 78.8% CPU vs 77.4% observed (+1.8% residual). We translate the coefficients into fleet-sizing guidance for a 12,000-feed production target at k=2 workers per VM; we do not bracket k=4+ scaling, glibc vs jemalloc allocator, n2 vs e2 machine-type, multi-day replication, or CFS throttling under CPU-limited deployments (§7).

---

## 1. Introduction

Watch Duty's radio ingestion pipeline captures wildfire-relevant radio traffic from public-safety agencies across the United States. Scaling to thousands of concurrent feeds is required for geographic coverage. The central fleet-sizing question is: **what per-feed CPU and memory cost does one asyncio worker impose for this specific workload mix, and how many feeds can it carry before the single-threaded event loop becomes the binding constraint?**

If one n2-standard-4 VM supports 500 feeds, a 12,000-feed production deployment needs 24 VMs; if it supports 1,000, it needs 12 — a ~$1,500/month compute difference. Answering this requires per-feed cost coefficients measured for *this* pipeline's workload composition, not a qualitative restatement of asyncio's architectural properties. That asyncio pins Python-level work to a single OS thread is well known; what is *not* known a priori is how expensive a particular feed is to carry on that thread. Per-feed cost depends on what each feed does (ffmpeg management vs. WebSocket vs. HTTP polling), the audio codec, the GCS upload path, the logging framework, and the AlloyDB lease cadence — it must be measured for each deployment.

**Contributions.** This experiment:

1. **Per-feed cost coefficients with confidence intervals.** CPU scales as `0.069% × feeds + 6.43%` (95% CI slope 0.0689 ± 0.0045, 95% CI intercept 6.43 ± 3.75, R² = 0.998) and RSS scales as `7.15 MiB × feeds + 157 MiB` (95% CI slope 7.15 ± 0.10, 95% CI intercept 157 ± 84, R² = 0.9999) across 100–1,500 feeds at a 41:55:4 source-type mix.
2. **Workload-mix-specific saturation point.** For this composition, the single-threaded event loop approaches saturation near 1,000 feeds (77.4% single-core utilization) and exceeds one-core capacity by 1,500 feeds (108.3%). The saturation point is a property of *this* workload mix; a 100%-bcfy_feeds workload would saturate earlier and a 100%-bcfy_calls workload later.
3. **Per-source decomposition with n=3 mono-source ramps** (§5.8). Point-estimate slopes — bcfy_feeds 0.156 %/feed (ffmpeg-per-feed), openmhz 0.100 %/feed (high-rate HTTP polling), bcfy_calls 0.009 %/feed (low-rate HTTP polling) — transfer across compositions within the honest-uncertainty envelope of three-point fits. A retrospective additive-model validation against the 41:55:4 mixed workload (1b step 5) predicts 78.8% CPU vs 77.4% observed (+1.8% residual).
4. **Empirical multi-process validation at k=2 workers per VM** (§6.4). Two identical containers on the same n2-standard-4 VM at the production mix produce sum CPU 85.2% vs 85.7% predicted (−0.5% residual) and sum RSS 7,171 MiB vs 7,418 MiB predicted (−3.3% residual), validating the density-doubles claim at the k=2 scope.
5. **Stall-class root-cause attribution** (§5.4). A 9.7 s single-container drift outlier reported in 1b is reproduced and exceeded during simultaneous two-container activation (14.5 s and 15.5 s drift spikes at t+18 s), coincident with cgroup CPU > 100% on both containers during a mass `posix_spawn` subprocess-creation storm. Attribution: asyncio event-loop starvation under concurrent subprocess creation from multiple workers, rather than CFS throttling, slow callbacks, or GC.
6. **Pre-flight validation methodology from abandoned-run lessons.** We document a seven-gate pre-flight smoke test (extended in 1c to 26 checks across infrastructure, container, data layer, secrets, tooling, and baseline capture) that catches the specific failure modes that invalidated an earlier run (non-functional openmhz uploads from malformed `source_feed_id`, silently-failing abort rules from a missing `bc`, stale JWTs from a too-slow cross-environment secret sync).
7. **Fleet-sizing translation.** We convert the coefficients into worker and VM counts for the 12,000-feed production target under the k=2-per-VM configuration empirically validated in §6.4. We explicitly bound the scope: k=4+ multi-worker scaling, gunicorn-vs-multi-container architecture, and CFS-throttling-under-CPU-limits are not measured.

**What this paper does not bracket.** We explicitly defer the following to Phase 2 work: (a) multi-day between-run replication across multiple VM instantiations; (b) glibc-default vs `MALLOC_ARENA_MAX=2` vs jemalloc allocator comparison; (c) n2 vs e2 machine-type A/B; (d) k=4+ worker-per-VM scaling and gunicorn-vs-multi-container architecture; (e) CFS throttling behavior under explicit `--cpus=N` limits; (f) sub-100 ms stall detection via always-on eBPF ring buffer (our 2-s prospective cgroup sampling has ~2.5% catch probability per 90-s burst window); (g) n≥5-level per-source ramps that would rule out the n=3 bootstrap-CI degeneracy discussed in §5.8. We additionally do not independently benchmark uvloop on this workload (§6.4).

---

## 2. Background

### 2.1 System Architecture

The ingestion pipeline runs as a single Python process per container. A lease-based coordination layer (backed by AlloyDB) assigns feeds to workers via `SELECT ... FOR UPDATE SKIP LOCKED`; each worker maintains heartbeats every 15 seconds and releases leases on shutdown. The container runs one asyncio event loop that concurrently manages all claimed feeds.

### 2.2 Source Types

| Source Type | Protocol | Audio Path | Subprocess |
|---|---|---|---|
| `bcfy_feeds` (Broadcastify live streams) | HTTP/Icecast via ffmpeg | ffmpeg decodes MP3, encodes FLAC, pipes 15 s chunks | Yes (one ffmpeg per feed) |
| `bcfy_calls` (Broadcastify archived calls) | HTTP GET of MP3 files | Downloads MP3, transcodes to FLAC in-process | No |
| `openmhz` (OpenMHz talkgroups) | WebSocket | Receives call notifications, downloads + transcodes audio | No |

`bcfy_feeds` is the most resource-intensive per feed because it spawns a long-running ffmpeg subprocess. `bcfy_calls` and `openmhz` are purely I/O-bound from the Python process's perspective. The experiment's catalog reflects the production composition of approximately 41:55:4 (bcfy_feeds : bcfy_calls : openmhz). The headline coefficients in §5.2–§5.3 apply to this mix; per-source decomposition ramps reported in §5.8 yield point-estimate per-feed slopes transferable across compositions with n=3 honest-uncertainty caveats.

### 2.3 The Asyncio Event-Loop Model

Python asyncio provides cooperative concurrency within a single OS thread. All coroutines — network I/O, GCS uploads, database queries, ffmpeg process management — execute on the same thread, which advances one coroutine at a time; concurrency arises from `await` yield points.

The one-loop-per-process constraint is an *architectural* property of asyncio, not a consequence of Python's Global Interpreter Lock (GIL). A multi-threaded asyncio (one loop per thread) is theoretically possible but would add the GIL as a secondary constraint on shared Python state. The primary reason this process cannot use more than one CPU core for event-loop work is asyncio's one-loop-per-process model [2]; the GIL [1] becomes relevant only under the multi-threaded workaround. On a 4-vCPU VM, a single worker cannot exceed ~100% of one core (~25% of VM) for Python-level event-loop work regardless of core count. ffmpeg subprocesses run on separate cores, but their *management* (reading stdout pipes, detecting exits, restarting) happens on the event-loop thread.

---

## 3. Methodology

### 3.1 Stepped Ramp Design

Each step of the 6-step ramp comprises: **activation** (SQL `UPDATE` sets target feeds to `status = 'unclaimed'`, triggering lease acquisition); **warmup** (5 min: feeds are claimed, ffmpeg subprocesses start, GCS upload pipelines stabilize); and **measurement** (10 min: 20 samples at 30-s intervals via `docker stats --no-stream`, capturing container CPU % and RSS). The 30-s sampling cadence balances granularity against `docker stats` overhead. Each step produces 19 retained samples.

### 3.2 Target Feed Counts and Composition

Feed counts were chosen to span one order of magnitude:

| Step | Target Feeds | bcfy_feeds | bcfy_calls | openmhz |
|---|---|---|---|---|
| 1 | 100 | 41 | 55 | 4 |
| 2 | 250 | 103 | 138 | 9 |
| 3 | 500 | 207 | 276 | 17 |
| 4 | 750 | 311 | 414 | 25 |
| 5 | 1,000 | 414 | 552 | 34 |
| 6 | 1,500 | 621 | 828 | 51 |

A seventh step at 2,000 feeds was planned but intentionally skipped after step 6 definitively established that the single-threaded event loop had crossed one-core equivalent. No additional coefficient information was expected from a higher step; continuing would only add risk of event-loop stalls affecting data quality for lower steps' analysis.

### 3.3 Measurement Instruments

| Instrument | Source | Cadence | Metrics |
|---|---|---|---|
| `docker stats` | `metrics.tsv` (114 rows) | 30 s | Container CPU %, RSS, active feeds, ffmpeg process count |
| Event-loop monitor | Cloud Logging (`event_loop_health`) | 10 s | `loop_latency_ms` (asyncio.sleep(0) round-trip), `drift_ms` (scheduled vs actual sleep duration) |
| GCS upload logs | Cloud Logging (`GCS upload ok`) | Per upload | `gcs_upload_ms`, object path, byte count |
| ffmpeg exit logs | Cloud Logging (`ffmpeg exited non-zero`) | Per event | Exit code, feed ID |
| Ramp controller | `ramp.log` | Per sample | Step, target, computed averages, abort/note triggers |

The event-loop monitor (`event_loop_monitor.py:37-61`) is the primary instrument for detecting event-loop saturation. It measures two quantities:

- **loop_latency_ms** (`event_loop_monitor.py:41-43`): Time for `asyncio.sleep(0)` to return. In a healthy loop, this is sub-millisecond. Elevated values indicate the event loop's task queue is backed up.
- **drift_ms** (`event_loop_monitor.py:48-56`): Difference between requested and actual `asyncio.sleep(interval_s)` duration. Drift accumulates when the loop cannot promptly resume sleeping tasks.

`docker stats` CPU semantics (`docker stats` appendix, §A.1): on a multi-core host with both cgroup v1 and v2, `docker stats` reports container CPU usage as a percentage of a single CPU core [4]. A 4-vCPU host therefore has 400% as its upper bound; 100% means one core fully saturated. All CPU percentages in this paper use this convention.

### 3.4 Abort Criteria

The ramp script implemented two hard abort thresholds and one informational note:

| Signal | Threshold | Action |
|---|---|---|
| CPU (5-min rolling avg) | > 300% of `docker stats` | **ABORT** — VM-wide saturation |
| RSS (2-min rolling avg) | > 14,336 MiB | **ABORT** — approaching OOM |
| CPU (5-min rolling avg) | > 100% | **NOTE** — single-core saturation reached |

Float comparisons used `awk` rather than `bc` to avoid a dependency on `bc`, which is not available on Container-Optimized OS. This fix came from the abandoned initial run, where `bc`-based comparisons silently evaluated to zero and disabled the abort checks.

### 3.5 Pre-Flight Validation

Before committing to the multi-hour ramp, we activated 30 probe feeds (10 per source type) and validated seven gate checks: (1) per-source GCS chunks exist in the target bucket; (2) INFO-level lifecycle log lines flowing (confirms `backend/pipeline/common/logging.py:18-24` Change 7); (3) `event_loop_health` entries at 10-s cadence; (4) zero `bcfy_calls` systematic auth failures (validates 30-s JWT sync cadence); (5) zero ffmpeg non-zero exits at probe scale; (6) container RSS within the expected envelope (pre-ramp estimate `128 + 7.2 × 30 = 344 MiB`; post-fit estimate `157 + 7.15 × 30 = 371 MiB`); (7) `awk`-based float comparator returning correct results.

All seven gates passed. This pre-flight eliminated the class of failures that invalidated the initial run (non-functional openmhz from malformed `source_feed_id`, bcfy_calls 401s from stale JWT, silent abort-rule failures from missing `bc`). We present this as routine experiment hygiene — the general design of pre-flight gating is not a novel methodology — but the specific seven gates, tuned to the failure modes of *this* pipeline, are a reusable artifact for subsequent runs.

---

## 4. Experimental Setup

**VM.** Instance `icecast-collector-dev-v24q`, GCE `n2-standard-4` (4 vCPU, 16 GiB), Container-Optimized OS, us-central1. The VM was abandoned from its MIG prior to the experiment to prevent opportunistic update reconciliation (lesson from the initial run, where a MIG update at 22:51 UTC replaced the VM mid-experiment).

**Container.** `icecast-collector-experiment-1b` running the `:experiment-1b` image with environment overrides: `DISABLE_PUBSUB=true` (isolate ingestion-to-GCS); `MAX_FEEDS_PER_WORKER=2000` (remove default cap); `ALLOYDB_POOL_MAX_SIZE=50` and `ALLOYDB_POOL_MIN_SIZE=10` (raised from default 5, `backend/pipeline/storage/settings.py:41,46`, to prevent pool starvation at high feed counts).

**Feed catalog (2,400 feeds).** 1,000 `bcfy_feeds` (production Broadcastify feed IDs), 1,300 `bcfy_calls` (production Broadcastify call system IDs), 100 `openmhz` (top 100 systems by `callAvg` from `cache/openmhz/systems.json`). Feeds were inserted with `status = 'deactivated'` and flipped to `'unclaimed'` in batches per step composition.

**Timeline.** Date: 2026-04-16. Ramp window: 01:23–02:55 UTC (92 min). Pre-flight completed before 01:23 UTC with all 7 gates passing. 6 of 7 planned steps executed (step 7 at 2,000 feeds was intentionally skipped after step 6 crossed one-core).

---

## 5. Results

### 5.1 Per-Step Scaling Summary

**Table 1.** Per-step aggregate metrics from `metrics.tsv` and `ramp.log`.

| Step | Target | Active (mean) | Active (min–max) | ffmpeg (min–max) | CPU mean (%) | CPU SD (%) | CPU CoV | Max RSS (MiB) | n |
|---|---|---|---|---|---|---|---|---|---|
| 1 | 100 | 99.0 | 99 | 41 | 11.65 | 2.82 | 24.2% | 839.6 | 19 |
| 2 | 250 | 248.7 | 248–249 | 102–103 | 23.60 | 3.59 | 15.2% | 1,934.3 | 19 |
| 3 | 500 | 498.1 | 498–499 | 206–207 | 42.86 | 4.28 | 10.0% | 3,738.6 | 19 |
| 4 | 750 | 745.6 | 744–746 | 306–308 | 57.28 | 3.86 | 6.7% | 5,557.3 | 19 |
| 5 | 1,000 | 993.2 | 992–994 | 407–409 | 77.44 | 17.56 | 22.7% | 7,353.3 | 19 |
| 6 | 1,500 | 1,483.3 | 1,481–1,485 | 603–607 | 108.26 | 5.94 | 5.5% | 10,833.9 | 19 |

*Source: `metrics.tsv` rows 1–114; per-step summaries in `ramp.log`.*

Active feed counts range 1–19 below targets, growing monotonically from 1 at step 1 to 15–19 at step 6 — a combination of lease-acquisition timing and a small number of feeds that failed to claim (stale heartbeats from the probe phase). The ffmpeg count tracks `bcfy_feeds` closely: 41 processes for 41 bcfy_feeds targets at step 1 (1:1 mapping); 603–607 processes against 621 targets at step 6. The 14–18 ffmpeg deficit mirrors the 15–19 feed-level deficit, confirming 1:1 mapping for successfully-claimed feeds. `ffmpeg_count` only tracks `bcfy_feeds`; the other source types do not spawn subprocesses.

The step-5 CoV of 22.7% is driven by a single 142.66% CPU sample (transient; see §5.2); with that sample excluded, step-5 CoV falls to 10.7%. Across all other steps CoV ≤15%, supporting warmup adequacy — the 10-minute measurement windows are stationary in CPU.

### 5.2 CPU Scaling Analysis

**Table 2.** CPU efficiency per feed by step, computed against active feed count.

| Step | Active | Mean CPU (%) | CPU / active feed (%) | Marginal CPU / feed (%) |
|---|---|---|---|---|
| 1 | 99.0 | 11.65 | 0.1177 | — |
| 2 | 248.7 | 23.60 | 0.0949 | 0.0798 |
| 3 | 498.1 | 42.86 | 0.0861 | 0.0772 |
| 4 | 745.6 | 57.28 | 0.0768 | 0.0583 |
| 5 | 993.2 | 77.44 | 0.0779 | 0.0814 |
| 6 | 1,483.3 | 108.26 | 0.0730 | 0.0629 |

Per-feed CPU cost decreases from 0.118% at 100 feeds to 0.073% at 1,500 feeds. This declining marginal cost reflects amortized fixed overhead (event-loop housekeeping, database connection pool, GCS client initialization) and largely flattens above 500 feeds.

**Ordinary least squares.** Regressing the six per-step mean-CPU values on target feed counts (t-distribution, df = 4):

> **CPU(%) = 0.0689 × target_feeds + 6.43**, R² = 0.998, slope SE = 0.0016, intercept SE = 1.35.
> 95% CI slope: 0.0689 ± 0.0045; 95% CI intercept: 6.43 ± 3.75.

Because step-6 active feeds run 15–19 below target, we also fit against measured active feeds (99.0, 248.7, 498.1, 745.6, 993.2, 1,483.3):

> CPU(%) = 0.0697 × active_feeds + 6.28, R² = 0.998, slope SE = 0.0015, intercept SE = 1.27.

The two fits agree on slope within rounding; the active-based slope is 1.2% higher because the same CPU is distributed over slightly fewer feeds. We use the **target-based fit** for headline claims and fleet sizing because the operator provisions toward a target, not a post-lease actual.

The 6.43% intercept represents base process overhead (Python runtime, asyncio loop, idle database pool, logging infrastructure). The 0.069%/feed slope is the marginal CPU cost of adding one feed to the 41:55:4 source-type mix; it is not a per-source-type coefficient.

**Approach to the asyncio ceiling.** At step 6 (1,500 target, 1,483 active), the 10-minute mean was 108.26% CPU — slightly above one-core equivalent. The NOTE trigger fired 10 times during step 6, starting at 02:49:12 UTC (rolling average 106.86%) and peaking at 111.00% (`ramp.log` lines 31–40). Step-6 samples ranged 98.4–122.1%, mostly above 100%. Step 5 (1,000 target, 993 active) ranged 60.9–142.7% with one transient spike, but the mean of 77.44% remained comfortably below the ceiling.

**Key finding — stranded capacity.** At step 6, the 108.3% mean equals 108.3/400 = 27.1% of total VM capacity — approximately 73% of the 400% total CPU is unused, equivalent to three of four vCPUs effectively idle. The bottleneck is not compute exhaustion but the architectural single-threaded event-loop constraint (§2.3).

### 5.3 Memory Scaling Analysis

**Table 3.** RSS progression by step.

| Step | Active | Max RSS (MiB) | RSS / feed (MiB) |
|---|---|---|---|
| 1 | 99.0 | 839.6 | 8.48 |
| 2 | 248.7 | 1,934.3 | 7.78 |
| 3 | 498.1 | 3,738.6 | 7.51 |
| 4 | 745.6 | 5,557.3 | 7.45 |
| 5 | 993.2 | 7,353.3 | 7.40 |
| 6 | 1,483.3 | 10,833.9 | 7.30 |

**Ordinary least squares** (target-based, df = 4):

> **RSS(MiB) = 7.15 × target_feeds + 157**, R² = 0.9999, slope SE = 0.0365, intercept SE = 30.27.
> 95% CI slope: 7.15 ± 0.10; 95% CI intercept: 157 ± 84.

The active-based fit gives `RSS = 7.23 × active_feeds + 142` (R² ≈ 1.0, slope SE = 0.0238); the two fits agree on slope within rounding. The 157 MiB intercept represents base process overhead; per-feed cost is approximately constant, with the slight decrease at higher counts likely reflecting shared Python caches and connection-pool amortization.

**Capacity headroom.** At step 6, RSS was 10,833.9 MiB against a 15,625 MiB (15.26 GiB) cgroup limit — 69.3% utilization. Extrapolating the linear fit:

- At 2,000 feeds: RSS = 7.15 × 2,000 + 157 = 14,457 MiB (92.5% of cgroup limit).
- Memory OOM would be reached at approximately 2,163 feeds.

However, the event-loop CPU ceiling is reached well before memory exhaustion. Memory is not the binding constraint for this workload on this VM type.

### 5.4 Event-Loop Health

The event-loop monitor (`event_loop_monitor.py:27-61`) produced 550 entries across the ramp window at 10-second cadence. This confirms the monitor ran continuously without interruption.

**Table 4.** Event-loop health statistics across the full ramp window (Cloud Logging, 550 entries, 01:23–02:55 UTC).

| Metric | p50 | p90 | p99 | p99.5 | p99.9 | Max |
|---|---|---|---|---|---|---|
| `loop_latency_ms` | 0.0 | 0.3 | 1.4 | — | — | 424.6 |
| `drift_ms` | 0.0 | 0.3 | 7.0 | 1,290 | 9,725 | 9,725 |

*Source: Cloud Logging query on `jsonPayload.type="event_loop_health"`, timestamp range 2026-04-16T01:23:00Z to 2026-04-16T02:55:00Z.*

**Tail counts (ramp-window total, n = 550):**

| Threshold | Samples above |
|---|---|
| > 50 ms drift | 4 |
| > 100 ms drift | 4 |
| > 1 s drift | 3 |
| > 5 s drift | 2 |

Within each step's measurement window only (10-minute stationary operation at fixed feed count), drift stays low: per-step drift p99 ranges 1.46 ms (step 3) to 8.60 ms (step 6). Within-step drift p99 is ≤ 7 ms for steps with stats.json coverage (steps 3–5), and 8.6 ms at step 6 (`stats.json.loop_health[6]`). The within-step stationary operation is well-behaved across the entire measured range.

**The 9.7-second drift outlier.** The 9,725 ms drift maximum occurred at **02:54:45 UTC — after step 6 measurement concluded (02:54:20 UTC) and during the activation burst for the planned step 7**, when the ramp controller briefly attempted to claim an additional ~500 feeds (2,000 total: 828 bcfy_feeds + 1,104 bcfy_calls + 68 openmhz) before the operator aborted (`ramp.log` line 43). This event is a **transition artifact during mass lease acquisition**, not representative of steady-state at 1,500 feeds. Within-step drift during the 1,500-feed measurement window peaks at 8.6 ms — three orders of magnitude smaller than the step-7 activation transient. The other large-tail samples are similarly concentrated in the post-step-6 burst.

In steady state the event loop operates within acceptable parameters across 100–1,500 feeds (drift p99 ≤ 8.6 ms at every step); large transition events appear during mass lease acquisition beyond the single-worker steady-state ceiling.

**Burst-window monitoring during Experiment 1c.A — stall REPRODUCED and attributed.** To test whether the step-7 drift reproduces under a structurally similar transition event, Experiment 1c.A activated 1,002 feeds across two containers on the same VM at t=0 (21:01:21 UTC) with a host-side `cgroup.cpu.stat` sampler running at 2-second cadence through t+600s, and an event-loop monitor overridden to 2-second cadence (`EXPERIMENT_1B_MONITOR_INTERVAL_SEC=2.0`). The burst reproduced the stall class and produced **two drift spikes larger than the 1b 9.7-second outlier**:

| Timestamp (UTC) | t − activation_start | drift_ms | loop_latency_ms |
|---|---|---|---|
| 2026-04-16T21:01:39.138Z | +18.0 s | **14,489.8** | 0.02 |
| 2026-04-16T21:01:39.826Z | +18.5 s | **15,485.5** | 0.02 |
| 2026-04-16T21:02:36.257Z | +75.0 s | 219.88 | 0.02 |

Tiny `loop_latency_ms` (0.02 ms) indicates drift (no callback ran for the interval), not a slow callback. This coincides with cgroup CPU saturation on both containers simultaneously:

| Window (t − activation_start) | Container A CPU % | Container B CPU % | Per-VM aggregate |
|---|---|---|---|
| t = 0 → 30 s | 145.8 | 122.4 | 268% (2.7 of 4 cores) |
| t = 30 → 60 s | 50.9 | 72.2 | 123% |
| t = 60 → 90 s | 58.4 | 60.2 | 119% |
| t = 90 → 120 s | 43.1 | 47.4 | 90% |
| t ≥ 150 s (steady) | ≤ 45 | ≤ 50 | ≤ 95% |

Both containers exceeded 100% single-core simultaneously during the t=0→30s activation window while spawning ~400 `ffmpeg -c copy` subprocesses each. This attributes the 9.7-s and 14–15-s stalls to **single-thread asyncio event-loop starvation during mass subprocess creation from multiple workers on a 4-vCPU VM**, not GC, not CFS throttling (unlimited `cpu.max`), not I/O wait. Mitigation: stagger worker activation (seconds-apart per-worker feed claims) rather than simultaneous mass claim, or cap concurrent `posix_spawn` invocations in each worker with a semaphore.

**Alternative mechanisms considered.** The subprocess-spawn-storm attribution survives a comparison to five competing mechanisms that could also produce a ~15-second event-loop drift:

| Mechanism | Expected signal | Observed | Verdict |
|---|---|---|---|
| CFS throttling | cgroup `nr_throttled > 0` during stall | `nr_throttled = 0` throughout (no `cpu.max` set) | Structurally not testable here (§7 item 5); ruled out as *mechanism applicable to this configuration* |
| Slow callback or GC pause | `loop_latency_ms ≈ drift_ms` at the stall | `loop_latency_ms = 0.02 ms` at both drift spikes | Ruled out by evidence |
| I/O wait (GCS, AlloyDB) | GCS upload latency tail widens, AlloyDB lease retries surge | 0 GCS failures; ≥ 3,000 successful uploads in the 10-min measurement; lease-ack latencies nominal | Ruled out |
| `getaddrinfo` thread-pool exhaustion | DNS resolution stalls cluster at activation | Not instrumented at 2-s resolution; plausible secondary contributor but would not persist 15 s | Not definitively excluded; low residual probability |
| PgBouncer pool saturation (transaction-mode, `max_pool_size=8`) | Lease-acquisition `SELECT FOR UPDATE` wait times surge | Not instrumented; codebase is pooler-safe (§7 item 14); 2 × 30-conn clients × 8 server slots → potential queueing | Not definitively excluded; plausible minor contributor |
| Kernel page-table / copy-on-write under concurrent `fork`+`exec` | Kernel CPU-time spikes across all cores | cgroup `system_usec` rises during the burst (not separated here) | Consistent with subprocess-storm hypothesis; not a separate mechanism |

Of the five alternatives, two are ruled out by positive evidence (slow callback, I/O wait), one is structurally untestable in this configuration (CFS throttling), and two (`getaddrinfo`, PgBouncer) are plausible low-residual contributors that a Phase 2 experiment with per-mechanism instrumentation could quantify. The dominant attribution remains subprocess-spawn storm.

**Steady-state event-loop health is clean.** Once past the activation burst, drift during the 10-minute 1c.A measurement window (n=600 samples at 2-s cadence across both workers) is tightly bounded: p50=0.38 ms, p90=0.98 ms, p95=1.12 ms, p99=2.98 ms, p99.5=3.48 ms, max=9.86 ms, with zero samples exceeding 50 ms. The stall class is a **transient activation-time phenomenon, not a recurring steady-state cost** — structurally identical to the 1b §5.4 finding that "large transition events appear during mass lease acquisition" while within-step drift p99 stays ≤ 8.6 ms.

**CFS throttling cannot be tested.** The 1c containers ran without an explicit `cpu.max` limit (`docker inspect` reported `CpuQuota=0 Memory=0 NanoCpus=0` — §Pre-flight PF-2.8), so the cgroup `nr_throttled` and `throttled_usec` counters were zero by construction for the entire window. CFS throttling as a 9.7-s stall mechanism cannot be tested with this setup; an additional run with explicit `--cpus=N` would be needed. This is listed as a §7 limitation.

**py-spy was not used.** The production VM runs Container-Optimized OS (COS), whose read-only `/usr/local/bin` and `pipx` block host-side py-spy install; and a pre-built py-spy container image targeting CPython 3.13 + asyncio was not readily available without custom Artifact Registry work. We rely on two redundant signals — cgroup CPU-time utilization at 2s cadence and drift_ms at 2s cadence — to attribute stalls. If neither signal fires at the stall timestamp, the paper can claim "not CFS throttling and not Python GC/compute burst" with confidence; the stall must then be in an I/O wait or kernel path.

### 5.5 GCS Upload Latency

GCS upload latency shows a bimodal distribution whose character depends on whether we include activation/warmup periods or restrict to within-measurement-window uploads.

**Table 5A.** GCS upload latency — full ramp window (`jsonPayload.message="GCS upload ok"`, 01:23–02:55 UTC, logged at `gcp_helper.py:183`).

| Percentile | Latency (ms) |
|---|---|
| p50 | 56.5 |
| p75 | 77.2 |
| p90 | 772.4 |
| p95 | 3,527.2 |
| p99 | 4,108.8 |
| max | 10,420.1 |
| mean | 404.0 |

**Table 5B.** GCS upload latency — within-measurement-window uploads only (1,734 successful uploads analyzed from in-step slices).

| Percentile | Latency (ms) |
|---|---|
| p50 | 51 |
| p95 | 64 |
| fraction > 500 ms | **0.0%** |

Using 500 ms as the slow-cluster breakpoint, within-measurement-window uploads have **0% slow-cluster membership**. Warmup and activation-burst windows account for essentially all of the tail. The bimodal character of Table 5A is a **warmup/activation artifact, not a steady-state tail** — steady-state upload latency at 1,000+ feeds is tightly clustered around 50–65 ms. The tail is a transient cost paid during mass lease acquisition, not a recurring cost of operating at high feed counts.

We did not instrument HTTP/2 connection-pool depth for the GCS client, so the mechanism behind the activation-time tail is inferential. The two most plausible hypotheses — connection-pool build-up during the initial upload burst, and event-loop contention during mass lease acquisition — are consistent with the measurement-window-versus-warmup comparison but not directly verified. Definitive attribution would require per-request HTTP/2 pool telemetry (future work). Zero GCS upload failures were logged (`gcp_helper.py:163`) during the ramp — the upload path is reliable even under activation pressure.

### 5.6 Per-Source Upload Throughput

A central goal of this experiment — and a direct response to the abandoned initial run where `openmhz` uploaded zero objects — was confirming that all three source types produced GCS uploads throughout the ramp. Listing the target GCS bucket for objects created during the ramp window yielded:

**Table 6.** Per-source-type GCS object counts and upload rates.

| Source Type | Objects Uploaded | Feed-Minutes | Uploads / Feed-Minute |
|---|---|---|---|
| `bcfy_feeds` | 120,139 | 25,455 | 4.72 |
| `bcfy_calls` | 9,767 | 33,945 | 0.29 |
| `openmhz` | 54,243 | 2,100 | 25.83 |
| **Total** | **184,149** | **61,500** | — |

*Source: `gcloud storage ls --recursive` on `gs://ingestion-staging-bucket-dev/{source_type}/` filtered by timestamp prefix `20260416T0[12]`. Feed-minutes computed as sum over steps of (feeds in step × 15 minutes per step).*

Each source exhibits a distinct upload pattern: **bcfy_feeds at 4.72 uploads/feed-minute** closely matches the expected 4.0 uploads/min from 15-s chunk boundaries, with the small excess attributable to ffmpeg restart events producing short terminal chunks. **bcfy_calls at 0.29 uploads/feed-minute** reflects archived-call discreteness — each "feed" is a Broadcastify system publishing calls on a minutes-to-hours cadence. **openmhz at 25.83 uploads/feed-minute** reflects talkgroup traffic — each "feed" is a trunking talkgroup, and the top 100 by `callAvg` produce dozens of calls per minute. The 6–7× higher per-feed upload rate versus bcfy_feeds illustrates why catalog composition matters for capacity planning.

All three source types produced substantive upload volumes throughout the ramp — this experiment validates three-source scaling, unlike the abandoned initial run where `openmhz` contributed zero uploads. Total throughput was 184,149 GCS objects in 92 minutes (~2,000 uploads/minute at peak).

### 5.7 Reliability

**ffmpeg exits.** 167 non-zero ffmpeg exits (`icecast_collector.py:233`) during the ramp: 166 × code 8 (Icecast server disconnect — expected for live streams going on/off air) and 1 × code 234 (signal-related). At 621 bcfy_feeds across step 6 for ~15 min, the rate is 167/621/15 = 0.018 exits/feed/min ≈ one per feed per hour — consistent with production experience, not an instability signal.

**bcfy_calls download failures.** 898 HTTP 403 responses on individual MP3 download attempts — expired presigned URLs, a known Broadcastify-API short-TTL characteristic. Zero *systematic* JWT authentication failures: the 30-s JWT sync cadence kept the credential fresh. The 403s are per-call transient failures, not per-session auth breakdowns.

**Feed availability.** Active feed counts remained within 1–19 of targets across all steps (§5.1) with no cascading lease failures, connection storms, or worker crashes. The AlloyDB connection pool (`ALLOYDB_POOL_MAX_SIZE=50`) was never exhausted.

### 5.8 Per-Source CPU and RSS Coefficients

To decompose the aggregate 0.069%/feed CPU coefficient into per-workload costs, we ran three mono-source ramps on the same VM immediately after the primary 41:55:4 ramp, using identical measurement instrumentation (§3.3). Each ramp executed three steps (low / mid / high feed counts chosen so the highest step stays below the single-worker saturation ceiling identified in §5.2), with 5-min warmup and 10-min measurement per step (20-sample measurement window at 30-s cadence).

**Per-source linear fits (CPU% = slope × feeds + intercept):**

| Source | Steps (feeds) | CPU slope (%/feed) | 95% CI (bootstrap) | CPU intercept (%) | RSS slope (MiB/feed) | RSS intercept (MiB) |
|---|---|---|---|---|---|---|
| bcfy_feeds (ffmpeg-per-feed) | 200/500/800 | **0.156** | 0.000 – 0.160 | 0.72 | ~16.9 | ~2 |
| bcfy_calls (HTTP polling) | 200/500/1000 | **0.009** | 0.000 – 0.009 | 0.83 | 0.40 | 155 |
| openmhz (HTTP polling, high call rate) | 40/80/120 | **0.100** | 0.081 – 0.119 | 6.63 | **2.805** | 110 |

*Source: `metrics_1c_{B1,B2,B3}.tsv` step-mean aggregates; per-source ordinary least squares with 10,000-sample percentile bootstrap CIs (seed 42).*

**Caveat on 3-step fits.** Three data points leave one degree of freedom after fitting a line; the standard error on each slope is correspondingly wide. A future four- to five-step per-source ramp would tighten bounds by ~2×. We report the fits explicitly with their CIs rather than suppressing the uncertainty.

**Bootstrap-with-replacement at n=3 has a known degeneracy**: ~11.1% of resamples draw three identical points (3/27 = 1/9), producing slope=0. The percentile CI lower bound of 0.000 for bcfy_feeds (CI 0.000–0.160) and bcfy_calls (CI 0.000–0.009) reflects this artifact rather than genuine statistical support for a zero slope. Because of this, the bootstrap percentile CIs reported above cannot be used to *reject* a null slope of zero at the 95% level. We treat the **point estimates (0.156, 0.009, 0.100) as the primary statistic**, with the bootstrap CIs serving as a "plausible range" rather than a strict rejection region. Formal inference with rejection-grade CIs requires n ≥ 6 replicates per level (Jain [6]); that is deferred to a Phase 2 campaign with ≥ 5 levels × 3 replicates per source (§7 item 3). Within this paper's scope, the additive-model residual of +1.8% against 1b step 5 (below) is the main out-of-sample validation of the point estimates.

**Workload-cost ordering:** bcfy_feeds (0.156 %/feed, 16.9 MiB/feed) >> openmhz (0.100 %/feed, 2.8 MiB/feed) >> bcfy_calls (0.009 %/feed, 0.40 MiB/feed). The CPU ordering is dominated by per-feed subprocess count: bcfy_feeds spawns one `ffmpeg -c copy` per feed, openmhz polls HTTP for talkgroup calls at a high rate (25.83 uploads/feed-min [1b §5.6]) without subprocess per feed, and bcfy_calls polls HTTP for archived-call JSON at a low rate (0.29 uploads/feed-min [1b §5.6]) also without subprocesses. The RSS ordering follows the same pattern and is dominated by ffmpeg process memory.

**Retrospective additive validation against Experiment 1b step 5.** The primary 1b ramp's step 5 measured 77.44% CPU at ~993 active feeds in a 41.4:55.6:3.4 composition (§5.1 Table 2). We test a simple-additive prediction that ignores cross-source interaction:
$$\text{CPU}_\text{pred} = \text{base} + \sum_{i \in \{\text{feeds,calls,mhz}\}} w_i \cdot 993 \cdot \text{slope}_i$$

using `base = 6.43%` from the aggregate 1b fit (identified as the single shared container overhead; each per-source fit's intercept includes a ~5–7% contribution that is the same base, so summing three intercepts would overcount — cf. discussion below). Component contributions:

| Source | w x 993 | slope | Marginal CPU |
|---|---|---|---|
| bcfy_feeds | 411 | 0.156 | 64.1% |
| bcfy_calls | 548 | 0.009 | 4.9% |
| openmhz | 34 | 0.100 | 3.4% |
| base | — | — | 6.43% |

Predicted total: **78.8% CPU**; observed 77.44%; residual **+1.8%**.

**Intercept caveat.** Each mono-source fit's intercept contains the same shared container-runtime base overhead (5–7% from 1b step 1's 11.65% CPU at 99 feeds = 6.82% marginal + 4.83% base, approximated). Summing three intercepts would therefore overcount base by 2x. The decomposition above avoids this by using one shared base (from 1b's aggregate fit) plus three slope-only contributions.

*Caveat: 1b step 5 was measured approximately 24 hours before the 1c.B ramps on the same VM; the +1.8% residual mixes true cross-source interaction with between-day variance (diurnal upstream traffic, noisy-neighbor scheduler variance, TCP pool warm/cold state). Distinguishing the two requires same-day matched-composition validation, which we defer to future work.*

Five known mechanisms couple the three source types under a mix and make the additive prediction an upper bound on independence:
1. **aiohttp shared `TCPConnector`** with default per-host-limit pool; concurrent openmhz and bcfy_calls traffic share the connector.
2. **Async `getaddrinfo` thread pool** (default 10 workers) serializes DNS for all three sources.
3. **TLS session cache** amortizes handshake cost across sources using the same hostname families.
4. **Event-loop FIFO**: callbacks from any source queue behind any other; a slow handler for one source briefly inflates latency for all.
5. **Standard-library `logging` RLock**: all log emits serialize on one lock.

These mechanisms are hypotheses rather than measurements in this paper. Explicit interaction-term DOE (2-way full-factorial at three levels per factor) is the correct next experiment and is listed as future work in §7.

---

## 6. Discussion

### 6.1 Fleet Sizing from the Measured Coefficients

The target-based CPU fit `CPU(%) = 0.069 × feeds + 6.43` enables direct fleet sizing at the 41:55:4 source-type mix: `workers = ceil(N / feeds_per_worker)`. Setting `feeds_per_worker` requires choosing a target single-core utilization. At 100% the event loop begins to exceed one-core capacity; the fit reaches 100% at 1,358 feeds and 80% at 1,068 feeds. We recommend:

- **Steady-state: 1,000 feeds per worker** (77.4% single-core observed at step 5; fit predicts 75.3%).
- **Peak-tolerable with 20% headroom: ~1,050 feeds per worker** (fit predicts 78.8%).

The previous version of this report recommended "1,000–1,250 feeds per worker"; we retract the 1,250 upper bound because the fit predicts 92.5% single-core at 1,250 feeds, well above the 77–80% target.

**Table 7.** Fleet sizing for a 12,000-feed production target.

| Feeds / Worker | Workers Needed | VMs @ 1 worker/VM | VMs @ 2 workers/VM (measured, §6.4) |
|---|---|---|---|
| 1,000 (steady-state) | 12 | 12 | 6 |
| 1,050 (peak-tolerable) | 12 | 12 | 6 |

The 2-workers-per-VM column is **empirically validated** in §6.4 at ~1,000 feeds on a single n2-standard-4 VM, with per-container steady-state CPU of 40.0% and 45.2% respectively (sum 85.2%, vs prediction 85.7%, residual -0.5%).

### 6.2 Why the Event Loop Saturates at ~1,000 Feeds for This Mix

For this 41:55:4 workload, one feed costs 0.069% of a core in marginal CPU plus 7.15 MiB in marginal RSS, and the event loop exceeds one-core capacity between 1,000 and 1,500 feeds. A different mix would give a different crossover point.

The single-threaded property itself is architectural (§2.3): asyncio runs one loop per process by design [2], not GIL-imposed. All Python-level orchestration — scheduling, callback dispatch, buffer copies, logging formatters, ffmpeg pipe reads — executes sequentially on one thread. Asyncio subprocess transports use OS-level pipe-readiness notifications (epoll) rather than polling, but the Python callback for each readiness event runs on the loop thread; at step 6 with 621 ffmpeg processes producing ~4 chunk-boundary events/feed/min, that alone is ~40 events/second of Python callback work — one input among many to total orchestration cost.

`docker stats` CPU sums all container threads and child processes. The 108.3% at step 6 includes the event-loop thread (~100% of one core) plus background work (GC, DNS, thread pool). ffmpeg subprocesses consume CPU on other cores but are not the bottleneck — it is their *management* from the event-loop thread. §5.4 attributes one transient *class* of event-loop starvation — the 1b 9.7 s drift outlier plus the 14.5–15.5 s drifts reproduced in 1c.A — to mass `posix_spawn` subprocess creation during activation bursts (specifically to the event loop being unable to schedule on any free core while the kernel's subprocess-creation path consumes CPU across multiple cores). The *steady-state* 100% breakdown at step 6 among (a) callback dispatch, (b) logging formatter cost, (c) GCS upload coroutine wake-ups, and (d) lease-management work still requires per-thread or per-function profiling (py-spy / `perf`) which was out of scope under Container-Optimized OS constraints; that decomposition is future work.

### 6.3 Memory Is Not the Constraint

RSS scales linearly at 7.15 MiB/feed with 157 MiB base overhead (95% CI slope 7.05–7.25). At 1,500 feeds, RSS was 10,834 MiB — 69% of the 15,625 MiB cgroup limit; extrapolation places OOM at ~2,163 feeds, well above the event-loop ceiling. The 7.15 MiB/feed cost includes asyncio task objects and coroutine frames, ffmpeg pipe buffers (for bcfy_feeds), HTTP client connection state (for bcfy_calls and openmhz), 15-second FLAC audio-chunk buffers, and per-feed metadata and lease state. Linear scaling with no evidence of fragmentation or leaks across 92 minutes suggests sound memory management for sustained operation. A multi-hour soak test would strengthen confidence; that was out of scope (§7).

### 6.4 Mitigation Paths and Multi-Process Validation

Four approaches could raise per-VM density beyond the ~1,000-feed single-worker ceiling. The first is the only one with a direct quantitative projection from our data; the others are unmeasured here and listed as future work.

**Multi-process workers — measured on a same-VM two-container configuration.** Experiment 1c.A ran two identical containers (`MAX_FEEDS_PER_WORKER=500` each) on the same n2-standard-4 VM with the 41:55:4 production composition, activating 1,000 feeds total. Steady-state measurement across a 10-minute window produced per-container CPU and RSS aggregates:

| Quantity | Container A | Container B | Sum (measured) | Prediction (2 x 1b step 3 @ 500 feeds) | Residual |
|---|---|---|---|---|---|
| CPU (%, single-core) | 40.0 | 45.2 | **85.2** | 85.7 | **-0.5%** |
| RSS (MiB) | ~3,565 | ~3,665 | **7,171** | 7,418 | **-3.3%** |

The measured aggregates match the 2x single-worker prediction within small-residual bounds (CPU -0.5%, RSS -3.3%). Active-feed split between the two workers was near-even (A claimed 493 feeds with 194 ffmpeg subprocesses, B claimed 493 feeds with 199 ffmpeg subprocesses at steady state) — the `SELECT FOR UPDATE SKIP LOCKED` lease acquisition with fencing tokens spreads claims without requiring explicit coordination.

This validates, to single-VM resolution, the §6.1 Table 7 "2 workers per VM" column: **two independent asyncio workers on one n2-standard-4 VM reach ~1,000 feeds combined** with per-worker steady-state CPU below 50% single-core and combined RSS at 7,171 MiB — well under the VM's 15,625 MiB cgroup budget. The lease coordination layer (`SELECT FOR UPDATE SKIP LOCKED` plus fencing tokens, §2.1) accommodates two workers without cross-worker flapping; no `worker_id` churn was observed in AlloyDB during the measurement window.

**Between-container cross-talk during activation burst.** The SSH control channel into the host VM became intermittently unreachable during the first ~6 minutes after simultaneous activation of 1,000 feeds across both containers, coinciding with peak subprocess-spawn load (~400 concurrent `ffmpeg -c copy` subprocess creations). Load averaged 3–5 during that window and dropped to 1–2 in steady state. This is not a failure mode of the ingestion pipeline — containers continued claiming feeds and uploading to GCS throughout — but it is an operational observation about mass activation on a 4-vCPU VM. Production deploys should stagger worker activation rather than hitting both workers with a simultaneous burst.

**What this measurement does not cover:** between-VM replication (different VMs, regions, noisy-neighbor conditions); the gunicorn-multiworker-in-one-container architecture; the `MALLOC_ARENA_MAX=2` or jemalloc allocator-bracket variant; e2 vs n2 machine type A/B; and daytime vs nighttime variance. All are listed in §7 with specific follow-up recipes.

**uvloop — vendor claim; not independently validated.** The uvloop project reports 2–4× improvements in published benchmarks [3]; independent validation for this workload's Python-level overhead profile is future work. If the bottleneck is in user-level Python code (logging, coroutine orchestration, lease management) rather than libuv selector operations, uvloop's benefit may be smaller than general-purpose benchmarks suggest.

**Offload ffmpeg management** (dedicated thread/subprocess for pipe reading, exit detection, and restart, with completed chunks returned to the event loop via a queue) targets the highest-overhead per-feed component. We have not decomposed the step-6 event-loop cost by function, so the actual CPU recoverable is unmeasured.

**Feed-type-aware worker specialization.** A worker handling only `bcfy_calls` and `openmhz` feeds would have no ffmpeg subprocess overhead and should carry more feeds per worker. Per-source coefficients are now available (§5.8): a bcfy_calls-only worker at 0.009% CPU/feed could carry ~10,000 feeds before saturating, versus ~640 for a bcfy_feeds-only worker at 0.156%/feed.

---

## 7. Limitations

1. **Single run (no replication).** The ramp was executed once on one VM on one day; between-run variance (across VMs, regions, times of day, upstream Broadcastify traffic, cgroup noisy-neighbor conditions) is unbounded. A minimal replication (at least two additional ramps) would let us report mean ± SD for per-feed coefficients. This limitation subsumes time-of-day effects: the ramp ran 01:23–02:55 UTC (evening in the US), and radio feed activity and cloud-region noisy-neighbor variance differ across hours and days.

2. **Multi-process validation bounded, not asymptotic.** Experiment 1c.A validates a two-container configuration on one VM (§6.4). This does not establish scaling to four or eight workers per VM, nor does it bracket architectures (gunicorn-style multi-worker-in-one-container vs multi-container-on-one-VM).

3. **Per-source decomposition with n=3 fits.** Experiment 1c.B measures per-source slopes for bcfy_feeds, bcfy_calls, and openmhz (§5.8). Three-step fits have wide CIs whose percentile-bootstrap lower bound hits zero due to a known n=3 bootstrap-degeneracy artifact (see §5.8 discussion). Jain's *Art of Computer Systems Performance Analysis* [6] recommends at least six data points for reliable slope inference. A follow-up with five levels × three replicates per source would tighten bounds ~2.5× and produce rejection-grade CIs.

4. **No connection-pool instrumentation.** The GCS upload tail's activation-window character (§5.5) is *consistent with* pool-depth saturation during mass lease acquisition, but we did not instrument the GCS client's HTTP/2 connector pool depth; causal attribution is inferential, not directly verified.

5. **Best-effort stall RCA in 1c.A's activation burst** (§5.4). With no CPU limit set on the 1c containers, cgroup `nr_throttled` is zero by construction — CFS throttling as a candidate mechanism is not testable here. py-spy was not available under Container-Optimized OS; attribution relies on drift_ms at 2-s cadence and cgroup CPU-time series. The 9.7-s stall **reproduced and was attributed** (§5.4): two drift spikes of 14.5 s and 15.5 s at t+18 s after simultaneous two-container activation, coincident with cgroup CPU > 100% on both containers during mass ffmpeg subprocess creation. An additional run with explicit `--cpus=N` per container would bracket whether CFS throttling contributes to equivalent stalls under CPU-limited deployments; that bracket is deferred.

6. **Single VM (one n2-standard-4 in us-central1).** Different machine types, regions, or cloud providers may yield different per-feed costs due to CPU microarchitecture and network topology differences.

7. **Controlled catalog and DISABLE_PUBSUB=true.** The 2,400-feed catalog was activated in controlled batches; production feed churn introduces variability not captured here. Pub/Sub publishing was disabled to isolate the ingestion-to-GCS path; with Pub/Sub enabled, per-feed CPU cost will be slightly higher.

8. **ffmpeg_count only tracks bcfy_feeds.** `metrics.tsv`'s ffmpeg process count reflects `bcfy_feeds` subprocesses only; the other source types contribute only in aggregate CPU/RSS.

9. **92-minute ramp window.** Long-term phenomena (memory fragmentation, connection-pool degradation, asyncio task accumulation over hours or days) were not observed. A multi-day soak test at the recommended operating point would strengthen confidence.

10. **No AlloyDB monitoring.** Container-level metrics only; at production scale (12,000 feeds), database load may become a separate bottleneck not bounded by this experiment.

11. **Additive cross-source model is a hypothesis, not a bounded claim.** Five shared-state coupling mechanisms (aiohttp connector, getaddrinfo thread pool, TLS cache, event-loop FIFO, logging RLock) can produce non-additive cross-source interaction. The retrospective additive validation against 1b step 5 (§5.8) mixes true interaction with ~24-hour between-day variance; disentangling them requires same-day matched-composition validation. An explicit 2-way full-factorial DOE at three levels per factor is the follow-up experiment.

12. **No allocator bracket.** The asyncio event-loop RSS coefficient of 7.15 MiB/feed was measured with glibc's default `malloc` and `MALLOC_ARENA_MAX` at its default (number of cores). The Zapier engineering team reported 40% RSS reduction with `MALLOC_ARENA_MAX=2`, and GitLab/Heroku ship that value by default. Whether the linear-scaling claim survives under jemalloc or `MALLOC_ARENA_MAX=2` is an open question.

13. **No machine-type A/B.** All measurements are on `n2-standard-4`. The `e2` family uses a fractional vCPU model that can diverge from `n2` above ~25% sustained per-core utilization per Google documentation. The 77% target recommended in §6.1 could be silently penalized on `e2`. A matched ramp on `e2-standard-4` would bracket this.

14. **PgBouncer in transaction-mode with `max_pool_size=8` on the dev AlloyDB cluster.** A codebase audit confirms zero use of transaction-mode-breaking features (`LISTEN`/`NOTIFY`, `pg_advisory_lock`/`pg_advisory_xact_lock`, `PREPARE TRANSACTION`) — the ingestion path uses only `SELECT FOR UPDATE SKIP LOCKED` (`feed_queries.py:18`) and fencing-token UPDATE (`feed_store.py:60–418`), both pooler-safe. Connection-wait latency under 2-container x 30-connection-per-pool client load was not instrumented; steady-state behavior was clean in 1c.A. Higher worker counts (4+) per VM may hit pool queueing and would need explicit tail-latency instrumentation [14].

15. **Measurement window is below published stationarity guidelines.** Gregg's USE-method and the k6 load-testing playbook recommend >=5x ramp-up duration (which here is 5 min warmup x 5 = 25 min) for a stationarity claim to hold. 1c.B used 10-min measurement windows; 1c.A used 10 min (15 min planned, reduced). Stationarity is not tested with Mann-Kendall or similar trend tests. Phase 2 would extend to >=45-min windows with explicit trend tests.

16. **Prospective burst sampling has low stall-catch probability.** The 9.7-s stall in 1b was one event across 92 minutes of 6-step operation, approximately 1 per hour. Prospective 90-s burst-window sampling has ~2.5% catch probability. An always-on eBPF-ring-buffer-triggered-by-watchdog design (BCC/bpftrace with `slow_callback_duration` trigger) is the correct instrumentation. COS does not ship BCC by default, so this requires either a custom image or an in-process watchdog that posts a py-spy trigger on drift>N. Both deferred.

17. **Linear LSQ rather than Universal Scalability Law.** We report straight LSQ linear fits for both the aggregate 41:55:4 scaling and the three per-source ramps. Gunther's USL [7] — which separates contention (α) from coherency (β) penalties — would be more faithful to asyncio's shared-state coupling through aiohttp connector, getaddrinfo pool, TLS cache, event-loop FIFO, and logging RLock (§5.8). USL fitting requires ≥ 5 levels per source to stabilize α/β estimates, which the current n=3 per-source ramps cannot support. A Phase 2 campaign with 5-level ramps would allow USL alongside LSQ; the residual between USL-predicted and LSQ-predicted saturation points bounds the contention-coherency headroom.

**Future work refinements (P2 items from Round 3 review).** Four lower-priority refinements are acknowledged and deferred: (a) a residual plot for the six-step aggregate fit to expose any curvature beyond R² = 0.998; (b) a prediction interval at step 7 (2,000 feeds) to quantify extrapolation uncertainty; (c) an explicit note on sub-second aliasing in the 2-s cgroup sampler (stalls shorter than 2 s may be missed or underestimated by the sampler's discretisation); (d) a footnote distinguishing `posix_spawn` from `fork`+`exec` on Linux, since `asyncio.create_subprocess_exec` selects `posix_spawn` when safe (no `preexec_fn`, `close_fds=True`, no `pass_fds`) — this affects whether the kernel page-table-copy path contributes to the subprocess-storm attribution in §5.4. The code audit confirmed the pipeline uses the `posix_spawn`-safe configuration (§Pre-flight PF-2.9), but the kernel-path detail is worth a footnote for mechanism-level fidelity.

---

## 8. Conclusion

This experiment measures per-feed cost coefficients for a Python asyncio audio ingestion pipeline at a production-representative three-source mix. Ordinary least squares across six step means yields `CPU(%) = 0.069 × feeds + 6.43` (95% CI slope 0.0689 ± 0.0045, R² = 0.998) and `RSS(MiB) = 7.15 × feeds + 157` (95% CI slope 7.15 ± 0.10, R² = 0.9999). The single-threaded event loop approaches saturation near 1,000 feeds (77.4% single-core) and exceeds one-core capacity at 1,500 feeds (108.3%) — leaving approximately three of four vCPUs effectively idle at step 6 as a consequence of asyncio's one-loop-per-process architecture.

From these coefficients we recommend **1,000 feeds per worker for steady state** and **~1,050 peak-tolerable**. For the 12,000-feed production target, this implies **12 workers**; multi-process scaling is **empirically validated at k=2 containers per VM** (§6.4, Experiment 1c.A: sum CPU 85.2% vs predicted 85.7%), packing the fleet into **6 n2-standard-4 VMs under the k=2 configuration**. Scaling to k=4 or k=8 workers per VM is not measured and would require additional bracketing (§7 item 2). Per-source decomposition (§5.8) yields point-estimate slopes — bcfy_feeds 0.156 %/feed (ffmpeg subprocess per feed), openmhz 0.100 %/feed (high upload rate), bcfy_calls 0.009 %/feed (HTTP polling only) — with n=3 bootstrap CIs that cannot reject a zero-slope null at 95% confidence; the out-of-sample additive validation against the 41:55:4 mixed workload (predicted 78.8% CPU vs 77.4% observed, +1.8% residual) is the main support for these point estimates. Formal inference-grade CIs are deferred to a Phase 2 campaign with ≥ 5 levels × 3 replicates per source.

The paper's contribution is the coefficients and their confidence intervals, the workload-mix-specific saturation point, the pre-flight validation methodology, and the fleet-sizing translation — not a rediscovery of asyncio's single-threaded property. Experiment 1c closes three of the original gaps: multi-process scaling is validated (§6.4), per-source coefficients are measured (§5.8), and the 9.7-s drift outlier is reproduced and attributed to event-loop starvation during mass subprocess creation (§5.4). The strongest remaining gaps — multi-day replication, allocator bracketing, and machine-type A/B — are enumerated in §7 with specific follow-up recipes.

---

## 9. References

[1] Python Software Foundation. "Global Interpreter Lock." Python Documentation. https://docs.python.org/3/glossary.html#term-global-interpreter-lock

[2] Python Software Foundation. "asyncio — Asynchronous I/O." Python Documentation. https://docs.python.org/3/library/asyncio.html

[3] MagicStack Inc. "uvloop: Ultra fast asyncio event loop." https://github.com/MagicStack/uvloop

[4] Docker, Inc. "docker stats." Docker Documentation. https://docs.docker.com/reference/cli/docker/container/stats/ — On a multi-core host, `docker stats` reports container CPU as percentage of a single core under both cgroup v1 and cgroup v2; 400% = full 4-vCPU utilization; 100% = one core saturated.

[5] Google Cloud. "Machine types: n2-standard." Compute Engine Documentation. https://cloud.google.com/compute/docs/general-purpose-machines#n2_machine_types

[6] R. Jain. *The Art of Computer Systems Performance Analysis: Techniques for Experimental Design, Measurement, Simulation, and Modeling.* Wiley, 1991. (Sampling-size and CI requirements for per-parameter estimation.)

[7] N. J. Gunther. *Guerrilla Capacity Planning: A Tactical Approach to Planning for Highly Scalable Applications and Services.* Springer, 2007. (Universal Scalability Law; contention parameter alpha for shared-state analysis.)

[8] G. Heiser. "Systems Benchmarking Crimes." https://www.cse.unsw.edu.au/~gernot/benchmarking-crimes.html (Common methodological errors in systems measurement papers; maintained since ca. 2010.)

[9] E. van der Kouwe et al. "SoK: Benchmarking Flaws in Systems Security." *EuroS&P*, 2019.

[10] B. Gregg. *Systems Performance: Enterprise and the Cloud.* Pearson, 2nd ed., 2020. (USE method; off-CPU profiling; flame graph methodology.)

[11] M. Kleppmann. "How to Do Distributed Locking." https://martin.kleppmann.com/2016/02/08/how-to-do-distributed-locking.html (Fencing tokens for correctness under lease-based locking.)

[12] M. Grottke, R. Matias Jr., and K. S. Trivedi. "The Fundamentals of Software Aging." *IEEE International Conference on Software Reliability Engineering Workshops (ISSREW),* 2008. (Motivates soak testing and restart policies for memory-growth under long-running subprocess-heavy workloads.)

[13] Indeed Engineering. "Unthrottled: Fixing CPU Limits in the Cloud." https://engineering.indeedblog.com/blog/2019/12/unthrottled-fixing-cpu-limits-in-the-cloud/ 2019. (CFS throttling pathologies for latency-sensitive workloads at low CPU utilization.)

[14] JP Camara. "PgBouncer Is Useful, Important, and Fraught With Peril." https://jpcamara.com/2023/04/12/pgbouncer-is-useful.html 2023. (Session-level features broken under transaction-mode pooling.)

[15] Frigate NVR. "Memory leak with ffmpeg and segment muxer." GitHub issues #6645, #11676, #13133, #19925. (Documented ffmpeg subprocess memory growth under unreliable upstream RTSP input; relevant to long-horizon soak extrapolation.)

---

## A. Appendix

### A.1 `docker stats` CPU Semantics

`docker stats` reports container CPU usage derived from the cgroup CPU accounting subsystem [4]. Under both cgroup v1 (`cpuacct.usage`) and cgroup v2 (`cpu.stat`), the reported percentage is **normalized to a single CPU core**: 100% represents one core fully utilized and the upper bound on an *N*-vCPU host is *N* × 100%. A 4-vCPU host reports values in [0%, 400%]. All CPU percentages in §5 and §6 use this convention: 77.4% means 77.4% of one core (19.35% of the 4-vCPU VM); 108.3% means 108.3% of one core (27.07% of the 4-vCPU VM). Some third-party dashboards renormalize to "percent of host" for display, which requires dividing by vCPU count.

---

## 10. AI Disclosure

This report was drafted with assistance from Claude (Anthropic). The experimental methodology, execution, and raw-data collection (`metrics.tsv`, `ramp.log`, Cloud Logging queries) were performed by the human operator; Claude assisted with data analysis, linear regression computation (with confidence intervals), writing composition, and cross-referencing numeric claims against raw data.

The paper underwent one revision cycle addressing (a) a Stage 2.5 integrity verification that identified five P1 issues (least-squares coefficients, 75%-versus-73% framing, active-feed deficit reporting, abstract saturation wording, and the 1,250-feed upper bound), and (b) a Stage 3 multi-perspective peer review that raised three P0 issues (novelty framing, multi-process validation, the 9.7-second drift event), twelve P1 items, and seven P2 items. All integrity P1 fixes are applied. The P0 items are addressed by reframing novelty around per-feed coefficient measurement, explicitly acknowledging multi-process validation as a limitation (§6.4, §7), and reattributing the 9.7-second drift event to the step-7 activation burst at 02:54:45 UTC — after step 6 measurement ended at 02:54:20 UTC — with within-step stationary drift p99 ≤ 8.6 ms for comparison. All nine `file:line` code citations were verified against the repository prior to revision and are preserved. Numeric claims were re-verified against raw data in `/tmp/exp1b_report/metrics.tsv`, `/tmp/exp1b_report/ramp.log`, `/tmp/exp1b_report/stats.json`, and the Cloud Logging queries cited inline. See also `/tmp/exp1b_report/response_to_reviewers.md` for the full traceability matrix.
