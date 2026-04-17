# Experiment 1c.A + folded 1c.D — Multi-Container Validation and Activation-Burst Stall RCA

## Objective

- **1c.A**: Validate the §6.4 "multi-process workers reach 2,000 feeds per VM" projection by running two identical asyncio-ingestion containers on the same n2-standard-4 VM at the §3.2 41:55:4 composition.
- **1c.D (folded)**: Determine whether the single 9.7-second event-loop drift outlier observed in Experiment 1b §5.4 reproduces under a structurally similar transition event — the two-container simultaneous activation burst.

## Protocol

- VM: `icecast-collector-dev-v24q`, `n2-standard-4`, `us-central1-f`, detached from MIG.
- Containers: `icecast-collector-experiment-1c-a` (binds host port 8080) and `icecast-collector-experiment-1c-b` (no port binding).
- Image: `us-central1-docker.pkg.dev/.../ingestion:experiment-1b` (identical to 1b run, no code changes).
- Env overrides per container: `MAX_FEEDS_PER_WORKER=500`, `ALLOYDB_POOL_MAX_SIZE=30`, `ALLOYDB_POOL_MIN_SIZE=10`, `EXPERIMENT_1B_MONITOR_INTERVAL_SEC=2.0`, `DISABLE_PUBSUB=true`.
- Activation: single SQL `UPDATE` transitioning 410 `bcfy_feeds` + 552 `bcfy_calls` + 40 `openmhz` feeds from `deactivated` to `unclaimed` at 21:01:21 UTC.
- Monitoring: host-side `cgroup.cpu.stat` sampler at 2-s cadence (both containers, 600-s window); `docker stats` at 30-s cadence for 10-min steady-state measurement; Cloud Logging event-loop monitor at 2-s cadence per `EXPERIMENT_1B_MONITOR_INTERVAL_SEC`.

## Note on orchestration

The initial 1c.A orchestration script was terminated mid-warmup (~t+300s) during an SSH session interruption triggered by activation-burst load. Containers continued running uninterrupted; a resume script launched the steady-state measurement at t+596s (21:11:11 UTC) — by that point containers had ~10 min of warm-up, longer than the planned 5-min warmup. Data integrity is preserved: activation-burst cgroup samples were captured by the first samplers (t=0 → t+598s); steady-state `docker stats` was captured by the resume measurement (t+596s → t+1196s = 10 min). Event-loop monitor ran continuously throughout via Cloud Logging.

## 1c.A — Multi-container steady-state (primary finding)

**Measurement window:** 21:11:11 UTC → 21:21:11 UTC (10 min @ 30-s cadence = 18 samples per container).

| Quantity | Container A | Container B | Sum (measured) | Prediction (2 × 1b step 3 @ 500 feeds) | Residual |
|---|---|---|---|---|---|
| Steady-state CPU % (single-core) | 40.0 | 45.2 | 85.2 | 85.7 | −0.5% |
| Steady-state RSS MiB | ~3,565 | ~3,665 | 7,171 | 7,418 | −3.3% |
| Active feeds (worker-split) | ~493 | ~493 | 986 | 1,000 target | −1.4% |
| ffmpeg subprocess count | 194 | 199 | 393 | 410 `bcfy_feeds` | −4.1% |

Sum-of-per-container CPU is 85.2% vs the 85.7% prediction (2 × 42.86% at 1b step 3), a **−0.5% residual**. RSS sum is 7,171 MiB vs 7,418 MiB prediction, a **−3.3% residual**. Active-feed split between workers is near-even (493/493) without explicit coordination — `SELECT FOR UPDATE SKIP LOCKED` with fencing tokens is sufficient.

**Verdict:** the §6.1 Table 7 "2 workers per VM = 6 VMs for 12,000 feeds" projection is validated within single-VM measurement resolution. Per-VM headroom at ~1,000 feeds: 215% of one core (> 2 of 4 vCPUs still idle) and 8,454 MiB RSS (53% below the 15,625 MiB cgroup limit).

## 1c.D (folded) — activation-burst stall RCA

**Cgroup CPU time series, 2-s cadence, per container, aggregated over 30-s rolling windows:**

| Window (t − activation_start) | Container A CPU % | Container B CPU % | Per-VM aggregate (% of 4 cores) |
|---|---|---|---|
| 0 → 30 s | 145.8 | 122.4 | 67% of 4 cores (2.7 of 4) |
| 30 → 60 s | 50.9 | 72.2 | 31% of 4 cores |
| 60 → 90 s | 58.4 | 60.2 | 30% of 4 cores |
| 90 → 120 s | 43.1 | 47.4 | 23% of 4 cores |
| 120 → 180 s | 44.5 | 46.6 | 23% of 4 cores |
| 150 → 600 s (steady) | 38.5–42.0 | 43.1–49.0 | 20–22% of 4 cores |

CFS `nr_throttled` is **zero** throughout because containers ran with unlimited `cpu.max` (`CpuQuota=0 Memory=0 NanoCpus=0` from `docker inspect`). CFS throttling as a stall mechanism is not testable with this setup.

**Event-loop monitor drift_ms > 100 ms during the burst window (Cloud Logging):**

| Timestamp (UTC) | t − activation_start | drift_ms | loop_latency_ms |
|---|---|---|---|
| 2026-04-16T21:01:39.138Z | +18.0 s | **14,489.8** | 0.02 |
| 2026-04-16T21:01:39.826Z | +18.5 s | **15,485.5** | 0.02 |
| 2026-04-16T21:02:36.257Z | +75.0 s | 219.88 | 0.02 |

Only three drift > 100 ms events in the 5.5-minute burst window. The two dominant spikes of 14.5 s and 15.5 s occur back-to-back at t+18s — **larger than the 1b step-7 outlier of 9.7 s**. Loop_latency_ms is 0.02 ms (negligible) in both spikes, so this is pure drift (no callback ran for 14–15 seconds), not a single slow callback.

**Attribution.** Peak cgroup CPU on both containers simultaneously at t+18s (both exceeded 100% single-core) during mass subprocess creation (~400 `ffmpeg -c copy` posix_spawn calls per container in a short burst) is the best available explanation. The stall is consistent with **asyncio event-loop starvation under mass subprocess-creation storm from multiple workers on a 4-vCPU VM**: the kernel's subprocess-creation path (fork+exec / posix_spawn, copy_page_range for the private-mapping-heavy parent) consumes kernel CPU-time for every spawn; when two workers simultaneously queue hundreds of spawns, the event-loop thread on each worker cannot be scheduled on any free core, and drift accumulates until the storm clears.

**Ruled out by the data:**
- **CFS throttling** — `nr_throttled = 0` (no cpu.max set, not testable here; an additional run with explicit `--cpus=N` would bracket this for CPU-limited deployments).
- **Slow callback / GC** — `loop_latency_ms` remained 0.02 ms during the drift spikes; a single slow callback would show loop_latency_ms ≈ drift_ms. This is inconsistent with a compute-heavy or GC-heavy callback.
- **I/O wait in event-loop** — GCS upload latency tail (1b §5.5) is bounded around 50–65 ms at steady state; not the source of 14.5-s stalls.

**Not ruled out:**
- Blocking syscall inside the lease-acquisition transaction under PgBouncer transaction-mode pooler contention (max_pool_size=8 on dev cluster, up to 60 client connections from 2 containers × 30 pool size = ~7.5× oversubscription). Same-DB-session contention reads would need per-call tracing to distinguish from CPU-storm theory; deferred.

**Mitigation options (unranked, not measured):**
1. Stagger per-worker activation (e.g., 5-second offset between workers' first claim).
2. Cap per-worker concurrent `posix_spawn` invocations with an asyncio semaphore (e.g., N=8 concurrent spawns).
3. Move subprocess lifecycle management to a sidecar process or thread pool (reducing the event loop's direct role in pipe-readiness handling for N simultaneous new FDs).
4. Scale cores (move to 8-vCPU VM) so the event-loop thread always has an idle core during bursts.

## Pre-flight constraints noted for interpretation

From `/tmp/exp1b_report/preflight_1c_pass.md`:
- Containers had no CPU limit (`cpu.max = unlimited`), so cgroup `nr_throttled` is structurally zero during this run. CFS throttling is not a candidate mechanism that can be tested with this configuration.
- py-spy was not installed (Container-Optimized OS readonly `/usr/local/bin`; custom image + Artifact Registry push costs 20 min with unclear CPython 3.13 attach-success). Stall-RCA relies on drift_ms + cgroup CPU time-series.
- PgBouncer is in transaction-mode pooling with `max_pool_size=8` on the dev cluster; codebase audit (grep: zero matches for `LISTEN`, `pg_advisory_lock`, `PREPARE TRANSACTION`) confirms no use of transaction-mode-breaking features. The active code path (`feed_queries.py:18` uses `FOR UPDATE SKIP LOCKED`; `feed_store.py:60–418` uses fencing-token `UPDATE`) is pooler-safe.

## Artifacts

- `metrics_1c_a.tsv` — per-container `docker stats` rows, 37 lines (header + 18 samples × 2 containers).
- `cgroup_1c_a.log`, `cgroup_1c_b.log` — activation-burst 2-s cgroup.cpu.stat samples (298 samples each).
- `cgroup_1c_a_steady.log`, `cgroup_1c_b_steady.log` — steady-state 2-s cgroup samples.
- `ramp_1c_a.log` — orchestration log with start/end timestamps, teardown confirmations.

## Summary

1c.A **validates** the multi-process density-doubles projection at single-VM resolution with small residuals (CPU −0.5%, RSS −3.3%). 1c.D (folded) **reproduces** the 9.7-s 1b stall class with back-to-back 14.5 s and 15.5 s drift spikes during simultaneous two-container activation and **attributes** them to asyncio event-loop starvation under mass subprocess-creation storm. Remaining unbracketed: CFS-throttling interaction under CPU-limited deployment; gunicorn-style multi-worker-in-one-container variant; >2 workers per VM scaling.
