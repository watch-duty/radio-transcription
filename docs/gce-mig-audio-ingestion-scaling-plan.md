# GCE MIG audio ingestion — scaling plan (final)

**Author:** Shuojing
**Date:** 2026-04-18
**Status:** Final, for review

## Overview

The audio ingestion pipeline ingests up to 12,027 concurrent audio feeds from three upstream sources (Broadcastify continuous feeds, Broadcastify Calls API, OpenMHZ) and writes segments to GCS for downstream transcription. Today it runs on a single n2-standard-4 VM with one worker process — adequate for development, but a SPOF with no path to scale as the catalog grows past ~1,600 feeds.

This plan converts the deployment into a **regional Managed Instance Group of 2–8 VMs (ceiling 10) running two identical worker containers each**, autoscaled by a two-signal policy. Each worker handles all three feed types, and the claim path self-balances via `SELECT ... FOR UPDATE SKIP LOCKED`: if any type backs up, its share of the unclaimed pool grows and workers select it more often automatically.

All 37 changes below fall into five groups:

| Group | What it solves |
|---|---|
| **1. Schema & DB** | Heap-Only Tuple (HOT) updates are currently broken by `last_heartbeat` being in the leasing index — at peak, this would produce ~170M dead tuples/day, recoverable only by VACUUM FULL. This group fixes HOT, adds the columns the new claim path and autoscaler need, deploys maintenance jobs via pg_cron, and adds a CI guard against future index regressions. |
| **2. Claim query rewrite** | `bcfy_feeds` is 40× heavier in memory per feed than `bcfy_calls`; adversarial clustering of claimed rows can OOM a worker. This group introduces a per-type memory budget enforced structurally by PostgreSQL, coalesces writes to cut dead-tuple rate another ~4×, and replaces the single-transaction SIGTERM release with a batched + jittered release to prevent thundering-herd reclaim. |
| **3. AlloyDB + pooler config** | Size the managed pooler and asyncpg pools for the new peak (16 workers); release row locks promptly when a worker dies. |
| **4. Worker code** | Raise per-worker capacity from 250 → 800 feeds; make graceful shutdown work at the new scale; prevent startup stampedes via layered jitter; ship the runtime tunings the 1b experiment's performance numbers already assumed. |
| **5. Infrastructure** | Deploy the MIG with even zonal distribution; configure the two-signal autoscaler (no hardcoded "feeds per VM" constant in the scaling path); build the 50-LOC publisher that exposes the autoscaler's backlog signal; wire the ramp knob for a graduated rollout. |

A deploy-ordering note at the end (§6) identifies which items must land together — HOT requires both the fillfactor change and the index restructure to ship in one migration, for instance.

---

## 1. Schema & DB

Workers coordinate through PostgreSQL row locks. Every active lease gets a heartbeat update every ~20 s and progress-bookmark writes every ~10 s. In PostgreSQL, an UPDATE creates a new row version (MVCC dead tuple) — unless **Heap-Only Tuple (HOT)** updates apply, which perform the update in-page and skip index writes. HOT requires two conditions: (a) page slack (`fillfactor < 100`), and (b) no index on any mutated column.

**Neither condition holds today.** The `feeds` table uses the default `fillfactor=100`, and `idx_feeds_leasing` includes `last_heartbeat` — a column every heartbeat mutates. Without this fix, HOT is off at cutover: dead tuples accumulate at ~170M/day, and only `VACUUM FULL` (exclusive table lock) can reclaim them.

### 1.1 Table storage parameters

```sql
ALTER TABLE feeds SET (
  fillfactor = 70,
  autovacuum_vacuum_scale_factor = 0.05,
  autovacuum_vacuum_cost_delay = 10
);
VACUUM FULL feeds;   -- one-time, rewrites pages with fillfactor=70 slack
```

`fillfactor=70` reserves 30% of every page for HOT updates. The autovacuum parameters are aggressive defaults for this table specifically — `feeds` is small (~12k rows, ~430 pages), so there is no cost to running autovacuum more often.

### 1.2 Index changes

Replace the bloat-prone leasing index with four HOT-safe partial indexes.

| Index | Definition | Serves |
|---|---|---|
| `idx_feeds_leasing` | — | **DROP** (indexes `last_heartbeat`, blocks HOT) |
| `idx_feeds_unclaimed` | `(id) WHERE status='unclaimed'` | admin paths / id-only access |
| `idx_feeds_failing_retryable` | `(retry_after) WHERE status='failing'` | recovery-path claim (see §2.4) |
| `idx_feeds_active` | `(id) WHERE status='active'` | abandoned-lease sweep (see §1.4) |
| `feeds_claim_by_type_idx` | `(source_type, id) WHERE status='unclaimed'` | per-type branches in the new claim CTE (see §2.1) |

None of these indexed columns are mutated on the hot path — HOT updates remain valid after the change.

### 1.3 New columns

```sql
ALTER TABLE feeds ADD COLUMN unclaimed_since  TIMESTAMP WITH TIME ZONE;
ALTER TABLE feeds ADD COLUMN last_progress_at TIMESTAMP WITH TIME ZONE;  -- unindexed
```

- **`unclaimed_since`** feeds the autoscaler's `oldest_unclaimed_feed_age` signal (§5.3). Set by INSERT, the sweep (§1.4), and the SIGTERM release path (§2.7). Backfill `unclaimed_since = created_at` for existing `unclaimed` rows as part of the migration.
- **`last_progress_at`** absorbs progress-bookmark writes that currently target `last_heartbeat`. **Deliberately unindexed** — that's what keeps those writes HOT-eligible (see §2.5).

### 1.4 Scheduled jobs (pg_cron)

Two jobs.

**Abandoned-lease sweep — every 30 s, batched at LIMIT 500:**

```sql
UPDATE feeds
   SET status='unclaimed', worker_id=NULL, unclaimed_since=NOW()
 WHERE id IN (
     SELECT id FROM feeds
      WHERE status='active' AND last_heartbeat < NOW() - INTERVAL '60 seconds'
      LIMIT 500
 );
```

This is the recovery mechanism when a worker dies without releasing its leases. **Batching matters**: a zonal outage can abandon thousands of leases at once, and flipping them all to `unclaimed` in one transaction would trigger a fleet-wide polling stampede as every surviving worker hits the same new pool on its next poll. With `LIMIT 500` and 30 s cadence, a ~4,000-lease drain spreads over several minutes.

**Minute-cadence VACUUM — for line-pointer-array maintenance:**

```sql
SELECT cron.schedule('feeds-vac', '* * * * *', 'VACUUM (ANALYZE) feeds');
```

Even with HOT working perfectly, PostgreSQL's opportunistic `heap_page_prune_opt` reclaims tuple bytes but **does not shrink the line-pointer (ItemId) array**. Each 8 KB page has a hard cap at ~291 LP slots; once the LP array fills, new HOT updates on that page are denied even though free tuple-byte space remains, and bloat returns. Only `VACUUM` pushes `LP_DEAD → LP_UNUSED`. On a 430-page cached table, each run completes in tens of milliseconds at negligible cost.

### 1.5 Pre-deploy CI guard

The HOT guarantee is fragile. One future migration adding an index on any mutated column silently breaks it, and the symptom (rising bloat) takes hours to become visible in production. A CI check parses every proposed migration and fails the build if it adds an index referencing any of the eight guarded columns.

**Guarded column list:** `last_heartbeat`, `unclaimed_since`, `worker_id`, `fencing_token`, `last_processed_filename`, `last_bookmark_time`, `failure_count`, `retry_after`.

**Allow-list exception:** `idx_feeds_failing_retryable` is the only permitted index on `retry_after` — failure is rare, so the bloat frequency is acceptable, and this index is how the recovery path (§2.4) finds rows.

```sql
SELECT i.indexname, a.attname
  FROM pg_indexes i
  JOIN pg_class c ON c.relname = i.indexname
  JOIN pg_index x ON x.indexrelid = c.oid
  JOIN pg_attribute a ON a.attrelid = x.indrelid
 WHERE i.schemaname = 'public'
   AND a.attname IN (
     'last_heartbeat','unclaimed_since','worker_id','fencing_token',
     'last_processed_filename','last_bookmark_time','failure_count','retry_after'
   )
   AND a.attnum = ANY(x.indkey)
   AND i.indexname != 'idx_feeds_failing_retryable';
-- CI fails if any row returned.
```

---

## 2. Claim query rewrite

The claim query is where workers compete for work. Today it is a single `SELECT ... FOR UPDATE SKIP LOCKED` against the unclaimed pool — each worker takes as many feeds as fit its remaining slack. That is fine for uniform workloads but creates an **OOM risk under mix variance**: bcfy_feeds is 16.9 MiB/feed, bcfy_calls is 0.40 MiB/feed, openmhz is 2.8 MiB/feed. If bcfy_feeds rows happen to be temporally clustered in the heap (e.g., a bulk catalog import), one worker can claim a heavily bcfy_feeds-weighted batch. At 800 feeds of pure bcfy_feeds, that's 13.5 GiB on a worker sharing a 16 GiB VM with a second container — instant OOM.

This section introduces a **per-type memory budget** enforced structurally by PostgreSQL (worker computes per-type LIMITs, planner honors them) plus **write coalescing** that cuts the bloat-risk UPDATE rate ~10× before HOT does any work.

### 2.1 Primary CTE — per-type UNION ALL with MATERIALIZED

```sql
WITH claimed AS MATERIALIZED (
  (SELECT id FROM feeds
     WHERE source_type='bcfy_feeds' AND status='unclaimed'
       AND (('x'||substr(md5(id::text),1,7))::bit(28)::integer) % 100 < $3  -- ramp filter
     ORDER BY id
     FOR NO KEY UPDATE SKIP LOCKED
     LIMIT $4)
  UNION ALL
  (SELECT id FROM feeds
     WHERE source_type='bcfy_calls' AND status='unclaimed'
       AND (('x'||substr(md5(id::text),1,7))::bit(28)::integer) % 100 < $3
     ORDER BY id
     FOR NO KEY UPDATE SKIP LOCKED
     LIMIT $5)
  UNION ALL
  (SELECT id FROM feeds
     WHERE source_type='openmhz' AND status='unclaimed'
       AND (('x'||substr(md5(id::text),1,7))::bit(28)::integer) % 100 < $3
     ORDER BY id
     FOR NO KEY UPDATE SKIP LOCKED
     LIMIT $6)
)
UPDATE feeds
   SET status='active', worker_id=$1, fencing_token=fencing_token+1, last_heartbeat=NOW()
  FROM claimed
 WHERE feeds.id = claimed.id
RETURNING feeds.*;
```

Three elements are load-bearing and cannot be dropped:

- **`AS MATERIALIZED`** — without it, the planner is free to inline the CTE into the outer UPDATE and, under a nested-loop plan, re-evaluate the UNION ALL per outer row. Each re-evaluation locks *different* rows (SKIP LOCKED interacts with in-flight locks), and the UPDATE can bypass the LIMITs entirely. `MATERIALIZED` forces single-evaluation into a bounded worktable.
- **`FOR NO KEY UPDATE`** — weaker lock than `FOR UPDATE`, sufficient because the claim mutates no unique keys. Reduces lock-manager contention at peak.
- **`ORDER BY id` per branch** — paired with `feeds_claim_by_type_idx (source_type, id)`, produces an index-only scan per branch with no sort node.

The ramp filter uses **md5, not `hashtext()`**. `hashtext()` is a documented-internal PostgreSQL function whose algorithm has historically changed across major versions; an AlloyDB minor-version upgrade mid-ramp could silently re-shuffle feeds between enabled and disabled buckets, violating the ramp's determinism and rollback semantics. md5 is documented stable across versions.

### 2.2 Per-type caps

Shippable as env vars so they can be tuned without redeploy:

| Type | Env var | Cap | Why |
|---|---|---|---|
| bcfy_feeds | `cap_bcfy_feeds` | **240** | Memory-heavy (16.9 MiB/feed); 240 × 16.9 ≈ 4 GiB per worker, fits k=2 × 16 GiB VM with headroom. |
| bcfy_calls | `cap_bcfy_calls` | **600** | Cheap; cap sized to keep per-worker DB load balanced. |
| openmhz | `cap_openmhz` | **900** | Cheap; highest cap. |

### 2.3 Per-branch LIMIT computation

A per-call LIMIT alone does not bound total-held feeds across many claim cycles — a worker polling every 5 s with a fixed LIMIT of 240 would still accumulate arbitrarily many bcfy_feeds until `max_feeds_per_worker` is reached. Bounding the total requires the worker to adjust its per-call LIMIT to reflect current holdings.

Each claim cycle, the worker computes:

```python
remaining_budget[type] = cap[type] - current_held[type]
total_slack            = max_feeds_per_worker - sum(current_held.values())

# LIMIT for each CTE branch:
limit[type] = max(0, min(cap[type], remaining_budget[type], total_slack))
```

The overall per-call row count is additionally bounded by the pinned claim batch size (§4.1). PostgreSQL enforces whatever LIMIT the worker passes — the DB is the structural guarantee that the worker receives at most what it asked for; the worker is responsible for asking correctly.

### 2.4 Recovery query

When the primary CTE returns fewer rows than requested, the worker issues a secondary query against the recovery branches (`failing` with retry elapsed, `active` with stale heartbeat):

```sql
SELECT * FROM feeds
 WHERE (
     (status='failing' AND (retry_after IS NULL OR retry_after <= NOW()))
  OR (status='active'  AND last_heartbeat < NOW() - $2::interval)
 )
   AND (('x'||substr(md5(id::text),1,7))::bit(28)::integer) % 100 < $3
 ORDER BY retry_after ASC NULLS FIRST, id
 LIMIT $4
 FOR NO KEY UPDATE SKIP LOCKED;
```

No per-type budget on the recovery path — `failing` is volume-bounded by operational reality (failure is rare), and `active`-abandoned is drained by the sweep (§1.4). The ordering prioritizes rows whose retry window opened earliest.

### 2.5 Write coalescing — drop `last_heartbeat` side-effects

Three queries today mutate `last_heartbeat` as a side effect of unrelated work. Remove each:

| Query | Before | After |
|---|---|---|
| `UPDATE_PROGRESS_SQL` | writes `last_heartbeat = NOW()` | writes `last_progress_at = NOW()` instead (the new unindexed column, §1.3) |
| `RELEASE_FEEDS_BATCH_SQL` | writes `last_heartbeat = NOW()` | drop the write (release sets `status`/`worker_id`/`unclaimed_since` only) |
| `REPORT_FAILURE_SQL` | writes `last_heartbeat = NOW()` | drop the write |

After this, heartbeat renewal is the only remaining writer of `last_heartbeat`. Progress writes still happen (every ~10 s × ~1,500 feeds per VM ≈ 1,200/sec fleet-wide) but target an unindexed column — fully HOT-eligible, no index-write amplification.

### 2.6 Skip-if-recent heartbeat predicate

```sql
UPDATE feeds
   SET last_heartbeat = NOW()
 WHERE worker_id = $1
   AND id = ANY($2)
   AND last_heartbeat < NOW() - INTERVAL '15 seconds';
```

PostgreSQL MVCC rule: an UPDATE whose WHERE matches always writes a new tuple; when WHERE does not match, zero tuples are written — zero WAL, zero dead-tuple accounting. The `last_heartbeat < NOW() - INTERVAL '15 seconds'` predicate filters out redundant heartbeats (e.g., after a brief worker pause that processes backlog in a burst), where the 15 s window has not yet elapsed for some of the worker's leases.

Combined with §2.5 and the cadence relax in §4.1 (15 s → 20 s), the UPDATE rate on `last_heartbeat` drops from ~2,000/sec to ~490/sec — dead-tuple generation falls from ~170M/day to ~42M/day. HOT is still load-bearing, but has ~4× less work to do.

### 2.7 SIGTERM batched + jittered release

Today's shutdown path releases all of a worker's leases in a single atomic UPDATE. At 1,500 feeds per VM, that flips 1,500 rows to `unclaimed` in one commit — every surviving worker sees 1,500 new claimable rows simultaneously on their next poll, producing a thundering-herd claim burst against AlloyDB.

Replace with per-batch commits of ~50 rows with 0–2 s jitter between batches:

```python
for batch in chunks(held_feed_ids, 50):
    async with pool.acquire() as conn:
        async with conn.transaction():     # explicit per-batch COMMIT
            await conn.execute(
                "UPDATE feeds SET status='unclaimed', worker_id=NULL, "
                "unclaimed_since=NOW() WHERE worker_id=$1 AND id = ANY($2)",
                worker_id, batch,
            )
    await asyncio.sleep(random.uniform(0, 2))
```

**Per-batch COMMIT is required**: the whole release runs for ~30 s (30 batches × ~1 s avg jitter); a single transaction that long would hit the `idle_in_transaction_session_timeout` (§3) if AlloyDB is slow, and the entire release would be aborted — leaving leases orphaned for the sweep to reclaim later.

This runs as step 3 of the existing shutdown sequence: `heartbeat-off → cancel-tasks → release`. Step order is preserved deliberately — reversing it would reintroduce the fence-violation self-termination race the current code comments warn against.

---

## 3. AlloyDB + pooler config

At peak (16 workers × 9 client slots each = 144 client connections, ~80 concurrent backend transactions), the current pool defaults are undersized. Connection hygiene is also inadequate: a worker that crashes without closing its connection leaves its row locks held for up to 2 hours under default TCP timeouts, blocking other workers from claiming those rows.

| Setting | Location | Value | Why |
|---|---|---|---|
| Pooler mode | Managed pooler | `transaction` | Compatible with `SELECT ... FOR UPDATE SKIP LOCKED` — row locks are held inside the transaction and released at COMMIT. |
| Server-side prepared statements | Managed pooler | off (or confirm pgbouncer ≥ 1.21 transaction-mode prepared-statement support) | Standard transaction-mode limitation. |
| `default_pool_size` | Managed pooler backend (per user/db) | **160** (raise if below) | Covers ~64 steady-state backends + 16 heartbeat peaks + headroom. |
| asyncpg main pool min/max | `storage/settings.py` | **8 / 8** (from 5 / 5) | Sized for claim + write + progress concurrency per worker. |
| asyncpg heartbeat pool | (unchanged) | 1 / 1 | Dedicated heartbeat isolation on its daemon thread — correct as-is. |
| asyncpg `connect_args` TCP keepalives | worker | `idle=60, interval=10, count=3` | AlloyDB detects a dead peer within ~90 s and releases its row locks, instead of the default ~2 h. |
| `idle_in_transaction_session_timeout` | AlloyDB GUC | **`30s`** | If a worker is alive but its transaction is stuck (e.g., frozen event loop), AlloyDB aborts it and releases its locks. |

**Nomenclature note.** AlloyDB has three distinct connection limits — server-side `max_connections` (default 1,000, unchanged by this plan), pooler frontend `max_client_conn` (already 800 by existing module, unchanged), and pooler backend `default_pool_size` (this plan raises to 160). The server default covers the 160-backend pool plus admin overhead; no server-side change is needed.

---

## 4. Worker code

Worker-side changes support a **~3× raise in per-worker capacity** (250 → 800 feeds) and a **9× raise in graceful-shutdown budget** (10 s → 90 s) required for the new batched release at scale, plus a set of runtime tunings that prevent startup stampedes and realize the 1b experiment's performance numbers (uvloop, jemalloc, aiohttp keep-alive, expanded port range).

### 4.1 Settings

| Setting | Current | Target | Why |
|---|---|---|---|
| `max_feeds_per_worker` | 250 | **800** | Per-worker RSS math on n2-standard-4 at k=2 leaves comfortable headroom at 800; 1,000 leaves too little. |
| `heartbeat_interval_sec` | 15 | **20** | 1:3 ratio vs the 60 s abandonment window (matches Kubernetes kubelet NodeLease / etcd lease convention). Drops heartbeat row-update rate by 25%. |
| `graceful_shutdown_timeout_sec` | 10 | **90** | Covers cancel-tasks (≤30 s) + batched release (~30 s) + pool close + slack. Still inside GCE's 120 s ACPI soft-off window. |
| Claim batch size | dynamic (up to 250) | **pinned to 10** per claim call | Predictable AlloyDB load; per-branch LIMITs (§2.3) sum to ≤ 10 per call. |

### 4.2 Shutdown sub-timeout

The 90 s shutdown budget is fragile if task-cancellation consumes it. The existing `asyncio.wait(feed_tasks, timeout=graceful_shutdown_timeout_sec)` lets one stuck task (e.g., a feed in its third GCS upload retry) eat the entire window, leaving nothing for the batched release. Add an explicit sub-timeout:

```python
await asyncio.wait(self._feed_tasks.values(), timeout=TASK_CANCEL_BUDGET_SEC)
```

with `TASK_CANCEL_BUDGET_SEC = 30`. After 30 s, still-running tasks are forcibly abandoned — their leases get released by the batched UPDATE anyway. This reserves ≥60 s for the release to run to completion.

### 4.3 Startup stagger + jitter

Three layers of randomization prevent co-activation stampedes against AlloyDB when many VMs or containers boot together (MIG scale-out, rolling deploy, zonal recovery).

**Inter-VM (cloud-init):**

```bash
sleep $(( 16#$(hostname | md5sum | head -c 8) % 60 ))
```

Each VM sleeps 0–60 s based on a deterministic hash of its hostname. The **`16#` hex prefix is non-negotiable** — without it, bash interprets the hex string as base-10, the expression silently yields 0 on every host, and the stagger collapses. (Subtle enough to pass review and fail only at scale.)

**Intra-VM:** container A starts immediately (at the inter-VM offset); container B sleeps `30 + random(0, 30)` seconds — ≥30 s decoupling between the two workers on the same VM, so their ffmpeg subprocess activation bursts never overlap.

**Pre-first-poll jitter:** every worker sleeps `random.uniform(0, 2)` before its first AlloyDB poll, desynchronizing first-poll timing across the fleet.

### 4.4 ffmpeg spawn gating

Every `asyncio.create_subprocess_exec(ffmpeg, ...)` call is gated by an `asyncio.Semaphore(N)`. Concurrent `posix_spawn` from many asyncio tasks can cause GIL contention long enough to stall the event loop. The semaphore bounds concurrency to N in-flight spawns; tune N in `{8, 12, 16, 24, 32}` during Phase 0 via 1b-replay, picking the value where event-loop p99 lag stays < 100 ms under a 1,000-feed activation burst.

### 4.5 Runtime environment

| Item | Setting | Why |
|---|---|---|
| Memory allocator | `LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libjemalloc.so.2` + `MALLOC_ARENA_MAX=2` (container entrypoint) | Long-running Python processes accumulate per-thread glibc allocator arenas that retain freed memory as process-level RSS. `MALLOC_ARENA_MAX=2` addresses the multi-arena fragmentation on multi-core hosts; jemalloc is paired as defense-in-depth. |
| TCP source port range | `net.ipv4.ip_local_port_range = 10000 65535` (cloud-init sysctl) | Default 32768–60999 can exhaust under bursty reconnects × 800 feeds × k=2. |
| Event loop | **uvloop** as asyncio policy | The 1b experiment numbers this plan relies on assume uvloop; installing it is a prerequisite to realizing those numbers in production. |
| HTTP client | shared `aiohttp.ClientSession` with `TCPConnector(limit=500, limit_per_host=0)` | Enables Keep-Alive and TLS-session reuse across polls, avoiding ~634 TLS handshakes/sec at peak on the bcfy_calls path. |

### 4.6 Dead code removal

Remove `LEASE_FEED_SQL` from `feed_queries.py` if confirmed unused at Phase 0 (the batched `ACQUIRE_FEEDS_BATCH_SQL` is the only code path in `_leasing_loop`; keeping `LEASE_FEED_SQL` around invites future drift onto the wrong path).

---

## 5. Infrastructure

Converts the single-VM deployment into a regional MIG of 2–8 VMs (ceiling 10) autoscaled by two signals. **No hardcoded "feeds per VM" constant appears anywhere in the scaling path** — fleet size responds to observed latency and CPU, so catalog or workload changes adapt the fleet without Terraform edits.

### 5.1 Regional MIG

```hcl
distribution_policy_target_shape = "EVEN"
distribution_policy_zones        = ["us-central1-a", "us-central1-b", "us-central1-c"]
```

`EVEN` forces the MIG to rebalance across zones during replacement cycles. Without it, a regional MIG distributes best-effort and can skew to 4+2+2 or worse after outage-recovery cycles — a single-zone outage could then take out 4 of 8 VMs instead of ≤ 3.

### 5.2 Autoscaler — two-signal MAX policy

Scale-out fires on the max of either signal; scale-in requires **both** under threshold.

| Signal | Target | What it catches |
|---|---|---|
| `oldest_unclaimed_feed_age` | `utilization_target = 60` seconds | Backlog: idle-fleet catalog bursts, workers falling behind. |
| CPU utilization | `utilization_target = 0.75` | Saturation: feeds getting more expensive, gradual load rise. |

Replica bounds and update policy:

| Parameter | Value | Why |
|---|---|---|
| `min_replicas` | 2 | Zonal HA minimum — a one-VM fleet is a SPOF. |
| `max_replicas` | 10 | Absorbs unexpected catalog growth, zonal replacement, and transient overloads during scale-out. |
| `initialization_period_sec` | 180 | Cloud-init + startup jitter + container warmup; health-check failures inside this window do not trigger replacement. |
| `min_ready_sec` | 60 | VM must be healthy for 60 s before counted as "ready" for update-policy decisions. |
| `max_surge` | 2 | Rolling deploy adds up to 2 new VMs before removing old ones. |
| `max_unavailable` | 1 | At most one VM in an "unavailable" state during rolling deploy. |

Neither signal assumes a feeds-per-VM constant: the 60 s target is a latency SLO (how long we are willing to let a feed sit unclaimed); the 0.75 CPU target is a headroom policy (leave 25% slack for bursts).

### 5.3 Publisher — Cloud Run Function

The `oldest_unclaimed_feed_age` signal does not exist as a GCP-native metric; it has to be computed and published. A 50-LOC Cloud Run **Function** (not a service) triggered by Cloud Scheduler every 60 s runs one query and publishes the result as a custom metric:

```sql
SELECT COALESCE(EXTRACT(epoch FROM NOW() - MIN(unclaimed_since)), 0.0)
  FROM feeds WHERE status='unclaimed';
```

Stateless; ~$0 cost (inside Cloud Run Functions free tier at 1 invocation/minute). Choosing a Function over a long-running controller-service is deliberate: no reconciliation loop, no leader election, no crash-recovery state. If the function dies, the CPU signal keeps sizing the fleet correctly until the function is redeployed.

Required error handling:

| Condition | Published value | Why |
|---|---|---|
| Empty pool | `0.0` (via `COALESCE`) | Legitimate and frequent state; "0 s of waiting" is accurate. |
| Query timeout | sentinel `-1.0` + error log | NOT `0.0` — would look like "fleet caught up" and suppress needed scale-out. |
| Connection failure | sentinel `-1.0` + error log | Same reason. |

Alert separately on `oldest_unclaimed_feed_age < 0` (absolute value) so the sentinel cannot be silently interpreted as healthy state.

### 5.4 Ramp filter

The full deployment rolls out against the production catalog in graduated stages (1% → 20% → 50% → 80% → 100%). The ramp mechanism is a Terraform-driven filter in the claim query (§2.1, §2.4): only feeds whose md5-bucketed id falls below `ramp_pct` are eligible for claim.

- Single-source-of-truth Terraform variable `ramp_pct`, **starts at 0**.
- md5-based expression — stable across PostgreSQL minor-version upgrades (see §2.1 for why `hashtext` would have been unsafe).
- Raising or rolling back `ramp_pct` is a standard rolling deploy. Disabled feeds return to the unprocessed pool within ~75 s (one sweep cycle + worker poll).

---

## 6. Deploy ordering

Several changes must land together. Order:

1. **Pooler + GUC** (§3). Independent; safe to land first. TCP keepalives and `idle_in_transaction_session_timeout` take effect only on new connections.
2. **Schema migration** (§1.1–§1.3) **+ CI guard** (§1.5) **+ claim query rewrite** (§2) **+ worker settings** (§4.1). **Must land together** — HOT requires both `fillfactor=70` and the index restructure; the claim query rewrite references `feeds_claim_by_type_idx` and the per-type-cap env vars; worker settings (including the pinned batch size of 10) must match the new claim path's per-branch LIMIT math.
3. **pg_cron jobs** (§1.4). After schema is in place.
4. **Worker runtime** (§4.2–§4.6). Can land in the same deploy as step 2 or immediately after.
5. **MIG + autoscaler + publisher** (§5.1–§5.3). After all worker/DB changes are in production at `ramp_pct=0`.
6. **Ramp** (§5.4). Graduated rollout per operational plan.

**Deploy-gate check for step 2**: the `hot_pct` metric (`n_tup_hot_upd / n_tup_upd` on `feeds`) should be `> 95%` in steady state within one hour of cutover. A drop below 90% indicates an index regression and should trigger rollback.

---

## Summary of change-list coverage

| Group | Items | Section |
|---|---|---|
| Schema & DB | 10 | §1 |
| Claim query rewrite | 7 | §2 |
| AlloyDB + pooler | 5 | §3 |
| Worker code | 11 | §4 |
| Infrastructure | 4 | §5 |
| **Total** | **37** | |

End of plan.
