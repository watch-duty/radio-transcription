# GCE MIG audio ingestion — scaling plan (final)

**Author:** Shuojing
**Date:** 2026-04-18
**Status:** Final, for review

This plan covers only the changes listed below. Deferred work is out of scope.

---

## 1. Schema & DB

### 1.1 Table storage parameters

```sql
ALTER TABLE feeds SET (
  fillfactor = 70,
  autovacuum_vacuum_scale_factor = 0.05,
  autovacuum_vacuum_cost_delay = 10
);
VACUUM FULL feeds;   -- one-time, rewrites pages with fillfactor=70 slack
```

### 1.2 Index changes

Drop the old leasing index and replace with four HOT-safe partial indexes.

| Index | Definition | Serves |
|---|---|---|
| `idx_feeds_leasing` | — | **DROP** |
| `idx_feeds_unclaimed` | `(id) WHERE status='unclaimed'` | admin paths / id-only access |
| `idx_feeds_failing_retryable` | `(retry_after) WHERE status='failing'` | recovery-path claim |
| `idx_feeds_active` | `(id) WHERE status='active'` | abandoned-lease sweep |
| `feeds_claim_by_type_idx` | `(source_type, id) WHERE status='unclaimed'` | per-type branches in primary claim CTE |

None of these indexed columns are mutated on the hot path — HOT updates remain valid.

### 1.3 New columns

```sql
ALTER TABLE feeds ADD COLUMN unclaimed_since  TIMESTAMP WITH TIME ZONE;
ALTER TABLE feeds ADD COLUMN last_progress_at TIMESTAMP WITH TIME ZONE;  -- unindexed
```

Backfill `unclaimed_since = created_at` for existing `unclaimed` rows as part of the migration. `unclaimed_since` is set by INSERT, the sweep, and the SIGTERM release path. `last_progress_at` is deliberately unindexed — progress writes target it instead of `last_heartbeat`, keeping them fully HOT-eligible.

### 1.4 Scheduled jobs (pg_cron)

**Abandoned-lease sweep — every 30 s, batched:**

```sql
UPDATE feeds
   SET status='unclaimed', worker_id=NULL, unclaimed_since=NOW()
 WHERE id IN (
     SELECT id FROM feeds
      WHERE status='active' AND last_heartbeat < NOW() - INTERVAL '60 seconds'
      LIMIT 500
 );
```

Batching prevents a single-cycle "fleet-wide polling stampede" after a zonal outage.

**Minute-cadence VACUUM — for line-pointer-array maintenance:**

```sql
SELECT cron.schedule('feeds-vac', '* * * * *', 'VACUUM (ANALYZE) feeds');
```

Opportunistic `heap_page_prune_opt` reclaims tuple bytes but does not shrink the LP array; only VACUUM does.

### 1.5 Pre-deploy CI guard

CI fails the build if any migration adds an index referencing a mutated hot-path column. Guarded column list (8): `last_heartbeat`, `unclaimed_since`, `worker_id`, `fencing_token`, `last_processed_filename`, `last_bookmark_time`, `failure_count`, `retry_after`. Allow-list exception: `idx_feeds_failing_retryable` is the only permitted index on `retry_after`.

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
-- Build fails if any row returned.
```

---

## 2. Claim query rewrite

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

- **`AS MATERIALIZED`** — without it, the planner may inline the CTE and re-evaluate the UNION ALL per outer row, bypassing the LIMITs.
- **`FOR NO KEY UPDATE`** — sufficient (claim mutates no unique keys); reduces lock-manager contention vs `FOR UPDATE`.
- **`ORDER BY id` per branch** — produces an index-only scan against `feeds_claim_by_type_idx`, no sort node.

md5 (not `hashtext`) is used in the ramp filter: `hashtext()`'s algorithm is a documented-internal function that can change across PostgreSQL minor upgrades; an AlloyDB minor-version upgrade mid-ramp would silently re-shuffle feeds between enabled and disabled buckets. md5 is documented stable.

### 2.2 Per-type caps

Shippable as env vars:

| Type | Env var | Cap | Rationale |
|---|---|---|---|
| bcfy_feeds | `cap_bcfy_feeds` | **240** | Memory-heavy (16.9 MiB/feed); cap prevents adversarial mix-variance OOM |
| bcfy_calls | `cap_bcfy_calls` | **600** | Cheap; cap set for balanced DB load |
| openmhz | `cap_openmhz` | **900** | Cheap; highest cap |

### 2.3 Per-branch LIMIT computation

Worker tracks per-type holdings (`current_held[type]`) and passes each claim cycle:

```python
remaining_budget[type] = cap[type] - current_held[type]
total_slack            = max_feeds_per_worker - sum(current_held.values())

# LIMIT for each CTE branch:
limit[type] = max(0, min(cap[type], remaining_budget[type], total_slack))
```

The total per-call row count is additionally bounded by the pinned claim batch size (§4.1). PostgreSQL enforces whatever LIMIT the worker passes; worker-side tracking is required because a per-call LIMIT cannot bound total-held alone.

### 2.4 Recovery query

Runs only when the primary CTE returns fewer rows than requested (covers `failing`-with-retry-elapsed and `active`-abandoned branches):

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

No per-type budget on the recovery path — `failing` is low-volume and `active`-abandoned is drained by the pg_cron sweep.

### 2.5 Write coalescing — drop `last_heartbeat` side-effects

Remove `last_heartbeat = NOW()` from:

- `UPDATE_PROGRESS_SQL` → write `last_progress_at = NOW()` instead.
- `RELEASE_FEEDS_BATCH_SQL` → remove entirely (release sets `status`/`worker_id`/`unclaimed_since`).
- `REPORT_FAILURE_SQL` → remove entirely.

Heartbeat renewal becomes the only remaining writer of `last_heartbeat`.

### 2.6 Skip-if-recent heartbeat predicate

```sql
UPDATE feeds
   SET last_heartbeat = NOW()
 WHERE worker_id = $1
   AND id = ANY($2)
   AND last_heartbeat < NOW() - INTERVAL '15 seconds';
```

MVCC writes zero tuples when WHERE does not match, so redundant same-window heartbeats cost nothing.

Combined with (2.5) and the cadence relax in §4.1, heartbeat UPDATE rate on `last_heartbeat` drops from ~2,000/sec to ~490/sec (170M → 42M dead tuples/day).

### 2.7 SIGTERM batched + jittered release

Replace the single atomic `RELEASE_FEEDS_BATCH_SQL` with per-batch commits of ~50 rows, 0–2 s jitter between batches:

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

Per-batch COMMIT prevents the whole release from being aborted by `idle_in_transaction_session_timeout` if AlloyDB is slow. Runs as step 3 of the existing shutdown sequence (heartbeat-off → cancel-tasks → release); step order is preserved.

---

## 3. AlloyDB + pooler config

| Setting | Location | Value |
|---|---|---|
| Pooler mode | Managed pooler | `transaction` |
| Server-side prepared statements | Managed pooler | off (or confirm pgbouncer ≥ 1.21 transaction-mode prepared-statement support) |
| `default_pool_size` (pooler backend pool per user/db) | Managed pooler | **160** (raise if currently below) |
| asyncpg main pool min/max | `storage/settings.py` | **8 / 8** (from 5 / 5) |
| asyncpg heartbeat pool | (unchanged) | 1 / 1 |
| asyncpg `connect_args` TCP keepalives | worker | `idle=60, interval=10, count=3` |
| `idle_in_transaction_session_timeout` | AlloyDB GUC | **`30s`** |

Keepalives cut dead-peer-detection from ~2 h to ~90 s, releasing row locks held by dead workers. The transaction-level GUC aborts stuck idle transactions, releasing any `FOR NO KEY UPDATE` locks held by frozen workers.

---

## 4. Worker code

### 4.1 Settings

| Setting | Current | Target |
|---|---|---|
| `max_feeds_per_worker` | 250 | **800** |
| `heartbeat_interval_sec` | 15 | **20** (1:3 ratio vs 60 s abandonment; matches kubelet NodeLease / etcd) |
| `graceful_shutdown_timeout_sec` | 10 | **90** (inside GCE's 120 s ACPI window) |
| Claim batch size | dynamic | **pinned to 10** per claim call |

### 4.2 Shutdown sub-timeout

Add `TASK_CANCEL_BUDGET_SEC = 30` to bound the task-cancellation phase explicitly:

```python
await asyncio.wait(self._feed_tasks.values(), timeout=TASK_CANCEL_BUDGET_SEC)
```

Without this, a single task stuck on a GCS retry round can consume the entire 90 s `graceful_shutdown_timeout_sec`, starving the batched release in §2.7. The 30 s budget leaves ≥60 s for the release to run.

### 4.3 Startup stagger + jitter

**Inter-VM (cloud-init):**

```bash
sleep $(( 16#$(hostname | md5sum | head -c 8) % 60 ))
```

The `16#` hex prefix is **non-negotiable** — without it, bash interprets the hex string as base-10 and the expression silently yields 0 on every host, collapsing the stagger.

**Intra-VM:** container A starts immediately (at the inter-VM offset); container B sleeps `30 + random(0, 30)` seconds.

**Pre-first-poll jitter:** every worker sleeps `random.uniform(0, 2)` before its first AlloyDB poll.

### 4.4 ffmpeg spawn gating

Gate every `asyncio.create_subprocess_exec(ffmpeg, ...)` call behind an `asyncio.Semaphore(N)`. Tune N in `{8, 12, 16, 24, 32}` during Phase 0 via 1b-replay against a 1,000-feed activation burst; pick the value where event-loop p99 lag stays < 100 ms.

### 4.5 Runtime environment

| Item | Setting |
|---|---|
| Memory allocator | `LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libjemalloc.so.2` + `MALLOC_ARENA_MAX=2` (container entrypoint) |
| TCP source port range | `net.ipv4.ip_local_port_range = 10000 65535` (cloud-init sysctl) |
| Event loop | **uvloop** as asyncio policy (1b numbers assume it) |
| HTTP client | shared `aiohttp.ClientSession` with `TCPConnector(limit=500, limit_per_host=0)` — Keep-Alive + TLS reuse |

### 4.6 Dead code removal

Remove `LEASE_FEED_SQL` from `feed_queries.py` if confirmed unused at Phase 0 (the batched `ACQUIRE_FEEDS_BATCH_SQL` is the only path in `_leasing_loop`).

---

## 5. Infrastructure

### 5.1 Regional MIG

```hcl
distribution_policy_target_shape = "EVEN"
distribution_policy_zones        = ["us-central1-a", "us-central1-b", "us-central1-c"]
```

`EVEN` forces rebalancing during replacement cycles; without it, a zonal outage can take out 4+ of 8 VMs.

### 5.2 Autoscaler — two-signal MAX policy

Scale-out fires on either signal; scale-in requires both under threshold.

| Signal | Target | Notes |
|---|---|---|
| `oldest_unclaimed_feed_age` | `utilization_target = 60` seconds | Backlog / surge detector |
| CPU utilization | `utilization_target = 0.75` | Saturation detector (GCP-native) |

Replica bounds and update policy:

| Parameter | Value |
|---|---|
| `min_replicas` | 2 |
| `max_replicas` | 10 |
| `initialization_period_sec` | 180 |
| `min_ready_sec` | 60 |
| `max_surge` | 2 |
| `max_unavailable` | 1 |

### 5.3 Publisher — Cloud Run Function

50-LOC function triggered by Cloud Scheduler every 60 s. Publishes `oldest_unclaimed_feed_age` as a custom metric.

```sql
SELECT COALESCE(EXTRACT(epoch FROM NOW() - MIN(unclaimed_since)), 0.0)
  FROM feeds WHERE status='unclaimed';
```

Required error handling:

| Condition | Published value |
|---|---|
| Empty pool | `0.0` (via `COALESCE`) |
| Query timeout | sentinel `-1.0` + error log |
| Connection failure | sentinel `-1.0` + error log |

Alert separately on `oldest_unclaimed_feed_age < 0` (publisher misbehaving) so the sentinel is not silently interpreted as "fleet caught up."

### 5.4 Ramp filter

Single-source-of-truth Terraform variable `ramp_pct`, starts at **0**. Workers filter claims by `(('x'||substr(md5(id::text),1,7))::bit(28)::integer) % 100 < :ramp_pct` (applied in both the primary CTE and the recovery query above — §2.1, §2.4). Ramp up/down is a standard rolling deploy of the Terraform variable.

---

## Summary of change-list coverage

| Category | Items | Section |
|---|---|---|
| Schema & DB | 10 | §1 |
| Claim query rewrite | 7 | §2 |
| AlloyDB + pooler | 5 | §3 |
| Worker code | 11 | §4 |
| Infrastructure | 4 | §5 |
| **Total** | **37** | |

End of plan.
