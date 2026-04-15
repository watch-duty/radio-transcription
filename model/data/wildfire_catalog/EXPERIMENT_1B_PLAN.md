# Experiment 1b (v2) — Stream-Copy Ingestion Capacity Test

**Status:** Ready to execute after Step 0 prerequisites
**Derived from:** Original "Experiment 1b" plan + all 9 items from [`EXPERIMENT_1B_REVIEW.md`](./EXPERIMENT_1B_REVIEW.md)
**Companion docs:** [`FINDINGS.md`](./FINDINGS.md), [`SCALING_PLAN_REVIEW.md`](./SCALING_PLAN_REVIEW.md)
**Date:** 2026-04-12

## Goal

Measure how many audio feeds a single n2-standard-4 VM (4 vCPU, 16 GiB) can sustain in **stream-copy / no-conversion mode** — the proposed architecture for horizontal scale-out. Produce a per-source-type resource profile (CPU, RSS, event-loop latency) that lets us size the MIG fleet for any feed composition, at any scope (6.5K, 10K, or 12K).

## What changed from v1 (the original plan)

Every fix from the review applied:

| Review item | Fix in this plan |
|---|---|
| Stream-copy + FLAC contradiction | Committed to **Path A (MP3 output)**; downstream format change is now a hard Step 0 prerequisite |
| Feeds-table not populated | New **Step 0.1** bulk-inserts Tier 1+2 catalog as `deactivated`; Step 3 activates feeds per-step via SQL (no bulk pre-activation) |
| Pricing error ($98 vs $141) | Corrected: $141/VM/mo on-demand |
| `/healthz` endpoint missing | Dropped curl fallback; use **Python-side event-loop monitor only** |
| Lease doesn't control ratio | **DB-side activation**: each ramp step flips additional feeds to `unclaimed` in the 41/55/3 ratio via one SQL UPDATE. Zero Python changes. |
| Prod AlloyDB load unmonitored | Step 2.2 operator checks AlloyDB Cloud Console + Step 2.3 periodic production-health query |
| Experiment cost not estimated | **$250–650 budget** itemized |
| `DISABLE_PUBSUB` flag unverified | Marked as a code change in Step 0.2 (Change 4), not an assumption |
| Branch strategy ambiguous | Fresh `experiment/1b-stream-copy` off `main` |
| (New) Bulk pre-activation blast radius | Step 0.4 no longer flips all 11,473 feeds — feeds stay `deactivated` until Step 3 activates them per-step |
| (New) Scripted AlloyDB watcher was wrong | Replaced with Cloud Console + human operator at fixed intervals |
| (New) Cleanup required "original feed IDs" | Step 6.2 filters by `created_at >= '${EXP_INSERT_TS}'` |
| (New) Production health unmonitored | Step 2.3 checks prod active-feed count every 15 min, aborts on >20% drop from baseline |

---

## Hard prerequisites (block the experiment)

### P1. Transcription-team sign-off on MP3/M4A input

Stream-copy mode changes downstream format: bcfy_feeds outputs MP3 (not FLAC), bcfy_calls stores MP3 as-is, openmhz stores M4A as-is. Transcription must confirm one of:

- **(a)** Whisper/whatever-STT accepts MP3 and M4A directly without loss of quality;
- **(b)** A re-encode step is inserted downstream (adds CPU cost on a different VM — net savings must still be positive);
- **(c)** We accept this experiment measures an architecture that requires a follow-up transcription change before production shipping.

**Document the answer before provisioning VMs.** Without this, we're measuring a dead-end.

### P2. SIGTERM lease-release behavior

When the experiment VM is deleted, leased feeds must return to `unclaimed` quickly or they sit idle for 60 seconds (stale-heartbeat window) before prod workers pick them up. Verify in `backend/pipeline/ingestion/normalizer_runtime.py` signal-handling code:

- If SIGTERM → `release_all_leased_feeds()` is wired up, proceed.
- If not, add it before running. Roughly: `async def _on_sigterm(self): for f in self._leased: await self._store.release_feed(...)`.

### P3. Service-account IAM roles

The experiment VM SA needs (on the experiment-specific resources):

| Role | On |
|---|---|
| `roles/alloydb.client` | Prod AlloyDB cluster (reused from production SA) |
| `roles/storage.objectCreator` | Test bucket only (not prod) |
| `roles/pubsub.publisher` | Test topic only |
| `roles/secretmanager.secretAccessor` | Broadcastify JWT secrets (reused from prod SA) |
| `roles/monitoring.metricWriter` | Project (for custom metrics) |

Reuse the production ingestion SA and grant the bucket/topic roles on the new resources.

---

## Step 0 — Prep work (days 1–3, no scale impact)

### 0.1 Populate feeds table with Tier 1+2 catalog

The experiment needs ~2,000 leasable feeds in the 41/55/3 mix, activated progressively via SQL at each ramp step (see Step 3). Production currently runs ~250. Bulk-insert the Tier 1+2 catalog with `status = 'deactivated'`:

```python
# one-off script, reads output/wildfire_feed_catalog_admin_review.csv
# (filter source != 'echo' since Echo is on Cloud Run)
# INSERT INTO feeds (name, source_type, status, source_feed_id, ...)
# VALUES (..., 'deactivated', ...) for each row
# ~11,473 rows total; at 100 rows/sec = ~2 minutes
```

**Capture the insert timestamp** immediately after the bulk insert completes — Step 6.2's cleanup uses it as a filter:

```bash
# Just after the bulk-insert script finishes, record the boundary.
# Shave off a second to be safely inclusive of any late-arriving rows.
EXP_INSERT_TS=$(date -u -d '2 seconds ago' +%Y-%m-%dT%H:%M:%SZ)
echo "EXP_INSERT_TS=${EXP_INSERT_TS}" | tee -a experiment_1b.env
# Also cross-check against the DB:
psql -c "SELECT MIN(created_at), MAX(created_at), COUNT(*)
         FROM feeds
         WHERE status = 'deactivated'
           AND created_at >= '${EXP_INSERT_TS}';"
```

Schema target columns (from `terraform/modules/alloydb/sql/ingestion/003_feeds.sql`):
- `id`: UUID (generate)
- `name`: from catalog's `feed_name`
- `source_type`: 'bcfy_feeds' | 'bcfy_calls' | 'openmhz'
- `status`: **'deactivated'** (important — starts dormant)
- `source_feed_id` in `feed_properties` side table

Verify the load worked:

```sql
SELECT source_type, status, COUNT(*)
FROM feeds
WHERE source_type IN ('bcfy_feeds','bcfy_calls','openmhz')
GROUP BY source_type, status;
```

Expected:
- bcfy_feeds / deactivated: **~4,757**
- bcfy_calls / deactivated: **~6,335**
- openmhz / deactivated: **~381**

Plus whatever's currently active from the production ~250.

### 0.2 Code changes on `experiment/1b-stream-copy` branch

Branch off the current `main`:

```bash
git fetch origin
git checkout -b experiment/1b-stream-copy origin/main
```

**Change 1 — bcfy_feeds ffmpeg stream-copy** (`backend/pipeline/ingestion/collectors/icecast_collector.py:289-314`):

Replace the FLAC-encoding flags with stream-copy:

```diff
-        "-vn", "-sn", "-dn",
-        "-acodec", AUDIO_FORMAT,     # drop — was 'flac'
-        "-ar", str(SAMPLE_RATE_HZ),  # drop — was '16000'
-        "-sample_fmt", SAMPLE_FORMAT, # drop — was 's16'
-        "-ac", str(NUM_AUDIO_CHANNELS), # drop — was '1'
-        "-compression_level", "0",   # drop
+        "-vn", "-sn", "-dn",
+        "-c:a", "copy",               # stream-copy MP3 input to MP3 output
         "-f", "segment",
         "-segment_time", str(CHUNK_DURATION_SECONDS),
-        "-segment_format", AUDIO_FORMAT,  # change 'flac' → 'mp3'
+        "-segment_format", "mp3",
```

Update `segment_pattern` to produce `.mp3` extension (e.g., `.../chunk_%06d.mp3`).

**Change 2 — bcfy_calls skip FLAC conversion** (`backend/pipeline/ingestion/collectors/bcfy_calls/bcfy_calls_collector.py`):

Find the `convert_to_flac(mp3_bytes, "mp3")` call (grep for `convert_to_flac`). Replace the yield:

```diff
-        flac_bytes = await asyncio.to_thread(convert_to_flac, mp3_bytes, "mp3")
-        yield CapturedChunk(audio_bytes=flac_bytes, ..., extension="flac")
+        yield CapturedChunk(audio_bytes=mp3_bytes, ..., extension="mp3")
```

If `extension` isn't a `CapturedChunk` field today, add one and make the normalizer use it for the GCS object suffix.

**Change 3 — openmhz skip FLAC conversion** (`backend/pipeline/ingestion/collectors/openmhz/collector.py:146-148`):

Same pattern as bcfy_calls, but input is M4A:

```diff
-        flac_bytes = await asyncio.to_thread(convert_to_flac, m4a_bytes, "m4a")
-        yield CapturedChunk(audio_bytes=flac_bytes, ..., extension="flac")
+        yield CapturedChunk(audio_bytes=m4a_bytes, ..., extension="m4a")
```

**Change 4 — `DISABLE_PUBSUB` env flag** (`backend/pipeline/common/gcp_helper.py` or wherever `publish_audio_chunk` lives):

```diff
 async def publish_audio_chunk(...):
+    if os.environ.get("DISABLE_PUBSUB", "").lower() == "true":
+        return "disabled-via-env"
     # existing publish logic
```

**Change 5 — add event-loop health monitor** (`backend/pipeline/ingestion/event_loop_monitor.py`, new file):

```python
"""Asyncio task that logs event-loop responsiveness every N seconds."""
import asyncio, json, sys, time

async def monitor_event_loop(interval_s: float = 10.0) -> None:
    while True:
        t0 = time.monotonic()
        await asyncio.sleep(0)  # yield immediately — healthy loop returns in <1ms
        loop_latency_ms = (time.monotonic() - t0) * 1000

        t1 = time.monotonic()
        await asyncio.sleep(interval_s)
        actual = time.monotonic() - t1

        print(json.dumps({
            "type": "event_loop_health",
            "loop_latency_ms": round(loop_latency_ms, 2),
            "drift_ms": round((actual - interval_s) * 1000, 2),
        }), file=sys.stderr)
```

Wire into `normalizer_runtime.py`'s async startup path as a background task. Drop it completely on exit.

Commit:

```bash
git add -A
git commit -m "experiment/1b: stream-copy + file-copy mode for capacity testing

- bcfy_feeds: ffmpeg -c copy (MP3 in, MP3 out; no transcoding)
- bcfy_calls: skip FLAC conversion, store downloaded MP3 as-is
- openmhz: skip FLAC conversion, store downloaded M4A as-is
- Add DISABLE_PUBSUB env flag
- Add event_loop_monitor asyncio task"

git push -u origin experiment/1b-stream-copy
```

**Do not merge to main.** This branch exists only for the experiment.

### 0.3 Provision isolated infrastructure

```bash
# Test GCS bucket with 7-day lifecycle
BUCKET="audio-experiment-1b-$(date +%Y%m%d)"
gcloud storage buckets create gs://${BUCKET} \
    --location=us-central1 \
    --uniform-bucket-level-access
gcloud storage buckets update gs://${BUCKET} \
    --lifecycle-file=<(echo '{"rule":[{"action":{"type":"Delete"},"condition":{"age":7}}]}')

# Test Pub/Sub topic (no subscriptions)
gcloud pubsub topics create audio-experiment-1b-sink

# Grant experiment SA write access to both
SA="<INGESTION_SA_EMAIL>"
gcloud storage buckets add-iam-policy-binding gs://${BUCKET} \
    --member="serviceAccount:${SA}" --role="roles/storage.objectCreator"
gcloud pubsub topics add-iam-policy-binding audio-experiment-1b-sink \
    --member="serviceAccount:${SA}" --role="roles/pubsub.publisher"
```

### 0.4 Capture production health baseline

Before any feeds are activated, record the production VM's active-feed count — Step 2.3 monitors it during the experiment and aborts on a >20% drop.

```bash
# Find the production worker_id (the e2-small VM has ~250 active leases)
PROD_WORKER_ID=$(psql -t -c "
    SELECT worker_id FROM feeds
    WHERE status = 'active'::feed_status
    GROUP BY worker_id
    ORDER BY COUNT(*) DESC
    LIMIT 1;
" | tr -d ' ')
echo "PROD_WORKER_ID=${PROD_WORKER_ID}" | tee -a experiment_1b.env

PROD_BASELINE=$(psql -t -c "
    SELECT COUNT(*) FROM feeds
    WHERE worker_id = '${PROD_WORKER_ID}'
      AND status = 'active'::feed_status;
" | tr -d ' ')
echo "PROD_BASELINE=${PROD_BASELINE}" | tee -a experiment_1b.env
```

**No feeds are flipped to `unclaimed` at this point.** Feeds stay `deactivated` until Step 3 activates them per-step in the 41/55/3 ratio. Blast radius is capped at ~2,000 activated feeds at peak (Step 8), not the full 11,473 Tier 1+2.

---

## Step 1 — Provision experiment VM

```bash
gcloud compute instances create experiment-1b \
    --machine-type=n2-standard-4 \
    --zone=us-central1-a \
    --image-family=debian-12 \
    --image-project=debian-cloud \
    --boot-disk-size=20GB \
    --scopes=cloud-platform \
    --service-account=<INGESTION_SA_EMAIL> \
    --metadata=startup-script='#!/bin/bash
      echo "fs.file-max = 1000000" >> /etc/sysctl.conf
      echo "net.ipv4.tcp_keepalive_time = 60" >> /etc/sysctl.conf
      echo "net.ipv4.tcp_keepalive_intvl = 10" >> /etc/sysctl.conf
      echo "net.ipv4.tcp_keepalive_probes = 6" >> /etc/sysctl.conf
      sysctl -p
      ulimit -n 65535
      apt-get update && apt-get install -y libjemalloc2
    '
```

SSH in and deploy:

```bash
gcloud compute ssh experiment-1b --zone=us-central1-a

# On the VM:
git clone <repo> && cd radio-transcription
git checkout experiment/1b-stream-copy
uv sync  # or whatever the project uses

export GCS_BUCKET="audio-experiment-1b-YYYYMMDD"
export PUBSUB_TOPIC="audio-experiment-1b-sink"
export DISABLE_PUBSUB="true"
export MAX_FEEDS_PER_WORKER="2000"   # set ONCE — ramp is DB-driven
export LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libjemalloc.so.2
# AlloyDB env vars: same as production SA already scoped via IAM
```

Setting `MAX_FEEDS_PER_WORKER=2000` once at startup is the DB-side-activation corollary: the VM is ready to hold up to 2,000 feeds, but only leases what's `unclaimed` in the feeds table. Step 3 controls the ramp by flipping feeds to `unclaimed` in stages — **no process restarts between ramp steps are needed.**

---

## Step 2 — Measurement scripts

### 2.1 System resource sampler (`measure.sh`)

```bash
#!/bin/bash
# ./measure.sh <ramp_step> | tee -a experiment_1b_results.tsv
RAMP_STEP=$1
INTERVAL=30

echo -e "timestamp\tramp_step\tffmpeg_count\tffmpeg_cpu_pct\tffmpeg_rss_mb\tpython_cpu_pct\tpython_rss_mb\ttotal_used_mb\tfd_count\tload_avg_1m"

while true; do
    TS=$(date -u +%Y-%m-%dT%H:%M:%SZ)
    FFMPEG_COUNT=$(pgrep -c ffmpeg 2>/dev/null || echo 0)
    FFMPEG_CPU=$(ps -C ffmpeg -o %cpu --no-headers 2>/dev/null | awk '{sum+=$1} END {printf "%.1f", sum}')
    FFMPEG_RSS=$(ps -C ffmpeg -o rss --no-headers 2>/dev/null | awk '{sum+=$1} END {printf "%.1f", sum/1024}')
    PY_CPU=$(ps -C python3 -o %cpu --no-headers 2>/dev/null | awk '{sum+=$1} END {printf "%.1f", sum}')
    PY_RSS=$(ps -C python3 -o rss --no-headers 2>/dev/null | awk '{sum+=$1} END {printf "%.1f", sum/1024}')
    TOTAL_USED=$(awk '/MemTotal/{t=$2} /MemAvailable/{a=$2} END {printf "%.1f", (t-a)/1024}' /proc/meminfo)
    FD_COUNT=$(cat /proc/sys/fs/file-nr | awk '{print $1}')
    LOAD=$(awk '{print $1}' /proc/loadavg)
    echo -e "${TS}\t${RAMP_STEP}\t${FFMPEG_COUNT}\t${FFMPEG_CPU}\t${FFMPEG_RSS}\t${PY_CPU}\t${PY_RSS}\t${TOTAL_USED}\t${FD_COUNT}\t${LOAD}"
    sleep $INTERVAL
done
```

### 2.2 AlloyDB monitoring — operator-driven, Cloud Console

The abort decision involves human judgment (transient spike vs sustained regression), so this is intentionally not scripted. During each 2-hour ramp step, an operator checks the **AlloyDB Cloud Console** at minute 10 / 30 / 60 / 90. During the 72-hour soak, check at least once per shift.

Look at:

| Metric | Where | Abort threshold |
|---|---|---|
| Instance CPU utilization | AlloyDB Instance → Overview | Sustained > 70% for ≥ 5 min |
| p99 transaction latency | AlloyDB Query Insights → Load | Sustained > 20 ms for ≥ 5 min |
| pgBouncer `cl_waiting` (per pool) | `psql -h <POOLER_IP> -p 6432 -U admin -d pgbouncer -c "SHOW POOLS"` | > 0 on any pool, sustained for 5 min |

If any threshold trips, invoke the abort sequence (Step 6.1) and then triage. The same operator also runs the per-step measurement and owns the abort call.

### 2.3 Production health sanity check

Every 15 minutes during ramp + soak, confirm the production VM's leased-feed count hasn't degraded. If it falls > 20% below the baseline captured in Step 0.4, the experiment is somehow starving prod — abort.

```bash
# Run on your workstation every 15 min, or put in a cron:
CURRENT=$(psql -t -c "
    SELECT COUNT(*) FROM feeds
    WHERE worker_id = '${PROD_WORKER_ID}'
      AND status = 'active'::feed_status;
" | tr -d ' ')
THRESHOLD=$(( PROD_BASELINE * 80 / 100 ))
if (( CURRENT < THRESHOLD )); then
    echo "ABORT: prod active feeds ${CURRENT} < 80% of baseline ${PROD_BASELINE}"
fi
```

This catches failure modes that AlloyDB CPU alone wouldn't surface — e.g., if prod's lease-acquire calls start losing races to the experiment VM.

### 2.4 Event-loop latency

Python-side monitor already runs as a background task (from Step 0.2 Change 5). Its output goes to stderr; capture via:

```bash
journalctl -u ingestion-service -f | grep event_loop_health > event_loop.jsonl
```

---

## Step 3 — Ramp (DB-side activation)

**Ratio control is done at the database, not in Python.** Each ramp step is one SQL UPDATE that flips additional feeds from `deactivated` to `unclaimed` in the 41.4 / 55.2 / 3.3 % mix. The existing lease loop (`backend/pipeline/ingestion/normalizer_runtime.py:229-243`, using `acquire_feeds_batch`) picks them up on its next poll (~5s cadence). **No process restarts between ramp steps.** No Python code changes.

### Cumulative activation by step (pre-computed)

Targets and the per-step additions to reach them:

| Step | Target | bcfy_feeds | bcfy_calls | openmhz | Additions from prev step |
|---:|---:|---:|---:|---:|---|
| 1 | 100 | 41 | 55 | 4 | +41 / +55 / +4 |
| 2 | 250 | 103 | 138 | 9 | +62 / +83 / +5 |
| 3 | 500 | 207 | 276 | 17 | +104 / +138 / +8 |
| 4 | 750 | 311 | 414 | 25 | +104 / +138 / +8 |
| 5 | 1,000 | 414 | 552 | 34 | +103 / +138 / +9 |
| 6 | 1,250 | 518 | 690 | 42 | +104 / +138 / +8 |
| 7 | 1,500 | 621 | 828 | 51 | +103 / +138 / +9 |
| 8 | 2,000 | 828 | 1,104 | 68 | +207 / +276 / +17 |

### SQL template for each ramp step

Substitute `<N_BCFY>`, `<N_CALLS>`, `<N_OMHZ>` with the additions-from-prev-step values:

```sql
WITH to_activate AS (
  (SELECT id FROM feeds
   WHERE source_type = 'bcfy_feeds'
     AND status = 'deactivated'::feed_status
     AND created_at >= '${EXP_INSERT_TS}'
   ORDER BY id
   LIMIT <N_BCFY>)
  UNION ALL
  (SELECT id FROM feeds
   WHERE source_type = 'bcfy_calls'
     AND status = 'deactivated'::feed_status
     AND created_at >= '${EXP_INSERT_TS}'
   ORDER BY id
   LIMIT <N_CALLS>)
  UNION ALL
  (SELECT id FROM feeds
   WHERE source_type = 'openmhz'
     AND status = 'deactivated'::feed_status
     AND created_at >= '${EXP_INSERT_TS}'
   ORDER BY id
   LIMIT <N_OMHZ>)
)
UPDATE feeds
SET status = 'unclaimed'::feed_status
WHERE id IN (SELECT id FROM to_activate);
```

The `created_at >= '${EXP_INSERT_TS}'` clause ensures we only activate feeds this experiment inserted, never production's original feeds.

### At each ramp step

1. **Activate:** run the SQL above with the correct `<N_BCFY>` / `<N_CALLS>` / `<N_OMHZ>` values
2. **Wait ~10 s** for the lease loop to pick up the newly-`unclaimed` feeds (`LEASE_POLL_INTERVAL_SEC = 5s` default)
3. **Wait 5 min** for all feeds to connect and reach steady state
4. **Start measurement:** `./measure.sh <step> | tee -a experiment_1b_results.tsv`
5. **Run for 2 hours**
6. **Record actual feed composition** — confirm the lease picked the expected ~41/55/3 mix:
   ```sql
   SELECT source_type, COUNT(*)
   FROM feeds
   WHERE worker_id = '<experiment_worker_id>'
     AND status = 'active'::feed_status
   GROUP BY source_type;
   ```
7. **Note per-source-type resource breakdown** (see 5.2 for the analysis method):
   - `ffmpeg_count` ≈ bcfy_feeds count (1 ffmpeg per bcfy_feeds feed under stream-copy)
   - Python RSS growth from step N-1 to N = memory consumed by newly-leased feeds
8. **Check stop criteria** before proceeding to the next activation

### Stop criteria (per-step)

| Condition | Threshold | Action |
|---|---|---|
| Experiment VM CPU | sustained > 75% for 5 min | Stop ramp at current step |
| Experiment VM memory | > 85% of 16 GiB for 2 min | Stop ramp at current step |
| Event-loop p95 latency | > 100 ms for 5 min | Stop ramp at current step |
| AlloyDB signals | per Step 2.2 | Abort experiment |
| Prod health drop | per Step 2.3 (>20% below baseline) | Abort experiment |

---

## Step 4 — 72-hour soak at max stable count

After the highest-succeeded step, hold at that feed count for 72 hours. Track RSS drift specifically:

```bash
# Sample every 5 minutes for 72 hours
while true; do
    echo "$(date -u +%Y-%m-%dT%H:%M:%SZ) ffmpeg_rss=$(ps -C ffmpeg -o rss --no-headers | awk '{sum+=$1} END {print sum/1024}') python_rss=$(ps -C python3 -o rss --no-headers | awk '{sum+=$1} END {print sum/1024}')"
    sleep 300
done | tee -a experiment_1b_rss_drift.tsv
```

Watch for: RSS drift > 10% over 72h, feed drops/re-leases, GCS 429/5xx errors, AlloyDB `cl_waiting`.

**Optional A/B:** run 36h with `LD_PRELOAD` jemalloc and 36h without to quantify fragmentation impact.

---

## Step 5 — Deliverables

### 5.1 Per-step summary table

| Step | Feeds | bcfy_feeds | bcfy_calls | openmhz | ffmpeg count | Avg CPU % | Avg mem used (MiB) | P95 loop latency (ms) | P95 GCS upload (ms) | Stable? |
|---|---|---|---|---|---|---|---|---|---|---|

### 5.2 Per-source-type resource profile (key deliverable)

Compute step-over-step deltas where only one source's count changed meaningfully:

| Source | Per-feed CPU % | Per-feed RSS (MiB) | Per-feed network (kbps) | Has ffmpeg subprocess? |
|---|---|---|---|---|
| bcfy_feeds | ? | ? | ? | Yes |
| bcfy_calls | ? | ? | ? | No |
| openmhz | ? | ? | ? | No |

### 5.3 Derived fleet sizing

For both scopes:

- Max stable feeds/VM at 80% autoscaler target: `max_stable × 0.8`
- Weighted capacity for production mix:
  - Total memory = `(bcfy_feeds_count × bcfy_feeds_MiB) + (bcfy_calls_count × bcfy_calls_MiB) + (openmhz_count × openmhz_MiB)`
  - Total CPU = same formula with per-source CPU% × vCPU
  - VMs needed = `max(total_memory_need / 14 GiB, total_CPU_need / 4 vCPU) / 0.8`
- Monthly cost estimate:
  - `VMs × $141/mo` (on-demand)
  - `VMs × $97/mo` (1-year CUD)
  - `VMs × $69/mo` (3-year CUD)

### 5.4 72-hour soak results

- RSS at t=0, t=24h, t=48h, t=72h
- RSS drift percentage
- Feed drops / GCS errors / event-loop spikes during soak
- jemalloc impact (if A/B tested)

### 5.5 Recommendation

State explicitly:
1. Per-source-type profile (CPU and RSS)
2. Binding constraint at the max stable point (CPU or memory)
3. Max feeds per n2-standard-4 at production mix
4. VMs needed at 6.5K and 12K scopes
5. Memory fragmentation concern (yes/no based on 72h drift)
6. Monthly compute cost at both scopes

---

## Step 6 — Cleanup

### 6.1 Release experiment VM's feeds quickly

```sql
-- Force-release feeds held by the experiment worker
UPDATE feeds
SET status = 'unclaimed'::feed_status,
    worker_id = NULL,
    last_heartbeat = NULL
WHERE worker_id = '<experiment_vm_worker_id>';
```

### 6.2 Return experiment-inserted feeds to `deactivated`

Filter by `created_at >= '${EXP_INSERT_TS}'` — the timestamp captured in Step 0.1 — so production's original feeds are untouched. No need to snapshot "original production feed IDs" ahead of time.

```sql
-- Leave production's original feeds intact; only deactivate what Step 0.1 inserted
UPDATE feeds
SET status = 'deactivated'::feed_status,
    worker_id = NULL,
    last_heartbeat = NULL
WHERE source_type IN ('bcfy_feeds','bcfy_calls','openmhz')
  AND created_at >= '${EXP_INSERT_TS}';
```

Optionally — if the team wants the Tier 1+2 catalog retained for future experiments — leave the rows in the feeds table. Otherwise, delete them after deactivation:

```sql
DELETE FROM feeds
WHERE source_type IN ('bcfy_feeds','bcfy_calls','openmhz')
  AND created_at >= '${EXP_INSERT_TS}'
  AND status = 'deactivated'::feed_status;
```

### 6.3 Delete infrastructure

```bash
gcloud compute instances delete experiment-1b --zone=us-central1-a --quiet
gcloud storage rm -r gs://audio-experiment-1b-YYYYMMDD
gcloud pubsub topics delete audio-experiment-1b-sink
# Branch stays on remote for reference; don't merge to main
```

---

## Budget

| Cost driver | Estimate |
|---|---:|
| VM runtime (16h ramp + 72h soak = 88h) | ~$17 |
| GCS Class A during ramp (~200–500 PUTs/sec × 16h) | $60–180 |
| GCS Class A during soak (~100–300/sec × 72h) | $130–390 |
| GCS storage (7-day lifecycle, ~10 TB peak) | ~$40 |
| Pub/Sub (with `DISABLE_PUBSUB=true`) | ~$0 |
| AlloyDB impact | $0 (already provisioned) |
| **Total** | **$250–650** |

---

## Abort criteria (pre-commit to these)

| Signal | Threshold | Action |
|---|---|---|
| Experiment VM CPU (all cores) | sustained > 75% for 5 min | Stop ramp at current step |
| Experiment VM memory | > 85% of 16 GiB for 2 min | Stop ramp at current step |
| Python asyncio event-loop latency | p95 > 100ms for 5 min | Stop ramp at current step |
| Prod AlloyDB CPU | sustained > 70% for 5 min | Abort experiment, force-release feeds |
| Prod AlloyDB p99 query latency | sustained > 20ms for 5 min | Abort experiment, force-release feeds |
| pgBouncer `cl_waiting` | > 0 for 5 min on any pool | Abort experiment |
| **Production active-feed count** | **> 20% drop from `PROD_BASELINE` (Step 0.4)** | **Abort experiment, force-release feeds** |
| GCS upload error rate | > 1% 5xx/429 | Pause, investigate before continuing |
| Any ffmpeg crashes | > 1% of ffmpeg processes in any 10-min window | Pause, investigate |

---

## What we'll know after this experiment

- Max feeds/VM for each source type individually
- Whether the binding constraint at n2-standard-4 is CPU (n2-highcpu VMs are better) or memory (n2-standard is right)
- Whether memory fragmentation requires jemalloc
- Fleet size for 6.5K and 12K deployment scopes
- Monthly compute cost at both scopes, on-demand and with CUD
- Whether the production AlloyDB handles ~11× today's write load without regression

All directly feeds the Phase-1/2/3 migration plan in the scaling plan.

---

## What we *won't* know (and still need separately)

- Whether stream-copy to MP3 is production-acceptable (transcription team owns this — P1 above)
- OpenMHZ Cloudflare reconnect-stampede behavior (needs Experiment 3 from the scaling plan)
- GCS sustained PUT rate beyond ~500/sec (needs Experiment 3 from scaling plan)
- Broadcastify `/calls/v1/live/` rate limit at 634 polls/sec (needs Experiment 8 from scaling plan)

These are orthogonal — can run in parallel or after Experiment 1b.
