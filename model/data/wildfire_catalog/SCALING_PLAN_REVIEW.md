# Scaling Plan Review

**Reviewer:** Data / Ingestion team
**Reviewing:** "Audio ingestion pipeline: scaling to 6,575 feeds on GCP"
**Date:** 2026-04-12
**Companion doc:** [`FINDINGS.md`](./FINDINGS.md) (catalog-side view)

## TL;DR — the shape is right, but three load-bearing claims don't survive code review

- ✅ **The architectural direction is sound.** Horizontal compute, MIG autoscaling on a custom feed-count metric, keeping AlloyDB at 2 vCPU, external IPs over Cloud NAT — all of these are correct calls.
- ❌ **Three claims fail verification against the repo** and they cascade into the sizing math: (1) ffmpeg is **not** stream-copy, it's real transcoding; (2) multi-VM feed coordination is **already built**, not a Phase-1 prerequisite; (3) the current compute tier is **e2-small** not e2-medium.
- ⚠ **The cost model has two math errors** that affect sticker price: Cloud NAT savings are ~$1,600/mo (not $5,800), and AlloyDB is regional/HA by default (~$455/mo, not zonal $227).
- ⚠ **The plan pre-dates the latest findings.** It targets 6,575 feeds with `bcfy_calls = 0`; our current recommended deployment is **12,027 feeds (all Tier 1+2)** with `bcfy_calls = 6,335`. A scale-up pass is needed before green-lighting.
- ✅ **Recommend approving with corrections** — the substantive analysis holds after corrections. No architectural rewrite required.

---

## 1. Critical errors (load-bearing, change conclusions)

### 1.1 ❌ The ffmpeg is transcoding, not stream-copying

The plan asserts:

> "Stream-copy mode makes CPU nearly irrelevant" … "stream-copy ffmpeg uses ~0.1% CPU per feed"

**False.** The actual ffmpeg command in `backend/pipeline/ingestion/collectors/icecast_collector.py:289-314` performs a full decode/resample/re-encode cycle:

```
ffmpeg -nostdin
  -reconnect 1 ...
  -i {url}
  -vn -sn -dn
  -acodec flac             # ← encoder, not 'copy'
  -ar 16000                # resampling to 16 kHz
  -ac 1                    # downmix to mono
  -sample_fmt s16          # 16-bit samples
  -compression_level 0     # FLAC encode (cheapest setting, still real work)
  -f segment
  -segment_time 15
  -segment_format flac
  ...
```

Per-feed, ffmpeg: (a) decodes MP3 input from Icecast, (b) resamples to 16 kHz mono, (c) encodes FLAC at compression_level=0. All three are CPU-bound. The "~0.1% CPU per feed" number in the plan doesn't match this workload.

**Downstream implications:**
- "Memory is the binding constraint" is only partially true. CPU may bind at a lower feed-per-VM count than memory does.
- "n2-standard-4 at 500 feeds is the sweet spot" becomes an assumption, not a conclusion. Experiment 1 must measure **per-feed CPU alongside RSS**, and set the scale-out threshold to whichever saturates first.
- If CPU is the real constraint, n2-highcpu-4 or n2-highcpu-8 instances may be more cost-efficient than n2-standard-4.

**What to fix in the plan:**
- Remove the stream-copy framing.
- Rewrite Experiment 1: target = {feeds/VM, per-feed CPU, per-feed RSS, total p99 event-loop latency}. Fail criterion: either CPU > 75% or memory > 85% or event-loop lag > 100ms, whichever hits first.
- Acknowledge that n2-highcpu-* may be a better VM family if CPU is the binding constraint.
- CUD commitment should wait until Experiment 1 confirms the VM family.

---

### 1.2 ❌ Multi-VM feed coordination is already built in the codebase

The plan's Open Question §9 states:

> "The current architecture uses a single worker. A multi-VM deployment needs either a centralized coordinator (AlloyDB-backed lease table), consistent hashing, or an external scheduler. **This coordination mechanism is not addressed in the current codebase and is a prerequisite for Phase 1.**"

**False.** The codebase has a complete lease/fencing system for multi-worker coordination.

Evidence from `backend/pipeline/storage/feed_queries.py`:

- **Atomic leasing via `SELECT FOR UPDATE SKIP LOCKED`** (`LEASE_FEED_SQL:18`):
  ```sql
  ORDER BY (status = 'unclaimed'::feed_status) DESC, ...
  LIMIT 1
  FOR UPDATE SKIP LOCKED
  ```
- **Per-row worker ownership** (`LEASE_FEED_SQL:22`): `SET worker_id = $1`.
- **Fencing tokens incremented on lease** (`:26`: `fencing_token = fencing_token + 1`) and **validated on every write** (`UPDATE_PROGRESS_SQL:46`: `WHERE id = $2 AND worker_id = $3 AND fencing_token = $4`).
- **Stale-lease handoff after 60s**: `LEASE_FEED_SQL:10-11` matches `status = 'active'::feed_status AND last_heartbeat < NOW() - INTERVAL '60 seconds'`, so a dead worker's feeds are automatically pickable by another worker.
- **Batch heartbeat renewal** runs every 15s in a dedicated thread (`normalizer_runtime.py:495-551`), refreshing `last_heartbeat` for all leased feeds in one query.

**Downstream implications:**
- Phase 1 can start immediately with 2 parallel VMs — the lease table handles contention.
- No centralized coordinator, consistent-hashing ring, or external scheduler is required.
- The "feed redistribution" concern in the plan's Option B autoscaler discussion is a non-issue: when a VM dies, its feeds become leasable by the survivors on the next scheduled lease poll.

**What to fix in the plan:**
- Remove the §9 open question on coordination.
- Update the migration phases: Phase 1 can run two fresh VMs that each poll the lease table — no manual feed assignment needed.
- The only remaining coordination question is: which VMs go down gracefully vs. abruptly during rolling updates? The existing `min_ready_sec = 300` in the plan's `update_policy` block is a correct lever; add a graceful-shutdown hook that calls `release_feed` before termination.

---

### 1.3 ❌ Current compute is e2-small, not e2-medium

The plan's cost-comparison table says:

> Current compute: 1× e2-medium (2 vCPU, 4 GiB) = ~$34/mo

**Actual:** `radio-transcription-deployment/terraform/modules/services/ingestion/main.tf:37` sets `machine_type = "e2-small"` (2 vCPU, **2 GiB** RAM) with `target_size = 1`. On-demand e2-small is ~$17/mo, not $34.

**Implications:** Low stakes for the forward-looking numbers, but the "current vs. scaled" table shows 2× actual current cost. Worth correcting so the ratio story is accurate.

---

## 2. Moderate errors (numbers, not conclusions)

### 2.1 ⚠ Cloud NAT savings are real but ~3.5× smaller than claimed

The plan claims:

> "Replacing Cloud NAT with external IPs alone saves ~$5,800/month."
>
> "…$0.045/GiB × ~130 TB ingress processing"

**Actual bandwidth** (from our FINDINGS.md duty-cycle analysis, cross-checked against the bcfy_feeds bitrate distribution in the catalog):

| Bitrate assumption | Aggregate bandwidth | TB/month | NAT cost @ $0.045/GB |
|---|---|---:|---:|
| 16 kbps per feed (plan's own figure used in §4 bandwidth calc) | 96 Mbps | 31 TB | $1,400/mo |
| **19 kbps weighted avg** (measured across 7,582 bcfy_feeds in the catalog — 79% are 16 kbps, 8% at 32 kbps, a handful higher) | 111 Mbps | 36 TB | **~$1,620/mo** |
| 64 kbps uniform (roughly what the plan's 130 TB figure implies) | 384 Mbps | 125 TB | $5,620/mo |

The plan's own **§4 Network bandwidth** section says 96 Mbps sustained — that math gives 31 TB/mo, not 130. There's an internal inconsistency within the plan itself. The real number is somewhere around 36 TB/mo / **$1,620/mo NAT cost**.

**External IP cost (14 VMs × $0.005/hr × 730 hr):** $51/mo.

**Net savings: ~$1,570/mo**, not $5,800.

Still a real saving, but not the largest single optimization in the plan. Rework the cost-comparison table.

### 2.2 ⚠ AlloyDB is HA/regional by default, cost is ~2× what the plan shows

`terraform/modules/alloydb/variables.tf:94` sets `availability_type = "REGIONAL"` as the default. Regional AlloyDB is multi-zone HA and costs ~2× zonal.

**Corrected AlloyDB compute cost:** ~$455/mo (not $227). Total-cost table needs updating; delta is +$228/mo.

If leadership decides HA isn't needed for this workload, switching to zonal saves $228/mo. That's a product/risk decision, not an eng decision — flag it for them.

### 2.3 ⚠ Connection pool numbers are wrong (but conclusion still holds)

The plan claims:

> Per-VM asyncpg data pool: 10 connections
> Per-VM asyncpg heartbeat pool: 1 connection
> Total client connections (14 VMs): 154

**Actual** from `backend/pipeline/storage/settings.py:39-48` and `backend/pipeline/ingestion/normalizer_runtime.py:154-166`:

- Data pool: `pool_min_size = pool_max_size = 5` (from `ALLOYDB_POOL_MIN_SIZE` / `ALLOYDB_POOL_MAX_SIZE` env vars, default **5**)
- Heartbeat pool: `pool_min_size = pool_max_size = 1`
- **Per VM: 6 connections (not 11)**
- **At 14 VMs: 84 connections (not 154)**

Multiplexing ratio: 84 / 8 = **10.5:1** (plan claimed 19:1).

pgBouncer's `max_client_connections = 800` (`variables.tf:152`) gives 10× headroom on client connections. The plan's conclusion ("2 vCPU fits") holds because the backend connection count is what matters, and that's `max_pool_size = 8` which is unchanged.

### 2.4 ⚠ Cloud NAT line item can't be verified in this repo

The plan lists "Current Cloud NAT: ~$700/mo" in §5. But there's no `google_compute_router_nat` resource in `radio-transcription/terraform/` or `radio-transcription-deployment/terraform/`. Either:
- (a) NAT is deployed outside this terraform (organization-level or shared networking module) — plan should cite where, or
- (b) NAT doesn't currently exist — then "current $700/mo" is fiction and the "savings from removing NAT" framing is misleading ("choosing external IPs over no NAT" is the correct framing).

Worth a 15-minute check with the networking owner to resolve which it is.

---

## 3. Alignment with the latest findings (12K Tier 1+2 deployment)

The scaling plan targets **6,575 feeds (bcfy_calls = 0)**. Since the plan was authored, FINDINGS.md (commit `95b6d74`) has moved to a **~12,027 feed recommendation (all Tier 1+2 across all four sources, including bcfy_calls = 6,335)**. If leadership adopts the 12K scope, the plan needs a scale-up pass:

| Metric | Plan's 6.5K scope | 12K Tier 1+2 scope |
|---|---|---|
| Feed count | 6,575 | **12,027** |
| VMs needed (at 500 feeds/VM, memory-bound) | 14 | **25** |
| Chunks/sec (agg) | ~435 | **~444–679** (mostly from more openmhz systems) |
| AlloyDB writes/sec | ~873 (22% util vs. 4,000 TPS estimate) | **~1,246–1,481** (31–37% util) — still well within 2 vCPU headroom |
| GCS Class A ops | ~$5,640/mo (or $2,820 w/ batching) | **~$5,800–$8,800/mo** (or $2,900–$4,400 w/ batching) |
| bcfy_calls polling on `/calls/v1/live/` | 0/sec (source excluded) | **634/sec** at default 10s poll interval — **new external-rate-limit risk** |
| Bandwidth (ingress) | 96 Mbps | ~76 Mbps (fewer bcfy_feeds) |

**Changes the plan needs for 12K scope:**

1. **Add bcfy_calls as an infrastructure-affecting source.** Poll rate of 634/sec to Broadcastify's `/calls/v1/live/` is new; doesn't affect our compute/DB, but is a third-party-rate-limit risk the plan doesn't cover. Mitigation: bump `_POLL_INTERVAL_SEC = 10.0` (in `backend/pipeline/ingestion/collectors/bcfy_calls/bcfy_calls_collector.py:26`) to 20s — halves the load to 317/sec, adds ≤10s latency.

2. **Fleet size becomes 25 VMs, not 14.** The cost table doubles roughly accordingly on compute ($1,985/mo → $3,550/mo at n2-standard-4 on-demand).

3. **AlloyDB is still fine at 2 vCPU** even at 12K scope (37% utilization in worst case), *if* the plan's pgbench estimate of 4,000 TPS per vCPU holds. Experiment 2 should be re-run at 1,500 TPS (not 900) for a true 12K test.

4. **GCS batching becomes a bigger lever.** At 6.5K, 2:1 batching saves ~$2,800/mo. At 12K, it saves up to **$4,400/mo**. That's enough to pay for the extra ~11 VMs. Scope this refactor before locking CUDs.

---

## 4. What the plan got right (preserve these)

- **AlloyDB write-capacity analysis** is solid. Simple single-row UPDATEs on fully-cached tables really do execute in 1–3 ms on AlloyDB; the 4,000 TPS-per-vCPU figure is a reasonable rule of thumb and matches what we'd expect. Do not pre-provision 4 vCPU.
- **Non-disruptive machine-type change** is correctly characterized — AlloyDB instance machine-type updates with `FORCE_APPLY` take ~1 minute of interruption and 10–15 minutes of operation time.
- **"Drop `feed_id` from log-based metric labels"** is a real issue the plan correctly flags. At 6,575 feeds × 14 VMs = 92,050 time series per metric, and GCP's soft limit is ~30,000 per log-based metric. Must be enforced before scale-out.
- **Three-layer failure detection** (MIG autohealing + custom-metric absence alert + fleet-silent alert) is the right design. The insight that "the health check must check only local process health, not downstream dependencies" is correct and important — our existing `quarantine_telemetry.py` quarantine-on-failure pattern means a downstream outage shouldn't cascade into a VM-replacement storm.
- **Migration phasing** (Phase 0 prep → 2-VM canary → half → full) with `min_ready_sec = 300` and `max_unavailable_fixed = 1` is correctly conservative.
- **The five experiments are correctly scoped**, modulo adding CPU measurement to Experiment 1.
- **The "CUD 10 VMs, leave 4 on-demand"** strategy is a smart way to preserve elasticity while capturing committed-use discount. Works at 12K scope too with `CUD 18, on-demand 7`.
- **Per-group autoscaler metric (Option A)** is the right choice over per-instance (Option B). Single source of truth, no oscillation during redistribution. `single_instance_assignment = 500` with `cooldown_period = 180s` is a reasonable starting config.

---

## 5. New items to investigate before committing

Adding to the plan's existing experiment list:

**Experiment 1 (updated):** Measure **both** per-feed CPU and RSS under load. Target: find the feeds/VM value where either CPU > 75% OR memory > 85%, whichever comes first. If CPU binds first, evaluate n2-highcpu-4 (same $ as n2-standard-4 but 4 vCPU / 4 GiB — trades memory for CPU at same cost) before standardizing.

**Experiment 6 (new):** **FLAC stream-copy concatenation spike.** The plan claims "FLAC files cannot be naively byte-concatenated" and proposes 30s segments as the only viable batching strategy. This is overstated — FLAC supports frame-level append via `ffmpeg -f concat -c copy -i list.txt out.flac` without re-encoding. Spend one engineer-day validating; if stream-copy concat works, we can batch 2 × 15s → 30s in-application without changing the downstream 15s chunk contract, and preserve transcription-side compatibility. This opens the $2,800–$4,400/mo batching lever without the SLO risk of 30s segments.

**Experiment 7 (new):** **AlloyDB write load at 1,500 TPS.** The plan's Experiment 2 targets 900 TPS (the 6.5K number). At 12K scope, target 1,500 TPS to validate the 2 vCPU headroom for the realistic scenario. If p99 > 10 ms or `cl_waiting` > 0, upgrade to 4 vCPU before scale-out.

**Experiment 8 (new):** **Broadcastify `/calls/v1/live/` rate-limit probe.** Before bringing bcfy_calls up to 6,335 feeds, run a contained test from one VM polling 1,000 known groupIds at 10s interval = 100 polls/sec sustained for 1 hour. Watch for 429 responses. If 100/sec is accepted, step to 300/sec, then 634/sec. Confirms the polling-rate headroom with Broadcastify before committing to full scope.

---

## 6. Recommendation

**Approve with corrections.** The plan's architecture is sound; the proposed steady-state (MIG + custom metric + external IPs + no DB upgrade) works. The three critical errors (§1) invalidate specific quantitative claims but don't require an architectural rewrite. The cost-model errors (§2) need to be corrected before leadership sees final numbers — the real savings are meaningful but smaller than advertised. The 12K scope alignment (§3) is the biggest open question: leadership should decide between 6.5K (plan as-written) and 12K (latest FINDINGS.md recommendation) before committing to VM counts and CUDs.

**Suggested order of operations:**

1. **Decide the scope** — 6.5K or 12K. (1 meeting with Product/Leadership)
2. **Run Experiment 1** with CPU measurement. Determines VM family and feeds/VM. (Infra, 1 week)
3. **Run Experiment 6** (FLAC stream-copy spike). Determines whether batching is cheap. (1 day)
4. **Correct the cost-model numbers** in the plan (NAT, AlloyDB HA, pool sizes, current compute tier). (0.5 day)
5. **Update the plan** to cover bcfy_calls at chosen scope. (1 day)
6. **Then** do the 2-VM canary (Phase 1).

---

## Appendix A — Code citations (verified 2026-04-12)

| Claim | File | Line |
|---|---|---:|
| ffmpeg uses FLAC encoder, not stream-copy | `backend/pipeline/ingestion/collectors/icecast_collector.py` | 289–314 |
| `-acodec flac`, `-ar 16000`, `-ac 1`, `-compression_level 0` | same | 301–305 |
| `AUDIO_FORMAT = "flac"` | `backend/pipeline/common/constants.py` | 7 |
| Atomic lease with `FOR UPDATE SKIP LOCKED` | `backend/pipeline/storage/feed_queries.py` | 3–38 |
| Fencing token increment on lease | same | 26 |
| Fencing token validation on write | same | 40–47 |
| 60-second stale-lease reacquisition window | same | 11 |
| Data pool default min=max=5 | `backend/pipeline/storage/settings.py` | 39–48 |
| Separate heartbeat pool (size 1) | `backend/pipeline/ingestion/normalizer_runtime.py` | 159–166 |
| MAX_FEEDS_PER_WORKER default 250 | `backend/pipeline/ingestion/settings.py` | 45 |
| Current VM: e2-small, target_size=1 | `radio-transcription-deployment/terraform/modules/services/ingestion/main.tf` | 37–38 |
| AlloyDB 2 vCPU default | `terraform/modules/alloydb/variables.tf` | 80–89 |
| AlloyDB availability_type = REGIONAL (HA) default | same | 91–100 |
| pgBouncer max_pool_size = 8 | same | 149–153 |
| pgBouncer max_client_connections = 800 | same | 152 |
| pgBouncer pool_mode = transaction | same | 150 |
| bcfy_calls poll interval = 10s | `backend/pipeline/ingestion/collectors/bcfy_calls/bcfy_calls_collector.py` | 26 |
| No autoscaler / health-check / router_nat in terraform | (absence verified) | — |
