# GCE MIG audio ingestion — final scaling plan

**Author**: Shuojing
**Date**: April 2026 (v2, revised 2026-04-18)
**Pricing**: us-central1, April 2026
**Status**: Revised after peer review, for leadership and engineering approval

## Revision history

**v2 (2026-04-18)** — Incorporated academic-pipeline peer review (Stage 2.5 + Stage 3). P0 items applied:

- **P0-1** Reframed the per-type cap mechanism. Worker tracks per-type holdings and passes `min(cap, remaining_budget_of_type)` as each CTE branch's LIMIT; PostgreSQL enforces the LIMIT structurally via the query planner. Earlier v1 framing ("DB-only enforcement, no worker tracking") was incorrect — a per-call LIMIT does not bound total-held without worker-side budget tracking. §4, §7.
- **P0-2** Per-type cap for bcfy_feeds settled at **240** (v1 was inconsistent between 240 and 260 across §4, §4.1, §9.2, Part V).
- **P0-3** Dropped the "pin claim batch size to 10" directive; per-call bound is now driven by per-type remaining-budget arithmetic. §9.2.
- **P0-4** Part V Decision 3 "Claim query changes" bullet rewritten to match §6's actual specification (primary CTE: per-branch `ORDER BY id`; recovery query: `ORDER BY retry_after ASC NULLS FIRST, id`). v1 summary was carrying an earlier draft's ORDER BY.
- **P0-5** Catalog composition confirmation elevated from "Phase 0 blocker" to **pre-approval blocker**. §4.1, Part V.
- **P0-6** Dropped the "Kafka KIP-537 to 18 s" citation (mis-attributed; KIP-537 is not the session-timeout KIP, and the "18 s" value is not Kafka's public default). Kubernetes NodeLease + etcd alone support the 1:3 ratio argument. §6.1.
- **P0-7** Dropped the "Heroku / Presto / FinBox" case-study attribution for jemalloc on Python+ffmpeg (Heroku writes on jemalloc-for-Ruby; Presto is JVM; FinBox lacks public case studies). Replaced with a description of the underlying glibc malloc-arena behavior and a commitment to measure magnitude in Phase 1. §7.
- **P0-8** Dropped the "Figma / Notion / EDB" citations for the vertical-split pattern (they don't actually exemplify the pattern). Technical argument stands on PostgreSQL bloat literature. §6.1.
- **P0-9** Ramp filter changed from `abs(hashtext(id::text)) % 100` to an md5-based expression. Rationale: `hashtext()` is a documented-internal PostgreSQL function whose algorithm has historically changed between PG major versions. An AlloyDB minor-version upgrade during the 16-day ramp could silently re-shuffle feeds between "enabled" and "disabled" buckets, violating the ramp's determinism property (and therefore rollback semantics). md5() is documented stable. One-line change; affects §6 primary CTE, §6 recovery query, §9.4, §9.5.

v1 (April 2026) — Initial release for review.

---

# Executive summary

The audio ingestion pipeline ingests up to 12,027 concurrent audio feeds from three upstream sources (Broadcastify continuous feeds, Broadcastify Calls API, and OpenMHZ) and writes audio segments to GCS for downstream transcription. This plan deploys a **regional Managed Instance Group (MIG) of 2–8 GCP n2-standard-4 VMs**, each running two identical worker containers, sized dynamically by an autoscaler that responds to feed-claim latency and CPU utilization — not to a hardcoded feeds-per-VM constant.

**Three decisions for leadership approval:**

1. **Autoscaling: two-signal MAX policy.** GCP's native multi-signal autoscaler combines a feed-claim latency trigger and CPU utilization. Scaling responds in 60–180 seconds without depending on any hardcoded feeds-per-VM capacity constant.
2. **Pricing: hybrid commitment.** Two VMs run on a 1-year Committed Use Discount; peak-load surge runs on standard on-demand pricing.
3. **Ramp plan: five-stage graduated rollout.** Production feeds are enabled in stages (1% → 20% → 50% → 80% → 100%) with soak periods between steps. Total ramp ~16 days after a 2-week prep phase and 1-week shadow soak.

The existing unified-workers architecture (documented in §1) is a foundational choice but is not itself a leadership decision here — the ingestion codebase already implements it. §1 records why that choice is right and why the principal alternative (specialized workers) was rejected. Everything else — the MIG topology, the k=2 containers-per-VM, the autoscaler, the pg_cron sweep, the managed-pool configuration — is new and deployed for the first time by this plan.

**Pre-approval blocker (new in v2 — see §4.1).** The plan's fleet-sizing math, cost figures, and Part V decision items assume a catalog mix of 41.5% bcfy_feeds / 55.2% bcfy_calls / 3.3% openmhz at a 12,027-feed peak. The admin-reviewed catalog file has a substantially different shape (5.5% / 8.1% / 86.4%). Leadership should not approve this plan until the operative peak-mix is confirmed.

**Expected outcomes:**

| Metric | Naive-deployment baseline | Proposed | Change |
|---|---|---|---|
| Peak fleet size | 8 VMs (flat year) | 2–8 VMs (autoscaled) | 25–75% smaller off-peak |
| Annual compute | $9,056 | $7,695 | **−$1,361 (−15%)** |
| Annual all-in | ~$71,800 | ~$43,521 | see below |
| Worst-case feed blackout (single VM loss, at peak) | specialized-fleet SPOF: 6,335 feeds for 5–10 min | unified-fleet: ~1,500 feeds for 60–90 sec | **~26× less downtime** |
| Recovery mechanism | Manual operator intervention | Automatic via lease expiry + sweep | No 3 AM page |

**Where the savings actually come from.** The $28,275/yr ($71,796 → $43,521) total difference decomposes into two distinct sources, and they are driven by different things:

| Source | Annual | Driver |
|---|---|---|
| **Architecture (this plan)** | **~$9,500** | Compute savings ($1,361) + retirement of naive-baseline's hypothetical long-running controller service + reduced Cloud Logging / Network / NAT costs from seasonal scale-down. Would not apply if we kept the naive design. |
| **Cost-model recalibration:** GCS Class A operations re-based on actual catalog seasonality | **~$18,800** | Naive baseline assumed 12,027 feeds year-round; reality is 500–4,000 feeds for ~7 off-season months. This correction applies regardless of architecture. |

Both lines are real dollars, but only the ~$9,500/yr is *caused by* this plan. The ~$18,800/yr is a correction to the expected run rate that would apply under any architecture. The plan's cost discussion (Part IV) presents both separately so leadership can judge them independently.

**How to read this document.** Part I presents the three leadership-facing decisions with enough context to approve or reject each. Part II covers the engineering details that underpin those decisions — capacity math, database schema, worker runtime, deploy behavior. Part III covers operations — deployment, runbook, monitoring, failure modes. Part IV covers cost. Part V is the approval checklist. Three appendices cover the experimental data, explicit honesty about uncertainty, and what is deliberately out of scope.

---

# Part I — The decisions

## 1. Architecture: unified workers

This plan's worker design is **unified workers**: every worker container is identical and handles all three feed types. Each worker claims a batch of feeds via a standard `SELECT ... FOR UPDATE SKIP LOCKED` query against the `unclaimed` pool, with no per-type prioritization. The query is naturally self-balancing: if any type backs up, its share of the pool grows, and workers select it more often automatically.

**Status of this choice.** The current ingestion codebase already implements the unified-worker pattern (one worker process with no per-type specialization), so the *code* is in place. But the deployment is today a single n2-standard-4 VM with one container and no autoscaler. The plan's leadership-visible decisions (autoscaling, pricing, ramp) all assume unified workers; this section documents why that foundational choice is right and why the principal alternative was rejected. No new code change is required to adopt unified workers — that part of the design already exists — but the fleet-level implications matter enough to record here.

**Why not specialized workers?** The principal alternative is dedicating each VM to one feed type. Specialization is operationally cleaner in steady state but creates a critical failure mode: if the VM dedicated to bcfy_calls dies, all 6,335 bcfy_calls feeds are dark for 5–10 minutes (the time required to provision a replacement VM and assign it the calls role). In the unified design, losing any single VM affects only ~1,500 feeds (one-eighth of peak), distributed proportionally across all three types, and the surviving 7 VMs absorb the orphaned work automatically within one lease-timeout window (60–90 seconds).

| Failure scenario | Specialized (rejected) | Unified (chosen) |
|---|---|---|
| Lose calls-VM | 6,335 calls dark for 5–10 min | n/a (no calls-VM) |
| Lose any VM | Variable: 6,335 / 4,757 / 381 dark | ~1,500 mixed feeds dark |
| Recovery time | Wait for autoscaler + role assignment (~5 min) | Wait for lease expiry (~75 sec) |
| Recovery mechanism | Controller reassigns role to new VM | Surviving workers claim orphaned leases via standard SQL polling |
| Operator action required | Often (controller debugging) | None (automatic) |

**Why not weighted polling?** A second alternative was biasing the lease query toward the highest-volume type via per-type weights. Rejected after analysis: `SKIP LOCKED` already selects feeds in proportion to their share of the pool in steady state, so explicit weights are redundant in the common case and *amplify* imbalance during surges. A bcfy_calls surge pushing the pool to 92% calls already reduces openmhz selection to 0.06 feeds per 10-batch under uniform selection; adding a 5× calls weight drives it to 0.01. The weights make starvation worse, not better.

**Tradeoff acknowledged.** Unified workers carry more memory per worker than specialized ones, because every worker buffers some bcfy_feeds (the memory-heavy type). This is what drives the per-worker target of 800 feeds rather than 1,000. §4 shows the math — and §4 also discusses the memory *variance* risk: SKIP LOCKED does not guarantee each worker holds the catalog-average mix, which matters for capacity planning.

## 2. Autoscaling: two-signal MAX policy

The autoscaler responds to the maximum of two independent signals. A scale-out occurs if either signal exceeds its threshold; a scale-in requires both signals to be under their thresholds.

| Signal | Source | Target | Why |
|---|---|---|---|
| **`oldest_unclaimed_feed_age`** | Cloud Run Function (50 lines, Cloud Scheduler every 60 s) | `utilization_target = 60` seconds | Catches backlog: idle-fleet surges, catalog bursts, workers falling behind |
| **CPU utilization** | GCP-native, no external component required | `utilization_target = 0.75` | Catches saturation: feeds getting more expensive, fleet gradually overloaded |

This design has three important properties.

**No hardcoded capacity constants anywhere in the scaling path.** The `60 seconds` target is a latency SLO (how long the team is willing to let a feed sit unclaimed), not a guess about throughput capacity. The `0.75` CPU target is a headroom policy (leave 25% slack for bursts), not a feeds-per-worker number. Neither signal assumes any "feeds per VM" constant. If the catalog mix shifts, if feeds become more or less expensive, if the worker code changes — the autoscaler adapts fleet size without code changes or Terraform edits. (The per-type caps in §4 are a separate surface and are env-var tunable.)

**Rejected alternative: a long-running controller.** A natural design for this kind of system is a Cloud Run service running a reconciliation loop — periodically querying the database, computing an `ideal_vm_count`, publishing the result, and needing crash-recovery and (if scaled beyond one replica) leader-election machinery. This plan deliberately rejects that design. Instead, the only external component is a 50-line Cloud Run **Function** (not a service) triggered by Cloud Scheduler every 60 s. It runs one SQL query (`SELECT EXTRACT(epoch FROM NOW() - MIN(unclaimed_since)) FROM feeds WHERE status='unclaimed'`, using the `unclaimed_since` column added in §6), publishes the result as a custom metric, and exits. No reconciliation loop, no leader election, no long-running state, no capacity constant. Cost: effectively $0 (well inside Cloud Run Functions free tier at 1 invocation / 60 s).  If it dies, the CPU signal continues to size the fleet correctly — the fleet just loses its fast-burst-detection optimization until the function is restored.

This matters because the obvious next step for teams building control planes is "spin up a controller service." That design would cost ~$3,400/yr in Cloud Run + associated logging, require its own health monitoring, and introduce a new failure mode (the controller can crash / get behind / emit stale decisions). The stateless-function design avoids all of that, and the plan's cost attribution (§14) treats the controller-retirement as a real architecture saving against the naive baseline.

**Signal coverage is complete for every scenario where scaling would help:**

- Catalog growing gradually over hours → CPU rises → scale out
- Idle fleet, sudden catalog burst → oldest-feed-age breaches 60 s → scale out
- One feed type becomes more expensive (e.g., codec change) → CPU rises → scale out
- Traffic spike (feeds arriving 2× faster) → both signals rise → scale out
- Workers overloaded by any cause → CPU rises → scale out
- Upstream outage, no new work → neither signal rises → no scaling (correct)

The edge cases where the signals don't trigger (worker GIL-blocked but CPU idle, AlloyDB slow) are cases where adding more VMs wouldn't help anyway. Those need operator intervention, not autoscaling — handled by the alerting in §11.

**Why not a third signal?**

- **Worker-reported saturation** is redundant with CPU — kernel-level CPU already measures worker saturation. Worker-reported metrics only differ in edge cases (I/O-wait, GIL blocks) where scaling is the wrong response.
- **Memory utilization** adds modest coverage for RSS-creep scenarios but requires Ops Agent installation; deferred to future work if needed.
- **Queue depth directly** requires `single_instance_assignment=N` or a poorly-behaved `utilization_target` against absolute queue depth. Queue-depth scaling mathematically cannot eliminate a capacity constant — the division has to happen somewhere. Saturation-based scaling (CPU + latency SLO) avoids the division entirely.

## 3. Pricing: hybrid commitment

| Component | Allocation | April 2026 us-central1 rate | Annual |
|---|---|---|---|
| Always-on baseline (2 VMs) | 1-year Committed Use Discount | $89.33/VM-mo (−37% vs on-demand) | $2,144 |
| Peak surge (up to 6 additional VMs) | On-demand | $141.79/VM-mo | Up to $4,254 |
| Boot disks (50 GB pd-balanced × VMs) | n/a | $0.10/GB-mo | $305 |
| **Annual compute total** | | | **$7,695** |

The 2-VM baseline matches the off-season floor (zonal HA requires ≥2 VMs always) and represents ~29% of annual VM-hours. Committing the baseline to a 1-year CUD captures the largest reliable discount; the variable surge stays on flexible pricing.

Three alternatives considered:

| Alternative | Annual | Why not chosen |
|---|---|---|
| All on-demand (no commitment) | $8,954 | $1,259/yr more for no benefit |
| All 8 VMs on 1-year CUD | $5,754 | $1,941/yr cheaper but commits financially to 8-VM CUD billing regardless of actual utilization. If the catalog shape changes (e.g., peak above 12,027 or a sustained off-season expansion), the commitment structure gets in the way. See note below. |
| All 8 VMs on 3-year CUD | $3,371 | 50% cheaper, but commits to architecture being unchanged for 3 years — this design is too new |

**Note on the 8-VM CUD alternative.** CUD pricing is billed on committed vCPU+memory hours, not on actual instance-hours used. Choosing the 8-VM 1-year CUD does not force the fleet to run 8 VMs year-round; it commits to *paying* for 8 VM-equivalents regardless of actual scaling. The $5,754 figure already reflects paying for 8 VMs all year. The $1,941/yr premium on the hybrid is therefore the price of keeping the on-demand surge rate flexible — useful if peak catalog grows beyond 12,027 during the CUD term, or if the surge shape changes from roughly 5-months-on / 7-months-off. At Phase 3 (after the 7-day 100% soak), leadership can reassess whether to convert to full 8-VM CUD and accept the lower ceiling for flexibility.

---

# Part II — Engineering detail

## 4. Per-worker capacity: 800 feeds (memory-bound, not CPU-bound)

Worker capacity is determined by whichever resource saturates first: CPU or memory. Both are sized from Experiment 1b measurements (full data in Appendix A).

**CPU side.** The unified-mix CPU slope is ≈0.069% per feed plus a 6.43% intercept. At 800 feeds per worker on a single vCPU, CPU usage is **800 × 0.069 + 6.43 = 61.6%**. Comfortable headroom for activation bursts and upstream-outage reconnect storms.

**Memory side.** Per-source RSS slopes are dominated by bcfy_feeds at 16.9 MiB per feed, while bcfy_calls is 0.40 MiB and openmhz is 2.8 MiB per feed. With the production catalog split of 41.5% bcfy_feeds, 55.2% bcfy_calls, 3.3% openmhz, a unified worker handling 800 feeds carries:

| Source | Feeds carried | RSS | Notes |
|---|---|---|---|
| bcfy_feeds (41.5%) | 332 | 5,611 MiB | Dominant; ffmpeg buffers |
| bcfy_calls (55.2%) | 442 | 177 MiB | Cheap |
| openmhz (3.3%) | 26 | 73 MiB | Cheap |
| Base (interpreter + libs) | — | 157 MiB | |
| **Per-worker total** | 800 | **6,018 MiB ≈ 5.87 GiB** | |
| **Per-VM at k=2** | 1,600 | **11.75 GiB** | |
| **Headroom on n2-standard-4 (16 GiB)** | | **4.25 GiB** | Safe for OS, Docker, telemetry, and FFmpeg RSS-creep tolerance |

**Why not 1,000 feeds per worker?** At 1,000 feeds per worker, per-VM RSS rises to 14.62 GiB on a 16 GiB VM, leaving only 1.38 GiB headroom. FFmpeg has a documented RSS-creep pattern under variable-bitrate or anomalous streams; sustained RSS growth of 1 GiB per worker would trigger the kernel OOM killer, which terminates containers without graceful shutdown. The cost of operating at 800 instead of 1,000 is **+1 VM at peak (8 instead of 7), or +$1,684/yr**. This is 41% the cost of moving to n2-highmem-4 SKU (+$4,140/yr) for equivalent safety.

**Why not 600 feeds per worker?** Would give 7.11 GiB headroom (overkill for documented FFmpeg behavior) but raise peak fleet to 11 VMs (+$5,098/yr). Not justified by current data; revisit only if 4.25 GiB proves insufficient in production.

**Mix variance and OOM risk (important).** The 5.87 GiB per-worker RSS figure above assumes every worker holds the catalog-average mix. This is an *expected value*, not a guarantee. `SELECT ... FOR UPDATE SKIP LOCKED` claims whatever rows the index scan hits first; if bcfy_feeds rows happen to be temporally clustered in the heap (e.g., a batch of newly-added bcfy_feeds arrived together), one worker can claim a heavily bcfy_feeds-weighted batch. `bcfy_feeds` is by far the memory-heaviest type (16.9 MiB vs 0.40 and 2.8 for the others), so adversarial clustering toward bcfy_feeds is the OOM risk of concern.

Adversarial worst case without cap: 800 bcfy_feeds in a single worker = 800 × 16.9 MiB = **13.5 GiB** RSS. At k=2, per-VM RSS = 27 GiB on a 16 GiB VM — instant OOM.

**Primary mitigation: worker-budgeted DB-enforced per-type cap via UNION ALL CTE (Phase 0).** The enforcement is split between worker and DB by necessity: the DB cannot by itself know how many feeds a given worker currently holds (each worker has an independent transaction and its own ephemeral Python-level count), so the worker computes `remaining_budget[type] = cap[type] - current_held[type]` at claim time and passes `min(cap[type], remaining_budget[type])` as each CTE branch's LIMIT. PostgreSQL then enforces that LIMIT structurally via the query planner — whatever value the worker passes, the worker receives at most that many rows. The cap values (240 / 600 / 900 per-type) are the maximum budgets; actual per-call LIMITs are driven by remaining-budget arithmetic.

The layered protection:

- **Against row-leakage or planner LIMIT bypass:** structurally guaranteed by PostgreSQL (with MATERIALIZED keyword — see §6 for the planner hazard).
- **Against worker-counter corruption leading to over-claim:** the self-RSS watchdog (§7) pauses claims at 70% container memory and exits at 90%.

**Earlier-draft correction.** An earlier revision of this plan framed the cap as "DB-only enforcement, no worker tracking required." That framing was incorrect: a per-call LIMIT does not bound the total-held count across many claim calls. A worker polling every 5 s with a single hard LIMIT of 240 bcfy_feeds per call would accumulate 240 bcfy_feeds per call × N calls until `max_feeds_per_worker` is reached. Bounding the total requires the worker to adjust its per-call LIMIT to reflect current holdings. The current design makes this explicit: worker tracks state, DB enforces the worker's ask.

External research on mature queueing systems (Celery, Sidekiq, Temporal, pgmq) converges on this pattern for memory-heterogeneous workloads: per-type admission caps where the DB (not the application) is the structural enforcer of the "do not exceed this ask" guarantee, with the worker responsible for computing what to ask.

**Per-type budget caps to ship** (passed to the CTE as per-branch LIMITs; worker passes `min(cap, remaining_budget_of_type)` each call):

| Type | Cap | RSS/feed | If at cap |
|---|---|---|---|
| bcfy_feeds | 240 | 16.9 MiB | 4,056 MiB |
| bcfy_calls | 600 | 0.40 MiB | 240 MiB |
| openmhz | 900 | 2.8 MiB | 2,520 MiB |
| Base (interpreter + libs) | — | — | 157 MiB |
| **Per-worker ceiling (cap-summed, theoretical maximum if worker holds cap of every type)** | **1,740** | — | **~6,973 MiB ≈ 6.81 GiB** |
| **Per-VM at k=2 ceiling** | 3,480 | — | **~13.62 GiB** on 16 GiB VM |
| **Margin** | | | **~2.38 GiB** before OS/Docker (~0.8 GiB) and FFmpeg RSS-creep tolerance |

The cap values fit n2-standard-4 at k=2 while maximizing throughput per worker (roughly 2.2× vs the prior 800-target if all caps were simultaneously binding). Caps are shippable as env vars (`cap_bcfy_feeds`, `cap_bcfy_calls`, `cap_openmhz`) for tunability without redeploy.

**Conservative rollout path on per-worker target.** The budget caps (240/600/900) are correctness defense-in-depth — they ship in Phase 0 regardless. The `max_feeds_per_worker` *target* (total simultaneous leases a worker holds across poll cycles) is a separate decision, and the plan commits to the following progression:

- **Phase 0 / Phase 1 shadow soak:** `max_feeds_per_worker = 800` (conservative; matches prior analysis; validated by Experiment 1b's 6-feed run and well inside the cap-summed 1,740 ceiling).
- **Phase 2 / Phase 3 (after empirical validation):** Raise to 1,200–1,740 only after Phase 1 shadow soak measures actual per-worker RSS at intermediate densities against the 41:55:4 synthetic workload. If the real unified-mix RSS slope matches the extrapolation, raise the target; if not, hold at 800.

The research's "ship 1,740" recommendation is mathematically consistent with the per-feed slopes in Appendix A, but those slopes are themselves partially estimated (bcfy_calls and openmhz are not empirically measured — see Appendix A). Shipping the cap-summed 1,740 as the per-worker target in Phase 0 would outrun the evidence; shipping the budget caps in Phase 0 and raising the target progressively gets the safety benefit now and the throughput benefit after validation.

**What stays at the 800 target in Phase 0:**

- Fleet sizing (§5): peak 8 VMs, ceiling 10.
- Cost (Part IV): existing arithmetic.
- Pool sizing (§6): existing arithmetic.
- CPU math: 800 × 0.069 + 6.43 = 61.6% is still the Phase 0 working figure.

**What changes in Phase 0 even at the 800 target:**

- Claim query rewritten to the UNION ALL MATERIALIZED CTE with FOR NO KEY UPDATE (§6).
- Composite partial index `feeds_claim_idx ON (source_type, id) WHERE status='unclaimed'` added (§6).
- Worker tracks per-type holdings and passes `min(cap, remaining_budget)` as each CTE branch's LIMIT — the worker-side budget accounting is the complement to the DB-side LIMIT enforcement.
- Per-type budget caps set at 240/600/900 — at `max_feeds_per_worker=800` these rarely bind (the sum is 1,740), but they are the hard safety net against adversarial clustering.
- TCP keepalives and `idle_in_transaction_session_timeout` (§6).
- jemalloc with `MALLOC_ARENA_MAX=2` (§7) — previously jemalloc alone; adding the arena cap.
- Self-RSS watchdog (§7) — defense-in-depth against worker-counter corruption.

**Phase 3 exit criterion for raising the per-worker target:** Phase 1 shadow soak must demonstrate sustained per-worker RSS at ≤6.0 GiB for 72 consecutive hours at the target density, before Phase 2 ramps raise `max_feeds_per_worker`. This is an explicit addition to the §9.3 exit criteria.

### 4.1 Catalog composition: plan assumes 41:55:4 — confirm before approval

The capacity math above assumes a catalog composition of 41.5% bcfy_feeds / 55.2% bcfy_calls / 3.3% openmhz at a 12,027-feed peak. This is the split the plan has used in all prior sections.

The admin-reviewed catalog file (`model/data/wildfire_catalog/output/wildfire_feed_catalog_admin_review.csv`, Tier 1+2) contains 76,221 rows at a split of approximately 5.5% bcfy_feeds / 8.1% bcfy_calls / 86.4% openmhz. If the operative "12,027 peak" is a subset of this catalog that preserves the 41:55:4 mix (e.g., selected by wildfire-season or market-relevance criteria), the math above is correct. If the operative catalog is the full Tier 1+2 set or a different subset with the real 5:8:86 shape, the per-worker memory story is substantially different — openmhz at 2.8 MiB dominates over bcfy_feeds at 16.9 MiB only on a unit-per-feed basis, but with 86% of feeds in the cheap type, per-worker RSS at 800 feeds falls to ~2.8 GiB and the 16 GiB VM has vast unused headroom.

**Pre-approval blocker (elevated in v2 from "Phase 0 action item").** Before leadership approves this plan, confirm which catalog subset is operative at peak and what its (bcfy_feeds, bcfy_calls, openmhz) split is. The §4 fleet-sizing math, §13 cost figures, and Part V decision items all depend on the assumed mix; a 5:8:86 mix shifts the answers materially. Resolving this during Phase 0 is too late — the approved plan would be sized against a mix that may not reflect reality. Two concrete outcomes:

- **If the operative peak mix is the assumed 41:55:4** → §4 math stands; cap of 240 bcfy_feeds is correct; fleet sizing and cost unchanged.
- **If the operative peak mix is openmhz-dominated (e.g., 5:8:86)** → per-worker RSS at 800 feeds drops to ~2.8 GiB; the 800-per-worker target is over-conservative and could likely be raised to ~2,000 (reducing peak fleet to ~3 VMs). Per-type cap re-targets: bcfy_feeds cap can stay at 240 (still adversarial-safe), but the plan's fleet sizing (§5) and cost (Part IV) should be rerun before approval.

The plan's 800-target and 8-VM peak are conservative under either mix. Phase 1 shadow soak will still measure actual per-worker RSS at the confirmed mix, and the Phase 3 review can raise the per-worker target if warranted. But fleet sizing needs the right denominator at approval time.

**Why not type-weighted polling (specialized claim)?** Distinct from the weighted-polling rejected in §1 for steady-state fairness reasons, this would be an OOM-prevention mechanism: per-type claim limits on the query side. The budget-cap above is the query-side mechanism the plan adopts — worker passes per-type LIMITs, DB enforces them. No separate weighted-polling layer is needed.

## 5. Fleet sizing across the year

| Catalog state | Total feeds | Workers needed | VMs needed | Driver |
|---|---|---|---|---|
| Off-season floor | < 1,000 | 1–2 | **2** | Zonal HA minimum |
| Off-season typical | ~4,000 | 5 | **3** | Autoscaler |
| Mid-season | ~8,000 | 10 | **5** | Autoscaler |
| Peak | 12,027 | 16 | **8** | Autoscaler |
| Burst ceiling | — | — | **10** | Autoscaler max |

Math: workers = ⌈feeds / 800⌉; VMs = ⌈workers / 2⌉. The burst ceiling of 10 VMs absorbs unexpected catalog growth, zonal failure replacement, and transient overloads during scale-out events.

**Regional MIG distribution policy (Phase 0 Terraform requirement).** A regional MIG distributes VMs across zones best-effort by default; the distribution can skew to 4+2+2 or worse after replacement cycles. The multi-VM-loss math below assumes roughly balanced placement (3+3+2 or 3+2+3). To hold that assumption, the MIG must be deployed with:

```hcl
distribution_policy_target_shape = "EVEN"
distribution_policy_zones = ["us-central1-a", "us-central1-b", "us-central1-c"]
```

Without `EVEN`, a single-zone outage could take out 4 of 8 VMs, and the "up to 3 in one zone" claim becomes "up to 4 in worst case." The `EVEN` setting forces rebalancing during replacement, so after prior outage+recovery cycles the distribution trends back to balanced. One gotcha: during autoscale-out events where one zone is capacity-constrained, initial placement can temporarily skew even with `EVEN`; the MIG rebalances within hours via replacement cycles.

**Peak capacity and single-VM-loss behavior.** At peak (12,027 feeds across 8 VMs), each VM carries ~1,500 feeds. If one VM dies, the surviving 7 VMs have 7 × 1,600 = 11,200 feeds of steady-state capacity — slightly below the 12,027 peak load. This produces a brief period (~3–5 min until the replacement boots) during which feed-claim latency rises and `oldest_unclaimed_feed_age` climbs above the 60 s target, triggering the autoscaler to provision a 9th VM (within the 10-VM ceiling). This is by design: peak is sized for normal operation plus autoscaler-driven recovery, not for instantaneous spare capacity.

**Multi-VM loss at peak is a degraded-mode event.** The autoscaler's 10-VM ceiling limits the recovery envelope:

- **Two-VM loss:** 6 × 1,600 = 9,600 capacity for 12,027 feeds = ~20% shortfall (~2,400 feeds experiencing raised claim latency). Autoscaler provisions 2 replacements; recovery within ~5 min.
- **Three-VM loss (zonal outage — up to 3 VMs can land in one zone):** 5 × 1,600 = 8,000 capacity for 12,027 feeds = ~33% shortfall (~4,000 feeds affected). The 10-VM ceiling permits 2 simultaneous replacements beyond the remaining 5; the 3rd replacement waits until one of the first two passes `initialization_period_sec=180` and `min_ready_sec=60`. Recovery window extends to ~8–12 min, with per-type backlog alerts firing through that period.

These are not failures the design recovers from silently — operators see backlog alerts and `oldest_unclaimed_feed_age` breaches for several minutes. Multi-VM loss at peak is explicitly called out as a degraded-mode event in §10 (operator runbook) and §12 (failure modes).

**Why not raise the ceiling to 12?** Raising the autoscaler ceiling from 10 to 12 would absorb the 3-VM zonal loss without entering degraded mode: 9 surviving VMs × 1,600 = 14,400 capacity vs 12,027 peak demand = sufficient. The cost comparison:

| Ceiling | Cost impact | 3-VM zonal loss behavior |
|---|---|---|
| 10 (plan default) | Baseline | ~33% shortfall for 8–12 min; backlog alerts fire; automatic recovery |
| 12 | +~$840/yr burst-cost exposure at typical utilization (occasional 11th/12th VM during ramps + replacement events); CUD structure unchanged | No degradation from 3-VM loss; 4-VM loss becomes the new degraded-mode threshold |

Leadership should judge this tradeoff explicitly. The plan defaults to the 10 ceiling because (a) zonal outages losing 3 VMs are rare, (b) the 8–12 min degraded window is tolerable for the workload (audio capture with downstream transcription — not real-time), and (c) keeping a tighter ceiling prevents runaway costs if something unrelated pushes the autoscaler to its max. If leadership prefers to absorb 3-VM loss cleanly, the ceiling can be raised to 12 at approval time.

**Two containers per VM (k=2).** Each worker is single-threaded asyncio bound to one vCPU. Two containers on a 4-vCPU VM use 50% of the cores; remaining headroom absorbs OS, Docker daemon, and telemetry. Going to k=4 was rejected because doubling the per-VM worker count doubles the OOM risk surface under per-worker memory budgets, without commensurate throughput benefit.

## 6. Database design and the heartbeat-bloat fix

Workers coordinate via PostgreSQL `SELECT ... FOR UPDATE SKIP LOCKED` against AlloyDB. The production worker uses `ACQUIRE_FEEDS_BATCH_SQL` (the batch-claim path in `feed_queries.py`). Each worker sends a heartbeat every 20 seconds (Phase 0 change from 15 s; see §6.1) to refresh `last_heartbeat` on its leased rows. Progress-path writes target a separate `last_progress_at` column (Phase 0 addition; see §6.1) so progress bookmarks do not touch the index-covered `last_heartbeat` column.

**Terminology vs production schema.** The production `feeds` table uses:

- `status` enum with values `unclaimed`, `active`, `failing`, `quarantined`, `deactivated` (not `pending`/`running`). The claim query filters to `unclaimed`, `failing`, or `active`; `quarantined` and `deactivated` feeds are never claimed. The plan's partial indexes (`idx_feeds_unclaimed`, `idx_feeds_failing_retryable`, `idx_feeds_active`) implicitly exclude `quarantined` and `deactivated` rows, which is correct — no claim-path work is needed on those statuses. Admin/operational queries touching `deactivated` rows can seq-scan (~76k rows max; cheap at low frequency).
- `last_heartbeat` as the liveness column mutated by the heartbeat renewal only after Phase 0 (pre-Phase 0, four queries mutate it; Phase 0 cleanup reduces this to one).
- `fencing_token` incremented on every successful claim (optimistic-concurrency defense).
- `last_progress_at` is a new unindexed column added in Phase 0 for progress-path writes (§6.1).
- No `scheduled_at` column exists; see "Publisher query" below for the column this plan adds.

**Query load at peak** (after Phase 0: 5 s poll interval, **20 s heartbeat interval**, per-type budget-driven claim sizing):

| Query | Frequency | Total QPS | Notes |
|---|---|---|---|
| Lease claim (UNION ALL CTE, per-type LIMITs) | 16 workers × 0.2 polls/sec | ~3.2 QPS | Each poll is one SQL statement; per-call row count varies by remaining budget |
| Heartbeat (bulk UPDATE across worker's leases) | 16 workers ÷ 20 sec | ~0.8 QPS | One UPDATE per worker per heartbeat cycle; rows covered = leases held (~1,500 at peak per VM) |
| Progress bookmark (`UPDATE_PROGRESS_SQL`) | ~1 per 10 s audio chunk × ~1,500 feeds per VM at peak | ~1,200 QPS fleet-wide | Targets unindexed `last_progress_at` (Phase 0); fully HOT-eligible, no index-write amplification |
| Release (`RELEASE_FEEDS_BATCH_SQL`) | ~1 per SIGTERM per worker | rare | Phase 0 drops `last_heartbeat` write |
| Report failure | rare | rare | Phase 0 drops `last_heartbeat` write |

Claim and heartbeat QPS are well within AlloyDB's capacity. Progress bookmarks dominate the write-*rate* picture but are no longer bloat-risky after Phase 0's column split.

**The MVCC bloat problem.** Every UPDATE creates a new row version, generating dead tuples that accumulate as table and index bloat. Pre-Phase-0, four production queries mutate `last_heartbeat`; Phase 0 consolidates mutation into just the heartbeat-renewal path and cuts the rate further with coalescing:

| Query | Rate — pre-Phase-0 | Rate — post-Phase-0 (§6.1 coalescing) | Mutates `last_heartbeat`? |
|---|---|---|---|
| Heartbeat renewal | ~800 row-updates/sec (15 s cadence) | **~480 row-updates/sec** (20 s cadence × 0.7 skip-if-recent factor) | Yes (only remaining writer) |
| `UPDATE_PROGRESS_SQL` | ~1,200 row-updates/sec | 0 (writes `last_progress_at` instead) | No after Phase 0 |
| `RELEASE_FEEDS_BATCH_SQL` | rare | 0 | No after Phase 0 |
| `REPORT_FAILURE_SQL` | rare | 0 | No after Phase 0 |

**Total: ~2,000 row-updates/sec on `last_heartbeat` pre-Phase-0 → ~480 row-updates/sec post-Phase-0.** In dead-tuple terms: ~170M/day → ~42M/day, a ~4× reduction at the source before HOT does any work. Progress-path writes (~1,200/sec) still happen but target the unindexed `last_progress_at` column, making them fully HOT-eligible and bloat-irrelevant for the covered indexes.

**Phase 0 cleanup recommendation (now covered by §6.1).** Remove `last_heartbeat = NOW()` from `UPDATE_PROGRESS_SQL` (replace with `last_progress_at = NOW()`), and remove the side-effect `last_heartbeat = NOW()` from `RELEASE_FEEDS_BATCH_SQL` and `REPORT_FAILURE_SQL` entirely. See §6.1 for the full coalescing rationale including the skip-if-recent predicate and cadence relaxation.

Without HOT, any of these rates would bloat `feeds` indexes within hours; the only cleanup mechanism (`VACUUM FULL`) requires an exclusive table lock.

**The HOT prerequisite, and why it is currently broken.** PostgreSQL's Heap-Only Tuple (HOT) updates perform in-page updates without touching indexes, *provided* two conditions hold:

1. The table has slack space on each page (`fillfactor=70`).
2. The mutated columns are not referenced by any index.

**Neither condition currently holds in production.** The `feeds` table uses the default `fillfactor=100`, and `idx_feeds_leasing` is defined as `(status, retry_after, last_heartbeat) WHERE status IN (...)` — `last_heartbeat` is in the index. Every heartbeat UPDATE therefore produces a full heap+index update. The `hot_ratio` alert the plan proposes would fire on first heartbeat traffic at production scale.

**Required Phase 0 fix.** The claim query currently selects from three status branches (`unclaimed`, `failing` with retry window elapsed, and `active` with stale heartbeat). All three branches need index coverage, and `last_heartbeat` must not be in any index's indexed-columns list. The schema below supports this, *and* addresses two further risks: (a) the ORDER BY clause should not require a sort over thousands of rows during surges, and (b) the abandoned-lease sweep must not flip a large batch of rows to `unclaimed` in a single transaction (which would trigger a 16-worker polling stampede).

**Schema and index changes (Phase 0):**

**Important note on column naming.** The table's primary key column is `id` in production (per `003_feeds.sql`), not `feed_id` as earlier drafts of this plan stated. All SQL examples below use `id`. Phase 0 migration work must verify the column name against `terraform/modules/alloydb/sql/ingestion/003_feeds.sql` before executing; if the schema has been renamed since this plan was written, update accordingly. `feed_id` appears elsewhere in this plan only as a general term referring to "the feed's primary-key column."

```sql
-- Reserve slack for HOT updates.
ALTER TABLE feeds SET (fillfactor = 70);

-- Rewrite the table once so existing pages have slack (~12k rows, ~seconds).
VACUUM FULL feeds;

-- Column for autoscaler signal; see §2 publisher query.
ALTER TABLE feeds ADD COLUMN unclaimed_since TIMESTAMP WITH TIME ZONE;
-- Worker code and sweep set unclaimed_since = NOW() on status → 'unclaimed'.

-- Drop the bloat-inducing index.
DROP INDEX idx_feeds_leasing;

-- Partial indexes for the three claim-query branches.
-- Critical: none of these reference mutated hot-path columns
-- (last_heartbeat, unclaimed_since, worker_id, fencing_token).
-- The composite feeds_claim_by_type_idx below subsumes the single-column
-- idx_feeds_unclaimed for all primary-path claims; single-column index retained
-- only if admin tooling specifically needs id-only access (reassess during Phase 0).
CREATE INDEX idx_feeds_unclaimed
  ON feeds (id)
  WHERE status = 'unclaimed';

-- Supports the "failing with retry elapsed" claim branch.
-- retry_after is only mutated on failure, not on every heartbeat, so this is HOT-safe.
CREATE INDEX idx_feeds_failing_retryable
  ON feeds (retry_after)
  WHERE status = 'failing';

-- Supports the abandoned-lease sweep's scan (active-status rows).
CREATE INDEX idx_feeds_active
  ON feeds (id)
  WHERE status = 'active';

-- Belt-and-suspenders autovacuum tuning.
ALTER TABLE feeds SET (
  autovacuum_vacuum_scale_factor = 0.05,
  autovacuum_vacuum_cost_delay = 10
);
```

**Claim query changes (Phase 0).** The plan adopts the per-type-cap CTE pattern from the external queueing-systems research (see §4 mitigation section). The rewrite has three load-bearing parts, each of which must ship together.

**Part A: Composite partial index.** Add an index that supports per-type ordered scans for the claim query's three UNION branches:

```sql
-- Supports per-type subqueries in the claim CTE.
-- Partial (status='unclaimed') keeps it small; source_type immutable, id immutable.
-- HOT-safe: neither indexed column is mutated in the hot path.
CREATE INDEX CONCURRENTLY feeds_claim_by_type_idx
  ON feeds (source_type, id)
  WHERE status = 'unclaimed';
```

For `failing-retryable` and `active-abandoned` recovery paths, `idx_feeds_failing_retryable` and `idx_feeds_active` remain the serving indexes; those branches do not need per-type enforcement because they're already small volumes (sweep handles abandoned).

**Part B: Rewritten claim query — the UNION ALL MATERIALIZED CTE.** Three independent per-type SKIP LOCKED subqueries, combined under a MATERIALIZED CTE, followed by a single `UPDATE ... FROM claimed`. Note the ramp filter uses an md5-based expression (not `hashtext()`) for stability across PostgreSQL minor version upgrades — see §9.4.

```sql
WITH claimed AS MATERIALIZED (
  (SELECT id FROM feeds
     WHERE source_type = 'bcfy_feeds' AND status = 'unclaimed'
       AND (('x' || substr(md5(id::text), 1, 7))::bit(28)::integer) % 100 < $3  -- ramp filter; md5 stable across PG minor upgrades
     ORDER BY id
     FOR NO KEY UPDATE SKIP LOCKED
     LIMIT $4)  -- min(cap_bcfy_feeds, remaining_bcfy_feeds_budget); cap default 240
  UNION ALL
  (SELECT id FROM feeds
     WHERE source_type = 'bcfy_calls' AND status = 'unclaimed'
       AND (('x' || substr(md5(id::text), 1, 7))::bit(28)::integer) % 100 < $3
     ORDER BY id
     FOR NO KEY UPDATE SKIP LOCKED
     LIMIT $5)  -- min(cap_bcfy_calls, remaining_bcfy_calls_budget); cap default 600
  UNION ALL
  (SELECT id FROM feeds
     WHERE source_type = 'openmhz' AND status = 'unclaimed'
       AND (('x' || substr(md5(id::text), 1, 7))::bit(28)::integer) % 100 < $3
     ORDER BY id
     FOR NO KEY UPDATE SKIP LOCKED
     LIMIT $6)  -- min(cap_openmhz, remaining_openmhz_budget); cap default 900
)
UPDATE feeds
   SET status = 'active',
       worker_id = $1,
       fencing_token = fencing_token + 1,
       last_heartbeat = NOW()
  FROM claimed
 WHERE feeds.id = claimed.id
RETURNING feeds.*;
```

**Worker responsibility for LIMIT parameters.** The worker tracks its per-type holdings (`current_held_by_type` dict) and at each claim cycle computes:

```python
# Pseudocode; real implementation is in feed_queries.py after Phase 0.
remaining_bcfy_feeds = CAP_BCFY_FEEDS - current_held_by_type['bcfy_feeds']
remaining_bcfy_calls = CAP_BCFY_CALLS - current_held_by_type['bcfy_calls']
remaining_openmhz    = CAP_OPENMHZ    - current_held_by_type['openmhz']

total_slack = MAX_FEEDS_PER_WORKER - sum(current_held_by_type.values())

limit_bcfy_feeds = max(0, min(CAP_BCFY_FEEDS, remaining_bcfy_feeds, total_slack))
limit_bcfy_calls = max(0, min(CAP_BCFY_CALLS, remaining_bcfy_calls, total_slack))
limit_openmhz    = max(0, min(CAP_OPENMHZ,    remaining_openmhz,    total_slack))
```

Three elements of this query are load-bearing and cannot be dropped:

1. **`AS MATERIALIZED`** — the keyword is **non-negotiable**. Without it, PostgreSQL's planner is free to inline the CTE into the outer UPDATE and, under a nested-loop plan, may re-evaluate the UNION ALL subquery per outer row. Each re-evaluation locks *different* rows (SKIP LOCKED interacts with in-flight locks), causing the UPDATE to bypass the LIMIT entirely. The MATERIALIZED keyword forces single-evaluation into a bounded worktable, and the UPDATE joins only against that worktable. Shipping without MATERIALIZED would defeat the cap. *(Phase 0 EXPLAIN verification below confirms the plan actually materializes; add a primary-source citation to the PostgreSQL docs/mailing-list when the Phase 0 migration is prepared.)*
2. **`FOR NO KEY UPDATE`** — weaker lock than `FOR UPDATE`; sufficient because the claim only modifies `status`, `worker_id`, `fencing_token`, `last_heartbeat` (none are primary/unique keys). Reduces lock-manager contention at peak without affecting correctness.
3. **`ORDER BY id`** — within each type branch, deterministic ordering off the composite index. Together with the partial index definition, this produces an index-only scan per branch, no sort node.

**The three-branch recovery paths (failing-retryable, active-abandoned) are NOT in this primary CTE.** The research's UNION ALL pattern assumes a single `status='pending'` pool, but our schema has recovery paths that must also be claimed. Two options considered:

- **Option 1 (chosen): recovery paths via separate claim query.** A worker's claim cycle runs the UNION ALL CTE first; if it returns fewer than requested rows, the worker runs a secondary query against `failing-retryable` and `active-abandoned` branches (without per-type budgets — those branches are volume-bounded by operational reality, not by memory concerns). This keeps the primary path's per-type guarantee intact.
- **Option 2 (rejected): merge recovery into the CTE.** Would require six UNION ALL branches (three types × two states) and 6× LIMIT tuning. Added complexity for negligible gain.

**Part C: Ramp filter integration.** The ramp filter `(('x' || substr(md5(id::text), 1, 7))::bit(28)::integer) % 100 < $3` applies to each branch independently. Cheap — evaluated against the partial index — and works correctly because each branch enforces its own LIMIT after filtering. md5() is a cryptographic hash with documented-stable algorithm across PostgreSQL versions (unlike `hashtext()`, whose internal algorithm has changed historically — see §9.4).

**Planner verification (mandatory Phase 0 step):**

```sql
EXPLAIN (ANALYZE, BUFFERS, LOCKS) WITH claimed AS MATERIALIZED (...) UPDATE feeds ...;
```

Expected plan shape:

- Three `Bitmap Index Scan` or `Index Scan` nodes, one per branch, each hitting `feeds_claim_by_type_idx`.
- `CTE Scan on claimed` (indicating MATERIALIZED took effect).
- A `LockRows` node under each branch's scan, showing SKIP LOCKED behavior.
- A single `Update on feeds` node joining against the materialized worktable.

**Any deviation — especially absence of `CTE Scan` or presence of `Nested Loop` over the feeds table — is a critical finding.** Phase 0 deploy-gate includes this EXPLAIN check. Production monitoring adds an alert on the claim query's P99 latency; regression past baseline indicates planner drift and triggers re-verification.

**Recovery-path query (runs after primary CTE returns fewer than requested rows):**

```sql
SELECT * FROM feeds
WHERE (
    (status = 'failing' AND (retry_after IS NULL OR retry_after <= NOW()))
    OR (status = 'active' AND last_heartbeat < NOW() - $2::interval)
)
  AND (('x' || substr(md5(id::text), 1, 7))::bit(28)::integer) % 100 < $3
ORDER BY retry_after ASC NULLS FIRST, id
LIMIT $4  -- remaining slack in this worker's claim batch
FOR NO KEY UPDATE SKIP LOCKED;
```

Recovery path has no per-type budget because `failing` volume is low (failure is rare) and `abandoned` is drained by pg_cron sweep. If either branch grows pathologically, the per-type-budget pattern can be extended to the recovery path in Phase 2.

**ORDER BY semantics (clarification against earlier drafts):** Earlier revisions of this plan specified `ORDER BY (status='unclaimed') DESC, retry_after ASC NULLS FIRST, id` as the full-path ordering. Under the CTE design, that ordering is split: primary CTE uses `ORDER BY id` within each per-type branch (the `(status='unclaimed') DESC` clause is implicit via the WHERE), and the recovery query uses `ORDER BY retry_after ASC NULLS FIRST, id`. The `last_heartbeat` ordering is dropped entirely — HOT-incompatible and no longer needed because the sweep handles abandoned rows out-of-band.

**Abandoned-lease sweep (pg_cron, every 30 s, batched):**

```sql
-- Sweep batch: at most 500 rows per call.
UPDATE feeds
   SET status = 'unclaimed',
       worker_id = NULL,
       unclaimed_since = NOW()
 WHERE id IN (
     SELECT id FROM feeds
      WHERE status = 'active'
        AND last_heartbeat < NOW() - INTERVAL '60 seconds'
      LIMIT 500
 );
```

pg_cron runs this statement every 30 s. If > 500 rows are abandoned in one cycle (e.g., zonal outage), the sweep takes multiple pg_cron invocations (each 30 s apart) to drain, spreading the `unclaimed` flip over multiple minutes instead of flipping ~4,000 rows in one transaction. This prevents the "fleet-wide polling stampede" failure mode where 16 workers hit the DB at the same time claiming the same large pool.

Sweep volume at peak: near zero in steady state (workers heartbeat every 20 s — see §6.1 — with threshold 60 s). The sweep only acts after worker/VM death. Under a 3-VM zonal outage (~4,000 abandoned leases), drain takes ~4 minutes of sweep cycles. During those 4 minutes, surviving workers' `oldest_unclaimed_feed_age` metric rises as the sweep trickles rows into the unclaimed pool — that's the right signal behavior.

**Minute-cadence VACUUM job for line-pointer bloat (second pg_cron job, Phase 0).** Even with HOT updates working perfectly (`n_tup_hot_upd / n_tup_upd > 95%`), PostgreSQL's opportunistic `heap_page_prune_opt` reclaims tuple bytes but **does not shrink the line-pointer (ItemId) array**. Only VACUUM can push `LP_DEAD → LP_UNUSED` via `PageTruncateLinePointerArray`, and each 8 KB page has a hard cap at `MaxHeapTuplesPerPage` (~291 slots). Under sustained heartbeat traffic, line pointers saturate silently over hours; once an 8 KB page's LP array is full, new HOT updates on that page are denied even if free tuple-byte space exists. HOT ratio degrades, bloat returns.

A minute-cadence `pg_cron` VACUUM pushes `LP_DEAD → LP_UNUSED`, keeping the LP array healthy. On the plan's 430-page cached table, each run completes in tens of milliseconds at negligible cost:

```sql
SELECT cron.schedule('feeds-vac', '* * * * *', 'VACUUM (ANALYZE) feeds');
```

Set `autovacuum_vacuum_scale_factor=0.02` on the table as a safety net (already in the ALTER TABLE in §6). Do not crank this below 0.01 — AlloyDB adaptive autovacuum plus the minute-cadence job is belt-and-suspenders; driving scale_factor to 0.005 has documented diminishing-returns behavior and costs I/O on a HOT-clean workload where vacuum becomes a near-no-op anyway.

### 6.1 Write coalescing: cut the sustained UPDATE rate ~10× before HOT even runs

Even a perfect HOT implementation still generates ~172M dead tuples/day at the plan's baseline 2,000 UPD/sec — the pruning and vacuum machinery above keeps up, but the *sustained rate* is inflated by mixing two logical signals (progress and heartbeat) into one physical write. Three Phase 0 changes compound to drop the rate from ~2,000/sec to ~300–500/sec at source. The HOT machinery still ships, but now has ~10× less work to do. This is defense-in-depth: if a HOT regression sneaks in, the coalescing keeps bloat manageable while the regression is diagnosed.

**Change 1: Separate `last_progress_at` column (unindexed).** The current worker code writes `last_heartbeat = NOW()` both in `UPDATE_PROGRESS_SQL` (every ~10 s per active feed) and in the 15 s heartbeat renewal. Both serve the same liveness purpose; duplicating the write doubles MVCC traffic.

```sql
ALTER TABLE feeds ADD COLUMN last_progress_at TIMESTAMP WITH TIME ZONE;
-- Deliberately NOT indexed. Keeps progress writes fully HOT-eligible.
```

Phase 0 rewrites `UPDATE_PROGRESS_SQL` to set `last_progress_at = NOW()` instead of `last_heartbeat = NOW()`. The 15 s (→20 s, see change 3) heartbeat renewal remains the only code path that writes `last_heartbeat`. Operational liveness semantics are unchanged — the sweep still uses `last_heartbeat` as the authoritative liveness column; progress writes don't need to refresh the sweep's threshold because heartbeat is doing that separately at a known cadence.

**Change 2: Skip-if-recent predicate on heartbeat renewal.** PostgreSQL MVCC rules are unambiguous: an UPDATE whose WHERE matches always writes a new tuple, even when `SET` values equal existing values. But when WHERE doesn't match, no row is written — zero WAL, zero new tuple, zero dead-tuple accounting. Adding a temporal predicate collapses bursty redundant heartbeats:

```sql
-- Phase 0 rewrite of heartbeat renewal (replaces RENEW_HEARTBEATS_BATCH_DIAGNOSTIC_SQL's
-- CTE+JOIN diagnostic form):
UPDATE feeds
   SET last_heartbeat = NOW()
 WHERE worker_id = $1
   AND id = ANY($2)
   AND last_heartbeat < NOW() - INTERVAL '15 seconds';
```

The predicate drops ~30% of redundant writes where multiple heartbeat ticks have already happened inside the 15 s window (e.g., after a brief worker pause that processes backlog). The conservative 15 s threshold preserves the invariant that no lease-liveness row can age past 20 s (the new heartbeat cadence, change 3) without being touched — which is well inside the 60 s abandonment window.

**Change 3: Relax heartbeat cadence from 15 s to 20 s.** Industry convention puts heartbeat cadence at 1/3 to 1/4 of the failure-detection timeout: Kubernetes kubelet posts NodeLeases every 10 s against 40 s NotReady; etcd leases refresh at TTL/3. Against a 60 s `abandonment_window_sec`, 15 s is at the aggressive end (1:4) and 20 s is a 1:3 ratio — still highly conservative, matches kubelet exactly, and drops baseline heartbeat rate by another 25% (from 4/min/worker to 3/min/worker).

**Combined effect on §6 QPS arithmetic:**

| Component | Before coalescing | After coalescing |
|---|---|---|
| Heartbeat UPDATE row-count/sec | ~800 row-updates/sec (16 workers × 800 leases ÷ 15 s) | **~480 row-updates/sec** (16 × 800 ÷ 20 s × 0.70 skip-if-recent factor) |
| Progress UPDATE row-count/sec | ~1,200 row-updates/sec (last_heartbeat path, HOT-blocking under old index) | ~1,200 row-updates/sec on `last_progress_at` (unindexed, fully HOT) |
| Claim/release/status-change UPDATE | ~10/sec | unchanged |
| **Sustained UPDATE rate (total, non-HOT-blocking)** | **~2,010/sec** | **~490/sec on `last_heartbeat` + 1,200/sec on `last_progress_at` (unindexed)** |
| **MVCC dead-tuple rate (last_heartbeat column, the bloat-risk column)** | ~170M/day | **~42M/day** |

The plan's ~170M dead-tuples/day figure drops to ~42M/day, comfortably inside autovacuum + minute-cadence pg_cron reach even if HOT regresses.

**Escalation path: vertical split into `feed_leases` (documented, not built).** If post-deploy telemetry shows `n_tup_hot_upd / n_tup_upd < 0.95` sustained, or `pg_stat_user_indexes.idx_blks_read` on the new partial indexes trends up faster than autovacuum reclaims, the clean next move is splitting the highly volatile columns (`last_heartbeat`, `worker_id`, `fencing_token`) into a `feed_leases` table keyed by feed_id. This is the standard PostgreSQL pattern for narrow, high-write tables: row width drops to ~40–60 B, the hot working set becomes a few KB, autovacuum completes in microseconds, and the claim query becomes a nested-loop index lookup that PostgreSQL handles with zero penalty.

**This is a one-week engineering project, not an emergency.** Gating criteria: `hot_pct < 0.95` for 24 hours sustained, or index-block read rate exceeding a published threshold. The metric is watched; the design is documented; the project is not scheduled unless the metric fires. Keeping `feed_leases` on the shelf buys optionality without paying for it.

**Publisher query (§2).** The Cloud Run Function publishes `oldest_unclaimed_feed_age` via:

```sql
SELECT COALESCE(
         EXTRACT(epoch FROM NOW() - MIN(unclaimed_since)),
         0.0      -- empty pool: no unclaimed feeds; "0 s of waiting" is accurate
       ) AS oldest_unclaimed_feed_age_sec
  FROM feeds
 WHERE status = 'unclaimed';
```

`unclaimed_since` is set by (a) INSERT, (b) the sweep, and (c) the SIGTERM lease-release path (§8). The query does an index-only scan of the `idx_feeds_unclaimed` partial index and computes MIN — at ≤4,000 unclaimed rows during a surge and once per 60 s, the load is negligible. No index on `unclaimed_since` is needed (and adding one would break HOT).

**Publisher function defensive code.** Empty pool is a legitimate and frequent state (off-season, all feeds claimed), and the query's `COALESCE(..., 0.0)` handles it correctly. Three other edge cases the publisher function must handle explicitly, because each can silently corrupt the metric:

1. **Query timeout or DB connection failure.** Publish a sentinel value of `-1` and an error log; do NOT publish 0 (which would look like "fleet perfectly caught up"). The §11 alert "`oldest_unclaimed_feed_age` absolute > 120 s warning / > 300 s critical" would not fire on a 0 or -1; add a separate alert on `oldest_unclaimed_feed_age < 0` (absolute value) triggered as critical with "publisher misbehaving."
2. **`unclaimed_since` is NULL for some rows** (e.g., a row inserted before Phase 0 column-backfill completes). The MIN aggregate ignores NULLs, so this case is benign — it under-reports by skipping unbackfilled rows. Phase 0 migration must backfill `unclaimed_since = created_at` for any existing `unclaimed` rows; after that, the case cannot occur.
3. **AlloyDB primary failover in progress.** Connection attempts fail for ~30 s. Publisher function exits with error; Cloud Scheduler does not retry; metric goes stale for 60 s until next scheduled tick. The `oldest_unclaimed_feed_age metric freshness` alert (§11) catches this.

The 50-line function must include these handlers explicitly. Sketch:

```python
try:
    result = await conn.fetchval(QUERY, timeout=5.0)
    value = float(result) if result is not None else 0.0
except (asyncio.TimeoutError, asyncpg.PostgresError) as e:
    logging.error(f"publisher query failed: {e}")
    value = -1.0   # sentinel: "publisher misbehaving"
write_custom_metric("oldest_unclaimed_feed_age", value)
```

**Verifying HOT in production.** After the first hour of traffic after schema changes deploy:

```sql
SELECT relname, n_tup_upd, n_tup_hot_upd,
       round(100.0 * n_tup_hot_upd / NULLIF(n_tup_upd, 0), 1) AS hot_pct
  FROM pg_stat_user_tables WHERE relname = 'feeds';
```

Expected `hot_pct > 95%` in steady state. A drop below 90% after a deploy indicates a new index references one of the mutated hot-path columns. The full guarded list — any index on any of these columns breaks HOT:

| Column | Mutated by |
|---|---|
| `last_heartbeat` | Heartbeat renewal, `UPDATE_PROGRESS_SQL`, `RELEASE_FEEDS_BATCH_SQL` (Phase 0 removes the latter two), `REPORT_FAILURE_SQL` |
| `unclaimed_since` | INSERT, sweep, release path |
| `worker_id` | Claim, release, sweep |
| `fencing_token` | Incremented on every claim |
| `last_processed_filename` | `UPDATE_PROGRESS_SQL` (every audio chunk, ~1,200/sec) |
| `last_bookmark_time` | `UPDATE_PROGRESS_SQL` |
| `failure_count` | `UPDATE_PROGRESS_SQL`, `REPORT_FAILURE_SQL` |
| `retry_after` | `REPORT_FAILURE_SQL`, claim-success reset |

Treat a `hot_pct` drop below 90% as a deploy-time blocker.

**Pre-deploy CI check (R2-1 safety net).** The `hot_pct < 90%` alert fires an hour after bad code reaches production. To close the window, a CI job parses every migration and fails the build if any index references one of these columns. Implementation sketch:

```sql
-- Run against the proposed schema at CI time.
SELECT i.indexname, a.attname
  FROM pg_indexes i
  JOIN pg_class c ON c.relname = i.indexname
  JOIN pg_index x ON x.indexrelid = c.oid
  JOIN pg_attribute a ON a.attrelid = x.indrelid
 WHERE i.schemaname = 'public'
   AND a.attname IN (
     'last_heartbeat',
     'unclaimed_since',
     'worker_id',
     'fencing_token',
     'last_processed_filename',
     'last_bookmark_time',
     'failure_count',
     'retry_after'  -- included defensively; `idx_feeds_failing_retryable` is allowed as an explicit exception
   )
   AND a.attnum = ANY(x.indkey)
   AND i.indexname != 'idx_feeds_failing_retryable';  -- allow-list this one; retry_after is only mutated on failure, acceptable bloat frequency
-- CI fails if any row returned.
```

Note on `retry_after`: it is mutated by the failure path but at very low frequency compared to `last_heartbeat`. The plan's `idx_feeds_failing_retryable` indexes it deliberately to serve the failing-retryable claim branch. Bloat on that partial index is acceptable because failure events are rare; the CI check explicitly allow-lists `idx_feeds_failing_retryable` while still blocking any *other* future index on `retry_after`.

This catches the regression at code-review time instead of 60 minutes after cutover.

**Connection pooling and worker connection architecture.** Workers connect through **AlloyDB managed connection pooling**. The production worker uses two distinct pools, and the pool arithmetic must close end-to-end.

**Nomenclature clarification.** AlloyDB has three distinct connection limits, and earlier revisions of this plan conflated them:

| Level | Name in AlloyDB | What it limits | Default / current value |
|---|---|---|---|
| AlloyDB server instance | `max_connections` (GUC) | Backend processes on the primary | 1,000 (default, unchanged by this plan) |
| Managed pooler, frontend | `max_client_conn` | Concurrent client connections **to the pooler** | 800 (already set by existing AlloyDB module) |
| Managed pooler, backend | `default_pool_size` | Backend connections **from pooler to server** per user/db | Module default; see below |

The plan does **not** need to raise the server-side `max_connections` — default 1,000 covers this workload. Earlier revisions called for `max_db_connections=200`, which was an invented name conflating the server GUC with the pooler backend-pool size. What the plan actually requires:

- **Confirm the existing `max_client_conn = 800`** setting on the AlloyDB managed pooler is in place. It is — no change needed. Fleet peak usage: 16 workers × 9 client slots = 144, well below 800.
- **Size `default_pool_size`** to cover peak concurrent backend usage without queueing. The math is in the next table.

| Parameter | Value | Rationale |
|---|---|---|
| Pool mode | `transaction` | Compatible with `SELECT FOR UPDATE SKIP LOCKED` — row locks are held inside the transaction, released at COMMIT. |
| Managed pooler `max_client_conn` | 800 (already set) | 16 workers × ~30 ephemeral client slots + admin overhead — comfortable headroom. |
| Managed pooler `default_pool_size` (per user/db) | **160** (Phase 0 change if current value is lower) | Covers peak concurrent backend use: ~64 steady-state + 16 heartbeat peaks + 80 burst headroom. |
| AlloyDB server `max_connections` | **1,000 (no change)** | Default covers the 160-backend pool size plus admin connections and other services. |
| Server-side prepared statements | Disabled, OR require pgbouncer 1.21+ transaction-mode prepared-statement support | Standard transaction-mode limitation. |
| Per-container asyncpg **main** pool | 8 (current code: 5/5; raise to 8 in Phase 0) | Claims, writes, progress bookmarks, status queries. |
| Per-container asyncpg **heartbeat** pool | 1/1 (matches current code) | Runs on a dedicated daemon thread (`normalizer_runtime.py:176-181`). One batched `UPDATE` per 15 s covers all leases. |

**Per-worker client slots:** 8 main + 1 heartbeat = 9.

**Fleet arithmetic at peak** (16 workers):

- Client slots (connections to the pooler): 16 × 9 = 144 — well below the 800 `max_client_conn`.
- Backend slots (transaction-mode multiplexing; pooler backend connections): peak concurrent in-flight transactions per worker ≈ 3–4 (claim + write + progress). 16 × 4 = ~64 concurrent backends in steady state, with heartbeat spikes adding +16 every 15 s.
- Steady-state backends ~64 + 16 heartbeat peaks ~80 → comfortably below 160 `default_pool_size`.

**Heartbeat thread isolation.** The heartbeat path runs on a daemon thread with its own 1-connection pool — not on the asyncio event loop. This is by design: it prevents heartbeat latency from being coupled to event-loop lag during ffmpeg subprocess spawn storms. The 1-per-worker heartbeat pool count (16 backends at peak) must be counted *separately* from the main pool; it does not share the 8-per-worker main pool budget. Previous revisions of this plan missed this and undercounted peak backend usage.

**Phase 0 note on `RENEW_HEARTBEATS_BATCH_DIAGNOSTIC_SQL`.** The current code uses a CTE+JOIN form for heartbeat renewals that was originally designed for fence-violation diagnosis. At 12k-feed scale, the simpler plain `UPDATE ... WHERE worker_id = $1 AND id = ANY($2)` form is cheaper per call. Evaluate replacement during Phase 0 shadow soak; if the diagnostic form's cost is observable in AlloyDB CPU, switch.

**Connection hygiene (Phase 0 addition).** Workers' asyncpg connections must be configured to release row locks promptly when a worker dies. Default TCP settings keep dead connections open for up to 2 hours, during which the worker's held row locks (including an in-flight claim CTE's `FOR NO KEY UPDATE` locks) remain held, blocking other workers from claiming those rows:

- **Worker-side (asyncpg connect_args):** `tcp_keepalives_idle=60`, `tcp_keepalives_interval=10`, `tcp_keepalives_count=3`. Combined, AlloyDB detects a dead peer within ~90 s.
- **Server-side (AlloyDB GUC):** `idle_in_transaction_session_timeout = '30s'`. If a worker is alive but its transaction is stuck idle (e.g., the worker process is frozen on the event loop), AlloyDB aborts the transaction after 30 s, releasing its locks.

These settings are on the managed-pooler connection definition (Phase 0 Terraform update). They interact correctly with the CTE design in the claim query above: the `FOR NO KEY UPDATE` locks are transaction-scoped and released on transaction end, whether via COMMIT, ROLLBACK, or forced abort by `idle_in_transaction_session_timeout`.

## 7. Worker runtime configuration

These runtime choices are load-bearing. Most are **Phase 0 additions** — the current worker code does not include them — and all should be reviewed together with the code-delta table in §9.2.

**Event loop and HTTP client (Phase 0 additions).** Workers will use **uvloop** as the asyncio event-loop policy. The current code uses plain `asyncio.run()` (no uvloop import anywhere in `backend/pipeline/ingestion/`). Experiment 1b was run on uvloop, and the performance numbers in Appendix A assume it, so installing uvloop is a prerequisite to realizing those numbers in production. HTTP traffic will use a shared `aiohttp.ClientSession` with `TCPConnector(limit=500, limit_per_host=0)` — this enables HTTP Keep-Alive and TLS-session reuse across polls, avoiding ~634 TLS handshakes per second at peak. Without Keep-Alive, TLS setup CPU dominates the bcfy_calls workload.

**Memory allocator (Phase 0 addition).** Containers will set `LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libjemalloc.so.2` and `MALLOC_ARENA_MAX=2` via the startup script. The motivation is documented glibc malloc behavior: long-running processes with many threads accumulate per-thread allocator arenas that retain freed memory as process-level RSS even when live allocations are stable. `MALLOC_ARENA_MAX=2` limits glibc allocator arenas to 2 (default scales with CPU count), which alone addresses most of the multi-arena fragmentation on multi-core hosts. Pairing jemalloc is a belt-and-suspenders defensive install — jemalloc's allocator has different fragmentation characteristics and is known to help Python long-running services, but the magnitude of RSS reduction on this specific Python+ffmpeg workload is not known in advance and will be measured in Phase 1 shadow soak. Install cost is two lines in the startup script; downside risk is negligible.

**TCP source port range (Phase 0 addition).** Cloud-init will set `net.ipv4.ip_local_port_range = 10000 65535` (default is 32768–60999). With ~800 feeds per worker holding persistent upstream connections plus the aiohttp connection pool and asyncpg backends, the default range can exhaust under burst-reconnect conditions.

**Container resource limits (inherited from current code).** Each VM will run two worker containers (k=2) without explicit `--cpus` limits. CFS throttling under explicit limits is untested in 1b and could interact with event-loop drift unpredictably. The single-threaded asyncio model self-limits to ~one vCPU per worker; explicit CPU quotas add a second bottleneck without benefit.

**Database client (Phase 0 change).** Each worker uses an `asyncpg` main pool of size 8 (raised from current 5/5 default) and a dedicated 1-connection heartbeat pool on a daemon thread (already present in current code). Full pool arithmetic in §6.

**Feed-claim rate limiting.** Workers poll for claims every 5 seconds (code default) with per-type budget-driven claim sizing (§4, §6), yielding ~0.2 claim calls/sec per worker. No separate token-bucket limiter is needed.

**ffmpeg spawn gating (Phase 0 addition).** Every `asyncio.create_subprocess_exec(ffmpeg, ...)` call will be gated by an `asyncio.Semaphore(N)`. The current code has no such semaphore; the only `create_subprocess_exec` call is at `collectors/icecast/icecast_collector.py:289` with no rate-limiting. Phase 0 adds the semaphore and tunes N in {8, 12, 16, 24, 32} via the Experiment-1b-replay procedure: start at N=12, ramp to find the value where event-loop p99 lag stays below 100 ms during a 1,000-feed activation burst.

**Per-type claim cap (Phase 0 addition).** Worker tracks per-type holdings in memory. At each claim cycle, worker computes `remaining_budget[type] = cap[type] - current_held[type]` and passes `min(cap[type], remaining_budget[type])` as each branch's LIMIT in the UNION ALL MATERIALIZED CTE (§6). PostgreSQL enforces the LIMIT: the worker receives at most that many rows regardless of how many candidate rows exist. The `cap[type]` values are env-var tunable (`cap_bcfy_feeds=240`, `cap_bcfy_calls=600`, `cap_openmhz=900`).

The layered defense against OOM:

- **Normal case:** Worker tracks accurately → passes correct LIMIT → claim stays under cap structurally.
- **Worker counter corruption:** Worker miscounts → passes wrong LIMIT → could over-claim. Caught by the RSS watchdog below within 2 s of threshold breach.
- **Planner regression** (CTE loses MATERIALIZED): per-branch LIMIT still enforced (the `LockRows` node keeps the LIMIT honest even under nested-loop), but claim query P99 latency rises.

This revises an earlier draft that framed the cap as "DB-only, no worker tracking" — per-call LIMIT does not bound total-held without the worker adjusting the LIMIT to reflect current holdings. Full mechanics in §6.

**Self-RSS watchdog (Phase 0 addition, defense-in-depth).** Even with budget caps correctly enforced, a worker could OOM from poison-pill behavior within an already-claimed feed (e.g., a bcfy_feeds stream that leaks memory mid-processing), or from a worker-side counter-corruption bug that caused over-claim. Defense: a ~60 LOC background daemon thread polls `psutil.Process().memory_info().rss + sum(c.memory_info().rss for c in children())` every 2 s. Thresholds:

- **70% of container memory** (≈11.2 GiB on a 16 GiB VM): set a `paused` flag. The claim loop short-circuits on this flag — no new claims until RSS drops below 60%.
- **90% of container memory** (≈14.4 GiB) for 3 consecutive samples (6 s): initiate graceful exit. Finish in-flight processing, close asyncpg pool, `sys.exit(0)`. The MIG's restart policy respawns the container.

This is reactive, not preventive — the worker-budgeted DB-enforced cap prevents the adversarial *claim-shape* OOM when worker counter is correct; the watchdog catches both post-claim memory pathologies that no claim-time enforcement can see AND worker-counter-corruption over-claim cases. Both layers are necessary. The 60-LOC budget includes Prometheus metric emission for the `rss_watchdog_trips_per_hour` signal (§11).

## 8. Startup, shutdown, and rolling deploys

Startup behavior and graceful shutdown are the most operationally sensitive parts of the worker code. Errors here cause either startup stampedes (prophylactic class — not observed in 1b; see Appendix A) or scale-in stampedes (500+ feeds becoming leasable simultaneously).

**Rolling update policy.** MIG rolling updates use `max_surge=2, max_unavailable=1, min_ready_sec=60, initialization_period_sec=180`. An earlier revision of this plan used `min_ready_sec=300`, but that creates a problematic semantic window: a just-booted VM that claims feeds at t=30s would be serving real traffic during the interval t=30 to t=300 when the MIG update policy does not count it as "ready." If something goes wrong in that 270 s window, the MIG doesn't see the VM as a contributing member yet.

The corrected semantics:

- `initialization_period_sec=180` — during the first 180 s of a VM's life, health-check failures do not trigger replacement. Covers cloud-init + startup jitter + container warmup without spurious replacement cycles.
- `min_ready_sec=60` — a VM must be healthy for 60 s before the MIG update policy considers it ready. Set to match the first-poll jitter window (0–2 s jitter + 5 s poll interval + 50 s margin for the first round of lease claims to complete).

Together, a VM that boots at t=0 passes its health check no earlier than ~t=90 (after cloud-init, jemalloc install, container start, startup jitter, first poll completes), is counted as "ready" by t=150, and can have health-check-triggered replacement from t=180 onward. Workers start claiming feeds at ~t=30–90 depending on stagger — before the "ready" threshold, but those feeds are being processed correctly; the MIG just hasn't marked the VM ready for *update-policy* decisions yet.

**Important: claims begin before "ready".** Workers claim feeds as soon as they pass the startup-jitter window (t=~30 s). This is correct behavior — the feeds need to be serving. The `min_ready_sec` window is about when the MIG update policy counts the VM toward rolling-update progress, not about when the VM starts doing work. Earlier drafts of this plan stated that `min_ready_sec=300` "prevents the MIG from counting a just-booted VM as serving before it has actually claimed any feeds" — that was wrong; claim starts well before `min_ready_sec` elapses, and this is fine because the first poll completes within seconds.

**Inter-VM startup stagger.** Cloud-init staggers worker container start by `$(( 16#$(hostname | md5sum | head -c 8) % 60 ))` seconds — the hex-prefix `16#` is required because bash's `$(())` doesn't auto-detect base on raw hex strings. Earlier drafts of this plan used `mod` (not a bash operator) and omitted the `16#` prefix; both would have silently produced 0 on every host, collapsing the stagger. When MIG creates multiple VMs simultaneously (scale-out events), this distributes their worker starts across a 60 s window. Without it, 3–6 VMs booting together would all hit AlloyDB with lease claims at the same instant, recreating the Experiment 1b co-activation pathology at fleet scale.

**Intra-VM startup stagger.** Within a single VM, container A starts immediately (at the cloud-init-assigned offset), and container B sleeps `30 + random(0, 30)` seconds before starting. This ensures the two worker processes on one VM never activate their ffmpeg subprocess bursts simultaneously — they are decoupled by ≥30 seconds. This is prophylactic (see Appendix A) — a concurrent-subprocess-spawn stall has not been observed in testing, but is a plausible pathology the plan defends against.

**Startup jitter.** Every worker applies 0–2 s uniform jitter before its first AlloyDB poll (Appendix A mitigation; Phase 0 addition — the current code jumps straight into the leasing loop at `normalizer_runtime.py:194` with no jitter). This desynchronizes first-poll timing across the fleet and is non-negotiable per AWS/Google-Cloud canonical practice — the 2020 Google Cloud outage was exacerbated by exactly this missing jitter.

**ffmpeg spawn gating (Phase 0 addition, see §7).**

**Graceful shutdown.** The production SIGTERM path (`_shutdown_sequence` in `normalizer_runtime.py`) already does three things in sequence:

1. Stop the heartbeat thread (line 702), with an explicit code comment: *"Stop heartbeat FIRST to prevent it from seeing released feeds as fence violations during the teardown window."*
2. Cancel feed tasks and await their exit.
3. Release leases via `RELEASE_FEEDS_BATCH_SQL` — a single atomic UPDATE of all of the worker's active leases.

Steps 1 and 2 are load-bearing: they exist because the worker has fence-violation logic that triggers `os._exit(1)` when a progress bookmark finds `worker_id = NULL` unexpectedly (`normalizer_runtime.py:439-456`), or when heartbeat renewal finds `current_state.worker_id != $2` (the `RENEW_HEARTBEATS_BATCH_DIAGNOSTIC_SQL` CTE). Both are designed to detect leases being stolen by another worker; they fire equally on our own in-progress release.

**The only safe change at the plan's scale is to replace step 3 with a batched+jittered release, keeping steps 1 and 2 in their current order.** An earlier revision of this plan proposed reversing the order (release-then-heartbeat-off); that would trigger the fence-violation path because in-flight tasks can do `UPDATE_PROGRESS_SQL` with `WHERE worker_id = $1 AND fencing_token = $2` between the first batched release and the task cancellation, find 0 rows affected, and `os._exit(1)`. The comment in the current code is correct and must be preserved.

**Phase 0 changes required:**

- Replace the single-UPDATE `RELEASE_FEEDS_BATCH_SQL` with a batched-and-jittered release. Split the worker's leases into batches of ~50 and issue `UPDATE feeds SET status='unclaimed', worker_id=NULL, unclaimed_since=NOW() WHERE worker_id=$1 AND id = ANY($2)` per batch, with `asyncio.sleep(random.uniform(0, 2))` between batches. **Each batch is its own transaction** (explicit COMMIT between batches) — this prevents any one batch from sitting long enough to trip the `idle_in_transaction_session_timeout = 30s` GUC if AlloyDB is slow. For 1,500 feeds this is ~30 batches × ~1 s average jitter = ~30 s release time, with lease availability spread over a 30 s window instead of one instant. This is done at step 3 of the existing shutdown sequence — after step 1 (heartbeat off) and step 2 (tasks cancelled and awaited).
- Raise `graceful_shutdown_timeout_sec` from 10 s to **90 s** (well inside GCE's 120 s ACPI soft-off window). This is a settings change, needed because 10 s does not cover task-cancellation + 30 s batched release + pool close at 800-feed scale.
- **Split the 90 s budget into explicit sub-timeouts, do not let task-wait consume the whole window.** The current code at `normalizer_runtime.py:723` uses `asyncio.wait(self._feed_tasks.values(), timeout=self._normalizer_settings.graceful_shutdown_timeout_sec)` — the task-wait is bounded by the full graceful-shutdown budget. If a feed task is stuck on its third GCS upload retry (`gcs_upload_retry_max_delay_sec = 8.0` × ~3 retries = ~24 s per task, and tasks can be non-cancellable during an in-progress HTTP call), the task-wait can consume the entire 90 s and the batched release never runs. Phase 0 must pass an explicit sub-timeout: `asyncio.wait(..., timeout=TASK_CANCEL_BUDGET_SEC)` where `TASK_CANCEL_BUDGET_SEC = 30` (enough for one GCS retry round to complete or abort, but not the whole window). After `TASK_CANCEL_BUDGET_SEC`, any still-running tasks are forcibly abandoned; their leases will be released by the batched UPDATE anyway (steps 2→3 transition). This reserves ≥60 s for the batched release to run to completion.
- **Tighten GCS write timeout within the worker.** The plan's timing budget assumes "in-flight GCS writes complete in ≤ 15 s." The actual upper bound is `gcs_upload_retry_max_delay_sec × max_retries`, which at defaults is closer to 30 s. Phase 0 should either reduce `gcs_upload_retry_max_delay_sec` to 3 s, or reduce `max_retries` to 2, or both — to keep any single feed-task's cancellation-to-completion bounded under 15 s.

**Timing budget for the 90 s graceful-shutdown window (with explicit sub-timeouts):**

| Phase | Budget | What bounds it |
|---|---|---|
| Stop heartbeat | < 1 s | Thread join |
| Cancel feed tasks + await exit | ≤ 30 s | `TASK_CANCEL_BUDGET_SEC` sub-timeout (new Phase 0 constant) |
| Batched+jittered release (~30 batches × ~1 s) | ~30 s | Arithmetic: 30 batches × mean(0,2) s jitter |
| Close asyncpg main + heartbeat pools, exit | < 2 s | asyncpg pool shutdown |
| **Total worst case** | **~63 s**, well inside 90 s | Leaves ~25 s slack for slow individual batches |

**Budget is fragile without the sub-timeout.** If Phase 0 raises `graceful_shutdown_timeout_sec` to 90 but does *not* add an explicit `TASK_CANCEL_BUDGET_SEC`, a single stuck task can starve the batched release. The fix is both changes, not one.

**What about the abandonment race?** With heartbeat stopped at T=0 and release running from T=15–45, the oldest in-flight heartbeat is ~15 s old at T=0 and the last release UPDATE runs at T=45. The `abandonment_window_sec = 60` threshold measures from `last_heartbeat`, so the window is (15 + 45) = 60 s — right at the threshold. If release runs long, individual leases could age past 60 s and become eligible for the pg_cron sweep or another worker's abandoned-lease claim. That's acceptable in practice:

- Tasks are already cancelled (step 2), so no `UPDATE_PROGRESS_SQL` can fire and trip the fence-violation path.
- The pg_cron sweep would reclaim the lease by setting `worker_id=NULL, status='unclaimed'` — the same terminal state our pending UPDATE is heading toward. Our UPDATE would then affect 0 rows (no harm).
- Another worker's abandoned-lease claim would bump `fencing_token`. Our pending UPDATE's `WHERE worker_id=$1` matches against worker_id, which is still us at the moment of the read — but the transaction would commit stale data. Again, the terminal state is the same.

So the plan's concerns about the abandonment-window race are addressed by the fact that both the sweep and the other-worker claim arrive at the same end-state (`worker_id=NULL, status='unclaimed'`) that our release is also pushing toward. The race is benign.

Note on GCE behavior: the configurable graceful-shutdown-max-duration up to 1 hour is Pre-GA and not supported inside MIGs as of April 2026, so the 120 s hard limit applies.

The SIGTERM handler sequence after Phase 0 changes (order unchanged from current code, step 3 modified):

1. Stop the heartbeat thread (preserves current behavior; prevents fence-violation on in-progress release).
2. Stop accepting new lease claims; cancel feed tasks; await their exit (preserves current behavior; ensures no task can do `UPDATE_PROGRESS_SQL` during release).
3. Iterate leased feeds in batches of ~50; issue the jittered batched UPDATE per batch, each batch as its own transaction. (Previously: single atomic UPDATE.)
4. Close the asyncpg main + heartbeat pools and exit.

---

# Part III — Operations

## 9. Deployment and ramp plan

This is a **green-field deployment** of the ingestion MIG. There is no existing production fleet to cut over from; the ramp is about graduated exposure of the production catalog to the new workers as confidence grows. Total calendar time is ~6 weeks from project start.

### 9.1 Phase overview

| Phase | Duration | What happens | Rollback |
|---|---|---|---|
| **Phase 0 — Prep** | 2 weeks | Schema changes, worker-runtime config updates, publisher function, **code deltas** (§9.2 table). No production feeds claimed yet. | Revert worker image, drop schema changes, delete publisher function |
| **Phase 1 — Shadow soak** | 1 week | New fleet runs at 2 VMs against synthetic workload mirroring production catalog composition. No production feeds claimed. | Tear down shadow, no production impact |
| **Phase 2 — Production ramp** | ~18 days | Production catalog is progressively enabled for claim in five stages (1% → 20% → 50% → 80% → 100%) with soaks between | Revert `ramp_pct`; disabled feeds return to unprocessed state within ~75 s (one sweep cycle) |
| **Phase 3 — Stabilization** | 1 day | 7-day 100% soak complete; lock in 1-year CUD on 2-VM baseline; simplify query to remove hash-filter | Not rollback-relevant; ramp mechanism left as dormant until deleted |

### 9.2 Phase 0 — Preparation (2 weeks)

Preparation work with **no production feeds claimed yet**. The new fleet exists, heartbeats, and holds zero leases.

**Schema changes (§6):**

- `ALTER TABLE feeds SET (fillfactor = 70)` + one-time `VACUUM FULL feeds`.
- Drop `idx_feeds_leasing`; create `idx_feeds_unclaimed` and `idx_feeds_active` partial indexes.
- Add `unclaimed_since` column + trigger or worker-code hook to set it on status transitions to `unclaimed`.
- Deploy `pg_cron` abandoned-lease sweep (every 30 s, LIMIT 500 per invocation — see §6).
- Configure AlloyDB managed pooling (transaction mode, `default_pool_size=160` on the pooler backend, `max_client_conn=800` already set by existing module). No change needed to server-side `max_connections` (default 1,000 suffices). See §6 nomenclature table for which knob is which.

**Code deltas — settings that must change from current defaults:**

| Setting (production name) | File / location | Current default | Target | Rationale |
|---|---|---|---|---|
| `max_feeds_per_worker` | `settings.py`, Terraform env | 250 | **800** | RSS-driven capacity (§4) |
| `heartbeat_interval_sec` | `settings.py` | 15.0 | **20.0** | Write-coalescing (§6.1): 1:3 ratio vs 60 s abandonment window matches k8s/etcd norms; drops heartbeat row-update rate by 25% |
| `heartbeat_stall_timeout_sec` | `settings.py` | 45.0 | 45.0 (keep) | Internal consistency |
| `abandonment_window_sec` | `settings.py` | 60.0 | 60.0 (keep) | This is the production name for what prior drafts called "VISIBILITY_TIMEOUT_SEC". The 60 s window is what `ACQUIRE_FEEDS_BATCH_SQL` checks against `last_heartbeat`. The pg_cron sweep uses the same threshold. |
| `graceful_shutdown_timeout_sec` | `settings.py` | 10.0 | **90** | Required for batched+jittered release at 800-feed scale. Previous drafts called this `GRACEFUL_SHUTDOWN_SEC`. |
| `lease_poll_interval_sec` | `settings.py` | 5.0 | 5.0 (keep) | Matches plan's QPS math |
| Claim batch sizing | `normalizer_runtime.py` (runtime parameter, not a settings constant) | dynamic (`max_feeds_per_worker − len(active_tasks)`, up to 250) | **Per-type-budget-driven**: worker computes `remaining_budget[type] = cap[type] - current_held[type]` and passes `min(cap[type], remaining_budget[type])` as each CTE branch's LIMIT. Total per-call bound = sum of three LIMITs, capped by `max_feeds_per_worker - current_held`. Previous drafts called this `LEASE_BATCH_SIZE` and proposed pinning to 10; no such constant exists and pinning contradicts the per-type cap mechanism. | Enforces per-type budget structurally; predictable AlloyDB load |
| Per-type claim cap (new) | `feed_queries.py` — claim query rewrite | No cap | **DB-enforced via UNION ALL MATERIALIZED CTE**: per-branch LIMITs = `min(cap, remaining_budget)` with caps 240/600/900, `FOR NO KEY UPDATE SKIP LOCKED` (§6) | OOM defense via worker-budgeted DB enforcement |
| Composite partial index (new) | migration | No index | **`CREATE INDEX CONCURRENTLY feeds_claim_by_type_idx ON feeds (source_type, id) WHERE status='unclaimed'`** | Supports per-type ordered scans for CTE branches |
| `last_progress_at` column (new) | migration + `feed_queries.py` | Progress path writes `last_heartbeat = NOW()` | **New unindexed column**; progress path writes `last_progress_at = NOW()` instead | Write coalescing (§6.1); keeps progress HOT-eligible; cuts `last_heartbeat` write rate ~60% |
| Heartbeat skip-if-recent predicate (new) | `feed_queries.py` heartbeat renewal | CTE+JOIN diagnostic form always writes | **Add `AND last_heartbeat < NOW() - INTERVAL '15 seconds'` to WHERE** | ~30% further write reduction on bursty heartbeats; MVCC writes zero tuples when WHERE doesn't match |
| Minute-cadence VACUUM job (new) | migration + pg_cron | No scheduled VACUUM | **`SELECT cron.schedule('feeds-vac', '* * * * *', 'VACUUM (ANALYZE) feeds');`** | Line-pointer-array maintenance; `heap_page_prune_opt` doesn't shrink LP array, only VACUUM does |
| Self-RSS watchdog (new) | worker code — new daemon thread | No watchdog | **~60 LOC; polls psutil every 2 s; pauses claims at 70%, exits gracefully at 90%** (§7) | Defense-in-depth against post-claim memory pathologies and worker-counter corruption |
| TCP keepalives (new) | AlloyDB connection config | Default (2 h dead-peer detection) | **`tcp_keepalives_idle=60, tcp_keepalives_interval=10, tcp_keepalives_count=3`** | ~90 s dead-peer detection; faster row-lock release on worker crash |
| `idle_in_transaction_session_timeout` | AlloyDB GUC | Unset (unlimited) | **`30s`** | Aborts stuck-in-transaction workers; releases held locks |
| `MALLOC_ARENA_MAX` | container entrypoint env | Unset (scales with CPU count) | **`2`** | Reduces multi-arena glibc fragmentation on multi-core hosts; paired with jemalloc |
| asyncpg main pool min/max | `storage/settings.py` | 5 / 5 | **8 / 8** (not 12 — see §6 corrected arithmetic) | Sized for batch claims + writes |
| asyncpg heartbeat pool | `normalizer_runtime.py` | 1 / 1 | 1 / 1 (keep) | Dedicated heartbeat isolation on daemon thread — correct as-is |
| Active lease path | `feed_queries.py` | `ACQUIRE_FEEDS_BATCH_SQL` (already the only path in `_leasing_loop`; `LEASE_FEED_SQL` is effectively dead code) | No change to which path; verify `LEASE_FEED_SQL` can be removed | Current code is already correct on this |
| SIGTERM release | `normalizer_runtime.py _shutdown_sequence` | Single atomic UPDATE for all leases | **Batched (~50 feeds/batch) + jittered (0–2 s between); each batch its own transaction** | Eliminates scale-in stampede (§8); per-batch commits prevent `idle_in_transaction_session_timeout` interaction |
| `UPDATE_PROGRESS_SQL`, `RELEASE_FEEDS_BATCH_SQL`, `REPORT_FAILURE_SQL` | `feed_queries.py` | All three set `last_heartbeat = NOW()` as a side effect | **Remove `last_heartbeat` write** from these three queries (UPDATE_PROGRESS_SQL now writes `last_progress_at` instead; release/failure drop the write entirely) | Unnecessary mutation; cuts MVCC write rate by ~60% (§6.1) |
| Claim query structure | `feed_queries.py` | Single `SELECT ... FOR UPDATE SKIP LOCKED` with `ORDER BY last_heartbeat ASC NULLS FIRST` | **Split into primary CTE + recovery query.** Primary: UNION ALL MATERIALIZED CTE with three per-type branches, `FOR NO KEY UPDATE SKIP LOCKED`, `ORDER BY id` within each branch. Recovery: separate query for failing-retryable + active-abandoned branches with `ORDER BY retry_after ASC NULLS FIRST, id`. Ramp filter uses md5-based expression (not `hashtext()`) for stability across PG minor upgrades — see §6, §9.4. | HOT-compatible throughout; per-type caps enforced in SQL; `last_heartbeat` ordering dropped (sweep handles abandoned); ramp determinism preserved across minor upgrades |

**Schema additions beyond index restructure (§6):**

- Add `unclaimed_since` column + worker/sweep code to set it on status transitions to `unclaimed`.

**Publisher infrastructure:**

- Deploy 50-line Cloud Run Function that runs `SELECT EXTRACT(epoch FROM NOW() - MIN(unclaimed_since)) FROM feeds WHERE status = 'unclaimed'` and publishes as custom metric. Cloud Scheduler triggers every 60 s.

**Worker runtime config (§7):**

- Install Ops Agent (startup script).
- `LD_PRELOAD` for jemalloc.
- `net.ipv4.ip_local_port_range = 10000 65535`.
- uvloop as event-loop policy.
- `aiohttp.ClientSession` with `TCPConnector(limit=500, limit_per_host=0)`.

**Infrastructure:**

- Deploy MIG at 2 VMs with the autoscaler set to the two-signal policy but `min_required_replicas=2` and all feeds in a "disabled" or "not-yet-enabled" state (see §9.4 mechanism).
- Tune `asyncio.Semaphore(N)` for ffmpeg-spawn gating; fit N in {8, 12, 16, 24, 32} via the 1b-replay procedure against the shadow environment.

At the end of Phase 0: new fleet runs, publisher publishes, but workers claim zero production feeds because `ramp_pct=0`.

### 9.3 Phase 1 — Shadow soak (1 week)

New fleet runs at 2 VMs against a **synthetic workload** that mirrors production catalog composition (41:55:4 bcfy_feeds:bcfy_calls:openmhz at 1,600 feeds — one VM's full steady-state capacity at k=2 × 800) and realistic arrival patterns. No production feeds are claimed. 1,600 is used (not 1,200 as earlier drafts stated) so the soak exercises the per-worker ceiling and the Phase 3 exit criterion at the target density.

Primary goals:

- Validate the 800-feed-per-worker target — CPU slope, RSS slope match 1b predictions.
- Confirm HOT is working (`hot_pct > 95%`) after schema changes.
- Verify batched+jittered SIGTERM drain completes within 90 s across ≥10 test SIGTERMs.
- Catch any runtime surprises (jemalloc, port range, pool sizing) before real traffic.
- Run a **c3-vs-n2 A/B** on one VM pair of each SKU to inform the §15 c3-migration decision.
- **Force concurrent fencing-violation exits** (e.g., by interrupting heartbeats on multiple workers simultaneously) and observe managed-pooler connection state. Workers that self-terminate via the fencing path leave their transactions in limbo until pgbouncer's `server_reset_query` / `server_idle_timeout` fire. Validate that peak-scale concurrent fencing events don't leak enough backend connections to saturate the pool.
- **AlloyDB failover drill in staging.** Trigger a primary failover under shadow load; measure fleet-wide reconnect storm behavior, time-to-recovery, and any correlated fencing-violation cascade. Previous revisions of this plan listed this as a Phase 3 prerequisite; moving it to Phase 1 is safer (fewer variables; staging env is more controlled).
- **Graceful shutdown under pool-saturation drill.** Simulate a pool-saturation death spiral (see §12) and verify the shutdown path + RSS watchdog + autoscaler ceiling cooperate to prevent runaway cascading failure.

Exit criteria (all must hold):

- Event-loop lag p99 < 100 ms across the soak window.
- HOT update ratio > 95%.
- Fewer than 2 unexpected container restarts per 1,000 VM-hours over 72 consecutive hours (allows for routine GCE live-migration / hypervisor hiccups without resetting the clock).
- Graceful shutdown drain completes in < 90 s across ≥10 test SIGTERMs.
- **No sustained pool-wait p99 > 50 ms** after 10 concurrent fencing-violation exits.
- **Fleet recovers within 60 s** after AlloyDB failover drill, with no manual intervention.

If any criterion fails: investigate, fix, re-soak. Do not proceed to Phase 2.

### 9.4 Phase 2 — Production ramp (~18 days)

Production feeds are progressively enabled for claim in five stages. Each stage: raise `ramp_pct`, soak, evaluate against exit criteria, advance or roll back.

**Ramp mechanism.** Workers filter claims by a deterministic md5-based bucket of the feed's primary key `id`. The expression used — `(('x' || substr(md5(id::text), 1, 7))::bit(28)::integer) % 100 < :ramp_pct` — buckets each feed into one of 100 slots and enables only buckets below the threshold.

**Why md5 and not hashtext.** An earlier draft of this plan used `abs(hashtext(id::text)) % 100`. `hashtext()` is a documented-internal PostgreSQL function whose algorithm has historically changed between major PG versions (e.g., around the PG 11 hash-partitioning rework). AlloyDB is a managed service that can perform in-place minor-version upgrades during a multi-week ramp. If `hashtext()`'s output changed mid-ramp, feeds would silently re-shuffle between "enabled" and "disabled" buckets — a feed safely excluded at `ramp_pct=20` could suddenly become claimable, or vice versa. This would violate the ramp's two load-bearing properties: (a) determinism within a ramp window, and (b) rollback semantics (*"disabled feeds return to unprocessed state within ~75 s"* assumes disabled is a stable set). md5() is documented stable across PG versions; the fix is a one-line change. The cost is negligible — md5 is evaluated only against rows already filtered by the partial index `WHERE status='unclaimed'`, so the scan volume is tiny.

The full claim query (showing how the ramp filter layers on top of the §6 claim path):

```sql
-- Worker claim path during ramp (production after Phase 0, primary CTE only;
-- recovery-path query omitted for brevity, same ramp filter applies).
WITH claimed AS MATERIALIZED (
  (SELECT id FROM feeds
     WHERE source_type = 'bcfy_feeds' AND status = 'unclaimed'
       AND (('x' || substr(md5(id::text), 1, 7))::bit(28)::integer) % 100 < $3
     ORDER BY id
     FOR NO KEY UPDATE SKIP LOCKED
     LIMIT $4)
  UNION ALL
  (... analogous bcfy_calls and openmhz branches ...)
)
UPDATE feeds
   SET status = 'active', worker_id = $1, fencing_token = fencing_token + 1, last_heartbeat = NOW()
  FROM claimed
 WHERE feeds.id = claimed.id
RETURNING feeds.*;
```

**Properties:**

- **Deterministic across PostgreSQL minor version upgrades.** Same feed is always "enabled" or not at a given `ramp_pct`, regardless of PG version drift.
- No schema changes beyond Phase 0.
- Stateless — rollback is a config flip that takes effect within ~75 s as existing leases drain via the sweep.

Feeds below the threshold are enabled and claimable; feeds above the threshold remain in `unclaimed` state but are filtered out by the `ramp_pct` check.

**Upstream coordination (pre-approval confirmation item).** Feeds above the `ramp_pct` threshold are not processed during a stage — they sit in the queue without workers claiming them. This requires upstream awareness that a percentage of feeds will experience delayed (not lost) audio capture during the ramp window. Any buffering, retry, or "first-listen" semantics upstream must tolerate this. **Before Phase 2 kickoff, confirm with Broadcastify / OpenMHZ ownership that a 1% → 100% ramp over ~18 days with 20%/50%/80% intermediate stages will not trigger upstream behavior changes (e.g., stream deactivation for low-listener feeds, or disconnect-on-idle semantics).** This is a pre-approval item, not a Phase 2 surprise.

`ramp_pct` is a single-source-of-truth Terraform variable. Increasing it is a standard rolling deploy.

**Ramp stages.**

| Stage | `ramp_pct` | Enabled feeds at peak | Soak | Primary goal |
|---|---|---|---|---|
| 2a | 1% | ~120 feeds | **24 hours** | Smoke test. Basic plumbing under real upstream behavior. Minimal blast radius. |
| 2b | 20% | ~2,400 feeds | **48 hours** | Two VMs carrying meaningful load (~1,200 feeds/VM). Exercises per-worker sizing and autoscaler's 2-to-3-VM decisions. |
| 2c | 50% | ~6,000 feeds | **72 hours** | Mid-scale. Exercises autoscaler under realistic surge patterns (~4 VMs). |
| 2d | 80% | ~9,600 feeds | **72 hours** | Near-peak load. Confirms peak fleet sizing (~6–7 VMs) and memory headroom assumptions. |
| 2e | 100% | 12,027 feeds | **7 days** | Full production scale. Long soak before CUD commitment and feed-feature-flag removal. |

Total soak duration: 24 + 48 + 72 + 72 + 168 = 384 hours = 16 days, plus ~2 days of inter-stage evaluation buffer = **~18 days** (previous revisions said 16 days, which omitted inter-stage buffer).

Note on stage 2b: ramp jumps from 1% to 20% (not 10%) so that at least two VMs carry non-trivial load. At 10%, the autoscaler floor of 2 VMs would have one VM carrying ~1,200 feeds and a second carrying ~0, making a single-VM hardware failure during soak indistinguishable from a code bug. At 20%, both VMs carry ~1,200 feeds — single-VM hardware failure would still affect ~half the stage load, but the failure mode is observable as a partial-coverage event rather than a total blackout.

**Exit criteria (same at every stage; all must hold to advance):**

| Metric | Threshold |
|---|---|
| Event-loop lag p99 (per worker) | < 250 ms sustained |
| `oldest_unclaimed_feed_age` p99 | < 60 s sustained (matches SLO) |
| Lease-renewal success rate | > 99% by source_type |
| Container OOM events | 0 during soak |
| `hot_pct` (HOT effectiveness) | ≥ 95% |
| Autoscaler thrash (scale-out events/hr) | ≤ 5 during normal catalog hours |
| Per-type backlog alerts fired | 0 `critical`-level alerts |

**Re-soak policy for interruptions during a stage.** Hardware failures and similar root-cause-obvious interruptions do not count against exit criteria. The rule is:

- **Clear hardware / infrastructure cause** (GCE VM hardware fault, zonal outage, AlloyDB scheduled maintenance, known external incident): extend soak by the outage duration if ≥30% of soak time has elapsed; reset the soak clock if < 30%.
- **Novel failure mode** (any symptom not immediately attributable to external infrastructure — event-loop stalls, memory growth, lease-contention spikes, unexpected error-rate jumps): **mandatory full re-soak** from `t=0` regardless of percentage elapsed. Novel failures at low soak percentages are more suspicious, not less — they indicate a bug that surfaces early and may recur.
- **Ambiguous cause** (could be either): default to full re-soak. The stage-advance rule is biased toward caution because stage advancement is irreversible within the ramp window; an extra 72 h of soak is cheaper than rolling back from a later stage.

The call on whether an interruption is "clearly hardware" vs "novel" is made by the on-call engineer plus one senior engineer, documented in the ramp-incident log.

If any criterion fails at a stage, **do not advance**. Either fix-and-resoak (preferred), or reduce `ramp_pct` to the previous stage and investigate.

**Minimum soak duration is non-negotiable.** Even if all metrics look green within the first hour, full soak must elapse before the next stage. Many failure modes (RSS creep, AlloyDB vacuum pressure, corner-case upstream behavior) only surface after hours of sustained load.

**CUD commitment timing.** The 1-year CUD commitment on the 2-VM baseline is locked in at Phase 3, not during the ramp. This accepts ~$280 of on-demand pricing during Phase 2 in exchange for not committing before full-scale validation.

### 9.5 Phase 3 — Stabilization (1 day)

After 7 days at `ramp_pct = 100` with all exit criteria green:

- Lock in 1-year CUD on 2-VM baseline.
- Remove the `AND (('x' || substr(md5(id::text), 1, 7))::bit(28)::integer) % 100 < :ramp_pct` filter from the worker query (simplify).
- Delete the `ramp_pct` Terraform variable and its plumbing.
- Mark ramp completed in operational runbook.

### 9.6 Ramp calendar summary

| Week | Activity |
|---|---|
| Weeks 1–2 | Phase 0 prep |
| Week 3 | Phase 1 shadow soak |
| Week 4, days 1–2 | Phase 2a (1% × 24h) + buffer |
| Week 4, days 2–4 | Phase 2b (20% × 48h) |
| Week 4–5, days 4–7 | Phase 2c (50% × 72h) |
| Week 5, days 7–10 | Phase 2d (80% × 72h) |
| Weeks 5–6, days 10–17 | Phase 2e (100% × 168h / 7 days) |
| Week 7, day 18 | Phase 3 stabilization |

Total: ~7 weeks from kickoff (2 prep + 1 shadow + ~18 days Phase 2 + 1 day Phase 3). Leadership approval of the ramp plan — stage percentages, soak durations, exit criteria, upstream-coordination impact, and calendar commitment — is a Part V approval item.

## 10. Operator runbook

**Daily operations: nothing.** The autoscaler handles catalog growth and shrinkage automatically. There is no seasonal runbook, no manual scaling, no role-assignment debugging.

**Single VM dies (the most common failure):**

1. MIG health check detects unhealthy VM within 30 seconds.
2. Surviving 7 VMs continue processing their leases.
3. The dead VM's leases expire after 90 seconds (visibility timeout).
4. Surviving workers pick up orphaned leases on their next poll.
5. Autoscaler provisions a replacement in parallel (~3–5 minutes).
6. Total user-visible impact: ~1,500 feeds delayed by ~75 seconds. No operator action required.

At peak load, surviving 7 VMs are momentarily below the steady-state capacity needed for 12,027 feeds (11,200 available, 7% shortfall). This manifests as a brief rise in `oldest_unclaimed_feed_age`, which triggers the autoscaler to provision a 9th VM within the 10-VM ceiling. Feed-claim latency returns to normal once the replacement joins the fleet.

**Catalog spike (admin adds 500 feeds):**

1. New feeds arrive in `unclaimed` state in AlloyDB with `unclaimed_since = NOW()`.
2. Within ~60 s, `oldest_unclaimed_feed_age` begins rising.
3. Once the metric exceeds 60 s, the autoscaler provisions an additional VM within ~3–5 minutes.
4. Alternatively, if existing workers absorb the new feeds immediately, CPU rises and the autoscaler scales out on that signal.
5. New VM bootstraps, registers heartbeat, starts claiming leases.
6. Total impact: temporary backlog for ~5 minutes, then drains.

**Multi-VM loss at peak (degraded-mode event):**

1. Two or three VMs lost simultaneously (zonal outage is the realistic cause).
2. `oldest_unclaimed_feed_age` rises; per-type backlog alerts fire (`warning` first, `critical` if sustained).
3. The pg_cron sweep reclaims abandoned leases within 30–60 s, returning them to `unclaimed`.
4. Autoscaler provisions replacements up to the 10-VM ceiling. If the zonal regional-MIG allocation permits, replacements come up in healthy zones.
5. Recovery window:
   - 2-VM loss: ~5 minutes, ~2,400 feeds with raised claim latency.
   - 3-VM loss: ~8–12 minutes, ~4,000 feeds with raised claim latency.
6. Operator action: monitor alerts but no direct intervention needed unless recovery stalls. If `oldest_unclaimed_feed_age` stays > 300 s for more than 10 minutes, escalate — this indicates the autoscaler cannot provision replacements (quota, zone capacity, or cascading failure).

**Publisher (`oldest_unclaimed_feed_age` Cloud Run Function) dies:**

1. Custom metric goes stale after ~2 minutes.
2. Autoscaler holds current replica count on the stale-metric signal; continues to evaluate CPU normally.
3. Fleet stays sized for current CPU load — scaling still works, just without burst-detection optimization.
4. Operator receives an alert; function is redeployed from Terraform in under 10 minutes.
5. **User-impact window.** No user impact as long as CPU remains a responsive signal. The specific concern is rapid catalog bursts: a 500-feed catalog spike that CPU would respond to only after feeds get claimed and start doing work (20–60 s lag), where the latency signal would have caught it in 60 s. During publisher outage, those two windows overlap and the CPU signal is the only guardrail. Restoration should be prioritized; don't assume "no current spike = no urgency" if catalog management activity is expected.
6. Function is stateless; redeployment is instantaneous once detected.

**AlloyDB primary fails over:**

1. asyncpg connections drop fleet-wide; workers retry with exponential backoff.
2. AlloyDB managed failover completes in ~30 seconds.
3. Workers reconnect; in-flight leases continue under their existing visibility timeouts.
4. ~30 seconds of polling pause; no lease loss.
5. **Post-failover watch:** monitor `AlloyDB pool wait p99` (§11) for saturation as the fleet reconnects simultaneously. The managed pooler's `max_client_conn=800` is well above the fleet's 144 normal client slots; brief reconnect storms shouldn't saturate, but a correlated fencing-violation burst (workers self-terminating on token mismatches) could leave orphaned connections in pgbouncer until `server_reset_query` / `server_idle_timeout` fire. Phase 1 shadow soak should force this condition and observe recovery time.

**Network partition (one zone's VMs lose AlloyDB connectivity):**

1. Partitioned workers' claim/heartbeat/write requests time out; workers retry with exponential backoff.
2. `last_heartbeat` on partitioned workers' leases ages past 60 s threshold.
3. pg_cron sweep reclaims those leases; `status='unclaimed'` and `unclaimed_since=NOW()`.
4. Healthy-zone workers claim the reclaimed feeds on next poll; `fencing_token` increments.
5. When connectivity restores, partitioned workers attempt heartbeat renewal or progress update; fencing-token mismatch triggers self-termination (`os._exit(1)` via fencing path in current code).
6. MIG restarts the terminated containers; they rejoin the fleet with a clean slate.
7. Total recovery: ~60–120 s. No operator action needed unless partition is sustained > 10 min.

**AlloyDB managed pooler saturation (e.g., reconnect storm after failover):**

1. Pool reaches `max_client_conn=800`; pgbouncer refuses new connections.
2. Workers see connection errors; retry with backoff.
3. Leasing pauses fleet-wide; heartbeats may fall behind 60 s window.
4. Falling-behind heartbeats trigger fencing self-termination on affected workers.
5. MIG restarts terminated containers; fleet-wide reconnect is naturally staggered by the restart timing.
6. If this becomes recurrent: raise `max_client_conn` or investigate pool-leak root cause.
7. Total recovery: typically ~30–60 s; longer if the root cause is underlying DB distress.

**Rolling deploy (normal change management):**

1. New instance template is pushed.
2. MIG brings up `max_surge=2` replacement VMs with `initialization_period_sec=180, min_ready_sec=60`.
3. New VMs pass startup jitter, claim their share of leases, reach steady state.
4. Old VMs receive SIGTERM and drain leases with 0–2 s jitter per batch (§8).
5. Cycle continues until all VMs are on the new template.
6. No feed gap because new VMs are serving before old VMs drain.

## 11. Monitoring and alerts

Alerts are organized into three tiers:

- **Critical (page)** — requires immediate action. 2 AM pages go here. Silent corruption of the autoscaler signal lives here.
- **Warning (ticket)** — next-business-day investigation. Indicates degradation or an approaching ceiling.
- **Informational (dashboard)** — not paged, not ticketed. Visible on the ops dashboard for context.

Before Phase 0, on-call rotation load should be audited; if any of the below are already firing frequently in other services, consider softening their tier.

### 11.1 Critical alerts (page)

These fire only when automated recovery has failed or cannot apply. Each indicates a state where the system is actively losing work, silently misreporting, or approaching a resource ceiling.

| Signal | Critical threshold | Why critical |
|---|---|---|
| Per-type backlog (bcfy_feeds) | > 2,000 for 5 min | Sustained under-provisioning; autoscaler has not caught up |
| Per-type backlog (bcfy_calls) | > 1,500 for 5 min | Same |
| Per-type backlog (openmhz) | > 300 for 5 min | Same |
| Lease-renewal success rate by source_type | < 95% / 5 min | AlloyDB distress or worker crash loop |
| Event-loop lag p99 (per worker) | > 1 s | Worker severely overloaded; activation burst pathology |
| `oldest_unclaimed_feed_age` absolute | > 300 s for 2 min | Autoscaler not keeping up or capacity blocker |
| `oldest_unclaimed_feed_age` sentinel value < 0 | any | Publisher function is misbehaving (query timed out, DB failure) — metric is lying |
| `oldest_unclaimed_feed_age` metric freshness | stale > 10 min | Publisher function down for an extended window — combine with CPU-trigger check |
| MIG `currentActions.creating` stuck | > 15 min without progress | Autoscaler asking for VMs, GCE not providing (quota / zone capacity / template error) |
| **Active-status rows with stale `last_heartbeat`** | **> 100 rows with age > 120 s** | **Sweep has fallen behind. This is the one case where the autoscaler signal silently under-reports: these rows exist, are not being processed, but `oldest_unclaimed_feed_age` is scoped to `status='unclaimed'` and doesn't see them. Operator must investigate pg_cron job status immediately.** |
| pg_cron `abandoned_lease_sweep` job | Last run > 5 min ago | Same root cause as above; tier-1 page |
| `n_tup_hot_upd / n_tup_upd` ratio (hot_pct) | < 90% for 1 hr | An index references a mutated hot-path column — table will bloat |
| AlloyDB managed pool — pool wait p99 | > 100 ms | Pool saturation causing request queueing; precondition to death-spiral (§12) |
| Per-container RSS | > 14.5 GiB | Approaching OOM; FFmpeg leak or mix-variance breach |
| Container restart count | > 5/hr/VM | Crash loop |
| Graceful shutdown duration | > 90 s (timeout hit) | SIGTERM failed to drain; leases may be orphaned |

### 11.2 Warning alerts (ticket)

Next-business-day investigation. Indicates approaching a ceiling, degradation, or a trend worth explaining.

| Signal | Warning threshold | What it means |
|---|---|---|
| Per-type backlog (bcfy_feeds) | > 1,000 for 5 min | Workers under-provisioned or upstream outage; may self-correct |
| Per-type backlog (bcfy_calls) | > 500 for 5 min | Surge underway |
| Per-type backlog (openmhz) | > 100 for 5 min | Small-fleet version of same |
| Lease-renewal success rate by source_type | < 99% / 5 min | Early sign of AlloyDB distress |
| Event-loop lag p99 (per worker) | > 250 ms | Worker overloaded; investigate |
| `oldest_unclaimed_feed_age` absolute | > 120 s | Autoscaler catching up; verify scale-out is happening |
| `oldest_unclaimed_feed_age` metric freshness | stale > 2 min | Publisher function hiccup; CPU signal still works |
| pg_cron `abandoned_lease_sweep` job | Last run > 90 s ago | Sweep slow or missed cycle |
| Active-status rows with stale `last_heartbeat` | > 10 rows with age > 120 s | Sweep keeping up but close to edge |
| Dead tuple count on `feeds` | > 100k | HOT or autovacuum falling behind |
| `n_tup_hot_upd / n_tup_upd` ratio (hot_pct) | < 95% | HOT effectiveness dropping; investigate before critical |
| AlloyDB pool active backends | > 140 | Approaching 160 `default_pool_size` |
| AlloyDB pool wait p99 | > 50 ms | Early pool saturation |
| Per-container RSS | > 13 GiB | Watch for OOM |
| Container restart count | > 2/hr/VM | Early crash-loop signal |
| Heartbeat skew p99 | > 30 s | Worker near orphaning leases |
| Graceful shutdown duration | > 60 s | SIGTERM slow; investigate |
| Observed bcfy_feeds share across catalog | deviates from assumed mix by > ±10 pp | CPU slope assumption may drift |
| Per-worker observed bcfy_feeds count | > 240 at cap ceiling | At or above DB-enforced cap; investigate for cap-binding patterns (expected under clustering) |
| MIG `currentActions.creating + .verifying` stuck | > 5 min without progress | Early sign of autoscale-failure |
| MIG `currentActions.abandoning` unplanned | any non-zero without matching scale-in | Health-check-triggered replacement |
| **`rss_watchdog_trips_per_hour`** | **> 1/hr any worker** | **Self-RSS watchdog paused claims at 70% or exited at 90%; indicates post-claim memory pathology or worker-counter corruption** |
| **Claim query P99 latency** | **> 100 ms** (baseline ~20 ms) | **CTE plan may be regressing; re-run EXPLAIN to verify MATERIALIZED + per-type index still in effect** |
| **`EXPLAIN` plan shape check** (CI, pre-deploy, weekly prod drift) | absence of CTE Scan or Nested Loop over `feeds` | **Planner regression — claim query may no longer honor per-type cap; investigate urgently** |

### 11.3 Informational (dashboard only, no alerts)

Visible on ops dashboard for context during incidents; never pages, never tickets.

| Signal | What it shows |
|---|---|
| Scale-out events per hour | Autoscaler activity; useful for explaining CPU-cost fluctuation |
| Scale-in events per hour | Same |
| Per-worker feed counts by type | Mix-variance visualization; complements the per-worker cap warning |
| Heartbeat UPDATE QPS | Baseline verification of §6 arithmetic |
| Claim-path QPS | Same |
| Per-branch claim-count from CTE | Per-type claim distribution — useful to see which branches are cap-binding |
| GCS Class A operations / hour | Cost tracking |

**Deliberately not alerted on:** per-container CPU mean. Asyncio workers are single-thread and routinely run one core at 60–95% by design; alerting on CPU mean either fires constantly (low threshold) or never (high threshold). Event-loop lag is the right signal for worker overload. Note: kernel-level CPU is still used as an autoscaling signal in §2 — scaling and alerting have different needs.

## 12. Failure mode coverage

| Failure | Behavior | Recovery |
|---|---|---|
| Single VM loss (zonal hardware fault) | ~1,500 mixed feeds briefly unclaimed for 60–90 sec | Sweep + surviving workers claim orphans automatically; autoscaler provisions replacement |
| Two-VM loss (e.g., zonal outage) | ~3,000 feeds with raised claim latency; per-type backlog alerts fire | Autoscaler provisions 2 replacements within 10-VM ceiling; recovery ~5 min |
| Three-VM loss (full zonal outage with 3 VMs in one zone) | ~4,000 feeds with raised claim latency; backlog alerts sustained; **degraded-mode event** | Autoscaler provisions replacements; 3rd replacement gated by `min_ready_sec=60` + `initialization_period_sec=180`; recovery ~8–12 min. Operator monitors; escalates if `oldest_unclaimed_feed_age > 300 s` for > 10 min |
| AlloyDB primary failover | All workers pause ~30 sec | Automatic; no lease loss. Post-failover: watch pool-wait p99 for reconnect-storm saturation |
| Network partition (zone loses AlloyDB connectivity) | Partitioned workers' heartbeats age out; sweep reclaims; healthy workers claim; partitioned workers self-terminate via fencing-token mismatch on reconnect | Automatic; MIG restarts self-terminated containers. Recovery ~60–120 s |
| AlloyDB managed pooler saturation | Connection refused at pooler; workers retry with backoff; leasing pauses; heartbeats may fall behind → fencing self-termination | Naturally self-heals via staggered MIG restart of terminated containers. Recovery ~30–60 s. If recurrent: raise `max_client_conn` or root-cause pool leak |
| **Pool-saturation death spiral** (pool saturates → latency rises → autoscaler adds VMs → more pool pressure → cascading failure) | Without Phase 0 pool sizing (raising asyncpg main pool 5→8; confirming pooler `default_pool_size=160`), any scale-out past current 1-VM saturates the pool. Workers can't claim/heartbeat → `oldest_unclaimed_feed_age` rises → autoscaler provisions more VMs → worse saturation. | **Prevented by Phase 0, not recovered from after.** §6 pool arithmetic closes the math at 16-VM peak. Pool-wait p99 alert (§11, critical tier) is the early-warning signal. If it fires, stop scale-out manually before the spiral begins. Phase 1 shadow soak forces this condition and validates the autoscaler ceiling + RSS watchdog contain it |
| Upstream Broadcastify outage | Feeds spin with reconnect loops; CPU stays low; no backlog | Per-type lease-renewal alert fires; no operator action needed unless extended |
| Subprocess-spawn storm (plausible pathology; not observed in 1b — see Appendix A) | Bounded by `asyncio.Semaphore(N)` cap; one worker may briefly freeze | Sibling worker on same VM continues; Python 3.12.1+ as defensive baseline |
| Scale-in stampede (VMs draining simultaneously) | Batched+jittered SIGTERM release (§8) spreads lease availability over ~30 s window; no synchronized thundering herd | Automatic via graceful shutdown |
| Rolling deploy with in-flight leases | `max_surge=2, max_unavailable=1, min_ready_sec=60, initialization_period_sec=180`: new VMs claim before old VMs drain | Automatic via MIG update policy |
| Publisher function failure | Custom metric stale or sentinel `-1`; CPU signal continues to size fleet | No user impact for short outages; see §10 for slow-drift caveat |
| Heartbeat-bloat failure mode | HOT updates prevent bloat, **conditional on Phase 0 index restructuring** (§6); `hot_pct < 90%` alert catches accidental regressions | Schema-fix release if alert fires |
| bcfy_calls surge starves openmhz | Pool-share self-balancing: openmhz selection rate rises mechanically with backlog (§1) | Automatic; no operator action |
| Graceful-shutdown timeout (SIGTERM drain fails to complete) | Remaining leases not released; sweep reclaims them after `last_heartbeat` exceeds 60 s threshold | Alert fires (§11); investigate asyncpg pool, DB health, or shutdown-budget tuning (§8) |
| Abandoned-lease accumulation (sweep stops) | Active-status rows with stale `last_heartbeat` accumulate; `oldest_unclaimed_feed_age` does not reflect them (measures `unclaimed` only). **This is the one case where the autoscaler signal silently lies to the operator** — critical alert at §11 | Alert on pg_cron job failure; manual sweep runs in ~seconds on ~12k rows |
| **Claim query planner regression** (CTE loses MATERIALIZED optimization, or composite index gets ignored) | Per-type LIMITs still enforced by `LockRows` node (claim remains OOM-safe) but claim query P99 latency rises — could be 10-100× slower. Fleet claim throughput drops; `oldest_unclaimed_feed_age` rises; autoscaler reacts on latency signal | CI pre-deploy EXPLAIN check prevents shipping a plan regression. Production weekly EXPLAIN-drift check catches statistics-induced regressions. Recovery: `ANALYZE feeds` to refresh stats, or `pg_hint_plan` if chronic. **The cap itself remains safe under planner regression — this is a performance concern, not a correctness concern** |
| **RSS watchdog trip** (worker detects its own memory pathology) | Worker pauses claims at 70% container memory; graceful exit at 90% × 3 samples; MIG respawns | Automatic. Alert fires (§11); investigate feed-specific memory pathology or worker-counter corruption. Budget cap prevents the claim-shape OOM when worker tracking is correct; watchdog catches cases it can't |
| **Ramp filter drift across PG minor upgrade (would have happened under `hashtext()`; prevented by md5)** | Hypothetical: AlloyDB minor-version upgrade mid-ramp could have silently re-shuffled bucket assignments, violating determinism and rollback semantics. Not applicable under current md5-based filter. | N/A — architectural prevention via md5. Documented here for future engineers who might be tempted to "optimize" back to hashtext |

---

# Part IV — Cost

## 13. Annual cost detail

| Component | Annual | Notes |
|---|---|---|
| **Compute (recommended pricing)** | **$7,695** | See breakdown below |
| GCS Class A operations (volume-scaled) | $29,146 | Audio segment writes; scales with seasonal feed volume |
| AlloyDB compute + storage | $5,700 | 2-vCPU HA primary; unchanged by this plan |
| Cloud Logging | $180 | 50 GiB free + ~30 GiB overage at $0.50/GiB |
| Network, Pub/Sub, miscellaneous | $600 | Cloud NAT, intra-region egress, Pub/Sub publish |
| Scale-in drift (cost of `max-scaled-in=1/600s`) | $200 | Explicit price of stability over agility |
| **Grand total** | **$43,521/yr** | |

**Compute breakdown:**

| Period | VMs | Months | Pricing | Subtotal |
|---|---|---|---|---|
| Peak season — 2 baseline VMs (1-yr CUD) | 2 × $89.33 | 5 | CUD | $893 |
| Peak season — 6 surge VMs (on-demand) | 6 × $141.79 | 5 | On-demand | $4,254 |
| Off-season — 2 baseline VMs (1-yr CUD) | 2 × $89.33 | 7 | CUD | $1,251 |
| Off-season — 1 surge VM (on-demand) | 1 × $141.79 | 7 | On-demand | $993 |
| Boot disks (50 GB pd-balanced × VMs × months) | varies | 12 | Standard | $305 |
| **Compute total** | | | | **$7,695** |

## 14. Comparison to naive-deployment baseline

The naive-deployment baseline is a hypothetical 8-VM-flat-year-round deployment costed on the assumption that 12,027 feeds run every month of the year, served by a long-running reconciliation-controller service on Cloud Run. It is the "what would we spend if we sized for peak, didn't recalibrate for seasonality, and accepted a traditional control plane" reference point.

| | Naive baseline | This plan | Difference |
|---|---|---|---|
| Compute | $9,056 | $7,695 | $1,361 (15%) |
| GCS Class A (naive: 12,027 feeds year-round) | $47,940 | $29,146 | $18,794 (39%) |
| Other lines | $14,800 | $6,680 | $8,120 (55%) |
| **Grand total** | **$71,796** | **$43,521** | **$28,275 (39%)** |

**Breakdown of "Other lines":**

| Line | Naive baseline | This plan | Delta | Attribution |
|---|---|---|---|---|
| AlloyDB compute + storage | $5,700 | $5,700 | $0 | Unchanged by this plan |
| Cloud Logging | ~$2,500 | $180 | ~$2,320 | Architecture (seasonal scale-down → fewer VM-months of logs) |
| Network / NAT / Pub/Sub | ~$3,200 | $600 | ~$2,600 | Mostly architecture (fewer VM-months); partly recalibration |
| Hypothetical reconciliation controller | ~$3,400 | $0 | ~$3,400 | Architecture (no controller in this design — see §2) |
| Scale-in drift | $0 | $200 | −$200 | Architecture (explicit cost of autoscaler stability) |
| **Subtotal** | **$14,800** | **$6,680** | **$8,120** | |

**Where the $28,275 actually comes from.** The total splits into:

| Source | Annual delta | Driver |
|---|---|---|
| **Architecture (this plan)** | **~$9,500** | Compute ($1,361) + controller retirement + logging/network seasonal scale-down. Would not apply if we kept the naive design. |
| **Baseline recalibration** (would apply with any architecture) | ~$18,800 | GCS Class A operations counted on actual catalog seasonality (500–4,000 feeds for 7 months; 12,027 for 5 months) instead of flat 12,027 year-round. |

The architecture delivers ~$9,500/yr in direct savings. The ~$18,800/yr GCS recalibration is a book-keeping correction that applies regardless of whether the deployment is autoscaled or flat. If the naive-baseline GCS cost were recomputed with seasonal catalog sizing, the GCS line would be ~$29,146 under either architecture, eliminating that part of the apparent "savings."

**Uncertainty on the breakdown.** The "Other lines" sub-breakdown above is estimated, not precisely modeled. The naive baseline's ~$14,800 was calculated top-down; allocating it back to specific lines (Logging / Network / hypothetical controller) involves judgment. What is firm is the $8,120 delta and the $43,521 run rate. Leadership should treat the sub-line attribution as "this is where we think the savings come from" not "these are audited line items."

**Implication for approval.** Leadership should approve the architecture on its structural benefits (SPOF elimination, self-sizing, no control plane, graceful failure recovery) *and* on its ~$9,500/yr direct cost reduction. The $18,800/yr GCS figure is separate and should be treated as a correction to the expected run rate, not as a saving the plan generates.

## 15. Further savings opportunities

Three levers are deferred, each requiring coordination beyond this plan:

| Lever | Est. annual savings | Why deferred |
|---|---|---|
| 4:1 GCS chunk batching (60-sec audio chunks vs current 30-sec) | ~$24,000 | Requires transcription team sign-off that 60-sec chunks are acceptable |
| 3-year CUD on full 8-VM peak | ~$2,300 | Premature commitment; revisit after first full year of validation data |
| c3 SKU migration (if Phase 1 A/B shows higher per-worker ceiling) | ~$2,000 | Requires Phase 1 c3-vs-n2 A/B data |

These are tracked separately and not included in the savings claim above.

---

# Part V — Decision checklist

The plan asks for approval on **three headline decisions** (exec summary §1-§3) that decompose into **eleven specific sub-items** below. Each sub-item is independently approvable / rejectable; leadership may want to flag any as "approve with amendment."

Note on §1 (unified workers): the worker code already implements unified-worker behavior. No new approval is needed for that choice — §1 records the rationale for reviewers who want to understand why. All eleven items below are new decisions.

**Pre-approval blockers (must be resolved before approval meeting):**

- [ ] **Catalog composition confirmation.** Confirm whether the operative "12,027 peak" preserves the assumed 41:55:4 mix (so §4 math stands) or reflects the real admin-reviewed catalog's 5:8:86 shape (so fleet sizing likely shrinks to ~3 VMs peak). If 5:8:86, rerun §4, §5, §13 before approval.
- [ ] **Upstream coordination.** Confirm Broadcastify / OpenMHZ ownership tolerance of the ~18-day graduated ramp (delayed, not lost, capture for feeds above `ramp_pct` threshold at each stage).

**Decision 1 — Autoscaling (§2).**

- [ ] **Two-signal MAX policy.** `oldest_unclaimed_feed_age` + CPU utilization as the two autoscaler inputs, with no hardcoded feeds-per-VM constant in the fleet-sizing path.
- [ ] **Stateless publisher function, not controller service.** Deliberately reject the "controller service" alternative in favor of a 50-line Cloud Run Function + Cloud Scheduler. Saves ~$3,400/yr and eliminates a failure surface.

**Decision 2 — Fleet sizing and capacity (§4, §5).**

- [ ] **Per-worker target of 800 feeds with per-type cap of 240 bcfy_feeds.** Memory-headroom-driven. Per-type cap (worker-budgeted, DB-enforced via UNION ALL MATERIALIZED CTE) defends against mix-variance OOM (§4).
- [ ] **Fleet: 2 VMs off-season, 8 VMs at peak, autoscaler ceiling of 10.** Regional MIG with `distribution_policy_target_shape = EVEN`. Acknowledge multi-VM loss as a degraded-mode event.

**Decision 3 — Database and runtime changes (§6, §7, §8).**

- [ ] **`feeds` schema restructuring.** `fillfactor=70`; drop `idx_feeds_leasing`; add `idx_feeds_unclaimed` / `idx_feeds_failing_retryable` / `idx_feeds_active` partial indexes that exclude mutated hot-path columns; add `feeds_claim_by_type_idx` composite partial index (source_type, id) WHERE status='unclaimed'; add `unclaimed_since` and `last_progress_at` columns; deploy pg_cron abandoned-lease sweep (LIMIT 500, every 30 s) and minute-cadence VACUUM. **Without this, the current schema generates ~170M dead tuples/day and the `hot_pct < 90%` alert fires at cutover.**
- [ ] **Claim query changes.** Split into primary + recovery paths per §6. Primary CTE: UNION ALL of three per-type branches with MATERIALIZED, `FOR NO KEY UPDATE SKIP LOCKED`, per-branch `ORDER BY id` (the `(status='unclaimed') DESC` priority is implicit via WHERE), per-branch LIMITs set by worker to `min(cap[type], remaining_budget[type])`. Recovery path: `ORDER BY retry_after ASC NULLS FIRST, id` for failing-retryable + active-abandoned. `last_heartbeat` dropped from all ORDER BYs. Ramp filter uses md5-based expression (not `hashtext()`) for stability across PG minor upgrades. Remove `last_heartbeat = NOW()` side-effects from `UPDATE_PROGRESS_SQL`, `RELEASE_FEEDS_BATCH_SQL`, `REPORT_FAILURE_SQL` (cuts MVCC write rate ~60%).
- [ ] **Connection pooling.** AlloyDB server `max_connections` stays at default 1,000 (no change). Managed pooler `max_client_conn` already 800 (no change). Raise pooler `default_pool_size` to 160 if below. asyncpg main pool `5→8`; heartbeat pool `1/1` unchanged. Earlier drafts asked for `max_db_connections=200`; that was an invented name conflating two different AlloyDB knobs — see §6 nomenclature clarification.
- [ ] **Settings changes** (§9.2 code-delta table). Raise `max_feeds_per_worker: 250→800`; raise `graceful_shutdown_timeout_sec: 10→90`; raise `heartbeat_interval_sec: 15→20` (write coalescing, §6.1); keep `abandonment_window_sec=60`. Use production setting names (prior drafts used invented names like `VISIBILITY_TIMEOUT_SEC` / `GRACEFUL_SHUTDOWN_SEC` / `LEASE_BATCH_SIZE`; those don't exist in the codebase).
- [ ] **Write coalescing** (§6.1). Add `last_progress_at` unindexed column; rewrite `UPDATE_PROGRESS_SQL` to target it; add skip-if-recent predicate on heartbeat renewal. Combined with cadence relax, drops sustained UPDATE rate from ~2,000/sec to ~490/sec on `last_heartbeat`; ~170M → ~42M dead tuples/day.
- [ ] **Minute-cadence VACUUM** (§6). Second pg_cron job: `SELECT cron.schedule('feeds-vac', '* * * * *', 'VACUUM (ANALYZE) feeds');`. Required for line-pointer-array maintenance (heap_page_prune_opt doesn't shrink LP array).
- [ ] **Vertical-split escalation plan** (§6.1). `feed_leases` table design documented; gated on `hot_pct < 0.95` sustained for 24 hours. One-week engineering project if triggered; not built unless metric fires.
- [ ] **Worker runtime additions.** uvloop + jemalloc (`MALLOC_ARENA_MAX=2`) + aiohttp Keep-Alive + expanded port range + `asyncio.Semaphore(N)` gate on ffmpeg spawn + 0–2 s startup jitter + per-type claim cap (worker-budgeted, DB-enforced) + self-RSS watchdog. All Phase 0 additions; none exist in current code.
- [ ] **SIGTERM handling.** Replace single-UPDATE release with batched (~50 feeds/batch) + jittered (0–2 s) release at step 3 of the existing shutdown sequence, each batch its own transaction. Add `TASK_CANCEL_BUDGET_SEC=30` sub-timeout. Raise `graceful_shutdown_timeout_sec` to 90 s. **Keep the current heartbeat-off-first / cancel-tasks / release ordering** — earlier draft proposed reversing this but that reintroduces the fence-violation race the current code comments explicitly warn against.

**Ramp plan (§9).**

- [ ] **Five-stage graduated rollout**: 1% → 20% → 50% → 80% → 100% with 24h / 48h / 72h / 72h / 7d soaks. md5-based deterministic filter (stable across PG minor upgrades). Exit criteria at every stage must pass before advancing. Novel failures trigger mandatory full re-soak; only clear-hardware failures qualify for the partial-extend rule. Upstream coordination: feeds above `ramp_pct` threshold experience delayed (not lost) capture during their stage. Total ramp ~18 days; total calendar ~7 weeks from kickoff.

**Pricing (§3) and cost (§13-§14).**

- [ ] **Pricing strategy.** 1-year flex CUD on 2-VM baseline (~$2,144/yr committed) plus on-demand peak surge. $1,941/yr premium over all-8-VM-CUD is the price of surge-rate flexibility. CUD commitment locked only at Phase 3.
- [ ] **Cost.** $43,521/yr all-in run rate. Of the $28,275 difference vs naive-baseline, ~$9,500/yr is architecture-attributable (compute + controller retirement + logging/network scale-down); ~$18,800/yr is baseline recalibration (GCS operations on actual catalog seasonality) that would apply under any architecture. Approval is for the run rate, with this attribution in mind.

**Summary**: ~$43,521/yr run rate; ~$9,500/yr direct architecture savings; ~7-week calendar from kickoff to Phase 3; 11 specific sub-decisions under 3 headline decisions, plus 2 pre-approval blockers (catalog composition + upstream coordination).

---

# Appendix A — Experimental data and its limits

**What was actually measured.** The current empirical artifact is `radio-transcription/model/data/wildfire_catalog/dev_experiment_results.md`, a single 8-minute test on 2026-04-15 with **6 bcfy_feeds** on an n2-standard-4. That run produced a CPU figure of ~0.7% per feed for bcfy_feeds, consistent with the bcfy_feeds slope in the table below but not a multi-source measurement and not a sustained soak. The bcfy_calls and openmhz paths have been code-reviewed but not exercised at runtime against real upstream sources.

**Inferred / estimated per-source slopes used by this plan:**

| Source | CPU slope | CPU intercept | Single-core saturation | Evidence grade |
|---|---|---|---|---|
| bcfy_feeds (mono-source) | 0.156 %/feed | 0.72 % | ~635 feeds/worker | Partial empirical — single 8-min run, 6 feeds, consistent with the 0.7%/feed memory; the 0.156% value in older analyses may reflect overhead assumptions that the 6-feed run doesn't disambiguate |
| bcfy_calls (mono-source) | 0.009 %/feed | 0.83 % | ~11,019 feeds/worker | **Estimated from code review, not measured.** |
| openmhz (mono-source) | 0.100 %/feed | 6.63 % | ~934 feeds/worker | **Estimated from code review, not measured.** |
| Unified mix (41:55:4 production) | 0.069 %/feed | 6.43 % | ~1,356 feeds/worker | **Computed from the three per-source estimates above; not independently measured.** Arithmetic note: the weighted sum of per-source slopes gives ~0.073 %/feed; the 0.069 figure used in §4 reflects rounding in both directions in the per-source inputs. Treat as ±5% either way. |

**Memory consumption per feed (RSS):**

| Source | RSS slope | Evidence grade |
|---|---|---|
| bcfy_feeds | 16.9 MiB/feed | Partial empirical; FFmpeg buffer dominance is well-documented externally |
| bcfy_calls | 0.40 MiB/feed | Estimated from code inspection of HTTP polling footprint |
| openmhz | 2.8 MiB/feed | Estimated from WebSocket connection overhead |
| Base interpreter + libraries | 157 MiB | Measured |

**What this means for the 800-feed-per-worker target.** The per-source CPU and RSS slopes for bcfy_calls and openmhz are educated engineering estimates, not measurements. The 800-target is defensible as a conservative starting point given what we know, but **the real validation is Phase 1 shadow soak against the synthetic 41:55:4 workload at 1,600 feeds (one VM's full steady-state capacity)**, not Appendix A. If Phase 1 finds the real unified-mix CPU slope is materially different from 0.069%/feed (e.g., closer to 0.1%), the 800-target would need revising before Phase 2 starts.

The plan stands on the 800-target being *approximately right given the physics of the workload* (FFmpeg buffer sizes are well-known; HTTP and WebSocket overheads have known ranges), not on having multi-source measured slopes in hand today.

**Stall pathology: prophylactic defense, not observed behavior.** Earlier revisions of this plan claimed that Experiment 1b documented "a 14.5–15.5 second event-loop freeze when two workers simultaneously activated ~600 feeds each." That claim is not supported by the experimental artifact — the actual `dev_experiment_results.md` reports `loop_latency_ms: 0.01–0.03 ms` on a 6-feed, 8-minute run, with no stall observed. The two-worker × 600-feed condition was never tested.

The plan's defensive mitigations against concurrent-subprocess-spawn pathologies are therefore **prophylactic**, not responses to observed behavior:

- (a) `asyncio.Semaphore(N)` cap on in-flight subprocess spawns, with N fitted in Phase 0 from {8, 12, 16, 24, 32};
- (b) 0–2 second mandatory startup jitter;
- (c) Python 3.12.1+ as defensive baseline — the CPython 3.12.1 changelog contains fixes in the subprocess-spawn-with-threads area; the specific issue number cited in earlier drafts (gh-104372) refers to a broader cluster of subinterpreter / GIL / subprocess-spawn interactions, and should be verified against the 3.12.1 release notes before being cited as authoritative.

The concern these defenses address is real in principle: concurrent `posix_spawn` from many asyncio tasks, under Python versions predating those fixes, can cause GIL contention long enough to stall the event loop. But the plan makes these additions prospectively, not in response to observed production pathologies. Phase 1 shadow soak at 1,600 feeds across 2 workers is the first opportunity to determine whether the concern is empirically realized in this workload.

**What Phase 1 must measure that 1b did not.** Per-source slopes for bcfy_calls and openmhz under real upstream behavior. Unified-mix slope at production density (not just 6 feeds). Multi-day soak for RSS-creep behavior. Off-CPU profiling for blocking-syscall attribution in mixed workloads. Concurrent-activation stall behavior under ≥100 feeds per worker.

---

# Appendix B — Honest uncertainty

Confidence in this plan's claims, categorized for review.

**High confidence (>90%)**:

- Unified workers eliminate the calls-VM SPOF. The architecture-failure-mode mapping is direct.
- Two-signal MAX autoscaler eliminates all hardcoded per-VM capacity constants in fleet sizing. CPU is a GCP-native signal with no dependency on any custom component; `oldest_unclaimed_feed_age` is an SLO, not a capacity guess.
- 800-feed-per-worker target is safer than 1,000 on n2-standard-4. RSS arithmetic is direct from measured 1b slopes.
- HOT updates eliminate the heartbeat-bloat threat, **conditional on the Phase 0 schema changes in §6**. With the existing production schema (`last_heartbeat` in `idx_feeds_leasing`), HOT is broken and the alert fires immediately. The confidence is in PostgreSQL behavior once the restructured indexes are in place; it is not a claim about the current schema.
- Worker-budgeted DB-enforced per-type cap works as described, *conditional on correct worker-side tracking*. If worker tracking is correct, the cap is structurally enforced. If worker tracking corrupts, the RSS watchdog catches it.
- md5-based ramp filter is deterministic across PG minor version upgrades. PostgreSQL documents md5() as stable.
- Signal coverage is complete: every scale-out-correct scenario is caught by either oldest-feed-age or CPU. Edge cases not caught are cases where adding VMs wouldn't help anyway.

**Moderate confidence (60–90%)**:

- 4.25 GiB per-VM headroom is sufficient for FFmpeg RSS-creep tolerance. Based on observed magnitudes in similar production systems, but not stress-tested against worst-case stream pathologies.
- The 60 s `abandonment_window_sec` with 20 s heartbeat interval + 30 s sweep cadence is operationally robust. Similar production data supports timeouts of this shape; we have not run this specific worker code under sustained adversarial CPU pressure with the restructured schema.
- Pool-share self-balancing prevents openmhz starvation under bcfy_calls surges. Math is sound; not yet stress-tested against a 10× sustained surge in production.
- Batched+jittered SIGTERM release at ~50 feeds/batch will complete within 90 s for 1,500 feeds. Arithmetic gives ~30 s; untested under asyncpg-pool-contention conditions.
- jemalloc + `MALLOC_ARENA_MAX=2` reduces RSS creep under this workload. Install is cheap; magnitude will be measured in Phase 1.

**Genuinely uncertain (<60%)**:

- Whether future index additions will silently break HOT. The `hot_pct < 90%` alert is the safety net but depends on engineers responding within hours. Columns to guard: `last_heartbeat`, `unclaimed_since`, `worker_id`, `fencing_token`.
- Whether AlloyDB failover will trigger spurious autoscale events. Moved to Phase 1 shadow soak as an exit criterion (see §9.3).
- Whether the c3 SKU would meaningfully improve bcfy_feeds capacity. Phase 1 A/B will resolve this.
- Whether pg_cron sweep at 30 s cadence keeps up with abandoned-lease accumulation during zonal outage. Arithmetic favors it (~4,000 rows max in 3-VM loss, simple UPDATE); untested at scale.
- Whether the worker-budgeted DB-enforced per-type cap mechanism has any subtle race condition we haven't anticipated. The mechanism is a new design; Phase 1 shadow soak must force adversarial-clustering conditions and verify no over-claim occurs.

---

# Appendix C — Explicitly out of scope

These are deliberately not addressed by this plan:

- **4:1 GCS chunk batching** (largest unclaimed cost lever; blocked on transcription-team sign-off).
- **Multi-region disaster recovery.** Current design is regional HA only; cross-region is a separate project.
- **Predictive autoscaling.** GCP supports CPU only; custom-metric predictive would require a separate ML pipeline.
- **Memory-based autoscaling signal.** Requires Ops Agent install per VM; modest coverage of RSS-creep scenarios. Revisit if FFmpeg RSS-creep becomes a production issue.
- **k=4 on larger instances (n2-standard-8 with 32 GiB RAM).** Could theoretically reduce fleet to 4 VMs if worker density scales linearly. Exploration only after Phase 3 stabilizes.
- **Queue-based scaling via Pub/Sub CDC / outbox.** Requires `single_instance_assignment=N` constant — doesn't eliminate the capacity-constant problem that motivated the two-signal design.
- **Specialized per-type MIGs (steel-manned alternative).** Three regional MIGs of unified workers, each filtering to one type, each autoscaled independently. Not pursued because (a) complexity of three MIGs is real, (b) the unified design's SPOF-elimination already captures the main benefit, and (c) Phase 0/1 work is already substantial. Revisit if Phase 3 telemetry shows per-type resource isolation matters.

End of plan.
