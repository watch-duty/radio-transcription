# Scaling Plan Review (v2)

**Reviewer:** Data / Ingestion team
**Reviewing:** "Audio Ingestion Pipeline — Scaling Plan" (v2 — addresses prior review)
**Date:** 2026-04-12
**Companion doc:** [`FINDINGS.md`](./FINDINGS.md) (catalog-side view)

## TL;DR — ready to approve with 2 number fixes

- ✅ **All prior critical and moderate issues addressed.** The v2 plan fixes the stream-copy framing, corrects the "coordination not built" claim, uses e2-small instead of e2-medium, reflects AlloyDB HA cost, updates connection-pool sizes, realistic NAT cost, and now covers both 6.5K and 12K scopes.
- ❌ **Scenario B (CPU-bound) compute math is internally inconsistent.** VM-type label, feeds/VM range, and per-VM cost don't agree. Scenario B may cost ~2× what the plan shows.
- ⚠ **12K echo allocation under-uses available channels** (311 vs. the 554 Tier 1+2 that FINDINGS.md documents).
- ⚠ **bcfy_calls chunks/sec at 12K is 2× FINDINGS.md's estimate.** Either the plan's ~30% duty cycle is a deliberate conservative assumption (defensible) or it should be reconciled.
- ✅ **Good new additions:** port-capacity comparison (Cloud NAT vs external IPs), firewall rules for GCP health-check + IAP ranges, feed admin UI called out as a gap, leadership decision checklist, Experiments 6–8.
- **Recommendation: approve with corrections.** One blocking fix (Scenario B), five polish fixes.

---

## 1. ❌ Scenario B (CPU-bound) compute math is inconsistent

§3.1 Scenario B table:

| Plan says | Value |
|---|---|
| VM type | n2-highcpu-8 (8 vCPU, 8 GiB) |
| Feeds/VM | 200–300 |
| Fleet size at 6.5K | 25–33 VMs |
| Monthly cost (on-demand) | $2,310 – $3,050 |

**The arithmetic implies ~$92/VM/mo**, but that's not n2-highcpu-8 pricing:

| VM family | On-demand (us-central1, approx) | Matches $92? |
|---|---:|---|
| n2-standard-4 (4 vCPU, 16 GiB) | $141/VM/mo | No (matches Scenario A) |
| n2-highcpu-4 (4 vCPU, 4 GiB) | **~$92/VM/mo** | **Yes — matches Scenario B cost** |
| n2-highcpu-8 (8 vCPU, 8 GiB) | ~$185/VM/mo | No (2× over Scenario B cost) |

The **cost** is for **n2-highcpu-4** but the **label** says n2-highcpu-8. That breaks the feeds/VM math:

- n2-highcpu-4 has only 4 GiB RAM. At the plan's own 25 MiB/feed estimate, that's ~160 feeds max before memory saturates — the 200–300 range **exceeds memory**, let alone CPU.
- n2-highcpu-8 at 200–300 feeds/VM *does* fit the 8 GiB × 1024 / 25 MiB ≈ 327 memory cap, but the cost doubles.

**Corrected scenarios (both plausible, both cost more than Scenario A):**

| Option | VM | Feeds/VM | VMs @ 6.5K | Cost/mo @ 6.5K |
|---|---|---|---:|---:|
| B1 (if truly n2-highcpu-4) | n2-highcpu-4 | ~100–150 | 44–66 | $4,000–$6,000 |
| B2 (if truly n2-highcpu-8) | n2-highcpu-8 | 200–300 | 22–33 | $4,100–$6,100 |

**Either way, Scenario B compute exceeds Scenario A ($1,985), not undercuts it.** The §8 statement "Compute cost increases 30-60%, but total cost increase is ~10-15%" is directionally right but the numbers in the §3.1 table are misleading.

**This is the only blocking issue.** Fix before leadership reads the plan.

---

## 2. ⚠ 12K echo allocation under-uses available channels

Plan's §2 target mix for 12K vs. FINDINGS.md Tier 1+2 recommendation:

| Source | Scaling plan | FINDINGS.md | Delta |
|---|---:|---:|---:|
| bcfy_feeds | 5,000 | 4,757 | +243 |
| openmhz | 381 | 381 | 0 |
| echo | **311** | **554** | **−243** |
| bcfy_calls | 6,335 | 6,335 | 0 |
| **Total** | 12,027 | 12,027 | 0 |

The plan shifts 243 feeds from echo to bcfy_feeds. Both totals sum to 12,027 — but **echo is hardware-capped at 718**; shipping fewer than the available 554 Tier 1+2 channels leaves coverage on the table. Echo is the most scarce source; use all of it.

**Fix:** set echo=554 and bcfy_feeds=4,757 (match FINDINGS.md). Downstream math updates: bcfy_feeds chunks/sec goes 333 → 317 (trivial), echo stays negligible.

---

## 3. ⚠ bcfy_calls chunks/sec estimate is 2× FINDINGS.md

At 6,335 bcfy_calls groups:

| Source | chunks/sec | Implied duty cycle |
|---|---|---|
| FINDINGS.md §3 | 32–95 | 5–15% |
| Scaling plan §2 (via `~0.3 calls / 10s poll`) | ~190 | **~30%** |

Not necessarily wrong — fire-tagged groups may have higher duty than a random openmhz talkgroup. But the two documents should agree. Options:

1. **Plan is being conservative** (size for 30%, expect 5–15%) — then say so explicitly in the plan so it's clear the number is an upper bound, not a point estimate.
2. **FINDINGS.md underestimates** — then update the findings to match, but this needs prod telemetry to settle definitively.

Net effect on 12K total: plan's 623 chunks/sec vs. FINDINGS's 679 (upper bound). Close enough that it doesn't move VM sizing, but worth reconciling so leadership sees one number they can cite.

---

## 4. ⚠ Smaller items worth tightening

### 4.1 OpenMHZ reconnect-stampede mitigation lacks specifics

§9 lists "OpenMHZ reconnect jitter" as a must-fix, but no concrete code change. Add to §9:

> **Mitigation:** in `backend/pipeline/ingestion/collectors/openmhz/collector.py:197-200`, add `await asyncio.sleep(random.uniform(0, _RECONNECT_BACKOFF_CAP_SEC))` jitter before each reconnect. Estimated ~30 minutes of work.

### 4.2 Log-based metric cardinality is worse at 12K scope

Plan §6 correctly says drop `feed_id` labels — but the scale-up magnifies the risk:

- 6.5K scope: 6,575 × 14 VMs = **92,050 time series/metric** (already above GCP's ~30K soft limit if `feed_id` kept)
- 12K scope: 12,027 × 25 VMs = **300,675 time series/metric** (10× worse at 12K)

Worth calling out explicitly in §6 so leadership understands the risk scales non-linearly.

### 4.3 "The existing SIGTERM handler should call `release_feed`" is ambiguous

§3.5 wording is unclear: does the handler *already* call `release_feed`, or does it *need to*? Verify against `normalizer_runtime.py` signal handling and state definitively. If the behavior doesn't exist yet, add it to Phase 0 checklist.

### 4.4 GCS storage math is slightly optimistic

Plan's $614/mo at 6.5K / 7-day retention implies ~30 TB steady-state. Check:

- 435 chunks/sec × ~125 KB/chunk × 86,400 s/day × 7 days = ~33 TB steady-state
- 33 TB × $0.02/GB-mo = **~$660/mo** (plan says $614)

Off by ~$50/mo. Not material; fix if you're tightening up the cost table.

---

## 5. ✅ What the v2 revision got right (all prior issues addressed)

| Prior review issue | v2 status |
|---|---|
| "Stream-copy ffmpeg" claim | Removed; §1 and §3.1 name CPU as a real constraint |
| "Feed coordination not built in codebase" | Cited `feed_queries.py:3-47`, no longer a blocker |
| Current compute "e2-medium" → actual e2-small | Corrected with file/line citation |
| AlloyDB zonal → regional HA cost | Corrected ($455/mo) |
| Connection pool 10+1=11 → actual 5+1=6 | Corrected (84 at 14 VMs, 150 at 25 VMs) |
| Cloud NAT existence unverified | Now explicitly flagged in §1 and §3.2 |
| Cloud NAT savings $5,800 → realistic $1,570 | Corrected throughout |
| 12K scope ignored | Dual-tracked in every relevant section |
| Experiment 1 missing CPU measurement | Added as primary signal |
| FLAC concat feasibility | Added as Experiment 6 |
| AlloyDB at 1,500 TPS | Added as Experiment 7 |
| Broadcastify polling probe | Added as Experiment 8 |

New additions that land well:

- **Firewall rules for external IPs** (health-check + IAP SSH ranges). Real security concern I hadn't flagged.
- **Port-capacity comparison** (Cloud NAT 64–4,096 per VM vs external IP 64,512). Valid technical advantage of external IPs I hadn't called out.
- **Feed admin UI** in §9. At 6.5K+ feeds manual management is infeasible — good to surface to leadership as a known gap.
- **Decision checklist §10.** Five blocking decisions with cost impact and timing — leadership-friendly format.
- **AlloyDB metadata retention** in §9. Real operational concern at 890 writes/sec sustained. Deserves its own experiment eventually.

---

## 6. Recommendation

**Approve with corrections.** The plan is substantively right and ready to execute after six edits:

1. **Fix Scenario B compute math** (the one blocking issue — reconcile VM-type label, feeds/VM range, and cost).
2. **Match 12K feed mix to FINDINGS.md** (echo=554, bcfy_feeds=4,757).
3. **Reconcile bcfy_calls duty cycle** with FINDINGS.md, or state the 30% as a conservative ceiling.
4. **Add OpenMHZ jitter mitigation** code pointer to §9.
5. **Flag 12K metric cardinality** explicitly in §6 (300K series vs. 92K at 6.5K).
6. **Verify SIGTERM handler behavior** before Phase 0 ("should call release_feed" → "does" or "needs to").

Items 2–6 are 30-minute edits. Item 1 requires someone to decide which VM family they meant and fix the math for real.

**Then:** proceed to Experiment 1. It remains the critical path — VM family decision blocks everything else. Scope (6.5K vs 12K) can be decided in parallel with the experiment.

---

## Appendix A — Pricing references used in this review

Cost figures are from public Google Cloud pricing for us-central1 on-demand, as of my training data; treat as approximate and verify with the team's actual GCP billing. Key rates:

| Resource | Rate |
|---|---|
| n2-standard-4 on-demand | ~$141/VM/mo ($0.194/hr × 730 hr) |
| n2-highcpu-4 on-demand | ~$92/VM/mo |
| n2-highcpu-8 on-demand | ~$185/VM/mo |
| GCS Class A operations | $0.005 per 1,000 ops |
| GCS Standard storage | $0.02/GB-mo |
| Cloud NAT data processing | $0.045/GB |
| External ephemeral IP | $0.005/hr ($3.65/mo) |
| AlloyDB (per vCPU-hr, zonal) | ~$0.156 (regional is 2×) |
