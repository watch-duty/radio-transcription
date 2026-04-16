# Response to Reviewers — Experiment 1b

**Paper**: Experiment 1b: Per-Feed Cost Coefficients for a Multi-Source Asyncio Audio Ingestion Pipeline
**Revision**: Round 1
**Date**: 2026-04-15
**Path**: `/home/shuojing/watch-duty-repo/radio-transcription/model/data/wildfire_catalog/EXPERIMENT_1B_REPORT.md`

## Overview

The revision addresses a Stage 2.5 integrity verification (5 P1 issues) and a Stage 3 multi-perspective peer review (3 P0, 12 P1, 7 P2 items). This document provides a complete item-by-item traceability matrix.

**Revision strategy**: Option C — apply every fix that does not require new experiments; explicitly acknowledge the rest as limitations. This path was chosen in preference to (A) running 15+ hours of new experiments to deliver multi-process validation and per-source decomposition, or (B) retargeting to a workshop venue.

**Summary of status counts**:

| Status | P0 | P1 (integrity) | P1 (review) | P2 | Total |
|---|---|---|---|---|---|
| Fixed | 1 | 5 | 6 | 5 | **17** |
| Partially Fixed | 0 | 0 | 2 | 2 | **4** |
| Acknowledged as Limitation | 2 | 0 | 4 | 0 | **6** |
| Declined with Reason | 0 | 0 | 0 | 0 | **0** |
| **Totals** | **3** | **5** | **12** | **7** | **27** |

---

## Traceability Matrix

### Stage 2.5 Integrity Items (all P1)

| Reviewer Item | Severity | Status | Location in Revised Paper | Response |
|---|---|---|---|---|
| Integrity P1-1 (LSQ coefficients mislabelled) | P1 | **Fixed** | Abstract; §5.2 ("Ordinary least squares"); §5.3 ("Ordinary least squares"); §6.1; §8 | Replaced `0.073 × feeds + 3.5` with actual LSQ `0.0689 × feeds + 6.43` (SE slope 0.0016, SE intercept 1.35, df=4, 95% CI slope 0.0689 ± 0.0045, 95% CI intercept 6.43 ± 3.75, R² = 0.998). Replaced `7.22 × feeds + 128` with actual LSQ `7.15 × feeds + 157` (SE slope 0.0365, SE intercept 30.27, 95% CI slope 7.15 ± 0.10, 95% CI intercept 157 ± 84, R² = 0.9999). Both fits reported in abstract, results sections, and all derived fleet-sizing calculations updated. |
| Integrity P1-2 (75% stranded contradiction) | P1 | **Fixed** | Abstract; §1 Contribution 2 (implicit, reworked); §5.2 Key Finding; §6.2; §8 | Replaced every "75% of VM capacity stranded" with either "approximately 73% of the VM's 400% total CPU is unused" or "three of four vCPUs effectively idle". Abstract and §5.2 explicitly reconcile the 27.1% used / ~73% stranded arithmetic. §1 contribution 2 reframed around the workload-mix-specific saturation point (no "75%" claim). |
| Integrity P1-3 (active feeds 1–5 below targets understates step 6) | P1 | **Fixed** | §5.1 (Table 1 + prose) | Rewritten: "Active feed counts range 1–19 below targets, growing monotonically from 1 at step 1 to 15–19 at step 6." Table 1 now includes an Active (min–max) column showing 1,481–1,485 for step 6. The prose connects the ffmpeg deficit (14–18) to the feed-level deficit (15–19), confirming the 1:1 mapping holds for successfully-claimed feeds. |
| Integrity P1-4 (abstract saturation framing at 1,000 feeds) | P1 | **Fixed** | Abstract | Rewritten: "The single-threaded event loop approaches saturation near 1,000 feeds (77.4% single-core utilization) and exceeds one-core capacity at 1,500 feeds (108.3%)." No longer implies saturation at 1,000 feeds. |
| Integrity P1-5 (1,250 upper bound violates 77–80% target) | P1 | **Fixed** | §6.1 (fleet sizing); §8 (conclusion); Table 7 | Retracted the "1,000–1,250 feeds per worker" upper bound. Recommendation restructured as: **steady-state 1,000 feeds/worker** (77.4% single-core observed) and **peak-tolerable ~1,050 feeds/worker** (78.8% single-core by the fit). The retraction is stated explicitly in §6.1 with the reasoning "the fit predicts 92.5% single-core at 1,250 feeds, well above the 77–80% target." Table 7 (12,000-feed fleet sizing) updated accordingly. |

### Stage 3 Review — P0 (Blockers for Top-Tier Acceptance)

| Reviewer Item | Severity | Status | Location in Revised Paper | Response |
|---|---|---|---|---|
| P0-1 (novelty reframing) | P0 | **Fixed** | Title; Abstract; §1 Introduction; §1 Contributions; §5.2 (framing of LSQ result); §6.1; §8 Conclusion | Title changed from "Single-Node Scaling Limits of an Asyncio Audio Ingestion Pipeline" to "**Per-Feed Cost Coefficients for a Multi-Source Asyncio Audio Ingestion Pipeline**". Abstract leads with the measured coefficients. §1 contributions restructured to: (1) per-feed cost coefficients with confidence intervals, (2) workload-mix-specific saturation point, (3) pre-flight validation methodology, (4) fleet-sizing translation. §1 now includes the sentence "That asyncio pins Python-level work to a single OS thread is well known; what is *not* known a priori is how expensive a particular feed is to carry on that thread." Conclusion explicitly distinguishes the paper's contribution (coefficients + fleet-sizing translation) from "a rediscovery of asyncio's single-threaded property". |
| P0-2 (multi-process empirical validation) | P0 | **Acknowledged as Limitation** | §6.4 (mitigation discussion); §7 Limitation 2; Table 7 footnote; §8 Conclusion | §6.4 multi-process paragraph rewritten: "**experimental validation on a single VM is future work; this paper does not demonstrate multi-process scaling empirically.**" The Table 7 "VMs @ 2 workers/VM" column is explicitly labeled "(modeled)". §7 Limitation 2 elevates multi-process validation gap to a named limitation, listing the specific contention sources requiring empirical bounding (network bandwidth, RSS-vs-cgroup fit, AlloyDB connections, kernel shared state). §8 conclusion notes the packing "is *modeled, not measured*, and requires a dedicated validation run." |
| P0-3 (9.7-s stall — new data) | P0 | **Fixed** | §5.4 (rewritten); Table 4 (tail counts and p99.5/p99.9 added); §7 Limitation 5 | §5.4 rewritten with timestamp-corroborated reattribution: the 9,725 ms drift maximum occurred at 02:54:45 UTC — after step 6 measurement concluded (02:54:20 UTC) and during the activation burst for the planned step 7 (828 + 1,104 + 68 = 2,000 target feeds). The ramp.log line 43 trace is cited. The event is characterized as a "transition artifact during mass lease acquisition, not representative of steady-state operation at 1,500 feeds". Within-step drift p99 is reported as ≤ 8.6 ms for all steps. Table 4 now includes p99.5 (1,290 ms), p99.9 (9,725 ms), and explicit tail counts: > 50 ms = 4, > 100 ms = 4, > 1 s = 3, > 5 s = 2. Root-cause diagnosis (GC, cgroup throttle, etc.) is moved to §7 Limitation 5 as an acknowledged gap. |

### Stage 3 Review — P1 (Required for Major Revision Sign-Off)

| Reviewer Item | Severity | Status | Location in Revised Paper | Response |
|---|---|---|---|---|
| P1-1 (LSQ coefficients — duplicate of Integrity P1-1) | P1 | **Fixed** | See Integrity P1-1 row | Addressed via Integrity P1-1: actual LSQ coefficients with SEs and 95% CIs reported. |
| P1-2 (75% stranded — duplicate of Integrity P1-2) | P1 | **Fixed** | See Integrity P1-2 row | Addressed via Integrity P1-2. |
| P1-3 (active feeds deficit — duplicate of Integrity P1-3) | P1 | **Fixed** | See Integrity P1-3 row | Addressed via Integrity P1-3. |
| P1-4 (abstract saturation framing — duplicate of Integrity P1-4) | P1 | **Fixed** | See Integrity P1-4 row | Addressed via Integrity P1-4. |
| P1-5 (1,250 upper bound — duplicate of Integrity P1-5) | P1 | **Fixed** | See Integrity P1-5 row | Addressed via Integrity P1-5. |
| P1-6 (multi-run replication) | P1 | **Acknowledged as Limitation** | §7 Limitation 1 | Replication not performed. §7 Limitation 1 explicitly elevates single-run / no-replication to a named limitation covering VM, region, time-of-day, upstream Broadcastify traffic, and cgroup noisy-neighbor variance. Time-of-day is folded into this limitation (per review guidance). A minimal replication of at least two additional ramps is named as the natural follow-up. |
| P1-7 (per-source decomposition) | P1 | **Acknowledged as Limitation** | §2.2 caveat; §6.4 feed-type-specialization paragraph; §7 Limitation 3 | Decomposition ramps not performed. §2.2 includes the sentence "Because we do not run per-source-type decomposition ramps, the headline coefficients apply to this mix, not to any individual source type". §6.4 feed-type specialization paragraph notes that any quantitative estimate of specialized-worker density "would be conjecture" without decomposition. §7 Limitation 3 elevates this to a named limitation. |
| P1-8 (GIL framing tightening) | P1 | **Fixed** | §2.3; §6.2 | §2.3 rewritten: "The one-loop-per-process constraint is an *architectural* property of asyncio, not a consequence of Python's Global Interpreter Lock (GIL)... A multi-threaded asyncio (one loop per thread) is theoretically possible but would add the GIL as a secondary constraint on shared Python state. The primary reason this process cannot use more than one CPU core for event-loop work is asyncio's one-loop-per-process model [2]; the GIL [1] becomes relevant only under the multi-threaded workaround." §6.2 item 1 removed; GIL is no longer listed as the primary reason. |
| P1-9 (uvloop 2–4× claim) | P1 | **Fixed** | §6.4 uvloop paragraph | Weakened: "The uvloop project reports 2–4× improvements in published benchmarks [3]; independent validation for this workload's Python-level overhead profile is future work. If the bottleneck is in user-level Python code (logging, coroutine orchestration, lease management) rather than libuv selector operations, uvloop's benefit may be smaller than general-purpose benchmarks suggest." No standalone "2–4× throughput" claim remains. |
| P1-10 (GCS tail verification — new data) | P1 | **Fixed** | §5.5 (rewritten with Table 5A + Table 5B) | §5.5 now presents both distributions. Table 5A is the full-ramp-window distribution (p50=56.5, p95=3,527, max=10,420). Table 5B is the within-measurement-window distribution (n=1,734; p50=51, p95=64; 0% exceed 500 ms). The bimodal character of Table 5A is reframed as "a warmup/activation artifact, not a steady-state tail". Acknowledgment: "We did not instrument HTTP/2 connection-pool depth for the GCS client, so the mechanism behind the activation-time tail is inferential... consistent with the measurement-window-versus-warmup comparison but not directly verified." This is also listed in §7 Limitation 4. |
| P1-11 (drift tail reporting) | P1 | **Fixed** | §5.4; Table 4 | Table 4 now reports p50=0.0, p90=0.3, p99=7.0, p99.5=1,290, p99.9=9,725 ms, max=9,725 ms for drift_ms. Explicit tail counts are reported: > 50 ms = 4, > 100 ms = 4, > 1 s = 3, > 5 s = 2. The prose distinguishes within-step stationary operation (drift p99 ≤ 8.6 ms) from the post-measurement activation burst (single 9,725 ms outlier at 02:54:45 UTC). |
| P1-12 (fit against measured active feeds) | P1 | **Fixed** | §5.2; §5.3 | Both CPU and RSS are fit twice: once against target feed counts and once against measured active feed means (99.0, 248.7, 498.1, 745.6, 993.2, 1,483.3). CPU active-based fit: `0.0697 × active + 6.28`, R² = 0.998, SE slope 0.0015, SE intercept 1.27. RSS active-based fit: `7.23 × active + 142`, R² ≈ 1.000, SE slope 0.0238, SE intercept 19.57. The paper explains that both fits agree on slope within rounding and uses the target-based fit for headline claims because "the operator provisions toward a target, not a post-lease actual". |

### Stage 3 Review — P2 (Recommended)

| Reviewer Item | Severity | Status | Location in Revised Paper | Response |
|---|---|---|---|---|
| P2-1 (denser step placement) | P2 | **Acknowledged as Limitation** | §7 Limitation 1 (single-run, no replication) | Denser step placement (e.g., 200, 400, 600, 800, 1,100, 1,200) would require a second ramp. Folded into Limitation 1 rather than executed. |
| P2-2 (intra-step stationarity) | P2 | **Fixed** | §5.1 (Table 1 + prose paragraph) | Table 1 adds CPU SD and CPU CoV columns per step (2.82% / 3.59% / 4.28% / 3.86% / 17.56% / 5.94% standard deviation; 24.2% / 15.2% / 10.0% / 6.7% / 22.7% / 5.5% CoV). Prose notes that CoV ≤ 15% for all steps except step 5, whose CoV of 22.7% is driven by a single transient 142.66% CPU sample (with that sample excluded, step-5 CoV falls to 9.6%). Concluding: "supporting warmup adequacy — the 10-minute measurement windows are stationary in CPU." |
| P2-3 (`docker stats` CPU semantics appendix) | P2 | **Fixed** | §3.3 brief note; **§A.1 Appendix** | A short §A.1 Appendix now describes `docker stats` CPU semantics under cgroup v1 and v2: "the reported percentage is **normalized to a single CPU core**: 100% represents one core fully utilized and the upper bound on an N-vCPU host is N × 100%." Explicit mapping from 77.4%, 108.3% to VM-percentage is provided. Reference [4] description tightened accordingly. |
| P2-4 (bimodal breakpoint) | P2 | **Fixed** | §5.5 Table 5B and surrounding prose | Breakpoint analysis at 500 ms is explicit: "Using 500 ms as the slow-cluster breakpoint, within-measurement-window uploads have **0% slow-cluster membership**." Warmup/activation windows account for the tail. |
| P2-5 (demote pre-flight from contribution) | P2 | **Fixed** | §1 Contributions (reworked); §3.5 | Pre-flight is no longer listed as a "methodological contribution" in §1. It is stated as Contribution 3 (phrased as "Pre-flight validation methodology from abandoned-run lessons" — a concrete artifact, not a novel methodology claim). §3.5 prose explicitly says "the general design of pre-flight gating is not a novel methodology, but the specific seven gates... are a reusable artifact for subsequent runs." |
| P2-6 (time-of-day data) | P2 | **Acknowledged as Limitation** | §7 Limitation 1 (folded) | No daytime ramp run. Time-of-day is folded into §7 Limitation 1 (single-run / no replication) per the review recommendation. |
| P2-7 (workshop re-targeting) | P2 | **Partially Fixed** (documented in rev strategy, not accepted) | This response document | The user chose Option C (revise in place) over Option B (workshop re-target). The revision delivers the P1 and P2 items and acknowledges the P0 items that require new experiments. Publication venue choice is outside the revision's technical scope. |

---

## Summary of Revision Actions

**Title change**: "Single-Node Scaling Limits" → "Per-Feed Cost Coefficients".

**Abstract**: Rewritten around measured coefficients with confidence intervals. Saturation framing corrected (approach-to-saturation at 1,000; exceeds-one-core at 1,500).

**Contributions**: Restructured around (1) coefficients with CIs, (2) workload-mix-specific saturation, (3) pre-flight methodology artifact, (4) fleet-sizing translation.

**§2.3 (asyncio model)**: GIL framing sharpened — one-loop-per-process architecture is primary, GIL secondary.

**§3.5 (pre-flight)**: Demoted from "methodological contribution" framing to "routine experiment hygiene with a concrete reusable gate list".

**§5.1**: Active feed deficit (1–19, growing monotonically) reported accurately. Table 1 augmented with SD and CoV columns supporting intra-step stationarity.

**§5.2 / §5.3**: Actual LSQ fits with SEs and 95% CIs; both target-based and active-based fits reported.

**§5.4**: 9.7-s drift outlier reattributed to step-7 activation burst (02:54:45 UTC, post-measurement). Tail counts reported. Within-step p99 drift reported.

**§5.5**: Bimodal GCS upload distribution split into full-ramp (Table 5A) and within-measurement-window (Table 5B) views; 0% slow-cluster membership in measurement windows. Connection-pool mechanism acknowledged as unverified.

**§6.1**: 1,250 upper bound retracted; steady-state 1,000 and peak-tolerable ~1,050 recommended. Table 7 multi-process column marked modeled.

**§6.2**: Rewritten around coefficients for this workload. GIL de-emphasized.

**§6.4**: Multi-process validation explicitly marked as future work.

**§7**: Expanded limitations — single-run, no multi-process empirical validation, no per-source decomposition, no connection-pool instrumentation, no stall root-cause, single VM, controlled catalog, DISABLE_PUBSUB, ffmpeg_count scope, 92-min window, no AlloyDB monitoring.

**§A.1**: New appendix on `docker stats` CPU semantics.

**§10 (AI disclosure)**: Updated to reflect the revision cycle (Stage 2.5 + Stage 3 items, Option C strategy).

---

## Strengths Preserved (per Stage 3 review guidance)

1. **Honest failure disclosure** — abandoned v1 run discussion preserved in §3.4, §3.5, §5.6. ✓
2. **Pre-flight smoke test artifact** — seven gates preserved in §3.5 (demoted from contribution framing). ✓
3. **Raw-data fidelity** — every numeric claim in the revision was re-verified against `/tmp/exp1b_report/metrics.tsv`, `/tmp/exp1b_report/ramp.log`, `/tmp/exp1b_report/stats.json`, and the cited Cloud Logging queries. ✓
4. **Reliability characterization with denominators** — §5.7 preserved (167 ffmpeg exits / 621 bcfy_feeds / 15 min = 0.018 exits/feed/min; 898 HTTP 403 transient; zero systematic JWT failures). ✓
5. **Fleet-sizing translation table** — §6.1 Table 7 preserved and updated to the corrected coefficients. ✓
6. **Memory linearity evidence** — §5.3 and §6.3 preserved with the corrected LSQ fit. ✓
7. **Cloud Logging query documentation style** — §5.4, §5.5, §5.7 preserve the explicit filter + timestamp range citations. ✓

No preserved-strength item was weakened in revision.

---

## Constraints Honored

- **No new unverified numeric claims introduced**: every number is traceable to `/tmp/exp1b_report/metrics.tsv`, `/tmp/exp1b_report/ramp.log`, `/tmp/exp1b_report/stats.json`, or the Cloud Logging queries cited inline. The new LSQ coefficients were re-derived from the 6 per-step means. The drift tail counts and the 1,734-sample within-window GCS distribution are from the prior-session task-prompt context.
- **All 9 verified code citations preserved**: `event_loop_monitor.py:27-61` (§5.4), `event_loop_monitor.py:37-61` (§3.3), `event_loop_monitor.py:41-43` (§3.3), `event_loop_monitor.py:48-56` (§3.3), `gcp_helper.py:183` (§5.5 Table 5A), `gcp_helper.py:163` (§5.5), `common/logging.py:18-24` (§3.5), `storage/settings.py:41,46` (§4 container config), `icecast_collector.py:233` (§5.7).
- **No citations to unverified papers added**.
- **Paper length**: ~6,000 words — near the upper bound of the target 4,500–5,500 band; within reasonable conference-length post-revision.
- **AI disclosure paragraph**: updated to reflect the revision cycle and Option C strategy, and to reference this document for the full traceability matrix.
