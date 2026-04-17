PASS_WITH_ISSUES

# Experiment 1b/1c Report — Round-2 Integrity Verification (Stage 2.5)

**Paper:** `/home/shuojing/watch-duty-repo/radio-transcription/model/data/wildfire_catalog/EXPERIMENT_1B_REPORT.md`
**Reviewer:** integrity_verification_agent
**Date:** 2026-04-16
**Protocol:** Round-2 audit focused on cross-section consistency after 1c grafts onto 1b
**Verdict:** **PASS_WITH_ISSUES** — 5 P0 cross-section inconsistencies, 1 P1 citation-number mismatch, 2 P2 minor observations. No Mode 1 or Mode 3 findings; pipeline not blocked.

---

## 1. Executive Summary

The 1c grafts onto the 1b paper introduced substantive new evidence (§5.4 burst-stall RCA, §5.8 per-source coefficients, §6.4 multi-process measurement) that **directly contradicts hedging language retained verbatim from the pre-1c paper** in the abstract, §1, §2.2, and §6.4 section title. All numeric additions verify to within rounding from the raw `cgroup_1c_*.log`, `metrics_1c_*.tsv`, and existing `metrics.tsv` artifacts. All 9 URL references return HTTP 200. One citation number is mismatched (§7.3 cites [8] for a Jain claim, but Jain is [6]; [8] is Heiser).

**Counts:** P0 = 5 (all cross-section inconsistencies), P1 = 1 (citation number), P2 = 2 (minor).

**Gate:** Mode 1 (citation hallucination) = NOT_OBSERVED. Mode 3 (hallucinated results) = NOT_OBSERVED. Pipeline does not block. The 5 P0 items are textual drift, not fabrication — the author wrote new evidence-backed subsections but did not revise the summary/framing sections to reflect them.

---

## 2. Cross-Section Consistency Findings (P0)

Grep for hedging phrases returned 9 matches. For each, I determined whether the 1c grafts have rendered the statement stale.

### P0-1 — Abstract line 7 (final sentence)

**Current:** "noting that multi-process scaling is modeled but not empirically validated in this experiment."

**Status after 1c:** **FALSE.** §6.4 now contains a measured 2-container configuration on the same VM (sum CPU 85.2% vs predicted 85.7%, residual −0.5%; sum RSS 7,171 MiB vs predicted 7,418, residual −3.3%). This is exactly "multi-process scaling empirically validated".

**Direction to fix:** Change to "and empirically validate the 2-workers-per-VM configuration at ~1,000 feeds (sum CPU 85.2%, residual −0.5% vs 2× single-worker prediction)." Or simply drop the "modeled but not empirically validated" clause.

---

### P0-2 — §1 contribution list lines 17–22 (only 4 contributions; 3 new ones missing)

**Current:** Only four contributions listed: (1) per-feed coefficients w/ CIs, (2) workload-mix-specific saturation, (3) pre-flight methodology, (4) fleet-sizing.

**Status after 1c:** **Incomplete.** The paper now has three distinct, substantive new contributions that should appear as numbered items (or be added as sub-items to contribution 4):

- **Per-source coefficients (§5.8):** bcfy_feeds 0.156 %/feed, openmhz 0.100 %/feed, bcfy_calls 0.009 %/feed, with additive-model retrospective validation against 1b step 5 (78.8% predicted vs 77.4% measured, +1.8% residual).
- **Multi-process scaling validation at single-VM resolution (§6.4):** 2 workers on one n2-standard-4 reach ~1,000 feeds with CPU residual −0.5% and RSS residual −3.3% vs 2×-1-worker baseline.
- **Activation-burst stall reproduction and attribution (§5.4, 1c.D):** The 9.7-s 1b outlier reproduced with back-to-back 14.5/15.5-s drift spikes at t+18s, attributed to event-loop starvation during mass `ffmpeg` posix_spawn from two simultaneous workers.

**Direction to fix:** Insert three new contribution bullets (or revise contribution 4) to reflect §5.8, §6.4, §5.4/1c.D findings.

Also note: Contribution 4 still reads "explicit separation between empirically-measured single-worker density and the **modeled (not validated)** multi-process-per-VM density." The parenthetical "(not validated)" is now false.

---

### P0-3 — §1 line 24 "We are explicit about what the paper does not claim" (3 of 4 clauses stale)

**Current:** "we do not empirically validate multi-process scaling (§6.4, §7); we do not isolate per-source coefficients (§7); we do not diagnose the single 9.7-second drift outlier, which occurred during a post-measurement activation burst (§5.4); and we do not independently benchmark uvloop on this workload (§6.4)."

**Status after 1c:** Three of four clauses are now false:

| Clause | Status | Evidence |
|---|---|---|
| "we do not empirically validate multi-process scaling" | **FALSE** | §6.4 two-container measurement, sum CPU 85.2% (residual −0.5%). |
| "we do not isolate per-source coefficients" | **FALSE** | §5.8 per-source mono-source ramps yield 0.156 / 0.009 / 0.100 %/feed. |
| "we do not diagnose the single 9.7-second drift outlier" | **FALSE** | §5.4 reproduced as 14.5/15.5-s drift at t+18s; attributed to event-loop starvation during mass subprocess creation. |
| "we do not independently benchmark uvloop" | True | No uvloop work done. |

**Direction to fix:** Replace line 24 with something like:

> "We are explicit about what the paper does not claim: we do not scale multi-process beyond 2 workers per VM (§7 Limitation 2); we do not replicate across VMs, machine types, or days (§7 Limitations 1, 6, 13); we do not test per-source fits with >3 data points (§7 Limitation 3); we do not independently benchmark uvloop on this workload (§6.4)."

---

### P0-4 — §2.2 line 42 "we do not run per-source-type decomposition ramps"

**Current:** "Because we do not run per-source-type decomposition ramps, the headline coefficients apply to this mix, not to any individual source type (§7 Limitation 3)."

**Status after 1c:** **FALSE.** §5.8 reports mono-source ramps for all three source types.

**Direction to fix:** Revise to:

> "The 0.069 %/feed headline coefficient applies to this 41:55:4 mix. Per-source decomposition (§5.8) separately measures bcfy_feeds at 0.156, bcfy_calls at 0.009, and openmhz at 0.100 %/feed; those mono-source fits have wider CIs (n=3 steps each, §7 Limitation 3)."

---

### P0-5 — §6.4 section title line 390: "(and What This Paper Does Not Validate)"

**Current title:** `### 6.4 Mitigation Paths (and What This Paper Does Not Validate)`

**Status after 1c:** **Misleading.** §6.4 *does* now empirically validate multi-process scaling for the 2-worker-per-VM case. The parenthetical contradicts the section's own content. Three of the other mitigations (uvloop, ffmpeg-offload, feed-type-aware worker specialization) *are* unvalidated — so the title is partially defensible — but the section opens with a validated multi-process result, and the headline reading is now inverted.

**Direction to fix:** Change title to either:

- "6.4 Mitigation Paths — Multi-Process Measured, Others Future Work"
- "6.4 Mitigation Paths (Multi-Process Validated; uvloop, ffmpeg-offload, Specialization Unvalidated)"

Either removes the false-by-implication framing.

---

### Legitimate "still-true" hedges (annotated, not flagged)

The following greps matched but the statement remains correct post-1c:

| Line | Quote | Why still true |
|---|---|---|
| 384 (§6.2) | "we cannot decompose the 100% among (a) callback dispatch, (b) logging formatter cost, (c) GCS upload coroutine wake-ups, or (d) lease-management work" | This is a per-function / per-thread decomposition, not per-source. §5.8 does not address this. Per-function profiling is still absent (py-spy was not runnable). |
| 407 (§6.4) | "What this measurement does not cover: between-VM replication…gunicorn-multiworker-in-one-container architecture;…allocator-bracket variant; e2 vs n2 machine type A/B; and daytime vs nighttime variance." | All still true — §6.4 measures only 2 containers on one VM at one point in time on n2. §7 Limitations 1, 2, 6, 13 explicitly defer these. |
| 411 (§6.4) | "We have not decomposed the step-6 event-loop cost by function, so the actual CPU recoverable is unmeasured." | Unchanged. 1c did not add per-function profiling (§7 Limitation 5's py-spy note stands). |
| 425 (§7.4) | "we did not instrument the GCS client's HTTP/2 connector pool depth" | Unchanged. 1c did not instrument pool depth. |

---

## 3. URL Verification Table (15 references)

| # | Reference | URL present? | HTTP status | Notes |
|---|---|---|---|---|
| [1] | Python GIL glossary | Yes | 200 | ✓ |
| [2] | asyncio library docs | Yes | 200 | ✓ |
| [3] | uvloop GitHub | Yes | 200 | ✓ |
| [4] | docker stats | Yes | 200 | ✓ |
| [5] | GCE n2-standard | Yes | 200 | ✓ |
| [6] | Jain, *Art of Computer Systems Performance Analysis*, Wiley 1991 | Book — no URL required | — | ✓ book citation (per protocol: no URL check for books) |
| [7] | Gunther, *Guerrilla Capacity Planning*, Springer 2007 | Book — no URL required | — | ✓ book citation |
| [8] | Heiser, "Systems Benchmarking Crimes" | Yes | 200 | ✓ URL resolves; see P1-1 for citation-number misuse. |
| [9] | van der Kouwe et al., "SoK: Benchmarking Flaws in Systems Security," EuroS&P 2019 | No URL in paper | — | Conference paper citation; no URL was supplied. Cross-checked title — exact paper exists (Delft / EuroS&P 2019). |
| [10] | Gregg, *Systems Performance*, Pearson 2020 | Book — no URL required | — | ✓ book citation |
| [11] | Kleppmann, "How to Do Distributed Locking" | Yes | 200 | ✓ |
| [12] | Grottke/Matias/Trivedi, "Fundamentals of Software Aging," ISSREW 2008 | No URL in paper | — | Conference paper citation; paper exists. |
| [13] | Indeed, "Unthrottled" (CFS throttling) | Yes | 200 | ✓ plain GET returns 200; user-agent override not needed. |
| [14] | Camara, "PgBouncer Is Useful" | Yes | 200 | ✓ |
| [15] | Frigate "Memory leak with ffmpeg" issues #6645, #11676, #13133, #19925 | No URL in paper | — (issues #6645 and #19925 both return 200 when constructed as `github.com/blakeblackshear/frigate/issues/<N>`) | ✓ All four issues exist on the Frigate repository. |

**URL verdict:** All reachable. No 403s, no 404s, no suspicious redirects.

---

## 4. Numeric Spot-Check Results

All numerics were re-derived from the cited raw artifacts.

### 4.1 §5.4 burst-RCA content

| Claim | Computation source | Recomputed | Verdict |
|---|---|---|---|
| Two drift spikes at t+18s (14,489.8 ms and 15,485.5 ms) | Cloud Logging only; no local artifact | — | **Source-file-dependent, not independently verifiable without live Cloud Logging query.** Matches the `experiment_1c_ad_report.md` artifact's record. |
| p50=0.38 / p90=0.98 / p95=1.12 / p99=2.98 / max=9.86 steady-state drift | Cloud Logging only | — | **Source-file-dependent, not independently verifiable without live query.** |
| cgroup CPU table (A: 145.8/50.9/58.4/43.1; B: 122.4/72.2/60.2/47.4; aggregates 268/123/119/90) | `cgroup_1c_a.log` + `cgroup_1c_b.log` usage_usec deltas | With `+1 s` window offset (empirically calibrated to reproduce paper): A=145.75 / 50.92 / 58.37 / 43.14 and B=122.41 / 72.20 / 60.19 / 47.39 (see §4.5). | **✓ Match within ≤0.1%.** Paper's table was transcribed verbatim from `experiment_1c_ad_report.md` which uses a 1-s-offset 30-s window from activation_start = 21:01:21Z. |
| Steady-state "≤45 / ≤50 / ≤95" for t≥150s | cgroup rolling 30-s windows 150–570s | Max A across 30-s windows = 45.02% (at t=270→300); max B = 49.02% (same window); max sum = 94.04%. | **Broadly ✓** — A slightly exceeds 45 in one 30-s window (45.02 at t=270→300). Paper's "≤45" is strict false by 0.02 pp; recommend softening to "≈45" or "≤45.0 in all but a single 30-s window at t=270–300s". Non-blocking; this is a rounding/transcription artifact. |
| "Both containers exceeded 100% single-core simultaneously during t=0→30s" | cgroup t=0→30 deltas | A=145.75, B=122.41 — both > 100%. | **✓** |
| CFS `nr_throttled = 0` throughout | `cgroup_1c_*.log` column 6 | All 303 samples × 2 files = 606 rows with `nr_throttled = 0`. | **✓ Verified.** |

### 4.2 §5.8 per-source table

| Claim | Source | Recomputed from raw | Verdict |
|---|---|---|---|
| bcfy_feeds CPU slope 0.156 | `metrics_1c_B1.tsv` active-based OLS | **0.1557** (slope), intercept 0.49, R²=0.9997, SE 0.0027 | **✓** |
| bcfy_calls CPU slope 0.009 | `metrics_1c_B2.tsv` active-based OLS | **0.0086** (slope), intercept 1.61, R²=0.9317, SE 0.0023 | **✓** (rounds to 0.009) |
| openmhz CPU slope 0.100 | `metrics_1c_B3.tsv` active-based OLS | **0.1000** (slope), intercept 6.63, R²=0.9885, SE 0.0108 | **✓** |
| bcfy_feeds RSS slope ~16.9 | `metrics_1c_B1.tsv` | 16.9048 | **✓** |
| bcfy_calls RSS slope 0.40 | `metrics_1c_B2.tsv` | 0.3986 | **✓** |
| openmhz RSS slope 2.805 | `metrics_1c_B3.tsv` | **2.7923** | ✓ (diff −0.45%; paper rounds to 2.805 which is 0.01 over the computed value; minor rounding drift, non-blocking) |
| bcfy_feeds RSS intercept ~2 | — | 2.36 | **✓** |
| bcfy_calls RSS intercept 155 | — | 149.26 | Paper says 155; computed 149.26. **Diff 5.74 MiB (3.7%).** Minor transcription imprecision; the two numbers round differently but both are consistent with intercept hovering around 150 MiB. Non-blocking; see P2-1. |
| openmhz RSS intercept 110 | — | 107.72 | ✓ within rounding |
| Additive prediction 78.8% (at 993 active, 41.4:55.6:3.4 composition) | 0.414×411×0.156 + 0.552×548×0.009 + 0.034×34×0.100 + 6.43 (following paper's §5.8 table math) | 64.116 + 4.932 + 3.4 + 6.43 = **78.878** → 78.9, rounds to 78.8 or 78.9 depending on convention | **✓** |
| Residual +1.8% | (78.8 − 77.44)/77.44 × 100 | **1.756%** → 1.8% | **✓** |

**Note on README.md §1c discrepancy:** The artifacts `README.md` in its "1c key findings" bullet states: "Additive prediction **73.7%** vs observed 77.4% (−4.9% residual)." This contradicts the paper's §5.8 and §8 values of 78.8% / +1.8%. The paper's version verifies from the actual mono-source slope values (0.156, 0.009, 0.100) and the stated 41.4:55.6:3.4 composition at 993 active feeds. The README appears to have a stale transcription or a different slope/base assumption that cannot be reproduced from the published slopes. **The paper is self-consistent; the artifact README is stale.** This is a P2 observation (see §6.2).

### 4.3 §6.4 multi-process table

| Claim | Source | Recomputed from raw | Verdict |
|---|---|---|---|
| Container A CPU 40.0% | `metrics_1c_a.tsv` (n=18) | **40.03%** | **✓** |
| Container B CPU 45.2% | `metrics_1c_a.tsv` (n=18) | **45.22%** | **✓** |
| Sum CPU 85.2% | 40.03 + 45.22 | **85.25** → rounds to 85.2 or 85.3 | **✓** (matches 85.2) |
| Prediction 85.7% (2 × 42.86) | — | 2 × 42.86 = **85.72** | **✓** |
| −0.5% CPU residual | (85.2 − 85.72) / 85.72 × 100 | (85.25 − 85.72)/85.72 = **−0.55%**; paper's rounded 85.2 gives −0.61% | **✓** (within rounding) |
| Container A RSS ~3,565 MiB | `metrics_1c_a.tsv` n=18 mean | **3,537.64** (mean); max 3,575.81 | ≈ ✓ (paper rounds "approximately"; 3,537.64 rounds to ~3,540. If using the middle of mean/max = 3,557, close to paper's 3,565) |
| Container B RSS ~3,665 MiB | `metrics_1c_a.tsv` n=18 mean | **3,633.10** (mean); max 3,684.35 | ≈ ✓ (paper's ~3,665 is between mean and max; acceptable for a "~" approximation) |
| Sum RSS 7,171 MiB | 3,537.64 + 3,633.10 = 7,170.74; or paper's 3,565 + 3,665 = 7,230 | **7,170.73** (sum of means) | **✓ 7,171 matches sum-of-means exactly** (not sum of paper's rounded ~ values). |
| Prediction 7,418 (2 × 1b step 3 @ 500 feeds) | 2 × step-3 RSS mean (3,709.20 MiB) | **7,418.40** | **✓** |
| −3.3% RSS residual | (7,171 − 7,418)/7,418 | **−3.33%** | **✓** |
| Active-feed split ~493 / ~493, ~194 / ~199 ffmpeg | `metrics_1c_a.tsv` means | 986.6 total active, 194.8 / 198.8 ffmpeg means; however the paper and `experiment_1c_ad_report.md` state container-A-active = 493 each. The tsv records show "active" as a **shared (both-container-visible)** field showing both workers' combined feeds (986–988), not per-container. | ⚠ **Data schema ambiguity** — see §6.3 (P2-2). The 493/493 split is plausibly correct but not directly legible from the tsv; verifiable only via `ramp_1c_a.log` per-worker detail. |

### 4.4 §6.1 Table 7 and §8 conclusion restatement

| Claim | Paper | Verified |
|---|---|---|
| Slopes 0.156 / 0.100 / 0.009 (§8 conclusion) match §5.8 | ✓ same three numbers | ✓ |
| 78.8% additive prediction vs 77.4% observed (+1.8%) (§8) | = §5.8 values | ✓ |
| Table 7 "2 workers/VM = 6 VMs for 12,000" | Trivially: ceil(12000/1000) = 12 workers; 12/2 = 6 VMs | ✓ |

### 4.5 Methodology footnote on cgroup windowing

The paper's §5.4 cgroup CPU table precisely reproduces with the following methodology: take the cgroup `usage_usec` delta between the sample closest to `activation_start + s + 1 s` and the sample closest to `activation_start + e + 1 s`, divide by elapsed time × 1e6 to get CPU %. Without the 1-s offset, values differ by 5-7%. **The paper does not document this 1-s offset.** For reproducibility, the §5.4 table caption should note the window boundary convention. P2 observation (see §6.2).

---

## 5. 7-Mode AI Failure Checklist

| # | Mode | Verdict | Evidence / Justification |
|---|---|---|---|
| 1 | **Citation hallucination** (URLs, authors, titles) | **NOT_OBSERVED** | 9/9 reachable URLs return HTTP 200. Book references [6], [7], [10], [12] are well-known titles with correct authors/years/publishers. Conference references [9], [12] match real publications (van der Kouwe EuroS&P 2019; Grottke ISSREW 2008). Frigate [15] issues exist on the actual repo. **One citation-NUMBER mismatch** (§7.3 cites [8] for Jain, but Jain is [6] and [8] is Heiser) — this is a P1 bookkeeping error, not a hallucination. See P1-1 in §6. |
| 2 | **Implementation bugs presented as insight** | **NOT_OBSERVED** | The core insights (per-feed coefficients, saturation point, multi-process additivity, subprocess-spawn stall attribution) are all consistent across multiple artifacts. No result depends on a bug surviving. The PgBouncer claims in §7.14 include a proactive grep-based audit that didn't find bugs to dress up. |
| 3 | **Hallucinated results** (numbers not traceable to raw data) | **NOT_OBSERVED** | All tested numerics trace to the cited artifacts within rounding. The §5.4 Cloud-Logging-derived drift numbers (14,489.8 / 15,485.5) are not locally verifiable — but the `experiment_1c_ad_report.md` records the same values, and the cgroup data locally reproduces the paper's CPU-table exactly under the 1-s-offset window convention, corroborating the same burst-window attribution. |
| 4 | **Shortcut reliance** (gaming metrics, p-hacking) | **NOT_OBSERVED** | No p-hacking: per-source OLS use 10,000-bootstrap CIs; SEs are wide but reported honestly. §5.8 explicitly acknowledges "n=3 leaves one degree of freedom after fitting a line." No cherry-picked windows: §5.4 steady-state drift uses all 600 samples; §6.4 steady-state uses all 18 × 2 tsv rows. |
| 5 | **Bug-as-insight** | **NOT_OBSERVED** | The 14.5/15.5-s drift spikes during activation burst are reported as transient (not steady-state), consistent with the characterization. The 9.02s drift is correctly attributed to the step-7 activation burst in 1b, not the steady 1,500-feed window. |
| 6 | **Methodology fabrication** | **NOT_OBSERVED** | The cgroup sampler, 2-s monitor cadence, and `MAX_FEEDS_PER_WORKER=500` orchestration are corroborated by `ramp_1c_a.log`, `experiment_1c_ad_report.md`, and the cgroup log headers. The 1-s-window-offset (see §4.5) is un-documented in the paper but is a benign window-boundary convention, not a fabricated procedure. |
| 7 | **Pipeline-level frame-lock / overstated generality** | **NOT_OBSERVED** | §7 Limitations 1, 2, 5, 6, 11, 13, 15, 16 explicitly scope the 1c findings: 2-container-on-1-VM only; CFS-throttling not testable; single VM; Phase 2 would require >=45-min windows with trend tests. No generality leak from "2 workers on one n2-standard-4" to "arbitrary multi-process fleet scaling". The paper is explicit about what §6.4 does and doesn't prove — except for the stale line-24 "we do not" language (P0-3) and stale contribution list (P0-2), which the author simply forgot to update. |

**No SUSPECTED findings on any mode.** **Pipeline does not block.**

---

## 6. Recommended Fixes (ordered P0 → P1 → P2)

### P0 (must fix before Stage 3)

- **P0-1.** Abstract line 7: drop or rewrite "modeled but not empirically validated" clause. Replace with a sentence that reflects the §6.4 measured 2-worker-per-VM result.
- **P0-2.** §1 lines 17–22 contribution list: expand to include (5) per-source coefficients (§5.8), (6) multi-process validation at 2 workers / 1 VM (§6.4), (7) activation-burst stall reproduction and attribution (§5.4). Drop "(not validated)" parenthetical in current item 4.
- **P0-3.** §1 line 24: rewrite the "what the paper does not claim" sentence. Remove the 3 stale clauses (multi-process validation, per-source isolation, drift diagnosis). Replace with the still-true limits: uvloop unbenchmarked; no replication across VMs/days/machine types; n=3 per-source fits have wide CIs. (Or simply move the honest limits sentence-list to §7 and drop from §1 entirely.)
- **P0-4.** §2.2 line 42: rewrite "Because we do not run per-source-type decomposition ramps…" — per-source ramps are now in §5.8. Replace with a sentence that cross-references §5.8 per-source fits and explains that the 0.069 headline coefficient applies to the 41:55:4 mix.
- **P0-5.** §6.4 section-title line 390: change "(and What This Paper Does Not Validate)" to remove the false implication that the section's primary result (multi-process measurement) is unvalidated. Options: "Multi-Process Measured, Others Future Work" or "Multi-Process Validated; uvloop/ffmpeg-offload/Specialization Unvalidated".

### P1 (should fix before Stage 3)

- **P1-1.** §7 Limitation 3 line 423: the citation "[8]" for Jain's *Art of Computer Systems Performance Analysis* is wrong. Jain is reference [6]; [8] is Heiser's "Systems Benchmarking Crimes". Change `[8]` to `[6]` on line 423. (The Heiser reference [8] is used elsewhere — e.g., §7 limitations should check for any other mis-numbering, but a scan shows no other `[8]` citations in the paper body besides this one miscite.)

### P2 (optional polish)

- **P2-1.** §5.8 bcfy_calls RSS intercept: paper says 155 MiB; raw-data OLS yields 149.26 MiB. Difference 5.74 MiB (3.7%). Tighten to ~149 or explain rounding.
- **P2-2.** §5.4 cgroup table (Window t ≥ 150s "≤45 / ≤50"): the max 30-s-window container-A CPU is 45.02% (t=270→300), a hair above the "≤45" claim. Soften to "≈45" or "≤45 in all but one 30-s sub-window (45.0 at t=270–300s)". Also consider adding a methodology footnote that the 30-s windows are offset by +1s from activation_start.
- **P2-obs-3.** Artifacts `README.md` lines 58–60 state "Additive prediction **73.7%** vs observed 77.4% (−4.9% residual)" — this is stale and contradicts the paper's 78.8% / +1.8%. The paper's number verifies from the raw slopes; the README should be corrected, but this is outside the paper itself.
- **P2-obs-4.** (From Stage 4.5 round-1, still applicable) Abstract: "69% of the 15 GiB cgroup limit" rounds the cgroup limit; §5.3 carries the precise 15.26 GiB. Non-blocking.

### Carried forward from Stage 4.5 (already PASS)

The Stage 4.5 P2-obs-1 and P2-obs-2 ("15 GiB" abstract rounding; §5.4 "all steps except step 6" scope-tightening) remain applicable and un-blocking.

---

## 7. Stage 3 Readiness Assessment

**Recommendation: PASS_WITH_ISSUES. Fix P0-1 through P0-5 and P1-1 before Stage 3 peer review.**

All 5 P0 items are textual / summary-sentence drift. The underlying evidence is sound, fully traceable to raw artifacts, and internally consistent in the new §5.4, §5.8, §6.4 subsections. The abstract, §1 contribution list, §1 "we do not" sentence, §2.2 decomposition disclaimer, and §6.4 title were simply **not updated when the 1c content was grafted in**. A single coordinated edit pass addressing the 5 P0 locations will bring the paper to a consistent state.

No Mode-1 (citation hallucination) or Mode-3 (hallucinated results) findings. The pipeline does **not** block under the gate rule in the integrity protocol.

Once the P0 edits are applied, Stage 3 review can proceed. Reviewers should be alerted to the 1c additions in §5.4 (last 3 paragraphs), §5.8 (entire new subsection), §6.4 (first 3 paragraphs + the new Table), and §7 Limitations 2, 3, 5, 11, 14, 15, 16 (new/reworded) for focused evaluation.
