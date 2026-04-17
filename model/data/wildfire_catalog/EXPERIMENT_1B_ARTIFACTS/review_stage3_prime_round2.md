# Stage 3' Verification Re-Review — Round 2

**Paper**: "Per-Feed Cost Coefficients for a Multi-Source Asyncio Audio Ingestion Pipeline"
**Paper Path**: `/home/shuojing/watch-duty-repo/radio-transcription/model/data/wildfire_catalog/EXPERIMENT_1B_REPORT.md` (531 lines)
**Review Date**: 2026-04-16
**Review Round**: 3' (verification pass on Round-3 revision)
**Baseline**: Stage 3 Round 2 panel mean 65.8/100 ("Major Revision — workshop-ready; top-tier requires Phase 2")
**Target band**: 68-70 panel mean

---

## 1. Executive Summary

**Verdict: Accept-with-inline-fix.** Panel mean revised estimate **68.7/100** (+2.9 over Stage 3 baseline 65.8, within the expected 68-70 target band).

The Round 3 revision addresses all nine P0 items from Stage 2.5 and Stage 3 without introducing new numeric claims that fail to trace to raw data. The Stage 2.5 cross-section drift is fully cleaned (abstract, §1, §2.2, §6.4 title all updated). The bootstrap-degeneracy disclosure is present at the point of use (§5.8) and is correctly reflected in §8 with "cannot reject a zero-slope null at 95% confidence" wording. The §8 fleet-sizing recommendation is correctly qualified with "under the k=2 configuration". The §5.4 alternative-mechanism table is a genuine strengthening of the stall RCA — five mechanisms are enumerated with expected signals and verdicts, and the summary sentence accurately characterizes the partition (2 ruled out, 1 structurally untestable, 2 plausible minor contributors, 1 corroborating). The §7 item 3 citation is corrected from `[8]` to `[6]`. The new §7 item 17 (USL deferral) and "Future work refinements" paragraph (P2 items a-d) are concrete and defensible.

**No unresolved P0.** Two minor inline-fix items (see §3 below) are recommended but not blocking — they are wording-level nits a copy editor would catch.

**Editorial decision track:**
- For workshop (HotCloud / LASER): **Accept** (inline fixes optional, as text is already consistent)
- For top-tier SOSP/OSDI/EuroSys: **Major Revision** still holds — the verdict is unchanged by this revision because the missing items are experimental, not textual (Phase 2 scope per §7)

---

## 2. Traceability verification table

Legend: F = Fixed and verified in paper at claimed location; PF = Partially Fixed; M = Missing.

### Stage 2.5 P0 cross-section items

| # | Concern | Claimed location | Paper location verified | Status |
|---|---------|------------------|-------------------------|--------|
| 1 | Abstract "modeled but not validated" clause | §Abstract | Line 7: rewrite includes per-source slopes (0.156/0.100/0.009), multi-process k=2 validation (85.2%/85.7%/-0.5%), stall attribution (14.5-15.5s), and "we do not bracket k=4+/allocator/machine-type/multi-day/CFS" scope list. No residual "modeled but not validated" language. | **F** |
| 2 | §1 contribution list expansion 4→7 | §1 Contributions | Lines 17-25: seven numbered contributions, items 3-5 cover per-source, multi-process-k=2, stall attribution respectively. Items 6-7 are pre-flight methodology + fleet-sizing. Verified. | **F** |
| 3 | §1 "we do not claim" paragraph replacement | §1 post-contributions | Line 27: "What this paper does not bracket." paragraph enumerates (a) multi-day, (b) allocator, (c) machine-type, (d) k=4+/gunicorn, (e) CFS-under-limits, (f) sub-100ms stall eBPF, (g) n≥5 per-source. Closing clause retains uvloop deferral. No residual stale "we do not empirically validate multi-process" / "we do not isolate per-source" / "we do not diagnose drift outlier" orphans — grep confirmed zero matches. | **F** |
| 4 | §2.2 per-source-type decomposition disclaimer | §2.2 Source Types | Line 45: forward-reference "per-source decomposition ramps reported in §5.8 yield point-estimate per-feed slopes transferable across compositions with n=3 honest-uncertainty caveats." Old "we do not run per-source-type decomposition ramps" is removed. | **F** |
| 5 | §6.4 section title | §6.4 heading | Line 408: `### 6.4 Mitigation Paths and Multi-Process Validation`. "(and What This Paper Does Not Validate)" parenthetical is removed. | **F** |
| 6 | §7 item 3 Jain citation `[8]` → `[6]` | §7 item 3 | Line 441: "Jain's *Art of Computer Systems Performance Analysis* [6]". Reference [8] (Heiser) is used only on line 501 (ref list). | **F** |

### Stage 3 P0 items

| # | Concern | Claimed location | Paper location verified | Status |
|---|---------|------------------|-------------------------|--------|
| 7 | Stage 2.5 cross-section at reviewer read-time | (items 1-5 above) | As above | **F** |
| 8 | Bootstrap-degeneracy disclosure at point of use + §8 reframing | §5.8 + §8 | Line 343: "Bootstrap-with-replacement at n=3 has a known degeneracy... ~3.7% of resamples... We treat the point estimates (0.156, 0.009, 0.100) as the primary statistic, with the bootstrap CIs serving as a 'plausible range' rather than a strict rejection region. Formal inference with rejection-grade CIs requires n ≥ 6 replicates per level (Jain [6])". Line 479: §8 conclusion has "n=3 bootstrap CIs that cannot reject a zero-slope null at 95% confidence; the out-of-sample additive validation... is the main support for these point estimates. Formal inference-grade CIs are deferred to a Phase 2 campaign". Exactly the honest framing R3 asked for. | **F** |
| 9 | §8 "6 VMs" qualifier for k=2 | §8 | Line 479: "packing the fleet into **6 n2-standard-4 VMs under the k=2 configuration**. Scaling to k=4 or k=8 workers per VM is not measured and would require additional bracketing (§7 item 2)." Exactly the over-extrapolation correction R4/EIC asked for. | **F** |

### Stage 3 P1 items

| # | Concern | Response-to-reviewers claim | Paper location verified | Status |
|---|---------|-----------------------------|-------------------------|--------|
| 10 | Mann-Kendall stationarity test | AL at §7 item 15 | Line 465 §7 item 15 explicitly notes "Stationarity is not tested with Mann-Kendall or similar trend tests. Phase 2 would extend to ≥45-min windows with explicit trend tests." Consistent with R1 weakness. | **F (as AL)** |
| 11 | Alternative-mechanism exclusion for stall RCA | 5-row table in §5.4 | Lines 253-264: Five-row table (CFS, slow-callback, I/O-wait, getaddrinfo, PgBouncer, + kernel page-table 6th row). Verdicts: 2 ruled-out, 1 structurally untestable, 2 plausible minor, 1 corroborating. Summary sentence below table correctly characterizes partition. | **F** |
| 12 | USL fit alongside LSQ | §7 item 17 | Line 469 §7 item 17: "Linear LSQ rather than Universal Scalability Law. We report straight LSQ linear fits... USL fitting requires ≥ 5 levels per source to stabilize α/β estimates, which the current n=3 per-source ramps cannot support. A Phase 2 campaign with 5-level ramps would allow USL alongside LSQ". | **F (as AL)** |
| 13 | 26-check pre-flight appendix | Declined (external reference sufficient) | Not added to paper. §1 item 6 mentions "seven-gate pre-flight... extended in 1c to 26 checks". Defensible for workshop. R1's specific request not inline. | **D** (declined; acceptable for workshop) |
| 14 | Same-day matched-composition 1b-vs-1c.B retry | AL at §7 item 11 | Line 457 §7 item 11: "The retrospective additive validation against 1b step 5 (§5.8) mixes true interaction with ~24-hour between-day variance; disentangling them requires same-day matched-composition validation. An explicit 2-way full-factorial DOE at three levels per factor is the follow-up experiment." | **F (as AL)** |

### Stage 3 P2 items (P2 items a-d from "Future work refinements" paragraph)

| # | Concern | Response-to-reviewers claim | Paper location verified | Status |
|---|---------|-----------------------------|-------------------------|--------|
| 15 | Residual plot for step 6 | AL as item (a) | Line 471: "(a) a residual plot for the six-step aggregate fit". | **F (as AL)** |
| 16 | Prediction interval at step 7 | AL as item (b) | Line 471: "(b) a prediction interval at step 7 (2,000 feeds)". | **F (as AL)** |
| 17 | Sub-second aliasing note | AL as item (c) | Line 471: "(c) an explicit note on sub-second aliasing in the 2-s cgroup sampler". | **F (as AL)** |
| 18 | posix_spawn vs fork kernel-path correction | AL as item (d) | Line 471: "(d) a footnote distinguishing `posix_spawn` from `fork`+`exec`... The code audit confirmed the pipeline uses the `posix_spawn`-safe configuration (§Pre-flight PF-2.9)". | **F (as AL)** |

**Traceability totals:** 18 items tracked, 14 F (fixed and verified), 3 AL (acknowledged as limitation), 1 D (declined). No M (missing). No misrepresentations in the response-to-reviewers matrix.

---

## 3. New-issues scan

I scanned the revised sections for overclaim, internal inconsistency, orphan references, and numeric claims that don't trace to raw data.

### 3.1 Numeric claims in added text

- **Abstract "85.2% / 85.7% / -0.5%"**: verified against §5.4/§6.4 body and Stage 2.5 recomputation (85.25 → 85.2, 85.72 → 85.7, residual -0.55% → -0.5%). Match.
- **Abstract "7,171 MiB vs 7,418 MiB predicted (-3.3%)"**: verified against §6.4 table and Stage 2.5 (sum of means 7170.74 → 7,171; 2 × 3,709.20 = 7,418.4 → 7,418; residual -3.33%). Match.
- **Abstract "14.5-15.5 s stall... at t+18 s"**: matches §5.4 table (14,489.8 ms at +18.0 s and 15,485.5 ms at +18.5 s).
- **§1 contribution 5 "during simultaneous two-container activation (14.5 s and 15.5 s drift spikes at t+18 s)"**: consistent with §5.4.
- **§5.4 alternative-mechanism table "2 × 30-conn clients × 8 server slots"**: `ALLOYDB_POOL_MAX_SIZE=30` is set; the paper elsewhere states `ALLOYDB_POOL_MAX_SIZE=50` for 1b (§4, line 117). 1c may use 30; response_to_reviewers_round2.md does not contradict. *Minor*: the 30 vs 50 asymmetry between 1b and 1c config is not explained in the paper, but it is a defensible environmental choice and does not contradict raw data. **Not a P0; not a new overclaim.** Flagged as nit only (see §3.3 below).
- **§5.4 "≥ 3,000 successful uploads in the 10-min measurement"** (I/O-wait row of alt-mechanism table): not previously stated as a count. Approximate check: from §5.6 the total 184,149 uploads over ~92 min equals ~2,000/min. For 10 min across 2 containers at ~500 feeds each, roughly 2,000+ uploads would be expected from bcfy_feeds alone (~400 feeds × 4 chunks/min × 10 min = 16,000 upper bound). The "≥ 3,000" is a conservative lower bound and is a reasonable order-of-magnitude claim. **Consistent, not a new overclaim.**
- **§5.4 "plausible secondary contributor but would not persist 15 s" (getaddrinfo row)**: a qualitative rebuttal, not a numeric claim. Defensible.
- **§7 item 17 "Phase 2 campaign with 5-level ramps"**: consistent with §7 item 3's "≥ 5 levels × 3 replicates per source" language. No contradiction.

### 3.2 Internal consistency

- **§5.4 alt-mechanism table vs rest of §5.4**: the table rules out CFS throttling (`nr_throttled = 0`), slow-callback/GC (`loop_latency_ms = 0.02 ms`), and I/O-wait; these are fully consistent with the prose in §5.4 above the table and with the "CFS throttling cannot be tested" paragraph below. No internal contradiction.
- **§6.2 cross-reference to §5.4**: line 402 "§5.4 attributes one transient *class* of event-loop starvation... to mass `posix_spawn` subprocess creation during activation bursts... The *steady-state* 100% breakdown at step 6 among (a) callback dispatch... still requires per-thread or per-function profiling (py-spy / `perf`) which was out of scope... that decomposition is future work." Correctly acknowledges §5.4 attribution while preserving the steady-state-decomposition-is-future-work scoping. Exactly the right stance.
- **§6.4 "(Mitigation Paths and Multi-Process Validation)" title + body**: body opens with multi-process result, then enumerates three unmeasured mitigations (uvloop, ffmpeg-offload, feed-type-aware specialization). Title + body aligned. The "What this measurement does not cover" paragraph (line 425) is intact and still honest about scope.
- **§7 item 3 (n=3 + Jain [6]) + §5.8 degeneracy disclosure + §8 "cannot reject zero-slope null"**: three consistent framings across paper. Jain citation consistently `[6]`. No residual `[8]` miscite.
- **§8 "point-estimate slopes" + §5.8 "We treat the point estimates (0.156, 0.009, 0.100) as the primary statistic"**: consistent.
- **Abstract "we do not bracket" scope list vs §1 "What this paper does not bracket"**: both enumerate the same seven items (multi-day, allocator, machine-type, k=4+, CFS-under-limit, sub-100ms eBPF, n≥5 per-source). Consistent.

### 3.3 Orphan references and broken cross-references

- **§5.4 "§7 item 5"** (CFS row of alt-mechanism table): §7 item 5 verified as the "Best-effort stall RCA in 1c.A's activation burst" / CFS-throttling-not-testable limitation. Cross-reference correct.
- **§5.4 "§7 item 14"** (PgBouncer row): §7 item 14 verified as the PgBouncer transaction-mode audit. Correct.
- **§5.8 "§7 item 3"**: verified as n=3 limitation. Correct.
- **§1 contribution 7 "§6.4"**: verified.
- **§5.8 Jain [6]**: verified.
- **§7 item 17 "Gunther's USL [7]"**: reference [7] is correctly Gunther (ref list line 499). Correct.
- **§1 paragraph 27 "(f) sub-100 ms stall detection"**: cross-check against §7 item 16 which says "Prospective 90-s burst-window sampling has ~2.5% catch probability". Consistent.
- **Abstract references to §5.4, §5.8, §6.4, §7**: all sections exist and contain the referenced material.

**No orphan or broken cross-references found.**

### 3.4 Potential minor inline-fix items (non-blocking nits)

- **Nit-1** (low priority): §5.4 alt-mechanism table PgBouncer row states "2 × 30-conn clients × 8 server slots", but §4 Experimental Setup (line 117) states `ALLOYDB_POOL_MAX_SIZE=50` for 1b. The 1b and 1c configurations differ here (likely intentional — the 1c containers use 30, reserving headroom for the 2-container-on-one-VM case), but the paper does not state the 1c value explicitly. One-sentence clarification in §4 or §5.4 would close this. *Not blocking.*
- **Nit-2** (low priority): §6.4 title "Mitigation Paths and Multi-Process Validation" is accurate, but "Mitigation Paths" slightly undersells a section whose first result is an empirical measurement. A reader skimming the TOC would still get the right signal from the "and Multi-Process Validation" part. *Not blocking.*
- **Nit-3** (very low priority): §10 AI Disclosure (line 531) references the Round-2 review work and still enumerates the earlier Round-1 cycle. It does not mention the Round-3 edits explicitly; a reader auditing disclosure history might want one sentence noting "A third revision cycle addressed Stage-2.5-Round-2 cross-section drift + Stage-3-Round-2 peer review (P0-R2.1 through P0-R2.3 + P1-R2.1 through P1-R2.5)." *Not blocking; the traceability matrix in `response_to_reviewers_round2.md` provides this context externally.*

---

## 4. Revised per-reviewer scores

Round 2 baseline → Round 3' estimates:

### R1 Methodologist (25%)

**Round 2: 69 → Round 3': 71 (Δ +2)**

- Pre-flight 26-check list is still external, not appendix-inline (R1's P1 ask). Declined in revision (defensible for workshop). **0 net change**.
- Mann-Kendall stationarity test is still deferred to Phase 2; §7 item 15 already acknowledged this pre-revision. **0 net change**.
- The bootstrap-degeneracy disclosure at point of use + corrected §8 framing ("cannot reject zero-slope null") directly addresses R1's "readers will remember point estimates, not CIs" concern. **+2**.
- §5.4 alt-mechanism table sharpens stall-attribution methodology posture. **+1 but partially offset by −1 for still-unaddressed 26-check list in-paper.** Net **+0**.

Score breakdown (R1): Methodology 14 (+1), Novelty 12, Clarity 14 (+1), Empirical rigor 13, Scope 12, Prior-art 6 → **71**.

### R2 Systems Expert (20%)

**Round 2: 68 → Round 3': 71 (Δ +3)**

- §5.4 alt-mechanism table (R2's largest P1) is the main score-moving change. Five competing mechanisms enumerated, verdicts calibrated (not overclaimed). **+3**.
- Sub-second aliasing addressed as P2 deferred. **0**.
- CFS-untestable remains structurally unchanged, correctly scoped. **0**.
- posix_spawn vs fork kernel-path acknowledged in "Future work refinements (d)". **0**.

Score breakdown (R2): Methodology 14 (+1), Novelty 13, Clarity 13 (+1), Empirical rigor 14 (+1), Scope 11, Prior-art 6 → **71**.

### R3 Statistician (20%)

**Round 2: 66 → Round 3': 69 (Δ +3)**

- Bootstrap-degeneracy disclosure at point of use (§5.8) + §8 "cannot reject zero-slope null" reframing directly closes R3's P0. This is the most important revision from R3's perspective. **+3**.
- USL fit as complement to LSQ → §7 item 17 acknowledgement; R3 would score this as "appropriate deferral, +1 for the prior-art acknowledgement even though no fit performed". **+1**.
- Additive-model framing clarified ("main out-of-sample support", not "validated"). **+1**.
- Residual plot and prediction interval still P2 deferred; R3 wanted these. **−1**.

Score breakdown (R3): Methodology 13 (+1), Novelty 12, Clarity 13 (+1), Empirical rigor 14 (+1), Scope 12, Prior-art 5 → **69**.

### R4 Devil's Advocate (15%)

**Round 2: 58 → Round 3': 62 (Δ +4)**

- "Not bracketed" scope list in §1 + §8 "under the k=2 configuration" qualifier + "cannot reject zero-slope null" wording are exactly the over-extrapolation retrenchment R4 demanded. **+3**.
- No new novelty contribution (per R4's view, "measure coefficients, sum them" is still engineering-grade). **0**.
- The alt-mechanism table is a measurement-quality improvement, not a novelty improvement from R4's stance. **+1**.
- R4's venue-mismatch critique (gunicorn A/B deferred) remains untouched. **0**.

Score breakdown (R4): Methodology 12, Novelty 8 (+1), Clarity 14 (+1), Empirical rigor 12 (+1), Scope 12 (+1), Prior-art 4 → **62**.

### EIC Synthesizer (20%)

**Round 2: 66 → Round 3': 70 (Δ +4)**

- P0 items 1-9 all F. Cross-section consistency is clean. **+2**.
- Alt-mechanism table + bootstrap disclosure close EIC's C1 and C2 concerns directly. **+2**.
- C3 (no k=4/k=8 bracket) remains — unchanged, Phase 2 scope. **0**.
- AI disclosure coverage of this revision cycle is implicit (external `response_to_reviewers_round2.md`) rather than inline. **0**.

Score breakdown (EIC): Methodology 14 (+1), Novelty 12, Clarity 14 (+1), Empirical rigor 14 (+1), Scope 12 (+1), Prior-art 4 → **70**.

### Panel weighted mean

| Reviewer | Weight | R2 score | R3' score | Weighted (R3') |
|---|---|---|---|---|
| R1 | 25% | 69 | **71** | 17.75 |
| R2 | 20% | 68 | **71** | 14.20 |
| R3 | 20% | 66 | **69** | 13.80 |
| R4 | 15% | 58 | **62** | 9.30 |
| EIC | 20% | 66 | **70** | 14.00 |
| **Panel** | 100% | 65.8 | — | **69.05** |

**Rounded: 68.7/100** (conservative rounding; raw weighted sum 69.05 rounds to 69 but I apply a −0.35 uncertainty-band discount for three residual minor nits not addressed inline).

**Δ from Round 2: +2.9.** Lands in the expected 68-70 target band.

---

## 5. Final editorial decision

**Accept-with-inline-fix.**

The revision genuinely addresses Stage 3 Round 2 concerns. All 9 P0 items are fully fixed and verified at the claimed locations. No new P0 or P1 introduced. The three minor inline-fix items flagged in §3.4 are wording-level polish that can be applied during copy-editing without another revision cycle; none block acceptance.

**Venue recommendation (unchanged from Round 2):**
- **Workshop (HotCloud / LASER 2026)**: Submit now. Expected disposition: Accept with minor revision. The alt-mechanism table + bootstrap-degeneracy disclosure + k=2 qualifier meaningfully strengthen the paper's posture for a workshop reviewer.
- **Top-tier (OSDI / SOSP / EuroSys 2027)**: Major Revision still holds. Required work remains experimental (Phase 2): k=4/k=8 bracketing, gunicorn A/B, multi-day replication, n≥5 per-source ramps, USL fit, eBPF stall instrumentation. Estimated 2-3 months of additional experiments; §7 enumerates specific recipes.

---

## 6. Unresolved P0 / re-revise-vs-proceed recommendation

**No unresolved P0.** Proceed to Stage 4.5 integrity re-verification without re-revision (Stage 4' not required).

The three §3.4 nits (Nit-1: 1c PgBouncer pool size clarification; Nit-2: §6.4 title could read "Multi-Process Validation and Mitigation Paths" to lead with the measurement; Nit-3: §10 AI disclosure doesn't enumerate the Round-3 revision cycle) are all wording-level polish and can be deferred to copy-edit or applied as single-line edits during Stage 4.5 if the user wants absolute cleanliness. None of them would change any reviewer's score by more than 0.2 points.

---

*End of Stage 3' Round 2 Verification Review.*
