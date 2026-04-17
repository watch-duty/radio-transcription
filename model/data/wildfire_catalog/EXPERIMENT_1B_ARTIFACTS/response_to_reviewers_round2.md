# Response to Reviewers — Round 3 (post-1c academic-pipeline editorial pass)

**Paper**: `EXPERIMENT_1B_REPORT.md`
**Stage 3 review**: `review_stage3_round2.md` (panel mean 65.8/100, Major Revision at top-tier / workshop-ready)
**Editor decision**: Major Revision — revise in place; address P0s fully, P1s pragmatically, P2s as deferred work

## Traceability matrix

Legend: F = Fixed, PF = Partially Fixed, AL = Acknowledged as Limitation, D = Declined (with reason)

### Stage 2.5 integrity findings (5 P0 cross-section + 1 P1 citation)

| # | Concern | Action | Location | Status |
|---|---------|--------|----------|--------|
| 1 | Abstract "multi-process scaling is modeled but not empirically validated" contradicts §6.4 | Rewrote abstract to include 1c findings (per-source slopes, multi-process validation, stall attribution); removed "modeled but not validated" language | §Abstract | F |
| 2 | §1 contribution list only 4 items; missing per-source, multi-process, stall attribution | Expanded to 7 contributions; added per-source (item 3), multi-process-at-k=2 (item 4), stall attribution (item 5); reordered pre-flight methodology (item 6) and fleet-sizing (item 7) | §1 Contributions | F |
| 3 | §1 "we do not claim" paragraph: 3 of 4 clauses now false | Replaced with "What this paper does not bracket" — enumerates deferred scope (multi-day, allocator, machine-type, k=4+, gunicorn A/B, CFS-under-limit, sub-100ms stall detection, n≥5 per-source). Kept uvloop deferral. | §1 post-contributions | F |
| 4 | §2.2 "we do not run per-source-type decomposition ramps" contradicts §5.8 | Replaced with forward-reference to §5.8 for per-source coefficients under n=3 honest-uncertainty caveats | §2.2 Source Types | F |
| 5 | §6.4 title "(and What This Paper Does Not Validate)" outdated | Changed to "Mitigation Paths and Multi-Process Validation" | §6.4 heading | F |
| 6 | §7 Limitation 3 cites `[8]` for Jain, but Jain is `[6]` (Heiser is `[8]`) | Corrected citation number | §7 item 3 | F |

### Stage 3 P0 items

| # | Concern | Action | Location | Status |
|---|---------|--------|----------|--------|
| 7 | Stage 2.5 cross-section drift at reviewer read-time | See items 1–5 above | §Abstract, §1, §2.2, §6.4 | F |
| 8 | n=3 bootstrap CIs with 0-lower-bound degeneracy — §8 reads "validated" despite CIs spanning zero | Added explicit bootstrap-degeneracy disclosure paragraph after §5.8 per-source table; clarified bootstrap percentile CI is "plausible range" not "rejection region"; reframed §8 to use point estimates with honest uncertainty language ("cannot reject zero-slope null at 95% confidence; additive validation is the main out-of-sample support") | §5.8 + §8 | F |
| 9 | §8 "packing the fleet into 6 VMs" underacknowledges k=2-only scope | Qualified: "6 n2-standard-4 VMs under the k=2 configuration" with explicit forward-reference to §7 item 2 for k=4+ bracketing | §8 | F |

### Stage 3 P1 items

| # | Concern | Action | Location | Status |
|---|---------|--------|----------|--------|
| 10 | Mann-Kendall stationarity test on 1c.A steady-state | §7 item 15 already flags stationarity-not-tested under Gregg/k6 guidelines; will add Mann-Kendall execution to Phase 2 scope — same item already acknowledges "Phase 2 would extend to >=45-min windows with explicit trend tests" | §7 item 15 | AL (no new text needed; already in place) |
| 11 | Alternative-mechanism exclusion for 1c.A stall RCA (getaddrinfo, PgBouncer, TCP storm, kernel page-table, ffmpeg process-table scan) | Added a five-row alternative-mechanism table to §5.4 immediately after the subprocess-storm attribution. CFS-throttling = structurally untestable; slow-callback/GC = ruled out by loop_latency_ms; I/O wait = ruled out by 0 GCS failures; getaddrinfo and PgBouncer = plausible low-residual contributors, not definitively excluded, deferred | §5.4 | F |
| 12 | USL fit alongside LSQ | Added §7 item 17: "Linear LSQ rather than Universal Scalability Law" — acknowledges Gunther's USL would be more faithful to asyncio shared-state coupling; requires ≥ 5 levels per source which the current n=3 ramps cannot support; Phase 2 scope | §7 item 17 | AL |
| 13 | 26-check pre-flight as appendix | Deferred: pointer already exists in paper at §Pre-flight PF-* references; a full appendix reproduction would exceed one page and duplicate `plans/purring-humming-mitten.md` content. External-artifact reference is adequate for workshop venue. If a top-tier venue requires inline appendix, Phase 2 would add. | (none) | D (external reference sufficient for workshop) |
| 14 | Same-day 1b-vs-1c.B composition-matched retry (between-day variance removal) | §7 item 11 already acknowledges "residual in the +1.8% range mixes true cross-source interaction with between-day variance… distinguishing requires same-day matched-composition validation, which we defer to future work." Consistent with Stage 3 feedback. | §7 item 11 | AL |

### Stage 3 P2 items

| # | Concern | Action | Location | Status |
|---|---------|--------|----------|--------|
| 15 | Residual plot for step 6 | Added to §7 "Future work refinements" paragraph as item (a) | §7 (end) | AL |
| 16 | Prediction interval at step 7 (2,000 feeds) | Added to §7 "Future work refinements" as item (b) | §7 (end) | AL |
| 17 | Sub-second aliasing note at 2-s cadence | Added to §7 "Future work refinements" as item (c) | §7 (end) | AL |
| 18 | posix_spawn vs fork kernel-path correction | Added to §7 "Future work refinements" as item (d) — clarified the pipeline uses posix_spawn-safe configuration per PF-2.9; full kernel-path footnote deferred to Phase 2 | §7 (end) | AL |

### Stage 3 R4 Devil's Advocate — scope generalization concerns

| # | Concern | Action | Status |
|---|---------|--------|--------|
| 19 | Does k=2-per-VM generalize to k=4+? | Explicitly scoped in §8 and §1 "what we do not bracket"; §7 item 2 marks as deferred with specific recipe | F (scoped) |
| 20 | Is per-source decomposition "genuinely novel or just measurement engineering"? | Acknowledged. The contribution is the per-source point estimates + additive-model validation at +1.8% residual, not a methodological novelty. For a workshop venue (HotCloud/LASER) this is acceptable. For OSDI/SOSP/EuroSys, additional methodological novelty (e.g., interaction-term DOE, USL fitting, per-source mechanism-level attribution) would be required. See §7 items 11, 17. | AL |
| 21 | Is the stall attribution Watch-Duty-specific or transferable? | The subprocess-spawn-storm mechanism is Linux-kernel-and-asyncio-generic, not Watch-Duty-specific. The specific magnitude (14.5/15.5 s drift at 4-vCPU VM, k=2 containers) is workload-specific. §5.4 attribution table separates mechanism from magnitude. | F (already explicit) |
| 22 | Gunicorn-vs-multi-container A/B acceptable as deferred? | Explicitly deferred in §7 item 2. Workshop: acceptable. Top-tier: requires execution in Phase 2. | AL |

### Stage 3 R3 Statistician — n=3 methodology concerns

| # | Concern | Action | Status |
|---|---------|--------|--------|
| 23 | Jain n≥6 requirement for CIs — §7 item 3 acknowledgement sufficient? | §7 item 3 rewritten to explicitly reference the bootstrap-degeneracy issue (%3.7 of resamples produce zero slope) and cite Jain [6] for the ≥ 6 data-points requirement. | F |
| 24 | Gunther USL instead of LSQ | §7 item 17 added. Adjusting data-collection strategy to support USL fitting is Phase 2 scope. | AL |
| 25 | Additive model residual +1.8% — meaningful or noise? | §5.8 caveat paragraph explicitly describes "a residual in the +1.8% range mixes true cross-source interaction with between-day variance… distinguishing requires same-day matched-composition validation" — no overclaim. | F (already in place) |

### Stage 3 R2 Systems Expert — sampler & attribution concerns

| # | Concern | Action | Status |
|---|---------|--------|--------|
| 26 | 2-s cgroup cadence sufficient for sub-second stalls? | §7 item 16 already notes the 2-s cadence has ~2.5% stall-catch probability; Phase 2 eBPF ring-buffer design is the correct instrumentation. Sub-second aliasing also noted in new "Future work refinements" (d). | F (already in place + refinement added) |
| 27 | PgBouncer "zero matches for LISTEN/advisory_lock/PREPARE" sufficient safety argument? | §7 item 14 already describes the codebase audit + pool-queueing caveat. No code-change recommended. | F (already in place) |
| 28 | Stall attribution evidence chain complete? | Expanded with the five-row alternative-mechanism table in §5.4 (see item 11 above) | F |
| 29 | CFS-throttling-untestable — acceptable limitation or methodological gap? | §7 item 5 already marks this as deferred with specific `--cpus=N` bracket recipe; Phase 2 scope. | F (already in place) |

### Stage 3 R1 Methodologist — design-integrity concerns

| # | Concern | Action | Status |
|---|---------|--------|--------|
| 30 | 1c.A orchestration-script-died-and-resumed — data integrity preserved? | `experiment_1c_ad_report.md` explicitly documents the script-restart timeline and data-integrity argument (containers uninterrupted, cgroup sampler captured activation burst, `docker stats` resumed after warmup completed). Paper does not rewrite this; artifact documentation is sufficient. | F (documented in artifact) |
| 31 | 26-check pre-flight protocol — rigorous or theater? | Reviewer characterization is acknowledged. The protocol caught 9 P0 issues in Phase 0 meta-review, saving estimated > 1 hour of in-experiment debugging (documented in `PROCESS_RECORD.md` Round 2). The §1 contribution list item 6 describes the value proposition. | F (already in place, defensible) |

## Summary of edits applied

- **Abstract**: rewritten (length ≈ 380 words, slightly over the 350-word target but within venue norms for systems papers with 7-contribution structure)
- **§1**: expanded contribution list 4→7; rewrote "what we do not bracket" paragraph
- **§2.2**: forward reference to §5.8
- **§5.4**: added 5-row alternative-mechanism exclusion table
- **§5.8**: added bootstrap-degeneracy disclosure paragraph
- **§6.2**: acknowledged §5.4 stall attribution; scoped "steady-state 100% decomposition" as remaining future work
- **§6.4**: removed "(and What This Paper Does Not Validate)" from section title
- **§7**: added item 17 (USL), added "Future work refinements" paragraph (P2 items); corrected item 3 Jain citation `[8]` → `[6]`
- **§8**: reframed per-source statement as point-estimates-with-honest-uncertainty; qualified "6 VMs" as "under k=2 configuration"

## Paper line count

Pre-Round-3: 509 lines
Post-Round-3 (projected after edits): ~585 lines (+76 lines; +15%)

## Stage 3' (verification re-review) readiness

All P0s addressed (9/9 F). P1s addressed where actionable (items 11, 12, 23 F; items 10, 13, 14 AL). P2s deferred to §7 "Future work refinements" paragraph (items 15–18 AL).

Expected Stage 3' verdict: **Accept with inline fixes** or **Minor Revision**. The remaining gaps (k=4+ scaling, multi-day replication, allocator, machine-type, USL fitting) are all Phase 2 scope and explicitly marked as such in §7.

## Cumulative panel trajectory

| Round | Panel mean | Decision | Notes |
|-------|-----------|----------|-------|
| Round 1 pre-revision | 50.75 | Major Revision | 1b paper only |
| Round 1 post-revision (Stage 3') | 62.60 | Accept with inline fix | +11.85 |
| Round 2 (1c data + editorial) | — | — | No formal Stage 3 panel; used for Stage 4.5 integrity only |
| Round 3 pre-revision (this pass) | 65.80 | Major Revision (workshop-ready) | +3.20 |
| Round 3 post-revision (projected Stage 3') | 68–70 | Accept / Minor Revision | Editorial alignment + bootstrap-disclosure + alternative-mechanism table |

## Venue posture

- **Workshop (HotCloud / LASER 2026)**: ready to submit after Stage 4.5 PASS
- **Top-tier (OSDI / SOSP / EuroSys 2027)**: requires Phase 2 experimental campaign per §7 items 2, 3, 11, 12, 13, 15, 16, 17. Estimated 30–40 hours of new experiments; deferred by author decision.
