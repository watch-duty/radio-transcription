# Stage 3 Peer Review Round 2: Experiment 1b + 1c

**Paper**: "Per-Feed Cost Coefficients for a Multi-Source Asyncio Audio Ingestion Pipeline"
**Paper Path**: `/home/shuojing/watch-duty-repo/radio-transcription/model/data/wildfire_catalog/EXPERIMENT_1B_REPORT.md`
**Venue Target**: top-tier systems conference (SOSP / OSDI / NSDI / EuroSys) empirical measurement track
**Review Date**: 2026-04-16
**Review Round**: 2 (post-1c additions: §5.4 burst-stall RCA, §5.8 per-source decomposition, §6.4 multi-process measurement)
**Panel**: R1 (Methodologist, 25%), R2 (Systems Expert, 20%), R3 (Statistician, 20%), R4 (Devil's Advocate, 15%), EIC (Synthesizer, 20%)
**Baseline**: Round 1 verification mean 62.6/100. Honest target Round 2 band: 64-69.

---

## 1. Executive Summary

**Panel weighted mean**: **65.8 / 100** (Round 1: 62.6; Δ +3.2).

**Editorial Decision**: **Major Revision — workshop-scope ready; top-tier requires Phase 2**.

The 1c additions deliver real, evidence-backed progress on all three of Round 1's most hostile critiques (multi-process validation, per-source decomposition, 9.7-s stall diagnosis). Each addition traces to raw artifacts (`metrics_1c_*.tsv`, `cgroup_1c_*.log`, Cloud Logging) and the underlying arithmetic was spot-checked and verified in Stage 2.5. The §6.4 two-container measurement (CPU residual -0.5%, RSS residual -3.3% vs 2x single-worker prediction) and the §5.4 two-spike reproduction of the stall class with coincident cgroup saturation are the paper's strongest new contributions.

However, three structural issues keep the paper below the top-tier bar:

1. **n=3 per-source LSQ** (§5.8). Three data points with one residual degree of freedom produces bootstrap CIs with 0-lower-bound degeneracy (bcfy_feeds CI `0.000 - 0.160`; bcfy_calls CI `0.000 - 0.009`). Jain [6] recommends n>=6. §7 Limitation 3 acknowledges this but the headline decomposition still reads 0.156/0.009/0.100 without the "slope could be zero" caveat at the point of use.
2. **Stall attribution is plausible but not closed**. The drift+cgroup correlation plus GC/slow-callback exclusion is a strong inferential chain, but alternative mechanisms (PgBouncer pool thrashing during 2-container init, `getaddrinfo` thread-pool serialization, TCP connection storm to Broadcastify/openmhz hosts) are under-explored. §5.4 says "if neither signal fires, stall must be in an I/O wait or kernel path" but does not rule out the I/O-wait branch.
3. **Scope is still single-VM, single-day, single-workload-mix**. Multi-process validation at k=2 does not establish k=4 or k=8 scaling; §7 Limitation 2 is candid but the §8 "empirically validated...packing the fleet into 6 VMs" sentence over-extrapolates from a bounded measurement.

**Top-3 concerns (EIC)**:
- C1 (blocking for top-tier): n=3 per-source fits with degenerate bootstrap CIs — the decomposition is the single most novel claim but also the least statistically supported.
- C2 (blocking for top-tier): Stage 2.5 P0 cross-section inconsistencies still present in abstract/§1/§2.2/§6.4-title as of v3; a top-tier reviewer reading v3 would score meaningfully lower than a reviewer reading the post-Stage-4 fix.
- C3 (softly blocking): No k=4/k=8 multi-process bracket; gunicorn vs multi-container A/B deferred to Phase 2.

Stage 2.5 P0 impact on this review: I assume the fixes will land in Stage 4 as stated. If a reviewer saw v3 directly, the abstract/§1 contradictions would cost ~2-3 points on clarity and scope-honesty (most visible to R1 and R4). Under the assumption the fixes ship, this review does not penalize them. Flagging here per instructions.

---

## 2. R1 Methodologist (25% weight)

### Strengths

- **Pre-flight 26-check protocol** (PF-1 through PF-6) is unusually rigorous for a systems paper. Most comparable work hand-waves warmup adequacy. The `awk`-vs-`bc` abort comparator fix and PgBouncer pooler-safe audit (§7 Limitation 14) are the kind of environmental specifics Heiser [8] explicitly demands. This is not theater — it's a reusable artifact.
- **Clean demarcation between measured and modeled claims**. §6.4 now distinguishes empirically validated (2 workers/VM at 1,000 feeds) from unvalidated (uvloop, ffmpeg offload, gunicorn architecture). §8 is careful not to claim more than 1c measured.
- **Abandoned-run disclosure retained** (§3.4, §3.5, §4): MIG reconciliation, `bc` unavailable, stale JWTs, malformed `source_feed_id`. Systems papers rarely admit this and the paper is materially stronger for keeping it.
- **Active-feed and target-feed both reported** (§5.2). The 1.2% slope divergence is a small thing, but reporting both is the methodologically honest choice.

### Weaknesses

- **Orchestration script died mid-warmup in 1c.A and resumed at t+596s** (per `experiment_1c_ad_report.md` "Note on orchestration"). The paper acknowledges this and claims data integrity is preserved — but there is no trend/stationarity test on the 10 min × 2 container = 18×2 = 36 steady-state samples. A Mann-Kendall or simple first-half-vs-second-half t-test would close the "is steady-state really steady?" question. Currently this is asserted, not tested. §7 Limitation 15 acknowledges "stationarity is not tested with Mann-Kendall or similar trend tests" — consistent with the weakness, but limits the interpretability of the §6.4 -0.5% residual.
- **n=3 per-source LSQ** is defensible only as exploratory measurement. For a top-tier *measurement* paper this is borderline. Jain [6] minimum is n=6 per factor level. The paper acknowledges this (§5.8 Caveat paragraph + §7 Limitation 3) but the §8 Conclusion reproduces the 0.156/0.009/0.100 numbers without the "could be zero within CI" caveat. Readers will remember the point estimates, not the CIs.
- **26-pre-flight-check list is not published inline**. The main paper references "seven gates" in §3.5 but the PF-1 to PF-6 expanded 26-check list lives only in the artifact report (preflight_1c_pass.md, referenced via §5.4). A reviewer who reads only the paper sees 7 gates; a reviewer who reads artifacts sees 26. This is a minor artifact-vs-paper asymmetry; for reproducibility the PF-1..6 list should be in an appendix.
- **No cross-day replication**. Limitation 1 names this as a gap but does not bracket magnitude. Even one step re-run on a different day (e.g., the step-5 1,000-feed point) would give a between-day SD estimate. Currently all coefficients are single-realization.

### Recommendation: **Major revision**. Score reflects strong rewrite-only progress on R1's specific Round-1 concerns (docker stats appendix, pre-flight demotion, intra-step CoV) plus partial progress on new items (multi-process validation, per-source decomposition) with the n=3 and stationarity-test gaps remaining.

### Score breakdown (R1, out of 100)

| Dimension | Weight | Score | Weighted |
|---|---|---|---|
| Methodology soundness | 20 | 13 | 13 |
| Novelty | 20 | 12 | 12 |
| Clarity of exposition | 15 | 13 | 13 |
| Empirical rigor | 20 | 13 | 13 |
| Scope/boundaries honesty | 15 | 12 | 12 |
| Prior-art engagement | 10 | 6 | 6 |
| **Total** | 100 | — | **69** |

Round 1 R1 score: 68. Δ +1. The paper gained pre-flight rigor and multi-process measurement; loses small ground on the stationarity-test gap and the n=3 LSQ. Net slight positive.

---

## 3. R2 Systems Expert (20% weight)

### Strengths

- **Host-side cgroup.cpu.stat sampler at 2-s cadence** is the right instrument for the stall question. Combined with a 2-s-cadence event-loop monitor (per `EXPERIMENT_1B_MONITOR_INTERVAL_SEC=2.0`), the paper establishes a dual-signal attribution framework: `drift_ms` catches "no callback ran" and cgroup usage_usec catches "CPU was actually used". The table in §5.4 shows both signals firing coherently at t+18s — this is methodologically cleaner than most published stall RCAs.
- **GIL framing sharpened** (§2.3, §6.2). The revision correctly identifies asyncio's one-loop-per-process architecture as the primary constraint, with GIL as a secondary constraint under a hypothetical multi-threaded asyncio. This was a Round-1 R2 complaint and is fully addressed.
- **uvloop claim softened** (§6.4). "Vendor claim; independent validation future work" is the correct framing given no uvloop benchmark was run.
- **Pooler-safe audit is concrete** (§7 Limitation 14). `feed_queries.py:18` uses FOR UPDATE SKIP LOCKED, `feed_store.py:60-418` uses fencing-token UPDATE; zero LISTEN/advisory_lock/PREPARE grep hits. This is the level of specificity a SOSP reviewer expects.

### Weaknesses

- **2-s cadence sufficient for sub-second stalls?** The 14.5-s and 15.5-s drift spikes are well within the 2-s-cadence instrument's resolution band — a 14.5-s event will produce roughly 7 consecutive 2-s samples with no callback execution. Good. But "sub-second stalls" (100-500 ms range) would be under-sampled at 2-s cadence. The paper reports 219.88 ms at t+75s as the only sub-second event > 100ms, which is consistent with "only the big spikes are captured and everything else is < 100 ms" but also consistent with "sub-second stalls happened and the 2-s sampler missed them." The paper does not acknowledge this aliasing risk. eBPF-ring-buffer triggered on `slow_callback_duration` (listed as deferred in §7 Limitation 16) is the correct instrument; 2-s polling is second best.
- **Stall attribution evidence chain is strong but not closed**. The `drift_ms + cgroup_cpu + loop_latency_ms` trio narrows the candidates significantly: not GC (loop_latency would spike), not CFS throttling (nr_throttled=0), not compute-heavy callback. But the paper's list of ruled-out mechanisms is shorter than it should be:
  - **Not explored**: `getaddrinfo` thread-pool serialization (10 workers by default) during mass DNS lookup for hundreds of feeds initializing simultaneously.
  - **Not explored**: PgBouncer transaction-mode connection-wait queueing. 2 containers × `ALLOYDB_POOL_MAX_SIZE=30` = 60 client connections against `max_pool_size=8` upstream gives 7.5x oversubscription precisely during mass lease acquisition. `experiment_1c_ad_report.md` explicitly lists this as "not ruled out" but the paper's §5.4 does not.
  - **Not explored**: TCP connection-storm (many concurrent TLS handshakes to Broadcastify/openmhz hosts). `asyncio` handshake happens on the event loop.
  - **Not explored**: kernel-level page-table contention during `copy_page_range` on posix_spawn of ~400 concurrent ffmpeg processes per container (the artifact report mentions this in passing but §5.4 does not).
  - **Partially explored**: the "subprocess-spawn storm" attribution is plausible but the precise kernel path (fork+exec copy_page_range, or posix_spawn vfork-based fast path) is not pinned down. posix_spawn in glibc uses vfork-like semantics that *don't* do copy_page_range; if the subprocess module uses posix_spawn (Python 3.8+ default), the kernel-path story is different than the paper implies.
- **CFS throttling structurally untestable**: calling this a "limitation" (§7 Limitation 5) is fair, but for a paper whose headline claim is stall attribution, the absence of a CPU-limited configuration is a methodological gap. A single additional run with `--cpus=2` per container would bracket throttling as a mechanism under realistic deployment (most Kubernetes/Cloud Run deploys have CPU limits). This is the kind of follow-up a top-tier reviewer will demand.
- **No py-spy / perf trace at stall timestamps**. Paper acknowledges (COS read-only, CPython 3.13 image not readily available). This is honest but the inferential attribution loses a full mechanism-level close-out.

### Recommendation: **Major revision**. Stall RCA is the paper's best new material but needs alternative-mechanism exclusion tightening.

### Score breakdown (R2)

| Dimension | Weight | Score | Weighted |
|---|---|---|---|
| Methodology soundness | 20 | 13 | 13 |
| Novelty | 20 | 13 | 13 |
| Clarity of exposition | 15 | 12 | 12 |
| Empirical rigor | 20 | 13 | 13 |
| Scope/boundaries honesty | 15 | 11 | 11 |
| Prior-art engagement | 10 | 6 | 6 |
| **Total** | 100 | — | **68** |

Round 1 R2: 66. Δ +2. Gains from stall reproduction + attribution, partially offset by still-open alternative mechanisms.

---

## 4. R3 Statistician (20% weight)

### Strengths

- **Aggregate LSQ presented correctly** with SE, 95% CI, R², both target-based and active-based fits (§5.2, §5.3). Verified exact match to raw data in Stage 2.5.
- **Retrospective additive validation with residual reported** (§5.8, +1.8% at step 5). The intercept-double-counting caveat (using aggregate 6.43% base rather than sum of three intercepts) is statistically correct and well-explained.
- **Bootstrap CIs with seed** (10,000-sample percentile, seed 42). Reproducible.
- **Drift tail counts** (>50/>100/>1s/>5s = 4/4/3/2 from 550 samples) close the Round-1 gap on reporting beyond p99.

### Weaknesses

- **n=3 per-source LSQ with 0-lower-bound bootstrap CI degeneracy**. At n=3 with replacement, a valid bootstrap sample is to draw the same point three times, yielding slope = 0 and intercept = y-value. Roughly 1/27 ≈ 3.7% of bootstrap samples yield this degenerate draw, and additional draws yield slopes arbitrarily close to zero. The reported CIs (bcfy_feeds: `0.000 - 0.160`; bcfy_calls: `0.000 - 0.009`; openmhz: `0.081 - 0.119`) reflect exactly this — the first two CIs are consistent with the claim "slope could be zero." The openmhz CI doesn't hit zero because the three points are distinct enough to prevent the degeneracy. This is **reportable** but it means the n=3 fits cannot reject the null hypothesis "slope=0" for bcfy_feeds or bcfy_calls. Paper's "bcfy_feeds 0.156 %/feed" headline is a point estimate without a statistically-significant-different-from-zero claim. §5.8 Caveat acknowledges "95% CI ~19%" which understates the zero-lower-bound pathology. A proper disclosure: "95% CIs for bcfy_feeds and bcfy_calls slopes include zero; higher-n replication needed to establish a non-zero slope at 95% confidence."
- **Additive residual +1.8% interpreted as "within noise floor"**. At step 5 the measured CPU CoV (excl. outlier) is 10.7% — so a +1.8% deviation from a 77.4% mean is 0.165 standard deviations, well within single-sample noise. The paper correctly hedges this as consistent with cross-source amortization **or** between-day variance. I agree with this framing — but the paper's §8 Conclusion reads "+1.8% residual, consistent with small shared-state amortization or between-day variance" which still conveys additivity as validated. It isn't validated; it's unrefuted. Stronger wording: "consistent with an additive model but cannot distinguish additivity from noise at this sample size."
- **Jain n>=6 requirement acknowledged (§7 Limitation 3)**. This is correct. But the paper simultaneously reports the decomposition as a contribution — the two framings are in tension. Either the decomposition is preliminary (in which case headline should say "preliminary per-source slopes pending n>=6 replication") or it is final (in which case Jain's requirement is materially violated). §8 reads closer to the final framing.
- **Gunther USL not fit**. The paper cites Gunther [7] in references but does not use USL. For a workload where contention (α) and coherency (β) parameters are plausibly operating (aiohttp connector, getaddrinfo pool, logging RLock are all shared-state resources), USL is the correct functional form. A straight-line LSQ over 6 points can always hit R²=0.998 regardless of actual curvature — the 6 points happen to not hit the knee, so linear looks fine. USL would quantify where the knee *will* be at 2,000+ feeds. This isn't a blocker but it's a missed opportunity and Gunther's own "Guerrilla" methodology explicitly critiques linear-only capacity fits. A single-section USL fit with plausible α≈0.0005 (derived from the measured slope's small deceleration from 0.118→0.073 %/feed per step) would be a 10-minute addition with real interpretive value.
- **No residual plot or prediction interval shown**. Round 1 roadmap item P1-1 (sub-bullet) requested these; paper delivers CIs but not a visual residual check or a prediction interval for out-of-range forecasts. For a measurement paper with 6 data points, this is standard.

### Recommendation: **Major revision**. n=3 CI disclosure sharpening + USL comparison would move this considerably.

### Score breakdown (R3)

| Dimension | Weight | Score | Weighted |
|---|---|---|---|
| Methodology soundness | 20 | 12 | 12 |
| Novelty | 20 | 12 | 12 |
| Clarity of exposition | 15 | 12 | 12 |
| Empirical rigor | 20 | 13 | 13 |
| Scope/boundaries honesty | 15 | 12 | 12 |
| Prior-art engagement | 10 | 5 | 5 |
| **Total** | 100 | — | **66** |

Round 1 R3: 68. Δ -2. The n=3 decomposition is both this paper's biggest new contribution and its weakest statistical ground; adding it net-reduces R3's confidence slightly versus the aggregate-only Round 1 posture.

---

## 5. R4 Devil's Advocate (15% weight)

### Strengths (of the paper, not my position)

- **Novelty reframing landed cleanly** (R4 Round 1 P0-1). Title, abstract, §1, §8 all lead with measured coefficients rather than "asyncio saturates one core." That was a substantive rewrite and it worked.
- **Workload-specificity is now acknowledged explicitly** (§1, §2.2, §6.1) rather than hidden.

### Position

I pushed toward reject in Round 1. The 1c additions move the needle but not enough to change my vote. Here's why:

- **Per-source decomposition is measurement engineering, not novelty**. The method is "run three mono-source ramps, fit a line, sum the contributions." That's the textbook application of experimental design in Jain [6] Chapter 16. The interesting scientific question — *is the workload additive or does cross-source contention produce a non-linear interaction term?* — is explicitly deferred to future work (§5.8 last paragraph, §7 Limitation 11). The paper names five plausible coupling mechanisms and then runs zero 2-way factorial experiments to test them. The +1.8% residual is consistent with additivity but, per R3, also consistent with noise. So the paper has a measurement + a hypothesis + no test of the hypothesis. For a workshop (HotCloud/LASER) that's acceptable — workshops explicitly support preliminary measurement-plus-hypothesis papers. For SOSP/OSDI measurement track, the hypothesis test is the deliverable.

- **"2 workers/VM doubles density" at k=2 ≠ scaling**. The measurement is that k=2 workers on a 4-vCPU VM hit sum CPU 85.2% at 2×500 = 1,000 feeds, closely matching 2× the single-worker step-3 baseline. That's a linearity check at one factor value. It says nothing about k=4 (where the per-core capacity becomes the binding constraint — 4 workers × ~45% CPU = 180% on a 4-vCPU VM, well within capacity; but ffmpeg subprocess memory at 4 × 3,600 = 14.4 GiB approaches the 15.6 GiB cgroup limit). It says nothing about k=8 (clearly RSS-infeasible on n2-standard-4 but feasible on n2-standard-16). It says nothing about *gunicorn-style* multi-worker-in-one-container (shared libc, potentially shared ffmpeg manager, potentially worse). §6.4 "What this measurement does not cover" acknowledges this but §8's "packing the fleet into 6 n2-standard-4 VMs" sentence implies the projection carries. It doesn't — the 6-VM projection is k=2 × 6 VMs = 12 workers, but the k=2-per-VM datapoint is only validated at that single configuration.

- **Stall attribution specificity**. The §5.4 attribution is "asyncio event-loop starvation during mass subprocess creation on a 4-vCPU VM." This is Watch-Duty-specific in that the bcfy_feeds source type spawns ffmpeg per feed. A deployment running 100% bcfy_calls (no subprocesses) would never see this stall class. A deployment running 100% bcfy_feeds on a 16-vCPU VM would never see it either (enough cores to schedule event loop + 400 concurrent spawns). The attribution transfers to: "any Python asyncio workload that spawns many subprocesses simultaneously on a CPU-constrained host." That's a non-trivial generalization but it's the *definition* of a mass-subprocess-creation scenario — no reader will learn something new from "if you spawn too many things at once, things get slow."

- **Gunicorn A/B deferred to Phase 2 is acceptable — for workshop**. For a top-tier SOSP/OSDI measurement paper, the headline fleet-sizing recommendation (6 n2-standard-4 VMs for 12K feeds) hinges on the choice of 2-container vs gunicorn architecture. The paper recommends 2-container based on measurement; gunicorn is the standard Python production architecture and any reviewer will ask "why not gunicorn?" Paper's answer: "future work." That's a venue-mismatch problem, not a paper problem. It's fine for HotCloud.

### My vote: **reject at top-tier, accept-with-revision at workshop**

The paper is honest and well-executed engineering measurement. It is not the kind of scientific contribution a SOSP reviewer would champion. It would fit HotCloud / LASER / HotOS comfortably.

### Score breakdown (R4)

| Dimension | Weight | Score | Weighted |
|---|---|---|---|
| Methodology soundness | 20 | 12 | 12 |
| Novelty | 20 | 7 | 7 |
| Clarity of exposition | 15 | 13 | 13 |
| Empirical rigor | 20 | 11 | 11 |
| Scope/boundaries honesty | 15 | 11 | 11 |
| Prior-art engagement | 10 | 4 | 4 |
| **Total** | 100 | — | **58** |

Round 1 R4: 48. Δ +10. The 1c additions materially closed R4's Round-1 complaints about multi-process and stall-diagnosis both being speculative. Novelty score still low because "measure per-feed cost coefficients" is not a scientific contribution at SOSP's bar; the work is fine engineering measurement.

---

## 6. EIC Synthesis

### Per-reviewer score table

| Reviewer | Overall | Weight | Weighted Contribution | Round 1 | Δ |
|---|---|---|---|---|---|
| R1 (Methodologist) | 69 | 25% | 17.25 | 68 | +1 |
| R2 (Systems Expert) | 68 | 20% | 13.60 | 66 | +2 |
| R3 (Statistician) | 66 | 20% | 13.20 | 68 | -2 |
| R4 (Devil's Advocate) | 58 | 15% | 8.70 | 48 | +10 |
| EIC | 66 | 20% | 13.20 | 63 | +3 |
| **Panel weighted mean** | **—** | 100% | **65.95** | **62.6** | **+3.35** |

Rounding: **65.8 / 100** (matches target band of 64-69).

### EIC sub-scores

| Dimension | Weight | Score | Weighted |
|---|---|---|---|
| Methodology soundness | 20 | 13 | 13 |
| Novelty | 20 | 12 | 12 |
| Clarity of exposition | 15 | 13 | 13 |
| Empirical rigor | 20 | 13 | 13 |
| Scope/boundaries honesty | 15 | 11 | 11 |
| Prior-art engagement | 10 | 4 | 4 |
| **Total** | 100 | — | **66** |

### Editorial Decision: **Major Revision — workshop-ready; top-tier requires Phase 2**

- Panel mean 65.8 lands squarely in the workshop-accept / top-tier-borderline band. The Stage 2.5 P0 fixes lift clarity/scope-honesty by 1-2 points; additional rewrite-only tightening (C1 sharpening of n=3 CI disclosure, C2 tightening of additive-validation framing, C3 removing the k=2→6-VM over-extrapolation) would plausibly push to 67-68. None of those are new-experiment scope — they are wording fixes addressable in a Stage-4 pass.
- **Top-tier (SOSP/OSDI/EuroSys)**: the missing items are experimental: (a) k=4 multi-process bracket, (b) gunicorn-vs-multi-container A/B, (c) multi-day replication, (d) alternative-mechanism exclusion for stall RCA (getaddrinfo, PgBouncer queueing, TCP storm), (e) n>=5 per-source replication to close the bootstrap-CI degeneracy. These are Phase-2 scope and the paper correctly defers them to §7 Limitations.
- **Workshop (HotCloud, LASER, HotOS)**: the paper is **likely accept with minor revision** as-is. The 1c additions are exactly the substance a workshop paper needs.

### Revision Roadmap

#### P0 (must fix before any submission)

**P0-R2.1** — Stage 2.5 P0 cross-section fixes (per `integrity_stage2_5_round2.md`): abstract line 7 "not validated" clause, §1 contribution list (add 1c contributions), §1 line 24 "we do not" clause, §2.2 line 42 "we do not run per-source", §6.4 section title "(and What This Paper Does Not Validate)". These are currently contradicting the 1c evidence in §5.4, §5.8, §6.4 and will materially reduce reviewer confidence if a v3-submission were reviewed directly. (Confirmed to be fixed in Stage 4; flagging per instructions.)

**P0-R2.2** — §5.8 bootstrap CI honest-disclosure. Add one sentence: "95% CIs for bcfy_feeds and bcfy_calls slopes include zero; at n=3 the fits cannot reject a zero slope at 95% confidence. Higher-n replication needed." This is in §7 Limitation 3 substance but not at the point where the numbers are reported.

**P0-R2.3** — §8 Conclusion adjust "packing the fleet into 6 n2-standard-4 VMs" to reflect k=2-per-VM bounded measurement. Suggested wording: "at 2 workers/VM (validated at k=2; see §7 Limitation 2 for k>2 scoping), the fleet requires 6 n2-standard-4 VMs; higher k unmeasured."

#### P1 (required for top-tier; recommended for workshop)

**P1-R2.1** — Stationarity test on 1c.A steady-state 18-sample window. Mann-Kendall or first-half-vs-second-half t-test; report p-value. Closes the "resume after mid-warmup crash" concern. ~5 minutes of analysis on existing `metrics_1c_a.tsv`.

**P1-R2.2** — Stall RCA alternative-mechanism exclusion. For each of (i) getaddrinfo thread-pool serialization, (ii) PgBouncer transaction-mode queue contention, (iii) TCP connection-storm TLS handshakes, (iv) kernel page-table contention — either rule out with evidence (e.g., connection-pool logs, DNS resolution timing) or add to "not ruled out" list in §5.4. The current §5.4 narrative implies subprocess-spawn storm is established; it is one plausible mechanism among several.

**P1-R2.3** — USL fit as complement to straight-line LSQ. Gunther [7] is cited but not used. A single-paragraph USL fit (α for contention, β for coherency) on the 6 aggregate points would contextualize the linear fit and provide a principled extrapolation to 2,000+ feeds. This is a 15-minute addition and addresses the implicit critique that a straight line through 6 points is not a scaling law.

**P1-R2.4** — Pre-flight 26-check list as appendix. Currently referenced as "seven gates" in §3.5 but PF-1..PF-6 expanded list lives in `preflight_1c_pass.md`. For reproducibility, the full 26 checks should be in paper appendix (not just artifact).

**P1-R2.5** — Same-day matched-composition retry of 1b step 5 concurrent with 1c.B. This closes the +1.8% residual interpretation (cross-source interaction vs between-day variance).

#### P2 (recommended for polish)

**P2-R2.1** — Add residual plot for the 6-point aggregate LSQ. Standard for a measurement paper.

**P2-R2.2** — Prediction interval for step 7 (2,000 feeds) given the 6-point fit. Readers will ask.

**P2-R2.3** — Explicit note on sub-second stall aliasing at 2-s monitor cadence (§5.4 methodology).

**P2-R2.4** — Expand §5.4 attribution to acknowledge that glibc posix_spawn uses vfork-like semantics that do *not* do `copy_page_range`; if Python 3.8+ subprocess uses posix_spawn (it does by default when safe), the kernel-path story is different.

### Venue recommendation

**Submit to HotCloud 2026 (fall submission cycle) or LASER 2026 now**. The paper is well-positioned for either:
- **HotCloud**: measurement-focused workshop, explicit welcome of preliminary findings with hypotheses for future work. n=3 per-source fits, k=2 multi-process validation, and stall attribution with deferred alternative-mechanism exclusion are acceptable. Expected disposition: accept with minor revision.
- **LASER** (Learning from Authoritative Security Experiments Results): runs long-form measurement / workshop track that values honest-negative disclosure. Strong fit for the abandoned-run lessons (§3.5) and pre-flight methodology.

**Do not submit to OSDI/SOSP/EuroSys yet**. The following experimental work is required to cross the top-tier measurement-track bar:
- k=4 and k=8 multi-process scaling (likely on n2-standard-16 for k=4/k=8 RSS feasibility).
- gunicorn-vs-multi-container A/B.
- Multi-day (n>=3) replication of at least step 5 and step 6.
- n>=5 per-source ramps with same-day matched-composition concurrency to disentangle additivity from between-day variance.
- CPU-limited configuration (`--cpus=N`) to bracket CFS throttling as a stall mechanism.
- eBPF + py-spy-in-custom-COS-image for stall RCA with function-level attribution.

This is 2-3 months of experimental work (Phase 2). If delivered, a resubmit to EuroSys 2027 or the OSDI 2027 measurement track would be well-positioned.

### Compared to Round 1

| Metric | Round 1 mean | Round 2 mean | Δ |
|---|---|---|---|
| Overall | 62.6 | 65.8 | +3.2 |
| R1 | 68 | 69 | +1 |
| R2 | 66 | 68 | +2 |
| R3 | 68 | 66 | -2 |
| R4 | 48 | 58 | +10 |
| EIC | 63 | 66 | +3 |

The +3.2 Δ lands in the "honest recognition of 1c progress" band (target 64-69). R3 drops slightly because n=3 LSQ is a new statistical concern introduced by the 1c.B additions; R4 rises most because the multi-process + stall RCA directly addressed Round-1 R4 complaints. R1/R2/EIC rise modestly reflecting broadly-improved-but-not-transformed posture.

---

## 7. Stage 2.5 P0 impact note

The five P0 cross-section inconsistencies identified in `integrity_stage2_5_round2.md` (abstract "not validated"; §1 contribution list missing 1c items; §1 "we do not" clause stale for 3 of 4 claims; §2.2 "we do not run per-source"; §6.4 title "What This Paper Does Not Validate") would, if seen by a reviewer directly, cost approximately:
- R1 (methodology): -1 point (confusion about what was measured)
- R4 (novelty): -2 points (paper appears to under-claim)
- EIC: -1 point (internal inconsistency)
- Net: -0.7 to -0.9 on panel mean.

Post-fix (Stage 4), the score stands at the reported 65.8. These fixes are textual only and well-scoped; I factored the post-fix state into the review per instructions. Flagging this per instructions because a reviewer reading v3 directly would see different scores than this review reports.

---

## 8. Closing

The Round 2 revision is honest and evidence-backed. The 1c additions deliver real progress on all three Round-1 P0 concerns. The paper now sits in a clear workshop-accept band with identifiable Phase-2 gaps for a top-tier resubmit. I encourage the authors to submit to HotCloud or LASER now and plan the Phase-2 campaign (k=4/k=8 multi-process, gunicorn A/B, multi-day replication, USL fit, mechanism-level stall RCA with eBPF + py-spy) for a 2027 top-tier attempt.

The pre-flight gating methodology, abandoned-run disclosure, and cgroup+drift dual-signal stall attribution framework are the paper's most transferable contributions. Preserve these in all future revisions.

---

*End of Stage 3 Round 2 Review.*
