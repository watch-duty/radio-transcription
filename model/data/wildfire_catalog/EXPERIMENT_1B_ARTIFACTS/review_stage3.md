# Stage 3 Peer Review: Experiment 1b

**Paper**: Experiment 1b: Single-Node Scaling Limits of an Asyncio Audio Ingestion Pipeline
**Paper Path**: `/home/shuojing/watch-duty-repo/radio-transcription/model/data/wildfire_catalog/EXPERIMENT_1B_REPORT.md`
**Venue Target**: Top-tier systems conference (SOSP / OSDI / NSDI / SIGCOMM / EuroSys) -- empirical measurement track
**Review Date**: 2026-04-16
**Review Round**: 1 (first round)
**Panel**: R1 (Methodologist), R2 (Systems Expert), R3 (Statistician/Empiricist), R4 (Devil's Advocate), EIC
**Pre-review context**: Stage 2.5 integrity check identified 5 P1 issues (to be addressed in revision); all 9 code citations verified accurate; no fabricated refs or bugs-as-insights.

---

## Reviewer 1 (R1) -- Methodologist

### Summary of the Work
The paper reports a single-node stepped ramp (100, 250, 500, 750, 1000, 1500 concurrent feeds) on one n2-standard-4 VM to characterize CPU and RSS scaling of a Python asyncio ingestion pipeline. The authors instrument the container via `docker stats`, Cloud Logging (event-loop health, GCS upload, ffmpeg exit), and a ramp controller that enforces abort thresholds. They fit linear regressions to six aggregated points per axis and claim R^2 > 0.99.

### Methodological Assessment

**Positives.** The stepped-ramp skeleton is sound in concept: warmup + measurement window per step, hard abort thresholds, explicit sampling cadence, and a documented pre-flight smoke test. The decision to abandon MIG management before the run (to avoid a prior opportunistic reconciliation that replaced the VM mid-experiment in the initial run) shows authentic operational awareness. Reliability data (167 ffmpeg exit-8 events, 898 HTTP 403s classified as per-call transient) is presented with counts and rates rather than hand-waving. The fix from `bc` to `awk` for float comparisons (Container-Optimized OS lacks `bc`) and the 30-second JWT sync cadence are the sort of environmental specifics reproducibility papers ought to include.

**Concerns.**

1. **Warmup adequacy.** Five minutes of warmup is justified by appeal rather than by data. At step 6 (1,500 feeds, ~621 bcfy_feeds), ffmpeg startup and GCS connection pool settling likely take longer than at step 1. The authors should show the intra-step time series for CPU and active-feed count and demonstrate that warmup is sufficient at each step (e.g., the last 10 minutes are stationary). Without this, step 6's 108.3% average may over- or under-estimate the steady state.

2. **No replication.** A single ramp yields six aggregated measurements. There are no cross-run error bars. Even a 2x or 3x replicate (say, three ramps on different days/VMs) would let the authors report a between-run variance and rule out single-run artifacts (GCS region load, Broadcastify source churn, cgroup noisy neighbors). At a top venue, a single ramp is a hard sell for a measurement paper.

3. **`docker stats` semantics.** The paper says 108.3% = 27.1% of VM capacity (i.e., 100% = one core on a 4-core host) but this convention is not uniform across Docker/cgroup versions and container runtimes. A brief appendix showing how `docker stats` CPU is computed (cgroup v1 vs v2, nanoCPUs, containerd vs Docker Engine) would protect the paper from readers who use a different convention.

4. **30-s sampling cadence.** Chosen "to balance granularity against `docker stats` overhead." No numbers justify this. The event-loop monitor runs at 10 s -- inconsistent with `docker stats` at 30 s. For step 6 where the event loop is stressed, 30-s `docker stats` samples may alias bursty behavior; the ramp controller fires a NOTE trigger at 02:49:12 but no discussion of how many 30-s windows fell above 100%.

5. **Per-source disentanglement.** `ffmpeg_count` only tracks bcfy_feeds; bcfy_calls and openmhz contribute to CPU/RSS only in aggregate. The authors acknowledge this (Limitation 4), but for a measurement paper the right complement is a controlled decomposition experiment: three mono-source ramps (100% bcfy_feeds, 100% bcfy_calls, 100% openmhz) to fit per-source slopes, then validate the 41:55:4 mix additively. Otherwise the linear scaling model is overconstrained.

6. **Pre-flight smoke test as contribution.** Claiming pre-flight validation as a "methodological contribution" (Section 3.5, Contribution 3) is a stretch. It's a good debugging habit, not novel methodology. Downgrade to a paragraph in §3.

### Reproducibility
Code references are file-line cited. SQL UPDATEs, environment variables, GCS bucket names, and Cloud Logging queries are shown. This is above average for systems measurement work. However, the feed catalog (2,400 specific production IDs) is not shareable, which is a practical obstacle to external replication.

### Scores (R1)
- Overall: **58/100**
- Originality: 4/10
- Technical soundness: 6/10
- Empirical rigor: 5/10
- Clarity: 8/10
- Impact: 5/10

---

## Reviewer 2 (R2) -- Systems Expert

### Summary of Technical Framing
The paper frames the asyncio ceiling as a GIL-and-single-threaded-event-loop phenomenon. It attributes the 108.3% CPU at 1,500 feeds (27% of VM) to Python bytecode serialization, and proposes uvloop, multi-process workers, ffmpeg management offload, and source-type specialization as mitigations.

### Technical Assessment

**Positives.** The architectural framing is broadly correct -- asyncio does serialize coroutine orchestration on one thread, and ffmpeg pipe reading happens on the loop thread. The memory model (7.22 MiB/feed, linear, no fragmentation within 92 minutes) is plausible. The recommendation to run 2 workers per 4-vCPU VM is operationally sensible. Reliability characterization (exit code 8 = Icecast server disconnect) is grounded.

**Concerns.**

1. **GIL framing is misleading.** The paper invokes the GIL as the reason asyncio cannot use multiple cores (§2.3, §6.2 item 1). This is technically true but conceptually sideways: asyncio uses one thread *by design*, independent of the GIL. A threaded asyncio (event loop per thread) would still be constrained by the GIL for Python-level work, but the single-loop architecture is the primary reason, not GIL. The authors should either cut the GIL discussion or sharpen it: "asyncio's single-threaded design is the architectural reason; the GIL would prevent a naive multi-threaded workaround from helping."

2. **What actually saturates the loop?** The paper asserts "ffmpeg management overhead" is the bottleneck without decomposition. Is it (a) pipe read syscalls, (b) asyncio transport callback dispatch, (c) Python-level coroutine scheduling, (d) logging formatter cost, or (e) GCS upload coroutine wake-ups? Without `py-spy`, `cProfile` on-CPU sampling, or `perf` captures during step 6, the claim that `uvloop` (libuv) would yield 2-4x remains speculation. uvloop helps when libuv's selector/transport loop is faster than the pure-Python selector loop -- but if the cost is in userland Python (logging, callbacks), uvloop won't move the needle.

3. **9.7 s drift event.** A 9.7-second event-loop stall in a 15-second chunk window is a serious incident, yet the paper offers only "near-complete loop stall" with no diagnosis. Candidate causes: (a) Python GC stop-the-world, (b) cgroup CPU throttling with a short period, (c) synchronous network stall (DNS, TCP retransmit), (d) a long-blocking syscall in a sync code path, (e) a GCS client coroutine holding the loop. The paper needs to either capture the GC log / cgroup.cpu.stat at that moment, or acknowledge the one-off event as unattributed and not fit it into the scaling narrative.

4. **uvloop claim.** "2-4x throughput for I/O-heavy workloads [3]" cites the project README. This is an unadjudicated vendor claim, not a peer-reviewed benchmark. At a top systems venue this will be flagged. Either run a uvloop sidecar experiment (even small-scale) or weaken the claim to "published benchmarks from the uvloop project; independent validation for this workload is future work."

5. **Multi-process mitigation support.** Section 6.4 asserts the lease coordination layer "already handles multi-worker scenarios" but does not demonstrate this experimentally. Two workers on the same VM may contend for: (a) the same AlloyDB connection pool per-worker-container, (b) shared network bandwidth to GCS, (c) shared memory (two 10.8 GiB RSS processes on a 16 GiB VM won't fit). A mini-experiment with 2 workers at 500 feeds each on one VM would close this gap. Otherwise the "halve fleet size" recommendation is unvalidated.

6. **GCS upload tail.** The bimodal p50=56 ms / p95=3527 ms distribution is pinned on "connection pool exhaustion" and "event-loop contention" without measurement. There is no HTTP/2 connection pool metric (e.g., `aiohttp` connector stats or GCS client pool depth). For the tail hypothesis to be credible, a simple overlay of upload latency vs. concurrent in-flight uploads would do it. As written, the bullets in §5.5 are plausible but unverified.

7. **ffmpeg management on event-loop thread.** The paper claims the *management* of ffmpeg (pipe read, exit detection, restart) is the bottleneck, but asyncio subprocess transports use OS-level pipe readiness notifications (epoll) and do not actively poll. The overhead is the Python-level callback dispatch per readiness event. For 621 ffmpeg processes at step 6, that's ~4 chunks/feed/min x 621 / 60 s = ~41 chunk-boundary events/s -- not obviously a 100%-of-one-core workload. Either instrument this or soften the causal claim.

### Scores (R2)
- Overall: **55/100**
- Originality: 4/10
- Technical soundness: 5/10
- Empirical rigor: 5/10
- Clarity: 7/10
- Impact: 5/10

---

## Reviewer 3 (R3) -- Statistician / Empiricist

### Summary of Statistical Claims
Two linear regressions: CPU(%) = 0.073 * feeds + 3.5 (R^2 > 0.99) and RSS(MiB) = 7.22 * feeds + 128. Six data points each. Claim of "nearly perfect linear scaling."

### Statistical Assessment

**Positives.** The raw per-step summaries (Table 1) are cleanly presented with sample counts (n=19 per step). The event-loop health distribution is reported at p50/p90/p99/max. GCS upload latency uses p50/p75/p90/p95/p99/max and mean. Reliability rates are computed with denominators (exits per feed per minute). This is above the median for production systems measurement papers.

**Concerns.**

1. **n=6 linear fit.** Six data points with two free parameters leaves four degrees of freedom. R^2 > 0.99 is unsurprising when the underlying process is nearly linear and each point is already an average of 19 samples (so within-point noise is small). The informative statistic is not R^2 -- it's the *standard error of the slope*, a prediction interval for feeds outside the fit range, and a residual plot to check for curvature at the high end. The Stage 2.5 integrity finding that the reported "least-squares" coefficients are not actually LSQ is consistent with this weakness; the revision will present correct LSQ coefficients with SEs, but the authors should also show residuals, 95% CIs on both slopes, and a prediction interval for step 7 (2,000 feeds).

2. **Why not denser sampling?** A stepped ramp at 100/250/500/750/1000/1500 has wide gaps (250 -> 500 is a 100% increase, 1000 -> 1500 is 50%). Placements at 200, 400, 600, 800, 1100, 1200, 1300, 1400 would (a) let the authors fit a non-linear function and test for curvature, (b) tighten the estimate of the knee, and (c) provide a sanity check on the "linear with R^2 > 0.99" narrative. The current design over-samples the low end and under-samples the critical region near saturation.

3. **Bimodal GCS upload latency.** The paper describes a bimodal distribution -- p50=56, p95=3527, max=10420 -- but does not quantify what fraction of uploads are in the slow cluster. A natural breakpoint analysis (e.g., Gaussian mixture, or simply "fraction of uploads above 500 ms per step") would tell us whether the tail is 1%, 5%, or 20% of uploads and whether the fraction grows with step. If the tail grows with step, that's a finding; if it's constant across steps, the explanation is not loop contention.

4. **9.7 s drift reported as a single max.** 550 event_loop_health samples with a max of 9725 ms is one extreme value. A top-tier reader wants the full drift tail: p99, p99.9, and the count of samples above, say, 100 ms, 1 s, and 5 s. Reporting only the max and p99=7 ms understates the story.

5. **Single day, single VM, single time-of-day.** Section 7 Limitation 6 notes time-of-day effects but provides no data. Cloud-region noisy-neighbor variance and Broadcastify upstream load vary substantially across time. Without at least a "control" ramp on another day (even at one or two steps), the authors cannot bound between-day variance. A minimal extension: re-run step 5 (1,000 feeds) on three separate days and report CPU mean +/- SD.

6. **Active feeds below targets.** Step 1 "99 active for target 100" and Step 6 "1,481-1,485 active for target 1,500" -- the paper treats these as "within 1-5 of targets" uniformly. But the step 6 gap is 15-19, not 1-5 (this is one of the P1 items). The scaling is fit against *target*, not *measured active*. If the actual denominator at step 6 is 1,483 rather than 1,500, the slope estimate shifts. Fit should use measured active feed count.

7. **Error propagation.** No uncertainty on per-step CPU averages (19 samples per step -> a within-step SE of the mean is trivial to compute). Showing per-step means with SE bars on the scaling plot is a standard and easy fix.

### Scores (R3)
- Overall: **52/100**
- Originality: 4/10
- Technical soundness: 5/10
- Empirical rigor: 4/10
- Clarity: 7/10
- Impact: 5/10

---

## Reviewer 4 (R4) -- Devil's Advocate

I'll argue against acceptance.

### The "Is This Science?" Problem

The paper measures one particular Python pipeline on one VM type on one day. It concludes that asyncio is single-threaded and that one process can't use four cores -- which is, to put it bluntly, the *definition* of asyncio. The Python community has been writing about this constraint for over a decade. Glyph Lefkowitz's 2013 post "Unyielding" explained exactly this failure mode. The recommendation to "run multiple worker processes" has been in the Python packaging cookbook since gunicorn's initial release. I'm struggling to identify what a SOSP/OSDI reviewer would learn here that they didn't already know from reading the `asyncio` documentation.

### Novelty Audit

Let me enumerate what's actually new:

1. The *specific numbers* for this particular pipeline: 0.073%/feed, 7.22 MiB/feed, 1,000 feeds/worker. These are Watch-Duty-specific. A reader cannot apply these to their pipeline because the coefficients depend on what each "feed" does (ffmpeg vs. WebSocket vs. HTTP poll), the audio codec, the GCS upload path, and the logging framework.
2. The observation that 3 of 4 cores are idle. This is not an observation -- it's a theorem about single-process asyncio.
3. The pre-flight smoke test. This is the table stakes of any competent experimentation, not a contribution.
4. The 9.7-second loop stall. Reported without diagnosis, with n=1, so it's an anecdote.

Stripping the Watch-Duty-specific numbers, what remains is a restatement of known asyncio behavior. The paper is in essence a well-executed engineering report.

### Reproducibility Paradox

The reproducibility claim is strong: code references, env vars, SQL, GCS queries. But because reproducing the experiment requires access to Broadcastify feeds, OpenMHz data, GCS staging bucket, and AlloyDB, no external reviewer can reproduce it. So "reproducibility" here means "Watch Duty can re-run it." That's internal validation, not scientific reproducibility.

### Composition Sensitivity

The 41:55:4 composition is production-matched, but the bcfy_feeds share (41%) dominates CPU because it spawns ffmpeg. If a different operator ran 100% bcfy_feeds -- which might reflect a different deployment -- the saturation point would be much lower (probably 300-500 feeds, not 1,000). The paper does not explore this. The headline number "1,000 feeds" is not a property of asyncio or the pipeline -- it's a property of a specific workload mix. That's a local optimum, not a general finding.

### The Operational Obviousness Test

The recommendations are:
- Use multi-process workers. (Standard.)
- Try uvloop. (Standard.)
- Offload ffmpeg management. (Standard.)
- Target 75-80% single-core utilization. (Standard.)

If I removed the data and kept only the recommendations, a senior Python systems engineer would nod and move on. That's a tell.

### Why Not a Tech Blog?

This paper is a very strong internal tech report or engineering blog post. It has clear methodology, honest failure disclosure (the abandoned initial run), and careful data. For a SOSP measurement track, I'd expect either: (a) a surprising finding that challenges conventional wisdom, (b) a new methodology that generalizes to other systems, (c) a large-scale measurement (multiple tenants, multiple workloads, long duration), or (d) a novel instrumentation technique. I see none of these.

### What Might Redeem It

If the authors (a) extended to multi-VM measurements at 12K-feed production scale, (b) performed the 2-workers-per-VM validation they recommend, (c) did a multi-source decomposition ramp (3 mono-source runs + 1 mix, validating additivity), (d) provided a longer soak with time-of-day variance, and (e) diagnosed the 9.7 s stall -- the paper would cross the threshold. As is, I lean reject / workshop.

### Scores (R4)
- Overall: **38/100**
- Originality: 2/10
- Technical soundness: 6/10
- Empirical rigor: 4/10
- Clarity: 8/10
- Impact: 3/10

---

## EIC -- Editorial Synthesis

### Panel Summary

| Reviewer | Overall | Orig | Tech | Empir | Clarity | Impact |
|----------|---------|------|------|-------|---------|--------|
| R1 (Methodologist) | 58 | 4 | 6 | 5 | 8 | 5 |
| R2 (Systems) | 55 | 4 | 5 | 5 | 7 | 5 |
| R3 (Statistician) | 52 | 4 | 5 | 4 | 7 | 5 |
| R4 (Devil's Advocate) | 38 | 2 | 6 | 4 | 8 | 3 |
| **Mean** | **50.75** | **3.5** | **5.5** | **4.5** | **7.5** | **4.5** |

### Decision Rationale

Three reviewers converge around 52-58 (borderline); the devil's advocate pulls down sharply on novelty. The panel consensus is that the paper is *well-executed internal measurement work* that is *below the bar for a top-tier systems venue* in its current form.

The paper's strongest asset is its clarity and honest disclosure (the abandoned initial run, the `bc`-vs-`awk` fix, the MIG-abandonment lesson). Its weakest asset is novelty: single-process asyncio is known to saturate one core, and the paper's "1,000 feeds/worker" number is a point estimate for one workload mix, not a transferable finding.

For SOSP/OSDI/NSDI/EuroSys measurement tracks, the minimal additions to cross the bar are:

1. **Multi-run replication** to establish between-run variance.
2. **Per-source decomposition** (3 mono-source ramps) to produce transferable per-source coefficients, not workload-specific aggregates.
3. **Validation of the multi-process recommendation** -- even one 2-workers-per-VM data point.
4. **Diagnosis of the 9.7 s stall** (GC log, cgroup stats, or acknowledged as unattributed).
5. **Statistical rigor**: n=6 LSQ with SE on coefficients, residuals, prediction interval, per-step error bars.

For a workshop (HotCloud, LASER, HotOS), the paper is close to acceptance with modest tightening.

### Editorial Decision

**Major Revision**, conditional on the additions listed in the roadmap. The paper has a real dataset, honest methodology, and a clear narrative; these are hard to build and worth investing in. But the novelty deficit and the n=1 ramp problem are blocking for a top-tier measurement track. If multi-run + mono-source decomposition are out of scope for the revision window, the EIC recommends **retargeting to a workshop** (HotCloud, LASER) where the paper is a likely accept with Minor Revision.

If the panel had to vote a single disposition at the top-tier track as-is, it would be Reject. With the Major Revision items delivered, it would be a borderline accept.

### Scores (EIC)
- Overall: **50/100**
- Originality: 3/10
- Technical soundness: 6/10
- Empirical rigor: 4/10
- Clarity: 8/10
- Impact: 4/10

---

## Editorial Decision

**MAJOR REVISION** (top-tier systems venue)

Alternate recommendation if scope must stay constant: **retarget to HotCloud / LASER / HotOS** where the current contribution is a likely accept with Minor Revision.

---

## Revision Roadmap

Items are sorted by severity. P0 = blocks acceptance at any venue. P1 = blocks top-tier acceptance; required for major revision sign-off. P2 = improves the paper but not strictly required.

### P0 -- Must Fix

**P0-1.** Address the novelty framing. The "single-threaded event loop saturates one core" finding must be reframed as: this paper *quantifies the per-workload cost coefficients* for a multi-source Python asyncio pipeline, *not* that single-thread saturation exists. Rewrite Introduction/Contributions/Conclusion to lead with the measured per-feed coefficients and mitigation validation, not the qualitative ceiling claim. Without this reframing no amount of additional data rescues the paper.

**P0-2.** Validate the multi-process recommendation experimentally. Run at least one data point with 2 workers per VM (e.g., 2 workers x 500 feeds on one n2-standard-4) and report CPU/RSS/event-loop drift. The "halve fleet size" recommendation is a central claim and currently unvalidated.

**P0-3.** Diagnose or explicitly bound the 9.7 s event-loop stall. Capture GC log, cgroup.cpu.stat, or a `py-spy` trace at the stall timestamp. If none is available, explicitly mark the event as an unattributed outlier and remove it from the saturation narrative.

### P1 -- Required for Major Revision Sign-Off (includes Stage 2.5 integrity items)

**P1-1** (Integrity Stage 2.5 item). Replace "least-squares fit" coefficients with *actual* LSQ coefficients. Report `CPU = 0.069 * feeds + 6.4` and `RSS = 7.15 * feeds + 157` (per Stage 2.5 option a), with standard errors on both slope and intercept, residual plot, 95% CI, and prediction interval for out-of-range forecasts.

**P1-2** (Integrity Stage 2.5). Fix "75% stranded" -> "~73% stranded" or "three of four vCPUs" to be consistent with the reported 27.1% used. Cross-check every mention (abstract, §5.2 Key Finding, §6.2).

**P1-3** (Integrity Stage 2.5). Fix "Active feeds 1-5 below targets" understating step 6's 15-19 gap. Table 1 and prose.

**P1-4** (Integrity Stage 2.5). Fix abstract: "saturates at 1,000 feeds (77.4%)" is misleading. 77.4% is an *approach* to saturation, not saturation itself. Rewrite to: "approaches saturation at ~1,000 feeds (77.4% single-core utilization) and exceeds single-core at 1,500 feeds (108.3%)."

**P1-5** (Integrity Stage 2.5). Fix "1,000-1,250 feeds/worker" upper bound -- 1,250 feeds at 0.069% slope + 6.4 base = 92.65% single-core, which violates the 75-80% target. Either move to "~1,050 feeds/worker for 80% target" or widen the target to 90%.

**P1-6.** Multi-run replication. Re-run at least steps 3 (500) and 5 (1,000) on two additional days. Report between-run SD for CPU, RSS, event-loop drift. If feasible, add per-step error bars on all tables.

**P1-7.** Per-source decomposition ramps. Three mono-source ramps (pure bcfy_feeds, pure bcfy_calls, pure openmhz) at 100, 500, 1000 feeds each. Fit per-source linear models. Validate that the mix (41:55:4) is predicted by the linear combination.

**P1-8.** Tighten GIL/asyncio framing (R2). Remove or sharpen GIL references. Asyncio is single-threaded by design; GIL is secondary. This is a terminology fix not a data change.

**P1-9.** Drop or weaken uvloop "2-4x" claim. Either run a uvloop sidecar experiment or reduce to: "published benchmarks from the uvloop project report 2-4x; independent validation for this workload remains future work."

**P1-10.** Verify GCS upload tail hypothesis (R2). Overlay upload latency vs. concurrent in-flight uploads or HTTP/2 connection pool depth. If that instrumentation is not available, say so and present the tail as observed but unattributed.

**P1-11.** Event-loop drift tail reporting. Report p99.9 drift and the count of drift samples > 100 ms, > 1 s, > 5 s (R3).

**P1-12.** Fit against measured active-feed count (R3) rather than target count. Re-derive all coefficients using actual active feeds as the independent variable.

### P2 -- Recommended

**P2-1.** Denser step placement (R3). If re-running, add intermediate points (e.g., 200, 400, 600, 800, 1100, 1200) to test for non-linearity near the ceiling.

**P2-2.** Intra-step stationarity plot (R1). Show CPU and active-feed time series per step to demonstrate warmup sufficiency.

**P2-3.** `docker stats` CPU semantics appendix (R1). Brief appendix on cgroup v1 vs v2 and the 100% = one core convention.

**P2-4.** Bimodal upload latency breakpoint (R3). Gaussian mixture or simple threshold-count per step to quantify the fraction of uploads in the slow cluster.

**P2-5.** Downgrade pre-flight smoke test from "contribution" to a methodology paragraph (R1).

**P2-6.** Add time-of-day robustness data -- even a single daytime ramp vs. the current nighttime one (R3, Limitation 6).

**P2-7.** Consider re-targeting to a workshop (HotCloud, LASER, HotOS) if the P0/P1 items cannot be delivered in the revision window.

---

## Strengths to Preserve (Revision Must Not Weaken These)

1. **Honest failure disclosure.** The abandoned initial run (MIG reconciliation, openmhz bug, `bc`-not-available, bcfy_calls 401s) is exactly the sort of disclosure the systems community values. Keep this in the paper; do not sanitize in revision.

2. **Pre-flight smoke test design.** Even after being demoted from "contribution" to "methodology," the seven gates are a good concrete artifact. Keep the gate list in §3.

3. **Raw-data fidelity.** Stage 2.5 verified all 9 code citations and that raw numbers match within rounding. This is above the measurement-paper median. Do not allow revision rewrites to drift from raw data.

4. **Reliability characterization.** The 167 exit-8 events and 898 HTTP 403 categorization with rate computation (exits per feed per minute) is solid. Preserve the reliability subsection and its denominators.

5. **Clear fleet-sizing translation.** The Section 6.1 table linking feeds/worker to VM count to dollar cost is operationally valuable and uncommon in systems papers. Preserve (with corrected coefficients).

6. **Linear scaling evidence for memory.** RSS linearity with no observed fragmentation over 92 minutes is a meaningful stability finding worth preserving; just add per-step error bars.

7. **Cloud Logging query documentation.** The explicit Cloud Logging filters (jsonPayload.type, timestamp ranges) make the data traceable. Keep this citation style throughout.

---

## Cumulative Revision History

(None -- this is the first review round.)

---

*End of Stage 3 Review Report.*
