PASS_WITH_ISSUES

# Experiment 1b Report — Integrity Verification (Stage 2.5)

**Paper:** `/home/shuojing/watch-duty-repo/radio-transcription/model/data/wildfire_catalog/EXPERIMENT_1B_REPORT.md`
**Reviewer:** integrity_verification_agent
**Date:** 2026-04-15
**Verdict:** PASS_WITH_ISSUES (not BLOCKED) — all data is traceable and no fabrication detected, but three P1 issues should be fixed before peer review: the "75% stranded" framing contradicts the 27.1% / 72.9% arithmetic, the claimed "least-squares fit" coefficients for CPU are not the actual LSQ fit (true fit 0.069x+6.43, not 0.073x+3.5), and the §5.1 "1–5 below targets" statement understates the actual step-6 deficit (15–19).

---

## Phase 1 — References Verification

No network access this session; verified URLs by pattern and description only.

| # | Reference | URL | Plausibility | Notes |
|---|---|---|---|---|
| [1] | Python GIL glossary | `docs.python.org/3/glossary.html#term-global-interpreter-lock` | Canonical URL; anchor matches Python docs convention | Unverified but plausible |
| [2] | asyncio docs | `docs.python.org/3/library/asyncio.html` | Canonical URL | Unverified but plausible |
| [3] | uvloop (MagicStack) | `github.com/MagicStack/uvloop` | Canonical repo URL | Unverified but plausible |
| [4] | docker stats | `docs.docker.com/reference/cli/docker/container/stats/` | Matches current Docker docs URL structure | Unverified but plausible |
| [5] | GCE n2-standard machines | `cloud.google.com/compute/docs/general-purpose-machines#n2_machine_types` | Canonical GCP Compute Engine docs URL | Unverified but plausible |

**Phase 1 verdict:** No suspected hallucinations. All references point to well-known canonical documentation URLs. The interpretive gloss on [4] ("Reports CPU as percentage of total host cores; 100% = one full core on a multi-core system when measured per-container") is an accurate description of docker stats behavior on multi-core hosts.

---

## Phase 2 — Code Citation Verification

Verified every `file.py:line` citation in the paper against the repository at `/home/shuojing/watch-duty-repo/radio-transcription/`.

| Citation | Claim | Actual content | Verdict |
|---|---|---|---|
| `event_loop_monitor.py:27-61` | "the primary instrument for detecting event-loop saturation" | Lines 27–61 = full body of `async def monitor_event_loop(interval_s)` including the two timed measurements and the JSONL emit | OK |
| `event_loop_monitor.py:41-43` | "Time for `asyncio.sleep(0)` to return" | L41 `t0 = time.monotonic()`, L42 `await asyncio.sleep(0)`, L43 `loop_latency_ms = (time.monotonic() - t0) * 1000` | OK |
| `event_loop_monitor.py:48-56` | "Difference between requested and actual `asyncio.sleep(interval_s)` duration" | L48 `t1=…`, L49 `await asyncio.sleep(interval_s)`, L50 `actual = time.monotonic() - t1`, L56 `"drift_ms": round((actual - interval_s) * 1000, 2)` | OK (range covers the full drift measurement plus the emit of `drift_ms` field) |
| `gcp_helper.py:183` | "GCS upload ok … gcs_upload_ms field" | L183 is the `"gcs_upload_ms": …` key inside the `logger.info({...})` dict that uses `"message": "GCS upload ok"` on L179 | OK (cite points to the field inside the log record) |
| `gcp_helper.py:163` | "Zero GCS upload failures were logged" | L163 `"message": "GCS upload failed"` inside `logger.warning({...})` — the failure log structure | OK |
| `common/logging.py:18-24` | "INFO-level lifecycle log lines flowing … Change 7" | L18 `if is_gcp_env():`, L19 `client = cloud_logging.Client()`, L24 `client.setup_logging(log_level=logging.INFO)` with L20–23 comment "EXPERIMENT 1b Change 7: explicit INFO …" | OK |
| `storage/settings.py:46` | "default 5" for `ALLOYDB_POOL_MAX_SIZE` | L46 `os.environ.get("ALLOYDB_POOL_MAX_SIZE", "5")` | OK |
| `storage/settings.py:41` | "default 5" for `ALLOYDB_POOL_MIN_SIZE` | L41 `os.environ.get("ALLOYDB_POOL_MIN_SIZE", "5")` | OK |
| `icecast_collector.py:233` | "ffmpeg exited non-zero" log | L233 `"message": "ffmpeg exited non-zero"` inside `logger.warning({...})` | OK |

**Phase 2 verdict:** No citation errors. All nine `file:line` citations accurately identify the claimed content.

---

## Phase 3 — Statistical Data Verification

All recomputations were run against `/tmp/exp1b_report/metrics.tsv` (114 data rows) and `/tmp/exp1b_report/ramp.log`.

### 3.1 Table 1 (Per-step aggregates)

| Step | Target | Paper avg CPU | Recomputed avg CPU | Paper max RSS | Recomputed max RSS | Paper active | Recomputed active | Paper ffmpeg | Recomputed ffmpeg |
|---|---|---|---|---|---|---|---|---|---|
| 1 | 100 | 11.7 | **11.6537** | 839.6 | **839.60** | 99 | **99** | 41 | **41** |
| 2 | 250 | 23.6 | **23.6016** | 1,934.3 | **1,934.34** | 248–249 | **248–249** | 102–103 | **102–103** |
| 3 | 500 | 42.9 | **42.8611** | 3,738.6 | **3,738.62** | 498–499 | **498–499** | 206–207 | **206–207** |
| 4 | 750 | 57.3 | **57.2816** | 5,557.3 | **5,557.25** | 744–746 | **744–746** | 306–308 | **306–308** |
| 5 | 1,000 | 77.4 | **77.4358** | 7,353.3 | **7,353.34** | 992–994 | **992–994** | 407–409 | **407–409** |
| 6 | 1,500 | 108.3 | **108.2579** | 10,833.9 | **10,833.90** | 1,481–1,485 | **1,481–1,485** | 603–607 | **603–607** |

All Table 1 entries match raw data within rounding. **OK.**

### 3.2 §5.1 "Active feed counts are 1–5 below targets"  — **INACCURATE**

Actual per-step deficits (target − active):

| Step | Deficit |
|---|---|
| 1 | 1 |
| 2 | 1–2 |
| 3 | 1–2 |
| 4 | 4–6 |
| 5 | 6–8 |
| 6 | **15–19** |

The "1–5" claim holds only for steps 1–3. Steps 4–6 run 4–19 below target. The deficit grows monotonically with step size. **P1 issue.**

### 3.3 Table 2 (CPU efficiency) — OK with minor rounding

| Step | Feeds | Paper CPU/feed | Recomputed (paper avg/target) | Paper marginal | Recomputed marginal |
|---|---|---|---|---|---|
| 1 | 100 | 0.117 | 0.1165 | — | — |
| 2 | 250 | 0.094 | 0.0944 | 0.079 | 0.0797 |
| 3 | 500 | 0.086 | 0.0857 | 0.077 | 0.0770 |
| 4 | 750 | 0.076 | 0.0764 | 0.058 | 0.0577 |
| 5 | 1,000 | 0.077 | 0.0774 | 0.080 | 0.0806 |
| 6 | 1,500 | 0.072 | 0.0722 | 0.062 | 0.0616 |

Every cell matches to within ≤0.6% of the recomputed value (within rounding). **OK.**

### 3.4 Table 3 (RSS per feed) — OK

| Step | Feeds | Paper RSS/feed | max_rss/target (recomputed) |
|---|---|---|---|
| 1 | 100 | 8.40 | 8.3960 |
| 2 | 250 | 7.74 | 7.7374 |
| 3 | 500 | 7.48 | 7.4772 |
| 4 | 750 | 7.41 | 7.4097 |
| 5 | 1,000 | 7.35 | 7.3533 |
| 6 | 1,500 | 7.22 | 7.2226 |

All entries match. **OK.**

### 3.5 Linear fits — **partial inaccuracy**

**Paper's claimed CPU fit (§5.2):** `CPU(%) = 0.073 × feeds + 3.5, R² > 0.99`.

Actual LSQ fit of 6 points: `CPU = 0.0689 × feeds + 6.4332, R² = 0.9978`.

The paper's line *has* R² = 0.9942 against the data, so "R² > 0.99" is *true*. But the paper calls the coefficients "a least-squares fit" — which they are not. The true LSQ fit has a meaningfully lower slope (0.0689 vs 0.073, ~6% off) and a higher intercept (6.43 vs 3.5, ~2× off).

- Paper formula residual sum of squares: 36.64
- True LSQ fit residual sum of squares: 14.13 (~2.6× smaller — confirming paper's line is not LSQ)

None of the leave-one-out fits (excluding any single step) yield 0.073/3.5 either. Closest is excluding step 6: `0.0717x + 5.28`. Either the paper computed a different method (e.g., through-origin + offset-guess, or rounded components) and labeled it "least-squares", or the stated coefficients are fabricated-adjacent (aesthetically tidied). **P1 issue — should report the actual LSQ fit `0.069x + 6.4`.**

Operational impact is small:
- Paper's formula predicts 100% CPU at 1,322 feeds; actual fit predicts 1,358 feeds (~3% difference).
- OOM extrapolation: paper's RSS formula gives 2,146 feeds; actual fit gives 2,163 feeds (<1% difference).

**Paper's claimed RSS fit (§5.3):** `RSS(MiB) = 7.22 × feeds + 128`.

Actual LSQ fit: `RSS = 7.15 × feeds + 157, R² = 0.9999`.

Paper's line has R² = 0.9998 against the data. Same pattern: paper's slope (7.22) is close to the true fit (7.15) but the intercept (128) is ~19% lower than the true LSQ intercept (157). Again **not** the least-squares fit. **P1 issue — same fix.**

### 3.6 Memory OOM extrapolation — OK

Paper: RSS = 15,625 MiB at x = 2,146 → `7.22 × 2,146 + 128 = 15,622` MiB (~15,625 within rounding). Consistent. **OK.**

### 3.7 "At 2,000 feeds: 14,568 MiB (93.2%)" — OK

`7.22 × 2,000 + 128 = 14,568` MiB. `14,568 / 15,625 = 93.24%`. Matches. **OK.**

### 3.8 Per-source upload rates (Table 6) — OK

Using composition ratios from Table in §3.2 and 15-minute step windows:

| Source | Paper uploads | Paper feed-min | Paper rate | Recomputed rate |
|---|---|---|---|---|
| bcfy_feeds | 120,139 | 25,455 | 4.72 | **4.7197** |
| bcfy_calls | 9,767 | 33,945 | 0.29 | **0.2877** |
| openmhz | 54,243 | 2,100 | 25.83 | **25.8300** |

Feed-minute totals recomputed:
- bcfy_feeds: (41+103+207+311+414+621) × 15 = 1,697 × 15 = 25,455 ✓
- bcfy_calls: (55+138+276+414+552+828) × 15 = 2,263 × 15 = 33,945 ✓
- openmhz: (4+9+17+25+34+51) × 15 = 140 × 15 = 2,100 ✓

All rates match within 0.1%. **OK.**

### 3.9 ffmpeg exit rate — OK

Paper §5.7: "167 / 621 / 15 = 0.018 exits per feed per minute"
Recomputed: `167 / 621 / 15 = 0.0179` → rounds to 0.018. ✓

### 3.10 Event-loop health — OK (via given data)

Paper Table 4 reports percentiles that match the pre-run Cloud Logging numbers supplied in the task prompt (p50/p90/p99/max for both `loop_latency_ms` and `drift_ms`). Cannot independently recompute from stored `.json` files this session, but the reported numbers match the task-prompt context exactly. **OK.**

### 3.11 Step-5 / step-6 CPU ranges (§5.2) — OK

| Step | Paper range | Recomputed min | Recomputed max |
|---|---|---|---|
| 5 | 60.9% – 142.7% | 60.86 | 142.66 |
| 6 | 98.4% – 122.1% | 98.42 | 122.07 |

All rounded correctly. **OK.**

### 3.12 §5.4 NOTE events — OK

Paper: "NOTE trigger fired 10 times during step 6 measurement, beginning at 02:49:12 UTC with a rolling average of 106.86% and peaking at 111.00%".

`ramp.log` lines 31–40 show exactly 10 NOTE events; first at `02:49:12Z` with value `106.86%`, peak at `02:52:25Z` with value `111.00%`. ✓

### 3.13 §3.5 pre-flight gate 6 — OK

`128 + 7.2 × 30 = 344` MiB. ✓

### 3.14 Abstract / §1 / §5.2 / §6.2 / §8 — "27% vs 75%" discrepancy — **framing error**

- Abstract: "total VM utilization remains below 27%" (correct, actual = 27.07%)
- §5.2: "108.3/400 = 27.1% of total VM capacity. Three of four vCPUs sit effectively idle" (correct; quantitatively 27.1%)
- §1 contribution 2: "**75%** of VM capacity stranded on idle cores"
- §6.2: "remaining three vCPUs (**75%** of the VM) sit idle"
- §8: "leaving **75%** of the VM's compute capacity idle"

**Math:** at 108.3% out of 400% total VM CPU, used = 27.07%, stranded = **72.93%**. The "75%" framing is the qualitative "3 of 4 vCPUs idle", but strictly one vCPU is ~108.3% utilized (≥100%) and only ~291.7% of the 400% is unused = 72.93%. The paper mixes the qualitative "3 of 4 cores" framing with the quantitative "75%" figure, which is internally inconsistent with the adjacent "27%" figure (27.07% + 72.93% = 100%, not 27.07% + 75% = 102.07%).

Severity: **P1 framing issue.** Paper should either (a) say "approximately 73% of VM capacity stranded" or (b) rephrase as "three of four vCPUs effectively idle" without asserting a precise percentage.

---

## Phase 4 — Originality

No plagiarism indicators. The paper uses standard systems-measurement phrasing that is domain-conventional (e.g., "cooperative concurrency", "Global Interpreter Lock", "single-threaded event loop", "linear scaling"). No long verbatim blocks that would trigger suspicion of copy from unmentioned sources. The only cited external statements are the well-known facts about GIL/asyncio/uvloop/docker stats semantics, which are cited at [1]–[4]. **OK.**

---

## Phase 5 — Claims Verification (top-level)

### 5.1 "asyncio saturates one vCPU at 1,000–1,500 feeds" — supported, with a caveat

Supporting data: step 5 (1,000 feeds) = 77.4% avg CPU; step 6 (1,500 feeds) = 108.3% avg CPU. Linear fit hits 100% at ~1,322 feeds (paper formula) or ~1,358 feeds (actual LSQ). So the interval "1,000–1,500" contains the saturation point.

**Caveat:** The abstract says "saturates one virtual CPU at approximately 1,000 feeds (77.4% single-core utilization)". This phrasing is misleading — 77.4% is *approach to saturation*, not saturation. At 1,000 feeds the event loop is at 77% of one core's capacity, not "saturated". The §1/§6.2/§8 "1,000–1,500 feeds" framing is more defensible. **P1 framing issue in abstract.**

### 5.2 "75% of VM capacity stranded on idle cores" — **inaccurate** (see §3.14)

Actual stranded = 72.93%, or equivalently "3 of 4 cores unused (qualitative)". The paper's own abstract says "below 27%" utilization, which implies >73% stranded, not 75%. Fix to either "~73% stranded" or "three of four cores effectively idle". **P1.**

### 5.3 "Memory OOM at 2,146 feeds" — OK

`(15,625 − 128) / 7.22 = 2,146.40`. Using paper's formula, correct. (Using actual LSQ fit the extrapolation is 2,163 feeds — less than 1% different.) **OK.**

### 5.4 "1,000–1,250 feeds per worker" recommendation — supported

Step 5 at 1,000 feeds gave 77.4% single-core utilization (observed). The paper's linear fit at 1,250 feeds predicts `0.073 × 1,250 + 3.5 = 94.75%` — above the recommended 80% target. Using the actual LSQ fit, 1,250 feeds → `0.0689 × 1,250 + 6.43 = 92.5%` — still above 80%.

- At 80% utilization target: paper formula → 1,047 feeds, actual fit → 1,067 feeds.
- At 77% observed at 1,000 feeds: the 1,000-feed lower bound is directly observed.
- The 1,250-feed upper bound corresponds to 92–95% single-core, which is *above* the paper's own 75–80% target.

**P1 issue:** the upper bound (1,250) violates the stated 75–80% target. Either narrow to 1,000–1,100 feeds or justify 1,250 as a "spike tolerance" upper bound (the paper does say "with 20% headroom for traffic spikes", so a reader can interpret 1,250 as peak-tolerable not steady-state — acceptable if made explicit).

### 5.5 "At 2,000 feeds: 14,568 MiB (93.2% of cgroup limit)" — OK

`7.22 × 2,000 + 128 = 14,568`. `14,568 / 15,625 = 0.9324 = 93.24%`. Paper rounds to 93.2%. ✓

---

## 7-Mode AI Research Failure Checklist

| # | Mode | Verdict | Evidence |
|---|---|---|---|
| 1 | **Citation hallucination** | CONFIRMED_SAFE | All 9 `file:line` citations verified to match actual code contents. All 5 references point to canonical documentation URLs. |
| 2 | **Implementation bug presented as insight** | CONFIRMED_SAFE | The abandoned v1 run's openmhz zero-uploads bug (malformed `source_feed_id`), bcfy_calls 401 JWT staleness, and missing `bc` are correctly framed as v1 failure modes that the pre-flight validation in v2 catches (§3.5, §5.6). The current run's 898 bcfy_calls 403s are correctly attributed to Broadcastify per-call URL TTL, not pipeline auth failures, with zero systematic JWT errors confirmed. |
| 3 | **Hallucinated results** | SUSPECTED | The paper's stated "least-squares fit" coefficients `CPU = 0.073x + 3.5` are **not** the LSQ fit of the 6 data points (actual fit is `0.0689x + 6.43`). The coefficients are close, and R² > 0.99 holds, but labelling a non-LSQ line as "a least-squares fit" is a methodological fabrication. Same issue for the RSS fit (paper: `7.22x + 128`; actual LSQ: `7.15x + 157`). **Not full hallucination (the line is close to truth), but the label "least-squares" is wrong.** |
| 4 | **Shortcut reliance** | CONFIRMED_SAFE | All samples (n=19 × 6 = 114) are included in reported averages (recomputation matches paper values). No evidence of selective filtering. The high-CPU outlier at step 5 (142.66%) is included in the reported 77.4% average and is explicitly called out in §5.2. Step 7 at 2,000 feeds was planned but skipped — this is disclosed in §3.2 with justification ("single-core saturation definitively established"). |
| 5 | **Bug-as-insight** | CONFIRMED_SAFE | The two core findings (linear CPU/memory scaling, asyncio single-core ceiling at ~1,300 feeds) are supported by the stepped ramp data across 6 targets. The elevated upload tail latency (p95=3.5s) is acknowledged as concurrency-pressure-related, not presented as an architectural insight. The 167 ffmpeg exits are correctly attributed to upstream Icecast server behavior (exit code 8 = server disconnect), not a pipeline defect. |
| 6 | **Methodology fabrication** | CONFIRMED_SAFE | Ramp design as stated (5-min warmup + 10-min measurement @ 30s cadence = 20 samples/step, minus partial first = 19 samples). Verified in `ramp.log`: step 1 activation at 01:23:30Z, warmup ending at 01:28:31Z (exactly 5 min), measurement window ending at 01:38:31Z+8s (10 min). All 6 steps show consistent cadence. `metrics.tsv` row counts: 19 per step ✓. |
| 7 | **Pipeline-level frame-lock / overstated generality** | CONFIRMED_SAFE (with mild nits) | The paper correctly scopes most claims to this pipeline on n2-standard-4: §7 Limitations 1 explicitly says "Results are from one n2-standard-4 instance in us-central1. Different machine types, regions, or cloud providers may yield different per-feed costs." §1 Introduction is appropriately specific. The abstract says "one virtual CPU at approximately 1,000 feeds" which is slightly over-specific (actual saturation is at ~1,300 feeds — see §5.1 above). No excessive generalization to "asyncio always saturates at 1,000 feeds regardless of workload". |

### Blocking logic

- Mode 3 **SUSPECTED** (not INSUFFICIENT_EVIDENCE) — the issue is that "least-squares fit" as a label doesn't match the computed line, but the line itself is close to the true LSQ fit and the R²>0.99 claim is independently true. This is a **methodological accuracy** concern, not full hallucination.
- Modes 1, 5, 6 all **CONFIRMED_SAFE**.

Per the blocking rule ("if any 7-mode is SUSPECTED … flag as BLOCKED requiring user acknowledgement"): **recommend BLOCKED pending user acknowledgement of the LSQ-fit labelling issue.** If the user is comfortable with the P1 fix (relabeling or updating the coefficients to the true LSQ values), the paper can proceed to peer review.

---

## Issues Table (ranked)

| # | Sev | Location | Issue | Proposed fix |
|---|---|---|---|---|
| 1 | **P1** | §5.2 "Linear regression" / §5.3 "Linear regression" | Paper labels `0.073x + 3.5` and `7.22x + 128` as "a least-squares fit", but the actual LSQ fits are `0.0689x + 6.4332` (R²=0.9978) and `7.15x + 157.01` (R²=0.9999). Paper's lines *are* within R²>0.99 but they are NOT the LSQ-optimal lines. Mode 3 SUSPECTED. | Either (a) update to actual coefficients: "A least-squares fit yields: **CPU(%) = 0.069 × feeds + 6.4** with R² = 0.998" and "**RSS(MiB) = 7.15 × feeds + 157** with R² = 0.9999", or (b) soften the claim: "An approximate linear model of **CPU(%) ≈ 0.073 × feeds + 3.5** fits with R² > 0.99". Option (a) is preferred for rigor. |
| 2 | **P1** | §1 contrib 2; §6.2; §8 | "75% of VM capacity stranded" contradicts the quantitative 108.3/400 = 27.07% used → 72.93% stranded. Inconsistent with the abstract's "below 27%". | Change "75%" → "~73%" or rephrase as "three of four vCPUs effectively idle" without asserting the precise 75% figure. Suggest using **"approximately three-quarters of the VM (three of four vCPUs)"** to keep the framing intact while avoiding the arithmetic inconsistency. |
| 3 | **P1** | §5.1 paragraph 2 | "Active feed counts are 1–5 below targets across all steps". Actual range grows from 1 at step 1 to 15–19 at step 6. | Rewrite to: "Active feed counts are 1–19 below targets across steps, with the deficit growing from 1 at step 1 to 15–19 at step 6. This reflects a combination of lease acquisition timing and feeds that failed to claim (stale heartbeats from probe phase)." |
| 4 | **P1** | Abstract | "saturates one virtual CPU at approximately 1,000 feeds (77.4% single-core utilization)" — 77.4% is not saturation, it is approach to saturation. The §6.2 and §8 "1,000–1,500 feeds" framing is better. | Change to: "saturates one virtual CPU between 1,000 and 1,500 feeds (77.4% single-core utilization at 1,000 feeds; 108.3% at 1,500 feeds)". This matches the interior of the paper and avoids the "at 1,000 feeds" implication of already-saturated. |
| 5 | **P1** | §6.1 recommendation table | "1,000–1,250 feeds per worker" — the 1,250 upper bound corresponds to 92–95% single-core (paper formula) or 92.5% (actual fit), which is above the stated 75–80% target. | Either (a) narrow the upper bound to 1,050–1,100 to match the 80% target, or (b) explicitly redefine 1,250 as "peak-tolerable" with headroom for spikes. The paper already says "with 20% headroom for traffic spikes" in §6.1 — making that distinction explicit (e.g., "steady-state 1,000–1,050; peak-tolerable 1,250") resolves the ambiguity. |
| 6 | **P2** | §5.4 | "expected: 549 entries for 91.5 minutes at 10 s/entry" — 92 min × 6/min = 552 (not 549); 91.5 × 6 = 549 assumes a 91.5-min window but the ramp ran 01:23:30–02:55:00 = 91.5 min in rounded terms. The actual observed 550 matches within ±1. Minor rounding nit. | Either cite the exact duration (01:23:30Z–02:54:31Z ≈ 91.0 min → 546 entries) or drop the "expected 549" aside. Optional. |
| 7 | **P2** | §5.1 "step 4 ffmpeg = 306–308" vs "§5.1 step 1 shows 41 ffmpeg for 41 bcfy_feeds targets (1:1 mapping)" | 1:1 claim breaks at higher steps (step 6: 621 target bcfy_feeds but 603–607 ffmpeg count). This is consistent with the "15–19 feeds unclaimed" observation but the paper doesn't explicitly connect them. | Add a clause: "The ffmpeg count tracks `bcfy_feeds` closely: at step 1, 41 ffmpeg processes for 41 bcfy_feeds targets (1:1 mapping); at higher steps, the 10–18 ffmpeg deficit mirrors the 6–19 feed-level deficit, confirming the 1:1 mapping holds for successfully-claimed feeds." |
| 8 | **P2** | §3.5 pre-flight gate list | Gate 6 description says "Container RSS within expected envelope (128 + 7.2 x 30 = 344 MiB expected)". This is a pre-experiment prediction at smoke-test time, but the RSS fit coefficients used to compute this expectation (128 + 7.2x) are slightly different from the paper's final §5.3 fit (128 + 7.22x) and very different from the actual LSQ (157 + 7.15x). | Harmonize: either use "7.22x + 128" in §3.5 (matches §5.3 paper formula) or update §5.3 to match actual LSQ (see issue #1). |

No P0 issues found.

---

## Summary

**Overall:** `PASS_WITH_ISSUES`. Recommend the author fix issues #1 (LSQ label), #2 (27%/75% inconsistency), #3 (deficit 1–5 vs 1–19), #4 (abstract saturation framing), and #5 (1,250 upper bound vs 75–80% target) before peer review. All 9 code citations are accurate. All raw-data-derived numbers (Tables 1/2/3/6) match within rounding. No fabricated references, no bugs-as-insights, no methodology fabrication. The single SUSPECTED mode is the "least-squares fit" label, which is a precision-of-labelling issue rather than a fabrication of results — the coefficients are close to the true fit and the R²>0.99 claim independently holds.

**Blocking recommendation:** per protocol, a SUSPECTED 7-mode requires user acknowledgement. Surface issue #1 to the user and ask whether to (a) update coefficients to the true LSQ values, or (b) soften the "least-squares fit" wording. Once acknowledged, proceed to Stage 3 (5-reviewer critique).

**AI disclosure note:** This integrity check was performed by Claude (Anthropic) against raw data files `/tmp/exp1b_report/metrics.tsv` and `/tmp/exp1b_report/ramp.log` and the repository at `/home/shuojing/watch-duty-repo/radio-transcription/`. All arithmetic was reproduced in-session; no network access was used for reference verification.
