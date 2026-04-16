PASS

# Experiment 1b Report — Final Integrity Verification (Stage 4.5)

**Paper:** `/home/shuojing/watch-duty-repo/radio-transcription/model/data/wildfire_catalog/EXPERIMENT_1B_REPORT.md`
**Reviewer:** integrity_verification_agent
**Date:** 2026-04-15
**Protocol:** Full 5-phase + 7-mode checklist, independent verification from scratch
**Verdict:** PASS — zero P0, zero P1 issues. Two P2 observations noted below.

---

## Executive Summary

The revised Experiment 1b report passes independent re-verification. All numeric, code-citation, and reference claims were re-derived from the raw sources (`/tmp/exp1b_report/metrics.tsv`, `/tmp/exp1b_report/ramp.log`, `/tmp/exp1b_report/stats.json`, Cloud Logging summaries provided in the task context, and the repository at `/home/shuojing/watch-duty-repo/radio-transcription/`). The revision introduced no new errors. All five Stage 2.5 P1 issues are fully resolved. Cross-section consistency holds (abstract ↔ §1 ↔ §5 ↔ §6 ↔ §7 ↔ §8) for the three highest-risk claims from the prior revision cycle: the per-workload coefficient reframing, the "three of four vCPUs effectively idle" phrasing, and the multi-process "modeled, not validated" disclosure.

---

## Phase 1 — References Verification

| # | Reference | URL | Assessment |
|---|---|---|---|
| [1] | Python GIL glossary | `docs.python.org/3/glossary.html#term-global-interpreter-lock` | Canonical Python docs URL. Anchor convention correct. |
| [2] | asyncio library docs | `docs.python.org/3/library/asyncio.html` | Canonical Python docs URL. |
| [3] | uvloop (MagicStack) | `github.com/MagicStack/uvloop` | Canonical open-source repository URL. |
| [4] | docker stats | `docs.docker.com/reference/cli/docker/container/stats/` | Matches current Docker reference URL structure. |
| [5] | GCE n2-standard machines | `cloud.google.com/compute/docs/general-purpose-machines#n2_machine_types` | Canonical GCP Compute Engine docs URL. |

**Phase 1 verdict:** No plausibility concerns. All five references point to well-known canonical documentation URLs. The interpretive gloss on [4] (one-core normalization under cgroup v1 and v2) is an accurate description of `docker stats` behavior on multi-core hosts.

---

## Phase 2 — Code Citation Verification

Every `file.py:line` citation in the revised paper was independently checked against the repository.

| Citation | Claim in Paper | Actual Content at Line | Verdict |
|---|---|---|---|
| `event_loop_monitor.py:27-61` | "primary instrument for detecting event-loop saturation" | L27: `async def monitor_event_loop(interval_s: float = 10.0) -> None:` through L61 closing paren of `print(...)` — full function body | OK |
| `event_loop_monitor.py:37-61` | "(§3.3) event-loop monitor measurement loop" | L37: `while True:`; L61: close of print call — measurement loop body | OK |
| `event_loop_monitor.py:41-43` | "Time for `asyncio.sleep(0)` to return" | L41 `t0 = time.monotonic()`; L42 `await asyncio.sleep(0)`; L43 `loop_latency_ms = (time.monotonic() - t0) * 1000` | OK |
| `event_loop_monitor.py:48-56` | "Difference between requested and actual `asyncio.sleep(interval_s)` duration" | L48 `t1 = time.monotonic()`; L49 `await asyncio.sleep(interval_s)`; L50 `actual = time.monotonic() - t1`; L56 `"drift_ms": round((actual - interval_s) * 1000, 2)` | OK (range covers full drift measurement and field emit) |
| `gcp_helper.py:183` | "GCS upload ok … `gcs_upload_ms` field at `gcp_helper.py:183`" | L183: `"gcs_upload_ms": (time.monotonic() - t0) * 1000.0,` inside `logger.info({"message": "GCS upload ok", ...})` block starting at L177 | OK |
| `gcp_helper.py:163` | "Zero GCS upload failures were logged (`gcp_helper.py:163`)" | L163: `"message": "GCS upload failed"` inside `logger.warning({...})` | OK |
| `common/logging.py:18-24` | "INFO-level lifecycle log lines flowing … Change 7" | L18 `if is_gcp_env():`; L19 `client = cloud_logging.Client()`; L20–23 comment "EXPERIMENT 1b Change 7: explicit INFO"; L24 `client.setup_logging(log_level=logging.INFO)` | OK |
| `storage/settings.py:41,46` | "raised from default 5 … pool min/max sizes" | L41: `os.environ.get("ALLOYDB_POOL_MIN_SIZE", "5")`; L46: `os.environ.get("ALLOYDB_POOL_MAX_SIZE", "5")` | OK |
| `icecast_collector.py:233` | `"ffmpeg exited non-zero"` structured log | L233: `"message": "ffmpeg exited non-zero"` inside `logger.warning({...})` dict | OK |

**Phase 2 verdict:** All nine `file:line` citations accurately identify the claimed repository content. No hallucinated, mis-numbered, or stale citations.

---

## Phase 3 — Statistical Data Verification

All per-step aggregates, LSQ fits, derived ratios, and extrapolations were recomputed from `/tmp/exp1b_report/metrics.tsv` (114 data rows) and `/tmp/exp1b_report/ramp.log` (44 lines). Cloud Logging percentiles were cross-checked against the task-provided context values.

### 3.1 Table 1 — Per-step aggregates

| Step | Paper mean CPU | Recomputed | Paper SD / CoV | Recomputed SD / CoV | Paper max RSS | Recomputed max RSS |
|---|---|---|---|---|---|---|
| 1 | 11.65 | 11.6537 | 2.82 / 24.2% | 2.8187 / 24.19% | 839.6 | 839.60 |
| 2 | 23.60 | 23.6016 | 3.59 / 15.2% | 3.5866 / 15.20% | 1,934.3 | 1,934.34 |
| 3 | 42.86 | 42.8611 | 4.28 / 10.0% | 4.2824 / 9.99% | 3,738.6 | 3,738.62 |
| 4 | 57.28 | 57.2816 | 3.86 / 6.7% | 3.8631 / 6.74% | 5,557.3 | 5,557.25 |
| 5 | 77.44 | 77.4358 | 17.56 / 22.7% | 17.5644 / 22.68% | 7,353.3 | 7,353.34 |
| 6 | 108.26 | 108.2579 | 5.94 / 5.5% | 5.9372 / 5.48% | 10,833.9 | 10,833.90 |

All entries match raw data within rounding. Step-5 CoV of 10.7% with outlier excluded recomputes to 10.71% ✓ (paper claim verified).

### 3.2 CPU LSQ fit (target-based, df=4, t-crit=2.776)

Paper claim: `CPU(%) = 0.0689 × target_feeds + 6.43`, R² = 0.998, slope SE = 0.0016, intercept SE = 1.35, 95% CI slope ± 0.0045, 95% CI intercept ± 3.75.

Recomputed:
- slope = 0.06890 ± SE 0.00163 (95% CI ± 0.00452)
- intercept = 6.4332 ± SE 1.3514 (95% CI ± 3.7515)
- R² = 0.99777 → rounds to 0.998

**Match ✓ all fields.**

### 3.3 CPU LSQ fit (active-based)

Paper claim: `CPU(%) = 0.0697 × active_feeds + 6.28`, R² = 0.998, slope SE = 0.0015, intercept SE = 1.27.

Recomputed:
- slope = 0.06972 ± SE 0.00155
- intercept = 6.2790 ± SE 1.2706
- R² = 0.99804 → rounds to 0.998

**Match ✓.**

### 3.4 RSS LSQ fit (target-based)

Paper claim: `RSS(MiB) = 7.15 × target_feeds + 157`, R² = 0.9999, slope SE = 0.0365, intercept SE = 30.27, 95% CI slope ± 0.10, 95% CI intercept ± 84.

Recomputed:
- slope = 7.1500 ± SE 0.0364 (CI ± 0.1012)
- intercept = 157.009 ± SE 30.253 (CI ± 83.982)
- R² = 0.99990 → rounds to 0.9999

**Match ✓ all fields.**

### 3.5 RSS LSQ fit (active-based)

Paper claim: `RSS = 7.23 × active_feeds + 142`, R² ≈ 1.0, slope SE = 0.0238.

Recomputed:
- slope = 7.2293 ± SE 0.0238
- intercept = 141.52
- R² = 0.99996

**Match ✓.**

### 3.6 Fleet-sizing arithmetic (§6.1)

| Claim | Computation | Paper Value | Verdict |
|---|---|---|---|
| Fit reaches 100% at | (100 − 6.43) / 0.0689 | 1,358 feeds | ✓ (recomputed 1358.1) |
| CPU at 1,000 feeds | 0.0689 × 1000 + 6.43 | 75.3% | ✓ (recomputed 75.33) |
| CPU at 1,050 feeds | 0.0689 × 1050 + 6.43 | 78.8% | ✓ (recomputed 78.78) |
| CPU at 1,250 feeds | 0.0689 × 1250 + 6.43 | 92.5% | ✓ (recomputed 92.56) |
| CPU at 2,000 feeds RSS | 7.15 × 2000 + 157 | 14,457 MiB (92.5%) | ✓ (14457 MiB, 92.52%) |
| OOM feed count | (15625 − 157) / 7.15 | ≈ 2,163 feeds | ✓ (2163.4) |
| Step-6 VM utilization | 108.26 / 400 | 27.1% | ✓ (27.06%) |
| Cores idle at step 6 | (400 − 108.26) / 100 | "three of four vCPUs effectively idle" | ✓ (2.92 cores, ≈ 3) |
| 12,000 / 1,000 | — | 12 workers | ✓ |
| 12 / 2 | — | 6 VMs | ✓ |

### 3.7 Per-feed ratios (Table 2, Table 3)

| Step | Paper CPU/active | Recomputed | Paper marginal CPU | Recomputed marginal | Paper RSS/feed | Recomputed RSS/feed |
|---|---|---|---|---|---|---|
| 1 | 0.1177 | 0.1177 | — | — | 8.48 | 8.4808 |
| 2 | 0.0949 | 0.0949 | 0.0798 | 0.0798 | 7.78 | 7.7778 |
| 3 | 0.0861 | 0.0861 (rounded from 42.86/498.1) | 0.0772 | 0.0772 | 7.51 | 7.5058 |
| 4 | 0.0768 | 0.0768 | 0.0583 | 0.0583 | 7.45 | 7.4534 |
| 5 | 0.0779 | 0.0779 (77.44/993.2) | 0.0814 | 0.0814 | 7.40 | 7.4037 |
| 6 | 0.0730 | 0.0730 | 0.0629 | 0.0629 | 7.30 | 7.3039 |

All cells match the paper to ≤ 0.6% rounding. ✓

### 3.8 Upload rates per source (Table 6)

Feed-minutes = sum over steps of (feeds-in-step × 15 min):
- bcfy_feeds: 41+103+207+311+414+621 = 1,697 × 15 = 25,455 ✓ (paper 25,455)
- bcfy_calls: 55+138+276+414+552+828 = 2,263 × 15 = 33,945 ✓ (paper 33,945)
- openmhz: 4+9+17+25+34+51 = 140 × 15 = 2,100 ✓ (paper 2,100)
- Total: 61,500 ✓

Upload rates:
- bcfy_feeds: 120,139 / 25,455 = 4.7196 → 4.72 ✓
- bcfy_calls: 9,767 / 33,945 = 0.2877 → 0.29 ✓
- openmhz: 54,243 / 2,100 = 25.830 → 25.83 ✓
- Total objects: 120,139 + 9,767 + 54,243 = 184,149 ✓

Throughput: 184,149 / 92 min = 2,001.6 uploads/min → "~2,000 uploads/minute at peak" ✓

### 3.9 Reliability claims (§5.7)

- Paper: "167/621/15 = 0.018 exits/feed/min ≈ one per feed per hour"
  - Recomputed: 167 / 621 / 15 = 0.01793 → 0.018 ✓
  - 0.01793 × 60 = 1.076/hour → "one per feed per hour" ✓
- Paper: ffmpeg 166 × code 8 + 1 × code 234 = 167 total — matches Cloud Logging context (166 code=8, 1 code=234).
- Paper: 898 bcfy_calls warnings, all 403 — matches Cloud Logging context.

### 3.10 Table 4 (event-loop health) cross-check

Context-provided values vs paper:
- drift p99 = 7.0 ✓
- drift p99.5 = 1,290 ✓
- drift p99.9 = 9,725 ✓
- Max = 9,725 (at 02:54:45Z, post-step-6 measurement) ✓
- n = 550 ✓
- Counts: >50 ms = 4, >1s = 3, >5s = 2 ✓ (paper: >50 ms = 4, >100 ms = 4, >1s = 3, >5s = 2 — the >100 ms count of 4 is identical to >50 ms count, consistent with the drift histogram bimodality)

Per-step within-measurement-window drift p99 (from stats.json `loop_health`):
- Step 3: 1.46 ms ✓
- Step 4: 3.10 ms ✓
- Step 5: 2.50 ms ✓
- Step 6: 8.60 ms ✓

Paper's "1.46 ms (step 3) to 8.60 ms (step 6)" range holds for the stats.json-covered steps (3–6). Steps 1–2 are absent from stats.json; this is an incidental data-coverage gap rather than an error, but see P2-obs-2 below.

### 3.11 Table 5A/5B (GCS upload latency)

Cloud Logging context values match Table 5A exactly (p50=56.5, p75=77.2, p90=772.4, p95=3,527.2, p99=4,108.8, max=10,420.1, mean=404.0 ✓).

Context: within-measurement-window n=1,734, p50=51, p95=64, 0% >500 ms → Table 5B ✓.

### 3.12 Ramp timing / step-7 activation burst attribution

- Ramp window 01:23:30 → 02:55:00 = 92 min (paper: "92 min") ✓
- Step 6 measurement window 02:44:20Z → 02:54:20Z (600s) ✓
- Step 6 "steady" declared at 02:54:31Z (ramp.log line 41) ✓
- Step 7 activation started 02:54:31–32Z (ramp.log lines 42–43) ✓
- 9.7s drift max at 02:54:45Z: 25s after step 6 measurement ended, 13–14s into step-7 activation burst ✓
- Step 7 target composition 828:1104:68 → delta from step 6: 207+276+17 = 500 feeds ("~500 additional feeds") ✓

**Phase 3 verdict:** Every numeric claim reproduced from raw sources to within rounding. No fabrication, no over-reporting, no under-reporting.

---

## Phase 4 — Originality Check

Scanned the paper for copy-paste-looking text without attribution. No passages show signature of uncredited external sourcing. Technical descriptions of asyncio, docker stats, cgroups, and n2 machine types are paraphrased in the author's voice and directly cited to [1]–[5]. The uvloop "2–4× improvements" characterization is explicitly attributed as a vendor claim with [3] citation, not presented as the paper's measurement.

**Phase 4 verdict:** No originality concerns.

---

## Phase 5 — Claims Consistency & Resolution Check

### 5.1 Does the per-workload-coefficient reframing hold up?

Yes. The reframing is cleanly executed across:
- Title: "Per-Feed Cost Coefficients for a Multi-Source Asyncio Audio Ingestion Pipeline" (line 1)
- Abstract (line 7): leads with measured coefficients, CIs, and R²
- §1 Intro (lines 13–15): central question is "per-feed CPU and memory cost". "That asyncio pins Python-level work to a single OS thread is well known; what is *not* known a priori is how expensive a particular feed is to carry on that thread."
- §1 Contributions (lines 17–22): (1) coefficients with CIs, (2) workload-mix-specific saturation, (3) pre-flight artifact, (4) fleet-sizing translation.
- §8 Conclusion (line 363): "The paper's contribution is the coefficients and their confidence intervals, the workload-mix-specific saturation point, the pre-flight validation methodology, and the fleet-sizing translation — not a rediscovery of asyncio's single-threaded property."

Phrasing is consistent. Claim holds.

### 5.2 Is the 9.7s stall correctly attributed?

Yes. §5.4 (lines 225–228) attributes the 9,725 ms drift maximum to the 02:54:45Z step-7 activation burst (step-6 measurement concluded at 02:54:20Z, step-7 activation at 02:54:31Z). Timestamp arithmetic checks out. The paper explicitly separates within-step stationary drift p99 (≤ 8.6 ms) from the post-measurement activation transient. §7 Limitation 5 acknowledges the lack of root-cause diagnostics (GC, cgroup throttle, long-blocking syscall) for the outlier.

### 5.3 "Three of four vCPUs effectively idle" vs 27% VM utilization

Consistent usage across abstract, §5.2 "Key finding — stranded capacity" (line 173), and §8 Conclusion (line 359). The abstract says "roughly three of four vCPUs effectively idle"; §5.2 says "approximately 73% of the 400% total CPU is unused, equivalent to three of four vCPUs effectively idle"; §8 says "approximately three of four vCPUs effectively idle at step 6". The "approximately/effectively" hedging is appropriate: 108.26/400 = 27.06%, so 72.94% of VM is unused = 2.92 cores idle ≈ 3 cores.

### 5.4 Multi-process "modeled not validated" disclosure

Consistent across five places:
- Abstract (line 7): "multi-process scaling is modeled but not empirically validated in this experiment"
- §1 Contribution 4 (line 22): "explicit separation between empirically-measured single-worker density and the modeled (not validated) multi-process-per-VM density"
- §6.1 Table 7 footnote (line 305): "The 2-workers-per-VM column is **modeled**, not empirically measured (§6.4, §7)"
- §6.4 Multi-process paragraph (line 323): "**experimental validation on a single VM is future work; this paper does not demonstrate multi-process scaling empirically.**"
- §7 Limitation 2 (line 337): "No empirical multi-process validation…**modeled projections, not measurements**"
- §8 Conclusion (line 361): "*modeled, not measured*, and requires a dedicated validation run"

The §6.4 paragraph also explicitly flags the RSS-vs-cgroup fit problem ("two ~10.8 GiB RSS processes against a 16 GiB VM … exceeds the cgroup limit — implying 2 workers × 1,000 feeds per VM will not fit at the current RSS coefficient, and a multi-process configuration must therefore accept fewer feeds per worker"). This is an honest acknowledgment that the naive 6-VM packing wouldn't even be feasible at 2 × 1,000 feeds/VM given the RSS scaling.

### 5.5 Stage 2.5 P1 resolution audit

| Original P1 Issue | Resolution in Revised Paper | Verdict |
|---|---|---|
| LSQ label mismatch (`0.073x + 3.5` → `0.069x + 6.43`) | Abstract, §5.2, §5.3, §6.1, §8 all use `0.0689 × feeds + 6.43` (CPU) and `7.15 × feeds + 157` (RSS), with SEs and 95% CIs reported | ✓ Fully Resolved |
| 75% stranded contradiction | Replaced with "27.1%" / "approximately 73%" / "three of four vCPUs effectively idle" across all mentions | ✓ Fully Resolved |
| Deficit "1–5 below targets" understated step 6 | §5.1 now says "1–19 below targets, growing monotonically from 1 at step 1 to 15–19 at step 6"; Table 1 adds active (min–max) column with 1,481–1,485 for step 6 | ✓ Fully Resolved |
| Abstract saturation framing | Rewritten: "approaches saturation near 1,000 feeds (77.4%) and exceeds one-core capacity at 1,500 feeds (108.3%)" | ✓ Fully Resolved |
| 1,250 upper bound violates 77–80% target | §6.1 (line 296) explicitly retracts: "we retract the 1,250 upper bound because the fit predicts 92.5% single-core at 1,250 feeds, well above the 77–80% target". Replaced with 1,000 / ~1,050 recommendations in Table 7, §6.1, §8 | ✓ Fully Resolved |

All five Stage 2.5 P1 issues are resolved with no residual inconsistencies.

---

## 7-Mode Failure Checklist

| # | Mode | Verdict | Evidence |
|---|---|---|---|
| 1 | Citation hallucination | **NOT_OBSERVED** | All 9 `file:line` code citations verified against repository (Phase 2). All 5 references verified for canonical plausibility (Phase 1). Cloud Logging timestamps and event counts cross-checked with context-provided summaries. |
| 2 | Implementation bug presented as insight | **NOT_OBSERVED** | The central "insight" — that a single-threaded event loop saturates one core before exhausting a 4-vCPU VM — is architectural, well-understood, and the paper explicitly frames itself as measuring the cost, not claiming the architectural property as novel (lines 15, 363). The 9.7s drift event is correctly attributed to a known transient (mass lease acquisition), not spun as an insight about steady-state behavior. The ffmpeg exit rate is characterized as routine Icecast disconnect behavior, not a bug dressed as a finding. |
| 3 | Hallucinated results | **NOT_OBSERVED** | Every numeric in Tables 1–7, abstract, and §5–§8 reproduces from metrics.tsv / ramp.log / stats.json / Cloud Logging to within rounding (Phase 3). No numbers appear without a traceable source. |
| 4 | Shortcut reliance | **NOT_OBSERVED** | The paper does not rely on a proxy metric that masks the real question. `docker stats` container CPU is the appropriate instrument given the cgroup convention (Appendix A.1 explicitly explains the one-core normalization). The 30-s sampling cadence is called out (§3.1). Event-loop drift p99 within steps is reported for all instrumented steps. |
| 5 | Bug-as-insight | **NOT_OBSERVED** | (Closely related to Mode 2.) No instance identified. The reported 142.66% step-5 transient is flagged as a transient and excluded from the headline; the bimodal GCS upload distribution is correctly decomposed into warmup/activation vs. measurement-window distributions rather than passed through as a steady-state observation. |
| 6 | Methodology fabrication | **NOT_OBSERVED** | Methodology steps (6-step ramp, 5 min warmup, 10 min measurement, 30s sampling, 7-gate pre-flight) are concretely described and corroborated by `ramp.log` timestamps. The `awk`-based abort comparator is explained with provenance (lesson from abandoned run); the pre-flight gates are enumerated. No invented experimental procedures. |
| 7 | Pipeline-level frame-lock / overstated generality | **NOT_OBSERVED** | The paper is explicit about scope limits: single run, single VM, single day, single workload mix (41:55:4), single region. §2.2 explicitly flags the coefficients as mix-specific. §6.1 / §6.4 / §7 repeatedly separate measured single-worker density from modeled multi-process density. §7 Limitation 3 scopes the coefficients to the measured mix. No generality leak from "this pipeline at this mix" to "asyncio in general". |

No SUSPECTED findings. No INSUFFICIENT_EVIDENCE flags on the discriminating modes (1, 3, 5, 6). Pipeline does not block.

---

## Residual Issues

| ID | Severity | Location | Issue |
|---|---|---|---|
| P2-obs-1 | P2 | Abstract ("69% of the 15 GiB cgroup limit") | The abstract rounds the cgroup limit to "15 GiB" while §5.3 precisely states "15,625 MiB (15.26 GiB) cgroup limit". The abstract's "15 GiB" is a minor rounding that could read as the nominal VM memory rather than the cgroup limit. Non-blocking — §5.3 carries the precise value and 69.3% utilization. |
| P2-obs-2 | P2 | §5.4 ("≤ 7 ms for all steps except step 6") | `stats.json.loop_health` only contains entries for steps 3–6, so the "all steps except step 6" statement is strictly supported only for steps 3–6. Steps 1–2 drift p99 is not in stats.json. The trend and the in-ramp-window aggregate (drift p99 = 7 ms) do not contradict the claim, but a minor tightening to "steps 3–6" would be more precise. Non-blocking. |

No P0 or P1 issues.

---

## New Issues Introduced by Revision

**None identified.**

- The revised LSQ coefficients (0.0689, 7.15) exactly match recomputation against `metrics.tsv`, with correct SEs, CIs, and R².
- The revised "27.1% / three-of-four-vCPUs" phrasing is internally consistent and arithmetically correct.
- The revised active-based fits (`0.0697 × active + 6.28` for CPU; `7.23 × active + 142` for RSS) reproduce exactly.
- The revised 9.7s drift reattribution uses the correct timestamp (02:54:45Z), the correct step-6 measurement end (02:54:20Z), and the correct step-7 activation start (02:54:31Z), all corroborated by `ramp.log`.
- The revised retraction of the 1,250 upper bound is internally consistent with the new 1,000 / ~1,050 recommendations.
- The revised Table 5A / Table 5B split accurately reflects the Cloud Logging context-provided distributions.
- Multi-process "modeled, not validated" disclosure appears in exactly the places the revision summary claims, and no place understates the caveat.

The revision response document (`/tmp/exp1b_report/response_to_reviewers.md`) mentions "step-5 CoV falls to 9.6%" in one row (P2-2), but the paper itself correctly states 10.7% (which matches the 10.71% recomputed value). The paper is authoritative; the response document's 9.6% is an internal draft-note typo that does not appear in the deliverable.

---

## Final Verdict

**PASS** — zero P0, zero P1 issues. All Stage 2.5 P1 issues are resolved. All Stage 3 P0 reframing/disclosure items are reflected accurately and consistently across the paper. All numeric, code-citation, and reference claims trace cleanly to raw sources. The revision introduced no new integrity defects. Two P2 rounding/precision observations noted; neither blocks publication.

Pipeline may proceed to Stage 5 (finalization).
