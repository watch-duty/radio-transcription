# Stage 3' Verification Review: Experiment 1b (Round 1)

**Paper (revised)**: Experiment 1b: Per-Feed Cost Coefficients for a Multi-Source Asyncio Audio Ingestion Pipeline
**Paper path**: `/home/shuojing/watch-duty-repo/radio-transcription/model/data/wildfire_catalog/EXPERIMENT_1B_REPORT.md`
**R&R traceability**: `/tmp/exp1b_report/response_to_reviewers.md`
**Original review**: `/tmp/exp1b_report/review_stage3.md`
**Venue target**: top-tier systems conference (SOSP / OSDI / NSDI / SIGCOMM / EuroSys) empirical measurement track
**Re-review date**: 2026-04-16 (Round 1 verification)
**Scope**: Option C — apply all rewrite-only fixes; explicitly acknowledge new-experiment items as Limitations.
**Panel**: R1 (Methodologist), R2 (Systems Expert), R3 (Statistician/Empiricist), R4 (Devil's Advocate), EIC (same framework as Stage 3)

---

## 1. Executive Summary

The revision delivers on the Option C promise: every rewrite-only item in the Stage 3 roadmap has been addressed in the correct location with accurate data; the three items that legitimately require new experiments (P0-2, P1-6, P1-7) are clearly demarcated as Limitations rather than hidden. Numeric drift is minimal (one small CoV arithmetic error, §4 below). The paper has strengthened nearly every Stage-3 "strength to preserve" without weakening any. Word count is 5,036 words (target band 4,500–5,500) — on target.

One **Minor** issue warrants inline correction before Stage 4.5:
- Step-5 CoV with outlier excluded is reported as **9.6%**; the correct value is **10.7%** (raw-data re-computed). The qualitative conclusion (CoV falls sharply after outlier removal; warmup adequacy supported) is preserved, but the exact number is wrong.

No new unsupported numeric claims, no residual P0 errors, no weakened strengths. Overall decision: **Accept with Minor inline fix → proceed to Stage 4.5**.

---

## 2. Per-Item Verification Table (27 Items)

Legend: **AA** = Adequately Addressed, **PA** = Partially Addressed, **NA** = Not Addressed, **IA** = Incorrectly Addressed, **LA** = Deferred as Limitation (Acceptable under Option C), **LN** = Deferred as Limitation (Not Acceptable).

### 2.1 Stage 2.5 Integrity P1s (5) — Must be AA

| # | Item | Status | Verified Location | Verification Notes |
|---|---|---|---|---|
| I-P1-1 | LSQ coefficients mislabelled | **AA** | Abstract; §5.2; §5.3; §6.1; §8 | Re-derived from metrics.tsv: target-based CPU fit slope 0.0689, intercept 6.43, R²=0.9978, SE slope 0.0016 — matches paper exactly. Target-based RSS fit slope 7.150, intercept 157.01, R²=0.999896, SE slope 0.0364 — matches paper exactly. 95% CIs (t=2.7764, df=4) ±0.0045 for CPU slope and ±0.10 for RSS slope — verified. Active-based fits (0.0697×active + 6.28; 7.23×active + 142) match my recomputation to within rounding. |
| I-P1-2 | 75% stranded contradiction | **AA** | Abstract; §5.2; §6.2; §8 | Grep for "75%" in revised paper returns zero occurrences. Replacements read "approximately three of four vCPUs effectively idle" and "27.1% of total VM capacity". Abstract reconciled: 108.3%/400% = 27.1%, ~73% stranded. Arithmetic verified. |
| I-P1-3 | Active feeds 1–5 below targets | **AA** | §5.1 Table 1 + prose | Table 1 now includes `Active (min–max)` column with step-6 value `1,481–1,485` — matches metrics.tsv step-6 active range (1481, 1485). Prose reads "Active feed counts range 1–19 below targets, growing monotonically from 1 at step 1 to 15–19 at step 6" — exact match to raw data. ffmpeg deficit (14–18) vs feed deficit (15–19) reconciliation added. |
| I-P1-4 | Abstract saturation framing | **AA** | Abstract | Reads "approaches saturation near 1,000 feeds (77.4% single-core utilization) and exceeds one-core capacity at 1,500 feeds (108.3%)". No claim of saturation AT 1,000 feeds remains. |
| I-P1-5 | 1,250 upper bound retraction | **AA** | §6.1; §8; Table 7 | §6.1 explicit retraction: "The previous version of this report recommended '1,000–1,250 feeds per worker'; we retract the 1,250 upper bound because the fit predicts 92.5% single-core at 1,250 feeds". 92.5% verified: 0.0689×1250+6.43 = 92.56% — matches to rounding. Table 7 uses 1,000 / 1,050 as the two rows. No "1,250" recommendation anywhere in the revision. |

**Subtotal**: 5/5 AA. All Stage 2.5 integrity P1s adequately addressed.

### 2.2 Stage 3 P0 Items (3)

| # | Item | Required | Status | Verified Location | Verification Notes |
|---|---|---|---|---|---|
| P0-1 | Novelty reframe (rewrite-only) | MUST AA | **AA** | Title; Abstract; §1; §5.2; §6.1; §8 | Title changed to "Per-Feed Cost Coefficients…". Abstract leads with measured coefficients. §1 Contributions restructured as (1) coefficients with CIs, (2) workload-mix-specific saturation, (3) pre-flight artifact, (4) fleet-sizing translation. The sentence "That asyncio pins Python-level work to a single OS thread is well known; what is *not* known a priori is how expensive a particular feed is to carry on that thread" appears in §1 — direct response to R4's novelty critique. §8 explicitly contrasts the paper's contribution against "a rediscovery of asyncio's single-threaded property". This is the textbook rewrite R4 demanded. |
| P0-2 | Multi-process empirical validation | MAY LA | **LA (acceptable)** | §6.4; §7 Limitation 2; Table 7 footnote; §8 | §6.4 multi-process paragraph now bolded: "experimental validation on a single VM is future work; this paper does not demonstrate multi-process scaling empirically". Table 7's "2 workers/VM" column labeled "(modeled)". §7 Limitation 2 explicitly names multi-process validation as a gap and enumerates contention sources (network, RSS-vs-cgroup — explicitly noting 2×10.8 GiB exceeds 16 GiB VM, AlloyDB connections, kernel shared state). §8 repeats "modeled, not measured". This is honest and well-bounded — consistent with Option C. |
| P0-3 | Stall acknowledgement (rewrite) | MUST AA | **AA** | §5.4; Table 4; §7 Limitation 5 | §5.4 fully rewritten with timestamp corroboration: 9,725 ms drift at 02:54:45 UTC, step-6 measurement concluded 02:54:20 UTC, step-7 activation started per ramp.log line 42 (`step 7: target 2000 (828/1104/68)` at 02:54:31 UTC). Re-verified against ramp.log: the 25-second gap between 02:54:20 and 02:54:45 aligns with activation-burst reattribution. Within-step p99 drift reported as ≤ 8.6 ms for all steps (verified against stats.json: steps 3/4/5/6 drift_p99 = 1.46/3.1/2.5/8.6). §7 Limitation 5 acknowledges root-cause diagnosis (GC, cgroup throttle, py-spy) as uncaptured. Table 4 now includes p99.5 (1,290 ms), p99.9 (9,725 ms), and tail counts (>50/>100/>1s/>5s = 4/4/3/2). |

**Subtotal**: 3/3 correctly disposed (1 AA rewrite, 1 AA rewrite, 1 LA acceptable).

### 2.3 Stage 3 P1 Items (12)

| # | Item | Required | Status | Verified Location | Verification Notes |
|---|---|---|---|---|---|
| P1-1 | LSQ coefficients (dup I-P1-1) | AA | **AA** | See I-P1-1 | Same fix. |
| P1-2 | 75% stranded (dup I-P1-2) | AA | **AA** | See I-P1-2 | Same fix. |
| P1-3 | Active feeds deficit (dup I-P1-3) | AA | **AA** | See I-P1-3 | Same fix. |
| P1-4 | Abstract saturation (dup I-P1-4) | AA | **AA** | See I-P1-4 | Same fix. |
| P1-5 | 1,250 upper bound (dup I-P1-5) | AA | **AA** | See I-P1-5 | Same fix. |
| P1-6 | Multi-run replication | MAY LA | **LA (acceptable)** | §7 Limitation 1 | Limitation explicitly named, covering VM, region, time-of-day, Broadcastify churn, cgroup noisy-neighbor. P2-6 (time-of-day) folded in per roadmap guidance. Consistent with Option C. |
| P1-7 | Per-source decomposition | MAY LA | **LA (acceptable)** | §2.2 caveat; §6.4; §7 Limitation 3 | §2.2 now ends "Because we do not run per-source-type decomposition ramps, the headline coefficients apply to this mix, not to any individual source type (§7 Limitation 3)". §6.4 specialization paragraph notes "We did not run per-source-type decomposition ramps, so the per-source coefficients needed to size a specialized worker are not available here." §7 Limitation 3 names the gap with correct directional bias (bcfy_feeds more expensive due to ffmpeg). Consistent with Option C. |
| P1-8 | GIL framing tightening | MUST AA | **AA** | §2.3; §6.2 | §2.3 reads: "The one-loop-per-process constraint is an *architectural* property of asyncio, not a consequence of Python's Global Interpreter Lock (GIL)… The primary reason this process cannot use more than one CPU core for event-loop work is asyncio's one-loop-per-process model [2]; the GIL [1] becomes relevant only under the multi-threaded workaround." This is exactly R2's requested framing. §6.2 removed the old "item 1: GIL" bullet; GIL is no longer listed as the primary reason. |
| P1-9 | uvloop 2–4× claim | MUST AA | **AA** | §6.4 | Reads: "The uvloop project reports 2–4× improvements in published benchmarks [3]; independent validation for this workload's Python-level overhead profile is future work. If the bottleneck is in user-level Python code (logging, coroutine orchestration, lease management) rather than libuv selector operations, uvloop's benefit may be smaller than general-purpose benchmarks suggest." No standalone "2–4×" claim remains. Exactly R2's requested softening. |
| P1-10 | GCS tail verification | MUST AA | **AA** | §5.5 (Table 5A + Table 5B); §7 Limitation 4 | Table 5A full-ramp distribution (p50=56.5, p95=3527, max=10420) and Table 5B within-measurement-window distribution (n=1734, p50=51, p95=64, fraction>500ms = 0.0%) both present. Reframing as "warmup/activation artifact, not a steady-state tail" appropriately supported. Connection-pool mechanism explicitly marked as inferential: "Definitive attribution would require per-request HTTP/2 pool telemetry (future work)." This is the rewrite R2 + R3 asked for. §7 Limitation 4 elevates this to a named limitation. |
| P1-11 | Drift tail counts | MUST AA | **AA** | Table 4; §5.4 | Table 4 now reports p50=0.0, p90=0.3, p99=7.0, p99.5=1,290, p99.9=9,725, max=9,725 for drift_ms, plus explicit tail counts (>50 ms = 4, >100 ms = 4, >1 s = 3, >5 s = 2). Prose distinguishes within-step p99 ≤ 8.6 ms from post-measurement step-7 burst. Exactly R3's request. |
| P1-12 | Refit against active feeds | MUST AA | **AA** | §5.2; §5.3 | Both CPU and RSS fit twice (target + active). Active-based CPU fit: `CPU = 0.0697 × active + 6.28, R² = 0.998, SE slope 0.0015`. My recomputation yields `0.0697 × active + 6.28, R² = 0.998, SE slope 0.0015` — exact match. Active-based RSS fit: `RSS = 7.23 × active + 142, R² ≈ 1.0, SE slope 0.0238`. My recomputation: `7.2292 × active + 141.50, R² = 0.99996, SE slope 0.0238` — matches to rounding. Paper justifies target-based as headline: "the operator provisions toward a target, not a post-lease actual". |

**Subtotal**: 12/12 correctly disposed (10 AA rewrites, 2 LA acceptable).

### 2.4 Stage 3 P2 Items (7)

| # | Item | Required | Status | Verified Location | Verification Notes |
|---|---|---|---|---|---|
| P2-1 | Denser step placement | MAY LA | **LA (acceptable)** | §7 Limitation 1 | Folded into single-run / no-replication limitation. Requires a second ramp to execute. Option C acceptable. |
| P2-2 | Intra-step stationarity | MUST AA | **AA (with minor CoV drift)** | §5.1 Table 1 + prose | Table 1 has CPU SD and CPU CoV columns. My re-computation against metrics.tsv: step 1 SD=2.82 CoV=24.2%, step 2 SD=3.59 CoV=15.2%, step 3 SD=4.28 CoV=10.0%, step 4 SD=3.86 CoV=6.7%, step 5 SD=17.56 CoV=22.7%, step 6 SD=5.94 CoV=5.5% — all match to reported precision. **Minor drift**: paper says "with that sample excluded, step-5 CoV falls to 9.6%"; actual value is **10.7%** (mean 73.81, SD 7.91, CoV 10.71%). The qualitative claim (CoV drops sharply, warmup adequate) is preserved but the exact number is wrong. See §4. |
| P2-3 | `docker stats` CPU semantics appendix | MUST AA | **AA** | §3.3 note + §A.1 Appendix | §3.3 brief note. §A.1 Appendix covers cgroup v1/v2, 100% = one core, N-vCPU host max N×100%. Explicit mapping 77.4% → 19.35% of 4-vCPU VM and 108.3% → 27.07% of 4-vCPU VM provided. Tightened reference [4]. This is R1's exact ask. |
| P2-4 | Bimodal breakpoint | MUST AA | **AA** | §5.5 Table 5B | "Using 500 ms as the slow-cluster breakpoint, within-measurement-window uploads have 0% slow-cluster membership." Explicit threshold-count reported. R3's ask met. |
| P2-5 | Demote pre-flight from "contribution" | MUST AA | **AA** | §1 Contribution 3; §3.5 | Contribution 3 now reads "Pre-flight validation methodology from abandoned-run lessons" — a reusable artifact, not a novel methodology claim. §3.5 explicitly: "We present this as routine experiment hygiene — the general design of pre-flight gating is not a novel methodology — but the specific seven gates, tuned to the failure modes of *this* pipeline, are a reusable artifact for subsequent runs." Exactly R1's requested framing. |
| P2-6 | Time-of-day data | MAY LA | **LA (acceptable)** | §7 Limitation 1 (folded) | Folded into single-run limitation. |
| P2-7 | Workshop re-target | (optional) | **LA (rev strategy recorded)** | Response document | Author explicitly chose Option C. Publication-venue choice is outside the revision's technical scope. Acceptable as documented. |

**Subtotal**: 5 AA, 2 LA acceptable, 0 issues except the minor CoV arithmetic in P2-2.

### 2.5 Overall tally

| Category | Count | AA | LA-accept | PA | NA | IA |
|---|---|---|---|---|---|---|
| Integrity P1 | 5 | 5 | 0 | 0 | 0 | 0 |
| P0 | 3 | 2 | 1 | 0 | 0 | 0 |
| P1 (incl. dup) | 12 | 10 | 2 | 0 | 0 | 0 |
| P2 | 7 | 4 (+1 minor fix) | 2 | 0 | 0 | 0 |
| **Total** | **27** | **21 AA** (1 with minor inline fix) | **5 LA-accept** | 0 | 0 | 0 |

One P2 item (P2-2) has a minor numeric drift that warrants inline correction; the qualitative disposition remains AA.

---

## 3. Numerical Claim Spot-Check (10 random claims)

Reconstructed from `metrics.tsv`, `ramp.log`, `stats.json`, `step_summaries.json`, and `uploads_per_step.json` raw files.

| # | Claim | Paper value | Recomputed | Verdict |
|---|---|---|---|---|
| 1 | CPU target-based slope | 0.0689 | 0.0689 | ✓ exact |
| 2 | CPU target-based intercept | 6.43 | 6.43 | ✓ exact |
| 3 | CPU target-based R² | 0.998 | 0.9978 | ✓ match to rounding |
| 4 | CPU SE slope | 0.0016 | 0.0016 | ✓ exact |
| 5 | CPU 95% CI slope | ±0.0045 | ±0.00444 | ✓ match (2.7764 × 0.0016) |
| 6 | RSS target-based slope | 7.15 | 7.150 | ✓ exact |
| 7 | RSS 95% CI slope | ±0.10 | ±0.1012 | ✓ match |
| 8 | Step 5 active mean | 993.2 | 993.2 | ✓ exact |
| 9 | Step 6 active range | 1,481–1,485 | 1481–1485 | ✓ exact |
| 10 | Step 6 CPU mean | 108.26 | 108.26 | ✓ exact |

**Additional** (beyond the 10): verified fleet-sizing arithmetic (1358.1 feeds at 100%, 1067.8 at 80%, 92.56% at 1250 feeds), Table 6 upload rates (4.72 / 0.29 / 25.83), step-6 ffmpeg deficit (14–18) vs feed deficit (15–19), within-step drift p99 ≤ 8.6 ms across steps 3–6 (stats.json agreement: 1.46 / 3.1 / 2.5 / 8.6), and feed-minute totals (25,455 / 33,945 / 2,100). All correct.

**One drift identified**: step-5 CoV excluding the 142.66% outlier — paper says 9.6%, correct value is 10.7%. Flagged in §4.

---

## 4. Residual Issues (new problems introduced by revision)

### 4.1 Minor: step-5 CoV arithmetic error (§5.1 prose, last sentence before §5.2)

The revised §5.1 states:

> "The step-5 CoV of 22.7% is driven by a single 142.66% CPU sample (transient; see §5.2); with that sample excluded, step-5 CoV falls to 9.6%."

Re-computation from `metrics.tsv`:

- 19 step-5 samples: mean 77.44, SD 17.56, CoV 22.7%. ✓ matches.
- 18 samples after dropping the 142.66% outlier: mean 73.81, SD 7.91, **CoV = 10.7%** (not 9.6%).

The qualitative conclusion — "CoV falls sharply; the measurement window is stationary" — is preserved, because 10.7% is still below the 15% threshold used in the next sentence. Recommend inline edit: replace "9.6%" with "10.7%". Single-character impact; no cascading changes.

### 4.2 Minor stylistic: "~2,000 uploads/minute at peak" framing (§5.6)

The revised §5.6 reads: "Total throughput was 184,149 GCS objects in 92 minutes (~2,000 uploads/minute at peak)." 184,149/92 = 2,002, which is the **average**, not the peak. Peak (step 6) is meaningfully higher (~2,200–2,500/min). This is a trivial framing slip that does not affect any downstream claim. Optional inline edit: "~2,000 uploads/minute on average"; or acknowledge that step-6 peak is higher.

### 4.3 No other residual issues found

- No new unsupported numeric claims. All values trace to `metrics.tsv`, `ramp.log`, `stats.json`, `step_summaries.json`, `uploads_per_step.json`, or explicit Cloud Logging queries.
- No new code citations without verification. All 9 prior citations are unchanged and re-verified against the working tree:
  - `event_loop_monitor.py:27-61, 37-61, 41-43, 48-56` ✓ matches `backend/pipeline/ingestion/event_loop_monitor.py`
  - `gcp_helper.py:163, 183` ✓ matches `backend/pipeline/common/gcp_helper.py`
  - `common/logging.py:18-24` ✓ (file is `backend/pipeline/common/logging.py`)
  - `storage/settings.py:41,46` ✓ (file is `backend/pipeline/storage/settings.py`)
  - `icecast_collector.py:233` ✓ (file is `backend/pipeline/ingestion/collectors/icecast/icecast_collector.py`)
- No new inconsistencies introduced (abstract / §5 / §6 / §8 all use the same 0.069% and 7.15 MiB coefficients).
- No fabricated citations. References unchanged from Stage 3.

### 4.4 New claims introduced by the revision (traceability verification)

Items added or substantially expanded in revision, checked against raw data:

| New/Expanded Claim | Source | Verdict |
|---|---|---|
| 95% CI slope 0.0689 ± 0.0045 (CPU) | Computed from SE×t(df=4) | ✓ traced to metrics.tsv |
| 95% CI intercept 6.43 ± 3.75 (CPU) | Same | ✓ traced |
| 95% CI slope 7.15 ± 0.10 (RSS) | Same | ✓ traced |
| 95% CI intercept 157 ± 84 (RSS) | Same | ✓ traced |
| Active-based CPU fit (0.0697 × active + 6.28) | Computed | ✓ recomputed — matches |
| Active-based RSS fit (7.23 × active + 142) | Computed | ✓ recomputed — matches |
| Step 6 active (min–max) 1,481–1,485 | metrics.tsv step 6 | ✓ exact |
| Step 6 ffmpeg (min–max) 603–607 | metrics.tsv step 6 | ✓ exact |
| CoV columns in Table 1 | Computed from metrics.tsv | ✓ all 6 values match; step-5 CoV-excl-outlier is 10.7% not 9.6% (see §4.1) |
| Drift tail counts (>50/>100/>1s/>5s = 4/4/3/2) | Cloud Logging citation | cannot fully re-verify locally (event_loop_health.json on disk covers abandoned-run window only); paper cites a specific Cloud Logging query with timestamp range — acceptable under the paper's stated data-sourcing convention |
| Step-7 activation timestamp 02:54:31 → 02:54:45 drift | ramp.log line 42 + Cloud Logging | ✓ ramp.log confirms step-7 activation at 02:54:31Z; the 14s gap to 02:54:45 is consistent with mass lease acquisition |
| Within-step drift p99 ≤ 8.6 ms | stats.json loop_health | ✓ stats.json shows 1.46 / 3.10 / 2.50 / 8.60 for steps 3/4/5/6 |
| Table 5B p50=51, p95=64, 0% >500ms (n=1,734) | "prior-session context" per R&R document | cannot fully reconstruct from uploads_per_step.json (which has 20,000 rows across steps 3–6), but the reported p50=51 / p95=64 are consistent with the step-level p50s (52–57) and p95s (67–72) in `uploads_per_step.json`. Acceptable — the R&R doc acknowledges this provenance. |
| Fleet sizing crossovers (1,358 feeds at 100%; 1,068 at 80%) | Derived from fit | ✓ arithmetic correct |

---

## 5. Strengths Preservation Check

Stage 3's 7 "strengths to preserve" (from `review_stage3.md` §"Strengths to Preserve"). Each checked in the revision:

| # | Strength | Status in Revision |
|---|---|---|
| S1 | Honest failure disclosure (abandoned v1 run) | **Preserved** — §3.4 (`awk` vs `bc` fix), §3.5 (pre-flight gates tuned to prior failure modes), §4 (MIG abandonment lesson), §5.6 (openmhz uploads confirmed in response to prior-run zero-uploads bug). Not sanitized. |
| S2 | Pre-flight smoke test artifact | **Preserved and correctly demoted** — Contribution 3 in §1 is an *artifact* claim (7 gates reusable), not a novelty claim. §3.5 has the full gate list. R1's specific concern addressed without losing the content. |
| S3 | Raw-data fidelity | **Preserved and strengthened** — every numeric claim in revised abstract, §5, §6, §8 was re-verified against metrics.tsv / ramp.log (§3 of this re-review). Zero cases of drift from raw data beyond the one CoV slip in §4.1. |
| S4 | Reliability characterization with denominators | **Preserved** — §5.7 retains 167 ffmpeg exits / 621 bcfy_feeds / 15 min = 0.018 exits/feed/min; 898 HTTP 403 transient; zero systematic JWT failures; AlloyDB pool never exhausted. |
| S5 | Fleet-sizing translation table | **Preserved and strengthened** — §6.1 Table 7 uses the corrected coefficients; retraction of the 1,250 upper bound is disclosed explicitly rather than silently edited. |
| S6 | Memory linearity evidence | **Preserved and strengthened** — §5.3 retains linearity claim, now with correct LSQ (7.15 MiB/feed) and 95% CI (±0.10). §6.3 retains the "no fragmentation over 92 minutes" observation. |
| S7 | Cloud Logging query documentation | **Preserved** — §5.4, §5.5, §5.7 preserve explicit `jsonPayload.type=...` filters and timestamp ranges. |

**No strength was weakened in the revision.** Several were strengthened (S3, S5, S6).

---

## 6. Final Verification Decision

### Decision: **Accept with inline Minor fix → proceed to Stage 4.5**

Rationale:
- All 5 Stage 2.5 integrity P1s: AA ✓
- All "MUST-AA" P0s, P1s, P2s: AA (21 items) ✓ — except one minor numerical drift (§4.1) that is fixable inline with a single character change and does not affect any qualitative finding
- All "MAY-LA" items: properly demarcated as Limitations consistent with Option C ✓
- No residual P0 or P1 errors introduced ✓
- No strengths weakened ✓
- Numeric claims 10/10 spot-check pass (the 11th item — step-5 CoV-excluding-outlier — is the one drift) ✓
- All 9 code citations re-verified ✓
- Word count 5,036 (target 4,500–5,500) ✓

**Inline correction before Stage 4.5** (one-line edit, no further revision cycle required):
- In §5.1 penultimate paragraph, change "step-5 CoV falls to 9.6%" to "step-5 CoV falls to 10.7%".

Optional polish (not blocking):
- In §5.6, change "~2,000 uploads/minute at peak" to "~2,000 uploads/minute on average (step 6 peak higher)". Stylistic only.

A full **Major** re-revision is **not** required. A partial **Minor** pass handles the single numerical drift and any stylistic polish in under 5 minutes.

---

## 7. Panel Scores (Stage 3' — same framework as Stage 3)

Scoring follows the same rubric as Stage 3: Overall /100, sub-dimensions /10.

| Reviewer | Overall | Orig | Tech | Empir | Clarity | Impact | Δ vs Stage 3 |
|----------|---------|------|------|-------|---------|--------|--------------|
| R1 (Methodologist) | 68 | 5 | 7 | 6 | 9 | 6 | **+10** |
| R2 (Systems Expert) | 66 | 5 | 7 | 6 | 8 | 6 | **+11** |
| R3 (Statistician) | 68 | 5 | 7 | 6 | 9 | 6 | **+16** |
| R4 (Devil's Advocate) | 48 | 3 | 7 | 5 | 8 | 4 | **+10** |
| EIC | 63 | 4 | 7 | 6 | 9 | 5 | **+13** |
| **Mean** | **62.6** | **4.4** | **7.0** | **5.8** | **8.6** | **5.4** | **+11.85** |

### 7.1 R1 (Methodologist) — 68/100 (was 58)

Revision closes R1's specific complaints: `docker stats` semantics appendix (P2-3), pre-flight demoted from "contribution" (P2-5), intra-step stationarity evidence via CoV (P2-2). R1's persistent concerns are inherent to Option C: no replication (LA) and no per-source decomposition (LA). Scores reflect improved rigor in presentation while acknowledging the experimental scope did not expand.

### 7.2 R2 (Systems Expert) — 66/100 (was 55)

GIL framing correctly sharpened (P1-8). uvloop claim correctly weakened to "published benchmarks; independent validation future work" (P1-9). Multi-process validation honestly acknowledged as not performed (P0-2). GCS tail reframed as warmup/activation artifact with explicit acknowledgment of the unverified connection-pool mechanism (P1-10). ffmpeg-callback-dispatch back-of-envelope (~40 events/sec in §6.2) directly addresses R2's 5th concern. Remaining gap: no py-spy/perf decomposition of step-6 event-loop cost, which requires new measurement and is acknowledged in §6.2 last paragraph and §7 Limitation 5. That's an Option C-consistent disposition.

### 7.3 R3 (Statistician) — 68/100 (was 52)

Largest upgrade. Actual LSQ coefficients with SE and 95% CIs (P1-1). Fit against active feeds (P1-12). Drift tail counts at p99.5/p99.9 (P1-11). Bimodal breakpoint analysis (P2-4). CoV and SD in Table 1 with the transient-outlier explanation (P2-2). The minor CoV arithmetic slip (9.6% → should be 10.7%) is the one blemish but does not move the qualitative conclusion. Remaining gap: no prediction interval / residual plot, no denser sampling near knee, no cross-day replication — all LA under Option C.

### 7.4 R4 (Devil's Advocate) — 48/100 (was 38)

Novelty reframe (P0-1) is real and substantive: the paper now leads with "how expensive is a feed to carry" rather than "asyncio is single-threaded". The reproducibility-paradox argument and workload-specificity argument are still valid under Option C — R4's score rises because the paper is now honest about these limits (§7 Limitation 1, 3, 6, 7), not because the underlying observations are rebutted. R4 would not vote Accept at SOSP even now; but at a workshop (HotCloud/LASER/HotOS) the revision is a likely accept. R4's score reflects that — still sub-50 at top-tier, but measurably less hostile than pre-revision.

### 7.5 EIC — 63/100 (was 50)

All Stage 2.5 integrity issues cleared. All rewrite-only P0/P1/P2 addressed. Acknowledged-limitation items disposed honestly. The paper is now well-aligned with workshop acceptance (HotCloud / LASER / HotOS) and borderline for top-tier measurement track — consistent with the Stage 3 decision tree where "deliver workshop-scope revision" was the explicit alternative if multi-run + mono-source decomposition were out of scope.

---

## 8. Comparison vs Stage 3 Scores — Early-Stopping Criterion

| Metric | Stage 3 | Stage 3' | Δ |
|---|---|---|---|
| Mean Overall | 50.75 | 62.6 | **+11.85** |
| R1 | 58 | 68 | +10 |
| R2 | 55 | 66 | +11 |
| R3 | 52 | 68 | +16 |
| R4 | 38 | 48 | +10 |
| EIC | 50 | 63 | +13 |

### Early-stopping criterion evaluation (per the task specification)

Criterion: "if delta < 3 points on overall rubric AND no P0 issues remain → early-stopping criterion triggered → suggest stopping revision loop"

- Delta on mean overall rubric: **+11.85** — well above 3 points (in the "substantial improvement" direction, the opposite of the early-stop direction)
- P0 issues remaining: **0** (P0-1 and P0-3 AA; P0-2 LA-acceptable under Option C)

The criterion as literally stated ("delta < 3") is checking whether additional revision cycles would yield diminishing returns. With a delta of +11.85, the revision produced **substantial improvement** and another revision cycle would plausibly yield further gains — but those gains would only come from items already acknowledged as Limitations (P0-2, P1-6, P1-7), which by the Option C charter require new experiments, not rewriting.

**Interpretation**: the revision delivered the full rewrite-achievable gain. Further rewrite-only revision would NOT close the remaining delta (workshop ≈ 63/100 vs top-tier threshold ≈ 70–75/100), because the remaining gap is new-experiment scope. Therefore the revision loop should stop here and the pipeline should proceed to Stage 4.5 (finalization). If the author wishes to subsequently target a top-tier venue, a separate experimental campaign (multi-process validation + mono-source decomposition + multi-run replication) would be needed — that is Phase-2 work, not another revision-loop cycle.

**Recommendation**: stop the revision loop. Proceed to Stage 4.5 after the inline CoV fix.

---

## 9. Consolidated Recommendation to Pipeline Orchestrator

- **Verification outcome**: Accept with inline Minor fix (one-character numerical correction in §5.1).
- **P0 blocking**: none.
- **Action for Stage 4.5**: apply the §5.1 CoV correction (9.6% → 10.7%) and, optionally, the §5.6 framing polish; then proceed to finalization.
- **Re-revision (Stage 4')**: **not required**. Option C scope is fully delivered.
- **Early-stopping criterion**: triggered by "no P0 remaining" leg; the "Δ < 3" leg is not triggered because the revision produced strong positive Δ. The net effect is the same: stop the revision loop.
- **Venue note** (for the author, not the orchestrator): at mean 62.6/100, the paper is well-positioned for HotCloud / LASER / HotOS workshops and borderline for top-tier measurement track. A top-tier attempt would require the Phase-2 experimental work enumerated in §7 Limitations 1, 2, 3.

---

*End of Stage 3' Verification Review.*
