# Experiment 1b Paper — Process Record (Stage 6)

**Paper**: `/home/shuojing/watch-duty-repo/radio-transcription/model/data/wildfire_catalog/EXPERIMENT_1B_REPORT.md`
**Process record date**: 2026-04-16
**Pipeline orchestrator**: academic-pipeline v3.2.2
**Entry mode**: mid-entry at Stage 2.5 (user arrived with a 434-line draft plus raw data)
**Output**: this document

---

## Section 1: Paper Creation Journey

### 1.1 Starting state

The user entered the pipeline already carrying a 434-line draft of the Experiment 1b report and a directory of harvested raw data (`/tmp/exp1b_report/metrics.tsv`, `ramp.log`, `stats.json`, Cloud Logging exports). The draft's narrative arc — stepped ramp from 100 to 1,500 feeds on an n2-standard-4, claiming linear scaling and a single-core asyncio ceiling — was coherent, and the associated repository code had been ingested already (`event_loop_monitor.py`, `gcp_helper.py`, `icecast_collector.py`, `storage/settings.py`, `common/logging.py`). Because Stage 2 (writing) was already complete, the pipeline picked up at Stage 2.5 (pre-review integrity verification). The earlier phases — planning (`EXPERIMENT_1B_REDO_PLAN.md`, 153 lines) and execution — were out of scope for the pipeline; their artifacts exist in the same directory as durable context.

### 1.2 Stage 2.5 — pre-review integrity (`integrity_stage2_5.md`)

Verdict: **PASS_WITH_ISSUES → BLOCKED on Mode 3 SUSPECTED**. The integrity agent independently recomputed every number in the draft against raw files and verified all nine `file:line` code citations. Five P1 issues surfaced:

1. The draft labelled `CPU = 0.073 × feeds + 3.5` as "a least-squares fit", but the actual LSQ fit of the six per-step means is `0.0689 × feeds + 6.43` (RSS coefficients similarly drifted: `7.22x + 128` → true LSQ `7.15x + 157`). R² > 0.99 held in both cases, so the line was close to truth, but the methodological label was wrong. This tripped Mode 3 (Hallucinated results) to **SUSPECTED** — not full fabrication (the fit was within 6% of the real coefficients), but the "least-squares" label did not match the line.
2. The draft simultaneously reported "VM utilization remains below 27%" and "75% of VM capacity stranded on idle cores". The arithmetic is 27.07 + 72.93 = 100; "75%" made the totals exceed 100%.
3. "Active feed counts are 1–5 below targets" understated step 6's 15–19-feed deficit.
4. The abstract said asyncio "saturates one vCPU at approximately 1,000 feeds (77.4%)" — 77.4% is approach-to-saturation, not saturation.
5. The 1,000–1,250 feeds/worker recommendation violated the paper's own 75–80% target (1,250 feeds → 92% single-core under either fit).

Per protocol, the SUSPECTED Mode 3 finding triggered a MANDATORY user checkpoint. The user was offered:
- (a) update coefficients to the actual LSQ values with standard errors, or
- (b) soften the "least-squares fit" language to "an approximate linear model, R² > 0.99".

**User chose (a)** — report the actual LSQ fit with SEs and 95% CIs. This upgraded the paper's statistical rigor meaningfully, and set the template for how subsequent revision-stage statistical claims would be handled.

### 1.3 Stage 3 — five-reviewer peer review (`review_stage3.md`)

Panel: R1 Methodologist, R2 Systems Expert, R3 Statistician, R4 Devil's Advocate, EIC. Panel mean **50.75/100**; decision **Major Revision** (alternative: re-target to HotCloud / LASER / HotOS workshop for likely Minor Revision). Roadmap: 3 P0, 12 P1, 7 P2.

Three P0 items framed the crisis:
- **P0-1 novelty** (R4 lead): the headline "asyncio saturates one core" is the definition of asyncio, not a finding. Required reframe around measured per-feed cost coefficients as the contribution, not the single-threaded ceiling.
- **P0-2 multi-process validation**: "halve fleet size by running 2 workers/VM" was a central claim but had no empirical data behind it. Required either a new 2-workers-per-VM ramp or honest demotion to "modeled, not measured".
- **P0-3 9.7-s stall**: one event-loop drift spike of 9,725 ms sat in the steady-state saturation narrative without diagnosis. Required either GC/cgroup/py-spy evidence or explicit reattribution.

R2 (Systems) raised GIL-framing precision, uvloop citation hygiene, and the missing evidence for "ffmpeg management on loop thread = bottleneck". R3 (Statistician) hit the n=6 fit with no SEs, no residual plot, no prediction interval; fit against target rather than measured active feeds; single-max reporting of drift rather than p99.5/p99.9/tail counts. R1 (Methodology) questioned warmup adequacy, replication, and whether pre-flight smoke tests merited "methodological contribution" framing.

### 1.4 Stage 4 — revision (`response_to_reviewers.md`)

User was offered three options:
- (A) Run 15+ hours of new experiments (2-worker-per-VM, per-source decomposition, multi-day replication)
- (B) Retarget to a workshop venue where the current scope is a likely accept
- (C) Revise in place with rewrite-only fixes; acknowledge new-experiment items as Limitations

**User chose Option C**. This constrained the revision scope: the two P0 items requiring new data (P0-2 multi-process, a fraction of P0-3) would be demoted to Limitations, not fabricated.

Revision scope applied: 17 fixed, 4 partially fixed, 6 acknowledged as Limitation, 0 declined. Specific examples of what changed:

- **Title**: "Single-Node Scaling Limits of an Asyncio Audio Ingestion Pipeline" → "Per-Feed Cost Coefficients for a Multi-Source Asyncio Audio Ingestion Pipeline" (line 1 of report).
- **Abstract**: rewritten to lead with the measured coefficients (`0.069 × feeds + 6.43`, `7.15 × feeds + 157`) and their 95% CIs. "Saturates at 1,000 feeds" replaced with "approaches saturation near 1,000 feeds (77.4%) and exceeds one-core capacity at 1,500 feeds (108.3%)". "75% stranded" replaced with "roughly three of four vCPUs effectively idle".
- **§1 contributions**: restructured from a qualitative "we show asyncio saturates one core" to (1) coefficients with CIs, (2) workload-mix-specific saturation point, (3) pre-flight methodology artifact, (4) fleet-sizing translation. The key sentence "That asyncio pins Python-level work to a single OS thread is well known; what is *not* known a priori is how expensive a particular feed is to carry on that thread" (line 15) was the rhetorical pivot.
- **§2.3**: GIL framing sharpened — "The one-loop-per-process constraint is an *architectural* property of asyncio, not a consequence of Python's Global Interpreter Lock (GIL)".
- **§5.2 / §5.3**: both CPU and RSS fit twice — once against targets, once against measured active feeds — with SEs, 95% CIs, and R² to the full precision.
- **§5.4**: 9.7-s drift reattributed to 02:54:45 UTC, 25 seconds after step-6 measurement concluded at 02:54:20 and ~14 seconds into the step-7 activation burst triggered at 02:54:31 (ramp.log line 42). Within-step p99 drift reported as ≤ 8.6 ms.
- **§6.1**: "1,000–1,250 feeds per worker" explicitly retracted. New recommendation: steady-state 1,000 / peak-tolerable ~1,050. Table 7 multi-process column labelled "(modeled)".
- **§6.4, §7 Limitations 1–5**: multi-process, per-source decomposition, replication, connection-pool instrumentation, stall diagnosis all elevated to named limitations with the specific follow-up experiments enumerated.

### 1.5 Stage 3' — verification review (`review_stage3_prime.md`)

Same five-reviewer panel, now evaluating the revised paper. Mean **62.6/100** (**Δ +11.85** vs Stage 3). Decision: **Accept with inline fix → proceed to Stage 4.5**. Per-item verification: 21/27 Adequately Addressed, 6 Deferred as acceptable Limitation, 0 Not Addressed, 0 Incorrectly Addressed. One minor numerical drift introduced by the revision: §5.1 stated "step-5 CoV falls to 9.6%" after outlier removal; recomputed value is **10.7%**. This was fixed inline before Stage 4.5. The early-stopping criterion triggered on the "no P0 remaining" leg; a second revision cycle (Stage 4') was not needed.

Panel evolution:

| Reviewer | Stage 3 | Stage 3' | Δ |
|---|---|---|---|
| R1 (Methodologist) | 58 | 68 | +10 |
| R2 (Systems) | 55 | 66 | +11 |
| R3 (Statistician) | 52 | 68 | +16 |
| R4 (Devil's Advocate) | 38 | 48 | +10 |
| EIC | 50 | 63 | +13 |
| **Mean** | **50.75** | **62.6** | **+11.85** |

R3 gained the most (+16) — the addition of actual LSQ with SEs, active-based refits, drift tail counts, and bimodal breakpoint analysis was dense compared to a pre-revision state that had only R² and a max. R4 moved the least (+10) — the reproducibility-paradox and workload-specificity critiques are intrinsic to the scope, and Option C did not try to rebut them; the paper got credit for acknowledging them honestly in §7, not for resolving them.

### 1.6 Stage 4.5 — final integrity verification (`integrity_stage4_5.md`)

Independent from-scratch verification (not a diff against Stage 2.5 — a fresh five-phase audit). Verdict: **PASS** — zero P0, zero P1. All seven failure modes **NOT_OBSERVED**. All five Stage 2.5 P1 issues fully resolved. Every numeric claim in the revised paper reproduced from raw sources within rounding. No new errors introduced by the revision. Two P2 rounding nits observed (abstract "15 GiB" vs §5.3 "15,625 MiB"; §5.4 "all steps except step 6" strictly supported for steps 3–6 only). Neither blocked publication.

### 1.7 Stage 5 — finalization

User chose Markdown-only delivery (no LaTeX, no DOCX, no PDF). Two P2 polish fixes applied inline; no structural changes. Final paper: 393 lines, 39,753 bytes.

---

## Section 2: Collaboration Quality Evaluation

Scores are calibrated against the anti-pattern of score inflation. Each score cites specific instances of evidence.

### 2.1 Rigor — **82/100**

- Stage 2.5 independently recomputed every numeric claim against raw data (`integrity_stage2_5.md` Phase 3, 14 subsections). All nine `file:line` code citations independently matched to repository lines.
- Stage 4.5 was a from-scratch re-verification, not a diff; it independently re-derived the LSQ coefficients, SEs, CIs, R², and the fleet-sizing arithmetic (`integrity_stage4_5.md` §3.1–§3.12), catching the CoV drift even though Stage 3' had already flagged it.
- Mode 3 SUSPECTED was correctly flagged as a blocking checkpoint even though the line was *close* to truth (within 6% of the real LSQ slope). The protocol of "label mismatch is still Mode 3 SUSPECTED" was honored rather than rationalized away.

Deductions: Stage 2.5 missed the CoV-excluding-outlier arithmetic slip that crept in during the revision (it was Stage 3' that caught it). References [1]–[5] were verified only by URL-pattern plausibility, not by network fetch — appropriate given the offline constraint but limits Phase 1's depth.

### 2.2 Honesty — **88/100**

- Option C required disciplined restraint: the multi-process scaling claim (P0-2) was demoted to "modeled, not measured" in five places (Abstract, §1 Contribution 4, Table 7 footnote, §6.4, §7 Limitation 2, §8) rather than propped up with hand-waving. `integrity_stage4_5.md` §5.4 verified consistency across all five locations.
- The 1,250 feeds/worker upper bound was **explicitly retracted** in §6.1 with reasoning ("the fit predicts 92.5% single-core at 1,250 feeds, well above the 77–80% target") rather than silently edited.
- R4's Devil's Advocate critique was not rebutted; the Stage 3' R4 score rose only because the paper got more honest about scope limits (§7 Limitations 1, 3, 6, 7), not because the underlying concerns were refuted. R4 remained at 48/100, well below accept threshold — not gamed.
- The §5.4 9.7-s drift reattribution was defensible (25s post-measurement, 14s into activation burst) but the paper still named "no root-cause diagnostics (GC, cgroup throttle, long-blocking syscall)" as a §7 Limitation 5 rather than overclaiming certainty.

Deductions: the abstract's "~2,000 uploads/minute at peak" framing slip (Stage 3' §4.2) was not caught until Stage 3' and was left unfixed in the shipped MD — trivial but honest-slip-rate > 0.

### 2.3 Efficiency — **74/100**

- One revision cycle, not two. Early-stopping criterion correctly triggered on "no P0 remaining" after Stage 3'; a Stage 4' second revision was correctly avoided (`review_stage3_prime.md` §8). This saves ~1 full panel cycle.
- Option C selection avoided 15+ hours of speculative new experiments that the user had already made an operational decision not to fund at this pass.
- Stage 4.5 caught two P2 nits that Stage 3' had missed (abstract 15 GiB precision; §5.4 "all steps" scope) — an arguably over-thorough final gate given the P0/P1 cleanliness, but appropriate for a pre-publication gate.

Deductions: the Stage 2.5 recomputation was done inline in the integrity agent rather than by emitting a single Python script the user could re-run — slightly higher token cost than necessary for what is fundamentally `numpy.polyfit(..., full=True)`. The orchestrator's own token budget across Stages 2.5, 3, 4, 3', 4.5, 5 plus this Stage 6 is the dominant session cost; conservative estimate 180k–250k tokens of agent work on top of the orchestrator's own context (roughly 40-agent-turn equivalent). Efficient *for what was delivered* but not pareto-optimal.

### 2.4 Adaptability — **85/100**

- Mode 3 checkpoint: user chose option (a); the pipeline adopted that choice and propagated the LSQ fit (with SEs and CIs) through abstract, §5.2, §5.3, §6.1, §8 consistently, verified in Stage 4.5 §5.5.
- Option C scope-limit: the pipeline did not try to smuggle unmeasured P0-2 multi-process validation through the revision. Both P0-2 and P1-6/P1-7 were transparently disposed as Limitations. Pivot was clean.
- When Stage 3' caught the 9.6% / 10.7% CoV drift, the orchestrator applied the one-character fix inline before handing off to Stage 4.5 rather than opening a new revision cycle — proportional response to a minor arithmetic error.

Deductions: the transition from Mode 3 SUSPECTED-acknowledged (Stage 2.5) to revision (Stage 4) held the revision window narrower than the full reviewer panel strictly warranted — R4's reproducibility-paradox argument would have been stronger addressed than acknowledged. Option C was an adaptation to user constraints, not a refutation of R4.

### 2.5 Clarity — **86/100**

- Each MANDATORY checkpoint was framed with explicit options and tradeoffs. Mode 3 checkpoint: (a) update / (b) soften — not open-ended. Stage 4 revision strategy: (A) new experiments / (B) retarget workshop / (C) revise + limit — concrete alternatives. Stage 5 format: Markdown / LaTeX / DOCX / PDF — named.
- Panel score progression was reported at each stage with deltas (Stage 3' §8 showed +11.85 explicitly).
- `response_to_reviewers.md` provides a 27-row traceability matrix with status (Fixed / Partially Fixed / Limitation / Declined), location, and response — standard R&R document shape that a human reviewer can audit in one pass.

Deductions: the intermediate complexity of "seven failure modes × two stages × user-override tracking" was not summarized for the user mid-stream; it arrived consolidated in this Stage 6. A mid-pipeline summary after Stage 3' would have helped the user understand what the re-review decision hinged on.

### 2.6 Technical depth — **80/100**

- Correctly identified that `docker stats` CPU is cgroup-v2-normalized to one-core (100% = one full core) — appendix §A.1 makes this explicit and names the cgroup v1 vs v2 difference. This is a common measurement-paper failure mode that was pre-empted.
- Correctly distinguished asyncio's one-loop-per-process architectural property from the GIL, per R2's fix in P1-8. This is a precision distinction that many systems reviewers will check.
- R2's back-of-envelope for ffmpeg-callback-dispatch (~40 events/sec at step 6 — "not obviously a 100%-of-one-core workload") was folded into §6.2 of the revised paper as a caveat rather than ignored; the paper does not over-attribute to ffmpeg management without evidence.
- Integrity verification re-derived LSQ with correct df=4, t-critical 2.7764, slope SE = 0.0016, intercept SE = 1.35 — the statistical machinery was correct, not hand-waved.

Deductions: the pipeline did not catch or raise that a py-spy / cProfile sidecar would have cheaply closed R2's ffmpeg-bottleneck question — that insight appears in the paper as a §7 limitation but not as a pipeline-suggested experiment. Also: Mode 3 SUSPECTED was caught by label matching, not by running the LSQ first — a more proactive integrity agent would have recomputed before verifying.

### 2.7 Overall collaboration quality — **82.5/100** (average)

Reading: the pipeline produced a well-calibrated, honestly-scoped paper that gained +11.85 panel points in one revision cycle and passed independent final integrity. It did not over-deliver on items requiring new data, and did not hide that constraint. Remaining weaknesses are in proactive experimental suggestion and in efficiency of the integrity agent's numerical work.

---

## Section 3: AI Self-Reflection Report

### 3.1 What the AI did well

- **Caught the LSQ label mismatch in Stage 2.5** (Mode 3 SUSPECTED). This was a genuine methodological-accuracy bug, not a fabrication, and the graduated response (BLOCKED pending user acknowledgement, with two concrete options) was the right calibration. If the user had been rushed into "pass, minor rounding", the paper would have shipped with a false methodological label.
- **Honored Option C scope discipline**. When the user chose to not fund new experiments, the pipeline did not try to synthesize missing evidence or soft-pedal the P0-2 multi-process claim. It demoted multi-process to "modeled, not measured" in five places and let `integrity_stage4_5.md` §5.4 verify the consistency.
- **Independent Stage 4.5 verification**. The final integrity was not a Stage-2.5 diff; it was a from-scratch audit. This caught two P2 precision nits that Stage 3' had missed, and verified the one-character Stage 3' inline fix had actually been applied.
- **Early-stopping correctly triggered**. The pipeline declined to open a Stage 4' second revision cycle after Stage 3' because the residual gap to top-tier threshold (~63 vs ~70–75) was entirely in new-experiment territory, not in re-write territory. Continuing would have burned tokens without closing the gap.
- **Traceability artifact hygiene**. All six intermediate reports (`integrity_stage2_5.md`, `review_stage3.md`, `response_to_reviewers.md`, `review_stage3_prime.md`, `integrity_stage4_5.md`, and this one) exist as separate documents with explicit locations cited throughout. A future auditor can reconstruct every decision.

### 3.2 What the AI could have done better

- **Run LSQ before verifying, not after**. Stage 2.5 Phase 3.5 "Linear fits" caught the `0.073x+3.5` label mismatch by recomputing, but a more proactive agent would have run LSQ on the six means at the start of Phase 3 and then checked whether the paper's stated coefficients matched — rather than reading the paper's claim first. This would be ~20 tokens of Python vs ~2k tokens of narrative comparison.
- **Proactively suggested experiments during Stage 4**. Option C was framed as "revise + limit", but the pipeline could have added "here are three cheap sidecar experiments (py-spy at step 6 for 30s, cgroup.cpu.stat capture, a 2-worker mini-ramp at 500 each) that would close specific P0/P1 gaps without re-running the full ramp". This would have given the user a Cost-B.5 option between "full re-run" and "full limit".
- **Caught the ~2,000/min framing earlier**. Stage 3' §4.2 noted "~2,000 uploads/minute at peak" is actually the average (184,149 / 92 = 2,002); peak is ~2,200–2,500/min. Flagged as optional polish, shipped unfixed. Stage 4.5 should have either re-flagged or the Stage 5 polish pass should have applied it.
- **Did not summarize mid-pipeline**. After Stage 3', the user had no consolidated view of "what did we lose to Option C vs what did we keep" until this Stage 6. A Stage-3'-exit summary would have helped the user validate the path.
- **Reference verification was URL-pattern only**. No network fetch. All five references are canonical docs (Python, uvloop GitHub, Docker, GCP) so the risk is near-zero, but a stricter pipeline would have attempted fetch and logged 200/404 status.

### 3.3 What the AI got wrong

- **Stage 2.5 passed `0.073x+3.5` with a partial "P1 issue — should report actual LSQ" recommendation rather than treating label mismatch as immediately disqualifying**. The BLOCKED flag was correct per protocol (SUSPECTED triggers user ack), but the issue-table severity was "P1" when a conservative reading would be "this is a fabricated methodological label and should be P0 unless the user explicitly opts out". The difference is semantic but the checkpoint did force the right choice, so the practical outcome was correct.
- **Stage 4 revision introduced a new arithmetic error** (9.6% vs correct 10.7% step-5 CoV-excluding-outlier). This was caught in Stage 3' and fixed inline, but the fact that Stage 4 introduced it at all shows the revision agent's numeric discipline was weaker than the integrity agent's. In a more compressed pipeline without a verification round, this would have shipped.

### 3.4 7-mode failure-mode audit log (consolidated)

See Section 4 below for the full table. Summary: zero CONFIRMED verdicts. Mode 3 SUSPECTED at Stage 2.5 (LSQ label), resolved to NOT_OBSERVED at Stage 4.5 after the user chose option (a) and the actual LSQ fit was propagated. All six other modes CONFIRMED_SAFE at Stage 2.5 and NOT_OBSERVED at Stage 4.5. No user override was requested on any mode; the Mode 3 user decision was a choice between two concrete fixes, not an override of a SUSPECTED finding.

### 3.5 Lessons for future paper-pipeline runs on similar measurement papers

1. **Recompute before verifying**. For any paper with a fit, run the LSQ first, then check the paper. Cheap (~10 lines of numpy), catches the label-mismatch class immediately.
2. **Distinguish "fabricated label" from "fabricated number" explicitly**. Both belong in Mode 3, but they have different severity implications. A label mismatch with a close-to-correct line should trigger a user checkpoint but is usually a 10-minute fix; a fabricated number is a rewrite.
3. **Offer Option B.5** (targeted cheap sidecars) during the revision-strategy checkpoint, not just full-new-experiment vs full-limit. A 30-minute py-spy sidecar often closes a R2-style "cite evidence for the bottleneck" gap without the 15-hour full-rerun cost.
4. **Always name the exact raw-data file and line-range** in the integrity report (e.g., "metrics.tsv rows 95–113 are step 5"). Makes re-verification in Stage 4.5 mechanical.
5. **Early-stop the revision loop when the residual gap is new-experiment scope**. Don't run a Stage 4' if Stage 3' showed Δ > 0 and no P0 remains — further re-writing has diminishing returns, and venue-retarget (workshop) is usually the honest answer.
6. **Pre-emptively raise `docker stats` / cgroup semantics**. Any Python-on-container measurement paper is at risk of the "100% = one core" confusion; an appendix paragraph is cheap insurance.
7. **For papers with six-point linear fits, always report SE + 95% CI + df**. R² alone is noise-hiding at n=6.

---

## Section 4: Failure Mode Audit Log (7-mode, consolidated)

| # | Mode | Stage 2.5 verdict | Stage 4.5 verdict | User override? | Final status |
|---|---|---|---|---|---|
| 1 | Citation hallucination | CONFIRMED_SAFE (9/9 `file:line` verified; 5/5 refs canonical) | NOT_OBSERVED (9/9 re-verified; 5/5 refs re-verified) | N/A | **Clear** |
| 2 | Implementation bug as insight | CONFIRMED_SAFE (v1 openmhz + bcfy_calls + `bc` bugs correctly framed as pre-flight-motivating failure modes) | NOT_OBSERVED (central "insight" is measured per-feed cost, not asyncio's architecture) | N/A | **Clear** |
| 3 | Hallucinated results | **SUSPECTED** (LSQ label mismatch: paper `0.073x+3.5`, true LSQ `0.0689x+6.43`; close but methodologically mislabelled) | NOT_OBSERVED (actual LSQ with SE/CI adopted; Stage 3' and Stage 4.5 both re-verified) | User chose option (a): adopt actual LSQ coefficients with SE/CI | **Resolved** |
| 4 | Shortcut reliance | CONFIRMED_SAFE (all 114 samples included; step-5 transient disclosed, not excluded; step 7 at 2,000 feeds explicitly skipped with justification) | NOT_OBSERVED (per-step CoV columns added, within-window vs full-ramp distributions both reported) | N/A | **Clear** |
| 5 | Bug-as-insight | CONFIRMED_SAFE (167 ffmpeg exit-8 events correctly attributed to Icecast server disconnect, not pipeline defect; 898 HTTP 403 framed as per-call URL TTL) | NOT_OBSERVED (reframed 9.7s drift as activation-burst artifact, not saturation insight) | N/A | **Clear** |
| 6 | Methodology fabrication | CONFIRMED_SAFE (5-min warmup + 10-min measurement + 30s cadence verified against ramp.log timestamps; 19 samples/step × 6 = 114 rows in metrics.tsv) | NOT_OBSERVED (`awk`-vs-`bc` fix, pre-flight gate list verified; no invented procedures) | N/A | **Clear** |
| 7 | Frame-lock / overstated generality | CONFIRMED_SAFE with mild nits (§7 Limitation 1 scoped to one VM / region; abstract "saturates at 1,000" slightly over-specific) | NOT_OBSERVED (Option C reframe resolved: §1 explicitly scoped to "this workload mix", §2.2 caveat, §7 Limitations 1/3/6/7 name the scope bounds; §6.4/§8 multi-process is "modeled, not measured") | N/A | **Clear** |

Final: 7/7 modes clear at Stage 4.5. One mode (Mode 3) was SUSPECTED at Stage 2.5 and resolved to NOT_OBSERVED at Stage 4.5 through a user-directed fix.

---

## Section 5: Deliverables Manifest

| # | Path | Purpose | Lines | Bytes |
|---|---|---|---|---|
| 1 | `/home/shuojing/watch-duty-repo/radio-transcription/model/data/wildfire_catalog/EXPERIMENT_1B_REPORT.md` | Final published paper | 393 | 39,753 |
| 2 | `/home/shuojing/watch-duty-repo/radio-transcription/model/data/wildfire_catalog/EXPERIMENT_1B_REDO_PLAN.md` | Experiment execution plan (pre-pipeline, durable context) | 153 | 12,535 |
| 3 | `/tmp/exp1b_report/integrity_stage2_5.md` | Pre-review integrity report (Stage 2.5) | 278 | 24,054 |
| 4 | `/tmp/exp1b_report/review_stage3.md` | 5-reviewer critique (Stage 3) | 306 | 27,024 |
| 5 | `/tmp/exp1b_report/response_to_reviewers.md` | R&R traceability matrix (Stage 4) | 131 | 18,668 |
| 6 | `/tmp/exp1b_report/review_stage3_prime.md` | Verification review (Stage 3') | 289 | 29,153 |
| 7 | `/tmp/exp1b_report/integrity_stage4_5.md` | Final integrity verification (Stage 4.5) | 305 | 22,311 |
| 8 | `/home/shuojing/watch-duty-repo/radio-transcription/model/data/wildfire_catalog/EXPERIMENT_1B_PROCESS_RECORD.md` | **This document** (Stage 6) | — | — |

Supporting raw data (input, not pipeline output):
- `/tmp/exp1b_report/metrics.tsv` (114 rows, 5.6 KB) — 30-second docker stats samples across six steps
- `/tmp/exp1b_report/ramp.log` (44 lines, 3.4 KB) — step transitions and NOTE triggers
- `/tmp/exp1b_report/stats.json` (2.3 KB) — per-step loop-health percentiles
- `/tmp/exp1b_report/step_summaries.json`, `uploads_per_step.json`, `bcfy_calls_warnings.json`, `event_loop_health.json`, `ffmpeg_exit.json`, `gcs_upload_ok.json`, `gcs_upload_failed.json` — Cloud Logging exports
- `/tmp/exp1b_report/facts.md` — pre-pipeline fact sheet (15 KB)

---

## Section 6: Pipeline Statistics

- **Stages executed**: 6 — Stage 2.5 (integrity), Stage 3 (review), Stage 4 (revise), Stage 3' (re-review), Stage 4.5 (final integrity), Stage 5 (finalize), plus this Stage 6 (process record). Stages 1 (research), 2 (write), 4' (re-revise), and 3'' (final re-review) were not entered: 1 and 2 were done before pipeline entry; 4' and 3'' were pre-empted by the early-stopping criterion.
- **MANDATORY checkpoints honored**: 3 —
  1. Mode 3 SUSPECTED acknowledgement (Stage 2.5 → user chose option (a): adopt actual LSQ)
  2. Review decision (Stage 3 → user chose Option C: revise-in-place, limit-rest)
  3. Format choice (Stage 5 → user chose Markdown only)
- **Revision rounds**: 1 of max 2 (Stage 4 executed; Stage 4' not needed — early-stopping triggered).
- **User checkpoints** (chronological):
  1. Mode 3 SUSPECTED ack + option (a) vs (b) → (a)
  2. Stage 4 revision strategy option A/B/C → C
  3. Stage 5 delivery format → Markdown
- **Token spend estimate**: orchestrator + 4 sub-agents (integrity Stage 2.5, review Stage 3, revision Stage 4, re-review Stage 3', final integrity Stage 4.5, finalize Stage 5) — conservative estimate ~220k–280k tokens of agent work across the pipeline, dominated by the two integrity rounds (dense numeric recomputation) and the two review rounds (five-reviewer panels). Orchestrator context on top: ~100k–150k. Total session: order of 350k tokens.
- **Wall-clock**: one session, mid-April 2026.
- **Panel score delta**: 50.75 → 62.6 (**+11.85**) in one revision cycle.
- **Integrity verdicts**: Stage 2.5 PASS_WITH_ISSUES (BLOCKED→resolved); Stage 4.5 PASS (zero P0/P1). Both produced independent re-derivations of every numeric claim.
- **7-mode audit final state**: 7/7 clear.

---

## Round 2: Experiment 1c follow-up campaign

**Round 2 goal**: close three §7 limitations from the 1b paper (P0-2 multi-process validation, P0-3 9.7-s stall RCA, P1-7 per-source coefficients) within a 10-hour single-day operator budget, to move the revised paper from Major-Revision toward Minor-Revision at a top-tier venue.

**Entry mode**: Phase 0 meta-review at top of a new pipeline cycle — the 1c plan itself was run through academic-pipeline Stage 2.5-scaled integrity + Stage 3-scaled methodology review before the experiment clock started.

### Phase 0 — Plan meta-review

9 P0 issues caught pre-execution: 4 from scaled integrity (benfred/py-spy image nonexistent, invalid gcloud filter syntax, missing --cluster flag, wrong code-path reference) + 5 from methodology review (py-spy fails on COS, PgBouncer max_pool_size=8, scripts not pre-written, thin abort criteria, no data backup). Cost ~45 min; saved >1 hr of in-experiment debugging. User decisions applied: skip py-spy (COS blocks host install); pre-write all scripts to /tmp/; fold abort + backup into scripts.

### Phase PF — 26-check pre-flight

All 26 checks passed or documented. Key findings: PgBouncer transaction-mode max_pool_size=8 (codebase audit: pooler-safe, zero LISTEN/advisory_lock/PREPARE); no CPU limit on containers (nr_throttled structurally zero); JWT native rotation working (laptop sync workaround obsoleted and killed); Python 3.13.13, no uvloop, default asyncio. Pass document: `preflight_1c_pass.md`.

### 1c.A — Multi-container validation + stall RCA

Two containers on same n2-standard-4, 1,002 feeds at 41:55:4. **Result**: sum CPU 85.2% vs predicted 85.7% (−0.5% residual); sum RSS 7,171 MiB vs predicted 7,418 (−3.3%). Claim validated. **Stall reproduced**: 14.5s and 15.5s drift spikes at t+18s after simultaneous activation, coincident with cgroup CPU >100% on both containers during mass ffmpeg subprocess creation. Attribution: event-loop starvation under subprocess-spawn storm. Steady-state clean (p99=2.98ms, 0 spikes >50ms).

### 1c.B — Per-source decomposition

Three mono-source ramps (3 steps × 10-min measurement each):

| Source | Steps | CPU slope (%/feed) | RSS slope (MiB/feed) |
|---|---|---|---|
| bcfy_feeds | 200/500/800 | 0.156 | 16.9 |
| openmhz | 40/80/120 | 0.100 | 2.8 |
| bcfy_calls | 200/500/1000 | 0.009 | 0.4 |

Retrospective additive validation against 1b step 5: predicted 73.7% vs observed 77.4% (−4.9% residual). Consistent with small cross-source interaction or between-day variance.

### Round 2 pipeline statistics

- Phase 0 meta-review: 45 min (9 P0 caught)
- Phase PF: 30 min (26 checks, 0 P0)
- 1c.A: 35 min wall-clock (including orchestration-script restart)
- 1c.B: 2h25m (B3→B1→B2 sequential)
- Analysis + paper update: ~1h
- Total: ~5h of 10h budget used
- Data loss events: 0
- Abort events: 0
- Orchestration-script restarts: 1 (data integrity preserved)

### Updated limitations

Items 2, 3, 5 moved from unbounded to bounded. 6 new items added (11–16): additive-model hypothesis, allocator bracket, machine-type A/B, PgBouncer queueing, stationarity window, burst sampling probability.

---

## Round 3: Academic-pipeline editorial pass on 1c-extended paper

**Round 3 goal**: close the cross-section inconsistencies introduced when 1c content was grafted onto the 1b paper without updating abstract/§1/§2.2/§6.4-title; run a fresh 5-reviewer Stage 3 panel on the 1c-extended paper; absorb panel feedback; final integrity pass.

**No new experiments.** Pure editorial + reviewer-panel + revision pass. Entry at Stage 2.5 per academic-pipeline mid-entry protocol.

### Stage 2.5 — integrity on 1c-extended paper

Integrity agent found 5 P0 cross-section inconsistencies + 1 P1 citation + 2 P2 polish items. Mode 1 and Mode 3 both NOT_OBSERVED — no pipeline block. Specific P0s: abstract's "modeled but not empirically validated"; §1 "we do not empirically validate / isolate / diagnose"; §2.2 "we do not run per-source-type decomposition"; §6.4 title "(and What This Paper Does Not Validate)"; §1 contribution list only 4 items (missing 1c's 3 new contributions). P1: Jain cited as `[8]` when `[8]` is Heiser; Jain is `[6]`. All findings documented in `integrity_stage2_5_round2.md`.

### Stage 3 — 5-reviewer panel (fresh on 1c-extended paper)

Panel mean **65.8/100** (Round 1 was 62.6; Δ+3.2). Editorial Decision: **Major Revision at top-tier / workshop-ready at HotCloud or LASER**.

Per-reviewer scores:

| Reviewer | Round 3 | Round 1 | Δ |
|---|---|---|---|
| R1 Methodologist (25%) | 69 | 68 | +1 |
| R2 Systems Expert (20%) | 68 | 66 | +2 |
| R3 Statistician (20%) | 66 | 68 | **−2** |
| R4 Devil's Advocate (15%) | 58 | 48 | **+10** |
| EIC (20%) | 66 | 63 | +3 |

R3 dropped −2 because the 1c.B per-source decomposition is both the biggest new contribution and the weakest statistical ground (n=3 bootstrap CIs with 0-lower-bound degeneracy cannot reject zero slope). R4 rose +10 because 1c substantively addressed the Round-1 novelty and validation objections.

P0 items for revision: (1) fix Stage 2.5 cross-section drift; (2) sharpen n=3 bootstrap-CI disclosure; (3) adjust §8 k=2-per-VM scope.

Full panel at `review_stage3_round2.md`.

### Stage 4 — Revision

Applied 18 traceable changes tracked in `response_to_reviewers_round2.md`:
- **Abstract rewritten** (≈380 words) — includes per-source slopes, multi-process validation, stall attribution, explicit "what we do not bracket"
- **§1 contributions expanded 4 → 7** — added per-source decomposition, multi-process-at-k=2, stall-class attribution; reframed "we do not claim" as "what we do not bracket" scope statement
- **§2.2** — forward-reference to §5.8
- **§5.4** — added 5-row alternative-mechanism exclusion table (CFS throttling, slow callback/GC, I/O wait, getaddrinfo, PgBouncer, kernel page-table); attribution survives the comparison
- **§5.8** — added bootstrap-degeneracy disclosure paragraph (~11.1% of resamples produce slope=0 at n=3); treated point estimates as primary statistic
- **§6.2** — acknowledged §5.4 attribution; scoped steady-state 100% breakdown as future py-spy work
- **§6.4 title** — removed "(and What This Paper Does Not Validate)"
- **§7 item 3** — Jain citation `[8]` → `[6]`
- **§7 item 17** — added USL-vs-LSQ deferral
- **§7 "Future work refinements" paragraph** — residual plot, prediction interval, sub-second aliasing, posix_spawn/fork footnote
- **§8** — "under the k=2 configuration" qualifier; point-estimates-with-honest-uncertainty framing

### Stage 3' — Verification re-review

Verdict: **Accept-with-inline-fix**. Panel mean projected **68.7/100** (Δ+2.9 from 65.8 baseline; within 68–70 target band). 18/18 traceability items verified. No new issues introduced. Three copy-edit nits flagged as non-blocking. Documented at `review_stage3_prime_round2.md`.

### Stage 4.5 — Final integrity (MANDATORY)

Verdict: **PASS**. One P1 caught and fixed inline: the bootstrap-degeneracy figure in §5.8 said "~3.7%" but the correct combinatorial quantity is P(any-index all-same) = 3/27 = 1/9 ≈ 11.1%. Fixed to "11.1% of resamples (3/27 = 1/9)". Empirical simulation of 100,000 bootstrap samples confirmed 11.15% produce degenerate slope. Argument preserved (higher degeneracy rate actually strengthens the case for treating point estimates as primary). All 7 failure modes NOT_OBSERVED. Documented at `integrity_stage4_5_round2.md`.

### Cumulative panel-score trajectory

| Round | Stage | Panel mean | Decision | Δ |
|---|---|---|---|---|
| 1 (1b) | Stage 3 | 50.75 | Major Revision | baseline |
| 1 (1b) | Stage 3' | 62.60 | Accept with inline fix | +11.85 |
| 2 (1c extension) | Stage 4.5 only | — | Integrity PASS | (no formal panel) |
| 3 (editorial) | Stage 3 | 65.80 | Major Revision (workshop-ready) | +3.20 |
| 3 (editorial) | Stage 3' | **68.70** | **Accept with inline fix** | +2.90 |

### Venue posture after Round 3

- **Workshop now** (HotCloud 2026 / LASER 2026): ready to submit. Editorial coherence and Round-3 panel both support this.
- **Top-tier** (OSDI / SOSP / EuroSys 2027): requires Phase 2 experimental campaign per §7 items 2, 3, 11, 12, 13, 15, 17. Estimated 30–40 hours of new experiments across ≥ 5-level per-source ramps, k=4+ multi-process bracket, gunicorn A/B, n2/e2 A/B, allocator bracket, multi-day replication, CFS-throttling-under-CPU-limit. Deferred by author decision — this paper can land as-is at a workshop.

### Round 3 pipeline statistics

- Phase: Stage 2.5 → 3 → 4 → 3' → 4.5 → (5 skipped for MD-only) → 6
- Token spend estimate: ~350k across integrity agent, 5-reviewer-panel agent, revision (mix of agent + direct edits due to agent rate-limit mid-session), verification agent, final-integrity agent, and this process-record entry
- Wall-clock: ~2 hours in one session
- New artifacts: 5 under `EXPERIMENT_1B_ARTIFACTS/` (integrity_stage2_5_round2, review_stage3_round2, response_to_reviewers_round2, review_stage3_prime_round2, integrity_stage4_5_round2)
- Paper line count: 509 → 531 (+22 lines; +4.3%)
- 7-mode failure checklist final state: 7/7 NOT_OBSERVED
- P0/P1 post Stage 4.5: 0/0

---

*End of Round 3 Process Record.*
