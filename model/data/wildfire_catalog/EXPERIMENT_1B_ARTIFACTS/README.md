# Experiment 1b — Audit Trail Artifacts

Raw data and review artifacts from the ramp run on 2026-04-16 01:23–02:55 UTC. All numeric claims in `../EXPERIMENT_1B_REPORT.md` trace back to these files.

## Raw data

| File | Source | Purpose |
|------|--------|---------|
| `metrics.tsv` | `docker stats` 30s samples | 114 rows (19 per step × 6 steps). Columns: timestamp, step, target_feeds, active_feeds, ffmpeg_count, cpu_pct, rss_mb |
| `ramp.log` | Ramp controller (`/tmp/exp_ramp_v2.sh`) | Step boundaries, warmup/measurement markers, per-step aggregate summaries, NOTE events for single-core saturation |
| `stats.json` | Post-run aggregation | Per-step loop_health stats + other derived tables |

## Review / audit trail

| File | Stage | Purpose |
|------|-------|---------|
| `integrity_stage2_5.md` | 2.5 | Pre-review integrity check. Found 5 P1 issues (LSQ label, 75→73%, deficit 1-19, abstract saturation, 1,250 upper bound). Mode 3 SUSPECTED. |
| `review_stage3.md` | 3 | 5-reviewer panel critique (R1 Methodologist, R2 Systems, R3 Statistician, R4 Devil's Advocate, EIC). Mean 50.75/100. Major Revision. Roadmap of 3 P0 + 12 P1 + 7 P2. |
| `response_to_reviewers.md` | 4 | R&R traceability matrix (27 items: 17 Fixed / 4 Partially Fixed / 6 Acknowledged as Limitation / 0 Declined). |
| `review_stage3_prime.md` | 3' | Verification re-review. Mean 62.6/100 (Δ+11.85). Accept with inline fix (CoV 9.6→10.7%). |
| `integrity_stage4_5.md` | 4.5 | Final independent integrity check. PASS. Zero P0/P1. 7-mode checklist all NOT_OBSERVED. |

## Reproducibility

The paper's LSQ coefficients are directly reproducible from `metrics.tsv`:

```python
import numpy as np
x = [99.0, 248.7, 498.0, 745.5, 993.4, 1483.1]  # avg active feeds per step
y_cpu = [11.65, 23.60, 42.86, 57.28, 77.44, 108.26]  # avg CPU per step
y_rss = [839.60, 1934.34, 3738.62, 5557.25, 7353.34, 10833.90]  # max RSS per step
# Or use target feeds [100, 250, 500, 750, 1000, 1500] for the target-based fit
print(np.polyfit(x, y_cpu, 1))  # slope, intercept
print(np.polyfit(x, y_rss, 1))
```

---

## Experiment 1c follow-up (2026-04-16 21:01–23:46 UTC)

Follow-up campaign on the same VM addressing three §7 limitations (multi-process validation, stall RCA, per-source decomposition). No source-code changes; only orchestration scripts and container env overrides.

### 1c Raw data

| File | Source | Purpose |
|------|--------|---------|
| `metrics_1c_a.tsv` | 2-container `docker stats` 30s samples | 37 rows. Columns: timestamp, container, active, ffmpeg, cpu_pct, rss_mb. |
| `metrics_1c_B1.tsv` | Mono-source ramp (bcfy_feeds 200/500/800) | 3 steps × 19 samples. |
| `metrics_1c_B2.tsv` | Mono-source ramp (bcfy_calls 200/500/1000) | 3 steps × 19 samples. |
| `metrics_1c_B3.tsv` | Mono-source ramp (openmhz 40/80/120) | 3 steps × 19 samples. |
| `cgroup_1c_a.log`, `cgroup_1c_b.log` | Host-side cgroup.cpu.stat 2s samples | Activation-burst monitoring (298 samples each). |
| `ramp_1c_a.log` | 1c.A orchestration log | Includes resume after mid-warmup script restart. |
| `ramp_1c_B1.log`, `ramp_1c_B2.log`, `ramp_1c_B3.log` | Per-source ramp logs | Step boundaries and per-step aggregate summaries. |
| `experiment_1c_ad_report.md` | Combined 1c.A validation + 1c.D stall RCA findings | Primary finding: stall reproduced (14.5s + 15.5s drift); attributed to subprocess-spawn storm. |

### 1c key findings

- **Multi-process validation (§6.4):** 2 containers × ~500 feeds → sum CPU 85.2% (vs 85.7% predicted), sum RSS 7,171 MiB (vs 7,418 predicted).
- **Stall RCA (§5.4):** 9.7s drift reproduced during simultaneous activation; max drift 15.5s at t+18s coincident with cgroup CPU >100% per container.
- **Per-source (§5.8):** bcfy_feeds 0.156%/feed (ffmpeg), openmhz 0.100%/feed, bcfy_calls 0.009%/feed. Additive prediction 78.8% vs observed 77.4% (+1.8% residual).

---

## Round 3 — academic-pipeline editorial review pass (2026-04-16)

Full pipeline pass on the 1c-extended paper to close cross-section inconsistencies (abstract / §1 / §2.2 / §6.4-title) introduced when 1c content was grafted without updating the summary sections. Fresh 5-reviewer Stage 3 panel, revision, verification, final integrity.

| File | Purpose |
|------|---------|
| `integrity_stage2_5_round2.md` | Stage 2.5 integrity: 5 P0 cross-section + 1 P1 citation + 2 P2 polish; Mode 1/3 NOT_OBSERVED |
| `review_stage3_round2.md` | 5-reviewer panel; mean 65.8/100 (R1=69, R2=68, R3=66, R4=58, EIC=66); Major Revision / workshop-ready |
| `response_to_reviewers_round2.md` | 31-row traceability matrix: Fixed / Acknowledged / Declined across Stage 2.5 + Stage 3 concerns |
| `review_stage3_prime_round2.md` | Stage 3' verification: Accept-with-inline-fix; projected panel mean 68.7/100 |
| `integrity_stage4_5_round2.md` | Final integrity: PASS (1 P1 found — bootstrap degeneracy figure — fixed inline); 7/7 failure modes NOT_OBSERVED |

**Panel-score trajectory**: 50.75 → 62.6 (Round 1) → 65.8 → 68.7 (Round 3). Venue: workshop-ready (HotCloud / LASER 2026) as-is; top-tier requires Phase 2 experimental campaign.
