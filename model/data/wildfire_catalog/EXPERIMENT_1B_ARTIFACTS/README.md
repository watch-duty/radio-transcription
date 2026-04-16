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
