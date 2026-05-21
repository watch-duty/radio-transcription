# Framing-context A/B — decision report (framing_context_05_2026)

Scored **1042** segments; **746** (71.6%) framed (analysis below restricted to framed segments).

## chirp  —  ❌ do NOT ship

Framed segments: 746. Metric: **capped WER** (per-segment min(wer,100)). improvement = baseline − moderate (positive = framing better).

**Gates:**
- ❌ `hard_short_clip`
- ✅ `net_signal`
- ✅ `per_group`
- ✅ `over_insertion`

**Aggregate improvement (capped):** +2.02 WER pts (95% CI [+0.78, +3.29])
**Raw (uncapped) WER:** baseline 41.6 → moderate 38.9  ·  **hallucination rate:** 0.0% → 0.0% (pred >2× ref length)

**By GT-word bucket** (improvement, +=better):

| Bucket | n | base WER | mod WER | improvement | 95% CI |
|---|--:|--:|--:|--:|--:|
| 1-2 ⟵ hard gate | 162 | 51.9 | 45.4 | +6.48 | [+3.09, +10.19] |
| 3-5 ⟵ hard gate | 241 | 42.1 | 42.0 | +0.10 | [-2.39, +2.60] |
| 6-10 | 211 | 31.2 | 29.1 | +2.06 | [+0.59, +3.58] |
| 11-20 | 94 | 22.0 | 22.6 | -0.55 | [-3.01, +1.65] |
| 21+ | 38 | 22.1 | 20.9 | +1.20 | [-0.74, +3.30] |

**Short-clip over-insertion:** baseline 1.09 vs moderate 1.00 tokens/seg

## gemini  —  ✅ SHIP moderate

Framed segments: 746. Metric: **capped WER** (per-segment min(wer,100)). improvement = baseline − moderate (positive = framing better).

**Gates:**
- ✅ `hard_short_clip`
- ✅ `net_signal`
- ✅ `per_group`
- ✅ `over_insertion`

**Aggregate improvement (capped):** +2.34 WER pts (95% CI [+0.69, +4.01])
**Raw (uncapped) WER:** baseline 68.2 → moderate 52.7  ·  **hallucination rate:** 1.6% → 0.7% (pred >2× ref length)

**By GT-word bucket** (improvement, +=better):

| Bucket | n | base WER | mod WER | improvement | 95% CI |
|---|--:|--:|--:|--:|--:|
| 1-2 ⟵ hard gate | 162 | 63.9 | 59.0 | +4.94 | [+0.00, +9.88] |
| 3-5 ⟵ hard gate | 241 | 55.0 | 51.5 | +3.51 | [+0.76, +6.41] |
| 6-10 | 211 | 39.8 | 38.6 | +1.28 | [-1.19, +3.78] |
| 11-20 | 94 | 28.6 | 31.3 | -2.65 | [-6.68, +0.62] |
| 21+ | 38 | 22.9 | 20.8 | +2.03 | [+0.52, +3.62] |

**Short-clip over-insertion:** baseline 1.67 vs moderate 1.34 tokens/seg

**Flagged systems (CI worse):** Erie County Public Safety
