# Framing-context A/B — decision report (framing_context_05_2026)

Scored **1042** segments; **746** (71.6%) framed (per-segment gate analysis below restricted to framed segments).

## Aggregate WER (canonical: total errors / total words)

| arm | all GT segments | framed only |
|---|--:|--:|
| `chirp_baseline` | 28.01% | 27.57% |
| `chirp_moderate` | 26.83% | 25.97% |
| `gemini_baseline` | 31.62% | 30.78% |
| `gemini_moderate` | 32.31% | 31.72% |

## chirp  —  ❌ do NOT ship

Framed segments: 746. Metric: **capped WER** (per-segment min(wer,100)). improvement = baseline − moderate (positive = framing better).

**Gates:**
- ❌ `hard_short_clip`
- ✅ `net_signal`
- ✅ `per_group`
- ✅ `over_insertion`

**Aggregate improvement (capped):** +2.34 WER pts (95% CI [+1.22, +3.53])
**Raw (uncapped) WER:** baseline 38.6 → moderate 35.9  ·  **hallucination rate:** 0.0% → 0.0% (pred >2× ref length)

**By GT-word bucket** (improvement, +=better):

| Bucket | n | base WER | mod WER | improvement | 95% CI |
|---|--:|--:|--:|--:|--:|
| 1-2 ⟵ hard gate | 146 | 51.4 | 45.5 | +5.82 | [+2.40, +9.93] |
| 3-5 ⟵ hard gate | 247 | 39.3 | 37.2 | +2.12 | [-0.26, +4.58] |
| 6-10 | 217 | 25.8 | 24.7 | +1.16 | [-0.25, +2.55] |
| 11-20 | 98 | 22.6 | 21.8 | +0.82 | [-0.59, +2.27] |
| 21+ | 38 | 18.8 | 17.7 | +1.08 | [-0.40, +2.70] |

**Short-clip over-insertion:** baseline 1.00 vs moderate 0.89 tokens/seg

## gemini  —  ❌ do NOT ship

Framed segments: 746. Metric: **capped WER** (per-segment min(wer,100)). improvement = baseline − moderate (positive = framing better).

**Gates:**
- ❌ `hard_short_clip`
- ❌ `net_signal`
- ✅ `per_group`
- ❌ `over_insertion`

**Aggregate improvement (capped):** +0.01 WER pts (95% CI [-1.35, +1.35])
**Raw (uncapped) WER:** baseline 46.4 → moderate 51.5  ·  **hallucination rate:** 0.8% → 1.5% (pred >2× ref length)

**By GT-word bucket** (improvement, +=better):

| Bucket | n | base WER | mod WER | improvement | 95% CI |
|---|--:|--:|--:|--:|--:|
| 1-2 ⟵ hard gate | 146 | 48.6 | 50.3 | -1.71 | [-6.16, +2.40] |
| 3-5 ⟵ hard gate | 247 | 43.7 | 42.9 | +0.81 | [-1.79, +3.49] |
| 6-10 | 217 | 28.8 | 28.1 | +0.66 | [-1.12, +2.46] |
| 11-20 | 98 | 24.6 | 25.5 | -0.90 | [-3.68, +1.47] |
| 21+ | 38 | 16.1 | 16.0 | +0.09 | [-1.51, +1.52] |

**Short-clip over-insertion:** baseline 1.31 vs moderate 1.34 tokens/seg
