# Does metadata framing improve bcfy_calls transcription? — Conclusion (corrected)

**Date:** 2026-05-20 · **Eval:** Broadcastify-Calls, 1042 scored / 746 framed (72.9%), 24 states / 36 P25 systems · **Arms:** Gemini 3.1 Flash Lite + Chirp V3, each baseline vs moderate-framing · **WER:** canonical method from `evaluate_transcriptions.ipynb` (ITN digit-split, aggregate).

> This conclusion supersedes an earlier draft that reported "Gemini ships." That was an artifact of a hallucination-prone Gemini baseline (NOTEBOOK jargon prompt, no frequency_penalty) scored with a non-canonical normalizer. Fixed both: V14 dispatcher prompt + production penalties, and the canonical digit-split aggregate WER.

## Answer: it helps the **ASR (Chirp)**, not the LLM (Gemini) — and only modestly.

| | Gemini 3.1 Flash Lite | Chirp V3 |
|---|---|---|
| Baseline aggregate WER | 31.6% | 28.0% |
| + framing aggregate WER | 32.3% (**+0.7 worse**) | **26.8%** (−1.2 better) |
| Per-segment capped improvement | +0.01 [−1.35, +1.35] | **+2.34 [+1.22, +3.53]** |
| net_signal gate | ❌ | ✅ |
| hard short-clip gate (strict) | ❌ | ❌ (3-5w lower CI −0.26 ≈ neutral) |
| hallucination | 0.8% → **1.5%** (worse) | 0% (n/a) |
| **Verdict** | **does not help** | **modest help; misses strict gate by a hair** |

- **Gemini:** with a properly-configured baseline (which doesn't hallucinate), framing adds nothing — aggregate is dead neutral, short clips slightly worse, and it nudges hallucination *up*. The earlier apparent win was framing suppressing hallucination that the right config never produces.
- **Chirp:** framing genuinely helps (aggregate −1.2 WER, net-signal clears), concentrated on the **shortest 1-2w clips (+5.82)** — exactly WatchDuty's traffic. It fails the *strict* hard short-clip gate only because the 3-5w bucket lands at neutral (CI [−0.26, +4.58]).

This matches the literature: contextual biasing helps dedicated ASR models; an instruction-tuned LLM that already follows "transcribe verbatim" gains little from extra prose and can be mildly distracted.

## Findings that shape any rollout (Chirp)

1. **System name is the active ingredient (H2):** "+system" framing gives the effect (+2.90, CI>0); county/state adds little. Keep the framer short.
2. **Law-enforcement traffic drives the gain; fire is unproven:** law (n=607) +2.60 (CI>0); **fire (n=101) +1.03, CI spans 0.** WatchDuty is fire-focused — the fire benefit is *not established*.
3. **Short-clip intervention (H3):** effect peaks at 1-2 words, fades with length.

## Recommendation

- **Under the spec's strict gate, do not ship to either model** as a blanket change. Gemini: framing doesn't help. Chirp: net-positive but the 3-5w bucket is statistically flat.
- **Chirp is the promising direction**, specifically a **shortest-clip (1-2w), system-name framer** — but it needs (a) a fire-specific eval to confirm the benefit on WatchDuty's actual traffic, and (b) a relaxed/again-tested short-clip criterion given 3-5w sits at neutral, not negative.
- **Gemini framing is closed** for this prompt/config.
- Production caveat unchanged: the framing metadata came from eval-only WAV tags; productionizing needs it at ingestion time.

Artifacts: `decision_report.md` (gates + aggregate WER), `h2_h3_analysis.md`, `progress_<date>.html`, `../FINDINGS.md` (provenance), `../research/findings.md` (full narrative incl. the correction box).
