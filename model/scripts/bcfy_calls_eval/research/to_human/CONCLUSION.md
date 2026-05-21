# Does metadata framing improve bcfy_calls transcription? — Conclusion

**Date:** 2026-05-20 · **Eval:** Broadcastify-Calls, 1042 scored segments / 746 framed (72.9%), 24 states / 36 P25 systems · **Arms:** Gemini 3.1 Flash Lite + Chirp V3, each baseline vs moderate-framing.

## Answer: yes — modestly, and it depends on the model and the clip.

Appending a per-call **descriptive framing sentence** (system / agency / service / location, e.g. *"Law enforcement dispatch traffic for SCHP Florence in Chesterfield County, South Carolina on the Palmetto 800."*) to the production prompt **improves WER for both models** (both clear the net-signal gate, ~+2 capped-WER points). This is the **opposite** of WatchDuty's prior *lexical* context-injection experiment, which regressed short clips by over-inserting vocabulary. Descriptive prose ≠ word lists.

| | Gemini 3.1 Flash Lite | Chirp V3 |
|---|---|---|
| Aggregate (capped WER) | **+2.34** [+0.69, +4.01] | **+2.02** [+0.78, +3.29] |
| Hard short-clip gate (strict) | ✅ pass | ❌ (3-5w flat: +0.10 [−2.39,+2.60]) |
| Net-signal / per-group / over-insertion | ✅ ✅ ✅ | ✅ ✅ ✅ |
| Raw WER | 68.2 → **52.7** | 41.6 → 38.9 |
| Hallucination rate | 1.6% → **0.7%** | 0% (n/a) |
| **Verdict** | **SHIP-worthy** | net-positive, fails strict gate |

**Mechanism:** Gemini (an LLM) hallucinates entire fictional dispatch scenes on short noise; framing **reins that in** (the bulk of its gain). Chirp (a dedicated ASR) doesn't hallucinate, so it gets a smaller, real gain concentrated on the very shortest clips (+6.48 on 1-2w).

## Three findings that shape productionization

1. **The system name is the active ingredient (H2).** "+system" framing gives the full effect; adding county/state adds nothing measurable. → a production framer can be short: agency + service + system, **skip the geography**.
2. **The gain is in law-enforcement traffic; fire shows no clear benefit.** Law (n=607): +2.69/+2.33 (CI>0). **Fire (n=101): +0.54/+1.05, CIs span 0.** WatchDuty is fire-focused — so the headline win is driven by the law-heavy eval, and framing's value *on fire traffic specifically is unproven*.
3. **It's a short-clip intervention (H3, prediction refuted favorably).** Effect peaks at 1-2 words, fades by 6-10, slightly negative at 11-20. Fits WatchDuty's short-traffic profile, but it's not a general win.

## Recommendation

- **Gemini:** framing is worth productionizing **as a short-clip, system-name framer** — it clears every gate and cuts hallucination. **Before full rollout, validate on a fire-specific eval set** — the current eval can't confirm the benefit on the traffic WatchDuty actually transcribes.
- **Chirp:** hold under the strict gate (3-5w is statistically flat), but the 1-2w gain is real and harmless — a shortest-clip-only application is defensible if Chirp is the production model.
- **Next (deferred per spec):** a productionization spec — `feed_properties.broadcastify_metadata`, an enrichment step, prompt-builder wiring — written only if the fire-subset validation holds. Note: production would need the framing metadata at ingestion time (the WAV-tag source used here is eval-only).

Artifacts: `decision_report.md` (gates), `h2_h3_analysis.md` (slices), `progress_<date>.html` (charts), `../FINDINGS.md` (data provenance), `../research/findings.md` (full narrative).
