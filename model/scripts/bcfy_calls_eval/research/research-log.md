# Research log — bcfy_calls framing context

## 2026-05-20 — Bootstrap + provenance pivot + harness

- **Bootstrap.** Question scoped: does descriptive metadata framing improve
  bcfy_calls transcription WER without short-clip regression? Prior art (internal
  lexical-injection experiment) rejected *word-list* context; H1 tests
  *descriptive prose* — the untested condition. Metric: length-bucketed WER +
  decision gates; effect = baseline − moderate WER, 10k bootstrap CIs (seed 42).
- **Provenance investigation (major).** Spent significant effort discovering the
  eval clips' framing source. Original assumption (filename integer = Bcfy
  system sid) was wrong: it's a **talkgroup decimal (tgDec)**; the sid was
  dropped when the set was built. 3 API recovery methods → 0/277. Confirmed with
  the dataset author's collection scripts. The framing context turned out to be
  **embedded in the clips' WAV tags** (ProScan/SDRTrunk). Pivoted the context
  builder to parse WAV metadata. 202/277 (72.9%) clips frame. (`../FINDINGS.md`)
- **Harness built.** 4 arms (gemini+chirp × baseline+moderate); aggressive
  dropped (differed on ~2% of clips). Gemini via Vertex batch
  (gemini-3.1-flash-lite-preview), Chirp via STT-v2 sync recognize (chirp_3),
  production prompts + phrase-hints held constant. decision_gates.py: buckets,
  bootstrap CIs, 4 gates, framed-only. 62 unit tests pass.
- **Decision: drop aggressive → 4 arms** (user-confirmed). Rationale: the
  Moderate-vs-Aggressive contrast is near-empty in this data.
- **H1 experiment launched.** Smoke (50 segs × 4 arms) running; full run +
  decision report to follow. Chirp single-call probe validated the path.

## Decisions pending

- Read smoke health → launch full run → synthesize H1 → outer loop (H2/H3 as
  analyses of the full predictions). ✓ all done.

## 2026-05-20 (cont.) — H1/H2/H3 settled; concluded

- **Smoke** validated the pipeline + caught Gemini hallucination → switched gate
  metric to **capped WER** + hallucination rate.
- **Full run**: Gemini clean (Vertex batch). Chirp's first pass showed -24 WER —
  **diagnosed as a 429 throttle artifact** (409 swallowed-empty segments) via
  raw-output sanity check; fixed (retry/backoff, persist-only-successes), purged
  corrupted predictions, re-ran clean.
- **H1 SUPPORTED**: framing net-improves both (~+2 capped-WER); Gemini clears
  all 4 gates (SHIP), Chirp clears net but not the strict 3-5w gate. Mechanism =
  hallucination control (Gemini). Inverts the prior lexical-injection regression.
- **H2**: system-name is the active ingredient; geo adds nothing. Service: gain
  in law traffic; **fire (WatchDuty's domain) shows no clear gain**.
- **H3 REFUTED (favorably)**: framing helps the shortest clips most, not long.
- **Concluded** → `to_human/CONCLUSION.md` (recommendation: Gemini ship-worthy as
  a short-clip system-name framer, but validate on fire-specific data first).
  Productionization spec deferred per the original experiment spec.
