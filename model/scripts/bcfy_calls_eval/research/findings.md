# Findings — Does metadata framing improve bcfy_calls transcription?

_Research narrative + agent memory. Read at the start of every loop tick._

## Research question

Does inference-time **descriptive metadata framing** (system / agency / service /
location as natural-language prose) improve WER on short public-safety scanner
transmissions, for Chirp V3 and Gemini 3.1 Flash Lite, without regressing the
short clips that dominate WatchDuty traffic?

## Current understanding

- **Prior art (internal, decisive):** A previous Gemini autoresearch tested
  *lexical* context injection (per-source mined vocabulary + Chirp's 81-phrase
  domain list) and **rejected it** — short-clip WER got ~0.8 pts *worse*, CI
  crossing 0, from the model over-inserting domain words even with guardrails.
  WatchDuty traffic is mostly short (~1.6 s / ~2.8 GT words), so a lexical
  repeat cannot ship. → This experiment tests the *untested* condition:
  **descriptive framing prose, no word lists.** [[prior-lexical-injection]]
- **The eval set is richer than expected.** 277 unique calls span 24 states /
  36 P25 trunked systems (Palmetto 800, MSCommNet, WISCOM, KSICS, MOSWIN,
  HIWIN, WyoLink, …). Services: 158 law-enforcement, 29 fire, 8 fire/EMS,
  5 paging. Heavily state-police; the fire subset (37) is small but most
  relevant to WatchDuty.
- **Framing coverage is uniform across clip lengths** (69-76% per bucket),
  including the 1-2 and 3-5 word buckets where the hard gate lives — so the
  experiment can actually measure the effect where it matters.

## Patterns and insights

- **Smoke signal (n=40 framed, EXPLORATORY — full run is the confirmatory test):**
  the two models diverge sharply, and the *mechanism* is hallucination control.
  - **Gemini 3.1 Flash Lite** hallucinates entire fictional dispatch scenes on
    short noise (GT "four ten four" → a 96-word fabricated structure-fire
    narrative; raw WER up to 3200%). Descriptive framing **reins this in**:
    raw WER 146.6→89.9, hallucination 5.0%→2.5%, short-clip over-insertion
    4.55→2.32 tokens/seg, and it **passes the hard short-clip gate**
    (+10.0 / +3.97 capped-WER pts on 1-2 / 3-5 word clips). `net_signal` not yet
    cleared at n=40 (aggregate CI lower bound = 0).
  - **Chirp V3** does not hallucinate (0% rate; a dedicated ASR emits short or
    empty on noise), so framing has nothing to rein in → neutral/slightly
    negative, fails the hard gate.
- **This INVERTS the prior internal lexical-injection finding.** Word-list
  injection *increased* over-insertion (parroting); descriptive prose framing
  *decreases* it — at least for the LLM. Consistent with the literature: lexical
  biasing over-triggers on low-context input, while a good context *prompt* can
  reduce an LLM's hallucination (cf. "Whisper: Courtside Edition" — context nets
  improvement but with a per-segment helps-vs-hurts split). [[lit-courtside]]
- **Methodology:** raw WER is unusable as the headline (a few hallucination
  blowups dominate). Gate metric = **capped WER** (per-segment min(wer,100));
  raw WER + hallucination rate reported as the mechanism.
- **FULL RUN — Gemini CONFIRMED (n=746 framed, CONFIRMATORY):** framing clears
  **all four gates → SHIP**. Aggregate capped-WER improvement **+2.34**
  (95% CI [+0.69, +4.01], net_signal clears); short clips +4.94 (1-2w) /
  +3.51 (3-5w); raw WER **68.2→52.7**; hallucination **1.6%→0.7%**;
  over-insertion 1.67→1.34. Only 1 flagged system (Erie County), none blocking.
  The 11-20w bucket dips (-2.65, CI crosses 0) — framing helps short, is ~neutral
  long. H1 supported for the LLM.
- **FULL RUN — Chirp CONFIRMED (clean re-run; n=746 framed):** framing is
  **net-positive** (+2.02, 95% CI [+0.78, +3.29], net_signal clears) and
  **helps the shortest 1-2w clips most** (+6.48, CI [+3.09, +10.19]) — but it
  **fails the strict hard short-clip gate** because the 3-5w bucket is
  statistically *flat* (+0.10, CI [-2.39, +2.60], spans 0). Verdict: do NOT
  ship under the strict gate, but framing is **not harmful** to Chirp (no
  blocking systems; over-insertion 1.09→1.00). Raw WER 41.6→38.9; 0%
  hallucination (a dedicated ASR doesn't fabricate).
  - The first full pass's -24 WER was a **429 throttling artifact** (409
    swallowed-empty segments), caught by sanity-checking raw outputs and fixed
    (retry/backoff). [[429 lesson below]]
- **H1 VERDICT: framing helps, model-dependent strength.** Both models net
  improve (~+2 capped-WER pts, both clear net_signal) — descriptive prose
  framing is beneficial, decisively *not* the lexical-injection regression.
  **Gemini clears all gates (ship); Chirp clears net but not the strict 3-5w
  gate.** Mechanism differs: Gemini's gain is hallucination control (raw WER
  68→53, halluc 1.6→0.7%); Chirp (no hallucination) gets a smaller, real gain
  concentrated on the very shortest clips. The two diverge exactly where the
  spec predicted — and the strict gate is what distinguishes "ship" from
  "promising but unproven on short clips."

## Lessons and constraints

- **Provenance:** the eval filename integer is a **talkgroup decimal (tgDec)**,
  not a system sid. The sid was dropped when the dataset was built, so the
  Broadcastify API cannot recover per-call context (tgDec collides; a
  round-trip mis-resolved Palmetto 800 → "Baltimore County"). The faithful
  per-call source is the clips' embedded scanner-software (ProScan/SDRTrunk)
  WAV tags. See `../FINDINGS.md`.
- **Isolation:** phrase-hint adaptation (Chirp) and every other knob are held
  constant across baseline/moderate so the only variable is the framing prose.
- **Aggressive variant dropped:** it differed from Moderate on only ~2% of
  clips (tags rarely carry a distinct alpha tag + descriptor). 4 arms, not 6.
- **Cost:** a full 4-arm eval is ~$35-100. The predictions are a fixed dataset
  to be re-analyzed (length/service/system/over-insertion), NOT re-run per idea.
- **STT v2 per-minute quota (cost a near-miss false finding):** the `us`
  endpoint throttles Recognize requests per minute; a concurrent eval burst
  returns 429s. Swallowing a 429 as an empty transcript looks EXACTLY like a
  catastrophic framing regression (-24 WER, 41% empty). Always retry 429 with
  backoff and never persist a hard-failure as a prediction. Sanity-checking the
  raw outputs (not just the WER number) is what caught it.

## Open questions

- Does framing clear the hard short-clip gate, or repeat the lexical regression
  in prose form? (H1)
- Do Chirp and Gemini diverge — does one tolerate framing and the other not?
- Does effect size track metadata richness (H2) or clip length (H3)?
- If framing helps, would richer authoritative fields (Bcfy `group_get`
  tgDescr/tagDescr) help more — worth the unreliable keying? (deferred)
