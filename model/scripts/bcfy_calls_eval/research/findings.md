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

- _(populated by the outer loop once H1 results land)_

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

## Open questions

- Does framing clear the hard short-clip gate, or repeat the lexical regression
  in prose form? (H1)
- Do Chirp and Gemini diverge — does one tolerate framing and the other not?
- Does effect size track metadata richness (H2) or clip length (H3)?
- If framing helps, would richer authoritative fields (Bcfy `group_get`
  tgDescr/tagDescr) help more — worth the unreliable keying? (deferred)
