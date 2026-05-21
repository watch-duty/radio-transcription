# Literature Survey: Does inference-time metadata/context framing help ASR of short public-safety radio?

Scope: prior work on contextual biasing, prompt conditioning, and post-ASR correction, focused on the
question — *does providing descriptive natural-language metadata at inference time improve ASR of short
public-safety radio transmissions WITHOUT regressing on short / out-of-context clips?*

13 sources reviewed (all URLs/DOIs verified to resolve). Grouped by theme below.

---

## Theme 1 — Lexical contextual biasing (shallow fusion, deep/neural, attention)

- **Zhao et al. 2019, Shallow-Fusion End-to-End Contextual Biasing** (Interspeech) — canonical shallow-fusion
  biasing of a context word-list during beam search; first E2E system to beat a conventional model with biasing.
  Documents the central failure mode: **over-biasing causes spurious insertions / false triggers** on large or
  irrelevant lists. (`shallow-fusion-contextual-biasing.md`)
- **Gu et al. 2025, PSC-Joint / Robustness to Varying Biasing Volumes** (arXiv 2509.05908) — shows biasing
  strength tuned for large lists **degrades small-list / low-context inputs**; argues for selecting only the
  most-relevant context per representation. (`over-biasing-robustness-psc-joint.md`)
- **Ren, Shi, Li 2025, Lightweight Prompt Biasing** (arXiv 2506.06252) — adds a learned **"when to focus"** gate
  + entity filtering so biasing activates only when relevant; 18–31% Entity-WER gains; directly targets the
  false-trigger / over-insertion problem. (`lightweight-prompt-biasing-when-to-focus.md`)
- **Google Cloud STT model adaptation / Chirp (USM)** — production analog: `PhraseSet` + `boost` weights, up to
  ~1,000 phrases; docs warn excessive boost over-recognizes. No first-class "descriptive prose" knob.
  (`google-stt-chirp-model-adaptation.md`)

## Theme 2 — Prompt conditioning of Whisper / web-scale models

- **Yang, Huang, Lee 2024, Do Prompts Really Prompt?** (SLT; arXiv 2406.05806) — **the key cautionary paper.**
  Whisper does *not* understand textual prompts as humans expect: mismatched/irrelevant prompts often beat
  matched ones (up to ~11% rel.), and there is **no positive correlation** between topical relevance and WER.
  (`do-prompts-really-prompt-whisper.md`)
- **Peng et al. 2023, Prompting the Hidden Talent** (Interspeech; arXiv 2305.11095) — prompt engineering unlocks
  zero-shot tasks (+10–45%), but reports Whisper is **fairly robust/insensitive to prompt wording** — gains come
  from format, not naturalistic meaning. (`prompting-hidden-talent-zero-shot.md`)
- **Liao et al. 2023, Prompt-conditioning Fine-tuning (clairaudience)** (ASRU; arXiv 2307.10274) — fine-tuning
  Whisper to attend to a **domain-description prompt** gives up to **33% WER reduction** on unseen domains (incl.
  ATC); text-only variant up to 29%. Positive evidence — but the benefit required *training* the model to use the
  prompt. (`prompt-conditioning-finetuning-clairaudience.md`)
- **Jogi et al. 2025, B-Whisper / Rare-Word Zero-Shot** (SLT; arXiv 2502.11572) — supervised fine-tuning for
  prompt-based biasing; +45.6% rare-word, +60.8% unseen-word recall; generalizes across datasets/languages.
  (`bias-whisper-rare-word-zero-shot.md`)
- **Li et al. 2023/24, Multitask Whisper + OV-KWS** (Interspeech 2024; arXiv 2309.09552) — detect entities on the
  encoder, feed as decoder prompts; up to 80% entity-recall gain. Detection-before-biasing curbs over-insertion.
  (`multitask-whisper-biasing-keyword-spotting.md`)

## Theme 3 — Does context HURT? Short utterances, hallucination, over-insertion

- **Barański et al. 2025, Whisper Hallucinations from Non-Speech** (arXiv 2501.11378) — **silence at clip
  start/end directly triggers hallucination**; noisy/ambiguous/short input is high-risk; >30s files worse;
  post-hoc "bag of hallucinations" removal lowers WER. (`whisper-hallucinations-non-speech.md`)
- **Wang et al. 2025, Calm-Whisper** (Interspeech 2025; arXiv 2505.12969) — 3/20 decoder heads cause >75% of
  non-speech hallucinations; targeted fix gives >80% reduction at <0.1% WER cost. Hallucination is structural and
  fixable without harming in-context WER. (`calm-whisper-non-speech-hallucination-heads.md`)
- (Also relevant from Theme 1: Zhao 2019 over-insertion; Gu 2025 small-list degradation.)

## Theme 4 — LLM-based / post-ASR correction with domain or named-entity metadata

- **Trinh, He, Whitehill 2025, NE Transcription via LLM Revision** (arXiv 2506.10779) — post-ASR LLM revises
  misrecognized entities using world knowledge + phonetic/semantic context; up to 30% rel. entity-WER reduction.
  Fixes errors *after* the acoustic pass — avoids prompt-induced acoustic bias. (`named-entity-llm-revision-classroom.md`)
- **Ron, Gilboa, Dubnov 2026, Whisper: Courtside Edition** (arXiv 2602.18966) — multi-agent LLM **generates** a
  compact context prompt for Whisper's decoder; 17% rel. WER reduction on NBA commentary, **improving 40.1% of
  segments but degrading 7.1%** — quantifies the helps-vs-hurts split precisely. (`whisper-courtside-llm-context-generation.md`)

## Theme 5 — Public-safety / radio / dispatch / ATC benchmarks & domain adaptation

- **Srivastava et al. 2024, Police Radio Communication ASR** (arXiv 2409.10858) — ~46h BPC corpus; off-the-shelf
  Whisper/NeMo poor; **fine-tuning** closes most of the gap (scale alone does not). Closest domain match.
  (`police-radio-asr-uchicago.md`)
- **Gartner et al. 2025, 9-1-1 Dispatch ASR for Incident Detection** (Smart Cities, MDPI) — live Whisper on rural
  9-1-1 dispatch reaches **"usable accuracy"** for an operational pipeline; 100k+ short transmissions transcribed.
  (`public-safety-radio-911-incident-detection.md`)
- **Nigmatulina et al. 2022, ATC Two-Step Contextual** (ICASSP; arXiv 2202.03725) — external situational metadata
  (nearby-aircraft list) + n-gram boosting + NER correction gives up to **60.4% rel. callsign improvement** —
  but via **lexical entity biasing**, not prose. (`atc-two-step-contextual-callsign.md`)

---

## Synthesis

### (a) Where the literature says context biasing HELPS
- Lexical/entity biasing (shallow fusion, neural/attention, KWS-prompted) reliably raises recall of rare words,
  proper nouns and **named entities** — the highest-value tokens (Zhao 2019; Li 2024; Ren 2025; Jogi 2025).
- In constrained public-safety-adjacent domains (ATC), feeding **situational metadata** (who/what is nearby) as a
  dynamic bias list yields very large callsign-recognition gains (Nigmatulina 2022).
- Whisper **can** exploit a domain-description prompt — but most reliably **after prompt-conditioning fine-tuning**
  (Liao 2023; Jogi 2025). Multi-agent LLM-generated context prompts also net-improve a stock decoder (Ron 2026).
- Post-ASR LLM correction with domain/entity metadata is a robust, lower-risk way to inject context (Trinh 2025).

### (b) Where it WARNS of over-insertion / hallucination / short-utterance degradation
- Over-biasing → **spurious insertions / false triggers**, especially with large or irrelevant lists
  (Zhao 2019); biasing strength that helps large lists **hurts small/low-context inputs** (Gu 2025) — hence the
  need for "when to bias" gating (Ren 2025) and presence-detection before biasing (Li 2024).
- **Short / silent / noisy clips are the worst case for hallucination**: leading/trailing silence directly
  triggers fabricated text (Barański 2025); a few decoder heads drive non-speech hallucination (Wang 2025). This
  is exactly the regime of short public-safety transmissions with dead air.
- Even LLM-prompt-generation that nets positive **still degrades a measurable minority of segments** (~7%, Ron
  2026) — net gains hide per-clip regressions.
- Most strikingly, **stock Whisper prompts don't behave semantically**: matched topical prompts do not reliably
  beat mismatched ones, and relevance is uncorrelated with WER (Yang 2024). Apparent "gains" from prose framing
  may be artifacts, and the same framing can just as easily inject bias on out-of-context audio.

### (c) The gap around DESCRIPTIVE prose framing (vs lexical word-list biasing)
- Nearly all "context helps" evidence uses **lexical signals**: phrase/entity lists, n-grams, KWS-detected
  entities, boost weights (Zhao 2019; Li 2024; Ren 2025; Nigmatulina 2022; Google STT/Chirp). Production APIs
  (Chirp/USM, Google STT) expose **only** phrase-set biasing — no descriptive-prose knob.
- The few works using **natural-language descriptive prompts** either (i) had to **fine-tune** the model to make
  the prompt useful (Liao 2023; Jogi 2025), or (ii) found stock Whisper **does not understand** descriptive
  prompts in a human-expected way (Yang 2024; "robust to prompts," Peng 2023).
- **No paper directly isolates the effect of descriptive metadata framing (e.g., "this is a wildfire dispatch
  transmission near <county>") on SHORT, out-of-context clips, measuring both in-context gain AND no-context
  regression / over-insertion.** The closest measurement is Ron 2026's per-segment improve/degrade split, but on
  long sports commentary, not short safety radio, and with LLM-generated rather than fixed-template prose.
- **Implication for this project:** there is a genuine, under-studied gap. Expect descriptive prose to behave
  differently from lexical biasing; the literature strongly recommends (1) evaluating per-clip regression and
  over-insertion (not just aggregate WER), (2) gating/conditional application for low-content clips, and
  (3) treating post-ASR LLM correction as a lower-risk alternative or complement to acoustic-stage prompting.
