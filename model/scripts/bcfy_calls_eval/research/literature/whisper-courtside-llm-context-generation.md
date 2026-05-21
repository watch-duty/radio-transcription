# Whisper: Courtside Edition — Enhancing ASR Performance Through LLM-Driven Context Generation

- **Authors:** Yonathan Ron, Shiri Gilboa, Tammuz Dubnov (Reichman University)
- **Year:** 2026 (published Feb 24, 2026)
- **Venue:** arXiv preprint (eess.AS / cs.CL)
- **URL / arXiv:** https://arxiv.org/abs/2602.18966

## Key findings
- Multi-agent **LLM pipeline that generates the prompt** for Whisper without retraining: intercepts an initial transcript, runs LLM agents for domain-context identification, NER, and jargon detection, then emits a **compact prompt** that guides Whisper's decoder on a re-pass.
- Evaluated on **421 NBA basketball commentary segments**: **17.0% relative WER reduction** (0.217 -> 0.180, p < 0.001).
- **Asymmetric per-segment outcome:** improved **40.1%** of segments, **degraded only 7.1%** — net positive but explicitly quantifies that prompting hurts a non-trivial minority of segments.
- Outperforms direct LLM **post-editing** of the transcript (i.e., feeding context back into the acoustic decode beats text-only correction here).
- Argues prompt-based augmentation is a scalable, fine-tuning-free domain-adaptation route.

## Relevance
Highly relevant: a concrete "descriptive context as prompt" system showing net gains but with measured per-segment degradation (~7%) — exactly the helps-vs-regresses tradeoff the project cares about, and a template for measuring it.
