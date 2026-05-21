# Do Prompts Really Prompt? Exploring the Prompt Understanding Capability of Whisper

- **Authors:** Chih-Kai Yang, Kuan-Po Huang, Hung-yi Lee
- **Year:** 2024 (presented at IEEE SLT 2024)
- **Venue:** IEEE Spoken Language Technology Workshop (SLT) 2024
- **URL / arXiv:** https://arxiv.org/abs/2406.05806 (IEEE Xplore: https://ieeexplore.ieee.org/document/10832185/)

## Key findings
- Systematically tests whether Whisper's textual `initial_prompt` is understood the way humans expect, by comparing **matched** (correct topic) vs **mismatched** (wrong/irrelevant topic) prompts. The premise: if Whisper truly understood prompts, matched should beat mismatched.
- **Counterintuitive core result:** Whisper often performs *better* with mismatched topic prompts than with matched ones. Mismatched/irrelevant information can yield up to ~11% relative WER improvement — i.e., the gains are not coming from semantic topic understanding.
- Regression analysis finds **no positive correlation** between a prompt's topical relevance ("prompt understanding") and downstream WER — prompting "does not work as expected."
- **Language effect:** English prompts generally outperform Mandarin prompts even on Mandarin test data, reflecting training-data distribution rather than meaning.
- Whisper does correctly handle *language tokens* (ignores incorrect language tokens), so the failure is specifically about understanding free-text topical prompts, not all conditioning signals.

## Relevance
Direct and strong: this is the central cautionary paper for "does descriptive metadata framing help?" — it shows Whisper text prompts give inconsistent, sometimes spurious gains unrelated to actual topical correctness, so descriptive prose framing cannot be assumed to help and may "improve" WER for the wrong (non-semantic) reasons.
