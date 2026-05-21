# Prompting the Hidden Talent of Web-Scale Speech Models for Zero-Shot Task Generalization

- **Authors:** Puyuan Peng, Brian Yan, Shinji Watanabe, David Harwath (UT Austin, CMU)
- **Year:** 2023
- **Venue:** INTERSPEECH 2023
- **URL / arXiv:** https://arxiv.org/abs/2305.11095 (ISCA: https://www.isca-archive.org/interspeech_2023/peng23d_interspeech.html)

## Key findings
- Investigates emergent zero-shot abilities of Whisper via **prompt engineering** on three unseen tasks: audio-visual speech recognition (AVSR), code-switched ASR (CS-ASR), and speech translation (ST) for unseen language pairs.
- Carefully designed prompts improve performance by **10%–45%** over default prompts across the three tasks, sometimes beating supervised SOTA.
- Reports that Whisper is **fairly robust to prompts** (insensitive to many prompt variations) — a double-edged property: it limits harm but also limits fine-grained controllability.
- Surfaces other latent properties: accent bias, and multilingual structure in the latent space.
- Shows prompt *format/structure* (e.g., concatenation tricks) matters more than naturalistic phrasing for unlocking behavior.

## Relevance
Supports that prompting can reshape Whisper behavior, but its "robustness to prompts" finding hints that descriptive framing may have only a muted effect on a fixed model — relevant to expecting modest, not large, gains from metadata prose on short clips.
