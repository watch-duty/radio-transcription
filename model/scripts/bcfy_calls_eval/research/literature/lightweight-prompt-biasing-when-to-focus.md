# Lightweight Prompt Biasing for Contextualized End-to-End ASR Systems

- **Authors:** Bo Ren, Yu Shi, Jinyu Li (Microsoft)
- **Year:** 2025
- **Venue:** arXiv preprint (eess.AS); INTERSPEECH 2025 cycle
- **URL / arXiv:** https://arxiv.org/abs/2506.06252

## Key findings
- Adds **prompt-based contextual biasing** to E2E ASR with two pieces: a **prompt biasing model** that learns *when to focus* on prompt entities, and an **entity filtering** step that drops irrelevant entities before decoding.
- Unified **multi-task setup with task tokens** lets one model handle biasing and non-biasing inputs with no architectural change.
- **30.7%** (small entity list) and **18.0%** (large list) relative reduction in Entity-WER vs a shallow-fusion baseline.
- Core design insight: deciding *whether* to apply biasing for a given input is as important as the biasing itself — addresses the false-trigger / over-insertion problem head-on.
- "Lightweight" = no structural change, low overhead.

## Relevance
Embodies the mitigation principle most relevant to short clips: gate the biasing so it activates only when relevant, otherwise leave clean/no-context audio untouched. Argues against blanket descriptive priming and for conditional/selective context injection.
