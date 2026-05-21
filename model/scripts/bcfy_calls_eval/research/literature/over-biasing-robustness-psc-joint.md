# Enhancing the Robustness of Contextual ASR to Varying Biasing Information Volumes Through Purified Semantic Correlation Joint Modeling

- **Authors:** Yue Gu, Zhihao Du, Ying Shi, Shiliang Zhang, Qian Chen, Jiqing Han (Harbin Institute of Technology; Alibaba)
- **Year:** 2025
- **Venue:** arXiv preprint (eess.AS); INTERSPEECH 2025 cycle
- **URL / arXiv:** https://arxiv.org/abs/2509.05908

## Key findings
- Studies how **biasing-list size (information volume)** affects contextual ASR. Cross-attention biasing degrades as the list grows because it cannot focus on the few relevant entries.
- **Explicit over-biasing finding:** injecting more distractors during training improves performance on *large* biasing lists but **degrades performance on small lists** — there is no single biasing strength that is safe across volumes.
- Proposes **PSC-Joint** (purified semantic correlation joint modeling) to identify and integrate only the most-relevant biasing info per acoustic representation, rather than the whole list.
- Improves F1 on varying-length biasing lists across AISHELL-1, KeSpeech, AISHELL-NER vs baselines.
- Reinforces that the **amount** and relevance of injected context must be controlled or it hurts.

## Relevance
Direct evidence for the "context can hurt" side: over-supplying biasing content degrades exactly the low-context cases (small lists / clean utterances) — analogous to over-priming short radio clips with descriptive metadata they don't need.
