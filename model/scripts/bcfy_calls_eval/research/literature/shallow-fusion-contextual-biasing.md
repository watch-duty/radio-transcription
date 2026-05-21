# Shallow-Fusion End-to-End Contextual Biasing

- **Authors:** Ding Zhao, Tara N. Sainath, David Rybach, Pat Rondon, Deepti Bhatia, Bo Li, Ruoming Pang (Google)
- **Year:** 2019
- **Venue:** INTERSPEECH 2019, pp. 1418–1422
- **URL / DOI:** https://www.isca-archive.org/interspeech_2019/zhao19d_interspeech.html (DOI: 10.21437/Interspeech.2019-1209)

## Key findings
- Foundational paper on **shallow-fusion contextual biasing** for end-to-end (LAS-style) ASR: bias toward a context list (contacts, songs, apps) by adding scores during beam search, without retraining the core model.
- Introduces algorithmic + training improvements (e.g., biasing at the subword level, proper-noun handling, contextual FST construction) so that biased E2E beats a strong conventional system across tasks — a first at the time.
- Documents the central failure mode: **over-biasing causes spurious insertions / false triggers** when the bias list is large or irrelevant; careful weighting and prefix constraints are needed to avoid degrading non-context utterances.
- Establishes the persistent tension: aggressive biasing helps in-context words but **hurts general/no-context audio** if unchecked.

## Relevance
The canonical reference for "context biasing helps, but over-insertion is the price." Directly motivates the project's worry that priming short/out-of-context clips can inject content that isn't there. Lexical (word-list) biasing, contrasting with descriptive prose framing.
