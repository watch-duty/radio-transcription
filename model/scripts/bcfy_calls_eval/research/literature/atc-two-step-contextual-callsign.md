# A Two-Step Approach to Leverage Contextual Data: Speech Recognition in Air-Traffic Communications

- **Authors:** Iuliia Nigmatulina, Juan Zuluaga-Gomez, Amrutha Prasad, Seyyed Saeed Sarfjoo, Petr Motlicek (Idiap / BUT)
- **Year:** 2022
- **Venue:** IEEE ICASSP 2022
- **URL / arXiv:** https://arxiv.org/abs/2202.03725

## Key findings
- Targets **air-traffic control (ATC)** radio — a public-safety-adjacent domain of short, noisy, callsign-heavy transmissions with a constrained grammar.
- **Two-step contextual pipeline:** (1) ASR-stage boosting — reduce/adjust weights of probable **callsign n-grams** in the decoding/G FST using live surveillance data (list of nearby aircraft); (2) NLP-stage — NER extracts the spoken callsign and correlates it with surveillance data to pick the best match.
- Reports up to **53.7% absolute / 60.4% relative improvement in callsign recognition** when combining ASR-side biasing with NLP correction.
- Context here is a **dynamic list of relevant entities (callsigns)** derived from external metadata (surveillance), i.e., lexical/word-list biasing keyed on geography/situation — not descriptive prose.
- Shows large gains are achievable specifically on the high-value named entities, while general WER gains are smaller.

## Relevance
Strong analog for radio dispatch: external situational metadata (who/what is nearby) sharply improves entity recognition. But the mechanism is lexical entity biasing, not descriptive framing — highlights the gap the project is probing (prose vs word-list metadata).
