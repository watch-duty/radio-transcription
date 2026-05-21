# A Multitask Training Approach to Enhance Whisper with Contextual Biasing and Open-Vocabulary Keyword Spotting

- **Authors:** Yuang Li, Min Zhang, Chang Su, Yinglu Li, Xiaosong Qiao, Mengxin Ren, Miaomiao Ma, Daimeng Wei, Shimin Tao, Hao Yang (Huawei)
- **Year:** 2023 (accepted INTERSPEECH 2024)
- **Venue:** INTERSPEECH 2024 / arXiv (eess.AS)
- **URL / arXiv:** https://arxiv.org/abs/2309.09552

## Key findings
- Enhances Whisper for **contextual biasing** via **open-vocabulary keyword spotting (OV-KWS)** on the encoder hidden states: detect user-defined named entities, then feed them as **prompts to the decoder**.
- Up to **80% entity-recall improvement** on AISHELL hotword subsets and up to **10%** on internal code-switching data.
- KWS-based detection helps decide *which* entities are actually present, reducing blind injection of the full bias list.
- Combines the strengths of detection (precision on which words to bias) and prompting (decoder conditioning) within the Whisper architecture.
- Still relies on **lexical entity lists** as the biasing signal, not descriptive context.

## Relevance
Another data point that Whisper-specific lexical biasing strongly raises entity recall, and that detecting presence before biasing curbs over-insertion. Reinforces the lexical-vs-prose gap for the project's metadata-framing question.
