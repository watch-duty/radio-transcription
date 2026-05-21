# Improving Speech Recognition of Named Entities in Classroom Speech with LLM Revision and Phonetic-Semantic Context

(also circulated as "Improving Named Entity Transcription with Contextual LLM-based Revision")

- **Authors:** Viet Anh Trinh, Xinlu He, Jacob Whitehill (Worcester Polytechnic Institute)
- **Year:** 2025
- **Venue:** arXiv preprint (cs.CL / eess.AS)
- **URL / arXiv:** https://arxiv.org/abs/2506.10779

## Key findings
- **Post-ASR LLM revision** pipeline: an LLM revises misrecognized named entities in Whisper output using world knowledge plus available **phonetic and semantic context** (e.g., a known list of correct entities from lecture notes).
- New benchmark **NER-MIT-OpenCourseWare** (~45 h of MIT course audio); achieves up to **30% relative WER reduction on named entities**.
- With GPT-4o-mini, entity-WER drops to **22.7%** vs **25.3%** for a random-replacement baseline.
- Notes Whisper large-v3 alone is weak on entities (e.g., ~28.9% WER for persons, ~19.6% for organizations on the ConEC benchmark) — motivating external correction.
- Method is **interpretable** (LLM explains revisions) and works across closed/open LLMs and sizes; correction is applied at the text stage, not the acoustic stage.

## Relevance
Represents the "fix it after, with metadata/context" school: location/domain metadata can be injected as LLM context to correct entities post-transcription, sidestepping prompt-induced acoustic bias — a lower-risk alternative to descriptive prompt framing on short clips.
