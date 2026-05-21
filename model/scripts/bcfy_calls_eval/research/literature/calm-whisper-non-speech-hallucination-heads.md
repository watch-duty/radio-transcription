# Calm-Whisper: Reduce Whisper Hallucination On Non-Speech By Calming Crazy Heads Down

- **Authors:** Yingzhi Wang, Anas Alhmoud, Saad Alsahly, Muhammad Alqurishi, Mirco Ravanelli (Elm Company KSA; Concordia University; Mila)
- **Year:** 2025
- **Venue:** INTERSPEECH 2025
- **URL / arXiv:** https://arxiv.org/abs/2505.12969 (ISCA: https://www.isca-archive.org/interspeech_2025/wang25b_interspeech.html)

## Key findings
- Localizes Whisper's **non-speech hallucination** to a few decoder self-attention heads: head-wise masking shows **only 3 of 20 heads** account for **>75%** of hallucinations on the UrbanSound dataset.
- Fine-tuning just those three "crazy heads" on non-speech data yields **Calm-Whisper**: **>80% reduction in non-speech hallucination** with **<0.1% WER degradation** on LibriSpeech test-clean/test-other.
- Demonstrates hallucination on non-/low-speech input is a structural property of the decoder, not just a decoding-parameter artifact.
- Provides a targeted mitigation that does not broadly harm normal transcription.

## Relevance
Reinforces the short-clip risk: silence/non-speech segments (common in radio dead air between transmissions) drive fabricated text. Relevant because descriptive prompts that "expect" content could amplify this on empty/low-content clips; also shows hallucination is fixable without sacrificing in-context WER.
