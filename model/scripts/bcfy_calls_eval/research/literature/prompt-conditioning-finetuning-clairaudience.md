# Zero-shot Domain-sensitive Speech Recognition with Prompt-conditioning Fine-tuning

- **Authors:** Feng-Ting Liao, Yung-Chieh Chan, Yi-Chang Chen, Chan-Jan Hsu, Da-shan Shiu (MediaTek Research)
- **Year:** 2023
- **Venue:** IEEE ASRU 2023 (Automatic Speech Recognition and Understanding Workshop)
- **URL / arXiv:** https://arxiv.org/abs/2307.10274 (code: https://github.com/mtkresearch/clairaudience)

## Key findings
- Fine-tunes Whisper to **condition generation on a text prompt** describing the domain, teaching the model from demonstrations so it becomes "domain-sensitive" at inference time.
- Achieves up to **33% relative WER reduction** on unseen domains (medical conversation, air traffic control, financial meetings) when given an appropriate domain prompt at inference.
- Introduces a **text-only fine-tuning** variant (no paired audio needed) that still confers domain sensitivity, reaching up to **29% WER reduction** on medical conversation — useful when audio-transcript pairs are scarce.
- Prompts here are domain/context descriptions (closer to descriptive framing than to lexical hotword lists), but the capability is *learned via fine-tuning* — off-the-shelf Whisper does not reliably exhibit it (contrast with "Do Prompts Really Prompt?").
- Demonstrates prompts can carry genuine domain information *if the model is trained to attend to them*.

## Relevance
Key positive evidence that descriptive/domain prompt framing CAN help — but with the important caveat that the benefit came from prompt-conditioning *fine-tuning*, not from zero-shot prompting of a stock model. Relevant to deciding whether metadata framing needs fine-tuning to pay off on short radio clips.
