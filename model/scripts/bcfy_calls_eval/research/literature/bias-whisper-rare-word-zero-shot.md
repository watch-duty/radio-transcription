# Improving Rare-Word Recognition of Whisper in Zero-Shot Settings

- **Authors:** Yash Jogi, Vaibhav Aggarwal, Shabari S Nair, Yash Verma, Aayush Kubba
- **Year:** 2025
- **Venue:** IEEE SLT 2024 / arXiv (eess.AS)
- **URL / arXiv:** https://arxiv.org/abs/2502.11572

## Key findings
- Fine-tunes Whisper for **contextual biasing via prompting** — model named **B-Whisper / Bias-Whisper** — with a prompt-selection strategy and a weighted cross-entropy loss to focus learning on the biasing task.
- Trained on only **670 h of Common Voice English**, generalizes to **11 diverse English datasets**.
- **45.6% improvement** in rare-word recognition and **60.8%** improvement on words unseen during fine-tuning, vs the baseline biasing method.
- Biasing ability generalizes even to **languages unseen** during fine-tuning — suggesting the learned mechanism is fairly general.
- Confirms (consistent with clairaudience and "Do Prompts Really Prompt?") that robust prompt-based biasing on Whisper benefits from supervised fine-tuning rather than relying on stock zero-shot prompt sensitivity.

## Relevance
Evidence that prompt-based biasing works well for rare/domain words — but again via fine-tuning. The prompts are word/entity-list style; reinforces the gap between lexical biasing (well-studied) and descriptive prose framing (the project's question).
