# Speech Recognition for Analysis of Police Radio Communication

- **Authors:** Tejes Srivastava, Ju-Chieh Chou, Priyank Shroff, Karen Livescu, Christopher Graziul (University of Chicago; Toyota Technological Institute at Chicago)
- **Year:** 2024
- **Venue:** arXiv preprint (eess.AS / cs.CL); also in PMC
- **URL / arXiv:** https://arxiv.org/abs/2409.10858

## Key findings
- Collects ~**62,000 manually transcribed radio transmissions (~46 h)** of Broadcast Police Communications (BPC) to benchmark ASR feasibility on real public-safety radio.
- **Off-the-shelf large models perform poorly** on BPC due to domain mismatch (low-bandwidth, noisy, jargon, codes). Whisper large-v3 >> large-v2, but generally **slightly worse than NeMo** models out of the box.
- Scaling model size does **not** reliably close the domain gap (bigger NeMo models only sometimes help).
- **Fine-tuning** the NeMo Fast-Conformer CTC on BPC data yields dramatic WER improvement, approaching human-level range — domain adaptation matters far more than model scale.
- Highlights that public-safety radio is a genuinely hard, distinct ASR domain.

## Relevance
Closest domain match (police/dispatch radio). Establishes that short, noisy public-safety transmissions need real domain adaptation; useful baseline context for asking whether lightweight metadata framing can substitute for or complement fine-tuning on such clips.
