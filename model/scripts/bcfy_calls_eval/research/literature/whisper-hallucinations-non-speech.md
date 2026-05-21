# Investigation of Whisper ASR Hallucinations Induced by Non-Speech Audio

- **Authors:** Mateusz Barański, Jan Jasiński, Julitta Bartolewska, Stanisław Kacprzak, Marcin Witkowski, Konrad Kowalczyk (AGH University of Krakow)
- **Year:** 2025
- **Venue:** arXiv preprint (eess.AS); presented in ICASSP 2025 cycle
- **URL / arXiv:** https://arxiv.org/abs/2501.11378

## Key findings
- Studies Whisper hallucinations — text generated unrelated to the audio — triggered by **non-speech audio segments** during inference.
- Identifies contributing factors across three axes: **model design, training-data quality, and input ambiguity**; noisy/unclear/ambiguous input strongly increases hallucination.
- **Silences at the start/end of files directly trigger hallucinations**; trimming silence (adjusted dB thresholds) reduces them — directly relevant to short clips with leading/trailing dead air.
- Hallucination rate rises noticeably once a **file exceeds 30 s** (Whisper's 30 s decoding window), implicating segment handling.
- Builds a "**bag of hallucinations (BoH)**" of common spurious phrases; post-processing removal reduces WER.

## Relevance
Core warning for short, out-of-context public-safety clips: when audio is short/silent/noisy, Whisper fabricates text. Any descriptive prompt that primes content risks compounding this on no-/low-content segments, so over-insertion on short clips is a documented failure mode to guard against.
