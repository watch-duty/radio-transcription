# Google Cloud Speech-to-Text — Model Adaptation & Chirp Speech Adaptation (documentation)

- **Authors:** Google Cloud (product documentation, not a peer-reviewed paper)
- **Year:** current (accessed May 2026)
- **Venue:** Google Cloud Speech-to-Text V2 docs
- **URL:** https://cloud.google.com/speech-to-text/v2/docs/adaptation-model and https://cloud.google.com/speech-to-text/v2/docs/chirp-model

## Key findings
- **Model adaptation** lets you bias recognition toward specific words/phrases via a `PhraseSet` inside a `SpeechAdaptation` resource; multi-word phrases bias toward in-sequence recognition.
- **Boost** assigns per-phrase weights to raise recognition bias — a tunable knob, and (per docs) excessive boost can cause over-recognition / false positives, so values must be tuned.
- **Chirp / Chirp 3** (Google's USM-family models) support speech adaptation with **up to ~1,000 custom phrases** to handle brand names and technical jargon.
- This is the production analog of shallow-fusion **lexical phrase biasing** (word/phrase lists + weights), exposed as an API — not free-form descriptive prompting.
- Confirms that the dominant industrial mechanism for "context" in STT is curated phrase lists with weights, not natural-language scene descriptions.

## Relevance
Establishes the practical baseline for the Chirp/USM/Google-STT side of the brief: production "context" = lexical phrase-set biasing with boost. There is no first-class "descriptive prose prompt" knob, underscoring the gap the project investigates.
