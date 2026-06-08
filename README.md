# Radio Transcription

A monorepo consisting of a model, pipeline, and frontend UI for listening to and transcribing emergency radio traffic into text.

See [CONTRIBUTING.md](./CONTRIBUTING.md) to get started.

### Repository Documentation
- [Local Mock Audio Server](./documentation/local-dev-mock-audio.md) — How to mock incoming audio streams for local testing.


## Directory structure

- `model/` - everything related to building and evaluating the transcription models. See [ASR_CONTRIBUTING.md](./ASR_CONTRIBUTING.md) for evaluation guidelines. (Note: Use the `asr-eval` container if you need NeMo/Canary, otherwise use the lightweight `notebooks` container for pure Hugging Face evaluations like Whisper or Cohere).
- `backend/` - everything related to the processing pipeline and backend support for the UI
- `frontend/` - everything related to the frontend app/UI