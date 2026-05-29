# Research Log

## 2026-05-29

- Validated Watch Duty Gemini train/eval JSONL shape and SFT example quality.
- Confirmed the internal train split has 16,919 examples and the eval split has
  4,108 examples.
- Confirmed OR-Tools is usable through `uv run --with ortools`.
- Confirmed the installed Google Gen AI SDK exposes tuning checkpoint support
  through `export_last_checkpoint_only` and tuned model checkpoints.
- Sampled public ATC audio and verified in-memory 16 kHz mono FLAC transcoding.
- Found the public dataset metadata does not expose a license field through the
  Hugging Face API; retain this provenance caveat in reports.
- Chose four initial runs: WD-only adapter 4/8 and WD+ATC adapter 4/8, all at 5
  epochs and learning rate multiplier 1.0.

## Execution Boundary

This branch prepares data, configs, reports, and validation only. It must stop
before running any Vertex tuning job.
