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
- Audited the public selection against the locked objective. The first selected
  slice was optimal but still contained residual ATC vocabulary not covered by
  the soft penalties.
- Expanded the soft-penalty list with residual procedure terms and common
  airline/callsign tokens, re-solved the selector, restaged public artifacts,
  rebuilt the blended train JSONL, regenerated configs, and re-ran readiness.
- The refined selection remains optimal, hits all targets exactly, and reduces
  residual procedure-term rows from 620 to 110 and sampled airline-token rows
  from 488 to 119.
- Ran preflight on the two distinct train/validation pairs: WD-only plus WD eval,
  and WD+ATC plus WD eval. Both passed with zero failures. No tuning or Vertex
  batch-eval jobs were submitted.

## Execution Boundary

This branch prepares data, configs, reports, and validation only. It must stop
before running any Vertex tuning job.
