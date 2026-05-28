# Milestones

## v1.0 SFT Dataset Versioning (Shipped: 2026-05-28)

**Phases completed:** 5 phases, 17 plans, 46 tasks

**Known deferred items at close:** 2 (see `STATE.md` Deferred Items)

**Key accomplishments:**

- TOML dataset-version config parsing and strict injectable GCS JSON/JSONL input loading for SFT dataset manifests
- Leak-safe source group extraction and source-tagged row normalization for all Phase 1 dataset families
- Offline Phase 1 dataset-version validation command with contextual errors and per-dataset count summaries
- OR-Tools CP-SAT Source Group assignment with seed-free config and split metadata.
- Exact split leakage and duplicate-span validators for split-populated SFT rows.
- Weighted balance reports with component deltas attached to split results.
- Immutable dataset-version GCS layout with prefix checks and create-only upload precondition handling
- Validated canonical/per-dataset JSONL builders plus dataset-version metadata and generation reports
- Validated NeMo and Whisper dataset-version input builders that preserve original audio spans and report Whisper duration risks
- Gemini SFT JSONL/config fragments plus create-only publication of the complete dataset-version artifact tree
- FFmpeg-backed audio planning and provenance enrichment for reusable, copied, derived, and transcoded SFT clips
- Model-ready GCS audio hard gates for NeMo, Whisper, and Gemini with one prechecked audio-plus-text publication flow
- Canonical JSONL hard gate for gs:// model-ready audio and complete Phase 4 transformation provenance
- Dataset reports now audit model-ready audio actions, D-26 metadata coverage, and command-summary coverage for every published SFT example
- Runbook CLI for dry-run previews and GCS generation backed by shared dataset loading, splitting, and leakage checks
- Excluded-row sidecar reporting for successful dry-run and generate bundles without adding failure artifacts
- Existing README runbook terminology and targeted SFT test verification for the split dataset CLI

---
