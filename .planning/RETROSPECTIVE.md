# Project Retrospective

*A living document updated after each milestone. Lessons feed forward into future planning.*

## Milestone: v1.0 — SFT Dataset Versioning

**Shipped:** 2026-05-28
**Phases:** 5 | **Plans:** 17 | **Tasks:** 46

### What Was Built

- Dataset-version TOML config parsing and strict GCS JSON/JSONL manifest loading.
- Leak-safe source identity and normalization for Broadcastify Calls, Broadcastify Feeds, Echo, and Fire Notifications.
- Balance-first 80:20 train/SFT Eval Split assignment with hard Source Group, original-audio, model-ready URI, and duplicate-span gates.
- Immutable GCS artifact layout with canonical manifests, per-dataset slices, reports, and NeMo/Whisper/Gemini model inputs.
- Audio reuse/derivation planning with model-ready GCS URI hard gates and transformation provenance.
- `split_dataset.py dry-run` and `generate` CLI workflows with report sidecars and README terminology.

### What Worked

- Treating Source Group separation as the hard gate kept leak prevention simple and testable.
- Keeping model writers on one enriched row type avoided diverging NeMo, Whisper, and Gemini dataset semantics.
- Saving assignment, metadata, and reports made reproducibility independent of deterministic re-solving.
- Fake storage clients gave strong coverage for GCS layout and create-only behavior without production credentials.

### What Was Inefficient

- Phase 3 needed several review-gap passes around artifact publication and Gemini shape validation.
- Validation strategy files were not consistently backfilled after implementation; Phase 4 remains stale despite passed verification.
- Live GCS publication verification stayed deferred because it crosses bucket/IAM boundaries.

### Patterns Established

- Canonical dataset-version artifacts are reusable across many SFT runs; SFT run reports belong elsewhere.
- `model_ready_audio_uri` is required for model writers after audio preparation; writers do not fall back to raw source audio.
- Reports should summarize enough transformation and excluded-row context to debug production failures without storing raw command output.

### Key Lessons

1. Source identity must be derived from upstream feed/device/location semantics, not convenient filenames or sampling UUIDs.
2. Echo source identity needs `area_code` plus `echo_name`; `echo_name` alone is ambiguous.
3. Balance should optimize correlated audio features while leakage gates remain strict and non-negotiable.
4. External-service smoke tests should be either run early with credentials or explicitly tracked as deferred UAT.

### Cost Observations

- Sessions: multiple interactive GSD sessions across five phases.
- Notable: Repeated phase discussion clarified terminology enough to simplify implementation, especially around Dataset Version vs SFT Run and reuse vs derive.

---

## Cross-Milestone Trends

### Process Evolution

| Milestone | Sessions | Phases | Key Change |
|-----------|----------|--------|------------|
| v1.0 | multiple | 5 | Established source-safe dataset-versioning workflow and model-input publication path. |

### Cumulative Quality

| Milestone | Tests | Coverage | Zero-Dep Additions |
|-----------|-------|----------|-------------------|
| v1.0 | 182 SFT script tests, 59 subtests | Source identity, split, leakage, artifact writers, audio provenance, CLI | Most implementation used stdlib plus existing repo helpers; OR-Tools is the main optimizer dependency. |

### Top Lessons

1. Make leakage identity explicit before balancing or audio derivation.
2. Keep dataset-version artifacts separate from SFT run artifacts.
