# SFT Dataset Versioning

## What This Is

This project adds a deterministic, leak-safe dataset split and artifact generator for supervised fine tuning of emergency radio ASR models. It turns existing labeled audio manifests into versioned GCS artifacts that can be consumed by NeMo, Whisper, and Gemini fine-tuning workflows on Vertex AI or adjacent training runners.

The project is brownfield: the repository already has ingestion, transcription, evaluation, and early SFT manifest-building code. This work scopes the missing dataset-versioning layer: source-group-aware train/SFT Eval Split creation, model-specific input manifests, provenance, validation reports, and GCS organization.

## Core Value

Every SFT run must train and compare models on the same auditable dataset version without source leakage between train and SFT Eval Split.

## Requirements

### Validated

- [x] Existing ASR evaluation pipeline can score manifests using normalized WER, error counts, per-row metrics, duration buckets, keyword accuracy, and error analysis - existing
- [x] Existing GCS-backed manifest patterns support raw and segmented audio references for Broadcastify calls and feeds - existing
- [x] Existing SFT scripts can consume GCS manifests and build canonical/model-oriented JSONL rows for Gemini-style tuning inputs - existing
- [x] Existing codebase has reusable manifest/scoring helpers that should be extended instead of replaced - existing
- [x] Phase 1 validates dataset-version TOML configs, strict `gs://` JSON/JSONL inputs, source-key extraction for all four initial dataset families, and empty-text exclusion counts - Phase 1
- [x] Phase 3 generates immutable dataset-version artifact layouts, canonical and per-dataset train/eval manifests, NeMo/Whisper/Gemini model inputs/configs, JSON/Markdown reports, and create-only GCS publication guards - Phase 3
- [x] Phase 4 reuses supported standalone model-ready clips, derives longer-source spans into FLAC when needed, and records auditable model-ready audio provenance before publishing canonical/model artifacts - Phase 4

### Active

- [ ] Build a split script that creates an 80:20 train/SFT Eval Split from existing manifests while assigning every source group wholly to one split.
- [ ] Validate leakage prevention with actual manifest data, including source-group overlap, original-audio overlap, duplicate URI overlap, missing text rows, and parse failures.
- [ ] Balance the split across factors that can correlate with model performance: dataset family, source count, row count, audio duration, time/month/hour, transcript length, and duration buckets.
- [ ] Add focused tests for CLI generation and production GCS/report integration.

### Out of Scope

- Replacing existing historical benchmark/eval manifests - this project creates SFT dataset versions and leaves benchmarks intact.
- Sampling new upstream audio from Broadcastify, Echo, or Fire Notifications APIs - the splitter consumes existing labeled manifests or explicit inputs.
- Running actual fine-tuning jobs end to end - this project generates the inputs/configs that those jobs consume.
- Committing generated proprietary manifests or derived audio to GitHub - generated artifacts belong in GCS.
- Guaranteeing a mathematically perfect 80:20 split across every factor - source-group separation is the hard gate; balance is optimized, reported, and bounded.
- Treating the SFT Eval Split as a hidden holdout - it may be used for validation, selection, and post-run evaluation, so naming must remain explicit.

## Context

The current codebase already includes transcription backends, batch processing, evaluation notebooks/scripts, shared manifest/scoring helpers, and an early `model/scripts/sft` pipeline. The SFT path is currently Echo-oriented and has stubs around tuning/eval execution; this project should add dataset-versioning primitives that can be reused by that pipeline rather than hard-coding one dataset shape.

Validated dataset observations from the prior exploration:

- `bcfy_calls`: current GCS eval raw manifest had 1,809 rows, 1,043 rows with text, 57 group IDs, 277 original text files, and zero source-key parse failures. Segmented batch artifacts exist with 1,043 FLAC rows.
- `bcfy_feeds`: current GCS eval raw manifest had 8,588 rows, 5,098 rows with text, 115 feed IDs, 125 unique original labeled WAV files, and zero source-key parse failures. Example source files are multi-minute labeled WAVs rather than hour-long archive blobs.
- `echo`: existing eval artifacts often lack explicit `area_code` and `echo_name`. `all_echo_mono_streams.csv` has 718 rows, 582 unique echo names, and 98 duplicate echo names across areas, so `echo_name` alone is not a safe source identity.
- `echo_mono_samples_20251208.csv`: all 2,791 rows parse source identity from URL/S3 key shape `area_code/YYYYMMDD/echo_name_YYYYMMDD_HH.mp3`.
- `fire_notifications`: no GCS manifests were found in the checked prefixes. The sampling script emits stream/location information; the stable source group should be stream path/location, not the per-day UUID used during collection.

Glossary:

- Source Group: the upstream radio source identity that must be wholly assigned to train or SFT Eval Split.
- Labeled Segment: a raw annotation row with source audio, offset/duration, and target transcript.
- SFT Example: one model-ready utterance clip plus one target transcript.
- SFT Eval Split: the evaluation side of the SFT dataset version, used for validation/selection/post-run eval.
- Dataset Version: an immutable GCS artifact tree identified by `dataset_version_id`.

## Constraints

- **Leakage**: No Source Group may appear in both train and SFT Eval Split - same radio feed/device/location can leak speaker, scanner, agency, channel, acoustics, and phrase distribution.
- **Ambiguity**: Ambiguous source identity must fail rather than guess - especially Echo rows where `echo_name` is duplicated across area codes.
- **Compatibility**: Generated model inputs must match current NeMo, Whisper, and Gemini/Vertex AI requirements as verified from current docs during implementation.
- **Storage**: Generated dataset artifacts and derived clips live in GCS under `gs://wd-transcription-data/sft/{dataset_version_id}/`.
- **Reproducibility**: Splits must be deterministic by seed, input manifest set, and split configuration.
- **Minimal transformation**: Reuse existing clips when valid; derive audio only when needed; avoid padding and avoid resampling unless a target model/input format requires it.
- **Git hygiene**: Git stores code, tests, templates, and planning docs; not generated manifests, credentials, or audio payloads.

## Key Decisions

| Decision | Rationale | Outcome |
|----------|-----------|---------|
| Use `dataset_version_id` as the primary artifact identifier | Dataset artifacts are reused across model runs and should not be coupled to one training run | Validated in Phase 3 |
| Split Source Groups before slicing or deriving SFT examples | Prevents same-feed leakage even when multiple clips come from one long source recording | Pending |
| Call the evaluation side `SFT Eval Split` | It may be used for validation and selection, so "holdout" would overstate isolation | Pending |
| Use `area_code` + `echo_name` for Echo source identity | `echo_name` alone is ambiguous; validated CSV has duplicate names across areas | Validated in Phase 1 |
| Use `bcfy_calls:<groupId>` for Broadcastify Calls | Filenames and metadata expose stable group IDs, and actual manifests parse cleanly | Validated in Phase 1 |
| Use `bcfy_feeds:<feedId>` for Broadcastify Feeds | Archive URLs include feed IDs, and actual manifests parse cleanly | Validated in Phase 1 |
| Use Fire Notification stream path/location as source identity | Per-day UUIDs are sampling artifacts and would allow same stream across splits | Validated in Phase 1 |
| Store generated artifacts in GCS, not GitHub | Artifacts can be large/proprietary and should be immutable runtime data | Validated in Phase 3; live GCS smoke UAT pending |
| Reuse supported standalone audio clips before deriving new clips | Reduces unnecessary audio transforms and preserves provenance | Validated in Phase 4 |
| Exclude empty or missing normalized-text rows | Existing eval behavior already skips rows without usable ground truth | Validated in Phase 1 |

## Evolution

This document evolves at phase transitions and milestone boundaries.

**After each phase transition** (via `$gsd-transition`):
1. Requirements invalidated? -> Move to Out of Scope with reason
2. Requirements validated? -> Move to Validated with phase reference
3. New requirements emerged? -> Add to Active
4. Decisions to log? -> Add to Key Decisions
5. "What This Is" still accurate? -> Update if drifted

**After each milestone** (via `$gsd-complete-milestone`):
1. Full review of all sections
2. Core Value check - still the right priority?
3. Audit Out of Scope - reasons still valid?
4. Update Context with current state

---
*Last updated: 2026-05-28 after Phase 4 completion*
