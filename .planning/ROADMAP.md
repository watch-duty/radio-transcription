# Roadmap: SFT Dataset Versioning

## Overview

This roadmap builds the SFT dataset-versioning layer in dependency order: first normalize existing manifests and prove source identity, then implement source-group splitting and leakage validation, then write immutable GCS/model artifacts, then execute audio derivation with provenance, and finally wrap the workflow in a documented CLI with end-to-end verification.

## Phases

**Phase Numbering:**
- Integer phases (1, 2, 3): Planned milestone work
- Decimal phases (2.1, 2.2): Urgent insertions, if needed

- [x] **Phase 1: Manifest And Source Identity** - Normalize input manifests and resolve leak-safe Source Groups for every supported dataset family.
- [x] **Phase 2: Split Engine And Leakage Gates** - Generate balance-first 80:20 source-group splits with hard leak checks and balance reports.
- [ ] **Phase 3: GCS Artifacts And Model Writers** - Write immutable canonical artifacts plus NeMo, Whisper, and Gemini inputs/configs.
- [ ] **Phase 4: Audio Derivation And Provenance** - Reuse or derive clips with minimal transformation and complete provenance.
- [ ] **Phase 5: CLI, Reports, Docs, And Verification** - Expose dry-run/generate workflows, docs, and end-to-end verification.

## Phase Details

### Phase 1: Manifest And Source Identity
**Goal**: Users can load configured manifests and resolve unambiguous Source Groups for Broadcastify Calls, Broadcastify Feeds, Echo, and Fire Notifications.
**Depends on**: Nothing (first phase)
**Requirements**: INPT-01, INPT-02, INPT-03, INPT-04, SRC-01, SRC-02, SRC-03, SRC-04, SRC-05, SRC-06, TEST-01
**Success Criteria** (what must be TRUE):
  1. User can configure a dataset-version job and at least one input manifest.
  2. Supported manifest rows normalize into one internal labeled-segment shape with exclusion counts.
  3. Source-key extractors return stable Source Groups for valid rows and fail ambiguous Echo rows.
  4. Tests cover valid, missing, and ambiguous source-key cases for all supported dataset families.
**Plans**: 3 plans

Plans:
- [x] 01-01: Dataset-version config and manifest loading
- [x] 01-02: Source-key extraction and row normalization
- [x] 01-03: Source identity tests and exclusion reporting

### Phase 2: Split Engine And Leakage Gates
**Goal**: Users can produce a balance-first 80:20 train/SFT Eval Split that satisfies hard no-leak gates and reports balance quality.
**Depends on**: Phase 1
**Requirements**: SPLT-01, SPLT-02, SPLT-03, SPLT-04, SPLT-05, SPLT-06, SPLT-07, SPLT-08, TEST-02, TEST-03, TEST-04
**Success Criteria** (what must be TRUE):
  1. Split generation assigns whole Source Groups to exactly one split.
  2. Split output includes Source Group assignment and algorithm metadata for auditability.
  3. Source, original-audio, and model-ready URI overlap are hard failures.
  4. Balance reports show actual train/eval ratios and deltas across correlated factors.
**Plans**: 3 plans

Plans:
- [x] 02-01: Balance-first source-group split assignment
- [x] 02-02: Hard leakage validators
- [x] 02-03: Balance scoring and split reports

### Phase 3: GCS Artifacts And Model Writers
**Goal**: Users can write immutable canonical manifests, per-dataset slices, and NeMo/Whisper/Gemini model inputs for one dataset version.
**Depends on**: Phase 2
**Requirements**: ARTF-01, ARTF-02, ARTF-03, ARTF-04, ARTF-05, ARTF-06, MODL-01, MODL-02, MODL-03, MODL-04, MODL-05, MODL-06, MODL-07, MODL-08, TEST-05, TEST-06
**Success Criteria** (what must be TRUE):
  1. Dataset-version output paths are planned under `gs://wd-transcription-data/sft/{dataset_version_id}/`.
  2. Existing dataset-version paths fail without overwrite, resume, cleanup, or force.
  3. Canonical and per-dataset train/eval manifests are generated.
  4. NeMo, Whisper, and Gemini writers emit valid model-input shapes and config files.
  5. Existing benchmark/eval manifests are not modified.
**Plans**: 4 plans

Plans:
- [x] 03-01: GCS layout planner and overwrite protection
- [x] 03-02: Canonical and per-dataset artifact writers
- [x] 03-03: NeMo and Whisper model-input writers
- [ ] 03-04: Gemini model-input writer, config generation, and artifact publication

### Phase 4: Audio Derivation And Provenance
**Goal**: Users can reuse standalone clips or derive clips from longer labeled audio while preserving minimal transformation and auditable provenance.
**Depends on**: Phase 3
**Requirements**: AUD-01, AUD-02, AUD-03, AUD-04, AUD-05, AUD-06
**Success Criteria** (what must be TRUE):
  1. Rows already pointing to standalone clips are reused without unnecessary audio transformation.
  2. Rows pointing into longer files produce derived clips from offset/duration.
  3. Derived clips are mono when needed, unpadded, and not resampled by default.
  4. Every example records original audio, offset, duration, source group, split, and transformation provenance.
**Plans**: 3 plans

Plans:
- [ ] 04-01: Audio reuse/derive planning
- [ ] 04-02: Clip derivation execution and upload
- [ ] 04-03: Provenance and transformation report integration

### Phase 5: CLI, Reports, Docs, And Verification
**Goal**: Users can run dry-run and generation commands, inspect reports, and hand generated artifacts to SFT workflows with clear terminology.
**Depends on**: Phase 4
**Requirements**: CLI-01, CLI-02, CLI-03, CLI-04, CLI-05
**Success Criteria** (what must be TRUE):
  1. User can run a dry run that validates inputs and writes local reports without uploading derived audio.
  2. User can run generation that writes canonical/model inputs and reports to GCS.
  3. Hard validation failures exit nonzero and point to useful failure reports.
  4. Documentation explains Source Group, Labeled Segment, SFT Example, SFT Eval Split, and Dataset Version.
  5. End-to-end verification covers representative fixtures for all supported dataset families.
**Plans**: 3 plans

Plans:
- [ ] 05-01: Dry-run and generation CLI commands
- [ ] 05-02: Report bundle and failure UX
- [ ] 05-03: Documentation and end-to-end verification

## Progress

**Execution Order:**
Phases execute in numeric order: 1 -> 2 -> 3 -> 4 -> 5

| Phase | Plans Complete | Status | Completed |
|-------|----------------|--------|-----------|
| 1. Manifest And Source Identity | 3/3 | Complete | 2026-05-27 |
| 2. Split Engine And Leakage Gates | 3/3 | Complete | 2026-05-27 |
| 3. GCS Artifacts And Model Writers | 0/4 | Not started | - |
| 4. Audio Derivation And Provenance | 0/3 | Not started | - |
| 5. CLI, Reports, Docs, And Verification | 0/3 | Not started | - |
