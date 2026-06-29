# Roadmap: Gemini SFT Workflow Onboarding

## Overview

This roadmap turns the existing Gemini SFT and evaluation workflow into a
config-driven operator path. The phases follow the dependency chain surfaced by
research: lock the reporting contract first, normalize model target
configuration, package target execution with prompt/context parity, make
single-target durable eval GCS-authoritative, then document the stable workflow
and artifact hygiene rules. Scope stays within the stated anti-features: no
complex eval-sibling abstraction, no checkpoint-only primary CLI branch when a
unified model string works, no internal multi-target eval fan-out, no dataset
breakdown reports in this milestone, no local `results/` as source of truth, no
Linear automation, and no prompt file references.

## Phases

**Phase Numbering:**
- Integer phases (1, 2, 3): Planned milestone work
- Decimal phases (2.1, 2.2): Urgent insertions (marked with INSERTED)

Decimal phases appear between their surrounding integers in numeric order.

- [x] **Phase 1: Reporting Contract** - Stabilize metric semantics and shared report rendering across console, JSON, Markdown, batch eval, and checkpoint scoring. (completed 2026-06-28)
- [x] **Phase 2: Target Config** - Introduce unified model target configuration, validation, and explicit masked/unmasked config paths. (completed 2026-06-28)
- [x] **Phase 3: Target Execution** - Package backend-specific execution while preserving shared prompt, request, and prior-context behavior. (completed 2026-06-28)
- [x] **Phase 4: Durable Eval** - Run GCS-authoritative single-target evals, write normalized target manifests, and publish stable summary reports. (completed 2026-06-28)
- [x] **Phase 5: Operator Docs** - Document the stable operator workflow, example configs, metric interpretation, artifact sources, and commit hygiene checks. (completed 2026-06-29)

## Phase Details

### Phase 1: Reporting Contract
**Goal**: Operators and maintainers can trust one shared SFT eval metric and report contract across comparable batch and checkpoint paths.
**Depends on**: Nothing (first phase)
**Requirements**: RPT-01, RPT-02, RPT-03, RPT-04, RPT-05
**Success Criteria** (what must be TRUE):
  1. Operator can view a console-first eval report with WER, CER, keyword accuracy, empty-or-unintelligible rate, exact empty response rate, insertion/deletion/substitution counts, total reference word count, missing prediction count, and artifact URIs.
  2. Operator can open JSON and Markdown reports and see the same structured metric schema and report columns as the console output.
  3. Maintainer can verify exact empty responses are reported separately from empty-or-unintelligible responses in every report format.
  4. Maintainer can verify missing predictions remain in the WER/CER denominator as empty hypotheses across comparable batch eval and checkpoint scoring paths.
**Plans**:
  - **Wave 1**
    - `01-01` - Shared reporting foundation: create shared metric helpers,
      report schema, renderers, and contract tests.
  - **Wave 2** *(blocked on Wave 1 completion)*
    - `01-02` - Batch eval integration: render `gemini-sft eval` JSON,
      Markdown, and console output from the shared report schema.
    - `01-03` - Checkpoint scorer integration: render online checkpoint
      summaries and console output from the shared report schema.
**UI hint**: no

### Phase 2: Target Config
**Goal**: Operators can describe base models, tuned endpoints, checkpoint endpoints, and masked/unmasked evals through explicit validated configs before paid Vertex work starts.
**Depends on**: Phase 1
**Requirements**: CFG-01, CFG-02, CFG-03, CFG-04, CFG-05, CFG-06
**Success Criteria** (what must be TRUE):
  1. Operator can configure explicit model targets with a unified label/model shape that covers publisher/base models, tuned endpoints, and checkpoint endpoints.
  2. Operator gets a clear validation error for old eval configs that rely only on `base_model` plus the tuned endpoint stored in GCS `config.json`; the new explicit target config is required.
  3. Operator gets validation errors for missing, invalid, duplicate, or unsupported target fields before any paid Vertex operation starts.
  4. Operator can run masked and unmasked evals as separate labeled configs or manifests without an eval-sibling abstraction.
**Plans**: 3 plans
Plans:
**Wave 1**
- [x] 02-01-PLAN.md — Target config parser and artifact-label validation

**Wave 2** *(blocked on 02-01 completion)*
- [x] 02-02-PLAN.md — Durable config eval target guard
- [x] 02-03-PLAN.md — Target examples and masked/unmasked config shape
**UI hint**: no

### Phase 3: Target Execution
**Goal**: The packaged eval workflow executes the configured target through the correct backend while keeping prompt, request, and prior-context behavior identical across maintained paths.
**Depends on**: Phase 2
**Requirements**: EXEC-01, EXEC-02, EXEC-03, EXEC-04, EXEC-06
**Success Criteria** (what must be TRUE):
  1. Maintainer can verify SFT data generation, batch eval, online checkpoint scoring, and maintained notebooks reuse shared `common.gemini` prompt, request, generation, safety, and prior-context helpers.
  2. Operator can run evals where prior-context histories are built dynamically from same-source rows, source-order sorting, and usable previous transcripts, with no audio in prior turns when text-only prior context is selected.
  3. Operator can evaluate publisher/base and tuned targets through batch inference, and checkpoint endpoints through resumable online `generate_content`, unless live validation proves batch support.
  4. Operator can run smoke-limited or interrupted online evals with retry, row limit, and bounded concurrency controls while preserving partial predictions.
**Plans**: 4/4 plans complete
Plans:
**Wave 1**
- [x] 03-01-PLAN.md - Execution config and backend resolver

**Wave 2** *(blocked on 03-01 completion)*
- [x] 03-02-PLAN.md - Resumable online target execution

**Wave 3** *(blocked on 03-01 and 03-02 completion)*
- [x] 03-03-PLAN.md - Target-driven packaged eval integration

**Wave 4** *(blocked on 03-02 and 03-03 completion)*
- [x] 03-04-PLAN.md - Checkpoint scorer parity and execution docs
**UI hint**: no

### Phase 4: Durable Eval
**Goal**: Operators can run one configured target from durable GCS run state, with normalized target manifests and stable summary reports that do not depend on local result mirrors.
**Depends on**: Phase 3
**Requirements**: EXEC-05, DATA-01, DATA-02, DATA-03, DATA-04, DATA-05, DATA-06
**Success Criteria** (what must be TRUE):
  1. Operator can run exactly one configured target per packaged eval run and compare multiple targets by launching separate configs externally.
  2. Operator can evaluate from GCS `config.json` and receive one normalized inference manifest for the configured target label without using local `results/` as source of truth.
  3. Operator can view shared report columns for the target, including WER, CER, keyword accuracy, empty rates, edit counts, total reference word count, and row count metadata.
  4. Operator can follow report links to raw Vertex output, online prediction JSONL, normalized inference manifests, and GCS summary artifacts.
  5. Maintainer can verify existing batch or online outputs are reused only when they match the current config, target, prompt, eval manifest, and context settings.
**Plans**: 4/4 plans complete
Plans:
**Wave 1**
- [x] 04-01-PLAN.md — Singular eval model contract

**Wave 2** *(blocked on 04-01 completion)*
- [x] 04-02-PLAN.md — Batch request identity validation

**Wave 3** *(blocked on 04-01 and 04-02 completion)*
- [x] 04-03-PLAN.md — Durable single-target eval summaries

**Wave 4** *(blocked on 04-03 completion)*
- [x] 04-04-PLAN.md — Durable eval docs and checkpoint compatibility
**UI hint**: no

### Phase 5: Operator Docs
**Goal**: A new operator can follow documented commands and placeholder configs from manifests through reports while keeping local experiment artifacts out of source control.
**Depends on**: Phase 4
**Requirements**: DOC-01, DOC-02, DOC-03, DOC-04, DOC-05
**Success Criteria** (what must be TRUE):
  1. Operator can follow `model/scripts/sft/README.md` from manifests and config through prepare, tune, eval, checkpoint scoring, masked eval, and unmasked eval.
  2. Operator can start from placeholder example configs for base-only, tuned, checkpoint, masked, and unmasked evals without real local credentials or run artifacts.
  3. Operator can read the docs and understand every report metric, including exact empty response rate versus empty-or-unintelligible rate.
  4. Operator can distinguish durable GCS state from local cache/mirror outputs and identify files that must not be committed.
  5. Maintainer can run or follow a final artifact hygiene check that catches accidental commits of local `.local.toml`, raw prediction JSONL, inference outputs, or `results/` files.
**Plans**: 3/3 plans complete
Plans:
**Wave 1**
- [x] 05-01-PLAN.md — OKF runbook and README entrypoint
- [x] 05-02-PLAN.md — Config, metric, artifact, and hygiene references

**Wave 2** *(blocked on Wave 1 completion)*
- [x] 05-03-PLAN.md — Ignore rules and lightweight drift guards
**UI hint**: no

## Coverage

| Requirement | Phase |
|-------------|-------|
| RPT-01 | Phase 1 |
| RPT-02 | Phase 1 |
| RPT-03 | Phase 1 |
| RPT-04 | Phase 1 |
| RPT-05 | Phase 1 |
| CFG-01 | Phase 2 |
| CFG-02 | Phase 2 |
| CFG-03 | Phase 2 |
| CFG-04 | Phase 2 |
| CFG-05 | Phase 2 |
| CFG-06 | Phase 2 |
| EXEC-01 | Phase 3 |
| EXEC-02 | Phase 3 |
| EXEC-03 | Phase 3 |
| EXEC-04 | Phase 3 |
| EXEC-05 | Phase 4 |
| EXEC-06 | Phase 3 |
| DATA-01 | Phase 4 |
| DATA-02 | Phase 4 |
| DATA-03 | Phase 4 |
| DATA-04 | Phase 4 |
| DATA-05 | Phase 4 |
| DATA-06 | Phase 4 |
| DOC-01 | Phase 5 |
| DOC-02 | Phase 5 |
| DOC-03 | Phase 5 |
| DOC-04 | Phase 5 |
| DOC-05 | Phase 5 |

**Coverage Count**: 28/28 v1 requirements mapped
**Duplicate Mappings**: 0
**Unmapped Requirements**: 0

## Progress

**Execution Order:**
Phases execute in numeric order: 1 -> 2 -> 3 -> 4 -> 5

| Phase | Plans Complete | Status | Completed |
|-------|----------------|--------|-----------|
| 1. Reporting Contract | 3/3 | Complete   | 2026-06-28 |
| 2. Target Config | 3/3 | Complete   | 2026-06-28 |
| 3. Target Execution | 4/4 | Complete   | 2026-06-28 |
| 4. Durable Eval | 4/4 | Complete   | 2026-06-28 |
| 5. Operator Docs | 3/3 | Complete    | 2026-06-29 |

---
*Roadmap created: 2026-06-28*
