# Requirements: Gemini SFT Workflow Onboarding

**Defined:** 2026-06-28
**Core Value:** A new operator can run and compare Gemini SFT/eval experiments
from explicit configs and console reports without reverse-engineering notebooks
or prior chat history.

## v1 Requirements

### Reporting Contract

- [ ] **RPT-01**: Operator can view a console-first eval report with WER, CER,
  keyword accuracy, empty-or-unintelligible rate, exact empty response rate,
  insertion count, deletion count, substitution count, total reference word
  count, missing prediction count, and artifact URIs.
- [ ] **RPT-02**: JSON and Markdown reports use the same structured metric
  schema as the console report.
- [ ] **RPT-03**: Reports distinguish exact empty responses from the historical
  empty-or-unintelligible rate.
- [ ] **RPT-04**: Missing model predictions are scored as empty hypotheses and
  remain in the WER/CER denominator.
- [ ] **RPT-05**: Existing batch eval and checkpoint scoring paths can produce
  equivalent report columns for comparable targets.

### Config And Model Targets

- [ ] **CFG-01**: Operator can configure one or more eval targets through one
  unified `models`-style config shape.
- [ ] **CFG-02**: A model target can represent a publisher/base model, tuned
  endpoint, or checkpoint endpoint without separate checkpoint-specific CLI
  options.
- [ ] **CFG-03**: Target labels are validated for safe artifact paths and
  collision-free report columns.
- [ ] **CFG-04**: Existing configs that use `base_model` and a tuned endpoint
  in GCS `config.json` remain evaluable during migration.
- [ ] **CFG-05**: Masked and unmasked evals can be run as separate
  configs/manifests with explicit labels and without an eval-sibling
  abstraction.
- [ ] **CFG-06**: Config validation errors identify missing, invalid, or
  unsupported fields before paid Vertex work starts.

### Execution And Parity

- [ ] **EXEC-01**: Eval builds prior-context histories dynamically from
  same-source rows, source-order sorting, and usable previous transcripts.
- [ ] **EXEC-02**: SFT data generation, batch eval, online checkpoint scoring,
  and maintained notebooks reuse shared `common.gemini` prompt, request,
  generation, safety, and prior-context helpers.
- [ ] **EXEC-03**: The packaged eval workflow chooses batch inference for
  batch-supported publisher/tuned targets and online `generate_content` for
  checkpoint endpoints unless live validation proves batch support.
- [ ] **EXEC-04**: Online checkpoint execution supports resumable prediction
  writing, retry settings, sync cadence, log cadence, row limits, and bounded
  concurrency.
- [ ] **EXEC-05**: Multiple configured targets run in parallel by default when
  doing so is safe for their backend and configured concurrency.
- [ ] **EXEC-06**: Operator can run a smoke-limited eval before full inference
  without changing the target config semantics.

### Durable Artifacts And Dataset Reports

- [ ] **DATA-01**: Eval loads durable run state from GCS `config.json` and does
  not require local `results/` as source of truth.
- [ ] **DATA-02**: Eval writes one normalized inference manifest per evaluated
  target, with artifact labels derived from validated target labels.
- [ ] **DATA-03**: Reports include dataset breakdowns for bcfy_calls,
  bcfy_feeds, echo, and fire_notifications when those groups are present.
- [ ] **DATA-04**: Dataset breakdowns include WER, CER, keyword accuracy,
  empty-or-unintelligible rate, exact empty response rate, insertion count,
  deletion count, substitution count, total reference word count, and row count.
- [ ] **DATA-05**: Report output links to raw Vertex output, online prediction
  JSONL, normalized inference manifests, and GCS summary artifacts.
- [ ] **DATA-06**: Existing batch or online outputs are reused only when they
  match the current config, target, prompt, eval manifest, and context settings.

### Operator Documentation And Hygiene

- [ ] **DOC-01**: `model/scripts/sft/README.md` explains the config-driven
  operator path for prepare, tune, eval, checkpoint scoring, masked eval, and
  unmasked eval.
- [ ] **DOC-02**: The repo contains placeholder example configs for common
  base-only, tuned, checkpoint, masked, and unmasked eval runs without real
  local run credentials or run artifacts.
- [ ] **DOC-03**: Documentation explains every report metric, including exact
  empty response rate versus empty-or-unintelligible rate.
- [ ] **DOC-04**: Documentation identifies which artifacts are durable GCS
  state, which are local cache/mirror outputs, and which files must not be
  committed.
- [ ] **DOC-05**: Tests or docs include a final artifact hygiene check that
  prevents accidental commits of local `.local.toml`, raw prediction JSONL,
  inference outputs, or `results/` files.

## v2 Requirements

### Promotion Gates

- **PROM-01**: Operator can configure promotion thresholds by dataset and
  metric once the team agrees on acceptable regression bounds.
- **PROM-02**: Reports can emit a pass/fail promotion verdict based on those
  thresholds.

### Additional Slices

- **SLICE-01**: Reports can break down results by duration bucket.
- **SLICE-02**: Reports can break down results by prior-context depth.
- **SLICE-03**: Reports can compare prompt/context families across experiments.

### Automation

- **AUTO-01**: Stable JSON reports can be consumed by Linear, PR comments, or
  release-note automation after the operator workflow is stable.

## Out of Scope

| Feature | Reason |
|---------|--------|
| New transcription model architecture | This project improves Gemini SFT/eval operations around existing model APIs. |
| Production ingestion pipeline redesign | The target is research/operator workflow usability, not runtime pipeline behavior. |
| Complex eval-sibling abstraction | User requested simpler separate configs/manifests for masked and unmasked eval. |
| Checkpoint-only primary CLI branch | Checkpoints should be represented as model targets where possible. |
| Local `results/` as authoritative state | GCS run prefixes and normalized inference manifests remain durable state. |
| Linear comment automation | Useful later, but not required for a new operator to run and compare experiments. |
| Local prompt files in config | Prompt text must be stored in GCS `config.json` for reproducible resume/eval. |
| Wide multi-model prediction manifest | Current normalized inference manifest convention is one prediction field per target artifact. |

## Traceability

Traceability populated during roadmap creation.

| Requirement | Phase | Status |
|-------------|-------|--------|
| RPT-01 | Phase 1 | Pending |
| RPT-02 | Phase 1 | Pending |
| RPT-03 | Phase 1 | Pending |
| RPT-04 | Phase 1 | Pending |
| RPT-05 | Phase 1 | Pending |
| CFG-01 | Phase 2 | Pending |
| CFG-02 | Phase 2 | Pending |
| CFG-03 | Phase 2 | Pending |
| CFG-04 | Phase 2 | Pending |
| CFG-05 | Phase 2 | Pending |
| CFG-06 | Phase 2 | Pending |
| EXEC-01 | Phase 3 | Pending |
| EXEC-02 | Phase 3 | Pending |
| EXEC-03 | Phase 3 | Pending |
| EXEC-04 | Phase 3 | Pending |
| EXEC-05 | Phase 4 | Pending |
| EXEC-06 | Phase 3 | Pending |
| DATA-01 | Phase 4 | Pending |
| DATA-02 | Phase 4 | Pending |
| DATA-03 | Phase 4 | Pending |
| DATA-04 | Phase 4 | Pending |
| DATA-05 | Phase 4 | Pending |
| DATA-06 | Phase 4 | Pending |
| DOC-01 | Phase 5 | Pending |
| DOC-02 | Phase 5 | Pending |
| DOC-03 | Phase 5 | Pending |
| DOC-04 | Phase 5 | Pending |
| DOC-05 | Phase 5 | Pending |

**Coverage:**
- v1 requirements: 28 total
- Mapped to phases: 28
- Unmapped: 0
- Duplicate mappings: 0

---
*Requirements defined: 2026-06-28*
*Last updated: 2026-06-28 after roadmap creation*
