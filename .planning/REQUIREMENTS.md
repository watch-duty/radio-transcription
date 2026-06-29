# Requirements: Gemini SFT Workflow Onboarding

**Defined:** 2026-06-28
**Core Value:** A new operator can run and compare Gemini SFT/eval experiments
from explicit configs and console reports without reverse-engineering notebooks
or prior chat history.

## v1 Requirements

### Reporting Contract

- [x] **RPT-01**: Operator can view a console-first eval report with WER, CER,
  keyword accuracy, empty-or-unintelligible rate, exact empty response rate,
  insertion count, deletion count, substitution count, total reference word
  count, missing prediction count, and artifact URIs.
- [x] **RPT-02**: JSON and Markdown reports use the same structured metric
  schema as the console report.
- [x] **RPT-03**: Reports distinguish exact empty responses from the historical
  empty-or-unintelligible rate.
- [x] **RPT-04**: Missing model predictions are scored as empty hypotheses and
  remain in the WER/CER denominator.
- [x] **RPT-05**: Existing batch eval and checkpoint scoring paths can produce
  equivalent report columns for comparable targets.

### Config And Model Targets

- [x] **CFG-01**: Operator can configure one eval target per packaged eval run
  through one unified `[eval.model]` config shape.
- [x] **CFG-02**: A model target can represent a publisher/base model, tuned
  endpoint, or checkpoint endpoint without separate checkpoint-specific CLI
  options.
- [x] **CFG-03**: Target labels are validated for safe artifact paths and
  collision-free report columns.
- [x] **CFG-04**: Legacy eval target shapes, including `base_model` plus
  endpoint fallback and plural `[[eval.models]]` / `eval_models`, fail loudly
  so only the new config contract is supported.
- [x] **CFG-05**: Masked and unmasked evals can be run as separate
  configs/manifests with explicit labels and without an eval-sibling
  abstraction.
- [x] **CFG-06**: Config validation errors identify missing, invalid, or
  unsupported fields before paid Vertex work starts.

### Execution And Parity

- [x] **EXEC-01**: Eval builds prior-context histories dynamically from
  same-source rows, source-order sorting, and usable previous transcripts.
- [x] **EXEC-02**: SFT data generation, batch eval, online checkpoint scoring,
  and maintained notebooks reuse shared `common.gemini` prompt, request,
  generation, safety, and prior-context helpers.
- [x] **EXEC-03**: The packaged eval workflow chooses batch inference for
  batch-supported publisher/tuned targets and online `generate_content` for
  checkpoint endpoints unless live validation proves batch support.
- [x] **EXEC-04**: Online checkpoint execution supports resumable prediction
  writing, retry settings, row limits, and bounded concurrency.
- [x] **EXEC-05**: Packaged eval supports exactly one model target per run;
  operators compare base, tuned, and checkpoint targets by running separate
  configs externally when parallel comparison is needed.
- [x] **EXEC-06**: Operator can run a smoke-limited eval before full inference
  without changing the target config semantics.

### Durable Artifacts And Reports

- [x] **DATA-01**: Eval loads durable run state from GCS `config.json` and does
  not require local `results/` as source of truth.
- [x] **DATA-02**: Eval writes one normalized inference manifest per evaluated
  target, with artifact labels derived from validated target labels.
- [x] **DATA-03**: Successful eval uploads stable run-level summary JSON and
  Markdown reports under the GCS run prefix.
- [x] **DATA-04**: Reports include WER, CER, keyword accuracy,
  empty-or-unintelligible rate, exact empty response rate, insertion count,
  deletion count, substitution count, total reference word count, and row count
  metadata for the evaluated target.
- [x] **DATA-05**: Report output links to raw Vertex output, online prediction
  JSONL, normalized inference manifests, and GCS summary artifacts.
- [x] **DATA-06**: Existing batch or online outputs are reused only when they
  match the current config, target, prompt, eval manifest, and context settings.

### Operator Documentation And Hygiene

- [x] **DOC-01**: `model/scripts/sft/README.md` explains the config-driven
  operator path for prepare, tune, eval, checkpoint scoring, masked eval, and
  unmasked eval.
- [x] **DOC-02**: The repo contains placeholder example configs for common
  base-only, tuned, checkpoint, masked, and unmasked eval runs without real
  local run credentials or run artifacts.
- [x] **DOC-03**: Documentation explains every report metric, including exact
  empty response rate versus empty-or-unintelligible rate.
- [x] **DOC-04**: Documentation identifies which artifacts are durable GCS
  state, which are local cache/mirror outputs, and which files must not be
  committed.
- [x] **DOC-05**: Tests or docs include a final artifact hygiene check that
  prevents accidental commits of local `.local.toml`, raw prediction JSONL,
  inference outputs, or `results/` files.

## v2 Requirements

### Promotion Gates

- **PROM-01**: Operator can configure promotion thresholds by dataset and
  metric once the team agrees on acceptable regression bounds.
- **PROM-02**: Reports can emit a pass/fail promotion verdict based on those
  thresholds.

### Additional Slices

- **SLICE-00**: Reports can break down results by source dataset, including
  bcfy_calls, bcfy_feeds, echo, and fire_notifications.
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
| Internal multi-target eval fan-out | User requested one model per eval run; callers can run separate configs externally. |
| Dataset breakdown reports | User chose to skip dataset breakdowns in this milestone and revisit them later. |
| Local `results/` as authoritative state | GCS run prefixes and normalized inference manifests remain durable state. |
| Linear comment automation | Useful later, but not required for a new operator to run and compare experiments. |
| Local prompt files in config | Prompt text must be stored in GCS `config.json` for reproducible resume/eval. |
| Wide multi-model prediction manifest | Current normalized inference manifest convention is one prediction field per target artifact. |

## Traceability

Traceability populated during roadmap creation.

| Requirement | Phase | Status |
|-------------|-------|--------|
| RPT-01 | Phase 1 | Complete |
| RPT-02 | Phase 1 | Complete |
| RPT-03 | Phase 1 | Complete |
| RPT-04 | Phase 1 | Complete |
| RPT-05 | Phase 1 | Complete |
| CFG-01 | Phase 2 | Complete |
| CFG-02 | Phase 2 | Complete |
| CFG-03 | Phase 2 | Complete |
| CFG-04 | Phase 2 | Complete |
| CFG-05 | Phase 2 | Complete |
| CFG-06 | Phase 2 | Complete |
| EXEC-01 | Phase 3 | Complete |
| EXEC-02 | Phase 3 | Complete |
| EXEC-03 | Phase 3 | Complete |
| EXEC-04 | Phase 3 | Complete |
| EXEC-05 | Phase 4 | Complete |
| EXEC-06 | Phase 3 | Complete |
| DATA-01 | Phase 4 | Complete |
| DATA-02 | Phase 4 | Complete |
| DATA-03 | Phase 4 | Complete |
| DATA-04 | Phase 4 | Complete |
| DATA-05 | Phase 4 | Complete |
| DATA-06 | Phase 4 | Complete |
| DOC-01 | Phase 5 | Complete |
| DOC-02 | Phase 5 | Complete |
| DOC-03 | Phase 5 | Complete |
| DOC-04 | Phase 5 | Complete |
| DOC-05 | Phase 5 | Complete |

**Coverage:**
- v1 requirements: 28 total
- Mapped to phases: 28
- Unmapped: 0
- Duplicate mappings: 0

---
*Requirements defined: 2026-06-28*
*Last updated: 2026-06-29 after milestone audit reconciliation*
