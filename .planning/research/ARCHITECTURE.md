# Architecture Research: Gemini SFT/Eval Usability

**Domain:** Brownfield Gemini SFT and evaluation workflow onboarding  
**Researched:** 2026-06-28  
**Confidence:** HIGH for in-repo architecture, MEDIUM for Vertex batch-versus-endpoint service behavior

## Executive Summary

The next SFT/eval usability work should extend the existing `gemini_sft` workflow rather than introduce another script framework. The current architecture already has the right split: `gemini_sft` owns operator config, run state, and CLI orchestration; `common.gemini` owns prompt, prior-context, request, Vertex, and batch mechanics; `common.scoring` owns ASR metric primitives; GCS run prefixes are the durable state of record.

The highest-leverage change is to make configured model targets first-class inside `gemini_sft.config` and `gemini_sft.evaluate`. A target can be a base model, tuned endpoint, or checkpoint endpoint, but the operator should see one `models`-style config concept instead of separate checkpoint-specific flags. Execution strategy should be explicit or derived at validation time: batch for base model IDs and batch-capable tuned resources, online `generate_content` for checkpoint endpoints and any resource type that cannot run through Vertex batch.

Dataset breakdowns, exact empty-response reporting, total reference word count, and console-first summaries should be package-level SFT evaluation features, not checkpoint-script-only behavior. Move duplicated metric assembly from `model/src/gemini_sft/evaluate.py` and `model/scripts/sft/score_gemini_sft_checkpoints_online.py` into one importable `gemini_sft.metrics` module, then let `gemini_sft.records` render JSON, Markdown, and console text from the same summary object.

Prior context should stay owned by `common.gemini.context`. Config should validate count and mode; evaluation should build histories once from canonical eval source rows; target execution should consume those histories without recomputing or modifying them. This preserves prompt parity across SFT JSONL, batch eval, online checkpoint eval, and notebooks.

## Recommended Architecture

### System Overview

```text
operator TOML
    |
    v
gemini_sft.config
  - validate run fields
  - derive GCS paths
  - resolve prompts
  - resolve model targets
    |
    v
GCS run prefix: gs://<bucket>/sft/runs/<round-id>/
  - config.json
  - canonical manifests
  - tuning status
  - eval artifacts
    |
    v
gemini_sft.evaluate
  - load durable config.json
  - load canonical eval manifest
  - build prior histories once
  - dispatch target execution
    |
    +--> common.gemini.batch + common.gemini.vertex
    |      batch input/output for batch-capable targets
    |
    +--> gemini_sft.target_execution
           online generate_content for checkpoint/endpoints that need it
    |
    v
prediction maps keyed by audio_filepath
    |
    +--> common.inference_manifest
    |      normalized inference manifests
    |
    +--> gemini_sft.metrics
           overall and per-dataset scores
    |
    v
gemini_sft.records
  - console summary
  - wer/eval summary JSON
  - Markdown report
  - ledger row
```

### Component Responsibilities

| Component | Should Own | Should Not Own |
|-----------|------------|----------------|
| `gemini_sft.config` | TOML parsing, validation, defaults, derived GCS paths, prompt resolution, `models`/target definitions, backwards compatibility with `base_model` plus `endpoint` | GCS downloads, Vertex calls, scoring formulas, console rendering |
| `gemini_sft.prepare` | Copy canonical manifests, build train/validation Gemini SFT JSONL, preflight, write durable run config | Eval target selection, scoring, checkpoint endpoint discovery |
| `gemini_sft.tune` | Submit/resume tuning jobs, persist `job_name`, endpoint, and tuning status in GCS config/state | Eval-time model target execution |
| `gemini_sft.evaluate` | CLI orchestration: load durable config, load eval rows, build histories, call target execution, call metrics/report writers | Low-level Vertex request shape, retry loops, Markdown formatting details |
| `gemini_sft.target_execution` or `gemini_sft.inference` | Execute one validated model target and return prediction map plus provenance; choose batch vs online strategy; manage online checkpoint resume JSONL | TOML parsing, metric formulas, prompt/context construction rules |
| `common.gemini.context` | Same-source prior-context grouping, ordering, filtering, and prompt text helpers | Target selection, manifest IO, scoring |
| `common.gemini.vertex` | Canonical Gemini request dicts, safety/generation constants, tuning job calls, batch job submission/polling, batch output parsing | SFT run state, local `results/`, dataset reports |
| `common.gemini.batch` | Build/upload batch JSONL, reuse existing batch output, parse batch predictions | Metrics, console reports, config parsing |
| `common.scoring` | WER/CER, keyword metric primitives, hallucination/empty-unintelligible rate, duration bucket primitives, bootstrap | SFT-specific summary schemas or dataset grouping |
| `gemini_sft.metrics` | SFT eval score schema, exact empty-response rate, total reference word count, insertion/deletion/substitution counts, overall and dataset-breakdown scoring | Markdown/GCS writing, provider calls |
| `gemini_sft.records` | Render and write JSON, Markdown, ledger, and console-first summaries from metric objects | Recomputing metrics, running Vertex jobs |
| `common.inference_manifest` | Normalized inference manifest rows and GCS paths | Wide comparison reports or model execution |
| `model/scripts/sft/*` | Thin compatibility/experimental wrappers around package modules | Long-term ownership of workflow logic |

## Target Model Execution

Use a small target abstraction inside `gemini_sft`, not a generic plugin framework. The target object should be serializable into `config.json` so GCS remains authoritative after local TOML files disappear.

Recommended shape:

```python
@dataclass(frozen=True)
class ModelTarget:
    label: str
    model: str
    kind: Literal["base", "tuned_endpoint", "checkpoint"]
    execution: Literal["batch", "online"]
    artifact_label: str
    checkpoint_id: str | None = None
    epoch: str | None = None
    step: str | None = None
```

Rules:

- `gemini_sft.config` validates labels as safe path components and prevents duplicate labels.
- Existing configs without `models` should expand to a base target and, when `config.json` has `endpoint`, a tuned target. This keeps current runs usable.
- Checkpoint discovery from `job_name` can happen in a packaged helper, but the discovered endpoints should be materialized as `ModelTarget` records before execution begins.
- Each target writes under `evals/<artifact_label>/` or `evals/checkpoints/<artifact_label>/` and never reuses another target's output path.
- Target execution returns one canonical `PredictionResult` with `predictions_by_audio_uri`, raw provider output URI, local/GCS prediction artifact URIs when applicable, and error counts.

Keep the low-level provider split:

- Batch targets call `common.gemini.batch.run_batch_audio_inference`.
- Online targets reuse the checkpoint scorer's async `generate_content` loop, but move it into an importable package module.
- Both strategies must use `common.gemini.vertex.build_request` so prompt shape, history turns, safety settings, and generation settings do not drift.

## Data Flow

### Prepare/Tune Flow

```text
operator TOML
  -> gemini_sft.config.load_run_config
  -> RunConfig.to_record_dict
  -> gemini_sft.prepare
  -> GCS config.json + canonical manifests + Gemini train/validation JSONL
  -> gemini_sft.tune
  -> GCS config.json updated with job_name and endpoint
```

### Eval Flow

```text
gemini-sft eval --config run.toml
  -> load_eval_run_config for path/run identity
  -> download GCS config.json
  -> download GCS canonical_eval_uri
  -> canonical_rows_from_entries returns:
       source_rows: original manifest dicts
       eval_rows: parsed CanonicalRow objects
  -> common.gemini.context.build_context_histories(source_rows)
  -> execute each ModelTarget
  -> align predictions by eval_rows[*].audio_filepath
  -> upload normalized inference manifests
  -> gemini_sft.metrics builds overall and dataset breakdown scores
  -> gemini_sft.records writes console, JSON, Markdown, ledger
```

### Dataset Breakdown Flow

Dataset breakdowns should use `source_rows` because parsed `CanonicalRow` objects intentionally expose the canonical scoring fields, not every dataset provenance field.

```text
source_rows[i] + eval_rows[i] + target_hypotheses[i]
  -> group key from source_rows[i]:
       dataset_name
       else dataset_family
       else source_group
       else configured dataset
  -> score each group with the same metric builder used for the overall result
```

The required families `bcfy_calls`, `bcfy_feeds`, `echo`, and `fire_notifications` should be report groups, not hard-coded execution branches. If a group is absent, the report should show it as absent or omit it with an explicit `missing_expected_groups` field in JSON.

## Where To Reuse Existing Code

| Need | Reuse | Notes |
|------|-------|-------|
| Config parsing | `model/src/gemini_sft/config.py` | Extend this module for `models`; do not parse target resources in scripts. |
| Canonical eval rows | `gemini_sft.artifacts.canonical_rows_from_entries` | Keeps validation before any paid inference. |
| Prior context | `common.gemini.context.build_context_histories` | Build once, pass to every target. |
| Prompt/request parity | `common.gemini.vertex.build_request` | Online and batch must share this function. |
| Batch inference | `common.gemini.batch.run_batch_audio_inference` | Already handles duplicate URI rejection, batch input upload, output reuse, and missing predictions. |
| Online checkpoint scoring | Logic currently in `model/scripts/sft/score_gemini_sft_checkpoints_online.py` | Move into package module; keep script as wrapper if needed. |
| Metric primitives | `common.scoring` | Add exact empty-response rate and total reference word count in SFT metric layer unless they become broadly reusable. |
| Existing summary output | `gemini_sft.records.write_wer_summary` and `append_ledger` | Expand rendering instead of creating another report writer. |
| Normalized predictions | `common.inference_manifest.upload_inference_manifest` | Continue one normalized manifest per target artifact label. |

## Console Output Ownership

Console output should be rendered by `gemini_sft.records` and invoked by the CLI layer. Execution functions should log progress and return structured results; they should not print final summaries.

Recommended console sections:

```text
Run: <round_id>
Eval manifest: <canonical_eval_uri>
Targets: base, tuned, checkpoint_7, ...

Headline
| target | WER | CER | keyword_accuracy | hallucination_rate | empty_response_rate | total_ref_words |

Dataset Breakdown
| dataset | target | n | WER | CER | keyword_accuracy | empty_response_rate |

Artifacts
- <target>: raw output URI
- <target>: normalized inference manifest URI
```

This keeps console output useful for operators while making JSON/Markdown reports derive from the same data.

## Build Order Implications

1. **Consolidate metrics first.** Extract `score_predictions`, exact empty-response rate, total reference word count, and insertion/deletion/substitution reporting into `gemini_sft.metrics`. Update `evaluate.py` and the checkpoint scorer to use it without changing behavior.

2. **Move online checkpoint execution into the package.** Extract checkpoint endpoint discovery, resumable online prediction JSONL, async concurrency/retry logic, and summary writing from `model/scripts/sft/score_gemini_sft_checkpoints_online.py`. Keep the script as a thin wrapper during migration.

3. **Add model target config.** Extend `gemini_sft.config` with validated `models` support and a backwards-compatible adapter for current `base_model` plus stored `endpoint`. Persist resolved targets into GCS `config.json`.

4. **Refactor eval into a target loop.** `gemini_sft.evaluate` should build eval rows and histories once, then run each target through the shared target-execution module. Base/tuned batch targets and online checkpoint targets should produce the same `PredictionResult` contract.

5. **Add dataset breakdown reporting.** Group by dataset metadata from source rows and render overall plus per-dataset panels through `gemini_sft.records`.

6. **Update docs and examples.** Revise `model/scripts/sft/README.md` and `run_config.example.toml` with the model-target shape, console output expectations, and checkpoint scoring path.

## Risks And Mitigations

### Batch Inference vs Online Checkpoint Inference

**Risk:** Vertex batch and online `generate_content` do not have identical resource support. The current SDK docs show `client.batches.create(model=..., src=...)` for batch prediction and `client.models.generate_content(model=tuning_job.tuned_model.endpoint, ...)` for tuned endpoints, but the docs found here do not establish that checkpoint endpoints are batch-capable. The in-repo checkpoint scorer explicitly exists because checkpoint endpoints need online scoring.

**Mitigation:** Keep `execution` explicit per target. Default base model IDs to batch. Default checkpoint endpoints to online. Treat tuned endpoint batch support as a capability to validate with a smoke run before relying on it for a phase. Do not collapse all model resources into a batch-only path.

### Report Drift

**Risk:** `evaluate.py` and the checkpoint scorer currently assemble overlapping but different metric fields. This has already produced a split between `base_empty_rate` in batch eval and `empty_response_rate` in checkpoint eval.

**Mitigation:** Create one SFT metric schema and renderer. Include both hallucination/empty-unintelligible rate and exact empty-response rate with distinct names.

### Prompt Drift Between Batch And Online

**Risk:** Online checkpoint scoring manually converts the batch request into `contents` and a `GenerateContentConfig`. Future prompt or generation changes could update one path but not the other.

**Mitigation:** Keep `build_request` as the canonical request constructor. If the online path needs SDK-specific objects, add a tiny conversion helper near `common.gemini.vertex` constants and cover it with drift-guard tests.

### Target Output Collisions

**Risk:** Multiple configured models running in parallel can overwrite `evals/base`, `evals/tuned`, or checkpoint paths if labels are not validated.

**Mitigation:** Require unique safe labels in `gemini_sft.config`; derive default artifact labels from target kind and checkpoint ID; reject duplicate artifact labels before inference.

### Stale Base Predictions

**Risk:** The current checkpoint scorer loads base predictions from `evals/base/output/`. If the base output was produced with a different manifest, prompt, prior-context mode, or row limit, checkpoint deltas are misleading.

**Mitigation:** Unified eval should run or verify the base target in the same eval invocation. If reusing existing base output, compare config hash, canonical eval URI, prompt fields, prior context settings, and target model before scoring deltas.

### Online Quota And Partial Progress

**Risk:** Online checkpoint scoring is concurrency-sensitive and can hit quota, timeout, or return empty responses. Unlike batch jobs, progress is row-level and must be flushed frequently.

**Mitigation:** Keep conservative concurrency defaults, row-level retry/error capture, `sync_every`, resumable prediction JSONL in GCS, and explicit error counts in the summary. Missing or failed rows should score as empty hypotheses, matching current deletion-denominator semantics.

### Dataset Metadata Inconsistency

**Risk:** Older canonical manifests may use `dataset_name`, `dataset_family`, `source_group`, or only source URI-derived provenance.

**Mitigation:** Centralize group-key extraction in `gemini_sft.metrics` and record the field used. Report unknown groups explicitly rather than silently dropping rows.

## Anti-Patterns To Avoid

### Parallel Script Framework

Do not build a second eval runner under `model/scripts/sft` that parses its own config and writes separate summaries. Scripts should wrap package modules.

### Static Prior Context In Manifests

Do not store computed prior-turn text as durable manifest fields for this workflow. Prior context should remain dynamic from same-source rows and current mode/count so SFT data generation and eval stay identical.

### Local Results As Source Of Truth

Do not make `results/` required for dataset breakdowns or checkpoint comparisons. Local files are a mirror/cache. GCS config, canonical manifests, raw provider outputs, and normalized inference manifests must be sufficient to recompute reports.

### Model-Specific Metrics

Do not let batch eval and checkpoint eval define separate metric schemas. The target execution strategy may differ, but scoring and reporting should not.

## Phase Structure Recommendation

1. **Shared scoring/report foundation** - Lowest risk and unlocks console/dataset work. Extract duplicated metric logic and define one result schema.

2. **Packaged online target execution** - Moves checkpoint-specific behavior under `gemini_sft` while preserving the proven async/resume implementation.

3. **Model target config and eval target loop** - Introduces the operator-facing `models` field and makes base/tuned/checkpoint execution consistent.

4. **Dataset breakdown and console-first reports** - Adds the main usability output once all targets produce the same result contract.

5. **Docs and deprecation cleanup** - Updates README/examples and reduces scripts to wrappers after package behavior is covered by tests.

## Confidence Assessment

| Area | Confidence | Notes |
|------|------------|-------|
| In-repo component boundaries | HIGH | Verified against `.planning/PROJECT.md`, codebase architecture map, `gemini_sft` modules, and `common.gemini` modules. |
| Prior-context ownership | HIGH | `common.gemini.context` is already the shared implementation used by batch eval and checkpoint scoring. |
| Config ownership | HIGH | `gemini_sft.config` already validates TOML, prompts, GCS paths, tuning settings, and prior-context mode/count. |
| Reporting consolidation | HIGH | Duplicate metric/reporting code exists in `evaluate.py`, `records.py`, and checkpoint scorer; consolidation path is clear. |
| Vertex batch support for endpoints/checkpoints | MEDIUM | Current SDK docs confirm batch jobs and tuned endpoint `generate_content`, but not checkpoint endpoint batch support. In-repo script comments and design imply checkpoint online scoring is required. |

## Sources

- `.planning/PROJECT.md` - project requirements and active decisions.
- `.planning/codebase/ARCHITECTURE.md` - current repository architecture map.
- `.planning/codebase/STRUCTURE.md` - package and generated artifact conventions.
- `model/src/common/gemini/batch.py` - reusable batch inference orchestration.
- `model/src/common/gemini/context.py` - prior context construction.
- `model/src/common/gemini/vertex.py` - request construction, Vertex tuning, batch submission, batch parsing.
- `model/src/gemini_sft/config.py` - config parsing and durable run-path contract.
- `model/src/gemini_sft/evaluate.py` - current batch eval orchestration and metric assembly.
- `model/src/gemini_sft/records.py` - current JSON/Markdown/ledger writers.
- `model/src/gemini_sft/artifacts.py` - canonical manifest loading and GCS/local artifact helpers.
- `model/src/common/scoring.py` - scoring primitive ownership.
- `model/src/common/inference_manifest.py` - normalized inference manifest contract.
- `model/scripts/sft/score_gemini_sft_checkpoints_online.py` - current online checkpoint scorer.
- `model/scripts/sft/README.md` - operator workflow docs and run artifact contract.
- Context7, Google Gen AI Python SDK docs, `/googleapis/python-genai` - Vertex client initialization, `client.batches.create`, tuning, and tuned endpoint `generate_content` examples.

---
*Architecture research for Gemini SFT/eval usability onboarding.*
