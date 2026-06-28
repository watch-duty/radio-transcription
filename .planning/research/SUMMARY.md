# Project Research Summary

**Project:** Gemini SFT Workflow Onboarding
**Domain:** Brownfield Gemini SFT/evaluation operator workflow for Watch Duty radio transcription
**Researched:** 2026-06-28
**Confidence:** HIGH overall for repo-local direction; MEDIUM for live Vertex checkpoint/batch service behavior

## Executive Summary

This project is not a new model, service, notebook suite, or production transcription redesign. It is an operator workflow upgrade for the existing Gemini SFT/evaluation path in the Watch Duty radio-transcription repo: a new teammate should be able to start from explicit configs, run `prepare`, `tune`, `eval`, and checkpoint scoring, then read a console/GCS-linked report without prior experiment context. Experts would build this as a small extension of the current packaged workflow, not as a parallel orchestration layer.

The recommended approach is to keep the stack anchored in the `model/` Python package, the existing `gemini-sft` CLI, GCS-authoritative run prefixes, shared `common.gemini` prompt/request/context helpers, and `common.scoring` metric primitives. The main product changes are a unified `models`-style target config, shared reporting/metric schema, package-owned online checkpoint execution, dataset breakdowns, and docs/examples that make masked and unmasked workflows explicit through separate configs or manifests.

The key risks are metric ambiguity, prompt/prior-context drift, stale local or batch outputs, and forcing every model-like resource through one Vertex backend. Mitigate these by defining the reporting contract first, preserving dynamic prior-context construction, treating local `results/` only as a cache, validating target backend/resource/location before paid work, and keeping checkpoint endpoints routed to online `generate_content` unless live API validation proves batch support.

## Key Findings

### Recommended Stack

The right stack is the existing config-driven Python CLI inside the lightweight ASR runtime. Do not add an app, workflow engine, notebook-first path, new CLI framework, rich terminal UI, external experiment tracker, or production pipeline dependency. Durable state should remain in GCS under `gs://<bucket>/sft/runs/<round-id>/`; local `results/<round-id>/` should only mirror reports and cache artifacts.

**Core technologies:**
- Python `model/` package, Python >=3.11: owns ASR research helpers and `gemini-sft`; avoids mixing with root runtime dependencies.
- `gemini-sft` CLI: existing operator entry point for `prepare`, `tune`, and `eval`; extend it rather than adding a new command family.
- TOML plus stdlib `tomllib`: human-authored config with explicit dataclass validation; enough for this workflow without Pydantic/Typer.
- GCS with `google-cloud-storage>=2.19` (lock currently `3.10.1`): durable run config, manifests, model inputs, Vertex outputs, reports, and normalized inference manifests.
- Vertex AI/Gemini through `google-genai>=2.10,<3` (lock currently `2.10.0`): SFT tuning, batch inference, and online checkpoint/endpoint scoring through existing `common.gemini.vertex` helpers.
- Lightweight ASR Docker runtime, `asr-eval-docker-compose.yml` service `notebooks-cpu`: default operator environment with model extras installed editable.
- Google Cloud ADC and mounted gcloud config: required auth boundary for GCS and Vertex.

**Critical version notes:**
- Use the model package environment for SFT/eval because root and model lockfiles can carry different `google-genai` versions.
- Keep `jiwer>=3.1,<4` and `nemo_text_processing==1.1.0` behavior covered by tests because scoring/normalization changes affect WER.
- Unit tests should mock GCS/Vertex; live Vertex tuning, batch inference, and checkpoint sweeps are operator validation, not CI.

### Expected Features

The MVP should make the existing workflow runnable and interpretable by a new operator. The strongest feature dependency is that reporting semantics must stabilize before docs and comparison UX are polished.

**Must have (table stakes):**
- Documented operator path from config to GCS-linked report for base-only, tuned, checkpoint, masked, and unmasked examples.
- Committable placeholder example configs for common eval modes; no real run TOMLs or raw predictions in git.
- Unified `models`-style eval target config for base models, tuned endpoints, and checkpoint resources.
- Console-first comparison report with WER, CER, keyword accuracy, hallucination/empty-or-unintelligible rate, exact empty response rate, insertions, deletions, substitutions, total reference word count, deltas, and artifact URIs.
- Batch/checkpoint report parity through a shared scoring/report schema.
- Dataset breakdowns for `bcfy_calls`, `bcfy_feeds`, `echo`, and `fire_notifications` from source-row metadata, not local summaries.
- Dynamic prior-context parity across SFT JSONL, batch eval, checkpoint scoring, and notebooks.
- Masked and unmasked eval as separate configs/manifests, with explicit labels and artifact URIs.
- Smoke-limit, resume, and mocked verification paths for slow or paid work.

**Should have (after core path is stable):**
- Multi-model parallel execution when multiple targets are supplied, with explicit concurrency limits.
- Ranked scoreboard across base, tuned, and checkpoint candidates with deltas and empty/keyword tradeoffs.
- Tuning-job checkpoint discovery exposed through the same model-target model.
- Re-score/report generation from existing normalized inference manifests.
- Config lint/dry-run command for validation before paid operations.
- Stable machine-readable report schema for future automation consumers.

**Defer (v2+):**
- Promotion-gate thresholds until the team agrees on regression bounds by metric and dataset.
- Additional slice reports beyond dataset, such as duration, history depth, and prompt/context family.
- Linear or PR comment automation; stable JSON/Markdown reports are enough for this milestone.

**Explicit anti-features:**
- No complex eval-sibling abstraction for masked/unmasked runs.
- No checkpoint-only CLI branch as the primary interface if the target abstraction works.
- No local `results/` as source of truth.
- No Linear automation in this milestone.
- No local prompt file references, notebook-first operation, wide multi-model prediction manifests, or implicit paid Vertex work.

### Architecture Approach

Extend the current package boundaries. `gemini_sft` should own operator config, run state, CLI orchestration, target definitions, evaluation coordination, SFT-specific metrics, and report rendering. `common.gemini` should remain the owner of prompt text, prior-context construction, Vertex request shape, safety/generation settings, tuning, batch submission, and response parsing. `common.scoring` should remain the metric primitive owner. GCS run prefixes and normalized inference manifests should be sufficient to resume and recompute reports without local files.

**Major components:**
1. `gemini_sft.config`: parse TOML, validate fields, derive GCS paths, resolve prompts, validate `models` targets, and preserve backward compatibility with `base_model` plus stored `endpoint`.
2. `gemini_sft.evaluate`: load durable GCS config and canonical eval rows, build histories once, execute validated targets, align predictions by eval rows, upload normalized manifests, and invoke metric/report writers.
3. `gemini_sft.target_execution` or `gemini_sft.inference`: package the target backend split. Batch targets call `common.gemini.batch`; online checkpoint targets reuse the current async/resumable checkpoint scorer behavior through package code.
4. `gemini_sft.metrics`: define the SFT evaluation score schema, exact empty response rate, missing prediction count, total reference word count, insertion/deletion/substitution fields, and overall/dataset scoring.
5. `gemini_sft.records`: render console, JSON, Markdown, and ledger output from structured metric objects without recomputing metrics.
6. `common.gemini.context` and `common.gemini.vertex`: stay canonical for prior histories and request construction so batch, online, SFT data generation, and notebooks do not drift.
7. `common.inference_manifest`: continue one normalized inference manifest per target label, not a wide multi-model canonical artifact.

### Critical Pitfalls

1. **Ambiguous empty-rate metrics**: display `exact_empty_response_rate` separately from `empty_or_unintelligible_rate`; keep legacy aliases only as deprecated compatibility fields and test `""` versus `[UNINTELLIGIBLE]`.
2. **Missing predictions disappear from denominator**: build hypotheses by iterating eval rows, score missing predictions as `""`, and report `missing_prediction_count` separately.
3. **Local `results/` becomes source of truth**: every eval/scoring path should load GCS `config.json`, print GCS artifact URIs, and treat local outputs as cache/mirror only.
4. **Prompt/request or prior-context drift**: route all maintained paths through `common.gemini.prompts`, `common.gemini.vertex.build_request`, shared generation/safety settings, and `common.gemini.context.build_context_histories`.
5. **Wrong Vertex backend for checkpoints**: resolve each target to an explicit backend; default publisher/base resources to batch and checkpoint endpoints to online scoring unless live validation proves otherwise.
6. **Config becomes a workflow language**: keep one explicit run config with one eval manifest, one prompt pair, one context mode/count, and a simple list of target resources.
7. **Experiment artifacts leak into git**: commit placeholders and docs only; local `.local.toml`, raw predictions, scoring summaries, and `results/` stay untracked unless explicitly curated.

## Implications for Roadmap

Based on research, suggested phase structure:

### Phase 1: Reporting Contract

**Rationale:** Metric naming and denominator rules must be stable before docs, console output, and multi-target comparisons can be trusted.
**Delivers:** Shared SFT metric schema, exact empty versus empty-or-unintelligible fields, missing-prediction count, total reference word count, insertion/deletion/substitution fields, JSON/Markdown/console rendering from one summary object, and golden report tests.
**Addresses:** Console-first report, report parity between batch eval and checkpoint scoring, dataset-ready metric schema, mocked verification path.
**Avoids:** Ambiguous empty-rate metrics, missing predictions disappearing from WER, incomplete promotion-looking console reports.

### Phase 2: Config And Model Target UX

**Rationale:** New operators need one mental model for base, tuned, and checkpoint resources before checkpoint discovery, parallel execution, or ranked scoreboards make sense.
**Delivers:** Validated `models` target config, safe unique labels/artifact labels, backend classification, backward compatibility from existing `base_model` and GCS `endpoint`, explicit masked/unmasked config examples, and validation errors that name invalid fields.
**Uses:** `gemini_sft.config`, TOML, GCS `config.json`, `common.gcs_utils`.
**Implements:** `ModelTarget`-style records persisted into durable run state.
**Avoids:** Complex eval-sibling abstraction, checkpoint-only CLI branches as the primary interface, config DSL overgrowth, target output collisions.

### Phase 3: Packaged Target Execution And Parity

**Rationale:** The repo already has batch eval and an online checkpoint scorer; the architecture should make execution strategy internal while keeping prompt and context behavior identical.
**Delivers:** Package-owned online target execution extracted from `model/scripts/sft/score_gemini_sft_checkpoints_online.py`, a target execution result contract, shared request construction, online resume/sync controls, and drift tests for prompt/prior-context parity across prepare, batch eval, checkpoint scoring, and maintained notebooks.
**Addresses:** Unified model target execution, dynamic prior-context parity, smoke-limit/resume controls, report parity.
**Avoids:** Prompt/request drift, static prior-context manifests, forcing checkpoint endpoints through batch, losing partial online progress.

### Phase 4: Durable Multi-Target Eval And Dataset Reports

**Rationale:** Once targets produce one result contract, `gemini_sft.evaluate` can run model comparisons and dataset breakdowns without reading local result mirrors.
**Delivers:** Eval target loop that builds histories once, executes one or more targets with explicit concurrency limits, uploads one normalized inference manifest per target, writes GCS-authoritative summaries, reports `bcfy_calls`, `bcfy_feeds`, `echo`, and `fire_notifications` breakdowns, and guards against stale output reuse.
**Addresses:** Multi-model eval execution, ranked comparison foundation, dataset breakdowns, GCS-authoritative artifact links, missing-prediction handling, masked/unmasked denominator clarity.
**Avoids:** Local `results/` as source of truth, stale base predictions, dataset metadata inconsistency, wide canonical prediction manifests.

### Phase 5: Operator Docs And Hygiene

**Rationale:** Documentation should describe the stable workflow after config, execution, and report contracts exist; otherwise docs will drift as semantics change.
**Delivers:** Updated `model/scripts/sft/README.md`, placeholder example configs, command examples for base/tuned/checkpoint/masked/unmasked runs, explanation of report fields and GCS artifacts, commit-safe artifact guidance, and a final `git status --short` artifact review.
**Addresses:** Documented operator path, committable configs, local/GCS artifact interpretation, workflow onboarding.
**Avoids:** Experiment artifacts leaking into git, local-only docs, real run TOMLs in source control, Linear automation scope creep.

### Phase Ordering Rationale

- Reporting comes first because metric names, denominators, and artifacts define what the operator is supposed to trust.
- Target config comes before broad execution work because checkpoint discovery, parallelism, and scoreboards should hang off the same simple target model.
- Prompt and prior-context parity must be proven while packaging online execution, before checkpoint/base deltas are treated as model comparisons.
- Durable multi-target evaluation and dataset reporting need the shared target result contract from earlier phases.
- Docs come last because they should document the final operator path, not temporary migration details.

### Research Flags

Phases likely needing deeper research during planning:
- **Phase 2:** Verify current Google GenAI/Vertex resource forms, locations, and batch support for tuned endpoints versus checkpoint endpoints before hard-coding backend defaults.
- **Phase 3:** Verify online `generate_content` request object requirements, quota/concurrency behavior, retry semantics, and endpoint location extraction against the current SDK before moving checkpoint execution.
- **Phase 4:** Validate stale-output protection design against existing GCS artifact layout and decide whether input hashes belong in batch input metadata, report JSON, or run state.

Phases with standard patterns where `$gsd-research-phase` is probably unnecessary:
- **Phase 1:** In-repo metric/report consolidation is well documented by existing `evaluate.py`, `records.py`, checkpoint scorer, and `common.scoring`.
- **Phase 5:** Docs, example configs, and git hygiene are repo-convention work with clear sources.

## Confidence Assessment

| Area | Confidence | Notes |
|------|------------|-------|
| Stack | HIGH repo-local, MEDIUM external API | Strong local evidence from `model/pyproject.toml`, locks, ASR Docker runtime, GCS conventions, and existing helpers. Google GenAI/Vertex details should be checked again before paid runs or SDK upgrades. |
| Features | HIGH | Requirements, anti-features, and MVP scope are explicit in `.planning/PROJECT.md`, feature research, and codebase conventions. |
| Architecture | HIGH repo-local, MEDIUM service behavior | Component boundaries are clear. Uncertainty remains around Vertex batch compatibility for tuned/checkpoint resources. |
| Pitfalls | HIGH repo-local, MEDIUM checkpoint batch limitation | Most risks are already visible in current code and artifacts; official docs do not fully settle every checkpoint endpoint edge case. |

**Overall confidence:** HIGH for roadmap direction; MEDIUM for exact Vertex backend assumptions until validated during implementation.

### Gaps to Address

- Vertex backend support: confirm which publisher, tuned, endpoint, and checkpoint resource forms work with batch versus online scoring before relying on defaults.
- Target schema details: finalize exact `models` TOML shape, backward compatibility behavior, label validation, and artifact path derivation.
- Dataset metadata extraction: centralize priority order across `dataset_name`, `dataset_family`, `source_group`, and configured dataset; report absent expected groups explicitly.
- Stale-output validation: define the input/config hash fields needed to safely reuse existing batch or base outputs.
- Online checkpoint defaults: choose conservative concurrency, retry, sync, and error-reporting defaults based on current observed quota behavior.
- Documentation timing: update docs after report/config contracts stabilize so onboarding instructions do not encode transitional behavior.

## Sources

### Primary (HIGH confidence)

- `.planning/research/STACK.md`: stack recommendation, dependency/version notes, runtime and cloud layout, anti-stack decisions.
- `.planning/research/FEATURES.md`: table stakes, differentiators, anti-features, MVP/v1.x/v2 prioritization.
- `.planning/research/ARCHITECTURE.md`: component boundaries, data flow, target execution architecture, build order.
- `.planning/research/PITFALLS.md`: critical risks, phase vocabulary, pitfall-to-phase mapping, recovery strategies.
- `.planning/PROJECT.md`: project scope, active requirements, out-of-scope decisions, constraints, and key decisions.
- `.planning/codebase/*`: stack, architecture, conventions, integrations, testing, and concerns maps.
- `model/scripts/sft/README.md`: current operator workflow and artifact contract.
- `model/src/gemini_sft/*`: existing config, prepare, tune, eval, records, and artifacts boundaries.
- `model/src/common/gemini/*`: prompt, prior-context, Vertex, and batch helpers.
- `model/src/common/scoring.py` and `model/src/common/inference_manifest.py`: metric primitives and normalized manifest contract.
- `model/scripts/sft/score_gemini_sft_checkpoints_online.py`: current online checkpoint scoring behavior and report fields.

### Secondary (MEDIUM confidence)

- Context7 `/googleapis/python-genai`: Vertex client initialization, tuning, batch creation, tuned endpoint `generate_content`, and SDK request surfaces.
- Official Google Gen AI Python SDK sources surfaced by Context7: `https://github.com/googleapis/python-genai/blob/main/README.md` and `https://github.com/googleapis/python-genai/blob/main/docs/_sources/index.rst.txt`.
- Official Google Cloud docs surfaced by Context7 for Gemini batch prediction, tuning checkpoints, and supervised tuning use. These confirm broad API concepts but not every checkpoint endpoint batch edge case.

---
*Research completed: 2026-06-28*
*Ready for roadmap: yes*
