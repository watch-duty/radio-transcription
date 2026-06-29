# Gemini SFT Workflow Onboarding

## What This Is

This project makes the radio-transcription Gemini SFT and evaluation workflow
usable by someone who has not followed the prior experiment thread. It turns
the existing notebooks, scripts, SFT CLI, checkpoint scorer, manifests, and
GCS artifact conventions into a config-driven workflow with clear reports and
minimal hidden context.

The audience is Watch Duty engineers and researchers running prior-context
Gemini transcription experiments, comparing checkpoints, and deciding which
fine-tuned model or prompt setup should be promoted.

## Current State

v1.0 shipped on 2026-06-29. The stable operator path now includes:

- Shared Gemini SFT eval reporting with canonical WER, CER, keyword accuracy,
  empty-rate, S/I/D, total-reference-word, missing-prediction, and artifact
  columns.
- A single `[eval.model]` target contract for publisher models, tuned
  endpoints, and checkpoint endpoints.
- Dynamic prior-context history construction from canonical same-source rows.
- Batch and online backend routing with reusable request, prompt, generation,
  safety, and prior-context helpers.
- Resumable online checkpoint execution and fail-closed batch/online reuse
  metadata.
- Durable GCS summary artifacts and normalized inference manifests.
- OKF operator docs, placeholder configs, metric glossary, artifact reference,
  hygiene checklist, and drift guards.

## Core Value

A new operator can run and compare Gemini SFT/eval experiments from explicit
configs and console reports without reverse-engineering notebooks or prior
chat history.

## Requirements

### Validated

- [x] The repo has a production radio transcription pipeline for ingestion,
  segmentation, normalization, transcription, rules evaluation, notification,
  storage APIs, and frontend operations.
- [x] The model subtree has packaged shared helpers for canonical manifests,
  GCS, scoring, Gemini prompts, Gemini prior-context construction, Vertex
  requests, Vertex batch inference, and Gemini audio-SFT JSONL.
- [x] The `gemini-sft` CLI supports prepare, tune, and eval stages with
  GCS-authoritative run state under `gs://<bucket>/sft/runs/<round-id>/`.
- [x] The SFT workflow records prompt text in GCS `config.json`, derives
  train/validation Gemini JSONL from canonical manifests, and keeps eval
  manifests canonical until inference.
- [x] The checkpoint scorer can evaluate tuned checkpoint endpoints online and
  report WER, CER, keyword accuracy, empty rates, and insertion/deletion/
  substitution counts.
- [x] v1.0 provides the shared SFT eval report contract across console, JSON,
  Markdown, batch eval, and checkpoint scoring.
- [x] v1.0 supports one explicit `[eval.model]` target per packaged eval run,
  with the model string representing a publisher model, tuned endpoint, or
  checkpoint endpoint.
- [x] v1.0 rejects legacy target fallback and plural eval target config shapes
  before paid Vertex work starts.
- [x] v1.0 supports masked and unmasked eval workflows as separate
  configs/manifests.
- [x] v1.0 preserves dynamic prior-context and prompt/request parity through
  shared `common.gemini` helpers.
- [x] v1.0 writes durable GCS eval summaries and normalized target inference
  manifests while keeping local `results/` as a cache.
- [x] v1.0 includes an OKF runbook, placeholder configs, metric docs, artifact
  docs, hygiene docs, and drift guards for local artifact handling.

### Active

- [ ] Add dataset breakdown reports for bcfy_calls, bcfy_feeds, echo, and
  fire_notifications.
- [ ] Add promotion thresholds and pass/fail promotion verdicts by dataset and
  metric.
- [ ] Add optional report slices for duration buckets, prior-context depth,
  and prompt/context families.
- [ ] Add optional automation for Linear, PR comments, or release notes once
  report JSON remains stable across more runs.

### Out of Scope

- Building a new transcription model architecture from scratch - current work
  improves Gemini SFT/eval operations around the existing model APIs.
- Replacing the production ingestion/transcription pipeline - the target is
  research and promotion workflow usability, not runtime pipeline redesign.
- Adding local `results/` as authoritative state - GCS run prefixes remain the
  durable source of truth.
- Internal multi-target eval fan-out - v1.0 intentionally uses one model per
  config/run; callers can run separate configs externally.
- Dataset breakdown reports in v1.0 - the user explicitly deferred this to a
  follow-up milestone.
- Linear comment automation in v1.0 - useful later, but not required for an
  operator to run and compare experiments.
- Complex checkpoint-specific CLI branches - checkpoints should be treated as
  model resources wherever possible.

## Context

The existing repo is a brownfield monorepo with backend services, a frontend
UI, production audio processing, ASR research notebooks, and a packaged Gemini
SFT workflow. Recent experiment work centered on prior conversation context for
Gemini radio transcription: VAPO prompt sweeps, count-8 prior context SFT,
checkpoint scoring, masked/unmasked eval sets, empty response analysis,
keyword metrics, and dataset-level reporting.

The strongest operational pattern is config-driven SFT with GCS-authoritative
run records. Future work should extend that pattern rather than adding
notebook-only or local-results-only scripts.

Current terminology and constraints are documented in:

- `.planning/codebase/`
- `CONTEXT.md`
- `model/scripts/sft/README.md`
- `model/scripts/sft/docs/`
- `model/src/common/gemini/`
- `model/src/gemini_sft/`

## Constraints

- **Runtime**: Keep SFT/eval runnable from the lightweight ASR operator runtime
  described in `ASR_CONTRIBUTING.md` - this avoids NeMo/GPU overhead for
  Gemini work.
- **Durability**: GCS run prefixes and normalized inference manifests must be
  sufficient to resume, inspect, and compare runs - local `results/` is only a
  mirror/cache.
- **Prompt parity**: Prompt and request construction must come from shared
  helpers so notebooks, SFT JSONL, batch eval, and checkpoint scoring do not
  drift.
- **Prior context**: Prior transcripts are computed at run time from same
  original audio/session ordering; they are not static manifest fields.
- **Metrics**: Reports must distinguish exact empty responses from the
  historical empty/unintelligible rate.
- **Cost**: Vertex tuning and inference can spend money; paid operations need
  explicit commands, clear output locations, and resumable state.
- **Git hygiene**: Do not commit local experiment TOMLs, raw prediction JSONL,
  or `results/` outputs unless explicitly requested.

## Key Decisions

| Decision | Rationale | Outcome |
|---|---|---|
| Use GCS run prefixes as authoritative SFT state | Jobs and evals must survive local process exits and workstation cleanup | Validated through durable eval records and operator docs |
| Treat checkpoints as model resources in eval config | Avoids separate checkpoint-only CLI branches and matches the user's requested mental model | Validated as a documented `[eval.model]` resource shape |
| Support exactly one model target per packaged eval run | Keeps the CLI simple; callers can compare models by running separate configs externally | Validated in v1.0 durable eval |
| Reject legacy and plural target config shapes | Avoids migration ambiguity and stale endpoint fallback | Validated by config and workflow tests |
| Prefer separate configs/manifests for masked vs unmasked eval | Simpler than a special eval-sibling abstraction and easy to run independently | Validated through config docs and runbook guidance |
| Keep console reports first-class | Operators asked to inspect results directly without digging through local artifacts | Validated through shared report contract and docs |
| Defer dataset breakdowns | The user explicitly chose to skip this in v1.0 and revisit later | Carried as an Active follow-up |
| Keep Linear comment automation out of scope | Reporting automation is secondary to making the workflow runnable by a new teammate | Preserved through v1.0 |

## Next Milestone Goals

The likely next milestone should choose one narrow follow-up:

- Dataset breakdown reporting.
- Promotion gates and regression thresholds.
- Report slice expansion by duration, context depth, or prompt family.
- Downstream automation from stable report JSON.

## Evolution

This document evolves at phase transitions and milestone boundaries.

---
*Last updated: 2026-06-29 after v1.0 milestone archive*
