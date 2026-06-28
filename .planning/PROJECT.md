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
- [x] The `gemini-sft` CLI already supports prepare, tune, and eval stages
  with GCS-authoritative run state under `gs://<bucket>/sft/runs/<round-id>/`.
- [x] The SFT workflow records prompt text in GCS `config.json`, derives
  train/validation Gemini JSONL from canonical manifests, and keeps eval
  manifests canonical until batch inference.
- [x] The checkpoint scorer can evaluate tuned checkpoint endpoints online
  and report WER, CER, keyword accuracy, hallucination rate, exact empty
  response rate, and insertion/deletion/substitution counts.
- [x] A codebase map exists under `.planning/codebase/` with stack,
  integrations, architecture, structure, conventions, testing, and concerns.

### Active

- [ ] Provide a simple single-target evaluation setup where a model, tuned
  endpoint, or checkpoint resource is configured through the same `models`
  style field instead of separate checkpoint-specific options.
- [ ] Make multiple provided models run in parallel by default, while keeping
  one model per config/run simple enough to invoke separately for base and
  tuned comparisons.
- [ ] Support masked and unmasked eval workflows through separate configs or
  manifests instead of a complex eval-sibling abstraction.
- [ ] Produce console-first reports that include WER, CER, keyword accuracy,
  hallucination rate, exact empty response rate, insertion/deletion/
  substitution counts, and total reference word count.
- [ ] Produce dataset-breakdown reports for bcfy_calls, bcfy_feeds, echo, and
  fire_notifications without requiring local `results/` artifacts as the
  source of truth.
- [ ] Keep prior-context construction dynamic and identical across SFT and
  evaluation: same-source rows, source-order sorting, usable previous
  transcripts only, and no audio in prior turns when the chosen mode requires
  text-only prior context.
- [ ] Preserve prompt parity across notebooks, SFT data generation, batch eval,
  and checkpoint scoring through shared `common.gemini` helpers.
- [ ] Document the operator path so a teammate can start from manifests and a
  config file and know which command to run, which GCS artifacts are produced,
  and how to interpret the report.

### Out of Scope

- Building a new transcription model architecture from scratch - current work
  improves Gemini SFT/eval operations around the existing model APIs.
- Replacing the production ingestion/transcription pipeline - the target is
  research and promotion workflow usability, not runtime pipeline redesign.
- Adding local `results/` as authoritative state - GCS run prefixes remain the
  durable source of truth.
- Linear comment automation - useful for reporting, but not required for an
  operator to run and compare experiments.
- Complex checkpoint-specific CLI branches - checkpoints should be treated as
  model resources wherever possible.

## Context

The existing repo is a brownfield monorepo with backend services, a frontend
UI, production audio processing, ASR research notebooks, and a packaged Gemini
SFT workflow. The recent experiment work centered on prior conversation
context for Gemini radio transcription: VAPO prompt sweeps, count-8 prior
context SFT, checkpoint scoring, masked/unmasked eval sets, empty response
analysis, keyword metrics, and dataset-level reporting.

The strongest operational pattern already in the codebase is config-driven
SFT with GCS-authoritative run records. The next work should extend that
pattern rather than adding more notebook-only or local-results-only scripts.

Current terminology and constraints are documented in:

- `.planning/codebase/`
- `CONTEXT.md`
- `model/scripts/sft/README.md`
- `model/src/common/gemini/`
- `model/src/gemini_sft/`

## Constraints

- **Runtime**: Keep SFT/eval runnable from the lightweight ASR operator
  runtime described in `ASR_CONTRIBUTING.md` - this avoids NeMo/GPU overhead
  for Gemini work.
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
|----------|-----------|---------|
| Use GCS run prefixes as authoritative SFT state | Jobs and evals must survive local process exits and workstation cleanup | Pending |
| Treat checkpoints as model resources in eval config | Avoids separate checkpoint-only CLI branches and matches the user's requested mental model | Pending |
| Prefer separate configs/manifests for masked vs unmasked eval | Simpler than a special eval-sibling abstraction and easy to run independently | Pending |
| Keep console reports first-class | Operators asked to inspect results directly without digging through local artifacts | Pending |
| Keep Linear comment automation out of scope | Reporting automation is secondary to making the workflow runnable by a new teammate | Pending |

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
*Last updated: 2026-06-28 after initialization*
