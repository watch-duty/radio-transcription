# Phase 5: Operator Docs - Context

**Gathered:** 2026-06-28T22:30:00Z
**Status:** Ready for planning

<domain>
## Phase Boundary

Phase 5 turns the stable Gemini SFT workflow into human-facing operator
documentation. A new teammate should be able to start from canonical manifests
and placeholder configs, run prepare, tune, eval, checkpoint scoring, masked or
unmasked eval, read the console/GCS reports, and avoid committing local
experiment artifacts.

The phase is documentation and lightweight hygiene only. It should not add new
eval semantics, multi-model orchestration, dataset-breakdown features, prompt
file support, Linear automation, or paid Vertex validation.

</domain>

<decisions>
## Implementation Decisions

### OKF Operator Runbook
- **D-01:** Human-facing SFT docs should use Open Knowledge Format-compatible
  Markdown.
- **D-02:** The operator runbook itself is the canonical OKF document. It is
  not merely linked from an OKF index.
- **D-03:** `model/scripts/sft/README.md` should become a thin entrypoint that
  points to the OKF docs and avoids duplicating the runbook.
- **D-04:** The preferred doc shape is a small bundle under
  `model/scripts/sft/docs/`, with `runbook.md` as the primary workflow doc and
  focused companion docs for config examples, metric definitions, artifact
  locations, and hygiene.
- **D-05:** OKF frontmatter should be lightweight YAML metadata, for example
  `type`, `title`, `description`, and `tags`. Do not introduce a new required
  documentation build tool unless planning finds one already used locally.

### Runbook Journey
- **D-06:** The runbook should optimize for a first-time operator running the
  standard path, not for exhaustively documenting historical experiment
  variants.
- **D-07:** The main flow should be command-oriented:
  prepare config, run `gemini-sft prepare`, run `gemini-sft tune --confirm`,
  run `gemini-sft eval`, inspect console and GCS reports, optionally run the
  checkpoint scorer, then run artifact hygiene checks before committing.
- **D-08:** The runbook must make paid Vertex operations obvious. Commands that
  submit tuning or inference should state the expected GCS output prefix before
  the operator runs them.
- **D-09:** The docs should follow the current Phase 4 eval contract: one
  `[eval.model]` per config/run. To compare base, tuned, and checkpoint models,
  the operator uses separate configs or an external wrapper.

### Example Config Set
- **D-10:** Keep committed config examples small. One normal SFT/eval config
  plus at most one masked-eval variant is enough.
- **D-11:** Do not create a broad gallery of separate config files for every
  base, tuned, checkpoint, masked, unmasked, batch, and online combination.
- **D-12:** The existing `model/scripts/sft/run_config.example.toml` should
  remain the canonical full placeholder config. A second placeholder file or
  OKF config snippet may show the masked-eval variant by changing only
  `round_id`, `eval_manifest_uri`, and `inference_dataset_slug`.
- **D-13:** Base, tuned endpoint, and checkpoint endpoint targets should be
  documented as the same `[eval.model]` shape. Use short snippets for alternate
  `label` and `model` values rather than full duplicate config files.
- **D-14:** Examples must contain placeholders only. Do not include real local
  credentials, live run IDs, local `.local.toml` values, or generated result
  artifact paths as committed examples.

### Metric Glossary
- **D-15:** The docs must define every canonical report metric shown in the
  current console/JSON/Markdown reports: WER, CER, keyword accuracy,
  empty-or-unintelligible rate, exact empty response rate, insertion count,
  deletion count, substitution count, total reference word count, missing
  prediction count, row count when present, and artifact URIs.
- **D-16:** Use `empty_or_unintelligible_rate` for the historical metric that
  treats empty strings and exact `[UNINTELLIGIBLE]` hypotheses as empty-like.
- **D-17:** Use `empty_response_rate` only for exact empty model output after
  stripping whitespace.
- **D-18:** Avoid presenting legacy names such as `empty_rate`,
  `hallucination_rate`, `hits`, or `correct_words` as primary operator-facing
  columns in new docs. If historical output is mentioned, explain it as legacy
  terminology only.
- **D-19:** Explain that missing provider predictions are scored as empty
  hypotheses and stay in the WER/CER denominator, while
  `missing_prediction_count` remains operationally separate from exact empty
  model responses.

### Artifact Hygiene
- **D-20:** The runbook must distinguish durable GCS state from local cache or
  mirror files. GCS `config.json`, `status.json`, canonical manifests, Gemini
  model inputs, prediction outputs, normalized inference manifests, and
  `evals/wer_summary.{json,md}` are the durable inspection points.
- **D-21:** Local `results/<round-id>/` is a cache/mirror only. It must not be
  documented as the source of truth for successful eval reuse or report links.
- **D-22:** The runbook should end with an explicit artifact hygiene check that
  catches accidental commits of `.local.toml`, local `results/`, downloaded or
  generated inference manifests, raw prediction JSONL, and other local
  experiment outputs.
- **D-23:** Planning may add a small test or script for the hygiene check if it
  fits existing repo patterns, but Phase 5 should not create a heavy new CI
  policy or block unrelated workflows.
- **D-24:** `.gitignore` should be reviewed during planning. If existing ignore
  rules do not cover the files the runbook says never to commit, either update
  the ignore rules or make the hygiene check explicit enough to catch the gap.

### the agent's Discretion
The agent may choose exact OKF frontmatter fields, doc filenames, section
ordering, and whether the masked example is a second TOML file or a compact
snippet in the OKF config reference. Keep the result easy for a new operator to
follow and avoid duplicating long instructions across README and the OKF
runbook.

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Planning
- `.planning/PROJECT.md` - Core value, active docs requirement, artifact
  authority, prompt parity, and git hygiene constraints.
- `.planning/REQUIREMENTS.md` - DOC-01 through DOC-05 plus current out-of-scope
  boundaries.
- `.planning/ROADMAP.md` - Phase 5 goal and success criteria. Treat any stale
  multi-target or dataset-breakdown language as superseded by Phase 4 context.
- `.planning/phases/01-reporting-contract/01-CONTEXT.md` - Canonical report
  metric names, empty metric semantics, missing-prediction behavior, and shared
  report expectations.
- `.planning/phases/02-target-config/02-CONTEXT.md` - Historical target-config
  decisions. Read for label and masked/unmasked reasoning, but note that Phase
  4 supersedes plural `[[eval.models]]`.
- `.planning/phases/03-target-execution/03-CONTEXT.md` - Backend routing,
  execution knobs, smoke limit semantics, prompt/request parity, and
  prior-context behavior.
- `.planning/phases/04-durable-eval/04-CONTEXT.md` - Current durable eval
  contract: singular `[eval.model]`, GCS-authoritative summaries, reuse
  identity, local results as cache, and dataset-breakdown deferral.

### Codebase Maps
- `.planning/codebase/CONVENTIONS.md` - SFT source-of-truth rules, prompt
  helpers, safety settings, empty-output semantics, and git hygiene.
- `.planning/codebase/STRUCTURE.md` - Documentation locations and generated
  artifact directories to avoid committing.
- `.planning/codebase/TESTING.md` - Model test boundaries and no-paid-Vertex
  verification constraints.

### Current Operator Surface
- `model/scripts/sft/README.md` - Existing SFT command, config, record,
  artifact, eval, and prompt-parity documentation to reorganize.
- `model/scripts/sft/run_config.example.toml` - Existing placeholder config and
  current singular `[eval.model]` shape.
- `.gitignore` - Current ignore coverage for local configs, local results, and
  generated JSONL artifacts.

### Report And Config Code
- `model/src/gemini_sft/reporting.py` - Canonical target report fields and
  renderers that metric docs must match.
- `model/src/gemini_sft/config.py` - Current TOML/GCS config fields,
  `[eval.model]`, `[eval.execution]`, prompt override validation, and durable
  config serialization.
- `model/src/gemini_sft/evaluate.py` - Current eval flow, GCS artifact paths,
  normalized inference manifest behavior, summary uploads, and reuse semantics.
- `model/scripts/sft/score_gemini_sft_checkpoints_online.py` - Checkpoint sweep
  behavior to document as a specialized ranking path, not the main packaged
  eval path.
- `model/tests/gemini_sft/test_reporting.py` - Tests that assert public metric
  fields and reject legacy primary column names.
- `model/tests/common/tests/test_drift_guard.py` - Prompt/request/context drift
  guards that docs should not contradict.

### External Format Reference
- `https://github.com/GoogleCloudPlatform/knowledge-catalog/blob/main/okf/SPEC.md`
  - Open Knowledge Format reference for lightweight Markdown/YAML-frontmatter
  organization.
- `https://cloud.google.com/blog/products/data-analytics/how-the-open-knowledge-format-can-improve-data-sharing`
  - Background on OKF as a human- and machine-friendly documentation format.

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- `model/scripts/sft/README.md` already documents the raw command sequence,
  config shape, GCS record layout, eval semantics, prompt parity, and
  verification boundary. Phase 5 should reorganize it rather than invent a
  second semantic source of truth.
- `model/scripts/sft/run_config.example.toml` already uses the current singular
  `[eval.model]` and placeholder values.
- `gemini_sft.reporting` exposes the current report schema; docs should be
  generated or manually checked against that schema.
- Existing tests under `model/tests/gemini_sft` already assert metric names,
  config validation, artifact fields, and drift guards.

### Established Patterns
- `model/scripts/sft/README.md` is the current operator-facing entrypoint for
  Gemini SFT workflow semantics.
- GCS `config.json` under `gs://<bucket>/sft/runs/<round-id>/` is durable run
  state. Local TOML files are operator inputs, and local `results/` output is a
  mirror/cache.
- Prompt overrides are inline-only because resolved prompt text must be copied
  into durable GCS state.
- Model package tests mock GCS and Vertex; docs verification should not submit
  paid tuning jobs or inference jobs.
- Local generated artifacts are common in this repo, so committed docs should
  be explicit about what not to stage.

### Integration Points
- Replace or shorten the existing README content so it points to the OKF
  runbook and companion docs without duplicating long sections.
- Add `model/scripts/sft/docs/` with an OKF index, canonical runbook, config
  reference/examples, metric glossary, and artifact/hygiene reference.
- Update placeholder TOML examples only enough to show the current contract and
  one masked/unmasked variant. Do not add real run configs.
- Add focused documentation checks if planning finds an existing low-friction
  test pattern, such as asserting README links and example config parsing.

</code_context>

<specifics>
## Specific Ideas

Preferred documentation layout:

```text
model/scripts/sft/
  README.md
  run_config.example.toml
  docs/
    index.md
    runbook.md
    configs.md
    metrics.md
    artifacts.md
    hygiene.md
```

Preferred OKF-style frontmatter for the runbook:

```yaml
---
type: runbook
title: Gemini SFT Operator Runbook
description: End-to-end prepare, tune, eval, and report workflow.
tags: [gemini-sft, operator-docs]
---
```

The standard runbook path should use the existing CLI commands:

```bash
gemini-sft prepare --config /path/to/run.toml
gemini-sft tune --config /path/to/run.toml --confirm
gemini-sft eval --config /path/to/run.toml
```

The main config example should use:

```toml
[eval.model]
label = "base"
model = "gemini-3.1-flash-lite"
```

Endpoint or checkpoint examples should be snippets with the same shape:

```toml
[eval.model]
label = "checkpoint_6"
model = "projects/PROJECT/locations/us-central1/endpoints/ENDPOINT_ID"
```

Masked eval should be documented as a separate config/run by changing only the
run identity and eval manifest placement fields, for example:

```toml
round_id = "YYYY-MM-DD-short-description-masked"
inference_dataset_slug = "echo/masked_v2/eval"
eval_manifest_uri = "gs://your-bucket/path/manifests/echo/masked_v2/eval.jsonl"
```

</specifics>

<deferred>
## Deferred Ideas

- A large config gallery for every target/backend/masked combination is
  deferred. Use one full placeholder config and short snippets.
- Dataset-breakdown operator documentation is deferred until the feature is
  implemented. Do not document it as available if current eval produces one
  overall target row only.
- Internal multi-model orchestration remains out of scope. Operators can run
  separate configs in parallel with their own wrapper.
- Linear comments, release-note automation, and promotion gates remain out of
  scope for Phase 5.

</deferred>

---

*Phase: 5-Operator Docs*
*Context gathered: 2026-06-28T22:30:00Z*
