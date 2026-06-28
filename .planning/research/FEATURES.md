# Feature Research

**Domain:** Gemini SFT/evaluation workflow onboarding for Watch Duty radio transcription
**Researched:** 2026-06-28
**Confidence:** HIGH

## Feature Landscape

This milestone should make the existing Gemini SFT CLI and checkpoint scorer
usable by a teammate who does not have prior experiment context. The core
product is not another model, notebook, or production pipeline path; it is an
operator workflow that starts from explicit TOML configs and ends in readable,
GCS-linked comparison reports.

The most important preference from `.planning/PROJECT.md` is simplicity:
prefer explicit config files and manifests over complex eval-sibling
abstractions, and treat checkpoints as model resources instead of adding
checkpoint-only CLI branches.

### Table Stakes (New Operators Need These)

Features a new operator must have to run and compare SFT/eval experiments
without reverse-engineering notebooks, local artifacts, or chat history.

| Feature | Why Expected | Complexity | Dependencies | Notes |
|---------|--------------|------------|--------------|-------|
| Documented operator path from config to report | A new teammate needs the exact commands, expected GCS artifacts, and interpretation path for `prepare`, `tune`, `eval`, and checkpoint scoring. | LOW | Existing `model/scripts/sft/README.md`; current CLI commands | Update the nearest owner doc, not a separate narrative. Include base-only, tuned, checkpoint, masked, and unmasked examples. |
| Committable example configs for common eval modes | Operators need safe placeholders they can copy without committing real run TOMLs. | LOW | Current TOML parser; README update | Include base-only eval, tuned endpoint eval, checkpoint/resource eval, masked eval, and unmasked eval examples. Keep real local configs out of git. |
| Unified `models`-style eval target config | The same mental model should configure a base model, tuned endpoint, or checkpoint endpoint/resource. | HIGH | TOML schema change; model target validation; batch/online routing | This is the key onboarding simplification. Avoid separate checkpoint-specific options as the primary interface. |
| Multi-model eval execution with parallel default | Comparing base, tuned, and checkpoint candidates should not require separate manual invocations and hand-merged reports. | HIGH | Unified model targets; async/concurrency controls; stable artifact labels | Keep one model per config/run easy, but when multiple models are supplied, run them concurrently by default with explicit limits. |
| Console-first comparison report | Operators asked to inspect results directly without digging through local `results/`. | MEDIUM | Shared scoring panel; report renderer; GCS artifact links | Include WER, CER, keyword accuracy, hallucination rate, exact empty response rate, insertion/deletion/substitution counts, total reference word count, deltas, and best model. |
| Report parity between batch eval and online checkpoint scoring | The repo currently has two scoring paths; metric names and columns must stay aligned. | MEDIUM | Shared scoring helper or common report schema; tests for both paths | The online script already reports exact empty response rate; `gemini-sft eval` needs the same explicit metric semantics. |
| Dataset breakdown report for `bcfy_calls`, `bcfy_feeds`, `echo`, and `fire_notifications` | A single aggregate WER hides source-specific regressions, and PROJECT.md explicitly requires dataset-level reporting. | MEDIUM | Dataset field extraction from source rows; normalized inference manifests; shared scorer | Use source-row metadata such as `dataset_name`, `dataset_family`, or `source_group`; do not depend on local `results/` as source of truth. |
| GCS-authoritative report and artifact links | Runs must be inspectable and resumable after local cleanup. | MEDIUM | Existing run prefix contract; `config.json`; normalized inference manifests | Console output should name GCS summary JSON/MD, prediction JSONL, raw Vertex outputs, and normalized inference manifest URIs. |
| Dynamic prior-context parity across SFT, batch eval, and checkpoint scoring | Prior context is computed from same-source ordering and usable previous transcripts; static manifest fields would change experiment semantics. | MEDIUM | `common.gemini.context`; shared request builders; drift tests | Preserve same-source grouping, offset/order sorting, exclusion of empty and `[UNINTELLIGIBLE]` history, and text-only prior modes where required. |
| Masked and unmasked eval as separate configs/manifests | PROJECT.md rejects a complex eval-sibling abstraction; operators still need both workflows. | LOW | Example configs; manifest naming; report labels | Make the distinction explicit in `round_id`, `inference_dataset_slug`, or config filenames. Do not hide both under one magical config. |
| Smoke-limit and resume controls for paid/slow eval | Vertex eval and online checkpoint scoring spend money and time; operators need a cheap confidence pass. | MEDIUM | CLI flags; GCS prediction resume; preflight/report logging | Checkpoint scorer already has `--limit` and resumable prediction uploads; align `gemini-sft eval` where feasible. |
| Mocked verification path for workflow changes | This is a cloud-costly workflow; CI must validate schema, routing, request construction, and reports without paid calls. | MEDIUM | Existing mocked GCS/Vertex tests; report fixtures | Add tests at config, routing, scoring, and report-rendering boundaries. Do not run notebooks or live Vertex jobs in unit tests. |

### Differentiators (Strong Improvements If Feasible)

These are not strictly required for first usability, but they would make this
workflow notably better for experiment review and promotion decisions.

| Feature | Value Proposition | Complexity | Dependencies | Notes |
|---------|-------------------|------------|--------------|-------|
| Unified ranked scoreboard across base, tuned endpoint, and checkpoints | Gives one sorted answer to "which model should we promote?" | MEDIUM | Unified report schema; multi-model eval | Sort by primary WER, show delta vs chosen baseline, include empty response and keyword tradeoffs so the lowest WER is not blindly promoted. |
| Re-score from existing normalized inference manifests | Allows cheap report iteration without rerunning Vertex inference. | HIGH | Per-model normalized manifests; scorer that accepts manifest URIs | Valuable for adding dataset breakdowns or report formats after expensive inference has already completed. |
| Tuning-job checkpoint discovery as a model source | Reduces manual endpoint copying when scoring all checkpoints from a tuning job. | MEDIUM | Vertex tuning-job fetch; checkpoint record normalization | The script already discovers checkpoints internally; expose it through the same model-target model instead of a separate operator workflow. |
| Promotion-gate summary | Turns raw metrics into a concise "candidate improved/regressed" decision aid. | MEDIUM | Scoreboard; dataset breakdowns; configurable thresholds | Should remain report-only. Avoid making deployment or production promotion automatic. |
| Config comparison/provenance panel | Makes it easy to compare prompt, context count/mode, adapter size, epochs, LR multiplier, and git/dependency versions across runs. | MEDIUM | Existing `config.json`; `records.py` metadata; report renderer | Helps reviewers understand why two runs differ without opening multiple JSON files. |
| Stable machine-readable report schema | Enables later PR/Linear comments, notebooks, or dashboards without changing the core workflow. | LOW | JSON summary alongside console/MD report | Keep automation consumers downstream; do not build comment automation in this milestone. |
| Slice reports beyond dataset | Helps identify failures by duration bucket, history depth, masked/unmasked mode, or empty-reference behavior. | MEDIUM | Existing duration buckets; source metadata; shared scorer | Add after dataset breakdowns. Avoid too many slices in the first report if it obscures the headline result. |
| Operator config lint command | Catches unsafe or stale config values before a paid run. | MEDIUM | TOML schema; GCS URI validation; model target validation | Could be `gemini-sft validate --config ...`; useful after `models` config grows. |

### Anti-Features (Deliberately Avoid)

Features that look helpful but contradict project preferences, add hidden state,
or increase operator confusion.

| Feature | Why Requested | Why Problematic | Alternative | Dependency/Conflict |
|---------|---------------|-----------------|-------------|---------------------|
| Complex eval-sibling abstraction for masked/unmasked runs | It could avoid duplicating config files. | The user explicitly prefers separate configs or manifests; sibling magic hides which corpus was evaluated. | Use separate masked and unmasked configs/manifests with explicit labels. | Conflicts with simple operator onboarding. |
| Checkpoint-only CLI branches as the primary path | Checkpoints need online scoring because Vertex batch does not accept tuned checkpoint endpoints. | Adds a second mental model and drifts from base/tuned eval reporting. | Treat checkpoints as model resources in the same model-target config and route internally to online scoring. | Conflicts with unified `models` config. |
| Local `results/` as authoritative state | Local files are easy to inspect. | They are workstation-specific and can disappear or drift from GCS. | Keep GCS run prefixes, normalized inference manifests, and uploaded reports authoritative; local files are cache/mirror only. | Conflicts with resumability and shared review. |
| Committing real run TOMLs, prediction JSONL, or raw result mirrors | It seems convenient for sharing exact experiments. | Leaks local experiment state into source control and violates repo conventions. | Commit placeholders/examples only; share GCS URIs in reports. | Conflicts with git hygiene. |
| Prompt files or `@file` prompt references | Long prompts are easier to edit in files. | Runs become dependent on one developer's workstation, and prompt text may not be captured in `config.json`. | Keep inline prompt overrides copied into GCS `config.json`. | Conflicts with reproducible resume/eval. |
| Notebook-first SFT/eval operation | Existing experiments used notebooks. | New operators should not need notebook history or hidden state to run paid workflows. | Keep notebooks as exploratory consumers of packaged helpers; docs and CLI remain primary. | Conflicts with packaged CLI onboarding. |
| Linear/PR comment automation in this milestone | Automated summaries are convenient for review. | PROJECT.md marks Linear comment automation out of scope; it is secondary to runnable workflow. | Emit stable JSON/MD reports that automation can consume later. | Depends on report schema, but should not block MVP. |
| Implicit paid Vertex work | One command could prepare, tune, and score everything. | Hidden paid operations are risky and harder to resume/debug. | Keep paid steps explicit, with clear confirmations, smoke limits, and output URIs. | Conflicts with cloud-cost boundaries. |
| Static prior-context fields embedded in manifests | Precomputing context can make rows self-contained. | It can drift from the shared dynamic semantics and silently change history ordering/filtering. | Build histories at run time through `common.gemini.context`. | Conflicts with prior-context parity. |
| One giant wide `pred_text_*` manifest as the canonical multi-model artifact | It makes spreadsheet-style comparison tempting. | Current normalized manifest contract expects per-model artifacts and rejects non-target prediction fields; wide manifests become hard to resume and validate. | Keep per-model normalized manifests plus a separate comparison summary/report. | Conflicts with normalized inference manifest boundaries. |
| Rebuilding the production transcription pipeline or model architecture | Better models are always tempting. | This milestone is about SFT/eval workflow onboarding, not runtime pipeline redesign. | Improve config, routing, reporting, docs, and tests around existing Gemini APIs. | Out of scope for this milestone. |

## Feature Dependencies

```text
Documented operator path
    requires -> Committable example configs
    requires -> GCS-authoritative artifact links

Unified models-style eval target config
    requires -> TOML schema and validation updates
    requires -> Internal routing: batch for supported base/tuned targets,
                online generate_content for checkpoint endpoints/resources
    enables  -> Multi-model eval execution
    enables  -> Unified ranked scoreboard
    enables  -> Tuning-job checkpoint discovery

Shared scoring/report schema
    requires -> Explicit empty response metric distinct from hallucination rate
    requires -> Total reference word count
    requires -> Error count and rate fields
    enables  -> Console-first comparison report
    enables  -> Batch/checkpoint report parity
    enables  -> Dataset breakdown report
    enables  -> Stable machine-readable report schema

Dataset breakdown report
    requires -> Source-row dataset extraction
    requires -> Per-model hypotheses aligned to eval rows
    enhances -> Promotion-gate summary

Dynamic prior-context parity
    requires -> common.gemini.context and shared request builders
    blocks   -> Static prior-context manifest fields

Separate masked/unmasked configs or manifests
    conflicts -> Complex eval-sibling abstraction
    enhances  -> Clear report labels and reproducible reruns

Smoke-limit and resume controls
    requires -> Explicit paid-operation boundaries
    enhances -> Online checkpoint scoring and multi-model eval
```

### Dependency Notes

- **Start with report schema before polishing docs.** The docs need to describe
  a stable operator result. If metric names change later, onboarding docs will
  drift immediately.
- **Unify model target configuration before broad checkpoint UX work.**
  Checkpoint discovery, parallel scoring, and ranked scoreboards should hang
  off the same target model rather than expanding the current separate script
  interface.
- **Dataset breakdowns depend on row-aligned predictions.** Do not compute
  dataset metrics from local summaries; compute them from eval rows plus
  per-model hypotheses or normalized inference manifests.
- **Masked/unmasked support should be naming and config discipline, not a new
  abstraction.** Separate configs/manifests are cheaper to understand and easier
  to rerun independently.
- **Prior-context semantics are a hard invariant.** Any feature that changes
  where histories are built must prove parity with SFT data generation, batch
  eval, and checkpoint scoring.

## MVP Definition

### Launch With (v1)

- [ ] Documented operator path covering base-only, tuned, checkpoint, masked,
      and unmasked eval commands.
- [ ] Example configs with placeholders and clear naming conventions.
- [ ] Unified model target config for a single model resource.
- [ ] Console-first report with WER, CER, keyword accuracy, hallucination rate,
      exact empty response rate, insertion/deletion/substitution counts, total
      reference word count, deltas, and GCS artifact URIs.
- [ ] Report parity between `gemini-sft eval` and checkpoint scoring.
- [ ] Dataset breakdowns for `bcfy_calls`, `bcfy_feeds`, `echo`, and
      `fire_notifications`.
- [ ] Tests proving config validation, report rendering, metric semantics, and
      prior-context/request parity without live Vertex calls.

### Add After Validation (v1.x)

- [ ] Multi-model parallel execution by default when multiple targets are
      supplied.
- [ ] Ranked scoreboard across base, tuned, and checkpoint candidates.
- [ ] Tuning-job checkpoint discovery exposed through model targets.
- [ ] Re-score/report generation from existing normalized inference manifests.
- [ ] Config lint command for dry-run validation before paid operations.

### Future Consideration (v2+)

- [ ] Promotion-gate thresholds once the team agrees on acceptable regression
      bounds by dataset and metric.
- [ ] Additional slice reports by duration, history depth, and prompt/context
      family after dataset reporting is stable.
- [ ] Downstream automation that posts the stable report JSON/MD into Linear or
      PR discussions.

## Feature Prioritization Matrix

| Feature | User Value | Implementation Cost | Priority |
|---------|------------|---------------------|----------|
| Documented operator path | HIGH | LOW | P1 |
| Committable example configs | HIGH | LOW | P1 |
| Unified single-model target config | HIGH | HIGH | P1 |
| Console-first report | HIGH | MEDIUM | P1 |
| Batch/checkpoint report parity | HIGH | MEDIUM | P1 |
| Dataset breakdown report | HIGH | MEDIUM | P1 |
| Prior-context parity guards | HIGH | MEDIUM | P1 |
| Masked/unmasked via separate configs/manifests | HIGH | LOW | P1 |
| Smoke-limit and resume controls | MEDIUM | MEDIUM | P1 |
| Multi-model parallel execution | HIGH | HIGH | P2 |
| Unified ranked scoreboard | HIGH | MEDIUM | P2 |
| Tuning-job checkpoint discovery | MEDIUM | MEDIUM | P2 |
| Re-score from normalized inference manifests | MEDIUM | HIGH | P2 |
| Config lint command | MEDIUM | MEDIUM | P2 |
| Promotion-gate summary | MEDIUM | MEDIUM | P3 |
| Additional slice reports | MEDIUM | MEDIUM | P3 |
| Linear/PR comment automation | LOW | MEDIUM | P3/out of scope |

**Priority key:**
- P1: Must have for onboarding-quality workflow
- P2: Should have once the core path is stable
- P3: Useful later, but should not shape the first implementation

## Sources

- `.planning/PROJECT.md` - project goal, active requirements, out-of-scope
  decisions, and user preference for simple configs over eval-sibling
  abstractions.
- `.planning/codebase/CONCERNS.md` - current checkpoint eval split, empty
  response terminology, prior-context semantics, cloud-cost boundaries, and
  dirty worktree caveats.
- `.planning/codebase/CONVENTIONS.md` - GCS-authoritative run state, config
  conventions, prompt/request parity, and git hygiene.
- `model/scripts/sft/README.md` - current packaged SFT CLI contract, run config
  shape, data split contract, artifact layout, eval semantics, and verification
  boundaries.
- `model/src/gemini_sft/evaluate.py` - current batch eval flow, metric
  computation, inference manifest upload, and base/tuned handling.
- `model/scripts/sft/score_gemini_sft_checkpoints_online.py` - current online
  checkpoint scoring flow, resumable predictions, exact empty response metric,
  and checkpoint report format.
- `model/src/gemini_sft/config.py`, `model/src/gemini_sft/records.py`,
  `model/src/common/gemini/context.py`, and
  `model/src/common/inference_manifest.py` - additional local verification of
  config parsing, report rendering, prior-context construction, and normalized
  inference manifest boundaries.

---
*Feature research for: Gemini SFT/evaluation workflow onboarding*
*Researched: 2026-06-28*
