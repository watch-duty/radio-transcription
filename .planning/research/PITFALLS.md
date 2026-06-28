# Pitfalls Research

**Domain:** Gemini SFT/evaluation workflow usability for Watch Duty radio transcription
**Researched:** 2026-06-28
**Confidence:** HIGH for repo-specific risks; MEDIUM for Vertex checkpoint batch limitations because official docs describe batch and checkpoint resources but do not fully document every checkpoint endpoint edge case.

## Suggested Phase Vocabulary

These are roadmap-facing phase names used below:

1. **Reporting Contract** - stabilize metric names, console output, JSON/Markdown report shape, and dataset breakdowns.
2. **Config And Model Target UX** - introduce the simple `models`-style config without complex eval-sibling or checkpoint-only branches.
3. **Prompt And Prior-Context Parity** - keep SFT generation, batch eval, checkpoint scoring, and notebooks on shared prompt/request/context helpers.
4. **Durable Execution State** - preserve GCS-authoritative run state, batch/online output provenance, resumability, and stale-output protection.
5. **Operator Docs And Hygiene** - document the runnable path and keep local experiment artifacts out of commits.

## Critical Pitfalls

### Pitfall 1: Ambiguous Empty-Rate Metrics

**What goes wrong:**
Reports label multiple concepts as "empty rate" and operators compare the wrong number across base eval, tuned eval, and checkpoint scoring. In this repo there are at least two distinct rates:

- **Exact empty response rate:** model returned no text after stripping.
- **Historical empty/unintelligible rate:** model returned no text or exactly `[UNINTELLIGIBLE]`.

The checkpoint scorer already computes both, but also keeps `empty_rate` as an alias for the historical rate. `gemini_sft.evaluate` currently writes `base_empty_rate` / `tuned_empty_rate` using `hallucination_rate`, which is the historical empty-or-`[UNINTELLIGIBLE]` metric.

**Why it happens:**
Production transcription treats empty engine output differently than raw eval. Older reports used "empty" for the broader empty/unintelligible concept, while recent operator needs require exact empty response tracking for Gemini checkpoint regressions.

**How to avoid:**
Make the reporting contract explicit before adding more console output:

- Display `exact_empty_response_rate` and `empty_or_unintelligible_rate` as separate columns.
- Keep legacy JSON aliases only for backward compatibility, and document them as deprecated aliases.
- Add a golden test with hypotheses `["ok", "", "[UNINTELLIGIBLE]"]` that expects exact empty = `33.33` and empty-or-unintelligible = `66.67`.
- Include raw counts as well as percentages when possible, so small eval slices do not hide denominator changes.

**Warning signs:**

- A report has a column named only `empty_rate`.
- A chart compares `base_empty_rate` against `empty_response_rate`.
- Exact empty responses improve while WER gets worse, with no separate missing-prediction count.
- Tests assert only one empty metric.

**Phase to address:**
Phase 1: Reporting Contract.

---

### Pitfall 2: Missing Batch Predictions Disappear From The Denominator

**What goes wrong:**
An eval layer drops rows with missing Vertex output, accidentally lowering WER by removing hard failures. Current `gemini_sft.evaluate` intentionally fills missing predictions with `""`, making them count as full deletions. Normalized inference manifests also distinguish absent prediction fields for missing records from explicit empty model outputs.

**Why it happens:**
It is tempting to inner-join predictions to references by `audio_filepath` and score only rows that have predictions. Batch jobs can produce status/error rows, malformed rows, or no JSONL under the expected output prefix. Online checkpoint scoring can also exhaust retries and return an empty prediction with an error.

**How to avoid:**
Treat eval rows as the denominator and predictions as optional data:

- Build hypotheses by iterating the eval rows, not by iterating predictions.
- Score missing predictions as `""`.
- Report `missing_prediction_count` separately from exact empty response count.
- Preserve `prediction_gcs_uri`, raw Vertex batch output URI, and normalized inference-manifest URI in reports.
- Keep the duplicate-audio-URI rejection because one provider output cannot safely map to multiple manifest rows.

**Warning signs:**

- `n_eval_examples` differs from the number of scored hypotheses.
- WER improves after a batch job with warnings about missing output.
- A report has exact empty count but no missing prediction count.
- Code uses `for audio_uri, pred in preds.items()` to build score inputs.

**Phase to address:**
Phase 1: Reporting Contract, verified again in Phase 4: Durable Execution State.

---

### Pitfall 3: Local `results/` Becomes The Source Of Truth

**What goes wrong:**
Operators resume or compare runs from stale local `results/<round-id>/` artifacts instead of the GCS run prefix. This can mix old config, old batch output, and new console reports under the same visible round.

**Why it happens:**
The workflow mirrors useful summaries locally, but durable state is in GCS: `run_config.toml`, `config.json`, canonical manifests, model inputs, tuning status, eval inputs/outputs, checkpoint predictions, and normalized inference manifests. The worktree also currently has untracked local experiment TOMLs, inference manifests, summaries, and `results/`, making accidental reliance easy.

**How to avoid:**

- All eval/scoring commands should read authoritative run state from GCS `config.json`.
- Console reports should print the GCS run prefix and output URIs, not only local paths.
- Any local summary should say it is a mirror/cache.
- Reusing a `round_id` should be rejected or require an explicit resume path that validates the existing GCS prefix.
- Add a stale-output guard: if an eval input JSONL is regenerated for a label whose output prefix already exists, compare a stored input hash before reusing output.

**Warning signs:**

- A command succeeds without fetching `config.json` from GCS.
- A report references only `results/<round-id>/...`.
- Re-running with the same `round_id` silently reuses batch output after config or manifest changes.
- The local TOML is treated as the endpoint/job source after tune has already written GCS state.

**Phase to address:**
Phase 4: Durable Execution State.

---

### Pitfall 4: Prompt Or Request Construction Drifts Across Paths

**What goes wrong:**
Base eval, tuned eval, checkpoint scoring, notebooks, and SFT JSONL construction send subtly different requests. WER deltas then reflect prompt/request drift instead of model quality.

**Why it happens:**
The repo has two evaluation paths: config-driven batch eval in `gemini_sft.evaluate` and online checkpoint scoring in `model/scripts/sft/score_gemini_sft_checkpoints_online.py`. The correct pattern is to route both through `common.gemini.prompts`, `common.gemini.vertex.build_request`, shared generation config, and shared safety settings. Adding a usability layer may duplicate request construction while trying to simplify CLI UX.

**How to avoid:**

- Do not assemble Gemini request JSON in the new workflow layer.
- Keep prompt text resolved once into GCS `config.json`.
- Extend drift-guard tests so the checkpoint scorer, batch eval, and any maintained notebook import the canonical prompt/request helpers.
- Add a report field for prompt mode, `prior_context_count`, `prior_context_mode`, generation config, and safety config version.

**Warning signs:**

- New code contains hard-coded system or user prompt strings.
- Checkpoint scoring has a different temperature, max output tokens, or safety settings from batch eval.
- A notebook or script imports prompt constants from a local cell/script instead of `common.gemini`.
- A report cannot say which prompt text or helper version produced it.

**Phase to address:**
Phase 3: Prompt And Prior-Context Parity.

---

### Pitfall 5: Prior Context Is Treated As Static Manifest Data

**What goes wrong:**
SFT and eval use different prior turns for the same row, or masked/unmasked eval changes row ordering and therefore context. This makes checkpoint comparisons invalid even when the headline config says the same `prior_context_count`.

**Why it happens:**
Prior context is dynamic in this repo. It is built at run time by grouping same-source rows, sorting by original offset/order, keeping only usable previous transcripts, and excluding empty or `[UNINTELLIGIBLE]` history rows. It is not a field to copy from one manifest row to another. The allowed SFT context modes are text-only modes because Vertex SFT examples can contain only one audio part.

**How to avoid:**

- Keep `build_context_histories` as the only history builder for SFT, batch eval, and checkpoint scoring.
- Keep SFT config validation limited to `text_turns`, `transcript`, and `vapo_p3_transcript`; do not allow eval-only audio-history mode in SFT configs.
- Add fixture tests that feed the same canonical rows through prepare, eval, and checkpoint scoring and assert identical prior text for selected rows.
- Console reports should show `prior_context_count`, `prior_context_mode`, and number of rows with non-empty history.

**Warning signs:**

- A config or manifest contains serialized prior transcripts as source data.
- The workflow sorts by current `audio_filepath` rather than original offset/source ordering.
- Empty or `[UNINTELLIGIBLE]` reference rows become prior context.
- Masked and unmasked configs share a run prefix or reuse context artifacts.

**Phase to address:**
Phase 3: Prompt And Prior-Context Parity.

---

### Pitfall 6: Vertex Batch And Checkpoint Endpoints Are Forced Into One Backend

**What goes wrong:**
A unified model-list feature tries to run every model target through Vertex batch inference. Base publisher models and tuned model resources may work through batch, but intermediate checkpoint endpoints are currently scored online in this repo because batch support is not reliable for those resources.

**Why it happens:**
The desired operator mental model is "score these models," but Vertex exposes different resource shapes: publisher model IDs, tuned model/model endpoints, tuning jobs, and checkpoints. Current official docs describe Gemini batch prediction as a JSONL GCS batch job with model and region constraints, and tuning docs expose tuned model endpoints plus checkpoint metadata. They do not make checkpoint endpoint batch compatibility a safe portability assumption.

**How to avoid:**

- Implement a small model-target resolver, not a generic orchestration DSL.
- Classify targets into explicit backends: batch for supported publisher/tuned resources, online `generate_content` for checkpoint endpoints unless a phase proves batch support live.
- Keep smoke limits (`--limit`) and resumable online predictions for checkpoint scoring.
- Record endpoint location from full resource names; do not assume `[gcp].location` is correct for every endpoint.
- Keep retry, concurrency, and sync controls visible for online scoring.

**Warning signs:**

- Code passes a checkpoint endpoint blindly into `submit_batch_inference`.
- Batch failures are hidden by falling back to partial reports.
- New target config has many checkpoint-specific branches instead of a normalized target object with a backend.
- Location errors appear only after a paid job is submitted.

**Phase to address:**
Phase 2: Config And Model Target UX, with execution validation in Phase 4: Durable Execution State.

---

### Pitfall 7: Masked And Unmasked Eval Runs Are Coupled Too Tightly

**What goes wrong:**
Masked and unmasked eval results are compared as if they share the same dataset contract, or a single "eval sibling" abstraction hides which manifest was scored. Dataset breakdowns then become misleading, especially when reporting bcfy_calls, bcfy_feeds, echo, and fire_notifications.

**Why it happens:**
Masked/unmasked eval is operationally simple when represented as separate configs or manifests. It becomes hard to audit when hidden behind automatic sibling discovery, implicit manifest swaps, or shared run prefixes.

**How to avoid:**

- Prefer separate configs/manifests with explicit `round_id` and `inference_dataset_slug`.
- Print `canonical_eval_uri`, `inference_dataset_slug`, `n_eval_examples`, and total reference word count in every report.
- Dataset breakdowns should be derived from source rows in the scored manifest, not from local filenames or report labels.
- Compare masked vs unmasked only when the report shows denominator and source composition side by side.

**Warning signs:**

- A report says "masked" but does not include the eval manifest URI.
- Masked and unmasked output land under one run prefix.
- Dataset breakdown counts do not add up to `n_eval_examples`.
- A phase proposes automatic eval-sibling discovery before simple separate configs work.

**Phase to address:**
Phase 1: Reporting Contract and Phase 2: Config And Model Target UX.

---

### Pitfall 8: The Config Layer Becomes A Workflow Language

**What goes wrong:**
The usability layer introduces a large config abstraction for experiments: eval siblings, inherited defaults, checkpoint-specific option trees, local prompt files, output routing rules, and implicit run derivation. Operators still need hidden context to predict what will run.

**Why it happens:**
The project is trying to unify model, tuned endpoint, checkpoint, masked/unmasked, prior context, and reporting concerns. Without restraint, the config becomes a second workflow engine instead of a clear run declaration.

**How to avoid:**

- Keep one run config explicit: one eval manifest, one run prefix, one prompt pair, one prior-context mode/count.
- Let `models` be a list of model resources with optional stable labels; keep backend resolution in code.
- Use separate configs for masked/unmasked rather than a sibling abstraction.
- Continue rejecting local prompt files so resolved prompt text is copied into GCS `config.json`.
- Add validation errors that name the exact invalid field and the expected shape.

**Warning signs:**

- A config value depends on a local file path, current working directory, or previous local run.
- The roadmap includes generic inheritance before the basic operator path is done.
- Checkpoint scoring gets a separate CLI branch instead of using the same target list.
- Docs need a diagram just to explain how one eval manifest is selected.

**Phase to address:**
Phase 2: Config And Model Target UX.

---

### Pitfall 9: Experiment Artifacts Leak Into Git

**What goes wrong:**
Local SFT TOMLs, raw prediction JSONL, checkpoint summaries, inference manifests, or `results/` outputs are committed accidentally. This pollutes the repo with non-source artifacts and may expose experiment details that should live in GCS records or research notes.

**Why it happens:**
Several workflows generate files inside the repo. Current worktree status already shows untracked local run TOMLs, inference manifests, scoring summaries, and `results/`. Some generated source outputs are legitimate, so a blanket "ignore everything generated" rule is not safe.

**How to avoid:**

- Operator docs should list commit-safe outputs versus local/GCS artifacts.
- Before phase completion, require `git status --short` review focused on model experiment paths.
- Prefer `.local.toml` for local configs and keep examples as placeholders only.
- If a generated artifact must be committed, require an explicit roadmap/task reason.
- Consider tightening `.gitignore` for known local-only SFT/eval outputs after checking existing tracked files.

**Warning signs:**

- `git status` shows `model/research/.../*.local.toml`, `online_predictions.jsonl`, `checkpoint_score_summary.*`, or `results/`.
- A PR includes raw provider outputs instead of pointers to GCS.
- Docs tell operators to edit a tracked real run config.
- Local summaries are updated by tests or normal CLI usage.

**Phase to address:**
Phase 5: Operator Docs And Hygiene.

---

### Pitfall 10: Console Reports Look Complete But Cannot Support Promotion Decisions

**What goes wrong:**
The new console-first report prints WER and a best checkpoint, but lacks the provenance and denominators needed to decide whether a model should be promoted.

**Why it happens:**
Existing summaries are split between batch eval and checkpoint scoring. It is easy to add nice terminal formatting without enforcing a complete reporting schema.

**How to avoid:**
Every report should include:

- model label and resource name
- backend used: batch or online
- WER, CER, keyword accuracy, exact empty response rate, empty-or-unintelligible rate
- insertion, deletion, substitution counts/rates
- `n_eval_examples` and total reference word count
- dataset/source breakdown
- eval manifest URI, run prefix, raw prediction URI, normalized inference-manifest URI
- prior-context mode/count and prompt provenance

**Warning signs:**

- Console output cannot be traced back to a GCS artifact.
- The "best" model is sorted only by WER without empty response or dataset breakdown context.
- Total reference word count is missing.
- A local summary has more fields than the console report, or vice versa.

**Phase to address:**
Phase 1: Reporting Contract.

## Technical Debt Patterns

| Shortcut | Immediate Benefit | Long-term Cost | When Acceptable |
|----------|-------------------|----------------|-----------------|
| Keep `empty_rate` as the main display name | Avoids changing existing summaries | Operators confuse exact empty responses with empty-or-unintelligible rate | Only as a deprecated JSON alias, never as the primary console label |
| Read from local `results/` first | Faster local iteration | Stale state and irreproducible comparisons | Only as a cache after validating GCS state and artifact hashes |
| Duplicate prompt/request construction in a CLI wrapper | Fast implementation | Prompt drift invalidates checkpoint comparisons | Never for maintained workflow code |
| Add a rich config DSL | Seems flexible for future experiments | New operators must learn hidden execution rules | Never before simple explicit configs are complete |
| Commit real run TOMLs or raw predictions | Makes examples concrete | Pollutes source history and risks leaking experiment artifacts | Only when explicitly publishing a curated research artifact |

## Integration Gotchas

| Integration | Common Mistake | Correct Approach |
|-------------|----------------|------------------|
| Vertex Gemini batch prediction | Assuming every model-like resource can be batch-scored | Resolve target backend explicitly; use batch only for supported model resources and online scoring for checkpoints unless live API validation proves otherwise |
| Vertex locations | Using `[gcp].location` for all resources | Extract location from full model/endpoint resource names; keep special model-location rules visible |
| GCS run prefixes | Treating local files as canonical | Use `gs://<bucket>/sft/runs/<round_id>/config.json` and artifact URIs as durable state |
| Batch output parsing | Scoring only returned predictions | Iterate eval rows; missing predictions score as empty hypotheses and are reported separately |
| Gemini prior context | Copying prior turns into manifests | Build histories dynamically from canonical rows with shared helpers |

## Performance And Cost Traps

| Trap | Symptoms | Prevention | When It Breaks |
|------|----------|------------|----------------|
| Online checkpoint scoring without resume/sync | Process interruption loses hours of paid calls | Keep `online_predictions.jsonl` resumable, upload every `sync_every`, and download existing GCS predictions before scoring | Any full eval set or flaky network run |
| High concurrency without visible errors | Empty/error responses spike and look like model behavior | Report error count, exact empty count, retry settings, and concurrency in summary | Medium-to-large checkpoint sweeps |
| Full batch/eval runs for every config edit | Costly slow feedback | Keep unit tests mocked; use smoke `--limit` only for operator validation; paid operations require explicit commands | During config/report iteration |
| Reusing existing batch output after input changes | Fast but stale WER | Store/compare input hash or force new `round_id` | Any rerun with changed manifest, prompt, context, or model |

## UX Pitfalls

| Pitfall | User Impact | Better Approach |
|---------|-------------|-----------------|
| Report sorted by one metric only | Operator picks a checkpoint with bad empty-response or keyword behavior | Show WER plus CER, keyword accuracy, empty metrics, error breakdown, and dataset breakdown |
| Config hides masked/unmasked selection | Operator cannot tell what corpus was scored | Separate configs/manifests with explicit `canonical_eval_uri` in every report |
| Model target names are not normalized | Operator mixes publisher IDs, endpoints, models, and checkpoints incorrectly | Accept model resources through one `models` list, resolve backend/type in code, and print the resolved target |
| Local-only docs | New teammate cannot reproduce the run | Docs should start from manifests/config, show commands, and point to GCS outputs |

## Looks Done But Is Not Checklist

- [ ] **Reporting:** exact empty response rate and empty-or-unintelligible rate are both present, named clearly, and tested with `""` plus `[UNINTELLIGIBLE]`.
- [ ] **Scoring denominator:** `n_eval_examples`, scored hypothesis count, and total reference word count are visible and consistent.
- [ ] **Missing predictions:** missing provider outputs are counted separately and still score as empty hypotheses.
- [ ] **Prompt parity:** batch eval, checkpoint scoring, and maintained notebooks import shared `common.gemini` prompt/request helpers.
- [ ] **Prior context:** prepare/eval/checkpoint tests prove identical history construction from the same canonical rows.
- [ ] **Masked/unmasked:** each report names its eval manifest URI and source breakdown.
- [ ] **GCS state:** commands can resume from GCS `config.json`; local `results/` is not required.
- [ ] **Checkpoint scoring:** endpoint location, concurrency, retries, sync interval, and prediction GCS URI are recorded.
- [ ] **Artifact hygiene:** `git status --short` contains no accidental local TOMLs, raw predictions, or result mirrors.

## Recovery Strategies

| Pitfall | Recovery Cost | Recovery Steps |
|---------|---------------|----------------|
| Ambiguous empty metrics shipped | MEDIUM | Rename display fields, keep compatibility aliases, regenerate summaries from stored predictions, and add golden tests |
| Stale local or batch output used | HIGH | Rebuild report from GCS config and raw prediction URIs; rerun eval with a new `round_id` if input hash cannot be proven |
| Prompt drift found after comparisons | HIGH | Re-score affected models with shared helpers and mark old reports non-comparable |
| Prior-context mismatch found | HIGH | Regenerate SFT JSONL/eval inputs from canonical manifests and rerun affected comparisons |
| Checkpoint endpoint sent through wrong backend | MEDIUM | Switch target to online scorer, reuse any valid predictions if resource/request shape matches, otherwise rerun with smoke limit first |
| Experiment artifacts committed | LOW to MEDIUM | Remove artifacts in a follow-up cleanup commit or revert, then tighten docs/ignore rules for the specific path |

## Pitfall-To-Phase Mapping

| Pitfall | Prevention Phase | Verification |
|---------|------------------|--------------|
| Ambiguous empty-rate metrics | Phase 1: Reporting Contract | Golden test distinguishes exact empty `""` from `[UNINTELLIGIBLE]`; console and JSON use explicit names |
| Missing predictions disappear | Phase 1 and Phase 4 | Unit test scores missing predictions as `""` and reports missing count |
| Local `results/` becomes source of truth | Phase 4: Durable Execution State | Eval/scoring tests load GCS `config.json`; reports include GCS URIs |
| Prompt/request drift | Phase 3: Prompt And Prior-Context Parity | Drift-guard tests cover batch eval, checkpoint scorer, and notebook imports |
| Prior-context mismatch | Phase 3: Prompt And Prior-Context Parity | Fixture asserts identical context histories and request text across prepare/eval/checkpoint |
| Batch/checkpoint backend confusion | Phase 2 and Phase 4 | Target resolver tests classify publisher/tuned/checkpoint resources and record backend/location |
| Masked/unmasked coupling | Phase 1 and Phase 2 | Reports include eval manifest URI, source breakdown, `n_eval_examples`, and total ref words |
| Config layer over-complexity | Phase 2: Config And Model Target UX | Config schema remains explicit; separate configs cover masked/unmasked; no local prompt files |
| Artifact commits | Phase 5: Operator Docs And Hygiene | Phase closeout includes `git status --short` artifact review |
| Incomplete console reports | Phase 1: Reporting Contract | Console output matches JSON/Markdown fields required for promotion decisions |

## Sources

- `radio-transcription/.planning/PROJECT.md`
- `radio-transcription/.planning/codebase/CONCERNS.md`
- `radio-transcription/.planning/codebase/TESTING.md`
- `radio-transcription/model/scripts/sft/README.md`
- `radio-transcription/model/src/common/scoring.py`
- `radio-transcription/model/src/common/gemini/context.py`
- `radio-transcription/model/src/common/gemini/vertex.py`
- `radio-transcription/model/src/common/gemini/batch.py`
- `radio-transcription/model/src/gemini_sft/evaluate.py`
- `radio-transcription/model/src/gemini_sft/config.py`
- `radio-transcription/model/src/gemini_sft/prepare.py`
- `radio-transcription/model/scripts/sft/score_gemini_sft_checkpoints_online.py`
- `radio-transcription/model/tests/common/tests/test_drift_guard.py`
- `radio-transcription/model/tests/gemini_sft/test_checkpoint_scorer.py`
- Context7 lookup, official Google Cloud docs: `https://docs.cloud.google.com/gemini-enterprise-agent-platform/models/capabilities/batch-prediction-from-cloud-storage`
- Context7 lookup, official Google Cloud docs: `https://docs.cloud.google.com/gemini-enterprise-agent-platform/models/tuning/checkpoints`
- Context7 lookup, official Google Cloud docs: `https://docs.cloud.google.com/gemini-enterprise-agent-platform/models/tuning/supervised-tuning/use`

---
*Pitfalls research for: Gemini SFT/evaluation workflow onboarding*
*Researched: 2026-06-28*
