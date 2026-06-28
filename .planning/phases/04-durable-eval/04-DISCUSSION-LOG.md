# Phase 4: Durable Eval - Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in CONTEXT.md — this log preserves the alternatives considered.

**Date:** 2026-06-28T21:00:23Z
**Phase:** 4-Durable Eval
**Areas discussed:** target parallelism, model config shape, reuse validation, dataset breakdowns, durable summaries

---

## Target Parallelism

| Option | Description | Selected |
|--------|-------------|----------|
| Parallel all targets by default | Each configured target runs concurrently; online targets still use per-target row concurrency. | |
| Parallel batch, serialize online | Batch targets submit in parallel, endpoint/checkpoint online targets run one at a time. | |
| Parallel all with optional target cap | Add a target-level cap such as `eval.execution.target_concurrency`. | |
| One model per eval run | Do not support multiple targets in one eval run; external wrappers can run separate configs in parallel. | ✓ |

**User's choice:** One model per eval run.
**Notes:** The user clarified that the CLI should not run multiple models in one eval run. If callers want multiple models/checkpoints in parallel, they can run separate configs externally.

---

## Model Config Shape

| Option | Description | Selected |
|--------|-------------|----------|
| Exactly one required | Require one and only one eval model for `gemini-sft eval`; zero or multiple targets is a config error. | ✓ |
| Allow multiple but serialize | Keep current multi-target support, but do not add internal parallelism. | |
| You decide | Choose the smallest change that matches one model per eval run. | |

**User's choice:** Exactly one required.
**Notes:** The user also explicitly changed the config shape from plural `[[eval.models]]` to singular `[eval.model]`, with matching durable singular state.

---

## Reuse Validation

| Option | Description | Selected |
|--------|-------------|----------|
| Require identity metadata | Batch output is reusable only if a sidecar identity matches model, manifest, audio order, prompts, context, generation config, and safety settings. Missing metadata fails before paid work. | ✓ |
| Ignore old output and resubmit | If metadata is missing or mismatched, submit a new batch job. | |
| Reuse path-based output | Keep current path-only reuse behavior. | |

**User's choice:** Require identity metadata.
**Notes:** This mirrors the existing online reuse behavior introduced in Phase 3 and satisfies the stale-output validation requirement without surprise paid work.

---

## Dataset Breakdowns

| Option | Description | Selected |
|--------|-------------|----------|
| Add `[[eval.manifests]]` | Support multiple eval manifests with labels and use those labels for dataset breakdowns. | |
| Keep one manifest and require row metadata | Use an explicit row field such as `dataset`; fail if missing. | |
| Keep one manifest and infer labels | Infer dataset from GCS or audio path patterns. | |
| Defer breakdowns | Skip dataset breakdown requirements for now and keep one `eval_manifest_uri`. | ✓ |

**User's choice:** Defer breakdowns.
**Notes:** The current code supports one `eval_manifest_uri`, not multiple eval manifests. The user decided to skip dataset breakdown requirements for Phase 4 and revisit them in a follow-up.

---

## Durable Summaries

| Option | Description | Selected |
|--------|-------------|----------|
| Stable run-level paths | Upload `wer_summary.json` and `wer_summary.md` to `gs://<bucket>/sft/runs/<round_id>/evals/`. | ✓ |
| Per-model paths | Upload under `evals/<label>/summary.*`. | |
| Console/local only | Keep writing local summaries and console output only. | |

**User's choice:** Stable run-level paths.
**Notes:** This matches the one-model eval contract and makes reports durable without requiring local `results/`.

---

## Summary Overwrite Behavior

| Option | Description | Selected |
|--------|-------------|----------|
| Overwrite stable summary | `evals/wer_summary.{json,md}` always reflects the latest successful eval for the run. | ✓ |
| Version every summary | Write timestamped summaries plus a latest pointer. | |
| You decide | Choose the simplest durable behavior. | |

**User's choice:** Overwrite stable summary.
**Notes:** Stable summary paths should be updated after each successful eval rerun.

## the agent's Discretion

- Helper/module names and exact test organization are left to the implementing agent.
- The implementation must preserve the single-model eval contract, strict reuse metadata, and stable GCS summary paths.

## Deferred Ideas

- Internal multi-target execution and parallel target scheduling.
- Dataset-level breakdowns and multiple eval manifests.
- Versioned summary artifacts.
