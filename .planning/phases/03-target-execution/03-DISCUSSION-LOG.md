# Phase 3: Target Execution - Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in CONTEXT.md - this log preserves the alternatives considered.

**Date:** 2026-06-28
**Phase:** 3-Target Execution
**Areas discussed:** Backend routing policy, Target metadata and checkpoint discovery, Online prediction resume/reuse rules, Smoke and failure behavior

---

## Backend Routing Policy

| Option | Description | Selected |
|--------|-------------|----------|
| Conservative default | Publisher/base models use batch; full Vertex endpoint resources use online `generate_content`; no live probing. | yes |
| Probe then route | Try a small batch validation for endpoint resources and fall back to online if batch is rejected. | |
| Config-driven override | Infer a default but allow an override. | yes |

**User's choice:** Conservative default, plus a config setting to override the backend.
**Notes:** The user rejected an explicit `auto` value. If `[eval.execution].backend` is omitted, default routing applies. If present, `backend` can force `batch` or `online` for all targets in that config. No per-target backend override table in Phase 3.

---

## Target Metadata and Checkpoint Discovery

| Option | Description | Selected |
|--------|-------------|----------|
| Explicit targets only | Eval only runs targets listed in `[[eval.models]]`; no tuning-job checkpoint discovery or metadata enrichment. | yes |
| Explicit targets plus optional tuning-job enrichment | Eval only runs listed targets, but may fetch the tuning job to attach epoch/step metadata when endpoints match. | |
| Discovery mode | Eval may discover checkpoints from a tuning job and run them even if not listed in `[[eval.models]]`. | |

**User's choice:** Explicit targets only.
**Notes:** Phase 3 should not fetch tuning jobs for checkpoint discovery or epoch/step enrichment. Checkpoint endpoints are just configured model strings.

---

## Online Prediction Resume/Reuse Rules

| Option | Description | Selected |
|--------|-------------|----------|
| Resume by default | Download existing rows and skip completed `audio_filepath`s, like the current checkpoint scorer. | yes |
| Require explicit resume | Fail if predictions exist unless an explicit resume setting is used. | |
| Overwrite by default | Ignore old predictions unless resume is requested. | |

**User's choice:** Resume by default, but only if the setup is the same; otherwise fail loudly.
**Notes:** The user selected full request identity for "same setup": target label/model, eval manifest URI, evaluated audio URI set/order, system prompt, user prompt, prior context count/mode, generation config, and safety settings. Operational settings such as concurrency and sync cadence should not invalidate reuse.

---

## Smoke and Failure Behavior

| Option | Description | Selected |
|--------|-------------|----------|
| Keep failed rows as empty predictions with an error field | Score exhausted rows as empty hypotheses, report error count, and continue. | yes |
| Abort target on first exhausted row | Do not produce a partial target report. | |
| Complete predictions but fail final exit code if any row errors | Write artifacts and reports, but return non-zero when any row errors. | |

**User's choice:** Keep failed rows as empty predictions with an error field.
**Notes:** The command should continue and write artifacts/reports even when some online rows exhaust retries. Failed rows remain in the denominator. Report should expose error count.

---

## Execution Knobs

| Option | Description | Selected |
|--------|-------------|----------|
| CLI only | Keep limit, concurrency, retries, sync cadence, and log cadence as command options. | |
| Config only | Put operator-facing execution settings under `[eval.execution]`. | yes |
| Config defaults, CLI override | Config may set defaults, CLI can override per run. | |

**User's choice:** Config only.
**Notes:** Expose only `backend`, `limit`, `concurrency`, and `max_retries`. The user asked what `sync_every` and `limit` mean, then chose not to expose `retry_sleep_seconds`, `sync_every`, or `log_every` in the config contract.

## the agent's Discretion

- Exact module/type names are left to implementation planning.
- Internal defaults for retry sleep, sync cadence, and log cadence can follow
  existing checkpoint scorer behavior unless research finds a safer value.

## Deferred Ideas

- Live backend probing.
- Per-target backend overrides.
- Tuning-job checkpoint discovery and epoch/step enrichment.
- Full stale-output hashing for all output types.
- Multi-target parallel execution.
- Dataset breakdown reporting and full operator docs.
