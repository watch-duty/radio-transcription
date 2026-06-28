# Phase 1: Reporting Contract - Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in CONTEXT.md - this log preserves the alternatives considered.

**Date:** 2026-06-28
**Phase:** 1-Reporting Contract
**Areas discussed:** Metric vocabulary, report schema, missing predictions, batch/checkpoint parity boundary

---

## Metric Vocabulary

| Option | Description | Selected |
|--------|-------------|----------|
| Keep legacy `empty_rate` | Continue using historical names in operator reports. | |
| Canonical explicit names | Use `empty_or_unintelligible_rate` and `empty_response_rate` in reports. | x |
| Rename both later | Defer naming until implementation. | |

**User's choice:** Inferred from prior project decisions and Phase 1
requirements. Earlier discussion explicitly asked to evaluate exact empty
responses and historical empty rate, then keep one naming scheme to avoid
confusion.
**Notes:** Existing code uses both `empty_rate` and `hallucination_rate` for
the historical metric. New reports should avoid those as primary names.

---

## Report Schema

| Option | Description | Selected |
|--------|-------------|----------|
| Separate console/JSON/Markdown formatting | Patch each output independently. | |
| Shared structured report object | Render console, JSON, and Markdown from one schema. | x |
| Console only | Improve console output now and defer JSON/Markdown. | |

**User's choice:** Inferred from RPT-02 and prior feedback that console display
matters, while JSON and Markdown must remain comparable.
**Notes:** The current batch path writes `wer_summary.json` and
`wer_summary.md`; checkpoint scoring writes its own summary files. This phase
should make both paths use the same columns and metric semantics.

---

## Missing Predictions

| Option | Description | Selected |
|--------|-------------|----------|
| Drop missing predictions | Exclude missing provider rows from scoring. | |
| Score missing as empty and count separately | Keep them in WER/CER denominator and expose `missing_prediction_count`. | x |
| Fail on any missing prediction | Treat missing provider rows as fatal. | |

**User's choice:** Locked by RPT-04 and existing batch behavior.
**Notes:** Missing provider rows and exact empty model outputs both score as
empty hypotheses, but they need separate operational counters.

---

## Batch/Checkpoint Parity Boundary

| Option | Description | Selected |
|--------|-------------|----------|
| Reporting-only parity | Share metric/report helpers but leave inference execution unchanged. | x |
| Full target execution parity | Introduce unified target config and backend selection now. | |
| Checkpoint-only CLI path | Add checkpoint-specific primary CLI options now. | |

**User's choice:** Inferred from roadmap dependency order and earlier user
preference to represent checkpoints as normal model targets later, not as a
checkpoint-only CLI branch.
**Notes:** Phase 1 should produce comparable reports from existing paths. Phase
2 owns target config; Phase 3 owns backend execution.

---

## the agent's Discretion

- Exact module names and dataclass names for the report contract.
- Whether the old flat batch metrics remain as migration aliases during this
  milestone, as long as the new report outputs use the canonical contract.
- Exact console renderer implementation, provided it prints the full table.

## Deferred Ideas

- Unified `models` config shape.
- Masked/unmasked run setup.
- Checkpoint endpoint backend selection and parallel execution.
- Dataset breakdown reports.
- Operator documentation and artifact hygiene docs.
