# Phase 4: Operations and Rollout Proof - Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in CONTEXT.md — this log preserves the alternatives considered.

**Date:** 2026-06-27
**Phase:** 4-Operations and Rollout Proof
**Areas discussed:** none selected

---

## Gray Area Selection

| Option | Description | Selected |
|--------|-------------|----------|
| Operational Signals | Which logs/metrics/alerts should Phase 4 add versus rely on existing platform metrics? | |
| Staging Proof Method | Should proof be a manual runbook, a small script, or a CI/deploy workflow step? | |
| Failure And DLQ Drill | How should operators intentionally verify retryable failure, permanent/config failure, and DLQ behavior without affecting ingestion or production notifications? | |
| Runbook Ownership | Which docs belong in the public repo versus deployment repo? | |

**User's choice:** none

**Notes:** The user declined additional gray-area discussion. Phase 4 context
therefore carries forward locked decisions from Phases 1-3 and gives the
planner bounded discretion to choose the simplest maintainable observability,
staging proof, and rollout documentation plan that satisfies `OPS-01..04`.

---

## the agent's Discretion

- Choose exact log metric names, alert thresholds, dashboard panels, runbook
  file names, and staging verification command/script shape.
- Keep reusable code/contracts in the public repo and environment-specific
  operations/runbook details in the deployment repo.
- Do not add durable delivery, replay, database polling, delivery state tables,
  or write-path delivery coupling.

## Deferred Ideas

- Replay selected `feed_audit_events` rows by event ID or time range.
- Delivery attempt history outside Cloud Logging/Pub/Sub DLQ.
- Admin UI/API delivery status for individual audit events.
- Stronger outbound webhook authentication or zero-downtime key rotation.
- Multi-destination fanout.
