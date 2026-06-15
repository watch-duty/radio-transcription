# Phase 2: Runtime Routing And Telemetry - Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in CONTEXT.md — this log preserves the alternatives considered.

**Date:** 2026-06-15
**Phase:** 2-runtime-routing-and-telemetry
**Areas discussed:** Strict unannotated failure meaning, Source-class breaker intent without breaker state, Suppressed retry timing, Telemetry contract and severity

---

## Strict Unannotated Failure Meaning

| Option | Description | Selected |
|--------|-------------|----------|
| Runtime fallback only | Treat evidence-less `FeedFailure` as invalid; only untyped exceptions become UNKNOWN telemetry gap. | |
| Support legacy evidence-less `FeedFailure` | Loosen strict boundary and route old-style typed failures to telemetry gap. | |
| Split the term | Rename requirement/test language from unannotated `FeedFailure` to unknown runtime failure, and separately assert evidence-less `FeedFailure` is rejected. | ✓ |

**User's choice:** Split the term.
**Notes:** The strict `FeedFailure` boundary remains. Unknown runtime failures are separate fallback behavior.

---

## Source-Class Breaker Intent Without Breaker State

| Option | Description | Selected |
|--------|-------------|----------|
| Keep `OPEN_BREAKER` as intent | Telemetry names the desired operational lane even though v1 has no breaker state. | |
| Use `SUPPRESS_RETRY` until breakers exist | Telemetry names only what v1 actually executes. | |
| Split intent/action strictly | Keep `policy_intent=OPEN_BREAKER`, but pair with `executed_action=RELEASE_NON_BUDGETED_FAILURE` in v1. | ✓ |

**User's choice:** Split intent/action strictly.
**Notes:** No breaker persistence, canaries, source-class pause, or alert routing in Phase 2.

---

## Suppressed Retry Timing

| Option | Description | Selected |
|--------|-------------|----------|
| Lock 5-15 minutes globally | Simple and already implemented; exact bounds become contract. | |
| Make timing reason/status-specific now | More operational nuance, more policy/code surface. | |
| Keep timing intentionally coarse in v1 | Treat exact timing as implementation detail; test presence/future retry only. | ✓ |

**User's choice:** Keep timing intentionally coarse in v1.
**Notes:** Do not add reason-specific backoff in Phase 2.

---

## Telemetry Contract And Severity

| Option | Description | Selected |
|--------|-------------|----------|
| Minimal stable telemetry | Emit only enough to identify decision and action. | ✓ |
| Decision-mirror telemetry | Emit the full routing input/output for every decision. | |
| Publish-gap rich only | Keep general telemetry minimal but make publish-gap logs rich. | |

**User's choice:** User deferred; agent selected the simplest/lowest-maintenance option.
**Notes:** Telemetry is not routing authority. Routing uses status reason and evidence. Telemetry is only an audit/ops record. Keep existing richer fields if already implemented and tested, but do not expand the contract just for completeness. `post_bookmark_publish_failure` still needs `replay_missing=true` and `data_gap_known=true`.

---

## the agent's Discretion

- Exact helper names and test class split.
- Keep existing richer telemetry fields if removing them creates churn.

## Deferred Ideas

- Durable replay/outbox.
- Real source-class/credential breaker state and canary probing.
- Persistent structured policy audit table.
- Rich operator UI for suppressed retry states.
