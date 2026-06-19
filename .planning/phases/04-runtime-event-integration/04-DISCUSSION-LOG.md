# Phase 4: Runtime Event Integration - Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in CONTEXT.md — this log preserves the alternatives considered.

**Date:** 2026-06-19
**Phase:** 04-Runtime Event Integration
**Areas discussed:** Failure Outcome Audit Boundary, Recovery Event Boundary, Echo/Synchronous Store Parity, Runtime Actor and Diagnostic Detail Policy

---

## Failure Outcome Audit Boundary

| Option | Description | Selected |
|--------|-------------|----------|
| Persisted abnormal states | Emit events only when failure persistence changes lifecycle state/reason meaning. | ✓ |
| Budgeted only | Emit events only for failures that consume feed failure budget. | |
| All classified failures | Emit for every classified runtime failure. | |

**User's choice:** A new Feed Audit Event should be created if the failure causes the feed to change to a new `status` + `status_reason` combination.
**Notes:** Diagnostic-detail churn, retry timing, and failure count changes alone do not emit. Threshold crossing emits `feed.quarantined` only. Changing from one failing reason to another emits `feed.failure_reported`.

---

## Recovery Event Boundary

| Option | Description | Selected |
|--------|-------------|----------|
| Abnormal-to-normal runtime success | Emit when successful runtime activity clears `failing` or `quarantined` status. | ✓ |
| Claim-time recovery | Emit when a worker claims a failing feed. | |
| Any success after failure | Emit for any success if there was prior failure history. | |

**User's choice:** The feed changes from abnormal status including failing or quarantine to normal status.
**Notes:** Claiming is not recovery. Admin/manual reset remains `feed.reset`. If only `status_reason_detail` is cleared and status does not change, that is not `feed.recovered`.

---

## Echo/Synchronous Store Parity

| Option | Description | Selected |
|--------|-------------|----------|
| Full parity now | Echo sync failure and success paths emit the same v1 audit events as async runtime. | ✓ |
| Narrow failure parity only | Add failure/quarantine only and defer recovery. | |
| Defer Echo audit | Leave Echo for a later phase. | |

**User's choice:** Full parity now.
**Notes:** Echo can use sync-specific SQL/helper mechanics, but event semantics must match. Skipped Echo recordings for quarantined/deactivated feeds create no audit event when no DB mutation occurs. Echo success emits recovery if it moves abnormal status to normal.

---

## Runtime Actor and Diagnostic Detail Policy

| Option | Description | Selected |
|--------|-------------|----------|
| Semantic service actors | Use `service:collector-runtime` and `service:echo-ingestion`; keep GCP service account as provenance/fallback. | ✓ |
| GCP service account actors | Use `gcp-sa:<email>` as the normal production actor. | |
| Source-specific actor IDs | Encode source type into actor IDs. | |

**User's choice:** User accepted the design after GCP auth research.
**Notes:** Incoming auth claims can identify caller service account (`email`, `sub`, `azp`) for trust decisions. They do not identify the human causal admin. Runtime actor IDs stay semantic, with source type and GCP service-account email as optional metadata. No `system:` prefix remains.

---

## the agent's Discretion

- Exact helper names and SQL shape.
- Exact metadata fields for runtime provenance.
- Whether legacy `quarantine_reason` needs temporary mirroring to avoid breaking an existing flow.
- Concrete sanitizer implementation for bounded `status_reason_detail`.

## Deferred Ideas

- Watch Duty delivery and receiver design.
- Admin timeline APIs/UI.
- Retention enforcement.
- Routine lease/heartbeat event history.
- Signed actor-context JWT/HMAC if trusted caller topology becomes more complex.
