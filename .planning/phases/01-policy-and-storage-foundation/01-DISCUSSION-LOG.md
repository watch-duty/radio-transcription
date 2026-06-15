# Phase 1: Policy And Storage Foundation - Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution
> agents. Decisions are captured in CONTEXT.md — this log preserves the
> alternatives considered.

**Date:** 2026-06-15
**Phase:** 1-Policy And Storage Foundation
**Areas discussed:** Policy evidence contract, Non-budgeted storage semantics,
Status reason vocabulary

---

## Policy Evidence Contract

| Option | Description | Selected |
|--------|-------------|----------|
| `models.py` | Keep policy evidence next to `FeedFailure`. | |
| `failure_policy.py` | Create `backend/pipeline/ingestion/failure_policy.py`. | ✓ |
| Storage layer | Put policy enums/types near DB lifecycle state. | |

**User's choice:** `failure_policy.py`.
**Notes:** User accepted a strengthened option 2: types plus pure predicates and
pure evidence/status -> decision classification. Runtime side effects remain in
runtime/store/telemetry layers.

| Option | Description | Selected |
|--------|-------------|----------|
| Types only | Module owns only enum/dataclass vocabulary. | |
| Types + predicates | Module owns vocabulary and pure predicates. | ✓ |
| Full routing policy | Module owns runtime routing and side effects. | |

**User's choice:** Types plus predicates/classification, but not side effects.
**Notes:** The policy module should prevent callers from rediscovering routing
rules independently without becoming a runtime god module.

| Option | Description | Selected |
|--------|-------------|----------|
| Strict helper, compatible raw class | Shared helpers require evidence but raw `FeedFailure` remains loose. | |
| Strict everywhere | `FeedFailure` itself requires policy evidence. | ✓ |
| Soft warning first | Evidence optional in v1 with warnings. | |

**User's choice:** Strict everywhere.
**Notes:** User preferred doing migration now because there is no long-term valid
reason for typed classified failures to lack evidence.

| Option | Description | Selected |
|--------|-------------|----------|
| Required `reason_family` | Every failure provides a stable policy/root-cause family. | |
| Optional/omit | Route without `reason_family` in Phase 1. | ✓ |
| Derived from raw reason | Runtime derives family from reason string. | |

**User's choice:** Omit `reason_family` in Phase 1.
**Notes:** We verified current v1 routing can use `status_reason` plus
ownership/scope/endpoint/stage. Raw `reason` remains forensic detail only.

---

## Non-Budgeted Storage Semantics

| Option | Description | Selected |
|--------|-------------|----------|
| Keep for feed budget only | `failure_count` remains only for budget-eligible feed failures. | ✓ |
| Remove/redefine now | Replace with a new episode model immediately. | |
| Leave legacy meaning | Keep counting any feed/runtime failure. | |

**User's choice:** Keep `failure_count`, but narrow its meaning.
**Notes:** `failure_count` means consecutive feed-budget-eligible failures only.

| Option | Description | Selected |
|--------|-------------|----------|
| `status='failing'` | Reuse existing recovery path with `failure_count=0`. | ✓ |
| `status='unclaimed'` | Release immediately with `retry_after`. | |
| New status | Add `suppressed`/`retrying` lifecycle status. | |

**User's choice:** `status='failing'` with `failure_count=0`.
**Notes:** This preserves schema and scheduler compatibility.

| Option | Description | Selected |
|--------|-------------|----------|
| Write retry state only | Write `retry_after`, `status_reason`, release lease, never touch `quarantine_reason`. | ✓ |
| Clear `quarantine_reason` | Also clear raw forensic quarantine reason. | |
| Clear `status_reason` | Suppressed retry becomes invisible in feed row. | |

**User's choice:** Write retry state only.
**Notes:** `quarantine_reason` remains quarantine forensic data and must not be
mutated by the non-budgeted path.

| Option | Description | Selected |
|--------|-------------|----------|
| Reset to 0 always | Clear old budget debt for every non-budgeted failure. | ✓ |
| Keep if status matches | Preserve count when broad status reason matches. | |
| Reset only for pipeline | Reset only downstream failures. | |

**User's choice:** Reset to 0 always.
**Notes:** This prevents old budget debt from leaking into unrelated
non-budgeted observations.

---

## Status Reason Vocabulary

| Option | Description | Selected |
|--------|-------------|----------|
| Only needed values | Add only enum values current code paths need. | ✓ |
| Broader pipeline families | Add future `pipeline_*` families now. | |
| No new status reason | Reuse `system_pipeline_error` for everything. | |

**User's choice:** Add as many as needed, but no speculative values.
**Notes:** Current need is `pipeline_publish_after_bookmark_failed`.

| Option | Description | Selected |
|--------|-------------|----------|
| Allow `pipeline_` | Use `pipeline_*` for downstream post-capture semantics. | ✓ |
| Keep source/system only | Force all internal conditions under `system_*`. | |
| Rename under system | Use a `system_publish_after_bookmark_failed` value. | |

**User's choice:** Allow `pipeline_`.
**Notes:** `pipeline_*` is justified when recovery semantics are hold/replay or
known data-gap oriented, not simply because code lives in a pipeline package.

| Option | Description | Selected |
|--------|-------------|----------|
| Prefix decides budget | Use `source_`/`system_`/`pipeline_` prefix to route. | |
| Classifier decides budget | Use `FailurePolicyDecision` from the pure classifier. | ✓ |
| Mixed logic | Combine prefix and classifier rules. | |

**User's choice:** Classifier decides budget.
**Notes:** Prefixes are operator taxonomy. Budget eligibility must come from the
policy classifier.

## the agent's Discretion

- Exact enum member names and helper names may be chosen during planning, as
  long as ownership boundaries and tests enforce the locked policy.

## Deferred Ideas

- Durable replay/outbox.
- Source-class/credential breaker persistence.
- Persistent policy audit table.
- Renaming `FeedStatusReason`.
