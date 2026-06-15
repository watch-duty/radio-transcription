---
phase: 01-policy-and-storage-foundation
plan: 01
subsystem: ingestion
tags: [failure-policy, feed-status-reason, quarantine, runtime]
requires: []
provides:
  - Pure failure policy vocabulary and classifier.
  - Strict FeedFailure evidence boundary.
  - Pipeline publish-after-bookmark status reason.
affects: [policy-and-storage-foundation, ingestion-runtime, collectors]
tech-stack:
  added: []
  patterns:
    - "failure_policy.py owns pure evidence-to-intent classification."
    - "Runtime executes policy decisions; models carry facts-only evidence."
key-files:
  created:
    - backend/pipeline/ingestion/failure_policy.py
    - backend/pipeline/ingestion/tests/test_failure_policy.py
  modified:
    - backend/pipeline/ingestion/models.py
    - backend/pipeline/ingestion/collector_runtime.py
    - backend/pipeline/storage/feed_store.py
    - backend/pipeline/storage/tests/test_feed_store.py
key-decisions:
  - "Use facts-only FailurePolicyEvidence without reason_family."
  - "Keep feed quarantine verdicts in failure_policy.classify_failure_policy, not in collectors."
requirements-completed: [POL-01, POL-02, STAT-01]
duration: inline phase execution
completed: 2026-06-15
---

# Phase 01 Plan 01: Policy Model And Status Primitives Summary

**Pure failure policy classifier with strict FeedFailure evidence and pipeline publish-gap reason support**

## Performance

- **Duration:** Inline phase execution
- **Started:** 2026-06-15T02:00:00Z
- **Completed:** 2026-06-15T02:41:04Z
- **Tasks:** 3
- **Files modified:** 8

## Accomplishments

- Added `failure_policy.py` with owner scope, failure scope, endpoint kind, policy intent, executed action, pipeline stage, evidence, decision, classifier, and predicates.
- Made `FeedFailure` require `failure_policy.FailurePolicyEvidence`.
- Added `pipeline_publish_after_bookmark_failed` as a canonical status reason and documented that `pipeline_` reasons are non-budgeted.

## Task Commits

Executed inline in a shared dirty worktree; implementation was committed as one coherent phase slice:

1. **Phase implementation** - `f502e518` (`feat(ingestion): add quarantine failure policy foundation`)

## Files Created/Modified

- `backend/pipeline/ingestion/failure_policy.py` - Pure policy vocabulary and classifier.
- `backend/pipeline/ingestion/models.py` - Strict `FeedFailure` evidence boundary.
- `backend/pipeline/ingestion/tests/test_failure_policy.py` - Policy classifier coverage.
- `backend/pipeline/storage/feed_store.py` - Canonical `pipeline_` status reason enum value.

## Decisions Made

- No `reason_family` field was added; routing uses structured evidence plus `FeedStatusReason`.
- Evidence stays facts-only. Verdict fields are emitted on decisions, not evidence.
- `pipeline_` status reasons may explain ingestion progress gaps but must not increment feed budget.

## Deviations from Plan

The worktree already contained overlapping phase edits, so plans 01-01 through 01-04 were executed inline and committed as one cohesive implementation commit instead of separate per-plan commits. Scope remained within the phase files and verification commands.

## Issues Encountered

None.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness

Ready for collector call-site evidence wiring and runtime/storage verification.

## Self-Check: PASSED

Verified by targeted phase gate: `39 passed`; `git diff --check` passed before commit.

---
*Phase: 01-policy-and-storage-foundation*
*Completed: 2026-06-15*
