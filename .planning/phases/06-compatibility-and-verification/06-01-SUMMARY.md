---
phase: 06-compatibility-and-verification
plan: 01
subsystem: docs
tags: [collector-guide, failure-policy, quarantine-routing]
requires:
  - phase: 05-producer-and-runtime-routing-merge
    provides: "Final v1.1 runtime routing semantics for pipeline and system-owned failures."
provides:
  - "Collector authoring guide reflects final v1.1 Pub/Sub post-bookmark publish semantics."
affects: [collector-docs, failure-policy, phase-06-closeout]
tech-stack:
  added: []
  patterns:
    - "Collector docs describe budgeted versus non-budgeted policy lanes from failure_policy."
key-files:
  created:
    - .planning/phases/06-compatibility-and-verification/06-01-SUMMARY.md
  modified:
    - backend/pipeline/ingestion/collectors/README.md
key-decisions:
  - "Documented pipeline_publish_after_bookmark_failed as thresholded and budgeted in v1.1."
  - "Documented the split system status reasons needed for current backend routing."
patterns-established:
  - "Post-bookmark Pub/Sub publish failure docs must mention replay_missing=true and data_gap_known=true."
requirements-completed: [DOC-11]
duration: 12 min
completed: 2026-06-15
---

# Phase 06 Plan 01: Collector Guide Policy Semantics Summary

**Collector authoring guide now describes final v1.1 quarantine routing for post-bookmark Pub/Sub publish gaps and split system status reasons**

## Performance

- **Duration:** 12 min
- **Started:** 2026-06-15T19:18:00Z
- **Completed:** 2026-06-15T19:30:30Z
- **Tasks:** 2
- **Files modified:** 1

## Accomplishments

- Replaced stale prose that described all runtime `_PipelineFailure` cases as non-budgeted.
- Documented that GCS upload and bookmark-write failures remain `system_pipeline_error` and non-budgeted.
- Documented that `pipeline_publish_after_bookmark_failed` records `replay_missing=true` and `data_gap_known=true`, and can consume the feed quarantine budget after the existing threshold.
- Added README rows for `system_runtime_configuration_invalid`, `system_credential_access_failed`, and `system_source_payload_invalid`.
- Clarified that `system_authentication_failed` is upstream provider rejection, distinct from internal credential access failure.

## Task Commits

Each task was committed atomically:

1. **Task 1 and Task 2: Collector guide policy documentation** - `001c1b84` (docs)

## Files Created/Modified

- `backend/pipeline/ingestion/collectors/README.md` - Updates final v1.1 runtime pipeline and split system status reason guidance.
- `.planning/phases/06-compatibility-and-verification/06-01-SUMMARY.md` - Captures plan execution evidence.

## Decisions Made

- Kept the change scoped to backend collector documentation and Phase 6 planning artifacts.
- Preserved v1.1 wording that durable replay is not yet available, so the post-bookmark publish gap can become feed quarantine after the existing threshold.

## Verification

Acceptance checks:

```text
rg -n "must not quarantine the feed" backend/pipeline/ingestion/collectors/README.md
```

Result: no matches, as expected.

```text
rg -n "pipeline_publish_after_bookmark_failed.*threshold|threshold.*pipeline_publish_after_bookmark_failed" backend/pipeline/ingestion/collectors/README.md
```

Result: matched the updated `pipeline_publish_after_bookmark_failed` table row.

```text
rg -n "replay_missing=true.*data_gap_known=true|data_gap_known=true.*replay_missing=true" backend/pipeline/ingestion/collectors/README.md
```

Result: matched the updated `pipeline_publish_after_bookmark_failed` table row.

```text
rg -n "system_pipeline_error" backend/pipeline/ingestion/collectors/README.md
```

Result: matched the runtime boundary paragraph and system-owned reason table row.

```text
rg -n "system_runtime_configuration_invalid|system_credential_access_failed|system_source_payload_invalid" backend/pipeline/ingestion/collectors/README.md
```

Result: matched all three split system status reason rows.

```text
rg -n "upstream provider credential rejection|provider credential rejection|credentials, tokens, or partner auth are rejected" backend/pipeline/ingestion/collectors/README.md
```

Result: confirmed upstream provider auth rejection remains distinct from Watch Duty credential access failure.

```text
git diff --check
```

Result: no whitespace errors.

## Deviations from Plan

None - plan executed exactly as written.

---

**Total deviations:** 0 auto-fixed.
**Impact on plan:** No scope change.

## Issues Encountered

None.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness

Ready for Plan 06-02 focused backend verification and closeout documentation.

---
*Phase: 06-compatibility-and-verification*
*Completed: 2026-06-15*
