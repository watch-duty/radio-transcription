---
phase: 03-service-and-compatibility-surface
plan: "01"
subsystem: api
tags: [fastapi, pydantic, asyncpg, diagnostic-detail]
requires:
  - phase: 02-transactional-storage-writes
    provides: Storage-owned audited feed mutations and audit snapshots.
provides:
  - Public feed storage projections include status_reason_detail.
  - FastAPI Feed responses expose status_reason_detail.
  - FastAPI Feed responses omit the deprecated quarantine_reason field.
affects: [phase-03-bff-migration, phase-04-runtime-diagnostics]
tech-stack:
  added: []
  patterns: [canonical-diagnostic-detail, service-response-contract]
key-files:
  created:
    - .planning/phases/03-service-and-compatibility-surface/03-01-SUMMARY.md
  modified:
    - backend/pipeline/storage/feed_queries.py
    - backend/pipeline/storage/feed_store.py
    - backend/pipeline/storage/tests/test_feed_store.py
    - backend/services/feeds/models.py
    - backend/services/feeds/tests/test_api.py
    - backend/services/feeds/tests/test_service.py
key-decisions:
  - "Public FastAPI feed responses now expose status_reason_detail instead of quarantine_reason."
  - "Internal audit snapshots may still include quarantine_reason while public service contracts migrate."
patterns-established:
  - "Storage projections must return status_reason_detail wherever _row_to_feed maps a full feed."
  - "Public Feed API diagnostic detail is canonical status_reason_detail, with quarantine_reason kept out of response models."
requirements-completed: [DIAG-04, COMP-01, COMP-02]
duration: 9 min
completed: 2026-06-19
---

# Phase 03 Plan 01: Backend Diagnostic Detail Service Contract Summary

**FastAPI feed responses and storage projections now expose canonical status_reason_detail while omitting public quarantine_reason.**

## Performance

- **Duration:** 9 min
- **Started:** 2026-06-19T17:27:30Z
- **Completed:** 2026-06-19T17:36:49Z
- **Tasks:** 3
- **Files modified:** 6

## Accomplishments

- Added `status_reason_detail` to public feed SQL projections for create, get, list, update, and reset-backed row mapping.
- Added `status_reason_detail` to the storage `Feed` typed dict and `_row_to_feed` mapper.
- Replaced the public FastAPI `Feed.quarantine_reason` response field with `Feed.status_reason_detail`.
- Added storage, API, and service tests covering canonical diagnostic detail while keeping request/response actor fields absent.

## Task Commits

Each task was committed atomically:

1. **Task 1: Add status_reason_detail to storage public feed projections** - `de3158b3` (feat)
2. **Task 2: Move FastAPI Feed responses to status_reason_detail** - `54086ec6` (feat)
3. **Task 3: Update service fixtures for canonical detail** - `43e05799` (test)

## Files Created/Modified

- `backend/pipeline/storage/feed_queries.py` - Adds `status_reason_detail` to normal feed projections.
- `backend/pipeline/storage/feed_store.py` - Maps `status_reason_detail` into full feed rows.
- `backend/pipeline/storage/tests/test_feed_store.py` - Covers row mapping and SQL projection contracts.
- `backend/services/feeds/models.py` - Exposes canonical diagnostic detail in the FastAPI `Feed` model.
- `backend/services/feeds/tests/test_api.py` - Verifies API response field behavior.
- `backend/services/feeds/tests/test_service.py` - Keeps service fixtures aligned with the canonical field.

## Decisions Made

Public service responses now use `status_reason_detail` only. The storage and audit layer can still retain `quarantine_reason` internally until Phase 4 runtime cleanup.

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered

None.

## Verification

Passed:

```bash
safe-run -- uv run python -m pytest backend/pipeline/storage/tests/test_feed_store.py::TestStatusReasonRowMapping backend/pipeline/storage/tests/test_feed_store.py::TestStatusReasonSqlProjection backend/pipeline/storage/tests/test_feed_store.py::TestFeedAuditSql -q
safe-run -- uv run python -m py_compile backend/pipeline/storage/feed_queries.py backend/pipeline/storage/feed_store.py
safe-run -- uv run python -m pytest backend/services/feeds/tests/test_api.py -q
safe-run -- uv run python -m pytest backend/services/feeds/tests/test_service.py backend/services/feeds/tests/test_api.py -q
safe-run -- uv run python -m pytest backend/services/feeds/tests/test_api.py backend/services/feeds/tests/test_service.py -q
git diff --check -- backend/pipeline/storage/feed_queries.py backend/pipeline/storage/feed_store.py backend/pipeline/storage/tests/test_feed_store.py backend/services/feeds/models.py backend/services/feeds/tests/test_api.py backend/services/feeds/tests/test_service.py .planning/phases/03-service-and-compatibility-surface/03-01-PLAN.md
```

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness

Plan 03-02 can now migrate the BFF, shared TypeScript types, generated OpenAPI, and existing frontend status surfaces to consume `statusReasonDetail`.

## Self-Check: PASSED

---
*Phase: 03-service-and-compatibility-surface*
*Completed: 2026-06-19*
