---
phase: 02-transactional-storage-writes
plan: "03"
subsystem: storage
tags: [alloydb, asyncpg, feed-audit, feeds-service, pytest]

requires:
  - phase: 02-transactional-storage-writes
    provides: Plan 02 audit helpers, transaction mocks, and service actor fallback
provides:
  - Transactional audited FeedStore deactivate_feed, reset_feed, and delete_feed writes
  - feed.deleted insertion before current-state hard delete
  - Phase 2 feeds-service actor propagation for lifecycle mutations
affects: [feedstore, feeds-service, audit-writers, integration-tests]

tech-stack:
  added: []
  patterns:
    - Existing-feed lifecycle mutations lock/read audit snapshots before mutation
    - Lifecycle audit rows are inserted inside the same asyncpg transaction as state mutation
    - Direct storage callers pass explicit actor_id for audited lifecycle paths

key-files:
  created: []
  modified:
    - backend/pipeline/storage/feed_store.py
    - backend/pipeline/storage/tests/test_feed_store.py
    - backend/services/feeds/service.py
    - backend/services/feeds/tests/test_service.py
    - integration_tests/storage/test_feed_store_integration.py

key-decisions:
  - "FeedStore deactivate_feed, reset_feed, and delete_feed require explicit keyword-only actor_id and own lifecycle/delete audit construction."
  - "feed.deleted is inserted before DELETE_FEED_SQL using the locked full before snapshot and empty after_values."
  - "Feeds service lifecycle mutations continue using service:feeds-service as the Phase 2 causal actor."

patterns-established:
  - "Lifecycle audit methods lock GET_AUDIT_FEED_SNAPSHOT_SQL first, mutate current state, allocate feed_sequence, and insert audit inside one transaction."
  - "Hard delete audit uses the pre-delete snapshot as the audit identity source before current-state rows and feed_properties are removed."
  - "Integration tests that call FeedStore directly use _TEST_ACTOR_ID to satisfy required storage actor signatures."

requirements-completed: [AUD-04, EVT-03, EVT-04, EVT-05, CON-01, CON-02, CON-03, CON-04]

duration: 7 min
completed: 2026-06-19
---

# Phase 02 Plan 03: Transactional Deactivate Reset Delete Writes Summary

**FeedStore lifecycle and hard-delete mutations now write storage-owned audit rows transactionally with service actor attribution**

## Performance

- **Duration:** 7 min
- **Started:** 2026-06-19T14:46:15Z
- **Completed:** 2026-06-19T14:53:34Z
- **Tasks:** 3
- **Files modified:** 5

## Accomplishments

- Refactored `FeedStore.deactivate_feed` and `FeedStore.reset_feed` to require `actor_id`, capture full before/after snapshots, and emit `feed.deactivated` / `feed.reset` inside one transaction.
- Refactored `FeedStore.delete_feed` to capture the full before snapshot, insert `feed.deleted` with `{}` after values, then run `DELETE_FEED_SQL` in the same transaction.
- Wired `FeedService` lifecycle methods to pass `service:feeds-service` without changing FastAPI route inputs or outputs.
- Updated direct storage integration test call sites for the required lifecycle actor.

## Task Commits

Each TDD task was committed atomically:

1. **Task 1 RED: Audited deactivate/reset tests** - `8be204b4` (test)
2. **Task 1 GREEN: Audited deactivate/reset storage writes** - `bd68ecf9` (feat)
3. **Task 2 RED: Audited hard-delete tests** - `10e10d03` (test)
4. **Task 2 GREEN: Audited hard-delete storage writes** - `efb21405` (feat)
5. **Task 3 RED: Lifecycle service actor tests** - `8b33736c` (test)
6. **Task 3 GREEN: Lifecycle service actor wiring** - `7a75e464` (feat)
7. **Rule 3 fix: Integration lifecycle call sites** - `6cd9ff12` (fix)

**Plan metadata:** Recorded in final docs commit.

## Files Created/Modified

- `backend/pipeline/storage/feed_store.py` - Added transactional audit writes for deactivate, reset, and hard delete.
- `backend/pipeline/storage/tests/test_feed_store.py` - Added lifecycle/delete audit behavior coverage and required actor assertions.
- `backend/services/feeds/service.py` - Passed the Phase 2 `service:feeds-service` actor for lifecycle mutations.
- `backend/services/feeds/tests/test_service.py` - Added lifecycle service actor propagation tests plus invalid UUID short-circuit coverage.
- `integration_tests/storage/test_feed_store_integration.py` - Updated direct lifecycle storage calls for the required actor signature.

## Decisions Made

- Used the existing `FeedStore` lifecycle methods as the only audited paths; no parallel lifecycle audit methods were added.
- Used the locked pre-delete snapshot as both `before_values` and identity source for `feed.deleted` before current-state deletion.
- Kept route contracts actor-field-free; the service layer supplies the Phase 2 fallback actor.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] Updated direct integration lifecycle call sites for required actor**
- **Found during:** Plan verification after Task 3.
- **Issue:** Changing `FeedStore.deactivate_feed`, `delete_feed`, and `reset_feed` to require keyword-only `actor_id` left direct storage integration tests calling those methods without the required actor.
- **Fix:** Passed `_TEST_ACTOR_ID = "service:feeds-service"` through direct lifecycle integration test calls.
- **Files modified:** `integration_tests/storage/test_feed_store_integration.py`
- **Verification:** `safe-run -- uv run python -m py_compile backend/pipeline/storage/feed_store.py backend/services/feeds/service.py integration_tests/storage/test_feed_store_integration.py`; call-site `rg` scan confirmed direct lifecycle storage calls now pass actors.
- **Committed in:** `6cd9ff12`

---

**Total deviations:** 1 auto-fixed (1 blocking)
**Impact on plan:** The fix was required by the planned storage signature change and keeps existing integration coverage aligned. No production scope was added.

## Issues Encountered

- Targeted service/API tests emit existing Starlette/httpx deprecation warnings from `fastapi.testclient`; no test failures remain.
- Local Docker/testcontainers integration tests were intentionally not run because AGENTS.md forbids proactive resource-heavy integration lanes.

## Authentication Gates

None.

## Known Stubs

None. Stub scan found only existing test fixtures, type defaults, nullable assignments, empty-list assertions, and intentional audit `{}` payloads for create/delete semantics.

## Verification

- RED Task 1: `safe-run -- uv run python -m pytest backend/pipeline/storage/tests/test_feed_store.py::TestDeactivateFeed backend/pipeline/storage/tests/test_feed_store.py::TestResetFeed -q` - failed as expected with missing `actor_id` lifecycle signatures.
- GREEN Task 1: same command - passed, 4 tests.
- RED Task 2: `safe-run -- uv run python -m pytest backend/pipeline/storage/tests/test_feed_store.py::TestDeleteFeed -q` - failed as expected with missing `actor_id` delete signature.
- GREEN Task 2: same command - passed, 3 tests.
- RED Task 3: `safe-run -- uv run python -m pytest backend/services/feeds/tests/test_service.py backend/services/feeds/tests/test_api.py -q` - failed as expected because lifecycle service calls did not pass `actor_id`.
- GREEN Task 3: same command - passed, 42 tests and 13 subtests, with existing Starlette/httpx deprecation warnings.
- Plan verification: `safe-run -- uv run python -m pytest backend/pipeline/storage/tests/test_feed_store.py::TestDeactivateFeed backend/pipeline/storage/tests/test_feed_store.py::TestDeleteFeed backend/pipeline/storage/tests/test_feed_store.py::TestResetFeed backend/services/feeds/tests/test_service.py backend/services/feeds/tests/test_api.py -q` - passed, 49 tests and 13 subtests, with existing warnings.
- Plan whitespace check: `git diff --check -- .planning/phases/02-transactional-storage-writes/02-03-PLAN.md backend/pipeline/storage/feed_store.py backend/pipeline/storage/tests/test_feed_store.py backend/services/feeds/service.py backend/services/feeds/tests/test_service.py integration_tests/storage/test_feed_store_integration.py` - passed.
- Deviation verification: `safe-run -- uv run python -m py_compile backend/pipeline/storage/feed_store.py backend/services/feeds/service.py integration_tests/storage/test_feed_store_integration.py` - passed.

## TDD Gate Compliance

Passed. RED `test(02-03)` commits exist for all three TDD tasks and corresponding GREEN `feat(02-03)` commits follow them.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness

Ready for `02-04-PLAN.md`. Lifecycle storage audit paths now share the Plan 02 helper stack, and Plan 04 can focus on rollback/concurrency integration coverage and final hardening.

## Self-Check: PASSED

- Created files exist: `02-03-SUMMARY.md`.
- Modified implementation/test files exist: `backend/pipeline/storage/feed_store.py`, `backend/pipeline/storage/tests/test_feed_store.py`, `backend/services/feeds/service.py`, `backend/services/feeds/tests/test_service.py`, and `integration_tests/storage/test_feed_store_integration.py`.
- Task and deviation commits found: `8be204b4`, `bd68ecf9`, `10e10d03`, `efb21405`, `8b33736c`, `7a75e464`, `6cd9ff12`.
- No tracked file deletions were introduced by task or deviation commits.

---
*Phase: 02-transactional-storage-writes*
*Completed: 2026-06-19*
