---
phase: 04-runtime-event-integration
plan: "01"
subsystem: storage
tags: [feed-audit-events, runtime-events, asyncpg, pytest]

requires:
  - phase: 02-transactional-storage-writes
    provides: storage-owned feed audit insert primitives
  - phase: 03-service-and-compatibility-surface
    provides: canonical status_reason_detail compatibility surface
provides:
  - Async FeedStore runtime audit gates for failure, quarantine, and recovery
  - Previous-status lease carrier for runtime failure/recovery decisions
  - Bounded and redacted status_reason_detail persistence helper
affects: [04-runtime-event-integration, collector-runtime, echo-sync-parity]

tech-stack:
  added: []
  patterns:
    - Storage-owned runtime audit event selection
    - Prior logical state carried separately from maintained feed snapshots
    - Persistence-boundary diagnostic redaction before durable storage

key-files:
  created:
    - .planning/phases/04-runtime-event-integration/04-01-SUMMARY.md
  modified:
    - backend/pipeline/storage/feed_lifecycle.py
    - backend/pipeline/storage/feed_queries.py
    - backend/pipeline/storage/feed_store.py
    - backend/pipeline/storage/tests/test_feed_lifecycle.py
    - backend/pipeline/storage/tests/test_feed_query_contracts.py
    - backend/pipeline/storage/tests/test_feed_store.py

key-decisions:
  - "Runtime audit event actions are selected in FeedStore from caller-supplied prior logical state plus storage-maintained after snapshots."
  - "status_reason_detail and the compatibility quarantine_reason mirror use the same bounded redaction helper before persistence."
  - "Runtime failure methods require explicit actor_id and prior-state inputs; recovery-capable success methods accept optional actor/prior-state inputs."

patterns-established:
  - "Runtime prior state belongs in audit metadata; before_values and after_values remain maintained feed-row snapshots."
  - "Failure/quarantine events are suppressed unless the logical (status, status_reason) outcome changes."
  - "Recovery events require prior failing or quarantined status and a successful write that clears failure state."

requirements-completed: [AUD-01, EVT-06, EVT-07, EVT-08, EVT-09, DIAG-02, DIAG-03, ACT-03]

duration: 15 min
completed: 2026-06-19
---

# Phase 04 Plan 01: Shared Async Storage Runtime Audit Primitives Summary

**Async FeedStore runtime audit gates with prior-status leasing, sanitized diagnostic detail, and transactional failure/quarantine/recovery events**

## Performance

- **Duration:** 15 min
- **Started:** 2026-06-19T23:41:48Z
- **Completed:** 2026-06-19T23:56:49Z
- **Tasks:** 3
- **Files modified:** 6

## Accomplishments

- Added `previous_status` to generated primary and recovery lease SQL plus the `LeasedFeed` mapper.
- Added bounded, whitespace-normalizing, credential-redacting diagnostic detail persistence.
- Added storage-owned async runtime audit gates for `feed.failure_reported`, `feed.quarantined`, and `feed.recovered`.
- Kept runtime handlers out of audit row construction; callers pass actor/prior state and storage owns snapshots, sequence allocation, and inserts.

## Task Commits

1. **Task 1: Carry claim-time previous_status into LeasedFeed** - `b4ea7b0b` (feat)
2. **Task 2: Add bounded canonical status_reason_detail persistence** - `c71798f2` (feat)
3. **Task 3: Add async storage event gates for failure, quarantine, and recovery** - `eb3602ec` (feat)

## Files Created/Modified

- `backend/pipeline/storage/feed_lifecycle.py` - Added canonical diagnostic detail sanitizer and shared compatibility mirror behavior.
- `backend/pipeline/storage/feed_queries.py` - Added previous-status claim projection and canonical detail writes for async abnormal state SQL.
- `backend/pipeline/storage/feed_store.py` - Added runtime action selection, transactional runtime audit inserts, metadata support, and explicit runtime actor/prior-state inputs.
- `backend/pipeline/storage/tests/test_feed_lifecycle.py` - Covered detail normalization, redaction, and capping.
- `backend/pipeline/storage/tests/test_feed_query_contracts.py` - Covered previous-status claim SQL and async abnormal detail SQL contracts.
- `backend/pipeline/storage/tests/test_feed_store.py` - Covered runtime event/no-event behavior, explicit actor signatures, and transactional audit arguments.

## Verification

- `safe-run -- python3 -m pytest backend/pipeline/storage/tests/test_feed_query_contracts.py::TestBuildAcquireFeedsBatchSql backend/pipeline/storage/tests/test_feed_query_contracts.py::TestBuildAcquireFeedsRecoverySql backend/pipeline/storage/tests/test_feed_store.py::TestRowToLeasedFeed -q` - Passed, 17 tests.
- `safe-run -- python3 -m pytest backend/pipeline/storage/tests/test_feed_lifecycle.py backend/pipeline/storage/tests/test_feed_query_contracts.py::TestAsyncSyncFailureSqlContracts -q` - Passed, 14 tests.
- `safe-run -- python3 -m pytest backend/pipeline/storage/tests/test_feed_store.py::TestFeedRuntimeAuditEvents backend/pipeline/storage/tests/test_feed_store.py::TestReportFeedFailure backend/pipeline/storage/tests/test_feed_store.py::TestReleaseNonBudgetedFailure backend/pipeline/storage/tests/test_feed_store.py::TestUpdateFeedProgress backend/pipeline/storage/tests/test_feed_store.py::TestRecordSourceObservation -q` - Passed, 26 tests.
- `safe-run -- python3 -m pytest backend/pipeline/storage/tests/test_feed_lifecycle.py backend/pipeline/storage/tests/test_feed_query_contracts.py backend/pipeline/storage/tests/test_feed_store.py -q` - Passed, 188 tests.
- `git diff --check` - Passed.

## Decisions Made

- Runtime failure/quarantine action selection compares prior logical state from leasing against the storage-maintained after snapshot.
- Runtime recovery requires caller-supplied prior abnormal status; clearing detail while the prior status was normal remains no-event maintenance.
- Legacy `quarantine_reason` is sanitized with the same helper as `status_reason_detail` while it remains a compatibility mirror.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 2 - Missing Critical] Sanitized the compatibility quarantine_reason mirror**
- **Found during:** Task 2 (diagnostic detail persistence)
- **Issue:** Sanitizing only `status_reason_detail` would still allow the same diagnostic text to persist raw in the compatibility `quarantine_reason` field on threshold quarantine.
- **Fix:** Routed `quarantine_reason_storage_value()` through `status_reason_detail_storage_value()` so both persisted diagnostic fields are bounded and redacted.
- **Files modified:** `backend/pipeline/storage/feed_lifecycle.py`, `backend/pipeline/storage/tests/test_feed_lifecycle.py`
- **Verification:** Lifecycle sanitizer tests and Task 2 focused tests passed.
- **Committed in:** `c71798f2`

**2. [Rule 1 - Bug] Fixed authorization bearer token redaction edge case**
- **Found during:** Task 2 verification
- **Issue:** The first sanitizer regex redacted `Authorization: Bearer` but left the bearer token suffix in persisted detail.
- **Fix:** Expanded the credential-value regex so authorization header bearer values are consumed and redacted as one value.
- **Files modified:** `backend/pipeline/storage/feed_lifecycle.py`
- **Verification:** `test_status_reason_detail_storage_value_redacts_credentials` failed before the fix and passed after it.
- **Committed in:** `c71798f2`

---

**Total deviations:** 2 auto-fixed (1 missing critical, 1 bug)
**Impact on plan:** Both fixes tightened the planned security boundary without changing the storage-owned audit architecture.

## Issues Encountered

- Pytest emitted existing config warnings for `asyncio_default_fixture_loop_scope` and `asyncio_mode` under the local Python 3.12 pytest environment. Tests passed; no plan work was blocked.

## Known Stubs

None. Stub scan found only pre-existing TODO comments and intentional test/setup `None` or empty snapshot values.

## User Setup Required

None - no external service configuration required.

## Threat Flags

None. The new audit writes and diagnostic persistence changes are the exact threat surfaces covered by the plan threat model.

## Next Phase Readiness

Ready for `04-02-PLAN.md`: collector runtime can now pass explicit service actor and leased prior state into async storage without constructing audit rows itself.

## Self-Check: PASSED

- Summary file exists.
- Key modified files exist.
- Task commits found: `b4ea7b0b`, `c71798f2`, `eb3602ec`.

---
*Phase: 04-runtime-event-integration*
*Completed: 2026-06-19*
