---
phase: 03-verification-and-compatibility
plan: 02
subsystem: frontend-compatibility
tags: [openapi, typescript, status-reasons, quarantine-policy]

requires:
  - phase: 01-policy-and-storage-foundation
    provides: Backend FeedStatusReason vocabulary including pipeline publish-after-bookmark failures.
  - phase: 02-runtime-routing-and-telemetry
    provides: Runtime use of pipeline_publish_after_bookmark_failed for non-budgeted publish gaps.
provides:
  - OpenAPI BackendFeedStatusReason compatibility for pipeline_publish_after_bookmark_failed.
  - Shared TypeScript status reason compatibility and runtime allowlist coverage.
  - Operator UI display text for the tolerated pipeline status reason.
affects: [verification, frontend, openapi, quarantine-policy]

tech-stack:
  added: []
  patterns:
    - Backend status reason order is mirrored across OpenAPI and shared/frontend status maps with unknown as the compatibility sentinel.
    - Lifecycle status mapping remains separate from status reason display mapping.

key-files:
  created:
    - .planning/phases/03-verification-and-compatibility/03-02-SUMMARY.md
  modified:
    - frontend/api/openapi.yaml
    - frontend/common/src/types/feeds.ts
    - frontend/common/src/utils/statusUtils.ts
    - frontend/transcription-ui/src/components/common/FeedStatusIndicator.tsx

key-decisions:
  - "No feed lifecycle status was added; pipeline_publish_after_bookmark_failed remains a status reason only."
  - "Frontend status tests remained untouched because the compatibility edit did not break existing behavior."

patterns-established:
  - "Compatibility surfaces list the pipeline publish-after-bookmark reason consistently without introducing a UI lifecycle state."
  - "Frontend verification can require local Yarn/Corepack setup plus generated API routes before typecheck."

requirements-completed: [STAT-02]

duration: 5min
completed: 2026-06-15
---

# Phase 03 Plan 02: Status Compatibility Surfaces Summary

**OpenAPI, shared TypeScript, and operator status display surfaces tolerate `pipeline_publish_after_bookmark_failed` while preserving existing `error` lifecycle semantics.**

## Performance

- **Duration:** 5 min
- **Started:** 2026-06-15T04:53:02Z
- **Completed:** 2026-06-15T04:57:37Z
- **Tasks:** 2
- **Files modified:** 4

## Accomplishments

- Aligned `BackendFeedStatusReason` ordering in OpenAPI with the backend status reason vocabulary plus `unknown`.
- Aligned shared TypeScript status reason union, conversion allowlist, and UI display mapping for `pipeline_publish_after_bookmark_failed`.
- Confirmed `failing` and `quarantined` still map to UI `error` and no frontend test was changed.

## Task Commits

Each task was committed atomically:

1. **Task 1: Align OpenAPI status reason parity** - `2ec08972` (chore)
2. **Task 2: Preserve shared TypeScript and UI lifecycle compatibility** - `05ae2353` (chore)

**Plan metadata:** committed separately with SUMMARY, STATE, ROADMAP, and REQUIREMENTS updates.

## Files Created/Modified

- `frontend/api/openapi.yaml` - Lists `pipeline_publish_after_bookmark_failed` in `BackendFeedStatusReason` while leaving lifecycle `FeedStatus` unchanged.
- `frontend/common/src/types/feeds.ts` - Keeps `BackendFeedStatusReason` compatible with the backend status reason value.
- `frontend/common/src/utils/statusUtils.ts` - Keeps the runtime status reason allowlist accepting the pipeline publish-after-bookmark reason and preserves `failing`/`quarantined -> error`.
- `frontend/transcription-ui/src/components/common/FeedStatusIndicator.tsx` - Keeps the operator-facing display text `Pipeline Publish Failed After Bookmark`.
- `.planning/phases/03-verification-and-compatibility/03-02-SUMMARY.md` - Captures plan evidence and verification results.

## Requirement Evidence

| Requirement | Proof |
|-------------|-------|
| STAT-02 | `frontend/api/openapi.yaml`, `frontend/common/src/types/feeds.ts`, `frontend/common/src/utils/statusUtils.ts`, and `FeedStatusIndicator.tsx` all include `pipeline_publish_after_bookmark_failed`; lifecycle status enums remain unchanged; `convertFeedStatusBackend('failing')` and `convertFeedStatusBackend('quarantined')` still return `error`. |

## Decisions Made

- No lifecycle status was introduced; the new value remains a backend status reason used for compatibility and display.
- No frontend test was added or modified because existing UI behavior did not break.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] Restored local Yarn verification tooling**
- **Found during:** Task 2 verification
- **Issue:** `safe-run -- yarn --cwd frontend/common build` initially failed because `yarn` was not on PATH.
- **Fix:** Enabled Yarn v1 through Corepack and installed dependencies for the three targeted frontend packages with `--frozen-lockfile`.
- **Files modified:** Ignored local `node_modules/` directories only; no tracked source files.
- **Verification:** All three frontend compile/typecheck commands passed.
- **Committed in:** Not committed; local verification environment setup only.

**2. [Rule 3 - Blocking] Generated local API routes required by typecheck**
- **Found during:** Task 2 verification
- **Issue:** `frontend/api typecheck` failed because `src/generated/routes.js` was missing in the local ignored generated-output directory.
- **Fix:** Ran the package-local `generate-routes` script before rerunning typecheck.
- **Files modified:** Ignored `frontend/api/src/generated/` output only; no tracked source files.
- **Verification:** `safe-run -- yarn --cwd frontend/api typecheck` passed.
- **Committed in:** Not committed; local generated verification prerequisite only.

**Total deviations:** 2 auto-fixed (2 blocking).
**Impact on plan:** Both fixes were local verification prerequisites. No scope or tracked source behavior changed beyond the planned compatibility surfaces.

## Issues Encountered

- The TDD-labeled compatibility checks were already green before edits. I preserved existing tests and made semantic-preserving compatibility alignment changes rather than adding pipeline-specific frontend tests.

## Verification

```bash
safe-run -- uv run python -m pytest backend/pipeline/storage/tests/test_feed_store.py::TestFeedStatusReason -q -n 0
```

Result: `3 passed in 0.08s`.

```bash
safe-run -- yarn --cwd frontend/common build
```

Result: passed.

```bash
safe-run -- yarn --cwd frontend/api typecheck
```

Result: passed.

```bash
safe-run -- yarn --cwd frontend/transcription-ui typecheck
```

Result: passed.

```bash
git diff --check
```

Result: passed.

## Known Stubs

None. Stub-pattern scan only found a normal local array accumulator in `FeedStatusIndicator.tsx`, not a UI or data-source stub.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness

Plan 03-03 can run the final narrow verification and prepare the implementation summary with `STAT-02` complete. No broad frontend E2E, component suite, API local stack, Docker, or integration test was run.

## Self-Check: PASSED

- Found summary file: `.planning/phases/03-verification-and-compatibility/03-02-SUMMARY.md`
- Found task commits `2ec08972` and `05ae2353` in git history.
- `git diff --check` passed.

---
*Phase: 03-verification-and-compatibility*
*Completed: 2026-06-15*
