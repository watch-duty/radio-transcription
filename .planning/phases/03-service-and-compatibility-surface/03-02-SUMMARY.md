# 03-02 Summary: BFF and Frontend Diagnostic Contract

## Result

Completed. The TypeScript public feed contract now uses `statusReasonDetail`
instead of the legacy `quarantineReason` alias.

## Changes

- Updated the shared `Feed` type in `frontend/common` to expose
  `statusReasonDetail`.
- Updated the BFF feed backend DTO and converter to map
  `status_reason_detail` to `statusReasonDetail`.
- Updated the status indicator and all existing feed status call sites to pass
  and display `statusReasonDetail` with the same tooltip behavior as before.
- Regenerated `frontend/api/openapi.yaml` so the public schema exposes
  `statusReasonDetail`.
- Added regression coverage for BFF conversion and OpenAPI field naming.

## Commits

- `0d2156f7 feat(03-02): map canonical feed detail in BFF`
- `dadf3e86 feat(03-02): display canonical feed status detail`
- `4ead7b4c test(03-02): verify feed detail OpenAPI contract`

## Verification

- `safe-run -- yarn --cwd frontend/common build` passed.
- `safe-run -- yarn --cwd frontend/api test --run src/feeds/feedsController.test.ts` passed: 51 tests.
- `safe-run -- yarn --cwd frontend/api verify-spec` passed.
- `safe-run -- yarn --cwd frontend/api typecheck` passed.
- `safe-run -- yarn --cwd frontend/transcription-ui test --run src/components/common/FeedStatusIndicator.test.tsx src/components/feeds/FeedSearchView.test.tsx` passed: 11 tests.
- `safe-run -- yarn --cwd frontend/transcription-ui typecheck` passed.
- `gsd-sdk query verify.key-links .planning/phases/03-service-and-compatibility-surface/03-02-PLAN.md` passed: 3/3 links.
- `rg -n "quarantineReason|quarantine_reason" frontend/common frontend/api/src/feeds frontend/api/openapi.yaml frontend/transcription-ui/src` found no matches.
- `git diff --check -- frontend/common frontend/api frontend/transcription-ui` passed.

## Notes

- Frontend package dependencies were installed from existing Yarn lockfiles in
  the worktree to enable local verification; no lockfile or dependency source
  changes were made.
