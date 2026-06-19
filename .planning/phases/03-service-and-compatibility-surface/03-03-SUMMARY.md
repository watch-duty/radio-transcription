# 03-03 Summary: Trusted Feed Actor Propagation

## Result

Completed. Admin feed mutations now preserve the authenticated Google admin
subject as `user:google:<sub>` through the BFF, feeds-service route boundary,
service layer, and storage mutation call.

## Changes

- Added BFF actor derivation from authenticated admin `request.user.sub`.
- Forwarded `X-WD-Actor-Id` on create, update, reset, deactivate, and delete
  feed calls only.
- Added feeds-service route validation for forwarded actor context.
- Required `TRUSTED_ACTOR_FORWARDING_SERVICE_ACCOUNTS` allowlist matching in
  GCP mode; local mode still requires a well-formed actor header.
- Removed the admin-route `service:feeds-service` fallback from `FeedService`.
- Made admin mutation service methods require keyword-only `actor_id`.
- Added tests for missing/invalid admin subjects, missing/malformed actor
  headers, untrusted/trusted GCP callers, and explicit service actor signatures.

## Commits

- `c4dd7503 feat(03-03): forward trusted admin actor from BFF`
- `451401cc feat(03-03): enforce trusted feed actor boundary`
- `8576c4ac test(03-03): keep actor boundary grep enforceable`

## Verification

- `safe-run -- yarn --cwd frontend/api test --run src/feeds/feedsController.test.ts` passed: 56 tests.
- `safe-run -- yarn --cwd frontend/api typecheck` passed.
- `safe-run -- uv run python -m pytest backend/services/feeds/tests/test_service.py backend/services/feeds/tests/test_api.py backend/pipeline/storage/tests/test_feed_store.py::TestFeedAuditStorageBoundary -q` passed: 53 tests, 28 subtests.
- `gsd-sdk query verify.key-links .planning/phases/03-service-and-compatibility-surface/03-03-PLAN.md` passed: 4/4 links.
- `rg -n "service:feeds-service|user:null|user: |system:" backend/services/feeds frontend/api/src/feeds` found no matches.
- `safe-run -- uv run ruff format --check backend/services/feeds/main.py backend/services/feeds/service.py backend/services/feeds/tests/test_api.py backend/services/feeds/tests/test_service.py backend/pipeline/storage/tests/test_feed_store.py` passed.
- `git diff --check -- frontend/api/src/feeds/feedsController.ts frontend/api/src/feeds/feedsController.test.ts backend/services/feeds/main.py backend/services/feeds/service.py backend/services/feeds/tests/test_api.py backend/services/feeds/tests/test_service.py backend/pipeline/storage/tests/test_feed_store.py .planning/phases/03-service-and-compatibility-surface/03-03-PLAN.md` passed.

## Notes

- Read-only feed routes remain unchanged and do not require actor context.
- Actor IDs remain out of request and response bodies; actor context is carried
  only through the trusted internal header and service keyword argument.
