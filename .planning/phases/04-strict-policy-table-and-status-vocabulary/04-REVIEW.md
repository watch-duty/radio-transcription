---
phase: 04-strict-policy-table-and-status-vocabulary
status: clean
depth: standard
files_reviewed: 6
findings:
  critical: 0
  warning: 0
  info: 0
  total: 0
reviewed_at: 2026-06-15T16:53:44Z
---

# Phase 04 Code Review

Reviewed backend source and test changes from Phase 4:

- `backend/pipeline/ingestion/failure_policy.py`
- `backend/pipeline/ingestion/tests/test_failure_policy.py`
- `backend/pipeline/ingestion/collectors/failure_classification.py`
- `backend/pipeline/ingestion/collectors/tests/test_failure_classification.py`
- `backend/pipeline/storage/feed_store.py`
- `backend/pipeline/storage/tests/test_feed_store.py`

## Result

No open findings.

## Resolved During Review

### R-04-01: Promoted item-scope evidence fell through to telemetry gap

Existing collectors can promote all-items-failed source/auth item failures with
`FailureScope.ITEM`. The initial Phase 4 table rows allowed observation/feed/class
scopes but not item scope, so promoted item evidence would have fallen through to
telemetry gap.

Resolution:

- Added `test_promoted_item_scope_source_and_auth_routes_are_explicit`.
- Included `FailureScope.ITEM` in the explicit source-class and credential-scope
  rule scopes.
- Verified with the combined focused backend test command.

Commits:

- `cf83fba0` - test coverage
- `11791cc9` - policy fix

## Verification

```bash
safe-run -- uv run python -m pytest backend/pipeline/ingestion/tests/test_failure_policy.py backend/pipeline/ingestion/collectors/tests/test_failure_classification.py backend/pipeline/storage/tests/test_feed_store.py::TestFeedStatusReason::test_canonical_reason_values -q -n 0
```

Result: `20 passed, 23 subtests passed in 0.04s`.
