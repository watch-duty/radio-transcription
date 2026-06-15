---
phase: 04-strict-policy-table-and-status-vocabulary
verified: 2026-06-15T16:59:02Z
status: passed
score: "10/10 must-haves verified"
overrides_applied: 0
---

# Phase 04 Verification

Phase 04 is verified against the backend-only scope: strict policy table routing, backend status vocabulary splits, owner mapping, and focused regression coverage. Frontend/OpenAPI/generated compatibility remains an explicit Phase 6 follow-up.

## Observable Truths

1. `backend/pipeline/ingestion/failure_policy.py` now uses `_FailurePolicyRule` and `_POLICY_RULES` as the explicit status/evidence routing table.
2. Unsupported status/evidence combinations return telemetry-gap decisions instead of falling through broad owner-scope defaults.
3. No `reason_family` field was added; routing remains based on status reason plus structured evidence.
4. `pipeline_publish_after_bookmark_failed` with Pub/Sub publish-stage evidence is routed as budgeted feed quarantine for v1.
5. `system_pipeline_error` with GCS/bookmark pipeline evidence remains non-budgeted suppress-retry.
6. Backend `FeedStatusReason` includes `system_runtime_configuration_invalid`, `system_credential_access_failed`, and `system_source_payload_invalid`.
7. `owner_scope_for_status_reason(...)` maps the new statuses through `_STATUS_OWNER_SCOPES`.
8. Split status policy rows are covered for matching evidence and mismatch telemetry-gap behavior.
9. Promoted item-scope source/auth evidence is explicitly routed and no longer falls through to telemetry gap.
10. OpenAPI, generated metadata, shared frontend types, and UI labels were not changed in Phase 4.

## Artifact Coverage

| Artifact | Verification |
| --- | --- |
| `backend/pipeline/ingestion/failure_policy.py` | Explicit rule table, split-status rows, item-scope preservation |
| `backend/pipeline/ingestion/tests/test_failure_policy.py` | Current status routes, split routes, mismatch routes, promoted item evidence |
| `backend/pipeline/storage/feed_store.py` | Backend enum values added |
| `backend/pipeline/storage/tests/test_feed_store.py` | Canonical backend status reason coverage |
| `backend/pipeline/ingestion/collectors/failure_classification.py` | Owner-scope mapping table |
| `backend/pipeline/ingestion/collectors/tests/test_failure_classification.py` | Owner-scope assertions for split statuses |
| `04-REVIEW.md` | Clean review report with review-found issue resolved |

## GSD Checks

| Check | Result |
| --- | --- |
| `gsd-sdk query phase-plan-index 04` | Both plans have summaries; no incomplete plans |
| `gsd-sdk query verify.key-links .../04-01-PLAN.md` | All key links verified |
| `gsd-sdk query verify.key-links .../04-02-PLAN.md` | All key links verified |
| `gsd-sdk query verify.schema-drift 04` | `drift_detected:false`, `blocking:false` |

## Command Verification

```bash
safe-run -- uv run python -m pytest backend/pipeline/ingestion/tests/test_failure_policy.py backend/pipeline/ingestion/collectors/tests/test_failure_classification.py backend/pipeline/storage/tests/test_feed_store.py::TestFeedStatusReason::test_canonical_reason_values -q -n 0
```

Result: `20 passed, 23 subtests passed in 0.04s`.

```bash
safe-run -- uv run python -m pytest backend/pipeline/storage/tests/test_feed_store.py::TestNonBudgetedFailureSql backend/pipeline/storage/tests/test_feed_store.py::TestReleaseNonBudgetedFailure -q -n 0
```

Result: `6 passed in 0.04s`.

```bash
safe-run -- uv run ruff check backend/pipeline/ingestion/failure_policy.py backend/pipeline/ingestion/tests/test_failure_policy.py backend/pipeline/ingestion/collectors/failure_classification.py backend/pipeline/ingestion/collectors/tests/test_failure_classification.py backend/pipeline/storage/feed_store.py backend/pipeline/storage/tests/test_feed_store.py
```

Result: `All checks passed!`.

## Requirements Coverage

- `POL-11`, `POL-12`, `POL-13`, `POL-14`: satisfied by the fail-closed policy table and tests.
- `STAT-11`, `STAT-12`, `STAT-13`, `STAT-14`: satisfied by backend enum, owner mapping, and policy rows for split statuses.
- `TEST-11`, `TEST-12`: satisfied by RED/GREEN tests plus review-found item-scope regression coverage.

## Deferred Surfaces

- Full `TestFeedStatusReason` OpenAPI parity remains deferred to Phase 6.
- Runtime `_PipelineFailure` execution changes remain deferred to Phase 5.
- No database migration or schema drift was required in Phase 4.
