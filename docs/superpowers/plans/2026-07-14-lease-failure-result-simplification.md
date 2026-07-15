# Lease Failure Result Simplification Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> superpowers:executing-plans to implement this plan task-by-task. Steps use
> checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the redundant `LeaseFailureEffect` vocabulary with the
existing typed final Lease status while preserving exact-grant diagnostics.

**Architecture:** `LeaseOperationDisposition` continues to report whether the
fenced mutation applied. `LeaseFailureResult.final_status` reports only the
`FAILING` or `QUARANTINED` status produced by an applied failure; rejected
operations return `None`.

**Tech Stack:** Python 3.13, asyncpg, PostgreSQL 15/16/17, pytest, Ruff, ty.

## Global Constraints

- Do not change the two failure SQL mutations or their returned columns.
- Do not expose Lease heartbeat, retry, revision, or before/after snapshots.
- Do not add runtime, child Feed, schema, audit, deployment, or cutover work.
- Keep failure persistence one-shot with no internal database retry.

---

### Task 1: Return Typed Final Status

**Files:**
- Modify: `backend/pipeline/storage/ingestion_lease_store.py`
- Modify: `backend/pipeline/storage/tests/test_ingestion_lease_store.py`
- Modify: `backend/pipeline/storage/tests/test_ingestion_lease_queries.py`
- Modify: `integration_tests/storage/test_ingestion_lease_store_integration.py`
- Modify: `vulture_manual_whitelist.py`

**Interfaces:**
- Consumes: existing `LeaseOperationDisposition` and `feed_store.FeedStatus`.
- Produces:

```python
@dataclasses.dataclass(frozen=True, slots=True)
class LeaseFailureResult:
    disposition: LeaseOperationDisposition
    final_status: feed_store.FeedStatus | None
```

- [x] **Step 1: Change focused tests to specify the new contract**

Replace effect assertions with typed status assertions and add constructor
cases proving the closed invariant:

```python
LeaseFailureResult(LeaseOperationDisposition.APPLIED, FeedStatus.FAILING)
LeaseFailureResult(LeaseOperationDisposition.APPLIED, FeedStatus.QUARANTINED)
LeaseFailureResult(LeaseOperationDisposition.FENCE_MISMATCH, None)
```

Reject `APPLIED` with `None`, a rejection with a status, and applied statuses
other than `FAILING` or `QUARANTINED`.

- [x] **Step 2: Run the changed unit tests and observe the expected failure**

Run:

```bash
safe-run -- python3 -m pytest \
  backend/pipeline/storage/tests/test_ingestion_lease_queries.py \
  backend/pipeline/storage/tests/test_ingestion_lease_store.py -q
```

Expected: failure because `LeaseFailureResult` still exposes `effect` and
`LeaseFailureEffect` still exists.

- [x] **Step 3: Implement the narrow result**

Delete `LeaseFailureEffect`. Validate the result with:

```python
applied = self.disposition is LeaseOperationDisposition.APPLIED
valid_status = self.final_status in (
    feed_store.FeedStatus.FAILING,
    feed_store.FeedStatus.QUARANTINED,
)
if applied != valid_status:
    raise ValueError(
        "only an applied failure may return failing or quarantined status"
    )
```

Convert SQL `final_status` directly through `feed_store.FeedStatus`, return
`None` for every rejected disposition, and log `final_status` instead of
`failure_effect`.

- [x] **Step 4: Update integration and dead-code contracts**

Assert durable failure results using `FeedStatus.FAILING` and
`FeedStatus.QUARANTINED`; remove every whitelist/reference to
`LeaseFailureEffect`. Preserve all existing SQL mutation assertions.

- [x] **Step 5: Run focused and static verification**

Run:

```bash
safe-run -- python3 -m pytest \
  backend/pipeline/storage/tests/test_ingestion_lease_queries.py \
  backend/pipeline/storage/tests/test_ingestion_lease_store.py \
  integration_tests/storage/test_ingestion_lease_store_integration.py -q
safe-run -- uv run ruff check \
  backend/pipeline/storage/ingestion_lease_store.py \
  backend/pipeline/storage/tests/test_ingestion_lease_queries.py \
  backend/pipeline/storage/tests/test_ingestion_lease_store.py \
  integration_tests/storage/test_ingestion_lease_store_integration.py \
  vulture_manual_whitelist.py
safe-run -- uv run ruff format --check \
  backend/pipeline/storage/ingestion_lease_store.py \
  backend/pipeline/storage/tests/test_ingestion_lease_queries.py \
  backend/pipeline/storage/tests/test_ingestion_lease_store.py \
  integration_tests/storage/test_ingestion_lease_store_integration.py \
  vulture_manual_whitelist.py
```

Expected: focused tests pass, external PostgreSQL cases may skip without a DSN,
and both Ruff commands pass.

- [ ] **Step 6: Commit, update the PR description, and push**

```bash
git add \
  backend/pipeline/storage/ingestion_lease_store.py \
  backend/pipeline/storage/tests/test_ingestion_lease_queries.py \
  backend/pipeline/storage/tests/test_ingestion_lease_store.py \
  integration_tests/storage/test_ingestion_lease_store_integration.py \
  vulture_manual_whitelist.py \
  docs/superpowers/plans/2026-07-14-lease-failure-result-simplification.md
git commit -m "[GOO-774] Simplify Lease failure result"
git push origin agent/fenced-sid-lease-failure-finalization
```

Update #1007 to describe `disposition + final_status`, then confirm required CI
starts for the new head commit.
