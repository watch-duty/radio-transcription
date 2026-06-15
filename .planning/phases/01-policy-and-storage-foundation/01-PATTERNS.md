# Phase 1: Policy And Storage Foundation - Pattern Map

**Mapped:** 2026-06-15
**Files analyzed:** 10
**Analogs found:** 10 / 10

## File Classification

| New/Modified File | Role | Data Flow | Closest Analog | Match Quality |
|-------------------|------|-----------|----------------|---------------|
| `backend/pipeline/ingestion/failure_policy.py` | model / utility | transform | `backend/pipeline/ingestion/collectors/failure_classification.py` + `backend/pipeline/ingestion/models.py` | role-match |
| `backend/pipeline/ingestion/models.py` | model | event-driven boundary | `backend/pipeline/ingestion/models.py` | exact |
| `backend/pipeline/ingestion/collectors/failure_classification.py` | utility | transform | `backend/pipeline/ingestion/collectors/failure_classification.py` | exact |
| `backend/pipeline/ingestion/collector_runtime.py` | service / runtime | event-driven | `backend/pipeline/ingestion/collector_runtime.py` | exact, limited import/boundary reconciliation |
| `backend/pipeline/storage/feed_store.py` | service | CRUD | `backend/pipeline/storage/feed_store.py` | exact |
| `backend/pipeline/storage/feed_queries.py` | utility / config | CRUD | `backend/pipeline/storage/feed_queries.py` | exact |
| `backend/pipeline/storage/tests/test_feed_store.py` | test | CRUD | `backend/pipeline/storage/tests/test_feed_store.py` | exact |
| `backend/pipeline/ingestion/tests/test_failure_policy.py` | test | transform | `backend/pipeline/ingestion/collectors/tests/test_failure_classification.py` | role-match |
| `backend/pipeline/ingestion/tests/test_collector_runtime.py` | test | event-driven | `backend/pipeline/ingestion/tests/test_collector_runtime.py` | role-match |
| `backend/pipeline/ingestion/collectors/README.md` | documentation | event-driven guidance | `backend/pipeline/ingestion/collectors/README.md` | exact |

## Pattern Assignments

### `backend/pipeline/ingestion/failure_policy.py` (model / utility, transform)

**Analog:** `backend/pipeline/ingestion/collectors/failure_classification.py`

**Supporting analog:** `backend/pipeline/ingestion/models.py`

**Imports pattern** (`failure_classification.py` lines 9-22):

```python
from __future__ import annotations

import dataclasses

from backend.pipeline.ingestion.models import (
    EndpointKind,
    ExecutedAction,
    FailurePolicyEvidence,
    FailureScope,
    FeedFailure,
    OwnerScope,
    PolicyIntent,
)
from backend.pipeline.storage.feed_store import FeedStatusReason
```

For `failure_policy.py`, keep the same absolute-import style, but import only
what the pure policy module needs. `FeedFailure` should stay in `models.py`.

**Enum/value-object pattern** (`models.py` lines 119-196):

```python
class OwnerScope(StrEnum):
    """Operational owner scope for a failure policy decision."""

    FEED = "feed"
    CREDENTIAL_SCOPE = "credential_scope"
    SOURCE_CLASS = "source_class"
    PIPELINE = "pipeline"
    UNKNOWN = "unknown"


@dataclasses.dataclass(frozen=True)
class FailurePolicyEvidence:
    """Machine-readable routing evidence for a typed failure."""

    owner_scope: OwnerScope
    failure_scope: FailureScope
    endpoint_kind: EndpointKind
    policy_intent: PolicyIntent
    executed_action: ExecutedAction
    pipeline_stage: PipelineStage | None = None
```

Copy the `StrEnum` and frozen dataclass style, but do **not** copy the current
field ownership exactly. Phase decision D-04 requires `FailurePolicyEvidence`
to contain facts only; move `policy_intent` and `executed_action` to a new
`FailurePolicyDecision`.

**Pure transform pattern** (`failure_classification.py` lines 96-121):

```python
def promoted_failure(self) -> ItemFailure | None:
    """Promote all-items-failed observations to a feed-level failure.

    This avoids blaming a feed for isolated object races or corrupt files.
    Mixed canonical reasons are treated as system_collector_error because
    the collector no longer has a single reliable source/system owner to
    report.
    """
    if self._attempted_count <= 0:
        return None
    if self._chunk_produced:
        return None
    if len(self._failures) != self._attempted_count:
        return None
```

Use the same side-effect-free style for `classify_failure_policy(...)` and
predicate helpers: return a decision object, do not write storage, log, emit
telemetry, or parse `quarantine_reason`.

**Planning note:** There is no exact existing analog for
`FailurePolicyDecision`; implement it as the sibling to `FailurePolicyEvidence`
using the frozen dataclass pattern above.

---

### `backend/pipeline/ingestion/models.py` (model, event-driven boundary)

**Analog:** `backend/pipeline/ingestion/models.py`

**Imports pattern** (lines 71-88):

```python
from __future__ import annotations

import dataclasses
from typing import TYPE_CHECKING

import aiohttp  # noqa: TC002 - runtime use: CaptureResources holds aiohttp.ClientSession

from backend.pipeline.storage.feed_store import FeedStatusReason
```

When moving policy vocabulary out, preserve the full-path local import style:
`models.py` should import `FailurePolicyEvidence` from
`backend.pipeline.ingestion.failure_policy`.

**Boundary exception pattern** (lines 199-238):

```python
@dataclasses.dataclass(init=False, eq=False)
class FeedFailure(Exception):
    """Feed-level collector failure classified at the collector boundary."""

    status_reason: FeedStatusReason
    reason: str
    policy_evidence: FailurePolicyEvidence | None

    def __init__(
        self,
        status_reason: FeedStatusReason | str,
        reason: str,
        *,
        policy_evidence: FailurePolicyEvidence | None = None,
    ) -> None:
        """Normalize collector-provided values before the runtime sees them."""
        try:
            normalized_status_reason = FeedStatusReason(status_reason)
        except (TypeError, ValueError) as e:
            msg = f"Unknown feed status reason: {status_reason!r}"
            raise ValueError(msg) from e

        if not isinstance(reason, str) or not reason:
            msg = "FeedFailure.reason must be a non-empty string"
            raise ValueError(msg)

        self.status_reason = normalized_status_reason
        self.reason = reason[:200]
        self.policy_evidence = policy_evidence
        Exception.__init__(self, self.reason)
```

Keep this validation and exception-mutability pattern. Change the constructor
contract for Phase 1 so typed `FeedFailure` requires `policy_evidence`; reserve
runtime-synthesized unknown evidence for untyped exceptions, not for typed
collector failures.

**Test analog** (`test_collector_runtime.py` lines 84-133):

```python
class TestFeedFailureContract(unittest.TestCase):
    """Tests for the typed collector failure boundary contract."""

    def test_carries_status_reason_and_reason(self) -> None:
        """FeedFailure exposes canonical and raw failure data."""
        exc = FeedFailure(
            FeedStatusReason.SOURCE_OFFLINE,
            "source_offline",
        )

        self.assertIs(exc.status_reason, FeedStatusReason.SOURCE_OFFLINE)
        self.assertEqual(exc.reason, "source_offline")
        self.assertEqual(str(exc), "source_offline")
```

Update these tests rather than inventing a new style. Existing tests currently
allow optional evidence; Phase 1 should invert that expectation.

---

### `backend/pipeline/ingestion/collectors/failure_classification.py` (utility, transform)

**Analog:** `backend/pipeline/ingestion/collectors/failure_classification.py`

**Core helper pattern** (lines 124-135):

```python
def collector_failure(
    status_reason: FeedStatusReason,
    reason: str,
    *,
    policy_evidence: FailurePolicyEvidence | None = None,
) -> FeedFailure:
    """Build a typed feed-level collector failure."""
    return FeedFailure(
        status_reason=status_reason,
        reason=reason,
        policy_evidence=policy_evidence,
    )
```

For Phase 1, keep the helper but make evidence strict: callers should pass
`FailurePolicyEvidence`, and the helper should not provide an intentional
`None` path.

**Feed-actionable construction pattern** (lines 138-150):

```python
def missing_source_feed_id_failure() -> FeedFailure:
    """Build the typed failure for feeds missing source-specific ids."""
    return collector_failure(
        FeedStatusReason.SYSTEM_CONFIGURATION_INVALID,
        MISSING_SOURCE_FEED_ID_REASON,
        policy_evidence=FailurePolicyEvidence(
            owner_scope=OwnerScope.FEED,
            failure_scope=FailureScope.FEED,
            endpoint_kind=EndpointKind.FEED_CONFIGURATION,
            policy_intent=PolicyIntent.QUARANTINE_FEED,
            executed_action=ExecutedAction.INCREMENT_FEED_FAILURE_BUDGET,
        ),
    )
```

After moving policy types, import `FailurePolicyEvidence`, `OwnerScope`,
`FailureScope`, and `EndpointKind` from `failure_policy.py`. Do not leave
policy enums/classes in `models.py`.

**Unit test pattern** (`test_failure_classification.py` lines 146-167):

```python
def test_collector_failure_helper_returns_typed_exception(self) -> None:
    result = collector_failure(
        FeedStatusReason.SOURCE_UNREACHABLE,
        "source_unreachable",
    )

    self.assertIsInstance(result, FeedFailure)
    self.assertIs(
        result.status_reason,
        FeedStatusReason.SOURCE_UNREACHABLE,
    )
    self.assertEqual(str(result), "source_unreachable")
```

Add policy-evidence assertions beside these existing helper tests.

---

### `backend/pipeline/ingestion/collector_runtime.py` (service / runtime, event-driven)

**Analog:** `backend/pipeline/ingestion/collector_runtime.py`

**Scope note:** Phase 1 should only do import and boundary reconciliation here
if needed after moving policy classes. Broad runtime routing and telemetry are
Phase 2.

**Import pattern** (lines 30-44):

```python
from backend.pipeline.ingestion.models import (
    AudioMimeType,
    CapturedChunk,
    CaptureEvent,
    CaptureResources,
    EndpointKind,
    ExecutedAction,
    FailurePolicyEvidence,
    FailureScope,
    FeedFailure,
    OwnerScope,
    PipelineStage,
    PolicyIntent,
    SourceObservation,
)
```

After `failure_policy.py` exists, split policy imports out of `models.py` while
leaving capture boundary types in `models.py`.

**Runtime-owned side-effect boundary** (lines 82-96):

```python
class _PipelineFailure(Exception):
    """Post-capture runtime side-effect failure with a stable stage tag."""

    def __init__(
        self,
        reason: str,
        *,
        status_reason: FeedStatusReason,
        policy_evidence: FailurePolicyEvidence,
    ) -> None:
        super().__init__(reason)
        self.reason = reason
        self.status_reason = status_reason
        self.policy_evidence = policy_evidence
```

This is the correct owner for runtime side-effect failures. Do not move
runtime execution or store calls into `failure_policy.py`.

**Storage execution pattern** (lines 1084-1125 and 1138-1193):

```python
status = await self._store.report_feed_failure(
    feed["id"],
    worker_id,
    fencing_token,
    self._collector_settings.feed_failure_threshold,
    reason=reason,
    status_reason=status_reason,
)
```

```python
await self._store.release_non_budgeted_failure(
    feed["id"],
    worker_id,
    fencing_token,
    retry_after=retry_after,
    status_reason=status_reason,
)
```

These calls demonstrate the current runtime/store boundary. Phase 1 storage
work should preserve these signatures; Phase 2 decides when each path is used.

---

### `backend/pipeline/storage/feed_store.py` (service, CRUD)

**Analog:** `backend/pipeline/storage/feed_store.py`

**Imports pattern** (lines 16-35):

```python
from backend.pipeline.storage.feed_queries import (
    COUNT_HELD_BY_TYPE_SQL,
    CREATE_FEED_SQL,
    DEACTIVATE_FEED_SQL,
    DELETE_FEED_SQL,
    GET_FEED_SQL,
    LIST_FEEDS_ASC_SQL,
    LIST_FEEDS_DESC_SQL,
    RECORD_SOURCE_OBSERVATION_SQL,
    RELEASE_FEED_SQL,
    RELEASE_FEEDS_BATCH_SQL,
    RELEASE_NON_BUDGETED_FAILURE_SQL,
    RENEW_HEARTBEATS_BATCH_DIAGNOSTIC_SQL,
    REPORT_FAILURE_SQL,
    RESET_FEED_SQL,
    UPDATE_FEED_SQL,
    UPDATE_PROGRESS_SQL,
)
```

**Status enum pattern** (lines 100-114):

```python
class FeedStatusReason(enum.StrEnum):
    """Canonical abnormal feed reason stored in ``feeds.status_reason``."""

    PIPELINE_PUBLISH_AFTER_BOOKMARK_FAILED = (
        "pipeline_publish_after_bookmark_failed"
    )
    SOURCE_OFFLINE = "source_offline"
    SOURCE_UNREACHABLE = "source_unreachable"
    SOURCE_RATE_LIMITED = "source_rate_limited"
    SYSTEM_AUTHENTICATION_FAILED = "system_authentication_failed"
```

Add only the Phase 1 required value. Do not add speculative status reasons.

**Row parsing / validation pattern** (lines 231-242 and 265-278):

```python
status_reason_raw = row["status_reason"]
if status_reason_raw is None:
    status_reason = None
else:
    try:
        status_reason = FeedStatusReason(status_reason_raw)
    except ValueError as e:
        msg = (
            f"Unknown status reason {status_reason_raw!r} "
            f"for feed {row['id']}"
        )
        raise ValueError(msg) from e
```

```python
@staticmethod
def _parse_status_reason(
    raw: str | None,
    *,
    feed_id: object,
) -> FeedStatusReason | None:
    """Parse nullable status-reason text from database rows."""
```

**Budgeted failure path** (lines 421-432 and 473-484):

```python
async def report_feed_failure(
    self,
    feed_id: uuid.UUID,
    worker_id: uuid.UUID,
    fencing_token: int,
    failure_threshold: int = 5,
    backoff_base_sec: int = 15,
    backoff_max_sec: int = 600,
    *,
    reason: str | None = None,
    status_reason: FeedStatusReason | None = None,
) -> str | None:
```

```python
status_reason_value = status_reason.value if status_reason is not None else None  # fmt: skip
row = await self._pool.fetchrow(
    REPORT_FAILURE_SQL,
    feed_id,
    worker_id,
    failure_threshold,
    fencing_token,
    backoff_max_sec,
    backoff_base_sec,
    reason,
    status_reason_value,
)
```

`report_feed_failure(...)` remains the only incrementing quarantine-budget path.

**Non-budgeted release pattern** (lines 510-537):

```python
async def release_non_budgeted_failure(
    self,
    feed_id: uuid.UUID,
    worker_id: uuid.UUID,
    fencing_token: int,
    *,
    retry_after: datetime.datetime,
    status_reason: FeedStatusReason,
) -> str | None:
    """Release a non-feed-budgeted failure into retryable failing state."""
    row = await self._pool.fetchrow(
        RELEASE_NON_BUDGETED_FAILURE_SQL,
        feed_id,
        worker_id,
        fencing_token,
        retry_after,
        status_reason.value,
    )
    if row is None:
        return None
    return row["status"]
```

---

### `backend/pipeline/storage/feed_queries.py` (utility / config, CRUD)

**Analog:** `backend/pipeline/storage/feed_queries.py`

**Successful progress clearing pattern** (lines 12-23):

```python
UPDATE_PROGRESS_SQL = """\
UPDATE feeds
SET last_processed_filename = $1,
    last_bookmark_time = COALESCE($5, last_bookmark_time),
    failure_count = 0,
    status_reason_updated_at = CASE
        WHEN status_reason IS NOT NULL THEN NOW()
        ELSE status_reason_updated_at
    END,
    status_reason = NULL
WHERE id = $2 AND worker_id = $3 AND fencing_token = $4
"""
```

**Source observation clearing pattern** (lines 25-56):

```python
RECORD_SOURCE_OBSERVATION_SQL = """\
WITH current_state AS (
    SELECT id, worker_id, status, fencing_token
    FROM feeds
    WHERE id = $1
    FOR UPDATE
),
do_update AS (
    UPDATE feeds
    SET failure_count = 0,
        last_bookmark_time = GREATEST(last_bookmark_time, $4),
        status_reason_updated_at = CASE
            WHEN status_reason IS NOT NULL THEN NOW()
            ELSE status_reason_updated_at
        END,
        status_reason = NULL
```

Preserve these semantics while adding non-budgeted release.

**Budgeted SQL pattern** (lines 350-371):

```python
REPORT_FAILURE_SQL = """\
UPDATE feeds
SET status = CASE WHEN failure_count + 1 >= $3
                  THEN 'quarantined'::feed_status
                  ELSE 'failing'::feed_status END,
    failure_count = failure_count + 1,
    worker_id = NULL,
    retry_after = CASE WHEN failure_count + 1 < $3
                       THEN NOW() + LEAST($5 * INTERVAL '1 second',
                            $6 * INTERVAL '1 second' * POWER(2, failure_count))
                            + (RANDOM() * INTERVAL '10 seconds')
                       ELSE NULL END,
    quarantine_reason = CASE WHEN failure_count + 1 >= $3 THEN COALESCE($7, quarantine_reason) ELSE quarantine_reason END,
    status_reason = COALESCE($8, 'system_unexpected_error'),
    status_reason_updated_at = NOW()
WHERE id = $1 AND worker_id = $2 AND fencing_token = $4
  AND status = 'active'::feed_status
RETURNING status::text, failure_count, retry_after
"""
```

**Non-budgeted SQL pattern** (lines 373-386):

```python
RELEASE_NON_BUDGETED_FAILURE_SQL = """\
UPDATE feeds
SET status = 'failing'::feed_status,
    failure_count = 0,
    worker_id = NULL,
    retry_after = $4,
    unclaimed_since = NOW(),
    status_reason = $5,
    status_reason_updated_at = NOW()
WHERE id = $1 AND worker_id = $2
  AND fencing_token = $3
  AND status = 'active'::feed_status
RETURNING status::text, failure_count, retry_after
"""
```

The non-budgeted SQL must not mention `quarantine_reason`.

---

### `backend/pipeline/storage/tests/test_feed_store.py` (test, CRUD)

**Analog:** `backend/pipeline/storage/tests/test_feed_store.py`

**Imports and helpers pattern** (lines 1-23 and 66-69):

```python
from __future__ import annotations

import datetime
import json
import pathlib
import re
import unittest
import uuid
from typing import cast
from unittest import mock

import asyncpg

from backend.pipeline.storage import feed_queries
from backend.pipeline.storage.feed_store import (
    FeedStatus,
    FeedStatusReason,
    FeedStore,
    HeartbeatResult,
    SourceType,
)
```

```python
def _sql_without_comments(text: str) -> str:
    return "\n".join(
        line for line in text.splitlines() if not line.lstrip().startswith("--")
    )
```

**Status reason vocabulary tests** (lines 120-145):

```python
class TestFeedStatusReason(unittest.TestCase):
    """Contract tests for the canonical status reason vocabulary."""

    def test_canonical_reason_values(self) -> None:
        self.assertEqual(
            {reason.value for reason in FeedStatusReason},
            {
                "pipeline_publish_after_bookmark_failed",
                "source_offline",
                "source_unreachable",
                "source_rate_limited",
                "system_authentication_failed",
                "system_configuration_invalid",
                "system_collector_error",
                "system_pipeline_error",
                "system_unexpected_error",
            },
        )
```

**SQL contract tests** (lines 292-317 and 320-365):

```python
class TestNonBudgetedFailureSql(unittest.TestCase):
    """Tests for non-quarantine suppressed retry SQL."""

    def test_non_budgeted_failure_sql_releases_without_quarantine_budget(
        self,
    ) -> None:
        sql = _sql_without_comments(
            feed_queries.RELEASE_NON_BUDGETED_FAILURE_SQL
        )

        self.assertIn("status = 'failing'::feed_status", sql)
        self.assertIn("failure_count = 0", sql)
        self.assertIn("retry_after = $4", sql)
        self.assertIn("status_reason = $5", sql)
        self.assertIn("worker_id = NULL", sql)
        self.assertIn("WHERE id = $1 AND worker_id = $2", sql)
        self.assertIn("AND fencing_token = $3", sql)
        self.assertIn("AND status = 'active'::feed_status", sql)
        self.assertNotIn("quarantine_reason", sql)
```

```python
class TestStatusReasonClearSql(unittest.TestCase):
    """Tests for stale canonical reason clearing SQL."""

    def test_update_progress_sql_clears_stale_reason_without_lifecycle_recovery(
        self,
    ) -> None:
        sql = _sql_without_comments(feed_queries.UPDATE_PROGRESS_SQL)

        self.assertIn("status_reason = NULL", sql)
        self.assertIn("failure_count = 0", sql)
```

**Store method tests** (lines 726-806):

```python
class TestReleaseNonBudgetedFailure(unittest.IsolatedAsyncioTestCase):
    """Tests for FeedStore.release_non_budgeted_failure."""

    async def test_returns_status_when_lease_held(self) -> None:
        """Status string is returned when the non-budgeted update succeeds."""
        retry_after = datetime.datetime(
            2026, 6, 14, 12, 15, tzinfo=datetime.UTC
        )
        pool = make_mock_pool(
            fetchrow_result={
                "status": "failing",
                "failure_count": 0,
                "retry_after": retry_after,
            },
        )
        store = FeedStore(pool)

        result = await store.release_non_budgeted_failure(
            _FEED_ID,
            _WORKER_ID,
            1,
            retry_after=retry_after,
            status_reason=(
                FeedStatusReason.PIPELINE_PUBLISH_AFTER_BOOKMARK_FAILED
            ),
        )

        self.assertEqual(result, "failing")
```

Also copy the parameter-order assertion style from lines 773-805.

---

### `backend/pipeline/ingestion/tests/test_failure_policy.py` (test, transform)

**Analog:** `backend/pipeline/ingestion/collectors/tests/test_failure_classification.py`

**Imports pattern** (lines 1-15):

```python
"""Tests for shared collector failure classification helpers."""

from __future__ import annotations

import unittest

from backend.pipeline.ingestion.collectors.failure_classification import (
    FailureClassification,
    ItemBatchOutcome,
    ItemFailure,
    collector_failure,
    missing_source_feed_id_failure,
)
from backend.pipeline.ingestion.models import FeedFailure
from backend.pipeline.storage.feed_store import FeedStatusReason
```

Use this lightweight `unittest` style for the new pure policy tests. Import
`failure_policy` objects from `backend.pipeline.ingestion.failure_policy`.

**Pure contract assertion pattern** (lines 26-40 and 67-82):

```python
class TestItemBatchOutcome(unittest.TestCase):
    """Shared item-failure promotion rules."""

    def test_failure_classification_preserves_fields(self) -> None:
        classification = FailureClassification(
            FeedStatusReason.SOURCE_UNREACHABLE,
            "download_failed",
        )

        self.assertIs(
            classification.status_reason,
            FeedStatusReason.SOURCE_UNREACHABLE,
        )
        self.assertEqual(classification.reason, "download_failed")
```

```python
def test_any_success_returns_none(self) -> None:
    outcome = ItemBatchOutcome()
    failure = ItemFailure(
        FeedStatusReason.SOURCE_UNREACHABLE,
        "download_failed",
    )
    outcome.record_attempt()
    outcome.record_failure(failure)
    outcome.record_chunk_produced()

    self.assertIsNone(outcome.promoted_failure())
```

Apply this to policy decisions: assert exact enum identity with `assertIs`,
assert booleans and predicates directly, and include a test that
`pipeline_publish_after_bookmark_failed` is not feed-budget-eligible and is not
feed quarantine.

---

### `backend/pipeline/ingestion/tests/test_collector_runtime.py` (test, event-driven)

**Analog:** `backend/pipeline/ingestion/tests/test_collector_runtime.py`

**FeedFailure contract pattern** (lines 84-133):

```python
class TestFeedFailureContract(unittest.TestCase):
    """Tests for the typed collector failure boundary contract."""

    def test_normalizes_status_reason_values(self) -> None:
        """FeedFailure accepts canonical DB text values at the boundary."""
        exc = FeedFailure(
            "source_offline",
            "source_offline",
        )

        self.assertIs(exc.status_reason, FeedStatusReason.SOURCE_OFFLINE)
        self.assertEqual(exc.reason, "source_offline")
```

Modify this block for strict evidence. Do not broaden runtime routing tests in
Phase 1 beyond what is needed to keep the boundary contract coherent.

---

### `backend/pipeline/ingestion/collectors/README.md` (documentation, event-driven guidance)

**Analog:** `backend/pipeline/ingestion/collectors/README.md`

**Collector/runtime ownership pattern** (lines 20-38):

```markdown
VM collectors have one job: turn a source-specific stream or polling API into
`CapturedChunk` audio values, emit `SourceObservation` for successful non-audio
source checks, or raise a typed `FeedFailure` for known feed-level failures.
The runtime owns lifecycle state, leases, GCS upload, Pub/Sub publish, progress
bookmarks, heartbeats, retries after failure, and quarantine telemetry.

Do not write feed lifecycle state from a collector. A collector should yield
valid capture events or report source-specific feed failure evidence through
`FeedFailure`.
```

**Status reason guidance pattern** (lines 44-82):

```markdown
`feeds.status` remains lifecycle and scheduling state. `feeds.status_reason`
is a nullable, current abnormal-condition label that helps operators answer:
"is this caused by the upstream source, or by the ingestion system?" Successful
async progress, successful Echo heartbeat/progress, and manual reset clear
stale status reasons.

`quarantine_reason` is different. It preserves the short raw forensic reason
on quarantine transitions. Do not parse it for canonical ownership, and do not
replace it with `status_reason`.
```

If the planner updates docs in Phase 1, update only sections that become
misleading after strict evidence and the policy module move.

## Shared Patterns

### Python Imports

**Source:** `.github/instructions/PYTHON_STYLE.instructions.md` and source
imports in `models.py`, `failure_classification.py`, and `feed_store.py`.

**Apply to:** All Python files.

- Use `from __future__ import annotations`.
- Use absolute package imports, not relative imports.
- Import modules/packages and project modules explicitly; avoid ad hoc local
  imports except where existing code already has a justified local import.

### Policy Ownership

**Source:** `01-CONTEXT.md` D-01 through D-08 and
`backend/pipeline/ingestion/collectors/failure_classification.py`.

**Apply to:** `failure_policy.py`, `models.py`, collector helpers, and runtime
imports.

`failure_policy.py` owns vocabulary, `FailurePolicyEvidence`,
`FailurePolicyDecision`, `classify_failure_policy(...)`, and pure predicates.
It must not call stores, emit logs, publish telemetry, mutate runtime state, or
parse `quarantine_reason`.

### Fenced Storage Writes

**Source:** `backend/pipeline/storage/feed_queries.py` lines 350-386 and
`backend/pipeline/storage/feed_store.py` lines 421-537.

**Apply to:** `feed_queries.py`, `feed_store.py`, and storage tests.

All feed lifecycle writes are atomic SQL constants behind thin `FeedStore`
methods. New non-budgeted behavior must include `WHERE id`, `worker_id`,
`fencing_token`, and `status = 'active'::feed_status` guards.

### Status Reason Parsing

**Source:** `backend/pipeline/storage/feed_store.py` lines 231-278.

**Apply to:** status reason enum updates and row mapping tests.

DB text is parsed into `FeedStatusReason`, `None` is allowed, and unknown text
raises `ValueError` with feed context.

### Test Style

**Source:** `backend/pipeline/storage/tests/test_feed_store.py` and
`backend/pipeline/ingestion/collectors/tests/test_failure_classification.py`.

**Apply to:** all Phase 1 tests.

Use `unittest.TestCase` or `unittest.IsolatedAsyncioTestCase`, `make_mock_pool`
for store methods, string-contract tests for SQL constants, and exact enum
identity assertions with `assertIs`.

## No Analog Found

All files have at least a role-match analog. The only gap is that no existing
module exactly matches `FailurePolicyDecision` plus predicate helpers; use
`failure_classification.py` for pure transform style and `models.py` for
enum/dataclass style.

## Metadata

**Analog search scope:** `backend/pipeline/ingestion`, `backend/pipeline/storage`,
`.planning/codebase`, phase context/research files.

**Files scanned:** 92 Python/Markdown files under ingestion/storage, plus
planning artifacts.

**Project instructions read:** `AGENTS.md`, `.agents/instructions.md`,
`.github/instructions/PYTHON_STYLE.instructions.md`.

**Project skills:** No `.codex/skills/` or `.agents/skills/` directories found
in this worktree.

**Dirty worktree note:** Current uncommitted edits already touch ingestion,
collector docs/helpers, runtime, storage SQL/store, and storage/runtime tests.
Downstream planning should reconcile those edits rather than assuming a clean
base. In particular, current dirty code has policy classes in `models.py` and
verdict fields on `FailurePolicyEvidence`; Phase 1 decisions require moving
that ownership to `failure_policy.py` and splitting facts from verdicts.

**Pattern extraction date:** 2026-06-15
