# Phase 3: Verification And Compatibility - Pattern Map

**Mapped:** 2026-06-15
**Files analyzed:** 8
**Analogs found:** 8 / 8

## File Classification

| New/Modified File | Role | Data Flow | Closest Analog | Match Quality |
|-------------------|------|-----------|----------------|---------------|
| `backend/pipeline/storage/tests/test_feed_store.py` | test | CRUD | `backend/pipeline/storage/tests/test_feed_store.py` | exact |
| `backend/pipeline/ingestion/tests/test_collector_runtime.py` | test | event-driven | `backend/pipeline/ingestion/tests/test_collector_runtime.py` | exact |
| `frontend/api/openapi.yaml` | config / schema | request-response | `frontend/api/openapi.yaml` + `TestFeedStatusReason.test_matches_openapi_spec` | exact |
| `frontend/common/src/types/feeds.ts` | model / type | request-response | `frontend/common/src/types/feeds.ts` | exact |
| `frontend/common/src/utils/statusUtils.ts` | utility | transform | `frontend/common/src/utils/statusUtils.ts` | exact |
| `frontend/transcription-ui/src/components/common/FeedStatusIndicator.tsx` | component | transform / render | `frontend/transcription-ui/src/components/common/FeedStatusIndicator.tsx` | exact |
| `frontend/transcription-ui/src/components/common/FeedStatusIndicator.test.tsx` | test | render / request-response | `frontend/transcription-ui/src/components/common/FeedStatusIndicator.test.tsx` | exact, optional |
| `.planning/phases/03-verification-and-compatibility/03-03-SUMMARY.md` | documentation | batch / transform | `.planning/phases/02-runtime-routing-and-telemetry/02-03-SUMMARY.md` + `02-VERIFICATION.md` | role-match |

## Pattern Assignments

### `backend/pipeline/storage/tests/test_feed_store.py` (test, CRUD)

**Analog:** `backend/pipeline/storage/tests/test_feed_store.py`

**Imports and fixture pattern** (lines 1-24):

```python
from __future__ import annotations

import datetime
import pathlib
import unittest
import uuid
from unittest import mock

import yaml

from backend.pipeline.storage import feed_queries, quarantine_reason
from backend.pipeline.storage.feed_store import (
    FeedStatusReason,
    FeedStore,
)
from backend.pipeline.storage.tests.connection_util import make_mock_pool
```

**OpenAPI enum parity pattern** (lines 141-167):

```python
def test_matches_openapi_spec(self) -> None:
    current_file = pathlib.Path(__file__).resolve()
    repo_root = current_file.parents[4]
    openapi_path = repo_root / "frontend" / "api" / "openapi.yaml"

    with openapi_path.open("r") as f:
        spec = yaml.safe_load(f)

    schemas = spec.get("components", {}).get("schemas", {})
    backend_reasons = schemas.get("BackendFeedStatusReason", {}).get(
        "enum", []
    )

    python_reasons = {reason.value for reason in FeedStatusReason}
    expected_openapi_reasons = python_reasons | {"unknown"}

    self.assertEqual(set(backend_reasons), expected_openapi_reasons)
```

**SQL invariant pattern** (lines 380-406):

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
        self.assertNotIn("quarantine_reason", sql)
        self.assertNotIn("failure_count + 1", sql)
```

**Async store-call pattern** (lines 861-940):

```python
class TestReleaseNonBudgetedFailure(unittest.IsolatedAsyncioTestCase):
    """Tests for FeedStore.release_non_budgeted_failure."""

    async def test_returns_status_when_lease_held(self) -> None:
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

    async def test_passes_correct_parameters(self) -> None:
        await store.release_non_budgeted_failure(
            _FEED_ID,
            _WORKER_ID,
            1,
            retry_after=retry_after,
            status_reason=FeedStatusReason.SOURCE_OFFLINE,
        )

        args = pool.fetchrow.call_args[0]
        self.assertEqual(
            args[1:],
            (
                _FEED_ID,
                _WORKER_ID,
                1,
                retry_after,
                "source_offline",
            ),
        )
```

**Apply to Phase 3:** prove `TEST-01` and `TEST-02` with exact SQL/store state assertions for `status='failing'`, `failure_count=0`, `retry_after`, `status_reason`, and absence of `quarantine_reason`.

---

### `backend/pipeline/ingestion/tests/test_collector_runtime.py` (test, event-driven)

**Analog:** `backend/pipeline/ingestion/tests/test_collector_runtime.py`

**Imports and helper pattern** (lines 1-37, 148-169):

```python
from __future__ import annotations

import asyncio
import datetime
import logging
import unittest
from typing import Any, cast
from unittest import mock

from backend.pipeline.ingestion import failure_policy
from backend.pipeline.ingestion.collector_runtime import CollectorRuntime
from backend.pipeline.ingestion.models import CapturedChunk, FeedFailure
from backend.pipeline.storage.feed_store import FeedStatusReason, LeasedFeed

def _mock_pubsub_publish(message_id: str = "test-message-id") -> mock._patch:
    return mock.patch(
        "backend.pipeline.ingestion.collector_runtime.gcp_helper.publish_audio_chunk",
        new_callable=mock.AsyncMock,
        return_value=message_id,
    )
```

**Post-bookmark publish gap pattern** (lines 1882-1981):

```python
async def test_non_retryable_pubsub_failure_records_publish_gap_without_feed_budget(
    self,
) -> None:
    """Non-retryable Pub/Sub errors after bookmark do not burn feed budget."""
    rt = CollectorRuntime(capture_fn=_one_chunk, settings=_make_settings())
    rt._store = mock.AsyncMock()
    rt._store.update_feed_progress = mock.AsyncMock(side_effect=_bookmark)
    rt._store.release_non_budgeted_failure.return_value = "failing"

    with (
        mock.patch(
            "backend.pipeline.ingestion.collector_runtime.gcp_helper.publish_audio_chunk",
            mock.AsyncMock(side_effect=_publish),
        ),
        self.assertLogs(
            "backend.pipeline.ingestion.collector_runtime",
            level=logging.INFO,
        ) as cm,
        mock.patch.object(
            CollectorRuntime,
            "_non_budgeted_retry_after",
            return_value=retry_after,
        ),
    ):
        await rt._process_feed(_FEED)

    rt._store.report_feed_failure.assert_not_awaited()
    rt._store.release_non_budgeted_failure.assert_awaited_once()
    nb_kwargs = rt._store.release_non_budgeted_failure.await_args.kwargs
    self.assertIs(
        nb_kwargs["status_reason"],
        FeedStatusReason.PIPELINE_PUBLISH_AFTER_BOOKMARK_FAILED,
    )

    records = [
        cast("dict[str, Any]", r.__dict__.get("json_fields"))
        for r in cm.records
        if getattr(r, "json_fields", None)
    ]
    event_types = {r["event_type"] for r in records}
    self.assertIn("feed_failure_policy_decision", event_types)
    self.assertIn("post_bookmark_publish_failure", event_types)
```

**Budgeted quarantine control pattern** (lines 1990-2054):

```python
async def test_feed_config_quarantine_emits_telemetry(self) -> None:
    async def _failing_capture(feed, shutdown, _resources):
        raise missing_source_feed_id_failure()
        yield _make_captured_chunk(b"audio")

    rt._store.report_feed_failure.return_value = "quarantined"

    with mock.patch(
        "backend.pipeline.ingestion.collector_runtime.quarantine_telemetry"
    ) as mock_telemetry:
        mock_telemetry.emit_quarantine_event = mock.AsyncMock()
        await rt._process_feed(_FEED)

    rt._store.report_feed_failure.assert_awaited_once()
    rt._store.release_non_budgeted_failure.assert_not_awaited()
    mock_telemetry.emit_quarantine_event.assert_awaited_once_with(
        feed_id=str(_FEED_ID),
        feed_name="Test Feed",
        source_type="bcfy_feeds",
        reason="missing_source_feed_id",
        status_reason="system_configuration_invalid",
    )
```

**Non-budgeted no-quarantine pattern** (lines 2056-2086):

```python
async def test_non_budgeted_failure_does_not_emit_quarantine_telemetry(
    self,
) -> None:
    """Non-budgeted failures never emit feed_quarantined telemetry."""
    rt._store.release_non_budgeted_failure.return_value = "failing"

    with mock.patch(
        "backend.pipeline.ingestion.collector_runtime.quarantine_telemetry"
    ) as mock_telemetry:
        mock_telemetry.emit_quarantine_event = mock.AsyncMock()
        await rt._process_feed(_FEED)

    mock_telemetry.emit_quarantine_event.assert_not_awaited()
    rt._store.report_feed_failure.assert_not_awaited()
    rt._store.release_non_budgeted_failure.assert_awaited_once()
```

**Telemetry-gap pattern** (lines 2133-2183):

```python
async def test_untyped_runtime_exception_routes_to_telemetry_gap(
    self,
) -> None:
    """Untyped runtime failures use UNKNOWN evidence and no feed budget."""
    with self.assertLogs(
        "backend.pipeline.ingestion.collector_runtime",
        level=logging.INFO,
    ) as cm:
        await rt._process_feed(_FEED)

    rt._store.report_feed_failure.assert_not_awaited()
    rt._store.release_non_budgeted_failure.assert_awaited_once()
    policy_records = [
        cast("dict[str, Any]", record.__dict__["json_fields"])
        for record in cm.records
        if getattr(record, "json_fields", {}).get("event_type")
        == "feed_failure_policy_decision"
    ]
    policy_record = policy_records[0]
    self.assertEqual(policy_record["owner_scope"], "unknown")
    self.assertEqual(policy_record["policy_intent"], "telemetry_gap")
    self.assertEqual(
        policy_record["executed_action"],
        "suppress_feed_quarantine_telemetry_gap",
    )
```

**Table-driven non-actionable pattern** (lines 2224-2331):

```python
cases = (
    (
        "source_offline",
        FeedStatusReason.SOURCE_OFFLINE,
        "source_offline",
        failure_policy.FailurePolicyEvidence(
            owner_scope=failure_policy.OwnerScope.FEED,
            failure_scope=failure_policy.FailureScope.OBSERVATION,
            endpoint_kind=failure_policy.EndpointKind.STREAM,
        ),
    ),
    (
        "shared_auth",
        FeedStatusReason.SYSTEM_AUTHENTICATION_FAILED,
        "auth_failed",
        failure_policy.FailurePolicyEvidence(
            owner_scope=failure_policy.OwnerScope.CREDENTIAL_SCOPE,
            failure_scope=failure_policy.FailureScope.CLASS,
            endpoint_kind=failure_policy.EndpointKind.CALLS_API,
        ),
    ),
)

for name, status_reason, reason, evidence in cases:
    with self.subTest(name=name):
        raise FeedFailure(
            status_reason,
            reason,
            policy_evidence=evidence,
        )
        rt._store.report_feed_failure.assert_not_awaited()
        rt._store.release_non_budgeted_failure.assert_awaited_once()
        mock_telemetry.emit_quarantine_event.assert_not_awaited()
```

**Apply to Phase 3:** map `TEST-03` through `TEST-08` to store-call assertions first, then structured `json_fields` telemetry assertions where the event contract matters. Do not assert incidental log text or ordering outside `event_type`.

---

### `frontend/api/openapi.yaml` (config / schema, request-response)

**Analog:** `frontend/api/openapi.yaml`

**Enum surface pattern** (lines 280-292):

```yaml
BackendFeedStatusReason:
  type: string
  enum:
    - unknown
    - source_offline
    - source_unreachable
    - source_rate_limited
    - system_authentication_failed
    - system_configuration_invalid
    - system_collector_error
    - system_pipeline_error
    - pipeline_publish_after_bookmark_failed
    - system_unexpected_error
```

**Feed schema pattern** (lines 315-328):

```yaml
status:
  $ref: "#/components/schemas/FeedStatus"
substatus:
  $ref: "#/components/schemas/BackendFeedStatus"
statusReason:
  $ref: "#/components/schemas/BackendFeedStatusReason"
```

**Apply to Phase 3:** keep `pipeline_publish_after_bookmark_failed` in the status-reason enum and do not add a new lifecycle status. Backend parity is enforced by `TestFeedStatusReason.test_matches_openapi_spec`.

---

### `frontend/common/src/types/feeds.ts` (model / type, request-response)

**Analog:** `frontend/common/src/types/feeds.ts`

**Shared type pattern** (lines 9-28, 40-50):

```typescript
export type BackendFeedStatus =
  | 'unclaimed'
  | 'active'
  | 'failing'
  | 'quarantined'
  | 'deactivated';

export type BackendFeedStatusReason =
  | 'unknown'
  | 'source_offline'
  | 'source_unreachable'
  | 'source_rate_limited'
  | 'system_authentication_failed'
  | 'system_configuration_invalid'
  | 'system_collector_error'
  | 'system_pipeline_error'
  | 'pipeline_publish_after_bookmark_failed'
  | 'system_unexpected_error';

export interface Feed extends BaseFeed {
  status: FeedStatus;
  substatus: BackendFeedStatus;
  statusReason?: BackendFeedStatusReason;
}
```

**Apply to Phase 3:** add only the missing backend reason literal if absent. Preserve `BackendFeedStatus` and `FeedStatus` exactly so `failing` and `quarantined` continue to flow to existing UI `error` handling.

---

### `frontend/common/src/utils/statusUtils.ts` (utility, transform)

**Analog:** `frontend/common/src/utils/statusUtils.ts`

**Imports and allowlist pattern** (lines 1-25):

```typescript
import type {
  BackendFeedStatus,
  BackendFeedStatusReason,
  FeedStatus,
} from '../types/feeds.js';

const BACKEND_FEED_STATUS_REASONS = new Set<BackendFeedStatusReason>([
  'source_offline',
  'source_unreachable',
  'source_rate_limited',
  'system_authentication_failed',
  'system_configuration_invalid',
  'system_collector_error',
  'system_pipeline_error',
  'pipeline_publish_after_bookmark_failed',
  'system_unexpected_error',
]);

export function convertFeedStatusReason(
  reason: string | null | undefined
): BackendFeedStatusReason | undefined {
  if (!reason) return undefined;
  return BACKEND_FEED_STATUS_REASONS.has(reason as BackendFeedStatusReason)
    ? (reason as BackendFeedStatusReason)
    : 'unknown';
}
```

**Lifecycle preservation pattern** (lines 28-39, 42-53):

```typescript
export function convertFeedStatusBackend(status: BackendFeedStatus): FeedStatus {
  switch (status) {
    case 'active':
      return 'active';
    case 'quarantined':
    case 'failing':
      return 'error';
    case 'deactivated':
    case 'unclaimed':
    default:
      return 'inactive';
  }
}

export function mapFeedStatusToBackendStatuses(status: string): BackendFeedStatus[] {
  switch (status.toLowerCase()) {
    case 'error':
      return ['failing', 'quarantined'];
    default:
      return [];
  }
}
```

**Apply to Phase 3:** update the reason allowlist only where needed. Do not create a distinct frontend lifecycle status for `pipeline_publish_after_bookmark_failed`.

---

### `frontend/transcription-ui/src/components/common/FeedStatusIndicator.tsx` (component, transform / render)

**Analog:** `frontend/transcription-ui/src/components/common/FeedStatusIndicator.tsx`

**Import and display-map pattern** (lines 1-35):

```typescript
import Badge, { type BadgeProps } from '@mui/material/Badge';
import Box from '@mui/material/Box';
import Tooltip from '@mui/material/Tooltip';
import Typography from '@mui/material/Typography';
import type {
  BackendFeedStatus,
  BackendFeedStatusReason,
  FeedStatus,
} from '@transcription/common';

const FEED_STATUS_REASON_UI_TEXT_DISPLAY: Record<
  BackendFeedStatusReason,
  string
> = {
  unknown: 'Unknown Status',
  source_offline: 'Source Offline',
  source_unreachable: 'Source Unreachable',
  source_rate_limited: 'Source Rate Limited',
  system_authentication_failed: 'System Authentication Failed',
  system_configuration_invalid: 'System Configuration Invalid',
  system_collector_error: 'System Collector Error',
  system_pipeline_error: 'System Pipeline Error',
  pipeline_publish_after_bookmark_failed: 'Pipeline Publish Failed After Bookmark',
  system_unexpected_error: 'System Unexpected Error',
};
```

**Tooltip fallback pattern** (lines 49-78):

```typescript
function formatSubstatusTooltipText({
  substatus,
  statusReason,
  quarantineReason,
}: {
  substatus?: BackendFeedStatus;
  statusReason?: BackendFeedStatusReason;
  quarantineReason?: string;
}): string {
  const parts: string[] = [];

  if (substatus) {
    const substatusDisplay =
      FEED_SUBSTATUS_UI_TEXT_DISPLAY[substatus] ?? substatus;
    const reasonDisplay = statusReason
      ? ` (${FEED_STATUS_REASON_UI_TEXT_DISPLAY[statusReason] ?? statusReason})`
      : '';
    parts.push(`${substatusDisplay}${reasonDisplay}`);
  }

  if (quarantineReason) {
    parts.push(quarantineReason);
  }

  return parts.join(': ');
}
```

**Render pattern** (lines 80-169):

```typescript
export function FeedStatusIndicator({
  status,
  substatus,
  statusReason,
  quarantineReason,
  lastHeartbeat,
}: {
  status?: FeedStatus;
  substatus?: BackendFeedStatus;
  statusReason?: BackendFeedStatusReason;
  quarantineReason?: string;
  lastHeartbeat?: string;
}) {
  if (!status) {
    return null;
  }

  const statusConfig = FEED_STATUS_UI_CONFIG[status] ?? {
    displayText: status,
    color: 'default',
  };

  return (
    <Tooltip title={substatusText}>
      <Box>
        <Badge color={statusConfig.color} variant="dot" />
        <Typography variant="body2">{statusConfig.displayText}</Typography>
      </Box>
    </Tooltip>
  );
}
```

**Apply to Phase 3:** add display text only if the new reason is missing. Keep the visible badge label tied to `FeedStatus` (`error` for both `failing` and `quarantined`).

---

### `frontend/transcription-ui/src/components/common/FeedStatusIndicator.test.tsx` (test, render / request-response)

**Analog:** `frontend/transcription-ui/src/components/common/FeedStatusIndicator.test.tsx`

**Testing Library pattern** (lines 1-17):

```typescript
// @vitest-environment jsdom
import { afterEach, describe, expect, it, vi } from 'vitest';

import {
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
} from '@testing-library/react';
import type {
  BackendFeedStatus,
  BackendFeedStatusReason,
  FeedStatus,
} from '@transcription/common';

import FeedStatusIndicator from './FeedStatusIndicator';
```

**Tooltip reason pattern** (lines 60-72):

```typescript
it('displays substatus and statusReason in tooltip on hover', async () => {
  render(
    <FeedStatusIndicator
      status="error"
      substatus="failing"
      statusReason="source_offline"
    />
  );
  const statusText = screen.getByText('Error');
  fireEvent.mouseOver(statusText);
  await waitFor(() => {
    expect(screen.getByText('Failing (Source Offline)')).toBeTruthy();
  });
});
```

**Fallback reason pattern** (lines 75-87):

```typescript
it('displays custom substatus and reason in tooltip on hover', async () => {
  render(
    <FeedStatusIndicator
      status="error"
      substatus={'custom_substatus' as unknown as BackendFeedStatus}
      statusReason={'custom_reason' as unknown as BackendFeedStatusReason}
    />
  );
  const statusText = screen.getByText('Error');
  fireEvent.mouseOver(statusText);
  await waitFor(() => {
    expect(screen.getByText('custom_substatus (custom_reason)')).toBeTruthy();
  });
});
```

**Apply to Phase 3:** per D-03, do not add frontend test coverage solely for `pipeline_publish_after_bookmark_failed`. Touch this file only if existing tests fail after the compatibility update.

---

### `.planning/phases/03-verification-and-compatibility/03-03-SUMMARY.md` (documentation, batch / transform)

**Analog:** `.planning/phases/02-runtime-routing-and-telemetry/02-03-SUMMARY.md`

**Summary frontmatter pattern** (lines 1-30):

```markdown
---
phase: 02-runtime-routing-and-telemetry
plan: 03
subsystem: ingestion-runtime
tags: [telemetry, publish-gap, policy-decision]
provides:
  - Policy decision telemetry contract tests for budgeted and non-budgeted failures.
  - Post-bookmark publish-gap telemetry tests with replay flags.
key-files:
  created:
    - .planning/phases/02-runtime-routing-and-telemetry/02-03-SUMMARY.md
  modified:
    - backend/pipeline/ingestion/tests/test_collector_runtime.py
requirements-completed: [RUN-04, RUN-05, RUN-06, TEL-01, TEL-02, TEL-03, TEL-04, TEL-05]
completed: 2026-06-15
---
```

**Accomplishment and verification pattern** (lines 45-80):

````markdown
## Accomplishments

- Added budgeted policy-decision telemetry assertions for feed configuration quarantine.
- Added non-budgeted telemetry assertions for UNKNOWN telemetry gap and pipeline-owned failures.
- Proved Pub/Sub publish-after-bookmark emits `post_bookmark_publish_failure`.

## Verification

```bash
safe-run -- uv run python -m pytest backend/pipeline/ingestion/tests/test_failure_policy.py backend/pipeline/ingestion/tests/test_collector_runtime.py::TestProcessFeedQuarantine -q -n 0
```

Result: `27 passed, 7 subtests passed in 0.86s`.
````

**Requirement evidence matrix pattern** from `02-VERIFICATION.md` (lines 25-41):

```markdown
| Requirement | Status | Evidence |
|-------------|--------|----------|
| RUN-03 | Passed | Pub/Sub publish-after-bookmark tests assert `pipeline_publish_after_bookmark_failed`. |
| RUN-07 | Passed | Feed-config quarantine test asserts the budgeted path and quarantine telemetry still work. |
| TEL-05 | Passed | Non-budgeted source-class/UNKNOWN/pipeline tests assert quarantine telemetry is not emitted. |
```

**Apply to Phase 3:** include the two required tables from D-05 through D-09:

| Table | Required Contents |
|-------|-------------------|
| Requirement-to-proof matrix | `STAT-02` and `TEST-01` through `TEST-08`, each mapped to exact tests, files, or compatibility surfaces |
| Incident-taxonomy-to-policy-scenario matrix | Full original incident categories mapped to covered policy scenarios, without requiring one bespoke test per historic label |

## Shared Patterns

### Storage Budget Separation

**Source:** `backend/pipeline/storage/feed_store.py` lines 428-549 and `backend/pipeline/storage/feed_queries.py` lines 350-386

```python
async def report_feed_failure(..., status_reason: FeedStatusReason | None = None) -> str | None:
    row = await self._pool.fetchrow(
        REPORT_FAILURE_SQL,
        feed_id,
        worker_id,
        failure_threshold,
        fencing_token,
        backoff_max_sec,
        backoff_base_sec,
        stored_reason,
        status_reason_value,
    )

async def release_non_budgeted_failure(
    self,
    feed_id: uuid.UUID,
    worker_id: uuid.UUID,
    fencing_token: int,
    *,
    retry_after: datetime.datetime,
    status_reason: FeedStatusReason,
) -> str | None:
    row = await self._pool.fetchrow(
        RELEASE_NON_BUDGETED_FAILURE_SQL,
        feed_id,
        worker_id,
        fencing_token,
        retry_after,
        status_reason.value,
    )
```

Apply `report_feed_failure(...)` only to budgeted feed quarantine tests and `release_non_budgeted_failure(...)` to suppressed retry tests.

### Runtime Policy Telemetry

**Source:** `backend/pipeline/ingestion/collector_runtime.py` lines 837-895 and 1075-1195

```python
payload: dict[str, object] = {
    "event_type": "feed_failure_policy_decision",
    "feed_id": str(feed["id"]),
    "source_type": str(feed["source_type"]),
    "reason": reason,
    "status_reason": status_reason.value,
    "replay_missing": replay_missing,
    "data_gap_known": data_gap_known,
    **self._policy_evidence_fields(decision),
}

if replay_missing and data_gap_known:
    self._emit_post_bookmark_publish_failure(
        feed,
        reason=reason,
        status_reason=status_reason,
        evidence=evidence,
    )
```

Use `assertLogs(..., level=logging.INFO)` and filter `record.__dict__["json_fields"]` by `event_type`.

### Policy Classification

**Source:** `backend/pipeline/ingestion/failure_policy.py` lines 101-170

```python
if (
    status_reason
    is feed_store.FeedStatusReason.PIPELINE_PUBLISH_AFTER_BOOKMARK_FAILED
    or evidence.pipeline_stage is PipelineStage.PUBSUB_PUBLISH
    and evidence.owner_scope is OwnerScope.PIPELINE
):
    return FailurePolicyDecision(
        policy_intent=PolicyIntent.HOLD_FOR_REPLAY,
        executed_action=(
            ExecutedAction.SUPPRESS_FEED_QUARANTINE_RECORD_PUBLISH_GAP
        ),
        feed_budget_eligible=False,
        quarantine_feed=False,
    )

if evidence.owner_scope is OwnerScope.UNKNOWN:
    return FailurePolicyDecision(
        policy_intent=PolicyIntent.TELEMETRY_GAP,
        executed_action=(
            ExecutedAction.SUPPRESS_FEED_QUARANTINE_TELEMETRY_GAP
        ),
        feed_budget_eligible=False,
        quarantine_feed=False,
    )
```

Phase 3 tests should assert the resulting routing and telemetry, not duplicate policy classification logic inline.

### Status Compatibility

**Sources:** `frontend/common/src/types/feeds.ts` lines 18-28, `frontend/common/src/utils/statusUtils.ts` lines 7-25, `frontend/transcription-ui/src/components/common/FeedStatusIndicator.tsx` lines 21-35

```typescript
export type BackendFeedStatusReason =
  | 'unknown'
  | 'source_offline'
  | 'source_unreachable'
  | 'source_rate_limited'
  | 'system_authentication_failed'
  | 'system_configuration_invalid'
  | 'system_collector_error'
  | 'system_pipeline_error'
  | 'pipeline_publish_after_bookmark_failed'
  | 'system_unexpected_error';

const BACKEND_FEED_STATUS_REASONS = new Set<BackendFeedStatusReason>([
  'pipeline_publish_after_bookmark_failed',
]);

const FEED_STATUS_REASON_UI_TEXT_DISPLAY: Record<
  BackendFeedStatusReason,
  string
> = {
  pipeline_publish_after_bookmark_failed: 'Pipeline Publish Failed After Bookmark',
};
```

Keep the lifecycle mapping from `frontend/common/src/utils/statusUtils.ts` lines 28-39:

```typescript
case 'quarantined':
case 'failing':
  return 'error';
```

### Verification Commands

**Source:** `.planning/codebase/TESTING.md` and `.planning/phases/02-runtime-routing-and-telemetry/02-VERIFICATION.md`

Use narrow checks with `safe-run --`, for example:

```bash
safe-run -- uv run python -m pytest backend/pipeline/storage/tests/test_feed_store.py::TestFeedStatusReason backend/pipeline/storage/tests/test_feed_store.py::TestNonBudgetedFailureSql backend/pipeline/storage/tests/test_feed_store.py::TestReleaseNonBudgetedFailure -q -n 0
safe-run -- uv run python -m pytest backend/pipeline/ingestion/tests/test_collector_runtime.py::TestProcessFeedRetry backend/pipeline/ingestion/tests/test_collector_runtime.py::TestProcessFeedQuarantine -q -n 0
git diff --check
```

Do not run broad local E2E/API/component/Docker stacks unless the user explicitly approves.

## No Analog Found

None. Every implied Phase 3 file has an existing local analog. `FeedStatusIndicator.test.tsx` is optional per D-03 and should remain untouched unless the existing frontend tests fail.

## Metadata

**Context source:** `.planning/phases/03-verification-and-compatibility/03-CONTEXT.md`
**Research source:** No `03-RESEARCH.md` present in the phase directory
**Analog search scope:** `backend/pipeline/ingestion`, `backend/pipeline/storage`, `frontend`, `.planning/phases/01-policy-and-storage-foundation`, `.planning/phases/02-runtime-routing-and-telemetry`
**Files scanned:** 253
**Pattern extraction date:** 2026-06-15
