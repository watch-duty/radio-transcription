# Quarantine Detection Telemetry — Design Spec

## Problem

When the normalizer runtime quarantines a feed (after repeated failures), nothing alerts the team. Engineers only discover quarantined feeds through manual database inspection.

## Solution

Emit a Cloud Monitoring custom metric and a structured log on every quarantine transition. A Cloud Monitoring alerting policy can then page an engineer within seconds.

## Architecture

```
common/clients/monitoring_client.py    ← shared async GCP Monitoring wrapper
      ↑
ingestion/quarantine_telemetry.py      ← quarantine business logic + safety boundary
      ↑  (local import)
storage/feed_store.py                  ← detects quarantine via RETURNING clause
      ↑
ingestion/normalizer_runtime.py        ← configures telemetry at startup
```

## Components

### 1. MonitoringClient (`common/clients/monitoring_client.py`)

Shared async wrapper around `MetricServiceAsyncClient`. Follows the `GcsClient`/`PubSubClient` pattern.

```python
class MonitoringClient:
    def __init__(self, project_id: str) -> None
    def _get_client(self) -> MetricServiceAsyncClient  # lazy singleton
    async def write_time_series(
        self, metric_type: str, labels: dict[str, str], value: int,
    ) -> None  # GAUGE INT64, resource.type="global"
```

- **Lazy init:** `MetricServiceAsyncClient` created on first `write_time_series` call, not at construction. Prevents credential resolution at import time.
- **Error policy:** propagates all exceptions. This is a utility, not a safety boundary.
- **No `close()`:** no buffered state to flush (unlike `PubSubClient` which needs `stop()` for pending publishes). gRPC channel cleaned up on process exit.

### 2. quarantine_telemetry (`ingestion/quarantine_telemetry.py`)

Module-scoped telemetry for quarantine events. Owns the "never raise" safety guarantee.

**Module state:**
- `_client: MonitoringClient | None = None`

**`configure(gcp_project_id: str | None) -> None`**
- `gcp_project_id` set: creates `MonitoringClient(project_id)`, stores in `_client`.
- `None`: sets `_client = None`. Metric emission disabled. Default for local dev and tests.
- No GCP calls — client is lazy.

**`async def emit_quarantine_event(feed_id: str, feed_name: str, source_type: str) -> None`**
- **Never raises.** Entire body wrapped in `try/except Exception`.
- Emits structured log at ERROR level (always, even if metric emission disabled):
  ```python
  logger.error("Feed quarantined", extra={
      "event_type": "feed_quarantined",
      "feed_id": feed_id,        # str, not UUID
      "feed_name": feed_name,
      "source_type": source_type,
  })
  ```
- If `_client is None`: returns after the log.
- Otherwise: `await _client.write_time_series(...)` with metric type `custom.googleapis.com/feeds/quarantine_events`, labels `{feed_id, feed_name, source_type}`, value `1`.
- On exception: `logger.warning("Failed to emit quarantine metric: %s", exc)` inside a nested `try/except Exception: pass` so even a broken logging handler cannot violate the guarantee.

### 3. SQL change (`_REPORT_FAILURE_SQL`)

Extend the existing RETURNING clause from:
```sql
RETURNING status::text, failure_count, retry_after
```
to:
```sql
RETURNING status::text, failure_count, retry_after, name, source_type
```

Both columns exist on the `feeds` table being updated. Zero-cost extension.

### 4. FeedStore change (`report_feed_failure`)

In the `row["status"] == "quarantined"` branch, after the existing `logger.critical`, add:
```python
from backend.pipeline.ingestion import quarantine_telemetry  # noqa: PLC0415
await quarantine_telemetry.emit_quarantine_event(
    feed_id=str(feed_id),
    feed_name=row["name"],
    source_type=row["source_type"],
)
```

- Existing `logger.critical` stays — operational signal at the FeedStore level (includes `failure_count`).
- The ERROR log in `emit_quarantine_event` is a structured alerting signal (includes `event_type`, `feed_name`, `source_type`).
- Return type stays `bool`. No signature changes.
- Local import prevents `google-cloud-monitoring` from loading at import time in tests.

### 5. Settings + Runtime Wiring

**`NormalizerSettings`** — new optional field:
```python
gcp_project_id: str | None = field(
    default_factory=lambda: os.environ.get("GCP_PROJECT_ID"),
)
```

**`_main()`** — after heartbeat thread start, before leasing loop:
```python
from backend.pipeline.ingestion import quarantine_telemetry  # noqa: PLC0415
quarantine_telemetry.configure(settings.gcp_project_id)
```

### 6. Metric Descriptor Setup Script

`backend/pipeline/ingestion/scripts/create_quarantine_metric.py`

One-shot CLI: `python -m backend.pipeline.ingestion.scripts.create_quarantine_metric --project-id=<ID>`

- Uses sync `MetricServiceClient` (one-shot script, not async runtime).
- Creates `MetricDescriptor`: type `custom.googleapis.com/feeds/quarantine_events`, GAUGE INT64, labels `feed_id`/`feed_name`/`source_type` (all STRING).
- Handles `AlreadyExists` gracefully.
- Runs once per GCP project, not per deploy.

## Error Handling Chain

```
MonitoringClient          → propagates (thin utility)
  ↑ called by
quarantine_telemetry      → catches everything (safety boundary)
  ↑ called by (local import)
report_feed_failure       → doesn't handle telemetry errors (emit never raises)
  ↑ called by
_process_feed             → existing except Exception: handles DB errors
```

One safety boundary at `emit_quarantine_event`. Every other layer either propagates or has its own existing error handling.

## Custom Metric Details

- **Type:** `custom.googleapis.com/feeds/quarantine_events`
- **Kind:** GAUGE
- **Value type:** INT64
- **Value:** `1` (event signal)
- **Labels:** `feed_id` (STRING), `feed_name` (STRING), `source_type` (STRING)
- **Monitored resource:** `global`

GAUGE with value=1 is correct for a rare event signal. Cloud Monitoring alerting policies trigger on "any data point exists in the last N minutes."

## Testing Strategy

| Test file | Scope |
|-----------|-------|
| `common/clients/tests/test_monitoring_client.py` (new) | Lazy init, `create_time_series` called with correct args, errors propagate |
| `ingestion/tests/test_quarantine_telemetry.py` (new) | Structured log emitted, metric called when configured, skipped when `None`, never raises on error |
| `storage/tests/test_feed_store.py` (modify) | Add `name`/`source_type` to mock rows, verify telemetry called on quarantine, not called on failing |
| `ingestion/tests/test_settings.py` (modify) | Assert `gcp_project_id` from env, `None` when unset |
| `ingestion/tests/test_runtime.py` (modify) | Add `gcp_project_id: None` to `_make_settings` defaults |

All tests mock the GCP client. `quarantine_telemetry` tests reset `_client` in `tearDown`.

## Files Changed

| File | Action |
|------|--------|
| `backend/pipeline/common/clients/monitoring_client.py` | **Create** |
| `backend/pipeline/common/clients/tests/test_monitoring_client.py` | **Create** |
| `backend/pipeline/ingestion/quarantine_telemetry.py` | **Create** |
| `backend/pipeline/ingestion/tests/test_quarantine_telemetry.py` | **Create** |
| `backend/pipeline/ingestion/scripts/__init__.py` | **Create** |
| `backend/pipeline/ingestion/scripts/create_quarantine_metric.py` | **Create** |
| `backend/pipeline/storage/feed_store.py` | Modify (RETURNING clause + telemetry call) |
| `backend/pipeline/ingestion/settings.py` | Modify (add `gcp_project_id`) |
| `backend/pipeline/ingestion/normalizer_runtime.py` | Modify (configure call in `_main`) |
| `backend/pipeline/storage/tests/test_feed_store.py` | Modify (mock rows + telemetry tests) |
| `backend/pipeline/ingestion/tests/test_settings.py` | Modify (assert `gcp_project_id`) |
| `backend/pipeline/ingestion/tests/test_runtime.py` | Modify (add to `_make_settings`) |
