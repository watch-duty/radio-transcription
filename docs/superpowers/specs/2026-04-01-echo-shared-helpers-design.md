# Extract Shared Helpers from Echo Ingestion Handler

## Problem

The echo ingestion handler (`backend/pipeline/ingestion/collectors/echo/main.py`) contains inline implementations of database connections, SQL queries, and Pub/Sub publishing that:

1. Duplicate patterns already in shared modules (`AlloyDBSettings`, `gcp_helper.publish_audio_chunk`)
2. Place storage-level concerns (SQL, connection lifecycle, failure backoff config) in the ingestion layer
3. Make it harder to reuse these operations for future sync collectors

## Principle

The storage layer is an abstraction boundary. Ingestion logic calls store methods (`resolve_feed`, `record_heartbeat`, `record_failure`) without knowing about SQL, psycopg, connections, or backoff formulas. This makes it easy to swap storage implementations in the future.

## Design

### 1. SyncFeedStore

**File:** `backend/pipeline/storage/sync_feed_store.py`

A sync counterpart to the existing async `FeedStore`. Owns all SQL, connection lifecycle, and failure configuration.

```
SyncFeedStore(connect_db, failure_threshold=5, base_backoff_sec=15, max_backoff_sec=600)
  .resolve_echo_feed(channel_name) -> dict[str, Any] | None
  .record_heartbeat(feed_id: UUID) -> None
  .record_failure(feed_id: UUID) -> None
```

**Constructor parameters:**
- `connect_db: Callable[[], psycopg.Connection[dict[str, Any]]]` — connection factory, injected for testability
- `failure_threshold: int = 5` — failures before quarantine
- `base_backoff_sec: int = 15` — base for exponential backoff
- `max_backoff_sec: int = 600` — backoff cap

**Generic methods** (source-agnostic, operate on `feeds` table by ID):
- `record_heartbeat(feed_id)` — sets `last_heartbeat=NOW()`, resets `failure_count` and `status` if recovering from failure
- `record_failure(feed_id)` — increments `failure_count`, sets exponential backoff `retry_after`, quarantines at threshold

**Source-specific resolution** (each JOINs the relevant properties table):
- `resolve_echo_feed(channel_name)` — JOINs `feed_properties_echo`, returns `{id, status, failure_count}` or `None`
- Future source types add their own resolve method (e.g., `resolve_xyz_feed(...)`)

All SQL constants are private to this module. The handler never sees SQL.

### 2. Sync connect_db

**File:** `backend/pipeline/storage/connection.py` (add to existing)

Sync companion to `create_pool_from_settings`. Reuses `AlloyDBSettings` from `storage/settings.py`.

```
connect_db(settings: AlloyDBSettings | None = None) -> psycopg.Connection[dict[str, Any]]
```

If `settings` is `None`, constructs `AlloyDBSettings()` which reads from environment variables. Returns a psycopg v3 connection with `autocommit=True` and `dict_row`.

Exported from `storage/__init__.py`.

### 3. Consolidated publish_audio_chunk_sync

**File:** `backend/pipeline/common/gcp_helper.py` (add to existing)

Sync core function that builds the AudioChunk protobuf, publishes with ordering key, attaches the `_resume_on_err` done-callback, and blocks on `future.result()`.

```
publish_audio_chunk_sync(
    publisher, topic_path, feed_id, gcs_uri,
    session_id, start_timestamp, source_type=None
) -> str
```

The existing async `publish_audio_chunk` is refactored to wrap this via `asyncio.to_thread`. Its signature is unchanged — the normalizer runtime caller requires no changes.

This eliminates the duplicate `_resume_on_err` pattern that currently exists in both `gcp_helper.py` and echo `main.py`.

### 4. Echo handler changes

**File:** `backend/pipeline/ingestion/collectors/echo/main.py`

**Removed from handler:**
- `_connect_db()` — replaced by `storage.connection.connect_db`
- `_RESOLVE_FEED_SQL`, `_HEARTBEAT_SQL`, `_RECORD_FAILURE_SQL` — moved to `SyncFeedStore`
- `_publish_audio_chunk()` — replaced by `gcp_helper.publish_audio_chunk_sync`
- `ALLOYDB_HOST/PORT/USER/DB/PASSWORD` — handled by `AlloyDBSettings`
- `FAILURE_THRESHOLD`, `BASE_BACKOFF_SEC`, `MAX_BACKOFF_SEC` — constructor defaults on `SyncFeedStore`
- `psycopg`, `dict_row` imports — no longer used directly

**Stays in handler** (echo-specific, no duplication):
- `_convert_to_flac()` — only MP3-to-FLAC conversion in codebase
- `_parse_timestamp()` — echo filename format is unique
- `CANONICAL_BUCKET`, `RAW_AUDIO_TOPIC` — GCS/Pub/Sub config
- `TARGET_SAMPLE_WIDTH` — audio processing constant
- `_handle()` orchestration logic

**Globals become:**
```python
gcs_client: storage.Client | None = None
pubsub_client: PubSubClient | None = None
feed_store: SyncFeedStore | None = None
```

All lazy-initialized in `handle_notification`. `feed_store` is constructed with `connect_db` (no args — reads env vars via `AlloyDBSettings`).

**Handler flow after refactoring:**
```python
feed = feed_store.resolve_echo_feed(channel_name)
# ... guards (unknown, deactivated, quarantined, malformed filename) ...
try:
    mp3_bytes = gcs_client.bucket(bucket).blob(name).download_as_bytes()
    flac_bytes = _convert_to_flac(mp3_bytes)
    blob.upload_from_string(flac_bytes, ...)
    session_id = str(uuid.uuid5(uuid.NAMESPACE_URL, canonical_uri))
    publish_audio_chunk_sync(publisher, RAW_AUDIO_TOPIC, feed_id, ...)
    feed_store.record_heartbeat(feed["id"])
except Exception:
    try:
        feed_store.record_failure(feed["id"])
    except Exception:
        logger.exception(...)
    raise
```

### 5. What does NOT change

- `FeedStore` (async) — stays as-is, used by normalizer runtime
- `AlloyDBSettings` — reused as-is
- `create_pool` / `create_pool_from_settings` — async pool functions unchanged
- `normalizer_runtime.py` — still calls async `publish_audio_chunk` (signature unchanged)
- `PubSubClient` — echo still uses `pubsub_client.get_publisher()`
- `constants.py` — echo still imports `AUDIO_SAMPLE_RATE`, `NUM_AUDIO_CHANNELS`
- Terraform — no infrastructure changes

### 6. Testing

**Echo unit tests** (`tests/test_main.py`):
- Mock `SyncFeedStore` methods instead of raw connections/cursors
- `_set_feed` becomes `mock_store.resolve_echo_feed.return_value = {...}`
- Heartbeat assertions: `mock_store.record_heartbeat.assert_called_once_with(feed_id)`
- Failure assertions: `mock_store.record_failure.assert_called_once_with(feed_id)`

**Echo integration tests** (`tests/test_echo_collector_integration.py`):
- Construct real `SyncFeedStore` with `connect_db(test_settings)` as connection factory
- Patch as `feed_store` global

**New: SyncFeedStore unit tests** (`storage/tests/test_sync_feed_store.py`):
- Mock connection factory
- Verify SQL execution and parameter ordering for each method
- Test resolve returns dict or None
- Test failure threshold triggers quarantine status

**New: connect_db unit test** (`storage/tests/test_connection_sync.py`):
- Verify `psycopg.connect` called with correct args from `AlloyDBSettings`

**Updated: gcp_helper tests** (`common/tests/test_gcp_helper.py`):
- New tests for `publish_audio_chunk_sync` (direct sync call)
- Existing async tests verify delegation to sync core via `asyncio.to_thread`

## Files summary

| File | Change |
|------|--------|
| `backend/pipeline/storage/sync_feed_store.py` | **New** — SyncFeedStore class, SQL constants |
| `backend/pipeline/storage/connection.py` | Add sync `connect_db` |
| `backend/pipeline/storage/__init__.py` | Export `connect_db`, `SyncFeedStore` |
| `backend/pipeline/common/gcp_helper.py` | Add `publish_audio_chunk_sync`, refactor async wrapper |
| `backend/pipeline/ingestion/collectors/echo/main.py` | Remove inline DB/publish code, use shared modules |
| `backend/pipeline/ingestion/collectors/echo/tests/test_main.py` | Mock SyncFeedStore instead of connections |
| `backend/pipeline/ingestion/collectors/echo/tests/test_echo_collector_integration.py` | Use real SyncFeedStore with test DB |
| `backend/pipeline/common/tests/test_gcp_helper.py` | Add sync publish tests |
| `backend/pipeline/storage/tests/test_sync_feed_store.py` | **New** — SyncFeedStore unit tests |
| `backend/pipeline/storage/tests/test_connection_sync.py` | **New** — connect_db unit test |
