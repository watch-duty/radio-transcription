# Collector Failure Helper Refactor Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Refactor PR #570 collector failure handling so item-download result and item-download HTTP classification are shared across Broadcastify Calls, OpenMHz, and Fire Notifications while source endpoint classification stays collector-local.

**Architecture:** Add small item-download primitives to `failure_classification.py`, then migrate each item-based collector onto them without changing collector lifecycle or source endpoint status mapping. Broadcastify Feeds/Icecast remains stream-endpoint based and should not use the item-download helpers.

**Tech Stack:** Python 3.13, dataclasses, unittest/pytest, aiohttp, curl_cffi, Ruff.

---

## File Structure

- Modify `backend/pipeline/ingestion/collectors/failure_classification.py`
  - Owns shared failure primitives: `ItemFailure`, `ItemBatchOutcome`, `ItemDownloadResult`, item-download status mapping, and raising typed collector failures.
- Modify `backend/pipeline/ingestion/collectors/tests/test_failure_classification.py`
  - Owns focused tests for shared helper behavior.
- Modify `backend/pipeline/ingestion/collectors/bcfy_calls/bcfy_calls_collector.py`
  - Uses shared `ItemDownloadResult` for per-call audio downloads.
  - Keeps Broadcastify Calls API fetch classification local.
  - Keeps `_FetchCallsResult` and `_CallChunkResult` collector-local.
- Modify `backend/pipeline/ingestion/collectors/tests/test_bcfy_calls_collector.py`
  - Updates item-download expectations from `audio_*` raw reasons to `item_*`.
  - Verifies `Content-Type` is carried through `ItemDownloadResult.content_type`.
- Modify `backend/pipeline/ingestion/collectors/fire_notifications/collector.py`
  - Uses shared `ItemDownloadResult` for listed MP3 downloads.
  - Keeps poll status classification local, renamed to `_classify_poll_status_failure`.
- Modify `backend/pipeline/ingestion/collectors/tests/test_fire_notifications_collector.py`
  - Imports shared `ItemDownloadResult`.
  - Updates terminal HTTP status expectations to preserve `item_http_<status>`.
- Modify `backend/pipeline/ingestion/collectors/openmhz/collector.py`
  - Uses shared `ItemDownloadResult` for M4A downloads.
- Modify `backend/pipeline/ingestion/collectors/tests/test_openmhz_collector.py`
  - Imports shared `ItemDownloadResult`.
  - Updates stubs from local `_DownloadResult`.

Do not modify `backend/pipeline/ingestion/collectors/icecast/icecast_collector.py` except if an import or lint failure proves necessary.

---

### Task 1: Add Shared Item-Download Primitives

**Files:**
- Modify: `backend/pipeline/ingestion/collectors/failure_classification.py`
- Test: `backend/pipeline/ingestion/collectors/tests/test_failure_classification.py`

- [ ] **Step 1: Add failing shared helper tests**

Add imports:

```python
from backend.pipeline.ingestion.collectors.failure_classification import (
    ItemBatchOutcome,
    ItemDownloadResult,
    ItemFailure,
    collector_failure,
    item_download_http_failure,
    missing_source_feed_id_failure,
    raise_item_failure,
    standardize_item_download_result,
)
```

Add tests below `TestItemBatchOutcome` or in a new `TestItemDownloadResult` class:

```python
class TestItemDownloadResult(unittest.TestCase):
    def test_accepts_success_result_with_content_type(self) -> None:
        result = ItemDownloadResult(
            audio_bytes=b"audio",
            content_type="audio/mpeg; charset=binary",
        )

        self.assertEqual(result.audio_bytes, b"audio")
        self.assertIsNone(result.failure)
        self.assertEqual(result.content_type, "audio/mpeg; charset=binary")

    def test_accepts_failure_result(self) -> None:
        failure = ItemFailure(
            FeedStatusReason.SOURCE_UNREACHABLE,
            "item_download_failed",
        )

        result = ItemDownloadResult(failure=failure)

        self.assertIsNone(result.audio_bytes)
        self.assertIs(result.failure, failure)
        self.assertIsNone(result.content_type)

    def test_accepts_empty_result_for_shutdown_compatibility(self) -> None:
        result = ItemDownloadResult()

        self.assertIsNone(result.audio_bytes)
        self.assertIsNone(result.failure)
        self.assertIsNone(result.content_type)

    def test_rejects_success_and_failure_together(self) -> None:
        failure = ItemFailure(
            FeedStatusReason.SOURCE_UNREACHABLE,
            "item_download_failed",
        )

        with self.assertRaises(ValueError) as ctx:
            ItemDownloadResult(audio_bytes=b"audio", failure=failure)

        self.assertEqual(
            str(ctx.exception),
            "ItemDownloadResult cannot contain both audio_bytes and failure",
        )

    def test_standardize_item_download_result_preserves_typed_result(self) -> None:
        result = ItemDownloadResult(audio_bytes=b"audio")

        self.assertIs(standardize_item_download_result(result), result)

    def test_standardize_item_download_result_wraps_bytes(self) -> None:
        result = standardize_item_download_result(b"audio")

        self.assertEqual(result.audio_bytes, b"audio")
        self.assertIsNone(result.failure)

    def test_standardize_item_download_result_wraps_none(self) -> None:
        result = standardize_item_download_result(None)

        self.assertIsNone(result.audio_bytes)
        self.assertIsNone(result.failure)
```

Add item HTTP mapping tests:

```python
class TestItemDownloadHttpFailure(unittest.TestCase):
    def test_auth_statuses_are_system_authentication_failed(self) -> None:
        for status in (401, 403):
            with self.subTest(status=status):
                failure = item_download_http_failure(status)

                self.assertIs(
                    failure.status_reason,
                    FeedStatusReason.SYSTEM_AUTHENTICATION_FAILED,
                )
                self.assertEqual(failure.reason, f"item_http_{status}")

    def test_rate_limit_status_is_source_rate_limited(self) -> None:
        failure = item_download_http_failure(429)

        self.assertIs(
            failure.status_reason,
            FeedStatusReason.SOURCE_RATE_LIMITED,
        )
        self.assertEqual(failure.reason, "item_http_429")

    def test_other_statuses_are_source_unreachable_with_exact_status(self) -> None:
        for status in (404, 410, 500, 503):
            with self.subTest(status=status):
                failure = item_download_http_failure(status)

                self.assertIs(
                    failure.status_reason,
                    FeedStatusReason.SOURCE_UNREACHABLE,
                )
                self.assertEqual(failure.reason, f"item_http_{status}")

    def test_custom_reason_prefix_is_supported_for_compatibility(self) -> None:
        failure = item_download_http_failure(404, reason_prefix="custom_http")

        self.assertIs(
            failure.status_reason,
            FeedStatusReason.SOURCE_UNREACHABLE,
        )
        self.assertEqual(failure.reason, "custom_http_404")

    def test_raise_item_failure_preserves_status_and_reason(self) -> None:
        failure = ItemFailure(
            FeedStatusReason.SOURCE_UNREACHABLE,
            "item_http_503",
        )

        with self.assertRaises(CollectorFailure) as ctx:
            raise_item_failure(failure)

        self.assertIs(
            ctx.exception.status_reason,
            FeedStatusReason.SOURCE_UNREACHABLE,
        )
        self.assertEqual(str(ctx.exception), "item_http_503")
```

- [ ] **Step 2: Run the shared tests and verify they fail**

Run:

```bash
safe-run -- uv run pytest backend/pipeline/ingestion/collectors/tests/test_failure_classification.py -q
```

Expected: FAIL because `ItemDownloadResult`, `standardize_item_download_result`, `item_download_http_failure`, and `raise_item_failure` are not defined.

- [ ] **Step 3: Implement shared helpers**

In `failure_classification.py`, add `NoReturn` import:

```python
from typing import NoReturn
```

Add after `ItemFailure`:

```python
@dataclasses.dataclass(frozen=True)
class ItemDownloadResult:
    """Classified result of downloading one discrete audio item."""

    audio_bytes: bytes | None = None
    failure: ItemFailure | None = None
    content_type: str | None = None

    def __post_init__(self) -> None:
        if self.audio_bytes is not None and self.failure is not None:
            msg = "ItemDownloadResult cannot contain both audio_bytes and failure"
            raise ValueError(msg)
```

Add near the other helper functions:

```python
def standardize_item_download_result(
    result: ItemDownloadResult | bytes | None,
) -> ItemDownloadResult:
    """Adapt legacy item-download test doubles into the typed result."""
    if isinstance(result, ItemDownloadResult):
        return result
    if isinstance(result, bytes):
        return ItemDownloadResult(audio_bytes=result)
    return ItemDownloadResult()


def item_download_http_failure(
    status: int,
    *,
    reason_prefix: str = "item_http",
) -> ItemFailure:
    """Classify terminal HTTP status for one discrete audio item download."""
    reason = f"{reason_prefix}_{status}"
    if status in {401, 403}:
        return ItemFailure(
            FeedStatusReason.SYSTEM_AUTHENTICATION_FAILED,
            reason,
        )
    if status == 429:
        return ItemFailure(FeedStatusReason.SOURCE_RATE_LIMITED, reason)
    return ItemFailure(FeedStatusReason.SOURCE_UNREACHABLE, reason)


def raise_item_failure(failure: ItemFailure) -> NoReturn:
    """Raise a typed collector failure from a promoted item failure."""
    raise collector_failure(failure.status_reason, failure.reason)
```

- [ ] **Step 4: Run the shared tests and verify they pass**

Run:

```bash
safe-run -- uv run pytest backend/pipeline/ingestion/collectors/tests/test_failure_classification.py -q
```

Expected: PASS.

- [ ] **Step 5: Commit shared helper changes**

Run:

```bash
git add backend/pipeline/ingestion/collectors/failure_classification.py backend/pipeline/ingestion/collectors/tests/test_failure_classification.py
git commit -m "feat: add shared item download failure helpers"
```

---

### Task 2: Migrate Broadcastify Calls Item Downloads

**Files:**
- Modify: `backend/pipeline/ingestion/collectors/bcfy_calls/bcfy_calls_collector.py`
- Test: `backend/pipeline/ingestion/collectors/tests/test_bcfy_calls_collector.py`

- [ ] **Step 1: Update failing Broadcastify Calls tests**

Update imports in `test_bcfy_calls_collector.py`:

```python
from backend.pipeline.ingestion.collectors.failure_classification import (
    ItemDownloadResult,
    ItemFailure,
)
```

Update raw reason assertions:

```python
self.assertEqual(failure.reason, "item_download_failed")
self.assertEqual(failure.reason, "item_http_429")
```

Replace test stubs that return `ItemFailure` or bytes from `_download_audio` only where the test is specifically exercising the typed item-download path. Keep legacy byte/None stubs in `_create_chunk_from_call` tests to verify `standardize_item_download_result` compatibility.

Replace `test_create_chunk_captures_mime_type` side effect with:

```python
mock_dl.return_value = ItemDownloadResult(
    audio_bytes=b"mpeg_bytes",
    content_type="audio/mpeg",
)
```

Add a direct `_download_audio` test for content type:

```python
async def test_download_audio_success_preserves_content_type(self) -> None:
    resp = AsyncMock(status=200)
    resp.read.return_value = b"mpeg_bytes"
    resp.headers = {"Content-Type": "audio/mpeg"}
    cm = MagicMock()
    cm.__aenter__ = AsyncMock(return_value=resp)
    cm.__aexit__ = AsyncMock(return_value=False)
    self.session.get.return_value = cm

    result = await bcfy_calls_collector._download_audio(
        self.session,
        "https://audio.example/test.mp3",
        self.shutdown,
    )

    self.assertEqual(result.audio_bytes, b"mpeg_bytes")
    self.assertEqual(result.content_type, "audio/mpeg")
    self.assertIsNone(result.failure)
```

- [ ] **Step 2: Run Broadcastify Calls tests and verify they fail**

Run:

```bash
safe-run -- uv run pytest backend/pipeline/ingestion/collectors/tests/test_bcfy_calls_collector.py -q
```

Expected: FAIL because `_download_audio` still returns `bytes | ItemFailure | None`, raw reasons still use `audio_*`, and `out_headers` is still the MIME side channel.

- [ ] **Step 3: Update Broadcastify Calls imports and local names**

In `bcfy_calls_collector.py`, update failure imports:

```python
from backend.pipeline.ingestion.collectors.failure_classification import (
    ItemBatchOutcome,
    ItemDownloadResult,
    ItemFailure,
    collector_failure,
    item_download_http_failure,
    missing_source_feed_id_failure,
    raise_item_failure,
    standardize_item_download_result,
)
```

Rename:

```python
def _normalize_fetch_result(...)
```

to:

```python
def _standardize_fetch_result(...)
```

Rename:

```python
def _normalize_call_chunk_result(...)
```

to:

```python
def _standardize_call_chunk_result(...)
```

Update both call sites in `capture_bcfy_calls`.

Delete local `_raise_item_failure`; use imported `raise_item_failure`.

- [ ] **Step 4: Refactor `_download_audio` to return `ItemDownloadResult`**

Change the signature:

```python
async def _download_audio(  # noqa: PLR0911, PLR0912
    session: aiohttp.ClientSession,
    audio_url: str,
    shutdown_event: asyncio.Event,
) -> ItemDownloadResult:
```

Remove `out_headers`. On success:

```python
return ItemDownloadResult(
    audio_bytes=await audio_resp.read(),
    content_type=audio_resp.headers.get("Content-Type"),
)
```

For auth failures:

```python
return ItemDownloadResult(
    failure=item_download_http_failure(audio_resp.status)
)
```

For terminal 429:

```python
return ItemDownloadResult(
    failure=item_download_http_failure(audio_resp.status)
)
```

For terminal 5xx after retries:

```python
return ItemDownloadResult(
    failure=item_download_http_failure(audio_resp.status)
)
```

For other non-200:

```python
return ItemDownloadResult(
    failure=item_download_http_failure(audio_resp.status)
)
```

For shutdown during backoff:

```python
return ItemDownloadResult()
```

For network exception exhaustion:

```python
return ItemDownloadResult(
    failure=ItemFailure(
        FeedStatusReason.SOURCE_UNREACHABLE,
        "item_download_failed",
    )
)
```

At function fallthrough:

```python
return ItemDownloadResult()
```

- [ ] **Step 5: Refactor `_create_chunk_from_call` to use `ItemDownloadResult`**

Remove `out_headers` from the signature and body.

Replace the download call and handling with:

```python
download_result = standardize_item_download_result(
    await _download_audio(session, audio_url, shutdown_event)
)
if download_result.failure is not None:
    return _CallChunkResult(failure=download_result.failure)

audio_bytes = download_result.audio_bytes
if not audio_bytes:
    return _CallChunkResult()
```

Replace MIME detection with:

```python
mime_type = AudioMimeType.from_string(download_result.content_type)
```

In the broad exception handler, update the raw reason:

```python
return _CallChunkResult(
    failure=ItemFailure(
        FeedStatusReason.SOURCE_UNREACHABLE,
        "item_download_failed",
    )
)
```

- [ ] **Step 6: Run Broadcastify Calls tests**

Run:

```bash
safe-run -- uv run pytest backend/pipeline/ingestion/collectors/tests/test_bcfy_calls_collector.py -q
```

Expected: PASS.

- [ ] **Step 7: Commit Broadcastify Calls migration**

Run:

```bash
git add backend/pipeline/ingestion/collectors/bcfy_calls/bcfy_calls_collector.py backend/pipeline/ingestion/collectors/tests/test_bcfy_calls_collector.py
git commit -m "refactor: share broadcastify calls item download results"
```

---

### Task 3: Migrate Fire Notifications Item Downloads

**Files:**
- Modify: `backend/pipeline/ingestion/collectors/fire_notifications/collector.py`
- Test: `backend/pipeline/ingestion/collectors/tests/test_fire_notifications_collector.py`

- [ ] **Step 1: Update failing Fire Notifications tests**

In `test_fire_notifications_collector.py`, import shared `ItemDownloadResult`:

```python
from backend.pipeline.ingestion.collectors.failure_classification import (
    ItemDownloadResult,
    ItemFailure,
)
```

Replace `collector._DownloadResult(...)` with `ItemDownloadResult(...)`.

Update `test_non_retryable_4xx` expected raw reason:

```python
self.assertEqual(failure.reason, "item_http_404")
```

Add a test proving HTTP 5xx retry exhaustion preserves status:

```python
@patch(
    "backend.pipeline.ingestion.collectors.fire_notifications.collector._sleep_or_shutdown",
    new_callable=AsyncMock,
)
async def test_5xx_max_retries_preserves_http_status(
    self, mock_sleep: MagicMock
) -> None:
    mock_sleep.return_value = False
    resp503 = MagicMock(status_code=503)
    self.session.get = AsyncMock(return_value=resp503)

    result = await collector._download_audio(
        self.session, "http://url", self.shutdown
    )

    self.assertIsNone(result.audio_bytes)
    failure = _require_item_failure(result.failure)
    self.assertIs(
        failure.status_reason,
        FeedStatusReason.SOURCE_UNREACHABLE,
    )
    self.assertEqual(failure.reason, "item_http_503")
```

Rename poll helper references if any tests call it directly:

```python
collector._classify_poll_status_failure(...)
```

- [ ] **Step 2: Run Fire Notifications tests and verify they fail**

Run:

```bash
safe-run -- uv run pytest backend/pipeline/ingestion/collectors/tests/test_fire_notifications_collector.py -q
```

Expected: FAIL because the collector still defines local `_DownloadResult`, uses generic `item_download_failed` for 404/5xx, and has `_poll_status_failure`.

- [ ] **Step 3: Update Fire Notifications imports and remove local result type**

In `collector.py`, remove `import dataclasses` if unused after deletion.

Update imports:

```python
from backend.pipeline.ingestion.collectors.failure_classification import (
    ItemBatchOutcome,
    ItemDownloadResult,
    ItemFailure,
    collector_failure,
    item_download_http_failure,
    missing_source_feed_id_failure,
    raise_item_failure,
    standardize_item_download_result,
)
```

Delete local `_DownloadResult`.

Rename:

```python
def _poll_status_failure(status: int) -> ItemFailure:
```

to:

```python
def _classify_poll_status_failure(status: int) -> ItemFailure:
```

Keep its mapping unchanged.

- [ ] **Step 4: Refactor `_download_audio`**

Change return type:

```python
) -> ItemDownloadResult:
```

On success:

```python
return ItemDownloadResult(audio_bytes=resp.content)
```

For 401/403, 429, and non-retryable 4xx:

```python
return ItemDownloadResult(
    failure=item_download_http_failure(resp.status_code)
)
```

For terminal 5xx/non-200 after retry exhaustion, preserve the final HTTP status:

```python
return ItemDownloadResult(
    failure=item_download_http_failure(resp.status_code)
)
```

For shutdown-interrupted backoff:

```python
return ItemDownloadResult()
```

For network exception exhaustion with no HTTP status:

```python
return ItemDownloadResult(
    failure=ItemFailure(
        FeedStatusReason.SOURCE_UNREACHABLE,
        "item_download_failed",
    )
)
```

- [ ] **Step 5: Refactor file-list processing**

Replace:

```python
download_result = _normalize_download_result(
    await _download_audio(session, s3_url, shutdown_event)
)
```

with:

```python
download_result = standardize_item_download_result(
    await _download_audio(session, s3_url, shutdown_event)
)
```

Delete `_normalize_download_result`.

Replace `_raise_item_failure(promoted)` with:

```python
raise_item_failure(promoted)
```

Replace poll call site:

```python
poll_failure = _classify_poll_status_failure(resp.status_code)
```

- [ ] **Step 6: Run Fire Notifications tests**

Run:

```bash
safe-run -- uv run pytest backend/pipeline/ingestion/collectors/tests/test_fire_notifications_collector.py -q
```

Expected: PASS.

- [ ] **Step 7: Commit Fire Notifications migration**

Run:

```bash
git add backend/pipeline/ingestion/collectors/fire_notifications/collector.py backend/pipeline/ingestion/collectors/tests/test_fire_notifications_collector.py
git commit -m "refactor: share fire notifications item download results"
```

---

### Task 4: Migrate OpenMHz Item Downloads

**Files:**
- Modify: `backend/pipeline/ingestion/collectors/openmhz/collector.py`
- Test: `backend/pipeline/ingestion/collectors/tests/test_openmhz_collector.py`

- [ ] **Step 1: Update failing OpenMHz tests**

In `test_openmhz_collector.py`, replace the collector-local import:

```python
from backend.pipeline.ingestion.collectors.openmhz.collector import (
    MAX_ITEM_DOWNLOAD_FAILURES,
    MAX_RECONNECT_FAILURES,
    openmhz_collector,
)
from backend.pipeline.ingestion.collectors.failure_classification import (
    ItemDownloadResult,
    ItemFailure,
)
```

Replace `_DownloadResult(...)` with `ItemDownloadResult(...)`.

Search this file for `_DownloadResult(` and replace each test double with
`ItemDownloadResult(`. The existing OpenMHz raw reason assertions already use
`item_download_failed` and `item_http_429`; leave those assertions unchanged
unless the implementation change creates a precise terminal HTTP status test.

- [ ] **Step 2: Run OpenMHz tests and verify they fail**

Run:

```bash
safe-run -- uv run pytest backend/pipeline/ingestion/collectors/tests/test_openmhz_collector.py -q
```

Expected: FAIL because `_DownloadResult` is still local and the collector still uses `_normalize_download_result`.

- [ ] **Step 3: Update OpenMHz imports and remove local result type**

In `collector.py`, remove `import dataclasses` if no longer used.

Update imports:

```python
from backend.pipeline.ingestion.collectors.failure_classification import (
    ItemBatchOutcome,
    ItemDownloadResult,
    ItemFailure,
    collector_failure,
    item_download_http_failure,
    missing_source_feed_id_failure,
    raise_item_failure,
    standardize_item_download_result,
)
```

Delete local `_DownloadResult`.

- [ ] **Step 4: Refactor `_download_m4a`**

Change return type:

```python
) -> ItemDownloadResult:
```

On success:

```python
return ItemDownloadResult(audio_bytes=resp.content)
```

For 401/403, 429, and non-retryable 4xx:

```python
return ItemDownloadResult(
    failure=item_download_http_failure(resp.status_code)
)
```

For terminal 5xx/non-200 after retry exhaustion:

```python
return ItemDownloadResult(
    failure=item_download_http_failure(resp.status_code)
)
```

For shutdown-interrupted backoff:

```python
return ItemDownloadResult()
```

For network exception exhaustion without HTTP status:

```python
return ItemDownloadResult(
    failure=ItemFailure(
        FeedStatusReason.SOURCE_UNREACHABLE,
        "item_download_failed",
    )
)
```

- [ ] **Step 5: Refactor OpenMHz caller**

Replace:

```python
download_result = _normalize_download_result(
    await _download_m4a(download_session, call.url, shutdown_event)
)
```

with:

```python
download_result = standardize_item_download_result(
    await _download_m4a(download_session, call.url, shutdown_event)
)
```

Delete `_normalize_download_result`.

Replace `_raise_item_failure(pending_item_failure)` with:

```python
raise_item_failure(pending_item_failure)
```

Delete local `_raise_item_failure`.

- [ ] **Step 6: Run OpenMHz tests**

Run:

```bash
safe-run -- uv run pytest backend/pipeline/ingestion/collectors/tests/test_openmhz_collector.py -q
```

Expected: PASS.

- [ ] **Step 7: Commit OpenMHz migration**

Run:

```bash
git add backend/pipeline/ingestion/collectors/openmhz/collector.py backend/pipeline/ingestion/collectors/tests/test_openmhz_collector.py
git commit -m "refactor: share openmhz item download results"
```

---

### Task 5: Run Cross-Collector Verification and Clean Up

**Files:**
- Modify only if verification reveals a necessary import, lint, or test expectation fix.

- [ ] **Step 1: Run focused collector tests**

Run:

```bash
safe-run -- uv run pytest \
  backend/pipeline/ingestion/collectors/tests/test_failure_classification.py \
  backend/pipeline/ingestion/collectors/tests/test_bcfy_calls_collector.py \
  backend/pipeline/ingestion/collectors/tests/test_fire_notifications_collector.py \
  backend/pipeline/ingestion/collectors/tests/test_openmhz_collector.py \
  backend/pipeline/ingestion/tests/test_slo_contract_lint.py \
  -q
```

Expected: PASS.

- [ ] **Step 2: Run focused Ruff checks**

Run:

```bash
safe-run -- uv run ruff check \
  backend/pipeline/ingestion/collectors/failure_classification.py \
  backend/pipeline/ingestion/collectors/bcfy_calls/bcfy_calls_collector.py \
  backend/pipeline/ingestion/collectors/fire_notifications/collector.py \
  backend/pipeline/ingestion/collectors/openmhz/collector.py \
  backend/pipeline/ingestion/collectors/tests/test_failure_classification.py \
  backend/pipeline/ingestion/collectors/tests/test_bcfy_calls_collector.py \
  backend/pipeline/ingestion/collectors/tests/test_fire_notifications_collector.py \
  backend/pipeline/ingestion/collectors/tests/test_openmhz_collector.py
```

Expected: PASS.

- [ ] **Step 3: Search for stale local helper names and raw reasons**

Run:

```bash
rg -n "_DownloadResult|_normalize_download_result|_raise_item_failure|audio_http_|audio_download_failed|out_headers" backend/pipeline/ingestion/collectors
```

Expected:

- No `_DownloadResult` in OpenMHz or Fire Notifications.
- No `_normalize_download_result`.
- No `_raise_item_failure` in item-based collectors.
- No `audio_http_` or `audio_download_failed`.
- No `out_headers` in Broadcastify Calls item download flow.

It is acceptable for `_standardize_fetch_result` and `_standardize_call_chunk_result` to remain in Broadcastify Calls.

- [ ] **Step 4: Run formatting if Ruff requests it**

Run only if Ruff reports formatting/import order issues:

```bash
safe-run -- uv run ruff format \
  backend/pipeline/ingestion/collectors/failure_classification.py \
  backend/pipeline/ingestion/collectors/bcfy_calls/bcfy_calls_collector.py \
  backend/pipeline/ingestion/collectors/fire_notifications/collector.py \
  backend/pipeline/ingestion/collectors/openmhz/collector.py \
  backend/pipeline/ingestion/collectors/tests/test_failure_classification.py \
  backend/pipeline/ingestion/collectors/tests/test_bcfy_calls_collector.py \
  backend/pipeline/ingestion/collectors/tests/test_fire_notifications_collector.py \
  backend/pipeline/ingestion/collectors/tests/test_openmhz_collector.py
```

Expected: files are formatted. Re-run the focused tests and Ruff checks afterward.

- [ ] **Step 5: Commit cleanup**

If any cleanup changes were required, run:

```bash
git add backend/pipeline/ingestion/collectors backend/pipeline/ingestion/tests
git commit -m "test: verify shared item download helpers"
```

If no files changed, do not create an empty commit.

---

## Self-Review Checklist

- Spec coverage:
  - Shared item-download result: Task 1.
  - Content type metadata: Task 2.
  - Shared item HTTP mapping: Tasks 1-4.
  - Source API/poll/stream classification remains local: Tasks 2-4 and Task 5 stale-search.
  - Broadcastify raw reason migration: Task 2.
  - Fire poll classifier local rename: Task 3.
  - OpenMHz item failure window unchanged: Task 4.
  - `ItemBatchOutcome` promotion unchanged: no implementation task changes it; Task 5 checks focused tests.
- Placeholder scan:
  - No TBD/TODO implementation steps.
  - Each code-changing step includes the concrete snippet or exact replacement target.
- Type consistency:
  - Shared type is `ItemDownloadResult`.
  - Shared adapter is `standardize_item_download_result`.
  - Shared status helper is `item_download_http_failure`.
  - Shared raiser is `raise_item_failure`.
