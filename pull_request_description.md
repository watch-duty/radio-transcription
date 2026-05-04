## Description

**Summary:**
Resolves the hardcoded `.flac` fallback bug by dynamically resolving audio segment MIME-types from HTTP response headers in the ingestion collectors.

**Context & Motivation:**
During infrastructure spot checks, continuous feeds from Broadcastify Calls (`SourceType.BCFY_CALLS`) were discovered to be pushing raw MP3 and AAC continuous segments to GCS staging buckets with incorrect and misleading `.flac` file extensions due to hardcoded normalizer fallbacks. 

To solve this, we:
1. Upgraded the Ingestion Contract (`models.py`) with a strict, presence-tracking `AudioMimeType` StrEnum mapping and introduced `mime_type` in the `CapturedChunk` dataclass.
2. Intercepted the server's `Content-Type` response header out-of-band inside `bcfy_calls_collector.py` with safe `inspect.signature` adapters to preserve 100% backwards-compatibility for bare unit test mock and exception overrides.
3. Dynamically resolved extensions/content-types per chunk in `normalizer_runtime.py` during GCS upload pipelines.
4. Compiled the updated protobuf classes with the standard proto3 presence keyword `optional string session_id = 3` to enforce type safety across pub/sub wire boundaries.

**Future Work / Out of Scope:**
None. Backend pipeline code quality (`ruff`), type checks (`ty`), and test discovery pass with 100% success.

---

## How Has This Been Tested?
- [x] Unit Tests: 
  - Created `test_create_chunk_captures_mime_type` to verify out-of-band `Content-Type` capture, custom `StrEnum` parsing factories, and adapter compatibility inside test mock stubs.
  - Updated `test_session_id_none_preserved` inside `test_runtime.py` to assert raw optional values are preserved cleanly for discrete segmented feeds.
  - Executed the full pipeline unit test suite discovery (`unittest discover`) with complete success (**517 tests passed successfully**).

## Checklist
- [x] Self-review of my own code.
- [x] Commented code in hard-to-understand areas or complex logic.
- [x] Updated documentation.
- [x] Included any dependent changes that this PR is relying on in the description.
