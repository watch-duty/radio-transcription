"""Shared fixtures and helpers for collector tests.

Provides _default_resources() - a no-op CaptureResources for tests that
invoke a capture_* function directly. A mock session is sufficient;
constructing a real aiohttp.ClientSession would open real sockets,
which we want to avoid in unit tests.

Mirrors the _default_resources() helper in
backend/pipeline/ingestion/tests/test_collector_runtime.py - kept as a separate
module-level helper rather than a pytest fixture so existing
``unittest.IsolatedAsyncioTestCase`` test classes can call it imperatively
(pytest fixtures don't auto-inject into unittest.TestCase methods without
boilerplate).
"""

from __future__ import annotations

from unittest import mock

import aiohttp

from backend.pipeline.ingestion.models import (
    CapturedChunk,
    CaptureEvent,
    CaptureResources,
)


def _default_resources() -> CaptureResources:
    """Build a no-op CaptureResources for collector tests."""
    return CaptureResources(
        http_session=mock.AsyncMock(spec=aiohttp.ClientSession),
    )


def _require_captured_chunk(event: CaptureEvent) -> CapturedChunk:
    """Return an audio chunk for tests that intentionally expect one."""
    if not isinstance(event, CapturedChunk):
        msg = f"Expected CapturedChunk, got {event!r}"
        raise TypeError(msg)
    return event
