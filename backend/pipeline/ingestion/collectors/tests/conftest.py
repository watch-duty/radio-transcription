"""Shared fixtures and helpers for collector tests.

Provides _default_resources() - a no-op CaptureResources for tests that
invoke a capture_* function directly. Phase 2 collector bodies do NOT
touch http_session or spawn_semaphore; a mock session + a real Semaphore
is sufficient. Constructing a real aiohttp.ClientSession would open real
sockets; avoid in unit tests.

Mirrors the _default_resources() helper in
backend/pipeline/ingestion/tests/test_runtime.py - kept as a separate
module-level helper rather than a pytest fixture so existing
``unittest.IsolatedAsyncioTestCase`` test classes can call it imperatively
(pytest fixtures don't auto-inject into unittest.TestCase methods without
boilerplate).
"""

from __future__ import annotations

import asyncio
from unittest import mock

import aiohttp

from backend.pipeline.ingestion.models import CaptureResources


def _default_resources() -> CaptureResources:
    """Build a no-op CaptureResources for collector tests."""
    return CaptureResources(
        http_session=mock.AsyncMock(spec=aiohttp.ClientSession),
        spawn_semaphore=asyncio.Semaphore(8),
    )
