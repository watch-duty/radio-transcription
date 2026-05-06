"""Pytest conftest for echo Cloud Run service tests.

Sets default values for required env vars BEFORE any test module in this
directory is imported. The module under test (`backend.pipeline.ingestion.
collectors.echo.main`) calls `_require_env(...)` at module top, so without
these defaults pytest collection would fail with a ValueError.

Pattern: top-level `os.environ.setdefault(...)`. The two `setdefault`
statements at the bottom of this module are the entire mechanism — no
per-test setup hooks, no test-runtime env mutation. pytest loads
`conftest.py` before collecting test modules in the same package, so
the env lands in `os.environ` before the import-time `_require_env`
calls fire. `setdefault` (not assignment) preserves any real value a
developer has exported locally.
"""

from __future__ import annotations

import os

os.environ.setdefault("AUDIO_STAGING_BUCKET", "test-staging-bucket")
os.environ.setdefault(
    "RAW_AUDIO_TOPIC", "projects/test-project/topics/raw-audio"
)
