"""Pytest conftest for Broadcastify credential rotation tests.

Sets default values for all 7 required env vars BEFORE any test module in
this directory is imported. The module under test
(`backend.pipeline.ingestion.broadcastify_credential_rotation.main`) calls
`_require_env(...)` at module top, so without these defaults pytest
collection would fail with a ValueError.

Pattern: top-level `os.environ.setdefault(...)` statements at module
scope. The seven setdefault calls below are the entire mechanism — no
per-test setup hooks, no test-runtime env mutation. pytest loads
`conftest.py` before collecting test modules in the same package, so
the env lands in `os.environ` before the import-time `_require_env`
calls fire. `setdefault` (not assignment) preserves any real value a
developer has exported locally.

Note: tests that need different values (e.g., to verify JWT signing
logic) override the module-level constants in `main` via
`mock.patch.multiple(main, ...)` — see the `configured_module` helper
in test_broadcastify_credential_rotation.py. That mechanism overrides
these defaults at test time without touching `os.environ`.
"""

from __future__ import annotations

import os

os.environ.setdefault("BROADCASTIFY_USERNAME", "test_user")
os.environ.setdefault("BROADCASTIFY_PASSWORD", "test_password")
os.environ.setdefault("BROADCASTIFY_API_KEY", "test_api_key")
os.environ.setdefault("BROADCASTIFY_API_APP_ID", "test_app_id")
os.environ.setdefault("BROADCASTIFY_API_KEY_ID", "test_key_id")
os.environ.setdefault("GOOGLE_CLOUD_PROJECT", "test-project")
os.environ.setdefault("BROADCASTIFY_JWT_SECRET_ID", "test-broadcastify-jwt")
