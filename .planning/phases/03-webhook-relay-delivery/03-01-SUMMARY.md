---
phase: 03-webhook-relay-delivery
plan: 01
subsystem: relay
tags: [fastapi, cloud-run, settings, packaging, no-db-coupling]
requires:
  - phase: 03-webhook-relay-delivery
    provides: Phase 3 relay implementation plans
provides:
  - Public `backend/pipeline/feed_audit_webhook` service scaffold
  - Runtime settings contract for `WD_BACKEND_BASE_URL` and `WD_BACKEND_API_KEY`
  - Docker/Uvicorn packaging for the relay Cloud Run service
  - Source guardrail preventing storage and database coupling
affects: [phase-03, public-repo]
tech-stack:
  added: [FastAPI, Uvicorn, urllib3]
  patterns: [startup settings validation, stateless relay service, source-inspection coupling tests]
key-files:
  created:
    - backend/pipeline/feed_audit_webhook/README.md
    - backend/pipeline/feed_audit_webhook/__init__.py
    - backend/pipeline/feed_audit_webhook/main.py
    - backend/pipeline/feed_audit_webhook/settings.py
    - backend/pipeline/feed_audit_webhook/pyproject.toml
    - backend/pipeline/feed_audit_webhook/Dockerfile
    - backend/pipeline/feed_audit_webhook/tests/test_settings.py
    - backend/pipeline/feed_audit_webhook/tests/test_no_db_coupling.py
  modified:
    - pyproject.toml
    - uv.lock
key-decisions:
  - "Use `backend/pipeline/feed_audit_webhook` with package/service name `feed-audit-webhook`."
  - "Keep the WD webhook path fixed in code and configure only base URL plus API key."
  - "Fail startup on malformed relay configuration instead of retrying every Pub/Sub message with bad config."
patterns-established:
  - "The relay package must not import storage modules, asyncpg, psycopg, or AlloyDB helpers."
  - "The relay Cloud Run image starts `backend.pipeline.feed_audit_webhook.main:app` with a single Uvicorn worker."
requirements-completed:
  - RELAY-01
  - RELAY-05
  - RELAY-06
duration: 8min
completed: 2026-06-27
---

# Phase 03 Plan 01: Relay Scaffold Summary

**Public Feed Audit Webhook relay scaffold and configuration contract**

## Accomplishments

- Added the public `backend/pipeline/feed_audit_webhook` package with a FastAPI entrypoint and `POST /pubsub/feed-audit-notifications` route scaffold.
- Added startup settings validation for `WD_BACKEND_BASE_URL` and `WD_BACKEND_API_KEY`, with the Watch Duty audit webhook path fixed in code.
- Registered the package in the root uv workspace and added a Dockerfile that runs Uvicorn on port 8080.
- Documented the relay contract and added a source-inspection test to keep the relay decoupled from AlloyDB/storage code.

## Task Commits

1. `f9b78691` - `feat: add feed audit webhook relay scaffold`

## Verification

- `safe-run -- uv run ruff format backend/pipeline/feed_audit_webhook`
- `safe-run -- uv run python -m pytest backend/pipeline/feed_audit_webhook/tests/test_settings.py backend/pipeline/feed_audit_webhook/tests/test_no_db_coupling.py -q`
- `safe-run -- python3 -m py_compile backend/pipeline/feed_audit_webhook/main.py backend/pipeline/feed_audit_webhook/settings.py`
- `safe-run -- uv lock`

## Deviations From Plan

None.

## User Setup Required

None for this scaffold. Deployment wiring follows in Plan 03-04.
