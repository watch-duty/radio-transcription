---
phase: 03-webhook-relay-delivery
plan: 02
subsystem: relay
tags: [pubsub, cloud-logging, validation, fastapi]
requires:
  - phase: 03-webhook-relay-delivery
    provides: Plan 03-01 relay scaffold
provides:
  - Pub/Sub push envelope decoding for Cloud Logging LogEntry payloads
  - Shared Feed Audit Notification contract constants
  - v1 shallow notification validation
  - Endpoint parser wiring with non-2xx malformed-message behavior
affects: [phase-03, public-repo]
tech-stack:
  added: []
  patterns: [shared notification contract, strict envelope parsing, injectable delivery handler]
key-files:
  created:
    - backend/pipeline/common/feed_audit_notification_contract.py
    - backend/pipeline/feed_audit_webhook/pubsub.py
    - backend/pipeline/feed_audit_webhook/tests/test_pubsub.py
  modified:
    - backend/pipeline/feed_audit_webhook/README.md
    - backend/pipeline/feed_audit_webhook/main.py
    - backend/pipeline/storage/feed_audit_notifications.py
key-decisions:
  - "Accept only Pub/Sub push envelopes whose `message.data` decodes to a Cloud Logging LogEntry."
  - "Extract object `jsonPayload`, validate only the v1 contract, and forward the flat payload unchanged."
  - "Share Feed Audit Notification event type, schema version, and required fields through `backend.pipeline.common`."
patterns-established:
  - "Relay validation must stay shallow and avoid action-specific snapshot schema knowledge."
  - "Malformed Pub/Sub messages return non-2xx; valid messages proceed to an injectable delivery handler."
requirements-completed:
  - RELAY-01
  - RELAY-02
  - RELAY-05
  - RELAY-06
duration: 12min
completed: 2026-06-27
---

# Phase 03 Plan 02: Pub/Sub Parser Summary

**Strict Pub/Sub push parsing for Feed Audit Notification log entries**

## Accomplishments

- Added `pubsub.py` to decode Pub/Sub push envelopes, base64-decode Cloud Logging `LogEntry` data, extract object `jsonPayload`, and validate the Feed Audit Notification v1 contract.
- Added shared contract constants in `backend.pipeline.common.feed_audit_notification_contract` and reused them from the producer helper and relay parser.
- Wired the FastAPI route to reject malformed messages with HTTP 400 and pass valid payloads unchanged to an injectable downstream handler.
- Added parser and endpoint tests covering malformed envelopes, invalid decoded JSON, missing/non-object `jsonPayload`, wrong event contract, preserved extra fields, and endpoint NACK behavior.

## Task Commits

1. `70168c1e` - `feat: parse feed audit pubsub messages`

## Verification

- `safe-run -- uv run ruff format backend/pipeline/feed_audit_webhook backend/pipeline/common/feed_audit_notification_contract.py backend/pipeline/storage/feed_audit_notifications.py`
- `safe-run -- uv run python -m pytest backend/pipeline/feed_audit_webhook/tests backend/pipeline/storage/tests/test_feed_audit_notifications.py -q`
- `safe-run -- python3 -m py_compile backend/pipeline/feed_audit_webhook/main.py backend/pipeline/feed_audit_webhook/pubsub.py backend/pipeline/common/feed_audit_notification_contract.py backend/pipeline/storage/feed_audit_notifications.py`

## Deviations From Plan

- Extracted shared notification contract constants to `backend.pipeline.common` to avoid duplicating `event_type`, `schema_version`, and required field vocabulary between producer and relay code.

## User Setup Required

None.
