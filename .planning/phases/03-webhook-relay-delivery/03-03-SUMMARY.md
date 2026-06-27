---
phase: 03-webhook-relay-delivery
plan: 03
subsystem: relay
tags: [urllib3, webhook, retry, pubsub-ack, fastapi]
requires:
  - phase: 03-webhook-relay-delivery
    provides: Plans 03-01 and 03-02 relay scaffold/parser
provides:
  - Watch Duty webhook client with exact two-attempt retry policy
  - FastAPI endpoint wiring from Pub/Sub payload to WD send
  - ACK/NACK behavior: relay returns 204 only after WD 2xx
  - Structured delivery logs without API key or full request payload duplication
affects: [phase-03, public-repo]
tech-stack:
  added: []
  patterns: [explicit urllib3 retry classification, sync HTTP in async route via to_thread, fakeable app-state WD sender]
key-files:
  created:
    - backend/pipeline/feed_audit_webhook/wd_client.py
    - backend/pipeline/feed_audit_webhook/tests/__init__.py
    - backend/pipeline/feed_audit_webhook/tests/test_main.py
    - backend/pipeline/feed_audit_webhook/tests/test_wd_client.py
  modified:
    - backend/pipeline/feed_audit_webhook/main.py
    - backend/pipeline/feed_audit_webhook/settings.py
    - backend/pipeline/feed_audit_webhook/tests/test_pubsub.py
key-decisions:
  - "Use explicit local retry in the WD client instead of urllib3 Retry so the two-attempt policy is directly testable."
  - "Run the synchronous urllib3 send in `asyncio.to_thread` from the FastAPI route."
  - "Do not log the full forwarded request payload; log event identifiers plus WD status/body on failure."
patterns-established:
  - "The relay returns HTTP 204 to Pub/Sub only after the WD backend returns 2xx."
  - "Timeout, connection failure, 408, 429, and 5xx are retried exactly once; non-transient 4xx are not retried locally."
requirements-completed:
  - RELAY-01
  - RELAY-02
  - RELAY-03
  - RELAY-04
  - RELAY-05
  - RELAY-06
duration: 20min
completed: 2026-06-27
---

# Phase 03 Plan 03: WD Webhook Client Summary

**Feed Audit Notification forwarding from Pub/Sub relay to Watch Duty backend**

## Accomplishments

- Added `WatchDutyWebhookClient`, a urllib3-based client that POSTs the extracted flat audit payload to the fixed WD webhook URL with `X-Api-Key`.
- Implemented exactly two local attempts for transient failures: timeout/connection exceptions, HTTP 408, 429, and 5xx.
- Wired the FastAPI endpoint so valid messages call the WD client and return Pub/Sub ACK (`204`) only after WD returns 2xx.
- Added structured logs for delivery success/failure that include event identifiers and WD response body on failure while omitting the API key and full request payload.
- Added focused tests for retry classification, header/body behavior, secret-safe logging, endpoint ACK/NACK behavior, and malformed-message handling.

## Task Commits

1. `e029d5e1` - `feat: forward feed audit events to wd webhook`

## Verification

- `safe-run -- uv run ruff format backend/pipeline/feed_audit_webhook`
- `safe-run -- uv run ruff check backend/pipeline/feed_audit_webhook backend/pipeline/common/feed_audit_notification_contract.py backend/pipeline/storage/feed_audit_notifications.py`
- `safe-run -- uv run python -m pytest backend/pipeline/feed_audit_webhook/tests backend/pipeline/storage/tests/test_feed_audit_notifications.py -q`
- `safe-run -- python3 -m py_compile backend/pipeline/feed_audit_webhook/main.py backend/pipeline/feed_audit_webhook/wd_client.py backend/pipeline/feed_audit_webhook/pubsub.py backend/pipeline/feed_audit_webhook/settings.py`

## Deviations From Plan

- The route uses `asyncio.to_thread` around the synchronous urllib3 send so concurrent Pub/Sub requests are not blocked on the FastAPI event loop.

## User Setup Required

None in the public repo. Deployment wiring follows in Plan 03-04.
